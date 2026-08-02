#!/usr/bin/env python3
"""Probe Bright Data Remote MCP without logging credentials or response bodies.

The probe performs only the MCP handshake and tools/list. It does not scrape a
Xiaohongshu page and does not make a paid data-collection decision.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import re
import sys
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
KEY_ROOT = Path("/Users/mark/Documents/key/DcarKey")
HOST = "mcp.brightdata.com"
ENDPOINT = "/mcp"
REQUESTED_VERSION = "2025-06-18"
SUPPORTED_VERSIONS = {"2025-06-18", "2025-03-26", "2024-11-05"}
TIMEOUT_SECONDS = 30
MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class ProbeError(RuntimeError):
    """A safe error whose message contains no remote response body or URL."""


def load_env(path: Path) -> dict[str, str]:
    if not path.exists():
        raise ProbeError(f"credential file not found: {path.name}")
    if path.stat().st_mode & 0o077:
        raise ProbeError(f"{path.name} must not be readable by group or other users")

    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(
        path.read_text(encoding="utf-8-sig").splitlines(), start=1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            raise ProbeError(f"invalid env syntax at line {line_number}")
        name, value = line.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ProbeError(f"invalid env key at line {line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[name] = value
    return values


def read_limited(response: http.client.HTTPResponse) -> bytes:
    data = response.read(MAX_RESPONSE_BYTES + 1)
    if len(data) > MAX_RESPONSE_BYTES:
        raise ProbeError("response exceeded safety limit")
    return data


def read_sse(response: http.client.HTTPResponse, expected_id: int) -> dict[str, Any]:
    data_lines: list[str] = []
    consumed = 0

    def parse_event() -> dict[str, Any] | None:
        if not data_lines:
            return None
        try:
            message = json.loads("\n".join(data_lines))
        except json.JSONDecodeError:
            return None
        if isinstance(message, dict) and message.get("id") == expected_id:
            return message
        return None

    while True:
        raw_line = response.readline()
        if not raw_line:
            message = parse_event()
            if message is not None:
                return message
            break
        consumed += len(raw_line)
        if consumed > MAX_RESPONSE_BYTES:
            raise ProbeError("SSE response exceeded safety limit")
        line = raw_line.decode("utf-8").rstrip("\r\n")
        if not line:
            message = parse_event()
            data_lines = []
            if message is not None:
                return message
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    raise ProbeError("SSE ended before the expected JSON-RPC response")


class McpClient:
    def __init__(self, token: str, *, pro: bool, groups: str | None) -> None:
        query = {"token": token}
        if pro:
            query["pro"] = "1"
        if groups:
            query["groups"] = groups
        self.path = f"{ENDPOINT}?{urllib.parse.urlencode(query)}"
        self.session_id: str | None = None
        self.protocol_version: str | None = None
        self.server_info: dict[str, Any] = {}
        self.capabilities: dict[str, Any] = {}
        self.next_id = 1

    def post(
        self,
        message: dict[str, Any],
        *,
        expected_id: int | None,
        subsequent: bool,
    ) -> dict[str, Any] | None:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "User-Agent": "brightdata-mcp-readiness-probe/0.1",
        }
        if subsequent:
            if not self.protocol_version:
                raise ProbeError("MCP protocol version was not negotiated")
            headers["MCP-Protocol-Version"] = self.protocol_version
            if self.session_id:
                headers["Mcp-Session-Id"] = self.session_id

        body = json.dumps(message, separators=(",", ":")).encode("utf-8")
        connection = http.client.HTTPSConnection(HOST, timeout=TIMEOUT_SECONDS)
        try:
            connection.request("POST", self.path, body=body, headers=headers)
            response = connection.getresponse()
            returned_session = response.getheader("Mcp-Session-Id")
            if returned_session:
                if self.session_id and returned_session != self.session_id:
                    raise ProbeError("server changed MCP session id")
                self.session_id = returned_session

            if response.status == 202 and expected_id is None:
                read_limited(response)
                return None
            if not 200 <= response.status < 300:
                read_limited(response)
                if response.status == 401:
                    raise ProbeError("HTTP 401: API Token was rejected")
                raise ProbeError(f"HTTP {response.status}")
            if expected_id is None:
                read_limited(response)
                return None

            content_type = response.getheader("Content-Type", "").split(";", 1)[0].lower()
            if content_type == "application/json":
                output = json.loads(read_limited(response).decode("utf-8"))
            elif content_type == "text/event-stream":
                output = read_sse(response, expected_id)
            else:
                read_limited(response)
                raise ProbeError("unsupported response content type")
        finally:
            connection.close()

        if not isinstance(output, dict):
            raise ProbeError("invalid JSON-RPC response")
        if output.get("id") != expected_id:
            raise ProbeError("JSON-RPC response id mismatch")
        if "error" in output:
            error = output.get("error")
            code = error.get("code", "unknown") if isinstance(error, dict) else "unknown"
            raise ProbeError(f"JSON-RPC error code {code}")
        result = output.get("result")
        if not isinstance(result, dict):
            raise ProbeError("JSON-RPC result missing or invalid")
        return result

    def request(
        self, method: str, params: dict[str, Any] | None = None, *, initial: bool = False
    ) -> dict[str, Any]:
        request_id = self.next_id
        self.next_id += 1
        message: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
        }
        if params is not None:
            message["params"] = params
        result = self.post(message, expected_id=request_id, subsequent=not initial)
        if result is None:
            raise ProbeError("request returned no result")
        return result

    def notify(self, method: str) -> None:
        self.post(
            {"jsonrpc": "2.0", "method": method},
            expected_id=None,
            subsequent=True,
        )

    def initialize(self) -> None:
        result = self.request(
            "initialize",
            {
                "protocolVersion": REQUESTED_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "brightdata-mcp-readiness-probe",
                    "version": "0.1",
                },
            },
            initial=True,
        )
        version = result.get("protocolVersion")
        if version not in SUPPORTED_VERSIONS:
            raise ProbeError("server negotiated an unsupported MCP protocol version")
        self.protocol_version = str(version)
        self.server_info = result.get("serverInfo") or {}
        self.capabilities = result.get("capabilities") or {}
        self.notify("notifications/initialized")

    def list_tools(self) -> list[dict[str, Any]]:
        if "tools" not in self.capabilities:
            raise ProbeError("server did not advertise the tools capability")
        tools: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _ in range(100):
            params = {} if cursor is None else {"cursor": cursor}
            result = self.request("tools/list", params)
            page = result.get("tools")
            if not isinstance(page, list) or not all(isinstance(item, dict) for item in page):
                raise ProbeError("invalid tools/list response")
            tools.extend(page)
            next_cursor = result.get("nextCursor")
            if not next_cursor:
                return tools
            cursor = str(next_cursor)
            if cursor in seen_cursors:
                raise ProbeError("tools/list cursor loop")
            seen_cursors.add(cursor)
        raise ProbeError("tools/list exceeded 100 pages")


def safe_line(value: Any, limit: int = 500) -> str:
    return re.sub(r"[\r\n\t]+", " ", str(value or "")).strip()[:limit]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Handshake with Bright Data Remote MCP and list enabled tools safely."
    )
    parser.add_argument("--env", type=Path, default=KEY_ROOT / "brightdata.env")
    parser.add_argument("--output", type=Path, default=ROOT / "brightdata_mcp_tools.json")
    parser.add_argument("--pro", action="store_true", help="Request Bright Data Pro mode.")
    parser.add_argument("--groups", help="Optional Bright Data tool groups, such as social.")
    args = parser.parse_args()

    values = load_env(args.env)
    token = os.environ.get("BRIGHT_DATA_API_TOKEN") or values.get("BRIGHT_DATA_API_TOKEN")
    if not token or not token.strip():
        raise ProbeError("BRIGHT_DATA_API_TOKEN is missing")

    env_pro = values.get("BRIGHT_DATA_MCP_PRO", "").strip().lower() in {"1", "true", "yes"}
    groups = args.groups or values.get("BRIGHT_DATA_MCP_GROUPS") or None
    client = McpClient(token.strip(), pro=args.pro or env_pro, groups=groups)
    client.initialize()
    tools = client.list_tools()

    sanitized_tools = []
    for tool in tools:
        schema = tool.get("inputSchema") if isinstance(tool.get("inputSchema"), dict) else {}
        required = schema.get("required") if isinstance(schema.get("required"), list) else []
        sanitized_tools.append(
            {
                "name": safe_line(tool.get("name"), 200),
                "description": safe_line(tool.get("description")),
                "required_inputs": [safe_line(item, 100) for item in required],
            }
        )

    names = [item["name"].lower() for item in sanitized_tools]
    keywords = ("xiaohongshu", "redbook", "smallredbook", "xhs")
    result = {
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "connected": True,
        "protocol_version": client.protocol_version,
        "server": {
            "name": safe_line(client.server_info.get("name"), 200),
            "version": safe_line(client.server_info.get("version"), 100),
        },
        "pro_requested": bool(args.pro or env_pro),
        "groups_requested": groups,
        "tool_count": len(sanitized_tools),
        "has_named_xiaohongshu_tool": any(
            any(keyword in name for keyword in keywords) for name in names
        ),
        "tools": sanitized_tools,
    }
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print("connected=yes")
    print(f"protocol={client.protocol_version}")
    print(f"tool_count={len(sanitized_tools)}")
    print(f"has_named_xiaohongshu_tool={str(result['has_named_xiaohongshu_tool']).lower()}")
    print(f"output={args.output}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeError as exc:
        print(f"probe_failed={exc}", file=sys.stderr)
        raise SystemExit(2)
    except (
        OSError,
        http.client.HTTPException,
        UnicodeError,
        json.JSONDecodeError,
    ) as exc:
        # Never print exception text: network errors can embed the token-bearing URL.
        print(f"probe_failed={type(exc).__name__}", file=sys.stderr)
        raise SystemExit(3)
