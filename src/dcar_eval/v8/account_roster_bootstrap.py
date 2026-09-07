"""Verified official-export to existing-identity bootstrap conversion."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .account_roster import PLATFORMS, RosterError, normalize_profile


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_time(value: str | None) -> str | None:
    if not value or value.strip() in {"-", "--", "—", "未开启", "未监测"}:
        return None
    parsed = datetime.fromisoformat(value.strip().replace(" +", "+").replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    return parsed.isoformat()


def build_bootstrap_envelope(
    roster_path: Path, analysis_path: Path, source: sqlite3.Connection,
) -> dict[str, Any]:
    """Use Sheet2 only as verified UID evidence; runtime membership is the export."""
    roster_path, analysis_path = roster_path.resolve(), analysis_path.resolve()
    analysis_sha = _sha(analysis_path)
    official = json.loads(roster_path.read_bytes())
    analysis = json.loads(analysis_path.read_bytes())
    export = official.get("export_source") or {}
    if official.get("coverage") != "full" or not export.get("read_only_download"):
        raise RosterError("incomplete_scope", "Bootstrap source is not the verified complete official export")
    export_path = Path(export["path"])
    if not export_path.is_file() or export_path.is_symlink():
        raise RosterError("export_missing", "The corresponding original official workbook is required")
    rows = official.get("accounts")
    if not isinstance(rows, list) or len(rows) != export.get("rows") or len(rows) != official["summary"]["imported"]:
        raise RosterError("incomplete_roster", "Official export, UI and declared totals disagree")
    uid_map: dict[tuple[str, str], str] = {}
    for row in analysis["sheet2"]:
        platform = row["platform"]
        key = platform, normalize_profile(platform, row["link"])
        uid = row["uid"]
        if not isinstance(uid, str) or not uid:
            raise RosterError("identity_unresolved", "Sheet2 UID evidence is unresolved")
        if key in uid_map and uid_map[key] != uid:
            raise RosterError("identity_conflict", "Sheet2 contains conflicting verified homepage evidence")
        uid_map[key] = uid
    members, mapped_ids = [], []
    for row in rows:
        platform = {"抖音": "douyin", "小红书": "xiaohongshu"}.get(row["platform"], row["platform"])
        profile = normalize_profile(platform, row["profile_url"])
        uid = uid_map.get((platform, profile))
        identity = source.execute(
            "SELECT id FROM account_platform_identities WHERE platform=? AND uid=?", (platform, uid),
        ).fetchone() if uid else None
        if identity is None:
            raise RosterError("identity_unresolved", "An official member cannot be mapped to an existing verified identity")
        mapped_ids.append(int(identity["id"]))
        auth = dict(row.get("authorization_from_export") or {})
        auth_values = set(auth.values())
        authorization = (
            "unauthorized" if auth_values == {"未授权"}
            else "authorized" if "已授权" in auth_values else "unknown"
        )
        monitored = row.get("monitoring_enabled")
        members.append({
            "platform": platform, "matrix_account_id": row["matrix_account_id"],
            "profile_ref": profile, "uid": uid, "nickname": row.get("name", ""),
            "monitoring_status": "monitored" if monitored is True else "not_monitored" if monitored is False else "unknown",
            "authorization_status": authorization,
            "monitoring_started_at": _source_time(row.get("monitoring_started_at")),
            "metadata": {
                "display_account_id": row.get("display_account_id"),
                "authorization_capabilities": auth,
                "uid_evidence": {"source_sha256": analysis_sha, "section": "sheet2", "match": "platform+profile"},
            },
        })
    if len(set(mapped_ids)) != len(members):
        raise RosterError("identity_conflict", "Official members do not map one-to-one to local identities")
    captured = _source_time(export["created_at"])
    source_sha = _sha(roster_path)
    payload = {
        "source_type": "bootstrap_export", "require_existing_identities": True,
        "source_captured_at": captured,
        "scope": {"organization": official["organization"], "coverage": "full",
                  "account_scope": "all_added_accounts", "platforms": sorted(PLATFORMS)},
        "source_evidence": {
            "kind": "official_export", "evidence_kind": "official_export_metadata",
            "export_record_id": export_path.name, "exported_at": captured,
            "scope_evidence": f"{official['source']} ; {export['sheet']}!{export['range']}",
            "source_name": roster_path.name, "source_format": "matrix-ui-verified-export-v1",
            "source_sha256": source_sha, "official_workbook_sha256": _sha(export_path),
            "official_workbook_path": str(export_path),
            "uid_evidence_sha256": analysis_sha, "uid_evidence_path": str(analysis_path),
        },
        "declared_count": len(members),
        "pagination": {"expected_pages": 1, "pages": [1], "terminal": True,
                       "declared_totals": [len(members)]},
        "members": members,
    }
    return {"payload": payload, "source_path": str(roster_path), "source_sha256": source_sha,
            "mapping": {"member_count": len(members), "mapped_identity_count": len(set(mapped_ids)),
                        "identity_ids": sorted(mapped_ids)}}
