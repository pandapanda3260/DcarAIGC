#!/usr/bin/env python3
"""Normalize Xiaohongshu note links into auditable CSV files.

The script extracts note URLs from the two supplied text files, resolves
xhslink.com short links without downloading the destination page, parses note
IDs and xsec tokens, and writes both a full audit table and a deduplicated API
input table.
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parent
INPUTS = (
    (ROOT / "小红书汽车内容链接.txt", "auto", "A"),
    (ROOT / "小红书非汽车内容链接.txt", "non_auto", "N"),
)

URL_RE = re.compile(r"https?://[^\s\t\"'<>]+", re.IGNORECASE)
NOTE_ID_RE = re.compile(
    r"xiaohongshu\.com/(?:explore|discovery/item)/([0-9a-f]{24})(?:[/?#]|$)",
    re.IGNORECASE,
)
SHORT_RE = re.compile(r"^https?://(?:www\.)?xhslink\.com/o/", re.IGNORECASE)
CREATOR_RE = re.compile(r"^https?://creator\.xiaohongshu\.com/", re.IGNORECASE)

ALL_FIELDS = (
    "sample_id",
    "gold_label",
    "account_name",
    "share_title",
    "vv",
    "original_url",
    "resolved_url",
    "canonical_url",
    "note_id",
    "xsec_token",
    "xsec_source",
    "link_type",
    "source_file",
    "source_line",
    "parse_status",
    "duplicate_of",
)

UNIQUE_FIELDS = (
    "sample_id",
    "gold_label",
    "account_name",
    "share_title",
    "vv",
    "note_id",
    "canonical_url",
    "xsec_token",
    "xsec_source",
    "link_type",
    "source_file",
    "source_line",
)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


def clean_url(url: str) -> str:
    return url.strip().rstrip("),.;，。；）】》]")


def parse_note_url(url: str) -> dict[str, str]:
    match = NOTE_ID_RE.search(url)
    if not match:
        return {"note_id": "", "xsec_token": "", "xsec_source": "", "canonical_url": ""}

    note_id = match.group(1).lower()
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
    xsec_token = query.get("xsec_token", [""])[0]
    xsec_source = query.get("xsec_source", [""])[0]
    canonical = f"https://www.xiaohongshu.com/explore/{note_id}"
    canonical_query = {}
    if xsec_token:
        canonical_query["xsec_token"] = xsec_token
    if xsec_source:
        canonical_query["xsec_source"] = xsec_source
    if canonical_query:
        canonical += "?" + urllib.parse.urlencode(canonical_query)

    return {
        "note_id": note_id,
        "xsec_token": xsec_token,
        "xsec_source": xsec_source,
        "canonical_url": canonical,
    }


def resolve_short_url(url: str) -> tuple[str, str]:
    opener = urllib.request.build_opener(NoRedirect())
    for attempt in range(3):
        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
            method="GET",
        )
        try:
            with opener.open(request, timeout=20) as response:
                location = response.headers.get("Location", "")
                if location:
                    return urllib.parse.urljoin(url, location), "short_resolved"
                return response.geturl(), "short_no_redirect"
        except urllib.error.HTTPError as exc:
            if exc.code in {301, 302, 303, 307, 308}:
                location = exc.headers.get("Location", "")
                if location:
                    return urllib.parse.urljoin(url, location), "short_resolved"
            if 500 <= exc.code < 600 and attempt < 2:
                time.sleep(0.4 * (attempt + 1))
                continue
            return "", f"short_http_{exc.code}"
        except (urllib.error.URLError, TimeoutError):
            if attempt < 2:
                time.sleep(0.4 * (attempt + 1))
                continue
            return "", "short_network_error"
    return "", "short_network_error"


def extract_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    counters = {"A": 0, "N": 0}

    for path, label, prefix in INPUTS:
        with path.open("r", encoding="utf-8-sig", errors="replace") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                line = raw_line.strip()
                if not line:
                    continue

                urls = [clean_url(match.group(0)) for match in URL_RE.finditer(line)]
                if not urls:
                    continue

                parts = [part.strip().strip('"') for part in raw_line.rstrip("\r\n").split("\t")]
                account_name = ""
                vv = ""
                share_title = ""
                if label == "auto" and len(parts) >= 3 and parts[1].replace(",", "").isdigit():
                    account_name = parts[0]
                    vv = parts[1].replace(",", "")
                elif label == "non_auto":
                    first_url_start = min(match.start() for match in URL_RE.finditer(line))
                    share_title = line[:first_url_start].strip().strip('"')

                for url in urls:
                    if "itunes.apple.com" in url:
                        continue
                    if not (
                        "xhslink.com" in url
                        or "xiaohongshu.com" in url
                    ):
                        continue

                    counters[prefix] += 1
                    link_type = "short" if SHORT_RE.search(url) else "direct"
                    status = "pending_redirect" if link_type == "short" else "direct_pending_parse"
                    if CREATOR_RE.search(url):
                        link_type = "invalid"
                        status = "invalid_creator_page"

                    rows.append(
                        {
                            "sample_id": f"{prefix}{counters[prefix]:04d}",
                            "gold_label": label,
                            "account_name": account_name,
                            "share_title": share_title,
                            "vv": vv,
                            "original_url": url,
                            "resolved_url": "",
                            "canonical_url": "",
                            "note_id": "",
                            "xsec_token": "",
                            "xsec_source": "",
                            "link_type": link_type,
                            "source_file": path.name,
                            "source_line": str(line_no),
                            "parse_status": status,
                            "duplicate_of": "",
                        }
                    )
    return rows


def resolve_and_parse(rows: list[dict[str, str]]) -> None:
    short_urls = sorted({row["original_url"] for row in rows if row["link_type"] == "short"})
    resolved: dict[str, tuple[str, str]] = {}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(resolve_short_url, url): url for url in short_urls}
        for future in as_completed(futures):
            url = futures[future]
            try:
                resolved[url] = future.result()
            except Exception:  # Defensive: never expose URLs or tokens in errors.
                resolved[url] = ("", "short_unexpected_error")

    for row in rows:
        if row["link_type"] == "invalid":
            continue
        candidate_url = row["original_url"]
        status = "direct_parsed"
        if row["link_type"] == "short":
            candidate_url, status = resolved.get(candidate_url, ("", "short_not_resolved"))
            row["resolved_url"] = candidate_url
        if not candidate_url:
            row["parse_status"] = status
            continue

        parsed = parse_note_url(candidate_url)
        row.update(parsed)
        if parsed["note_id"]:
            row["parse_status"] = "short_resolved" if row["link_type"] == "short" else "direct_parsed"
        else:
            row["parse_status"] = f"{status}_no_note_id"

    seen: dict[str, dict[str, str]] = {}
    for row in rows:
        note_id = row["note_id"]
        if not note_id:
            continue
        if note_id in seen:
            row["duplicate_of"] = seen[note_id]["sample_id"]
            if seen[note_id]["gold_label"] != row["gold_label"]:
                row["parse_status"] = "label_conflict"
            else:
                row["parse_status"] += "_duplicate"
        else:
            seen[note_id] = row


def write_csv(path: Path, rows: list[dict[str, str]], fields: tuple[str, ...]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    rows = extract_rows()
    resolve_and_parse(rows)

    all_path = ROOT / "notes_all.csv"
    unique_path = ROOT / "notes_unique.csv"
    report_path = ROOT / "notes_prepare_report.json"

    unique_rows = [
        row
        for row in rows
        if row["note_id"]
        and not row["duplicate_of"]
        and row["parse_status"] != "label_conflict"
    ]

    write_csv(all_path, rows, ALL_FIELDS)
    write_csv(unique_path, unique_rows, UNIQUE_FIELDS)

    by_label = {}
    for label in ("auto", "non_auto"):
        label_rows = [row for row in rows if row["gold_label"] == label]
        unique_label_rows = [row for row in unique_rows if row["gold_label"] == label]
        by_label[label] = {
            "extracted_urls": len(label_rows),
            "unique_parsed_notes": len(unique_label_rows),
            "unresolved_or_invalid": sum(not row["note_id"] for row in label_rows),
            "duplicates": sum(bool(row["duplicate_of"]) for row in label_rows),
        }

    report = {
        "all_rows": len(rows),
        "unique_parsed_notes": len(unique_rows),
        "by_label": by_label,
        "outputs": [all_path.name, unique_path.name],
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
