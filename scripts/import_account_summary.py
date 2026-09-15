#!/usr/bin/env python3
"""Offline, explicit-path account summary import. Never opens a network service."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
from datetime import datetime, timezone
from urllib.parse import quote
import xml.etree.ElementTree as ET
import zipfile
import posixpath

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "dcar_eval"))

HEADERS = ["平台", "运营人员", "账号名称", "ID（抖音号/快手号/小红书号/视频号）", "uid", "粉丝", "更新状态", "质量标签", "业务标签", "是否开通接单", "手机号", "手机号开卡人姓名", "使用人证件号码", "持卡人", "是否实名", "实名来源"]
NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"


def stamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256(path):
    with Path(path).open("rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()


def private_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
        json.dump(value, f, ensure_ascii=False, indent=2, default=str)
        f.write("\n")
    path.chmod(0o600)


def readonly(path):
    return sqlite3.connect("file:" + quote(str(Path(path).absolute()), safe="/") + "?mode=ro", uri=True)


def read_workbook(path, metadata_path=None):
    """Read actual XLSX cells/comments with stdlib; never execute formulas/macros."""
    path = Path(path)
    with zipfile.ZipFile(path) as z:
        rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
        sheets = ET.fromstring(z.read("xl/workbook.xml")).find("s:sheets", NS)
        if len(sheets) != 1:
            raise ValueError("Expected the single-sheet reviewed summary workbook")
        sheet = sheets[0]
        target = rels[sheet.get("{" + REL + "}id")]
        target = posixpath.normpath(posixpath.join("xl", target)) if not target.startswith("/") else target.lstrip("/")
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            shared = ["".join(node.itertext()) for node in ET.fromstring(z.read("xl/sharedStrings.xml"))]
        formats, styles = {}, []
        if "xl/styles.xml" in z.namelist():
            style_root = ET.fromstring(z.read("xl/styles.xml"))
            formats = {x.get("numFmtId"): x.get("formatCode", "") for x in style_root.findall("s:numFmts/s:numFmt", NS)}
            styles = [formats.get(x.get("numFmtId"), "") for x in style_root.findall("s:cellXfs/s:xf", NS)]
        notes = {}
        rel_path = posixpath.join(posixpath.dirname(target), "_rels", posixpath.basename(target) + ".rels")
        if rel_path in z.namelist():
            for relation in ET.fromstring(z.read(rel_path)):
                if relation.get("Type", "").endswith("/comments"):
                    comment_target = relation.get("Target")
                    comment_target = comment_target.lstrip("/") if comment_target.startswith("/") else posixpath.normpath(posixpath.join(posixpath.dirname(target), comment_target))
                    for comment in ET.fromstring(z.read(comment_target)).findall("s:commentList/s:comment", NS):
                        notes[comment.get("ref")] = "".join(comment.find("s:text", NS).itertext())
        rows = []
        for row in ET.fromstring(z.read(target)).findall("s:sheetData/s:row", NS):
            index = int(row.get("r"))
            values = [None] * 16
            for cell in row.findall("s:c", NS):
                address = cell.get("r")
                letters = "".join(x for x in address if x.isalpha())
                col = 0
                for letter in letters:
                    col = col * 26 + ord(letter) - 64
                if cell.find("s:f", NS) is not None:
                    raise ValueError(f"Formula in source workbook: {address}")
                value_node = cell.find("s:v", NS)
                raw = value_node.text if value_node is not None else None
                kind = cell.get("t")
                if kind == "inlineStr":
                    inline = cell.find("s:is", NS)
                    value = "".join(inline.itertext()) if inline is not None else ""
                elif kind == "s":
                    value = shared[int(raw)]
                elif kind in {"str", "e"}:
                    value = raw
                elif raw is None:
                    value = None
                elif col == 6 and index > 1:
                    numeric = float(raw)
                    value = int(numeric) if numeric.is_integer() else numeric
                    format_code = styles[int(cell.get("s", "0"))] if styles else ""
                    if "约" in format_code:
                        value = "约" + str(value)
                else:
                    # Identifier numerics cannot establish lost precision or zeros.
                    if col in {4, 5, 11, 13} and index > 1:
                        raise ValueError(f"Identifier is not stored as text: {address}")
                    value = raw
                if col > 16 and value not in (None, ""):
                    raise ValueError(f"Unexpected nonempty column: {address}")
                if col <= 16:
                    values[col - 1] = value
            if index == 1:
                if values != HEADERS:
                    raise ValueError("Source workbook headers/order differ from the 16-column contract")
            elif any(v not in (None, "") for v in values) or notes.get(f"C{index}"):
                rows.append({"sourceRow": index, "raw": dict(zip(HEADERS, values)), "comment": notes.get(f"C{index}", ""), "metadata": {}})
    payload = {"sha256": sha256(path), "source": path.name, "sheet": sheet.get("name"), "records": rows}
    if metadata_path:
        metadata = json.loads(Path(metadata_path).read_text())
        indexed = {r["excel_row"]: r for r in metadata["rows"]}
        if len(indexed) != len(rows) or metadata["headers"] != HEADERS:
            raise ValueError("Enrichment metadata does not cover this workbook")
        def equal(a, b):
            return (a in (None, "") and b in (None, "")) or a == b
        for row in rows:
            evidence = indexed.get(row["sourceRow"])
            if evidence is None or not all(equal(a, b) for a, b in zip(row["raw"].values(), evidence["values"])) or row["comment"] != evidence["comment"]:
                raise ValueError(f"Workbook/evidence mismatch at row {row['sourceRow']}")
            row["metadata"] = {k: v for k, v in evidence.items() if k not in {"values", "comment"}}
        payload["metadata_sha256"] = sha256(metadata_path)
    return payload


def backup(source, destination):
    destination = Path(destination).absolute()
    if destination.exists():
        raise ValueError("Backup destination already exists; refusing overwrite")
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination.touch(mode=0o600, exist_ok=False)
    with readonly(source) as src, sqlite3.connect(destination) as dst:
        src.backup(dst, pages=4096, sleep=0.05)
        check = dst.execute("PRAGMA quick_check").fetchall()
        if check != [("ok",)]:
            raise ValueError("Backup quick_check failed")
    return {"path": str(destination), "sha256": sha256(destination), "bytes": destination.stat().st_size, "quick_check": "ok", "created_at": stamp()}


def counts(connection):
    return {r[0]: connection.execute('SELECT COUNT(*) FROM "' + r[0].replace('"', '""') + '"').fetchone()[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")}


IMPORT_TABLES = frozenset({
    "accounts", "account_platform_identities", "account_directory_rows",
    "account_intake_requests",
})


def authorize_import(action, table, column, database, trigger):
    """Limit imports, including trigger writes, to their four owned tables.

    Count checks alone cannot detect an UPDATE to an existing history row.
    SQLite authorizes ordinary and trigger statements before executing them;
    automatic AUTOINCREMENT bookkeeping does not need direct sequence writes.
    """
    if action == sqlite3.SQLITE_DELETE:
        return sqlite3.SQLITE_DENY
    if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE}:
        # Schema23 invalidates cached catalog plans through immutable schema
        # triggers. Imports may advance that revision, but cannot write the
        # counter directly or suppress/replace its projection guard.
        if (action == sqlite3.SQLITE_UPDATE and database == "main" and table == "capture_catalog_revision"
                and column == "revision" and trigger in {f"trg_catalog_revision_{name}_{event}"
                    for name in IMPORT_TABLES for event in ("insert", "update")}):
            return sqlite3.SQLITE_OK
        return sqlite3.SQLITE_OK if database == "main" and table in IMPORT_TABLES else sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True, help="Explicit isolated local candidate, never installed writer database")
    parser.add_argument("--xlsx", type=Path)
    parser.add_argument("--metadata", type=Path)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--backup", type=Path, help="Required pre-import backup destination with --apply")
    parser.add_argument("--apply", action="store_true", help="Otherwise transaction is rolled back")
    parser.add_argument("--clone-local-source", type=Path, help="Read-only SQLite backup into --db; do not import")
    args = parser.parse_args()
    os.umask(0o077)
    paths = [(label, path.resolve()) for label, path in vars(args).items() if isinstance(path, Path)]
    for index, (label, path) in enumerate(paths):
        for other_label, other in paths[index + 1:]:
            if path == other or path.exists() and other.exists() and path.samefile(other):
                parser.error(f"{label} and {other_label} must be separate files")
    if args.report.is_symlink():
        parser.error("Report destination cannot be a symlink")
    if args.clone_local_source:
        if args.apply or args.xlsx or args.metadata or args.backup:
            parser.error("Clone is a separate read-only operation")
        if not args.clone_local_source.is_absolute() or not args.clone_local_source.is_file() or not args.db.is_absolute():
            parser.error("Clone requires explicit existing absolute local source and absolute new target")
        receipt = backup(args.clone_local_source, args.db)
        receipt["source"] = str(args.clone_local_source)
        private_json(args.report, receipt)
        print(json.dumps({"status": "cloned", "quick_check": "ok", "bytes": receipt["bytes"]}))
        return
    if not args.xlsx or (args.apply and not args.backup):
        parser.error("--xlsx required; --apply also requires a new --backup destination")
    from v8.runtime_database import resolve_isolated_candidate
    from v8.account_summary_import import import_account_summary
    from v8.storage import configure_connection_safety
    access = resolve_isolated_candidate(args.db)
    payload = read_workbook(args.xlsx, args.metadata)
    backup_receipt = backup(access.database, args.backup) if args.apply else None
    connection = sqlite3.connect(access.database)
    connection.row_factory = sqlite3.Row
    try:
        configure_connection_safety(connection)
        connection.execute("BEGIN IMMEDIATE")
        before = counts(connection)
        schema_before = list(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"))
        connection.set_authorizer(authorize_import)
        result = import_account_summary(connection, payload, imported_at=stamp())
        after = counts(connection)
        schema_after = list(connection.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name"))
        if schema_before != schema_after:
            raise ValueError("Unexpected schema change")
        unrelated = {name: [before.get(name), after.get(name)] for name in before if name not in IMPORT_TABLES | {"sqlite_sequence"} and before.get(name) != after.get(name)}
        if unrelated:
            raise ValueError("Import changed unrelated table counts")
        foreign = [tuple(x) for x in connection.execute("PRAGMA foreign_key_check")]
        if foreign:
            raise ValueError("Foreign key check failed")
        result.update(mode="apply" if args.apply else "dry_run", input_path=str(args.xlsx.absolute()), metadata_sha256=payload.get("metadata_sha256"), database_identity=access.health_identity(), backup=backup_receipt, before_counts=before, after_counts=after, schema_unchanged=True, foreign_key_check="ok", completed_at=stamp())
        if args.apply:
            connection.commit()
        else:
            connection.rollback()
        private_json(args.report, result)
        print(json.dumps({"mode": result["mode"], "counts": result.get("counts"), "schema_unchanged": True, "foreign_key_check": "ok"}, ensure_ascii=False))
    except BaseException:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()
