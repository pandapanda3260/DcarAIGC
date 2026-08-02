"""Versioned selling-point draft CRUD and atomic publication."""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .storage import DEFAULT_DB, connect, now_utc, transaction


class TaxonomyError(RuntimeError):
    pass


SCENES = {"new_car", "used_car", "media"}
TIERS = {"core", "other"}
CODE_RE = re.compile(r"^[A-Z][1-9][0-9]?$", re.ASCII)
VERSION_RE = re.compile(r"^selling-points-v(\d+)\.(\d+)$")


def _next_version(current: str) -> str:
    match = VERSION_RE.fullmatch(current)
    if match is None:
        raise TaxonomyError(f"unsupported taxonomy version: {current}")
    return f"selling-points-v{int(match.group(1))}.{int(match.group(2)) + 1}"


def _validate_point(value: Mapping[str, Any], *, require_code: bool) -> Dict[str, Any]:
    code = str(value.get("code") or "").strip().upper()
    if require_code and not CODE_RE.fullmatch(code):
        raise TaxonomyError("selling point code must be one letter plus 1-2 digits")
    label = str(value.get("label") or "").strip()
    if not label:
        raise TaxonomyError("selling point label is required")
    tier = str(value.get("tier") or "")
    if tier not in TIERS:
        raise TaxonomyError("selling point tier must be core or other")
    scenes = list(dict.fromkeys(str(scene) for scene in value.get("scenes", [])))
    if not scenes or any(scene not in SCENES for scene in scenes):
        raise TaxonomyError("selling point must have one or more valid scenes")
    output = {
        "code": code,
        "label": label,
        "tier": tier,
        "definition": str(value.get("definition") or "").strip(),
        "positive_evidence": [
            str(item).strip() for item in value.get("positive_evidence", []) if str(item).strip()
        ],
        "negative_evidence": [
            str(item).strip() for item in value.get("negative_evidence", []) if str(item).strip()
        ],
        "boundary_rules": [
            str(item).strip() for item in value.get("boundary_rules", []) if str(item).strip()
        ],
        "scenes": scenes,
    }
    return output


def _taxonomy_row(connection: sqlite3.Connection, status: str) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT * FROM taxonomy_versions WHERE status=?
        ORDER BY created_at DESC LIMIT 1
        """,
        (status,),
    ).fetchone()


def ensure_draft(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        existing = _taxonomy_row(connection, "draft")
        if existing is not None:
            return dict(existing)
        published = _taxonomy_row(connection, "published")
        if published is None:
            raise TaxonomyError("no published taxonomy to clone")
        taxonomy_id = f"taxonomy-{uuid.uuid4().hex}"
        version = _next_version(str(published["version"]))
        captured_at = now_utc()
        connection.execute(
            """
            INSERT INTO taxonomy_versions(
                id, version, status, definition, source_path, source_sha256, created_at
            ) VALUES (?, ?, 'draft', ?, ?, ?, ?)
            """,
            (
                taxonomy_id, version, published["definition"], published["source_path"],
                published["source_sha256"], captured_at,
            ),
        )
        points = connection.execute(
            "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY id",
            (published["id"],),
        ).fetchall()
        for point in points:
            cursor = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, definition, positive_evidence_json,
                    negative_evidence_json, boundary_rules_json, enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    taxonomy_id, point["code"], point["tier"], point["label"],
                    point["definition"], point["positive_evidence_json"],
                    point["negative_evidence_json"], point["boundary_rules_json"],
                    point["enabled"],
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("selling point clone returned no id")
            scenes = connection.execute(
                "SELECT scene FROM selling_point_scenes WHERE selling_point_id=?",
                (point["id"],),
            ).fetchall()
            for scene in scenes:
                connection.execute(
                    "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, ?)",
                    (cursor.lastrowid, scene["scene"]),
                )
    return {
        "id": taxonomy_id,
        "version": version,
        "status": "draft",
        "created_at": captured_at,
    }


def list_points(
    *, status: str = "published", db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    if status not in {"published", "draft"}:
        raise TaxonomyError("taxonomy status must be published or draft")
    with connect(db_path) as connection:
        taxonomy = _taxonomy_row(connection, status)
        if taxonomy is None:
            return {"taxonomy": None, "items": []}
        rows = connection.execute(
            """
            SELECT * FROM selling_points WHERE taxonomy_id=? AND enabled=1
            ORDER BY substr(code, 1, 1), CAST(substr(code, 2) AS INTEGER)
            """,
            (taxonomy["id"],),
        ).fetchall()
        items: List[Dict[str, Any]] = []
        for row in rows:
            scenes = connection.execute(
                "SELECT scene FROM selling_point_scenes WHERE selling_point_id=? ORDER BY scene",
                (row["id"],),
            ).fetchall()
            items.append(
                {
                    "code": row["code"],
                    "tier": row["tier"],
                    "label": row["label"],
                    "definition": row["definition"],
                    "positive_evidence": json.loads(row["positive_evidence_json"]),
                    "negative_evidence": json.loads(row["negative_evidence_json"]),
                    "boundary_rules": json.loads(row["boundary_rules_json"]),
                    "scenes": [scene["scene"] for scene in scenes],
                }
            )
    return {
        "taxonomy": {
            "id": taxonomy["id"],
            "version": taxonomy["version"],
            "status": taxonomy["status"],
            "created_at": taxonomy["created_at"],
            "published_at": taxonomy["published_at"],
        },
        "items": items,
    }


def _draft(connection: sqlite3.Connection) -> sqlite3.Row:
    row = _taxonomy_row(connection, "draft")
    if row is None:
        raise TaxonomyError("create a draft before editing selling points")
    return row


def create_point(value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    point = _validate_point(value, require_code=True)
    with connect(db_path) as connection, transaction(connection):
        draft = _draft(connection)
        cursor = connection.execute(
            """
            INSERT INTO selling_points(
                taxonomy_id, code, tier, label, definition, positive_evidence_json,
                negative_evidence_json, boundary_rules_json, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                draft["id"], point["code"], point["tier"], point["label"],
                point["definition"], json.dumps(point["positive_evidence"], ensure_ascii=False),
                json.dumps(point["negative_evidence"], ensure_ascii=False),
                json.dumps(point["boundary_rules"], ensure_ascii=False),
            ),
        )
        if cursor.lastrowid is None:
            raise RuntimeError("selling point insert returned no id")
        for scene in point["scenes"]:
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, ?)",
                (cursor.lastrowid, scene),
            )
    return point


def update_point(
    code: str, value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB
) -> Dict[str, Any]:
    point = _validate_point({**value, "code": code}, require_code=True)
    with connect(db_path) as connection, transaction(connection):
        draft = _draft(connection)
        row = connection.execute(
            "SELECT id FROM selling_points WHERE taxonomy_id=? AND code=?",
            (draft["id"], code),
        ).fetchone()
        if row is None:
            raise TaxonomyError(f"selling point {code} does not exist in draft")
        connection.execute(
            """
            UPDATE selling_points SET tier=?, label=?, definition=?,
                positive_evidence_json=?, negative_evidence_json=?, boundary_rules_json=?
            WHERE id=?
            """,
            (
                point["tier"], point["label"], point["definition"],
                json.dumps(point["positive_evidence"], ensure_ascii=False),
                json.dumps(point["negative_evidence"], ensure_ascii=False),
                json.dumps(point["boundary_rules"], ensure_ascii=False), row["id"],
            ),
        )
        connection.execute(
            "DELETE FROM selling_point_scenes WHERE selling_point_id=?", (row["id"],)
        )
        for scene in point["scenes"]:
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, ?)",
                (row["id"], scene),
            )
    return point


def delete_point(code: str, *, db_path: Path = DEFAULT_DB) -> None:
    with connect(db_path) as connection, transaction(connection):
        draft = _draft(connection)
        cursor = connection.execute(
            "DELETE FROM selling_points WHERE taxonomy_id=? AND code=?",
            (draft["id"], code),
        )
        if cursor.rowcount != 1:
            raise TaxonomyError(f"selling point {code} does not exist in draft")


def publish_draft(*, db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        draft = _draft(connection)
        invalid = connection.execute(
            """
            SELECT sp.code FROM selling_points sp
            LEFT JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
            WHERE sp.taxonomy_id=? AND sp.enabled=1
            GROUP BY sp.id HAVING trim(sp.label)='' OR COUNT(sps.scene)=0
            """,
            (draft["id"],),
        ).fetchall()
        if invalid:
            raise TaxonomyError(
                "cannot publish selling points with blank labels or scenes: "
                + ",".join(str(row["code"]) for row in invalid)
            )
        count = int(
            connection.execute(
                "SELECT COUNT(*) FROM selling_points WHERE taxonomy_id=? AND enabled=1",
                (draft["id"],),
            ).fetchone()[0]
        )
        if count == 0:
            raise TaxonomyError("cannot publish an empty taxonomy")
        published_at = now_utc()
        connection.execute(
            "UPDATE taxonomy_versions SET status='retired' WHERE status='published'"
        )
        connection.execute(
            "UPDATE taxonomy_versions SET status='published', published_at=? WHERE id=?",
            (published_at, draft["id"]),
        )
    return {
        "id": draft["id"],
        "version": draft["version"],
        "status": "published",
        "published_at": published_at,
        "point_count": count,
    }
