"""Versioned selling-point taxonomy authoring.

``matcher_rule`` is the only writable source of matching behaviour.  The
human-readable evidence lists and scene rows are checked projections of that
rule, never independent authoring inputs.
"""

from __future__ import annotations

import json
import re
import sqlite3
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .matcher_dsl import (
    MatcherDslError,
    canonical_json,
    canonical_materialized_rule,
    project_materialized_rule,
    validate_materialized_rule,
)
from .storage import DEFAULT_DB, connect, now_utc, transaction


class TaxonomyError(RuntimeError):
    """Raised when persisted taxonomy state makes an operation unsafe."""


class TaxonomyValidationError(TaxonomyError):
    """Raised when an authoring payload does not satisfy the rule contract."""


TIERS = {"core", "other"}
CODE_RE = re.compile(r"^[A-Z][1-9][0-9]?$", re.ASCII)
VERSION_RE = re.compile(r"^selling-points-v(\d+)\.(\d+)$")
LEGACY_TAXONOMY_VERSION = "selling-points-v5.0"
_POINT_INPUT_KEYS = {"code", "tier", "label", "definition", "matcher_rule"}


def _next_version(current: str) -> str:
    match = VERSION_RE.fullmatch(current)
    if match is None:
        raise TaxonomyError(f"unsupported taxonomy version: {current}")
    return f"selling-points-v{int(match.group(1))}.{int(match.group(2)) + 1}"


def _normalized_code(value: Any) -> str:
    code = str(value or "").strip().upper()
    if not CODE_RE.fullmatch(code):
        raise TaxonomyValidationError(
            "selling point code must be one letter plus 1-2 digits"
        )
    return code


def _validate_point_input(
    value: Mapping[str, Any], *, expected_code: str | None
) -> dict[str, Any]:
    unknown = set(value) - _POINT_INPUT_KEYS
    if unknown:
        raise TaxonomyValidationError(
            f"unknown selling point fields: {sorted(unknown)}"
        )

    if expected_code is None:
        code = _normalized_code(value.get("code"))
    else:
        code = _normalized_code(expected_code)
        if "code" in value and value["code"] is not None:
            supplied_code = _normalized_code(value["code"])
            if supplied_code != code:
                raise TaxonomyValidationError(
                    f"payload code {supplied_code} does not match path code {code}"
                )

    label = str(value.get("label") or "").strip()
    if not label:
        raise TaxonomyValidationError("selling point label is required")
    tier = str(value.get("tier") or "")
    if tier not in TIERS:
        raise TaxonomyValidationError("selling point tier must be core or other")

    matcher_rule = value.get("matcher_rule")
    if not isinstance(matcher_rule, Mapping):
        raise TaxonomyValidationError("matcher_rule must be an object")
    try:
        validate_materialized_rule(matcher_rule)
        canonical_rule = canonical_materialized_rule(matcher_rule)
        canonical_value = json.loads(canonical_rule)
        projection = project_materialized_rule(canonical_value)
    except (MatcherDslError, TypeError, ValueError) as error:
        raise TaxonomyValidationError(f"invalid matcher_rule: {error}") from error
    point_id = str(canonical_value["rule"]["point_id"])
    if point_id != code:
        raise TaxonomyValidationError(
            f"selling point code {code} does not match matcher rule point_id {point_id}"
        )

    return {
        "code": code,
        "tier": tier,
        "label": label,
        "definition": str(value.get("definition") or "").strip(),
        "matcher_rule": canonical_value,
        "matcher_rule_json": canonical_rule,
        **projection,
    }


def _taxonomy_rows(connection: sqlite3.Connection, status: str) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT * FROM taxonomy_versions WHERE status=?
        ORDER BY created_at, id
        """,
        (status,),
    ).fetchall()


def _single_taxonomy(
    connection: sqlite3.Connection, status: str, *, required: bool
) -> sqlite3.Row | None:
    rows = _taxonomy_rows(connection, status)
    if len(rows) > 1:
        raise TaxonomyError(f"multiple {status} taxonomies exist")
    if not rows:
        if required:
            raise TaxonomyError(f"no {status} taxonomy exists")
        return None
    return rows[0]


def _release_reference(
    connection: sqlite3.Connection, taxonomy_version: str
) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT id FROM evaluation_releases
        WHERE taxonomy_version=? ORDER BY created_at, id LIMIT 1
        """,
        (taxonomy_version,),
    ).fetchone()


def _require_unfrozen_draft(
    connection: sqlite3.Connection,
) -> sqlite3.Row:
    draft = _single_taxonomy(connection, "draft", required=True)
    assert draft is not None
    release = _release_reference(connection, str(draft["version"]))
    if release is not None:
        raise TaxonomyError(
            f"draft {draft['version']} is immutable because evaluation release "
            f"{release['id']} references it"
        )
    return draft


def _decoded_list(value: Any, *, label: str) -> list[str]:
    try:
        decoded = json.loads(str(value))
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise TaxonomyError(f"stored {label} is not valid JSON") from error
    if not isinstance(decoded, list) or any(
        not isinstance(item, str) for item in decoded
    ):
        raise TaxonomyError(f"stored {label} must be a string list")
    return decoded


def _stored_scenes(connection: sqlite3.Connection, selling_point_id: int) -> list[str]:
    return [
        str(row["scene"])
        for row in connection.execute(
            """
            SELECT scene FROM selling_point_scenes
            WHERE selling_point_id=? ORDER BY scene
            """,
            (selling_point_id,),
        )
    ]


def serialize_point_row(
    connection: sqlite3.Connection,
    taxonomy: sqlite3.Row,
    row: sqlite3.Row,
) -> dict[str, Any]:
    """Validate and serialize one stored selling point.

    Callers with joined rows may pass them as long as all ``selling_points``
    columns remain available.  Hit counters and other read-model fields should
    be merged into this checked result by the caller.
    """

    code = str(row["code"])
    scenes = _stored_scenes(connection, int(row["id"]))
    positive = _decoded_list(
        row["positive_evidence_json"], label=f"positive evidence for {code}"
    )
    negative = _decoded_list(
        row["negative_evidence_json"], label=f"negative evidence for {code}"
    )
    boundary = _decoded_list(
        row["boundary_rules_json"], label=f"boundary rules for {code}"
    )
    stored_matcher_rule_json = str(row["matcher_rule_json"])
    try:
        matcher_rule = json.loads(stored_matcher_rule_json)
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise TaxonomyError(
            f"stored matcher rule for {code} is not valid JSON"
        ) from error

    if matcher_rule == {}:
        if not (
            str(taxonomy["status"]) == "published"
            and str(taxonomy["version"]) == LEGACY_TAXONOMY_VERSION
        ):
            raise TaxonomyError(f"taxonomy point {code} has no matcher rule")
        return {
            "code": code,
            "tier": row["tier"],
            "label": row["label"],
            "definition": row["definition"],
            "matcher_rule": None,
            "positive_evidence": positive,
            "negative_evidence": negative,
            "boundary_rules": boundary,
            "scenes": scenes,
        }
    if not isinstance(matcher_rule, Mapping):
        raise TaxonomyError(f"stored matcher rule for {code} must be an object")
    try:
        validate_materialized_rule(matcher_rule)
        point_id = str(matcher_rule["rule"]["point_id"])
        projection = project_materialized_rule(matcher_rule)
        canonical_text = canonical_materialized_rule(matcher_rule)
        canonical_rule = json.loads(canonical_text)
    except (MatcherDslError, TypeError, ValueError) as error:
        raise TaxonomyError(
            f"stored matcher rule for {code} is invalid: {error}"
        ) from error
    if point_id != code:
        raise TaxonomyError(
            f"taxonomy point {code} does not match matcher rule point_id {point_id}"
        )
    if stored_matcher_rule_json != canonical_text:
        raise TaxonomyError(f"stored matcher rule for {code} is not canonical")
    stored_projection = {
        "positive_evidence": positive,
        "negative_evidence": negative,
        "boundary_rules": boundary,
        "scenes": scenes,
    }
    if stored_projection != projection:
        raise TaxonomyError(f"stored projection for {code} does not match matcher rule")
    return {
        "code": code,
        "tier": row["tier"],
        "label": row["label"],
        "definition": row["definition"],
        "matcher_rule": canonical_rule,
        **projection,
    }


def _point_rows(
    connection: sqlite3.Connection,
    taxonomy_id: str,
    *,
    enabled_only: bool,
) -> list[sqlite3.Row]:
    enabled_clause = "AND enabled=1" if enabled_only else ""
    return connection.execute(
        f"""
        SELECT * FROM selling_points
        WHERE taxonomy_id=? {enabled_clause}
        ORDER BY substr(code, 1, 1), CAST(substr(code, 2) AS INTEGER), code
        """,
        (taxonomy_id,),
    ).fetchall()


def _taxonomy_summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "version": row["version"],
        "status": row["status"],
        "created_at": row["created_at"],
        "published_at": row["published_at"],
    }


def ensure_draft(*, db_path: Path = DEFAULT_DB) -> dict[str, Any]:
    with connect(db_path) as connection, transaction(connection):
        drafts = _taxonomy_rows(connection, "draft")
        if len(drafts) > 1:
            raise TaxonomyError("multiple draft taxonomies exist")
        if drafts:
            draft = _require_unfrozen_draft(connection)
            for row in _point_rows(connection, str(draft["id"]), enabled_only=False):
                serialize_point_row(connection, draft, row)
            return _taxonomy_summary(draft)

        published = _single_taxonomy(connection, "published", required=True)
        assert published is not None
        source_rows = _point_rows(connection, str(published["id"]), enabled_only=False)
        if not source_rows:
            raise TaxonomyError("cannot clone an empty published taxonomy")
        source_points = [
            (row, serialize_point_row(connection, published, row))
            for row in source_rows
        ]
        if any(point["matcher_rule"] is None for _, point in source_points):
            raise TaxonomyError(
                "published legacy taxonomy has no complete matcher rules to clone"
            )

        version = _next_version(str(published["version"]))
        version_conflict = connection.execute(
            "SELECT id,status FROM taxonomy_versions WHERE version=?",
            (version,),
        ).fetchone()
        if version_conflict is not None:
            raise TaxonomyError(
                f"next taxonomy version {version} already exists with status "
                f"{version_conflict['status']}"
            )
        taxonomy_id = f"taxonomy-{uuid.uuid4().hex}"
        captured_at = now_utc()
        connection.execute(
            """
            INSERT INTO taxonomy_versions(
                id, version, status, definition, source_path, source_sha256, created_at
            ) VALUES (?, ?, 'draft', ?, ?, ?, ?)
            """,
            (
                taxonomy_id,
                version,
                published["definition"],
                published["source_path"],
                published["source_sha256"],
                captured_at,
            ),
        )
        for source_row, point in source_points:
            matcher_rule = point["matcher_rule"]
            assert isinstance(matcher_rule, Mapping)
            cursor = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, definition,
                    positive_evidence_json, negative_evidence_json,
                    boundary_rules_json, matcher_rule_json, enabled
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    taxonomy_id,
                    point["code"],
                    point["tier"],
                    point["label"],
                    point["definition"],
                    canonical_json(point["positive_evidence"]),
                    canonical_json(point["negative_evidence"]),
                    canonical_json(point["boundary_rules"]),
                    canonical_materialized_rule(matcher_rule),
                    source_row["enabled"],
                ),
            )
            if cursor.lastrowid is None:
                raise RuntimeError("selling point clone returned no id")
            _replace_scenes(connection, int(cursor.lastrowid), point["scenes"])

        draft = connection.execute(
            "SELECT * FROM taxonomy_versions WHERE id=?", (taxonomy_id,)
        ).fetchone()
        assert draft is not None
        return _taxonomy_summary(draft)


def list_points(
    *, status: str = "published", db_path: Path = DEFAULT_DB
) -> dict[str, Any]:
    if status not in {"published", "draft"}:
        raise TaxonomyValidationError("taxonomy status must be published or draft")
    with connect(db_path) as connection:
        taxonomy = _single_taxonomy(connection, status, required=False)
        if taxonomy is None:
            return {"taxonomy": None, "items": []}
        items = [
            serialize_point_row(connection, taxonomy, row)
            for row in _point_rows(connection, str(taxonomy["id"]), enabled_only=True)
        ]
    return {"taxonomy": _taxonomy_summary(taxonomy), "items": items}


def _replace_scenes(
    connection: sqlite3.Connection,
    selling_point_id: int,
    scenes: list[str],
) -> None:
    connection.execute(
        "DELETE FROM selling_point_scenes WHERE selling_point_id=?",
        (selling_point_id,),
    )
    for scene in scenes:
        connection.execute(
            "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, ?)",
            (selling_point_id, scene),
        )


def _insert_point(
    connection: sqlite3.Connection,
    taxonomy_id: str,
    point: Mapping[str, Any],
) -> None:
    cursor = connection.execute(
        """
        INSERT INTO selling_points(
            taxonomy_id, code, tier, label, definition, positive_evidence_json,
            negative_evidence_json, boundary_rules_json, matcher_rule_json, enabled
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            taxonomy_id,
            point["code"],
            point["tier"],
            point["label"],
            point["definition"],
            canonical_json(point["positive_evidence"]),
            canonical_json(point["negative_evidence"]),
            canonical_json(point["boundary_rules"]),
            point["matcher_rule_json"],
        ),
    )
    if cursor.lastrowid is None:
        raise RuntimeError("selling point insert returned no id")
    _replace_scenes(connection, int(cursor.lastrowid), point["scenes"])


def create_point(
    value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB
) -> dict[str, Any]:
    point = _validate_point_input(value, expected_code=None)
    with connect(db_path) as connection, transaction(connection):
        draft = _require_unfrozen_draft(connection)
        existing = connection.execute(
            "SELECT 1 FROM selling_points WHERE taxonomy_id=? AND code=?",
            (draft["id"], point["code"]),
        ).fetchone()
        if existing is not None:
            raise TaxonomyError(
                f"selling point {point['code']} already exists in draft"
            )
        _insert_point(connection, str(draft["id"]), point)
    return _public_point(point)


def _public_point(point: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "code": point["code"],
        "tier": point["tier"],
        "label": point["label"],
        "definition": point["definition"],
        "matcher_rule": point["matcher_rule"],
        "positive_evidence": point["positive_evidence"],
        "negative_evidence": point["negative_evidence"],
        "boundary_rules": point["boundary_rules"],
        "scenes": point["scenes"],
    }


def update_point(
    code: str, value: Mapping[str, Any], *, db_path: Path = DEFAULT_DB
) -> dict[str, Any]:
    point = _validate_point_input(value, expected_code=code)
    with connect(db_path) as connection, transaction(connection):
        draft = _require_unfrozen_draft(connection)
        row = connection.execute(
            "SELECT id FROM selling_points WHERE taxonomy_id=? AND code=?",
            (draft["id"], point["code"]),
        ).fetchone()
        if row is None:
            raise TaxonomyError(
                f"selling point {point['code']} does not exist in draft"
            )
        connection.execute(
            """
            UPDATE selling_points SET tier=?, label=?, definition=?,
                positive_evidence_json=?, negative_evidence_json=?,
                boundary_rules_json=?, matcher_rule_json=?
            WHERE id=?
            """,
            (
                point["tier"],
                point["label"],
                point["definition"],
                canonical_json(point["positive_evidence"]),
                canonical_json(point["negative_evidence"]),
                canonical_json(point["boundary_rules"]),
                point["matcher_rule_json"],
                row["id"],
            ),
        )
        _replace_scenes(connection, int(row["id"]), point["scenes"])
    return _public_point(point)


def delete_point(code: str, *, db_path: Path = DEFAULT_DB) -> None:
    normalized_code = _normalized_code(code)
    with connect(db_path) as connection, transaction(connection):
        draft = _require_unfrozen_draft(connection)
        cursor = connection.execute(
            "DELETE FROM selling_points WHERE taxonomy_id=? AND code=?",
            (draft["id"], normalized_code),
        )
        if cursor.rowcount != 1:
            raise TaxonomyError(
                f"selling point {normalized_code} does not exist in draft"
            )


def publish_draft(*, db_path: Path = DEFAULT_DB) -> dict[str, Any]:
    """Reject standalone publication until release activation owns the transaction."""

    with connect(db_path) as connection:
        _require_unfrozen_draft(connection)
    raise TaxonomyError(
        "taxonomy publication requires atomic evaluation release activation; "
        "standalone publication is disabled"
    )
