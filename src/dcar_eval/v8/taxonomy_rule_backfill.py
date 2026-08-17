"""Isolated, deterministic materialization of matcher rules into a v5.1 draft.

This module deliberately does not publish the draft, create an evaluation
release, evaluate content, or generate reports.  The published v5.0 taxonomy
is a read-only clone source and remains the active legacy contract.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .matcher_dsl import (
    DEFAULT_BUNDLE_PATH,
    POINT_IDS,
    canonical_json,
    canonical_materialized_rule,
    load_bundle_bytes,
    materialize_point_rule,
    project_materialized_rule,
    taxonomy_matcher_sha256,
    validate_materialized_rule,
)
from .storage import (
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    now_utc,
    require_schema_compatibility,
    transaction,
)


LEGACY_TAXONOMY_VERSION = "selling-points-v5.0"
DRAFT_TAXONOMY_VERSION = "selling-points-v5.1"
DRAFT_TAXONOMY_ID = "taxonomy-selling-points-v5.1-rule-backfill"


class TaxonomyRuleBackfillError(RuntimeError):
    """Raised when the isolated backfill cannot proceed without ambiguity."""


@contextmanager
def _connect_read_only(path: Path) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve()}?mode=ro", uri=True, timeout=10
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        yield connection
    except sqlite3.Error as error:
        raise TaxonomyRuleBackfillError(
            f"cannot inspect database read-only: {error}"
        ) from error
    finally:
        if connection is not None:
            connection.close()


@contextmanager
def _connect_read_write(path: Path) -> Iterator[sqlite3.Connection]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve()}?mode=rw", uri=True, timeout=10
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA busy_timeout = 10000")
        yield connection
    except sqlite3.Error as error:
        raise TaxonomyRuleBackfillError(
            f"cannot update existing database read-write: {error}"
        ) from error
    finally:
        if connection is not None:
            connection.close()


def _require_current_schema(connection: sqlite3.Connection) -> None:
    try:
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
    except SchemaMigrationError as error:
        raise TaxonomyRuleBackfillError(
            f"complete schema v{SCHEMA_VERSION} is required"
        ) from error
    columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info(selling_points)")
    }
    required = {
        "matcher_rule_json",
        "positive_evidence_json",
        "negative_evidence_json",
        "boundary_rules_json",
    }
    if not required.issubset(columns):
        raise TaxonomyRuleBackfillError("selling_points is missing rule columns")


def _taxonomy_row(connection: sqlite3.Connection, version: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version=?", (version,)
    ).fetchone()


def _point_rows(
    connection: sqlite3.Connection, taxonomy_id: str
) -> dict[str, sqlite3.Row]:
    rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
        (taxonomy_id,),
    ).fetchall()
    return {str(row["code"]): row for row in rows}


def _require_exact_points(rows: Mapping[str, sqlite3.Row], *, label: str) -> None:
    actual = set(rows)
    if len(rows) != len(POINT_IDS) or actual != POINT_IDS:
        raise TaxonomyRuleBackfillError(
            f"{label} must contain exactly the 25 matcher points; "
            f"missing={sorted(POINT_IDS - actual)}, extra={sorted(actual - POINT_IDS)}"
        )


def _resolve_state(
    connection: sqlite3.Connection,
) -> tuple[sqlite3.Row, sqlite3.Row | None, dict[str, sqlite3.Row]]:
    _require_current_schema(connection)
    release = connection.execute(
        """
        SELECT id FROM evaluation_releases
        WHERE taxonomy_version=? LIMIT 1
        """,
        (DRAFT_TAXONOMY_VERSION,),
    ).fetchone()
    if release is not None:
        raise TaxonomyRuleBackfillError(
            "selling-points-v5.1 is already referenced by an evaluation release"
        )
    published = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE status='published' ORDER BY version"
    ).fetchall()
    if len(published) != 1 or str(published[0]["version"]) != LEGACY_TAXONOMY_VERSION:
        raise TaxonomyRuleBackfillError(
            "selling-points-v5.0 must be the only published taxonomy"
        )
    legacy = published[0]
    legacy_points = _point_rows(connection, str(legacy["id"]))
    _require_exact_points(legacy_points, label=LEGACY_TAXONOMY_VERSION)

    drafts = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE status='draft' ORDER BY version"
    ).fetchall()
    if drafts and (
        len(drafts) != 1 or str(drafts[0]["version"]) != DRAFT_TAXONOMY_VERSION
    ):
        raise TaxonomyRuleBackfillError(
            "the only permitted draft is selling-points-v5.1"
        )
    draft = drafts[0] if drafts else None
    existing_v5_1 = _taxonomy_row(connection, DRAFT_TAXONOMY_VERSION)
    if draft is None and existing_v5_1 is not None:
        raise TaxonomyRuleBackfillError(
            "selling-points-v5.1 exists but is not an isolated draft"
        )
    if draft is not None:
        draft_points = _point_rows(connection, str(draft["id"]))
        _require_exact_points(draft_points, label=DRAFT_TAXONOMY_VERSION)
    return legacy, draft, legacy_points


def _materialized_rules(
    bundle_path: Path,
) -> tuple[dict[str, dict[str, Any]], str, str]:
    resolved_bundle_path = bundle_path.resolve()
    try:
        payload = resolved_bundle_path.read_bytes()
    except OSError as error:
        raise TaxonomyRuleBackfillError(
            f"cannot read matcher bundle: {error}"
        ) from error
    bundle = load_bundle_bytes(payload)
    rules = {
        point_id: materialize_point_rule(bundle, point_id)
        for point_id in sorted(POINT_IDS)
    }
    if set(rules) != POINT_IDS:
        raise TaxonomyRuleBackfillError(
            "matcher bundle does not contain exactly 25 points"
        )
    try:
        source_path = resolved_bundle_path.relative_to(
            PROJECT_ROOT.resolve()
        ).as_posix()
    except ValueError:
        source_path = str(resolved_bundle_path)
    source_sha256 = hashlib.sha256(payload).hexdigest()
    return rules, source_sha256, source_path


def _projection_json(projection: Mapping[str, Any], key: str) -> str:
    return canonical_json(list(projection[key]))


def _scenes(connection: sqlite3.Connection, selling_point_id: int) -> list[str]:
    return [
        str(row["scene"])
        for row in connection.execute(
            "SELECT scene FROM selling_point_scenes WHERE selling_point_id=? ORDER BY scene",
            (selling_point_id,),
        )
    ]


def _point_needs_update(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    rule: Mapping[str, Any],
) -> bool:
    projection = project_materialized_rule(rule)
    expected_rule = canonical_materialized_rule(rule)
    stored_rule = str(row["matcher_rule_json"])
    if stored_rule != expected_rule and stored_rule.strip() != "{}":
        raise TaxonomyRuleBackfillError(
            f"draft matcher rule for {row['code']} differs from the approved "
            "backfill; refusing to overwrite non-empty rule"
        )
    return any(
        (
            stored_rule != expected_rule,
            str(row["positive_evidence_json"])
            != _projection_json(projection, "positive_evidence"),
            str(row["negative_evidence_json"])
            != _projection_json(projection, "negative_evidence"),
            str(row["boundary_rules_json"])
            != _projection_json(projection, "boundary_rules"),
            _scenes(connection, int(row["id"])) != list(projection["scenes"]),
        )
    )


def _snapshot_legacy(connection: sqlite3.Connection, legacy_id: str) -> str:
    taxonomy = dict(
        connection.execute(
            "SELECT * FROM taxonomy_versions WHERE id=?", (legacy_id,)
        ).fetchone()
    )
    points = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY id", (legacy_id,)
        )
    ]
    scenes = [
        dict(row)
        for row in connection.execute(
            """
            SELECT sps.* FROM selling_point_scenes sps
            JOIN selling_points sp ON sp.id=sps.selling_point_id
            WHERE sp.taxonomy_id=? ORDER BY sps.selling_point_id,sps.scene
            """,
            (legacy_id,),
        )
    ]
    releases = [
        dict(row)
        for row in connection.execute(
            """
            SELECT * FROM evaluation_releases
            WHERE taxonomy_version=? ORDER BY id
            """,
            (LEGACY_TAXONOMY_VERSION,),
        )
    ]
    return canonical_json(
        {
            "taxonomy": taxonomy,
            "points": points,
            "scenes": scenes,
            "releases": releases,
        }
    )


def _plan(
    connection: sqlite3.Connection,
    rules: Mapping[str, Mapping[str, Any]],
) -> tuple[sqlite3.Row, sqlite3.Row | None, dict[str, sqlite3.Row], dict[str, Any]]:
    legacy, draft, legacy_points = _resolve_state(connection)
    if draft is None:
        summary = {
            "created_draft": True,
            "created_points": len(rules),
            "updated_points": 0,
            "unchanged_points": 0,
        }
    else:
        draft_points = _point_rows(connection, str(draft["id"]))
        changed = sum(
            _point_needs_update(connection, draft_points[code], rule)
            for code, rule in rules.items()
        )
        summary = {
            "created_draft": False,
            "created_points": 0,
            "updated_points": changed,
            "unchanged_points": len(rules) - changed,
        }
    return legacy, draft, legacy_points, summary


def _insert_point(
    connection: sqlite3.Connection,
    *,
    draft_id: str,
    source: sqlite3.Row,
    rule: Mapping[str, Any],
) -> None:
    projection = project_materialized_rule(rule)
    cursor = connection.execute(
        """
        INSERT INTO selling_points(
            taxonomy_id,code,tier,label,definition,positive_evidence_json,
            negative_evidence_json,boundary_rules_json,matcher_rule_json,enabled
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        (
            draft_id,
            source["code"],
            source["tier"],
            source["label"],
            source["definition"],
            _projection_json(projection, "positive_evidence"),
            _projection_json(projection, "negative_evidence"),
            _projection_json(projection, "boundary_rules"),
            canonical_materialized_rule(rule),
            source["enabled"],
        ),
    )
    if cursor.lastrowid is None:
        raise TaxonomyRuleBackfillError("selling point insert returned no id")
    for scene in projection["scenes"]:
        connection.execute(
            "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
            (cursor.lastrowid, scene),
        )


def _update_point(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
    rule: Mapping[str, Any],
) -> None:
    projection = project_materialized_rule(rule)
    connection.execute(
        """
        UPDATE selling_points
        SET positive_evidence_json=?,negative_evidence_json=?,
            boundary_rules_json=?,matcher_rule_json=?
        WHERE id=?
        """,
        (
            _projection_json(projection, "positive_evidence"),
            _projection_json(projection, "negative_evidence"),
            _projection_json(projection, "boundary_rules"),
            canonical_materialized_rule(rule),
            row["id"],
        ),
    )
    connection.execute(
        "DELETE FROM selling_point_scenes WHERE selling_point_id=?", (row["id"],)
    )
    for scene in projection["scenes"]:
        connection.execute(
            "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
            (row["id"], scene),
        )


def _verify_draft(
    connection: sqlite3.Connection,
    *,
    draft_id: str,
    expected_hash: str,
    expected_source_path: str,
    expected_source_sha256: str,
) -> None:
    taxonomy = connection.execute(
        "SELECT source_path,source_sha256 FROM taxonomy_versions WHERE id=?",
        (draft_id,),
    ).fetchone()
    if taxonomy is None or (
        str(taxonomy["source_path"]) != expected_source_path
        or str(taxonomy["source_sha256"]) != expected_source_sha256
    ):
        raise TaxonomyRuleBackfillError("draft matcher source metadata is inconsistent")
    rows = _point_rows(connection, draft_id)
    _require_exact_points(rows, label=DRAFT_TAXONOMY_VERSION)
    stored_rules: dict[str, dict[str, Any]] = {}
    for code, row in rows.items():
        try:
            rule = json.loads(str(row["matcher_rule_json"]))
        except (json.JSONDecodeError, ValueError) as error:
            raise TaxonomyRuleBackfillError(
                f"stored matcher rule for {code} is invalid JSON"
            ) from error
        if not isinstance(rule, dict):
            raise TaxonomyRuleBackfillError(
                f"stored matcher rule for {code} is not an object"
            )
        validate_materialized_rule(rule)
        if str(row["matcher_rule_json"]) != canonical_materialized_rule(rule):
            raise TaxonomyRuleBackfillError(
                f"stored matcher rule for {code} is not canonical"
            )
        projection = project_materialized_rule(rule)
        if any(
            (
                str(row["positive_evidence_json"])
                != _projection_json(projection, "positive_evidence"),
                str(row["negative_evidence_json"])
                != _projection_json(projection, "negative_evidence"),
                str(row["boundary_rules_json"])
                != _projection_json(projection, "boundary_rules"),
                _scenes(connection, int(row["id"])) != list(projection["scenes"]),
            )
        ):
            raise TaxonomyRuleBackfillError(
                f"stored projection for {code} is inconsistent"
            )
        stored_rules[code] = rule
    if taxonomy_matcher_sha256(stored_rules) != expected_hash:
        raise TaxonomyRuleBackfillError("stored taxonomy matcher hash is inconsistent")


def backfill_v5_1_matcher_rules(
    *,
    db_path: Path,
    bundle_path: Path = DEFAULT_BUNDLE_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Create or repair only the isolated v5.1 taxonomy draft."""

    rules, source_sha256, source_path = _materialized_rules(bundle_path)
    matcher_sha256 = taxonomy_matcher_sha256(rules)
    if dry_run:
        with _connect_read_only(db_path) as connection:
            _, draft, _, summary = _plan(connection, rules)
        return {
            "dry_run": True,
            "taxonomy_version": DRAFT_TAXONOMY_VERSION,
            "taxonomy_id": str(draft["id"]) if draft is not None else DRAFT_TAXONOMY_ID,
            "matcher_rule_sha256": matcher_sha256,
            **summary,
        }

    with _connect_read_write(db_path) as connection, transaction(connection):
        legacy, draft, legacy_points, summary = _plan(connection, rules)
        legacy_snapshot = _snapshot_legacy(connection, str(legacy["id"]))
        if draft is None:
            if (
                connection.execute(
                    "SELECT 1 FROM taxonomy_versions WHERE id=?",
                    (DRAFT_TAXONOMY_ID,),
                ).fetchone()
                is not None
            ):
                raise TaxonomyRuleBackfillError(
                    f"reserved draft id already exists: {DRAFT_TAXONOMY_ID}"
                )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,source_path,source_sha256,created_at
                ) VALUES (?,?,'draft',?,?,?,?)
                """,
                (
                    DRAFT_TAXONOMY_ID,
                    DRAFT_TAXONOMY_VERSION,
                    legacy["definition"],
                    source_path,
                    source_sha256,
                    now_utc(),
                ),
            )
            draft_id = DRAFT_TAXONOMY_ID
            for code, rule in rules.items():
                _insert_point(
                    connection,
                    draft_id=draft_id,
                    source=legacy_points[code],
                    rule=rule,
                )
        else:
            draft_id = str(draft["id"])
            connection.execute(
                """
                UPDATE taxonomy_versions
                SET source_path=?,source_sha256=?
                WHERE id=? AND (
                    source_path IS NULL OR source_path<>? OR
                    source_sha256 IS NULL OR source_sha256<>?
                )
                """,
                (
                    source_path,
                    source_sha256,
                    draft_id,
                    source_path,
                    source_sha256,
                ),
            )
            draft_points = _point_rows(connection, draft_id)
            for code, rule in rules.items():
                row = draft_points[code]
                if _point_needs_update(connection, row, rule):
                    _update_point(connection, row, rule)
        _verify_draft(
            connection,
            draft_id=draft_id,
            expected_hash=matcher_sha256,
            expected_source_path=source_path,
            expected_source_sha256=source_sha256,
        )
        if _snapshot_legacy(connection, str(legacy["id"])) != legacy_snapshot:
            raise TaxonomyRuleBackfillError("legacy v5.0 data changed during backfill")
    return {
        "dry_run": False,
        "taxonomy_version": DRAFT_TAXONOMY_VERSION,
        "taxonomy_id": draft_id,
        "matcher_rule_sha256": matcher_sha256,
        **summary,
    }
