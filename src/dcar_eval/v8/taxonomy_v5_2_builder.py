"""Build the isolated selling-points-v5.2 draft without mutating v5.1.

The v5.1 taxonomy and matcher bundle are historical release inputs.  This
builder therefore creates a new draft from an exact published v5.1 base and
never repairs either the base or an unexpected target draft in place.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .matcher_dsl import (
    V4_BUNDLE_PATH,
    V5_1_POINT_SPEC,
    V5_2_POINT_SPEC,
    MatcherDslError,
    canonical_json,
    canonical_materialized_rule,
    load_bundle_bytes,
    materialize_point_rule,
    project_materialized_rule,
    taxonomy_matcher_sha256,
    validate_materialized_rule,
)
from .storage import (
    DEFAULT_DB,
    PROJECT_ROOT,
    SCHEMA_VERSION,
    SchemaMigrationError,
    configure_connection_safety,
    now_utc,
    require_schema_compatibility,
    transaction,
)


BASE_TAXONOMY_VERSION = "selling-points-v5.1"
TARGET_TAXONOMY_VERSION = "selling-points-v5.2"
TARGET_TAXONOMY_ID = "taxonomy-selling-points-v5.2"
DEFAULT_BUSINESS_SOURCE_PATH = (
    PROJECT_ROOT / "config" / "business_selling_points_v5_2.json"
)
BUILDER_CONTRACT = "selling-points-v5.2-isolated-draft-v1"
REMOVED_POINT_IDS = frozenset({"C1", "C2", "C3", "C4"})
RETAINED_POINT_IDS = frozenset(V5_1_POINT_SPEC) - REMOVED_POINT_IDS
NEW_POINT_IDS = frozenset(V5_2_POINT_SPEC) - RETAINED_POINT_IDS
SCENE_NAMES = {"二手车": "used_car", "新车": "new_car", "媒体-AI小懂": "media"}


class TaxonomyV52BuilderError(RuntimeError):
    """Raised when an isolated v5.2 draft cannot be proven safe."""


@contextmanager
def _connect(path: Path, *, read_only: bool) -> Iterator[sqlite3.Connection]:
    mode = "ro" if read_only else "rw"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(
            f"file:{path.resolve()}?mode={mode}", uri=True, timeout=10
        )
        connection.row_factory = sqlite3.Row
        configure_connection_safety(connection)
        connection.execute("PRAGMA busy_timeout=10000")
        if read_only:
            connection.execute("PRAGMA query_only=ON")
        yield connection
    except sqlite3.Error as error:
        operation = "inspect" if read_only else "update"
        raise TaxonomyV52BuilderError(
            f"cannot {operation} existing database: {error}"
        ) from error
    finally:
        if connection is not None:
            connection.close()


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _source_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return resolved.relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(resolved)


def _read_bytes(path: Path, *, label: str) -> bytes:
    try:
        return path.resolve().read_bytes()
    except OSError as error:
        raise TaxonomyV52BuilderError(f"cannot read {label}: {error}") from error


def _business_points(
    source_path: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any], str, str]:
    payload = _read_bytes(source_path, label="v5.2 business source")
    try:
        source = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise TaxonomyV52BuilderError(
            f"v5.2 business source is invalid JSON: {error}"
        ) from error
    if not isinstance(source, dict):
        raise TaxonomyV52BuilderError("v5.2 business source must be an object")
    required = {
        "database_taxonomy_version",
        "base_database_taxonomy_version",
        "definition",
        "core_ids",
        "retained_ids",
        "removed_ids",
        "new_ids",
        "labels",
    }
    missing = required - set(source)
    if missing:
        raise TaxonomyV52BuilderError(
            f"v5.2 business source is missing fields: {sorted(missing)}"
        )
    if source["database_taxonomy_version"] != TARGET_TAXONOMY_VERSION:
        raise TaxonomyV52BuilderError("business source target version is not v5.2")
    if source["base_database_taxonomy_version"] != BASE_TAXONOMY_VERSION:
        raise TaxonomyV52BuilderError("business source base version is not v5.1")
    if set(source["retained_ids"]) != RETAINED_POINT_IDS:
        raise TaxonomyV52BuilderError("business source retained point set is invalid")
    if set(source["removed_ids"]) != REMOVED_POINT_IDS:
        raise TaxonomyV52BuilderError("business source removed point set is invalid")
    if set(source["new_ids"]) != NEW_POINT_IDS:
        raise TaxonomyV52BuilderError("business source new point set is invalid")
    labels = source["labels"]
    if not isinstance(labels, list):
        raise TaxonomyV52BuilderError("business source labels must be a list")
    points: dict[str, dict[str, Any]] = {}
    allowed_keys = {"id", "tier", "label", "definition", "business_scene"}
    for index, value in enumerate(labels):
        if not isinstance(value, dict) or set(value) != allowed_keys:
            raise TaxonomyV52BuilderError(
                f"business source label {index} must contain exactly {sorted(allowed_keys)}"
            )
        code = str(value["id"])
        if code in points:
            raise TaxonomyV52BuilderError(f"duplicate business point: {code}")
        tier = str(value["tier"])
        label = str(value["label"])
        definition = str(value["definition"])
        scene = SCENE_NAMES.get(str(value["business_scene"]))
        if tier not in {"core", "other"} or not label or scene is None:
            raise TaxonomyV52BuilderError(f"invalid business metadata for {code}")
        if {scene} != set(V5_2_POINT_SPEC.get(code, set())):
            raise TaxonomyV52BuilderError(
                f"business scene for {code} does not match the v5.2 point contract"
            )
        points[code] = {
            "code": code,
            "tier": tier,
            "label": label,
            "definition": definition,
            "scenes": [scene],
        }
    if set(points) != set(V5_2_POINT_SPEC):
        raise TaxonomyV52BuilderError(
            "business source labels do not match the v5.2 point set"
        )
    core_ids = {code for code, point in points.items() if point["tier"] == "core"}
    if set(source["core_ids"]) != core_ids:
        raise TaxonomyV52BuilderError("business source core_ids do not match tiers")
    if any(
        points[code]["tier"] != "other" or points[code]["definition"] != ""
        for code in NEW_POINT_IDS
    ):
        raise TaxonomyV52BuilderError(
            "new v5.2 points must use tier other and an empty definition"
        )
    return points, source, _sha256(payload), _source_path(source_path)


def _matcher_rules(
    bundle_path: Path,
) -> tuple[dict[str, dict[str, Any]], str, str, str]:
    payload = _read_bytes(bundle_path, label="v5.2 matcher bundle")
    try:
        bundle = load_bundle_bytes(payload, point_spec=V5_2_POINT_SPEC)
        rules = {
            code: materialize_point_rule(
                bundle, code, point_spec=V5_2_POINT_SPEC
            )
            for code in sorted(V5_2_POINT_SPEC)
        }
        matcher_sha256 = taxonomy_matcher_sha256(
            rules, point_spec=V5_2_POINT_SPEC
        )
    except MatcherDslError as error:
        raise TaxonomyV52BuilderError(
            f"v5.2 matcher bundle is invalid: {error}"
        ) from error
    return rules, matcher_sha256, _sha256(payload), _source_path(bundle_path)


def _require_schema(connection: sqlite3.Connection) -> None:
    try:
        require_schema_compatibility(
            connection, supported_versions=frozenset({SCHEMA_VERSION})
        )
    except SchemaMigrationError as error:
        raise TaxonomyV52BuilderError(
            f"complete schema v{SCHEMA_VERSION} is required"
        ) from error
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        raise TaxonomyV52BuilderError("database has foreign-key violations")


def _point_rows(
    connection: sqlite3.Connection, taxonomy_id: str
) -> dict[str, sqlite3.Row]:
    rows = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
        (taxonomy_id,),
    ).fetchall()
    points = {str(row["code"]): row for row in rows}
    if len(points) != len(rows):
        raise TaxonomyV52BuilderError("taxonomy contains duplicate point codes")
    return points


def _scenes(connection: sqlite3.Connection, selling_point_id: int) -> list[str]:
    return [
        str(row["scene"])
        for row in connection.execute(
            "SELECT scene FROM selling_point_scenes WHERE selling_point_id=? ORDER BY scene",
            (selling_point_id,),
        )
    ]


def _projection_json(projection: Mapping[str, Any], key: str) -> str:
    return canonical_json(list(projection[key]))


def _taxonomy_snapshot(
    connection: sqlite3.Connection, taxonomy_id: str, taxonomy_version: str
) -> str:
    taxonomy = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE id=?", (taxonomy_id,)
    ).fetchone()
    if taxonomy is None:
        raise TaxonomyV52BuilderError(f"taxonomy does not exist: {taxonomy_id}")
    points = connection.execute(
        "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY id",
        (taxonomy_id,),
    ).fetchall()
    scenes = connection.execute(
        """
        SELECT sps.* FROM selling_point_scenes sps
        JOIN selling_points sp ON sp.id=sps.selling_point_id
        WHERE sp.taxonomy_id=? ORDER BY sps.selling_point_id,sps.scene
        """,
        (taxonomy_id,),
    ).fetchall()
    releases = connection.execute(
        "SELECT * FROM evaluation_releases WHERE taxonomy_version=? ORDER BY id",
        (taxonomy_version,),
    ).fetchall()
    return canonical_json(
        {
            "taxonomy": dict(taxonomy),
            "points": [dict(row) for row in points],
            "scenes": [dict(row) for row in scenes],
            "releases": [dict(row) for row in releases],
        }
    )


def _validated_base(
    connection: sqlite3.Connection,
    business_points: Mapping[str, Mapping[str, Any]],
    target_rules: Mapping[str, Mapping[str, Any]],
) -> tuple[sqlite3.Row, dict[str, sqlite3.Row], str]:
    published = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE status='published' ORDER BY version,id"
    ).fetchall()
    if len(published) != 1 or str(published[0]["version"]) != BASE_TAXONOMY_VERSION:
        raise TaxonomyV52BuilderError(
            "selling-points-v5.1 must be the only published taxonomy"
        )
    base = published[0]
    rows = _point_rows(connection, str(base["id"]))
    if set(rows) != set(V5_1_POINT_SPEC):
        raise TaxonomyV52BuilderError("published v5.1 point set is not exact")
    for code, row in rows.items():
        if int(row["enabled"]) != 1:
            raise TaxonomyV52BuilderError(f"published v5.1 point {code} is disabled")
        try:
            rule = json.loads(str(row["matcher_rule_json"]))
            if not isinstance(rule, dict):
                raise TypeError("matcher rule is not an object")
            validate_materialized_rule(rule, point_spec=V5_1_POINT_SPEC)
            canonical_rule = canonical_materialized_rule(
                rule, point_spec=V5_1_POINT_SPEC
            )
            projection = project_materialized_rule(rule, point_spec=V5_1_POINT_SPEC)
        except (MatcherDslError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise TaxonomyV52BuilderError(
                f"published v5.1 point {code} is invalid: {error}"
            ) from error
        if str(row["matcher_rule_json"]) != canonical_rule or any(
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
            raise TaxonomyV52BuilderError(
                f"published v5.1 point {code} has inconsistent rule projections"
            )
        if code in RETAINED_POINT_IDS:
            source = business_points[code]
            if any(
                (
                    str(row["tier"]) != source["tier"],
                    str(row["label"]) != source["label"],
                    str(row["definition"]) != source["definition"],
                    _scenes(connection, int(row["id"])) != source["scenes"],
                )
            ):
                raise TaxonomyV52BuilderError(
                    f"business source changes retained point metadata: {code}"
                )
            target_rule = canonical_materialized_rule(
                target_rules[code], point_spec=V5_2_POINT_SPEC
            )
            target_behavior = json.loads(target_rule)
            source_behavior = json.loads(canonical_rule)
            target_behavior["rule"].pop("explain", None)
            source_behavior["rule"].pop("explain", None)
            if canonical_json(target_behavior) != canonical_json(source_behavior):
                raise TaxonomyV52BuilderError(
                    f"v5.2 bundle changes retained matcher behavior: {code}"
                )
    snapshot = _taxonomy_snapshot(
        connection, str(base["id"]), BASE_TAXONOMY_VERSION
    )
    return base, rows, snapshot


def _verify_target(
    connection: sqlite3.Connection,
    *,
    target: sqlite3.Row,
    business_points: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Mapping[str, Any]],
    source_path: str,
    source_sha256: str,
    expected_definition: str,
    expected_matcher_sha256: str,
) -> None:
    if any(
        (
            str(target["id"]) != TARGET_TAXONOMY_ID,
            str(target["version"]) != TARGET_TAXONOMY_VERSION,
            str(target["status"]) != "draft",
            str(target["definition"] or "") != expected_definition,
            str(target["source_path"] or "") != source_path,
            str(target["source_sha256"] or "") != source_sha256,
            target["published_at"] is not None,
        )
    ):
        raise TaxonomyV52BuilderError("existing v5.2 draft metadata is inconsistent")
    rows = _point_rows(connection, str(target["id"]))
    if set(rows) != set(V5_2_POINT_SPEC):
        raise TaxonomyV52BuilderError("v5.2 draft point set is not exact")
    stored_rules: dict[str, dict[str, Any]] = {}
    for code, row in rows.items():
        source = business_points[code]
        expected_rule = canonical_materialized_rule(
            rules[code], point_spec=V5_2_POINT_SPEC
        )
        try:
            stored_rule = json.loads(str(row["matcher_rule_json"]))
            if not isinstance(stored_rule, dict):
                raise TypeError("matcher rule is not an object")
            validate_materialized_rule(stored_rule, point_spec=V5_2_POINT_SPEC)
            projection = project_materialized_rule(
                stored_rule, point_spec=V5_2_POINT_SPEC
            )
        except (MatcherDslError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise TaxonomyV52BuilderError(
                f"v5.2 draft point {code} is invalid: {error}"
            ) from error
        if any(
            (
                int(row["enabled"]) != 1,
                str(row["tier"]) != source["tier"],
                str(row["label"]) != source["label"],
                str(row["definition"]) != source["definition"],
                str(row["matcher_rule_json"]) != expected_rule,
                str(row["positive_evidence_json"])
                != _projection_json(projection, "positive_evidence"),
                str(row["negative_evidence_json"])
                != _projection_json(projection, "negative_evidence"),
                str(row["boundary_rules_json"])
                != _projection_json(projection, "boundary_rules"),
                _scenes(connection, int(row["id"])) != source["scenes"],
                list(projection["scenes"]) != source["scenes"],
            )
        ):
            raise TaxonomyV52BuilderError(
                f"v5.2 draft point {code} differs from its checked source"
            )
        stored_rules[code] = stored_rule
    if (
        taxonomy_matcher_sha256(stored_rules, point_spec=V5_2_POINT_SPEC)
        != expected_matcher_sha256
    ):
        raise TaxonomyV52BuilderError("v5.2 draft matcher hash is inconsistent")


def _plan(
    connection: sqlite3.Connection,
    *,
    business_points: Mapping[str, Mapping[str, Any]],
    rules: Mapping[str, Mapping[str, Any]],
    source_path: str,
    source_sha256: str,
    definition: str,
    matcher_sha256: str,
) -> tuple[sqlite3.Row, str, sqlite3.Row | None, dict[str, int | bool]]:
    _require_schema(connection)
    base, _, base_snapshot = _validated_base(connection, business_points, rules)
    releases = connection.execute(
        "SELECT id FROM evaluation_releases WHERE taxonomy_version=? ORDER BY id",
        (TARGET_TAXONOMY_VERSION,),
    ).fetchall()
    if releases:
        raise TaxonomyV52BuilderError(
            "selling-points-v5.2 is already frozen by an evaluation release"
        )
    targets = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version=? ORDER BY id",
        (TARGET_TAXONOMY_VERSION,),
    ).fetchall()
    if len(targets) > 1:
        raise TaxonomyV52BuilderError("multiple selling-points-v5.2 taxonomies exist")
    target = targets[0] if targets else None
    drafts = connection.execute(
        "SELECT id,version FROM taxonomy_versions WHERE status='draft' ORDER BY id"
    ).fetchall()
    if target is None:
        if drafts:
            raise TaxonomyV52BuilderError("another taxonomy draft already exists")
    elif len(drafts) != 1 or str(drafts[0]["id"]) != str(target["id"]):
        raise TaxonomyV52BuilderError("v5.2 is not the isolated taxonomy draft")
    if target is not None:
        _verify_target(
            connection,
            target=target,
            business_points=business_points,
            rules=rules,
            source_path=source_path,
            source_sha256=source_sha256,
            expected_definition=definition,
            expected_matcher_sha256=matcher_sha256,
        )
    summary: dict[str, int | bool] = {
        "created_draft": target is None,
        "created_points": len(V5_2_POINT_SPEC) if target is None else 0,
        "unchanged_points": 0 if target is None else len(V5_2_POINT_SPEC),
        "retained_points": len(RETAINED_POINT_IDS),
        "removed_points": len(REMOVED_POINT_IDS),
        "new_points": len(NEW_POINT_IDS),
    }
    return base, base_snapshot, target, summary


def _insert_point(
    connection: sqlite3.Connection,
    *,
    source: Mapping[str, Any],
    rule: Mapping[str, Any],
) -> None:
    projection = project_materialized_rule(rule, point_spec=V5_2_POINT_SPEC)
    cursor = connection.execute(
        """
        INSERT INTO selling_points(
            taxonomy_id,code,tier,label,definition,positive_evidence_json,
            negative_evidence_json,boundary_rules_json,matcher_rule_json,enabled
        ) VALUES (?,?,?,?,?,?,?,?,?,1)
        """,
        (
            TARGET_TAXONOMY_ID,
            source["code"],
            source["tier"],
            source["label"],
            source["definition"],
            _projection_json(projection, "positive_evidence"),
            _projection_json(projection, "negative_evidence"),
            _projection_json(projection, "boundary_rules"),
            canonical_materialized_rule(rule, point_spec=V5_2_POINT_SPEC),
        ),
    )
    if cursor.lastrowid is None:
        raise TaxonomyV52BuilderError("selling point insert returned no id")
    for scene in projection["scenes"]:
        connection.execute(
            "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
            (cursor.lastrowid, scene),
        )


def build_v5_2_taxonomy_draft(
    *,
    db_path: Path = DEFAULT_DB,
    business_source_path: Path = DEFAULT_BUSINESS_SOURCE_PATH,
    bundle_path: Path = V4_BUNDLE_PATH,
    dry_run: bool = True,
) -> dict[str, Any]:
    """Plan or create the isolated v5.2 draft.

    Dry-run opens SQLite in ``mode=ro``. Applying requires an existing database
    and creates only a previously absent, exact v5.2 draft. Replaying an exact
    draft is a zero-write success; divergent state fails closed.
    """

    business_points, source, source_sha256, source_path = _business_points(
        business_source_path
    )
    rules, matcher_sha256, bundle_sha256, bundle_source_path = _matcher_rules(
        bundle_path
    )

    def result(
        *,
        is_dry_run: bool,
        target: sqlite3.Row | None,
        base_snapshot: str,
        summary: Mapping[str, int | bool],
    ) -> dict[str, Any]:
        return {
            "contract": BUILDER_CONTRACT,
            "dry_run": is_dry_run,
            "base_taxonomy_version": BASE_TAXONOMY_VERSION,
            "taxonomy_version": TARGET_TAXONOMY_VERSION,
            "taxonomy_id": str(target["id"]) if target else TARGET_TAXONOMY_ID,
            "business_source_path": source_path,
            "business_source_sha256": source_sha256,
            "matcher_bundle_path": bundle_source_path,
            "matcher_bundle_sha256": bundle_sha256,
            "matcher_rule_sha256": matcher_sha256,
            "base_semantic_sha256": _sha256(base_snapshot.encode("utf-8")),
            "definition": str(source["definition"]),
            **summary,
        }

    if dry_run:
        with _connect(db_path, read_only=True) as connection:
            _, base_snapshot, target, summary = _plan(
                connection,
                business_points=business_points,
                rules=rules,
                source_path=source_path,
                source_sha256=source_sha256,
                definition=str(source["definition"]),
                matcher_sha256=matcher_sha256,
            )
        return result(
            is_dry_run=True,
            target=target,
            base_snapshot=base_snapshot,
            summary=summary,
        )

    with _connect(db_path, read_only=False) as connection, transaction(connection):
        base, base_snapshot, target, summary = _plan(
            connection,
            business_points=business_points,
            rules=rules,
            source_path=source_path,
            source_sha256=source_sha256,
            definition=str(source["definition"]),
            matcher_sha256=matcher_sha256,
        )
        if target is None:
            if connection.execute(
                "SELECT 1 FROM taxonomy_versions WHERE id=?",
                (TARGET_TAXONOMY_ID,),
            ).fetchone():
                raise TaxonomyV52BuilderError(
                    f"reserved taxonomy id already exists: {TARGET_TAXONOMY_ID}"
                )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,source_path,source_sha256,created_at
                ) VALUES (?,?,'draft',?,?,?,?)
                """,
                (
                    TARGET_TAXONOMY_ID,
                    TARGET_TAXONOMY_VERSION,
                    source["definition"],
                    source_path,
                    source_sha256,
                    now_utc(),
                ),
            )
            for code in sorted(V5_2_POINT_SPEC):
                _insert_point(
                    connection, source=business_points[code], rule=rules[code]
                )
            target = connection.execute(
                "SELECT * FROM taxonomy_versions WHERE id=?",
                (TARGET_TAXONOMY_ID,),
            ).fetchone()
            assert target is not None
        _verify_target(
            connection,
            target=target,
            business_points=business_points,
            rules=rules,
            source_path=source_path,
            source_sha256=source_sha256,
            expected_definition=str(source["definition"]),
            expected_matcher_sha256=matcher_sha256,
        )
        if (
            _taxonomy_snapshot(
                connection, str(base["id"]), BASE_TAXONOMY_VERSION
            )
            != base_snapshot
        ):
            raise TaxonomyV52BuilderError("published v5.1 semantics changed")
    return result(
        is_dry_run=False,
        target=target,
        base_snapshot=base_snapshot,
        summary=summary,
    )
