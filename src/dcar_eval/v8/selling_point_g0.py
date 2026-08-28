"""Finalize the v5.3 label standard and re-materialize its unfrozen draft."""

from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .matcher_dsl import (
    V4_BUNDLE_PATH,
    V5_2_POINT_SPEC,
    canonical_json,
    canonical_materialized_rule,
    load_bundle,
    materialize_point_rule,
    project_materialized_rule,
    taxonomy_matcher_sha256,
)
from .selling_point_label_cards import DEFAULT_LABEL_CARD_PATH, load_label_cards
from .storage import DEFAULT_DB, connect, transaction
from .taxonomy import serialize_point_row


class SellingPointG0Error(RuntimeError):
    """Raised when G0 cannot safely finalize the v5.3 draft."""


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def build_v5_3_points(
    *,
    config_path: Path = DEFAULT_LABEL_CARD_PATH,
    bundle_path: Path = V4_BUNDLE_PATH,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Build all 28 point rows from the finalized cards and checked DSL bundle."""

    labels = load_label_cards(config_path)
    bundle = load_bundle(bundle_path, point_spec=V5_2_POINT_SPEC)
    points: dict[str, dict[str, Any]] = {}
    for code, card in sorted(labels["cards"].items()):
        rule = materialize_point_rule(bundle, code, point_spec=V5_2_POINT_SPEC)
        finalized = copy.deepcopy(rule)
        finalized["rule"]["explain"] = {
            "positive_evidence": list(card["positive_evidence"]),
            "negative_evidence": list(card["negative_evidence"]),
            "boundary_rules": list(card["boundary_rules"]),
        }
        matcher_rule_json = canonical_materialized_rule(
            finalized,
            point_spec=V5_2_POINT_SPEC,
        )
        matcher_rule = json.loads(matcher_rule_json)
        projection = project_materialized_rule(
            matcher_rule,
            point_spec=V5_2_POINT_SPEC,
        )
        points[code] = {
            "code": code,
            "tier": str(card["tier"]),
            "label": str(card["label"]),
            "definition": str(card["definition"]),
            "matcher_rule": matcher_rule,
            "matcher_rule_json": matcher_rule_json,
            **projection,
        }
    if set(points) != set(V5_2_POINT_SPEC) or len(points) != 28:
        raise SellingPointG0Error("v5.3 materialization must contain all 28 codes")
    metadata = {
        "taxonomy_version": "selling-points-v5.3",
        "definition": str(labels["definition"]),
        "source_path": str(config_path.resolve()),
        "source_sha256": str(labels["source_sha256"]),
        "matcher_bundle_path": str(bundle_path.resolve()),
        "matcher_rule_sha256": taxonomy_matcher_sha256(
            {code: point["matcher_rule"] for code, point in points.items()},
            point_spec=V5_2_POINT_SPEC,
        ),
    }
    return points, metadata


def _draft_state(
    connection: sqlite3.Connection,
    points: Mapping[str, Mapping[str, Any]],
) -> tuple[sqlite3.Row, dict[str, sqlite3.Row], int]:
    rows = connection.execute(
        "SELECT * FROM taxonomy_versions WHERE version='selling-points-v5.3'"
    ).fetchall()
    if len(rows) != 1 or str(rows[0]["status"]) != "draft":
        raise SellingPointG0Error("selling-points-v5.3 must exist exactly once as draft")
    taxonomy = rows[0]
    references = int(
        connection.execute(
            "SELECT COUNT(*) FROM evaluation_releases WHERE taxonomy_version=?",
            (taxonomy["version"],),
        ).fetchone()[0]
    )
    if references != 0:
        raise SellingPointG0Error("selling-points-v5.3 is frozen by an evaluation release")
    point_rows = {
        str(row["code"]): row
        for row in connection.execute(
            "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
            (taxonomy["id"],),
        )
    }
    if set(point_rows) != set(points) or len(point_rows) != 28:
        missing = sorted(set(points) - set(point_rows))
        extra = sorted(set(point_rows) - set(points))
        raise SellingPointG0Error(
            f"v5.3 draft point set drifted: missing={missing}, extra={extra}"
        )
    return taxonomy, point_rows, references


def _point_changed(row: sqlite3.Row, point: Mapping[str, Any]) -> bool:
    return any(
        (
            str(row["tier"]) != point["tier"],
            str(row["label"]) != point["label"],
            str(row["definition"]) != point["definition"],
            str(row["positive_evidence_json"])
            != canonical_json(point["positive_evidence"]),
            str(row["negative_evidence_json"])
            != canonical_json(point["negative_evidence"]),
            str(row["boundary_rules_json"])
            != canonical_json(point["boundary_rules"]),
            str(row["matcher_rule_json"]) != point["matcher_rule_json"],
        )
    )


def _replace_scenes(
    connection: sqlite3.Connection,
    selling_point_id: int,
    scenes: list[str],
) -> None:
    connection.execute(
        "DELETE FROM selling_point_scenes WHERE selling_point_id=?",
        (selling_point_id,),
    )
    connection.executemany(
        "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
        [(selling_point_id, scene) for scene in scenes],
    )


def _receipt(
    *,
    taxonomy: sqlite3.Row,
    points: Mapping[str, Mapping[str, Any]],
    metadata: Mapping[str, Any],
    changed_points: int,
    dry_run: bool,
) -> dict[str, Any]:
    content = [
        {
            "code": code,
            "tier": point["tier"],
            "label": point["label"],
            "definition": point["definition"],
            "matcher_rule": point["matcher_rule"],
            "positive_evidence": point["positive_evidence"],
            "negative_evidence": point["negative_evidence"],
            "boundary_rules": point["boundary_rules"],
            "scenes": point["scenes"],
        }
        for code, point in sorted(points.items())
    ]
    value = {
        "version": "selling-point-g0-draft-receipt-v1",
        "dry_run": dry_run,
        "taxonomy_id": str(taxonomy["id"]),
        "taxonomy_version": str(taxonomy["version"]),
        "taxonomy_status": str(taxonomy["status"]),
        "release_reference_count": 0,
        "point_count": len(points),
        "changed_points": changed_points,
        "source_path": metadata["source_path"],
        "source_sha256": metadata["source_sha256"],
        "matcher_rule_sha256": metadata["matcher_rule_sha256"],
        "point_content_sha256": _sha256_json(content),
    }
    value["receipt_sha256"] = _sha256_json(value)
    return value


def materialize_v5_3_draft(
    *,
    db_path: Path = DEFAULT_DB,
    config_path: Path = DEFAULT_LABEL_CARD_PATH,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Replace all v5.3 draft points in one transaction and verify projections."""

    points, metadata = build_v5_3_points(config_path=config_path)
    if dry_run:
        with connect(db_path.resolve(), read_only=True) as connection:
            taxonomy, point_rows, _ = _draft_state(connection, points)
            changed = sum(
                _point_changed(point_rows[code], point) for code, point in points.items()
            )
            return _receipt(
                taxonomy=taxonomy,
                points=points,
                metadata=metadata,
                changed_points=changed,
                dry_run=True,
            )

    with connect(db_path.resolve()) as connection, transaction(connection):
        taxonomy, point_rows, _ = _draft_state(connection, points)
        changed = sum(
            _point_changed(point_rows[code], point) for code, point in points.items()
        )
        for code, point in sorted(points.items()):
            row = point_rows[code]
            connection.execute(
                """
                UPDATE selling_points
                SET tier=?,label=?,definition=?,positive_evidence_json=?,
                    negative_evidence_json=?,boundary_rules_json=?,matcher_rule_json=?
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
            _replace_scenes(connection, int(row["id"]), list(point["scenes"]))
        connection.execute(
            """
            UPDATE taxonomy_versions
            SET definition=?,source_path=?,source_sha256=?
            WHERE id=?
            """,
            (
                metadata["definition"],
                metadata["source_path"],
                metadata["source_sha256"],
                taxonomy["id"],
            ),
        )
        verified_taxonomy = connection.execute(
            "SELECT * FROM taxonomy_versions WHERE id=?", (taxonomy["id"],)
        ).fetchone()
        assert verified_taxonomy is not None
        verified_rows = connection.execute(
            "SELECT * FROM selling_points WHERE taxonomy_id=? ORDER BY code",
            (taxonomy["id"],),
        ).fetchall()
        serialized = {
            str(row["code"]): serialize_point_row(connection, verified_taxonomy, row)
            for row in verified_rows
        }
        if len(serialized) != 28 or any(
            serialized[code]["label"] != points[code]["label"] for code in points
        ):
            raise SellingPointG0Error("v5.3 draft verification failed")
        receipt = _receipt(
            taxonomy=verified_taxonomy,
            points=points,
            metadata=metadata,
            changed_points=changed,
            dry_run=False,
        )
    return receipt
