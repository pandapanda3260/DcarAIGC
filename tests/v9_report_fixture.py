"""Shared, exact evaluation-v9 fixture for report and API tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Sequence

from v8.evaluation import evaluate_release_content
from v8.matcher_dsl import (
    V4_BUNDLE_PATH,
    V5_2_POINT_SPEC,
    canonical_materialized_rule,
    load_bundle,
    materialize_point_rule,
    project_materialized_rule,
    taxonomy_matcher_sha256,
)
from v8.storage import connect, now_utc, transaction


V9_FIXTURE_RELEASE_ID = "evaluation-v9__selling-points-v5.2"
_V9_FIXTURE_TAXONOMY_ID = "taxonomy-v5.2-report-fixture"


def activate_v9_report_fixture(db_path: Path, content_ids: Sequence[int]) -> str:
    """Install the exact v5.2 matcher, evaluate, and activate v9."""

    bundle = load_bundle(V4_BUNDLE_PATH, point_spec=V5_2_POINT_SPEC)
    rules = {
        code: materialize_point_rule(
            bundle,
            code,
            point_spec=V5_2_POINT_SPEC,
        )
        for code in V5_2_POINT_SPEC
    }
    matcher_sha256 = taxonomy_matcher_sha256(
        rules,
        point_spec=V5_2_POINT_SPEC,
    )
    captured_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            "UPDATE taxonomy_versions SET status='retired' WHERE status='published'"
        )
        connection.execute(
            """
            INSERT INTO taxonomy_versions(
                id,version,status,definition,created_at,published_at
            ) VALUES (?, 'selling-points-v5.2', 'published',
                      'evaluation-v9 report test fixture', ?, ?)
            """,
            (_V9_FIXTURE_TAXONOMY_ID, captured_at, captured_at),
        )
        for code in sorted(V5_2_POINT_SPEC):
            rule = rules[code]
            projection = project_materialized_rule(
                rule,
                point_spec=V5_2_POINT_SPEC,
            )
            point = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id,code,tier,label,definition,
                    positive_evidence_json,negative_evidence_json,
                    boundary_rules_json,matcher_rule_json,enabled
                ) VALUES (?,?, 'other',?, 'test fixture',?,?,?,?,1)
                """,
                (
                    _V9_FIXTURE_TAXONOMY_ID,
                    code,
                    f"测试卖点 {code}",
                    json.dumps(
                        projection["positive_evidence"], ensure_ascii=False
                    ),
                    json.dumps(
                        projection["negative_evidence"], ensure_ascii=False
                    ),
                    json.dumps(projection["boundary_rules"], ensure_ascii=False),
                    canonical_materialized_rule(
                        rule,
                        point_spec=V5_2_POINT_SPEC,
                    ),
                ),
            )
            assert point.lastrowid is not None
            for scene in projection["scenes"]:
                connection.execute(
                    """
                    INSERT INTO selling_point_scenes(selling_point_id,scene)
                    VALUES (?,?)
                    """,
                    (point.lastrowid, scene),
                )
        connection.execute(
            """
            INSERT INTO evaluation_releases(
                id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                created_at,updated_at
            ) VALUES (?,'evaluation-v9','selling-points-v5.2',?,
                      'backfilling',?,?)
            """,
            (V9_FIXTURE_RELEASE_ID, matcher_sha256, captured_at, captured_at),
        )

    for content_id in content_ids:
        evaluate_release_content(
            int(content_id),
            release_id=V9_FIXTURE_RELEASE_ID,
            db_path=db_path,
        )

    activated_at = now_utc()
    with connect(db_path) as connection, transaction(connection):
        connection.execute(
            """
            UPDATE evaluation_releases
            SET status='retired',retired_at=?,updated_at=?
            WHERE status='active' AND id<>?
            """,
            (activated_at, activated_at, V9_FIXTURE_RELEASE_ID),
        )
        connection.execute(
            """
            UPDATE evaluation_releases
            SET status='active',activated_at=?,updated_at=? WHERE id=?
            """,
            (activated_at, activated_at, V9_FIXTURE_RELEASE_ID),
        )
    return V9_FIXTURE_RELEASE_ID
