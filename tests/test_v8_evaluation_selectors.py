from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from v8.evaluation_selectors import (
    EvaluationSelectorError,
    active_release,
    audit_evaluations,
    display_effective_evaluation,
    display_effective_evaluations,
    effective_direction,
    effective_direction_sql,
    formal_current_evaluations,
    release_current_evaluations,
    review_anchor_evaluation,
)
from v8.storage import connect, initialize_database


class EvaluationSelectorsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "selectors.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            for taxonomy_id, version, status in (
                ("taxonomy-v5", "selling-points-v5.0", "published"),
                ("taxonomy-v51", "selling-points-v5.1", "draft"),
                ("taxonomy-v4", "selling-points-v4.9", "retired"),
            ):
                connection.execute(
                    """
                    INSERT INTO taxonomy_versions(
                        id,version,status,definition,created_at,published_at
                    ) VALUES (?,?,?,?,?,?)
                    """,
                    (
                        taxonomy_id,
                        version,
                        status,
                        "test",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z" if status != "draft" else None,
                    ),
                )
            for release_id, rule, taxonomy, status in (
                ("release-v7", "evaluation-v7", "selling-points-v5.0", "active"),
                ("release-v8", "evaluation-v8", "selling-points-v5.1", "backfilling"),
                ("release-v6", "evaluation-v6", "selling-points-v4.9", "retired"),
            ):
                connection.execute(
                    """
                    INSERT INTO evaluation_releases(
                        id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                        created_at,updated_at,activated_at,retired_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        release_id,
                        rule,
                        taxonomy,
                        release_id[-1] * 64,
                        status,
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z" if status == "active" else None,
                        "2026-08-01T00:00:00Z" if status == "retired" else None,
                    ),
                )
            for content_id in (1, 2, 3):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        id,link_id,platform,canonical_url,title,body,content_type,
                        manual_content_direction,evaluation_content_direction,
                        imported_at,created_at,updated_at
                    ) VALUES (?,?,'douyin',?,'test','test','video','unknown','used_car',?,?,?)
                    """,
                    (
                        content_id,
                        f"SEL00{content_id}",
                        f"https://example.com/{content_id}",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                    ),
                )
            self._insert_evaluation(
                connection,
                1,
                "release-v7",
                "evaluation-v7",
                "selling-points-v5.0",
                "7" * 64,
                "media",
                "2026-08-01T01:00:00Z",
            )
            self._insert_evaluation(
                connection,
                1,
                "release-v8",
                "evaluation-v8",
                "selling-points-v5.1",
                "8" * 64,
                "new_car",
                "2026-08-01T02:00:00Z",
            )
            self._insert_evaluation(
                connection,
                2,
                "release-v6",
                "evaluation-v6",
                "selling-points-v4.9",
                "6" * 64,
                "used_car",
                "2026-08-01T03:00:00Z",
            )
            invalidated = self._insert_evaluation(
                connection,
                2,
                "release-v8",
                "evaluation-v8",
                "selling-points-v5.1",
                "8" * 64,
                "new_car",
                "2026-08-01T04:00:00Z",
            )
            self._insert_evaluation(
                connection,
                3,
                "release-v8",
                "evaluation-v8",
                "selling-points-v5.1",
                "8" * 64,
                "new_car",
                "2026-08-01T04:00:00Z",
            )
            connection.execute(
                """
                UPDATE evaluation_versions
                SET invalidated_at='2026-08-01T05:00:00Z',invalidation_reason='test'
                WHERE id=?
                """,
                (invalidated,),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _insert_evaluation(
        connection,
        content_id: int,
        release_id: str,
        rule_version: str,
        taxonomy_version: str,
        matcher_hash: str,
        direction: str,
        evaluated_at: str,
    ) -> int:
        cursor = connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id,release_id,rule_version,taxonomy_version,matcher_rule_sha256,
                evidence_sha256,evaluation_source,evaluation_status,evidence_level,
                selling_point_included,content_direction,pending_review,payload_json,
                evaluated_at
            ) VALUES (?,?,?,?,?,?,'automatic','evaluated','V3',0,?,0,?,?)
            """,
            (
                content_id,
                release_id,
                rule_version,
                taxonomy_version,
                matcher_hash,
                f"{content_id}{release_id}".encode().hex().ljust(64, "0")[:64],
                direction,
                json.dumps({"content_direction": direction}),
                evaluated_at,
            ),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def test_formal_display_review_and_audit_selectors_have_distinct_semantics(
        self,
    ) -> None:
        with connect(self.db) as connection:
            self.assertEqual(active_release(connection)["id"], "release-v7")
            formal = formal_current_evaluations(connection, [1, 2, 3])
            display = display_effective_evaluations(connection, [1, 2, 3])
            release_v8 = release_current_evaluations(
                connection, "release-v8", [1, 2, 3]
            )
            review = review_anchor_evaluation(connection, 1)
            backfill_only_review = review_anchor_evaluation(connection, 3)
            history = audit_evaluations(connection, 2)

        self.assertEqual(set(formal), {1})
        self.assertEqual(formal[1]["release_id"], "release-v7")
        self.assertEqual(display[1]["release_id"], "release-v7")
        self.assertEqual(display[1]["evaluation_freshness"], "current")
        self.assertEqual(display[2]["release_id"], "release-v6")
        self.assertEqual(display[2]["evaluation_freshness"], "stale")
        self.assertNotIn(3, display)
        self.assertEqual(set(release_v8), {1, 3})
        assert review is not None
        self.assertEqual(review["release_id"], "release-v8")
        assert backfill_only_review is not None
        self.assertEqual(backfill_only_review["release_id"], "release-v8")
        self.assertEqual(
            [row["release_id"] for row in history], ["release-v8", "release-v6"]
        )

    def test_display_and_formal_fail_closed_when_no_active_release(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at='2026-08-01T06:00:00Z'
                WHERE id='release-v7'
                """
            )
            connection.commit()
            with self.assertRaisesRegex(EvaluationSelectorError, "no active"):
                display_effective_evaluation(connection, 1)
            with self.assertRaisesRegex(EvaluationSelectorError, "no active"):
                formal_current_evaluations(connection, [1])

    def test_effective_direction_skips_unknown_and_sql_matches_python_order(
        self,
    ) -> None:
        content = {
            "manual_content_direction": "unknown",
            "evaluation_content_direction": "used_car",
            "account_content_direction": "media",
        }
        self.assertEqual(
            effective_direction(content, {"content_direction": "new_car"}), "new_car"
        )
        sql = effective_direction_sql()
        self.assertIn("NULLIF(c.manual_content_direction,'unknown')", sql)
        self.assertLess(
            sql.index("ev.content_direction"),
            sql.index("c.evaluation_content_direction"),
        )
        with self.assertRaisesRegex(ValueError, "simple identifiers"):
            effective_direction_sql(content_alias="c;drop")


if __name__ == "__main__":
    unittest.main()
