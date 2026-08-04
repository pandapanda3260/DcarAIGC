from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from v8.matcher_dsl import (
    DEFAULT_BUNDLE_PATH,
    POINT_IDS,
    POINT_SCENES,
    MatcherDslError,
    canonical_materialized_rule,
    load_bundle,
    load_bundle_bytes,
    materialize_point_rule,
    project_materialized_rule,
    taxonomy_matcher_sha256,
    validate_materialized_rule,
)
from v8.storage import (
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
)
from v8.taxonomy_rule_backfill import (
    DRAFT_TAXONOMY_ID,
    DRAFT_TAXONOMY_VERSION,
    LEGACY_TAXONOMY_VERSION,
    TaxonomyRuleBackfillError,
    backfill_v5_1_matcher_rules,
)


EXPECTED_TAXONOMY_MATCHER_SHA256 = (
    "1b2de7b0b2fe67439edd7acb68084b8ee22e82ffa3f880722de91615a98a3240"
)
EXPECTED_SOURCE_SHA256 = (
    "77dd82eebc50d527f1d7b1c641071dfc117216b55e98608fab4494eef29ed219"
)


class TaxonomyRuleBackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "taxonomy.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,source_path,source_sha256,
                    created_at,published_at
                ) VALUES (?,?, 'published','legacy taxonomy','legacy.json',?, ?,?)
                """,
                (
                    "taxonomy-v5",
                    LEGACY_TAXONOMY_VERSION,
                    "a" * 64,
                    captured_at,
                    captured_at,
                ),
            )
            for code in sorted(POINT_IDS):
                cursor = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,
                        positive_evidence_json,negative_evidence_json,
                        boundary_rules_json,matcher_rule_json,enabled
                    ) VALUES ('taxonomy-v5',?,?,?,?,'[]','[]','[]','{}',1)
                    """,
                    (
                        code,
                        "core"
                        if code in {"E1", "E2", "X1", "X2", "X3", "M1", "M2", "M3"}
                        else "other",
                        f"卖点 {code}",
                        f"定义 {code}",
                    ),
                )
                self.assertIsNotNone(cursor.lastrowid)
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (cursor.lastrowid, scene),
                    )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v6",
                taxonomy_version=LEGACY_TAXONOMY_VERSION,
            )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version=LEGACY_TAXONOMY_VERSION,
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _rows(
        self, query: str, parameters: tuple[Any, ...] = ()
    ) -> list[dict[str, Any]]:
        with connect(self.db) as connection:
            return [dict(row) for row in connection.execute(query, parameters)]

    def _legacy_snapshot(self) -> str:
        payload = {
            "taxonomy": self._rows(
                "SELECT * FROM taxonomy_versions WHERE version=?",
                (LEGACY_TAXONOMY_VERSION,),
            ),
            "points": self._rows(
                """
                SELECT * FROM selling_points
                WHERE taxonomy_id='taxonomy-v5' ORDER BY id
                """
            ),
            "scenes": self._rows(
                """
                SELECT sps.* FROM selling_point_scenes sps
                JOIN selling_points sp ON sp.id=sps.selling_point_id
                WHERE sp.taxonomy_id='taxonomy-v5'
                ORDER BY sps.selling_point_id,sps.scene
                """
            ),
            "releases": self._rows(
                """
                SELECT * FROM evaluation_releases
                WHERE taxonomy_version=? ORDER BY id
                """,
                (LEGACY_TAXONOMY_VERSION,),
            ),
        }
        return json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

    def _database_dump(self) -> str:
        with connect(self.db) as connection:
            return "\n".join(connection.iterdump())

    def test_empty_v5_creates_only_isolated_v5_1_with_self_contained_rules(
        self,
    ) -> None:
        legacy_before = self._legacy_snapshot()
        result = backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(
            result,
            {
                "dry_run": False,
                "taxonomy_version": DRAFT_TAXONOMY_VERSION,
                "taxonomy_id": DRAFT_TAXONOMY_ID,
                "matcher_rule_sha256": EXPECTED_TAXONOMY_MATCHER_SHA256,
                "created_draft": True,
                "created_points": 25,
                "updated_points": 0,
                "unchanged_points": 0,
            },
        )
        self.assertEqual(self._legacy_snapshot(), legacy_before)
        with connect(self.db) as connection:
            statuses = {
                str(row["version"]): str(row["status"])
                for row in connection.execute(
                    "SELECT version,status FROM taxonomy_versions ORDER BY version"
                )
            }
            self.assertEqual(
                statuses,
                {
                    LEGACY_TAXONOMY_VERSION: "published",
                    DRAFT_TAXONOMY_VERSION: "draft",
                },
            )
            draft_taxonomy = connection.execute(
                "SELECT source_path,source_sha256 FROM taxonomy_versions WHERE id=?",
                (DRAFT_TAXONOMY_ID,),
            ).fetchone()
            self.assertEqual(
                dict(draft_taxonomy),
                {
                    "source_path": "config/selling_point_matcher_v3.json",
                    "source_sha256": EXPECTED_SOURCE_SHA256,
                },
            )
            releases = connection.execute(
                "SELECT rule_version,taxonomy_version FROM evaluation_releases ORDER BY id"
            ).fetchall()
            self.assertEqual(
                {(row["rule_version"], row["taxonomy_version"]) for row in releases},
                {
                    ("evaluation-v6", LEGACY_TAXONOMY_VERSION),
                    ("evaluation-v7", LEGACY_TAXONOMY_VERSION),
                },
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_revisions").fetchone()[
                    0
                ],
                0,
            )
            rows = connection.execute(
                """
                SELECT * FROM selling_points
                WHERE taxonomy_id=? ORDER BY code
                """,
                (DRAFT_TAXONOMY_ID,),
            ).fetchall()
            self.assertEqual(len(rows), 25)
            stored_rules: dict[str, dict[str, Any]] = {}
            for row in rows:
                code = str(row["code"])
                rule = json.loads(str(row["matcher_rule_json"]))
                validate_materialized_rule(rule)
                self.assertEqual(
                    str(row["matcher_rule_json"]), canonical_materialized_rule(rule)
                )
                projection = project_materialized_rule(rule)
                self.assertEqual(
                    json.loads(str(row["positive_evidence_json"])),
                    projection["positive_evidence"],
                )
                self.assertEqual(
                    json.loads(str(row["negative_evidence_json"])),
                    projection["negative_evidence"],
                )
                self.assertEqual(
                    json.loads(str(row["boundary_rules_json"])),
                    projection["boundary_rules"],
                )
                scenes = {
                    str(item["scene"])
                    for item in connection.execute(
                        """
                        SELECT scene FROM selling_point_scenes
                        WHERE selling_point_id=?
                        """,
                        (row["id"],),
                    )
                }
                self.assertEqual(scenes, set(projection["scenes"]))
                stored_rules[code] = rule
            self.assertEqual(
                taxonomy_matcher_sha256(stored_rules),
                EXPECTED_TAXONOMY_MATCHER_SHA256,
            )

    def test_dry_run_is_zero_write_and_reports_the_same_plan(self) -> None:
        before = self._database_dump()
        with connect(self.db) as observer:
            version_before = int(observer.execute("PRAGMA data_version").fetchone()[0])
            result = backfill_v5_1_matcher_rules(db_path=self.db, dry_run=True)
            version_after = int(observer.execute("PRAGMA data_version").fetchone()[0])
        self.assertTrue(result["dry_run"])
        self.assertTrue(result["created_draft"])
        self.assertEqual(result["created_points"], 25)
        self.assertEqual(
            result["matcher_rule_sha256"], EXPECTED_TAXONOMY_MATCHER_SHA256
        )
        self.assertEqual(version_after, version_before)
        self.assertEqual(self._database_dump(), before)
        self.assertEqual(
            self._rows(
                "SELECT * FROM taxonomy_versions WHERE version=?",
                (DRAFT_TAXONOMY_VERSION,),
            ),
            [],
        )

    def test_bundle_bytes_are_read_once_and_hash_the_executed_payload(self) -> None:
        bundle = load_bundle()
        c1 = next(rule for rule in bundle["rules"] if rule["point_id"] == "C1")
        marker = "单次读取载荷"
        c1["explain"]["boundary_rules"][0] += marker
        payload = json.dumps(
            bundle,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        parsed = load_bundle_bytes(payload)
        expected_rules = {
            point_id: materialize_point_rule(parsed, point_id)
            for point_id in sorted(POINT_IDS)
        }
        bundle_path = DEFAULT_BUNDLE_PATH.with_name("injected-single-read.json")

        with patch.object(Path, "read_bytes", side_effect=[payload]) as mocked_read:
            result = backfill_v5_1_matcher_rules(
                db_path=self.db,
                bundle_path=bundle_path,
            )

        self.assertEqual(mocked_read.call_count, 1)
        self.assertEqual(
            result["matcher_rule_sha256"],
            taxonomy_matcher_sha256(expected_rules),
        )
        taxonomy = self._rows(
            "SELECT source_path,source_sha256 FROM taxonomy_versions WHERE id=?",
            (DRAFT_TAXONOMY_ID,),
        )
        self.assertEqual(
            taxonomy,
            [
                {
                    "source_path": "config/injected-single-read.json",
                    "source_sha256": hashlib.sha256(payload).hexdigest(),
                }
            ],
        )
        c1_rows = self._rows(
            """
            SELECT matcher_rule_json FROM selling_points
            WHERE taxonomy_id=? AND code='C1'
            """,
            (DRAFT_TAXONOMY_ID,),
        )
        stored_c1 = json.loads(c1_rows[0]["matcher_rule_json"])
        self.assertIn(marker, stored_c1["rule"]["explain"]["boundary_rules"][0])

    def test_second_run_is_idempotent_without_rewriting_points(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.db)
        before = self._rows(
            """
            SELECT sp.*,GROUP_CONCAT(sps.scene, ',') scenes
            FROM selling_points sp
            LEFT JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
            WHERE sp.taxonomy_id=? GROUP BY sp.id ORDER BY sp.code
            """,
            (DRAFT_TAXONOMY_ID,),
        )
        result = backfill_v5_1_matcher_rules(db_path=self.db)
        after = self._rows(
            """
            SELECT sp.*,GROUP_CONCAT(sps.scene, ',') scenes
            FROM selling_points sp
            LEFT JOIN selling_point_scenes sps ON sps.selling_point_id=sp.id
            WHERE sp.taxonomy_id=? GROUP BY sp.id ORDER BY sp.code
            """,
            (DRAFT_TAXONOMY_ID,),
        )
        self.assertFalse(result["created_draft"])
        self.assertEqual(result["updated_points"], 0)
        self.assertEqual(result["unchanged_points"], 25)
        self.assertEqual(after, before)

    def test_conflicting_draft_and_missing_legacy_point_fail_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO taxonomy_versions(id,version,status,definition,created_at)
                VALUES ('conflict','selling-points-v5.2','draft','conflict',?)
                """,
                (now_utc(),),
            )
            connection.commit()
        before = self._database_dump()
        with self.assertRaisesRegex(TaxonomyRuleBackfillError, "only permitted draft"):
            backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(self._database_dump(), before)

        with connect(self.db) as connection:
            connection.execute("DELETE FROM taxonomy_versions WHERE id='conflict'")
            connection.execute(
                "DELETE FROM selling_points WHERE taxonomy_id='taxonomy-v5' AND code='C4'"
            )
            connection.commit()
        before = self._database_dump()
        with self.assertRaisesRegex(TaxonomyRuleBackfillError, r"missing=\['C4'\]"):
            backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(self._database_dump(), before)

    def test_additional_published_taxonomy_fails_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('unexpected','selling-points-v4.9','published','unexpected',?,?)
                """,
                (now_utc(), now_utc()),
            )
            connection.commit()
        before = self._database_dump()
        with self.assertRaisesRegex(
            TaxonomyRuleBackfillError,
            "selling-points-v5.0 must be the only published taxonomy",
        ):
            backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(self._database_dump(), before)

    def test_v5_1_release_reference_blocks_dry_run_and_live_without_writes(
        self,
    ) -> None:
        backfill_v5_1_matcher_rules(db_path=self.db)
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,
                    status,created_at,updated_at
                ) VALUES ('v8-v5.1','evaluation-v8',?,?,
                          'draft',?,?)
                """,
                (
                    DRAFT_TAXONOMY_VERSION,
                    EXPECTED_TAXONOMY_MATCHER_SHA256,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        before = self._database_dump()

        for dry_run in (True, False):
            with self.assertRaisesRegex(
                TaxonomyRuleBackfillError,
                "already referenced by an evaluation release",
            ):
                backfill_v5_1_matcher_rules(db_path=self.db, dry_run=dry_run)
            self.assertEqual(self._database_dump(), before)

    def test_existing_v5_1_missing_point_fails_instead_of_recreating_it(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                "DELETE FROM selling_points WHERE taxonomy_id=? AND code='X8'",
                (DRAFT_TAXONOMY_ID,),
            )
            connection.commit()
        before = self._database_dump()
        with self.assertRaisesRegex(TaxonomyRuleBackfillError, r"missing=\['X8'\]"):
            backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(self._database_dump(), before)

    def test_existing_projection_drift_is_repaired_then_becomes_idempotent(
        self,
    ) -> None:
        backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT id FROM selling_points
                WHERE taxonomy_id=? AND code='E1'
                """,
                (DRAFT_TAXONOMY_ID,),
            ).fetchone()
            connection.execute(
                "UPDATE selling_points SET positive_evidence_json='[\"drift\"]' WHERE id=?",
                (row["id"],),
            )
            connection.execute(
                "DELETE FROM selling_point_scenes WHERE selling_point_id=?",
                (row["id"],),
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,'media')",
                (row["id"],),
            )
            connection.execute(
                """
                UPDATE taxonomy_versions
                SET source_path='legacy.json',source_sha256=?
                WHERE id=?
                """,
                ("a" * 64, DRAFT_TAXONOMY_ID),
            )
            connection.commit()
        repaired = backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(repaired["updated_points"], 1)
        self.assertEqual(repaired["unchanged_points"], 24)
        self.assertEqual(
            self._rows(
                "SELECT source_path,source_sha256 FROM taxonomy_versions WHERE id=?",
                (DRAFT_TAXONOMY_ID,),
            ),
            [
                {
                    "source_path": "config/selling_point_matcher_v3.json",
                    "source_sha256": EXPECTED_SOURCE_SHA256,
                }
            ],
        )
        stable = backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(stable["updated_points"], 0)

    def test_nonempty_different_draft_rule_is_never_overwritten(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT id,matcher_rule_json FROM selling_points
                WHERE taxonomy_id=? AND code='E1'
                """,
                (DRAFT_TAXONOMY_ID,),
            ).fetchone()
            changed_rule = json.loads(str(row["matcher_rule_json"]))
            changed_rule["rule"]["explain"]["boundary_rules"][0] += "。"
            changed_json = canonical_materialized_rule(changed_rule)
            connection.execute(
                "UPDATE selling_points SET matcher_rule_json=? WHERE id=?",
                (changed_json, row["id"]),
            )
            connection.commit()

        before = self._database_dump()
        for dry_run in (True, False):
            with self.assertRaisesRegex(
                TaxonomyRuleBackfillError,
                "refusing to overwrite non-empty rule",
            ):
                backfill_v5_1_matcher_rules(db_path=self.db, dry_run=dry_run)
            self.assertEqual(self._database_dump(), before)

    def test_readonly_open_failure_is_wrapped_without_writable_fallback(self) -> None:
        with patch(
            "v8.taxonomy_rule_backfill.sqlite3.connect",
            side_effect=sqlite3.OperationalError(
                "attempt to write a readonly database"
            ),
        ) as mocked_connect:
            with self.assertRaisesRegex(
                TaxonomyRuleBackfillError,
                "cannot inspect database read-only",
            ):
                backfill_v5_1_matcher_rules(db_path=self.db, dry_run=True)
        self.assertEqual(mocked_connect.call_count, 1)
        self.assertIn("mode=ro", mocked_connect.call_args.args[0])
        self.assertTrue(mocked_connect.call_args.kwargs["uri"])

    def test_live_open_is_mode_rw_and_never_creates_a_mistyped_path(self) -> None:
        missing_parent = Path(self.temp.name) / "mistyped-parent"
        missing_db = missing_parent / "missing.sqlite3"
        self.assertFalse(missing_parent.exists())
        with self.assertRaisesRegex(
            TaxonomyRuleBackfillError,
            "cannot update existing database read-write",
        ):
            backfill_v5_1_matcher_rules(db_path=missing_db)
        self.assertFalse(missing_parent.exists())
        self.assertFalse(missing_db.exists())

        with patch(
            "v8.taxonomy_rule_backfill.sqlite3.connect",
            side_effect=sqlite3.OperationalError("unable to open database file"),
        ) as mocked_connect:
            with self.assertRaisesRegex(
                TaxonomyRuleBackfillError,
                "cannot update existing database read-write",
            ):
                backfill_v5_1_matcher_rules(db_path=self.db)
        self.assertEqual(mocked_connect.call_count, 1)
        self.assertIn("mode=rw", mocked_connect.call_args.args[0])
        self.assertTrue(mocked_connect.call_args.kwargs["uri"])


class MaterializedPointRuleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.bundle = load_bundle()

    def test_rule_is_dependency_closed_and_not_a_bundle_pointer(self) -> None:
        e1 = materialize_point_rule(self.bundle, "E1")
        parsed = json.loads(canonical_materialized_rule(e1))
        validate_materialized_rule(parsed)
        self.assertIn("rule", parsed)
        self.assertIn("views", parsed)
        self.assertIn("predicates", parsed)
        self.assertIn("term_sets", parsed)
        self.assertNotIn("bundle_sha256", parsed)
        self.assertEqual(parsed["rule"]["point_id"], "E1")
        self.assertIn("explicit_dcar", parsed["predicates"])
        self.assertIn("used_basis", parsed["views"])
        self.assertEqual(parsed["views"]["used_basis"]["op"], "select")
        self.assertIn(parsed["views"]["used_basis"]["then"], parsed["views"])
        self.assertIn(parsed["views"]["used_basis"]["else"], parsed["views"])
        self.assertIn("is_image_post", parsed["predicates"])
        self.assertFalse(any(name.startswith("M") for name in parsed["term_sets"]))

        m1 = materialize_point_rule(self.bundle, "M1")
        self.assertFalse(any(name.startswith("E") for name in m1["term_sets"]))
        unrelated = copy.deepcopy(self.bundle)
        unrelated["term_sets"]["M1_when"][0] += "-unrelated-change"
        self.assertEqual(
            canonical_materialized_rule(materialize_point_rule(unrelated, "E1")),
            canonical_materialized_rule(e1),
        )

    def test_known_point_scene_is_fixed_but_custom_valid_code_is_supported(
        self,
    ) -> None:
        wrong_scene = materialize_point_rule(self.bundle, "E1")
        wrong_scene["rule"]["scene"] = "media"
        with self.assertRaises(MatcherDslError):
            validate_materialized_rule(wrong_scene)

        custom = materialize_point_rule(self.bundle, "E1")
        custom["rule"]["point_id"] = "Z1"
        custom["rule"]["scene"] = "media"
        validate_materialized_rule(custom)
        self.assertEqual(project_materialized_rule(custom)["scenes"], ["media"])

        invalid_custom = copy.deepcopy(custom)
        invalid_custom["rule"]["scene"] = "other"
        with self.assertRaises(MatcherDslError):
            validate_materialized_rule(invalid_custom)

    def test_taxonomy_hash_is_order_independent_and_content_bound(self) -> None:
        rules = {
            point_id: materialize_point_rule(self.bundle, point_id)
            for point_id in sorted(POINT_IDS)
        }
        self.assertEqual(
            taxonomy_matcher_sha256(rules), EXPECTED_TAXONOMY_MATCHER_SHA256
        )
        self.assertEqual(
            taxonomy_matcher_sha256(dict(reversed(list(rules.items())))),
            EXPECTED_TAXONOMY_MATCHER_SHA256,
        )
        changed = copy.deepcopy(rules)
        changed["C1"]["rule"]["explain"]["boundary_rules"][0] += "。"
        self.assertNotEqual(
            taxonomy_matcher_sha256(changed), EXPECTED_TAXONOMY_MATCHER_SHA256
        )


if __name__ == "__main__":
    unittest.main()
