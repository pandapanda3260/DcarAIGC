from __future__ import annotations

import copy
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from v8.matcher_dsl import (
    canonical_json,
    canonical_materialized_rule,
    load_bundle,
    materialize_point_rule,
    project_materialized_rule,
    validate_materialized_rule,
)
from v8.storage import connect, initialize_database, now_utc
from v8.taxonomy import (
    TaxonomyError,
    TaxonomyValidationError,
    create_point,
    delete_point,
    ensure_draft,
    list_points,
    publish_draft,
    update_point,
)


class V8TaxonomyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "taxonomy.sqlite3"
        self.c1_rule = materialize_point_rule(load_bundle(), "C1")
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, source_path, source_sha256,
                    created_at, published_at
                ) VALUES ('v5', 'selling-points-v5.0', 'published', 'base',
                          'rules.json', ?, ?, ?)
                """,
                ("a" * 64, now_utc(), now_utc()),
            )
            self._insert_point(
                connection,
                taxonomy_id="v5",
                code="C1",
                label="汽车知识",
                rule=self.c1_rule,
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    @staticmethod
    def _insert_point(
        connection: Any,
        *,
        taxonomy_id: str,
        code: str,
        label: str,
        rule: dict[str, Any],
    ) -> int:
        projection = project_materialized_rule(rule)
        cursor = connection.execute(
            """
            INSERT INTO selling_points(
                taxonomy_id, code, tier, label, definition,
                positive_evidence_json, negative_evidence_json,
                boundary_rules_json, matcher_rule_json, enabled
            ) VALUES (?, ?, 'other', ?, '定义', ?, ?, ?, ?, 1)
            """,
            (
                taxonomy_id,
                code,
                label,
                canonical_json(projection["positive_evidence"]),
                canonical_json(projection["negative_evidence"]),
                canonical_json(projection["boundary_rules"]),
                canonical_materialized_rule(rule),
            ),
        )
        assert cursor.lastrowid is not None
        for scene in projection["scenes"]:
            connection.execute(
                """
                INSERT INTO selling_point_scenes(selling_point_id, scene)
                VALUES (?, ?)
                """,
                (cursor.lastrowid, scene),
            )
        return int(cursor.lastrowid)

    def _custom_rule(self, code: str, scene: str) -> dict[str, Any]:
        rule = copy.deepcopy(self.c1_rule)
        rule["rule"]["point_id"] = code
        rule["rule"]["scene"] = scene
        validate_materialized_rule(rule)
        return rule

    @staticmethod
    def _payload(
        rule: dict[str, Any],
        *,
        code: str | None = None,
        label: str = "规则卖点",
    ) -> dict[str, Any]:
        value: dict[str, Any] = {
            "tier": "core",
            "label": label,
            "definition": "规则定义",
            "matcher_rule": rule,
        }
        if code is not None:
            value["code"] = code
        return value

    def _dump(self) -> str:
        with connect(self.db) as connection:
            return "\n".join(connection.iterdump())

    def _draft_id(self) -> str:
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT id FROM taxonomy_versions WHERE status='draft'"
            ).fetchone()
        assert row is not None
        return str(row["id"])

    def _freeze_draft(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id, rule_version, taxonomy_version, matcher_rule_sha256,
                    status, created_at, updated_at
                ) VALUES ('release-v8', 'evaluation-v8', 'selling-points-v5.1', ?,
                          'draft', ?, ?)
                """,
                ("b" * 64, captured_at, captured_at),
            )
            connection.commit()

    def test_draft_crud_uses_matcher_as_only_source_and_preserves_published(
        self,
    ) -> None:
        draft = ensure_draft(db_path=self.db)
        self.assertEqual(draft["version"], "selling-points-v5.1")
        self.assertEqual(ensure_draft(db_path=self.db)["id"], draft["id"])

        changed_c1 = copy.deepcopy(self.c1_rule)
        changed_c1["rule"]["explain"]["boundary_rules"][0] += "（已确认）"
        updated = update_point(
            "C1",
            self._payload(changed_c1, label="实用汽车知识"),
            db_path=self.db,
        )
        self.assertEqual(
            updated["boundary_rules"],
            project_materialized_rule(changed_c1)["boundary_rules"],
        )

        z1_rule = self._custom_rule("Z1", "new_car")
        created = create_point(
            self._payload(z1_rule, code="Z1", label="测试新卖点"),
            db_path=self.db,
        )
        self.assertEqual(created["scenes"], ["new_car"])
        self.assertEqual(created["matcher_rule"]["rule"]["point_id"], "Z1")

        published = list_points(status="published", db_path=self.db)
        draft_points = list_points(status="draft", db_path=self.db)
        self.assertEqual(published["items"][0]["label"], "汽车知识")
        self.assertEqual(
            published["items"][0]["matcher_rule"],
            json.loads(canonical_materialized_rule(self.c1_rule)),
        )
        self.assertEqual({item["code"] for item in draft_points["items"]}, {"C1", "Z1"})
        self.assertEqual(
            next(item for item in draft_points["items"] if item["code"] == "C1")[
                "boundary_rules"
            ],
            project_materialized_rule(changed_c1)["boundary_rules"],
        )

        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT matcher_rule_json, positive_evidence_json
                FROM selling_points
                WHERE taxonomy_id=? AND code='Z1'
                """,
                (draft["id"],),
            ).fetchone()
        assert row is not None
        self.assertEqual(
            str(row["matcher_rule_json"]), canonical_materialized_rule(z1_rule)
        )
        self.assertEqual(
            json.loads(str(row["positive_evidence_json"])),
            project_materialized_rule(z1_rule)["positive_evidence"],
        )

        delete_point("Z1", db_path=self.db)
        self.assertEqual(
            [
                item["code"]
                for item in list_points(status="draft", db_path=self.db)["items"]
            ],
            ["C1"],
        )

    def test_validation_error_rejects_old_write_fields_and_code_mismatches(
        self,
    ) -> None:
        ensure_draft(db_path=self.db)
        z1_rule = self._custom_rule("Z1", "new_car")

        with self.assertRaisesRegex(
            TaxonomyValidationError, "unknown selling point fields"
        ):
            create_point(
                {
                    **self._payload(z1_rule, code="Z1"),
                    "positive_evidence": ["旧写入口"],
                    "scenes": ["new_car"],
                },
                db_path=self.db,
            )
        with self.assertRaisesRegex(TaxonomyValidationError, "point_id C1"):
            create_point(self._payload(self.c1_rule, code="Z1"), db_path=self.db)
        with self.assertRaisesRegex(TaxonomyValidationError, "path code C1"):
            update_point(
                "C1",
                self._payload(z1_rule, code="Z1"),
                db_path=self.db,
            )
        with self.assertRaisesRegex(TaxonomyValidationError, "invalid matcher_rule"):
            create_point(self._payload({"rule": {}}, code="Z1"), db_path=self.db)

        self.assertEqual(
            [
                item["code"]
                for item in list_points(status="draft", db_path=self.db)["items"]
            ],
            ["C1"],
        )

    def test_draft_read_recomputes_and_checks_stored_projection(self) -> None:
        ensure_draft(db_path=self.db)
        draft_id = self._draft_id()
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE selling_points SET positive_evidence_json='["drift"]'
                WHERE taxonomy_id=? AND code='C1'
                """,
                (draft_id,),
            )
            connection.commit()
        with self.assertRaisesRegex(TaxonomyError, "stored projection for C1"):
            list_points(status="draft", db_path=self.db)
        with self.assertRaisesRegex(TaxonomyError, "stored projection for C1"):
            ensure_draft(db_path=self.db)

    def test_noncanonical_draft_matcher_fails_closed_without_rewrite(self) -> None:
        ensure_draft(db_path=self.db)
        draft_id = self._draft_id()
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE selling_points SET matcher_rule_json=?
                WHERE taxonomy_id=? AND code='C1'
                """,
                (
                    json.dumps(self.c1_rule, ensure_ascii=False, indent=2),
                    draft_id,
                ),
            )
            connection.commit()
        before = self._dump()

        with self.assertRaisesRegex(TaxonomyError, "is not canonical"):
            list_points(status="draft", db_path=self.db)
        with self.assertRaisesRegex(TaxonomyError, "is not canonical"):
            ensure_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)

    def test_nonempty_published_matcher_is_also_validated(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE selling_points SET boundary_rules_json='[]'
                WHERE taxonomy_id='v5' AND code='C1'
                """
            )
            connection.commit()
        with self.assertRaisesRegex(TaxonomyError, "stored projection for C1"):
            list_points(status="published", db_path=self.db)
        before = self._dump()
        with self.assertRaisesRegex(TaxonomyError, "stored projection for C1"):
            ensure_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)

    def test_legacy_empty_matcher_is_read_only_compatible_but_not_cloneable(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE selling_points SET matcher_rule_json='{}'
                WHERE taxonomy_id='v5' AND code='C1'
                """
            )
            connection.commit()
        published = list_points(status="published", db_path=self.db)
        self.assertIsNone(published["items"][0]["matcher_rule"])

        before = self._dump()
        with self.assertRaisesRegex(TaxonomyError, "no complete matcher rules"):
            ensure_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM taxonomy_versions WHERE status='draft'"
                ).fetchone()[0],
                0,
            )

    def test_ensure_draft_fails_closed_on_multiple_drafts(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            for taxonomy_id, version in (
                ("d1", "selling-points-v5.1"),
                ("d2", "selling-points-v5.2"),
            ):
                connection.execute(
                    """
                    INSERT INTO taxonomy_versions(
                        id, version, status, definition, created_at
                    ) VALUES (?, ?, 'draft', 'draft', ?)
                    """,
                    (taxonomy_id, version, captured_at),
                )
            connection.commit()
        before = self._dump()
        with self.assertRaisesRegex(TaxonomyError, "multiple draft taxonomies"):
            ensure_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)

    def test_ensure_draft_fails_closed_when_next_version_already_retired(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('retired-v5.1','selling-points-v5.1','retired',
                          'rollback history',?,?)
                """,
                (now_utc(), now_utc()),
            )
            connection.commit()
        before = self._dump()

        with self.assertRaisesRegex(
            TaxonomyError,
            "next taxonomy version selling-points-v5.1 already exists with status retired",
        ):
            ensure_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)

    def test_referenced_draft_blocks_ensure_and_all_authoring_operations(self) -> None:
        ensure_draft(db_path=self.db)
        z1_rule = self._custom_rule("Z1", "new_car")
        self._freeze_draft()
        before = self._dump()

        blocked_calls = (
            lambda: ensure_draft(db_path=self.db),
            lambda: create_point(self._payload(z1_rule, code="Z1"), db_path=self.db),
            lambda: update_point("C1", self._payload(self.c1_rule), db_path=self.db),
            lambda: delete_point("C1", db_path=self.db),
            lambda: publish_draft(db_path=self.db),
        )
        for call in blocked_calls:
            with self.subTest(call=call):
                with self.assertRaisesRegex(TaxonomyError, "is immutable"):
                    call()
                self.assertEqual(self._dump(), before)

    def test_publish_is_zero_write_and_requires_atomic_release_activation(self) -> None:
        ensure_draft(db_path=self.db)
        before = self._dump()
        with self.assertRaisesRegex(
            TaxonomyError, "requires atomic evaluation release activation"
        ):
            publish_draft(db_path=self.db)
        self.assertEqual(self._dump(), before)
        with connect(self.db) as connection:
            statuses = {
                str(row["version"]): str(row["status"])
                for row in connection.execute(
                    "SELECT version, status FROM taxonomy_versions ORDER BY version"
                )
            }
        self.assertEqual(
            statuses,
            {
                "selling-points-v5.0": "published",
                "selling-points-v5.1": "draft",
            },
        )

    def test_invalid_list_status_is_a_validation_error(self) -> None:
        with self.assertRaisesRegex(TaxonomyValidationError, "published or draft"):
            list_points(status="retired", db_path=self.db)


if __name__ == "__main__":
    unittest.main()
