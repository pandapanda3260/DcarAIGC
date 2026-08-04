from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import v8.legacy_scene_repair as repair_module
from v8.legacy_scene_repair import (
    INVALIDATION_REASON_PREFIX,
    QUEUE_REASON_CODE,
    LegacySceneRepairBoundary,
    LegacySceneRepairError,
    repair_legacy_illegal_scene_chains,
)
from v8.report_repair import UnsafeReportTarget
from v8.storage import (
    LEGACY_V6_RELEASE_ID,
    LEGACY_V7_RELEASE_ID,
    connect,
    initialize_database,
)


class LegacySceneRepairTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "current.sqlite3"
        self.manifest_path = self.root / "manifest.json"
        self.receipt_path = self.root / "receipt.json"
        self.manifest_path.write_text("{}\n", encoding="utf-8")
        self.receipt_path.write_text("{}\n", encoding="utf-8")
        self._create_database()
        manifest = SimpleNamespace(
            sha256="a" * 64,
            logical_snapshot_sha256="b" * 64,
            database_backup_sha256="c" * 64,
            source_database=self.db.resolve(),
            database_backup=(self.root / "frozen.sqlite3").resolve(),
        )
        self.report_target = UnsafeReportTarget(
            task_id="fixture-report",
            revision=1,
            contract_version="dcar-content-operations-report-v8.3",
            rule_version="evaluation-v7",
            taxonomy_version="selling-points-v5.0",
            report_json_path="reports/fixture-report/report.json",
            report_sha256="9" * 64,
            created_at="2026-08-03T00:00:00Z",
            creation_source="automatic",
            task_status="succeeded",
            task_started_at="2026-08-03T00:00:00Z",
            task_completed_at="2026-08-03T00:01:00Z",
            scheduler_run_ids=(1,),
        )
        self.boundary = LegacySceneRepairBoundary(
            manifest=manifest,
            report_boundary=SimpleNamespace(
                targets=(self.report_target,), manifest_sha256=manifest.sha256
            ),
            content_ids=(1, 2, 3),
            evaluation_high_water=6,
            frozen_evaluation_count=6,
            frozen_match_count=6,
            frozen_review_queue_count=1,
        )
        audit_id = (
            "release-backfill__evaluation-v8__selling-points-v5.1__"
            f"{manifest.logical_snapshot_sha256[:12]}__production"
        )
        with connect(self.db) as connection:
            stable_state = repair_module._protected_state(
                connection, manifest, activation_stable=True
            )
            connection.execute(
                """
                INSERT INTO migration_audit(
                    id,baseline_id,source_database,source_sha256,status,
                    summary_json,started_at,completed_at
                ) VALUES (?,?,?,?, 'succeeded',?,?,?)
                """,
                (
                    audit_id,
                    "release fixture",
                    str(self.db),
                    "f" * 64,
                    json.dumps(
                        {"activation_stable_state": stable_state}, sort_keys=True
                    ),
                    "2026-08-04T09:00:00Z",
                    "2026-08-04T09:00:00Z",
                ),
            )
            connection.commit()
        self.receipt = {
            "core_sha256": "d" * 64,
            "core": {
                "freeze_manifest_sha256": manifest.sha256,
                "target_taxonomy_semantic_sha256": "e" * 64,
                "release": {
                    "id": "evaluation-v8__selling-points-v5.1",
                    "rule_version": "evaluation-v8",
                    "taxonomy_version": "selling-points-v5.1",
                },
            },
            "execution": {
                "audit_id": audit_id,
                "activation_stable_state_sha256": stable_state["state_sha256"],
            },
        }
        self.original_report_repair_check = (
            repair_module._require_report_repair_completed
        )
        self.original_approved_plan_check = repair_module._require_approved_plan
        self.original_target_receipt_check = (
            repair_module._require_target_receipt_semantics
        )
        self.load_boundary = patch.object(
            repair_module, "_load_boundary", return_value=self.boundary
        ).start()
        self.read_receipt = patch.object(
            repair_module, "_read_receipt", return_value=self.receipt
        ).start()
        self.require_receipt = patch.object(
            repair_module, "_require_production_receipt_chain"
        ).start()
        self.target_semantic = patch.object(
            repair_module, "_target_semantic_sha256", return_value="e" * 64
        ).start()
        self.report_repair = patch.object(
            repair_module, "_require_report_repair_completed", return_value=()
        ).start()
        self.target_receipt = patch.object(
            repair_module, "_require_target_receipt_semantics"
        ).start()
        self.approved_plan = patch.object(
            repair_module, "_require_approved_plan"
        ).start()
        self.addCleanup(patch.stopall)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _create_database(self) -> None:
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5','selling-points-v5.0','retired','legacy',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v51','selling-points-v5.1','published','current',
                          '2026-08-04T09:00:00Z','2026-08-04T09:00:00Z')
                """
            )
            for code, scenes in {
                "C1": ("media", "used_car"),
                "C2": ("new_car", "used_car"),
                "X1": ("new_car",),
            }.items():
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy-v51',?,'other',?,?,'{}')
                    """,
                    (code, code, code),
                )
                for scene in scenes:
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            for release_id, rule_version, status, matcher in (
                (LEGACY_V6_RELEASE_ID, "evaluation-v6", "retired", "6" * 64),
                (LEGACY_V7_RELEASE_ID, "evaluation-v7", "retired", "7" * 64),
                (
                    "evaluation-v8__selling-points-v5.1",
                    "evaluation-v8",
                    "active",
                    "8" * 64,
                ),
            ):
                taxonomy = (
                    "selling-points-v5.1"
                    if rule_version == "evaluation-v8"
                    else "selling-points-v5.0"
                )
                connection.execute(
                    """
                    INSERT INTO evaluation_releases(
                        id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                        created_at,updated_at,activated_at,retired_at
                    ) VALUES (?,?,?,?,?,'2026-08-04T09:00:00Z',
                              '2026-08-04T09:00:00Z',?,?)
                    """,
                    (
                        release_id,
                        rule_version,
                        taxonomy,
                        matcher,
                        status,
                        "2026-08-04T09:00:00Z" if status == "active" else None,
                        "2026-08-04T09:00:00Z" if status == "retired" else None,
                    ),
                )
            for content_id in (1, 2, 3):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        id,link_id,platform,platform_content_id,canonical_url,title,
                        body,content_type,imported_at,created_at,updated_at
                    ) VALUES (?,?, 'douyin',?,?,?,'汽车正文','video',?,?,?)
                    """,
                    (
                        content_id,
                        f"T{content_id:05d}",
                        f"legacy-{content_id}",
                        f"https://example.test/{content_id}",
                        f"内容 {content_id}",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                        "2026-08-01T00:00:00Z",
                    ),
                )
            first_review = connection.execute(
                """
                INSERT INTO evaluation_reviews(
                    content_id,decision,reason,reviewer,created_at
                ) VALUES (2,'override','legacy manual','tester','2026-08-02T00:00:00Z')
                """
            ).lastrowid
            second_review = connection.execute(
                """
                INSERT INTO evaluation_reviews(
                    content_id,decision,reason,reviewer,created_at
                ) VALUES (2,'override','legacy manual 2','tester','2026-08-02T01:00:00Z')
                """
            ).lastrowid
            self._insert_evaluation(
                connection,
                evaluation_id=1,
                content_id=1,
                release_id=LEGACY_V6_RELEASE_ID,
                source="automatic",
                code="C1",
                scene="new_car",
                evaluated_at="2026-08-02T00:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=2,
                content_id=1,
                release_id=LEGACY_V7_RELEASE_ID,
                source="automatic",
                code="C1",
                scene="new_car",
                parent_id=1,
                evaluated_at="2026-08-02T01:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=3,
                content_id=2,
                release_id=LEGACY_V6_RELEASE_ID,
                source="automatic",
                code="X1",
                scene="new_car",
                evaluated_at="2026-08-02T00:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=4,
                content_id=2,
                release_id=LEGACY_V6_RELEASE_ID,
                source="manual_review",
                code="C2",
                scene="media",
                review_id=int(first_review),
                evaluated_at="2026-08-02T02:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=5,
                content_id=3,
                release_id=LEGACY_V6_RELEASE_ID,
                source="migrated_from_v5",
                code="C2",
                scene="media",
                evaluated_at="2026-08-02T02:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=6,
                content_id=2,
                release_id=LEGACY_V7_RELEASE_ID,
                source="manual_review",
                code="C2",
                scene="media",
                parent_id=4,
                review_id=int(second_review),
                evaluated_at="2026-08-02T03:00:00Z",
            )
            for content_id, evaluation_id in ((1, 7), (2, 8), (3, 9)):
                self._insert_evaluation(
                    connection,
                    evaluation_id=evaluation_id,
                    content_id=content_id,
                    release_id="evaluation-v8__selling-points-v5.1",
                    source="automatic",
                    code="X1",
                    scene="new_car",
                    evaluated_at="2026-08-04T09:00:00Z",
                )
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,message,created_at,started_at,
                    completed_at,updated_at
                ) VALUES (
                    'fixture-report','daily','fixture','2026-08-03','2026-08-03',
                    'automatic','succeeded',100,'','2026-08-03T00:00:00Z',
                    '2026-08-03T00:00:00Z','2026-08-03T00:01:00Z',
                    '2026-08-03T00:01:00Z'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at
                ) VALUES (
                    'fixture-report',1,?,
                    'dcar-content-operations-report-v8.3','evaluation-v7',
                    'selling-points-v5.0','reports/fixture-report/report.json',?,
                    '2026-08-03T00:00:00Z'
                )
                """,
                (LEGACY_V7_RELEASE_ID, "9" * 64),
            )
            connection.execute(
                """
                INSERT INTO task_events(
                    task_id,event_type,message,payload_json,created_at
                ) VALUES (
                    'fixture-report','task_completed','fixture','{}',
                    '2026-08-03T00:01:00Z'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,priority,status,
                    created_at,updated_at,resolved_at
                ) VALUES (1,7,'evaluation_gray_zone',50,'resolved',?,?,?)
                """,
                (
                    "2026-08-03T00:00:00Z",
                    "2026-08-03T00:00:00Z",
                    "2026-08-03T00:00:00Z",
                ),
            )
            connection.commit()

    @staticmethod
    def _insert_evaluation(
        connection,
        *,
        evaluation_id: int,
        content_id: int,
        release_id: str,
        source: str,
        code: str,
        scene: str,
        evaluated_at: str,
        parent_id: int | None = None,
        review_id: int | None = None,
    ) -> None:
        release = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (release_id,)
        ).fetchone()
        assert release is not None
        connection.execute(
            """
            INSERT INTO evaluation_versions(
                id,content_id,release_id,parent_evaluation_id,review_id,rule_version,
                taxonomy_version,matcher_rule_sha256,evidence_sha256,evaluation_source,
                evaluation_status,evidence_level,primary_selling_point_code,
                selling_point_score,selling_point_included,content_direction,
                content_automotive_score,pending_review,payload_json,evaluated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,'evaluated','V3',?,90,1,?,80,0,'{}',?)
            """,
            (
                evaluation_id,
                content_id,
                release_id,
                parent_id,
                review_id,
                release["rule_version"],
                release["taxonomy_version"],
                release["matcher_rule_sha256"],
                f"{evaluation_id:064x}",
                source,
                code,
                scene,
                evaluated_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO evaluation_matches(
                evaluation_id,selling_point_code,scene,match_role,score,evidence_json
            ) VALUES (?, ?,?,'primary',90,'{}')
            """,
            (evaluation_id, code, scene),
        )

    def _repair(
        self,
        *,
        apply: bool = False,
        expected_plan_sha256: str | None = None,
        acknowledge: bool = False,
    ) -> dict[str, object]:
        return repair_legacy_illegal_scene_chains(
            db_path=self.db,
            manifest_path=self.manifest_path,
            receipt_path=self.receipt_path,
            operator_reason="remove frozen illegal automatic scene chains",
            apply=apply,
            expected_plan_sha256=expected_plan_sha256,
            acknowledge_rollback_window_close=acknowledge,
        )

    def _simulate_report_repair(self) -> int:
        captured_at = "2026-08-04T09:30:00Z"
        payload = repair_module.report_invalidation_event_payload(
            self.report_target,
            manifest_sha256=self.boundary.manifest.sha256,
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE report_revisions
                SET invalidated_at=?,invalidation_reason=?
                WHERE task_id=? AND revision=?
                """,
                (
                    captured_at,
                    repair_module.REPORT_INVALIDATION_REASON,
                    *self.report_target.key,
                ),
            )
            cursor = connection.execute(
                """
                INSERT INTO task_events(
                    task_id,event_type,message,payload_json,created_at
                ) VALUES (?,?,?,?,?)
                """,
                (
                    self.report_target.task_id,
                    repair_module.REPORT_INVALIDATION_EVENT_TYPE,
                    "revision 1 invalidated from freeze manifest",
                    payload,
                    captured_at,
                ),
            )
            connection.commit()
        assert cursor.lastrowid is not None
        event_id = int(cursor.lastrowid)
        self.report_repair.return_value = (event_id,)
        return event_id

    def test_dry_run_is_stable_and_does_not_write(self) -> None:
        with connect(self.db) as connection:
            counts_before = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "evaluation_versions",
                    "evaluation_matches",
                    "review_queue",
                    "migration_audit",
                )
            }
        first = self._repair()
        second = self._repair()
        self.assertFalse(first["rollback_window_closed"])
        self.assertEqual(first["plan_sha256"], second["plan_sha256"])
        plan = first["plan"]
        self.assertEqual(plan["automatic_evaluation_ids"], [1, 2])
        self.assertEqual(plan["automatic_content_ids"], [1])
        self.assertEqual(plan["nonautomatic_evaluation_ids"], [4, 5, 6])
        self.assertEqual(plan["nonautomatic_content_ids"], [2, 3])
        self.assertEqual(plan["illegal_match_count"], 5)
        with connect(self.db) as connection:
            counts_after = {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in counts_before
            }
        self.assertEqual(counts_after, counts_before)

    def test_same_count_legacy_row_drift_is_rejected_by_receipt_attestation(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_versions SET payload_json=? WHERE id=3",
                ('{"same_count_drift":true}',),
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "protected table changed after activation"
        ):
            self._repair()

    def test_exact_approved_report_repair_is_the_only_attested_drift(self) -> None:
        event_id = self._simulate_report_repair()
        with connect(self.db) as connection:
            self.assertEqual(
                self.original_report_repair_check(connection, self.boundary),
                (event_id,),
            )
        result = self._repair()
        self.assertEqual(result["plan"]["automatic_evaluation_ids"], [1, 2])

    def test_report_repair_event_message_is_part_of_the_boundary(self) -> None:
        self._simulate_report_repair()
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE task_events SET message='tampered'
                WHERE event_type=?
                """,
                (repair_module.REPORT_INVALIDATION_EVENT_TYPE,),
            )
            connection.commit()
        with (
            connect(self.db) as connection,
            self.assertRaisesRegex(
                LegacySceneRepairError, "report repair event is incomplete"
            ),
        ):
            self.original_report_repair_check(connection, self.boundary)

    def test_approved_plan_is_bound_to_the_frozen_logical_snapshot(self) -> None:
        with self.assertRaisesRegex(
            LegacySceneRepairError, "not approved for this logical snapshot"
        ):
            self.original_approved_plan_check(self.boundary, {})

    def test_target_evaluation_semantics_are_bound_to_receipt(self) -> None:
        def semantic_core(connection, *_args, **_kwargs):
            payload = connection.execute(
                "SELECT payload_json FROM evaluation_versions WHERE id=8"
            ).fetchone()[0]
            core = dict(self.receipt["core"])
            if payload != "{}":
                core["semantic_sha256"] = "f" * 64
            return core

        with (
            connect(self.db) as connection,
            patch.object(repair_module, "_semantic_core", side_effect=semantic_core),
        ):
            self.original_target_receipt_check(
                connection,
                boundary=self.boundary,
                receipt=self.receipt,
                report_event_ids=(),
            )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_versions SET payload_json=? WHERE id=8",
                ('{"tampered_target":true}',),
            )
            connection.commit()
        with (
            connect(self.db) as connection,
            patch.object(repair_module, "_semantic_core", side_effect=semantic_core),
            self.assertRaisesRegex(
                LegacySceneRepairError, "semantics differ from the production receipt"
            ),
        ):
            self.original_target_receipt_check(
                connection,
                boundary=self.boundary,
                receipt=self.receipt,
                report_event_ids=(),
            )

    def test_real_target_semantic_snapshot_runs_inside_apply_transaction(
        self,
    ) -> None:
        with (
            patch.object(
                repair_module,
                "_require_target_receipt_semantics",
                wraps=self.original_target_receipt_check,
            ) as semantic_gate,
            patch.object(
                repair_module,
                "_semantic_core",
                return_value=self.receipt["core"],
            ),
        ):
            dry_run = self._repair()
            applied = self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )
        self.assertEqual(semantic_gate.call_count, 2)
        self.assertEqual(applied["invalidated_count"], 2)

    def test_chain_closure_includes_legal_parent_and_independent_v7(self) -> None:
        with connect(self.db) as connection:
            self._insert_evaluation(
                connection,
                evaluation_id=11,
                content_id=1,
                release_id=LEGACY_V6_RELEASE_ID,
                source="automatic",
                code="X1",
                scene="new_car",
                evaluated_at="2026-08-02T04:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=12,
                content_id=1,
                release_id=LEGACY_V7_RELEASE_ID,
                source="automatic",
                code="C1",
                scene="new_car",
                parent_id=11,
                evaluated_at="2026-08-02T05:00:00Z",
            )
            self._insert_evaluation(
                connection,
                evaluation_id=13,
                content_id=3,
                release_id=LEGACY_V7_RELEASE_ID,
                source="automatic",
                code="C1",
                scene="new_car",
                evaluated_at="2026-08-02T05:00:00Z",
            )
            closure = repair_module._automatic_chain_closure(
                connection,
                boundary=replace(self.boundary, evaluation_high_water=13),
                seed_ids={12, 13},
            )
        self.assertEqual(closure, (11, 12, 13))

    def test_extra_task_event_after_report_repair_is_rejected(self) -> None:
        self._simulate_report_repair()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO task_events(
                    task_id,event_type,message,payload_json,created_at
                ) VALUES (
                    'fixture-report','unexpected','drift','{}',
                    '2026-08-04T09:31:00Z'
                )
                """
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "task events contain changes outside"
        ):
            self._repair()

    def test_apply_is_append_only_and_repeat_is_zero_write(self) -> None:
        dry_run = self._repair()
        with connect(self.db) as connection:
            legacy_match_before = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM evaluation_matches WHERE evaluation_id<=6 ORDER BY evaluation_id"
                )
            ]
            legacy_nonautomatic_before = [
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT * FROM evaluation_versions
                    WHERE id IN (4,5,6) ORDER BY id
                    """
                )
            ]
            existing_queue_before = tuple(
                connection.execute(
                    "SELECT * FROM review_queue WHERE reason_code='evaluation_gray_zone'"
                ).fetchone()
            )
            directions_before = [
                tuple(row)
                for row in connection.execute(
                    "SELECT id,evaluation_content_direction "
                    "FROM content_items ORDER BY id"
                )
            ]
        applied = self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        self.assertEqual(applied["invalidated_count"], 2)
        self.assertEqual(applied["queues_inserted"], 2)
        with connect(self.db) as connection:
            repaired = connection.execute(
                """
                SELECT id,invalidated_at,invalidation_reason
                FROM evaluation_versions WHERE id IN (1,2) ORDER BY id
                """
            ).fetchall()
            self.assertTrue(all(row["invalidated_at"] for row in repaired))
            self.assertTrue(
                all(
                    str(row["invalidation_reason"]).startswith(
                        INVALIDATION_REASON_PREFIX
                    )
                    for row in repaired
                )
            )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT * FROM evaluation_matches WHERE evaluation_id<=6 ORDER BY evaluation_id"
                    )
                ],
                legacy_match_before,
            )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT * FROM evaluation_versions WHERE id IN (4,5,6) ORDER BY id"
                    )
                ],
                legacy_nonautomatic_before,
            )
            self.assertEqual(
                tuple(
                    connection.execute(
                        "SELECT * FROM review_queue WHERE reason_code='evaluation_gray_zone'"
                    ).fetchone()
                ),
                existing_queue_before,
            )
            queues = connection.execute(
                """
                SELECT content_id,evaluation_id,priority,status FROM review_queue
                WHERE reason_code=? ORDER BY content_id
                """,
                (QUEUE_REASON_CODE,),
            ).fetchall()
            self.assertEqual(
                [tuple(row) for row in queues],
                [(2, 8, 100, "manual_required"), (3, 9, 100, "manual_required")],
            )
            self.assertEqual(
                [
                    tuple(row)
                    for row in connection.execute(
                        "SELECT id,evaluation_content_direction "
                        "FROM content_items ORDER BY id"
                    )
                ],
                directions_before,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM migration_audit WHERE status='succeeded'"
                ).fetchone()[0],
                2,
            )
        reused = self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        self.assertTrue(reused["reused"])
        self.assertEqual(reused["invalidated_count"], 0)
        self.assertEqual(reused["queues_inserted"], 0)

    def test_apply_requires_acknowledgement_and_exact_plan_hash(self) -> None:
        dry_run = self._repair()
        with self.assertRaisesRegex(LegacySceneRepairError, "rollback window"):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
            )
        with self.assertRaisesRegex(LegacySceneRepairError, "does not match"):
            self._repair(
                apply=True,
                expected_plan_sha256="f" * 64,
                acknowledge=True,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
        with self.assertRaisesRegex(LegacySceneRepairError, "only valid with apply"):
            self._repair(acknowledge=True)

    def test_missing_database_error_is_wrapped_and_file_is_not_created(self) -> None:
        missing = self.root / "missing.sqlite3"
        with self.assertRaisesRegex(
            LegacySceneRepairError, "cannot access existing database read-only"
        ):
            repair_legacy_illegal_scene_chains(
                db_path=missing,
                manifest_path=self.manifest_path,
                receipt_path=self.receipt_path,
                operator_reason="remove frozen illegal automatic scene chains",
            )
        self.assertFalse(missing.exists())

    def test_successful_reuse_allows_queue_progress_and_new_v8_evaluation(
        self,
    ) -> None:
        dry_run = self._repair()
        self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        with connect(self.db) as connection:
            queue = connection.execute(
                "SELECT id FROM review_queue WHERE reason_code=? AND content_id=2",
                (QUEUE_REASON_CODE,),
            ).fetchone()
            assert queue is not None
            review_id = connection.execute(
                """
                INSERT INTO evaluation_reviews(
                    queue_id,content_id,previous_evaluation_id,decision,reason,
                    reviewer,created_at
                ) VALUES (?,2,8,'confirm','resolved','tester',
                          '2026-08-05T00:00:00Z')
                """,
                (queue["id"],),
            ).lastrowid
            assert review_id is not None
            self._insert_evaluation(
                connection,
                evaluation_id=11,
                content_id=2,
                release_id="evaluation-v8__selling-points-v5.1",
                source="manual_review",
                code="X1",
                scene="new_car",
                parent_id=8,
                review_id=int(review_id),
                evaluated_at="2026-08-05T00:00:00Z",
            )
            connection.execute(
                "UPDATE evaluation_reviews SET resulting_evaluation_id=11 WHERE id=?",
                (review_id,),
            )
            connection.execute(
                """
                UPDATE review_queue
                SET status='resolved',evaluation_id=11,
                    updated_at='2026-08-05T00:00:00Z',
                    resolved_at='2026-08-05T00:00:00Z'
                WHERE reason_code=? AND content_id=2
                """,
                (QUEUE_REASON_CODE,),
            )
            self._insert_evaluation(
                connection,
                evaluation_id=10,
                content_id=1,
                release_id="evaluation-v8__selling-points-v5.1",
                source="automatic",
                code="X1",
                scene="new_car",
                evaluated_at="2026-08-05T00:01:00Z",
            )
            connection.commit()
        repeated = self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        self.assertTrue(repeated["reused"])
        self.assertEqual(repeated["invalidated_count"], 0)
        self.assertEqual(repeated["queues_inserted"], 0)

    def test_successful_audit_provenance_is_required_for_reuse(self) -> None:
        dry_run = self._repair()
        self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE migration_audit SET baseline_id='tampered' WHERE id=?",
                (self.boundary.audit_id,),
            )
            connection.commit()
        with self.assertRaisesRegex(LegacySceneRepairError, "audit provenance changed"):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )

    def test_self_consistent_audit_plan_cannot_replace_receipt_anchors(self) -> None:
        dry_run = self._repair()
        self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT summary_json FROM migration_audit WHERE id=?",
                (self.boundary.audit_id,),
            ).fetchone()
            assert row is not None
            summary = json.loads(str(row["summary_json"]))
            plan = dict(summary["plan"])
            plan["attested_legacy_evaluation_rows_sha256"] = "0" * 64
            plan.pop("plan_sha256", None)
            tampered_sha256 = repair_module._sha256_json(plan)
            plan["plan_sha256"] = tampered_sha256
            summary["plan"] = plan
            summary["plan_sha256"] = tampered_sha256
            connection.execute(
                "UPDATE migration_audit SET summary_json=? WHERE id=?",
                (repair_module.canonical_json(summary), self.boundary.audit_id),
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "does not match receipt table anchors"
        ):
            self._repair(
                apply=True,
                expected_plan_sha256=tampered_sha256,
                acknowledge=True,
            )

    def test_reuse_rejects_queue_reanchored_to_legacy_evaluation(self) -> None:
        dry_run = self._repair()
        self._repair(
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            acknowledge=True,
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE review_queue
                SET evaluation_id=4,status='resolved',
                    updated_at='2026-08-05T00:00:00Z',
                    resolved_at='2026-08-05T00:00:00Z'
                WHERE reason_code=? AND content_id=2
                """,
                (QUEUE_REASON_CODE,),
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "review queues are incomplete"
        ):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )

    def test_existing_conflict_queue_without_audit_fails_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,priority,status,created_at,updated_at
                ) VALUES (2,8,?,100,'manual_required',?,?)
                """,
                (
                    QUEUE_REASON_CODE,
                    "2026-08-04T09:00:00Z",
                    "2026-08-04T09:00:00Z",
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "protected table changed after activation"
        ):
            self._repair()

    def test_different_invalidation_inside_automatic_chain_fails_closed(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions
                SET invalidated_at='2026-08-04T08:00:00Z',
                    invalidation_reason='another repair'
                WHERE id=1
                """
            )
            connection.commit()
        with self.assertRaisesRegex(
            LegacySceneRepairError, "protected table changed after activation"
        ):
            self._repair()

    def test_fault_after_queue_insert_rolls_back_the_whole_transaction(self) -> None:
        dry_run = self._repair()

        def fail(name: str) -> None:
            if name == "review-queues-inserted":
                raise RuntimeError("injected failure")

        with (
            patch.object(repair_module, "_repair_checkpoint", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "injected failure"),
        ):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE reason_code=?",
                    (QUEUE_REASON_CODE,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM migration_audit").fetchone()[
                    0
                ],
                1,
            )

    def test_fault_after_first_invalidation_rolls_back_the_whole_transaction(
        self,
    ) -> None:
        dry_run = self._repair()

        def fail(name: str) -> None:
            if name == "first-automatic-invalidated":
                raise RuntimeError("injected failure")

        with (
            patch.object(repair_module, "_repair_checkpoint", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "injected failure"),
        ):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions "
                    "WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM review_queue WHERE reason_code=?",
                    (QUEUE_REASON_CODE,),
                ).fetchone()[0],
                0,
            )

    def test_fault_after_audit_insert_rolls_back_the_whole_transaction(self) -> None:
        dry_run = self._repair()

        def fail(name: str) -> None:
            if name == "audit-inserted":
                raise RuntimeError("injected failure")

        with (
            patch.object(repair_module, "_repair_checkpoint", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "injected failure"),
        ):
            self._repair(
                apply=True,
                expected_plan_sha256=str(dry_run["plan_sha256"]),
                acknowledge=True,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions "
                    "WHERE invalidated_at IS NOT NULL"
                ).fetchone()[0],
                0,
            )
            self.assertIsNone(
                connection.execute(
                    "SELECT id FROM migration_audit WHERE id=?",
                    (self.boundary.audit_id,),
                ).fetchone()
            )


if __name__ == "__main__":
    unittest.main()
