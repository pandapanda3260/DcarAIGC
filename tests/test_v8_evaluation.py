from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8.evaluation import (
    EvaluationError,
    V8_RULE_VERSION,
    apply_gray_review_queue_sync,
    evaluate_content,
    evaluate_incremental,
    incremental_candidates,
    plan_gray_review_queue_sync,
    reopen_review,
    resolve_review,
    upgrade_evaluations_to_current_rule,
    upsert_comment_user_scores,
)
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.storage import connect, initialize_database, now_utc
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


class V8EvaluationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "evaluation.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (now_utc(), now_utc()),
            )
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy',?,'other',?,?,'{}')
                    """,
                    (code, f"卖点 {code}", f"定义 {code}"),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            captured_at = "2026-08-01T00:00:00Z"
            for index in (1, 2):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url,
                        title, body, content_type, imported_at, created_at, updated_at
                    ) VALUES (?, 'douyin', ?, ?, '汽车保养知识', '教你判断刹车故障',
                              'video', ?, ?, ?)
                    """,
                    (
                        f"A2BC3{index}",
                        str(index),
                        f"https://www.douyin.com/video/{index}",
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                )
                self._insert_artifacts(
                    connection,
                    index,
                    suffix="initial",
                    created_at="2026-08-02T00:00:00Z",
                )
            connection.commit()
        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE version='selling-points-v5.0'
                """
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES ('evaluation-v8__selling-points-v5.1','evaluation-v8',
                          'selling-points-v5.1',?,'active',?,?,?)
                """,
                (
                    matcher["matcher_rule_sha256"],
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_artifacts(
        self,
        connection,
        content_id: int,
        *,
        suffix: str,
        created_at: str,
        include_media: bool = True,
    ) -> None:
        paths: dict[str, Path] = {}
        if include_media:
            media = self.root / f"{content_id}-{suffix}.mp4"
            media.write_bytes(b"video" * 500)
            paths["media"] = media
        asr = self.root / f"{content_id}-{suffix}-asr.json"
        asr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "text": f"教你汽车刹车轮胎保养维修故障判断方法一定要注意安全驾驶技巧{suffix}",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        ocr = self.root / f"{content_id}-{suffix}-ocr.json"
        ocr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "ocr_observation_count": 2,
                    "combined_text": f"汽车常见故障图解 刹车片 平衡杆 轮胎保养维修方法{suffix}",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        paths.update({"asr": asr, "ocr": ocr})
        for artifact_type, path in paths.items():
            body = path.read_bytes()
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id, artifact_type, local_path, status, byte_size,
                    sha256, processor_version, created_at
                ) VALUES (?, ?, ?, 'available', ?, ?, 'test', ?)
                """,
                (
                    content_id,
                    artifact_type,
                    str(path),
                    len(body),
                    hashlib.sha256(body).hexdigest(),
                    created_at,
                ),
            )

    def _review_as_no_selling_point(self, evaluation_id: int) -> tuple[int, int]:
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'manual_fixture','pending',?,?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
        reviewed = resolve_review(
            queue_id,
            decision="override",
            reason="人工确认当前内容不构成卖点",
            reviewer="测试复核员",
            evidence_type="visual_summary",
            evidence_text="画面证据确认当前内容没有可复用的汽车卖点",
            base_evaluation_id=evaluation_id,
            overrides={
                "primary_selling_point_code": None,
                "selling_point_score": 0,
                "selling_point_included": False,
                "content_automotive_score": 0,
                "content_direction": "other",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            review_id = int(
                connection.execute(
                    "SELECT review_id FROM evaluation_versions WHERE id=?",
                    (reviewed.evaluation_id,),
                ).fetchone()[0]
            )
        return queue_id, review_id

    def test_evaluation_is_append_only_and_idempotent_for_same_evidence(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        second = evaluate_content(1, db_path=self.db)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.evaluation_id, second.evaluation_id)
        self.assertEqual(first.evidence_level, "V3")
        with connect(self.db) as connection:
            evaluation = connection.execute(
                "SELECT * FROM evaluation_versions"
            ).fetchone()
            matches = connection.execute("SELECT * FROM evaluation_matches").fetchall()
        self.assertEqual(evaluation["evaluation_source"], "automatic")
        self.assertEqual(evaluation["taxonomy_version"], "selling-points-v5.1")
        self.assertEqual(matches[0]["selling_point_code"], "C1")

    def test_automatic_gray_evaluation_materializes_one_idempotent_queue(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            first = evaluate_content(1, db_path=self.db)
            second = evaluate_content(1, db_path=self.db)

        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT * FROM review_queue
                WHERE content_id=1 AND reason_code='evaluation_gray_zone'
                """
            ).fetchall()
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["evaluation_id"], first.evaluation_id)
        self.assertEqual(rows[0]["status"], "pending")

    def test_new_gray_evaluation_reopens_resolved_queue_with_audit(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            first = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE review_queue SET status='resolved',resolved_at=?,updated_at=?
                WHERE content_id=1 AND reason_code='evaluation_gray_zone'
                """,
                (now_utc(), now_utc()),
            )
            self._insert_artifacts(
                connection,
                1,
                suffix="new-gray",
                created_at="2099-01-01T00:00:00Z",
                include_media=False,
            )
            connection.commit()
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            second = evaluate_content(1, db_path=self.db)

        with connect(self.db) as connection:
            queue = connection.execute(
                """
                SELECT * FROM review_queue
                WHERE content_id=1 AND reason_code='evaluation_gray_zone'
                """
            ).fetchone()
            reopen_events = connection.execute(
                "SELECT * FROM review_reopen_events WHERE queue_id=?",
                (queue["id"],),
            ).fetchall()
        self.assertTrue(first.created)
        self.assertTrue(second.created)
        self.assertEqual(queue["evaluation_id"], second.evaluation_id)
        self.assertEqual(queue["status"], "pending")
        self.assertIsNone(queue["resolved_at"])
        self.assertEqual(len(reopen_events), 1)
        self.assertEqual(reopen_events[0]["base_evaluation_id"], second.evaluation_id)

    def test_same_gray_evaluation_reopens_a_closed_queue(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            first = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE review_queue SET status='resolved',resolved_at=?,updated_at=?
                WHERE content_id=1 AND reason_code='evaluation_gray_zone'
                """,
                (now_utc(), now_utc()),
            )
            connection.commit()
        plan = plan_gray_review_queue_sync(db_path=self.db)
        self.assertEqual(plan["target_count"], 1)
        self.assertEqual(plan["action_counts"]["reopen"], 1)
        applied = apply_gray_review_queue_sync(
            expected_plan_sha256=plan["plan_sha256"], db_path=self.db
        )
        with connect(self.db) as connection:
            queue = connection.execute(
                "SELECT * FROM review_queue WHERE content_id=1"
            ).fetchone()
            reopen_count = connection.execute(
                "SELECT COUNT(*) FROM review_reopen_events WHERE queue_id=?",
                (queue["id"],),
            ).fetchone()[0]
        self.assertEqual(applied["applied_count"], 1)
        self.assertEqual(queue["evaluation_id"], first.evaluation_id)
        self.assertEqual(queue["status"], "pending")
        self.assertEqual(reopen_count, 1)

    def test_resolved_queue_with_current_cursor_remains_terminal(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            evaluation = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue_id = int(
                connection.execute(
                    "SELECT id FROM review_queue WHERE content_id=1"
                ).fetchone()[0]
            )
            connection.execute(
                """
                UPDATE review_queue SET status='resolved',resolved_at=?,updated_at=?
                WHERE id=?
                """,
                (now_utc(), now_utc(), queue_id),
            )
            connection.commit()

        with self.assertRaisesRegex(EvaluationError, "already resolved"):
            resolve_review(
                queue_id,
                decision="confirm",
                reason="重复提交",
                reviewer="测试复核员",
                evidence_type="review_note",
                evidence_text="重复提交不应重开终态队列",
                base_evaluation_id=evaluation.evaluation_id,
                db_path=self.db,
            )

    def test_automatic_non_gray_results_close_stale_gray_queues(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            evaluate_content(1, db_path=self.db)
            evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            self._insert_artifacts(
                connection,
                1,
                suffix="included",
                created_at="2099-01-01T00:00:00Z",
                include_media=False,
            )
            connection.execute(
                "UPDATE evidence_artifacts SET status='missing' WHERE content_id=2"
            )
            connection.execute(
                "UPDATE content_items SET title='仅有正文的新证据' WHERE id=2"
            )
            connection.commit()
        included_match = {
            "id": "C1",
            "score": 85,
            "reason": "测试明确命中",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[included_match],
        ):
            included = evaluate_content(1, db_path=self.db)
        insufficient = evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            queues = connection.execute(
                """
                SELECT * FROM review_queue
                WHERE reason_code='evaluation_gray_zone' ORDER BY content_id
                """
            ).fetchall()
        self.assertEqual(included.evidence_level, "V3")
        self.assertEqual(insufficient.evidence_level, "V1")
        self.assertEqual([row["status"] for row in queues], ["resolved", "resolved"])
        self.assertEqual(
            [row["evaluation_id"] for row in queues],
            [included.evaluation_id, insufficient.evaluation_id],
        )
        self.assertEqual(
            [row["assigned_to"] for row in queues],
            ["system:evaluation", "system:evaluation"],
        )

    def test_high_confidence_automatic_conflict_reopens_human_conclusion(self) -> None:
        high_match = {
            "id": "C1",
            "score": 87,
            "reason": "测试高置信命中",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            automatic = evaluate_content(1, db_path=self.db)
        manual_queue_id, manual_review_id = self._review_as_no_selling_point(
            automatic.evaluation_id
        )

        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            conflicting = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE review_queue SET priority=1
                WHERE content_id=1 AND reason_code='manual_conclusion_conflict'
                """
            )
            connection.commit()
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            reused = evaluate_content(1, db_path=self.db)

        with connect(self.db) as connection:
            manual_queue = connection.execute(
                "SELECT * FROM review_queue WHERE id=?", (manual_queue_id,)
            ).fetchone()
            conflict_queue = connection.execute(
                """
                SELECT * FROM review_queue
                WHERE content_id=1 AND reason_code='manual_conclusion_conflict'
                """
            ).fetchone()
            reopen_events = connection.execute(
                "SELECT * FROM review_reopen_events WHERE queue_id=?",
                (conflict_queue["id"],),
            ).fetchall()
        self.assertTrue(conflicting.created)
        self.assertFalse(reused.created)
        self.assertEqual(reused.evaluation_id, conflicting.evaluation_id)
        self.assertEqual(manual_queue["status"], "resolved")
        self.assertEqual(conflict_queue["status"], "manual_required")
        self.assertEqual(conflict_queue["priority"], 100)
        self.assertEqual(conflict_queue["evaluation_id"], conflicting.evaluation_id)
        self.assertEqual(len(reopen_events), 1)
        self.assertEqual(reopen_events[0]["previous_review_id"], manual_review_id)
        self.assertEqual(
            reopen_events[0]["base_evaluation_id"], conflicting.evaluation_id
        )

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET body='人工结论一致的新证据' WHERE id=1"
            )
            connection.commit()
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points", return_value=[]
        ):
            matching = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            closed = connection.execute(
                "SELECT * FROM review_queue WHERE id=?", (conflict_queue["id"],)
            ).fetchone()
        self.assertTrue(matching.created)
        self.assertEqual(closed["status"], "resolved")
        self.assertEqual(closed["evaluation_id"], matching.evaluation_id)
        self.assertEqual(closed["assigned_to"], "system:evaluation")

    def test_gray_automatic_result_does_not_duplicate_human_conflict_queue(self) -> None:
        high_match = {
            "id": "C1",
            "score": 87,
            "reason": "测试高置信命中",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            automatic = evaluate_content(1, db_path=self.db)
        self._review_as_no_selling_point(automatic.evaluation_id)
        gray_match = {**high_match, "score": 70, "reason": "测试灰区命中"}
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            reasons = {
                str(row["reason_code"]): str(row["status"])
                for row in connection.execute(
                    "SELECT reason_code,status FROM review_queue WHERE content_id=1"
                )
            }
        self.assertEqual(reasons["evaluation_gray_zone"], "pending")
        self.assertNotIn("manual_conclusion_conflict", reasons)

    def test_automatic_result_matching_human_conclusion_needs_no_conflict_queue(
        self,
    ) -> None:
        high_match = {
            "id": "C1",
            "score": 87,
            "reason": "测试高置信命中",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'manual_fixture','pending',?,?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        resolve_review(
            int(queue.lastrowid),
            decision="confirm",
            reason="人工确认自动结论",
            reviewer="测试复核员",
            evidence_type="review_note",
            evidence_text="人工证据确认 C1 媒体场景结论",
            base_evaluation_id=automatic.evaluation_id,
            db_path=self.db,
        )
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[high_match],
        ):
            evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            conflict_count = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM review_queue
                    WHERE reason_code='manual_conclusion_conflict'
                    """
                ).fetchone()[0]
            )
        self.assertEqual(conflict_count, 0)

    def test_gray_queue_sync_requires_plan_hash_and_converges_idempotently(
        self,
    ) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            evaluation = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                "DELETE FROM review_queue WHERE content_id=1"
            )
            connection.commit()

        plan = plan_gray_review_queue_sync(db_path=self.db)
        self.assertEqual(plan["target_count"], 1)
        self.assertEqual(plan["action_counts"]["create"], 1)
        with self.assertRaisesRegex(EvaluationError, "plan hash changed"):
            apply_gray_review_queue_sync(
                expected_plan_sha256="0" * 64,
                db_path=self.db,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0],
                0,
            )

        applied = apply_gray_review_queue_sync(
            expected_plan_sha256=plan["plan_sha256"],
            db_path=self.db,
        )
        self.assertEqual(applied["applied_count"], 1)
        self.assertEqual(applied["remaining_target_count"], 0)
        with connect(self.db) as connection:
            queue = connection.execute("SELECT * FROM review_queue").fetchone()
        self.assertEqual(queue["evaluation_id"], evaluation.evaluation_id)
        self.assertEqual(queue["status"], "pending")

        with self.assertRaisesRegex(EvaluationError, "plan hash changed"):
            apply_gray_review_queue_sync(
                expected_plan_sha256=plan["plan_sha256"],
                db_path=self.db,
            )
        empty_plan = plan_gray_review_queue_sync(db_path=self.db)
        repeated = apply_gray_review_queue_sync(
            expected_plan_sha256=empty_plan["plan_sha256"], db_path=self.db
        )
        self.assertEqual(repeated["applied_count"], 0)
        self.assertEqual(repeated["sqlite_changes"], 0)
        self.assertTrue(repeated["reused"])

    def test_gray_queue_sync_resolves_an_active_queue_after_gray_exit(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "reason": "测试灰区",
            "scene": "media",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            evaluation = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions SET pending_review=0
                WHERE id=?
                """,
                (evaluation.evaluation_id,),
            )
            connection.commit()

        plan = plan_gray_review_queue_sync(db_path=self.db)
        self.assertEqual(plan["target_count"], 1)
        self.assertEqual(plan["action_counts"]["resolve"], 1)
        applied = apply_gray_review_queue_sync(
            expected_plan_sha256=plan["plan_sha256"], db_path=self.db
        )
        with connect(self.db) as connection:
            queue = connection.execute("SELECT * FROM review_queue").fetchone()
        self.assertEqual(applied["applied_count"], 1)
        self.assertEqual(queue["evaluation_id"], evaluation.evaluation_id)
        self.assertEqual(queue["status"], "resolved")
        self.assertEqual(queue["assigned_to"], "system:evaluation")

    def test_incremental_candidates_only_include_changed_evidence(self) -> None:
        evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        self.assertEqual(incremental_candidates(db_path=self.db), [])
        with connect(self.db) as connection:
            self._insert_artifacts(
                connection,
                2,
                suffix="updated",
                created_at="2099-01-01T00:00:00Z",
                include_media=False,
            )
            connection.commit()
        self.assertEqual(incremental_candidates(db_path=self.db), [2])
        updated = evaluate_content(2, db_path=self.db)
        self.assertTrue(updated.created)
        with connect(self.db) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=2"
            ).fetchone()[0]
        self.assertEqual(count, 2)

    def test_content_type_change_creates_a_new_evidence_evaluation(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET content_type='image' WHERE id=1"
            )
            connection.commit()
        self.assertEqual(incremental_candidates(db_path=self.db), [1])
        second = evaluate_content(1, db_path=self.db)
        self.assertTrue(second.created)
        self.assertNotEqual(second.evidence_sha256, first.evidence_sha256)
        self.assertEqual(second.evidence_level, "V2")

    def test_invalidated_current_evidence_key_requires_a_new_release(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions
                SET invalidated_at=?,invalidation_reason='test invalidation'
                WHERE id=?
                """,
                (now_utc(), first.evaluation_id),
            )
            connection.commit()
        with self.assertRaisesRegex(
            EvaluationError, "create a new release before reevaluating: 1"
        ):
            incremental_candidates(db_path=self.db)
        with self.assertRaisesRegex(
            EvaluationError, "create a new release before reevaluating: 1"
        ):
            evaluate_incremental(db_path=self.db)
        with connect(self.db) as connection:
            count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=1"
                ).fetchone()[0]
            )
        self.assertEqual(count, 1)

    def test_incremental_candidates_ignore_timestamp_churn_when_evidence_hash_is_unchanged(
        self,
    ) -> None:
        comment_sha256 = "c" * 64
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, status, created_at
                ) VALUES (1, '2026-08-02T00:00:00Z', '2026-W31', 'test',
                          'comments-w31.json', ?, 'available', '2026-08-02T00:00:00Z')
                """,
                (comment_sha256,),
            )
            connection.commit()
        evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)

        with connect(self.db) as connection:
            latest_asr = connection.execute(
                """
                SELECT local_path,byte_size,sha256 FROM evidence_artifacts
                WHERE content_id=1 AND artifact_type='asr'
                ORDER BY id DESC LIMIT 1
                """
            ).fetchone()
            duplicate_asr = self.root / "1-timestamp-only-asr.json"
            duplicate_asr.write_bytes(Path(str(latest_asr["local_path"])).read_bytes())
            connection.execute(
                "UPDATE content_items SET updated_at='2099-01-01T00:00:00Z' WHERE id=1"
            )
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id, artifact_type, local_path, status, byte_size,
                    sha256, processor_version, created_at
                ) VALUES (1, 'asr', ?, 'available', ?, ?, 'timestamp-only',
                          '2099-01-01T00:00:00Z')
                """,
                (
                    str(duplicate_asr),
                    int(latest_asr["byte_size"]),
                    str(latest_asr["sha256"]),
                ),
            )
            connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, status, created_at
                ) VALUES (1, '2099-01-01T00:00:00Z', '2099-W01', 'test',
                          'comments-w01.json', ?, 'available', '2099-01-01T00:00:00Z')
                """,
                (comment_sha256,),
            )
            connection.commit()

        reused = evaluate_content(1, db_path=self.db)
        self.assertFalse(reused.created)
        self.assertEqual(incremental_candidates(db_path=self.db), [])
        self.assertEqual(incremental_candidates(db_path=self.db), [])

    def test_incremental_candidates_detect_hash_change_even_when_timestamps_are_stale(
        self,
    ) -> None:
        evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE content_items
                SET title='汽车保养知识发生真实变化',
                    updated_at='2026-08-01T00:00:00Z'
                WHERE id=1
                """
            )
            connection.commit()

        self.assertEqual(incremental_candidates(db_path=self.db), [1])

    def test_statistics_raw_response_and_media_source_do_not_change_evidence(
        self,
    ) -> None:
        first = evaluate_content(1, db_path=self.db)
        source = self.root / "source.json"
        source.write_text(
            '{"urls":["https://cdn.example/video.mp4"]}', encoding="utf-8"
        )
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    content_id, provider, operation, local_path, sha256, byte_size,
                    http_status, captured_at, source
                ) VALUES (1, 'TikHub', 'douyin_video_statistics', 'statistics.json',
                          ?, 10, 200, '2099-01-01T00:00:00Z', 'live')
                """,
                ("9" * 64,),
            )
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id, artifact_type, local_path, status, byte_size,
                    sha256, processor_version, created_at
                ) VALUES (1, 'media_source', ?, 'available', ?, ?, 'provider-media-source-v8.0',
                          '2099-01-01T00:00:00Z')
                """,
                (
                    str(source),
                    source.stat().st_size,
                    hashlib.sha256(source.read_bytes()).hexdigest(),
                ),
            )
            connection.commit()
        second = evaluate_content(1, db_path=self.db)
        self.assertFalse(second.created)
        self.assertEqual(second.evaluation_id, first.evaluation_id)
        self.assertEqual(second.evidence_sha256, first.evidence_sha256)
        self.assertEqual(incremental_candidates(db_path=self.db), [2])
        with connect(self.db) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=1"
            ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_current_schema_initialization_does_not_rewrite_evaluations(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        captured_at = "2099-01-01T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO provider_raw_responses(
                    content_id, provider, operation, local_path, sha256, byte_size,
                    http_status, captured_at, source
                ) VALUES (1, 'TikHub', 'douyin_video_statistics', 'statistics.json',
                          ?, 10, 200, ?, 'live')
                """,
                ("9" * 64, captured_at),
            )
            connection.commit()
            before = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (first.evaluation_id,),
                ).fetchone()
            )
            changes_before = connection.total_changes
            initialize_database(connection)
            after = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (first.evaluation_id,),
                ).fetchone()
            )
            self.assertEqual(connection.total_changes, changes_before)
        self.assertEqual(after, before)
        self.assertIsNone(after["invalidated_at"])
        self.assertEqual(incremental_candidates(db_path=self.db), [2])
        current = evaluate_content(1, db_path=self.db)
        self.assertFalse(current.created)
        self.assertEqual(current.evaluation_id, first.evaluation_id)

    def test_comment_scores_upsert_without_deleting_other_users(self) -> None:
        with connect(self.db) as connection:
            evidence = connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, status, created_at
                ) VALUES (1, ?, '2026-W31', 'test', 'comments.json', ?, 'available', ?)
                """,
                (now_utc(), "a" * 64, now_utc()),
            )
            connection.commit()
            evidence_id = int(evidence.lastrowid)
        upsert_comment_user_scores(
            1,
            evidence_id,
            [
                {
                    "anonymous_user_key": "u1",
                    "audience_automotive_score": 40,
                    "action_intent_score": 10,
                },
                {
                    "anonymous_user_key": "u2",
                    "audience_automotive_score": 50,
                    "action_intent_score": 20,
                },
            ],
            db_path=self.db,
        )
        upsert_comment_user_scores(
            1,
            evidence_id,
            [
                {
                    "anonymous_user_key": "u1",
                    "audience_automotive_score": 80,
                    "action_intent_score": 60,
                }
            ],
            db_path=self.db,
        )
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT anonymous_user_key, audience_automotive_score FROM comment_user_scores ORDER BY 1"
            ).fetchall()
        self.assertEqual([(row[0], row[1]) for row in rows], [("u1", 80), ("u2", 50)])

    def test_five_commenters_are_scorable_and_four_are_excluded(self) -> None:
        for content_id, user_count in ((1, 5), (2, 4)):
            with connect(self.db) as connection:
                evidence = connection.execute(
                    """
                    INSERT INTO comment_evidence_versions(
                        content_id, captured_at, iso_week, source, local_path,
                        sha256, status, created_at
                    ) VALUES (?, ?, '2026-W31', 'test', ?, ?, 'available', ?)
                    """,
                    (
                        content_id,
                        now_utc(),
                        f"comments-{content_id}.json",
                        str(content_id) * 64,
                        now_utc(),
                    ),
                )
                connection.commit()
            upsert_comment_user_scores(
                content_id,
                int(evidence.lastrowid),
                [
                    {
                        "anonymous_user_key": f"u-{content_id}-{index}",
                        "audience_automotive_score": 60,
                        "action_intent_score": 80,
                    }
                    for index in range(user_count)
                ],
                db_path=self.db,
            )

        evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            five = connection.execute(
                """
                SELECT audience_automotive_score,acquisition_potential_score,payload_json
                FROM evaluation_versions WHERE content_id=1 AND rule_version=?
                """,
                (V8_RULE_VERSION,),
            ).fetchone()
            four = connection.execute(
                """
                SELECT audience_automotive_score,acquisition_potential_score,payload_json
                FROM evaluation_versions WHERE content_id=2 AND rule_version=?
                """,
                (V8_RULE_VERSION,),
            ).fetchone()
        self.assertEqual(five["audience_automotive_score"], 60)
        self.assertIsNotNone(five["acquisition_potential_score"])
        self.assertEqual(json.loads(five["payload_json"])["valid_unique_commenters"], 5)
        self.assertIsNone(four["audience_automotive_score"])
        self.assertIsNone(four["acquisition_potential_score"])
        self.assertEqual(json.loads(four["payload_json"])["valid_unique_commenters"], 4)

    def test_rule_upgrade_copy_path_is_hard_disabled(self) -> None:
        previous = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            evidence = connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id, captured_at, iso_week, source, local_path,
                    sha256, status, created_at
                ) VALUES (1, ?, '2026-W31', 'test', 'manual-comments.json', ?,
                          'available', ?)
                """,
                (now_utc(), "f" * 64, now_utc()),
            )
            connection.commit()
            evidence_id = int(evidence.lastrowid)
        upsert_comment_user_scores(
            1,
            evidence_id,
            [
                {
                    "anonymous_user_key": f"manual-{index}",
                    "audience_automotive_score": 70,
                    "action_intent_score": 80,
                }
                for index in range(5)
            ],
            db_path=self.db,
        )

        with connect(self.db) as connection:
            before = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (previous.evaluation_id,),
                ).fetchone()
            )

        first = upgrade_evaluations_to_current_rule(db_path=self.db)
        second = upgrade_evaluations_to_current_rule(
            db_path=self.db, content_ids=[1], limit=1
        )
        expected = {
            "candidates": 0,
            "created": 0,
            "reused": 0,
            "content_ids": [],
            "disabled": True,
        }
        self.assertEqual(first, expected)
        self.assertEqual(second, expected)
        with connect(self.db) as connection:
            after = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (previous.evaluation_id,),
                ).fetchone()
            )
            version_count = connection.execute(
                "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=1"
            ).fetchone()[0]
        self.assertEqual(after, before)
        self.assertEqual(version_count, 1)

    def test_manual_review_adds_evidence_and_new_evaluation_version(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
        reviewed = resolve_review(
            queue_id,
            decision="override",
            reason="关键帧明确提供汽车故障解决方法",
            reviewer="测试复核员",
            evidence_type="visual_summary",
            evidence_text="画面连续展示刹车片和轮胎故障排查方法",
            base_evaluation_id=automatic.evaluation_id,
            overrides={
                "primary_selling_point_code": "C1",
                "selling_point_score": 92,
                "selling_point_included": True,
                "content_automotive_score": 95,
                "content_direction": "media",
            },
            db_path=self.db,
        )
        self.assertTrue(reviewed.created)
        self.assertNotEqual(reviewed.evaluation_id, automatic.evaluation_id)
        with connect(self.db) as connection:
            versions = connection.execute(
                """
                SELECT evaluation_source,review_id,parent_evaluation_id,release_id
                FROM evaluation_versions WHERE content_id=1 ORDER BY id
                """
            ).fetchall()
            queue_row = connection.execute(
                "SELECT * FROM review_queue WHERE id=?", (queue_id,)
            ).fetchone()
            review = connection.execute(
                "SELECT * FROM evaluation_reviews WHERE queue_id=?", (queue_id,)
            ).fetchone()
            evidence_count = connection.execute(
                "SELECT COUNT(*) FROM manual_evidence"
            ).fetchone()[0]
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(
            [row["evaluation_source"] for row in versions],
            ["automatic", "manual_review"],
        )
        self.assertEqual(versions[1]["review_id"], review["id"])
        self.assertEqual(versions[1]["parent_evaluation_id"], automatic.evaluation_id)
        self.assertEqual(versions[1]["release_id"], versions[0]["release_id"])
        self.assertEqual(queue_row["status"], "resolved")
        self.assertEqual(queue_row["evaluation_id"], reviewed.evaluation_id)
        self.assertEqual(review["previous_evaluation_id"], automatic.evaluation_id)
        self.assertEqual(review["resulting_evaluation_id"], reviewed.evaluation_id)
        self.assertEqual(evidence_count, 1)
        self.assertEqual(violations, [])
        self.assertEqual(incremental_candidates(db_path=self.db), [])
        incremental = evaluate_incremental(db_path=self.db)
        self.assertEqual(incremental["candidates"], 0)
        with connect(self.db) as connection:
            content_one_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=1"
                ).fetchone()[0]
            )
        self.assertEqual(content_one_count, 2)
        with self.assertRaises(EvaluationError):
            resolve_review(
                queue_id,
                decision="confirm",
                reason="重复提交",
                reviewer="测试复核员",
                evidence_type="review_note",
                evidence_text="重复证据",
                base_evaluation_id=automatic.evaluation_id,
                db_path=self.db,
            )

    def test_manual_override_without_selling_point_clears_automatic_matches(
        self,
    ) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
        reviewed = resolve_review(
            queue_id,
            decision="override",
            reason="本地媒体确认是非汽车旅行内容",
            reviewer="测试复核员",
            evidence_type="visual_summary",
            evidence_text="画面仅展示民宿、沙滩与泳池，不包含汽车产品或用车任务",
            base_evaluation_id=automatic.evaluation_id,
            overrides={
                "primary_selling_point_code": None,
                "selling_point_score": 0,
                "selling_point_included": False,
                "content_automotive_score": 0,
                "content_direction": "other",
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            row = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?",
                (reviewed.evaluation_id,),
            ).fetchone()
            matches = connection.execute(
                "SELECT * FROM evaluation_matches WHERE evaluation_id=?",
                (reviewed.evaluation_id,),
            ).fetchall()
        self.assertIsNone(row["primary_selling_point_code"])
        self.assertEqual(row["selling_point_score"], 0)
        self.assertEqual(matches, [])

        reopened = reopen_review(
            queue_id,
            reason="验证空主卖点的不变量",
            reopened_by="测试复核员",
            db_path=self.db,
        )
        with connect(self.db) as connection:
            before = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "evaluation_reviews",
                    "manual_evidence",
                    "evaluation_versions",
                )
            }
        for overrides in (
            {"selling_point_score": 70},
            {"selling_point_included": True},
        ):
            with (
                self.subTest(overrides=overrides),
                self.assertRaisesRegex(
                    EvaluationError, "without a primary selling point"
                ),
            ):
                resolve_review(
                    queue_id,
                    decision="override",
                    reason="空主卖点不能单独获得卖点分或计入",
                    reviewer="测试复核员",
                    evidence_type="review_note",
                    evidence_text="这条失败证据必须随事务完整回滚",
                    base_evaluation_id=reopened.base_evaluation_id,
                    overrides=overrides,
                    db_path=self.db,
                )
        with connect(self.db) as connection:
            after = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in before
            }
        self.assertEqual(after, before)

    def test_partial_override_inherits_parent_and_preserves_explicit_zero(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            parent = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (automatic.evaluation_id,),
                ).fetchone()
            )
            parent_matches = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT selling_point_code,scene,match_role,score,evidence_json
                    FROM evaluation_matches WHERE evaluation_id=? ORDER BY rowid
                    """,
                    (automatic.evaluation_id,),
                ).fetchall()
            ]
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        first = resolve_review(
            int(queue.lastrowid),
            decision="override",
            reason="只修正内容垂直度",
            reviewer="测试复核员",
            evidence_type="review_note",
            evidence_text="人工核验后内容垂直度应为十二分",
            base_evaluation_id=automatic.evaluation_id,
            overrides={"content_automotive_score": 12},
            db_path=self.db,
        )
        with connect(self.db) as connection:
            first_row = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (first.evaluation_id,),
                ).fetchone()
            )
            first_matches = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT selling_point_code,scene,match_role,score,evidence_json
                    FROM evaluation_matches WHERE evaluation_id=? ORDER BY rowid
                    """,
                    (first.evaluation_id,),
                ).fetchall()
            ]
        for field in (
            "primary_selling_point_code",
            "selling_point_score",
            "selling_point_included",
            "content_direction",
            "evidence_level",
        ):
            self.assertEqual(first_row[field], parent[field])
        self.assertEqual(first_row["content_automotive_score"], 12)
        self.assertEqual(first_matches, parent_matches)

        reopened = reopen_review(
            int(queue.lastrowid),
            reason="继续核对卖点分",
            reopened_by="测试复核员",
            db_path=self.db,
        )
        second = resolve_review(
            int(queue.lastrowid),
            decision="override",
            reason="卖点仍存在但本次人工评分为零",
            reviewer="测试复核员",
            evidence_type="review_note",
            evidence_text="人工复核明确记录零分结果",
            base_evaluation_id=reopened.base_evaluation_id,
            overrides={
                "selling_point_score": 0,
                "selling_point_included": False,
            },
            db_path=self.db,
        )
        with connect(self.db) as connection:
            second_row = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?",
                (second.evaluation_id,),
            ).fetchone()
            second_match = connection.execute(
                "SELECT * FROM evaluation_matches WHERE evaluation_id=?",
                (second.evaluation_id,),
            ).fetchone()
        self.assertEqual(
            second_row["primary_selling_point_code"],
            parent["primary_selling_point_code"],
        )
        self.assertEqual(second_row["selling_point_score"], 0)
        self.assertEqual(second_row["selling_point_included"], 0)
        self.assertEqual(second_row["content_automotive_score"], 12)
        self.assertEqual(second_match["score"], 0)

    def test_review_resolution_failure_rolls_back_the_entire_audit_chain(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
            envelope_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_envelopes"
                ).fetchone()[0]
            )

        with patch(
            "v8.evaluation.build_evidence_envelope",
            side_effect=EvaluationError("injected evaluation failure"),
        ):
            with self.assertRaisesRegex(EvaluationError, "injected evaluation failure"):
                resolve_review(
                    queue_id,
                    decision="confirm",
                    reason="验证事务回滚",
                    reviewer="测试复核员",
                    evidence_type="review_note",
                    evidence_text="这条人工证据会随失败事务完整回滚",
                    base_evaluation_id=automatic.evaluation_id,
                    db_path=self.db,
                )

        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_reviews"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM manual_evidence").fetchone()[
                    0
                ],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_envelopes"
                ).fetchone()[0],
                envelope_count,
            )
            queue_row = connection.execute(
                "SELECT * FROM review_queue WHERE id=?", (queue_id,)
            ).fetchone()
        self.assertEqual(queue_row["status"], "pending")
        self.assertEqual(queue_row["evaluation_id"], automatic.evaluation_id)

    def test_review_queue_completion_failure_rolls_back_every_new_row(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.execute(
                """
                CREATE TRIGGER reject_review_completion
                BEFORE UPDATE OF status ON review_queue
                WHEN NEW.status='resolved'
                BEGIN
                    SELECT RAISE(ABORT, 'injected queue completion failure');
                END
                """
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
            envelope_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evidence_envelopes"
                ).fetchone()[0]
            )

        with self.assertRaisesRegex(sqlite3.IntegrityError, "injected queue"):
            resolve_review(
                queue_id,
                decision="confirm",
                reason="验证最终队列写入失败",
                reviewer="测试复核员",
                evidence_type="review_note",
                evidence_text="整个复核链必须随最后一步失败一起回滚",
                base_evaluation_id=automatic.evaluation_id,
                db_path=self.db,
            )

        with connect(self.db) as connection:
            counts = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "evaluation_reviews",
                    "manual_evidence",
                    "evaluation_versions",
                    "evaluation_matches",
                    "evidence_envelopes",
                )
            }
            queue_row = connection.execute(
                "SELECT * FROM review_queue WHERE id=?", (queue_id,)
            ).fetchone()
        self.assertEqual(counts["evaluation_reviews"], 0)
        self.assertEqual(counts["manual_evidence"], 0)
        self.assertEqual(counts["evaluation_versions"], 1)
        self.assertEqual(counts["evaluation_matches"], 1)
        self.assertEqual(counts["evidence_envelopes"], envelope_count)
        self.assertEqual(queue_row["status"], "pending")
        self.assertEqual(queue_row["evaluation_id"], automatic.evaluation_id)

    def test_automatic_evaluation_rejects_manual_lineage_fields(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with self.assertRaisesRegex(EvaluationError, "review_id is only valid"):
            evaluate_content(2, review_id=1, db_path=self.db)
        with self.assertRaisesRegex(
            EvaluationError, "parent_evaluation_id is only valid"
        ):
            evaluate_content(
                2,
                parent_evaluation_id=automatic.evaluation_id,
                db_path=self.db,
            )
        with self.assertRaisesRegex(EvaluationError, "does not accept manual_override"):
            evaluate_content(
                2,
                manual_override={
                    "decision": "override",
                    "primary_selling_point_code": None,
                    "content_direction": "other",
                },
                db_path=self.db,
            )
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE content_id=2"
                ).fetchone()[0],
                0,
            )

    def test_reopen_queue_update_failure_rolls_back_the_audit_event(self) -> None:
        automatic = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            queue_id = int(queue.lastrowid)
        reviewed = resolve_review(
            queue_id,
            decision="confirm",
            reason="先完成首次复核",
            reviewer="测试复核员",
            evidence_type="review_note",
            evidence_text="首次复核证据",
            base_evaluation_id=automatic.evaluation_id,
            db_path=self.db,
        )
        with connect(self.db) as connection:
            before = dict(
                connection.execute(
                    "SELECT * FROM review_queue WHERE id=?", (queue_id,)
                ).fetchone()
            )
            connection.execute(
                """
                CREATE TRIGGER reject_review_reopen
                BEFORE UPDATE OF status ON review_queue
                WHEN NEW.status='in_review'
                BEGIN
                    SELECT RAISE(ABORT, 'injected reopen failure');
                END
                """
            )
            connection.commit()

        with self.assertRaisesRegex(sqlite3.IntegrityError, "injected reopen"):
            reopen_review(
                queue_id,
                reason="验证重开事务回滚",
                reopened_by="测试复核员",
                db_path=self.db,
            )
        with connect(self.db) as connection:
            after = dict(
                connection.execute(
                    "SELECT * FROM review_queue WHERE id=?", (queue_id,)
                ).fetchone()
            )
            event_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM review_reopen_events"
                ).fetchone()[0]
            )
        self.assertEqual(after, before)
        self.assertEqual(after["evaluation_id"], reviewed.evaluation_id)
        self.assertEqual(event_count, 0)


if __name__ == "__main__":
    unittest.main()
