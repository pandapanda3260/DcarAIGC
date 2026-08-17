from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8.evaluation import (
    EvaluationError,
    V8_RULE_VERSION,
    build_evidence_envelope,
    evaluate_content,
    evaluate_release_content,
    incremental_candidates,
    resolve_review,
)
from v8.storage import (
    PROJECT_ROOT,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
)
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


class V8ReleaseEvaluationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "release-evaluation.sqlite3"
        source = json.loads(
            (
                PROJECT_ROOT / "config" / "business_selling_points_v4_final.json"
            ).read_text(encoding="utf-8")
        )
        scene_map = {"二手车": "used_car", "新车": "new_car", "媒体-AI小懂": "media"}
        captured_at = "2026-08-04T00:00:00Z"
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5','selling-points-v5.0','published',?,?,?)
                """,
                (source["definition"], captured_at, captured_at),
            )
            for label in source["labels"]:
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition
                    ) VALUES ('taxonomy-v5',?,?,?,?)
                    """,
                    (
                        label["id"],
                        label["tier"],
                        label["label"],
                        label.get("definition") or "",
                    ),
                )
                for scene in label.get("business_scene_options") or [
                    label.get("business_scene")
                ]:
                    normalized = scene_map.get(scene)
                    if normalized:
                        connection.execute(
                            """
                            INSERT INTO selling_point_scenes(selling_point_id,scene)
                            VALUES (?,?)
                            """,
                            (point.lastrowid, normalized),
                        )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            self.content_id = self._insert_content(connection, 1)
            connection.execute(
                """
                UPDATE content_items SET evaluation_content_direction='media'
                WHERE id=?
                """,
                (self.content_id,),
            )
            connection.commit()
        result = backfill_v5_1_matcher_rules(db_path=self.db)
        self.matcher_hash = str(result["matcher_rule_sha256"])
        self.release_id = "evaluation-v8__selling-points-v5.1"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES (?,'evaluation-v8','selling-points-v5.1',?,'backfilling',?,?)
                """,
                (self.release_id, self.matcher_hash, captured_at, captured_at),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_content(self, connection, suffix: int) -> int:
        captured_at = "2026-08-04T00:00:00Z"
        content = connection.execute(
            """
            INSERT INTO content_items(
                link_id,platform,platform_content_id,canonical_url,title,body,
                content_type,imported_at,created_at,updated_at
            ) VALUES (?,'douyin',?,?,?,?,'video',?,?,?)
            """,
            (
                f"V8T00{suffix}",
                f"release-test-{suffix}",
                f"https://www.douyin.com/video/release-test-{suffix}",
                "汽车规则测试",
                "买二手车先看事故车和泡水车检测报告，检查维修记录。",
                captured_at,
                captured_at,
                captured_at,
            ),
        )
        assert content.lastrowid is not None
        content_id = int(content.lastrowid)
        media = self.root / f"{content_id}.mp4"
        media.write_bytes(b"video" * 500)
        asr = self.root / f"{content_id}-asr.json"
        asr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "text": "买二手车先看事故车和泡水车检测报告，检查维修记录。新车正式上市，发布会公布官图和预售信息。",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        ocr = self.root / f"{content_id}-ocr.json"
        ocr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "ocr_observation_count": 2,
                    "combined_text": "事故车泡水车检测报告与新车上市发布会预售信息",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        for artifact_type, path in (("media", media), ("asr", asr), ("ocr", ocr)):
            payload = path.read_bytes()
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id,artifact_type,local_path,status,byte_size,sha256,
                    processor_version,created_at
                ) VALUES (?,?,?,'available',?,?,?,?)
                """,
                (
                    content_id,
                    artifact_type,
                    str(path),
                    len(payload),
                    hashlib.sha256(payload).hexdigest(),
                    "test",
                    captured_at,
                ),
            )
        return content_id

    def _business_counts(self) -> dict[str, int | str | None]:
        with connect(self.db) as connection:
            return {
                "envelopes": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM evidence_envelopes"
                    ).fetchone()[0]
                ),
                "evaluations": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM evaluation_versions"
                    ).fetchone()[0]
                ),
                "matches": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM evaluation_matches"
                    ).fetchone()[0]
                ),
                "provider": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM provider_usage"
                    ).fetchone()[0]
                ),
                "direction": connection.execute(
                    "SELECT evaluation_content_direction FROM content_items WHERE id=?",
                    (self.content_id,),
                ).fetchone()[0],
            }

    def _insert_manual_visual_evidence(self) -> None:
        evidence_text = (
            "画面明确展示AI小懂解释故障报警灯和车辆异响，并给出安全处置建议。"
        )
        captured_at = "2026-08-04T00:30:00Z"
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,reason_code,status,created_at,updated_at,resolved_at
                ) VALUES (?,'manual_visual_fixture','resolved',?,?,?)
                """,
                (self.content_id, captured_at, captured_at, captured_at),
            )
            review = connection.execute(
                """
                INSERT INTO evaluation_reviews(
                    queue_id,content_id,decision,reason,reviewer,created_at
                ) VALUES (?,?,'override','测试人工画面证据','测试复核员',?)
                """,
                (queue.lastrowid, self.content_id, captured_at),
            )
            connection.execute(
                """
                INSERT INTO manual_evidence(
                    review_id,content_id,evidence_type,text_value,sha256,created_at
                ) VALUES (?,?,'visual_summary',?,?,?)
                """,
                (
                    review.lastrowid,
                    self.content_id,
                    evidence_text,
                    hashlib.sha256(evidence_text.encode("utf-8")).hexdigest(),
                    captured_at,
                ),
            )
            connection.commit()

    def _activate_release(self) -> None:
        activated_at = "2026-08-04T01:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at=?,updated_at=? WHERE status='active'
                """,
                (activated_at, activated_at),
            )
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
                (activated_at,),
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='active',activated_at=?,updated_at=? WHERE id=?
                """,
                (activated_at, activated_at, self.release_id),
            )
            connection.commit()

    def test_backfilling_release_uses_database_rules_and_candidate_local_scenes(
        self,
    ) -> None:
        before = self._business_counts()
        with patch(
            "label_douyin_video_evidence_v3.match_points",
            side_effect=AssertionError("legacy matcher must not run for evaluation-v8"),
        ):
            first = evaluate_release_content(
                self.content_id, release_id=self.release_id, db_path=self.db
            )
            second = evaluate_release_content(
                self.content_id, release_id=self.release_id, db_path=self.db
            )
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.evaluation_id, second.evaluation_id)
        with connect(self.db) as connection:
            evaluation = connection.execute(
                "SELECT * FROM evaluation_versions WHERE id=?", (first.evaluation_id,)
            ).fetchone()
            matches = connection.execute(
                """
                SELECT selling_point_code,scene,match_role,score
                FROM evaluation_matches WHERE evaluation_id=?
                ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,rowid
                """,
                (first.evaluation_id,),
            ).fetchall()
            payload = json.loads(str(evaluation["payload_json"]))
            violations = connection.execute("PRAGMA foreign_key_check").fetchall()
        self.assertEqual(evaluation["release_id"], self.release_id)
        self.assertEqual(evaluation["rule_version"], "evaluation-v8")
        self.assertEqual(evaluation["taxonomy_version"], "selling-points-v5.1")
        self.assertEqual(evaluation["matcher_rule_sha256"], self.matcher_hash)
        self.assertEqual(evaluation["content_direction"], "used_car")
        self.assertEqual(
            [
                (row["selling_point_code"], row["scene"], row["match_role"])
                for row in matches
            ],
            [("C1", "used_car", "primary"), ("C4", "new_car", "secondary")],
        )
        self.assertEqual(
            [(item["id"], item["scene"]) for item in payload["matches"]],
            [("C1", "used_car"), ("C4", "new_car")],
        )
        after = self._business_counts()
        self.assertEqual(before["direction"], "media")
        self.assertEqual(after["direction"], "media")
        self.assertEqual(after["provider"], before["provider"])
        self.assertEqual(violations, [])

    def test_backfilling_gray_result_does_not_write_review_queue(self) -> None:
        gray_match = {
            "id": "C1",
            "score": 70,
            "scene": "used_car",
            "reason": "测试灰区",
            "source": "test",
        }
        with patch(
            "v8.evaluation.MaterializedMatcher.match_points",
            return_value=[gray_match],
        ):
            result = evaluate_release_content(
                self.content_id, release_id=self.release_id, db_path=self.db
            )
        with connect(self.db) as connection:
            evaluation = connection.execute(
                "SELECT pending_review FROM evaluation_versions WHERE id=?",
                (result.evaluation_id,),
            ).fetchone()
            queue_count = connection.execute(
                "SELECT COUNT(*) FROM review_queue"
            ).fetchone()[0]
        self.assertEqual(evaluation["pending_review"], 1)
        self.assertEqual(queue_count, 0)

    def test_hash_mismatch_and_non_writable_statuses_fail_before_business_writes(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET matcher_rule_sha256=? WHERE id=?",
                ("f" * 64, self.release_id),
            )
            connection.commit()
        before = self._business_counts()
        with self.assertRaisesRegex(EvaluationError, "matcher hash"):
            evaluate_release_content(
                self.content_id, release_id=self.release_id, db_path=self.db
            )
        self.assertEqual(self._business_counts(), before)

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET matcher_rule_sha256=? WHERE id=?",
                (self.matcher_hash, self.release_id),
            )
            connection.commit()
        for status in ("draft", "ready", "retired", "failed"):
            with self.subTest(status=status), connect(self.db) as connection:
                connection.execute(
                    "UPDATE evaluation_releases SET status=? WHERE id=?",
                    (status, self.release_id),
                )
                connection.commit()
            before = self._business_counts()
            with self.assertRaisesRegex(
                EvaluationError,
                "requires an evaluation-v8 or evaluation-v9 release in backfilling",
            ):
                evaluate_release_content(
                    self.content_id, release_id=self.release_id, db_path=self.db
                )
            self.assertEqual(self._business_counts(), before)
            with connect(self.db) as connection:
                connection.execute(
                    "UPDATE evaluation_releases SET status='backfilling' WHERE id=?",
                    (self.release_id,),
                )
                connection.commit()

        with connect(self.db) as connection:
            legacy_release_id = str(
                connection.execute(
                    "SELECT id FROM evaluation_releases WHERE rule_version='evaluation-v7'"
                ).fetchone()["id"]
            )
            connection.execute(
                "UPDATE evaluation_releases SET status='retired' WHERE id=?",
                (legacy_release_id,),
            )
            connection.execute(
                "UPDATE evaluation_releases SET status='active' WHERE id=?",
                (self.release_id,),
            )
            connection.commit()
        before = self._business_counts()
        with self.assertRaisesRegex(
            EvaluationError,
            "requires an evaluation-v8 or evaluation-v9 release in backfilling",
        ):
            evaluate_release_content(
                self.content_id, release_id=self.release_id, db_path=self.db
            )
        self.assertEqual(self._business_counts(), before)

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET status='backfilling' WHERE id=?",
                (self.release_id,),
            )
            connection.execute(
                "UPDATE evaluation_releases SET status='active' WHERE id=?",
                (legacy_release_id,),
            )
            connection.commit()
        before = self._business_counts()
        with self.assertRaisesRegex(
            EvaluationError,
            "requires an evaluation-v8 or evaluation-v9 release in backfilling",
        ):
            evaluate_release_content(
                self.content_id, release_id=legacy_release_id, db_path=self.db
            )
        self.assertEqual(self._business_counts(), before)

    def test_after_activation_default_evaluator_routes_to_v8_and_updates_cache(
        self,
    ) -> None:
        evaluate_release_content(
            self.content_id, release_id=self.release_id, db_path=self.db
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET status='retired',retired_at=? WHERE status='active'",
                ("2026-08-04T01:00:00Z",),
            )
            connection.execute(
                "UPDATE taxonomy_versions SET status='retired' WHERE version='selling-points-v5.0'"
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                ("2026-08-04T01:00:00Z",),
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='active',activated_at=?,updated_at=? WHERE id=?
                """,
                (
                    "2026-08-04T01:00:00Z",
                    "2026-08-04T01:00:00Z",
                    self.release_id,
                ),
            )
            second_content_id = self._insert_content(connection, 2)
            connection.commit()
        self.assertIn(second_content_id, incremental_candidates(db_path=self.db))
        with patch(
            "label_douyin_video_evidence_v3.match_points",
            side_effect=AssertionError(
                "active evaluation-v8 must use database matcher"
            ),
        ):
            evaluated = evaluate_content(second_content_id, db_path=self.db)
        self.assertTrue(evaluated.created)
        with connect(self.db) as connection:
            row = connection.execute(
                """
                SELECT ev.release_id,ev.rule_version,ev.content_direction,
                       c.evaluation_content_direction
                FROM evaluation_versions ev
                JOIN content_items c ON c.id=ev.content_id
                WHERE ev.id=?
                """,
                (evaluated.evaluation_id,),
            ).fetchone()
        self.assertEqual(row["release_id"], self.release_id)
        self.assertEqual(row["rule_version"], "evaluation-v8")
        self.assertEqual(row["content_direction"], "used_car")
        self.assertEqual(row["evaluation_content_direction"], "used_car")
        self.assertNotIn(second_content_id, incremental_candidates(db_path=self.db))

    def test_backfill_routes_manual_visual_summary_to_visual_matcher_source(
        self,
    ) -> None:
        self._insert_manual_visual_evidence()

        evaluated = evaluate_release_content(
            self.content_id, release_id=self.release_id, db_path=self.db
        )
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT selling_point_code,scene,evidence_json
                FROM evaluation_matches WHERE evaluation_id=?
                ORDER BY CASE match_role WHEN 'primary' THEN 0 ELSE 1 END,rowid
                """,
                (evaluated.evaluation_id,),
            ).fetchall()
        visual_match = next(row for row in rows if row["selling_point_code"] == "M5")
        evidence = json.loads(str(visual_match["evidence_json"]))
        self.assertEqual(visual_match["scene"], "media")
        self.assertEqual(evidence["source"], "关键帧画面语义")
        self.assertIn("AI小懂", evidence["evidence_snippet"])

    def test_automatic_legacy_release_fails_closed_without_matcher(self) -> None:
        before = self._business_counts()
        with patch(
            "label_douyin_video_evidence_v3.match_points",
            side_effect=AssertionError("legacy matcher must not be called"),
        ):
            with self.assertRaisesRegex(
                EvaluationError, "requires a materialized release matcher"
            ):
                evaluate_content(self.content_id, db_path=self.db)
        self.assertEqual(self._business_counts(), before)

    def test_automatic_legacy_release_fails_before_idempotent_reuse(self) -> None:
        with connect(self.db) as connection:
            envelope_id, evidence_sha256, _ = build_evidence_envelope(
                connection, self.content_id, rule_version=V8_RULE_VERSION
            )
            release = connection.execute(
                "SELECT * FROM evaluation_releases WHERE status='active'"
            ).fetchone()
            assert release is not None
            connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id,evidence_envelope_id,release_id,rule_version,
                    taxonomy_version,matcher_rule_sha256,evidence_sha256,
                    evaluation_source,evaluation_status,evidence_level,
                    selling_point_score,selling_point_included,content_direction,
                    pending_review,payload_json,evaluated_at
                ) VALUES (?,?,?,?,?,?,?,'automatic','evaluated','V3',
                          0,0,'unknown',0,'{}',?)
                """,
                (
                    self.content_id,
                    envelope_id,
                    release["id"],
                    release["rule_version"],
                    release["taxonomy_version"],
                    release["matcher_rule_sha256"],
                    evidence_sha256,
                    now_utc(),
                ),
            )
            connection.commit()
        before = self._business_counts()
        with self.assertRaisesRegex(
            EvaluationError, "requires a materialized release matcher"
        ):
            evaluate_content(self.content_id, db_path=self.db)
        self.assertEqual(self._business_counts(), before)

    def test_automatic_legacy_release_fails_before_low_evidence_shortcut(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET status='missing' WHERE content_id=?",
                (self.content_id,),
            )
            connection.commit()
        before = self._business_counts()
        with self.assertRaisesRegex(
            EvaluationError, "requires a materialized release matcher"
        ):
            evaluate_content(self.content_id, db_path=self.db)
        self.assertEqual(self._business_counts(), before)

    def test_confirm_copies_parent_conclusion_without_rematching_manual_evidence(
        self,
    ) -> None:
        self._activate_release()
        automatic = evaluate_content(self.content_id, db_path=self.db)
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
                ) VALUES (?,?,'evaluation_gray_zone','pending',?,?)
                """,
                (self.content_id, automatic.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        confirmed = resolve_review(
            int(queue.lastrowid),
            decision="confirm",
            reason="确认原自动结论，不接受人工证据触发重新匹配",
            reviewer="测试复核员",
            evidence_type="visual_summary",
            evidence_text=(
                "画面明确展示AI小懂解释故障报警灯和车辆异响，并给出安全处置建议。"
            ),
            base_evaluation_id=automatic.evaluation_id,
            db_path=self.db,
        )
        with connect(self.db) as connection:
            child = dict(
                connection.execute(
                    "SELECT * FROM evaluation_versions WHERE id=?",
                    (confirmed.evaluation_id,),
                ).fetchone()
            )
            child_matches = [
                dict(row)
                for row in connection.execute(
                    """
                    SELECT selling_point_code,scene,match_role,score,evidence_json
                    FROM evaluation_matches WHERE evaluation_id=? ORDER BY rowid
                    """,
                    (confirmed.evaluation_id,),
                ).fetchall()
            ]
        for field in (
            "evaluation_status",
            "evidence_level",
            "primary_selling_point_code",
            "selling_point_score",
            "selling_point_included",
            "content_direction",
            "content_automotive_score",
            "audience_automotive_score",
            "acquisition_potential_score",
        ):
            self.assertEqual(child[field], parent[field])
        self.assertEqual(child_matches, parent_matches)
        self.assertEqual(child["evaluation_source"], "manual_review")
        self.assertEqual(child["parent_evaluation_id"], automatic.evaluation_id)
        self.assertEqual(child["pending_review"], 0)
        self.assertNotEqual(child["evidence_sha256"], parent["evidence_sha256"])
        self.assertNotIn(self.content_id, incremental_candidates(db_path=self.db))


if __name__ == "__main__":
    unittest.main()
