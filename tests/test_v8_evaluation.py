from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from v8.evaluation import (
    EvaluationError,
    evaluate_content,
    incremental_candidates,
    resolve_review,
    upsert_comment_user_scores,
)
from v8.storage import connect, initialize_database, now_utc


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
            point = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, positive_evidence_json
                ) VALUES ('taxonomy', 'C1', 'other', '汽车知识', '["保养","维修"]')
                """
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, 'media')",
                (point.lastrowid,),
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
                        f"A2BC3{index}", str(index), f"https://www.douyin.com/video/{index}",
                        captured_at, captured_at, captured_at,
                    ),
                )
                self._insert_artifacts(connection, index, suffix="initial", created_at="2026-08-02T00:00:00Z")
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
                    content_id, artifact_type, str(path), len(body),
                    hashlib.sha256(body).hexdigest(), created_at,
                ),
            )

    def test_evaluation_is_append_only_and_idempotent_for_same_evidence(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        second = evaluate_content(1, db_path=self.db)
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.evaluation_id, second.evaluation_id)
        self.assertEqual(first.evidence_level, "V3")
        with connect(self.db) as connection:
            evaluation = connection.execute("SELECT * FROM evaluation_versions").fetchone()
            matches = connection.execute("SELECT * FROM evaluation_matches").fetchall()
        self.assertEqual(evaluation["evaluation_source"], "automatic")
        self.assertEqual(evaluation["taxonomy_version"], "selling-points-v5.0")
        self.assertEqual(matches[0]["selling_point_code"], "C1")

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

    def test_statistics_raw_response_and_media_source_do_not_change_evidence(self) -> None:
        first = evaluate_content(1, db_path=self.db)
        source = self.root / "source.json"
        source.write_text('{"urls":["https://cdn.example/video.mp4"]}', encoding="utf-8")
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
                    str(source), source.stat().st_size,
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

    def test_existing_statistics_polluted_version_is_invalidated_and_not_current(self) -> None:
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
            envelope = connection.execute(
                """
                INSERT INTO evidence_envelopes(
                    content_id, schema_version, detail_raw_sha256, text_sha256,
                    evidence_sha256, components_json, created_at
                ) VALUES (1, 'evidence-v1', ?, ?, ?, '{}', ?)
                """,
                ("9" * 64, "8" * 64, "7" * 64, captured_at),
            )
            polluted = connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id, evidence_envelope_id, rule_version, taxonomy_version,
                    evidence_sha256, evaluation_source, evaluation_status, evidence_level,
                    payload_json, evaluated_at
                ) VALUES (1, ?, 'evaluation-v6', 'selling-points-v5.0', ?,
                          'automatic', 'evaluated', 'V3', '{}', ?)
                """,
                (envelope.lastrowid, "7" * 64, captured_at),
            )
            connection.commit()
            initialize_database(connection)
            invalidated = connection.execute(
                "SELECT invalidated_at FROM evaluation_versions WHERE id=?",
                (polluted.lastrowid,),
            ).fetchone()
        self.assertIsNotNone(invalidated["invalidated_at"])
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
                {"anonymous_user_key": "u1", "audience_automotive_score": 40, "action_intent_score": 10},
                {"anonymous_user_key": "u2", "audience_automotive_score": 50, "action_intent_score": 20},
            ],
            db_path=self.db,
        )
        upsert_comment_user_scores(
            1,
            evidence_id,
            [{"anonymous_user_key": "u1", "audience_automotive_score": 80, "action_intent_score": 60}],
            db_path=self.db,
        )
        with connect(self.db) as connection:
            rows = connection.execute(
                "SELECT anonymous_user_key, audience_automotive_score FROM comment_user_scores ORDER BY 1"
            ).fetchall()
        self.assertEqual([(row[0], row[1]) for row in rows], [("u1", 80), ("u2", 50)])

    def test_manual_review_adds_evidence_and_new_evaluation_version(self) -> None:
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
            reason="关键帧明确提供汽车故障解决方法",
            reviewer="测试复核员",
            evidence_type="visual_summary",
            evidence_text="画面连续展示刹车片和轮胎故障排查方法",
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
                "SELECT evaluation_source FROM evaluation_versions WHERE content_id=1 ORDER BY id"
            ).fetchall()
            queue_row = connection.execute("SELECT * FROM review_queue WHERE id=?", (queue_id,)).fetchone()
            review = connection.execute("SELECT * FROM evaluation_reviews WHERE queue_id=?", (queue_id,)).fetchone()
            evidence_count = connection.execute("SELECT COUNT(*) FROM manual_evidence").fetchone()[0]
        self.assertEqual([row[0] for row in versions], ["automatic", "manual_review"])
        self.assertEqual(queue_row["status"], "resolved")
        self.assertEqual(review["resulting_evaluation_id"], reviewed.evaluation_id)
        self.assertEqual(evidence_count, 1)
        with self.assertRaises(EvaluationError):
            resolve_review(
                queue_id,
                decision="confirm",
                reason="重复提交",
                reviewer="测试复核员",
                evidence_type="review_note",
                evidence_text="重复证据",
                db_path=self.db,
            )


if __name__ == "__main__":
    unittest.main()
