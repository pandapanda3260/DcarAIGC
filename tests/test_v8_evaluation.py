from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from v8.evaluation import (
    EvaluationError,
    V8_RULE_VERSION,
    evaluate_content,
    evaluate_incremental,
    incremental_candidates,
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

    def test_expected_active_release_fails_before_any_write_after_switch(self) -> None:
        expected_release_id = "evaluation-v8__selling-points-v5.1"
        switched_release_id = "evaluation-v8__selling-points-v5.2"
        switched_at = "2026-08-03T00:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at=?,updated_at=?
                WHERE id=?
                """,
                (switched_at, switched_at, expected_release_id),
            )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5.2','selling-points-v5.2','published',
                          'release-switch-test',?,?)
                """,
                (switched_at, switched_at),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (?,'evaluation-v8','selling-points-v5.2',?,'active',?,?,?)
                """,
                (switched_release_id, "b" * 64, switched_at, switched_at, switched_at),
            )
            before = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in (
                    "evidence_envelopes",
                    "evaluation_versions",
                    "evaluation_matches",
                )
            }
            connection.commit()

        with self.assertRaisesRegex(
            EvaluationError,
            "active evaluation release changed before evaluation write",
        ):
            evaluate_content(
                1,
                db_path=self.db,
                expected_active_release_id=expected_release_id,
            )

        with connect(self.db) as connection:
            after = {
                table: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in before
            }
            switched_evaluations = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE release_id=?",
                    (switched_release_id,),
                ).fetchone()[0]
            )
        self.assertEqual(after, before)
        self.assertEqual(switched_evaluations, 0)

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

if __name__ == "__main__":
    unittest.main()
