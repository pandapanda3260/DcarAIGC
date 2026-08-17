from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import v8.api as api_module
from v8.evaluation import (
    EvaluationError,
    V8_RULE_VERSION,
    V9_RULE_VERSION,
    _current_evidence_state,
    _load_release_runtime,
    apply_gray_review_queue_sync,
    evaluate_content,
    evaluate_release_content,
    incremental_candidates,
    plan_gray_review_queue_sync,
    reopen_review,
    resolve_review,
)
from v8.evaluation_selectors import formal_eligible_release_evaluations
from v8.matcher_dsl import MaterializedMatcher, POINT_IDS, POINT_SCENES
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules
from v8.taxonomy_v5_2_builder import build_v5_2_taxonomy_draft


V8_RELEASE_ID = "evaluation-v8__selling-points-v5.2"
V9_RELEASE_ID = "evaluation-v9__selling-points-v5.2"


class V9EvaluationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "evaluation-v9.sqlite3"
        captured_at = "2026-08-15T00:00:00Z"
        business_source = json.loads(
            (PROJECT_ROOT / "config" / "business_selling_points_v5_2.json").read_text(
                encoding="utf-8"
            )
        )
        business_points = {
            str(point["id"]): point for point in business_source["labels"]
        }
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5.0','selling-points-v5.0','published',
                          'v9 test base',?,?)
                """,
                (captured_at, captured_at),
            )
            for code in sorted(POINT_IDS):
                source = business_points.get(code, {})
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy-v5.0',?,?,?,?, '{}')
                    """,
                    (
                        code,
                        str(source.get("tier") or "other"),
                        str(source.get("label") or f"卖点 {code}"),
                        str(source.get("definition") or ""),
                    ),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            for content_id, title, body in (
                (1, "汽车刹车保养", "车辆维修和安全驾驶方法"),
                (2, "汽车轮胎保养", "车辆维修和安全驾驶方法"),
                (3, "只有正文", "现有文字证据不足"),
                (4, "", ""),
                (5, "人工证据不应生效", "只有文字"),
            ):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        id,link_id,platform,platform_content_id,canonical_url,
                        title,body,content_type,imported_at,created_at,updated_at
                    ) VALUES (?,?,'douyin',?,?,?,?,'video',?,?,?)
                    """,
                    (
                        content_id,
                        f"V9T00{content_id}",
                        str(content_id),
                        f"https://www.douyin.com/video/{content_id}",
                        title,
                        body,
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                )
            for content_id in (1, 2):
                self._insert_full_media(connection, content_id, captured_at)
            connection.commit()

        backfill_v5_1_matcher_rules(db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE taxonomy_versions SET status='retired' WHERE version='selling-points-v5.0'"
            )
            connection.execute(
                """
                UPDATE taxonomy_versions
                SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (captured_at,),
            )
            connection.commit()

        built = build_v5_2_taxonomy_draft(db_path=self.db, dry_run=False)
        matcher_sha256 = str(built["matcher_rule_sha256"])
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE taxonomy_versions SET status='retired' WHERE version='selling-points-v5.1'"
            )
            connection.execute(
                """
                UPDATE taxonomy_versions
                SET status='published',published_at=?
                WHERE version='selling-points-v5.2'
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (?,?, 'selling-points-v5.2',?,'active',?,?,?)
                """,
                (
                    V8_RELEASE_ID,
                    V8_RULE_VERSION,
                    matcher_sha256,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES (?,?, 'selling-points-v5.2',?,'backfilling',?,?)
                """,
                (
                    V9_RELEASE_ID,
                    V9_RULE_VERSION,
                    matcher_sha256,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_full_media(
        self, connection, content_id: int, captured_at: str
    ) -> None:
        media = self.root / f"{content_id}.mp4"
        media.write_bytes(b"video" * 500)
        asr = self.root / f"{content_id}-asr.json"
        asr.write_text(
            json.dumps(
                {
                    "status": "success",
                    "text": "汽车刹车轮胎保养维修故障判断方法需要注意安全驾驶技巧",
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
                    "combined_text": "汽车常见故障图解刹车片轮胎保养维修方法和安全提示",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        for artifact_type, path in (("media", media), ("asr", asr), ("ocr", ocr)):
            body = path.read_bytes()
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id,artifact_type,local_path,status,byte_size,sha256,
                    processor_version,created_at
                ) VALUES (?,?,?,'available',?,?,'v9-test',?)
                """,
                (
                    content_id,
                    artifact_type,
                    str(path),
                    len(body),
                    hashlib.sha256(body).hexdigest(),
                    captured_at,
                ),
            )

    def _activate_v9(self) -> None:
        captured_at = "2026-08-15T01:00:00Z"
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at=?,updated_at=? WHERE id=?
                """,
                (captured_at, captured_at, V8_RELEASE_ID),
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='active',activated_at=?,updated_at=? WHERE id=?
                """,
                (captured_at, captured_at, V9_RELEASE_ID),
            )
            connection.commit()

    def _insert_manual_evidence(self, content_id: int, text: str) -> None:
        with connect(self.db) as connection:
            review = connection.execute(
                """
                INSERT INTO evaluation_reviews(
                    content_id,decision,reason,reviewer,created_at
                ) VALUES (?,'confirm','legacy synthetic evidence','legacy-reviewer',?)
                """,
                (content_id, now_utc()),
            )
            assert review.lastrowid is not None
            connection.execute(
                """
                INSERT INTO manual_evidence(
                    review_id,content_id,evidence_type,text_value,sha256,created_at
                ) VALUES (?,?,'visual_summary',?,?,?)
                """,
                (
                    int(review.lastrowid),
                    content_id,
                    text,
                    hashlib.sha256(text.encode("utf-8")).hexdigest(),
                    now_utc(),
                ),
            )
            connection.commit()

    @staticmethod
    def _gray_match() -> list[dict[str, object]]:
        return [
            {
                "id": "E1",
                "score": 70,
                "scene": "used_car",
                "reason": "v9 gray fixture",
                "source": "matcher",
            }
        ]

    def test_v9_uses_shared_published_v52_for_backfill_ready_and_active(self) -> None:
        with connect(self.db) as connection:
            v9 = connection.execute(
                "SELECT * FROM evaluation_releases WHERE id=?", (V9_RELEASE_ID,)
            ).fetchone()
            assert v9 is not None
            self.assertIsNotNone(_load_release_runtime(connection, v9).matcher)
            connection.execute(
                "UPDATE evaluation_releases SET status='ready' WHERE id=?",
                (V9_RELEASE_ID,),
            )
            ready = connection.execute(
                "SELECT * FROM evaluation_releases WHERE id=?", (V9_RELEASE_ID,)
            ).fetchone()
            assert ready is not None
            self.assertIsNotNone(_load_release_runtime(connection, ready).matcher)

            connection.execute(
                "UPDATE evaluation_releases SET status='backfilling' WHERE id=?",
                (V8_RELEASE_ID,),
            )
            v8_backfill = connection.execute(
                "SELECT * FROM evaluation_releases WHERE id=?", (V8_RELEASE_ID,)
            ).fetchone()
            assert v8_backfill is not None
            with self.assertRaisesRegex(EvaluationError, "requires a draft taxonomy"):
                _load_release_runtime(connection, v8_backfill)

    def test_v9_gray_is_formal_but_never_pending_or_queue_driven(self) -> None:
        with patch.object(
            MaterializedMatcher, "match_points", return_value=self._gray_match()
        ):
            backfilled = evaluate_release_content(
                1, release_id=V9_RELEASE_ID, db_path=self.db
            )
        self._activate_v9()
        with connect(self.db) as connection:
            for reason_code in (
                "evaluation_gray_zone",
                "manual_conclusion_conflict",
            ):
                connection.execute(
                    """
                    INSERT INTO review_queue(
                        content_id,evaluation_id,reason_code,status,created_at,updated_at,
                        resolved_at
                    ) VALUES (1,?,?, 'resolved',?,?,?)
                    """,
                    (
                        backfilled.evaluation_id,
                        reason_code,
                        now_utc(),
                        now_utc(),
                        now_utc(),
                    ),
                )
            connection.commit()

        reused = evaluate_content(1, db_path=self.db)
        self.assertFalse(reused.created)
        with patch.object(
            MaterializedMatcher, "match_points", return_value=self._gray_match()
        ) as matcher:
            created = evaluate_content(2, db_path=self.db)
        self.assertTrue(created.created)
        call = matcher.call_args
        assert call is not None
        self.assertEqual(call.args[-1], {"summary": ""})
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT content_id,pending_review,selling_point_included,evidence_level
                FROM evaluation_versions WHERE release_id=? ORDER BY content_id
                """,
                (V9_RELEASE_ID,),
            ).fetchall()
            queue_rows = connection.execute(
                """
                SELECT content_id,reason_code,status FROM review_queue ORDER BY id
                """
            ).fetchall()
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (2,?,'manual_conclusion_conflict','manual_required',?,?)
                """,
                (created.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            formal = formal_eligible_release_evaluations(
                connection, V9_RELEASE_ID, [1, 2]
            )

        self.assertEqual(
            [tuple(row) for row in rows],
            [(1, 0, 0, "V3"), (2, 0, 0, "V3")],
        )
        self.assertEqual(
            [(row["reason_code"], row["status"]) for row in queue_rows],
            [
                ("evaluation_gray_zone", "resolved"),
                ("manual_conclusion_conflict", "resolved"),
            ],
        )
        self.assertEqual(set(formal), {1, 2})
        plan = plan_gray_review_queue_sync(db_path=self.db)
        self.assertEqual(plan["target_count"], 0)
        applied = apply_gray_review_queue_sync(
            expected_plan_sha256=str(plan["plan_sha256"]), db_path=self.db
        )
        self.assertEqual(applied["sqlite_changes"], 0)

    def test_v9_insufficient_and_manual_evidence_are_automatic_terminal_states(self) -> None:
        self._insert_manual_evidence(
            5, "人工画面声称连续展示车辆维修过程但不得进入v9自动证据"
        )
        self._activate_v9()
        with connect(self.db) as connection:
            v8_state = _current_evidence_state(
                connection, 5, rule_version=V8_RULE_VERSION
            )
            v9_state = _current_evidence_state(
                connection, 5, rule_version=V9_RULE_VERSION
            )
        self.assertIsNotNone(v8_state[1]["manual_evidence_sha256"])
        self.assertIsNone(v9_state[1]["manual_evidence_sha256"])
        self.assertEqual(v9_state[0]["manual_rows"], [])

        results = [evaluate_content(content_id, db_path=self.db) for content_id in (3, 4, 5)]
        self.assertEqual([result.evidence_level for result in results], ["V1", "V0", "V1"])
        self._insert_manual_evidence(5, "第二条合成人工证据也不得改变v9自动评估")
        reused = evaluate_content(5, db_path=self.db)
        self.assertFalse(reused.created)
        self.assertEqual(reused.evaluation_id, results[-1].evaluation_id)
        self.assertNotIn(5, incremental_candidates(db_path=self.db))
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT content_id,evaluation_status,pending_review,selling_point_included
                FROM evaluation_versions WHERE release_id=? ORDER BY content_id
                """,
                (V9_RELEASE_ID,),
            ).fetchall()
            envelope = connection.execute(
                """
                SELECT manual_evidence_sha256,components_json
                FROM evidence_envelopes WHERE id=?
                """,
                (results[-1].evidence_envelope_id,),
            ).fetchone()
            queue_count = int(
                connection.execute("SELECT COUNT(*) FROM review_queue").fetchone()[0]
            )
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                (3, "insufficient_evidence", 0, 0),
                (4, "insufficient_evidence", 0, 0),
                (5, "insufficient_evidence", 0, 0),
            ],
        )
        assert envelope is not None
        self.assertIsNone(envelope["manual_evidence_sha256"])
        self.assertIsNone(json.loads(envelope["components_json"])["manual_evidence_sha256"])
        self.assertEqual(queue_count, 0)

    def test_v9_review_entry_points_return_409_and_write_nothing(self) -> None:
        self._activate_v9()
        with patch.object(
            MaterializedMatcher,
            "match_points",
            return_value=[
                {
                    "id": "E1",
                    "score": 80,
                    "scene": "used_car",
                    "reason": "v9 included fixture",
                    "source": "matcher",
                }
            ],
        ):
            evaluation = evaluate_content(1, db_path=self.db)
        with connect(self.db) as connection:
            pending = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'legacy-manual-fixture','pending',?,?)
                """,
                (evaluation.evaluation_id, now_utc(), now_utc()),
            )
            resolved = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at,
                    resolved_at
                ) VALUES (1,?,'legacy-resolved-fixture','resolved',?,?,?)
                """,
                (
                    evaluation.evaluation_id,
                    now_utc(),
                    now_utc(),
                    now_utc(),
                ),
            )
            assert pending.lastrowid is not None and resolved.lastrowid is not None
            pending_id = int(pending.lastrowid)
            resolved_id = int(resolved.lastrowid)
            connection.commit()

        before = self._review_write_snapshot()
        with self.assertRaisesRegex(EvaluationError, "disabled for evaluation-v9"):
            reopen_review(
                resolved_id,
                reason="不得重开",
                reopened_by="v9-test",
                db_path=self.db,
            )
        with self.assertRaisesRegex(EvaluationError, "disabled for evaluation-v9"):
            resolve_review(
                pending_id,
                decision="confirm",
                reason="不得裁决",
                reviewer="v9-test",
                evidence_type="review_note",
                evidence_text="不得创建人工证据",
                base_evaluation_id=evaluation.evaluation_id,
                db_path=self.db,
            )
        with self.assertRaisesRegex(EvaluationError, "disabled for evaluation-v9"):
            evaluate_content(
                1,
                db_path=self.db,
                source="manual_review",
                manual_override={"decision": "confirm"},
                review_id=999,
                parent_evaluation_id=evaluation.evaluation_id,
            )
        self.assertEqual(self._review_write_snapshot(), before)

        config = api_module.ApiConfig(
            db_path=self.db,
            reports_root=self.root / "reports",
            legacy_db_path=self.root / "legacy.sqlite3",
            operator_freeze_lock=self.root / "freeze.lock",
            writer_lock=self.root / "writer.lock",
        )
        with TestClient(api_module.create_app(config)) as client:
            responses = (
                client.post(f"/api/v8/reviews/{pending_id}/start"),
                client.post(
                    f"/api/v8/reviews/{resolved_id}/reopen",
                    json={"reason": "不得重开", "reopened_by": "v9-test"},
                ),
                client.post(
                    f"/api/v8/reviews/{pending_id}/resolve",
                    json={
                        "base_evaluation_id": evaluation.evaluation_id,
                        "decision": "confirm",
                        "reason": "不得裁决",
                        "reviewer": "v9-test",
                        "evidence_type": "review_note",
                        "evidence_text": "不得创建人工证据",
                    },
                ),
            )
        self.assertEqual([response.status_code for response in responses], [409, 409, 409])
        self.assertTrue(
            all("disabled for evaluation-v9" in response.json()["detail"] for response in responses)
        )
        self.assertEqual(self._review_write_snapshot(), before)

    def _review_write_snapshot(self) -> dict[str, object]:
        with connect(self.db) as connection:
            return {
                "queues": [
                    tuple(row)
                    for row in connection.execute(
                        """
                        SELECT id,evaluation_id,status,assigned_to,resolved_at,updated_at
                        FROM review_queue ORDER BY id
                        """
                    ).fetchall()
                ],
                "reviews": int(
                    connection.execute("SELECT COUNT(*) FROM evaluation_reviews").fetchone()[0]
                ),
                "manual_evidence": int(
                    connection.execute("SELECT COUNT(*) FROM manual_evidence").fetchone()[0]
                ),
                "reopen_events": int(
                    connection.execute("SELECT COUNT(*) FROM review_reopen_events").fetchone()[0]
                ),
                "evaluations": int(
                    connection.execute("SELECT COUNT(*) FROM evaluation_versions").fetchone()[0]
                ),
            }


if __name__ == "__main__":
    unittest.main()
