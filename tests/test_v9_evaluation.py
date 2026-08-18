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
    V9_RULE_VERSION,
    _current_evidence_state,
    _load_release_runtime,
    evaluate_content,
    evaluate_release_content,
    incremental_candidates,
)
from v8.evaluation_selectors import formal_eligible_release_evaluations
from v8.matcher_dsl import MaterializedMatcher, POINT_IDS, POINT_SCENES
from v8.storage import PROJECT_ROOT, connect, initialize_database
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

    def test_v9_gray_stays_formal_without_any_queue_concept(self) -> None:
        with patch.object(
            MaterializedMatcher, "match_points", return_value=self._gray_match()
        ):
            backfilled = evaluate_release_content(
                1, release_id=V9_RELEASE_ID, db_path=self.db
            )
        self._activate_v9()
        self.assertIsNotNone(backfilled.evaluation_id)

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
                SELECT content_id,selling_point_included,evidence_level
                FROM evaluation_versions WHERE release_id=? ORDER BY content_id
                """,
                (V9_RELEASE_ID,),
            ).fetchall()
            formal = formal_eligible_release_evaluations(
                connection, V9_RELEASE_ID, [1, 2]
            )

        self.assertEqual(
            [tuple(row) for row in rows],
            [(1, 0, "V3"), (2, 0, "V3")],
        )
        # v9 灰区照常正式：60-74 排除规则只适用于 v9 之前的历史 release
        self.assertEqual(set(formal), {1, 2})

    def test_v9_insufficient_evidence_is_an_automatic_terminal_state(self) -> None:
        self._activate_v9()
        with connect(self.db) as connection:
            v8_state = _current_evidence_state(
                connection, 5, rule_version=V8_RULE_VERSION
            )
            v9_state = _current_evidence_state(
                connection, 5, rule_version=V9_RULE_VERSION
            )
        # v16 起人工证据域已删除：两个规则版本的该分量都恒为 None
        self.assertIsNone(v8_state[1]["manual_evidence_sha256"])
        self.assertIsNone(v9_state[1]["manual_evidence_sha256"])

        results = [evaluate_content(content_id, db_path=self.db) for content_id in (3, 4, 5)]
        self.assertEqual([result.evidence_level for result in results], ["V1", "V0", "V1"])
        reused = evaluate_content(5, db_path=self.db)
        self.assertFalse(reused.created)
        self.assertEqual(reused.evaluation_id, results[-1].evaluation_id)
        self.assertNotIn(5, incremental_candidates(db_path=self.db))
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT content_id,evaluation_status,selling_point_included
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
        self.assertEqual(
            [tuple(row) for row in rows],
            [
                (3, "insufficient_evidence", 0),
                (4, "insufficient_evidence", 0),
                (5, "insufficient_evidence", 0),
            ],
        )
        assert envelope is not None
        self.assertIsNone(envelope["manual_evidence_sha256"])
        self.assertIsNone(json.loads(envelope["components_json"])["manual_evidence_sha256"])


if __name__ == "__main__":
    unittest.main()
