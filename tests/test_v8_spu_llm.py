"""LLM 辅助链路（B 链）单测：四道闸、落库语义、缓存与回滚、schema v15。

不触网：调用层不测（call_llm 只在实跑走），覆盖纯函数与 SQLite 行为。
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8 import llm_assist
from v8.llm_assist import (
    LLM_PROMPT_VERSION,
    LLM_RULE_VERSION,
    LlmDisabledError,
    apply_verdict,
    build_candidates,
    fetch_cached_response,
    invalidate_llm_links,
    llm_config,
    parse_response,
    prepare_texts,
    target_needs_llm,
    text_sha256,
    upsert_judgement,
    validate_judgement,
)
from v8.storage import (
    SCHEMA_VERSION,
    configure_connection_safety,
    initialize_database,
    now_utc,
    transaction,
)


def _fresh_connection() -> sqlite3.Connection:
    from v8.spu_audience import ensure_assets

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.isolation_level = None  # 测试直插不留隐式事务，transaction() 正常工作
    configure_connection_safety(connection)
    initialize_database(connection)
    with transaction(connection):
        ensure_assets(connection)  # 44 车系 + P1–P8 + S1–S11 种子
    return connection


def _insert_content(connection: sqlite3.Connection, content_id: int) -> None:
    stamp = now_utc()
    connection.execute(
        """
        INSERT INTO content_items(
            id, link_id, platform, canonical_url, title, body,
            published_at, imported_at, created_at, updated_at
        ) VALUES (?, ?, 'douyin', ?, '标题', '正文', ?, ?, ?, ?)
        """,
        (
            content_id, f"L{content_id:05d}", f"https://example.com/{content_id}",
            stamp, stamp, stamp, stamp,
        ),
    )


def _assets(connection: sqlite3.Connection):
    from v8.spu_audience import _load_assets

    return _load_assets(connection)


_PREPARED = {
    "title": "坦克300 越野实测",
    "body": "这台坦克300带娃露营也够用，全家出行很舒服，性价比在线",
    "asr": "今天我们聊聊坦克300的脱困能力",
    "ocr": "",
}


def _target(**overrides):
    target = {
        "content_id": 1,
        "evidence_level": "V3",
        "title": _PREPARED["title"],
        "body": _PREPARED["body"],
        "artifact_paths": {},
        "spu_state": "none",
        "primary_slug": None,
        "trim_resolved": False,
        "gray_slugs": [],
        "scene_codes": [],
        "has_audience": False,
    }
    target.update(overrides)
    return target


def _judgement(**overrides):
    payload = {
        "spu": {
            "series_slug": "tank__300",
            "trim_id": None,
            "confidence": 0.9,
            "channel": "title",
            "quote": "坦克300",
        },
        "out_of_catalog": None,
        "scenes": [
            {"code": "S5", "confidence": 0.85, "channel": "body", "quote": "带娃露营"},
        ],
        "audience": {
            "code": "P7", "confidence": 0.8, "channel": "asr", "quote": "脱困能力",
        },
    }
    payload.update(overrides)
    return payload


class ValidateJudgementTest(unittest.TestCase):
    """四道闸：闭集 → 原文证据 → 置信度 → 规则优先。"""

    @classmethod
    def setUpClass(cls):
        cls.connection = _fresh_connection()
        cls.candidates = build_candidates(_assets(cls.connection))

    @classmethod
    def tearDownClass(cls):
        cls.connection.close()

    def test_happy_path_fill(self):
        verdict = validate_judgement(
            _judgement(), _PREPARED, self.candidates, _target()
        )
        self.assertIsNotNone(verdict["spu"])
        self.assertEqual(verdict["spu"]["series_slug"], "tank__300")
        self.assertEqual(verdict["spu"]["action"], "fill")
        self.assertEqual([scene["code"] for scene in verdict["scenes"]], ["S5"])
        self.assertEqual(verdict["audience"]["code"], "P7")
        self.assertEqual(verdict["rejects"], [])

    def test_gate1_closed_set(self):
        verdict = validate_judgement(
            _judgement(spu={
                "series_slug": "fake__series", "trim_id": None,
                "confidence": 0.99, "channel": "title", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, _target(),
        )
        self.assertIsNone(verdict["spu"])
        self.assertIn(
            "series-not-in-catalog",
            [reject["reason"] for reject in verdict["rejects"]],
        )

    def test_gate2_quote_must_exist_verbatim(self):
        verdict = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.99, "channel": "title", "quote": "编造的引文",
            }),
            _PREPARED, self.candidates, _target(),
        )
        self.assertIsNone(verdict["spu"])
        self.assertIn(
            "quote-not-found",
            [reject["reason"] for reject in verdict["rejects"]],
        )

    def test_gate2_quote_channel_mismatch(self):
        verdict = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.9, "channel": "ocr", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, _target(),
        )
        self.assertIsNone(verdict["spu"])

    def test_gate3_low_confidence(self):
        verdict = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.5, "channel": "title", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, _target(),
        )
        self.assertIsNone(verdict["spu"])
        self.assertIn(
            "low-confidence",
            [reject["reason"] for reject in verdict["rejects"]],
        )

    def test_gate4_confirmed_not_overridden(self):
        verdict = validate_judgement(
            _judgement(),
            _PREPARED, self.candidates,
            _target(spu_state="confirmed", primary_slug="byd__han", trim_resolved=True),
        )
        self.assertIsNone(verdict["spu"])  # 规则已确认，LLM 不得改车系

    def test_gray_upgrade_and_override_thresholds(self):
        gray_target = _target(spu_state="gray", gray_slugs=["tank__300"])
        upgrade = validate_judgement(
            _judgement(), _PREPARED, self.candidates, gray_target
        )
        self.assertEqual(upgrade["spu"]["action"], "gray-upgrade")

        other_gray = _target(spu_state="gray", gray_slugs=["byd__han"])
        need_override = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.75, "channel": "title", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, other_gray,
        )
        self.assertIsNone(need_override["spu"])  # 反向改判 0.75 < 0.8 被拦
        override = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.85, "channel": "title", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, other_gray,
        )
        self.assertEqual(override["spu"]["action"], "gray-override")

    def test_scene_and_audience_rule_priority(self):
        verdict = validate_judgement(
            _judgement(),
            _PREPARED, self.candidates,
            _target(scene_codes=["S5"], has_audience=True),
        )
        self.assertEqual(verdict["scenes"], [])  # 已识别的场景不重复落
        self.assertIsNone(verdict["audience"])  # 已归因的人群不覆盖

    def test_out_of_catalog_counted_not_linked(self):
        verdict = validate_judgement(
            _judgement(
                spu={"series_slug": None, "trim_id": None, "confidence": 0.9,
                     "channel": "title", "quote": "坦克300"},
                out_of_catalog="仰望U8",
            ),
            _PREPARED, self.candidates, _target(),
        )
        self.assertEqual(verdict["out_of_catalog"], "仰望U8")
        self.assertIsNone(verdict["spu"])

    def test_parse_error(self):
        verdict = validate_judgement(None, _PREPARED, self.candidates, _target())
        self.assertFalse(llm_assist.verdict_accepts(verdict))
        self.assertEqual(verdict["rejects"][0]["reason"], "parse-error")


class ApplyVerdictTest(unittest.TestCase):
    """落库语义：rule_version='spu-llm-v1'、主行唯一、灰区升级失效旧行、回滚。"""

    def setUp(self):
        self.connection = _fresh_connection()
        _insert_content(self.connection, 1)
        self.assets = _assets(self.connection)
        self.candidates = build_candidates(self.assets)

    def tearDown(self):
        self.connection.close()

    def _apply(self, verdict, target):
        with transaction(self.connection):
            return apply_verdict(
                self.connection, target, verdict, self.assets,
                "doubao-test", now_utc(), str(target["evidence_level"]),
            )

    def test_fill_writes_llm_rows(self):
        target = _target()
        verdict = validate_judgement(
            _judgement(), _PREPARED, self.candidates, target
        )
        counters = self._apply(verdict, target)
        self.assertEqual(counters["spu_filled"], 1)
        self.assertEqual(counters["scene_filled"], 1)
        self.assertEqual(counters["audience_filled"], 1)
        spu_row = self.connection.execute(
            "SELECT * FROM content_spu_links WHERE invalidated_at IS NULL"
        ).fetchone()
        self.assertEqual(str(spu_row["rule_version"]), LLM_RULE_VERSION)
        self.assertEqual(str(spu_row["status"]), "confirmed")
        self.assertEqual(int(spu_row["is_primary"]), 1)
        audience_row = self.connection.execute(
            "SELECT * FROM content_audience_links WHERE invalidated_at IS NULL"
        ).fetchone()
        self.assertEqual(str(audience_row["source"]), "llm")
        evidence = json.loads(str(audience_row["evidence_json"]))
        self.assertEqual(evidence["quote"], "脱困能力")

    def test_gray_upgrade_invalidates_gray_row(self):
        stamp = now_utc()
        self.connection.execute(
            """
            INSERT INTO content_spu_links(
                content_id, spu_id, resolved_level, is_primary, status, score,
                evidence_json, rule_version, created_at, invalidated_at
            ) VALUES (1, 'tank__300', 'series', 0, 'gray', 65, '{}',
                      'spu-association-v1', ?, NULL)
            """,
            (stamp,),
        )
        target = _target(spu_state="gray", gray_slugs=["tank__300"])
        verdict = validate_judgement(
            _judgement(), _PREPARED, self.candidates, target
        )
        counters = self._apply(verdict, target)
        self.assertEqual(counters["gray_upgraded"], 1)
        rows = self.connection.execute(
            "SELECT status, rule_version, invalidated_at, score FROM content_spu_links "
            "WHERE spu_id='tank__300' ORDER BY id"
        ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertIsNotNone(rows[0]["invalidated_at"])  # 灰区旧行被失效
        self.assertEqual(str(rows[1]["rule_version"]), LLM_RULE_VERSION)
        self.assertEqual(str(rows[1]["status"]), "confirmed")
        self.assertGreaterEqual(int(rows[1]["score"]), 65)

    def test_no_second_primary(self):
        stamp = now_utc()
        self.connection.execute(
            """
            INSERT INTO content_spu_links(
                content_id, spu_id, resolved_level, is_primary, status, score,
                evidence_json, rule_version, created_at, invalidated_at
            ) VALUES (1, 'byd__han', 'series', 1, 'confirmed', 80, '{}',
                      'spu-association-v1', ?, NULL)
            """,
            (stamp,),
        )
        target = _target(spu_state="gray", gray_slugs=["byd__han"])
        verdict = validate_judgement(
            _judgement(spu={
                "series_slug": "tank__300", "trim_id": None,
                "confidence": 0.9, "channel": "title", "quote": "坦克300",
            }),
            _PREPARED, self.candidates, target,
        )
        self._apply(verdict, target)
        primaries = self.connection.execute(
            "SELECT COUNT(*) FROM content_spu_links "
            "WHERE invalidated_at IS NULL AND is_primary=1"
        ).fetchone()[0]
        self.assertEqual(int(primaries), 1)

    def test_rollback_invalidates_only_llm_rows(self):
        target = _target()
        verdict = validate_judgement(
            _judgement(), _PREPARED, self.candidates, target
        )
        self._apply(verdict, target)
        stamp = now_utc()
        self.connection.execute(
            """
            INSERT INTO content_scene_links(
                content_id, scene_code, score, evidence_json, rule_version,
                created_at, invalidated_at
            ) VALUES (1, 'S1', 70, '{}', 'spu-association-v1', ?, NULL)
            """,
            (stamp,),
        )
        with transaction(self.connection):
            total = invalidate_llm_links(self.connection, now_utc())
        self.assertEqual(total, 3)  # spu + scene + audience 各 1 行
        remaining = self.connection.execute(
            "SELECT rule_version FROM content_scene_links WHERE invalidated_at IS NULL"
        ).fetchall()
        self.assertEqual(
            [str(row["rule_version"]) for row in remaining],
            ["spu-association-v1"],
        )


class JudgementCacheTest(unittest.TestCase):
    def setUp(self):
        self.connection = _fresh_connection()
        _insert_content(self.connection, 1)
        self.config = {"model": "doubao-test"}

    def tearDown(self):
        self.connection.close()

    def test_upsert_and_fetch_roundtrip(self):
        digest = text_sha256(prepare_texts(_PREPARED))
        self.assertIsNone(
            fetch_cached_response(self.connection, 1, digest, self.config)
        )
        with transaction(self.connection):
            upsert_judgement(
                self.connection, 1, digest, self.config,
                status="accepted", response_json='{"spu":null}',
                verdict_json="{}", usage={"input_tokens": 10, "output_tokens": 2},
                captured_at=now_utc(),
            )
        self.assertEqual(
            fetch_cached_response(self.connection, 1, digest, self.config),
            '{"spu":null}',
        )
        # error 状态视为未命中（下次重试真实调用）
        with transaction(self.connection):
            upsert_judgement(
                self.connection, 1, digest, self.config,
                status="error", response_json="", verdict_json="{}",
                usage={}, captured_at=now_utc(),
            )
        self.assertIsNone(
            fetch_cached_response(self.connection, 1, digest, self.config)
        )

    def test_text_change_misses_cache(self):
        digest = text_sha256(prepare_texts(_PREPARED))
        changed = dict(_PREPARED, body=_PREPARED["body"] + "（新增补充说明）")
        self.assertNotEqual(digest, text_sha256(prepare_texts(changed)))


class SchemaAndConfigTest(unittest.TestCase):
    def test_fresh_schema_is_v15_with_llm_tables(self):
        connection = _fresh_connection()
        try:
            self.assertEqual(SCHEMA_VERSION, 15)
            version = int(
                connection.execute("PRAGMA user_version").fetchone()[0]
            )
            self.assertEqual(version, 15)
            tables = {
                str(row[0]) for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
            self.assertIn("llm_judgements", tables)
            columns = [
                str(row["name"]) for row in connection.execute(
                    "PRAGMA table_info(content_audience_links)"
                )
            ]
            self.assertIn("evidence_json", columns)
            # source CHECK 已扩到 llm（直接插入验证约束）
            _insert_content(connection, 7)
            connection.execute(
                """
                INSERT INTO content_audience_links(
                    content_id, audience_code, source, conflict_flag,
                    consistency_flag, evidence_json, rule_version, created_at,
                    invalidated_at
                ) VALUES (7, 'P1', 'llm', 0, 0, '{}', ?, ?, NULL)
                """,
                (LLM_RULE_VERSION, now_utc()),
            )
        finally:
            connection.close()

    def test_llm_config_reads_key_file_and_disables_cleanly(self):
        saved_disabled = os.environ.pop("DCAR_LLM_DISABLED", None)
        with tempfile.TemporaryDirectory() as tmp:
            key_file = Path(tmp) / "dcar.env.local"
            key_file.write_text(
                'PROJECT_CLASSIFIER_API_KEY="k-123456"\n'
                'PROJECT_CLASSIFIER_MODEL="doubao-seed-2-1-pro-260628"\n',
                encoding="utf-8",
            )
            os.environ["DCAR_LLM_KEY_FILE"] = str(key_file)
            try:
                config = llm_config()
                self.assertEqual(config["api_key"], "k-123456")
                self.assertEqual(config["model"], "doubao-seed-2-1-pro-260628")
                self.assertTrue(config["api_base"].startswith("https://ark."))
                os.environ["DCAR_LLM_DISABLED"] = "1"
                with self.assertRaises(LlmDisabledError):
                    llm_config()
            finally:
                os.environ.pop("DCAR_LLM_KEY_FILE", None)
                os.environ.pop("DCAR_LLM_DISABLED", None)
                if saved_disabled is not None:
                    os.environ["DCAR_LLM_DISABLED"] = saved_disabled
        self.assertEqual(LLM_PROMPT_VERSION, "llm-prompt-v2")

    def test_parse_response_handles_fences(self):
        self.assertEqual(parse_response('{"spu": null}'), {"spu": None})
        self.assertEqual(
            parse_response('```json\n{"spu": null}\n```'), {"spu": None}
        )
        self.assertIsNone(parse_response("抱歉，我无法判断"))

    def test_target_needs_llm(self):
        connection = _fresh_connection()
        try:
            candidates = build_candidates(_assets(connection))
            self.assertTrue(target_needs_llm(_target(), candidates))
            resolved = _target(
                spu_state="confirmed", primary_slug="tank__300",
                trim_resolved=False, scene_codes=["S4"], has_audience=True,
            )
            # 款型库为空：细化无从谈起，已全部解决 → 不送 LLM
            self.assertFalse(target_needs_llm(resolved, candidates))
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
