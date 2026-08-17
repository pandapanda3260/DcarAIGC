"""SPU 用车场景 v0.2：商务接待与经营载货分流。"""

from __future__ import annotations

import json
import sqlite3
import unittest

from v8.spu_audience import (
    ASSET_SEED_VERSION,
    ASSOCIATION_RULE_VERSION,
    _load_assets,
    content_labels,
    ensure_assets,
    match_scenes,
)
from v8.storage import (
    configure_connection_safety,
    initialize_database,
    now_utc,
    transaction,
)


LEGACY_S6 = {
    "label": "商务接待载货",
    "definition": "商务接待与经营载货",
    "triggers": ["商务", "接待", "行政", "载货", "商用"],
    "negatives": [],
}

LEGACY_S8 = {
    "label": "拉货创富",
    "definition": "拉货、摆摊与小生意",
    "triggers": ["拉货", "摆摊", "进货", "小生意", "创业"],
    "negatives": [],
}


def _fresh_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.isolation_level = None
    configure_connection_safety(connection)
    initialize_database(connection)
    with transaction(connection):
        ensure_assets(connection)
    return connection


def _write_scene(
    connection: sqlite3.Connection, code: str, scene: dict[str, object]
) -> None:
    connection.execute(
        """
        UPDATE scene_dim
        SET label=?, definition=?, trigger_words_json=?, negative_words_json=?
        WHERE code=?
        """,
        (
            scene["label"],
            scene["definition"],
            json.dumps(scene["triggers"], ensure_ascii=False),
            json.dumps(scene["negatives"], ensure_ascii=False),
            code,
        ),
    )


class SceneTaxonomyV2Test(unittest.TestCase):
    def setUp(self) -> None:
        self.connection = _fresh_connection()

    def tearDown(self) -> None:
        self.connection.close()

    def test_fresh_assets_keep_s6_code_but_split_business_and_cargo(self) -> None:
        assets = _load_assets(self.connection)
        scenes = {str(scene["code"]): scene for scene in assets["scenes"]}

        self.assertEqual(ASSOCIATION_RULE_VERSION, "spu-association-v2")
        self.assertEqual(ASSET_SEED_VERSION, "spu-assets-v0.2")
        self.assertEqual(scenes["S6"]["label"], "商务接待")
        self.assertNotIn("载货", scenes["S6"]["triggers"])
        self.assertIn("载货", scenes["S8"]["triggers"])
        self.assertEqual(assets["scene_map"]["P3"]["core"], {"S6"})
        self.assertIn("S8", assets["scene_map"]["P8"]["core"])

    def test_rule_match_routes_business_to_s6_and_cargo_to_s8(self) -> None:
        assets = _load_assets(self.connection)

        business = match_scenes(
            {"title": "这台车适合商务接待", "body": "", "asr": "", "ocr": ""},
            "V3",
            assets,
        )
        cargo = match_scenes(
            {"title": "这台车日常用来载货", "body": "", "asr": "", "ocr": ""},
            "V3",
            assets,
        )

        self.assertEqual([row["scene_code"] for row in business], ["S6"])
        self.assertEqual([row["scene_code"] for row in cargo], ["S8"])

    def test_legacy_seed_is_upgraded_but_manual_edit_is_preserved(self) -> None:
        _write_scene(self.connection, "S6", LEGACY_S6)
        _write_scene(self.connection, "S8", LEGACY_S8)
        with transaction(self.connection):
            ensure_assets(self.connection)

        rows = {
            str(row["code"]): row
            for row in self.connection.execute(
                "SELECT * FROM scene_dim WHERE code IN ('S6','S8')"
            )
        }
        self.assertEqual(str(rows["S6"]["label"]), "商务接待")
        self.assertIn("载货", json.loads(str(rows["S8"]["trigger_words_json"])))

        customized = dict(LEGACY_S6, label="自定义商务场景")
        _write_scene(self.connection, "S6", customized)
        with transaction(self.connection):
            ensure_assets(self.connection)
        label = self.connection.execute(
            "SELECT label FROM scene_dim WHERE code='S6'"
        ).fetchone()[0]
        self.assertEqual(str(label), "自定义商务场景")

    def test_reading_an_old_database_projects_v2_without_writing(self) -> None:
        _write_scene(self.connection, "S6", LEGACY_S6)
        assets = _load_assets(self.connection)
        effective = next(scene for scene in assets["scenes"] if scene["code"] == "S6")
        stored = self.connection.execute(
            "SELECT label FROM scene_dim WHERE code='S6'"
        ).fetchone()[0]

        self.assertEqual(effective["label"], "商务接待")
        self.assertEqual(str(stored), "商务接待载货")

    def test_content_api_label_also_projects_legacy_s6_as_business_only(self) -> None:
        _write_scene(self.connection, "S6", LEGACY_S6)
        stamp = now_utc()
        self.connection.execute(
            """
            INSERT INTO content_items(
                id, link_id, platform, canonical_url, title, body,
                published_at, imported_at, created_at, updated_at
            ) VALUES (1, 'L00001', 'douyin', 'https://example.com/1',
                      '商务接待', '', ?, ?, ?, ?)
            """,
            (stamp, stamp, stamp, stamp),
        )
        self.connection.execute(
            """
            INSERT INTO content_scene_links(
                content_id, scene_code, score, evidence_json, rule_version,
                created_at, invalidated_at
            ) VALUES (1, 'S6', 70, '{"words":["商务"]}',
                      'spu-association-v1', ?, NULL)
            """,
            (stamp,),
        )

        labels = content_labels(self.connection, [1])
        self.assertEqual(labels[1]["scenes"], [{"code": "S6", "label": "商务接待"}])
        stored = self.connection.execute(
            "SELECT label FROM scene_dim WHERE code='S6'"
        ).fetchone()[0]
        self.assertEqual(str(stored), "商务接待载货")


if __name__ == "__main__":
    unittest.main()
