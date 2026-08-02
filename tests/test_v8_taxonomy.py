from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from v8.storage import connect, initialize_database, now_utc
from v8.taxonomy import (
    TaxonomyError,
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
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('v5', 'selling-points-v5.0', 'published', 'base', ?, ?)
                """,
                (now_utc(), now_utc()),
            )
            point = connection.execute(
                """
                INSERT INTO selling_points(taxonomy_id, code, tier, label)
                VALUES ('v5', 'C1', 'other', '汽车知识')
                """
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, 'media')",
                (point.lastrowid,),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_draft_crud_does_not_mutate_published_taxonomy(self) -> None:
        draft = ensure_draft(db_path=self.db)
        self.assertEqual(draft["version"], "selling-points-v5.1")
        self.assertEqual(ensure_draft(db_path=self.db)["id"], draft["id"])
        update_point(
            "C1",
            {
                "tier": "other",
                "label": "实用汽车知识",
                "definition": "解决用车问题",
                "positive_evidence": ["保养"],
                "negative_evidence": ["泛娱乐"],
                "boundary_rules": ["需要明确方法"],
                "scenes": ["used_car", "media"],
            },
            db_path=self.db,
        )
        create_point(
            {
                "code": "Z1",
                "tier": "core",
                "label": "测试新卖点",
                "definition": "测试定义",
                "positive_evidence": ["测试词"],
                "negative_evidence": [],
                "boundary_rules": [],
                "scenes": ["new_car"],
            },
            db_path=self.db,
        )
        published = list_points(status="published", db_path=self.db)
        draft_points = list_points(status="draft", db_path=self.db)
        self.assertEqual(published["items"][0]["label"], "汽车知识")
        self.assertEqual({item["code"] for item in draft_points["items"]}, {"C1", "Z1"})
        delete_point("Z1", db_path=self.db)
        self.assertEqual([item["code"] for item in list_points(status="draft", db_path=self.db)["items"]], ["C1"])

    def test_publish_is_atomic_and_requires_scenes(self) -> None:
        ensure_draft(db_path=self.db)
        with self.assertRaises(TaxonomyError):
            create_point(
                {
                    "code": "Z1",
                    "tier": "core",
                    "label": "没有场景",
                    "scenes": [],
                },
                db_path=self.db,
            )
        result = publish_draft(db_path=self.db)
        self.assertEqual(result["version"], "selling-points-v5.1")
        self.assertEqual(result["point_count"], 1)
        with connect(self.db) as connection:
            statuses = {
                row["version"]: row["status"]
                for row in connection.execute("SELECT version, status FROM taxonomy_versions")
            }
        self.assertEqual(
            statuses,
            {"selling-points-v5.0": "retired", "selling-points-v5.1": "published"},
        )


if __name__ == "__main__":
    unittest.main()
