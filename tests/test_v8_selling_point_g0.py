from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from dcar_eval.v8.matcher_dsl import canonical_json
from dcar_eval.v8.selling_point_g0 import (
    SellingPointG0Error,
    build_v5_3_points,
    materialize_v5_3_draft,
)


class SellingPointG0Tests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "g0.sqlite3"
        points, _ = build_v5_3_points()
        connection = sqlite3.connect(self.db)
        connection.executescript(
            """
            PRAGMA foreign_keys=ON;
            CREATE TABLE taxonomy_versions(
                id TEXT PRIMARY KEY,
                version TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL,
                definition TEXT NOT NULL,
                source_path TEXT,
                source_sha256 TEXT,
                created_at TEXT NOT NULL,
                published_at TEXT
            );
            CREATE TABLE selling_points(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                taxonomy_id TEXT NOT NULL REFERENCES taxonomy_versions(id),
                code TEXT NOT NULL,
                tier TEXT NOT NULL,
                label TEXT NOT NULL,
                definition TEXT NOT NULL,
                positive_evidence_json TEXT NOT NULL,
                negative_evidence_json TEXT NOT NULL,
                boundary_rules_json TEXT NOT NULL,
                enabled INTEGER NOT NULL,
                matcher_rule_json TEXT NOT NULL,
                UNIQUE(taxonomy_id,code)
            );
            CREATE TABLE selling_point_scenes(
                selling_point_id INTEGER NOT NULL REFERENCES selling_points(id) ON DELETE CASCADE,
                scene TEXT NOT NULL,
                PRIMARY KEY(selling_point_id,scene)
            );
            CREATE TABLE evaluation_releases(
                id TEXT PRIMARY KEY,
                taxonomy_version TEXT NOT NULL
            );
            INSERT INTO taxonomy_versions(
                id,version,status,definition,source_path,source_sha256,created_at,published_at
            ) VALUES
                ('v52','selling-points-v5.2','published','old','old','old','now','now'),
                ('v53','selling-points-v5.3','draft','old','old','old','now',NULL);
            """
        )
        for code, point in sorted(points.items()):
            cursor = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id,code,tier,label,definition,positive_evidence_json,
                    negative_evidence_json,boundary_rules_json,enabled,matcher_rule_json
                ) VALUES ('v53',?,?,?,?,?,?,?,?,?)
                """,
                (
                    code,
                    point["tier"],
                    f"old-{code}",
                    "old",
                    canonical_json(point["positive_evidence"]),
                    canonical_json(point["negative_evidence"]),
                    canonical_json(point["boundary_rules"]),
                    1,
                    point["matcher_rule_json"],
                ),
            )
            assert cursor.lastrowid is not None
            connection.executemany(
                "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
                [(int(cursor.lastrowid), scene) for scene in point["scenes"]],
            )
        connection.commit()
        connection.close()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_single_transaction_materialization_is_complete_and_idempotent(self) -> None:
        dry_run = materialize_v5_3_draft(db_path=self.db, dry_run=True)
        self.assertTrue(dry_run["dry_run"])
        self.assertEqual(dry_run["changed_points"], 28)
        receipt = materialize_v5_3_draft(db_path=self.db)
        self.assertFalse(receipt["dry_run"])
        self.assertEqual(receipt["point_count"], 28)
        self.assertEqual(receipt["changed_points"], 28)
        with sqlite3.connect(self.db) as connection:
            source_sha = connection.execute(
                "SELECT source_sha256 FROM taxonomy_versions WHERE id='v53'"
            ).fetchone()[0]
            labels = dict(
                connection.execute(
                    "SELECT code,label FROM selling_points WHERE taxonomy_id='v53'"
                )
            )
        self.assertEqual(source_sha, receipt["source_sha256"])
        self.assertEqual(
            labels["E2"],
            "通过懂车帝购买海量靠谱二手车，查看透明车况和价格有保障",
        )
        stable = materialize_v5_3_draft(db_path=self.db)
        self.assertEqual(stable["changed_points"], 0)
        self.assertEqual(stable["point_content_sha256"], receipt["point_content_sha256"])

    def test_missing_point_or_release_reference_fails_closed(self) -> None:
        with sqlite3.connect(self.db) as connection:
            connection.execute(
                "DELETE FROM selling_points WHERE taxonomy_id='v53' AND code='X11'"
            )
            connection.commit()
        with self.assertRaisesRegex(SellingPointG0Error, "point set drifted"):
            materialize_v5_3_draft(db_path=self.db)

        self.tearDown()
        self.setUp()
        with sqlite3.connect(self.db) as connection:
            connection.execute(
                "INSERT INTO evaluation_releases(id,taxonomy_version) VALUES ('r','selling-points-v5.3')"
            )
            connection.commit()
        with self.assertRaisesRegex(SellingPointG0Error, "frozen"):
            materialize_v5_3_draft(db_path=self.db)


if __name__ == "__main__":
    unittest.main()
