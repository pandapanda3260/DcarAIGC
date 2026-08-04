from __future__ import annotations

import hashlib
import itertools
import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from PIL import Image, ImageDraw

from v8.duplicates import (
    CALIBRATION_PATH,
    FINGERPRINT_VERSION,
    THRESHOLDS,
    _image_phash,
    _pending_content_ids,
    _simhash,
    calibrate,
    calibration_ready,
    compare_fingerprints,
    fingerprint_content,
    rebuild_duplicate_relations,
    run_duplicate_fingerprint_queue,
)
from v8.storage import connect, initialize_database, now_utc


class V8DuplicateDetectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "duplicates.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _content(self, link_id: str, *, published_at: str | None = None) -> int:
        captured_at = now_utc()
        with connect(self.db) as connection:
            cursor = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, title, body,
                    published_at, imported_at, created_at, updated_at
                ) VALUES (?, 'douyin', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    link_id, link_id, f"https://www.douyin.com/video/{link_id}",
                    f"汽车内容 {link_id}", f"汽车正文证据 {link_id}", published_at,
                    captured_at, captured_at, captured_at,
                ),
            )
            connection.commit()
            if cursor.lastrowid is None:
                raise RuntimeError("missing content id")
            return int(cursor.lastrowid)

    def _insert_fingerprint(
        self, content_id: int, *, text_sha256: str, simhash: str
    ) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO duplicate_fingerprints(
                    content_id,fingerprint_version,source_sha256,text_sha256,
                    media_sha256_json,frame_phashes_json,text_simhash,
                    text_char_count,payload_json,created_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    content_id, FINGERPRINT_VERSION, hashlib.sha256(str(content_id).encode()).hexdigest(),
                    text_sha256, "[]", "[]", simhash, 100, "{}", captured_at,
                ),
            )
            connection.commit()

    def _insert_artifact(
        self,
        content_id: int,
        *,
        suffix: str,
        body: bytes,
        created_at: str,
    ) -> Path:
        path = self.root / f"{content_id}-{suffix}.json"
        path.write_bytes(body)
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id,artifact_type,local_path,status,byte_size,sha256,
                    captured_at,processor_version,created_at
                ) VALUES (?, 'asr', ?, 'available', ?, ?, ?, 'test', ?)
                """,
                (
                    content_id,
                    str(path),
                    len(body),
                    hashlib.sha256(body).hexdigest(),
                    created_at,
                    created_at,
                ),
            )
            connection.commit()
        return path

    def test_phash_survives_jpeg_reencode_and_simhash_survives_punctuation(self) -> None:
        source = self.root / "source.png"
        reencoded = self.root / "reencoded.jpg"
        image = Image.new("RGB", (256, 192), "#6688aa")
        draw = ImageDraw.Draw(image)
        draw.rectangle((20, 80, 235, 155), fill="#b51f2e")
        draw.ellipse((45, 135, 85, 175), fill="#111111")
        draw.ellipse((170, 135, 210, 175), fill="#111111")
        draw.polygon([(70, 80), (110, 40), (175, 40), (210, 80)], fill="#dddddd")
        image.save(source)
        image.save(reencoded, quality=70)
        left = _image_phash(source)
        right = _image_phash(reencoded)
        self.assertIsNotNone(left)
        self.assertIsNotNone(right)
        self.assertLessEqual((int(left or "0", 16) ^ int(right or "0", 16)).bit_count(), 6)
        self.assertEqual(_simhash("懂车帝，汽车保养！新车。"), _simhash("懂车帝汽车保养新车"))

    def test_frozen_calibration_dataset_has_150_unique_balanced_pairs(self) -> None:
        dataset = json.loads(CALIBRATION_PATH.read_text(encoding="utf-8"))
        pairs = dataset["pairs"]
        identities = {
            tuple(sorted((item["left_link_id"], item["right_link_id"]))) for item in pairs
        }
        labels = Counter(item["label"] for item in pairs)
        self.assertEqual(len(pairs), 150)
        self.assertEqual(len(identities), 150)
        self.assertEqual(labels, {"duplicate": 75, "distinct": 75})
        self.assertTrue(all(item["rationale"] for item in pairs))

    def test_combined_visual_and_semantic_evidence_confirms_duplicate(self) -> None:
        text_hash = hashlib.sha256(b"left").hexdigest()
        left = {
            "media_sha256_json": "[]", "frame_phashes_json": json.dumps([
                "8f0f0f0f0f0f0f0f", "f0f00f0f0f0f0f0f", "cccc3333cccc3333"
            ]),
            "text_sha256": text_hash, "text_simhash": "1234567890abcdef",
            "asr_simhash": "fedcba0987654321", "ocr_simhash": None,
        }
        right = {
            "media_sha256_json": "[]", "frame_phashes_json": json.dumps([
                "8f0f0f0f0f0f0f0e", "f0f00f0f0f0f0f0e", "cccc3333cccc3332"
            ]),
            "text_sha256": hashlib.sha256(b"right").hexdigest(),
            "text_simhash": "1234567890abcdee", "asr_simhash": "fedcba0987654320",
            "ocr_simhash": None,
        }
        result = compare_fingerprints(left, right)
        self.assertTrue(result["confirmed"])
        self.assertIn("phash_plus_semantic", result["reasons"])
        self.assertLessEqual(result["phash_distance"], THRESHOLDS["phash_confirm_distance"])

    def test_identical_asr_and_ocr_without_visual_match_is_not_a_duplicate(self) -> None:
        left = {
            "media_sha256_json": "[]", "frame_phashes_json": json.dumps([
                "8f0f0f0f0f0f0f0f", "f0f00f0f0f0f0f0f", "cccc3333cccc3333"
            ]),
            "text_sha256": hashlib.sha256(b"left").hexdigest(),
            "text_simhash": "1234567890abcdef", "asr_simhash": "fedcba0987654321",
            "ocr_simhash": "1111222233334444",
        }
        right = {
            "media_sha256_json": "[]", "frame_phashes_json": json.dumps([
                "1111111122222222", "3333333344444444", "5555555566666666"
            ]),
            "text_sha256": hashlib.sha256(b"right").hexdigest(),
            "text_simhash": "9999aaaabbbbcccc", "asr_simhash": "fedcba0987654321",
            "ocr_simhash": "1111222233334444",
        }
        result = compare_fingerprints(left, right)
        self.assertFalse(result["confirmed"])
        self.assertGreater(result["phash_distance"], THRESHOLDS["phash_confirm_distance"])

    def test_fingerprint_slot_is_idempotent_and_queue_does_not_reprocess_itself(self) -> None:
        content_id = self._content("A2BC3D")
        first = fingerprint_content(content_id, db_path=self.db)
        second = fingerprint_content(content_id, db_path=self.db)
        self.assertEqual(first["source_sha256"], second["source_sha256"])
        queued = run_duplicate_fingerprint_queue(limit=None, db_path=self.db)
        self.assertEqual(queued["candidates"], 0)
        with connect(self.db) as connection:
            slots = connection.execute(
                "SELECT status,attempt_count FROM media_processing_slots WHERE processor_type='duplicate_fingerprint'"
            ).fetchall()
            fingerprints = connection.execute("SELECT * FROM duplicate_fingerprints").fetchall()
        self.assertEqual([(row["status"], row["attempt_count"]) for row in slots], [("succeeded", 1)])
        self.assertEqual(len(fingerprints), 1)

    def test_fingerprint_queue_ignores_timestamp_churn_for_same_source_sha256(self) -> None:
        content_id = self._content("Q2BC3D")
        asr_body = json.dumps(
            {"status": "success", "text": "相同的汽车语音证据"},
            ensure_ascii=False,
        ).encode("utf-8")
        self._insert_artifact(
            content_id,
            suffix="initial-asr",
            body=asr_body,
            created_at="2026-08-01T00:00:00Z",
        )
        first = fingerprint_content(content_id, db_path=self.db)
        self._insert_artifact(
            content_id,
            suffix="timestamp-only-asr",
            body=asr_body,
            created_at="2099-01-01T00:00:00Z",
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET updated_at='2099-01-01T00:00:00Z' WHERE id=?",
                (content_id,),
            )
            connection.commit()

        queued = run_duplicate_fingerprint_queue(limit=None, db_path=self.db)

        self.assertEqual(queued["candidates"], 0)
        with connect(self.db) as connection:
            current = connection.execute(
                """
                SELECT source_sha256 FROM duplicate_fingerprints
                WHERE content_id=? ORDER BY id DESC LIMIT 1
                """,
                (content_id,),
            ).fetchone()
        self.assertEqual(current["source_sha256"], first["source_sha256"])

    def test_fingerprint_queue_detects_source_hash_change_with_stale_timestamp(self) -> None:
        content_id = self._content("R2BC3D")
        first = fingerprint_content(content_id, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE content_items
                SET title='真实变化后的汽车内容', updated_at='2000-01-01T00:00:00Z'
                WHERE id=?
                """,
                (content_id,),
            )
            connection.commit()

        queued = run_duplicate_fingerprint_queue(limit=None, db_path=self.db)

        self.assertEqual(queued["candidates"], 1)
        self.assertEqual(queued["processed"], 1)
        with connect(self.db) as connection:
            rows = connection.execute(
                """
                SELECT source_sha256 FROM duplicate_fingerprints
                WHERE content_id=? ORDER BY id
                """,
                (content_id,),
            ).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[-1]["source_sha256"], first["source_sha256"])

    def test_pending_fingerprint_limit_is_applied_after_source_hash_filtering(self) -> None:
        false_candidate = self._content("S2BC3D")
        true_candidate = self._content("T2BC3D")
        asr_body = b'{"status":"success","text":"same source"}'
        self._insert_artifact(
            false_candidate,
            suffix="initial-limit-asr",
            body=asr_body,
            created_at="2026-08-01T00:00:00Z",
        )
        fingerprint_content(false_candidate, db_path=self.db)
        fingerprint_content(true_candidate, db_path=self.db)
        self._insert_artifact(
            false_candidate,
            suffix="timestamp-limit-asr",
            body=asr_body,
            created_at="2099-01-01T00:00:00Z",
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET updated_at='2099-01-01T00:00:00Z' WHERE id=?",
                (false_candidate,),
            )
            connection.execute(
                """
                UPDATE content_items
                SET body='真实变化但时间戳不前进', updated_at='2000-01-01T00:00:00Z'
                WHERE id=?
                """,
                (true_candidate,),
            )
            connection.commit()

        self.assertEqual(
            _pending_content_ids(limit=1, db_path=self.db),
            [true_candidate],
        )

    def test_queue_rebuilds_relations_only_after_the_full_backlog_is_drained(self) -> None:
        self._content("U2BC3D")
        self._content("V2BC3D")
        rebuilt_payload = {"duplicate_relations": 0}

        with (
            patch("v8.duplicates.calibration_ready", return_value=True),
            patch(
                "v8.duplicates.rebuild_duplicate_relations",
                return_value=rebuilt_payload,
            ) as rebuild,
        ):
            first = run_duplicate_fingerprint_queue(limit=1, db_path=self.db)
            self.assertEqual(first["processed"], 1)
            self.assertIsNone(first["relations"])
            rebuild.assert_not_called()

            second = run_duplicate_fingerprint_queue(limit=1, db_path=self.db)
            self.assertEqual(second["processed"], 1)
            self.assertEqual(second["relations"], rebuilt_payload)
            rebuild.assert_called_once_with(db_path=self.db)

    def test_queue_does_not_rebuild_when_batch_reports_failure_even_if_fingerprint_was_written(self) -> None:
        successful_id = self._content("W2BC3D")
        content_id = self._content("X2BC3D")
        real_fingerprint = fingerprint_content

        def fingerprint_then_fail(value: int, *, db_path: Path) -> dict[str, object]:
            result = real_fingerprint(value, db_path=db_path)
            if value == content_id:
                raise RuntimeError("post-write validation failed")
            return result

        with (
            patch("v8.duplicates.calibration_ready", return_value=True),
            patch(
                "v8.duplicates.fingerprint_content",
                side_effect=fingerprint_then_fail,
            ),
            patch("v8.duplicates.rebuild_duplicate_relations") as rebuild,
        ):
            result = run_duplicate_fingerprint_queue(limit=None, db_path=self.db)

        self.assertEqual(result["processed"], 1)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(_pending_content_ids(limit=1, db_path=self.db), [])
        rebuild.assert_not_called()
        self.assertEqual(result["failures"][0]["content_id"], content_id)
        self.assertNotEqual(successful_id, content_id)

    def test_150_pair_calibration_gates_relations_and_uses_earliest_original(self) -> None:
        duplicate_links = [f"D{i:05d}" for i in range(13)]
        distinct_links = [f"N{i:05d}" for i in range(6)]
        same_simhash = _simhash("完全相同的汽车媒体内容与语音证据")
        self.assertIsNotNone(same_simhash)
        for index, link_id in enumerate(duplicate_links):
            content_id = self._content(
                link_id,
                published_at=f"2026-07-{index + 1:02d}T00:00:00Z",
            )
            self._insert_fingerprint(content_id, text_sha256="same-text", simhash=str(same_simhash))
        for index, link_id in enumerate(distinct_links):
            content_id = self._content(link_id)
            value = f"完全不同的汽车主题与证据编号{index}号"
            self._insert_fingerprint(
                content_id,
                text_sha256=hashlib.sha256(value.encode()).hexdigest(),
                simhash=str(_simhash(value)),
            )
        positive_pairs = list(itertools.combinations(duplicate_links, 2))[:75]
        negative_pairs = list(itertools.product(duplicate_links, distinct_links))[:75]
        dataset = self.root / "calibration.json"
        dataset.write_text(
            json.dumps(
                {
                    "version": "duplicate-calibration-test-v1",
                    "pairs": [
                        {"left_link_id": left, "right_link_id": right, "label": "duplicate"}
                        for left, right in positive_pairs
                    ] + [
                        {"left_link_id": left, "right_link_id": right, "label": "distinct"}
                        for left, right in negative_pairs
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        calibration = calibrate(dataset, db_path=self.db)
        self.assertEqual(calibration["status"], "passed")
        self.assertEqual(calibration["pair_count"], 150)
        self.assertGreaterEqual(calibration["precision"], 0.95)
        self.assertTrue(calibration_ready(db_path=self.db))
        rebuilt = rebuild_duplicate_relations(db_path=self.db)
        self.assertEqual(rebuilt["duplicate_relations"], 12)
        with connect(self.db) as connection:
            originals = connection.execute(
                "SELECT DISTINCT original.link_id FROM duplicate_relations d JOIN content_items original ON original.id=d.original_content_id"
            ).fetchall()
        self.assertEqual([row[0] for row in originals], ["D00000"])


if __name__ == "__main__":
    unittest.main()
