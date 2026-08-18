from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.backfill as backfill_module
import v8.capture as capture_module
from v8.backfill import (
    BUDGET_ID,
    OPERATION,
    PROVIDER,
    RnoteVideoAdapter,
    normalize_video_detail,
    prepare_backfill_slots,
    process_paid_candidate,
    run_daily_backfill_batch,
    run_pilot,
)
from v8.capture import (
    CaptureError,
    ProviderResult,
    activate_pilot_budget,
    ensure_content_slot,
)
from v8.media import Artifact
from v8.storage import connect, initialize_database, now_utc


def provider_payload(note_id: str, *, with_video: bool = True) -> dict:
    note = {
        "id": note_id,
        "title": "刷新标题",
        "desc": "刷新正文",
        "time": 1775907584,
        "video_info_v2": {
            "master_url": "https://cdn.example/video.mp4" if with_video else ""
        },
    }
    return {"success": True, "billed": True, "data": {"note_list": [note]}}


class FakeAdapter:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.calls = 0

    def fetch(self, note_id: str) -> ProviderResult:
        self.calls += 1
        return ProviderResult(
            data=normalize_video_detail(self.payload, note_id),
            raw_response=self.payload,
            http_status=200,
            billed=True,
        )


class V8BackfillTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "backfill.sqlite3"
        self.raw_root = self.root / "raw"
        raw_root_patch = patch.object(capture_module, "RAW_ROOT", self.raw_root)
        raw_root_patch.start()
        self.addCleanup(raw_root_patch.stop)
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            for index, note_id in enumerate(("a" * 24, "b" * 24), 1):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id, platform, platform_content_id, canonical_url,
                        title, body, content_type, imported_at, created_at, updated_at
                    ) VALUES (?, 'xiaohongshu', ?, ?, '', '', 'video', ?, ?, ?)
                    """,
                    (
                        f"A2BC3{index}", note_id,
                        f"https://www.xiaohongshu.com/explore/{note_id}",
                        captured_at, captured_at, captured_at,
                    ),
                )
            connection.execute(
                """
                INSERT INTO provider_budget_batches(
                    id, purpose, provider, operation, currency, verified_unit_price,
                    max_billable_requests, max_amount, pilot_size, daily_quota,
                    price_verified_at, status, created_at, updated_at
                ) VALUES (?, 'test', ?, ?, 'USD', 0.008, 2, 0.016, 2, 2,
                          ?, 'draft', ?, ?)
                """,
                (BUDGET_ID, PROVIDER, OPERATION, captured_at, captured_at, captured_at),
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _seed_refresh_slot(self, content_id: int) -> None:
        """v16 后没有补证台账，测试自行准备一次性刷新槽。"""

        with connect(self.db) as connection:
            ensure_content_slot(
                connection,
                content_id=content_id,
                stage="media_source_refresh",
                window_key="lifetime",
                provider=PROVIDER,
                adapter_version="rnote-video-v8.0",
            )
            connection.commit()

    def test_prepare_is_a_no_op_after_the_queue_ledger_was_removed(self) -> None:
        """v16 删除 review_queue 后补证台账不存在，候选恒为空。"""

        result = prepare_backfill_slots(db_path=self.db)
        self.assertEqual(result, {"queue_total": 0, "local": 0, "paid": 0})
        with connect(self.db) as connection:
            rows = connection.execute("SELECT * FROM fetch_slots").fetchall()
        self.assertEqual(rows, [])

    def test_retired_adapter_fails_closed_before_network(self) -> None:
        with (
            patch("v8.backfill.urllib.request.urlopen") as urlopen,
            self.assertRaises(CaptureError) as raised,
        ):
            RnoteVideoAdapter("invalid").fetch("a" * 24)
        urlopen.assert_not_called()
        self.assertFalse(raised.exception.retryable)
        self.assertEqual(raised.exception.error_code, "provider_retired")
        self.assertEqual(str(raised.exception), "Rnote retired; use TikHub")

    def test_paid_candidate_saves_raw_source_and_routes_to_evidence_ready(self) -> None:
        self._seed_refresh_slot(1)
        activate_pilot_budget(BUDGET_ID, expected_unit_price=0.008, db_path=self.db)
        adapter = FakeAdapter(provider_payload("a" * 24))
        item = {
            "content_id": 1,
            "link_id": "A2BC31",
            "note_id": "a" * 24,
        }
        media_artifact = Artifact(
            id=99,
            content_id=1,
            artifact_type="media",
            local_path=str(self.root / "source.mp4"),
            sha256="f" * 64,
            processor_version="provider-media-download-v8.0",
        )
        with (
            patch("v8.backfill.MEDIA_ROOT", self.root / "media"),
            patch("v8.backfill.download_video_sources", return_value=media_artifact),
            patch("v8.backfill.process_video_evidence", return_value={}),
        ):
            status = process_paid_candidate(item, adapter=adapter, db_path=self.db)
        self.assertEqual(status, "evidence_ready")
        self.assertEqual(adapter.calls, 1)
        with connect(self.db) as connection:
            raw_count = connection.execute("SELECT COUNT(*) FROM provider_raw_responses").fetchone()[0]
            content = connection.execute(
                "SELECT title, body, published_at FROM content_items WHERE id=1"
            ).fetchone()
        self.assertEqual(raw_count, 1)
        self.assertEqual(content["title"], "刷新标题")
        self.assertEqual(content["body"], "刷新正文")
        self.assertTrue(content["published_at"].endswith("Z"))

    def test_successful_detail_without_video_is_terminal_not_retried(self) -> None:
        self._seed_refresh_slot(1)
        activate_pilot_budget(BUDGET_ID, expected_unit_price=0.008, db_path=self.db)
        adapter = FakeAdapter(provider_payload("a" * 24, with_video=False))
        with patch("v8.backfill.MEDIA_ROOT", self.root / "media"):
            status = process_paid_candidate(
                {"content_id": 1, "link_id": "A2BC31", "note_id": "a" * 24},
                adapter=adapter,
                db_path=self.db,
            )
        self.assertEqual(status, "terminal_failed")
        with connect(self.db) as connection:
            slot = connection.execute(
                "SELECT status FROM fetch_slots WHERE content_id=1"
            ).fetchone()[0]
        self.assertEqual(slot, "succeeded")

    def test_retired_pilot_and_daily_batch_do_not_read_key_or_mutate_state(self) -> None:
        self._seed_refresh_slot(1)
        with (
            patch("v8.backfill.load_key") as load_key,
            patch("v8.backfill.urllib.request.urlopen") as urlopen,
            self.assertRaisesRegex(CaptureError, "Rnote retired; use TikHub"),
        ):
            run_pilot(db_path=self.db, key_file=self.root / "unused")
        load_key.assert_not_called()
        urlopen.assert_not_called()
        with self.assertRaisesRegex(CaptureError, "Rnote retired; use TikHub"):
            run_daily_backfill_batch(db_path=self.db, key_file=self.root / "unused")
        with connect(self.db) as connection:
            attempts = connection.execute("SELECT * FROM fetch_attempts").fetchall()
            budget = connection.execute(
                "SELECT status, consumed_amount FROM provider_budget_batches"
            ).fetchone()
        self.assertEqual(attempts, [])
        self.assertEqual(budget["status"], "draft")
        self.assertEqual(budget["consumed_amount"], 0)

    def test_pilot_cli_is_retired_before_credentials_or_network(self) -> None:
        arguments = type(
            "Arguments",
            (),
            {"command": "pilot", "db": self.db, "key_file": self.root / "missing"},
        )()
        with (
            patch("v8.backfill.parse_args", return_value=arguments),
            patch("v8.backfill.load_key") as load_key,
            patch("v8.backfill.urllib.request.urlopen") as urlopen,
            self.assertRaisesRegex(CaptureError, "Rnote retired; use TikHub"),
        ):
            backfill_module.main()
        load_key.assert_not_called()
        urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
