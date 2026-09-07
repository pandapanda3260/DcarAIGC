from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from tests.roster_fixture import accept_roster
from v8 import providers, tikhub_scan
from v8.capture import ProviderResult
from v8.durable_runs import get_run, recover_run
from v8.storage import connect, initialize_database, transaction

NOW = "2026-08-29T04:00:00Z"
START = "2026-08-21T16:00:00Z"
END = "2026-08-28T16:00:00Z"
PUBLISHED = "2026-08-27T12:00:00Z"
UID = "12345678901"
REFERENCE = "MS4wLjAB" + "a" * 64


def _epoch(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())


def _later(value: str = NOW, seconds: int = 301) -> str:
    return (
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        + timedelta(seconds=seconds)
    ).isoformat()


def _item(number: int) -> dict[str, object]:
    return {
        "aweme_id": str(7600000000000000000 + number),
        "desc": f"fixture {number}",
        "author": {"uid": UID, "nickname": "fixture"},
        "create_time": _epoch(PUBLISHED),
        "video": {"play_addr": {"url_list": ["https://fixture.invalid/video.mp4"]}},
        "statistics": {"digg_count": 10, "comment_count": 2},
    }


def _page(
    items: list[dict[str, object]], *, more: bool = False, cursor: int | None = None,
) -> dict[str, object]:
    data: dict[str, object] = {"aweme_list": items, "has_more": more}
    if cursor is not None:
        data["max_cursor"] = cursor
    return {"code": 200, "data": data}


def _result(body: dict[str, object]) -> ProviderResult:
    return ProviderResult({}, copy.deepcopy(body), 200, True)


class LocalMaterializationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.db = self.root / "scan.sqlite3"
        self.raw_root = self.root / "raw"
        with connect(self.db) as connection:
            initialize_database(connection)
            account_id = connection.execute(
                "INSERT INTO accounts(phone,created_at,updated_at) VALUES ('',?,?)",
                (NOW, NOW),
            ).lastrowid
            connection.execute(
                "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) "
                "VALUES (?,'douyin',?,?,?)",
                (account_id, UID, NOW, NOW),
            )
            self.snapshot = accept_roster(connection, accepted_at=NOW)
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,"
                "reference_value,created_at,updated_at) VALUES (1,'TikHub','sec_user_id',?,?,?)",
                (REFERENCE, NOW, NOW),
            )
        for target in (
            "v8.capture.now_utc",
            "v8.providers.now_utc",
            "v8.tikhub_scan.now_utc",
        ):
            clock = patch(target, return_value=NOW)
            clock.start()
            self.addCleanup(clock.stop)
        network = patch(
            "urllib.request.urlopen",
            side_effect=AssertionError("Network forbidden in local materialization tests"),
        )
        network.start()
        self.addCleanup(network.stop)
        key = patch.object(providers, "_load_key", return_value="fixture-only")
        key.start()
        self.addCleanup(key.stop)

    def _scan(
        self,
        items: list[dict[str, object]],
        *,
        more: bool = True,
        cursor: int | None = 7,
        purpose: str = "reconcile",
        task_id: str | None = None,
    ) -> dict[str, object]:
        with patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=sqlite3.OperationalError("fixture materialization lock"),
        ):
            result = tikhub_scan.run_account_scan(
                1,
                window_start=START,
                window_end=END,
                purpose=purpose,
                roster_snapshot_id=self.snapshot["id"],
                roster_snapshot_hash=self.snapshot["members_sha256"],
                db_path=self.db,
                raw_root=self.raw_root,
                now=NOW,
                task_id=task_id,
                call_override=lambda _operation, _request: _result(
                    _page(items, more=more, cursor=cursor)
                ),
            )
        self.assertEqual((result["status"], result["reason"]), ("partial", "materialization_pending"))
        return result

    def _state(self, run_id: int) -> dict[str, object]:
        return get_run(run_id, db_path=self.db)["details"]["checkpoint"]

    def _resume(
        self, run_id: int, *, now: str = _later(), max_items: int = 50, deadline: float = 60.0,
    ) -> dict[str, object]:
        return tikhub_scan.resume_local_materialization(
            run_id,
            db_path=self.db,
            raw_root=self.raw_root,
            now=now,
            max_items=max_items,
            deadline=deadline,
        )

    def _source(self) -> str:
        with connect(self.db) as connection:
            return str(connection.execute(
                "SELECT source FROM provider_raw_responses ORDER BY id LIMIT 1"
            ).fetchone()[0])

    def test_rejects_missing_pending_and_contract_mismatch_before_claim(self) -> None:
        complete = tikhub_scan.run_account_scan(
            1,
            window_start=START,
            window_end=END,
            purpose="reconcile",
            roster_snapshot_id=self.snapshot["id"],
            roster_snapshot_hash=self.snapshot["members_sha256"],
            db_path=self.db,
            raw_root=self.raw_root,
            now=NOW,
            call_override=lambda _operation, _request: _result(_page([], more=False)),
        )
        with patch.object(
            tikhub_scan, "claim_run", side_effect=AssertionError("must reject before claim"),
        ), self.assertRaisesRegex(tikhub_scan.TikHubScanError, "no pending") as missing:
            self._resume(int(complete["scheduler_run_id"]))
        self.assertEqual(missing.exception.reason, "materialization_not_pending")

        pending = self._scan([_item(1)], task_id="contract-mismatch")
        run_id = int(pending["scheduler_run_id"])
        with connect(self.db) as connection, transaction(connection):
            run = get_run(run_id, db_path=self.db)
            details = run["details"]
            details["checkpoint"]["pending_materialization"]["identity"][
                "contract_version"
            ] = "unknown-contract"
            connection.execute(
                "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True, separators=(",", ":")), run_id),
            )
        with patch.object(
            tikhub_scan, "claim_run", side_effect=AssertionError("must reject before claim"),
        ), self.assertRaises(tikhub_scan.TikHubScanError) as mismatch:
            self._resume(run_id)
        self.assertEqual(mismatch.exception.reason, "materialization_integrity_error")

    def test_max_items_checkpoints_prefix_and_preserves_parent_cursor(self) -> None:
        pending = self._scan([_item(1), _item(2), _item(3)])
        run_id = int(pending["scheduler_run_id"])
        before = self._state(run_id)
        frozen = {
            key: copy.deepcopy(before[key])
            for key in (
                "cursor",
                "provider_next_cursor",
                "page_number",
                "counts",
                "last_manifest",
            )
        }
        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0), patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ), patch.object(
            tikhub_scan, "_provider_call", side_effect=AssertionError("provider path forbidden"),
        ):
            first = self._resume(run_id, max_items=2)
        self.assertEqual(
            (
                first["status"],
                first["reason"],
                first["processed_items"],
                first["succeeded_items"],
                first["remaining_items"],
                first["next_item_offset"],
            ),
            ("partial", "local_materialization_yield", 2, 2, 1, 2),
        )
        self.assertFalse(first["materialization_finalized"])
        state = self._state(run_id)
        self.assertIsNotNone(state["pending_materialization"])
        self.assertEqual({key: state[key] for key in frozen}, frozen)
        self.assertEqual(self._source(), "materialization_pending")
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT details_json,status FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        child_state = json.loads(child["details_json"])["checkpoint"]
        self.assertEqual(child["status"], "partial")
        self.assertEqual(
            (child_state["completed_indexes"], child_state["next_item_offset"]),
            ([0, 1], 2),
        )

        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0), patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ):
            final = self._resume(
                run_id,
                now=str(first["next_resume_at"]),
                max_items=1,
            )
        self.assertEqual(
            (
                final["status"],
                final["reason"],
                final["processed_items"],
                final["remaining_items"],
                final["next_item_offset"],
            ),
            ("partial", "materialization_replay_yield", 1, 0, 3),
        )
        self.assertTrue(final["materialization_finalized"])
        after = self._state(run_id)
        self.assertIsNone(after["pending_materialization"])
        self.assertEqual({key: after[key] for key in frozen}, frozen)
        self.assertEqual(self._source(), "derived_applied")

    def test_monotonic_deadline_stops_before_claiming_the_next_item(self) -> None:
        pending = self._scan([_item(1), _item(2), _item(3)])
        run_id = int(pending["scheduler_run_id"])
        real_materialize = providers.materialize_account_discovery_page
        with patch.object(
            tikhub_scan, "_monotonic_now", side_effect=[0.0, 0.0, 0.0, 1.0],
        ), patch.object(
            providers,
            "materialize_account_discovery_page",
            wraps=real_materialize,
        ) as materialize, patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ):
            result = self._resume(run_id, max_items=3, deadline=1.0)
        self.assertEqual(materialize.call_count, 1)
        self.assertEqual(
            (
                result["processed_items"],
                result["succeeded_items"],
                result["remaining_items"],
                result["next_item_offset"],
                result["deadline_reached"],
            ),
            (1, 1, 2, 1, True),
        )
        self.assertIsNotNone(self._state(run_id)["pending_materialization"])

    def test_expired_deadline_after_preflight_creates_no_attempt_or_usage(self) -> None:
        pending = self._scan([_item(1), _item(2)])
        run_id = int(pending["scheduler_run_id"])
        with connect(self.db) as connection:
            attempts_before = int(connection.execute(
                "SELECT COUNT(*) FROM scheduler_run_attempts"
            ).fetchone()[0])
            usage_before = int(connection.execute(
                "SELECT COUNT(*) FROM provider_usage"
            ).fetchone()[0])
        with patch.object(
            tikhub_scan, "_monotonic_now", side_effect=[0.0, 1.0],
        ), patch.object(
            tikhub_scan, "claim_run", side_effect=AssertionError("expired work must not claim"),
        ), patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ):
            result = self._resume(run_id, max_items=2, deadline=1.0)
        self.assertEqual(
            (
                result["reason"],
                result["processed_items"],
                result["remaining_items"],
                result["deadline_reached"],
            ),
            ("local_materialization_yield", 0, 2, True),
        )
        with connect(self.db) as connection:
            self.assertEqual(
                int(connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts"
                ).fetchone()[0]),
                attempts_before,
            )
            self.assertEqual(
                int(connection.execute(
                    "SELECT COUNT(*) FROM provider_usage"
                ).fetchone()[0]),
                usage_before,
            )

    def test_failed_item_counts_one_keeps_continuation_and_never_dispatches(self) -> None:
        pending = self._scan([_item(1), _item(2)])
        run_id = int(pending["scheduler_run_id"])
        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0), patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=sqlite3.OperationalError("local write failed"),
        ), patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ), patch.object(
            tikhub_scan, "_provider_call", side_effect=AssertionError("provider path forbidden"),
        ):
            result = self._resume(run_id, max_items=2)
        self.assertEqual(
            (
                result["status"],
                result["reason"],
                result["processed_items"],
                result["succeeded_items"],
                result["failed_items"],
                result["remaining_items"],
                result["next_item_offset"],
            ),
            ("partial", "materialization_pending", 1, 0, 1, 2, 0),
        )
        self.assertEqual(self._source(), "materialization_pending")
        self.assertIsNotNone(self._state(run_id)["pending_materialization"])
        with connect(self.db) as connection:
            child = connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE job_id=?",
                (tikhub_scan.MATERIALIZATION_JOB,),
            ).fetchone()
        child_details = json.loads(child["details_json"])
        self.assertEqual(result["next_resume_at"], child_details["next_resume_at"])

    def test_empty_pending_and_completed_child_finalize_for_one_unit(self) -> None:
        empty = self._scan([], more=False, cursor=None)
        empty_id = int(empty["scheduler_run_id"])
        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0), patch.object(
            tikhub_scan, "_reference", side_effect=AssertionError("reference path forbidden"),
        ), patch.object(
            tikhub_scan, "_raw", side_effect=AssertionError("raw fetch path forbidden"),
        ):
            finalized = self._resume(empty_id)
        self.assertEqual(
            (
                finalized["status"],
                finalized["complete"],
                finalized["processed_items"],
                finalized["succeeded_items"],
                finalized["remaining_items"],
            ),
            ("succeeded", True, 1, 0, 0),
        )
        self.assertTrue(finalized["materialization_finalized"])

        real_finish_parent = tikhub_scan._finish_materialization_parent
        with patch.object(
            tikhub_scan,
            "_finish_materialization_parent",
            side_effect=RuntimeError("crash after child completion"),
        ), self.assertRaises(RuntimeError):
            tikhub_scan.run_account_scan(
                1,
                window_start=START,
                window_end=END,
                purpose="reconcile",
                roster_snapshot_id=self.snapshot["id"],
                roster_snapshot_hash=self.snapshot["members_sha256"],
                db_path=self.db,
                raw_root=self.raw_root,
                now=NOW,
                task_id="finish-only",
                call_override=lambda _operation, _request: _result(
                    _page([_item(9)], more=False)
                ),
            )
        with connect(self.db) as connection:
            history_id = int(connection.execute(
                "SELECT id FROM scheduler_runs WHERE job_id='tikhub_reconcile' "
                "ORDER BY id DESC LIMIT 1"
            ).fetchone()[0])
        history = get_run(history_id, db_path=self.db)
        self.assertTrue(recover_run(
            history_id,
            expected_attempt_id=history["details"]["owner"]["attempt_id"],
            db_path=self.db,
            now=_later(),
        ))
        with patch.object(tikhub_scan, "_monotonic_now", return_value=0.0), patch.object(
            providers,
            "materialize_account_discovery_page",
            side_effect=AssertionError("completed child must not replay"),
        ), patch.object(
            tikhub_scan, "_finish_materialization_parent", wraps=real_finish_parent,
        ):
            finish_only = self._resume(history_id, now=_later(seconds=302))
        self.assertEqual(
            (
                finish_only["status"],
                finish_only["processed_items"],
                finish_only["succeeded_items"],
                finish_only["remaining_items"],
            ),
            ("succeeded", 1, 0, 0),
        )
        self.assertTrue(finish_only["materialization_finalized"])

    def test_limits_reject_before_claim_and_history_uses_actual_job_name(self) -> None:
        pending = self._scan([_item(1)])
        run_id = int(pending["scheduler_run_id"])
        self.assertEqual(tikhub_scan._job_id("history"), "history_recovery")
        for kwargs in (
            {"max_items": True, "deadline": 60.0},
            {"max_items": 51, "deadline": 60.0},
            {"max_items": 1, "deadline": 60.0001},
        ):
            with self.subTest(kwargs=kwargs), patch.object(
                tikhub_scan, "_monotonic_now", return_value=0.0,
            ), patch.object(
                tikhub_scan, "claim_run", side_effect=AssertionError("invalid limit must not claim"),
            ), self.assertRaises(tikhub_scan.TikHubScanError):
                self._resume(run_id, **kwargs)


if __name__ == "__main__":
    unittest.main()
