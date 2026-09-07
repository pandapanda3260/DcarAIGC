from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from v8.account_operating_receipts import (
    ACCOUNT_STATUS_JOB,
    AccountOperatingStatusError,
    find_status_request,
    load_update_frequencies,
    record_status_receipt,
)
from v8.operations import upsert_account
from v8.scheduler import SchedulerJobError, execute_job, recover_interrupted_scheduler_runs
from v8.storage import connect, initialize_database, transaction


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class AccountOperatingReceiptsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "receipts.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection)
        self.accounts = [
            upsert_account({"platforms": [{"platform": "douyin", "uid": str(123450 + index)}]}, db_path=self.db)["id"]
            for index in range(3)
        ]

    def tearDown(self) -> None:
        self.connection.close()
        self.temporary.cleanup()

    def record(self, account_id, frequency, request_id):
        identity_id = self.connection.execute(
            "SELECT id FROM account_platform_identities WHERE account_id=?", (account_id,)
        ).fetchone()[0]
        previous = load_update_frequencies(self.connection, [account_id])[account_id]
        with transaction(self.connection):
            return record_status_receipt(
                self.connection, request_id=request_id, account_id=account_id,
                account_identity_id=identity_id, requested_status=frequency,
                update_frequency=frequency,
                request={"account_status": frequency, "fields": {}}, actor="tester", reason="manual label",
                before={"enabled": True, "update_frequency": previous},
                after={"enabled": True, "update_frequency": frequency},
                result={"id": account_id, "account_status": frequency, "update_frequency": frequency,
                        "enabled": True, "status_request_id": request_id},
                timestamp="2026-09-06T10:00:00.000001Z",
            )

    def test_batch_projection_uses_one_query_and_keeps_unmarked_accounts(self) -> None:
        first, second, unmarked = self.accounts
        self.record(first, "daily", "first-daily")
        self.record(second, "daily", "second-daily")
        self.record(first, "weekly", "first-weekly")
        statements = []
        self.connection.set_trace_callback(statements.append)
        try:
            self.assertEqual(load_update_frequencies(self.connection, self.accounts), {
                first: "weekly", second: "daily", unmarked: None,
            })
        finally:
            self.connection.set_trace_callback(None)
        self.assertEqual(len(statements), 1)
        self.assertEqual(load_update_frequencies(self.connection), {first: "weekly", second: "daily"})
        self.assertEqual(load_update_frequencies(self.connection, []), {})

    def test_latest_parent_corruption_errors_instead_of_falling_back(self) -> None:
        account_id = self.accounts[0]
        self.record(account_id, "daily", "first")
        newest = self.record(account_id, "weekly", "second")
        self.connection.execute("UPDATE scheduler_runs SET details_json='{}' WHERE id=?", (newest["run_id"],))
        self.connection.commit()
        with self.assertRaises(AccountOperatingStatusError) as caught:
            load_update_frequencies(self.connection, [account_id])
        self.assertEqual(caught.exception.code, "account_status_receipt_invalid")

    def test_untrusted_account_id_cannot_hide_corruption_from_batch(self) -> None:
        account_id = self.accounts[0]
        receipt = self.record(account_id, "daily", "first")
        receipt["payload"]["account_id"] = self.accounts[1]
        receipt["self_sha256"] = hashlib.sha256(canonical({
            key: value for key, value in receipt.items() if key != "self_sha256"
        }).encode()).hexdigest()
        self.connection.execute("UPDATE scheduler_runs SET details_json=? WHERE id=?", (canonical(receipt), receipt["run_id"]))
        self.connection.commit()
        with self.assertRaises(AccountOperatingStatusError):
            load_update_frequencies(self.connection, [account_id])

    def test_terminal_attempt_cannot_be_changed_deleted_or_parent_deleted(self) -> None:
        receipt = self.record(self.accounts[0], "daily", "first")
        for sql, value in (
            ("UPDATE scheduler_run_attempts SET details_json='{}' WHERE id=?", receipt["attempt_id"]),
            ("DELETE FROM scheduler_run_attempts WHERE id=?", receipt["attempt_id"]),
            ("DELETE FROM scheduler_runs WHERE id=?", receipt["run_id"]),
        ):
            with self.subTest(sql=sql), self.assertRaises(sqlite3.IntegrityError):
                with transaction(self.connection):
                    self.connection.execute(sql, (value,))
        self.assertEqual(load_update_frequencies(self.connection)[self.accounts[0]], "daily")

    def test_additional_terminal_attempt_invalidates_the_manual_receipt(self) -> None:
        receipt = self.record(self.accounts[0], "daily", "first")
        self.connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,"
            "status,started_at,completed_at,details_json) VALUES (?,2,'operator_retry','succeeded',?,?,?)",
            (receipt["run_id"], receipt["recorded_at"], receipt["recorded_at"], canonical(receipt)),
        )
        self.connection.commit()
        with self.assertRaises(AccountOperatingStatusError):
            find_status_request(self.connection, request_id="first")

    def test_parent_without_immutable_attempt_is_not_a_label(self) -> None:
        self.connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,completed_at,details_json) "
            "VALUES (?,?,'succeeded','now','now','{}')", (ACCOUNT_STATUS_JOB, "missing-attempt"),
        )
        self.connection.commit()
        with self.assertRaises(AccountOperatingStatusError):
            load_update_frequencies(self.connection)

    def _forge_receipt(self, original, transform):
        """Insert a corrupt terminal pair without disabling schema protections."""
        timestamp = original["recorded_at"]
        run_id = self.connection.execute(
            "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at) VALUES (?,?,'running',?)",
            (ACCOUNT_STATUS_JOB, "forged", timestamp),
        ).lastrowid
        attempt_id = self.connection.execute(
            "INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at) "
            "VALUES (?,1,'operator_retry','running',?)", (run_id, timestamp),
        ).lastrowid
        receipt = json.loads(canonical(original))
        receipt.update(run_id=run_id, attempt_id=attempt_id, request_id="forged")
        receipt["payload"]["result"]["status_request_id"] = "forged"
        receipt.pop("self_sha256")
        receipt["self_sha256"] = hashlib.sha256(canonical(receipt).encode()).hexdigest()
        encoded = transform(receipt)
        self.connection.execute(
            "UPDATE scheduler_run_attempts SET status='succeeded',completed_at=?,details_json=? WHERE id=?",
            (timestamp, encoded, attempt_id),
        )
        self.connection.execute(
            "UPDATE scheduler_runs SET status='succeeded',completed_at=?,details_json=? WHERE id=?",
            (timestamp, encoded, run_id),
        )

    def test_matching_parent_and_attempt_still_require_hash_canonical_json_and_ids(self) -> None:
        receipt = self.record(self.accounts[0], "daily", "first")
        def changed_field(field, value):
            def change(details):
                details[field] = value
                return canonical(details)
            return change
        transforms = (
            lambda value: canonical(value).replace('"self_sha256":"', '"self_sha256":"wrong'),
            lambda value: json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2),
            changed_field("run_id", 999999),
            changed_field("attempt_id", 999999),
            changed_field("contract_version", "provider-capture-v1"),
        )
        for index, transform in enumerate(transforms):
            with self.subTest(index=index), transaction(self.connection):
                self.connection.execute("SAVEPOINT corrupt_pair")
                self._forge_receipt(receipt, transform)
                with self.assertRaises(AccountOperatingStatusError):
                    load_update_frequencies(self.connection)
                self.connection.execute("ROLLBACK TO corrupt_pair")
                self.connection.execute("RELEASE corrupt_pair")
        self.assertEqual(load_update_frequencies(self.connection)[self.accounts[0]], "daily")

    def test_sqlite_backup_retains_receipts_and_schema19(self) -> None:
        self.record(self.accounts[0], "weekly", "first")
        target = connect(self.root / "backup.sqlite3")
        try:
            self.connection.backup(target)
            self.assertEqual(target.execute("PRAGMA user_version").fetchone()[0], 19)
            self.assertEqual(load_update_frequencies(target), {self.accounts[0]: "weekly"})
        finally:
            target.close()

    def test_completed_manual_receipt_is_never_registered_or_recovered_for_execution(self) -> None:
        receipt = self.record(self.accounts[0], "daily", "first")
        self.assertEqual(recover_interrupted_scheduler_runs(db_path=self.db), 0)
        with self.assertRaisesRegex(SchedulerJobError, "unknown scheduler job"):
            execute_job(ACCOUNT_STATUS_JOB, datetime.now(timezone.utc), db_path=self.db, allow_retry=True)
        self.assertEqual(find_status_request(self.connection, request_id="first"), receipt)


if __name__ == "__main__":
    unittest.main()
