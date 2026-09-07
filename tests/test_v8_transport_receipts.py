from __future__ import annotations

import json
import os
import stat
import tempfile
import unittest
from pathlib import Path

from v8.storage import connect, initialize_database, transaction
from v8.transport_receipts import (
    TransportReceiptError,
    append_transport_receipt,
    read_transport_receipt,
)


RECORDED_AT = "2026-09-06T06:00:00Z"


class TransportReceiptTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-transport-receipt-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "transport-receipts.sqlite3"
        self.mirrors = self.root / "mirrors"
        with connect(self.db) as connection:
            initialize_database(connection)

    def _append(
        self,
        *,
        kind: str = "campaign",
        identity_key: str = "campaign:dev-new-stack:1",
        payload: dict[str, object] | None = None,
        at: str = RECORDED_AT,
    ) -> dict[str, object]:
        with connect(self.db) as connection, transaction(connection):
            return append_transport_receipt(
                connection,
                kind=kind,
                identity_key=identity_key,
                payload=payload
                or {"campaign_id": "fixture-campaign", "sample_cap": 20},
                at=at,
                mirror_root=self.mirrors,
            )

    def _read(self, receipt_id: int) -> dict[str, object]:
        with connect(self.db) as connection:
            return read_transport_receipt(connection, receipt_id)

    def _counts(self) -> tuple[int, int]:
        with connect(self.db) as connection:
            run_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs "
                    "WHERE job_id LIKE 'transport_receipt:%'"
                ).fetchone()[0]
            )
            attempt_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_run_attempts a "
                    "JOIN scheduler_runs r ON r.id=a.scheduler_run_id "
                    "WHERE r.job_id LIKE 'transport_receipt:%'"
                ).fetchone()[0]
            )
            return run_count, attempt_count

    def test_private_mirror_and_idempotent_terminal_receipt(self) -> None:
        first = self._append()
        receipt_id = int(first["receipt_id"])
        mirror = first["mirror"]
        self.assertIsInstance(mirror, dict)
        assert isinstance(mirror, dict)
        mirror_path = Path(str(mirror["path"]))

        self.assertEqual(stat.S_IMODE(self.mirrors.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(mirror_path.stat().st_mode), 0o600)
        self.assertEqual(mirror_path.stat().st_uid, os.geteuid())
        self.assertEqual(self._read(receipt_id), first)
        self.assertEqual(self._counts(), (1, 1))

        repeated = self._append(at="2026-09-06T07:00:00Z")
        self.assertEqual(repeated, first)
        self.assertEqual(self._counts(), (1, 1))
        with self.assertRaises(TransportReceiptError) as conflict:
            self._append(payload={"campaign_id": "changed", "sample_cap": 20})
        self.assertEqual(
            conflict.exception.code, "transport_receipt_idempotency_conflict"
        )
        self.assertEqual(self._counts(), (1, 1))

    def test_missing_or_non_private_mirror_fails_closed(self) -> None:
        missing_receipt = self._append(identity_key="campaign:missing-mirror")
        missing_path = Path(str(missing_receipt["mirror"]["path"]))  # type: ignore[index]
        missing_path.unlink()
        with self.assertRaises(TransportReceiptError) as missing:
            self._read(int(missing_receipt["receipt_id"]))
        self.assertEqual(missing.exception.code, "transport_receipt_mirror_missing")
        with self.assertRaises(TransportReceiptError):
            self._append(identity_key="campaign:missing-mirror")
        self.assertFalse(missing_path.exists())

        unsafe_receipt = self._append(identity_key="campaign:unsafe-mirror")
        unsafe_path = Path(str(unsafe_receipt["mirror"]["path"]))  # type: ignore[index]
        unsafe_path.chmod(0o644)
        with self.assertRaises(TransportReceiptError) as unsafe:
            self._read(int(unsafe_receipt["receipt_id"]))
        self.assertEqual(unsafe.exception.code, "transport_receipt_mirror_invalid")

    def test_mirror_and_db_tampering_are_detected(self) -> None:
        mirrored = self._append(identity_key="campaign:mirror-tamper")
        mirror_path = Path(str(mirrored["mirror"]["path"]))  # type: ignore[index]
        mirror_payload = json.loads(mirror_path.read_text())
        mirror_payload["payload"]["sample_cap"] = 99
        mirror_path.write_text(
            json.dumps(mirror_payload, sort_keys=True, separators=(",", ":")) + "\n"
        )
        mirror_path.chmod(0o600)
        with self.assertRaises(TransportReceiptError) as mirror_error:
            self._read(int(mirrored["receipt_id"]))
        self.assertEqual(
            mirror_error.exception.code, "transport_receipt_mirror_invalid"
        )

        durable = self._append(identity_key="campaign:db-tamper")
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE scheduler_runs SET details_json='{}' WHERE id=?",
                (int(durable["receipt_id"]),),
            )
        with self.assertRaises(TransportReceiptError) as db_error:
            self._read(int(durable["receipt_id"]))
        self.assertEqual(db_error.exception.code, "transport_receipt_db_invalid")

    def test_rollback_orphan_only_accepts_byte_identical_retry(self) -> None:
        payload = {"campaign_id": "rollback-fixture", "sample_cap": 20}
        orphan_path: Path | None = None
        try:
            with connect(self.db) as connection, transaction(connection):
                receipt = append_transport_receipt(
                    connection,
                    kind="campaign",
                    identity_key="campaign:rollback-gap",
                    payload=payload,
                    at=RECORDED_AT,
                    mirror_root=self.mirrors,
                )
                orphan_path = Path(str(receipt["mirror"]["path"]))
                raise RuntimeError("simulate caller rollback after mirror fsync")
        except RuntimeError as exc:
            self.assertEqual(str(exc), "simulate caller rollback after mirror fsync")
        assert orphan_path is not None
        self.assertTrue(orphan_path.is_file())
        self.assertEqual(self._counts(), (0, 0))

        with self.assertRaises(TransportReceiptError) as changed:
            self._append(
                identity_key="campaign:rollback-gap",
                payload={"campaign_id": "rollback-fixture", "sample_cap": 21},
            )
        self.assertEqual(changed.exception.code, "transport_receipt_mirror_invalid")
        self.assertEqual(self._counts(), (0, 0))

        recovered = self._append(identity_key="campaign:rollback-gap", payload=payload)
        self.assertEqual(Path(str(recovered["mirror"]["path"])), orphan_path)  # type: ignore[index]
        self.assertEqual(self._counts(), (1, 1))

    def test_append_requires_caller_transaction_and_whitelisted_kind(self) -> None:
        with connect(self.db) as connection:
            with self.assertRaises(TransportReceiptError) as transaction_error:
                append_transport_receipt(
                    connection,
                    kind="campaign",
                    identity_key="campaign:no-transaction",
                    payload={"campaign_id": "fixture"},
                    at=RECORDED_AT,
                    mirror_root=self.mirrors,
                )
        self.assertEqual(
            transaction_error.exception.code,
            "transport_receipt_transaction_required",
        )

        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(TransportReceiptError) as kind_error:
                append_transport_receipt(
                    connection,
                    kind="send_authorization",
                    identity_key="invalid-kind",
                    payload={"qualified": True},
                    at=RECORDED_AT,
                    mirror_root=self.mirrors,
                )
        self.assertEqual(kind_error.exception.code, "transport_receipt_kind_invalid")
        self.assertEqual(self._counts(), (0, 0))


if __name__ == "__main__":
    unittest.main()
