from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from v8.profile_activations import (
    MATRIX_PROFILE,
    TIKHUB_PROFILE,
    ProfileActivationError,
    activation_digest,
    activation_at,
    activation_by_id,
    append_activation,
    cancel_activation,
    replace_scheduled_activation,
)
from v8.storage import connect, initialize_database


class ProfileActivationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connection = connect(self.root / "activation.sqlite3")
        initialize_database(self.connection)
        self.matrix = self._snapshot("matrix", "matrix:1")
        self.system = self._snapshot("system", "uid:1")
        self.build = "b" * 64

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def _snapshot(self, family: str, suffix: str) -> dict:
        source = json.dumps({"family": family, "suffix": suffix}).encode()
        digest = hashlib.sha256(source).hexdigest()
        members = hashlib.sha256(suffix.encode()).hexdigest()
        path = self.root / f"{family}-{suffix.replace(':', '-')}.json"
        path.write_bytes(source)
        source_type = "system_managed" if family == "system" else "manual_export"
        cursor = self.connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES (?, ?, ?, '{}', ?, '2026-09-01T00:00:00Z',
                       '2026-09-01T00:00:00Z',0,0,?,?,?,'account-roster-v2','{}')""",
            (family, source_type, suffix, suffix, members, digest, str(path)),
        )
        self.connection.commit()
        return {"id": int(cursor.lastrowid or 0), "members_sha256": members}

    def _append(
        self,
        snapshot: dict,
        profile: str,
        *,
        effective: str = "2026-09-02T16:00:00Z",
        created: str = "2026-09-01T12:00:00Z",
    ) -> dict:
        return append_activation(
            self.connection,
            profile_id=profile,
            roster_snapshot_id=snapshot["id"],
            roster_members_sha256=snapshot["members_sha256"],
            effective_at=effective,
            build_receipt_sha256=self.build,
            actor="test",
            reason="test activation",
            created_at=created,
        )

    def assert_error(self, code: str, function, *args, **kwargs) -> None:
        with self.assertRaises(ProfileActivationError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_activation_is_not_effective_before_scheduled_time(self) -> None:
        value = self._append(self.matrix, MATRIX_PROFILE)
        self.assertIsNone(activation_at(self.connection, "2026-09-02T15:59:59Z"))
        active = activation_at(self.connection, "2026-09-02T16:00:00Z")
        self.assertIsNotNone(active)
        assert active is not None
        self.assertEqual(active["activation_id"], value["activation_id"])
        self.assertEqual(active["profile_id"], MATRIX_PROFILE)

    def test_profile_must_match_roster_family_and_hash(self) -> None:
        self.assert_error(
            "activation_roster_mismatch", self._append, self.system, MATRIX_PROFILE
        )
        self.assert_error(
            "activation_roster_mismatch",
            append_activation,
            self.connection,
            profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=self.system["id"],
            roster_members_sha256="a" * 64,
            effective_at="2026-09-02T16:00:00Z",
            build_receipt_sha256=self.build,
            actor="test",
            created_at="2026-09-01T12:00:00Z",
        )

    def test_acceptance_and_activation_are_separate(self) -> None:
        self.assertIsNone(activation_at(self.connection, "2026-09-03T00:00:00Z"))
        self._append(self.system, TIKHUB_PROFILE)
        self.assertIsNone(activation_at(self.connection, "2026-09-02T15:59:59Z"))

    def test_future_activation_can_be_cancelled_but_effective_one_cannot(self) -> None:
        value = self._append(self.matrix, MATRIX_PROFILE)
        cancellation = cancel_activation(
            self.connection,
            value["activation_id"],
            cancelled_at="2026-09-02T15:00:00Z",
            actor="operator",
            reason="gate failed",
        )
        self.assertEqual(cancellation["activation_id"], value["activation_id"])
        self.assertIsNone(activation_at(self.connection, "2026-09-03T00:00:00Z"))
        self.assert_error(
            "activation_already_cancelled",
            cancel_activation,
            self.connection,
            value["activation_id"],
            cancelled_at="2026-09-02T15:30:00Z",
            actor="operator",
            reason="again",
        )

        later = self._append(
            self.matrix,
            MATRIX_PROFILE,
            effective="2026-09-04T16:00:00Z",
            created="2026-09-03T12:00:00Z",
        )
        self.assert_error(
            "activation_already_effective",
            cancel_activation,
            self.connection,
            later["activation_id"],
            cancelled_at="2026-09-04T16:00:00Z",
            actor="operator",
            reason="late",
        )

    def test_replacement_is_atomic_and_retains_slot(self) -> None:
        old = self._append(self.matrix, MATRIX_PROFILE)
        new = replace_scheduled_activation(
            self.connection,
            old["activation_id"],
            profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=self.system["id"],
            roster_members_sha256=self.system["members_sha256"],
            effective_at=old["effective_at"],
            build_receipt_sha256=self.build,
            actor="operator",
            reason="switch",
            created_at="2026-09-02T12:00:00Z",
        )
        self.assertIsNotNone(activation_by_id(self.connection, old["activation_id"])["cancellation"])
        active = activation_at(self.connection, old["effective_at"])
        self.assertIsNotNone(active)
        assert active is not None
        self.assertEqual(active["activation_id"], new["activation_id"])
        self.assertEqual(active["previous_activation_id"], old["activation_id"])

    def test_chain_and_cancellation_tamper_fail_closed(self) -> None:
        value = self._append(self.matrix, MATRIX_PROFILE)
        self.connection.execute("DROP TRIGGER trg_profile_activations_no_update")
        self.connection.execute(
            "UPDATE acquisition_profile_activations SET reason='tampered' WHERE id=?",
            (value["activation_id"],),
        )
        self.connection.commit()
        self.assert_error(
            "activation_chain_invalid",
            activation_at,
            self.connection,
            "2026-09-03T00:00:00Z",
        )

    def test_direct_update_delete_and_duplicate_slot_are_rejected(self) -> None:
        value = self._append(self.matrix, MATRIX_PROFILE)
        with self.assertRaises(Exception):
            self.connection.execute(
                "UPDATE acquisition_profile_activations SET actor='other' WHERE id=?",
                (value["activation_id"],),
            )
        self.connection.rollback()
        with self.assertRaises(Exception):
            self.connection.execute(
                "DELETE FROM acquisition_profile_activations WHERE id=?",
                (value["activation_id"],),
            )
        self.connection.rollback()
        self.assert_error(
            "activation_schedule_conflict",
            self._append,
            self.matrix,
            MATRIX_PROFILE,
        )

    def test_noncanonical_timestamp_cannot_bypass_effective_slot_uniqueness(self) -> None:
        previous = self._append(self.matrix, MATRIX_PROFILE)
        value = {
            "profile_id": MATRIX_PROFILE,
            "roster_snapshot_id": self.matrix["id"],
            "roster_members_sha256": self.matrix["members_sha256"],
            "effective_at": "2026-09-02T16:00:00+00:00",
            "contract_version": "acquisition-profile-activation-v1",
            "build_receipt_sha256": self.build,
            "previous_activation_id": previous["activation_id"],
            "previous_activation_sha256": previous["activation_sha256"],
            "actor": "raw-test",
            "reason": "equivalent timestamp",
            "metadata": {},
            "created_at": "2026-09-01T12:00:00.000000Z",
        }
        value["activation_sha256"] = activation_digest(value)
        with self.assertRaises(sqlite3.IntegrityError):
            self.connection.execute(
                """INSERT INTO acquisition_profile_activations(
                       profile_id,roster_snapshot_id,roster_members_sha256,
                       effective_at,contract_version,build_receipt_sha256,
                       previous_activation_id,previous_activation_sha256,
                       activation_sha256,actor,reason,metadata_json,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    value["profile_id"],
                    value["roster_snapshot_id"],
                    value["roster_members_sha256"],
                    value["effective_at"],
                    value["contract_version"],
                    value["build_receipt_sha256"],
                    value["previous_activation_id"],
                    value["previous_activation_sha256"],
                    value["activation_sha256"],
                    value["actor"],
                    value["reason"],
                    "{}",
                    value["created_at"],
                ),
            )
        self.connection.rollback()


if __name__ == "__main__":
    unittest.main()
