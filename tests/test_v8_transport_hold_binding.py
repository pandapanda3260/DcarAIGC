from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from v8 import profile_control
from v8.paid_drain import issue_activation_permit_in_transaction
from v8.profile_activations import TIKHUB_PROFILE, append_activation
from v8.provider_budget import PRICES_MICROUSD
from v8.storage import connect, initialize_database, transaction
from v8.transport_hold_binding import (
    CONTRACT_VERSION,
    TransportHoldBindingError,
    read_current_diagnostic_hold,
)


INITIAL_AT = "2026-09-01T00:00:00Z"
HOLD_AT = "2026-09-06T04:00:00Z"
READ_AT = "2026-09-06T05:30:00Z"
EXPIRES_AT = "2026-09-10T00:00:00Z"
HOLD_ID = "transport-diagnostic-hold"
OWNER = "transport-release-owner"

LEGACY_BUILD = "1" * 64
LEGACY_RUNTIME = "2" * 64
BUILD = "3" * 64
RUNTIME = "4" * 64
CONFIG = "5" * 64
NEW_CONFIG = "6" * 64
PRICE = "7" * 64
BUDGET = "8" * 64
NEXT_BUILD = "9" * 64
NEXT_RUNTIME = "a" * 64
NEXT_CONFIG = "b" * 64
NEXT_PRICE = "c" * 64
NEXT_BUDGET = "d" * 64
QUALIFICATION = "e" * 64
CAPACITY = "f" * 64


class TransportHoldBindingTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-transport-hold-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "transport-hold.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
            roster = self._snapshot(connection)
            connection.commit()
            with transaction(connection):
                active = append_activation(
                    connection,
                    profile_id=TIKHUB_PROFILE,
                    roster_snapshot_id=int(roster["id"]),
                    roster_members_sha256=str(roster["members_sha256"]),
                    effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    actor="fixture",
                    reason="fixture immediate activation",
                    metadata={"effective_mode": "immediate"},
                    created_at=INITIAL_AT,
                )
                self.activation_id = int(active["activation_id"])
                issue_activation_permit_in_transaction(
                    connection,
                    activation_id=self.activation_id,
                    drain_id="initial-activation-permit",
                    source_activation_id=self.activation_id,
                    business_day="2026-09-01",
                    planned_effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    runtime_root_receipt_sha256=LEGACY_RUNTIME,
                    now=INITIAL_AT,
                )
        profile_control.begin_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor=OWNER,
            reason="qualify diagnostic transport",
            not_before_business_day="2026-09-08",
            now=HOLD_AT,
        )
        self._register_required()

    def _snapshot(self, connection: Any) -> dict[str, object]:
        source = b'{"family":"system"}'
        source_hash = hashlib.sha256(source).hexdigest()
        members_hash = hashlib.sha256(b"system-roster").hexdigest()
        source_path = self.root / "system-roster.json"
        source_path.write_bytes(source)
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES ('system','system_managed','system-roster','{}','system-roster',
                       '2026-09-01T00:00:00Z','2026-09-01T00:00:00Z',0,0,?,?,?,
                       'account-roster-v2','{}')""",
            (members_hash, source_hash, str(source_path)),
        )
        return {"id": int(cursor.lastrowid or 0), "members_sha256": members_hash}

    def _evidence(self, kind: str, artifact: str) -> dict[str, Any]:
        value: dict[str, Any] = {
            "valid": True,
            "readback": True,
            "artifact_sha256": artifact,
        }
        if kind == "price":
            value["prices_microusd"] = dict(PRICES_MICROUSD)
        elif kind == "budget":
            value.update(
                caps_microusd={
                    "discovery": 30_000_000,
                    "metrics": 15_000_000,
                    "automatic_repair": 0,
                    "automatic_total": 50_000_000,
                },
                forecast_microusd=44_246_200,
            )
        elif kind == "qualification":
            value["required_operations"] = [
                {"operation": operation, "qualified": True}
                for operation in PRICES_MICROUSD
            ]
        elif kind == "capacity":
            value.update(
                archive_independent=True,
                archive_recovery_tested=True,
                runway_days=90,
            )
        return value

    def _register(
        self,
        kind: str,
        artifact: str,
        *,
        generation: int | None = None,
        at: str = "2026-09-06T05:00:00Z",
        expires_at: str = EXPIRES_AT,
    ) -> None:
        profile_control.record_current_activation_hold_prerequisite(
            db_path=self.db,
            drain_id=HOLD_ID,
            kind=kind,
            artifact_sha256=artifact,
            receipt_contract_version=(
                profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
            ),
            expires_at=expires_at,
            evidence=self._evidence(kind, artifact),
            actor=OWNER,
            generation=generation,
            now=at,
        )

    def _register_required(self) -> None:
        for kind, artifact in {
            "build": BUILD,
            "runtime": RUNTIME,
            "config": CONFIG,
            "price": PRICE,
            "budget": BUDGET,
        }.items():
            self._register(kind, artifact)

    def _read(
        self, *, at: str = READ_AT, expected: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        with connect(self.db) as connection:
            return read_current_diagnostic_hold(
                connection, drain_id=HOLD_ID, at=at, expected=expected
            )

    def _row_counts(self) -> dict[str, int]:
        with connect(self.db) as connection:
            return {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in (
                    "scheduler_runs",
                    "scheduler_run_attempts",
                    "provider_usage",
                    "fetch_attempts",
                    "pipeline_paid_drain_events",
                )
            }

    def test_binding_is_db_derived_stable_read_only_and_exact_cas(self) -> None:
        # Generation 1 has no config in HOLD_BEGIN.  A newer durable config
        # receipt, not a caller hint, is therefore the current config.
        self._register("config", NEW_CONFIG, at="2026-09-06T05:10:00Z")
        before = self._row_counts()

        binding = self._read()

        self.assertEqual(binding["contract_version"], CONTRACT_VERSION)
        self.assertEqual(binding["drain_id"], HOLD_ID)
        self.assertEqual(binding["activation_id"], self.activation_id)
        self.assertEqual(binding["generation"], 1)
        self.assertEqual(binding["build_receipt_sha256"], BUILD)
        self.assertEqual(binding["runtime_root_receipt_sha256"], RUNTIME)
        self.assertEqual(binding["config_receipt_sha256"], NEW_CONFIG)
        self.assertEqual(binding["price_receipt_sha256"], PRICE)
        self.assertEqual(binding["budget_receipt_sha256"], BUDGET)
        self.assertEqual(binding["actor"], OWNER)
        with connect(self.db) as connection:
            release = connection.execute(
                "SELECT id,event_hash,event_type FROM pipeline_paid_drain_events "
                "WHERE id=(SELECT previous_event_id FROM pipeline_paid_drain_events WHERE id=?)",
                (binding["start_event_id"],),
            ).fetchone()
            self.assertEqual(release["event_type"], "release")
            self.assertEqual(binding["dispatch_legacy_release_anchor"], {
                "event_id": release["id"], "event_hash": release["event_hash"],
                "role": "historical_lineage_only",
            })
        self.assertEqual(
            binding["matrix_high_watermarks"],
            {
                "network_starts": 0,
                "dispatch_event_count": 0,
                "dispatch_event_high_watermark": 0,
                "dispatch_send_marked_count": 0,
                "raw_count": 0,
                "raw_high_watermark": 0,
                "content_observation_count": 0,
                "content_observation_high_watermark": 0,
                "account_observation_count": 0,
                "account_observation_high_watermark": 0,
            },
        )
        self.assertEqual(self._read(expected=binding), binding)
        self.assertEqual(self._row_counts(), before)

        stale = json.loads(json.dumps(binding))
        stale["actor"] = "caller-supplied-owner"
        with self.assertRaises(TransportHoldBindingError) as caught:
            self._read(expected=stale)
        self.assertEqual(caught.exception.code, "diagnostic_hold_binding_changed")

    def test_expired_or_sealed_hold_is_rejected(self) -> None:
        with self.assertRaises(TransportHoldBindingError) as expired:
            self._read(at="2026-09-11T00:00:00Z")
        self.assertEqual(expired.exception.code, "diagnostic_hold_prerequisite_invalid")

        self._register("qualification", QUALIFICATION)
        self._register("capacity", CAPACITY)
        profile_control.seal_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            final_build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            config_receipt_sha256=CONFIG,
            qualification_receipt_sha256=QUALIFICATION,
            capacity_receipt_sha256=CAPACITY,
            price_receipt_sha256=PRICE,
            budget_receipt_sha256=BUDGET,
            actor=OWNER,
            now="2026-09-06T06:00:00Z",
        )
        with self.assertRaises(TransportHoldBindingError) as sealed:
            self._read(at="2026-09-06T06:01:00Z")
        self.assertEqual(sealed.exception.code, "diagnostic_hold_not_current")

    def test_build_advance_invalidates_old_binding_and_requires_new_generation(
        self,
    ) -> None:
        old = self._read()
        for kind, artifact in {
            "build": NEXT_BUILD,
            "runtime": NEXT_RUNTIME,
            "config": NEXT_CONFIG,
        }.items():
            self._register(
                kind,
                artifact,
                generation=2,
                at="2026-09-06T05:31:00Z",
            )
        profile_control.advance_current_activation_hold_build(
            db_path=self.db,
            drain_id=HOLD_ID,
            from_build_receipt_sha256=BUILD,
            to_build_receipt_sha256=NEXT_BUILD,
            runtime_root_receipt_sha256=NEXT_RUNTIME,
            config_receipt_sha256=NEXT_CONFIG,
            actor=OWNER,
            reason="diagnostic implementation changed",
            now="2026-09-06T05:32:00Z",
        )
        self._register("price", NEXT_PRICE, generation=2, at="2026-09-06T05:33:00Z")
        self._register("budget", NEXT_BUDGET, generation=2, at="2026-09-06T05:33:00Z")

        current = self._read(at="2026-09-06T05:34:00Z")
        self.assertEqual(current["generation"], 2)
        self.assertEqual(current["build_receipt_sha256"], NEXT_BUILD)
        self.assertEqual(current["runtime_root_receipt_sha256"], NEXT_RUNTIME)
        self.assertEqual(current["config_receipt_sha256"], NEXT_CONFIG)
        self.assertEqual(current["price_receipt_sha256"], NEXT_PRICE)
        self.assertEqual(current["budget_receipt_sha256"], NEXT_BUDGET)
        with self.assertRaises(TransportHoldBindingError) as stale:
            self._read(at="2026-09-06T05:34:00Z", expected=old)
        self.assertEqual(stale.exception.code, "diagnostic_hold_binding_changed")

    def test_malformed_control_receipt_envelope_is_rejected(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO scheduler_runs(job_id,scheduled_for,status,started_at,"
                "completed_at,details_json) VALUES "
                "(?,'invalid-envelope','succeeded',?,?, '{}')",
                (
                    profile_control.CURRENT_HOLD_CONTROL_JOB,
                    "2026-09-06T05:20:00Z",
                    "2026-09-06T05:20:00Z",
                ),
            )

        with self.assertRaises(TransportHoldBindingError) as caught:
            self._read()
        self.assertEqual(caught.exception.code, "diagnostic_hold_generation_invalid")

    def test_matrix_delta_is_rejected_before_permit_or_seal(self) -> None:
        baseline = self._read()
        self.assertEqual(baseline["matrix_high_watermarks"]["raw_count"], 0)
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "INSERT INTO provider_raw_responses(provider,operation,local_path,"
                "sha256,byte_size,http_status,captured_at,source) VALUES "
                "('newrank_matrix','matrix_works_list',?,?,2,200,?,"
                "'matrix_page_pending')",
                (
                    str(self.root / "matrix-after-start.json"),
                    hashlib.sha256(b"{}").hexdigest(),
                    "2026-09-06T05:20:00Z",
                ),
            )

        with self.assertRaises(TransportHoldBindingError) as caught:
            self._read()
        self.assertEqual(caught.exception.code, "diagnostic_hold_matrix_delta")


if __name__ == "__main__":
    unittest.main()
