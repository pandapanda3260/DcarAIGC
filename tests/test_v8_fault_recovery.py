from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from v8.fault_recovery import (
    FaultRecoveryEvidenceError,
    verify_operation_recovery,
    verify_storage_recovery,
)
from v8.raw_evidence import RawEvidenceError, write_zstd_raw_evidence
from v8.storage import connect, initialize_database, transaction

AT = "2026-08-29T04:00:00Z"
AFTER = "2026-08-29T05:00:00Z"
EXPIRY = "2026-08-29T06:00:00Z"


def receipt(connection, contract, payload):
    cursor = connection.execute(
        """INSERT INTO scheduler_runs(
               job_id,scheduled_for,status,started_at,completed_at,details_json)
           VALUES (?,?,'succeeded',?,?,?)""",
        (contract, f"receipt:{connection.total_changes}", AT, AT,
         json.dumps({"contract_version": contract, "issued_at": AT,
                     "expires_at": EXPIRY, **payload})),
    )
    return int(cursor.lastrowid)


def seed_campaign(connection, root, current, *, transport=True):
    operation = current["operation"]
    scopes, due_by_scope = [], {}
    count = 203 if transport else 3
    for index in range(count):
        scope = hashlib.sha256(f"{current['generation']}:{index}".encode()).hexdigest()
        scopes.append(scope)
        sent_at = (
            datetime(2026, 8, 29, 4, tzinfo=timezone.utc)
            + timedelta(seconds=index * 300 if index < 3 else 601 + index)
        ).isoformat().replace("+00:00", "Z")
        due_by_scope[scope] = sent_at
        entity = json.dumps({"fixture": index}).encode()
        raw = write_zstd_raw_evidence(
            root / f"{scope}.json.zst", entity, provider="tikhub",
            operation=operation, response_identity=scope,
            paid_scope_identity=scope, sequence=0, evidence_root=root,
        )
        raw_id = connection.execute(
            """INSERT INTO provider_raw_responses(
                   provider,operation,local_path,sha256,byte_size,http_status,captured_at,source)
               VALUES ('TikHub',?,?,?,?,200,?,'live_applied')""",
            (operation, str(raw.path), raw.stored_sha256, raw.stored_size, sent_at),
        ).lastrowid
        connection.execute(
            """INSERT INTO provider_usage(
                   provider,operation,request_attempts,billed_requests,currency,
                   amount,recorded_at,details_json)
               VALUES ('TikHub',?,1,1,'USD',.001,?,?)""",
            (operation, sent_at, json.dumps({
                "paid_scope_identity": scope, "paid_sequence": 0,
                "state": "completed", "sent_at": sent_at, "raw_response_id": raw_id,
                "transport": {
                    "clean_eof": True, "length_match": True, "gzip_crc_ok": None,
                    "json_parse_ok": True, "http_status": 200,
                    "transport_route_id": "fixture-route", "route_generation": "fixture-build",
                    "entity_sha256": raw.entity_sha256,
                },
            })),
        )
    campaign_id = receipt(
        connection,
        "transport-requalification-campaign-v1" if transport else "rate-recovery-campaign-v1",
        {"provider": "tikhub", "operation": operation,
         "fault_generation": current["generation"],
         "fault_fingerprint": current["state_fingerprint"],
         "transport_route_id": "fixture-route", "route_generation": "fixture-build",
         "half_open_scopes": scopes[:3], "sample_scopes": scopes[3:],
         "due_by_scope": due_by_scope},
    )
    proof = {
        "contract_version": "transport-operation-requalification-v1" if transport else "rate-operation-recovery-v1",
        "fault_generation": current["generation"],
        "fault_fingerprint": current["state_fingerprint"], "campaign_id": campaign_id,
    }
    if not transport:
        proof["quota_receipt_id"] = receipt(connection, "provider-quota-window-v1", {
            "provider": "tikhub", "operation": operation, "price_valid": True, "reset_at": AT,
        })
    return proof


class FaultRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.raw = self.root / "raw"
        self.raw.mkdir(mode=0o700)
        self.db = self.root / "fixture.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection)
        self.fault = {"generation": "fixture-generation", "state_fingerprint": "a" * 64,
                      "operation": "douyin_video_detail", "fault_class": "transport"}

    def test_transport_gate_computes_fixed_sample_and_rejects_wrong_generation(self):
        with connect(self.db) as connection, transaction(connection):
            proof = seed_campaign(connection, self.raw, self.fault)
            verified = verify_operation_recovery(connection, self.fault, proof, AFTER)
            self.assertEqual(verified["sample_size"], 200)
            self.assertEqual(verified["raw_readback_count"], 200)
            self.assertLessEqual(verified["wilson_upper"], .02)
            with self.assertRaisesRegex(FaultRecoveryEvidenceError, "another fault"):
                verify_operation_recovery(connection, self.fault,
                                          {**proof, "fault_generation": "stale"}, AFTER)
            connection.execute(
                "UPDATE provider_usage SET details_json=json_set(details_json,'$.state','billing_unknown') WHERE id=4"
            )
            with self.assertRaisesRegex(FaultRecoveryEvidenceError, "transport contract"):
                verify_operation_recovery(connection, self.fault, proof, AFTER)

    def test_caller_success_counts_cannot_replace_ledger_evidence(self):
        with connect(self.db) as connection:
            with self.assertRaises(FaultRecoveryEvidenceError):
                verify_operation_recovery(connection, self.fault, {
                    "fault_generation": self.fault["generation"],
                    "fault_fingerprint": self.fault["state_fingerprint"],
                    "sample_size": 200, "wilson_upper": 0, "raw_readback_count": 200,
                }, AFTER)

    def test_rate_gate_checks_reset_and_actual_three_responses(self):
        self.fault["fault_class"] = "rate_limit"
        with connect(self.db) as connection, transaction(connection):
            proof = seed_campaign(connection, self.raw, self.fault, transport=False)
            result = verify_operation_recovery(connection, self.fault, proof, AFTER)
            self.assertEqual(result["consecutive_natural_due"], 3)
            connection.execute(
                "UPDATE provider_usage SET details_json=json_set(details_json,'$.transport.http_status',429) WHERE id=2"
            )
            with self.assertRaises(FaultRecoveryEvidenceError):
                verify_operation_recovery(connection, self.fault, proof, AFTER)

    def test_storage_gate_checks_disk_and_expiry_and_performs_readback(self):
        current = {**self.fault, "state_evidence": {"raw_root": str(self.raw.resolve())}}
        with connect(self.db) as connection, transaction(connection):
            capacity_id = receipt(connection, "storage_capacity_receipt_v1",
                                  {"admitted": True, "raw_root": str(self.raw)})
            proof = {"contract_version": "storage-recovery-v1", "capacity_receipt_id": capacity_id,
                     "fault_generation": current["generation"],
                     "fault_fingerprint": current["state_fingerprint"]}
            result = verify_storage_recovery(connection, current, proof, AFTER)
            self.assertTrue(result["hash_readback_passed"])
            self.assertEqual(list(self.raw.iterdir()), [])
            with self.assertRaises(FaultRecoveryEvidenceError):
                verify_storage_recovery(connection, current, proof, EXPIRY)
            with patch("v8.fault_recovery.read_raw_evidence", side_effect=RawEvidenceError("bad hash")):
                with self.assertRaises(RawEvidenceError):
                    verify_storage_recovery(connection, current, proof, AFTER)
            self.assertEqual(list(self.raw.iterdir()), [])
