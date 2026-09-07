from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from v8 import profile_control, transport_cohort
from v8.paid_drain import issue_activation_permit_in_transaction
from v8.profile_activations import TIKHUB_PROFILE, append_activation
from v8.provider_budget import PRICES_MICROUSD
from v8.raw_evidence import write_zstd_raw_evidence
from v8.storage import connect, initialize_database, transaction
from v8.transport_cohort import TransportCohortError, freeze_large_page_cohort


INITIAL_AT = "2026-09-01T00:00:00Z"
HOLD_AT = "2026-09-06T04:00:00Z"
FREEZE_AT = "2026-09-06T05:30:00Z"
HOLD_ID = "transport-cohort-hold"
OWNER = "transport-cohort-owner"

LEGACY_BUILD = "1" * 64
LEGACY_RUNTIME = "2" * 64
BUILD = "3" * 64
RUNTIME = "4" * 64
CONFIG = "5" * 64
NEW_CONFIG = "6" * 64
PRICE = "7" * 64
BUDGET = "8" * 64


class TransportCohortTest(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="dcar-transport-cohort-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / "cohort.sqlite3"
        self.raw_root = self.root / "raw"
        self.mirror_root = self.root / "mirrors"
        with connect(self.db) as connection:
            initialize_database(connection)
            roster = self._roster(connection, count=6)
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
                    reason="fixture activation",
                    metadata={"effective_mode": "immediate"},
                    created_at=INITIAL_AT,
                )
                activation_id = int(active["activation_id"])
                issue_activation_permit_in_transaction(
                    connection,
                    activation_id=activation_id,
                    drain_id="initial-activation-permit",
                    source_activation_id=activation_id,
                    business_day="2026-09-01",
                    planned_effective_at=INITIAL_AT,
                    build_receipt_sha256=LEGACY_BUILD,
                    runtime_root_receipt_sha256=LEGACY_RUNTIME,
                    now=INITIAL_AT,
                )
        self._before_hold()
        profile_control.begin_current_activation_hold(
            db_path=self.db,
            drain_id=HOLD_ID,
            build_receipt_sha256=BUILD,
            runtime_root_receipt_sha256=RUNTIME,
            actor=OWNER,
            reason="freeze large-page diagnostic cohort",
            not_before_business_day="2026-09-08",
            now=HOLD_AT,
        )
        for kind, artifact in {
            "build": BUILD,
            "runtime": RUNTIME,
            "config": CONFIG,
            "price": PRICE,
            "budget": BUDGET,
        }.items():
            self._register_prerequisite(kind, artifact)

    def _before_hold(self) -> None:
        """Sub-fixtures seed historical rows before the actual START watermark."""

    def _roster(self, connection: Any, *, count: int) -> dict[str, int | str]:
        for account_id in range(1, count + 1):
            uid = f"douyin-{account_id:02d}"
            connection.execute(
                "INSERT INTO accounts(id,phone,operator_name,account_type,"
                "content_direction,enabled,created_at,updated_at) "
                "VALUES (?,?,?,'unknown','unknown',1,?,?)",
                (
                    account_id,
                    str(account_id),
                    f"account-{account_id}",
                    INITIAL_AT,
                    INITIAL_AT,
                ),
            )
            connection.execute(
                "INSERT INTO account_platform_identities(id,account_id,platform,uid,"
                "nickname,source,created_at,updated_at) "
                "VALUES (?,?,'douyin',?,?, 'manual',?,?)",
                (account_id, account_id, uid, uid, INITIAL_AT, INITIAL_AT),
            )
        source = json.dumps({"family": "system", "count": count}).encode()
        source_path = self.root / "system-roster.json"
        source_path.write_bytes(source)
        source_path.chmod(0o600)
        members_hash = hashlib.sha256(b"system-roster-members").hexdigest()
        cursor = connection.execute(
            """INSERT INTO account_roster_snapshots(
                   source_family,source_type,scope_key,scope_json,source_instance_id,
                   source_captured_at,accepted_at,declared_count,member_count,
                   members_sha256,source_sha256,source_path,contract_version,metadata_json)
               VALUES ('system','system_managed','system-roster','{}','system-roster',
                       ?,?,?,?, ?,?,?, 'system-managed-roster-v1','{}')""",
            (
                INITIAL_AT,
                INITIAL_AT,
                count,
                count,
                members_hash,
                hashlib.sha256(source).hexdigest(),
                str(source_path),
            ),
        )
        snapshot_id = int(cursor.lastrowid or 0)
        for identity_id in range(1, count + 1):
            uid = f"douyin-{identity_id:02d}"
            connection.execute(
                "INSERT INTO account_roster_members(snapshot_id,account_identity_id,"
                "platform,member_key,uid,matrix_account_id,profile_ref,"
                "monitoring_status,authorization_status,metadata_json) "
                "VALUES (?,?,'douyin',?,?,NULL,NULL,'unknown','unknown','{}')",
                (snapshot_id, identity_id, f"uid:douyin:{uid}", uid),
            )
        return {"id": snapshot_id, "members_sha256": members_hash}

    def _evidence(self, kind: str, artifact: str) -> dict[str, Any]:
        evidence: dict[str, Any] = {
            "valid": True,
            "readback": True,
            "artifact_sha256": artifact,
        }
        if kind == "price":
            evidence["prices_microusd"] = dict(PRICES_MICROUSD)
        elif kind == "budget":
            evidence.update(
                caps_microusd={
                    "discovery": 30_000_000,
                    "metrics": 15_000_000,
                    "automatic_repair": 0,
                    "automatic_total": 50_000_000,
                },
                forecast_microusd=44_246_200,
            )
        return evidence

    def _register_prerequisite(
        self, kind: str, artifact: str, *, at: str = "2026-09-06T05:00:00Z"
    ) -> None:
        profile_control.record_current_activation_hold_prerequisite(
            db_path=self.db,
            drain_id=HOLD_ID,
            kind=kind,
            artifact_sha256=artifact,
            receipt_contract_version=(
                profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS[kind]
            ),
            expires_at="2026-09-10T00:00:00Z",
            evidence=self._evidence(kind, artifact),
            actor=OWNER,
            now=at,
        )

    def _raw(
        self,
        *,
        account_id: int,
        padding: int,
        captured_at: str,
    ) -> dict[str, Any]:
        value = {
            "data": {
                "aweme_list": [],
                "has_more": False,
                "max_cursor": 0,
                "padding": "x" * padding,
            }
        }
        entity = json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        raw_path = self.raw_root / f"account-{account_id}-{padding}.json.zst"
        identity = hashlib.sha256(
            f"{account_id}:{padding}:{captured_at}".encode()
        ).hexdigest()
        receipt = write_zstd_raw_evidence(
            raw_path,
            entity,
            provider="TikHub",
            operation="douyin_user_posts",
            response_identity=identity,
            paid_scope_identity=identity,
            sequence=0,
            evidence_root=self.raw_root,
        )
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                "INSERT INTO provider_raw_responses(account_id,provider,operation,"
                "local_path,sha256,byte_size,http_status,captured_at,source) "
                "VALUES (?,'TikHub','douyin_user_posts',?,?,?,200,?,'live')",
                (
                    account_id,
                    str(raw_path.relative_to(self.root)),
                    receipt.stored_sha256,
                    receipt.stored_size,
                    captured_at,
                ),
            )
        return {
            "raw_id": int(cursor.lastrowid or 0),
            "path": raw_path,
            "entity_size": receipt.entity_size,
            "entity_sha256": receipt.entity_sha256,
            "stored_size": receipt.stored_size,
        }

    def _freeze(self, *, at: str = FREEZE_AT) -> dict[str, Any]:
        with (
            patch.object(transport_cohort, "PROJECT_ROOT", self.root),
            connect(self.db) as connection,
            transaction(connection),
        ):
            return freeze_large_page_cohort(
                connection,
                drain_id=HOLD_ID,
                at=at,
                mirror_root=self.mirror_root,
            )

    def _seed_rank_fixture(self) -> dict[int, dict[str, Any]]:
        older = self._raw(
            account_id=1,
            padding=10_000,
            captured_at="2026-09-05T01:00:00Z",
        )
        latest = {
            1: self._raw(
                account_id=1,
                padding=10,
                captured_at="2026-09-05T02:00:00Z",
            ),
            2: self._raw(
                account_id=2,
                padding=100,
                captured_at="2026-09-05T02:00:00Z",
            ),
            3: self._raw(
                account_id=3,
                padding=200,
                captured_at="2026-09-05T02:00:00Z",
            ),
            4: self._raw(
                account_id=4,
                padding=200,
                captured_at="2026-09-05T02:00:00Z",
            ),
            5: self._raw(
                account_id=5,
                padding=5_000,
                captured_at="2026-09-05T02:00:00Z",
            ),
        }
        self.assertNotEqual(older["raw_id"], latest[1]["raw_id"])
        return latest

    def test_latest_per_account_p75_ties_entity_size_and_missing_exclusion(
        self,
    ) -> None:
        latest = self._seed_rank_fixture()

        receipt = self._freeze()

        payload = receipt["payload"]
        columns = payload["record_columns"]
        records = [dict(zip(columns, row, strict=True)) for row in payload["records"]]
        by_identity = {int(row["identity_id"]): row for row in records}
        expected_sizes = sorted(item["entity_size"] for item in latest.values())
        self.assertEqual(payload["complete_page_account_count"], 5)
        self.assertEqual(payload["roster_douyin_account_count"], 6)
        self.assertEqual(payload["p75_rank"], 4)
        self.assertEqual(payload["p75_entity_bytes"], expected_sizes[3])
        self.assertEqual(payload["selected_identity_ids"], [3, 4, 5])
        self.assertEqual(payload["missing_complete_raw_identity_ids"], [6])
        self.assertEqual(by_identity[1]["raw_id"], latest[1]["raw_id"])
        self.assertEqual(by_identity[5]["entity_bytes"], latest[5]["entity_size"])
        self.assertNotEqual(latest[5]["entity_size"], latest[5]["stored_size"])
        self.assertEqual(by_identity[5]["entity_sha256"], latest[5]["entity_sha256"])

    def test_invalid_latest_raw_fails_closed_instead_of_using_older_page(self) -> None:
        self._raw(
            account_id=1,
            padding=100,
            captured_at="2026-09-05T01:00:00Z",
        )
        newest = self._raw(
            account_id=1,
            padding=200,
            captured_at="2026-09-05T02:00:00Z",
        )
        newest["path"].write_bytes(b"corrupted-zstd-evidence")
        newest["path"].chmod(0o600)

        with self.assertRaises(TransportCohortError) as caught:
            self._freeze()
        self.assertEqual(caught.exception.code, "transport_cohort_raw_invalid")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs "
                    "WHERE job_id='transport_receipt:cohort'"
                ).fetchone()[0],
                0,
            )

    def test_retry_never_reselects_later_raw(self) -> None:
        self._seed_rank_fixture()
        frozen = self._freeze()
        payload = frozen["payload"]
        first_hwm = int(payload["raw_high_watermark"])
        first_records = payload["records"]
        later = self._raw(
            account_id=1,
            padding=20_000,
            captured_at="2026-09-06T05:45:00Z",
        )
        self.assertGreater(later["raw_id"], first_hwm)

        repeated = self._freeze(at="2026-09-06T06:00:00Z")

        self.assertEqual(repeated["receipt_id"], frozen["receipt_id"])
        self.assertEqual(repeated["payload"]["raw_high_watermark"], first_hwm)
        self.assertEqual(repeated["payload"]["records"], first_records)

    def test_existing_cohort_rejects_changed_hold_binding(self) -> None:
        self._seed_rank_fixture()
        frozen = self._freeze()
        self._register_prerequisite("config", NEW_CONFIG, at="2026-09-06T05:45:00Z")

        with self.assertRaises(TransportCohortError) as caught:
            self._freeze(at="2026-09-06T06:00:00Z")

        self.assertEqual(caught.exception.code, "transport_cohort_hold_changed")
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs "
                    "WHERE job_id='transport_receipt:cohort'"
                ).fetchone()[0],
                1,
            )
        self.assertEqual(
            frozen["payload"]["hold_binding"]["config_receipt_sha256"], CONFIG
        )


if __name__ == "__main__":
    unittest.main()
