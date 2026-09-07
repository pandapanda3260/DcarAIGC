from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from v8.account_roster import (
    MATRIX_SOURCE_FAMILY,
    PLATFORMS,
    RosterError,
    SYSTEM_SOURCE_FAMILY,
    accept_candidate,
    account_metadata,
    account_summary,
    candidate_diff,
    current_snapshot,
    get_current_members,
    latest_family_snapshot,
    prepare_candidate,
    require_active_member,
    runtime_snapshot,
    snapshot_by_id,
    validate_payload,
    validate_bootstrap_extension,
)
from v8.profile_activations import append_activation, cancel_activation
from v8.storage import connect, initialize_database


class AccountRosterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.connection = connect(self.root / "roster.sqlite3")
        initialize_database(self.connection)
        self.base = datetime(2026, 8, 28, tzinfo=timezone.utc)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def member(self, number: int, *, uid: bool = True) -> dict:
        return {
            "platform": "douyin",
            "matrix_account_id": f"matrix-{number}",
            "profile_ref": f"https://www.douyin.com/user/MS4w-{number}",
            "uid": str(10000000 + number) if uid else None,
            "nickname": f"member {number}",
            "monitoring_status": "not_monitored",
            "authorization_status": "unknown",
        }

    def payload(self, numbers: list[int], minute: float = 0, **options) -> tuple[dict, bytes]:
        members = [self.member(n) for n in numbers]
        source = json.dumps({"members": members, "minute": minute}).encode()
        captured = (self.base + timedelta(minutes=minute)).isoformat()
        payload = {
            "source_type": "bootstrap_export" if minute == 0 else "manual_export",
            "source_captured_at": captured,
            "scope": {
                "organization": "test organization", "coverage": "full",
                "account_scope": "all_added_accounts", "platforms": sorted(PLATFORMS),
            },
            "source_evidence": {
                "kind": "official_export", "evidence_kind": "operator_declaration",
                "export_record_id": f"export-{minute}", "exported_at": captured,
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "source_name": "official-export.json",
                "scope_evidence": "Operator confirms the complete all-account export.",
            },
            "declared_count": len(numbers),
            "pagination": {"pages": [1], "expected_pages": 1, "terminal": True,
                           "declared_totals": [len(numbers)]},
            "members": members,
            **options,
        }
        return payload, source

    def source_for(self, payload: dict) -> bytes:
        source = json.dumps({"members": payload["members"], "exported_at": payload["source_captured_at"]}).encode()
        payload["source_evidence"]["source_sha256"] = hashlib.sha256(source).hexdigest()
        return source

    def stage(self, payload: dict, source: bytes) -> dict:
        return prepare_candidate(
            self.connection, payload, source_bytes=source,
            raw_root=self.root / "raw",
            observed_at=(self.base + timedelta(days=1)).isoformat(),
        )

    def system_payload(self, numbers: list[int]) -> tuple[dict, bytes]:
        captured = (self.base + timedelta(days=1)).isoformat()
        members = [
            {
                "platform": "douyin",
                "uid": str(10000000 + number),
                "profile_ref": None,
                "sec_user_id": f"MS4w-{number}",
                "nickname": f"managed {number}",
            }
            for number in numbers
        ]
        source = json.dumps({"members": members}).encode()
        return {
            "source_type": "system_managed",
            "source_captured_at": captured,
            "scope": {
                "organization": "test organization",
                "coverage": "full",
                "account_scope": "all_managed_accounts",
                "platforms": sorted(PLATFORMS),
            },
            "source_evidence": {
                "kind": "system_roster_seal",
                "evidence_kind": "system_roster_manifest",
                "source_format": "system-roster-json-v1",
                "seal_id": "system-seal-1",
                "sealed_at": captured,
                "source_sha256": hashlib.sha256(source).hexdigest(),
                "source_name": "system-roster.json",
                "scope_evidence": "Complete system-managed account list.",
            },
            "declared_count": len(members),
            "pagination": {
                "pages": [1],
                "expected_pages": 1,
                "terminal": True,
                "declared_totals": [len(members)],
            },
            "members": members,
        }, source

    def accept(self, candidate_id: int) -> dict:
        return accept_candidate(
            self.connection, candidate_id,
            accepted_at=(self.base + timedelta(days=1)).isoformat(),
        )

    def bootstrap(self, numbers: list[int] | None = None) -> dict:
        payload, source = self.payload(numbers or [1, 2])
        return self.accept(self.stage(payload, source)["candidate_id"])

    def assert_roster_error(self, code: str, function, *args, **kwargs) -> None:
        with self.assertRaises(RosterError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_no_accepted_snapshot_never_falls_back_to_enabled_accounts(self) -> None:
        self.assertFalse(account_summary(self.connection)["ready"])
        self.assert_roster_error(
            "roster_activation_required", require_active_member, self.connection, 1
        )
        payload, source = self.payload([1, 2])
        self.stage(payload, source)
        self.assertIsNone(current_snapshot(self.connection))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)

    def test_rejects_empty_partial_duplicate_statistics_and_rounded_uid(self) -> None:
        payload, source = self.payload([1, 2])
        cases = []
        empty = copy.deepcopy(payload)
        empty.update(members=[], declared_count=0)
        cases.append(("incomplete_roster", empty))
        partial = copy.deepcopy(payload)
        partial["pagination"].update(expected_pages=2, declared_totals=[2, 2])
        cases.append(("incomplete_pagination", partial))
        duplicate = copy.deepcopy(payload)
        duplicate["members"][1] = duplicate["members"][0]
        cases.append(("duplicate_member", duplicate))
        statistics = copy.deepcopy(payload)
        statistics["source_type"] = "api_fullroster"
        statistics["source_evidence"].update(validated_contract_id="trust-me", operation="/api/matrix/v1/account/list")
        cases.append(("manual_export_required", statistics))
        rounded = copy.deepcopy(payload)
        rounded["members"][0]["uid"] = 10000001
        cases.append(("invalid_uid", rounded))
        unnamed = copy.deepcopy(payload)
        unnamed["source_evidence"].pop("source_name")
        cases.append(("missing_source_name", unnamed))
        substituted = copy.deepcopy(payload)
        substituted["members"][0]["matrix_account_id"] = "not-in-original-export"
        cases.append(("source_member_mismatch", substituted))
        for code, value in cases:
            with self.subTest(code=code):
                self.assert_roster_error(code, validate_payload, value, source_bytes=source)
        self.assertIsNone(current_snapshot(self.connection))

    def test_matrix_v1_member_digest_remains_compatible_with_schema18(self) -> None:
        payload, source = self.payload([1, 2])
        normalized = validate_payload(payload, source_bytes=source)
        expected = hashlib.sha256(
            json.dumps(
                [("douyin", "matrix-1"), ("douyin", "matrix-2")],
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        self.assertEqual(normalized["contract_version"], "matrix-full-roster-v1")
        self.assertEqual(normalized["members_sha256"], expected)

    def test_new_accounts_are_created_only_on_acceptance(self) -> None:
        self.bootstrap([1])
        payload, source = self.payload([1, 2], 20)
        candidate = self.stage(payload, source)
        self.assertEqual(account_summary(self.connection)["current_count"], 1)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1)
        result = self.accept(candidate["candidate_id"])
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(account_summary(self.connection)["current_count"], 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_removal_and_addition_wait_for_independent_complete_export(self) -> None:
        self.bootstrap([1, 2])
        first_payload, first_source = self.payload([1, 3], 20)
        first = self.stage(first_payload, first_source)
        result = self.accept(first["candidate_id"])
        self.assertEqual(result["status"], "pending_removal_confirmation")
        self.assertEqual(account_summary(self.connection)["pending_removal_count"], 1)
        self.assertEqual({m["matrix_account_id"] for m in get_current_members(self.connection)}, {"matrix-1", "matrix-2"})
        replay = copy.deepcopy(first_payload)
        replay["source_evidence"]["export_record_id"] = "renamed-file"
        replay_result = self.stage(replay, first_source)
        self.assertTrue(replay_result["replayed"])
        self.assertEqual(replay_result["candidate_id"], first["candidate_id"])
        second = self.stage(*self.payload([1, 3], 30))
        result = self.accept(second["candidate_id"])
        self.assertEqual(result["status"], "accepted")
        self.assertEqual({m["matrix_account_id"] for m in get_current_members(self.connection)}, {"matrix-1", "matrix-3"})
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 3)

    def test_nine_minutes_does_not_confirm_and_changed_set_resets(self) -> None:
        self.bootstrap([1, 2, 3])
        first = self.stage(*self.payload([1, 2], 20))
        self.accept(first["candidate_id"])
        early = self.stage(*self.payload([1, 2], 29))
        self.assertEqual(self.accept(early["candidate_id"])["status"], "pending_removal_confirmation")
        changed = self.stage(*self.payload([1, 3], 30))
        self.accept(changed["candidate_id"])
        back = self.stage(*self.payload([1, 2], 40))
        self.assertEqual(self.accept(back["candidate_id"])["status"], "pending_removal_confirmation")
        confirmed = self.stage(*self.payload([1, 2], 50))
        self.assertEqual(self.accept(confirmed["candidate_id"])["status"], "accepted")

    def test_same_export_record_with_changed_time_and_bytes_is_not_independent(self) -> None:
        self.bootstrap([1, 2])
        first = self.stage(*self.payload([1], 20))
        self.accept(first["candidate_id"])
        payload, source = self.payload([1], 40)
        payload["source_evidence"]["export_record_id"] = "export-20"
        second = self.stage(payload, source)
        self.assertEqual(self.accept(second["candidate_id"])["status"], "pending_removal_confirmation")
        payload["source_evidence"]["source_sha256"] = "0" * 64
        self.assert_roster_error("source_hash_mismatch", self.stage, payload, source)

    def test_member_gate_rechecks_enabled_and_latest_membership(self) -> None:
        self.bootstrap([1, 2])
        snapshot = current_snapshot(self.connection)
        members = get_current_members(self.connection)
        identity = members[1]["identity_id"]
        account = members[1]["account_id"]
        frozen = {"snapshot_id": snapshot["id"], "snapshot_hash": snapshot["members_sha256"]}
        require_active_member(self.connection, identity, **frozen)
        self.connection.execute("UPDATE accounts SET enabled=0 WHERE id=?", (account,))
        self.connection.commit()
        self.assert_roster_error("member_scope_changed", require_active_member, self.connection, identity, **frozen)
        self.connection.execute("UPDATE accounts SET enabled=1 WHERE id=?", (account,))
        self.connection.commit()
        first = self.stage(*self.payload([1], 20))
        self.accept(first["candidate_id"])
        require_active_member(self.connection, identity, **frozen)
        second = self.stage(*self.payload([1], 30))
        self.accept(second["candidate_id"])
        # Accepting a newer family snapshot does not change the effective roster.
        require_active_member(self.connection, identity, **frozen)
        latest = latest_family_snapshot(self.connection, MATRIX_SOURCE_FAMILY)
        self.assert_roster_error(
            "member_scope_changed",
            require_active_member,
            self.connection,
            identity,
            snapshot_id=latest["id"],
            snapshot_hash=latest["members_sha256"],
        )
        self.assert_roster_error("roster_evidence_mismatch", require_active_member, self.connection, members[0]["identity_id"],
                                snapshot_id=snapshot["id"], snapshot_hash="0" * 64)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)

    def test_unresolved_member_visible_but_uid_required_dispatch_is_blocked(self) -> None:
        payload, source = self.payload([1])
        payload["members"][0]["uid"] = None
        self.accept(self.stage(payload, source)["candidate_id"])
        member = get_current_members(self.connection)[0]
        snapshot = current_snapshot(self.connection)
        frozen = {
            "snapshot_id": snapshot["id"],
            "snapshot_hash": snapshot["members_sha256"],
        }
        self.assertEqual(account_summary(self.connection)["unresolved_count"], 1)
        self.assertEqual(member["enabled"], 0)
        self.assert_roster_error(
            "member_scope_changed",
            require_active_member,
            self.connection,
            member["identity_id"],
            **frozen,
        )
        self.connection.execute(
            "UPDATE accounts SET enabled=1 WHERE id=?", (member["account_id"],)
        )
        self.connection.commit()
        self.assert_roster_error(
            "identity_unresolved",
            require_active_member,
            self.connection,
            member["identity_id"],
            **frozen,
        )
        require_active_member(
            self.connection, member["identity_id"], require_uid=False, **frozen
        )
        metadata = account_metadata(self.connection, member["account_id"])
        self.assertEqual(metadata["roster_status"], "current")
        self.assertEqual(metadata["monitoring_status"], "not_monitored")
        self.assertEqual(metadata["authorization_status"], "unknown")

    def test_conflicting_stable_identity_rolls_back_the_entire_acceptance(self) -> None:
        self.bootstrap([1, 2])
        before = current_snapshot(self.connection)["id"]
        payload, source = self.payload([1, 2, 3], 20)
        # New row sorts before the conflict, exercising rollback of a created account.
        payload["members"][2]["matrix_account_id"] = "aaa-new-member"
        payload["members"][1]["uid"] = str(10000001)
        payload["members"][0]["uid"] = None
        source = self.source_for(payload)
        candidate = self.stage(payload, source)
        self.assert_roster_error("identity_conflict", self.accept, candidate["candidate_id"])
        self.assertEqual(current_snapshot(self.connection)["id"], before)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0], 1)
        self.assertEqual(candidate_diff(self.connection, candidate["candidate_id"])["status"], "candidate")

    def test_source_or_manifest_tampering_prevents_acceptance(self) -> None:
        candidate = self.stage(*self.payload([1]))
        raw = self.connection.execute("SELECT local_path FROM provider_raw_responses").fetchone()
        Path(raw["local_path"]).write_bytes(b"changed")
        self.assert_roster_error("roster_raw_mismatch", self.accept, candidate["candidate_id"])
        self.assertIsNone(current_snapshot(self.connection))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)

    def test_rejoin_uses_original_identity_and_local_notes_survive(self) -> None:
        self.bootstrap([1, 2])
        old = get_current_members(self.connection)[1]
        self.connection.execute("UPDATE accounts SET operator_name='keep me',enabled=0 WHERE id=?", (old["account_id"],))
        self.connection.commit()
        first = self.stage(*self.payload([1], 20))
        self.accept(first["candidate_id"])
        second = self.stage(*self.payload([1], 30))
        self.accept(second["candidate_id"])
        payload, source = self.payload([1, 2], 40)
        payload["members"][1]["matrix_account_id"] = "new-official-id"
        payload["members"][1]["nickname"] = "renamed"
        source = self.source_for(payload)
        self.accept(self.stage(payload, source)["candidate_id"])
        rejoined = next(row for row in get_current_members(self.connection) if row["matrix_account_id"] == "new-official-id")
        self.assertEqual(rejoined["identity_id"], old["identity_id"])
        self.assertEqual(rejoined["enabled"], 0)
        account = self.connection.execute("SELECT operator_name FROM accounts WHERE id=?", (old["account_id"],)).fetchone()
        self.assertEqual(account["operator_name"], "keep me")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)

    def test_bootstrap_325_matches_existing_rows_and_only_appends_expected_domains(self) -> None:
        stamp = self.base.isoformat()
        for number in range(1, 352):
            self.connection.execute(
                "INSERT INTO accounts(id,phone,phone_normalized,operator_name,enabled,created_at,updated_at) VALUES (?,'',NULL,'unchanged',?,?,?)",
                (number, int(number != 2), stamp, stamp),
            )
            self.connection.execute(
                "INSERT INTO account_platform_identities(id,account_id,platform,uid,nickname,created_at,updated_at) VALUES (?,?,'douyin',?,'unchanged',?,?)",
                (number, number, str(10000000 + number), stamp, stamp),
            )
        self.connection.execute(
            "INSERT INTO account_provider_references VALUES (1,'tikhub','sec_uid','MS4w-1',NULL,?,?)", (stamp, stamp),
        )
        self.connection.commit()
        source_copy = sqlite3.connect(":memory:")
        source_copy.row_factory = sqlite3.Row
        self.connection.backup(source_copy)
        before_accounts = [tuple(row) for row in self.connection.execute("SELECT * FROM accounts ORDER BY id")]
        before_identities = [tuple(row) for row in self.connection.execute("SELECT * FROM account_platform_identities ORDER BY id")]
        before_ref = tuple(self.connection.execute("SELECT * FROM account_provider_references WHERE provider='tikhub'").fetchone())
        payload, source = self.payload(list(range(1, 326)), require_existing_identities=True)
        candidate = self.stage(payload, source)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_roster_members").fetchone()[0], 0)
        self.accept(candidate["candidate_id"])
        self.assertEqual([tuple(row) for row in self.connection.execute("SELECT * FROM accounts ORDER BY id")], before_accounts)
        self.assertEqual([tuple(row) for row in self.connection.execute("SELECT * FROM account_platform_identities ORDER BY id")], before_identities)
        self.assertEqual(tuple(self.connection.execute("SELECT * FROM account_provider_references WHERE provider='tikhub'").fetchone()), before_ref)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_provider_references WHERE provider='newrank_matrix'").fetchone()[0], 650)
        summary = account_summary(self.connection)
        self.assertEqual(summary["current_count"], 325)
        self.assertEqual(summary["enabled_current_count"], 324)
        self.assertEqual(summary["historical_identity_count"], 26)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_metric_observations").fetchone()[0], 0)
        self.assertEqual(self.connection.execute("PRAGMA foreign_key_check").fetchall(), [])
        extension = validate_bootstrap_extension(source_copy, self.connection)
        self.assertEqual(extension["member_count"], 325)
        self.assertEqual(len(extension["added_rows"]["account_provider_references"]), 650)
        self.assertEqual(extension["sqlite_sequence"], {
            "provider_raw_responses": 1, "scheduler_runs": 1, "account_roster_snapshots": 1,
        })
        self.connection.execute("UPDATE sqlite_sequence SET seq=99 WHERE name='scheduler_runs'")
        self.assert_roster_error("invalid_bootstrap_extension", validate_bootstrap_extension, source_copy, self.connection)
        self.connection.rollback()
        self.connection.execute(
            "UPDATE account_provider_references SET updated_at='changed' WHERE provider='newrank_matrix' AND account_identity_id=1",
        )
        self.assert_roster_error("invalid_bootstrap_extension", validate_bootstrap_extension, source_copy, self.connection)
        self.connection.rollback()
        self.connection.execute(
            "INSERT INTO provider_raw_responses(provider,operation,local_path,sha256,byte_size,captured_at) VALUES ('other','other','missing','0',1,?)",
            (stamp,),
        )
        self.assert_roster_error("invalid_bootstrap_extension", validate_bootstrap_extension, source_copy, self.connection)
        self.connection.rollback()
        source_copy.close()

    def test_accepted_diff_remains_frozen_after_current_changes(self) -> None:
        first = self.bootstrap([1, 2])
        first_diff = candidate_diff(self.connection, first["candidate_id"])
        self.assertEqual(len(first_diff["added"]), 2)
        second = self.stage(*self.payload([1, 2, 3], 20))
        self.accept(second["candidate_id"])
        self.assertEqual(candidate_diff(self.connection, first["candidate_id"]), first_diff)
        self.assertEqual(len(account_summary(self.connection)["diff"]["added"]), 1)
        self.assertEqual(account_summary(self.connection)["pending_removal_count"], 0)

    def test_strict_bootstrap_does_not_create_unknown_identity(self) -> None:
        payload, source = self.payload([1], require_existing_identities=True)
        candidate = self.stage(payload, source)
        self.assert_roster_error("identity_unresolved", self.accept, candidate["candidate_id"])
        self.assertIsNone(current_snapshot(self.connection))
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 0)

    def test_complete_zero_roster_requires_two_independent_exports_and_preserves_archives(self) -> None:
        self.bootstrap([1, 2])
        old_snapshot = current_snapshot(self.connection)
        old_member = get_current_members(self.connection)[0]
        first = self.stage(*self.payload([], 20))
        self.assertEqual(self.accept(first["candidate_id"])["status"], "pending_removal_confirmation")
        self.assertEqual(account_summary(self.connection)["current_count"], 2)
        self.assertEqual(account_summary(self.connection)["pending_removal_count"], 2)
        # A real complete header-only CSV is a second zero-member export,
        # independently identified and captured at least ten minutes later.
        second_payload, _ = self.payload([], 30)
        second_source = b"platform,matrix_account_id,profile_ref\n"
        second_payload["source_evidence"]["source_name"] = "official-empty.csv"
        second_payload["source_evidence"]["source_sha256"] = hashlib.sha256(second_source).hexdigest()
        second = self.stage(second_payload, second_source)
        result = self.accept(second["candidate_id"])
        self.assertEqual(result["status"], "accepted")
        summary = account_summary(self.connection)
        self.assertTrue(summary["ready"])
        self.assertEqual(summary["current_count"], 0)
        self.assertEqual(summary["pending_removal_count"], 0)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 2)
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM account_platform_identities").fetchone()[0], 2)
        require_active_member(
            self.connection,
            old_member["identity_id"],
            snapshot_id=old_snapshot["id"],
            snapshot_hash=old_snapshot["members_sha256"],
        )
        empty_snapshot = current_snapshot(self.connection)
        self.assert_roster_error(
            "member_scope_changed",
            require_active_member,
            self.connection,
            old_member["identity_id"],
            snapshot_id=empty_snapshot["id"],
            snapshot_hash=empty_snapshot["members_sha256"],
        )

    def test_initial_and_unproven_zero_rosters_remain_rejected(self) -> None:
        for minute in (0, 20):
            with self.subTest(initial_minute=minute):
                payload, source = self.payload([], minute)
                self.assert_roster_error("incomplete_roster", self.stage, payload, source)
        self.assertIsNone(current_snapshot(self.connection))
        self.bootstrap([1, 2])
        payload, source = self.payload([], 20)
        partial = copy.deepcopy(payload)
        partial["pagination"]["terminal"] = False
        self.assert_roster_error("incomplete_pagination", self.stage, partial, source)
        missing_total = copy.deepcopy(payload)
        missing_total.pop("declared_count")
        self.assert_roster_error("incomplete_roster", self.stage, missing_total, source)
        for bad_source, source_name, code in (
            (b'{"code":5000,"data":null}', "failed.json", "invalid_export"),
            (b"\n", "blank.csv", "empty_export"),
        ):
            with self.subTest(source=source_name):
                invalid = copy.deepcopy(payload)
                invalid["source_evidence"]["source_name"] = source_name
                invalid["source_evidence"]["source_sha256"] = hashlib.sha256(bad_source).hexdigest()
                self.assert_roster_error(code, self.stage, invalid, bad_source)
        _, nonempty_source = self.payload([1, 2], 20)
        payload["source_evidence"]["source_sha256"] = hashlib.sha256(nonempty_source).hexdigest()
        self.assert_roster_error("source_member_mismatch", self.stage, payload, nonempty_source)
        self.assertEqual(account_summary(self.connection)["current_count"], 2)

    def test_matrix_and_system_families_are_independent_and_activation_bound(self) -> None:
        self.bootstrap([1])
        matrix_snapshot = current_snapshot(self.connection)
        system_payload, system_source = self.system_payload([1])
        system_candidate = self.stage(system_payload, system_source)
        system_result = self.accept(system_candidate["candidate_id"])
        self.assertEqual(system_result["status"], "accepted")
        system_snapshot = latest_family_snapshot(
            self.connection, SYSTEM_SOURCE_FAMILY
        )
        self.assertNotEqual(matrix_snapshot["id"], system_snapshot["id"])
        self.assertEqual(current_snapshot(self.connection)["id"], matrix_snapshot["id"])
        self.assertEqual(
            account_summary(
                self.connection, source_family=SYSTEM_SOURCE_FAMILY
            )["current_count"],
            1,
        )
        system_member = get_current_members(
            self.connection, snapshot_id=system_snapshot["id"]
        )[0]
        self.assertEqual(system_member["member_key"], "uid:douyin:10000001")
        self.assertEqual(system_member["uid"], "10000001")
        self.assertIsNone(system_member["matrix_account_id"])
        self.assertIsNone(system_member["profile_ref"])
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1
        )

        activation_created = (self.base + timedelta(days=1)).isoformat()
        matrix_activation = append_activation(
            self.connection,
            profile_id="matrix_hybrid_v1",
            roster_snapshot_id=matrix_snapshot["id"],
            roster_members_sha256=matrix_snapshot["members_sha256"],
            effective_at=(self.base + timedelta(days=2)).isoformat(),
            build_receipt_sha256="a" * 64,
            actor="test",
            created_at=activation_created,
        )
        system_activation = append_activation(
            self.connection,
            profile_id="tikhub_managed_v1",
            roster_snapshot_id=system_snapshot["id"],
            roster_members_sha256=system_snapshot["members_sha256"],
            effective_at=(self.base + timedelta(days=2, minutes=1)).isoformat(),
            build_receipt_sha256="b" * 64,
            actor="test",
            created_at=activation_created,
        )
        matrix_runtime = runtime_snapshot(self.connection, matrix_activation)
        system_runtime = runtime_snapshot(
            self.connection, system_activation["activation_id"]
        )
        self.assertEqual(matrix_runtime["id"], matrix_snapshot["id"])
        self.assertEqual(system_runtime["id"], system_snapshot["id"])
        self.assertEqual(
            require_active_member(
                self.connection,
                system_member["identity_id"],
                activation=system_activation,
            )["roster_snapshot_id"],
            system_snapshot["id"],
        )
        self.assertEqual(snapshot_by_id(self.connection, system_snapshot["id"])["id"], system_snapshot["id"])
        self.assert_roster_error(
            "roster_evidence_mismatch",
            runtime_snapshot,
            self.connection,
            {
                **system_activation,
                "profile_id": "matrix_hybrid_v1",
            },
        )
        self.assert_roster_error(
            "roster_activation_required", runtime_snapshot, self.connection, {}
        )
        cancel_activation(
            self.connection,
            system_activation["activation_id"],
            cancelled_at=(self.base + timedelta(days=1, minutes=1)).isoformat(),
            actor="test",
            reason="test cancellation",
        )
        self.assert_roster_error(
            "roster_activation_cancelled",
            runtime_snapshot,
            self.connection,
            system_activation["activation_id"],
        )

        # A later system seal cannot make an older-but-new-for-Matrix export stale.
        matrix_candidate = self.stage(*self.payload([1, 2], 20))
        self.assertEqual(self.accept(matrix_candidate["candidate_id"])["status"], "accepted")
        self.assertEqual(
            latest_family_snapshot(self.connection, SYSTEM_SOURCE_FAMILY)["id"],
            system_snapshot["id"],
        )

    def test_matrix_profile_resolves_existing_tikhub_sec_user_id(self) -> None:
        stamp = self.base.isoformat()
        account = self.connection.execute(
            """INSERT INTO accounts(phone,created_at,updated_at)
               VALUES ('',?,?)""",
            (stamp, stamp),
        )
        identity = self.connection.execute(
            """INSERT INTO account_platform_identities(
                      account_id,platform,uid,nickname,source,created_at,updated_at)
               VALUES (?,'douyin','10000001','known','tikhub',?,?)""",
            (account.lastrowid, stamp, stamp),
        )
        self.connection.execute(
            """INSERT INTO account_provider_references(
                      account_identity_id,provider,reference_kind,reference_value,
                      created_at,updated_at)
               VALUES (?,'tikhub','sec_user_id','MS4w-1',?,?)""",
            (identity.lastrowid, stamp, stamp),
        )
        self.connection.commit()
        payload, source = self.payload([1])
        payload["members"][0]["uid"] = None
        source = self.source_for(payload)
        self.accept(self.stage(payload, source)["candidate_id"])
        member = get_current_members(self.connection)[0]
        self.assertEqual(member["identity_id"], identity.lastrowid)
        self.assertEqual(
            self.connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0], 1
        )
