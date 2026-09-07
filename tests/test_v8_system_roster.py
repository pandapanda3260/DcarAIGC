from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from v8.account_roster import (
    PLATFORMS,
    RosterError,
    SYSTEM_SOURCE_FAMILY,
    accept_candidate,
    get_current_members,
    latest_family_snapshot,
    prepare_candidate,
    runtime_account_summary,
)
from v8.operations import account_read_model, upsert_account
from v8.profile_activations import MATRIX_PROFILE, TIKHUB_PROFILE, append_activation
from v8.storage import connect, initialize_database
from v8.system_roster import (
    bootstrap_system_roster_from_matrix,
    current_system_members,
    remove_system_member,
    seal_system_members,
    upsert_system_members,
)


class SystemRosterTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "system-roster.sqlite3"
        self.connection = connect(self.db)
        initialize_database(self.connection)
        self.raw = self.root / "raw"

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def _account(self, uid: str = "123456789") -> int:
        value = upsert_account(
            {
                "platforms": [
                    {"platform": "douyin", "uid": uid, "nickname": "矩阵昵称"}
                ]
            },
            db_path=self.db,
        )
        return int(value["id"])

    def _bootstrap(self) -> tuple[int, dict]:
        account_id = self._account()
        captured = "2026-09-02T11:00:00Z"
        members = [
            {
                "platform": "douyin",
                "matrix_account_id": "matrix-1",
                "profile_ref": "https://www.douyin.com/user/MS4w.valid",
                "uid": "123456789",
                "nickname": "矩阵昵称",
            }
        ]
        source = json.dumps({"members": members}).encode()
        candidate = prepare_candidate(
            self.connection,
            {
                "source_type": "bootstrap_export",
                "require_existing_identities": True,
                "source_captured_at": captured,
                "scope": {
                    "organization": "test",
                    "coverage": "full",
                    "account_scope": "all_added_accounts",
                    "platforms": sorted(PLATFORMS),
                },
                "source_evidence": {
                    "kind": "official_export",
                    "evidence_kind": "operator_declaration",
                    "export_record_id": "matrix-bootstrap",
                    "exported_at": captured,
                    "source_sha256": hashlib.sha256(source).hexdigest(),
                    "source_name": "matrix.json",
                    "scope_evidence": "complete test Matrix roster",
                },
                "declared_count": 1,
                "pagination": {
                    "pages": [1],
                    "expected_pages": 1,
                    "terminal": True,
                    "declared_totals": [1],
                },
                "members": members,
            },
            source_bytes=source,
            raw_root=self.raw,
            observed_at=captured,
        )
        matrix = accept_candidate(
            self.connection, int(candidate["candidate_id"]), accepted_at=captured
        )
        snapshot = latest_family_snapshot(self.connection, "matrix")
        assert snapshot is not None
        append_activation(
            self.connection,
            profile_id=MATRIX_PROFILE,
            roster_snapshot_id=int(matrix["snapshot_id"]),
            roster_members_sha256=str(snapshot["members_sha256"]),
            effective_at=captured,
            build_receipt_sha256="b" * 64,
            actor="test",
            reason="activate Matrix fixture",
            created_at=captured,
        )
        accepted = bootstrap_system_roster_from_matrix(
            self.connection,
            raw_root=self.raw,
            actor="operator",
            reason="prepare TikHub managed mode",
            sealed_at="2026-09-02T12:00:00Z",
        )
        return account_id, accepted

    def assert_roster_error(self, code: str, function, *args, **kwargs) -> None:
        with self.assertRaises(RosterError) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)

    def test_bootstrap_copies_uid_members_without_activating_them(self) -> None:
        account_id, accepted = self._bootstrap()
        self.assertEqual(accepted["status"], "accepted")
        self.assertEqual(accepted["activation_status"], "pending_activation")
        snapshot = latest_family_snapshot(self.connection, SYSTEM_SOURCE_FAMILY)
        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        members = get_current_members(
            self.connection,
            snapshot_id=int(snapshot["id"]),
            source_family=SYSTEM_SOURCE_FAMILY,
        )
        self.assertEqual(len(members), 1)
        self.assertEqual(members[0]["account_id"], account_id)
        self.assertEqual(members[0]["uid"], "123456789")
        self.assertEqual(
            members[0]["profile_ref"],
            "https://www.douyin.com/user/MS4w.valid",
        )
        activations = self.connection.execute(
            "SELECT profile_id FROM acquisition_profile_activations ORDER BY id"
        ).fetchall()
        self.assertEqual([row[0] for row in activations], ["matrix_hybrid_v1"])

    def test_upsert_is_one_full_snapshot_and_rejects_bad_rows_individually(self) -> None:
        self._bootstrap()
        before = self.connection.execute(
            "SELECT COUNT(*) FROM account_roster_snapshots WHERE source_family='system'"
        ).fetchone()[0]
        result = upsert_system_members(
            self.connection,
            [
                {
                    "platform": "douyin",
                    "uid": "123456789",
                    "nickname": "更新昵称",
                    "sec_user_id": "MS4w.valid",
                },
                {"platform": "unknown", "uid": "bad"},
                {
                    "platform": "xiaohongshu",
                    "uid": "5c668b3e0000000012021605",
                    "nickname": "新增账号",
                },
            ],
            raw_root=self.raw,
            actor="operator",
            reason="bulk managed account import",
            sealed_at="2026-09-02T12:01:00Z",
        )
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(
            [row["status"] for row in result["results"]],
            ["updated", "rejected", "inserted"],
        )
        after = self.connection.execute(
            "SELECT COUNT(*) FROM account_roster_snapshots WHERE source_family='system'"
        ).fetchone()[0]
        self.assertEqual(after, before + 1)
        members = current_system_members(self.connection)
        self.assertEqual(len(members), 2)
        self.assertEqual(members[0]["sec_user_id"], "MS4w.valid")

    def test_runtime_reads_follow_activation_not_latest_family_snapshot(self) -> None:
        account_id, accepted = self._bootstrap()
        snapshot = latest_family_snapshot(self.connection, SYSTEM_SOURCE_FAMILY)
        assert snapshot is not None
        activation = append_activation(
            self.connection,
            profile_id=TIKHUB_PROFILE,
            roster_snapshot_id=int(accepted["snapshot_id"]),
            roster_members_sha256=str(snapshot["members_sha256"]),
            effective_at="2026-09-02T12:00:00Z",
            build_receipt_sha256="c" * 64,
            actor="operator",
            reason="switch to managed mode",
            created_at="2026-09-02T12:00:00Z",
        )
        roster = runtime_account_summary(
            self.connection, at="2026-09-02T12:00:00Z"
        )
        self.assertEqual(roster["active_profile_id"], TIKHUB_PROFILE)
        self.assertEqual(roster["activation_id"], activation["activation_id"])
        self.assertEqual(roster["source_family"], SYSTEM_SOURCE_FAMILY)
        members = get_current_members(
            self.connection, snapshot_id=roster["snapshot_id"], enabled_only=True
        )
        self.assertEqual([member["account_id"] for member in members], [account_id])
        account = self.connection.execute(
            "SELECT * FROM accounts WHERE id=?", (account_id,)
        ).fetchone()
        self.assertIsNotNone(account)
        assert account is not None
        self.assertEqual(int(account["id"]), account_id)
        model = account_read_model(self.connection, account, roster=roster)
        self.assertNotIn("roster_state", model)
        self.assertEqual(model["account_status"], "unmarked")
        self.assertEqual(model["platforms"][0]["matrix_account_id"], None)

    def test_duplicate_import_uses_last_row_and_noop_does_not_seal(self) -> None:
        self._bootstrap()
        result = upsert_system_members(
            self.connection,
            [
                {"platform": "douyin", "uid": "123456789", "nickname": "旧"},
                {
                    "platform": "douyin",
                    "uid": "123456789",
                    "nickname": "矩阵昵称",
                },
            ],
            raw_root=self.raw,
            actor="operator",
            reason="deduplicate import",
            sealed_at="2026-09-02T12:01:00Z",
        )
        self.assertEqual(result["status"], "unchanged")
        self.assertEqual(
            [row["status"] for row in result["results"]],
            ["duplicate_in_file", "unchanged"],
        )
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM account_roster_snapshots WHERE source_family='system'"
            ).fetchone()[0],
            1,
        )

    def test_remove_seals_empty_roster_without_deleting_identity_history(self) -> None:
        account_id, _ = self._bootstrap()
        result = remove_system_member(
            self.connection,
            account_id,
            raw_root=self.raw,
            actor="operator",
            reason="remove managed member",
            sealed_at="2026-09-02T12:02:00Z",
        )
        self.assertEqual(result["status"], "accepted")
        self.assertEqual(current_system_members(self.connection), [])
        self.assertEqual(
            self.connection.execute(
                "SELECT COUNT(*) FROM account_platform_identities"
            ).fetchone()[0],
            1,
        )
        self.assert_roster_error(
            "system_member_not_found",
            remove_system_member,
            self.connection,
            account_id,
            raw_root=self.raw,
            actor="operator",
            reason="remove again",
        )

    def test_initial_empty_roster_and_unresolved_matrix_bootstrap_fail_closed(self) -> None:
        self.assert_roster_error(
            "incomplete_roster",
            seal_system_members,
            self.connection,
            [],
            raw_root=self.raw,
            actor="operator",
            reason="empty bootstrap",
            sealed_at="2026-09-02T12:00:00Z",
        )
        self.connection.execute(
            """INSERT INTO accounts(phone,enabled,created_at,updated_at)
               VALUES ('',1,'2026-09-02T00:00:00Z','2026-09-02T00:00:00Z')"""
        )
        account_id = int(self.connection.execute("SELECT last_insert_rowid()").fetchone()[0])
        self.connection.execute(
            """INSERT INTO account_platform_identities(
                   account_id,platform,uid,nickname,source,created_at,updated_at)
               VALUES (?,'douyin',NULL,'','test','2026-09-02T00:00:00Z',
                       '2026-09-02T00:00:00Z')""",
            (account_id,),
        )
        self.connection.commit()
        captured = "2026-09-02T11:00:00Z"
        members = [
            {
                "platform": "douyin",
                "matrix_account_id": "matrix-unresolved",
                "profile_ref": "https://www.douyin.com/user/MS4w.unresolved",
                "uid": None,
                "nickname": "",
            }
        ]
        source = json.dumps({"members": members}).encode()
        candidate = prepare_candidate(
            self.connection,
            {
                "source_type": "bootstrap_export",
                "source_captured_at": captured,
                "scope": {
                    "organization": "test",
                    "coverage": "full",
                    "account_scope": "all_added_accounts",
                    "platforms": sorted(PLATFORMS),
                },
                "source_evidence": {
                    "kind": "official_export",
                    "evidence_kind": "operator_declaration",
                    "export_record_id": "matrix-unresolved",
                    "exported_at": captured,
                    "source_sha256": hashlib.sha256(source).hexdigest(),
                    "source_name": "matrix.json",
                    "scope_evidence": "complete test Matrix roster",
                },
                "declared_count": 1,
                "pagination": {
                    "pages": [1],
                    "expected_pages": 1,
                    "terminal": True,
                    "declared_totals": [1],
                },
                "members": members,
            },
            source_bytes=source,
            raw_root=self.raw,
            observed_at=captured,
        )
        accept_candidate(
            self.connection, int(candidate["candidate_id"]), accepted_at=captured
        )
        self.assert_roster_error(
            "identity_unresolved",
            bootstrap_system_roster_from_matrix,
            self.connection,
            raw_root=self.raw,
            actor="operator",
            reason="unresolved bootstrap",
            sealed_at="2026-09-02T12:00:00Z",
        )


if __name__ == "__main__":
    unittest.main()
