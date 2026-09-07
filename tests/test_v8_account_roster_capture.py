"""Real schema20 roster/drain/gate/A validation and an actual temporary Writer lease.

Only installed filesystem acceptance and historical sample qualification use the
existing isolated fixture; no provider calls or production database are allowed.
"""

from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch

from tests import test_v8_capture_activation_release as fixture
from v8 import account_roster_capture as roster, capture_authorizations as auth
from v8 import capture_activation_release as successor, capture_release as release
from v8 import (
    paid_drain,
    pipeline,
    provider_budget,
    storage,
    account_roster,
    capture_runtime,
    capture_planning,
)
from v8.profile_activations import activation_at, cancel_activation
from v8.runtime_database import (
    DatabaseAccessMode,
    FileIdentity,
    InstalledWriterContract,
    ResolvedDatabaseAccess,
    acquire_writer_lock,
    require_current_process_writer_lock,
)

AFTER = fixture.AFTER
NEXT = "2026-09-02T16:00:00.000000Z"
LONG_TTL = "2026-09-05T11:00:00Z"
OP = fixture.OPERATION
OPERATIONS = ("douyin_user_posts", "douyin_video_detail", OP)


class AccountRosterCaptureTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.object(fixture, "EXPIRES", LONG_TTL))
        self.base = fixture.CaptureActivationReleaseTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db, self.root = self.base.db, self.base.base.root
        for operation in OPERATIONS:
            qualification = copy.deepcopy(self.base.qualifications[OP])
            qualification["operation"] = operation
            if operation != OP:
                with (
                    storage.connect(self.db) as connection,
                    storage.transaction(connection),
                ):
                    ready = dict(
                        connection.execute(
                            "SELECT * FROM provider_readiness_receipts WHERE id=?",
                            (qualification["readiness_id"],),
                        ).fetchone()
                    )
                    ready = {key: ready[key] for key in roster._READY_KEYS}
                    ready["operation"] = operation
                    qualification["readiness_id"] = connection.execute(
                        f"INSERT INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(ready) + 1))})",
                        (*ready.values(), auth.digest(ready)),
                    ).lastrowid
                    gate = dict(
                        connection.execute(
                            "SELECT * FROM capture_paid_send_gate_events WHERE id=?",
                            (qualification["gate_id"],),
                        ).fetchone()
                    )
                    gate = {key: gate[key] for key in roster._GATE_KEYS}
                    gate["operation"] = operation
                    qualification["gate_id"] = connection.execute(
                        f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate) + 1))})",
                        (*gate.values(), auth.digest(gate)),
                    ).lastrowid
            qualification["snapshot_sha256"] = auth.digest(
                {k: v for k, v in qualification.items() if k != "snapshot_sha256"}
            )
            self.base.qualifications[operation] = qualification

        def snapshot():
            with storage.connect(self.db) as connection:
                return successor.snapshot_source_operations(
                    connection, operations=OPERATIONS, at=fixture.AT
                )

        self.enterContext(patch.object(self.base, "_snapshot", side_effect=snapshot))
        self.base._begin()
        self.base._complete()
        for operation in OPERATIONS:
            with (
                storage.connect(self.db) as connection,
                storage.transaction(connection),
            ):
                successor.publish_target_operation_gate(
                    connection, operation=operation, at=AFTER
                )
        # Undo the older fixture's mocked lease checks. All new paths and A use
        # the actual fcntl-held lock and database inode registry below.
        for module in (successor, auth):
            self.enterContext(
                patch.object(
                    module,
                    "require_current_process_writer_lock",
                    require_current_process_writer_lock,
                )
            )
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(
            self.root,
            self.root / "fixture.plist",
            self.root,
            self.root / "fixture.py",
            self.db,
            lock,
            {},
        )
        self.access = ResolvedDatabaseAccess(
            DatabaseAccessMode.WRITER,
            self.db,
            FileIdentity.from_stat(self.db.stat()),
            self.root,
            lock,
            installed,
        )
        self.lease = self.enterContext(acquire_writer_lock(self.access))

    def schedule(self, snapshot=None, at=AFTER, account_id=None):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            return roster.schedule_account_roster_capture_in_transaction(
                connection,
                roster_snapshot_id=(snapshot or self.base.base.system_next)["id"],
                actor="fixture",
                reason="new account",
                now=at,
                account_id=account_id,
            )

    def authorize(self, at):
        with (
            storage.connect(self.db) as connection,
            storage.transaction(connection),
            auth.runtime_authority(release.current_runtime_bindings),
        ):
            bindings = release.current_runtime_bindings(connection, OP, at)
            return auth.validate_authorization(
                connection,
                runtime_bindings=bindings,
                operation=OP,
                request_identity=auth.digest("fixture-unpurchased-request"),
                at=at,
                amount_microusd=provider_budget.PRICES_MICROUSD[OP],
            )

    def counts(self):
        with storage.connect(self.db) as connection:
            return {
                table: connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in (
                    "acquisition_profile_activations",
                    "activation_cancellations",
                    "pipeline_paid_drain_events",
                    "provider_readiness_receipts",
                    "capture_paid_send_gate_events",
                    "provider_request_start_events",
                )
            }

    def publish(self, at=NEXT):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            return roster.activate_prepared_roster_capture_in_transaction(
                connection, at=at
            )

    def test_current_gate_untouched_then_real_target_A_and_idempotent_writer(self):
        before = self.counts()
        self.authorize(AFTER)
        scheduled = self.schedule()
        self.assertTrue(scheduled["scheduled"])
        self.assertEqual(scheduled["activation"]["effective_at"], NEXT)
        self.assertEqual(
            self.counts()["provider_readiness_receipts"],
            before["provider_readiness_receipts"],
        )
        self.assertEqual(
            self.counts()["capture_paid_send_gate_events"],
            before["capture_paid_send_gate_events"],
        )
        self.authorize(AFTER)
        self.assertEqual(self.publish(AFTER)["status"], "skipped")
        with self.assertRaises(auth.AuthorizationError):
            self.authorize(NEXT)  # No gate copied forward merely by activation.
        self.assertEqual(self.publish()["status"], "issued")
        self.authorize(NEXT)
        after = self.counts()
        self.assertEqual(
            self.publish("2026-09-02T16:05:00Z")["status"], "already_issued"
        )
        self.assertEqual(self.counts(), after)
        self.assertEqual(after["provider_request_start_events"], 0)
        with storage.connect(self.db) as connection:
            gate = connection.execute(
                "SELECT evidence_json FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            payload = json.loads(gate[0])
            self.assertEqual(
                payload["bindings"]["activation_id"],
                scheduled["activation"]["activation_id"],
            )
            self.assertEqual(payload["expires_at"], LONG_TTL)

    def test_same_night_replaces_and_repeat_is_idempotent(self):
        first = self.schedule()
        same = self.schedule()
        self.assertTrue(same["idempotent"])
        with storage.connect(self.db) as connection, storage.transaction(connection):
            other = self.base.base._snapshot(connection, "system", "system-three")
        second = self.schedule(other, "2026-09-01T17:00:00Z")
        self.assertNotEqual(
            first["activation"]["activation_id"], second["activation"]["activation_id"]
        )
        self.publish()
        with storage.connect(self.db) as connection:
            self.assertEqual(
                activation_at(connection, NEXT)["activation_id"],
                second["activation"]["activation_id"],
            )
            self.assertIsNotNone(
                connection.execute(
                    "SELECT id FROM activation_cancellations WHERE activation_id=?",
                    (first["activation"]["activation_id"],),
                ).fetchone()
            )
        self.authorize(NEXT)

    def test_two_explicit_account_additions_keep_both_pending_routes(self):
        first = self.make_roster(uid="123456")
        self.schedule(first, account_id=first["test_account_id"])
        second = self.make_roster(uid="234567", include_snapshot=first)
        self.schedule(
            second, at="2026-09-01T17:00:00Z", account_id=second["test_account_id"]
        )
        result = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(result["created"], 2)
        with storage.connect(self.db) as connection:
            rows = connection.execute(
                "SELECT state,envelope_json FROM capture_work_items WHERE operation='douyin_user_posts'"
            ).fetchall()
            self.assertEqual(
                {json.loads(row["envelope_json"])["uid"] for row in rows},
                {"123456", "234567"},
            )
            self.assertTrue(all(row["state"] == "runnable" for row in rows))
        self.assertEqual(self.counts()["provider_request_start_events"], 0)

    def test_two_pending_additions_then_pause_first_only_activates_second(self):
        first = self.make_roster(uid="123456")
        self.schedule(first, account_id=first["test_account_id"])
        second = self.make_roster(uid="234567", include_snapshot=first)
        self.schedule(
            second, at="2026-09-01T17:00:00Z", account_id=second["test_account_id"]
        )
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute(
                "UPDATE accounts SET enabled=0 WHERE id=?", (first["test_account_id"],)
            )
        result = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(result["created"], 1)
        with storage.connect(self.db) as connection:
            rows = connection.execute(
                "SELECT state,envelope_json FROM capture_work_items WHERE operation='douyin_user_posts'"
            ).fetchall()
            self.assertEqual(
                [json.loads(row["envelope_json"])["uid"] for row in rows], ["234567"]
            )
            self.assertEqual(rows[0]["state"], "runnable")
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM capture_route_assignments WHERE account_id=?",
                    (first["test_account_id"],),
                ).fetchone()[0],
                0,
            )
        self.assertEqual(self.counts()["provider_request_start_events"], 0)

    def test_second_midnight_inherits_legal_chain_without_extending_ttl(self):
        self.schedule()
        self.publish()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            other = self.base.base._snapshot(connection, "system", "system-third-night")
        third = self.schedule(other, "2026-09-02T17:00:00Z")
        self.assertEqual(
            third["activation"]["effective_at"], "2026-09-03T16:00:00.000000Z"
        )
        self.publish(third["activation"]["effective_at"])
        self.authorize(third["activation"]["effective_at"])
        self.assertEqual(self.counts()["provider_request_start_events"], 0)

    def test_expiry_before_midnight_rolls_back_all_preparation(self):
        before = self.counts()
        with self.assertRaisesRegex(roster.AccountRosterCaptureError, "生效时间"):
            self.schedule(at="2026-09-05T01:00:00Z")
        self.assertEqual(before, self.counts())

    def test_no_actual_writer_lease_rejects_even_if_caller_has_a_boolean(self):
        from v8 import runtime_database

        before = self.counts()
        with patch.object(runtime_database, "_PROCESS_WRITER_LEASES", {}):
            with self.assertRaises(roster.AccountRosterCaptureError):
                self.schedule()
        self.assertEqual(before, self.counts())

    def test_mode_b_rejected_without_false_scheduled_success(self):
        before = self.counts()
        with self.assertRaisesRegex(
            roster.AccountRosterCaptureError, "采集服务尚未就绪"
        ):
            self.schedule(at=fixture.AT)
        self.assertEqual(before, self.counts())

    def test_new_platform_requires_its_existing_qualified_operations(self):
        other = self.make_roster("xiaohongshu", "692fe74d000000003801e9ca")
        before = self.counts()
        with self.assertRaises(roster.AccountRosterCaptureError):
            self.schedule(other)
        self.assertEqual(before, self.counts())

    def make_roster(self, platform="douyin", uid="123456", include_snapshot=None):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "INSERT INTO accounts(phone,created_at,updated_at) VALUES('',?,?)",
                (AFTER, AFTER),
            ).lastrowid
            identity = connection.execute(
                "INSERT INTO account_platform_identities(account_id,platform,uid,created_at,updated_at) VALUES(?,?,?,?,?)",
                (account, platform, uid, AFTER, AFTER),
            ).lastrowid
            if platform == "douyin":
                connection.execute(
                    "INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) VALUES(?,'TikHub','sec_user_id',?,?,?)",
                    (identity, "MS4wLjAB" + "x" * 32 + uid, AFTER, AFTER),
                )
            snapshot = dict(
                connection.execute(
                    "SELECT * FROM account_roster_snapshots WHERE id=?",
                    (self.base.base.system_next["id"],),
                ).fetchone()
            )
            snapshot.pop("id")
            existing = (
                [
                    dict(row)
                    for row in connection.execute(
                        "SELECT * FROM account_roster_members WHERE snapshot_id=?",
                        (include_snapshot["id"],),
                    )
                ]
                if include_snapshot
                else []
            )
            snapshot.update(
                source_instance_id="fixture-new-member:" + uid,
                declared_count=1 + len(existing),
                member_count=1 + len(existing),
                source_sha256=auth.digest([platform, uid]),
                members_sha256=auth.digest([platform, uid]),
            )
            snapshot_id = connection.execute(
                f"INSERT INTO account_roster_snapshots({','.join(snapshot)}) VALUES ({','.join('?' for _ in snapshot)})",
                tuple(snapshot.values()),
            ).lastrowid
            connection.execute(
                "INSERT INTO account_roster_members(snapshot_id,account_identity_id,platform,member_key,uid,monitoring_status,authorization_status) VALUES(?,?,?,?,?,'unknown','unknown')",
                (snapshot_id, identity, platform, "uid:" + platform + ":" + uid, uid),
            )
            for member in existing:
                member["snapshot_id"] = snapshot_id
                connection.execute(
                    f"INSERT INTO account_roster_members({','.join(member)}) VALUES ({','.join('?' for _ in member)})",
                    tuple(member.values()),
                )
            return {
                **account_roster.snapshot_by_id(connection, snapshot_id),
                "test_account_id": account,
            }

    def test_new_member_enters_real_planner_and_pause_leaves_cohort(self):
        target = self.schedule(self.make_roster())["activation"]
        # Actual installed Writer job: automatic issuance, route CAS, planner,
        # reference/readiness checks and persisted runnable work; no mock plan.
        result = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(result["created"], 1)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            work = connection.execute(
                "SELECT * FROM capture_work_items WHERE operation='douyin_user_posts'"
            ).fetchone()
            self.assertIsNotNone(work)
            self.assertEqual(work["state"], "runnable")
            envelope = json.loads(work["envelope_json"])
            self.assertEqual(envelope["uid"], "123456")
            self.assertEqual(
                envelope["roster_snapshot_id"], target["roster_snapshot_id"]
            )
            account_id = work["account_id"]
            self.assertTrue(
                roster.validate_current_account_capture(
                    connection, account_id=account_id, now=NEXT
                )["validated"]
            )
            connection.execute(
                "UPDATE accounts SET enabled=0 WHERE id=?", (account_id,)
            )
            active = activation_at(connection, NEXT)
            plan = capture_runtime._cohort_plan(
                connection, active, at="2026-09-03T00:11:00Z", shadow=False
            )
            self.assertEqual(plan["cohort"], [])
        self.assertEqual(self.counts()["provider_request_start_events"], 0)

    def test_first_tick_preserves_post_midnight_gate_hold_and_not_ready(self):
        self.schedule()
        for table, fields, checksum in (
            (
                "capture_paid_send_gate_events",
                {
                    "provider": "tikhub",
                    "operation": OP,
                    "state": "closed",
                    "reason": "operator-hold",
                    "evidence_json": "{}",
                    "recorded_at": "2026-09-02T16:01:00Z",
                },
                "event_sha256",
            ),
            (
                "provider_readiness_receipts",
                {
                    "provider": "tikhub",
                    "operation": OP,
                    "status": "blocked",
                    "reason": "operator-hold",
                    "evidence_json": "{}",
                    "created_at": "2026-09-02T16:01:00Z",
                    "expires_at": LONG_TTL,
                },
                "receipt_sha256",
            ),
        ):
            with self.subTest(table=table), storage.connect(self.db) as connection:
                connection.execute("BEGIN")
                connection.execute(
                    f"INSERT INTO {table}({','.join(fields)},{checksum}) VALUES ({','.join('?' for _ in range(len(fields) + 1))})",
                    (*fields.values(), auth.digest(fields)),
                )
                before = connection.total_changes
                with self.assertRaises(auth.AuthorizationError):
                    roster.activate_prepared_roster_capture_in_transaction(
                        connection, at="2026-09-02T16:02:00Z"
                    )
                self.assertEqual(connection.total_changes, before)
                connection.rollback()

    def test_first_tick_refuses_route_change_without_partial_gates(self):
        snapshot = self.make_roster()
        self.schedule(snapshot)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "SELECT i.account_id FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
            capture_planning.assign_route(
                connection,
                scope_type="account",
                scope_key=str(account),
                provider="tikhub",
                operation="douyin_user_posts",
                expected_generation=0,
                route="legacy",
                mode="disabled",
                effective_at="2026-09-02T16:01:00Z",
                recorded_at="2026-09-02T16:01:00Z",
                account_id=account,
            )
        before = self.counts()
        with self.assertRaisesRegex(auth.AuthorizationError, "路由在安排后已改变"):
            self.publish("2026-09-02T16:02:00Z")
        self.assertEqual(before, self.counts())

    def test_pre_midnight_route_change_keeps_old_activation_and_authority(self):
        snapshot = self.make_roster()
        self.schedule(snapshot)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "SELECT i.account_id FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
            capture_planning.assign_route(
                connection,
                scope_type="account",
                scope_key=str(account),
                provider="tikhub",
                operation="douyin_user_posts",
                expected_generation=0,
                route="legacy",
                mode="disabled",
                effective_at="2026-09-02T15:59:00Z",
                recorded_at="2026-09-02T15:59:00Z",
                account_id=account,
            )
            self.assertEqual(
                activation_at(connection, NEXT)["activation_id"],
                self.base.target["activation_id"],
            )
        self.assertEqual(self.publish()["status"], "skipped")
        self.authorize(NEXT)

    def test_content_override_refuses_admission_and_is_preserved(self):
        snapshot = self.make_roster()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "SELECT i.account_id FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
            content = connection.execute(
                "INSERT INTO content_items(link_id,platform,canonical_url,account_id,title,imported_at,created_at,updated_at) VALUES('abcdef','douyin','https://www.douyin.com/video/123',?,'fixture',?,?,?)",
                (account, AFTER, AFTER, AFTER),
            ).lastrowid
            route_id = capture_planning.assign_route(
                connection,
                scope_type="content",
                scope_key=str(content),
                provider="tikhub",
                operation="douyin_video_detail",
                expected_generation=0,
                route="legacy",
                mode="disabled",
                effective_at=AFTER,
                recorded_at=AFTER,
                account_id=account,
                content_id=content,
            )
        before = self.counts()
        with self.assertRaisesRegex(roster.AccountRosterCaptureError, "已有采集限制"):
            self.schedule(snapshot)
        self.assertEqual(before, self.counts())
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT mode FROM capture_route_assignments WHERE id=?", (route_id,)
                ).fetchone()[0],
                "disabled",
            )

    def test_source_budget_caps_are_inherited_without_increase(self):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            row = connection.execute(
                "SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1",
                (OP,),
            ).fetchone()
            gate = {key: row[key] for key in roster._GATE_KEYS}
            payload = json.loads(gate["evidence_json"])
            payload["budget"]["total_microusd"] //= 2
            payload["budget"]["bucket_microusd"] //= 2
            expected = dict(payload["budget"])
            gate["evidence_json"] = auth.canonical(payload)
            gate["reason"] = "fixture-reduced-budget"
            connection.execute(
                f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate) + 1))})",
                (*gate.values(), auth.digest(gate)),
            )
        self.schedule()
        self.publish()
        with storage.connect(self.db) as connection:
            row = connection.execute(
                "SELECT evidence_json FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1",
                (OP,),
            ).fetchone()
            self.assertEqual(json.loads(row[0])["budget"], expected)
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_qualification_expiry_does_not_reset_issued_gates(self):
        self.schedule()
        self.publish()
        before = self.counts()
        self.assertEqual(
            self.publish("2026-09-05T12:00:00Z")["status"], "already_issued"
        )
        self.assertEqual(before, self.counts())
        with self.assertRaises(auth.AuthorizationError):
            self.authorize("2026-09-05T12:00:00Z")

    def test_existing_manual_route_rejects_before_scheduled(self):
        snapshot = self.make_roster()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "SELECT i.account_id FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
            capture_planning.assign_route(
                connection,
                scope_type="account",
                scope_key=str(account),
                provider="tikhub",
                operation="douyin_user_posts",
                expected_generation=0,
                route="legacy",
                mode="active",
                effective_at=AFTER,
                recorded_at=AFTER,
                account_id=account,
            )
        before = self.counts()
        with self.assertRaisesRegex(roster.AccountRosterCaptureError, "已有采集限制"):
            self.schedule(snapshot)
        self.assertEqual(before, self.counts())

    def test_pause_before_first_tick_does_not_make_later_missing_route_active(self):
        snapshot = self.make_roster()
        self.schedule(snapshot)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            account = connection.execute(
                "SELECT i.account_id FROM account_roster_members m JOIN account_platform_identities i ON i.id=m.account_identity_id WHERE m.snapshot_id=?",
                (snapshot["id"],),
            ).fetchone()[0]
            connection.execute("UPDATE accounts SET enabled=0 WHERE id=?", (account,))
        self.publish()
        with storage.connect(self.db) as connection:
            with (
                self.assertRaisesRegex(
                    roster.AccountRosterCaptureError, "尚未具备完整的采集设置"
                ),
                storage.transaction(connection),
            ):
                connection.execute(
                    "UPDATE accounts SET enabled=1 WHERE id=?", (account,)
                )
                roster.validate_current_account_capture(
                    connection, account_id=account, now="2026-09-02T16:05:00Z"
                )
            self.assertEqual(
                connection.execute(
                    "SELECT enabled FROM accounts WHERE id=?", (account,)
                ).fetchone()[0],
                0,
            )

    def test_partial_issuance_failure_rolls_back_all_gates_and_routes(self):
        self.schedule(self.make_roster())
        original = successor.publish_target_operation_gate

        def publish(connection, *, operation, at):
            if operation == OPERATIONS[-1]:
                raise auth.AuthorizationError("fixture late failure")
            return original(connection, operation=operation, at=at)

        before = self.counts()
        with patch.object(
            successor, "publish_target_operation_gate", side_effect=publish
        ):
            with self.assertRaises(auth.AuthorizationError):
                self.publish()
        self.assertEqual(before, self.counts())
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM capture_route_assignments WHERE mode='active'"
                ).fetchone()[0],
                0,
            )

    def test_cancelled_target_never_issues_and_keeps_current_gate(self):
        target = self.schedule()["activation"]
        with storage.connect(self.db) as connection, storage.transaction(connection):
            cancel_activation(
                connection,
                target["activation_id"],
                actor="fixture",
                reason="cancel",
                cancelled_at="2026-09-01T18:00:00Z",
            )
        before = self.counts()
        self.assertEqual(self.publish()["status"], "skipped")
        self.assertEqual(before, self.counts())
        self.authorize(NEXT)

    def test_closed_source_gate_rejects_and_does_not_reopen_after_issuance(self):
        target = self.schedule()["activation"]
        self.publish()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            gate = {
                "provider": "tikhub",
                "operation": OP,
                "state": "closed",
                "reason": "operator-hold",
                "evidence_json": "{}",
                "recorded_at": "2026-09-02T17:00:00.000000Z",
            }
            connection.execute(
                f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate) + 1))})",
                (*gate.values(), auth.digest(gate)),
            )
        before = self.counts()
        self.assertEqual(
            self.publish("2026-09-02T17:01:00Z")["status"], "already_issued"
        )
        self.assertEqual(before, self.counts())
        with self.assertRaises(auth.AuthorizationError):
            self.authorize("2026-09-02T17:01:00Z")
        self.assertIsInstance(target["activation_id"], int)

    def test_writer_job_automatically_issues_before_plan(self):
        self.schedule()

        def plan(*args, **kwargs):
            del args, kwargs
            self.authorize(NEXT)
            return {"status": "fixture-planned", "provider_calls": 0}

        with patch("v8.capture_runtime.plan_tick", side_effect=plan):
            result = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(result["status"], "fixture-planned")
        self.assertEqual(self.counts()["provider_request_start_events"], 0)

    def test_frozen_source_proof_tamper_blocks_first_issuance(self):
        self.schedule()
        self.base.qualifications[OP]["expires_at"] = "2099-01-01T00:00:00Z"
        before = self.counts()
        with self.assertRaises(auth.AuthorizationError):
            self.publish()
        self.assertEqual(before, self.counts())

    def test_hold_before_midnight_makes_target_ineligible(self):
        target = self.schedule()["activation"]
        with storage.connect(self.db) as connection, storage.transaction(connection):
            paid_drain.start_profile_drain_in_transaction(
                connection,
                "fixture-hold",
                switch_kind="same_profile",
                now="2026-09-01T18:00:00Z",
                binding={
                    "source_activation_id": self.base.target["activation_id"],
                    "target_activation_id": self.base.target["activation_id"],
                    "business_day": "2026-09-01",
                    "planned_effective_at": self.base.target["effective_at"],
                    "build_receipt_sha256": fixture.fixture.BUILD,
                    "runtime_root_receipt_sha256": fixture.fixture.RUNTIME,
                },
            )
            self.assertFalse(
                roster.activation_eligibility(connection, target)["eligible"]
            )
        before = self.counts()
        self.assertEqual(self.publish()["status"], "skipped")
        self.assertEqual(before, self.counts())
        with self.assertRaises(auth.AuthorizationError):
            self.authorize(NEXT)


if __name__ == "__main__":
    unittest.main()
