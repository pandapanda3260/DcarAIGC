"""Actual operator admission, roster, route, Writer and parent ledger fixtures.

Only accepted private installation files are stubbed. Operator issuance,
frozen proof, source/target validation, scheduling, renewal and claims are real.
HTTP and socket calls are forbidden; no formal database is accessed.
"""
from __future__ import annotations

import copy
import json
import socket
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from tests import test_v8_capture_activation_release as fixture
from tests import test_v8_account_roster_capture as account_fixture
from tests.test_v8_account_roster_capture import AFTER, NEXT, OPERATIONS
from v8 import account_roster_capture as roster
from v8 import capture_activation_release as successor, capture_authorizations as auth
from v8 import capture_operator_release as operator, capture_release as release
from v8 import capture_runtime, capture_singletons, capture_integrated_natural_due as natural
from v8 import pipeline, provider_budget, storage, profile_control
from v8.profile_activations import activation_at
from v8.runtime_database import (DatabaseAccessMode, FileIdentity, InstalledWriterContract,
    ResolvedDatabaseAccess, acquire_writer_lock, require_current_process_writer_lock)

SNAPSHOT = release.snapshot_operation_qualification
VERIFY_FROZEN = release.validate_frozen_operation_qualification


class OperatorRosterCaptureTest(unittest.TestCase):
    schedule = account_fixture.AccountRosterCaptureTest.schedule
    make_roster = account_fixture.AccountRosterCaptureTest.make_roster
    publish = account_fixture.AccountRosterCaptureTest.publish
    counts = account_fixture.AccountRosterCaptureTest.counts

    def setUp(self):
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.http = self.enterContext(patch.object(capture_runtime.providers, "_request_json", side_effect=AssertionError("HTTP forbidden")))
        self.base = fixture.CaptureActivationReleaseTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db, self.root = self.base.db, self.base.base.root
        self.enterContext(patch.object(release, "snapshot_operation_qualification", SNAPSHOT))
        self.enterContext(patch.object(release, "validate_frozen_operation_qualification", VERIFY_FROZEN))
        for module in (successor, auth):
            self.enterContext(patch.object(module, "require_current_process_writer_lock", require_current_process_writer_lock))
        lock = self.root / "operator-writer.lock"
        lock.touch(mode=0o600)
        installed = InstalledWriterContract(self.root, self.root / "fixture.plist", self.root,
            self.root / "fixture.py", self.db, lock, {})
        self.access = ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), self.root, lock, installed)
        self.lease = self.enterContext(acquire_writer_lock(self.access))
        self.decision = {
            "contract_version": operator.DECISION_CONTRACT,
            "production_rollout": "approved_by_user", "business_e2e": "deferred_by_user",
            "transport_qualification": "not_verified", "operations": list(OPERATIONS),
            "decision_sha256": "1" * 64, "actor": "fixture", "reason": "explicit fixture authority",
            "issued_at": fixture.AT, "runtime_bindings": successor._runtime(self.base.evidence),
            "transport_manifest": self.base.evidence["manifest"],
            "approved_target_profile": "integrated_route_v1", "bindings": successor._active(self.base.source),
        }
        self.base.evidence["deployment"]["release_decision"] = self.decision
        self.enterContext(patch.object(release, "_release_tools", return_value=SimpleNamespace(
            validate_deployment_receipt=lambda *args, **kwargs: copy.deepcopy(self.base.evidence["deployment"]))))
        with storage.connect(self.db) as connection, storage.transaction(connection):
            for operation in OPERATIONS:
                operator.publish(connection, evidence=self.base.evidence, operation=operation, at=fixture.AT)
        def snapshot():
            with storage.connect(self.db) as connection:
                return successor.snapshot_source_operations(connection, operations=OPERATIONS, at=fixture.AT)
        self.enterContext(patch.object(self.base, "_snapshot", side_effect=snapshot))
        self.base._begin()
        profile_control.complete_cross_profile_switch(db_path=self.db, drain_id="integrated", now="2026-09-01T12:05:00Z")
        with storage.connect(self.db) as connection, storage.transaction(connection):
            for operation in OPERATIONS:
                successor.publish_target_operation_gate(connection, operation=operation, at=AFTER)
        self.addCleanup(self.assert_no_provider_calls)

    def assert_no_provider_calls(self):
        self.assertEqual(self.network.call_count, 0)
        self.assertEqual(self.http.call_count, 0)
        with storage.connect(self.db) as connection:
            for table in ("provider_request_start_events", "provider_usage", "fetch_attempts"):
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)

    def authorize(self, at=NEXT):
        with storage.connect(self.db) as connection, storage.transaction(connection), auth.runtime_authority(release.current_runtime_bindings):
            for operation in OPERATIONS:
                bindings = release.current_runtime_bindings(connection, operation, at)
                auth.validate_authorization(connection, runtime_bindings=bindings, operation=operation,
                    request_identity=auth.digest([operation, "unbought"]), at=at,
                    amount_microusd=provider_budget.PRICES_MICROUSD[operation])

    def hold(self, at, *, ready=False):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            if ready:
                row = dict(connection.execute("SELECT * FROM provider_readiness_receipts WHERE operation=? ORDER BY id DESC LIMIT 1", (OPERATIONS[0],)).fetchone())
                body = {key: row[key] for key in roster._READY_KEYS}
                body.update(status="blocked", reason="operator hold", created_at=at)
                connection.execute(f"INSERT INTO provider_readiness_receipts({','.join(body)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(body)+1))})", (*body.values(), auth.digest(body)))
            else:
                body = {"provider": "tikhub", "operation": OPERATIONS[0], "state": "closed",
                        "reason": "operator hold", "evidence_json": "{}", "recorded_at": at}
                connection.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(body)},event_sha256) VALUES ({','.join('?' for _ in range(len(body)+1))})", (*body.values(), auth.digest(body)))

    def test_operator_schedule_midnight_claim_and_real_authorization(self):
        member = self.make_roster(uid="123456")
        self.schedule(member, account_id=member["test_account_id"])
        planned = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(planned["created"], 1)
        self.authorize()
        inspected = []
        def inspect(envelope, **kwargs):
            with storage.connect(self.db) as connection, storage.transaction(connection):
                work = dict(connection.execute("SELECT * FROM capture_work_items WHERE account_id=? AND operation=?", (envelope["account_id"], envelope["operation"])).fetchone())
                scope = provider_budget.freeze_scope(connection, content_id=None, account_id=work["account_id"], stage="discovery")
                request = natural._single_identity(connection, work)
                capture_singletons.freeze(connection, request=request, scope=scope, at=NEXT)
                proof = natural.validate_native_due_request(connection, request.scope_identity, work["operation"], NEXT)
                self.assertEqual(proof["source_run_id"], scope.scheduler_run_id)
                self.assertEqual(proof["primary_work_id"], work["id"])
                self.assertEqual(natural.native_route(connection, proof, NEXT), work["assignment_id"])
                bindings = release.current_runtime_bindings(connection, work["operation"], NEXT)
                auth.validate_authorization(connection, runtime_bindings=bindings, operation=work["operation"],
                    request_identity=request.scope_identity, at=NEXT,
                    amount_microusd=provider_budget.PRICES_MICROUSD[work["operation"]])
                owners = connection.execute("SELECT COUNT(*) FROM scheduler_runs r JOIN scheduler_run_attempts a ON a.scheduler_run_id=r.id WHERE r.id=? AND a.id=? AND r.status='running' AND a.status='running'", (scope.scheduler_run_id, scope.scheduler_attempt_id)).fetchone()[0]
                self.assertEqual(owners, 1)
                inspected.append(work["id"])
            raise RuntimeError("stopped before provider")
        # The lease runtime reads actual execution time separately from the
        # scheduler's logical due time; both belong to this temporary fixture.
        with patch.object(capture_runtime, "_execute_one", side_effect=inspect), \
                patch.object(capture_runtime, "now_utc", return_value=NEXT):
            capture_runtime.run_one(self.db, NEXT)
        self.assertEqual(len(inspected), 1)
        before = self.counts()
        self.assertEqual(self.publish("2026-09-02T16:03:00Z")["status"], "already_issued")
        self.assertEqual(self.counts(), before)

    def test_legitimate_renewal_keeps_scheduled_roster_and_budget(self):
        member = self.make_roster()
        self.schedule(member, account_id=member["test_account_id"])
        at = "2026-09-02T12:00:00Z"
        with storage.connect(self.db) as connection, storage.transaction(connection):
            result = release.maintain_operation_qualifications(connection, at=at, mirror_root=self.root)
            self.assertEqual({item["status"] for item in result["operations"].values()}, {"renewed", "not_enabled"})
        self.assertEqual(self.publish()["status"], "issued")
        self.authorize()
        with storage.connect(self.db) as connection:
            active = activation_at(connection, NEXT)
            for operation, frozen in roster._shape(active)["source_gates"].items():
                latest = json.loads(connection.execute("SELECT evidence_json FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1", (operation,)).fetchone()[0])
                self.assertEqual(latest["budget"], frozen["budget"])
                self.assertEqual(latest["expires_at"], "2026-09-03T16:00:00Z")

    def test_changed_source_budget_does_not_activate_prepared_roster(self):
        self.schedule(self.make_roster())
        with storage.connect(self.db) as connection, storage.transaction(connection):
            at = "2026-09-02T12:00:00Z"
            evidence = release._installed_evidence(connection, at=at)
            with patch.object(provider_budget, "AUTOMATIC_MICROUSD", provider_budget.AUTOMATIC_MICROUSD - 1):
                operator.publish(connection, evidence=evidence, operation=OPERATIONS[0], at=at)
        self.assertEqual(self.publish()["status"], "skipped")

    def test_hold_then_explicit_reopen_does_not_revive_old_preparation(self):
        self.schedule(self.make_roster())
        self.hold("2026-09-02T12:00:00.000000Z")
        with storage.connect(self.db) as connection, storage.transaction(connection):
            at = "2026-09-02T12:01:00Z"
            operator.publish(connection, evidence=release._installed_evidence(connection, at=at),
                             operation=OPERATIONS[0], at=at)
        self.assertEqual(self.publish()["status"], "skipped")

    def install_verified_code_fixture(self, *, supports_rosters=True):
        """Only the accepted code proof/files are fixtures; roster bridge is real."""
        with storage.connect(self.db) as connection:
            source = activation_at(connection, AFTER)
            state = release.paid_drain.dispatch_state(connection, at=AFTER)
            source_release = dict(connection.execute("SELECT id,event_hash FROM pipeline_paid_drain_events WHERE id=?", (state.permit_event_id,)).fetchone())
        execution = {"build_sha256": "2" * 64, "runtime_sha256": "3" * 64,
                     "config_sha256": self.decision["runtime_bindings"]["config_sha256"]}
        def installed(connection, *, at):
            active = activation_at(connection, at)
            result = {**copy.deepcopy(self.base.evidence), **execution, "active": active}
            result["activation_successor"] = successor.validate_installed_activation_successor(
                connection, source_deployment=result["deployment"], current_active=active,
                runtime_bindings=self.decision["runtime_bindings"], manifest=result["manifest"], at=at)
            code = {"origin_runtime_bindings": self.decision["runtime_bindings"], "runtime_bindings": execution,
                    "active": successor._active(active), "decision_receipt": {"fixture": "private-files-only"},
                    "plan_payload": {"active": successor._active(source), "operations": list(OPERATIONS)}}
            if supports_rosters:
                code["roster_successor_contract"] = roster.CODE_SUCCESSOR_CONTRACT
            if active["activation_id"] != source["activation_id"]:
                code["roster_successor"] = roster.validate_code_plan_roster_successor(connection,
                    source_active=successor._active(source), source_release=source_release, current_active=active,
                    source_deployment=result["deployment"], origin_runtime_bindings=self.decision["runtime_bindings"],
                    runtime_bindings=execution, manifest=result["manifest"], operations=OPERATIONS, at=at)
            code["proof_sha256"] = auth.digest(code)
            result["code_successor"] = code
            return result
        self.enterContext(patch.object(release, "_installed_evidence", side_effect=installed))
        at = "2026-09-01T16:02:00Z"
        with storage.connect(self.db) as connection, storage.transaction(connection):
            evidence = installed(connection, at=at)
            for operation in OPERATIONS:
                operator.publish(connection, evidence=evidence, operation=operation, at=at)
        return at

    def test_code_runtime_changes_keep_original_decision_and_real_new_account_queue(self):
        at = self.install_verified_code_fixture()
        member = self.make_roster()
        self.schedule(member, at=at, account_id=member["test_account_id"])
        result = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=NEXT)
        self.assertEqual(result["created"], 1)
        self.authorize()
        with storage.connect(self.db) as connection:
            active = activation_at(connection, NEXT)
            snapshot = roster._shape(active)
            self.assertNotEqual(snapshot["runtime_bindings"], snapshot["origin_runtime_bindings"])
            self.assertEqual(snapshot["origin_runtime_bindings"], self.decision["runtime_bindings"])

    def test_old_code_plan_rejects_before_preparing_false_scheduled_success(self):
        at = self.install_verified_code_fixture(supports_rosters=False)
        member = self.make_roster()
        before = self.counts()
        with self.assertRaises(roster.AccountRosterCaptureError):
            self.schedule(member, at=at, account_id=member["test_account_id"])
        self.assertEqual(before, self.counts())

    def test_second_night_keeps_exact_operator_and_code_origin_chain(self):
        at = self.install_verified_code_fixture()
        first = self.make_roster(uid="123456")
        self.schedule(first, at=at, account_id=first["test_account_id"])
        self.publish()
        second = self.make_roster(uid="234567", include_snapshot=first)
        scheduled = self.schedule(second, at="2026-09-02T17:00:00Z", account_id=second["test_account_id"])
        after = scheduled["activation"]["effective_at"]
        self.assertEqual(self.publish(after)["status"], "issued")
        self.authorize(after)
        planned = pipeline._capture_v25_job(kind="plan", db_path=self.db, at=after)
        self.assertEqual(planned["created"], 2)

    def test_hold_after_issuance_is_not_reopened_by_later_ticks(self):
        self.schedule(self.make_roster())
        self.publish()
        self.hold("2026-09-02T16:01:00.000000Z")
        before = self.counts()
        self.assertEqual(self.publish("2026-09-02T16:02:00Z")["status"], "already_issued")
        with self.assertRaises(auth.AuthorizationError):
            self.authorize("2026-09-02T16:02:00Z")
        self.assertEqual(before, self.counts())

    def test_hold_before_midnight_retains_previous_active(self):
        with storage.connect(self.db) as connection:
            old = activation_at(connection, AFTER)["activation_id"]
        self.schedule(self.make_roster())
        self.hold("2026-09-02T12:00:00.000000Z")
        self.assertEqual(self.publish()["status"], "skipped")
        with storage.connect(self.db) as connection:
            self.assertEqual(activation_at(connection, NEXT)["activation_id"], old)

    def test_hold_after_midnight_before_first_tick_does_not_reopen(self):
        self.schedule(self.make_roster())
        self.hold("2026-09-02T16:01:00.000000Z")
        before = self.counts()
        with self.assertRaises(auth.AuthorizationError):
            self.publish("2026-09-02T16:02:00Z")
        self.assertEqual(before, self.counts())

    def test_not_ready_after_midnight_before_first_tick_does_not_reopen(self):
        self.schedule(self.make_roster())
        self.hold("2026-09-02T16:01:00.000000Z", ready=True)
        before = self.counts()
        with self.assertRaises(auth.AuthorizationError):
            self.publish("2026-09-02T16:02:00Z")
        self.assertEqual(before, self.counts())

    test_two_explicit_account_additions_keep_both_pending_routes = account_fixture.AccountRosterCaptureTest.test_two_explicit_account_additions_keep_both_pending_routes
    test_two_pending_additions_then_pause_first_only_activates_second = account_fixture.AccountRosterCaptureTest.test_two_pending_additions_then_pause_first_only_activates_second

    def test_restore_existing_active_validates_operator_route(self):
        member = self.make_roster()
        self.schedule(member, account_id=member["test_account_id"])
        self.publish()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            result = roster.validate_current_account_capture(connection, account_id=member["test_account_id"], now=NEXT)
            self.assertTrue(result["validated"])

    def test_code_plan_bridge_preserves_exact_origin_and_rejects_changed_release(self):
        with storage.connect(self.db) as connection:
            source = activation_at(connection, AFTER)
            control = release.paid_drain.dispatch_state(connection, at=AFTER)
            row = connection.execute("SELECT id,event_hash FROM pipeline_paid_drain_events WHERE id=?", (control.permit_event_id,)).fetchone()
            original_release = dict(row)
        self.schedule(self.make_roster())
        self.publish()
        with storage.connect(self.db) as connection:
            arguments = dict(source_active=successor._active(source), source_release=original_release,
                current_active=activation_at(connection, NEXT), source_deployment=self.base.evidence["deployment"],
                origin_runtime_bindings=successor._runtime(self.base.evidence), runtime_bindings=successor._runtime(self.base.evidence),
                manifest=self.base.evidence["manifest"], operations=OPERATIONS, at=NEXT)
            before = connection.total_changes
            proof = roster.validate_code_plan_roster_successor(connection, **arguments)
            self.assertEqual(proof["source_active"], successor._active(source))
            self.assertEqual(proof["decision_sha256"], self.decision["decision_sha256"])
            self.assertEqual(connection.total_changes, before)
            with self.assertRaises(auth.AuthorizationError):
                roster.validate_code_plan_roster_successor(connection, **{**arguments, "source_release": {**original_release, "event_hash": "bad"}})


if __name__ == "__main__":
    unittest.main()
