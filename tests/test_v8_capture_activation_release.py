"""Real schema20 activation/drain ledgers, isolated installation/sample proofs.

Installed filesystem and already-tested 200-sample verification are mocked here;
these fixtures are never production qualification or a provider execution.
"""

from __future__ import annotations

import copy
import unittest
from unittest.mock import patch

from tests import test_v8_profile_control as fixture
from v8 import capture_activation_release as successor
from v8 import capture_authorizations as auth, capture_release as release
from v8 import paid_drain, profile_control, provider_budget, storage
from v8.profile_activations import (
    INTEGRATED_PROFILE,
    TIKHUB_PROFILE,
    activation_at,
    append_activation,
    cancel_activation,
)

AT = fixture.BEGIN
AFTER = "2026-09-01T16:01:00Z"
EXPIRES = "2026-09-02T11:00:00Z"
OPERATION = "douyin_video_statistics"


class CaptureActivationReleaseTest(unittest.TestCase):
    def setUp(self):
        base = fixture.ProfileControlTest()
        base.setUp()
        self.addCleanup(base.doCleanups)
        self.base, self.db = base, base.db
        with storage.connect(self.db) as connection, storage.transaction(connection):
            self.source = append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=base.system["id"],
                roster_members_sha256=base.system["members_sha256"],
                effective_at="2026-09-01T01:00:00Z",
                build_receipt_sha256=fixture.BUILD,
                actor="fixture",
                created_at="2026-09-01T01:00:00Z",
            )
            paid_drain.start_profile_drain_in_transaction(
                connection,
                "mode-b",
                switch_kind="cross_profile",
                now="2026-09-01T01:00:00Z",
                binding={
                    "target_activation_id": self.source["activation_id"],
                    "source_activation_id": base.initial_activation_id,
                    "business_day": "2026-09-01",
                    "planned_effective_at": self.source["effective_at"],
                    "build_receipt_sha256": fixture.BUILD,
                    "runtime_root_receipt_sha256": fixture.RUNTIME,
                },
            )
            paid_drain.seal_profile_drain_in_transaction(
                connection, "mode-b", now="2026-09-01T01:00:00Z"
            )
            paid_drain.release_profile_drain_in_transaction(
                connection, "mode-b", now="2026-09-01T01:00:00Z"
            )
        with storage.connect(self.db) as connection:
            storage.initialize_database(connection, target_version=20)
        for module in (successor, auth, profile_control):
            self.enterContext(
                patch.object(module, "require_current_process_writer_lock")
            )
        self.evidence = {
            "active": self.source,
            "build_sha256": fixture.BUILD,
            "runtime_sha256": fixture.RUNTIME,
            "config_sha256": "c" * 64,
            "manifest": {
                "request_host": "fixture.invalid",
                "http_stack": "fixture-stack",
                "transport_route_id": "fixture-route",
                "route_generation": "fixture-generation",
            },
            "deployment": {
                "status": "accepted",
                "deployment_id": "only-acceptance",
                "receipt_sha256": "d" * 64,
                "bindings": successor._active(self.source),
            },
        }
        self.enterContext(
            patch.object(release, "_installed_evidence", side_effect=self._installed)
        )
        self.qualifications = {}
        self.enterContext(
            patch.object(
                release,
                "snapshot_operation_qualification",
                side_effect=self._qualification,
            )
        )
        self.frozen_verifier = self.enterContext(
            patch.object(
                release,
                "validate_frozen_operation_qualification",
                side_effect=self._verify_qualification,
            )
        )
        with storage.connect(self.db) as connection, storage.transaction(connection):
            connection.execute(
                "INSERT INTO deployment_readiness_receipts(deployment_id,status,payload_json,recorded_at,receipt_sha256) VALUES('only-acceptance','accepted','{}',?,?)",
                (AT, "d" * 64),
            )
            bindings = {
                **{
                    key: self.source[key]
                    for key in auth.BINDING_KEYS
                    if key in self.source
                },
                "build_receipt_sha256": fixture.BUILD,
                "runtime_root_receipt_sha256": fixture.RUNTIME,
                "config_receipt_sha256": "c" * 64,
                "continuity_permit_sha256": "e" * 64,
            }
            ready = {
                "provider": "tikhub",
                "operation": OPERATION,
                "status": "ready",
                "reason": "source-fixture",
                "evidence_json": "{}",
                "created_at": AT,
                "expires_at": EXPIRES,
            }
            ready_id = connection.execute(
                f"INSERT INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) VALUES ({','.join('?' for _ in range(len(ready) + 1))})",
                (*ready.values(), auth.digest(ready)),
            ).lastrowid
            gate = {
                "provider": "tikhub",
                "operation": OPERATION,
                "state": "open",
                "reason": "source-fixture",
                "evidence_json": auth.canonical({"bindings": bindings}),
                "recorded_at": AT,
            }
            gate_id = connection.execute(
                f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) VALUES ({','.join('?' for _ in range(len(gate) + 1))})",
                (*gate.values(), auth.digest(gate)),
            ).lastrowid
        qualification = {
            "contract": "capture-operation-qualification-snapshot-v1",
            "operation": OPERATION,
            "runtime_bindings": {
                "active": successor._active(self.source),
                **successor._runtime(self.evidence),
            },
            "manifest": self.evidence["manifest"],
            "transport_code_sha256": release._transport_code(),
            "expires_at": EXPIRES,
            "gate_id": gate_id,
            "readiness_id": ready_id,
            "authority_bindings": bindings,
        }
        self.qualifications[OPERATION] = {
            **qualification,
            "snapshot_sha256": auth.digest(qualification),
        }

    def _qualification(self, connection, operation, at):
        del connection, at
        if operation not in self.qualifications:
            raise auth.AuthorizationError(
                "Source operation has no actual qualification"
            )
        return copy.deepcopy(self.qualifications[operation])

    def _verify_qualification(self, connection, qualification, at):
        del connection, at
        if qualification != self.qualifications.get(qualification.get("operation")):
            raise auth.AuthorizationError("Frozen source sample proof changed")

    def _installed(self, connection, *, at):
        active = activation_at(connection, at)
        result = {**copy.deepcopy(self.evidence), "active": active}
        if active["profile_id"] == INTEGRATED_PROFILE:
            result["activation_successor"] = (
                successor.validate_installed_activation_successor(
                    connection,
                    source_deployment=result["deployment"],
                    current_active=active,
                    runtime_bindings=successor._runtime(result),
                    manifest=result["manifest"],
                    at=at,
                )
            )
        return result

    def _snapshot(self):
        with storage.connect(self.db) as connection:
            before = connection.total_changes
            snapshot = successor.snapshot_source_operations(
                connection, operations=[OPERATION], at=AT
            )
            self.assertEqual(connection.total_changes, before)
            return snapshot

    def _begin(self, *, snapshot=True):
        value = self._snapshot() if snapshot else None
        result = profile_control.begin_cross_profile_switch(
            db_path=self.db,
            drain_id="integrated",
            target_profile_id=INTEGRATED_PROFILE,
            roster_snapshot_id=self.base.system["id"],
            build_receipt_sha256=fixture.BUILD,
            runtime_root_receipt_sha256=fixture.RUNTIME,
            actor="fixture",
            reason="integrated handoff",
            now=AT,
            capture_source_operations=value,
        )
        self.target = result["activation"]
        return result

    def _complete(self):
        return profile_control.complete_cross_profile_switch(
            db_path=self.db,
            drain_id="integrated",
            now=fixture.COMPLETE,
        )

    def _publish(self, at=AFTER):
        with storage.connect(self.db) as connection, storage.transaction(connection):
            return successor.publish_target_operation_gate(
                connection, operation=OPERATION, at=at
            )

    def _assert_no_new_capture(self):
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_request_start_events"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM transport_continuity_permits"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM deployment_readiness_receipts WHERE status='accepted'"
                ).fetchone()[0],
                1,
            )

    def test_snapshot_readonly_and_independent_target_gate_after_legal_effective(self):
        self._begin()
        self._complete()
        receipt = self._publish()
        self.assertTrue(receipt["ordinary_paid_authorized"])
        self.assertEqual(receipt["activation_id"], self.target["activation_id"])
        self.assertFalse(receipt["coverage_complete"])
        self.assertEqual(receipt, self._publish())
        with storage.connect(self.db) as connection:
            bindings = successor.current_runtime_bindings(connection, OPERATION, AFTER)
            self.assertNotEqual(bindings["continuity_permit_sha256"], "e" * 64)
            self.assertEqual(bindings["profile_id"], INTEGRATED_PROFILE)
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM capture_paid_send_gate_events"
                ).fetchone()[0],
                2,
            )
        self._assert_no_new_capture()

    def test_a_and_b_revalidate_current_target_and_authority_readonly(self):
        self._begin()
        self._complete()
        self._publish()
        with (
            storage.connect(self.db) as connection,
            storage.transaction(connection),
            auth.runtime_authority(successor.current_runtime_bindings),
        ):
            before = connection.total_changes
            values = {
                "operation": OPERATION,
                "request_identity": "f" * 64,
                "at": AFTER,
                "amount_microusd": provider_budget.PRICES_MICROUSD[OPERATION],
            }
            first = auth.validate_authorization(
                connection,
                runtime_bindings=auth.current_runtime_bindings(
                    connection, OPERATION, AFTER
                ),
                **values,
            )
            second = auth.validate_authorization(
                connection,
                runtime_bindings=auth.current_runtime_bindings(
                    connection, OPERATION, AFTER
                ),
                expected_authority_sha256=first["authority_sha256"],
                **values,
            )
            self.assertEqual(first, second)
            self.assertEqual(connection.total_changes, before)
        self._assert_no_new_capture()

    def test_schema20_and_legacy_admission_share_fixed_budget_boundaries(self):
        self._begin()
        self._complete()
        self._publish()
        amount = provider_budget.PRICES_MICROUSD[OPERATION]
        with storage.connect(self.db) as connection, storage.transaction(connection):
            bindings = successor.current_runtime_bindings(connection, OPERATION, AFTER)
            values = {"runtime_bindings": bindings, "operation": OPERATION,
                      "request_identity": "f" * 64, "at": AFTER, "amount_microusd": amount}
            initial = auth.validate_authorization(connection, **values)
            for total, metrics, allowed in (
                (15_000_000 - amount, 15_000_000 - amount, True),
                (15_000_000, 15_000_000, False),
                (50_000_000 - amount, 0, True),
                (50_000_000, 0, False),
                (100_000_000, 0, False),
            ):
                summary = {"budget_day": provider_budget.budget_day(AFTER), "total_microusd": total,
                           "buckets_microusd": {"discovery": 0, "metrics": metrics, "repair": 0}}
                with self.subTest(total=total, metrics=metrics), patch.object(
                        provider_budget, "budget_summary", return_value=summary):
                    calls = (
                        lambda: provider_budget.check_reservation(connection,
                            scope=provider_budget.PaidScope(category="metrics"), operation=OPERATION,
                            unit_price=amount / 1_000_000, currency="USD", at=AFTER),
                        lambda: auth.validate_authorization(connection, **values),
                        lambda: auth.validate_authorization(connection,
                            expected_authority_sha256=initial["authority_sha256"], **values),
                    )
                    for call in calls:
                        if allowed:
                            self.assertEqual(call()["budget_bucket"], "metrics")
                        else:
                            with self.assertRaises(provider_budget.PaidScopeBlocked):
                                call()
        self._assert_no_new_capture()

    def test_shared_budget_policy_preserves_narrower_immutable_envelopes(self):
        summary = {"total_microusd": 1_000, "buckets_microusd": {"metrics": 1_000}}
        budget = {"total_microusd": 50_000_000, "bucket": "metrics", "bucket_microusd": 2_000}
        bucket, blocker = provider_budget.assess_budget_capacity(summary, operation=OPERATION,
            amount_microusd=1_000, authorization_budget=budget)
        self.assertEqual(bucket, "metrics")
        self.assertIsNone(blocker)
        _, blocker = provider_budget.assess_budget_capacity(summary, operation=OPERATION,
            amount_microusd=1_001, authorization_budget=budget)
        self.assertEqual(blocker.error_code, "metrics_budget_exhausted")
        for changed in ({"total_microusd": 50_000_001}, {"bucket_microusd": 15_000_001},
                        {"bucket": "discovery"}, {"total_microusd": True}, {"bucket_microusd": 0}):
            with self.subTest(changed=changed):
                _, blocker = provider_budget.assess_budget_capacity(summary, operation=OPERATION,
                    amount_microusd=1_000, authorization_budget={**budget, **changed})
                self.assertEqual(blocker.error_code, "authorization_budget_invalid")

    def test_no_snapshot_generic_legal_activation_has_no_paid_gate(self):
        self._begin(snapshot=False)
        self._complete()
        with self.assertRaisesRegex(
            auth.AuthorizationError, "no qualified source snapshot"
        ):
            self._publish()
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM capture_paid_send_gate_events"
                ).fetchone()[0],
                1,
            )
        self._assert_no_new_capture()

    def test_source_diagnostic_or_missing_qualification_cannot_begin(self):
        self.qualifications.clear()
        with self.assertRaisesRegex(auth.AuthorizationError, "no actual qualification"):
            successor.begin_integrated_switch(
                db_path=self.db,
                drain_id="integrated",
                roster_snapshot_id=self.base.system["id"],
                operations=[OPERATION],
                actor="fixture",
                reason="missing source",
                now=AT,
            )
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0],
                2,
            )

    def test_integrated_begin_wrapper_is_idempotent_during_its_own_drain(self):
        arguments = {
            "db_path": self.db,
            "drain_id": "integrated",
            "roster_snapshot_id": self.base.system["id"],
            "operations": [OPERATION],
            "actor": "fixture",
            "reason": "qualified switch",
            "now": AT,
        }
        first = successor.begin_integrated_switch(**arguments)
        repeated = successor.begin_integrated_switch(**arguments)
        self.assertEqual(first["activation"], repeated["activation"])
        self.assertTrue(repeated["idempotent"])
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM pipeline_paid_drain_events WHERE drain_id='integrated'",
                ).fetchone()[0],
                1,
            )
        self._assert_no_new_capture()

    def test_forged_snapshot_is_rechecked_before_target_is_written(self):
        snapshot = self._snapshot()
        snapshot["operations"][OPERATION]["expires_at"] = "2026-09-03T00:00:00Z"
        snapshot["snapshot_sha256"] = auth.digest(
            {key: value for key, value in snapshot.items() if key != "snapshot_sha256"}
        )
        with self.assertRaisesRegex(auth.AuthorizationError, "Frozen source sample"):
            profile_control.begin_cross_profile_switch(
                db_path=self.db,
                drain_id="forged",
                target_profile_id=INTEGRATED_PROFILE,
                roster_snapshot_id=self.base.system["id"],
                build_receipt_sha256=fixture.BUILD,
                runtime_root_receipt_sha256=fixture.RUNTIME,
                actor="fixture",
                reason="forgery",
                now=AT,
                capture_source_operations=snapshot,
            )
        with storage.connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM acquisition_profile_activations"
                ).fetchone()[0],
                2,
            )

    def test_before_complete_before_effective_and_cancelled_target_are_rejected(self):
        self._begin()
        with self.assertRaises(auth.AuthorizationError):
            self._publish()
        self._complete()
        with self.assertRaises(auth.AuthorizationError):
            self._publish(at=fixture.COMPLETE)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            cancel_activation(
                connection,
                self.target["activation_id"],
                cancelled_at="2026-09-01T15:00:00Z",
                actor="fixture",
                reason="cancel target",
            )
        with self.assertRaises(auth.AuthorizationError):
            self._publish()
        self._assert_no_new_capture()

    def test_expiry_runtime_manifest_and_source_deployment_drift_rejected(self):
        self._begin()
        self._complete()
        with self.assertRaisesRegex(auth.AuthorizationError, "expired"):
            self._publish(at=EXPIRES)
        original = copy.deepcopy(self.evidence)
        for key in ("build_sha256", "runtime_sha256", "config_sha256"):
            with self.subTest(key=key):
                self.evidence[key] = "0" * 64
                with self.assertRaises(auth.AuthorizationError):
                    self._publish()
                self.evidence = copy.deepcopy(original)
        self.evidence["manifest"]["route_generation"] = "changed"
        with self.assertRaises(auth.AuthorizationError):
            self._publish()
        self.evidence = copy.deepcopy(original)
        self.evidence["deployment"]["receipt_sha256"] = "0" * 64
        with self.assertRaises(auth.AuthorizationError):
            self._publish()
        self._assert_no_new_capture()

    def test_target_hold_after_a_prevents_b(self):
        self._begin()
        self._complete()
        self._publish()
        with storage.connect(self.db) as connection:
            successor.current_runtime_bindings(connection, OPERATION, AFTER)
        with storage.connect(self.db) as connection, storage.transaction(connection):
            profile_control.begin_current_activation_hold_in_transaction(
                connection,
                drain_id="post-switch-hold",
                build_receipt_sha256=fixture.BUILD,
                runtime_root_receipt_sha256=fixture.RUNTIME,
                actor="fixture",
                reason="new hold",
                not_before_business_day="2026-09-03",
                now="2026-09-01T16:02:00Z",
            )
        with storage.connect(self.db) as connection:
            with self.assertRaisesRegex(
                auth.AuthorizationError, "drain or RELEASE changed"
            ):
                successor.current_runtime_bindings(
                    connection, OPERATION, "2026-09-01T16:03:00Z"
                )
        self._assert_no_new_capture()

    def test_operation_not_in_frozen_source_cannot_get_target_authority(self):
        self._begin()
        self._complete()
        with storage.connect(self.db) as connection, storage.transaction(connection):
            with self.assertRaisesRegex(
                auth.AuthorizationError, "no frozen source qualification"
            ):
                successor.publish_target_operation_gate(
                    connection, operation="douyin_video_detail", at=AFTER
                )
        self._assert_no_new_capture()

    def test_expired_sibling_operation_does_not_revoke_valid_operation(self):
        sibling = "douyin_video_detail"
        expiry = "2026-09-01T16:00:30Z"
        qualification = copy.deepcopy(self.qualifications[OPERATION])
        with storage.connect(self.db) as connection, storage.transaction(connection):
            for table, sha_column, id_key in (
                ("provider_readiness_receipts", "receipt_sha256", "readiness_id"),
                ("capture_paid_send_gate_events", "event_sha256", "gate_id"),
            ):
                values = dict(
                    connection.execute(
                        f"SELECT * FROM {table} ORDER BY id LIMIT 1"
                    ).fetchone()
                )
                values.pop("id")
                values.pop(sha_column)
                values["operation"] = sibling
                if table == "provider_readiness_receipts":
                    values["expires_at"] = expiry
                identifier = connection.execute(
                    f"INSERT INTO {table}({','.join(values)},{sha_column}) VALUES ({','.join('?' for _ in range(len(values) + 1))})",
                    (*values.values(), auth.digest(values)),
                ).lastrowid
                qualification[id_key] = identifier
        qualification["operation"] = sibling
        qualification["expires_at"] = expiry
        qualification.pop("snapshot_sha256")
        qualification["snapshot_sha256"] = auth.digest(qualification)
        self.qualifications[sibling] = qualification
        with storage.connect(self.db) as connection:
            snapshot = successor.snapshot_source_operations(
                connection, operations=[OPERATION, sibling], at=AT
            )
        profile_control.begin_cross_profile_switch(
            db_path=self.db,
            drain_id="integrated",
            target_profile_id=INTEGRATED_PROFILE,
            roster_snapshot_id=self.base.system["id"],
            build_receipt_sha256=fixture.BUILD,
            runtime_root_receipt_sha256=fixture.RUNTIME,
            actor="fixture",
            reason="two operations",
            now=AT,
            capture_source_operations=snapshot,
        )
        self._complete()
        with storage.connect(self.db) as connection:
            before = successor.current_runtime_bindings(
                connection, OPERATION, "2026-09-01T16:00:10Z"
            )
            after = successor.current_runtime_bindings(connection, OPERATION, AFTER)
            self.assertEqual(before, after)
            with self.assertRaisesRegex(auth.AuthorizationError, "expired"):
                successor.current_runtime_bindings(connection, sibling, AFTER)
        self.assertTrue(self._publish()["ordinary_paid_authorized"])
        self._assert_no_new_capture()


if __name__ == "__main__":
    unittest.main()
