"""Real catalog work, A/B claims, errors, accounting and bounded recovery.

Only installed source/qualification evidence and the HTTP response are fixtures.
All databases/raw bodies belong to a temporary Writer lease; sockets are forbidden.
"""
from __future__ import annotations

import copy
from contextlib import ExitStack
import hashlib
import json
import unittest
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from tests import test_v8_catalog_profile_execution as fixture
from tests import test_v8_account_profile_authority as profile_fixture
from tests import test_v8_provider_transport as transport_fixture
from v8 import capture_authorizations as auth, capture_compensation
from v8 import capture_runtime as runtime, capture_release as release
from v8 import capture_release_commands, profile_control, provider_budget, providers, usage_settlements
from v8.provider_transport import request_json
from v8.storage import connect, transaction

AT = fixture.AT
OPERATION = fixture.OPERATION
EXPIRES = "2026-09-02T11:00:00Z"
RETRY_AT = "2026-09-01T16:07:00Z"
REAL_AUTHORIZATION = auth.validate_authorization


class CatalogProfileCompensationTest(unittest.TestCase):
    def setUp(self):
        self.base = fixture.CatalogProfileExecutionTest()
        self.base.setUp()
        self.addCleanup(self.base.doCleanups)
        self.db, self.root = self.base.db, self.base.root
        self.work_id, self.iid, self.aid = self.base.work_id, self.base.iid, self.base.aid
        self.success_response = copy.deepcopy(self.base.response)
        self.status = 200
        self.partial_response = False
        self.echo_uid = False
        self.locators = {}
        self.at = AT
        self.enterContext(patch.object(providers, "request_json_transport", side_effect=self.http))
        # The parent fixture stubs the installed authority. Restore the complete
        # A/B validator so real request/member grants, caps and consumption run.
        self.base.authorizations.side_effect = REAL_AUTHORIZATION
        with connect(self.db) as connection, transaction(connection):
            source = json.loads(connection.execute("SELECT evidence_json FROM capture_paid_send_gate_events "
                "WHERE operation='douyin_video_statistics' ORDER BY id DESC LIMIT 1").fetchone()[0])
            self.bindings = source["bindings"]
            auth.current_runtime_bindings.return_value = self.bindings
            scope_hash = auth.scope_hash(runtime_bindings=self.bindings, provider="tikhub", operation=OPERATION)
            ready_evidence = {"contract": auth.READINESS_CONTRACT, "bindings": self.bindings,
                "scope_hash": scope_hash, "qualification": "qualified",
                "continuity_permit_sha256": self.bindings["continuity_permit_sha256"],
                "transport_manifest_sha256": source["transport_manifest_sha256"]}
            ready = {"provider": "tikhub", "operation": OPERATION, "status": "ready",
                "reason": "isolated installed qualification", "evidence_json": auth.canonical(ready_evidence),
                "created_at": AT, "expires_at": EXPIRES}
            ready_id = connection.execute(f"INSERT INTO provider_readiness_receipts({','.join(ready)},receipt_sha256) "
                f"VALUES({','.join('?' for _ in range(len(ready)+1))})", (*ready.values(), auth.digest(ready))).lastrowid
            payload = {**source, "operation": OPERATION, "scope_hash": scope_hash, "issued_at": AT,
                "expires_at": EXPIRES, "readiness_receipt_id": ready_id, "readiness_receipt_sha256": auth.digest(ready),
                "budget": {"bucket": "discovery", "bucket_microusd": 30_000_000, "total_microusd": 50_000_000}}
            gate = {"provider": "tikhub", "operation": OPERATION, "state": "open",
                "reason": "isolated installed qualification", "evidence_json": auth.canonical(payload), "recorded_at": AT}
            connection.execute(f"INSERT INTO capture_paid_send_gate_events({','.join(gate)},event_sha256) "
                f"VALUES({','.join('?' for _ in range(len(gate)+1))})", (*gate.values(), auth.digest(gate)))

    def http(self, request, **kwargs):
        self.base.calls.append(request.full_url)
        payload = copy.deepcopy(self.base.response)
        if self.echo_uid:
            uid = parse_qs(urlsplit(request.full_url).query)["uid"][0]
            payload["data"]["data"]["id_str"] = uid
            if uid in self.locators:
                payload["data"]["data"]["sec_uid"] = self.locators[uid]
        body = json.dumps(payload).encode()
        response = transport_fixture.FakeResponse(body, status=self.status,
            headers={"Content-Length": str(len(body) + (10 if self.partial_response else 0))},
            response_url=request.full_url)
        return request_json(request, **kwargs, opener=transport_fixture.FakeOpener(response), clock=lambda: self.at, chunk_size=13)

    def create_failure(self, kind="unbilled"):
        if kind == "unbilled":
            self.status = 400
            self.base.response = {"detail": {"code": 400, "message": "Please retry. This request will not be charged."}}
        elif kind == "billed":
            self.base.response = {"code": 200, "data": {"status_code": 10033, "status_msg": "fixture upstream error"}}
        elif kind == "unknown":
            self.partial_response = True
        else:
            raise AssertionError(kind)
        outcome = self.base.run_profile()
        self.assertEqual(outcome["status"], "paid_identity_hold", outcome)
        self.assertEqual(len(self.base.calls), 1)
        with connect(self.db) as connection:
            self.original_work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone())
            self.original_usage = dict(connection.execute("SELECT * FROM provider_usage WHERE operation=? ORDER BY id DESC LIMIT 1", (OPERATION,)).fetchone())
            self.details = json.loads(self.original_usage["details_json"])
            raw = connection.execute("SELECT * FROM provider_raw_responses WHERE operation=? ORDER BY id DESC LIMIT 1", (OPERATION,)).fetchone()
            self.original_raw = dict(raw) if raw else None
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_paid_scope_claims").fetchone()[0], 2)
        self.status, self.partial_response = 200, False
        self.base.response = copy.deepcopy(self.success_response)

    def file_reference(self, name, value):
        path = self.root / name
        encoded = (auth.canonical(value) + "\n").encode()
        path.write_bytes(encoded)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(encoded).hexdigest(), "byte_size": len(encoded)}

    def install_recovery_authority(self, *, changes=None):
        with connect(self.db) as connection:
            previous = copy.deepcopy(release._installed_evidence(connection, at=AT))
        evidence = profile_fixture.fixture_evidence()
        evidence["active"] = previous["active"]
        evidence["manifest"] = self.base.transport["manifest"]
        evidence["install"] = {"formal_database": str(self.db.resolve()),
            "installed": {"device": self.db.stat().st_dev, "inode": self.db.stat().st_ino}}
        inherited = evidence["profile_operation_authority"]
        inherited["authorization_payload"].update(issued_at=AT, formal_database={
            "path": str(self.db.resolve()), "device": self.db.stat().st_dev, "inode": self.db.stat().st_ino})
        inherited.update(issued_at=AT, transport_manifest=evidence["manifest"])
        profile_fixture.reseal(evidence)
        parent = self.file_reference("parent-build.json", {"contract": "offline-parent-build"})
        source = self.file_reference("source-tree.json", {"contract": "offline-source-tree"})
        loaded = self.file_reference("loaded-build.json", {"contract": "offline-loaded-build"})
        envelope = json.loads(self.original_work["envelope_json"])
        payload = {"contract": "account-profile-compensation-user-authorization-v1",
            "production_rollout": "approved_by_user", "business_e2e": "required",
            "transport_qualification": "not_verified", "actor": "isolated-test",
            "reason": "explicit bounded retry of the original complete failed profile response",
            "user_instruction": "offline explicit recovery fixture", "source_thread_id": "offline-test",
            "issued_at": AT, "expires_at": EXPIRES,
            "original_cohort_sha256": "dad90e9d01d834a8081e47de819c847fcb6d6f272d73cf7a5a664bb9ceaf5d9f",
            "original_cohort": {"path": str(self.root / "original-cohort.json"),
                "sha256": "dad90e9d01d834a8081e47de819c847fcb6d6f272d73cf7a5a664bb9ceaf5d9f"},
            "original_work_ids": [self.work_id, *range(100000, 100110)],
            "max_starts": 1, "max_total_microusd": 1000, "max_amount_microusd": 1000,
            "targets": [{"work_id": self.work_id, "source_plan_id": envelope["catalog_plan_id"],
                "identity_id": self.iid, "usage_id": self.original_usage["id"],
                "raw_response_id": self.original_raw["id"] if self.original_raw else 999999}],
            "parent_build": parent, "source_tree": source,
            "catalog_policy_sha256": evidence["catalog_capture_policy_sha256"]}
        payload.update(changes or {})
        statement = self.file_reference("recovery-authorization.json", payload)
        proof = {"contract": "account-profile-compensation-authority-v1", "authorization": statement,
            "authorization_payload": payload, "loaded_build": loaded, "parent_build": parent,
            "source_tree": source, "catalog_policy_sha256": payload["catalog_policy_sha256"],
            "profile_authority_proof_sha256": inherited["proof_sha256"]}
        proof["proof_sha256"] = auth.digest(proof)
        evidence.update(profile_compensation_authority=proof)
        self.statement, self.installed_evidence = statement, evidence
        self.enterContext(patch.object(release, "_installed_evidence", return_value=evidence))

    def enqueue_recovery(self, *, at=AT):
        from v8.account_profile_recovery import enqueue_profile_compensation
        with connect(self.db) as connection, transaction(connection):
            return enqueue_profile_compensation(connection, work_id=self.work_id, at=at)

    def recovered_worker(self, *, at=RETRY_AT, compensation=True):
        self.at = at
        with ExitStack() as stack:
            for name in ("capture", "providers", "provider_budget", "account_metrics", "capture_runtime", "durable_runs"):
                stack.enter_context(patch("v8." + name + ".now_utc", return_value=at))
            return (capture_compensation.run_authorized_work(self.db, self.work_id, at)
                if compensation else runtime._run_single(self.db, at))

    def assert_original_history_retained(self):
        with connect(self.db) as connection:
            current = dict(connection.execute("SELECT * FROM provider_usage WHERE id=?", (self.original_usage["id"],)).fetchone())
            self.assertEqual(current, self.original_usage)
            settlement = connection.execute("SELECT id FROM provider_usage_settlements WHERE provider_usage_id=?", (current["id"],)).fetchone()
            self.assertIsNotNone(settlement)
            settled = usage_settlements.read_settlement(connection, settlement[0])
            self.assertEqual(settled["amount_microunits"], round(current["amount"] * 1_000_000))
            self.assertEqual(settled["state"], "charged_unverified")

    def test_zero_billed_original_recovers_once_with_real_grants_and_caps(self):
        self.create_failure("unbilled")
        self.install_recovery_authority()
        queued = self.enqueue_recovery()
        self.assertEqual(queued["sequence"], 1)
        self.assertEqual(len(self.base.calls), 1)
        result = self.recovered_worker()
        self.assertEqual(result["status"], "terminal", result)
        self.assertEqual(len(self.base.calls), 2)
        self.assert_original_history_retained()
        with connect(self.db) as connection:
            work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (self.work_id,)).fetchone())
            self.assertEqual(work["work_identity"], self.original_work["work_identity"])
            self.assertEqual(work["source_plan_id"], self.original_work["source_plan_id"])
            self.assertEqual(json.loads(work["envelope_json"])["logical_due"], json.loads(self.original_work["envelope_json"])["logical_due"])
            self.assertEqual(connection.execute("SELECT count(*) FROM authorization_issuance_consumptions").fetchone()[0], 2)
            usages = [dict(row) for row in connection.execute("SELECT * FROM provider_usage WHERE operation=? ORDER BY id", (OPERATION,))]
            self.assertEqual([item["amount"] for item in usages], [0, .001])
            self.assertEqual([json.loads(item["details_json"])["paid_sequence"] for item in usages], [0, 1])
            self.assertEqual(json.loads(usages[0]["details_json"])["paid_identity"], json.loads(usages[1]["details_json"])["paid_identity"])
            observation = connection.execute("SELECT payload_json FROM account_metric_observations WHERE account_identity_id=?", (self.iid,)).fetchone()
            field = json.loads(observation[0])["fields"]["follower_count"]
            self.assertEqual((field["status"], field["value"]), ("provided", 0))
        self.assertEqual(self.recovered_worker()["status"], "idle")
        self.assertEqual(len(self.base.calls), 2)
        repeated = self.enqueue_recovery(at=RETRY_AT)
        self.assertTrue(repeated["idempotent"])
        self.assertEqual(repeated["sequence"], 1)
        self.assertEqual(self.recovered_worker()["status"], "idle")
        self.assertEqual(len(self.base.calls), 2)

    def test_known_billed_original_keeps_charge_and_new_request_is_1000_microusd(self):
        self.create_failure("billed")
        self.install_recovery_authority()
        self.enqueue_recovery()
        result = self.recovered_worker()
        self.assertEqual(result["status"], "terminal", result)
        self.assert_original_history_retained()
        with connect(self.db) as connection:
            usages = connection.execute("SELECT amount,billed_requests FROM provider_usage WHERE operation=? ORDER BY id", (OPERATION,)).fetchall()
            self.assertEqual([tuple(row) for row in usages], [(.001, 1), (.001, 1)])

    def test_unknown_billing_without_full_raw_cannot_be_authorized(self):
        self.create_failure("unknown")
        self.install_recovery_authority()
        with self.assertRaises((ValueError, RuntimeError)):
            self.enqueue_recovery()
        self.assertEqual(len(self.base.calls), 1)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM compensation_authorization_issuances").fetchone()[0], 0)

    def test_expanded_caps_wrong_target_and_expired_statement_refuse_before_grants(self):
        self.create_failure()
        self.install_recovery_authority()
        target = copy.deepcopy(self.installed_evidence["profile_compensation_authority"]["authorization_payload"]["targets"])
        target[0]["identity_id"] += 9999
        for changes in ({"max_amount_microusd": 2000}, {"max_total_microusd": 2000},
                {"max_starts": 2}, {"targets": target}, {"expires_at": AT}):
            with self.subTest(changes=changes):
                self.install_recovery_authority(changes=changes)
                with self.assertRaises((ValueError, RuntimeError)):
                    self.enqueue_recovery()
                with connect(self.db) as connection:
                    self.assertEqual(connection.execute("SELECT count(*) FROM compensation_authorization_issuances").fetchone()[0], 0)
        self.assertEqual(len(self.base.calls), 1)

    def test_enqueued_statement_expiry_and_route_drift_are_rechecked_before_http(self):
        self.create_failure()
        self.install_recovery_authority()
        self.enqueue_recovery()
        result = self.recovered_worker(at=EXPIRES)
        self.assertEqual(result["status"], "paid_identity_hold", result)
        self.assertEqual(len(self.base.calls), 1)
        # Reread the same still-unused proof at a valid clock, with a changed
        # installed transport, to ensure it cannot borrow the old route proof.
        with connect(self.db) as connection, transaction(connection):
            self.installed_evidence["manifest"] = {**self.installed_evidence["manifest"], "route_generation": "changed"}
            with self.assertRaises((ValueError, RuntimeError)):
                capture_compensation._validate_proof(connection, self.work_id, at=RETRY_AT)
        self.assertEqual(len(self.base.calls), 1)

    def test_successful_original_does_not_hold_next_natural_profile_cycle(self):
        self.create_failure()
        self.install_recovery_authority()
        self.enqueue_recovery()
        self.assertEqual(self.recovered_worker()["status"], "terminal")
        later = "2026-09-01T22:01:00Z"
        with connect(self.db) as connection, transaction(connection):
            self.assertTrue(runtime._enqueue(connection, self.base.plan, self.base.plan["cohort"][0],
                stage="account_metrics", operation=OPERATION,
                logical_due="account-metrics:" + runtime._bucket(later, 6 * 3600), at=later))
        result = self.recovered_worker(at=later, compensation=False)
        self.assertEqual(result["status"], "terminal", result)
        self.assertEqual(len(self.base.calls), 3)
        with connect(self.db) as connection:
            rows = connection.execute("SELECT details_json FROM provider_usage WHERE operation=? ORDER BY id", (OPERATION,)).fetchall()
            details = [json.loads(row[0]) for row in rows]
            self.assertEqual([item["paid_sequence"] for item in details], [0, 1, 0])
            self.assertNotEqual(details[0]["paid_scope_identity"], details[2]["paid_scope_identity"])

    def test_real_writer_command_parsing_queue_dispatch_requires_no_new_hold(self):
        self.create_failure()
        self.install_recovery_authority()
        parameters = {"action": "profile_compensate", "work_id": self.work_id}
        self.assertEqual(capture_release_commands.validate_parameters(parameters), parameters)
        for invalid in ({**parameters, "max_amount_microusd": 9999}, {**parameters, "work_id": True}):
            with self.assertRaises(profile_control.ProfileControlError):
                capture_release_commands.validate_parameters(invalid)
        command = profile_control.enqueue_current_activation_hold_command(db_path=self.db,
            command_id="offline-profile-recovery", command="capture_release",
            parameters=parameters, submitted_at=AT)
        self.assertEqual(command["status"], "queued")
        with patch.object(profile_control, "now_utc", return_value=AT), \
                patch.object(capture_release_commands, "now_utc", return_value=AT):
            processed = profile_control.process_current_activation_hold_commands(db_path=self.db, limit=1)
        self.assertEqual(processed["processed"], [{"run_id": command["run_id"], "status": "succeeded"}], processed)
        self.assertEqual(len(self.base.calls), 1)
        self.assertEqual(self.recovered_worker()["status"], "terminal")
        self.assertEqual(len(self.base.calls), 2)

    def test_four_physical_retries_fit_4000_cap_despite_eight_scope_grants(self):
        from v8.account_operating_receipts import record_status_receipt
        from v8.operations import upsert_account
        self.create_failure()
        targets = [{"work_id": self.work_id, "source_plan_id": self.original_work["source_plan_id"],
            "identity_id": self.iid, "usage_id": self.original_usage["id"], "raw_response_id": self.original_raw["id"]}]
        new_identities = []
        for number in range(3):
            uid = str(223456780 + number)
            locator = "MS4wLjAB" + chr(ord("B") + number) * 64
            self.locators[uid] = locator
            account = upsert_account({"phone": "", "platforms": [
                {"platform": "douyin", "uid": uid, "nickname": "offline aggregate fixture"}]}, db_path=self.db)
            with connect(self.db) as connection, transaction(connection):
                iid = connection.execute("SELECT id FROM account_platform_identities WHERE account_id=?", (account["id"],)).fetchone()[0]
                new_identities.append(iid)
                connection.execute("INSERT INTO account_provider_references(account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) VALUES(?,'TikHub','sec_user_id',?,?,?)",
                    (iid, locator, AT, AT))
                connection.execute("INSERT INTO account_directory_rows(source_sha256,source_name,source_sheet,source_row,account_id,platform,uid,account_status,identity_status,raw_json,imported_at,updated_at) VALUES(?,'fixture','fixture',? ,?,'douyin',?,'daily','existing_verified','{}',?,?)",
                    ("a" * 64, number + 10, account["id"], uid, AT, AT))
                record_status_receipt(connection, request_id="aggregate-profile-" + str(number), account_id=account["id"],
                    account_identity_id=iid, requested_status="daily", update_frequency="daily",
                    request={"account_status": "daily", "fields": {}, "admission": {"member": {
                        "platform": "douyin", "uid": uid, "metadata": {"sec_user_id": locator}}}},
                    actor="fixture", reason="verified profile admission", before={"enabled": False, "update_frequency": None},
                    after={"enabled": True, "update_frequency": "daily"}, result={"id": account["id"],
                        "status_request_id": "aggregate-profile-" + str(number), "account_status": "daily",
                        "enabled": True, "update_frequency": "daily"}, timestamp=AT)
        plan = self.base.fixture.plan()
        with connect(self.db) as connection, transaction(connection):
            for member in plan["cohort"]:
                if member["identity_id"] in new_identities:
                    self.assertTrue(runtime._enqueue(connection, plan, member, stage="account_metrics", operation=OPERATION,
                        logical_due="account-metrics:" + runtime._bucket(AT, 6 * 3600), at=AT))
        self.status = 400
        self.base.response = {"detail": {"code": 400, "message": "Please retry. This request will not be charged."}}
        for _ in new_identities:
            outcome = self.base.run_profile()
            self.assertEqual(outcome["status"], "paid_identity_hold", outcome)
            with connect(self.db) as connection:
                work = dict(connection.execute("SELECT * FROM capture_work_items WHERE id=?", (outcome["work_id"],)).fetchone())
                usage = connection.execute("SELECT id FROM provider_usage WHERE operation=? ORDER BY id DESC LIMIT 1", (OPERATION,)).fetchone()
                raw = connection.execute("SELECT id FROM provider_raw_responses WHERE operation=? ORDER BY id DESC LIMIT 1", (OPERATION,)).fetchone()
                targets.append({"work_id": work["id"], "source_plan_id": work["source_plan_id"],
                    "identity_id": json.loads(work["envelope_json"])["identity_id"], "usage_id": usage[0], "raw_response_id": raw[0]})
        self.status, self.echo_uid = 200, True
        self.base.response = copy.deepcopy(self.success_response)
        self.install_recovery_authority(changes={"targets": targets, "max_starts": 4, "max_total_microusd": 4000,
            "original_work_ids": [*[target["work_id"] for target in targets], *range(100000, 100107)]})
        for target in targets:
            self.work_id = target["work_id"]
            self.assertEqual(self.enqueue_recovery()["sequence"], 1)
            self.assertEqual(self.recovered_worker()["status"], "terminal")
        self.assertEqual(len(self.base.calls), 8)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM authorization_issuance_consumptions").fetchone()[0], 8)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage WHERE operation=? AND json_extract(details_json,'$.paid_sequence')=1", (OPERATION,)).fetchone()[0], 4)
            self.assertEqual(connection.execute("SELECT sum(amount) FROM provider_usage WHERE operation=?", (OPERATION,)).fetchone()[0], .004)
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_work_items WHERE operation=? AND state='terminal'", (OPERATION,)).fetchone()[0], 4)

    def test_original_failure_preserves_real_complete_raw_and_original_charge(self):
        self.create_failure("unbilled")
        self.assertEqual((self.original_usage["billed_requests"], self.original_usage["amount"]), (0, 0))
        self.assertIsNotNone(self.original_raw)
        self.assertTrue(self.details["transport"]["clean_eof"])
        self.assertTrue(self.details["transport"]["json_parse_ok"])


if __name__ == "__main__":
    unittest.main()
