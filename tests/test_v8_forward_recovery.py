from __future__ import annotations

import hashlib
import json
import os
import plistlib
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore[import-untyped]

from tests import test_v8_transport_hold_binding as hold_fixture
from tests import test_seal_r0_receipts as seal_fixture
from tests.test_v8_runtime_database import InstalledRuntimeFixture
from v8 import forward_recovery as recovery, paid_drain, profile_control, runtime_receipts
from v8.profile_activations import activation_at
from v8.runtime_database import acquire_writer_lock, load_installed_writer_contract, resolve_installed_database_access
from v8.storage import connect, transaction
from v8.transport_receipts import append_transport_receipt

AT = hold_fixture.READ_AT


class ForwardRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.fixture = hold_fixture.TransportHoldBindingTest(methodName="runTest")
        self.addCleanup(self.fixture.doCleanups)
        original_begin = profile_control.begin_current_activation_hold

        def begin_with_legacy_history(**arguments):
            with connect(arguments["db_path"]) as connection, transaction(connection):
                account = connection.execute(
                    "INSERT INTO accounts(phone,created_at,updated_at) VALUES ('','2026-08-04T00:00:00Z','2026-08-04T00:00:00Z')",
                ).lastrowid
                self.legacy_slot = connection.execute(
                    "INSERT INTO fetch_slots(account_id,stage,window_key,provider,adapter_version,status,last_error_code,created_at,updated_at) "
                    "VALUES (?,'discovery','legacy','TikHub','legacy','retryable_failed','billing_unknown_retry_blocked',?,?)",
                    (account, "2026-08-04T00:00:00Z", "2026-08-04T00:00:00Z"),
                ).lastrowid
                connection.execute(
                    "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at) VALUES (?,1,'2026-08-04T00:00:00Z')",
                    (self.legacy_slot,),
                )
                connection.execute(
                    "INSERT INTO provider_usage(provider,operation,request_attempts,currency,amount,recorded_at,details_json) "
                    "VALUES ('TikHub','douyin_user_posts',1,'USD',0.001,'2026-08-04T00:00:00Z',?)",
                    (json.dumps({"state": "reserved"}),),
                )
                # A never-claimed historical inventory row is not a live owner.
                connection.execute(
                    "INSERT INTO scheduler_runs(job_id,scheduled_for,started_at,status,details_json) "
                    "VALUES ('tikhub_reconcile','legacy-unused','2026-08-04T00:00:00Z','running','{}')",
                )
                self.legacy_rows = {
                    table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")]
                    for table in ("provider_usage", "fetch_slots", "fetch_attempts")
                }
            return original_begin(**arguments)

        with mock.patch.object(profile_control, "begin_current_activation_hold", side_effect=begin_with_legacy_history):
            self.fixture.setUp()
        self.db, self.root = self.fixture.db, self.fixture.root
        self.mirror = self.root / "forward-mirror"
        self.raw = self.root / "raw"
        self.raw.mkdir()
        self.route = {"api_base": "https://api.tikhub.io", "http_stack": "urllib-stream-v1"}
        runtime = InstalledRuntimeFixture(self.root / "installed-runtime")
        runtime.database = self.db
        payload = plistlib.loads(runtime.plist.read_bytes())
        payload["EnvironmentVariables"]["DCAR_V8_DB"] = str(self.db)
        runtime.plist.write_bytes(plistlib.dumps(payload))
        installed = load_installed_writer_contract(home=runtime.home)
        self.access = resolve_installed_database_access("writer", database=self.db, project_root=runtime.project,
                                                        environ=runtime.environment, installed=installed)
        self.lock = self.enterContext(acquire_writer_lock(self.access))
        self.scheduler = BackgroundScheduler(timezone="UTC")
        self.scheduler.start(paused=True)
        self.addCleanup(self.scheduler.shutdown, wait=False)
        self.enterContext(mock.patch.dict(os.environ, {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-09-06"}))
        self.enterContext(mock.patch.object(recovery, "now_utc", return_value=AT))
        self.enterContext(mock.patch.object(profile_control, "now_utc", return_value=AT))
        self.identity = self.enterContext(mock.patch.object(recovery, "_runtime_identity", return_value={"verified_fixture": True}))
        self.enterContext(mock.patch.object(recovery, "_released_runtime_identity", side_effect=self.identity))
        self.enterContext(mock.patch.object(recovery, "_route", return_value=self.route))
        self.enterContext(mock.patch("v8.capture.RAW_ROOT", self.raw))
        self.disk = self.enterContext(mock.patch.object(recovery.shutil, "disk_usage", return_value=SimpleNamespace(
            total=100 * 1024**3, used=20 * 1024**3, free=80 * 1024**3)))
        evidence = self.fixture._evidence("config", hold_fixture.NEW_CONFIG)
        profile_control.record_current_activation_hold_prerequisite(
            db_path=self.db, drain_id=hold_fixture.HOLD_ID, kind="config", artifact_sha256=hold_fixture.NEW_CONFIG,
            receipt_contract_version=profile_control.CURRENT_HOLD_PREREQUISITE_CONTRACTS["config"],
            expires_at=hold_fixture.EXPIRES_AT, evidence={**evidence, "selected_route": self.route},
            actor="fixture", now="2026-09-06T05:10:00Z",
        )
        with connect(self.db) as connection, transaction(connection):
            campaign = append_transport_receipt(connection, kind="campaign", identity_key="forward-test-campaign",
                                                payload={"request_transport": {"manifest": self.route}}, at=AT, mirror_root=self.mirror)
            self.verdict = append_transport_receipt(connection, kind="route_verdict", identity_key="forward-test-control",
                                                     payload={"selected_route": self.route, "status": "passed", "route_passed": True,
                                                              "campaign_receipt_id": campaign["receipt_id"],
                                                              "campaign_receipt_sha256": campaign["self_sha256"]},
                                                     at=AT, mirror_root=self.mirror)
        self.control = self.enterContext(mock.patch.object(recovery, "_passed_control", return_value={
            "receipt_id": self.verdict["receipt_id"], "receipt_sha256": self.verdict["self_sha256"],
            "campaign_receipt_id": campaign["receipt_id"], "campaign_receipt_sha256": campaign["self_sha256"],
            "arm": "control_io", "selected_route": self.route, "source_verdict_status": "passed", "source_route_passed": True,
            "admission": "complete_transport", "retry_members": [],
        }))

    def enqueue(self, command_id="future-1", **changes):
        return profile_control.enqueue_current_activation_hold_command(
            db_path=self.db, command_id=command_id, command="forward_only_release", parameters={
                "drain_id": hold_fixture.HOLD_ID, "route_verdict_receipt_id": self.verdict["receipt_id"],
                "scope_start": "2026-09-06", "actor": "operator", **changes,
            },
        )

    def process(self):
        return profile_control.process_current_activation_hold_commands(db_path=self.db, mirror_root=self.mirror,
                                                                          scheduler=self.scheduler)

    def release(self):
        self.enqueue()
        result = self.process()
        self.assertEqual(result["processed"][0]["status"], "succeeded", result)
        with connect(self.db) as connection:
            return profile_control._hold_event(connection, drain_id=hold_fixture.HOLD_ID, event_type="release")

    def assert_still_held(self):
        with connect(self.db) as connection:
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "draining")
            self.assertIsNone(profile_control._hold_event(connection, drain_id=hold_fixture.HOLD_ID, event_type="sealed"))

    def test_writer_queue_midday_release_preserves_activation_and_does_not_claim_qualification(self):
        release = self.release()
        control = release["payload"]["control"]
        self.assertEqual(control["control_purpose"], "forward_only_release")
        self.assertFalse(control["operation_qualified"])
        self.assertFalse(control["historical_backfill_authorized"])
        self.assertEqual(set(control["hold_binding"]["prerequisites"]), {"build", "runtime", "config", "price", "budget"})
        with connect(self.db) as connection, transaction(connection):
            state = paid_drain.require_paid_dispatch_open(connection, provider="TikHub", operation="douyin_user_posts", at=AT)
            self.assertEqual(state.activation_id, self.fixture.activation_id)
            self.assertEqual(state.permit_event_id, release["event_id"])
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM acquisition_profile_activations").fetchone()[0], 1)
        self.assertEqual(self.scheduler.state, 1)
        self.assertEqual(self.enqueue()["status"], "succeeded")
        self.assertEqual(self.process()["count"], 0)
        self.scheduler.pause()
        self.enqueue("future-idempotent")
        self.assertEqual(self.process()["processed"][0]["status"], "succeeded")

    def test_expiration_is_checked_at_release_but_does_not_stop_durable_release_later(self):
        self.release()
        with connect(self.db) as connection, transaction(connection):
            state = paid_drain.require_paid_dispatch_open(connection, provider="TikHub", operation="douyin_user_posts",
                                                          at="2026-09-12T03:00:00Z")
            self.assertTrue(state.paid_dispatch_open)

    def test_pre_policy_reservation_and_guarded_legacy_attempt_are_unchanged_after_release(self):
        self.release()
        with connect(self.db) as connection:
            for table, previous in self.legacy_rows.items():
                self.assertEqual([tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY id")], previous)
            self.assertEqual(connection.execute("SELECT last_error_code FROM fetch_slots WHERE id=?", (self.legacy_slot,)).fetchone()[0],
                             "billing_unknown_retry_blocked")

    def test_post_start_unclosed_attempt_is_rejected_by_exact_tail(self):
        with connect(self.db) as connection, transaction(connection):
            connection.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at) VALUES (?,2,?)",
                               (self.legacy_slot, AT))
        self.enqueue()
        result = self.process()["processed"][0]
        self.assertEqual(result["status"], "failed")
        self.assertIn("post-START fetch_attempt_ids", result["error"]["message"])
        self.assert_still_held()

    def test_post_start_live_paid_owner_is_rejected_by_exact_tail(self):
        from v8 import durable_runs

        claim = durable_runs.claim_run("tikhub_reconcile", {"fixture": "live-paid-owner"}, db_path=self.db, now=AT)
        self.assertIsNotNone(claim)
        self.enqueue()
        result = self.process()["processed"][0]
        self.assertEqual(result["status"], "failed")
        self.assertIn("post-START paid_run_ids", result["error"]["message"])
        self.assert_still_held()

    def test_expired_prerequisites_cannot_create_a_release(self):
        self.enqueue()
        with mock.patch.object(recovery, "now_utc", return_value="2026-09-12T03:00:00Z"):
            self.assertEqual(self.process()["processed"][0]["status"], "failed")
        self.assert_still_held()

    def test_failed_control_and_low_capacity_each_leave_no_sealed_event(self):
        self.enqueue()
        self.control.side_effect = profile_control.ProfileControlError("forward_control_not_passed", "fixture incomplete control")
        self.assertEqual(self.process()["processed"][0]["error"]["code"], "forward_control_not_passed")
        self.assert_still_held()
        self.control.side_effect = None
        self.disk.return_value = SimpleNamespace(total=100 * 1024**3, used=95 * 1024**3, free=5 * 1024**3)
        self.enqueue("future-2")
        self.assertEqual(self.process()["processed"][0]["error"]["code"], "forward_capacity_blocked")
        self.assert_still_held()

    def test_runtime_identity_and_route_drift_block_without_rewriting_release(self):
        self.release()
        self.identity.return_value = {"different_writer": True}
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(paid_drain.PaidDrainBlocked):
                paid_drain.require_paid_dispatch_open(connection, provider="TikHub", operation="douyin_user_posts", at=AT)
            self.assertEqual(paid_drain.dispatch_state(connection, at=AT).state, "open")

    def test_scope_cannot_be_backdated_or_set_to_tomorrow(self):
        for index, scope in enumerate(("2026-09-05", "2026-09-07")):
            self.enqueue(f"future-{index}", scope_start=scope)
            self.assertEqual(self.process()["processed"][0]["error"]["code"], "forward_scope_invalid")
        self.assert_still_held()

    def test_caller_cannot_override_clock_capacity_or_db(self):
        for key in ("now", "db_path", "free_bytes"):
            with self.assertRaises(profile_control.ProfileControlError):
                self.enqueue(**{key: "injected"})

    def test_paused_writer_is_required_even_for_an_existing_release_request(self):
        self.enqueue()
        self.scheduler.resume()
        self.assertEqual(self.process()["processed"][0]["error"]["code"], "forward_scheduler_active")
        self.assert_still_held()

    def test_resume_occurs_only_after_release_is_visible_to_another_connection(self):
        resume = self.scheduler.resume

        def observe_commit():
            with connect(self.db) as connection:
                released = profile_control._hold_event(connection, drain_id=hold_fixture.HOLD_ID, event_type="release")
                self.assertIsNotNone(released)
            resume()

        with mock.patch.object(self.scheduler, "resume", side_effect=observe_commit) as observed:
            self.release()
        observed.assert_called_once()

    def test_resume_failure_is_reported_and_durable_release_can_resume_on_retry(self):
        self.enqueue()
        with mock.patch.object(self.scheduler, "resume", side_effect=RuntimeError("fixture resume failure")):
            failed = self.process()["processed"][0]
        self.assertEqual(failed["error"]["code"], "forward_scheduler_resume_failed")
        self.assertEqual(self.scheduler.state, 2)
        self.enqueue("resume-retry")
        result = self.process()["processed"][0]
        self.assertEqual(result["status"], "succeeded", result)
        self.assertEqual(self.scheduler.state, 1)

    def test_existing_reconcile_job_is_due_at_current_time_after_commit(self):
        self.scheduler.add_job(lambda: None, "interval", hours=1, id="pipeline_reconcile")
        with mock.patch.object(self.scheduler, "modify_job", wraps=self.scheduler.modify_job) as modified:
            self.release()
        self.assertEqual(modified.call_args.args, ("pipeline_reconcile",))
        self.assertEqual(modified.call_args.kwargs["next_run_time"], recovery.parse_time(AT))

    def test_daily_binding_uses_durable_release_without_claiming_a_day_complete(self):
        released = self.release()
        with connect(self.db) as connection:
            past = runtime_receipts._day_release_binding(connection, activation_id=self.fixture.activation_id, business_day="2026-09-05")
            later = runtime_receipts._day_release_binding(connection, activation_id=self.fixture.activation_id, business_day="2026-09-10")
            readiness = runtime_receipts.current_activation_readiness(connection, at="2026-09-11T03:00:00Z")
        self.assertIsNone(past["release_event_id"])
        self.assertEqual(later["release_event_id"], released["event_id"])
        self.assertFalse(readiness["data_readiness"])

    def test_readonly_readiness_does_not_compare_snapshot_inode_to_writer_inode(self):
        released = self.release()
        self.identity.side_effect = AssertionError("read-only projection must not claim writer identity")
        with connect(self.db) as connection:
            active = activation_at(connection, AT)
            recovery.validate_forward_release(connection, active=active, release_control=released["payload"]["control"],
                                               at="2026-09-12T03:00:00Z", check_runtime=False)


class RetryAdmissionTest(unittest.TestCase):
    def setUp(self):
        self.base = ForwardRecoveryTest(methodName="runTest")
        self.addCleanup(self.base.doCleanups)
        self.base.setUp()
        self.members = [{"member_receipt_id": rank, "rank": rank, "effective_starts": 1,
                         "response_complete": True, "accounting_terminal": True, "state": "succeeded"}
                        for rank in range(1, 21)]
        self.owners = {"members": [{"materialization_run_id": rank} for rank in range(1, 21)]}
        self.payload = {"status": "failed", "route_passed": False, "usable_page_count": 19, "selected_route": None}

    def retry(self, message="Request failed. Please retry. You won't be charged for this request."):
        from v8.raw_evidence import write_zstd_raw_evidence

        body = recovery.canonical_json_bytes({"detail": {"code": 400, "message": message}})
        path = self.base.raw / "retry.json.zst"
        raw = write_zstd_raw_evidence(path, body, provider="TikHub", operation="douyin_user_posts",
                                      response_identity="a" * 64, paid_scope_identity="b" * 64, sequence=0,
                                      evidence_root=self.base.raw)
        with connect(self.base.db) as connection, transaction(connection):
            attempt = connection.execute(
                "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,response_finished_at,http_status,billed,amount,error_code) "
                "VALUES (?,2,?,?,400,0,0,'provider_retry_requested')", (self.base.legacy_slot, AT, AT),
            ).lastrowid
            usage = connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,billed_requests,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_user_posts',1,0,'USD',0,?,?)",
                (AT, json.dumps({"state": "failed", "error_code": "provider_retry_requested"})),
            ).lastrowid
            raw_id = connection.execute(
                "INSERT INTO provider_raw_responses(fetch_attempt_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) "
                "VALUES (?,'TikHub','douyin_user_posts',?,?,?,400,?)",
                (attempt, str(path), raw.stored_sha256, raw.stored_size, AT),
            ).lastrowid
        self.members[0].update(state="failed", billing_settled=True, amount_microusd=0,
                               raw_response_id=raw_id, usage_id=usage, fetch_attempt_id=attempt)
        self.owners["members"][0]["materialization_run_id"] = None

    def admission(self):
        with connect(self.base.db) as connection:
            return recovery._control_admission(connection, self.payload, self.owners, self.members)

    def test_one_complete_explicitly_unbilled_retry_is_a_distinct_admission(self):
        self.retry()
        result = self.admission()
        self.assertEqual(result["admission"], "complete_transport_with_one_unbilled_retry")
        self.assertEqual(result["retry_members"][0]["member_receipt_id"], 1)
        self.assertEqual(result["retry_members"][0]["amount_microusd"], 0)
        self.assertFalse(self.payload["route_passed"])
        self.assertEqual(self.payload["status"], "failed")

    def test_http_400_without_explicit_unbilled_statement_is_rejected(self):
        self.retry("Request failed. Please retry.")
        with self.assertRaises(profile_control.ProfileControlError):
            self.admission()

    def test_two_provider_retries_are_rejected(self):
        self.retry()
        self.members[1]["state"] = "failed"
        self.owners["members"][1]["materialization_run_id"] = None
        self.payload["usable_page_count"] = 18
        with self.assertRaises(profile_control.ProfileControlError):
            self.admission()

    def test_unknown_or_incomplete_response_is_rejected(self):
        self.retry()
        self.members[1].update(state="billing_unknown", response_complete=False)
        with self.assertRaises(profile_control.ProfileControlError):
            self.admission()

    def test_nonzero_accounting_is_rejected_even_when_response_says_unbilled(self):
        self.retry()
        with connect(self.base.db) as connection, transaction(connection):
            connection.execute("UPDATE provider_usage SET amount=0.001 WHERE id=?", (self.members[0]["usage_id"],))
        with self.assertRaises(profile_control.ProfileControlError):
            self.admission()


class RuntimeEvidenceTest(unittest.TestCase):
    def test_runtime_evidence_binds_actual_database_and_code_and_rejects_drift(self):
        import sqlite3
        import tempfile

        def seal(path, contract, payload):
            seal_fixture.receipts._write_exclusive(path, seal_fixture.receipts._envelope(contract, payload))
            return hashlib.sha256(path.read_bytes()).hexdigest()

        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory).resolve()
            code = project / "critical.py"
            code.write_text("version = 1\n")
            database = project / "formal.sqlite3"
            with sqlite3.connect(database) as connection:
                identity = database.stat()
                runtime = project / "runtime.json"
                runtime_sha = seal(runtime, "runtime-root-binding-v1", {
                    "project_root": str(project), "formal_database": {
                        "path": str(database), "device": identity.st_dev, "inode": identity.st_ino,
                    },
                })
                build = project / "build.json"
                build_sha = seal(build, "sealed-build-receipt-v1", {
                    "status": "succeeded", "runtime_root_receipt": {"path": str(runtime), "sha256": runtime_sha},
                    "critical_files": {"critical.py": hashlib.sha256(code.read_bytes()).hexdigest()},
                })
                with mock.patch.object(recovery, "PROJECT_ROOT", project), mock.patch.dict(os.environ, {
                    "DCAR_LOADED_BUILD_RECEIPT": str(build), "DCAR_LOADED_BUILD_ID": "sha256:" + build_sha,
                }):
                    binding = {"build_receipt_sha256": build_sha, "runtime_root_receipt_sha256": runtime_sha}
                    read = recovery._runtime_identity(connection, binding)
                    self.assertEqual(read["database_inode"], identity.st_ino)
                    code.write_text("version = 2\n")
                    with self.assertRaises(profile_control.ProfileControlError):
                        recovery._runtime_identity(connection, binding)

    def test_private_artifact_checks_content_hash_and_permissions(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "receipt.json"
            payload = {"status": "succeeded"}
            contract = "sealed-build-receipt-v1"
            seal_fixture.receipts._write_exclusive(path, seal_fixture.receipts._envelope(contract, payload))
            sha = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(recovery._private_receipt(path, sha, contract), payload)
            path.chmod(0o644)
            with self.assertRaises(profile_control.ProfileControlError):
                recovery._private_receipt(path, sha, contract)
            path.chmod(0o600)
            path.write_text("{}")
            with self.assertRaises(profile_control.ProfileControlError):
                recovery._private_receipt(path, sha, contract)

    def test_real_seal_build_and_runtime_files_are_accepted_by_forward_reader(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            fixture = seal_fixture.ReceiptFixture(Path(directory))
            with mock.patch.object(seal_fixture.receipts, "_database_holders", return_value=[]):
                result = seal_fixture.receipts.seal(
                    project_root=fixture.project, evidence_dir=fixture.evidence,
                    expected_head=fixture.head, test_results=fixture.test_result_specifications(), home=fixture.home,
                )
            for name, contract in (("sealed_build", "sealed-build-receipt-v1"), ("runtime_root", "runtime-root-binding-v1")):
                with self.subTest(contract=contract):
                    path = Path(result[name + "_receipt"])
                    expected = json.loads(path.read_bytes())["payload"]
                    self.assertEqual(recovery._private_receipt(path, result[name + "_receipt_sha256"], contract), expected)

    def test_sealed_receipt_rejects_body_contract_digest_and_file_hash_drift(self):
        import tempfile

        with tempfile.TemporaryDirectory() as directory:
            for contract in ("sealed-build-receipt-v1", "runtime-root-binding-v1"):
                for drift in ("payload", "contract", "file_hash", "newline_digest"):
                    with self.subTest(contract=contract, drift=drift):
                        path = Path(directory) / (contract + "-" + drift + ".json")
                        payload = {"status": "succeeded", "label": "正式封版", "nested": {"b": 2, "a": 1}}
                        envelope = seal_fixture.receipts._envelope(contract, payload)
                        if drift == "payload":
                            envelope["payload"]["status"] = "changed"
                        elif drift == "contract":
                            envelope["contract_version"] = "other-contract"
                        elif drift == "newline_digest":
                            newline_digest = recovery._digest(payload)
                            self.assertNotEqual(envelope["payload_sha256"], newline_digest)
                            envelope["payload_sha256"] = newline_digest
                        seal_fixture.receipts._write_exclusive(path, envelope)
                        sha = hashlib.sha256(path.read_bytes()).hexdigest()
                        if drift == "file_hash":
                            sha = "0" * 64
                        with self.assertRaises(profile_control.ProfileControlError) as raised:
                            recovery._private_receipt(path, sha, contract)
                        self.assertEqual(raised.exception.code, "forward_build_invalid")

    def test_successful_primary_diagnostic_is_not_a_control_release(self):
        from tests.test_v8_transport_runner import TransportRunnerTest, AT as runner_at
        from v8.transport_verdict import record_primary_route_verdict

        fixture = TransportRunnerTest(methodName="runTest")
        self.addCleanup(fixture.doCleanups)
        fixture.setUp()
        fixture._run()
        base = fixture.fixture.fixture
        with connect(fixture.db) as connection, transaction(connection):
            verdict = record_primary_route_verdict(connection, base.campaign["receipt_id"], at=runner_at, mirror_root=base.mirror_root)
            with self.assertRaises(profile_control.ProfileControlError) as raised:
                recovery._passed_control(connection, verdict["receipt_id"], base.campaign["payload"]["hold_binding"], at=runner_at)
        self.assertEqual(raised.exception.code, "forward_control_not_passed")


if __name__ == "__main__":
    unittest.main()
