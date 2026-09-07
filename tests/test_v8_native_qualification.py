"""Native qualification uses real legacy owners/A-B starts/raw, never source19 proof."""
from __future__ import annotations

import unittest
import json
import urllib.parse
from pathlib import Path
from datetime import timedelta
from unittest.mock import patch

from tests import test_v8_legacy_continuity as fixture
from v8 import capture_authorizations as auth, capture_release as release, capture_planning
from v8 import providers, tikhub_scan, transport_execution, durable_runs, capture, metric_observations, provider_budget
from v8.source_routing import parse_time
from v8.storage import connect, transaction

AT, OPERATION = fixture.AT, fixture.OPERATION


class NativeQualificationTest(unittest.TestCase):
    platform = "douyin"
    member_count = 2
    roster = fixture.LegacyContinuityTest.roster
    _natural_children = fixture.LegacyContinuityTest._natural_children
    _transport = fixture.LegacyContinuityTest._transport
    _scope = fixture.LegacyContinuityTest._scope
    _claim_queue = fixture.LegacyContinuityTest._claim_queue

    def setUp(self):
        fixture.LegacyContinuityTest.setUp(self)
        self.at = AT
        with connect(self.db) as connection, transaction(connection):
            # The production planner links the actual returned children before
            # their next invocation (pipeline._round_job). Preserve that link.
            durable_runs.checkpoint(connection, self.parent_claim,
                {"child_run_ids": sorted(self.children), "started": True}, now=AT)
        for module in (capture, providers, tikhub_scan, transport_execution, metric_observations):
            self.enterContext(patch.object(module, "now_utc", side_effect=lambda: self.at))

    def claim_round(self, identity, *, at=AT):
        self.parent_claim = fixture.LegacyContinuityTest.claim_round(self, identity, at=at)
        return self.parent_claim

    def _http(self, request, **kwargs):
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        account = int(params["sec_user_id"][0].split("r")[-1])
        cursor = int(params.get("max_cursor", ["0"])[0])
        self.calls.append((account, cursor))
        item = fixture.scan_fixture.dy_item(number=cursor*10+account,
            author={"uid": f"douyin-{account:02d}", "nickname": "fixture"},
            create_time=fixture.scan_fixture.epoch("2026-09-05T10:00:00Z"),
            statistics={"play_count": 321, "digg_count": 10, "comment_count": 2, "share_count": 1, "collect_count": 3})
        body = json.dumps(fixture.scan_fixture.dy_page([item], more=True, cursor=cursor+1)).encode()
        response = fixture.transport_fixture.FakeResponse(body, response_url=request.full_url,
            headers={"Content-Length": str(len(body))})
        return fixture.request_json(request, **kwargs, opener=fixture.transport_fixture.FakeOpener(response), clock=lambda: self.at, chunk_size=13)

    def freeze(self, *, at=None):
        with connect(self.db) as connection, transaction(connection):
            return release.freeze_operation_cohort(connection, operation=OPERATION, at=at or self.at, mirror_root=self.root / "receipts")

    def maintenance(self, *, at=None):
        with connect(self.db) as connection, transaction(connection):
            return release.maintain_operation_qualifications(connection, at=at or self.at, mirror_root=self.root / "receipts")

    def run_natural_pages(self, scopes, total):
        while len(self.calls) < total:
            self.at = release.utc((parse_time(self.at)+timedelta(seconds=301)).isoformat())
            before_calls = len(self.calls)
            with auth.runtime_authority(release.current_runtime_bindings):
                for scope in scopes:
                    if len(self.calls) >= total:
                        break
                    tikhub_scan._run(scope, db_path=self.db, max_pages=min(20, total-len(self.calls)),
                        raw_root=self.raw_root, now=self.at, call_override=None)
            self.assertGreater(len(self.calls), before_calls)

    def execute(self, rank):
        # Normal scheduler reclaim of the SAME partial scans; never new work,
        # fake accounts, replacement cursors, or a diagnostic owner.
        with connect(self.db) as connection, transaction(connection):
            child_ids = [child.scheduler_run_id for child in self.children.values()]
            rows = connection.execute(f"SELECT * FROM scheduler_runs WHERE id IN ({','.join('?' for _ in child_ids)}) AND status IN ('running','partial')", child_ids).fetchall()
            if not any(row["status"] == "running" for row in rows):
                for row in rows:
                    details = json.loads(row["details_json"])
                    claim = durable_runs.claim_run_in_transaction(connection, row["job_id"], details["identity"], now=self.at)
                    self.assertIsNotNone(claim)
                    self.children[claim.scheduler_run_id] = claim
        return transport_execution.execute_native_member_page(self.cohort["receipt_id"], rank,
            db_path=self.db, raw_root=self.raw_root, at=self.at)

    def test_native_200_without_source_qualification_renews_and_next_batch_cannot_reuse(self):
        with patch.object(release, "_qualification", side_effect=AssertionError("native must not need source19")):
            self.cohort = self.freeze()
            self.assertEqual(self.freeze(), self.cohort)
            with connect(self.db) as connection, transaction(connection):
                self.assertEqual(connection.execute("SELECT count(*) FROM transport_continuity_permits").fetchone()[0], 0)
                self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
                with self.assertRaisesRegex(auth.AuthorizationError, "before 200|Candidate cannot"):
                    release.publish_operation_gate(connection, operation=OPERATION, at=self.at)
            with patch.object(tikhub_scan, "_apply", side_effect=RuntimeError("post-C fixture crash")):
                with self.assertRaisesRegex(RuntimeError, "post-C fixture crash"):
                    self.execute(1)
            with patch.object(providers, "request_json_transport", side_effect=AssertionError("local replay bought HTTP")):
                replay = transport_execution.resume_native_member_local(self.cohort["receipt_id"], 1,
                    db_path=self.db, raw_root=self.raw_root, at=self.at)
            self.assertEqual(replay["provider_calls"], 0)
            # Hand the existing owners back exactly as the normal scheduler does.
            for child in self.children.values():
                with connect(self.db) as connection:
                    running = connection.execute("SELECT status FROM scheduler_runs WHERE id=?", (child.scheduler_run_id,)).fetchone()[0] == "running"
                if running:
                    tikhub_scan._partial(child, "fixture scheduler yield", db_path=self.db, now=self.at, pages=0)
            scopes = []
            with connect(self.db) as connection:
                for child in self.children.values():
                    row = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (child.scheduler_run_id,)).fetchone()
                    scopes.append(json.loads(row[0])["identity"])
            # Real normal scanner reclaim, pagination, A admission and yield.
            # Two accounts suffice: no 200 simultaneously running fake owners.
            self.run_natural_pages(scopes, 200)
            self.assertEqual(len(self.calls), 200)
            self.assertEqual({account for account, _ in self.calls}, {1, 2})
            self.assertGreaterEqual(max(cursor for _, cursor in self.calls), 99)
            with self.assertRaises(auth.AuthorizationError):
                transport_execution.execute_native_member_page(self.cohort["receipt_id"], 201,
                    db_path=self.db, raw_root=self.raw_root, at=self.at)
            with connect(self.db) as connection, transaction(connection):
                qualification = release.record_native_operation_qualification(connection, cohort_receipt_id=self.cohort["receipt_id"], at=self.at, mirror_root=self.root / "receipts")
                self.assertEqual(qualification["payload"]["sample_count"], 200)
                self.assertEqual(qualification["payload"]["complete_count"], 200)
                self.assertEqual(qualification["payload"]["schema_version"], 20)
                self.assertEqual(release.record_native_operation_qualification(connection, cohort_receipt_id=self.cohort["receipt_id"], at=self.at, mirror_root=self.root / "receipts"), qualification)
                with self.assertRaisesRegex(auth.AuthorizationError, "Candidate cannot"):
                    release.renew_operation_gate(connection, operation=OPERATION, at=self.at)
                self.evidence["deployment"]["status"] = "accepted"
                gate = release.renew_operation_gate(connection, operation=OPERATION, at=self.at)
                again = release.renew_operation_gate(connection, operation=OPERATION, at=self.at)
                self.assertEqual(gate, again)
                self.assertTrue(gate["ordinary_paid_authorized"])
                self.assertFalse(gate["coverage_complete"])
                self.assertEqual(gate["expires_at"], qualification["payload"]["expires_at"])
                snapshot = release.snapshot_operation_qualification(connection, OPERATION, self.at)
                self.assertEqual(snapshot["qualification_kind"], "native20")
                release.validate_frozen_operation_qualification(connection, snapshot, self.at)
                path = Path(connection.execute("SELECT hot_path FROM provider_raw_blobs LIMIT 1").fetchone()[0])
                original = path.read_bytes()
                try:
                    path.write_bytes(b"corrupt fixture")
                    with self.assertRaises(auth.AuthorizationError):
                        release.current_runtime_bindings(connection, OPERATION, self.at)
                finally:
                    path.write_bytes(original)
                with self.assertRaisesRegex(auth.AuthorizationError, "expired"):
                    release.current_runtime_bindings(connection, OPERATION, gate["expires_at"])
                before = connection.total_changes
                with self.assertRaisesRegex(auth.AuthorizationError, "expired"):
                    release.renew_operation_gate(connection, operation=OPERATION, at=gate["expires_at"])
                self.assertEqual(connection.total_changes, before)
            self.assertEqual(self.maintenance()["operations"][OPERATION]["status"], "fresh")
            next_cohort = self.freeze()
            self.assertNotEqual(next_cohort["receipt_id"], self.cohort["receipt_id"])
            self.assertGreaterEqual(next_cohort["payload"]["start_high_watermark"], qualification["payload"]["last_marker_id"])
            previous_cohort = self.cohort
            self.cohort = next_cohort
            self.at = release.utc((parse_time(self.at)+timedelta(seconds=301)).isoformat())
            self.execute(1)
            with connect(self.db) as connection:
                first = {p["proof"]["paid_scope_identity"] for p in release._native_members(connection, previous_cohort)}
                second = {p["proof"]["paid_scope_identity"] for p in release._native_members(connection, next_cohort)}
            self.assertFalse(first & second)
            with connect(self.db) as connection, transaction(connection):
                with self.assertRaisesRegex(auth.AuthorizationError, "fewer than 200"):
                    release.record_native_operation_qualification(connection, cohort_receipt_id=next_cohort["receipt_id"], at=self.at, mirror_root=self.root / "receipts")
                with self.assertRaisesRegex(auth.AuthorizationError, "superseded"):
                    release.record_native_operation_qualification(connection, cohort_receipt_id=previous_cohort["receipt_id"], at=self.at, mirror_root=self.root / "receipts")
            collecting = self.maintenance()
            self.assertEqual(collecting["operations"][OPERATION]["actual_starts"], 1)
            self.assertEqual(collecting["provider_calls"], 0)
            for child in self.children.values():
                with connect(self.db) as connection:
                    running = connection.execute("SELECT status FROM scheduler_runs WHERE id=?", (child.scheduler_run_id,)).fetchone()[0] == "running"
                    current = child if running else None
                if current is not None:
                    tikhub_scan._partial(current, "fixture scheduler yield", db_path=self.db, now=self.at, pages=0)
            self.run_natural_pages(scopes, 400)
            # No qualify/renew command: the existing maintenance entry completes
            # the actual next 200 and emits the fresh ordinary gate itself.
            automatic = self.maintenance()
            self.assertEqual(automatic["provider_calls"], 0)
            self.assertEqual(automatic["operations"][OPERATION]["status"], "renewed")
            renewed_expiry = automatic["operations"][OPERATION]["expires_at"]
            self.assertGreater(parse_time(renewed_expiry), parse_time(gate["expires_at"]))
            self.assertEqual(self.maintenance()["operations"][OPERATION]["status"], "fresh")
            with connect(self.db) as connection:
                before = connection.total_changes
                release.current_runtime_bindings(connection, OPERATION, self.at)
                self.assertEqual(connection.total_changes, before)
            # After downtime and expiry, maintenance starts another immutable
            # next-batch manifest without requiring ordinary permission first.
            self.at = renewed_expiry
            recovery = self.maintenance()
            self.assertEqual(recovery["operations"][OPERATION]["status"], "collecting")
            self.assertEqual(recovery["operations"][OPERATION]["actual_starts"], 0)
            self.assertEqual(len(self.calls), 400)
            self.assertNotEqual(recovery["operations"][OPERATION]["cohort_id"], next_cohort["receipt_id"])

    def test_maintenance_never_first_enables_candidate_or_unqualified_operations(self):
        with connect(self.db) as connection:
            before = connection.execute("SELECT count(*) FROM scheduler_runs").fetchone()[0]
        self.assertEqual(self.maintenance()["status"], "blocked")
        self.evidence["deployment"]["status"] = "accepted"
        self.assertTrue(all(value["status"] == "not_enabled" for value in self.maintenance()["operations"].values()))
        self.cohort = self.freeze()
        with connect(self.db) as connection:
            frozen_count = connection.execute("SELECT count(*) FROM scheduler_runs").fetchone()[0]
        result = self.maintenance()
        self.assertEqual(result["operations"][OPERATION]["status"], "blocked")
        self.assertIn("never been explicitly", result["operations"][OPERATION]["reason"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM scheduler_runs").fetchone()[0], frozen_count)
            self.assertGreater(frozen_count, before)
            self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
        self.assertEqual(self.calls, [])

    def test_maintenance_respects_real_provider_circuit_without_work_or_http(self):
        self.evidence["deployment"]["status"] = "accepted"
        with connect(self.db) as connection, transaction(connection):
            provider_budget.record_circuit(connection, reason="provider_balance_blocked", usage_id=None, at=self.at)
        with connect(self.db) as connection, transaction(connection):
            before = connection.total_changes
            result = release.maintain_operation_qualifications(connection, at=self.at, mirror_root=self.root / "receipts")
            self.assertEqual(result["status"], "blocked")
            self.assertIn("Provider circuit", result["reason"])
            self.assertEqual(connection.total_changes, before)
        self.assertEqual(self.calls, [])

    def test_route_change_and_expiry_reject_before_http(self):
        self.cohort = self.freeze()
        with connect(self.db) as connection, transaction(connection):
            release.native_member(connection, cohort_id=self.cohort["receipt_id"], rank=1, at=self.at)
        with connect(self.db) as connection, transaction(connection):
            current = capture_planning.resolve_route(connection, account_id=1, content_id=None, operation=OPERATION, at=self.at)
            capture_planning.assign_route(connection, scope_type="account", scope_key="1", account_id=1, operation=OPERATION,
                provider="tikhub", expected_generation=current["generation"], route="integrated", mode="active", effective_at=self.at, recorded_at=self.at)
        with self.assertRaisesRegex(auth.AuthorizationError, "legacy route"):
            self.execute(1)
        self.assertEqual(self.calls, [])


    def test_runtime_change_refuses_ordinary_but_can_freeze_fresh_diagnostic_manifest(self):
        self.cohort = self.freeze()
        self.evidence["build_sha256"] = "e" * 64
        with connect(self.db) as connection:
            with self.assertRaisesRegex(auth.AuthorizationError, "runtime or route changed"):
                release.current_runtime_bindings(connection, OPERATION, self.at)
        successor = self.freeze()
        self.assertNotEqual(successor["receipt_id"], self.cohort["receipt_id"])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
        self.assertEqual(self.calls, [])

    def test_unstarted_rank_is_never_skipped_or_replaced(self):
        self.cohort = self.freeze()
        with connect(self.db) as connection, transaction(connection):
            _, proof, _ = release.native_member(connection, cohort_id=self.cohort["receipt_id"], rank=1, at=self.at)
            before = release._native_members(connection, self.cohort)
            with self.assertRaisesRegex(auth.AuthorizationError, "cannot skip"):
                release.native_member(connection, cohort_id=self.cohort["receipt_id"], rank=3, at=self.at)
            child = self.children[proof["source_run_id"]]
            durable_runs.checkpoint(connection, child, {"cursor": 777}, now=self.at)
            with self.assertRaises(auth.AuthorizationError):
                release.native_member(connection, cohort_id=self.cohort["receipt_id"], rank=1, at=self.at)
            self.assertEqual(release._native_members(connection, self.cohort), before)
        self.assertEqual(self.calls, [])

    def test_expired_cohort_can_freeze_new_diagnostic_without_ordinary_or_due_200(self):
        self.cohort = self.freeze()
        self.at = self.cohort["payload"]["expires_at"]
        with connect(self.db) as connection:
            with self.assertRaisesRegex(auth.AuthorizationError, "expired"):
                release.current_runtime_bindings(connection, OPERATION, self.at)
        replacement = self.freeze()
        self.assertNotEqual(replacement["receipt_id"], self.cohort["receipt_id"])
        with connect(self.db) as connection:
            self.assertEqual(release._native_members(connection, replacement), [])
            self.assertEqual(connection.execute("SELECT state FROM capture_paid_send_gate_events ORDER BY id DESC LIMIT 1").fetchone()[0], "diagnostic_only")
        self.assertEqual(self.calls, [])
