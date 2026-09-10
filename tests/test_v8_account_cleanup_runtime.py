"""Temporary installed generation: real receipts, roster, drain and operator gates.

No formal database or provider calls. Only installed plist discovery, transport
configuration and filesystem capacity are supplied by the local fixture.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from v8 import account_cleanup_runtime as cleanup, account_roster, capture_authorizations as auth
from v8 import capture_operator_release as operator, capture_release as release, forward_recovery, paid_drain
from v8 import provider_budget, runtime_database, storage
from v8.profile_activations import append_activation
from v8.system_roster import seal_system_members

AT = "2026-09-07T12:00:00Z"
LATER = "2026-09-08T12:00:00Z"
SEC = "MS4wLjAB" + "x" * 40


class CleanupRuntimeTest(unittest.TestCase):
    def write(self, name, value):
        path = self.root / name
        body = auth.canonical(value).encode()
        path.write_bytes(body)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(body).hexdigest()}

    def envelope(self, name, contract, payload):
        return self.write(name, {"contract_version": contract, "payload": payload,
                                 "payload_sha256": auth.digest(payload)})

    def capsule(self):
        original = {"contract_version": "v25-user-release-decision-v1", "production_rollout": "approved_by_user",
                    "business_e2e": "deferred_by_user", "transport_qualification": "not_verified",
                    "approved_target_profile": "integrated_route_v1", "operations": sorted(cleanup.OPERATIONS),
                    "transport_manifest": self.manifest}
        decision = self.write("original-decision.json", original)
        source_active = {"activation_id": 99, "profile_id": "integrated_route_v1", "roster_snapshot_id": 98,
                         "roster_members_sha256": "9" * 64, "activation_sha256": "8" * 64}
        bindings = {k: source_active[k] for k in cleanup.ACTIVE_KEYS if k != "activation_sha256"}
        bindings.update(build_receipt_sha256="b" * 64, runtime_root_receipt_sha256="c" * 64,
                        config_receipt_sha256="d" * 64, continuity_permit_sha256="e" * 64)
        operations = {}
        for index, operation in enumerate(sorted(cleanup.OPERATIONS), 1):
            common = {"bindings": bindings, "scope_hash": auth.scope_hash(runtime_bindings=bindings, provider="tikhub", operation=operation),
                      "release_decision_sha256": decision["sha256"], "release_event_id": 77,
                      "transport_manifest_sha256": auth.digest(self.manifest), "business_e2e": "deferred_by_user",
                      "transport_qualification": "not_verified"}
            ready = {"provider": "tikhub", "operation": operation, "status": "ready", "reason": "source operator",
                     "created_at": "2026-09-07T10:00:00Z", "expires_at": "2026-09-08T10:00:00Z",
                     "evidence_json": auth.canonical({**common, "contract": auth.READINESS_CONTRACT,
                         "qualification": "operator_authorized"})}
            ready.update(id=index, receipt_sha256=auth.digest(ready))
            bucket = "discovery" if operation in provider_budget.DISCOVERY_OPERATIONS else "metrics"
            payload = {**common, "contract": auth.CONTRACT, "operation": operation,
                       "issued_at": ready["created_at"], "expires_at": ready["expires_at"],
                       "readiness_receipt_id": index, "readiness_receipt_sha256": ready["receipt_sha256"],
                       "budget": {"total_microusd": provider_budget.AUTOMATIC_MICROUSD, "bucket": bucket,
                                  "bucket_microusd": provider_budget.BUDGET_BUCKET_MICROUSD[bucket]}}
            gate = {"provider": "tikhub", "operation": operation, "state": "open", "reason": "source operator",
                    "recorded_at": ready["created_at"], "evidence_json": auth.canonical(payload)}
            gate.update(id=index, event_sha256=auth.digest(gate))
            operations[operation] = {"gate": gate, "readiness": ready}
        value = {"contract": cleanup.SOURCE, "source_database_sha256": "f" * 64, "frozen_at": AT,
                 "source_active": source_active, "source_roster_member_count": 1, "source_directory_sha256": "a" * 64,
                 "source_members": [{**self.member, "update_status": "日更", "sec_user_id_sha256": hashlib.sha256(SEC.encode()).hexdigest()}],
                 "eligible_members": [self.member], "selection_sha256": auth.digest([self.member]),
                 "operations": operations, "decision_receipt": decision, "source_release_event_id": 77,
                 "transport_manifest": self.manifest}
        value["snapshot_sha256"] = auth.digest(value)
        return value

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.project = self.root / "project"
        self.project.mkdir()
        self.source = self.root / "source"
        self.source.mkdir()
        self.db = self.root / "candidate.sqlite3"
        self.connection = sqlite3.connect(self.db)
        self.connection.row_factory = sqlite3.Row
        self.addCleanup(self.connection.close)
        self.connection.execute("PRAGMA recursive_triggers=ON")
        self.connection.execute("PRAGMA foreign_keys=ON")
        storage.initialize_database(self.connection, target_version=20)
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute("BEGIN IMMEDIATE")
        roster = seal_system_members(self.connection, [{"platform": "douyin", "uid": "123456", "nickname": "fixture",
            "sec_user_id": SEC, "profile_ref": "https://www.douyin.com/user/" + SEC,
            "monitoring_status": "monitored", "authorization_status": "authorized", "metadata": {}}],
            raw_root=self.root / "raw", actor="fixture", reason="temporary approved roster", sealed_at=AT)
        snapshot = self.connection.execute("SELECT * FROM account_roster_snapshots WHERE id=?", (roster["snapshot_id"],)).fetchone()
        row = account_roster.get_current_members(self.connection, roster["snapshot_id"])[0]
        self.member = {"account_identity_id": row["account_identity_id"], "account_id": row["account_id"],
                       "platform": row["platform"], "uid": row["uid"]}
        self.manifest = {"api_base": "https://api.tikhub.io", "http_stack": "urllib-stream-v1"}
        self.authority = self.capsule()
        authority_ref = self.write("authority.json", self.authority)
        migration = {"contract": "account-cleanup-projection-v1", "status": "candidate_verified",
                     "verification": {"foreign_key_check": "ok", "integrity_check": "ok", "projected_values_sha256_verified": True},
                     "source_backup": {"sha256": "f" * 64}, "source_attachment_sha256": "a" * 64}
        migration["receipt_sha256"] = auth.digest(migration)
        migration_ref = self.write("migration.json", migration)
        critical = {}
        for name in ("account_cleanup_runtime", "capture_release", "capture_operator_release", "provider_budget",
                     "paid_dispatch", "paid_drain", "capture_authorizations", "runtime_database", "runtime_paths"):
            relative = "src/dcar_eval/v8/" + name + ".py"
            path = self.source / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# sealed fixture source\n")
            critical[relative] = cleanup.file_sha(path)
        tree = self.write("tree.json", {"contract": "writer-source-tree-v1", "source_root": str(self.source),
                          "files": [{"path": p, "sha256": sha} for p, sha in critical.items()]})
        identity = self.db.stat()
        runtime = self.envelope("runtime.json", "runtime-root-binding-v1", {"project_root": str(self.project),
            "formal_database": {"path": str(self.db), "device": identity.st_dev, "inode": identity.st_ino}})
        self.generation = {"contract": cleanup.GENERATION, "generation_id": "cleanup-test", "source_database_sha256": "f" * 64,
            "migration_receipt": migration_ref, "source_authority": authority_ref, "source_tree": tree,
            "selection_sha256": self.authority["selection_sha256"], "config_sha256": "d" * 64, "transport_manifest": self.manifest,
            "actor": "fixture", "reason": "already approved source subset", "issued_at": AT}
        build = self.envelope("build.json", "sealed-build-receipt-v1", {"status": "succeeded",
            "schema_contract": {"code_schema": 20, "formal_schema": 20}, "runtime_root_receipt": runtime,
            "critical_files": critical, "account_cleanup_generation": self.generation})
        self.active = append_activation(self.connection, profile_id="integrated_route_v1", roster_snapshot_id=roster["snapshot_id"],
            roster_members_sha256=snapshot["members_sha256"], effective_at=AT, created_at=AT, build_receipt_sha256=build["sha256"],
            actor="fixture", reason="cleanup", metadata={"account_cleanup": {k: self.generation[k] for k in ("contract", "generation_id", "selection_sha256")}})
        preliminary = {"active": self.active, "manifest": self.manifest, "build_sha256": build["sha256"],
            "runtime_sha256": runtime["sha256"], "config_sha256": "d" * 64, "account_cleanup_generation": self.generation}
        control = cleanup.release_control(preliminary)
        binding = {"source_activation_id": self.active["activation_id"], "target_activation_id": self.active["activation_id"],
            "business_day": "2026-09-07", "planned_effective_at": AT, "build_receipt_sha256": build["sha256"],
            "runtime_root_receipt_sha256": runtime["sha256"]}
        paid_drain.start_profile_drain_in_transaction(self.connection, "cleanup-test", binding=binding, switch_kind="same_profile", now=AT, control=control)
        paid_drain.seal_profile_drain_in_transaction(self.connection, "cleanup-test", now=AT, control=control)
        paid_drain.release_profile_drain_in_transaction(self.connection, "cleanup-test", now=AT, control=control)
        self.install = {"contract": "account-cleanup-install-v1", "status": "installed", "formal_database": str(self.db),
            "installed": {"device": identity.st_dev, "inode": identity.st_ino, "sha256": "7" * 64}, "build_receipt": build,
            "installed_at": AT, "source_database_sha256": "f" * 64, "expected_scope": {
                "active": {k: self.active[k] for k in cleanup.ACTIVE_KEYS}, "migration_receipt_sha256": migration_ref["sha256"],
                "selection_sha256": self.authority["selection_sha256"]}}
        install_ref = self.write("install.json", self.install)
        self.connection.commit()
        env = {"DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT": install_ref["path"], "DCAR_LOADED_BUILD_RECEIPT": build["path"],
               "DCAR_WRITER_SOURCE_ROOT": str(self.source), "DCAR_PROJECT_ROOT": str(self.project)}
        lock = self.root / "writer.lock"
        lock.touch(mode=0o600)
        installed = runtime_database.InstalledWriterContract(self.project, self.root / "fixture.plist", self.project,
            self.root / "fixture.py", self.db, lock, {"EnvironmentVariables": env})
        access = runtime_database.ResolvedDatabaseAccess(runtime_database.DatabaseAccessMode.WRITER, self.db,
            runtime_database.FileIdentity.from_stat(identity), self.project, lock, installed)
        self.enterContext(runtime_database.acquire_writer_lock(access))
        self.enterContext(patch.dict(os.environ, {**env, "DCAR_LOADED_BUILD_ID": "sha256:" + build["sha256"]}))
        self.enterContext(patch.object(runtime_database, "load_installed_writer_contract", return_value=installed))
        for module in (cleanup, forward_recovery):
            self.enterContext(patch.object(module, "PROJECT_ROOT", self.project))
        self.enterContext(patch.object(forward_recovery, "_route", return_value=self.manifest))
        self.enterContext(patch.object(forward_recovery, "_capacity", return_value={"fixture": "sufficient"}))
        self.network = self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        self.addCleanup(lambda: self.assertEqual(self.network.call_count, 0))
        self.connection.execute("BEGIN IMMEDIATE")

    def maintenance(self, at=AT):
        return release.maintain_operation_qualifications(self.connection, at=at, mirror_root=self.root / "mirrors")

    def test_installed_first_operator_gate_and_renewal_without_provider_work(self):
        evidence = release._installed_evidence(self.connection, at=AT)
        self.assertIsNone(evidence["activation_successor"])
        first = self.maintenance()
        self.assertEqual({op for op, result in first["operations"].items() if result["status"] == "initialized"}, cleanup.OPERATIONS, first)
        self.assertEqual({result["status"] for result in self.maintenance()["operations"].values()}, {"fresh", "not_enabled"})
        renewed = self.maintenance(LATER)
        self.assertEqual({result["status"] for result in renewed["operations"].values()}, {"renewed", "not_enabled"}, renewed)
        for result in renewed["operations"].values():
            if result["status"] == "renewed":
                self.assertEqual(result["qualification"], "operator_authorized")
                self.assertEqual(result["expires_at"], "2026-09-09T12:00:00Z")
                self.assertEqual(result["provider_calls"], 0)
                self.assertFalse(result["coverage_complete"])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM capture_paid_send_gate_events").fetchone()[0], 8)
        for table in ("provider_usage", "provider_request_start_events", "fetch_attempts", "capture_work_items"):
            self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)

    def test_temporary_operation_fault_does_not_expire_operator_authority(self):
        from v8.provider_budget import fault_state, record_fault_state
        self.maintenance()
        operation = "douyin_video_detail"
        record_fault_state(self.connection, scope_kind="operation", operation=operation,
            fault_class="transport", reason="transport_ratio_immediate", usage_id=None, at=AT)
        result = self.maintenance(LATER)["operations"][operation]
        self.assertEqual(result["status"], "renewed", result)
        self.assertEqual(result["provider_calls"], 0)
        self.assertTrue(fault_state(self.connection, scope_kind="operation", operation=operation)["open"])
        for table in ("provider_usage", "provider_request_start_events", "fetch_attempts"):
            self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM " + table).fetchone()[0], 0)

    def test_field_contract_fault_still_blocks_authority_renewal(self):
        from v8.provider_budget import record_fault_state
        self.maintenance()
        operation = "douyin_video_detail"
        record_fault_state(self.connection, scope_kind="operation", operation=operation,
            fault_class="field_contract", reason="field_contract_invalid", usage_id=None, at=AT)
        before = self.connection.execute("SELECT count(*) FROM capture_paid_send_gate_events WHERE operation=?", (operation,)).fetchone()[0]
        result = self.maintenance(LATER)["operations"][operation]
        self.assertEqual(result["status"], "blocked", result)
        self.assertEqual(before, self.connection.execute("SELECT count(*) FROM capture_paid_send_gate_events WHERE operation=?", (operation,)).fetchone()[0])

    def test_explicit_closed_gate_is_never_initialized(self):
        operation = sorted(cleanup.OPERATIONS)[0]
        gate = {"provider": "tikhub", "operation": operation, "state": "closed", "reason": "explicit operator hold",
                "evidence_json": "{}", "recorded_at": AT}
        self.connection.execute("INSERT INTO capture_paid_send_gate_events(provider,operation,state,reason,evidence_json,recorded_at,event_sha256) VALUES(?,?,?,?,?,?,?)",
                                (*gate.values(), auth.digest(gate)))
        result = self.maintenance()
        self.assertEqual(result["operations"][operation]["status"], "not_enabled")
        with self.assertRaisesRegex(auth.AuthorizationError, "existing gate"):
            cleanup.bootstrap_operator(self.connection, evidence=release._installed_evidence(self.connection, at=AT), operation=operation, at=AT)

    def test_wrong_database_inode_or_code_change_is_blocked(self):
        self.install["installed"]["inode"] += 1
        self.write("install.json", self.install)
        self.assertEqual(self.maintenance()["status"], "blocked")
        self.install["installed"]["inode"] -= 1
        self.write("install.json", self.install)
        path = self.source / "src/dcar_eval/v8/paid_dispatch.py"
        path.write_text("# changed\n")
        self.assertEqual(self.maintenance()["status"], "blocked")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM capture_paid_send_gate_events").fetchone()[0], 0)

    def test_missing_installed_proof_or_changed_roster_cannot_bootstrap(self):
        Path(os.environ["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"]).unlink()
        self.assertEqual(self.maintenance()["status"], "blocked")
        self.write("install.json", self.install)
        self.connection.execute("UPDATE accounts SET enabled=0")
        self.assertEqual(self.maintenance()["status"], "blocked")

    def test_source_snapshot_accepts_later_renewal_but_rejects_tampered_pair(self):
        cleanup.validate_source(self.authority, at=LATER)
        invalid = copy.deepcopy(self.authority)
        pair = next(iter(invalid["operations"].values()))
        pair["gate"]["state"] = "closed"
        pair["gate"]["event_sha256"] = auth.digest({k: pair["gate"][k] for k in cleanup.GATE_KEYS})
        invalid["snapshot_sha256"] = auth.digest({k: v for k, v in invalid.items() if k != "snapshot_sha256"})
        with self.assertRaises(auth.AuthorizationError):
            cleanup.validate_source(invalid, at=LATER)

    def test_source_scope_and_selection_cannot_expand(self):
        invalid = copy.deepcopy(self.authority)
        invalid["eligible_members"].append({**self.member, "account_identity_id": 999})
        invalid["selection_sha256"] = auth.digest(invalid["eligible_members"])
        invalid["snapshot_sha256"] = auth.digest({k: v for k, v in invalid.items() if k != "snapshot_sha256"})
        with self.assertRaises(auth.AuthorizationError):
            cleanup.validate_source(invalid, at=AT)
        evidence = release._installed_evidence(self.connection, at=AT)
        self.assertIsNone(operator._decision(evidence, "xiaohongshu_note_detail", AT))
        evidence["deployment"]["release_decision"]["qualification"] = "passed"
        with self.assertRaises(auth.AuthorizationError):
            operator._decision(evidence, "douyin_user_posts", AT)

    def test_drain_or_provider_circuit_still_blocks_initialization(self):
        with patch.object(paid_drain, "dispatch_state", return_value=paid_drain.DrainState("closed")):
            self.assertEqual(self.maintenance()["status"], "blocked")
        with patch.object(provider_budget, "circuit_state", return_value={"open": True}):
            self.assertEqual(self.maintenance()["status"], "blocked")
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM capture_paid_send_gate_events").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
