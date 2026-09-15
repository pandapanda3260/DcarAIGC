"""Disposable schema23 inheritance and installation; no formal Writer mutations."""

from __future__ import annotations

from dataclasses import replace
import ast
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import plistlib
import shutil
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from tests import test_four_platform_flow_release as fixtures
from v8 import account_taxonomy_code_successor as release, runtime_database
from v8 import runtime_evidence_context as context

ROOT = Path(__file__).resolve().parents[1]
FROZEN_FLOW = Path(
    "/Users/mark/Library/Application Support/DcarAIGC/writer-sources/20260913-four-platform-flow-v5"
)
DISPATCH = """    if build.get("account_taxonomy_code_successor") is not None:
        from .account_taxonomy_code_successor import verify_inheritance as verify_taxonomy
        return verify_taxonomy(build=build, build_ref=build_ref, install_path=install_path,
            database=database, source=source, at=at, connection=connection)
"""


class AccountTaxonomyCodeSuccessorTest(unittest.TestCase):
    def setUp(self):
        self.fixture = fixture = fixtures.FourPlatformFlowReleaseTest()
        if not FROZEN_FLOW.is_dir():
            self.skipTest(
                "Immutable schema23 parent fixture source is not installed on this host"
            )
        with patch.object(fixtures, "ROOT", FROZEN_FLOW):
            fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        self.f = f = fixture.f
        # Produce a real schema23 parent with its complete original verifier.
        entry = fixture.source / release.ENTRY
        body = entry.read_text()
        self.assertNotIn(DISPATCH, body)
        for name in (release.MODULE, release.SCRIPT):
            (fixture.source / name).unlink(missing_ok=True)
        fixture.tree = fixtures.release.inventory(fixture.source)
        fixture.tree_ref = self.write("parent-tree-final.json", fixture.tree)
        changes = fixtures.release.source_changes(fixture.fixture.tree, fixture.tree)
        for name, ref in fixture.checks.items():
            record = fixtures.release.object_at(ref)
            record.update(source_tree=fixture.tree_ref, changes=changes)
            fixture.checks[name] = self.write(
                "parent-check-final-" + name + ".json", record
            )
        fixture.args.source_tree = Path(fixture.tree_ref["path"])
        fixture.args.check_report = [
            name + "=" + ref["path"] for name, ref in fixture.checks.items()
        ]
        migration = fixture.installer.install(fixture.args)
        parent_proposal = fixture.prepare(migration)
        self.parent_ref = parent_proposal["child_build"]
        self.parent = release.payload_at(self.parent_ref, "sealed-build-receipt-v1")
        self.installed_path = fixture.installed_path
        self.installed_path.write_bytes(
            Path(parent_proposal["next_plist"]["path"]).read_bytes()
        )
        self.parent_plist = self.installed_path.read_bytes()

        def installed(**_):
            payload = plistlib.loads(self.installed_path.read_bytes())
            return replace(
                fixture.installed,
                payload=payload,
                home=f.home,
                plist_path=self.installed_path,
                project_root=f.project,
                database=f.db,
                writer_lock=Path(payload["EnvironmentVariables"]["DCAR_WRITER_LOCK"]),
                program=Path(payload["ProgramArguments"][0]),
            )

        self.enterContext(
            patch.object(
                runtime_database,
                "load_installed_writer_contract",
                side_effect=installed,
            )
        )
        self.source = f.root / "taxonomy-source"
        shutil.copytree(fixture.source, self.source)
        for name in release.BUSINESS_FILES | release.CONTRACT_FILES:
            path = self.source / name
            path.write_bytes(path.read_bytes() + b"\n")
        for name in (release.MODULE, release.SCRIPT, release.ENTRY):
            shutil.copy2(ROOT / name, self.source / name)
        self.tree = release.inventory(self.source)
        self.tree_ref = self.write("taxonomy-tree.json", self.tree)
        self.changes = release.source_changes(fixture.tree, self.tree)
        self.review_ref = self.write(
            "taxonomy-review.json",
            {
                "contract": release.REVIEW_CONTRACT,
                "status": "reviewed",
                "scope": release.SCOPE,
                "parent_build": self.parent_ref,
                "changes": {
                    name: self.changes[name]
                    for name in sorted(release.BUSINESS_FILES | release.CONTRACT_FILES)
                },
                "reviewed_by": ["disposable-test-fixture"],
                "reviewed_at": "2026-09-13T00:00:00+00:00",
            },
        )
        self.checks = {}
        for name in sorted(release.CHECKS):
            log = f.root / ("taxonomy-" + name + ".log")
            log.write_text(
                "Synthetic receipt for disposable unit fixture only.\nRan 1 test in 0.1s\nOK\n"
            )
            log.chmod(0o600)
            self.checks[name] = self.write(
                "taxonomy-check-" + name + ".json",
                {
                    "contract": release.CHECK_CONTRACT,
                    "name": name,
                    "source_tree": self.tree_ref,
                    "changes": self.changes,
                    "parent_build": self.parent_ref,
                    "review_manifest": self.review_ref,
                    "command": release.test_command(name, self.parent),
                    "status": "passed",
                    "exit_code": 0,
                    "test_count": 1,
                    "skipped_tests": 0,
                    "output": release.reference(log),
                },
            )
        self.cli = fixtures.script("prepare_account_taxonomy_release")
        self.enterContext(patch.object(self.cli, "ROOT", self.source))
        self.enterContext(
            patch.object(
                self.cli, "load_installed_writer_contract", side_effect=installed
            )
        )
        self.args = SimpleNamespace(
            parent_build=Path(self.parent_ref["path"]),
            source_tree=Path(self.tree_ref["path"]),
            review_manifest=Path(self.review_ref["path"]),
            installed_plist=self.installed_path,
            check_report=[
                name + "=" + ref["path"] for name, ref in self.checks.items()
            ],
            evidence_root=f.root / "taxonomy-prepared",
            actor="disposable fixture",
            reason="offline tests only",
        )
        self.enterContext(
            patch(
                "socket.socket.connect",
                side_effect=AssertionError("provider network forbidden"),
            )
        )

    def write(self, name, value):
        path = self.f.root / name
        path.write_text(json.dumps(value, ensure_ascii=False, sort_keys=True))
        path.chmod(0o600)
        return release.reference(path)

    def verify_checks(self, checks=None, review=None):
        return release.verify_checks(
            checks or self.checks,
            tree_ref=self.tree_ref,
            changes=self.changes,
            parent_ref=self.parent_ref,
            review_ref=review or self.review_ref,
        )

    def test_prepare_install_retry_and_rollback_preserve_database_and_paid_proof(self):
        original = self.f.db.stat()
        paid_before = self.f.connection.execute(
            "SELECT count(*) FROM capture_paid_send_gate_events"
        ).fetchone()[0]
        _, parent_proof = release.parent_context(
            self.parent_ref, install_path=self.f.install_path, database=self.f.db
        )
        preflight = self.cli.preflight(self.args)
        self.assertTrue(preflight["activation_ready"], preflight)
        proposal = self.cli.prepare(self.args)
        self.assertEqual(self.installed_path.read_bytes(), self.parent_plist)
        child = release.payload_at(proposal["child_build"], "sealed-build-receipt-v1")
        inherited = fixtures.release.verify_inheritance(
            build=child,
            build_ref=proposal["child_build"],
            install_path=self.f.install_path,
            database=self.f.db,
            source=self.source,
        )
        self.assertEqual(
            {k: v for k, v in inherited.items() if k != "account_taxonomy_code_proof"},
            parent_proof,
        )
        activating = SimpleNamespace(
            proposal=Path(proposal["proposal"]["path"]),
            output_dir=self.f.root / "taxonomy-install",
        )
        receipt = self.cli.activate(activating)
        self.assertEqual(receipt["status"], "installed_stopped")
        self.assertEqual(self.cli.activate(activating), receipt)
        self.assertEqual(self.f.db.stat().st_ino, original.st_ino)
        self.assertEqual(
            self.f.connection.execute(
                "SELECT count(*) FROM capture_paid_send_gate_events"
            ).fetchone()[0],
            paid_before,
        )
        self.f.connection.execute(
            "UPDATE accounts SET operator_name='after code installation' WHERE id=(SELECT min(id) FROM accounts)"
        )
        self.f.connection.commit()
        rolling_back = SimpleNamespace(
            proposal=activating.proposal,
            activation_dir=activating.output_dir,
            output=self.f.root / "taxonomy-rollback.json",
        )
        restored = self.cli.rollback(rolling_back)
        self.assertFalse(restored["database_restored"])
        self.assertEqual(self.cli.rollback(rolling_back), restored)
        self.assertEqual(self.installed_path.read_bytes(), self.parent_plist)
        self.assertEqual(
            self.f.connection.execute(
                "SELECT operator_name FROM accounts ORDER BY id LIMIT 1"
            ).fetchone()[0],
            "after code installation",
        )

    def test_fixed_checks_reject_noop_missing_tests_and_wrong_review(self):
        self.verify_checks()
        for replacement in (
            {"command": ["/usr/bin/true"]},
            {"test_count": 0},
            {"skipped_tests": 1},
            {"review_manifest": self.parent_ref},
        ):
            with self.subTest(replacement=replacement):
                record = {
                    **release.object_at(self.checks["taxonomy_api"]),
                    **replacement,
                }
                checks = {
                    **self.checks,
                    "taxonomy_api": self.write("bad-check.json", record),
                }
                with self.assertRaisesRegex(ValueError, "check does not bind"):
                    self.verify_checks(checks)
        review = release.object_at(self.review_ref)
        review["changes"]["src/dcar_eval/v8/api.py"]["after_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "independently reviewed"):
            self.verify_checks(review=self.write("wrong-review.json", review))

    def test_plist_copy_and_proposal_target_substitution_are_rejected(self):
        copied = self.f.root / "another.plist"
        copied.write_bytes(self.parent_plist)
        copied.chmod(0o600)
        with self.assertRaisesRegex(ValueError, "actual installed Writer"):
            self.cli.installed_context(
                SimpleNamespace(**{**vars(self.args), "installed_plist": copied})
            )
        proposal = self.cli.prepare(self.args)
        changed = release.object_at(proposal["proposal"])
        changed["installed_plist"] = str(copied)
        fake = self.write("fake-proposal.json", changed)
        with self.assertRaisesRegex(ValueError, "actual installed Writer"):
            self.cli.activate(
                SimpleNamespace(
                    proposal=Path(fake["path"]),
                    output_dir=self.f.root / "bad-activation",
                )
            )
        self.assertFalse((self.f.root / "bad-activation").exists())
        self.assertEqual(self.installed_path.read_bytes(), self.parent_plist)

    def test_before_plist_must_rebind_parent_database_and_install(self):
        proposal = self.cli.prepare(self.args)
        changed = release.object_at(proposal["proposal"])
        before = plistlib.loads(self.parent_plist)
        before["EnvironmentVariables"]["DCAR_ACCOUNT_CLEANUP_INSTALL_RECEIPT"] = str(
            self.f.root / "wrong-install.json"
        )
        path = self.f.root / "fake-before.plist"
        path.write_bytes(plistlib.dumps(before))
        path.chmod(0o600)
        changed["before_plist"] = release.reference(path)
        path = self.f.root / "fake-after.plist"
        path.write_bytes(
            plistlib.dumps(
                self.cli.next_plist(before, self.source, proposal["child_build"]),
                sort_keys=True,
            )
        )
        path.chmod(0o600)
        changed["next_plist"] = release.reference(path)
        fake = self.write("fake-before-proposal.json", changed)
        with self.assertRaisesRegex(ValueError, "exact requested parent"):
            self.cli.verified_proposal(Path(fake["path"]))

    def test_changed_source_and_paid_authority_fail_closed(self):
        proposal = self.cli.prepare(self.args)
        child = release.payload_at(proposal["child_build"], "sealed-build-receipt-v1")
        child["four_platform_flow_successor"]["scope"] = "expanded"
        ref = self.write(
            "wrong-child.json",
            {
                "contract_version": "sealed-build-receipt-v1",
                "payload": child,
                "payload_sha256": release.digest(child),
            },
        )
        with self.assertRaisesRegex(ValueError, "paid authority changed"):
            release.verify_inheritance(
                build=child,
                build_ref=ref,
                install_path=self.f.install_path,
                database=self.f.db,
                source=self.source,
            )
        path = self.source / "src/dcar_eval/v8/api.py"
        path.write_bytes(path.read_bytes() + b"\n# race after validation\n")
        with self.assertRaisesRegex(ValueError, "independently sealed"):
            self.cli.activate(
                SimpleNamespace(
                    proposal=Path(proposal["proposal"]["path"]),
                    output_dir=self.f.root / "race-install",
                )
            )
        self.assertEqual(self.installed_path.read_bytes(), self.parent_plist)

    def test_rollback_output_may_not_pollute_either_sealed_source_or_data(self):
        proposal = self.cli.prepare(self.args)
        for root in (self.source, self.fixture.source, self.f.project):
            with (
                self.subTest(root=root),
                self.assertRaisesRegex(ValueError, "must be independent"),
            ):
                self.cli.rollback(
                    SimpleNamespace(
                        proposal=Path(proposal["proposal"]["path"]),
                        activation_dir=self.f.root / "not-installed",
                        output=root / "invalid-new-receipt.json",
                    )
                )
            self.assertFalse((root / "invalid-new-receipt.json").exists())

    def test_maintenance_lock_blocks_activation_before_backup(self):
        proposal = self.cli.prepare(self.args)
        with runtime_database.hold_formal_mutation(
            self.f.db, project_root=self.f.project
        ):
            with self.assertRaises(runtime_database.RuntimeDatabaseError):
                self.cli.activate(
                    SimpleNamespace(
                        proposal=Path(proposal["proposal"]["path"]),
                        output_dir=self.f.root / "locked-install",
                    )
                )
        self.assertFalse((self.f.root / "locked-install").exists())
        self.assertEqual(self.installed_path.read_bytes(), self.parent_plist)

    @contextmanager
    def child_writer(self):
        proposal = self.cli.prepare(self.args)
        body = Path(proposal["next_plist"]["path"]).read_bytes()
        self.installed_path.write_bytes(body)
        environment = plistlib.loads(body)["EnvironmentVariables"]
        with (
            patch.dict(
                os.environ,
                {
                    **environment,
                    "DCAR_LOADED_BUILD_ID": "sha256:"
                    + proposal["child_build"]["sha256"],
                },
            ),
            patch.object(context, "_loaded_source_root", return_value=self.source),
        ):
            access = runtime_database.resolve_installed_database_access(
                runtime_database.DatabaseAccessMode.WRITER,
                database=self.f.db,
                project_root=self.f.project,
            )
            with runtime_database.acquire_writer_lock(access):
                yield proposal

    def test_real_child_paid_boundaries_reuse_taxonomy_proof_and_preserve_live_gates(
        self,
    ):
        from v8 import capture_authorizations as auth, capture_release, provider_budget
        from v8.storage import transaction, now_utc

        operation = "wechat_channels_video_comments"
        connection = self.f.connection

        def authorize(expected=None):
            at = now_utc()
            bindings = capture_release.current_runtime_bindings(
                connection, operation, at
            )
            return auth.validate_authorization(
                connection,
                runtime_bindings=bindings,
                operation=operation,
                request_identity="f" * 64,
                amount_microusd=provider_budget.PRICES_MICROUSD[operation],
                at=at,
                expected_authority_sha256=expected,
            )

        with self.child_writer() as proposal:
            with transaction(connection):
                capture_release.publish_operation_gate(
                    connection,
                    operation=operation,
                    at=now_utc(),
                    mirror_root=self.f.root / "taxonomy-gates",
                )
            before = connection.execute(
                "SELECT count(*) FROM provider_usage"
            ).fetchone()[0]
            child = release.payload_at(
                proposal["child_build"], "sealed-build-receipt-v1"
            )
            arguments = dict(
                build=child,
                build_ref=proposal["child_build"],
                install_path=self.f.install_path,
                database=self.f.db,
                source=self.source,
            )
            cold = time.perf_counter()
            with transaction(connection):
                first = authorize()
                authorize(first["authority_sha256"])
            cold = time.perf_counter() - cold
            real_taxonomy, real_inventory, real_run = (
                release.verify_inheritance,
                release.inventory,
                subprocess.run,
            )
            cold_calls, inventory_calls, hooks = [], [], []

            def taxonomy(**kwargs):
                cold_calls.append(connection.in_transaction)
                self.assertFalse(
                    connection.in_transaction,
                    "taxonomy full proof entered paid write transaction",
                )
                return real_taxonomy(**kwargs)

            def inventory(source):
                inventory_calls.append(connection.in_transaction)
                self.assertFalse(
                    connection.in_transaction,
                    "source inventory entered paid write transaction",
                )
                return real_inventory(source)

            def run(command, *args, **kwargs):
                if command and Path(command[0]).name == "git":
                    self.assertFalse(
                        connection.in_transaction, "Git entered paid write transaction"
                    )
                return real_run(command, *args, **kwargs)

            with (
                patch.object(release, "verify_inheritance", side_effect=taxonomy),
                patch.object(release, "inventory", side_effect=inventory),
                patch.object(subprocess, "run", side_effect=run),
                patch.object(sys, "addaudithook", side_effect=hooks.append),
            ):
                hot = []
                for _ in range(2):
                    with context.prepare_inheritance(self.f.db) as prepared:
                        self.assertIn(Path(self.review_ref["path"]), prepared.files)
                        self.assertIn(self.source / release.MODULE, prepared.files)
                        self.assertIn(
                            self.fixture.source / release.ENTRY, prepared.files
                        )
                        started = time.perf_counter()
                        with (
                            transaction(connection),
                            context.inheritance_boundary(connection),
                        ):
                            a = authorize()
                            authorize(a["authority_sha256"])
                            proof = fixtures.release.verify_inheritance(
                                **arguments, at=now_utc(), connection=connection
                            )
                            self.assertEqual(
                                proof["account_taxonomy_code_proof"]["loaded_build"],
                                proposal["child_build"],
                            )
                            self.assertEqual(
                                proof["four_platform_flow_proof"]["loaded_build"],
                                self.parent_ref,
                            )
                            proof["account_taxonomy_code_proof"]["scope"] = (
                                "caller mutation"
                            )
                            again = fixtures.release.verify_inheritance(
                                **arguments, at=now_utc(), connection=connection
                            )
                            self.assertEqual(
                                again["account_taxonomy_code_proof"]["scope"],
                                release.SCOPE,
                            )
                        hot.append(time.perf_counter() - started)
                self.assertEqual(cold_calls, [False, False])
                self.assertTrue(inventory_calls)
                self.assertEqual(
                    hooks,
                    [],
                    "same parent module registered another permanent audit hook",
                )
                # A legitimate gate close between preparation and B wins over
                # the immutable proof cache, without entering a cold verifier.
                with context.prepare_inheritance(self.f.db):
                    with transaction(connection):
                        old = connection.execute(
                            "SELECT * FROM capture_paid_send_gate_events WHERE operation=? ORDER BY id DESC LIMIT 1",
                            (operation,),
                        ).fetchone()
                        row = {
                            key: old[key]
                            for key in (
                                "provider",
                                "operation",
                                "state",
                                "reason",
                                "evidence_json",
                                "recorded_at",
                            )
                        }
                        row.update(
                            state="closed",
                            reason="offline taxonomy revoke",
                            recorded_at=now_utc(),
                        )
                        connection.execute(
                            f"INSERT INTO capture_paid_send_gate_events({','.join(row)},event_sha256) VALUES ({','.join('?' for _ in range(len(row) + 1))})",
                            (*row.values(), auth.digest(row)),
                        )
                    with (
                        transaction(connection),
                        context.inheritance_boundary(connection),
                    ):
                        with self.assertRaisesRegex(
                            auth.AuthorizationError, "current authorization"
                        ):
                            authorize(first["authority_sha256"])
                # Review changes after reuse are fenced before commit. No
                # fallback to expensive source verification is allowed.
                with context.prepare_inheritance(self.f.db):
                    with (
                        self.assertRaises(context.RuntimeEvidenceChanged),
                        transaction(connection),
                        context.inheritance_boundary(connection),
                    ):
                        fixtures.release.verify_inheritance(
                            **arguments, at=now_utc(), connection=connection
                        )
                        path = Path(self.review_ref["path"])
                        path.write_bytes(path.read_bytes() + b"\n")
                self.assertEqual(cold_calls, [False] * 4)
                self.assertEqual(hooks, [])
            self.assertEqual(
                connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0],
                before,
            )
            self.assertLess(max(hot), 1.0)
            self.assertLess(max(hot), cold * 0.1)
            print(
                "\n"
                + json.dumps(
                    {
                        "contract": "temporary-taxonomy-paid-inheritance-performance-v1",
                        "cold_lock_seconds": cold,
                        "prepared_lock_seconds": hot,
                        "provider_calls": 0,
                        "formal_runtime_acceptance": False,
                    },
                    sort_keys=True,
                )
            )

    def test_cached_parent_module_does_not_skip_source_verification(self):
        self.cli.prepare(self.args)
        with release._parent_verifier(
            self.fixture.source, self.parent_ref["sha256"]
        ) as first:
            with release._parent_verifier(
                self.fixture.source, self.parent_ref["sha256"]
            ) as second:
                self.assertIs(first, second)
        path = self.fixture.source / "src/dcar_eval/v8/provider_budget.py"
        path.write_bytes(
            path.read_bytes() + b"\n# disposable parent changed after import\n"
        )
        with self.assertRaisesRegex(
            ValueError, "complete parent source or verifier changed"
        ):
            release.parent_context(
                self.parent_ref, install_path=self.f.install_path, database=self.f.db
            )


class TaxonomyS5EntryTest(unittest.TestCase):
    def test_only_exact_dispatch_after_unchanged_s5_reuse_prefix_is_accepted(self):
        parent = (FROZEN_FLOW / release.ENTRY).read_bytes()
        child = (ROOT / release.ENTRY).read_bytes()
        expected = release._dispatch_ast(parent, child=False)
        self.assertEqual(release._dispatch_ast(child, child=True), expected)
        module = ast.parse(child)
        function = next(
            node
            for node in module.body
            if isinstance(node, ast.FunctionDef) and node.name == "verify_inheritance"
        )
        first = ast.parse(child)
        target = next(
            node
            for node in first.body
            if isinstance(node, ast.FunctionDef) and node.name == "verify_inheritance"
        )
        target.body.insert(0, target.body.pop(3))
        duplicate = ast.parse(child)
        target = next(
            node
            for node in duplicate.body
            if isinstance(node, ast.FunctionDef) and node.name == "verify_inheritance"
        )
        target.body.insert(4, function.body[3])
        wrong_connection = child.replace(
            b"reused = reuse_inheritance(connection=connection",
            b"reused = reuse_inheritance(connection=None",
            1,
        )
        candidates = (
            ast.unparse(first).encode(),
            ast.unparse(duplicate).encode(),
            wrong_connection,
            child + b"\nUNREVIEWED_CHANGE=True\n",
        )
        for value in candidates:
            with self.subTest(candidate=value[-50:]):
                try:
                    normalized = release._dispatch_ast(value, child=True)
                except ValueError:
                    continue
                self.assertNotEqual(
                    normalized, expected, "unreviewed entry change accepted"
                )

    def test_parent_module_identity_is_stable_concurrent_and_build_specific(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary).resolve()
            package = source / "src/dcar_eval/v8"
            package.mkdir(parents=True)
            (package / "four_platform_flow_release.py").write_text(
                "from . import runtime_evidence_context\n"
            )
            (package / "runtime_evidence_context.py").write_text(
                "import sys\nsys.addaudithook(lambda event, arguments: None)\n"
            )
            identity = hashlib.sha256(str(source).encode()).hexdigest()
            other_identity = hashlib.sha256(
                (str(source) + "different build").encode()
            ).hexdigest()
            prefixes = tuple(
                "_dcar_taxonomy_parent_" + key for key in (identity, other_identity)
            )
            self.addCleanup(
                lambda: [
                    sys.modules.pop(name, None)
                    for name in list(sys.modules)
                    if any(
                        name == prefix or name.startswith(prefix + ".")
                        for prefix in prefixes
                    )
                ]
            )
            hooks = []

            def load(_):
                with release._parent_verifier(source, identity) as module:
                    return module

            with patch.object(sys, "addaudithook", side_effect=hooks.append):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    modules = list(pool.map(load, range(8)))
                self.assertTrue(all(module is modules[0] for module in modules))
                self.assertEqual(len(hooks), 1)
                self.assertEqual(load(None), modules[0])
                self.assertEqual(len(hooks), 1)
                with release._parent_verifier(source, other_identity) as other:
                    self.assertIsNot(other, modules[0])
                self.assertEqual(len(hooks), 2)
                with self.assertRaisesRegex(
                    ValueError, "cached parent verifier source differs"
                ):
                    with release._parent_verifier(source / "other", identity):
                        pass


class TaxonomyQuiescenceTest(unittest.TestCase):
    def test_valid_lease_blocks_even_without_running_state(self):
        import sqlite3

        with sqlite3.connect(":memory:") as connection:
            connection.execute("PRAGMA user_version=23")
            for table, columns in {
                "scheduler_runs": "status TEXT",
                "scheduler_run_attempts": "status TEXT,owner_token TEXT,lease_expires_at TEXT",
                "capture_work_items": "state TEXT,owner_token TEXT,lease_expires_at TEXT",
                "fetch_slots": "status TEXT",
                "media_processing_slots": "status TEXT",
                "report_tasks": "task_status TEXT",
            }.items():
                connection.execute("CREATE TABLE " + table + "(" + columns + ")")
            connection.execute(
                "INSERT INTO capture_work_items VALUES('pending','worker','2026-09-13T00:10:00Z')"
            )
            connection.execute(
                "INSERT INTO scheduler_run_attempts VALUES('queued',NULL,NULL)"
            )
            self.assertEqual(
                release.quiescence(connection, at="2026-09-13T00:00:00Z")[
                    "capture_work_items"
                ],
                1,
            )
            self.assertFalse(
                any(release.quiescence(connection, at="2026-09-13T00:11:00Z").values())
            )


if __name__ == "__main__":
    unittest.main()
