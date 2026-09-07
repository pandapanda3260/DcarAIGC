from __future__ import annotations

import ast
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from tests import test_seal_r0_receipts as seal_fixture
from v8 import forward_recovery as recovery
from v8.profile_control import ProfileControlError

FORWARD_PATH = "src/dcar_eval/v8/forward_recovery.py"


class ForwardBuildSuccessorTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(self.enterContext(tempfile.TemporaryDirectory())).resolve()
        self.project = self.root / "project"
        self.project.mkdir()
        self.database = self.root / "formal.sqlite3"
        self.connection = sqlite3.connect(self.database)
        self.addCleanup(self.connection.close)
        self.connection.execute("PRAGMA user_version=19")
        self.connection.execute("CREATE TABLE sentinel(id INTEGER PRIMARY KEY,value TEXT)")
        self.connection.execute("INSERT INTO sentinel VALUES(1,'original-release-and-paid-state')")
        self.connection.commit()
        self.critical = [*recovery._SUCCESSOR_SEND_FILES, FORWARD_PATH, "guard.py", "src/dcar_eval/v8/api.py"]
        original_project = Path(__file__).resolve().parents[1]
        self.original_forward = subprocess.run(
            ["git", "-C", str(original_project), "show", "8ae11f9:" + FORWARD_PATH],
            check=True, capture_output=True,
        ).stdout
        self.new_forward = Path(recovery.__file__).read_bytes()
        self.original_sources = {relative: subprocess.run(
            ["git", "-C", str(original_project), "show", "8ae11f9:" + relative], check=True, capture_output=True,
        ).stdout for relative in recovery._SUCCESSOR_OVERVIEW_TRANSITIONS}
        for relative in self.critical:
            self.write(relative, self.original_forward if relative == FORWARD_PATH
                       else self.original_sources.get(relative, b"VERSION = 1\n"))
        self.write("src/dcar_eval/v8/source_routing.py", self.original_sources["src/dcar_eval/v8/source_routing.py"])
        self.write("app/web/page.tsx", b"export const page = 1;\n")
        self.git("init", "-q")
        self.commit()
        self.base = self.seal("base")
        self.git("apply", str(original_project / "tests/fixtures/overview_release_compat.patch"))
        self.overview_sources = {relative: (self.project / relative).read_bytes() for relative in self.original_sources}
        for relative, data in self.original_sources.items():
            self.write(relative, data)
        self.historical_readers = {revision: subprocess.run(
            ["git", "-C", str(original_project), "show", revision + ":" + FORWARD_PATH],
            check=True, capture_output=True,
        ).stdout for revision in ("9ee4eee", "23831115", "100bcbb")}
        self.stream_source = recovery._successor_patch(
            self.overview_sources["src/dcar_eval/v8/source_routing.py"],
            (original_project / "tests/fixtures/metric_stream_release_compat.diff").read_bytes(),
        )
        self.binding = {"build_receipt_sha256": self.base["sha256"],
                        "runtime_root_receipt_sha256": self.base["payload"]["runtime_root_receipt"]["sha256"]}
        self.enterContext(mock.patch.object(recovery, "PROJECT_ROOT", self.project))
        self.before = list(self.connection.execute("SELECT * FROM sentinel"))

    def write(self, relative, data):
        path = self.project / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def git(self, *arguments):
        return subprocess.run(["git", "-C", str(self.project), *arguments], check=True,
                              capture_output=True).stdout.decode().strip()

    def commit(self):
        self.git("add", ".")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                 "commit", "-qm", "sealed fixture", "--allow-empty")

    def receipt(self, name, contract, payload):
        path = self.root / (name + ".json")
        envelope = seal_fixture.receipts._envelope(contract, payload)
        seal_fixture.receipts._write_exclusive(path, envelope)
        return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "payload_sha256": envelope["payload_sha256"]}

    def seal(self, name, previous=None, *, tests_pass=True, schema=19, inode=None,
             lineage=None, corrupt_payload=False, working_tree=False, runtime_project=None):
        git = {"head": self.git("rev-parse", "HEAD"), "tree": self.git("rev-parse", "HEAD^{tree}")}
        source_archive = None
        if working_tree:
            git.update(mode="working-tree-source-v1", working_tree=seal_fixture.receipts._working_tree_manifest(self.project))
            archive_path = self.root / (name + "-source.tar")
            seal_fixture.receipts._write_source_archive(self.project, archive_path, git)
            source_archive = seal_fixture.receipts._source_archive_record(archive_path, git)
        db = self.database.stat()
        runtime = self.receipt(name + "-runtime", "runtime-root-binding-v1", {
            "project_root": str(runtime_project or self.project), "formal_database": {
                "path": str(self.database), "device": db.st_dev,
                "inode": db.st_ino if inode is None else inode, "user_version": schema,
            },
        })
        names = ["backend", "frontend", "lint", "typecheck", "ruff", "mypy"]
        tests = self.receipt(name + "-tests", "test-results-v1", {
            "status": "passed" if tests_pass else "failed", "git": git, "required_results": names,
            "results": {key: {"status": "passed", "exit_code": 0} for key in names},
        })
        payload = {"status": "succeeded", "git": git,
                   "critical_files": {key: hashlib.sha256((self.project / key).read_bytes()).hexdigest() for key in self.critical},
                   "schema_contract": {"formal_schema": schema, "code_schema": schema,
                                       "operation": "code_update", "transition": f"{schema}-to-{schema}"},
                   "runtime_root_receipt": runtime, "test_results_receipt": tests,
                   "postmigration_lineage": {
                       "install_receipt": {"path": "/same/install", "sha256": "a" * 64},
                       "migration_receipt": {"path": "/same/migration", "sha256": "b" * 64},
                       **({"previous_build_receipt": {key: previous[key] for key in ("path", "sha256")}} if previous else {}),
                   }}
        if lineage:
            payload["postmigration_lineage"].update(lineage)
        if source_archive:
            payload["source_archive"] = source_archive
        reference = self.receipt(name, "sealed-build-receipt-v1", payload)
        if corrupt_payload:
            path = Path(reference["path"])
            value = json.loads(path.read_text())
            value["payload"]["status"] = "corrupted"
            path.write_text(json.dumps(value))
            reference["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
        return {**reference, "payload": payload}

    def read(self, build):
        with mock.patch.dict(os.environ, {"DCAR_LOADED_BUILD_ID": "sha256:" + build["sha256"],
                                          "DCAR_LOADED_BUILD_RECEIPT": build["path"]}):
            return recovery._released_runtime_identity(self.connection, self.binding)

    def successor(self, name="next", previous=None, **options):
        self.write("app/web/page.tsx", ("export const page = '" + name + "';\n").encode())
        self.commit()
        return self.seal(name, previous or self.base, **options)

    def test_ui_successor_is_accepted_idempotently_without_new_paid_or_activation_state(self):
        build = self.successor()
        first = self.read(build)
        self.assertEqual(first["build_receipt_sha256"], self.binding["build_receipt_sha256"])
        self.assertEqual(self.read(build), first)
        self.assertEqual(list(self.connection.execute("SELECT * FROM sentinel")), self.before)
        self.assertEqual(self.connection.total_changes, 1)
        self.assertEqual(list(self.connection.execute("SELECT name FROM sqlite_master WHERE type='table'")), [("sentinel",)])

    def test_reader_upgrade_is_exactly_bounded_and_works_through_two_code_updates(self):
        self.assertTrue(recovery._successor_reader_compatible(self.original_forward, self.new_forward))
        self.write(FORWARD_PATH, self.new_forward)
        first = self.successor("guard-fix")
        second = self.successor("later-ui", previous=first)
        self.assertEqual(self.read(second)["build_receipt_sha256"], self.base["sha256"])
        changed = self.new_forward.replace(b"MIN_FREE_BYTES = 10 * 1024**3", b"MIN_FREE_BYTES = 1")
        self.assertFalse(recovery._successor_reader_compatible(self.original_forward, changed))
        changed_helper = self.new_forward.replace(b"for hop in range(16)", b"for hop in range(160)")
        self.assertFalse(recovery._successor_reader_compatible(self.original_forward, changed_helper))

    def test_missing_chain_wrong_hash_and_corrupt_producer_payload_are_rejected(self):
        absent = self.seal("unlinked")
        with self.assertRaises(ProfileControlError):
            self.read(absent)
        bad = self.successor("wrong-link", lineage={"previous_build_receipt": {"path": self.base["path"], "sha256": "0" * 64}})
        with self.assertRaises(ProfileControlError):
            self.read(bad)
        forged = self.successor("forged-payload", corrupt_payload=True)
        with self.assertRaises(ProfileControlError):
            self.read(forged)

    def test_source_reader_normalization_rejects_other_root_import_and_guard_changes(self):
        mutations = (
            (b"path = source_root(PROJECT_ROOT) / relative", b"path = source_root(Path('/other')) / relative"),
            (b"and hashlib.sha256(path.read_bytes()).hexdigest() == expected", b"and True"),
            (b"from .runtime_paths import source_root, verified_git", b"from .runtime_paths import source_root, verified_git, unreviewed"),
            (b"from .runtime_paths import source_root, verified_git", b"from .runtime_paths import source_root as PROJECT_ROOT, verified_git"),
            (b"return verified_git(source_root(Path(project)), *arguments)", b"return verified_git(Path(project), *arguments)"),
        )
        for before, after in mutations:
            with self.subTest(change=before):
                self.assertIn(before, self.new_forward)
                changed = self.new_forward.replace(before, after, 1)
                self.assertFalse(recovery._successor_reader_compatible(self.original_forward, changed))

    def test_frozen_source_retains_chain_when_data_checkout_git_and_bytes_change(self):
        self.write(FORWARD_PATH, self.new_forward)
        build = self.successor("source-root-reader")
        source = self.root / "frozen-source"
        shutil.copytree(self.project, source)
        (self.project / ".git").rename(self.root / "old-data-git")
        self.write("src/dcar_eval/v8/capture.py", b"UNREVIEWED_DEVELOPMENT_EDIT = True\n")
        recovery._successor_git.cache_clear()
        with mock.patch.dict(os.environ, {"DCAR_PROJECT_ROOT": str(self.project),
                                         "DCAR_WRITER_SOURCE_ROOT": str(source)}):
            self.assertEqual(self.read(build)["build_receipt_sha256"], self.base["sha256"])
            self.assertEqual(list(self.connection.execute("SELECT * FROM sentinel")), self.before)
            self.assertEqual(self.connection.total_changes, 1)
            (source / "src/dcar_eval/v8/capture.py").write_bytes(b"UNSEALED_RUNTIME_EDIT = True\n")
            with self.assertRaisesRegex(ProfileControlError, "Loaded safety code"):
                self.read(build)

    def test_schema_database_lineage_and_failed_tests_reject_successor(self):
        for name, options in [
            ("schema", {"schema": 20}), ("database", {"inode": 7}), ("tests", {"tests_pass": False}),
            ("install", {"lineage": {"install_receipt": {"path": "/other/install", "sha256": "c" * 64}}}),
        ]:
            with self.subTest(name=name):
                with self.assertRaises(ProfileControlError):
                    self.read(self.successor(name, **options))

    def test_schema_drift_is_rejected_even_when_loaded_build_id_still_equals_release(self):
        self.connection.execute("PRAGMA user_version=20")
        with self.assertRaises(ProfileControlError):
            self.read(self.base)

    def test_isolated_runtime_checkout_preserves_historical_receipts_and_formal_database(self):
        old_checkout = self.root / "historical-checkout"
        old_checkout.mkdir()
        self.base = self.seal("historical-root", runtime_project=old_checkout)
        self.binding = {"build_receipt_sha256": self.base["sha256"],
                        "runtime_root_receipt_sha256": self.base["payload"]["runtime_root_receipt"]["sha256"]}
        self.read(self.successor("isolated-runtime"))
        with self.assertRaisesRegex(ProfileControlError, "same formal database"):
            self.read(self.successor("wrong-live-root", runtime_project=old_checkout))

    def test_private_receipt_permissions_and_live_code_drift_are_rejected(self):
        build = self.successor()
        Path(build["path"]).chmod(0o644)
        with self.assertRaises(ProfileControlError):
            self.read(build)
        Path(build["path"]).chmod(0o600)
        self.write("src/dcar_eval/v8/capture.py", b"VERSION = 'unsealed edit'\n")
        with self.assertRaises(ProfileControlError):
            self.read(build)

    def test_capture_paid_and_transport_changes_require_a_new_release(self):
        for index, path in enumerate(("src/dcar_eval/v8/capture.py", "src/dcar_eval/v8/paid_dispatch.py",
                                      "src/dcar_eval/v8/provider_transport.py", "guard.py")):
            with self.subTest(path=path):
                self.write(path, b"VERSION = 2\n")
                build = self.successor("critical-" + str(index))
                with self.assertRaises(ProfileControlError):
                    self.read(build)
                self.write(path, b"VERSION = 1\n")

    def test_exact_overview_api_critical_and_selector_patch_is_allowed(self):
        for relative, body in self.overview_sources.items():
            self.assertEqual((hashlib.sha256(self.original_sources[relative]).hexdigest(), hashlib.sha256(body).hexdigest()),
                             recovery._SUCCESSOR_OVERVIEW_TRANSITIONS[relative])
            self.write(relative, body)
        allowed = self.successor("projection")
        self.read(allowed)

    def test_metric_schedule_and_dependencies_cannot_hide_inside_a_projection_update(self):
        relative = "src/dcar_eval/v8/source_routing.py"
        for name in ("metric_refresh_due", "metric_cycle_key", "metric_freshness_seconds", "_age_days", "_policy", "parse_time"):
            with self.subTest(function=name):
                source = self.overview_sources[relative].decode()
                node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == name)
                lines = source.splitlines(keepends=True)
                lines.insert(node.body[0].lineno - 1, "    return True\n")
                self.write(relative, "".join(lines).encode())
                with self.assertRaises(ProfileControlError):
                    self.read(self.successor("schedule-" + name))

    def test_api_write_guard_change_is_not_an_overview_patch(self):
        relative = "src/dcar_eval/v8/api.py"
        self.write(relative, self.overview_sources[relative] + b"\nREAD_ONLY_POST_PATHS = frozenset()\n")
        with self.assertRaises(ProfileControlError):
            self.read(self.successor("api-write-change"))

    def test_approved_account_read_removal_can_join_the_exact_overview_patch(self):
        relative = "src/dcar_eval/v8/api.py"
        source = self.overview_sources[relative].decode()
        node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "_account_search")
        lines = source.splitlines(keepends=True)
        original = "".join(lines[node.lineno - 1:node.end_lineno]).encode()
        fixture = Path(__file__).parent / "fixtures/account_read_release_compat.diff"
        revised = recovery._successor_patch(original, fixture.read_bytes())
        merged = "".join(lines[:node.lineno - 1]).encode() + revised + "".join(lines[node.end_lineno:]).encode()
        self.assertEqual(hashlib.sha256(merged).hexdigest(), "81c4b9ff0ab25b4b0a924b3debde873cb89309a146e7ebab835a6b47862b283c")
        for key, body in self.overview_sources.items():
            self.write(key, merged if key == relative else body)
        self.write("app/web/accounts-panel.tsx", b"export const removedPanel = true;\n")
        self.read(self.seal("account-and-overview-wip", self.base, working_tree=True))

    def test_sealed_working_tree_and_untracked_ui_are_verified_against_live_bytes(self):
        for relative, body in self.overview_sources.items():
            self.write(relative, body)
        self.write("app/web/new.tsx", b"export const untracked = true;\n")
        self.write("app/web/page.tsx", b"export const edited = true;\n")
        candidate = self.seal("wip", self.base, working_tree=True)
        self.read(candidate)
        self.write("app/web/new.tsx", b"export const untracked = false;\n")
        with self.assertRaisesRegex(ProfileControlError, "Live working-tree"):
            self.read(candidate)

    def test_working_tree_cannot_reuse_head_only_tests_or_a_forged_source_archive(self):
        self.write("app/web/new.tsx", b"export const untracked = true;\n")
        candidate = self.seal("wip-corrupt", self.base, working_tree=True)
        tests_ref = candidate["payload"]["test_results_receipt"]
        tests_path = Path(tests_ref["path"])
        tests = json.loads(tests_path.read_bytes())["payload"]
        tests["git"] = {key: tests["git"][key] for key in ("head", "tree")}
        new_tests = self.receipt("stale-head-tests", "test-results-v1", tests)
        payload = dict(candidate["payload"], test_results_receipt=new_tests)
        stale = self.receipt("stale-wip-build", "sealed-build-receipt-v1", payload)
        with self.assertRaisesRegex(ProfileControlError, "successful tests"):
            self.read(stale)
        archive = Path(candidate["payload"]["source_archive"]["path"])
        archive.write_bytes(archive.read_bytes() + b"tampered")
        with self.assertRaisesRegex(ProfileControlError, "archive hash"):
            self.read(candidate)

    def test_working_tree_safety_change_still_requires_new_release(self):
        self.write("src/dcar_eval/v8/capture.py", b"VERSION = 'unapproved source'\n")
        with self.assertRaisesRegex(ProfileControlError, "safety code"):
            self.read(self.seal("wip-safety", self.base, working_tree=True))

    def test_all_historical_reader_fingerprints_remain_accepted(self):
        for revision, reader in self.historical_readers.items():
            with self.subTest(revision=revision):
                self.assertTrue(recovery._successor_reader_compatible(self.original_forward, reader))
                self.assertTrue(recovery._successor_reader_compatible(reader, self.new_forward))
                tampered = reader.replace(b"for hop in range(16)", b"for hop in range(160)")
                self.assertFalse(recovery._successor_reader_compatible(self.original_forward, tampered))

    def test_metric_stream_follows_historical_readers_and_sealed_overview_without_writes(self):
        relative = "src/dcar_eval/v8/source_routing.py"
        change = (hashlib.sha256(self.overview_sources[relative]).hexdigest(),
                  hashlib.sha256(self.stream_source).hexdigest())
        self.assertEqual(recovery._SUCCESSOR_METRIC_READ_TRANSITIONS, frozenset({change}))
        self.write(FORWARD_PATH, self.historical_readers["9ee4eee"])
        first = self.successor("historical-reader")
        self.write(FORWARD_PATH, self.historical_readers["23831115"])
        second = self.successor("historical-wip-reader", previous=first)
        for path, body in self.overview_sources.items():
            self.write(path, body)
        overview = self.seal("historical-overview-wip", second, working_tree=True)
        self.write(FORWARD_PATH, self.new_forward)
        self.write(relative, self.stream_source)
        self.write("app/web/stream.tsx", b"export const stream = true;\n")
        stream = self.seal("stream-wip", overview, working_tree=True)
        self.assertEqual(self.read(stream)["build_receipt_sha256"], self.base["sha256"])
        self.assertEqual(list(self.connection.execute("SELECT * FROM sentinel")), self.before)
        self.assertEqual(self.connection.total_changes, 1)

    def test_metric_stream_cannot_skip_the_frozen_overview_predecessor(self):
        self.write(FORWARD_PATH, self.new_forward)
        self.write("src/dcar_eval/v8/source_routing.py", self.stream_source)
        with self.assertRaisesRegex(ProfileControlError, "safety code"):
            self.read(self.successor("skip-overview"))

    def test_metric_stream_tamper_and_reversal_require_new_release(self):
        relative = "src/dcar_eval/v8/source_routing.py"
        self.write(FORWARD_PATH, self.new_forward)
        for path, body in self.overview_sources.items():
            self.write(path, body)
        overview = self.successor("before-stream")
        self.write(relative, self.stream_source + b"\nMETRIC_REFRESH_DISABLED = True\n")
        with self.assertRaisesRegex(ProfileControlError, "safety code"):
            self.read(self.successor("tampered-stream", previous=overview))
        self.write(relative, self.stream_source)
        stream = self.successor("approved-stream", previous=overview)
        self.read(stream)
        self.write(relative, self.overview_sources[relative])
        with self.assertRaisesRegex(ProfileControlError, "safety code"):
            self.read(self.successor("reverse-stream", previous=stream))

    def test_metric_stream_fixture_preserves_all_paid_schedule_dependencies(self):
        relative = "src/dcar_eval/v8/source_routing.py"
        before, after = ast.parse(self.overview_sources[relative]), ast.parse(self.stream_source)
        for name in ("load_policy", "_policy", "parse_time", "_age_days", "metric_freshness_seconds",
                     "metric_refresh_due", "metric_cycle_key"):
            with self.subTest(function=name):
                old = next(n for n in before.body if isinstance(n, ast.FunctionDef) and n.name == name)
                new = next(n for n in after.body if isinstance(n, ast.FunctionDef) and n.name == name)
                self.assertEqual(ast.dump(old, include_attributes=False), ast.dump(new, include_attributes=False))

    def account_status_fixture(self, *, tamper_before=None):
        """Synthetic exact-hash policy inputs; no real release is manufactured."""
        additions = recovery._SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS
        new_modules = recovery._SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES
        paths = additions | {"src/dcar_eval/v8/api.py", "scripts/seal_r0_receipts.py"}
        before = {path: None if path in new_modules else ("BEFORE = " + repr(path) + "\n").encode()
                  for path in paths}
        after = {path: ("AFTER = " + repr(path) + "\n").encode() for path in paths}
        for path, body in before.items():
            if body is not None:
                self.write(path, body)
        # Match the current schema19 predecessor, rather than reintroducing the
        # much older reader into this independent business-write fixture.
        before_reader = subprocess.run(
            ["git", "-C", str(Path(__file__).resolve().parents[1]), "show", "HEAD:" + FORWARD_PATH],
            check=True, capture_output=True,
        ).stdout
        self.write(FORWARD_PATH, before_reader)
        if tamper_before is not None:
            self.write(tamper_before, before[tamper_before] + b"UNREVIEWED_PREDECESSOR = True\n")
        self.write("src/dcar_eval/v8/provider_budget.py", b"BUDGET_GUARD = True\n")
        self.critical.extend(["scripts/seal_r0_receipts.py", "src/dcar_eval/v8/provider_budget.py"])
        self.commit()
        self.base = self.seal("account-source")
        self.binding = {"build_receipt_sha256": self.base["sha256"],
                        "runtime_root_receipt_sha256": self.base["payload"]["runtime_root_receipt"]["sha256"]}
        transitions = {path: (hashlib.sha256(before[path]).hexdigest() if before[path] is not None else None,
                              hashlib.sha256(body).hexdigest()) for path, body in after.items()}
        self.enterContext(mock.patch.object(recovery, "_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS", transitions))
        for path, body in after.items():
            self.write(path, body)
        self.write(FORWARD_PATH, self.new_forward)
        self.critical.extend(sorted(additions))
        return after

    def test_exact_account_status_write_package_adds_only_seven_bound_dependencies(self):
        self.account_status_fixture()
        package = self.seal("account-package", self.base, working_tree=True)
        first = self.read(package)
        self.assertEqual(first["build_receipt_sha256"], self.binding["build_receipt_sha256"])
        followup = self.successor("account-later-ui", previous=package)
        self.assertEqual(self.read(followup), first)
        self.assertEqual(list(self.connection.execute("SELECT * FROM sentinel")), self.before)
        self.assertEqual(self.connection.total_changes, 1)

    def test_account_status_predecessor_outside_exact_review_is_rejected(self):
        self.account_status_fixture(tamper_before="src/dcar_eval/v8/operations.py")
        with self.assertRaisesRegex(ProfileControlError, "critical inventory"):
            self.read(self.seal("account-unreviewed-baseline", self.base, working_tree=True))

    def test_account_status_requires_frozen_approval(self):
        self.account_status_fixture()
        with mock.patch.object(recovery, "_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS", {}):
            with self.assertRaisesRegex(ProfileControlError, "critical inventory"):
                self.read(self.seal("account-unapproved", self.base, working_tree=True))

    def test_account_status_production_hash_policy_is_frozen_with_reviewed_sources(self):
        project = Path(__file__).resolve().parents[1]
        # The deterministic fixture contains only the nine source files from
        # reviewed schema19 commit f0b3972b0b13b9293aa393dd9bd96323b41f2b61.
        # Their frozen hashes authorize that historical release, not the
        # later account-workbench/schema20 source in the current checkout.
        transitions = recovery._SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS
        self.assertEqual(set(transitions), recovery._SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS
                         | {"src/dcar_eval/v8/api.py", "scripts/seal_r0_receipts.py"})
        with tarfile.open(project / "tests/fixtures/account_status_schema19_reviewed_sources.tar.gz", "r:gz") as archive:
            self.assertEqual(set(archive.getnames()), set(transitions))
            for relative, (before, after) in transitions.items():
                with self.subTest(path=relative):
                    self.assertTrue(archive.getmember(relative).isfile())
                    reviewed_file = archive.extractfile(relative)
                    self.assertIsNotNone(reviewed_file)
                    reviewed_source = reviewed_file.read() if reviewed_file else b""
                    self.assertEqual(hashlib.sha256(reviewed_source).hexdigest(), after)
                    self.assertEqual(before is None, relative in recovery._SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES)
        digest = transitions["src/dcar_eval/v8/api.py"][1].encode()
        tampered_policy = self.new_forward.replace(digest, b"0" * 64)
        self.assertNotEqual(tampered_policy, self.new_forward)
        self.assertFalse(recovery._successor_reader_compatible(self.original_forward, tampered_policy))

    def test_account_status_helper_and_api_write_tampering_are_rejected(self):
        approved = self.account_status_fixture()
        paths = (*sorted(recovery._SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES), "src/dcar_eval/v8/api.py")
        for number, path in enumerate(paths):
            with self.subTest(path=path):
                self.write(path, approved[path] + b"UNREVIEWED_WRITE = True\n")
                with self.assertRaises(ProfileControlError):
                    self.read(self.seal(f"account-helper-tamper-{number}", self.base, working_tree=True))
                self.write(path, approved[path])

    def test_account_status_cannot_change_collectors_budget_or_add_unreviewed_inventory(self):
        self.account_status_fixture()
        for number, path in enumerate(("src/dcar_eval/v8/providers.py", "src/dcar_eval/v8/provider_budget.py")):
            before = (self.project / path).read_bytes()
            self.write(path, before + b"UNREVIEWED_PROVIDER_CHANGE = True\n")
            with self.subTest(path=path), self.assertRaisesRegex(ProfileControlError, "safety code"):
                self.read(self.seal(f"account-provider-tamper-{number}", self.base, working_tree=True))
            self.write(path, before)
        self.write("unreviewed.py", b"NOT_APPROVED = True\n")
        self.critical.append("unreviewed.py")
        with self.assertRaisesRegex(ProfileControlError, "critical inventory"):
            self.read(self.seal("account-extra-critical", self.base, working_tree=True))

    def test_account_status_partial_inventory_and_live_drift_are_rejected(self):
        approved = self.account_status_fixture()
        omitted = "src/dcar_eval/v8/statistics_scope.py"
        self.critical.remove(omitted)
        with self.assertRaises(ProfileControlError):
            self.read(self.seal("account-missing-critical", self.base, working_tree=True))
        self.critical.append(omitted)
        package = self.seal("account-before-drift", self.base, working_tree=True)
        self.write(omitted, approved[omitted] + b"CHANGED_AFTER_TESTING = True\n")
        with self.assertRaisesRegex(ProfileControlError, "Live working-tree"):
            self.read(package)

    def test_successor_reader_fingerprint_is_frozen(self):
        module = ast.parse(self.new_forward)
        names = {"_successor_git", "_successor_receipt", "_successor_patch", "_successor_patch_sources",
                 "_successor_archive", "_successor_build", "_successor_reader_compatible",
                 "_successor_source", "_released_runtime_identity"}
        bodies = [node for node in module.body if (isinstance(node, ast.FunctionDef) and node.name in names)
                  or (isinstance(node, ast.Assign) and any(isinstance(name, ast.Name)
                      and name.id in {"_SUCCESSOR_SEND_FILES", "_SUCCESSOR_OVERVIEW_TRANSITIONS", "_SUCCESSOR_ACCOUNT_READ_TRANSITIONS", "_SUCCESSOR_METRIC_READ_TRANSITIONS",
                                      "_SUCCESSOR_ACCOUNT_STATUS_CRITICAL_ADDITIONS", "_SUCCESSOR_ACCOUNT_STATUS_NEW_MODULES"} for name in node.targets))
                  or (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
                      and node.target.id == "_SUCCESSOR_ACCOUNT_STATUS_TRANSITIONS")]
        digest = hashlib.sha256(ast.dump(ast.Module(body=bodies, type_ignores=[]), include_attributes=False).encode()).hexdigest()
        self.assertEqual(digest, recovery._SUCCESSOR_READER_SHA256)


if __name__ == "__main__":
    unittest.main()
