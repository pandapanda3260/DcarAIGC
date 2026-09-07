"""Verify executable source before importing it, including Git/cache injection."""
from __future__ import annotations

import hashlib
import json
import os
import plistlib
import py_compile
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from v8.runtime_paths import verify_source_before_import


def digest(body):
    return hashlib.sha256(body).hexdigest()


class SourcePreimportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.source, self.data, self.home, self.evidence = (self.root / name for name in ("source", "data", "home", "evidence"))
        for directory in (self.source, self.data, self.home, self.evidence):
            directory.mkdir()
        self.verifier = self.source / "src/dcar_eval/v8/runtime_paths.py"
        self.verifier.parent.mkdir(parents=True)
        shutil.copyfile(Path(__file__).resolve().parents[1] / "src/dcar_eval/v8/runtime_paths.py", self.verifier)
        self.business = self.verifier.with_name("runtime_source_successor.py")
        self.business.write_text("VALUE = 1\n")
        (self.source / ".gitignore").write_text("__pycache__/\n*.pyc\n")
        self.git("init", "-q", "-b", "sealed")
        self.git("add", ".")
        self.git("-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
                 "-c", "commit.gpgsign=false", "commit", "-qm", "sealed")
        record = {"head": self.git("rev-parse", "HEAD").decode().strip(),
                  "tree": self.git("rev-parse", "HEAD^{tree}").decode().strip(),
                  "branch": "sealed", "status_porcelain_sha256": digest(b"")}
        names = [os.fsdecode(name) for name in self.git("ls-files", "-z").split(b"\0") if name]
        manifest = {"contract": "writer-source-tree-v1", "source_root": str(self.source), "git": record,
                    "files": [{"path": name, "sha256": digest((self.source / name).read_bytes()),
                               "byte_size": (self.source / name).stat().st_size,
                               "mode": stat.S_IMODE((self.source / name).stat().st_mode)} for name in sorted(names)]}
        plan = {"contract": "writer-source-isolation-successor-plan-v1",
                "transition": "writer-source-isolation-20260907-v1", "project_root": str(self.data),
                "source_root": str(self.source), "git": record, "source_tree": self.receipt("source.json", manifest)}
        payload = {"status": "succeeded", "git": record, "code_successor_plan": self.receipt("plan.json", plan)}
        envelope = {"contract_version": "sealed-build-receipt-v1", "payload": payload,
                    "payload_sha256": digest(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode())}
        self.build = Path(self.receipt("build.json", envelope)["path"])
        self.plist = self.home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        self.plist.parent.mkdir(parents=True)
        self.plist.write_bytes(plistlib.dumps({"Label": "cn.tj.dcar.writer-worker", "WorkingDirectory": str(self.data),
            "ProgramArguments": [str(self.source / "deploy/macos/run_writer_worker.sh")],
            "EnvironmentVariables": {"DCAR_PROJECT_ROOT": str(self.data), "DCAR_WRITER_SOURCE_ROOT": str(self.source),
                                     "DCAR_LOADED_BUILD_RECEIPT": str(self.build)}}))
        self.plist.chmod(0o600)

    def git(self, *args):
        return subprocess.run(["git", "-C", str(self.source), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

    def receipt(self, name, value):
        path = self.evidence / name
        body = json.dumps(value).encode()
        path.write_bytes(body)
        path.chmod(0o600)
        return {"path": str(path), "sha256": digest(body), "byte_size": len(body)}

    def verify(self):
        return verify_source_before_import(data=self.data, source=self.source, build_receipt=self.build, home=self.home)

    def test_valid_source_checks_without_loading_business_module(self):
        self.assertEqual(self.verify()["files"], 3)

    def test_source_hash_change_is_rejected_before_import(self):
        marker = self.root / "business-executed"
        self.business.write_text(f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n")
        command = """import importlib.util, pathlib, pwd, sys, types
p=pathlib.Path(sys.argv[1]); spec=importlib.util.spec_from_file_location('verified_entry',p)
module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
pwd.getpwuid=lambda uid: types.SimpleNamespace(pw_dir=sys.argv[2])
module.main(['--verify-source','--project-root',sys.argv[3],'--source-root',sys.argv[4],'--mode','writer','--build-receipt',sys.argv[5]])
"""
        run = subprocess.run([sys.executable, "-I", "-B", "-c", command, str(self.verifier), str(self.home),
            str(self.data), str(self.source), str(self.build)], capture_output=True, text=True,
            env={**os.environ, "DCAR_PROJECT_ROOT": str(self.data), "DCAR_WRITER_SOURCE_ROOT": str(self.source)})
        self.assertNotEqual(run.returncode, 0)
        self.assertIn("bootstrap source", run.stderr)
        self.assertFalse(marker.exists())

    def test_ignored_bytecode_cannot_bypass_source_hashes(self):
        py_compile.compile(str(self.business), doraise=True)
        with self.assertRaisesRegex(ValueError, "unlisted bootstrap executable"):
            self.verify()

    def test_git_fsmonitor_is_never_executed(self):
        marker = self.root / "git-helper-executed"
        helper = self.root / "fsmonitor"
        helper.write_text(f"#!/bin/sh\ntouch '{marker}'\n")
        helper.chmod(0o700)
        self.git("config", "core.fsmonitor", str(helper))
        with self.assertRaisesRegex(ValueError, "external Git helper"):
            self.verify()
        self.assertFalse(marker.exists())

    def test_git_clean_filter_is_rejected_without_execution(self):
        marker = self.root / "git-filter-executed"
        helper = self.root / "clean-filter"
        helper.write_text(f"#!/bin/sh\ntouch '{marker}'\ncat\n")
        helper.chmod(0o700)
        attributes = self.source / ".git/info/attributes"
        attributes.write_text("*.py filter=untrusted\n")
        self.git("config", "filter.untrusted.clean", str(helper))
        with self.assertRaisesRegex(ValueError, "external Git helper"):
            self.verify()
        self.assertFalse(marker.exists())

    def test_receipt_change_and_wrong_installed_source_are_rejected(self):
        plan = self.evidence / "plan.json"
        plan.write_bytes(plan.read_bytes() + b" ")
        with self.assertRaisesRegex(ValueError, "receipt SHA"):
            self.verify()


if __name__ == "__main__":
    unittest.main()
