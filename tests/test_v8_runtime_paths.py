"""Regression coverage for a sealed runtime beside a changing development tree."""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from v8.runtime_paths import project_root, source_root


class RuntimePathsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name).resolve()
        self.data = self.base / "development"
        self.source = self.base / "sealed"
        self.fixture = self.base / "fixture"
        for directory in (self.data, self.source, self.fixture):
            directory.mkdir()
        self.environment = {"DCAR_PROJECT_ROOT": str(self.data),
                            "DCAR_WRITER_SOURCE_ROOT": str(self.source)}

    def test_legacy_roots_remain_unchanged(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(project_root(self.data), self.data)
            self.assertEqual(source_root(self.data), self.data)

    def test_source_imports_keep_data_and_explicit_fixtures_separate(self):
        with patch.dict(os.environ, self.environment, clear=True):
            self.assertEqual(project_root(self.source), self.data)
            self.assertEqual(source_root(self.data), self.source)
            self.assertEqual(project_root(self.fixture), self.fixture)
            self.assertEqual(source_root(self.fixture), self.fixture)

    def test_nested_or_symlink_source_is_rejected(self):
        nested = self.data / "release"
        nested.mkdir()
        alias = self.base / "alias"
        alias.symlink_to(self.source, target_is_directory=True)
        for root in (nested, alias, self.data):
            with self.subTest(root=root), patch.dict(os.environ,
                    {**self.environment, "DCAR_WRITER_SOURCE_ROOT": str(root)}, clear=True):
                with self.assertRaises(ValueError):
                    source_root(self.data)

    def test_source_selection_requires_explicit_existing_data_root(self):
        with patch.dict(os.environ, {"DCAR_WRITER_SOURCE_ROOT": str(self.source)}, clear=True):
            with self.assertRaises(ValueError):
                project_root(self.source)

    def test_sealer_reads_source_git_while_development_changes(self):
        repository = Path(__file__).resolve().parents[1]
        name = "_test_runtime_paths_sealer"
        spec = importlib.util.spec_from_file_location(name, repository / "scripts/seal_r0_receipts.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {name: module}):
            spec.loader.exec_module(module)
        def git(root, *args):
            return subprocess.run(["git", "-C", str(root), *args], check=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
        for root in (self.data, self.source):
            git(root, "init", "-q", "-b", "test")
        (self.source / "code.py").write_text("VALUE = 1\n")
        git(self.source, "add", "code.py")
        git(self.source, "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "-qm", "sealed")
        with patch.dict(os.environ, self.environment):
            before = module._git("status", "--porcelain", project_root=self.data)
            (self.data / "unrelated.py").write_text("VALUE = 999\n")
            self.assertTrue(git(self.data, "status", "--porcelain"))
            self.assertEqual(before, module._git("status", "--porcelain", project_root=self.data))
            (self.source / "code.py").write_text("VALUE = 2\n")
            self.assertNotEqual(before, module._git("status", "--porcelain", project_root=self.data))


if __name__ == "__main__":
    unittest.main()
