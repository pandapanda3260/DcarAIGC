"""Offline black-box checks for the file-only snapshot thumbnail reader."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

from tests import test_web_thumbnail_deployment_contract as deployment_fixtures


ROOT = Path(__file__).resolve().parents[1]


class ThumbnailStreamSecurityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = deployment_fixtures.ReplicaThumbnailRelocationTestCase(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.manifest = json.loads(self.fixture.manifest.read_text())

    def seal(self, body: bytes | None = None) -> None:
        body = body if body is not None else json.dumps(self.manifest, separators=(",", ":")).encode()
        self.fixture.manifest.write_bytes(body)
        receipt = json.loads(self.fixture.receipt.read_text())
        receipt["manifest_sha256"] = hashlib.sha256(body).hexdigest()
        self.fixture.receipt.write_text(json.dumps(receipt))

    def assert_unavailable(self) -> None:
        result = self.fixture.run_reader()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(json.loads(result.stdout), {"error": "thumbnail_projection_unavailable"})

    def assert_authorized_cover(self) -> None:
        result = self.fixture.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["items"]["1"]["remote_url"], self.fixture.cover)

    def load_helper(self):
        spec = importlib.util.spec_from_file_location(
            "thumbnail_stream_security_helper", ROOT / "app/web/server/content_thumbnails.py"
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_valid_scalar_and_multibyte_utf8_values_cross_the_read_boundary(self) -> None:
        remaining = json.dumps(self.manifest, separators=(",", ":")).encode()[1:]
        for prefix, suffix, boundary_bytes in (
            (b'","scalar":', b'123456789,', 3),
            (b'","scalar":', b'123456.789,', 7),
            (b'","scalar":', b'123456e-12,', 7),
            (b'","scalar":', b'-123456e+12,', 9),
            (b'","unicode":"', '懂车帝",'.encode(), 1),
        ):
            with self.subTest(prefix=prefix):
                start = b'{"padding":"'
                padding = 64 * 1024 - len(start) - len(prefix) - boundary_bytes
                body = start + b"a" * padding + prefix + suffix + remaining
                json.loads(body)  # Independently confirm this is complete, valid JSON.
                self.seal(body)
                self.assert_authorized_cover()

    def test_signed_truncated_tail_is_rejected_even_after_all_requested_files(self) -> None:
        prefix = json.dumps(self.manifest, separators=(",", ":")).encode()[:-1]
        self.seal(prefix + b',"ignored":[1,2,{"incomplete":')
        self.assert_unavailable()

    def test_all_manifest_bytes_are_hashed_after_the_last_requested_file(self) -> None:
        self.manifest["ignored"] = "a" * (128 * 1024)
        self.seal()
        body = self.fixture.manifest.read_bytes()
        self.fixture.manifest.write_bytes(body[:-3] + b"b" + body[-2:])
        self.assert_unavailable()

    def test_repeated_top_level_or_nested_receipt_fields_are_rejected(self) -> None:
        original = json.dumps(self.manifest, separators=(",", ":")).encode()
        duplicate_top = b'{"files":[],' + original[1:]
        duplicate_nested = original.replace(b'"project_path":', b'"sha256":"' + b"0" * 64 + b'","project_path":', 1)
        for body in (duplicate_top, duplicate_nested):
            with self.subTest(body=body[:90]):
                self.seal(body)
                self.assert_unavailable()

    def test_duplicate_paths_across_required_and_optional_files_are_rejected(self) -> None:
        self.manifest["optional_reuse_files"] = [dict(self.manifest["files"][0])]
        self.seal()
        self.assert_unavailable()

    def test_invalid_unrequested_file_paths_invalidate_the_manifest(self) -> None:
        for name in ("/data/cache/escape.json", "data/cache/../escape.json", "data/cache//escape.json",
                     "data/cache/./escape.json", "data/cache\\escape.json", "data/cachex/escape.json"):
            with self.subTest(name=name):
                self.manifest["optional_reuse_files"] = [{
                    "project_path": name, "sha256": "a" * 64, "byte_size": 1,
                }]
                self.seal()
                self.assert_unavailable()

    def test_managed_originals_cannot_be_duplicated_into_the_file_allowlist(self) -> None:
        self.manifest["managed_originals"]["bundles"] = [{"members": [self.manifest["files"][0]]}]
        self.seal()
        self.assert_unavailable()

    def test_managed_original_members_never_authorize_thumbnail_raw_reads(self) -> None:
        row = self.manifest["files"].pop(0)
        self.manifest["managed_originals"]["bundles"] = [{"members": [row]}]
        self.seal()
        result = self.fixture.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIsNone(json.loads(result.stdout)["items"]["1"]["remote_url"])

    def test_parent_directory_symlink_cannot_relocate_an_authorized_file(self) -> None:
        original = self.fixture.project / "data/cache/v8/raw_responses"
        destination = original.with_name("moved_raw_responses")
        original.rename(destination)
        original.symlink_to(destination, target_is_directory=True)
        result = self.fixture.run_reader()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIsNone(json.loads(result.stdout)["items"]["1"]["remote_url"])

    def test_manifest_replacement_with_identical_bytes_during_read_is_rejected(self) -> None:
        from v8 import artifact_paths

        helper = self.load_helper()
        body = self.fixture.manifest.read_bytes()
        replacement = self.fixture.manifest.with_name("replacement.json")
        replacement.write_bytes(body)
        replacement.chmod(0o640)
        real_fdopen = helper.os.fdopen

        class ReplacingReader:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.stream.close()

            def read(inner, size):
                block = inner.stream.read(size)
                if not block and replacement.exists():
                    os.replace(replacement, self.fixture.manifest)
                return block

        with patch.object(helper.os, "fdopen", side_effect=lambda *args: ReplacingReader(real_fdopen(*args))):
            with self.assertRaises((helper.ReadError, artifact_paths.ArtifactPathError)):
                helper._snapshot_manifest(self.fixture.manifest, hashlib.sha256(body).hexdigest(), artifact_paths)

    def test_active_receipt_replacement_after_manifest_validation_is_rejected(self) -> None:
        from v8 import artifact_paths

        helper = self.load_helper()
        replacement = self.fixture.receipt.with_name("replacement.json")
        replacement.write_bytes(self.fixture.receipt.read_bytes())
        replacement.chmod(0o640)
        read_manifest = helper._snapshot_manifest

        def replace_receipt(*args):
            result = read_manifest(*args)
            os.replace(replacement, self.fixture.receipt)
            return result

        with patch.dict(os.environ, {"DCAR_ACTIVE_SNAPSHOT": str(self.fixture.receipt)}), \
                patch.object(helper, "_snapshot_manifest", side_effect=replace_receipt):
            with self.assertRaises((helper.ReadError, artifact_paths.ArtifactPathError)):
                helper._replica_context(self.fixture.project)


if __name__ == "__main__":
    unittest.main()
