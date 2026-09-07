"""Verified remote bundle reuse must preserve sealed source and independent files."""

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from tests import test_macos_snapshot_publisher as baseline
from tests import test_macos_snapshot_publisher_recovery as recovery

publisher = baseline.publisher


class BundleBasisTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "incoming"
        self.previous_id = "20260906T010000Z-" + "a" * 12
        self.target_id = "20260907T010000Z-" + "b" * 12
        self.basis = self.root / self.previous_id / "bundle"
        (self.basis / "databases").mkdir(parents=True)
        self.database = self.basis / "databases/dcar_insight.sqlite3"
        self.database.write_bytes(b"verified previous database")
        self.manifest = {
            "snapshot_id": self.previous_id, "runtime_identity": {"schema": 20},
            "snapshot_contract": {"schema": "fixture"}, "databases": [{
                "name": self.database.name, "bundle_path": "databases/" + self.database.name,
                "sha256": baseline.sha(self.database), "byte_size": self.database.stat().st_size,
            }],
        }
        self.previous = {key: self.manifest[key] for key in (
            "snapshot_id", "runtime_identity", "snapshot_contract")}
        self.previous["database_sha256"] = baseline.sha(self.database)
        self.seal_manifest()
        self.config = SimpleNamespace(remote_python=sys.executable, remote_incoming_root=str(self.root))

    def seal_manifest(self):
        path = self.basis / "manifest.json"
        path.write_text(json.dumps(self.manifest))
        self.previous["manifest_sha256"] = baseline.sha(path)
        (self.basis / "manifest.sha256").write_text(baseline.sha(path) + "  manifest.json\n")

    def verify(self):
        def local_boundary(args, **kwargs):
            return subprocess.run(shlex.split(args[-1]), **kwargs)
        return publisher._verified_bundle_basis(self.config, ["ssh", "fixture"],
            runner=local_boundary, previous_remote=self.previous, snapshot_id=self.target_id)

    def test_uses_only_receipt_bound_previous_bundle_without_mutation(self):
        before = {str(p): p.read_bytes() for p in self.basis.rglob("*") if p.is_file()}
        self.assertEqual(self.verify(), str(self.basis))
        self.assertEqual(before, {str(p): p.read_bytes() for p in self.basis.rglob("*") if p.is_file()})
        self.assertFalse((self.root / self.target_id).exists())

    def test_missing_basis_fails_closed(self):
        shutil.rmtree(self.basis)
        with self.assertRaises(publisher.SnapshotPublishError):
            self.verify()

    def test_conflicting_manifest_fails_closed(self):
        (self.basis / "manifest.json").write_text("{}")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "manifest hash differs"):
            self.verify()

    def test_corrupt_database_fails_closed(self):
        self.database.write_bytes(b"corrupt previous database")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "database hash differs"):
            self.verify()

    def test_manifest_seal_conflict_fails_closed(self):
        (self.basis / "manifest.sha256").write_text("0" * 64 + "  manifest.json\n")
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "manifest seal differs"):
            self.verify()

    def test_previous_identity_conflict_fails_closed(self):
        self.previous["runtime_identity"] = {"schema": 19}
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "active identity differs"):
            self.verify()

    def test_same_snapshot_is_never_a_basis(self):
        self.target_id = self.previous_id
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "equals target"):
            self.verify()

    def test_symlink_hardlink_and_writable_basis_fail_closed(self):
        content = self.database.read_bytes()
        outside = Path(self.temporary.name) / "outside"
        outside.write_bytes(content)
        for kind in ("symlink", "hardlink", "writable"):
            with self.subTest(kind=kind):
                self.database.unlink()
                if kind == "symlink":
                    self.database.symlink_to(outside)
                elif kind == "hardlink":
                    os.link(outside, self.database)
                else:
                    self.database.write_bytes(content)
                    self.database.chmod(0o666)
                with self.assertRaisesRegex(publisher.SnapshotPublishError, "unsafe"):
                    self.verify()

    @unittest.skipUnless(shutil.which("rsync"), "rsync unavailable")
    def test_copy_dest_produces_complete_independent_files_with_full_hashes(self):
        source = Path(self.temporary.name) / "source"
        target = Path(self.temporary.name) / "target"
        shutil.copytree(self.basis, source)
        original = self.database.read_bytes()
        (source / "databases/dcar_insight.sqlite3").write_bytes(original + b"new rows")
        subprocess.run(["rsync", "-a", "--checksum", "--no-whole-file",
            "--copy-dest=" + self.verify(), "--delay-updates", str(source) + "/", str(target) + "/"],
            check=True, capture_output=True, timeout=30)
        for p in source.rglob("*"):
            if p.is_file():
                copied = target / p.relative_to(source)
                prior = self.basis / p.relative_to(source)
                self.assertEqual(baseline.sha(p), baseline.sha(copied))
                self.assertNotEqual(prior.stat().st_ino, copied.stat().st_ino)
                self.assertEqual(copied.stat().st_nlink, 1)
        self.assertEqual(self.database.read_bytes(), original)


class DeltaPublicationTests(unittest.TestCase):
    setUp = recovery.ResumeTests.setUp
    config = recovery.ResumeTests.config
    runner = recovery.ResumeTests.runner
    builder = recovery.ResumeTests.builder
    write_manifest = recovery.ResumeTests.write_manifest
    staged_resume = recovery.ResumeTests.staged_resume
    resume_local = recovery.ResumeTests.resume_local
    publish = baseline.MacOSSnapshotPublisherTest.publish

    def test_new_publication_also_uses_verified_basis(self):
        runner = self.runner()
        receipt = self.publish(runner=runner)
        self.assertTrue(receipt["bundle_delta_basis"].endswith("/bundle"))
        self.assertTrue(any(a[0] == "rsync" and "--dry-run" not in a
                            and "--no-whole-file" in a for a in runner.commands))

    def test_complete_staged_resume_keeps_existing_no_transfer_contract(self):
        _, manifest, runner = self.staged_resume()
        receipt = self.publish(runner=runner, resume_staged_snapshot_id=manifest["snapshot_id"])
        self.assertIsNone(receipt["bundle_delta_basis"])
        self.assertEqual(receipt["staging_bytes"], 0)
        self.assertFalse(any(a[0] == "rsync" and "--dry-run" not in a for a in runner.commands))
        self.assertFalse(any("verified-active-bundle-delta-basis" in " ".join(a) for a in runner.commands))

    def test_resume_uses_verified_previous_basis_and_keeps_source_seal(self):
        output, manifest, runner = self.staged_resume()
        before = {name: baseline.sha(output / name) for name in (
            "manifest.json", "manifest.sha256", publisher.SOURCE_RECEIPT_FILENAME,
            "databases/dcar_insight.sqlite3")}
        receipt = self.resume_local(manifest, runner)
        bundle = next(a for a in runner.commands if a[0] == "rsync"
                      and "--dry-run" not in a and any(v.startswith("--copy-dest=") for v in a))
        self.assertIn("--no-whole-file", bundle)
        self.assertIn("--checksum", bundle)
        self.assertIn("--copy-dest=" + self.config().remote_incoming_root
                      + "/20260828T010000Z-" + "b" * 12 + "/bundle", bundle)
        self.assertNotIn("--inplace", bundle)
        self.assertFalse(any("--link-dest" in v or "--compare-dest" in v for v in bundle))
        for name, digest in before.items():
            self.assertEqual(digest, baseline.sha(output / name))
        self.assertEqual(receipt["manifest_sha256"], before["manifest.json"])
        self.assertEqual(sum(" verify --bundle " in " ".join(a) for a in runner.commands), 1)
        self.assertEqual(sum(" install --bundle " in " ".join(a) for a in runner.commands), 1)

    def test_local_copy_reserves_complete_bundle_even_when_transfer_is_zero(self):
        _, manifest, runner = self.staged_resume()
        def no_bundle_transfer(args, **kwargs):
            if args[0] == "rsync" and "--dry-run" in args and any(a.startswith("--copy-dest=") for a in args):
                return subprocess.CompletedProcess(args, 0, stdout="Total transferred file size: 0 bytes\n", stderr="")
            return runner(args, **kwargs)
        receipt = self.resume_local(manifest, no_bundle_transfer)
        self.assertEqual(receipt["rsync_dry_run_bundle_bytes"], 0)
        self.assertEqual(receipt["staging_bytes"], receipt["rsync_dry_run_transfer_bytes"] + receipt["bundle_bytes"])
        self.assertEqual(receipt["required_remote_bytes"], receipt["staging_bytes"]
                         + receipt["install_headroom"]["total_bytes"] + self.config().minimum_remote_free_bytes)

    def test_bad_remote_basis_stops_before_transport_and_install(self):
        _, manifest, runner = self.staged_resume()
        def corrupt_basis(args, **kwargs):
            if "verified-active-bundle-delta-basis" in " ".join(args):
                return subprocess.CompletedProcess(args, 1, stdout="", stderr="bundle delta basis database hash differs")
            return runner(args, **kwargs)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, "database hash differs"):
            self.resume_local(manifest, corrupt_basis)
        self.assertFalse(any(a[0] == "rsync" for a in runner.commands))
        self.assertFalse(any(" install --bundle " in " ".join(a) for a in runner.commands))


if __name__ == "__main__":
    unittest.main()
