from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location(
    "cleanup_account_media", Path(__file__).resolve().parents[1] / "scripts/cleanup_account_media.py")
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)


class MediaCleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "data/cache/old").mkdir(parents=True)
        (self.root / "data/cache/keep").mkdir()
        (self.root / "reports").mkdir()
        self.old = self.root / "data/cache/old/video.bin"
        self.keep = self.root / "data/cache/keep/video.bin"
        self.old.write_bytes(b"old evidence")
        self.keep.write_bytes(b"retained evidence")
        self.source = self.root / "before.sqlite3"
        self.target = self.root / "installed.sqlite3"
        with sqlite3.connect(self.source) as c:
            c.executescript("""
                CREATE TABLE accounts(id INTEGER PRIMARY KEY);
                CREATE TABLE content_items(id INTEGER PRIMARY KEY, account_id INTEGER);
                CREATE TABLE evidence_artifacts(id INTEGER PRIMARY KEY,content_id INTEGER,
                    artifact_type TEXT,local_path TEXT,metadata_json TEXT);
                CREATE TABLE auxiliary(path TEXT, metadata_json TEXT);
                CREATE TABLE events(metadata_json TEXT);
                INSERT INTO accounts VALUES(1),(2);
                INSERT INTO content_items VALUES(10,1),(20,2);
                INSERT INTO evidence_artifacts VALUES
                    (1,10,'media','data/cache/old/video.bin','{}'),
                    (2,20,'media','data/cache/keep/video.bin','{}');
            """)
        with sqlite3.connect(self.source) as c, sqlite3.connect(self.target) as t:
            c.backup(t)
            t.executescript("DELETE FROM evidence_artifacts WHERE content_id=10;"
                            "DELETE FROM content_items WHERE id=10; DELETE FROM accounts WHERE id=1;")
        self.scope = {"deleted_content_ids": [10], "deleted_account_ids": [1],
                      "retained_content_ids": [20], "retained_account_ids": [2]}

    def plan(self):
        return cli.make_plan(self.source, self.target, self.scope, self.root)

    def save(self, plan):
        path = self.root / "manifest.json"
        cli.write_json(path, plan)
        return path, hashlib.sha256(path.read_bytes()).hexdigest()

    def apply(self, plan):
        manifest, sha = self.save(plan)
        return cli.apply_plan(manifest, sha, self.target, self.root, self.root / "progress.jsonl")

    def add_artifact(self, database, content, path, kind="frames_manifest"):
        with sqlite3.connect(database) as c:
            c.execute("INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,metadata_json) VALUES(?,?,?,'{}')",
                      (content, kind, str(path.relative_to(self.root))))

    def test_apply_is_bounded_and_idempotent(self):
        before = self.target.read_bytes()
        plan = self.plan()
        manifest, sha = self.save(plan)
        journal = self.root / "progress.jsonl"
        first = cli.apply_plan(manifest, sha, self.target, self.root, journal)
        second = cli.apply_plan(manifest, sha, self.target, self.root, journal)
        self.assertEqual(first, second)
        self.assertEqual(first["counts"], {"unlinked": 1})
        self.assertFalse(self.old.exists())
        self.assertTrue(self.keep.exists())
        self.assertEqual(before, self.target.read_bytes())

    def test_changed_file_is_skipped(self):
        plan = self.plan()
        self.old.write_bytes(b"replacement evidence")
        result = self.apply(plan)
        self.assertEqual(result["counts"], {"changed_or_unsafe": 1})
        self.assertTrue(self.old.exists())

    def test_manifest_children_and_retained_cross_reference(self):
        frame = self.old.parent / "frame.jpg"
        other = self.old.parent / "unregistered.jpg"
        frame.write_bytes(b"frame")
        other.write_bytes(b"unregistered should stay")
        manifest = self.old.parent / "frames.json"
        manifest.write_text(json.dumps({"frames": [{"path": "frame.jpg"}]}))
        self.add_artifact(self.source, 10, manifest)
        retained = self.keep.parent / "frames.json"
        retained.write_text(json.dumps({"frames": [{"path": str(frame)}]}))
        self.add_artifact(self.target, 20, retained)
        self.apply(self.plan())
        self.assertTrue(frame.exists())
        self.assertTrue(other.exists())
        self.assertTrue(retained.exists())
        self.assertFalse(manifest.exists())

    def test_explicit_manifest_child_can_be_removed_without_recursive_walk(self):
        child = self.old.parent / "frame.jpg"
        child.write_bytes(b"frame")
        manifest = self.old.parent / "frames.json"
        manifest.write_text(json.dumps({"frames": [{"path": "frame.jpg"}]}))
        self.add_artifact(self.source, 10, manifest)
        plan = self.plan()
        self.assertIn(str(child), [e["path"] for e in plan["entries"]])
        self.apply(plan)
        self.assertFalse(child.exists())

    def test_retained_directory_and_metadata_protect_descendants(self):
        with sqlite3.connect(self.target) as c:
            c.execute("INSERT INTO auxiliary VALUES(NULL,?)", (json.dumps({"cache_dir": str(self.old.parent)}),))
        plan = self.plan()
        self.assertEqual(plan["entries"], [])
        self.assertEqual(plan["skipped"][0]["reason"], "retained_reference_or_directory")

    def test_new_reference_or_id_change_blocks_apply(self):
        plan = self.plan()
        with sqlite3.connect(self.target) as c:
            c.execute("INSERT INTO auxiliary VALUES(?,NULL)", (str(self.old),))
        with self.assertRaises(cli.Unsafe):
            self.apply(plan)
        self.assertTrue(self.old.exists())

    def test_unrelated_runtime_event_does_not_change_reference_binding(self):
        plan = self.plan()
        with sqlite3.connect(self.target) as c:
            c.execute("INSERT INTO events VALUES(?)", (json.dumps({"attempt": 1, "status": "success"}),))
        self.assertEqual(self.apply(plan)["counts"], {"unlinked": 1})

    def test_hardlink_and_parent_symlink_never_deleted(self):
        hardlink = self.root / "saved.bin"
        os.link(self.old, hardlink)
        self.assertEqual(self.plan()["entries"], [])
        hardlink.unlink()
        self.old.unlink()
        self.old.parent.rmdir()
        self.old.parent.symlink_to(self.keep.parent, target_is_directory=True)
        plan = self.plan()
        self.assertEqual(plan["entries"], [])
        self.assertTrue(self.keep.exists())

    def test_traversal_and_manifest_hash_mismatch_rejected(self):
        b = cli.Boundary(self.root)
        for bad in ("data/cache/../outside", str(self.root / "unrelated"), "/etc/passwd"):
            with self.assertRaises(cli.Unsafe):
                b.path(bad)
        manifest, _ = self.save(self.plan())
        with self.assertRaises(cli.Unsafe):
            cli.apply_plan(manifest, "0" * 64, self.target, self.root, self.root / "journal")
        self.assertTrue(self.old.exists())

    def test_unreadable_retained_manifest_fails_closed(self):
        manifest = self.keep.parent / "frames.json"
        manifest.write_text("broken JSON")
        self.add_artifact(self.target, 20, manifest)
        with self.assertRaises(cli.Unsafe):
            self.plan()

    def test_existing_nonjournal_is_not_modified(self):
        manifest, sha = self.save(self.plan())
        journal = self.root / "private-note.txt"
        journal.write_bytes(b"do not truncate")
        with self.assertRaises(cli.Unsafe):
            cli.apply_plan(manifest, sha, self.target, self.root, journal)
        self.assertEqual(journal.read_bytes(), b"do not truncate")
        self.assertTrue(self.old.exists())

    def test_resumes_after_unlink_before_result(self):
        plan = self.plan()
        manifest, sha = self.save(plan)
        journal = self.root / "progress.jsonl"
        with journal.open("wb") as f:
            cli.emit(f, {"event": "header", "version": cli.VERSION, "manifest_sha256": sha,
                         "scope_sha256": plan["scope_sha256"]})
            cli.emit(f, {"event": "intent", "indices": [0]})
            f.write(b"{partial-result")
        self.old.unlink()
        result = cli.apply_plan(manifest, sha, self.target, self.root, journal)
        self.assertEqual(result["counts"], {"already_missing": 1})
        self.assertEqual(result["observed_unlinked_allocated_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
