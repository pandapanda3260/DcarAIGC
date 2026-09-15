from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import duplicate_index as index, duplicate_runtime as runtime, schema_v24
from v8.duplicates import FINGERPRINT_VERSION, THRESHOLDS
from v8.storage import connect, initialize_database, now_utc, transaction


class DuplicateRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "graph.sqlite3"
        with connect(self.db) as c:
            initialize_database(c, target_version=23)
            c.execute("BEGIN IMMEDIATE")
            schema_v24.create_tables(c)
            c.execute("PRAGMA user_version=24")
            self.gid = index.create_generation(c)["generation_id"]
            c.execute("UPDATE duplicate_index_generations SET state='ready' WHERE generation_id=?", (self.gid,))
            c.execute("INSERT INTO duplicate_calibration_runs(id,calibration_version,fingerprint_version,dataset_sha256,pair_count,positive_count,negative_count,"
                "predicted_positive_count,true_positive_count,false_positive_count,precision,recall,thresholds_json,status,created_at) "
                "VALUES('test','test',?,'test',150,75,75,75,75,0,1,1,?,'passed',?)",
                (FINGERPRINT_VERSION, json.dumps(THRESHOLDS, sort_keys=True, separators=(",", ":")), now_utc()))
            c.commit()

    def tearDown(self):
        self.temp.cleanup()

    def add(self, cid, media):
        with connect(self.db) as c, transaction(c):
            now = now_utc()
            c.execute("INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,title,body,published_at,imported_at,created_at,updated_at) "
                      "VALUES(?,?,'douyin',?,?,?,'',?,?,?,?)",
                      (cid, f"T{cid:05d}", str(cid), f"https://douyin.com/video/{cid}", str(cid), f"2026-01-{cid:02d}", now, now, now))
            self._index(c, cid, media)

    def _index(self, c, cid, media):
        source = hashlib.sha256(json.dumps([cid, media]).encode()).hexdigest()
        c.execute("INSERT OR IGNORE INTO duplicate_fingerprints(content_id,fingerprint_version,source_sha256,text_sha256,media_sha256_json,frame_phashes_json,payload_json,created_at) "
                  "VALUES(?,?,?,?,?,'[]','{}',?)", (cid, FINGERPRINT_VERSION, source, str(cid), json.dumps(media), now_utc()))
        fid = c.execute("SELECT id FROM duplicate_fingerprints WHERE content_id=? AND source_sha256=?", (cid, source)).fetchone()[0]
        return index.index_fingerprint(c, content_id=cid, fingerprint_id=fid, source_sha256=source)

    def update(self, cid, media):
        with connect(self.db) as c, transaction(c):
            index.invalidate_content(c, cid)
            self._index(c, cid, media)

    def drain(self, **kwargs):
        return runtime.drain_duplicate_work(db_path=self.db, **kwargs)

    def rows(self, sql, args=()):
        with connect(self.db) as c:
            return [dict(row) for row in c.execute(sql, args)]

    def chain(self):
        self.add(1, ["a"])
        self.add(2, ["a", "b"])
        self.add(3, ["b"])
        result = self.drain()
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(len(self.rows("SELECT * FROM duplicate_match_edges")), 2)

    def test_atomic_fingerprint_facade_and_repeated_ready_zero_write(self):
        from v8 import duplicates
        self.add(1, ["placeholder"])
        first = duplicates.run_duplicate_fingerprint_queue(db_path=self.db, scope_content_ids=[1], process_relations=False)
        self.assertEqual(first["processed"], 1, first)
        self.assertEqual(first["relation_status"], "pending", first)
        self.assertEqual(self.drain(scope_content_ids=[1])["relation_status"], "ready")
        with patch.object(duplicates, "transaction", side_effect=AssertionError("fingerprint replay wrote")):
            second = duplicates.refresh_content_duplicates(1, db_path=self.db)
            duplicates.fingerprint_content(1, db_path=self.db)
        self.assertEqual(second["relation_status"], "ready", second)
        self.assertTrue(second["source_sha256"])

    def test_source_changes_before_fingerprint_commit_reject_old_payload(self):
        from v8 import duplicates
        self.add(1, ["placeholder"])
        original = duplicates._run_processing_slot
        def mutate(**kwargs):
            artifact = original(**kwargs)
            with connect(self.db) as c, transaction(c):
                index.invalidate_content(c, 1)
                c.execute("UPDATE content_items SET title='a different input' WHERE id=1")
            return artifact
        with patch.object(duplicates, "_run_processing_slot", side_effect=mutate):
            with self.assertRaisesRegex(duplicates.DuplicateDetectionError, "source identity changed"):
                duplicates.fingerprint_content(1, db_path=self.db)
        self.assertEqual(self.rows("SELECT input_status FROM duplicate_current_fingerprints")[0]["input_status"], "unavailable")
        self.assertEqual(len(self.rows("SELECT * FROM duplicate_fingerprints")), 1)

    def test_media_evidence_change_dirties_same_transaction_and_unchanged_does_not(self):
        from v8.media import register_artifact
        self.chain()
        path = Path(self.temp.name) / "transcript.json"
        path.write_text('{"text":"first transcript"}')
        with connect(self.db) as c, transaction(c):
            register_artifact(c, content_id=2, artifact_type="asr", path=path, processor_version="test")
            self.assertEqual(c.execute("SELECT input_status FROM duplicate_current_fingerprints WHERE content_id=2").fetchone()[0], "unavailable")
            self.assertEqual(c.execute("SELECT state FROM duplicate_components").fetchone()[0], "dirty")
        revision = self.rows("SELECT input_revision FROM duplicate_current_fingerprints WHERE content_id=2")[0]["input_revision"]
        with connect(self.db) as c, transaction(c):
            register_artifact(c, content_id=2, artifact_type="asr", path=path, processor_version="test")
        self.assertEqual(self.rows("SELECT input_revision FROM duplicate_current_fingerprints WHERE content_id=2")[0]["input_revision"], revision)

    def test_retry_backoff_is_pending_even_with_no_eligible_batch(self):
        self.add(1, ["unique"])
        with patch.object(runtime.graph, "compute_graph_delta", side_effect=RuntimeError("temporary")):
            self.assertEqual(self.drain()["relation_status"], "pending")
        with patch.object(runtime, "transaction", side_effect=AssertionError("not due work wrote")):
            again = self.drain()
        self.assertEqual(again["relation_status"], "pending", again)
        self.assertTrue(again["has_more"])
        self.assertEqual(self.rows("SELECT attempt_count FROM duplicate_dirty_work")[0]["attempt_count"], 1)

    def test_negative_comparison_checkpoint_resumes_after_budget_in_new_call(self):
        for cid in range(1, 522):
            self.add(cid, [f"unique-{cid}"])
        with connect(self.db) as c, transaction(c):
            c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision WHERE content_id<>1")
        first = self.drain(scope_content_ids=[1], fallback_full_scan=True, time_budget_seconds=0.001)
        self.assertEqual(first["relation_status"], "pending", first)
        self.assertEqual(first["compared_pairs"], 512)
        self.assertEqual(len(self.rows("SELECT * FROM duplicate_work_staging WHERE record_type='comparison'")), 512)
        second = self.drain(scope_content_ids=[1], fallback_full_scan=True)
        self.assertEqual(second["relation_status"], "ready", second)
        self.assertEqual(second["compared_pairs"], 8)
        self.assertEqual(self.rows("SELECT * FROM duplicate_work_staging"), [])

    def test_completed_graph_staging_survives_owner_restart_without_rebuild(self):
        self.add(1, ["unique"])
        token = runtime._acquire(self.db, self.gid)
        with runtime._read(self.db) as c:
            works = runtime._eligible(c, self.gid, [1], 20)
            snapshot = runtime._match_snapshot(c, self.gid, works, False)
        computation = runtime.graph.compute_graph_delta(snapshot, works)
        runtime._persist_computation(self.db, snapshot, computation, token)
        with runtime._read(self.db) as c:
            delta = runtime._publication_snapshot(c, self.gid, 1)
        runtime._stage_graph_delta(delta, 1, token, self.db)
        runtime._release(self.db, self.gid, token)
        self.assertTrue(self.rows("SELECT * FROM duplicate_work_staging WHERE record_type='graph'"))
        with patch.object(runtime.graph, "build_component_delta", side_effect=AssertionError("staged graph rebuilt")):
            result = self.drain(scope_content_ids=[1])
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(self.rows("SELECT * FROM duplicate_work_staging"), [])

    def test_stale_staged_endpoint_resets_original_work_not_unrelated_retry_seed(self):
        self.chain()
        self.update(1, ["a"])
        self.update(2, ["a", "b"])
        self.assertEqual(self.drain(limit=1)["relation_status"], "pending")
        # Work 1 has a complete positive checkpoint bound to fingerprint B.
        # Work 2 changes before finishing; its publication must invalidate 1's
        # checkpoint as well, rather than endlessly retrying only work 2.
        self.update(2, ["new-b"])
        stale = self.drain(limit=1)
        self.assertEqual(stale["error_code"], "stale_comparison_checkpoint", stale)
        for _ in range(4):
            result = self.drain(limit=1)
            if result["relation_status"] == "ready":
                break
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(self.rows("SELECT * FROM duplicate_match_edges"), [])

    def test_date_only_change_reuses_verified_edges_and_reprojects_canonical(self):
        self.chain()
        with connect(self.db) as c, transaction(c):
            index.mark_content_dirty(c, 3)
            c.execute("UPDATE content_items SET published_at='2025-01-01' WHERE id=3")
        with patch.object(index, "compare_prepared", side_effect=AssertionError("date-only input was compared")):
            result = self.drain()
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(result["compared_pairs"], 0)
        self.assertEqual({row["original_content_id"] for row in self.rows("SELECT * FROM duplicate_relations")}, {3})
        self.assertEqual(len(self.rows("SELECT * FROM duplicate_match_edges")), 2)

    def test_batch_unions_two_new_seeds_joining_different_members_of_old_component(self):
        self.add(1, ["a", "b"])
        self.add(2, ["a"])
        self.assertEqual(self.drain()["relation_status"], "ready")
        self.add(3, ["b"])
        self.add(4, ["a"])
        result = self.drain(scope_content_ids=[3, 4])
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual({(row["left_content_id"], row["right_content_id"]) for row in self.rows("SELECT * FROM duplicate_match_edges")},
                         {(1, 2), (1, 3), (1, 4), (2, 4)})
        self.assertEqual(self.rows("SELECT member_count FROM duplicate_components"), [{"member_count": 4}])
        self.assertEqual({row["duplicate_content_id"] for row in self.rows("SELECT * FROM duplicate_relations")}, {2, 3, 4})

    def test_scoped_ready_has_more_ignores_unrelated_pending_inputs(self):
        self.add(1, ["one"])
        self.assertEqual(self.drain(scope_content_ids=[1])["relation_status"], "ready")
        self.add(2, ["two"])
        scoped = self.drain(scope_content_ids=[1])
        self.assertEqual(scoped["relation_status"], "ready")
        self.assertFalse(scoped["has_more"])
        self.assertTrue(self.drain(limit=0)["has_more"])

    def test_source_replacement_splits_real_chain(self):
        self.chain()
        self.update(2, ["different"])
        result = self.drain()
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(self.rows("SELECT * FROM duplicate_match_edges"), [])
        self.assertEqual(self.rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'"), [])
        self.assertEqual(self.rows("SELECT * FROM duplicate_components"), [])

    def test_multiple_dirty_members_do_not_publish_partial_component(self):
        self.chain()
        self.update(2, ["new-b"])
        self.update(3, ["new-c"])
        first = self.drain(limit=1)
        self.assertEqual(first["relation_status"], "pending", first)
        self.assertEqual(self.rows("SELECT state FROM duplicate_components")[0]["state"], "dirty")
        self.assertEqual(self.rows("SELECT status FROM duplicate_dirty_work WHERE content_id=2")[0]["status"], "pending")
        second = self.drain(limit=1)
        self.assertEqual(second["relation_status"], "ready", second)
        self.assertEqual(self.rows("SELECT * FROM duplicate_components"), [])

    def test_deleted_bridge_tombstone_removes_projection_and_is_acknowledged(self):
        self.chain()
        with connect(self.db) as c, transaction(c):
            index.invalidate_content(c, 2, deleted=True, reason="content_deleted")
            c.execute("DELETE FROM content_items WHERE id=2")
        result = self.drain()
        self.assertIsNone(result["error_code"], result)
        self.assertEqual(self.rows("SELECT * FROM duplicate_match_edges"), [])
        self.assertEqual(self.rows("SELECT * FROM duplicate_relations WHERE method='fingerprint_v1'"), [])
        self.assertEqual(self.rows("SELECT status FROM duplicate_dirty_work WHERE content_id=2")[0]["status"], "ready")

    def test_repeat_ready_request_acquires_no_write_transaction(self):
        self.add(1, ["alone"])
        self.assertEqual(self.drain()["relation_status"], "ready")
        with patch.object(runtime, "transaction", side_effect=AssertionError("ready replay wrote")):
            result = self.drain(scope_content_ids=[1])
        self.assertEqual(result["relation_status"], "ready", result)

    def test_restore_older_source_uses_explicit_current_pointer(self):
        self.add(1, ["A"])
        initial = self.rows("SELECT fingerprint_id FROM duplicate_current_fingerprints")[0]["fingerprint_id"]
        self.update(1, ["B"])
        self.update(1, ["A"])
        self.assertEqual(self.rows("SELECT fingerprint_id FROM duplicate_current_fingerprints")[0]["fingerprint_id"], initial)
        self.assertEqual(self.drain()["relation_status"], "ready")

    def test_full_scan_fallback_has_identical_graph(self):
        self.chain()
        before = self.rows("SELECT duplicate_content_id,original_content_id,evidence_json FROM duplicate_relations")
        with connect(self.db) as c, transaction(c):
            index.mark_content_dirty(c, 2)
        result = self.drain(fallback_full_scan=True)
        self.assertEqual(result["relation_status"], "ready", result)
        self.assertEqual(before, self.rows("SELECT duplicate_content_id,original_content_id,evidence_json FROM duplicate_relations"))

    def test_pointer_arriving_after_read_is_matched_by_later_durable_work(self):
        self.add(1, ["same"])
        original = runtime._persist_computation
        added = False
        def persist(*args, **kwargs):
            nonlocal added
            if not added:
                added = True
                self.add(2, ["same"])
            return original(*args, **kwargs)
        with patch.object(runtime, "_persist_computation", side_effect=persist):
            self.drain(scope_content_ids=[1])
        self.assertEqual(self.drain()["relation_status"], "ready")
        edges = self.rows("SELECT left_content_id,right_content_id FROM duplicate_match_edges")
        self.assertEqual(edges, [{"left_content_id": 1, "right_content_id": 2}])

    def test_expired_token_cannot_commit(self):
        self.add(1, ["alone"])
        token = runtime._acquire(self.db, self.gid)
        self.assertIsNotNone(token)
        self.assertIsNone(runtime._acquire(self.db, self.gid))
        with connect(self.db) as c, transaction(c):
            c.execute("UPDATE duplicate_index_generations SET lease_until='2000-01-01T00:00:00Z'")
        replacement = runtime._acquire(self.db, self.gid)
        self.assertNotEqual(token, replacement)
        with connect(self.db) as c:
            with self.assertRaises(runtime.DuplicateConflict):
                runtime._fence(c, self.gid, token)
        runtime._release(self.db, self.gid, token)
        self.assertEqual(self.rows("SELECT lease_token FROM duplicate_index_generations")[0]["lease_token"], replacement)
        runtime._release(self.db, self.gid, replacement)


if __name__ == "__main__":
    unittest.main()
