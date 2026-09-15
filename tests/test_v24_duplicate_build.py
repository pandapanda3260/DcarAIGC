"""Full direct graph migration, resumability and same-inode rollback."""
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest.mock import patch

from tests.test_v24_duplicate_index import add_fingerprint, AT
from tests.test_v24_duplicate_integration import calibrate
from v8 import duplicate_index as index, schema_v24
from v8.duplicate_index_build import build_existing_fingerprints
from v8.storage import connect, initialize_database


class DuplicateBuildTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'candidate.sqlite3'
        self.connection = connect(self.path); self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=23)
        calibrate(self.connection)
        self.ids = []
        for media in (['A'], ['A', 'C'], ['C'], ['isolated']):
            cid, fid = add_fingerprint(self.connection, media=media, publish=False)
            state = index.source_current(self.connection, cid)
            self.connection.execute('UPDATE duplicate_fingerprints SET source_sha256=? WHERE id=?', (state['source_sha256'], fid))
            self.ids.append(cid)
        self.connection.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
            "VALUES(?,?,'text_sha256',1,'{}','confirmed',?)", (self.ids[1], self.ids[0], AT))
        self.connection.commit()
        self.backup = Path(self.temp.name) / 'before.sqlite3'
        with sqlite3.connect(self.backup) as original: self.connection.backup(original)
        self.backup.chmod(0o600)
        self.before_identity = self.path.stat().st_ino
        schema_v24.migrate(self.connection)

    def test_build_stores_actual_chain_edges_and_canonical_projection(self):
        report = build_existing_fingerprints(self.connection)
        self.assertEqual(report['provider_calls'], 0); self.assertEqual(report['fingerprints_regenerated'], 0)
        self.assertEqual(report['counts']['duplicate_match_edges'], 2)
        self.assertEqual(report['counts']['duplicate_components'], 1)
        edges = [tuple(row) for row in self.connection.execute('SELECT left_content_id,right_content_id FROM duplicate_match_edges ORDER BY 1,2')]
        self.assertEqual(edges, [(self.ids[0], self.ids[1]), (self.ids[1], self.ids[2])])
        projected = self.connection.execute("SELECT original_content_id,evidence_json FROM duplicate_relations WHERE duplicate_content_id=? AND method='fingerprint_v1'", (self.ids[2],)).fetchone()
        self.assertEqual(projected[0], self.ids[0])
        evidence = json.loads(projected[1]); self.assertNotIn('cluster_members', evidence)
        self.assertEqual(evidence['best_edge']['left'], self.ids[1])
        self.assertEqual(len(report['removed_legacy_text_relation_ids']), 1)
        self.assertEqual(build_existing_fingerprints(self.connection), report)

    def test_resume_rejects_changed_business_facts(self):
        def interrupt(value):
            if value['phase'] == 'postings': raise RuntimeError('simulated process stop')
        with self.assertRaisesRegex(RuntimeError, 'simulated'):
            build_existing_fingerprints(self.connection, progress=interrupt)
        self.connection.execute("UPDATE content_items SET title='different' WHERE id=?", (self.ids[0],)); self.connection.commit()
        with self.assertRaisesRegex(ValueError, 'business inputs changed'):
            build_existing_fingerprints(self.connection)
        self.assertIsNone(index.active_generation(self.connection))

    def test_unpassed_calibration_cannot_activate_a_generation(self):
        self.connection.execute("UPDATE duplicate_calibration_runs SET status='failed'")
        self.connection.commit()
        with self.assertRaisesRegex(ValueError, 'calibration has not passed'):
            build_existing_fingerprints(self.connection)
        self.assertIsNone(index.active_generation(self.connection))

    def test_resume_after_posting_and_edge_checkpoints(self):
        phases = set()
        def interrupt(value):
            if value['phase'] not in phases:
                phases.add(value['phase']); raise RuntimeError('stop after committed checkpoint')
        for _ in range(2):
            with self.assertRaisesRegex(RuntimeError, 'committed checkpoint'):
                build_existing_fingerprints(self.connection, progress=interrupt)
        report = build_existing_fingerprints(self.connection)
        self.assertEqual(report['counts']['duplicate_match_edges'], 2)
        self.assertEqual(len(self.connection.execute('SELECT * FROM duplicate_fingerprints').fetchall()), 4)

    def test_before_traffic_rollback_preserves_inode_and_all_old_rows(self):
        scripts = str(Path(__file__).resolve().parents[1] / 'scripts')
        with patch.object(sys, 'path', [scripts, *sys.path]):
            from install_duplicate_index_release import restore_database_before_traffic, connection_at, identity
        expected_identity = identity(self.path)
        report = build_existing_fingerprints(self.connection)
        original = connection_at(self.backup, read_only=True); self.addCleanup(original.close)
        result = restore_database_before_traffic(self.connection, original, expected_identity=expected_identity, database=self.path,
            allowed_text_deletions=report['removed_legacy_text_relation_ids'])
        self.assertTrue(result['all_original_tables_verified'])
        self.assertEqual(self.path.stat().st_ino, self.before_identity)
        self.assertEqual(self.connection.execute('PRAGMA user_version').fetchone()[0], 23)

    def test_rollback_refuses_new_business_data(self):
        scripts = str(Path(__file__).resolve().parents[1] / 'scripts')
        with patch.object(sys, 'path', [scripts, *sys.path]):
            from install_duplicate_index_release import restore_database_before_traffic, connection_at, identity
        build_existing_fingerprints(self.connection)
        self.connection.execute("UPDATE content_items SET body='new user data' WHERE id=?", (self.ids[0],)); self.connection.commit()
        original = connection_at(self.backup, read_only=True); self.addCleanup(original.close)
        with self.assertRaisesRegex(ValueError, 'business data advanced'):
            restore_database_before_traffic(self.connection, original, expected_identity=identity(self.path), database=self.path)
        self.assertEqual(self.connection.execute('PRAGMA user_version').fetchone()[0], 24)

    def test_rollback_preserves_new_manual_relation_and_rejects_started_writer(self):
        scripts = str(Path(__file__).resolve().parents[1] / 'scripts')
        with patch.object(sys, 'path', [scripts, *sys.path]):
            from install_duplicate_index_release import restore_database_before_traffic, connection_at, identity
        from v8.duplicate_index_release import mark_traffic_started
        report = build_existing_fingerprints(self.connection)
        original = connection_at(self.backup, read_only=True); self.addCleanup(original.close)
        self.connection.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) "
            "VALUES(?,?,'manual',1,'{}','confirmed',?)", (self.ids[3], self.ids[0], AT)); self.connection.commit()
        with self.assertRaisesRegex(ValueError, 'non-fingerprint duplicate relations'):
            restore_database_before_traffic(self.connection, original, expected_identity=identity(self.path), database=self.path,
                allowed_text_deletions=report['removed_legacy_text_relation_ids'])
        self.assertEqual(self.connection.execute("SELECT COUNT(*) FROM duplicate_relations WHERE method='manual'").fetchone()[0], 1)
        mark_traffic_started(self.connection)
        writes = self.connection.total_changes
        mark_traffic_started(self.connection)
        self.assertEqual(self.connection.total_changes, writes)
        with self.assertRaisesRegex(ValueError, 'traffic already started'):
            restore_database_before_traffic(self.connection, original, expected_identity=identity(self.path), database=self.path,
                allowed_text_deletions=report['removed_legacy_text_relation_ids'])
        self.assertEqual(self.connection.execute('PRAGMA user_version').fetchone()[0], 24)



class DuplicateMergedBuildTest(unittest.TestCase):
    def setUp(self):
        from v8 import operations
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'merged-before-upgrade.sqlite3'
        self.c = connect(self.path); self.addCleanup(self.c.close)
        initialize_database(self.c, target_version=23)
        calibrate(self.c)
        self.ids = []
        for media in (['A'], ['A', 'C'], ['C'], ['A']):
            cid, fid = add_fingerprint(self.c, media=media, publish=False)
            source = index.source_current(self.c, cid)
            self.c.execute('UPDATE duplicate_fingerprints SET source_sha256=? WHERE id=?', (source['source_sha256'], fid))
            self.ids.append(cid)
        self.winner, self.loser = self.ids[:2]
        operations._merge_content_records_schema20(self.c,
            self.c.execute('SELECT * FROM content_items WHERE id=?', (self.winner,)).fetchone(),
            self.c.execute('SELECT * FROM content_items WHERE id=?', (self.loser,)).fetchone(), histories={})
        # An alias without its immutable event and confirmed relation is not a
        # logical deletion. It must retain ordinary fingerprint processing.
        link = self.c.execute('SELECT link_id FROM content_items WHERE id=?', (self.ids[3],)).fetchone()[0]
        self.c.execute("INSERT INTO content_aliases(alias_link_id,content_id,reason,created_at) VALUES (?,?,'fixture_alias_only',?)", (link, self.winner, AT))
        self.c.commit()
        self.assertIsNotNone(index.source_current(self.c, self.loser)['fingerprint_id'])

    def assert_merged_tombstone(self, *, revision=1):
        c = self.c
        self.assertIsNotNone(c.execute('SELECT id FROM content_items WHERE id=?', (self.loser,)).fetchone())
        self.assertIsNone(c.execute('SELECT fingerprint_id FROM duplicate_current_fingerprints WHERE content_id=?', (self.loser,)).fetchone())
        work = c.execute('SELECT reason,target_fingerprint_id,target_input_revision,status,completed_input_revision FROM duplicate_dirty_work WHERE content_id=?', (self.loser,)).fetchone()
        self.assertEqual(tuple(work), ('content_merged', None, revision, 'ready', revision))
        self.assertEqual(index.ready_status(c, [self.loser])[self.loser]['relation_status'], 'ready')
        self.assertFalse(index.ready_status(c, [self.loser])[self.loser]['fingerprint_available'])
        self.assertEqual(c.execute('SELECT count(*) FROM duplicate_match_edges WHERE left_content_id=? OR right_content_id=?', (self.loser, self.loser)).fetchone()[0], 0)
        self.assertEqual(c.execute('SELECT count(*) FROM duplicate_component_members WHERE content_id=?', (self.loser,)).fetchone()[0], 0)
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_relations WHERE method='fingerprint_v1' AND (duplicate_content_id=? OR original_content_id=?)", (self.loser, self.loser)).fetchone()[0], 0)
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_relations WHERE method='identity_merge' AND duplicate_content_id=?", (self.loser,)).fetchone()[0], 1)
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_current_fingerprints WHERE input_status='available'").fetchone()[0], 3)
        self.assertIsNotNone(c.execute('SELECT fingerprint_id FROM duplicate_current_fingerprints WHERE content_id=?', (self.ids[3],)).fetchone())

    def test_schema23_logical_merge_does_not_revive_on_offline_rebuild(self):
        schema_v24.migrate(self.c)
        report = build_existing_fingerprints(self.c)
        self.assert_merged_tombstone()
        self.assertEqual(report['current_fingerprints'], 3)
        self.assertEqual(report['merged_content_tombstones'], 1)

    def test_resume_keeps_one_tombstone_revision_and_never_prepares_merged_source(self):
        schema_v24.migrate(self.c)
        def interrupt(value):
            if value['phase'] == 'postings':
                raise RuntimeError('stop after merged tombstone checkpoint')
        with self.assertRaisesRegex(RuntimeError, 'tombstone checkpoint'):
            build_existing_fingerprints(self.c, progress=interrupt)
        work = self.c.execute('SELECT reason,target_input_revision,status FROM duplicate_dirty_work WHERE content_id=?', (self.loser,)).fetchone()
        self.assertEqual(tuple(work), ('content_merged', 1, 'pending'))
        original = index.source_current
        def checked(connection, cid, **kwargs):
            self.assertNotEqual(cid, self.loser)
            return original(connection, cid, **kwargs)
        with patch.object(index, 'source_current', side_effect=checked):
            report = build_existing_fingerprints(self.c)
            self.assertEqual(build_existing_fingerprints(self.c), report)
        self.assert_merged_tombstone()

    def test_immutable_event_and_alias_without_matching_relation_do_not_delete_content(self):
        event = self.c.execute('SELECT id FROM content_identity_merge_events WHERE loser_content_id=?', (self.loser,)).fetchone()[0]
        self.c.execute("UPDATE duplicate_relations SET evidence_json=? WHERE method='identity_merge' AND duplicate_content_id=?",
            (json.dumps({'merge_event_id': str(event)}), self.loser))
        self.c.commit()
        schema_v24.migrate(self.c)
        report = build_existing_fingerprints(self.c)
        self.assertEqual(report['merged_content_tombstones'], 0)
        self.assertEqual(report['current_fingerprints'], 4)
        self.assertEqual(self.c.execute('SELECT input_status FROM duplicate_current_fingerprints WHERE content_id=?', (self.loser,)).fetchone()[0], 'available')

    def test_resume_cleans_a_previously_indexed_merge_loser_and_its_direct_edges(self):
        schema_v24.migrate(self.c)
        generation = index.create_generation(self.c)
        gid = generation['generation_id']
        for cid in self.ids:
            state = index.source_current(self.c, cid)
            index.index_fingerprint(self.c, content_id=cid, fingerprint_id=state['fingerprint_id'],
                source_sha256=state['source_sha256'], generation_id=gid)
        raw = index.read_current_fingerprints(self.c, generation_id=gid)
        result = index.compare_prepared(index.prepare_fingerprint(raw[self.winner]), index.prepare_fingerprint(raw[self.loser]))
        self.c.execute('INSERT INTO duplicate_match_edges VALUES(?,?,?,?,?,?,?,?,?,1)',
            (gid, self.winner, self.loser, raw[self.winner]['fingerprint_id'], raw[self.loser]['fingerprint_id'], 1, 1,
             result['confidence'], json.dumps(result, sort_keys=True, separators=(',', ':'))))
        self.c.commit()
        report = build_existing_fingerprints(self.c)
        self.assertEqual(report['merged_content_tombstones'], 1)
        self.assert_merged_tombstone(revision=2)
