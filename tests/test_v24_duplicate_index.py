from __future__ import annotations

import itertools
import json
import random
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import duplicate_index as index
from v8 import schema_v24 as schema
from v8.duplicates import FINGERPRINT_VERSION
from v8.storage import connect, initialize_database

AT = "2026-09-15T00:00:00Z"


def create_schema24_fixture(path: Path, *, ready: bool = True) -> tuple[sqlite3.Connection, str]:
    """Shared offline fixture; historical default-schema fixtures stay untouched."""
    connection = connect(path)
    initialize_database(connection, target_version=23)
    schema.migrate(connection)
    generation = index.create_generation(connection, generation_id="fixture-v24")
    if ready:
        connection.execute("UPDATE duplicate_index_generations SET state='ready',activated_at=? WHERE generation_id=?", (AT, generation["generation_id"]))
    connection.commit()
    return connection, str(generation["generation_id"])


def add_fingerprint(connection: sqlite3.Connection, *, content_id: int | None = None,
                    source: str | None = None, frames: list[str] | None = None,
                    media: list[str] | None = None, text: str | None = None,
                    simhash: str | None = None, publish: bool = True,
                    generation_id: str | None = None, created_at: str = AT) -> tuple[int, int]:
    """Insert valid isolated content/fingerprint facts without media or paid calls."""
    if content_id is None:
        serial = connection.execute("SELECT COALESCE(MAX(id),0)+1 FROM content_items").fetchone()[0]
        link = f"{serial:06d}"
        content_id = connection.execute("""INSERT INTO content_items(link_id,platform,platform_content_id,
            canonical_url,title,body,published_at,imported_at,created_at,updated_at)
            VALUES(?,'douyin',?,?,?,'fixture body',?,?,?,?)""",
            (link, link, "https://www.douyin.com/video/" + link, "fixture title " + str(serial), AT, AT, AT, AT)).lastrowid
    source = source or f"{content_id:064x}"
    fingerprint_id = connection.execute("""INSERT INTO duplicate_fingerprints(content_id,fingerprint_version,
        source_sha256,text_sha256,media_sha256_json,frame_phashes_json,text_simhash,asr_simhash,ocr_simhash,
        text_char_count,asr_char_count,ocr_char_count,payload_json,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,100,0,0,'{}',?)""",
        (content_id, FINGERPRINT_VERSION, source, text, json.dumps(media or []), json.dumps(frames or []), simhash, None, None, created_at)).lastrowid
    if publish:
        index.index_fingerprint(connection, content_id=content_id, fingerprint_id=fingerprint_id,
            source_sha256=source, generation_id=generation_id)
    return int(content_id), int(fingerprint_id)


class DuplicateIndexTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "index.sqlite3"
        self.connection, self.generation_id = create_schema24_fixture(self.path)
        self.addCleanup(self.connection.close)

    def test_mih_all_distances_and_spread_bits_preserve_exact_radius(self):
        c = self.connection
        seed, _ = add_fingerprint(c, frames=["8000000000000000"])
        expected = set()
        for bit_count in range(8):
            mask = sum(1 << bit for bit in range(bit_count))
            cid, _ = add_fingerprint(c, frames=[f"{(1 << 63) ^ mask:016x}"])
            if bit_count <= 6:
                expected.add(cid)
        for positions in ((0, 1, 16, 17, 32, 48), (48, 49)):
            mask = sum(1 << bit for bit in positions)
            cid, _ = add_fingerprint(c, frames=[f"{(1 << 63) ^ mask:016x}"])
            expected.add(cid)
        c.commit()
        self.assertEqual(index.query_candidate_ids(c, [seed])[seed], expected)
        self.assertEqual(len(index.band_neighbors(0)), 17)
        self.assertIn(0, index.band_neighbors(0))

    def test_batched_seeds_exact_tokens_empty_frames_and_no_semantic_only(self):
        c = self.connection
        first, _ = add_fingerprint(c, frames=[], text=" old raw token ", media=["Token", "Token"], simhash="0" * 16)
        text, _ = add_fingerprint(c, frames=[], text=" old raw token ")
        media, _ = add_fingerprint(c, media=["Token"])
        other, _ = add_fingerprint(c, text="old raw token", media=["token"], simhash="0" * 16)
        c.commit()
        candidates = index.query_candidate_ids(c, [first, text, media, other])
        self.assertEqual(candidates[first], {text, media})
        self.assertIn(first, candidates[text])
        self.assertEqual(candidates[other], set())
        self.assertNotIn("payload_json", index.read_current_fingerprints(c, [first])[first])

    def test_blob_preserves_zero_high_bit_duplicate_ordinals_and_rejects_overflow(self):
        c = self.connection
        _, fid = add_fingerprint(c, frames=["0" * 16, "f" * 16, "f" * 16])
        c.commit()
        rows = c.execute("SELECT frame_ordinal,typeof(phash),phash,band0,band3 FROM duplicate_fingerprint_frames WHERE fingerprint_id=? ORDER BY frame_ordinal", (fid,)).fetchall()
        self.assertEqual([row[0] for row in rows], [0, 1, 2])
        self.assertEqual(rows[0][1:3], ("blob", b"\0" * 8))
        self.assertEqual(rows[1][2:], (b"\xff" * 8, 65535, 65535))
        self.assertEqual(index.validate_postings(c)["frame_postings"], 3)
        for invalid in ("-1", "10000000000000000", "oops"):
            with self.subTest(invalid=invalid), self.assertRaises(index.DuplicateIndexError):
                index.phash_to_blob(invalid)
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("UPDATE duplicate_fingerprint_frames SET phash='12345678' WHERE fingerprint_id=?", (fid,))
        c.rollback()

    def test_pointer_reuses_source_a_after_b_without_timestamp_selection(self):
        c = self.connection
        cid, a = add_fingerprint(c, source="a" * 64, text="source a", created_at="2026-01-01")
        _, b = add_fingerprint(c, content_id=cid, source="b" * 64, text="source b", created_at="2026-02-01")
        c.commit()
        c.execute("BEGIN")
        answer = index.index_fingerprint(c, content_id=cid, fingerprint_id=a, source_sha256="a" * 64,
            expected_input_revision=2)
        c.commit()
        self.assertEqual(answer["input_revision"], 3)
        self.assertEqual(index.read_current_fingerprints(c, [cid])[cid]["fingerprint_id"], a)
        self.assertNotEqual(a, b)
        with patch("v8.duplicates._current_source_state", return_value=({}, "a" * 64)):
            self.assertEqual(index.source_current(c, cid)["fingerprint_id"], a)

    def test_index_and_dirty_rollback_together_and_mismatched_identity_rejected(self):
        c = self.connection
        cid, fid = add_fingerprint(c, frames=["f" * 16], publish=False)
        c.commit()
        c.execute("BEGIN")
        index.index_fingerprint(c, content_id=cid, fingerprint_id=fid, source_sha256=f"{cid:064x}", expected_input_revision=0)
        c.rollback()
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_fingerprint_frames").fetchone()[0], 0)
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_dirty_work").fetchone()[0], 0)
        c.execute("BEGIN")
        with self.assertRaises(index.DuplicateInputChanged):
            index.index_fingerprint(c, content_id=cid, fingerprint_id=fid, source_sha256="stale")
        c.rollback()

    def test_source_invalidation_keeps_old_component_and_delete_tombstone(self):
        c = self.connection
        cid, _ = add_fingerprint(c)
        c.execute("INSERT INTO duplicate_components VALUES(?,?,?,1,'ready',1,?)", (self.generation_id, "old", cid, AT))
        c.execute("INSERT INTO duplicate_component_members VALUES(?,?,?)", (self.generation_id, cid, "old"))
        c.commit()
        c.execute("BEGIN")
        changed = index.invalidate_content(c, cid, deleted=True, reason="deleted")
        c.execute("DELETE FROM content_items WHERE id=?", (cid,))
        c.commit()
        work = c.execute("SELECT target_input_revision,old_component_id,status FROM duplicate_dirty_work WHERE content_id=?", (cid,)).fetchone()
        self.assertEqual(tuple(work), (changed["input_revision"], "old", "pending"))
        self.assertEqual(c.execute("SELECT state FROM duplicate_components WHERE component_id='old'").fetchone()[0], "dirty")
        self.assertEqual(c.execute("SELECT count(*) FROM duplicate_component_members").fetchone()[0], 1)
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.assertEqual(index.ready_status(c, [cid])[cid]["relation_status"], "pending")
        c.execute("DELETE FROM duplicate_components WHERE component_id='old'")
        c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=?", (AT,))
        c.commit()
        status = index.ready_status(c, [cid])[cid]
        self.assertEqual(status["relation_status"], "ready")
        self.assertFalse(status["fingerprint_available"])
        self.assertFalse(status["retryable"])

    def test_noop_is_zero_writes_and_revision_cas_rejects_stale(self):
        c = self.connection
        cid, fid = add_fingerprint(c)
        c.commit()
        before = c.total_changes
        c.execute("BEGIN")
        self.assertFalse(index.index_fingerprint(c, content_id=cid, fingerprint_id=fid, source_sha256=f"{cid:064x}")["changed"])
        c.commit()
        self.assertEqual(c.total_changes, before)
        c.execute("BEGIN")
        index.invalidate_content(c, cid)
        with self.assertRaises(index.DuplicateInputChanged):
            index.index_fingerprint(c, content_id=cid, fingerprint_id=fid, source_sha256=f"{cid:064x}", expected_input_revision=1)
        c.rollback()

    def test_merge_tombstone_requires_ack_event_alias_and_confirmed_relation(self):
        from v8.metric_field_facts import append_identity_merge
        c = self.connection
        loser, _ = add_fingerprint(c)
        winner, _ = add_fingerprint(c)
        index.invalidate_content(c, loser, deleted=True, reason="content_merged")
        c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision WHERE content_id=?", (loser,))
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")
        event_id = append_identity_merge(c, winner_id=winner, loser_id=loser,
            recorded_at=AT, identity_snapshot={"fixture": "immutable alias identity"})
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")
        link = c.execute("SELECT link_id FROM content_items WHERE id=?", (loser,)).fetchone()[0]
        c.execute("INSERT INTO content_aliases(alias_link_id,content_id,reason,created_at) VALUES (?,?,'identity_upgrade_merge',?)", (link, winner, AT))
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")
        relation_id = c.execute("INSERT INTO duplicate_relations(duplicate_content_id,original_content_id,method,confidence,evidence_json,status,created_at) VALUES (?,?,'identity_merge',1,?,'confirmed',?)",
            (loser, winner, json.dumps({"merge_event_id": event_id + 1}), AT)).lastrowid
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")
        c.execute("UPDATE duplicate_relations SET evidence_json=? WHERE id=?", (json.dumps({"merge_event_id": str(event_id)}), relation_id))
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")
        c.execute("UPDATE duplicate_relations SET evidence_json=? WHERE id=?", (json.dumps({"merge_event_id": event_id}), relation_id))
        status = index.ready_status(c, [loser])[loser]
        self.assertEqual(status["relation_status"], "ready")
        self.assertFalse(status["fingerprint_available"])
        self.assertIsNotNone(c.execute("SELECT id FROM content_items WHERE id=?", (loser,)).fetchone())
        c.execute("UPDATE duplicate_dirty_work SET completed_input_revision=NULL WHERE content_id=?", (loser,))
        self.assertEqual(index.ready_status(c, [loser])[loser]["relation_status"], "pending")

    def test_ready_requires_ack_and_clean_component_but_not_nonempty_frames(self):
        c = self.connection
        cid, fid = add_fingerprint(c, frames=[])
        c.commit()
        self.assertEqual(index.ready_status(c, [cid])[cid]["relation_status"], "pending")
        c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision,completed_at=?", (AT,))
        c.commit()
        self.assertEqual(index.ready_status(c, [cid])[cid]["relation_status"], "ready")
        c.execute("BEGIN")
        index.mark_content_dirty(c, cid)
        c.commit()
        row = index.read_current_fingerprints(c, [cid])[cid]
        self.assertEqual(row["fingerprint_id"], fid)
        self.assertEqual(row["input_revision"], 2)
        self.assertEqual(index.ready_status(c, [cid])[cid]["relation_status"], "pending")

    def test_unready_generation_never_returns_negative_and_corruption_detected(self):
        c = self.connection
        cid, fid = add_fingerprint(c, frames=["f" * 16])
        c.commit()
        c.execute("UPDATE duplicate_fingerprint_frames SET band0=0 WHERE fingerprint_id=?", (fid,))
        c.commit()
        with self.assertRaisesRegex(index.DuplicateIndexError, "posting mismatch"):
            index.validate_postings(c)
        c.execute("UPDATE duplicate_index_generations SET state='building'")
        c.commit()
        with self.assertRaises(index.DuplicateIndexUnavailable):
            index.query_candidate_ids(c, [cid])

    def test_date_only_reuses_only_previously_completed_comparisons(self):
        c = self.connection
        cid, _ = add_fingerprint(c)
        index.mark_content_dirty(c, cid)
        self.assertEqual(json.loads(c.execute("SELECT checkpoint_json FROM duplicate_dirty_work").fetchone()[0]), {})
        c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision")
        index.mark_content_dirty(c, cid)
        self.assertEqual(json.loads(c.execute("SELECT checkpoint_json FROM duplicate_dirty_work").fetchone()[0]), {"reuse_verified_edges": True})
        index.mark_content_dirty(c, cid)
        self.assertEqual(json.loads(c.execute("SELECT checkpoint_json FROM duplicate_dirty_work").fetchone()[0]), {"reuse_verified_edges": True})
        index.invalidate_content(c, cid)
        self.assertEqual(json.loads(c.execute("SELECT checkpoint_json FROM duplicate_dirty_work").fetchone()[0]), {})

    def test_candidate_superset_against_full_comparison_with_repeats(self):
        rng = random.Random(420)
        c = self.connection
        base = rng.getrandbits(64)
        for number in range(60):
            frames = [f"{base ^ rng.getrandbits(number % 18):016x}" for _ in range(number % 5)]
            add_fingerprint(c, frames=frames, text="same exact" if number % 11 == 0 else None,
                media=["shared"] if number % 17 == 0 else [], simhash="0" * 16 if number % 3 else None)
        c.commit()
        prepared = index.prepare_fingerprints(index.read_current_fingerprints(c))
        candidates = index.query_candidate_ids(c, prepared)
        for left, right in itertools.combinations(prepared, 2):
            if index.compare_prepared(prepared[left], prepared[right])["confirmed"]:
                self.assertIn(right, candidates[left], (left, right))
                self.assertIn(left, candidates[right], (left, right))

    def test_changed_rule_generation_cannot_serve_or_report_ready(self):
        c = self.connection
        cid, _ = add_fingerprint(c)
        c.execute("UPDATE duplicate_dirty_work SET status='ready',completed_input_revision=target_input_revision")
        c.execute("UPDATE duplicate_index_generations SET rule_digest='changed-rule'")
        c.commit()
        with self.assertRaises(index.DuplicateIndexUnavailable):
            index.query_candidate_ids(c, [cid])
        status = index.ready_status(c, [cid])[cid]
        self.assertEqual(status["relation_status"], "pending")
        self.assertEqual(status["error_code"], "generation_contract_changed")

    def test_prepared_preserves_zero_confidence_and_original_frame_count(self):
        raw = {"content_id": 1, "media_sha256_json": "[]", "frame_phashes_json": json.dumps(["0" * 16] * 3),
            "text_sha256": None, "text_simhash": None, "asr_simhash": None, "ocr_simhash": None}
        a = index.prepare_fingerprint(raw)
        comparison = index.compare_prepared(a, a)
        self.assertEqual(comparison, {"confirmed": False, "confidence": 0.0, "reasons": [],
            "exact_media": False, "exact_text": False, "phash_distance": 0.0, "phash_match_count": 3,
            "similarities": {"text": None, "asr": None, "ocr": None}})
        exact = index.prepare_fingerprint({**raw, "text_sha256": "legacy-token", "frame_phashes_json": "[]"})
        self.assertEqual(index.compare_prepared(exact, exact)["reasons"], ["text_sha256"])
        repeated = index.prepare_fingerprint({**raw, "frame_phashes_json": json.dumps(["0" * 16] * 3 + ["f" * 16])})
        self.assertEqual(index.compare_prepared(repeated, a)["phash_distance"], 8.0)


class Schema24Test(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "schema.sqlite3"
        self.connection = connect(self.path)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=23)

    def test_explicit_migration_retains_parent_proof_and_is_idempotent(self):
        c = self.connection
        from v8.schema_v23 import migration_proof
        parent = migration_proof(c)
        before = schema.objects(c)
        result = schema.migrate(c)
        self.assertEqual(result["parent_receipt_sha256"], parent["receipt_sha256"])
        self.assertEqual([tuple(value) for value in result["source_objects"]], before)
        changes = c.total_changes
        self.assertEqual(schema.migrate(c)["status"], "unchanged")
        self.assertEqual(changes, c.total_changes)
        self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 24)
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("UPDATE duplicate_index_migrations SET applied_at='tamper'")
        c.rollback()
        c.execute("DROP INDEX idx_duplicate_frame_band0")
        with self.assertRaisesRegex(ValueError, "structure"):
            schema.validate_structure(c)

    def test_migration_failure_rolls_back_schema_manifest_and_version(self):
        c = self.connection
        before = schema.objects(c)
        original = schema.create_tables
        def fail_after_ddl(connection):
            original(connection)
            raise RuntimeError("injected")
        with patch.object(schema, "create_tables", side_effect=fail_after_ddl), self.assertRaisesRegex(RuntimeError, "injected"):
            schema.migrate(c)
        self.assertEqual(schema.objects(c), before)
        self.assertEqual(c.execute("PRAGMA user_version").fetchone()[0], 23)
        self.assertFalse(c.in_transaction)

    def test_migration_retains_approved_schema23_additive_index(self):
        from v8.capture_work_index import INDEX_SQL, INDEX_NAME
        c = self.connection
        c.execute(INDEX_SQL)
        c.commit()
        proof = schema.migrate(c)
        self.assertIn(INDEX_NAME, [value[1] for value in proof["source_objects"]])
        schema.validate_structure(c)
        c.execute("DROP INDEX " + INDEX_NAME)
        with self.assertRaisesRegex(ValueError, "structure"):
            schema.validate_structure(c)

    def test_parent_catalog_revision_type_predicate_is_preserved(self):
        c = self.connection
        schema.migrate(c)
        c.execute("UPDATE capture_catalog_revision SET revision='bad' WHERE id=1")
        with self.assertRaisesRegex(ValueError, "catalog revision"):
            schema.validate_structure(c)

    def test_resigned_proof_cannot_omit_current_additions_or_adopt_unknown_objects(self):
        c = self.connection
        schema.migrate(c)
        for mutation in ("DROP INDEX idx_scheduler_attempts_contract_run_v24", "CREATE INDEX undeclared_v24 ON duplicate_dirty_work(content_id)"):
            with self.subTest(mutation=mutation):
                c.execute("SAVEPOINT malformed_variant")
                proof = json.loads(c.execute("SELECT payload_json FROM duplicate_index_migrations").fetchone()[0])
                trigger = c.execute("SELECT sql FROM sqlite_master WHERE name='trg_duplicate_index_migration_update'").fetchone()[0]
                c.execute(mutation)
                expected_target = schema.objects(c)
                c.execute("DROP TRIGGER trg_duplicate_index_migration_update")
                proof["target_schema_sha256"] = schema.digest(expected_target)
                c.execute("UPDATE duplicate_index_migrations SET payload_json=?,receipt_sha256=?", (json.dumps(proof, sort_keys=True, separators=(",", ":")), schema.digest(proof)))
                c.execute(trigger)
                with self.assertRaisesRegex(ValueError, "declared additions"):
                    schema.validate_structure(c)
                c.execute("ROLLBACK TO malformed_variant")
                c.execute("RELEASE malformed_variant")
        schema.validate_structure(c)

    def test_resigned_proof_cannot_disguise_new_object_as_parent(self):
        c = self.connection
        schema.migrate(c)
        proof = json.loads(c.execute("SELECT payload_json FROM duplicate_index_migrations").fetchone()[0])
        c.execute("CREATE TABLE undeclared_parent_fixture(value TEXT)")
        adopted = tuple(c.execute("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name='undeclared_parent_fixture'").fetchone())
        proof["source_objects"] = sorted([tuple(row) for row in proof["source_objects"]] + [adopted])
        proof["source_schema_sha256"] = schema.digest(proof["source_objects"])
        proof["target_schema_sha256"] = schema.digest(schema.objects(c))
        trigger = c.execute("SELECT sql FROM sqlite_master WHERE name='trg_duplicate_index_migration_update'").fetchone()[0]
        c.execute("DROP TRIGGER trg_duplicate_index_migration_update")
        c.execute("UPDATE duplicate_index_migrations SET payload_json=?,receipt_sha256=?", (json.dumps(proof, sort_keys=True, separators=(",", ":")), schema.digest(proof)))
        c.execute(trigger)
        with self.assertRaisesRegex(ValueError, "immutable schema23"):
            schema.validate_structure(c)


if __name__ == "__main__":
    unittest.main()
