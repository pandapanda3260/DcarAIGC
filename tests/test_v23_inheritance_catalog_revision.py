"""Real schema23 state predicates and the exact frozen-ancestor read-set bridge."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from types import MappingProxyType
import unittest
from unittest.mock import patch

from v8 import account_intake, runtime_database, runtime_evidence_context as evidence, schema_v23
from v8.storage import connect, initialize_database, now_utc, transaction


FROZEN_SCHEMA = Path("/Users/mark/Library/Application Support/DcarAIGC/writer-sources/"
                     "20260913-four-platform-flow-v6/src/dcar_eval/v8/schema_v23.py")
OLD_SQL = "SELECT revision,projection_depth FROM capture_catalog_revision WHERE id=1"


class InheritanceCatalogRevisionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.db = self.root/"fixture.sqlite3"
        self.c = connect(self.db); self.addCleanup(self.c.close)
        initialize_database(self.c, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.enterContext(patch.object(runtime_database, "require_current_process_writer_lock"))
        self.old = self.load_legacy(FROZEN_SCHEMA)

    def load_legacy(self, path):
        if not path.is_file():
            self.skipTest("Exact immutable schema23 S6 ancestor fixture is unavailable: " + str(path))
        name = "v8._catalog_ancestor_fixture_" + hashlib.sha256(str(path).encode()).hexdigest()[:16]
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module; self.addCleanup(sys.modules.pop, name, None)
        spec.loader.exec_module(module)
        return module

    def snapshot(self, *schemas, extra_sql=()):
        self.c.commit()
        reader = sqlite3.connect(self.db.as_uri()+"?mode=ro", uri=True)
        reader.row_factory = sqlite3.Row
        reader.execute("PRAGMA foreign_keys=ON"); reader.execute("PRAGMA recursive_triggers=ON")
        reader.execute("PRAGMA query_only=ON"); reader.execute("BEGIN")
        try:
            recorded = evidence._ReadSet(reader)
            proofs = [module.migration_proof(recorded) for module in schemas]
            for sql, args in extra_sql:
                recorded.execute(sql, args).fetchall()
            metadata = self.db.stat()
            prepared = evidence.PreparedInheritance(self.db, (metadata.st_dev, metadata.st_ino),
                self.root, "{}", "{}", self.root/"install.json", now_utc(),
                MappingProxyType(dict(os.environ)), MappingProxyType({}),
                tuple((key, (tuple(tuple(row) for row in rows), description))
                      for key, (rows, description) in recorded.queries.items()),
                json.dumps({"migration": proofs[0]} if proofs else {}), threading.get_ident())
        finally:
            reader.close()
        return prepared, proofs

    @contextmanager
    def boundary(self, prepared):
        token = evidence._PREPARED.set(prepared)
        try:
            with transaction(self.c), evidence.inheritance_boundary(self.c):
                yield
        finally:
            evidence._PREPARED.reset(token)

    def reuse(self, prepared):
        return evidence.reuse_inheritance(connection=self.c, build={}, build_ref={},
            install_path=prepared.install_path, database=self.db, source=self.root, at=now_utc())

    def add_intake(self):
        with transaction(self.c):
            return account_intake.submit_account_intake(self.c, request_key="normal-new-account",
                value={"platform":"kuaishou", "uid":"123456789", "account_status":"paused"},
                source={"kind":"web"}, at=now_utc())

    def test_current_and_exact_old_ancestor_allow_real_intake_revision_growth(self):
        self.assertEqual(hashlib.sha256(FROZEN_SCHEMA.read_bytes()).hexdigest(), evidence._LEGACY_SCHEMA_SHA256)
        before = schema_v23.migration_proof(self.c)
        prepared, proofs = self.snapshot(schema_v23, self.old)
        self.assertEqual(proofs, [before, before])
        self.assertNotIn(OLD_SQL, [key[0] for key, _ in prepared.queries])
        self.assertEqual(sum(key[0] == evidence._CATALOG_STRUCTURE_SQL for key, _ in prepared.queries), 1)
        result = self.add_intake()
        self.assertGreater(self.c.execute("SELECT revision FROM capture_catalog_revision").fetchone()[0], 0)
        self.assertEqual(schema_v23.migration_proof(self.c), before)
        self.assertEqual(self.old.migration_proof(self.c), before)
        with self.boundary(prepared):
            self.assertEqual(self.reuse(prepared)["migration"], before)
            self.c.execute("UPDATE account_directory_rows SET account_status='daily' WHERE id=?",
                           (result["directory_row_id"],))
            self.assertEqual(self.reuse(prepared)["migration"], before)
        self.assertEqual(self.c.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_unknown_caller_with_identical_sql_keeps_exact_revision_cas(self):
        prepared, _ = self.snapshot(schema_v23, self.old, extra_sql=[(OLD_SQL, ())])
        self.assertIn(OLD_SQL, [key[0] for key, _ in prepared.queries])
        self.add_intake()
        with self.assertRaisesRegex(evidence.RuntimeEvidenceChanged, "database proof changed"), self.boundary(prepared):
            pass

    def test_similar_parameterized_and_other_column_reads_remain_exact(self):
        for sql, args in ((OLD_SQL+" ", ()),
                ("SELECT revision,projection_depth FROM capture_catalog_revision WHERE id=?", (1,)),
                ("SELECT revision FROM capture_catalog_revision WHERE id=1", ())):
            with self.subTest(sql=sql):
                prepared, _ = self.snapshot(self.old, extra_sql=[(sql, args)])
                with transaction(self.c):
                    self.c.execute("UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1")
                with self.assertRaises(evidence.RuntimeEvidenceChanged), self.boundary(prepared):
                    pass

    def test_changed_ancestor_source_cannot_receive_the_legacy_projection(self):
        path = self.root/"schema_v23.py"
        path.write_bytes(FROZEN_SCHEMA.read_bytes()+b"\n# changed source is outside the exact bridge\n")
        changed = self.load_legacy(path)
        prepared, _ = self.snapshot(changed)
        self.assertIn(OLD_SQL, [key[0] for key, _ in prepared.queries])
        self.add_intake()
        with self.assertRaises(evidence.RuntimeEvidenceChanged), self.boundary(prepared):
            pass

    def test_nonidle_missing_and_negative_state_still_fail_both_verifiers_and_boundary(self):
        for mutation in ("UPDATE capture_catalog_revision SET projection_depth=1 WHERE id=1",
                         "UPDATE capture_catalog_revision SET revision=-1 WHERE id=1",
                         "DELETE FROM capture_catalog_revision WHERE id=1"):
            with self.subTest(mutation=mutation):
                prepared, _ = self.snapshot(schema_v23, self.old)
                # Only the disposable fixture bypasses the table CHECK to
                # prove the verifier itself still rejects a corrupt counter.
                self.c.execute("PRAGMA ignore_check_constraints=ON")
                self.c.execute("SAVEPOINT corrupt")
                try:
                    self.c.execute(mutation)
                    for module in (schema_v23, self.old):
                        with self.assertRaisesRegex(ValueError, "revision state invalid"):
                            module.validate_structure(self.c)
                    token = evidence._PREPARED.set(prepared)
                    try:
                        with self.assertRaises(evidence.RuntimeEvidenceChanged), evidence.inheritance_boundary(self.c):
                            pass
                    finally:
                        evidence._PREPARED.reset(token)
                finally:
                    self.c.execute("ROLLBACK TO corrupt"); self.c.execute("RELEASE corrupt")
                    self.c.execute("PRAGMA ignore_check_constraints=OFF")

    def test_non_numeric_revision_is_not_accepted_by_sqlite_affinity_or_comparison_order(self):
        for bad in ("not-a-number", sqlite3.Binary(b"123")):
            with self.subTest(type=type(bad).__name__):
                prepared, _ = self.snapshot(schema_v23, self.old)
                self.c.execute("SAVEPOINT corrupt_type")
                try:
                    # SQLite's INTEGER affinity/CHECK can accept text or blobs;
                    # the old Python numeric comparison correctly rejected it.
                    self.c.execute("UPDATE capture_catalog_revision SET revision=? WHERE id=1", (bad,))
                    with self.assertRaises(TypeError):
                        self.old.validate_structure(self.c)
                    with self.assertRaisesRegex(ValueError, "revision state invalid"):
                        schema_v23.validate_structure(self.c)
                    token = evidence._PREPARED.set(prepared)
                    try:
                        with self.assertRaises(evidence.RuntimeEvidenceChanged), evidence.inheritance_boundary(self.c):
                            pass
                    finally:
                        evidence._PREPARED.reset(token)
                finally:
                    self.c.execute("ROLLBACK TO corrupt_type"); self.c.execute("RELEASE corrupt_type")
        with self.assertRaises(sqlite3.IntegrityError):
            self.c.execute("UPDATE capture_catalog_revision SET revision=NULL WHERE id=1")

    def test_old_legal_numeric_range_keeps_the_same_structural_meaning(self):
        prepared, _ = self.snapshot(schema_v23, self.old)
        for value in (0, 1, 1.5, 9223372036854775807, float("inf")):
            with self.subTest(value=value), self.boundary(prepared):
                self.c.execute("UPDATE capture_catalog_revision SET revision=? WHERE id=1", (value,))
                self.old.validate_structure(self.c)
                schema_v23.validate_structure(self.c)
                self.reuse(prepared)

    def test_semantic_state_invalidated_after_staged_write_rolls_back_at_exit(self):
        prepared, _ = self.snapshot(schema_v23, self.old)
        before = self.c.execute("SELECT count(*) FROM account_intake_requests").fetchone()[0]
        with self.assertRaises(evidence.RuntimeEvidenceChanged), self.boundary(prepared):
            account_intake.submit_account_intake(self.c, request_key="staged-account",
                value={"platform":"kuaishou", "uid":"123456789"}, source={"kind":"web"}, at=now_utc())
            self.c.execute("UPDATE capture_catalog_revision SET projection_depth=1 WHERE id=1")
        self.assertEqual(self.c.execute("SELECT count(*) FROM account_intake_requests").fetchone()[0], before)
        self.assertEqual(self.c.execute("SELECT projection_depth FROM capture_catalog_revision").fetchone()[0], 0)

    def test_exact_account_read_still_denies_changed_identity_after_valid_revision_growth(self):
        result = self.add_intake()
        prepared, _ = self.snapshot(schema_v23, self.old, extra_sql=[(
            "SELECT platform,uid,account_id,identity_status FROM account_directory_rows WHERE id=?",
            (result["directory_row_id"],))])
        with transaction(self.c):
            self.c.execute("UPDATE account_directory_rows SET uid='987654321' WHERE id=?",
                           (result["directory_row_id"],))
        schema_v23.validate_structure(self.c)
        with self.assertRaises(evidence.RuntimeEvidenceChanged), self.boundary(prepared):
            pass

    def test_other_schema_and_proof_queries_keep_full_equality(self):
        for mutation in ("CREATE TABLE unauthorized_schema_change(value)",
                         "UPDATE account_classification_migrations SET id=id"):
            # DDL is a meaningful negative; immutable proof writes must already
            # be refused by their original triggers before proof reuse.
            if mutation.startswith("UPDATE"):
                with self.assertRaises(sqlite3.DatabaseError):
                    self.c.execute(mutation)
                continue
            prepared, _ = self.snapshot(schema_v23, self.old)
            with self.assertRaises(evidence.RuntimeEvidenceChanged), self.boundary(prepared):
                self.c.execute(mutation)
            self.assertIsNone(self.c.execute("SELECT 1 FROM sqlite_master WHERE name='unauthorized_schema_change'").fetchone())


if __name__ == "__main__":
    unittest.main()
