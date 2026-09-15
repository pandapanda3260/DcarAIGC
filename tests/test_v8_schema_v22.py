from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from v8 import schema_v22 as schema
from v8.storage import (SchemaMigrationError, configure_connection_safety, connect,
                       initialize_database, require_schema_compatibility, schema_compatibility_state)

AT = "2026-09-12T00:00:00Z"
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_account_intake.py"
spec = importlib.util.spec_from_file_location("test_account_intake_migration_cli", SCRIPT)
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


class Schema22Test(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.path = self.root / "source.sqlite3"
        self.connection = connect(self.path)
        self.addCleanup(self.connection.close)
        initialize_database(self.connection, target_version=21)
        self.enterContext(patch.object(socket.socket, "connect", side_effect=AssertionError("network forbidden")))
        c = self.connection
        for ident, platform in ((1, "douyin"), (2, "xiaohongshu"), (3, "douyin")):
            c.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES (?,'',?,?)", (ident, AT, AT))
            c.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,created_at,updated_at) VALUES (?,?,?,?,?,?)", (ident, ident, platform, "000000" + str(ident), AT, AT))
        c.execute("INSERT INTO fetch_slots(id,account_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) VALUES (31,1,'discovery','old-window','tikhub','fixture','succeeded',?,?)", (AT, AT))
        c.execute("INSERT INTO fetch_attempts(id,slot_id,attempt_number,request_started_at,http_status,billed) VALUES (41,31,1,?,200,1)", (AT,))
        c.execute("INSERT INTO provider_raw_responses(id,fetch_attempt_id,account_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at,paid_scope_identity) VALUES (61,41,1,'TikHub','fixture_profile','fixture.json',?,3,200,?,'original-paid-identity')", ("c" * 64, AT))
        c.execute("INSERT INTO account_provider_references VALUES (1,'TikHub','fixture_uid','00001',61,?,?)", (AT, AT))
        c.execute("INSERT INTO capture_route_assignments(id,scope_type,scope_key,provider,operation,account_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES (71,'account','1','tikhub','douyin_discovery',1,1,'integrated','active',?,?,?)", (AT, AT, "d" * 64))
        c.execute("INSERT INTO capture_work_items(id,work_identity,assignment_id,account_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,attempt_count,created_at,updated_at) VALUES (81,?,71,1,'tikhub','douyin_discovery',?,'2026-09-12','paid_identity_hold','existing_hold','{}',1,?,?)", ("e" * 64, AT, AT, AT))
        c.execute("INSERT INTO fetch_request_batches(id,work_id,request_scope_identity,provider,operation,parameters_json,created_at) VALUES (91,81,'original-scope','tikhub','douyin_discovery','{}',?)", (AT,))
        c.execute("INSERT INTO fetch_request_batch_members(id,batch_id,member_scope_identity,sequence,account_id) VALUES (101,91,'old-member',0,1)")
        c.execute("UPDATE sqlite_sequence SET seq=999 WHERE name='fetch_slots'")
        c.commit()
        self.before = schema._table_digests(c)
        self.objects = schema._objects(c)
        self.backup = self.root / "backup.sqlite3"
        with connect(self.backup) as backup:
            c.backup(backup)

    def intake(self, key="fixture", platform="douyin"):
        return self.connection.execute("INSERT INTO account_intake_requests(request_key,input_sha256,preparation_key,platform,input_json,source_json,created_at,updated_at) VALUES (?,?,?,?, '{}','{}',?,?)", (key, "a" * 64, "same-preparation", platform, AT, AT)).lastrowid

    def test_migration_preserves_all_rows_ids_relations_and_sequences(self):
        c = self.connection
        result = schema.migrate(c)
        self.assertEqual(result["target_version"], 22)
        self.assertEqual(require_schema_compatibility(c), 22)
        with connect(self.backup) as source:
            proof = schema.validate_lineage(source, c)
        self.assertTrue(proof["retained_tables_verified"])
        self.assertEqual(proof["retained_table_count"], len(self.before))
        self.assertEqual(c.execute("SELECT slot_id FROM fetch_attempts WHERE id=41").fetchone()[0], 31)
        self.assertEqual(c.execute("SELECT fetch_attempt_id FROM provider_raw_responses WHERE id=61").fetchone()[0], 41)
        self.assertEqual(c.execute("SELECT state,reason,assignment_id FROM capture_work_items WHERE id=81").fetchone()[:], ("paid_identity_hold", "existing_hold", 71))
        self.assertEqual(c.execute("SELECT platform FROM account_provider_references").fetchone()[0], "douyin")
        self.assertEqual(c.execute("SELECT seq FROM sqlite_sequence WHERE name='fetch_slots'").fetchone()[0], 999)
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.assertEqual(c.execute("PRAGMA quick_check").fetchone()[0], "ok")

    def test_repeat_is_a_verified_no_write_operation(self):
        schema.migrate(self.connection)
        changes = self.connection.total_changes
        proof = schema.migration_proof(self.connection)
        self.assertEqual(schema.migrate(self.connection)["status"], "unchanged")
        self.assertEqual(self.connection.total_changes, changes)
        self.assertEqual(schema.migration_proof(self.connection), proof)

    def test_failure_after_alter_rolls_back_all_ddl_rows_and_pragmas(self):
        with patch.object(schema, "_fetch_sql", side_effect=RuntimeError("injected failure")), self.assertRaisesRegex(RuntimeError, "injected"):
            schema.migrate(self.connection)
        self.assertEqual(schema._objects(self.connection), self.objects)
        self.assertEqual(schema._table_digests(self.connection), self.before)
        self.assertEqual(require_schema_compatibility(self.connection), 21)
        self.assertEqual(self.connection.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertFalse(self.connection.in_transaction)

    def test_readonly_formal_and_active_transaction_guards(self):
        c = self.connection
        c.execute("PRAGMA query_only=ON")
        with self.assertRaisesRegex(ValueError, "writable"):
            schema.migrate(c)
        c.execute("PRAGMA query_only=OFF")
        with patch("v8.storage.is_formal_database_path", return_value=True), self.assertRaises(SchemaMigrationError):
            schema.migrate(c)
        c.execute("BEGIN")
        with self.assertRaisesRegex(ValueError, "idle"):
            schema.migrate(c)
        c.rollback()
        self.assertEqual(schema._objects(c), self.objects)

    def test_runtime_never_implicitly_upgrades_existing_schema21(self):
        initialize_database(self.connection, allow_migrations=False)
        self.assertEqual(require_schema_compatibility(self.connection), 21)
        with self.assertRaisesRegex(SchemaMigrationError, "explicit offline migration"):
            initialize_database(self.connection, target_version=22, allow_migrations=False)
        self.assertEqual(schema._objects(self.connection), self.objects)

    def test_reference_uniqueness_is_platform_scoped_and_identity_is_checked(self):
        c = self.connection
        schema.migrate(c)
        c.execute("INSERT INTO account_provider_references VALUES (2,'TikHub','fixture_uid','00001',NULL,?,?,'xiaohongshu')", (AT, AT))
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO account_provider_references VALUES (3,'TikHub','fixture_uid','00001',NULL,?,?,'douyin')", (AT, AT))
        with self.assertRaisesRegex(sqlite3.IntegrityError, "platform"):
            c.execute("UPDATE account_provider_references SET platform='kuaishou' WHERE account_identity_id=2")
        self.assertEqual(c.execute("SELECT reference_value FROM account_provider_references WHERE account_identity_id=1").fetchone()[0], "00001")

    def test_intake_constraints_and_three_way_slot_target(self):
        c = self.connection
        schema.migrate(c)
        intake = self.intake()
        self.intake("second-request")  # Preparation keys intentionally are not unique.
        with self.assertRaises(sqlite3.IntegrityError):
            self.intake()
        with self.assertRaises(sqlite3.IntegrityError):
            self.intake("bad-platform", "unknown")
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("UPDATE account_intake_requests SET input_json='broken' WHERE id=?", (intake,))
        slot = c.execute("INSERT INTO fetch_slots(intake_request_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) VALUES (?,'profile_prepare','window','tikhub','fixture','pending',?,?)", (intake, AT, AT)).lastrowid
        self.assertGreater(slot, 999)
        for assignment in ("account_id=1", "intake_request_id=NULL", "stage='discovery'"):
            with self.subTest(assignment=assignment), self.assertRaises(sqlite3.IntegrityError):
                c.execute(f"UPDATE fetch_slots SET {assignment} WHERE id=?", (slot,))
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO fetch_slots(intake_request_id,stage,window_key,provider,adapter_version,status,created_at,updated_at) VALUES (?,'profile_prepare','window','tikhub','other','pending',?,?)", (intake, AT, AT))
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("DELETE FROM account_intake_requests WHERE id=?", (intake,))

    def test_route_and_batch_can_target_an_intake_without_a_fake_account(self):
        c = self.connection
        schema.migrate(c)
        intake = self.intake()
        route = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,intake_request_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES ('intake',?,'tikhub','douyin_profile_prepare',?,1,'integrated','active',?,?,?)", (str(intake), intake, AT, AT, "f" * 64)).lastrowid
        c.execute("INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,intake_request_id) VALUES (91,'new-intake-member',0,?)", (intake,))
        self.assertIsNone(c.execute("SELECT account_id FROM capture_route_assignments WHERE id=?", (route,)).fetchone()[0])
        with self.assertRaises(sqlite3.IntegrityError):
            c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES ('intake','invalid','tikhub','douyin_profile_prepare',1,'integrated','active',?,?,?)", (AT, AT, "0" * 64))
        self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_receipt_and_inherited_immutable_evidence_are_preserved(self):
        c = self.connection
        schema.migrate(c)
        for statement in ("UPDATE account_intake_migrations SET payload_json='{}'", "DELETE FROM account_intake_migrations", "UPDATE capture_route_assignments SET operation='changed' WHERE id=71"):
            with self.subTest(statement=statement), self.assertRaises(sqlite3.IntegrityError):
                c.execute(statement)
        c.execute("DROP INDEX uq_fetch_intake_slot")
        self.assertFalse(schema_compatibility_state(c)["compatible"])
        with self.assertRaises(ValueError):
            schema.migration_proof(c)

    def test_lineage_rejects_changed_historical_business_data(self):
        schema.migrate(self.connection)
        self.connection.execute("UPDATE accounts SET operator_name='changed' WHERE id=1")
        with connect(self.backup) as source, self.assertRaisesRegex(ValueError, "retained table changed: accounts"):
            schema.validate_lineage(source, self.connection)

    def test_lineage_rejects_issued_ids_even_when_no_new_business_rows_remain(self):
        schema.migrate(self.connection)
        self.connection.execute("UPDATE sqlite_sequence SET seq=seq+1 WHERE name='accounts'")
        with connect(self.backup) as source, self.assertRaisesRegex(ValueError,"sequence changed: accounts"):
            schema.validate_lineage(source,self.connection)

    def test_cli_produces_verified_backup_candidate_without_source_writes_and_replays(self):
        source_objects = schema._objects(self.connection)
        source_rows = schema._table_digests(self.connection)
        backup = self.root / "cli.backup.sqlite3"
        candidate = self.root / "cli.candidate.sqlite3"
        report = self.root / "cli.report.json"
        receipt = cli.create_candidate(source=self.path, backup=backup, candidate=candidate, report=report)
        self.assertEqual(receipt["status"], "candidate")
        hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in (backup, candidate, report)}
        replay = cli.create_candidate(source=self.path, backup=backup, candidate=candidate, report=report)
        self.assertEqual(replay["status"], "unchanged")
        self.assertEqual(hashes, {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in hashes})
        self.assertEqual(schema._objects(self.connection), source_objects)
        self.assertEqual(schema._table_digests(self.connection), source_rows)
        self.assertEqual(require_schema_compatibility(self.connection), 21)

    def test_cli_refuses_aliases_symlinks_existing_outputs_and_formal_database(self):
        targets = {"source": self.path, "backup": self.root / "b.sqlite3", "candidate": self.root / "c.sqlite3", "report": self.root / "r.json"}
        with self.assertRaisesRegex(ValueError, "distinct"):
            cli.create_candidate(**{**targets, "candidate": self.path})
        link = self.root / "link.sqlite3"; link.symlink_to(self.path)
        with self.assertRaisesRegex(ValueError, "non-symlink"):
            cli.create_candidate(**{**targets, "source": link})
        with patch.object(cli, "is_formal_database_path", return_value=True), self.assertRaisesRegex(ValueError, "formal"):
            cli.create_candidate(**targets)
        targets["backup"].write_bytes(b"do not overwrite")
        with self.assertRaisesRegex(ValueError, "partial outputs"):
            cli.create_candidate(**targets)
        self.assertEqual(targets["backup"].read_bytes(), b"do not overwrite")


if __name__ == "__main__":
    unittest.main()
