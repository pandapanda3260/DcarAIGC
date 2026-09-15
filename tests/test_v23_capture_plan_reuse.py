"""Real schema23 plan storage, authority generation CAS and write-lock boundaries."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from v8 import account_catalog_capture as catalog, capture_plan_reuse as reuse, catalog_revision, storage
from v8.account_catalog_capture_release import ACCOUNT_CATALOG_POLICY_V3 as POLICY
from v8.operations import upsert_account
from v8.storage import connect, initialize_database, transaction

AT = "2026-09-12T03:00:00Z"
ACTIVE = {"activation_id": 1, "profile_id": "integrated_route_v1", "activation_sha256": "b" * 64,
          "roster_snapshot_id": None, "roster_members_sha256": "a" * 64}


class CapturePlanReuseV23Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name) / "test.sqlite3"
        with connect(self.db) as connection:
            initialize_database(connection, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))
        self.active = self.enterContext(patch.object(reuse, "activation_at", return_value=ACTIVE))
        self.policy = self.enterContext(patch.object(catalog, "installed_policy", return_value=POLICY))

    def rev(self):
        with connect(self.db) as connection:
            return catalog_revision.revision(connection)

    def test_hundred_pages_reuse_one_snapshot_and_no_provider_usage(self):
        original = catalog.freeze_snapshot
        def freeze(connection, **kw):
            self.assertNotEqual(storage._SQLITE_WRITE_TRANSACTION_LOCK._owner, threading.get_ident())
            return original(connection, **kw)
        with patch.object(catalog, "freeze_snapshot", side_effect=freeze) as freeze_call:
            first = reuse.ensure(self.db, at=AT)
            for _ in range(100):
                self.assertEqual(reuse.ensure(self.db, at=AT)["plan"]["id"], first["plan"]["id"])
        self.assertEqual(freeze_call.call_count, 1)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_source_plans").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_work_items").fetchone()[0], 0)

    def test_real_discovery_followup_entry_reuses_plan_in_schema23(self):
        from v8 import capture_runtime
        with patch.object(capture_runtime, "activation_at", return_value=ACTIVE), patch.object(catalog, "freeze_snapshot", wraps=catalog.freeze_snapshot) as freeze_call:
            for _ in range(20):
                result = capture_runtime.enqueue_discovered_metrics([999], db_path=self.db, at=AT)
                self.assertEqual(result["status"], "planned")
                self.assertEqual(result["provider_calls"], 0)
        self.assertEqual(freeze_call.call_count, 1)

    def test_semantic_noop_does_not_change_revision_and_projection_does_not_evict(self):
        account = upsert_account({"phone": "", "enabled": True, "platforms": [{"platform": "douyin", "uid": "123456789"}]}, db_path=self.db)
        first = reuse.ensure(self.db, at=AT)
        before = self.rev()
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=enabled WHERE id=?", (account["id"],))
            with catalog_revision.projection(connection):
                connection.execute("UPDATE accounts SET enabled=0 WHERE id=?", (account["id"],))
        self.assertEqual(self.rev(), before)
        self.assertEqual(reuse.ensure(self.db, at=AT)["plan"]["id"], first["plan"]["id"])
        with connect(self.db) as connection, transaction(connection):
            connection.execute("UPDATE accounts SET enabled=1 WHERE id=?", (account["id"],))
        self.assertGreater(self.rev(), before)
        self.assertNotEqual(reuse.ensure(self.db, at=AT)["plan"]["id"], first["plan"]["id"])

    def test_racing_input_change_discards_cold_snapshot_before_short_write(self):
        original = catalog.freeze_snapshot
        changed = []
        def freeze(connection, **kw):
            result = original(connection, **kw)
            if not changed:
                changed.append(True)
                with connect(self.db) as writer, transaction(writer):
                    writer.execute("UPDATE capture_catalog_revision SET revision=revision+1 WHERE id=1")
            return result
        with patch.object(catalog, "freeze_snapshot", side_effect=freeze) as calls:
            ready = reuse.ensure(self.db, at=AT)
        self.assertEqual(calls.call_count, 2)
        self.assertEqual(ready["key"]["catalog_revision"], 1)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM capture_source_plans").fetchone()[0], 1)

    def test_two_cold_writers_converge_on_one_persisted_plan(self):
        barrier = threading.Barrier(2)
        original = catalog.freeze_snapshot
        def freeze(connection, **kw):
            result = original(connection, **kw); barrier.wait(timeout=10); return result
        with patch.object(catalog, "freeze_snapshot", side_effect=freeze), ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: reuse.ensure(self.db, at=AT), range(2)))
        self.assertEqual(len({item["plan"]["id"] for item in results}), 1)
        self.assertEqual(sum(not item["reused"] for item in results), 1)

    def test_day_activation_and_shadow_have_separate_immutable_plans(self):
        first = reuse.ensure(self.db, at=AT)
        next_day = reuse.ensure(self.db, at="2026-09-13T03:00:00Z")
        shadow = reuse.ensure(self.db, at=AT, shadow=True)
        self.active.return_value = {**ACTIVE, "activation_id": 2, "activation_sha256": "c" * 64}
        switched = reuse.ensure(self.db, at=AT)
        self.assertEqual(len({r["plan"]["id"] for r in (first, next_day, shadow, switched)}), 4)
        with connect(self.db) as connection:
            saved = json.loads(connection.execute("SELECT payload_json FROM capture_source_plans WHERE id=?", (first["plan"]["id"],)).fetchone()[0])
        self.assertEqual(saved["activation_id"], 1)

    def test_projection_exception_rolls_back_depth_and_change(self):
        before = self.rev()
        with self.assertRaisesRegex(RuntimeError, "fixture"):
            with connect(self.db) as connection, transaction(connection):
                with catalog_revision.projection(connection):
                    connection.execute("UPDATE capture_catalog_revision SET revision=999 WHERE id=1")
                    raise RuntimeError("fixture")
        self.assertEqual(self.rev(), before)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 23)
            storage.require_schema_compatibility(connection)
