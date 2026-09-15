"""One source generation, quote and existing durable command; all IO offline."""
import json
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import capture_commands, capture_manual, media_source_refresh as refresh, storage
from v8.runtime_database import DatabaseAccessMode, FileIdentity, InstalledWriterContract, ResolvedDatabaseAccess, acquire_writer_lock

AT = "2026-09-12T00:00:00Z"


class MediaSourceRefreshTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name).resolve()
        self.db = root / "fixture.db"
        with storage.connect(self.db) as c:
            storage.initialize_database(c, target_version=23)
            c.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(1,'',0,?,?)", (AT, AT))
            c.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,source,created_at,updated_at) VALUES(1,'douyin','123456789','fixture','manual',?,?)", (AT, AT))
            c.execute("INSERT INTO content_items(id,account_id,link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,published_at,created_at,updated_at,imported_at) VALUES(1,1,'A2BC3D','douyin','7380000000000000001','https://www.douyin.com/video/7380000000000000001','fixture','video','123456789',?,?,?,?)", (AT, AT, AT, AT))
        lock = root / "writer.lock"; lock.touch(mode=0o600)
        installed = InstalledWriterContract(root, root / "fixture.plist", root, root / "fixture.py", self.db, lock, {})
        self.enterContext(acquire_writer_lock(ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, self.db,
            FileIdentity.from_stat(self.db.stat()), root, lock, installed)))
        self.enterContext(patch.object(refresh, "now_utc", return_value=AT))

    def prepare(self, request_id="fixture-quote"):
        with storage.connect(self.db) as c:
            return refresh.prepare_media_source_refresh(c, content_id=1, request_id=request_id, source_generation="missing", at=AT)

    def execute(self, receipt, **kwargs):
        with storage.connect(self.db) as c:
            return refresh.execute_media_source_refresh(c, content_id=1, task_id=receipt["task_id"],
                source_generation="missing", at=kwargs.get("at", AT))

    def link_work(self, accepted, *, state="runnable", reason=""):
        with storage.connect(self.db) as c:
            details = json.loads(c.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (accepted["run_id"],)).fetchone()[0])
            spec = details["identity"]["specification"]
            assignment = c.execute("INSERT INTO capture_route_assignments(scope_type,scope_key,provider,operation,content_id,generation,route,mode,effective_at,recorded_at,assignment_sha256) VALUES('content',?,'tikhub','douyin_video_detail',1,1,'integrated','active',?,?,?)", (accepted["task_id"], AT, AT, hashlib.sha256(accepted["task_id"].encode()).hexdigest())).lastrowid
            work_id = c.execute("INSERT INTO capture_work_items(work_identity,assignment_id,account_id,content_id,provider,operation,due_at,data_business_day,state,reason,envelope_json,owner_token,created_at,updated_at,completed_at) VALUES(?,?,1,1,'tikhub','douyin_video_detail',?,'2026-09-12',?,?,?,?,?,?,?)",
                (hashlib.sha256(str(accepted["run_id"]).encode()).hexdigest(), assignment, AT, state, reason, json.dumps(spec), "fixture-owner" if state in {"running", "leased"} else None, AT, AT, AT if state == "terminal" else None)).lastrowid
            details["checkpoint"] = {"complete": True, "result": {"status": "queued", "reason": "", "work_ids": [work_id]}}
            c.execute("UPDATE scheduler_runs SET status='succeeded',details_json=? WHERE id=?", (json.dumps(details), accepted["run_id"]))
            return work_id

    def singleton_raw(self, accepted, *, error=None, send_markers=1, add_source=True, add_raw=True, db=None):
        """Schema23 physical request: batch-owned attempt plus durable send events.

        Keep foreign keys and production dispatch append helpers enabled. The
        scheduler slot deliberately does not own fetch_attempts.slot_id.
        """
        from tests.roster_fixture import accept_roster
        from v8.paid_drain import dispatch_state
        from v8 import paid_dispatch
        db = db or self.db
        raw_body = b'{"code":200,"data":{"aweme_details":null,"filter_list":[{"reason":5}]}}' if error else b'{"code":200,"data":{"fixture":true}}'
        raw_path = db.parent / (accepted["task_id"].replace(":", "-") + ".json")
        raw_path.write_bytes(raw_body); raw_path.chmod(0o600)
        with storage.connect(db) as c:
            proposal = c.execute("SELECT * FROM media_source_refresh_proposals WHERE id=?", (accepted["task_id"],)).fetchone()
            at = proposal["created_at"]
            accept_roster(c, accepted_at=at)
            slot = c.execute("INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,attempt_count,created_at,updated_at) VALUES(1,'detail',?,'TikHub','fixture',?,1,?,?)", (refresh.logical_due(proposal), "terminal_failed" if error else "succeeded", at, at)).lastrowid
            batch = c.execute("INSERT INTO fetch_request_batches(request_scope_identity,sequence,provider,operation,parameters_json,created_at) VALUES(?,0,'tikhub','douyin_video_detail','{}',?)", (accepted["task_id"], at)).lastrowid
            c.execute("INSERT INTO fetch_request_batch_members(batch_id,member_scope_identity,sequence,content_id,account_id) VALUES(?,?,0,1,1)", (batch, accepted["task_id"]))
            attempt = c.execute("INSERT INTO fetch_attempts(slot_id,request_batch_id,attempt_number,request_started_at,response_finished_at,http_status,billed,amount,currency,error_code) VALUES(NULL,?,1,?,?,200,1,.001,'USD',?)", (batch, at, at, error)).lastrowid
            usage = c.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,amount,currency,recorded_at) VALUES(?,'TikHub','douyin_video_detail',1,1,.001,'USD',?)", (accepted["task_id"], at)).lastrowid
            raw = c.execute("INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(?,1,'TikHub',?,?,?,?,200,?)", (attempt, proposal["detail_operation"], str(raw_path), hashlib.sha256(raw_body).hexdigest(), len(raw_body), at)).lastrowid if add_raw else None
            run_attempt = c.execute("INSERT INTO scheduler_run_attempts(scheduler_run_id,attempt_number,invocation_source,status,started_at,details_json) VALUES(?,1,'scheduled','running',?,'{}')", (accepted["run_id"], at)).lastrowid
            activation = dispatch_state(c, at=at).activation_id
            for _ in range(send_markers):
                event = paid_dispatch.reserve_dispatch_in_transaction(c, provider="TikHub", operation=proposal["detail_operation"], activation_id=activation, business_day="2026-09-12", scheduler_run_id=accepted["run_id"], scheduler_attempt_id=run_attempt, scope={"content_id": 1}, provider_usage_id=usage, fetch_slot_id=slot, created_at=at)
                paid_dispatch.mark_dispatch_sent_in_transaction(c, event.dispatch_id, fetch_attempt_id=attempt, created_at=at)
                paid_dispatch.finish_dispatch_in_transaction(c, event.dispatch_id, outcome="billing_unknown" if not add_raw else "failed" if error else "succeeded", raw_response_id=raw, created_at=at)
            source = c.execute("INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) VALUES(1,'media_source','source','available',1,?,?,'fixture',?,?)", ("d" * 64, at, json.dumps({"raw_response_id": raw}), at)).lastrowid if add_source else None
            self.assertIsNone(c.execute("SELECT slot_id FROM fetch_attempts WHERE id=?", (attempt,)).fetchone()[0])
            self.assertEqual(c.execute("PRAGMA foreign_key_check").fetchall(), [])
        return {"source": source, "slot": slot, "attempt": attempt, "raw": raw, "usage": usage}

    def test_singleton_success_replays_after_expiry_and_new_generation_without_second_purchase(self):
        old = self.prepare(); self.execute(old)
        with storage.connect(self.db) as c:
            quote = refresh.prepare_media_source_refresh(c, content_id=1, request_id="explicit-second-quote", source_generation="missing", at="2026-09-12T02:00:00Z")
        accepted = self.execute(quote, at="2026-09-12T02:00:00Z")
        evidence = self.singleton_raw(accepted)
        with patch.object(refresh, "now_utc", return_value="2026-10-12T00:00:00Z"), storage.connect(self.db) as c:
            before = c.total_changes
            c.execute("PRAGMA query_only=ON")
            with patch.object(refresh, "attempt_slot_sql", return_value="a.slot_id"):
                self.assertIsNone(refresh.authorized_source_request(c, 1, evidence["source"]))
            self.assertEqual(refresh.authorized_source_request(c, 1, evidence["source"]), quote["task_id"])
            self.assertEqual(capture_manual.validate_command(c, accepted["run_id"], content_id=1, stage="detail")["task_id"], quote["task_id"])
            self.assertFalse(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["can_requote"])
            self.assertEqual(c.total_changes, before)
            self.assertEqual(c.execute("SELECT status FROM media_source_refresh_proposals WHERE id=?", (old["task_id"],)).fetchone()[0], "expired")
            self.assertEqual(c.execute("SELECT sum(request_attempts) FROM provider_usage").fetchone()[0], 1)
        self.assertEqual(self.execute(quote, at="2026-10-12T00:00:00Z")["run_id"], accepted["run_id"])

    def test_singleton_raw_evidence_prevents_retirement_even_without_usage_send_projection(self):
        accepted = self.execute(self.prepare()); self.singleton_raw(accepted, add_source=False)
        with storage.connect(self.db) as c:
            # A damaged/lagging usage projection must not erase durable raw proof.
            c.execute("UPDATE provider_usage SET request_attempts=0")
            c.commit()
            self.assertFalse(refresh.read_proposal(c, task_id=accepted["task_id"], at="2026-10-12T00:00:00Z")["can_requote"])

    def test_any_send_marker_blocks_requote_and_resend_when_usage_projection_is_zero(self):
        for add_raw, markers in ((False, 1), (True, 2)):
            with self.subTest(raw=add_raw, send_markers=markers):
                fixture = MediaSourceRefreshTest(); fixture.setUp()
                try:
                    accepted = fixture.execute(fixture.prepare())
                    fixture.singleton_raw(accepted, add_raw=add_raw, add_source=False, send_markers=markers)
                    with storage.connect(fixture.db) as c:
                        c.execute("UPDATE provider_usage SET request_attempts=0,billed_requests=0,amount=0")
                        c.commit()
                        before = c.total_changes
                        view = refresh.read_proposal(c, task_id=accepted["task_id"], at="2026-09-12T02:00:00Z")
                        self.assertEqual((view["status"], view["can_requote"]), ("queued", False))
                        self.assertEqual(c.total_changes, before)
                        same = refresh.prepare_media_source_refresh(c, content_id=1, request_id="projection-is-not-renewal", source_generation="missing", at="2026-09-12T02:00:00Z")
                        self.assertEqual(same["task_id"], accepted["task_id"])
                        for at in (AT, "2026-09-12T02:00:00Z"):
                            with self.assertRaises(refresh.MediaSourceRefreshError) as blocked:
                                refresh.manual_spec(c, content_id=1, task_id=accepted["task_id"], at=at)
                            self.assertEqual(blocked.exception.error_code, "media_source_refresh_request_limit")
                        self.assertEqual(c.execute("SELECT count(*) FROM media_source_refresh_proposals").fetchone()[0], 1)
                        self.assertEqual(c.execute("SELECT count(*) FROM paid_provider_dispatch_events WHERE event_type='send_marked'").fetchone()[0], markers)
                finally:
                    fixture.doCleanups()

    def test_singleton_raw_without_unique_send_marker_cannot_authorize_replay(self):
        for markers in (0, 2):
            with self.subTest(send_markers=markers):
                fixture = MediaSourceRefreshTest(); fixture.setUp()
                try:
                    accepted = fixture.execute(fixture.prepare())
                    evidence = fixture.singleton_raw(accepted, send_markers=markers)
                    with storage.connect(fixture.db) as c:
                        self.assertIsNone(refresh.authorized_source_request(c, 1, evidence["source"]))
                        with self.assertRaises(refresh.MediaSourceRefreshError) as blocked:
                            refresh.manual_spec(c, content_id=1, task_id=accepted["task_id"], at="2026-10-12T00:00:00Z")
                        self.assertEqual(blocked.exception.error_code, "media_source_refresh_request_limit")
                finally:
                    fixture.doCleanups()

    def test_singleton_content_unavailable_stays_paid_hold_after_expiry_and_cannot_requote(self):
        from v8.media_work_queue import list_pending_media_work
        accepted = self.execute(self.prepare())
        self.link_work(accepted, state="paid_identity_hold", reason="paid_identity_hold:content_unavailable")
        self.singleton_raw(accepted, error="content_unavailable", add_source=False)
        with storage.connect(self.db) as c:
            task = list_pending_media_work(c, at="2026-09-12T02:00:00Z")["refresh_tasks"][0]
            self.assertEqual((task["status"], task["can_requote"]), ("blocked", False))
            self.assertEqual(task["reason_label"], "数据源本次未返回该作品，已发送一次请求；不会自动再次付费。")
            same = refresh.prepare_media_source_refresh(c, content_id=1, request_id="cannot-rebuy", source_generation="missing", at="2026-09-12T02:00:00Z")
            self.assertEqual(same["task_id"], accepted["task_id"])
            with self.assertRaises(refresh.MediaSourceRefreshError) as blocked:
                refresh.manual_spec(c, content_id=1, task_id=accepted["task_id"], at="2026-09-12T02:00:00Z")
            self.assertEqual(blocked.exception.error_code, "media_source_refresh_request_limit")
            self.assertEqual(c.execute("SELECT sum(request_attempts) FROM provider_usage").fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT count(*) FROM evidence_artifacts WHERE artifact_type='media_source'").fetchone()[0], 0)

    def test_prepare_never_enqueues_and_exact_repeat_returns_same_quote(self):
        first = self.prepare(); second = self.prepare()
        self.assertEqual(first, second)
        self.assertEqual(first["request_limit"], 1)
        self.assertEqual(first["provider_calls"], 0)
        self.assertGreater(first["max_amount"], 0)
        with storage.connect(self.db) as c:
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=?", (capture_commands.JOB,)).fetchone()[0], 0)
            self.assertEqual(c.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_explicit_execute_uses_one_durable_command_and_never_lifetime(self):
        proposal = self.prepare(); one = self.execute(proposal); two = self.execute(proposal)
        self.assertEqual(one, two)
        third = self.prepare("another-tab")
        self.assertEqual(third["task_id"], one["task_id"])
        with storage.connect(self.db) as c:
            spec = capture_manual.validate_command(c, one["run_id"], content_id=1, stage="detail")
            self.assertEqual(spec["kind"], "media_source_refresh")
            self.assertEqual(len(spec["targets"]), 1)
            self.assertIn(proposal["task_id"], spec["targets"][0]["logical_due"])
            self.assertNotEqual(spec["targets"][0]["logical_due"], "lifetime")
            self.assertEqual(spec["task_max_amount"], proposal["max_amount"])
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=?", (capture_commands.JOB,)).fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT enabled FROM accounts WHERE id=1").fetchone()[0], 0)

    def test_two_prepared_tabs_cannot_queue_twice(self):
        first, second = self.prepare("tab-one"), self.prepare("tab-two")
        self.execute(first)
        with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "已有更新任务"):
            self.execute(second)

    def test_generation_change_blocks_send_but_does_not_break_read_receipt(self):
        proposal = self.prepare(); accepted = self.execute(proposal)
        with storage.connect(self.db) as c:
            c.execute("INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) VALUES(1,'media_source','fixture','available',1,?,?,'fixture','{}',?)", ("a" * 64, AT, AT))
        with storage.connect(self.db) as c:
            with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "来源已经变化"):
                capture_manual.validate_command(c, accepted["run_id"], content_id=1, stage="detail")
            self.assertEqual(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["run_id"], accepted["run_id"])
        self.assertEqual(self.execute(proposal)["run_id"], accepted["run_id"])

    def test_quote_expiry_and_unverified_media_type_block_before_command(self):
        quote = self.prepare()
        with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "已过期"):
            self.execute(quote, at="2026-09-12T02:00:00Z")
        with storage.connect(self.db) as c:
            c.execute("UPDATE content_items SET content_type='unknown' WHERE id=1")
        with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "作品类型"):
            self.prepare("unknown-type")

    def test_successful_new_raw_keeps_reacquire_proof_after_quote_expiry(self):
        proposal = self.prepare(); accepted = self.execute(proposal)
        raw_path = self.db.parent / "successful-raw.json"
        raw_body = b'{"code":200,"data":{"fixture":true}}'
        raw_path.write_bytes(raw_body); raw_path.chmod(0o600)
        with storage.connect(self.db) as c:
            row = c.execute("SELECT * FROM media_source_refresh_proposals WHERE id=?", (proposal["task_id"],)).fetchone()
            slot = c.execute("INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,status,attempt_count,created_at,updated_at) VALUES(1,'detail',?,'TikHub','fixture','succeeded',1,?,?)", (refresh.logical_due(row), AT, AT)).lastrowid
            attempt = c.execute("INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,response_finished_at,billed) VALUES(?,1,?,?,1)", (slot, AT, AT)).lastrowid
            raw = c.execute("INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(?,1,'TikHub',?,?,?,?,200,?)", (attempt, row["detail_operation"], str(raw_path), hashlib.sha256(raw_body).hexdigest(), len(raw_body), AT)).lastrowid
            source = c.execute("INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,status,byte_size,sha256,captured_at,processor_version,metadata_json,created_at) VALUES(1,'media_source','source','available',1,?,?,'fixture',?,?)", ("c" * 64, AT, json.dumps({"raw_response_id": raw}), AT)).lastrowid
        with patch.object(refresh, "now_utc", return_value="2026-10-12T00:00:00Z"), storage.connect(self.db) as c:
            self.assertEqual(refresh.authorized_source_request(c, 1, source), proposal["task_id"])
            self.assertEqual(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["kind"], "media_source_refresh")
            self.assertFalse(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["can_requote"])
            self.assertEqual(capture_manual.validate_command(c, accepted["run_id"], content_id=1, stage="detail")["kind"], "media_source_refresh")

    def test_api_read_only_projection_prepare_execute_and_replay(self):
        from fastapi.testclient import TestClient
        from v8 import api
        from tests.test_v8_api import _test_config
        config = _test_config(self.db.parent, db_name=self.db.name)
        with patch.object(api, "now_utc", return_value=AT):
            client = TestClient(api.create_app(config))
            self.addCleanup(client.close)
            path = "/api/v8/contents/1/media/source-refresh"
            response = client.post(path + "/prepare", json={"request_id": "api-quote", "source_generation": "missing"})
            self.assertEqual(response.status_code, 200, response.text)
            quoted = response.json()
            with storage.connect(self.db) as c:
                before = c.execute("SELECT count(*) FROM scheduler_runs").fetchone()[0]
            for _ in range(2):
                read = client.get("/api/v8/media/pending-work")
                self.assertEqual(read.status_code, 200, read.text)
                self.assertEqual(read.json()["provider_calls"], 0)
            with storage.connect(self.db) as c:
                self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs").fetchone()[0], before)
            body = {"task_id": quoted["task_id"], "source_generation": "missing"}
            accepted = client.post(path + "/execute", json=body)
            self.assertEqual(accepted.status_code, 202, accepted.text)
            self.assertEqual(client.post(path + "/execute", json=body).json(), accepted.json())
            with patch.object(refresh, "now_utc", return_value="2026-09-12T02:00:00Z"), patch.object(api, "now_utc", return_value="2026-09-12T02:00:00Z"):
                command = client.get(f"/api/v8/contents/1/update-data/commands/{accepted.json()['run_id']}")
                self.assertEqual(command.status_code, 200, command.text)
                self.assertEqual((command.json()["status"], command.json()["can_requote"]), ("expired", True))
                self.assertEqual(client.get("/api/v8/media/pending-work").json()["refresh_tasks"][0]["status"], "expired")
            with storage.connect(self.db) as c:
                self.assertEqual(c.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_any_physical_send_consumes_the_one_request_even_when_unbilled(self):
        proposal = self.prepare(); accepted = self.execute(proposal)
        with storage.connect(self.db) as c:
            c.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,recorded_at) VALUES(?,'TikHub','douyin_video_detail',1,0,?)", (proposal["task_id"], AT))
        with storage.connect(self.db) as c:
            with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "请求已发送"):
                capture_manual.validate_command(c, accepted["run_id"], content_id=1, stage="detail")

    def test_expired_never_sent_task_needs_new_explicit_quote_and_keeps_old_receipt(self):
        first = self.prepare(); accepted = self.execute(first)
        with storage.connect(self.db) as c:
            replacement = refresh.prepare_media_source_refresh(c, content_id=1, request_id="renewed-by-operator", source_generation="missing", at="2026-09-12T02:00:00Z")
            self.assertNotEqual(replacement["task_id"], first["task_id"])
            self.assertEqual(c.execute("SELECT status FROM media_source_refresh_proposals WHERE id=?", (first["task_id"],)).fetchone()[0], "expired")
            self.assertEqual(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["run_id"], accepted["run_id"])
            self.assertEqual(c.execute("SELECT count(*) FROM scheduler_runs WHERE job_id=?", (capture_commands.JOB,)).fetchone()[0], 1)

    def test_expired_queued_get_and_debt_are_read_only_and_do_not_renew(self):
        from v8.media_work_queue import list_pending_media_work
        first = self.prepare(); accepted = self.execute(first); self.link_work(accepted)
        with storage.connect(self.db) as c:
            c.execute("PRAGMA query_only=ON")
            before = c.total_changes
            for _ in range(2):
                command = capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1, at="2026-09-12T02:00:00Z")
                self.assertEqual((command["status"], command["can_requote"]), ("expired", True))
                page = list_pending_media_work(c, at="2026-09-12T02:00:00Z")
                self.assertEqual((page["refresh_tasks"][0]["status"], page["refresh_tasks"][0]["can_requote"]), ("expired", True))
            self.assertEqual(c.total_changes, before)
            self.assertEqual(c.execute("SELECT status FROM media_source_refresh_proposals").fetchone()[0], "queued")
            self.assertEqual(c.execute("SELECT state FROM capture_work_items").fetchone()[0], "runnable")
            self.assertEqual(c.execute("SELECT count(*) FROM provider_usage").fetchone()[0], 0)

    def test_expired_sent_without_raw_is_held_and_never_requoted(self):
        from v8.media_work_queue import list_pending_media_work
        first = self.prepare(); accepted = self.execute(first)
        self.link_work(accepted, state="paid_identity_hold", reason="paid_identity_hold:billing_unknown")
        with storage.connect(self.db) as c:
            c.execute("INSERT INTO provider_usage(task_id,provider,operation,request_attempts,billed_requests,details_json,recorded_at) VALUES(?,'TikHub','douyin_video_detail',1,0,?,?)", (first["task_id"], json.dumps({"error_code": "billing_unknown", "sent_at": AT}), AT))
            c.commit()
            view = capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1, at="2026-09-12T02:00:00Z")
            self.assertEqual((view["status"], view["can_requote"]), ("blocked", False))
            self.assertIn("billing_unknown", view["reason"])
            self.assertIn("计费结果尚未确认", list_pending_media_work(c, at="2026-09-12T02:00:00Z")["refresh_tasks"][0]["reason_label"])
            replacement = refresh.prepare_media_source_refresh(c, content_id=1, request_id="operator-reopens", source_generation="missing", at="2026-09-12T02:00:00Z")
            self.assertEqual(replacement["task_id"], first["task_id"])
            self.assertFalse(replacement["can_requote"])
            self.assertEqual(c.execute("SELECT count(*) FROM media_source_refresh_proposals").fetchone()[0], 1)
            self.assertEqual(c.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 0)
            with patch.object(refresh, "now_utc", return_value="2026-09-12T02:00:00Z"):
                with self.assertRaises(refresh.MediaSourceRefreshError) as blocked:
                    capture_manual.validate_command(c, accepted["run_id"], content_id=1, stage="detail")
                self.assertEqual(blocked.exception.error_code, "media_source_refresh_request_limit")

    def test_expired_active_worker_cannot_be_retired_by_a_new_quote(self):
        first = self.prepare(); accepted = self.execute(first); self.link_work(accepted, state="running")
        with storage.connect(self.db) as c:
            view = capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1, at="2026-09-12T02:00:00Z")
            self.assertEqual((view["status"], view["can_requote"]), ("running", False))
            replacement = refresh.prepare_media_source_refresh(c, content_id=1, request_id="active-reopens", source_generation="missing", at="2026-09-12T02:00:00Z")
            self.assertEqual(replacement["task_id"], first["task_id"])

    def test_terminal_media_failure_is_not_reported_as_success(self):
        first = self.prepare(); accepted = self.execute(first)
        self.link_work(accepted, state="terminal", reason="media_source_changed")
        with storage.connect(self.db) as c:
            view = capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1, at=AT)
            self.assertEqual((view["status"], view["reason"]), ("failed", "media_source_changed"))

    def test_expired_managed_reacquire_creates_new_instance_preserving_old_deadline(self):
        from tests.test_v8_media_lifecycle import MediaLifecycleFixture
        from v8 import media_lifecycle as lifecycle
        base = MediaLifecycleFixture(); base.setUp(); self.addCleanup(base.doCleanups)
        with storage.connect(base.db) as c:
            storage.initialize_database(c, target_version=23)
            c.execute("INSERT INTO accounts(id,phone,enabled,created_at,updated_at) VALUES(1,'',1,?,?)", (AT, AT))
            c.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,source,created_at,updated_at) VALUES(1,'douyin','99887766','fixture','manual',?,?)", (AT, AT))
            c.execute("UPDATE content_items SET account_id=1 WHERE id=1")
        lock = base.root / "writer.lock"; lock.touch(mode=0o600)
        installed = InstalledWriterContract(base.root, base.root / "fixture.plist", base.root, base.root / "fixture.py", base.db, lock, {})
        with acquire_writer_lock(ResolvedDatabaseAccess(DatabaseAccessMode.WRITER, base.db, FileIdentity.from_stat(base.db.stat()), base.root, lock, installed)):
            archived = base.archived(base.registered())
            with storage.connect(base.db) as c:
                generation = refresh.source_generation(c, 1)
                with self.assertRaisesRegex(refresh.MediaSourceRefreshError, "免费恢复"):
                    refresh.prepare_media_source_refresh(c, content_id=1, request_id="archive", source_generation=generation, at=AT)
            with storage.connect(base.db) as c, storage.transaction(c):
                old = lifecycle.update_state(c, archived, {"storage_state": "expired", "operation_state": "idle", "deleted_at": archived["state"]["delete_due_at"], "deleted_members": ["m0000"]}, archived["state"]["revision"])
            with storage.connect(base.db) as c:
                quote = refresh.prepare_media_source_refresh(c, content_id=1, request_id="expired", source_generation=generation, at=AT)
                accepted = refresh.execute_media_source_refresh(c, content_id=1, task_id=quote["task_id"], source_generation=generation, at=AT)
            raw = self.singleton_raw(accepted, db=base.db, add_source=False)["raw"]
            new_source = base.source("video", suffix="-reacquired")
            with storage.connect(base.db) as c:
                source = c.execute("SELECT * FROM evidence_artifacts WHERE id=?", (new_source,)).fetchone()
                metadata = json.loads(source["metadata_json"]); metadata["raw_response_id"] = raw
                c.execute("UPDATE evidence_artifacts SET metadata_json=? WHERE id=?", (json.dumps(metadata), new_source))
            new_slot = base.slot(new_source)
            intent = lifecycle.prepare_download(1, new_source, base.media_root, base.db, new_slot,
                reacquire_request_id=quote["task_id"], download_source_sha256=source["sha256"])
            self.assertIsNotNone(intent)
            new = base.register(intent, new_slot)
            self.assertNotEqual(new["bundle_id"], old["bundle_id"])
            with storage.connect(base.db) as c:
                retained = lifecycle.load_bundle(c, old["bundle_id"])
                self.assertEqual(retained["state"], old["state"])
                self.assertEqual(retained["state"]["storage_state"], "expired")
                self.assertEqual(lifecycle.current_bundle(c, 1)["bundle_id"], new["bundle_id"])
                self.assertEqual(capture_commands.read_command(c, run_id=accepted["run_id"], content_id=1)["kind"], "media_source_refresh")


if __name__ == "__main__":
    unittest.main()
