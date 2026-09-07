"""Real legacy due/lease/A-B/raw path; only external installation and HTTP are fixtures."""
from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from tests import test_v8_pipeline as pipeline_fixture
from tests import test_v8_transport_members as members_fixture
from tests import test_v8_transport_cohort as cohort_fixture
from tests import test_v8_transport_campaign as campaign_fixture
from tests import test_v8_provider_transport as transport_fixture
from tests import test_v8_tikhub_scan as scan_fixture
from tests import test_v8_transport_natural_due as due_fixture
from tests.test_v8_runtime_database import InstalledRuntimeFixture
from v8 import capture, capture_authorizations as auth, capture_release as release
from v8 import capture_planning, durable_runs, metric_observations, paid_drain, providers, schema_v20, tikhub_scan
from v8 import transport_execution, comment_paging
from v8.profile_activations import append_activation
from v8.provider_transport import request_json
from v8.runtime_database import acquire_writer_lock, load_installed_writer_contract, resolve_installed_database_access
from v8.storage import connect, initialize_database, transaction

AT = members_fixture.AT
OPERATION = "douyin_user_posts"
ACTIVATED_AT = "2026-09-01T16:00:00Z"


class LegacyContinuityTest(unittest.TestCase):
    platform = "douyin"
    member_count = 20
    claim_round = pipeline_fixture.PipelineTest.claim_round
    _natural_children = members_fixture.TransportMembersTest._natural_children
    _transport = campaign_fixture.TransportCampaignTest._transport
    _scope = due_fixture.TransportNaturalDueTest._scope
    _claim_queue = due_fixture.TransportNaturalDueTest._claim_queue

    def roster(self, connection):
        if self.platform == "douyin":
            return cohort_fixture.TransportCohortTest._roster(self, connection, count=self.member_count)
        for index in range(1, 21):
            connection.execute("INSERT INTO accounts(id,phone,created_at,updated_at) VALUES(?,?,?,?)", (index, str(index), cohort_fixture.INITIAL_AT, cohort_fixture.INITIAL_AT))
            connection.execute("INSERT INTO account_platform_identities(id,account_id,platform,uid,source,created_at,updated_at) VALUES(?,?,'xiaohongshu',?,'manual',?,?)",
                (index, index, f"{index:024x}", cohort_fixture.INITIAL_AT, cohort_fixture.INITIAL_AT))
        body = json.dumps([f"uid:xiaohongshu:{index:024x}" for index in range(1, 21)]).encode()
        checksum = hashlib.sha256(body).hexdigest()
        source = self.root / "xhs-roster.json"
        source.write_bytes(body)
        identifier = connection.execute("""INSERT INTO account_roster_snapshots(source_family,source_type,scope_key,scope_json,
            source_instance_id,source_captured_at,accepted_at,declared_count,member_count,members_sha256,source_sha256,source_path,contract_version,metadata_json)
            VALUES('system','system_managed','xhs','{}','xhs',?,?,20,20,?,?,?,'system-managed-roster-v1','{}')""",
            (cohort_fixture.INITIAL_AT, cohort_fixture.INITIAL_AT, checksum, checksum, str(source))).lastrowid
        connection.executemany("""INSERT INTO account_roster_members(snapshot_id,account_identity_id,platform,member_key,uid,
            monitoring_status,authorization_status,metadata_json) VALUES(?,?,'xiaohongshu',?,?,'unknown','unknown','{}')""",
            [(identifier, index, f"uid:xiaohongshu:{index:024x}", f"{index:024x}") for index in range(1, 21)])
        return {"id": identifier, "members_sha256": checksum}

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="legacy-continuity-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.runtime = InstalledRuntimeFixture(self.root / "installation")
        self.db = self.runtime.database
        self.db.write_bytes(b"")
        self.raw_root = self.root / "raw"
        with connect(self.db) as connection:
            initialize_database(connection)
            roster = self.roster(connection)
            connection.commit()
            with transaction(connection):
                self.active = append_activation(connection, profile_id="tikhub_managed_v1",
                    roster_snapshot_id=roster["id"], roster_members_sha256=roster["members_sha256"],
                    effective_at=ACTIVATED_AT, build_receipt_sha256="b"*64,
                    actor="fixture", reason="fixture", metadata={}, created_at=cohort_fixture.INITIAL_AT)
                paid_drain.issue_activation_permit_in_transaction(connection,
                    activation_id=self.active["activation_id"], drain_id="fixture-initial", source_activation_id=self.active["activation_id"],
                    business_day="2026-09-02", planned_effective_at=ACTIVATED_AT,
                    build_receipt_sha256="b"*64, runtime_root_receipt_sha256="c"*64, now=ACTIVATED_AT)
            with patch.object(schema_v20, "datetime", wraps=datetime) as clock:
                clock.now.return_value = datetime.fromisoformat(AT.replace("Z", "+00:00"))
                schema_v20.migrate(connection)
        installed = load_installed_writer_contract(home=self.runtime.home)
        access = resolve_installed_database_access("writer", database=self.db, project_root=self.runtime.project,
            environ=self.runtime.environment, installed=installed)
        self.enterContext(acquire_writer_lock(access))
        self.children = {}
        self._natural_children(range(1, self.member_count + 1))
        if self.platform == "xiaohongshu":
            with connect(self.db) as connection, transaction(connection):
                for child in self.children.values():
                    durable_runs.checkpoint(connection, child, {"cursor": "", "reference": None}, now=AT)
        self.transport = self._transport()
        self.evidence = {"active": self.active, "build_sha256": "b"*64, "runtime_sha256": "c"*64,
            "config_sha256": "d"*64, "manifest": self.transport["manifest"], "deployment": {"status": "candidate"}}
        self.qualification = {"self_sha256": "f"*64, "payload": {"expires_at": "2026-09-07T05:30:00Z"}}
        self.enterContext(patch.object(release, "_installed_evidence", return_value=self.evidence))
        self.enterContext(patch.object(release, "_qualification", return_value=self.qualification))
        self.enterContext(patch.object(providers, "_freeze_tikhub_transport", return_value=self.transport))
        self.enterContext(patch.object(providers, "_load_key", return_value="fixture-no-network-key"))
        for module in (capture, providers, tikhub_scan, transport_execution, metric_observations):
            self.enterContext(patch.object(module, "now_utc", return_value=AT))
        self.calls = []
        self.enterContext(patch.object(comment_paging, "MANIFEST_ROOT", self.root / "comment-manifests"))
        self.enterContext(patch.object(comment_paging, "now_utc", return_value=AT))
        self.fail_http = False
        self.enterContext(patch.object(providers, "request_json_transport", side_effect=self._http))

    def _http(self, request, **kwargs):
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        if self.platform == "xiaohongshu":
            return self.xhs_http(request, params, **kwargs)
        rank = int(params["sec_user_id"][0].split("r")[-1]) if "sec_user_id" in params else int(params.get("aweme_id", params.get("aweme_ids"))[0])
        self.calls.append(rank)
        item = scan_fixture.dy_item(number=rank, author={"uid": f"douyin-{rank:02d}", "nickname": "fixture"},
            create_time=scan_fixture.epoch("2026-09-05T10:00:00Z"), statistics={"play_count": 321, "digg_count": 10,
                "comment_count": 2, "share_count": 1, "collect_count": 3})
        if "sec_user_id" in params:
            response_body = scan_fixture.dy_page([item], more=False, cursor=0)
        elif "statistics" in request.full_url:
            response_body = {"code": 200, "data": {"statistics_list": [{"aweme_id": str(rank), "play_count": 321, "digg_count": 10, "share_count": 1}]}}
        elif "comments" in request.full_url:
            response_body = {"code": 200, "data": {"comments": [{"cid": str(rank), "text": "fixture", "user": {"uid": str(rank)}}],
                "total": 1, "has_more": False, "cursor": 0}}
        else:
            item["aweme_id"] = str(rank)
            response_body = {"code": 200, "data": {"aweme_detail": item}}
        body = json.dumps(response_body).encode()
        response = transport_fixture.FakeResponse(body, response_url=request.full_url,
            headers={"Content-Length": str(len(body))})
        return request_json(request, **kwargs, opener=transport_fixture.FakeOpener(response), clock=lambda: AT, chunk_size=13)

    def xhs_http(self, request, params, **kwargs):
        rank = int(params.get("user_id", params.get("note_id"))[0], 16)
        self.calls.append(rank)
        note = {"note_id": f"{rank:024x}", "id": f"{rank:024x}", "type": "video", "title": "fixture",
            "desc": "fixture", "time": scan_fixture.epoch("2026-09-05T10:00:00Z"),
            "user": {"user_id": f"{rank:024x}"}, "liked_count": "5", "comments_count": "2", "collected_count": "3", "shared_count": "1"}
        if "user_id" in params:
            data = {"notes": [note], "has_more": False, "cursor": ""}
        elif "comment" in request.full_url:
            data = {"comments": [{"id": str(rank), "content": "fixture", "user": {"user_id": f"{rank:024x}"}}],
                "has_more": False, "cursor": "", "total": 1}
        else:
            data = note
        body = json.dumps({"code": 200, "data": {"code": 0, "success": True, "data": data}}).encode()
        response = transport_fixture.FakeResponse(body, response_url=request.full_url, headers={"Content-Length": str(len(body))})
        return request_json(request, **kwargs, opener=transport_fixture.FakeOpener(response), clock=lambda: AT, chunk_size=13)

    def freeze(self, operation=OPERATION):
        with connect(self.db) as connection, transaction(connection):
            result = release.freeze_continuity_permit(connection, operation=operation, qualification_receipt_id=42, at=AT)
            release.publish_continuity_gate(connection, operation=operation, at=AT)
            return result

    def queue(self, kind):
        with connect(self.db) as connection, transaction(connection):
            for index in range(1, 21):
                connection.execute("""INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,account_id,
                    raw_account_uid,content_type,published_at,imported_at,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,'video','2026-09-05T10:00:00Z',?,?,?)""",
                    (index, f"C{index:05d}", self.platform, str(index) if self.platform == "douyin" else f"{index:024x}",
                     f"https://fixture.invalid/{index}", index, f"douyin-{index:02d}" if self.platform == "douyin" else f"{index:024x}", AT, AT, AT))
            contents = [dict(row) for row in connection.execute("SELECT * FROM content_items ORDER BY id")]
        for content in contents:
            self.identity_id = content["account_id"]
            with patch.object(due_fixture, "AT", AT):
                self._claim_queue(kind, content)

    def test_legacy_statistics_queue_one_request_preserves_metric_mapping(self):
        self.queue("metrics_backfill")
        self.permit = self.freeze("douyin_video_statistics")
        result = self.execute(1)
        self.assertTrue(result["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT view_count FROM content_metric_observations WHERE content_id=1 ORDER BY id DESC LIMIT 1").fetchone()[0], 321)
        with self.assertRaises(auth.AuthorizationError):
            self.execute(1)
        self.assertEqual(self.calls, [1])

    def test_legacy_detail_queue_executes_one_existing_detail_adapter(self):
        self.queue("content_pipeline")
        self.permit = self.freeze("douyin_video_detail")
        self.assertTrue(self.execute(1)["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT source FROM provider_raw_responses").fetchone()[0], "live_applied")

    def test_legacy_comment_queue_uses_single_page_materializer(self):
        self.queue("comments_refresh")
        self.permit = self.freeze("douyin_video_comments")
        result = self.execute(1)
        self.assertTrue(result["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM comment_capture_pages").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT count(*) FROM comments").fetchone()[0], 1)

    def execute(self, rank):
        return transport_execution.execute_continuity_member_page(self.permit["permit_id"], rank,
            db_path=self.db, raw_root=self.raw_root, at=AT)

    def test_existing_legacy_twenty_execute_once_and_ordinary_requires_accepted(self):
        self.permit = self.freeze()
        self.assertEqual(self.freeze(), self.permit)
        results = [self.execute(rank) for rank in range(1, 21)]
        self.assertEqual(self.calls, list(range(1, 21)))
        self.assertTrue(all(result["materialized"] and not result["qualified"] for result in results))
        with connect(self.db) as connection, transaction(connection):
            permit = dict(connection.execute("SELECT * FROM transport_continuity_permits").fetchone())
            self.assertEqual(len(release._continuity_complete(connection, permit)), 20)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_request_start_events").fetchone()[0], 20)
            self.assertEqual(connection.execute("SELECT count(*) FROM content_items").fetchone()[0], 20)
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])
            with self.assertRaisesRegex(auth.AuthorizationError, "Candidate cannot"):
                release.publish_operation_gate(connection, operation=OPERATION, at=AT)
            self.evidence["deployment"]["status"] = "accepted"
            gate = release.publish_operation_gate(connection, operation=OPERATION, at=AT)
            self.assertTrue(gate["ordinary_paid_authorized"])
            self.assertFalse(gate["coverage_complete"])
        with self.assertRaises(auth.AuthorizationError):
            self.execute(1)
        self.assertEqual(len(self.calls), 20)

    def test_cursor_or_integrated_route_change_is_rejected_before_paid_start(self):
        self.permit = self.freeze()
        with connect(self.db) as connection, transaction(connection):
            first = next(iter(self.children.values()))
            durable_runs.checkpoint(connection, first, {"cursor": 123}, now=AT)
        with self.assertRaises(auth.AuthorizationError):
            self.execute(1)
        with connect(self.db) as connection, transaction(connection):
            current = capture_planning.resolve_route(connection, account_id=2, content_id=None, operation=OPERATION, at=AT)
            capture_planning.assign_route(connection, scope_type="account", scope_key="2", account_id=2, operation=OPERATION,
                provider="tikhub", expected_generation=current["generation"], route="integrated", mode="active", effective_at=AT, recorded_at=AT)
        with self.assertRaisesRegex(auth.AuthorizationError, "legacy route"):
            self.execute(2)
        self.assertEqual(self.calls, [])


class XiaohongshuContinuityTest(unittest.TestCase):
    member_count = 20
    platform = "xiaohongshu"
    setUp = LegacyContinuityTest.setUp
    roster = LegacyContinuityTest.roster
    claim_round = LegacyContinuityTest.claim_round
    _natural_children = LegacyContinuityTest._natural_children
    _transport = LegacyContinuityTest._transport
    _scope = LegacyContinuityTest._scope
    _claim_queue = LegacyContinuityTest._claim_queue
    _http = LegacyContinuityTest._http
    xhs_http = LegacyContinuityTest.xhs_http
    freeze = LegacyContinuityTest.freeze
    execute = LegacyContinuityTest.execute
    queue = LegacyContinuityTest.queue

    def test_xhs_discovery_materializes_one_real_enveloped_page(self):
        self.permit = self.freeze("xiaohongshu_user_posts")
        self.assertTrue(self.execute(1)["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT platform FROM content_items").fetchone()[0], "xiaohongshu")

    def test_xhs_statistics_uses_actual_note_adapter_and_field_mapping(self):
        self.queue("metrics_backfill")
        self.permit = self.freeze("xiaohongshu_note_statistics")
        self.assertTrue(self.execute(1)["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            row = connection.execute("SELECT like_count,comment_count,collect_count,share_count FROM content_metric_observations WHERE content_id=1 ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(tuple(row), (5, 2, 3, 1))

    def test_xhs_detail_materializes_actual_note_adapter(self):
        self.queue("content_pipeline")
        self.permit = self.freeze("xiaohongshu_note_detail")
        self.assertTrue(self.execute(1)["materialized"])
        self.assertEqual(self.calls, [1])

    def test_xhs_comments_materializes_one_page_without_followup_call(self):
        self.queue("comments_refresh")
        self.permit = self.freeze("xiaohongshu_note_comments")
        self.assertTrue(self.execute(1)["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM comment_capture_pages").fetchone()[0], 1)

    def test_content_raw_crash_replays_same_stats_without_network(self):
        self.queue("metrics_backfill")
        self.permit = self.freeze("xiaohongshu_note_statistics")
        with patch.object(providers, "_store_stage_result", side_effect=RuntimeError("fixture content crash")):
            with self.assertRaisesRegex(RuntimeError, "fixture content crash"):
                self.execute(1)
        with patch.object(providers, "request_json_transport", side_effect=AssertionError("replay HTTP")):
            result = transport_execution.resume_continuity_member_local(self.permit["permit_id"], 1,
                db_path=self.db, raw_root=self.raw_root, at=AT)
        self.assertEqual(result["provider_calls"], 0)
        self.assertTrue(result["materialized"])
        self.assertEqual(self.calls, [1])
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT like_count FROM content_metric_observations WHERE content_id=1").fetchone()[0], 5)
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_request_start_events").fetchone()[0], 1)

    def test_response_commit_before_scanner_apply_replays_only_exact_frozen_page(self):
        self.permit = self.freeze("xiaohongshu_user_posts")
        with patch.object(tikhub_scan, "_apply", side_effect=RuntimeError("fixture post-C crash")):
            with self.assertRaisesRegex(RuntimeError, "fixture post-C crash"):
                self.execute(1)
        with patch.object(providers, "request_json_transport", side_effect=AssertionError("replay HTTP")):
            result = transport_execution.resume_continuity_member_local(self.permit["permit_id"], 1,
                db_path=self.db, raw_root=self.raw_root, at=AT)
        self.assertTrue(result["materialized"])
        self.assertEqual(result["provider_calls"], 0)
        self.assertEqual(self.calls, [1])

    def test_local_crash_retains_raw_and_cannot_purchase_same_member_again(self):
        self.permit = self.freeze("xiaohongshu_user_posts")
        with patch.object(tikhub_scan, "_materialize_pending", side_effect=RuntimeError("fixture crash")):
            with self.assertRaisesRegex(RuntimeError, "fixture crash"):
                self.execute(1)
        with self.assertRaises(auth.AuthorizationError):
            self.execute(1)
        self.assertEqual(self.calls, [1])
        with patch.object(providers, "request_json_transport", side_effect=AssertionError("local replay attempted HTTP")):
            resumed = transport_execution.resume_continuity_member_local(self.permit["permit_id"], 1,
                db_path=self.db, raw_root=self.raw_root, at=AT)
        self.assertTrue(resumed["materialized"])
        self.assertEqual(resumed["provider_calls"], 0)
        with connect(self.db) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM provider_raw_responses").fetchone()[0], 1)
            first = next(iter(self.children.values()))
            run = durable_runs.get_run(first.scheduler_run_id, db_path=self.db)
            self.assertIsNone(run["details"]["checkpoint"]["pending_materialization"])
        for rank in range(2, 21):
            self.execute(rank)
        with connect(self.db) as connection:
            permit = dict(connection.execute("SELECT * FROM transport_continuity_permits").fetchone())
            self.assertEqual(len(release._continuity_complete(connection, permit)), 20)
        self.assertEqual(self.calls, list(range(1, 21)))

    def test_missing_twenty_and_profile_drift_do_not_write_permit(self):
        with connect(self.db) as connection, transaction(connection):
            first = next(iter(self.children.values()))
            durable_runs.checkpoint(connection, first, {"pending_materialization": {}}, now=AT)
        with connect(self.db) as connection, transaction(connection):
            before = connection.total_changes
            with self.assertRaisesRegex(auth.AuthorizationError, "missing_frozen_work"):
                release.freeze_continuity_permit(connection, operation="xiaohongshu_user_posts", qualification_receipt_id=42, at=AT)
            self.assertEqual(connection.total_changes, before)
        self.evidence["active"] = {**self.active, "profile_id": "integrated_route_v1"}
        with self.assertRaisesRegex(auth.AuthorizationError, "Mode B legacy only"):
            self.freeze()
        self.assertEqual(self.calls, [])
