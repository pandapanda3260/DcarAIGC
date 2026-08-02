from __future__ import annotations

import io
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import v8.api as api_module
from v8.contracts import CURRENT_REPORT_VERSION
from v8.evaluation import evaluate_content
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc


app = api_module.app


class V8ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client_context = TestClient(app)
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)

    def test_health_reports_v8_database(self) -> None:
        response = self.client.get("/api/v8/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(response.json()["database"], "dcar_insight.sqlite3")

    def test_overview_has_three_shanghai_windows_and_no_fake_forecast(self) -> None:
        response = self.client.get("/api/v8/overview")
        self.assertEqual(response.status_code, 200)
        value = response.json()
        self.assertEqual(set(value["windows"]), {"yesterday", "this_week", "last_week"})
        self.assertEqual(value["timezone"], "Asia/Shanghai")
        for window in value["windows"].values():
            metrics = window["metrics"]
            self.assertEqual(metrics["estimated_new_users"]["value"], None)
            self.assertEqual(metrics["estimated_new_users"]["unit"], "person")
            self.assertNotEqual(metrics["estimated_new_users"]["status"], "partial")

    def test_five_page_read_models_use_migrated_v8_data(self) -> None:
        tasks = self.client.get("/api/v8/tasks")
        accounts = self.client.post(
            "/api/v8/accounts/search", json={"query": "13800138000"}
        )
        contents = self.client.post(
            "/api/v8/contents/search", json={"page": 1, "page_size": 20}
        )
        pending = self.client.post(
            "/api/v8/contents/search",
            json={"review_status": "pending", "page_size": 1},
        )
        selling_points = self.client.get("/api/v8/selling-points")

        self.assertEqual(tasks.status_code, 200)
        self.assertEqual(tasks.json()["total"], len(tasks.json()["items"]))
        for task in tasks.json()["items"]:
            self.assertTrue(
                {
                    "id", "task_type", "period_start", "period_end", "task_status",
                    "content_count", "missing_boundary_count", "revision_count",
                }.issubset(task)
            )
        self.assertEqual(accounts.status_code, 200)
        self.assertEqual(accounts.json()["total"], 0)
        self.assertEqual(accounts.json()["legacy_unassociated_content_count"], 776)
        self.assertEqual(contents.status_code, 200)
        self.assertEqual(contents.json()["total"], 776)
        self.assertEqual(len(contents.json()["items"]), 20)
        self.assertEqual(pending.status_code, 200)
        overview = self.client.get("/api/v8/overview")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(
            pending.json()["total"], overview.json()["data_quality"]["pending_reviews"]
        )
        self.assertEqual(selling_points.status_code, 200)
        self.assertEqual(
            selling_points.json()["taxonomy"]["version"], "selling-points-v5.0"
        )
        self.assertEqual(len(selling_points.json()["items"]), 25)
        m_points = [
            item for item in selling_points.json()["items"] if item["code"].startswith("M")
        ]
        self.assertTrue(m_points)
        self.assertTrue(all(item["primary_hits"] == 0 for item in m_points))

    def test_content_filters_preserve_migrated_enums(self) -> None:
        response = self.client.post(
            "/api/v8/contents/search",
            json={"account_type": "boutique_ip", "content_direction": "new_car"},
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreater(response.json()["total"], 0)
        for item in response.json()["items"]:
            self.assertEqual(item["account_type"], "boutique_ip")
            self.assertEqual(item["content_direction"], "new_car")

    def test_all_five_v7_revisions_are_listed_read_only(self) -> None:
        response = self.client.get("/api/v7/history/reports")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["revisions"]), 5)
        first = response.json()["revisions"][0]
        report = self.client.get(
            f"/api/v7/history/reports/{first['run_id']}/revisions/{first['revision']}"
        )
        self.assertEqual(report.status_code, 200)
        self.assertEqual(report.json()["report_version"], "channel-structured-conclusions-v7.0")

    def test_existing_frontend_read_routes_remain_available(self) -> None:
        overview = self.client.get("/api/overview")
        latest = self.client.get("/api/report/latest")
        runs = self.client.get("/api/runs")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(latest.status_code, 200)
        self.assertEqual(runs.status_code, 200)
        self.assertEqual(overview.json()["report_version"], "channel-structured-conclusions-v7.0")

    def test_legacy_writes_return_migration_conflict(self) -> None:
        response = self.client.post("/api/runs/full", json={})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["migration_target"], "/api/v8")

    def test_phone_like_search_value_is_not_written_to_request_log(self) -> None:
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logger = logging.getLogger("dcar.api")
        previous_level = logger.level
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            response = self.client.get("/api/v8/health?phone=13800138000")
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)
        self.assertEqual(response.status_code, 200)
        log_value = stream.getvalue()
        self.assertIn("/api/v8/health", log_value)
        self.assertNotIn("13800138000", log_value)
        self.assertNotIn("phone=", log_value)


class V8ReviewAndTaxonomyApiTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.db = Path(self.temp.name) / "api.sqlite3"
        self.original_db = api_module.API_DB_PATH
        self.original_reports_root = api_module.API_REPORTS_ROOT
        api_module.API_DB_PATH = self.db
        api_module.API_REPORTS_ROOT = Path(self.temp.name) / "reports"
        with connect(self.db) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            point = connection.execute(
                """
                INSERT INTO selling_points(
                    taxonomy_id, code, tier, label, definition, positive_evidence_json
                ) VALUES ('taxonomy', 'C1', 'core', '汽车服务', 'test', '["保养"]')
                """
            )
            connection.execute(
                "INSERT INTO selling_point_scenes(selling_point_id, scene) VALUES (?, 'media')",
                (point.lastrowid,),
            )
            content = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url, published_at, title, body,
                    content_type, imported_at, created_at, updated_at
                ) VALUES (
                    'A2BC3D', 'douyin', '1', 'https://www.douyin.com/video/1', '2026-07-01T04:00:00Z',
                    '汽车保养', '保养知识', 'video', ?, ?, ?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()
            self.content_id = int(content.lastrowid)
        evaluation = evaluate_content(self.content_id, db_path=self.db)
        with connect(self.db) as connection:
            queue = connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (?, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (self.content_id, evaluation.evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            self.queue_id = int(queue.lastrowid)
        self.client_context = TestClient(app)
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)
        api_module.API_DB_PATH = self.original_db
        api_module.API_REPORTS_ROOT = self.original_reports_root
        self.temp.cleanup()

    def test_review_api_starts_and_resolves_as_append_only_override(self) -> None:
        listed = self.client.get("/api/v8/reviews")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.json()["total"], 1)
        self.assertEqual(listed.json()["status_counts"], {"pending": 1})

        started = self.client.post(f"/api/v8/reviews/{self.queue_id}/start")
        self.assertEqual(started.status_code, 200)
        self.assertEqual(started.json()["status"], "in_review")
        resolved = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "decision": "override",
                "reason": "画面明确展示汽车保养流程",
                "reviewer": "测试复核员",
                "evidence_type": "visual_summary",
                "evidence_text": "连续画面展示机油更换和车辆保养操作",
                "primary_selling_point_code": "C1",
                "selling_point_score": 92,
                "selling_point_included": True,
                "content_automotive_score": 95,
                "content_direction": "media",
            },
        )
        self.assertEqual(resolved.status_code, 200)
        self.assertEqual(resolved.json()["status"], "resolved")
        with connect(self.db) as connection:
            versions = connection.execute(
                "SELECT evaluation_source FROM evaluation_versions ORDER BY id"
            ).fetchall()
            evidence_count = connection.execute(
                "SELECT COUNT(*) FROM manual_evidence"
            ).fetchone()[0]
            status = connection.execute(
                "SELECT status FROM review_queue WHERE id=?", (self.queue_id,)
            ).fetchone()[0]
        self.assertEqual([row[0] for row in versions], ["automatic", "manual_review"])
        self.assertEqual(evidence_count, 1)
        self.assertEqual(status, "resolved")

    def test_selling_point_api_edits_only_a_draft_then_publishes(self) -> None:
        drafted = self.client.post("/api/v8/selling-points/draft")
        self.assertEqual(drafted.status_code, 200)
        self.assertEqual(drafted.json()["version"], "selling-points-v5.1")
        updated = self.client.patch(
            "/api/v8/selling-points/items/C1",
            json={
                "tier": "core",
                "label": "汽车养护服务",
                "definition": "车辆保养与维修能力",
                "positive_evidence": ["保养", "维修"],
                "negative_evidence": [],
                "boundary_rules": ["必须有明确服务能力"],
                "scenes": ["media", "used_car"],
            },
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["scenes"], ["media", "used_car"])
        published = self.client.post("/api/v8/selling-points/publish")
        self.assertEqual(published.status_code, 200)
        self.assertEqual(published.json()["status"], "published")
        current = self.client.get("/api/v8/selling-points")
        self.assertEqual(current.json()["taxonomy"]["version"], "selling-points-v5.1")
        self.assertEqual(current.json()["items"][0]["label"], "汽车养护服务")

    def test_custom_task_generates_revision_and_downloads_run_scoped_files(self) -> None:
        created = self.client.post(
            "/api/v8/tasks",
            json={"period_start": "2026-07-01", "period_end": "2026-07-01"},
        )
        self.assertEqual(created.status_code, 200)
        value = created.json()
        self.assertEqual(value["task_status"], "partial")
        self.assertEqual(len(value["revisions"]), 1)
        task_id = value["id"]
        report = self.client.get(f"/api/v8/tasks/{task_id}/revisions/1/report")
        self.assertEqual(report.status_code, 200)
        self.assertEqual(report.json()["metadata"]["task_id"], task_id)
        download = self.client.get(
            f"/api/v8/tasks/{task_id}/revisions/1/files/report-markdown"
        )
        self.assertEqual(download.status_code, 200)
        self.assertTrue(download.content.startswith(b"# "))
        image = self.client.get(
            f"/api/v8/tasks/{task_id}/revisions/1/files/summary-image"
        )
        self.assertEqual(image.status_code, 200)
        self.assertIn(image.headers["content-type"], {"image/svg+xml", "image/png"})

    def test_account_crud_import_and_export_keep_full_phone_with_post_search(self) -> None:
        created = self.client.post(
            "/api/v8/accounts",
            json={
                "phone": "13800138000", "operator_name": "运营甲",
                "account_type": "original", "content_direction": "new_car",
                "platforms": [{"platform": "douyin", "uid": "123456789", "nickname": "账号甲", "real_name_status": "yes"}],
            },
        )
        self.assertEqual(created.status_code, 200)
        account_id = created.json()["id"]
        updated = self.client.patch(
            f"/api/v8/accounts/{account_id}",
            json={
                "phone": "+86 138-0013-8000", "operator_name": "运营乙",
                "account_type": "boutique_ip", "content_direction": "media",
                "platforms": [{"platform": "douyin", "uid": "123456789", "nickname": "账号乙", "real_name_status": "no"}],
            },
        )
        self.assertEqual(updated.status_code, 200)
        searched = self.client.post("/api/v8/accounts/search", json={"query": "13800138000"})
        self.assertEqual(searched.json()["total"], 1)
        self.assertEqual(searched.json()["items"][0]["phone"], "+86 138-0013-8000")
        exported = self.client.get("/api/v8/accounts/export")
        self.assertEqual(exported.status_code, 200)
        self.assertIn("+86 138-0013-8000", exported.content.decode("utf-8-sig"))

    def test_content_validate_import_search_and_export(self) -> None:
        invalid = self.client.post(
            "/api/v8/contents/validate",
            json={"source_name": "input.csv", "rows": [{"platform": "douyin", "canonical_url": "https://v.douyin.com/abc"}]},
        )
        self.assertEqual(invalid.status_code, 200)
        self.assertEqual(invalid.json()["rejected"], 1)
        imported = self.client.post(
            "/api/v8/contents/import",
            json={
                "source_name": "input.csv",
                "rows": [{
                    "platform": "douyin", "canonical_url": "https://www.douyin.com/video/999999999",
                    "title": "导入的汽车内容", "body": "导入的汽车内容完整正文",
                    "published_at": "2026-07-03T08:00:00+08:00",
                }],
            },
        )
        self.assertEqual(imported.status_code, 200)
        self.assertEqual(imported.json()["inserted_rows"], 1)
        searched = self.client.post(
            "/api/v8/contents/search", json={"query": "999999999", "page_size": 10}
        )
        self.assertEqual(searched.status_code, 200)
        self.assertEqual(searched.json()["total"], 1)
        exported = self.client.get("/api/v8/contents/export")
        self.assertEqual(exported.status_code, 200)
        self.assertIn("999999999", exported.content.decode("utf-8-sig"))

    def test_update_data_route_returns_provider_execution_result(self) -> None:
        expected = {
            "content_id": self.content_id, "status": "succeeded", "stages": [],
            "evaluation_id": 1, "evaluation_created": False,
            "provider_cost": 0.001, "currency": "USD",
        }
        with patch.object(api_module, "update_content_data", return_value=expected) as mocked:
            response = self.client.post(f"/api/v8/contents/{self.content_id}/update-data")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        mocked.assert_called_once_with(self.content_id, db_path=self.db)


if __name__ == "__main__":
    unittest.main()
