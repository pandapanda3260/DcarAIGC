from __future__ import annotations

import io
import json
import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

import v8.api as api_module
from v8.contracts import CURRENT_REPORT_VERSION
from v8.evaluation import evaluate_content
from v8.reports import create_task
from v8.storage import PROJECT_ROOT, connect, initialize_database, now_utc


app = api_module.app


class V8ApiTest(unittest.TestCase):
    def test_openapi_version_matches_v8_contract_release(self) -> None:
        self.assertEqual(app.version, "8.2")

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
            self.assertIn("duplicate_rate", metrics)
            self.assertEqual(list(window["channels"]), ["douyin", "xiaohongshu"])
            for channel in window["channels"].values():
                self.assertEqual(
                    list(channel["summary"]["metrics"]),
                    [
                        "selling_point_count_share",
                        "core_selling_point_count_share",
                        "selling_point_exposure_share",
                        "core_selling_point_exposure_share",
                        "content_verticality",
                        "audience_verticality",
                        "acquisition_potential",
                    ],
                )
                self.assertEqual(list(channel["scenes"]), ["used_car", "new_car", "media"])
                for scene in channel["scenes"].values():
                    self.assertEqual(
                        list(scene["metrics"]), list(channel["summary"]["metrics"])
                    )
        self.assertIn("duplicate_fingerprint_coverage", value["data_quality"])
        self.assertIn("duplicate_calibration_ready", value["data_quality"])

    def test_overview_channel_conclusions_restore_v7_denominators_and_scores(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "overview.sqlite3"
            created_at = "2026-08-02T08:00:00Z"
            with connect(db_path) as connection:
                initialize_database(connection)
                connection.execute(
                    """
                    INSERT INTO taxonomy_versions(
                        id,version,status,definition,created_at,published_at
                    ) VALUES ('tax-current','selling-points-test','published','{}',?,?)
                    """,
                    (created_at, created_at),
                )
                connection.executemany(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition
                    ) VALUES ('tax-current',?,?,?,'')
                    """,
                    [("E1", "core", "核心卖点"), ("C1", "other", "其他卖点")],
                )
                content_rows = [
                    ("TST1A2", "content-a", "new_car", "2026-08-02T10:00:00Z"),
                    ("TST2B3", "content-b", "used_car", "2026-08-02T11:00:00Z"),
                    ("TST3C4", "content-c", "other", "2026-08-02T12:00:00Z"),
                ]
                for link_id, platform_id, direction, published_at in content_rows:
                    connection.execute(
                        """
                        INSERT INTO content_items(
                            link_id,platform,platform_content_id,canonical_url,title,
                            content_type,published_at,evaluation_content_direction,
                            imported_at,created_at,updated_at
                        ) VALUES (?,'douyin',?,?,?,'video',?,?,?, ?,?)
                        """,
                        (
                            link_id, platform_id, f"https://example.com/{platform_id}",
                            platform_id, published_at, direction,
                            created_at, created_at, created_at,
                        ),
                    )
                ids = {
                    row["platform_content_id"]: int(row["id"])
                    for row in connection.execute(
                        "SELECT id,platform_content_id FROM content_items"
                    )
                }
                evaluations = [
                    (ids["content-a"], "a" * 64, "E1", 1, "new_car", 80, 60, 55),
                    (ids["content-b"], "b" * 64, "C1", 1, "used_car", 60, None, None),
                    (ids["content-c"], "c" * 64, None, 0, "other", 40, None, None),
                ]
                for content_id, evidence_sha, code, included, direction, content_score, audience_score, acquisition_score in evaluations:
                    connection.execute(
                        """
                        INSERT INTO evaluation_versions(
                            content_id,rule_version,taxonomy_version,evidence_sha256,
                            evaluation_source,evaluation_status,evidence_level,
                            primary_selling_point_code,selling_point_score,
                            selling_point_included,content_direction,
                            content_automotive_score,audience_automotive_score,
                            acquisition_potential_score,pending_review,payload_json,evaluated_at
                        ) VALUES (?,?,?,?,'automatic','evaluated','V3',?,90,?,?,?,?,?,0,'{}',?)
                        """,
                        (
                            content_id, api_module.RULE_VERSION, "selling-points-test",
                            evidence_sha, code, included, direction, content_score,
                            audience_score, acquisition_score, created_at,
                        ),
                    )
                connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,rule_version,taxonomy_version,evidence_sha256,
                        evaluation_source,evaluation_status,evidence_level,
                        selling_point_included,content_direction,pending_review,
                        payload_json,evaluated_at
                    ) VALUES (?,'old-rule','selling-points-test',?,'automatic',
                              'evaluated','V3',0,'new_car',0,'{}','2026-08-02T13:00:00Z')
                    """,
                    (ids["content-a"], "d" * 64),
                )
                for content_id, view_count in (
                    (ids["content-a"], 100),
                    (ids["content-b"], 300),
                    (ids["content-c"], 100),
                ):
                    connection.execute(
                        """
                        INSERT INTO content_metric_snapshots(
                            content_id,captured_at,window_key,view_count,status,source
                        ) VALUES (?,'2026-08-02T14:00:00Z','2026-08-02',?,'available','test')
                        """,
                        (content_id, view_count),
                    )
                connection.commit()
                window = api_module._window_summary(
                    connection,
                    datetime(2026, 8, 2, tzinfo=timezone.utc),
                    datetime(2026, 8, 3, tzinfo=timezone.utc),
                )

            douyin = window["channels"]["douyin"]
            summary = douyin["summary"]["metrics"]
            self.assertEqual(douyin["publication_count"], 3)
            self.assertEqual(summary["selling_point_count_share"]["percentage"], 66.67)
            self.assertEqual(summary["core_selling_point_count_share"]["percentage"], 33.33)
            self.assertEqual(summary["selling_point_exposure_share"]["percentage"], 80.0)
            self.assertEqual(summary["core_selling_point_exposure_share"]["percentage"], 20.0)
            self.assertEqual(summary["content_verticality"]["value"], 60)
            self.assertEqual(summary["audience_verticality"]["value"], 60)
            self.assertEqual(summary["audience_verticality"]["status"], "sample_only")
            self.assertEqual(summary["acquisition_potential"]["value"], 55)

            new_car = douyin["scenes"]["new_car"]["metrics"]
            used_car = douyin["scenes"]["used_car"]["metrics"]
            media = douyin["scenes"]["media"]["metrics"]
            self.assertEqual(new_car["selling_point_count_share"]["denominator"], 3)
            self.assertEqual(new_car["selling_point_exposure_share"]["denominator"], 500)
            self.assertEqual(new_car["selling_point_exposure_share"]["percentage"], 20.0)
            self.assertEqual(used_car["selling_point_exposure_share"]["percentage"], 60.0)
            self.assertEqual(media["selling_point_count_share"]["percentage"], 0.0)
            self.assertEqual(media["content_verticality"]["status"], "not_applicable")

            xiaohongshu = window["channels"]["xiaohongshu"]
            self.assertEqual(xiaohongshu["publication_count"], 0)
            self.assertTrue(
                all(
                    metric["status"] == "not_applicable"
                    for metric in xiaohongshu["summary"]["metrics"].values()
                )
            )

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
        self.assertEqual(accounts.json()["pending_platform_identity_count"], 30)
        self.assertEqual(len(accounts.json()["pending_platform_identities"]), 30)
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
        self.evaluation_id = evaluation.evaluation_id
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
                "base_evaluation_id": started.json()["base_evaluation_id"],
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

    def test_review_api_reopens_with_audit_event_and_appends_new_version(self) -> None:
        started = self.client.post(f"/api/v8/reviews/{self.queue_id}/start")
        self.assertEqual(started.status_code, 200)
        first = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": started.json()["base_evaluation_id"],
                "decision": "override",
                "reason": "第一次代理复核",
                "reviewer": "Codex代理",
                "evidence_type": "visual_summary",
                "evidence_text": "第一次人工证据摘要",
                "primary_selling_point_code": "C1",
                "selling_point_score": 82,
                "selling_point_included": True,
                "content_automotive_score": 86,
                "content_direction": "media",
            },
        )
        self.assertEqual(first.status_code, 200)
        first_evaluation_id = int(first.json()["evaluation_id"])
        with connect(self.db) as connection:
            first_review_id = int(
                connection.execute(
                    "SELECT id FROM evaluation_reviews WHERE queue_id=?",
                    (self.queue_id,),
                ).fetchone()[0]
            )

        reopened = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/reopen",
            json={
                "reason": "业务负责人要求重新核验代理判定",
                "reopened_by": "运营复核员",
            },
        )
        self.assertEqual(reopened.status_code, 200)
        self.assertEqual(reopened.json()["status"], "in_review")
        self.assertEqual(reopened.json()["base_evaluation_id"], first_evaluation_id)

        duplicate_reopen = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/reopen",
            json={"reason": "重复重开", "reopened_by": "运营复核员"},
        )
        self.assertEqual(duplicate_reopen.status_code, 409)

        second = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": reopened.json()["base_evaluation_id"],
                "decision": "override",
                "reason": "业务负责人完成二次核验",
                "reviewer": "运营复核员",
                "evidence_type": "visual_summary",
                "evidence_text": "业务人员重新查看画面后形成的新证据摘要",
                "primary_selling_point_code": "C1",
                "selling_point_score": 94,
                "selling_point_included": True,
                "content_automotive_score": 96,
                "content_direction": "media",
            },
        )
        self.assertEqual(second.status_code, 200)
        self.assertNotEqual(second.json()["evaluation_id"], first_evaluation_id)

        with connect(self.db) as connection:
            reopen_event = connection.execute(
                "SELECT * FROM review_reopen_events WHERE queue_id=?",
                (self.queue_id,),
            ).fetchone()
            sources = connection.execute(
                "SELECT evaluation_source FROM evaluation_versions ORDER BY id"
            ).fetchall()
            review_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_reviews WHERE queue_id=?",
                    (self.queue_id,),
                ).fetchone()[0]
            )
            evidence_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM manual_evidence WHERE content_id=?",
                    (self.content_id,),
                ).fetchone()[0]
            )
            queue_status = str(
                connection.execute(
                    "SELECT status FROM review_queue WHERE id=?", (self.queue_id,)
                ).fetchone()[0]
            )
        self.assertEqual(reopen_event["previous_review_id"], first_review_id)
        self.assertEqual(reopen_event["base_evaluation_id"], first_evaluation_id)
        self.assertEqual(reopen_event["reopened_by"], "运营复核员")
        self.assertEqual(reopen_event["reason"], "业务负责人要求重新核验代理判定")
        self.assertEqual(
            [row[0] for row in sources],
            ["automatic", "manual_review", "manual_review"],
        )
        self.assertEqual(review_count, 2)
        self.assertEqual(evidence_count, 2)
        self.assertEqual(queue_status, "resolved")

    def test_review_api_rejects_selling_point_scene_conflict(self) -> None:
        started = self.client.post(f"/api/v8/reviews/{self.queue_id}/start")
        self.assertEqual(started.status_code, 200)
        rejected = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": started.json()["base_evaluation_id"],
                "decision": "override",
                "reason": "尝试提交冲突场景",
                "reviewer": "测试复核员",
                "evidence_type": "visual_summary",
                "evidence_text": "画面证据摘要",
                "primary_selling_point_code": "C1",
                "selling_point_score": 90,
                "selling_point_included": True,
                "content_automotive_score": 90,
                "content_direction": "new_car",
            },
        )
        self.assertEqual(rejected.status_code, 409)
        self.assertIn("does not allow content direction", rejected.json()["detail"])
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM evaluation_reviews").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM review_queue WHERE id=?", (self.queue_id,)
                ).fetchone()[0],
                "in_review",
            )

    def test_review_api_rejects_stale_evaluation_cursor(self) -> None:
        started = self.client.post(f"/api/v8/reviews/{self.queue_id}/start")
        self.assertEqual(started.status_code, 200)
        stale_id = started.json()["base_evaluation_id"]
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET body='保养知识已更新',updated_at=? WHERE id=?",
                (now_utc(), self.content_id),
            )
            connection.commit()
        changed = evaluate_content(self.content_id, db_path=self.db)
        self.assertNotEqual(changed.evaluation_id, stale_id)
        rejected = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": stale_id,
                "decision": "confirm",
                "reason": "旧页面提交",
                "reviewer": "测试复核员",
                "evidence_type": "review_note",
                "evidence_text": "这是基于旧评估打开的复核页面",
            },
        )
        self.assertEqual(rejected.status_code, 409)
        self.assertIn("请刷新证据", rejected.json()["detail"])

    def test_evidence_media_file_and_processing_search_are_readable(self) -> None:
        media_path = Path(self.temp.name) / "evidence.jpg"
        asr_path = Path(self.temp.name) / "asr.json"
        ocr_path = Path(self.temp.name) / "ocr.json"
        media_path.write_bytes(b"local-image-evidence")
        asr_path.write_text(
            json.dumps({"status": "success", "model": "pinned", "text": "完整的本地语音证据"}),
            encoding="utf-8",
        )
        ocr_path.write_text(
            json.dumps(
                {
                    "status": "success",
                    "ocr_observation_count": 3,
                    "combined_text": "",
                    "observations": [
                        {"status": "success", "text": "关键帧文字证据"},
                        {"status": "success", "text": "车辆保养流程"},
                        {"status": "success", "text": "关键帧文字证据"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        with connect(self.db) as connection:
            artifact_ids = []
            for artifact_type, path in (("media", media_path), ("asr", asr_path), ("ocr", ocr_path)):
                cursor = connection.execute(
                    """
                    INSERT INTO evidence_artifacts(
                        content_id,artifact_type,local_path,status,sha256,created_at
                    ) VALUES (?,?,?,'available',?,?)
                    """,
                    (self.content_id, artifact_type, str(path), artifact_type * 16, now_utc()),
                )
                artifact_ids.append(int(cursor.lastrowid))
            comments = Path(self.temp.name) / "comments.json"
            comments.write_text("{}", encoding="utf-8")
            version = connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    content_id,captured_at,iso_week,source,local_path,sha256,
                    comment_count,status,created_at
                ) VALUES (?,?,?,'test',?,?,1,'available',?)
                """,
                (self.content_id, now_utc(), "2026-W31", str(comments), "c" * 64, now_utc()),
            )
            connection.execute(
                """
                INSERT INTO comments(evidence_version_id,platform_comment_id,body,like_count)
                VALUES (?,'comment-1','这是一条评论摘要',3)
                """,
                (version.lastrowid,),
            )
            connection.execute(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    output_artifact_id,attempt_count,created_at,updated_at
                ) VALUES (?,?,'ocr','ocr-test','succeeded',?,1,?,?)
                """,
                (self.content_id, "m" * 64, artifact_ids[-1], now_utc(), now_utc()),
            )
            connection.commit()
        evidence = self.client.get(f"/api/v8/contents/{self.content_id}/evidence")
        self.assertEqual(evidence.status_code, 200)
        value = evidence.json()
        self.assertEqual(value["base_evaluation_id"], self.evaluation_id)
        self.assertEqual(value["asr"]["text"], "完整的本地语音证据")
        self.assertEqual(value["ocr"]["text"], "关键帧文字证据\n车辆保养流程")
        self.assertEqual(value["comments"]["stored_count"], 1)
        self.assertEqual(len(value["media"]), 1)
        media = self.client.get(value["media"][0]["url"])
        self.assertEqual(media.status_code, 200)
        self.assertEqual(media.content, b"local-image-evidence")
        processing = self.client.post(
            "/api/v8/media-processing/search",
            json={"content_id": self.content_id, "status": "succeeded"},
        )
        self.assertEqual(processing.status_code, 200)
        self.assertEqual(processing.json()["total"], 1)
        existing_evidence = self.client.post(
            f"/api/v8/contents/{self.content_id}/media/retry",
            json={"allow_paid_refresh": False},
        )
        self.assertEqual(existing_evidence.status_code, 200)
        self.assertEqual(existing_evidence.json()["status"], "evidence_ready")
        self.assertEqual(existing_evidence.json()["provider_cost"], 0.0)

    def test_task_cancel_and_resume_routes_preserve_revision_history(self) -> None:
        resolved = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": self.evaluation_id,
                "decision": "insufficient_evidence",
                "reason": "任务控制测试先清零首发闸门",
                "reviewer": "测试复核员",
                "evidence_type": "review_note",
                "evidence_text": "确认当前测试内容没有足够媒体证据",
            },
        )
        self.assertEqual(resolved.status_code, 200)
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        cancelled = self.client.post(f"/api/v8/tasks/{task['id']}/cancel")
        self.assertEqual(cancelled.status_code, 200)
        self.assertEqual(cancelled.json()["task_status"], "cancelled")
        resumed = self.client.post(f"/api/v8/tasks/{task['id']}/resume")
        self.assertEqual(resumed.status_code, 200)
        self.assertIn(resumed.json()["task_status"], {"succeeded", "partial"})
        self.assertEqual(len(resumed.json()["revisions"]), 1)

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
        reviewed = self.client.post(
            f"/api/v8/reviews/{self.queue_id}/resolve",
            json={
                "base_evaluation_id": self.evaluation_id,
                "decision": "insufficient_evidence",
                "reason": "报告测试先清零人工复核闸门",
                "reviewer": "测试复核员",
                "evidence_type": "review_note",
                "evidence_text": "测试内容没有本地媒体，人工确认当前证据不足",
            },
        )
        self.assertEqual(reviewed.status_code, 200)
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
