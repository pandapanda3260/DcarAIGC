from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import v8.reports as reports_module
from v8.contracts import CURRENT_REPORT_VERSION
from v8.duplicates import FINGERPRINT_VERSION
from v8.evaluation import build_evidence_envelope, evaluate_release_content
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.reports import (
    ReportTaskError,
    assert_report_runtime_ready,
    create_and_run_task,
    create_task,
    get_task,
    request_task_cancel,
    retry_task,
    resume_task,
    run_task,
)
from v8.storage import (
    LEGACY_V7_RELEASE_ID,
    PROJECT_ROOT,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
)
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


class V8ReportTaskTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.root = Path(self.temp.name)
        self.db = self.root / "reports.sqlite3"
        self.reports_root = self.root / "reports"
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at, published_at
                ) VALUES ('taxonomy', 'selling-points-v5.0', 'published', 'test', ?, ?)
                """,
                (captured_at, captured_at),
            )
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id, code, tier, label, positive_evidence_json
                    ) VALUES ('taxonomy', ?, ?, ?, ?)
                    """,
                    (
                        code,
                        "core" if code == "C1" else "other",
                        "汽车服务" if code == "C1" else f"卖点 {code}",
                        '["保养"]' if code == "C1" else "[]",
                    ),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id, scene)
                        VALUES (?, ?)
                        """,
                        (point.lastrowid, scene),
                    )
            content = connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url,
                    published_at, title, body, content_type, imported_at, created_at, updated_at
                ) VALUES (
                    'A2BC3D', 'douyin', '1', 'https://www.douyin.com/video/1',
                    '2026-07-01T04:00:00Z', '汽车保养', '保养知识', 'video', ?, ?, ?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id, platform, platform_content_id, canonical_url,
                    published_at, title, content_type, imported_at, created_at, updated_at
                ) VALUES (
                    'E4FG5H', 'xiaohongshu', '2', 'https://www.xiaohongshu.com/explore/2',
                    NULL, '', 'normal', ?, ?, ?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            content_id = int(content.lastrowid)
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    content_id, stage, window_key, provider, adapter_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (?, 'detail', 'lifetime', 'migration', 'v8', 'succeeded', 1, ?, ?)
                """,
                (content_id, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_metric_snapshots(
                    content_id, captured_at, window_key, view_count, status, source
                ) VALUES (?, '2026-07-02T00:00:00Z', 'historical', 1234,
                          'available', 'migrated_historical')
                """,
                (content_id,),
            )
            legacy_release = ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            envelope_id, evidence_sha256, _ = build_evidence_envelope(connection, 1)
            connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id,evidence_envelope_id,release_id,rule_version,
                    taxonomy_version,matcher_rule_sha256,evidence_sha256,
                    evaluation_source,evaluation_status,evidence_level,
                    selling_point_score,selling_point_included,content_direction,
                    pending_review,payload_json,evaluated_at
                ) VALUES (1,?,?,?,?,?,?,'automatic','insufficient_evidence','V1',
                          0,0,'unknown',0,?,?)
                """,
                (
                    envelope_id,
                    legacy_release["id"],
                    legacy_release["rule_version"],
                    legacy_release["taxonomy_version"],
                    legacy_release["matcher_rule_sha256"],
                    evidence_sha256,
                    json.dumps(
                        {
                            "evaluation_status": "insufficient_evidence",
                            "evidence_level": "V1",
                            "evidence_summary": "legacy report fixture",
                            "primary_selling_point_id": "",
                            "selling_point_score": 0,
                            "selling_point_included": False,
                            "pending_review": False,
                            "content_direction": "unknown",
                            "content_automotive_score": None,
                            "audience_automotive_score": None,
                            "action_intent_score": None,
                            "valid_unique_commenters": 0,
                            "acquisition_potential": None,
                            "matches": [],
                            "evaluation_source": "automatic",
                            "release_id": legacy_release["id"],
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    captured_at,
                ),
            )
            connection.commit()

        matcher = backfill_v5_1_matcher_rules(db_path=self.db)
        self.release_id = "evaluation-v8__selling-points-v5.1"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES (?,'evaluation-v8','selling-points-v5.1',?,'backfilling',?,?)
                """,
                (
                    self.release_id,
                    matcher["matcher_rule_sha256"],
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()
        evaluate_release_content(1, release_id=self.release_id, db_path=self.db)
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='retired',retired_at=?,updated_at=?
                WHERE status='active' AND id<>?
                """,
                (captured_at, captured_at, self.release_id),
            )
            connection.execute(
                """
                UPDATE taxonomy_versions SET status='retired'
                WHERE status='published' AND version<>'selling-points-v5.1'
                """
            )
            connection.execute(
                """
                UPDATE taxonomy_versions
                SET status='published',published_at=?
                WHERE version='selling-points-v5.1'
                """,
                (captured_at,),
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET status='active',activated_at=?,updated_at=? WHERE id=?
                """,
                (captured_at, captured_at, self.release_id),
            )
            connection.commit()

    def test_conclusion_cell_uses_cause_and_never_publishes_blocked_value(
        self,
    ) -> None:
        blocked = {
            "kind": "ratio",
            "percentage": 88.0,
            "status": "below_threshold",
            "reason": "用户身份覆盖率 64.0%，低于 95% 门槛",
        }
        self.assertEqual(reports_module._conclusion_cell(blocked), "身份数据待补齐")
        self.assertNotIn("88", reports_module._conclusion_cell(blocked))
        self.assertEqual(
            reports_module._conclusion_cell(
                {
                    "kind": "ratio",
                    "percentage": None,
                    "status": "below_threshold",
                    "reason": "去重有效用户 29 人，低于 30 人门槛",
                }
            ),
            "互动用户少于30人",
        )
        self.assertEqual(
            reports_module._conclusion_cell(
                {
                    "kind": "ratio",
                    "percentage": None,
                    "status": "below_threshold",
                    "reason": "",
                }
            ),
            "暂不发布",
        )
        self.assertEqual(
            reports_module._conclusion_cell(
                {
                    "kind": "ratio",
                    "percentage": 12.5,
                    "status": "sample_only",
                    "reason": "分类器未经金标核对，数值仅供参考",
                }
            ),
            "12.5%（仅样本）",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _insert_report_content(
        self,
        connection,
        *,
        suffix: str,
        manual_direction: str | None = None,
    ) -> int:
        captured_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO content_items(
                link_id,platform,platform_content_id,canonical_url,published_at,
                title,body,content_type,manual_content_direction,
                imported_at,created_at,updated_at
            ) VALUES (?, 'kuaishou', ?, ?, '2026-07-03T04:00:00Z', ?, ?,
                      'video', ?, ?, ?, ?)
            """,
            (
                (f"M{suffix.upper()}00000")[:6],
                f"matrix-{suffix}",
                f"https://www.kuaishou.com/short-video/matrix-{suffix}",
                f"矩阵内容 {suffix}",
                f"矩阵内容 {suffix} 的完整汽车正文证据",
                manual_direction,
                captured_at,
                captured_at,
                captured_at,
            ),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def _insert_unsafe_legacy_report(
        self, *, creation_source: str = "automatic"
    ) -> str:
        task_id = f"UNSAFE-{creation_source.upper()}"
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,message,created_at,started_at,completed_at,
                    updated_at
                ) VALUES (?,'daily','unsafe legacy','2026-06-01','2026-06-01',?,
                          'partial',100,'legacy','2026-08-04T07:45:50Z',
                          '2026-08-04T07:45:50Z','2026-08-04T07:45:50Z',
                          '2026-08-04T07:45:50Z')
                """,
                (task_id, creation_source),
            )
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at
                ) VALUES (?,1,?,?,
                          'evaluation-v7','selling-points-v5.0',?,?,'2026-08-04T07:45:50Z')
                """,
                (
                    task_id,
                    LEGACY_V7_RELEASE_ID,
                    CURRENT_REPORT_VERSION,
                    f"reports/runs/v8/{task_id}/revision_001/report.json",
                    "7" * 64,
                ),
            )
            connection.commit()
        return task_id

    def _insert_report_evaluation(
        self,
        connection,
        *,
        content_id: int,
        evidence_level: str,
        pending_review: int,
        included: int,
        direction: str,
        code: str,
        release_id: str | None = None,
        evaluated_at: str | None = None,
    ) -> int:
        selected_release = release_id or self.release_id
        release = connection.execute(
            "SELECT * FROM evaluation_releases WHERE id=?", (selected_release,)
        ).fetchone()
        assert release is not None
        cursor = connection.execute(
            """
            INSERT INTO evaluation_versions(
                content_id,release_id,rule_version,taxonomy_version,
                matcher_rule_sha256,evidence_sha256,evaluation_source,
                evaluation_status,evidence_level,primary_selling_point_code,
                selling_point_score,selling_point_included,content_direction,
                content_automotive_score,pending_review,payload_json,evaluated_at
            ) VALUES (?,?,?,?,?,?,'automatic','evaluated',?,?,90,?,?,80,0+?,'{}',?)
            """,
            (
                content_id,
                selected_release,
                release["rule_version"],
                release["taxonomy_version"],
                release["matcher_rule_sha256"],
                f"{content_id:064x}"[-64:],
                evidence_level,
                code,
                included,
                direction,
                pending_review,
                evaluated_at or now_utc(),
            ),
        )
        assert cursor.lastrowid is not None
        return int(cursor.lastrowid)

    def test_report_task_freezes_scope_and_appends_immutable_revisions(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["rule_version"], "evaluation-v8")
        self.assertEqual(report["taxonomy_version"], "selling-points-v5.1")
        self.assertEqual(report["task"]["task_status"], "partial")
        self.assertEqual(report["summary_metrics"]["publication_count"]["value"], 1)
        self.assertEqual(report["summary_metrics"]["view_count"]["status"], "stale")
        self.assertEqual(
            report["summary_metrics"]["comment_count"]["status"], "missing"
        )
        self.assertEqual(
            report["summary_metrics"]["estimated_leads"]["status"], "not_calculable"
        )
        self.assertEqual(report["summary_metrics"]["estimated_leads"]["unit"], "lead")
        self.assertEqual(
            report["summary_metrics"]["duplicate_rate"]["status"], "not_calculable"
        )
        self.assertEqual(
            report["data_quality"]["duplicate_fingerprint_coverage"], 0.0
        )
        self.assertFalse(report["data_quality"]["duplicate_calibration_ready"])
        self.assertIn(
            "尚未完成定标",
            report["summary_metrics"]["duplicate_rate"]["reason"],
        )
        state = get_task(task["id"], db_path=self.db)
        self.assertNotIn("覆盖率不足", state["message"])
        self.assertIn("未达发布门槛", state["message"])
        self.assertIn("重复指纹定标未通过", state["message"])
        self.assertEqual(report["scope"]["period_end"], "2026-07-02T00:00:00+08:00")
        self.assertEqual(
            state["content_counts"], {"excluded_missing_boundary": 1, "included": 1}
        )
        self.assertEqual(len(state["revisions"]), 1)
        self.assertEqual(state["revisions"][0]["release_id"], self.release_id)
        self.assertEqual(state["revisions"][0]["rule_version"], "evaluation-v8")
        self.assertEqual(
            state["revisions"][0]["taxonomy_version"], "selling-points-v5.1"
        )
        first_path = PROJECT_ROOT / state["revisions"][0]["report_json_path"]
        first_bytes = first_path.read_bytes()
        first_value = json.loads(first_bytes)
        self.assertEqual(first_value["metadata"]["revision"], 1)
        self.assertTrue(
            any(
                item["file_kind"] == "summary-svg"
                for item in state["revisions"][0]["files"]
            )
        )

        second = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(second["metadata"]["revision"], 2)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(len(state["revisions"]), 2)
        self.assertEqual(state["current_valid_revision"]["revision"], 2)
        self.assertIsNone(state["stale_display_revision"])
        self.assertEqual(state["display_effective_revision"]["revision"], 2)
        self.assertEqual(state["historical_revision_count"], 1)
        self.assertEqual(
            [row["revision_state"] for row in state["revisions"]],
            ["current", "historical"],
        )
        self.assertEqual(first_path.read_bytes(), first_bytes)

    def test_report_keeps_actual_fingerprint_coverage_when_uncalibrated(self) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO duplicate_fingerprints(
                    content_id,fingerprint_version,source_sha256,payload_json,created_at
                ) VALUES (1,?,?,?,?)
                """,
                (FINGERPRINT_VERSION, "f" * 64, "{}", now_utc()),
            )
            connection.commit()

        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        duplicate = report["summary_metrics"]["duplicate_rate"]
        self.assertEqual(report["data_quality"]["duplicate_fingerprint_coverage"], 100.0)
        self.assertFalse(report["data_quality"]["duplicate_calibration_ready"])
        self.assertEqual(report["task"]["task_status"], "partial")
        self.assertEqual(duplicate["status"], "not_calculable")
        self.assertEqual(duplicate["coverage_percentage"], 100.0)
        self.assertIsNone(duplicate["percentage"])
        self.assertIn("尚未完成定标", duplicate["reason"])

    def test_current_report_publishes_channel_conclusions_without_fabrication(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(report["evidence_version"], "evidence-v2")
        channels = report["channels"]
        self.assertEqual(sorted(channels.keys()), ["douyin", "xiaohongshu"])
        douyin = channels["douyin"]
        self.assertEqual(douyin["publication_count"], 1)
        self.assertEqual(
            sorted(douyin["scenes"].keys()), ["media", "new_car", "used_car"]
        )
        self.assertEqual(
            list(douyin["summary"]["metrics"].keys()),
            [
                "selling_point_count_share",
                "core_selling_point_count_share",
                "selling_point_exposure_share",
                "core_selling_point_exposure_share",
                "content_verticality",
                "automotive_user_rate",
                "acquisition_potential",
            ],
        )
        rate = douyin["summary"]["metrics"]["automotive_user_rate"]
        # No interaction users exist in this fixture and the classifier is
        # uncalibrated: the user-level rate must never fabricate a percentage.
        self.assertEqual(rate["status"], "missing")
        self.assertIsNone(rate["percentage"])
        self.assertIsNone(rate["numerator"])
        self.assertEqual(rate["denominator"], 0)
        self.assertEqual(rate["eligible_count"], 0)
        quality = douyin["summary"]["audience_quality"]
        self.assertEqual(quality["user_key_version"], "platform-user-hmac-v2")
        self.assertEqual(
            quality["audience_definition_version"], "audience-definition-v1"
        )
        self.assertTrue(quality["warm_up"])
        empty_channel = channels["xiaohongshu"]
        self.assertEqual(empty_channel["publication_count"], 0)
        self.assertEqual(
            empty_channel["summary"]["metrics"]["automotive_user_rate"]["status"],
            "not_applicable",
        )

        state = get_task(task["id"], db_path=self.db)
        kinds = {item["file_kind"] for item in state["revisions"][0]["files"]}
        self.assertIn("channel-csv", kinds)
        revision_dir = (
            PROJECT_ROOT / state["revisions"][0]["report_json_path"]
        ).parent
        csv_text = (revision_dir / "channel_conclusions.csv").read_text(
            encoding="utf-8-sig"
        )
        csv_lines = csv_text.splitlines()
        self.assertTrue(csv_lines[0].startswith("platform,platform_label,scope"))
        self.assertEqual(len(csv_lines), 1 + 2 * 4 * 7)
        self.assertIn("automotive_user_rate", csv_text)
        markdown = (revision_dir / "report.md").read_text(encoding="utf-8")
        self.assertIn("## 渠道结论", markdown)
        self.assertIn("互动用户汽车兴趣占比", markdown)
        self.assertIn("评论未采集", markdown)
        self.assertIn("曝光归类待补齐", markdown)
        self.assertNotIn("覆盖不足", markdown)
        self.assertNotIn("有效样本不足", markdown)
        self.assertNotIn("暂不可计算%", markdown)
        svg = (revision_dir / "core_summary.svg").read_text(encoding="utf-8")
        self.assertNotIn("互动用户", svg)
        content_csv = (revision_dir / "content_details.csv").read_text(
            encoding="utf-8-sig"
        )
        self.assertNotIn("automotive_user_rate", content_csv)

    def test_revision_read_model_marks_legal_retired_report_stale(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with connect(self.db) as connection:
            retired = connection.execute(
                """
                SELECT * FROM evaluation_releases
                WHERE status='retired' AND rule_version='evaluation-v6'
                """
            ).fetchone()
            if retired is None:
                captured_at = now_utc()
                connection.execute(
                    """
                    INSERT INTO evaluation_releases(
                        id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                        created_at,updated_at,retired_at
                    ) VALUES ('evaluation-v6-report-test','evaluation-v6',
                              'selling-points-v5.0',?,'retired',?,?,?)
                    """,
                    ("6" * 64, captured_at, captured_at, captured_at),
                )
                retired = connection.execute(
                    """
                    SELECT * FROM evaluation_releases
                    WHERE id='evaluation-v6-report-test'
                    """
                ).fetchone()
            assert retired is not None
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at
                ) VALUES (?,1,?,'dcar-content-operations-report-v8.2',?,?,?,?,'2026-08-03T00:00:00Z')
                """,
                (
                    task["id"],
                    retired["id"],
                    retired["rule_version"],
                    retired["taxonomy_version"],
                    "reports/runs/v8/stale/revision_001/report.json",
                    "1" * 64,
                ),
            )
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at,
                    invalidated_at,invalidation_reason
                ) VALUES (?,2,?,'dcar-content-operations-report-v8.3',?,?,?,?,'2026-08-04T00:00:00Z',
                          '2026-08-04T01:00:00Z','invalidated test revision')
                """,
                (
                    task["id"],
                    retired["id"],
                    retired["rule_version"],
                    retired["taxonomy_version"],
                    "reports/runs/v8/stale/revision_002/report.json",
                    "2" * 64,
                ),
            )
            connection.commit()

        stale = get_task(task["id"], db_path=self.db)
        self.assertIsNone(stale["current_valid_revision"])
        self.assertEqual(stale["stale_display_revision"]["revision"], 1)
        self.assertEqual(stale["display_effective_revision"]["revision"], 1)
        self.assertEqual(stale["historical_revision_count"], 2)
        self.assertEqual(
            [row["revision_state"] for row in stale["revisions"]],
            ["historical", "stale"],
        )

        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        current = get_task(task["id"], db_path=self.db)
        self.assertEqual(current["current_valid_revision"]["revision"], 3)
        self.assertIsNone(current["stale_display_revision"])
        self.assertEqual(current["display_effective_revision"]["revision"], 3)
        self.assertEqual(current["historical_revision_count"], 2)
        self.assertEqual(
            [row["revision_state"] for row in current["revisions"]],
            ["current", "historical", "historical"],
        )

    def test_formal_report_uses_only_v2_v3_non_pending_evaluations(self) -> None:
        first_release = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        run_task(
            first_release["id"], db_path=self.db, reports_root=self.reports_root
        )
        with connect(self.db) as connection:
            pending = self._insert_report_content(
                connection, suffix="pending", manual_direction="media"
            )
            low_evidence = self._insert_report_content(
                connection, suffix="v1", manual_direction="used_car"
            )
            eligible_only = self._insert_report_content(connection, suffix="eligible")
            included = self._insert_report_content(connection, suffix="included")
            self._insert_report_evaluation(
                connection,
                content_id=pending,
                evidence_level="V3",
                pending_review=1,
                included=1,
                direction="new_car",
                code="X1",
            )
            self._insert_report_evaluation(
                connection,
                content_id=low_evidence,
                evidence_level="V1",
                pending_review=0,
                included=1,
                direction="new_car",
                code="X1",
            )
            self._insert_report_evaluation(
                connection,
                content_id=eligible_only,
                evidence_level="V3",
                pending_review=0,
                included=0,
                direction="media",
                code="M1",
            )
            self._insert_report_evaluation(
                connection,
                content_id=included,
                evidence_level="V3",
                pending_review=0,
                included=1,
                direction="new_car",
                code="X2",
            )
            retired_release = connection.execute(
                """
                SELECT id FROM evaluation_releases
                WHERE status='retired' AND rule_version='evaluation-v7'
                """
            ).fetchone()[0]
            self._insert_report_evaluation(
                connection,
                content_id=included,
                evidence_level="V3",
                pending_review=0,
                included=1,
                direction="used_car",
                code="C1",
                release_id=str(retired_release),
                evaluated_at="2099-01-01T00:00:00Z",
            )
            connection.commit()

        task = create_task(
            task_type="custom",
            period_start="2026-07-03",
            period_end="2026-07-03",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["data_quality"]["evaluation_coverage"], 50.0)
        self.assertEqual(
            report["summary_metrics"]["verticality_rate"],
            {
                "kind": "ratio",
                "numerator": 2,
                "denominator": 4,
                "percentage": None,
                "unit": "percent",
                "status": "below_threshold",
                "eligible_count": 2,
                "coverage_percentage": 50.0,
                "reason": "正式评估覆盖率为 50.00%，低于 95% 发布阈值",
            },
        )
        selling_metric = report["summary_metrics"]["selling_point_coverage_rate"]
        self.assertEqual(selling_metric["numerator"], 1)
        self.assertEqual(selling_metric["denominator"], 4)
        self.assertEqual(selling_metric["eligible_count"], 2)
        self.assertEqual(
            report["selling_point_dimensions"],
            [{"code": "X2", "count": 1, "percentage": 25.0}],
        )
        direction_counts = {
            row["key"]: row["count"] for row in report["content_direction_dimensions"]
        }
        self.assertEqual(direction_counts, {"media": 2, "new_car": 1, "used_car": 1})
        details = {row["content_id"]: row for row in report["content_details"]}
        for content_id, expected_direction in (
            (pending, "media"),
            (low_evidence, "used_car"),
        ):
            self.assertFalse(details[content_id]["evaluation_current"])
            self.assertIsNone(details[content_id]["evidence_level"])
            self.assertIsNone(details[content_id]["primary_selling_point_code"])
            self.assertEqual(
                details[content_id]["content_direction"], expected_direction
            )
        self.assertTrue(details[eligible_only]["evaluation_current"])
        self.assertEqual(details[eligible_only]["primary_selling_point_code"], "M1")
        self.assertTrue(details[included]["evaluation_current"])
        self.assertEqual(details[included]["primary_selling_point_code"], "X2")
        self.assertEqual(details[included]["content_direction"], "new_car")

    def test_report_fails_closed_when_active_release_is_not_current(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with connect(self.db) as connection:
            captured_at = now_utc()
            connection.execute(
                """
                UPDATE evaluation_releases SET status='retired',retired_at=?,updated_at=?
                WHERE id=?
                """,
                (captured_at, captured_at, self.release_id),
            )
            connection.execute(
                """
                UPDATE evaluation_releases SET status='active',activated_at=?,updated_at=?
                WHERE rule_version='evaluation-v7'
                """,
                (captured_at, captured_at),
            )
            connection.commit()
        with self.assertRaisesRegex(ReportTaskError, "current report contract"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_revisions").fetchone()[
                    0
                ],
                0,
            )
        self.assertFalse((self.reports_root / task["id"]).exists())

    def test_report_fails_closed_without_a_unique_active_release(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evaluation_releases SET status='retired' WHERE status='active'"
            )
            connection.commit()
        with self.assertRaisesRegex(ReportTaskError, "no active"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            connection.execute("DROP INDEX uq_evaluation_releases_one_active")
            connection.execute(
                """
                UPDATE evaluation_releases SET status='active'
                WHERE rule_version IN ('evaluation-v7','evaluation-v8')
                """
            )
            connection.commit()
        with self.assertRaisesRegex(ReportTaskError, "multiple active"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_revisions").fetchone()[
                    0
                ],
                0,
            )
        self.assertFalse((self.reports_root / task["id"]).exists())

    def test_release_switch_before_registration_removes_unregistered_outputs(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        original_validate = reports_module.validate_report

        def validate_then_switch(report):
            original_validate(report)
            with connect(self.db) as connection:
                captured_at = now_utc()
                connection.execute(
                    """
                    UPDATE evaluation_releases
                    SET status='retired',retired_at=?,updated_at=? WHERE id=?
                    """,
                    (captured_at, captured_at, self.release_id),
                )
                connection.execute(
                    """
                    UPDATE taxonomy_versions SET status='retired'
                    WHERE version='selling-points-v5.1'
                    """
                )
                connection.execute(
                    """
                    UPDATE taxonomy_versions SET status='published',published_at=?
                    WHERE version='selling-points-v5.0'
                    """,
                    (captured_at,),
                )
                connection.execute(
                    """
                    UPDATE evaluation_releases
                    SET status='active',activated_at=?,updated_at=?
                    WHERE rule_version='evaluation-v7'
                    """,
                    (captured_at, captured_at),
                )
                connection.commit()

        with (
            patch.object(
                reports_module, "validate_report", side_effect=validate_then_switch
            ),
            self.assertRaisesRegex(ReportTaskError, "current report contract"),
        ):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_revisions").fetchone()[
                    0
                ],
                0,
            )
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM report_files").fetchone()[0],
                0,
            )
        task_root = self.reports_root / task["id"]
        self.assertEqual(list(task_root.iterdir()) if task_root.exists() else [], [])

    def test_gray_created_during_render_is_rechecked_before_registration(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        original_validate = reports_module.validate_report

        def validate_then_create_gray(report):
            original_validate(report)
            with connect(self.db) as connection:
                connection.execute(
                    """
                    UPDATE evaluation_versions
                    SET pending_review=1,evidence_level='V3'
                    WHERE content_id=1 AND release_id=?
                    """,
                    (self.release_id,),
                )
                connection.commit()

        with (
            patch.object(
                reports_module,
                "validate_report",
                side_effect=validate_then_create_gray,
            ),
            self.assertRaisesRegex(ReportTaskError, "1 条灰区内容"),
        ):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(state["events"][-1]["event_type"], "review_gate_blocked")
        task_root = self.reports_root / task["id"]
        self.assertEqual(list(task_root.iterdir()) if task_root.exists() else [], [])

    def test_discovery_coverage_requires_platform_user_posts_slots(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            douyin = connection.execute(
                """
                INSERT INTO accounts(
                    phone, phone_normalized, created_at, updated_at
                ) VALUES ('13800138001', '13800138001', ?, ?)
                """,
                (captured_at, captured_at),
            )
            xiaohongshu = connection.execute(
                """
                INSERT INTO accounts(
                    phone, phone_normalized, created_at, updated_at
                ) VALUES ('13800138002', '13800138002', ?, ?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO account_platform_identities(
                    account_id, platform, uid, created_at, updated_at
                ) VALUES (?, 'douyin', 'douyin-user', ?, ?)
                """,
                (douyin.lastrowid, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO account_platform_identities(
                    account_id, platform, uid, created_at, updated_at
                ) VALUES (?, 'xiaohongshu', 'xhs-user', ?, ?)
                """,
                (xiaohongshu.lastrowid, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    account_id, stage, window_key, provider, adapter_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (?, 'discovery', 'profile:lifetime', 'TikHub',
                          'tikhub-user-profile-v8.1', 'succeeded', 1, ?, ?)
                """,
                (douyin.lastrowid, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    account_id, stage, window_key, provider, adapter_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (?, 'discovery', '2026-07-03', 'TikHub',
                          'tikhub-xhs-app-v2-user-posts-v8.1',
                          'succeeded', 1, ?, ?)
                """,
                (xiaohongshu.lastrowid, captured_at, captured_at),
            )
            connection.commit()

        task = create_task(
            task_type="custom",
            period_start="2026-07-03",
            period_end="2026-07-03",
            creation_source="manual",
            db_path=self.db,
        )
        partial = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(partial["data_quality"]["discovery_coverage"], 50.0)
        self.assertEqual(partial["task"]["task_status"], "partial")

        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO fetch_slots(
                    account_id, stage, window_key, provider, adapter_version,
                    status, attempt_count, created_at, updated_at
                ) VALUES (?, 'discovery', '2026-07-03', 'TikHub',
                          'tikhub-user-posts-v8.1', 'succeeded', 1, ?, ?)
                """,
                (douyin.lastrowid, captured_at, captured_at),
            )
            connection.commit()

        complete = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(complete["data_quality"]["discovery_coverage"], 100.0)
        self.assertEqual(complete["task"]["task_status"], "succeeded")

    def test_relative_reports_root_resolves_under_project_without_leaking_temp_dirs(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        relative_root = self.reports_root.relative_to(PROJECT_ROOT)

        report = run_task(
            task["id"],
            db_path=self.db,
            reports_root=relative_root,
        )

        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(report["metadata"]["revision"], 1)
        self.assertIn(state["task_status"], {"succeeded", "partial"})
        report_path = PROJECT_ROOT / state["revisions"][0]["report_json_path"]
        self.assertTrue(report_path.is_file())
        revision_parent = self.reports_root / str(task["id"])
        self.assertEqual(
            list(revision_parent.glob(".revision_001-*")),
            [],
        )

    def test_artifact_initialization_failure_marks_task_failed_and_cleans_temp_dir(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        revision_parent = self.reports_root / str(task["id"])

        with (
            patch(
                "v8.reports._relative", side_effect=ValueError("relative path failed")
            ),
            self.assertRaisesRegex(ValueError, "relative path failed"),
        ):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)

        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["message"], "relative path failed")
        self.assertEqual(state["events"][-1]["event_type"], "failed")
        self.assertEqual(list(revision_parent.glob(".revision_001-*")), [])
        self.assertFalse((revision_parent / "revision_001").exists())

    def test_revision_registration_failure_removes_only_current_unregistered_output(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        revision_parent = self.reports_root / str(task["id"])

        with (
            patch(
                "v8.reports.uuid.uuid4",
                side_effect=RuntimeError("report file registration failed"),
            ),
            self.assertRaisesRegex(RuntimeError, "registration failed"),
        ):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)

        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(list(revision_parent.glob(".revision_001-*")), [])
        self.assertFalse((revision_parent / "revision_001").exists())

    def test_reports_root_outside_project_is_rejected_without_creating_artifacts(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        outside = PROJECT_ROOT.parent / f"outside-reports-{task['id']}"
        self.assertFalse(outside.exists())

        with self.assertRaisesRegex(ReportTaskError, "reports_root"):
            run_task(task["id"], db_path=self.db, reports_root=outside)

        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["events"][-1]["event_type"], "failed")
        self.assertFalse(outside.exists())

    def test_task_id_cannot_escape_resolved_reports_root(self) -> None:
        task_id = "../escaped-report-task"
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,message,created_at,updated_at
                ) VALUES (?,'custom','unsafe id','2026-07-01','2026-07-01',
                          'manual','queued',0,'',?,?)
                """,
                (task_id, captured_at, captured_at),
            )
            connection.commit()
        escaped = self.reports_root.parent / "escaped-report-task"
        self.assertFalse(escaped.exists())

        with self.assertRaisesRegex(ReportTaskError, "reports_root"):
            run_task(task_id, db_path=self.db, reports_root=self.reports_root)

        state = get_task(task_id, db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertFalse(escaped.exists())

    def test_task_calendar_rules_are_strict(self) -> None:
        with self.assertRaisesRegex(ReportTaskError, "exactly one"):
            create_task(
                task_type="daily",
                period_start="2026-07-01",
                period_end="2026-07-02",
                creation_source="automatic",
                db_path=self.db,
            )
        with self.assertRaisesRegex(ReportTaskError, "Monday through Sunday"):
            create_task(
                task_type="weekly",
                period_start="2026-07-01",
                period_end="2026-07-07",
                creation_source="automatic",
                db_path=self.db,
            )

    def test_succeeded_task_can_queue_a_new_immutable_revision(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        first = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        queued = retry_task(task["id"], db_path=self.db)
        self.assertEqual(queued["task_status"], "queued")
        second = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(
            (first["metadata"]["revision"], second["metadata"]["revision"]), (1, 2)
        )
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(len(state["revisions"]), 2)
        self.assertIn(
            "retry_requested", [event["event_type"] for event in state["events"]]
        )

    def test_cancelled_task_can_resume_and_create_a_new_revision(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        cancelled = request_task_cancel(task["id"], db_path=self.db)
        self.assertEqual(cancelled["task_status"], "cancelled")
        with self.assertRaisesRegex(ReportTaskError, "not runnable"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        resumed = resume_task(task["id"], db_path=self.db)
        self.assertEqual(resumed["task_status"], "queued")
        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        completed = get_task(task["id"], db_path=self.db)
        self.assertIn(completed["task_status"], {"succeeded", "partial"})
        self.assertEqual(len(completed["revisions"]), 1)
        self.assertEqual(
            [event["event_type"] for event in completed["events"]][1:3],
            ["cancelled", "resumed"],
        )

    def test_pending_gray_review_blocks_first_report_without_creating_revision(
        self,
    ) -> None:
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                """
                SELECT id FROM evaluation_versions
                WHERE content_id=1 AND release_id=? ORDER BY id DESC
                """,
                (self.release_id,),
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with self.assertRaisesRegex(ReportTaskError, "1 条灰区内容"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(state["events"][-1]["event_type"], "review_gate_blocked")

    def test_pending_manual_conclusion_conflict_blocks_first_report(self) -> None:
        with connect(self.db) as connection:
            evaluation_id = self._insert_report_evaluation(
                connection,
                content_id=1,
                evidence_level="V3",
                pending_review=0,
                included=1,
                direction="media",
                code="M1",
            )
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,priority,status,created_at,updated_at
                ) VALUES (1,?,'manual_conclusion_conflict',90,'manual_required',?,?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with self.assertRaisesRegex(ReportTaskError, "人工结论冲突"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(state["events"][-1]["event_type"], "review_gate_blocked")

    def test_manual_conclusion_conflict_is_excluded_after_first_report(self) -> None:
        with connect(self.db) as connection:
            evaluation_id = self._insert_report_evaluation(
                connection,
                content_id=1,
                evidence_level="V3",
                pending_review=0,
                included=1,
                direction="media",
                code="M1",
            )
            connection.commit()
        first = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        first_report = run_task(
            first["id"], db_path=self.db, reports_root=self.reports_root
        )
        self.assertEqual(first_report["data_quality"]["evaluation_coverage"], 100.0)

        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,priority,status,created_at,updated_at
                ) VALUES (1,?,'manual_conclusion_conflict',90,'manual_required',?,?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        second = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        second_report = run_task(
            second["id"], db_path=self.db, reports_root=self.reports_root
        )
        self.assertEqual(second_report["data_quality"]["evaluation_coverage"], 0.0)
        verticality = second_report["summary_metrics"]["verticality_rate"]
        self.assertEqual(verticality["denominator"], 1)
        self.assertEqual(verticality["eligible_count"], 0)
        self.assertEqual(verticality["numerator"], 0)

    def test_pending_gray_evaluation_blocks_first_report_without_queue(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions
                SET pending_review=1,evidence_level='V3'
                WHERE content_id=1 AND release_id=?
                """,
                (self.release_id,),
            )
            connection.commit()
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with self.assertRaisesRegex(ReportTaskError, "1 条灰区内容"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        state = get_task(task["id"], db_path=self.db)
        self.assertEqual(state["task_status"], "failed")
        self.assertEqual(state["revisions"], [])
        self.assertEqual(state["events"][-1]["event_type"], "review_gate_blocked")

    def test_pending_gray_review_is_reported_but_does_not_block_after_first_release(
        self,
    ) -> None:
        first = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        run_task(first["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                """
                SELECT id FROM evaluation_versions
                WHERE content_id=1 AND release_id=? ORDER BY id DESC
                """,
                (self.release_id,),
            ).fetchone()[0]
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
        later = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-02",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(later["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["metadata"]["revision"], 1)
        self.assertEqual(report["review_summary"][0]["status"], "pending")

    def test_unsafe_automatic_current_contract_report_blocks_without_new_task(
        self,
    ) -> None:
        self._insert_unsafe_legacy_report()
        with connect(self.db) as connection:
            with self.assertRaisesRegex(ReportTaskError, "outside the active release"):
                assert_report_runtime_ready(connection)
            counts_before = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in ("report_tasks", "report_revisions", "task_events")
            }
        with self.assertRaisesRegex(ReportTaskError, "outside the active release"):
            create_and_run_task(
                task_type="custom",
                period_start="2026-07-01",
                period_end="2026-07-01",
                creation_source="manual",
                db_path=self.db,
                reports_root=self.reports_root,
            )
        with connect(self.db) as connection:
            counts_after = {
                table: int(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                )
                for table in ("report_tasks", "report_revisions", "task_events")
            }
        self.assertEqual(counts_after, counts_before)

    def test_run_task_runtime_block_rolls_back_before_task_or_file_changes(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        self._insert_unsafe_legacy_report()
        with connect(self.db) as connection:
            task_before = tuple(
                connection.execute(
                    "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
                ).fetchone()
            )
            events_before = connection.execute(
                "SELECT COUNT(*) FROM task_events WHERE task_id=?", (task["id"],)
            ).fetchone()[0]
        with self.assertRaisesRegex(ReportTaskError, "outside the active release"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            task_after = tuple(
                connection.execute(
                    "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
                ).fetchone()
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM task_events WHERE task_id=?", (task["id"],)
                ).fetchone()[0],
                events_before,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM report_revisions WHERE task_id=?",
                    (task["id"],),
                ).fetchone()[0],
                0,
            )
        self.assertEqual(task_after, task_before)
        self.assertFalse((self.reports_root / str(task["id"])).exists())

    def test_invalidated_legacy_accident_no_longer_blocks_runtime(self) -> None:
        task_id = self._insert_unsafe_legacy_report()
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE report_revisions
                SET invalidated_at='2026-08-04T09:00:00Z',
                    invalidation_reason='test manifest repair'
                WHERE task_id=? AND revision=1
                """,
                (task_id,),
            )
            connection.commit()
        with connect(self.db) as connection:
            release = assert_report_runtime_ready(connection)
        self.assertEqual(release["id"], self.release_id)

    def test_retired_release_gray_queue_does_not_block_current_first_report(
        self,
    ) -> None:
        with connect(self.db) as connection:
            legacy_evaluation = connection.execute(
                """
                SELECT id FROM evaluation_versions
                WHERE content_id=1 AND release_id=? AND invalidated_at IS NULL
                ORDER BY id DESC LIMIT 1
                """,
                (LEGACY_V7_RELEASE_ID,),
            ).fetchone()
            assert legacy_evaluation is not None
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (legacy_evaluation["id"], now_utc(), now_utc()),
            )
            connection.commit()
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        report = run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        self.assertEqual(report["metadata"]["revision"], 1)

    def test_retired_release_report_does_not_release_current_gray_gate(self) -> None:
        self._insert_unsafe_legacy_report(creation_source="manual")
        with connect(self.db) as connection:
            current_evaluation = connection.execute(
                """
                SELECT id FROM evaluation_versions
                WHERE content_id=1 AND release_id=? AND invalidated_at IS NULL
                ORDER BY id DESC LIMIT 1
                """,
                (self.release_id,),
            ).fetchone()
            assert current_evaluation is not None
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id,evaluation_id,reason_code,status,created_at,updated_at
                ) VALUES (1,?,'evaluation_gray_zone','pending',?,?)
                """,
                (current_evaluation["id"], now_utc(), now_utc()),
            )
            connection.commit()
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        with self.assertRaisesRegex(ReportTaskError, "1 条灰区内容"):
            run_task(task["id"], db_path=self.db, reports_root=self.reports_root)

    def test_current_schema_initialization_does_not_rewrite_reports(self) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        run_task(task["id"], db_path=self.db, reports_root=self.reports_root)
        with connect(self.db) as connection:
            evaluation_id = connection.execute(
                "SELECT id FROM evaluation_versions WHERE content_id=1 ORDER BY id DESC"
            ).fetchone()[0]
            connection.execute(
                "UPDATE report_revisions SET contract_version='dcar-content-operations-report-v8.0'"
            )
            connection.execute(
                """
                INSERT INTO review_queue(
                    content_id, evaluation_id, reason_code, status, created_at, updated_at
                ) VALUES (1, ?, 'evaluation_gray_zone', 'pending', ?, ?)
                """,
                (evaluation_id, now_utc(), now_utc()),
            )
            connection.commit()
            revision_before = dict(
                connection.execute("SELECT * FROM report_revisions").fetchone()
            )
            task_before = dict(
                connection.execute(
                    "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
                ).fetchone()
            )
            changes_before = connection.total_changes
            initialize_database(connection)
            revision_after = dict(
                connection.execute("SELECT * FROM report_revisions").fetchone()
            )
            task_after = dict(
                connection.execute(
                    "SELECT * FROM report_tasks WHERE id=?", (task["id"],)
                ).fetchone()
            )
            self.assertEqual(connection.total_changes, changes_before)
        self.assertEqual(revision_after, revision_before)
        self.assertEqual(task_after, task_before)
        self.assertIsNone(revision_after["invalidated_at"])


if __name__ == "__main__":
    unittest.main()
