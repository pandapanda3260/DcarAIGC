from __future__ import annotations

import base64
import csv
import io
import hashlib
import json
import logging
import os
import sqlite3
import tempfile
import threading
import unittest
import zipfile
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from urllib.parse import quote
from uuid import uuid4
from xml.etree import ElementTree

from fastapi import FastAPI, Response
from fastapi.testclient import TestClient

# 测试环境一律关闭 LLM 补空：Mac 上 key 真实存在，associate/update-data 相关
# 用例若不关会真实调用豆包付费（TestClient 会同步执行 BackgroundTasks）。
os.environ.setdefault("DCAR_LLM_DISABLED", "1")

import v8.api as api_module
from v8 import media_lifecycle
from v8.account_roster import runtime_account_summary
from v8.contracts import CURRENT_REPORT_VERSION
from v8.operations import upsert_account
from v8.paid_drain import issue_activation_permit_in_transaction
from v8.profile_activations import MATRIX_PROFILE, TIKHUB_PROFILE, append_activation
from v8.system_roster import seal_system_members
from v8.evaluation import (
    RULE_VERSION,
    build_evidence_envelope,
)
from v8.matcher_dsl import (
    POINT_IDS,
    canonical_json,
    canonical_materialized_rule,
    load_bundle,
    materialize_point_rule,
    project_materialized_rule,
)
from v8.reports import create_task
from v8.storage import (
    CURRENT_SCHEMA_MIGRATION_NAME,
    LEGACY_MATCHER_RULE_SHA256,
    PROJECT_ROOT,
    RUNTIME_COMPATIBLE_SCHEMA_VERSIONS,
    SCHEMA_VERSION,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
    transaction,
)
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules
from tests.v9_report_fixture import activate_v9_report_fixture
from tests.schema_fixture import initialize_historical_schema
from workflow.storage import connect as legacy_connect
from workflow.storage import migrate as migrate_legacy


_SHEET_NAMESPACE = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def _xlsx_sheet_values(payload: bytes) -> list[list[str]]:
    root = ElementTree.fromstring(payload)
    rows: list[list[str]] = []
    for row in root.findall(".//x:sheetData/x:row", _SHEET_NAMESPACE):
        values: list[str] = []
        for cell in row.findall("x:c", _SHEET_NAMESPACE):
            inline = cell.find("x:is", _SHEET_NAMESPACE)
            if inline is not None:
                values.append(
                    "".join(
                        value.text or ""
                        for value in inline.findall(".//x:t", _SHEET_NAMESPACE)
                    )
                )
                continue
            value = cell.find("x:v", _SHEET_NAMESPACE)
            values.append(value.text if value is not None and value.text else "")
        rows.append(values)
    return rows


def _test_config(
    root: Path, *, db_name: str = "dcar_insight.sqlite3"
) -> api_module.ApiConfig:
    root.mkdir(parents=True, exist_ok=True)
    db_path = root / db_name
    if db_path.resolve() == api_module.DEFAULT_DB.resolve():
        raise AssertionError("API tests must never use the production database")
    return api_module.ApiConfig(
        db_path=db_path,
        reports_root=root / "reports",
        legacy_db_path=root / "legacy.sqlite3",
        operator_freeze_lock=root / "nonexistent-freeze.lock",
        scheduler_enabled=False,
        startup_catchup_enabled=False,
        project_root=root,
    )


def _insert_legacy_automatic_fixture(connection, content_id: int) -> int:
    release = ensure_legacy_evaluation_release(
        connection,
        rule_version="evaluation-v7",
        taxonomy_version="selling-points-v5.0",
    )
    envelope_id, evidence_sha256, _ = build_evidence_envelope(
        connection,
        content_id,
        rule_version=str(release["rule_version"]),
    )
    cursor = connection.execute(
        """
        INSERT INTO evaluation_versions(
            content_id,evidence_envelope_id,release_id,rule_version,
            taxonomy_version,matcher_rule_sha256,evidence_sha256,
            evaluation_source,evaluation_status,evidence_level,
            selling_point_score,selling_point_included,content_direction,
            payload_json,evaluated_at
        ) VALUES (?,?,?,?,?,?,?,'automatic','insufficient_evidence','V1',
                  0,0,'unknown',?,?)
        """,
        (
            content_id,
            envelope_id,
            release["id"],
            release["rule_version"],
            release["taxonomy_version"],
            release["matcher_rule_sha256"],
            evidence_sha256,
            json.dumps(
                {
                    "evaluation_status": "insufficient_evidence",
                    "evidence_level": "V1",
                    "evidence_summary": "legacy API fixture",
                    "primary_selling_point_id": "",
                    "selling_point_score": 0,
                    "selling_point_included": False,
                    "content_direction": "unknown",
                    "content_automotive_score": None,
                    "audience_automotive_score": None,
                    "action_intent_score": None,
                    "valid_unique_commenters": 0,
                    "acquisition_potential": None,
                    "matches": [],
                    "evaluation_source": "automatic",
                    "release_id": release["id"],
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            now_utc(),
        ),
    )
    assert cursor.lastrowid is not None
    return int(cursor.lastrowid)


def _seed_read_model_database(db_path: Path) -> None:
    source = json.loads(
        (PROJECT_ROOT / "config" / "business_selling_points_v4_final.json").read_text(
            encoding="utf-8"
        )
    )
    created_at = "2026-08-04T00:00:00Z"
    scene_map = {"二手车": "used_car", "新车": "new_car", "媒体-AI小懂": "media"}
    with connect(db_path) as connection:
        initialize_database(connection)
        connection.execute(
            """
            INSERT INTO taxonomy_versions(
                id,version,status,definition,source_path,source_sha256,
                created_at,published_at
            ) VALUES ('taxonomy-v5','selling-points-v5.0','published',?,?,?, ?,?)
            """,
            (
                source["definition"],
                "config/business_selling_points_v4_final.json",
                "fixture",
                created_at,
                created_at,
            ),
        )
        for label in source["labels"]:
            point = connection.execute(
                """
                INSERT INTO selling_points(taxonomy_id,code,tier,label,definition)
                VALUES ('taxonomy-v5',?,?,?,'')
                """,
                (label["id"], label["tier"], label["label"]),
            )
            scenes = label.get("business_scene_options") or [
                label.get("business_scene")
            ]
            for scene in scenes:
                normalized = scene_map.get(scene)
                if normalized:
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, normalized),
                    )
        ensure_legacy_evaluation_release(
            connection,
            rule_version="evaluation-v7",
            taxonomy_version="selling-points-v5.0",
        )
        account = connection.execute(
            """
            INSERT INTO accounts(
                phone,phone_normalized,operator_name,account_type,
                content_direction,created_at,updated_at
            ) VALUES ('13800138000','13800138000','fixture','boutique_ip',
                      'new_car',?,?)
            """,
            (created_at, created_at),
        )
        account_id = int(account.lastrowid)
        connection.execute(
            """
            INSERT INTO account_platform_identities(
                account_id,platform,uid,nickname,created_at,updated_at
            ) VALUES (?,'douyin','fixture-uid','fixture-account',?,?)
            """,
            (account_id, created_at, created_at),
        )
        connection.execute(
            """
            INSERT INTO content_items(
                link_id,platform,platform_content_id,canonical_url,account_id,
                raw_account_uid,raw_account_name,legacy_account_type,title,body,
                content_type,published_at,source_group,source_label,source_path,
                imported_at,created_at,updated_at
            ) VALUES (
                'T2ST3A','douyin','fixture-content',
                'https://www.douyin.com/video/fixture-content',?,
                'fixture-uid','fixture-account','boutique_ip','测试新车内容',
                '用于 API 临时库隔离测试','video','2026-08-03T08:00:00Z',
                'test','test','tests/test_v8_api.py',?,?,?
            )
            """,
            (account_id, created_at, created_at, created_at),
        )
        connection.commit()


def _seed_legacy_database(db_path: Path, root: Path) -> None:
    created_at = "2026-08-04T00:00:00Z"
    report_dir = root / "legacy-report"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "report.json"
    report = {
        "report_version": "channel-structured-conclusions-v7.0",
        "rule_version": "evaluation-v6",
        "metadata": {
            "generated_at": created_at,
            "run_id": "LEGACY-FIXTURE",
            "revision": 5,
        },
        "run_summary": {},
        "channels": {
            channel: {
                "denominator": 0,
                "count_distribution": {},
                "verticality": {},
            }
            for channel in ("douyin", "xiaohongshu")
        },
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")
    with legacy_connect(db_path) as connection:
        migrate_legacy(connection)
        connection.execute(
            """
            INSERT INTO runs(
                id,created_at,updated_at,mode,channel,status,progress,input_count,
                message,output_path,output_sha256,run_kind,scope,rule_version,
                report_version,is_formal_baseline,report_revision,report_stale
            ) VALUES (
                'LEGACY-FIXTURE',?,?,'test','dual','completed',100,0,'fixture',
                ?,'fixture','formal','dual','evaluation-v6',
                'channel-structured-conclusions-v7.0',1,5,0
            )
            """,
            (created_at, created_at, str(report_path)),
        )
        connection.execute(
            """
            INSERT INTO formal_baseline(singleton_id,run_id,selected_at)
            VALUES (1,'LEGACY-FIXTURE',?)
            """,
            (created_at,),
        )
        connection.executemany(
            """
            INSERT INTO report_revisions(
                run_id,revision,created_at,report_json_path,report_markdown_path,
                summary_image_path,output_sha256,source_evaluation_sha256,is_current
            ) VALUES ('LEGACY-FIXTURE',?,?,?,'fixture.md','fixture.png',
                      'fixture','fixture',?)
            """,
            [
                (revision, created_at, str(report_path), int(revision == 5))
                for revision in range(1, 6)
            ],
        )
        connection.commit()


class ApiFactoryIsolationTest(unittest.TestCase):
    def test_two_apps_keep_database_and_runtime_state_isolated(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp") as temporary:
            root = Path(temporary)
            config_a = _test_config(root / "a")
            config_b = _test_config(root / "b")
            _seed_read_model_database(config_a.db_path)
            _seed_read_model_database(config_b.db_path)
            created_at = "2026-08-04T00:00:00Z"
            with connect(config_b.db_path) as connection:
                connection.execute(
                    """
                    INSERT INTO accounts(
                        phone,phone_normalized,operator_name,account_type,
                        content_direction,created_at,updated_at
                    ) VALUES ('13900139000','13900139000','fixture-b','original',
                              'media',?,?)
                    """,
                    (created_at, created_at),
                )
                connection.commit()

            app_a = api_module.create_app(config_a)
            app_b = api_module.create_app(config_b)
            with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
                total_a = client_a.post("/api/v8/accounts/search", json={}).json()[
                    "total"
                ]
                total_b = client_b.post("/api/v8/accounts/search", json={}).json()[
                    "total"
                ]

            self.assertEqual(total_a, 1)
            self.assertEqual(total_b, 2)
            self.assertEqual(app_a.state.config.db_path, config_a.db_path)
            self.assertEqual(app_b.state.config.db_path, config_b.db_path)
            self.assertIsNot(app_a.state, app_b.state)
            self.assertIsNot(
                app_a.state.data_freshness_cache,
                app_b.state.data_freshness_cache,
            )
            self.assertIsNot(app_a.state.read_model_cache, app_b.state.read_model_cache)


class DataFreshnessCacheTest(unittest.TestCase):
    def test_ttl_deepcopy_and_failed_load_recovery(self) -> None:
        now = [10.0]
        calls = [0]
        cache = api_module.DataFreshnessCache(
            ttl_seconds=60.0,
            clock=lambda: now[0],
        )

        def load() -> dict[str, object]:
            calls[0] += 1
            return {"call": calls[0], "nested": {"status": "current"}}

        first = cache.get(load)
        first["nested"]["status"] = "mutated"  # type: ignore[index]
        second = cache.get(load)
        self.assertEqual(calls[0], 1)
        self.assertEqual(second["nested"], {"status": "current"})

        now[0] = 70.0
        self.assertEqual(cache.get(load)["call"], 2)
        self.assertEqual(calls[0], 2)

        attempts = [0]
        recovering = api_module.DataFreshnessCache(ttl_seconds=60.0)

        def fail_once() -> dict[str, object]:
            attempts[0] += 1
            if attempts[0] == 1:
                raise RuntimeError("fixture load failed")
            return {"status": "current"}

        with self.assertRaisesRegex(RuntimeError, "fixture load failed"):
            recovering.get(fail_once)
        self.assertEqual(recovering.get(fail_once), {"status": "current"})
        self.assertEqual(attempts[0], 2)

    def test_concurrent_misses_share_one_loader(self) -> None:
        cache = api_module.DataFreshnessCache(ttl_seconds=60.0)
        barrier = threading.Barrier(8)
        release = threading.Event()
        calls = [0]
        calls_lock = threading.Lock()
        results: list[dict[str, object] | None] = [None] * 8

        def load() -> dict[str, object]:
            with calls_lock:
                calls[0] += 1
            self.assertTrue(release.wait(2))
            return {"status": "current"}

        def read(index: int) -> None:
            barrier.wait()
            results[index] = cache.get(load)

        threads = [threading.Thread(target=read, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        release.set()
        for thread in threads:
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(calls[0], 1)
        self.assertEqual(results, [{"status": "current"}] * 8)


class ReadModelCacheTest(unittest.TestCase):
    def test_ttl_bounds_staleness_and_returns_deep_copies(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "cache.sqlite3"
            db_path.write_bytes(b"db-v1")
            now = [10.0]
            calls = [0]
            cache = api_module.ReadModelCache(
                ttl_seconds=30.0,
                max_entries=4,
                clock=lambda: now[0],
            )

            def load() -> dict[str, object]:
                calls[0] += 1
                return {"call": calls[0], "nested": {"status": "ready"}}

            first = cache.get(db_path, ("overview",), load)
            first["nested"]["status"] = "changed"  # type: ignore[index]
            self.assertEqual(
                cache.get(db_path, ("overview",), load),
                {"call": 1, "nested": {"status": "ready"}},
            )

            # Unrelated operational WAL writes must not defeat the deliberately
            # bounded read-model TTL.
            db_path.write_bytes(b"db-version-two")
            self.assertEqual(cache.get(db_path, ("overview",), load)["call"], 1)
            now[0] = 41.0
            self.assertEqual(cache.get(db_path, ("overview",), load)["call"], 2)

    def test_concurrent_misses_share_one_read_model_load(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "cache.sqlite3"
            db_path.write_bytes(b"fixture")
            cache = api_module.ReadModelCache(ttl_seconds=30.0)
            barrier = threading.Barrier(6)
            release = threading.Event()
            calls = [0]
            calls_lock = threading.Lock()
            results: list[dict[str, object] | None] = [None] * 6

            def load() -> dict[str, object]:
                with calls_lock:
                    calls[0] += 1
                self.assertTrue(release.wait(2))
                return {"status": "ready"}

            def read(index: int) -> None:
                barrier.wait()
                results[index] = cache.get(db_path, ("overview",), load)

            threads = [threading.Thread(target=read, args=(index,)) for index in range(6)]
            for thread in threads:
                thread.start()
            release.set()
            for thread in threads:
                thread.join(timeout=2)
                self.assertFalse(thread.is_alive())
            self.assertEqual(calls[0], 1)
            self.assertEqual(results, [{"status": "ready"}] * 6)

    def test_clear_during_load_uses_a_new_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "cache.sqlite3"
            db_path.write_bytes(b"fixture")
            cache = api_module.ReadModelCache(ttl_seconds=30.0)
            started = threading.Event()
            release = threading.Event()
            old_result: list[dict[str, object]] = []

            def old_load() -> dict[str, object]:
                started.set()
                self.assertTrue(release.wait(2))
                return {"generation": "old"}

            thread = threading.Thread(
                target=lambda: old_result.append(
                    cache.get(db_path, ("overview",), old_load)
                )
            )
            thread.start()
            self.assertTrue(started.wait(2))
            cache.clear()
            fresh = cache.get(
                db_path,
                ("overview",),
                lambda: {"generation": "fresh"},
            )
            release.set()
            thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(old_result, [{"generation": "old"}])
            self.assertEqual(fresh, {"generation": "fresh"})
            self.assertEqual(
                cache.get(
                    db_path,
                    ("overview",),
                    lambda: {"generation": "unexpected"},
                ),
                {"generation": "fresh"},
            )

    def test_successful_mutations_clear_cache_but_read_posts_do_not(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "cache.sqlite3"
            db_path.write_bytes(b"fixture")
            cache = api_module.ReadModelCache(ttl_seconds=30.0)
            application = FastAPI()
            application.state.read_model_cache = cache
            application.middleware("http")(api_module.privacy_safe_request_log)

            @application.post("/api/v8/contents/search")
            def search() -> dict[str, bool]:
                return {"ok": True}

            @application.post("/write")
            def write() -> dict[str, bool]:
                return {"ok": True}

            calls = [0]

            def load() -> dict[str, int]:
                calls[0] += 1
                return {"call": calls[0]}

            self.assertEqual(cache.get(db_path, ("overview",), load)["call"], 1)
            with TestClient(application) as client:
                self.assertEqual(client.post("/api/v8/contents/search").status_code, 200)
                self.assertEqual(cache.get(db_path, ("overview",), load)["call"], 1)
                self.assertEqual(client.post("/write").status_code, 200)
            self.assertEqual(cache.get(db_path, ("overview",), load)["call"], 2)

    def test_background_spu_job_clears_interim_cached_stats_on_completion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db_path = Path(temporary) / "cache.sqlite3"
            db_path.write_bytes(b"fixture")
            cache = api_module.ReadModelCache(ttl_seconds=30.0)
            started = threading.Event()
            release = threading.Event()
            calls = [0]

            def blocked_association(**_kwargs: object) -> None:
                started.set()
                self.assertTrue(release.wait(2))

            def load() -> dict[str, int]:
                calls[0] += 1
                return {"call": calls[0]}

            with (
                patch.object(
                    api_module,
                    "run_spu_association",
                    side_effect=blocked_association,
                ),
                patch.object(api_module, "default_spu_llm_hook", return_value=None),
            ):
                thread = threading.Thread(
                    target=api_module._run_spu_association_job,
                    args=(db_path, 1, None, None, cache),
                )
                thread.start()
                self.assertTrue(started.wait(2))
                self.assertEqual(
                    cache.get(db_path, ("spu-audience-stats", "all", ""), load)[
                        "call"
                    ],
                    1,
                )
                release.set()
                thread.join(timeout=2)

            self.assertFalse(thread.is_alive())
            self.assertEqual(
                cache.get(db_path, ("spu-audience-stats", "all", ""), load)[
                    "call"
                ],
                2,
            )


class JSONGZipMiddlewareTest(unittest.TestCase):
    def test_binary_range_response_is_not_transformed(self) -> None:
        application = FastAPI()
        application.add_middleware(
            api_module.JSONGZipMiddleware, minimum_size=1_000, compresslevel=5
        )
        body = b"0123456789" * 200

        @application.get("/range")
        def ranged() -> Response:
            return Response(
                body,
                status_code=206,
                media_type="video/mp4",
                headers={
                    "Accept-Ranges": "bytes",
                    "Content-Range": "bytes 0-1999/4000",
                },
            )

        with TestClient(application) as client:
            response = client.get("/range", headers={"Accept-Encoding": "gzip"})

        self.assertEqual(response.status_code, 206)
        self.assertEqual(response.content, body)
        self.assertEqual(response.headers["content-range"], "bytes 0-1999/4000")
        self.assertEqual(response.headers["content-length"], str(len(body)))
        self.assertNotIn("content-encoding", response.headers)


class ApiStartupSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def _patch_runtime_recovery(self):
        return (
            patch.object(
                api_module,
                "recover_interrupted_scheduler_runs",
                return_value=0,
            ),
            patch.object(
                api_module,
                "recover_stale_fetch_slots",
                return_value={"stale_candidates": 0, "recovered": 0},
            ),
            patch.object(
                api_module,
                "recover_stale_media_processing_slots",
                return_value={
                    "stale_candidates": 0,
                    "recovered": 0,
                    "retryable_failed": 0,
                    "terminal_failed": 0,
                    "cas_conflicts": 0,
                    "exhausted_normalized": 0,
                },
            ),
            patch.object(api_module, "_recover_interrupted_tasks", return_value=0),
            patch.object(
                api_module,
                "recover_orphan_association_runs",
                return_value=0,
            ),
        )

    def test_environment_isolated_candidate_is_rejected_before_sqlite_access(self) -> None:
        missing = self.root / "missing-candidate.sqlite3"
        config = _test_config(self.root, db_name=missing.name)
        config = replace(
            config,
            runtime_access_mode=api_module.DatabaseAccessMode.ISOLATED_CANDIDATE,
        )
        with (
            patch.object(
                api_module,
                "connect",
                side_effect=AssertionError("candidate preflight must precede SQLite"),
            ) as connection,
            self.assertRaisesRegex(RuntimeError, "isolated candidate database must already exist"),
        ):
            with TestClient(api_module.create_app(config)):
                pass
        connection.assert_not_called()
        self.assertFalse(missing.exists())

    def test_direct_config_cannot_relabel_installed_database_as_fixture(self) -> None:
        database = self.root / "installed.sqlite3"
        database.write_bytes(b"installed-sentinel")
        config = _test_config(self.root, db_name=database.name)
        with (
            patch.object(
                api_module,
                "is_installed_formal_database",
                return_value=True,
            ),
            patch.object(
                api_module,
                "connect",
                side_effect=AssertionError("formal preflight must precede SQLite"),
            ) as connection,
            self.assertRaisesRegex(RuntimeError, "explicit writer access mode"),
        ):
            with TestClient(api_module.create_app(config)):
                pass
        connection.assert_not_called()
        self.assertEqual(database.read_bytes(), b"installed-sentinel")

    def test_formal_database_detection_uses_existing_file_identity(self) -> None:
        formal = self.root / "formal.sqlite3"
        alias = self.root / "apfs-firmlink-spelling.sqlite3"
        formal.write_bytes(b"formal-sentinel")
        alias.hardlink_to(formal)
        self.assertNotEqual(alias.resolve(), formal.resolve())
        with patch.object(api_module, "DEFAULT_DB", formal):
            self.assertTrue(api_module._uses_formal_database(alias))

    def test_temporary_writable_database_is_still_initialized(self) -> None:
        config = _test_config(self.root, db_name="new.sqlite3")
        self.assertFalse(config.db_path.exists())
        recovery_patches = self._patch_runtime_recovery()
        with (
            patch.object(
                api_module,
                "initialize_database",
                wraps=initialize_database,
            ) as initialize,
            recovery_patches[0],
            recovery_patches[1],
            recovery_patches[2],
            recovery_patches[3],
            recovery_patches[4],
        ):
            with TestClient(api_module.create_app(config)):
                pass
        initialize.assert_called_once()
        # A newly initialized writable database may still have committed schema
        # pages in its WAL.  Use a normal SQLite reader here; immutable replica
        # mode intentionally ignores WAL files.
        with sqlite3.connect(config.db_path) as connection:
            self.assertEqual(
                int(connection.execute("PRAGMA user_version").fetchone()[0]),
                SCHEMA_VERSION,
            )

    def test_writable_lifespan_keeps_wal_sidecar_inodes_stable(self) -> None:
        config = _test_config(self.root, db_name="anchor.sqlite3")
        with connect(config.db_path) as connection:
            initialize_database(connection)
        probe = api_module._open_sqlite_runtime_anchor(config.db_path)
        try:
            self.assertEqual(probe.execute("PRAGMA query_only").fetchone()[0], 1)
            self.assertEqual(probe.execute("PRAGMA journal_mode").fetchone()[0], "wal")
            self.assertFalse(probe.in_transaction)
        finally:
            probe.close()
        recovery_patches = self._patch_runtime_recovery()
        application = api_module.create_app(config)
        with (
            recovery_patches[0],
            recovery_patches[1],
            recovery_patches[2],
            recovery_patches[3],
            recovery_patches[4],
            TestClient(application),
        ):
            anchor = getattr(application.state, "sqlite_runtime_anchor", None)
            self.assertIsNotNone(anchor)
            sidecars = [
                config.db_path.with_name(config.db_path.name + suffix)
                for suffix in ("-wal", "-shm")
            ]
            self.assertTrue(all(path.is_file() for path in sidecars))
            inodes = [path.stat().st_ino for path in sidecars]
            for value in range(3):
                with connect(config.db_path) as connection:
                    connection.execute(
                        "UPDATE schema_migrations SET applied_at=? WHERE version=?",
                        (f"2026-09-01T00:00:0{value}Z", SCHEMA_VERSION),
                    )
                    connection.commit()
            self.assertEqual([path.stat().st_ino for path in sidecars], inodes)
        self.assertIsNone(application.state.sqlite_runtime_anchor)

    def test_writable_anchor_closes_when_startup_recovery_fails(self) -> None:
        config = _test_config(self.root, db_name="anchor-failure.sqlite3")
        with connect(config.db_path) as connection:
            initialize_database(connection)
        application = api_module.create_app(config)
        with (
            patch.object(
                api_module,
                "recover_interrupted_scheduler_runs",
                side_effect=RuntimeError("injected recovery failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected recovery failure"),
        ):
            with TestClient(application):
                pass
        self.assertIsNone(
            getattr(application.state, "sqlite_runtime_anchor", None)
        )
        with sqlite3.connect(config.db_path) as connection:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0],
                "delete",
            )

    def test_existing_local_historical_database_requires_explicit_migration(self) -> None:
        config = _test_config(self.root, db_name="local-v16.sqlite3")
        with connect(config.db_path) as connection:
            initialize_historical_schema(connection, target_version=16)
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        before = config.db_path.read_bytes()
        sidecars = [
            config.db_path.with_name(config.db_path.name + suffix)
            for suffix in ("-wal", "-shm")
        ]
        with (
            patch.object(api_module, "initialize_database") as initialize,
            patch.object(api_module, "recover_interrupted_scheduler_runs") as recovery,
            self.assertRaisesRegex(RuntimeError, "offline schema migration is required"),
        ):
            with TestClient(api_module.create_app(config)):
                pass
        initialize.assert_not_called()
        recovery.assert_not_called()
        self.assertEqual(config.db_path.read_bytes(), before)
        self.assertFalse(any(path.exists() for path in sidecars))

    def test_existing_empty_local_database_can_initialize(self) -> None:
        config = _test_config(self.root, db_name="empty.sqlite3")
        with sqlite3.connect(config.db_path) as connection:
            connection.execute("VACUUM")
        recovery_patches = self._patch_runtime_recovery()
        with (
            recovery_patches[0],
            recovery_patches[1],
            recovery_patches[2],
            recovery_patches[3],
            recovery_patches[4],
        ):
            with TestClient(api_module.create_app(config)):
                pass
        with sqlite3.connect(config.db_path) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)

    def test_formal_compatible_database_is_validated_without_initialization(
        self,
    ) -> None:
        config = _test_config(self.root, db_name="formal-v16.sqlite3")
        _seed_read_model_database(config.db_path)
        recovery_patches = self._patch_runtime_recovery()
        with (
            patch.object(api_module, "DEFAULT_DB", config.db_path),
            patch.object(api_module, "initialize_database") as initialize,
            recovery_patches[0],
            recovery_patches[1],
            recovery_patches[2],
            recovery_patches[3],
            recovery_patches[4],
        ):
            with TestClient(api_module.create_app(config)):
                pass
        initialize.assert_not_called()

    def test_formal_schema_mismatch_fails_without_db_or_sidecar_writes(self) -> None:
        config = _test_config(self.root, db_name="formal-v15.sqlite3")
        with sqlite3.connect(config.db_path) as connection:
            connection.execute(
                "CREATE TABLE schema_migrations("
                "version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO schema_migrations(version,name,applied_at) "
                "VALUES (15,'spu-llm-assist','2026-08-18T00:00:00Z')"
            )
            connection.execute("PRAGMA user_version=15")
        sidecars = [
            config.db_path.with_name(config.db_path.name + suffix)
            for suffix in ("-wal", "-shm")
        ]
        before = hashlib.sha256(config.db_path.read_bytes()).hexdigest()
        recovery_patches = self._patch_runtime_recovery()
        with (
            patch.object(api_module, "DEFAULT_DB", config.db_path),
            patch.object(api_module, "initialize_database") as initialize,
            recovery_patches[0] as scheduler_recovery,
            recovery_patches[1] as fetch_recovery,
            recovery_patches[2] as media_recovery,
            recovery_patches[3] as task_recovery,
            recovery_patches[4] as association_recovery,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                r"offline schema migration is required:.*supported=\[19, 20\]",
            ):
                with TestClient(api_module.create_app(config)):
                    pass
        initialize.assert_not_called()
        scheduler_recovery.assert_not_called()
        fetch_recovery.assert_not_called()
        media_recovery.assert_not_called()
        task_recovery.assert_not_called()
        association_recovery.assert_not_called()
        self.assertEqual(hashlib.sha256(config.db_path.read_bytes()).hexdigest(), before)
        self.assertFalse(any(sidecar.exists() for sidecar in sidecars))

    def test_formal_freeze_blocks_before_writer_lock_creation(self) -> None:
        freeze_lock = self.root / "operator-freeze.lock"
        freeze_lock.write_text("{}", encoding="utf-8")
        writer_lock = self.root / "writer-worker.lock"
        config = api_module.ApiConfig(
            db_path=self.root / "formal.sqlite3",
            reports_root=self.root / "reports",
            legacy_db_path=self.root / "legacy.sqlite3",
            operator_freeze_lock=freeze_lock,
            writer_lock=writer_lock,
            scheduler_enabled=True,
            startup_catchup_enabled=False,
            daily_capture_reconcile_from=date(2026, 8, 21),
        )
        with (
            patch.object(api_module, "_uses_formal_database", return_value=True),
            patch.object(
                api_module,
                "_writer_process_lock",
                wraps=api_module._writer_process_lock,
            ) as writer_lock_context,
            patch.object(api_module, "initialize_database") as initialize,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "production startup blocked by operator freeze lock",
            ):
                with TestClient(api_module.create_app(config)):
                    pass
        writer_lock_context.assert_not_called()
        initialize.assert_not_called()
        self.assertFalse(writer_lock.exists())

    def test_reconcile_environment_date_is_strict_and_required_for_scheduler(
        self,
    ) -> None:
        with patch.dict(
            os.environ,
            {
                "DCAR_SCHEDULER_ENABLED": "1",
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-08-21",
            },
            clear=True,
        ):
            config = api_module.ApiConfig.from_env()
        self.assertEqual(config.daily_capture_reconcile_from, date(2026, 8, 21))
        self.assertEqual(
            config.effective_daily_capture_reconcile_from,
            date(2026, 8, 21),
        )

        with patch.dict(
            os.environ,
            {"DCAR_SCHEDULER_ENABLED": "1"},
            clear=True,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM is required",
            ):
                api_module.ApiConfig.from_env()

        invalid_values = (
            "",
            "2026-8-21",
            "20260821",
            "2026-02-30",
            " 2026-08-21",
            "2026-08-21 ",
        )
        for value in invalid_values:
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"DCAR_DAILY_CAPTURE_RECONCILE_FROM": value},
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, "strict|valid"):
                    api_module.ApiConfig.from_env()

    def test_reconcile_environment_rejects_non_scheduler_and_read_only_modes(
        self,
    ) -> None:
        invalid_environments = (
            (
                {
                    "DCAR_SCHEDULER_ENABLED": "0",
                    "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-08-21",
                },
                "requires DCAR_SCHEDULER_ENABLED=1",
            ),
            (
                {
                    "DCAR_SCHEDULER_ENABLED": "1",
                    "DCAR_DAILY_CAPTURE_RECONCILE_FROM": "2026-08-21",
                    "DCAR_READ_ONLY": "1",
                },
                "requires writable mode",
            ),
        )
        for environment, error in invalid_environments:
            with self.subTest(environment=environment), patch.dict(
                os.environ,
                environment,
                clear=True,
            ):
                with self.assertRaisesRegex(RuntimeError, error):
                    api_module.ApiConfig.from_env()

    def test_invalid_reconcile_config_fails_before_freeze_lock_writer_lock_and_db(
        self,
    ) -> None:
        reconcile_from = date(2026, 8, 21)
        invalid_configs = (
            (
                replace(
                    _test_config(self.root / "disabled"),
                    writer_lock=self.root / "disabled-writer.lock",
                    daily_capture_reconcile_from=reconcile_from,
                ),
                "requires DCAR_SCHEDULER_ENABLED=1",
            ),
            (
                replace(
                    _test_config(self.root / "read-only"),
                    writer_lock=self.root / "read-only-writer.lock",
                    scheduler_enabled=True,
                    read_only=True,
                    daily_capture_reconcile_from=reconcile_from,
                ),
                "requires writable mode",
            ),
        )
        for config, error in invalid_configs:
            with (
                self.subTest(error=error),
                patch.object(api_module, "_uses_formal_database") as formal_check,
                patch.object(api_module, "_writer_process_lock") as writer_lock,
                patch.object(api_module, "initialize_database") as initialize,
            ):
                with self.assertRaisesRegex(RuntimeError, error):
                    with TestClient(api_module.create_app(config)):
                        pass
                formal_check.assert_not_called()
                writer_lock.assert_not_called()
                initialize.assert_not_called()
                self.assertFalse(config.writer_lock.exists())

    def test_scheduler_missing_reconcile_date_fails_before_writer_lock(self) -> None:
        writer_lock = self.root / "missing-date-writer.lock"
        config = replace(
            _test_config(self.root / "missing-date"),
            writer_lock=writer_lock,
            scheduler_enabled=True,
        )
        with patch.object(api_module, "initialize_database") as initialize:
            with self.assertRaisesRegex(
                RuntimeError,
                "DCAR_DAILY_CAPTURE_RECONCILE_FROM is required",
            ):
                with TestClient(api_module.create_app(config)):
                    pass
        initialize.assert_not_called()
        self.assertFalse(writer_lock.exists())

    def test_scheduler_installs_reconcile_and_reports_status(self) -> None:
        config = replace(
            _test_config(self.root / "enabled"),
            scheduler_enabled=True,
            startup_catchup_enabled=True,
            daily_capture_reconcile_from=date(2026, 8, 21),
        )
        _seed_read_model_database(config.db_path)
        scheduler = MagicMock()
        scheduler.state = api_module.STATE_RUNNING
        scheduler.get_jobs.return_value = [
            SimpleNamespace(id="pipeline_reconcile"),
            SimpleNamespace(id="daily_report"),
        ]
        recovery_patches = self._patch_runtime_recovery()
        catchup_started = threading.Event()
        with (
            patch.object(
                api_module, "BackgroundScheduler", return_value=scheduler
            ) as scheduler_factory,
            patch.object(api_module, "install_jobs") as install_jobs,
            patch.object(
                api_module, "_run_startup_catchup",
                side_effect=lambda *_args, **_kwargs: catchup_started.set(),
            ) as startup_worker,
            patch.object(api_module, "assert_report_runtime_ready"),
            recovery_patches[0],
            recovery_patches[1],
            recovery_patches[2],
            recovery_patches[3],
            recovery_patches[4],
        ):
            with TestClient(api_module.create_app(config)) as client:
                self.assertTrue(catchup_started.wait(2))
                self.assertEqual(startup_worker.call_args.kwargs, {
                    "db_path": config.db_path, "reports_root": config.reports_root,
                    "effective_from": date(2026, 8, 21),
                })
                response = client.get("/api/v8/scheduler")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json()["pipeline_reconcile"],
                    {
                        "mode": "current_day_only",
                        "enabled": True,
                        "interval_seconds": 300,
                        "paid_round_cutoff": "20:00",
                    },
                )
                self.assertEqual(
                    response.json()["registered_job_ids"],
                    ["daily_report", "pipeline_reconcile"],
                )
                self.assertNotIn("daily_capture_reconcile", response.json())
        scheduler_factory.assert_called_once_with(
            timezone="Asia/Shanghai",
            executors={
                "default": {"type": "threadpool", "max_workers": 20},
                "control": {"type": "threadpool", "max_workers": 8},
                "report": {"type": "threadpool", "max_workers": 1},
                "reconcile": {"type": "threadpool", "max_workers": 1},
            },
        )
        install_jobs.assert_called_once_with(
            scheduler,
            db_path=config.db_path,
            reports_root=config.reports_root,
            capture_call_override=None,
            reconcile_effective_date=date(2026, 8, 21),
        )
        scheduler.start.assert_called_once_with()


class V8ApiTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.config = _test_config(root)
        self.db = self.config.db_path
        _seed_read_model_database(self.db)
        _seed_legacy_database(self.config.legacy_db_path, root)
        self.app = api_module.create_app(self.config)
        self.assertEqual(self.app.state.config.db_path, self.db)
        self.assertNotEqual(self.db.resolve(), api_module.DEFAULT_DB.resolve())
        self.client_context = TestClient(self.app)
        empty_recovery = {
            "stale_candidates": 0,
            "recovered": 0,
            "retryable_failed": 0,
            "terminal_failed": 0,
            "cas_conflicts": 0,
            "exhausted_normalized": 0,
        }
        with (
            patch.object(
                api_module,
                "recover_stale_fetch_slots",
                return_value={"stale_candidates": 0, "recovered": 0},
            ),
            patch.object(
                api_module,
                "recover_stale_media_processing_slots",
                return_value=empty_recovery,
            ),
            patch.object(api_module, "_recover_interrupted_tasks", return_value=0),
        ):
            self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)

    def test_openapi_version_matches_v8_contract_release(self) -> None:
        self.assertEqual(self.app.version, CURRENT_REPORT_VERSION)

    def test_large_json_responses_are_gzipped_without_changing_payload(self) -> None:
        compressed = self.client.get(
            "/api/v8/overview", headers={"Accept-Encoding": "gzip"}
        )
        identity = self.client.get(
            "/api/v8/overview", headers={"Accept-Encoding": "identity"}
        )

        self.assertEqual(compressed.status_code, 200)
        self.assertEqual(compressed.headers.get("content-encoding"), "gzip")
        self.assertNotIn("content-encoding", identity.headers)
        self.assertEqual(compressed.json(), identity.json())

    def test_health_reports_v8_database(self) -> None:
        response = self.client.get("/api/v8/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(response.json()["database"], "dcar_insight.sqlite3")
        self.assertIn("data_freshness", response.json())
        compatibility = response.json()["database_state"]["schema_compatibility"]
        self.assertTrue(compatibility["compatible"])
        # v14 落地时这里漏改成常量导致断言过期；改为跟随 SCHEMA_VERSION。
        self.assertEqual(compatibility["user_version"], SCHEMA_VERSION)
        self.assertEqual(
            compatibility["supported_versions"],
            sorted(RUNTIME_COMPATIBLE_SCHEMA_VERSIONS),
        )
        identity = response.json()["database_state"]["runtime_identity"]
        self.assertEqual(identity["schema"], "dcar-runtime-identity-v1")
        self.assertEqual(identity["report_version"], CURRENT_REPORT_VERSION)
        self.assertEqual(identity["database_schema_version"], SCHEMA_VERSION)
        self.assertEqual(
            identity["database_schema_migration"], CURRENT_SCHEMA_MIGRATION_NAME
        )
        with connect(self.db, read_only=True) as connection:
            release = connection.execute(
                "SELECT * FROM evaluation_releases WHERE status='active'"
            ).fetchone()
        self.assertIsNotNone(release)
        assert release is not None
        self.assertEqual(identity["active_release_id"], release["id"])
        self.assertEqual(identity["matcher_rule_sha256"], release["matcher_rule_sha256"])
        self.assertEqual(response.json()["paid_dispatch_state"], "invalid")
        self.assertEqual(response.json()["cutover_state"]["state"], "invalid")
        self.assertEqual(
            response.json()["cutover_state"]["reason"],
            "no acquisition profile is active",
        )
        self.assertFalse(response.json()["lifecycle_jobs_enabled"])
        self.assertIsNone(response.json()["media_lifecycle"]["activation"])

    def test_livez_never_opens_sqlite_and_readyz_uses_only_db_receipts(self) -> None:
        with patch.object(
            api_module, "connect", side_effect=AssertionError("livez opened SQLite")
        ):
            response = self.client.get("/api/v8/livez")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "alive"})

        with patch(
            "v8.scan_receipts.runtime_coverage",
            side_effect=AssertionError("readyz traversed raw scan manifests"),
        ):
            response = self.client.get("/api/v8/readyz")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "ready")
        self.assertTrue(all(response.json()["conditions"].values()))
        self.assertIsNone(response.json()["profile_day_receipt"])

    def test_health_requires_active_lifecycle_and_all_three_registered_jobs(self) -> None:
        archive = self.config.db_path.parent / "media-archive"
        archive.mkdir(mode=0o700)
        with connect(self.db) as connection, transaction(connection):
            media_lifecycle.activate(
                connection,
                mode="enrollment_only",
                activation_id="health-fixture",
                release="health-release",
                rules_sha256="f" * 64,
                archive_root=archive,
                now="2026-08-29T00:00:00Z",
            )
        scheduler = MagicMock()
        scheduler.state = api_module.STATE_RUNNING
        scheduler.get_jobs.return_value = [
            SimpleNamespace(id="media_archive"),
            SimpleNamespace(id="media_retention"),
            SimpleNamespace(id="media_restore"),
        ]
        self.app.state.scheduler = scheduler
        self.app.state.scheduler_enabled = True
        enrollment_health = self.client.get("/api/v8/health").json()
        self.assertEqual(
            enrollment_health["media_lifecycle"]["activation"]["mode"],
            "enrollment_only",
        )
        self.assertFalse(enrollment_health["lifecycle_jobs_enabled"])

        with connect(self.db) as connection, transaction(connection):
            media_lifecycle.activate(
                connection,
                mode="active",
                activation_id="health-fixture",
                release="health-release",
                rules_sha256="f" * 64,
                archive_root=archive,
                proofs={
                    "contract_version": media_lifecycle.FIXTURE_PROOF_CONTRACT,
                    "fixture_only": True,
                    "mac_consumers": True,
                    "server_pairing": True,
                    "canary_restore": True,
                },
                now="2026-08-29T00:01:00Z",
            )
        active_health = self.client.get("/api/v8/health").json()
        self.assertEqual(
            active_health["media_lifecycle"]["activation"]["mode"], "active"
        )
        self.assertTrue(active_health["lifecycle_jobs_enabled"])

        scheduler.get_jobs.return_value.pop()
        incomplete_health = self.client.get("/api/v8/health").json()
        self.assertFalse(incomplete_health["lifecycle_jobs_enabled"])

    def test_daily_capture_freshness_excludes_backfill_slots(self) -> None:
        reference = datetime(2026, 8, 11, tzinfo=timezone.utc)
        daily_finished = reference - timedelta(hours=37)
        backfill_finished = reference - timedelta(hours=1)
        with connect(self.db) as connection:
            account_id = int(
                connection.execute(
                    "SELECT id FROM accounts ORDER BY id LIMIT 1"
                ).fetchone()[0]
            )
            connection.executemany(
                """
                INSERT INTO fetch_slots(
                    account_id,content_id,stage,window_key,provider,adapter_version,
                    status,attempt_count,finished_at,created_at,updated_at
                ) VALUES (?,NULL,'discovery',?,'fixture','fixture-v1','succeeded',
                          1,?,?,?)
                """,
                [
                    (
                        account_id,
                        "range:2010-01-01:20260811T000000:douyin:fixture",
                        backfill_finished.isoformat().replace("+00:00", "Z"),
                        backfill_finished.isoformat().replace("+00:00", "Z"),
                        backfill_finished.isoformat().replace("+00:00", "Z"),
                    ),
                    (
                        account_id,
                        "2026-08-10:douyin:page:1",
                        daily_finished.isoformat().replace("+00:00", "Z"),
                        daily_finished.isoformat().replace("+00:00", "Z"),
                        daily_finished.isoformat().replace("+00:00", "Z"),
                    ),
                ],
            )
            connection.execute(
                """
                INSERT INTO scheduler_runs(
                    job_id,scheduled_for,status,started_at,completed_at,details_json
                ) VALUES ('daily_capture','2026-08-10T18:00:00Z','failed',
                          '2026-08-10T18:00:00Z','2026-08-10T18:30:00Z','{}')
                """
            )
            connection.commit()

            freshness = api_module._data_freshness(
                connection,
                current_at=reference,
            )
            self.assertEqual(freshness["status"], "unknown")
            self.assertIsNone(freshness["last_successful_capture_at"])
            self.assertIsNone(freshness["latest_capture_run"])
            self.assertFalse(freshness["discovery_coverage"]["complete"])
            self.assertIsNone(freshness["discovery_coverage"]["tikhub_expected_members"])

            current_finished = reference - timedelta(hours=35)
            connection.execute(
                """
                UPDATE fetch_slots SET finished_at=?,updated_at=?
                WHERE window_key='2026-08-10:douyin:page:1'
                """,
                (
                    current_finished.isoformat().replace("+00:00", "Z"),
                    current_finished.isoformat().replace("+00:00", "Z"),
                ),
            )
            connection.commit()
            self.assertEqual(
                api_module._data_freshness(connection, current_at=reference)["status"],
                "unknown",
            )

    def test_startup_catchup_worker_records_results(self) -> None:
        fake_app = SimpleNamespace(
            state=SimpleNamespace(
                catchup_status="running", catchup_results=[], catchup_error=None
            )
        )
        expected = [{"job_id": "daily_report", "status": "partial"}]
        with patch.object(api_module, "startup_catchup", return_value=expected) as catchup:
            api_module._run_startup_catchup(
                fake_app,
                db_path=self.db,
                reports_root=self.config.reports_root,
                effective_from=date(2026, 9, 3),
            )
        catchup.assert_called_once_with(
            db_path=self.db, reports_root=self.config.reports_root,
            effective_from=date(2026, 9, 3),
        )
        self.assertEqual(fake_app.state.catchup_status, "succeeded")
        self.assertEqual(fake_app.state.catchup_results, expected)
        self.assertIsNone(fake_app.state.catchup_error)

    def test_startup_catchup_worker_propagates_item_failure_and_deferral(self) -> None:
        cases = (
            (
                [{"job_id": "daily_report", "status": "deferred"}],
                "deferred",
            ),
            (
                [
                    {"job_id": "daily_report", "status": "deferred"},
                    {"job_id": "weekly_report", "status": "failed"},
                ],
                "failed",
            ),
            ([{"job_id": "daily_report", "status": "unexpected"}], "failed"),
        )
        for results, expected_status in cases:
            with self.subTest(expected_status=expected_status, results=results):
                fake_app = SimpleNamespace(
                    state=SimpleNamespace(
                        catchup_status="running",
                        catchup_results=[],
                        catchup_error=None,
                    )
                )
                with patch.object(
                    api_module, "startup_catchup", return_value=results
                ):
                    api_module._run_startup_catchup(
                        fake_app,
                        db_path=self.db,
                        reports_root=self.config.reports_root,
                    )
                self.assertEqual(fake_app.state.catchup_status, expected_status)
                self.assertEqual(fake_app.state.catchup_results, results)

    def test_startup_catchup_worker_records_top_level_exception(self) -> None:
        fake_app = SimpleNamespace(
            state=SimpleNamespace(
                catchup_status="running", catchup_results=[], catchup_error=None
            )
        )
        with patch.object(
            api_module, "startup_catchup", side_effect=RuntimeError("catchup boom")
        ), self.assertLogs(api_module.LOGGER, level="ERROR") as logs:
            api_module._run_startup_catchup(
                fake_app,
                db_path=self.db,
                reports_root=self.config.reports_root,
            )
        self.assertEqual(fake_app.state.catchup_status, "failed")
        self.assertEqual(fake_app.state.catchup_results, [])
        self.assertEqual(fake_app.state.catchup_error, "catchup boom")
        self.assertIn("startup catch-up failed", "\n".join(logs.output))

    def test_scheduler_status_reports_disabled_startup_catchup(self) -> None:
        response = self.client.get("/api/v8/scheduler")
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["requested"])
        self.assertFalse(response.json()["enabled"])
        self.assertEqual(
            response.json()["report_runtime"], {"ready": None, "error": None}
        )
        self.assertEqual(response.json()["startup_catchup"]["status"], "disabled")
        self.assertEqual(response.json()["startup_catchup"]["mode"], "report_only")
        self.assertEqual(
            response.json()["scheduler_run_recovery"], {"interrupted": 0}
        )
        self.assertEqual(
            response.json()["pipeline_reconcile"],
            {
                "mode": "current_day_only",
                "enabled": False,
                "interval_seconds": 300,
                "paid_round_cutoff": "20:00",
            },
        )
        self.assertEqual(response.json()["registered_job_ids"], [])
        self.assertNotIn("daily_capture_reconcile", response.json())
        self.assertIn("fetch_slot_recovery", response.json())
        self.assertIn("data_freshness", response.json())

    def test_background_report_waits_for_shared_pipeline_report_lock(self) -> None:
        called = threading.Event()
        thread = threading.Thread(
            target=api_module._run_task_in_background,
            kwargs={
                "task_id": "fixture-task",
                "db_path": self.db,
                "reports_root": self.config.reports_root,
            },
        )
        with patch.object(api_module, "run_task", side_effect=lambda *_args, **_kwargs: called.set()):
            with api_module.PIPELINE_REPORT_EXECUTION_LOCK:
                thread.start()
                self.assertFalse(called.wait(0.05))
            thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertTrue(called.is_set())

    def test_freshness_is_consistent_across_operational_endpoints(self) -> None:
        expected = {"status": "current", "marker": "shared-cache-fixture"}
        with patch.object(api_module, "_data_freshness", return_value=expected) as freshness:
            health = self.client.get("/api/v8/health").json()["data_freshness"]
            overview = self.client.get("/api/v8/overview").json()["data_freshness"]
            scheduler = self.client.get("/api/v8/scheduler").json()["data_freshness"]
        freshness.assert_called_once()
        self.assertEqual(overview, health)
        self.assertEqual(scheduler, health)

    def test_partial_publishable_receipt_is_stale_not_unknown(self) -> None:
        coverage = {
            "status": "partial_publishable",
            "complete": False,
            "partial_publishable": True,
            "round_run_id": None,
        }
        with patch(
            "v8.runtime_receipts.latest_runtime_coverage",
            return_value=coverage,
        ):
            freshness = self.client.get("/api/v8/health").json()["data_freshness"]
        self.assertEqual(freshness["status"], "stale")
        self.assertEqual(freshness["discovery_coverage"], coverage)

    def test_health_exposes_runtime_database_and_writer_lock_identities(self) -> None:
        health = self.client.get("/api/v8/health").json()
        identity = self.db.stat()
        self.assertEqual(
            health["runtime_database_identity"],
            {
                "canonical_path": str(self.db.resolve(strict=True)),
                "device": identity.st_dev,
                "inode": identity.st_ino,
                "nlink": identity.st_nlink,
                "access_mode": "isolated_candidate",
            },
        )
        scheduler = self.client.get("/api/v8/scheduler").json()
        self.assertEqual(
            scheduler["writer_lock"],
            {
                "path": str(self.config.writer_lock),
                "device": None,
                "inode": None,
                "held": False,
            },
        )
        self.assertEqual(health["writer_lock"], scheduler["writer_lock"])

    def test_read_only_replica_allows_search_and_blocks_writes(self) -> None:
        replica_config = api_module.ApiConfig(
            db_path=self.config.db_path,
            reports_root=self.config.reports_root,
            legacy_db_path=self.config.legacy_db_path,
            operator_freeze_lock=self.config.operator_freeze_lock,
            scheduler_enabled=False,
            startup_catchup_enabled=False,
            read_only=True,
        )
        replica = api_module.create_app(replica_config)
        with TestClient(replica) as replica_client:
            health = replica_client.get("/api/v8/health")
            self.assertEqual(health.status_code, 200)
            self.assertEqual(health.json()["mode"], "read_only_replica")
            self.assertTrue(health.json()["read_only"])
            self.assertEqual(
                health.json()["runtime_database_identity"]["access_mode"],
                "formal_read",
            )
            self.assertEqual(
                replica_client.post("/api/v8/accounts/search", json={}).status_code,
                200,
            )
            exported = replica_client.post(
                "/api/v8/accounts/export",
                json={"douyin_authorization_targets": []},
            )
            self.assertEqual(exported.status_code, 200)
            self.assertEqual(
                exported.headers["content-type"],
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            blocked = replica_client.post("/api/v8/tasks", json={})
            self.assertEqual(blocked.status_code, 403)
            self.assertEqual(
                blocked.json()["detail"],
                "当前处于只读保护模式：可以查看数据，但暂时不能新增、编辑或刷新。请等写入服务恢复后再试。",
            )
            self.assertEqual(
                replica_client.patch("/api/v8/accounts/1", json={}).status_code,
                403,
            )

    def test_read_only_replica_never_changes_sqlite_or_creates_sidecars(self) -> None:
        with connect(self.db) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        sidecars = [
            self.db.with_name(self.db.name + suffix) for suffix in ("-wal", "-shm")
        ]
        for sidecar in sidecars:
            sidecar.unlink(missing_ok=True)
        before = hashlib.sha256(self.db.read_bytes()).hexdigest()
        replica_config = api_module.ApiConfig(
            db_path=self.config.db_path,
            reports_root=self.config.reports_root,
            legacy_db_path=self.config.legacy_db_path,
            operator_freeze_lock=self.config.operator_freeze_lock,
            scheduler_enabled=False,
            startup_catchup_enabled=False,
            read_only=True,
        )
        with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
            replica = api_module.create_app(replica_config)
            with TestClient(replica) as replica_client:
                self.assertIsNone(
                    getattr(replica.state, "sqlite_runtime_anchor", None)
                )
                health = replica_client.get("/api/v8/health")
                self.assertEqual(health.status_code, 200)
                self.assertEqual(health.json()["database_state"]["sha256"], before)
                self.assertEqual(
                    replica_client.get("/api/v8/overview").status_code,
                    200,
                )
                self.assertEqual(
                    replica_client.get("/api/v8/scheduler").status_code,
                    200,
                )
                self.assertEqual(
                    replica_client.get("/api/v8/tasks").status_code,
                    200,
                )
                self.assertEqual(
                    replica_client.post("/api/v8/contents/search", json={}).status_code,
                    200,
                )
        after = hashlib.sha256(self.db.read_bytes()).hexdigest()
        self.assertEqual(after, before)
        self.assertFalse(any(sidecar.exists() for sidecar in sidecars))

    def test_overview_has_three_shanghai_windows_and_no_fake_forecast(self) -> None:
        response = self.client.get("/api/v8/overview")
        self.assertEqual(response.status_code, 200)
        value = response.json()
        self.assertEqual(set(value["windows"]), {"yesterday", "this_week", "last_week"})
        self.assertEqual(value["timezone"], "Asia/Shanghai")
        self.assertNotIn("并非抓取故障", json.dumps(value, ensure_ascii=False))
        for window in value["windows"].values():
            self.assertNotIn("empty_explanation", window)
            metrics = window["metrics"]
            self.assertEqual(metrics["estimated_new_users"]["value"], None)
            self.assertEqual(metrics["estimated_new_users"]["unit"], "person")
            self.assertNotEqual(metrics["estimated_new_users"]["status"], "partial")
            self.assertIn("duplicate_rate", metrics)
            self.assertEqual(list(window["channels"]), ["douyin", "xiaohongshu"])
            for channel in window["channels"].values():
                self.assertIsInstance(channel["selling_points"], list)
                self.assertEqual(
                    list(channel["summary"]["metrics"]),
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
                self.assertEqual(
                    list(channel["scenes"]), ["used_car", "new_car", "media"]
                )
                for scene in channel["scenes"].values():
                    self.assertEqual(
                        list(scene["metrics"]), list(channel["summary"]["metrics"])
                    )
        self.assertIn("duplicate_fingerprint_coverage", value["data_quality"])
        self.assertIn("duplicate_calibration_ready", value["data_quality"])

    def test_overview_uses_one_cutoff_for_all_audience_slices(self) -> None:
        cutoff = "2026-08-07T06:30:00Z"
        with patch.object(api_module, "now_utc", return_value=cutoff) as clock:
            value = api_module.v8_overview(self.db)
        self.assertEqual(clock.call_count, 1)
        self.assertEqual(value["generated_at"], cutoff)
        for window in value["windows"].values():
            for channel in window["channels"].values():
                self.assertEqual(
                    channel["summary"]["audience_quality"]["report_cutoff_at"],
                    cutoff,
                )
                for scene in channel["scenes"].values():
                    self.assertEqual(
                        scene["audience_quality"]["report_cutoff_at"], cutoff
                    )

    def test_overview_reuses_facts_without_changing_window_results(self) -> None:
        now = datetime(2026, 8, 4, 12, tzinfo=api_module.SHANGHAI)
        cutoff = "2026-08-04T04:00:00Z"
        bounds = api_module._windows(now)
        # The fixture's August 3 publication belongs to both yesterday and
        # this_week. Compare full summaries with independently selected facts.
        with connect(self.db) as connection:
            expected = {
                key: api_module._window_summary(
                    connection, start, end, report_cutoff_at=cutoff
                )
                for key, (start, end) in bounds.items()
            }
        with (
            patch.object(api_module, "_windows", return_value=bounds),
            patch.object(api_module, "now_utc", return_value=cutoff),
            patch.object(
                api_module, "select_content_metrics", wraps=api_module.select_content_metrics
            ) as metrics,
            patch.object(
                api_module, "formal_eligible_release_evaluations",
                wraps=api_module.formal_eligible_release_evaluations,
            ) as evaluations,
        ):
            result = api_module.v8_overview(self.db)
        self.assertEqual(result["windows"], expected)
        self.assertEqual(metrics.call_count, 1)
        self.assertEqual(evaluations.call_count, 1)
        self.assertEqual(len(metrics.call_args.args[1]), 1)
        self.assertNotIn("cutoff_at", metrics.call_args.kwargs)
        self.assertEqual(result["windows"]["yesterday"]["metrics"]["publication_count"]["value"], 1)
        self.assertEqual(result["windows"]["this_week"]["metrics"]["publication_count"]["value"], 1)
        self.assertEqual(result["windows"]["last_week"]["metrics"]["publication_count"]["value"], 0)

    def test_overview_timing_distinguishes_cached_reads_without_exposing_data(self) -> None:
        first = self.client.get("/api/v8/overview")
        second = self.client.get("/api/v8/overview")
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json(), second.json())
        self.assertIn('cache;desc="miss"', first.headers["server-timing"])
        self.assertIn('cache;desc="hit"', second.headers["server-timing"])
        self.assertIn("queue;dur=", first.headers["server-timing"])
        self.assertIn("total;dur=", first.headers["server-timing"])
        self.assertNotIn(str(self.db), first.headers["server-timing"])
        self.assertNotIn("timings", first.json())

    def test_overview_channel_conclusions_use_latest_metrics_and_valid_exposure(
        self,
    ) -> None:
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
                connection.executemany(
                    """
                    INSERT INTO evaluation_releases(
                        id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                        created_at,updated_at,activated_at,retired_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            "test-current-release",
                            RULE_VERSION,
                            "selling-points-test",
                            LEGACY_MATCHER_RULE_SHA256,
                            "active",
                            created_at,
                            created_at,
                            created_at,
                            None,
                        ),
                        (
                            "test-old-release",
                            "evaluation-v6",
                            "selling-points-test",
                            LEGACY_MATCHER_RULE_SHA256,
                            "retired",
                            created_at,
                            created_at,
                            created_at,
                            created_at,
                        ),
                    ],
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
                            link_id,
                            platform_id,
                            f"https://example.com/{platform_id}",
                            platform_id,
                            published_at,
                            direction,
                            created_at,
                            created_at,
                            created_at,
                        ),
                    )
                ids = {
                    row["platform_content_id"]: int(row["id"])
                    for row in connection.execute(
                        "SELECT id,platform_content_id FROM content_items"
                    )
                }
                connection.execute(
                    "UPDATE content_items SET manual_content_direction='unknown' WHERE id=?",
                    (ids["content-a"],),
                )
                evaluations = [
                    (ids["content-a"], "a" * 64, "E1", 1, "new_car", 80, 60, 55),
                    (ids["content-b"], "b" * 64, "C1", 1, "used_car", 60, None, None),
                    (ids["content-c"], "c" * 64, None, 0, "other", 40, None, None),
                ]
                for (
                    content_id,
                    evidence_sha,
                    code,
                    included,
                    direction,
                    content_score,
                    audience_score,
                    acquisition_score,
                ) in evaluations:
                    connection.execute(
                        """
                        INSERT INTO evaluation_versions(
                            content_id,release_id,rule_version,taxonomy_version,
                            matcher_rule_sha256,evidence_sha256,
                            evaluation_source,evaluation_status,evidence_level,
                            primary_selling_point_code,selling_point_score,
                            selling_point_included,content_direction,
                            content_automotive_score,audience_automotive_score,
                            acquisition_potential_score,payload_json,evaluated_at
                        ) VALUES (?,?,?,?,?,?,'automatic','evaluated','V3',?,90,?,?,?,?,?,'{}',?)
                        """,
                        (
                            content_id,
                            "test-current-release",
                            RULE_VERSION,
                            "selling-points-test",
                            LEGACY_MATCHER_RULE_SHA256,
                            evidence_sha,
                            code,
                            included,
                            direction,
                            content_score,
                            audience_score,
                            acquisition_score,
                            created_at,
                        ),
                    )
                connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,release_id,rule_version,taxonomy_version,
                        matcher_rule_sha256,evidence_sha256,
                        evaluation_source,evaluation_status,evidence_level,
                        selling_point_included,content_direction,
                        payload_json,evaluated_at
                    ) VALUES (?,?,'evaluation-v6','selling-points-test',?,?,'automatic',
                              'evaluated','V3',0,'new_car','{}','2026-08-02T13:00:00Z')
                    """,
                    (
                        ids["content-a"],
                        "test-old-release",
                        LEGACY_MATCHER_RULE_SHA256,
                        "d" * 64,
                    ),
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
                connection.execute(
                    """
                    INSERT INTO content_metric_snapshots(
                        content_id,captured_at,window_key,view_count,status,source
                    ) VALUES (
                        ?,'2026-08-02T15:00:00Z','2026-08-02',NULL,'missing','test-missing'
                    )
                    ON CONFLICT(content_id,window_key) DO UPDATE SET
                        captured_at=excluded.captured_at, view_count=excluded.view_count,
                        status=excluded.status, source=excluded.source
                    """,
                    (ids["content-a"],),
                )
                self.assertEqual(connection.execute(
                    "SELECT COUNT(*) FROM content_metric_snapshots WHERE content_id=? AND window_key='2026-08-02'",
                    (ids["content-a"],),
                ).fetchone()[0], 1)
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
            self.assertEqual(
                summary["core_selling_point_count_share"]["percentage"], 33.33
            )
            self.assertEqual(
                summary["selling_point_exposure_share"]["percentage"], 75.0
            )
            self.assertEqual(
                summary["core_selling_point_exposure_share"]["percentage"], 0.0
            )
            self.assertEqual(douyin["valid_exposure_items"], 2)
            self.assertEqual(douyin["exposure_coverage_percentage"], 100.0)
            points = {item["code"]: item for item in douyin["selling_points"]}
            self.assertEqual(set(points), {"E1", "C1"})
            self.assertEqual(points["E1"]["label"], "核心卖点")
            self.assertEqual(points["C1"]["label"], "其他卖点")
            self.assertEqual(points["E1"]["tier"], "core")
            self.assertEqual(sum(point["publication_count"] for point in points.values()), 2)
            self.assertEqual(points["E1"]["count_share"]["denominator"], 3)
            self.assertIsNone(points["E1"]["view_count"]["value"])
            self.assertEqual(points["E1"]["view_count"]["status"], "missing")
            self.assertEqual(points["C1"]["view_count"]["value"], 300)
            self.assertEqual(points["C1"]["view_count"]["status"], "stale")
            self.assertEqual(points["C1"]["stale_view_items"], 1)
            self.assertEqual(points["C1"]["exposure_share"]["denominator"], 400)
            self.assertEqual(points["C1"]["exposure_share"]["status"], "stale")
            self.assertIsNone(points["C1"]["exposure_share"]["percentage"])
            self.assertEqual(summary["content_verticality"]["value"], 60)
            self.assertEqual(summary["automotive_user_rate"]["kind"], "ratio")
            self.assertIsNone(summary["automotive_user_rate"]["percentage"])
            self.assertEqual(summary["automotive_user_rate"]["status"], "missing")
            self.assertEqual(summary["acquisition_potential"]["value"], 55)

            new_car = douyin["scenes"]["new_car"]["metrics"]
            used_car = douyin["scenes"]["used_car"]["metrics"]
            media = douyin["scenes"]["media"]["metrics"]
            self.assertEqual(new_car["selling_point_count_share"]["denominator"], 3)
            self.assertEqual(
                new_car["selling_point_exposure_share"]["denominator"], 400
            )
            self.assertEqual(new_car["selling_point_exposure_share"]["percentage"], 0.0)
            self.assertEqual(
                used_car["selling_point_exposure_share"]["percentage"], 75.0
            )
            self.assertEqual(media["selling_point_count_share"]["percentage"], 0.0)
            self.assertEqual(media["content_verticality"]["status"], "not_applicable")

            xiaohongshu = window["channels"]["xiaohongshu"]
            self.assertEqual(xiaohongshu["selling_points"], [])
            self.assertEqual(xiaohongshu["publication_count"], 0)
            self.assertTrue(
                all(
                    metric["status"] == "not_applicable"
                    for metric in xiaohongshu["summary"]["metrics"].values()
                )
            )

    def test_five_page_read_models_use_current_v8_data(self) -> None:
        with connect(self.db) as connection:
            expected_account_count = int(
                connection.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
            )
            expected_content_count = int(
                connection.execute("SELECT COUNT(*) FROM content_items").fetchone()[0]
            )
        with patch.object(
            api_module,
            "_data_freshness",
            side_effect=AssertionError("task list must not verify scan receipts"),
        ) as task_freshness:
            tasks = self.client.get("/api/v8/tasks")
        task_freshness.assert_not_called()
        accounts = self.client.post("/api/v8/accounts/search", json={})
        with patch.object(
            api_module,
            "_data_freshness",
            side_effect=AssertionError("content search must not verify scan receipts"),
        ) as freshness:
            contents = self.client.post(
                "/api/v8/contents/search", json={"page": 1, "page_size": 20}
            )
        freshness.assert_not_called()
        selling_points = self.client.get("/api/v8/selling-points")

        self.assertEqual(tasks.status_code, 200)
        self.assertNotIn("data_freshness", tasks.json())
        self.assertEqual(tasks.json()["total"], len(tasks.json()["items"]))
        for task in tasks.json()["items"]:
            self.assertTrue(
                {
                    "id",
                    "task_type",
                    "period_start",
                    "period_end",
                    "task_status",
                    "content_count",
                    "missing_boundary_count",
                    "revision_count",
                    "historical_revision_count",
                    "current_valid_revision",
                    "stale_display_revision",
                    "display_effective_revision",
                }.issubset(task)
            )
        self.assertEqual(accounts.status_code, 200)
        self.assertEqual(accounts.json()["total"], expected_account_count)
        self.assertNotIn("legacy_unassociated_content_count", accounts.json())
        self.assertNotIn("pending_platform_identity_count", accounts.json())
        self.assertNotIn("pending_platform_identities", accounts.json())
        self.assertEqual(contents.status_code, 200)
        self.assertNotIn("data_freshness", contents.json())
        self.assertEqual(contents.json()["total"], expected_content_count)
        self.assertEqual(len(contents.json()["items"]), min(expected_content_count, 20))
        overview = self.client.get("/api/v8/overview")
        self.assertEqual(overview.status_code, 200)
        # v16 起概览数据质量不再有复核计数
        self.assertNotIn("pending_reviews", overview.json()["data_quality"])
        self.assertEqual(selling_points.status_code, 200)
        self.assertEqual(
            selling_points.json()["taxonomy"]["version"], "selling-points-v5.0"
        )
        self.assertEqual(len(selling_points.json()["items"]), 25)
        self.assertTrue(
            all(
                item["matcher_rule"] is None
                and set(item["scene_hits"]) == {"used_car", "new_car", "media"}
                and {
                    "positive_evidence",
                    "negative_evidence",
                    "boundary_rules",
                    "scenes",
                }.issubset(item)
                for item in selling_points.json()["items"]
            )
        )
        m_points = [
            item
            for item in selling_points.json()["items"]
            if item["code"].startswith("M")
        ]
        self.assertTrue(m_points)
        self.assertTrue(all(item["primary_hits"] == 0 for item in m_points))

    def test_selling_point_read_model_rejects_multiple_published_taxonomies(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('unexpected-published','selling-points-v4.9','published',
                          'unexpected',?,?)
                """,
                (now_utc(), now_utc()),
            )
            connection.commit()
            before = "\n".join(connection.iterdump())

        response = self.client.get("/api/v8/selling-points")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], "卖点标准暂时无法读取，请联系管理员。")
        with connect(self.db) as connection:
            after = "\n".join(connection.iterdump())
        self.assertEqual(after, before)

    def test_selling_point_read_model_rejects_active_release_taxonomy_mismatch(
        self,
    ) -> None:
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('retired-v5.1','selling-points-v5.1','retired',
                          'mismatch',?,?)
                """,
                (now_utc(), now_utc()),
            )
            connection.execute(
                """
                UPDATE evaluation_releases
                SET taxonomy_version='selling-points-v5.1'
                WHERE status='active'
                """
            )
            connection.commit()
            before = "\n".join(connection.iterdump())

        response = self.client.get("/api/v8/selling-points")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], "卖点标准暂时无法读取，请联系管理员。")
        with connect(self.db) as connection:
            after = "\n".join(connection.iterdump())
        self.assertEqual(after, before)

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
        self.assertEqual(
            report.json()["report_version"], "channel-structured-conclusions-v7.0"
        )

    def test_existing_frontend_read_routes_remain_available(self) -> None:
        overview = self.client.get("/api/overview")
        latest = self.client.get("/api/report/latest")
        runs = self.client.get("/api/runs")
        self.assertEqual(overview.status_code, 200)
        self.assertEqual(latest.status_code, 200)
        self.assertEqual(runs.status_code, 200)
        self.assertEqual(
            overview.json()["report_version"], "channel-structured-conclusions-v7.0"
        )

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
        self.config = _test_config(Path(self.temp.name), db_name="api.sqlite3")
        self.assertEqual(self.config.db_path, self.db)
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
            self.content_id = int(content.lastrowid)
            self.evaluation_id = _insert_legacy_automatic_fixture(
                connection, self.content_id
            )
            connection.commit()
        self.app = api_module.create_app(self.config)
        self.client_context = TestClient(self.app)
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)
        self.temp.cleanup()

    def _activate_current_report_release(self) -> None:
        activate_v9_report_fixture(self.db, [self.content_id])

    def test_backfilling_evaluation_is_hidden_from_display_surfaces(
        self,
    ) -> None:
        with connect(self.db) as connection:
            captured_at = "2099-01-01T00:00:00Z"
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at
                ) VALUES ('taxonomy-v51','selling-points-v5.1','draft','test',?)
                """,
                (captured_at,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at
                ) VALUES ('release-v8','evaluation-v8','selling-points-v5.1',?,
                          'backfilling',?,?)
                """,
                ("8" * 64, captured_at, captured_at),
            )
            backfill = connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id,release_id,rule_version,taxonomy_version,
                    matcher_rule_sha256,evidence_sha256,evaluation_source,
                    evaluation_status,evidence_level,primary_selling_point_code,
                    selling_point_score,selling_point_included,content_direction,
                    payload_json,evaluated_at
                ) VALUES (?,'release-v8','evaluation-v8','selling-points-v5.1',?,?,
                          'automatic','evaluated','V3','X8',99,1,'new_car',?,?)
                """,
                (
                    self.content_id,
                    "8" * 64,
                    "9" * 64,
                    json.dumps(
                        {
                            "primary_selling_point_id": "X8",
                            "content_direction": "new_car",
                        }
                    ),
                    captured_at,
                ),
            )
            connection.commit()
            backfill_id = int(backfill.lastrowid)

        searched = self.client.post(
            "/api/v8/contents/search", json={"page": 1, "page_size": 50}
        )
        self.assertEqual(searched.status_code, 200)
        item = searched.json()["items"][0]
        self.assertEqual(item["display_evaluation_id"], self.evaluation_id)
        self.assertNotEqual(item["primary_selling_point_code"], "X8")
        self.assertEqual(item["evaluation_freshness"], "current")
        self.assertFalse(item["evaluation_is_stale"])

        evidence = self.client.get(f"/api/v8/contents/{self.content_id}/evidence")
        self.assertEqual(evidence.status_code, 200)
        evidence_value = evidence.json()
        self.assertEqual(evidence_value["display_evaluation_id"], self.evaluation_id)
        # v16 起证据接口是纯只读的，不再暴露复核 CAS 锚点
        self.assertNotIn("base_evaluation_id", evidence_value)
        self.assertNotIn("review", evidence_value)
        self.assertNotEqual(
            evidence_value["evaluation"]["primary_selling_point_id"], "X8"
        )
        self.assertEqual(evidence_value["evaluation_freshness"], "current")

        # v16 起复核接口整体下线
        self.assertEqual(self.client.get("/api/v8/reviews").status_code, 404)
        self.assertGreater(backfill_id, 0)

    def test_selling_point_hits_are_split_into_the_latest_business_scene(self) -> None:
        with connect(self.db) as connection:
            point = connection.execute(
                """
                INSERT INTO selling_points(taxonomy_id,code,tier,label,definition)
                VALUES ('taxonomy','C2','other','跨场景卖点','test')
                """
            )
            connection.executemany(
                "INSERT INTO selling_point_scenes(selling_point_id,scene) VALUES (?,?)",
                [(point.lastrowid, "used_car"), (point.lastrowid, "new_car")],
            )
            created_at = now_utc()
            content_ids = []
            for link_id, platform_id in (("B2CD3E", "scene-a"), ("C2DE3F", "scene-b")):
                content = connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,title,content_type,
                        imported_at,created_at,updated_at
                    ) VALUES (?,'douyin',?,?,'跨场景','video',?,?,?)
                    """,
                    (
                        link_id,
                        platform_id,
                        f"https://www.douyin.com/video/{platform_id}",
                        created_at,
                        created_at,
                        created_at,
                    ),
                )
                content_ids.append(int(content.lastrowid))

            connection.execute(
                "UPDATE content_items SET manual_content_direction='other' WHERE id=?",
                (content_ids[0],),
            )
            connection.execute(
                "UPDATE content_items SET manual_content_direction='new_car' WHERE id=?",
                (content_ids[1],),
            )

            evaluation_rows = [
                (content_ids[0], "1" * 64, "used_car"),
                (content_ids[0], "2" * 64, "new_car"),
                (content_ids[1], "3" * 64, "used_car"),
            ]
            release = ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            for content_id, evidence_sha, scene in evaluation_rows:
                evaluation = connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        content_id,release_id,rule_version,taxonomy_version,
                        matcher_rule_sha256,evidence_sha256,
                        evaluation_source,evaluation_status,evidence_level,
                        primary_selling_point_code,selling_point_score,selling_point_included,
                        content_direction,payload_json,evaluated_at
                    ) VALUES (?,?,'evaluation-v7','selling-points-v5.0',?,?,'automatic',
                              'evaluated','V3','C2',90,1,?,'{}',?)
                    """,
                    (
                        content_id,
                        release["id"],
                        LEGACY_MATCHER_RULE_SHA256,
                        evidence_sha,
                        scene,
                        created_at,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO evaluation_matches(
                        evaluation_id,selling_point_code,scene,match_role,score,evidence_json
                    ) VALUES (?,'C2',?,'primary',90,'{}')
                    """,
                    (evaluation.lastrowid, scene),
                )

            retired_release = ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v6",
                taxonomy_version="selling-points-v5.0",
            )
            retired = connection.execute(
                """
                INSERT INTO evaluation_versions(
                    content_id,release_id,rule_version,taxonomy_version,
                    matcher_rule_sha256,evidence_sha256,
                    evaluation_source,evaluation_status,evidence_level,
                    primary_selling_point_code,selling_point_score,selling_point_included,
                    content_direction,payload_json,evaluated_at
                ) VALUES (?,?,'evaluation-v6','selling-points-v5.0',?,?,'automatic',
                          'evaluated','V3','C2',90,1,'new_car','{}',?)
                """,
                (
                    content_ids[1],
                    retired_release["id"],
                    LEGACY_MATCHER_RULE_SHA256,
                    "4" * 64,
                    created_at,
                ),
            )
            connection.execute(
                """
                INSERT INTO evaluation_matches(
                    evaluation_id,selling_point_code,scene,match_role,score,evidence_json
                ) VALUES (?,'C2','new_car','primary',90,'{}')
                """,
                (retired.lastrowid,),
            )
            connection.commit()

        response = self.client.get("/api/v8/selling-points")
        self.assertEqual(response.status_code, 200)
        point_value = next(
            item for item in response.json()["items"] if item["code"] == "C2"
        )
        self.assertEqual(point_value["primary_hits"], 2)
        self.assertEqual(point_value["scene_hits"]["new_car"]["primary_hits"], 1)
        self.assertEqual(point_value["scene_hits"]["used_car"]["primary_hits"], 1)
        self.assertEqual(point_value["scene_hits"]["media"]["primary_hits"], 0)

    def test_evidence_media_file_and_processing_search_are_readable(self) -> None:
        media_path = Path(self.temp.name) / "evidence.jpg"
        asr_path = Path(self.temp.name) / "asr.json"
        ocr_path = Path(self.temp.name) / "ocr.json"
        media_path.write_bytes(b"local-image-evidence")
        asr_path.write_text(
            json.dumps(
                {"status": "success", "model": "pinned", "text": "完整的本地语音证据"}
            ),
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
            for artifact_type, path in (
                ("media", media_path),
                ("asr", asr_path),
                ("ocr", ocr_path),
            ):
                cursor = connection.execute(
                    """
                    INSERT INTO evidence_artifacts(
                        content_id,artifact_type,local_path,status,sha256,created_at
                    ) VALUES (?,?,?,'available',?,?)
                    """,
                    (
                        self.content_id,
                        artifact_type,
                        str(path),
                        artifact_type * 16,
                        now_utc(),
                    ),
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
                (
                    self.content_id,
                    now_utc(),
                    "2026-W31",
                    str(comments),
                    "c" * 64,
                    now_utc(),
                ),
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
            connection.executemany(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,status,
                    attempt_count,created_at,updated_at
                ) VALUES (?,?,?,?,?,1,?,?)
                """,
                [
                    (
                        self.content_id,
                        "p" * 64,
                        "download",
                        "download-test",
                        "pending",
                        now_utc(),
                        now_utc(),
                    ),
                    (
                        self.content_id,
                        "r" * 64,
                        "frames",
                        "frames-running",
                        "running",
                        now_utc(),
                        now_utc(),
                    ),
                    (
                        self.content_id,
                        "f" * 64,
                        "frames",
                        "frames-failed",
                        "retryable_failed",
                        now_utc(),
                        now_utc(),
                    ),
                    (
                        self.content_id,
                        "t" * 64,
                        "asr",
                        "asr-terminal",
                        "terminal_failed",
                        now_utc(),
                        now_utc(),
                    ),
                ],
            )
            connection.commit()
        evidence = self.client.get(f"/api/v8/contents/{self.content_id}/evidence")
        self.assertEqual(evidence.status_code, 200)
        value = evidence.json()
        self.assertEqual(value["display_evaluation_id"], self.evaluation_id)
        self.assertEqual(value["asr"]["text"], "完整的本地语音证据")
        self.assertEqual(value["ocr"]["text"], "关键帧文字证据\n车辆保养流程")
        self.assertEqual(value["comments"]["stored_count"], 1)
        self.assertEqual(len(value["media"]), 1)
        self.assertEqual(value["media_availability"]["status"], "available")
        media = self.client.get(value["media"][0]["url"])
        self.assertEqual(media.status_code, 200)
        self.assertEqual(media.content, b"local-image-evidence")
        processing = self.client.post(
            "/api/v8/media-processing/search",
            json={"content_id": self.content_id, "status": "succeeded"},
        )
        self.assertEqual(processing.status_code, 200)
        self.assertEqual(processing.json()["total"], 1)
        self.assertEqual(
            processing.json()["slot_status_counts"],
            {
                "pending": 0,
                "running": 0,
                "succeeded": 1,
                "retryable_failed": 0,
                "terminal_failed": 0,
            },
        )
        frame_processing = self.client.post(
            "/api/v8/media-processing/search",
            json={"content_id": self.content_id, "processor_type": "frames"},
        )
        self.assertEqual(frame_processing.status_code, 200)
        self.assertEqual(frame_processing.json()["total"], 2)
        self.assertEqual(
            frame_processing.json()["slot_status_counts"],
            {
                "pending": 0,
                "running": 1,
                "succeeded": 0,
                "retryable_failed": 1,
                "terminal_failed": 0,
            },
        )
        self._activate_current_report_release()
        existing_evidence = self.client.post(
            f"/api/v8/contents/{self.content_id}/media/retry",
            json={"allow_paid_refresh": False},
        )
        self.assertEqual(existing_evidence.status_code, 200)
        self.assertEqual(existing_evidence.json()["status"], "evidence_ready")
        self.assertEqual(existing_evidence.json()["provider_cost"], 0.0)

        with connect(self.db) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        read_only_app = api_module.create_app(replace(self.config, read_only=True))
        with TestClient(read_only_app) as read_only_client:
            replica_evidence = read_only_client.get(
                f"/api/v8/contents/{self.content_id}/evidence"
            )
            self.assertEqual(replica_evidence.status_code, 200)
            self.assertEqual(replica_evidence.json()["media"], [])
            self.assertEqual(
                replica_evidence.json()["media_availability"]["status"], "omitted"
            )
            omitted_file = read_only_client.get(
                f"/api/v8/contents/{self.content_id}/evidence/files/"
                f"{artifact_ids[0]}/0"
            )
            self.assertEqual(omitted_file.status_code, 410)
            self.assertEqual(
                omitted_file.json()["detail"],
                "线上版本没有包含这张图片或视频，当前无法查看。请回到本地重新处理并发布。",
            )

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE evidence_artifacts SET sha256=? WHERE id=?",
                (hashlib.sha256(media_path.read_bytes()).hexdigest(), artifact_ids[0]),
            )
            connection.commit()
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        verified_read_only_app = api_module.create_app(
            replace(self.config, read_only=True)
        )
        with TestClient(verified_read_only_app) as verified_client:
            verified_evidence = verified_client.get(
                f"/api/v8/contents/{self.content_id}/evidence"
            ).json()
            self.assertEqual(
                verified_evidence["media_availability"]["status"], "available"
            )
            verified_file = verified_client.get(verified_evidence["media"][0]["url"])
            self.assertEqual(verified_file.status_code, 200)
            self.assertEqual(verified_file.content, b"local-image-evidence")

    def test_task_cancel_and_resume_routes_preserve_revision_history(self) -> None:
        with connect(self.db) as connection:
            manual_before = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_versions
                    WHERE evaluation_source='manual_review'
                    """
                ).fetchone()[0]
            )
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
        self._activate_current_report_release()
        resumed = self.client.post(f"/api/v8/tasks/{task['id']}/resume")
        self.assertEqual(resumed.status_code, 200)
        self.assertEqual(resumed.json()["task_status"], "queued")
        generated = self.client.get(f"/api/v8/tasks/{task['id']}").json()
        self.assertIn(generated["task_status"], {"succeeded", "partial"})
        self.assertEqual(len(generated["revisions"]), 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_versions
                    WHERE evaluation_source='manual_review'
                    """
                ).fetchone()[0],
                manual_before,
            )

    def test_selling_point_api_edits_only_an_unreleased_matcher_draft(self) -> None:
        bundle = load_bundle()
        with connect(self.db) as connection:
            for code in sorted(POINT_IDS - {"C1"}):
                rule = materialize_point_rule(bundle, code)
                projection = project_materialized_rule(rule)
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition
                    ) VALUES ('taxonomy',?,?,?,'test')
                    """,
                    (code, "other", f"测试卖点 {code}"),
                )
                for scene in projection["scenes"]:
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            connection.commit()
        backfill_v5_1_matcher_rules(db_path=self.db)
        drafted = self.client.post("/api/v8/selling-points/draft")
        self.assertEqual(drafted.status_code, 200)
        self.assertEqual(drafted.json()["version"], "selling-points-v5.1")
        invalid_delete = self.client.delete("/api/v8/selling-points/items/not-a-code")
        self.assertEqual(invalid_delete.status_code, 422)
        matcher_rule = materialize_point_rule(bundle, "C1")
        projection = project_materialized_rule(matcher_rule)
        updated = self.client.patch(
            "/api/v8/selling-points/items/C1",
            json={
                "tier": "core",
                "label": "汽车养护服务",
                "definition": "车辆保养与维修能力",
                "matcher_rule": matcher_rule,
            },
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["matcher_rule"], matcher_rule)
        self.assertEqual(updated.json()["scenes"], projection["scenes"])
        self.assertEqual(
            updated.json()["positive_evidence"], projection["positive_evidence"]
        )

        legacy_writable_projection = self.client.patch(
            "/api/v8/selling-points/items/C1",
            json={
                "tier": "core",
                "label": "汽车养护服务",
                "definition": "车辆保养与维修能力",
                "matcher_rule": matcher_rule,
                "scenes": ["media"],
            },
        )
        self.assertEqual(legacy_writable_projection.status_code, 422)

        mismatched_rule = materialize_point_rule(bundle, "C2")
        mismatched = self.client.patch(
            "/api/v8/selling-points/items/C1",
            json={
                "tier": "core",
                "label": "汽车养护服务",
                "definition": "车辆保养与维修能力",
                "matcher_rule": mismatched_rule,
            },
        )
        self.assertEqual(mismatched.status_code, 422)

        published = self.client.post("/api/v8/selling-points/publish")
        self.assertEqual(published.status_code, 409)
        current = self.client.get("/api/v8/selling-points")
        self.assertEqual(current.json()["taxonomy"]["version"], "selling-points-v5.0")
        draft = self.client.get("/api/v8/selling-points/draft")
        self.assertEqual(draft.status_code, 200)
        c1 = next(item for item in draft.json()["items"] if item["code"] == "C1")
        self.assertEqual(c1["label"], "汽车养护服务")
        self.assertEqual(c1["matcher_rule"], matcher_rule)

    def test_selling_point_draft_version_conflict_is_zero_write_409(self) -> None:
        rule = materialize_point_rule(load_bundle(), "C1")
        projection = project_materialized_rule(rule)
        captured_at = now_utc()
        with connect(self.db) as connection:
            point = connection.execute(
                """
                SELECT id FROM selling_points
                WHERE taxonomy_id='taxonomy' AND code='C1'
                """
            ).fetchone()
            assert point is not None
            connection.execute(
                """
                UPDATE selling_points
                SET positive_evidence_json=?,negative_evidence_json=?,
                    boundary_rules_json=?,matcher_rule_json=?
                WHERE id=?
                """,
                (
                    canonical_json(projection["positive_evidence"]),
                    canonical_json(projection["negative_evidence"]),
                    canonical_json(projection["boundary_rules"]),
                    canonical_materialized_rule(rule),
                    point["id"],
                ),
            )
            connection.execute(
                "DELETE FROM selling_point_scenes WHERE selling_point_id=?",
                (point["id"],),
            )
            for scene in projection["scenes"]:
                connection.execute(
                    """
                    INSERT INTO selling_point_scenes(selling_point_id,scene)
                    VALUES (?,?)
                    """,
                    (point["id"], scene),
                )
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('retired-v5.1','selling-points-v5.1','retired',
                          'rollback history',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.commit()
            before = "\n".join(connection.iterdump())

        response = self.client.post("/api/v8/selling-points/draft")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["detail"], "无法进入草稿编辑，请刷新页面后重试。")
        with connect(self.db) as connection:
            after = "\n".join(connection.iterdump())
        self.assertEqual(after, before)

    def test_custom_task_generates_revision_and_downloads_run_scoped_files(
        self,
    ) -> None:
        with connect(self.db) as connection:
            manual_before = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_versions
                    WHERE evaluation_source='manual_review'
                    """
                ).fetchone()[0]
            )
        self._activate_current_report_release()
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,published_at,
                    title,content_type,imported_at,created_at,updated_at
                ) VALUES (
                    'LONGID','douyin','7668604214154726706',
                    'https://www.douyin.com/video/7668604214154726706',
                    '2026-07-01T05:00:00Z','历史导出兼容样本','image',?,?,?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()
        created = self.client.post(
            "/api/v8/tasks",
            json={"period_start": "2026-07-01", "period_end": "2026-07-01"},
        )
        self.assertEqual(created.status_code, 200)
        queued = created.json()
        # Generation runs outside the request, so creation answers immediately and
        # the workbench polls the task read model for progress.
        self.assertEqual(queued["task_status"], "queued")
        self.assertEqual(queued["progress"], 0)
        self.assertEqual(queued["revisions"], [])
        task_id = queued["id"]
        with patch.object(
            api_module,
            "_data_freshness",
            side_effect=AssertionError("task detail must not verify scan receipts"),
        ) as task_freshness:
            detail_response = self.client.get(f"/api/v8/tasks/{task_id}")
        task_freshness.assert_not_called()
        self.assertEqual(detail_response.status_code, 200)
        self.assertNotIn("data_freshness", detail_response.json())
        value = detail_response.json()
        self.assertEqual(value["task_status"], "partial")
        self.assertEqual(value["progress"], 100)
        self.assertEqual(len(value["revisions"]), 1)
        report = self.client.get(f"/api/v8/tasks/{task_id}/revisions/1/report")
        self.assertEqual(report.status_code, 200)
        self.assertEqual(report.json()["metadata"]["task_id"], task_id)
        self.assertEqual(
            report.json()["metadata"]["collection_cutoff_at"],
            value["created_at"],
        )
        self.assertEqual(
            report.json()["data_quality_details"]["metrics_freshness"]["status"],
            "below_threshold",
        )
        self.assertEqual(
            report.json()["data_quality_details"]["discovery_coverage"]["status"],
            "unknown",
        )
        self.assertFalse(report.json()["data_quality"]["roster_evidence_valid"])
        self.assertFalse(report.json()["data_quality"]["scan_traceable"])
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_versions
                    WHERE evaluation_source='manual_review'
                    """
                ).fetchone()[0],
                manual_before,
            )
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

        # A revision keeps its archived image immutable, while the ZIP image is
        # a current-template derivative of that revision's verified report.json.
        # Replacing the archived SVG here proves the bundle does not regress to
        # the legacy three-card artwork for historical revisions.
        legacy_svg = (
            b'<svg xmlns="http://www.w3.org/2000/svg" width="1200" '
            b'height="675"><text>legacy-three-card</text></svg>'
        )
        with connect(self.db) as connection:
            summary_row = connection.execute(
                """
                SELECT local_path FROM report_files
                WHERE task_id=? AND revision=1 AND file_kind='summary-svg'
                """,
                (task_id,),
            ).fetchone()
            self.assertIsNotNone(summary_row)
            summary_path = PROJECT_ROOT / str(summary_row["local_path"])
            summary_path.write_bytes(legacy_svg)
            connection.execute(
                """
                UPDATE report_files SET sha256=?,byte_size=?
                WHERE task_id=? AND revision=1 AND file_kind='summary-svg'
                """,
                (
                    hashlib.sha256(legacy_svg).hexdigest(),
                    len(legacy_svg),
                    task_id,
                ),
            )
            content_row = connection.execute(
                """
                SELECT local_path FROM report_files
                WHERE task_id=? AND revision=1 AND file_kind='content-csv'
                """,
                (task_id,),
            ).fetchone()
            self.assertIsNotNone(content_row)
            content_path = PROJECT_ROOT / str(content_row["local_path"])
            current_content = list(
                csv.DictReader(
                    io.StringIO(
                        content_path.read_text(encoding="utf-8-sig"), newline=""
                    )
                )
            )
            legacy_fields = [
                field
                for field in current_content[0]
                if field
                not in {
                    "platform_content_id",
                    "content_type",
                    "primary_selling_point_label",
                }
            ]
            legacy_buffer = io.StringIO(newline="")
            legacy_writer = csv.DictWriter(
                legacy_buffer,
                fieldnames=legacy_fields,
                extrasaction="ignore",
                lineterminator="\r\n",
            )
            legacy_writer.writeheader()
            legacy_writer.writerows(current_content)
            legacy_content = ("\ufeff" + legacy_buffer.getvalue()).encode("utf-8")
            content_path.write_bytes(legacy_content)
            connection.execute(
                """
                UPDATE report_files SET sha256=?,byte_size=?
                WHERE task_id=? AND revision=1 AND file_kind='content-csv'
                """,
                (
                    hashlib.sha256(legacy_content).hexdigest(),
                    len(legacy_content),
                    task_id,
                ),
            )
            connection.commit()

        with connect(self.db) as connection:
            before_download = {
                "task": tuple(
                    connection.execute(
                        "SELECT task_status,progress,message FROM report_tasks WHERE id=?",
                        (task_id,),
                    ).fetchone()
                ),
                "revision": tuple(
                    connection.execute(
                        "SELECT report_sha256,invalidated_at FROM report_revisions WHERE task_id=? AND revision=1",
                        (task_id,),
                    ).fetchone()
                ),
                "file_count": connection.execute(
                    "SELECT COUNT(*) FROM report_files WHERE task_id=? AND revision=1",
                    (task_id,),
                ).fetchone()[0],
                "database": "\n".join(connection.iterdump()),
                "file_hashes": {
                    str(row["local_path"]): hashlib.sha256(
                        (PROJECT_ROOT / str(row["local_path"])).read_bytes()
                    ).hexdigest()
                    for row in connection.execute(
                        "SELECT local_path FROM report_files WHERE task_id=? AND revision=1 AND status='available'",
                        (task_id,),
                    ).fetchall()
                },
            }
        with patch.object(api_module, "render_summary_png", return_value=False):
            derived = self.client.get(
                f"/api/v8/tasks/{task_id}/revisions/1/download"
            )
        self.assertEqual(derived.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(derived.content)) as archive:
            derived_svg = archive.read("01_图片报告.svg")
        self.assertIn("DCar Insight · 渠道与内容结构".encode(), derived_svg)
        self.assertIn("账号类型构成".encode(), derived_svg)
        self.assertNotIn(b"legacy-three-card", derived_svg)

        bundle = self.client.get(f"/api/v8/tasks/{task_id}/revisions/1/download")
        self.assertEqual(bundle.status_code, 200)
        self.assertEqual(bundle.headers["content-type"], "application/zip")
        expected_bundle_name = f'{value["name"]}.zip'
        self.assertEqual(
            bundle.headers["content-disposition"],
            f'attachment; filename="report.zip"; filename*=UTF-8\'\'{quote(expected_bundle_name, safe="")}',
        )
        self.assertEqual(bundle.headers["cache-control"], "private, no-store")
        with zipfile.ZipFile(io.BytesIO(bundle.content)) as archive:
            self.assertIsNone(archive.testzip())
            names = archive.namelist()
            self.assertEqual(len(names), 2)
            self.assertIn("02_数据明细.xlsx", names)
            image_names = [
                name for name in names if name in {"01_图片报告.png", "01_图片报告.svg"}
            ]
            self.assertEqual(len(image_names), 1)
            self.assertTrue(
                all(
                    not name.startswith("/") and ".." not in Path(name).parts
                    for name in names
                )
            )
            bundled_image = archive.read(image_names[0])
            if image_names[0].endswith(".png"):
                self.assertEqual(
                    (
                        int.from_bytes(bundled_image[16:20], "big"),
                        int.from_bytes(bundled_image[20:24], "big"),
                    ),
                    (1200, 675),
                )
            else:
                self.assertIn(b'<svg xmlns="http://www.w3.org/2000/svg"', bundled_image)
            workbook = archive.read("02_数据明细.xlsx")
        with zipfile.ZipFile(io.BytesIO(workbook)) as xlsx:
            self.assertIsNone(xlsx.testzip())
            workbook_xml = xlsx.read("xl/workbook.xml")
            self.assertIn("报告说明".encode(), workbook_xml)
            self.assertIn("内容明细".encode(), workbook_xml)
            self.assertIn("渠道结论".encode(), workbook_xml)
            content_sheet = xlsx.read("xl/worksheets/sheet2.xml")
        content_values = _xlsx_sheet_values(content_sheet)
        self.assertEqual(
            content_values[0],
            [
                "周期",
                "报告任务",
                "平台作品编号",
                "系统内容编号",
                "平台",
                "内容类型",
                "发布时间（北京时间）",
                "内容链接",
                "标题",
                "平台账号编号",
                "账号名称",
                "账号类型",
                "内容方向",
                "资料完整度",
                "主要卖点编号",
                "卖点信息",
                "卖点评分",
                "内容垂直度",
                "播放/阅读数",
                "评论数",
            ],
        )
        historical_row = next(row for row in content_values[1:] if row[3] == "LONGID")
        self.assertEqual(historical_row[2], "7668604214154726706")
        self.assertEqual(historical_row[2], historical_row[7].rsplit("/", 1)[-1])
        self.assertEqual(historical_row[5], "图文")
        self.assertEqual(historical_row[13], "还没有评估")
        self.assertEqual(
            historical_row[14:16], ["卖点资料不足", "卖点资料不足"]
        )
        with (
            patch.object(api_module, "render_summary_png", return_value=False),
            patch.object(
                api_module,
                "_report_export_context",
                side_effect=AssertionError("image download must not read workbook context"),
            ),
            patch.object(
                api_module,
                "build_report_detail_workbook",
                side_effect=AssertionError("image download must not generate a workbook"),
            ),
            patch.object(
                api_module,
                "build_report_download_bundle",
                side_effect=AssertionError("single file download must not create a ZIP"),
            ),
        ):
            single_image = self.client.get(
                f"/api/v8/tasks/{task_id}/revisions/1/download?format=image"
            )
        self.assertEqual(single_image.status_code, 200)
        self.assertEqual(single_image.headers["content-type"], "image/svg+xml")
        self.assertEqual(single_image.content, derived_svg)
        image_name = f'{value["name"]}_{task_id}_v1_图片报告.svg'
        self.assertEqual(
            single_image.headers["content-disposition"],
            f'attachment; filename="report.svg"; filename*=UTF-8\'\'{quote(image_name, safe="")}',
        )

        with (
            patch.object(
                api_module,
                "_report_download_image",
                side_effect=AssertionError("workbook download must not render an image"),
            ),
            patch.object(
                api_module,
                "build_report_download_bundle",
                side_effect=AssertionError("single file download must not create a ZIP"),
            ),
        ):
            single_workbook = self.client.get(
                f"/api/v8/tasks/{task_id}/revisions/1/download?format=xlsx"
            )
        self.assertEqual(single_workbook.status_code, 200)
        self.assertEqual(
            single_workbook.headers["content-type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertEqual(single_workbook.content, workbook)
        workbook_name = f'{value["name"]}_{task_id}_v1_数据明细.xlsx'
        self.assertEqual(
            single_workbook.headers["content-disposition"],
            f'attachment; filename="report.xlsx"; filename*=UTF-8\'\'{quote(workbook_name, safe="")}',
        )

        # Both direct formats remain available on the read-only replica and do
        # not write any derived file or new report revision back to its database.
        with connect(self.db) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        readonly_database_hash = hashlib.sha256(self.db.read_bytes()).hexdigest()
        replica = api_module.create_app(replace(self.config, read_only=True))
        with (
            TestClient(replica) as replica_client,
            patch.object(api_module, "render_summary_png", return_value=False),
        ):
            for export_format, expected in (
                ("image", single_image),
                ("xlsx", single_workbook),
            ):
                with self.subTest(readonly_format=export_format):
                    response = replica_client.get(
                        f"/api/v8/tasks/{task_id}/revisions/1/download?format={export_format}"
                    )
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.content, expected.content)
                    self.assertEqual(response.headers["cache-control"], "private, no-store")
                    self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(
            hashlib.sha256(self.db.read_bytes()).hexdigest(), readonly_database_hash
        )

        with connect(self.db) as connection:
            after_download = {
                "task": tuple(
                    connection.execute(
                        "SELECT task_status,progress,message FROM report_tasks WHERE id=?",
                        (task_id,),
                    ).fetchone()
                ),
                "revision": tuple(
                    connection.execute(
                        "SELECT report_sha256,invalidated_at FROM report_revisions WHERE task_id=? AND revision=1",
                        (task_id,),
                    ).fetchone()
                ),
                "file_count": connection.execute(
                    "SELECT COUNT(*) FROM report_files WHERE task_id=? AND revision=1",
                    (task_id,),
                ).fetchone()[0],
                "database": "\n".join(connection.iterdump()),
                "file_hashes": {
                    str(row["local_path"]): hashlib.sha256(
                        (PROJECT_ROOT / str(row["local_path"])).read_bytes()
                    ).hexdigest()
                    for row in connection.execute(
                        "SELECT local_path FROM report_files WHERE task_id=? AND revision=1 AND status='available'",
                        (task_id,),
                    ).fetchall()
                },
            }
        self.assertEqual(after_download, before_download)
        for export_format in ("zip", "image", "xlsx"):
            with self.subTest(missing_format=export_format):
                missing = self.client.get(
                    f"/api/v8/tasks/{task_id}/revisions/999/download?format={export_format}"
                )
                self.assertEqual(missing.status_code, 404)
        unsupported = self.client.get(
            f"/api/v8/tasks/{task_id}/revisions/1/download?format=pdf"
        )
        self.assertEqual(unsupported.status_code, 422)

    def test_single_report_downloads_isolate_artifacts_and_preserve_integrity_checks(
        self,
    ) -> None:
        self._activate_current_report_release()
        tasks = []
        for _ in range(2):
            created = self.client.post(
                "/api/v8/tasks",
                json={"period_start": "2026-07-01", "period_end": "2026-07-01"},
            )
            self.assertEqual(created.status_code, 200)
            tasks.append(created.json())
        self.assertEqual(tasks[0]["name"], tasks[1]["name"])
        self.assertNotEqual(tasks[0]["id"], tasks[1]["id"])
        workbook_names = []
        for task in tasks:
            response = self.client.get(
                f'/api/v8/tasks/{task["id"]}/revisions/1/download?format=xlsx'
            )
            self.assertEqual(response.status_code, 200)
            filename = f'{task["name"]}_{task["id"]}_v1_数据明细.xlsx'
            self.assertIn(
                quote(filename, safe=""), response.headers["content-disposition"]
            )
            workbook_names.append(response.headers["content-disposition"])
        self.assertNotEqual(workbook_names[0], workbook_names[1])

        task_id = tasks[0]["id"]
        download_url = f"/api/v8/tasks/{task_id}/revisions/1/download"
        with connect(self.db) as connection:
            paths = {
                row["file_kind"]: PROJECT_ROOT / str(row["local_path"])
                for row in connection.execute(
                    "SELECT file_kind,local_path FROM report_files WHERE task_id=? AND revision=1 AND status='available'",
                    (task_id,),
                ).fetchall()
            }

        png_payload = bytes.fromhex(
            "89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c489"
            "0000000b49444154789c636000020000050001a5f645400000000049454e44ae426082"
        )

        def render_png_fixture(_svg_path: Path, png_path: Path) -> bool:
            png_path.write_bytes(png_payload)
            return True

        with patch.object(api_module, "render_summary_png", side_effect=render_png_fixture):
            png_response = self.client.get(f"{download_url}?format=image")
        self.assertEqual(png_response.status_code, 200)
        self.assertEqual(png_response.headers["content-type"], "image/png")
        self.assertEqual(png_response.content, png_payload)
        self.assertIn(
            quote(f"_{task_id}_v1_图片报告.png", safe=""),
            png_response.headers["content-disposition"],
        )

        # Missing or damaged artifacts only block the format that consumes them.
        # ZIP still requires both sides, retaining its original integrity rules.
        for file_kind, dependent_format, independent_format in (
            ("content-csv", "xlsx", "image"),
            ("channel-csv", "xlsx", "image"),
            ("report-json", "image", "xlsx"),
        ):
            path = paths[file_kind]
            original = path.read_bytes()
            try:
                for state, expected_status in (("damaged", 409), ("missing", 410)):
                    with self.subTest(file_kind=file_kind, state=state):
                        if state == "damaged":
                            path.write_bytes(bytes([original[0] ^ 1]) + original[1:])
                        else:
                            path.unlink()
                        with patch.object(api_module, "render_summary_png", return_value=False):
                            independent = self.client.get(
                                f"{download_url}?format={independent_format}"
                            )
                        self.assertEqual(independent.status_code, 200)
                        dependent = self.client.get(
                            f"{download_url}?format={dependent_format}"
                        )
                        self.assertEqual(dependent.status_code, expected_status)
                        bundle = self.client.get(download_url)
                        self.assertEqual(bundle.status_code, expected_status)
            finally:
                path.write_bytes(original)

        with (
            patch.object(api_module, "render_summary_svg", side_effect=ValueError("legacy report")),
            patch.object(api_module, "render_summary_png", return_value=False),
        ):
            archived = self.client.get(f"{download_url}?format=image")
        self.assertEqual(archived.status_code, 200)
        self.assertEqual(archived.headers["content-type"], "image/svg+xml")
        self.assertEqual(archived.content, paths["summary-svg"].read_bytes())

        with connect(self.db) as connection:
            connection.execute(
                "DELETE FROM report_files WHERE task_id=? AND revision=1 AND file_kind IN ('report-json','summary-svg','summary-png')",
                (task_id,),
            )
            connection.commit()
        self.assertEqual(
            self.client.get(f"{download_url}?format=image").status_code, 404
        )
        self.assertEqual(
            self.client.get(f"{download_url}?format=xlsx").status_code, 200
        )

    def test_report_export_context_recovers_url_id_and_rejects_mismatch(
        self,
    ) -> None:
        task = create_task(
            task_type="custom",
            period_start="2026-07-01",
            period_end="2026-07-01",
            creation_source="manual",
            db_path=self.db,
        )
        canonical_url = "https://www.kuaishou.com/short-video/url-only-123"
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE content_items
                SET platform='kuaishou',platform_content_id=NULL,
                    canonical_url=?,content_type='video'
                WHERE id=?
                """,
                (canonical_url, self.content_id),
            )
            connection.execute(
                """
                INSERT INTO task_contents(
                    task_id,content_id,inclusion_status,reason
                ) VALUES (?,?,'included','fixture')
                """,
                (task["id"], self.content_id),
            )
            connection.commit()
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(
            buffer,
            fieldnames=(
                "content_id",
                "link_id",
                "platform",
                "platform_content_id",
                "canonical_url",
                "content_type",
                "primary_selling_point_code",
                "primary_selling_point_label",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "content_id": self.content_id,
                "link_id": "A2BC3D",
                "platform": "kuaishou",
                "platform_content_id": "",
                "canonical_url": canonical_url,
                "content_type": "video",
                "primary_selling_point_code": "",
                "primary_selling_point_label": "",
            }
        )
        content_csv = ("\ufeff" + buffer.getvalue()).encode("utf-8")
        request = SimpleNamespace(app=self.app)

        enrichment, _ = api_module._report_export_context(
            request,
            task_id=task["id"],
            taxonomy_version="selling-points-v5.0",
            content_csv=content_csv,
        )
        self.assertEqual(
            enrichment[str(self.content_id)]["platform_content_id"],
            "url-only-123",
        )

        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET platform_content_id='different' WHERE id=?",
                (self.content_id,),
            )
            connection.commit()
        with self.assertRaises(api_module.HTTPException) as caught:
            api_module._report_export_context(
                request,
                task_id=task["id"],
                taxonomy_version="selling-points-v5.0",
                content_csv=content_csv,
            )
        self.assertEqual(caught.exception.status_code, 409)

    def test_custom_task_rejects_an_unclosed_period_without_persisting_task(
        self,
    ) -> None:
        self._activate_current_report_release()
        with connect(self.db) as connection:
            before = connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[
                0
            ]
        rejected = self.client.post(
            "/api/v8/tasks",
            json={"period_start": "2099-01-01", "period_end": "2099-01-01"},
        )
        self.assertEqual(rejected.status_code, 409)
        self.assertEqual(
            rejected.json()["detail"],
            "结束日期只能选昨天或更早。",
        )
        with connect(self.db) as connection:
            after = connection.execute("SELECT COUNT(*) FROM report_tasks").fetchone()[
                0
            ]
        self.assertEqual(after, before)

    def test_writer_acceptance_schedules_same_profile_roster_in_transaction(
        self,
    ) -> None:
        upsert_account(
            {
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "9876543212345678900",
                        "nickname": "名单成员",
                    }
                ]
            },
            db_path=self.db,
        )
        source = json.dumps(
            {
                "members": [
                    {
                        "platform": "douyin",
                        "matrix_account_id": "matrix-writer-schedule",
                        "uid": "9876543212345678900",
                        "profile_ref": "https://www.douyin.com/user/MS4w.writer-schedule",
                        "nickname": "名单成员",
                    }
                ]
            },
            ensure_ascii=False,
        ).encode()

        def schedule(connection, **kwargs):
            self.assertTrue(connection.in_transaction)
            self.assertGreater(kwargs["roster_snapshot_id"], 0)
            return {
                "scheduled": True,
                "activation": {
                    "activation_id": 99,
                    "effective_at": "2026-09-02T16:00:00.000000Z",
                },
            }

        self.app.state.writer_lock_held = True
        try:
            with patch.object(
                api_module,
                "schedule_accepted_roster_activation_in_transaction",
                side_effect=schedule,
            ) as scheduled:
                response = self.client.post(
                    "/api/v8/account-roster/import",
                    json={
                        "source_name": "matrix-full.json",
                        "content_base64": base64.b64encode(source).decode(),
                        "organization": "测试矩阵组织",
                        "source_exported_at": (
                            datetime.now(timezone.utc) - timedelta(hours=1)
                        ).isoformat(),
                        "source_instance_id": "writer-schedule-export",
                        "declared_count": 1,
                        "evidence_note": "当前组织全部平台和全部已添加账号的完整导出。",
                    },
                )
        finally:
            self.app.state.writer_lock_held = False

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["activation_status"], "scheduled")
        self.assertEqual(response.json()["scheduled_activation_id"], 99)
        self.assertEqual(scheduled.call_count, 1)


    @patch("v8.account_roster.now_utc", return_value="2026-09-30T00:00:00Z")
    def test_roster_activation_and_sync_do_not_hide_saved_account_directory(
        self, _runtime_clock: MagicMock
    ) -> None:
        member_account = upsert_account({
            "phone": "13800138000", "operator_name": "保留运营人",
            "platforms": [{"platform": "douyin", "uid": "9876543212345678900", "nickname": "历史昵称"}],
        }, db_path=self.db)
        upsert_account({
            "phone": "13800138000",
            "platforms": [{"platform": "xiaohongshu", "uid": "5c668b3e0000000012021605"}],
        }, db_path=self.db)
        def active_roster():
            with connect(self.db) as connection:
                return runtime_account_summary(connection)

        before = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(before["total"], 2)
        self.assertEqual(before["account_management_version"], 2)
        self.assertNotIn("archive_total", before)
        self.assertFalse(active_roster()["ready"])
        members = [{
            "platform": "douyin", "matrix_account_id": "matrix-official-1",
            "uid": "9876543212345678900", "profile_ref": "https://www.douyin.com/user/MS4w.member1",
            "nickname": "当前成员", "monitoring_status": "monitored", "authorization_status": "unknown",
            "avatar_url": "https://example.invalid/avatar.jpg", "unique_id": "member-short-id",
        }]
        exported = datetime.now(timezone.utc) - timedelta(hours=2)

        def upload_rows(rows, sequence, declared_count=None):
            source = json.dumps({"members": rows, "export_sequence": sequence}, ensure_ascii=False).encode()
            return self.client.post("/api/v8/account-roster/import", json={
                "source_name": "matrix-full.json", "content_base64": base64.b64encode(source).decode(),
                "organization": "测试矩阵组织", "source_exported_at": (exported + timedelta(minutes=15 * sequence)).isoformat(),
                "source_instance_id": f"official-export-{sequence}",
                "declared_count": len(rows) if declared_count is None else declared_count,
                "evidence_note": "测试中的官方导出记录，覆盖本组织全部平台和全部已添加账号。",
            })

        def activate_snapshot(snapshot_id: int) -> None:
            activation_time = datetime.now(timezone.utc).isoformat()
            with connect(self.db) as connection:
                snapshot = connection.execute(
                    "SELECT members_sha256 FROM account_roster_snapshots WHERE id=?",
                    (snapshot_id,),
                ).fetchone()
                assert snapshot is not None
                append_activation(
                    connection,
                    profile_id=MATRIX_PROFILE,
                    roster_snapshot_id=snapshot_id,
                    roster_members_sha256=snapshot["members_sha256"],
                    effective_at=activation_time,
                    build_receipt_sha256="b" * 64,
                    actor="test",
                    reason="activate accepted API roster",
                    created_at=activation_time,
                )

        initial_zero = upload_rows([], 0)
        self.assertEqual(initial_zero.status_code, 409)
        self.assertFalse(self.client.get("/api/v8/account-roster").json()["ready"])
        wrong_total = upload_rows(members, 0, 2)
        self.assertEqual(wrong_total.status_code, 409)
        self.assertFalse(self.client.get("/api/v8/account-roster").json()["ready"])
        accepted = upload_rows(members, 1)
        self.assertEqual(accepted.status_code, 200, accepted.text)
        self.assertEqual(accepted.json()["status"], "accepted")
        pending = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(pending["total"], 2)
        self.assertEqual(
            active_roster()["pending_snapshot_id"],
            accepted.json()["snapshot_id"],
        )
        activate_snapshot(accepted.json()["snapshot_id"])
        current = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(current["total"], 2)
        self.assertEqual(active_roster()["current_count"], 1)
        prepared_system = self.client.post(
            "/api/v8/account-roster/system/bootstrap",
            json={"reason": "prepare managed profile rollback-safe roster"},
        )
        self.assertEqual(prepared_system.status_code, 200, prepared_system.text)
        self.assertEqual(prepared_system.json()["activation_status"], "pending_activation")
        self.assertEqual(
            active_roster()["active_profile_id"],
            MATRIX_PROFILE,
        )
        self.assertNotIn("archive_total", current)
        item = next(row for row in current["items"] if row["id"] == member_account["id"])
        self.assertEqual(item["id"], member_account["id"])
        self.assertEqual(item["operator_name"], "保留运营人")
        self.assertNotIn("roster_state", item)
        self.assertEqual(item["account_status"], "unmarked")
        identity = item["platforms"][0]
        self.assertEqual(identity["uid"], "9876543212345678900")
        self.assertEqual(identity["nickname"], "当前成员")
        with connect(self.db) as connection:
            self.assertEqual(connection.execute(
                "SELECT nickname FROM account_platform_identities WHERE account_id=?",
                (member_account["id"],),
            ).fetchone()[0], "历史昵称")
        self.assertEqual(identity["matrix_account_id"], "matrix-official-1")
        self.assertEqual(identity["unique_id"], "member-short-id")
        self.assertEqual(identity["monitoring_status"], "monitored")
        self.assertEqual(identity["authorization_status"], "unknown")
        self.assertIsNone(identity["follower_count"])
        self.assertIsNone(identity["platform_work_count"])
        self.assertEqual(self.client.post("/api/v8/accounts/search", json={"query": "member-short-id"}).json()["total"], 1)
        self.assertEqual(self.client.post("/api/v8/accounts/search", json={"query": "当前成员"}).json()["total"], 1)
        saved_ids = {row["id"] for row in current["items"]}
        for legacy_scope in ("current", "history", "unresolved", "all"):
            with self.subTest(legacy_scope=legacy_scope):
                compatible = self.client.post(
                    "/api/v8/accounts/search", json={"scope": legacy_scope}
                ).json()
                self.assertEqual(compatible["total"], 2)
                self.assertEqual({row["id"] for row in compatible["items"]}, saved_ids)
        unresolved = {
            "platform": "douyin", "matrix_account_id": "matrix-official-unresolved", "uid": None,
            "profile_ref": "https://www.douyin.com/user/MS4w.unresolved", "nickname": "待对齐成员",
        }
        added = upload_rows([*members, unresolved], 2)
        self.assertEqual(added.status_code, 200, added.text)
        self.assertEqual(
            self.client.post(
                "/api/v8/accounts/search", json={}
            ).json()["total"],
            3,
        )
        activate_snapshot(added.json()["snapshot_id"])
        pending = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(pending["total"], 3)
        unresolved_items = [row for row in pending["items"] if row["platforms"][0]["uid"] is None]
        self.assertEqual(len(unresolved_items), 1)
        self.assertNotIn("roster_state", unresolved_items[0])
        self.assertEqual(active_roster()["unresolved_count"], 1)
        self.assertEqual(self.client.post("/api/v8/accounts/search", json={}).json()["total"], 3)
        with connect(self.db) as connection:
            counts = tuple(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                           for table in ("accounts", "account_roster_snapshots", "provider_raw_responses", "provider_usage"))
        synced = self.client.post("/api/v8/account-roster/sync", json={})
        self.assertEqual(synced.status_code, 200)
        self.assertEqual(synced.json()["status"], "manual_export_required")
        with connect(self.db) as connection:
            after_counts = tuple(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                                 for table in ("accounts", "account_roster_snapshots", "provider_raw_responses", "provider_usage"))
        self.assertEqual(after_counts, counts)
        diff = self.client.get(f"/api/v8/account-roster/candidates/{accepted.json()['candidate_id']}")
        self.assertEqual(diff.status_code, 200)
        self.assertEqual(diff.json()["status"], "accepted")
        with TestClient(api_module.create_app(replace(self.config, read_only=True))) as replica:
            self.assertEqual(replica.get("/api/v8/account-roster").status_code, 200)
            self.assertEqual(replica.post("/api/v8/account-roster/sync", json={}).status_code, 403)
            self.assertEqual(replica.post("/api/v8/account-roster/import", json={}).status_code, 403)
        self.assertEqual(self.client.get("/api/v8/account-roster/candidates/999999").status_code, 409)
        first_zero = upload_rows([], 3)
        self.assertEqual(first_zero.status_code, 200, first_zero.text)
        self.assertEqual(first_zero.json()["status"], "pending_removal_confirmation")
        self.assertEqual(self.client.post("/api/v8/accounts/search", json={}).json()["total"], 3)
        confirmed_zero = upload_rows([], 4)
        self.assertEqual(confirmed_zero.status_code, 200, confirmed_zero.text)
        self.assertEqual(confirmed_zero.json()["status"], "accepted")
        self.assertEqual(self.client.post("/api/v8/accounts/search", json={}).json()["total"], 3)
        activate_snapshot(confirmed_zero.json()["snapshot_id"])
        empty_current = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(empty_current["total"], counts[0])
        self.assertNotIn("archive_total", empty_current)
        self.assertTrue(active_roster()["ready"])
        self.assertEqual(active_roster()["current_count"], 0)

    def test_accounts_allow_missing_or_shared_phone_with_id_only_updates(self) -> None:
        first = upsert_account(
            {"operator_name": "运营甲", "platforms": [
                {"platform": "douyin", "uid": "phone-optional-a", "nickname": "无手机号甲"}
            ]},
            db_path=self.db,
        )
        second = upsert_account(
            {"phone": None, "platforms": [
                {"platform": "douyin", "uid": "phone-optional-b", "nickname": "无手机号乙"}
            ]},
            db_path=self.db,
        )
        self.assertNotEqual(first["id"], second["id"])
        searched = self.client.post(
            "/api/v8/accounts/search", json={"query": "phone-optional"}
        )
        self.assertEqual(searched.json()["total"], 2)
        self.assertEqual([item["phone"] for item in searched.json()["items"]], ["", ""])
        for account in (first, second):
            updated = self.client.patch(
                f"/api/v8/accounts/{account['id']}", json={"phone": "13800138009"}
            )
            self.assertEqual(updated.status_code, 200)
            self.assertEqual(updated.json()["id"], account["id"])
        denied = self.client.patch(
            f"/api/v8/accounts/{first['id']}",
            json={"platforms": [{"platform": "douyin", "uid": "replacement"}]},
        )
        self.assertEqual(denied.status_code, 422)
        searched = self.client.post(
            "/api/v8/accounts/search", json={"query": "phone-optional-a"}
        )
        self.assertEqual(searched.json()["total"], 1)
        item = searched.json()["items"][0]
        self.assertEqual(item["phone"], "13800138009")
        self.assertEqual(item["operator_name"], "运营甲")
        self.assertEqual(item["platforms"][0]["uid"], "phone-optional-a")
        self.assertEqual(item["platforms"][0]["nickname"], "无手机号甲")
        self.assertEqual(len(item["platforms"]), 1)
        self.assertIsNone(item["platforms"][0]["platform_work_count"])
        for route, payload, expected_status in (
            ("/api/v8/accounts", {"phone": "13800138010"}, 422),
            ("/api/v8/accounts/import", {"source_name": "old.csv", "rows": []}, 410),
        ):
            with self.subTest(route=route):
                self.assertEqual(self.client.post(route, json=payload).status_code, expected_status)

    @patch("v8.account_roster.now_utc", return_value="2026-09-30T00:00:00Z")
    def test_managed_profile_account_membership_is_immutable_and_activation_bound(
        self, _runtime_clock: MagicMock
    ) -> None:
        _runtime_clock.return_value = datetime.now(timezone.utc).isoformat()
        initial_seal_at = (
            datetime.now(timezone.utc) - timedelta(seconds=10)
        ).isoformat()
        with connect(self.db) as connection:
            accepted = seal_system_members(
                connection,
                [
                    {
                        "platform": "douyin",
                        "uid": "123456789",
                        "nickname": "首个系统账号",
                    }
                ],
                raw_root=self.db.parent / "roster_sources",
                actor="test",
                reason="initialize managed roster",
                sealed_at=initial_seal_at,
            )
            snapshot = connection.execute(
                "SELECT members_sha256 FROM account_roster_snapshots WHERE id=?",
                (accepted["snapshot_id"],),
            ).fetchone()
            assert snapshot is not None
            first_at = (
                datetime.now(timezone.utc) - timedelta(seconds=9)
            ).isoformat(timespec="seconds")
            first_activation = append_activation(
                connection,
                profile_id=TIKHUB_PROFILE,
                roster_snapshot_id=accepted["snapshot_id"],
                roster_members_sha256=snapshot["members_sha256"],
                effective_at=first_at,
                build_receipt_sha256="c" * 64,
                actor="test",
                reason="activate managed roster",
                created_at=first_at,
            )

        before = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(before["total"], 1)
        self.assertEqual(before["roster"]["active_profile_id"], TIKHUB_PROFILE)
        profile_url = "https://www.xiaohongshu.com/user/profile/5c668b3e0000000012021605"
        payload = {
            "profile_url": profile_url, "phone": "", "operator_name": "",
            "account_status": "daily", "request_id": str(uuid4()),
        }
        protected_tables = (
            "accounts", "account_platform_identities", "account_roster_snapshots",
            "account_state_events", "scheduler_runs", "scheduler_run_attempts",
            "acquisition_profile_activations", "pipeline_paid_drain_events",
        )
        with connect(self.db) as connection:
            before_rows = {
                table: [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")]
                for table in protected_tables
            }
        self.app.state.writer_lock_held = True
        with patch.object(api_module, "_resolve_account_creation_profile", return_value={
            "platform": "xiaohongshu", "uid": "5c668b3e0000000012021605",
            "nickname": "新增系统账号", "profile_ref": profile_url,
        }):
            # An active profile without its real RELEASE permit cannot admit
            # the new member; account, roster, status and receipt writes roll back.
            rejected = self.client.post("/api/v8/accounts", json=payload)
            self.assertEqual(rejected.status_code, 409, rejected.text)
            self.assertEqual(rejected.headers["X-DCAR-Roster-Error"], "profile_control_dispatch_not_open")
            with connect(self.db) as connection:
                for table, rows in before_rows.items():
                    self.assertEqual(
                        [tuple(row) for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")], rows,
                        table,
                    )
                with transaction(connection):
                    issue_activation_permit_in_transaction(
                        connection, activation_id=first_activation["activation_id"],
                        drain_id="managed-account-fixture:initial",
                        source_activation_id=first_activation["activation_id"],
                        business_day=first_at[:10], planned_effective_at=first_activation["effective_at"],
                        build_receipt_sha256="c" * 64, runtime_root_receipt_sha256="d" * 64,
                        now=first_activation["effective_at"],
                    )
            created = self.client.post("/api/v8/accounts", json=payload)
        self.app.state.writer_lock_held = False
        self.assertEqual(created.status_code, 200, created.text)
        self.assertEqual(created.json()["activation_status"], "scheduled")
        self.assertEqual(created.json()["account_status"], "daily")
        directory = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(directory["total"], 2)
        self.assertEqual(directory["roster"]["current_count"], 1)
        with connect(self.db) as connection:
            scheduled = connection.execute(
                "SELECT * FROM acquisition_profile_activations WHERE id=?",
                (created.json()["scheduled_activation_id"],),
            ).fetchone()
            assert scheduled is not None
            self.assertEqual(scheduled["roster_snapshot_id"], created.json()["snapshot_id"])
            self.assertEqual(scheduled["effective_at"], created.json()["scheduled_effective_at"])
            self.assertEqual(connection.execute(
                "SELECT COUNT(*) FROM pipeline_paid_drain_events WHERE target_activation_id=? AND event_type='release'",
                (scheduled["id"],),
            ).fetchone()[0], 1)
        _runtime_clock.return_value = created.json()["scheduled_effective_at"]
        active = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(active["total"], 2)
        added_account_id = created.json()["account_id"]
        changed = self.client.patch(
            f"/api/v8/accounts/{added_account_id}", json={"account_status": "paused"}
        )
        self.assertEqual(changed.status_code, 200, changed.text)
        directory = self.client.post("/api/v8/accounts/search", json={}).json()
        self.assertEqual(directory["total"], 2)
        paused = next(row for row in directory["items"] if row["id"] == added_account_id)
        self.assertEqual(paused["account_status"], "paused")
        self.assertNotIn("roster_state", paused)
        with connect(self.db) as connection:
            event = connection.execute(
                "SELECT activation_id,new_enabled FROM account_state_events ORDER BY id DESC LIMIT 1"
            ).fetchone()
            assert event is not None
            self.assertEqual(event["activation_id"], first_activation["activation_id"] + 1)
            self.assertEqual(event["new_enabled"], 0)

        with connect(self.db) as connection:
            obsolete_before = {
                row[0]: sorted((tuple(value) for value in connection.execute(
                    'SELECT * FROM "' + row[0].replace('"', '""') + '"'
                )), key=repr)
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
        imported = self.client.post(
            "/api/v8/accounts/import",
            json={
                "source_name": "managed.json",
                "rows": [
                    {"platform": "douyin", "uid": "987654321", "nickname": "批量新增"},
                    {"platform": "invalid", "uid": "bad"},
                ],
            },
        )
        self.assertEqual(imported.status_code, 410, imported.text)
        self.assertIn("主页", imported.json()["detail"])
        removed = self.client.delete(f"/api/v8/accounts/{added_account_id}")
        self.assertEqual(removed.status_code, 410, removed.text)
        self.assertIn("暂停", removed.json()["detail"])
        with connect(self.db) as connection:
            for table, rows in obsolete_before.items():
                self.assertEqual(sorted((tuple(value) for value in connection.execute(
                    'SELECT * FROM "' + table.replace('"', '""') + '"'
                )), key=repr), rows, table)

    def test_account_id_update_and_export_keep_full_phone_with_post_search(
        self,
    ) -> None:
        created = upsert_account(
            {
                "phone": "13800138000",
                "operator_name": "运营甲",
                "account_type": "original",
                "content_direction": "new_car",
                "platforms": [
                    {
                        "platform": "douyin",
                        "uid": "123456789",
                        "nickname": "账号甲",
                        "real_name_status": "yes",
                    }
                ],
            },
            db_path=self.db,
        )
        account_id = created["id"]
        updated = self.client.patch(
            f"/api/v8/accounts/{account_id}",
            json={
                "phone": "+86 138-0013-8000",
                "operator_name": "运营乙",
                "account_type": "boutique_ip",
                "content_direction": "media",
            },
        )
        self.assertEqual(updated.status_code, 200)
        linked_content = self.client.post(
            "/api/v8/contents",
            json={
                "platform": "douyin",
                "canonical_url": "https://www.douyin.com/video/1234567890000000001",
                "published_at": "2026-07-03T08:00:00+08:00",
                "title": "账号关联内容",
                "body": "用于验证账号列表的平台关联内容量",
                "content_type": "video",
                "account_uid": "123456789",
                "account_name": "账号乙",
                "account_type": "boutique_ip",
                "content_direction": "media",
            },
        )
        self.assertEqual(linked_content.status_code, 200)
        searched = self.client.post(
            "/api/v8/accounts/search", json={"query": "13800138000"}
        )
        self.assertEqual(searched.json()["total"], 1)
        self.assertEqual(searched.json()["items"][0]["phone"], "+86 138-0013-8000")
        identity = searched.json()["items"][0]["platforms"][0]
        self.assertIsNone(identity["follower_count"])
        self.assertEqual(identity["content_count"], 1)
        exported = self.client.post(
            "/api/v8/accounts/export",
            json={
                "douyin_authorization_targets": [
                    {
                        "account_id": account_id,
                        "platform_uid": "123456789",
                        "state": "authorized",
                    }
                ]
            },
            headers={"Origin": "http://127.0.0.1:4174"},
        )
        self.assertEqual(exported.status_code, 200)
        self.assertEqual(
            exported.headers["content-type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertEqual(exported.headers["cache-control"], "private, no-store")
        self.assertEqual(exported.headers["x-content-type-options"], "nosniff")
        self.assertEqual(
            exported.headers["access-control-expose-headers"], "Content-Disposition"
        )
        disposition = exported.headers["content-disposition"]
        self.assertIn('filename="dcar-accounts.xlsx"', disposition)
        self.assertRegex(
            disposition,
            r"filename\*=UTF-8''%E8%B4%A6%E5%8F%B7%E8%A1%A8%E6%A0%BC_\d{8}_\d{4}\.xlsx",
        )
        with zipfile.ZipFile(io.BytesIO(exported.content)) as archive:
            sheet = archive.read("xl/worksheets/sheet1.xml")
        values = _xlsx_sheet_values(sheet)
        flattened = [value for row in values for value in row]
        self.assertIn("手机号", flattened)
        self.assertIn("抖音开平授权", flattened)
        self.assertIn("+86 138-0013-8000", flattened)
        self.assertIn("已授权", flattened)

        needs_reauthorization = self.client.post(
            "/api/v8/accounts/export",
            json={
                "douyin_authorization_targets": [
                    {
                        "account_id": account_id,
                        "platform_uid": "123456789",
                        "state": "needs_reauthorization",
                    }
                ]
            },
        )
        self.assertEqual(needs_reauthorization.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(needs_reauthorization.content)) as archive:
            needs_values = _xlsx_sheet_values(
                archive.read("xl/worksheets/sheet1.xml")
            )
        self.assertIn(
            "需重新授权", [value for row in needs_values for value in row]
        )

        degraded = self.client.post("/api/v8/accounts/export", json={})
        self.assertEqual(degraded.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(degraded.content)) as archive:
            degraded_values = _xlsx_sheet_values(
                archive.read("xl/worksheets/sheet1.xml")
            )
        self.assertIn(
            "状态异常", [value for row in degraded_values for value in row]
        )

        legacy = self.client.post(
            "/api/v8/accounts/export",
            json={"douyin_authorized_account_ids": [account_id]},
        )
        self.assertEqual(legacy.status_code, 422)

        invalid_targets = [
            {
                "account_id": 0,
                "platform_uid": "123456789",
                "state": "authorized",
            },
            {
                "account_id": account_id,
                "platform_uid": "12345",
                "state": "authorized",
            },
            {
                "account_id": account_id,
                "platform_uid": "12345x",
                "state": "authorized",
            },
            {
                "account_id": account_id,
                "platform_uid": "123456789",
                "state": "expired",
            },
            {"account_id": account_id, "platform_uid": "123456789"},
            {
                "account_id": account_id,
                "platform_uid": "123456789",
                "state": "authorized",
                "unexpected": True,
            },
        ]
        for target in invalid_targets:
            with self.subTest(target=target):
                response = self.client.post(
                    "/api/v8/accounts/export",
                    json={"douyin_authorization_targets": [target]},
                )
                self.assertEqual(response.status_code, 422)

    def test_partial_content_patch_preserves_omitted_and_effective_fields(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            account = connection.execute(
                """
                INSERT INTO accounts(
                    phone,phone_normalized,operator_name,account_type,
                    content_direction,created_at,updated_at
                ) VALUES ('13900139000','13900139000','PATCH fixture','original',
                          'new_car',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO account_platform_identities(
                    account_id,platform,uid,nickname,created_at,updated_at
                ) VALUES (?,'douyin','patch-api-uid','PATCH API',?,?)
                """,
                (account.lastrowid, captured_at, captured_at),
            )
            connection.execute(
                """
                UPDATE content_items
                SET account_id=?,raw_account_uid='patch-api-uid',
                    raw_account_name='PATCH API',legacy_account_type=NULL,
                    manual_content_direction=NULL
                WHERE id=?
                """,
                (account.lastrowid, self.content_id),
            )
            before = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (self.content_id,)
                ).fetchone()
            )
            connection.commit()

        searched = self.client.post(
            "/api/v8/contents/search",
            json={"query": "A2BC3D", "page_size": 10},
        )
        self.assertEqual(searched.status_code, 200)
        item = searched.json()["items"][0]
        self.assertEqual(item["account_type"], "original")
        self.assertEqual(item["content_direction"], "new_car")

        updated = self.client.patch(
            f"/api/v8/contents/{self.content_id}", json={"title": "仅修改标题"}
        )
        self.assertEqual(updated.status_code, 200)
        with connect(self.db) as connection:
            after = dict(
                connection.execute(
                    "SELECT * FROM content_items WHERE id=?", (self.content_id,)
                ).fetchone()
            )
        self.assertEqual(after["title"], "仅修改标题")
        self.assertIsNone(after["legacy_account_type"])
        self.assertIsNone(after["manual_content_direction"])
        for field in (
            "platform",
            "platform_content_id",
            "canonical_url",
            "published_at",
            "body",
            "content_type",
            "raw_account_uid",
            "raw_account_name",
        ):
            self.assertEqual(after[field], before[field], field)

        self.assertEqual(
            self.client.patch(
                f"/api/v8/contents/{self.content_id}", json={}
            ).status_code,
            422,
        )
        self.assertEqual(
            self.client.patch(
                f"/api/v8/contents/{self.content_id}", json={"unknown_field": 1}
            ).status_code,
            422,
        )
        self.assertEqual(
            self.client.post(
                "/api/v8/contents", json={"title": "缺少身份"}
            ).status_code,
            422,
        )

        overridden = self.client.patch(
            f"/api/v8/contents/{self.content_id}",
            json={"content_direction": "media"},
        )
        self.assertEqual(overridden.status_code, 200)
        cleared = self.client.patch(
            f"/api/v8/contents/{self.content_id}",
            json={"content_direction": "unknown"},
        )
        self.assertEqual(cleared.status_code, 200)
        with connect(self.db) as connection:
            manual_direction = connection.execute(
                "SELECT manual_content_direction FROM content_items WHERE id=?",
                (self.content_id,),
            ).fetchone()[0]
        self.assertIsNone(manual_direction)

    def test_content_validate_import_search_and_export(self) -> None:
        invalid = self.client.post(
            "/api/v8/contents/validate",
            json={
                "source_name": "input.csv",
                "rows": [
                    {"platform": "douyin", "canonical_url": "https://v.douyin.com/abc"}
                ],
            },
        )
        self.assertEqual(invalid.status_code, 200)
        self.assertEqual(invalid.json()["rejected"], 1)
        imported = self.client.post(
            "/api/v8/contents/import",
            json={
                "source_name": "input.csv",
                "rows": [
                    {
                        "platform": "douyin",
                        "canonical_url": "https://www.douyin.com/video/999999999",
                        "title": '=HYPERLINK("https://example.invalid")',
                        "body": "导入的汽车内容完整正文",
                        "published_at": "2026-07-03T08:00:00+08:00",
                    }
                ],
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
        export_text = exported.content.decode("utf-8-sig")
        self.assertIn("999999999", export_text)
        self.assertIn("evaluation_freshness", export_text.splitlines()[0])
        exported_rows = list(csv.DictReader(io.StringIO(export_text, newline="")))
        injected_row = next(
            row for row in exported_rows if row["platform_content_id"] == "999999999"
        )
        self.assertEqual(
            injected_row["title"], "'=HYPERLINK(\"https://example.invalid\")"
        )

    def test_content_search_filters_by_selling_point(self) -> None:
        # v16 起没有人工改判入口：直接把当前评估设成 C1 命中来构造筛选样本
        with connect(self.db) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions
                SET primary_selling_point_code='C1',selling_point_score=92,
                    selling_point_included=1,content_direction='media',
                    evaluation_status='evaluated',evidence_level='V3',
                    content_automotive_score=95
                WHERE id=?
                """,
                (self.evaluation_id,),
            )
            connection.execute(
                """
                INSERT INTO evaluation_matches(
                    evaluation_id,selling_point_code,scene,match_role,score,evidence_json
                ) VALUES (?, 'C1', 'media', 'primary', 92, '{}')
                """,
                (self.evaluation_id,),
            )
            connection.commit()
        imported = self.client.post(
            "/api/v8/contents/import",
            json={
                "source_name": "input.csv",
                "rows": [
                    {
                        "platform": "douyin",
                        "canonical_url": "https://www.douyin.com/video/888888888",
                        "title": "尚未评估的内容",
                        "body": "尚未评估的内容完整正文",
                        "published_at": "2026-07-04T08:00:00+08:00",
                    }
                ],
            },
        )
        self.assertEqual(imported.status_code, 200)
        self.assertEqual(imported.json()["inserted_rows"], 1)

        matched = self.client.post(
            "/api/v8/contents/search",
            json={"selling_point": "C1", "page_size": 10},
        )
        self.assertEqual(matched.status_code, 200)
        self.assertEqual(matched.json()["total"], 1)
        self.assertEqual(matched.json()["items"][0]["link_id"], "A2BC3D")
        self.assertEqual(matched.json()["items"][0]["primary_selling_point_code"], "C1")

        missing = self.client.post(
            "/api/v8/contents/search",
            json={"selling_point": "__none__", "page_size": 10},
        )
        self.assertEqual(missing.status_code, 200)
        self.assertEqual(missing.json()["total"], 1)
        self.assertIsNone(missing.json()["items"][0]["primary_selling_point_code"])

        unmatched = self.client.post(
            "/api/v8/contents/search",
            json={"selling_point": "E9", "page_size": 10},
        )
        self.assertEqual(unmatched.status_code, 200)
        self.assertEqual(unmatched.json()["total"], 0)
        combined = self.client.post(
            "/api/v8/contents/search",
            json={"selling_point": "C1", "platform": "xiaohongshu", "page_size": 10},
        )
        self.assertEqual(combined.status_code, 200)
        self.assertEqual(combined.json()["total"], 0)

    def test_content_search_candidate_page_preserves_ties_pages_and_nulls(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.executemany(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,published_at,
                    title,body,content_type,imported_at,created_at,updated_at
                ) VALUES (?, 'douyin', ?, ?, ?, ?, '分页排序样本', 'video', ?, ?, ?)
                """,
                [
                    (
                        f"P{index:05d}",
                        f"page-{index}",
                        f"https://www.douyin.com/video/page-{index}",
                        None if index == 55 else "2099-01-01T00:00:00Z",
                        f"分页排序样本 {index}",
                        captured_at,
                        captured_at,
                        captured_at,
                    )
                    for index in range(56)
                ],
            )
            tied_ids = [
                int(row["id"])
                for row in connection.execute(
                    """
                    SELECT id FROM content_items
                    WHERE platform_content_id LIKE 'page-%' AND published_at IS NOT NULL
                    ORDER BY id DESC
                    """
                )
            ]
            connection.commit()

        first = self.client.post(
            "/api/v8/contents/search", json={"page": 1, "page_size": 50}
        ).json()
        second = self.client.post(
            "/api/v8/contents/search", json={"page": 2, "page_size": 50}
        ).json()

        self.assertEqual([item["id"] for item in first["items"]], tied_ids[:50])
        self.assertEqual(
            [item["id"] for item in second["items"][:5]], tied_ids[50:]
        )
        self.assertIsNone(second["items"][-1]["published_at"])

    def test_content_search_total_matches_rows_for_every_count_dependency(self) -> None:
        captured_at = now_utc()
        with connect(self.db) as connection:
            account = connection.execute(
                """
                INSERT INTO accounts(
                    phone,phone_normalized,operator_name,account_type,
                    content_direction,created_at,updated_at
                ) VALUES ('13700137000','13700137000','count fixture','original',
                          'new_car',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                "UPDATE content_items SET account_id=? WHERE id=?",
                (account.lastrowid, self.content_id),
            )
            connection.execute(
                """
                UPDATE evaluation_versions
                SET primary_selling_point_code='C1',selling_point_score=92,
                    selling_point_included=1,content_direction='media',
                    evaluation_status='evaluated',evidence_level='V3',
                    content_automotive_score=95
                WHERE id=?
                """,
                (self.evaluation_id,),
            )
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,published_at,
                    title,body,content_type,imported_at,created_at,updated_at
                ) VALUES (
                    'B2CD3E','xiaohongshu','2','https://www.xiaohongshu.com/explore/2',
                    '2026-07-02T04:00:00Z','其他内容','没有关联标签','image',?,?,?
                )
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO spu_catalog(
                    spu_id,brand,series,series_slug,trim_label,is_series_node,
                    powertrain,body_style,created_at,updated_at
                ) VALUES ('fixture-series','测试品牌','测试车系','fixture-series',
                          NULL,1,'ev','suv',?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO audience_dim(code,label,definition)
                VALUES ('P1','家庭用户','测试')
                """
            )
            connection.execute(
                """
                INSERT INTO scene_dim(code,label,definition)
                VALUES ('S1','城市通勤','测试')
                """
            )
            connection.execute(
                """
                INSERT INTO content_spu_links(
                    content_id,spu_id,resolved_level,is_primary,status,score,
                    evidence_json,rule_version,created_at
                ) VALUES (?,'fixture-series','series',1,'confirmed',90,'{}',
                          'count-fixture',?)
                """,
                (self.content_id, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_audience_links(
                    content_id,audience_code,source,conflict_flag,consistency_flag,
                    evidence_json,rule_version,created_at
                ) VALUES (?,'P1','content_explicit',0,1,'{}','count-fixture',?)
                """,
                (self.content_id, captured_at),
            )
            connection.execute(
                """
                INSERT INTO content_scene_links(
                    content_id,scene_code,score,evidence_json,rule_version,created_at
                ) VALUES (?,'S1',90,'{}','count-fixture',?)
                """,
                (self.content_id, captured_at),
            )
            connection.commit()

        cases = {
            "default": {},
            "query": {"query": "汽车保养"},
            "platform": {"platform": "douyin"},
            "account_type": {"account_type": "original"},
            "content_direction": {"content_direction": "media"},
            "selling_point": {"selling_point": "C1"},
            "selling_point_none": {"selling_point": "__none__"},
            "spu_series": {"spu_series": "fixture-series"},
            "spu_series_none": {"spu_series": "__none__"},
            "audience": {"audience": "P1"},
            "audience_none": {"audience": "__none__"},
            "scene": {"scene": "S1"},
            "scene_none": {"scene": "__none__"},
            "account_and_selling": {
                "account_type": "original",
                "selling_point": "C1",
                "platform": "douyin",
            },
            "direction_and_spu_dimensions": {
                "content_direction": "media",
                "spu_series": "fixture-series",
                "audience": "P1",
                "scene": "S1",
            },
        }
        for name, filters in cases.items():
            with self.subTest(name=name):
                response = self.client.post(
                    "/api/v8/contents/search",
                    json={**filters, "page": 1, "page_size": 100},
                )
                self.assertEqual(response.status_code, 200)
                value = response.json()
                expected_total = 2 if name == "default" else 1
                self.assertEqual(value["total"], expected_total)
                self.assertEqual(value["total"], len(value["items"]))

    def test_update_data_route_returns_provider_execution_result(self) -> None:
        expected = {
            "content_id": self.content_id,
            "status": "succeeded",
            "stages": [],
            "evaluation_id": 1,
            "evaluation_created": False,
            "provider_cost": 0.001,
            "currency": "USD",
        }
        with patch.object(
            api_module, "update_content_data", return_value=expected
        ) as mocked:
            response = self.client.post(
                f"/api/v8/contents/{self.content_id}/update-data"
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), expected)
        mocked.assert_called_once_with(self.content_id, db_path=self.db)


    def _insert_content(self, connection, link_id: str, platform: str, content_type: str) -> int:
        captured_at = now_utc()
        cursor = connection.execute(
            """
            INSERT INTO content_items(
                link_id, platform, platform_content_id, canonical_url, published_at, title, body,
                content_type, imported_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, '2026-07-02T04:00:00Z', ?, '', ?, ?, ?, ?)
            """,
            (
                link_id,
                platform,
                link_id.lower(),
                f"https://example.invalid/{link_id}",
                f"媒体框 {link_id}",
                content_type,
                captured_at,
                captured_at,
                captured_at,
            ),
        )
        return int(cursor.lastrowid)

    def test_content_search_reports_local_media_availability(self) -> None:
        bundle_id = "b" * 32
        with connect(self.db) as connection:
            legacy = self._insert_content(connection, "LEGACY", "douyin", "video")
            archived = self._insert_content(connection, "ARCHIV", "douyin", "video")
            hot = self._insert_content(connection, "HOTBND", "douyin", "video")
            preview = self._insert_content(connection, "PREVIW", "xiaohongshu", "image")
            rows = [
                (legacy, "media", "data/cache/v8/media/LEGACY/video.mp4", "{}"),
                (
                    archived,
                    "media",
                    "data/cache/v8/media/managed-v1/ARCHIV/originals/source.mp4",
                    json.dumps({"media_lifecycle": {"bundle_id": bundle_id}}),
                ),
                (
                    archived,
                    "media_lifecycle_manifest",
                    "data/cache/v8/media/managed-v1/ARCHIV/evidence/lifecycle-manifest.json",
                    json.dumps({"media_lifecycle": {"bundle_id": bundle_id,
                                                    "storage_state": "archived",
                                                    "operation_state": "idle"}}),
                ),
                (
                    hot,
                    "media_lifecycle_manifest",
                    "data/cache/v8/media/managed-v1/HOTBND/evidence/lifecycle-manifest.json",
                    json.dumps({"media_lifecycle": {"bundle_id": "c" * 32,
                                                    "storage_state": "hot",
                                                    "operation_state": "idle"}}),
                ),
                (
                    preview,
                    "media_preview_manifest",
                    "data/cache/v8/media/managed-v1/PREVIW/evidence/preview-manifest.json",
                    "{}",
                ),
            ]
            for content_id, artifact_type, local_path, metadata in rows:
                connection.execute(
                    """
                    INSERT INTO evidence_artifacts(
                        content_id, artifact_type, local_path, status, sha256, metadata_json, created_at
                    ) VALUES (?, ?, ?, 'available', ?, ?, ?)
                    """,
                    (content_id, artifact_type, local_path, "a" * 64, metadata, now_utc()),
                )
            connection.commit()

        searched = self.client.post(
            "/api/v8/contents/search", json={"query": "", "page_size": 20}
        )
        self.assertEqual(searched.status_code, 200)
        flags = {item["link_id"]: item["local_media_available"] for item in searched.json()["items"]}
        self.assertEqual(
            flags,
            {"A2BC3D": False, "LEGACY": True, "ARCHIV": False, "HOTBND": True, "PREVIW": True},
        )
        self.assertTrue(all(isinstance(value, bool) for value in flags.values()))

        with connect(self.db) as connection:
            connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        read_only_app = api_module.create_app(replace(self.config, read_only=True))
        with TestClient(read_only_app) as read_only_client:
            replica = read_only_client.post(
                "/api/v8/contents/search", json={"query": "", "page_size": 20}
            )
            self.assertEqual(replica.status_code, 200)
            self.assertEqual(
                {item["local_media_available"] for item in replica.json()["items"]}, {False}
            )

    def test_evidence_bin_files_are_served_with_sniffed_mime(self) -> None:
        images_dir = Path(self.temp.name) / "images"
        images_dir.mkdir()
        payloads = {
            "image-000.bin": b"\xff\xd8\xff\xe0" + b"jpeg-body" * 8,
            "image-001.bin": b"\x89PNG\r\n\x1a\n" + b"png-body" * 8,
            "image-002.bin": b"plain-bytes-without-magic" * 4,
        }
        for name, body in payloads.items():
            (images_dir / name).write_bytes(body)
        manifest_path = images_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps({"status": "complete",
                        "image_paths": [str(images_dir / name) for name in payloads]}),
            encoding="utf-8",
        )
        with connect(self.db) as connection:
            cursor = connection.execute(
                """
                INSERT INTO evidence_artifacts(
                    content_id, artifact_type, local_path, status, sha256, created_at
                ) VALUES (?, 'media_manifest', ?, 'available', ?, ?)
                """,
                (self.content_id, str(manifest_path), "d" * 64, now_utc()),
            )
            artifact_id = int(cursor.lastrowid)
            connection.commit()

        evidence = self.client.get(f"/api/v8/contents/{self.content_id}/evidence")
        self.assertEqual(evidence.status_code, 200)
        self.assertEqual([item["kind"] for item in evidence.json()["media"]], ["image"] * 3)

        expected = ["image/jpeg", "image/png", "application/octet-stream"]
        for index, media_type in enumerate(expected):
            response = self.client.get(
                f"/api/v8/contents/{self.content_id}/evidence/files/{artifact_id}/{index}"
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.headers["content-type"], media_type)
            self.assertEqual(response.content, list(payloads.values())[index])
        head = self.client.head(
            f"/api/v8/contents/{self.content_id}/evidence/files/{artifact_id}/0"
        )
        self.assertEqual(head.status_code, 200)
        self.assertEqual(head.headers["content-type"], "image/jpeg")

    def test_media_content_type_prefers_suffix_then_magic(self) -> None:
        from v8 import media_api

        self.assertEqual(media_api.media_content_type(Path("clip.mp4"), b""), "video/mp4")
        self.assertEqual(media_api.media_content_type(Path("photo.jpg"), b""), "image/jpeg")
        cases = {
            b"\xff\xd8\xff\xe1": "image/jpeg",
            b"\x89PNG\r\n\x1a\n": "image/png",
            b"GIF89a": "image/gif",
            b"RIFF\x00\x00\x00\x00WEBPVP8 ": "image/webp",
            b"\x00\x00\x00\x18ftypisom": "video/mp4",
            b"\x1a\x45\xdf\xa3": "video/webm",
        }
        for header, media_type in cases.items():
            self.assertEqual(media_api.sniff_media_type(header), media_type)
            self.assertEqual(media_api.media_content_type(Path("image-000.bin"), header), media_type)
        self.assertIsNone(media_api.sniff_media_type(b""))
        self.assertEqual(
            media_api.media_content_type(Path("image-000.bin"), b"nothing"),
            "application/octet-stream",
        )


class V8MediaRecoveryApiTest(unittest.TestCase):
    def setUp(self) -> None:
        (PROJECT_ROOT / "tmp").mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=PROJECT_ROOT / "tmp")
        self.db = Path(self.temp.name) / "recovery.sqlite3"
        self.config = _test_config(Path(self.temp.name), db_name="recovery.sqlite3")
        self.assertEqual(self.config.db_path, self.db)
        with connect(self.db) as connection:
            initialize_database(connection)
            created_at = now_utc()
            content = connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,title,
                    content_type,imported_at,created_at,updated_at
                ) VALUES ('R2CV3Y','douyin','recovery-content',
                          'https://www.douyin.com/video/recovery-content',
                          '恢复测试','video',?,?,?)
                """,
                (created_at, created_at, created_at),
            )
            content_id = int(content.lastrowid)
            connection.executemany(
                """
                INSERT INTO media_processing_slots(
                    content_id,source_sha256,processor_type,processor_version,
                    status,attempt_count,created_at,updated_at
                ) VALUES (?,?,'frames',?,'running',2,?,?)
                """,
                [
                    (
                        content_id,
                        "s" * 64,
                        "stale-frames",
                        "2000-01-01T00:00:00Z",
                        "2000-01-01T00:00:00Z",
                    ),
                    (
                        content_id,
                        "n" * 64,
                        "fresh-frames",
                        created_at,
                        created_at,
                    ),
                ],
            )
            connection.commit()
        self.app = api_module.create_app(self.config)
        self.client_context = TestClient(self.app)
        self.client = self.client_context.__enter__()

    def tearDown(self) -> None:
        self.client_context.__exit__(None, None, None)
        self.temp.cleanup()

    def test_startup_does_not_recover_stale_media_slot_without_upstream(self) -> None:
        recovery = self.app.state.recovered_media_slots
        self.assertEqual(recovery["stale_candidates"], 1)
        self.assertEqual(recovery["recovered"], 0)
        self.assertEqual(recovery["retryable_failed"], 0)
        self.assertEqual(recovery["terminal_failed"], 0)
        with connect(self.db) as connection:
            statuses = {
                row["processor_version"]: row["status"]
                for row in connection.execute(
                    "SELECT processor_version,status FROM media_processing_slots"
                )
            }
        self.assertEqual(statuses["stale-frames"], "running")
        self.assertEqual(statuses["fresh-frames"], "running")

        response = self.client.get("/api/v8/scheduler")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["media_slot_recovery"], recovery)


if __name__ == "__main__":
    unittest.main()
