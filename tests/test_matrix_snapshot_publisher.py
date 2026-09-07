"""Publisher integration fixtures: real schema, roster, scans and report freezes.

Only external transports, renderer availability and wall clocks are mocked.
No real provider request or deployment command is permitted.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import importlib.util
import io
import json
import os
import plistlib
import sqlite3
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from tests.schema_fixture import initialize_historical_schema
from tests.v9_report_fixture import activate_v9_report_fixture
from v8 import (
    account_roster,
    matrix_scan,
    paid_drain,
    pipeline,
    pipeline_cutover,
    profile_activations,
    reports,
    runtime_receipts,
    scan_receipts,
    scheduler,
    tikhub_scan,
)
from v8.capture import CaptureError, ProviderResult
from v8.newrank_matrix import GATEWAY, MatrixConfig, NewrankMatrixClient
from v8.snapshot_contract import descriptor
from v8.source_routing import parse_time
from v8.storage import (
    configure_connection_safety,
    connect,
    require_schema_compatibility,
    initialize_database,
    migrate_database,
    transaction,
)

ROOT = Path(__file__).resolve().parents[1]
BEIJING = ZoneInfo("Asia/Shanghai")
DAY = date(2026, 8, 29)
DY_UID = "12345678901"
XHS_UID = "0123456789abcdef01234567"


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def load_publisher():
    name = "dcar_matrix_snapshot_publisher"
    if name not in sys.modules:
        spec = importlib.util.spec_from_file_location(name, ROOT / "deploy/macos/publish_snapshot.py")
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
    return sys.modules[name]


@dataclass
class WriterFixture:
    project: Path
    database: Path
    day: date
    roster: dict
    activation_id: int
    scan_ids: list[int]
    report_tasks: list[str]
    runtime_identity: dict
    writer_lock: Path
    matrix_calls: list
    tikhub_calls: list

    @property
    def now(self):
        return datetime.combine(self.day, time(9), BEIJING)

    def fetch(self, url):
        database_metadata = self.database.stat()
        lock_metadata = self.writer_lock.stat()
        runtime_database_identity = {
            "canonical_path": str(self.database.resolve()),
            "device": database_metadata.st_dev,
            "inode": database_metadata.st_ino,
            "nlink": database_metadata.st_nlink,
            "access_mode": "writer",
        }
        writer_lock = {
            "path": str(self.writer_lock.resolve()),
            "device": lock_metadata.st_dev,
            "inode": lock_metadata.st_ino,
            "held": True,
        }
        if url.endswith("/health"):
            return {"status": "ok", "database": self.database.name,
                    "runtime_database_identity": runtime_database_identity,
                    "writer_lock": writer_lock,
                    "snapshot_contract": descriptor(), "database_state": {"runtime_identity": self.runtime_identity}}
        if url.endswith("/scheduler"):
            return {"requested": True, "enabled": True, "writer_lock": writer_lock,
                    "startup_catchup": {"mode": "report_only", "requested": True, "enabled": True,
                                        "status": "succeeded", "results": []}}
        raise AssertionError(url)


def create_writer_fixture(project: Path, *, day=DAY, with_content=True, preparation=True,
                          report=True, late_scan=False, matrix_windows=60,
                          preparation_delay_minutes=0, extra_douyin_members=0,
                          terminal_error=None, additional_terminal_errors=(),
                          legacy_schema18=False) -> WriterFixture:
    project = project.resolve()
    project.mkdir(parents=True, exist_ok=True)
    database = project / "app/data/dcar_insight.sqlite3"
    database.parent.mkdir(parents=True)
    writer_lock = project.parent / f"{project.name}-writer-worker.lock"
    writer_lock.write_bytes(b"fixture writer lock\n")
    writer_lock.chmod(0o600)
    raw_root = project / "data/cache/provider_raw"
    reports_root = project / "reports"
    midnight = datetime.combine(day, time.min, BEIJING)
    accepted_at = iso(midnight)
    current = [accepted_at]
    matrix_calls: list[str] = []
    tikhub_calls: list[tuple] = []
    scan_ids: list[int] = []
    report_tasks: list[str] = []
    with ExitStack() as stack:
        for name in ("v8.capture.now_utc", "v8.providers.now_utc", "v8.tikhub_scan.now_utc",
                     "v8.matrix_scan.now_utc", "v8.durable_runs.now_utc", "v8.pipeline.now_utc",
                     "v8.reports.now_utc", "v8.report_inputs.now_utc", "v8.scheduler.now_utc",
                     "v8.operations.now_utc", "v8.metric_observations.now_utc", "v8.pipeline_cutover.now_utc", "tests.v9_report_fixture.now_utc"):
            stack.enter_context(patch(name, side_effect=lambda: current[0]))
        stack.enter_context(patch("v8.capture.RAW_ROOT", raw_root))
        stack.enter_context(patch("v8.tikhub_scan.RAW_ROOT", raw_root))
        network = stack.enter_context(patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden in publisher fixture")))
        stack.enter_context(patch("v8.providers._load_key", return_value="fixture-only"))
        stack.enter_context(patch.object(reports, "render_summary_png", return_value=False))
        stack.enter_context(patch.object(reports, "PROJECT_ROOT", project))
        if legacy_schema18:
            stack.enter_context(
                patch.object(
                    reports,
                    "CURRENT_REPORT_VERSION",
                    pipeline_cutover.LEGACY_REPORT_VERSION,
                )
            )
        with connect(database) as connection:
            if legacy_schema18:
                initialize_historical_schema(connection, target_version=17)
                migrate_database(connection, from_version=17, to_version=18)
            else:
                initialize_database(connection)
            douyin_references = {DY_UID: "MS4wLjAB" + "a" * 64}
            members = [
                {"platform": "douyin", "matrix_account_id": "fixture-douyin", "uid": DY_UID,
                 "profile_ref": "https://www.douyin.com/user/" + douyin_references[DY_UID],
                 "nickname": "fixture douyin", "monitoring_status": "monitored"},
                {"platform": "xiaohongshu", "matrix_account_id": "fixture-xhs", "uid": XHS_UID,
                 "profile_ref": "https://www.xiaohongshu.com/user/profile/" + XHS_UID,
                 "nickname": "fixture xhs", "monitoring_status": "not_monitored"},
            ]
            for index in range(extra_douyin_members):
                uid = str(int(DY_UID) + index + 1)
                reference = "MS4wLjAB" + f"{index + 1:064x}"
                douyin_references[uid] = reference
                members.append({
                    "platform": "douyin",
                    "matrix_account_id": f"fixture-douyin-{index + 1}",
                    "uid": uid,
                    "profile_ref": "https://www.douyin.com/user/" + reference,
                    "nickname": f"fixture douyin {index + 1}",
                    "monitoring_status": "monitored",
                })
            source = json.dumps({"members": members}).encode()
            payload = {"source_type": "bootstrap_export", "source_captured_at": accepted_at,
                "scope": {"organization": "isolated publisher test", "coverage": "full", "account_scope": "all_added_accounts", "platforms": sorted(account_roster.PLATFORMS)},
                "source_evidence": {"kind": "official_export", "evidence_kind": "operator_declaration", "source_name": "fixture.json",
                    "export_record_id": "publisher-fixture", "exported_at": accepted_at,
                    "source_sha256": hashlib.sha256(source).hexdigest(), "scope_evidence": "Complete isolated fixture export"},
                "declared_count": len(members), "members": members,
                "pagination": {"pages": [1], "expected_pages": 1, "terminal": True, "declared_totals": [len(members)]}}
            candidate = account_roster.prepare_candidate(connection, payload, source_bytes=source,
                raw_root=project / "data/cache/rosters", observed_at=accepted_at)
            account_roster.accept_candidate(connection, candidate["candidate_id"], accepted_at=accepted_at)
            roster = account_roster.current_snapshot(connection)
            assert roster is not None
            with transaction(connection):
                target_identity_id = None
                for uid, reference in douyin_references.items():
                    identity_id = connection.execute(
                        "SELECT id FROM account_platform_identities "
                        "WHERE platform='douyin' AND uid=?",
                        (uid,),
                    ).fetchone()[0]
                    if uid == DY_UID:
                        target_identity_id = identity_id
                    connection.execute(
                        "INSERT INTO account_provider_references("
                        "account_identity_id,provider,reference_kind,reference_value,created_at,updated_at) "
                        "VALUES (?,'TikHub','sec_user_id',?,?,?)",
                        (identity_id, reference, accepted_at, accepted_at),
                    )
            assert target_identity_id is not None
        activate_v9_report_fixture(database, [])
        if legacy_schema18:
            activation_id = pipeline_cutover.record_activation(
                db_path=database,
                at=accepted_at,
            )["activation"]["run_id"]
        else:
            with connect(database) as connection:
                activation = profile_activations.append_activation(
                    connection,
                    profile_id=profile_activations.MATRIX_PROFILE,
                    roster_snapshot_id=int(roster["id"]),
                    roster_members_sha256=str(roster["members_sha256"]),
                    effective_at=accepted_at,
                    build_receipt_sha256=hashlib.sha256(
                        b"publisher-fixture-schema19-build"
                    ).hexdigest(),
                    actor="publisher-fixture",
                    reason="activate the schema19 publisher fixture",
                    metadata={"fixture": True},
                    created_at=accepted_at,
                )
                activation_id = int(activation["activation_id"])
                with transaction(connection):
                    paid_drain.issue_activation_permit_in_transaction(
                        connection,
                        activation_id=activation_id,
                        drain_id=f"publisher-fixture:{activation_id}",
                        source_activation_id=activation_id,
                        business_day=day.isoformat(),
                        planned_effective_at=accepted_at,
                        build_receipt_sha256=hashlib.sha256(
                            b"publisher-fixture-schema19-build"
                        ).hexdigest(),
                        runtime_root_receipt_sha256=hashlib.sha256(
                            b"publisher-fixture-runtime-root"
                        ).hexdigest(),
                        now=accepted_at,
                    )
        current[0] = iso(midnight + timedelta(hours=3))

        def supplier(operation, request):
            tikhub_calls.append((operation, copy.deepcopy(request)))
            if (
                terminal_error is not None
                and operation == "douyin_user_posts"
                and request["reference"] == douyin_references[DY_UID]
            ):
                if terminal_error == "invalid_response":
                    return ProviderResult(
                        {},
                        {"code": 200, "data": {}},
                        200,
                        True,
                    )
                raise CaptureError(
                    "fixture terminal",
                    retryable=terminal_error in {
                        "transport_error",
                        "provider_retry_requested",
                    },
                    error_code=terminal_error,
                    http_status=(
                        503
                        if terminal_error
                        in {"transport_error", "provider_retry_requested"}
                        else None
                    ),
                    billed=False,
                )
            if operation == "douyin_user_posts":
                body = {"code": 200, "data": {"aweme_list": [], "has_more": False}}
            elif operation == "xiaohongshu_user_posts":
                body = {"code": 200, "data": {"code": 0, "success": True, "data": {"notes": [], "has_more": False}}}
            else:
                raise AssertionError("Unexpected paid operation: " + operation)
            return ProviderResult({}, body, 200, True)

        result = pipeline.dispatch("tikhub_reconcile", db_path=database, reports_root=reports_root, at=current[0], call_override=supplier)
        if result.get("status") not in ({"succeeded", "partial"} if terminal_error else {"succeeded"}):
            raise AssertionError(result)
        scan_ids.extend(item["scheduler_run_id"] for item in result["scans"])
        if terminal_error in {
            "transport_error",
            "provider_retry_requested",
            "invalid_response",
        }:
            partial = next(
                item for item in result["scans"]
                if item["status"] == "partial" and item["reason"] == terminal_error
            )
            terminal = partial
            for _attempt in range(3):
                if terminal.get("status") == "failed":
                    break
                current[0] = terminal["next_resume_at"]
                terminal = tikhub_scan.resume_account_scan(
                    partial["scheduler_run_id"],
                    db_path=database,
                    raw_root=raw_root,
                    now=current[0],
                    call_override=supplier,
                )
            if terminal.get("status") != "failed":
                raise AssertionError(terminal)
        for index, error_code in enumerate(additional_terminal_errors):
            current[0] = iso(midnight + timedelta(hours=3, minutes=index + 10))

            def extra_supplier(operation, request, *, reason=error_code):
                tikhub_calls.append((operation, copy.deepcopy(request)))
                raise CaptureError(
                    "fixture additional terminal",
                    retryable=False,
                    error_code=reason,
                    billed=False,
                )

            terminal = tikhub_scan.run_account_scan(
                target_identity_id,
                window_start=iso(midnight - timedelta(days=7)),
                window_end=iso(midnight),
                purpose="reconcile",
                roster_snapshot_id=roster["id"],
                roster_snapshot_hash=roster["members_sha256"],
                db_path=database,
                task_id=f"publisher-terminal-proof-{index}",
                task_max_amount=3,
                raw_root=raw_root,
                now=current[0],
                call_override=extra_supplier,
            )
            if terminal.get("status") != "failed":
                raise AssertionError(terminal)
            scan_ids.append(terminal["scheduler_run_id"])
        tick = [0.0]

        def advance(delay):
            tick[0] += delay

        limiter = matrix_scan.MatrixRateLimiter(clock=lambda: tick[0], sleeper=advance)
        config = MatrixConfig(api_url=GATEWAY, n_token="fixture-token", key_id="fixture-key", secret_key="fixture-sign")
        for index in range(matrix_windows):
            offset, platform = index // 2, ("douyin", "xiaohongshu")[index % 2]
            current[0] = iso(midnight + timedelta(hours=8, minutes=5)) if late_scan and index == 0 else iso(midnight + timedelta(hours=3))
            work = {"platType": 2, "awemeId": "7379190309625810185", "uid": DY_UID, "nickname": "fixture",
                    "createTime": (midnight - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S"),
                    "title": "fixture work", "playCount": 123, "diggCount": 4, "commentCount": 0,
                    "shareCount": 1, "favoriteCount": 2, "scrollId": [12345, "7379190309625810185"]}
            pages = iter([[work], []] if with_content and index == 0 else [[]])

            def transport(request, timeout):
                matrix_calls.append(json.loads(request.data)["pathName"])
                return 200, json.dumps({"code": 0, "data": json.dumps(next(pages))}).encode()

            client = NewrankMatrixClient(config, transport=transport, clock=lambda: parse_time(current[0]))
            result = matrix_scan.run_matrix_scan("works", platform, purpose="daily",
                start_at=iso(midnight - timedelta(days=offset + 1)), end_at=iso(midnight - timedelta(days=offset)),
                overall_start_at=iso(midnight - timedelta(days=30)), overall_end_at=iso(midnight),
                roster_snapshot_id=roster["id"], roster_snapshot_hash=roster["members_sha256"],
                db_path=database, raw_root=raw_root, client=client, rate_limiter=limiter, now=current[0])
            if result.get("status") != "succeeded":
                raise AssertionError(result)
            scan_ids.append(result["scheduler_run_id"])
        if not late_scan and not legacy_schema18:
            current[0] = iso(midnight + timedelta(hours=7))
            receipt_refresh = runtime_receipts.refresh_runtime_receipts(
                db_path=database,
                cutoff_at=current[0],
                evidence_root=project / "runtime/coverage-receipts",
            )
            if receipt_refresh.get("status") != "succeeded":
                raise AssertionError(receipt_refresh)
        if preparation:
            current[0] = iso(midnight + timedelta(
                hours=7,
                minutes=30 + preparation_delay_minutes,
            ))
            result = pipeline.dispatch("daily_pipeline_summary", db_path=database, reports_root=reports_root, at=current[0])
            if result.get("status") != "succeeded":
                raise AssertionError(result)
        if report:
            jobs = [("daily_report", 0)] + ([("weekly_report", 30)] if day.weekday() == 0 else [])
            for job_id, minute in jobs:
                scheduled = midnight + timedelta(hours=8, minutes=minute)
                current[0] = iso(scheduled + timedelta(minutes=1))
                result = scheduler.execute_job(job_id, scheduled, db_path=database, reports_root=reports_root)
                if result.get("status") not in {"succeeded", "partial"}:
                    raise AssertionError(result)
                with connect(database) as connection:
                    details = json.loads(connection.execute("SELECT details_json FROM scheduler_runs WHERE job_id=? ORDER BY id DESC LIMIT 1", (job_id,)).fetchone()[0])
                    report_tasks.append(details["task_id"])
        if late_scan and not legacy_schema18:
            current[0] = iso(midnight + timedelta(hours=8, minutes=10))
            receipt_refresh = runtime_receipts.refresh_runtime_receipts(
                db_path=database,
                cutoff_at=current[0],
                evidence_root=project / "runtime/coverage-receipts",
            )
            if receipt_refresh.get("status") != "succeeded":
                raise AssertionError(receipt_refresh)
        with connect(database) as connection:
            identity = (
                {}
                if legacy_schema18
                else load_publisher()._database_runtime_identity(connection)
            )
        for salt in (".comment_hash_salt", ".platform_user_salt"):
            target = project / "data/cache" / salt
            target.write_text("publisher-fixture-salt", encoding="utf-8")
            target.chmod(0o600)
        network.assert_not_called()
    return WriterFixture(project, database, day, roster, activation_id, scan_ids, report_tasks, identity, writer_lock, matrix_calls, tikhub_calls)


class MatrixSnapshotPublisherIntegrationTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="dcar-publisher-contract-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        isolated_home = self.root / "isolated-os-user"
        isolated_home.mkdir()
        identity = patch.object(pipeline_cutover.pwd, "getpwuid", return_value=SimpleNamespace(pw_dir=str(isolated_home)))
        identity.start()
        self.addCleanup(identity.stop)
        self.publisher = load_publisher()
        self.network = patch("urllib.request.urlopen", side_effect=AssertionError("Network forbidden"))
        self.network.start()
        self.addCleanup(self.network.stop)

    def freshness(self, fixture):
        return self.publisher.check_writer_freshness(fixture.database, now=fixture.now,
            maximum_content_lag_days=1, fetch_json=fixture.fetch, project_root=fixture.project)

    def manual_report(self, fixture, *, at="2026-08-29T00:45:00Z"):
        with ExitStack() as stack:
            for name in ("v8.reports.now_utc", "v8.report_inputs.now_utc", "v8.operations.now_utc"):
                stack.enter_context(patch(name, return_value=at))
            stack.enter_context(patch.object(reports, "PROJECT_ROOT", fixture.project))
            stack.enter_context(patch.object(reports, "render_summary_png", return_value=False))
            with connect(fixture.database) as connection:
                schema_version = int(
                    connection.execute("PRAGMA user_version").fetchone()[0]
                )
            if schema_version == 18:
                stack.enter_context(patch.object(
                    reports, "require_schema_compatibility",
                    side_effect=lambda connection, **_: require_schema_compatibility(
                        connection, supported_versions=frozenset({18})
                    ),
                ))
                stack.enter_context(
                    patch.object(
                        reports,
                        "CURRENT_REPORT_VERSION",
                        pipeline_cutover.LEGACY_REPORT_VERSION,
                    )
                )
            return reports.create_and_run_task(task_type="custom", period_start=(fixture.day - timedelta(days=1)).isoformat(),
                period_end=(fixture.day - timedelta(days=1)).isoformat(), creation_source="manual",
                db_path=fixture.database, reports_root=fixture.project / "reports")

    def cutover(self, fixture):
        task = self.manual_report(fixture)
        receipt = pipeline_cutover.record_cutover(db_path=fixture.database, activation_run_id=fixture.activation_id,
            report_task_ids=[task["id"]], scan_run_ids=fixture.scan_ids, cutoff_at="2026-08-29T00:45:00Z",
            at=iso(fixture.now), project_root=fixture.project)
        return task, receipt

    def installed_writer_fixture(self, fixture, *, name="formal-user"):
        """Only temporary files: reproduce actual installed template fields."""
        home = self.root / name
        plist_path = home / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        plist_path.parent.mkdir(parents=True)
        template = (ROOT / "deploy/macos/cn.tj.dcar.writer-worker.plist.template").read_text()
        value = plistlib.loads(template.replace("__PROJECT_ROOT_XML__", str(fixture.project))
            .replace("__HOME_XML__", str(home)).replace("__RECONCILE_FROM_XML__", fixture.day.isoformat()).encode())
        installed_database = Path(value["EnvironmentVariables"]["DCAR_V8_DB"])
        installed_database.parent.mkdir(parents=True)
        if fixture.database != installed_database:
            fixture.database.replace(installed_database)
            fixture.database = installed_database
        plist_path.write_bytes(plistlib.dumps(value))
        plist_path.chmod(0o644)
        lock = Path(value["EnvironmentVariables"]["DCAR_WRITER_LOCK"])
        lock.parent.mkdir(parents=True)
        lock.write_bytes(b"fixture writer lock\n")
        lock.chmod(0o600)
        return home, plist_path, lock

    def formal_cli_context(self, fixture, home, lock_value):
        stack = ExitStack()
        stack.enter_context(patch.object(pipeline_cutover, "PROJECT_ROOT", fixture.project))
        stack.enter_context(patch.object(pipeline_cutover.pwd, "getpwuid", return_value=SimpleNamespace(pw_dir=str(home))))
        stack.enter_context(patch.dict(os.environ, {
            "DCAR_V8_DB": str(fixture.database),
            "DCAR_WRITER_LOCK": lock_value,
            "DCAR_PROJECT_ROOT": str(fixture.project),
            "DCAR_TEST_DENY_FORMAL_DB": "0",
        }))
        return stack

    def test_publication_evidence_accepts_only_versioned_native_or_legacy_discovery(self):
        report = {
            "status": "succeeded",
            "input_sha256": "a" * 64,
            "scope_sha256": "b" * 64,
            "report_file": {"path": "report.json", "sha256": "c" * 64},
            "files": [{"file_kind": "report-json"}],
        }
        value = {
            "schema": self.publisher.FRESHNESS_SCHEMA,
            "beijing_date": DAY.isoformat(),
            "verified_at": iso(datetime.combine(DAY, time(9), BEIJING)),
            "status": "succeeded",
            "mode": "scheduled",
            "discovery": {
                "contract_version": pipeline_cutover.PROFILE_DAY_DISCOVERY_CONTRACT
            },
            "preparation": {"job_id": "daily_pipeline_summary"},
            "reports": [report],
            "cutover": None,
        }
        self.assertEqual(self.publisher._validate_publication_evidence(value), value)
        for contract in ("matrix-publication-discovery-v1", "unversioned-summary"):
            changed = copy.deepcopy(value)
            changed["discovery"]["contract_version"] = contract
            if contract == "matrix-publication-discovery-v1":
                self.assertEqual(
                    self.publisher._validate_publication_evidence(changed), changed
                )
            else:
                with self.assertRaisesRegex(
                    self.publisher.SnapshotPublishError,
                    "no real scan/preparation dependencies",
                ):
                    self.publisher._validate_publication_evidence(changed)

    def test_real_sixty_matrix_windows_and_seven_day_accounts_bind_frozen_partial_report(self):
        fixture = create_writer_fixture(self.root / "project")
        before = fixture.database.read_bytes()
        result = self.freshness(fixture)
        self.assertEqual(result.evidence["discovery"]["coverage"]["matrix_complete_windows"], 60)
        self.assertEqual(result.evidence["discovery"]["coverage"]["tikhub_complete_members"], 2)
        self.assertEqual(len(fixture.matrix_calls), 61)
        self.assertEqual(len(fixture.tikhub_calls), 2)
        self.assertEqual(result.daily_report_status, "partial")
        self.assertEqual(result.evidence["reports"][0]["task_id"], fixture.report_tasks[0])
        self.assertEqual(result.evidence["preparation"]["job_id"], "daily_pipeline_summary")
        self.assertNotIn("capture_status", result.evidence)
        self.assertEqual(fixture.database.read_bytes(), before)

    def test_delayed_preparation_keeps_the_frozen_seven_thirty_receipt(self):
        fixture = create_writer_fixture(
            self.root / "delayed-preparation",
            preparation_delay_minutes=5,
        )
        result = self.freshness(fixture)
        expected = iso(datetime.combine(fixture.day, time(7, 30), BEIJING))
        self.assertEqual(result.evidence["preparation"]["scheduled_at"], expected)
        with connect(fixture.database) as connection:
            row = connection.execute(
                "SELECT details_json FROM scheduler_runs "
                "WHERE job_id='daily_pipeline_summary' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        details = json.loads(row["details_json"])
        self.assertEqual(details["identity"]["at"], expected)
        self.assertEqual(details["summary"]["captured_at"], expected)

    def test_late_scan_remains_frozen_partial_but_is_publishable_after_actual_completion(self):
        fixture = create_writer_fixture(self.root / "late", late_scan=True)
        result = self.freshness(fixture)
        self.assertTrue(result.evidence["discovery"]["coverage"]["complete"])
        self.assertEqual(result.evidence["status"], "partial")
        self.assertEqual(result.evidence["reports"][0]["partial_reasons"]["discovery_coverage"]["complete"], False)

    def test_ninety_nine_percent_terminal_coverage_is_deep_verified_and_publishable(self):
        fixture = create_writer_fixture(
            self.root / "terminal-partial",
            extra_douyin_members=98,
            terminal_error="invalid_response",
        )
        result = self.freshness(fixture)
        coverage = result.evidence["discovery"]["coverage"]
        self.assertEqual(coverage["status"], "partial_publishable")
        self.assertFalse(coverage["complete"])
        self.assertTrue(coverage["partial_publishable"])
        self.assertEqual(coverage["tikhub_expected_members"], 100)
        self.assertEqual(coverage["tikhub_succeeded_members"], 99)
        self.assertEqual(coverage["tikhub_blocked_members"], 1)
        self.assertEqual(coverage["tikhub_accounted_members"], 100)
        terminal_proofs = list(coverage["days"][0]["terminal_blockers"].values())
        self.assertEqual(len(terminal_proofs), 1)
        self.assertEqual(terminal_proofs[0]["terminal_class"], "provider_transient")
        self.assertFalse(terminal_proofs[0]["publication_blocker"])
        self.assertEqual(result.evidence["mode"], "scheduled")
        self.assertEqual(result.evidence["status"], "partial")

    def test_worst_terminal_class_blocks_partial_publication_and_is_not_masked(self):
        fixture = create_writer_fixture(
            self.root / "terminal-blocker",
            extra_douyin_members=98,
            terminal_error="invalid_response",
            additional_terminal_errors=("identity_conflict", "provider_auth_blocked"),
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        cutoff = "2026-08-29T00:45:00Z"
        with connect(fixture.database) as connection:
            frozen = scan_receipts.coverage(
                connection,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )
            pipeline_cutover.verify_frozen_scans(
                connection,
                frozen,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )
        self.assertFalse(frozen["partial_publishable"])
        blocker = next(iter(frozen["days"][0]["terminal_blockers"].values()))
        self.assertEqual(blocker["terminal_class"], "integrity")
        self.assertEqual(
            blocker["terminal_classes"],
            ["integrity", "provider_transient", "readiness_operator"],
        )
        self.assertEqual(len(blocker["scheduler_run_ids"]), 3)
        self.assertTrue(blocker["publication_blocker"])
        with connect(fixture.database) as connection, self.assertRaisesRegex(
            pipeline_cutover.PublicationEvidenceError,
            "publication_runtime_coverage_not_publishable",
        ):
            pipeline_cutover.runtime_evidence(connection, at=iso(fixture.now))

    def test_failed_terminal_attempt_tamper_is_rejected(self):
        fixture = create_writer_fixture(
            self.root / "terminal-tamper",
            extra_douyin_members=98,
            terminal_error="invalid_response",
        )
        with connect(fixture.database) as connection, transaction(connection):
            row = connection.execute(
                "SELECT id,details_json FROM scheduler_runs "
                "WHERE job_id='tikhub_reconcile' AND status='failed' "
                "ORDER BY id LIMIT 1"
            ).fetchone()
            details = json.loads(row["details_json"])
            details["summary"]["reason"] = "http_503"
            connection.execute(
                "UPDATE scheduler_runs SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), row["id"]),
            )
        with self.assertRaisesRegex(
            self.publisher.SnapshotPublishError,
            "publication_runtime_receipt_lineage_invalid",
        ):
            self.freshness(fixture)

    def test_successful_scan_raw_tamper_does_not_reopen_a_sealed_receipt(self):
        fixture = create_writer_fixture(self.root / "scan-raw-tamper")
        with connect(fixture.database) as connection:
            frozen = scan_receipts.coverage(
                connection,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at="2026-08-29T00:45:00Z",
            )
            raw_id = next(
                reference["raw_id"]
                for proof in frozen["scan_references"]
                for reference in proof["references"]
            )
            raw_path = Path(
                connection.execute(
                    "SELECT local_path FROM provider_raw_responses WHERE id=?",
                    (raw_id,),
                ).fetchone()[0]
            )
        raw_path.write_bytes(raw_path.read_bytes() + b"\n")
        freshness = self.freshness(fixture)
        self.assertTrue(freshness.evidence["discovery"]["coverage"]["complete"])

    def test_benign_late_duplicate_keeps_complete_frozen_proof_traceable(self):
        fixture = create_writer_fixture(
            self.root / "benign-late-duplicate",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        cutoff = "2026-08-29T00:45:00Z"
        current = ["2026-08-29T00:44:00Z"]
        midnight = datetime.combine(fixture.day, time.min, BEIJING)
        tick = [0.0]

        def transport(request, timeout):
            current[0] = "2026-08-29T00:46:00Z"
            return 200, json.dumps({"code": 0, "data": "[]"}).encode()

        limiter = matrix_scan.MatrixRateLimiter(
            clock=lambda: tick[0],
            sleeper=lambda delay: tick.__setitem__(0, tick[0] + delay),
        )
        client = NewrankMatrixClient(
            MatrixConfig(
                api_url=GATEWAY,
                n_token="fixture-token",
                key_id="fixture-key",
                secret_key="fixture-sign",
            ),
            transport=transport,
            clock=lambda: parse_time(current[0]),
        )
        with patch("v8.matrix_scan.now_utc", side_effect=lambda: current[0]), patch(
            "v8.durable_runs.now_utc", side_effect=lambda: current[0]
        ):
            duplicate = matrix_scan.run_matrix_scan(
                "works",
                "douyin",
                purpose="late-duplicate-proof",
                start_at=iso(midnight - timedelta(days=1)),
                end_at=iso(midnight),
                overall_start_at=iso(midnight - timedelta(days=30)),
                overall_end_at=iso(midnight),
                roster_snapshot_id=fixture.roster["id"],
                roster_snapshot_hash=fixture.roster["members_sha256"],
                db_path=fixture.database,
                raw_root=fixture.project / "data/cache/provider_raw",
                client=client,
                rate_limiter=limiter,
            )
        self.assertEqual(duplicate["status"], "succeeded")
        with connect(fixture.database) as connection:
            frozen = scan_receipts.coverage(
                connection,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )
            self.assertTrue(frozen["complete"])
            self.assertTrue(frozen["scan_traceable"])
            self.assertEqual(
                frozen["scan_errors"],
                {str(duplicate["scheduler_run_id"]): "scan_not_complete_at_cutoff"},
            )
            pipeline_cutover.verify_frozen_scans(
                connection,
                frozen,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )

    def test_legacy_v1_frozen_coverage_uses_only_its_explicit_verifier(self):
        fixture = create_writer_fixture(
            self.root / "legacy-coverage-v1",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        cutoff = "2026-08-29T00:45:00Z"
        with connect(fixture.database) as connection:
            current = scan_receipts.coverage(
                connection,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )
            legacy = copy.deepcopy(current)
            legacy["contract_version"] = "matrix-first-coverage-v1"
            legacy.pop("partial_publishable")
            for day in legacy["days"]:
                for key in (
                    "partial_publishable",
                    "succeeded_identity_ids",
                    "blocked_identity_ids",
                    "not_applicable_identity_ids",
                    "accounted_identity_ids",
                    "required_identity_ids",
                    "terminal_blockers",
                    "success_percentage",
                    "accounted_percentage",
                ):
                    day.pop(key)
            detail = legacy["discovery_coverage"]
            for key in (
                "succeeded_identity_occurrence_count",
                "blocked_identity_occurrence_count",
                "not_applicable_identity_occurrence_count",
                "accounted_identity_occurrence_count",
                "required_identity_occurrence_count",
                "accounted_percentage",
                "partial_publishable",
            ):
                detail.pop(key)
            detail["success_rule"] = "matrix-first-coverage-v1"
            pipeline_cutover.verify_frozen_scans(
                connection,
                legacy,
                period_start="2026-08-28",
                period_end="2026-08-28",
                cutoff_at=cutoff,
            )

            missing_version = copy.deepcopy(legacy)
            missing_version.pop("contract_version")
            with self.assertRaisesRegex(
                pipeline_cutover.PublicationEvidenceError,
                "publication_frozen_scan_scope_invalid",
            ):
                pipeline_cutover.verify_frozen_scans(
                    connection,
                    missing_version,
                    period_start="2026-08-28",
                    period_end="2026-08-28",
                    cutoff_at=cutoff,
                )

            wrong_version = copy.deepcopy(legacy)
            wrong_version["contract_version"] = scan_receipts.CONTRACT_VERSION
            if scan_receipts.CONTRACT_VERSION != "matrix-first-coverage-v1":
                with self.assertRaises(pipeline_cutover.PublicationEvidenceError):
                    pipeline_cutover.verify_frozen_scans(
                        connection,
                        wrong_version,
                        period_start="2026-08-28",
                        period_end="2026-08-28",
                        cutoff_at=cutoff,
                    )

    def test_actual_empty_pages_are_healthy_without_a_new_content_timestamp(self):
        fixture = create_writer_fixture(self.root / "empty", with_content=False)
        result = self.freshness(fixture)
        self.assertTrue(result.evidence["discovery"]["coverage"]["complete"])
        self.assertEqual(result.content_count, 0)
        self.assertIsNone(result.latest_published_at)

    def test_activation_generator_is_idempotent_and_keeps_one_real_immutable_attempt(self):
        fixture = create_writer_fixture(
            self.root / "activation",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        before = fixture.database.read_bytes()
        first = pipeline_cutover.record_activation(db_path=fixture.database, at=iso(fixture.now))
        second = pipeline_cutover.record_activation(db_path=fixture.database, at=iso(fixture.now + timedelta(days=1)))
        self.assertEqual(first, second)
        self.assertEqual(first["activation"]["run_id"], fixture.activation_id)
        self.assertEqual(first["cutover_at"], "2026-08-28T16:00:00Z")
        self.assertEqual(first["roster_snapshot_hash"], fixture.roster["members_sha256"])
        with connect(fixture.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?", (pipeline_cutover.ACTIVATION_JOB,)).fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_run_attempts WHERE scheduler_run_id=?", (fixture.activation_id,)).fetchone()[0], 1)
            with self.assertRaisesRegex(sqlite3.IntegrityError, "one running-to-terminal update"):
                connection.execute("UPDATE scheduler_run_attempts SET details_json='{}' WHERE scheduler_run_id=?", (fixture.activation_id,))
        self.assertEqual(fixture.database.read_bytes(), before)

    def test_activation_rejects_tampered_roster_source_and_formal_clock_override(self):
        fixture = create_writer_fixture(
            self.root / "bad-roster",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        path = Path(fixture.roster["source_path"])
        path.write_bytes(path.read_bytes() + b"\nchanged")
        with self.assertRaisesRegex(ValueError, "roster_source_missing_or_changed"):
            pipeline_cutover.record_activation(db_path=fixture.database, at=iso(fixture.now))
        with patch.object(pipeline_cutover, "is_formal_database_path", return_value=True), patch.object(
            pipeline_cutover, "connect", side_effect=AssertionError("Formal clock override must fail before DB access")):
            with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "formal_activation_clock_override_forbidden"):
                pipeline_cutover.record_activation(db_path=fixture.database, at=iso(fixture.now))

    def test_activation_cannot_be_created_for_a_source16_or17_database(self):
        for version in (16, 17):
            database = self.root / f"schema-{version}.sqlite3"
            with self.subTest(version=version), sqlite3.connect(database) as connection:
                connection.row_factory = sqlite3.Row
                configure_connection_safety(connection)
                initialize_historical_schema(connection, target_version=version)
            with sqlite3.connect(database) as connection:
                before = "\n".join(connection.iterdump())
            with self.assertRaises(RuntimeError):
                pipeline_cutover.record_activation(
                    db_path=database,
                    at=iso(datetime.combine(DAY, time(9), BEIJING)),
                )
            with sqlite3.connect(database) as connection:
                after = "\n".join(connection.iterdump())
            self.assertEqual(after, before)

    def test_actual_cutover_seals_manual_frozen_partial_without_fabricating_cron_runs(self):
        fixture = create_writer_fixture(
            self.root / "cutover",
            preparation=False,
            report=False,
            legacy_schema18=True,
        )
        task, receipt = self.cutover(fixture)
        self.assertEqual(receipt["payload"]["status"], "partial")
        self.assertEqual(receipt["payload"]["report_task_ids"], [task["id"]])
        path = Path(receipt["receipt"]["path"])
        self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(), receipt["receipt"]["sha256"])
        with connect(fixture.database) as connection:
            verified = pipeline_cutover.verify_cutover(
                connection,
                run_id=receipt["terminal"]["run_id"],
                at=iso(fixture.now),
                project_root=fixture.project,
            )
        self.assertEqual(verified["payload"], receipt["payload"])
        self.assertEqual(verified["payload"]["reports"][0]["task_id"], task["id"])
        self.assertTrue(verified["payload"]["reports"][0]["partial_reasons"])
        with connect(fixture.database) as connection:
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM scheduler_runs WHERE job_id IN ('daily_capture','daily_pipeline_summary','daily_report')").fetchone()[0], 0)
        repeated = pipeline_cutover.record_cutover(db_path=fixture.database, activation_run_id=fixture.activation_id,
            report_task_ids=[task["id"]], scan_run_ids=fixture.scan_ids, cutoff_at="2026-08-29T00:45:00Z",
            at=iso(fixture.now + timedelta(minutes=1)), project_root=fixture.project)
        self.assertEqual(receipt, repeated)

    def test_cutover_cannot_rebind_scans_extend_day_or_pass_a_tampered_seal(self):
        fixture = create_writer_fixture(
            self.root / "bounded-cutover",
            preparation=False,
            report=False,
            legacy_schema18=True,
        )
        task, receipt = self.cutover(fixture)
        with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "cutover_cannot_rebind_reports_or_scans"):
            pipeline_cutover.record_cutover(db_path=fixture.database, activation_run_id=fixture.activation_id,
                report_task_ids=[task["id"]], scan_run_ids=fixture.scan_ids[:-1], cutoff_at="2026-08-29T00:45:00Z",
                at=iso(fixture.now), project_root=fixture.project)
        with connect(fixture.database) as connection:
            with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "cutover_outside_first_beijing_day"):
                pipeline_cutover.verify_cutover(connection, run_id=receipt["terminal"]["run_id"],
                    at=iso(fixture.now + timedelta(days=1)), project_root=fixture.project)
        path = Path(receipt["receipt"]["path"])
        path.write_bytes(path.read_bytes() + b"\n")
        with connect(fixture.database) as connection, self.assertRaisesRegex(
            pipeline_cutover.PublicationEvidenceError,
            "publication_file_hash_mismatch",
        ):
            pipeline_cutover.verify_cutover(
                connection,
                run_id=receipt["terminal"]["run_id"],
                at=iso(fixture.now),
                project_root=fixture.project,
            )

    def test_activation_cli_uses_canonical_writer_lock_and_does_not_reset_event(self):
        fixture = create_writer_fixture(
            self.root / "cli",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        (fixture.project / "runtime").mkdir(exist_ok=True)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.assertEqual(pipeline_cutover.main(["--db", str(fixture.database), "--project-root", str(fixture.project),
                "--isolated-candidate", "--activate-only", "--at", iso(fixture.now)]), 0)
        value = json.loads(buffer.getvalue())
        self.assertEqual(value["activation"]["run_id"], fixture.activation_id)
        self.assertEqual(value["cutover_at"], "2026-08-28T16:00:00Z")
        self.assertTrue((fixture.project / "runtime/writer-worker.lock").is_file())

    def test_formal_cli_rejects_held_external_writer_lock_before_any_database_access(self):
        fixture = create_writer_fixture(self.root / "held-formal")
        home, _plist, external = self.installed_writer_fixture(fixture)
        decoy = fixture.project / "runtime/writer-worker.lock"
        decoy.parent.mkdir(exist_ok=True)
        decoy.write_bytes(b"unlocked checkout decoy")
        database_before, lock_before = fixture.database.read_bytes(), external.read_bytes()
        handle = os.open(external, os.O_RDWR)
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.formal_cli_context(fixture, home, str(external)), patch.object(
                    pipeline_cutover, "connect", side_effect=AssertionError("occupied writer must prevent all DB access")) as connection:
                with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "cutover_writer_lock_held"):
                    pipeline_cutover.main(["--db", str(fixture.database), "--project-root", str(fixture.project), "--activate-only"])
                connection.assert_not_called()
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
            os.close(handle)
        self.assertEqual(fixture.database.read_bytes(), database_before)
        self.assertEqual(external.read_bytes(), lock_before)
        self.assertEqual(decoy.read_bytes(), b"unlocked checkout decoy")

    def test_formal_cli_rejects_missing_wrong_or_substituted_lock_without_database_changes(self):
        fixture = create_writer_fixture(self.root / "formal-lock-config")
        before = fixture.database.read_bytes()
        for kind in ("missing_env", "wrong_env", "missing_installed_lock", "wrong_installed_lock", "missing_lock_file", "symlink_lock"):
            with self.subTest(kind=kind):
                home, plist_path, external = self.installed_writer_fixture(fixture, name=kind)
                configured = str(external)
                if kind == "missing_env":
                    configured = ""
                elif kind == "wrong_env":
                    configured = str(fixture.project / "runtime/writer-worker.lock")
                elif kind in {"missing_installed_lock", "wrong_installed_lock"}:
                    value = plistlib.loads(plist_path.read_bytes())
                    if kind == "missing_installed_lock":
                        value["EnvironmentVariables"].pop("DCAR_WRITER_LOCK")
                    else:
                        value["EnvironmentVariables"]["DCAR_WRITER_LOCK"] = str(fixture.project / "runtime/writer-worker.lock")
                    plist_path.write_bytes(plistlib.dumps(value))
                else:
                    external.unlink()
                    if kind == "symlink_lock":
                        alternate = home / "alternate.lock"
                        alternate.write_bytes(b"unlocked alternative")
                        external.symlink_to(alternate)
                with self.formal_cli_context(fixture, home, configured), patch.object(
                        pipeline_cutover, "connect", side_effect=AssertionError("invalid lock must prevent all DB access")) as connection:
                    with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError, "cutover_formal_writer_lock_"):
                        pipeline_cutover.main(["--db", str(fixture.database), "--project-root", str(fixture.project), "--activate-only"])
                    connection.assert_not_called()
                self.assertEqual(fixture.database.read_bytes(), before)
                if kind == "missing_lock_file":
                    self.assertFalse(external.exists())
                self.assertFalse((fixture.project / "runtime/writer-worker.lock").exists())

    def test_formal_cli_uses_valid_installed_external_lock_without_touching_checkout_decoy(self):
        fixture = create_writer_fixture(
            self.root / "formal-valid",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        home, _plist, external = self.installed_writer_fixture(fixture)
        decoy = fixture.project / "runtime/writer-worker.lock"
        decoy.parent.mkdir(exist_ok=True)
        decoy.write_bytes(b"held checkout decoy")
        before = fixture.database.read_bytes()
        handle = os.open(decoy, os.O_RDWR)
        output = io.StringIO()
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.formal_cli_context(fixture, home, str(external)), patch.object(
                    pipeline_cutover, "now_utc", return_value=iso(fixture.now)), patch.object(
                    pipeline_cutover,
                    "record_activation",
                    return_value={"activation": {"run_id": fixture.activation_id}},
                ) as activation, redirect_stdout(output):
                self.assertEqual(pipeline_cutover.main(["--db", str(fixture.database),
                    "--project-root", str(fixture.project), "--activate-only"]), 0)
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
            os.close(handle)
        self.assertEqual(json.loads(output.getvalue())["activation"]["run_id"], fixture.activation_id)
        activation.assert_called_once_with(db_path=fixture.database, at=None)
        self.assertEqual(fixture.database.read_bytes(), before)
        self.assertEqual(decoy.read_bytes(), b"held checkout decoy")
        self.assertEqual(external.read_bytes(), b"fixture writer lock\n")

    def test_cross_checkout_cannot_hide_installed_writer_database_or_hardlink_alias(self):
        fixture = create_writer_fixture(self.root / "installed-production-fixture")
        home, _plist, external = self.installed_writer_fixture(fixture)
        other = self.root / "different-code-checkout"
        (other / "runtime").mkdir(parents=True)
        alias = other / "database-hardlink.sqlite3"
        os.link(fixture.database, alias)
        before = fixture.database.read_bytes()
        for root, database in ((other, fixture.database), (fixture.project, fixture.database), (other, alias)):
            with self.subTest(root=root, database=database), patch.object(pipeline_cutover, "PROJECT_ROOT", other), patch.object(
                    pipeline_cutover.pwd, "getpwuid", return_value=SimpleNamespace(pw_dir=str(home))), patch.dict(
                    os.environ, {"DCAR_PROJECT_ROOT": str(root), "DCAR_V8_DB": str(fixture.database), "DCAR_WRITER_LOCK": str(external)}), patch.object(
                    pipeline_cutover, "connect", side_effect=AssertionError("cross-checkout call must not access DB")) as connection:
                self.assertTrue(pipeline_cutover._cli_database_is_formal(database))
                with self.assertRaisesRegex(pipeline_cutover.PublicationEvidenceError,
                        "cutover_formal_writer_(root_mismatch|lock_config_mismatch)"):
                    pipeline_cutover.main(["--db", str(database), "--project-root", str(root), "--activate-only"])
                connection.assert_not_called()
                self.assertEqual(fixture.database.read_bytes(), before)
                self.assertFalse((other / "runtime/writer-worker.lock").exists())

    def test_separate_offline_fixture_remains_usable_with_another_installed_writer(self):
        installed = create_writer_fixture(self.root / "installed-fixture")
        offline = create_writer_fixture(
            self.root / "offline-fixture",
            legacy_schema18=True,
            preparation=False,
            report=False,
        )
        home, _plist, external = self.installed_writer_fixture(installed)
        (offline.project / "runtime").mkdir(exist_ok=True)
        before = installed.database.read_bytes()
        handle = os.open(external, os.O_RDWR)
        output = io.StringIO()
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(pipeline_cutover.pwd, "getpwuid", return_value=SimpleNamespace(pw_dir=str(home))), redirect_stdout(output):
                self.assertFalse(pipeline_cutover._cli_database_is_formal(offline.database))
                self.assertEqual(pipeline_cutover.main(["--db", str(offline.database), "--project-root", str(offline.project),
                    "--isolated-candidate", "--activate-only", "--at", iso(offline.now)]), 0)
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)
            os.close(handle)
        self.assertEqual(json.loads(output.getvalue())["activation"]["run_id"], offline.activation_id)
        self.assertEqual(installed.database.read_bytes(), before)
        self.assertTrue((offline.project / "runtime/writer-worker.lock").is_file())


if __name__ == "__main__":
    unittest.main()
