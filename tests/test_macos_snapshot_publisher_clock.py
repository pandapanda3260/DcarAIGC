"""Run exact current timestamp/terminal functions against isolated SQLite fixtures.

AST extraction avoids importing source modules with unrelated import side effects.
Validation outside the time boundary uses deterministic fixture adapters. No live
HTTP, provider, production database, installed source, or service is touched.
"""
from __future__ import annotations

import ast
from contextlib import closing, nullcontext
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from zoneinfo import ZoneInfo

SOURCE = Path(__file__).resolve().parents[1]
PUBLISHER = SOURCE / "deploy/macos/publish_snapshot.py"
CUTOVER = SOURCE / "src/dcar_eval/v8/pipeline_cutover.py"
SHANGHAI = ZoneInfo("Asia/Shanghai")
START = datetime(2026, 9, 11, 18, 26, 2, tzinfo=SHANGHAI)


def stamp(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def function_source(path, name):
    text = path.read_text()
    tree = ast.parse(text)
    node = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == name)
    return ast.get_source_segment(text, node) + "\n"


class SnapshotPublishError(RuntimeError):
    pass


class PublicationEvidenceError(ValueError):
    pass


class Clock(datetime):
    value = START

    @classmethod
    def now(cls, tz=None):
        return cls.value.astimezone(tz)


def run_case(name, *, explicit_now=False, commit_at_first_read=False,
             forged_future=False, changed_after_snapshot=False, subsecond=False):
    Clock.value = START
    with tempfile.TemporaryDirectory(prefix="publication-time-race-") as temporary:
        root = Path(temporary)
        database = root / "fixture.sqlite3"
        writer = sqlite3.connect(database)
        writer.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA user_version=21;
            CREATE TABLE scheduler_runs(id INTEGER,job_id TEXT,scheduled_for TEXT,status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT);
            CREATE TABLE scheduler_run_attempts(id INTEGER,scheduler_run_id INTEGER,attempt_number INTEGER,status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT);
            CREATE TABLE content_items(id INTEGER,published_at TEXT,updated_at TEXT);
            CREATE TABLE content_metric_observations(id INTEGER,captured_at TEXT);
            CREATE TABLE account_metric_observations(id INTEGER,captured_at TEXT);
            CREATE TABLE report_tasks(id TEXT,task_status TEXT,progress INTEGER,message TEXT,updated_at TEXT,period_start TEXT);
        """)
        scheduled = "2026-09-11T08:00:00+08:00"
        started = stamp(START - timedelta(seconds=1))
        details = json.dumps({"task_id": "fixture-report", "task_status": "partial"})
        writer.execute("INSERT INTO scheduler_runs VALUES(1,'daily_report',?,'running',?,NULL,?)", (scheduled, started, details))
        writer.execute("INSERT INTO scheduler_run_attempts VALUES(1,1,1,'running',?,NULL,?)", (started, details))
        writer.commit()
        events = []

        def finish(seconds=11):
            completed = stamp(START + timedelta(seconds=seconds))
            for table in ("scheduler_runs", "scheduler_run_attempts"):
                writer.execute(f"UPDATE {table} SET status='partial',completed_at=?", (completed,))
            writer.commit()
            events.append({"event": "writer_commit", "completed_at": completed})
            return completed

        metadata = database.stat()
        identity = {"path": str(database), "inode": metadata.st_ino}
        runtime = {"schema": 21}
        lock = {"held": True}
        contract = {"fixture": True}

        def fetch(url):
            events.append({"event": "fixture_fetch", "endpoint": url.rsplit("/", 1)[-1]})
            if url.endswith("health"):
                return {"status": "ok", "database": database.name,
                        "runtime_database_identity": {**identity, "access_mode": "writer"},
                        "database_state": {"runtime_identity": runtime}, "writer_lock": lock,
                        "snapshot_contract": contract}
            if not commit_at_first_read:
                finish(seconds=50 if forged_future else (0.2 if subsecond else 11))
                Clock.value = START + timedelta(seconds=0.3 if subsecond else 12)
            return {"requested": True, "enabled": True, "writer_lock": lock,
                    "reconcile_from": "2026-09-01"}

        def schema(connection, **_):
            if commit_at_first_read:
                finish()
                Clock.value = START + timedelta(seconds=12)
            assert connection.execute("PRAGMA user_version").fetchone()[0] == 21
            events.append({"event": "sqlite_read_snapshot_established", "clock": stamp(Clock.value)})

        def runtime_identity(connection, **_):
            # This is a real database read inside the already opened transaction.
            assert connection.execute("SELECT COUNT(*) FROM scheduler_runs").fetchone()[0] == 1
            if changed_after_snapshot:
                finish(seconds=20)
                Clock.value = START + timedelta(seconds=21)
            return runtime

        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        def digest(value):
            return hashlib.sha256(canonical(value).encode()).hexdigest()

        env = dict(sqlite3=sqlite3, datetime=Clock, date=date, time=time, timedelta=timedelta,
                   timezone=timezone, SHANGHAI=SHANGHAI, Path=Path, json=json, hashlib=hashlib,
                   SnapshotPublishError=SnapshotPublishError, PublicationEvidenceError=PublicationEvidenceError,
                   PACKAGE_ROOT=root / "src/dcar_eval", EXPECTED_DATABASE_SCHEMA_VERSION=21,
                   TERMINAL_REPORT_STATUSES=frozenset({"succeeded", "partial"}), FRESHNESS_SCHEMA="fixture",
                   _default_fetch_json=fetch, _require_regular_local_file=lambda path, **_: path,
                   _file_identity=lambda *_args, **_kwargs: identity,
                   _validate_runtime_database_identity=lambda value, **_: value,
                   _validate_runtime_identity=lambda value, **_: value,
                   _validate_snapshot_contract=lambda value, **_: value,
                   _validate_writer_lock_observation=lambda value, **_: value,
                   configure_connection_safety=lambda _: None,
                   require_schema_compatibility=schema, _database_runtime_identity=runtime_identity,
                   WriterFreshness=lambda **kwargs: kwargs, canonical=canonical, digest=digest,
                   parse_time=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
                   durable_runs=SimpleNamespace(CONTRACT_VERSION="durable-run-v1"),
                   _account_classification_publication_evidence=lambda _: None,
                   _account_intake_publication_evidence=lambda _: None)
        source = function_source(CUTOVER, "terminal_run")
        exec(compile("from __future__ import annotations\n" + source, str(CUTOVER), "exec"), env)
        pipeline = SimpleNamespace(
            terminal_run=env["terminal_run"], digest=digest,
            runtime_evidence=lambda *_args, **_kwargs: {"coverage": {"complete": False}},
            PublicationEvidenceError=PublicationEvidenceError,
            report_dependency=lambda *_args, **_kwargs: {
                "status": "partial", "creation_source": "automatic", "task_type": "daily",
                "period_start": "2026-09-10", "period_end": "2026-09-10", "cutoff_at": scheduled,
            },
        )
        env["pipeline_cutover"] = pipeline
        for function in ("_parse_iso", "_reconcile_from", "_run_observation", "_report_run_dependency", "_observed_publication_evidence"):
            exec(compile("from __future__ import annotations\n" + function_source(PUBLISHER, function), str(PUBLISHER), "exec"), env)

        def evidence(connection, *, current, at, project_root):
            events.append({"event": "publication_at", "value": at})
            return env["_observed_publication_evidence"](connection, current=current, at=at,
                                                        project_root=project_root, boundary=date(2026, 9, 1))

        env["_schema20_publication_evidence"] = evidence
        source = function_source(PUBLISHER, "check_writer_freshness")
        exec(compile("from __future__ import annotations\n" + source, str(PUBLISHER), "exec"), env)
        try:
            result = env["check_writer_freshness"](
                database, now=START if explicit_now else None, maximum_content_lag_days=1,
                project_root=root, fetch_json=fetch, expected_user_version=21,
            )
            outcome = {"status": "accepted", "verified_at": result["evidence"]["verified_at"],
                       "report_completed_at": result["evidence"]["report_observations"][0]["run"]["completed_at"]}
        except SnapshotPublishError as error:
            outcome = {"status": "rejected", "error": str(error)}
        writer.close()
        return {"name": name, "explicit_old_now": explicit_now,
                "events": events, **outcome}



class PublisherReadClockTest(unittest.TestCase):
    def test_commit_during_http_is_not_future(self):
        result = run_case("http-race")
        self.assertEqual(result["status"], "accepted", result)
        self.assertEqual(result["verified_at"], stamp(START + timedelta(seconds=12)))

    def test_explicit_fixture_now_remains_fixed(self):
        result = run_case("explicit-now", explicit_now=True)
        self.assertEqual(result["status"], "rejected", result)
        self.assertTrue(result["error"].endswith("publication_run_time_invalid"))

    def test_commit_after_begin_before_first_database_read_is_included(self):
        result = run_case("first-read", commit_at_first_read=True)
        self.assertEqual(result["status"], "accepted", result)

    def test_forged_future_completion_is_rejected(self):
        result = run_case("future", forged_future=True)
        self.assertEqual(result["status"], "rejected", result)
        self.assertTrue(result["error"].endswith("publication_run_time_invalid"))

    def test_later_commit_is_excluded_by_read_snapshot(self):
        result = run_case("snapshot", changed_after_snapshot=True)
        self.assertEqual(result["status"], "accepted", result)
        self.assertEqual(result["report_completed_at"], stamp(START + timedelta(seconds=11)))

    def test_same_second_completion_preserves_subsecond_cutoff(self):
        result = run_case("subsecond", subsecond=True)
        self.assertEqual(result["status"], "accepted", result)
        self.assertEqual(result["verified_at"], stamp(START + timedelta(seconds=0.3)))
        self.assertEqual(result["report_completed_at"], stamp(START + timedelta(seconds=0.2)))


class StopBeforeSnapshotValidation(Exception):
    pass


def run_entrypoint(*, explicit_now=False, cross_at=None, direct=False):
    """Execute real automatic and new-publish entrypoints through the builder.

    Only service observations, retention and build/transport boundaries are
    fixture callbacks. The callbacks capture call order, clocks and day checks.
    """
    Clock.value = START
    trace = []
    with tempfile.TemporaryDirectory(prefix="publisher-entry-clock-") as temporary:
        project = Path(temporary)
        config = SimpleNamespace(snapshot_root=project / "snapshots", expected_user_version=21)
        next_day = START + timedelta(days=1)
        observations = []

        def observe(_database, *, now, **_kwargs):
            index = len(observations) + 1
            observations.append(now)
            trace.append("observe-" + str(index))
            if cross_at == index:
                Clock.value = next_day
            else:
                Clock.value = START + timedelta(seconds=index * 11)
            day = (now or Clock.value).date().isoformat()
            return SimpleNamespace(freshness=SimpleNamespace(evidence={"beijing_date": day}, runtime_identity={}))

        def builder(**kwargs):
            trace.append("build")
            Clock.value = next_day if cross_at == "build" else START + timedelta(seconds=40.3)
            return {}

        def verify(*_args, **kwargs):
            trace.append("verify")
            trace.append({"snapshot_clock": kwargs["current"].isoformat()})
            raise StopBeforeSnapshotValidation()

        env = dict(datetime=Clock, date=date, timezone=timezone, SHANGHAI=SHANGHAI,
                   subprocess=subprocess, nullcontext=nullcontext, SnapshotPublishError=SnapshotPublishError,
                   _default_fetch_json=lambda _: None, _require_current_config=lambda _: None,
                   AUTOMATIC_START_HOUR=9, _publisher_lock=lambda _: nullcontext(),
                   _read_pending_state=lambda _: None, _daily_automatic_success=lambda *_a, **_k: None,
                   _observe_formal_read_source=observe,
                   _prune_local_snapshots=lambda *_a, **_k: trace.append("prune"),
                   _require_regular_local_file=lambda path, **_: path, _verify_local_snapshot=verify)
        for function in ("publish_snapshot", "publish_snapshot_automatically"):
            exec(compile("from __future__ import annotations\n" + function_source(PUBLISHER, function), str(PUBLISHER), "exec"), env)
        entry = env["publish_snapshot" if direct else "publish_snapshot_automatically"]
        try:
            entry(project_root=project, database=project / "fixture.sqlite3", legacy_database=None,
                  config=config, now=START if explicit_now else None, build_snapshot=builder)
        except StopBeforeSnapshotValidation:
            status = "reached_snapshot_validation"
        except SnapshotPublishError as error:
            status = str(error)
        return {"status": status, "trace": trace, "observations": observations}


class PublisherEntryClockTest(unittest.TestCase):
    def test_automatic_keeps_live_clock_for_both_observations_and_post_build(self):
        result = run_entrypoint()
        self.assertEqual(result["observations"], [None, None])
        self.assertEqual(result["trace"][:5], ["observe-1", "observe-2", "prune", "build", "verify"])
        self.assertEqual(result["trace"][-1], {"snapshot_clock": (START + timedelta(seconds=40.3)).isoformat()})

    def test_explicit_clock_is_preserved_across_all_calls(self):
        result = run_entrypoint(explicit_now=True)
        self.assertEqual(result["observations"], [START, START])
        self.assertEqual(result["trace"][-1], {"snapshot_clock": START.isoformat()})

    def test_first_observation_cross_day_stops_before_prune_and_build(self):
        result = run_entrypoint(cross_at=1)
        self.assertEqual(result["status"], "snapshot crossed the publication business day")
        self.assertEqual(result["trace"], ["observe-1"])

    def test_second_observation_cross_day_stops_before_prune_and_build(self):
        result = run_entrypoint(cross_at=2)
        self.assertEqual(result["status"], "snapshot crossed the publication business day")
        self.assertEqual(result["trace"], ["observe-1", "observe-2"])

    def test_direct_publish_cross_day_stops_before_build(self):
        result = run_entrypoint(cross_at=1, direct=True)
        self.assertEqual(result["status"], "snapshot crossed the publication business day")
        self.assertEqual(result["trace"], ["observe-1"])

    def test_build_cross_day_stops_before_snapshot_validation_and_transport(self):
        result = run_entrypoint(cross_at="build")
        self.assertEqual(result["status"], "snapshot crossed the publication business day")
        self.assertEqual(result["trace"], ["observe-1", "observe-2", "prune", "build"])


@dataclass
class FixtureFreshness:
    evidence: dict
    runtime_identity: dict


def detached_clock_case(*, future=False, resume=False, crosses_day=False):
    with tempfile.TemporaryDirectory(prefix="publisher-detached-clock-") as temporary:
        root = Path(temporary)
        database = root / "snapshot.sqlite3"
        connection = sqlite3.connect(database)
        connection.execute("CREATE TABLE scheduler_run_attempts(id INTEGER,scheduler_run_id INTEGER,attempt_number INTEGER,status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT)")
        completed = stamp(START + timedelta(seconds=1 if future else 0.2))
        started = stamp(START - timedelta(seconds=1))
        connection.execute("INSERT INTO scheduler_run_attempts VALUES(1,1,1,'partial',?,?,'{}')", (started, completed))
        connection.commit()
        connection.close()
        row = dict(id=1, job_id="daily_report", status="partial", started_at=started,
                   completed_at=completed, details_json="{}")
        prior = FixtureFreshness({"mode": "observed", "verified_at": stamp(START), "reconcile_from": "2026-09-01"}, {})
        evidence_seen = []

        def canonical(value):
            return json.dumps(value, sort_keys=True, separators=(",", ":"))

        env = dict(sqlite3=sqlite3, datetime=datetime, date=date, timezone=timezone, SHANGHAI=SHANGHAI,
                   Path=Path, json=json, closing=closing, replace=replace,
                   SnapshotPublishError=SnapshotPublishError, PublicationEvidenceError=PublicationEvidenceError,
                   SNAPSHOT_ID_RE=re.compile(r"[0-9]{8}T[0-9]{6}Z-[0-9a-f]{12}"),
                   _validate_runtime_identity=lambda value, **_: value, _validate_manifest_contract=lambda *_a, **_k: None,
                   _snapshot_database_manifest_item=lambda _: {"bundle_path": database.name, "sha256": "a" * 64, "byte_size": 1},
                   configure_connection_safety=lambda _: None, canonical=canonical,
                   digest=lambda value: hashlib.sha256(canonical(value).encode()).hexdigest(),
                   parse_time=lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")),
                   durable_runs=SimpleNamespace(CONTRACT_VERSION="durable-run-v1"))
        exec(compile("from __future__ import annotations\n" + function_source(CUTOVER, "terminal_run"), str(CUTOVER), "exec"), env)

        def observe(connection, *, at, **_kwargs):
            env["terminal_run"](connection, row, at=at)
            return {"verified_at": at}

        def dependencies(_output, _manifest, freshness, **_kwargs):
            evidence_seen.append(freshness.evidence)
            raise StopBeforeSnapshotValidation()

        env.update(pipeline_cutover=SimpleNamespace(verify_file=lambda *_a, **_k: None),
                   _schema20_publication_evidence=observe, _verify_snapshot_dependencies=dependencies)
        for function in ("_parse_iso", "_reconcile_from", "_verify_local_snapshot"):
            exec(compile("from __future__ import annotations\n" + function_source(PUBLISHER, function), str(PUBLISHER), "exec"), env)
        try:
            env["_verify_local_snapshot"](
                root, {"runtime_identity": {}, "snapshot_id": "20260911T102602Z-" + "a" * 12,
                       "created_at": stamp(START + timedelta(days=1 if crosses_day else 0))},
                current=START + timedelta(seconds=0.3), config=SimpleNamespace(expected_user_version=21),
                project_root=root, fetch_json=lambda _: None, sealed_freshness=prior, freeze_observed=not resume,
            )
        except StopBeforeSnapshotValidation:
            return evidence_seen[0]


class PublisherDetachedClockTest(unittest.TestCase):
    def test_subsecond_post_backup_cutoff_is_preserved(self):
        self.assertEqual(detached_clock_case(), {"verified_at": stamp(START + timedelta(seconds=0.3))})

    def test_future_snapshot_run_is_still_rejected(self):
        with self.assertRaisesRegex(PublicationEvidenceError, "publication_run_time_invalid"):
            detached_clock_case(future=True)

    def test_resume_keeps_sealed_evidence_without_resampling(self):
        self.assertEqual(detached_clock_case(resume=True)["verified_at"], stamp(START))

    def test_snapshot_manifest_cross_day_remains_rejected(self):
        with self.assertRaisesRegex(SnapshotPublishError, "snapshot crossed the publication business day"):
            detached_clock_case(crosses_day=True)


if __name__ == "__main__":
    unittest.main()
