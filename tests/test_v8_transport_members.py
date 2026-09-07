from __future__ import annotations

import json
import plistlib
import unittest
from dataclasses import replace

from tests import test_v8_pipeline as pipeline_fixtures
from tests import test_v8_transport_campaign as campaign_fixtures
from tests import test_v8_transport_cohort as cohort_fixtures
from tests.test_v8_runtime_database import InstalledRuntimeFixture
from v8 import durable_runs, pipeline, providers, tikhub_scan
from v8.profile_activations import activation_at
from v8.provider_transport import RequestTransportBindingError
from v8.runtime_database import (
    RuntimeDatabaseError,
    acquire_writer_lock,
    load_installed_writer_contract,
    require_current_process_writer_lock,
    resolve_installed_database_access,
)
from v8.source_routing import parse_time
from v8.storage import connect, transaction
from v8.transport_due_candidates import list_primary_due_candidates
from v8.transport_hold_binding import TransportHoldBindingError
from v8.transport_members import (
    OPERATOR_JOB,
    DiagnosticMemberError,
    issue_primary_member_batch,
    primary_operator_identity,
    validate_primary_member_for_send,
)
from v8.transport_natural_due import NaturalDueError


AT = cohort_fixtures.FREEZE_AT


class TransportMembersTest(unittest.TestCase):
    _evidence = cohort_fixtures.TransportCohortTest._evidence
    _register_prerequisite = cohort_fixtures.TransportCohortTest._register_prerequisite
    _raw = cohort_fixtures.TransportCohortTest._raw
    _freeze = cohort_fixtures.TransportCohortTest._freeze
    _transport = campaign_fixtures.TransportCampaignTest._transport
    _campaign = campaign_fixtures.TransportCampaignTest._campaign
    claim_round = pipeline_fixtures.PipelineTest.claim_round

    def _roster(self, connection, *, count):
        return cohort_fixtures.TransportCohortTest._roster(self, connection, count=20)

    def _before_hold(self):
        # Equal actual entity sizes make every account qualify at the P75 boundary.
        for account_id in range(1, 21):
            self._raw(
                account_id=account_id, padding=1000, captured_at="2026-09-05T02:00:00Z"
            )

    def setUp(self):
        cohort_fixtures.TransportCohortTest.setUp(self)
        self.cohort = self._freeze()
        self.assertEqual(
            self.cohort["payload"]["selected_identity_ids"], list(range(1, 21))
        )
        self.transport = self._transport()
        self.campaign = self._campaign(self.cohort, self.transport)
        self.operator = durable_runs.claim_run(
            OPERATOR_JOB,
            primary_operator_identity(self.campaign),
            db_path=self.db,
            invocation_source="operator_retry",
            now=AT,
        )
        assert self.operator is not None
        self.children = {}

        # Install a real fixture contract around this already initialized DB.
        runtime = InstalledRuntimeFixture(self.root / "installed-runtime")
        runtime.database = self.db
        payload = plistlib.loads(runtime.plist.read_bytes())
        payload["EnvironmentVariables"]["DCAR_V8_DB"] = str(self.db)
        runtime.plist.write_bytes(plistlib.dumps(payload))
        installed = load_installed_writer_contract(home=runtime.home)
        assert installed is not None
        self.writer_access = resolve_installed_database_access(
            "writer",
            database=self.db,
            project_root=runtime.project,
            environ=runtime.environment,
            installed=installed,
        )

    def _natural_children(self, identity_ids, *, registration="tikhub_works_scan"):
        with connect(self.db) as connection:
            active = activation_at(connection, AT)
        assert active is not None
        local = parse_time(AT).astimezone(pipeline.BEIJING)
        scheduled = pipeline._scheduled_round_at(registration, local)
        assert scheduled is not None
        identity = {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "beijing_day": local.date().isoformat(),
            "round_id": f"{registration}:{scheduled.hour:02d}:{scheduled.minute:02d}",
            "registration_id": registration,
            "job_id": pipeline.CRON_ROUNDS[registration][0],
            "scheduled_at": pipeline._iso(scheduled),
            "roster_snapshot_id": active["roster_snapshot_id"],
            "roster_snapshot_hash": active["roster_members_sha256"],
            "activation_id": active["activation_id"],
            "activation_sha256": active["activation_sha256"],
            "profile_id": active["profile_id"],
            "eligible_identity_ids": list(identity_ids),
        }
        self.claim_round(identity, at=AT)
        start, end = (
            ("2026-08-06T16:00:00Z", "2026-09-05T16:00:00Z")
            if registration == "tikhub_works_scan"
            else ("2026-09-04T04:00:00Z", "2026-09-06T04:00:00Z")
        )
        for identity_id in identity_ids:
            frozen = tikhub_scan._freeze(
                identity_id,
                window_start=start,
                window_end=end,
                purpose="reconcile",
                roster_snapshot_id=active["roster_snapshot_id"],
                roster_snapshot_hash=active["roster_members_sha256"],
                db_path=self.db,
                task_id=None,
                task_max_amount=None,
                activation_id=active["activation_id"],
                profile_id=active["profile_id"],
                activation_sha256=active["activation_sha256"],
                at=AT,
            )
            reference = "MS4wLjAB" + "r" * 40 + str(identity_id)
            child = durable_runs.claim_run(
                "tikhub_reconcile",
                frozen,
                db_path=self.db,
                now=AT,
                initial_checkpoint={
                    "cursor": 0,
                    "generation": 0,
                    "page_number": 0,
                    "counts": dict.fromkeys(tikhub_scan.DISPOSITIONS, 0),
                    "raw_items": 0,
                    "reference": reference,
                    "pending_raw": None,
                    "pending_materialization": None,
                    "complete": False,
                },
            )
            assert child is not None
            self.children[child.scheduler_run_id] = child
            with connect(self.db) as connection, transaction(connection):
                connection.execute(
                    "INSERT OR IGNORE INTO account_provider_references(account_identity_id,provider,"
                    "reference_kind,reference_value,created_at,updated_at) "
                    "VALUES (?,'TikHub','sec_user_id',?,?,?)",
                    (identity_id, reference, AT, AT),
                )

    def _prepare(self, *, count=20, extra=False):
        self._natural_children(range(1, count + 1))
        if extra:
            self._natural_children([1], registration="tikhub_works_refresh")
        with connect(self.db) as connection:
            return list_primary_due_candidates(connection, at=AT)

    def _issue(self, connection):
        return issue_primary_member_batch(
            connection,
            campaign_receipt_id=self.campaign["receipt_id"],
            operator_claim=self.operator,
            at=AT,
            mirror_root=self.mirror_root,
        )

    def _verify(self, connection, member, candidate, **overrides):
        return validate_primary_member_for_send(
            connection,
            **{
                "member_receipt_id": member["receipt_id"],
                "operator_claim": self.operator,
                "scope": candidate["scope"],
                "request_identity": candidate["request_identity"],
                "request_transport": self.transport,
                "at": AT,
                **overrides,
            },
        )

    def _member_count(self, connection):
        return connection.execute(
            "SELECT COUNT(*) FROM scheduler_runs WHERE job_id='transport_receipt:member_permit'"
        ).fetchone()[0]

    def test_twenty_fixed_ranks_are_idempotent_and_verify_without_writes(self):
        candidates = self._prepare()
        self.assertEqual(len(candidates), 20)
        with (
            acquire_writer_lock(self.writer_access),
            connect(self.db) as connection,
            transaction(connection),
        ):
            members = self._issue(connection)
            self.assertEqual(
                [item["payload"]["rank"] for item in members], list(range(1, 21))
            )
            self.assertEqual(self._issue(connection), members)
            self.assertEqual(self._member_count(connection), 20)
            writer = require_current_process_writer_lock(connection)
            before = connection.total_changes
            for member, candidate in zip(members, candidates, strict=True):
                self.assertEqual(member["payload"]["natural_due"], candidate["proof"])
                self.assertEqual(member["payload"]["writer"], writer)
            self.assertEqual(
                self._verify(connection, members[0], candidates[0]), members[0]
            )
            with self.assertRaises(DiagnosticMemberError) as caught:
                self._verify(connection, members[1], candidates[1])
            self.assertEqual(caught.exception.code, "diagnostic_rank_not_due")
            self.assertEqual(connection.total_changes, before)
            for table in (
                "provider_usage",
                "fetch_attempts",
                "paid_provider_dispatch_events",
            ):
                self.assertEqual(
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0], 0
                )

    def test_missing_writer_or_insufficient_due_creates_zero_members(self):
        self.assertEqual(len(self._prepare(count=19)), 19)
        with connect(self.db) as connection, transaction(connection):
            with self.assertRaises(RuntimeDatabaseError):
                self._issue(connection)
            self.assertEqual(self._member_count(connection), 0)
        with (
            acquire_writer_lock(self.writer_access),
            connect(self.db) as connection,
            transaction(connection),
        ):
            with self.assertRaises(DiagnosticMemberError) as caught:
                self._issue(connection)
            self.assertEqual(
                caught.exception.code, "diagnostic_insufficient_natural_due"
            )
            self.assertEqual(self._member_count(connection), 0)

    def test_request_cursor_config_owner_and_hold_drift_fail_closed(self):
        candidate = self._prepare()[0]
        with acquire_writer_lock(self.writer_access):
            with connect(self.db) as connection, transaction(connection):
                member = self._issue(connection)[0]
                original = candidate["request_identity"].document
                forged = providers._paid_request_identity(
                    operation=original["operation"],
                    platform=original["platform"],
                    subject=original["subject"],
                    params={**original["request_parameters"], "max_cursor": 1},
                    cursor=1,
                    due_bucket=original["due_bucket"],
                    request_window=original["request_window"],
                )
                with self.assertRaises(NaturalDueError):
                    self._verify(connection, member, candidate, request_identity=forged)
                with self.assertRaises(durable_runs.LostOwnership):
                    self._verify(
                        connection,
                        member,
                        candidate,
                        operator_claim=replace(self.operator, owner_token="changed"),
                    )
                child = self.children[candidate["scope"].scheduler_run_id]
                durable_runs.checkpoint(connection, child, {"cursor": 1}, now=AT)
                with self.assertRaises(NaturalDueError):
                    self._verify(connection, member, candidate)
                durable_runs.checkpoint(connection, child, {"cursor": 0}, now=AT)
                self._transport("api.tikhub.io")
                with self.assertRaises(RequestTransportBindingError):
                    self._verify(connection, member, candidate)
                self._transport()
                self.assertEqual(self._verify(connection, member, candidate), member)
            self._register_prerequisite(
                "config", cohort_fixtures.NEW_CONFIG, at="2026-09-06T06:00:00Z"
            )
            with connect(self.db) as connection, transaction(connection):
                with self.assertRaises(TransportHoldBindingError):
                    self._verify(
                        connection, member, candidate, at="2026-09-06T06:01:00Z"
                    )
                self.assertEqual(self._member_count(connection), 20)

    def test_used_leading_scope_is_not_replaced_by_rank_twenty_one(self):
        candidates = self._prepare(extra=True)
        self.assertEqual(len(candidates), 21)
        leading = candidates[0]["request_identity"].scope_identity
        with (
            acquire_writer_lock(self.writer_access),
            connect(self.db) as connection,
            transaction(connection),
        ):
            connection.execute(
                "INSERT INTO provider_usage(provider,operation,request_attempts,currency,amount,recorded_at,details_json) "
                "VALUES ('TikHub','douyin_user_posts',1,'USD',0.001,?,?)",
                (
                    AT,
                    json.dumps(
                        {"paid_scope_identity": leading, "state": "billing_unknown"}
                    ),
                ),
            )
            self.assertEqual(list_primary_due_candidates(connection, at=AT), candidates)
            with self.assertRaises(DiagnosticMemberError) as caught:
                self._issue(connection)
            self.assertEqual(caught.exception.code, "diagnostic_scope_already_used")
            self.assertEqual(self._member_count(connection), 0)
