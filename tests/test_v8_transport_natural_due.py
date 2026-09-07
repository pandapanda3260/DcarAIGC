from __future__ import annotations

import hashlib
import json
import unittest
from datetime import timedelta

from tests import test_v8_pipeline as pipeline_fixtures
from v8 import durable_runs, pipeline, providers, tikhub_scan
from v8.comment_paging import cursor_sha256, page_window_key
from v8.provider_budget import PaidScope, freeze_scope
from v8.source_routing import metric_cycle_key, parse_time
from v8.storage import connect, transaction
from v8.transport_natural_due import (
    NaturalDueError,
    validate_natural_due_request,
)


AT = pipeline_fixtures.AT
LATER = pipeline_fixtures.LATER
REFERENCE = "MS4wLjAB" + "r" * 40


class TransportNaturalDueTest(unittest.TestCase):
    # Bind the established real temp-DB fixture without inheriting its suite.
    setUp = pipeline_fixtures.PipelineTest.setUp
    account_row = pipeline_fixtures.PipelineTest.account_row
    roster = pipeline_fixtures.PipelineTest.roster
    activate = pipeline_fixtures.PipelineTest.activate
    content = pipeline_fixtures.PipelineTest.content
    round_identity = pipeline_fixtures.PipelineTest.round_identity
    activation_identity = pipeline_fixtures.PipelineTest.activation_identity
    claim_round = pipeline_fixtures.PipelineTest.claim_round

    def _claim_queue(self, kind: str, content: dict) -> tuple[object, dict, PaidScope]:
        with connect(self.db) as connection:
            active = pipeline.activation(connection, at=AT)
        self.assertIsNotNone(active)
        assert active is not None
        item = {
            "id": int(content["id"]),
            "identity_id": int(self.identity_id),
            "historical": False,
        }
        if kind == "metrics_backfill":
            item["cycle_key"] = metric_cycle_key(
                int(content["id"]), content["published_at"], as_of=AT
            )
        elif kind == "comments_refresh":
            item["comment_as_of"] = (
                parse_time(AT).astimezone(pipeline.BEIJING).date().isoformat()
            )
        identity = {
            "pipeline_version": pipeline.PIPELINE_VERSION,
            "kind": kind,
            "roster_snapshot_id": int(active["roster_snapshot_id"]),
            "roster_snapshot_hash": str(active["roster_snapshot_hash"]),
            "created_for": AT,
            "candidate_ids": [int(content["id"])],
            "activation_id": int(active["activation_id"]),
            "activation_sha256": str(active["activation_sha256"]),
            "profile_id": str(active["profile_id"]),
        }
        claim = durable_runs.claim_run(
            kind,
            identity,
            db_path=self.db,
            now=AT,
            initial_checkpoint={
                "pending_ids": [int(content["id"])],
                "items": [item],
                "results": {},
                "complete": False,
            },
        )
        self.assertIsNotNone(claim)
        assert claim is not None
        stage = {
            "content_pipeline": "detail",
            "metrics_backfill": "metrics",
            "comments_refresh": "comments",
        }[kind]
        scope = self._scope(
            claim,
            identity,
            purpose=stage,
            stage=stage,
            content_id=int(content["id"]),
            account_id=None,
            business_day=parse_time(AT)
            .astimezone(pipeline.BEIJING)
            .date()
            .isoformat(),
        )
        return claim, identity, scope

    def _scope(
        self,
        claim,
        identity: dict,
        *,
        purpose: str,
        stage: str,
        content_id: int | None,
        account_id: int | None,
        business_day: str,
    ) -> PaidScope:
        base = PaidScope(
            purpose=purpose,
            activation_id=int(identity["activation_id"]),
            roster_snapshot_id=int(identity["roster_snapshot_id"]),
            roster_snapshot_hash=str(identity["roster_snapshot_hash"]),
            scheduler_run_id=int(claim.scheduler_run_id),
            scheduler_attempt_id=int(claim.attempt_id),
            business_day=business_day,
        )
        with connect(self.db) as connection:
            return freeze_scope(
                connection,
                content_id=content_id,
                account_id=account_id,
                stage=stage,
                scope=base,
            )

    def _validate(self, scope: PaidScope, request, *, stage: str, at: str = AT) -> dict:
        with connect(self.db) as connection:
            before = connection.total_changes
            receipt = validate_natural_due_request(
                connection,
                scope=scope,
                request_identity=request,
                stage=stage,
                at=at,
            )
            self.assertEqual(connection.total_changes, before)
            return receipt

    def _assert_blocked(
        self, scope: PaidScope, request, *, stage: str, at: str = AT
    ) -> NaturalDueError:
        with connect(self.db) as connection:
            before = connection.total_changes
            with self.assertRaises(NaturalDueError) as caught:
                validate_natural_due_request(
                    connection,
                    scope=scope,
                    request_identity=request,
                    stage=stage,
                    at=at,
                )
            self.assertEqual(connection.total_changes, before)
        return caught.exception

    def test_detail_receipt_is_stable_full_request_and_rejects_forged_params_time(self):
        self.activate()
        content = self.content()
        claim, _identity, scope = self._claim_queue("content_pipeline", content)
        content_key = str(content["platform_content_id"])
        request = providers._paid_request_identity(
            operation="douyin_video_detail",
            platform="douyin",
            subject=content_key,
            params={"aweme_id": content_key},
            cursor=None,
            due_bucket="lifetime",
        )

        first = self._validate(scope, request, stage="detail")
        second = self._validate(scope, request, stage="detail")

        self.assertEqual(first, second)
        self.assertEqual(first["scheduled_for"], AT)
        self.assertTrue(first["source_scheduled_for"].startswith("scan:"))
        self.assertEqual(first["source_run_id"], claim.scheduler_run_id)
        self.assertEqual(first["request_document"], request.document)
        self.assertEqual(first["request_document_sha256"], request.scope_identity)
        self.assertEqual(first["paid_scope_identity"], request.scope_identity)
        self.assertEqual(first["sequence"], 0)
        self.assertEqual(first["proof"]["frozen_item"]["id"], content["id"])

        forged = providers._paid_request_identity(
            operation="douyin_video_detail",
            platform="douyin",
            subject=content_key,
            params={"aweme_id": "different-content"},
            cursor=None,
            due_bucket="lifetime",
        )
        self.assertEqual(
            self._assert_blocked(scope, forged, stage="detail").code,
            "natural_due_request_mismatch",
        )
        future_at = (
            parse_time(AT) - timedelta(seconds=1)
        ).isoformat().replace("+00:00", "Z")
        self.assertEqual(
            self._assert_blocked(scope, request, stage="detail", at=future_at).code,
            "natural_due_future",
        )
        next_day = (
            parse_time(AT) + timedelta(days=1)
        ).isoformat().replace("+00:00", "Z")
        self.assertEqual(
            self._assert_blocked(scope, request, stage="detail", at=next_day).code,
            "natural_due_day_expired",
        )

    def test_metrics_group_is_derived_from_frozen_cycle(self):
        self.activate()
        content = self.content()
        _claim, identity, scope = self._claim_queue("metrics_backfill", content)
        with connect(self.db) as connection:
            item = json.loads(
                connection.execute(
                    "SELECT details_json FROM scheduler_runs WHERE id=?",
                    (scope.scheduler_run_id,),
                ).fetchone()[0]
            )["checkpoint"]["items"][0]
        cycle = str(item["cycle_key"])
        content_key = str(content["platform_content_id"])
        request = providers._paid_request_identity(
            operation="douyin_video_detail",
            platform="douyin",
            subject=content_key,
            params={"aweme_id": content_key},
            cursor=None,
            due_bucket=f"{cycle}:detail_counts",
        )

        receipt = self._validate(scope, request, stage="metrics")

        self.assertEqual(receipt["proof"]["target"]["group"], "detail_counts")
        self.assertEqual(receipt["proof"]["frozen_item"]["cycle_key"], cycle)
        self.assertEqual(receipt["scope_identity"]["activation_id"], identity["activation_id"])

    def _insert_comment_page(self, content: dict, week: str, cursor: dict) -> None:
        first_window = page_window_key(week, None)
        with connect(self.db) as connection, transaction(connection):
            slot = connection.execute(
                "INSERT INTO fetch_slots(content_id,stage,window_key,provider,adapter_version,"
                "status,created_at,updated_at) VALUES (?,'comments',?,'TikHub','fixture',"
                "'succeeded',?,?)",
                (content["id"], first_window, AT, AT),
            )
            attempt = connection.execute(
                "INSERT INTO fetch_attempts(slot_id,attempt_number,request_started_at,"
                "response_finished_at,http_status,billed,amount,currency) "
                "VALUES (?,1,?,?,200,1,0.001,'USD')",
                (slot.lastrowid, AT, AT),
            )
            raw = connection.execute(
                "INSERT INTO provider_raw_responses(fetch_attempt_id,content_id,provider,"
                "operation,local_path,sha256,byte_size,http_status,captured_at,source) "
                "VALUES (?,?,'TikHub','douyin_video_comments',? ,?,2,200,?,'live_applied')",
                (
                    attempt.lastrowid,
                    content["id"],
                    f"fixture-comment-{content['id']}.json",
                    hashlib.sha256(b"{}").hexdigest(),
                    AT,
                ),
            )
            run = connection.execute(
                "INSERT INTO comment_capture_runs(content_id,window_key,provider,adapter_version,"
                "status,started_at,created_at,updated_at) VALUES (?,?,'TikHub','fixture',"
                "'running',?,?,?)",
                (content["id"], week, AT, AT, AT),
            )
            connection.execute(
                "INSERT INTO comment_capture_pages(capture_run_id,page_number,request_cursor_json,"
                "request_cursor_sha256,next_cursor_json,next_cursor_sha256,fetch_slot_id,"
                "raw_response_id,has_more,captured_at) VALUES (?,1,'null',?,?,?,?,?,1,?)",
                (
                    run.lastrowid,
                    cursor_sha256(None),
                    json.dumps(cursor, sort_keys=True),
                    cursor_sha256(cursor),
                    slot.lastrowid,
                    raw.lastrowid,
                    AT,
                ),
            )

    def test_comments_use_persisted_next_cursor_and_reject_self_consistent_forgery(self):
        self.activate()
        content = self.content()
        day = parse_time(AT).astimezone(pipeline.BEIJING).date()
        iso = day.isocalendar()
        week = f"{iso.year}-W{iso.week:02d}"
        cursor = {"cursor": 20}
        self._insert_comment_page(content, week, cursor)
        _claim, _identity, scope = self._claim_queue("comments_refresh", content)
        content_key = str(content["platform_content_id"])
        request = providers._paid_request_identity(
            operation="douyin_video_comments",
            platform="douyin",
            subject=content_key,
            params={"aweme_id": content_key, "cursor": 20, "count": 20},
            cursor=cursor,
            due_bucket=page_window_key(week, cursor),
        )

        receipt = self._validate(scope, request, stage="comments")

        self.assertEqual(receipt["proof"]["cursor"], cursor)
        self.assertEqual(receipt["proof"]["previous_page_number"], 1)
        forged_cursor = {"cursor": 999}
        forged = providers._paid_request_identity(
            operation="douyin_video_comments",
            platform="douyin",
            subject=content_key,
            params={"aweme_id": content_key, "cursor": 999, "count": 20},
            cursor=forged_cursor,
            due_bucket=page_window_key(week, forged_cursor),
        )
        self.assertEqual(
            self._assert_blocked(scope, forged, stage="comments").code,
            "natural_due_request_mismatch",
        )

    def test_account_profile_is_due_from_round_and_current_freshness(self):
        snapshot = self.activate()
        identity = self.round_identity(snapshot, key="matrix_account_metrics", at=AT)
        claim = self.claim_round(identity, at=AT)
        scope = self._scope(
            claim,
            identity,
            purpose="metrics",
            stage="discovery",
            content_id=None,
            account_id=int(self.account["id"]),
            business_day=str(identity["beijing_day"]),
        )
        uid = str(scope.uid)
        request = providers._paid_request_identity(
            operation="douyin_uid_profile",
            platform="douyin",
            subject=uid,
            params={"uid": uid},
            cursor=None,
            due_bucket=f"matrix-first:profile:{identity['beijing_day']}",
        )

        receipt = self._validate(scope, request, stage="discovery")

        self.assertEqual(receipt["scheduled_for"], identity["scheduled_at"])
        self.assertEqual(receipt["proof"]["kind"], "account_profile")
        self.assertEqual(receipt["proof"]["follower_evidence"]["freshness"], "unknown")

    def _scan_claims(self):
        snapshot = self.activate()
        parent_identity = self.round_identity(snapshot, key="tikhub_reconcile", at=AT)
        parent = self.claim_round(parent_identity, at=AT)
        child_identity = tikhub_scan._freeze(
            int(self.identity_id),
            window_start="2026-08-21T16:00:00Z",
            window_end="2026-08-28T16:00:00Z",
            purpose="reconcile",
            roster_snapshot_id=int(snapshot["id"]),
            roster_snapshot_hash=str(snapshot["members_sha256"]),
            db_path=self.db,
            task_id=None,
            task_max_amount=None,
            activation_id=int(parent_identity["activation_id"]),
            profile_id=str(parent_identity["profile_id"]),
            activation_sha256=str(parent_identity["activation_sha256"]),
            at=AT,
        )
        child = durable_runs.claim_run(
            "tikhub_reconcile",
            child_identity,
            db_path=self.db,
            now=AT,
            initial_checkpoint={
                "cursor": 0,
                "generation": 0,
                "page_number": 0,
                "counts": dict.fromkeys(tikhub_scan.DISPOSITIONS, 0),
                "raw_items": 0,
                "last_manifest": None,
                "pending_raw": None,
                "reference": None,
                "provider_transient": None,
                "provider_next_cursor": None,
                "completion_reason": None,
                "qualifying_old_page_count": 0,
                "pending_materialization": None,
                "complete": False,
            },
        )
        self.assertIsNotNone(child)
        assert child is not None
        return parent, parent_identity, child, child_identity

    def test_scan_reference_requires_running_natural_parent(self):
        _parent, parent_identity, child, child_identity = self._scan_claims()
        scope = self._scope(
            child,
            child_identity,
            purpose="reconcile",
            stage="discovery",
            content_id=None,
            account_id=int(self.account["id"]),
            business_day=str(parent_identity["beijing_day"]),
        )
        uid = str(scope.uid)
        window = f"reference:{scope.identity_id}:{tikhub_scan._digest(uid)}:v1"
        request = providers._paid_request_identity(
            operation="douyin_uid_profile",
            platform="douyin",
            subject=uid,
            params={"uid": uid},
            cursor=None,
            due_bucket=window,
            request_window={
                "start": child_identity["window_start"],
                "end": child_identity["window_end"],
            },
        )

        receipt = self._validate(scope, request, stage="discovery")

        self.assertEqual(
            receipt["proof"]["natural_parent"]["link_kind"],
            "running_first_dispatch",
        )
        self.assertEqual(
            receipt["scheduled_for"], parent_identity["scheduled_at"]
        )

    def test_scan_posts_resume_requires_parent_link_and_exact_cursor_window(self):
        parent, _parent_identity, child, child_identity = self._scan_claims()
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection,
                parent,
                {"child_run_ids": [child.scheduler_run_id]},
                now=AT,
            )
            durable_runs.checkpoint(
                connection, child, {"reference": REFERENCE}, now=AT
            )
            connection.execute(
                "INSERT INTO account_provider_references(account_identity_id,provider,"
                "reference_kind,reference_value,created_at,updated_at) "
                "VALUES (?,'TikHub','sec_user_id',?,?,?)",
                (self.identity_id, REFERENCE, AT, AT),
            )
        durable_runs.finish_run(
            child,
            status="partial",
            db_path=self.db,
            now=AT,
            next_resume_at=AT,
        )
        resumed = durable_runs.claim_run(
            "tikhub_reconcile", child_identity, db_path=self.db, now=LATER
        )
        self.assertIsNotNone(resumed)
        assert resumed is not None
        scope = self._scope(
            resumed,
            child_identity,
            purpose="reconcile",
            stage="discovery",
            content_id=None,
            account_id=int(self.account["id"]),
            business_day=parse_time(LATER)
            .astimezone(pipeline.BEIJING)
            .date()
            .isoformat(),
        )
        state = durable_runs.get_run(resumed.scheduler_run_id, db_path=self.db)[
            "details"
        ]["checkpoint"]
        window = tikhub_scan._page_key(resumed, state)
        request = providers._paid_request_identity(
            operation="douyin_user_posts",
            platform="douyin",
            subject=REFERENCE,
            params={
                "sec_user_id": REFERENCE,
                "max_cursor": 0,
                "count": 20,
                "sort_type": 0,
            },
            cursor=0,
            due_bucket=window,
            request_window={
                "start": child_identity["window_start"],
                "end": child_identity["window_end"],
            },
        )

        receipt = self._validate(scope, request, stage="discovery", at=LATER)

        self.assertEqual(
            receipt["proof"]["natural_parent"]["link_kind"],
            "existing_child_link",
        )
        forged = providers._paid_request_identity(
            operation="douyin_user_posts",
            platform="douyin",
            subject=REFERENCE,
            params={
                "sec_user_id": REFERENCE,
                "max_cursor": 0,
                "count": 20,
                "sort_type": 0,
            },
            cursor=0,
            due_bucket=window,
            request_window={
                "start": child_identity["window_start"],
                "end": "2026-08-28T15:59:59Z",
            },
        )
        self.assertEqual(
            self._assert_blocked(
                scope, forged, stage="discovery", at=LATER
            ).code,
            "natural_due_request_mismatch",
        )

    def test_pending_member_direct_and_history_boundaries_fail_closed(self):
        self.activate()
        content = self.content()
        claim, queue_identity, scope = self._claim_queue("content_pipeline", content)
        key = str(content["platform_content_id"])
        request = providers._paid_request_identity(
            operation="douyin_video_detail",
            platform="douyin",
            subject=key,
            params={"aweme_id": key},
            cursor=None,
            due_bucket="lifetime",
        )
        with connect(self.db) as connection, transaction(connection):
            durable_runs.checkpoint(
                connection, claim, {"pending_ids": []}, now=AT
            )
        self.assertEqual(
            self._assert_blocked(scope, request, stage="detail").code,
            "natural_due_not_pending",
        )

        direct_identity = {
            **queue_identity,
            "contract_version": "direct-paid-dispatch-owner-v1",
            "request_id": "manual-fixture",
            "purpose": "detail",
            "business_day": parse_time(AT)
            .astimezone(pipeline.BEIJING)
            .date()
            .isoformat(),
        }
        direct = durable_runs.claim_run(
            "paid_capture_direct", direct_identity, db_path=self.db, now=AT
        )
        self.assertIsNotNone(direct)
        assert direct is not None
        direct_scope = self._scope(
            direct,
            direct_identity,
            purpose="detail",
            stage="detail",
            content_id=int(content["id"]),
            account_id=None,
            business_day=str(direct_identity["business_day"]),
        )
        self.assertEqual(
            self._assert_blocked(direct_scope, request, stage="detail").code,
            "natural_due_source_rejected",
        )

        history_identity = {**queue_identity, "purpose": "history"}
        history = durable_runs.claim_run(
            "history_recovery", history_identity, db_path=self.db, now=AT
        )
        self.assertIsNotNone(history)
        assert history is not None
        history_scope = self._scope(
            history,
            history_identity,
            purpose="history",
            stage="detail",
            content_id=int(content["id"]),
            account_id=None,
            business_day=str(direct_identity["business_day"]),
        )
        self.assertEqual(
            self._assert_blocked(history_scope, request, stage="detail").code,
            "natural_due_history_excluded",
        )

        # The accepted roster remains immutable. Disabling its account legally
        # removes active eligibility while preserving the frozen owner/membership.
        fresh_content = self.content()
        _fresh_claim, _fresh_identity, fresh_scope = self._claim_queue(
            "content_pipeline", fresh_content
        )
        fresh_key = str(fresh_content["platform_content_id"])
        fresh_request = providers._paid_request_identity(
            operation="douyin_video_detail",
            platform="douyin",
            subject=fresh_key,
            params={"aweme_id": fresh_key},
            cursor=None,
            due_bucket="lifetime",
        )
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE accounts SET enabled=0 WHERE id=?",
                (fresh_scope.account_id,),
            )
        self.assertEqual(
            self._assert_blocked(
                fresh_scope, fresh_request, stage="detail"
            ).code,
            "natural_due_nonmember",
        )


if __name__ == "__main__":
    unittest.main()
