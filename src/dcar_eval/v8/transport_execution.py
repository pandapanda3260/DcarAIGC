"""Execute one frozen primary member through the existing scan materializer.

This writer-only primitive never chooses targets, prepares due work, changes a
route, replaces a failed rank, or retries a request. The campaign coordinator
must issue the complete fixed batch before calling it in rank order.
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Any

from apscheduler.schedulers.base import BaseScheduler  # type: ignore[import-untyped]

from . import capture, durable_runs, providers, tikhub_scan
from .account_roster import RosterError
from .paid_dispatch import dispatch_events
from .paid_identity import build_paid_request_identity
from .provider_budget import BudgetBlocked, PaidScope, paid_scope
from .storage import connect, now_utc, transaction
from .transport_authority import (
    authorize_diagnostic_request,
    current_diagnostic_request_binding,
    diagnostic_request_context,
)
from .transport_members import DiagnosticMemberError
from .transport_receipts import read_transport_receipt


def execute_continuity_member_page(permit_id: int, rank: int, *, db_path: Path,
                                   raw_root: Path, at: str | None = None, _native: bool = False) -> dict[str, Any]:
    """Execute one already frozen Mode B legacy page through normal A/B/C.

    No diagnostic HOLD, alternate owner, candidate work or retry is created.
    Local failure preserves the normal scanner's raw/materialization checkpoint
    for its existing local-only recovery path, never repurchases this member.
    """
    from . import capture_authorizations as auth, capture_release as release
    from .runtime_database import require_current_process_writer_lock

    timestamp = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        if _native:
            payload, proof, _ = release.native_member(connection, cohort_id=permit_id, rank=rank, at=timestamp)
            operation = payload["operation"]
        else:
            row = connection.execute("SELECT operation FROM transport_continuity_permits WHERE id=?", (permit_id,)).fetchone()
            if row is None or type(rank) is not int or not 1 <= rank <= 20:
                raise auth.AuthorizationError("Continuity permit or rank is invalid")
            operation = row["operation"]
            if operation not in release.CONTINUITY_OPERATIONS:
                raise auth.AuthorizationError("legacy_continuity_executor_unsupported")
            evidence = release._installed_evidence(connection, at=timestamp)
            permit, payload = release._permit(connection, operation=operation, at=timestamp, evidence=evidence)
            if permit["id"] != permit_id:
                raise auth.AuthorizationError("Continuity permit was superseded")
            member = connection.execute("SELECT request_scope_identity FROM transport_continuity_permit_members WHERE permit_id=? AND rank=?", (permit_id, rank)).fetchone()
            proof = release.validate_continuity_natural_request(connection, permit=permit,
                request_identity=member[0], at=timestamp)
        paid = PaidScope(**proof["scope_identity"])
        if proof["stage"] != "discovery":
            # Content work keeps its existing queue owner; never manufacture a
            # replacement queue or complete other fields/pages of that owner.
            content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (paid.content_id,)).fetchone())
        else:
            content = None
        source = connection.execute("SELECT details_json FROM scheduler_runs WHERE id=?", (paid.scheduler_run_id,)).fetchone()
        details = json.loads(source[0])
        owner, scope = details["owner"], details["identity"]
        claim = durable_runs.DurableClaim(scheduler_run_id=int(paid.scheduler_run_id or 0),
            attempt_id=int(owner["attempt_id"]), attempt_number=int(owner["attempt_number"]),
            owner_token=str(owner["token"]), scan_id=str(details["scan_id"]))
        if content is None:
            tikhub_scan._owned_scope(connection, claim, scope)
    if content is not None:
        with auth.runtime_authority(release.current_runtime_bindings):
            return _execute_continuity_content(permit_id, rank, proof=proof, payload=payload,
                content=content, paid=paid, db_path=db_path, raw_root=raw_root)
    window = proof["request_document"]["due_bucket"]
    # This reads and validates any committed manifest before purchasing.
    state = tikhub_scan._state(claim, scope, db_path)
    if tikhub_scan._page_key(claim, state) != window:
        raise auth.AuthorizationError("Natural continuity cursor changed")
    from .provider_transport import request_transport_context
    with auth.runtime_authority(release.current_runtime_bindings), request_transport_context(payload["request_transport"]):
        raw, replayed = tikhub_scan._raw(claim, scope, window_key=window, operation=operation,
            request={**scope, "reference": proof["request_document"]["subject"], "cursor": proof["request_document"]["cursor"]},
            db_path=db_path, raw_root=raw_root, call_override=None)
        if replayed:
            raise auth.AuthorizationError("Continuity cannot count an earlier raw replay as a new start")
        tikhub_scan._apply(claim, scope, raw, window_key=window, db_path=db_path, raw_root=raw_root, now=timestamp)
        pending = tikhub_scan._state(claim, scope, db_path).get("pending_materialization")
        if pending is None:
            raise tikhub_scan.TikHubScanError("materialization_integrity_error", "Continuity page has no local child")
        complete = tikhub_scan._materialize_pending(claim, scope, pending, db_path=db_path, raw_root=raw_root, now=timestamp)
        if complete:
            state = tikhub_scan._state(claim, scope, db_path)
            result = tikhub_scan._finish_success(claim, str(state["completion_reason"]), db_path=db_path, now=timestamp, pages=1)
        else:
            result = tikhub_scan._partial(claim, "continuity_page_yield", db_path=db_path, now=timestamp, pages=1)
    return {"permit_id": permit_id, "rank": rank, "raw_response_id": raw.raw_response_id,
            "materialized": True, "scan": result, "qualified": False}


def _execute_continuity_content(permit_id: int, rank: int, *, proof: dict[str, Any], payload: dict[str, Any],
                               content: dict[str, Any], paid: PaidScope, db_path: Path, raw_root: Path) -> dict[str, Any]:
    """One exact queue HTTP request; reuse normal parsers and page materializer."""
    from .paid_identity import build_paid_request_identity
    from .provider_transport import request_transport_context

    document = proof["request_document"]
    stage, operation, window = proof["stage"], proof["operation"], document["due_bucket"]
    source_stage = "detail" if operation in {"douyin_video_detail", "xiaohongshu_note_detail"} else stage
    request = build_paid_request_identity(provider=document["provider"], operation=operation, platform=document["platform"],
        subject=document["subject"], request_parameters=document["request_parameters"], cursor=document["cursor"],
        due_bucket=window, request_window=document["request_window"], sequence=0)
    provider, adapter, expected_operation, price = providers.STAGE_CONFIG[(content["platform"], source_stage)]
    if expected_operation != operation:
        raise ValueError("Continuity source operation differs from the existing adapter")
    key = providers._load_key(providers.TIKHUB_KEY_FILE, "TIKHUB_API_KEY")

    def call():
        if content["platform"] == "douyin":
            result = providers._douyin_call(source_stage, content["platform_content_id"], key, cursor=document["cursor"])
        else:
            result = providers._xhs_call(source_stage, content["platform_content_id"], key, content["content_type"], cursor=document["cursor"])
        if stage == "metrics" and source_stage == "detail":
            result = replace(result, data=dict(result.data["metrics"]))
        return result

    with paid_scope(str(paid.purpose), activation_id=paid.activation_id, roster_snapshot_id=paid.roster_snapshot_id,
            roster_snapshot_hash=paid.roster_snapshot_hash, scheduler_run_id=paid.scheduler_run_id,
            scheduler_attempt_id=paid.scheduler_attempt_id, business_day=paid.business_day), request_transport_context(payload["request_transport"]):
        budget_id = providers._budget_for_call(provider=provider, operation=operation, price=price,
            task_id=None, task_max_amount=None, db_path=db_path)
        outcome = capture.execute_content_fetch(content_id=content["id"], stage=stage, window_key=window,
            provider=provider, adapter_version=adapter, operation=operation, call=call, db_path=db_path,
            raw_root=raw_root, budget_id=budget_id, paid_request_identity=request, request_transport=payload["request_transport"])
        materialization = _materialize_continuity_content(content, proof, outcome, db_path=db_path)
    return {"permit_id": permit_id, "rank": rank, "raw_response_id": outcome.raw_response_id,
            "materialized": True, "materialization": materialization, "qualified": False}


def _materialize_continuity_content(content: dict[str, Any], proof: dict[str, Any], outcome: capture.CaptureOutcome,
                                   *, db_path: Path) -> dict[str, Any]:
    from . import comment_paging
    stage, window = proof["stage"], proof["request_document"]["due_bucket"]
    if stage != "comments":
        providers._store_stage_result(content, stage, window, outcome, db_path=db_path)
        return {"status": "succeeded", "stage": stage}
    def one_page(_page_number, cursor):
        if comment_paging.cursor_sha256(cursor) != comment_paging.cursor_sha256(proof["request_document"]["cursor"]):
            raise comment_paging.PageFetchDeferred("continuity_fixed_page_only")
        return comment_paging.PageFetch(raw_response_id=outcome.raw_response_id, fetch_slot_id=outcome.slot_id, result=outcome)
    provider, adapter, _, _ = providers.STAGE_CONFIG[(content["platform"], stage)]
    result = comment_paging.capture_content_comments(content, window_key=window.rsplit(":page:", 1)[0],
        page_fetcher=one_page, provider=provider, adapter_version=adapter, db_path=db_path, max_pages=1)
    providers._mark_raw_response_applied(outcome.raw_response_id, applied_source="live_applied", db_path=db_path)
    return result


def resume_continuity_member_local(permit_id: int, rank: int, *, db_path: Path,
                                  raw_root: Path, at: str | None = None, _native: bool = False) -> dict[str, Any]:
    """Resume the existing scanner child after C; never reenter A/B or next page."""
    from . import capture_authorizations as auth, capture_release as release
    from .runtime_database import require_current_process_writer_lock

    timestamp = at or now_utc()
    with connect(db_path) as connection, transaction(connection):
        require_current_process_writer_lock(connection)
        if _native:
            payload, proof, attempts = release.native_member(connection, cohort_id=permit_id, rank=rank, at=timestamp, local_replay=True)
        else:
            row = connection.execute("SELECT operation FROM transport_continuity_permits WHERE id=?", (permit_id,)).fetchone()
            if row is None or type(rank) is not int or not 1 <= rank <= 20:
                raise auth.AuthorizationError("Continuity permit or rank is invalid")
            evidence = release._installed_evidence(connection, at=timestamp, maintenance_only=True)
            permit, payload = release._permit(connection, operation=row["operation"], at=timestamp, evidence=evidence)
            if permit["id"] != permit_id:
                raise auth.AuthorizationError("Continuity permit was superseded")
            attempts = release._continuity_complete(connection, permit, rank=rank)
            proof = payload["natural_members"][rank-1]
        run = connection.execute("SELECT * FROM scheduler_runs WHERE id=?", (proof["source_run_id"],)).fetchone()
        details = json.loads(run["details_json"])
        scope = details["identity"]
        if tikhub_scan._digest(scope) != proof["source_identity_sha256"]:
            raise auth.AuthorizationError("Continuity source identity changed")
        raw_id = connection.execute("SELECT id FROM provider_raw_responses WHERE fetch_attempt_id=?", (attempts[0],)).fetchone()[0]
        content_replay = proof["stage"] != "discovery"
        if content_replay:
            content = dict(connection.execute("SELECT * FROM content_items WHERE id=?", (proof["scope_identity"]["content_id"],)).fetchone())
            owner = details["owner"]
            claim = durable_runs.DurableClaim(scheduler_run_id=run["id"], attempt_id=owner["attempt_id"],
                attempt_number=owner["attempt_number"], owner_token=owner["token"], scan_id=details["scan_id"])
            # Recovery never steals a current queue owner or substitutes a new
            # natural scope to fill the fixed sample. Interrupted queues use
            # their existing scheduler recovery before this local helper.
            durable_runs.assert_owner(connection, claim)
            raw = capture.load_succeeded_raw_response(content_id=content["id"], stage=proof["stage"],
                window_key=proof["request_document"]["due_bucket"], operation=proof["operation"], db_path=db_path)
            if raw.raw_response_id != raw_id:
                raise auth.AuthorizationError("Continuity content raw differs from successful start")
            source_stage = "detail" if proof["operation"] in {"douyin_video_detail", "xiaohongshu_note_detail"} else proof["stage"]
            parsed = providers._parse_douyin_stage_payload(source_stage, content["platform_content_id"], raw.value,
                status=raw.http_status or 200) if content["platform"] == "douyin" else providers._parse_xhs_stage_payload(
                    source_stage, content["platform_content_id"], content["content_type"], raw.value, status=raw.http_status or 200)
            data = dict(parsed.data["metrics"]) if proof["stage"] == "metrics" and source_stage == "detail" else dict(parsed.data)
            data["_evidence_captured_at"] = raw.captured_at
            replay = capture.CaptureOutcome(slot_id=raw.slot_id, attempt_id=attempts[0], raw_response_id=raw_id,
                data=data, billed=False, amount=0.0, currency="USD")
            paid = PaidScope(**proof["scope_identity"])
        else:
            content = None
        pending = details["checkpoint"].get("pending_materialization")
        if pending is None and not content_replay:
            head = details["checkpoint"].get("last_manifest")
            if isinstance(head, dict):
                from .raw_evidence import _read_single_regular
                import hashlib
                body = _read_single_regular(Path(head["path"]), max_bytes=int(head["byte_size"]))
                if hashlib.sha256(body).hexdigest() != head["sha256"]:
                    raise auth.AuthorizationError("Continuity manifest bytes changed")
                manifest = json.loads(body)
                if manifest["raw"]["raw_response_id"] == raw_id and details["checkpoint"].get("pending_raw") is None:
                    return {"permit_id": permit_id, "rank": rank, "raw_response_id": raw_id, "provider_calls": 0,
                            "materialized": True, "already_materialized": True}
        elif pending is not None and pending["identity"]["raw_response_id"] != raw_id:
            raise auth.AuthorizationError("Continuity pending raw differs from the successful start")
        owned = run["status"] == "running"
        if owned:
            owner = details["owner"]
            claim = durable_runs.DurableClaim(scheduler_run_id=run["id"], attempt_id=owner["attempt_id"],
                attempt_number=owner["attempt_number"], owner_token=owner["token"], scan_id=details["scan_id"])
            durable_runs.assert_owner(connection, claim)
        elif pending is None:
            recovered_claim = durable_runs.claim_run_in_transaction(connection, run["job_id"], scope,
                invocation_source="operator_retry", now=timestamp)
            if recovered_claim is None:
                raise auth.AuthorizationError("Continuity local owner is unavailable")
            claim = recovered_claim
            owned = True
    if content is not None:
        with paid_scope(str(paid.purpose), activation_id=paid.activation_id, roster_snapshot_id=paid.roster_snapshot_id,
                roster_snapshot_hash=paid.roster_snapshot_hash, scheduler_run_id=paid.scheduler_run_id,
                scheduler_attempt_id=paid.scheduler_attempt_id, business_day=paid.business_day):
            result = _materialize_continuity_content(content, proof, replay, db_path=db_path)
        return {"permit_id": permit_id, "rank": rank, "raw_response_id": raw_id, "provider_calls": 0,
                "materialized": True, "materialization": result}
    if not owned:
        result = tikhub_scan.resume_local_materialization(proof["source_run_id"], db_path=db_path,
            raw_root=raw_root, now=timestamp, deadline=tikhub_scan._monotonic_now()+50)
        return {"permit_id": permit_id, "rank": rank, "raw_response_id": raw_id, "provider_calls": 0, "local": result}
    # Exact same existing owner is still live (e.g. a caught local exception).
    # The scanner's normal guard makes provider acquisition impossible here.
    token = tikhub_scan._LOCAL_REPLAY.set((str(db_path.resolve()), claim.scheduler_run_id, tikhub_scan._digest(scope)))
    try:
        if pending is None:
            # C committed before _apply: locate only the exact frozen slot and
            # successful dispatch raw. No provider adapter is invoked.
            window = proof["request_document"]["due_bucket"]
            if tikhub_scan._page_key(claim, details["checkpoint"]) != window:
                raise auth.AuthorizationError("Continuity local cursor differs from frozen raw")
            raw = capture.load_succeeded_raw_response(account_id=proof["scope_identity"]["account_id"],
                stage="discovery", window_key=window, operation=proof["operation"], db_path=db_path)
            if raw.raw_response_id != raw_id:
                raise auth.AuthorizationError("Continuity local raw identity changed")
            tikhub_scan._apply(claim, scope, raw, window_key=window, db_path=db_path, raw_root=raw_root, now=timestamp)
            pending = tikhub_scan._state(claim, scope, db_path)["pending_materialization"]
        tikhub_scan._local_materialization_preflight(claim.scheduler_run_id, db_path=db_path)
        complete = tikhub_scan._materialize_pending(claim, scope, pending, db_path=db_path, raw_root=raw_root, now=timestamp)
        state = tikhub_scan._state(claim, scope, db_path)
        result = tikhub_scan._finish_success(claim, str(state["completion_reason"]), db_path=db_path, now=timestamp, pages=0) if complete else tikhub_scan._partial(claim, "continuity_local_yield", db_path=db_path, now=timestamp, pages=0)
    finally:
        tikhub_scan._LOCAL_REPLAY.reset(token)
    return {"permit_id": permit_id, "rank": rank, "raw_response_id": raw_id, "provider_calls": 0,
            "materialized": True, "scan": result}


def execute_native_member_page(cohort_id: int, rank: int, *, db_path: Path,
                               raw_root: Path, at: str | None = None) -> dict[str, Any]:
    return execute_continuity_member_page(cohort_id, rank, db_path=db_path, raw_root=raw_root, at=at, _native=True)


def resume_native_member_local(cohort_id: int, rank: int, *, db_path: Path,
                               raw_root: Path, at: str | None = None) -> dict[str, Any]:
    return resume_continuity_member_local(cohort_id, rank, db_path=db_path, raw_root=raw_root, at=at, _native=True)


def execute_primary_member_page(
    member_receipt_id: int, *, operator_claim: durable_runs.DurableClaim,
    scheduler: BaseScheduler, db_path: Path, raw_root: Path,
) -> dict[str, Any]:
    """Run the exact owned page once and locally apply it, without callbacks.

    Provider, configuration, request bytes and ordinary budgets come from the
    same adapters as normal capture. Tests replace the HTTP transport, not the
    permission gate. Successful transport is reported separately from local
    materialization and whole-scan completeness.
    """
    with diagnostic_request_context(member_receipt_id, operator_claim, scheduler=scheduler):
        binding = current_diagnostic_request_binding()
        assert binding is not None
        with connect(db_path) as connection, transaction(connection):
            member = read_transport_receipt(connection, member_receipt_id)
            payload = member["payload"]
            document = payload["natural_due"]["request_document"]
            request_identity = build_paid_request_identity(
                provider=document["provider"], operation=document["operation"],
                platform=document["platform"], subject=document["subject"],
                request_parameters=document["request_parameters"], cursor=document["cursor"],
                due_bucket=document["due_bucket"], request_window=document["request_window"],
                sequence=payload["sequence"],
            )
            paid = PaidScope(**payload["natural_due"]["scope_identity"])
            authorize_diagnostic_request(
                connection, binding=binding, scope=paid, request_identity=request_identity,
                request_transport=payload["request_transport"], stage="discovery", at=now_utc(),
            )
            run = connection.execute(
                "SELECT details_json FROM scheduler_runs WHERE id=?", (paid.scheduler_run_id,),
            ).fetchone()
            details = json.loads(run["details_json"])
            owner = details["owner"]
            claim = durable_runs.DurableClaim(
                scheduler_run_id=int(paid.scheduler_run_id or 0),
                attempt_id=int(owner["attempt_id"]), attempt_number=int(owner["attempt_number"]),
                owner_token=str(owner["token"]), scan_id=str(details["scan_id"]),
            )
            scope = details["identity"]
            tikhub_scan._owned_scope(connection, claim, scope)
        window = request_identity.document["due_bucket"]
        operation = "douyin_user_posts"
        task_id = str(scope["task_id"] or f"tikhub-scan-{claim.scan_id}")
        task_max_amount = int(scope["task_max_microusd"]) / 1_000_000
        raw_id = None
        materialized = False
        pages = 0
        try:
            # _owned_scope fences DB ownership; _state additionally verifies
            # the committed manifest bytes. Do that before any paid purchase.
            state = tikhub_scan._state(claim, scope, db_path)
            if tikhub_scan._page_key(claim, state) != window:
                raise tikhub_scan.TikHubScanError(
                    "diagnostic_cursor_not_canonical", "Stored cursor differs from the frozen request page key",
                )
            budget_id = providers._budget_for_call(
                provider="TikHub", operation=operation, price=providers.TIKHUB_PRICE,
                task_id=task_id, task_max_amount=task_max_amount, db_path=db_path,
            )
            with paid_scope(
                str(paid.purpose), activation_id=paid.activation_id,
                roster_snapshot_id=paid.roster_snapshot_id, roster_snapshot_hash=paid.roster_snapshot_hash,
                scheduler_run_id=claim.scheduler_run_id, scheduler_attempt_id=claim.attempt_id,
                business_day=paid.business_day,
            ):
                outcome = capture.execute_account_fetch(
                    account_id=int(scope["account_id"]), stage="discovery", window_key=window,
                    provider="TikHub", adapter_version=str(scope["contract_version"]),
                    operation=operation, db_path=db_path, raw_root=raw_root,
                    budget_id=budget_id, task_id=task_id, task_max_amount=task_max_amount,
                    paid_request_identity=request_identity, request_transport=payload["request_transport"],
                    call=lambda: tikhub_scan._provider_call(operation, {
                        **scope, "reference": document["request_parameters"]["sec_user_id"],
                        "cursor": document["cursor"],
                    }, None),
                )
            raw_id = outcome.raw_response_id
            raw = capture.load_succeeded_raw_response(
                stage="discovery", window_key=window, account_id=int(scope["account_id"]),
                operation=operation, db_path=db_path,
            )
            if raw.raw_response_id != raw_id:
                raise tikhub_scan.TikHubScanError("raw_identity_conflict", "Diagnostic raw changed after capture")
            tikhub_scan._apply(
                claim, scope, raw, window_key=window, db_path=db_path, raw_root=raw_root, now=now_utc(),
            )
            pages = 1
            state = tikhub_scan._state(claim, scope, db_path)
            pending = state.get("pending_materialization")
            if pending is None:
                raise tikhub_scan.TikHubScanError("materialization_integrity_error", "Diagnostic page has no local child")
            complete = tikhub_scan._materialize_pending(
                claim, scope, pending, db_path=db_path, raw_root=raw_root, now=now_utc(),
            )
            materialized = True
            if complete:
                state = tikhub_scan._state(claim, scope, db_path)
                result = tikhub_scan._finish_success(
                    claim, str(state["completion_reason"]), db_path=db_path, now=now_utc(), pages=pages,
                )
            else:
                result = tikhub_scan._partial(
                    claim, "diagnostic_page_yield", db_path=db_path, now=now_utc(), pages=pages,
                )
        except durable_runs.LostOwnership:
            raise  # Never finish another owner's attempt or declare it complete.
        except (capture.CaptureError, BudgetBlocked, capture.SlotUnavailable,
                RosterError, tikhub_scan.TikHubScanError, DiagnosticMemberError) as error:
            reason = str(getattr(error, "error_code", None) or getattr(error, "reason", None)
                         or getattr(error, "code", None) or "diagnostic_execution_failed")
            result = tikhub_scan._classified_failure(
                claim, scope, reason, db_path=db_path, now=now_utc(), pages=pages,
                http_status=getattr(error, "http_status", None),
                has_raw=raw_id is not None or getattr(error, "raw_response", None) is not None,
            )
        except (capture.RawResponseIntegrityError, OSError, sqlite3.Error):
            # Keep the exact request/received raw recoverable; never pay again.
            tikhub_scan._partial(
                claim, "diagnostic_local_evidence_failed", db_path=db_path, now=now_utc(), pages=pages,
            )
            raise

        with connect(db_path) as connection:
            rows = connection.execute(
                "SELECT DISTINCT dispatch_id FROM paid_provider_dispatch_events "
                "WHERE json_extract(scope_json,'$.diagnostic_member.receipt_id')=?",
                (member_receipt_id,),
            ).fetchall()
            if len(rows) > 1:
                raise DiagnosticMemberError("diagnostic_duplicate_dispatch", "Member has multiple dispatches")
            events = dispatch_events(connection, rows[0]["dispatch_id"]) if rows else []
            transport = None
            if events:
                terminal_raw = events[-1].raw_response_id
                if raw_id is not None and raw_id != terminal_raw:
                    raise DiagnosticMemberError("diagnostic_raw_changed", "Result raw differs from its dispatch")
                raw_id = terminal_raw
                usage = connection.execute(
                    "SELECT details_json FROM provider_usage WHERE id=?", (events[-1].provider_usage_id,),
                ).fetchone()
                transport = json.loads(usage["details_json"]).get("transport") if usage else None
            response_complete = isinstance(transport, dict) and (
                transport.get("status") == "succeeded" and transport.get("error_code") is None
                and transport.get("clean_eof") is True and transport.get("json_parse_ok") is True
                and transport.get("length_match") in (None, True)
                and (transport.get("content_length") is None or transport.get("length_match") is True)
                and transport.get("content_encoding") in {"identity", "gzip"}
                and (transport.get("gzip_crc_ok") is True if transport.get("content_encoding") == "gzip"
                     else transport.get("gzip_crc_ok") is None)
            )
        return {
            "member_receipt_id": member_receipt_id, "rank": payload["rank"],
            "dispatch_id": events[0].dispatch_id if events else None,
            "dispatch_terminal": events[-1].event_type if events else "not_reserved",
            "effective_starts": sum(event.event_type == "send_marked" for event in events),
            "raw_response_id": raw_id,
            "response_complete": response_complete, "transport_receipt": transport,
            "materialized": materialized, "scan": result,
            "qualified": False,  # Only the later fixed-denominator verifier can qualify.
        }
