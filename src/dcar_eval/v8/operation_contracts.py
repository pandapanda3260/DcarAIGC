"""Reuse provider parsers to qualify a transient recovery response, without I/O."""
from __future__ import annotations

from typing import Any, Mapping

SUPPORTED_OPERATIONS = frozenset({
    "douyin_uid_profile", "douyin_user_posts", "xiaohongshu_user_posts",
    "douyin_video_detail", "douyin_video_statistics",
    "xiaohongshu_note_detail", "xiaohongshu_note_statistics",
})


class OperationContractError(ValueError):
    pass


def require_response(operation: str, identity: Mapping[str, Any], payload: Any, *, account_uid: str | None = None) -> None:
    """Require the actual requested members and page contract, not merely HTTP 200.

    Comments are excluded: their legacy parsers create privacy hashers and are
    not a pure recovery check. Unsupported operations keep their typed recovery.
    """
    from . import capture, providers
    from .tikhub_scan import TikHubScanError

    if (operation not in SUPPORTED_OPERATIONS or not isinstance(payload, dict) or not isinstance(identity, Mapping)
            or identity.get("operation") != operation or not isinstance(identity.get("provider"), str)
            or identity["provider"].lower() != "tikhub" or not isinstance(identity.get("request_parameters", {}), Mapping)
            or not isinstance(identity.get("subject"), str) or not identity["subject"]
            or "stage" in payload or "metrics" in payload):
        raise OperationContractError("Recovery response lacks a supported provider/request contract")
    try:
        subject = identity["subject"]
        params = identity.get("request_parameters", {})
        # Recovery uses actual provider envelopes, never local replay shortcuts.
        if operation.startswith("douyin_"):
            providers._tikhub_douyin_data(payload)
        else:
            providers._tikhub_xhs_data(payload)
        if operation == "douyin_uid_profile":
            from .account_metrics import parse_tikhub_profile
            providers._parse_douyin_reference_payload(payload)
            profile = parse_tikhub_profile(payload, platform="douyin", uid=params.get("uid", subject))
            if profile["field_status"]["follower_count"]["status"] != "provided":
                raise OperationContractError("Recovery profile omitted a valid follower count")
        elif operation in {"douyin_user_posts", "xiaohongshu_user_posts"}:
            from .tikhub_scan import _item, _item_evidence, _page_payload
            platform = "douyin" if operation == "douyin_user_posts" else "xiaohongshu"
            items, more, cursor, _ = _page_payload(payload, platform)
            if more and (not items or cursor in (None, "") or cursor == identity.get("cursor")):
                raise OperationContractError("Recovery page has no advancing cursor")
            if more:
                valid_cursor = ((type(cursor) is int and cursor >= 0) or
                    (isinstance(cursor, str) and cursor.isascii() and cursor.isdecimal())) if platform == "douyin" else (
                        isinstance(cursor, str) and bool(cursor.strip()))
                if not valid_cursor or str(cursor) == str(identity.get("cursor")):
                    raise OperationContractError("Recovery page cursor is invalid or unchanged")
            evidence = [_item_evidence(platform, value) for value in items]
            if any(item["event_tuple"] is None for item in evidence):
                raise OperationContractError("Recovery page omitted a member identity or publication time")
            identifiers = [item["platform_content_id"] for item in evidence]
            if len(set(identifiers)) != len(identifiers):
                raise OperationContractError("Recovery page contains duplicate members")
            if items and (not account_uid or any(_item(platform, value)["account_uid"] != account_uid for value in items)):
                raise OperationContractError("Recovery page does not match the frozen account UID")
        elif operation == "douyin_video_statistics":
            from .capture_batches import parse_statistics_members
            requested = str(params.get("aweme_ids", subject)).split(",")
            if any(not identifier for identifier in requested) or set(requested) != set(subject.split(",")):
                raise OperationContractError("Recovery statistics request identity differs")
            parsed = parse_statistics_members(payload, requested)
            if not all(value["disposition"] == "valid" for value in parsed.values()):
                raise OperationContractError("Recovery statistics omitted a valid requested member")
        elif operation == "douyin_video_detail":
            providers._parse_douyin_stage_payload("detail", subject, payload)
        else:
            stage = "detail" if operation.endswith("_detail") else "metrics"
            parsed = providers._parse_xhs_stage_payload(stage, subject, "unknown", payload).data
            if stage == "metrics" and not any(value.get("status") == "provided"
                    for value in parsed.get("_field_status", {}).values()):
                raise OperationContractError("Recovery note statistics have no provided metrics")
    except (capture.CaptureError, TikHubScanError, ValueError, TypeError, KeyError, OverflowError,
            AttributeError, RecursionError) as error:
        raise OperationContractError("Recovery response failed the provider operation contract") from error
