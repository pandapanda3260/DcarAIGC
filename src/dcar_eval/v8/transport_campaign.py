"""Immutable primary transport campaign, closed to sending until member permits.

Only the first, fixed mainland route arm is issued here. Diagnostic runners must
add separately verified natural-due members; a header is not a drain exception.
Control-arm and qualification receipts cannot be fabricated through this API.
"""

from __future__ import annotations

import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any, Mapping

from .provider_budget import PRICES_MICROUSD
from .provider_transport import validate_request_transport_binding
from .source_routing import parse_time
from .transport_cohort import CONTRACT_VERSION as COHORT_CONTRACT
from .transport_hold_binding import read_current_diagnostic_hold
from .transport_receipts import append_transport_receipt, read_transport_receipt

CONTRACT_VERSION = "transport-diagnostic-campaign-v1"
OPERATION = "douyin_user_posts"
SELECTION_ORDER = ["scheduled_for", "account_uid", "cursor_canonical_json", "paid_scope_identity"]
CONTROL_ARMS = frozenset({"control_io", "control_legacy"})
_HOLD_LINEAGE_KEYS = (
    "contract_version",
    "control_contract_version",
    "drain_id",
    "start_event_id",
    "start_event_hash",
    "dispatch_legacy_release_anchor",
    "activation_id",
    "profile_id",
    "roster_snapshot_id",
    "roster_snapshot_hash",
    "matrix_high_watermarks",
    "actor",
)


class TransportCampaignError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def same_hold_lineage(source: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    """Return True when two HOLD bindings share the same unsealed START lineage.

    Build/runtime/config/prerequisite artifacts are deliberately excluded: a
    control arm may run on a later build generation while citing the failed
    primary route verdict that justified the diagnostic branch.
    """

    try:
        source_generation = source.get("generation")
        current_generation = current.get("generation")
        if (
            type(source_generation) is not int
            or type(current_generation) is not int
            or source_generation < 1
            or current_generation < 1
            or source_generation > current_generation
        ):
            return False
        return all(source.get(key) == current.get(key) for key in _HOLD_LINEAGE_KEYS)
    except AttributeError:
        return False


def freeze_primary_transport_campaign(
    connection: sqlite3.Connection, *, drain_id: str, cohort_receipt_id: int,
    request_transport: Mapping[str, Any], at: str, mirror_root: Path,
) -> dict[str, Any]:
    """Freeze the one primary arm for this HOLD/build, never reset its denominator.

    With still-valid HOLD prerequisites, a receipt is idempotent after its own
    deadline; prerequisite expiry fails closed and never allows a new sample.
    The future runner must leave insufficient samples unqualified,
    and use the receipt's exact 20-member prefix without replacing failures.
    """
    if not connection.in_transaction:
        raise TransportCampaignError(
            "transport_campaign_transaction_required", "Campaign freeze requires a transaction",
        )
    hold = read_current_diagnostic_hold(connection, drain_id=drain_id, at=at)
    transport = validate_request_transport_binding(request_transport)
    manifest = transport["manifest"]
    if (
        manifest["api_base"] != "https://api.tikhub.dev"
        or manifest["request_host"] != "api.tikhub.dev"
        or manifest["http_stack"] != "urllib-stream-v1"
    ):
        raise TransportCampaignError(
            "transport_campaign_primary_route_invalid",
            "Primary diagnostics require the fixed mainland streaming route",
        )
    cohort = read_transport_receipt(connection, cohort_receipt_id)
    payload = cohort["payload"]
    if (
        cohort["kind"] != "cohort" or payload.get("contract_version") != COHORT_CONTRACT
        or payload.get("hold_binding") != hold
        or not payload.get("selected_identity_ids")
        or parse_time(cohort["recorded_at"]) > parse_time(at)
    ):
        raise TransportCampaignError(
            "transport_campaign_cohort_invalid", "Campaign cohort is missing or belongs to another HOLD",
        )
    key = f"primary:{hold['start_event_hash']}:{hold['generation']}"
    existing = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign' "
        "AND scheduled_for=?", (key,),
    ).fetchone()
    if existing:
        receipt = read_transport_receipt(connection, int(existing["id"]))
        frozen = receipt["payload"]
        if (
            frozen.get("contract_version") != CONTRACT_VERSION
            or frozen.get("hold_binding") != hold
            or frozen.get("cohort_receipt_id") != cohort_receipt_id
            or frozen.get("cohort_receipt_sha256") != cohort["self_sha256"]
            or frozen.get("request_transport") != transport
        ):
            raise TransportCampaignError(
                "transport_campaign_changed", "Campaign binding changed; its sample cannot be reset",
            )
        return receipt
    # BEGIN IMMEDIATE plus the durable root key serialize concurrent issuers.
    # Until the terminal verifier exists, no terminal receipt can release this
    # same-generation exclusivity gate, including an expired campaign.
    for row in connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign'"
    ):
        previous = read_transport_receipt(connection, int(row["id"]))["payload"]
        previous_hold = previous.get("hold_binding", {})
        if (
            previous_hold.get("start_event_hash") == hold["start_event_hash"]
            and previous_hold.get("generation") == hold["generation"]
        ):
            raise TransportCampaignError(
                "transport_campaign_already_active", "Only one diagnostic campaign may own this HOLD generation",
            )
    issued = parse_time(at)
    expires = min(
        issued + timedelta(hours=24),
        *(parse_time(value["expires_at"]) for value in hold["prerequisites"].values()),
    )
    unit_price = int(PRICES_MICROUSD[OPERATION])
    high_watermarks = {
        table: int(connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0])
        for table in (
            "provider_usage", "fetch_attempts", "provider_raw_responses",
            "paid_provider_dispatch_events", "scheduler_runs",
        )
    }
    campaign = {
        "contract_version": CONTRACT_VERSION,
        "campaign_key": key,
        "hold_binding": hold,
        "cohort_receipt_id": cohort_receipt_id,
        "cohort_receipt_sha256": cohort["self_sha256"],
        "request_transport": transport,
        "operation": OPERATION,
        "arm": "primary",
        "sample_limit": 20,
        "rank_start": 1,
        "selection_order": SELECTION_ORDER,
        "membership_rule": "continuous_first_natural_due_prefix_no_replacement",
        "cohort_rule": "only_frozen_large_page_account_cohort",
        "failure_denominator": "retain_every_effective_start",
        "scope_sequence": 0,
        "normal_budget_only": True,
        "unit_price_microusd": unit_price,
        "max_cost_microusd": 20 * unit_price,
        "starting_high_watermarks": high_watermarks,
        "actor": hold["actor"],
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
    }
    return append_transport_receipt(
        connection, kind="campaign", identity_key=key, payload=campaign,
        at=at, mirror_root=mirror_root,
    )


def freeze_control_transport_campaign(
    connection: sqlite3.Connection, *, drain_id: str, source_verdict_receipt_id: int,
    arm: str, request_transport: Mapping[str, Any], at: str, mirror_root: Path,
) -> dict[str, Any]:
    """Freeze one post-primary control arm with a disjoint natural-due sample."""

    if not connection.in_transaction:
        raise TransportCampaignError(
            "transport_campaign_transaction_required", "Campaign freeze requires a transaction",
        )
    if arm not in CONTROL_ARMS:
        raise TransportCampaignError(
            "transport_campaign_control_arm_invalid", "Control arm is unsupported",
        )
    hold = read_current_diagnostic_hold(connection, drain_id=drain_id, at=at)
    source_verdict = read_transport_receipt(connection, source_verdict_receipt_id)
    source_payload = source_verdict["payload"]
    source_hold = source_payload.get("hold_binding")
    if (
        source_verdict["kind"] != "route_verdict"
        or source_payload.get("contract_version") != "transport-primary-route-verdict-v1"
        or not isinstance(source_hold, Mapping)
        or not same_hold_lineage(source_hold, hold)
        or source_payload.get("status") != "failed"
        or source_payload.get("next_action") != "run_disjoint_control_arms"
        or source_payload.get("effective_starts") != 20
        or source_payload.get("route_passed") is not False
    ):
        raise TransportCampaignError(
            "transport_campaign_primary_verdict_invalid",
            "Control diagnostics require the failed primary route verdict",
        )
    primary = read_transport_receipt(connection, int(source_payload["campaign_receipt_id"]))
    primary_payload = primary["payload"]
    if (
        primary["kind"] != "campaign"
        or primary_payload.get("contract_version") != CONTRACT_VERSION
        or primary_payload.get("arm") != "primary"
        or primary_payload.get("hold_binding") != source_hold
    ):
        raise TransportCampaignError(
            "transport_campaign_primary_invalid", "Control diagnostics lost the primary campaign",
        )
    transport = validate_request_transport_binding(request_transport)
    manifest = transport["manifest"]
    expected = {
        "control_io": ("https://api.tikhub.io", "api.tikhub.io", "urllib-stream-v1"),
        "control_legacy": ("https://api.tikhub.dev", "api.tikhub.dev", "urllib-legacy-v1"),
    }[arm]
    if (manifest["api_base"], manifest["request_host"], manifest["http_stack"]) != expected:
        raise TransportCampaignError(
            "transport_campaign_control_route_invalid",
            "Control diagnostics require their fixed route arm",
        )
    key = f"{arm}:{hold['start_event_hash']}:{hold['generation']}"
    existing = connection.execute(
        "SELECT id FROM scheduler_runs WHERE job_id='transport_receipt:campaign' "
        "AND scheduled_for=?", (key,),
    ).fetchone()
    if existing:
        receipt = read_transport_receipt(connection, int(existing["id"]))
        frozen = receipt["payload"]
        if (
            frozen.get("contract_version") != CONTRACT_VERSION
            or frozen.get("hold_binding") != hold
            or frozen.get("cohort_receipt_id") != primary_payload["cohort_receipt_id"]
            or frozen.get("source_verdict_receipt_id") != source_verdict_receipt_id
            or frozen.get("request_transport") != transport
            or frozen.get("arm") != arm
        ):
            raise TransportCampaignError(
                "transport_campaign_changed", "Control campaign binding changed",
            )
        return receipt
    issued = parse_time(at)
    expires = min(
        issued + timedelta(hours=24),
        *(parse_time(value["expires_at"]) for value in hold["prerequisites"].values()),
    )
    unit_price = int(PRICES_MICROUSD[OPERATION])
    high_watermarks = {
        table: int(connection.execute(f"SELECT COALESCE(MAX(id),0) FROM {table}").fetchone()[0])
        for table in (
            "provider_usage", "fetch_attempts", "provider_raw_responses",
            "paid_provider_dispatch_events", "scheduler_runs",
        )
    }
    campaign = {
        "contract_version": CONTRACT_VERSION,
        "campaign_key": key,
        "hold_binding": hold,
        "cohort_receipt_id": primary_payload["cohort_receipt_id"],
        "cohort_receipt_sha256": primary_payload["cohort_receipt_sha256"],
        "source_verdict_receipt_id": source_verdict_receipt_id,
        "source_verdict_receipt_sha256": source_verdict["self_sha256"],
        "request_transport": transport,
        "operation": OPERATION,
        "arm": arm,
        "sample_limit": 20,
        "rank_start": 1,
        "selection_order": SELECTION_ORDER,
        "membership_rule": "first_unused_natural_due_prefix_no_replacement",
        "cohort_rule": "only_frozen_large_page_account_cohort",
        "failure_denominator": "retain_every_effective_start",
        "scope_sequence": 0,
        "normal_budget_only": True,
        "unit_price_microusd": unit_price,
        "max_cost_microusd": 20 * unit_price,
        "starting_high_watermarks": high_watermarks,
        "actor": hold["actor"],
        "issued_at": issued.isoformat(),
        "expires_at": expires.isoformat(),
    }
    return append_transport_receipt(
        connection, kind="campaign", identity_key=key, payload=campaign,
        at=at, mirror_root=mirror_root,
    )
