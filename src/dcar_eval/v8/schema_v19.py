"""Schema 19 contract and the offline schema-18 bridge migration."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Mapping


MIGRATION_NAME = "dual-acquisition-profile-roster-v1"
MATRIX_PROFILE = "matrix_hybrid_v1"
TIKHUB_PROFILE = "tikhub_managed_v1"
LEGACY_ACTIVATION_JOB = "matrix_pipeline_activation"
LEGACY_SCHEMA18_REPORT_VERSION = "dcar-content-operations-report-v8.8"
NEW_TABLES = (
    "acquisition_profile_activations",
    "activation_cancellations",
    "account_state_events",
    "scan_verification_receipts",
    "profile_day_coverage_receipts",
    "runtime_receipt_revocations",
    "pipeline_paid_drain_events",
    "paid_provider_dispatch_events",
)


PROFILE_SCHEMA_SQL = r"""
CREATE INDEX IF NOT EXISTS idx_accounts_phone_normalized ON accounts(phone_normalized);
CREATE UNIQUE INDEX IF NOT EXISTS uq_account_provider_reference_value
ON account_provider_references(provider, reference_kind, reference_value);
CREATE UNIQUE INDEX IF NOT EXISTS uq_metric_snapshot_canonical
ON content_metric_snapshots(content_id, window_key);

CREATE TABLE IF NOT EXISTS account_roster_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_family TEXT NOT NULL CHECK(source_family IN ('matrix','system')),
    source_type TEXT NOT NULL
        CHECK(source_type IN ('bootstrap_export','manual_export','api_fullroster','system_managed')),
    scope_key TEXT NOT NULL,
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    source_instance_id TEXT NOT NULL,
    source_captured_at TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    declared_count INTEGER NOT NULL CHECK(declared_count>=0),
    member_count INTEGER NOT NULL CHECK(member_count=declared_count),
    members_sha256 TEXT NOT NULL CHECK(length(members_sha256)=64),
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256)=64),
    source_path TEXT NOT NULL,
    contract_version TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    UNIQUE(source_family, scope_key, source_instance_id),
    CHECK(
        (source_family='matrix' AND source_type IN ('bootstrap_export','manual_export','api_fullroster'))
        OR (source_family='system' AND source_type='system_managed')
    )
);

CREATE INDEX IF NOT EXISTS idx_roster_snapshots_current
ON account_roster_snapshots(source_family, scope_key, accepted_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS account_roster_members (
    snapshot_id INTEGER NOT NULL REFERENCES account_roster_snapshots(id) ON DELETE RESTRICT,
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
    platform TEXT NOT NULL CHECK(platform IN ('douyin','xiaohongshu','wechat_channels','kuaishou')),
    member_key TEXT NOT NULL CHECK(length(trim(member_key))>0),
    uid TEXT CHECK(uid IS NULL OR length(trim(uid))>0),
    matrix_account_id TEXT CHECK(matrix_account_id IS NULL OR length(trim(matrix_account_id))>0),
    profile_ref TEXT CHECK(profile_ref IS NULL OR length(trim(profile_ref))>0),
    monitoring_status TEXT NOT NULL CHECK(monitoring_status IN ('unknown','monitored','not_monitored')),
    authorization_status TEXT NOT NULL CHECK(authorization_status IN ('unknown','authorized','unauthorized')),
    monitoring_started_at TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    PRIMARY KEY(snapshot_id, account_identity_id),
    UNIQUE(snapshot_id, member_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_roster_members_matrix_id
ON account_roster_members(snapshot_id, platform, matrix_account_id)
WHERE matrix_account_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_roster_members_identity
ON account_roster_members(account_identity_id, snapshot_id);

CREATE TABLE IF NOT EXISTS account_metric_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
    source TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
    contract_version TEXT NOT NULL,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    observation_sha256 TEXT NOT NULL UNIQUE CHECK(length(observation_sha256)=64)
);

CREATE INDEX IF NOT EXISTS idx_account_metrics_identity_capture
ON account_metric_observations(account_identity_id, captured_at DESC, recorded_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS acquisition_profile_activations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    profile_id TEXT NOT NULL CHECK(profile_id IN ('matrix_hybrid_v1','tikhub_managed_v1')),
    roster_snapshot_id INTEGER NOT NULL REFERENCES account_roster_snapshots(id) ON DELETE RESTRICT,
    roster_members_sha256 TEXT NOT NULL CHECK(length(roster_members_sha256)=64),
    effective_at TEXT NOT NULL CHECK(
        length(effective_at)=27
        AND effective_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(effective_at) IS NOT NULL
    ),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    build_receipt_sha256 TEXT NOT NULL CHECK(length(build_receipt_sha256)=64),
    previous_activation_id INTEGER REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    previous_activation_sha256 TEXT CHECK(previous_activation_sha256 IS NULL OR length(previous_activation_sha256)=64),
    activation_sha256 TEXT NOT NULL UNIQUE CHECK(length(activation_sha256)=64),
    actor TEXT NOT NULL CHECK(length(trim(actor))>0),
    reason TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL CHECK(
        length(created_at)=27
        AND created_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(created_at) IS NOT NULL
    ),
    CHECK(
        (previous_activation_id IS NULL AND previous_activation_sha256 IS NULL)
        OR (previous_activation_id IS NOT NULL AND previous_activation_sha256 IS NOT NULL)
    ),
    CHECK(created_at<=effective_at)
);

CREATE INDEX IF NOT EXISTS idx_profile_activations_effective
ON acquisition_profile_activations(effective_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS activation_cancellations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    activation_id INTEGER NOT NULL UNIQUE REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    cancelled_at TEXT NOT NULL CHECK(
        length(cancelled_at)=27
        AND cancelled_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(cancelled_at) IS NOT NULL
    ),
    actor TEXT NOT NULL CHECK(length(trim(actor))>0),
    reason TEXT NOT NULL CHECK(length(trim(reason))>0),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    cancellation_sha256 TEXT NOT NULL UNIQUE CHECK(length(cancellation_sha256)=64),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL CHECK(
        length(created_at)=27
        AND created_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(created_at) IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS account_state_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_identity_id INTEGER NOT NULL REFERENCES account_platform_identities(id) ON DELETE RESTRICT,
    activation_id INTEGER REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    old_enabled INTEGER NOT NULL CHECK(old_enabled IN (0,1)),
    new_enabled INTEGER NOT NULL CHECK(new_enabled IN (0,1)),
    effective_at TEXT NOT NULL CHECK(
        length(effective_at)=27
        AND effective_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(effective_at) IS NOT NULL
    ),
    actor TEXT NOT NULL CHECK(length(trim(actor))>0),
    reason TEXT NOT NULL CHECK(length(trim(reason))>0),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    event_sha256 TEXT NOT NULL UNIQUE CHECK(length(event_sha256)=64),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL CHECK(
        length(created_at)=27
        AND created_at GLOB '????-??-??T??:??:??.??????Z'
        AND julianday(created_at) IS NOT NULL
    ),
    CHECK(old_enabled<>new_enabled)
);

CREATE INDEX IF NOT EXISTS idx_account_state_identity_effective
ON account_state_events(account_identity_id, effective_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS scan_verification_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_bridge_run_id INTEGER NOT NULL UNIQUE REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    source_bridge_attempt_id INTEGER NOT NULL UNIQUE REFERENCES scheduler_run_attempts(id) ON DELETE RESTRICT,
    scan_run_id INTEGER NOT NULL REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    scan_attempt_id INTEGER NOT NULL REFERENCES scheduler_run_attempts(id) ON DELETE RESTRICT,
    scan_status TEXT NOT NULL CHECK(scan_status IN ('succeeded','failed')),
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(length(receipt_sha256)=64),
    recorded_at TEXT NOT NULL,
    UNIQUE(scan_run_id, scan_attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_scan_receipts_scan
ON scan_verification_receipts(scan_run_id, recorded_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS profile_day_coverage_receipts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_bridge_run_id INTEGER NOT NULL UNIQUE REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    source_bridge_attempt_id INTEGER NOT NULL UNIQUE REFERENCES scheduler_run_attempts(id) ON DELETE RESTRICT,
    activation_id INTEGER NOT NULL REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    profile_id TEXT NOT NULL CHECK(profile_id IN ('matrix_hybrid_v1','tikhub_managed_v1')),
    roster_snapshot_id INTEGER NOT NULL REFERENCES account_roster_snapshots(id) ON DELETE RESTRICT,
    roster_members_sha256 TEXT NOT NULL CHECK(length(roster_members_sha256)=64),
    business_day TEXT NOT NULL CHECK(length(business_day)=10),
    sequence INTEGER NOT NULL CHECK(sequence>=1),
    sealed_at TEXT NOT NULL,
    status TEXT NOT NULL CHECK(length(trim(status))>0),
    complete INTEGER NOT NULL CHECK(complete IN (0,1)),
    partial_publishable INTEGER NOT NULL CHECK(partial_publishable IN (0,1)),
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    summary_json TEXT NOT NULL CHECK(json_valid(summary_json)),
    evidence_json TEXT NOT NULL CHECK(json_valid(evidence_json)),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    receipt_sha256 TEXT NOT NULL UNIQUE CHECK(length(receipt_sha256)=64),
    recorded_at TEXT NOT NULL,
    UNIQUE(activation_id, business_day, sequence),
    CHECK(complete=0 OR partial_publishable=0)
);

CREATE INDEX IF NOT EXISTS idx_day_receipts_cutoff
ON profile_day_coverage_receipts(activation_id, business_day, sequence DESC, sealed_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS runtime_receipt_revocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_receipt_id INTEGER UNIQUE REFERENCES scan_verification_receipts(id) ON DELETE RESTRICT,
    day_receipt_id INTEGER UNIQUE REFERENCES profile_day_coverage_receipts(id) ON DELETE RESTRICT,
    revoked_at TEXT NOT NULL,
    actor TEXT NOT NULL CHECK(length(trim(actor))>0),
    reason TEXT NOT NULL CHECK(length(trim(reason))>0),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    revocation_sha256 TEXT NOT NULL UNIQUE CHECK(length(revocation_sha256)=64),
    metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),
    created_at TEXT NOT NULL,
    CHECK((scan_receipt_id IS NOT NULL)<>(day_receipt_id IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS pipeline_paid_drain_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    drain_id TEXT NOT NULL CHECK(length(trim(drain_id))>0),
    target_activation_id INTEGER NOT NULL
        REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 3),
    event_type TEXT NOT NULL CHECK(event_type IN ('start','sealed','release')),
    previous_event_id INTEGER REFERENCES pipeline_paid_drain_events(id) ON DELETE RESTRICT,
    previous_event_hash TEXT CHECK(previous_event_hash IS NULL OR length(previous_event_hash)=64),
    bridge_previous_event_id INTEGER,
    bridge_run_id INTEGER UNIQUE REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    bridge_attempt_id INTEGER UNIQUE REFERENCES scheduler_run_attempts(id) ON DELETE RESTRICT,
    payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    event_hash TEXT NOT NULL UNIQUE CHECK(length(event_hash)=64),
    created_at TEXT NOT NULL,
    UNIQUE(drain_id, sequence),
    UNIQUE(drain_id, event_type),
    CHECK(
        (previous_event_id IS NULL AND previous_event_hash IS NULL)
        OR (previous_event_id IS NOT NULL AND previous_event_hash IS NOT NULL)
    ),
    CHECK(
        (bridge_run_id IS NULL AND bridge_attempt_id IS NULL AND bridge_previous_event_id IS NULL)
        OR (bridge_run_id IS NOT NULL AND bridge_attempt_id IS NOT NULL)
    ),
    CHECK(
        (event_type='start' AND sequence=1)
        OR (event_type='sealed' AND sequence=2)
        OR (event_type='release' AND sequence=3)
    )
);

CREATE TABLE IF NOT EXISTS paid_provider_dispatch_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dispatch_id TEXT NOT NULL CHECK(length(trim(dispatch_id))>0),
    sequence INTEGER NOT NULL CHECK(sequence BETWEEN 1 AND 3),
    event_type TEXT NOT NULL CHECK(event_type IN (
        'reserved','send_marked','succeeded','failed','billing_unknown','not_sent'
    )),
    provider TEXT NOT NULL CHECK(length(trim(provider))>0),
    operation TEXT NOT NULL CHECK(length(trim(operation))>0),
    activation_id INTEGER NOT NULL REFERENCES acquisition_profile_activations(id) ON DELETE RESTRICT,
    business_day TEXT NOT NULL CHECK(length(business_day)=10),
    permit_event_id INTEGER NOT NULL REFERENCES pipeline_paid_drain_events(id) ON DELETE RESTRICT,
    scheduler_run_id INTEGER NOT NULL REFERENCES scheduler_runs(id) ON DELETE RESTRICT,
    scheduler_attempt_id INTEGER NOT NULL REFERENCES scheduler_run_attempts(id) ON DELETE RESTRICT,
    scope_json TEXT NOT NULL CHECK(json_valid(scope_json)),
    provider_usage_id INTEGER REFERENCES provider_usage(id) ON DELETE RESTRICT,
    fetch_slot_id INTEGER REFERENCES fetch_slots(id) ON DELETE RESTRICT,
    fetch_attempt_id INTEGER REFERENCES fetch_attempts(id) ON DELETE RESTRICT,
    raw_response_id INTEGER REFERENCES provider_raw_responses(id) ON DELETE RESTRICT,
    cursor_identity_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(cursor_identity_json)),
    previous_event_id INTEGER REFERENCES paid_provider_dispatch_events(id) ON DELETE RESTRICT,
    previous_event_hash TEXT CHECK(previous_event_hash IS NULL OR length(previous_event_hash)=64),
    contract_version TEXT NOT NULL CHECK(length(trim(contract_version))>0),
    event_hash TEXT NOT NULL UNIQUE CHECK(length(event_hash)=64),
    created_at TEXT NOT NULL,
    UNIQUE(dispatch_id, sequence),
    UNIQUE(dispatch_id, event_type),
    CHECK(
        (previous_event_id IS NULL AND previous_event_hash IS NULL)
        OR (previous_event_id IS NOT NULL AND previous_event_hash IS NOT NULL)
    ),
    CHECK(
        (event_type='reserved' AND sequence=1)
        OR (event_type IN ('send_marked','not_sent') AND sequence=2)
        OR (event_type IN ('succeeded','failed','billing_unknown') AND sequence=3)
    )
);

CREATE INDEX IF NOT EXISTS idx_paid_dispatch_activation_day
ON paid_provider_dispatch_events(activation_id, business_day, provider, operation, id);

CREATE TRIGGER IF NOT EXISTS trg_roster_snapshots_no_update
BEFORE UPDATE ON account_roster_snapshots
BEGIN
    SELECT RAISE(ABORT, 'accepted roster snapshots are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_snapshots_no_delete
BEFORE DELETE ON account_roster_snapshots
BEGIN
    SELECT RAISE(ABORT, 'accepted roster snapshots are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_no_update
BEFORE UPDATE ON account_roster_members
BEGIN
    SELECT RAISE(ABORT, 'accepted roster members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_no_delete
BEFORE DELETE ON account_roster_members
BEGIN
    SELECT RAISE(ABORT, 'accepted roster members are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_contract
BEFORE INSERT ON account_roster_members
WHEN NOT EXISTS (
    SELECT 1 FROM account_roster_snapshots s
    JOIN account_platform_identities i ON i.id=NEW.account_identity_id
    WHERE s.id=NEW.snapshot_id
      AND i.platform=NEW.platform
      AND (NEW.uid IS NULL OR i.uid=NEW.uid)
      AND (
        (s.source_family='matrix'
         AND NEW.matrix_account_id IS NOT NULL
         AND NEW.profile_ref IS NOT NULL
         AND NEW.member_key='matrix:'||NEW.platform||':'||NEW.matrix_account_id)
        OR
        (s.source_family='system'
         AND NEW.uid IS NOT NULL
         AND NEW.matrix_account_id IS NULL
         AND NEW.member_key='uid:'||NEW.platform||':'||NEW.uid)
      )
)
BEGIN
    SELECT RAISE(ABORT, 'roster member differs from its family or identity');
END;

CREATE TRIGGER IF NOT EXISTS trg_roster_members_declared_count
BEFORE INSERT ON account_roster_members
WHEN (
    SELECT COUNT(*) FROM account_roster_members WHERE snapshot_id=NEW.snapshot_id
) >= (
    SELECT member_count FROM account_roster_snapshots WHERE id=NEW.snapshot_id
)
BEGIN
    SELECT RAISE(ABORT, 'accepted roster exceeds declared member count');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_metrics_no_update
BEFORE UPDATE ON account_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'account metric observations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_metrics_no_delete
BEFORE DELETE ON account_metric_observations
BEGIN
    SELECT RAISE(ABORT, 'account metric observations are immutable');
END;

CREATE TRIGGER IF NOT EXISTS trg_profile_activations_chain
BEFORE INSERT ON acquisition_profile_activations
WHEN NEW.previous_activation_id IS NOT (
         SELECT id FROM acquisition_profile_activations ORDER BY id DESC LIMIT 1
     )
  OR NEW.previous_activation_sha256 IS NOT (
         SELECT activation_sha256 FROM acquisition_profile_activations ORDER BY id DESC LIMIT 1
     )
BEGIN
    SELECT RAISE(ABORT, 'activation event chain is disconnected');
END;

CREATE TRIGGER IF NOT EXISTS trg_profile_activations_roster
BEFORE INSERT ON acquisition_profile_activations
WHEN NOT EXISTS (
    SELECT 1 FROM account_roster_snapshots
    WHERE id=NEW.roster_snapshot_id
      AND members_sha256=NEW.roster_members_sha256
      AND source_family=CASE NEW.profile_id
          WHEN 'matrix_hybrid_v1' THEN 'matrix'
          WHEN 'tikhub_managed_v1' THEN 'system'
      END
)
BEGIN
    SELECT RAISE(ABORT, 'activation roster binding is invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_profile_activations_schedule
BEFORE INSERT ON acquisition_profile_activations
WHEN EXISTS (
    SELECT 1 FROM acquisition_profile_activations a
    WHERE a.effective_at=NEW.effective_at
      AND NOT EXISTS (
          SELECT 1 FROM activation_cancellations c WHERE c.activation_id=a.id
      )
)
BEGIN
    SELECT RAISE(ABORT, 'activation effective time is already occupied');
END;

CREATE TRIGGER IF NOT EXISTS trg_profile_activations_no_update
BEFORE UPDATE ON acquisition_profile_activations
BEGIN
    SELECT RAISE(ABORT, 'profile activations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_profile_activations_no_delete
BEFORE DELETE ON acquisition_profile_activations
BEGIN
    SELECT RAISE(ABORT, 'profile activations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_activation_cancellations_contract
BEFORE INSERT ON activation_cancellations
WHEN NOT EXISTS (
    SELECT 1 FROM acquisition_profile_activations
    WHERE id=NEW.activation_id AND NEW.cancelled_at<effective_at
)
BEGIN
    SELECT RAISE(ABORT, 'only a future activation can be cancelled');
END;

CREATE TRIGGER IF NOT EXISTS trg_activation_cancellations_no_update
BEFORE UPDATE ON activation_cancellations
BEGIN
    SELECT RAISE(ABORT, 'activation cancellations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_activation_cancellations_no_delete
BEFORE DELETE ON activation_cancellations
BEGIN
    SELECT RAISE(ABORT, 'activation cancellations are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_state_events_contract
BEFORE INSERT ON account_state_events
WHEN NOT EXISTS (
    SELECT 1 FROM account_platform_identities i JOIN accounts a ON a.id=i.account_id
    WHERE i.id=NEW.account_identity_id AND a.enabled=NEW.new_enabled
)
OR EXISTS (
    SELECT 1 FROM account_state_events previous
    WHERE previous.account_identity_id=NEW.account_identity_id
      AND previous.id=(
          SELECT MAX(id) FROM account_state_events
          WHERE account_identity_id=NEW.account_identity_id
      )
      AND previous.new_enabled<>NEW.old_enabled
)
OR EXISTS (
    SELECT 1 FROM account_state_events previous
    WHERE previous.account_identity_id=NEW.account_identity_id
      AND previous.effective_at>=NEW.effective_at
)
BEGIN
    SELECT RAISE(ABORT, 'account state event does not match the atomic account update');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_state_events_no_update
BEFORE UPDATE ON account_state_events
BEGIN
    SELECT RAISE(ABORT, 'account state events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_account_state_events_no_delete
BEFORE DELETE ON account_state_events
BEGIN
    SELECT RAISE(ABORT, 'account state events are append-only');
END;

CREATE TRIGGER IF NOT EXISTS trg_scan_receipts_binding
BEFORE INSERT ON scan_verification_receipts
WHEN NOT EXISTS (
    SELECT 1 FROM scheduler_run_attempts
    WHERE id=NEW.source_bridge_attempt_id AND scheduler_run_id=NEW.source_bridge_run_id
)
OR NOT EXISTS (
    SELECT 1 FROM scheduler_run_attempts
    WHERE id=NEW.scan_attempt_id AND scheduler_run_id=NEW.scan_run_id
)
BEGIN
    SELECT RAISE(ABORT, 'scan receipt run and attempt bindings differ');
END;

CREATE TRIGGER IF NOT EXISTS trg_day_receipts_binding
BEFORE INSERT ON profile_day_coverage_receipts
WHEN NOT EXISTS (
    SELECT 1 FROM scheduler_run_attempts
    WHERE id=NEW.source_bridge_attempt_id AND scheduler_run_id=NEW.source_bridge_run_id
)
OR NOT EXISTS (
    SELECT 1 FROM acquisition_profile_activations a
    WHERE a.id=NEW.activation_id
      AND a.profile_id=NEW.profile_id
      AND a.roster_snapshot_id=NEW.roster_snapshot_id
      AND a.roster_members_sha256=NEW.roster_members_sha256
)
BEGIN
    SELECT RAISE(ABORT, 'day receipt bindings differ');
END;

CREATE TRIGGER IF NOT EXISTS trg_scan_receipts_no_update
BEFORE UPDATE ON scan_verification_receipts
BEGIN SELECT RAISE(ABORT, 'scan receipts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_scan_receipts_no_delete
BEFORE DELETE ON scan_verification_receipts
BEGIN SELECT RAISE(ABORT, 'scan receipts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_day_receipts_no_update
BEFORE UPDATE ON profile_day_coverage_receipts
BEGIN SELECT RAISE(ABORT, 'day receipts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_day_receipts_no_delete
BEFORE DELETE ON profile_day_coverage_receipts
BEGIN SELECT RAISE(ABORT, 'day receipts are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_receipt_revocations_no_update
BEFORE UPDATE ON runtime_receipt_revocations
BEGIN SELECT RAISE(ABORT, 'receipt revocations are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_receipt_revocations_no_delete
BEFORE DELETE ON runtime_receipt_revocations
BEGIN SELECT RAISE(ABORT, 'receipt revocations are append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_paid_drain_events_binding
BEFORE INSERT ON pipeline_paid_drain_events
WHEN (NEW.bridge_attempt_id IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM scheduler_run_attempts
    WHERE id=NEW.bridge_attempt_id AND scheduler_run_id=NEW.bridge_run_id
))
OR NOT EXISTS (
    SELECT 1 FROM acquisition_profile_activations a
    WHERE a.id=NEW.target_activation_id
      AND julianday(a.created_at)<=julianday(NEW.created_at)
      AND NOT EXISTS (
          SELECT 1 FROM activation_cancellations c
          WHERE c.activation_id=a.id AND julianday(c.cancelled_at)<=julianday(NEW.created_at)
      )
)
OR NEW.previous_event_id IS NOT (
    SELECT id FROM pipeline_paid_drain_events ORDER BY id DESC LIMIT 1
)
OR NEW.previous_event_hash IS NOT (
    SELECT event_hash FROM pipeline_paid_drain_events ORDER BY id DESC LIMIT 1
)
OR (NEW.event_type='start' AND EXISTS (
    SELECT 1 FROM pipeline_paid_drain_events WHERE id=(
        SELECT MAX(id) FROM pipeline_paid_drain_events
    ) AND event_type<>'release'
))
OR (NEW.event_type='sealed' AND NOT EXISTS (
    SELECT 1 FROM pipeline_paid_drain_events
    WHERE id=NEW.previous_event_id AND drain_id=NEW.drain_id
      AND target_activation_id=NEW.target_activation_id AND event_type='start'
))
OR (NEW.event_type='release' AND NOT EXISTS (
    SELECT 1 FROM pipeline_paid_drain_events
    WHERE id=NEW.previous_event_id AND drain_id=NEW.drain_id
      AND target_activation_id=NEW.target_activation_id AND event_type='sealed'
))
BEGIN
    SELECT RAISE(ABORT, 'paid drain event chain is invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_paid_drain_events_no_update
BEFORE UPDATE ON pipeline_paid_drain_events
BEGIN SELECT RAISE(ABORT, 'paid drain events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_paid_drain_events_no_delete
BEFORE DELETE ON pipeline_paid_drain_events
BEGIN SELECT RAISE(ABORT, 'paid drain events are append-only'); END;

CREATE TRIGGER IF NOT EXISTS trg_paid_dispatch_events_binding
BEFORE INSERT ON paid_provider_dispatch_events
WHEN NOT EXISTS (
    SELECT 1 FROM pipeline_paid_drain_events permit
    WHERE permit.id=NEW.permit_event_id AND permit.event_type='release'
      AND permit.target_activation_id=NEW.activation_id
      AND julianday(permit.created_at)<=julianday(NEW.created_at)
)
OR (NEW.event_type IN ('reserved','send_marked') AND NOT EXISTS (
    SELECT 1 FROM acquisition_profile_activations active
    WHERE active.id=NEW.activation_id
      AND julianday(active.effective_at)<=julianday(NEW.created_at)
      AND NOT EXISTS (
          SELECT 1 FROM activation_cancellations cancelled
          WHERE cancelled.activation_id=active.id
      )
      AND NOT EXISTS (
          SELECT 1 FROM acquisition_profile_activations later
          WHERE julianday(later.effective_at)<=julianday(NEW.created_at)
            AND NOT EXISTS (
                SELECT 1 FROM activation_cancellations later_cancelled
                WHERE later_cancelled.activation_id=later.id
            )
            AND (
                julianday(later.effective_at)>julianday(active.effective_at)
                OR (
                    julianday(later.effective_at)=julianday(active.effective_at)
                    AND later.id>active.id
                )
            )
      )
))
OR NOT EXISTS (
    SELECT 1 FROM scheduler_run_attempts
    WHERE id=NEW.scheduler_attempt_id AND scheduler_run_id=NEW.scheduler_run_id
)
OR (NEW.event_type='reserved' AND NEW.previous_event_id IS NOT NULL)
OR (NEW.event_type='reserved' AND NEW.fetch_attempt_id IS NOT NULL)
OR (NEW.event_type IN ('reserved','send_marked','not_sent')
    AND NEW.raw_response_id IS NOT NULL)
OR (NEW.event_type IN ('send_marked','not_sent') AND NOT EXISTS (
    SELECT 1 FROM paid_provider_dispatch_events previous
    WHERE previous.id=NEW.previous_event_id
      AND previous.dispatch_id=NEW.dispatch_id
      AND previous.event_type='reserved'
      AND previous.event_hash=NEW.previous_event_hash
      AND previous.provider=NEW.provider
      AND previous.operation=NEW.operation
      AND previous.activation_id=NEW.activation_id
      AND previous.business_day=NEW.business_day
      AND previous.permit_event_id=NEW.permit_event_id
      AND previous.scheduler_run_id=NEW.scheduler_run_id
      AND previous.scheduler_attempt_id=NEW.scheduler_attempt_id
      AND previous.scope_json=NEW.scope_json
      AND previous.provider_usage_id IS NEW.provider_usage_id
      AND previous.fetch_slot_id IS NEW.fetch_slot_id
      AND previous.raw_response_id IS NEW.raw_response_id
      AND previous.cursor_identity_json=NEW.cursor_identity_json
      AND (
          NEW.event_type='send_marked'
          OR previous.fetch_attempt_id IS NEW.fetch_attempt_id
      )
))
OR (NEW.event_type IN ('succeeded','failed','billing_unknown') AND NOT EXISTS (
    SELECT 1 FROM paid_provider_dispatch_events previous
    WHERE previous.id=NEW.previous_event_id
      AND previous.dispatch_id=NEW.dispatch_id
      AND previous.event_type='send_marked'
      AND previous.event_hash=NEW.previous_event_hash
      AND previous.provider=NEW.provider
      AND previous.operation=NEW.operation
      AND previous.activation_id=NEW.activation_id
      AND previous.business_day=NEW.business_day
      AND previous.permit_event_id=NEW.permit_event_id
      AND previous.scheduler_run_id=NEW.scheduler_run_id
      AND previous.scheduler_attempt_id=NEW.scheduler_attempt_id
      AND previous.scope_json=NEW.scope_json
      AND previous.provider_usage_id IS NEW.provider_usage_id
      AND previous.fetch_slot_id IS NEW.fetch_slot_id
      AND previous.fetch_attempt_id IS NEW.fetch_attempt_id
      AND previous.cursor_identity_json=NEW.cursor_identity_json
))
BEGIN
    SELECT RAISE(ABORT, 'paid dispatch event chain is invalid');
END;

CREATE TRIGGER IF NOT EXISTS trg_paid_dispatch_events_no_update
BEFORE UPDATE ON paid_provider_dispatch_events
BEGIN SELECT RAISE(ABORT, 'paid dispatch events are append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_paid_dispatch_events_no_delete
BEFORE DELETE ON paid_provider_dispatch_events
BEGIN SELECT RAISE(ABORT, 'paid dispatch events are append-only'); END;
"""


_DECLARATION = re.compile(
    r"CREATE (?:UNIQUE )?(TABLE|INDEX|TRIGGER) IF NOT EXISTS ([a-z_]+)\b"
)


def _statements(sql: str = PROFILE_SCHEMA_SQL) -> list[str]:
    statements: list[str] = []
    pending = ""
    for line in sql.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statements.append(pending.strip().removesuffix(";").strip())
            pending = ""
    if pending.strip():
        raise RuntimeError("incomplete schema19 SQL statement")
    return statements


def expected_schema_objects() -> dict[tuple[str, str], str]:
    from .schema_v18 import expected_schema_objects as expected_v18

    expected = expected_v18()
    expected.pop(("trigger", "trg_roster_members_identity_platform"))
    for statement in _statements():
        match = _DECLARATION.match(statement)
        if match is None:
            raise RuntimeError("invalid schema19 object declaration")
        expected[(match[1].lower(), match[2])] = statement.replace(
            " IF NOT EXISTS", "", 1
        )
    return expected


def validate_structure(connection: sqlite3.Connection) -> None:
    from . import storage

    expected = expected_schema_objects()
    actual = {
        (str(row[0]), str(row[1])): str(row[2])
        for row in connection.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'"
        )
    }
    if set(actual) != set(expected):
        raise storage.SchemaMigrationError(
            "schema v19 object set differs from sealed contract"
        )
    for key, definition in expected.items():
        allowed = {storage._normalized_historical_sql(definition)}
        if key[0] == "table" and key[1] in storage._historical_table_variants():
            allowed.add(
                storage._normalized_historical_sql(
                    storage._historical_table_variants()[key[1]]
                )
            )
        if storage._normalized_historical_sql(actual[key]) not in allowed:
            raise storage.SchemaMigrationError(
                f"schema v19 object definition drifted: {key[1]}"
            )


def _canonical(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc).isoformat(timespec="microseconds").replace(
        "+00:00", "Z"
    )


def _fail(message: str) -> None:
    from .storage import SchemaMigrationError

    raise SchemaMigrationError(message)


def _legacy_drain_cycles(drains: list[Any]) -> list[list[Any]]:
    """Partition a validated global bridge chain into migration drain cycles."""

    cycles: list[list[Any]] = []
    seen: set[str] = set()
    for receipt in drains:
        if not cycles or receipt.drain_id != cycles[-1][0].drain_id:
            if receipt.drain_id in seen:
                _fail("schema18 paid drain identifier was reused")
            seen.add(receipt.drain_id)
            cycles.append([])
        cycles[-1].append(receipt)
    for index, cycle in enumerate(cycles):
        event_types = [receipt.event_type for receipt in cycle]
        if index < len(cycles) - 1:
            if event_types != ["start", "sealed", "release"]:
                _fail("schema18 prior paid drain cycle is incomplete")
        elif event_types not in (
            ["start", "sealed"],
            ["start", "sealed", "release"],
        ):
            _fail("schema18 latest paid drain is not a sealed migration boundary")
    return cycles


def _legacy_activation(
    connection: sqlite3.Connection, drains: list[Any]
) -> dict[str, Any] | None:
    from .profile_activations import CONTRACT_VERSION, activation_digest
    from .snapshot_contract import validate_descriptor
    from .source_routing import load_policy

    rows = connection.execute(
        "SELECT * FROM scheduler_runs WHERE job_id=? ORDER BY id",
        (LEGACY_ACTIVATION_JOB,),
    ).fetchall()
    if not rows:
        if drains:
            _fail("schema18 paid drain has no bridge activation")
        return None
    if len(rows) != 1:
        _fail("schema18 activation bridge is ambiguous")
    run = dict(rows[0])
    attempts = connection.execute(
        "SELECT * FROM scheduler_run_attempts WHERE scheduler_run_id=? ORDER BY id",
        (run["id"],),
    ).fetchall()
    if len(attempts) != 1:
        _fail("schema18 activation bridge is not one terminal run and attempt")
    attempt = dict(attempts[0])
    if (
        run["status"] != "succeeded"
        or run["started_at"] is None
        or run["completed_at"] is None
        or attempt["attempt_number"] != 1
        or attempt["invocation_source"] != "operator_retry"
        or attempt["status"] != "succeeded"
        or attempt["started_at"] != run["started_at"]
        or attempt["completed_at"] != run["completed_at"]
        or attempt["details_json"] != run["details_json"]
    ):
        _fail("schema18 activation bridge is not one terminal run and attempt")
    try:
        details = json.loads(str(run["details_json"]))
    except (TypeError, ValueError):
        _fail("schema18 activation bridge details are invalid")
    expected_fields = {
        "contract_version",
        "mode",
        "cutover_at",
        "roster_snapshot_id",
        "roster_snapshot_hash",
        "source_policy_sha256",
        "snapshot_contract",
        "schema_version",
        "schema_migration",
        "report_version",
        "active_release_id",
        "matcher_rule_sha256",
    }
    if (
        not isinstance(details, dict)
        or set(details) != expected_fields
        or details.get("contract_version") != "matrix-first-pipeline-v1"
        or details.get("mode") != "active"
        or details.get("schema_version") != 18
        or details.get("schema_migration") != "matrix-roster-source-routing"
        or details.get("report_version") != LEGACY_SCHEMA18_REPORT_VERSION
        or details.get("source_policy_sha256") != _digest(load_policy())
        or type(details.get("roster_snapshot_id")) is not int
        or not isinstance(details.get("roster_snapshot_hash"), str)
        or len(details["roster_snapshot_hash"]) != 64
        or not isinstance(details.get("cutover_at"), str)
    ):
        _fail("schema18 activation bridge contract is invalid")
    try:
        validate_descriptor(details.get("snapshot_contract"))
        started_at = _timestamp(str(run["started_at"]))
        completed_at = _timestamp(str(run["completed_at"]))
        scheduled_for = _timestamp(str(run["scheduled_for"]))
        effective_at = _timestamp(str(details["cutover_at"]))
    except (TypeError, ValueError):
        _fail("schema18 activation bridge contract is invalid")
    if (
        started_at != effective_at
        or scheduled_for != effective_at
        or completed_at < started_at
    ):
        _fail("schema18 activation bridge timing is invalid")
    roster = connection.execute(
        "SELECT members_sha256 FROM account_roster_snapshots WHERE id=?",
        (details["roster_snapshot_id"],),
    ).fetchone()
    if roster is None or roster["members_sha256"] != details["roster_snapshot_hash"]:
        _fail("schema18 activation bridge roster differs")
    releases = connection.execute(
        "SELECT er.*,tv.status taxonomy_status FROM evaluation_releases er "
        "JOIN taxonomy_versions tv ON tv.version=er.taxonomy_version "
        "WHERE er.status='active'"
    ).fetchall()
    if (
        len(releases) != 1
        or releases[0]["id"] != details.get("active_release_id")
        or releases[0]["matcher_rule_sha256"] != details.get("matcher_rule_sha256")
        or releases[0]["id"] != "evaluation-v9__selling-points-v5.2"
        or releases[0]["rule_version"] != "evaluation-v9"
        or releases[0]["taxonomy_version"] != "selling-points-v5.2"
        or releases[0]["taxonomy_status"] != "published"
        or re.fullmatch(r"[0-9a-f]{64}", str(releases[0]["matcher_rule_sha256"]))
        is None
    ):
        _fail("schema18 activation bridge release differs")
    cycles = _legacy_drain_cycles(drains)
    if not cycles:
        _fail("schema18 activation bridge requires a sealed paid drain")
    build_receipt_sha256: object = None
    for cycle in cycles:
        binding = cycle[0].payload.get("binding")
        if (
            not isinstance(binding, dict)
            or binding.get("source_activation_id") != int(run["id"])
            or binding.get("target_activation_id") != "schema19-bridge"
        ):
            _fail("schema18 activation bridge paid drain binding is invalid")
        build_receipt_sha256 = binding.get("build_receipt_sha256")
        if re.fullmatch(r"[0-9a-f]{64}", str(build_receipt_sha256)) is None:
            _fail("schema18 activation bridge build receipt is invalid")
    metadata = {
        "migration": "schema18-activation-bridge-v1",
        "legacy_run_id": int(run["id"]),
        "legacy_attempt_id": int(attempt["id"]),
        "legacy_details_sha256": hashlib.sha256(
            str(run["details_json"]).encode("utf-8")
        ).hexdigest(),
    }
    value = {
        "profile_id": MATRIX_PROFILE,
        "roster_snapshot_id": int(details["roster_snapshot_id"]),
        "roster_members_sha256": str(details["roster_snapshot_hash"]),
        "effective_at": effective_at,
        "contract_version": CONTRACT_VERSION,
        "build_receipt_sha256": str(build_receipt_sha256),
        "previous_activation_id": None,
        "previous_activation_sha256": None,
        "actor": "schema18_migration",
        "reason": "legacy bridge",
        "metadata": metadata,
        "created_at": started_at,
    }
    value["activation_sha256"] = activation_digest(value)
    return value


def _runtime_bridges(connection: sqlite3.Connection) -> dict[str, list[dict[str, Any]]]:
    from . import runtime_receipts

    result: dict[str, list[dict[str, Any]]] = {"scan": [], "day": []}
    for job, kind in (
        (runtime_receipts.SCAN_RECEIPT_JOB, "scan"),
        (runtime_receipts.DAY_RECEIPT_JOB, "day"),
    ):
        for row in connection.execute(
            "SELECT * FROM scheduler_runs WHERE job_id=? ORDER BY id", (job,)
        ):
            details = runtime_receipts._read_one_shot(
                connection, row, expected_job=job
            )
            if kind == "scan":
                source = runtime_receipts.read_scan_verification_receipt(
                    connection, int(details.get("summary", {}).get("scan_run_id", 0))
                )
                if source is None or source.get("self_sha256") != details.get(
                    "self_sha256"
                ):
                    _fail("schema18 scan receipt source binding is invalid")
            result[kind].append(details)
    return result


def _drain_bridges(connection: sqlite3.Connection) -> list[Any]:
    from .paid_drain import _validated_chain

    return list(_validated_chain(connection))


def migration_plan(connection: sqlite3.Connection) -> dict[str, Any]:
    from . import storage

    storage.require_schema_compatibility(
        connection, supported_versions=frozenset({18})
    )
    if connection.execute("PRAGMA foreign_key_check").fetchall():
        _fail("schema19 source has foreign-key violations")
    drains = _drain_bridges(connection)
    activation = _legacy_activation(connection, drains)
    receipts = _runtime_bridges(connection)
    day_activation_ids = {
        details.get("scope", {}).get("activation_id") for details in receipts["day"]
    }
    legacy_id = (
        activation["metadata"]["legacy_run_id"] if activation is not None else None
    )
    if day_activation_ids - {legacy_id}:
        _fail("schema18 day receipt references an unknown activation")
    return {
        "contract_version": "schema18-to-19-migration-plan-v1",
        "source_version": 18,
        "candidate_version": 19,
        "roster_snapshot_count": int(
            connection.execute("SELECT COUNT(*) FROM account_roster_snapshots").fetchone()[0]
        ),
        "roster_member_count": int(
            connection.execute("SELECT COUNT(*) FROM account_roster_members").fetchone()[0]
        ),
        "legacy_activation": activation,
        "scan_receipts": receipts["scan"],
        "day_receipts": receipts["day"],
        "drain_receipts": [receipt.as_dict() for receipt in drains],
        "paid_dispatch_import_count": 0,
    }


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    quoted = '"' + table.replace('"', '""') + '"'
    return [
        str(row[1]) for row in connection.execute(f"PRAGMA table_info({quoted})")
    ]


def _row_dicts(
    connection: sqlite3.Connection,
    query: str,
    parameters: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    cursor = connection.execute(query, parameters)
    names = [str(description[0]) for description in cursor.description or ()]
    return [dict(zip(names, tuple(row), strict=True)) for row in cursor.fetchall()]


def _encoded_row(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(value),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda item: {"bytes_hex": bytes(item).hex()},
        ).encode("utf-8")
        + b"\n"
    )


def _rows_sha256(rows: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(_encoded_row(row))
    return digest.hexdigest()


def _ordered_table_query(connection: sqlite3.Connection, table: str) -> str:
    columns = _table_columns(connection, table)
    quoted_table = '"' + table.replace('"', '""') + '"'
    primary = sorted(
        (
            (int(row[5]), str(row[1]))
            for row in connection.execute(f"PRAGMA table_info({quoted_table})")
            if int(row[5]) > 0
        )
    )
    order_columns = [name for _, name in primary] or columns
    order = ",".join('"' + name.replace('"', '""') + '"' for name in order_columns)
    return f"SELECT * FROM {quoted_table} ORDER BY {order}"


def _ordered_table_rows(
    connection: sqlite3.Connection, table: str
) -> list[dict[str, Any]]:
    return _row_dicts(connection, _ordered_table_query(connection, table))


def _table_manifest(connection: sqlite3.Connection, table: str) -> dict[str, Any]:
    cursor = connection.execute(_ordered_table_query(connection, table))
    names = [str(description[0]) for description in cursor.description or ()]
    digest = hashlib.sha256()
    count = 0
    for row in cursor:
        digest.update(_encoded_row(dict(zip(names, tuple(row), strict=True))))
        count += 1
    return {"columns": names, "row_count": count, "sha256": digest.hexdigest()}


def _sequence_state(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        str(row[0]): int(row[1])
        for row in connection.execute("SELECT name,seq FROM sqlite_sequence ORDER BY name")
    }


def _expected_roster_rows(
    source: sqlite3.Connection,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    snapshots = _row_dicts(source, "SELECT * FROM account_roster_snapshots ORDER BY id")
    expected_snapshots = [
        {
            "id": row["id"],
            "source_family": "matrix",
            **{key: value for key, value in row.items() if key != "id"},
        }
        for row in snapshots
    ]
    members = _row_dicts(
        source,
        "SELECT m.*,i.uid AS identity_uid FROM account_roster_members m "
        "JOIN account_platform_identities i ON i.id=m.account_identity_id "
        "ORDER BY m.snapshot_id,m.account_identity_id",
    )
    expected_members = [
        {
            "snapshot_id": row["snapshot_id"],
            "account_identity_id": row["account_identity_id"],
            "platform": row["platform"],
            "member_key": (
                f"matrix:{row['platform']}:{row['matrix_account_id']}"
            ),
            "uid": row["identity_uid"],
            "matrix_account_id": row["matrix_account_id"],
            "profile_ref": row["profile_ref"],
            "monitoring_status": row["monitoring_status"],
            "authorization_status": row["authorization_status"],
            "monitoring_started_at": row["monitoring_started_at"],
            "metadata_json": row["metadata_json"],
        }
        for row in members
    ]
    return expected_snapshots, expected_members


def _expected_bridge_rows(plan: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    activation = plan["legacy_activation"]
    activation_id = 1 if activation is not None else None
    activations = (
        [
            {
                "id": 1,
                "profile_id": activation["profile_id"],
                "roster_snapshot_id": activation["roster_snapshot_id"],
                "roster_members_sha256": activation["roster_members_sha256"],
                "effective_at": activation["effective_at"],
                "contract_version": activation["contract_version"],
                "build_receipt_sha256": activation["build_receipt_sha256"],
                "previous_activation_id": activation["previous_activation_id"],
                "previous_activation_sha256": activation[
                    "previous_activation_sha256"
                ],
                "activation_sha256": activation["activation_sha256"],
                "actor": activation["actor"],
                "reason": activation["reason"],
                "metadata_json": _canonical(activation["metadata"]),
                "created_at": activation["created_at"],
            }
        ]
        if activation is not None
        else []
    )
    scans = [
        {
            "id": index,
            "source_bridge_run_id": receipt["run_id"],
            "source_bridge_attempt_id": receipt["attempt_id"],
            "scan_run_id": receipt["scope"]["scan_run_id"],
            "scan_attempt_id": receipt["scope"]["scan_attempt_id"],
            "scan_status": receipt["scope"]["scan_status"],
            "scope_json": _canonical(receipt["scope"]),
            "summary_json": _canonical(receipt["summary"]),
            "evidence_json": _canonical(receipt["evidence"]),
            "contract_version": "schema18-runtime-receipt-bridge-v2",
            "receipt_sha256": receipt["self_sha256"],
            "recorded_at": receipt["recorded_at"],
        }
        for index, receipt in enumerate(plan["scan_receipts"], start=1)
    ]
    days = [
        {
            "id": index,
            "source_bridge_run_id": receipt["run_id"],
            "source_bridge_attempt_id": receipt["attempt_id"],
            "activation_id": activation_id,
            "profile_id": MATRIX_PROFILE,
            "roster_snapshot_id": receipt["scope"]["roster_snapshot_id"],
            "roster_members_sha256": receipt["scope"]["roster_snapshot_hash"],
            "business_day": receipt["scope"]["business_day"],
            "sequence": receipt["summary"]["sequence"],
            "sealed_at": receipt["summary"]["sealed_at"],
            "status": receipt["summary"]["status"],
            "complete": int(bool(receipt["summary"]["complete"])),
            "partial_publishable": int(
                bool(receipt["summary"]["coverage"].get("partial_publishable", False))
            ),
            "scope_json": _canonical(receipt["scope"]),
            "summary_json": _canonical(receipt["summary"]),
            "evidence_json": _canonical(receipt["evidence"]),
            "contract_version": "schema18-runtime-receipt-bridge-v2",
            "receipt_sha256": receipt["self_sha256"],
            "recorded_at": receipt["recorded_at"],
        }
        for index, receipt in enumerate(plan["day_receipts"], start=1)
    ]
    drains = [
        {
            "id": index,
            "drain_id": receipt["drain_id"],
            "target_activation_id": activation_id,
            "sequence": receipt["sequence"],
            "event_type": receipt["event_type"],
            "previous_event_id": index - 1 if index > 1 else None,
            "previous_event_hash": receipt["previous_event_hash"],
            "bridge_previous_event_id": receipt["previous_event_id"],
            "bridge_run_id": receipt["run_id"],
            "bridge_attempt_id": receipt["attempt_id"],
            "payload_json": _canonical(receipt["payload"]),
            "contract_version": "pipeline-paid-drain-bridge-v1",
            "event_hash": receipt["event_hash"],
            "created_at": receipt["created_at"],
        }
        for index, receipt in enumerate(plan["drain_receipts"], start=1)
    ]
    return {
        "acquisition_profile_activations": activations,
        "activation_cancellations": [],
        "account_state_events": [],
        "scan_verification_receipts": scans,
        "profile_day_coverage_receipts": days,
        "runtime_receipt_revocations": [],
        "pipeline_paid_drain_events": drains,
        "paid_provider_dispatch_events": [],
    }


def validate_lineage(
    source: sqlite3.Connection, candidate: sqlite3.Connection
) -> dict[str, Any]:
    """Independently prove the exact sealed schema-18 to schema-19 delta."""

    from . import storage

    plan = migration_plan(source)
    storage.require_schema_compatibility(
        candidate, supported_versions=frozenset({19})
    )
    validate_structure(candidate)
    source_tables = storage._table_names(source)
    candidate_tables = storage._table_names(candidate)
    if candidate_tables - source_tables != set(NEW_TABLES) or source_tables - candidate_tables:
        _fail("v19 lineage has unexpected added or removed tables")

    source_migrations = _row_dicts(
        source, "SELECT * FROM schema_migrations ORDER BY version"
    )
    candidate_migrations = _row_dicts(
        candidate, "SELECT * FROM schema_migrations ORDER BY version"
    )
    if (
        len(candidate_migrations) != len(source_migrations) + 1
        or candidate_migrations[:-1] != source_migrations
        or candidate_migrations[-1].get("version") != 19
        or candidate_migrations[-1].get("name") != MIGRATION_NAME
        or not candidate_migrations[-1].get("applied_at")
    ):
        _fail("v19 lineage changed historical migration records")

    retained: dict[str, Any] = {}
    excluded = {
        "schema_migrations",
        "account_roster_snapshots",
        "account_roster_members",
    }
    for table in sorted(source_tables - excluded):
        source_manifest = _table_manifest(source, table)
        candidate_manifest = _table_manifest(candidate, table)
        if source_manifest != candidate_manifest:
            _fail(f"v19 lineage changed protected rows or columns in {table}")
        retained[table] = source_manifest

    expected_snapshots, expected_members = _expected_roster_rows(source)
    candidate_snapshots = _row_dicts(
        candidate, "SELECT * FROM account_roster_snapshots ORDER BY id"
    )
    candidate_members = _row_dicts(
        candidate,
        "SELECT * FROM account_roster_members "
        "ORDER BY snapshot_id,account_identity_id",
    )
    if candidate_snapshots != expected_snapshots:
        _fail("v19 lineage changed the deterministic roster snapshot transform")
    if candidate_members != expected_members:
        _fail("v19 lineage changed the deterministic roster member transform")

    expected_bridges = _expected_bridge_rows(plan)
    bridge_manifest: dict[str, Any] = {}
    for table, expected_rows in expected_bridges.items():
        actual_rows = _ordered_table_rows(candidate, table)
        if actual_rows != expected_rows:
            _fail(f"v19 lineage changed migration bridge rows in {table}")
        bridge_manifest[table] = {
            "row_count": len(actual_rows),
            "sha256": _rows_sha256(actual_rows),
        }

    expected_sequences = _sequence_state(source)
    for table, rows in expected_bridges.items():
        if rows:
            expected_sequences[table] = int(rows[-1]["id"])
        else:
            expected_sequences.pop(table, None)
    candidate_sequences = _sequence_state(candidate)
    if candidate_sequences != expected_sequences:
        _fail("v19 lineage changed sqlite_sequence outside the sealed delta")

    for connection, label in ((source, "source"), (candidate, "candidate")):
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            _fail(f"v19 {label} has foreign-key violations")
        if str(connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            _fail(f"v19 {label} failed integrity_check")

    objects = [
        [str(row[0]), str(row[1]), str(row[2] or "")]
        for row in candidate.execute(
            "SELECT type,name,sql FROM sqlite_master "
            "WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
        )
    ]
    return {
        "schema_version": "dcar-v19-offline-allowed-differences-v1",
        "source_table_count": len(source_tables),
        "candidate_table_count": len(candidate_tables),
        "retained_table_count": len(retained),
        "retained_tables": retained,
        "roster_snapshots": {
            "row_count": len(candidate_snapshots),
            "sha256": _rows_sha256(candidate_snapshots),
        },
        "roster_members": {
            "row_count": len(candidate_members),
            "sha256": _rows_sha256(candidate_members),
        },
        "bridge_tables": bridge_manifest,
        "added_tables": sorted(NEW_TABLES),
        "removed_tables": [],
        "appended_migration_versions": [19],
        "sqlite_sequence": candidate_sequences,
        "schema_objects_sha256": hashlib.sha256(
            _canonical(objects).encode("utf-8")
        ).hexdigest(),
        "migration_plan_sha256": _digest(plan),
    }


def _drop_v18_roster_objects(connection: sqlite3.Connection) -> None:
    for kind, name in (
        ("TRIGGER", "trg_roster_snapshots_no_update"),
        ("TRIGGER", "trg_roster_snapshots_no_delete"),
        ("TRIGGER", "trg_roster_members_no_update"),
        ("TRIGGER", "trg_roster_members_no_delete"),
        ("TRIGGER", "trg_roster_members_identity_platform"),
        ("TRIGGER", "trg_roster_members_declared_count"),
        ("INDEX", "idx_roster_snapshots_current"),
        ("INDEX", "idx_roster_members_identity"),
    ):
        connection.execute(f"DROP {kind} IF EXISTS {name}")


def _insert_activation(
    connection: sqlite3.Connection, value: Mapping[str, Any]
) -> int:
    cursor = connection.execute(
        """INSERT INTO acquisition_profile_activations(
               profile_id,roster_snapshot_id,roster_members_sha256,effective_at,
               contract_version,build_receipt_sha256,previous_activation_id,
               previous_activation_sha256,activation_sha256,actor,reason,
               metadata_json,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            value["profile_id"],
            value["roster_snapshot_id"],
            value["roster_members_sha256"],
            value["effective_at"],
            value["contract_version"],
            value["build_receipt_sha256"],
            value["previous_activation_id"],
            value["previous_activation_sha256"],
            value["activation_sha256"],
            value["actor"],
            value["reason"],
            _canonical(value["metadata"]),
            value["created_at"],
        ),
    )
    return int(cursor.lastrowid or 0)


def _import_runtime_receipts(
    connection: sqlite3.Connection,
    plan: Mapping[str, Any],
    *,
    activation_id: int | None,
) -> None:
    for details in plan["scan_receipts"]:
        scope = details["scope"]
        summary = details["summary"]
        connection.execute(
            """INSERT INTO scan_verification_receipts(
                   source_bridge_run_id,source_bridge_attempt_id,scan_run_id,
                   scan_attempt_id,scan_status,scope_json,summary_json,evidence_json,
                   contract_version,receipt_sha256,recorded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                details["run_id"],
                details["attempt_id"],
                scope["scan_run_id"],
                scope["scan_attempt_id"],
                scope["scan_status"],
                _canonical(scope),
                _canonical(summary),
                _canonical(details["evidence"]),
                details["contract_version"],
                details["self_sha256"],
                details["recorded_at"],
            ),
        )
    for details in plan["day_receipts"]:
        if activation_id is None:
            _fail("schema18 day receipt has no bridge activation")
        scope = details["scope"]
        summary = details["summary"]
        coverage = summary.get("coverage")
        if not isinstance(coverage, dict):
            _fail("schema18 day receipt coverage is invalid")
        connection.execute(
            """INSERT INTO profile_day_coverage_receipts(
                   source_bridge_run_id,source_bridge_attempt_id,activation_id,
                   profile_id,roster_snapshot_id,roster_members_sha256,business_day,
                   sequence,sealed_at,status,complete,partial_publishable,scope_json,
                   summary_json,evidence_json,contract_version,receipt_sha256,recorded_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                details["run_id"],
                details["attempt_id"],
                activation_id,
                MATRIX_PROFILE,
                scope["roster_snapshot_id"],
                scope["roster_snapshot_hash"],
                scope["business_day"],
                summary["sequence"],
                summary["sealed_at"],
                summary["status"],
                int(bool(summary["complete"])),
                int(bool(coverage.get("partial_publishable", False))),
                _canonical(scope),
                _canonical(summary),
                _canonical(details["evidence"]),
                details["contract_version"],
                details["self_sha256"],
                details["recorded_at"],
            ),
        )


def _import_drain_events(
    connection: sqlite3.Connection,
    plan: Mapping[str, Any],
    *,
    activation_id: int | None,
) -> None:
    if plan["drain_receipts"] and activation_id is None:
        _fail("schema18 paid drain has no target activation")
    previous_id: int | None = None
    for event in plan["drain_receipts"]:
        cursor = connection.execute(
            """INSERT INTO pipeline_paid_drain_events(
                   drain_id,target_activation_id,sequence,event_type,previous_event_id,
                   previous_event_hash,bridge_previous_event_id,bridge_run_id,
                   bridge_attempt_id,payload_json,contract_version,event_hash,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event["drain_id"],
                activation_id,
                event["sequence"],
                event["event_type"],
                previous_id,
                event["previous_event_hash"],
                event["previous_event_id"],
                event["run_id"],
                event["attempt_id"],
                _canonical(event["payload"]),
                "pipeline-paid-drain-bridge-v1",
                event["event_hash"],
                event["created_at"],
            ),
        )
        previous_id = int(cursor.lastrowid or 0)


def migrate(connection: sqlite3.Connection) -> dict[str, Any]:
    from . import storage

    storage._require_initialization_safety(connection)
    if connection.in_transaction:
        _fail("v19 migration requires no active transaction")
    if int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) != 1:
        _fail("v19 migration requires foreign_keys=ON")
    legacy_alter = int(connection.execute("PRAGMA legacy_alter_table").fetchone()[0])
    connection.execute("PRAGMA foreign_keys=OFF")
    connection.execute("PRAGMA legacy_alter_table=ON")
    try:
        connection.execute("BEGIN IMMEDIATE")
        plan = migration_plan(connection)
        storage._migration_checkpoint("v19_preflight_complete")
        snapshots = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM account_roster_snapshots ORDER BY id"
            )
        ]
        members = [
            dict(row)
            for row in connection.execute(
                "SELECT m.*,i.uid FROM account_roster_members m "
                "JOIN account_platform_identities i ON i.id=m.account_identity_id "
                "ORDER BY m.snapshot_id,m.account_identity_id"
            )
        ]
        _drop_v18_roster_objects(connection)
        connection.execute(
            "ALTER TABLE account_roster_members RENAME TO account_roster_members_v18_old"
        )
        connection.execute(
            "ALTER TABLE account_roster_snapshots RENAME TO account_roster_snapshots_v18_old"
        )
        for statement in _statements():
            connection.execute(statement)
        for row in snapshots:
            connection.execute(
                """INSERT INTO account_roster_snapshots(
                       id,source_family,source_type,scope_key,scope_json,
                       source_instance_id,source_captured_at,accepted_at,
                       declared_count,member_count,members_sha256,source_sha256,
                       source_path,contract_version,metadata_json)
                   VALUES (?,'matrix',?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["id"],
                    row["source_type"],
                    row["scope_key"],
                    row["scope_json"],
                    row["source_instance_id"],
                    row["source_captured_at"],
                    row["accepted_at"],
                    row["declared_count"],
                    row["member_count"],
                    row["members_sha256"],
                    row["source_sha256"],
                    row["source_path"],
                    row["contract_version"],
                    row["metadata_json"],
                ),
            )
        for row in members:
            connection.execute(
                """INSERT INTO account_roster_members(
                       snapshot_id,account_identity_id,platform,member_key,uid,
                       matrix_account_id,profile_ref,monitoring_status,
                       authorization_status,monitoring_started_at,metadata_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    row["snapshot_id"],
                    row["account_identity_id"],
                    row["platform"],
                    f"matrix:{row['platform']}:{row['matrix_account_id']}",
                    row["uid"],
                    row["matrix_account_id"],
                    row["profile_ref"],
                    row["monitoring_status"],
                    row["authorization_status"],
                    row["monitoring_started_at"],
                    row["metadata_json"],
                ),
            )
        connection.execute("DROP TABLE account_roster_members_v18_old")
        connection.execute("DROP TABLE account_roster_snapshots_v18_old")
        activation_id = (
            _insert_activation(connection, plan["legacy_activation"])
            if plan["legacy_activation"] is not None
            else None
        )
        _import_runtime_receipts(connection, plan, activation_id=activation_id)
        _import_drain_events(connection, plan, activation_id=activation_id)
        storage._migration_checkpoint("v19_bridges_imported")
        if int(connection.execute("SELECT COUNT(*) FROM paid_provider_dispatch_events").fetchone()[0]):
            _fail("v19 migration cannot manufacture historical paid dispatch events")
        applied_at = storage.now_utc()
        connection.execute(
            "INSERT INTO schema_migrations(version,name,applied_at) VALUES (19,?,?)",
            (MIGRATION_NAME, applied_at),
        )
        connection.execute("PRAGMA user_version=19")
        validate_structure(connection)
        if connection.execute("PRAGMA foreign_key_check").fetchall():
            _fail("v19 migration has foreign-key violations")
        if str(connection.execute("PRAGMA quick_check").fetchone()[0]) != "ok":
            _fail("v19 migration failed SQLite quick_check")
        storage._migration_checkpoint("v19_before_commit")
        connection.commit()
        return plan
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.execute(f"PRAGMA legacy_alter_table={legacy_alter}")
        connection.execute("PRAGMA foreign_keys=ON")
