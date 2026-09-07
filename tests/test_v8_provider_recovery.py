from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from v8.capture import CaptureError, ProviderResult
from v8.paid_drain import issue_activation_permit_in_transaction
from v8.profile_activations import MATRIX_PROFILE, TIKHUB_PROFILE, append_activation
from v8.provider_budget import PROBE_JOB, circuit_state, record_circuit
from v8.provider_recovery import (
    CONFIRMATION,
    ProviderRecoveryError,
    _validate_formal_key_binding,
    main,
)
from v8.runtime_database import InstalledWriterContract
from v8.storage import connect, initialize_database, transaction


NOW = "2026-09-05T04:00:00.000000Z"
BUILD = "b" * 64
RUNTIME = "c" * 64


class ProviderRecoveryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "isolated.sqlite3"
        self.raw = self.root / "raw"
        self.calls = 0
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO accounts(
                       id,phone,phone_normalized,operator_name,account_type,
                       content_direction,enabled,created_at,updated_at)
                   VALUES (1,'',NULL,'','unknown','unknown',1,?,?)""",
                (NOW, NOW),
            )
            connection.execute(
                """INSERT INTO account_platform_identities(
                       id,account_id,platform,uid,nickname,source,created_at,updated_at)
                   VALUES (1,1,'douyin','10000001','managed','manual',?,?)""",
                (NOW, NOW),
            )
            connection.execute(
                """INSERT INTO content_items(
                       id,link_id,platform,platform_content_id,canonical_url,
                       account_id,raw_account_uid,published_at,imported_at,
                       created_at,updated_at)
                   VALUES (1,'C00001','douyin','7000000001',
                           'https://www.douyin.com/video/7000000001',1,'10000001',
                           '2026-09-03T05:00:00Z',?,?,?)""",
                (NOW, NOW, NOW),
            )
            connection.commit()
        self._activate(TIKHUB_PROFILE, "system")
        with connect(self.db) as connection, transaction(connection):
            record_circuit(
                connection,
                reason="provider_balance_blocked",
                usage_id=None,
                at=NOW,
            )
        self.db.chmod(0o600)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _activate(self, profile: str, family: str) -> None:
        source = json.dumps({"family": family}).encode()
        source_path = self.root / f"{family}-roster.json"
        source_path.write_bytes(source)
        source_path.chmod(0o600)
        members_hash = hashlib.sha256(f"{family}-members".encode()).hexdigest()
        with connect(self.db) as connection, transaction(connection):
            cursor = connection.execute(
                """INSERT INTO account_roster_snapshots(
                       source_family,source_type,scope_key,scope_json,
                       source_instance_id,source_captured_at,accepted_at,
                       declared_count,member_count,members_sha256,source_sha256,
                       source_path,contract_version,metadata_json)
                   VALUES (?,?,?,'{}',?,?,?,1,1,?,?,?,?,'{}')""",
                (
                    family,
                    "system_managed" if family == "system" else "manual_export",
                    f"isolated-{family}",
                    f"{family}-1",
                    NOW,
                    NOW,
                    members_hash,
                    hashlib.sha256(source).hexdigest(),
                    str(source_path),
                    "system-managed-roster-v1"
                    if family == "system"
                    else "matrix-full-roster-v1",
                ),
            )
            snapshot_id = int(cursor.lastrowid or 0)
            connection.execute(
                """INSERT INTO account_roster_members(
                       snapshot_id,account_identity_id,platform,member_key,uid,
                       matrix_account_id,profile_ref,monitoring_status,
                       authorization_status,metadata_json)
                   VALUES (?,?,?,?,?,?,?,'unknown','unknown','{}')""",
                (
                    snapshot_id,
                    1,
                    "douyin",
                    "uid:douyin:10000001"
                    if family == "system"
                    else "matrix:douyin:matrix-1",
                    "10000001",
                    None if family == "system" else "matrix-1",
                    None
                    if family == "system"
                    else "https://www.douyin.com/user/MS4w-1",
                ),
            )
            activation = append_activation(
                connection,
                profile_id=profile,
                roster_snapshot_id=snapshot_id,
                roster_members_sha256=members_hash,
                effective_at="2026-09-05T03:00:00Z",
                build_receipt_sha256=BUILD,
                actor="test",
                reason="isolated recovery fixture",
                created_at="2026-09-05T03:00:00Z",
            )
            issue_activation_permit_in_transaction(
                connection,
                activation_id=int(activation["activation_id"]),
                drain_id=f"fixture:{activation['activation_id']}",
                source_activation_id=int(activation["activation_id"]),
                business_day="2026-09-05",
                planned_effective_at="2026-09-05T03:00:00Z",
                build_receipt_sha256=BUILD,
                runtime_root_receipt_sha256=RUNTIME,
                now="2026-09-05T03:00:00Z",
            )

    def _args(self, *extra: str, at: str = NOW) -> list[str]:
        return [
            "--db",
            str(self.db),
            "--content-id",
            "1",
            "--isolated",
            "--at",
            at,
            *extra,
        ]

    def _run(
        self,
        *extra: str,
        at: str = NOW,
        call_override=None,
    ) -> tuple[int, dict]:
        output = io.StringIO()
        with ExitStack() as stack:
            stack.enter_context(redirect_stdout(output))
            stack.enter_context(patch("v8.capture.RAW_ROOT", self.raw))
            stack.enter_context(patch("v8.capture.now_utc", return_value=at))
            stack.enter_context(patch("v8.providers.now_utc", return_value=at))
            stack.enter_context(patch("v8.provider_budget.now_utc", return_value=at))
            stack.enter_context(patch("v8.storage.now_utc", return_value=at))
            stack.enter_context(patch("v8.metric_observations.now_utc", return_value=at))
            stack.enter_context(patch("v8.provider_recovery.now_utc", return_value=at))
            code = main(self._args(*extra, at=at), call_override=call_override)
        return code, json.loads(output.getvalue())

    def _preview(self) -> dict:
        code, result = self._run()
        self.assertEqual(code, 0, result)
        self.assertEqual(result["status"], "preview")
        return result

    def _success(self, stage, content):
        self.calls += 1
        self.assertEqual(stage, "metrics")
        self.assertEqual(content["id"], 1)
        return ProviderResult(
            {
                "view_count": 123,
                "comment_count": 4,
                "like_count": 5,
                "share_count": 6,
                "collect_count": 7,
            },
            {"code": 0, "data": {"aweme_id": "7000000001"}},
            200,
            True,
        )

    def test_default_is_preview_and_has_no_paid_side_effect(self) -> None:
        result = self._preview()
        self.assertEqual(result["operation"], "douyin_video_statistics")
        self.assertEqual(result["price_usd"], 0.001)
        self.assertEqual(result["target"]["content_id"], 1)
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM scheduler_runs WHERE job_id=?", (PROBE_JOB,)
                ).fetchone()[0],
                0,
            )

    def test_apply_requires_exact_confirmation_and_fresh_fingerprint(self) -> None:
        preview = self._preview()
        code, result = self._run(
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            call_override=self._success,
        )
        self.assertEqual((code, result["error_code"]), (2, "recovery_confirmation_required"))
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE content_items SET updated_at='2026-09-05T04:01:00Z' WHERE id=1"
            )
        code, result = self._run(
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            "--confirm",
            CONFIRMATION,
            call_override=self._success,
        )
        self.assertEqual((code, result["error_code"]), (2, "recovery_preview_stale"))
        self.assertEqual(self.calls, 0)

    def test_expired_preview_is_rejected_before_authorization(self) -> None:
        preview = self._preview()
        later = (
            datetime.fromisoformat(NOW.replace("Z", "+00:00"))
            + timedelta(minutes=6)
        ).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        code, result = self._run(
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            "--confirm",
            CONFIRMATION,
            at=later,
            call_override=self._success,
        )
        self.assertEqual((code, result["error_code"]), (2, "recovery_preview_expired"))

    def test_isolated_apply_without_override_never_reaches_network(self) -> None:
        preview = self._preview()
        with patch("v8.providers._douyin_call") as network:
            code, result = self._run(
                "--apply",
                "--preview-fingerprint",
                preview["preview_fingerprint"],
                "--confirm",
                CONFIRMATION,
            )
        self.assertEqual(
            (code, result["error_code"]),
            (2, "recovery_isolated_live_call_forbidden"),
        )
        network.assert_not_called()
        with connect(self.db) as connection:
            self.assertEqual(
                connection.execute("SELECT COUNT(*) FROM provider_usage").fetchone()[0],
                0,
            )

    def test_formal_key_must_match_the_installed_writer_binding(self) -> None:
        writer_env = self.root / "writer.env"
        key_file = self.root / "tikhub.env"
        other_key = self.root / "other-tikhub.env"
        key_file.write_text("TIKHUB_API_KEY=test\n", encoding="utf-8")
        other_key.write_text("TIKHUB_API_KEY=other\n", encoding="utf-8")
        key_file.chmod(0o600)
        other_key.chmod(0o600)
        writer_env.write_text(
            f"TIKHUB_API_KEY_FILE={key_file}\n"
            "DCAR_DAILY_COST_AUTHORIZATION=I_ACKNOWLEDGE_DAILY_PROVIDER_LIMIT_USD_100\n",
            encoding="utf-8",
        )
        writer_env.chmod(0o600)
        installed = InstalledWriterContract(
            home=self.root,
            plist_path=self.root / "writer.plist",
            project_root=self.root / "project",
            program=self.root / "project/run_writer_worker.sh",
            database=self.db,
            writer_lock=self.root / "writer.lock",
            payload={
                "EnvironmentVariables": {
                    "DCAR_WRITER_ENV_FILE": str(writer_env)
                }
            },
        )
        self.assertEqual(
            _validate_formal_key_binding(
                installed, environ={"TIKHUB_API_KEY_FILE": str(key_file)}
            ),
            key_file.resolve(),
        )
        with self.assertRaises(ProviderRecoveryError) as caught:
            _validate_formal_key_binding(
                installed, environ={"TIKHUB_API_KEY_FILE": str(other_key)}
            )
        self.assertEqual(
            caught.exception.code, "recovery_credential_identity_unresolved"
        )

    def test_mode_b_and_recent_content_are_mandatory(self) -> None:
        with connect(self.db) as connection, transaction(connection):
            connection.execute(
                "UPDATE content_items SET published_at='2026-08-20T00:00:00Z' WHERE id=1"
            )
        code, result = self._run()
        self.assertEqual((code, result["error_code"]), (2, "recovery_target_missing"))

        other = self.root / "matrix.sqlite3"
        with connect(other) as connection:
            initialize_database(connection)
            connection.execute(
                """INSERT INTO accounts(
                       id,phone,phone_normalized,operator_name,account_type,
                       content_direction,enabled,created_at,updated_at)
                   VALUES (1,'',NULL,'','unknown','unknown',1,?,?)""",
                (NOW, NOW),
            )
            connection.execute(
                """INSERT INTO account_platform_identities(
                       id,account_id,platform,uid,nickname,source,created_at,updated_at)
                   VALUES (1,1,'douyin','10000001','managed','manual',?,?)""",
                (NOW, NOW),
            )
            connection.commit()
        original = self.db
        self.db = other
        try:
            self._activate(MATRIX_PROFILE, "matrix")
            with connect(self.db) as connection, transaction(connection):
                record_circuit(
                    connection,
                    reason="provider_balance_blocked",
                    usage_id=None,
                    at=NOW,
                )
            self.db.chmod(0o600)
            code, result = self._run()
            self.assertEqual((code, result["error_code"]), (2, "recovery_profile_invalid"))
        finally:
            self.db = original

    def test_success_is_one_shot_and_closes_the_circuit(self) -> None:
        preview = self._preview()
        apply_args = (
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            "--confirm",
            CONFIRMATION,
        )
        code, result = self._run(*apply_args, call_override=self._success)
        self.assertEqual(code, 0, result)
        self.assertEqual(result["status"], "succeeded")
        self.assertFalse(result["circuit_open"])
        self.assertEqual(result["probe"]["state"], "succeeded")
        self.assertEqual(result["usage"]["request_attempts"], 1)
        self.assertEqual(self.calls, 1)
        with connect(self.db) as connection:
            self.assertFalse(circuit_state(connection)["open"])
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM content_metric_observations WHERE content_id=1"
                ).fetchone()[0],
                1,
            )
        code, replay = self._run(*apply_args, call_override=self._success)
        self.assertEqual(code, 2, replay)
        self.assertEqual(self.calls, 1)

    def test_failed_probe_stays_open_and_is_not_retried(self) -> None:
        preview = self._preview()

        def fail(_stage, _content):
            self.calls += 1
            raise CaptureError(
                "timeout",
                retryable=True,
                error_code="transport_error",
                billed=None,
            )

        apply_args = (
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            "--confirm",
            CONFIRMATION,
        )
        code, result = self._run(*apply_args, call_override=fail)
        self.assertEqual(code, 2, result)
        self.assertEqual(result["status"], "failed")
        self.assertTrue(result["circuit_open"])
        self.assertEqual(result["probe"]["state"], "failed")
        self.assertFalse(result["automatic_retry"])
        self.assertEqual(self.calls, 1)
        code, replay = self._run(*apply_args, call_override=fail)
        self.assertEqual(code, 2, replay)
        self.assertEqual(self.calls, 1)

    def test_materialization_failure_does_not_close_circuit(self) -> None:
        preview = self._preview()
        apply_args = (
            "--apply",
            "--preview-fingerprint",
            preview["preview_fingerprint"],
            "--confirm",
            CONFIRMATION,
        )
        with patch(
            "v8.providers._store_stage_result",
            side_effect=RuntimeError("injected materialization failure"),
        ):
            code, result = self._run(*apply_args, call_override=self._success)
        self.assertEqual(code, 2, result)
        self.assertEqual(result["status"], "failed")
        self.assertFalse(result["materialization_verified"])
        self.assertTrue(result["circuit_open"])
        self.assertEqual(result["probe"]["state"], "failed")
        self.assertEqual(result["usage"]["details"]["state"], "completed")
        self.assertEqual(self.calls, 1)
        with connect(self.db) as connection:
            raw = connection.execute(
                "SELECT source FROM provider_raw_responses ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertEqual(raw["source"], "live")
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM content_metric_observations WHERE content_id=1"
                ).fetchone()[0],
                0,
            )
        code, replay = self._run(*apply_args, call_override=self._success)
        self.assertEqual(code, 2, replay)
        self.assertEqual(self.calls, 1)


if __name__ == "__main__":
    unittest.main()
