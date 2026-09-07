"""Real temporary prepare/seal/postseal/operator-gate account successor chain."""

from __future__ import annotations

import copy
import hashlib
import os
import shutil
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from tests import test_v8_capture_code_successor as original
from v8 import account_code_successor as account, capture_code_successor as code
from v8 import capture_release as release

ROOT = Path(__file__).resolve().parents[1]
SEALER = original.sealer
ACCOUNT_CRITICAL = tuple(SEALER.V20_ACCOUNT_CRITICAL_FILES)


def file_sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class AccountReleaseIntegrationTest(unittest.TestCase):
    def _logs(self, previous, root, names):
        result = dict(previous)
        for name in sorted(names):
            path = root / (name + "-parser-fixture.log")
            path.write_text(
                "isolated parser fixture only, not production evidence\n"
                f"DCAR_TEST_RESULT name={name} exit=0\n"
            )
            path.chmod(0o600)
            result[name] = path
        return result

    def test_real_runtime_v2_to_account_seal_decision_and_operator_gate(self):
        fixture = original.CaptureCodeSuccessorTest(methodName="runTest")
        self.addCleanup(fixture.doCleanups)
        with fixture.ready(
            operations=(
                "douyin_user_posts",
                "douyin_video_detail",
                "douyin_video_statistics",
            )
        ) as case:
            project, connection = case["project"], case["connection"]
            state = project.parent / "test-home/Library/Application Support/DcarAIGC"
            mirrors = state / "data/current-hold-control"
            mirrors.mkdir(parents=True, mode=0o700)
            first = fixture.seal(case)
            code.issue_decision(
                connection,
                project_root=project,
                build_path=first,
                mirror_root=mirrors,
                at=case["at"],
            )
            business = project / code.BUSINESS
            business.write_bytes(
                business.read_bytes()
                + b"\n# real temporary runtime-v2 source fixture\n"
            )
            runtime_logs = self._logs(case["logs"], first.parent.parent, code.V2_CHECKS)
            with patch.dict(os.environ, {"DCAR_LOADED_BUILD_RECEIPT": str(first)}):
                runtime_plan = code.prepare_plan(
                    connection,
                    project_root=project,
                    previous_build=first,
                    evidence_dir=first.parent.parent / "runtime-v2-fixture",
                    tests=runtime_logs,
                    actor="fixture",
                    reason="runtime-v2 parent fixture",
                    at=case["at"],
                )
                case.update(previous=first, plan=runtime_plan, logs=runtime_logs)
                runtime_build = fixture.seal(case)
                runtime_proof = code.issue_decision(
                    connection,
                    project_root=project,
                    build_path=runtime_build,
                    mirror_root=mirrors,
                    at=case["at"],
                )
            self.assertEqual(runtime_proof["plan_payload"]["contract"], code.PLAN_V2)
            original_rows = [
                tuple(r)
                for r in connection.execute(
                    "SELECT * FROM deployment_readiness_receipts ORDER BY id"
                )
            ]
            api = project / "src/dcar_eval/v8/api.py"
            before_api = file_sha(api)
            api.write_bytes(
                api.read_bytes()
                + b"\n# reviewed temporary account business source fixture\n"
            )
            fixed = {"src/dcar_eval/v8/api.py": (before_api, file_sha(api))}
            for relative in (
                account.MODULE,
                "src/dcar_eval/v8/capture_code_successor.py",
            ):
                target = project / relative
                before = file_sha(target) if target.exists() else None
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / relative, target)
                if relative != account.MODULE:
                    fixed[relative] = (before, file_sha(target))
            logs = self._logs(
                runtime_logs, runtime_build.parent.parent, account.REQUIRED_CHECKS
            )
            with (
                patch.object(account, "PARENT_BUILD_SHA256", file_sha(runtime_build)),
                patch.object(account, "HISTORICAL_MODULE_SHA256", hashlib.sha256(account._LOADED_SOURCE).hexdigest()),
                patch.object(
                    account,
                    "SOURCE_TRANSITIONS",
                    fixed,
                ),
                patch.object(SEALER, "V20_CRITICAL_FILES", ACCOUNT_CRITICAL),
                patch.dict(
                    os.environ,
                    {"DCAR_LOADED_BUILD_RECEIPT": str(runtime_build)},
                ),
            ):
                reference = code.prepare_plan(
                    connection,
                    project_root=project,
                    previous_build=runtime_build,
                    evidence_dir=runtime_build.parent.parent / "account-fixture",
                    tests=logs,
                    actor="fixture",
                    reason="reviewed account transition fixture",
                    at=case["at"],
                    transition=account.TRANSITION,
                )
                case.update(previous=runtime_build, plan=reference, logs=logs)
                account_build = fixture.seal(case)
                with self.assertRaises(code.auth.AuthorizationError):
                    code.current_proof(
                        connection,
                        project_root=project,
                        build_path=account_build,
                        at=case["at"],
                    )
                proof = code.issue_decision(
                    connection,
                    project_root=project,
                    build_path=account_build,
                    mirror_root=mirrors,
                    at=case["at"],
                )
                self.assertEqual(proof["contract"], account.PROOF)
                self.assertEqual(
                    proof["roster_successor_contract"],
                    "account-roster-code-plan-successor-v1",
                )
                self.assertEqual(
                    proof["origin_runtime_bindings"],
                    runtime_proof["origin_runtime_bindings"],
                )
                self.assertEqual(
                    proof["installed_parent_proof"]["build_reference"]["sha256"],
                    file_sha(runtime_build),
                )
                case["evidence"].update(proof["runtime_bindings"])
                case["evidence"]["code_successor"] = proof
                for operation in proof["plan_payload"]["operations"]:
                    gate = release.publish_operation_gate(
                        connection, operation=operation, at=case["at"]
                    )
                    self.assertTrue(gate["ordinary_paid_authorized"])
                    self.assertEqual(gate["provider_calls"], 0)
                    bindings = release.current_runtime_bindings(
                        connection, operation, case["at"]
                    )
                    self.assertEqual(
                        bindings["build_receipt_sha256"], file_sha(account_build)
                    )
                self.assertEqual(
                    code.validate_portable(
                        connection, proof, deployment=case["accepted"], at=case["at"]
                    ),
                    proof,
                )
                self.assertEqual(
                    original_rows,
                    [
                        tuple(r)
                        for r in connection.execute(
                            "SELECT * FROM deployment_readiness_receipts ORDER BY id"
                        )
                    ],
                )
                self.assertEqual(
                    case["decision_path"].read_bytes(), case["original_decision"]
                )
                for name in (
                    "provider_usage",
                    "provider_request_start_events",
                    "fetch_attempts",
                ):
                    self.assertEqual(
                        connection.execute("SELECT count(*) FROM " + name).fetchone()[
                            0
                        ],
                        0,
                    )
                # The real online snapshot builder must consume this exact new
                # proof without reinterpreting its historical full-check source.
                import build_server_snapshot as builder
                from v8 import storage

                for name in (".comment_hash_salt", ".platform_user_salt"):
                    salt = project / "data/cache" / name
                    salt.parent.mkdir(parents=True, exist_ok=True)
                    salt.write_bytes(b"temporary-fixture-salt-not-production")
                    salt.chmod(0o600)
                connection.commit()
                with (
                    patch.object(
                        code, "_installed_build_path", return_value=account_build
                    ),
                    patch.object(
                        builder,
                        "_utc_now",
                        return_value=case["at"],
                    ),
                    patch.object(storage, "now_utc", return_value=case["at"]),
                    patch.object(
                        builder,
                        "FORMAL_STATE_ROOT",
                        state,
                    ),
                ):
                    manifest = builder.build_snapshot(
                        project_root=project,
                        database=case["database"],
                        output=account_build.parent / "snapshot",
                        expected_user_version=20,
                        require_accepted_deployment=True,
                    )
                self.assertEqual(manifest["code_successor"], proof)
                case["test_now"] = case["at"]
                with (
                    patch.object(
                        code, "_installed_build_path", return_value=account_build
                    ),
                    patch.object(
                        storage,
                        "now_utc",
                        side_effect=lambda: case["test_now"],
                    ),
                ):
                    self._admit_next_day(case, account_build, proof)
                # An authentic current envelope cannot mask changed live source.
                api.write_bytes(api.read_bytes() + b"\n# unapproved drift\n")
                with self.assertRaises((ValueError, RuntimeError)):
                    code.current_proof(
                        connection,
                        project_root=project,
                        build_path=account_build,
                        at=case["at"],
                    )
                changed = copy.deepcopy(proof)
                changed["plan_payload"]["installed_parent"]["sha256"] = "f" * 64
                changed["proof_sha256"] = code.auth.digest(
                    {k: v for k, v in changed.items() if k != "proof_sha256"}
                )
                with self.assertRaises(code.auth.AuthorizationError):
                    code.validate_portable(
                        connection, changed, deployment=case["accepted"], at=case["at"]
                    )

    def _admit_next_day(self, case, build, original_proof):
        from v8 import (
            account_roster_capture as roster,
            account_states,
            operations,
            system_roster,
        )
        from v8.capture_activation_release import (
            validate_installed_activation_successor,
        )
        from v8.profile_activations import activation_at
        from v8.source_routing import parse_time
        from v8 import provider_budget

        connection, project, at = case["connection"], case["project"], case["at"]
        connection.execute("BEGIN IMMEDIATE")
        current = activation_at(connection, at)
        uid = "987654321098765432"
        created = operations.create_account_in_transaction(
            connection,
            {"platform": "douyin", "uid": uid, "nickname": "fixture new account"},
        )
        identity = connection.execute(
            "SELECT id FROM account_platform_identities WHERE account_id=?",
            (created["id"],),
        ).fetchone()[0]
        account_states.set_account_enabled_in_transaction(
            connection,
            identity,
            enabled=True,
            effective_at=at,
            created_at=at,
            actor="fixture",
            reason="admission",
            activation_id=current["activation_id"],
        )
        sec = "MS4wLjABAAAA_fixture_new_account_123456789"
        snapshot = system_roster.upsert_system_members(
            connection,
            [
                {
                    "platform": "douyin",
                    "uid": uid,
                    "nickname": "fixture new account",
                    "sec_user_id": sec,
                    "profile_ref": "https://www.douyin.com/user/" + sec,
                }
            ],
            raw_root=project / "runtime/account-test-raw",
            actor="fixture",
            reason="admission",
            sealed_at=at,
        )
        scheduled = roster.schedule_account_roster_capture_in_transaction(
            connection,
            roster_snapshot_id=snapshot["snapshot_id"],
            account_id=created["id"],
            actor="fixture",
            reason="admission",
            now=at,
        )
        next_at = scheduled["activation"]["effective_at"]
        case["test_now"] = next_at
        current = activation_at(connection, next_at)
        case["evidence"]["active"] = current
        case["evidence"]["activation_successor"] = (
            validate_installed_activation_successor(
                connection,
                source_deployment=case["accepted"],
                current_active=current,
                runtime_bindings=original_proof["origin_runtime_bindings"],
                manifest=case["evidence"]["manifest"],
                at=next_at,
            )
        )
        proof = code.current_proof(
            connection, project_root=project, build_path=build, at=next_at
        )
        self.assertEqual(
            proof["plan_payload"]["active"], original_proof["plan_payload"]["active"]
        )
        self.assertNotEqual(proof["active"], original_proof["active"])
        self.assertEqual(proof["active"]["activation_id"], current["activation_id"])
        self.assertEqual(
            proof["origin_runtime_bindings"], original_proof["origin_runtime_bindings"]
        )
        self.assertEqual(proof["runtime_bindings"], original_proof["runtime_bindings"])
        self.assertEqual(
            proof["roster_successor"]["contract"],
            "account-roster-code-plan-successor-v1",
        )
        case["evidence"]["code_successor"] = proof
        issued = roster.activate_prepared_roster_capture_in_transaction(
            connection, at=next_at
        )
        self.assertEqual(issued["status"], "issued")
        self.assertEqual(issued["provider_calls"], 0)
        for operation in proof["plan_payload"]["operations"]:
            bindings = release.current_runtime_bindings(connection, operation, next_at)
            self.assertEqual(
                bindings["build_receipt_sha256"],
                proof["runtime_bindings"]["build_sha256"],
            )
            self.assertEqual(bindings["activation_id"], current["activation_id"])
        self.assertEqual(
            code.validate_portable(
                connection, proof, deployment=case["accepted"], at=next_at
            ),
            proof,
        )
        # Runtime succession never makes later plan issuance or expired paid
        # readiness valid; original parent plans remain fixed to their control.
        with self.assertRaises(code.auth.AuthorizationError):
            with account.using_plan(
                connection, proof["plan_reference"], project_root=project, at=next_at
            ):
                pass
        with self.assertRaises(code.auth.AuthorizationError):
            code.current_proof(
                connection,
                project_root=project,
                build_path=build,
                at=next_at,
                require_decision=False,
            )
        self.assertIsNone(account._RUNTIME.get())
        changed = copy.deepcopy(proof)
        changed["roster_successor"]["target_release"]["event_hash"] = "f" * 64
        changed["proof_sha256"] = code.auth.digest(
            {key: value for key, value in changed.items() if key != "proof_sha256"}
        )
        with self.assertRaises(code.auth.AuthorizationError):
            code.validate_portable(
                connection, changed, deployment=case["accepted"], at=next_at
            )

        def authorize(timestamp):
            bindings = release.current_runtime_bindings(
                connection, "douyin_user_posts", timestamp
            )
            with code.auth.runtime_authority(release.current_runtime_bindings):
                return code.auth.validate_authorization(
                    connection,
                    runtime_bindings=bindings,
                    operation="douyin_user_posts",
                    request_identity=code.auth.digest(["fixture", "unbought"]),
                    at=timestamp,
                    amount_microusd=provider_budget.PRICES_MICROUSD[
                        "douyin_user_posts"
                    ],
                )

        authorize(next_at)
        expired = (parse_time(next_at) + timedelta(hours=25)).isoformat()
        case["test_now"] = expired
        with self.assertRaisesRegex(code.auth.AuthorizationError, "expired"):
            authorize(expired)
        case["test_now"] = next_at
        for table in (
            "provider_usage",
            "provider_request_start_events",
            "fetch_attempts",
        ):
            self.assertEqual(
                connection.execute("SELECT count(*) FROM " + table).fetchone()[0], 0
            )
