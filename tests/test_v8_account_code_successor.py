"""Account-only source pins and portable ledger, without network or formal DB."""

from __future__ import annotations

import copy
import hashlib
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import seal_r0_receipts as sealer
from v8 import account_code_successor as account, capture_code_successor as code
from v8 import forward_recovery
from tests import test_v8_capture_code_successor_v2 as prior

ROOT = Path(__file__).resolve().parents[1]


def sha(value):
    return hashlib.sha256(value).hexdigest() if value is not None else None


class AccountSourceBoundaryTest(unittest.TestCase):
    def setUp(self):
        self.temp = self.enterContext(tempfile.TemporaryDirectory())
        self.root = (Path(self.temp) / "project").resolve()
        self.root.mkdir()
        self.git("init", "-q")
        self.api = self.root / "api.py"
        self.api.write_text("old source\n")
        self.git("add", ".")
        self.git(
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-qm",
            "fixture",
        )
        self.parent = self.archive("parent")
        self.api.write_text("reviewed source\n")
        target = self.root / account.MODULE
        target.parent.mkdir(parents=True)
        shutil.copy2(ROOT / account.MODULE, target)
        self.current = self.archive("current")
        self.enterContext(patch.object(forward_recovery, "PROJECT_ROOT", self.root))
        self.enterContext(patch.object(account, "PARENT_BUILD_SHA256", "a" * 64))
        self.enterContext(
            patch.object(
                account,
                "SOURCE_TRANSITIONS",
                {"api.py": (sha(b"old source\n"), sha(b"reviewed source\n"))},
            )
        )

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.root), *args], check=True, capture_output=True
        ).stdout

    def archive(self, name):
        git = sealer._git_record(self.root, allow_working_tree=True)
        path = Path(self.temp) / (name + ".tar")
        sealer._write_source_archive(self.root, path, git)
        return {"git": git, "source_archive": sealer._source_archive_record(path, git)}

    def test_actual_private_archives_match_only_the_exact_reviewed_bytes(self):
        self.assertEqual(
            account.verify_delta(self.root, self.parent, self.current),
            account.approved_changes(),
        )

    def test_unfrozen_or_wrong_pins_cannot_be_used_as_caller_approval(self):
        with (
            patch.object(account, "PARENT_BUILD_SHA256", ""),
            self.assertRaises(code.auth.AuthorizationError),
        ):
            account.verify_delta(self.root, self.parent, self.current)
        with (
            patch.object(account, "SOURCE_TRANSITIONS", {}),
            self.assertRaises(code.auth.AuthorizationError),
        ):
            account.verify_delta(self.root, self.parent, self.current)
        with (
            patch.object(
                account,
                "SOURCE_TRANSITIONS",
                {"api.py": (sha(b"other"), sha(b"reviewed source\n"))},
            ),
            self.assertRaises(code.auth.AuthorizationError),
        ):
            account.verify_delta(self.root, self.parent, self.current)

    def test_live_drift_extra_transport_and_missing_helper_are_rejected(self):
        original = self.api.read_bytes()
        self.api.write_bytes(original + b"# drift\n")
        with self.assertRaises((ValueError, RuntimeError)):
            account.verify_delta(self.root, self.parent, self.current)
        self.api.write_bytes(original)
        for mode in ("extra", "missing"):
            with self.subTest(mode=mode):
                if mode == "extra":
                    (self.root / "provider_transport.py").write_text(
                        "unreviewed transport\n"
                    )
                else:
                    (self.root / "provider_transport.py").unlink()
                    (self.root / account.MODULE).unlink()
                after = self.archive(mode)
                with self.assertRaises(code.auth.AuthorizationError):
                    account.verify_delta(self.root, self.parent, after)

    def test_changed_source_mode_is_rejected_even_with_matching_bytes(self):
        (self.root / account.MODULE).chmod(0o755)
        changed = self.archive("executable")
        with self.assertRaises(code.auth.AuthorizationError):
            account.verify_delta(self.root, self.parent, changed)

    def test_current_checks_keep_parent_results_and_require_all_new_checks(self):
        parent = {"results": {"backend": {"sha256": "old-log"}}}
        current = {
            "status": "passed",
            "git": self.current["git"],
            "results": {
                **parent["results"],
                **{key: {"sha256": key} for key in account.REQUIRED_CHECKS},
            },
        }
        account.verify_checks(current, parent, git=self.current["git"])
        for change in ("replace", "omit"):
            value = copy.deepcopy(current)
            if change == "replace":
                value["results"]["backend"]["sha256"] = "new-but-not-historical"
            else:
                value["results"].pop("account_successor")
            with self.assertRaises(code.auth.AuthorizationError):
                account.verify_checks(value, parent, git=self.current["git"])


class AccountPortableLedgerTest(unittest.TestCase):
    def setUp(self):
        self.fixture = prior.PortableSuccessorTests(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.connection = self.fixture.connection
        self.parent = self.fixture.third
        self.enterContext(
            patch(
                "v8.profile_activations.activation_at", return_value=self.fixture.active
            )
        )
        self.enterContext(
            patch.object(
                account, "PARENT_BUILD_SHA256", self.parent["build_reference"]["sha256"]
            )
        )
        self.enterContext(
            patch.object(
                account, "SOURCE_TRANSITIONS", {"api.py": ("a" * 64, "b" * 64)}
            )
        )
        previous = self.parent["plan_payload"]
        self.proof = copy.deepcopy(self.parent)
        plan = {
            key: previous[key]
            for key in (
                "previous_build",
                "source_deployment",
                "source_decision_sha256",
                "active",
                "release",
                "operations",
                "manifest",
            )
        }
        plan.update(
            contract=account.PLAN,
            transition=account.TRANSITION,
            project_root="/temporary/fixture",
            installed_parent=self.parent["build_reference"],
            origin_runtime_bindings=self.fixture.origin,
            required_checks=sorted(account.REQUIRED_CHECKS),
            changes=account.approved_changes(),
            git={"head": "fixture"},
            source_archive=prior.reference("account-source"),
            full_checks=prior.reference("account-checks"),
            actor="fixture",
            reason="fixture-only-account-source",
            issued_at="2026-09-07T01:00:00Z",
            business_e2e="deferred_by_user",
            transport_qualification="not_verified",
        )
        self.proof.update(
            contract=account.PROOF,
            roster_successor_contract="account-roster-code-plan-successor-v1",
            plan_payload=plan,
            plan_reference=prior.reference("account-plan"),
            installed_parent_proof=copy.deepcopy(self.parent),
            build_reference=prior.reference("account-build"),
            runtime_reference=prior.reference("account-runtime"),
        )
        self.proof["runtime_bindings"] = {
            "build_sha256": self.proof["build_reference"]["sha256"],
            "runtime_sha256": self.proof["runtime_reference"]["sha256"],
            "config_sha256": self.fixture.origin["config_sha256"],
        }
        self.fixture.seal(self.proof)

    def verify(self, proof=None):
        return code.validate_portable(
            self.connection,
            proof or self.proof,
            deployment=self.fixture.deployment,
            at="2026-09-07T02:00:00Z",
        )

    def test_real_ledger_accepts_account_after_v2_without_rewriting_authority(self):
        before = self.connection.total_changes
        self.assertEqual(self.verify(), self.proof)
        self.assertEqual(self.connection.total_changes, before)
        self.assertEqual(
            self.proof["origin_runtime_bindings"],
            self.parent["origin_runtime_bindings"],
        )
        self.assertNotEqual(
            self.proof["runtime_bindings"], self.parent["runtime_bindings"]
        )

    def test_wrong_parent_unapproved_source_or_claimed_qualification_are_rejected(self):
        for change in ("parent", "source", "qualification"):
            with self.subTest(change=change):
                proof = copy.deepcopy(self.proof)
                if change == "parent":
                    proof["plan_payload"]["installed_parent"]["sha256"] = "f" * 64
                elif change == "source":
                    proof["plan_payload"]["changes"]["provider_transport.py"] = {
                        "before_sha256": None,
                        "after_sha256": "f" * 64,
                    }
                else:
                    proof["plan_payload"]["transport_qualification"] = "passed"
                proof["proof_sha256"] = code.auth.digest(
                    {k: v for k, v in proof.items() if k != "proof_sha256"}
                )
                with self.assertRaises(code.auth.AuthorizationError):
                    self.verify(proof)

    def test_missing_decision_and_changed_control_fail_closed(self):
        with (
            patch.object(code, "_decision_row", return_value=None),
            self.assertRaises(code.auth.AuthorizationError),
        ):
            self.verify()
        with self.assertRaises(code.auth.AuthorizationError):
            account.control_precheck(
                self.proof["plan_payload"],
                self.fixture.active,
                {"id": 999, "event_hash": "f" * 64},
            )
        self.assertIsNone(account._RUNTIME.get())
