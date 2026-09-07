from __future__ import annotations

import unittest

from v8 import scan_terminals


class ScanTerminalPolicyTest(unittest.TestCase):
    def test_fixed_error_classes_and_raw_invalid_response_rule(self):
        self.assertEqual(
            scan_terminals.classify_error("http_503"), "provider_transient"
        )
        self.assertEqual(
            scan_terminals.classify_error("business_day_expired"), "deadline"
        )
        self.assertEqual(
            scan_terminals.classify_error("provider_auth_blocked"),
            "readiness_operator",
        )
        for reason in (
            "provider_circuit_open",
            "provider_blocked",
            "operation_blocked",
            "storage_hard",
            "authorization_hard",
            "paid_identity_hold",
            "incident_authorization_invalid",
            "compensation_authorization_invalid",
            "compensation_authorization_consumed",
            "compensation_gap_invalid",
            "operation_recovery_invalid",
            "storage_recovery_invalid",
        ):
            with self.subTest(reason=reason):
                self.assertEqual(
                    scan_terminals.classify_error(reason), "readiness_operator"
                )
        for reason in (
            "incident_total_budget_exhausted",
            "incident_bucket_budget_exhausted",
        ):
            with self.subTest(reason=reason):
                self.assertEqual(
                    scan_terminals.classify_error(reason), "budget_deferred"
                )
        self.assertEqual(
            scan_terminals.classify_error("manifest_integrity_error"), "integrity"
        )
        self.assertEqual(
            scan_terminals.classify_error("invalid_response", has_raw=True),
            "provider_transient",
        )
        self.assertEqual(
            scan_terminals.classify_error("invalid_response", has_raw=False),
            "integrity",
        )

    def test_legacy_provider_circuit_receipt_keeps_frozen_v1_class(self):
        legacy = {
            "terminal_contract_version": "scan-terminal-v1",
            "terminal_class": "budget_deferred",
            "accounted": True,
            "required": True,
            "publication_blocker": False,
            "reason": "provider_circuit_open",
            "blocker": "provider_circuit_open",
        }
        self.assertEqual(scan_terminals.validate_terminal_summary(legacy), legacy)
        current = scan_terminals.terminal_summary(
            reason="provider_circuit_open", terminal_class="readiness_operator"
        )
        self.assertEqual(scan_terminals.validate_terminal_summary(current), current)

    def test_operator_paused_is_accounted_but_not_required(self):
        summary = scan_terminals.terminal_summary(
            reason="operator_paused", terminal_class="not_applicable"
        )
        self.assertEqual(
            (
                summary["accounted"],
                summary["required"],
                summary["publication_blocker"],
                summary["blocker"],
            ),
            (True, False, False, None),
        )
        self.assertEqual(scan_terminals.validate_terminal_summary(summary), summary)

    def test_blocker_priority_never_lets_a_soft_deadline_mask_a_veto(self):
        ordered = [
            "deadline",
            "budget_deferred",
            "provider_transient",
            "readiness_operator",
            "integrity",
        ]
        self.assertEqual(
            sorted(ordered, key=scan_terminals.blocker_priority), ordered
        )
        with self.assertRaisesRegex(ValueError, "not a blocker"):
            scan_terminals.blocker_priority("success")

    def test_coverage_formula_requires_all_accounted_and_ninety_nine_percent_success(self):
        provider_gap = scan_terminals.coverage_decision(
            scope_total=100,
            succeeded=99,
            blocked=1,
            not_applicable=0,
            blocker_classes=frozenset({"provider_transient"}),
        )
        self.assertFalse(provider_gap["complete"])
        self.assertTrue(provider_gap["partial_publishable"])
        self.assertEqual(
            (provider_gap["accounted"], provider_gap["required"]), (100, 100)
        )

        for terminal_class in ("readiness_operator", "integrity"):
            with self.subTest(terminal_class=terminal_class):
                blocked = scan_terminals.coverage_decision(
                    scope_total=100,
                    succeeded=99,
                    blocked=1,
                    not_applicable=0,
                    blocker_classes=frozenset({terminal_class}),
                )
                self.assertFalse(blocked["partial_publishable"])

        below_threshold = scan_terminals.coverage_decision(
            scope_total=100,
            succeeded=98,
            blocked=2,
            not_applicable=0,
            blocker_classes=frozenset({"deadline"}),
        )
        self.assertFalse(below_threshold["partial_publishable"])

        paused = scan_terminals.coverage_decision(
            scope_total=100,
            succeeded=99,
            blocked=0,
            not_applicable=1,
        )
        self.assertTrue(paused["complete"])
        self.assertEqual((paused["required"], paused["accounted"]), (99, 100))


if __name__ == "__main__":
    unittest.main()
