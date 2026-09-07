from __future__ import annotations

import unittest

from v8.paid_identity import (
    PaidIdentityError,
    PaidRequestIdentity,
    build_paid_request_identity,
    validate_paid_request_identity,
)


class PaidIdentityTest(unittest.TestCase):
    def identity(self, **overrides):
        values = {
            "provider": "TikHub",
            "operation": "douyin_user_posts",
            "platform": "douyin",
            "subject": "MS4wLjAB-account",
            "request_parameters": {"count": 20, "max_cursor": 7, "sort_type": 0},
            "cursor": 7,
            "due_bucket": "2026-09-05T04:30:00Z",
            "request_window": {
                "start": "2026-09-02T12:34:56.789+00:00",
                "end": "2026-09-05T12:34:56+00:00",
            },
            "sequence": 0,
        }
        values.update(overrides)
        return build_paid_request_identity(**values)

    def test_mapping_order_and_timezone_spelling_do_not_change_identity(self):
        first = self.identity()
        second = self.identity(
            provider="tikhub",
            request_parameters={"sort_type": 0, "max_cursor": 7, "count": 20},
            request_window={
                "end": "2026-09-05T20:34:56+08:00",
                "start": "2026-09-02T20:34:56+08:00",
            },
        )
        self.assertEqual(first.scope_identity, second.scope_identity)
        self.assertEqual(
            first.document["request_window"]["start"], "2026-09-02T12:34:56Z"
        )

    def test_route_stack_build_worker_and_retry_time_are_not_inputs(self):
        first = self.identity()
        second = self.identity()
        self.assertEqual(first, second)
        self.assertNotIn("route", first.document)
        self.assertNotIn("worker", first.document)

    def test_each_provider_request_dimension_changes_scope(self):
        baseline = self.identity().scope_identity
        variations = (
            {"operation": "douyin_video_detail"},
            {"platform": "xiaohongshu"},
            {"subject": "another-account"},
            {"request_parameters": {"count": 10, "max_cursor": 7, "sort_type": 0}},
            {"cursor": 8},
            {"due_bucket": "2026-09-05T05:00:00Z"},
            {
                "request_window": {
                    "start": "2026-09-03T00:00:00Z",
                    "end": "2026-09-05T12:34:56Z",
                }
            },
        )
        self.assertTrue(
            all(
                self.identity(**change).scope_identity != baseline
                for change in variations
            )
        )

    def test_compensation_sequence_changes_execution_not_request_identity(self):
        normal = self.identity(sequence=0)
        compensation = self.identity(sequence=1)
        self.assertEqual(normal.scope_identity, compensation.scope_identity)
        self.assertNotEqual(normal.execution_identity, compensation.execution_identity)

    def test_rejects_secret_values_by_key_and_invalid_sequences(self):
        with self.assertRaises(PaidIdentityError):
            self.identity(request_parameters={"Authorization": "Bearer secret"})
        for sequence in (-1, 5, True):
            with self.subTest(sequence=sequence), self.assertRaises(PaidIdentityError):
                self.identity(sequence=sequence)

    def test_rejects_naive_or_inverted_windows(self):
        with self.assertRaises(PaidIdentityError):
            self.identity(
                request_window={
                    "start": "2026-09-05T00:00:00",
                    "end": "2026-09-06T00:00:00Z",
                }
            )
        with self.assertRaises(PaidIdentityError):
            self.identity(
                request_window={
                    "start": "2026-09-06T00:00:00Z",
                    "end": "2026-09-05T00:00:00Z",
                }
            )

    def test_claim_boundary_rebuilds_hashes_and_binds_frozen_target(self):
        identity = self.identity()
        validated = validate_paid_request_identity(
            identity,
            provider="TikHub",
            operation="douyin_user_posts",
            platform="douyin",
            subject="MS4wLjAB-account",
            due_bucket="2026-09-05T04:30:00Z",
        )
        self.assertEqual(validated, identity)

        identity.document["subject"] = "mutated-account"
        with self.assertRaisesRegex(PaidIdentityError, "hashes"):
            validate_paid_request_identity(
                identity,
                provider="TikHub",
                operation="douyin_user_posts",
                platform="douyin",
                subject="MS4wLjAB-account",
                due_bucket="2026-09-05T04:30:00Z",
            )

        forged = PaidRequestIdentity(
            scope_identity="0" * 64,
            execution_identity="1" * 64,
            sequence=0,
            document=dict(self.identity().document),
        )
        with self.assertRaisesRegex(PaidIdentityError, "hashes"):
            validate_paid_request_identity(
                forged,
                provider="TikHub",
                operation="douyin_user_posts",
                platform="douyin",
                subject="MS4wLjAB-account",
                due_bucket="2026-09-05T04:30:00Z",
            )

    def test_claim_boundary_rejects_valid_identity_for_another_target(self):
        with self.assertRaisesRegex(PaidIdentityError, "frozen request target"):
            validate_paid_request_identity(
                self.identity(),
                provider="TikHub",
                operation="douyin_user_posts",
                platform="douyin",
                subject="another-account",
                due_bucket="2026-09-05T04:30:00Z",
            )


if __name__ == "__main__":
    unittest.main()
