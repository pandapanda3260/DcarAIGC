from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import v8.audience_rate as ar
import v8.storage as storage
from v8.identity import PlatformUserHasher, comment_identity_key, insert_comment_rows


class StatusMachineTest(unittest.TestCase):
    def _decision(self, **kw):
        base = dict(
            publication_count=10,
            candidate_users=200,
            classified_users=200,
            total_users=200,
            identity_coverage=97.0,
            classification_coverage=100.0,
            classification_complete=True,
            comment_coverage=95.0,
            capped_content_count=0,
            classifier_state="approved",
        )
        base.update(kw)
        return ar._decide_metric(**base)

    def _status(self, **kw):
        return self._decision(**kw).status

    def test_no_content_is_not_applicable(self) -> None:
        self.assertEqual(self._status(publication_count=0), "not_applicable")

    def test_content_but_no_users_is_missing(self) -> None:
        self.assertEqual(
            self._status(candidate_users=0, total_users=0), "missing"
        )

    def test_identity_coverage_outranks_headcount(self) -> None:
        # 500 users but 94% identity coverage -> below_threshold
        self.assertEqual(
            self._status(total_users=500, identity_coverage=94.0),
            "below_threshold",
        )

    def test_small_universe_is_below_threshold(self) -> None:
        self.assertEqual(self._status(total_users=29), "below_threshold")

    def test_rejected_classifier_is_below_threshold(self) -> None:
        self.assertEqual(
            self._status(classifier_state="rejected"), "below_threshold"
        )

    def test_incomplete_classification_never_publishes_zero(self) -> None:
        self.assertEqual(
            self._status(
                classification_coverage=99.99, classification_complete=False
            ),
            "below_threshold",
        )
        self.assertEqual(
            self._status(
                classifier_state="uncalibrated",
                classification_coverage=0.0,
                classification_complete=False,
            ),
            "below_threshold",
        )

    def test_rounded_coverage_cannot_bypass_integer_completeness(self) -> None:
        rounded = round(20_000 * 100 / 20_001, 2)
        self.assertEqual(rounded, 100.0)
        decision = self._decision(
            total_users=20_001,
            candidate_users=20_001,
            classified_users=20_000,
            classification_coverage=rounded,
            classification_complete=False,
        )
        self.assertEqual(decision.status, "below_threshold")
        self.assertIn("用户分类覆盖率", decision.reason)
        self.assertNotIn("定标未通过", decision.reason)

    def test_uncalibrated_publishes_capped_at_sample_only(self) -> None:
        # 2026-08-07 owner decision: never-calibrated classifier publishes,
        # but can never reach "available" and other gates still apply.
        self.assertEqual(
            self._status(classifier_state="uncalibrated"), "sample_only"
        )
        self.assertEqual(
            self._status(classifier_state="uncalibrated", identity_coverage=94.0),
            "below_threshold",
        )
        self.assertEqual(
            self._status(classifier_state="uncalibrated", total_users=29),
            "below_threshold",
        )
        self.assertEqual(
            self._status(classifier_state="uncalibrated", total_users=0), "missing"
        )

    def test_sample_band_and_conservative_and_coverage(self) -> None:
        self.assertEqual(self._status(total_users=35), "sample_only")
        self.assertEqual(
            self._status(classifier_state="conservative"), "sample_only"
        )
        self.assertEqual(self._status(comment_coverage=89.99), "sample_only")
        self.assertEqual(self._status(comment_coverage=None), "sample_only")

    def test_full_gate_is_available(self) -> None:
        self.assertEqual(self._status(), "available")

    def test_capped_comments_can_only_publish_as_sample(self) -> None:
        decision = self._decision(capped_content_count=1)
        self.assertEqual(decision.status, "sample_only")
        self.assertIn("评论采集上限", decision.reason)
        self.assertNotIn("保守识别", decision.reason)

    def test_approximate_identity_keys_can_only_publish_as_sample(self) -> None:
        decision = self._decision(approximate_identity_keys=True)
        self.assertEqual(decision.status, "sample_only")
        self.assertIn("历史内容级近似身份键", decision.reason)

    def test_boundary_examples_from_plan(self) -> None:
        # 92% comment coverage, 96% identity, 100 users -> available
        self.assertEqual(
            self._status(total_users=100, identity_coverage=96.0, comment_coverage=92.0),
            "available",
        )
        # 94% identity, 500 users -> below_threshold
        self.assertEqual(
            self._status(total_users=500, identity_coverage=94.0),
            "below_threshold",
        )
        # 96% identity, 35 users -> sample_only
        self.assertEqual(
            self._status(total_users=35, identity_coverage=96.0, comment_coverage=95.0),
            "sample_only",
        )


class SliceRateTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "rate.sqlite3"
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        self.hasher = PlatformUserHasher(salt_path=Path(self.temp.name) / ".salt")
        self._seed_content()

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def _seed_content(self) -> None:
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id, link_id, platform, canonical_url,
                    imported_at, created_at, updated_at
                ) VALUES (1,'AAAAAA','douyin','https://www.douyin.com/video/1',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
            self.connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    id, content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (1,1,'2026-08-01T01:00:00Z','2026-W31','douyin',
                          'data/cache/c1.json', ?, 4, 'available',
                          '2026-08-01T01:00:00Z')
                """,
                ("1" * 64,),
            )

    def _add_user(self, uid: str, body: str, *, stable: bool = True, label: str | None = None) -> None:
        pseudonymous = self.hasher.user_key("douyin", uid) if stable else ""
        comment = {
            "platform_comment_id": f"c-{uid}",
            "anonymous_user_key": f"U-{uid}",
            "pseudonymous_user_key": pseudonymous,
            "body": body,
            "published_at": "2026-07-20T10:00:00Z",
            "parent_comment_id": None,
            "comment_identity_key": comment_identity_key(
                platform_comment_id=f"c-{uid}",
                pseudonymous_user_key=pseudonymous or f"U-{uid}",
                body=body,
                published_at="2026-07-20T10:00:00Z",
            ),
        }
        with storage.transaction(self.connection):
            insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=1,
                comments=[comment],
                captured_at="2026-08-01T01:00:00Z",
            )
        if label is not None and stable:
            interaction_id = self.connection.execute(
                "SELECT id FROM interaction_users WHERE pseudonymous_user_key=?",
                (pseudonymous,),
            ).fetchone()[0]
            with storage.transaction(self.connection):
                self.connection.execute(
                    """
                    INSERT INTO interaction_user_classification_versions(
                        interaction_user_id, audience_definition_version,
                        classifier_version, evidence_window_start, evidence_window_end,
                        evidence_sha256, label, created_at
                    ) VALUES (?, ?, ?, '2026-05-05T00:00:00Z','2026-08-03T00:00:00Z',
                              ?, ?, '2026-08-01T02:00:00Z')
                    """,
                    (
                        int(interaction_id),
                        ar.AUDIENCE_DEFINITION_VERSION,
                        ar.CLASSIFIER_VERSION,
                        f"sha-{uid}".ljust(64, "0"),
                        label,
                    ),
                )

    def _capture_run(
        self,
        declared,
        captured,
        completion,
        *,
        window_key: str = "2026-W31",
        updated_at: str = "2026-08-01T00:00:00Z",
    ) -> None:
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO comment_capture_runs(
                    content_id, window_key, provider, adapter_version, status,
                    completion_kind, declared_total_count, captured_distinct_count,
                    valid_comment_count, page_count, created_at, updated_at
                ) VALUES (1,?,'TikHub','tikhub-comments-v8.0+paged-comments-v2',
                          'succeeded', ?, ?, ?, ?, 1,
                          ?,?)
                """,
                (
                    window_key,
                    completion,
                    declared,
                    captured,
                    captured,
                    updated_at,
                    updated_at,
                ),
            )

    def test_ratio_is_user_union_not_content_average(self) -> None:
        self._add_user("u1", "秦L真香", label="automotive")
        self._add_user("u2", "我提车了落地价不错", label="automotive")
        self._add_user("u3", "好看", label="not_identified")
        self._add_user("u4", "路过", label="not_identified")
        self._capture_run(declared=4, captured=4, completion="provider_exhausted")

        result = ar.compute_slice_rate(
            self.connection,
            [1],
            publication_count=1,
            classifier_state="approved",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
            report_cutoff_at="2026-08-03T02:00:00Z",
            warm_up=False,
        )
        metric = result["metric"]
        # U=4, A=2 -> but 4 < 30 -> below_threshold, no percentage
        self.assertEqual(metric["denominator"], 4)
        self.assertEqual(metric["status"], "below_threshold")
        self.assertIsNone(metric["percentage"])
        quality = result["audience_quality"]
        self.assertEqual(quality["identity_coverage_percentage"], 100.0)
        self.assertEqual(quality["comment_collection_coverage_percentage"], 100.0)
        self.assertEqual(quality["user_key_version"], "platform-user-hmac-v2")

    def test_provider_exhaustion_uses_accessible_l1_set_as_coverage_denominator(
        self,
    ) -> None:
        self._add_user("u1", "秦L真香", label="automotive")
        self._capture_run(
            declared=570, captured=1, completion="provider_exhausted"
        )

        result = ar.compute_slice_rate(
            self.connection,
            [1],
            publication_count=1,
            classifier_state="approved",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
            report_cutoff_at="2026-08-03T02:00:00Z",
            warm_up=False,
        )

        quality = result["audience_quality"]
        self.assertEqual(quality["captured_comment_count"], 1)
        self.assertEqual(quality["declared_comment_count"], 1)
        self.assertEqual(quality["comment_collection_coverage_percentage"], 100.0)

    def test_identity_coverage_reflects_unstable_comments(self) -> None:
        self._add_user("s1", "秦L真香", stable=True, label="automotive")
        self._add_user("s2", "变速箱异响", stable=True, label="automotive")
        self._add_user("x1", "无身份一", stable=False)
        self._capture_run(declared=3, captured=3, completion="provider_exhausted")
        result = ar.compute_slice_rate(
            self.connection,
            [1],
            publication_count=1,
            classifier_state="approved",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
            report_cutoff_at="2026-08-03T02:00:00Z",
            warm_up=False,
        )
        # 2 of 3 valid L1 comments carry a stable identity
        self.assertAlmostEqual(
            result["audience_quality"]["identity_coverage_percentage"], 66.67
        )
        self.assertEqual(result["metric"]["denominator"], 2)

    def test_empty_classification_table_cannot_publish_false_zero(self) -> None:
        for index in range(30):
            self._add_user(f"unclassified-{index}", "路过看看")
        self._capture_run(declared=30, captured=30, completion="provider_exhausted")

        result = ar.compute_slice_rate(
            self.connection,
            [1],
            publication_count=1,
            classifier_state="uncalibrated",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
            report_cutoff_at="2026-08-03T02:00:00Z",
            warm_up=False,
        )

        metric = result["metric"]
        quality = result["audience_quality"]
        self.assertEqual(metric["denominator"], 30)
        self.assertEqual(metric["status"], "below_threshold")
        self.assertIsNone(metric["numerator"])
        self.assertIsNone(metric["percentage"])
        self.assertEqual(quality["classified_user_count"], 0)
        self.assertEqual(quality["classification_coverage_percentage"], 0.0)
        self.assertIn("用户分类覆盖率 0.0%", metric["reason"])

    def test_latest_classification_overrides_older_automotive_label(self) -> None:
        self._add_user("changed", "秦L真香", label="automotive")
        user_id = int(
            self.connection.execute(
                "SELECT id FROM interaction_users"
            ).fetchone()[0]
        )
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO interaction_user_classification_versions(
                    interaction_user_id, audience_definition_version,
                    classifier_version, evidence_window_start, evidence_window_end,
                    evidence_sha256, label, created_at
                ) VALUES (?, ?, ?, '2026-05-05T00:00:00Z','2026-08-03T00:00:00Z',
                          ?, 'not_identified', '2026-08-01T03:00:00Z')
                """,
                (
                    user_id,
                    ar.AUDIENCE_DEFINITION_VERSION,
                    ar.CLASSIFIER_VERSION,
                    "newer".ljust(64, "0"),
                ),
            )

        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T02:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["total_users"], 1)
        self.assertEqual(universe["classified_users"], 1)
        self.assertEqual(universe["automotive_users"], 0)

    def test_excluded_classification_never_enters_user_denominator(self) -> None:
        self._add_user("spam", "加微信vx123领优惠券", label="excluded")
        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T02:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["candidate_users"], 1)
        self.assertEqual(universe["classified_users"], 1)
        self.assertEqual(universe["total_users"], 0)
        self.assertEqual(universe["automotive_users"], 0)

    def test_classification_cannot_use_future_evidence_window(self) -> None:
        self._add_user("future-window", "秦L真香", label="automotive")
        user_id = int(
            self.connection.execute("SELECT id FROM interaction_users").fetchone()[0]
        )
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO interaction_user_classification_versions(
                    interaction_user_id, audience_definition_version,
                    classifier_version, evidence_window_start, evidence_window_end,
                    evidence_sha256, label, created_at
                ) VALUES (?, ?, ?, '2026-05-06T00:00:00Z','2026-08-04T00:00:00Z',
                          ?, 'not_identified', '2026-08-01T03:00:00Z')
                """,
                (
                    user_id,
                    ar.AUDIENCE_DEFINITION_VERSION,
                    ar.CLASSIFIER_VERSION,
                    "future".ljust(64, "0"),
                ),
            )

        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T02:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["classified_users"], 1)
        self.assertEqual(universe["automotive_users"], 1)

    def test_latest_evidence_window_outranks_later_backfill_creation(self) -> None:
        self._add_user("window-order", "秦L真香", label="automotive")
        user_id = int(
            self.connection.execute("SELECT id FROM interaction_users").fetchone()[0]
        )
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO interaction_user_classification_versions(
                    interaction_user_id, audience_definition_version,
                    classifier_version, evidence_window_start, evidence_window_end,
                    evidence_sha256, label, created_at
                ) VALUES (?, ?, ?, '2026-05-04T00:00:00Z','2026-08-02T00:00:00Z',
                          ?, 'not_identified', '2026-08-02T05:00:00Z')
                """,
                (
                    user_id,
                    ar.AUDIENCE_DEFINITION_VERSION,
                    ar.CLASSIFIER_VERSION,
                    "older-window".ljust(64, "0"),
                ),
            )
        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T02:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["automotive_users"], 1)

    def test_classification_window_must_cover_latest_user_behavior(self) -> None:
        self._add_user("behavior-after-window", "秦L真香")
        user_id = int(
            self.connection.execute("SELECT id FROM interaction_users").fetchone()[0]
        )
        with storage.transaction(self.connection):
            self.connection.execute(
                "UPDATE comments SET published_at='2026-08-02T00:00:00Z'"
            )
            self.connection.execute(
                """
                INSERT INTO interaction_user_classification_versions(
                    interaction_user_id, audience_definition_version,
                    classifier_version, evidence_window_start, evidence_window_end,
                    evidence_sha256, label, created_at
                ) VALUES (?, ?, ?, '2026-05-03T00:00:00Z','2026-08-01T00:00:00Z',
                          ?, 'automotive', '2026-08-05T00:00:00Z')
                """,
                (
                    user_id,
                    ar.AUDIENCE_DEFINITION_VERSION,
                    ar.CLASSIFIER_VERSION,
                    "behavior-before-window".ljust(64, "0"),
                ),
            )
        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-06T00:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["candidate_users"], 1)
        self.assertEqual(universe["classified_users"], 0)

    def test_classification_window_cannot_extend_past_report_cutoff(self) -> None:
        self._add_user("future-declared-window", "秦L真香")
        user_id = int(
            self.connection.execute("SELECT id FROM interaction_users").fetchone()[0]
        )
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO interaction_user_classification_versions(
                    interaction_user_id, audience_definition_version,
                    classifier_version, evidence_window_start, evidence_window_end,
                    evidence_sha256, label, created_at
                ) VALUES (?, ?, ?, '2026-05-04T18:00:00Z','2026-08-02T18:00:00Z',
                          ?, 'automotive', '2026-08-01T02:00:00Z')
                """,
                (
                    user_id,
                    ar.AUDIENCE_DEFINITION_VERSION,
                    ar.CLASSIFIER_VERSION,
                    "window-past-cutoff".ljust(64, "0"),
                ),
            )
        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-02T12:00:00Z",
            evidence_window_start="2026-05-05T00:00:00Z",
            evidence_window_end="2026-08-03T00:00:00Z",
        )
        self.assertEqual(universe["candidate_users"], 1)
        self.assertEqual(universe["classified_users"], 0)

    def test_latest_comment_evidence_is_selected_at_report_cutoff(self) -> None:
        self._add_user("old-1", "秦L真香", label="automotive")
        self._add_user("old-2", "好看", label="not_identified")
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    id, content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (2,1,'2026-08-02T01:00:00Z','2026-W31','douyin',
                          'data/cache/c2.json', ?, 1, 'available',
                          '2026-08-02T01:00:00Z')
                """,
                ("2" * 64,),
            )
        pseudonymous = self.hasher.user_key("douyin", "new-only")
        with storage.transaction(self.connection):
            insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=2,
                comments=[
                    {
                        "platform_comment_id": "c-new-only",
                        "anonymous_user_key": "U-new-only",
                        "pseudonymous_user_key": pseudonymous,
                        "body": "新评论",
                        "published_at": "2026-07-20T10:00:00Z",
                        "parent_comment_id": None,
                        "comment_identity_key": comment_identity_key(
                            platform_comment_id="c-new-only",
                            pseudonymous_user_key=pseudonymous,
                            body="新评论",
                            published_at="2026-07-20T10:00:00Z",
                        ),
                    }
                ],
                captured_at="2026-08-02T01:00:00Z",
            )

        before = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-01T12:00:00Z",
        )
        after = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T00:00:00Z",
        )
        self.assertEqual(before["candidate_users"], 2)
        self.assertEqual(before["classified_users"], 0)
        self.assertEqual(after["candidate_users"], 1)
        self.assertEqual(after["total_users"], 0)
        self.assertEqual(after["identity_coverage_percentage"], 100.0)

    def test_backfilled_evidence_created_after_cutoff_is_excluded(self) -> None:
        self._add_user("original", "秦L真香", label="automotive")
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO comment_evidence_versions(
                    id, content_id, captured_at, iso_week, source, local_path,
                    sha256, comment_count, status, created_at
                ) VALUES (2,1,'2026-08-01T06:00:00Z','2026-W31','douyin',
                          'data/cache/backfill.json', ?, 0, 'available',
                          '2026-08-04T00:00:00Z')
                """,
                ("b" * 64,),
            )

        universe = ar._slice_user_universe(
            self.connection,
            [1],
            report_cutoff_at="2026-08-01T12:00:00Z",
        )
        self.assertEqual(universe["candidate_users"], 1)
        self.assertEqual(universe["classified_users"], 0)

    def test_capped_content_does_not_drag_coverage(self) -> None:
        self._capture_run(
            declared=None, captured=1000, completion="cap_reached"
        )
        coverage = ar._slice_comment_coverage(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T00:00:00Z",
        )
        self.assertEqual(coverage["capped_content_count"], 1)
        self.assertEqual(coverage["coverage_percentage"], 100.0)

    def test_known_declared_total_is_ignored_after_cap(self) -> None:
        self._capture_run(
            declared=5000, captured=1000, completion="cap_reached"
        )
        coverage = ar._slice_comment_coverage(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T00:00:00Z",
        )
        self.assertEqual(coverage["captured"], 1000)
        self.assertEqual(coverage["declared"], 1000)
        self.assertEqual(coverage["coverage_percentage"], 100.0)

    def test_comment_coverage_uses_latest_run_at_cutoff(self) -> None:
        self._capture_run(
            declared=100,
            captured=100,
            completion="provider_exhausted",
            window_key="2026-W30",
            updated_at="2026-08-01T00:00:00Z",
        )
        self._capture_run(
            declared=100,
            captured=50,
            completion="coverage_target_reached",
            window_key="2026-W31",
            updated_at="2026-08-03T00:00:00Z",
        )
        before = ar._slice_comment_coverage(
            self.connection,
            [1],
            report_cutoff_at="2026-08-02T00:00:00Z",
        )
        after = ar._slice_comment_coverage(
            self.connection,
            [1],
            report_cutoff_at="2026-08-04T00:00:00Z",
        )
        self.assertEqual(before["captured"], 100)
        self.assertEqual(before["coverage_percentage"], 100.0)
        self.assertEqual(after["captured"], 50)
        self.assertEqual(after["coverage_percentage"], 50.0)

    def test_content_without_capture_run_makes_coverage_unknown(self) -> None:
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO content_items(
                    id,link_id,platform,canonical_url,imported_at,created_at,updated_at
                ) VALUES (2,'BBBBBB','douyin','https://example.com/2',
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z',
                          '2026-08-01T00:00:00Z')
                """
            )
        self._capture_run(
            declared=10, captured=10, completion="provider_exhausted"
        )
        coverage = ar._slice_comment_coverage(
            self.connection,
            [1, 2],
            report_cutoff_at="2026-08-03T00:00:00Z",
        )
        self.assertIsNone(coverage["coverage_percentage"])

    def test_unknown_declared_with_more_pages_is_unknown_coverage(self) -> None:
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO comment_capture_runs(
                    content_id, window_key, provider, adapter_version, status,
                    completion_kind, declared_total_count, captured_distinct_count,
                    valid_comment_count, page_count, created_at, updated_at
                ) VALUES (1,'2026-W31','TikHub','a','retryable_failed',
                          NULL, NULL, 40, 40, 1,
                          '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                """
            )
        coverage = ar._slice_comment_coverage(
            self.connection,
            [1],
            report_cutoff_at="2026-08-03T00:00:00Z",
        )
        self.assertIsNone(coverage["coverage_percentage"])


class CalibrationGateTest(unittest.TestCase):
    """active_classifier_state resolves the gold-set record fail-closed."""

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "audience_calibration_v1.json"

    def _platform(self, tp: int, fp: int, fn: int, tn: int) -> dict:
        return {
            "true_positive": tp,
            "false_positive": fp,
            "false_negative": fn,
            "true_negative": tn,
        }

    def _record(self, **overrides) -> dict:
        record = {
            "record_version": "audience-calibration-v1",
            "audience_definition_version": "audience-definition-v1",
            "classifier_version": ar.CLASSIFIER_VERSION,
            "evaluated_at": "2026-08-20T00:00:00Z",
            "operator": "mark",
            "platforms": {
                # precision 95/99≈0.9596, recall 95/110≈0.8636 -> approved
                "douyin": self._platform(95, 4, 15, 386),
                "xiaohongshu": self._platform(95, 4, 15, 386),
            },
        }
        record.update(overrides)
        return record

    def _write(self, record: dict) -> Path:
        import json

        self.path.write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return self.path

    def test_missing_record_is_uncalibrated(self) -> None:
        # 2026-08-07 owner decision: a record that has never been created
        # publishes machine estimates capped at sample_only.
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "uncalibrated"
        )

    def test_unparsable_record_is_rejected(self) -> None:
        # An EXISTING but broken record still fails closed: tampering or a
        # half-written calibration must not publish anything.
        self.path.write_text("{not json", encoding="utf-8")
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "rejected"
        )

    def test_valid_record_with_both_platforms_passing_is_approved(self) -> None:
        self._write(self._record())
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "approved"
        )

    def test_precision_exactly_at_gate_passes(self) -> None:
        # precision 95/100 = 0.95 exactly, recall 95/114≈0.8333 -> approved
        record = self._record()
        record["platforms"]["douyin"] = self._platform(95, 5, 19, 381)
        self._write(record)
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "approved"
        )

    def test_low_recall_platform_downgrades_to_conservative(self) -> None:
        # precision 81/84≈0.964, recall 81/111≈0.7297 -> conservative
        record = self._record()
        record["platforms"]["xiaohongshu"] = self._platform(81, 3, 30, 386)
        self._write(record)
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "conservative"
        )

    def test_low_precision_platform_rejects_everything(self) -> None:
        # precision 90/96=0.9375 < 0.95 hard gate -> rejected
        record = self._record()
        record["platforms"]["douyin"] = self._platform(90, 6, 14, 390)
        self._write(record)
        value = ar.load_calibration_record(self.path)
        self.assertEqual(value["state"], "rejected")
        self.assertEqual(value["platforms"]["douyin"], "rejected")

    def test_sub_500_sample_is_rejected(self) -> None:
        record = self._record()
        record["platforms"]["douyin"] = self._platform(95, 4, 15, 385)
        self._write(record)
        value = ar.load_calibration_record(self.path)
        self.assertEqual(value["state"], "rejected")
        self.assertTrue(any("低于 500 人门槛" in reason for reason in value["reasons"]))

    def test_wrong_classifier_version_is_rejected(self) -> None:
        self._write(self._record(classifier_version="audience-classifier-v999"))
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "rejected"
        )

    def test_missing_platform_is_rejected(self) -> None:
        record = self._record()
        del record["platforms"]["xiaohongshu"]
        self._write(record)
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "rejected"
        )

    def test_declared_state_must_match_recomputed_state(self) -> None:
        record = self._record()
        record["platforms"]["douyin"]["expected_state"] = "conservative"
        self._write(record)
        value = ar.load_calibration_record(self.path)
        self.assertEqual(value["state"], "rejected")
        self.assertTrue(any("不一致" in reason for reason in value["reasons"]))

    def test_negative_or_missing_counts_are_rejected(self) -> None:
        record = self._record()
        record["platforms"]["douyin"]["false_positive"] = -1
        self._write(record)
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "rejected"
        )
        record = self._record()
        del record["platforms"]["douyin"]["true_negative"]
        self._write(record)
        self.assertEqual(
            ar.active_classifier_state(None, record_path=self.path), "rejected"
        )

    def test_default_path_points_at_config_record(self) -> None:
        self.assertEqual(
            ar.CALIBRATION_RECORD_PATH,
            storage.PROJECT_ROOT / "config" / "audience_calibration_v1.json",
        )


if __name__ == "__main__":
    unittest.main()
