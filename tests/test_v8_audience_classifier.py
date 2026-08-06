from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import List

import v8.audience_classifier as ac
import v8.storage as storage
from v8.identity import PlatformUserHasher, comment_identity_key, insert_comment_rows


def _c(
    content_id: int,
    body: str,
    *,
    published_at: str = "2026-07-15T10:00:00Z",
    author_reply: bool = False,
    context: bool = False,
) -> ac.CommentEvidence:
    return ac.CommentEvidence(
        content_id=content_id,
        body=body,
        published_at=published_at,
        is_author_reply=author_reply,
        context_automotive=context,
    )


def _user(comments: List[ac.CommentEvidence], uid: int = 1) -> ac.UserEvidence:
    return ac.UserEvidence(interaction_user_id=uid, platform="douyin", comments=comments)


class ClassifyUserRulesTest(unittest.TestCase):
    def test_strong_owner_experience_is_automotive(self) -> None:
        result = ac.classify_user(_user([_c(1, "我家这车开了三年十几万公里没修过")]))
        self.assertEqual(result.label, "automotive")
        self.assertIn("strong_evidence", result.reasons)

    def test_technical_model_reference_is_automotive(self) -> None:
        result = ac.classify_user(_user([_c(1, "这台秦L的DM-i变速箱调校真不错")]))
        self.assertEqual(result.label, "automotive")

    def test_high_intent_needs_explicit_context(self) -> None:
        without_context = ac.classify_user(_user([_c(1, "多少钱")]))
        self.assertEqual(without_context.label, "not_identified")
        with_context = ac.classify_user(_user([_c(1, "多少钱", context=True)]))
        self.assertEqual(with_context.label, "automotive")

    def test_two_substantive_across_contents_is_automotive(self) -> None:
        result = ac.classify_user(
            _user(
                [
                    _c(1, "这车油耗怎么样", context=True),
                    _c(2, "后排空间够用吗", context=True),
                ]
            )
        )
        self.assertEqual(result.label, "automotive")
        self.assertIn("two_substantive_comments_across_contents", result.reasons)

    def test_two_substantive_same_content_is_not_enough(self) -> None:
        result = ac.classify_user(
            _user(
                [
                    _c(1, "这车油耗怎么样", context=True),
                    _c(1, "后排空间够用吗", context=True),
                ]
            )
        )
        self.assertEqual(result.label, "not_identified")

    def test_generic_only_never_qualifies(self) -> None:
        for body in ("好看", "哈哈哈", "多少钱", "沙发", "支持一下"):
            result = ac.classify_user(_user([_c(1, body), _c(2, body)]))
            self.assertEqual(result.label, "not_identified", body)

    def test_author_reply_and_spam_are_excluded(self) -> None:
        author = ac.classify_user(_user([_c(1, "感谢支持", author_reply=True)]))
        self.assertEqual(author.label, "excluded")
        spam = ac.classify_user(_user([_c(1, "加微信vx123领优惠券")]))
        self.assertEqual(spam.label, "excluded")
        emoji = ac.classify_user(_user([_c(1, "[赞][赞]")]))
        self.assertEqual(emoji.label, "excluded")

    def test_not_identified_keeps_user_but_makes_no_negative_claim(self) -> None:
        result = ac.classify_user(_user([_c(1, "路过看看")]))
        self.assertEqual(result.label, "not_identified")
        self.assertIsNone(result.confidence)

    def test_evidence_sha_is_order_independent_and_idempotent(self) -> None:
        a = _user([_c(1, "秦L真香"), _c(2, "油耗多少", context=True)])
        b = _user([_c(2, "油耗多少", context=True), _c(1, "秦L真香")])
        self.assertEqual(
            ac.classify_user(a).evidence_sha256,
            ac.classify_user(b).evidence_sha256,
        )


class ClassifyWindowPersistenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / "cls.sqlite3"
        self.connection = storage.connect(self.db)
        storage.initialize_database(self.connection)
        self.hasher = PlatformUserHasher(salt_path=Path(self.temp.name) / ".salt")
        self._seed()

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def _seed(self) -> None:
        with storage.transaction(self.connection):
            self.connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id, version, status, definition, created_at
                ) VALUES ('tax-v5','selling-points-v5.0','published','{}',
                          '2026-08-01T00:00:00Z')
                """
            )
            self.connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id, rule_version, taxonomy_version, matcher_rule_sha256,
                    status, created_at, updated_at
                ) VALUES ('rel-v8', 'evaluation-v8', 'selling-points-v5.0', ?,
                          'active', '2026-08-01T00:00:00Z', '2026-08-01T00:00:00Z')
                """,
                ("a" * 64,),
            )
            for cid in (1, 2):
                self.connection.execute(
                    """
                    INSERT INTO content_items(
                        id, link_id, platform, canonical_url,
                        imported_at, created_at, updated_at
                    ) VALUES (?, ?, 'douyin', ?, '2026-08-01T00:00:00Z',
                              '2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')
                    """,
                    (cid, f"LINK{cid:02d}", f"https://www.douyin.com/video/{cid}"),
                )
                self.connection.execute(
                    """
                    INSERT INTO comment_evidence_versions(
                        id, content_id, captured_at, iso_week, source, local_path,
                        sha256, comment_count, status, created_at
                    ) VALUES (?, ?, '2026-08-01T01:00:00Z','2026-W31','douyin',
                              ?, ?, 1, 'available', '2026-08-01T01:00:00Z')
                    """,
                    (cid, cid, f"data/cache/c{cid}.json", f"{cid:064d}"),
                )
                # content is automotive context
                self.connection.execute(
                    """
                    INSERT INTO evaluation_versions(
                        id, content_id, release_id, rule_version, taxonomy_version,
                        matcher_rule_sha256, evidence_sha256, evaluation_source,
                        evaluation_status, evidence_level, content_direction,
                        content_automotive_score, payload_json, evaluated_at
                    ) VALUES (?, ?, 'rel-v8', 'evaluation-v8', 'selling-points-v5.0', ?,
                              ?, 'automatic', 'evaluated', 'V3', 'used_car', 80, '{}',
                              '2026-08-01T01:00:00Z')
                    """,
                    (cid, cid, "a" * 64, f"ev{cid:062d}"),
                )

    def _add_comment(
        self, cid: int, raw_uid: str, body: str, published_at: str
    ) -> None:
        pseudonymous = self.hasher.user_key("douyin", raw_uid)
        comment = {
            "platform_comment_id": f"{cid}-{raw_uid}",
            "anonymous_user_key": f"U-{cid}-{raw_uid}",
            "pseudonymous_user_key": pseudonymous,
            "body": body,
            "published_at": published_at,
            "parent_comment_id": None,
            "comment_identity_key": comment_identity_key(
                platform_comment_id=f"{cid}-{raw_uid}",
                pseudonymous_user_key=pseudonymous,
                body=body,
                published_at=published_at,
            ),
        }
        with storage.transaction(self.connection):
            insert_comment_rows(
                self.connection,
                platform="douyin",
                evidence_version_id=cid,
                comments=[comment],
                captured_at="2026-08-01T01:00:00Z",
            )

    def test_window_classifies_and_is_idempotent(self) -> None:
        # user A: strong on content 1
        self._add_comment(1, "uA", "我提车了，秦L落地价谈到位了", "2026-07-20T10:00:00Z")
        # user B: two substantive across content 1 and 2
        self._add_comment(1, "uB", "这车油耗怎么样", "2026-07-21T10:00:00Z")
        self._add_comment(2, "uB", "后排空间够用吗", "2026-07-22T10:00:00Z")
        # user C: generic only
        self._add_comment(1, "uC", "好看", "2026-07-23T10:00:00Z")

        summary = ac.classify_window(
            self.connection,
            content_ids=[1, 2],
            evidence_window_start="2026-05-05T00:00:00+00:00",
            evidence_window_end="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(summary["label_counts"]["automotive"], 2)
        self.assertEqual(summary["label_counts"]["not_identified"], 1)
        self.assertEqual(summary["total_users"], 3)

        rows_before = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM interaction_user_classification_versions"
            ).fetchone()[0]
        )
        self.assertEqual(rows_before, 3)

        # re-run: append-only + idempotent → no new rows
        ac.classify_window(
            self.connection,
            content_ids=[1, 2],
            evidence_window_start="2026-05-05T00:00:00+00:00",
            evidence_window_end="2026-08-03T00:00:00+00:00",
        )
        rows_after = int(
            self.connection.execute(
                "SELECT COUNT(*) FROM interaction_user_classification_versions"
            ).fetchone()[0]
        )
        self.assertEqual(rows_after, 3)

    def test_out_of_window_comment_excluded_from_evidence(self) -> None:
        self._add_comment(1, "uOld", "秦L真香", "2026-01-01T10:00:00Z")
        summary = ac.classify_window(
            self.connection,
            content_ids=[1, 2],
            evidence_window_start="2026-05-05T00:00:00+00:00",
            evidence_window_end="2026-08-03T00:00:00+00:00",
        )
        self.assertEqual(summary["total_users"], 0)


class CalibrationTest(unittest.TestCase):
    def test_precision_gate_rejects_below_95(self) -> None:
        # 19 TP, 2 FP -> precision 0.905 < 0.95
        gold = {f"tp{i}": "automotive" for i in range(19)}
        gold.update({f"fp{i}": "not_identified" for i in range(2)})
        preds = {k: "automotive" for k in gold}
        result = ac.evaluate_calibration(preds, gold)
        self.assertEqual(result.state, "rejected")
        self.assertLess(result.precision, 0.95)

    def test_high_precision_low_recall_is_conservative(self) -> None:
        # precision 1.0, recall 0.75
        gold = {f"tp{i}": "automotive" for i in range(8)}
        preds = {f"tp{i}": "automotive" for i in range(6)}
        preds.update({f"tp{i}": "not_identified" for i in range(6, 8)})
        result = ac.evaluate_calibration(preds, gold)
        self.assertEqual(result.state, "conservative")
        self.assertEqual(result.precision, 1.0)
        self.assertAlmostEqual(result.recall, 0.75)

    def test_high_precision_high_recall_is_approved(self) -> None:
        gold = {f"tp{i}": "automotive" for i in range(20)}
        gold.update({f"tn{i}": "not_identified" for i in range(20)})
        preds = {f"tp{i}": "automotive" for i in range(19)}
        preds["tp19"] = "not_identified"
        preds.update({f"tn{i}": "not_identified" for i in range(20)})
        result = ac.evaluate_calibration(preds, gold)
        self.assertEqual(result.state, "approved")
        self.assertEqual(result.precision, 1.0)
        self.assertGreaterEqual(result.recall, 0.80)


if __name__ == "__main__":
    unittest.main()
