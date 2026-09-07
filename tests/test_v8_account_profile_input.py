from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import Mock

from v8.account_operating_receipts import record_status_receipt
from v8.account_profile_input import (
    ParsedProfile,
    ProfileInputError,
    ResolvedProfile,
    parse_profile_input,
    resolve_known_profile,
    resolve_profile,
)


SEC = "MS4wLjAB" + "a" * 40
OTHER_SEC = "MS4wLjAB" + "b" * 40
DY_URL = "https://www.douyin.com/user/" + SEC
XHS_UID = "0123456789abcdef01234567"
XHS_URL = "https://www.xiaohongshu.com/user/profile/" + XHS_UID


class ProfileParsingTest(unittest.TestCase):
    def test_long_profiles_auto_detect_platform_and_strip_tracking(self) -> None:
        dy = parse_profile_input(" https://douyin.com:443/user/" + SEC + "/?from=share#tracking ")
        self.assertEqual(dy, ParsedProfile("douyin", DY_URL, None, SEC))
        self.assertEqual(parse_profile_input("https://douyin.com/user/123456789").uid, "123456789")
        xhs = parse_profile_input(XHS_URL.upper().replace("/USER/PROFILE/", "/user/profile/") + "?xsec_token=test")
        self.assertEqual(xhs, ParsedProfile("xiaohongshu", XHS_URL, XHS_UID, None))

    def test_invalid_or_non_homepage_urls_are_rejected_before_expansion(self) -> None:
        values = (
            "http://www.douyin.com/user/" + SEC,
            "https://user:pass@www.douyin.com/user/" + SEC,
            "https://@www.douyin.com/user/" + SEC,
            "https://www.douyin.com:8443/user/" + SEC,
            "https://127.0.0.1/user/" + SEC,
            "https://[::1]/user/" + SEC,
            "https://www.douyin.com.evil.example/user/" + SEC,
            "https://www.douyin.com./user/" + SEC,
            "https://www.douyin.com\\@evil.example/user/" + SEC,
            "https://www.douyin.com/user/%2e%2e/" + SEC,
            "https://www.douyin.com/user/" + SEC + "%2fignored",
            "https://www.douyin.com/video/123456789",
            "https://www.xiaohongshu.com/explore/" + XHS_UID,
            "https://www.douyin.com/user/short-name",
            "https://www.douyin.com/user/MS4w.fake",
            "https://www.douyin.com/user/12345",
            "https://www.douyin.com/user/" + SEC + "//",
            "https://www.xiaohongshu.com/user/profile/123456",
            "https://www.kuaishou.com/profile/123456789",
            "https://www.douyin.com/\nuser/" + SEC,
            "https://www.douyin.com/user/" + SEC + "\x00",
            "https://v.douyin.com/../abc",
            "https://v.douyin.com/a%2fb",
        )
        expand = Mock()
        for value in values:
            with self.subTest(value=value), self.assertRaises(ProfileInputError):
                parse_profile_input(value, expand_url=expand)
        expand.assert_not_called()

    def test_short_links_require_explicit_expander(self) -> None:
        with self.assertRaises(ProfileInputError) as caught:
            parse_profile_input("https://v.douyin.com/abc/")
        self.assertEqual(caught.exception.code, "profile_expansion_required")

    def test_controlled_expansion_receives_clean_short_link_and_checks_final(self) -> None:
        expand = Mock(return_value=DY_URL + "?tracking=1")
        result = parse_profile_input("https://v.douyin.com/abc/?tracking=2#fragment", expand_url=expand)
        self.assertEqual(result.profile_url, DY_URL)
        expand.assert_called_once_with("https://v.douyin.com/abc/")
        self.assertEqual(parse_profile_input("https://xhslink.com/a/abc", expand_url=lambda _: XHS_URL).uid, XHS_UID)
        for target in (XHS_URL, "http://www.douyin.com/user/" + SEC,
                       "https://127.0.0.1/x", "https://v.douyin.com/another",
                       "https://www.douyin.com/video/123456789"):
            with self.subTest(target=target), self.assertRaises(ProfileInputError):
                parse_profile_input("https://v.douyin.com/abc", expand_url=lambda _: target)

    def test_expander_exception_is_safe_error(self) -> None:
        with self.assertRaises(ProfileInputError) as caught:
            parse_profile_input("https://v.douyin.com/abc", expand_url=Mock(side_effect=RuntimeError("secret")))
        self.assertEqual(caught.exception.code, "profile_expansion_failed")
        self.assertNotIn("secret", str(caught.exception))


class KnownProfileTest(unittest.TestCase):
    def setUp(self) -> None:
        # Explicit disposable database; no storage module or default DB path.
        self.connection = sqlite3.connect(":memory:")
        self.connection.executescript("""
            CREATE TABLE account_platform_identities (
                id INTEGER PRIMARY KEY, account_id INTEGER, platform TEXT, uid TEXT, nickname TEXT);
            CREATE TABLE account_provider_references (
                account_identity_id INTEGER, provider TEXT, reference_kind TEXT, reference_value TEXT);
            CREATE TABLE scheduler_runs (id INTEGER PRIMARY KEY,job_id TEXT,scheduled_for TEXT,
                status TEXT,started_at TEXT,completed_at TEXT,details_json TEXT);
            CREATE TABLE scheduler_run_attempts (id INTEGER PRIMARY KEY,scheduler_run_id INTEGER,
                attempt_number INTEGER,invocation_source TEXT,status TEXT,started_at TEXT,
                completed_at TEXT,details_json TEXT);
        """)

    def tearDown(self) -> None:
        self.connection.close()

    def identity(self, identity_id: int = 1, uid: str = "123456789", platform: str = "douyin") -> None:
        self.connection.execute("INSERT INTO account_platform_identities VALUES (?,?,?,?,?)",
                                (identity_id, identity_id + 100, platform, uid, "测试账号"))

    def reference(self, value: str = SEC, *, identity_id: int = 1,
                  provider: str = "tikhub", kind: str = "sec_user_id") -> None:
        self.connection.execute("INSERT INTO account_provider_references VALUES (?,?,?,?)",
                                (identity_id, provider, kind, value))

    def admission(self, *, identity_id: int = 1, uid: str = "123456789", request_id: str = "paused-admission") -> None:
        self.connection.row_factory = sqlite3.Row
        record_status_receipt(self.connection, request_id=request_id,
            account_id=identity_id + 100, account_identity_id=identity_id,
            requested_status="paused", update_frequency=None,
            request={"account_status": "paused", "fields": {}, "admission": {"member": {
                "platform": "douyin", "uid": uid, "profile_ref": DY_URL, "nickname": "暂停账号",
                "metadata": {"sec_user_id": SEC, "display_account_id": "public-paused"}}}},
            actor="test", reason="explicit paused creation",
            before={"enabled": False, "update_frequency": None},
            after={"enabled": False, "update_frequency": None},
            result={"id": identity_id + 100, "account_status": "paused", "update_frequency": None,
                    "enabled": False, "status_request_id": request_id},
            timestamp="2026-09-06T12:00:00Z")

    def test_unknown_sec_stays_unresolved_and_xhs_uid_is_extracted(self) -> None:
        dy = resolve_known_profile(DY_URL, self.connection)
        self.assertIs(type(dy), ParsedProfile)
        self.assertIsNone(dy.uid)
        self.assertEqual(resolve_known_profile(XHS_URL, self.connection).uid, XHS_UID)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM account_platform_identities").fetchone()[0], 0)

    def test_provider_names_are_case_insensitive(self) -> None:
        self.identity()
        for provider in ("tikhub", "TikHub", "TIKHUB", "newrank_matrix", "NEWrank_MATRIX"):
            with self.subTest(provider=provider):
                self.connection.execute("DELETE FROM account_provider_references")
                self.reference(provider=provider)
                result = resolve_known_profile(DY_URL, self.connection)
                self.assertIsInstance(result, ResolvedProfile)
                assert isinstance(result, ResolvedProfile)
                self.assertEqual((result.uid, result.account_id, result.identity_id), ("123456789", 101, 1))

    def test_arbitrary_provider_reference_is_not_trusted(self) -> None:
        self.identity()
        self.reference(provider="caller_assertion")
        self.assertIs(type(resolve_known_profile(DY_URL, self.connection)), ParsedProfile)

    def test_canonical_profile_reference_can_resolve_sec(self) -> None:
        self.identity()
        self.reference(DY_URL, kind="profile_url", provider="newrank_matrix")
        self.assertEqual(resolve_known_profile(DY_URL + "?from=share", self.connection).uid, "123456789")

    def test_local_read_only_with_tuple_and_row_factories(self) -> None:
        self.identity()
        self.reference()
        self.connection.commit()
        self.connection.execute("PRAGMA query_only=ON")
        before = self.connection.total_changes
        self.connection.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK
                                       if action in (sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION)
                                       else sqlite3.SQLITE_DENY)
        for row_factory in (None, sqlite3.Row):
            self.connection.row_factory = row_factory
            result = resolve_known_profile(DY_URL, self.connection)
            self.assertEqual(result.uid, "123456789")
        self.assertEqual(self.connection.total_changes, before)
        self.assertFalse(self.connection.in_transaction)

    def test_cross_platform_reference_is_conflict(self) -> None:
        self.identity(platform="xiaohongshu", uid=XHS_UID)
        self.reference()
        with self.assertRaises(ProfileInputError) as caught:
            resolve_known_profile(DY_URL, self.connection)
        self.assertEqual(caught.exception.code, "identity_conflict")

    def test_multiple_identities_or_uid_reference_disagreement_is_conflict(self) -> None:
        self.identity()
        self.identity(2, "987654321")
        self.reference()
        self.reference(identity_id=2)
        with self.assertRaises(ProfileInputError):
            resolve_known_profile(DY_URL, self.connection)
        self.connection.execute("DELETE FROM account_provider_references")
        self.reference("https://www.douyin.com/user/123456789", identity_id=2, kind="profile_url")
        with self.assertRaises(ProfileInputError):
            resolve_known_profile("https://www.douyin.com/user/123456789", self.connection)

    def test_cached_sec_must_not_hide_conflicting_or_fake_numeric_uid(self) -> None:
        self.identity(uid=SEC)
        self.reference()
        with self.assertRaises(ProfileInputError) as caught:
            resolve_known_profile(DY_URL, self.connection)
        self.assertEqual(caught.exception.code, "identity_unresolved")
        self.connection.execute("UPDATE account_platform_identities SET uid='123456789'")
        self.reference(OTHER_SEC, provider="TikHub")
        with self.assertRaises(ProfileInputError):
            resolve_known_profile(DY_URL, self.connection)

    def test_known_identity_bypasses_lookup(self) -> None:
        self.identity()
        self.reference()
        lookup = Mock()
        self.assertEqual(resolve_profile(DY_URL, self.connection, profile_lookup=lookup).uid, "123456789")
        lookup.assert_not_called()

    def test_paused_admission_resolves_without_provider_reference_or_network(self) -> None:
        self.identity()
        self.connection.execute("UPDATE account_platform_identities SET nickname=''")
        self.admission()
        self.connection.row_factory = None
        self.connection.commit()
        self.connection.execute("PRAGMA query_only=ON")
        before = self.connection.total_changes
        lookup = Mock()
        result = resolve_profile(DY_URL, self.connection, profile_lookup=lookup)
        lookup.assert_not_called()
        self.assertIsInstance(result, ResolvedProfile)
        assert isinstance(result, ResolvedProfile)
        self.assertEqual((result.uid, result.sec_user_id, result.identity_id), ("123456789", SEC, 1))
        self.assertEqual((result.nickname, result.display_account_id), ("暂停账号", "public-paused"))
        self.assertEqual(self.connection.total_changes, before)
        self.assertEqual(self.connection.execute("SELECT count(*) FROM account_provider_references").fetchone()[0], 0)

    def test_paused_receipt_and_other_provider_identity_cannot_claim_same_homepage(self) -> None:
        self.identity()
        self.admission()
        self.identity(2, "987654321")
        self.reference(identity_id=2)
        lookup = Mock()
        with self.assertRaises(ProfileInputError) as caught:
            resolve_profile(DY_URL, self.connection, profile_lookup=lookup)
        self.assertEqual(caught.exception.code, "identity_conflict")
        lookup.assert_not_called()

    def test_paused_admission_must_still_bind_current_uid(self) -> None:
        self.identity()
        self.admission()
        self.connection.execute("UPDATE account_platform_identities SET uid='987654321'")
        with self.assertRaises(ProfileInputError) as caught:
            resolve_profile(DY_URL, self.connection, profile_lookup=Mock())
        self.assertEqual(caught.exception.code, "identity_conflict")

    def test_tampered_paused_receipt_is_not_a_trusted_profile(self) -> None:
        self.identity()
        self.admission()
        self.connection.execute("UPDATE scheduler_runs SET details_json='{}'")
        lookup = Mock()
        with self.assertRaises(ProfileInputError) as caught:
            resolve_profile(DY_URL, self.connection, profile_lookup=lookup)
        self.assertEqual(caught.exception.code, "identity_conflict")
        lookup.assert_not_called()

    def test_known_numeric_uid_without_sec_requires_lookup(self) -> None:
        self.identity()
        numeric_url = "https://www.douyin.com/user/123456789"
        lookup = Mock(return_value={"platform": "douyin", "uid": "123456789",
                                   "sec_user_id": SEC, "nickname": "补全账号"})
        result = resolve_profile(numeric_url, self.connection, profile_lookup=lookup)
        lookup.assert_called_once()
        self.assertEqual(lookup.call_args.args[0].uid, "123456789")
        self.assertIsNone(lookup.call_args.args[0].sec_user_id)
        self.assertIsInstance(result, ResolvedProfile)
        assert isinstance(result, ResolvedProfile)
        self.assertEqual((result.identity_id, result.sec_user_id, result.nickname), (1, SEC, "补全账号"))

    def test_incomplete_known_profile_without_lookup_is_not_resolved(self) -> None:
        self.identity()
        result = resolve_profile("https://www.douyin.com/user/123456789", self.connection)
        self.assertIs(type(result), ParsedProfile)
        self.assertEqual(result.uid, "123456789")

    def test_incomplete_known_profile_lookup_failure_never_returns_success(self) -> None:
        self.identity()
        for lookup in (Mock(side_effect=RuntimeError("unavailable")), Mock(return_value={
                "platform": "douyin", "uid": "123456789", "nickname": "仍缺主页标识"})):
            with self.subTest(lookup=lookup), self.assertRaises(ProfileInputError):
                resolve_profile("https://www.douyin.com/user/123456789", self.connection,
                                profile_lookup=lookup)
            lookup.assert_called_once()

    def test_known_profile_without_nickname_requires_matching_lookup(self) -> None:
        self.identity()
        self.reference()
        self.connection.execute("UPDATE account_platform_identities SET nickname=' '")
        lookup = Mock(return_value={"platform": "douyin", "uid": "123456789",
                                   "sec_user_id": SEC, "nickname": "查到昵称"})
        result = resolve_profile(DY_URL, self.connection, profile_lookup=lookup)
        lookup.assert_called_once()
        self.assertIsInstance(result, ResolvedProfile)
        assert isinstance(result, ResolvedProfile)
        self.assertEqual((result.identity_id, result.nickname), (1, "查到昵称"))
        with self.assertRaises(ProfileInputError):
            resolve_profile(DY_URL, self.connection, profile_lookup=lambda _: {
                "platform": "douyin", "uid": "987654321", "sec_user_id": SEC, "nickname": "其他账号"})

    def test_valid_lookup_resolves_identity_without_writes(self) -> None:
        lookup = Mock(return_value={"platform": "douyin", "uid": "123456789", "sec_user_id": SEC,
                                   "nickname": " 新账号 ", "display_account_id": "public-name", "source_raw_response_id": 4})
        before = self.connection.total_changes
        result = resolve_profile(DY_URL, self.connection, profile_lookup=lookup)
        self.assertIsInstance(result, ResolvedProfile)
        assert isinstance(result, ResolvedProfile)
        self.assertEqual((result.uid, result.nickname, result.source), ("123456789", "新账号", "lookup"))
        self.assertIsNone(result.account_id)
        self.assertEqual(self.connection.total_changes, before)
        lookup.assert_called_once_with(ParsedProfile("douyin", DY_URL, None, SEC))

    def test_lookup_must_match_both_identifiers_and_return_complete_profile(self) -> None:
        valid = {"platform": "douyin", "uid": "123456789", "sec_user_id": SEC, "nickname": "账号"}
        for patch in ({"platform": "xiaohongshu"}, {"uid": SEC}, {"uid": 123456789},
                      {"sec_user_id": OTHER_SEC}, {"sec_user_id": None}, {"nickname": ""},
                      {"display_account_id": 123}, {"identity_id": 1}, {"account_id": 1},
                      {"source_raw_response_id": True}, {"source_raw_response_id": -1}):
            with self.subTest(patch=patch), self.assertRaises(ProfileInputError):
                resolve_profile(DY_URL, self.connection, profile_lookup=lambda _: {**valid, **patch})
        with self.assertRaises(ProfileInputError):
            resolve_profile("https://www.douyin.com/user/987654321", self.connection,
                            profile_lookup=lambda _: valid)

    def test_callback_failure_never_becomes_success(self) -> None:
        with self.assertRaises(ProfileInputError) as caught:
            resolve_profile(DY_URL, self.connection, profile_lookup=Mock(side_effect=RuntimeError("secret")))
        self.assertEqual(caught.exception.code, "profile_lookup_failed")
        self.assertNotIn("secret", str(caught.exception))

    def test_returned_uid_rechecks_existing_conflicting_sec(self) -> None:
        self.identity()
        self.reference(OTHER_SEC)
        with self.assertRaises(ProfileInputError):
            resolve_profile(DY_URL, self.connection, profile_lookup=lambda _: {
                "platform": "douyin", "uid": "123456789", "sec_user_id": SEC, "nickname": "账号"})

    def test_returned_uid_can_identify_existing_account_without_sec_reference(self) -> None:
        self.identity()
        result = resolve_profile(DY_URL, self.connection, profile_lookup=lambda _: {
            "platform": "douyin", "uid": "123456789", "sec_user_id": SEC, "nickname": "账号"})
        self.assertIsInstance(result, ResolvedProfile)
        assert isinstance(result, ResolvedProfile)
        self.assertEqual(result.identity_id, 1)

    def test_xhs_lookup_must_agree_with_url(self) -> None:
        result = resolve_profile(XHS_URL, self.connection, profile_lookup=lambda _: {
            "platform": "xiaohongshu", "uid": XHS_UID, "nickname": "小红书账号"})
        self.assertIsInstance(result, ResolvedProfile)
        for patch in ({"uid": "f" * 24}, {"sec_user_id": SEC}):
            with self.subTest(patch=patch), self.assertRaises(ProfileInputError):
                resolve_profile(XHS_URL, self.connection, profile_lookup=lambda _: {
                    "platform": "xiaohongshu", "uid": XHS_UID, "nickname": "账号", **patch})


if __name__ == "__main__":
    unittest.main()
