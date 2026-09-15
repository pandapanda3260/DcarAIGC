"""Focused regressions for the two confirmed identity audit findings."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import content_identity as identity
from v8.operations import OperationError, import_contents, upsert_content
from v8.storage import connect, initialize_database


class IdentityAuditRegressionsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "fixture.sqlite3"
        with connect(self.path) as db:
            initialize_database(db, target_version=23)
        self.enterContext(patch("socket.socket.connect", side_effect=AssertionError("network forbidden")))

    def test_modal_query_canonicalizes_and_import_keeps_different_works_separate(self):
        identifiers = ("7123456789012345678", "7123456789012345679")
        urls = [f"https://www.douyin.com/?modal_id={value}&utm_source=fixture" for value in identifiers]
        for url, identifier in zip(urls, identifiers):
            parsed = identity.parse("douyin", url, identifier)
            self.assertEqual(parsed["platform_content_id"], identifier)
            self.assertEqual(parsed["canonical_url"], f"https://www.douyin.com/video/{identifier}")
        self.assertNotEqual(identity.alias_url_key("douyin", urls[0]), identity.alias_url_key("douyin", urls[1]))
        result = import_contents([{"platform": "douyin", "canonical_url": url} for url in urls],
                                 source_name="fixture", db_path=self.path)
        self.assertEqual(result["inserted_rows"], 2)
        with connect(self.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM content_link_intakes").fetchone()[0], 0)
            self.assertEqual([row[0] for row in db.execute("SELECT platform_content_id FROM content_items ORDER BY id")], list(identifiers))

    def test_modal_query_rejects_conflicting_or_invalid_identity(self):
        identifier = "7123456789012345678"
        base = f"https://www.douyin.com/video/{identifier}"
        self.assertEqual(identity.parse("douyin", f"{base}?modal_id={identifier}")["platform_content_id"], identifier)
        for url, explicit, code in (
            (f"{base}?modal_id=7123456789012345679", None, "identity_conflict"),
            (f"https://www.douyin.com/?modal_id={identifier}", "7123456789012345679", "identity_conflict"),
            (f"{base}?modal_id={identifier}&modal_id={identifier}", None, "identity_conflict"),
            (f"{base}?modal_id=", None, "identity_conflict"),
            ("https://www.douyin.com/?modal_id=1e18", None, "identity_unresolved"),
        ):
            with self.subTest(url=url, explicit=explicit):
                with self.assertRaises(identity.ContentIdentityError) as raised:
                    identity.parse("douyin", url, explicit)
                self.assertEqual(raised.exception.code, code)

    def claimed_content(self):
        timestamp = "2026-09-13T00:00:00Z"
        with connect(self.path) as db:
            account_id = db.execute("INSERT INTO accounts(phone,operator_name,created_at,updated_at) VALUES('','',?,?)",
                                    (timestamp, timestamp)).lastrowid
            db.execute("INSERT INTO account_platform_identities(account_id,platform,uid,nickname,created_at,updated_at) "
                       "VALUES(?,'douyin','123456789','fixture',?,?)", (account_id, timestamp, timestamp))
        value = {"platform": "douyin", "canonical_url": "https://www.douyin.com/video/7123456789012345678"}
        content_id = upsert_content(value, db_path=self.path)["id"]
        with connect(self.path) as db:
            db.execute("UPDATE content_items SET account_id=?,raw_account_uid=NULL WHERE id=?", (account_id, content_id))
        return value, content_id, account_id

    def test_missing_raw_author_preserves_binding_and_matching_author_can_complete(self):
        value, content_id, account_id = self.claimed_content()
        upsert_content({**value, "title": "updated"}, db_path=self.path)
        with connect(self.path) as db:
            self.assertEqual(tuple(db.execute("SELECT account_id,raw_account_uid,title FROM content_items WHERE id=?",
                                              (content_id,)).fetchone()), (account_id, None, "updated"))
        upsert_content({**value, "account_uid": "123456789"}, db_path=self.path,
                       verified_provider_identity=("douyin", "123456789"))
        with connect(self.path) as db:
            self.assertEqual(tuple(db.execute("SELECT account_id,raw_account_uid FROM content_items WHERE id=?",
                                              (content_id,)).fetchone()), (account_id, "123456789"))

    def test_missing_raw_author_does_not_allow_an_unknown_author_to_replace_binding(self):
        value, content_id, account_id = self.claimed_content()
        with self.assertRaisesRegex(OperationError, "归属冲突"):
            upsert_content({**value, "account_uid": "987654321", "title": "wrong author"}, db_path=self.path,
                           verified_provider_identity=("douyin", "987654321"))
        with connect(self.path) as db:
            self.assertEqual(tuple(db.execute("SELECT account_id,raw_account_uid,title FROM content_items WHERE id=?",
                                              (content_id,)).fetchone()), (account_id, None, ""))


if __name__ == "__main__":
    unittest.main()
