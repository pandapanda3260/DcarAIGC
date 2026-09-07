from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from v8 import audience_classifier, audience_selectors, storage


class AudienceSelectorStreamingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.connection = storage.connect(Path(self.temp.name) / 'selectors.sqlite3')
        storage.initialize_database(self.connection)
        self.connection.execute(
            "INSERT INTO content_items(id,link_id,platform,canonical_url,imported_at,created_at,updated_at) "
            "VALUES (1,'FIX001','douyin','https://example.com/1','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')"
        )
        self.connection.execute(
            "INSERT INTO interaction_users(id,platform,key_version,pseudonymous_user_key,first_seen_at,last_seen_at) "
            "VALUES (1,'douyin','platform-user-hmac-v2','fixture-user','2026-08-01T00:00:00Z','2026-08-01T00:00:00Z')"
        )
        self.connection.execute(
            "INSERT INTO comment_evidence_versions(id,content_id,captured_at,iso_week,source,local_path,sha256,comment_count,status,created_at) "
            "VALUES (1,1,'2026-08-01T01:00:00Z','2026-W31','douyin','fixture.json',?,1,'available','2026-08-01T01:00:00Z')",
            ('e' * 64,),
        )
        self.body = '我家这车开了三年十几万公里'
        self.raw = json.dumps({'fixture': 'complete raw payload'}, ensure_ascii=False)
        self.connection.execute(
            "INSERT INTO comments(id,evidence_version_id,platform_comment_id,anonymous_user_key,body,published_at,like_count,interaction_user_id,comment_identity_key,raw_json) "
            "VALUES (1,1,'comment-1','anonymous-1',?,'2026-07-20T10:00:00Z',7,1,'identity-1',?)",
            (self.body, self.raw),
        )
        self.connection.execute(
            "INSERT INTO interaction_user_classification_versions(id,interaction_user_id,audience_definition_version,classifier_version,evidence_window_start,evidence_window_end,evidence_sha256,label,confidence,reason_json,created_at) "
            "VALUES (1,1,?,?,'2026-05-05T00:00:00Z','2026-08-03T00:00:00Z',?,'automotive',0.99,?,'2026-08-01T02:00:00Z')",
            (audience_classifier.AUDIENCE_DEFINITION_VERSION, audience_classifier.CLASSIFIER_VERSION,
             'c' * 64, '["complete reason"]'),
        )
        self.connection.commit()

    def tearDown(self) -> None:
        self.connection.close()
        self.temp.cleanup()

    def assert_unique_columns_and_factory(self, queries: list[str]) -> None:
        self.assertEqual(len(queries), 1)
        cursor = self.connection.execute(queries[0])
        names = [column[0].casefold() for column in cursor.description]
        self.assertEqual(len(names), len(set(names)))
        self.assertIs(self.connection.row_factory, sqlite3.Row)
        self.assertIsInstance(cursor.fetchone(), sqlite3.Row)

    def test_comments_keep_full_payload_and_classifier_evidence(self) -> None:
        expected = dict(self.connection.execute('SELECT * FROM comments WHERE id=1').fetchone())
        expected.update(
            content_id=1, selected_evidence_version_id=1,
            selected_evidence_sha256='e' * 64, evidence_captured_at='2026-08-01T01:00:00Z',
            evidence_status='available', interaction_user_platform='douyin',
            interaction_user_key_version='platform-user-hmac-v2',
        )
        queries = []
        self.connection.set_trace_callback(queries.append)
        try:
            actual = audience_selectors.latest_comment_rows(
                self.connection, [1], report_cutoff_at='2026-08-03T00:00:00Z',
                evidence_window_start='2026-05-05T00:00:00Z', evidence_window_end='2026-08-03T00:00:00Z',
            )
        finally:
            self.connection.set_trace_callback(None)
        self.assertEqual(actual, [expected])
        self.assertEqual(actual[0]['raw_json'], self.raw)
        self.assert_unique_columns_and_factory(queries)
        with patch.object(audience_classifier, '_content_context', return_value={1: True}):
            users = audience_classifier.gather_user_evidence(
                self.connection, content_ids=[1], evidence_window_end='2026-08-03T00:00:00Z',
                report_cutoff_at='2026-08-03T00:00:00Z',
            )
        self.assertEqual(len(users), 1)
        comment = users[0].comments[0]
        self.assertEqual(comment.body, self.body)
        self.assertEqual(comment.source_evidence_version_id, 1)
        self.assertEqual(comment.source_evidence_sha256, 'e' * 64)
        self.assertEqual(comment.source_evidence_captured_at, '2026-08-01T01:00:00Z')
        self.assertIs(self.connection.row_factory, sqlite3.Row)

    def test_classifications_keep_all_fields_and_connection_factory(self) -> None:
        expected = dict(self.connection.execute('SELECT * FROM interaction_user_classification_versions WHERE id=1').fetchone())
        expected['selector_rank'] = 1
        queries = []
        self.connection.set_trace_callback(queries.append)
        try:
            actual = audience_selectors.latest_user_classifications(
                self.connection, [1], audience_definition_version=audience_classifier.AUDIENCE_DEFINITION_VERSION,
                classifier_version=audience_classifier.CLASSIFIER_VERSION,
                report_cutoff_at='2026-08-03T00:00:00Z', evidence_window_end='2026-08-03T00:00:00Z',
            )
        finally:
            self.connection.set_trace_callback(None)
        self.assertEqual(actual, {1: expected})
        self.assert_unique_columns_and_factory(queries)


if __name__ == '__main__':
    unittest.main()
