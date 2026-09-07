from __future__ import annotations

import copy
import hashlib
import http.client
import json
import os
import tempfile
import unittest
import urllib.error
import urllib.request
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, patch

from v8.capture import CaptureError
from v8.newrank_matrix import (
    ACCOUNTS_PATH, ACCOUNT_METRIC_FIELDS, CONTENT_METRIC_FIELDS, GATEWAY,
    WORKS_PATH, MatrixConfig, MatrixConfigurationError, MatrixRowError,
    NewrankMatrixClient, _NoRedirect, _transport, beijing_query_bounds,
    load_config, normalize_account, normalize_gateway, normalize_work,
    sign_request,
)

TEST_CONFIG = MatrixConfig(
    api_url=GATEWAY, n_token="fixture-token-secret",
    key_id="fixture-key-id", secret_key="fixture-signing-secret",
)
START = "2026-08-26T16:00:00Z"
END = "2026-08-27T16:00:00Z"
NOW = datetime(2026, 8, 28, 1, 2, 3, tzinfo=timezone.utc)
LONG_ID = "7379190309625810185"
CURSOR = [1718236740000, LONG_ID]


def wire(data, *, code=0):
    return json.dumps({"code": code, "data": data}, ensure_ascii=False).encode()


def work(**changes):
    row = {
        "platType": 2, "awemeId": LONG_ID, "uid": "1234567890123456789",
        "nickname": "账号", "title": "测试作品",
        "createTime": "2026-08-27 10:11:12",
        "dyShareUrl": "https://www.iesdouyin.com/share/video/" + LONG_ID + "?share=original",
        "playCount": 123, "diggCount": 4, "commentCount": 0,
        "shareCount": 1, "favoriteCount": 2, "scrollId": copy.deepcopy(CURSOR),
    }
    row.update(changes)
    return row


def account(**changes):
    row = {
        "platType": 2, "uid": "1234567890123456789", "rankDate": "2026-08-27",
        "uniqueId": "display-001", "cNickname": "账号",
        "cAvatar": "https://example.com/avatar", "cDescible": "简介",
        "verifyType": 0, "enterpriseVerifyReason": "认证",
        "cTotalFans": 123, "awemeCount": 321, "totalFavorited": 456,
        "workPlayCountAdd": 0, "workFavoritedCountAdd": 1,
        "workCommentCountAdd": 2, "workShareCountAdd": 3,
    }
    row.update(changes)
    return row


class MatrixConfigTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "matrix.env"

    def write_config(self, extra="", *, secret_name="NEWRANK_MATRIX_SECRET_KEY"):
        self.path.write_text(
            "# another module remains untouched\nOTHER_TOKEN=unrelated\n"
            f"export NEWRANK_MATRIX_API_URL='{GATEWAY}' # approved\n"
            "NEWRANK_MATRIX_N_TOKEN=fixture-token-secret\n"
            "NEWRANK_MATRIX_KEY_ID=fixture-key-id\n"
            f'{secret_name}="fixture-signing-secret"\n' + extra,
            encoding="utf-8",
        )
        self.path.chmod(0o600)

    def test_canonical_and_known_full_work_url_only(self):
        for url in (GATEWAY, GATEWAY + "/", GATEWAY + WORKS_PATH):
            with self.subTest(url=url):
                self.assertEqual(normalize_gateway(url), GATEWAY)
        self.assertEqual(
            normalize_gateway("https://GW.NEWRANK.CN:443/api/data_management_api"),
            GATEWAY,
        )

    def test_unsafe_or_unverified_gateway_is_rejected(self):
        for url in (
            "http://gw.newrank.cn/api/data_management_api",
            "https://gw.newrank.cn.evil.invalid/api/data_management_api",
            "https://evil.invalid/api/data_management_api",
            "https://user:password@gw.newrank.cn/api/data_management_api",
            GATEWAY + "?redirect=evil", GATEWAY + "#fragment",
            GATEWAY + ACCOUNTS_PATH, GATEWAY + "/unknown",
            "https://gw.newrank.cn:444/api/data_management_api",
            GATEWAY + "\n", GATEWAY.replace("/api/", "/%61pi/"),
        ):
            with self.subTest(url=url), self.assertRaises(MatrixConfigurationError):
                normalize_gateway(url)

    def test_module_read_preserves_file_and_does_not_use_static_sign(self):
        self.write_config("NEWRANK_MATRIX_SIGN=static-value-not-used\n")
        before = self.path.read_bytes()
        config = load_config(self.path, environ={})
        self.assertEqual(config, TEST_CONFIG)
        self.assertEqual(self.path.read_bytes(), before)
        for value in (config.n_token, config.secret_key, config.key_id):
            self.assertNotIn(value, repr(config))

    def test_legacy_secret_alias_requires_agreement(self):
        self.write_config(secret_name="secretKey")
        self.assertEqual(load_config(self.path, environ={}), TEST_CONFIG)
        self.write_config("secretKey=fixture-signing-secret\n")
        self.assertEqual(load_config(self.path, environ={}), TEST_CONFIG)
        self.write_config("secretKey=conflicting-secret-value\n")
        with self.assertRaises(MatrixConfigurationError) as caught:
            load_config(self.path, environ={})
        self.assertNotIn("conflicting-secret-value", str(caught.exception))
        self.assertNotIn("fixture-signing-secret", str(caught.exception))

    def test_environment_override_and_missing_file(self):
        env = {
            "NEWRANK_MATRIX_API_URL": GATEWAY,
            "NEWRANK_MATRIX_N_TOKEN": TEST_CONFIG.n_token,
            "NEWRANK_MATRIX_KEY_ID": TEST_CONFIG.key_id,
            "NEWRANK_MATRIX_SECRET_KEY": TEST_CONFIG.secret_key,
            "NEWRANK_MATRIX_CONFIG_FILE": str(self.path),
        }
        self.assertEqual(load_config(environ=env), TEST_CONFIG)
        self.write_config()
        env["NEWRANK_MATRIX_N_TOKEN"] = "different-test-token"
        self.assertEqual(load_config(self.path, environ=env).n_token, "different-test-token")

    def test_missing_secret_cannot_be_replaced_by_static_signature(self):
        self.path.write_text(
            f"NEWRANK_MATRIX_API_URL={GATEWAY}\n"
            "NEWRANK_MATRIX_N_TOKEN=token\nNEWRANK_MATRIX_KEY_ID=key\n"
            "NEWRANK_MATRIX_SIGN=pretend-static-signature\n",
            encoding="utf-8",
        )
        self.path.chmod(0o600)
        with self.assertRaises(MatrixConfigurationError):
            load_config(self.path, environ={})

    def test_duplicate_keys_and_invalid_quotes_are_rejected(self):
        for extra in (
            "NEWRANK_MATRIX_N_TOKEN=repeated-secret\n",
            "secretKey='unclosed-value\n",
        ):
            with self.subTest(extra=extra):
                self.write_config(extra)
                with self.assertRaises(MatrixConfigurationError):
                    load_config(self.path, environ={})

    def test_private_regular_file_is_required(self):
        self.write_config()
        self.path.chmod(0o644)
        with self.assertRaises(MatrixConfigurationError):
            load_config(self.path, environ={})
        self.path.chmod(0o600)
        alias = self.path.with_name("alias.env")
        alias.symlink_to(self.path)
        with self.assertRaises(MatrixConfigurationError):
            load_config(alias, environ={})

    def test_config_rejects_header_injection_and_empty_credentials(self):
        for token in ("", "  ", "token\r\nAuthorization: unexpected", "token\x00"):
            with self.subTest(token=token), self.assertRaises(MatrixConfigurationError):
                MatrixConfig(GATEWAY, token, "id", "secret")

    def test_directory_and_fifo_configuration_fail_without_waiting_for_input(self):
        with self.assertRaises(MatrixConfigurationError):
            load_config(Path(self.temp.name), environ={})
        fifo = Path(self.temp.name) / "config.fifo"
        os.mkfifo(fifo, 0o600)
        with self.assertRaises(MatrixConfigurationError):
            load_config(fifo, environ={})

    def test_signature_known_answer(self):
        self.assertEqual(
            sign_request(
                "sample-secret", WORKS_PATH, '{"pageSize":100,"platType":2}'
            ),
            "c1ef6ea2b5448deebd259a35f5ddbf2aa2a16d89",
        )
        with self.assertRaises(ValueError):
            sign_request("sample-secret", GATEWAY + WORKS_PATH, "{}")


class MatrixClientTest(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.responses = []
        self.client = NewrankMatrixClient(
            TEST_CONFIG, transport=self.transport, clock=lambda: NOW
        )

    def transport(self, request, timeout):
        self.calls.append((request, timeout))
        return self.responses.pop(0)

    def fetch(self, **kwargs):
        return self.client.fetch_works_page("douyin", START, END, **kwargs)

    def test_signs_exact_sent_json_and_uses_only_business_path(self):
        self.responses = [(200, wire([]))]
        page = self.fetch(scroll_id=[1718236740000, LONG_ID, "原样"])
        request, timeout = self.calls[0]
        body = json.loads(request.data.decode())
        self.assertEqual(request.full_url, GATEWAY + WORKS_PATH)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("N-token"), TEST_CONFIG.n_token)
        self.assertEqual(timeout, 45)
        self.assertEqual(set(body), {"keyId", "sign", "pathName", "reqJson"})
        self.assertEqual(body["pathName"], WORKS_PATH)
        self.assertIn("原样", body["reqJson"])
        material = (
            TEST_CONFIG.secret_key + "pathName" + WORKS_PATH + "reqJson"
            + body["reqJson"] + "secretKey" + TEST_CONFIG.secret_key
        )
        self.assertEqual(body["sign"], hashlib.sha1(material.encode()).hexdigest())
        query = json.loads(body["reqJson"])
        self.assertEqual(query, page.query)
        self.assertEqual(query["startDate"], "2026-08-27 00:00:00")
        self.assertEqual(query["endDate"], "2026-08-27 23:59:59")
        self.assertEqual(query["scrollId"], [1718236740000, LONG_ID, "原样"])
        self.assertNotIn("uid", query)
        self.assertNotIn("accountFilter", query)
        self.assertEqual(page.captured_at, "2026-08-28T01:02:03Z")

    def test_cursor_dates_and_platform_are_resigned_each_time(self):
        self.responses = [(200, wire([]))] * 4
        self.fetch()
        self.fetch(scroll_id=CURSOR)
        self.client.fetch_works_page("xiaohongshu", START, END)
        self.client.fetch_works_page(
            "douyin", "2026-08-27T16:00:00Z", "2026-08-28T16:00:00Z"
        )
        bodies = [json.loads(request.data) for request, _ in self.calls]
        self.assertEqual(len({body["sign"] for body in bodies}), 4)

    def test_rank_data_defaults_to_previous_beijing_day(self):
        self.responses = [(200, wire([]))]
        page = self.client.fetch_accounts_page("douyin")
        self.assertEqual(page.path_name, ACCOUNTS_PATH)
        self.assertEqual(page.query["rankData"], "2026-08-27")
        self.assertNotIn("rankDate", page.query)
        self.assertEqual(page.query["platType"], 2)

    def test_double_json_keeps_rows_and_raw_without_auth_headers(self):
        rows = [work(), None, "invalid row", work(awemeId="second-work")]
        payload = wire(json.dumps(rows, ensure_ascii=False))
        self.responses = [(200, payload)]
        page = self.fetch()
        self.assertEqual(page.rows, rows)
        self.assertEqual(page.raw_response, json.loads(payload))
        self.assertEqual(page.next_cursor, CURSOR)
        self.assertFalse(page.terminal)
        self.assertIsNone(page.pagination_error)
        page.next_cursor[0] = 0
        self.assertEqual(page.rows[-1]["scrollId"], CURSOR)
        text = json.dumps(page.raw_response)
        for value in (TEST_CONFIG.n_token, TEST_CONFIG.key_id, TEST_CONFIG.secret_key):
            self.assertNotIn(value, text)

    def test_short_nonempty_is_not_terminal_and_empty_is_terminal(self):
        self.responses = [(200, wire([work()])), (200, wire([]))]
        first = self.fetch()
        self.assertFalse(first.terminal)
        self.assertIsNotNone(first.next_cursor)
        second = self.fetch(scroll_id=first.next_cursor)
        self.assertTrue(second.terminal)
        self.assertIsNone(second.next_cursor)
        self.assertEqual(len(self.calls), 2)

    def test_missing_or_invalid_cursor_retains_page_for_ledger(self):
        for last in (work(scrollId=None), work(scrollId=[]), work(scrollId="bad"), "bad"):
            with self.subTest(last=last):
                self.responses = [(200, wire([last]))]
                page = self.fetch()
                self.assertFalse(page.terminal)
                self.assertEqual(page.rows, [last])
                self.assertIsNone(page.next_cursor)
                self.assertEqual(page.pagination_error, "missing_or_invalid_last_scroll_id")

    def test_repeated_cursor_is_retained_not_auto_fetched(self):
        self.responses = [(200, wire([work()]))]
        page = self.fetch(scroll_id=CURSOR)
        self.assertEqual(page.next_cursor, CURSOR)
        self.assertFalse(page.terminal)
        self.assertEqual(len(self.calls), 1)

    def test_invalid_inputs_fail_before_transport(self):
        for page_size in (0, 101, 1.5, True):
            with self.subTest(page_size=page_size), self.assertRaises(ValueError):
                self.fetch(page_size=page_size)
        for cursor in ("text", {}, [], [float("nan")]):
            with self.subTest(cursor=cursor), self.assertRaises(ValueError):
                self.fetch(scroll_id=cursor)
        with self.assertRaises(ValueError):
            self.client.fetch_accounts_page("unsupported")
        with self.assertRaises(ValueError):
            self.client.fetch_accounts_page("douyin", "2026-08-27T00:00:00Z")
        self.assertEqual(self.calls, [])

    def test_http_errors_are_not_success_envelopes(self):
        for status, retryable in ((302, False), (401, False), (403, False), (429, True), (500, True)):
            with self.subTest(status=status):
                self.responses = [(status, wire([]))]
                with self.assertRaises(CaptureError) as caught:
                    self.fetch()
                self.assertEqual(caught.exception.http_status, status)
                self.assertEqual(caught.exception.retryable, retryable)
                self.assertFalse(caught.exception.billed)

    def test_business_code_is_required_and_not_boolean(self):
        for code in (False, True, 0.0, 5000, "5000", "000", None):
            with self.subTest(code=code):
                self.responses = [(200, wire([], code=code))]
                with self.assertRaises(CaptureError) as caught:
                    self.fetch()
                self.assertEqual(caught.exception.error_code, "matrix_business_error")
        self.responses = [(200, wire([], code="0"))]
        self.assertTrue(self.fetch().terminal)


    def test_malformed_payloads_fail_without_becoming_empty_pages(self):
        for payload, reason in (
            (b"not-json", "matrix_invalid_envelope"),
            (b'{"data":[]}', "matrix_invalid_envelope"),
            (b'{"code":0,"data":[NaN]}', "matrix_invalid_envelope"),
            (wire(None), "matrix_invalid_data"),
            (wire({}), "matrix_invalid_data"),
            (wire("{broken"), "matrix_invalid_data"),
            (wire('"still a string"'), "matrix_invalid_data"),
        ):
            with self.subTest(payload=payload):
                self.responses = [(200, payload)]
                with self.assertRaises(CaptureError) as caught:
                    self.fetch()
                self.assertEqual(caught.exception.error_code, reason)
                self.assertIsNotNone(caught.exception.raw_response)

    def test_response_and_error_redact_echoed_credentials(self):
        rows = [work(**{
            "N-Token": TEST_CONFIG.n_token,
            "secretKey": "foreign-echoed-secret",
            "nested": {"Authorization": "Bearer should-not-escape"},
        })]
        payload = {
            "code": 0, "data": json.dumps(rows),
            "msg": "echo " + TEST_CONFIG.secret_key,
            "keyId": TEST_CONFIG.key_id,
        }
        self.responses = [(200, json.dumps(payload).encode())]
        page = self.fetch()
        serialized = json.dumps(page.raw_response)
        for secret in (
            TEST_CONFIG.n_token, TEST_CONFIG.key_id, TEST_CONFIG.secret_key,
            "foreign-echoed-secret", "should-not-escape",
        ):
            self.assertNotIn(secret, serialized)
        self.assertEqual(page.rows[0]["N-Token"], "[REDACTED]")
        self.assertEqual(page.rows[0]["nested"]["Authorization"], "[REDACTED]")
        self.responses = [(403, json.dumps(payload).encode())]
        with self.assertRaises(CaptureError) as caught:
            self.fetch()
        self.assertNotIn(TEST_CONFIG.secret_key, str(caught.exception))
        self.assertNotIn(TEST_CONFIG.secret_key, json.dumps(caught.exception.raw_response))

    def test_transport_exception_does_not_leak_error_text(self):
        self.client = NewrankMatrixClient(
            TEST_CONFIG,
            transport=Mock(side_effect=OSError("echo " + TEST_CONFIG.n_token)),
        )
        with self.assertRaises(CaptureError) as caught:
            self.fetch()
        self.assertEqual(caught.exception.error_code, "matrix_transport_failed")
        self.assertNotIn(TEST_CONFIG.n_token, str(caught.exception))
        self.assertIsNone(caught.exception.raw_response)

    def test_bounded_never_accepts_oversized_response(self):
        self.responses = [(200, b"x" * 11)]
        with patch("v8.newrank_matrix.MAX_RESPONSE_BYTES", 10):
            with self.assertRaises(CaptureError) as caught:
                self.fetch()
        self.assertEqual(caught.exception.error_code, "matrix_response_size_limit")

    def test_beijing_window_conversion_requires_full_seconds_and_timezone(self):
        self.assertEqual(
            beijing_query_bounds(START, END),
            ("2026-08-27 00:00:00", "2026-08-27 23:59:59"),
        )
        self.assertEqual(
            beijing_query_bounds("2026-08-27T00:00:00+08:00", "2026-08-27T00:00:01+08:00"),
            ("2026-08-27 00:00:00", "2026-08-27 00:00:00"),
        )
        for start, end in (
            (END, START), (START, START),
            ("2026-08-27 00:00:00", END),
            ("2026-08-26T16:00:00.001Z", END),
        ):
            with self.subTest(start=start), self.assertRaises(ValueError):
                beijing_query_bounds(start, end)


class MatrixTransportTest(unittest.TestCase):
    def request(self):
        return urllib.request.Request(GATEWAY + WORKS_PATH, data=b"{}")

    def test_redirect_handler_refuses_all_targets(self):
        handler = _NoRedirect()
        for target in (GATEWAY + ACCOUNTS_PATH, "https://evil.invalid"):
            self.assertIsNone(handler.redirect_request(None, None, 302, "", {}, target))

    def test_default_transport_uses_redirect_blocker(self):
        response = BytesIO(wire([]))
        response.status = 200
        response.geturl = lambda: GATEWAY + WORKS_PATH
        opener = Mock()
        opener.open.return_value = response
        with patch("v8.newrank_matrix.urllib.request.build_opener", return_value=opener) as builder:
            status, body = _transport(self.request(), 1)
        self.assertIsInstance(builder.call_args.args[0], _NoRedirect)
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"code": 0, "data": []})
        self.assertEqual(opener.open.call_count, 1)

    def test_unexpected_final_url_is_rejected_before_body_read(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        response.geturl.return_value = "https://evil.invalid"
        opener = Mock()
        opener.open.return_value = response
        with patch("v8.newrank_matrix.urllib.request.build_opener", return_value=opener):
            with self.assertRaises(CaptureError):
                _transport(self.request(), 1)
        response.read.assert_not_called()

    def test_incomplete_read_requires_independently_complete_json(self):
        response = Mock()
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=None)
        response.geturl.return_value = GATEWAY + WORKS_PATH
        response.status = 200
        response.read.side_effect = http.client.IncompleteRead(wire([]), 100)
        opener = Mock()
        opener.open.return_value = response
        with patch("v8.newrank_matrix.urllib.request.build_opener", return_value=opener):
            status, body = _transport(self.request(), 1)
        client = NewrankMatrixClient(TEST_CONFIG, transport=lambda req, timeout: (status, body))
        self.assertTrue(client.fetch_works_page("douyin", START, END).terminal)
        response.read.side_effect = http.client.IncompleteRead(b'{"code":0,', 100)
        with patch("v8.newrank_matrix.urllib.request.build_opener", return_value=opener):
            status, body = _transport(self.request(), 1)
        with self.assertRaises(CaptureError):
            client.fetch_works_page("douyin", START, END)


class MatrixNormalizationTest(unittest.TestCase):
    def test_work_identity_links_and_media_boundary(self):
        source = work(
            mType=2, dyCreationType=1, duration=90, cover="cover-url",
            anaInteraction=999, firstDayFansCountIncrease=-845,
        )
        before = copy.deepcopy(source)
        result = normalize_work(source, platform="douyin")
        self.assertEqual(result["platform_content_id"], LONG_ID)
        self.assertEqual(result["account_uid"], source["uid"])
        self.assertEqual(result["original_share_url"], source["dyShareUrl"])
        self.assertEqual(result["canonical_url"], "https://www.douyin.com/video/" + LONG_ID)
        self.assertEqual(result["published_at"], "2026-08-27T02:11:12Z")
        self.assertEqual(result["content_type"], "unknown")
        self.assertIsNone(result["body"])
        self.assertNotIn("media_urls", result)
        self.assertNotIn("duration", result)
        self.assertNotIn("interaction_count", result["metrics"])
        self.assertEqual(result["metadata"]["raw_only"]["firstDayFansCountIncrease"], -845)
        self.assertEqual(result["metadata"]["raw_only"]["anaInteraction"], 999)
        self.assertEqual(source, before)

    def test_integer_ids_preserve_all_digits_digits_string(self):
        source = work(awemeId=int(LONG_ID), uid=1234567890123456789)
        result = normalize_work(source, platform="douyin")
        self.assertEqual(result["platform_content_id"], LONG_ID)
        self.assertEqual(result["account_uid"], "1234567890123456789")
        self.assertIsInstance(result["platform_content_id"], str)
        for invalid in (float(LONG_ID), True, None, "", "bad/id", "0", -1):
            with self.subTest(invalid=invalid), self.assertRaises(MatrixRowError):
                normalize_work(work(awemeId=invalid), platform="douyin")

    def test_malformed_rows_and_platform_mismatch_are_explicit(self):
        for row in (None, [], "bad", {"awemeId": LONG_ID}, work(platType=6), work(platType=True)):
            with self.subTest(row=row), self.assertRaises(MatrixRowError):
                normalize_work(row, platform="douyin")
        with self.assertRaises(MatrixRowError):
            normalize_account(account(uid=None), platform="douyin", rank_date="2026-08-27")

    def test_missing_uid_is_not_guessed_from_nickname(self):
        result = normalize_work(work(uid=None), platform="douyin")
        self.assertIsNone(result["account_uid"])
        self.assertEqual(result["field_status"]["account_uid"]["status"], "missing")
        self.assertEqual(result["account_name"], "账号")

    def test_five_metric_states_are_distinct_and_zero_is_valid(self):
        result = normalize_work(
            work(platType=6, playCount=123, diggCount=0, commentCount=None, shareCount=-1, favoriteCount=99),
            platform="xiaohongshu",
            requested_fields={"view_count", "like_count", "comment_count", "share_count"},
        )
        self.assertEqual(
            {key: status["status"] for key, status in result["field_status"].items() if key in CONTENT_METRIC_FIELDS},
            {
                "view_count": "not_applicable", "like_count": "provided",
                "comment_count": "missing", "share_count": "invalid",
                "collect_count": "not_requested",
            },
        )
        self.assertEqual(result["metrics"]["like_count"], 0)
        for key in ("view_count", "comment_count", "share_count", "collect_count"):
            self.assertIsNone(result["metrics"][key])

    def test_counts_are_exact_integers_not_floats_booleans_or_units(self):
        for invalid in (-1, True, 1.0, float("inf"), float("nan"), "1.2", "1万", str(2**63), "1" * 5000, {}):
            with self.subTest(invalid=invalid):
                result = normalize_work(work(playCount=invalid), platform="douyin")
                self.assertEqual(result["field_status"]["view_count"]["status"], "invalid")
                self.assertIsNone(result["metrics"]["view_count"])
        result = normalize_work(work(playCount="0"), platform="douyin")
        self.assertEqual(result["metrics"]["view_count"], 0)
        self.assertEqual(result["field_status"]["view_count"]["status"], "provided")
        with self.assertRaises(ValueError):
            normalize_work(work(), platform="douyin", requested_fields={"unknown_field"})

    def test_missing_datetime_is_not_capture_time_and_date_only_is_invalid(self):
        for missing in (None, ""):
            result = normalize_work(work(createTime=missing), platform="douyin")
            self.assertIsNone(result["published_at"])
            self.assertEqual(result["field_status"]["published_at"]["status"], "missing")
        for invalid in ("2026-08-27", "2026-99-27 12:00:00", 1724724000):
            result = normalize_work(work(createTime=invalid), platform="douyin")
            self.assertIsNone(result["published_at"])
            self.assertEqual(result["field_status"]["published_at"]["status"], "invalid")
        result = normalize_work(work(createTime="2026-08-27T01:00:00Z"), platform="douyin")
        self.assertEqual(result["published_at"], "2026-08-27T01:00:00Z")

    def test_share_link_invalid_protocols_never_enter_renderable_fields(self):
        for invalid in ("javascript:alert(1)", "file:///secret", "https://u:p@example.com",
                        "https://example.com/\npath", "/relative"):
            result = normalize_work(work(dyShareUrl=invalid), platform="douyin")
            self.assertIsNone(result["original_share_url"])
            self.assertEqual(result["field_status"]["original_share_url"]["status"], "invalid")
            self.assertEqual(result["canonical_url"], "https://www.douyin.com/video/" + LONG_ID)
        result = normalize_work(
            work(platType=6, xhsShareUrl="https://xhslink.com/a/valid", dyShareUrl="wrong-platform"),
            platform="xiaohongshu",
        )
        self.assertEqual(result["original_share_url"], "https://xhslink.com/a/valid")
        self.assertEqual(result["canonical_url"], "https://www.xiaohongshu.com/explore/" + LONG_ID)

    def test_account_uses_identity_rankdate_and_signed_daily_increments(self):
        result = normalize_account(
            account(cTotalFans=0, workPlayCountAdd=-7, workFavoritedCountAdd="-2"),
            platform="douyin", rank_date=date(2026, 8, 27),
        )
        self.assertEqual(result["identity"], {"platform": "douyin", "uid": "1234567890123456789"})
        self.assertEqual(result["statistics_date"], "2026-08-27")
        self.assertEqual(result["basis"], "matrix_daily")
        self.assertEqual(set(result["metrics"]), set(ACCOUNT_METRIC_FIELDS))
        self.assertEqual(result["metrics"]["follower_count"], 0)
        self.assertEqual(result["metrics"]["work_view_daily_increment"], -7)
        self.assertEqual(result["metrics"]["work_like_daily_increment"], -2)
        self.assertEqual(result["metrics"]["platform_work_count"], 321)
        self.assertEqual(result["metrics"]["total_likes"], 456)
        self.assertEqual(result["field_status"]["collect_daily_increment"]["status"], "not_requested")
        self.assertEqual(result["field_status"]["total_likes_and_collects"]["status"], "not_applicable")

    def test_account_platform_semantics_and_unverified_fields(self):
        result = normalize_account(account(platType=6), platform="xiaohongshu", rank_date="2026-08-27")
        self.assertEqual(result["metrics"]["total_likes_and_collects"], 456)
        self.assertIsNone(result["metrics"]["total_likes"])
        self.assertIsNone(result["metrics"]["work_view_daily_increment"])
        self.assertEqual(result["field_status"]["total_likes"]["status"], "not_applicable")
        self.assertEqual(result["field_status"]["work_view_daily_increment"]["status"], "not_applicable")
        self.assertEqual(result["field_status"]["collect_daily_increment"]["status"], "not_requested")

    def test_account_returned_rankdate_must_match_requested_day(self):
        for value in (None, "", "2026-08-26", "2026-08-27T00:00:00Z", "invalid"):
            with self.subTest(value=value):
                result = normalize_account(account(rankDate=value), platform="douyin", rank_date="2026-08-27")
                self.assertNotEqual(result["field_status"]["statistics_date"]["status"], "provided")
                self.assertIsNone(result["metrics"]["follower_count"])
                self.assertEqual(result["field_status"]["follower_count"]["status"], "invalid")
        result = normalize_account(account(rankDate="2026-08-26"), platform="douyin", rank_date="2026-08-27")
        self.assertEqual(result["statistics_date"], "2026-08-26")
        self.assertEqual(result["requested_statistics_date"], "2026-08-27")

    def test_account_cumulative_values_cannot_be_negative(self):
        result = normalize_account(
            account(cTotalFans=-1, awemeCount=-1, totalFavorited=-1),
            platform="douyin", rank_date="2026-08-27",
        )
        for field in ("follower_count", "platform_work_count", "total_likes"):
            self.assertIsNone(result["metrics"][field])
            self.assertEqual(result["field_status"][field]["status"], "invalid")
        result = normalize_account(account(), platform="douyin", rank_date="2026-08-27", requested_fields={"follower_count"})
        self.assertEqual(result["field_status"]["platform_work_count"]["status"], "not_requested")
        self.assertEqual(result["metrics"]["follower_count"], 123)


if __name__ == "__main__":
    unittest.main()
