import unittest

from v8.operation_contracts import OperationContractError, require_response


class OperationContractTest(unittest.TestCase):
    def check(self, operation, payload, *, subject="1", params=None, cursor=None, account_uid="1"):
        return require_response(operation, {"provider": "tikhub", "operation": operation,
            "subject": subject, "request_parameters": params or {}, "cursor": cursor}, payload, account_uid=account_uid)

    def test_statistics_requires_every_frozen_member(self):
        op = "douyin_video_statistics"
        valid = {"code": 200, "data": {"statistics_list": [
            {"aweme_id": "2", "play_count": 0}, {"aweme_id": "1", "play_count": 3}]}}
        self.check(op, valid, subject="1,2", params={"aweme_ids": "1,2"})
        for rows in ([{"aweme_id": "1", "play_count": 1}],
                     [{"aweme_id": "1"}, {"aweme_id": "2", "play_count": 1}],
                     [{"aweme_id": "1", "play_count": 1}] * 2,
                     [{"aweme_id": "1", "play_count": "invalid"}]):
            with self.subTest(rows=rows), self.assertRaises(OperationContractError):
                self.check(op, {"code": 200, "data": {"statistics_list": rows}}, subject="1,2")

    def test_discovery_accepts_empty_terminal_page_but_rejects_invalid_members(self):
        for op, key in (("douyin_user_posts", "aweme_list"), ("xiaohongshu_user_posts", "notes")):
            self.check(op, {"code": 200, "data": {key: [], "has_more": False}} if key == "aweme_list"
                else {"code": 200, "data": {"code": 0, "data": {key: [], "has_more": False}}})
        bad = ({"has_more": False}, {"aweme_list": [{}], "has_more": False},
               {"aweme_list": [{"aweme_id": "1"}], "has_more": False},
               {"aweme_list": [], "has_more": True, "max_cursor": 2})
        for value in bad:
            with self.subTest(value=value), self.assertRaises(OperationContractError):
                self.check("douyin_user_posts", {"code": 200, "data": value})
        page = {"aweme_list": [{"aweme_id": "1", "create_time": 1700000000, "author": {"uid": "1"}}], "has_more": True, "max_cursor": 2}
        self.check("douyin_user_posts", {"code": 200, "data": page}, cursor=1)
        with self.assertRaises(OperationContractError):
            self.check("douyin_user_posts", {"code": 200, "data": page}, cursor=2)
        with self.assertRaises(OperationContractError):
            self.check("douyin_user_posts", {"code": 200, "data": page}, cursor=1, account_uid="2")
        for cursor in ([], {}, True, -1, "1"):
            with self.subTest(cursor=cursor), self.assertRaises(OperationContractError):
                self.check("douyin_user_posts", {"code": 200, "data": {**page, "max_cursor": cursor}}, cursor=1)

    def test_provider_error_and_raw_replay_shapes_cannot_prove_recovery(self):
        for payload in ({"code": 500, "notes": [], "has_more": False}, {"notes": [], "has_more": False}):
            with self.assertRaises(OperationContractError):
                self.check("xiaohongshu_user_posts", payload)

    def test_invalid_identity_shapes_return_contract_error(self):
        good = {"provider": "tikhub", "subject": "1", "operation": "douyin_video_detail"}
        for identity in (None, [], {**good, "provider": None}, {**good, "request_parameters": []}):
            with self.subTest(identity=identity), self.assertRaises(OperationContractError):
                require_response("douyin_video_detail", identity, {"code": 200, "data": {}})

    def test_legacy_normalized_shapes_are_not_provider_recovery_proofs(self):
        for op, payload in (("douyin_video_detail", {"stage": "detail", "data": {}}),
                            ("xiaohongshu_note_statistics", {"metrics": {}})):
            with self.assertRaises(OperationContractError):
                self.check(op, payload)

    def test_detail_requires_requested_content(self):
        self.check("douyin_video_detail", {"code": 200, "data": {"aweme_id": "1", "desc": "one"}})
        with self.assertRaises(OperationContractError):
            self.check("douyin_video_detail", {"code": 200, "data": {"aweme_id": "2"}})

    def test_profile_requires_exact_uid_followers_and_reference(self):
        payload = {"code": 200, "data": {"status_code": 0, "data": {
            "id_str": "1", "sec_uid": "MS4wLjAB" + "A" * 68, "follow_info": {"follower_count": 0}}}}
        self.check("douyin_uid_profile", payload, params={"uid": "1"})
        with self.assertRaises(OperationContractError):
            self.check("douyin_uid_profile", payload, params={"uid": "2"})
        payload["data"]["data"]["follow_info"] = {}
        with self.assertRaises(OperationContractError):
            self.check("douyin_uid_profile", payload)
