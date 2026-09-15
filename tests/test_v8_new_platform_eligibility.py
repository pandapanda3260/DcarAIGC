"""New-platform qualification must isolate malformed evidence per account."""
import unittest

from tests import test_v8_account_capture_eligibility as helpers


class NewPlatformEligibilityTests(unittest.TestCase):
    setUp = helpers.AccountCaptureEligibilityTest.setUp
    add = helpers.AccountCaptureEligibilityTest.add
    raw = helpers.AccountCaptureEligibilityTest.raw
    read = helpers.AccountCaptureEligibilityTest.read
    reason = helpers.AccountCaptureEligibilityTest.reason

    def profile(self, platform, uid):
        if platform == "kuaishou":
            return {"code": 200, "router": "/api/v1/kuaishou/app/fetch_one_user_v2",
                    "params": {"user_id": uid}, "data": {"result": 1,
                    "userProfile": {"profile": {"user_id": uid, "user_name": "测试账号"}}}}
        return {"code": 200, "router": "/api/v1/wechat_channels/v2/fetch_user_profile",
                "params": {"username": uid}, "data": {"baseResponse": {"ret": 0},
                "contact": {"username": uid, "nickname": "测试账号"}}}

    def add_platform(self, platform, *, account_id=1, uid=None, status="daily", payload=None, evidence=True):
        uid = uid or ("0012345678" if platform == "kuaishou" else "v2_00aabbcc@finder")
        self.add(account_id, platform=platform, uid=uid, status=status, identity_status="uid_unverified")
        if evidence:
            kind = "kuaishou_user_id" if platform == "kuaishou" else "finder_username"
            self.connection.execute("INSERT INTO account_provider_references VALUES(?,'TikHub',?,?,?)", (account_id, kind, uid, account_id))
            self.raw(account_id, payload if payload is not None else self.profile(platform, uid), operation=platform + "_user_profile")
        return uid

    def test_profiles_admit_both_platforms_with_paused_labels_and_missing_fans(self):
        self.add_platform("kuaishou", account_id=1, status="paused")
        self.add_platform("wechat_channels", account_id=2, status="paused")
        result = self.read()
        self.assertEqual(len(result["eligible_members"]), 2)
        self.assertEqual(result["excluded_members"], [])
        self.assertTrue(all(row["account_status"] == "paused" for row in result["eligible_members"]))

    def test_uid_without_full_profile_does_not_admit(self):
        self.add_platform("kuaishou", account_id=1, evidence=False)
        self.add_platform("wechat_channels", account_id=2, evidence=False)
        result = self.read()
        self.assertEqual(result["eligible_members"], [])
        self.assertEqual([row["reason_code"] for row in result["excluded_members"]], ["identity_evidence_missing"] * 2)

    def test_bad_profile_hash_rejects_only_that_account(self):
        self.add_platform("kuaishou", account_id=1)
        self.add_platform("wechat_channels", account_id=2)
        (self.root / "1.json").write_text('{}')
        result = self.read()
        self.assertEqual([row["account_id"] for row in result["eligible_members"]], [2])
        self.assertEqual(result["excluded_members"][0]["reason_code"], "reference_evidence_unavailable")

    def test_wrong_response_uid_does_not_crash_entire_directory(self):
        self.add_platform("kuaishou", account_id=1,
                          payload=self.profile("kuaishou", "9912345678"))
        self.add_platform("wechat_channels", account_id=2)
        result = self.read()
        self.assertEqual([row["account_id"] for row in result["eligible_members"]], [2])
        self.assertEqual(result["excluded_members"][0]["reason_code"], "reference_identity_mismatch")

    def test_inner_wechat_blocked_does_not_crash_other_accounts(self):
        self.add_platform("kuaishou", account_id=1)
        blocked = self.profile("wechat_channels", "v2_00aabbcc@finder")
        blocked["data"] = {"ret": 100001, "msg": "Blocked"}
        self.add_platform("wechat_channels", account_id=2, payload=blocked)
        result = self.read()
        self.assertEqual([row["account_id"] for row in result["eligible_members"]], [1])
        self.assertEqual(result["excluded_members"][0]["reason_code"], "reference_identity_mismatch")

    def test_wechat_public_id_is_not_a_finder_uid(self):
        self.add_platform("wechat_channels", uid="sphPublicID", evidence=False)
        self.assertEqual(self.reason(), "identity_conflict")

    def test_wechat_profile_request_binding_must_match(self):
        payload = self.profile("wechat_channels", "v2_00aabbcc@finder")
        payload["params"]["username"] = "v2_00aabbdd@finder"
        self.add_platform("wechat_channels", payload=payload)
        self.assertEqual(self.reason(), "reference_identity_mismatch")

    def test_wechat_non_profile_route_cannot_be_used_as_profile_proof(self):
        payload = self.profile("wechat_channels", "v2_00aabbcc@finder")
        payload["router"] = "/api/v1/wechat_channels/v2/fetch_user_videos"
        self.add_platform("wechat_channels", payload=payload)
        self.assertEqual(self.reason(), "reference_identity_mismatch")


if __name__ == "__main__":
    unittest.main()
