#!/usr/bin/env python3

import unittest
import subprocess
import sys
import tempfile
from pathlib import Path

import collect_tikhub_douyin_enrichment_v6 as v6


class TikHubDouyinEnrichmentV6Test(unittest.TestCase):
    def test_import_is_read_only_and_concurrent_first_hash_keeps_one_salt(self) -> None:
        source = Path(v6.__file__).parent
        script = '''
import concurrent.futures
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from workflow import contracts
contracts.PROJECT_ROOT = Path(sys.argv[2])
import collect_tikhub_douyin_enrichment_v6 as module
salt = contracts.PROJECT_ROOT / 'data/cache/.comment_hash_salt'
assert not salt.exists(), 'import created a persistent salt'
assert module.anon_user_key('999', {}) == ''
assert not salt.exists(), 'empty identity created a persistent salt'
with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
    keys = list(pool.map(lambda _: module.anon_user_key('999', {'uid': '123'}), range(32)))
assert len(set(keys)) == 1 and keys[0].startswith('U')
assert salt.is_file() and len(salt.read_bytes()) == 32
'''
        with tempfile.TemporaryDirectory() as temporary:
            subprocess.run([sys.executable, "-I", "-B", "-c", script, str(source), temporary], check=True)

    def test_historical_network_entry_is_retired(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "v8 writer capture path"):
            v6.api_call(v6.STATS_ENDPOINT, {"aweme_ids": "1"}, "fixture-key")

    def test_sanitizer_retains_no_raw_identity(self) -> None:
        payload = {
            "data": {
                "cursor": 20,
                "has_more": True,
                "total": 30,
                "comments": [{
                    "text": "我的秦L高速油耗5.2",
                    "level": 1,
                    "digg_count": 3,
                    "user": {"uid": "123", "sec_uid": "MS4w", "nickname": "张三"},
                }],
            }
        }
        page = v6.sanitize_comment_page(
            aweme_id="999", author_uid="456", cursor_requested=0, payload=payload
        )
        encoded = str(page)
        self.assertNotIn("张三", encoded)
        self.assertNotIn("MS4w", encoded)
        self.assertNotIn("'123'", encoded)
        self.assertTrue(page["comments"][0]["user_key"].startswith("U"))

    def test_author_spam_and_empty_are_not_valid(self) -> None:
        pages = [{"comments": [
            {"user_key": "U1", "is_author": True, "text": "作者回复"},
            {"user_key": "U2", "is_author": False, "text": "加微信进群"},
            {"user_key": "U3", "is_author": False, "text": "😂😂"},
            {"user_key": "U4", "is_author": False, "text": "落地多少钱"},
            {"user_key": "U4", "is_author": False, "text": "哪个配置"},
        ]}]
        users = v6.valid_unique_comments(pages)
        self.assertEqual(users, {"U4": "落地多少钱；哪个配置"})

    def test_chunking_is_two_per_statistics_call(self) -> None:
        self.assertEqual(list(v6.chunks(["1", "2", "3"], 2)), [["1", "2"], ["3"]])


if __name__ == "__main__":
    unittest.main()
