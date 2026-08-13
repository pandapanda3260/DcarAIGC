from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from workflow.privacy import CommentHasher


class CommentHasherTest(unittest.TestCase):
    def test_hmac_is_stable_but_scoped_to_platform_and_content(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "salt"
            first = CommentHasher(path)
            second = CommentHasher(path)
            key = first.user_key("douyin", "content-a", "raw-user")
            self.assertEqual(key, second.user_key("douyin", "content-a", "raw-user"))
            self.assertNotEqual(key, first.user_key("douyin", "content-b", "raw-user"))
            self.assertNotEqual(key, first.user_key("xiaohongshu", "content-a", "raw-user"))
            self.assertNotIn("raw-user", key)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_read_only_replica_accepts_preprovisioned_group_read_salt(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "salt"
            path.write_bytes(b"s" * 32)
            path.chmod(0o640)
            with patch.dict(os.environ, {"DCAR_READ_ONLY": "1"}):
                CommentHasher(path)
            self.assertEqual(os.stat(path).st_mode & 0o777, 0o640)


if __name__ == "__main__":
    unittest.main()
