"""Offline verification against the pinned official decoder and bounded media IO."""
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from v8 import media
from v8 import storage
from v8.wechat_video_crypto import PREFIX_SIZE, WeChatDecryptionError, decrypt_spool, keystream, media_material


class WeChatVideoCryptoTest(unittest.TestCase):
    def test_official_decoder_fixed_vector_and_exact_uint64_input(self):
        stream = keystream("2136343393")
        self.assertEqual(len(stream), PREFIX_SIZE)
        self.assertEqual(hashlib.sha256(stream).hexdigest(),
            "49b96d6fc75ba5215fbb773ce98f6b20f6441a7ac40abc9582b1e42c5f3cd9d8")
        for key in (2136343393, "1.2", "-1", "18446744073709551616", "1; echo secret"):
            with self.subTest(key=key), self.assertRaises(WeChatDecryptionError):
                keystream(key)

    def test_same_response_url_token_key_and_video_kind_are_mandatory(self):
        asset = {"url": "https://finder.video.qq.com/v", "url_token": "?token=abc+//==",
            "full_url": "https://finder.video.qq.com/v?token=abc+//==", "decode_key": "2136343393"}
        value = {"platform": "wechat_channels", "content_type": "video", "media_evidence": [asset]}
        self.assertEqual(media_material(value)["url"], asset["full_url"])
        for override in ({"full_url": "https://finder.video.qq.com/other"}, {"decode_key": None},
                         {"decode_key": 2136343393}, {"url_token": "?different"}):
            with self.subTest(override=override), self.assertRaises(WeChatDecryptionError):
                media_material({**value, "media_evidence": [{**asset, **override}]})
        for override in ({"content_type": "image"}, {"media_evidence": [asset, asset]}):
            with self.assertRaises(WeChatDecryptionError):
                media_material({**value, **override})

    def test_invalid_or_truncated_ciphertext_never_modifies_private_spool(self):
        for cipher in (b"short", b"not a video" * 20000):
            with tempfile.TemporaryFile("w+b") as handle:
                handle.write(cipher)
                with patch("v8.wechat_video_crypto.keystream", return_value=bytes(PREFIX_SIZE)):
                    with self.assertRaises(WeChatDecryptionError):
                        decrypt_spool(handle, "1")
                handle.seek(0)
                self.assertEqual(handle.read(), cipher)

    def test_only_encrypted_prefix_changes_and_receipt_has_no_secret(self):
        plain = b"\x00\x00\x00\x18ftypisom" + b"x" * (PREFIX_SIZE + 256)
        stream = keystream("2136343393")
        encrypted = bytes(a ^ b for a, b in zip(plain[:PREFIX_SIZE], stream)) + plain[PREFIX_SIZE:]
        with tempfile.TemporaryFile("w+b") as handle:
            handle.write(encrypted)
            receipt = decrypt_spool(handle, "2136343393")
            self.assertEqual(handle.read(), plain)
            self.assertEqual(receipt["encrypted_sha256"], hashlib.sha256(encrypted).hexdigest())
            self.assertEqual(receipt["sha256"], hashlib.sha256(plain).hexdigest())
            self.assertNotIn("decode_key", receipt)
            self.assertNotIn("2136343393", json.dumps({k: v for k, v in receipt.items() if k != "header"}))

    @unittest.skipUnless(shutil.which("ffmpeg"), "ffmpeg is required for publication validation")
    def test_encrypted_download_is_fully_decoded_before_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original = root / "original.mp4"
            target = root / "decrypted.mp4"
            subprocess.run(["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-f", "lavfi", "-i",
                "color=c=black:s=320x240:d=1", "-c:v", "libx264", str(original)], check=True, timeout=20)
            plain = original.read_bytes()
            stream = keystream("2136343393")
            encrypted = bytes(a ^ b for a, b in zip(plain[:PREFIX_SIZE], stream)) + plain[PREFIX_SIZE:]
            url = "https://finder.video.qq.com/encrypted"
            class Response(io.BytesIO):
                headers = {"Content-Length": str(len(encrypted))}
                def geturl(self): return url
            receipt = {}
            media._download_video([url], target, urlopen_fn=lambda *_args, **_kwargs: Response(encrypted),
                decryption_material={"url": url, "decode_key": "2136343393"}, decryption_receipt=receipt)
            self.assertEqual(target.read_bytes(), plain)
            self.assertEqual(receipt["sha256"], hashlib.sha256(plain).hexdigest())
            wrong = root / "wrong.mp4"
            with self.assertRaises(WeChatDecryptionError):
                media._download_video([url], wrong, urlopen_fn=lambda *_args, **_kwargs: Response(encrypted),
                    decryption_material={"url": url, "decode_key": "1"})
            self.assertFalse(wrong.exists())
            self.assertFalse(any(p.name.endswith("candidate-0") for p in root.iterdir()))

    def test_source_raw_binding_download_receipt_cache_and_recovery_share_one_plaintext(self):
        from v8.wechat_channels_adapter import parse_stage
        from v8.capture import RawResponseIntegrityError
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve(); db = root / "fixture.db"
            original = root / "original.mp4"
            subprocess.run(["ffmpeg", "-y", "-nostdin", "-loglevel", "error", "-f", "lavfi", "-i", "color=c=black:s=320x240:d=1", "-c:v", "libx264", str(original)], check=True, timeout=20)
            plain = original.read_bytes(); stream = keystream("2136343393")
            encrypted = bytes(a ^ b for a, b in zip(plain[:PREFIX_SIZE], stream)) + plain[PREFIX_SIZE:]
            uid, pid, url = "v2_0011aabb@finder", "1234567890", "https://finder.video.qq.com/test?token=a+//=="
            payload = {"code": 200, "data": {"id": pid, "username": uid, "object_desc": {"description": "fixture", "media": [{"media_type": 4, "url": "https://finder.video.qq.com/test", "url_token": "?token=a+//==", "decode_key": "2136343393"}]}}}
            raw_path = root / "response.json"; raw_bytes = json.dumps(payload).encode(); raw_path.write_bytes(raw_bytes); raw_path.chmod(0o600)
            with storage.connect(db) as c:
                storage.initialize_database(c, target_version=19)
                at = storage.now_utc()
                c.execute("INSERT INTO content_items(id,link_id,platform,platform_content_id,canonical_url,title,content_type,raw_account_uid,created_at,updated_at,imported_at) VALUES(1,'A2BC3D','wechat_channels',?,'','fixture','video',?,?,?,?)", (pid, uid, at, at, at))
                raw_id = c.execute("INSERT INTO provider_raw_responses(content_id,provider,operation,local_path,sha256,byte_size,http_status,captured_at) VALUES(1,'TikHub','wechat_channels_video_detail',?,?,?,200,?)", (str(raw_path), hashlib.sha256(raw_bytes).hexdigest(), len(raw_bytes), at)).lastrowid
            class Response(io.BytesIO):
                headers = {"Content-Length": str(len(encrypted))}
                def geturl(self): return url
            with patch.object(media, "MEDIA_ROOT", root / "media"):
                source = media.store_media_source_from_detail(1, parse_stage("detail", pid, payload, expected_uid=uid), raw_response_id=raw_id, db_path=db)
                self.assertIsNotNone(source)
                artifact = media.download_video_sources(1, [url], db_path=db, urlopen_fn=lambda *_a, **_k: Response(encrypted))
                again = media.download_video_sources(1, [url], db_path=db, urlopen_fn=lambda *_a, **_k: self.fail("cached plaintext must not download"))
                self.assertEqual(artifact.id, again.id)
                self.assertEqual(Path(artifact.local_path).read_bytes(), plain)
                with storage.connect(db) as c:
                    proof = json.loads(c.execute("SELECT metadata_json FROM evidence_artifacts WHERE id=?", (artifact.id,)).fetchone()[0])
                    self.assertEqual(proof["decryption"]["source_raw_response_id"], raw_id)
                    self.assertNotIn("decode_key", json.dumps(proof))
                    self.assertIsNotNone(media._current_recovery_download(c, content_id=1, require_succeeded=True))
                raw_path.write_bytes(b"{}")
                with self.assertRaises(RawResponseIntegrityError):
                    media.download_video_sources(1, [url], db_path=db, urlopen_fn=lambda *_a, **_k: self.fail("bad raw must not download"))


if __name__ == "__main__":
    unittest.main()
