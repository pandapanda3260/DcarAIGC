"""Repeated immutable evidence checks remain strict without rereading bytes."""
from __future__ import annotations

import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import v20_release_contract as contract  # noqa: E402


class EvidenceHashCacheTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name).resolve() / "sealed-evidence.json"
        self.path.write_bytes(b'{"value":1}\n')
        self.path.chmod(0o600)
        self.reference = {"path": str(self.path), "sha256": hashlib.sha256(self.path.read_bytes()).hexdigest()}
        contract._evidence_file_digest.cache_clear()
        self.addCleanup(contract._evidence_file_digest.cache_clear)

    def test_unchanged_bytes_read_once_but_each_requested_hash_checked(self) -> None:
        self.assertEqual(contract.verified_reference(self.reference), self.reference)
        with patch.object(contract.os, "open", side_effect=AssertionError("cached file reread")):
            self.assertEqual(contract.verified_reference(self.reference), self.reference)
            with self.assertRaisesRegex(contract.ReleaseContractError, "SHA-256 differs"):
                contract.verified_reference({**self.reference, "sha256": "0" * 64})
        self.assertEqual(contract._evidence_file_digest.cache_info().currsize, 1)
        self.assertEqual(contract._evidence_file_digest.cache_info().maxsize, 256)

    def test_same_size_rewrite_with_restored_mtime_invalidates(self) -> None:
        contract.verified_reference(self.reference)
        previous = self.path.stat()
        self.path.write_bytes(b'{"value":2}\n')
        os.utime(self.path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        with self.assertRaisesRegex(contract.ReleaseContractError, "SHA-256 differs"):
            contract.verified_reference(self.reference)

    def test_atomic_replacement_with_same_size_mtime_invalidates(self) -> None:
        contract.verified_reference(self.reference)
        previous = self.path.stat()
        replacement = self.path.with_suffix(".replacement")
        replacement.write_bytes(b'{"value":2}\n')
        replacement.chmod(0o600)
        os.utime(replacement, ns=(previous.st_atime_ns, previous.st_mtime_ns))
        replacement.replace(self.path)
        with self.assertRaisesRegex(contract.ReleaseContractError, "SHA-256 differs"):
            contract.verified_reference(self.reference)

    def test_cached_file_cannot_be_replaced_by_symlink_or_hardlink(self) -> None:
        contract.verified_reference(self.reference)
        original = self.path.with_suffix(".original")
        self.path.rename(original)
        self.path.symlink_to(original)
        with self.assertRaises(contract.ReleaseContractError):
            contract.verified_reference(self.reference)
        self.path.unlink()
        os.link(original, self.path)
        with self.assertRaises(contract.ReleaseContractError):
            contract.verified_reference(self.reference)

    def test_writeable_evidence_is_not_cached(self) -> None:
        self.path.chmod(0o666)
        contract.verified_reference(self.reference)
        contract.verified_reference(self.reference)
        self.assertEqual(contract._evidence_file_digest.cache_info().currsize, 0)

    def test_change_during_first_read_is_refused_and_not_cached(self) -> None:
        real_hash = hashlib.sha256
        path = self.path

        class MutatingHash:
            def __init__(self) -> None:
                self.inner = real_hash()

            def update(self, value: bytes) -> None:
                self.inner.update(value)
                path.write_bytes(b'{"value":2}\n')

            def hexdigest(self) -> str:
                return self.inner.hexdigest()

        with patch.object(contract.hashlib, "sha256", MutatingHash):
            with self.assertRaisesRegex(contract.ReleaseContractError, "changed while hashing"):
                contract.verified_reference(self.reference)
        self.assertEqual(contract._evidence_file_digest.cache_info().currsize, 0)

    def test_change_on_cache_hit_is_refused_by_final_identity_check(self) -> None:
        contract.verified_reference(self.reference)
        reader = contract._evidence_file_digest

        def changed_after_hit(name: str, version: tuple[int, ...]) -> str:
            result = reader(name, version)
            self.path.write_bytes(b'{"value":2}\n')
            return result

        with patch.object(contract, "_evidence_file_digest", changed_after_hit):
            with self.assertRaisesRegex(contract.ReleaseContractError, "changed during verification"):
                contract.verified_reference(self.reference)


if __name__ == "__main__":
    unittest.main()
