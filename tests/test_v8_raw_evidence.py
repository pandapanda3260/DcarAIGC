from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from v8 import raw_evidence


class RawEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.paid_identity = hashlib.sha256(b"paid-scope").hexdigest()
        self.response_identity = hashlib.sha256(b"response-sequence-0").hexdigest()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write(
        self, entity: bytes = b'{"items":[1,2,3]}\n'
    ) -> raw_evidence.RawEvidenceReceipt:
        return raw_evidence.write_zstd_raw_evidence(
            self.root / "response.json.zst",
            entity,
            provider="TikHub",
            operation="douyin_user_posts",
            response_identity=self.response_identity,
            paid_scope_identity=self.paid_identity,
            sequence=0,
            evidence_root=self.root,
        )

    def test_zstd_round_trip_records_entity_and_stored_receipts(self) -> None:
        entity = b'{"items":[' + (b'"same-value",' * 1000) + b"null]}\n"

        receipt = self.write(entity)
        loaded = raw_evidence.read_raw_evidence(
            receipt.path,
            expected_stored_sha256=receipt.stored_sha256,
            expected_stored_size=receipt.stored_size,
        )

        self.assertEqual(loaded.entity_bytes, entity)
        self.assertEqual(receipt.entity_sha256, hashlib.sha256(entity).hexdigest())
        self.assertEqual(receipt.entity_size, len(entity))
        self.assertEqual(
            receipt.stored_sha256,
            hashlib.sha256(receipt.path.read_bytes()).hexdigest(),
        )
        self.assertEqual(receipt.stored_size, receipt.path.stat().st_size)
        self.assertLess(receipt.stored_size, receipt.entity_size)
        self.assertEqual(stat.S_IMODE(receipt.path.stat().st_mode), 0o600)
        assert receipt.sidecar_path is not None
        self.assertEqual(stat.S_IMODE(receipt.sidecar_path.stat().st_mode), 0o600)
        sidecar = json.loads(receipt.sidecar_path.read_text())
        self.assertEqual(sidecar["zstd_level"], 3)
        self.assertEqual(sidecar["entity_sha256"], receipt.entity_sha256)
        self.assertEqual(sidecar["stored_sha256"], receipt.stored_sha256)
        self.assertEqual(raw_evidence.read_raw_json(receipt.path)["items"][-1], None)

    def test_zstd_storage_overhead_does_not_consume_the_entity_limit(self) -> None:
        receipt = self.write(b"{}")
        self.assertGreater(receipt.stored_size, receipt.entity_size)
        self.assertGreater(raw_evidence.MAX_STORED_BYTES, raw_evidence.MAX_RAW_BYTES)

        with patch.object(raw_evidence, "MAX_RAW_BYTES", receipt.entity_size):
            loaded = raw_evidence.read_raw_evidence(receipt.path)

        self.assertEqual(loaded.entity_bytes, b"{}")

    def test_exact_replay_is_idempotent_but_different_bytes_conflict(self) -> None:
        first = self.write()
        raw_identity = first.path.stat().st_ino
        assert first.sidecar_path is not None
        sidecar_identity = first.sidecar_path.stat().st_ino

        second = self.write()

        self.assertEqual(second, first)
        self.assertEqual(second.path.stat().st_ino, raw_identity)
        assert second.sidecar_path is not None
        self.assertEqual(second.sidecar_path.stat().st_ino, sidecar_identity)
        with self.assertRaises(raw_evidence.RawEvidenceConflict):
            self.write(b'{"items":[4]}\n')

    def test_one_sided_raw_artifact_is_a_hard_conflict(self) -> None:
        receipt = self.write()
        assert receipt.sidecar_path is not None
        receipt.sidecar_path.unlink()

        with self.assertRaisesRegex(
            raw_evidence.RawEvidenceConflict,
            "incomplete",
        ):
            self.write()

    def test_legacy_json_uses_safe_identity_receipt(self) -> None:
        path = self.root / "legacy.json"
        entity = b'{"legacy":true}\n'
        path.write_bytes(entity)
        path.chmod(0o600)

        loaded = raw_evidence.read_raw_evidence(
            path,
            expected_stored_sha256=hashlib.sha256(entity).hexdigest(),
            expected_stored_size=len(entity),
        )

        self.assertEqual(loaded.entity_bytes, entity)
        self.assertEqual(loaded.receipt.codec, "identity")
        self.assertIsNone(loaded.receipt.sidecar_path)
        self.assertEqual(raw_evidence.read_raw_json(path), {"legacy": True})

    def test_legacy_json_accepts_historical_0644_but_rejects_shared_write(self) -> None:
        path = self.root / "historical.json"
        entity = b'{"legacy":true}\n'
        path.write_bytes(entity)
        path.chmod(0o644)

        self.assertEqual(raw_evidence.read_raw_json(path), {"legacy": True})
        path.chmod(0o664)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.read_raw_json(path)

    def test_corrupt_stored_bytes_or_sidecar_are_rejected(self) -> None:
        receipt = self.write()
        receipt.path.write_bytes(receipt.path.read_bytes() + b"corrupt")
        receipt.path.chmod(0o600)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.read_raw_evidence(receipt.path)

        other = self.root / "other.json.zst"
        other_response = hashlib.sha256(b"response-sequence-1").hexdigest()
        other_receipt = raw_evidence.write_zstd_raw_evidence(
            other,
            b'{"ok":true}\n',
            provider="TikHub",
            operation="douyin_user_posts",
            response_identity=other_response,
            paid_scope_identity=self.paid_identity,
            sequence=1,
        )
        assert other_receipt.sidecar_path is not None
        sidecar = json.loads(other_receipt.sidecar_path.read_text())
        sidecar["entity_size"] += 1
        other_receipt.sidecar_path.write_bytes(
            raw_evidence.canonical_json_bytes(sidecar)
        )
        other_receipt.sidecar_path.chmod(0o600)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.read_raw_evidence(other_receipt.path)

    def test_safe_reader_rejects_symlinks_and_hardlinks(self) -> None:
        receipt = self.write()
        symlink = self.root / "alias.json.zst"
        symlink.symlink_to(receipt.path)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.read_raw_evidence(symlink)

        hardlink = self.root / "hardlink.json.zst"
        os.link(receipt.path, hardlink)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.read_raw_evidence(receipt.path)

    def test_publish_fsyncs_file_before_rename_and_directory_after(self) -> None:
        events: list[str] = []
        real_fsync = raw_evidence.os.fsync
        real_replace = raw_evidence.os.replace

        def observed_fsync(descriptor: int) -> None:
            metadata = os.fstat(descriptor)
            events.append(
                "directory-fsync" if stat.S_ISDIR(metadata.st_mode) else "file-fsync"
            )
            real_fsync(descriptor)

        def observed_replace(source: Path, destination: Path) -> None:
            events.append("rename")
            real_replace(source, destination)

        with (
            patch.object(raw_evidence.os, "fsync", side_effect=observed_fsync),
            patch.object(raw_evidence.os, "replace", side_effect=observed_replace),
        ):
            self.write()

        self.assertEqual(events[:3], ["file-fsync", "rename", "directory-fsync"])
        self.assertEqual(events[3:6], ["file-fsync", "rename", "directory-fsync"])

    def test_paid_send_claim_has_exactly_one_concurrent_winner(self) -> None:
        def claim() -> raw_evidence.PaidSendClaim | str:
            try:
                return raw_evidence.claim_paid_send(
                    self.root / "claims",
                    paid_scope_identity=self.paid_identity,
                    sequence=0,
                    claim={"operation": "douyin_user_posts", "slot_id": 42},
                )
            except raw_evidence.PaidSendClaimHeld:
                return "held"

        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: claim(), range(16)))

        winners = [result for result in results if result != "held"]
        self.assertEqual(len(winners), 1)
        winner = winners[0]
        self.assertIsInstance(winner, raw_evidence.PaidSendClaim)
        assert isinstance(winner, raw_evidence.PaidSendClaim)
        self.assertEqual(stat.S_IMODE(winner.path.stat().st_mode), 0o600)
        self.assertEqual(
            winner.sha256, hashlib.sha256(winner.path.read_bytes()).hexdigest()
        )
        with self.assertRaises(raw_evidence.PaidSendClaimHeld):
            raw_evidence.claim_paid_send(
                self.root / "claims",
                paid_scope_identity=self.paid_identity,
                sequence=0,
                claim={"operation": "douyin_user_posts", "slot_id": 42},
            )

    def test_incomplete_claim_is_retained_as_paid_identity_hold(self) -> None:
        def partial_write(descriptor: int, value: bytes) -> None:
            os.write(descriptor, value[:1])
            raise OSError("simulated crash after claim creation")

        with patch.object(raw_evidence, "_write_all", side_effect=partial_write):
            with self.assertRaisesRegex(OSError, "simulated crash"):
                raw_evidence.claim_paid_send(
                    self.root / "claims",
                    paid_scope_identity=self.paid_identity,
                    sequence=7,
                    claim={"operation": "douyin_user_posts"},
                )

        claim_path = (
            self.root
            / "claims"
            / self.paid_identity[:2]
            / f"{self.paid_identity}.sequence-00000007.claim.json"
        )
        self.assertEqual(claim_path.read_bytes(), b"{")
        with self.assertRaises(raw_evidence.PaidSendClaimHeld):
            raw_evidence.claim_paid_send(
                self.root / "claims",
                paid_scope_identity=self.paid_identity,
                sequence=7,
                claim={"operation": "douyin_user_posts"},
            )

    def test_quarantine_is_immutable_and_idempotent(self) -> None:
        path = self.root / "quarantine" / "scope.partial"
        first = raw_evidence.write_quarantine_evidence(path, b"partial")
        second = raw_evidence.write_quarantine_evidence(path, b"partial")
        self.assertEqual(first, second)
        self.assertEqual(first.sha256, hashlib.sha256(b"partial").hexdigest())
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        with self.assertRaises(raw_evidence.RawEvidenceConflict):
            raw_evidence.write_quarantine_evidence(path, b"different")

    def test_provider_components_and_ancestor_symlinks_cannot_escape_root(self) -> None:
        outside = self.root / "outside"
        outside.mkdir()
        evidence = self.root / "evidence"

        for field, provider, operation in (
            ("provider", "../../../outside", "douyin_user_posts"),
            ("operation", "TikHub", "../../../outside"),
        ):
            with (
                self.subTest(field=field),
                self.assertRaises(ValueError),
            ):
                raw_evidence.write_zstd_raw_evidence(
                    evidence / "safe" / "response.json.zst",
                    b'{"ok":true}',
                    provider=provider,
                    operation=operation,
                    response_identity=self.response_identity,
                    paid_scope_identity=self.paid_identity,
                    sequence=0,
                    evidence_root=evidence,
                )

        evidence.mkdir(mode=0o700)
        (evidence / "TikHub").symlink_to(outside, target_is_directory=True)
        with self.assertRaises(raw_evidence.RawEvidenceError):
            raw_evidence.write_zstd_raw_evidence(
                evidence / "TikHub" / "response.json.zst",
                b'{"ok":true}',
                provider="TikHub",
                operation="douyin_user_posts",
                response_identity=self.response_identity,
                paid_scope_identity=self.paid_identity,
                sequence=0,
                evidence_root=evidence,
            )
        self.assertEqual(list(outside.iterdir()), [])

    def test_quarantine_receipt_is_immutable_and_confined(self) -> None:
        receipt = raw_evidence.write_immutable_json_receipt(
            self.root / "quarantine" / "scope.partial.receipt.json",
            {"status": "failed", "partial_sha256": self.response_identity},
            evidence_root=self.root,
        )
        second = raw_evidence.write_immutable_json_receipt(
            receipt.path,
            {"status": "failed", "partial_sha256": self.response_identity},
            evidence_root=self.root,
        )
        self.assertEqual(receipt, second)
        self.assertEqual(stat.S_IMODE(receipt.path.stat().st_mode), 0o600)
        with self.assertRaises(raw_evidence.RawEvidenceConflict):
            raw_evidence.write_immutable_json_receipt(
                receipt.path,
                {"status": "succeeded"},
                evidence_root=self.root,
            )


if __name__ == "__main__":
    unittest.main()
