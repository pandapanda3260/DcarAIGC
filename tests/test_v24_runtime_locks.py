"""A schema24 prepared snapshot retains live guards without historical scans."""
import hashlib
import os
from pathlib import Path
from unittest.mock import patch

from tests.test_v23_runtime_evidence_context import RuntimeEvidenceContextTest
from v8 import runtime_evidence_context as context, duplicate_index_release


class Schema24EvidenceContextTest(RuntimeEvidenceContextTest):
    def setUp(self):
        super().setUp()
        self.connection.execute("PRAGMA user_version=24")
        self.build["critical_files"] = {"src/code.py": hashlib.sha256(self.code.read_bytes()).hexdigest()}
        self.build_ref = self.write('build.json', {'contract_version': 'sealed-build-receipt-v1',
            'payload': self.build, 'payload_sha256': duplicate_index_release.digest(self.build)})
        os.environ['DCAR_LOADED_BUILD_ID'] = 'sha256:' + self.build_ref['sha256']
        self.enterContext(patch.object(duplicate_index_release, 'verify_inheritance', side_effect=self.verifier))

    def test_verified_code_and_receipt_bytes_are_frozen_per_context(self):
        with context.prepare_inheritance(self.db) as prepared, self.boundary():
            self.assertEqual(prepared.schema_version, 24)
            self.assertTrue(context.prepared_critical_inventory(self.source, self.build['critical_files']))
            self.assertEqual(context.prepared_file_bytes(self.code), b'safe = True\n')
            with patch.object(Path, 'read_bytes', side_effect=AssertionError('file bytes reread inside lock')):
                self.assertEqual(context.prepared_file_bytes(Path(self.build_ref['path']), private=True),
                                 prepared.frozen_files[Path(self.build_ref['path'])])
            with self.assertRaises(context.RuntimeEvidenceChanged):
                context.prepared_critical_inventory(self.source, {'src/code.py': '0' * 64})
        self.assertIsNone(context.prepared_file_bytes(self.code))

    def test_history_is_frozen_but_live_source_refs_and_receipts_stay_fenced(self):
        ancestor = self.root / 'historical-source'
        ancestor.mkdir(); historical = ancestor / 'old.py'; historical.write_bytes(b'old')
        objects = self.source / '.git/objects/ab/cd'; objects.parent.mkdir(parents=True)
        objects.write_bytes(b'git object already verified')
        ref = self.source / '.git/refs/heads/main'
        names = (historical, objects, self.code, ref, self.plist, self.backup)
        values = {name: context._generation(name) for name in names}
        live = context._schema24_commit_files(values, {ancestor: {}, self.source: {}}, self.source)
        self.assertEqual(set(live), {self.code, ref, self.plist, self.backup})

    def test_frozen_file_change_is_detected_before_returning_cached_bytes(self):
        with self.assertRaises(context.RuntimeEvidenceChanged), context.prepare_inheritance(self.db), self.boundary():
            old = self.code.read_bytes()
            try:
                self.code.write_bytes(b'evil = True\n')
                with self.assertRaises(context.RuntimeEvidenceChanged):
                    context.prepared_file_bytes(self.code)
            finally:
                self.code.write_bytes(old)
            # Even restoring bytes cannot hide the generation change at exit.
            # This assertion is exercised in the inherited rollback tests.

    def test_schema22_and_diagnostic_keep_original_contract(self):
        super().test_schema22_and_diagnostic_keep_original_contract()
