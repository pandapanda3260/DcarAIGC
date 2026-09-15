"""Registered external proof relocation and required originals use real bytes."""
import copy
import hashlib
import json
import shutil
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch
from tests import test_server_snapshot_deployment as fixture
from v8 import artifact_paths

builder, installer = fixture.builder, fixture.installer


class RequiredOriginalsTest(unittest.TestCase):
    def setUp(self):
        self.f=fixture.ServerSnapshotDeploymentTest(methodName='runTest')
        self.f.setUp(); self.addCleanup(self.f.tearDown)

    def register(self, path, kind):
        with sqlite3.connect(self.f.database) as c:
            c.execute("UPDATE content_items SET published_at='2026-09-01T01:00:00Z' WHERE id=1")
            return c.execute("INSERT INTO evidence_artifacts(content_id,artifact_type,local_path,sha256,byte_size,status,created_at) "
                "VALUES(1,?,?,?,?, 'available','2026-09-15T00:00:00Z')",
                (kind,str(path),fixture._sha256(path),path.stat().st_size)).lastrowid

    def stage(self, manifest):
        config=self.f.server_config()
        shutil.copytree(self.f.bundle/'frozen-artifacts', self.f.bundle.parent/'artifacts')
        installer.verify_bundle(self.f.bundle,config)
        return config

    def imported(self):
        path=self.f.root/'outside-project'/'proof.json';path.parent.mkdir()
        path.write_text(json.dumps({'contract':'standalone-douyin-history-import-v1','platform_content_id':'123'})+'\n')
        path.chmod(0o600)
        self.register(path,'standalone_history_import')
        return path

    def test_registered_import_keeps_exact_bytes_database_and_resolvable_alias(self):
        path=self.imported();body=path.read_bytes()
        manifest=self.f.build_bundle();config=self.stage(manifest)
        alias=artifact_paths.imported_evidence_aliases(manifest)[str(path)]
        with sqlite3.connect(self.f.bundle/'databases/dcar_insight.sqlite3') as c:
            self.assertEqual(c.execute("SELECT local_path FROM evidence_artifacts WHERE artifact_type='standalone_history_import'").fetchone()[0],str(path))
        copied=self.f.bundle/'frozen-artifacts/cache'/Path(alias['project_path']).relative_to('data/cache')
        self.assertEqual(copied.read_bytes(),body)
        self.assertEqual(path.read_bytes(),body)
        context={'runtime_evidence_aliases':{},'imported_evidence_aliases':{str(path):alias}}
        with patch.object(artifact_paths,'installed_snapshot',return_value=context):
            self.assertEqual(artifact_paths.resolve(path,fallback_root=config.cache_root.parent),config.cache_root.parent/alias['project_path'])

    def test_tampered_source_or_unregistered_type_is_rejected(self):
        path=self.imported();path.write_bytes(b'{"contract":"changed"}')
        with self.assertRaises(builder.SnapshotBuildError):self.f.build_bundle()
        with sqlite3.connect(self.f.database) as c:c.execute("UPDATE evidence_artifacts SET artifact_type='media_source' WHERE local_path=?",(str(path),))
        with self.assertRaises(builder.SnapshotBuildError):self.f.build_bundle()

    def test_alias_for_wrong_database_row_is_rejected_by_receiver(self):
        path=self.imported();manifest=self.f.build_bundle();self.stage(manifest)
        bad=copy.deepcopy(manifest);bad['imported_evidence_aliases']['files'][0]['artifact_id']+=100
        fixture._write_bundle_manifest(self.f.bundle,bad)
        with self.assertRaisesRegex(installer.SnapshotInstallError,'registered database row'):
            installer.verify_bundle(self.f.bundle,self.f.server_config())

    def test_september_registered_video_is_required_and_cannot_be_downgraded(self):
        video=self.f.project/'data/cache/v8/media/test/video.mp4';self.register(video,'media')
        manifest=self.f.build_bundle();self.stage(manifest)
        row=next(r for r in manifest['files'] if r['project_path'].endswith('video.mp4'))
        self.assertFalse(any(r['project_path']==row['project_path'] for r in manifest['optional_reuse_files']))
        self.assertEqual((self.f.bundle/'frozen-artifacts'/row['root']/row['path']).read_bytes(),video.read_bytes())
        bad=copy.deepcopy(manifest);bad['files'].remove(row);bad['optional_reuse_files'].append({**row,'reason':'large_binary'})
        with self.assertRaisesRegex(installer.SnapshotInstallError,'required, hash-bound'):
            installer._verify_managed_originals(self.f.bundle,bad,self.f.bundle/'databases/dcar_insight.sqlite3',self.f.server_config(),verify_artifacts=False)

    def test_missing_september_registered_video_blocks_snapshot(self):
        video=self.f.project/'data/cache/v8/media/test/video.mp4';self.register(video,'media');video.unlink()
        with self.assertRaisesRegex(builder.SnapshotBuildError,'missing or unsafe'):
            self.f.build_bundle()

    def test_september_manifest_original_images_are_required_before_analysis(self):
        image=self.f.project/'data/cache/v8/media/test/original.png';image.write_bytes(b'image-fixture')
        manifest_path=image.with_name('media.json')
        manifest_path.write_text(json.dumps({'image_paths':[str(image)]})+'\n')
        with sqlite3.connect(self.f.database) as c:
            c.execute("UPDATE content_items SET published_at='2026-09-01T00:00:00Z'")
            c.execute("UPDATE evidence_artifacts SET sha256=?,byte_size=? WHERE artifact_type='media_manifest'",
                (fixture._sha256(manifest_path),manifest_path.stat().st_size))
        manifest=self.f.build_bundle();self.stage(manifest)
        self.assertTrue(any(r['project_path'].endswith('/original.png') for r in manifest['files']))
        self.assertFalse(any(r['project_path'].endswith('/original.png') for r in manifest['optional_reuse_files']))

    def test_large_valid_coverage_receipt_keeps_self_hash_and_bounded_size(self):
        from v8 import runtime_receipts
        state=self.f.root/'Library/Application Support/DcarAIGC'
        receipt=runtime_receipts._write_evidence(self.f.database,'profile-day-coverage-v3',
            {'contract_version':'profile-day-coverage-evidence-v3','coverage':'x'*70000},
            evidence_root=state/'evidence')
        path=Path(receipt['path']);body=path.read_bytes();sha=hashlib.sha256(body).hexdigest()
        with patch.object(builder,'FORMAL_STATE_ROOT',state):
            canonical=builder._declared_project_reference(str(path),sha,len(body),
                project_root=self.f.project,aliases={})
            self.assertEqual((self.f.project/canonical).read_bytes(),body)
            with self.assertRaises(builder.SnapshotBuildError):
                builder._declared_project_reference(str(path),sha,builder.MAX_RUNTIME_EVIDENCE_BYTES+1,
                    project_root=self.f.project,aliases={})
