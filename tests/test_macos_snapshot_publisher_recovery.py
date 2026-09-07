import json
import hashlib
import shlex
import tempfile
from types import SimpleNamespace
import subprocess
import sys
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from tests import test_macos_snapshot_publisher as baseline

publisher = baseline.publisher


class RetryTests(unittest.TestCase):
    def test_transient_disconnect_retries_exact_command_then_succeeds(self):
        calls = []
        def runner(args, **kwargs):
            calls.append((args, kwargs))
            return subprocess.CompletedProcess(args, 255 if len(calls) < 3 else 0,
                stdout='verified' if len(calls) == 3 else '', stderr="Can't assign requested address")
        with patch.object(publisher.clock_time, 'sleep') as sleep:
            result = publisher._run_rsync_checked(runner, ['rsync', '--dry-run', 'src/', 'dst/'], timeout=300)
        self.assertEqual(result, 'verified')
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(c[0] == calls[0][0] for c in calls))
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [2, 10])

    def test_exhaustion_is_bounded_and_keeps_failure(self):
        with patch.object(publisher.clock_time, 'sleep'), patch.object(publisher, '_run_checked',
                side_effect=publisher.SnapshotPublishError('rsync: Broken pipe')) as run:
            with self.assertRaisesRegex(publisher.SnapshotPublishError, 'Broken pipe'):
                publisher._run_rsync_checked(lambda: None, ['rsync'], timeout=300)
        self.assertEqual(run.call_count, 3)

    def test_permission_or_checksum_errors_are_not_retried(self):
        for reason in ('Permission denied (publickey)', 'manifest checksum mismatch', 'unexpected end of file'):
            with self.subTest(reason=reason), patch.object(publisher, '_run_checked',
                    side_effect=publisher.SnapshotPublishError(reason)) as run:
                with self.assertRaisesRegex(publisher.SnapshotPublishError, reason.split()[0]):
                    publisher._run_rsync_checked(lambda: None, ['rsync'], timeout=300)
                self.assertEqual(run.call_count, 1)

    def test_retry_does_not_extend_original_deadline(self):
        with patch.object(publisher.clock_time, 'monotonic', side_effect=[0, 0, 299]), patch.object(
                publisher, '_run_checked', side_effect=publisher.SnapshotPublishError('Broken pipe')) as run:
            with self.assertRaises(publisher.SnapshotPublishError):
                publisher._run_rsync_checked(lambda: None, ['rsync'], timeout=300)
        self.assertEqual(run.call_count, 1)


class StagingContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / 'incoming'
        self.config = SimpleNamespace(remote_python=sys.executable, remote_incoming_root=str(self.root))
        self.snapshot = '20260907T013956Z-' + 'b' * 12
        self.digest = hashlib.sha256(b'bound manifest').hexdigest()

    def prepare(self):
        def local_boundary(args, **kwargs):
            return subprocess.run(shlex.split(args[-1]), **kwargs)
        publisher._prepare_local_resume_incoming(self.config, ['ssh', 'unused'],
            runner=local_boundary, snapshot_id=self.snapshot, manifest_sha256=self.digest)

    def test_empty_staging_is_created_and_repeated_safely(self):
        self.prepare()
        self.prepare()
        self.assertTrue((self.root / self.snapshot / 'artifacts/cache').is_dir())

    def test_same_manifest_partial_transfer_is_preserved(self):
        self.prepare()
        bundle = self.root / self.snapshot / 'bundle'
        (bundle / 'manifest.json').write_bytes(b'bound manifest')
        (bundle / 'partial-data').write_bytes(b'preserve')
        self.prepare()
        self.assertEqual((bundle / 'partial-data').read_bytes(), b'preserve')

    def test_manifest_collision_is_rejected_without_overwrite(self):
        self.prepare()
        manifest = self.root / self.snapshot / 'bundle/manifest.json'
        manifest.write_bytes(b'wrong manifest')
        with self.assertRaisesRegex(publisher.SnapshotPublishError, 'identity differs'):
            self.prepare()
        self.assertEqual(manifest.read_bytes(), b'wrong manifest')

    def test_symlinked_staging_is_rejected(self):
        self.root.mkdir()
        outside = Path(self.tmp.name) / 'outside'
        outside.mkdir()
        (self.root / self.snapshot).symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(publisher.SnapshotPublishError, 'unsafe'):
            self.prepare()
        self.assertEqual(list(outside.iterdir()), [])


class ResumeTests(unittest.TestCase):
    # Reuse the real detached DB/evidence fixture without re-running inherited tests.
    setUp = baseline.MacOSSnapshotPublisherTest.setUp
    config = baseline.MacOSSnapshotPublisherTest.config
    runner = baseline.MacOSSnapshotPublisherTest.runner
    builder = baseline.MacOSSnapshotPublisherTest.builder
    write_manifest = baseline.MacOSSnapshotPublisherTest.write_manifest
    staged_resume = baseline.MacOSSnapshotPublisherTest.staged_resume

    def resume_local(self, manifest, runner, *, now=None):
        with patch.object(publisher.Path, 'home', return_value=self.fake_home):
            return publisher.publish_snapshot(project_root=self.project, database=None, legacy_database=None,
                config=self.config(), now=now or self.now, runner=runner,
                fetch_json=lambda _: self.fail('resume queried live Writer'),
                build_snapshot=lambda **_: self.fail('resume rebuilt snapshot'),
                resume_local_snapshot_id=manifest['snapshot_id'])

    def test_local_resume_transfers_same_frozen_bundle_then_installs_once(self):
        output, manifest, runner = self.staged_resume()
        before = baseline.sha(output / 'manifest.json')
        receipt = self.resume_local(manifest, runner)
        self.assertEqual(receipt['transport_mode'], 'resumed-verified-local-snapshot')
        self.assertEqual(before, baseline.sha(output / 'manifest.json'))
        transfers = [a for a in runner.commands if a[0] == 'rsync' and '--dry-run' not in a]
        self.assertEqual(len(transfers), 3)
        self.assertEqual(sum(' install --bundle ' in ' '.join(a) for a in runner.commands), 1)
        self.assertTrue(any('local resume remote manifest identity differs' in ' '.join(a) for a in runner.commands))

    def test_local_resume_rejects_previous_day_before_ssh(self):
        _, manifest, runner = self.staged_resume()
        with self.assertRaisesRegex(publisher.SnapshotPublishError, 'original Beijing business day'):
            self.resume_local(manifest, runner, now=self.now + timedelta(days=1))
        self.assertEqual(runner.commands, [])

    def test_local_resume_rejects_source_receipt_tampering(self):
        output, manifest, runner = self.staged_resume()
        path = output / publisher.SOURCE_RECEIPT_FILENAME
        body = json.loads(path.read_text())
        body['content_count'] += 1
        path.write_text(json.dumps(body))
        with self.assertRaises(publisher.SnapshotPublishError):
            self.resume_local(manifest, runner)
        self.assertEqual(runner.commands, [])

    def test_local_resume_counts_previously_uploaded_files_in_install_headroom(self):
        _, manifest, runner = self.staged_resume()
        cache = [row for row in manifest['files'] if row['root'] == 'cache']
        self.assertGreaterEqual(len(cache), 2)
        def partial_boundary(args, **kwargs):
            rendered = ' '.join(args)
            if '--dry-run' in rendered and ('cache-files-from0' in rendered or '/artifacts/cache/' in rendered):
                # First cache item remains local; the second was already uploaded.
                row = cache[0] if args[0] == 'rsync' else cache[1]
                return subprocess.CompletedProcess(args, 0, stdout=(
                    '>f+++++++++' + publisher.RSYNC_ITEM_SEPARATOR + row['path'] + '\n'
                    + 'Total transferred file size: ' + str(row['byte_size']) + ' bytes\n'), stderr='')
            return runner(args, **kwargs)
        receipt = self.resume_local(manifest, partial_boundary)
        report_bytes = sum(r['byte_size'] for r in manifest['files'] if r['root'] == 'reports')
        self.assertEqual(receipt['install_headroom']['artifact_activation_bytes'],
                         cache[0]['byte_size'] + cache[1]['byte_size'] + report_bytes)
        self.assertEqual(receipt['rsync_dry_run_cache_bytes'], cache[0]['byte_size'])

    def test_local_resume_transport_disconnect_reuses_output(self):
        output, manifest, runner = self.staged_resume()
        calls = []
        def disconnect_once(args, **kwargs):
            if args[0] == 'rsync' and '--dry-run' in args and not calls:
                calls.append(args)
                return subprocess.CompletedProcess(args, 255, stdout='', stderr='Connection reset by peer')
            return runner(args, **kwargs)
        with patch.object(publisher.clock_time, 'sleep'):
            receipt = self.resume_local(manifest, disconnect_once)
        self.assertEqual(receipt['snapshot_id'], manifest['snapshot_id'])
        self.assertEqual(len(calls), 1)
        self.assertTrue((output / 'publisher-receipt.json').is_file())


if __name__ == '__main__':
    suite = unittest.TestSuite()
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(RetryTests))
    suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(StagingContractTests))
    selected = [n for n in dir(ResumeTests) if n.startswith('test_local_resume_')]
    selected += [n for n in dir(ResumeTests) if n.startswith('test_resume_')]
    for name in selected:
        suite.addTest(ResumeTests(name))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(not result.wasSuccessful())
