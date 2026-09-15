"""Lexical path selection parity and unchanged preparation generation fences."""
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path
import random
import unittest
from unittest.mock import patch

from tests import test_v23_runtime_evidence_context as fixtures
from v8 import duplicate_index_release, runtime_evidence_context as context
from v8 import runtime_proof_workers as workers


def original_observed_roots(roots, observed):
    return {root for root in roots if any(name.is_relative_to(root) for name in observed)}


def original_commit_files(files, manifests, source):
    ancestors = tuple(root for root in manifests if root != source)
    git_objects = source / '.git' / 'objects'
    return {name: generation for name, generation in files.items()
            if not any(name == root or name.is_relative_to(root) for root in ancestors)
            and name != git_objects and not name.is_relative_to(git_objects)}


class PreparedPathIndexTest(unittest.TestCase):
    def test_lexical_membership_matches_native_path_for_edge_cases(self):
        paths = [Path(value) for value in (
            '', '.', '..', '../..', 'a', 'a/b', 'a/b/..', 'a/../b', 'a/b-other',
            './a//b/.', 'a/../../b', 'A/b', '/a', '/', '/a/b', '/a/b/..',
            '/a/../b', '/a/b-other', '//', '//a', '//a/b', '///a/b',
            '/does-not-exist/source/file.py', '/does-not-exist/source-other/file.py')]
        index = context._PathRootIndex(paths)
        for name in paths:
            with self.subTest(path=name):
                expected = {root for root in paths if name.is_relative_to(root)}
                self.assertEqual(set(index.matching_roots(name)), expected)
                self.assertEqual(index.contains(name), bool(expected))
        self.assertFalse(context._PathRootIndex([Path('.')]).contains(Path('/a')))
        self.assertFalse(context._PathRootIndex([Path('/source')]).contains(Path('/source-other/file')))
        self.assertTrue(context._PathRootIndex([Path('/source')]).contains(Path('/source/../other')))
        self.assertEqual(context._observed_source_roots(paths, []), set())
        self.assertEqual(context._observed_source_roots([], paths), set())

    def test_overlapping_roots_current_root_and_git_object_pruning_match(self):
        for source, roots in (
            ('/trees/current', ['/trees/current', '/trees/old', '/trees/old/nested']),
            ('trees/current', ['trees/current', 'trees/old', 'trees/old/nested']),
            ('/trees/current', ['/trees', '/trees/current']),
            ('/trees/current', []),
            ('.', ['.']),
            ('/', ['/']),
        ):
            source = Path(source)
            manifests = dict.fromkeys(map(Path, roots))
            names = [source, source / 'src/code.py', source / '.git', source / '.git/objects',
                     source / '.git/objects/pack/data', source / '.git/objects-other',
                     source / '.git/objects/../refs/main', source / '.git/refs/main',
                     Path('/trees/old'), Path('/trees/old/nested/code.py'),
                     Path('/trees/oldish/code.py'), Path('trees/old/code.py'), Path('unrelated')]
            files = {name: ('original-generation', number) for number, name in enumerate(names)}
            with self.subTest(source=source, roots=roots):
                self.assertEqual(list(context._schema24_commit_files(files, manifests, source).items()),
                                 list(original_commit_files(files, manifests, source).items()))
                self.assertEqual(context._observed_source_roots(manifests, files),
                                 original_observed_roots(manifests, files))

    def test_deterministic_many_path_diff_has_no_filesystem_or_pairwise_path_calls(self):
        randomizer = random.Random(20260915)
        roots = [Path(base) / str(number) for base in ('/sources', '//host/sources', 'sources')
                 for number in range(30)]
        roots += [root / 'nested' for root in roots[:20]]
        paths = []
        for _ in range(700):
            base = randomizer.choice(roots + [Path('/elsewhere'), Path('elsewhere')])
            parts = randomizer.choices(['nested', '..', 'component', 'component-other', '文件'],
                                       k=randomizer.randrange(0, 9))
            paths.append(base.joinpath(*parts))
        files = {name: ('g', number) for number, name in enumerate(paths)}
        source = roots[0]
        expected_roots = original_observed_roots(roots, files)
        expected_files = original_commit_files(files, roots, source)
        with patch.object(Path, 'is_relative_to', side_effect=AssertionError('pairwise path traversal')), \
                patch.object(Path, 'stat', side_effect=AssertionError('unexpected filesystem read')), \
                patch.object(Path, 'lstat', side_effect=AssertionError('unexpected filesystem read')), \
                patch.object(Path, 'resolve', side_effect=AssertionError('unexpected path resolution')):
            self.assertEqual(context._observed_source_roots(roots, files), expected_roots)
            self.assertEqual(list(context._schema24_commit_files(files, roots, source).items()),
                             list(expected_files.items()))

    @contextmanager
    def prepared_fixture(self, schema):
        fixture = fixtures.RuntimeEvidenceContextTest()
        fixture.setUp()
        try:
            old_root = fixture.root / 'ancestor'
            (old_root / '.git/refs').mkdir(parents=True)
            (old_root / 'src').mkdir()
            ancestor = old_root / 'src/ancestor.py'
            ancestor.write_text('ancestor = True\n')
            fixture.build['ancestor_tree'] = fixture.write('ancestor-tree.json', {
                'contract': 'writer-source-tree-v1', 'source_root': str(old_root),
                'files': [{'path': 'src/ancestor.py', 'sha256': hashlib.sha256(ancestor.read_bytes()).hexdigest()}]})
            if schema == 24:
                fixture.connection.execute('PRAGMA user_version=24')
                fixture.build['critical_files'] = {'src/code.py': hashlib.sha256(fixture.code.read_bytes()).hexdigest()}
            fixture.build_ref = fixture.write('build.json', {'contract_version': 'sealed-build-receipt-v1',
                'payload': fixture.build, 'payload_sha256': hashlib.sha256(
                    context._canonical(fixture.build).encode()).hexdigest()})
            fixture.enterContext(patch.dict(os.environ, {
                'DCAR_LOADED_BUILD_ID': 'sha256:' + fixture.build_ref['sha256']}))
            original = fixture.verifier.side_effect

            def proof(**kwargs):
                value = original(**kwargs)
                self.assertEqual(ancestor.read_text(), 'ancestor = True\n')
                return value

            fixture.verifier.side_effect = proof
            with patch.object(workers, 'enabled', return_value=False), \
                    patch.object(duplicate_index_release, 'verify_inheritance', side_effect=fixture.verifier):
                yield fixture, ancestor
        finally:
            fixture.doCleanups()

    def test_schema23_retains_ancestor_fences_schema24_prunes_only_after_snapshot(self):
        for schema in (23, 24):
            with self.subTest(schema=schema), self.prepared_fixture(schema) as (fixture, ancestor):
                with context.prepare_inheritance(fixture.db) as prepared:
                    self.assertEqual(prepared.schema_version, schema)
                    self.assertIn(fixture.code, prepared.files)
                    self.assertIn(fixture.source, prepared.files)
                    self.assertEqual(ancestor in prepared.files, schema == 23)
                    ancestor.write_text('ancestor = None\n')
                    if schema == 23:
                        with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'dependency changed'):
                            with fixture.boundary():
                                pass
                    else:
                        with fixture.boundary():
                            self.assertEqual(fixture.reuse(), {'immutable': 'original'})

    def test_changed_ancestor_during_verification_rejected_before_schema24_pruning(self):
        for schema in (23, 24):
            with self.subTest(schema=schema), self.prepared_fixture(schema) as (fixture, ancestor):
                original = fixture.verifier.side_effect

                def proof(**kwargs):
                    value = original(**kwargs)
                    before = ancestor.stat()
                    ancestor.write_text('ancestor = None\n')
                    os.utime(ancestor, ns=(before.st_atime_ns, before.st_mtime_ns))
                    self.assertEqual(ancestor.stat().st_ino, before.st_ino)
                    self.assertEqual(ancestor.stat().st_size, before.st_size)
                    return value

                fixture.verifier.side_effect = proof
                with patch.object(context, '_schema24_commit_files', wraps=context._schema24_commit_files) as pruning:
                    with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'changed during preparation'):
                        with context.prepare_inheritance(fixture.db):
                            self.fail('changed proof became usable')
                    pruning.assert_not_called()

    def test_current_source_change_still_denies_both_schema_boundaries(self):
        for schema in (23, 24):
            with self.subTest(schema=schema), self.prepared_fixture(schema) as (fixture, _ancestor):
                with context.prepare_inheritance(fixture.db):
                    before = fixture.code.stat()
                    fixture.code.write_text('safe = None\n')
                    os.utime(fixture.code, ns=(before.st_atime_ns, before.st_mtime_ns))
                    with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'dependency changed'):
                        with fixture.boundary():
                            pass

    def test_repeated_open_generation_check_still_rejects_before_read(self):
        with self.prepared_fixture(23) as (fixture, _ancestor):
            observed = {}
            token = context._OBSERVED.set(observed)
            try:
                self.assertEqual(fixture.code.read_text(), 'safe = True\n')
                fixture.code.write_text('safe = None\n')
                with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'changed during preparation'):
                    fixture.code.read_text()
            finally:
                context._OBSERVED.reset(token)


if __name__ == '__main__':
    unittest.main()
