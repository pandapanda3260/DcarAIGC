"""Real Git parity and mutation rejection for request-local ancestor manifests."""
from contextlib import contextmanager
from contextvars import ContextVar
import builtins
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch
from uuid import uuid4

from v8 import pure_source_inventory as inventory
from v8 import pure_source_normalization as normalization
from v8 import runtime_evidence_context as context


# Exact old verified_git implementation, kept at its original line 24 so the
# production bytecode pin is exercised. The two release modules are copied from
# this checkout; fixtures permit only their actual on-disk module SHA.
_GIT_FUNCTION = r'''def verified_git(root: Path, *arguments: str) -> bytes:
    """Read local Git without running repository filters, monitors or textconv."""
    environment = {key: value for key, value in os.environ.items()
                   if not key.startswith("GIT_CONFIG_") and key not in {
                       "GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR",
                       "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
                       "GIT_EXTERNAL_DIFF", "GIT_DIFF_OPTS", "GIT_ATTR_SOURCE"}}
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                       GIT_NO_LAZY_FETCH="1", GIT_OPTIONAL_LOCKS="0", GIT_PAGER="cat")
    prefix = ["git", "-c", "core.fsmonitor=false", "-C", str(root)]
    configured = subprocess.run([*prefix, "config", "--null", "--list"], check=True,
                                env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    for item in configured.split(b"\0"):
        key, _, value = item.partition(b"\n")
        name = key.decode("utf-8").lower()
        if (name.startswith("filter.") or name == "diff.external"
                or re.fullmatch(r"diff\..+\.(command|textconv)", name)
                or (name == "core.fsmonitor" and value.lower() not in {b"false", b"0", b"no", b"off"})):
            raise ValueError("external Git helper is forbidden in a sealed source repository")
    return subprocess.run([*prefix, *arguments], check=True, env=environment,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
'''
_COMMANDS = ContextVar('inventory_test_subprocesses', default=None)
_SCANS = ContextVar('inventory_test_scans', default=None)


def _audit(event, args):
    if event == 'subprocess.Popen' and _COMMANDS.get() is not None:
        _COMMANDS.get().append(tuple(args[1]))
    if event == 'os.scandir' and _SCANS.get() is not None:
        _SCANS.get().append(str(args[0]))


sys.addaudithook(_audit)


class PreparedInventoryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = self.enterContext(tempfile.TemporaryDirectory())
        self.directory = Path(self.temporary).resolve()
        self.root = self.directory / 'source'
        self.root.mkdir()
        self.home = self.directory / 'home'
        self.home.mkdir()
        self.enterContext(patch.dict(os.environ, {'HOME': str(self.home),
            'XDG_CONFIG_HOME': str(self.home / '.config'), 'GIT_CONFIG_NOSYSTEM': '1',
            'GIT_CONFIG_GLOBAL': os.devnull, 'PYTHONDONTWRITEBYTECODE': '1'}))
        package = self.root / 'src/dcar_eval/v8'
        package.mkdir(parents=True)
        implementation = Path(inventory.__file__).parent
        for name in ('account_intake_release.py', 'account_classification_release.py'):
            shutil.copyfile(implementation / name, package / name)
        header = 'from __future__ import annotations\nimport os\nimport re\nimport subprocess\nfrom pathlib import Path\n'
        old_git = header + '\n' * (23 - header.count('\n')) + _GIT_FUNCTION
        (package / 'runtime_paths.py').write_text(old_git)
        (package / 'four_platform_flow_release.py').write_text(
            'from .account_intake_release import inventory\n'
            'def inspect(source):\n    return inventory(source)\n')
        self.enterContext(patch.object(inventory, '_GIT_MODULES',
            inventory._GIT_MODULES | {hashlib.sha256(old_git.encode()).hexdigest()}))
        self.payload = self.root / 'payload.py'
        self.payload.write_text('VALUE = True\n')
        (self.root / '.gitignore').write_text('ignored/\n')
        (self.root / 'ignored/deep').mkdir(parents=True)
        self.ignored = self.root / 'ignored/deep/control'
        self.ignored.write_text('ignored original\n')
        self.git('init', '-q')
        self.git('add', '.')
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                 'commit', '-qm', 'fixture')
        self.module = self.load_module()
        self.original = self.module.inventory
        self.manifest = self.original(self.root)
        self.bind_manifest()

    def git(self, *args):
        return subprocess.run(['git', '-C', str(self.root), *args], check=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout

    def load_module(self):
        name = '_dcar_inventory_fixture_' + uuid4().hex
        module = types.ModuleType(name)
        module.__path__ = [str(self.root / 'src/dcar_eval/v8')]
        module.__package__ = name
        sys.modules[name] = module
        self.addCleanup(lambda: [sys.modules.pop(key, None) for key in list(sys.modules)
                                 if key == name or key.startswith(name + '.')])
        return importlib.import_module(name + '.account_intake_release')

    def bind_manifest(self):
        self.manifests = {self.root: self.manifest}
        self.enterContext(patch.object(inventory, '_REVIEWED_MANIFESTS',
            inventory._REVIEWED_MANIFESTS | {inventory._manifest_digest(self.manifest)}))

    @contextmanager
    def request(self, observed=None):
        observed = {} if observed is None else observed
        with normalization.normalization_request(), inventory.inventory_request(
                self.manifests, observed, generation=context._generation):
            yield observed

    def prime(self):
        result = self.module.inventory(self.root)
        self.assertEqual(result, self.manifest)
        self.assertEqual(inventory.request_stats()['computations'], 1)
        return result

    def test_real_git_parity_two_namespaces_deepcopy_and_exact_command_counts(self):
        commands = []
        token = _COMMANDS.set(commands)
        try:
            with self.request() as observed:
                self.assertEqual(self.prime(), self.original(self.root))
                self.assertEqual(len(commands), 16 + 12)
                commands.clear()
                other = self.load_module()
                returned = other.inventory(self.root)
                self.assertEqual(returned, self.manifest)
                self.assertEqual(len(commands), 6)
                self.assertEqual([args[5] for args in commands], ['config', 'status', 'config', 'ls-files', 'config', 'status'])
                returned['git']['head'] = 'edited returned value'
                returned['files'][0]['sha256'] = 'edited nested value'
                returned['files'].clear()
                self.assertEqual(self.module.inventory(self.root), self.manifest)
                self.assertEqual(inventory.request_stats(), {'computations': 1, 'hits': 2, 'bypasses': 0})
                self.assertIn(self.ignored, observed)
                self.assertIn(self.root / 'ignored/deep', observed)
        finally:
            _COMMANDS.reset(token)

    def test_cached_tracked_or_ignored_edit_with_restored_mtime_is_rejected(self):
        for path in (self.payload, self.ignored, self.root / '.git/HEAD'):
            with self.subTest(path=path):
                before_body = path.read_bytes()
                with self.request():
                    self.prime()
                    before = path.stat()
                    path.write_bytes(before_body.replace(before_body[:1], b'X', 1))
                    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
                    self.assertEqual(path.stat().st_size, before.st_size)
                    with self.assertRaisesRegex(inventory.InventoryChanged, 'source changed'):
                        self.module.inventory(self.root)
                path.write_bytes(before_body)

    def test_ignored_new_child_and_directory_read_failure_fail_closed(self):
        with self.request():
            self.prime()
            (self.root / 'ignored/deep/new-child').write_text('new')
            with self.assertRaisesRegex(inventory.InventoryChanged, 'source changed'):
                self.module.inventory(self.root)
        (self.root / 'ignored/deep/new-child').unlink()
        with self.request():
            self.prime()
            with patch.object(inventory, '_full_fence', side_effect=PermissionError('unreadable')):
                with self.assertRaisesRegex(inventory.InventoryChanged, 'ineligible'):
                    self.module.inventory(self.root)

    def test_external_default_ignore_is_read_fresh_on_every_hit(self):
        ignore = self.home / '.config/git/ignore'
        ignore.parent.mkdir(parents=True)
        ignore.write_text('external-only\n')
        (self.root / 'external-only').write_text('excluded using default global ignore')
        self.manifest = self.original(self.root)
        self.bind_manifest()
        with self.request():
            self.prime()
            ignore.write_text('')
            with self.assertRaisesRegex(inventory.InventoryChanged, 'Git status differs'):
                self.module.inventory(self.root)

    def test_unsafe_first_config_bypasses_and_post_cache_config_change_rejects(self):
        included = self.home / 'included-config'
        included.write_text('[user]\n name = Fixture\n')
        self.git('config', 'include.path', str(included))
        with self.request():
            self.assertEqual(self.module.inventory(self.root), self.original(self.root))
            self.assertEqual(inventory.request_stats(), {'computations': 0, 'hits': 0, 'bypasses': 1})
        self.git('config', '--unset', 'include.path')
        with self.request():
            self.prime()
            self.git('config', 'filter.unsafe.clean', 'must-not-execute')
            with self.assertRaisesRegex(inventory.InventoryChanged, 'source changed'):
                self.module.inventory(self.root)

    def test_unknown_root_uses_original(self):
        with patch.object(inventory, '_REVIEWED_MANIFESTS', frozenset()), self.request():
            self.assertEqual(self.module.inventory(self.root), self.manifest)
            self.assertEqual(inventory.request_stats()['computations'], 0)

    def test_ignored_symbolic_leaf_is_opaque_and_preserves_target_generation(self):
        external = self.directory / 'external'
        (external / 'nested').mkdir(parents=True)
        (external / 'nested/secret').write_text('must not traverse')
        link = self.root / 'ignored/link'
        link.symlink_to(external, target_is_directory=True)
        scans = []
        token = _SCANS.set(scans)
        try:
            with self.request() as observed:
                self.prime()
                self.assertEqual(self.module.inventory(self.root), self.manifest)
                self.assertEqual(inventory.request_stats()['hits'], 1)
                self.assertIn(link, observed)
                self.assertFalse(any(Path(name).is_relative_to(external) for name in scans))
                (external / 'new-child').write_text('target directory generation changes')
                with self.assertRaisesRegex(inventory.InventoryChanged, 'source changed'):
                    self.module.inventory(self.root)
        finally:
            _SCANS.reset(token)

    def test_symbolic_leaf_rejects_listed_self_descendants_git_and_missing_target(self):
        link = self.root / 'ignored/link'
        link.symlink_to(self.directory, target_is_directory=True)
        fence = inventory._full_fence(self.root, context._generation)
        self.assertFalse(inventory._opaque_leaves_excluded(self.root, fence, ['ignored/link'], self.manifest))
        self.assertFalse(inventory._opaque_leaves_excluded(self.root, fence, ['ignored/link/missing-tracked'], self.manifest))
        altered = dict(self.manifest, files=[*self.manifest['files'], {'path': 'ignored/link/manifest-child'}])
        self.assertFalse(inventory._opaque_leaves_excluded(self.root, fence, [], altered))
        link.unlink()
        link.symlink_to(self.directory / 'missing-target')
        with self.request():
            self.assertEqual(self.module.inventory(self.root), self.manifest)
            self.assertEqual(inventory.request_stats()['bypasses'], 1)
        link.unlink()
        (self.root / '.git/info/forbidden-link').symlink_to(self.ignored)
        with self.request():
            self.assertEqual(self.module.inventory(self.root), self.manifest)
            self.assertEqual(inventory.request_stats()['bypasses'], 1)

    def test_preloaded_same_package_flow_alias_is_rebound_and_checked(self):
        flow = importlib.import_module(self.module.__package__ + '.four_platform_flow_release')
        self.assertIs(flow.inventory, self.original)
        with self.request():
            self.assertIs(flow.inventory, self.module.inventory)
            self.prime()
            self.assertEqual(flow.inspect(self.root), self.manifest)
            self.assertEqual(inventory.request_stats()['hits'], 1)
            with patch.object(flow, 'inventory', self.original):
                with self.assertRaisesRegex(inventory.InventoryChanged, 'implementation'):
                    self.module.inventory(self.root)
        self.assertEqual(flow.inspect(self.root), self.manifest)
        with self.request():
            self.assertEqual(flow.inspect(self.root), self.manifest)
            self.assertEqual(inventory.request_stats()['computations'], 1)
            self.assertEqual(inventory.request_stats()['hits'], 0)

    def test_foreign_alias_or_changed_flow_source_is_never_rebound(self):
        flow = importlib.import_module(self.module.__package__ + '.four_platform_flow_release')
        different = lambda source: {'different': True}
        with patch.object(flow, 'inventory', different), self.request():
            self.assertIs(flow.inventory, different)
        body = Path(flow.__file__).read_bytes()
        Path(flow.__file__).write_bytes(body + b'# altered source\n')
        with self.request():
            self.assertIs(flow.inventory, self.original)

    def test_tracked_missing_file_retains_original_complete_list_semantics(self):
        self.payload.unlink()
        self.manifest = self.original(self.root)
        self.bind_manifest()
        self.assertNotIn('payload.py', [row['path'] for row in self.manifest['files']])
        with self.request():
            self.prime()
            self.assertEqual(self.module.inventory(self.root), self.manifest)
            self.assertIn('payload.py', inventory._request()['cache'][self.root]['names'])

    def test_nested_request_exception_and_thread_do_not_share_results(self):
        observed = {}
        with self.assertRaisesRegex(RuntimeError, 'fixture abort'):
            with self.request(observed):
                self.prime()
                with inventory.inventory_request(self.manifests, observed, generation=context._generation):
                    self.module.inventory(self.root)
                self.assertEqual(inventory.request_stats()['hits'], 1)
                other_stats = []
                def other_thread():
                    with self.request():
                        self.module.inventory(self.root)
                        other_stats.append(inventory.request_stats())
                worker = threading.Thread(target=other_thread)
                worker.start()
                worker.join(10)
                self.assertFalse(worker.is_alive())
                self.assertEqual(other_stats[0]['computations'], 1)
                self.assertEqual(other_stats[0]['hits'], 0)
                raise RuntimeError('fixture abort')
        self.assertIsNone(inventory.request_stats())
        with self.request():
            self.prime()
            self.assertEqual(inventory.request_stats()['hits'], 0)

    def test_import_binding_environment_namespace_and_git_identity_changes_reject(self):
        for change in ('os_module', 'stat_module', 'import_builtin', 'environment', 'raw', 'git_identity', 'code'):
            with self.subTest(change=change), self.request():
                self.prime()
                if change.endswith('_module'):
                    name = change.split('_')[0]
                    changed = patch.dict(sys.modules, {name: types.ModuleType(name)})
                elif change == 'import_builtin':
                    old_import = builtins.__import__
                    changed = patch.object(builtins, '__import__', lambda *a, **k: old_import(*a, **k))
                elif change == 'environment':
                    changed = patch.dict(os.environ, {'INVENTORY_INPUT_CHANGED': '1'})
                elif change == 'raw':
                    changed = patch.object(self.module, 'raw', lambda *a, **k: b'different')
                elif change == 'code':
                    original = self.module.__dict__[inventory._MARKER]['original']
                    @contextmanager
                    def replace_code():
                        before = original.__code__
                        original.__code__ = (lambda source: {}).__code__
                        try:
                            yield
                        finally:
                            original.__code__ = before
                    changed = replace_code()
                else:
                    changed = patch.object(inventory, '_git_identity', return_value=('different',))
                with changed, self.assertRaises(inventory.InventoryChanged):
                    self.module.inventory(self.root)

    def test_full_fence_is_merged_and_repeated_open_still_rejects_before_read(self):
        observed = {}
        token = context._OBSERVED.set(observed)
        try:
            with self.request(observed):
                self.prime()
                self.module.inventory(self.root)
                old_generation = observed[self.ignored]
                self.ignored.write_text('last hit is finished; change must survive final CAS')
                self.assertNotEqual(context._generation(self.ignored), old_generation)
                with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'changed during preparation'):
                    self.ignored.read_bytes()
        finally:
            context._OBSERVED.reset(token)

    def test_ignored_change_after_last_hit_fails_final_cas_before_schema24_pruning(self):
        from tests import test_v23_runtime_evidence_context as fixtures
        from v8 import duplicate_index_release, runtime_proof_workers
        for schema in (23, 24):
            with self.subTest(schema=schema):
                fixture = fixtures.RuntimeEvidenceContextTest()
                fixture.setUp()
                try:
                    fixture.build['ancestor_tree'] = fixture.write('inventory-ancestor.json', self.manifest)
                    if schema == 24:
                        fixture.connection.execute('PRAGMA user_version=24')
                        fixture.build['critical_files'] = {'src/code.py': hashlib.sha256(fixture.code.read_bytes()).hexdigest()}
                    fixture.build_ref = fixture.write('build.json', {'contract_version': 'sealed-build-receipt-v1',
                        'payload': fixture.build, 'payload_sha256': hashlib.sha256(
                            context._canonical(fixture.build).encode()).hexdigest()})
                    fixture.enterContext(patch.dict(os.environ, {'DCAR_LOADED_BUILD_ID': 'sha256:' + fixture.build_ref['sha256']}))
                    original = fixture.verifier.side_effect
                    def proof(**kwargs):
                        result = original(**kwargs)
                        self.assertEqual(self.module.inventory(self.root), self.manifest)
                        self.assertEqual(self.module.inventory(self.root), self.manifest)
                        self.assertEqual(inventory.request_stats()['hits'], 1)
                        self.ignored.write_text('changed after the final inventory hit')
                        return result
                    fixture.verifier.side_effect = proof
                    with patch.object(runtime_proof_workers, 'enabled', return_value=False), \
                            patch.object(duplicate_index_release, 'verify_inheritance', side_effect=fixture.verifier), \
                            patch.object(context, '_schema24_commit_files', wraps=context._schema24_commit_files) as pruning:
                        with self.assertRaisesRegex(context.RuntimeEvidenceChanged, 'changed during preparation'):
                            with context.prepare_inheritance(fixture.db):
                                self.fail('changed ignored dependency became usable')
                        pruning.assert_not_called()
                finally:
                    fixture.doCleanups()


if __name__ == '__main__':
    unittest.main()
