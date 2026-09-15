from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
import hashlib
import importlib
import importlib.abc
import importlib.util
import json
from pathlib import Path
import re
import sys
import tempfile
import threading
import types
import unittest
from unittest.mock import patch
from uuid import uuid4

from v8 import pure_source_normalization as normal


def function_codes(body, filename):
    module = compile(body, filename, 'exec', dont_inherit=True)
    return {code.co_name: code for code in module.co_consts if isinstance(code, types.CodeType)}


class NormalizationRequestTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        self.package_name = '_dcar_normalizer_test_' + uuid4().hex
        package = types.ModuleType(self.package_name)
        package.__path__ = [str(self.root)]
        sys.modules[self.package_name] = package
        self.addCleanup(self.remove_modules)
        self.name = self.package_name + '.account_intake_code_successor'
        self.path = self.root / 'account_intake_code_successor.py'
        lines = ['from __future__ import annotations', 'import ast, re, json, hashlib']
        lines += [f'{name} = {value!r}' for name, value in normal._POLICY_CONSTANTS.items()]
        lines += ['def require(value, message):', '    if not value: raise ValueError(message)',
                  'def digest(value):', '    return value']
        for name in sorted(normal._NORMALIZERS):
            lines += [f'def {name}(body):', "    require(body != b'error', 'normalizer failed')",
                      '    return [str(body.decode())]' if name == '_stable_runtime_nodes' else '    return str(body.decode())']
        self.body = ('\n'.join(lines) + '\n').encode()
        self.path.write_bytes(self.body)
        codes = function_codes(self.body, str(self.path))
        pins = {name: normal._code_digest(code) for name, code in codes.items() if name != 'digest'}
        for attribute, value in [('_VERIFIER_SHA', hashlib.sha256(self.body).hexdigest()),
                                 ('_CODE_PINS', pins), ('_DIGEST_PIN', normal._code_digest(codes['digest']))]:
            mock = patch.object(normal, attribute, value)
            mock.start()
            self.addCleanup(mock.stop)

    def remove_modules(self):
        for name in list(sys.modules):
            if name == self.package_name or name.startswith(self.package_name + '.'):
                sys.modules.pop(name, None)

    def module(self, suffix=''):
        name = self.name if not suffix else '_dcar_normalizer_' + suffix + '.account_intake_code_successor'
        module = types.ModuleType(name)
        module.__file__ = str(self.path)
        exec(compile(self.body, str(self.path), 'exec', dont_inherit=True), vars(module))
        sys.modules[name] = module
        if suffix:
            self.addCleanup(sys.modules.pop, name, None)
        return module

    def test_same_input_reuses_representation_and_freshly_reads_verifier(self):
        module = self.module()
        with normal.normalization_request():
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats(), {'hits': 1, 'computations': 1, 'bypasses': 0, 'verifier_reads': 2})
        self.assertIsNone(normal.request_stats())

    def test_two_requests_each_start_cold(self):
        module = self.module()
        for _ in range(2):
            with normal.normalization_request():
                module._stable_intake_nodes(b'one')
                self.assertEqual(normal.request_stats()['computations'], 1)
                self.assertEqual(normal.request_stats()['hits'], 0)

    def test_nested_context_borrows_request(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            with normal.normalization_request():
                module._stable_intake_nodes(b'one')
                self.assertEqual(normal.request_stats()['hits'], 1)
            self.assertEqual(normal.request_stats()['computations'], 1)

    def test_identical_helper_private_namespace_shares_request(self):
        module = self.module()
        spec = importlib.util.spec_from_file_location('_dcar_helper_alias_' + uuid4().hex, normal.__file__)
        alias = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(alias)
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            with alias.normalization_request():
                module._stable_intake_nodes(b'one')
                self.assertEqual(alias.request_stats()['hits'], 1)
                self.assertEqual(alias.request_stats(), normal.request_stats())

    def test_two_threads_have_independent_cold_caches(self):
        module = self.module()
        barrier = threading.Barrier(2)
        def work(_):
            with normal.normalization_request():
                barrier.wait()
                module._stable_intake_nodes(b'one')
                module._stable_intake_nodes(b'one')
                return normal.request_stats()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(work, range(2)))
        self.assertEqual([row['computations'] for row in results], [1, 1])
        self.assertEqual([row['hits'] for row in results], [1, 1])

    def test_copied_context_cannot_share_cache_with_another_thread(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            inherited = copy_context()
            def work():
                with normal.normalization_request():
                    module._stable_intake_nodes(b'one')
                    return normal.request_stats()
            with ThreadPoolExecutor(max_workers=1) as pool:
                result = pool.submit(inherited.run, work).result()
            self.assertEqual(result['computations'], 1)
            self.assertEqual(result['hits'], 0)
            self.assertEqual(normal.request_stats()['computations'], 1)

    def test_changed_input_is_computed_again(self):
        module = self.module()
        with normal.normalization_request():
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(module._stable_intake_nodes(b'two'), 'two')
            self.assertEqual(normal.request_stats()['computations'], 2)

    def test_changed_verifier_never_reuses_old_result(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            self.path.write_bytes(self.body + b'\n# changed verifier\n')
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['hits'], 0)
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_unsafe_verifier_mode_bypasses_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            self.path.chmod(0o666)
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_mutable_list_results_do_not_poison_cache(self):
        module = self.module()
        with normal.normalization_request():
            first = module._stable_runtime_nodes(b'one')
            first.append('caller mutation')
            second = module._stable_runtime_nodes(b'one')
            self.assertEqual(second, ['one'])
            second.clear()
            self.assertEqual(module._stable_runtime_nodes(b'one'), ['one'])

    def test_import_without_context_has_original_behavior(self):
        module = importlib.import_module(self.name)
        self.assertFalse(hasattr(module._stable_intake_nodes, '__wrapped__'))
        self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
        with normal.normalization_request():
            self.assertTrue(hasattr(module._stable_intake_nodes, '__wrapped__'))
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['computations'], 1)

    def test_context_import_wraps_exact_standard_loader(self):
        with normal.normalization_request():
            module = importlib.import_module(self.name)
            self.assertTrue(hasattr(module._stable_intake_nodes, '__wrapped__'))
            module._stable_intake_nodes(b'one')
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['hits'], 1)

    def test_nonmatching_source_is_not_wrapped(self):
        module = self.module()
        self.path.write_bytes(self.body + b'\n# other source\n')
        with normal.normalization_request():
            self.assertFalse(hasattr(module._stable_intake_nodes, '__wrapped__'))
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['computations'], 0)

    def test_nonmatching_original_code_is_not_wrapped(self):
        module = self.module()
        module._stable_intake_nodes = lambda body: 'different original'
        with normal.normalization_request():
            self.assertEqual(module._stable_intake_nodes(b'one'), 'different original')
            self.assertFalse(hasattr(module._stable_runtime_nodes, '__wrapped__'))

    def test_changed_original_code_bypasses_cache(self):
        module = self.module()
        with normal.normalization_request():
            wrapped = module._stable_intake_nodes
            wrapped(b'one')
            wrapped.__wrapped__.__code__ = (lambda body: 'changed original').__code__
            self.assertEqual(wrapped(b'one'), 'changed original')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_changed_original_defaults_bypass_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            module._stable_intake_nodes.__wrapped__.__defaults__ = (b'new default',)
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_changed_original_kwdefaults_bypass_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            module._stable_intake_nodes.__wrapped__.__kwdefaults__ = {'body': b'new default'}
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_shadowed_builtin_global_changes_original_result(self):
        module = self.module()
        with normal.normalization_request():
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            module.str = lambda value: 'changed global'
            self.assertEqual(module._stable_intake_nodes(b'one'), 'changed global')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_initial_global_override_cannot_seed_other_namespace_cache(self):
        altered = self.module()
        clean = self.module('clean_' + uuid4().hex)
        altered.str = lambda value: 'altered namespace'
        with normal.normalization_request():
            self.assertFalse(hasattr(altered._stable_intake_nodes, '__wrapped__'))
            self.assertEqual(altered._stable_intake_nodes(b'one'), 'altered namespace')
            self.assertEqual(clean._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['computations'], 1)
            self.assertEqual(normal.request_stats()['hits'], 0)

    def test_changed_direct_stdlib_function_bypasses_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            with patch.object(ast, 'parse', wraps=ast.parse):
                self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_unrelated_non_dict_registration_marker_is_ignored(self):
        module = self.module()
        setattr(module, normal._MARKER, 'unrelated module metadata')
        with normal.normalization_request():
            self.assertFalse(hasattr(module._stable_intake_nodes, '__wrapped__'))
            self.assertEqual(module._stable_intake_nodes(b'one'), 'one')
            self.assertEqual(normal.request_stats()['computations'], 0)

    def test_concurrent_registration_publishes_one_wrapper_layer(self):
        module = self.module()
        barrier = threading.Barrier(2)
        def register(_):
            barrier.wait()
            return normal._register_module(module)
        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(register, range(2))), [True, True])
        self.assertTrue(hasattr(module._stable_intake_nodes, '__wrapped__'))
        self.assertFalse(hasattr(module._stable_intake_nodes.__wrapped__, '__wrapped__'))

    def test_changed_helper_binding_bypasses_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            module.require = lambda value, message: None
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_changed_policy_constant_bypasses_cache(self):
        module = self.module()
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            module.PREPARATION_REPAIR_HELPERS = dict(module.PREPARATION_REPAIR_HELPERS, unapproved='change')
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['bypasses'], 1)

    def test_kwargs_and_unsupported_input_keep_original_semantics(self):
        module = self.module()
        with normal.normalization_request():
            self.assertEqual(module._stable_intake_nodes(body=b'one'), 'one')
            self.assertEqual(module._stable_intake_nodes(bytearray(b'one')), 'one')
            with self.assertRaises(TypeError):
                module._stable_intake_nodes(b'one', extra=True)
            self.assertEqual(normal.request_stats()['bypasses'], 2)

    def test_original_exceptions_are_not_cached(self):
        module = self.module()
        with normal.normalization_request():
            for _ in range(2):
                with self.assertRaisesRegex(ValueError, 'normalizer failed'):
                    module._stable_intake_nodes(b'error')
            self.assertEqual(normal.request_stats()['computations'], 2)
            self.assertEqual(normal.request_stats()['hits'], 0)

    def test_same_bytes_in_two_isolated_modules_share_only_representation(self):
        first = self.module()
        second = self.module('other_' + uuid4().hex)
        with normal.normalization_request():
            self.assertIsNot(first._stable_intake_nodes, second._stable_intake_nodes)
            first._stable_intake_nodes(b'one')
            second._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['computations'], 1)
            self.assertEqual(normal.request_stats()['hits'], 1)

    def test_bounded_cache_evicts_without_changing_results(self):
        module = self.module()
        with patch.object(normal, '_MAX_ENTRIES', 2), normal.normalization_request():
            for body in (b'one', b'two', b'three', b'one'):
                self.assertEqual(module._stable_intake_nodes(body), body.decode())
            self.assertEqual(normal.request_stats()['computations'], 4)
            self.assertEqual(len(normal._request()['cache']), 2)

    def test_custom_finder_between_adapter_and_pathfinder_keeps_priority(self):
        calls = []
        class Loader(importlib.abc.Loader):
            def create_module(self, spec): return None
            def exec_module(self, module): calls.append('exec'); module.custom = True
        loader = Loader()
        wanted = self.name
        class Finder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname == wanted:
                    calls.append('find')
                    return importlib.util.spec_from_loader(fullname, loader)
                return None
        finder = Finder()
        with normal.normalization_request():
            index = sys.meta_path.index(normal._registry().finder)
            sys.meta_path.insert(index + 1, finder)
            try:
                module = importlib.import_module(self.name)
            finally:
                sys.meta_path.remove(finder)
            self.assertTrue(module.custom)
            self.assertIs(module.__loader__, loader)
            self.assertEqual(calls, ['find', 'exec'])

    def test_decorator_cleans_cache_after_failure(self):
        module = self.module()
        @normal.normalization_request()
        def work():
            module._stable_intake_nodes(b'one')
            raise RuntimeError('consumer failure')
        with self.assertRaisesRegex(RuntimeError, 'consumer failure'):
            work()
        self.assertIsNone(normal.request_stats())
        with normal.normalization_request():
            module._stable_intake_nodes(b'one')
            self.assertEqual(normal.request_stats()['hits'], 0)


class ActualNormalizerDifferentialTest(unittest.TestCase):
    def test_all_twelve_frozen_original_inputs_match_wrapped_outputs(self):
        root = Path('/Users/mark/Library/Application Support/DcarAIGC/writer-sources')
        child = root / '20260912-account-intake-v9'
        parent = root / '20260912-account-intake-v3'
        path = child / 'src/dcar_eval/v8/account_intake_code_successor.py'
        if not path.is_file() or not parent.is_dir():
            self.skipTest('the immutable historical source fixture is unavailable')
        body = path.read_bytes()
        self.assertEqual(hashlib.sha256(body).hexdigest(), normal._VERIFIER_SHA)
        name = '_dcar_actual_normalizer_' + uuid4().hex + '.account_intake_code_successor'
        module = types.ModuleType(name)
        module.__file__ = str(path)
        module.ast, module.re = ast, re
        namespace = vars(module)
        nodes = [node for node in ast.parse(body).body
                 if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.lineno < 60]
        future = ast.ImportFrom(module='__future__', names=[ast.alias(name='annotations')], level=0)
        exec(compile(ast.fix_missing_locations(ast.Module(body=[future, *nodes], type_ignores=[])), str(path), 'exec', dont_inherit=True), namespace)
        # Preserve code generated from the complete original module: Python
        # 3.12 specializes imported-global calls differently in AST subsets.
        codes = function_codes(body, str(path))
        for function in normal._CODE_PINS:
            namespace[function] = types.FunctionType(codes[function], namespace, function)
        digest_path = path.with_name('account_classification_release.py')
        digest_globals = {'hashlib': hashlib, 'json': json}
        digest_code = function_codes(digest_path.read_bytes(), str(digest_path))['digest']
        namespace['digest'] = types.FunctionType(digest_code, digest_globals, 'digest')
        originals = {key: namespace[key] for key in normal._NORMALIZERS}
        sys.modules[name] = module
        self.addCleanup(sys.modules.pop, name, None)
        files = {'account_preparation.py': '_stable_planning_nodes', 'capture.py': '_stable_capture_nodes',
                 'pipeline.py': '_stable_pipeline_nodes', 'capture_runtime.py': '_stable_runtime_nodes',
                 'account_intake.py': '_stable_intake_nodes', 'platform_adapters.py': '_stable_adapter_nodes'}
        with normal.normalization_request():
            self.assertTrue(all(hasattr(namespace[key], '__wrapped__') for key in normal._NORMALIZERS))
            for source in (parent, child):
                for filename, function in files.items():
                    with self.subTest(source=source.name, function=function):
                        source_body = (source / 'src/dcar_eval/v8' / filename).read_bytes()
                        expected = originals[function](source_body)
                        for _ in range(2):
                            actual = namespace[function](source_body)
                            self.assertIs(type(actual), type(expected))
                            self.assertEqual(actual, expected)
            self.assertEqual(normal.request_stats()['computations'], 12)
            self.assertEqual(normal.request_stats()['hits'], 12)
            self.assertEqual(normal.request_stats()['verifier_reads'], 24)
            # An imported helper's global lookup is part of syntax semantics:
            # changing its JSON implementation must not reuse a prior result.
            old_json = digest_globals['json']
            digest_globals['json'] = types.SimpleNamespace(dumps=lambda *a, **k: 'changed JSON')
            changed_body = (child / 'src/dcar_eval/v8/account_preparation.py').read_bytes()
            try:
                with self.assertRaises(ValueError):
                    originals['_stable_planning_nodes'](changed_body)
                with self.assertRaises(ValueError):
                    namespace['_stable_planning_nodes'](changed_body)
                self.assertEqual(normal.request_stats()['bypasses'], 1)
            finally:
                digest_globals['json'] = old_json


if __name__ == '__main__':
    unittest.main()
