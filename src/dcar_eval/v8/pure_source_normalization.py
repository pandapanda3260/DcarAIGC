"""Request-local reuse of six exact historical source normalizers.

This caches only immutable syntax representations. Every caller still reads its
source, every wrapper freshly reads/hashes its verifier, and all original source
comparisons, inventories, database checks and authority decisions run normally.
The narrowly scoped import adapter supports isolated ancestor package names; it
never replaces ast.parse or a verifier/authorization function.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
from contextvars import ContextVar
import _imp
import ast
import builtins
import hashlib
import importlib.machinery
import json
import os
from pathlib import Path
import re
import stat
import sys
import threading
import types
from functools import wraps

_HELPER_SHA = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_POLICY_VERSION = 'intake-v9-six-normalizers-v1'
_VERIFIER_SHA = 'd74a5bb982b46d34da3051b5ea7115f2e179715a8ac94a4110b3dba7b5ad677a'
_CODE_PINS = {'require': 'b7ee66addb4e087f8082c2afcdc367eafdb3fd73d58c40c58d83fbee4628cf9d', '_dump': '54ac97738c1fb5b2c7541f155c91703592909fffcf1fc7a71fae015bab7eb539', '_statement': 'f425d94f2b77320e1b590a0650a7ffb66ae88fbc055d0ed38a065086dace2710', '_stable_capture_nodes': '0cba8c3f42c494bba8a4f16cbb6272410e2aa624c555622cba69a11e22a0dfdb', '_normalize_admission_refresh': '07402dc8b785c299af5d3a34cac6e50115a627bfed117d567130306a837762c6', '_stable_pipeline_nodes': '5630a4e84cdf3c246af5e34936686836f1485957fdbcf9cb96cd996007cdcd00', '_stable_runtime_nodes': 'ea6794c299486429ef2d491505670c290b6b184e28d5abf39d1698a8777348f8', '_stable_intake_nodes': 'f7a89bb1baf03d7211ac22fdee80187b485b4b47901390da7b84720df315ca91', '_stable_adapter_nodes': '7466f31d0a3a2d09817f0d21ef2e60e80295d3ca08dd87478e8cf44e0e73cf30', '_stable_retry_evidence': '8e8b1027d77dca2d5e56fbd604246a2916d068a888a2e7700306687e5bea4411', '_stable_preparation_execution': '0d3ae608acb33d745b7d57b9790535a9405cd9a159fbe21d256022e99ab98483', '_stable_planning_nodes': 'd80b3508d6f9ecd25ed025ca3d94acb9673032c548e5d13a29be1c51ebc3d217'}
_DIGEST_PIN = '6cf348198f785127939c55e155afaa8df7fa06ba0d83731fbc2645f623340d14'
_NORMALIZERS = frozenset({
    '_stable_planning_nodes', '_stable_capture_nodes', '_stable_pipeline_nodes',
    '_stable_runtime_nodes', '_stable_intake_nodes', '_stable_adapter_nodes',
})
_POLICY_CONSTANTS = {
    'PLANNING_FUNCTIONS': frozenset({'validate_paid_target', 'enqueue_pending', '_planning_cache',
        '_planning_validation', '_validated_plan', 'planning_reconsideration', '_reuse_existing_profile'}),
    'PREPARATION_REPAIR_HELPERS': {
        '_load_resolver_parser_replay': '27d63c8ea3b2745a036043293b8055588137460b4ce9508d21d7dabbd7808ee9',
        '_never_sent_reservation_evidence': 'efee5f694a018aced22d7d8eab74b60c7e5b9eccdbfae451a9d4563400fd0109',
        '_recover_never_sent_preparation': '8dec1f6b50da57923a45485a54cb65a84a3a0734e8a6de22581a85f2e746f181',
    },
    'NEVER_SENT_PLANNER_AST': '0d20ffaa16bb48db70786f5e39c5fdd6cdd0a8db127a107aba8c2405413ed618',
}
_MAX_ENTRIES = 64
_MAX_RESULT_BYTES = 8 * 1024 * 1024
_MARKER = '_dcar_pure_normalizer_binding_v1'
_MISSING = object()
_BUILTINS = dict(vars(builtins))
_STDLIB_INPUTS = tuple((module, name, getattr(module, name)) for module, names in (
    (ast, ('AST', 'AnnAssign', 'Assign', 'AsyncFunctionDef', 'Attribute', 'Call', 'Constant',
           'FunctionDef', 'If', 'IfExp', 'ImportFrom', 'JoinedStr', 'Load', 'Module', 'Name',
           'Try', 'With', 'arg', 'dump', 'iter_fields', 'parse', 'stmt', 'walk')),
    (re, ('findall', 'fullmatch')), (json, ('dumps',)), (hashlib, ('sha256',)),
) for name in names)


def _constant(value):
    if isinstance(value, types.CodeType):
        return ['code', _code_shape(value)]
    if isinstance(value, bytes):
        return ['bytes', value.hex()]
    if isinstance(value, tuple):
        return ['tuple', [_constant(child) for child in value]]
    if isinstance(value, frozenset):
        return ['frozenset', sorted((_constant(child) for child in value), key=repr)]
    if isinstance(value, float):
        return ['float', value.hex()]
    if isinstance(value, complex):
        return ['complex', value.real.hex(), value.imag.hex()]
    if value is Ellipsis:
        return ['ellipsis']
    return [type(value).__name__, value]


def _code_shape(code):
    # Filename is checked independently against the freshly hashed module path.
    # All executable bytes, nested functions, constants and argument/line shape
    # are bound. A different Python compiler safely misses the fixed pins.
    return [code.co_name, code.co_qualname, code.co_argcount, code.co_posonlyargcount,
            code.co_kwonlyargcount, code.co_flags, code.co_code.hex(),
            [_constant(value) for value in code.co_consts], code.co_names,
            code.co_varnames, code.co_freevars, code.co_cellvars, code.co_firstlineno,
            code.co_linetable.hex(), code.co_exceptiontable.hex()]


def _code_digest(code):
    return hashlib.sha256(json.dumps(_code_shape(code), ensure_ascii=False,
                                    separators=(',', ':')).encode()).hexdigest()


def _fresh_verifier(path):
    before = path.lstat()
    if (path.resolve(strict=True) != path or not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid() or before.st_nlink != 1
            or before.st_mode & 0o022 or not 0 <= before.st_size <= 1024 * 1024):
        return None
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        body = stream.read(1024 * 1024 + 1)
        opened = os.fstat(stream.fileno())
    after = path.lstat()
    fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
    if (any(getattr(before, key) != getattr(item, key) for key in fields for item in (opened, after))
            or len(body) != before.st_size):
        return None
    return hashlib.sha256(body).hexdigest()


def _eligible_name(name):
    return (isinstance(name, str) and name.startswith('_dcar_')
            and name.endswith('.account_intake_code_successor'))


def _registry():
    # Identical reviewed helper bytes imported under v8 or private packages must
    # borrow the same request. Different helper bytes never share a registry.
    name = '_dcar_pure_normalization_registry_' + _HELPER_SHA
    _imp.acquire_lock()
    try:
        registry = sys.modules.get(name)
        if registry is None:
            registry = types.ModuleType(name)
            registry.implementation_sha = _HELPER_SHA
            registry.current = ContextVar(name, default=None)
            registry.finder = _Finder()
            sys.modules[name] = registry
        if (type(registry) is not types.ModuleType or registry.implementation_sha != _HELPER_SHA
                or not isinstance(registry.current, ContextVar)):
            raise RuntimeError('pure normalizer implementation registry differs')
        return registry
    finally:
        _imp.release_lock()


def _request():
    value = _registry().current.get()
    return value if value is not None and value['owner'] == threading.get_ident() else None


def request_stats():
    """Diagnostics only; no cache values or validity decisions are exposed."""
    state = _request()
    return None if state is None else dict(state['stats'])


def _same_policy(namespace):
    return all(type(namespace.get(name)) is type(expected) and namespace[name] == expected
               for name, expected in _POLICY_CONSTANTS.items())


def _global_inputs(function, overrides=None):
    # co_names includes attributes as well as LOAD_GLOBAL names. Checking the
    # superset is conservative: an irrelevant rebinding can only lose a hit.
    names = set()
    def collect(code):
        names.update(code.co_names)
        for constant in code.co_consts:
            if isinstance(constant, types.CodeType):
                collect(constant)
    collect(function.__code__)
    namespace, builtins = function.__globals__, function.__builtins__
    overrides = overrides or {}
    return tuple((namespace, builtins, name, overrides.get(name, namespace.get(name, builtins.get(name, _MISSING))))
                 for name in names)


def _guard(binding):
    namespace = binding['namespace']
    if namespace.get('__file__') != str(binding['path']) or not _same_policy(namespace):
        return False
    for name, (function, code) in binding['dependencies'].items():
        if (namespace.get(name) is not function or function.__code__ is not code
                or function.__defaults__ is not None or function.__kwdefaults__ is not None
                or function.__closure__ is not None):
            return False
    for name, wrapper in binding['wrappers'].items():
        if namespace.get(name) is not wrapper:
            return False
    if any(globals_.get(name, builtins.get(name, _MISSING)) is not expected
           for globals_, builtins, name, expected in binding['global_inputs']):
        return False
    if any(getattr(module, name, _MISSING) is not expected for module, name, expected in _STDLIB_INPUTS):
        return False
    return namespace.get('ast') is ast and namespace.get('re') is re


def _initial_globals_valid(namespace, functions):
    # The same source SHA can occur in different isolated namespaces. Their
    # initial semantic inputs must match the reviewed policy, not merely remain
    # unchanged after registration, or one altered namespace could seed a
    # representation later reused by a clean namespace.
    modules = {'ast': ast, 're': re, 'json': json, 'hashlib': hashlib}
    for function in functions:
        for globals_, _builtins, name, value in _global_inputs(function):
            if value is _MISSING:
                continue
            if globals_ is namespace and (name in _CODE_PINS or name in _POLICY_CONSTANTS or name == 'digest'):
                continue  # Bound separately to exact code or policy constants.
            if name in modules and value is modules[name]:
                continue
            if name in _BUILTINS and value is _BUILTINS[name]:
                continue
            return False
    return True


def _wrap(name, original, binding):
    original_code = original.__code__

    @wraps(original)
    def wrapped(*args, **kwargs):
        state = _request()
        if state is None:
            return original(*args, **kwargs)
        body = args[0] if len(args) == 1 and not kwargs else kwargs.get('body') if not args and set(kwargs) == {'body'} else None
        if (type(body) is not bytes or original.__code__ is not original_code
                or original.__defaults__ is not None or original.__kwdefaults__ is not None
                or original.__closure__ is not None or not _guard(binding)):
            state['stats']['bypasses'] += 1
            return original(*args, **kwargs)
        try:
            verifier_sha = _fresh_verifier(binding['path'])
        except (OSError, ValueError):
            verifier_sha = None
        state['stats']['verifier_reads'] += 1
        if verifier_sha != _VERIFIER_SHA:
            state['stats']['bypasses'] += 1
            return original(*args, **kwargs)
        key = (_POLICY_VERSION, verifier_sha, name, hashlib.sha256(body).hexdigest(), len(body))
        if key in state['cache']:
            kind, value, _size = state['cache'][key]
            state['cache'].move_to_end(key)
            state['stats']['hits'] += 1
            return list(value) if kind == 'list' else value
        state['stats']['computations'] += 1
        result = original(*args, **kwargs)
        if type(result) is str:
            kind, value = 'str', result
        elif type(result) is list and all(type(item) is str for item in result):
            kind, value = 'list', tuple(result)
        else:
            return result
        size = len(value.encode()) if kind == 'str' else sum(len(item.encode()) for item in value)
        if size <= _MAX_RESULT_BYTES:
            while state['cache'] and (len(state['cache']) >= _MAX_ENTRIES
                                     or state['bytes'] + size > _MAX_RESULT_BYTES):
                _key, (_kind, _value, old_size) = state['cache'].popitem(last=False)
                state['bytes'] -= old_size
            state['cache'][key] = kind, value, size
            state['bytes'] += size
        return result
    return wrapped


def _register_module_unlocked(module):
    """Register only exact source/function/policy matches; otherwise do nothing."""
    if not isinstance(module, types.ModuleType) or not _eligible_name(module.__name__):
        return False
    namespace = vars(module)
    if _MARKER in namespace:
        binding = namespace[_MARKER]
        return (isinstance(binding, dict) and binding.get('implementation_sha') == _HELPER_SHA
                and binding.get('namespace') is namespace)
    try:
        path = Path(namespace['__file__'])
        if path.name != 'account_intake_code_successor.py' or _fresh_verifier(path) != _VERIFIER_SHA:
            return False
        if not _same_policy(namespace) or namespace.get('ast') is not ast or namespace.get('re') is not re:
            return False
        dependencies = {}
        for name, expected in _CODE_PINS.items():
            function = namespace.get(name)
            if (not isinstance(function, types.FunctionType) or function.__globals__ is not namespace
                    or Path(function.__code__.co_filename) != path or _code_digest(function.__code__) != expected
                    or function.__defaults__ is not None or function.__kwdefaults__ is not None
                    or function.__closure__ is not None):
                return False
            if name not in _NORMALIZERS:
                dependencies[name] = function, function.__code__
        digest = namespace.get('digest')
        if (not isinstance(digest, types.FunctionType) or _code_digest(digest.__code__) != _DIGEST_PIN
                or digest.__defaults__ is not None or digest.__kwdefaults__ is not None
                or digest.__closure__ is not None
                or digest.__globals__.get('json') is not json
                or digest.__globals__.get('hashlib') is not hashlib):
            return False
        dependencies['digest'] = digest, digest.__code__
        binding = {'implementation_sha': _HELPER_SHA, 'namespace': namespace,
                   'path': path, 'dependencies': dependencies, 'wrappers': {}, 'global_inputs': ()}
        originals = tuple(namespace[name] for name in _CODE_PINS) + (digest,)
        if not _initial_globals_valid(namespace, originals):
            return False
        for name in _NORMALIZERS:
            binding['wrappers'][name] = _wrap(name, namespace[name], binding)
        binding['global_inputs'] = tuple(item for function in originals
            for item in _global_inputs(function, binding['wrappers'] if function.__globals__ is namespace else None))
        # No yield or external callback occurs during the small publication.
        namespace.update(binding['wrappers'])
        namespace[_MARKER] = binding
        return True
    except (KeyError, OSError, ValueError, TypeError, AttributeError):
        return False


def _register_module(module):
    # A concurrent request may scan an already loaded ancestor while its loader
    # finishes. Publish one fully prepared set of wrappers, never a half-bound
    # cache guard or wrappers wrapped a second time.
    _imp.acquire_lock()
    try:
        return _register_module_unlocked(module)
    finally:
        _imp.release_lock()


class _Loader:
    def __init__(self, original):
        self.original = original

    def create_module(self, spec):
        return self.original.create_module(spec)

    def exec_module(self, module):
        self.original.exec_module(module)
        if _request() is not None:
            _register_module(module)

    def __getattr__(self, name):
        return getattr(self.original, name)


class _Finder:
    def find_spec(self, fullname, path=None, target=None):
        if not _eligible_name(fullname) or _request() is None:
            return None
        # Insert immediately before the standard PathFinder. Earlier custom
        # finders retain priority; if ordering changes, leave all resolution to
        # the original chain instead of bypassing another finder or loader.
        try:
            position = sys.meta_path.index(self)
            if sys.meta_path[position + 1] is not importlib.machinery.PathFinder:
                return None
        except (ValueError, IndexError):
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is not None and type(spec.loader) is importlib.machinery.SourceFileLoader:
            spec.loader = _Loader(spec.loader)
        return spec


@contextmanager
def normalization_request():
    """One request; same-thread nested callers and helper namespaces borrow it."""
    registry = _registry()
    if _request() is not None:
        yield
        return
    state = {'owner': threading.get_ident(), 'cache': OrderedDict(), 'bytes': 0,
             'stats': {'hits': 0, 'computations': 0, 'bypasses': 0, 'verifier_reads': 0}}
    token = registry.current.set(state)
    try:
        _imp.acquire_lock()
        try:
            if registry.finder not in sys.meta_path:
                try:
                    position = sys.meta_path.index(importlib.machinery.PathFinder)
                except ValueError:
                    position = len(sys.meta_path)
                sys.meta_path.insert(position, registry.finder)
            for name, module in list(sys.modules.items()):
                if _eligible_name(name):
                    _register_module(module)
        finally:
            _imp.release_lock()
        yield
    finally:
        state['cache'].clear()
        state['bytes'] = 0
        registry.current.reset(token)
