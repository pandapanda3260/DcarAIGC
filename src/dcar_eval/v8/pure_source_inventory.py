"""Reuse two exact ancestor inventories within one fully observed preparation.

This is a file-manifest optimization, never an inheritance/permission cache.
The first call runs the original inventory. Every hit runs the original guarded
Git status/list/status and checks every file and directory generation twice.
All those generations join the preparation's existing final observation fence.
Unknown sources or implementations retain the original path.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from copy import deepcopy
from functools import wraps
import _imp
import builtins
import hashlib
import importlib
import importlib.machinery
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import threading
import types

from .pure_source_normalization import _code_digest, _fresh_verifier, _global_inputs

_HELPER_SHA = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
_MARKER = '_dcar_request_inventory_v1'
# Entire reviewed source-tree contents, excluding only the relocatable root.
# They are supplied by the already hash-bound receipt dependency graph.
_REVIEWED_MANIFESTS = frozenset({
    'fbc12620cd87d59c2b8c8c0fc0bc05cbff3813e5335967064fa70b20cb15a48c',  # intake v9
    '545ba927c9438bf13dc9a7eacd8456db8a7693f95ede57c45ceddf088e00844f',  # intake v3
})
_INVENTORY_MODULES = {
    '625325c2079b8dd33024f56c08111d2e2ecf8f2210e6bc4a196b50fbc44ecf62':
        '057e295e099ae9ac416f9087ba4f70084085a4ec004f0bbabc3dc66a96b754d6',
    'eccf1c1ae6d99892670685ef88d326b4b03941224f0f4619ff93e8d87195f1c6':
        'bbc059251954cca016b9c47725b3d263da4909b151809b1e2289ca17bfdc7918',
}
_RAW_MODULES = frozenset({
    '46d2a1c3fc0082978a064d0c637d6abf10ad23ce4afaf00107c98ee08382cef1',
    '21aabea53952be26c23fd6331a957e7dd959b225ca54a4476e43937a52096067',
})
_GIT_MODULES = frozenset({
    '1f866a730cba9bd195df70e26d4f319af9fe600c863745d32cee9219700634e2',
    '2a2be04ae5141591333fab2bfa38adfa294a6ca62bbf39635418a3eb8f0ce0bb',
})
_FUNCTION_PINS = {
    'intake_require': 'b425274b368c92c1f09f67dbd89895bf2f162cee81f927a10a423591541ee0b1',
    'raw': 'a98e88b9e392827144158484cd1ccd21205875b6ef28556ef349058c38c5b563',
    'raw_require': 'd76dea7caeecc00f59a596b87f545d3e451f90cdd26bf01504f9c5ccfca4d754',
    'verified_git': '39de3c97c289306103257aebc6bb86b7b9b603daf090d23ae9214009159f2e89',
}
_STATUS = ('status', '--porcelain=v1', '--untracked-files=all')
_LIST = ('ls-files', '--cached', '--others', '--exclude-standard', '-z')
_BUILTINS = dict(vars(builtins))
_MISSING = object()
_MODULE_INPUTS = {'hashlib': hashlib, 'os': os, 'stat': stat, 're': re,
                  'subprocess': subprocess, 'Path': Path}
_STDLIB_INPUTS = tuple((module, name, getattr(module, name)) for module, names in (
    (builtins, ('__import__',)),
    (hashlib, ('sha256',)), (os, ('fsdecode', 'getenv', 'get_exec_path', 'geteuid',
        'open', 'fdopen', 'fstat', 'scandir', 'lstat', 'stat')),
    (stat, ('S_IMODE', 'S_ISREG', 'S_ISDIR', 'S_ISLNK')),
    (subprocess, ('run', 'Popen', 'PIPE')), (re, ('fullmatch',)),
) for name in names)


class InventoryChanged(ValueError):
    pass


class _Ineligible(ValueError):
    pass


def _require(condition, message):
    if not condition:
        raise InventoryChanged('ancestor inventory: ' + message)


def _manifest_digest(manifest):
    return hashlib.sha256(json.dumps({key: value for key, value in manifest.items()
        if key != 'source_root'}, ensure_ascii=False, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()


def _eligible_name(name):
    return (isinstance(name, str) and name.startswith('_dcar_')
            and name.endswith('.account_intake_release'))


def _registry():
    name = '_dcar_inventory_registry_' + _HELPER_SHA
    _imp.acquire_lock()
    try:
        registry = sys.modules.get(name)
        if registry is None:
            registry = types.ModuleType(name)
            registry.implementation_sha = _HELPER_SHA
            registry.current = ContextVar(name, default=None)
            registry.finder = _Finder()
            sys.modules[name] = registry
        _require(type(registry) is types.ModuleType and registry.implementation_sha == _HELPER_SHA
                 and isinstance(registry.current, ContextVar), 'implementation registry differs')
        return registry
    finally:
        _imp.release_lock()


def _request():
    state = _registry().current.get()
    return state if state is not None and state['owner'] == threading.get_ident() else None


def request_stats():
    state = _request()
    return None if state is None else dict(state['stats'])


def _full_fence(root, generation):
    """Include ignored files and directories; never follow another namespace."""
    if not root.is_absolute() or root.resolve(strict=True) != root:
        raise _Ineligible('noncanonical root')
    result = {}
    todo = [root]
    while todo:
        if len(result) >= 100_000:
            raise _Ineligible('source dependency set is not bounded')
        path = todo.pop()
        value = generation(path)
        if value == ('missing',):
            raise _Ineligible('missing path or symbolic target')
        if stat.S_ISLNK(value[3]):
            if ('.git' in path.relative_to(root).parts
                    # _generation contains two nine-field stat identities;
                    # st_mode is field three in each identity.
                    or not (stat.S_ISREG(value[12]) or stat.S_ISDIR(value[12]))):
                raise _Ineligible('unsafe symbolic dependency')
            # An ignored symlink is a leaf to Git. Qualification against the
            # complete Git list and the bound manifest happens before caching.
            result[path] = value
            continue
        if not (stat.S_ISREG(value[3]) or stat.S_ISDIR(value[3])):
            raise _Ineligible('nonregular source dependency')
        result[path] = value
        if stat.S_ISDIR(value[3]):
            with os.scandir(path) as entries:
                todo.extend(path / entry.name for entry in entries)
    git = root / '.git'
    if git not in result or not stat.S_ISDIR(result[git][3]):
        raise _Ineligible('Git directory is not independent')
    for relative in ('.git/commondir', '.git/gitdir', '.git/objects/info/alternates',
                     '.git/objects/info/http-alternates', '.gitmodules', '.git/config.worktree',
                     '.git/worktrees', '.git/modules'):
        if root / relative in result:
            raise _Ineligible('external Git structure')
    if any(path.name == '.git' and path != git for path in result):
        raise _Ineligible('nested repository')
    return result


def _opaque_leaves_excluded(root, fence, names, manifest):
    members = [root / name for name in names]
    members.extend(root / row['path'] for row in manifest['files'])
    return all(not any(member == leaf or member.is_relative_to(leaf) for member in members)
               for leaf, value in fence.items() if stat.S_ISLNK(value[3]))


def _merge_observed(state, fence):
    observed = state['observed']
    for path, value in fence.items():
        _require(path not in observed or observed[path] == value,
                 'dependency changed before inventory')
        observed[path] = value


def _git_identity(generation):
    executable = shutil.which('git')
    if executable is None:
        raise _Ineligible('Git executable unavailable')
    path = Path(os.path.abspath(executable))
    resolved = path.resolve(strict=True)
    # Bind selection and symlink components, not only the last inode. The OS
    # and Git installation remain trusted in exactly the original execution.
    paths = {path, resolved, *path.parents, *resolved.parents}
    return str(path), str(resolved), tuple(sorted((str(item), generation(item)) for item in paths))


def _safe_configuration(binding, root):
    # This is additional eligibility, not a cached replacement for verified_git.
    # All subsequent commands still execute their own original config guard.
    configured = binding['git'](root, 'config', '--null', '--list')
    allowed = {'core.repositoryformatversion', 'core.filemode', 'core.bare',
               'core.logallrefupdates', 'core.ignorecase', 'core.precomposeunicode',
               'core.fsmonitor', 'user.name', 'user.email'}
    for item in configured.split(b'\0'):
        if not item:
            continue
        raw_key, _, value = item.partition(b'\n')
        key = raw_key.decode('utf-8').lower()
        if key not in allowed and not re.fullmatch(r'(remote\..+\.(url|fetch)|branch\..+\.(remote|merge))', key):
            return False
        if ((key == 'core.repositoryformatversion' and value != b'0')
                or (key == 'core.bare' and value.lower() not in {b'false', b'0', b'no', b'off'})):
            return False
    return True


def _guard(binding):
    if any(sys.modules.get(module.__name__) is not module
           for module in _MODULE_INPUTS.values() if isinstance(module, types.ModuleType)):
        return False
    for module, name, original in _STDLIB_INPUTS:
        if getattr(module, name, _MISSING) is not original:
            return False
    for module, path, expected in binding['modules']:
        if (sys.modules.get(module.__name__) is not module or vars(module).get('__file__') != str(path)
                or _fresh_verifier(path) != expected):
            return False
    for module, path, expected in binding['aliases']:
        if (sys.modules.get(module.__name__) is not module
                or module.__dict__.get('__file__') != str(path)
                or module.__dict__.get('inventory') is not binding['wrapper']
                or _fresh_verifier(path) != expected):
            return False
    for function, code, defaults, kwdefaults, package in binding['functions']:
        if (function.__code__ is not code or function.__defaults__ is not defaults
                or not _same_kwdefaults(function.__kwdefaults__, kwdefaults) or function.__closure__ is not None
                or function.__builtins__.get('__import__') is not _BUILTINS['__import__']
                or function.__globals__.get('__package__') != package):
            return False
    if any(namespace.get(name, builtins_.get(name, _MISSING)) is not expected
           for namespace, builtins_, name, expected in binding['global_inputs']):
        return False
    return (binding['namespace'].get('inventory') is binding['wrapper']
            and binding['git_module'].verified_git is binding['git'])


def _use(binding, root):
    state = _request()
    original = binding['original']
    if state is None or not isinstance(root, Path) or root not in state['manifests']:
        return original(root)
    cached = state['cache'].get(root)
    if root in state['bypassed']:
        state['stats']['bypasses'] += 1
        return original(root)
    try:
        valid = _guard(binding) and dict(os.environ) == state['environment']
        _require(valid or cached is None, 'implementation or environment changed')
        if not valid:
            raise _Ineligible('implementation does not match')
        before = _full_fence(root, state['generation'])
        identity = _git_identity(state['generation'])
        if cached is None:
            if not _safe_configuration(binding, root):
                raise _Ineligible('Git configuration has unbound dependencies')
            names = sorted({os.fsdecode(item) for item in binding['git'](root, *_LIST).split(b'\0') if item})
            if not _opaque_leaves_excluded(root, before, names, state['manifests'][root]):
                raise _Ineligible('symbolic leaf occurs in the inventory namespace')
            result = original(root)
            _require(result == state['manifests'][root], 'first inventory differs from bound manifest')
            present = [name for name in names if (root / name).exists() or (root / name).is_symlink()]
            _require(present == [row['path'] for row in result['files']], 'first fresh Git file list differs')
            _require(_full_fence(root, state['generation']) == before, 'source changed during first inventory')
            _require(_git_identity(state['generation']) == identity, 'Git execution changed during first inventory')
            _require(_guard(binding) and dict(os.environ) == state['environment'],
                     'implementation or environment changed during first inventory')
            _merge_observed(state, before)
            state['cache'][root] = {'manifest': deepcopy(result), 'fence': before,
                                    'git_identity': identity, 'names': names}
            state['stats']['computations'] += 1
            return deepcopy(result)
        _require(before == cached['fence'], 'source changed before reused inventory')
        _require(identity == cached['git_identity'], 'Git execution identity changed')
        _merge_observed(state, before)
        first = binding['git'](root, *_STATUS)
        listed = binding['git'](root, *_LIST)
        last = binding['git'](root, *_STATUS)
        _require(first == last and hashlib.sha256(first).hexdigest()
                 == cached['manifest']['git']['status_porcelain_sha256'], 'fresh Git status differs')
        names = sorted({os.fsdecode(item) for item in listed.split(b'\0') if item})
        _require(names == cached['names'], 'fresh complete Git file list differs')
        _require(_opaque_leaves_excluded(root, before, names, cached['manifest']),
                 'symbolic leaf entered the inventory namespace')
        # The original inventory omits tracked paths which are presently absent.
        actual = [name for name in names if (root / name).exists() or (root / name).is_symlink()]
        _require(actual == [row['path'] for row in cached['manifest']['files']], 'fresh Git file list differs')
        _require(_full_fence(root, state['generation']) == before, 'source changed during reused inventory')
        _require(_git_identity(state['generation']) == identity, 'Git execution changed during reused inventory')
        _require(_guard(binding) and dict(os.environ) == state['environment'],
                 'implementation or environment changed during reused inventory')
        state['stats']['hits'] += 1
        return deepcopy(cached['manifest'])
    except (_Ineligible, OSError):
        _require(cached is None, 'reused inventory became ineligible')
        state['bypassed'].add(root)
        state['stats']['bypasses'] += 1
        return original(root)


def _same_kwdefaults(actual, expected):
    return actual is None if expected is None else (type(actual) is dict
        and actual.keys() == expected.keys() and all(actual[key] is value for key, value in expected.items()))


def _function(function, path, expected, *, kwdefaults=None):
    if (not isinstance(function, types.FunctionType) or Path(function.__code__.co_filename) != path
            or _code_digest(function.__code__) != expected or function.__defaults__ is not None
            or not _same_kwdefaults(function.__kwdefaults__, kwdefaults) or function.__closure__ is not None):
        raise _Ineligible('function code differs')
    if function.__builtins__.get('__import__') is not _BUILTINS['__import__']:
        raise _Ineligible('function import binding differs')
    return function, function.__code__, None, deepcopy(kwdefaults), function.__globals__.get('__package__')


def _register_module_unlocked(module):
    if not isinstance(module, types.ModuleType) or not _eligible_name(module.__name__):
        return False
    namespace = vars(module)
    if _MARKER in namespace:
        return namespace[_MARKER].get('implementation_sha') == _HELPER_SHA
    try:
        path = Path(namespace['__file__'])
        sha = _fresh_verifier(path)
        if sha not in _INVENTORY_MODULES:
            return False
        package = namespace['__package__']
        raw_module = sys.modules[package + '.account_classification_release']
        raw_path = Path(raw_module.__file__)
        raw_sha = _fresh_verifier(raw_path)
        git_path = path.with_name('runtime_paths.py')
        git_sha = _fresh_verifier(git_path)
        if raw_sha not in _RAW_MODULES or git_sha not in _GIT_MODULES:
            return False
        git_module = importlib.import_module(package + '.runtime_paths')
        original, raw = namespace['inventory'], namespace['raw']
        if (original.__globals__ is not namespace or raw is not raw_module.raw
                or raw.__globals__ is not vars(raw_module)
                or namespace['require'].__globals__ is not namespace
                or raw_module.require.__globals__ is not vars(raw_module)
                or git_module.verified_git.__globals__ is not vars(git_module)):
            return False
        functions = [
            _function(original, path, _INVENTORY_MODULES[sha]),
            _function(namespace['require'], path, _FUNCTION_PINS['intake_require']),
            _function(raw, raw_path, _FUNCTION_PINS['raw'], kwdefaults={'private': True}),
            _function(raw_module.require, raw_path, _FUNCTION_PINS['raw_require']),
            _function(git_module.verified_git, git_path, _FUNCTION_PINS['verified_git']),
        ]
        permitted = {id(item[0]) for item in functions}
        inputs = tuple(item for function, *_ in functions for item in _global_inputs(function)
                       if item[2] in item[0] or item[2] in item[1])
        for globals_, builtins_, name, value in inputs:
            if value is _MISSING or name not in globals_ and name not in builtins_:
                continue
            if (id(value) not in permitted and value is not _MODULE_INPUTS.get(name, _MISSING)
                    and value is not _BUILTINS.get(name, _MISSING)):
                return False
        binding = {'implementation_sha': _HELPER_SHA, 'namespace': namespace,
            'original': original, 'git': git_module.verified_git, 'git_module': git_module,
            'modules': ((module, path, sha), (raw_module, raw_path, raw_sha), (git_module, git_path, git_sha)),
            'functions': functions, 'global_inputs': inputs, 'aliases': []}

        @wraps(original)
        def wrapped(*args, **kwargs):
            if len(args) == 1 and not kwargs:
                return _use(binding, args[0])
            if not args and set(kwargs) == {'source'}:
                return _use(binding, kwargs['source'])
            return original(*args, **kwargs)

        binding['wrapper'] = wrapped
        namespace['inventory'] = wrapped
        namespace[_MARKER] = binding
        return True
    except (KeyError, OSError, ValueError, TypeError, AttributeError):
        return False


def _register_module(module):
    _imp.acquire_lock()
    try:
        return _register_module_unlocked(module)
    finally:
        _imp.release_lock()


def _register_flow_alias(module, state):
    """Repair one explicit preloaded from-import alias; never sweep globals."""
    if (not isinstance(module, types.ModuleType) or not module.__name__.startswith('_dcar_')
            or not module.__name__.endswith('.four_platform_flow_release')):
        return False
    try:
        namespace = vars(module)
        package = namespace['__package__']
        if (package != module.__name__.rsplit('.', 1)[0]
                or sys.modules.get(module.__name__) is not module):
            return False
        origin = sys.modules[package + '.account_intake_release']
        binding = vars(origin).get(_MARKER)
        if (not binding or binding['implementation_sha'] != _HELPER_SHA
                or namespace.get('inventory') is not binding['original']):
            return False
        path = Path(namespace['__file__'])
        if path != Path(origin.__file__).with_name('four_platform_flow_release.py'):
            return False
        tree = state['input_manifests'].get(path.parents[3])
        if tree is None:
            return False
        expected = next((row['sha256'] for row in tree['files']
                         if row['path'] == 'src/dcar_eval/v8/four_platform_flow_release.py'), None)
        if expected is None or _fresh_verifier(path) != expected:
            return False
        namespace['inventory'] = binding['wrapper']
        binding['aliases'].append((module, path, expected))
        return True
    except (KeyError, OSError, ValueError, TypeError, AttributeError):
        return False


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
        from . import pure_source_normalization as normalization
        try:
            following = sys.meta_path[sys.meta_path.index(self) + 1:]
            until_pathfinder = following[:following.index(importlib.machinery.PathFinder)]
            # The existing normalizer only handles code_successor, a disjoint
            # name. Keep it immediately before PathFinder so both adapters run.
            if any(finder is not normalization._registry().finder for finder in until_pathfinder):
                return None
        except (ValueError, IndexError):
            return None
        spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
        if spec is not None and type(spec.loader) is importlib.machinery.SourceFileLoader:
            spec.loader = _Loader(spec.loader)
        return spec


@contextmanager
def inventory_request(manifests, observed, *, generation):
    """Bound manifests and the exact enclosing proof's final observation map."""
    registry = _registry()
    existing = _request()
    if existing is not None:
        _require(existing['observed'] is observed and existing['generation'] is generation
                 and existing['input_manifests'] is manifests, 'nested preparation binding differs')
        yield
        return
    eligible = {root: deepcopy(manifest) for root, manifest in manifests.items()
                if _manifest_digest(manifest) in _REVIEWED_MANIFESTS
                and manifest.get('source_root') == str(root)}
    state = {'owner': threading.get_ident(), 'input_manifests': manifests,
        'manifests': eligible, 'observed': observed, 'generation': generation,
        'environment': dict(os.environ), 'cache': {}, 'bypassed': set(),
        'stats': {'computations': 0, 'hits': 0, 'bypasses': 0}}
    token = registry.current.set(state)
    try:
        if eligible:
            _imp.acquire_lock()
            try:
                from . import pure_source_normalization as normalization
                if registry.finder not in sys.meta_path:
                    normal_finder = normalization._registry().finder
                    target = normal_finder if normal_finder in sys.meta_path else importlib.machinery.PathFinder
                    sys.meta_path.insert(sys.meta_path.index(target), registry.finder)
                for name, module in list(sys.modules.items()):
                    if _eligible_name(name):
                        _register_module(module)
                for module in list(sys.modules.values()):
                    _register_flow_alias(module, state)
            finally:
                _imp.release_lock()
        yield
    finally:
        state['cache'].clear()
        registry.current.reset(token)
