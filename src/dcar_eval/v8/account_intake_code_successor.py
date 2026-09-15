"""A local schema22 planning fix inherits the immutable installed intake release.

The parent migration and operation authority keep their original source binding.
Only a separately checked, bounded code delta may run under that authority.
No migration, provider request, gate issuance or database write is performed.
"""
from __future__ import annotations

import ast
from contextlib import contextmanager
from datetime import datetime
import importlib
from pathlib import Path
import re
import sys
import types
from typing import Any, Mapping
from uuid import uuid4

from .account_classification_release import digest, object_at, payload_at, raw, records, reference

CONTRACT = 'account-intake-planning-code-successor-v1'
CHECK_CONTRACT = 'account-intake-planning-code-check-v1'
FIELD = 'account_intake_code_successor'
MODULE = 'src/dcar_eval/v8/account_intake_code_successor.py'
ENTRY = 'src/dcar_eval/v8/account_intake_release.py'
PLANNING = 'src/dcar_eval/v8/account_preparation.py'
RUNTIME = 'src/dcar_eval/v8/capture_runtime.py'
CAPTURE = 'src/dcar_eval/v8/capture.py'
PIPELINE = 'src/dcar_eval/v8/pipeline.py'
INTAKE = 'src/dcar_eval/v8/account_intake.py'
ADAPTERS = 'src/dcar_eval/v8/platform_adapters.py'
DISPLAY_FILES = frozenset({'app/web/app/accounts/AccountsPage.tsx',
    'app/web/app/accounts/CreateAccountDialog.tsx', 'app/web/tests/account-create.test.mjs'})
CLI_FILES = frozenset({'scripts/install_account_intake.py', 'scripts/import_installed_account_summary.py'})
ALLOWED_FILES = frozenset({MODULE, ENTRY, PLANNING, RUNTIME, CAPTURE, PIPELINE, INTAKE, ADAPTERS,
    'scripts/prepare_account_intake_code_successor.py',
    'tests/test_account_intake_code_successor.py',
    'tests/test_v8_preparation_planning_cache.py',
    'tests/test_v8_periodic_fetch_recovery.py', 'tests/test_v8_account_intake.py',
    'tests/test_v8_preparation_profile_reuse.py',
    'tests/test_v8_capture_rolling_scheduler.py', 'tests/test_v8_wechat_mixed_search.py',
    'tests/test_v8_resolver_parser_replay.py', 'tests/test_v8_unsent_admission_refresh.py',
    'tests/test_v8_preparation_never_sent_recovery.py'}) | CLI_FILES | DISPLAY_FILES
CHECKS = frozenset({'planning_behavior', 'planning_successor'})
PLANNING_FUNCTIONS = frozenset({'validate_paid_target', 'enqueue_pending',
    '_planning_cache', '_planning_validation', '_validated_plan', 'planning_reconsideration',
    '_reuse_existing_profile'})
# These added helpers are compared in full, not masked by function name.
PREPARATION_REPAIR_HELPERS = {
    '_load_resolver_parser_replay': '27d63c8ea3b2745a036043293b8055588137460b4ce9508d21d7dabbd7808ee9',
    '_never_sent_reservation_evidence': 'efee5f694a018aced22d7d8eab74b60c7e5b9eccdbfae451a9d4563400fd0109',
    '_recover_never_sent_preparation': '8dec1f6b50da57923a45485a54cb65a84a3a0734e8a6de22581a85f2e746f181',
}
NEVER_SENT_PLANNER_AST = '0d20ffaa16bb48db70786f5e39c5fdd6cdd0a8db127a107aba8c2405413ed618'


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError('Intake planning successor: ' + message)


def repair_scope(changes: Mapping[str, Any]) -> str:
    recovery = {CAPTURE, PIPELINE} & set(changes)
    require(not recovery or recovery == {CAPTURE, PIPELINE}, 'periodic recovery requires both exact entry changes')
    if INTAKE in changes or DISPLAY_FILES & set(changes):
        return 'local_account_preparation_queue_and_display_with_inherited_repairs'
    return 'local_planning_and_existing_slot_recovery_only' if recovery else 'local_planning_performance_only'


def _dump(node: ast.AST) -> str:
    return ast.dump(node, include_attributes=False)


def _statement(source: str) -> ast.stmt:
    return ast.parse(source).body[0]


def _stable_capture_nodes(body: bytes) -> str:
    module = ast.parse(body)
    _normalize_admission_refresh(module)
    function = next(node for node in module.body if isinstance(node, ast.FunctionDef)
        and node.name == 'recover_stale_fetch_slots')
    matches = [index for index, argument in enumerate(function.args.kwonlyargs)
        if argument.arg == 'preserve_live_owners']
    if not matches:
        return _dump(module)
    index = matches[0]
    require(len(matches) == 1 and index == len(function.args.kwonlyargs)-1 and
        _dump(function.args.kwonlyargs[index]) == _dump(ast.arg(arg='preserve_live_owners', annotation=ast.Name(id='bool',ctx=ast.Load()))) and
        _dump(function.args.kw_defaults[index]) == _dump(ast.Constant(value=False)), 'periodic recovery default changed')
    del function.args.kwonlyargs[index]; del function.args.kw_defaults[index]
    rows = [node for node in ast.walk(function) if isinstance(node,ast.Assign)
        and len(node.targets)==1 and isinstance(node.targets[0],ast.Name) and node.targets[0].id=='rows']
    require(len(rows)==1, 'periodic recovery candidate assignment changed')
    outer = rows[0].value
    require(isinstance(outer,ast.Call) and not outer.args and not outer.keywords and
        isinstance(outer.func,ast.Attribute) and outer.func.attr=='fetchall', 'periodic recovery candidate handling changed')
    execute = outer.func.value
    require(isinstance(execute,ast.Call) and not execute.keywords and len(execute.args)==2 and
        _dump(execute.func)==_dump(ast.parse('connection.execute',mode='eval').body), 'periodic recovery candidate execution changed')
    query, parameters = execute.args
    condition = _dump(ast.parse('not preserve_live_owners',mode='eval').body)
    require(isinstance(query,ast.IfExp) and isinstance(parameters,ast.IfExp) and
        _dump(query.test)==condition==_dump(parameters.test), 'periodic recovery live-owner guard changed')
    approved_sql = """SELECT id FROM fetch_slots
        WHERE status='running' AND COALESCE(started_at,updated_at) < ?
        AND NOT EXISTS (SELECT 1 FROM paid_provider_dispatch_events d
        JOIN scheduler_run_attempts a ON a.id=d.scheduler_attempt_id
        AND a.scheduler_run_id=d.scheduler_run_id
        WHERE d.fetch_slot_id=fetch_slots.id AND a.status='running'
        AND a.owner_token IS NOT NULL
        AND julianday(a.lease_expires_at)>=julianday(?)) ORDER BY id"""
    tokens = lambda sql: re.findall(r"'(?:''|[^'])*'|[A-Za-z_]\w*|>=|<=|<>|!=|[^\s]", sql)
    require(isinstance(query.orelse,ast.Constant) and isinstance(query.orelse.value,str) and
        tokens(query.orelse.value)==tokens(approved_sql) and
        _dump(parameters.orelse)==_dump(ast.parse('(cutoff_at, captured_at)',mode='eval').body),
        'periodic recovery exact live-owner selection changed')
    execute.args = [query.body,parameters.body]
    return _dump(module)


def _normalize_admission_refresh(module: ast.Module) -> None:
    """Only released-unsent reservations may refresh their current amount/day."""
    functions = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == '_claim_paid_tikhub']
    require(len(functions) == 1, 'periodic recovery: original paid claim function changed')
    function = functions[0]
    calls = [node for node in ast.walk(function) if isinstance(node, ast.Call) and node.args
             and _dump(node.func) == _dump(ast.parse('connection.execute', mode='eval').body)
             and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str)
             and 'INSERT INTO admission_reservations' in node.args[0].value]
    require(len(calls) == 1, 'periodic recovery: admission reservation statement changed')
    original = """INSERT INTO admission_reservations(batch_id,state,amount_microusd,
        charge_business_day,created_at,expires_at,updated_at) VALUES(?,'reserved_unsent',?,?,?,?,?)
        ON CONFLICT(batch_id) DO UPDATE SET state='reserved_unsent',updated_at=excluded.updated_at,
        expires_at=excluded.expires_at WHERE admission_reservations.state='released_unsent'"""
    approved = original.replace("state='reserved_unsent',updated_at=excluded.updated_at,",
        "state='reserved_unsent',updated_at=excluded.updated_at,"
        'amount_microusd=excluded.amount_microusd,charge_business_day=excluded.charge_business_day,')
    tokens = lambda sql: re.findall(r"'(?:''|[^'])*'|[A-Za-z_]\w*|>=|<=|<>|!=|[^\s]", sql)
    query = calls[0].args[0]
    require(tokens(query.value) in (tokens(original), tokens(approved)),
            'periodic recovery: admission refresh changed state guard, identity or approved fields')
    query.value = 'reviewed_released_unsent_admission_refresh'


def _stable_pipeline_nodes(body: bytes) -> str:
    module = ast.parse(body)
    function = next(node for node in module.body if isinstance(node,ast.FunctionDef) and node.name=='_capture_v25_job')
    for call in ast.walk(function):
        if not isinstance(call,ast.Call) or _dump(call.func) != _dump(ast.parse('capture_runtime.run_ready',mode='eval').body):
            continue
        rolling = [item for item in call.keywords if item.arg == 'rolling']
        if rolling:
            require(len(rolling)==1 and _dump(rolling[0].value)==_dump(ast.parse('schema_version == 22',mode='eval').body),
                    'rolling execution must retain the schema22 gate')
            call.keywords.remove(rolling[0])
    connected = next(node for node in function.body if isinstance(node,ast.With))
    declaration = _statement('schema_version = connection.execute("PRAGMA user_version").fetchone()[0]')
    if _dump(connected.body[0]) != _dump(declaration):
        return _dump(module)
    check = connected.body[1]
    require(isinstance(check,ast.If) and _dump(check.test)==_dump(ast.parse('schema_version not in {20,21,22}',mode='eval').body),
        'periodic recovery schema gate changed')
    check.test.left = declaration.value
    del connected.body[0]
    branches = [node for node in ast.walk(function) if isinstance(node,ast.If) and
        _dump(node.test)==_dump(ast.parse('kind == "maintenance"',mode='eval').body)]
    require(len(branches)==1, 'periodic recovery maintenance branch changed')
    branch = branches[0]
    before = _statement('''if schema_version == 22:
    from .capture import recover_stale_fetch_slots
    recovered_fetch_slots = recover_stale_fetch_slots(db_path=db_path,
        current_time=datetime.fromisoformat(timestamp.replace("Z", "+00:00")), preserve_live_owners=True)''')
    after = _statement('''if schema_version == 22:
    result["recovered_fetch_slots"] = recovered_fetch_slots''')
    require(len(branch.body)>=3 and _dump(branch.body[0])==_dump(before) and
        _dump(branch.body[1])==_dump(_statement('result = capture_quality.maintenance_tick(db_path=db_path, at=timestamp)')) and
        _dump(branch.body[2])==_dump(after), 'periodic recovery call, transaction position or live protection changed')
    del branch.body[2]; del branch.body[0]
    return _dump(module)


def _stable_runtime_nodes(body: bytes) -> list[str]:
    """Allow queue ordering/refill, retaining every existing claim and send check."""
    module = ast.parse(body)
    originals = {
        'run_one': '''row = connection.execute("""SELECT id,operation,envelope_json FROM capture_work_items WHERE state='runnable'
            AND due_at<=? ORDER BY due_at,id LIMIT 1""", (planning.timestamp(at),)).fetchone()''',
        '_run_single': '''row = connection.execute("""SELECT * FROM capture_work_items WHERE state='runnable' AND due_at<=?
            """ + normal_operation + only + " ORDER BY due_at,id LIMIT 1", (planning.timestamp(at), *ids)).fetchone()''',
    }
    replacements = {
        'run_one': 'row = _select_runnable_work(connection, at)',
        '_run_single': 'row = _select_runnable_work(connection, at, filters=normal_operation + only, parameters=ids)',
    }
    helpers = {'_plan_due','run_ready','_select_runnable_work','_run_ready_one','_run_ready_rolling',
               '_has_due_runnable_work'}
    constants = {
        '_WORK_SELECTION_LANE': '_WORK_SELECTION_LANE: ContextVar[str | None] = ContextVar("capture_work_selection_lane", default=None)',
        '_RUN_READY_LANES': '_RUN_READY_LANES = ("ordinary", "douyin", "kuaishou", "xiaohongshu", "wechat_channels")',
        '_ROLLING_MAX_ITEMS': '_ROLLING_MAX_ITEMS = 16',
    }
    result = []
    for node in module.body:
        if isinstance(node,ast.FunctionDef) and node.name in helpers:
            continue
        if isinstance(node,ast.FunctionDef) and node.name in originals:
            rows = [item for item in ast.walk(node) if isinstance(item,ast.Assign) and
                len(item.targets)==1 and isinstance(item.targets[0],ast.Name) and item.targets[0].id=='row']
            require(len(rows)==1, 'runtime candidate assignment changed')
            row = rows[0]
            if _dump(row) == _dump(_statement(replacements[node.name])):
                row.value = _statement(originals[node.name]).value
            else:
                require(_dump(row)==_dump(_statement(originals[node.name])), 'runtime selection filters or due checks changed')
        if isinstance(node,(ast.Assign,ast.AnnAssign)):
            targets = node.targets if isinstance(node,ast.Assign) else [node.target]
            names = [target.id for target in targets if isinstance(target,ast.Name) and target.id in constants]
            if names:
                require(len(names)==1 and _dump(node)==_dump(_statement(constants[names[0]])),
                        'rolling concurrency or bounded selection constants changed')
                continue
        if isinstance(node,ast.ImportFrom) and node.module in {'contextvars','concurrent.futures'}:
            node.names = [alias for alias in node.names if (node.module,alias.name) not in {
                ('contextvars','ContextVar'),('concurrent.futures','wait'),('concurrent.futures','FIRST_COMPLETED')}]
            if not node.names:
                continue
        result.append(_dump(node))
    return result


def _stable_intake_nodes(body: bytes) -> str:
    """The intake delta is display text only; input, identity and state logic stay fixed."""
    module = ast.parse(body)
    text = {
        '待补齐账号资料':'待接入', '正在补齐账号资料':'正在准备',
        '账号资料补齐失败':'准备失败', '账号资料已就绪':'主页准备完成',
        '；后续按有效采集计划自动采集作品和指标，尚不表示作品抓取已完成。':'；后续按有效采集计划执行，尚不表示作品抓取已完成。',
        '：系统将自动获取主页资料并核对账号是否一致。':'：资料已接收，等待账号准备任务。',
    }
    for function in module.body:
        if isinstance(function,ast.FunctionDef) and function.name=='preparation_status':
            for item in ast.walk(function):
                if isinstance(item,ast.Constant) and isinstance(item.value,str) and item.value in text:
                    item.value = text[item.value]
    return _dump(module)


def _stable_adapter_nodes(body: bytes) -> str:
    """Remove only the exact reviewed official-account filter at its call site."""
    module = ast.parse(body)
    functions = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == '_finder_candidates']
    require(len(functions) == 1, 'platform adapter finder function changed')
    approved = _dump(_statement('''if (item.get("accTypeName") == "公众号"
        and isinstance(jump.get("userName"), str)
        and re.fullmatch(r"gh_[0-9a-f]{12}", jump["userName"])
        and notice.get("finderUsername") in (None, "")):
    continue'''))
    matches = []
    for node in ast.walk(functions[0]):
        for _, value in ast.iter_fields(node):
            if isinstance(value, list):
                matches.extend((value, index) for index, item in enumerate(value)
                               if isinstance(item, ast.If) and _dump(item) == approved)
    if matches:
        require(len(matches) == 1, 'platform adapter filter duplicated')
        block, index = matches[0]
        before = _dump(_statement('jump, notice = item.get("jumpInfo") or {}, item.get("noticeParam") or {}'))
        after = [_dump(_statement('add(jump.get("userName"))')), _dump(_statement('add(notice.get("finderUsername"))'))]
        require(index > 0 and _dump(block[index-1]) == before and
                [_dump(item) for item in block[index+1:index+3]] == after,
                'platform adapter filter position or candidate checks changed')
        del block[index]
    return _dump(module)


def _stable_retry_evidence(node: ast.FunctionDef) -> ast.FunctionDef:
    """Normalize the reviewed SELECT and exact parser replay; retain billing checks."""
    replay = _statement('''if (request["platform"] == target.get("platform") == "wechat_channels"
        and target["operation"] == "wechat_channels_resolve"
        and attempt["slot_status"] == "terminal_failed" and attempt["error_code"] == "invalid_finder_candidate"
        and raw["http_status"] == 200 and error_code is None and next_target is not None
        and next_target["operation"] == "wechat_channels_channel_info"):
    return {**base, "kind": "replay", "replay_reason": "wechat_resolver_parser_repair"}''')
    inserted = [index for index, item in enumerate(node.body) if _dump(item) == _dump(replay)]
    if inserted:
        index = inserted[0]
        predecessor = _statement('''if attempt["slot_status"] == "succeeded" and error_code is None:
    return {**base, "kind": "replay"}''')
        successor = _statement('''if (attempt["slot_status"] != "retryable_failed"
        or attempt["error_code"] not in {"provider_business_failure", "provider_rate_limited", "rate_limit_exceeded", "http_429"}
        or error_code != "provider_business_failure"):
    raise _blocked("preparation_retry_not_transient")''')
        require(len(inserted) == 1 and index > 0 and index + 1 < len(node.body)
                and _dump(node.body[index-1]) == _dump(predecessor) and _dump(node.body[index+1]) == _dump(successor),
                'non-planning or payment: parser replay position or original retry guards changed')
        del node.body[index]
        initial = _statement('error_code, next_target = None, None')
        originals = [i for i, item in enumerate(node.body) if _dump(item) == _dump(initial)]
        require(len(originals) == 1, 'non-planning or payment: parser replay error handling changed')
        node.body[originals[0]] = _statement('error_code = None')
        call = '''adapters.next_profile_request(json.loads(request["input_json"]), responses=[*_responses(connection, request),
            {"operation": target["operation"], "payload": json.loads(entity), "raw_response_id": raw["id"]}])'''
        assignment = _statement('next_target = ' + call)
        validators = [item for item in ast.walk(node) if isinstance(item, ast.Try)
                      and item.body and _dump(item.body[0]) == _dump(assignment)]
        require(len(validators) == 1, 'non-planning or payment: parser replay original validation changed')
        validators[0].body[0] = _statement(call)
    matches = [item for item in ast.walk(node) if isinstance(item, ast.Assign)
        and len(item.targets) == 1 and isinstance(item.targets[0], ast.Name)
        and item.targets[0].id == 'attempt']
    require(len(matches) == 1, 'retry attempt query assignment changed')
    outer = matches[0].value
    require(isinstance(outer, ast.Call) and not outer.args and not outer.keywords
        and isinstance(outer.func, ast.Attribute) and outer.func.attr == 'fetchone',
        'retry attempt query result handling changed')
    execute = outer.func.value
    require(isinstance(execute, ast.Call) and not execute.keywords and len(execute.args) == 2
        and isinstance(execute.func, ast.Attribute) and execute.func.attr == 'execute'
        and isinstance(execute.func.value, ast.Name) and execute.func.value.id == 'connection',
        'retry attempt query execution changed')
    query = execute.args[0]
    old_format = ast.dump(ast.parse("f\"{capture_singletons.attempt_slot_sql(connection, 'a')}\"", mode='eval').body.values[0],
        include_attributes=False)
    values = query.values if isinstance(query, ast.JoinedStr) else [query]
    require(all(isinstance(value, ast.Constant) and isinstance(value.value, str)
        or ast.dump(value, include_attributes=False) == old_format for value in values)
        and values and isinstance(values[0], ast.Constant)
        and values[0].value.lstrip().upper().startswith(('SELECT ', 'WITH ')),
        'retry attempt query must remain a static read query')
    # The actual optimized query preserves the original parameter tuple. Do not
    # mask it: a changed identity, statement receiver, or paid check must fail.
    execute.args[0] = ast.Constant(value='reviewed_retry_attempt_select')
    return node


def _stable_preparation_execution(node: ast.FunctionDef) -> ast.FunctionDef:
    """Normalize one exact offline load branch; the HTTP/fee path stays compared."""
    approved = _statement('''if envelope.get("preparation_retry_proof", {}).get("replay_reason") == "wechat_resolver_parser_repair":
    with connect(db_path) as connection:
        raw_id, payload = _load_resolver_parser_replay(connection, envelope=envelope,
            request=_current_request(connection, intake_id), target=target)
    cost = 0.0
else:
    raw = capture.load_succeeded_raw_response(db_path=db_path, intake_request_id=intake_id,
        stage="profile_prepare", window_key=window, operation=operation)
    raw_id, payload, cost = raw.raw_response_id, raw.value, 0.0''')
    branches = [(item, index) for item in ast.walk(node) if isinstance(item, ast.Try)
                for index, child in enumerate(item.body) if _dump(child) == _dump(approved)]
    if branches:
        block, index = branches[0]
        require(len(branches) == 1 and index == 0, 'non-planning or payment: parser replay loading position changed')
        block.body[index:index+1] = approved.orelse
    return node


def _stable_planning_nodes(body: bytes) -> list[str]:
    module = ast.parse(body)
    replay_functions = {node.name for node in module.body if isinstance(node, ast.FunctionDef)
        and any(isinstance(item, ast.Constant) and item.value == 'wechat_resolver_parser_repair' for item in ast.walk(node))}
    require(replay_functions in (set(), {'_retry_evidence', '_load_resolver_parser_replay', 'execute_step'}),
            'non-planning or payment: parser replay proof, loader and execution must remain coupled')
    unsent_helpers = {'_never_sent_reservation_evidence', '_recover_never_sent_preparation'}
    present = {node.name for node in module.body if isinstance(node, ast.FunctionDef)} & unsent_helpers
    if present or any(isinstance(node, ast.Name) and node.id in unsent_helpers for node in ast.walk(module)):
        planners = [node for node in module.body if isinstance(node, ast.FunctionDef) and node.name == 'enqueue_pending']
        # The inherited planning allowlist must not loosen the newly added
        # recovery: bind its transaction, shadow guard and call position too.
        require(present == unsent_helpers and len(planners) == 1
                and digest(_dump(planners[0])) == NEVER_SENT_PLANNER_AST,
                'non-planning or payment: never-sent recovery and its exact transactional planner must remain coupled')
    result = []
    for node in module.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in PLANNING_FUNCTIONS:
            continue
        if isinstance(node, ast.FunctionDef) and node.name == '_retry_evidence':
            node = _stable_retry_evidence(node)
        if isinstance(node, ast.FunctionDef) and node.name == 'execute_step':
            node = _stable_preparation_execution(node)
        if isinstance(node, ast.FunctionDef) and node.name in PREPARATION_REPAIR_HELPERS:
            require(digest(_dump(node)) == PREPARATION_REPAIR_HELPERS[node.name],
                    'non-planning or payment: preparation recovery helper differs from the reviewed exact AST')
            continue
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(target, ast.Name) and target.id == '_PLANNING_VALIDATION' for target in targets):
                continue
        if isinstance(node, ast.ImportFrom) and node.module in {'contextlib', 'contextvars', 'threading'}:
            names = [alias for alias in node.names if (node.module, alias.name) not in {
                ('contextlib', 'contextmanager'), ('contextvars', 'ContextVar'), ('threading', 'get_ident')}]
            if not names:
                continue
            node.names = names
        result.append(ast.dump(node, include_attributes=False))
    return result


def source_changes(parent_tree: Mapping[str, Any], tree: Mapping[str, Any]) -> dict[str, Any]:
    left, right = records(parent_tree), records(tree)
    changed = {name for name in set(left) | set(right) if left.get(name) != right.get(name)}
    require({MODULE, ENTRY, PLANNING} <= changed and changed <= ALLOWED_FILES,
            'source change exceeds the exact planning repair allowlist')
    repair_scope({name:None for name in changed})
    parent_source, source = Path(parent_tree['source_root']), Path(tree['source_root'])
    for name in changed:
        require(name in right, 'source deletions are not permitted')
        if name in left:
            require(left[name]['mode'] == right[name]['mode'], 'source permissions changed')
        if name == PLANNING:
            require(_stable_planning_nodes(raw(parent_source/name, private=False)) ==
                    _stable_planning_nodes(raw(source/name, private=False)),
                    'non-planning or payment code changed')
        if name in {CAPTURE, PIPELINE}:
            normalize = _stable_capture_nodes if name == CAPTURE else _stable_pipeline_nodes
            require(normalize(raw(parent_source/name,private=False)) == normalize(raw(source/name,private=False)),
                'periodic recovery changed original fee handling or another function')
        if name == RUNTIME:
            require(_stable_runtime_nodes(raw(parent_source/name,private=False)) ==
                    _stable_runtime_nodes(raw(source/name,private=False)), 'runtime code outside _plan_due or queue selection changed')
        if name == INTAKE:
            require(_stable_intake_nodes(raw(parent_source/name,private=False)) ==
                    _stable_intake_nodes(raw(source/name,private=False)), 'intake change exceeds approved display text')
        if name == ADAPTERS:
            require(_stable_adapter_nodes(raw(parent_source/name,private=False)) ==
                    _stable_adapter_nodes(raw(source/name,private=False)),
                    'platform adapter change exceeds reviewed official-account filtering')
        if name in CLI_FILES:
            before = raw(parent_source/name, private=False)
            after = raw(source/name, private=False)
            before_ast, after_ast = ast.parse(before), ast.parse(after)
            marker = ast.dump(ast.parse('sys.dont_write_bytecode = True').body[0], include_attributes=False)
            nodes = [node for node in after_ast.body if ast.dump(node, include_attributes=False) != marker]
            require(len(after_ast.body)-len(nodes) == 1 and
                    [ast.dump(node,include_attributes=False) for node in nodes] ==
                    [ast.dump(node,include_attributes=False) for node in before_ast.body],
                    'CLI change must only disable bytecode before application imports')
    return {name: {'before_sha256': left[name]['sha256'] if name in left else None,
                   'after_sha256': right[name]['sha256']} for name in sorted(changed)}


@contextmanager
def _parent_module(source: Path, identity: str):
    name = '_dcar_intake_parent_' + identity[:24] + '_' + uuid4().hex
    package = types.ModuleType(name)
    package.__path__ = [str(source/'src/dcar_eval/v8')]
    package.__package__ = name
    sys.modules[name] = package
    try:
        yield importlib.import_module(name+'.account_intake_release')
    finally:
        for key in list(sys.modules):
            if key == name or key.startswith(name+'.'):
                sys.modules.pop(key, None)


def parent_context(parent_ref: Mapping[str, Any], *, install_path: Path, database: Path,
                   at: str | None = None, connection=None) -> tuple[dict, dict]:
    from .account_intake_release import inventory
    parent = payload_at(parent_ref, 'sealed-build-receipt-v1')
    require(parent.get('status') == 'succeeded' and
            parent.get('schema_contract') == {'code_schema': 22, 'formal_schema': 22} and
            FIELD not in parent, 'parent must be the installed original schema22 intake build')
    intake = parent.get('account_intake_successor', {})
    require(intake.get('production_rollout') == 'not_authorized' and
            intake.get('local_activation') == 'approved_by_user' and intake.get('operation_authorization'),
            'parent must retain its explicit local capture authorization')
    source = Path(parent['source_root'])
    tree = object_at(parent['account_cleanup_generation']['source_tree'])
    require(inventory(source) == tree, 'complete immutable parent source changed')
    require(parent['critical_files'].get(ENTRY) == records(tree)[ENTRY]['sha256'],
            'original parent verifier is not in its critical source inventory')
    with _parent_module(source, parent_ref['sha256']) as verifier:
        inherited = verifier.verify_inheritance(build=parent, build_ref=parent_ref,
            install_path=install_path, database=database, source=source, at=at, connection=connection)
    require(inventory(source) == tree, 'parent source changed during verification')
    require(inherited.get('preparation_operation_authority', {}).get('loaded_build') == dict(parent_ref),
            'parent local operation authority is incomplete')
    return parent, inherited


def verify_inheritance(*, build: Mapping[str, Any], build_ref: Mapping[str, Any],
                       install_path: Path, database: Path, source: Path, at: str | None = None,
                       connection=None) -> dict[str, Any]:
    from .account_intake_release import inventory
    plan = build.get(FIELD, {})
    require(plan.get('contract') == CONTRACT and dict(build) == payload_at(build_ref, 'sealed-build-receipt-v1'),
            'child build or code successor receipt differs')
    parent, inherited = parent_context(plan['parent_build'], install_path=install_path,
        database=database, at=at, connection=connection)
    allowed = {'source_root', 'git', 'critical_files', 'code_successor_plan',
               'account_cleanup_generation', FIELD, 'created_at', 'validation_scope'}
    require({k:v for k,v in build.items() if k not in allowed} ==
            {k:v for k,v in parent.items() if k not in allowed},
            'migration, original intake plan or execution authority changed')
    require({k:v for k,v in build['account_cleanup_generation'].items() if k != 'source_tree'} ==
            {k:v for k,v in parent['account_cleanup_generation'].items() if k != 'source_tree'},
            'original runtime, transport or operator generation changed')
    tree = object_at(plan['source_tree'])
    require(source != Path(parent['source_root']) and build['source_root'] == str(source) and
            tree == inventory(source) and tree['git'] == build['git'] and
            build['account_cleanup_generation']['source_tree'] == plan['source_tree'],
            'complete child source or build identity differs')
    changes = source_changes(object_at(parent['account_cleanup_generation']['source_tree']), tree)
    require(changes == plan.get('changes'), 'planning source delta changed')
    require(build['critical_files'] == {name:item['sha256'] for name,item in records(tree).items()
        if name.startswith(('src/', 'config/')) and name.endswith(('.py', '.json'))},
        'child critical inventory differs')
    require(object_at(build['code_successor_plan']) == {'contract':'account-cleanup-source-plan-v1',
        'transition':'account-cleanup-0907-v1','project_root':build['project_root'],
        'source_root':str(source),'git':build['git'],'source_tree':plan['source_tree']}, 'child source plan differs')
    require(plan.get('scope') == repair_scope(changes) and
            plan.get('schema_migration_repeated') is False and plan.get('database_writes') == 0 and
            plan.get('paid_gates_reopened') is False and plan.get('publisher_authorized') is False and
            plan.get('remote_database_authorized') is False and plan.get('business_scope_change') == 'none',
            'code repair cannot expand execution authority')
    require(plan.get('parent_intake_proof_sha256') == inherited['intake_proof']['proof_sha256'] and
            plan.get('parent_operation_authority_sha256') == inherited['preparation_operation_authority']['proof_sha256'],
            'original migration and local authorization proof changed')
    identity = database.stat()
    require(plan.get('database_identity') == {'path':str(database), 'device':identity.st_dev, 'inode':identity.st_ino},
            'installed database inode changed')
    issued = datetime.fromisoformat(plan['issued_at'].replace('Z','+00:00'))
    require(issued.utcoffset() is not None and build['created_at'] == plan['issued_at'] and
            datetime.fromisoformat(parent['created_at'].replace('Z','+00:00')) <= issued and
            (at is None or issued <= datetime.fromisoformat(at.replace('Z','+00:00'))) and
            bool(str(plan.get('actor','')).strip()) and bool(str(plan.get('reason','')).strip()), 'repair provenance differs')
    previous_ref = plan.get('previous_code_build')
    if previous_ref is not None:
        previous = payload_at(previous_ref, 'sealed-build-receipt-v1')
        previous_plan = previous.get(FIELD, {})
        require(previous_ref != dict(build_ref) and previous.get('status') == 'succeeded' and
            previous_plan.get('parent_build') == plan['parent_build'] and
            previous_plan.get('database_identity') == plan['database_identity'] and
            previous.get('source_root') != str(source) and
            {k:v for k,v in previous.items() if k not in allowed} ==
            {k:v for k,v in parent.items() if k not in allowed} and
            datetime.fromisoformat(previous['created_at'].replace('Z','+00:00')) <= issued and
            digest({**previous_plan, 'loaded_build':previous_ref}) == plan.get('previous_code_proof_sha256'),
            'previous installed code provenance differs')
    else:
        require(plan.get('previous_code_proof_sha256') is None, 'previous code proof lacks its build')
    require(set(plan.get('checks',{})) == CHECKS, 'focused repair checks are missing')
    for name, ref in plan['checks'].items():
        report = object_at(ref)
        require(report.get('contract') == CHECK_CONTRACT and report.get('name') == name and
                report.get('status') == 'passed' and report.get('exit_code') == 0 and
                report.get('source_tree') == plan['source_tree'] and report.get('changes') == changes and
                report.get('command') and reference(Path(report['output']['path'])) == report['output'],
                'focused check does not bind the exact repaired source')
    proof = {**plan, 'loaded_build':dict(build_ref)}
    proof['proof_sha256'] = digest(proof)
    # Deliberately preserve the original proof objects: their source bindings
    # describe F3 authorization, while this separate proof authorizes F4 code.
    return {**inherited, 'intake_code_proof':proof}
