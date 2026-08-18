"""全量历史回溯：发现 → 报价 → 指标/详情/评论 → 证据 → 重复 → 分类。

用法（仓库根目录，建议加 caffeinate 防休眠）：

    caffeinate -dims <python> scripts/run_full_history_backfill.py --dry-run

正式执行必须显式给出固定截止时间、证据回溯边界和付费预算。默认不执行付费调用。

- 区间与边界由 ``--end`` / ``--archive-before`` 固定并写入状态合同。
- 防重复计费：所有付费 content 阶段一律 ``--history-only``（只买 history-* 标记
  的新入库内容）；既有语料缺口需独立审计、报价和授权。
- 预算：发现首段与追加段分别显式审批；下游 ``--auto-ceiling`` 不设默认值。
- 幂等续跑：阶段完成态与预算记录在 runtime/full_history_backfill/state.json；
  重跑跳过已完成阶段，未完成阶段靠幂等槽免费重放继续。
- 任何阶段 blocked / partial / 测试红灯 / 报价超闸 → 立即停，退出码非 0。
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import shlex
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parents[1]
FORMAL_DB = REPO / "app" / "data" / "dcar_insight.sqlite3"
DB = FORMAL_DB
STATE_DIR = REPO / "runtime" / "full_history_backfill"
STATE_FILE = STATE_DIR / "state.json"
OPERATOR_FREEZE_LOCK = REPO / "runtime" / "operator-freeze.lock"
TIKHUB_KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/TikHub.env.local")
TIKHUB_USER_INFO = "https://api.tikhub.io/api/v1/tikhub/user/get_user_info"

METRICS_SAFETY, COMMENTS_SAFETY = 1.2, 1.3
DOUYIN_UNIT_PRICE = 0.001
LOCAL_BATCH_LIMIT = 600


class AbortRun(RuntimeError):
    def __init__(self, message: str, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def log(message: str) -> None:
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    line = f"[{stamp}] {message}"
    print(line, flush=True)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with (STATE_DIR / "run.log").open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


def load_state() -> Dict[str, Any]:
    if STATE_FILE.is_file():
        value = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        if isinstance(value, dict):
            return value
    return {"phases": {}, "budgets": {}}


def save_state(state: Dict[str, Any]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    temporary = STATE_FILE.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(STATE_FILE)


def bind_campaign_contract(
    state: Dict[str, Any], *, end: str, archive_before: str,
    allow_enabled_without_identity: bool = False,
) -> None:
    git_result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if git_result.returncode != 0:
        raise AbortRun("无法读取当前 git commit，禁止绑定战役合同")
    git_commit = git_result.stdout.strip()
    db_stat = DB.stat()
    connection = open_readonly_database(DB)
    connection.row_factory = sqlite3.Row
    try:
        schema_row = connection.execute(
            "SELECT COALESCE(MAX(version),0) version FROM schema_migrations"
        ).fetchone()
        release = connection.execute(
            """
            SELECT *
            FROM evaluation_releases WHERE status='active'
            ORDER BY activated_at DESC,id DESC LIMIT 1
            """
        ).fetchone()
        identities = connection.execute(
            """
            SELECT api.account_id,api.platform,api.uid
            FROM account_platform_identities api
            JOIN accounts a ON a.id=api.account_id
            WHERE a.enabled=1
              AND api.platform IN ('douyin','xiaohongshu')
            ORDER BY api.platform,api.account_id,api.uid
            """
        ).fetchall()
        enabled_accounts = connection.execute(
            "SELECT id FROM accounts WHERE enabled=1 ORDER BY id"
        ).fetchall()
        missing_identities = connection.execute(
            """
            SELECT a.id FROM accounts a
            WHERE a.enabled=1 AND NOT EXISTS (
                SELECT 1 FROM account_platform_identities api
                WHERE api.account_id=a.id
                  AND api.platform IN ('douyin','xiaohongshu')
            ) ORDER BY a.id
            """
        ).fetchall()
        if release is None:
            raise AbortRun("没有 active evaluation release；禁止先付费后才发现无法评估")
        matcher_sha = str(release["matcher_rule_sha256"] or "")
        if len(matcher_sha) != 64:
            raise AbortRun("active release 缺少有效 matcher hash；禁止付费回溯")
        source_root = str(REPO / "src" / "dcar_eval")
        if source_root not in sys.path:
            sys.path.insert(0, source_root)
        from v8.evaluation import (
            V8_RULE_VERSION,
            V9_RULE_VERSION,
            _load_release_runtime,
        )

        if str(release["rule_version"]) not in {
            V8_RULE_VERSION,
            V9_RULE_VERSION,
        }:
            raise AbortRun(
                "active evaluation release 不是受支持的物化规则；"
                "禁止进入全量付费回溯"
            )

        try:
            runtime = _load_release_runtime(connection, release)
        except Exception as exc:
            raise AbortRun(
                f"active evaluation runtime 无法物化：{type(exc).__name__}: {exc}"
            ) from exc
        if runtime.matcher is None:
            raise AbortRun("active evaluation release 没有物化 matcher；禁止付费回溯")
    finally:
        connection.close()
    identity_payload = [
        [int(row["account_id"]), str(row["platform"]), str(row["uid"])]
        for row in identities
    ]
    identity_hash = hashlib.sha256(
        json.dumps(identity_payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()
    missing_identity_hash = hashlib.sha256(
        json.dumps(
            [int(row["id"]) for row in missing_identities], separators=(",", ":")
        ).encode("utf-8")
    ).hexdigest()
    if missing_identities and not allow_enabled_without_identity:
        raise AbortRun(
            f"有 {len(missing_identities)} 个启用账号缺少抖音/小红书平台身份；"
            "无法声称抓取全部账号。先补身份，或显式批准排除。"
        )
    expected = {
        "campaign_id": campaign_id(end, archive_before),
        "full_history_start": "2010-01-01T00:00:00+08:00",
        "end": end,
        "archive_before": archive_before,
        "git_commit": git_commit,
        "database_path": str(DB.resolve()),
        "database_device": int(db_stat.st_dev),
        "database_inode": int(db_stat.st_ino),
        "schema_version": int(schema_row["version"]),
        "enabled_identity_count": len(identity_payload),
        "enabled_identity_sha256": identity_hash,
        "enabled_account_count": len(enabled_accounts),
        "enabled_accounts_without_identity": len(missing_identities),
        "enabled_accounts_without_identity_sha256": missing_identity_hash,
        "scope_exception_approved": bool(allow_enabled_without_identity),
        "active_release": dict(release) if release is not None else None,
    }
    existing = state.get("campaign_contract")
    if existing is not None and existing != expected:
        raise AbortRun(
            "state.json 的截止时间或证据边界与本次不同；"
            "不得在同一战役中改窗，需另建状态目录/任务"
        )
    state["campaign_contract"] = expected
    save_state(state)


def campaign_id(end: str, archive_before: str) -> str:
    return hashlib.sha256(
        f"{end}|{archive_before}".encode("utf-8")
    ).hexdigest()[:10]


def campaign_task_id(campaign: str, phase: str, tranche: int = 1) -> str:
    return f"fh-{campaign}-{phase}-{tranche:02d}"


def open_readonly_database(path: Path) -> sqlite3.Connection:
    source_root = str(REPO / "src" / "dcar_eval")
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    from v8.storage import is_formal_database_path

    if (
        os.environ.get("DCAR_TEST_DENY_FORMAL_DB") == "1"
        and is_formal_database_path(path, formal_database=FORMAL_DB)
    ):
        raise RuntimeError("test process attempted to open the formal DCar database")
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


def _history_scope_rows() -> List[Dict[str, Any]]:
    connection = open_readonly_database(DB)
    connection.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in connection.execute(
                """
                SELECT id,source_group FROM content_items
                WHERE source_group IN ('history-archive','history-backfill')
                ORDER BY id
                """
            ).fetchall()
        ]
    finally:
        connection.close()


def ensure_campaign_scope_baseline(state: Dict[str, Any]) -> None:
    if "history_scope_baseline" in state:
        return
    rows = _history_scope_rows()
    if rows:
        raise AbortRun(
            "开跑前已存在 history-archive/history-backfill 队列；"
            "必须先完成或另行批准接管，禁止混入本战役付费范围"
        )
    state["history_scope_baseline"] = {
        "content_ids": [int(row["id"]) for row in rows],
        "count": len(rows),
        "captured_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    save_state(state)


def freeze_campaign_cohort(state: Dict[str, Any]) -> None:
    """Freeze new source-group rows before local evidence clears the tag."""

    if "campaign_cohort" in state:
        return
    baseline = {
        int(value)
        for value in state.get("history_scope_baseline", {}).get("content_ids", [])
    }
    rows = [row for row in _history_scope_rows() if int(row["id"]) not in baseline]
    state["campaign_cohort"] = {
        "contents": [
            {
                "content_id": int(row["id"]),
                "initial_scope": str(row["source_group"]),
            }
            for row in rows
        ],
        "count": len(rows),
        "frozen_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    save_state(state)


def python_prefix() -> List[str]:
    import os

    override = os.environ.get("DCAR_BACKFILL_PY")
    if override:
        return shlex.split(override)
    # The wrapper itself is already running inside the selected project
    # interpreter.  Reuse it so execution does not depend on ``uv`` being on
    # the non-interactive shell PATH.
    return [sys.executable]


def run_command(
    arguments: List[str], *, parse_json: bool = False,
    allow_exit_2: bool = False,
    environment: Dict[str, str] | None = None,
) -> Any:
    import os

    env = {**os.environ, "PYTHONPATH": "src/dcar_eval"}
    if environment:
        env.update(environment)
    log(f"$ {' '.join(shlex.quote(item) for item in arguments)}")
    completed = subprocess.run(
        arguments, cwd=REPO, env=env, capture_output=True, text=True
    )
    if completed.returncode not in ((0, 2) if allow_exit_2 else (0,)):
        log(completed.stdout[-2000:])
        log(completed.stderr[-2000:])
        raise AbortRun(f"命令退出码 {completed.returncode}：{arguments[-1]}")
    if not parse_json:
        return completed.stdout
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        log(completed.stdout[-2000:])
        raise AbortRun("阶段输出不是合法 JSON，停止") from exc


def rb(phase: str, end: str, *extra: str) -> List[str]:
    return [
        *python_prefix(), "-m", "v8.range_backfill", phase,
        "--full-history", "--end", end, *extra,
    ]


def gate_environment() -> None:
    if not DB.is_file():
        raise AbortRun(f"正式库不存在：{DB}")
    if OPERATOR_FREEZE_LOCK.exists():
        raise AbortRun(f"存在运维冻结锁 {OPERATOR_FREEZE_LOCK.name}，先核清来源")
    if shutil.which("sqlite3") is None:
        raise AbortRun("缺少 sqlite3 命令行，无法在线备份")
    gate_services_stopped()


def gate_services_stopped() -> None:
    for port in (4173, 4174, 8765):
        with socket.socket() as probe:
            probe.settimeout(0.2)
            if probe.connect_ex(("127.0.0.1", port)) == 0:
                raise AbortRun(f"本地端口 {port} 仍有服务监听，先停止正式服务")


def gate_clean_snapshot() -> None:
    """Require an auditable code snapshot; never commit user work from a data job."""

    completed = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        raise AbortRun("无法读取 git 工作树状态")
    dirty = [line for line in completed.stdout.splitlines() if line.strip()]
    if dirty:
        raise AbortRun(
            f"工作树存在 {len(dirty)} 项未提交改动；先形成一致代码快照再跑正式库"
        )


def gate_repository_hygiene() -> None:
    """Reject a clean-but-polluted snapshot containing known temporary payloads."""

    completed = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO,
        capture_output=True,
    )
    if completed.returncode != 0:
        raise AbortRun("无法读取 git 跟踪文件清单")
    tracked = [
        value.decode("utf-8", "replace")
        for value in completed.stdout.split(b"\0")
        if value
    ]
    transient = [path for path in tracked if path.startswith("_to_delete/")]
    oversized = []
    for path in tracked:
        target = REPO / path
        try:
            if target.is_file() and target.stat().st_size > 100 * 1024 * 1024:
                oversized.append(path)
        except OSError:
            continue
    if transient or oversized:
        raise AbortRun(
            "当前提交混入临时/超大文件，不能作为生产回溯快照："
            f"_to_delete={len(transient)}，over_100MiB={len(oversized)}"
        )


def acquire_operation_lock(*, end: str, archive_before: str) -> None:
    OPERATOR_FREEZE_LOCK.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "campaign": "full_history_backfill",
        "end": end,
        "archive_before": archive_before,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    try:
        with OPERATOR_FREEZE_LOCK.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
    except FileExistsError as exc:
        raise AbortRun("运维冻结锁被并发创建，停止") from exc
    log(f"已建立运维冻结锁：{OPERATOR_FREEZE_LOCK.name}")


def release_operation_lock() -> None:
    if not OPERATOR_FREEZE_LOCK.exists():
        return
    stamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    released = OPERATOR_FREEZE_LOCK.with_name(
        f"operator-freeze.full-history-released-{stamp}.lock"
    )
    OPERATOR_FREEZE_LOCK.replace(released)
    log(f"已释放运维冻结锁：{released.name}")


def phase_preflight() -> None:
    """开跑预检：密钥可读、TikHub 可达且鉴权通过、余额可见。

    只用免费的账户信息接口；密钥仅进内存，绝不写日志/输出。
    鉴权失败或网络不通 → 硬停；余额字段只记录并提醒，不做数值硬闸
    （余额真不足时，预算机制会以 provider_balance_blocked 失败关闭）。
    """

    import urllib.error
    import urllib.request

    if not TIKHUB_KEY_FILE.is_file():
        raise AbortRun(f"TikHub 密钥文件不存在：{TIKHUB_KEY_FILE}")
    key = ""
    for line in TIKHUB_KEY_FILE.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("TIKHUB_API_KEY="):
            key = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not key:
        raise AbortRun("密钥文件缺少 TIKHUB_API_KEY 变量")
    request = urllib.request.Request(
        TIKHUB_USER_INFO,
        headers={
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
            "User-Agent": "DCar-Insight/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        if error.code in (401, 403):
            raise AbortRun(
                f"TikHub 鉴权失败（HTTP {error.code}）：密钥无效或过期"
            ) from error
        raise AbortRun(f"TikHub 预检失败（HTTP {error.code}）") from error
    except OSError as error:
        raise AbortRun(f"无法访问 TikHub（检查网络）：{error}") from error

    balances: Dict[str, Any] = {}

    def walk(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            for name, child in node.items():
                walk(child, f"{path}.{name}".strip("."))
        elif isinstance(node, (int, float)) and not isinstance(node, bool):
            if any(term in path.lower() for term in ("balance", "credit")):
                balances[path] = node

    walk(payload)
    if balances:
        log(
            "TikHub 预检通过；balance_fields_detected=true；"
            "精确账户余额不写入持久日志"
        )
    else:
        log("TikHub 预检通过（鉴权有效；响应未含可识别余额字段，跳过余额提示）")


def gate_tests() -> None:
    log("测试门：python -m unittest discover -s tests")
    run_command(
        [*python_prefix(), "-m", "unittest", "discover", "-s", "tests"],
        environment={"DCAR_TEST_DENY_FORMAL_DB": "1"},
    )


def backup_database() -> Dict[str, Any]:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = REPO / "backups" / f"dcar_insight_before_full_history_{stamp}.sqlite3"
    target.parent.mkdir(exist_ok=True)
    run_command(["sqlite3", str(DB), f".backup '{target}'"])
    connection = sqlite3.connect(f"file:{target}?mode=ro&immutable=1", uri=True)
    try:
        quick_check = [str(row[0]) for row in connection.execute("PRAGMA quick_check")]
        foreign_key_violations = connection.execute(
            "PRAGMA foreign_key_check"
        ).fetchall()
        counts = {
            table: int(
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
            for table in (
                "accounts",
                "account_platform_identities",
                "content_items",
                "content_metric_snapshots",
                "comment_capture_runs",
                "evaluation_versions",
                "evidence_artifacts",
                "provider_usage",
            )
        }
    finally:
        connection.close()
    if quick_check != ["ok"] or foreign_key_violations:
        raise AbortRun(
            "在线备份校验失败："
            f"quick_check={quick_check}, fk={len(foreign_key_violations)}"
        )
    digest = hashlib.sha256()
    with target.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    manifest = {
        "database": str(target),
        "sha256": digest.hexdigest(),
        "byte_size": target.stat().st_size,
        "quick_check": quick_check[0],
        "foreign_key_violations": 0,
        "table_counts": counts,
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = STATE_DIR / f"{target.stem}.manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    log(f"备份完成并通过逻辑校验：{target.name}")
    return {**manifest, "manifest": str(manifest_path)}


def phase_recover_stale_slots(state: Dict[str, Any]) -> None:
    """Recover only timed-out work; any fresh running slot blocks takeover."""

    source_root = str(REPO / "src" / "dcar_eval")
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    from v8.capture import recover_stale_fetch_slots
    from v8.media import processor_versions, recover_stale_media_processing_slots

    fetch = recover_stale_fetch_slots(db_path=DB)
    media = recover_stale_media_processing_slots(
        db_path=DB,
        processor_version_by_type=processor_versions(),
    )
    connection = open_readonly_database(DB)
    try:
        fresh_fetch = int(
            connection.execute(
                "SELECT COUNT(*) FROM fetch_slots WHERE status='running'"
            ).fetchone()[0]
        )
        fresh_media = int(
            connection.execute(
                "SELECT COUNT(*) FROM media_processing_slots WHERE status='running'"
            ).fetchone()[0]
        )
    finally:
        connection.close()
    payload = {
        "fetch": fetch,
        "media": media,
        "remaining_running_fetch": fresh_fetch,
        "remaining_running_media": fresh_media,
        "checked_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    state.setdefault("stale_recovery_runs", []).append(payload)
    save_state(state)
    if fresh_fetch or fresh_media:
        raise AbortRun(
            "仍有未超时的 running 槽（fetch="
            f"{fresh_fetch}, media={fresh_media}）；禁止强抢，稍后重试或人工核查"
        )
    log(
        "中断槽恢复完成："
        f"fetch={fetch.get('recovered', 0)}，media={media.get('recovered', 0)}"
    )


def phase_discover(
    state: Dict[str, Any],
    *,
    campaign: str,
    end: str,
    as_of: str,
    archive_before: str,
    first_cap: float,
    extension_cap: float,
    max_pages: int,
) -> None:
    if state["phases"].get("discover") == "succeeded":
        log("发现阶段已完成，跳过")
        return
    legs = [(campaign_task_id(campaign, "discover", 1), first_cap)]
    if extension_cap > 0:
        legs.append((campaign_task_id(campaign, "discover", 2), extension_cap))
    for leg, (task_id, cap) in enumerate(legs, 1):
        result = run_command(
            rb("discover", end, "--as-of", as_of,
               "--archive-before", archive_before,
               "--task-id", task_id, "--max-amount", str(cap),
               "--max-pages", str(max_pages), "--workers", "4", "--compact",
               "--require-live-detail", "--skip-existing-derived-stages"),
            parse_json=True, allow_exit_2=True,
        )
        log(
            f"发现第 {leg} 段：{result['status']}；账号完成 "
            f"{result.get('accounts_completed', 0)}/{result['accounts_considered']}，"
            f"页 {result['pages_processed']}，新增 {result['inserted']}，"
            f"花费 ${result['usage']['amount']}"
        )
        state.setdefault("discover_runs", []).append(
            {"task_id": task_id, "status": result["status"],
             "usage": result["usage"], "inserted": result["inserted"],
             "accounts_considered": result.get("accounts_considered"),
             "accounts_completed": result.get("accounts_completed"),
             "pages_processed": result.get("pages_processed"),
             "stopped_reason": result.get("stopped_reason"),
             "content_manifest": result.get("content_manifest")}
        )
        if result["status"] == "succeeded":
            state["phases"]["discover"] = "succeeded"
            save_state(state)
            return
        if result["status"] == "partial":
            state["phases"]["discover"] = "partial"
            save_state(state)
            raise AbortRun(
                "发现未证明全部账号到底（失败页、游标异常或页数上限）；"
                "禁止进入报价与下游阶段",
                2,
            )
        if result.get("stopped_reason") not in {"budget_blocked", "BudgetBlocked"}:
            state["phases"]["discover"] = "blocked"
            save_state(state)
            raise AbortRun(
                "发现因非预算原因阻断（"
                f"{result.get('stopped_reason')}）；禁止用追加 tranche 重试",
                2,
            )
        # 仅 task budget ceiling 触顶时允许换新 task_id 追加一段；
        # 已成功的固定游标页由槽位免费重放。
        save_state(state)
    raise AbortRun("发现预算已用尽，停止（详见 state.json）", 2)


def phase_quote(
    state: Dict[str, Any],
    *,
    end: str,
    as_of: str,
    archive_before: str,
    discovery_ceiling: float,
    auto_ceiling: float | None,
    quote_only: bool = False,
) -> Dict[str, float]:
    result = run_command(
        rb(
            "status", end, "--as-of", as_of,
            "--archive-before", archive_before, "--history-only",
        ),
        parse_json=True,
    )
    repair = run_command(
        rb(
            "repair-metrics", end, "--as-of", as_of,
            "--platform", "douyin", "--history-only",
        ),
        parse_json=True,
    )
    repair_candidates = int(repair.get("candidates") or 0)
    repair_budget = (
        round(max(1.0, repair_candidates * DOUYIN_UNIT_PRICE * 1.5), 2)
        if repair_candidates
        else 0.0
    )
    costs = result["estimated_costs_usd"]
    computed_budgets = {
        "metrics_douyin": round(max(5.0, costs.get("douyin_metrics", 0) * METRICS_SAFETY), 2),
        "metrics_repair_douyin": repair_budget,
        "detail_douyin": round(
            max(2.0, costs.get("douyin_detail", 0) * METRICS_SAFETY), 2
        ),
        "detail_xhs": round(max(2.0, costs.get("xiaohongshu_detail", 0) * METRICS_SAFETY), 2),
        "comments_douyin": round(max(5.0, costs.get("douyin_comments", 0) * COMMENTS_SAFETY), 2),
        "comments_xhs": round(max(5.0, costs.get("xiaohongshu_comments", 0) * COMMENTS_SAFETY), 2),
    }
    budgets = state["budgets"] or computed_budgets
    missing_budget_keys = sorted(set(computed_budgets) - set(budgets))
    if missing_budget_keys:
        raise AbortRun(
            "state.json 的预算合同来自旧版脚本，缺少："
            + ",".join(missing_budget_keys)
            + "；禁止沿用未计入的付费阶段"
        )
    projected = round(sum(budgets.values()), 2)
    campaign_total = round(discovery_ceiling + projected, 2)
    quote_payload = {
        "range_status": result,
        "placeholder_metrics_repair": repair,
        "repair_budget_usd": repair_budget,
        "stage_budgets_usd": budgets,
        "discovery_ceiling_usd": round(discovery_ceiling, 2),
        "downstream_ceiling_usd": projected,
        "campaign_total_ceiling_usd": campaign_total,
    }
    (STATE_DIR / "quote.json").write_text(
        json.dumps(quote_payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    log(f"报价（history-only 口径）：{json.dumps(costs, ensure_ascii=False)}")
    log(
        f"阶段预算：{json.dumps(budgets, ensure_ascii=False)}；"
        f"发现上限 ${discovery_ceiling}，后续上限 ${projected}，"
        f"战役总上限 ${campaign_total}"
    )
    if quote_only:
        log("仅发现/报价模式：未授权执行下游付费阶段")
        return budgets
    if auto_ceiling is None:
        raise AbortRun(
            f"下游报价上限为 ${projected}；未提供 --auto-ceiling，已停在审批闸",
            3,
        )
    if projected > auto_ceiling:
        raise AbortRun(
            f"报价合计 ${projected} 超过自动放行上限 ${auto_ceiling}，已停。"
            f"报价在 {STATE_DIR / 'quote.json'}；确认后用 --auto-ceiling 提高上限重跑。",
            3,
        )
    state["budgets"] = budgets
    state["budget_ceiling"] = {
        "discovery": round(discovery_ceiling, 2),
        "downstream": projected,
        "campaign_total": campaign_total,
    }
    save_state(state)
    return budgets


def run_content_phase(
    state: Dict[str, Any], key: str, task_id: str, cap: float,
    end: str, as_of: str, *extra: str
) -> None:
    if state["phases"].get(key) == "succeeded":
        log(f"{key} 已完成，跳过")
        return
    result = run_command(
        rb("content", end, "--as-of", as_of,
           "--task-id", task_id, "--max-amount", str(cap),
           "--workers", "4", "--compact", "--history-only", *extra),
        parse_json=True, allow_exit_2=True,
    )
    log(f"{key}：{result['status']}；候选 {result['candidates']}，"
        f"完成 {result['processed']}，花费 ${result['usage']['amount']}")
    state["phases"][key] = result["status"]
    save_state(state)
    if result["status"] != "succeeded":
        raise AbortRun(
            f"{key} 未完整成功（{result['status']} / "
            f"{result.get('stopped_reason')}），禁止继续下游阶段",
            2,
        )


def phase_repair_metrics(
    state: Dict[str, Any], *, campaign: str, end: str, as_of: str, cap: float
) -> None:
    if state["phases"].get("metrics_repair_douyin") == "succeeded":
        log("metrics_repair_douyin 已完成，跳过")
        return
    dry = run_command(
        rb(
            "repair-metrics", end, "--as-of", as_of,
            "--platform", "douyin", "--history-only"
        ),
        parse_json=True,
    )
    candidates = int(dry.get("candidates") or 0)
    if candidates:
        if cap <= 0:
            raise AbortRun("占位曝光修复有候选，但审批预算为 0")
        log(f"占位曝光核查：{candidates} 条，执行修复+重抓")
        run_command(
            rb(
                "repair-metrics", end, "--as-of", as_of,
                "--platform", "douyin",
                "--history-only", "--apply",
            ),
            parse_json=True,
        )
    elif cap <= 0:
        log("占位曝光核查：0 条，无需修复")
        state["phases"]["metrics_repair_douyin"] = "succeeded"
        save_state(state)
        return
    # 即使本次 dry-run 为 0 也要执行：上次可能已把槽位改成
    # retryable_failed 却在真正重抓前中断。
    result = run_command(
        rb("fetch-repaired-metrics", end, "--as-of", as_of,
           "--platform", "douyin",
           "--task-id", campaign_task_id(campaign, "metrics-repair"),
           "--max-amount", str(cap),
           "--workers", "4", "--compact", "--history-only"),
        parse_json=True, allow_exit_2=True,
    )
    if result["status"] != "succeeded":
        raise AbortRun(
            f"占位曝光修复未完整成功（{result['status']}），禁止继续",
            2,
        )
    state["phases"]["metrics_repair_douyin"] = "succeeded"
    state["metrics_repair_douyin"] = {
        "reopened": candidates,
        "processed": result.get("processed"),
        "usage": result.get("usage"),
    }
    save_state(state)


def phase_classifier(state: Dict[str, Any]) -> None:
    if state["phases"].get("audience_classifier") == "succeeded":
        log("audience_classifier 已完成，跳过")
        return
    classifier = REPO / "scripts" / "run_audience_classifier.py"
    if not classifier.is_file():
        raise AbortRun(f"受众分类器脚本不存在：{classifier}")
    snapshot_end = state.get("classification_snapshot_end")
    if not snapshot_end:
        snapshot_end = datetime.now().astimezone().isoformat(timespec="seconds")
        state["classification_snapshot_end"] = snapshot_end
        save_state(state)
    run_command(
        [
            *python_prefix(),
            str(classifier),
            "--db",
            str(DB),
            "--all-contents",
            "--snapshot-end",
            str(snapshot_end),
            "--apply",
        ]
    )
    state["phases"]["audience_classifier"] = "succeeded"
    save_state(state)
    log("受众分类器已按固定截面扫描全部内容；不倒造历史时点快照")


def remaining_backfill_tags() -> int:
    probe = subprocess.run(
        ["sqlite3", str(DB),
         "SELECT COUNT(*) FROM content_items WHERE source_group='history-backfill';"],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        raise AbortRun(f"读取 history-backfill 剩余量失败：{probe.stderr[-300:]}")
    return int(probe.stdout.strip() or 0)


def phase_local_evidence(
    state: Dict[str, Any], batches: int, *, campaign: str, end: str,
    as_of: str,
) -> int:
    for batch in range(1, batches + 1):
        remaining = remaining_backfill_tags()
        if remaining == 0:
            break
        log(f"本地证据批次 {batch}/{batches}：剩余 {remaining} 条待媒体+评估")
        result = run_command(
            rb("local-evidence", end, "--as-of", as_of, "--task-id",
               campaign_task_id(campaign, "local-evidence"), "--max-amount", "1",
               "--limit", str(LOCAL_BATCH_LIMIT), "--tagged-only", "--compact"),
            parse_json=True, allow_exit_2=True,
        )
        log(f"批次完成：清标 {result['tags_cleared']}，失败 {result['failed']}")
        if result["tags_cleared"] == 0 and result["candidates"] > 0:
            log("警告：本批次零清标（媒体源可能过期）。按手册第 8 节处置卡住项后重跑")
            break
    remaining = remaining_backfill_tags()
    state["phases"]["local_evidence"] = (
        "succeeded" if remaining == 0 else f"remaining:{remaining}"
    )
    save_state(state)
    return remaining


def phase_duplicate_rebuild(state: Dict[str, Any]) -> None:
    if state["phases"].get("duplicate_relations") == "succeeded":
        log("duplicate_relations 已完成，跳过")
        return
    fingerprint_result = run_command(
        [
            *python_prefix(),
            "-m",
            "v8.duplicates",
            "fingerprint",
            "--db",
            str(DB),
            "--limit",
            "0",
        ],
        parse_json=True,
    )
    if int(fingerprint_result.get("failed") or 0):
        raise AbortRun(
            "重复指纹队列存在失败，禁止宣告收尾完成",
            2,
        )
    # 队列在本次处理了新指纹时会自动 rebuild 一次；如果队列原本
    # 已空，local-evidence 仍可能刚改了指纹，因此显式收尾。
    result = fingerprint_result.get("relations")
    if result is None:
        result = run_command(
            [
                *python_prefix(),
                "-m",
                "v8.duplicates",
                "rebuild",
                "--db",
                str(DB),
            ],
            parse_json=True,
        )
    state["phases"]["duplicate_relations"] = "succeeded"
    state["duplicate_relations"] = {
        "fingerprint_queue": fingerprint_result,
        "relations": result,
    }
    save_state(state)
    log(
        "重复关系已全库收尾重算一次："
        f"fingerprints={result.get('fingerprints')}，"
        f"relations={result.get('duplicate_relations')}"
    )


def phase_postflight(state: Dict[str, Any], *, campaign: str) -> None:
    """Gate the exact inserted cohort using discovery-side manifests."""

    inserted: Dict[int, Dict[str, Any]] = {
        int(entry["content_id"]): {
            "content_id": int(entry["content_id"]),
            "source_group": str(entry["initial_scope"]),
            "cohort_source": "source_group_delta",
        }
        for entry in state.get("campaign_cohort", {}).get("contents", [])
        if isinstance(entry, dict) and entry.get("content_id") is not None
    }
    manifest_paths: set[str] = set()
    for run in state.get("discover_runs") or []:
        manifest = run.get("content_manifest") or {}
        path = str(manifest.get("path") or "")
        if not path or path in manifest_paths:
            continue
        manifest_paths.add(path)
        target = Path(path)
        if not target.is_file():
            raise AbortRun(f"发现 cohort manifest 丢失：{target}")
        payload = json.loads(target.read_text(encoding="utf-8"))
        for entry in payload.get("contents") or []:
            if not isinstance(entry, dict) or entry.get("content_id") is None:
                continue
            content_id = int(entry["content_id"])
            existing = inserted.get(content_id)
            if entry.get("first_action") == "inserted" or existing is not None:
                inserted[content_id] = existing or dict(entry)
    snapshot_at = str(state.get("data_snapshot_at") or "")
    if not snapshot_at:
        raise AbortRun("postflight 缺少 data_snapshot_at")
    snapshot_dt = datetime.fromisoformat(snapshot_at.replace("Z", "+00:00"))
    day_key = snapshot_dt.date().isoformat()
    iso = snapshot_dt.date().isocalendar()
    week_key = f"{iso.year}-W{iso.week:02d}"
    rows: List[Dict[str, Any]] = []
    connection = open_readonly_database(DB)
    connection.row_factory = sqlite3.Row
    try:
        active = connection.execute(
            "SELECT * FROM evaluation_releases WHERE status='active' "
            "ORDER BY activated_at DESC,id DESC LIMIT 1"
        ).fetchone()
        expected_release = state.get("campaign_contract", {}).get(
            "active_release"
        )
        if active is None or dict(active) != expected_release:
            raise AbortRun(
                "active evaluation release已偏离战役合同；禁止跨规则继续回溯"
            )
        active_release_id = str(active["id"]) if active is not None else ""
        ids = sorted(inserted)
        for offset in range(0, len(ids), 400):
            batch = ids[offset:offset + 400]
            placeholders = ",".join("?" for _ in batch)
            batch_rows = connection.execute(
                f"""
                SELECT c.id,c.platform,COALESCE(c.source_group,'') source_group,
                  EXISTS(
                    SELECT 1 FROM fetch_slots fs
                    WHERE fs.content_id=c.id AND fs.stage='detail'
                      AND fs.window_key='lifetime' AND fs.status='succeeded'
                  ) detail_ready,
                  EXISTS(
                    SELECT 1 FROM fetch_slots fs
                    WHERE fs.content_id=c.id AND fs.stage='metrics'
                      AND fs.window_key=? AND fs.status='succeeded'
                  ) metrics_ready,
                  EXISTS(
                    SELECT 1 FROM comment_capture_runs cr
                    WHERE cr.content_id=c.id AND cr.window_key=?
                      AND cr.status='succeeded'
                  ) comments_ready,
                  EXISTS(
                    SELECT 1 FROM evaluation_versions ev
                    WHERE ev.content_id=c.id AND ev.release_id=?
                      AND ev.evidence_level IN ('V2','V3')
                  ) v2_v3_ready,
                  EXISTS(
                    SELECT 1 FROM duplicate_fingerprints df
                    WHERE df.content_id=c.id
                  ) fingerprint_ready
                FROM content_items c WHERE c.id IN ({placeholders})
                ORDER BY c.id
                """,
                (day_key, week_key, active_release_id, *batch),
            ).fetchall()
            rows.extend(dict(row) for row in batch_rows)
        usage = dict(
            connection.execute(
                """
                SELECT COALESCE(SUM(billed_requests),0) billed_requests,
                       COALESCE(SUM(amount),0) amount
                FROM provider_usage WHERE task_id LIKE ?
                """,
                (f"fh-{campaign}-%",),
            ).fetchone()
        )
    finally:
        connection.close()
    by_id = {int(row["id"]): row for row in rows}
    failures: List[Dict[str, Any]] = []
    for content_id, initial in sorted(inserted.items()):
        row = by_id.get(content_id)
        reasons: List[str] = []
        if row is None:
            reasons.append("content_missing")
        else:
            if not row["detail_ready"]:
                reasons.append("detail_missing")
            if not row["metrics_ready"]:
                reasons.append("metrics_missing")
            if not row["comments_ready"]:
                reasons.append("comments_missing")
            if not row["fingerprint_ready"]:
                reasons.append("fingerprint_missing")
            initial_scope = str(initial.get("source_group") or "")
            if initial_scope == "history-backfill":
                if row["source_group"]:
                    reasons.append("history_backfill_not_released")
                if not row["v2_v3_ready"]:
                    reasons.append("v2_v3_missing")
            elif initial_scope == "history-archive":
                if row["source_group"] != "history-archive":
                    reasons.append("history_archive_scope_changed")
            else:
                reasons.append("initial_scope_missing")
        if reasons:
            failures.append({"content_id": content_id, "reasons": reasons})
    payload = {
        "campaign_id": campaign,
        "status": "succeeded" if not failures else "partial",
        "data_snapshot_at": snapshot_at,
        "inserted_contents": len(inserted),
        "verified_rows": len(rows),
        "active_release_id": active_release_id,
        "provider_usage": usage,
        "failures": failures,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
    }
    target = STATE_DIR / "postflight.json"
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    state["postflight"] = {**payload, "path": str(target)}
    state["phases"]["postflight"] = payload["status"]
    save_state(state)
    if failures:
        raise AbortRun(
            f"postflight 有 {len(failures)} 条本次新增内容未通过，禁止宣告完成",
            2,
        )


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run", action="store_true", help="只打印命令计划，不做任何变更"
    )
    mode.add_argument(
        "--execute", action="store_true", help="执行正式回溯（仍受预算/测试/快照闸门保护）"
    )
    parser.add_argument("--end", help="固定回溯截止时间（必须含时区）")
    parser.add_argument(
        "--archive-before",
        help="证据回溯边界（必须含时区）；更早内容只入库/指标/评论，不做媒体评估",
    )
    parser.add_argument(
        "--discovery-budget",
        type=float,
        help="发现首段明确批准的美元上限",
    )
    parser.add_argument(
        "--discovery-extension-budget",
        type=float,
        default=0.0,
        help="发现首段触顶后追加段的独立美元上限（默认 0，不自动追加）",
    )
    parser.add_argument(
        "--auto-ceiling",
        type=float,
        help="下游指标/详情/评论合计自动放行美元上限；省略即停在报价闸",
    )
    parser.add_argument(
        "--discover-only",
        action="store_true",
        help="只执行发现、入库、分组和 $0 报价，不执行下游阶段",
    )
    parser.add_argument("--max-pages", type=int, default=400)
    parser.add_argument("--local-batches", type=int, default=1)
    parser.add_argument(
        "--allow-enabled-without-identity",
        action="store_true",
        help="显式批准排除缺少抖音/小红书身份的启用账号；默认硬停",
    )
    values = parser.parse_args(argv)
    end = values.end or "<固定截止时间>"
    archive_before = values.archive_before or "<证据回溯边界>"
    campaign = campaign_id(end, archive_before)
    discovery_budget = values.discovery_budget or 0.0
    if values.dry_run:
        snapshot = "<实际数据采集截面>"
        discovery_plan = [
            rb("discover", end, "--as-of", snapshot,
               "--archive-before", archive_before, "--task-id",
               campaign_task_id(campaign, "discover", 1), "--max-amount",
               str(discovery_budget or "<预算>"),
               "--max-pages", str(values.max_pages), "--workers", "4", "--compact",
               "--require-live-detail", "--skip-existing-derived-stages"),
        ]
        if values.discovery_extension_budget > 0:
            discovery_plan.append(
                rb("discover", end, "--as-of", snapshot,
                   "--archive-before", archive_before, "--task-id",
                   campaign_task_id(campaign, "discover", 2), "--max-amount",
                   str(values.discovery_extension_budget), "--max-pages",
                   str(values.max_pages), "--workers", "4", "--compact",
                   "--require-live-detail", "--skip-existing-derived-stages")
            )
        plan = [
            *discovery_plan,
            rb("status", end, "--as-of", snapshot,
               "--archive-before", archive_before, "--history-only"),
            rb("content", end, "--as-of", snapshot,
               "--platform", "douyin", "--stage", "metrics",
               "--task-id", campaign_task_id(campaign, "metrics"),
               "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--as-of", snapshot,
               "--platform", "douyin", "--stage", "detail",
               "--task-id", campaign_task_id(campaign, "douyin-detail"),
               "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--as-of", snapshot,
               "--platform", "xiaohongshu", "--stage", "detail",
               "--stage", "metrics",
               "--task-id", campaign_task_id(campaign, "xhs-detail"),
               "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("repair-metrics", end, "--as-of", snapshot,
               "--platform", "douyin", "--history-only"),
            rb("repair-metrics", end, "--as-of", snapshot,
               "--platform", "douyin", "--history-only", "--apply"),
            rb("fetch-repaired-metrics", end, "--as-of", snapshot,
               "--platform", "douyin", "--task-id",
               campaign_task_id(campaign, "metrics-repair"),
               "--max-amount", "<修复报价>", "--workers", "4", "--compact",
               "--history-only"),
            rb("content", end, "--as-of", snapshot,
               "--platform", "douyin", "--stage", "comments",
               "--task-id", campaign_task_id(campaign, "comments-dy"),
               "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--as-of", snapshot,
               "--platform", "xiaohongshu", "--stage", "comments",
               "--task-id", campaign_task_id(campaign, "comments-xhs"),
               "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("local-evidence", end, "--as-of", snapshot, "--task-id",
               campaign_task_id(campaign, "local-evidence"), "--max-amount", "1",
               "--limit", str(LOCAL_BATCH_LIMIT), "--tagged-only", "--compact"),
            [*python_prefix(), "-m", "v8.duplicates", "fingerprint",
             "--db", str(DB), "--limit", "0"],
            [*python_prefix(), str(REPO / "scripts" / "run_audience_classifier.py"),
             "--db", str(DB), "--all-contents", "--snapshot-end",
             "<证据完成后固定截面>", "--apply"],
        ]
        print(
            json.dumps(
                {
                    "discovery_ceiling_usd": round(
                        discovery_budget + values.discovery_extension_budget, 2
                    ),
                    "commands": [" ".join(item) for item in plan],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if not values.end or not values.archive_before:
        parser.error("--execute 必须同时提供 --end 与 --archive-before")
    if values.discovery_budget is None or values.discovery_budget <= 0:
        parser.error("--execute 必须提供正数 --discovery-budget")
    if values.discovery_extension_budget < 0:
        parser.error("--discovery-extension-budget 不能为负数")
    if values.auto_ceiling is not None and values.auto_ceiling <= 0:
        parser.error("--auto-ceiling 必须为正数")
    if values.max_pages <= 0 or values.local_batches <= 0:
        parser.error("--max-pages 与 --local-batches 必须为正数")
    try:
        end_dt = datetime.fromisoformat(values.end.replace("Z", "+00:00"))
        archive_dt = datetime.fromisoformat(
            values.archive_before.replace("Z", "+00:00")
        )
    except ValueError as exc:
        parser.error(f"时间格式无效：{exc}")
    if end_dt.tzinfo is None or archive_dt.tzinfo is None:
        parser.error("--end 与 --archive-before 必须包含时区")
    if archive_dt > end_dt:
        parser.error("--archive-before 不能晚于 --end")
    if end_dt > datetime.now(end_dt.tzinfo) + timedelta(minutes=1):
        parser.error("--end 不能晚于实际执行时间")

    lock_acquired = False
    try:
        gate_environment()
        acquire_operation_lock(end=values.end, archive_before=values.archive_before)
        lock_acquired = True
        # Close the check/create race: if a service started between the first
        # port probe and the atomic lock creation, stop before any test or DB work.
        gate_services_stopped()
        gate_clean_snapshot()
        gate_repository_hygiene()
        state = load_state()
        bind_campaign_contract(
            state,
            end=values.end,
            archive_before=values.archive_before,
            allow_enabled_without_identity=values.allow_enabled_without_identity,
        )
        ensure_campaign_scope_baseline(state)
        if "backup" not in state:
            state["backup"] = backup_database()
            save_state(state)
        phase_recover_stale_slots(state)
        phase_preflight()
        gate_tests()
        discovery_snapshot_at = state.get("discovery_snapshot_at")
        if not discovery_snapshot_at:
            discovery_snapshot_at = datetime.now().astimezone().isoformat(
                timespec="seconds"
            )
            state["discovery_snapshot_at"] = discovery_snapshot_at
            save_state(state)
        phase_discover(
            state,
            campaign=campaign,
            end=values.end,
            as_of=discovery_snapshot_at,
            archive_before=values.archive_before,
            first_cap=values.discovery_budget,
            extension_cap=values.discovery_extension_budget,
            max_pages=values.max_pages,
        )
        freeze_campaign_cohort(state)
        downstream_started = any(
            name != "discover"
            for name in state.get("phases", {})
        )
        if downstream_started:
            data_snapshot_at = state.get("data_snapshot_at")
            if not data_snapshot_at:
                raise AbortRun(
                    "下游阶段已有状态但缺少固定 data_snapshot_at；"
                    "禁止把当前累计值写入不明历史窗口"
                )
        else:
            # 报价闸前只生成候选截面；若审批隔天才继续，重跑时会刷新。
            # 仅在下游真正获批开跑时才把截面写入战役合同。
            data_snapshot_at = datetime.now().astimezone().isoformat(
                timespec="seconds"
            )
        budgets = phase_quote(
            state,
            end=values.end,
            as_of=str(data_snapshot_at),
            archive_before=values.archive_before,
            discovery_ceiling=(
                values.discovery_budget + values.discovery_extension_budget
            ),
            auto_ceiling=values.auto_ceiling,
            quote_only=values.discover_only,
        )
        if values.discover_only:
            log("=== 全量发现与报价完成；下游未执行 ===")
            return 0
        if not downstream_started:
            state["data_snapshot_at"] = data_snapshot_at
            save_state(state)
        run_content_phase(state, "metrics_douyin",
                          campaign_task_id(campaign, "metrics"),
                          budgets["metrics_douyin"], values.end, data_snapshot_at,
                          "--platform", "douyin", "--stage", "metrics")
        run_content_phase(state, "detail_douyin",
                          campaign_task_id(campaign, "douyin-detail"),
                          budgets["detail_douyin"], values.end, data_snapshot_at,
                          "--platform", "douyin", "--stage", "detail")
        run_content_phase(state, "detail_xhs",
                          campaign_task_id(campaign, "xhs-detail"),
                          budgets["detail_xhs"], values.end, data_snapshot_at,
                          "--platform", "xiaohongshu", "--stage", "detail",
                          "--stage", "metrics")
        phase_repair_metrics(
            state,
            campaign=campaign,
            end=values.end,
            as_of=data_snapshot_at,
            cap=budgets["metrics_repair_douyin"],
        )
        run_content_phase(state, "comments_douyin",
                          campaign_task_id(campaign, "comments-dy"),
                          budgets["comments_douyin"], values.end, data_snapshot_at,
                          "--platform", "douyin", "--stage", "comments")
        run_content_phase(state, "comments_xhs",
                          campaign_task_id(campaign, "comments-xhs"),
                          budgets["comments_xhs"], values.end, data_snapshot_at,
                          "--platform", "xiaohongshu", "--stage", "comments")
        remaining = phase_local_evidence(
            state,
            values.local_batches,
            campaign=campaign,
            end=values.end,
            as_of=str(data_snapshot_at),
        )
        log(f"阶段状态：{json.dumps(state['phases'], ensure_ascii=False)}")
        if remaining:
            raise AbortRun(
                f"本地证据仍剩 {remaining} 条；本轮只是推进，尚未完成全量回溯",
                4,
            )
        phase_duplicate_rebuild(state)
        phase_classifier(state)
        phase_postflight(state, campaign=campaign)
        log(
            "=== 全量历史采集、当前快照与证据推进完成；"
            "质量披露和新报告版本仍需独立验收 ==="
        )
        return 0
    except AbortRun as abort:
        log(f"停止：{abort}")
        return abort.exit_code
    finally:
        if lock_acquired:
            release_operation_lock()


if __name__ == "__main__":
    sys.exit(main())
