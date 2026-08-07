"""全量历史回溯编排：发现 → 报价闸 → 指标/详情 → 评论 → 当前窗口分类 → 本地证据。

用法（仓库根目录，建议加 caffeinate 防休眠）：

    caffeinate -dims <python> scripts/run_full_history_backfill.py --dry-run

正式执行必须显式给出固定截止时间、证据回溯边界和付费预算。默认不执行付费调用。

- 区间与边界由 ``--end`` / ``--archive-before`` 固定并写入状态合同。
- 防重复计费：所有付费 content 阶段一律 ``--history-only``（只买 history-* 标记
  的新入库内容）；既有语料由每日调度维护，本脚本绝不重买。
- 预算：发现首段与追加段分别显式审批；下游 ``--auto-ceiling`` 不设默认值。
- 幂等续跑：阶段完成态与预算记录在 runtime/full_history_backfill/state.json；
  重跑跳过已完成阶段，未完成阶段靠幂等槽免费重放继续。
- 任何阶段 blocked / partial / 测试红灯 / 报价超闸 → 立即停，退出码非 0。
"""

from __future__ import annotations

import argparse
import json
import socket
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

REPO = Path(__file__).resolve().parents[1]
DB = REPO / "app" / "data" / "dcar_insight.sqlite3"
STATE_DIR = REPO / "runtime" / "full_history_backfill"
STATE_FILE = STATE_DIR / "state.json"
OPERATOR_FREEZE_LOCK = REPO / "runtime" / "operator-freeze.lock"
TIKHUB_KEY_FILE = Path("/Users/mark/Documents/key/DcarKey/TikHub.env.local")
TIKHUB_USER_INFO = "https://api.tikhub.io/api/v1/tikhub/user/get_user_info"

METRICS_SAFETY, COMMENTS_SAFETY = 1.2, 1.3
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
    state: Dict[str, Any], *, end: str, archive_before: str
) -> None:
    expected = {
        "full_history_start": "2010-01-01T00:00:00+08:00",
        "end": end,
        "archive_before": archive_before,
    }
    existing = state.get("campaign_contract")
    if existing is not None and existing != expected:
        raise AbortRun(
            "state.json 的截止时间或证据边界与本次不同；"
            "不得在同一战役中改窗，需另建状态目录/任务"
        )
    state["campaign_contract"] = expected
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
    arguments: List[str], *, parse_json: bool = False, allow_exit_2: bool = False
) -> Any:
    import os

    env = {**os.environ, "PYTHONPATH": "src/dcar_eval"}
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
    for port in (4173, 8765):
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
        TIKHUB_USER_INFO, headers={"Authorization": f"Bearer {key}"}
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
        log(f"TikHub 预检通过；余额相关字段：{json.dumps(balances, ensure_ascii=False)}")
        log("提示：若余额低于本轮预算合计，先充值可避免半夜熔断")
    else:
        log("TikHub 预检通过（鉴权有效；响应未含可识别余额字段，跳过余额提示）")


def gate_tests(skip: bool) -> None:
    if skip:
        log("跳过测试门（--skip-tests）")
        return
    log("测试门：python -m unittest discover -s tests")
    run_command([*python_prefix(), "-m", "unittest", "discover", "-s", "tests"])


def backup_database() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = REPO / "backups" / f"dcar_insight_before_full_history_{stamp}.sqlite3"
    target.parent.mkdir(exist_ok=True)
    run_command(["sqlite3", str(DB), f".backup '{target}'"])
    log(f"备份完成：{target.name}")
    return str(target)


def phase_discover(
    state: Dict[str, Any],
    *,
    end: str,
    archive_before: str,
    first_cap: float,
    extension_cap: float,
    max_pages: int,
) -> None:
    if state["phases"].get("discover") == "succeeded":
        log("发现阶段已完成，跳过")
        return
    legs = [("fh-discover-01", first_cap)]
    if extension_cap > 0:
        legs.append(("fh-discover-02", extension_cap))
    for leg, (task_id, cap) in enumerate(legs, 1):
        result = run_command(
            rb("discover", end, "--archive-before", archive_before,
               "--task-id", task_id, "--max-amount", str(cap),
               "--max-pages", str(max_pages), "--workers", "4", "--compact"),
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
             "usage": result["usage"], "inserted": result["inserted"]}
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
        # blocked：预算触顶换新 task_id 追加一段，已成功页自动免费重放
        save_state(state)
    raise AbortRun("发现预算已用尽，停止（详见 state.json）", 2)


def phase_quote(
    state: Dict[str, Any],
    *,
    end: str,
    archive_before: str,
    auto_ceiling: float | None,
    quote_only: bool = False,
) -> Dict[str, float]:
    result = run_command(
        rb("status", end, "--archive-before", archive_before, "--history-only"),
        parse_json=True,
    )
    (STATE_DIR / "quote.json").write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    costs = result["estimated_costs_usd"]
    budgets = state["budgets"] or {
        "metrics_douyin": round(max(5.0, costs.get("douyin_metrics", 0) * METRICS_SAFETY), 2),
        "detail_xhs": round(max(2.0, costs.get("xiaohongshu_detail", 0) * METRICS_SAFETY), 2),
        "comments_douyin": round(max(5.0, costs.get("douyin_comments", 0) * COMMENTS_SAFETY), 2),
        "comments_xhs": round(max(5.0, costs.get("xiaohongshu_comments", 0) * COMMENTS_SAFETY), 2),
    }
    projected = round(sum(budgets.values()), 2)
    log(f"报价（history-only 口径）：{json.dumps(costs, ensure_ascii=False)}")
    log(f"阶段预算：{json.dumps(budgets, ensure_ascii=False)}，合计上限 ${projected}")
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
    save_state(state)
    return budgets


def run_content_phase(
    state: Dict[str, Any], key: str, task_id: str, cap: float, end: str, *extra: str
) -> None:
    if state["phases"].get(key) == "succeeded":
        log(f"{key} 已完成，跳过")
        return
    result = run_command(
        rb("content", end, "--task-id", task_id, "--max-amount", str(cap),
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


def phase_repair_metrics(*, end: str) -> None:
    dry = run_command(
        rb("repair-metrics", end, "--platform", "douyin"), parse_json=True,
    )
    if int(dry.get("candidates") or 0) == 0:
        log("占位曝光核查：0 条，无需修复")
        return
    log(f"占位曝光核查：{dry['candidates']} 条，执行修复+重抓")
    run_command(
        rb("repair-metrics", end, "--platform", "douyin", "--apply"),
        parse_json=True,
    )
    cap = round(max(1.0, dry["candidates"] * 0.001 * 1.5), 2)
    result = run_command(
        rb("fetch-repaired-metrics", end, "--platform", "douyin",
           "--task-id", "fh-fetch-repaired-01", "--max-amount", str(cap),
           "--workers", "4", "--compact"),
        parse_json=True, allow_exit_2=True,
    )
    if result["status"] != "succeeded":
        raise AbortRun(
            f"占位曝光修复未完整成功（{result['status']}），禁止继续",
            2,
        )


def phase_classifier() -> None:
    classifier = REPO / "scripts" / "run_audience_classifier.py"
    if not classifier.is_file():
        raise AbortRun(f"受众分类器脚本不存在：{classifier}")
    run_command(
        [
            *python_prefix(),
            str(classifier),
            "--db",
            str(DB),
            "--days",
            "30",
            "--apply",
        ]
    )
    log("受众分类器已重算当前 30 天报告窗口；历史截面需按报告截止日另行回放")


def remaining_backfill_tags() -> int:
    probe = subprocess.run(
        ["sqlite3", str(DB),
         "SELECT COUNT(*) FROM content_items WHERE source_group='history-backfill';"],
        capture_output=True, text=True,
    )
    if probe.returncode != 0:
        raise AbortRun(f"读取 history-backfill 剩余量失败：{probe.stderr[-300:]}")
    return int(probe.stdout.strip() or 0)


def phase_local_evidence(state: Dict[str, Any], batches: int, *, end: str) -> int:
    for batch in range(1, batches + 1):
        remaining = remaining_backfill_tags()
        if remaining == 0:
            break
        log(f"本地证据批次 {batch}/{batches}：剩余 {remaining} 条待媒体+评估")
        result = run_command(
            rb("local-evidence", end, "--task-id", "fh-local-01", "--max-amount", "1",
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


def main() -> int:
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
    parser.add_argument("--skip-tests", action="store_true")
    values = parser.parse_args()
    end = values.end or "<固定截止时间>"
    archive_before = values.archive_before or "<证据回溯边界>"
    discovery_budget = values.discovery_budget or 0.0
    if values.dry_run:
        plan = [
            rb("discover", end, "--archive-before", archive_before, "--task-id",
               "fh-discover-01", "--max-amount", str(discovery_budget or "<预算>"),
               "--max-pages", str(values.max_pages), "--workers", "4", "--compact"),
            rb("status", end, "--archive-before", archive_before, "--history-only"),
            rb("content", end, "--platform", "douyin", "--stage", "metrics",
               "--task-id", "fh-metrics-01", "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--platform", "xiaohongshu", "--stage", "detail",
               "--task-id", "fh-xhs-detail-01", "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--platform", "douyin", "--stage", "comments",
               "--task-id", "fh-comments-dy-01", "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", end, "--platform", "xiaohongshu", "--stage", "comments",
               "--task-id", "fh-comments-xhs-01", "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("local-evidence", end, "--task-id", "fh-local-01", "--max-amount", "1",
               "--limit", str(LOCAL_BATCH_LIMIT), "--tagged-only", "--compact"),
        ]
        print(json.dumps([" ".join(item) for item in plan], ensure_ascii=False, indent=2))
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

    lock_acquired = False
    try:
        gate_environment()
        gate_clean_snapshot()
        phase_preflight()
        gate_tests(values.skip_tests)
        acquire_operation_lock(end=values.end, archive_before=values.archive_before)
        lock_acquired = True
        state = load_state()
        bind_campaign_contract(
            state, end=values.end, archive_before=values.archive_before
        )
        if "backup" not in state:
            state["backup"] = backup_database()
            save_state(state)
        phase_discover(
            state,
            end=values.end,
            archive_before=values.archive_before,
            first_cap=values.discovery_budget,
            extension_cap=values.discovery_extension_budget,
            max_pages=values.max_pages,
        )
        budgets = phase_quote(
            state,
            end=values.end,
            archive_before=values.archive_before,
            auto_ceiling=values.auto_ceiling,
            quote_only=values.discover_only,
        )
        if values.discover_only:
            log("=== 全量发现与报价完成；下游未执行 ===")
            return 0
        run_content_phase(state, "metrics_douyin", "fh-metrics-01",
                          budgets["metrics_douyin"], values.end,
                          "--platform", "douyin", "--stage", "metrics")
        run_content_phase(state, "detail_xhs", "fh-xhs-detail-01",
                          budgets["detail_xhs"], values.end,
                          "--platform", "xiaohongshu", "--stage", "detail")
        phase_repair_metrics(end=values.end)
        run_content_phase(state, "comments_douyin", "fh-comments-dy-01",
                          budgets["comments_douyin"], values.end,
                          "--platform", "douyin", "--stage", "comments")
        run_content_phase(state, "comments_xhs", "fh-comments-xhs-01",
                          budgets["comments_xhs"], values.end,
                          "--platform", "xiaohongshu", "--stage", "comments")
        phase_classifier()
        remaining = phase_local_evidence(
            state, values.local_batches, end=values.end
        )
        log(f"阶段状态：{json.dumps(state['phases'], ensure_ascii=False)}")
        if remaining:
            raise AbortRun(
                f"本地证据仍剩 {remaining} 条；本轮只是推进，尚未完成全量回溯",
                4,
            )
        log("=== 全量历史回溯全部完成 ===")
        return 0
    except AbortRun as abort:
        log(f"停止：{abort}")
        return abort.exit_code
    finally:
        if lock_acquired:
            release_operation_lock()


if __name__ == "__main__":
    sys.exit(main())
