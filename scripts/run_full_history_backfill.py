"""全量历史回溯自动驾驶：一条命令跑完 discover → 报价闸 → 指标/详情 → 评论 → 分类器 → 本地证据。

用法（仓库根目录，建议加 caffeinate 防休眠）：

    caffeinate -dims uv run python scripts/run_full_history_backfill.py

设计（2026-08-07 负责人授权的自动决策，全部失败关闭）：

- 区间与边界固定：FULL_HISTORY(2010-01-01) → 2026-08-07，证据窗边界 2026-02-07。
- 防重复计费：所有付费 content 阶段一律 ``--history-only``（只买 history-* 标记
  的新入库内容）；既有语料由每日调度维护，本脚本绝不重买。
- 预算：发现 $30，触顶自动追加一段（共 $60）；报价闸 ``--auto-ceiling``
  （默认 $250，按 status 报价×安全系数折算）之内自动放行，超了停下等人。
- 幂等续跑：阶段完成态与预算记录在 runtime/full_history_backfill/state.json；
  重跑跳过已完成阶段，未完成阶段靠幂等槽免费重放继续。
- 任何阶段 blocked / 测试红灯 / 报价超闸 → 立即停，退出码非 0，摘要落盘。
"""

from __future__ import annotations

import argparse
import json
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

FH_END = "2026-08-07T00:00:00+08:00"
FH_ARCHIVE = "2026-02-07T00:00:00+08:00"
DISCOVER_CAP = 30.0
DISCOVER_EXTENSION_CAP = 30.0
DEFAULT_AUTO_CEILING = 250.0
METRICS_SAFETY, COMMENTS_SAFETY = 1.2, 1.3
LOCAL_BATCH_LIMIT = 600
CAMPAIGN_FILES = [
    "src/dcar_eval/v8/range_backfill.py",
    "src/dcar_eval/v8/storage.py",
    "src/dcar_eval/v8/evaluation.py",
    "src/dcar_eval/v8/scheduler.py",
    "tests/test_v8_history_backfill_scopes.py",
    "docs/v8/全量历史回溯运行手册_2026-08-07.md",
    "scripts/run_full_history_backfill.py",
]


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


def python_prefix() -> List[str]:
    import os

    override = os.environ.get("DCAR_BACKFILL_PY")
    if override:
        return shlex.split(override)
    return ["uv", "run", "python"]


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


def rb(phase: str, *extra: str) -> List[str]:
    return [
        *python_prefix(), "-m", "v8.range_backfill", phase,
        "--full-history", "--end", FH_END, *extra,
    ]


def gate_environment() -> None:
    if not DB.is_file():
        raise AbortRun(f"正式库不存在：{DB}")
    locks = sorted(REPO.glob("runtime/operator-freeze.lock"))
    if locks:
        raise AbortRun(f"存在运维冻结锁 {locks[0].name}，先解除再跑")
    if shutil.which("sqlite3") is None:
        raise AbortRun("缺少 sqlite3 命令行，无法在线备份")


def gate_tests(skip: bool) -> None:
    if skip:
        log("跳过测试门（--skip-tests）")
        return
    log("测试门：uv run pytest tests -q")
    run_command([*python_prefix()[:-1], "pytest", "tests", "-q"])


def commit_campaign_files() -> None:
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=REPO, capture_output=True, text=True
    )
    if status.returncode != 0:
        log("警告：git 不可用，跳过提交（不阻塞数据操作）")
        return
    dirty = [line[3:] for line in status.stdout.splitlines() if line.strip()]
    if not dirty:
        return
    ours = [path for path in CAMPAIGN_FILES if any(path in item for item in dirty)]
    others = [item for item in dirty if not any(path in item for path in CAMPAIGN_FILES)]
    if others:
        log(f"警告：工作树有 {len(others)} 个非本次变更文件未提交，保持原样：{others[:5]}")
    if not ours:
        return
    subprocess.run(["git", "add", *ours], cwd=REPO, check=False)
    committed = subprocess.run(
        ["git", "commit", "-m", "全量历史回溯：range_backfill 全量能力+防洪闸+history-only 防重复计费+一键执行"],
        cwd=REPO, capture_output=True, text=True,
    )
    log("git 提交：" + (committed.stdout or committed.stderr).strip()[:200])


def backup_database() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    target = REPO / "backups" / f"dcar_insight_before_full_history_{stamp}.sqlite3"
    target.parent.mkdir(exist_ok=True)
    run_command(["sqlite3", str(DB), f".backup '{target}'"])
    log(f"备份完成：{target.name}")
    return str(target)


def phase_discover(state: Dict[str, Any]) -> None:
    if state["phases"].get("discover") == "succeeded":
        log("发现阶段已完成，跳过")
        return
    for leg, (task_id, cap) in enumerate(
        (("fh-discover-01", DISCOVER_CAP),
         ("fh-discover-02", DISCOVER_CAP + DISCOVER_EXTENSION_CAP)), 1
    ):
        result = run_command(
            rb("discover", "--archive-before", FH_ARCHIVE,
               "--task-id", task_id, "--max-amount", str(cap),
               "--max-pages", "400", "--workers", "4", "--compact"),
            parse_json=True, allow_exit_2=True,
        )
        log(f"发现第 {leg} 段：{result['status']}；页 {result['pages_processed']}，"
            f"新增 {result['inserted']}，花费 ${result['usage']['amount']}")
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
            log("发现存在失败页（重跑可续），继续后续阶段")
            return
        # blocked：预算触顶换新 task_id 追加一段，已成功页自动免费重放
        save_state(state)
    raise AbortRun("发现阶段两段预算仍被熔断，停止（详见 state.json）", 2)


def phase_quote(state: Dict[str, Any], auto_ceiling: float) -> Dict[str, float]:
    result = run_command(
        rb("status", "--archive-before", FH_ARCHIVE, "--history-only"),
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
    state: Dict[str, Any], key: str, task_id: str, cap: float, *extra: str
) -> None:
    if state["phases"].get(key) == "succeeded":
        log(f"{key} 已完成，跳过")
        return
    result = run_command(
        rb("content", "--task-id", task_id, "--max-amount", str(cap),
           "--workers", "4", "--compact", "--history-only", *extra),
        parse_json=True, allow_exit_2=True,
    )
    log(f"{key}：{result['status']}；候选 {result['candidates']}，"
        f"完成 {result['processed']}，花费 ${result['usage']['amount']}")
    state["phases"][key] = result["status"]
    save_state(state)
    if result["status"] == "blocked":
        raise AbortRun(
            f"{key} 被熔断（{result.get('stopped_reason')}）。预算按报价×安全系数给出仍触顶，"
            "说明报价失真，需人工核对 quote.json 后换新 task_id 续跑", 2,
        )


def phase_repair_metrics() -> None:
    dry = run_command(
        rb("repair-metrics", "--platform", "douyin"), parse_json=True,
    )
    if int(dry.get("candidates") or 0) == 0:
        log("占位曝光核查：0 条，无需修复")
        return
    log(f"占位曝光核查：{dry['candidates']} 条，执行修复+重抓")
    run_command(rb("repair-metrics", "--platform", "douyin", "--apply"), parse_json=True)
    cap = round(max(1.0, dry["candidates"] * 0.001 * 1.5), 2)
    run_command(
        rb("fetch-repaired-metrics", "--platform", "douyin",
           "--task-id", "fh-fetch-repaired-01", "--max-amount", str(cap),
           "--workers", "4", "--compact"),
        parse_json=True, allow_exit_2=True,
    )


def phase_classifier() -> None:
    run_command(
        [*python_prefix(), "scripts/run_audience_classifier.py",
         "--db", str(DB), "--apply"]
    )
    log("受众分类器已覆盖新互动用户")


def remaining_backfill_tags() -> int:
    probe = subprocess.run(
        ["sqlite3", str(DB),
         "SELECT COUNT(*) FROM content_items WHERE source_group='history-backfill';"],
        capture_output=True, text=True,
    )
    return int(probe.stdout.strip() or 0)


def phase_local_evidence(state: Dict[str, Any], batches: int) -> int:
    for batch in range(1, batches + 1):
        remaining = remaining_backfill_tags()
        if remaining == 0:
            break
        log(f"本地证据批次 {batch}/{batches}：剩余 {remaining} 条待媒体+评估")
        result = run_command(
            rb("local-evidence", "--task-id", "fh-local-01", "--max-amount", "1",
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
    parser.add_argument("--auto-ceiling", type=float, default=DEFAULT_AUTO_CEILING)
    parser.add_argument("--local-batches", type=int, default=1)
    parser.add_argument("--skip-tests", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="只打印将执行的命令计划，不做任何变更")
    values = parser.parse_args()
    if values.dry_run:
        plan = [
            rb("discover", "--archive-before", FH_ARCHIVE, "--task-id",
               "fh-discover-01", "--max-amount", str(DISCOVER_CAP),
               "--max-pages", "400", "--workers", "4", "--compact"),
            rb("status", "--archive-before", FH_ARCHIVE, "--history-only"),
            rb("content", "--platform", "douyin", "--stage", "metrics",
               "--task-id", "fh-metrics-01", "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", "--platform", "xiaohongshu", "--stage", "detail",
               "--task-id", "fh-xhs-detail-01", "--max-amount", "<报价×1.2>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", "--platform", "douyin", "--stage", "comments",
               "--task-id", "fh-comments-dy-01", "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("content", "--platform", "xiaohongshu", "--stage", "comments",
               "--task-id", "fh-comments-xhs-01", "--max-amount", "<报价×1.3>",
               "--workers", "4", "--compact", "--history-only"),
            rb("local-evidence", "--task-id", "fh-local-01", "--max-amount", "1",
               "--limit", str(LOCAL_BATCH_LIMIT), "--tagged-only", "--compact"),
        ]
        print(json.dumps([" ".join(item) for item in plan], ensure_ascii=False, indent=2))
        return 0
    try:
        gate_environment()
        gate_tests(values.skip_tests)
        commit_campaign_files()
        state = load_state()
        if "backup" not in state:
            state["backup"] = backup_database()
            save_state(state)
        phase_discover(state)
        budgets = phase_quote(state, values.auto_ceiling)
        run_content_phase(state, "metrics_douyin", "fh-metrics-01",
                          budgets["metrics_douyin"],
                          "--platform", "douyin", "--stage", "metrics")
        run_content_phase(state, "detail_xhs", "fh-xhs-detail-01",
                          budgets["detail_xhs"],
                          "--platform", "xiaohongshu", "--stage", "detail")
        phase_repair_metrics()
        run_content_phase(state, "comments_douyin", "fh-comments-dy-01",
                          budgets["comments_douyin"],
                          "--platform", "douyin", "--stage", "comments")
        run_content_phase(state, "comments_xhs", "fh-comments-xhs-01",
                          budgets["comments_xhs"],
                          "--platform", "xiaohongshu", "--stage", "comments")
        phase_classifier()
        remaining = phase_local_evidence(state, values.local_batches)
        log("=== 全量历史回溯本轮完成 ===")
        log(f"阶段状态：{json.dumps(state['phases'], ensure_ascii=False)}")
        if remaining:
            log(f"本地证据剩余 {remaining} 条：每晚重跑本命令即可续推（其余阶段自动跳过）")
        return 0
    except AbortRun as abort:
        log(f"停止：{abort}")
        return abort.exit_code


if __name__ == "__main__":
    sys.exit(main())
