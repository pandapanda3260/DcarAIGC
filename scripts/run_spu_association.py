#!/usr/bin/env python3
"""车型×人群×场景 关联回刷 CLI（系统级执行通道，不经 HTTP）。

只处理 V2/V3 已发布内容（SQL 预过滤，非 V2/V3 不进入循环）。默认试算
（dry-run）：仅统计处理范围与词表规模，不写库；加 ``--apply`` 才真正执行。
实跑时批量预取证据等级与 ASR/OCR 路径、每批一个事务提交、进度实时打印并
写回 ``spu_association_runs``，与页面「运行关联」共用同一套 run_association。

规则链后自动挂 LLM 补空（B 链，豆包 doubao-seed-2-1-pro，key 从
DcarKey/dcar.env.local 读取；key 缺失自动跳过）。判定按 内容+文本哈希 缓存，
重跑不重复付费。

用法：
    python3 scripts/run_spu_association.py                     # 试算，不写库
    python3 scripts/run_spu_association.py --apply --llm-limit 100
        # 全量规则 + 只对前 100 条未解决内容调大模型（放量前的质量/成本试算）
    python3 scripts/run_spu_association.py --apply             # 全量（规则+LLM）
    python3 scripts/run_spu_association.py --apply --no-llm    # 只跑规则
    python3 scripts/run_spu_association.py --rollback-llm      # 一键失效 LLM 行
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8.llm_assist import invalidate_llm_links, llm_available  # noqa: E402
from v8.spu_audience import (  # noqa: E402
    SpuAudienceError,
    default_llm_hook,
    dry_run_summary,
    run_association,
)
from v8.storage import DEFAULT_DB, connect, now_utc, transaction  # noqa: E402

_DRY_RUN_FIELDS = (
    ("published_total", "已发布内容总数"),
    ("eligible_v23", "V2/V3 参与关联"),
    ("skipped_not_v23", "非V2/V3 跳过"),
    ("series_count", "车系数"),
    ("alias_count", "识别别名词数"),
    ("scene_count", "场景数"),
    ("estimated_minutes", "预计耗时（分钟，粗估）"),
)

_RESULT_FIELDS = (
    ("run_id", "运行记录编号"),
    ("contents_total", "V2/V3 参与"),
    ("spu_linked", "归到车型"),
    ("trim_resolved", "细化到款型"),
    ("gray_count", "车型灰区"),
    ("scene_linked", "识别到场景"),
    ("audience_linked", "归因到人群"),
    ("insufficient_evidence", "非V2/V3 跳过"),
)

_LLM_FIELDS = (
    ("targets", "未解决、送大模型判定"),
    ("cache_hits", "命中判定缓存（零成本）"),
    ("called", "真实调用次数"),
    ("accepted", "判定通过四道闸"),
    ("rejected", "判定被闸拦下"),
    ("errors", "调用失败"),
    ("spu_filled", "补上车型"),
    ("gray_upgraded", "灰区升级确认"),
    ("gray_overridden", "灰区反向改判"),
    ("trim_refined", "细化到款型"),
    ("scene_filled", "补上场景"),
    ("audience_filled", "补上人群"),
    ("out_of_catalog", "疑似库外新车系"),
    ("input_tokens", "输入 tokens"),
    ("output_tokens", "输出 tokens"),
    ("duration_seconds", "LLM 阶段耗时（秒）"),
)


def _print_llm_summary(llm: dict) -> None:
    if not llm:
        return
    if llm.get("enabled") is False or llm.get("note"):
        print(f"LLM 补充未执行：{llm.get('note') or llm.get('error') or '未启用'}")
        return
    print("LLM 补充（B 链）：")
    for key, label in _LLM_FIELDS:
        if key in llm:
            print(f"  {label}: {llm.get(key)}")
    if llm.get("reject_reasons"):
        print(f"  拒绝原因分布: {llm['reject_reasons']}")
    if llm.get("aborted"):
        print(f"  ⚠ {llm['aborted']}")
    if llm.get("error"):
        print(f"  ⚠ LLM 阶段异常：{llm['error']}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="车型×人群×场景 关联回刷（只处理 V2/V3；默认试算不写库）"
    )
    parser.add_argument(
        "--apply", action="store_true", help="真正执行；缺省仅试算处理范围"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="只处理前 N 条 V2/V3 内容（试跑用；试跑不做历史行清理）",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="每个事务提交的内容条数"
    )
    parser.add_argument(
        "--no-llm", action="store_true", help="跳过 LLM 补空，只跑规则链"
    )
    parser.add_argument(
        "--llm-limit", type=int, default=None,
        help="只对前 N 条未解决内容调大模型（放量前的质量/成本试算）",
    )
    parser.add_argument(
        "--window", choices=("yesterday", "this_week", "last_week"), default=None,
        help="只重算发布时间在该统计窗口内的 V2/V3 内容（与页面弹窗同口径）",
    )
    parser.add_argument(
        "--llm-concurrency", type=int, default=None, help="大模型并发调用数（默认 8）"
    )
    parser.add_argument(
        "--rollback-llm", action="store_true",
        help="一键失效所有 LLM 补充行（判定缓存保留），然后退出",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="数据库路径")
    args = parser.parse_args()

    if args.rollback_llm:
        with connect(args.db) as connection:
            with transaction(connection):
                total = invalidate_llm_links(connection, now_utc())
        print(f"已失效 {total} 行 LLM 补充关联（判定缓存保留，重跑可低成本重放）。")
        return 0

    try:
        report = dry_run_summary(db_path=args.db, limit=args.limit, window=args.window)
    except SpuAudienceError as error:
        print(f"无法执行：{error}")
        return 1
    if args.window:
        print(f"重算范围：统计窗口「{args.window}」内发布的 V2/V3 内容")
    print("处理范围试算：")
    for key, field_label in _DRY_RUN_FIELDS:
        value = report.get(key)
        print(f"  {field_label}: {value if value is not None else '—'}")
    llm_ready = llm_available() and not args.no_llm
    print(f"  LLM 补空: {'启用（doubao）' if llm_ready else '关闭（--no-llm 或 key 缺失）'}")
    if not args.apply:
        print("试算完成（未写库）。确认无误后加 --apply 执行。")
        return 0

    started = time.monotonic()

    def _progress(processed: int, total: int) -> None:
        elapsed = time.monotonic() - started
        rate = processed / elapsed if elapsed > 0 else 0.0
        remaining_minutes = ((total - processed) / rate / 60) if rate > 0 else 0.0
        print(
            f"  已处理 {processed}/{total}"
            f"（{rate:.0f} 条/秒，预计还需 {remaining_minutes:.1f} 分钟）",
            flush=True,
        )

    def _llm_progress(processed: int, total: int) -> None:
        print(f"  LLM 补充 {processed}/{total}", flush=True)

    llm_hook = None
    if llm_ready:
        llm_hook = default_llm_hook(
            limit=args.llm_limit,
            concurrency=args.llm_concurrency,
            progress_callback=_llm_progress,
        )

    try:
        summary = run_association(
            db_path=args.db,
            limit=args.limit,
            scope_window=args.window,
            batch_size=max(1, args.batch_size),
            progress_callback=_progress,
            llm_hook=llm_hook,
        )
    except SpuAudienceError as error:
        print(f"执行失败：{error}")
        return 1
    print("关联完成：")
    for key, field_label in _RESULT_FIELDS:
        print(f"  {field_label}: {summary.get(key)}")
    _print_llm_summary(summary.get("llm") or {})
    print(f"总耗时 {(time.monotonic() - started) / 60:.1f} 分钟")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
