#!/usr/bin/env python3
"""Reversibly reorganize the DCar workspace without deleting source assets."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = ROOT / "docs" / "migration" / "迁移映射_2026-08-02.json"

CURRENT_CODE = {
    "analyze_douyin_tikhub_v6.py",
    "build_rnote_three_proposition_report.py",
    "collect_douyin_by_uid.py",
    "collect_rnote_pilot.py",
    "collect_tikhub_douyin_enrichment_v6.py",
    "collect_xhs_public.py",
    "fetch_onebound_comments.py",
    "fetch_onebound_details.py",
    "generate_channel_core_visual_v6_tikhub.py",
    "generate_three_proposition_report.py",
    "generate_three_proposition_visual.py",
    "label_douyin_batch_v2.py",
    "label_douyin_video_evidence_v3.py",
    "probe_tikhub_douyin.py",
    "process_douyin_image_posts_v3.py",
    "process_douyin_video_evidence_v3.py",
    "extract_douyin_video_ocr_v3.py",
    "generate_video_contact_sheets_v3.py",
    "merge_douyin_ocr_results_v3.py",
    "rebuild_channel_evaluation_v4.py",
    "restructure_channel_report_v5.py",
    "restructure_channel_report_v6_tikhub.py",
    "three_proposition_scoring.py",
    "xhs_browser_comment_collector.js",
    "vision_ocr.swift",
}

CURRENT_CONFIG = {
    "business_selling_points_v3_final.json": "config/business_selling_points_v3_final.json",
    "business_selling_points_v4_final.json": "config/business_selling_points_v4_final.json",
    "懂车帝内容评估判断标准与流程_v4_终版.md": "config/懂车帝内容评估判断标准与流程_v4_终版.md",
    "requirements-douyin.txt": "config/requirements-douyin.txt",
    "brightdata.env.example": "config/providers/brightdata.env.example",
}

CURRENT_REPORTS = {
    "双渠道结构化结论_v6.2_TikHub_2026-08-02.json",
    "双渠道结构化结论报告_v6.2_TikHub_2026-08-02.md",
    "双渠道核心结论_v6_TikHub补充_2026-08-02.png",
    "抖音438条内容渠道评估_v6_TikHub补充_2026-08-02.csv",
    "小红书渠道评估样本与数据缺口_v4_2026-08-02.csv",
}

DOUYIN_INPUTS = {
    "douyin_30_account_content_sample_2026-08-01.jsonl",
    "douyin_30_account_draw_order_2026-08-01.csv",
    "douyin_30_account_draw_order_2026-08-01.metadata.json",
    "douyin_30_account_final_sample_2026-08-01.csv",
    "douyin_30_account_final_sample_2026-08-01.metadata.json",
    "douyin_account_uid_pool_2026-08-01.tsv",
}

XHS_INPUTS = {
    "notes_all.csv",
    "notes_unique.csv",
    "notes_prepare_report.json",
    "pilot_sample_10_blind.csv",
    "pilot_sample_10_labels.csv",
    "pilot_sample_10_metadata.json",
    "小红书汽车内容链接.txt",
    "小红书非汽车内容链接.txt",
}

ACTIVE_DOUYIN_DATA = {
    "douyin_selling_point_labels_v3_video_2026-08-01.jsonl",
    "douyin_selling_point_labels_v4_full_publication_2026-08-02.jsonl",
    "douyin_selling_point_summary_v3_video_2026-08-01.json",
    "douyin_caption_vs_video_comparison_v3_2026-08-01.json",
    "channel_evaluation_summary_v4_2026-08-02.json",
}

ACTIVE_XHS_DATA = {
    "rnote_final_scores_v1.jsonl",
    "rnote_three_proposition_results.csv",
    "rnote_three_proposition_results.jsonl",
    "rnote_three_proposition_summary.json",
    "rnote_collection_attempts.csv",
    "pilot_content_scores_v0.3.jsonl",
}

CACHE_DIRS = {
    "douyin_cache": "data/cache/douyin_public",
    "douyin_video_analysis_v3": "data/cache/douyin_media",
    "tikhub_douyin_enrichment_2026-08-02": "data/cache/tikhub/2026-08-02",
    "rnote_cache": "data/cache/rnote",
    "raw_responses": "data/cache/raw_responses",
    "pilot_media": "data/cache/pilot_media",
}

DEPENDENCY_DIRS = {
    ".video_deps": "runtime/dependencies/video",
    ".douyin_deps": "runtime/dependencies/douyin",
}

PROVIDER_DOCS = {
    "Bright_Data_小红书渠道验收说明_2026-07-19.md",
    "Chrome_登录态小红书评论采集验收_2026-07-19.md",
    "万邦小红书评论接口故障诊断_2026-07-19.md",
    "抖音UID最近作品采集说明_v1.md",
}


def explicit_mapping() -> dict[Path, Path]:
    mapping: dict[Path, Path] = {}
    if (ROOT / "tmp").exists():
        mapping[ROOT / "tmp"] = ROOT / "runtime" / "tmp" / "legacy_workspace_tmp"
    for source, target in CACHE_DIRS.items():
        mapping[ROOT / source] = ROOT / target
    for source, target in DEPENDENCY_DIRS.items():
        mapping[ROOT / source] = ROOT / target
    for name in CURRENT_CODE:
        mapping[ROOT / name] = ROOT / "src" / "dcar_eval" / name
    for path in ROOT.glob("test_*"):
        if path.is_file() and path.suffix in {".py", ".js"}:
            mapping[path] = ROOT / "tests" / path.name
    for source, target in CURRENT_CONFIG.items():
        mapping[ROOT / source] = ROOT / target
    for name in DOUYIN_INPUTS:
        mapping[ROOT / name] = ROOT / "data" / "inputs" / "douyin" / name
    for name in XHS_INPUTS:
        mapping[ROOT / name] = ROOT / "data" / "inputs" / "xiaohongshu" / name
    for name in ACTIVE_DOUYIN_DATA:
        mapping[ROOT / name] = ROOT / "data" / "processed" / "current" / "douyin" / name
    for name in ACTIVE_XHS_DATA:
        mapping[ROOT / name] = ROOT / "data" / "processed" / "current" / "xiaohongshu" / name
    for name in CURRENT_REPORTS:
        mapping[ROOT / name] = ROOT / "reports" / "current" / name
    users = ROOT / "抖音评论匿名用户评分_v6_TikHub_2026-08-02.jsonl"
    mapping[users] = ROOT / "data" / "processed" / "current" / "douyin" / users.name
    for name in PROVIDER_DOCS:
        mapping[ROOT / name] = ROOT / "docs" / "providers" / name
    if (ROOT / "执行说明.md").exists():
        mapping[ROOT / "执行说明.md"] = ROOT / "docs" / "执行说明.md"
    return mapping


def remaining_mapping(existing: dict[Path, Path]) -> dict[Path, Path]:
    mapping: dict[Path, Path] = {}
    reserved = {
        ".git", ".gitignore", "archive", "app", "config", "data", "docs", "reports",
        "runtime", "src", "tests", "README.md", "pyproject.toml",
    }
    mapped_sources = set(existing)
    for source in ROOT.iterdir():
        if source in mapped_sources or source.name in reserved:
            continue
        if source.name == "__pycache__":
            mapping[source] = ROOT / "runtime" / "tmp" / "pycache_root"
        elif source.name == ".DS_Store":
            mapping[source] = ROOT / "runtime" / "tmp" / ".DS_Store"
        elif source.is_dir():
            mapping[source] = ROOT / "archive" / "unclassified" / source.name
        elif source.suffix in {".py", ".js", ".swift"}:
            mapping[source] = ROOT / "archive" / "legacy_scripts" / source.name
        elif source.suffix == ".md" or source.suffix == ".png":
            mapping[source] = ROOT / "reports" / "archive" / source.name
        elif source.suffix in {".csv", ".json", ".jsonl", ".tsv", ".txt", ".svg"}:
            mapping[source] = ROOT / "data" / "processed" / "archive" / source.name
        else:
            mapping[source] = ROOT / "archive" / "unclassified" / source.name
    return mapping


def build_mapping() -> dict[Path, Path]:
    explicit = explicit_mapping()
    return explicit | remaining_mapping(explicit)


def validate_mapping(mapping: dict[Path, Path]) -> None:
    targets: dict[Path, Path] = {}
    for source, target in mapping.items():
        if not source.exists():
            continue
        if target in targets:
            raise RuntimeError(f"duplicate target: {target} from {source} and {targets[target]}")
        targets[target] = source
        if target.exists():
            raise RuntimeError(f"target already exists: {target}")
        if ROOT not in target.parents:
            raise RuntimeError(f"target escapes project: {target}")


def run_migration(mapping: dict[Path, Path], dry_run: bool) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for source, target in sorted(mapping.items(), key=lambda item: (len(item[0].parts), str(item[0]))):
        if not source.exists():
            continue
        is_dir = source.is_dir()
        if is_dir:
            file_count = sum(1 for p in source.rglob("*") if p.is_file())
            total_bytes = sum(p.stat().st_size for p in source.rglob("*") if p.is_file())
        else:
            file_count = 1
            total_bytes = source.stat().st_size
        records.append({
            "source": str(source.relative_to(ROOT)),
            "target": str(target.relative_to(ROOT)),
            "is_directory": is_dir,
            "file_count": file_count,
            "bytes": total_bytes,
        })
        if not dry_run:
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(source), str(target))
    return records


def rollback() -> None:
    payload = json.loads(LOG_PATH.read_text(encoding="utf-8"))
    for record in reversed(payload["moves"]):
        source = ROOT / record["source"]
        target = ROOT / record["target"]
        if not target.exists():
            continue
        if source.exists():
            raise RuntimeError(f"rollback source already exists: {source}")
        source.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(target), str(source))
    print("rollback complete")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--rollback", action="store_true")
    args = parser.parse_args()
    if args.rollback:
        rollback()
        return 0
    mapping = build_mapping()
    validate_mapping(mapping)
    records = run_migration(mapping, dry_run=not args.apply)
    payload = {
        "schema_version": "1.0",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "mode": "applied" if args.apply else "dry_run",
        "project_root": str(ROOT),
        "move_count": len(records),
        "moves": records,
    }
    if args.apply:
        LOG_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "mode": payload["mode"],
        "move_count": len(records),
        "file_count": sum(int(r["file_count"]) for r in records),
        "bytes": sum(int(r["bytes"]) for r in records),
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
