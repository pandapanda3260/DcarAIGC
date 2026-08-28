#!/usr/bin/env python3
"""Build and inspect the frozen selling-points-v5.3 offline evaluation set."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "src" / "dcar_eval"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from v8 import selling_point_offline as offline  # type: ignore[import-not-found] # noqa: E402
from v8 import selling_point_paid_eval as paid  # type: ignore[import-not-found] # noqa: E402
from v8 import selling_point_g1 as g1  # type: ignore[import-not-found] # noqa: E402


DEFAULT_GOLD = Path(
    "/Users/mark/Documents/Dcar/准确度训练_卖点内容链接_修订版_2026-08-23.xlsx"
)
DEFAULT_DB = PROJECT_ROOT / "app" / "data" / "dcar_insight.sqlite3"
DEFAULT_G1_MODEL_CONFIG = (
    PROJECT_ROOT / "config" / "selling_point_stage_a_g1_v11.json"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    build = commands.add_parser("build-manifest")
    build.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    build.add_argument("--db", type=Path, default=DEFAULT_DB)
    build.add_argument("--output-dir", type=Path, required=True)

    dry_run = commands.add_parser("dry-run")
    dry_run.add_argument("--manifest", type=Path, required=True)
    dry_run.add_argument("--row", type=int, required=True)

    run = commands.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument(
        "--model-config",
        type=Path,
        default=paid.DEFAULT_MODEL_CONFIG_PATH,
    )
    run.add_argument("--output-dir", type=Path, required=True)
    run.add_argument("--concurrency", type=int, default=6)
    run.add_argument("--model", dest="models", action="append")

    run_g1 = commands.add_parser("run-g1")
    run_g1.add_argument("--manifest", type=Path, required=True)
    run_g1.add_argument(
        "--model-config",
        type=Path,
        default=DEFAULT_G1_MODEL_CONFIG,
    )
    run_g1.add_argument("--output-dir", type=Path, required=True)
    run_g1.add_argument("--concurrency", type=int, default=6)
    run_g1.add_argument("--prior-g1-summary", type=Path)
    return parser


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise offline.SellingPointOfflineError(
            f"cannot read JSON artifact {path}: {error}"
        ) from error
    if not isinstance(value, dict):
        raise offline.SellingPointOfflineError(f"JSON artifact must be an object: {path}")
    return value


def _verify_manifest(value: dict[str, Any]) -> None:
    expected = str(value.get("manifest_sha256") or "")
    unsigned = dict(value)
    unsigned.pop("manifest_sha256", None)
    actual = offline.sha256_json(unsigned)
    if not expected or actual != expected:
        raise offline.SellingPointOfflineError("development manifest hash mismatch")


def _write_new_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as output:
        json.dump(
            value,
            output,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        output.write("\n")


def _build_manifest(arguments: argparse.Namespace) -> dict[str, Any]:
    gold_path = arguments.gold.resolve()
    db_path = arguments.db.resolve()
    output_dir = arguments.output_dir.resolve()
    if not gold_path.is_file():
        raise offline.SellingPointOfflineError(f"gold workbook is missing: {gold_path}")
    if not db_path.is_file():
        raise offline.SellingPointOfflineError(f"database is missing: {db_path}")
    source_sha_before = offline.sha256_file(gold_path)
    manifest, exclusion = offline.build_development_artifacts(
        gold_path=gold_path,
        db_path=db_path,
    )
    index = offline.CharNgramTfidfIndex(manifest["rows"])
    index_payload = index.freeze_payload(
        development_manifest_sha256=str(manifest["manifest_sha256"])
    )
    index_payload["manifest_sha256"] = offline.sha256_json(index_payload)
    derived_gold = offline.derive_gold_manifest(manifest)
    g0_assertions = offline.build_g0_assertion_report(manifest)

    manifest_path = output_dir / "development_manifest.json"
    exclusion_path = output_dir / "development_exclusion_manifest.json"
    index_path = output_dir / "retrieval_index.json"
    derived_gold_path = output_dir / "derived_gold_manifest.json"
    g0_assertions_path = output_dir / "g0_assertions.json"
    _write_new_json(manifest_path, manifest)
    _write_new_json(exclusion_path, exclusion)
    _write_new_json(index_path, index_payload)
    _write_new_json(derived_gold_path, derived_gold)
    _write_new_json(g0_assertions_path, g0_assertions)
    source_sha_after = offline.sha256_file(gold_path)
    if source_sha_after != source_sha_before:
        raise offline.SellingPointOfflineError("source workbook changed during manifest build")
    return {
        "development_manifest": str(manifest_path),
        "development_manifest_sha256": manifest["manifest_sha256"],
        "development_exclusion_manifest": str(exclusion_path),
        "development_exclusion_manifest_sha256": exclusion["manifest_sha256"],
        "derived_gold_manifest": str(derived_gold_path),
        "derived_gold_manifest_sha256": derived_gold["manifest_sha256"],
        "g0_assertions": str(g0_assertions_path),
        "g0_assertions_sha256": g0_assertions["report_sha256"],
        "retrieval_index": str(index_path),
        "retrieval_index_sha256": index.index_sha256,
        "source_gold_sha256_before": source_sha_before,
        "source_gold_sha256_after": source_sha_after,
        "counts": manifest["counts"],
        "duplicate_groups": manifest["duplicate_groups"],
        "exclusion_count": len(exclusion["entries"]),
        "retrieval_document_count": len(index.rows),
    }


def _dry_run(arguments: argparse.Namespace) -> dict[str, Any]:
    manifest = _load_json(arguments.manifest.resolve())
    _verify_manifest(manifest)
    rows = manifest.get("rows")
    if not isinstance(rows, list):
        raise offline.SellingPointOfflineError("development manifest rows are missing")
    matches = [row for row in rows if int(row.get("excel_row") or 0) == arguments.row]
    if len(matches) != 1:
        raise offline.SellingPointOfflineError(
            f"development row must exist exactly once: {arguments.row}"
        )
    target = matches[0]
    if target.get("evidence_level") == "V0":
        return {
            "excel_row": arguments.row,
            "content_id": target["content_id"],
            "evidence_level": "V0",
            "model_call": False,
            "reason": "V0 rows count as incorrect and never generate a prompt",
        }
    index = offline.CharNgramTfidfIndex(rows)
    prompt = offline.build_prompt(target, index=index)
    return {
        "excel_row": arguments.row,
        "content_id": target["content_id"],
        "evidence_level": target["evidence_level"],
        "model_call": True,
        "prompt_version": prompt["prompt_version"],
        "retrieval_index_sha256": index.index_sha256,
        "hard_priority": prompt["priority"],
        "example_rows": [item["excel_row"] for item in prompt["examples"]],
        "system_chars": len(prompt["system"]),
        "user_chars": len(prompt["user"]),
    }


def _run(arguments: argparse.Namespace) -> dict[str, Any]:
    manifest = _load_json(arguments.manifest.resolve())
    _verify_manifest(manifest)
    return paid.run_bakeoff(
        manifest=manifest,
        model_config_path=arguments.model_config.resolve(),
        output_dir=arguments.output_dir.resolve(),
        concurrency=int(arguments.concurrency),
        selected_model_ids=arguments.models,
    )


def _run_g1(arguments: argparse.Namespace) -> dict[str, Any]:
    manifest = _load_json(arguments.manifest.resolve())
    _verify_manifest(manifest)
    prior_cost = 0.0
    if arguments.prior_g1_summary is not None:
        prior = _load_json(arguments.prior_g1_summary.resolve())
        if prior.get("development_manifest_sha256") != manifest["manifest_sha256"]:
            raise offline.SellingPointOfflineError(
                "prior G1 summary uses a different development manifest"
            )
        prior_cost = float(prior.get("new_cost_cny_upper_bound") or 0)
    return g1.run_g1(
        manifest=manifest,
        model_config_path=arguments.model_config.resolve(),
        output_dir=arguments.output_dir.resolve(),
        concurrency=int(arguments.concurrency),
        prior_new_cost_cny_upper_bound=prior_cost,
    )


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        if arguments.command == "build-manifest":
            result = _build_manifest(arguments)
        elif arguments.command == "dry-run":
            result = _dry_run(arguments)
        elif arguments.command == "run":
            result = _run(arguments)
        elif arguments.command == "run-g1":
            result = _run_g1(arguments)
        else:  # pragma: no cover - argparse owns this invariant
            raise offline.SellingPointOfflineError(
                f"unsupported command: {arguments.command}"
            )
    except Exception as error:  # noqa: BLE001 - CLI boundary must report exact failure
        json.dump(
            {"ok": False, "error": str(error), "error_type": type(error).__name__},
            sys.stderr,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        sys.stderr.write("\n")
        return 1
    json.dump(
        {"ok": True, "result": result},
        sys.stdout,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
