#!/usr/bin/env python3
"""Run the bounded, offline acceptance checks sealed into this classification release."""
from pathlib import Path
import os
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
SUITES = {
    "backend": ["test_v8_account_classification", "test_v8_account_directory", "test_v8_account_operating_status",
        "test_v8_account_status_api", "test_v8_account_creation_api", "test_v8_operations",
        "test_v8_account_directory_status", "test_v8_cleanup_account_status",
        "test_v8_account_operating_receipts", "test_v8_account_states"],
    "reports": ["test_v8_evaluation_selectors", "test_v8_report_export", "test_v8_report_inputs", "test_v8_reports"],
    "release": ["test_account_classification_install", "test_account_classification_release",
        "test_account_classification_snapshot_deployment", "test_account_classification_publication",
        "test_server_snapshot_deployment", "test_server_schema_upgrade", "test_macos_snapshot_publisher",
        "test_macos_snapshot_publisher_recovery", "test_matrix_snapshot_publisher"],
}


def main():
    part = sys.argv[1]
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": os.pathsep.join((str(ROOT / "src/dcar_eval"), str(ROOT / "scripts")))}
    if part in SUITES:
        subprocess.run([sys.executable, "-B", "-m", "unittest", *["tests." + name for name in SUITES[part]], "-q"],
            cwd=ROOT, env=environment, check=True)
        return
    if part != "frontend":
        raise ValueError("Unknown classification check")
    commands = [
        ["./node_modules/.bin/tsc", "--noEmit"],
        ["npm", "exec", "--yes", "--package=node@22.13.1", "--", "node", "--experimental-strip-types", "--test",
            "tests/account-create.test.mjs", "tests/query-cache.test.mjs", "tests/account-access.test.mjs"],
        ["npm", "exec", "--yes", "--package=node@22.13.1", "--", "node", "--experimental-strip-types", "--test",
            "--test-name-pattern=routes preserve", "tests/rendered-html.test.mjs"],
        ["npm", "run", "build"],
    ]
    for command in commands:
        subprocess.run(command, cwd=ROOT / "app/web", env=environment, check=True)


if __name__ == "__main__":
    main()
