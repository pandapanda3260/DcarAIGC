from __future__ import annotations

import hashlib
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import run_local_analysis_canary as local_canary
from scripts import run_paid_source_refresh_canary as paid
from tests.test_run_local_analysis_canary import (
    LocalAnalysisCanaryControllerTest as _LocalAnalysisCanaryControllerTest,
)


def _subprocess_paid_refresh_worker(config_path: str) -> None:
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    marker = Path(config["marker"])
    detail_counter = Path(config["detail_counter"])
    metadata_counter = Path(config["metadata_counter"])

    def stop_at_marker(value: str) -> None:
        marker.write_text(value + "\n", encoding="utf-8")
        while True:
            time.sleep(1)

    def detail(_content_id: str, _key: str):
        count = int(detail_counter.read_text(encoding="utf-8")) if detail_counter.exists() else 0
        detail_counter.write_text(str(count + 1), encoding="utf-8")
        if config["mode"] == "opening":
            stop_at_marker("detail-entered")
        if config["mode"] == "transport_db":
            raise OSError("subprocess simulated TLS failure")
        return config["payload"], config["transcript"]

    def metadata_call(kind: str, value):
        count = (
            int(metadata_counter.read_text(encoding="utf-8"))
            if metadata_counter.exists()
            else 0
        )
        metadata_counter.write_text(str(count + 1), encoding="utf-8")
        if config["mode"] == kind:
            stop_at_marker(f"{kind}-entered")
        return value

    kwargs = {
        "source_db_path": Path(config["source_db_path"]),
        "source_completion_path": Path(config["source_completion_path"]),
        "expected_source_db_sha256": config["expected_source_db_sha256"],
        "expected_source_completion_sha256": config[
            "expected_source_completion_sha256"
        ],
        "db_path": Path(config["db_path"]),
        "raw_root": Path(config["raw_root"]),
        "media_root": Path(config["media_root"]),
        "run_root": Path(config["run_root"]),
        "content_ids": [1],
        "endpoint_info_fetcher": lambda: metadata_call("price", config["price"]),
        "balance_checker": lambda *_: metadata_call("user_info", config["balance"]),
        "detail_fetcher": detail,
        "key_loader": lambda: "fixture-key",
    }
    mode = config["mode"]
    if mode == "transport_db":
        original_transport_commit = paid._commit_transport_failed_capture

        def transport_commit_then_wait(*args, **kwargs):
            stop_at_marker("transport-ledger-terminal")
            return original_transport_commit(*args, **kwargs)

        with patch.object(
            paid,
            "_commit_transport_failed_capture",
            side_effect=transport_commit_then_wait,
        ):
            paid.run_refresh(**kwargs)
        return

    if mode == "raw_wal":
        original_commit = paid._commit_successful_capture

        def commit_then_wait(*args, **kwargs):
            outcome = original_commit(*args, **kwargs)
            stop_at_marker("raw-committed")
            return outcome

        with patch.object(paid, "_commit_successful_capture", commit_then_wait):
            paid.run_refresh(**kwargs)
        return

    if mode == "raw_final":
        original_atomic_bytes = paid.capture_module._atomic_bytes

        def raw_final_then_wait(path, body):
            original_atomic_bytes(path, body)
            stop_at_marker("raw-final")

        with patch.object(
            paid.capture_module, "_atomic_bytes", side_effect=raw_final_then_wait
        ):
            paid.run_refresh(**kwargs)
        return

    if mode == "manifest_final":
        original_stage_manifest = paid.media_module._stage_private_media_source_json

        def manifest_final_then_wait(path, body, **kwargs):
            result = original_stage_manifest(path, body, **kwargs)
            stop_at_marker("manifest-final")
            return result

        with patch.object(
            paid.media_module,
            "_stage_private_media_source_json",
            side_effect=manifest_final_then_wait,
        ):
            paid.run_refresh(**kwargs)
        return

    if mode == "artifact_committed":
        original_records = paid._completion_records

        def artifact_then_wait(*args, **kwargs):
            stop_at_marker("artifact-committed")
            return original_records(*args, **kwargs)

        with patch.object(paid, "_completion_records", side_effect=artifact_then_wait):
            paid.run_refresh(**kwargs)
        return

    rename_modes = {
        "terminal_ledger_temp",
        "terminal_ledger_final",
        "state_temp",
        "receipt_temp",
        "completion_temp",
    }
    if mode in rename_modes:
        destinations = {
            "terminal_ledger_temp": Path(config["run_root"]) / "provider-ledger.json",
            "terminal_ledger_final": Path(config["run_root"]) / "provider-ledger.json",
            "state_temp": Path(config["run_root"]) / "refresh-state.json",
            "receipt_temp": Path(config["run_root"]) / "refresh-receipt.json",
            "completion_temp": Path(config["run_root"]) / "completion.json",
        }
        target = destinations[mode]
        original_replace = os.replace

        def replace(source, destination):
            source_path = Path(source)
            destination_path = Path(destination)
            if destination_path == target:
                should_stop = mode != "terminal_ledger_temp"
                if mode.startswith("terminal_ledger"):
                    try:
                        candidate = json.loads(source_path.read_bytes())
                    except (OSError, json.JSONDecodeError):
                        candidate = {}
                    should_stop = len(candidate.get("events") or []) == 2
                if should_stop:
                    if mode == "terminal_ledger_final":
                        original_replace(source, destination)
                    stop_at_marker(mode)
            return original_replace(source, destination)

        with patch.object(paid.local_controller.os, "replace", side_effect=replace):
            paid.run_refresh(**kwargs)
        return

    paid.run_refresh(**kwargs)


class PaidSourceRefreshCanaryTest(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = _LocalAnalysisCanaryControllerTest(methodName="runTest")
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.root = self.fixture.root / "paid"
        self.root.mkdir()
        self.db_parent = self.root / "db"
        self.db_parent.mkdir()
        self.db = self.db_parent / "refresh.sqlite3"
        self.raw_root = self.root / "raw"
        self.media_root = self.root / "media"
        self.run_root = self.root / "run"
        self.source_db_sha = local_canary._sha256_file(self.fixture.source_db)
        self.source_completion_sha = local_canary._sha256_file(
            self.fixture.source_completion
        )
        self.video_url = "https://v26.douyinvod.com/fresh-video.mp4?token=frozen"
        self.music_url = "https://sf6-cdn-tos.douyinstatic.com/music/fresh.mp3"
        self.calls = {"price": 0, "balance": 0, "detail": 0}

    @staticmethod
    def _sha(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()

    def _price(self, *, checked_at: str | None = None):
        self.calls["price"] += 1
        rows = []
        for index, (endpoint, cost) in enumerate(
            (
                (paid.ENDPOINT_INFO_PATH, 0.0),
                (paid.USER_INFO_PATH, 0.0),
                (paid.DETAIL_PATH, paid.UNIT_PRICE),
            )
        ):
            rows.append(
                {
                    "queried_endpoint": endpoint,
                    "response": {
                        "url_sha256": paid._request_url_sha256(
                            paid.ENDPOINT_INFO_PATH, {"endpoint": endpoint}
                        ),
                        "response_sha256": self._sha(f"price-body-{index}"),
                        "response_bytes": 100 + index,
                        "http_status": 200,
                        "mime_type": "application/json",
                    },
                    "fields": {
                        "requested_endpoint": endpoint,
                        "endpoint_uri": endpoint,
                        "endpoint_cost": cost,
                        "self_operated": True,
                        "rate_limit": "1/second" if index < 2 else "10/second",
                    },
                }
            )
        return {
            "checked_at": checked_at
            or datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "records": rows,
            "records_sha256": paid._json_sha256(rows),
        }

    def _balance(self, _key: str, price_sha256: str):
        self.calls["balance"] += 1
        return {
            "checked_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "endpoint": paid.USER_INFO_PATH,
            "endpoint_cost": 0.0,
            "balance_sufficient": True,
            "response_sha256": self._sha("balance-body"),
            "response_bytes": 123,
            "price_evidence_sha256": price_sha256,
        }

    def _payload(self, *, music_urls: list[str] | None = None):
        return {
            "code": 200,
            "params": {"aweme_id": "canary-1"},
            "data": {
                "aweme_detail": {
                    "aweme_id": "canary-1",
                    "desc": "完整标题",
                    "create_time": 1_786_000_000,
                    "author": {"uid": "fixture-uid", "nickname": "fixture"},
                    "video": {
                        "play_addr": {"url_list": [self.video_url]},
                        "download_addr": {"url_list": []},
                        "bit_rate": [],
                    },
                    "music": {
                        "play_url": {"url_list": music_urls or [self.music_url]},
                    },
                }
            },
        }

    def _detail(self, _content_id: str, _key: str):
        self.calls["detail"] += 1
        payload = self._payload()
        body = json.dumps(payload, ensure_ascii=False).encode()
        return payload, {
            "url_sha256": paid._request_url_sha256(
                paid.DETAIL_PATH, {"aweme_id": "canary-1"}
            ),
            "response_sha256": hashlib.sha256(body).hexdigest(),
            "response_json_sha256": hashlib.sha256(
                paid.capture_module.canonical_json_bytes(payload)
            ).hexdigest(),
            "response_bytes": len(body),
            "http_status": 200,
            "mime_type": "application/json",
            "endpoint": paid.DETAIL_PATH,
            "aweme_id": "canary-1",
        }

    def _kwargs(self) -> dict[str, object]:
        return {
            "source_db_path": self.fixture.source_db,
            "source_completion_path": self.fixture.source_completion,
            "expected_source_db_sha256": self.source_db_sha,
            "expected_source_completion_sha256": self.source_completion_sha,
            "db_path": self.db,
            "raw_root": self.raw_root,
            "media_root": self.media_root,
            "run_root": self.run_root,
            "content_ids": [1],
        }

    def _run(self, **overrides):
        values = {
            **self._kwargs(),
            "endpoint_info_fetcher": self._price,
            "balance_checker": self._balance,
            "detail_fetcher": self._detail,
            "key_loader": lambda: "fixture-key",
            **overrides,
        }
        return paid.run_refresh(**values)

    def _new_case(self) -> "PaidSourceRefreshCanaryTest":
        value = PaidSourceRefreshCanaryTest(methodName="runTest")
        value.setUp()
        self.addCleanup(value.doCleanups)
        return value

    def _subprocess_config(self, *, mode: str) -> tuple[Path, Path]:
        price = self._price()
        balance = self._balance("fixture-key", paid._json_sha256(price["records"][1]))
        payload = self._payload()
        body = json.dumps(payload, ensure_ascii=False).encode()
        transcript = {
            "url_sha256": paid._request_url_sha256(
                paid.DETAIL_PATH, {"aweme_id": "canary-1"}
            ),
            "response_sha256": hashlib.sha256(body).hexdigest(),
            "response_json_sha256": hashlib.sha256(
                paid.capture_module.canonical_json_bytes(payload)
            ).hexdigest(),
            "response_bytes": len(body),
            "http_status": 200,
            "mime_type": "application/json",
            "endpoint": paid.DETAIL_PATH,
            "aweme_id": "canary-1",
        }
        marker = self.root / f"{mode}.marker"
        detail_counter = self.root / f"{mode}.detail-count"
        metadata_counter = self.root / f"{mode}.metadata-count"
        config = {
            **{
                key: str(value)
                for key, value in self._kwargs().items()
                if key != "content_ids"
            },
            "mode": mode,
            "marker": str(marker),
            "detail_counter": str(detail_counter),
            "metadata_counter": str(metadata_counter),
            "price": price,
            "balance": balance,
            "payload": payload,
            "transcript": transcript,
        }
        config_path = self.root / f"{mode}.config.json"
        config_path.write_text(
            json.dumps(config, ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return config_path, marker

    def _subprocess_detail_count(self, *, mode: str) -> int:
        path = self.root / f"{mode}.detail-count"
        return int(path.read_text(encoding="utf-8")) if path.exists() else 0

    def _kill_subprocess_at_marker(self, *, mode: str) -> Path:
        config_path, marker = self._subprocess_config(mode=mode)
        environment = {
            **os.environ,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPATH": ".:src/dcar_eval",
            "DCAR_TEST_DENY_FORMAL_DB": "1",
            "HF_HUB_OFFLINE": "1",
            "TRANSFORMERS_OFFLINE": "1",
        }
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "from tests.test_run_paid_source_refresh_canary import "
                "_subprocess_paid_refresh_worker as worker; "
                "import sys; worker(sys.argv[1])",
                str(config_path),
            ],
            cwd=paid.storage_module.PROJECT_ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 10
        while not marker.exists() and process.poll() is None:
            if time.monotonic() >= deadline:
                process.kill()
                process.wait(timeout=5)
                self.fail(f"subprocess did not reach {mode} marker")
            time.sleep(0.02)
        if not marker.exists():
            return_code = process.wait(timeout=5)
            self.fail(f"subprocess exited before {mode} marker: {return_code}")
        os.kill(process.pid, signal.SIGKILL)
        self.assertEqual(process.wait(timeout=5), -signal.SIGKILL)
        return marker

    @staticmethod
    def _tree(root: Path) -> list[tuple[str, int, int, str]]:
        rows = []
        for path in sorted(root.rglob("*")):
            metadata = path.lstat()
            if path.is_file():
                rows.append(
                    (
                        str(path.relative_to(root)),
                        metadata.st_size,
                        metadata.st_mtime_ns,
                        local_canary._sha256_file(path),
                    )
                )
        return rows

    @staticmethod
    def _sequence_snapshot(database: Path) -> dict[str, int]:
        with closing(sqlite3.connect(database)) as connection:
            return {
                str(name): int(sequence)
                for name, sequence in connection.execute(
                    "SELECT name,seq FROM sqlite_sequence ORDER BY name"
                )
            }

    @staticmethod
    def _expected_sequences(
        contract: dict[str, object], *, incremented: set[str]
    ) -> dict[str, int]:
        baseline = contract["database_baseline"]
        assert isinstance(baseline, dict)
        values = baseline["sqlite_sequence"]
        assert isinstance(values, dict)
        names = set(values) | incremented
        return {
            str(name): int(values.get(name, 0)) + (1 if name in incremented else 0)
            for name in names
        }

    def test_default_plan_is_read_only_and_zero_network(self) -> None:
        before = self._tree(self.fixture.step3_root)
        result = paid.plan_refresh(**self._kwargs())
        self.assertEqual(result["status"], "planned")
        self.assertEqual(result["provider_calls_planned"], 1)
        self.assertEqual(result["metadata_calls_planned"], 4)
        self.assertEqual(len(result["metadata_request_plan"]), 4)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.raw_root.exists())
        self.assertFalse(self.media_root.exists())
        self.assertFalse(self.run_root.exists())
        self.assertEqual(before, self._tree(self.fixture.step3_root))
        self.assertEqual(self.calls, {"price": 0, "balance": 0, "detail": 0})

    def test_task_identity_uses_capture_canonical_budget_id(self) -> None:
        identity = paid._task_identity(
            source_db_sha256="a" * 64,
            source_completion_sha256="b" * 64,
            content_id=1,
        )
        task_digest = hashlib.sha256(
            identity["task_id"].encode("utf-8")
        ).hexdigest()[:16]
        self.assertEqual(
            identity["budget_id"],
            f"task-{task_digest}-tikhub-{paid.OPERATION}-v1",
        )

    def test_success_binds_durable_metadata_ledger_and_frozen_transport(self) -> None:
        result = self._run()
        self.assertEqual(result["status"], "succeeded")
        metadata_path = self.run_root / "metadata-ledger.json"
        metadata = paid._read_json(metadata_path, label="metadata ledger")
        metadata_contract = paid._read_json(
            self.run_root / "metadata-contract.json", label="metadata contract"
        )
        self.assertEqual(metadata["version"], 2)
        self.assertEqual(metadata["completion_kind"], "paid-source-refresh-v2")
        self.assertEqual(metadata["transport_profile"], paid.TRANSPORT_PROFILE)
        self.assertEqual(len(metadata_contract["request_plan"]), 4)
        self.assertEqual(len(metadata["events"]), 8)
        for request in metadata_contract["request_plan"]:
            request_events = [
                event
                for event in metadata["events"]
                if event["request_id"] == request["request_id"]
            ]
            self.assertEqual(
                [event["phase"] for event in request_events],
                ["opening", "terminal"],
            )
        self.assertEqual(
            [event["outcome"] for event in metadata["events"] if event["phase"] == "terminal"],
            ["response_received"] * 4,
        )
        metadata_sha = local_canary._sha256_file(metadata_path)
        contract = paid._read_json(
            self.run_root / "refresh-contract.json", label="contract"
        )
        state = paid._read_json(self.run_root / "refresh-state.json", label="state")
        receipt = paid._read_json(
            self.run_root / "refresh-receipt.json", label="receipt"
        )
        self.assertEqual(
            contract["metadata"]["price_prefix_sha256"],
            paid._json_sha256(metadata["events"][:6]),
        )
        self.assertEqual(state["metadata_ledger_sha256"], metadata_sha)
        self.assertEqual(receipt["metadata_ledger_sha256"], metadata_sha)

    def test_metadata_contract_precedes_price_and_binds_code_and_runtime(self) -> None:
        observed: dict[str, object] = {}

        def price_after_contract():
            metadata_contract = paid._read_json(
                self.run_root / "metadata-contract.json",
                label="pre-price metadata contract",
            )
            observed["code"] = metadata_contract["code"]
            observed["runtime"] = metadata_contract["runtime"]
            return self._price()

        self._run(endpoint_info_fetcher=price_after_contract)
        self.assertEqual(observed["code"], paid._code_snapshot())
        self.assertEqual(observed["runtime"], paid._runtime_snapshot())
        runtime = observed["runtime"]
        self.assertEqual(runtime["implementation"], "CPython")
        self.assertEqual(runtime["openssl_version"], paid.ssl.OPENSSL_VERSION)
        executable = runtime["executable"]
        self.assertEqual(executable["path"], str(Path(sys.executable).resolve()))
        self.assertRegex(executable["sha256"], r"^[0-9a-f]{64}$")
        self.assertGreater(executable["byte_size"], 0)

    def test_runtime_drift_blocks_before_metadata_callback(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="price")
        self.assertTrue(marker.exists())
        current = paid._runtime_snapshot()
        drifted = {**current, "openssl_version": current["openssl_version"] + "-drift"}
        callbacks = {"count": 0}

        def forbidden(*_args, **_kwargs):
            callbacks["count"] += 1
            raise AssertionError("runtime drift reached metadata callback")

        with patch.object(paid, "_runtime_snapshot", return_value=drifted):
            with self.assertRaises(paid.PaidSourceRefreshError):
                paid.run_refresh(
                    **self._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
        self.assertEqual(callbacks["count"], 0)

    def test_success_and_idempotent_results_expose_exact_call_and_amount_history(self) -> None:
        first = self._run()
        current = first["provider_call_accounting"]["current"]
        historical = first["provider_call_accounting"]["historical"]
        self.assertEqual(
            current,
            {
                "endpoint_info": 3,
                "user_info": 1,
                "metadata": 4,
                "detail": 1,
                "total": 5,
            },
        )
        self.assertEqual(historical["calls"], current)
        self.assertEqual(
            historical["amounts"],
            {
                "currency": "USD",
                "endpoint_info": 0.0,
                "user_info": 0.0,
                "metadata": 0.0,
                "detail": paid.UNIT_PRICE,
                "total": paid.UNIT_PRICE,
                "basis": "successful_detail_provider_usage_exact",
            },
        )
        receipt = paid._read_json(
            self.run_root / "refresh-receipt.json", label="call history receipt"
        )
        completion = paid._read_json(
            self.run_root / "completion.json", label="call history completion"
        )
        self.assertEqual(receipt["provider_call_history"], historical)
        self.assertEqual(completion["provider_call_history"], historical)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("idempotent accounting attempted network")

        rerun = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertEqual(
            rerun["provider_call_accounting"]["current"],
            {
                "endpoint_info": 0,
                "user_info": 0,
                "metadata": 0,
                "detail": 0,
                "total": 0,
            },
        )
        self.assertEqual(
            rerun["provider_call_accounting"]["historical"], historical
        )

    def test_exception_secrets_never_persist_in_outputs_or_database_text(self) -> None:
        sentinel = "Bearer fixture-key SUPER-SECRET-SENTINEL-9d3d"

        def assert_secret_absent(case: "PaidSourceRefreshCanaryTest") -> None:
            needle = sentinel.encode()
            for root in (case.run_root, case.raw_root, case.media_root):
                if root.exists():
                    for path in root.rglob("*"):
                        if path.is_file():
                            self.assertNotIn(needle, path.read_bytes(), str(path))
            if case.db.exists():
                paid.local_controller._finalize_database(case.db)
                with closing(sqlite3.connect(case.db)) as connection:
                    tables = [
                        str(row[0])
                        for row in connection.execute(
                            "SELECT name FROM sqlite_master WHERE type='table'"
                        )
                        if not str(row[0]).startswith("sqlite_")
                    ]
                    for table in tables:
                        escaped = table.replace('"', '""')
                        for row in connection.execute(f'SELECT * FROM "{escaped}"'):
                            for value in row:
                                if isinstance(value, str):
                                    self.assertNotIn(sentinel, value, table)

        for phase in ("user_info", "detail", "rejected"):
            with self.subTest(phase=phase):
                case = self._new_case()
                overrides: dict[str, object] = {
                    "key_loader": lambda: "fixture-key"
                }
                context = patch.object(
                    paid.providers_module,
                    "_parse_douyin_stage_payload",
                    wraps=paid.providers_module._parse_douyin_stage_payload,
                )
                if phase == "user_info":
                    overrides["balance_checker"] = lambda *_: (_ for _ in ()).throw(
                        RuntimeError(sentinel)
                    )
                elif phase == "detail":
                    overrides["detail_fetcher"] = lambda *_: (_ for _ in ()).throw(
                        RuntimeError(sentinel)
                    )
                else:
                    context = patch.object(
                        paid.providers_module,
                        "_parse_douyin_stage_payload",
                        side_effect=RuntimeError(sentinel),
                    )
                with context, self.assertRaises(paid.PaidSourceRefreshError):
                    case._run(**overrides)
                assert_secret_absent(case)

    def test_cli_error_is_type_and_code_only(self) -> None:
        sentinel = "Bearer fixture-key CLI-SUPER-SECRET"
        argv = [
            "--source-db",
            str(self.fixture.source_db),
            "--source-completion",
            str(self.fixture.source_completion),
            "--expected-source-db-sha256",
            self.source_db_sha,
            "--expected-source-completion-sha256",
            self.source_completion_sha,
            "--db",
            str(self.db),
            "--raw-root",
            str(self.raw_root),
            "--media-root",
            str(self.media_root),
            "--run-root",
            str(self.run_root),
            "--content-id",
            "1",
            "--apply",
        ]
        for error in (
            paid.PaidSourceRefreshError(sentinel),
            OSError(sentinel),
        ):
            with self.subTest(error_type=type(error).__name__), patch.object(
                paid, "run_refresh", side_effect=error
            ), patch("builtins.print") as printed:
                self.assertEqual(paid.main(argv), 2)
            output = printed.call_args.args[0]
            self.assertNotIn(sentinel, output)
            self.assertEqual(
                json.loads(output),
                {
                    "ok": False,
                    "status": "blocked",
                    "error_type": type(error).__name__,
                    "error_code": "paid_source_refresh_blocked",
                },
            )

    def test_exception_class_name_is_sanitized_before_persistence(self) -> None:
        secret_type = "Bearer_fixture_key_CLASS_SECRET_51a9"
        secret_error = type(secret_type, (RuntimeError,), {})

        def failure(*_args, **_kwargs):
            raise secret_error("safe-message")

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(balance_checker=failure)
        for path in self.run_root.rglob("*"):
            if path.is_file():
                self.assertNotIn(secret_type.encode(), path.read_bytes(), str(path))
        ledger = paid._read_json(
            self.run_root / "metadata-ledger.json", label="sanitized type ledger"
        )
        self.assertEqual(ledger["events"][-1]["error_type"], "Exception")

    def test_price_sigkill_is_durably_consumed_and_never_retried(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="price")
        self.assertEqual(marker.read_text(encoding="utf-8"), "price-entered\n")
        metadata = paid._read_json(
            self.run_root / "metadata-ledger.json", label="price opening metadata"
        )
        self.assertEqual(len(metadata["events"]), 3)
        self.assertTrue(all(event["phase"] == "opening" for event in metadata["events"]))
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("price SIGKILL prefix retried metadata network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)

    def test_transport_terminal_ledger_running_db_split_recovers_locally(self) -> None:
        def transport_failure(*_args, **_kwargs):
            raise OSError("simulated TLS failure")

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(detail_fetcher=transport_failure)
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute(
                "UPDATE provider_usage SET details_json=? "
                "WHERE task_id LIKE 'paid-source-refresh-%'",
                ('{"state":"reserved"}',),
            )
            connection.execute(
                "UPDATE fetch_attempts SET response_finished_at=NULL,http_status=NULL,"
                "error_code=NULL,error_message=NULL WHERE slot_id=(SELECT id FROM "
                "fetch_slots WHERE content_id=1 AND stage=?)",
                (paid.STAGE,),
            )
            connection.execute(
                "UPDATE fetch_slots SET status='running',finished_at=NULL,"
                "last_error_code=NULL,last_error_message=NULL WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            )
            connection.commit()
        paid.local_controller._finalize_database(self.db)
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("split recovery used network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)
        with closing(local_canary._immutable_connection(self.db)) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            ).fetchone()
            usage = connection.execute(
                "SELECT details_json FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
            ).fetchone()
        self.assertEqual(tuple(slot), ("terminal_failed", "transport_failed"))
        self.assertEqual(json.loads(usage[0])["outcome"], "transport_failed")

    def test_real_sigkill_transport_terminal_before_db_commit_recovers_zero_network(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="transport_db")
        self.assertEqual(
            marker.read_text(encoding="utf-8"), "transport-ledger-terminal\n"
        )
        self.assertEqual(self._subprocess_detail_count(mode="transport_db"), 1)
        ledger = paid._read_json(
            self.run_root / "provider-ledger.json", label="transport terminal ledger"
        )
        self.assertEqual(ledger["events"][-1]["outcome"], "transport_failed")
        with closing(sqlite3.connect(self.db)) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            ).fetchone()
            usage = connection.execute(
                "SELECT details_json FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
            ).fetchone()
        self.assertEqual(tuple(slot), ("running", None))
        self.assertEqual(json.loads(usage[0]), {"state": "reserved"})
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("transport split SIGKILL recovery used network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)
        self.assertEqual(self._subprocess_detail_count(mode="transport_db"), 1)
        with closing(local_canary._immutable_connection(self.db)) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            ).fetchone()
            usage = connection.execute(
                "SELECT details_json FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
            ).fetchone()
        self.assertEqual(tuple(slot), ("terminal_failed", "transport_failed"))
        self.assertEqual(
            json.loads(usage[0])["billing_basis"],
            "conservative_upper_bound",
        )

    def test_user_info_sigkill_is_durably_consumed_and_never_retried(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="user_info")
        self.assertEqual(marker.read_text(encoding="utf-8"), "user_info-entered\n")
        metadata = paid._read_json(
            self.run_root / "metadata-ledger.json", label="user opening metadata"
        )
        self.assertEqual(len(metadata["events"]), 7)
        self.assertEqual(metadata["events"][-1]["phase"], "opening")
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("user-info SIGKILL prefix retried network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)

    def test_detail_transport_failure_closes_db_upper_bound_and_reentry_zero_network(self) -> None:
        def transport_failure(*_args, **_kwargs):
            self.calls["detail"] += 1
            raise OSError("simulated TLS failure")

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(detail_fetcher=transport_failure)
        self.assertEqual(self.calls["detail"], 1)
        with closing(local_canary._immutable_connection(self.db)) as connection:
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            ).fetchone()
            attempt = connection.execute(
                "SELECT http_status,billed,amount,currency,error_code FROM fetch_attempts "
                "WHERE slot_id=(SELECT id FROM fetch_slots WHERE content_id=1 AND stage=?)",
                (paid.STAGE,),
            ).fetchone()
            usage = connection.execute(
                "SELECT request_attempts,billed_requests,amount,details_json FROM provider_usage "
                "WHERE task_id LIKE 'paid-source-refresh-%'"
            ).fetchone()
            budget = connection.execute(
                "SELECT consumed_requests,consumed_amount FROM provider_budget_batches "
                "WHERE purpose LIKE 'paid_source_refresh_%'"
            ).fetchone()
        self.assertEqual(tuple(slot), ("terminal_failed", "transport_failed"))
        self.assertEqual(tuple(attempt), (None, 1, paid.UNIT_PRICE, "USD", "transport_failed"))
        self.assertEqual(tuple(usage[:3]), (1, 1, paid.UNIT_PRICE))
        self.assertEqual(
            json.loads(usage[3]),
            {
                "billing_basis": "conservative_upper_bound",
                "outcome": "transport_failed",
                "slot_id": 1,
                "state": "completed",
            },
        )
        self.assertEqual(tuple(budget), (1, paid.UNIT_PRICE))
        ledger = paid._read_json(
            self.run_root / "provider-ledger.json", label="transport ledger"
        )
        self.assertEqual(ledger["events"][-1]["outcome"], "transport_failed")
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("transport terminal reentry used network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)

    def test_v1_root_is_rejected_without_mutation_or_network(self) -> None:
        self._run()
        contract_path = self.run_root / "refresh-contract.json"
        contract = dict(paid._read_json(contract_path, label="v2 contract"))
        contract["version"] = 1
        contract["completion_kind"] = "paid-source-refresh-v1"
        contract_path.write_bytes(paid._canonical_bytes(contract))
        before = self._tree(self.root)
        calls = {"count": 0}

        def forbidden(*_args, **_kwargs):
            calls["count"] += 1
            raise AssertionError("v1 rejection used network")

        with self.assertRaisesRegex(
            paid.PaidSourceRefreshError, "fresh v2 root"
        ):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(calls["count"], 0)
        self.assertEqual(before, self._tree(self.root))

    def test_blocked_state_temp_is_promoted_before_prefix_validation(self) -> None:
        state_path = self.run_root / "refresh-state.json"
        state_temp = self.run_root / ".refresh-state.json.tmp"
        original_replace = os.replace
        interrupted = {"value": False}

        def replace(source, destination):
            destination_path = Path(destination)
            if destination_path == state_path and not interrupted["value"]:
                candidate = json.loads(Path(source).read_bytes())
                if candidate.get("phase") == "blocked":
                    interrupted["value"] = True
                    raise RuntimeError("kill before blocked state rename")
            return original_replace(source, destination)

        def balance_failure(*_args, **_kwargs):
            raise RuntimeError("simulated user-info failure")

        with patch.object(
            paid.local_controller.os, "replace", side_effect=replace
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run(balance_checker=balance_failure)
        self.assertTrue(interrupted["value"])
        self.assertTrue(state_temp.is_file())
        self.assertFalse(state_path.exists())
        callbacks = {"count": 0}

        def forbidden(*_args, **_kwargs):
            callbacks["count"] += 1
            raise AssertionError("blocked state recovery used callback")

        original_generation_gate = paid._read_only_generation_gate
        gate_observed: dict[str, bool] = {}

        def generation_gate(paths, **keywords):
            gate_observed["state_final"] = state_path.is_file()
            gate_observed["state_temp"] = state_temp.exists()
            return original_generation_gate(paths, **keywords)

        with patch.object(
            paid, "_read_only_generation_gate", side_effect=generation_gate
        ), self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(callbacks["count"], 0)
        self.assertEqual(
            gate_observed, {"state_final": False, "state_temp": True}
        )
        self.assertTrue(state_path.is_file())
        self.assertFalse(state_temp.exists())
        state = paid._read_json(state_path, label="recovered blocked state")
        self.assertEqual(state["phase"], "blocked")

    def test_blocked_state_temp_race_accepts_only_the_exact_promoted_final(self) -> None:
        state_path = self.run_root / "refresh-state.json"
        state_temp = self.run_root / ".refresh-state.json.tmp"
        original_replace = os.replace

        def interrupt(source, destination):
            if Path(destination) == state_path:
                raise RuntimeError("kill before blocked state rename")
            return original_replace(source, destination)

        with patch.object(
            paid.local_controller.os, "replace", side_effect=interrupt
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run(
                balance_checker=lambda *_: (_ for _ in ()).throw(
                    RuntimeError("simulated user-info failure")
                )
            )
        self.assertTrue(state_temp.is_file())
        candidate = state_temp.read_bytes()
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )

        def competing_replace(source, destination):
            original_replace(source, destination)
            raise FileNotFoundError("another coordinator promoted the exact temp")

        with patch.object(paid.os, "replace", side_effect=competing_replace):
            paid._recover_blocked_state_temp(
                paths,
                expected_source_db_sha256=self.source_db_sha,
                expected_source_completion_sha256=self.source_completion_sha,
                content_id=1,
            )
        self.assertFalse(state_temp.exists())
        self.assertEqual(state_path.read_bytes(), candidate)

    def test_stale_blocked_temp_never_overwrites_a_succeeded_state(self) -> None:
        first = self._run()
        successful_tree = self._tree(self.root)
        state_path = self.run_root / "refresh-state.json"
        state_before = state_path.read_bytes()
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        state_temp = self.run_root / ".refresh-state.json.tmp"
        state_temp.write_bytes(
            paid._canonical_bytes(
                paid._blocked_state_value(
                    paths,
                    error=RuntimeError("stale blocked temp"),
                    error_code="paid_source_refresh_failed",
                )
            )
        )

        def forbidden(*_args, **_kwargs):
            raise AssertionError("stale blocked temp recovery used callback")

        rerun = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertTrue(rerun["idempotent"])
        self.assertEqual(rerun["completion_sha256"], first["completion_sha256"])
        self.assertEqual(state_path.read_bytes(), state_before)
        self.assertFalse(state_temp.exists())
        self.assertEqual(self._tree(self.root), successful_tree)

    def test_terminal_success_race_wins_before_blocked_temp_promotion(self) -> None:
        first = self._run()
        state_path = self.run_root / "refresh-state.json"
        receipt_path = self.run_root / "refresh-receipt.json"
        completion_path = self.run_root / "completion.json"
        succeeded_state = state_path.read_bytes()
        succeeded_receipt = receipt_path.read_bytes()
        succeeded_completion = completion_path.read_bytes()
        receipt_path.unlink()
        completion_path.unlink()
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        state_temp = self.run_root / ".refresh-state.json.tmp"
        state_temp.write_bytes(
            paid._canonical_bytes(
                paid._blocked_state_value(
                    paths,
                    error=RuntimeError("stale coordinator blocked state"),
                    error_code="paid_source_refresh_failed",
                )
            )
        )
        original_validate = paid._validate_succeeded_state_prefix
        terminal_landed = {"value": False}

        def land_terminal_success(prefix_paths, value):
            original_validate(prefix_paths, value)
            state_path.write_bytes(succeeded_state)
            receipt_path.write_bytes(succeeded_receipt)
            completion_path.write_bytes(succeeded_completion)
            terminal_landed["value"] = True

        def forbidden(*_args, **_kwargs):
            raise AssertionError("terminal-success race used callback")

        with patch.object(
            paid,
            "_validate_succeeded_state_prefix",
            side_effect=land_terminal_success,
        ):
            rerun = paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertTrue(terminal_landed["value"])
        self.assertTrue(rerun["idempotent"])
        self.assertEqual(rerun["completion_sha256"], first["completion_sha256"])
        self.assertEqual(state_path.read_bytes(), succeeded_state)
        self.assertEqual(receipt_path.read_bytes(), succeeded_receipt)
        self.assertEqual(completion_path.read_bytes(), succeeded_completion)
        self.assertFalse(state_temp.exists())

    def test_blocked_temp_closes_incomplete_success_prefix_locally(self) -> None:
        for mode in ("state-only-success", "receipt-only"):
            with self.subTest(mode=mode):
                case = self._new_case()
                case._run()
                completion_path = case.run_root / "completion.json"
                completion_path.unlink()
                if mode == "state-only-success":
                    (case.run_root / "refresh-receipt.json").unlink()
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                state_temp = case.run_root / ".refresh-state.json.tmp"
                state_temp.write_bytes(
                    paid._canonical_bytes(
                        paid._blocked_state_value(
                            paths,
                            error=RuntimeError("completion prefix interrupted"),
                            error_code="paid_source_refresh_failed",
                        )
                    )
                )

                def forbidden(*_args, **_kwargs):
                    raise AssertionError("incomplete success prefix used callback")

                resumed = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
                self.assertEqual(resumed["status"], "succeeded")
                self.assertEqual(resumed["provider_calls"], 0)
                self.assertTrue(completion_path.is_file())
                self.assertFalse(state_temp.exists())

    def test_v1_state_with_exact_v2_blocked_temp_rejects_without_mutation(self) -> None:
        self.run_root.mkdir()
        state_path = self.run_root / "refresh-state.json"
        state_path.write_text(
            '{"completion_kind":"paid-source-refresh-v1",'
            '"phase":"blocked","version":1}\n',
            encoding="utf-8",
        )
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        (self.run_root / ".refresh-state.json.tmp").write_bytes(
            paid._canonical_bytes(
                paid._blocked_state_value(
                    paths,
                    error=RuntimeError("exact v2 temp on v1 prefix"),
                    error_code="paid_source_refresh_failed",
                )
            )
        )
        before = self._tree(self.root)
        callbacks = {"count": 0}

        def forbidden(*_args, **_kwargs):
            callbacks["count"] += 1
            raise AssertionError("mixed v1/v2 prefix reached callback")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(callbacks["count"], 0)
        self.assertEqual(self._tree(self.root), before)

    def test_state_only_copy_only_and_unknown_prefixes_reject_before_any_write(self) -> None:
        for mode in ("state-only", "copy-only", "unknown"):
            with self.subTest(mode=mode):
                case = self._new_case()
                case.run_root.mkdir()
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                if mode == "state-only":
                    (case.run_root / "refresh-state.json").write_text(
                        '{"completion_kind":"paid-source-refresh-v1",'
                        '"phase":"blocked","version":1}\n',
                        encoding="utf-8",
                    )
                elif mode == "copy-only":
                    paths.local_paths.copy_intent.write_text("{}\n", encoding="utf-8")
                    paths.local_paths.copy_receipt.write_text("{}\n", encoding="utf-8")
                else:
                    (case.run_root / "unknown-prefix.json").write_text(
                        "{}\n", encoding="utf-8"
                    )
                before = case._tree(case.root)
                callbacks = {"count": 0}

                def forbidden(*_args, **_kwargs):
                    callbacks["count"] += 1
                    raise AssertionError("legacy prefix reached callback")

                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.run_refresh(
                        **case._kwargs(),
                        endpoint_info_fetcher=forbidden,
                        balance_checker=forbidden,
                        detail_fetcher=forbidden,
                        key_loader=forbidden,
                    )
                self.assertEqual(callbacks["count"], 0)
                self.assertEqual(before, case._tree(case.root))
                self.assertFalse(case.db.exists())
                self.assertFalse(case.raw_root.exists())
                self.assertFalse(case.media_root.exists())

    def test_unknown_raw_prefix_without_run_root_rejects_before_any_write(self) -> None:
        self.raw_root.mkdir()
        (self.raw_root / "unknown-old-prefix.json").write_text("{}\n", encoding="utf-8")
        before = self._tree(self.root)
        callbacks = {"count": 0}

        def forbidden(*_args, **_kwargs):
            callbacks["count"] += 1
            raise AssertionError("raw-only legacy prefix reached callback")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(callbacks["count"], 0)
        self.assertEqual(before, self._tree(self.root))
        self.assertFalse(self.db.exists())
        self.assertFalse(self.run_root.exists())
        self.assertFalse(self.media_root.exists())

    def test_fake_v2_anchor_rejects_before_any_write(self) -> None:
        self.run_root.mkdir()
        (self.run_root / "metadata-contract.json").write_text(
            '{"completion_kind":"paid-source-refresh-v2","version":2}\n',
            encoding="utf-8",
        )
        before = self._tree(self.root)
        callbacks = {"count": 0}

        def forbidden(*_args, **_kwargs):
            callbacks["count"] += 1
            raise AssertionError("fake v2 anchor reached callback")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(callbacks["count"], 0)
        self.assertEqual(self._tree(self.root), before)
        self.assertFalse(self.db.exists())
        self.assertFalse(self.raw_root.exists())
        self.assertFalse(self.media_root.exists())

    def test_allowed_generation_name_link_rejects_before_any_write(self) -> None:
        for mode in ("symlink", "hardlink"):
            with self.subTest(mode=mode):
                case = self._new_case()
                case.run_root.mkdir()
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                metadata_contract = paid._metadata_contract_value(
                    paths,
                    expected_source_db_sha256=case.source_db_sha,
                    expected_source_completion_sha256=case.source_completion_sha,
                    content_id=1,
                )
                (case.run_root / "metadata-contract.json").write_bytes(
                    paid._canonical_bytes(metadata_contract)
                )
                outside = case.root / f"{mode}-allowed-name-target.json"
                outside.write_text("{}\n", encoding="utf-8")
                linked = case.run_root / "refresh-state.json"
                if mode == "symlink":
                    linked.symlink_to(outside)
                else:
                    os.link(outside, linked)
                before = case._tree(case.root)
                callbacks = {"count": 0}

                def forbidden(*_args, **_kwargs):
                    callbacks["count"] += 1
                    raise AssertionError("linked allowed name reached callback")

                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.run_refresh(
                        **case._kwargs(),
                        endpoint_info_fetcher=forbidden,
                        balance_checker=forbidden,
                        detail_fetcher=forbidden,
                        key_loader=forbidden,
                    )
                self.assertEqual(callbacks["count"], 0)
                self.assertEqual(case._tree(case.root), before)
                self.assertFalse(case.db.exists())
                self.assertFalse(case.raw_root.exists())
                self.assertFalse(case.media_root.exists())

    def test_success_exact_delta_typed_handoff_and_idempotent_zero_network(self) -> None:
        result = self._run()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["provider_calls"], 1)
        completion_sha = result["completion_sha256"]
        database_sha = local_canary._sha256_file(self.db)
        evidence = paid.validate_completion_for_local_analysis(
            source_db_path=self.db,
            source_completion_path=self.run_root / "completion.json",
            expected_source_db_sha256=database_sha,
            expected_source_completion_sha256=completion_sha,
            content_ids=[1],
        )
        self.assertEqual(evidence["completion_kind"], paid.COMPLETION_KIND)
        with closing(local_canary._immutable_connection(self.db)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT source_group FROM content_items WHERE id=1"
                ).fetchone()[0],
                "history-backfill",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
                ).fetchone()[0],
                1,
            )
        handoff_root = self.root / "analysis-handoff"
        handoff_root.mkdir()
        (handoff_root / "db").mkdir()
        with patch.object(local_canary, "_local_tools", return_value=self.fixture.tools):
            planned = local_canary.plan_canary(
                source_db_path=self.db,
                source_completion_path=self.run_root / "completion.json",
                expected_source_db_sha256=database_sha,
                expected_source_completion_sha256=completion_sha,
                db_path=handoff_root / "db" / "analysis.sqlite3",
                media_root=handoff_root / "media",
                run_root=handoff_root / "run",
                content_ids=[1],
            )
        self.assertEqual(planned["source_completion"]["completion_kind"], paid.COMPLETION_KIND)
        tree = self._tree(self.root)
        calls = dict(self.calls)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("idempotent rerun performed network or key access")

        rerun = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertTrue(rerun["idempotent"])
        self.assertEqual(rerun["completion_sha256"], completion_sha)
        self.assertEqual(self.calls, calls)
        self.assertEqual(tree, self._tree(self.root))

    def test_exact_douyin_zjcdn_direct_video_url_is_materialized(self) -> None:
        self.video_url = (
            "https://v5-dy-ov-experiment.zjcdn.com/"
            f"{'a' * 32}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
            f"{'C' * 38}/?token=frozen"
        )
        result = self._run()
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["provider_calls"], 1)
        completion = json.loads((self.run_root / "completion.json").read_text())
        self.assertEqual(len(completion["url_provenance"]["allowed_urls"]), 1)
        self.assertEqual(self.calls["detail"], 1)

    def test_douyin_zjcdn_renditions_allow_only_exact_direct_hosts(self) -> None:
        allowed: list[str] = []
        denied: list[str] = []
        for index in range(3):
            object_path = (
                f"/{index:032x}/{'b' * 8}/video/tos/cn/tos-cn-ve-15/"
                f"{'C' * 37}{index}/"
            )
            for host in sorted(local_canary.DOUYIN_DIRECT_VIDEO_CDN_HOSTS):
                allowed.append(
                    f"https://{host}{object_path}?token=frozen-{index}"
                )
            for host in ("api-play.amemv.com", "api.amemv.com"):
                denied.append(
                    f"https://{host}/aweme/v1/play/?video_id=frozen-{index}"
                )
        payload = self._payload()
        payload["data"]["aweme_detail"]["video"] = {
            "play_addr": {"url_list": allowed[:2] + denied[:2]},
            "download_addr": {"url_list": []},
            "bit_rate": [
                {"play_addr": {"url_list": allowed[2:4] + denied[2:4]}},
                {"play_addr": {"url_list": allowed[4:6] + denied[4:6]}},
            ],
        }
        parsed = paid.providers_module._parse_douyin_stage_payload(
            "detail", "canary-1", payload, status=200
        )
        provenance = paid._validate_live_payload(
            payload=payload,
            data=parsed.data,
            target={
                "platform": "douyin",
                "platform_content_id": "canary-1",
                "title": "完整标题",
                "body": "完整正文",
            },
        )
        self.assertEqual(len(provenance["video_urls"]), 12)
        self.assertEqual(provenance["allowed_urls"], allowed)
        self.assertEqual(provenance["intersection_count"], 0)

    def test_success_extra_empty_directory_blocks_idempotent_and_handoff(self) -> None:
        result = self._run()
        database_sha = local_canary._sha256_file(self.db)
        completion_path = self.run_root / "completion.json"
        completion_sha = result["completion_sha256"]
        unknown = self.raw_root / "unknown-empty-directory"
        unknown.mkdir()
        before_files = self._tree(self.root)
        calls = dict(self.calls)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("success-prefix drift attempted network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.validate_completion_for_local_analysis(
                source_db_path=self.db,
                source_completion_path=completion_path,
                expected_source_db_sha256=database_sha,
                expected_source_completion_sha256=completion_sha,
                content_ids=[1],
            )
        self.assertTrue(unknown.is_dir())
        self.assertEqual(self.calls, calls)
        self.assertEqual(before_files, self._tree(self.root))

    def test_stale_success_handoff_requires_a_new_refresh_root(self) -> None:
        completed_at = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        ).isoformat(timespec="seconds")
        with patch.object(paid, "_now_text", return_value=completed_at):
            result = self._run()
        evidence = paid.validate_completion_for_local_analysis(
            source_db_path=self.db,
            source_completion_path=self.run_root / "completion.json",
            expected_source_db_sha256=local_canary._sha256_file(self.db),
            expected_source_completion_sha256=result["completion_sha256"],
            content_ids=[1],
        )
        self.assertEqual(evidence["completed_at"], completed_at)
        analysis_root = self.root / "stale-analysis"
        analysis_root.mkdir()
        (analysis_root / "db").mkdir()
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid._require_handoff_fresh(
                evidence["completed_at"],
                maximum_age_seconds=evidence["max_handoff_age_seconds"],
            )
        with self.assertRaises(local_canary.LocalAnalysisCanaryError):
            local_canary.plan_canary(
                source_db_path=self.db,
                source_completion_path=self.run_root / "completion.json",
                expected_source_db_sha256=local_canary._sha256_file(self.db),
                expected_source_completion_sha256=result["completion_sha256"],
                db_path=analysis_root / "db" / "analysis.sqlite3",
                media_root=analysis_root / "media",
                run_root=analysis_root / "run",
                content_ids=[1],
            )

    def test_completed_local_handoff_remains_static_and_idempotent_after_age(self) -> None:
        paid_result = self._run()
        analysis_root = self.root / "completed-analysis"
        analysis_root.mkdir()
        (analysis_root / "db").mkdir()
        fixture = self.fixture
        fixture.source_db = self.db
        fixture.source_completion = self.run_root / "completion.json"
        fixture.source_db_sha = local_canary._sha256_file(self.db)
        fixture.source_completion_sha = paid_result["completion_sha256"]
        fixture.db = analysis_root / "db" / "analysis.sqlite3"
        fixture.media_root = analysis_root / "media"
        fixture.run_root = analysis_root / "run"
        fixture.urls = [self.video_url]
        first = fixture._run()
        self.assertEqual(first["status"], "succeeded")
        tree = self._tree(analysis_root)
        calls = dict(fixture.calls)

        with patch.object(
            paid,
            "_require_handoff_fresh",
            side_effect=paid.PaidSourceRefreshError("simulated expired handoff"),
        ):
            static = paid.validate_completion_for_local_analysis(
                source_db_path=self.db,
                source_completion_path=self.run_root / "completion.json",
                expected_source_db_sha256=local_canary._sha256_file(self.db),
                expected_source_completion_sha256=paid_result["completion_sha256"],
                content_ids=[1],
            )
            rerun = fixture._run()
        self.assertEqual(static["completion_kind"], paid.COMPLETION_KIND)
        self.assertTrue(rerun["idempotent"])
        self.assertEqual(fixture.calls, calls)
        self.assertEqual(self._tree(analysis_root), tree)

    def test_music_nested_overlap_blocks_after_one_call_and_never_retries(self) -> None:
        def overlapping(_content_id: str, _key: str):
            self.calls["detail"] += 1
            payload = self._payload(music_urls=[])
            payload["data"]["aweme_detail"]["music"] = {
                "backup": {"nested_url": self.video_url}
            }
            body = json.dumps(payload).encode()
            return payload, {
                "url_sha256": paid._request_url_sha256(
                    paid.DETAIL_PATH, {"aweme_id": "canary-1"}
                ),
                "response_sha256": hashlib.sha256(body).hexdigest(),
                "response_json_sha256": hashlib.sha256(
                    paid.capture_module.canonical_json_bytes(payload)
                ).hexdigest(),
                "response_bytes": len(body),
                "http_status": 200,
                "mime_type": "application/json",
                "endpoint": paid.DETAIL_PATH,
                "aweme_id": "canary-1",
            }

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(detail_fetcher=overlapping)
        self.assertEqual(self.calls["detail"], 1)
        self.assertFalse((self.run_root / "completion.json").exists())
        with closing(local_canary._immutable_connection(self.db)) as connection:
            usage = connection.execute(
                "SELECT request_attempts,billed_requests,amount,details_json "
                "FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
            ).fetchone()
            self.assertEqual(tuple(usage[:3]), (1, 1, paid.UNIT_PRICE))
            self.assertEqual(
                json.loads(usage[3]),
                {
                    "http_status": 200,
                    "outcome": "rejected_source",
                    "slot_id": 1,
                    "state": "completed",
                },
            )
            slot = connection.execute(
                "SELECT status,last_error_code FROM fetch_slots "
                "WHERE content_id=1 AND stage=?",
                (paid.STAGE,),
            ).fetchone()
            self.assertEqual(tuple(slot), ("terminal_failed", "rejected_source"))
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=lambda: (_ for _ in ()).throw(
                    AssertionError("price called")
                ),
                balance_checker=lambda *_: (_ for _ in ()).throw(
                    AssertionError("balance called")
                ),
                detail_fetcher=lambda *_: (_ for _ in ()).throw(
                    AssertionError("detail called twice")
                ),
                key_loader=lambda: (_ for _ in ()).throw(
                    AssertionError("key called")
                ),
            )
        self.assertEqual(self.calls["detail"], 1)

    def test_committed_raw_and_registered_manifest_resume_without_network_or_db_rewrite(self) -> None:
        def crash_after_fetch() -> None:
            raise RuntimeError("kill-after-raw-commit")

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(after_fetch_hook=crash_after_fetch)
        self.assertEqual(self.calls["detail"], 1)
        result = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=lambda: (_ for _ in ()).throw(
                AssertionError("price called on raw resume")
            ),
            balance_checker=lambda *_: (_ for _ in ()).throw(
                AssertionError("balance called on raw resume")
            ),
            detail_fetcher=lambda *_: (_ for _ in ()).throw(
                AssertionError("detail called on raw resume")
            ),
            key_loader=lambda: (_ for _ in ()).throw(
                AssertionError("key called on raw resume")
            ),
        )
        self.assertEqual(result["provider_calls"], 0)
        with closing(local_canary._immutable_connection(self.db)) as connection:
            sequences = dict(connection.execute("SELECT name,seq FROM sqlite_sequence"))
        db_sha = local_canary._sha256_file(self.db)
        original_records = paid._completion_records

        def crash_before_records(*_args, **_kwargs):
            raise RuntimeError("kill-after-manifest")

        # A second independent fixture exercises registered artifact -> receipt recovery.
        second = PaidSourceRefreshCanaryTest(methodName="runTest")
        second.setUp()
        self.addCleanup(second.doCleanups)
        with patch.object(paid, "_completion_records", side_effect=crash_before_records):
            with self.assertRaises(paid.PaidSourceRefreshError):
                second._run()
        second_sha = local_canary._sha256_file(second.db)
        with closing(local_canary._immutable_connection(second.db)) as connection:
            second_sequences = dict(connection.execute("SELECT name,seq FROM sqlite_sequence"))
        with patch.object(paid, "_completion_records", original_records):
            resumed = paid.run_refresh(
                **second._kwargs(),
                endpoint_info_fetcher=lambda: (_ for _ in ()).throw(AssertionError()),
                balance_checker=lambda *_: (_ for _ in ()).throw(AssertionError()),
                detail_fetcher=lambda *_: (_ for _ in ()).throw(AssertionError()),
                key_loader=lambda: (_ for _ in ()).throw(AssertionError()),
            )
        self.assertEqual(resumed["provider_calls"], 0)
        self.assertEqual(second_sha, local_canary._sha256_file(second.db))
        with closing(local_canary._immutable_connection(second.db)) as connection:
            self.assertEqual(
                second_sequences,
                dict(connection.execute("SELECT name,seq FROM sqlite_sequence")),
            )
        self.assertTrue(db_sha)
        self.assertTrue(sequences)

    def test_stale_price_blocks_before_key_balance_or_paid_opening(self) -> None:
        stale = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        ).isoformat(timespec="seconds")
        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(endpoint_info_fetcher=lambda: self._price(checked_at=stale))
        self.assertEqual(self.calls["balance"], 0)
        self.assertEqual(self.calls["detail"], 0)
        ledger = paid._read_json(self.run_root / "provider-ledger.json", label="ledger")
        self.assertFalse(ledger["attempt_consumed"])
        with closing(local_canary._immutable_connection(self.db)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_usage WHERE task_id LIKE 'paid-source-refresh-%'"
                ).fetchone()[0],
                0,
            )

    def test_exact_price_and_balance_shapes_reject_bool_or_stale_values(self) -> None:
        valid_endpoint = {
            "code": 200,
            "router": paid.ENDPOINT_INFO_PATH,
            "params": {"endpoint": paid.USER_INFO_PATH},
            "data": {
                "endpoint_uri": paid.USER_INFO_PATH,
                "endpoint_cost": 0.0,
                "endpoint_type": "self-operated",
                "rate_limit": "1/second",
            },
        }
        fields = paid._extract_endpoint_fields(
            valid_endpoint,
            expected_endpoint=paid.USER_INFO_PATH,
            expected_cost=0.0,
        )
        self.assertEqual(fields["endpoint_cost"], 0.0)
        for mutation in (
            {"code": False},
            {"data": {**valid_endpoint["data"], "endpoint_cost": False}},
            {"data": {key: value for key, value in valid_endpoint["data"].items() if key != "endpoint_cost"}},
        ):
            candidate = json.loads(json.dumps(valid_endpoint))
            candidate.update(mutation)
            with self.assertRaises(paid.PaidSourceRefreshError):
                paid._extract_endpoint_fields(
                    candidate,
                    expected_endpoint=paid.USER_INFO_PATH,
                    expected_cost=0.0,
                )
        user_payload = {
            "code": 200,
            "user_data": {"balance": False, "free_credit": 1.0},
        }
        with patch.object(
            paid,
            "_exact_json_request",
            return_value=(
                user_payload,
                {"response_sha256": "a" * 64, "response_bytes": 20},
            ),
        ), self.assertRaises(paid.PaidSourceRefreshError):
            paid._default_balance_check("secret", "b" * 64)

        def stale_balance(_key: str, price_sha256: str):
            value = self._balance(_key, price_sha256)
            value["checked_at"] = (
                datetime.now(timezone.utc) - timedelta(hours=1)
            ).isoformat(timespec="seconds")
            return value

        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(balance_checker=stale_balance)
        self.assertEqual(self.calls["detail"], 0)
        ledger = paid._read_json(self.run_root / "provider-ledger.json", label="ledger")
        self.assertFalse(ledger["attempt_consumed"])

    def test_price_transcript_types_and_costs_are_exact_before_key_or_opening(self) -> None:
        transcript_mutations = (
            ("response_bytes", True),
            ("response_bytes", 1.5),
            ("http_status", 200.5),
            ("http_status", "200"),
        )
        for record_index in range(3):
            for field, changed in transcript_mutations:
                with self.subTest(
                    record_index=record_index, field=field, changed=changed
                ):
                    case = self._new_case()
                    price = case._price()
                    price["records"][record_index]["response"][field] = changed
                    price["records_sha256"] = paid._json_sha256(price["records"])
                    key_calls = {"count": 0}

                    def forbidden_key():
                        key_calls["count"] += 1
                        raise AssertionError("invalid price reached key load")

                    with self.assertRaises(paid.PaidSourceRefreshError):
                        case._run(
                            endpoint_info_fetcher=lambda value=price: value,
                            key_loader=forbidden_key,
                        )
                    self.assertEqual(key_calls["count"], 0)
                    self.assertEqual(case.calls["balance"], 0)
                    self.assertEqual(case.calls["detail"], 0)
                    self.assertFalse(
                        (case.run_root / "provider-ledger.json").exists()
                    )

        for record_index, changed in ((0, 5e-10), (1, 5e-10), (2, 0.0010000005)):
            with self.subTest(record_index=record_index, changed_cost=changed):
                case = self._new_case()
                price = case._price()
                price["records"][record_index]["fields"]["endpoint_cost"] = changed
                price["records_sha256"] = paid._json_sha256(price["records"])
                with self.assertRaises(paid.PaidSourceRefreshError):
                    case._run(endpoint_info_fetcher=lambda value=price: value)
                self.assertEqual(case.calls["balance"], 0)
                self.assertEqual(case.calls["detail"], 0)
                self.assertFalse((case.run_root / "provider-ledger.json").exists())

        for record_index, old_rate in ((0, "1/s"), (1, "1/s"), (2, "10/s")):
            with self.subTest(record_index=record_index, old_rate=old_rate):
                case = self._new_case()
                price = case._price()
                price["records"][record_index]["fields"]["rate_limit"] = old_rate
                price["records_sha256"] = paid._json_sha256(price["records"])
                with self.assertRaises(paid.PaidSourceRefreshError):
                    case._run(endpoint_info_fetcher=lambda value=price: value)
                self.assertEqual(case.calls["balance"], 0)
                self.assertEqual(case.calls["detail"], 0)
                self.assertFalse((case.run_root / "provider-ledger.json").exists())

    def test_user_info_code_and_balance_threshold_are_exact(self) -> None:
        transcript = {
            "response_sha256": "a" * 64,
            "response_bytes": 100,
        }
        for code in ("200", 200.5, True):
            with self.subTest(code=code):
                payload = {
                    "code": code,
                    "user_data": {"balance": 1.0, "free_credit": 0.0},
                }
                with patch.object(
                    paid,
                    "_exact_json_request",
                    return_value=(payload, transcript),
                ), self.assertRaises(paid.PaidSourceRefreshError):
                    paid._default_balance_check("fixture-key", "b" * 64)

        for balance, free_credit in (
            (0.0009999995, 0.0),
            (0.0005, 0.0004999995),
            (True, 1.0),
            (1.0, False),
        ):
            with self.subTest(balance=balance, free_credit=free_credit):
                payload = {
                    "code": 200,
                    "user_data": {
                        "balance": balance,
                        "free_credit": free_credit,
                    },
                }
                with patch.object(
                    paid,
                    "_exact_json_request",
                    return_value=(payload, transcript),
                ), self.assertRaises(paid.PaidSourceRefreshError):
                    paid._default_balance_check("fixture-key", "b" * 64)

        payload = {
            "code": 200,
            "user_data": {"balance": 0.0005, "free_credit": 0.0005},
        }
        with patch.object(
            paid,
            "_exact_json_request",
            return_value=(payload, transcript),
        ):
            accepted = paid._default_balance_check("fixture-key", "b" * 64)
        self.assertTrue(accepted["balance_sufficient"])

    def test_exact_json_request_disables_proxy_redirect_and_streams_with_caps(self) -> None:
        url = (
            f"{paid.providers_module.TIKHUB_BASE}{paid.ENDPOINT_INFO_PATH}"
            "?endpoint=%2Fapi%2Fv1%2Ftikhub%2Fuser%2Fget_user_info"
        )

        class Response:
            def __init__(self, body: bytes, *, headers=None, response_url=url):
                self.body = body
                self.offset = 0
                self.headers = headers or {"Content-Type": "application/json"}
                self.status = 200
                self.response_url = response_url

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def geturl(self):
                return self.response_url

            def read(self, size: int):
                block = self.body[self.offset : self.offset + size]
                self.offset += len(block)
                return block

        class Opener:
            def __init__(self, response):
                self.response = response

            def open(self, request, *, timeout):
                self.request = request
                self.timeout = timeout
                return self.response

        def run(response, *, maximum_bytes=16):
            captured = {}
            tls_context = paid.ssl.create_default_context()

            def build(*handlers):
                captured["handlers"] = handlers
                captured["opener"] = Opener(response)
                return captured["opener"]

            with patch.object(
                paid.ssl, "create_default_context", return_value=tls_context
            ) as context_factory, patch.object(
                paid.urllib.request, "build_opener", side_effect=build
            ):
                result = paid._exact_json_request(
                    url,
                    expected_path=paid.ENDPOINT_INFO_PATH,
                    expected_query={"endpoint": paid.USER_INFO_PATH},
                    authorization=None,
                    maximum_bytes=maximum_bytes,
                )
            self.assertEqual(captured["handlers"][0].proxies, {})
            self.assertIsInstance(captured["handlers"][1], paid._NoRedirect)
            self.assertIsInstance(
                captured["handlers"][2], paid.urllib.request.HTTPSHandler
            )
            self.assertIs(captured["handlers"][2]._context, tls_context)
            context_factory.assert_called_once_with()
            self.assertTrue(tls_context.check_hostname)
            self.assertEqual(tls_context.verify_mode, paid.ssl.CERT_REQUIRED)
            self.assertEqual(captured["opener"].request.get_method(), "GET")
            self.assertEqual(
                captured["opener"].request.get_header("User-agent"),
                paid.TRANSPORT_PROFILE["user_agent"],
            )
            self.assertEqual(
                captured["opener"].timeout,
                paid.TRANSPORT_PROFILE["timeout_seconds"],
            )
            return result

        payload, transcript = run(Response(b"{}"))
        self.assertEqual(payload, {})
        self.assertEqual(transcript["response_bytes"], 2)
        with self.assertRaises(paid.PaidSourceRefreshError):
            run(Response(b'{"long":true}'), maximum_bytes=4)
        with self.assertRaises(paid.PaidSourceRefreshError):
            run(
                Response(
                    b"{}",
                    headers={
                        "Content-Type": "application/json",
                        "Content-Length": "10",
                    },
                )
            )
        with self.assertRaises(paid.PaidSourceRefreshError):
            run(Response(b"{}", response_url=url + "&redirected=1"))

    def test_db_commit_wal_window_is_recovered_without_second_detail_call(self) -> None:
        with patch.object(
            paid.local_controller,
            "_finalize_database",
            side_effect=RuntimeError("simulated hard kill before WAL checkpoint"),
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run()
        self.assertEqual(self.calls["detail"], 1)
        self.assertTrue(
            any(
                path.exists()
                for path in (Path(f"{self.db}-wal"), Path(f"{self.db}-shm"))
            )
        )

        def forbidden(*_args, **_kwargs):
            raise AssertionError("WAL recovery attempted network/key access")

        resumed = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertEqual(resumed["provider_calls"], 0)
        self.assertEqual(self.calls["detail"], 1)
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())

    def test_real_sigkill_after_detail_entry_never_calls_detail_twice(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="opening")
        self.assertEqual(marker.read_text(encoding="utf-8"), "detail-entered\n")
        self.assertEqual(self._subprocess_detail_count(mode="opening"), 1)
        ledger = paid._read_json(
            self.run_root / "provider-ledger.json", label="opening ledger"
        )
        self.assertEqual(len(ledger["events"]), 1)
        self.assertTrue(ledger["attempt_consumed"])
        with closing(sqlite3.connect(self.db)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses "
                    "WHERE provider=? AND operation=?",
                    (paid.PROVIDER, paid.OPERATION),
                ).fetchone()[0],
                0,
            )

        restart_calls = 0

        def forbidden(*_args, **_kwargs):
            nonlocal restart_calls
            restart_calls += 1
            raise AssertionError("post-SIGKILL resume attempted a second detail call")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(marker.read_text(encoding="utf-8"), "detail-entered\n")
        self.assertEqual(restart_calls, 0)
        self.assertEqual(self._subprocess_detail_count(mode="opening"), 1)
        contract = dict(
            paid._read_json(self.run_root / "refresh-contract.json", label="contract")
        )
        self.assertEqual(
            self._sequence_snapshot(self.db),
            self._expected_sequences(
                contract,
                incremented={"provider_usage", "fetch_slots", "fetch_attempts"},
            ),
        )

    def test_real_sigkill_raw_wal_commit_resumes_zero_network_exactly_once(self) -> None:
        marker = self._kill_subprocess_at_marker(mode="raw_wal")
        self.assertEqual(marker.read_text(encoding="utf-8"), "raw-committed\n")
        self.assertEqual(self._subprocess_detail_count(mode="raw_wal"), 1)
        self.assertTrue(
            any(
                path.exists()
                for path in (Path(f"{self.db}-wal"), Path(f"{self.db}-shm"))
            )
        )

        def forbidden(*_args, **_kwargs):
            raise AssertionError("raw-WAL SIGKILL recovery attempted network")

        result = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertEqual(result["provider_calls"], 0)
        self.assertFalse(Path(f"{self.db}-wal").exists())
        self.assertFalse(Path(f"{self.db}-shm").exists())
        contract = dict(
            paid._read_json(self.run_root / "refresh-contract.json", label="contract")
        )
        expected_sequences = self._expected_sequences(
            contract, incremented=set(paid.AUTOINCREMENT_DELTA_TABLES)
        )
        self.assertEqual(self._sequence_snapshot(self.db), expected_sequences)
        with closing(sqlite3.connect(self.db)) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM provider_raw_responses "
                    "WHERE provider=? AND operation=?",
                    (paid.PROVIDER, paid.OPERATION),
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM fetch_attempts fa JOIN fetch_slots fs "
                    "ON fs.id=fa.slot_id WHERE fs.stage=?",
                    (paid.STAGE,),
                ).fetchone()[0],
                1,
            )
        tree = self._tree(self.root)
        rerun = paid.run_refresh(
            **self._kwargs(),
            endpoint_info_fetcher=forbidden,
            balance_checker=forbidden,
            detail_fetcher=forbidden,
            key_loader=forbidden,
        )
        self.assertTrue(rerun["idempotent"])
        self.assertEqual(tree, self._tree(self.root))
        self.assertEqual(self._sequence_snapshot(self.db), expected_sequences)
        self.assertEqual(self._subprocess_detail_count(mode="raw_wal"), 1)

    def test_real_sigkill_terminal_ledger_and_raw_orphan_never_recall_detail(self) -> None:
        for mode in ("terminal_ledger_temp", "terminal_ledger_final", "raw_final"):
            with self.subTest(mode=mode):
                case = self._new_case()
                marker = case._kill_subprocess_at_marker(mode=mode)
                expected_marker = "raw-final" if mode == "raw_final" else mode
                self.assertEqual(
                    marker.read_text(encoding="utf-8"), f"{expected_marker}\n"
                )
                self.assertEqual(case._subprocess_detail_count(mode=mode), 1)
                raw_files = (
                    [path for path in case.raw_root.rglob("*") if path.is_file()]
                    if case.raw_root.exists()
                    else []
                )
                raw_bytes = {
                    str(path): path.read_bytes()
                    for path in raw_files
                }

                restart_calls = 0

                def forbidden(*_args, **_kwargs):
                    nonlocal restart_calls
                    restart_calls += 1
                    raise AssertionError("post-SIGKILL blocked prefix attempted network")

                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.run_refresh(
                        **case._kwargs(),
                        endpoint_info_fetcher=forbidden,
                        balance_checker=forbidden,
                        detail_fetcher=forbidden,
                        key_loader=forbidden,
                    )
                self.assertEqual(restart_calls, 0)
                self.assertEqual(case._subprocess_detail_count(mode=mode), 1)
                ledger = paid._read_json(
                    case.run_root / "provider-ledger.json",
                    label=f"{mode} ledger",
                )
                self.assertEqual(len(ledger["events"]), 2)
                self.assertEqual(ledger["events"][1]["outcome"], "response_received")
                with closing(local_canary._immutable_connection(case.db)) as connection:
                    self.assertEqual(
                        connection.execute(
                            "SELECT COUNT(*) FROM provider_raw_responses "
                            "WHERE provider=? AND operation=?",
                            (paid.PROVIDER, paid.OPERATION),
                        ).fetchone()[0],
                        0,
                    )
                contract = dict(
                    paid._read_json(
                        case.run_root / "refresh-contract.json", label=f"{mode} contract"
                    )
                )
                self.assertEqual(
                    case._sequence_snapshot(case.db),
                    case._expected_sequences(
                        contract,
                        incremented={
                            "provider_usage",
                            "fetch_slots",
                            "fetch_attempts",
                        },
                    ),
                )
                self.assertEqual(
                    {str(path): path.read_bytes() for path in raw_files}, raw_bytes
                )
                if mode == "raw_final":
                    self.assertEqual(len(raw_files), 1)
                    self.assertTrue(raw_files[0].is_file())

    def test_real_sigkill_post_raw_windows_resume_zero_network_and_exact_sequences(self) -> None:
        modes = (
            "manifest_final",
            "artifact_committed",
            "state_temp",
            "receipt_temp",
            "completion_temp",
        )
        for mode in modes:
            with self.subTest(mode=mode):
                case = self._new_case()
                marker = case._kill_subprocess_at_marker(mode=mode)
                expected_marker = {
                    "manifest_final": "manifest-final",
                    "artifact_committed": "artifact-committed",
                }.get(mode, mode)
                self.assertEqual(
                    marker.read_text(encoding="utf-8"), f"{expected_marker}\n"
                )
                self.assertEqual(case._subprocess_detail_count(mode=mode), 1)
                before_database_sha256 = local_canary._sha256_file(case.db)
                before_sequences = case._sequence_snapshot(case.db)

                def forbidden(*_args, **_kwargs):
                    raise AssertionError("post-SIGKILL recovery attempted network")

                result = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
                self.assertEqual(result["provider_calls"], 0)
                self.assertEqual(case._subprocess_detail_count(mode=mode), 1)
                after_sequences = case._sequence_snapshot(case.db)
                if mode == "manifest_final":
                    expected_sequences = dict(before_sequences)
                    expected_sequences["evidence_artifacts"] = (
                        before_sequences["evidence_artifacts"] + 1
                    )
                    self.assertEqual(after_sequences, expected_sequences)
                else:
                    self.assertEqual(after_sequences, before_sequences)
                    self.assertEqual(
                        local_canary._sha256_file(case.db),
                        before_database_sha256,
                    )
                tree = case._tree(case.root)
                rerun = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
                self.assertTrue(rerun["idempotent"])
                self.assertEqual(case._tree(case.root), tree)
                self.assertEqual(case._subprocess_detail_count(mode=mode), 1)

    def test_opening_or_orphan_raw_never_permits_a_second_paid_call(self) -> None:
        orphan: dict[str, object] = {}

        def leave_orphan(
            paths,
            _contract,
            *,
            claim,
            usage_id: int,
            result,
        ):
            del usage_id
            body = paid.capture_module.canonical_json_bytes(result.raw_response)
            digest = hashlib.sha256(body).hexdigest()
            path = (
                paths.raw_root
                / claim.provider.lower()
                / str(claim.content_id)
                / paid.OPERATION
                / f"attempt-{claim.attempt_number:03d}-{digest[:12]}.json"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            orphan["path"] = path
            orphan["body"] = body
            raise RuntimeError("kill after raw final before DB raw row")

        with patch.object(
            paid, "_commit_successful_capture", side_effect=leave_orphan
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run()
        orphan_path = orphan["path"]
        self.assertIsInstance(orphan_path, Path)
        self.assertEqual(orphan_path.read_bytes(), orphan["body"])
        self.assertEqual(self.calls["detail"], 1)

        def forbidden(*_args, **_kwargs):
            raise AssertionError("orphan raw recovery attempted a second call")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(orphan_path.read_bytes(), orphan["body"])
        self.assertEqual(self.calls["detail"], 1)

    def test_manifest_final_without_artifact_is_exactly_resumed_zero_network(self) -> None:
        original = paid.media_module.store_media_source_manifest
        orphan: dict[str, Path] = {}

        def leave_manifest(content_id: int, **kwargs):
            urls, source_sha = paid.media_module._media_source_identity(
                kwargs["media_kind"], kwargs["urls"]
            )
            path = (
                Path(kwargs["media_root"])
                / "C4N4RY"
                / "sources"
                / f"source-{kwargs['raw_response_id']}-{source_sha[:12]}.json"
            )
            paid.media_module._atomic_json(
                path,
                {
                    "schema_version": paid.media_module.MEDIA_SOURCE_VERSION,
                    "media_kind": "video",
                    "urls": urls,
                    "source_sha256": source_sha,
                    "raw_response_id": kwargs["raw_response_id"],
                    "captured_at": paid.storage_module.now_utc(),
                },
            )
            orphan["path"] = path
            raise RuntimeError(f"kill before artifact row for {content_id}")

        with patch.object(
            paid.media_module,
            "store_media_source_manifest",
            side_effect=leave_manifest,
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run()
        before = orphan["path"].read_bytes()

        def forbidden(*_args, **_kwargs):
            raise AssertionError("manifest recovery attempted network")

        with patch.object(
            paid.media_module,
            "now_utc",
            return_value="2026-08-11T00:00:00Z",
        ), patch.object(
            paid.media_module, "store_media_source_manifest", wraps=original
        ):
            resumed = paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(resumed["provider_calls"], 0)
        self.assertEqual(orphan["path"].read_bytes(), before)
        self.assertEqual(self.calls["detail"], 1)

    def test_orphan_manifest_unknown_shape_is_preserved_and_blocked(self) -> None:
        for mutation in ("extra", "missing-captured-at"):
            with self.subTest(mutation=mutation):
                case = self._new_case()
                orphan: dict[str, Path] = {}

                def leave_manifest(content_id: int, **kwargs):
                    urls, source_sha = paid.media_module._media_source_identity(
                        kwargs["media_kind"], kwargs["urls"]
                    )
                    path = (
                        Path(kwargs["media_root"])
                        / "C4N4RY"
                        / "sources"
                        / f"source-{kwargs['raw_response_id']}-{source_sha[:12]}.json"
                    )
                    body = {
                        "schema_version": paid.media_module.MEDIA_SOURCE_VERSION,
                        "media_kind": "video",
                        "urls": urls,
                        "source_sha256": source_sha,
                        "raw_response_id": kwargs["raw_response_id"],
                        "captured_at": paid.storage_module.now_utc(),
                    }
                    if mutation == "extra":
                        body["unknown"] = True
                    else:
                        del body["captured_at"]
                    paid.media_module._atomic_json(path, body)
                    orphan["path"] = path
                    raise RuntimeError(f"kill before artifact row for {content_id}")

                with patch.object(
                    paid.media_module,
                    "store_media_source_manifest",
                    side_effect=leave_manifest,
                ), self.assertRaises(paid.PaidSourceRefreshError):
                    case._run()
                before = orphan["path"].read_bytes()
                db_sha = local_canary._sha256_file(case.db)

                def forbidden(*_args, **_kwargs):
                    raise AssertionError("unknown orphan attempted network")

                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.run_refresh(
                        **case._kwargs(),
                        endpoint_info_fetcher=forbidden,
                        balance_checker=forbidden,
                        detail_fetcher=forbidden,
                        key_loader=forbidden,
                    )
                self.assertEqual(orphan["path"].read_bytes(), before)
                self.assertEqual(local_canary._sha256_file(case.db), db_sha)
                self.assertEqual(case.calls["detail"], 1)

    def test_manifest_atomic_prefix_windows_resume_or_block_without_network(self) -> None:
        for mode in (
            "directory",
            "partial-directory",
            "unknown-directory",
            "full-temp",
            "partial-temp",
        ):
            with self.subTest(mode=mode):
                case = self._new_case()
                original = paid.media_module._stage_private_media_source_json
                evidence: dict[str, Path] = {}

                def interrupt(path: Path, body, **_kwargs):
                    if mode in {"partial-directory", "unknown-directory"}:
                        path.parent.parent.mkdir(parents=True, exist_ok=True)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                    if mode == "unknown-directory":
                        (path.parents[2] / "unknown-sibling").mkdir()
                    temporary = path.with_name(f".{path.name}.tmp")
                    evidence["path"] = path
                    evidence["temporary"] = temporary
                    if mode == "full-temp":
                        temporary.write_text(
                            json.dumps(
                                body,
                                ensure_ascii=False,
                                indent=2,
                                sort_keys=True,
                                allow_nan=False,
                            )
                            + "\n",
                            encoding="utf-8",
                        )
                    elif mode == "partial-temp":
                        temporary.write_bytes(b'{"schema_version":')
                    raise RuntimeError(f"kill in manifest {mode}")

                with patch.object(
                    paid.media_module,
                    "_stage_private_media_source_json",
                    side_effect=interrupt,
                ), self.assertRaises(paid.PaidSourceRefreshError):
                    case._run()
                self.assertEqual(case.calls["detail"], 1)

                def forbidden(*_args, **_kwargs):
                    raise AssertionError("manifest prefix recovery attempted network")

                if mode in {"partial-temp", "unknown-directory"}:
                    before = (
                        evidence["temporary"].read_bytes()
                        if mode == "partial-temp"
                        else None
                    )
                    with self.assertRaises(paid.PaidSourceRefreshError):
                        paid.run_refresh(
                            **case._kwargs(),
                            endpoint_info_fetcher=forbidden,
                            balance_checker=forbidden,
                            detail_fetcher=forbidden,
                            key_loader=forbidden,
                        )
                    if mode == "partial-temp":
                        self.assertEqual(evidence["temporary"].read_bytes(), before)
                    else:
                        self.assertTrue(
                            (evidence["path"].parents[2] / "unknown-sibling").is_dir()
                        )
                    self.assertFalse(evidence["path"].exists())
                else:
                    with patch.object(
                        paid.media_module,
                        "_stage_private_media_source_json",
                        wraps=original,
                    ):
                        resumed = paid.run_refresh(
                            **case._kwargs(),
                            endpoint_info_fetcher=forbidden,
                            balance_checker=forbidden,
                            detail_fetcher=forbidden,
                            key_loader=forbidden,
                        )
                    self.assertEqual(resumed["provider_calls"], 0)
                    self.assertTrue(evidence["path"].is_file())
                    self.assertFalse(evidence["temporary"].exists())
                    with closing(sqlite3.connect(case.db)) as connection:
                        self.assertEqual(
                            connection.execute(
                                "SELECT COUNT(*) FROM evidence_artifacts "
                                "WHERE artifact_type='media_source'"
                            ).fetchone()[0],
                            2,
                        )

    def test_state_receipt_and_completion_full_temps_recover_exactly(self) -> None:
        for attribute in ("state", "receipt", "completion"):
            with self.subTest(attribute=attribute):
                case = self._new_case()
                paths = paid._paths(**{
                    "source_db_path": case.fixture.source_db,
                    "source_completion_path": case.fixture.source_completion,
                    "db_path": case.db,
                    "raw_root": case.raw_root,
                    "media_root": case.media_root,
                    "run_root": case.run_root,
                })
                target = getattr(paths, attribute)
                original_replace = os.replace
                crashed = {"value": False}

                def replace(source, destination):
                    if Path(destination) == target and not crashed["value"]:
                        crashed["value"] = True
                        raise RuntimeError(f"kill before {attribute} rename")
                    return original_replace(source, destination)

                with patch.object(
                    paid.local_controller.os, "replace", side_effect=replace
                ), self.assertRaises(paid.PaidSourceRefreshError):
                    case._run()
                self.assertTrue(crashed["value"])
                db_sha = local_canary._sha256_file(case.db)
                with closing(sqlite3.connect(case.db)) as connection:
                    sequences = list(
                        connection.execute(
                            "SELECT name,seq FROM sqlite_sequence ORDER BY name"
                        )
                    )

                def forbidden(*_args, **_kwargs):
                    raise AssertionError("atomic record recovery attempted network")

                resumed = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
                self.assertEqual(resumed["provider_calls"], 0)
                self.assertEqual(local_canary._sha256_file(case.db), db_sha)
                with closing(sqlite3.connect(case.db)) as connection:
                    self.assertEqual(
                        list(
                            connection.execute(
                                "SELECT name,seq FROM sqlite_sequence ORDER BY name"
                            )
                        ),
                        sequences,
                    )
                self.assertFalse(target.with_name(f".{target.name}.tmp").exists())
                tree = case._tree(case.root)
                rerun = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
                self.assertTrue(rerun["idempotent"])
                self.assertEqual(tree, case._tree(case.root))

    def test_contract_intent_and_ledger_atomic_windows_resume_exactly(self) -> None:
        for attribute in ("contract", "intent", "ledger"):
            with self.subTest(attribute=attribute):
                case = self._new_case()
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                target = getattr(paths, attribute)
                original_replace = os.replace
                crashed = {"value": False}

                def replace(source, destination):
                    if Path(destination) == target and not crashed["value"]:
                        crashed["value"] = True
                        raise RuntimeError(f"kill before {attribute} rename")
                    return original_replace(source, destination)

                with patch.object(
                    paid.local_controller.os, "replace", side_effect=replace
                ), self.assertRaises(Exception):
                    case._run()
                self.assertTrue(crashed["value"])

                def no_price(*_args, **_kwargs):
                    raise AssertionError("atomic record recovery refetched price")

                resumed = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=no_price,
                    balance_checker=case._balance,
                    detail_fetcher=case._detail,
                    key_loader=lambda: "fixture-key",
                )
                self.assertEqual(resumed["provider_calls"], 1)
                self.assertEqual(case.calls["price"], 1)
                self.assertEqual(case.calls["detail"], 1)
                self.assertFalse(target.with_name(f".{target.name}.tmp").exists())

    def test_missing_intent_or_ledger_is_rebuilt_only_from_pristine_prefix(self) -> None:
        for attribute in ("intent", "ledger"):
            with self.subTest(attribute=attribute):
                case = self._new_case()
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                target = getattr(paths, attribute)
                original_write = paid._write_json
                interrupted = {"value": False}

                def write(path, value, *, immutable):
                    if path == target and not interrupted["value"]:
                        interrupted["value"] = True
                        raise RuntimeError(f"kill before {attribute} writer")
                    return original_write(path, value, immutable=immutable)

                with patch.object(paid, "_write_json", side_effect=write), self.assertRaises(
                    RuntimeError
                ):
                    case._run()
                self.assertTrue(interrupted["value"])
                self.assertFalse(target.exists())
                self.assertFalse(target.with_name(f".{target.name}.tmp").exists())

                def no_price(*_args, **_kwargs):
                    raise AssertionError("pristine record rebuild refetched price")

                resumed = paid.run_refresh(
                    **case._kwargs(),
                    endpoint_info_fetcher=no_price,
                    balance_checker=case._balance,
                    detail_fetcher=case._detail,
                    key_loader=lambda: "fixture-key",
                )
                self.assertEqual(resumed["provider_calls"], 1)
                self.assertEqual(case.calls["price"], 1)
                self.assertEqual(case.calls["detail"], 1)

    def test_missing_ledger_with_database_sidecar_blocks_without_reconstruction(self) -> None:
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        original_write = paid._write_json

        def write(path, value, *, immutable):
            if path == paths.ledger:
                raise RuntimeError("kill before ledger writer")
            return original_write(path, value, immutable=immutable)

        with patch.object(paid, "_write_json", side_effect=write), self.assertRaises(
            RuntimeError
        ):
            self._run()
        self.assertFalse(paths.ledger.exists())
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("BEGIN IMMEDIATE")

            def forbidden(*_args, **_kwargs):
                raise AssertionError("sidecar prefix attempted network")

            with self.assertRaises(paid.PaidSourceRefreshError):
                paid.run_refresh(
                    **self._kwargs(),
                    endpoint_info_fetcher=forbidden,
                    balance_checker=forbidden,
                    detail_fetcher=forbidden,
                    key_loader=forbidden,
                )
            self.assertFalse(paths.ledger.exists())
            self.assertEqual(self.calls["detail"], 0)

    def test_schema_pragma_drift_blocks_before_key_or_paid_call(self) -> None:
        stale = (
            datetime.now(timezone.utc) - timedelta(hours=1)
        ).isoformat(timespec="seconds")
        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(endpoint_info_fetcher=lambda: self._price(checked_at=stale))
        with closing(sqlite3.connect(self.db)) as connection:
            connection.execute("PRAGMA user_version=99")

        def forbidden(*_args, **_kwargs):
            raise AssertionError("tampered prefix attempted key/network")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(self.calls["detail"], 0)

    def test_unknown_output_or_record_temp_blocks_before_any_paid_call(self) -> None:
        self.run_root.mkdir()
        unknown = self.run_root / "unknown-record.json"
        unknown.write_text('{"unknown":true}\n', encoding="utf-8")
        before = unknown.read_bytes()
        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run()
        self.assertEqual(self.calls, {"price": 0, "balance": 0, "detail": 0})
        self.assertEqual(unknown.read_bytes(), before)

        case = self._new_case()
        with self.assertRaises(paid.PaidSourceRefreshError):
            case._run(key_loader=lambda: "")
        case.raw_root.mkdir(exist_ok=True)
        unknown_raw = case.raw_root / "unknown.json"
        unknown_raw.write_text("{}\n", encoding="utf-8")

        def forbidden(*_args, **_kwargs):
            raise AssertionError("unknown output prefix attempted paid call")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **case._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(unknown_raw.read_text(encoding="utf-8"), "{}\n")
        self.assertEqual(case.calls["detail"], 0)

    def test_ledger_attempt_flag_tamper_cannot_reenable_detail(self) -> None:
        with self.assertRaises(paid.PaidSourceRefreshError):
            self._run(key_loader=lambda: "")
        contract = paid._read_json(
            self.run_root / "refresh-contract.json", label="contract"
        )
        ledger_path = self.run_root / "provider-ledger.json"
        ledger = dict(paid._read_json(ledger_path, label="ledger"))
        balance = self._balance(
            "fixture-key", paid._json_sha256(contract["price_evidence"]["records"][1])
        )
        opening = {
            "index": 0,
            "phase": "opening",
            "request_id": ledger["request_id"],
            "endpoint": paid.DETAIL_PATH,
            "aweme_id": "canary-1",
            "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        ledger.update(
            {
                "attempt_consumed": False,
                "balance_check": balance,
                "events": [opening],
                "events_sha256": paid._json_sha256([opening]),
            }
        )
        ledger_path.write_bytes(paid._canonical_bytes(ledger))

        def forbidden(*_args, **_kwargs):
            raise AssertionError("tampered ledger re-enabled detail")

        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.run_refresh(
                **self._kwargs(),
                endpoint_info_fetcher=forbidden,
                balance_checker=forbidden,
                detail_fetcher=forbidden,
                key_loader=forbidden,
            )
        self.assertEqual(self.calls["detail"], 0)

    def test_handoff_rejects_contract_sha_before_contract_lineage_use(self) -> None:
        result = self._run()
        completion_path = self.run_root / "completion.json"
        completion = dict(paid._read_json(completion_path, label="completion"))
        completion["contract_sha256"] = "f" * 64
        completion_path.write_bytes(paid._canonical_bytes(completion))
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.validate_completion_for_local_analysis(
                source_db_path=self.db,
                source_completion_path=completion_path,
                expected_source_db_sha256=local_canary._sha256_file(self.db),
                expected_source_completion_sha256=local_canary._sha256_file(
                    completion_path
                ),
                content_ids=[1],
            )
        self.assertTrue(result["completion_sha256"])

    def test_contract_budget_amounts_and_counts_are_exact(self) -> None:
        for field, changed in (
            ("verified_unit_price", 0.0010000005),
            ("max_amount", 0.0010000005),
            ("max_billable_requests", 1.0),
            ("pilot_size", True),
        ):
            with self.subTest(field=field, changed=changed):
                case = self._new_case()
                case._run()
                contract_path = case.run_root / "refresh-contract.json"
                completion_path = case.run_root / "completion.json"
                contract = dict(
                    paid._read_json(contract_path, label="paid contract")
                )
                contract["budget"] = {**contract["budget"], field: changed}
                contract_path.write_bytes(paid._canonical_bytes(contract))
                completion = dict(
                    paid._read_json(completion_path, label="paid completion")
                )
                completion["contract_sha256"] = local_canary._sha256_file(
                    contract_path
                )
                completion_path.write_bytes(paid._canonical_bytes(completion))
                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.validate_completion_for_local_analysis(
                        source_db_path=case.db,
                        source_completion_path=completion_path,
                        expected_source_db_sha256=local_canary._sha256_file(case.db),
                        expected_source_completion_sha256=local_canary._sha256_file(
                            completion_path
                        ),
                        content_ids=[1],
                    )

    def test_reserved_unit_price_is_exact_before_detail(self) -> None:
        original = paid.capture_module._reserve_budget

        def drifted(*args, **kwargs):
            usage_id, _unit_price, currency = original(*args, **kwargs)
            return usage_id, 0.0010000005, currency

        with patch.object(
            paid.capture_module, "_reserve_budget", side_effect=drifted
        ), self.assertRaises(paid.PaidSourceRefreshError):
            self._run()
        self.assertEqual(self.calls["detail"], 0)
        ledger = paid._read_json(
            self.run_root / "provider-ledger.json", label="reserve drift ledger"
        )
        self.assertEqual(len(ledger["events"]), 1)

    def test_materialized_database_numbers_require_exact_types_and_amounts(self) -> None:
        mutations = (
            ("fetch_slots", "attempt_count", 1.5),
            ("fetch_attempts", "http_status", 200.5),
            ("fetch_attempts", "amount", 0.0010000005),
            ("provider_budget_batches", "max_billable_requests", 1.5),
            ("provider_usage", "amount", 0.0010000005),
        )
        for table, column, changed in mutations:
            with self.subTest(table=table, column=column, changed=changed):
                case = self._new_case()
                case._run()
                with closing(sqlite3.connect(case.db)) as connection:
                    connection.execute(f'UPDATE "{table}" SET "{column}"=?', (changed,))
                    connection.commit()
                paid.local_controller._finalize_database(case.db)
                contract = paid._read_json(
                    case.run_root / "refresh-contract.json", label="paid contract"
                )
                paths = paid._paths(
                    source_db_path=case.fixture.source_db,
                    source_completion_path=case.fixture.source_completion,
                    db_path=case.db,
                    raw_root=case.raw_root,
                    media_root=case.media_root,
                    run_root=case.run_root,
                )
                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid._validate_refresh_materialization(paths, contract)
                if table == "fetch_attempts" and column == "amount":
                    with self.assertRaises(paid.PaidSourceRefreshError):
                        paid._committed_raw(paths, contract)

        metadata_case = self._new_case()
        metadata_case._run()
        with closing(sqlite3.connect(metadata_case.db)) as connection:
            row = connection.execute(
                "SELECT id,metadata_json FROM evidence_artifacts "
                "WHERE artifact_type='media_source' ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row)
            metadata = json.loads(str(row[1]))
            metadata["source_count"] = 1.0
            connection.execute(
                "UPDATE evidence_artifacts SET metadata_json=? WHERE id=?",
                (json.dumps(metadata, sort_keys=True), row[0]),
            )
            connection.commit()
        paid.local_controller._finalize_database(metadata_case.db)
        metadata_contract = paid._read_json(
            metadata_case.run_root / "refresh-contract.json", label="paid contract"
        )
        metadata_paths = paid._paths(
            source_db_path=metadata_case.fixture.source_db,
            source_completion_path=metadata_case.fixture.source_completion,
            db_path=metadata_case.db,
            raw_root=metadata_case.raw_root,
            media_root=metadata_case.media_root,
            run_root=metadata_case.run_root,
        )
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid._validate_refresh_materialization(metadata_paths, metadata_contract)

        usage_case = self._new_case()
        usage_case._run()
        with closing(sqlite3.connect(usage_case.db)) as connection:
            row = connection.execute(
                "SELECT id,details_json FROM provider_usage ORDER BY id DESC LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(row)
            details = json.loads(str(row[1]))
            details["http_status"] = 200.0
            connection.execute(
                "UPDATE provider_usage SET details_json=? WHERE id=?",
                (json.dumps(details, sort_keys=True), row[0]),
            )
            connection.commit()
        paid.local_controller._finalize_database(usage_case.db)
        usage_contract = paid._read_json(
            usage_case.run_root / "refresh-contract.json", label="paid contract"
        )
        usage_paths = paid._paths(
            source_db_path=usage_case.fixture.source_db,
            source_completion_path=usage_case.fixture.source_completion,
            db_path=usage_case.db,
            raw_root=usage_case.raw_root,
            media_root=usage_case.media_root,
            run_root=usage_case.run_root,
        )
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid._validate_refresh_materialization(usage_paths, usage_contract)

    def test_receipt_nested_numeric_types_are_canonical_exact(self) -> None:
        self._run()
        receipt_path = self.run_root / "refresh-receipt.json"
        completion_path = self.run_root / "completion.json"
        receipt = dict(paid._read_json(receipt_path, label="paid receipt"))
        receipt["capture"] = {**receipt["capture"], "http_status": 200.0}
        receipt_path.write_bytes(paid._canonical_bytes(receipt))
        completion = {**receipt, "receipt_sha256": local_canary._sha256_file(receipt_path)}
        completion_path.write_bytes(paid._canonical_bytes(completion))
        contract = paid._read_json(
            self.run_root / "refresh-contract.json", label="paid contract"
        )
        paths = paid._paths(
            source_db_path=self.fixture.source_db,
            source_completion_path=self.fixture.source_completion,
            db_path=self.db,
            raw_root=self.raw_root,
            media_root=self.media_root,
            run_root=self.run_root,
        )
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid._validate_success_records(
                paths,
                contract,
                expected_source_db_sha256=self.source_db_sha,
                expected_source_completion_sha256=self.source_completion_sha,
            )

    def test_paid_handoff_rejects_transitive_step3_output_overlap(self) -> None:
        result = self._run()
        analysis_root = self.root / "separation-analysis"
        analysis_root.mkdir()
        (analysis_root / "db").mkdir()
        before = self._tree(self.fixture.step3_root)
        with self.assertRaises(local_canary.LocalAnalysisCanaryError):
            local_canary.plan_canary(
                source_db_path=self.db,
                source_completion_path=self.run_root / "completion.json",
                expected_source_db_sha256=local_canary._sha256_file(self.db),
                expected_source_completion_sha256=result["completion_sha256"],
                db_path=analysis_root / "db" / "analysis.sqlite3",
                media_root=self.fixture.source_root / "forbidden-local-media",
                run_root=analysis_root / "run",
                content_ids=[1],
            )
        self.assertEqual(before, self._tree(self.fixture.step3_root))

    def test_paid_static_completion_revalidates_parent_separation(self) -> None:
        result = self._run()
        completion_path = self.run_root / "completion.json"
        database_sha256 = local_canary._sha256_file(self.db)
        original = paid._validate_parent_separation
        seen: list[tuple[paid.RefreshPaths, object]] = []

        def validating(paths, source_evidence):
            seen.append((paths, source_evidence))
            return original(paths, source_evidence)

        with patch.object(paid, "_validate_parent_separation", side_effect=validating):
            evidence = paid.validate_completion_for_local_analysis(
                source_db_path=self.db,
                source_completion_path=completion_path,
                expected_source_db_sha256=database_sha256,
                expected_source_completion_sha256=result["completion_sha256"],
                content_ids=[1],
            )
        self.assertEqual(evidence["completion_kind"], paid.COMPLETION_KIND)
        self.assertGreaterEqual(len(seen), 1)
        self.assertEqual(
            seen[-1][1]["database"]["path"], str(self.fixture.source_db)
        )

        contract_path = self.run_root / "refresh-contract.json"
        contract = dict(paid._read_json(contract_path, label="paid contract"))
        contract["roots"] = {
            **contract["roots"],
            "raw_root": contract["base_source"]["contract"]["derived_raw_root"],
        }
        contract_path.write_bytes(paid._canonical_bytes(contract))
        completion = dict(
            paid._read_json(completion_path, label="paid completion")
        )
        completion["contract_sha256"] = local_canary._sha256_file(contract_path)
        completion_path.write_bytes(paid._canonical_bytes(completion))
        step3_tree = self._tree(self.fixture.step3_root)
        with self.assertRaises(paid.PaidSourceRefreshError):
            paid.validate_completion_for_local_analysis(
                source_db_path=self.db,
                source_completion_path=completion_path,
                expected_source_db_sha256=database_sha256,
                expected_source_completion_sha256=local_canary._sha256_file(
                    completion_path
                ),
                content_ids=[1],
            )
        self.assertEqual(self._tree(self.fixture.step3_root), step3_tree)

    def test_completion_and_state_times_remain_bound_to_terminal_ledger(self) -> None:
        for mutation in ("completion", "state"):
            with self.subTest(mutation=mutation):
                case = self._new_case()
                case._run()
                state_path = case.run_root / "refresh-state.json"
                receipt_path = case.run_root / "refresh-receipt.json"
                completion_path = case.run_root / "completion.json"
                state = dict(paid._read_json(state_path, label="state"))
                receipt = dict(paid._read_json(receipt_path, label="receipt"))
                changed = "2030-01-01T00:00:00+00:00"
                if mutation == "completion":
                    receipt["completed_at"] = changed
                else:
                    state["updated_at"] = changed
                    state_path.write_bytes(paid._canonical_bytes(state))
                    receipt["state_sha256"] = local_canary._sha256_file(state_path)
                receipt_path.write_bytes(paid._canonical_bytes(receipt))
                completion = {
                    **receipt,
                    "receipt_sha256": local_canary._sha256_file(receipt_path),
                }
                completion_path.write_bytes(paid._canonical_bytes(completion))
                with self.assertRaises(paid.PaidSourceRefreshError):
                    paid.validate_completion_for_local_analysis(
                        source_db_path=case.db,
                        source_completion_path=completion_path,
                        expected_source_db_sha256=local_canary._sha256_file(case.db),
                        expected_source_completion_sha256=local_canary._sha256_file(
                            completion_path
                        ),
                        content_ids=[1],
                    )


if __name__ == "__main__":
    unittest.main()
