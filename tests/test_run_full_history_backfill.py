from __future__ import annotations

import json
import os
import tempfile
import unittest
from contextlib import ExitStack, redirect_stdout
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_full_history_backfill as runner
from v8.operations import upsert_content
from v8.storage import connect, initialize_database


class FullHistoryWrapperTest(unittest.TestCase):
    def test_readonly_guard_rejects_existing_formal_alias_by_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            formal = root / "formal.sqlite3"
            alias = root / "apfs-firmlink-spelling.sqlite3"
            formal.write_bytes(b"formal-sentinel")
            alias.hardlink_to(formal)
            with (
                patch.object(runner, "FORMAL_DB", formal),
                patch.dict(os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "1"}),
                self.assertRaisesRegex(RuntimeError, "formal DCar database"),
            ):
                runner.open_readonly_database(alias)

    @staticmethod
    def _discovery_result(status: str, reason: str | None) -> dict:
        return {
            "status": status,
            "stopped_reason": reason,
            "accounts_completed": 0 if status != "succeeded" else 1,
            "accounts_considered": 1,
            "pages_processed": 1,
            "inserted": 0,
            "usage": {"amount": 1.0},
            "content_manifest": {},
        }

    @staticmethod
    def _seed_contract_database(database: Path, *, rule_version: str) -> None:
        captured_at = "2026-08-07T12:00:00Z"
        with connect(database) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES (
                    'taxonomy-test','selling-points-v5.2','published','{}',?,?
                )
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (
                    'release-test',?,'selling-points-v5.2',?,
                    'active',?,?,?
                )
                """,
                (
                    rule_version,
                    "a" * 64,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            connection.commit()

    def test_discovery_extension_only_handles_task_budget_block(self) -> None:
        state = {"phases": {}}
        with patch.object(
            runner,
            "run_command",
            return_value=self._discovery_result(
                "blocked", "provider_auth_blocked"
            ),
        ) as command, patch.object(runner, "save_state"), patch.object(
            runner, "log"
        ):
            with self.assertRaisesRegex(runner.AbortRun, "非预算原因"):
                runner.phase_discover(
                    state,
                    campaign="campaign",
                    end="2026-08-07T20:00:00+08:00",
                    as_of="2026-08-07T20:01:00+08:00",
                    archive_before="2026-02-07T00:00:00+08:00",
                    first_cap=1.0,
                    extension_cap=1.0,
                    max_pages=20,
                )
        self.assertEqual(command.call_count, 1)

        state = {"phases": {}}
        with patch.object(
            runner,
            "run_command",
            side_effect=[
                self._discovery_result("blocked", "task_budget_exhausted"),
                self._discovery_result("succeeded", None),
            ],
        ) as command, patch.object(runner, "save_state"), patch.object(
            runner, "log"
        ):
            runner.phase_discover(
                state,
                campaign="campaign",
                end="2026-08-07T20:00:00+08:00",
                as_of="2026-08-07T20:01:00+08:00",
                archive_before="2026-02-07T00:00:00+08:00",
                first_cap=1.0,
                extension_cap=1.0,
                max_pages=20,
            )
        self.assertEqual(command.call_count, 2)

    def test_preflight_uses_provider_user_agent_and_redacts_balance(self) -> None:
        class Response:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def read() -> bytes:
                return b'{"data":{"balance":12.34}}'

        with tempfile.TemporaryDirectory() as temporary:
            key_file = Path(temporary) / "TikHub.env.local"
            key_file.write_text("TIKHUB_API_KEY=test-secret\n", encoding="utf-8")
            with patch.object(runner, "TIKHUB_KEY_FILE", key_file), patch(
                "urllib.request.urlopen", return_value=Response()
            ) as urlopen, patch.object(runner, "log") as log:
                runner.phase_preflight()

        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), "DCar-Insight/1.0")
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertNotIn("12.34", " ".join(str(call) for call in log.call_args_list))

    def test_campaign_contract_loads_complete_active_release_row(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "contract.sqlite3"
            self._seed_contract_database(db, rule_version="evaluation-v8")

            state: dict = {}
            with patch.object(runner, "DB", db), patch.object(
                runner, "save_state"
            ), patch(
                "v8.evaluation._load_release_runtime",
                return_value=SimpleNamespace(matcher=object()),
            ) as load_runtime:
                runner.bind_campaign_contract(
                    state,
                    end="2026-08-07T23:00:00+08:00",
                    archive_before="2026-02-07T00:00:00+08:00",
                )

        release = load_runtime.call_args.args[1]
        self.assertEqual(release["status"], "active")
        self.assertEqual(release["activated_at"], "2026-08-07T12:00:00Z")
        self.assertEqual(
            state["campaign_contract"]["active_release"]["status"], "active"
        )

    def test_campaign_contract_accepts_active_v9_materialized_release(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "contract.sqlite3"
            self._seed_contract_database(db, rule_version="evaluation-v9")
            state: dict = {}
            with patch.object(runner, "DB", db), patch.object(
                runner, "save_state"
            ), patch(
                "v8.evaluation._load_release_runtime",
                return_value=SimpleNamespace(matcher=object()),
            ):
                runner.bind_campaign_contract(
                    state,
                    end="2026-08-07T23:00:00+08:00",
                    archive_before="2026-02-07T00:00:00+08:00",
                )
        self.assertEqual(
            state["campaign_contract"]["active_release"]["rule_version"],
            "evaluation-v9",
        )

    def test_existing_v8_campaign_fails_closed_after_v9_switch(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "contract.sqlite3"
            self._seed_contract_database(db, rule_version="evaluation-v8")
            state: dict = {}
            with patch.object(runner, "DB", db), patch.object(
                runner, "save_state"
            ), patch(
                "v8.evaluation._load_release_runtime",
                return_value=SimpleNamespace(matcher=object()),
            ):
                runner.bind_campaign_contract(
                    state,
                    end="2026-08-07T23:00:00+08:00",
                    archive_before="2026-02-07T00:00:00+08:00",
                )
                with connect(db) as connection:
                    connection.execute(
                        "UPDATE evaluation_releases SET rule_version='evaluation-v9'"
                    )
                    connection.commit()
                with self.assertRaisesRegex(runner.AbortRun, "不得在同一战役"):
                    runner.bind_campaign_contract(
                        state,
                        end="2026-08-07T23:00:00+08:00",
                        archive_before="2026-02-07T00:00:00+08:00",
                    )

    def test_campaign_scope_baseline_rejects_preexisting_queue(self) -> None:
        state: dict = {}
        with patch.object(
            runner,
            "_history_scope_rows",
            return_value=[{"id": 7, "source_group": "history-backfill"}],
        ), patch.object(runner, "save_state"):
            with self.assertRaisesRegex(runner.AbortRun, "已存在 history"):
                runner.ensure_campaign_scope_baseline(state)
        self.assertNotIn("history_scope_baseline", state)

    def test_campaign_cohort_uses_atomic_source_group_delta(self) -> None:
        state = {
            "history_scope_baseline": {"content_ids": [], "count": 0},
        }
        with patch.object(
            runner,
            "_history_scope_rows",
            return_value=[{"id": 9, "source_group": "history-archive"}],
        ), patch.object(runner, "save_state"):
            runner.freeze_campaign_cohort(state)
        self.assertEqual(
            state["campaign_cohort"]["contents"],
            [{"content_id": 9, "initial_scope": "history-archive"}],
        )

    def test_stale_slots_recover_but_fresh_running_slots_block(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = Path(temporary) / "recovery.sqlite3"
            with connect(db) as connection:
                initialize_database(connection)
            content = upsert_content(
                {
                    "platform": "douyin",
                    "canonical_url": "https://www.douyin.com/video/9988776655",
                    "title": "中断恢复",
                },
                db_path=db,
            )
            old = (
                datetime.now(timezone.utc) - timedelta(minutes=20)
            ).isoformat(timespec="seconds").replace("+00:00", "Z")
            with connect(db) as connection:
                connection.execute(
                    """
                    INSERT INTO fetch_slots(
                        content_id,stage,window_key,provider,adapter_version,
                        status,attempt_count,started_at,created_at,updated_at
                    ) VALUES (?, 'detail', 'lifetime', 'TikHub', 'test-v1',
                              'running', 1, ?, ?, ?)
                    """,
                    (content["id"], old, old, old),
                )
                connection.commit()

            state: dict = {}
            with patch.object(runner, "DB", db), patch.object(
                runner, "save_state"
            ), patch.object(runner, "log"):
                runner.phase_recover_stale_slots(state)
            with connect(db) as connection:
                slot = connection.execute(
                    "SELECT status,last_error_code FROM fetch_slots"
                ).fetchone()
            self.assertEqual(tuple(slot), ("retryable_failed", "interrupted"))
            self.assertEqual(
                state["stale_recovery_runs"][-1]["fetch"]["recovered"], 1
            )

            fresh = datetime.now(timezone.utc).isoformat(
                timespec="seconds"
            ).replace("+00:00", "Z")
            with connect(db) as connection:
                connection.execute(
                    """
                    UPDATE fetch_slots SET status='running',started_at=?,updated_at=?
                    """,
                    (fresh, fresh),
                )
                connection.commit()
            with patch.object(runner, "DB", db), patch.object(
                runner, "save_state"
            ), patch.object(runner, "log"), self.assertRaisesRegex(
                runner.AbortRun, "禁止强抢"
            ):
                runner.phase_recover_stale_slots(state)

    def test_dry_run_discloses_both_discovery_tranches_and_all_phases(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            exit_code = runner.main(
                [
                    "--dry-run",
                    "--end",
                    "2026-08-07T20:00:00+08:00",
                    "--archive-before",
                    "2026-02-07T00:00:00+08:00",
                    "--discovery-budget",
                    "30",
                    "--discovery-extension-budget",
                    "30",
                ]
            )

        self.assertEqual(exit_code, 0)
        plan = json.loads(output.getvalue())
        self.assertEqual(plan["discovery_ceiling_usd"], 60.0)
        commands = plan["commands"]
        self.assertEqual(
            sum(" discover " in command for command in commands), 2
        )
        discovery_commands = [
            command for command in commands if " discover " in command
        ]
        self.assertTrue(
            all("--require-live-detail" in command for command in discovery_commands)
        )
        self.assertTrue(
            all(
                "--skip-existing-derived-stages" in command
                for command in discovery_commands
            )
        )
        self.assertTrue(
            any("--platform douyin --stage detail" in command for command in commands)
        )
        self.assertTrue(
            any(
                "--platform xiaohongshu --stage detail --stage metrics" in command
                for command in commands
            )
        )
        self.assertTrue(
            any("fetch-repaired-metrics" in command for command in commands)
        )
        self.assertTrue(any("v8.duplicates fingerprint" in command for command in commands))
        self.assertTrue(any("--all-contents" in command for command in commands))
        paid = [
            command
            for command in commands
            if " content " in command or "fetch-repaired-metrics" in command
        ]
        self.assertTrue(paid)
        self.assertTrue(all("--history-only" in command for command in paid))
        self.assertTrue(all("--as-of" in command for command in paid))
        local_evidence = [
            command
            for command in commands
            if "range_backfill local-evidence" in command
        ]
        self.assertEqual(len(local_evidence), 1)
        self.assertIn("--as-of <实际数据采集截面>", local_evidence[0])

    def test_execute_orders_evidence_before_classifier_and_always_releases_lock(self) -> None:
        events: list[str] = []
        state = {"phases": {}, "budgets": {}}
        budgets = {
            "metrics_douyin": 1.0,
            "metrics_repair_douyin": 1.0,
            "detail_douyin": 1.0,
            "detail_xhs": 1.0,
            "comments_douyin": 1.0,
            "comments_xhs": 1.0,
        }

        def mark(name):
            def inner(*_args, **_kwargs):
                events.append(name)
            return inner

        def content(*args, **_kwargs):
            events.append(f"content:{args[1]}")

        with ExitStack() as stack:
            stack.enter_context(patch.object(runner, "log"))
            stack.enter_context(patch.object(runner, "gate_environment", side_effect=mark("gate")))
            stack.enter_context(patch.object(runner, "acquire_operation_lock", side_effect=mark("lock")))
            stack.enter_context(patch.object(runner, "gate_services_stopped", side_effect=mark("ports")))
            stack.enter_context(patch.object(runner, "gate_clean_snapshot", side_effect=mark("clean")))
            stack.enter_context(patch.object(runner, "gate_repository_hygiene", side_effect=mark("hygiene")))
            stack.enter_context(patch.object(runner, "load_state", return_value=state))
            stack.enter_context(patch.object(runner, "save_state"))
            stack.enter_context(patch.object(runner, "bind_campaign_contract", side_effect=mark("contract")))
            stack.enter_context(patch.object(runner, "ensure_campaign_scope_baseline", side_effect=mark("baseline")))
            stack.enter_context(patch.object(runner, "freeze_campaign_cohort", side_effect=mark("cohort")))
            stack.enter_context(patch.object(runner, "backup_database", side_effect=lambda: events.append("backup") or {"database": "backup"}))
            stack.enter_context(patch.object(runner, "phase_recover_stale_slots", side_effect=mark("recover")))
            stack.enter_context(patch.object(runner, "phase_preflight", side_effect=mark("preflight")))
            stack.enter_context(patch.object(runner, "gate_tests", side_effect=mark("tests")))
            stack.enter_context(patch.object(runner, "phase_discover", side_effect=mark("discover")))
            stack.enter_context(patch.object(runner, "phase_quote", side_effect=lambda *_args, **_kwargs: events.append("quote") or budgets))
            stack.enter_context(patch.object(runner, "run_content_phase", side_effect=content))
            stack.enter_context(patch.object(runner, "phase_repair_metrics", side_effect=mark("repair")))
            stack.enter_context(patch.object(runner, "phase_local_evidence", side_effect=lambda *_args, **_kwargs: events.append("local") or 0))
            stack.enter_context(patch.object(runner, "phase_duplicate_rebuild", side_effect=mark("duplicates")))
            stack.enter_context(patch.object(runner, "phase_classifier", side_effect=mark("classifier")))
            stack.enter_context(patch.object(runner, "phase_postflight", side_effect=mark("postflight")))
            release = stack.enter_context(patch.object(runner, "release_operation_lock", side_effect=mark("release")))

            exit_code = runner.main(
                [
                    "--execute",
                    "--end",
                    "2026-08-07T20:00:00+08:00",
                    "--archive-before",
                    "2026-02-07T00:00:00+08:00",
                    "--discovery-budget",
                    "1",
                    "--auto-ceiling",
                    "10",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertLess(events.index("backup"), events.index("tests"))
        self.assertLess(events.index("backup"), events.index("recover"))
        self.assertLess(events.index("recover"), events.index("preflight"))
        self.assertLess(events.index("local"), events.index("duplicates"))
        self.assertLess(events.index("duplicates"), events.index("classifier"))
        self.assertLess(events.index("classifier"), events.index("postflight"))
        self.assertEqual(
            [event for event in events if event.startswith("content:")],
            [
                "content:metrics_douyin",
                "content:detail_douyin",
                "content:detail_xhs",
                "content:comments_douyin",
                "content:comments_xhs",
            ],
        )
        release.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
