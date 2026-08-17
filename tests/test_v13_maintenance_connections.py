from __future__ import annotations

import importlib.util
import sqlite3
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import ModuleType
from typing import Callable, ContextManager

import v8.storage as storage  # type: ignore[import-untyped]
from scripts import run_full_history_cache_batches as history_cache_batches
from scripts import run_full_local_analysis_batches as full_local_batches
from scripts import run_local_analysis_canary as local_canary
from v8 import release_management  # type: ignore[import-untyped]
from v8 import release_management_v5_1  # type: ignore[import-untyped]
from v8 import release_management_v9  # type: ignore[import-untyped]
from v8 import report_repair  # type: ignore[import-untyped]
from v8 import taxonomy_rule_backfill  # type: ignore[import-untyped]
from v8 import taxonomy_v5_2_builder  # type: ignore[import-untyped]


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATHS = (
    PROJECT_ROOT / "scripts" / "repair_comment_parent_zero.py",
    PROJECT_ROOT / "scripts" / "disable_unresolvable_identities.py",
)
CAPTURED_AT = "2026-08-15T00:00:00Z"


def _load_script(path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load maintenance script: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class V13MaintenanceConnectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "maintenance.sqlite3"
        with storage.connect(self.database) as connection:
            storage.initialize_database(connection)
            self.run_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_runs(
                        job_id,scheduled_for,status,started_at,completed_at,
                        details_json
                    ) VALUES ('daily_report',?,'succeeded',?,?,'{}')
                    """,
                    (CAPTURED_AT, CAPTURED_AT, "2026-08-15T00:01:00Z"),
                ).lastrowid
            )
            self.attempt_id = int(
                connection.execute(
                    """
                    INSERT INTO scheduler_run_attempts(
                        scheduler_run_id,attempt_number,invocation_source,status,
                        started_at,completed_at,details_json
                    ) VALUES (?,1,'scheduled','succeeded',?,?,?)
                    """,
                    (
                        self.run_id,
                        CAPTURED_AT,
                        "2026-08-15T00:01:00Z",
                        '{"original":true}',
                    ),
                ).lastrowid
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _assert_guarded_writable(self, connection: sqlite3.Connection) -> None:
        self.assertEqual(
            int(connection.execute("PRAGMA recursive_triggers").fetchone()[0]), 1
        )
        self.assertEqual(
            int(connection.execute("PRAGMA foreign_keys").fetchone()[0]), 1
        )
        with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
            connection.execute(
                """
                INSERT OR REPLACE INTO scheduler_run_attempts(
                    id,scheduler_run_id,attempt_number,
                    invocation_source,status,started_at,completed_at,
                    details_json
                ) VALUES (?, ?, 1, 'operator_retry', 'failed', ?, ?, ?)
                """,
                (
                    self.attempt_id,
                    self.run_id,
                    CAPTURED_AT,
                    "2026-08-15T01:00:00Z",
                    '{"tampered":true}',
                ),
            )
        connection.rollback()
        with storage.connect(self.database, read_only=False) as verifier:
            row = verifier.execute(
                """
                SELECT invocation_source,status,details_json
                FROM scheduler_run_attempts WHERE id=?
                """,
                (self.attempt_id,),
            ).fetchone()
            self.assertEqual(
                tuple(row),
                ("scheduled", "succeeded", '{"original":true}'),
            )

    def test_maintenance_connections_preserve_attempt_append_only_guards(self) -> None:
        for path in SCRIPT_PATHS:
            module = _load_script(path)
            with self.subTest(script=path.name):
                with closing(
                    module._connect(self.database, read_only=True)
                ) as read_only:
                    self.assertEqual(
                        int(
                            read_only.execute("PRAGMA recursive_triggers").fetchone()[0]
                        ),
                        1,
                    )
                    self.assertEqual(
                        int(read_only.execute("PRAGMA foreign_keys").fetchone()[0]),
                        1,
                    )

                with closing(
                    module._connect(self.database, read_only=False)
                ) as writable:
                    self.assertEqual(
                        int(
                            writable.execute("PRAGMA recursive_triggers").fetchone()[0]
                        ),
                        1,
                    )
                    self.assertEqual(
                        int(writable.execute("PRAGMA foreign_keys").fetchone()[0]),
                        1,
                    )
                    with self.assertRaisesRegex(sqlite3.IntegrityError, "append-only"):
                        writable.execute(
                            """
                            INSERT OR REPLACE INTO scheduler_run_attempts(
                                id,scheduler_run_id,attempt_number,
                                invocation_source,status,started_at,completed_at,
                                details_json
                            ) VALUES (?, ?, 1, 'operator_retry', 'failed', ?, ?, ?)
                            """,
                            (
                                self.attempt_id,
                                self.run_id,
                                CAPTURED_AT,
                                "2026-08-15T01:00:00Z",
                                '{"tampered":true}',
                            ),
                        )
                    writable.rollback()

                with storage.connect(self.database, read_only=False) as connection:
                    row = connection.execute(
                        """
                        SELECT invocation_source,status,details_json
                        FROM scheduler_run_attempts WHERE id=?
                        """,
                        (self.attempt_id,),
                    ).fetchone()
                    self.assertEqual(
                        tuple(row),
                        ("scheduled", "succeeded", '{"original":true}'),
                    )

    def test_formal_capable_wrappers_share_verified_connection_guards(self) -> None:
        factories: tuple[
            tuple[str, Callable[[Path], ContextManager[sqlite3.Connection]]], ...
        ] = (
            (
                "taxonomy_rule_backfill",
                taxonomy_rule_backfill._connect_read_write,
            ),
            (
                "taxonomy_v5_2_builder",
                lambda path: taxonomy_v5_2_builder._connect(path, read_only=False),
            ),
            (
                "release_management",
                lambda path: release_management._existing_connection(
                    path, read_only=False
                ),
            ),
            (
                "release_management_v5_1",
                lambda path: release_management_v5_1._existing_connection(
                    path, read_only=False
                ),
            ),
            (
                "release_management_v9",
                lambda path: release_management_v9._connect(path, read_only=False),
            ),
            (
                "report_repair",
                lambda path: report_repair._existing_connection(
                    path, read_only=False
                ),
            ),
        )
        for name, factory in factories:
            with self.subTest(connector=name), factory(self.database) as connection:
                self._assert_guarded_writable(connection)

    def test_clone_dml_connections_share_verified_connection_guards(self) -> None:
        factories: tuple[
            tuple[str, Callable[[Path], sqlite3.Connection]], ...
        ] = (
            (
                "history_interrupted_detail",
                lambda path: history_cache_batches._writable_connection(
                    path, row_factory=True
                ),
            ),
            (
                "history_pending_raw",
                history_cache_batches._writable_connection,
            ),
            ("full_local_deferred_anchor", full_local_batches._writable_connection),
            ("local_output_recovery", local_canary._writable_connection),
        )
        for name, factory in factories:
            with self.subTest(connector=name), closing(factory(self.database)) as connection:
                self._assert_guarded_writable(connection)


if __name__ == "__main__":
    unittest.main()
