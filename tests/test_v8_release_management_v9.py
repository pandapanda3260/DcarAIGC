from __future__ import annotations

import tempfile
import unittest
import sqlite3
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

from v8 import release_management_v9 as releases  # type: ignore[import-untyped]
from v8.evaluation import evaluate_content  # type: ignore[import-untyped]
from v8.storage import connect, initialize_database, now_utc  # type: ignore[import-untyped]
from v8.taxonomy_v5_2_builder import (  # type: ignore[import-untyped]
    TARGET_TAXONOMY_ID,
    _business_points,
    _insert_point,
    _matcher_rules,
)


class EvaluationV9ReleaseLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.db = self.root / "snapshot.sqlite3"
        self.manifest = self.root / "manifest.json"
        self.receipt = self.root / "receipt.json"
        self._build_source_database()
        prepared = releases.prepare_manifest(
            db_path=self.db, manifest_path=self.manifest
        )
        self.manifest_sha256 = str(prepared["manifest_sha256"])
        self.assertEqual(prepared["content_count"], 2)
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        # v16 起复核域已删除：新清单里活跃队列快照恒为空
        self.assertEqual(manifest["legacy_active_review_queues"], [])

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _build_source_database(self) -> None:
        business, source, source_sha, source_path = _business_points(
            Path("config/business_selling_points_v5_2.json").resolve()
        )
        rules, matcher_sha, _bundle_sha, _bundle_path = _matcher_rules(
            Path("config/selling_point_matcher_v4.json").resolve()
        )
        captured_at = now_utc()
        with connect(self.db) as connection:
            initialize_database(connection)
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,source_path,source_sha256,
                    created_at,published_at
                ) VALUES (?,?,'published',?,?,?,?,?)
                """,
                (
                    TARGET_TAXONOMY_ID,
                    releases.TAXONOMY_VERSION,
                    source["definition"],
                    source_path,
                    source_sha,
                    captured_at,
                    captured_at,
                ),
            )
            for code in sorted(rules):
                _insert_point(connection, source=business[code], rule=rules[code])
            connection.execute(
                """
                INSERT INTO evaluation_releases(
                    id,rule_version,taxonomy_version,matcher_rule_sha256,status,
                    created_at,updated_at,activated_at
                ) VALUES (?,?,?,?, 'active',?,?,?)
                """,
                (
                    releases.SOURCE_RELEASE_ID,
                    releases.SOURCE_RULE_VERSION,
                    releases.TAXONOMY_VERSION,
                    matcher_sha,
                    captured_at,
                    captured_at,
                    captured_at,
                ),
            )
            for suffix in range(1, 4):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,title,body,
                        content_type,evaluation_content_direction,imported_at,created_at,
                        updated_at
                    ) VALUES (?,'douyin',?,?,?,'普通汽车内容','video','unknown',?,?,?)
                    """,
                    (
                        f"V9{suffix:04d}",
                        f"v9-{suffix}",
                        f"https://example.test/v9/{suffix}",
                        f"规则切换样本 {suffix}",
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                )
            connection.commit()
        evaluate_content(1, db_path=self.db)
        evaluate_content(2, db_path=self.db)
        self.queue_baseline = []
        checkpoint = sqlite3.connect(self.db)
        try:
            checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            checkpoint.close()

    def _create_and_backfill(self) -> None:
        created = releases.create(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
        )
        self.assertEqual(created["target_release"]["status"], "draft")
        result = releases.backfill(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
        )
        self.assertEqual(result["provider_usage_rows_added"], 0)

    def _verify_ready(self) -> str:
        result = releases.verify_ready(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            receipt_path=self.receipt,
        )
        self.assertEqual(result["status"], "ready")
        return str(result["receipt_sha256"])

    def test_full_lifecycle_reuses_taxonomy_closes_queue_and_rolls_back(self) -> None:
        with connect(self.db) as connection:
            taxonomy_before = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM taxonomy_versions ORDER BY version"
                )
            ]
        self._create_and_backfill()
        receipt_sha = self._verify_ready()
        activated = releases.activate(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            receipt_path=self.receipt,
            receipt_sha256=receipt_sha,
        )
        self.assertEqual(activated["status"], "active")
        with connect(self.db) as connection:
            target_rows = connection.execute(
                "SELECT evaluation_source FROM evaluation_versions WHERE release_id=?",
                (releases.TARGET_RELEASE_ID,),
            ).fetchall()
            self.assertEqual(len(target_rows), 2)
            self.assertTrue(
                all(row["evaluation_source"] == "automatic" for row in target_rows)
            )
            taxonomy_after = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM taxonomy_versions ORDER BY version"
                )
            ]
        self.assertEqual(taxonomy_after, taxonomy_before)
        rolled_back = releases.rollback_before_resume(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            receipt_path=self.receipt,
            receipt_sha256=receipt_sha,
            reason="fault before report resume",
        )
        self.assertEqual(rolled_back["restored_release_id"], releases.SOURCE_RELEASE_ID)

    def test_backfill_is_resumable_after_fault(self) -> None:
        releases.create(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
        )

        def fail(name: str) -> None:
            if name == "backfill_batch_verified":
                raise RuntimeError("injected batch fault")

        with (
            patch.object(releases, "_checkpoint", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "injected batch fault"),
        ):
            releases.backfill(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
            )
        resumed = releases.backfill(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
        )
        self.assertEqual((resumed["created"], resumed["reused"]), (0, 2))

    def test_backfill_full_digest_runs_once_not_once_per_batch(self) -> None:
        releases.create(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
        )
        original = releases._protected_state
        with (
            patch.object(releases, "BATCH_SIZE", 1),
            patch.object(releases, "_protected_state", wraps=original) as protected,
        ):
            result = releases.backfill(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
            )
        self.assertEqual(result["completed_batches"], 2)
        self.assertEqual(protected.call_count, 1)

    def test_activation_fault_rolls_back_single_transaction(self) -> None:
        self._create_and_backfill()
        receipt_sha = self._verify_ready()

        def fail(name: str) -> None:
            if name == "activation_legacy_queues_closed":
                raise RuntimeError("injected activation fault")

        with (
            patch.object(releases, "_checkpoint", side_effect=fail),
            self.assertRaisesRegex(RuntimeError, "injected activation fault"),
        ):
            releases.activate(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
                receipt_path=self.receipt,
                receipt_sha256=receipt_sha,
            )
        with connect(self.db) as connection:
            states = {
                str(row["id"]): str(row["status"])
                for row in connection.execute(
                    "SELECT id,status FROM evaluation_releases"
                )
            }
        self.assertEqual(states[releases.SOURCE_RELEASE_ID], "active")
        self.assertEqual(states[releases.TARGET_RELEASE_ID], "ready")

    def test_abort_fails_nonactive_release_and_invalidates_partial_rows(self) -> None:
        self._create_and_backfill()
        with connect(self.db) as connection:
            connection.execute(
                "UPDATE content_items SET title='source drift after backfill' WHERE id=3"
            )
            connection.commit()
        result = releases.abort(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            reason="operator cancelled before readiness",
        )
        self.assertEqual(result["status"], "failed")
        with connect(self.db) as connection:
            release = connection.execute(
                "SELECT status,failure_reason FROM evaluation_releases WHERE id=?",
                (releases.TARGET_RELEASE_ID,),
            ).fetchone()
            valid = connection.execute(
                """
                SELECT COUNT(*) FROM evaluation_versions
                WHERE release_id=? AND invalidated_at IS NULL
                """,
                (releases.TARGET_RELEASE_ID,),
            ).fetchone()[0]
        self.assertEqual(release["status"], "failed")
        failure = json.loads(str(release["failure_reason"]))
        self.assertEqual(failure["reason"], "operator cancelled before readiness")
        self.assertIn("content_items", failure["protected_drift"])
        self.assertIn("content_items", result["protected_drift"])
        self.assertEqual(valid, 0)

    def test_rehashed_manifest_subset_is_rejected_against_source_selector(self) -> None:
        value = json.loads(self.manifest.read_text(encoding="utf-8"))
        value["inventory"] = value["inventory"][:-1]
        value["content_count"] = len(value["inventory"])
        value["content_high_water"] = value["inventory"][-1]["content_id"]
        value["inventory_sha256"] = hashlib.sha256(
            releases._canonical_json(value["inventory"]).encode("utf-8")
        ).hexdigest()
        tampered = self.root / "subset-manifest.json"
        tampered.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        file_sha = hashlib.sha256(tampered.read_bytes()).hexdigest()
        with self.assertRaisesRegex(
            releases.ReleaseV9Error, "not the exact source automatic selector"
        ):
            releases.create(
                db_path=self.db,
                manifest_path=tampered,
                manifest_sha256=file_sha,
            )

    def test_existing_receipt_cannot_be_reused_for_a_different_manifest_hash(self) -> None:
        self._create_and_backfill()
        self._verify_ready()
        value = json.loads(self.manifest.read_text(encoding="utf-8"))
        value["generated_at"] = "2026-08-15T23:59:59Z"
        alternate = self.root / "alternate-manifest.json"
        alternate.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        alternate_sha = hashlib.sha256(alternate.read_bytes()).hexdigest()
        with self.assertRaisesRegex(
            releases.ReleaseV9Error, "current manifest binding"
        ):
            releases.verify_ready(
                db_path=self.db,
                manifest_path=alternate,
                manifest_sha256=alternate_sha,
                receipt_path=self.receipt,
            )

    def test_rollback_rejects_new_content_even_before_any_report(self) -> None:
        self._create_and_backfill()
        receipt_sha = self._verify_ready()
        releases.activate(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            receipt_path=self.receipt,
            receipt_sha256=receipt_sha,
        )
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO content_items(
                    link_id,platform,platform_content_id,canonical_url,title,body,
                    content_type,imported_at,created_at,updated_at
                ) VALUES ('V90004','douyin','v9-4','https://example.test/v9/4',
                          'post activation','', 'video',?,?,?)
                """,
                (captured_at, captured_at, captured_at),
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseV9Error, "rollback-before-resume boundary"
        ):
            releases.rollback_before_resume(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
                receipt_path=self.receipt,
                receipt_sha256=receipt_sha,
                reason="unsafe after new content",
            )

    def test_manifest_and_receipt_require_external_hashes(self) -> None:
        with self.assertRaisesRegex(releases.ReleaseV9Error, "manifest file SHA"):
            releases.status(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256="0" * 64,
            )
        self._create_and_backfill()
        receipt_sha = self._verify_ready()
        self.assertNotEqual(receipt_sha, "0" * 64)
        with self.assertRaisesRegex(releases.ReleaseV9Error, "receipt file SHA"):
            releases.activate(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
                receipt_path=self.receipt,
                receipt_sha256="0" * 64,
            )

    def test_rollback_is_forbidden_after_v8_6_revision(self) -> None:
        self._create_and_backfill()
        receipt_sha = self._verify_ready()
        releases.activate(
            db_path=self.db,
            manifest_path=self.manifest,
            manifest_sha256=self.manifest_sha256,
            receipt_path=self.receipt,
            receipt_sha256=receipt_sha,
        )
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,created_at,updated_at
                ) VALUES ('v9-task','daily','v9','2026-08-04','2026-08-04',
                          'manual','succeeded',100,?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at
                ) VALUES ('v9-task',1,?,?,?,?,?,?,?)
                """,
                (
                    releases.TARGET_RELEASE_ID,
                    releases.REPORT_VERSION,
                    releases.TARGET_RULE_VERSION,
                    releases.TAXONOMY_VERSION,
                    str(self.root / "report.json"),
                    "a" * 64,
                    captured_at,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(releases.ReleaseV9Error, "rollback is forbidden"):
            releases.rollback_before_resume(
                db_path=self.db,
                manifest_path=self.manifest,
                manifest_sha256=self.manifest_sha256,
                receipt_path=self.receipt,
                receipt_sha256=receipt_sha,
                reason="too late",
            )

    def test_prepare_rejects_nonempty_wal(self) -> None:
        alternate = self.root / "alternate.json"
        wal = Path(f"{self.db}-wal")
        wal.write_bytes(b"not-empty")
        with self.assertRaisesRegex(releases.ReleaseV9Error, "WAL must be empty"):
            releases.prepare_manifest(db_path=self.db, manifest_path=alternate)

    def test_prepare_rejects_existing_v8_6_revision(self) -> None:
        alternate = self.root / "alternate.json"
        captured_at = now_utc()
        with connect(self.db) as connection:
            connection.execute(
                """
                INSERT INTO report_tasks(
                    id,task_type,name,period_start,period_end,creation_source,
                    task_status,progress,created_at,updated_at
                ) VALUES ('source-v8.6','daily','source','2026-08-04','2026-08-04',
                          'manual','succeeded',100,?,?)
                """,
                (captured_at, captured_at),
            )
            connection.execute(
                """
                INSERT INTO report_revisions(
                    task_id,revision,release_id,contract_version,rule_version,
                    taxonomy_version,report_json_path,report_sha256,created_at
                ) VALUES ('source-v8.6',1,?,?,?,?,?,?,?)
                """,
                (
                    releases.SOURCE_RELEASE_ID,
                    releases.REPORT_VERSION,
                    releases.SOURCE_RULE_VERSION,
                    releases.TAXONOMY_VERSION,
                    str(self.root / "source-report.json"),
                    "b" * 64,
                    captured_at,
                ),
            )
            connection.commit()
        checkpoint = sqlite3.connect(self.db)
        try:
            checkpoint.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            checkpoint.close()
        with self.assertRaisesRegex(releases.ReleaseV9Error, "v8.6 report revisions"):
            releases.prepare_manifest(db_path=self.db, manifest_path=alternate)

    def test_formal_cli_guard_requires_freeze_and_zero_writer_handles(self) -> None:
        lock = self.root / "operator-freeze.lock"
        with (
            patch.object(releases, "DEFAULT_DB", self.db),
            patch.object(releases, "DEFAULT_OPERATOR_FREEZE_LOCK", lock),
        ):
            with self.assertRaisesRegex(releases.ReleaseV9Error, "freeze lock"):
                releases.assert_cli_cutover_guard(
                    db_path=self.db,
                    freeze_lock=lock,
                    isolated_clone=False,
                )
            lock.write_text("frozen\n", encoding="utf-8")
            alternate_lock = self.root / "alternate-freeze.lock"
            alternate_lock.write_text("frozen\n", encoding="utf-8")
            with self.assertRaisesRegex(
                releases.ReleaseV9Error, "canonical operator freeze lock"
            ):
                releases.assert_cli_cutover_guard(
                    db_path=self.db,
                    freeze_lock=alternate_lock,
                    isolated_clone=False,
                )
            with (
                patch.object(
                    releases,
                    "_database_writer_handles",
                    return_value=[
                        {
                            "command": "python",
                            "pid": 123,
                            "descriptor": "4u",
                            "path": str(self.db),
                        }
                    ],
                ),
                self.assertRaisesRegex(releases.ReleaseV9Error, "writer handles"),
            ):
                releases.assert_cli_cutover_guard(
                    db_path=self.db,
                    freeze_lock=lock,
                    isolated_clone=False,
                )
            with patch.object(
                releases, "_database_writer_handles", return_value=[]
            ):
                releases.assert_cli_cutover_guard(
                    db_path=self.db,
                    freeze_lock=lock,
                    isolated_clone=False,
                )
            with self.assertRaisesRegex(
                releases.ReleaseV9Error, "cannot be marked as an isolated clone"
            ):
                releases.assert_cli_cutover_guard(
                    db_path=self.db,
                    freeze_lock=lock,
                    isolated_clone=True,
                )

    def test_formal_cli_guard_rejects_existing_alias_as_formal(self) -> None:
        alias = self.root / "apfs-firmlink-spelling.sqlite3"
        alias.hardlink_to(self.db)
        with (
            patch.object(releases, "DEFAULT_DB", self.db),
            self.assertRaisesRegex(
                releases.ReleaseV9Error,
                "formal database cannot be marked as an isolated clone",
            ),
        ):
            releases.assert_cli_cutover_guard(
                db_path=alias,
                freeze_lock=self.root / "unused.lock",
                isolated_clone=True,
            )

    def test_writer_handle_parser_rejects_macos_lock_suffix(self) -> None:
        completed = type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": (
                    "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\n"
                    f"python 123 mark 4uW REG 1,2 10 20 {self.db}\n"
                ),
                "stderr": "",
            },
        )()
        with patch.object(releases.subprocess, "run", return_value=completed):
            handles = releases._database_writer_handles(self.db)
        self.assertEqual(len(handles), 1)
        self.assertEqual(handles[0]["descriptor"], "4uW")

    def test_isolated_clone_guard_rejects_formal_data_directory(self) -> None:
        clone = self.root / "clone.sqlite3"
        clone.write_bytes(b"clone")
        with (
            patch.object(releases, "DEFAULT_DB", self.db),
            self.assertRaisesRegex(releases.ReleaseV9Error, "formal app/data"),
        ):
            releases.assert_cli_cutover_guard(
                db_path=clone,
                freeze_lock=self.root / "unused.lock",
                isolated_clone=True,
            )


if __name__ == "__main__":
    unittest.main()
