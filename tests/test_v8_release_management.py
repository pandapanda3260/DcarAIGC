from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from typing import Any
from unittest.mock import patch

from v8.evaluation import evaluate_content
from v8.matcher_dsl import POINT_IDS, POINT_SCENES
from v8.operations import normalize_unknown_content_directions
from v8 import release_management as releases
from v8.storage import (
    LEGACY_V7_RELEASE_ID,
    connect,
    ensure_legacy_evaluation_release,
    initialize_database,
    now_utc,
)
from v8.taxonomy_rule_backfill import backfill_v5_1_matcher_rules


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class ReleaseManagementTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.production = self.root / "production.sqlite3"
        self.freeze_lock = self.root / "operator-freeze.lock"
        self.freeze_lock.write_text("frozen\n", encoding="utf-8")
        self._create_legacy_database(self.production)
        self.manifest = self._create_manifest(self.production)
        self.rehearsal_one = self.root / "rehearsal-one.sqlite3"
        self.rehearsal_two = self.root / "rehearsal-two.sqlite3"
        shutil.copy2(self.root / "frozen.sqlite3", self.rehearsal_one)
        shutil.copy2(self.root / "frozen.sqlite3", self.rehearsal_two)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _create_legacy_database(
        self, database: Path, *, content_count: int = 2
    ) -> None:
        with connect(database) as connection:
            initialize_database(connection)
            captured_at = now_utc()
            connection.execute(
                """
                INSERT INTO taxonomy_versions(
                    id,version,status,definition,created_at,published_at
                ) VALUES ('taxonomy-v5','selling-points-v5.0','published',
                          'legacy',?,?)
                """,
                (captured_at, captured_at),
            )
            for code in sorted(POINT_IDS):
                point = connection.execute(
                    """
                    INSERT INTO selling_points(
                        taxonomy_id,code,tier,label,definition,matcher_rule_json
                    ) VALUES ('taxonomy-v5',?, 'other',?,?,'{}')
                    """,
                    (code, f"卖点 {code}", f"定义 {code}"),
                )
                for scene in sorted(POINT_SCENES[code]):
                    connection.execute(
                        """
                        INSERT INTO selling_point_scenes(selling_point_id,scene)
                        VALUES (?,?)
                        """,
                        (point.lastrowid, scene),
                    )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v6",
                taxonomy_version="selling-points-v5.0",
            )
            ensure_legacy_evaluation_release(
                connection,
                rule_version="evaluation-v7",
                taxonomy_version="selling-points-v5.0",
            )
            for index in range(1, content_count + 1):
                connection.execute(
                    """
                    INSERT INTO content_items(
                        link_id,platform,platform_content_id,canonical_url,
                        raw_account_uid,raw_account_name,title,body,content_type,
                        imported_at,created_at,updated_at
                    ) VALUES (?, 'douyin',?,?, 'uid','测试账号','普通汽车内容',
                              '没有完整媒体证据','video',?,?,?)
                    """,
                    (
                        f"R{index:05d}",
                        f"video-{index}",
                        f"https://example.test/{index}",
                        captured_at,
                        captured_at,
                        captured_at,
                    ),
                )
            connection.commit()
        evaluate_content(1, db_path=database)

    def _create_manifest(self, database: Path, *, prefix: str = "") -> Path:
        inventory_rows: list[dict[str, object]] = []
        states: Counter[str] = Counter()
        with connect(database) as connection:
            content_ids = [
                int(row[0])
                for row in connection.execute(
                    "SELECT id FROM content_items ORDER BY id"
                )
            ]
            for content_id in content_ids:
                freeze_components, freeze_sha, _, current_sha = (
                    releases._freeze_v1_and_current_evidence_state(
                        connection, content_id
                    )
                )
                envelopes = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT evidence_sha256 FROM evidence_envelopes WHERE content_id=?",
                        (content_id,),
                    )
                }
                state = (
                    "exact"
                    if freeze_sha in envelopes
                    else "stale"
                    if envelopes
                    else "absent"
                )
                states[state] += 1
                self.assertNotEqual(freeze_sha, current_sha)
                inventory_rows.append(
                    {
                        "content_id": content_id,
                        "current_evidence_sha256": freeze_sha,
                        "envelope_state": state,
                        "components": freeze_components,
                    }
                )
            tables = [
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name
                    """
                )
            ]
            table_snapshot: dict[str, dict[str, object]] = {}
            for table in tables:
                columns = tuple(
                    str(row[1])
                    for row in connection.execute(f'PRAGMA table_info("{table}")')
                )
                count, rows_sha256 = releases._rows_sha256(
                    connection,
                    table=table,
                    columns=columns,
                )
                table_snapshot[table] = {
                    "count": count,
                    "rows_sha256": rows_sha256,
                }
            frozen_name = f"{prefix}frozen.sqlite3"
            backup = sqlite3.connect(self.root / frozen_name)
            try:
                connection.backup(backup)
            finally:
                backup.close()
        inventory_path = self.root / f"{prefix}content_evidence.jsonl"
        inventory_path.write_text(
            "".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
                for row in inventory_rows
            ),
            encoding="utf-8",
        )
        artifact_path = self.root / f"{prefix}evidence_artifacts.jsonl"
        artifact_path.write_bytes(b"")
        frozen_database = self.root / frozen_name
        logical_sha = _sha256(frozen_database)
        manifest = {
            "schema_version": releases.FREEZE_SCHEMA_VERSION,
            "source_database": str(database.resolve()),
            "freeze_lock": str(self.freeze_lock.resolve()),
            "logical_snapshot_sha256": logical_sha,
            "database_backup": {
                "path": frozen_database.name,
                "sha256": _sha256(frozen_database),
            },
            "content_evidence_inventory": {
                "path": inventory_path.name,
                "sha256": _sha256(inventory_path),
                "row_count": len(content_ids),
                "min_content_id": min(content_ids),
                "max_content_id": max(content_ids),
                "envelope_states": dict(sorted(states.items())),
            },
            "evidence_artifact_inventory": {
                "path": artifact_path.name,
                "sha256": _sha256(artifact_path),
                "row_count": 0,
                "unique_local_paths": 0,
                "integrity_states": {},
            },
            "table_snapshot": table_snapshot,
        }
        path = self.root / f"{prefix}manifest.json"
        path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return path

    def _prepare_release(self, database: Path, receipt: Path) -> dict[str, object]:
        backfill_v5_1_matcher_rules(db_path=database)
        first = releases.create(db_path=database, manifest_path=self.manifest)
        second = releases.create(db_path=database, manifest_path=self.manifest)
        self.assertEqual(first["release"]["status"], "draft")
        self.assertEqual(second["release"]["status"], "draft")
        releases.backfill(db_path=database, manifest_path=self.manifest)
        result = releases.verify_ready(
            db_path=database,
            manifest_path=self.manifest,
            receipt_path=receipt,
        )
        self.assertEqual(result["status"], "ready")
        return result

    def test_two_rehearsals_production_activation_and_rollback(self) -> None:
        receipt_one = self.root / "rehearsal-one.json"
        receipt_two = self.root / "rehearsal-two.json"
        first = self._prepare_release(self.rehearsal_one, receipt_one)
        second = self._prepare_release(self.rehearsal_two, receipt_two)
        self.assertEqual(first["core"], second["core"])
        execution_one = json.loads(receipt_one.read_text())["execution"]
        execution_two = json.loads(receipt_two.read_text())["execution"]
        self.assertNotEqual(
            execution_one["rehearsal_run_id"], execution_two["rehearsal_run_id"]
        )
        self.assertNotEqual(
            execution_one["target_db_pre_hash"], execution_two["target_db_pre_hash"]
        )

        backfill_v5_1_matcher_rules(db_path=self.production)
        releases.create(db_path=self.production, manifest_path=self.manifest)
        releases.backfill(db_path=self.production, manifest_path=self.manifest)
        production_receipt = self.root / "production.json"
        releases.verify_ready(
            db_path=self.production,
            manifest_path=self.manifest,
            receipt_path=production_receipt,
            rehearsal_receipt_paths=(receipt_one, receipt_two),
            production=True,
        )
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "production receipt"
        ):
            releases.activate(
                db_path=self.production,
                manifest_path=self.manifest,
                receipt_path=receipt_one,
            )

        def fail_after_target_activation(name: str) -> None:
            if name == "activation_target_release_active":
                raise RuntimeError("injected activation failure")

        with (
            patch.object(
                releases, "_checkpoint", side_effect=fail_after_target_activation
            ),
            self.assertRaisesRegex(RuntimeError, "injected activation failure"),
        ):
            releases.activate(
                db_path=self.production,
                manifest_path=self.manifest,
                receipt_path=production_receipt,
            )
        with connect(self.production) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                "ready",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (LEGACY_V7_RELEASE_ID,),
                ).fetchone()[0],
                "active",
            )
            legacy = connection.execute(
                """
                SELECT id,payload_json FROM evaluation_versions
                WHERE release_id=? ORDER BY id LIMIT 1
                """,
                (LEGACY_V7_RELEASE_ID,),
            ).fetchone()
            self.assertIsNotNone(legacy)
            legacy_id = int(legacy["id"])
            legacy_payload = str(legacy["payload_json"])
            connection.execute(
                "UPDATE evaluation_versions SET payload_json='{}' WHERE id=?",
                (legacy_id,),
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "evaluation_versions rows changed"
        ):
            releases.activate(
                db_path=self.production,
                manifest_path=self.manifest,
                receipt_path=production_receipt,
            )
        with connect(self.production) as connection:
            connection.execute(
                "UPDATE evaluation_versions SET payload_json=? WHERE id=?",
                (legacy_payload, legacy_id),
            )
            connection.commit()

        activated = releases.activate(
            db_path=self.production,
            manifest_path=self.manifest,
            receipt_path=production_receipt,
        )
        self.assertEqual(activated["release"]["status"], "active")
        idempotent = releases.activate(
            db_path=self.production,
            manifest_path=self.manifest,
            receipt_path=production_receipt,
        )
        self.assertEqual(idempotent["release"]["status"], "active")

        rolled_back = releases.rollback_before_resume(
            db_path=self.production,
            manifest_path=self.manifest,
            receipt_path=production_receipt,
            reason="operator rollback test",
        )
        self.assertEqual(rolled_back["release"]["status"], "retired")
        with connect(self.production) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (LEGACY_V7_RELEASE_ID,),
                ).fetchone()[0],
                "active",
            )
            self.assertEqual(
                [
                    row[0]
                    for row in connection.execute(
                        "SELECT evaluation_content_direction FROM content_items ORDER BY id"
                    )
                ],
                ["unknown", "unknown"],
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_versions
                    WHERE release_id=? AND invalidated_at IS NULL
                    """,
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                0,
            )

    def test_failed_batch_rolls_back_and_resume_succeeds(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.rehearsal_one)
        releases.create(db_path=self.rehearsal_one, manifest_path=self.manifest)
        original = releases._evaluate_content
        calls = 0

        def fail_second(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("injected batch failure")
            return original(*args, **kwargs)

        with patch.object(releases, "_evaluate_content", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "injected batch failure"):
                releases.backfill(
                    db_path=self.rehearsal_one, manifest_path=self.manifest
                )
        with connect(self.rehearsal_one) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE release_id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                "backfilling",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM migration_audit ORDER BY started_at DESC LIMIT 1"
                ).fetchone()[0],
                "failed",
            )
        resumed = releases.backfill(
            db_path=self.rehearsal_one, manifest_path=self.manifest
        )
        self.assertEqual(resumed["created"], 2)

    def test_later_batch_failure_preserves_first_batch_and_resumes(self) -> None:
        source = self.root / "multi-source.sqlite3"
        target = self.root / "multi-target.sqlite3"
        self._create_legacy_database(source, content_count=251)
        manifest = self._create_manifest(source, prefix="multi-")
        shutil.copy2(self.root / "multi-frozen.sqlite3", target)
        backfill_v5_1_matcher_rules(db_path=target)
        releases.create(db_path=target, manifest_path=manifest)
        original = releases._evaluate_content

        def fail_last(content_id: int, **kwargs: Any) -> Any:
            if content_id == 251:
                raise RuntimeError("injected second-batch failure")
            return original(content_id, **kwargs)

        with (
            patch.object(releases, "_evaluate_content", side_effect=fail_last),
            self.assertRaisesRegex(RuntimeError, "second-batch failure"),
        ):
            releases.backfill(db_path=target, manifest_path=manifest)
        with connect(target) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE release_id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                250,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                "backfilling",
            )
        resumed = releases.backfill(db_path=target, manifest_path=manifest)
        self.assertEqual(resumed["created"], 1)
        self.assertEqual(resumed["reused"], 250)
        with connect(target) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM evaluation_versions WHERE release_id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                251,
            )

    def test_manifest_preserves_absent_and_stale_and_fixed_batches(self) -> None:
        manifest = releases._load_freeze_manifest(self.manifest)
        self.assertEqual(
            [item.envelope_state for item in manifest.contents], ["stale", "absent"]
        )
        items = tuple(
            releases.FrozenContent(
                content_id=index + 1,
                evidence_sha256="a" * 64,
                components={key: None for key in releases.EVIDENCE_COMPONENT_KEYS},
                envelope_state="absent",
            )
            for index in range(501)
        )
        self.assertEqual(
            [len(batch) for batch in releases._batched(items)], [250, 250, 1]
        )

    def test_abort_is_idempotent_and_preserves_append_only_history(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.rehearsal_one)
        releases.create(db_path=self.rehearsal_one, manifest_path=self.manifest)
        releases.backfill(db_path=self.rehearsal_one, manifest_path=self.manifest)
        with connect(self.rehearsal_one) as connection:
            matches_before = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_matches m
                    JOIN evaluation_versions e ON e.id=m.evaluation_id
                    WHERE e.release_id=?
                    """,
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0]
            )
        first = releases.abort(
            db_path=self.rehearsal_one,
            manifest_path=self.manifest,
            reason="operator abandoned rehearsal",
        )
        second = releases.abort(
            db_path=self.rehearsal_one,
            manifest_path=self.manifest,
            reason="operator abandoned rehearsal",
        )
        self.assertEqual(first["release"]["status"], "failed")
        self.assertEqual(second["release"]["status"], "failed")
        with connect(self.rehearsal_one) as connection:
            total, valid = connection.execute(
                """
                SELECT COUNT(*),SUM(invalidated_at IS NULL)
                FROM evaluation_versions WHERE release_id=?
                """,
                (releases.TARGET_RELEASE_ID,),
            ).fetchone()
            self.assertEqual((total, valid), (2, 0))
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM evaluation_matches m
                    JOIN evaluation_versions e ON e.id=m.evaluation_id
                    WHERE e.release_id=?
                    """,
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                matches_before,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (LEGACY_V7_RELEASE_ID,),
                ).fetchone()[0],
                "active",
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM migration_audit ORDER BY started_at DESC LIMIT 1"
                ).fetchone()[0],
                "failed",
            )

    def test_ready_rejects_envelope_and_status_corruption(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.rehearsal_one)
        releases.create(db_path=self.rehearsal_one, manifest_path=self.manifest)
        releases.backfill(db_path=self.rehearsal_one, manifest_path=self.manifest)
        with connect(self.rehearsal_one) as connection:
            row = connection.execute(
                """
                SELECT e.id,e.payload_json,e.evidence_envelope_id,env.text_sha256
                FROM evaluation_versions e
                JOIN evidence_envelopes env ON env.id=e.evidence_envelope_id
                WHERE e.release_id=? ORDER BY e.content_id DESC LIMIT 1
                """,
                (releases.TARGET_RELEASE_ID,),
            ).fetchone()
            self.assertIsNotNone(row)
            evaluation_id = int(row["id"])
            envelope_id = int(row["evidence_envelope_id"])
            original_text_sha = str(row["text_sha256"])
            original_payload = str(row["payload_json"])
            connection.execute(
                "UPDATE evidence_envelopes SET text_sha256=? WHERE id=?",
                ("f" * 64, envelope_id),
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "evidence envelope"
        ):
            releases.verify_ready(
                db_path=self.rehearsal_one,
                manifest_path=self.manifest,
                receipt_path=self.root / "corrupt-envelope.json",
            )
        with connect(self.rehearsal_one) as connection:
            connection.execute(
                "UPDATE evidence_envelopes SET text_sha256=? WHERE id=?",
                (original_text_sha, envelope_id),
            )
            payload = json.loads(original_payload)
            payload["evaluation_status"] = "evaluated"
            connection.execute(
                """
                UPDATE evaluation_versions
                SET evaluation_status='evaluated',payload_json=? WHERE id=?
                """,
                (
                    json.dumps(
                        payload,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    evaluation_id,
                ),
            )
            connection.commit()
        with self.assertRaisesRegex(releases.ReleaseManagementError, "inclusion gate"):
            releases.verify_ready(
                db_path=self.rehearsal_one,
                manifest_path=self.manifest,
                receipt_path=self.root / "corrupt-status.json",
            )

    def test_fake_second_rehearsal_receipt_cannot_pass_production_gate(self) -> None:
        receipt_one = self.root / "real-rehearsal.json"
        self._prepare_release(self.rehearsal_one, receipt_one)
        fake_receipt = self.root / "fake-rehearsal.json"
        payload = json.loads(receipt_one.read_text(encoding="utf-8"))
        payload["execution"].update(
            {
                "rehearsal_run_id": "fake-independent-run",
                "target_database": str(self.rehearsal_two.resolve()),
                "target_db_pre_hash": "f" * 64,
            }
        )
        payload["execution_sha256"] = hashlib.sha256(
            releases.canonical_json(payload["execution"]).encode("utf-8")
        ).hexdigest()
        fake_receipt.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        backfill_v5_1_matcher_rules(db_path=self.production)
        releases.create(db_path=self.production, manifest_path=self.manifest)
        releases.backfill(db_path=self.production, manifest_path=self.manifest)
        with self.assertRaisesRegex(releases.ReleaseManagementError, "audit"):
            releases.verify_ready(
                db_path=self.production,
                manifest_path=self.manifest,
                receipt_path=self.root / "must-not-exist.json",
                rehearsal_receipt_paths=(receipt_one, fake_receipt),
                production=True,
            )
        with connect(self.production) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                "backfilling",
            )

    def test_freeze_anchors_prevent_pre_audit_business_and_legacy_drift(self) -> None:
        backfill_v5_1_matcher_rules(db_path=self.rehearsal_one)
        with connect(self.rehearsal_one) as connection:
            connection.execute(
                "UPDATE content_items SET canonical_url=? WHERE id=1",
                ("https://tampered.example/content",),
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "content_items rows changed"
        ):
            releases.create(db_path=self.rehearsal_one, manifest_path=self.manifest)

        backfill_v5_1_matcher_rules(db_path=self.rehearsal_two)
        with connect(self.rehearsal_two) as connection:
            connection.execute(
                """
                UPDATE evaluation_versions SET content_direction='new_car'
                WHERE id=(
                    SELECT id FROM evaluation_versions
                    WHERE release_id=? ORDER BY id LIMIT 1
                )
                """,
                (LEGACY_V7_RELEASE_ID,),
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "evaluation_versions rows changed"
        ):
            releases.create(db_path=self.rehearsal_two, manifest_path=self.manifest)

    def test_unknown_direction_normalization_is_semantically_stable(self) -> None:
        with connect(self.production) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='unknown' WHERE id=1"
            )
            connection.commit()
        manifest = self._create_manifest(self.production, prefix="direction-")
        frozen = self.root / "direction-frozen.sqlite3"

        normalized = self.root / "direction-normalized.sqlite3"
        shutil.copy2(frozen, normalized)
        backfill_v5_1_matcher_rules(db_path=normalized)
        releases.create(db_path=normalized, manifest_path=manifest)
        releases.backfill(db_path=normalized, manifest_path=manifest)
        with connect(normalized) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction=NULL WHERE id=1"
            )
            connection.commit()
        receipt = self.root / "direction-normalized-receipt.json"
        ready = releases.verify_ready(
            db_path=normalized,
            manifest_path=manifest,
            receipt_path=receipt,
        )
        self.assertEqual(ready["status"], "ready")
        payload = json.loads(receipt.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], "dcar-evaluation-release-ready-v2")
        with connect(normalized) as connection:
            summary = json.loads(
                connection.execute(
                    "SELECT summary_json FROM migration_audit ORDER BY id DESC LIMIT 1"
                ).fetchone()[0]
            )
        self.assertEqual(
            summary["protected_state"]["contract"], "backfill-protected-v2"
        )
        self.assertEqual(
            summary["activation_stable_state"]["contract"],
            "activation-stable-v2",
        )

        tampered = self.root / "direction-tampered.sqlite3"
        shutil.copy2(frozen, tampered)
        backfill_v5_1_matcher_rules(db_path=tampered)
        with connect(tampered) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='new_car' WHERE id=1"
            )
            connection.commit()
        with self.assertRaisesRegex(
            releases.ReleaseManagementError, "content_items rows changed"
        ):
            releases.create(db_path=tampered, manifest_path=manifest)

    def test_unknown_normalization_survives_activation_idempotency_and_rollback(
        self,
    ) -> None:
        with connect(self.production) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='unknown' WHERE id=1"
            )
            connection.commit()
        manifest = self._create_manifest(self.production, prefix="direction-cycle-")
        frozen = self.root / "direction-cycle-frozen.sqlite3"
        databases = [
            self.root / "direction-cycle-rehearsal-a.sqlite3",
            self.root / "direction-cycle-rehearsal-b.sqlite3",
            self.production,
        ]
        receipts = [
            self.root / "direction-cycle-rehearsal-a.json",
            self.root / "direction-cycle-rehearsal-b.json",
            self.root / "direction-cycle-production.json",
        ]
        for database in databases:
            if database != self.production:
                shutil.copy2(frozen, database)
            backfill_v5_1_matcher_rules(db_path=database)
            releases.create(db_path=database, manifest_path=manifest)
            releases.backfill(db_path=database, manifest_path=manifest)
            normalized = normalize_unknown_content_directions(db_path=database)
            self.assertEqual(normalized["updated_rows"], 1)

        for database, receipt in zip(databases[:2], receipts[:2], strict=True):
            ready = releases.verify_ready(
                db_path=database,
                manifest_path=manifest,
                receipt_path=receipt,
            )
            self.assertEqual(ready["status"], "ready")
        production_ready = releases.verify_ready(
            db_path=databases[2],
            manifest_path=manifest,
            receipt_path=receipts[2],
            rehearsal_receipt_paths=(receipts[0], receipts[1]),
            production=True,
        )
        self.assertEqual(production_ready["status"], "ready")

        activated = releases.activate(
            db_path=databases[2],
            manifest_path=manifest,
            receipt_path=receipts[2],
        )
        repeated = releases.activate(
            db_path=databases[2],
            manifest_path=manifest,
            receipt_path=receipts[2],
        )
        self.assertEqual(activated["release"]["status"], "active")
        self.assertEqual(repeated["release"]["status"], "active")
        with connect(databases[2]) as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT manual_content_direction FROM content_items WHERE id=1"
                ).fetchone()[0]
            )

        rolled_back = releases.rollback_before_resume(
            db_path=databases[2],
            manifest_path=manifest,
            receipt_path=receipts[2],
            reason="normalized direction rollback test",
        )
        self.assertEqual(rolled_back["release"]["status"], "retired")
        with connect(databases[2]) as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT manual_content_direction FROM content_items WHERE id=1"
                ).fetchone()[0]
            )
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (LEGACY_V7_RELEASE_ID,),
                ).fetchone()[0],
                "active",
            )

    def test_ready_rejects_real_direction_change_after_backfill(self) -> None:
        with connect(self.production) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='unknown' WHERE id=1"
            )
            connection.commit()
        manifest = self._create_manifest(self.production, prefix="direction-late-")
        database = self.root / "direction-late-change.sqlite3"
        shutil.copy2(self.root / "direction-late-frozen.sqlite3", database)
        backfill_v5_1_matcher_rules(db_path=database)
        releases.create(db_path=database, manifest_path=manifest)
        releases.backfill(db_path=database, manifest_path=manifest)
        with connect(database) as connection:
            connection.execute(
                "UPDATE content_items SET manual_content_direction='new_car' WHERE id=1"
            )
            connection.commit()

        with self.assertRaisesRegex(
            releases.ReleaseManagementError,
            "frozen content_items rows changed after freeze",
        ):
            releases.verify_ready(
                db_path=database,
                manifest_path=manifest,
                receipt_path=self.root / "direction-late-receipt.json",
            )

    def test_production_gate_binds_complete_target_taxonomy_semantics(self) -> None:
        receipt_one = self.root / "taxonomy-rehearsal-one.json"
        receipt_two = self.root / "taxonomy-rehearsal-two.json"
        self._prepare_release(self.rehearsal_one, receipt_one)
        self._prepare_release(self.rehearsal_two, receipt_two)
        backfill_v5_1_matcher_rules(db_path=self.production)
        with connect(self.production) as connection:
            connection.execute(
                """
                UPDATE selling_points SET label='被篡改标签'
                WHERE taxonomy_id=(
                    SELECT id FROM taxonomy_versions WHERE version=?
                ) AND code='E1'
                """,
                (releases.TARGET_TAXONOMY_VERSION,),
            )
            connection.commit()
        releases.create(db_path=self.production, manifest_path=self.manifest)
        releases.backfill(db_path=self.production, manifest_path=self.manifest)
        with self.assertRaisesRegex(releases.ReleaseManagementError, "semantic core"):
            releases.verify_ready(
                db_path=self.production,
                manifest_path=self.manifest,
                receipt_path=self.root / "taxonomy-production.json",
                rehearsal_receipt_paths=(receipt_one, receipt_two),
                production=True,
            )
        with connect(self.production) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT status FROM evaluation_releases WHERE id=?",
                    (releases.TARGET_RELEASE_ID,),
                ).fetchone()[0],
                "backfilling",
            )


if __name__ == "__main__":
    unittest.main()
