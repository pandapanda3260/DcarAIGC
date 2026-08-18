from __future__ import annotations

import io
import hashlib
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import import_dongchedi_spu_trims as cli
from v8 import spu_catalog_import as importer
from v8.spu_audience import _load_assets, ensure_assets, resolve_trim
from v8.storage import connect, initialize_database


class SpuCatalogImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "candidate.sqlite3"
        self.input_path = self.root / "normalized.json"
        self.mapping_path = self.root / "mapping.json"
        self.backups = self.root / "backups"
        self.mapping_path.write_text(
            json.dumps(
                {
                    "mapping_version": "test-v1",
                    "entries": {
                        "toyota__camry": {
                            "official_series": [
                                {
                                    "series_id": 535,
                                    "include_recent": True,
                                }
                            ]
                        }
                    },
                    "unresolved": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        with connect(self.database) as connection:
            initialize_database(connection)
            ensure_assets(connection)
            connection.commit()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _row(
        *,
        car_id: str = "250001",
        series_slug: str = "toyota__camry",
        dcd_series_id: str = "535",
        trim_label: str = "2025款 双擎 2.0HG 尊贵版",
        model_year: int = 2025,
        price_low: float | None = 19.68,
        price_high: float | None = 19.68,
        aliases: list[object] | None = None,
        brand: str = "丰田",
        series: str = "凯美瑞",
    ) -> dict[str, object]:
        return {
            "series_slug": series_slug,
            "dcd_series_id": dcd_series_id,
            "dcd_series_name": series,
            "car_id": car_id,
            "trim_label": trim_label,
            "model_year": model_year,
            "powertrain": "hev",
            "body_style": "轿车",
            "brand": brand,
            "series": series,
            "price_low": price_low,
            "price_high": price_high,
            "aliases": aliases
            if aliases is not None
            else [
                {
                    "alias": trim_label,
                    "alias_type": "official",
                    "ambiguous": False,
                },
                "双擎 2.0HG 尊贵版",
            ],
            "source_bucket": "online",
        }

    def _write(self, rows: list[dict[str, object]]) -> Path:
        pairs: dict[tuple[str, int], dict[str, int]] = {}
        for row in rows:
            pair = (str(row["series_slug"]), int(row["dcd_series_id"]))
            counts = pairs.setdefault(pair, {"online": 0, "offline": 0})
            counts[str(row["source_bucket"])] += 1
        mapping_entries: dict[str, dict[str, object]] = {}
        for slug, series_id in sorted(pairs):
            entry = mapping_entries.setdefault(slug, {"official_series": []})
            entry["official_series"].append(
                {"series_id": series_id, "include_recent": True}
            )
        self.mapping_path.write_text(
            json.dumps(
                {
                    "mapping_version": "test-v1",
                    "entries": mapping_entries,
                    "unresolved": [],
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )
        row_slugs = {str(row["series_slug"]) for row in rows}
        online_rows = sum(row["source_bucket"] == "online" for row in rows)
        offline_rows = len(rows) - online_rows
        payload = {
            "schema": importer.SOURCE_ARTIFACT_SCHEMA,
            "schema_version": importer.INPUT_SCHEMA_VERSION,
            "generated_at": "2026-08-16T00:00:00Z",
            "source": {
                "provider": importer.SOURCE_PROVIDER,
                "endpoint": importer.SOURCE_ENDPOINT,
                "mapping_version": "test-v1",
                "mapping_sha256": hashlib.sha256(
                    self.mapping_path.read_bytes()
                ).hexdigest(),
                "city_name": "北京",
                "buckets_included": ["online", "offline"],
                "buckets_excluded": ["presale_car", "urban"],
                "online_policy": "all_through_max_model_year",
                "offline_min_model_year": 2024,
                "max_model_year": 2026,
            },
            "summary": {
                "configured_series_slugs": len(row_slugs),
                "resolved_series_slugs": len(row_slugs),
                "unresolved_series_slugs": 0,
                "official_series_requests": len(pairs),
                "rows": len(rows),
                "online_rows": online_rows,
                "offline_rows": offline_rows,
            },
            "unresolved": [],
            "series_summaries": [
                {
                    "series_slug": slug,
                    "dcd_series_id": series_id,
                    "dcd_series_name": "测试车系",
                    "url": f"https://www.dongchedi.com/auto/series/{series_id}",
                    "response_sha256": "a" * 64,
                    "online_seen": counts["online"],
                    "online_selected": counts["online"],
                    "offline_seen": counts["offline"],
                    "offline_selected": counts["offline"],
                }
                for (slug, series_id), counts in sorted(pairs.items())
            ],
            "rows": rows,
        }
        payload["catalog_sha256"] = hashlib.sha256(
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        self.input_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )
        return self.input_path

    def _dry_run(self) -> dict[str, object]:
        return importer.execute_import(
            self.input_path,
            db_path=self.database,
            mapping_path=self.mapping_path,
        )

    def _read_payload(self) -> dict[str, object]:
        return json.loads(self.input_path.read_text(encoding="utf-8"))

    def _write_payload(self, payload: dict[str, object], *, rehash: bool) -> None:
        if rehash:
            payload.pop("catalog_sha256", None)
            payload["catalog_sha256"] = hashlib.sha256(
                json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
        self.input_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _apply(
        self,
        dry_run: dict[str, object],
        *,
        skip_backup: bool = True,
        receipt_path: Path | None = None,
    ) -> dict[str, object]:
        return importer.execute_import(
            self.input_path,
            db_path=self.database,
            apply=True,
            expected_plan_sha256=str(dry_run["plan_sha256"]),
            backup_dir=self.backups,
            skip_backup=skip_backup,
            receipt_path=receipt_path,
            mapping_path=self.mapping_path,
        )

    def test_dry_run_is_read_only_and_plans_stable_ids(self) -> None:
        self._write([self._row()])
        before_stat = self.database.stat()
        receipt = self._dry_run()
        after_stat = self.database.stat()

        self.assertFalse(receipt["applied"])
        self.assertEqual(receipt["mode"], "dry_run")
        self.assertEqual(receipt["operations"]["catalog_insert"], 1)
        self.assertEqual(receipt["operations"]["alias_insert"], 2)
        self.assertEqual(len(str(receipt["plan_sha256"])), 64)
        self.assertEqual(before_stat.st_size, after_stat.st_size)
        self.assertFalse(self.backups.exists())
        with importer._connect_read_only(self.database) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM spu_catalog WHERE is_series_node=0"
                ).fetchone()[0],
                0,
            )

    def test_source_attestation_rejects_tampering_and_metadata_drift(self) -> None:
        self._write([self._row()])
        payload = self._read_payload()
        payload["rows"][0]["trim_label"] = "2025款 被篡改"
        self._write_payload(payload, rehash=False)
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "catalog_sha256"):
            self._dry_run()

        self._write([self._row()])
        payload = self._read_payload()
        payload["summary"]["rows"] += 1
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "summary.rows"):
            self._dry_run()

        self._write([self._row()])
        payload = self._read_payload()
        payload["source"]["offline_min_model_year"] = 2023
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "window"):
            self._dry_run()

        self._write([self._row()])
        payload = self._read_payload()
        payload["unresolved"].append({"series_slug": "toyota__camry", "reason": "x"})
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "overlap"):
            self._dry_run()

    def test_approved_mapping_hash_and_series_whitelist_are_enforced(self) -> None:
        self._write([self._row()])
        original_mapping = self.mapping_path.read_text(encoding="utf-8")
        self.mapping_path.write_text(original_mapping + "\n", encoding="utf-8")
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "mapping_sha256"):
            importer.execute_import(
                self.input_path,
                db_path=self.database,
                mapping_path=self.mapping_path,
            )

        self.mapping_path.write_text(original_mapping, encoding="utf-8")
        self._write([self._row(dcd_series_id="4802")])
        self.mapping_path.write_text(original_mapping, encoding="utf-8")
        payload = self._read_payload()
        payload["source"]["mapping_sha256"] = hashlib.sha256(
            self.mapping_path.read_bytes()
        ).hexdigest()
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(
            importer.SpuCatalogImportError, "official-series requests"
        ):
            importer.execute_import(
                self.input_path,
                db_path=self.database,
                mapping_path=self.mapping_path,
            )

    def test_apply_takes_valid_online_backup_and_is_idempotent(self) -> None:
        self._write([self._row()])
        dry_run = self._dry_run()
        receipt_path = self.root / "receipts" / "import.json"
        applied = self._apply(
            dry_run,
            skip_backup=False,
            receipt_path=receipt_path,
        )

        self.assertTrue(applied["applied"])
        self.assertTrue(receipt_path.is_file())
        backup = applied["backup"]
        self.assertIsNotNone(backup)
        assert isinstance(backup, dict)
        backup_path = Path(str(backup["path"]))
        self.assertTrue(backup_path.is_file())
        self.assertEqual(backup["validation"]["quick_check"], "ok")
        self.assertEqual(backup["validation"]["foreign_key_violations"], 0)
        self.assertEqual(len(str(backup["sha256"])), 64)

        with importer._connect_read_only(self.database) as connection:
            row = connection.execute(
                """
                SELECT series_slug,trim_label,external_ref,enabled
                FROM spu_catalog WHERE spu_id='toyota__camry__dcd-250001'
                """
            ).fetchone()
            self.assertIsNotNone(row)
            assert row is not None
            self.assertEqual(row["series_slug"], "toyota__camry")
            self.assertEqual(row["external_ref"], "dongchedi:car:250001")
            self.assertEqual(row["enabled"], 1)

        second_dry_run = self._dry_run()
        self.assertEqual(second_dry_run["operations"]["total_changes"], 0)
        second_apply = self._apply(second_dry_run, skip_backup=False)
        self.assertFalse(second_apply["applied"])
        self.assertIsNone(second_apply["backup"])
        self.assertEqual(len(list(self.backups.glob("*.sqlite3"))), 1)

    def test_same_series_can_use_multiple_dongchedi_series_ids(self) -> None:
        rows = [
            self._row(car_id="250011", dcd_series_id="4802"),
            self._row(
                car_id="250012",
                dcd_series_id="4899",
                trim_label="2025款 EV 510KM 领先型",
                aliases=["EV 510KM 领先型"],
                price_low=14.98,
                price_high=14.98,
            ),
        ]
        self._write(rows)
        receipt = self._dry_run()
        self.assertEqual(receipt["operations"]["catalog_insert"], 2)

    def test_aliases_are_incremental_and_absence_never_deletes_manual_alias(
        self,
    ) -> None:
        self._write([self._row()])
        self._apply(self._dry_run())
        with connect(self.database) as connection:
            connection.execute(
                """
                INSERT INTO spu_alias(alias,alias_type,spu_scope,spu_id,ambiguous,enabled)
                VALUES ('人工补充称呼','nickname','trim',
                        'toyota__camry__dcd-250001',0,1)
                """
            )
            connection.commit()

        updated = self._row(
            price_low=20.18,
            price_high=20.18,
            aliases=["双擎 2.0HG 尊贵版"],
        )
        self._write([updated])
        dry_run = self._dry_run()
        self.assertEqual(dry_run["operations"]["catalog_update"], 1)
        self._apply(dry_run)

        with importer._connect_read_only(self.database) as connection:
            aliases = {
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT alias FROM spu_alias
                    WHERE spu_id='toyota__camry__dcd-250001'
                    """
                )
            }
            price = connection.execute(
                """
                SELECT price_low FROM spu_catalog
                WHERE spu_id='toyota__camry__dcd-250001'
                """
            ).fetchone()[0]
        self.assertIn("人工补充称呼", aliases)
        self.assertEqual(price, 20.18)

    def test_identity_conflicts_and_unknown_series_fail_closed(self) -> None:
        self._write([self._row(), self._row()])
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "duplicate car_id"):
            importer.load_frozen_catalog(self.input_path)

        self._write(
            [
                self._row(
                    series_slug="unknown__series",
                    brand="不存在",
                    series="不存在",
                )
            ]
        )
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "unknown existing"):
            self._dry_run()

    def test_external_ref_cannot_move_to_another_spu(self) -> None:
        self._write([self._row()])
        self._apply(self._dry_run())
        self._write(
            [
                self._row(
                    series_slug="toyota__corolla",
                    dcd_series_id="542",
                    brand="丰田",
                    series="卡罗拉",
                    trim_label="2025款 双擎 旗舰版",
                )
            ]
        )
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "external_ref"):
            self._dry_run()

    def test_running_association_and_plan_drift_block_apply(self) -> None:
        self._write([self._row()])
        with connect(self.database) as connection:
            connection.execute(
                """
                INSERT INTO spu_association_runs(started_at,status,rule_version)
                VALUES ('2026-08-16T00:00:00Z','running','test')
                """
            )
            connection.commit()
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "are running"):
            self._dry_run()
        with connect(self.database) as connection:
            connection.execute(
                """
                UPDATE spu_association_runs
                SET status='failed',finished_at='2026-08-16T00:00:01Z'
                WHERE status='running'
                """
            )
            connection.commit()

        dry_run = self._dry_run()
        with connect(self.database) as connection:
            connection.execute(
                """
                UPDATE spu_alias SET enabled=0
                WHERE spu_id='toyota__camry' AND alias='凯美瑞'
                """
            )
            connection.commit()
        with self.assertRaisesRegex(importer.SpuCatalogImportError, "plan hash"):
            self._apply(dry_run)
        with importer._connect_read_only(self.database) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM spu_catalog WHERE is_series_node=0"
                ).fetchone()[0],
                0,
            )

    def test_failed_batch_rolls_back_every_catalog_and_alias_write(self) -> None:
        self._write([self._row()])
        dry_run = self._dry_run()
        original_apply = importer._apply_plan

        def fail_after_writes(connection, plan, *, captured_at):
            original_apply(connection, plan, captured_at=captured_at)
            raise RuntimeError("injected failure")

        with patch.object(importer, "_apply_plan", side_effect=fail_after_writes):
            with self.assertRaisesRegex(RuntimeError, "injected failure"):
                self._apply(dry_run)
        with importer._connect_read_only(self.database) as connection:
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM spu_catalog WHERE is_series_node=0"
                ).fetchone()[0],
                0,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM spu_alias WHERE spu_scope='trim'"
                ).fetchone()[0],
                0,
            )

    def test_skip_backup_is_forbidden_when_target_is_default_database(self) -> None:
        self._write([self._row()])
        dry_run = self._dry_run()
        with patch.object(importer, "DEFAULT_DB", self.database), patch.dict(
            importer.os.environ,
            {"DCAR_TEST_DENY_FORMAL_DB": "0"},
        ):
            with self.assertRaisesRegex(importer.SpuCatalogImportError, "forbidden"):
                self._apply(dry_run, skip_backup=True)

    def test_skip_backup_is_forbidden_for_existing_default_database_alias(self) -> None:
        self._write([self._row()])
        formal = self.root / "formal.sqlite3"
        source = sqlite3.connect(self.database)
        destination = sqlite3.connect(formal)
        try:
            source.backup(destination)
        finally:
            destination.close()
            source.close()
        alias = self.root / "apfs-firmlink-spelling.sqlite3"
        alias.hardlink_to(formal)
        with (
            patch.object(importer, "DEFAULT_DB", formal),
            patch.dict(importer.os.environ, {"DCAR_TEST_DENY_FORMAL_DB": "0"}),
        ):
            dry_run = importer.execute_import(
                self.input_path,
                db_path=alias,
                mapping_path=self.mapping_path,
            )
            with self.assertRaisesRegex(importer.SpuCatalogImportError, "forbidden"):
                importer.execute_import(
                    self.input_path,
                    db_path=alias,
                    apply=True,
                    expected_plan_sha256=str(dry_run["plan_sha256"]),
                    backup_dir=self.backups,
                    skip_backup=True,
                    mapping_path=self.mapping_path,
                )

    def test_receipt_cannot_overwrite_input_database_or_sidecars(self) -> None:
        self._write([self._row()])
        protected = [
            self.input_path,
            self.database,
            *(
                Path(f"{self.database}{suffix}")
                for suffix in ("-wal", "-shm", "-journal")
            ),
        ]
        for destination in protected:
            with self.subTest(destination=destination):
                with self.assertRaisesRegex(
                    importer.SpuCatalogImportError, "must not overwrite"
                ):
                    importer.execute_import(
                        self.input_path,
                        db_path=self.database,
                        receipt_path=destination,
                        mapping_path=self.mapping_path,
                    )

    def test_imported_aliases_drive_unique_trim_resolution(self) -> None:
        rows = [
            self._row(
                car_id="250021",
                trim_label="2025款 双擎 2.0HG 尊贵版",
                aliases=["双擎 2.0HG 尊贵版", "尊贵版"],
                price_low=22.0,
                price_high=22.0,
            ),
            self._row(
                car_id="250022",
                trim_label="2025款 双擎 2.0HE 领先版",
                aliases=["双擎 2.0HE 领先版", "领先版"],
                price_low=17.98,
                price_high=17.98,
            ),
        ]
        self._write(rows)
        self._apply(self._dry_run())
        with importer._connect_read_only(self.database) as connection:
            assets = _load_assets(connection)
        resolved = resolve_trim(
            {"title": "2025款 凯美瑞双擎 2.0HG 尊贵版到店", "body": ""},
            "toyota__camry",
            assets,
        )
        self.assertEqual(resolved, "toyota__camry__dcd-250021")

        price_only = resolve_trim(
            {"title": "凯美瑞现在优惠后大约22万", "body": ""},
            "toyota__camry",
            assets,
        )
        self.assertIsNone(price_only)

        year_and_price = resolve_trim(
            {"title": "2025款凯美瑞现在优惠后大约22万", "body": ""},
            "toyota__camry",
            assets,
        )
        self.assertEqual(year_and_price, "toyota__camry__dcd-250021")

    def test_cli_defaults_to_dry_run_and_emits_json_receipt(self) -> None:
        self._write([self._row()])
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli.main(
                [
                    "--input",
                    str(self.input_path),
                    "--mapping",
                    str(self.mapping_path),
                    "--db",
                    str(self.database),
                ]
            )
        self.assertEqual((code, stderr.getvalue()), (0, ""))
        receipt = json.loads(stdout.getvalue())
        self.assertEqual(receipt["mode"], "dry_run")
        self.assertFalse(receipt["applied"])


if __name__ == "__main__":
    unittest.main()
