from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from scripts import import_dongchedi_spu_series as cli
from v8 import spu_series_catalog_import as importer
from v8.spu_audience import ensure_assets
from v8.storage import connect, initialize_database


class SpuSeriesCatalogImportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.database = self.root / "candidate.sqlite3"
        self.input_path = self.root / "official-series.json"
        self.backups = self.root / "backups"
        with connect(self.database) as connection:
            initialize_database(connection)
            ensure_assets(connection)
            connection.execute(
                """
                INSERT INTO spu_catalog(
                    spu_id,brand,series,series_slug,trim_label,is_series_node,
                    model_year,powertrain,body_style,price_low,price_high,
                    external_ref,enabled,created_at,updated_at
                ) VALUES (
                    'benz__c-class__test-trim','奔驰','奔驰C级','benz__c-class',
                    '2026款 C 260 L',0,2026,'ice','轿车',35.0,35.0,
                    'test:trim:1',1,'2026-08-16T00:00:00Z','2026-08-16T00:00:00Z'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO spu_alias(
                    alias,alias_type,spu_scope,spu_id,ambiguous,enabled
                ) VALUES ('C 260 L','official','trim','benz__c-class__test-trim',0,1)
                """
            )
            connection.commit()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def _row(
        *,
        dcd_brand_id: str = "1",
        brand: str = "奔驰",
        dcd_series_id: str = "197",
        series: str = "奔驰E级",
        body_style: str = "轿车",
        business_status: str | int = "on_sale",
        price_low: float | None = 44.72,
        price_high: float | None = 59.98,
        existing_series_slug: str | None = None,
        represented_ids: list[str] | None = None,
        aliases: list[object] | None = None,
    ) -> dict[str, object]:
        row: dict[str, object] = {
            "dcd_brand_id": dcd_brand_id,
            "brand": brand,
            "dcd_series_id": dcd_series_id,
            "series": series,
            "body_style": body_style,
            "business_status": business_status,
            "price": {"low": price_low, "high": price_high, "unit": "万元"},
            "external_ref": f"dongchedi:series:{dcd_series_id}",
            "aliases": aliases
            if aliases is not None
            else [
                {
                    "alias": series,
                    "alias_type": "official",
                    "ambiguous": False,
                    "enabled": True,
                },
                {
                    "alias": "E级",
                    "alias_type": "model_code",
                    "ambiguous": True,
                    "enabled": True,
                },
            ],
        }
        if existing_series_slug is not None:
            row["existing_series_slug"] = existing_series_slug
        if represented_ids is not None:
            row["represented_official_series_ids"] = represented_ids
        return row

    @classmethod
    def _existing_c_row(cls) -> dict[str, object]:
        return cls._row(
            dcd_series_id="195",
            series="奔驰C级",
            price_low=33.0,
            price_high=38.0,
            existing_series_slug="benz__c-class",
            represented_ids=["195", "1458", "6436"],
            aliases=[
                {
                    "alias": "奔驰C级",
                    "alias_type": "official",
                    "ambiguous": False,
                    "enabled": True,
                },
                {
                    "alias": "C级",
                    "alias_type": "official",
                    "ambiguous": True,
                    "enabled": True,
                },
            ],
        )

    def _write(self, rows: list[dict[str, object]]) -> Path:
        represented_count = sum(
            len(row.get("represented_official_series_ids", [row["dcd_series_id"]]))
            for row in rows
        )
        payload: dict[str, object] = {
            "schema": importer.SOURCE_ARTIFACT_SCHEMA,
            "schema_version": importer.INPUT_SCHEMA_VERSION,
            "generated_at": "2026-08-16T00:00:00Z",
            "source": {
                "provider": "dongchedi",
                "brand_endpoint": importer.BRAND_CATALOG_ENDPOINT,
                "series_endpoint": importer.BRAND_SERIES_ENDPOINT,
                "city_name": "北京",
                "included_business_statuses": [0, 2],
                "brands_requested": 645,
                "brand_response_sha256": "a" * 64,
                "series_requests": [
                    {
                        "dcd_brand_id": str(brand_id),
                        "response_sha256": hashlib.sha256(
                            f"brand:{brand_id}".encode("utf-8")
                        ).hexdigest(),
                    }
                    for brand_id in range(1, 646)
                ],
                "source_snapshot": "unit-test-fixture",
            },
            "summary": {
                "logical_import_rows": len(rows),
                "represented_official_series_id_count": represented_count,
                "logical_existing_series_rows": sum(
                    bool(row.get("existing_series_slug")) for row in rows
                ),
                "logical_new_series_rows": sum(
                    not bool(row.get("existing_series_slug")) for row in rows
                ),
                "selected_count": represented_count,
            },
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

    def _payload(self) -> dict[str, object]:
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

    def _dry_run(self) -> dict[str, object]:
        return importer.execute_import(self.input_path, db_path=self.database)

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
        )

    def _protected_state(self) -> tuple[list[tuple], list[tuple], list[tuple]]:
        with importer._connect_read_only(self.database) as connection:
            trims = [
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM spu_catalog WHERE is_series_node=0 ORDER BY spu_id"
                )
            ]
            trim_aliases = [
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT a.* FROM spu_alias a
                    JOIN spu_catalog c ON c.spu_id=a.spu_id
                    WHERE c.is_series_node=0 ORDER BY a.id
                    """
                )
            ]
            audiences = [
                tuple(row)
                for row in connection.execute(
                    """
                    SELECT * FROM spu_audience_map
                    ORDER BY scope,scope_key,audience_code,role
                    """
                )
            ]
        return trims, trim_aliases, audiences

    def test_dry_run_plans_existing_binding_and_stable_new_slug_read_only(self) -> None:
        self._write([self._existing_c_row(), self._row()])
        before_stat = self.database.stat()
        receipt = self._dry_run()
        after_stat = self.database.stat()

        self.assertEqual((receipt["mode"], receipt["applied"]), ("dry_run", False))
        self.assertEqual(receipt["operations"]["series_insert"], 1)
        self.assertEqual(receipt["operations"]["series_update"], 1)
        self.assertEqual(receipt["operations"]["alias_insert"], 2)
        self.assertEqual(len(str(receipt["plan_sha256"])), 64)
        self.assertEqual(before_stat.st_size, after_stat.st_size)
        self.assertFalse(self.backups.exists())
        audit = receipt["input"]["series_id_audit"]
        self.assertEqual(
            audit[0]["represented_official_series_ids"], ["195", "1458", "6436"]
        )
        with importer._connect_read_only(self.database) as connection:
            self.assertIsNone(
                connection.execute(
                    "SELECT 1 FROM spu_catalog WHERE spu_id='dcd__series-197'"
                ).fetchone()
            )

    def test_apply_backs_up_is_idempotent_and_preserves_trims_and_audiences(
        self,
    ) -> None:
        self._write([self._existing_c_row(), self._row()])
        protected_before = self._protected_state()
        receipt_path = self.root / "receipts" / "series-import.json"
        applied = self._apply(
            self._dry_run(), skip_backup=False, receipt_path=receipt_path
        )

        self.assertTrue(applied["applied"])
        self.assertTrue(receipt_path.is_file())
        backup = applied["backup"]
        self.assertIsInstance(backup, dict)
        self.assertEqual(backup["validation"]["quick_check"], "ok")
        self.assertEqual(backup["validation"]["foreign_key_violations"], 0)
        self.assertTrue(Path(backup["path"]).is_file())
        self.assertEqual(self._protected_state(), protected_before)

        with importer._connect_read_only(self.database) as connection:
            existing = connection.execute(
                """
                SELECT external_ref,powertrain FROM spu_catalog
                WHERE spu_id='benz__c-class'
                """
            ).fetchone()
            added = connection.execute(
                """
                SELECT spu_id,series_slug,external_ref,is_series_node,powertrain
                FROM spu_catalog WHERE spu_id='dcd__series-197'
                """
            ).fetchone()
            new_audience = connection.execute(
                """
                SELECT COUNT(*) FROM spu_audience_map
                WHERE scope='series' AND scope_key='dcd__series-197'
                """
            ).fetchone()[0]
        self.assertEqual(tuple(existing), ("dongchedi:series:195", "ice"))
        self.assertEqual(
            tuple(added),
            ("dcd__series-197", "dcd__series-197", "dongchedi:series:197", 1, ""),
        )
        self.assertEqual(new_audience, 0)

        second = self._dry_run()
        self.assertEqual(second["operations"]["total_changes"], 0)
        no_op = self._apply(second, skip_backup=False)
        self.assertFalse(no_op["applied"])
        self.assertIsNone(no_op["backup"])
        self.assertEqual(len(list(self.backups.glob("*.sqlite3"))), 1)

    def test_artifact_hash_required_fields_and_official_endpoint_fail_closed(
        self,
    ) -> None:
        self._write([self._row()])
        payload = self._payload()
        payload["rows"][0]["series"] = "篡改车系"
        self._write_payload(payload, rehash=False)
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "catalog_sha256"
        ):
            self._dry_run()

        self._write([self._row()])
        payload = self._payload()
        del payload["rows"][0]["price"]
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(importer.SpuSeriesCatalogImportError, "price"):
            self._dry_run()

        self._write([self._row()])
        payload = self._payload()
        payload["source"]["series_endpoint"] = "https://example.com/not-official"
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "get_brand_series_list"
        ):
            self._dry_run()

    def test_source_request_attestation_summary_and_status_enum_are_exact(self) -> None:
        self._write([self._row(business_status="upcoming")])
        receipt = self._dry_run()
        self.assertEqual(receipt["input"]["business_statuses"], {"upcoming": 1})

        for field, invalid, message in (
            ("city_name", "上海", "city_name"),
            ("included_business_statuses", [0], "exactly"),
            ("brands_requested", 644, "must be 645"),
        ):
            with self.subTest(field=field):
                self._write([self._row()])
                payload = self._payload()
                payload["source"][field] = invalid
                self._write_payload(payload, rehash=True)
                with self.assertRaisesRegex(
                    importer.SpuSeriesCatalogImportError, message
                ):
                    self._dry_run()

        self._write([self._row()])
        payload = self._payload()
        payload["source"]["series_requests"][0].pop("response_sha256")
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "response_sha256"
        ):
            self._dry_run()

        self._write([self._row()])
        payload = self._payload()
        payload["summary"]["selected_count"] += 1
        self._write_payload(payload, rehash=True)
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "summary.selected_count"
        ):
            self._dry_run()

        self._write([self._row(business_status="停售")])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "on_sale, upcoming, 0, or 2"
        ):
            self._dry_run()

    def test_represented_ids_are_audited_global_unique_and_new_series_is_one_to_one(
        self,
    ) -> None:
        self._write([self._existing_c_row()])
        receipt = self._dry_run()
        self.assertEqual(
            receipt["input"]["artifact_summary"][
                "represented_official_series_id_count"
            ],
            3,
        )

        duplicate = self._row(
            dcd_series_id="6436",
            series="奔驰测试分支",
            aliases=["奔驰测试分支"],
        )
        self._write([self._existing_c_row(), duplicate])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError,
            "represented official series ID 6436",
        ):
            self._dry_run()

        invalid_new = self._row(represented_ids=["197", "9197"])
        self._write([invalid_new])
        with self.assertRaisesRegex(importer.SpuSeriesCatalogImportError, "one ID"):
            self._dry_run()

    def test_existing_series_requires_explicit_binding_and_identity_match(self) -> None:
        unbound = self._row(
            dcd_series_id="195",
            series="奔驰C级",
            price_low=33.0,
            price_high=38.0,
            aliases=["奔驰C级"],
        )
        self._write([unbound])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "existing_series_slug explicitly"
        ):
            self._dry_run()

        mismatched = self._existing_c_row()
        mismatched["series"] = "奔驰S级"
        mismatched["aliases"] = ["奔驰S级"]
        self._write([mismatched])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "immutable series"
        ):
            self._dry_run()

    def test_external_ref_and_unambiguous_alias_collisions_fail_closed(self) -> None:
        with connect(self.database) as connection:
            connection.execute(
                """
                UPDATE spu_catalog SET external_ref='dongchedi:series:197'
                WHERE spu_id='benz__glc'
                """
            )
            connection.commit()
        self._write([self._row()])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "external_ref"
        ):
            self._dry_run()

        with connect(self.database) as connection:
            connection.execute(
                "UPDATE spu_catalog SET external_ref='' WHERE spu_id='benz__glc'"
            )
            connection.commit()
        alias_conflict = self._row(
            aliases=[
                "奔驰E级",
                {
                    "alias": "GLC",
                    "alias_type": "model_code",
                    "ambiguous": False,
                    "enabled": True,
                },
            ]
        )
        self._write([alias_conflict])
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "must be ambiguous"
        ):
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
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "are running"
        ):
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
                WHERE spu_id='benz__c-class' AND alias='奔驰C级'
                """
            )
            connection.commit()
        with self.assertRaisesRegex(importer.SpuSeriesCatalogImportError, "plan hash"):
            self._apply(dry_run)
        with importer._connect_read_only(self.database) as connection:
            count = connection.execute(
                "SELECT COUNT(*) FROM spu_catalog WHERE spu_id='dcd__series-197'"
            ).fetchone()[0]
        self.assertEqual(count, 0)

    def test_failed_batch_rolls_back_all_series_and_alias_writes(self) -> None:
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
            series_count = connection.execute(
                "SELECT COUNT(*) FROM spu_catalog WHERE spu_id='dcd__series-197'"
            ).fetchone()[0]
            alias_count = connection.execute(
                "SELECT COUNT(*) FROM spu_alias WHERE spu_id='dcd__series-197'"
            ).fetchone()[0]
        self.assertEqual((series_count, alias_count), (0, 0))

    def test_schema_v15_plan_sha_backup_and_receipt_guards(self) -> None:
        self._write([self._row()])
        dry_run = self._dry_run()
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "expect-plan"
        ):
            importer.execute_import(
                self.input_path,
                db_path=self.database,
                apply=True,
                skip_backup=True,
            )

        with connect(self.database) as connection:
            connection.execute("PRAGMA user_version=14")
            connection.commit()
        with self.assertRaisesRegex(
            importer.SpuSeriesCatalogImportError, "must be v15"
        ):
            self._dry_run()
        with connect(self.database) as connection:
            connection.execute("PRAGMA user_version=15")
            connection.commit()

        dry_run = self._dry_run()
        with patch.object(importer, "DEFAULT_DB", self.database):
            with self.assertRaisesRegex(
                importer.SpuSeriesCatalogImportError, "forbidden"
            ):
                self._apply(dry_run, skip_backup=True)

        for destination in (
            self.input_path,
            self.database,
            Path(f"{self.database}-wal"),
            Path(f"{self.database}-shm"),
            Path(f"{self.database}-journal"),
        ):
            with self.subTest(destination=destination):
                with self.assertRaisesRegex(
                    importer.SpuSeriesCatalogImportError, "must not overwrite"
                ):
                    importer.execute_import(
                        self.input_path,
                        db_path=self.database,
                        receipt_path=destination,
                    )

    def test_cli_defaults_to_dry_run_and_emits_json_receipt(self) -> None:
        self._write([self._row()])
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = cli.main(
                ["--input", str(self.input_path), "--db", str(self.database)]
            )
        self.assertEqual((code, stderr.getvalue()), (0, ""))
        receipt = json.loads(stdout.getvalue())
        self.assertEqual(receipt["mode"], "dry_run")
        self.assertFalse(receipt["applied"])


if __name__ == "__main__":
    unittest.main()
