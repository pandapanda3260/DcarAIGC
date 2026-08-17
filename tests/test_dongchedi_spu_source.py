from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import unittest
import urllib.parse
from contextlib import redirect_stderr
from pathlib import Path

from scripts import fetch_dongchedi_spu_trims as fetch_cli
from v8.dongchedi_spu_source import (
    DongchediSourceError,
    build_normalized_catalog,
    build_trim_aliases,
    fetch_normalized_catalog,
    write_json_atomic,
)


def _mapping(*, mixed: bool = True) -> dict:
    return {
        "mapping_version": "test-v1",
        "entries": {
            "brand__series": {
                "default_powertrain": "hev" if mixed else "ev",
                "powertrain_resolution": (
                    "infer_from_official_series_or_trim_spec" if mixed else "fixed"
                ),
                "official_series": [
                    {
                        "series_id": 101,
                        "official_name": "测试车系",
                        "powertrain": "mixed" if mixed else "ev",
                        "include_recent": True,
                        "include_history": True,
                    }
                ],
            },
            "brand__unresolved": {
                "default_powertrain": "ice",
                "powertrain_resolution": "fixed",
                "official_series": [
                    {
                        "series_id": 102,
                        "official_name": "旧车系",
                        "powertrain": "ice",
                        "include_recent": False,
                        "include_history": True,
                    }
                ],
            },
        },
        "unresolved": [
            {
                "series_slug": "brand__unresolved",
                "reason": "没有近期官方款型，不能拿旧款代替",
            }
        ],
    }


def _local_series() -> dict:
    return {
        "brand__series": {
            "spu_id": "brand__series",
            "brand": "测试品牌",
            "series": "测试车系",
            "series_slug": "brand__series",
            "powertrain": "hev",
            "body_style": "轿车",
        },
        "brand__unresolved": {
            "spu_id": "brand__unresolved",
            "brand": "测试品牌",
            "series": "旧车系",
            "series_slug": "brand__unresolved",
            "powertrain": "ice",
            "body_style": "SUV",
        },
    }


def _car(
    car_id: int,
    name: str,
    year: int,
    *,
    price: object = None,
    official_price: object = None,
    item_type: object = "1037",
    car_name: str = "",
) -> dict:
    info = {
        "car_id": car_id,
        "series_id": 101,
        "name": name,
        "year": year,
        "price": price,
        "official_price": official_price,
        "car_name": car_name,
        "tags": ["前驱"],
    }
    return {"type": item_type, "info": info}


def _success_response(*, online: list, offline: list) -> dict:
    return {
        "status": "success",
        "message": "data is load",
        "data": {
            "series_id": 101,
            "series_name": "测试车系",
            "online": online,
            "offline": offline,
            "presale_car": [_car(999, "2027款 纯电 Future", 2027, official_price=99.0)],
            "urban": [_car(998, "2027款 城市版", 2027, official_price=88.0)],
        },
    }


class DongchediSpuSourceTest(unittest.TestCase):
    def test_recent_policy_normalization_aliases_and_powertrain(self) -> None:
        response = _success_response(
            online=[
                {"type": "1036", "info": {"name": "2023款"}},
                _car(
                    1,
                    "2023款 2.0L 豪华版",
                    2023,
                    official_price="17.18",
                    car_name="2.0L 豪华版",
                ),
                _car(4, "2027款 纯电 Future", 2027, official_price=31.0),
                _car(5, "2026款 Max", 2026, official_price=25.0),
                _car(6, "2023款 超混电驱 全电驱Pro", 2023, official_price=14.0),
            ],
            offline=[
                _car(
                    2, "2024款 双擎 2.0L 智享版", 2024, price="20.58万", item_type=1037
                ),
                _car(3, "2023款 1.5T 舒适版", 2023, price="12.00万"),
                _car(1, "2023款 2.0L 豪华版", 2023, price="16.88万"),
                _car(7, "2024款 经典 1.6L XE+ CVT大屏版", 2024, price="10.86万"),
            ],
        )

        calls: list[str] = []

        def fake_fetch(url: str, timeout: float) -> dict:
            calls.append(url)
            self.assertEqual(timeout, 9.0)
            self.assertEqual(
                urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["series_id"],
                ["101"],
            )
            return response

        payload = build_normalized_catalog(
            mapping=_mapping(),
            local_series=_local_series(),
            offline_min_model_year=2024,
            max_model_year=2026,
            workers=1,
            timeout=9.0,
            retries=1,
            fetch_json=fake_fetch,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(
            payload["schema_version"], "dcar-dongchedi-spu-trims-normalized-v1"
        )
        self.assertEqual(payload["summary"]["rows"], 5)
        self.assertEqual(payload["summary"]["online_rows"], 3)
        self.assertEqual(payload["summary"]["offline_rows"], 2)
        self.assertEqual(payload["summary"]["unresolved_series_slugs"], 1)
        self.assertEqual(payload["unresolved"][0]["series_slug"], "brand__unresolved")

        rows = {row["car_id"]: row for row in payload["rows"]}
        self.assertEqual(set(rows), {1, 2, 5, 6, 7})
        self.assertEqual(rows[1]["source_bucket"], "online")
        self.assertEqual(rows[1]["powertrain"], "ice")
        self.assertEqual(rows[2]["powertrain"], "hev")
        self.assertEqual(rows[6]["powertrain"], "hev")
        self.assertEqual(rows[7]["powertrain"], "ice")
        self.assertEqual(rows[1]["body_style"], "轿车")
        self.assertEqual(rows[1]["price_low"], 17.18)
        self.assertEqual(rows[2]["price_high"], 20.58)

        aliases = {item["alias"] for item in rows[1]["aliases"]}
        self.assertIn("2023款 2.0L 豪华版", aliases)
        self.assertIn("2023款2.0L豪华版", aliases)
        self.assertIn("2.0L 豪华版", aliases)
        self.assertIn("2.0L豪华版", aliases)
        max_aliases = {item["alias"] for item in rows[5]["aliases"]}
        self.assertEqual(max_aliases, {"2026款 Max", "2026款Max"})
        self.assertNotIn(999, rows)
        self.assertNotIn(998, rows)
        artifact_without_hash = dict(payload)
        recorded_hash = artifact_without_hash.pop("catalog_sha256")
        canonical = json.dumps(
            artifact_without_hash,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(hashlib.sha256(canonical).hexdigest(), recorded_hash)

    def test_retry_covers_transport_or_schema_failure(self) -> None:
        attempts = 0
        response = _success_response(
            online=[_car(7, "2026款 纯电 长续航版", 2026, official_price=22.0)],
            offline=[],
        )

        def flaky_fetch(url: str, timeout: float) -> dict:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return {"status": "fail", "message": "temporary"}
            return response

        sleeps: list[float] = []
        payload = build_normalized_catalog(
            mapping=_mapping(mixed=False),
            local_series=_local_series(),
            offline_min_model_year=2024,
            max_model_year=2026,
            workers=1,
            retries=2,
            fetch_json=flaky_fetch,
            sleep_fn=sleeps.append,
        )
        self.assertEqual(attempts, 2)
        self.assertEqual(sleeps, [0.5])
        self.assertEqual(payload["rows"][0]["powertrain"], "ev")

    def test_wrong_official_series_name_fails_closed(self) -> None:
        response = _success_response(online=[], offline=[])
        response["data"]["series_name"] = "另一个车系"
        with self.assertRaisesRegex(DongchediSourceError, "车系名错位"):
            build_normalized_catalog(
                mapping=_mapping(),
                local_series=_local_series(),
                offline_min_model_year=2024,
                max_model_year=2026,
                workers=1,
                retries=1,
                fetch_json=lambda url, timeout: response,
            )

    def test_generic_configuration_alias_is_not_emitted(self) -> None:
        aliases = {item["alias"] for item in build_trim_aliases("2026款 Ultra")}
        self.assertEqual(aliases, {"2026款 Ultra", "2026款Ultra"})

    def test_bare_hybrid_names_are_not_misclassified_as_ice(self) -> None:
        response = _success_response(
            online=[
                _car(21, "2026款 混动 2.0L 两驱智睿版", 2026, official_price=21.99),
                _car(22, "2024款 油混 1.5T 两驱长风版", 2024, official_price=16.77),
            ],
            offline=[],
        )
        payload = build_normalized_catalog(
            mapping=_mapping(),
            local_series=_local_series(),
            offline_min_model_year=2024,
            max_model_year=2026,
            workers=1,
            retries=1,
            fetch_json=lambda url, timeout: response,
        )
        self.assertEqual(
            {row["car_id"]: row["powertrain"] for row in payload["rows"]},
            {21: "hev", 22: "hev"},
        )

    def test_mixed_series_can_split_pure_ev_and_range_extender(self) -> None:
        response = _success_response(
            online=[
                _car(23, "2026款 纯电 Max 四驱版", 2026, official_price=31.98),
                _car(24, "2026款 增程 1704 Max", 2026, official_price=20.68),
            ],
            offline=[],
        )
        payload = build_normalized_catalog(
            mapping=_mapping(),
            local_series=_local_series(),
            offline_min_model_year=2024,
            max_model_year=2026,
            workers=1,
            retries=1,
            fetch_json=lambda url, timeout: response,
        )
        self.assertEqual(
            {row["car_id"]: row["powertrain"] for row in payload["rows"]},
            {23: "ev", 24: "erev"},
        )

    def test_empty_include_recent_official_subseries_fails_closed(self) -> None:
        mapping = _mapping(mixed=False)
        mapping["entries"]["brand__series"]["official_series"].append(
            {
                "series_id": 103,
                "official_name": "测试空子车系",
                "powertrain": "ev",
                "include_recent": True,
                "include_history": True,
            }
        )

        def fake_fetch(url: str, timeout: float) -> dict:
            series_id = int(
                urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)["series_id"][0]
            )
            if series_id == 101:
                return _success_response(
                    online=[
                        _car(31, "2026款 纯电 长续航版", 2026, official_price=22.0)
                    ],
                    offline=[],
                )
            return {
                "status": "success",
                "message": "data is load",
                "data": {
                    "series_id": 103,
                    "series_name": "测试空子车系",
                    "online": [],
                    "offline": [],
                },
            }

        with self.assertRaisesRegex(DongchediSourceError, "拒绝静默少导"):
            build_normalized_catalog(
                mapping=mapping,
                local_series=_local_series(),
                offline_min_model_year=2024,
                max_model_year=2026,
                workers=1,
                retries=1,
                fetch_json=fake_fetch,
            )

    def test_mapping_and_database_are_read_only_inputs_and_output_is_atomic(
        self,
    ) -> None:
        mapping = _mapping(mixed=False)
        response = _success_response(
            online=[_car(8, "2026款 纯电 标准续航版", 2026, official_price=18.0)],
            offline=[],
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mapping_path = root / "mapping.json"
            mapping_path.write_text(
                json.dumps(mapping, ensure_ascii=False), encoding="utf-8"
            )
            database = root / "catalog.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    """
                    CREATE TABLE spu_catalog (
                        spu_id TEXT, brand TEXT, series TEXT, series_slug TEXT,
                        powertrain TEXT, body_style TEXT,
                        is_series_node INTEGER, enabled INTEGER
                    )
                    """
                )
                for row in _local_series().values():
                    connection.execute(
                        "INSERT INTO spu_catalog VALUES (?, ?, ?, ?, ?, ?, 1, 1)",
                        (
                            row["spu_id"],
                            row["brand"],
                            row["series"],
                            row["series_slug"],
                            row["powertrain"],
                            row["body_style"],
                        ),
                    )
                connection.commit()
            finally:
                connection.close()
            before = database.read_bytes()
            payload = fetch_normalized_catalog(
                mapping_path=mapping_path,
                db_path=database,
                offline_min_model_year=2024,
                max_model_year=2026,
                workers=1,
                retries=1,
                fetch_json=lambda url, timeout: response,
            )
            self.assertEqual(database.read_bytes(), before)
            output = root / "nested" / "trims.json"
            write_json_atomic(output, payload)
            decoded = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(decoded["catalog_sha256"], payload["catalog_sha256"])
            self.assertEqual(list(output.parent.glob(".*.tmp")), [])

    def test_cli_output_cannot_overwrite_database_or_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            mapping = root / "mapping.json"
            database = root / "catalog.sqlite3"
            mapping.write_text("{}\n", encoding="utf-8")
            database.write_bytes(b"sqlite-placeholder")
            protected = [
                database,
                *(
                    Path(f"{database}{suffix}")
                    for suffix in ("-wal", "-shm", "-journal")
                ),
            ]
            for output in protected:
                with self.subTest(output=output), redirect_stderr(io.StringIO()):
                    self.assertEqual(
                        fetch_cli.main(
                            [
                                "--mapping",
                                str(mapping),
                                "--db",
                                str(database),
                                "--output",
                                str(output),
                            ]
                        ),
                        2,
                    )


if __name__ == "__main__":
    unittest.main()
