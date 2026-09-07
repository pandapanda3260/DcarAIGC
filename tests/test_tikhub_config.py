from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from probe_tikhub_douyin import fetch as legacy_probe_fetch
from probe_tikhub_douyin import load_key as load_probe_key
from tikhub_config import (
    DEFAULT_TIKHUB_API_BASE,
    DEFAULT_TIKHUB_CONFIG_FILE,
    TikHubConfigurationError,
    load_tikhub_api_base,
    load_tikhub_http_stack,
    load_tikhub_api_key,
    resolve_tikhub_transport_manifest,
    validate_current_tikhub_transport_manifest,
)


class TikHubConfigurationTest(unittest.TestCase):
    def test_legacy_probe_network_entry_is_retired(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "v8 writer capture path"):
            legacy_probe_fetch("/paid", {"id": "123"}, "fixture-key")

    def test_api_import_does_not_access_any_tikhub_config_file(self) -> None:
        repository = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            environment = {
                key: value
                for key, value in os.environ.items()
                if not key.startswith("TIKHUB_")
            }
            environment.update(
                {
                    "PYTHONPATH": os.pathsep.join(
                        (str(repository / "src"), str(repository / "src" / "dcar_eval"))
                    ),
                    "PYTHONDONTWRITEBYTECODE": "1",
                    "DCAR_V8_DB": str(Path(temporary) / "not-opened.sqlite3"),
                    "DCAR_READ_ONLY": "1",
                    "DCAR_SCHEDULER_ENABLED": "0",
                    "DCAR_STARTUP_CATCHUP_ENABLED": "0",
                    "DCAR_LLM_DISABLED": "1",
                    "DCAR_TEST_DENY_FORMAL_DB": "1",
                    "TIKHUB_API_KEY_FILE": str(Path(temporary) / "unreadable.env"),
                }
            )
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from unittest.mock import patch\n"
                    "import tikhub_config\n"
                    "with patch.object(tikhub_config, '_safe_config_file', "
                    "side_effect=AssertionError('credentials read during import')):\n"
                    "    import v8.api\n",
                ],
                cwd=repository,
                env=environment,
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_default_file_is_the_central_dcar_environment(self) -> None:
        self.assertEqual(
            DEFAULT_TIKHUB_CONFIG_FILE,
            Path("/Users/mark/Documents/key/DcarKey/dcar.env.local"),
        )

    def test_key_and_each_approved_base_load_from_the_same_overridden_file(
        self,
    ) -> None:
        for api_base in ("https://api.tikhub.dev", "https://api.tikhub.io"):
            with (
                self.subTest(api_base=api_base),
                tempfile.TemporaryDirectory() as temporary,
            ):
                config = Path(temporary) / "dcar.env.local"
                config.write_text(
                    f"TIKHUB_API_BASE={api_base}\n"
                    "TIKHUB_API_KEY=test-secret\n",
                    encoding="utf-8",
                )
                config.chmod(0o600)
                with patch.dict(
                    os.environ,
                    {"TIKHUB_API_KEY_FILE": str(config)},
                    clear=True,
                ):
                    self.assertEqual(load_tikhub_api_key(), "test-secret")
                    self.assertEqual(load_tikhub_api_base(), api_base)

    def test_file_base_takes_precedence_over_direct_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.dev\n"
                "TIKHUB_API_KEY=test-secret\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(
                os.environ,
                {
                    "TIKHUB_API_KEY_FILE": str(config),
                    "TIKHUB_API_BASE": "https://api.tikhub.io",
                },
                clear=True,
            ):
                self.assertEqual(load_tikhub_api_base(), "https://api.tikhub.dev")

    def test_direct_key_keeps_existing_precedence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text("TIKHUB_API_KEY=file-secret\n", encoding="utf-8")
            config.chmod(0o600)
            with patch.dict(
                os.environ,
                {
                    "TIKHUB_API_KEY": "direct-secret",
                    "TIKHUB_API_KEY_FILE": str(config),
                },
                clear=True,
            ):
                self.assertEqual(load_tikhub_api_key(), "direct-secret")

    def test_key_only_legacy_file_keeps_canonical_base_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "TikHub.env.local"
            config.write_text("TIKHUB_API_KEY=test-secret\n", encoding="utf-8")
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True):
                self.assertEqual(
                    load_tikhub_api_base(config), DEFAULT_TIKHUB_API_BASE
                )

    def test_transport_manifest_freezes_only_canonical_non_secret_route_fields(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.dev\n"
                "TIKHUB_API_KEY=test-secret\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True):
                manifest = resolve_tikhub_transport_manifest(config)

        unsigned = {
            "contract_version": "tikhub-request-transport-v1",
            "api_base": "https://api.tikhub.dev",
            "request_host": "api.tikhub.dev",
            "transport_route_id": "tikhub-api.tikhub.dev-stream-v1",
            "http_stack": "urllib-stream-v1",
            "route_generation": (
                "route-config-sha256:"
                + hashlib.sha256(b"https://api.tikhub.dev").hexdigest()
            ),
        }
        self.assertEqual(
            manifest,
            {
                **unsigned,
                "config_sha256": hashlib.sha256(
                    json.dumps(
                        unsigned,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode("utf-8")
                ).hexdigest(),
            },
        )
        self.assertNotIn("test-secret", json.dumps(manifest, sort_keys=True))

    def test_transport_manifest_can_freeze_legacy_control_stack(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "control.env"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.dev\n"
                "TIKHUB_HTTP_STACK=urllib-legacy-v1\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True):
                manifest = resolve_tikhub_transport_manifest(
                    config,
                    honor_environment=False,
                )
                self.assertEqual(load_tikhub_http_stack(config), "urllib-legacy-v1")

        self.assertEqual(manifest["api_base"], "https://api.tikhub.dev")
        self.assertEqual(manifest["request_host"], "api.tikhub.dev")
        self.assertEqual(manifest["http_stack"], "urllib-legacy-v1")
        self.assertEqual(
            manifest["transport_route_id"],
            "tikhub-api.tikhub.dev-legacy-v1",
        )

    def test_transport_manifest_validation_reloads_and_rejects_route_drift(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.dev\n"
                "TIKHUB_API_KEY=test-secret\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True):
                manifest = resolve_tikhub_transport_manifest(config)
                self.assertEqual(
                    validate_current_tikhub_transport_manifest(manifest, config),
                    manifest,
                )
                config.write_text(
                    "TIKHUB_API_BASE=https://api.tikhub.io\n"
                    "TIKHUB_API_KEY=test-secret\n",
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(
                    TikHubConfigurationError, "当前 route 配置不一致"
                ):
                    validate_current_tikhub_transport_manifest(manifest, config)

    def test_transport_manifest_validation_rejects_tampered_or_extra_fields(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.io\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True):
                manifest = resolve_tikhub_transport_manifest(config)
                for field, value in (
                    ("http_stack", "alternate-stack-v1"),
                    ("config_sha256", "0" * 64),
                    ("unexpected", True),
                ):
                    with self.subTest(field=field), self.assertRaises(
                        TikHubConfigurationError
                    ):
                        validate_current_tikhub_transport_manifest(
                            {**manifest, field: value},
                            config,
                        )

    def test_unapproved_base_is_rejected_before_any_request(self) -> None:
        invalid_bases = (
            "http://api.tikhub.dev",
            "https://attacker.example",
            "https://user@api.tikhub.dev",
            "https://api.tikhub.dev/api",
            "https://api.tikhub.dev?route=other",
            "https://api.tikhub.dev#fragment",
            "https://api.tikhub.dev/",
            "https://api.tikhub.dev:443",
        )
        for api_base in invalid_bases:
            with (
                self.subTest(api_base=api_base),
                tempfile.TemporaryDirectory() as temporary,
            ):
                config = Path(temporary) / "dcar.env.local"
                config.write_text(
                    f"TIKHUB_API_BASE={api_base}\n"
                    "TIKHUB_API_KEY=test-secret\n",
                    encoding="utf-8",
                )
                config.chmod(0o600)
                with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(
                    TikHubConfigurationError, "https://api.tikhub.io"
                ):
                    load_tikhub_api_base(config)

    def test_probe_validates_base_before_loading_the_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "probe.env"
            config.write_text(
                "TIKHUB_API_BASE=https://attacker.example\n"
                "TIKHUB_API_KEY=test-secret\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch(
                "probe_tikhub_douyin.load_tikhub_api_key"
            ) as load_key, self.assertRaisesRegex(RuntimeError, "https://api.tikhub.io"):
                load_probe_key(config)
            load_key.assert_not_called()

    def test_probe_explicit_config_keeps_precedence_over_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "probe.env"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.io\n"
                "TIKHUB_API_KEY=explicit-test-secret\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(
                os.environ,
                {
                    "TIKHUB_API_KEY_FILE": str(Path(temporary) / "unused.env"),
                    "TIKHUB_API_BASE": "https://attacker.example",
                    "TIKHUB_API_KEY": "unused-test-secret",
                },
            ):
                self.assertEqual(load_probe_key(config), "explicit-test-secret")

    def test_duplicate_tikhub_entries_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_KEY=first\nTIKHUB_API_KEY=second\n",
                encoding="utf-8",
            )
            config.chmod(0o600)
            with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(
                TikHubConfigurationError, "重复配置"
            ):
                load_tikhub_api_key(config)

    def test_group_or_world_readable_config_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text("TIKHUB_API_KEY=test-secret\n", encoding="utf-8")
            config.chmod(0o644)
            with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(
                TikHubConfigurationError, "0400 或 0600"
            ):
                load_tikhub_api_key(config)

    def test_symlink_config_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "target.env"
            target.write_text("TIKHUB_API_KEY=test-secret\n", encoding="utf-8")
            target.chmod(0o600)
            link = Path(temporary) / "dcar.env.local"
            link.symlink_to(target)
            with patch.dict(os.environ, {}, clear=True), self.assertRaisesRegex(
                TikHubConfigurationError, "符号链接"
            ):
                load_tikhub_api_key(link)


if __name__ == "__main__":
    unittest.main()
