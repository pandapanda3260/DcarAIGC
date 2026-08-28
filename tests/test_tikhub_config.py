from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from probe_tikhub_douyin import load_key as load_probe_key
from tikhub_config import (
    DEFAULT_TIKHUB_API_BASE,
    DEFAULT_TIKHUB_CONFIG_FILE,
    TikHubConfigurationError,
    load_tikhub_api_base,
    load_tikhub_api_key,
)


class TikHubConfigurationTest(unittest.TestCase):
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

    def test_key_and_base_load_from_the_same_overridden_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://api.tikhub.io\n"
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
                self.assertEqual(load_tikhub_api_base(), DEFAULT_TIKHUB_API_BASE)

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

    def test_unapproved_base_is_rejected_before_any_request(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            config = Path(temporary) / "dcar.env.local"
            config.write_text(
                "TIKHUB_API_BASE=https://attacker.example\n"
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
