from __future__ import annotations

import os
import plistlib
import tempfile
import unittest
from contextlib import ExitStack, asynccontextmanager
from pathlib import Path
from unittest.mock import patch

from v8 import api
from v8.runtime_database import (
    DatabaseAccessMode,
    RuntimeDatabaseError,
    acquire_writer_lock,
    hold_formal_mutation,
    is_installed_formal_database,
    load_installed_writer_contract,
    observe_writer_lock,
    resolve_installed_database_access,
    resolve_isolated_candidate,
    resolve_read_only_replica,
)


class InstalledRuntimeFixture:
    def __init__(self, root: Path) -> None:
        self.home = root / "home"
        self.project = root / "project"
        self.database = root / "external" / "data" / "dcar.sqlite3"
        self.writer_lock = root / "external" / "runtime" / "writer.lock"
        self.plist = (
            self.home
            / "Library/LaunchAgents/cn.tj.dcar.writer-worker.plist"
        )
        (self.project / "deploy/macos").mkdir(parents=True)
        (self.project / "deploy/macos/run_writer_worker.sh").write_bytes(b"#!/bin/bash\n")
        self.database.parent.mkdir(parents=True)
        self.database.write_bytes(b"not-opened-by-resolver")
        os.chmod(self.database, 0o600)
        self.writer_lock.parent.mkdir(parents=True)
        self.writer_lock.write_bytes(b"")
        os.chmod(self.writer_lock, 0o600)
        self.plist.parent.mkdir(parents=True)
        payload = {
            "Label": "cn.tj.dcar.writer-worker",
            "WorkingDirectory": str(self.project),
            "ProgramArguments": [
                str(self.project / "deploy/macos/run_writer_worker.sh")
            ],
            "EnvironmentVariables": {
                "DCAR_PROJECT_ROOT": str(self.project),
                "DCAR_V8_DB": str(self.database),
                "DCAR_WRITER_LOCK": str(self.writer_lock),
            },
        }
        self.plist.write_bytes(plistlib.dumps(payload))
        os.chmod(self.plist, 0o600)

    @property
    def environment(self) -> dict[str, str]:
        return {
            "DCAR_PROJECT_ROOT": str(self.project),
            "DCAR_V8_DB": str(self.database),
            "DCAR_WRITER_LOCK": str(self.writer_lock),
        }


class RuntimeDatabaseResolverTest(unittest.TestCase):
    def setUp(self) -> None:
        self.stack = ExitStack()
        self.temp = Path(
            self.stack.enter_context(tempfile.TemporaryDirectory())
        ).resolve(strict=True)
        self.fixture = InstalledRuntimeFixture(self.temp)
        self.installed = load_installed_writer_contract(home=self.fixture.home)
        assert self.installed is not None

    def tearDown(self) -> None:
        self.stack.close()

    def test_writer_resolution_exposes_exact_database_identity(self) -> None:
        access = resolve_installed_database_access(
            DatabaseAccessMode.WRITER,
            database=self.fixture.database,
            project_root=self.fixture.project,
            environ=self.fixture.environment,
            installed=self.installed,
        )

        value = access.health_identity()
        identity = self.fixture.database.stat()
        self.assertEqual(
            value,
            {
                "canonical_path": str(self.fixture.database.resolve(strict=True)),
                "device": identity.st_dev,
                "inode": identity.st_ino,
                "nlink": 1,
                "access_mode": "writer",
            },
        )

    def test_writer_lease_exposes_exact_lock_identity_and_blocks_peer(self) -> None:
        access = resolve_installed_database_access(
            "writer",
            database=self.fixture.database,
            project_root=self.fixture.project,
            environ=self.fixture.environment,
            installed=self.installed,
        )

        with acquire_writer_lock(access) as lease:
            value = lease.health_identity(held=True)
            lock_identity = self.fixture.writer_lock.stat()
            self.assertEqual(
                value,
                {
                    "path": str(self.fixture.writer_lock),
                    "device": lock_identity.st_dev,
                    "inode": lock_identity.st_ino,
                    "held": True,
                },
            )
            with self.assertRaisesRegex(RuntimeDatabaseError, "already held"):
                with acquire_writer_lock(access):
                    pass
            self.assertTrue(observe_writer_lock(access)["held"])
        self.assertFalse(observe_writer_lock(access)["held"])

    def test_formal_mutation_resolves_installed_identity_and_holds_writer_lock(
        self,
    ) -> None:
        with hold_formal_mutation(
            self.fixture.database,
            project_root=self.fixture.project,
            environ=self.fixture.environment,
            installed=self.installed,
        ) as access:
            self.assertEqual(
                access.access_mode,
                DatabaseAccessMode.FORMAL_MUTATION,
            )
            self.assertEqual(access.database, self.fixture.database.resolve())
            self.assertTrue(observe_writer_lock(access)["held"])
        self.assertFalse(observe_writer_lock(access)["held"])

    def test_missing_installed_lock_is_not_created(self) -> None:
        access = resolve_installed_database_access(
            "writer",
            database=self.fixture.database,
            project_root=self.fixture.project,
            environ=self.fixture.environment,
            installed=self.installed,
        )
        self.fixture.writer_lock.unlink()

        with self.assertRaises(FileNotFoundError):
            with acquire_writer_lock(access):
                pass
        self.assertFalse(self.fixture.writer_lock.exists())

    def test_clone_with_same_bytes_is_not_the_installed_database(self) -> None:
        clone = self.temp / "clone.sqlite3"
        clone.write_bytes(self.fixture.database.read_bytes())
        os.chmod(clone, 0o600)

        with self.assertRaisesRegex(
            RuntimeDatabaseError, "does not identify the installed writer database"
        ):
            resolve_installed_database_access(
                "writer",
                database=clone,
                project_root=self.fixture.project,
                environ={**self.fixture.environment, "DCAR_V8_DB": str(clone)},
                installed=self.installed,
            )

    def test_hardlink_is_recognized_as_formal_then_rejected(self) -> None:
        alias = self.temp / "database-hardlink.sqlite3"
        os.link(self.fixture.database, alias)
        self.assertTrue(
            is_installed_formal_database(alias, installed=self.installed)
        )

        with self.assertRaisesRegex(RuntimeDatabaseError, "single-link"):
            resolve_installed_database_access(
                "formal_mutation",
                database=alias,
                project_root=self.fixture.project,
                environ={
                    "DCAR_V8_DB": str(alias),
                    "DCAR_WRITER_LOCK": str(self.fixture.writer_lock),
                },
                installed=self.installed,
            )

    def test_isolated_candidate_rejects_installed_inode(self) -> None:
        with self.assertRaisesRegex(RuntimeDatabaseError, "cannot be an isolated"):
            resolve_isolated_candidate(
                self.fixture.database,
                installed=self.installed,
            )

        candidate = self.temp / "candidate.sqlite3"
        candidate.write_bytes(b"isolated")
        os.chmod(candidate, 0o600)
        access = resolve_isolated_candidate(candidate, installed=self.installed)
        self.assertEqual(access.access_mode, DatabaseAccessMode.ISOLATED_CANDIDATE)
        self.assertIsNone(access.writer_lock)

    def test_formal_read_reports_identity_without_writer_lock(self) -> None:
        installed_read = resolve_installed_database_access(
            "formal_read",
            database=self.fixture.database,
            project_root=self.fixture.project,
            environ={"DCAR_V8_DB": str(self.fixture.database)},
            installed=self.installed,
        )
        self.assertEqual(
            installed_read.health_identity()["access_mode"], "formal_read"
        )

        replica = self.temp / "replica.sqlite3"
        replica.write_bytes(b"snapshot")
        os.chmod(replica, 0o600)

        access = resolve_read_only_replica(replica)

        self.assertEqual(access.health_identity()["access_mode"], "formal_read")
        self.assertIsNone(access.writer_lock)

    def test_read_only_replica_accepts_root_owned_group_read_database(self) -> None:
        replica = self.temp / "root-owned-replica.sqlite3"
        replica.write_bytes(b"snapshot")
        os.chmod(replica, 0o640)
        original_stat = Path.stat

        def root_owned_stat(
            path: Path, *, follow_symlinks: bool = True
        ) -> os.stat_result:
            value = original_stat(path, follow_symlinks=follow_symlinks)
            if os.fspath(path) != os.fspath(replica):
                return value
            fields = list(value)
            fields[4] = 0
            return os.stat_result(fields)

        with patch.object(Path, "stat", root_owned_stat):
            access = resolve_read_only_replica(replica)

        self.assertEqual(access.database_identity.uid, 0)
        self.assertEqual(access.database_identity.mode, 0o640)
        self.assertEqual(access.access_mode, DatabaseAccessMode.FORMAL_READ)

    def test_read_only_replica_rejects_untrusted_owner_and_unsafe_file(self) -> None:
        replica = self.temp / "replica-safety.sqlite3"
        replica.write_bytes(b"snapshot")
        os.chmod(replica, 0o640)
        original_stat = Path.stat

        def untrusted_owned_stat(
            path: Path, *, follow_symlinks: bool = True
        ) -> os.stat_result:
            value = original_stat(path, follow_symlinks=follow_symlinks)
            if os.fspath(path) != os.fspath(replica):
                return value
            fields = list(value)
            fields[4] = os.geteuid() + 1
            return os.stat_result(fields)

        with (
            patch.object(Path, "stat", untrusted_owned_stat),
            self.assertRaisesRegex(RuntimeDatabaseError, "current user or root"),
        ):
            resolve_read_only_replica(replica)

        alias = self.temp / "replica-symlink.sqlite3"
        alias.symlink_to(replica)
        with self.assertRaisesRegex(RuntimeDatabaseError, "non-symlink"):
            resolve_read_only_replica(alias)

        hardlink = self.temp / "replica-hardlink.sqlite3"
        os.link(replica, hardlink)
        with self.assertRaisesRegex(RuntimeDatabaseError, "single-link"):
            resolve_read_only_replica(replica)
        hardlink.unlink()

        os.chmod(replica, 0o660)
        with self.assertRaisesRegex(RuntimeDatabaseError, "group/world writable"):
            resolve_read_only_replica(replica)

    def test_formal_writer_database_still_rejects_root_owner(self) -> None:
        original_stat = Path.stat

        def root_owned_stat(
            path: Path, *, follow_symlinks: bool = True
        ) -> os.stat_result:
            value = original_stat(path, follow_symlinks=follow_symlinks)
            if os.fspath(path) != os.fspath(self.fixture.database):
                return value
            fields = list(value)
            fields[4] = 0
            return os.stat_result(fields)

        with (
            patch.object(Path, "stat", root_owned_stat),
            self.assertRaisesRegex(RuntimeDatabaseError, "owned by the current user"),
        ):
            resolve_installed_database_access(
                "writer",
                database=self.fixture.database,
                project_root=self.fixture.project,
                environ=self.fixture.environment,
                installed=self.installed,
            )

    def test_writer_rejects_missing_explicit_environment_before_lock(self) -> None:
        before = self.fixture.writer_lock.stat()
        for missing in (
            "DCAR_V8_DB",
            "DCAR_PROJECT_ROOT",
            "DCAR_WRITER_LOCK",
        ):
            with self.subTest(missing=missing):
                environment = self.fixture.environment.copy()
                environment.pop(missing)
                with self.assertRaises(RuntimeDatabaseError):
                    resolve_installed_database_access(
                        "writer",
                        database=self.fixture.database,
                        project_root=self.fixture.project,
                        environ=environment,
                        installed=self.installed,
                    )
        after = self.fixture.writer_lock.stat()
        self.assertEqual((before.st_dev, before.st_ino), (after.st_dev, after.st_ino))
        self.assertEqual(self.fixture.writer_lock.read_bytes(), b"")


class ApiInstalledWriterLeaseTest(unittest.IsolatedAsyncioTestCase):
    async def test_writer_holds_installed_lease_without_scheduler(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            fixture = InstalledRuntimeFixture(Path(temp).resolve(strict=True))
            # Unlike the path-only resolver cases, API startup also starts its
            # real durable control executor. Give that worker a valid empty DB
            # instead of generating an unobserved background "not a database".
            from v8.storage import connect, initialize_database
            fixture.database.write_bytes(b"")
            with connect(fixture.database) as connection:
                initialize_database(connection)
            installed = load_installed_writer_contract(home=fixture.home)
            assert installed is not None
            access = resolve_installed_database_access(
                "writer",
                database=fixture.database,
                project_root=fixture.project,
                environ=fixture.environment,
                installed=installed,
            )
            config = api.ApiConfig(
                db_path=fixture.database,
                reports_root=fixture.project / "reports",
                legacy_db_path=fixture.project / "legacy.sqlite3",
                operator_freeze_lock=fixture.project / "freeze.lock",
                writer_lock=fixture.writer_lock,
                scheduler_enabled=False,
                runtime_access_mode=DatabaseAccessMode.WRITER,
                project_root=fixture.project,
            )
            first = api.create_app(config)
            second = api.create_app(config)

            @asynccontextmanager
            async def runtime(_application):
                yield

            with (
                patch.object(
                    api,
                    "resolve_installed_database_access",
                    return_value=access,
                ),
                patch.object(api, "_lifespan_runtime", runtime),
            ):
                async with api.lifespan(first):
                    lock = first.state.writer_lock
                    self.assertEqual(set(lock), {"path", "device", "inode", "held"})
                    self.assertTrue(lock["held"])
                    self.assertEqual(
                        first.state.runtime_database_identity,
                        access.health_identity(),
                    )
                    with self.assertRaisesRegex(RuntimeDatabaseError, "already held"):
                        async with api.lifespan(second):
                            pass
                self.assertFalse(first.state.writer_lock["held"])


if __name__ == "__main__":
    unittest.main()
