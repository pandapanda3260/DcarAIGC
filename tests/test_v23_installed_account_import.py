"""A real schema23 frozen-source import authority and transaction fixture."""
import base64
from dataclasses import replace
import importlib.util
import os
from pathlib import Path
import plistlib
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from tests import test_four_platform_flow_release as flow_fixtures
from tests.test_account_summary_import_cli import FIXTURE_XLSX
from v8 import runtime_database

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location('installed_account_import_v23_cli',ROOT/'scripts/import_installed_account_summary.py')
cli=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(cli)


class InstalledAccountImportV23Test(unittest.TestCase):
    def test_actual_successor_chain_and_import_trigger_authorizer(self):
        case=flow_fixtures.FourPlatformFlowReleaseTest();case.setUp();self.addCleanup(case.doCleanups)
        migration=case.installer.install(case.args);plan=case.prepare(migration)
        payload=plistlib.loads(Path(plan['next_plist']['path']).read_bytes())
        case.installed_path.write_bytes(plistlib.dumps(payload));case.installed_path.chmod(0o600)
        installed=replace(case.installed,payload=payload,plist_path=case.installed_path,home=case.f.home,program=case.source/'deploy/macos/run_writer_worker.sh')
        xlsx=case.f.root/'summary23.xlsx';xlsx.write_bytes(base64.b64decode(FIXTURE_XLSX))
        def args(number,apply):
            return SimpleNamespace(database=case.f.db,project_root=case.f.project,xlsx=xlsx,metadata=None,
                backup=case.f.root/f'summary23-backup-{number}.sqlite3',report=case.f.root/f'summary23-report-{number}.json',apply=apply)
        with patch.object(cli,'ROOT',case.source),patch.object(runtime_database,'load_installed_writer_contract',return_value=installed),patch.dict(os.environ,payload['EnvironmentVariables']):
            original=cli._protected(case.f.connection)
            dry=cli.run_import(args(1,False))
            self.assertEqual(dry['status'],'rolled_back');self.assertEqual(cli._protected(case.f.connection),original)
            self.assertIn('four_platform_flow_proof_sha256',dry['authority'])
            self.assertEqual(dry['authority']['source_tree'],case.tree_ref)
            done=cli.run_import(args(2,True))
            self.assertEqual(done['status'],'applied')
            self.assertGreater(done['writes'],0)
            saved=cli._protected(case.f.connection)
            self.assertGreater(saved['catalog_revision']['revision'],original['catalog_revision']['revision'])
            self.assertEqual(saved['catalog_revision']['projection_depth'],0)
            replay=cli.run_import(args(3,True))
            self.assertEqual(replay['database_writes'],0);self.assertEqual(cli._protected(case.f.connection),saved)
            case.f.connection.set_authorizer(cli.authorize_import)
            with self.assertRaises(Exception):case.f.connection.execute('UPDATE capture_catalog_revision SET revision=revision+1')
            case.f.connection.set_authorizer(None)
