"""Only the changed parent/child boundary; no service, SQL or provider calls."""
from pathlib import Path
import importlib.util
import json
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

PATH = Path(__file__).resolve().parents[1] / 'scripts/deploy_discovery_metrics_release.py'
spec = importlib.util.spec_from_file_location('discovery_overview_deploy_test', PATH)
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DiscoveryMetricsDeployTests(unittest.TestCase):
    def test_original_guarded_install_is_inherited(self):
        self.assertEqual(deploy.EXPECTED_PARENT_BUILD_SHA256,
            'bdc2baee628b10ca32d5603b17eed407e88d1e54d189bd3a612adfe6e09acc8f')
        for name in ('run', 'stop', 'replace', 'backup', 'rollback', 'wait_health', 'verify_bootstrap'):
            self.assertIs(getattr(deploy.Deployer, name), getattr(deploy.base.Deployer, name))

    def test_earlier_daily_parent_is_rejected_before_build_read_or_service_action(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp).resolve() / 'proposal.json'
            path.write_text(json.dumps({'contract': 'discovery-metrics-install-proposal-v1',
                'status': 'prepared', 'parent_build': {'sha256': 'e5d53ba78459301648fbe080aa26beff810b135edd2f91021ea9bbc1f51931e8'},
                'child_build': {'sha256': '1' * 64}, 'database_writes': 0, 'provider_calls': 0,
                'services_changed': False, 'paid_gates_reopened': False,
                'schema_migration_repeated': False, 'business_scope_change': 'none'}))
            path.chmod(0o600)
            instance = object.__new__(deploy.Deployer)
            instance.args = SimpleNamespace(proposal=path, expected_child_build='1' * 64)
            instance.stop = Mock(side_effect=AssertionError('service call forbidden'))
            with patch.object(deploy.base, 'build_at', side_effect=AssertionError('build read forbidden')):
                with self.assertRaisesRegex(ValueError, 'Unexpected overview proposal'):
                    instance.preflight()
            instance.stop.assert_not_called()
