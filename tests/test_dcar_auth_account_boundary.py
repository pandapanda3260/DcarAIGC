from __future__ import annotations

import sqlite3
import unittest
from unittest.mock import patch

from tests import test_dcar_auth_gateway as fixture_module

gateway = fixture_module.auth_gateway

class AccountGatewayBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.replace_gateway = patch.object(fixture_module, 'auth_gateway', gateway)
        self.replace_gateway.start()
        self.addCleanup(self.replace_gateway.stop)
        self.fixture = fixture_module.DcarAuthGatewayTestCase()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def login_role(self, client, config, role):
        username = 'operator' if role == 'operator' else 'boundary_' + role
        if role != 'operator':
            self.fixture._seed_user(config, username, role=role)
        self.fixture._login_as(client, config.base_path, username)
        return username

    def assert_denied(self, response, status=403, code="account_admin_required"):
        self.assertEqual(response.status_code, status, response.text)
        self.assertEqual(response.headers.get('cache-control'), 'no-store')
        if status == 403:
            self.assertEqual(response.json()['code'], code)
            self.assertNotIn('upstream', response.json())

    def test_operator_cannot_read_full_account_api_directly(self):
        with self.fixture._client('') as (client, config):
            self.login_role(client, config, 'operator')
            response = client.post('/api/v8/accounts/search', json={'scope': 'all'}, headers={'X-Dcar-Role': 'admin'})
            self.assert_denied(response)

    def test_account_page_flight_and_data_denied_for_nonadmins(self):
        for base in ('', '/dcar'):
            for role in ('operator', 'new_user'):
                with self.subTest(base=base, role=role), self.fixture._client(base) as (client, config):
                    if role == 'new_user' and self.fixture._store_for(config).get_user('boundary_new_user'):
                        self.fixture._login_as(client, base, 'boundary_new_user')
                    else:
                        self.login_role(client, config, role)
                    for path in ('/accounts', '/accounts/', '/accounts/douyin-authorization'):
                        response = client.get(base + path, headers={'Accept': 'text/html'}, follow_redirects=False)
                        self.assert_denied(response, 303)
                        self.assertEqual(response.headers['location'], base + '/overview')
                    for path, headers in (
                        ('/accounts.rsc', {}), ('/accounts/douyin-authorization.rsc', {}),
                        ('/accounts?_rsc=fixture', {'Accept': 'text/html'}),
                        ('/accounts', {'RSC': '1', 'Accept': 'text/x-component'}),
                        ('/accounts', {'Next-Router-Prefetch': '1'}),
                    ):
                        self.assert_denied(client.get(base + path, headers=headers, follow_redirects=False), code="account_admin_required")

    def test_account_management_api_method_and_normalization_matrix(self):
        with self.fixture._client('/dcar', douyin_enabled=True) as (client, config):
            self.login_role(client, config, 'operator')
            paths = ('/api/v8/accounts', '/api/v8/accounts/search', '/api/v8/accounts/export',
                     '/api/v8/accounts/123', '/api/v8/account-roster', '/api/v8/account-roster/import',
                     '/api/douyin/authorizations', '/api/douyin/authorization-statuses',
                     '/api/douyin/authorizations/reauthorize', '/api/douyin/authorizations/unbind')
            for method in ('GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS'):
                for path in paths:
                    with self.subTest(method=method, path=path):
                        self.assert_denied(client.request(method, '/dcar' + path, json={}, follow_redirects=False))
            head = client.head('/dcar/api/v8/accounts/search')
            self.assertEqual(head.status_code, 403)
            for path in ('/api/v8/%61ccounts/search', '/api/v8/contents/%2e%2e/accounts/search',
                         '/api/v8/contents/%252e%252e/accounts/search', '/contents/%2e%2e/accounts.rsc',
                         '/api/v8/accounts/%7f'):
                with self.subTest(path=path):
                    self.assert_denied(client.get('/dcar' + path, follow_redirects=False))

    def test_admins_keep_access_and_session_role_downgrade_takes_effect(self):
        for role in ('admin', 'superadmin'):
            with self.subTest(role=role), self.fixture._client('/dcar') as (client, config):
                username = self.login_role(client, config, role)
                for method, path in (('GET', '/accounts'), ('GET', '/accounts.rsc'), ('POST', '/api/v8/accounts/search'),
                                     ('POST', '/api/v8/accounts/export'), ('POST', '/api/v8/account-roster/import')):
                    response = client.request(method, '/dcar' + path, follow_redirects=False)
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertIn(response.json()['upstream'], ('web', 'api'))
                with sqlite3.connect(config.session_db_path) as connection:
                    connection.execute('UPDATE auth_users SET role=? WHERE username=?', ('operator', username))
                self.assert_denied(client.post('/dcar/api/v8/accounts/search', json={}))

    def test_new_user_shell_has_no_accounts_navigation_and_other_data_stays_blocked(self):
        with self.fixture._client('') as (client, config):
            self.login_role(client, config, 'new_user')
            response = client.get('/overview', headers={'Accept': 'text/html'})
            self.assertEqual(response.status_code, 200)
            self.assertNotIn('href="/accounts"', response.text)
            self.assertIn('href="/contents"', response.text)
            self.assertEqual(client.post('/api/v8/contents/search', json={}).status_code, 403)
            self.assertNotIn('/accounts', gateway.NEW_USER_PAGES)

    def test_content_filters_exports_and_oauth_completion_are_not_account_management(self):
        with self.fixture._client('', douyin_enabled=True) as (client, config):
            self.login_role(client, config, 'operator')
            for method, path in (('GET', '/contents'), ('GET', '/overview'), ('POST', '/api/v8/contents/search'),
                                 ('POST', '/api/v8/contents/export'), ('GET', '/api/v8/accounts-other'),
                                 ('GET', '/api/v8/account-roster-other'),
                                 ('GET', gateway.DOUYIN_CALLBACK_PATH)):
                response = client.request(method, path, follow_redirects=False)
                self.assertEqual(response.status_code, 200, (method, path, response.text))
                self.assertIn('upstream', response.json())

    def test_anonymous_login_contract_and_roleless_bypass_fail_closed(self):
        with self.fixture._client('') as (client, config):
            self.assertEqual(client.post('/api/v8/accounts/search', json={}).status_code, 401)
        with self.fixture._client('', bypass_auth=True) as (client, config):
            self.assert_denied(client.post('/api/v8/accounts/search', json={}))
            self.assert_denied(client.get('/accounts', headers={'Accept': 'text/html'}, follow_redirects=False), 303)
            self.assertEqual(client.get('/contents').status_code, 200)

if __name__ == '__main__':
    unittest.main()
