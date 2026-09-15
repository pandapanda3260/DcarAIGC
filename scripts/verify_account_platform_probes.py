#!/usr/bin/env python3
"""Verify local original probe bytes and platform contracts without network/DB."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import hashlib
import json
import os
from pathlib import Path
import socket
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src/dcar_eval'))


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence', type=Path, required=True)
    p.add_argument('--report', type=Path, required=True)
    a = p.parse_args()
    from v8 import kuaishou_adapter as ks, wechat_channels_adapter as wx
    from v8.capture import CaptureError
    def deny(*args, **kwargs):
        raise AssertionError('Offline evidence verification cannot access network')
    socket.socket.connect = socket.socket.connect_ex = socket.create_connection = socket.getaddrinfo = deny
    root = a.evidence.resolve(strict=True)
    rows, pages, candidates, confirmations, profiles = [], defaultdict(list), {}, {}, {}
    loaded = []
    for path in sorted(root.glob('*.response.json')):
        envelope = json.loads(path.read_bytes())
        entity_path = root / envelope['entity_file']
        assert entity_path.parent == root and not entity_path.is_symlink()
        entity = entity_path.read_bytes()
        receipt = envelope['receipt']
        assert hashlib.sha256(entity).hexdigest() == receipt['entity_sha256']
        assert len(entity) == receipt['entity_bytes']
        assert json.loads(entity) == envelope['payload']
        loaded.append((path, envelope))
    uid_by_row = {d['source_directory_row_id']: d['params'].get('user_id', d['params'].get('username'))
                  for _, d in loaded if d['params'].get('user_id') or d['params'].get('username')}
    for path, d in loaded:
        route, params, payload, row_id = d['route'], d['params'], d['payload'], d['source_directory_row_id']
        result = {'directory_row_id': row_id, 'route': route, 'evidence_file': path.name,
                  'http_status': d['http_status'], 'original_entity_verified': True}
        rows.append(result)
        if d['http_status'] != 200:
            result.update(status='request_rejected', reason='http_' + str(d['http_status']))
            continue
        try:
            adapter = ks if '/kuaishou/' in route else wx
            if route.endswith('/fetch_channel_id_to_username'):
                parsed = wx.parse_reference(payload, params['channel_id'])
                candidates[row_id] = parsed
            elif route.endswith('/fetch_channel_info'):
                parsed = wx.parse_channel_info(payload, params['username'])
                confirmations[row_id] = parsed
            elif route.endswith(('/fetch_one_user_v2', '/fetch_user_profile')):
                parsed = adapter.parse_profile(payload, uid_by_row[row_id])
                profiles[row_id] = parsed
            elif route.endswith(('/fetch_user_post_v2', '/fetch_user_videos')):
                parsed = adapter.parse_discovery(payload, uid_by_row[row_id])
                pages[row_id].append((d, parsed))
                result.update(items=len(parsed['items']), has_more=parsed['has_more'])
            else:
                parsed = adapter.parse_stage('detail', params.get('photo_id', params.get('object_id')), payload,
                                             expected_uid=uid_by_row[row_id])
                result['metric_statuses'] = {k: v['status'] for k, v in parsed['metrics']['_field_status'].items()}
            result['status'] = 'validated'
        except CaptureError as error:
            result.update(status='provider_business_failure', reason=error.error_code)
    chains = []
    for row_id, candidate in candidates.items():
        info, profile = confirmations.get(row_id), profiles.get(row_id)
        matched = bool(info and profile and info['uid'] == profile['uid'] == candidate['uid']
                       and info['channel_id'] == candidate['channel_id'])
        chains.append({'directory_row_id': row_id, 'resolver_reverse_profile_match': matched})
    coverage = []
    for row_id, values in pages.items():
        remaining = list(values)
        cursor = ''
        ordered = []
        while remaining:
            matches = [(d, page) for d, page in remaining if d['params'].get('pcursor', d['params'].get('last_buffer', '')) == cursor]
            assert len(matches) == 1, 'Missing or duplicate page in saved successful cursor chain'
            d, page = matches[0]
            remaining.remove((d, page))
            ordered.append(page)
            if not page['has_more']:
                assert not remaining
                break
            assert page['next_cursor'] not in ('', None, cursor), 'Non-advancing page'
            cursor = page['next_cursor']
        keys = [item['platform_content_id'] for page in ordered for item in page['items']]
        coverage.append({'directory_row_id': row_id, 'validated_pages': len(ordered),
                         'unique_contents': len(set(keys)), 'terminal_observed': not ordered[-1]['has_more']})
    failures = [json.loads(path.read_text()) for path in root.glob('*.failure.json')]
    report = {'verification': 'PASS', 'network_calls': 0, 'database_writes': 0,
              'attempts': len(list(root.glob('*.attempt.json'))), 'verified_original_entities': len(loaded),
              'response_states': dict(Counter(row['status'] for row in rows)),
              'transport_failures': len(failures), 'account_identity_chains': chains, 'pages': coverage,
              'responses': rows, 'all_accounts_live_verified': False,
              'scope': 'Bounded platform integration samples; not a full 203-account crawl or monitoring acceptance'}
    fd = os.open(a.report, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)
        handle.write('\n')
    print(json.dumps({k:v for k,v in report.items() if k != 'responses'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
