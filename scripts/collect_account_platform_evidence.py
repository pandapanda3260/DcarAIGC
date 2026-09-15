#!/usr/bin/env python3
"""Collect bounded public platform responses into private local evidence files.

The input is an explicit request manifest, not a scheduler plan. No database is
opened and no installation, paid-send gate or historical receipt is changed.
Default is a preview; --execute sends each previously unattempted request once.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'src'), str(ROOT / 'src/dcar_eval')]
ROUTES = {
    '/api/v1/kuaishou/app/fetch_one_user_v2': 'GET',
    '/api/v1/kuaishou/app/fetch_user_post_v2': 'GET',
    '/api/v1/kuaishou/app/fetch_one_video': 'GET',
    '/api/v1/wechat_channels/v2/fetch_channel_id_to_username': 'POST',
    '/api/v1/wechat_channels/v2/fetch_channel_info': 'POST',
    '/api/v1/wechat_channels/v2/fetch_user_profile': 'POST',
    '/api/v1/wechat_channels/v2/fetch_user_videos': 'POST',
    '/api/v1/wechat_channels/v2/fetch_video_detail': 'POST',
}


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def validate_request(row):
    route, params = row.get('route'), row.get('params')
    if route not in ROUTES or not isinstance(params, dict):
        raise ValueError('Unsupported public request')
    if '/kuaishou/' in route:
        field = 'photo_id' if route.endswith('/fetch_one_video') else 'user_id'
        pattern = r'[0-9A-Za-z]+' if field == 'photo_id' else r'[0-9]{1,24}'
    else:
        field = ('channel_id' if route.endswith('/fetch_channel_id_to_username') else
                 'object_id' if route.endswith('/fetch_video_detail') else 'username')
        pattern = {'channel_id': r'sph[A-Za-z0-9_-]{1,61}', 'object_id': r'[0-9]{1,32}',
                   'username': r'v2_[0-9a-fA-F]+@finder'}[field]
    value = params.get(field)
    if not isinstance(value, str) or len(value) > 256 or re.fullmatch(pattern, value) is None:
        raise ValueError('Public request requires an exact supported text identifier')
    return route, params


def save(path, body):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(body)
        stream.flush()
        os.fsync(stream.fileno())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--requests', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--max-requests', type=int, default=10)
    p.add_argument('--max-usd', type=float, default=1.0)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    rows = json.loads(a.requests.read_text())
    if not isinstance(rows, list) or not rows or not 1 <= a.max_requests <= 300 or not 0 < a.max_usd <= 5:
        p.error('Require a nonempty explicit manifest, 1-300 calls and at most USD 5')
    output = a.output.absolute()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if output != output.resolve(strict=True) or output.is_symlink():
        p.error('Output must not traverse symlinks')
    prepared = {}
    for row in rows:
        try:
            route, params = validate_request(row)
        except ValueError as error:
            p.error(str(error))
        signature = hashlib.sha256(canonical({'route': route, 'params': params})).hexdigest()
        prepared.setdefault(signature, row)
    # A write-ahead record also fences uncertain/interrupted attempts. Replays
    # must never automatically repeat a request whose billing is unknown.
    existing = list(output.glob('*.attempt.json'))
    pending = [(r, s) for s, r in prepared.items() if not (output / (s + '.attempt.json')).exists()]
    if len(pending) + len(existing) > a.max_requests or (len(pending) + len(existing)) * .01 > a.max_usd + 1e-9:
        p.error('Cumulative request or conservative USD 0.01/call ceiling exceeded')
    print(json.dumps({'planned': len(rows), 'pending': len(pending), 'already_attempted': len(rows)-len(pending),
                      'cumulative_upper_bound_usd': round((len(pending)+len(existing))*.01, 2), 'execute': a.execute}), flush=True)
    if not a.execute or not pending:
        return
    from tikhub_config import load_tikhub_api_key, resolve_tikhub_transport_manifest
    from v8.provider_transport import request_json
    key, manifest = load_tikhub_api_key(), resolve_tikhub_transport_manifest()
    if urllib.parse.urlsplit(manifest['api_base']).hostname != 'api.tikhub.io':
        raise ValueError('Public evidence probe requires the configured official TikHub endpoint')
    failed = 0
    for row, signature in pending:
        route, params = row['route'], row['params']
        stamp = datetime.now(timezone.utc).isoformat()
        save(output / (signature + '.attempt.json'), canonical({'contract': 'local-public-platform-probe-v1',
             'captured_at': stamp, 'request': row, 'upper_bound_usd': .01}))
        headers = {'Authorization': 'Bearer '+key, 'Accept': 'application/json', 'User-Agent': 'DCar-Insight-v8/1.0'}
        method, url, body = ROUTES[route], manifest['api_base'] + route, None
        if method == 'POST':
            body = canonical(params)
            headers['Content-Type'] = 'application/json'
        else:
            url += '?' + urllib.parse.urlencode(params)
        try:
            response = request_json(urllib.request.Request(url, data=body, headers=headers, method=method),
                route_id=manifest['transport_route_id'], route_generation=manifest['route_generation'],
                http_stack=manifest['http_stack'], timeout=45)
            # Keep original bytes separately; JSON reserialization is not a
            # substitute for an original transport hash.
            save(output / (signature + '.entity.json'), response.entity_body)
            envelope = {'captured_at': datetime.now(timezone.utc).isoformat(), 'route': route, 'params': params,
                        'http_status': response.status, 'payload': response.payload, 'receipt': response.receipt,
                        'entity_file': signature + '.entity.json', 'source_directory_row_id': row.get('directory_row_id')}
            save(output / (signature + '.response.json'), json.dumps(envelope, ensure_ascii=False,
                 separators=(',', ':'), allow_nan=False).encode())
            print(json.dumps({'directory_row_id': row.get('directory_row_id'), 'route': route,
                              'http_status': response.status, 'evidence': signature + '.response.json'}), flush=True)
            if response.status in (401, 402, 403):
                break
            payload = response.payload if isinstance(response.payload, dict) else {}
            data = payload.get('data') if isinstance(payload.get('data'), dict) else {}
            successful = (response.status == 200 and payload.get('code') == 200
                and data.get('ret', 0) == 0 and data.get('result', 1) == 1)
            failed = 0 if successful else failed + 1
        except Exception as error:
            save(output / (signature + '.failure.json'), canonical({'captured_at': datetime.now(timezone.utc).isoformat(),
                'error_type': type(error).__name__, 'error_code': getattr(error, 'error_code', None),
                'http_status': getattr(error, 'http_status', None)}))
            print(json.dumps({'directory_row_id': row.get('directory_row_id'), 'error_type': type(error).__name__}), flush=True)
            failed += 1
        if failed >= 3:
            break
        time.sleep(1.2)


if __name__ == '__main__':
    main()
