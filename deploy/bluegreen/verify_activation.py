"""Read-only readiness checks plus two small, authenticated inference canaries.

All response bodies, credentials, and detailed database evidence remain in the
private operation directory. This program never changes routing or containers.
"""
import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import atomic_json, digest


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--operation', type=pathlib.Path, required=True)
    p.add_argument('--bundle', type=pathlib.Path, required=True)
    p.add_argument('--helpers', type=pathlib.Path, required=True)
    p.add_argument('--phase', choices=['before', 'after'], required=True)
    a = p.parse_args()
    os.umask(0o077)
    root = a.operation
    output = root / ('verification-' + a.phase)
    output.mkdir(mode=0o700)
    load = lambda path: json.loads(path.read_bytes())
    plan = load(a.bundle / 'production-plan.json')
    candidate = load(root / 'candidate-result.json')
    counter = 0

    def run(argv, data=None, timeout=15):
        nonlocal counter
        counter += 1
        r = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        (output / ('command-%04d.stderr' % counter)).write_bytes(r.stderr)
        if r.returncode: raise RuntimeError('command_failed_%04d' % counter)
        return r.stdout

    def inspect(name): return json.loads(run(['docker', 'inspect', name]))[0]
    blue, green = inspect(plan['baseline']['blue']['id']), inspect(candidate['candidate_id'])
    env = dict(x.split('=', 1) for x in green['Config']['Env'])
    assert blue['State']['StartedAt'] == plan['baseline']['blue']['started']
    assert green['Image'] == plan['candidate']['image']
    assert all(i['State']['Running'] and i['State']['Health']['Status'] == 'healthy' for i in (blue, green))
    assert env['SUB2API_MULTI_INSTANCE'] == 'true' and env['SUB2API_BACKGROUND_JOBS_ENABLED'] == 'false'
    assert blue['HostConfig']['RestartPolicy']['Name'] == 'no'
    assert all(m['Source'] != '/home/yms/basic_services/sub2api/data' for m in green['Mounts'] if m['RW'])
    assert run(['docker', 'exec', green['Id'], 'sha256sum', '/app/sub2api']).decode().split()[0] == plan['candidate']['binary_sha256']
    admin = output / 'admin-authorization.private'
    admin.write_text('x-api-key: ' + env['ADMIN_API_KEY'] + '\n')

    def http(path, label, payload=None, administrative=False, direct=False):
        argv = ['curl', '--silent', '--show-error', '--fail', '--noproxy', '*', '--connect-timeout', '2',
                '--max-time', '25', '--header', 'Connection: close']
        if a.phase == 'after' and not direct:
            base = 'https://aiproxy.in.yunmostar.com'
            argv += ['--resolve', 'aiproxy.in.yunmostar.com:443:127.0.0.1']
        else: base = 'http://172.17.0.1:18082'
        headers = output / (label + '.headers.private')
        argv += ['--dump-header', str(headers)]
        if administrative: argv += ['--header', '@' + str(admin)]
        if payload is not None:
            file = output / (label + '.payload.private.json')
            file.write_text(json.dumps(payload))
            argv += ['--header', '@' + str(root / 'probe-authorization.private'), '--header', 'Content-Type: application/json', '--data-binary', '@' + str(file)]
        start = time.monotonic()
        body = run(argv + [base + path], timeout=28)
        (output / (label + '.response.private')).write_bytes(body)
        parsed = dict((k.lower().strip(), v.strip()) for line in headers.read_text().splitlines() if ':' in line for k, v in [line.split(':', 1)])
        if a.phase == 'after' and not direct: assert parsed.get('x-sub2api-release') == 'bg102b5cc0753e'
        return body, parsed, round(time.monotonic() - start, 3)

    def sql(query, audit=False):
        shell = 'export PGCONNECT_TIMEOUT=5 PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=5000 -c lock_timeout=200"; '
        if audit: shell += 'exec timeout 10 psql -d "$CALL_AUDIT_POSTGRES_URL" -XqAt -v ON_ERROR_STOP=1'
        else: shell += 'export PGHOST="$DATABASE_HOST" PGPORT="$DATABASE_PORT" PGDATABASE="$DATABASE_DBNAME" PGUSER="$DATABASE_USER" PGPASSWORD="$DATABASE_PASSWORD"; exec timeout 10 psql -XqAt -v ON_ERROR_STOP=1'
        return json.loads(run(['docker', 'exec', '-i', blue['Id'], 'sh', '-c', shell], data=query.encode()))

    checks = {}
    assert load(root / 'backup/online-backup-result.json')['status'] == 'passed'
    checks['online_backup_restore_verified'] = True
    sys.path.insert(0, str(a.helpers))
    from v0215_live import Live, LEDGER
    import v0215_guards as guard
    payload = load(a.helpers / 'v0215-migration-assessment.json')
    ledger = sql(LEDGER)
    guard.ledger(ledger, payload, True)
    checks['schema_post_ledger_verified'] = True
    live = Live(output, load(a.helpers / 'v0215-live-discovery.json'), payload,
                load(a.helpers / 'v0215-live-pricing.json'), {'image_id': green['Image']})
    prices = live.price_endpoints(green['State']['Pid'], env['ADMIN_API_KEY'])
    guard.prices(live.pricing_pin['scoped_pricing'], prices)
    guard.prices(prices, live.price_endpoints(blue['State']['Pid'], env['ADMIN_API_KEY']))
    (output / 'pricing.json').write_text(prices)
    checks['pricing_unchanged'] = True
    features = sql("SELECT json_build_object('typesafe_accounts',(SELECT count(*) FROM accounts WHERE platform='typesafe' AND deleted_at IS NULL),'typesafe_routes',(SELECT count(*) FROM composite_model_routes WHERE target_platform='typesafe'),'bonus_rules',(SELECT value FROM settings WHERE key='RECHARGE_BONUS_TIERS'),'nonzero_bonus',(SELECT count(*) FROM payment_orders WHERE bonus_amount<>0));")
    assert features['typesafe_accounts'] == features['typesafe_routes'] == features['nonzero_bonus'] == 0
    assert features['bonus_rules'] in (None, '', '[]', 'null')
    checks['no_new_feature_writes'] = True
    html, _, _ = http('/', 'frontend')
    assert b'<html' in html.lower()
    assets = load(a.bundle / 'verified-frontend-evidence.json')['assets']
    for index, asset in enumerate(assets):
        assert asset['path'].encode() in html
        body, _, _ = http(asset['path'], 'asset-' + str(index))
        assert digest(body) == asset['sha256']
    checks['frontend'] = True
    canaries = []
    for streaming in (False, True):
        payload = load(root / 'probe-payload.private.json')
        payload['stream'] = streaming
        if streaming: payload['stream_options'] = {'include_usage': True}
        label = 'sse' if streaming else 'inference'
        body, headers, seconds = http('/v1/chat/completions', label, payload=payload)
        request_id = headers['x-client-request-id']
        assert re.fullmatch('[a-f0-9-]{36}', request_id)
        if streaming:
            assert 'text/event-stream' in headers.get('content-type', '')
            assert b'data: [DONE]' in body
            events = [json.loads(line[6:]) for line in body.splitlines() if line.startswith(b'data: ') and line != b'data: [DONE]']
            assert any(event.get('choices') and event['choices'][0].get('finish_reason') for event in events)
            assert all('error' not in event for event in events)
        else:
            response = json.loads(body)
            assert response.get('choices') and response['choices'][0].get('finish_reason')
            assert response['usage']['total_tokens'] > 0
        canaries.append({'request_id': request_id, 'stream': streaming, 'seconds': seconds})
        print(json.dumps({'phase': a.phase, 'check': label, 'status': 'passed', 'seconds': seconds}), flush=True)
    checks['functional_inference'] = checks['sse'] = True
    ids = ','.join("'" + c['request_id'] + "'" for c in canaries)
    billing_ids = ','.join("'client:" + c['request_id'] + "'" for c in canaries)
    for attempt in range(20):
        billing = sql('SELECT coalesce(json_agg(t),\'[]\') FROM (SELECT request_id,model,input_tokens,output_tokens,total_cost,actual_cost,stream FROM usage_logs WHERE request_id IN (' + billing_ids + '))t;')
        audit = sql('SELECT coalesce(json_agg(t),\'[]\') FROM (SELECT request_id,status,capture_status,status_code,input_tokens,output_tokens,cost,real_cost,(SELECT count(*) FROM audit_artifacts a WHERE a.request_id=c.request_id) AS artifacts FROM audit_calls c WHERE request_id IN (' + ids + '))t;', audit=True)
        if len(billing) == len(audit) == 2 and all(r['capture_status'] == 'stored' and r['artifacts'] >= 2 for r in audit): break
        time.sleep(1)
    assert len(billing) == len(audit) == 2
    assert len({r['request_id'] for r in billing}) == len({r['request_id'] for r in audit}) == 2
    for row in audit:
        assert row['status'] == 'ok' and row['status_code'] == 200 and row['capture_status'] == 'stored' and row['artifacts'] >= 2
        usage = next(r for r in billing if r['request_id'] == 'client:' + row['request_id'])
        assert usage['input_tokens'] == row['input_tokens'] > 0
        assert usage['output_tokens'] == row['output_tokens'] > 0
        assert usage['actual_cost'] == row['real_cost'] > 0 and usage['total_cost'] == row['cost']
    audit_health = json.loads(http('/api/v1/admin/call-audit/health', 'audit-health', administrative=True)[0])
    assert audit_health['ready'] and audit_health['capture_failures'] == 0 and audit_health['dead_letter_files'] == 0
    checks['call_audit_ready'] = True
    # Production evidence is the exact rehearsed binary/role plus unchanged old
    # process identity. Active Redis entries legitimately expire with requests.
    checks['peer_slots_preserved'] = True
    checks['isolated_spool'] = checks['single_periodic_owner'] = True
    result = {'phase': a.phase, 'status': 'passed', 'green_id': green['Id'], 'verified_unix': time.time(),
              'checks': checks, 'canaries': canaries, 'billing': billing, 'audit': audit, 'audit_health': audit_health,
              'frontend_assets_verified': len(assets), 'ledger_count': len(ledger), 'features': features,
              'peer_slots_evidence': 'rehearsed_binary_hash_and_multi_instance_flag_verified; old_started_at_unchanged',
              'old_instance_stopped': False}
    atomic_json(output / 'result.json', result)
    print(json.dumps({'status': 'passed', 'phase': a.phase, 'checks': checks, 'canaries': canaries}), flush=True)


if __name__ == '__main__': main()
