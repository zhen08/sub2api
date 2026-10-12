"""Approved production preparation; retains the serving old instance."""
import argparse
import fcntl
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time
import urllib.request

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import atomic_json, digest, require_approval


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', type=pathlib.Path, required=True)
    parser.add_argument('--bundle', type=pathlib.Path, required=True)
    parser.add_argument('--helpers', type=pathlib.Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root = args.operation
    plan = json.loads((args.bundle / 'production-plan.json').read_bytes())
    approval = json.loads((root / 'approval.json').read_bytes())
    require_approval(plan, approval)
    required = {'production_database_migration', 'start_production_candidate', 'temporarily_disable_blue_automatic_restart'}
    if not required <= set(approval['actions']): raise PermissionError('preparation_scope_not_confirmed')
    backup = json.loads((root / 'backup/online-backup-result.json').read_bytes())
    assert backup['status'] == 'passed' and backup['cleanup']['status'] == 'cleaned'
    sys.path.insert(0, str(args.helpers))
    from v0215_live import Live, LEDGER
    import v0215_guards as guard
    counter = 0
    def run(argv, data=None, timeout=15):
        nonlocal counter
        counter += 1
        result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
        (root / ('prepare-%04d.stderr' % counter)).write_bytes(result.stderr)
        if result.returncode: raise RuntimeError('prepare_command_failed_%04d' % counter)
        return result.stdout
    def inspect(identity): return json.loads(run(['docker', 'inspect', identity]))[0]
    load = lambda name: json.loads((args.helpers / name).read_bytes())
    payload = load('v0215-migration-assessment.json')
    live = Live(root, load('v0215-live-discovery.json'), payload, load('v0215-live-pricing.json'),
                {'image_id': plan['candidate']['image']})
    before = live.check_live()
    blue = inspect(plan['baseline']['blue']['id'])
    assert blue['State']['StartedAt'] == plan['baseline']['blue']['started']
    assert blue['HostConfig']['RestartPolicy']['Name'] == 'unless-stopped'
    config = pathlib.Path('/home/yms/basic_services/nginx-reverse-proxy/nginx/conf.d/aiproxy.in.yunmostar.com.conf')
    assert digest(config.read_bytes()) == plan['baseline']['nginx_config_sha256']
    assert digest(run(['docker', 'exec', plan['baseline']['nginx']['id'], 'nginx', '-T'])) == plan['baseline']['nginx_full_config_sha256']
    sql_shell = ('export PGHOST="$DATABASE_HOST" PGPORT="$DATABASE_PORT" PGDATABASE="$DATABASE_DBNAME" '
                 'PGUSER="$DATABASE_USER" PGPASSWORD="$DATABASE_PASSWORD" PGCONNECT_TIMEOUT=5; '
                 'exec timeout -s TERM -k 2 12 psql -XqAt -v ON_ERROR_STOP=1')
    def sql(query, write=False):
        options = '-c default_transaction_read_only=' + ('off' if write else 'on') + ' -c statement_timeout=5000 -c lock_timeout=500 -c idle_in_transaction_session_timeout=5000'
        return run(['docker', 'exec', '-i', '-e', 'PGAPPNAME=bluegreen_23b766b5653a', '-e', 'PGOPTIONS=' + options,
                    blue['Id'], 'sh', '-c', sql_shell], data=query.encode(), timeout=16).decode().strip()
    guard.ledger(json.loads(sql(LEDGER)), payload, False)
    # No new-version-only business data may be introduced during rollback overlap.
    feature_query = "SELECT json_build_object('typesafe_accounts',(SELECT count(*) FROM accounts WHERE platform='typesafe' AND deleted_at IS NULL),'typesafe_routes',(SELECT count(*) FROM composite_model_routes WHERE target_platform='typesafe'),'bonus_rules',(SELECT coalesce(value,'[]') FROM settings WHERE key='RECHARGE_BONUS_TIERS'));"
    features = json.loads(sql(feature_query))
    assert features['typesafe_accounts'] == 0 and features['typesafe_routes'] == 0
    assert features['bonus_rules'] in (None, '', '[]', 'null')
    atomic_json(root / 'feature-baseline.json', features)
    migration = (args.bundle / 'online-migration.proposed.sql').read_bytes()
    assert digest(migration) == plan['proposed']['online_migration_sha256']
    candidate = None
    policy_changed = False
    result = {'status': 'running', 'old_instance_stopped': False}
    with open('/run/lock/sub2api-bluegreen.lock', 'a') as release_lock, open('/var/lib/aiproxy-hourly-policy/lock', 'r') as policy_lock:
        fcntl.flock(release_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(policy_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            print(json.dumps({'phase': 'online_migration'}), flush=True)
            atomic_json(root / 'migration-intent.json', {'sql_sha256': digest(migration), 'before_count': 290, 'started_unix': time.time()})
            started = time.monotonic()
            sql(migration.decode(), write=True)
            elapsed = round(time.monotonic() - started, 3)
            ledger = json.loads(sql(LEDGER)); guard.ledger(ledger, payload, True)
            atomic_json(root / 'migration-result.json', {'status': 'passed', 'seconds': elapsed, 'ledger_count': len(ledger), 'sql_sha256': digest(migration), 'ledger': ledger})
            assert inspect(blue['Id'])['State']['StartedAt'] == blue['State']['StartedAt']
            project = pathlib.Path('/home/yms/basic_services/sub2api/bluegreen/bg102b5cc0753e')
            project.mkdir(parents=True, mode=0o700, exist_ok=False)
            data = project / 'data'; data.mkdir(mode=0o700)
            source = pathlib.Path('/home/yms/basic_services/sub2api/data')
            copied = {}
            for name in ('config.yaml', 'model_pricing.json', 'model_pricing.sha256', 'plugins', 'pages'):
                src, dst = source / name, data / name
                if not src.exists(): continue
                assert not src.is_symlink()
                if src.is_dir():
                    assert not any(p.is_symlink() for p in src.rglob('*'))
                    shutil.copytree(src, dst, ignore=shutil.ignore_patterns('runtime'))
                else:
                    shutil.copy2(src, dst); copied[name] = digest(dst.read_bytes())
            assert copied['model_pricing.json'] == backup['snapshot']['price_sha256']
            env = dict(entry.split('=', 1) for entry in blue['Config']['Env'])
            original_env_digest = digest(json.dumps(env, sort_keys=True, separators=(',', ':')).encode())
            env.update(plan['proposed']['env_additions'])
            assert env['CALL_AUDIT_SPOOL_DIR'] == '/app/data/audit-spool'
            assert env['CALL_AUDIT_ENABLED'] == env['CALL_AUDIT_WORKER_ENABLED'] == 'true'
            assert env['BILLING_INFLIGHT_RESERVATION_ENABLED'] == 'false'
            assert all('\n' not in v and '\r' not in v for v in env.values())
            envfile = project / 'candidate.env'
            envfile.write_text(''.join(k + '=' + v + '\n' for k, v in sorted(env.items())))
            name = 'sub2api-green-0215-bg1'
            compose = {'name': 'sub2api-bluegreen', 'services': {'green': {
                'image': 'zhen-yms/sub2api:0.2.15-bg-102b5cc0753e', 'pull_policy': 'never',
                'container_name': name, 'restart': 'unless-stopped', 'env_file': [str(envfile)],
                'ports': ['172.17.0.1:18082:8080'], 'volumes': [str(data) + ':/app/data'],
                'networks': ['sub2api'], 'extra_hosts': ['host.docker.internal:host-gateway'],
                'security_opt': ['no-new-privileges:true'], 'mem_limit': '4g', 'cpus': 6,
                'logging': {'driver': 'json-file', 'options': {'max-size': '50m', 'max-file': '5', 'compress': 'true'}},
                'labels': {'org.yunmostar.bluegreen.operation': root.name}}},
                'networks': {'sub2api': {'external': True, 'name': 'sub2api'}}}
            atomic_json(project / 'compose.json', compose)
            assert json.loads(run(['docker', 'image', 'inspect', compose['services']['green']['image']]))[0]['Id'] == plan['candidate']['image']
            atomic_json(root / 'candidate-intent.json', {'name': name, 'image': plan['candidate']['image'], 'project': str(project), 'data': str(data), 'source_env_sha256': original_env_digest, 'copied_files': copied})
            run(['docker', 'compose', '-f', str(project / 'compose.json'), 'create', '--pull', 'never', 'green'], timeout=90)
            candidate = inspect(name)
            assert candidate['Image'] == plan['candidate']['image']
            assert candidate['Config']['Labels']['org.yunmostar.bluegreen.operation'] == root.name
            run(['docker', 'update', '--restart=no', blue['Id']]); policy_changed = True
            print(json.dumps({'phase': 'start_candidate_keep_blue_running', 'migration_seconds': elapsed}), flush=True)
            run(['docker', 'start', candidate['Id']], timeout=30)
            for _ in range(100):
                candidate = inspect(candidate['Id'])
                if candidate['State'].get('Health', {}).get('Status') == 'healthy': break
                if not candidate['State']['Running']: raise RuntimeError('candidate_exited')
                time.sleep(1)
            assert candidate['State'].get('Health', {}).get('Status') == 'healthy'
            assert run(['docker', 'exec', candidate['Id'], 'sha256sum', '/app/sub2api']).decode().split()[0] == plan['candidate']['binary_sha256']
            result.update(status='prepared', candidate_id=candidate['Id'], candidate_started=candidate['State']['StartedAt'], candidate_name=name,
                          candidate_image=candidate['Image'], project=str(project), migration_seconds=elapsed, old_restart_policy='no')
            atomic_json(root / 'candidate-result.json', result)
            print(json.dumps(result), flush=True)
        except Exception as exc:
            result.update(status='failed', error=type(exc).__name__ + ':' + str(exc))
            # No traffic switch is part of this program. Remove only this
            # operation's never-routed candidate before restoring blue policy.
            if digest(config.read_bytes()) == plan['baseline']['nginx_config_sha256']:
                if candidate is not None:
                    current = inspect(candidate['Id'])
                    assert current['Config']['Labels']['org.yunmostar.bluegreen.operation'] == root.name
                    run(['docker', 'rm', '-f', candidate['Id']], timeout=30)
                if policy_changed and candidate is not None:
                    run(['docker', 'update', '--restart=unless-stopped', blue['Id']])
            atomic_json(root / 'candidate-result.json', result)
            raise


if __name__ == '__main__': main()
