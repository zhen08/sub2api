"""Deterministic server-side hourly OpenAI policy."""

import argparse
import contextlib
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.request
import urllib.parse


class PolicyError(Exception):
    pass


def pages(get, path, **params):
    result, seen, total, page = [], set(), None, 1
    while True:
        data = get(path, dict(params, page=page, page_size=100, sort_by='id', sort_order='asc'))
        n = data['total']
        if not isinstance(n, int) or n < 0 or (total is not None and total != n) or data['page'] != page:
            raise PolicyError('pagination total/page changed')
        total = n
        rows = data['items']
        for row in rows:
            if row['id'] in seen:
                raise PolicyError('duplicate pagination ID')
            seen.add(row['id'])
            result.append(row)
        if len(result) == total:
            return result
        if not rows or len(result) > total or page > 100000:
            raise PolicyError('incomplete pagination')
        page += 1


def window_evidence(hour, tokens):
    return {'hour':hour, 'hourly_tokens':tokens,
            'hour_start':dt.datetime.fromtimestamp(hour*3600,dt.timezone.utc).isoformat(),
            'hour_end':dt.datetime.fromtimestamp((hour+1)*3600,dt.timezone.utc).isoformat()}


def canonical_zero_window(evidence, hour):
    return (isinstance(evidence, dict)
            and type(evidence.get('hour')) is int
            and type(evidence.get('hourly_tokens')) is int
            and evidence == window_evidence(hour, 0))


def decide(state, hour, tokens, luna_enabled=True, terra_enabled=True, luna_recovery_hours=8):
    if not luna_enabled and state.get('level') == 'luna':
        state = dict(state, level='terra')
    if not terra_enabled and state.get('level') == 'terra' and hour >= state.get('last', -1):
        level = 'luna' if luna_enabled and tokens > 50_000_000 else 'full'
        state = dict(state, level=level, low=0, low_windows=[])
    if hour <= state.get('last', -1):
        return dict(state)
    level = state.get('level', 'original')
    low = 0
    windows = []
    if luna_enabled and tokens > 50_000_000:
        level = 'luna'
    elif terra_enabled and tokens > 20_000_000:
        level = 'luna' if level == 'luna' else 'terra'
    elif level in ('terra', 'luna') and tokens == 0 and hour >= state.get('recovery_from_hour', 0):
        # Legacy low counts may include nonzero usage: evidence, not the
        # counter, is authoritative. Never synthesize missing evidence.
        required = luna_recovery_hours if level == 'luna' else 8
        prior = state.get('low_windows')
        if state.get('last') == hour - 1 and isinstance(prior, list):
            for evidence in reversed(prior[-(required - 1):]):
                previous_hour = hour - len(windows) - 1
                if (previous_hour < state.get('recovery_from_hour', 0)
                        or not canonical_zero_window(evidence, previous_hour)):
                    break
                windows.insert(0, evidence)
        windows = windows + [window_evidence(hour, tokens)]
        low = len(windows)
        if low == required:
            level, low = 'full', 0
    result = dict(state, level=level, last=hour, low=low, low_windows=windows)
    return result


def plan(users, hour, totals, keys, policies, luna_enabled=True, terra_enabled=True, luna_recovery_hours=8):
    import copy
    users = copy.deepcopy(users)
    by_user = {}
    for key in keys:
        by_user.setdefault(str(key['user_id']), []).append(key)
    ops, transitions = [], []
    for uid in sorted(set(by_user) | set(users) | {str(uid) for uid in totals if uid in policies} | {str(uid) for uid,level in policies.items() if level != 'original'}, key=int):
        user_keys = by_user.get(uid, [])
        old = users.get(uid, {})
        current = policies.get(int(uid))
        if current != old.get('level', 'original'):
            raise PolicyError('user policy drift or state missing')
        if terra_enabled and old.get('level','original') == 'original' and any(k['group_id'] == 8 for k in user_keys):
            old = {'level':'terra', 'last':hour-1, 'low':0}
        nxt = decide(old, hour, totals.get(int(uid), 0), luna_enabled=luna_enabled, terra_enabled=terra_enabled,luna_recovery_hours=luna_recovery_hours)
        terra_relaxing = not terra_enabled and old.get('level') == 'terra' and nxt['level'] == 'full'
        relaxing = not luna_enabled and old.get('level') == 'luna' and nxt['level'] in ('terra', 'full')
        if not relaxing and nxt['level'] in ('terra','luna') and nxt['level'] != old.get('level','original'):
            nxt['recovery_from_hour'] = hour + 1
        users[uid] = nxt
        level = nxt['level']
        if level != current:
            tokens = totals.get(int(uid), 0)
            reason = ('automatic_terra_suspended' if terra_relaxing else
                      'automatic_luna_suspended' if relaxing else
                      'tokens_gt_50000000' if luna_enabled and tokens > 50_000_000 else
                      'tokens_gt_20000000' if tokens > 20_000_000 else
                      ('twelve_consecutive_hours_eq_0' if old.get('level') == 'luna' and luna_recovery_hours == 12
                       else 'eight_consecutive_hours_eq_0') if level == 'full' else
                      'bootstrap_group_8')
            transitions.append(dict(window_evidence(hour,tokens), user_id=int(uid),
                **{'from':current,'to':level,'reason':reason,'low_streak':nxt.get('low_windows', [])}))
            ops.append({'id':int(uid), 'from':current, 'to':level})
    return {'users':users, 'ops':ops, 'transitions':transitions, 'hour':hour}


class Store:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.path = self.directory / 'state.json'

    def load(self):
        if not self.path.exists():
            return {'version':1, 'users':{}}
        data = json.loads(self.path.read_text())
        if data.get('version') != 1:
            raise PolicyError('unsupported state version')
        return data

    def save(self, data):
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix='.state-', dir=self.directory)
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f, sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(name, self.path)
            fd = os.open(self.directory, os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        finally:
            if os.path.exists(name): os.unlink(name)

    @contextlib.contextmanager
    def lock(self):
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd = os.open(self.directory / 'lock', os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
        finally:
            os.close(fd)


def commit_plan(store, planned, read_policy, update_policy, guard=lambda: None, terra_enabled=True, luna_recovery_hours=8):
    guard()
    data = store.load()
    if data.get('pending') and data['pending'] != planned:
        raise PolicyError('unresolved intent')
    # Validate recovery evidence even when a journal retry finds the remote
    # write already applied. Unsafe legacy intents require reconciliation;
    # leave both durable state and remote restrictions untouched.
    for op in planned['ops']:
        if op['to'] == 'full':
            relaxation = (not terra_enabled and op['from'] == 'terra'
                          and planned['users'].get(str(op['id']), {}).get('level') == 'full'
                          and any(t.get('user_id') == op['id'] and t.get('from') == 'terra'
                                  and t.get('to') == 'full' and t.get('reason') == 'automatic_terra_suspended'
                                  for t in planned.get('transitions', [])))
            if relaxation:
                continue
            required = luna_recovery_hours if op['from'] == 'luna' else 8
            hour = planned['hour']
            user = planned['users'].get(str(op['id']), {})
            evidence = user.get('low_windows')
            if (op['from'] not in ('terra', 'luna')
                    or user.get('level') != 'full' or user.get('last') != hour
                    or hour - (required - 1) < user.get('recovery_from_hour', 0)
                    or not isinstance(evidence, list) or len(evidence) != required
                    or not all(canonical_zero_window(v, h) for v, h in
                               zip(evidence, range(hour - (required - 1), hour + 1)))):
                raise PolicyError('invalid recovery evidence: manual reconciliation required; no writes')
    # Validate the ENTIRE write set before the first mutation.
    for op in planned['ops']:
        if read_policy(op['id']) not in (op['from'], op['to']):
            raise PolicyError('user policy drift')
    data['pending'] = planned
    guard()
    store.save(data)
    for op in planned['ops']:
        current = read_policy(op['id'])
        if current == op['to']:
            continue
        if current != op['from']:
            raise PolicyError('user policy drift')
        guard()
        update_policy(op['id'], op['to'])
        if read_policy(op['id']) != op['to']:
            raise PolicyError('user policy readback mismatch')
    data['users'] = planned['users']
    del data['pending']
    guard()
    store.save(data)
    return planned['transitions']


def usage_sql(hour):
    if type(hour) is not int or hour < 0:
        raise PolicyError('invalid hour')
    return f"""BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
WITH w AS MATERIALIZED (
 SELECT u.*, g.platform FROM usage_logs u LEFT JOIN groups g ON g.id=u.group_id
 WHERE u.created_at >= to_timestamp({hour*3600}) AND u.created_at < to_timestamp({(hour+1)*3600})
), per_user AS (
 SELECT user_id, count(*) AS rows, count(DISTINCT api_key_id) AS keys,
 sum(input_tokens::bigint) AS input, sum(output_tokens::bigint) AS output,
 sum(cache_creation_tokens::bigint) AS cache_creation, sum(cache_read_tokens::bigint) AS cache_read,
 sum(input_tokens::bigint + output_tokens::bigint + cache_creation_tokens::bigint + cache_read_tokens::bigint) AS total
 FROM w WHERE platform = 'openai' GROUP BY user_id
)
SELECT json_build_object('hour',{hour},'all_rows',(SELECT count(*) FROM w),
 'openai_rows',(SELECT count(*) FROM w WHERE platform = 'openai'),
 'unknown_groups',(SELECT count(*) FROM w WHERE platform IS NULL),
 'invalid_tokens',(SELECT count(*) FROM w WHERE platform = 'openai' AND (
 input_tokens IS NULL OR output_tokens IS NULL OR cache_creation_tokens IS NULL OR cache_read_tokens IS NULL
 OR input_tokens<0 OR output_tokens<0 OR cache_creation_tokens<0 OR cache_read_tokens<0)),
 'users',coalesce((SELECT json_agg(per_user ORDER BY user_id) FROM per_user),'[]'::json));
COMMIT;
"""


def validate_usage(doc, hour):
    if doc['hour'] != hour or doc['unknown_groups'] or doc['invalid_tokens']:
        raise PolicyError('incomplete or invalid usage snapshot')
    if not 0 <= doc['openai_rows'] <= doc['all_rows']:
        raise PolicyError('usage row counts invalid')
    result, rows = {}, 0
    for u in doc['users']:
        if u['user_id'] in result or not 0 < u['keys'] <= u['rows']:
            raise PolicyError('usage user/key counts invalid')
        values = [u[k] for k in ('input','output','cache_creation','cache_read')]
        if any(type(v) is not int or v < 0 for v in values) or sum(values) != u['total']:
            raise PolicyError('usage total mismatch')
        rows += u['rows']
        result[u['user_id']] = u['total']
    if rows != doc['openai_rows']:
        raise PolicyError('usage rows incomplete')
    return result


def collect_usage(hour):
    # Container already has psql and its DB credentials. No password in argv/output.
    command = ('PGPASSWORD="$DATABASE_PASSWORD" PGCONNECT_TIMEOUT=10 '
               'PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=60000 -c lock_timeout=5000" '
               'psql -h "$DATABASE_HOST" -p "$DATABASE_PORT" -U "$DATABASE_USER" '
               '-d "$DATABASE_DBNAME" -XqAt -v ON_ERROR_STOP=1')
    result = subprocess.run(['docker','exec','-i','sub2api','sh','-c',command],
                            input=usage_sql(hour), text=True, capture_output=True, timeout=80)
    if result.returncode:
        raise PolicyError('read-only SQL query failed (details suppressed)')
    try:
        doc = json.loads(result.stdout)
        validate_usage(doc, hour)
        return doc
    except (ValueError, KeyError, TypeError):
        raise PolicyError('invalid SQL snapshot response') from None


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class API:
    def __init__(self, base, token):
        self.base, self.token = base, token
        # Never send privileged API headers through an ambient outbound proxy.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), RejectRedirects())

    @classmethod
    def from_docker(cls):
        result = subprocess.run(['docker','inspect','sub2api'], capture_output=True, text=True, timeout=20)
        if result.returncode:
            raise PolicyError('cannot inspect Sub2API container')
        try:
            info = json.loads(result.stdout)[0]
            env = dict(v.split('=',1) for v in info['Config']['Env'] if '=' in v)
            token = env['ADMIN_API_KEY']
            ports = info['NetworkSettings']['Ports']['8080/tcp']
            if len(ports) != 1 or not token:
                raise ValueError()
            host = ports[0]['HostIp']
            if host == '0.0.0.0': host = '127.0.0.1'
            if host not in ('127.0.0.1','172.17.0.1'):
                raise ValueError()
            return cls('http://' + host + ':' + str(int(ports[0]['HostPort'])), token)
        except (ValueError, KeyError, TypeError, IndexError):
            raise PolicyError('unexpected container API configuration') from None

    def request(self, method, path, params=None, payload=None):
        url = self.base + '/api/v1/admin' + path
        if params: url += '?' + urllib.parse.urlencode(params)
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(url, data=body, method=method,
                    headers={'x-api-key':self.token,'Content-Type':'application/json'})
        try:
            with self.opener.open(request, timeout=30) as response:
                envelope = json.load(response)
            if envelope.get('code',0) != 0 or 'data' not in envelope:
                raise PolicyError('invalid API envelope')
            return envelope['data']
        except urllib.error.HTTPError as error:
            error.close()
            raise PolicyError(f'admin API {method} {path}: HTTP {error.code}') from None
        except (urllib.error.URLError, ValueError, TimeoutError):
            raise PolicyError('admin API transport/JSON failure') from None

    def get(self, path, params=None):
        return self.request('GET',path,params=params)

    def put(self, path, payload):
        return self.request('PUT',path,payload=payload)


class PolicyBackend:
    def __init__(self, api):
        self.api = api

    def read(self, uid):
        data = self.api.get(f'/users/{int(uid)}/openai-model-policy')
        level = data.get('level')
        if level not in ('original','terra','luna','full'):
            raise PolicyError('invalid user policy response')
        return level

    def write(self, uid, level):
        if level not in ('terra','luna','full'):
            raise PolicyError('invalid policy write')
        self.api.put(f'/users/{int(uid)}/openai-model-policy', {'level':level})


def inventory(api):
    groups = {g['id']:g for g in pages(api.get, '/groups')}
    for gid, name in ((6,'openai-default'),(8,'openai-max-terra')):
        g = groups.get(gid, {})
        if (g.get('platform'),g.get('name'),g.get('status')) != ('openai',name,'active'):
            raise PolicyError('source group configuration drift')
    channels = pages(api.get, '/channels')
    for gid, cid in ((6,2),(8,1)):
        found = [v for v in channels if gid in v.get('group_ids', [])]
        mapping = {'codex-auto-review':'gpt-6-luna', 'gpt-5.5':'gpt-6-luna',
                   'gpt-5.6-sol':'gpt-6.1-sol'}
        # Permit each complete current/live or new configuration during migration.
        # Never accept subsets, extra aliases, or edit channels from the controller.
        legacy_mapping = dict(mapping, **{'gpt-5.6-sol':'gpt-6-sol'})
        accepted_mappings = ({'openai':mapping}, {'openai':legacy_mapping})
        if len(found) != 1:
            raise PolicyError('source channel membership drift')
        ch = found[0]
        if (ch['id'] != cid or ch['group_ids'] != [gid] or ch.get('model_mapping') not in accepted_mappings
            or ch.get('status') != 'active' or ch.get('restrict_models') is not False
            or ch.get('billing_model_source') != 'channel_mapped'
            or any(ch.get(k) for k in ('features','features_config','model_pricing','apply_pricing_to_account_stats','account_stats_pricing_rules'))):
            raise PolicyError('source channel configuration drift')
    users = pages(api.get, '/users')
    safe_users, keys, key_count, seen = [], [], 0, set()
    for u in users:
        safe_users.append({k:u.get(k,'') for k in ('id','username','email')})
        rows = pages(api.get, f"/users/{u['id']}/api-keys")
        key_count += len(rows)
        for row in rows:
            if row['id'] in seen or row['user_id'] != u['id']:
                raise PolicyError('key identity enumeration mismatch')
            seen.add(row['id'])
            gid = row.get('group_id')
            if gid not in groups:
                raise PolicyError('key has unknown/unbound group')
            if groups[gid]['platform'] == 'openai':
                keys.append({k:row[k] for k in ('id','user_id','group_id')})
    return {'users':safe_users,'keys':keys,'user_count':len(users),'all_key_count':key_count,'openai_key_count':len(keys)}


def run(api, store, hour, collector=collect_usage, apply=False, collect_only=False, guard=lambda: None, luna_enabled=True, terra_enabled=True, luna_recovery_hours=8):
    guard()
    inv = inventory(api)
    usage = collector(hour)
    totals = validate_usage(usage, hour)
    data = store.load()
    identities = {u['id']:u for u in inv['users']}
    if any(int(uid) not in identities for uid in data['users']):
        raise PolicyError('managed user missing from complete inventory')
    summary = {'mode':'apply' if apply else 'collect-only' if collect_only else 'dry-run',
               'hour':hour, 'hour_start':dt.datetime.fromtimestamp(hour*3600,dt.timezone.utc).isoformat(),
               'hour_end':dt.datetime.fromtimestamp((hour+1)*3600,dt.timezone.utc).isoformat(),
               'inventory':{k:inv[k] for k in ('user_count','all_key_count','openai_key_count')},
               'usage':usage, 'targets':[], 'status':'ok'}
    if collect_only:
        summary['status'] = 'collected_only_backend_not_checked'
        summary['bootstrap_candidates'] = [dict(identities[uid], keys=[k for k in inv['keys'] if k['user_id']==uid])
            for uid in sorted({k['user_id'] for k in inv['keys'] if k['group_id']==8})]
        return summary
    backend = PolicyBackend(api)
    # Read ALL users before the first write. 404 is an error, never original.
    policies = {uid:backend.read(uid) for uid in identities}
    pending = data.get('pending')
    resumed_ops, resumed_transitions = [], []
    if pending:
        if not terra_enabled and any(op['to'] == 'terra' for op in pending['ops']):
            raise PolicyError('automatic terra suspended: pending escalation requires reconciliation; no writes')
        if not luna_enabled and any(op['to'] == 'luna' for op in pending['ops']):
            raise PolicyError('automatic luna suspended: pending escalation requires reconciliation; no writes')
        if pending['hour'] != hour:
            raise PolicyError('stale pending intent: manual reconciliation required; no writes')
        if not apply:
            summary.update(status='pending_intent',pending=pending)
            return summary
        resumed_transitions = commit_plan(store,pending,backend.read,backend.write,guard,terra_enabled=terra_enabled,luna_recovery_hours=luna_recovery_hours)
        resumed_ops = pending['ops']
        policies = {uid:backend.read(uid) for uid in identities}
        data = store.load()
    planned = plan(data['users'],hour,totals,inv['keys'],policies,luna_enabled=luna_enabled,terra_enabled=terra_enabled,luna_recovery_hours=luna_recovery_hours)
    summary['targets'] = [dict(op,user_id=op['id'],username=identities[op['id']]['username'],
                              email=identities[op['id']]['email'],keys=[k for k in inv['keys'] if k['user_id']==op['id']])
                          for op in resumed_ops + planned['ops']]
    summary['transitions'] = resumed_transitions + planned['transitions']
    evidence = {v['user_id']:v for v in summary['transitions']}
    for target in summary['targets']:
        target.update(evidence.get(target['user_id'], {}))
    if apply:
        commit_plan(store,planned,backend.read,backend.write,guard,terra_enabled=terra_enabled,luna_recovery_hours=luna_recovery_hours)
    return summary


def load_policy_config(path):
    try:
        config = json.loads(Path(path).read_text())
    except FileNotFoundError:
        return {'luna_enabled': True, 'terra_enabled': True}
    except (OSError, ValueError):
        raise PolicyError('invalid policy configuration') from None
    if (not isinstance(config, dict) or 'luna_enabled' not in config
            or not set(config) <= {'luna_enabled', 'terra_enabled', 'luna_recovery_hours'}
            or any(type(config[k]) is not bool for k in ('luna_enabled','terra_enabled') if k in config)
            or ('luna_recovery_hours' in config and
                (type(config['luna_recovery_hours']) is not int or config['luna_recovery_hours'] not in (8,12)))):
        raise PolicyError('invalid policy configuration')
    return dict(terra_enabled=True, **config) if 'terra_enabled' not in config else config


def load_luna_enabled(path):
    return load_policy_config(path)['luna_enabled']


def processed_hour():
    return int(dt.datetime.now(dt.timezone.utc).timestamp() // 3600) - 1


def main(argv=None, api=None, collector=collect_usage):
    parser = argparse.ArgumentParser(description='Hourly per-user OpenAI model policy; defaults to read-only dry-run')
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument('--dry-run',action='store_true')
    modes.add_argument('--collect-only',action='store_true')
    modes.add_argument('--apply',action='store_true')
    parser.add_argument('--state-dir',default='/var/lib/aiproxy-hourly-policy')
    parser.add_argument('--expected-hour',type=int)
    args = parser.parse_args(argv)
    hour = processed_hour() if args.expected_hour is None else args.expected_hour
    def guard():
        if processed_hour() != hour:
            raise PolicyError('processed hour mismatch/rollover; no further writes')
    store = Store(args.state_dir)
    try:
        guard()
        api = api or API.from_docker()
        lock = store.lock() if args.apply else contextlib.nullcontext()
        with lock:
            config = load_policy_config(store.directory / 'config.json')
            summary = run(api,store,hour,collector,apply=args.apply,collect_only=args.collect_only,guard=guard,
                          **config)
        if not args.apply:
            print(json.dumps(summary,sort_keys=True))
        elif summary.get('transitions'):
            print(json.dumps({'status':'transitions','hour':hour,'targets':summary['targets']},sort_keys=True))
        return 0
    except PolicyError as error:
        print(json.dumps({'status':'error','hour':hour,'error':str(error)}))
        return 1
    except Exception as error:
        # Never interpolate arbitrary response bodies, subprocess stderr or env.
        print(json.dumps({'status':'error','hour':hour,'error':type(error).__name__}))
        return 1


if __name__ == '__main__':
    import signal
    def timeout_handler(signum, frame):
        raise PolicyError('controller deadline exceeded')
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(240)
    sys.exit(main())
