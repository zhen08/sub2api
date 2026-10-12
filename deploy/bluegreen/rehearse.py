"""Actual images, shared retained database, Redis and Nginx; network-none only.

Production credentials, listeners, containers, database and nginx config are
unreachable through this runner. Only owned scratch objects can be mutated.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
import uuid

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import Handoff, atomic_json
from migration import render
from supervisor import supervise


OLD = 'sha256:bd8e1681089feb67a4c4e4609bfe9745cc354ea1bb0a8681a18c35234416093e'
NEW = 'sha256:22662cfe27b7413899cd1e764e1605979ccc5e8b1ad1496775888d99cb190d06'
PG = 'sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2'
REDIS = 'sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99'
NGINX = 'sha256:0d3b80406a13a767339fbe2f41406d6c7da727ab89cf8fae399e81f780f814d1'


def nginx_config(side):
    port = 8081 if side == 'blue' else 8082
    return ('''pid /tmp/nginx.pid;
error_log /tmp/nginx-error.log notice;
worker_processes 2;
events { worker_connections 1024; }
http {
  access_log /tmp/nginx-access.log;
  client_body_temp_path /tmp/body;
  proxy_temp_path /tmp/proxy;
  fastcgi_temp_path /tmp/fastcgi;
  uwsgi_temp_path /tmp/uwsgi;
  scgi_temp_path /tmp/scgi;
  server {
    listen 127.0.0.1:8088;
    location / {
      proxy_pass http://127.0.0.1:PORT;
      proxy_http_version 1.1;
      proxy_set_header Connection "";
      proxy_buffering off;
      proxy_read_timeout 240s;
      proxy_connect_timeout 1s;
      proxy_next_upstream off;
      add_header X-Lab-Release SIDE always;
    }
  }
}
'''.replace('PORT', str(port)).replace('SIDE', side)).encode()


def app_config(port, jwt):
    return {'server': {'host': '127.0.0.1', 'port': port, 'mode': 'release'},
            'database': {'host': '127.0.0.1', 'port': 5432, 'user': 'postgres', 'password': '', 'dbname': 'restorecheck', 'sslmode': 'disable', 'max_open_conns': 10, 'max_idle_conns': 2},
            'redis': {'host': '127.0.0.1', 'port': 6379, 'db': 0, 'password': '', 'pool_size': 5, 'min_idle_conns': 1},
            'jwt': {'secret': jwt},
            'pricing': {'data_dir': '/app/data', 'fallback_file': '/app/resources/model-pricing/model_prices_and_context_window.json', 'update_interval_hours': 8760, 'hash_check_interval_minutes': 1440},
            'security': {'url_allowlist': {'enabled': False, 'upstream_hosts': ['127.0.0.1'], 'allow_private_hosts': True, 'allow_insecure_http': True}},
            'token_refresh': {'enabled': False}, 'call_audit': {'enabled': False, 'worker': {'enabled': False}},
            'ops': {'enabled': False, 'cleanup': {'enabled': False}, 'aggregation': {'enabled': False}},
            'dashboard_aggregation': {'enabled': False}, 'usage_cleanup': {'enabled': False},
            'batch_image': {'enabled': False, 'queue_enabled': False},
            'gateway': {'cn_providers': {'balance_check_enabled': False}}}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bundle', type=pathlib.Path, required=True)
    parser.add_argument('--incident', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    parser.add_argument('--candidate-image', default=NEW)
    args = parser.parse_args()
    candidate_image = args.candidate_image
    assert re.fullmatch(r'sha256:[a-f0-9]{64}', candidate_image)
    os.umask(0o077); sys.dont_write_bytecode = True
    code = pathlib.Path(__file__).resolve().parent
    sys.path.insert(0, str(args.bundle.resolve()))
    from v0215_live import Live, LEDGER
    from v0215_rollout import Session, REV, sha
    from v0215_cache_rehearsal import backend_class
    from v0215_cache_process import run
    import v0215_spill_probe as probe
    import v0215_guards as guard
    from v0215_restore_catalog import verify_restore_catalog

    available = int(re.search(r'MemAvailable:\s+(\d+)', pathlib.Path('/proc/meminfo').read_text())[1])
    if available < 12 * 1024**2: raise RuntimeError('insufficient_headroom_for_isolated_lab')
    args.output.mkdir(mode=0o700)
    images = {OLD, candidate_image, PG, REDIS, NGINX}
    owned, names, pids = set(), set(), set()
    count = 0
    children = []

    def isolated(argv, **kwargs):
        nonlocal count
        if argv[0] == 'nsenter':
            assert argv[1] == '-t' and argv[2] in pids and argv[3:6] == ['-n', 'python3', '-B']
        elif argv[0] == 'docker':
            action = argv[1]
            if action == 'create':
                name = argv[argv.index('--name')+1]
                assert re.fullmatch(r'v0215-(database|redis|bgblue|bggreen|bgnginx)-[a-f0-9]{32}', name)
                assert len(images.intersection(argv)) == 1
                network = [x for x in argv if x.startswith('--network=')]
                assert len(network) == 1 and (network[0] == '--network=none' or network[0][10:] in {'container:'+x for x in owned})
                assert '--read-only' in argv and '--cap-drop=ALL' in argv
                assert not any(x.startswith(('--volume', '--mount', '--publish', '--privileged')) or x in ('-v','-p') for x in argv)
                names.add(name)
            elif action in ('inspect', 'start', 'exec', 'rm', 'pause', 'unpause'):
                assert any(x in owned or x in names for x in argv[2:])
            elif action == 'ps':
                filters = {'name=^/'+n+'$' for n in names} | {'label=org.sub2api.deploy.operation='+n.rsplit('-',1)[1] for n in names}
                assert argv[2:5] == ['-aq','--no-trunc','--filter'] and len(argv) == 6 and argv[5] in filters
            elif action == 'image':
                assert argv[2:] and argv[2] == 'inspect' and len(argv) == 4 and argv[3] in images
            else: raise RuntimeError('unapproved_docker_action')
        else: raise RuntimeError('unapproved_host_command')
        count += 1
        with (args.output/('command-%05d-%d.stderr' % (count, os.getpid()))).open('xb') as err:
            result = run(argv, stderr_out=err, **kwargs)
        if argv[:2] == ['docker','create']: owned.add(result.decode().strip())
        if argv[:2] == ['docker','inspect']:
            for item in json.loads(result):
                if item['Id'] in owned and item['State'].get('Pid'): pids.add(str(item['State']['Pid']))
        return result

    class Backend(backend_class(Live, probe)):
        def check_live(self):
            for image in images:
                assert json.loads(self.run(['docker','image','inspect',image],timeout=15))[0]['Id'] == image
        def sql(self, session, container, text, timeout=180):
            if text.startswith("UPDATE settings SET value='scratch-"):
                restored = Live.capture(self, session, container)
                report['restore_comparison'] = verify_restore_catalog(source_catalog, restored)
            return super().sql(session, container, text, timeout=timeout)

    class ReadySession(Session):
        def exec(self, container, command, timeout=60, **kwargs):
            if command == ['pg_isready','-U','postgres']:
                command = ['pg_isready','-h','127.0.0.1','-U','postgres','-d','postgres']
            return super().exec(container, command, timeout=timeout, **kwargs)

    load = lambda name: json.loads((args.bundle/name).read_bytes())
    source_catalog = json.loads((args.incident/'catalog_before.json').read_bytes())
    report = {'status': 'running', 'production_access': False, 'network': 'none', 'started': time.time(), 'scenarios': {}, 'old_image': OLD, 'candidate_image': candidate_image}
    backend = Backend(args.output, load('v0215-live-discovery.json'), load('v0215-migration-assessment.json'), load('v0215-live-pricing.json'), {'image_id': candidate_image}, runner=isolated)
    backend.source = args.incident/'backup'; backend.source_snapshot_sha256 = sha(backend.source/'snapshot.json')
    assert sha(backend.source/'database.dump') == 'e030e0a799f0ee323885403d2c09fc7b3d9b08e845971d8ff46d0dd2bbb08f23'
    key, limited_key = 'sk-bg-'+uuid.uuid4().hex, 'sk-bg-limit-'+uuid.uuid4().hex
    config_file = args.output/'fixture.private.json'
    atomic_json(config_file, {'key': key, 'limited_key': limited_key})
    phase = 'restore'
    def deadline(*_): raise RuntimeError('lab_deadline')
    signal.signal(signal.SIGALRM, deadline); signal.signal(signal.SIGTERM, deadline); signal.alarm(2400)
    try:
        backend.check_live()
        with ReadySession(args.output, isolated) as session:
            print(json.dumps({'phase': phase}), flush=True)
            pg = backend.restore(session, backend.snapshot(session))
            sql = lambda text, timeout=180: backend.sql(session, pg, text, timeout)
            # The retained production setting suppresses INFO startup evidence.
            # Remove it only in the verified, isolated clone.
            sql("DELETE FROM settings WHERE key='ops_runtime_log_config';")
            pid = str(backend.inspect(pg.cid)['State']['Pid'])
            def ns_python(script, timeout=20):
                return isolated(['nsenter','-t',pid,'-n','python3','-B','-c',script],timeout=timeout)
            def fixture_process(mode, output):
                # This process joins only the owned database namespace; it has no external network.
                pg.check(pg.cid)
                assert pid in pids
                fd = (args.output/(mode+'-'+uuid.uuid4().hex+'.log')).open('xb')
                proc = subprocess.Popen(['nsenter','-t',pid,'-n','python3','-B',str(code/'fixture.py'),mode,'--config',str(config_file),'--output',str(output)],stdout=fd,stderr=fd,start_new_session=True)
                fd.close(); children.append(proc); return proc
            def fixture_call(body, timeout=20):
                prefix = 'import sys,json,pathlib;sys.path.insert(0,'+repr(str(code))+');from fixture import get,request;cfg=json.loads(pathlib.Path('+repr(str(config_file))+').read_bytes());'
                return json.loads(ns_python(prefix+body, timeout))
            fixture_process('upstream', args.output/'upstream.json')
            time.sleep(.3)
            phase = 'seed_fake_tenants'
            # Existing cloned credentials are never selected by the two synthetic groups.
            ids = []
            for name, secret, concurrency in [('bg-main',key,100),('bg-limit',limited_key,2)]:
                result = sql("WITH u AS (INSERT INTO users(email,password_hash,balance,concurrency,balance_notify_enabled) VALUES('"+name+"@invalid.local','lab-no-login',1000,"+str(concurrency)+",false) RETURNING id), g AS (INSERT INTO groups(name,platform,rate_multiplier) VALUES('"+name+"','openai',1) RETURNING id), a AS (INSERT INTO accounts(name,platform,type,credentials,extra,concurrency) VALUES('"+name+"','openai','apikey','{\"api_key\":\"fake-only\",\"base_url\":\"http://127.0.0.1:9099\",\"model_mapping\":{\"gpt-4o\":\"gpt-4o\"}}','{\"openai_responses_mode\":\"force_chat_completions\",\"openai_responses_supported\":false}',"+str(concurrency)+") RETURNING id), k AS (INSERT INTO api_keys(user_id,key,name,group_id) SELECT u.id,'"+secret+"','"+name+"',g.id FROM u,g RETURNING id), ag AS (INSERT INTO account_groups(account_id,group_id) SELECT a.id,g.id FROM a,g) SELECT json_build_object('user',(SELECT id FROM u),'group',(SELECT id FROM g),'account',(SELECT id FROM a),'key',(SELECT id FROM k));")
                ids.append(json.loads(result))
            report['fixture_ids'] = ids
            jwt = uuid.uuid4().hex + uuid.uuid4().hex
            def launch(side, image, port):
                app = session.container('bg'+side, image, backend.options('container:'+pg.cid,'1536m','1000:1000',('/app/data:rw,nosuid,nodev,size=256m,uid=1000,gid=1000,mode=0700',))+['-e','BILLING_INFLIGHT_RESERVATION_ENABLED=false','-e','DATA_DIR=/app/data','-e','SUB2API_MULTI_INSTANCE=true','-e','SUB2API_BACKGROUND_JOBS_ENABLED=false','-e','HTTP_PROXY=http://127.0.0.1:9','-e','HTTPS_PROXY=http://127.0.0.1:9','-e','NO_PROXY=localhost,127.0.0.1'],['sh','-c','while [ ! -f /app/data/ready ]; do sleep 1; done; exec /app/docker-entrypoint.sh /app/sub2api > /app/data/lab-boot.log 2>&1'])
                backend.isolated(app,'container:'+pg.cid)
                if side == 'green':
                    assert session.exec(app,['sha256sum','/app/sub2api']).decode().split()[0] == '102b5cc0753ee033ddce2d0a6538dff38aa48d1964a832e3dbbfec536ae8985b'
                for name, data in [('config.yaml',json.dumps(app_config(port,jwt)).encode()),('model_pricing.json',backend.price)]:
                    session.exec(app,['sh','-c','cat > /app/data/'+name],data=data)
                session.exec(app,['touch','/app/data/ready'])
                for _ in range(90):
                    try:
                        healthy = fixture_call('print(json.dumps(get('+str(port)+')[0]))') == 200
                    except Exception: healthy = False
                    if healthy: return app
                    time.sleep(.5)
                raise RuntimeError('lab_app_unhealthy_'+side)
            phase = 'blue_start'; print(json.dumps({'phase': phase}), flush=True)
            blue = launch('blue',OLD,8081)
            blue_started = backend.inspect(blue.cid)['State']['StartedAt']
            first = fixture_call("print(json.dumps(request(8081,cfg['key'])))")
            atomic_json(args.output/'first-smoke.json', first)
            if first['status'] != 200:
                (args.output/'blue-startup.private.log').write_bytes(session.exec(blue,['cat','/app/data/lab-boot.log']))
            assert first['status'] == 200 and first['complete'], 'blue_api_smoke'
            report['ledger_before'] = len(json.loads(sql(LEDGER)))
            phase = 'online_migration'; print(json.dumps({'phase': phase}), flush=True)
            migration_sql = render(backend.payload)
            # A held read lock must make DDL abort promptly, leaving blue serving.
            lock_statement = "BEGIN; LOCK TABLE payment_orders IN ACCESS SHARE MODE; SELECT pg_sleep(30); ROLLBACK;"
            blocker_name='bluegreen_blocker_'+uuid.uuid4().hex
            lock_log = (args.output/'ddl-blocker.log').open('xb')
            blocker = subprocess.Popen(['docker','exec','-i','-e','PGAPPNAME='+blocker_name,pg.cid,'timeout','35','psql','-XqAt','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=lock_log,stderr=lock_log,start_new_session=True)
            children.append(blocker);lock_log.close();blocker.stdin.write(lock_statement.encode());blocker.stdin.close()
            for _ in range(30):
                active=sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='"+blocker_name+"' AND wait_event='PgSleep';")
                if active=='1':break
                time.sleep(.1)
            assert active=='1'
            begin=time.monotonic()
            try:
                sql(migration_sql,timeout=10)
            except RuntimeError:
                report['scenarios']['ddl_lock_timeout']={'rejected':True,'seconds':round(time.monotonic()-begin,3),'ledger_count':len(json.loads(sql(LEDGER)))}
            else:raise AssertionError('blocked_migration_was_not_rejected')
            assert report['scenarios']['ddl_lock_timeout']['seconds']<3 and report['scenarios']['ddl_lock_timeout']['ledger_count']==290
            # Cancel only the uniquely identified blocker inside the owned database.
            sql("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND application_name='"+blocker_name+"';")
            blocker.wait(timeout=5)
            begin = time.monotonic(); sql(migration_sql,timeout=15)
            report['online_migration_seconds'] = round(time.monotonic()-begin,3)
            assert len(json.loads(sql(LEDGER))) == 293
            assert backend.inspect(blue.cid)['State']['StartedAt'] == blue_started
            hold_path=args.output/'startup-hold.json';hold=fixture_process('hold',hold_path)
            for _ in range(100):
                if hold_path.with_suffix('.started').exists():break
                if hold.poll() is not None:raise RuntimeError('startup_hold_did_not_start')
                time.sleep(.1)
            assert hold_path.with_suffix('.started').exists()
            def slots():
                return session.exec(backend.redis,['redis-cli','--raw','ZRANGE','concurrency:account:'+str(ids[1]['account']),'0','-1']).decode().splitlines()
            slots_before=slots();assert slots_before,'old_request_slot_missing_before_green'
            phase = 'green_start'; print(json.dumps({'phase': phase}), flush=True)
            green = launch('green',candidate_image,8082)
            assert set(slots_before)<=set(slots()), 'green_start_deleted_blue_live_slots'
            report['scenarios']['preserve_peer_slots']={'passed':True,'slots_preserved':len(slots_before)}
            logs=session.exec(green,['cat','/app/data/lab-boot.log'])
            (args.output/'green-startup.private.log').write_bytes(logs)
            assert b'periodic job disabled for gateway instance' in logs,'standby_role_not_applied'
            assert b'[ScheduledTestRunner] started' not in logs and b'channel_monitor: runner started' not in logs,'duplicate_background_scheduler_started'
            smoke = fixture_call("print(json.dumps([request(p,cfg['key']) for p in (8081,8082)]))")
            atomic_json(args.output/'two-instance-smoke.json',smoke)
            assert all(x['status'] == 200 and x['complete'] for x in smoke), 'two_instance_api_smoke'
            nginx = session.container('bgnginx',NGINX,backend.options('container:'+pg.cid,'128m','nginx'),['sh','-c','while [ ! -f /tmp/ready ]; do sleep 1; done; exec nginx -c /tmp/lab.conf -g "daemon off;"'])
            backend.isolated(nginx,'container:'+pg.cid)
            blue_config, green_config = nginx_config('blue'), nginx_config('green')
            session.exec(nginx,['sh','-c','cat > /tmp/lab.conf'],data=blue_config)
            session.exec(nginx,['touch','/tmp/ready']); time.sleep(.5)

            class Adapter:
                pause_after_reload = False
                kill_after_reload = False
                def read(self): return session.exec(nginx,['cat','/tmp/lab.conf'])
                def install(self,value):
                    session.exec(nginx,['sh','-c','cat > /tmp/lab.next && mv /tmp/lab.next /tmp/lab.conf'],data=value)
                def validate(self): session.exec(nginx,['sh','-c','nginx -t -c /tmp/lab.conf > /tmp/validation.log 2>&1'],timeout=5)
                def reload(self):
                    session.exec(nginx,['sh','-c','nginx -s reload -c /tmp/lab.conf > /tmp/reload.log 2>&1'],timeout=5)
                    if self.read() == green_config:
                        if self.pause_after_reload: isolated(['docker','pause',green.cid],timeout=5)
                        if self.kill_after_reload: os.kill(os.getpid(),signal.SIGKILL)
                def ready(self,side):
                    port = 8081 if side == 'blue' else 8082
                    try:
                        result = fixture_call("print(json.dumps(request("+str(port)+",cfg['key'])))")
                        return result['status'] == 200 and result['complete']
                    except Exception: return False
                def probe(self,side):
                    try:
                        result = fixture_call("s,h,b=get(8088,timeout=.6);print(json.dumps({'status':s,'side':h.get('X-Lab-Release'),'healthy':b==b'{\"status\":\"ok\"}'}))",timeout=3)
                        return result['status'] == 200 and result['side'] == side and result['healthy']
                    except Exception: return False
            adapter = Adapter()
            for _ in range(30):
                if adapter.probe('blue'):break
                time.sleep(.2)
            if not adapter.probe('blue'):
                diagnostics=fixture_call("s,h,b=get(8088);print(json.dumps({'status':s,'headers':h,'body':b.decode()}))")
                atomic_json(args.output/'nginx-startup-probe.json',diagnostics)
                raise AssertionError('isolated_nginx_startup_probe_failed')
            phase = 'pre_switch_rejections'; print(json.dumps({'phase': phase}),flush=True)
            scenario=args.output/'unready';scenario.mkdir(mode=0o700)
            original_ready=adapter.ready
            adapter.ready=lambda side: side=='blue' and original_ready(side)
            report['scenarios']['unready']=Handoff(adapter,scenario,blue_config,green_config).run()
            adapter.ready=original_ready
            assert report['scenarios']['unready']['state']=='not_switched' and adapter.probe('blue')
            scenario=args.output/'bad-config';scenario.mkdir(mode=0o700)
            report['scenarios']['invalid_nginx']=Handoff(adapter,scenario,blue_config,green_config+b'\ninvalid_lab_directive;\n').run()
            assert report['scenarios']['invalid_nginx']['state']=='rolled_back' and adapter.probe('blue')
            phase = 'stream_load_and_handoff'; print(json.dumps({'phase': phase}),flush=True)
            stream_path=args.output/'stream.json'; load_path=args.output/'load.json'; poll_path=args.output/'poll.json'
            stream=fixture_process('stream',stream_path)
            for _ in range(100):
                if stream_path.with_suffix('.started').exists(): break
                if stream.poll() is not None: raise RuntimeError('long_stream_did_not_start')
                time.sleep(.1)
            assert stream_path.with_suffix('.started').exists()
            load_process=fixture_process('load',load_path); polling=fixture_process('poll',poll_path)
            api_path=args.output/'api-poll.json'; api_poll=fixture_process('api-poll',api_path)
            happy_started=time.monotonic()
            handoff_dir=args.output/'happy';handoff_dir.mkdir(mode=0o700)
            handoff=Handoff(adapter,handoff_dir,blue_config,green_config)
            report['scenarios']['switch_under_load']=handoff.run()
            assert report['scenarios']['switch_under_load']['state']=='active'
            assert adapter.probe('green') and backend.inspect(blue.cid)['State']['Running']
            # Switch back while the original blue stream is still alive.
            report['scenarios']['route_rollback']=handoff.rollback('exercise')
            happy_finished=time.monotonic()
            api_path.with_suffix('.stop').touch();api_poll.wait(timeout=25)
            api_samples=json.loads(api_path.read_bytes())
            assert len(api_samples)>20 and all(x['status']==200 and x['complete'] for x in api_samples), 'authenticated_cutover_traffic_failed'
            report['scenarios']['authenticated_traffic']={'requests':len(api_samples),'failures':0}
            load_process.wait(timeout=60)
            assert load_process.returncode==0
            hold.wait(timeout=60)
            assert hold.returncode==0 and json.loads(hold_path.read_bytes())['complete']
            phase='candidate_hang';print(json.dumps({'phase':phase}),flush=True)
            scenario=args.output/'candidate-hang';scenario.mkdir(mode=0o700)
            adapter.pause_after_reload=True
            try:
                started=time.monotonic()
                report['scenarios']['candidate_hang']=Handoff(adapter,scenario,blue_config,green_config).run()
                report['scenarios']['candidate_hang']['total_seconds']=round(time.monotonic()-started,3)
            finally:
                adapter.pause_after_reload=False
                if backend.inspect(green.cid)['State']['Paused']:isolated(['docker','unpause',green.cid],timeout=5)
            assert report['scenarios']['candidate_hang']['state']=='rolled_back'
            phase='coordinator_kill';print(json.dumps({'phase':phase}),flush=True)
            scenario=args.output/'coordinator-kill';scenario.mkdir(mode=0o700)
            started=time.monotonic()
            adapter.kill_after_reload=True
            report['scenarios']['coordinator_kill']=supervise(Handoff(adapter,scenario,blue_config,green_config))
            adapter.kill_after_reload=False
            report['scenarios']['coordinator_kill']['total_seconds']=round(time.monotonic()-started,3)
            assert report['scenarios']['coordinator_kill']['state']=='rolled_back'
            stream.wait(timeout=160)
            assert stream.returncode==0
            stream_result=json.loads(stream_path.read_bytes())
            assert stream_result['complete'] and stream_result['seconds']>=130 and stream_result['side']=='blue'
            assert backend.inspect(blue.cid)['State']['StartedAt']==blue_started
            poll_path.with_suffix('.stop').touch();polling.wait(timeout=5)
            load_result=json.loads(load_path.read_bytes())
            assert all(x['status']==200 and x['complete'] for x in load_result['parallel'])
            assert all(x['status'] in (200,429) for x in load_result['limited'])
            stats=fixture_call("print(get(9099,'/stats')[2].decode())")
            assert stats['max_active']['limited']<=2, 'shared_concurrency_limit_exceeded'
            assert stats['max_active']['limited_account_total']<=2, 'startup_and_load_combined_limit_exceeded'
            expected=sum(x['completed'] for x in stats['requests'])
            users=','.join(str(x['user']) for x in ids)
            for _ in range(30):
                billing=json.loads(sql("SELECT json_build_object('usage_count',(SELECT count(*) FROM usage_logs WHERE user_id IN ("+users+")),'distinct_requests',(SELECT count(DISTINCT request_id) FROM usage_logs WHERE user_id IN ("+users+")),'actual_cost',(SELECT coalesce(sum(actual_cost),0)::text FROM usage_logs WHERE user_id IN ("+users+")),'balance_spent',(SELECT (2000-sum(balance))::text FROM users WHERE id IN ("+users+")));"))
                if billing['usage_count']==expected:break
                time.sleep(.5)
            from decimal import Decimal
            assert billing['usage_count']==billing['distinct_requests']==expected,'lost_or_duplicate_usage'
            assert Decimal(billing['actual_cost'])==Decimal(billing['balance_spent']) and Decimal(billing['actual_cost'])>0,'billing_balance_mismatch'
            report.update(status='passed',stream=stream_result,billing=billing,expected_billed_requests=expected,
                          shared_concurrency_max=stats['max_active']['limited'],parallel_requests=len(load_result['parallel']),
                          production_requests_sent=0,blue_stayed_running=True)
            samples=json.loads(poll_path.read_bytes());failed=[x for x in samples if x['status']!=200]
            failure_start=None;max_failure=0
            for sample in samples:
                if sample['status']!=200:
                    if failure_start is None:failure_start=sample['at']
                elif failure_start is not None:
                    max_failure=max(max_failure,sample['at']-failure_start);failure_start=None
            assert failure_start is None, 'traffic_did_not_recover'
            happy=[x for x in samples if happy_started<=x['at']<=happy_finished]
            assert happy and all(x['status']==200 for x in happy), 'healthy_handoff_dropped_probe'
            report['traffic']={'samples':len(samples),'failed_including_injected_hang':len(failed),'normal_handoff_failures':0,'max_observed_failure_window_seconds':round(max_failure,3),'poll_interval_seconds':0.1}
            assert max_failure<120, 'two_minute_failure_budget_exceeded'
            report['final_ledger_count']=len(json.loads(sql(LEDGER)))
            assert report['final_ledger_count']==293
            for process in children:
                if process.poll() is None:os.killpg(process.pid,signal.SIGTERM);process.wait(timeout=5)
    except Exception as exc:
        report.update(status='failed',phase=phase,error_type=type(exc).__name__,error=str(exc))
        print(json.dumps({'status':'failed','phase':phase,'error':str(exc)}),flush=True)
    finally:
        signal.alarm(0)
        for process in children:
            if process.poll() is None:
                os.killpg(process.pid,signal.SIGTERM)
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
        receipt=args.output/'session.json'
        report['cleanup']=json.loads(receipt.read_bytes()) if receipt.exists() else None
        report['elapsed_seconds']=round(time.time()-report['started'],3)
        atomic_json(args.output/'report.json',report)
    print(json.dumps(report),flush=True)
    assert report['status']=='passed' and report['cleanup']['status']=='cleaned'


if __name__ == '__main__': main()
