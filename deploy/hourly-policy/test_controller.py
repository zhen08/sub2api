import unittest
import controller as c

class PolicyTests(unittest.TestCase):
    def setUp(self):
        import os
        from unittest.mock import patch
        env = patch.dict(os.environ, {'HOURLY_POLICY_SSH_HOST': 'policy.example.test',
                                     'HOURLY_POLICY_SSH_PORT': '22'})
        env.start()
        self.addCleanup(env.stop)

    def test_thresholds(self):
        for n,want in [(0,'original'),(20_000_000,'original'),(20_000_001,'terra'),(50_000_000,'terra'),(50_000_001,'luna')]:
            self.assertEqual(c.decide({},100,n)['level'],want)

    def test_sticky_recovery_repeat_gap_and_bootstrap(self):
        s=c.decide({},100,50_000_001)
        self.assertEqual(c.decide(s,101,30_000_000)['level'],'luna')
        low=c.decide(s,101,0)
        self.assertEqual(low['level'],'luna')
        self.assertEqual(c.decide(low,101,0),low)
        self.assertEqual(c.decide(low,103,0)['level'],'luna')
        recovered = low
        for hour in range(102, 109):
            recovered = c.decide(recovered, hour, 0)
        self.assertEqual(recovered['level'], 'full')
        self.assertEqual(c.decide(low,102,10_000_000)['level'],'luna')
        bootstrap=c.decide({'level':'terra','last':99,'low':0},100,0)
        for hour in range(101, 108):
            bootstrap = c.decide(bootstrap, hour, 0)
        self.assertEqual(bootstrap['level'], 'full')
        self.assertEqual(c.decide(s,99,0),s)

    def test_pagination_all_pages_and_fail_closed(self):
        self.assertTrue(hasattr(c,'pages'), 'pagination missing')
        def api(path,params):
            return {'items':[{'id':params['page']}], 'total':3,'page_size':1,'page':params['page']}
        self.assertEqual([v['id'] for v in c.pages(api,'/x')],[1,2,3])
        with self.assertRaises(c.PolicyError): c.pages(lambda p,q:{'items':[{'id':1}], 'total':3,'page_size':1,'page':q['page']},'/x')
        with self.assertRaises(c.PolicyError): c.pages(lambda p,q:{'items':[], 'total':3,'page_size':1,'page':q['page']},'/x')
        with self.assertRaises(c.PolicyError): c.pages(lambda p,q:{'items':[{'id':q['page']}], 'total':q['page']+2,'page_size':1,'page':q['page']},'/x')

    def test_user_policy_plan_no_key_moves_and_bootstrap(self):
        keys=[{'id':1,'user_id':9,'group_id':8},{'id':2,'user_id':9,'group_id':6}]
        p=c.plan({},100,{9:0},keys,{9:'original'})
        self.assertEqual(p['users']['9']['level'],'terra')
        self.assertEqual(p['ops'],[{'id':9,'from':'original','to':'terra'}])
        p2 = p
        for hour in range(101, 108):
            p2=c.plan(p2['users'],hour,{9:0},keys,{9:'terra'})
        self.assertEqual(p2['ops'],[{'id':9,'from':'terra','to':'full'}])
        p3=c.plan(p2['users'],108,{9:51_000_000},keys,{9:'full'})
        self.assertEqual(p3['ops'],[{'id':9,'from':'full','to':'luna'}])
        p4=c.plan(p3['users'],108,{9:0},keys+[{'id':3,'user_id':9,'group_id':6}],{9:'luna'})
        self.assertEqual(p4['users'],p3['users'])
        self.assertEqual(p4['ops'],[])
        untouched=c.plan({},100,{9:0},[{'id':1,'user_id':9,'group_id':6}],{9:'original'})
        self.assertEqual(untouched['ops'],[])
        with self.assertRaises(c.PolicyError): c.plan({},100,{},keys,{9:'luna'})
        with self.assertRaises(c.PolicyError): c.plan(p3['users'],109,{},keys,{9:'original'})

    def test_durable_intent_partial_failure_retry_and_drift(self):
        self.assertTrue(hasattr(c,'commit_plan'), 'transaction missing')
        import tempfile,json,pathlib
        with tempfile.TemporaryDirectory() as d:
            store=c.Store(d)
            p={'users':{'9':{'level':'luna','last':100,'low':0}},'ops':[{'id':1,'user_id':9,'from':6,'to':12},{'id':2,'user_id':9,'from':6,'to':12}], 'transitions':[], 'hour':100}
            live={1:6,2:6}; calls=[]
            def update(k,g):
                self.assertTrue(store.load().get('pending'))
                calls.append(k)
                live[k]=g
                if k==2: raise RuntimeError('response lost after write')
            with self.assertRaises(RuntimeError): c.commit_plan(store,p,lambda k:live[k],update)
            self.assertIn('pending',store.load())
            c.commit_plan(store,store.load()['pending'],lambda k:live[k],update)
            self.assertEqual(calls,[1,2])
            self.assertNotIn('pending',store.load())
            self.assertEqual(store.load()['users'],p['users'])
            live[1]=99
            with self.assertRaises(c.PolicyError): c.commit_plan(store,p,lambda k:live[k],update)
            self.assertEqual(live[1],99)

    def test_usage_snapshot_multi_key_sum_and_completeness(self):
        self.assertTrue(hasattr(c,'validate_usage'), 'SQL snapshot validator missing')
        doc={'hour':100,'all_rows':4,'openai_rows':3,'unknown_groups':0,'invalid_tokens':0,'users':[{'user_id':9,'rows':2,'keys':2,'input':10,'output':20,'cache_creation':30,'cache_read':40,'total':100},{'user_id':10,'rows':1,'keys':1,'input':1,'output':2,'cache_creation':3,'cache_read':4,'total':10}]}
        self.assertEqual(c.validate_usage(doc,100),{9:100,10:10})
        import copy
        for field,value in [('openai_rows',4),('unknown_groups',1),('invalid_tokens',1),('hour',99)]:
            bad=copy.deepcopy(doc);bad[field]=value
            with self.assertRaises(c.PolicyError): c.validate_usage(bad,100)
        bad=copy.deepcopy(doc);bad['users'][0]['total']=101
        with self.assertRaises(c.PolicyError): c.validate_usage(bad,100)
        sql=c.usage_sql(100)
        self.assertIn('REPEATABLE READ READ ONLY',sql)
        self.assertIn('LEFT JOIN groups',sql)
        self.assertIn("platform = 'openai'",sql)
        self.assertIn('>= to_timestamp(360000)',sql)
        self.assertIn('< to_timestamp(363600)',sql)

    def test_explicit_recovery_window_guard(self):
        self.assertTrue('recovery_from_hour' in c.decide({'level':'luna','last':100,'low':0,'recovery_from_hour':102},101,0))
        restricted={'level':'luna','last':100,'low':0,'recovery_from_hour':102}
        partial=c.decide(restricted,101,0)
        self.assertEqual(partial['low'],0)
        low=c.decide(partial,102,0)
        self.assertEqual(low['low'],1)
        for hour in range(103, 110):
            low = c.decide(low, hour, 0)
        self.assertEqual(low['level'], 'full')

    def test_inventory_no_secrets_disabled_keys_and_policy_adapter(self):
        self.assertTrue(hasattr(c,'inventory'), 'inventory missing')
        api=FakeAPI()
        inv=c.inventory(api)
        self.assertEqual(len(inv['keys']),2)
        self.assertNotIn('SECRET',str(inv))
        backend=c.PolicyBackend(api)
        self.assertEqual(backend.read(9),'original')
        backend.write(9,'terra')
        self.assertEqual(backend.read(9),'terra')
        self.assertEqual(api.writes,[('/users/9/openai-model-policy',{'level':'terra'})])
        api.groups[0]['platform']='anthropic'
        with self.assertRaises(c.PolicyError): c.inventory(api)

    def test_http_adapter_error_sanitization_and_contract(self):
        self.assertTrue(hasattr(c,'API'), 'HTTP adapter missing')
        import http.server,threading,json
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path.endswith('/missing'):
                    self.send_response(404);self.end_headers();self.wfile.write(b'SECRET');return
                self.send_response(200);self.end_headers();self.wfile.write(json.dumps({'code':0,'data':{'level':'original'}}).encode())
            def do_PUT(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                self.send_response(200);self.end_headers();self.wfile.write(json.dumps({'code':0,'data':body}).encode())
            def log_message(self,*args): pass
        server=http.server.HTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            api=c.API('http://127.0.0.1:'+str(server.server_port),'SECRET')
            self.assertEqual(api.get('/x'),{'level':'original'})
            self.assertEqual(api.put('/x',{'level':'terra'}),{'level':'terra'})
            with self.assertRaises(c.PolicyError) as error: api.get('/missing')
            self.assertIn('404',str(error.exception));self.assertNotIn('SECRET',str(error.exception))
        finally: server.shutdown();server.server_close();thread.join()

    def test_end_to_end_dryrun_apply_repeat_and_fail_closed(self):
        self.assertTrue(hasattr(c,'run'), 'runner missing')
        import tempfile
        doc={'hour':100,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
        with tempfile.TemporaryDirectory() as d:
            store=c.Store(d);api=FakeAPI()
            summary=c.run(api,store,100,lambda h:doc,apply=False)
            self.assertEqual(summary['targets'][0]['user_id'],9)
            self.assertFalse(store.path.exists());self.assertEqual(api.writes,[])
            c.run(api,store,100,lambda h:doc,apply=True)
            self.assertEqual(api.level,'terra')
            c.run(api,store,100,lambda h:doc,apply=True)
            self.assertEqual(len(api.writes),1)
            bad=dict(doc,unknown_groups=1)
            with self.assertRaises(c.PolicyError): c.run(api,store,101,lambda h:bad,apply=True)
            self.assertEqual(len(api.writes),1)
            for hour in range(101, 108):
                doc['hour']=hour
                c.run(api,store,hour,lambda h:doc,apply=True)
                self.assertEqual(api.level, 'terra' if hour < 107 else 'full')
            self.assertEqual(api.level,'full')
            self.assertEqual(len(api.writes),2)

    def test_stale_pending_recovery_never_restores(self):
        import tempfile
        doc={'hour':101,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
        with tempfile.TemporaryDirectory() as d:
            store=c.Store(d);api=FakeAPI();api.level='terra'
            store.save({'version':1,'users':{'9':{'level':'terra','last':99,'low':1}},'pending':{'users':{'9':{'level':'full','last':100,'low':0}},'ops':[{'id':9,'from':'terra','to':'full'}],'transitions':[],'hour':100}})
            with self.assertRaises(c.PolicyError): c.run(api,store,101,lambda h:doc,apply=True)
            self.assertEqual(api.writes,[])
            self.assertEqual(api.level,'terra')

    def test_high_usage_without_current_keys_and_any_openai_group(self):
        p=c.plan({},100,{9:51_000_000},[],{9:'original'})
        self.assertEqual(p['ops'],[{'id':9,'from':'original','to':'luna'}])
        self.assertEqual(p['users']['9']['recovery_from_hour'],101)
        later=c.plan(p['users'],101,{9:0},[],{9:'luna'})
        self.assertEqual(later['users']['9']['low'],1)
        recovered = later
        for hour in range(102, 109):
            recovered=c.plan(recovered['users'],hour,{9:0},[],{9:'luna'})
        self.assertEqual(recovered['ops'],[{'id':9,'from':'luna','to':'full'}])
        p=c.plan({},100,{9:21_000_000},[{'id':1,'user_id':9,'group_id':99}],{9:'original'})
        self.assertEqual(p['ops'][0]['to'],'terra')

    def test_cli_collect_only_is_readonly_and_always_summary(self):
        self.assertTrue(hasattr(c,'main'), 'CLI missing')
        import tempfile,contextlib,io,json
        with tempfile.TemporaryDirectory() as d:
            api=FakeAPI();out=io.StringIO()
            def collect(h): return {'hour':h,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
            with contextlib.redirect_stdout(out):
                rc=c.main(['--collect-only','--state-dir',d],api=api,collector=collect)
            self.assertEqual(rc,0)
            self.assertEqual(json.loads(out.getvalue())['status'],'collected_only_backend_not_checked')
            self.assertEqual(api.writes,[])
            from pathlib import Path
            self.assertEqual(list(Path(d).iterdir()),[])

    def test_ssh_wrapper_help_and_exact_command(self):
        from pathlib import Path
        self.assertTrue(Path('run_remote.py').exists(),'SSH wrapper missing')
        import run_remote
        cmd=run_remote.command('--collect-only')
        self.assertEqual(cmd[-1],'sudo -n python3 - --collect-only')
        self.assertIn('BatchMode=yes',cmd)
        import subprocess,sys
        result=subprocess.run([sys.executable,'run_remote.py','--help'],capture_output=True,text=True)
        self.assertEqual(result.returncode,0)
        self.assertIn('--apply',result.stdout)

    def test_nonoriginal_policy_without_state_even_without_keys_fails(self):
        with self.assertRaises(c.PolicyError): c.plan({},100,{},[],{9:'full'})

    def test_same_hour_pending_completion_reports_transition(self):
        import tempfile
        doc={'hour':100,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
        with tempfile.TemporaryDirectory() as d:
            store=c.Store(d);api=FakeAPI()
            pending={'users':{'9':{'level':'terra','last':100,'low':1}},'ops':[{'id':9,'from':'original','to':'terra'}],'transitions':[{'user_id':9,'from':'original','to':'terra'}],'hour':100}
            store.save({'version':1,'users':{},'pending':pending})
            result=c.run(api,store,100,lambda h:doc,apply=True)
            self.assertEqual(result['transitions'],pending['transitions'])
            self.assertEqual(len(result['targets']),1)

class FakeAPI:
    def __init__(self):
        self.groups=[{'id':6,'platform':'openai','name':'openai-default','status':'active'},{'id':8,'platform':'openai','name':'openai-max-terra','status':'active'}]
        self.writes=[];self.level='original'
    def get(self,path,params=None):
        if path.endswith('openai-model-policy'): return {'level':self.level}
        if path=='/groups': rows=self.groups
        elif path=='/users': rows=[{'id':9,'username':'test','email':'test@example.test'}]
        elif path=='/users/9/api-keys': rows=[{'id':1,'user_id':9,'group_id':6,'key':'SECRET','status':'active'},{'id':2,'user_id':9,'group_id':8,'key':'SECRET','status':'disabled'}]
        elif path=='/channels':
            mapping={'openai':{'codex-auto-review':'gpt-6-luna','gpt-5.5':'gpt-6-luna','gpt-5.6-sol':'gpt-6-sol'}}
            rows=[dict(id=2,group_ids=[6],model_mapping=mapping,status='active',billing_model_source='channel_mapped',restrict_models=False,features='',features_config={},model_pricing=[],apply_pricing_to_account_stats=False,account_stats_pricing_rules=[]),dict(id=1,group_ids=[8],model_mapping=mapping,status='active',billing_model_source='channel_mapped',restrict_models=False,features='',features_config={},model_pricing=[],apply_pricing_to_account_stats=False,account_stats_pricing_rules=[])]
        else: raise AssertionError(path)
        return {'items':rows,'total':len(rows),'page':1,'page_size':100}
    def put(self,path,payload):
        self.writes.append((path,payload));self.level=payload['level'];return {'level':self.level}

if __name__ == '__main__': unittest.main()
