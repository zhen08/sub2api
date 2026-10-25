import unittest
import tempfile
from pathlib import Path
import controller as c
from test_controller import FakeAPI


class TerraSuspendTests(unittest.TestCase):
    def test_thresholds_and_existing_luna(self):
        self.assertIn('terra_enabled', __import__('inspect').signature(c.decide).parameters)
        for tokens in (0, 20_000_000, 20_000_001, 50_000_000, 50_000_001, 10**18):
            self.assertEqual(c.decide({}, 100, tokens, terra_enabled=False)['level'],
                             'luna' if tokens > 50_000_000 else 'original')
            self.assertEqual(c.decide({'level':'luna','last':99}, 100, tokens,
                                      terra_enabled=False)['level'], 'luna')

    def test_managed_terra_relaxes_but_over_limit_goes_luna(self):
        self.assertIn('terra_enabled', __import__('inspect').signature(c.plan).parameters)
        for hour in (100, 101):
            for tokens in (0, 30_000_000, 50_000_001):
                old={'9':{'level':'terra','last':100}}
                p=c.plan(old,hour,{9:tokens},[],{9:'terra'},terra_enabled=False)
                want='luna' if tokens>50_000_000 else 'full'
                self.assertEqual(p['ops'],[{'id':9,'from':'terra','to':want}])
                self.assertEqual(c.plan(p['users'],hour,{9:tokens},[],{9:want},terra_enabled=False)['ops'],[])
        with self.assertRaises(c.PolicyError):
            c.plan({},100,{},[],{9:'terra'},terra_enabled=False)
        p=c.plan({},100,{},[{'id':1,'user_id':9,'group_id':8}],{9:'original'},terra_enabled=False)
        self.assertEqual(p['ops'],[])

    def test_apply_relaxation_retry_requires_disabled_flag(self):
        self.assertIn('terra_enabled', __import__('inspect').signature(c.run).parameters)
        doc={'hour':100,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
        for lost in (False,True):
            with tempfile.TemporaryDirectory() as d:
                api=FakeAPI();api.level='terra';store=c.Store(d)
                store.save({'version':1,'users':{'9':{'level':'terra','last':100}}})
                put=api.put
                def interrupted(path,payload):
                    if lost: put(path,payload)
                    raise OSError('lost')
                api.put=interrupted
                with self.assertRaises(OSError):
                    c.run(api,store,100,lambda h:doc,apply=True,terra_enabled=False)
                before=store.load()
                with self.assertRaises(c.PolicyError):
                    c.run(api,store,100,lambda h:doc,apply=True)
                self.assertEqual(store.load(),before)
                api.put=put
                c.run(api,store,100,lambda h:doc,apply=True,terra_enabled=False)
                self.assertEqual(api.level,'full');self.assertEqual(len(api.writes),1)
                self.assertEqual(c.run(api,store,100,lambda h:doc,apply=True,terra_enabled=False)['targets'],[])

    def test_pending_terra_escalation_blocked(self):
        with tempfile.TemporaryDirectory() as d:
            api=FakeAPI();store=c.Store(d)
            data={'version':1,'users':{},'pending':{'hour':100,'ops':[{'id':9,'from':'original','to':'terra'}]}}
            store.save(data)
            doc={'hour':100,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
            with self.assertRaises(c.PolicyError):
                c.run(api,store,100,lambda h:doc,apply=True,terra_enabled=False)
            self.assertEqual(store.load(),data);self.assertEqual(api.writes,[])

    def test_config_and_cli(self):
        self.assertTrue(hasattr(c,'load_policy_config'))
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json'
            self.assertEqual(c.load_policy_config(p),{'luna_enabled':True,'terra_enabled':True})
            p.write_text('{"luna_enabled": true}')
            self.assertTrue(c.load_policy_config(p)['terra_enabled'])
            p.write_text('{"luna_enabled": true, "terra_enabled": false}')
            self.assertFalse(c.load_policy_config(p)['terra_enabled'])
            api=FakeAPI();api.level='terra';store=c.Store(d)
            hour=c.processed_hour()
            store.save({'version':1,'users':{'9':{'level':'terra','last':hour}}})
            doc={'hour':hour,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
            self.assertEqual(c.main(['--apply','--state-dir',d],api=api,collector=lambda h:doc),0)
            self.assertEqual(api.level,'full')
            for text in ('{}','{"terra_enabled": false}','{"luna_enabled": true,"terra_enabled":0}',
                         '{"luna_enabled": true,"terra_enabled":false,"unknown":true}'):
                p.write_text(text)
                with self.assertRaises(c.PolicyError): c.load_policy_config(p)

    def test_luna_recovery_unchanged(self):
        state={'level':'luna','last':99}
        for h in range(100,108):
            state=c.decide(state,h,0,terra_enabled=False)
            self.assertEqual(state['level'],'full' if h==107 else 'luna')
        self.assertEqual(len(state['low_windows']),8)
