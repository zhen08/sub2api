import unittest,tempfile
from pathlib import Path
import controller as c
from test_controller import FakeAPI

class HalfHourTests(unittest.TestCase):
    def test_threshold_priority_idempotency_and_hourly_disabled(self):
        for n,want in ((10_000_000,'original'),(10_000_001,'terra')):
            p=c.plan({},100,{9:25_000_000},[],{9:'original'},terra_half_hour=True,half_slot=201,half_totals={9:n})
            self.assertEqual(p['users']['9']['level'],want)
            again=c.plan(p['users'],100,{9:25_000_000},[],{9:want},terra_half_hour=True,half_slot=201,half_totals={9:n})
            self.assertEqual(again['ops'],[])
        p=c.plan({},100,{9:50_000_001},[],{9:'original'},terra_half_hour=True,half_slot=201,half_totals={9:20_000_000})
        self.assertEqual(p['users']['9']['level'],'luna')

    def test_half_tick_never_advances_recovery_twice_and_resets_terra(self):
        old={'9':{'level':'terra','last':99}}
        p=c.plan(old,100,{9:0},[],{9:'terra'},terra_half_hour=True,half_slot=201,half_totals={9:0})
        self.assertEqual(p['users']['9']['low'],1)
        q=c.plan(p['users'],100,{9:0},[],{9:'terra'},terra_half_hour=True,half_slot=202,half_totals={9:0})
        self.assertEqual(q['users']['9']['low'],1)
        r=c.plan(p['users'],100,{9:0},[],{9:'terra'},terra_half_hour=True,half_slot=202,half_totals={9:10_000_001})
        self.assertEqual(r['users']['9']['low'],0)
        self.assertEqual(r['users']['9']['last'],100)
        self.assertEqual(r['users']['9']['recovery_from_hour'],102)

    def test_sql_half_boundaries_and_config(self):
        s=c.usage_sql(201,1800)
        self.assertIn('to_timestamp(361800)',s);self.assertIn('to_timestamp(363600)',s)
        self.assertIn('cache_read_tokens',s)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json';p.write_text('{"luna_enabled":true,"terra_half_hour":true}')
            self.assertTrue(c.load_policy_config(p)['terra_half_hour'])
        with self.assertRaises(c.PolicyError):c.usage_sql(2,900)

    def test_run_lost_response_retry_and_slot_journal_guard(self):
        def doc(slot,n):
            return {'hour':slot,'all_rows':1,'openai_rows':1,'unknown_groups':0,'invalid_tokens':0,
                'users':[{'user_id':9,'rows':1,'keys':1,'input':n,'output':0,'cache_creation':0,'cache_read':0,'total':n}]}
        with tempfile.TemporaryDirectory() as d:
            api=FakeAPI();store=c.Store(d);put=api.put
            def lost(path,payload):
                put(path,payload);raise OSError('lost response')
            api.put=lost
            kw=dict(terra_half_hour=True,half_slot=201,half_collector=lambda h:doc(h,10_000_001),apply=True)
            with self.assertRaises(OSError):c.run(api,store,100,lambda h:doc(h,10_000_001),**kw)
            pending=store.load()
            with self.assertRaisesRegex(c.PolicyError,'stale half-hour'):
                c.run(api,store,100,lambda h:doc(h,10_000_001),**dict(kw,half_slot=202))
            self.assertEqual(store.load(),pending)
            api.put=put
            result=c.run(api,store,100,lambda h:doc(h,10_000_001),**kw)
            self.assertEqual(result['half_slot'],201)
            self.assertEqual(result['half_usage'],doc(201,10_000_001))
            self.assertEqual(len(api.writes),1)
            self.assertEqual(c.run(api,store,100,lambda h:doc(h,10_000_001),**kw)['targets'],[])
            self.assertNotIn('pending',store.load())

    def test_recovery_requires_full_hours_and_half_only_users(self):
        s={'9':{'level':'luna','last':99}}
        for hour in range(100,112):
            p=c.plan(s,hour,{9:0},[],{9:s['9']['level']},terra_half_hour=True,
                     half_slot=2*hour+1,half_totals={},luna_recovery_hours=12)
            s=p['users'];self.assertEqual(s['9']['level'],'full' if hour==111 else 'luna')
            p=c.plan(s,hour,{9:0},[],{9:s['9']['level']},terra_half_hour=True,
                     half_slot=2*hour+2,half_totals={},luna_recovery_hours=12)
            s=p['users'];self.assertEqual(s['9']['level'],'full' if hour==111 else 'luna')
        p=c.plan({},100,{},[],{9:'original'},terra_half_hour=True,half_slot=202,half_totals={9:10_000_001})
        self.assertEqual(p['ops'],[{'id':9,'from':'original','to':'terra'}])
