import unittest,tempfile
from pathlib import Path
import controller as c

class TwelveHourTests(unittest.TestCase):
    def test_twelve_not_eight_reuses_real_evidence_and_resets(self):
        self.assertIn('luna_recovery_hours', __import__('inspect').signature(c.decide).parameters)
        s={'level':'luna','last':99}
        for h in range(100,112):
            s=c.decide(s,h,0,luna_recovery_hours=12)
            self.assertEqual(s['level'],'full' if h==111 else 'luna')
        self.assertEqual(len(s['low_windows']),12)
        for tokens,hour in ((1,111),(0,113)):
            old={'level':'luna','last':110,'low_windows':[c.window_evidence(h,0) for h in range(100,111)]}
            new=c.decide(old,hour,tokens,luna_recovery_hours=12)
            self.assertEqual(new['level'],'luna')
            self.assertEqual(new['low'],0 if tokens else 1)

    def test_config_cli_and_journal_reject_old_eight(self):
        from test_controller import FakeAPI
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'config.json';p.write_text('{"luna_enabled":true,"terra_enabled":false,"luna_recovery_hours":12}')
            try: config=c.load_policy_config(p)
            except c.PolicyError: self.fail('12-hour configuration not supported')
            api=FakeAPI();api.level='luna';store=c.Store(d);hour=c.processed_hour()
            old={'level':'luna','last':hour-1,'low_windows':[c.window_evidence(h,0) for h in range(hour-11,hour)]}
            store.save({'version':1,'users':{'9':old}})
            doc={'hour':hour,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
            self.assertEqual(c.main(['--apply','--state-dir',d],api=api,collector=lambda h:doc),0)
            self.assertEqual(api.level,'full')
            self.assertEqual(len(store.load()['users']['9']['low_windows']),12)
            self.assertEqual(c.run(api,store,hour,lambda h:doc,apply=True,**config)['targets'],[])
            # Legacy 8-window recovery intent cannot write, even if backend already says full.
            for live in ('luna','full'):
                api.level=live
                old={'level':'luna','last':hour-1,'low_windows':[c.window_evidence(h,0) for h in range(hour-7,hour)]}
                plan=c.plan({'9':old},hour,{9:0},[],{9:'luna'})
                data={'version':1,'users':{'9':old},'pending':plan};store.save(data)
                before=len(api.writes)
                with self.assertRaises(c.PolicyError):c.run(api,store,hour,lambda h:doc,apply=True,**config)
                self.assertEqual(len(api.writes),before);self.assertEqual(store.load(),data)
            for value in (True,0,1,13,'12'):
                p.write_text(__import__('json').dumps({'luna_enabled':True,'luna_recovery_hours':value}))
                with self.assertRaises(c.PolicyError):c.load_policy_config(p)
