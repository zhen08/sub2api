import json
import tempfile
import unittest
import controller as c
from test_controller import FakeAPI
import test_remote_config


class CurrentAPI(FakeAPI):
    def __init__(self):
        super().__init__()
        self.groups = [g for g in self.groups if g['id'] != 8]
        self.orphan_key = False
        self.bad_channel = False

    def get(self, path, params=None):
        data = super().get(path, params)
        if path.endswith('/api-keys') and not self.orphan_key:
            data['items'] = [k for k in data['items'] if k['group_id'] != 8]
            data['total'] = len(data['items'])
        if path == '/channels' and self.bad_channel:
            data['items'][1]['status'] = 'disabled'
        return data


class OptionalTerraGroupTests(unittest.TestCase):
    def test_missing_legacy_terra_group_does_not_disable_user_policy(self):
        api = CurrentAPI()
        self.assertEqual(c.inventory(api)['openai_key_count'], 1)
        for enabled in (False, True):
            with tempfile.TemporaryDirectory() as d:
                doc = {'hour':100,'all_rows':1,'openai_rows':1,'unknown_groups':0,'invalid_tokens':0,
                       'users':[{'user_id':9,'rows':1,'keys':1,'input':20_000_001,'output':0,
                                 'cache_creation':0,'cache_read':0,'total':20_000_001}]}
                result = c.run(api,c.Store(d),100,lambda h:doc,terra_enabled=enabled,luna_recovery_hours=12)
                self.assertEqual(result['status'],'ok')
                self.assertEqual([t['to'] for t in result['targets']], ['terra'] if enabled else [])
        self.assertEqual(api.writes, [])

    def test_present_terra_and_required_default_still_strict(self):
        for gid in (6,8):
            for field,value in (('platform','anthropic'),('status','disabled'),('name','wrong')):
                api = FakeAPI()
                next(g for g in api.groups if g['id']==gid)[field]=value
                with self.assertRaisesRegex(c.PolicyError, 'source group configuration drift'):
                    c.inventory(api)
        api = CurrentAPI();api.groups=[]
        with self.assertRaisesRegex(c.PolicyError, 'source group configuration drift'):
            c.inventory(api)

    def test_orphan_key_and_orphan_channel_guards_preserved(self):
        api=CurrentAPI();api.orphan_key=True
        with self.assertRaisesRegex(c.PolicyError,'key has unknown/unbound group'):
            c.inventory(api)
        api=CurrentAPI();api.bad_channel=True
        with self.assertRaisesRegex(c.PolicyError,'source channel configuration drift'):
            c.inventory(api)

    def test_group_drift_diagnostic_static_allowlist(self):
        helper=test_remote_config.RemoteConfigTests()
        safe='source group configuration drift'
        records=helper.run_failure(json.dumps({'status':'error','error':safe,'details':'SECRET'}).encode())
        self.assertEqual(records[0].get('remote_error'),safe)
        self.assertNotIn('SECRET',json.dumps(records))
        records=helper.run_failure(json.dumps({'status':'error','error':safe+' SECRET'}).encode())
        self.assertTrue(all('remote_error' not in r for r in records))
