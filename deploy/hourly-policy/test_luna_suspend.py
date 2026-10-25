import unittest
import controller as c


class LunaSuspendTests(unittest.TestCase):
    def test_disabled_caps_even_huge_usage_at_terra(self):
        for tokens, expected in ((20_000_000, 'original'), (20_000_001, 'terra'),
                                 (50_000_000, 'terra'), (50_000_001, 'terra'),
                                 (10**18, 'terra')):
            with self.subTest(tokens=tokens):
                self.assertEqual(c.decide({}, 100, tokens, luna_enabled=False)['level'], expected)

    def test_managed_luna_same_hour_relaxes_without_fabricating_recovery(self):
        old = {'level': 'luna', 'last': 100, 'low': 3,
               'low_windows': [c.window_evidence(h, 0) for h in range(98, 101)],
               'recovery_from_hour': 98}
        p = c.plan({'9': old}, 100, {9: 10**18}, [], {9: 'luna'}, luna_enabled=False)
        self.assertEqual(p['ops'], [{'id': 9, 'from': 'luna', 'to': 'terra'}])
        self.assertEqual(p['users']['9'], dict(old, level='terra'))
        self.assertEqual(p['transitions'][0]['reason'], 'automatic_luna_suspended')
        self.assertEqual(c.plan(p['users'], 100, {}, [], {9: 'terra'}, luna_enabled=False)['ops'], [])

    def test_unmanaged_or_drifted_luna_fails_closed(self):
        for users in ({}, {'9': {'level': 'terra', 'last': 99}}):
            with self.assertRaises(c.PolicyError):
                c.plan(users, 100, {}, [], {9: 'luna'}, luna_enabled=False)

    def test_recovery_still_requires_eight_complete_zero_windows(self):
        state = {'level': 'luna', 'last': 99}
        for hour in range(100, 108):
            state = c.decide(state, hour, 0, luna_enabled=False)
            self.assertEqual(state['level'], 'full' if hour == 107 else 'terra')
        self.assertEqual(len(state['low_windows']), 8)

    def test_default_keeps_original_luna_behavior(self):
        self.assertEqual(c.decide({}, 100, 50_000_001)['level'], 'luna')
        self.assertEqual(c.decide({'level': 'luna'}, 100, 20_000_001)['level'], 'luna')

    def test_config_is_explicit_boolean_and_backwards_compatible(self):
        import tempfile
        from pathlib import Path
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'config.json'
            self.assertTrue(c.load_luna_enabled(p))
            p.write_text('{"luna_enabled": false}')
            self.assertFalse(c.load_luna_enabled(p))
            for text in ('{"luna_enabled": "false"}', '{"luna_enabled": 0}',
                         '{"wrong": false}', '[]', '{bad'):
                p.write_text(text)
                with self.assertRaises(c.PolicyError):
                    c.load_luna_enabled(p)

    def test_run_relaxation_journal_retry_and_notice(self):
        import tempfile
        from test_controller import FakeAPI
        for response_lost in (False, True):
            with tempfile.TemporaryDirectory() as d:
                api = FakeAPI(); api.level = 'luna'
                store = c.Store(d)
                store.save({'version': 1, 'users': {'9': {'level': 'luna', 'last': 100}}})
                doc = {'hour': 100, 'all_rows': 0, 'openai_rows': 0,
                       'unknown_groups': 0, 'invalid_tokens': 0, 'users': []}
                put = api.put
                def interrupted(path, payload):
                    if response_lost:
                        put(path, payload)
                    raise OSError('interrupted')
                api.put = interrupted
                with self.assertRaises(OSError):
                    c.run(api, store, 100, lambda h: doc, apply=True, luna_enabled=False)
                api.put = put
                summary = c.run(api, store, 100, lambda h: doc, apply=True, luna_enabled=False)
                self.assertEqual(api.level, 'terra')
                self.assertEqual(len(api.writes), 1)
                self.assertEqual(summary['targets'][0]['from'], 'luna')
                self.assertEqual(summary['targets'][0]['to'], 'terra')
                self.assertIn('email', summary['targets'][0])
                self.assertEqual(c.run(api, store, 100, lambda h: doc, apply=True,
                                       luna_enabled=False)['targets'], [])

    def test_pending_luna_escalation_is_preserved_without_writes(self):
        import tempfile
        from test_controller import FakeAPI
        with tempfile.TemporaryDirectory() as d:
            api = FakeAPI(); store = c.Store(d)
            data = {'version': 1, 'users': {}, 'pending': {
                'hour': 100, 'ops': [{'id': 9, 'from': 'original', 'to': 'luna'}]}}
            store.save(data)
            doc = {'hour': 100, 'all_rows': 0, 'openai_rows': 0,
                   'unknown_groups': 0, 'invalid_tokens': 0, 'users': []}
            with self.assertRaisesRegex(c.PolicyError, 'suspended'):
                c.run(api, store, 100, lambda h: doc, apply=True, luna_enabled=False)
            self.assertEqual(api.writes, [])
            self.assertEqual(store.load(), data)
