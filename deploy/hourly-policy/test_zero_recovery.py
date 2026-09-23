import unittest

import controller as c


class ZeroRecoveryTests(unittest.TestCase):
    def test_legacy_counter_requires_contiguous_positive_zero_evidence(self):
        for prior in (None, [], [c.window_evidence(100, None)],
                      [c.window_evidence(100, 1)], [c.window_evidence(99, 0)],
                      [{'hour': 100, 'hourly_tokens': 0}],
                      [dict(c.window_evidence(100, 0), hour_end='invalid')]):
            with self.subTest(prior=prior):
                state = {'level': 'luna', 'last': 100, 'low': 99,
                         'low_windows': prior}
                nxt = c.decide(state, 101, 0)
                self.assertEqual(nxt['level'], 'luna')
                self.assertEqual(nxt['low'], 1)
                self.assertEqual(nxt['low_windows'], [c.window_evidence(101, 0)])
                for hour in range(102, 109):
                    nxt = c.decide(nxt, hour, 0)
                self.assertEqual(nxt['level'], 'full')

    def test_proven_zero_counts_without_trusting_legacy_counter(self):
        state = {'level': 'terra', 'last': 100, 'low': 0,
                 'low_windows': [c.window_evidence(100, 0)]}
        self.assertEqual(c.decide(state, 101, 0)['low'], 2)
        recovered = state
        for hour in range(101, 108):
            recovered = c.decide(recovered, hour, 0)
        self.assertEqual(recovered['level'], 'full')
        state['recovery_from_hour'] = 101
        self.assertEqual(c.decide(state, 101, 0)['level'], 'terra')

    def test_pending_recovery_requires_eight_zero_windows_before_any_write(self):
        import tempfile
        from test_controller import FakeAPI
        for evidence in ([], [c.window_evidence(99, None), c.window_evidence(100, 0)],
                         [c.window_evidence(99, 1), c.window_evidence(100, 0)],
                         [c.window_evidence(98, 0), c.window_evidence(100, 0)]):
            for live in ('terra', 'full'):
                with self.subTest(evidence=evidence, live=live), tempfile.TemporaryDirectory() as d:
                    api = FakeAPI(); api.level = live
                    store = c.Store(d)
                    pending = {'hour': 100, 'users': {'9': {'level': 'full', 'last': 100,
                               'low': 0, 'low_windows': evidence}},
                               'ops': [{'id': 9, 'from': 'terra', 'to': 'full'}],
                               'transitions': []}
                    original = {'version': 1, 'users': {'9': {'level': 'terra', 'last': 99, 'low': 1}},
                                'pending': pending}
                    store.save(original)
                    doc = {'hour': 100, 'all_rows': 0, 'openai_rows': 0,
                           'unknown_groups': 0, 'invalid_tokens': 0, 'users': []}
                    with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                        c.run(api, store, 100, lambda h: doc, apply=True)
                    self.assertEqual(api.writes, [])
                    self.assertEqual(store.load(), original)

    def test_invalid_recovery_rejects_entire_write_set(self):
        import tempfile
        from unittest.mock import Mock
        with tempfile.TemporaryDirectory() as d:
            store = c.Store(d)
            planned = {'hour': 100, 'users': {'9': {'level': 'full'}},
                       'ops': [{'id': 8, 'from': 'original', 'to': 'terra'},
                               {'id': 9, 'from': 'terra', 'to': 'full'}], 'transitions': []}
            read, write = Mock(return_value='original'), Mock()
            with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                c.commit_plan(store, planned, read, write)
            read.assert_not_called()
            write.assert_not_called()
            self.assertFalse(store.path.exists())

    def test_valid_zero_recovery_journal_retries_preserve_evidence(self):
        import tempfile
        from test_controller import FakeAPI
        for response_lost in (False, True):
            with self.subTest(response_lost=response_lost), tempfile.TemporaryDirectory() as d:
                store = c.Store(d); api = FakeAPI(); api.level = 'luna'
                first = {'level': 'luna', 'last': 92}
                for hour in range(93, 100):
                    first = c.decide(first, hour, 0)
                store.save({'version': 1, 'users': {'9': first}})
                doc = {'hour': 100, 'all_rows': 0, 'openai_rows': 0,
                       'unknown_groups': 0, 'invalid_tokens': 0, 'users': []}
                put = api.put
                def interrupted(path, payload):
                    if response_lost:
                        put(path, payload)
                    raise OSError('interrupted')
                api.put = interrupted
                with self.assertRaises(OSError):
                    c.run(api, store, 100, lambda h: doc, apply=True)
                pending = store.load()['pending']
                api.put = put
                summary = c.run(api, store, 100, lambda h: doc, apply=True)
                self.assertEqual(api.level, 'full')
                self.assertEqual(len(api.writes), 1)
                self.assertEqual(summary['transitions'], pending['transitions'])
                self.assertEqual(summary['targets'][0]['low_streak'],
                                 [c.window_evidence(h, 0) for h in range(93, 101)])
                self.assertNotIn('pending', store.load())
                self.assertEqual(c.run(api, store, 100, lambda h: doc, apply=True)['targets'], [])
                self.assertEqual(len(api.writes), 1)

    def test_positive_windows_never_recover_and_gaps_restart(self):
        for level in ('terra', 'luna'):
            for tokens in (1, 42, 9_999_999, 10_000_000, 20_000_000):
                with self.subTest(level=level, tokens=tokens):
                    s = {'level': level, 'last': 99}
                    s = c.decide(c.decide(s, 100, tokens), 101, tokens)
                    self.assertEqual(s['level'], level)
                    self.assertEqual(s['low'], 0)
                    first = c.decide(s, 102, 0)
                    self.assertEqual(c.decide(first, 102, 0), first)
                    gap = c.decide(first, 104, 0)
                    self.assertEqual(gap['level'], level)
                    self.assertEqual(gap['low_windows'], [c.window_evidence(104, 0)])
                    for hour in range(105, 112):
                        gap = c.decide(gap, hour, 0)
                    self.assertEqual(gap['level'], 'full')

    def test_migration_preserves_restrictions_and_already_full_users(self):
        import tempfile
        from test_controller import FakeAPI
        for level in ('terra', 'luna', 'full'):
            with self.subTest(level=level), tempfile.TemporaryDirectory() as d:
                store = c.Store(d); api = FakeAPI(); api.level = level
                store.save({'version': 1, 'users': {'9': {'level': level, 'last': 99, 'low': 100}}})
                doc = {'hour': 100, 'all_rows': 0, 'openai_rows': 0,
                       'unknown_groups': 0, 'invalid_tokens': 0, 'users': []}
                summary = c.run(api, store, 100, lambda h: doc, apply=True)
                self.assertEqual(summary['targets'], [])
                self.assertEqual(api.level, level)
                self.assertEqual(api.writes, [])
                self.assertEqual(store.load()['users']['9']['level'], level)

    def test_one_token_resets_zero_streak(self):
        for level in ('terra', 'luna'):
            with self.subTest(level=level):
                first = c.decide({'level': level, 'last': 99}, 100, 0)
                positive = c.decide(first, 101, 1)
                self.assertEqual(positive['level'], level)
                self.assertEqual(positive['low'], 0)
                self.assertEqual(positive['low_windows'], [])
                restart = c.decide(positive, 102, 0)
                self.assertEqual(restart['level'], level)
                for hour in range(103, 110):
                    restart = c.decide(restart, hour, 0)
                self.assertEqual(restart['level'], 'full')
