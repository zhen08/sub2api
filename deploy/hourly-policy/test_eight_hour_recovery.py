import copy
import tempfile
import unittest
from unittest.mock import Mock


def windows(start, end):
    return [c.window_evidence(h, 0) for h in range(start, end + 1)]


def recovery_plan(evidence):
    return {'hour': 107, 'users': {'9': {'level': 'full', 'last': 107,
            'low': 0, 'low_windows': evidence}},
            'ops': [{'id': 8, 'from': 'original', 'to': 'terra'},
                    {'id': 9, 'from': 'luna', 'to': 'full'}], 'transitions': []}

import controller as c


class EightHourRecoveryTests(unittest.TestCase):
    def test_positive_usage_resets_seven_hour_streak(self):
        for level in ('terra', 'luna'):
            for tokens in (1, 9_999_999, 20_000_000, 20_000_001, 50_000_000, 50_000_001):
                with self.subTest(level=level, tokens=tokens):
                    state = {'level': level, 'last': 106, 'low': 7,
                             'low_windows': windows(100, 106)}
                    reset = c.decide(state, 107, tokens)
                    expected = 'luna' if level == 'luna' or tokens > 50_000_000 else 'terra'
                    self.assertEqual(reset['level'], expected)
                    self.assertEqual(reset['low'], 0)
                    self.assertEqual(reset['low_windows'], [])
                    for hour in range(108, 116):
                        reset = c.decide(reset, hour, 0)
                        self.assertEqual(reset['level'], expected if hour < 115 else 'full')

    def test_gap_and_duplicate_cannot_advance_seven_hour_streak(self):
        state = {'level': 'terra', 'last': 106, 'low': 7,
                 'low_windows': windows(100, 106)}
        self.assertEqual(c.decide(state, 106, 0), state)
        self.assertEqual(c.decide(state, 105, 0), state)
        gap = c.decide(state, 108, 0)
        self.assertEqual(gap['level'], 'terra')
        self.assertEqual(gap['low_windows'], windows(108, 108))

    def test_migration_carries_only_verified_contiguous_suffix(self):
        cases = [
            (None, 0, windows(107, 107)),
            (windows(106, 106), 0, windows(106, 107)),
            (windows(100, 106), 0, windows(100, 107)),
            ([c.window_evidence(103, 1)] + windows(104, 106), 0, windows(104, 107)),
            (windows(100, 101) + windows(104, 106), 0, windows(104, 107)),
            (windows(100, 106), 104, windows(104, 107)),
            (windows(100, 106), 107, windows(107, 107)),
        ]
        for prior, guard, expected in cases:
            for count in (0, 99):
                with self.subTest(prior=prior, guard=guard, count=count):
                    state = {'level': 'luna', 'last': 106, 'low': count,
                             'low_windows': prior, 'recovery_from_hour': guard}
                    original = copy.deepcopy(state)
                    nxt = c.decide(state, 107, 0)
                    self.assertEqual(state, original)
                    self.assertEqual(nxt['low_windows'], expected)
                    self.assertEqual(nxt['level'], 'full' if len(expected) == 8 else 'luna')
                    self.assertEqual(nxt['low'], 0 if len(expected) == 8 else len(expected))

    def test_corrupt_or_incomplete_eight_window_intents_fail_closed(self):
        variants = [None, [], windows(101, 107), windows(99, 107),
                    windows(100, 106) + windows(106, 106),
                    list(reversed(windows(100, 107)))]
        for index in range(8):
            for field, value in (('hourly_tokens', 1), ('hourly_tokens', None),
                                 ('hour_start', 'invalid'), ('hour_end', 'invalid'),
                                 ('extra', 0)):
                bad = windows(100, 107)
                bad[index][field] = value
                variants.append(bad)
            bad = windows(100, 107)
            bad[index] = None
            variants.append(bad)
        for evidence in variants:
            with self.subTest(evidence=evidence), tempfile.TemporaryDirectory() as d:
                store = c.Store(d)
                store.save = Mock(side_effect=AssertionError('journal write'))
                read, write = Mock(), Mock()
                with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                    c.commit_plan(store, recovery_plan(evidence), read, write)
                store.save.assert_not_called()
                read.assert_not_called()
                write.assert_not_called()

    def test_eight_window_intent_must_respect_recovery_guard_and_identity(self):
        for field, value in (('recovery_from_hour', 101), ('last', 106), ('level', 'terra')):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as d:
                planned = recovery_plan(windows(100, 107))
                planned['users']['9'][field] = value
                store = c.Store(d)
                read, write = Mock(), Mock()
                with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                    c.commit_plan(store, planned, read, write)
                read.assert_not_called()
                write.assert_not_called()
                self.assertFalse(store.path.exists())

    def test_noncanonical_numeric_evidence_never_counts_or_commits(self):
        for field, value in (('hourly_tokens', False), ('hourly_tokens', 0.0), ('hour', 106.0)):
            with self.subTest(field=field, value=value):
                prior = windows(100, 106)
                prior[-1][field] = value
                state = c.decide({'level': 'luna', 'last': 106, 'low_windows': prior}, 107, 0)
                self.assertEqual(state['level'], 'luna')
                self.assertEqual(state['low_windows'], windows(107, 107))
                planned = recovery_plan(prior + windows(107, 107))
                with tempfile.TemporaryDirectory() as d:
                    store = c.Store(d)
                    read, write = Mock(), Mock()
                    with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                        c.commit_plan(store, planned, read, write)
                    read.assert_not_called()
                    write.assert_not_called()
                    self.assertFalse(store.path.exists())

    def test_old_two_window_intents_block_before_any_journal_or_policy_write(self):
        for live in ('luna', 'full'):
            for pending in (False, True):
                with self.subTest(live=live, pending=pending), tempfile.TemporaryDirectory() as d:
                    store = c.Store(d)
                    planned = recovery_plan(windows(106, 107))
                    original = {'version': 1, 'users': {}}
                    if pending:
                        original['pending'] = planned
                    store.save(original)
                    before = store.path.read_bytes()
                    store.save = Mock(side_effect=AssertionError('journal write'))
                    read, write = Mock(return_value=live), Mock()
                    with self.assertRaisesRegex(c.PolicyError, 'recovery evidence'):
                        c.commit_plan(store, planned, read, write)
                    store.save.assert_not_called()
                    read.assert_not_called()
                    write.assert_not_called()
                    self.assertEqual(store.path.read_bytes(), before)

    def test_valid_eight_window_legacy_pending_can_resume(self):
        for live in ('luna', 'full'):
            with self.subTest(live=live), tempfile.TemporaryDirectory() as d:
                store = c.Store(d)
                planned = recovery_plan(windows(100, 107))
                store.save({'version': 1, 'users': {}, 'pending': planned})
                remote = {8: 'original', 9: live}
                writes = []
                def write(uid, level):
                    writes.append((uid, level))
                    remote[uid] = level
                c.commit_plan(store, planned, remote.get, write)
                self.assertEqual(remote, {8: 'terra', 9: 'full'})
                self.assertEqual(writes, [(8, 'terra')] + ([(9, 'full')] if live == 'luna' else []))
                self.assertNotIn('pending', store.load())
                self.assertEqual(store.load()['users'], planned['users'])

    def test_seven_stays_restricted_eighth_recovers_with_all_evidence(self):
        for level in ('terra', 'luna'):
            with self.subTest(level=level):
                state = {'level': level, 'last': 99}
                for hour in range(100, 108):
                    state = c.decide(state, hour, 0)
                    self.assertEqual(state['level'], level if hour < 107 else 'full')
                    self.assertEqual(state['low_windows'],
                                     [c.window_evidence(h, 0) for h in range(100, hour + 1)])
                    self.assertEqual(state['low'], hour - 99 if hour < 107 else 0)
                    self.assertEqual(c.decide(state, hour, 0), state)
