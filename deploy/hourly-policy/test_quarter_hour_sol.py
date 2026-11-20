import contextlib
import datetime as dt
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import controller as c
from test_controller import FakeAPI


class QuarterHourSolTests(unittest.TestCase):
    def test_controller_rejects_retry_after_quarter_rollover(self):
        class Clock(dt.datetime):
            @classmethod
            def now(cls, tz):
                return cls.fromtimestamp(405 * 900, tz)

        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'config.json').write_text(
                '{"luna_enabled": true, "sol_quarter_hour": true}')
            output = io.StringIO()
            api = FakeAPI()
            with patch.object(c.dt, 'datetime', Clock), contextlib.redirect_stdout(output):
                status = c.main(['--apply', '--state-dir', directory,
                                 '--expected-hour', '100', '--expected-quarter-slot', '403'],
                                api=api)
            self.assertEqual(status, 1)
            self.assertIn('quarter-hour mismatch/rollover', output.getvalue())
            self.assertFalse(Path(directory, 'state.json').exists())
            self.assertEqual(api.writes, [])

    def test_threshold_and_tier_priority(self):
        for quarter_tokens, half_tokens, hourly_tokens, expected in (
            (5_000_000, 0, 0, 'original'),
            (5_000_001, 0, 0, 'sol'),
            (6_000_000, 10_000_001, 0, 'terra'),
            (6_000_000, 10_000_001, 50_000_001, 'luna'),
        ):
            with self.subTest(expected=expected):
                plan = c.plan({}, 100, {9: hourly_tokens}, [], {9: 'original'},
                              terra_half_hour=True, half_slot=202, half_totals={9: half_tokens},
                              quarter_slot=404, quarter_totals={9: quarter_tokens})
                self.assertEqual(plan['users']['9']['level'], expected)
                if expected == 'sol':
                    transition, = plan['transitions']
                    self.assertEqual(transition['reason'], 'quarter_hour_tokens_gt_5000000')
                    self.assertEqual(transition['quarter_hour_tokens'], quarter_tokens)
        for level in ('terra', 'luna'):
            plan = c.plan({'9': {'level': level, 'last': 100}}, 100, {}, [], {9: level},
                          quarter_slot=405, quarter_totals={9: 5_000_001})
            self.assertEqual(plan['ops'], [])

    def test_recovery_uses_complete_hours_and_can_be_committed(self):
        users = {'9': {'level': 'sol', 'last': 100, 'last_quarter_slot': 403}}
        plan = c.plan(users, 100, {}, [], {9: 'sol'},
                      quarter_slot=404, quarter_totals={9: 5_000_001})
        self.assertEqual(plan['users']['9']['recovery_from_hour'], 102)
        repeat = c.plan(plan['users'], 100, {}, [], {9: 'sol'},
                        quarter_slot=404, quarter_totals={9: 6_000_000})
        self.assertEqual(repeat['users'], plan['users'])
        for hour in range(101, 110):
            plan = c.plan(plan['users'], hour, {}, [], {9: 'sol'},
                          quarter_slot=4 * hour + 3, quarter_totals={})
            self.assertEqual(plan['users']['9']['level'], 'full' if hour == 109 else 'sol')
            if hour < 109:
                repeat = c.plan(plan['users'], hour, {}, [], {9: 'sol'},
                                quarter_slot=4 * hour + 4, quarter_totals={})
                self.assertEqual(repeat['users']['9']['low'], plan['users']['9']['low'])
                plan = repeat

        with tempfile.TemporaryDirectory() as directory:
            live = {9: 'sol'}
            c.commit_plan(c.Store(directory), plan, live.__getitem__, live.__setitem__)
            self.assertEqual(live, {9: 'full'})

    def test_collect_apply_and_reject_stale_quarter_intent(self):
        self.assertIn('>= to_timestamp(363600)', c.usage_sql(404, 900))
        self.assertIn('< to_timestamp(364500)', c.usage_sql(404, 900))

        def doc(slot, tokens=0):
            return {
                'hour': slot, 'all_rows': 1, 'openai_rows': 1, 'unknown_groups': 0,
                'invalid_tokens': 0, 'users': [
                    {'user_id': 9, 'rows': 1, 'keys': 1, 'input': tokens, 'output': 0,
                     'cache_creation': 0, 'cache_read': 0, 'total': tokens},
                ],
            }

        with tempfile.TemporaryDirectory() as directory:
            api = FakeAPI()
            store = c.Store(directory)
            put = api.put

            def lost_response(path, payload):
                put(path, payload)
                raise OSError('lost response')

            api.put = lost_response
            options = dict(terra_half_hour=True, half_slot=202, half_collector=doc,
                           sol_quarter_hour=True, quarter_slot=404,
                           quarter_collector=lambda slot: doc(slot, 5_000_001))
            with self.assertRaises(OSError):
                c.run(api, store, 100, doc, apply=True, **options)
            saved = store.load()
            with self.assertRaisesRegex(c.PolicyError, 'stale quarter-hour'):
                c.run(api, store, 100, doc, apply=True, **dict(options, quarter_slot=405))
            self.assertEqual(store.load(), saved)

            api.put = put
            c.run(api, store, 100, doc, apply=True, **options)
            self.assertEqual(len(api.writes), 1)
            self.assertEqual(api.level, 'sol')
            self.assertNotIn('pending', store.load())
