"""Deterministic retry scheduling; no real SSH or real sleeps."""
import contextlib
import io
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

import run_remote as r


def transport(code='timeout'):
    result = r.SSHResult([], 255, b'DUMMY-SECRET', b'present')
    result.ssh_error = code
    return result


class RetryScheduleTests(unittest.TestCase):
    def invoke(self, results, durations=None, start=101 * 3600 + 300, sleep_hook=None):
        clock = {'mono': 0.0, 'wall': float(start)}
        calls, sleeps = [], []
        out = io.StringIO()
        def advance(seconds):
            clock['mono'] += seconds
            clock['wall'] += seconds
        def attempt(cmd, source, timeout, limit):
            index = len(calls)
            calls.append((clock['mono'], timeout, cmd))
            advance(durations[index] if durations else 0)
            result = results[index]
            if isinstance(result, Exception):
                raise result
            return result
        def sleep(seconds):
            sleeps.append(seconds)
            advance(seconds)
            if sleep_hook:
                sleep_hook(clock)
        with patch.dict(os.environ, {'HOURLY_POLICY_SSH_HOST': 'fixture.test'}, clear=True), \
                patch.object(r, 'bounded_process', side_effect=attempt), \
                patch.object(r.time, 'monotonic', side_effect=lambda: clock['mono']), \
                patch.object(r.time, 'time', side_effect=lambda: clock['wall']), \
                patch.object(r.time, 'sleep', side_effect=sleep), \
                patch.object(sys, 'argv', ['run_remote.py', '--apply']), \
                contextlib.redirect_stdout(out):
            code = r.main()
        return code, out.getvalue(), calls, sleeps, clock

    def test_full_attempt_budgets_fit_900_second_window_including_cleanup(self):
        code, output, calls, sleeps, clock = self.invoke([transport()] * 3, [270, 270, 270])
        self.assertEqual([c[0] for c in calls], [0, 300, 630])
        self.assertEqual([c[1] for c in calls], [269, 269, 269])
        self.assertEqual(sleeps, [30, 60])
        self.assertEqual(clock['mono'], 900)
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.splitlines()[-1])['error'], 'retries_exhausted')

    def test_sleep_overrun_clips_last_attempt_to_exact_remaining_budget(self):
        def overrun(clock):
            if clock['mono'] == 630:
                clock['mono'] += 100
                clock['wall'] += 100
        code, _, calls, _, _ = self.invoke([transport()] * 3, sleep_hook=overrun)
        self.assertEqual([c[0] for c in calls], [0, 300, 730])
        self.assertEqual([c[1] for c in calls], [269, 269, 169])
        self.assertEqual(code, 1)

    def test_budget_spent_during_sleep_never_starts_another_attempt(self):
        def overrun(clock):
            clock['mono'] = 900
        code, output, calls, _, _ = self.invoke([transport()] * 3, sleep_hook=overrun)
        self.assertEqual((code, len(calls)), (1, 1))
        self.assertEqual(json.loads(output.splitlines()[-1])['error'], 'deadline_exhausted')

    def test_hour_rollover_during_sleep_never_retargets_or_starts_process(self):
        def rollover(clock):
            clock['wall'] = 102 * 3600
        code, output, calls, _, _ = self.invoke([transport()] * 3, sleep_hook=rollover)
        self.assertEqual((code, len(calls)), (1, 1))
        records = [json.loads(line) for line in output.splitlines()]
        self.assertEqual(records[-1]['error'], 'hour_rollover')
        self.assertTrue(all(row['hour'] == 100 for row in records))

    def test_near_hour_end_clips_timeout_and_refuses_unreachable_slot(self):
        code, output, calls, sleeps, _ = self.invoke([transport()] * 3, start=102 * 3600 - 100)
        self.assertEqual((code, len(calls), sleeps), (1, 1, []))
        self.assertEqual(calls[0][1], 99)
        self.assertEqual(json.loads(output.splitlines()[-1])['error'], 'deadline_exhausted')

    def test_recovered_transport_faults_are_silent_for_every_transient_class(self):
        success = subprocess.CompletedProcess([], 0, b'', b'')
        for category in ('timeout', 'refused', 'network_unreachable', 'connection_closed'):
            with self.subTest(category=category):
                code, output, calls, _, _ = self.invoke([transport(category), transport(category), success])
                self.assertEqual((code, output), (0, ''))
                self.assertEqual([c[0] for c in calls], [0, 300, 630])

    def test_permanent_ssh_security_failure_overrides_transient_words(self):
        for stderr, expected in ((b'Permission denied after connection timeout', 'auth_failed'),
                                 (b'Host key verification failed; connection closed', 'host_key')):
            self.assertEqual(r.classify_ssh_stderr(stderr), expected)

    def test_only_classified_transient_ssh_transport_failures_retry(self):
        failures = [transport('auth_failed'), transport('host_key'), transport('unknown'),
                    subprocess.CompletedProcess([], 1, b'DUMMY-SECRET', b''),
                    subprocess.CompletedProcess([], 0, b'', b'DUMMY-SECRET'),
                    r.WrapperError('output_limit'), OSError('DUMMY-SECRET'),
                    r.WrapperError('timeout'), subprocess.TimeoutExpired('DUMMY-SECRET', 1)]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__, classification=getattr(failure, 'ssh_error', None)):
                code, output, calls, sleeps, _ = self.invoke([failure] * 3)
                self.assertEqual(len(calls), 1)
                self.assertEqual(sleeps, [])
                self.assertEqual(code, 1)
                self.assertEqual(json.loads(output.splitlines()[-1])['error'], 'non_retryable_failure')
                self.assertNotIn('DUMMY-SECRET', output)

    def test_fast_transport_failures_are_dispersed_from_start_not_last_exit(self):
        code, output, calls, sleeps, clock = self.invoke([transport()] * 3, [15, 20, 15])
        self.assertEqual([c[0] for c in calls], [0, 300, 630])
        self.assertEqual(sleeps, [285, 310])
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(output.splitlines()[-1])['error'], 'retries_exhausted')
        self.assertNotIn('DUMMY-SECRET', output)
        self.assertTrue(all('--expected-hour 100' in c[2][-1] for c in calls))
