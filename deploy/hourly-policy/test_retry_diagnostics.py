"""Retry notification contract; all subprocesses are local synthetic fixtures."""
import contextlib
import io
import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import run_remote as r


class RetryDiagnosticsTests(unittest.TestCase):
    def test_bounded_stderr_classification_never_returns_raw_bytes(self):
        cases = [
            (b'ssh: connect to host fixture port 22: Connection timed out', 'timeout'),
            (b'Connection refused', 'refused'),
            (b'Network is unreachable', 'network_unreachable'),
            (b'No route to host', 'network_unreachable'),
            (b'Permission denied (publickey).', 'auth_failed'),
            (b'Host key verification failed.', 'host_key'),
            (b'WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!', 'host_key'),
            (b'Connection closed by remote host', 'connection_closed'),
            (b'Connection reset by peer', 'connection_closed'),
            (b'client_loop: send disconnect: Broken pipe', 'connection_closed'),
            (b'\xff\x00DUMMY-SECRET\n{"ssh_error":"injected"}', 'unknown'),
            (b'x' * 4096 + b'Connection refused', 'unknown'),
        ]
        for message, expected in cases:
            with self.subTest(expected=expected, message=message[:60]):
                script = ('import os,sys,time; sys.stdin.read(); '
                          f'data={message + b" DUMMY-SECRET"!r}; '
                          'os.write(2,data[:8]); time.sleep(.01); os.write(2,data[8:]); sys.exit(255)')
                result = r.bounded_process([sys.executable, '-c', script], b'', 2, 20000)
                self.assertEqual(getattr(result, 'ssh_error', None), expected)
                self.assertEqual(result.stderr, b'present')
                self.assertNotIn('DUMMY-SECRET', repr((result.stdout, result.stderr, result.ssh_error)))
                code, output, calls = self.invoke([result] * 3)
                self.assertEqual((code, calls), (1, 3))
                records = [json.loads(line) for line in output.splitlines()]
                self.assertEqual([row.get('ssh_error') for row in records[:-1]], [expected] * 3)
                self.assertNotIn('DUMMY-SECRET', output)

    def test_local_record_is_private_bounded_and_retained_across_clean_success(self):
        failure = subprocess.CompletedProcess([], 255, b'DUMMY-SECRET', b'DUMMY-SECRET')
        success = subprocess.CompletedProcess([], 0, b'', b'')
        with tempfile.TemporaryDirectory() as directory:
            record = Path(directory) / 'last-retry-failure.json'
            env = {'HOURLY_POLICY_LOCAL_STATE_DIR': directory}
            self.assertEqual(self.invoke([failure, failure, success], env=env)[:2], (0, ''))
            self.assertTrue(record.exists(), 'recovered retry evidence was discarded')
            saved = record.read_bytes()
            data = json.loads(saved)
            self.assertEqual(data['outcome'], 'recovered')
            self.assertEqual(data['hour'], 100)
            self.assertEqual([row['attempt'] for row in data['failures']], [1, 2])
            self.assertNotIn(b'DUMMY-SECRET', saved)
            self.assertLessEqual(len(saved), 4096)
            self.assertEqual(record.stat().st_mode & 0o777, 0o600)
            self.assertEqual(self.invoke([success], env=env)[:2], (0, ''))
            self.assertEqual(record.read_bytes(), saved)
            for _ in range(5):
                code, output, calls = self.invoke([failure] * 3, env=env)
                self.assertEqual((code, calls), (1, 3))
                data = json.loads(record.read_bytes())
                self.assertEqual(data['outcome'], 'retries_exhausted')
                self.assertEqual(len(data['failures']), 3)
                self.assertLessEqual(record.stat().st_size, 4096)
            code, output, calls = self.invoke([failure], env=env,
                sleep=lambda clock: clock.__setitem__(0, 102 * 3600 + 1))
            self.assertEqual((code, calls), (1, 1))
            data = json.loads(record.read_bytes())
            self.assertEqual(data['outcome'], 'hour_rollover')
            self.assertEqual(len(data['failures']), 1)
            self.assertEqual(data['failures'][0], json.loads(output.splitlines()[0]))
            self.assertEqual([item.name for item in Path(directory).iterdir()], [record.name])

    def test_wrapper_exception_codes_are_static_and_silent_on_recovery(self):
        success = subprocess.CompletedProcess([], 0, b'', b'')
        for error, expected in ((r.WrapperError('timeout'), 'timeout'),
                (subprocess.TimeoutExpired('DUMMY-SECRET', 1, stderr=b'DUMMY-SECRET'), 'timeout'),
                (OSError('DUMMY-SECRET'), 'unknown'),
                (r.WrapperError('output_limit'), 'unknown')):
            with self.subTest(expected=expected):
                code, output, calls = self.invoke([error] * 3)
                self.assertEqual((code, calls), (1, 3))
                self.assertEqual([json.loads(line).get('ssh_error') for line in output.splitlines()[:-1]], [expected] * 3)
                self.assertNotIn('DUMMY-SECRET', output)
                self.assertEqual(self.invoke([error, success])[:2], (0, ''))

    def test_local_record_failures_never_change_notification_or_follow_target_symlink(self):
        failure = subprocess.CompletedProcess([], 255, b'DUMMY-SECRET', b'DUMMY-SECRET')
        success = subprocess.CompletedProcess([], 0, b'', b'')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = root / 'state'
            state.mkdir(mode=0o700)
            target = root / 'must-not-change'
            target.write_text('original')
            record = state / 'last-retry-failure.json'
            record.symlink_to(target)
            env = {'HOURLY_POLICY_LOCAL_STATE_DIR': str(state)}
            self.assertEqual(self.invoke([failure, success], env=env)[:2], (0, ''))
            self.assertEqual(target.read_text(), 'original')
            self.assertFalse(record.is_symlink())
            saved = record.read_bytes()
            with patch.object(r.os, 'replace', side_effect=OSError('DUMMY-SECRET')):
                self.assertEqual(self.invoke([failure, success], env=env)[:2], (0, ''))
            self.assertEqual(record.read_bytes(), saved)
            self.assertEqual([p.name for p in state.iterdir()], [record.name])
            for invalid in (str(root / 'absent'), 'relative/state', str(Path.home()), '/'):
                with self.subTest(invalid=invalid):
                    self.assertEqual(self.invoke([failure, success], env={'HOURLY_POLICY_LOCAL_STATE_DIR': invalid})[:2], (0, ''))
            self.assertFalse((root / 'absent').exists())
            state.chmod(0o755)
            self.assertEqual(self.invoke([failure] * 3, env=env)[0], 1)
            self.assertEqual(record.read_bytes(), saved)

    def test_local_record_stale_temp_is_not_overwritten_or_accumulated(self):
        failure = subprocess.CompletedProcess([], 255, b'', b'')
        success = subprocess.CompletedProcess([], 0, b'', b'')
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory) / '.last-retry-failure.tmp'
            temporary.write_text('previous interrupted or concurrent writer')
            for _ in range(3):
                self.assertEqual(self.invoke([failure, success], env={
                    'HOURLY_POLICY_LOCAL_STATE_DIR': directory})[:2], (0, ''))
            self.assertEqual(list(Path(directory).iterdir()), [temporary])
            self.assertEqual(temporary.read_text(), 'previous interrupted or concurrent writer')

    def test_local_record_cleanup_never_unlinks_next_writers_slot(self):
        failure = subprocess.CompletedProcess([], 255, b'', b'')
        success = subprocess.CompletedProcess([], 0, b'', b'')
        replace = os.replace
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory) / '.last-retry-failure.tmp'
            def next_writer(*args, **kwargs):
                replace(*args, **kwargs)
                temporary.write_text('next writer')
            with patch.object(r.os, 'replace', side_effect=next_writer):
                self.assertEqual(self.invoke([failure, success], env={
                    'HOURLY_POLICY_LOCAL_STATE_DIR': directory})[:2], (0, ''))
            self.assertTrue(temporary.exists(), 'cleanup removed a concurrent writer slot')
            self.assertEqual(temporary.read_text(), 'next writer')

    def invoke(self, results, cron=False, sleep=None, env=None):
        out, err = io.StringIO(), io.StringIO()
        clock = [101 * 3600 + 300]
        with patch.dict(os.environ, {'HOURLY_POLICY_SSH_HOST': 'policy.example.test', **(env or {})}, clear=True), \
                patch.object(r, 'bounded_process', side_effect=results) as process, \
                patch.object(r.time, 'time', side_effect=lambda: clock[0]), \
                patch.object(r.time, 'sleep', side_effect=lambda delay: sleep(clock) if sleep else None), \
                patch.object(sys, 'argv', ['run_remote.py', '--apply']), \
                contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            if cron:
                with self.assertRaises(SystemExit) as exit:
                    runpy.run_path(str(Path(__file__).with_name('run_cron.py')), run_name='__main__')
                code = exit.exception.code
            else:
                code = r.main()
        self.assertEqual(err.getvalue(), '')
        return code, out.getvalue(), process.call_count

    def test_two_ssh255_then_success_only_emits_successful_stdout(self):
        transition = json.dumps({'status': 'transitions', 'hour': 100, 'targets': [
            {'email': 'fixture@example.test', 'from': 'original', 'to': 'terra',
             'hourly_tokens': 21000001, 'user_id': 9}]}) + '\n'
        for cron in (False, True):
            for final in ('', transition):
                with self.subTest(cron=cron, final=final):
                    failures = [subprocess.CompletedProcess([], 255, b'DUMMY-SECRET', b'DUMMY-SECRET')] * 2
                    code, output, calls = self.invoke(failures + [subprocess.CompletedProcess([], 0, final.encode(), b'')], cron)
                    expected = (json.dumps({'email': 'fixture@example.test', 'from': 'original', 'to': 'terra'}) + '\n'
                                if cron and final else final)
                    self.assertEqual((code, calls), (0, 3))
                    self.assertEqual(output, expected)

    def test_exhaustion_and_rollover_retain_buffered_attempt_diagnostics(self):
        failure = subprocess.CompletedProcess([], 255, b'DUMMY-SECRET', b'DUMMY-SECRET')
        for sleep, expected, count in ((None, 'retries_exhausted', 3),
                (lambda clock: clock.__setitem__(0, 102 * 3600 + 1), 'hour_rollover', 1)):
            with self.subTest(expected=expected):
                code, output, calls = self.invoke([failure] * 3, cron=True, sleep=sleep)
                records = [json.loads(line) for line in output.splitlines()]
                self.assertEqual((code, calls), (1, count))
                self.assertEqual([row['attempt'] for row in records[:-1]], list(range(1, count + 1)))
                self.assertTrue(all(row['hour'] == 100 for row in records))
                self.assertEqual(records[-1]['error'], expected)
                self.assertNotIn('DUMMY-SECRET', output)
