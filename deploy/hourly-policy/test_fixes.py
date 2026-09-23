import contextlib
import http.server
import io
import json
import tempfile
import threading
import unittest
from unittest.mock import patch

import controller as c
import run_remote as r
from test_controller import FakeAPI


class FixTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(r.os.environ, {'HOURLY_POLICY_SSH_HOST': 'policy.example.test',
                                       'HOURLY_POLICY_SSH_PORT': '22'})
        env.start()
        self.addCleanup(env.stop)

    def test_remote_lock_and_entrypoint_deadline_retained(self):
        import runpy
        with tempfile.TemporaryDirectory() as directory:
            store = c.Store(directory)
            def collector(hour):
                with self.assertRaises(BlockingIOError):
                    with store.lock(): pass
                return {'hour':hour,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(c.main(['--apply','--state-dir',directory], api=FakeAPI(), collector=collector), 0)
            with store.lock(): pass
        with patch('signal.signal'), patch('signal.alarm') as alarm, patch('subprocess.run', side_effect=OSError('no docker invocation')), patch('sys.argv', ['controller.py']), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit) as exit:
                runpy.run_path('controller.py', run_name='__main__')
            self.assertEqual(exit.exception.code, 1)
            alarm.assert_called_once_with(240)

    def test_retry_rollover_during_delay_does_not_start_second_attempt(self):
        import subprocess
        clock = [101*3600+300]
        out = io.StringIO()
        def sleep(delay): clock[0] = 102*3600+1
        with patch.object(r, 'bounded_process', return_value=subprocess.CompletedProcess([],255,b'')) as attempt, patch.object(r.time, 'time', side_effect=lambda:clock[0]), patch.object(r.time, 'sleep', side_effect=sleep), patch('sys.argv',['run_remote.py']), contextlib.redirect_stdout(out):
            self.assertEqual(r.main(), 1)
        self.assertEqual(attempt.call_count, 1)
        self.assertEqual(json.loads(out.getvalue().splitlines()[-1])['error'], 'hour_rollover')

    def test_zero_exit_stderr_is_sanitized_failure(self):
        import sys
        out = io.StringIO()
        fake = [sys.executable, '-c', "import sys; sys.stdin.read(); sys.stderr.write('DUMMY-SECRET')"]
        with patch.object(r, 'command', return_value=fake), patch.object(r.time, 'time', return_value=101*3600+300), patch.object(r.time, 'sleep'), patch('sys.argv', ['run_remote.py']), contextlib.redirect_stdout(out):
            self.assertEqual(r.main(), 1)
        self.assertEqual(json.loads(out.getvalue().splitlines()[0])['error'], 'unexpected_stderr')
        self.assertNotIn('DUMMY-SECRET', out.getvalue())

    def test_bootstrap_and_eight_zero_windows_evidence(self):
        keys = [{'id':1,'user_id':9,'group_id':8}]
        bootstrap = c.plan({}, 100, {9:0}, keys, {9:'original'})
        self.assertEqual(bootstrap['transitions'][0]['reason'], 'bootstrap_group_8')
        recovered = bootstrap
        for hour in range(101, 108):
            recovered = c.plan(recovered['users'], hour, {9:0}, keys, {9:'terra'})
        evidence = recovered['transitions'][0]
        self.assertEqual(evidence['reason'], 'eight_consecutive_hours_eq_0')
        self.assertEqual([v['hour'] for v in evidence['low_streak']], list(range(100, 108)))
        self.assertEqual([v['hourly_tokens'] for v in evidence['low_streak']], [0]*8)
        self.assertTrue(all(v['hour_start'] and v['hour_end'] for v in evidence['low_streak']))
        # A gap/nonzero window must not retain misleading recovery evidence.
        gap = c.plan(bootstrap['users'], 102, {9:0}, keys, {9:'terra'})
        self.assertEqual([v['hour'] for v in gap['users']['9']['low_windows']], [102])
        reset = c.plan(bootstrap['users'], 101, {9:1}, keys, {9:'terra'})
        self.assertEqual(reset['users']['9']['low_windows'], [])
        legacy = c.plan({'9':{'level':'terra','last':100,'low':1}}, 101, {9:0}, keys, {9:'terra'})
        self.assertEqual(legacy['transitions'], [])
        self.assertEqual(legacy['users']['9']['low_windows'], [c.window_evidence(101, 0)])

    def test_transition_evidence_persisted_and_original_on_retry_stdout(self):
        def doc(tokens):
            return {'hour':100,'all_rows':1,'openai_rows':1,'unknown_groups':0,'invalid_tokens':0,
                    'users':[{'user_id':9,'rows':1,'keys':1,'input':tokens,'output':0,'cache_creation':0,'cache_read':0,'total':tokens}]}
        with tempfile.TemporaryDirectory() as directory:
            api = FakeAPI(); store = c.Store(directory)
            put = api.put
            def lost(path, payload):
                put(path, payload)
                raise OSError('lost response')
            api.put = lost
            with self.assertRaises(OSError):
                c.run(api, store, 100, lambda h: doc(51000001), apply=True)
            transition = store.load()['pending']['transitions'][0]
            self.assertEqual(transition.get('hourly_tokens'), 51000001, 'intent lacks original tokens')
            self.assertEqual(transition['hour_start'], '1970-01-05T04:00:00+00:00')
            self.assertEqual(transition['hour_end'], '1970-01-05T05:00:00+00:00')
            self.assertEqual(transition['reason'], 'tokens_gt_50000000')
            api.put = put; out = io.StringIO()
            with patch.object(c, 'processed_hour', return_value=100), contextlib.redirect_stdout(out):
                self.assertEqual(c.main(['--apply','--expected-hour','100','--state-dir',directory], api=api, collector=lambda h:doc(0)), 0)
            target = json.loads(out.getvalue())['targets'][0]
            for field in ('hourly_tokens','hour_start','hour_end','reason','low_streak'):
                self.assertEqual(target[field], transition[field])
            self.assertNotIn('pending', store.load())
            out = io.StringIO()
            with patch.object(c, 'processed_hour', return_value=100), contextlib.redirect_stdout(out):
                self.assertEqual(c.main(['--apply','--state-dir',directory], api=api, collector=lambda h:doc(0)), 0)
            self.assertEqual(out.getvalue(), '')

    def test_retry_aborts_before_sleep_when_hour_or_total_budget_expires(self):
        import subprocess
        for kind in ('hour', 'deadline', 'rollover'):
            out = io.StringIO(); sleeps = []; calls = []
            clock = {'wall':101*3600+300, 'mono':0}
            def attempt(*args):
                calls.append(args)
                if kind == 'hour': clock['wall'] = 102*3600-2
                elif kind == 'rollover': clock['wall'] = 102*3600+1
                else: clock['mono'] = 899
                return subprocess.CompletedProcess([], 255, b'')
            def sleep(delay):
                sleeps.append(delay)
                clock['mono'] += delay; clock['wall'] += delay
            with patch.object(r, 'bounded_process', side_effect=attempt), patch.object(r.time, 'time', side_effect=lambda:clock['wall']), patch.object(r.time, 'monotonic', side_effect=lambda:clock['mono']), patch.object(r.time, 'sleep', side_effect=sleep), patch('sys.argv', ['run_remote.py']), contextlib.redirect_stdout(out):
                self.assertEqual(r.main(), 1)
            self.assertEqual(len(calls), 1)
            self.assertEqual(sleeps, [], 'cannot sleep/retry past pinned hour or total deadline')
            self.assertIn(json.loads(out.getvalue().splitlines()[-1])['error'], ('hour_rollover', 'deadline_exhausted'))

    def test_source_read_errors_are_sanitized_stdout(self):
        for error in (FileNotFoundError('DUMMY-SECRET'), PermissionError('DUMMY-SECRET')):
            out = io.StringIO()
            with patch.object(r.Path, 'read_bytes', side_effect=error), patch('sys.argv', ['run_remote.py']), contextlib.redirect_stdout(out):
                self.assertEqual(r.main(), 1)
            self.assertEqual(json.loads(out.getvalue())['error'], 'source_read_failed')
            self.assertNotIn('DUMMY-SECRET', out.getvalue())

    def test_wrapper_real_ssh_sudo_failure_and_exhaustion_sanitized(self):
        import sys
        for code in (255, 1):
            delays = []; out = io.StringIO(); err = io.StringIO()
            fake = [sys.executable, '-c', f"import sys; sys.stderr.write('sudo/ssh DUMMY-SECRET'); print('DUMMY-SECRET'); sys.exit({code})"]
            with patch.object(r, 'command', return_value=fake) as command, patch.object(r.time, 'time', return_value=101*3600+300), patch.object(r.time, 'sleep', side_effect=delays.append), patch('sys.argv', ['run_remote.py']), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertEqual(r.main(), 1)
            records = [json.loads(line) for line in out.getvalue().splitlines()]
            self.assertEqual([v['exit_code'] for v in records[:-1]], [code]*3)
            self.assertEqual(records[-1]['error'], 'retries_exhausted')
            self.assertEqual(command.call_count, 3)
            self.assertEqual(delays, [5, 15])
            self.assertNotIn('DUMMY-SECRET', out.getvalue() + err.getvalue())

    def test_wrapper_timeout_and_oserror_are_sanitized(self):
        import subprocess
        for error in (r.WrapperError('timeout'), OSError('DUMMY-SECRET'), subprocess.TimeoutExpired('DUMMY-SECRET', 1, stderr=b'DUMMY-SECRET')):
            out = io.StringIO()
            with patch.object(r, 'bounded_process', side_effect=error), patch.object(r.time, 'time', return_value=101*3600+300), patch.object(r.time, 'sleep'), patch('sys.argv', ['run_remote.py']), contextlib.redirect_stdout(out):
                self.assertEqual(r.main(), 1)
            self.assertEqual(len(out.getvalue().splitlines()), 4)
            self.assertNotIn('DUMMY-SECRET', out.getvalue())

    def test_wrapper_success_nochange_silence_and_readonly_summary(self):
        import sys
        for mode, output in (('--apply', ''), ('--dry-run', '{"status":"ok"}'), ('--collect-only', '{"status":"collected_only_backend_not_checked"}')):
            out = io.StringIO()
            fake = [sys.executable, '-c', f'import sys; sys.stdin.read(); sys.stdout.write({output!r})']
            with patch.object(r, 'command', return_value=fake), patch('sys.argv', ['run_remote.py', mode]), contextlib.redirect_stdout(out):
                self.assertEqual(r.main(), 0)
            self.assertEqual(out.getvalue(), output)

    def test_wrapper_same_hour_retry_recovers_partial_intent(self):
        import subprocess
        calls, delays = [], []
        doc = {'hour':100,'all_rows':0,'openai_rows':0,'unknown_groups':0,'invalid_tokens':0,'users':[]}
        with tempfile.TemporaryDirectory() as directory:
            api = FakeAPI(); store = c.Store(directory)
            original = api.put
            def lost_response(path, payload):
                original(path, payload)
                raise OSError('DUMMY-SECRET response lost')
            api.put = lost_response
            def attempt(cmd, source, timeout, output_limit):
                calls.append(cmd)
                self.assertIn('--expected-hour 100', cmd[-1])
                self.assertLessEqual(timeout, 270)
                self.assertLessEqual(output_limit, 1048576)
                try:
                    summary = c.run(api, store, 100, lambda h: doc, apply=True)
                    return subprocess.CompletedProcess(cmd, 0, json.dumps(summary).encode())
                except OSError:
                    self.assertIn('pending', store.load())
                    api.put = original
                    return subprocess.CompletedProcess(cmd, 255, b'DUMMY-SECRET')
            out = io.StringIO()
            with patch.object(r.subprocess, 'run', side_effect=AssertionError('unbounded single attempt path')), patch.object(r, 'bounded_process', side_effect=attempt), patch.object(r.time, 'time', return_value=101*3600+300), patch.object(r.time, 'sleep', side_effect=delays.append), patch('sys.argv', ['run_remote.py', '--apply']), contextlib.redirect_stdout(out):
                self.assertEqual(r.main(), 0)
            self.assertEqual(len(calls), 2)
            self.assertEqual(delays, [5])
            self.assertNotIn('pending', store.load())
            self.assertEqual(len(api.writes), 1)
            self.assertNotIn('DUMMY-SECRET', out.getvalue())

    def test_expected_hour_rejects_before_inventory_and_rollover_before_write(self):
        self.assertTrue(hasattr(c, 'processed_hour'), 'remote hour guard missing')
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as directory, patch.object(c, 'processed_hour', return_value=101):
            api = FakeAPI()
            with contextlib.redirect_stdout(out):
                rc = c.main(['--apply', '--expected-hour', '100', '--state-dir', directory], api=api)
            self.assertEqual(rc, 1)
            self.assertIn('hour', json.loads(out.getvalue())['error'])
            self.assertEqual(api.writes, [])
            store = c.Store(directory)
            planned = c.plan({}, 100, {9:21000000}, [], {9:'original'})
            clock = [100]
            def guard():
                if clock[0] != 100: raise c.PolicyError('hour rollover')
            reads = []
            def read(uid):
                reads.append(uid)
                if len(reads) == 2: clock[0] = 101
                return 'original'
            with self.assertRaises(c.PolicyError):
                c.commit_plan(store, planned, read, lambda *a: self.fail('stale write'), guard=guard)
            self.assertIn('pending', store.load())

    def test_bounded_process_concurrent_io_deadline_and_output(self):
        import sys, time
        self.assertTrue(hasattr(r, 'bounded_process'), 'bounded subprocess missing')
        payload = b'x' * 200000
        start = time.monotonic()
        result = r.bounded_process([sys.executable, '-c',
            "import sys; sys.stdout.write('y'*100000); sys.stdout.flush(); "
            "data=sys.stdin.buffer.read(); print(len(data))"], payload, 2, 300000)
        self.assertEqual(result.returncode, 0)
        self.assertTrue(result.stdout.endswith(b'200000\n'))
        for script, reason in [
            ("import time; time.sleep(10)", 'timeout'),
            ("import os,time; os.close(1); os.close(2); time.sleep(10)", 'timeout'),
            ("import sys; sys.stdout.write('x'*200000)", 'output_limit'),
            ("import sys; sys.stderr.write('x'*200000)", 'output_limit'),
            ("import signal,time; signal.signal(signal.SIGTERM,signal.SIG_IGN); time.sleep(10)", 'timeout')]:
            with self.subTest(reason=reason, script=script):
                with self.assertRaises(r.WrapperError) as error:
                    r.bounded_process([sys.executable, '-c', script], payload, .15, 10000)
                self.assertEqual(str(error.exception), reason)
        self.assertLess(time.monotonic() - start, 4)

    def test_reject_all_redirects_without_forwarding_dummy_credential(self):
        received = []
        class Destination(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                received.append(self.headers.get('x-api-key'))
                self.send_response(200); self.end_headers()
                self.wfile.write(b'{"data":{}}')
            do_PUT = do_GET
            def log_message(self, *args): pass
        with contextlib.ExitStack() as stack:
            destination = http.server.HTTPServer(('127.0.0.1', 0), Destination)
            class Redirect(Destination):
                code = 302
                same_origin = False
                def do_GET(self):
                    if self.path == '/destination':
                        return super().do_GET()
                    self.send_response(self.code)
                    self.send_header('Location', '/destination' if self.same_origin else
                                     f'http://127.0.0.1:{destination.server_port}/destination')
                    self.end_headers()
                do_PUT = do_GET
            origin = http.server.HTTPServer(('127.0.0.1', 0), Redirect)
            for server in (destination, origin):
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                stack.callback(thread.join)
                stack.callback(server.server_close)
                stack.callback(server.shutdown)
            api = c.API(f'http://127.0.0.1:{origin.server_port}', 'DUMMY-SECRET')
            for same in (False, True):
                Redirect.same_origin = same
                for code in range(300, 309):
                    Redirect.code = code
                    for method in ('GET', 'PUT'):
                        with self.subTest(same=same, code=code, method=method):
                            with self.assertRaises(c.PolicyError):
                                api.request(method, '/x')
            self.assertEqual(received, [])
