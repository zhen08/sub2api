import contextlib
import io
import json
import os
import unittest
from unittest.mock import patch

import run_remote as r


class RemoteConfigTests(unittest.TestCase):
    def run_failure(self, stdout, stderr=b'', code=1):
        out = io.StringIO()
        result = r.subprocess.CompletedProcess([], code, stdout, stderr)
        with patch.dict(os.environ, {'HOURLY_POLICY_SSH_HOST': 'policy.example.test'}, clear=True), \
                patch.object(r, 'bounded_process', return_value=result) as process, \
                patch.object(r.time, 'time', return_value=360100), \
                patch.object(r.time, 'sleep'), patch('sys.argv', ['run_remote.py']), \
                contextlib.redirect_stdout(out):
            self.assertEqual(r.main(), 1)
        self.assertEqual(process.call_count, 3)
        return [json.loads(line) for line in out.getvalue().splitlines()]

    def test_allowlisted_channel_drift_visible_on_remote_exit_one(self):
        records = self.run_failure(json.dumps({
            'status': 'error', 'hour': 99,
            'error': 'source channel configuration drift'}).encode(), b'DUMMY-SECRET')
        for record in records[:-1]:
            self.assertEqual(record['error'], 'ssh_nonzero')
            self.assertEqual(record.get('remote_error'), 'source channel configuration drift')
            self.assertEqual(record['exit_code'], 1)
        self.assertEqual(records[-1]['error'], 'retries_exhausted')
        self.assertNotIn('DUMMY-SECRET', json.dumps(records))

    def test_remote_failure_diagnostics_never_echo_untrusted_data(self):
        safe = 'source channel configuration drift'
        secret = 'DUMMY-SECRET'
        rejected = [b'', b'not-json DUMMY-SECRET', b'\xff', b'[]', b'null',
                    b'"DUMMY-SECRET"', b'[' * 2000,
                    json.dumps({'status': 'error', 'error': secret}).encode(),
                    json.dumps({'status': 'error', 'error': safe + secret}).encode(),
                    json.dumps({'status': 'ok', 'error': safe}).encode(),
                    json.dumps({'error': safe}).encode(),
                    json.dumps({'status': 'error', 'error': [safe]}).encode(),
                    json.dumps({'status': 'error', 'error': safe}).encode() + b'\nDUMMY-SECRET']
        for stdout in rejected:
            with self.subTest(stdout=stdout[:80]):
                records = self.run_failure(stdout, secret.encode())
                self.assertNotIn(secret, json.dumps(records))
                self.assertTrue(all('remote_error' not in r for r in records))
        payload = json.dumps({'status': 'error', 'error': safe,
                              'hour': secret, 'details': secret}).encode()
        records = self.run_failure(payload, secret.encode())
        self.assertNotIn(secret, json.dumps(records))
        self.assertEqual(records[0]['remote_error'], safe)
        for code in (0, 2, 255, -15):
            with self.subTest(code=code):
                records = self.run_failure(payload, secret.encode(), code)
                self.assertNotIn(secret, json.dumps(records))
                self.assertTrue(all('remote_error' not in r for r in records))

    def test_missing_host_fails_closed_without_process_or_retry(self):
        out = io.StringIO()
        with patch.dict(os.environ, {}, clear=True), patch.object(r, 'bounded_process', side_effect=AssertionError('must not start a process')) as process, \
                patch.object(r.time, 'sleep') as sleep, patch('sys.argv', ['run_remote.py', '--apply']), \
                contextlib.redirect_stdout(out):
            self.assertEqual(r.main(), 1)
        process.assert_not_called()
        sleep.assert_not_called()
        self.assertEqual(json.loads(out.getvalue())['error'], 'invalid_ssh_configuration')

    def test_configured_destination_and_default_port(self):
        with patch.dict(os.environ, {'HOURLY_POLICY_SSH_HOST': 'operator@policy.example.test'}, clear=True):
            cmd = r.command('--dry-run', 100)
            self.assertEqual(cmd[-2], 'operator@policy.example.test')
            self.assertEqual(cmd[cmd.index('-p') + 1], '22')
            self.assertEqual(cmd[-1], 'sudo -n python3 - --dry-run --expected-hour 100')
            os.environ['HOURLY_POLICY_SSH_PORT'] = '2200'
            self.assertEqual(r.command('--apply')[-3], '2200')

    def test_invalid_config_rejected_without_echoing_values(self):
        for host, port in (('', '22'), ('-oProxyCommand=bad', '22'),
                           ('host\nsecret', '22'), ('host name', '22'),
                           ('policy.example.test', '0'), ('policy.example.test', '65536'),
                           ('policy.example.test', 'secret')):
            with self.subTest(host=host, port=port), patch.dict(os.environ,
                    {'HOURLY_POLICY_SSH_HOST': host, 'HOURLY_POLICY_SSH_PORT': port}, clear=True):
                with self.assertRaisesRegex(ValueError, '^invalid SSH configuration$'):
                    r.command('--apply')
