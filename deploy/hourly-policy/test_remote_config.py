import contextlib
import io
import json
import os
import unittest
from unittest.mock import patch

import run_remote as r


class RemoteConfigTests(unittest.TestCase):
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
