import contextlib
import io
import json
import runpy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
import run_remote

class CronOutputTests(unittest.TestCase):
    def run_entry(self, text, code=0):
        output = io.StringIO()
        def fake_main():
            print(text, end='')
            return code
        with patch.object(sys, 'argv', sys.argv[:]), patch.object(run_remote, 'main', fake_main), contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as result:
                runpy.run_path(str(Path(__file__).with_name('run_cron.py')), run_name='__main__')
        self.assertEqual(result.exception.code, code)
        return output.getvalue()

    def test_only_requested_fields_for_each_transition(self):
        targets = [dict(email='a@example.com', **{'from':'original'}, to='terra', hourly_tokens=21666183, user_id=26), dict(email='b@example.com', **{'from':'luna'}, to='full', low_streak=[1,2])]
        actual = self.run_entry(json.dumps({'hour':497191, 'status':'transitions', 'targets':targets})+'\n')
        self.assertEqual([json.loads(line) for line in actual.splitlines()], [{k:t[k] for k in ('email','from','to')} for t in targets])

    def test_silent_stays_silent(self):
        self.assertEqual(self.run_entry(''), '')

    def test_errors_remain_visible(self):
        text='{"status":"error","error":"SSHFailure"}\n'
        self.assertEqual(self.run_entry(text, 1), text)

if __name__ == '__main__':
    unittest.main()
