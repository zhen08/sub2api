import runpy
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import run_remote


class CronEntrypointTests(unittest.TestCase):
    def test_cron_invokes_apply_and_preserves_wrapper_exit(self):
        entrypoint = Path(__file__).with_name('run_cron.py')
        self.assertTrue(entrypoint.is_file(), 'cron apply entrypoint is not implemented')
        original = sys.argv[:]
        observed = []
        def fake_main():
            observed.append(sys.argv[:])
            return 42
        try:
            with patch.object(run_remote, 'main', fake_main):
                with self.assertRaises(SystemExit) as result:
                    runpy.run_path(str(Path(__file__).with_name('run_cron.py')), run_name='__main__')
            self.assertEqual(result.exception.code, 42)
            self.assertEqual(observed, [[str(Path(__file__).with_name('run_remote.py')), '--apply']])
        finally:
            sys.argv[:] = original


if __name__ == '__main__':
    unittest.main()
