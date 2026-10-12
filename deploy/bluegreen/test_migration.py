import copy
import hashlib
import pathlib
import unittest

from migration import PENDING, render


class Tests(unittest.TestCase):
    def payload(self):
        root = pathlib.Path(__file__).resolve().parents[2] / 'backend/migrations'
        baseline = {str(i) + '.sql': '0' * 64 for i in range(290)}
        return dict(baseline=baseline, expected_post_ledger={**baseline, **PENDING},
                    pending={name: (root / name).read_text() for name in PENDING})

    def test_only_reviewed_delta_and_no_lock_wait(self):
        sql = render(self.payload())
        self.assertIn('ACCESS EXCLUSIVE MODE NOWAIT', sql)
        self.assertIn('pg_try_advisory_xact_lock', sql)
        self.assertIn('migration ledger drift', sql)
        self.assertEqual(sql.count('INSERT INTO schema_migrations'), 3)
        self.assertTrue(sql.endswith('COMMIT;'))

    def test_modified_migration_or_ledger_is_rejected(self):
        for mutation in ('sql', 'ledger'):
            payload = self.payload()
            if mutation == 'sql': payload['pending'][next(iter(PENDING))] += '\nDROP TABLE users;'
            else: payload['expected_post_ledger']['extra.sql'] = '0' * 64
            with self.assertRaises(ValueError): render(payload)


if __name__ == '__main__': unittest.main()
