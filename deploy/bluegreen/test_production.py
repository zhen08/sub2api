import json
import pathlib
import tempfile
import time
import unittest
from unittest.mock import patch

from controller import digest
from production import Adapter, preflight


class Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)
        self.path = self.root / 'site.conf'
        self.path.write_bytes(b'original')
        self.adapter = Adapter({'nginx_config': str(self.path), 'nginx_id': 'test'},
                               b'original', b'candidate', self.root)

    def test_foreign_config_is_preserved(self):
        self.path.write_bytes(b'another operator changed this')
        with self.assertRaisesRegex(RuntimeError, 'foreign_config'):
            self.adapter.install(b'candidate')
        self.assertEqual(self.path.read_bytes(), b'another operator changed this')

    def test_candidate_payload_outside_plan_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, 'unapproved_payload'):
            self.adapter.install(b'unreviewed config')
        self.assertEqual(self.path.read_bytes(), b'original')

    def test_stale_or_incomplete_proof_cannot_reach_docker(self):
        proof = self.root / 'proof.json'
        for payload in ({'verified_unix': time.time(), 'checks': {}, 'green_id': 'green'},
                        {'verified_unix': 0, 'checks': {}, 'green_id': 'green'}):
            proof.write_text(json.dumps(payload))
            plan = {'readiness_receipt_file': str(proof),
                    'readiness_receipt_sha256': digest(proof.read_bytes()), 'green_id': 'green'}
            with patch('production.command') as command:
                with self.assertRaisesRegex(RuntimeError, 'fresh_complete'):
                    preflight(plan)
                command.assert_not_called()

    def test_symlink_config_cannot_replace_another_file(self):
        target = self.root / 'target'
        target.write_bytes(b'original')
        self.path.unlink(); self.path.symlink_to(target)
        with self.assertRaisesRegex(RuntimeError, 'symlink'):
            self.adapter.install(b'candidate')
        self.assertEqual(target.read_bytes(), b'original')


if __name__ == '__main__': unittest.main()
