import json
import pathlib
import tempfile
import unittest
import os
import signal

from controller import Handoff, digest, require_approval
from supervisor import supervise
from production import configs


class Fake:
    def __init__(self):
        self.config = b'blue'
        self.route = 'blue'
        self.candidate_ready = True
        self.bad_syntax = False
        self.green_failure = False
        self.reloads = []

    def read(self): return self.config
    def install(self, value): self.config = value
    def validate(self):
        if self.bad_syntax and self.config == b'green': raise RuntimeError('syntax')
    def reload(self):
        self.route = self.config.decode()
        self.reloads.append(self.route)
    def ready(self, side): return side == 'blue' or self.candidate_ready
    def probe(self, side): return self.route == side and not (side == 'green' and self.green_failure)


class Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.time = 0
        self.adapter = Fake()
        self.handoff = Handoff(self.adapter, self.tmp.name, b'blue', b'green',
                               clock=lambda: self.time, sleep=self.advance,
                               observation_seconds=1, switch_seconds=1, rollback_seconds=1)
    def advance(self, seconds): self.time += seconds

    def test_success_keeps_old_instance(self):
        result = self.handoff.run()
        self.assertEqual(result['state'], 'active')
        self.assertFalse(result['old_instance_stopped'])

    def test_unready_candidate_never_changes_proxy(self):
        self.adapter.candidate_ready = False
        self.assertEqual(self.handoff.run()['state'], 'not_switched')
        self.assertEqual(self.adapter.reloads, [])

    def test_invalid_config_restores_old_route(self):
        self.adapter.bad_syntax = True
        self.assertEqual(self.handoff.run()['state'], 'rolled_back')
        self.assertEqual(self.adapter.route, 'blue')

    def test_functional_failure_rolls_back(self):
        self.adapter.green_failure = True
        self.assertEqual(self.handoff.run()['state'], 'rolled_back')
        self.assertEqual(self.adapter.route, 'blue')

    def test_coordinator_death_after_reload_is_recovered(self):
        self.handoff.record('switching')
        self.adapter.install(b'green'); self.adapter.reload()
        self.assertEqual(self.handoff.recover()['state'], 'rolled_back')
        self.assertEqual(self.handoff.recover()['state'], 'rolled_back')

    def test_foreign_change_is_never_overwritten(self):
        self.handoff.record('switching')
        self.adapter.install(b'another operation')
        with self.assertRaisesRegex(RuntimeError, 'another_operator'):
            self.handoff.recover()
        self.assertEqual(self.adapter.read(), b'another operation')

    def test_production_confirmation_is_explicit_bound_and_expiring(self):
        plan = {'production': True, 'old': 'old-id', 'new': 'new-id'}
        with self.assertRaises(PermissionError): require_approval(plan, None, now=100)
        approval = {'user_confirmed': True, 'user_message': '确认本次切流及自动回切',
                    'expires_unix': 200, 'actions': ['switch_traffic', 'automatic_route_rollback'],
                    'plan_sha256': digest(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode())}
        require_approval(plan, approval, now=100)
        for altered in ({**approval, 'user_confirmed': False}, {**approval, 'expires_unix': 99},
                        {**approval, 'actions': ['switch_traffic']}, {**approval, 'plan_sha256': 'wrong'}):
            with self.assertRaises(PermissionError): require_approval(plan, altered, now=100)

    def test_real_supervisor_survives_child_sigkill(self):
        class PersistentFake(Fake):
            def __init__(self, path):
                super().__init__()
                self.path = path
                self.path.write_bytes(b'blue')
            def read(self): return self.path.read_bytes()
            def install(self, value): self.path.write_bytes(value)
            def reload(self):
                if self.read() == b'green': os.kill(os.getpid(), signal.SIGKILL)
            def probe(self, side): return self.read() == side.encode()
        adapter = PersistentFake(pathlib.Path(self.tmp.name) / 'proxy.conf')
        handoff = Handoff(adapter, self.tmp.name, b'blue', b'green')
        self.assertEqual(supervise(handoff)['state'], 'rolled_back')
        self.assertEqual(adapter.read(), b'blue')

    def test_proposed_config_preserves_other_locations(self):
        original = b'location /audit-query/ { proxy_pass http://audit; }\n        proxy_pass http://host.docker.internal:18081;\nproxy_read_timeout 600s;\n'
        blue, green = configs(original, 'bg102b5cc0753e')
        self.assertEqual(blue, original)
        self.assertIn(b'location /audit-query/ { proxy_pass http://audit; }', green)
        self.assertIn(b'proxy_read_timeout 600s;', green)
        self.assertIn(b'18082;', green)
        with self.assertRaises(ValueError): configs(original + original, 'bg102b5cc0753e')


if __name__ == '__main__': unittest.main()
