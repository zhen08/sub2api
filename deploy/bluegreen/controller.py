"""Bounded proxy handoff. No container stop, database restore, or nft operations.

Adapters own atomic config installation, syntax checking, reload and functional
probes. An independent supervisor must call recover() after coordinator death.
Production adapters require an explicit, plan-bound operator approval.
"""
import hashlib
import json
import os
import pathlib
import tempfile
import time


def digest(data):
    return hashlib.sha256(data).hexdigest()


def atomic_json(path, value):
    path = pathlib.Path(path)
    fd, name = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'w') as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def require_approval(plan, approval, now=None):
    """Validate an operator receipt; callers must never synthesize user consent."""
    now = time.time() if now is None else now
    expected = digest(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode())
    if not approval or approval.get('plan_sha256') != expected:
        raise PermissionError('explicit_plan_bound_user_confirmation_required')
    if approval.get('user_confirmed') is not True or not approval.get('user_message', '').strip():
        raise PermissionError('explicit_user_message_required')
    if not now < approval.get('expires_unix', 0) <= now + 3600:
        raise PermissionError('confirmation_expired_or_too_long')
    required = {'switch_traffic', 'automatic_route_rollback'}
    if not required <= set(approval.get('actions', [])):
        raise PermissionError('confirmation_action_scope')


class Handoff:
    def __init__(self, adapter, directory, blue, green, *, clock=time.monotonic,
                 sleep=time.sleep, observation_seconds=30, switch_seconds=15,
                 rollback_seconds=30):
        self.adapter = adapter
        self.directory = pathlib.Path(directory)
        self.blue = blue
        self.green = green
        self.clock = clock
        self.sleep = sleep
        self.observation_seconds = observation_seconds
        self.switch_seconds = switch_seconds
        self.rollback_seconds = rollback_seconds
        if not 0 < observation_seconds <= 60 or not 0 < switch_seconds <= 20 or not 0 < rollback_seconds <= 30:
            raise ValueError('handoff_budget_exceeds_two_minutes')
        self.journal = self.directory / 'handoff.json'

    def record(self, state, **values):
        result = dict(state=state, blue_sha256=digest(self.blue), green_sha256=digest(self.green),
                      updated_monotonic=self.clock(), old_instance_stopped=False, **values)
        atomic_json(self.journal, result)
        return result

    def verify_owned_config(self):
        current = self.adapter.read()
        if current not in (self.blue, self.green):
            raise RuntimeError('proxy_configuration_changed_by_another_operator')
        return current

    def rollback(self, reason):
        start = self.clock()
        self.verify_owned_config()
        self.record('rolling_back', reason=reason)
        self.adapter.install(self.blue)
        self.adapter.validate()
        self.adapter.reload()
        deadline = start + self.rollback_seconds
        while self.clock() < deadline:
            if self.adapter.probe('blue'):
                return self.record('rolled_back', reason=reason,
                                   rollback_seconds=round(self.clock() - start, 3))
            self.sleep(.25)
        self.record('rollback_failed', reason=reason)
        raise RuntimeError('old_route_not_recovered_within_budget')

    def run(self):
        if self.journal.exists():
            raise RuntimeError('existing_handoff_requires_recovery')
        if self.adapter.read() != self.blue:
            raise RuntimeError('original_proxy_configuration_changed')
        # Direct readiness uses a real authenticated request as well as health.
        if not self.adapter.ready('blue') or not self.adapter.ready('green'):
            return self.record('not_switched', reason='backend_not_ready')
        start = self.clock()
        self.record('switching', started_monotonic=start,
                    supervisor_deadline=start + self.switch_seconds + self.observation_seconds + 10)
        try:
            self.adapter.install(self.green)
            self.adapter.validate()
            self.adapter.reload()
            deadline = start + self.switch_seconds
            while not self.adapter.probe('green'):
                if self.clock() >= deadline:
                    raise RuntimeError('new_route_not_ready')
                self.sleep(.25)
            switched = self.clock()
            self.record('observing', started_monotonic=start,
                        supervisor_deadline=start + self.switch_seconds + self.observation_seconds + 10)
            failures = 0
            while self.clock() - switched < self.observation_seconds:
                failures = 0 if self.adapter.probe('green') else failures + 1
                if failures >= 2:
                    raise RuntimeError('candidate_probe_failed')
                self.sleep(.5)
            return self.record('active', switch_seconds=round(switched - start, 3),
                               observation_seconds=round(self.clock() - switched, 3),
                               drain_policy='retain_blue_until_streams_finish_and_operator_confirms_retirement')
        except Exception as exc:
            return self.rollback(type(exc).__name__ + ':' + str(exc))

    def recover(self):
        """Called by a separate supervisor, never by the killed coordinator."""
        if not self.journal.exists():
            return {'state': 'not_started'}
        state = json.loads(self.journal.read_bytes())
        if state['blue_sha256'] != digest(self.blue) or state['green_sha256'] != digest(self.green):
            raise RuntimeError('recovery_plan_mismatch')
        if state['state'] in ('active', 'rolled_back', 'not_switched'):
            return state
        return self.rollback('coordinator_died_or_deadline_expired')
