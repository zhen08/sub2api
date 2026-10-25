"""Independent process supervising a bounded, journaled proxy coordinator."""
import json
import os
import signal
import time


def supervise(handoff, *, preparation_seconds=50):
    """Run inside a detached host service, not an SSH terminal process group.

    Only the child changes routing on the happy path. The parent survives a
    coordinator SIGKILL and recovers incomplete intent. No container is stopped.
    """
    child = os.fork()
    if child == 0:
        try:
            handoff.run()
        except Exception:
            os._exit(1)
        os._exit(0)
    started = time.monotonic()
    critical_deadline = None
    reaped = False
    try:
        while True:
            done, status = os.waitpid(child, os.WNOHANG)
            if done:
                reaped = True
                if os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0:
                    return json.loads(handoff.journal.read_bytes())
                return handoff.recover()
            if handoff.journal.exists():
                state = json.loads(handoff.journal.read_bytes())
                # The deadline remains in the last durable pre-mutation record.
                if state.get('supervisor_deadline') is not None:
                    critical_deadline = state['supervisor_deadline']
                deadline = critical_deadline or started + preparation_seconds
            else:
                deadline = started + preparation_seconds
            if time.monotonic() > deadline:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
                reaped = True
                return handoff.recover()
            time.sleep(.1)
    finally:
        if not reaped:
            try:
                os.kill(child, signal.SIGKILL)
                os.waitpid(child, 0)
            except ProcessLookupError:
                pass

