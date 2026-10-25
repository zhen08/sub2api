"""Online consistent backup and full isolated restore before approved activation."""
import argparse
import json
import os
import pathlib
import signal
import sys
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import atomic_json, require_approval


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=pathlib.Path, required=True)
    p.add_argument('--helpers', type=pathlib.Path, required=True)
    p.add_argument('--approved-plan', type=pathlib.Path, required=True)
    p.add_argument('--approval', type=pathlib.Path, required=True)
    a = p.parse_args()
    os.umask(0o077)
    plan = json.loads(a.approved_plan.read_bytes())
    require_approval(plan, json.loads(a.approval.read_bytes()))
    sys.path.insert(0, str(a.helpers))
    from v0215_live import Live
    from v0215_rollout import Session
    from v0215_cache_process import run

    a.root.mkdir(mode=0o700)
    number = 0
    def runner(argv, **kwargs):
        nonlocal number
        number += 1
        with (a.root / ('command-%05d.stderr' % number)).open('xb') as error:
            return run(argv, stderr_out=error, **kwargs)

    class ReadySession(Session):
        def exec(self, container, command, timeout=60, **kwargs):
            if command == ['pg_isready', '-U', 'postgres']:
                command = ['pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'postgres']
            return super().exec(container, command, timeout=timeout, **kwargs)

    load = lambda name: json.loads((a.helpers / name).read_bytes())
    live = Live(a.root, load('v0215-live-discovery.json'),
                load('v0215-migration-assessment.json'), load('v0215-live-pricing.json'),
                {'image_id': plan['candidate']['image']}, runner=runner)
    started = time.time()
    before = live.check_live()
    atomic_json(a.root / 'production-before.json', before)
    report = {'status': 'running', 'production_writes': False, 'writers_stopped': False}
    def deadline(*_): raise RuntimeError('backup_deadline')
    signal.signal(signal.SIGALRM, deadline)
    signal.signal(signal.SIGTERM, deadline)
    signal.alarm(1800)
    try:
        with ReadySession(a.root, runner) as session:
            print(json.dumps({'phase': 'online_consistent_backup'}), flush=True)
            snapshot = live.snapshot(session)
            print(json.dumps({'phase': 'isolated_full_restore', 'dump_sha256': snapshot['dump_sha256']}), flush=True)
            live.restore(session, snapshot)
        live.postcheck(before)
        report.update(status='passed', snapshot=snapshot,
                      restored_tables=len(live.tables), production_unchanged=True)
    except Exception as exc:
        report.update(status='failed', error=type(exc).__name__ + ':' + str(exc))
        raise
    finally:
        signal.alarm(0)
        report['elapsed_seconds'] = round(time.time() - started, 3)
        if (a.root / 'session.json').exists():
            report['cleanup'] = json.loads((a.root / 'session.json').read_bytes())
        atomic_json(a.root / 'online-backup-result.json', report)
        print(json.dumps(report), flush=True)


if __name__ == '__main__': main()
