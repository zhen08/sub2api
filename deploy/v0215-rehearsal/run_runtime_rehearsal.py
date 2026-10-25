"""Run real old/new/old images against a retained dump in a network-none stack.

No production discovery, snapshot acquisition, deployment, nft, or cron entrypoint.
The runtime bundle supplies the existing data, price, and cache-effect guards.
"""
import argparse
import hashlib
import json
import os
import pathlib
import re
import signal
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bundle', type=pathlib.Path, required=True)
    parser.add_argument('--incident', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
    from v0215_restore_catalog import verify_restore_catalog
    from v0215_frontend_contract import frontend_digest, verify_frontend
    sys.path.insert(0, str(args.bundle.resolve()))
    from v0215_live import Live
    from v0215_rollout import Session, OLD, BIN, REV, sha
    from v0215_cache_rehearsal import backend_class
    from v0215_cache_process import run
    import v0215_spill_probe as probe
    from decimal import Decimal

    old = OLD
    candidate = 'sha256:22662cfe27b7413899cd1e764e1605979ccc5e8b1ad1496775888d99cb190d06'
    pg = 'sha256:d3e1620b530c944afa6e887d22eb899824da68e19c52024bf98f5220c88a65b2'
    redis = 'sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99'
    images = {old, candidate, pg, redis}
    memory = dict(re.findall(r'^(\w+):\s+(\d+)', pathlib.Path('/proc/meminfo').read_text(), re.M))
    if int(memory['MemAvailable']) < 11 * 1024**2:
        raise RuntimeError('insufficient_isolated_rehearsal_headroom')
    args.output.mkdir(mode=0o700)
    owned, names, pids = set(), set(), set()
    counter = 0

    def isolated_run(argv, **kwargs):
        nonlocal counter
        if argv[0] == 'nsenter':
            assert argv[1] == '-t' and argv[2] in pids and argv[3:6] == ['-n', 'python3', '-c']
        elif argv[0] == 'docker':
            action = argv[1]
            if action == 'create':
                name = argv[argv.index('--name') + 1]
                assert re.fullmatch('v0215-(database|redis|app)-[a-f0-9]{32}', name)
                assert len(images.intersection(argv)) == 1
                network = [x for x in argv if x.startswith('--network=')]
                assert len(network) == 1 and (network[0] == '--network=none' or network[0][10:] in {'container:' + x for x in owned})
                assert '--read-only' in argv and '--cap-drop=ALL' in argv
                assert not any(x.startswith(('--volume', '--mount', '--publish', '--privileged')) or x in ('-v', '-p') for x in argv)
                names.add(name)
            elif action in ('inspect', 'start', 'exec', 'rm'):
                assert any(x in owned or x in names for x in argv[2:])
            elif action == 'ps':
                filters = {'name=^/' + n + '$' for n in names} | {'label=org.sub2api.deploy.operation=' + n.rsplit('-', 1)[1] for n in names}
                assert argv[2:5] == ['-aq', '--no-trunc', '--filter'] and len(argv) == 6 and argv[5] in filters
            elif action == 'image':
                assert argv[2:3] == ['inspect'] and len(argv) == 4 and argv[3] in images
            else:
                raise RuntimeError('docker_action_forbidden:' + action)
        else:
            raise RuntimeError('non_isolated_command_forbidden')
        counter += 1
        with (args.output / ('command-%05d.stderr' % counter)).open('xb') as err:
            raw = run(argv, stderr_out=err, **kwargs)
        if argv[:2] == ['docker', 'create']:
            owned.add(raw.decode().strip())
        if argv[:2] == ['docker', 'inspect']:
            for item in json.loads(raw):
                if item['Id'] in owned and item['State'].get('Pid', 0):
                    pids.add(str(item['State']['Pid']))
        return raw

    load = lambda name: json.loads((args.bundle / name).read_bytes())
    source_catalog = json.loads((args.incident / 'catalog_before.json').read_bytes())
    report = {'status': 'running', 'production_access': False, 'network': 'none',
              'revision': REV, 'runs': [], 'started_unix': time.time()}

    class Backend(backend_class(Live, probe)):
        def http(self, pid, path, key=''):
            raw = super().http(pid, path, key)
            label = getattr(self, 'active_label', None)
            if label:
                if not hasattr(self, 'frontend_artifacts'):
                    self.frontend_artifacts = {}
                item = self.frontend_artifacts.setdefault(label, {})
                if path == '/':
                    repeated = super().http(pid, path, key)
                    assert frontend_digest(raw) == frontend_digest(repeated)
                    assert raw != repeated
                    item['html'] = repeated
                    item['frontend_contract_sha256'] = frontend_digest(raw)
                elif path.startswith('/assets/'):
                    item['asset'] = raw
            return raw

        def check_live(self):
            for image in images:
                assert json.loads(self.run(['docker', 'image', 'inspect', image], timeout=15))[0]['Id'] == image
            return {'images': sorted(images)}

        def sql(self, session, container, text, timeout=180):
            if text.startswith("UPDATE settings SET value='scratch-"):
                restored = Live.capture(self, session, container)
                report['restore_comparison'] = verify_restore_catalog(source_catalog, restored)
                (self.root / 'restore-comparison.json').write_text(json.dumps(report['restore_comparison']))
            return super().sql(session, container, text, timeout=timeout)

    class ReadySession(Session):
        def exec(self, container, command, timeout=60, **kwargs):
            if command == ['pg_isready', '-U', 'postgres']:
                command = ['pg_isready', '-h', '127.0.0.1', '-U', 'postgres', '-d', 'postgres']
            return super().exec(container, command, timeout=timeout, **kwargs)

    discovery = load('v0215-live-discovery.json')
    assert discovery['pg_image'] == pg and discovery['redis_image'] == redis
    pins = {'image_id': candidate, 'binary_sha256': BIN,
            'resource_sha256': '8490b7b9ad0e928a5b1a0e244fa61b5698600a33e47adaba22bed6f45632ce3f'}
    backend = Backend(args.output, discovery, load('v0215-migration-assessment.json'),
                      json.loads((args.bundle / 'v0215-live-pricing.json').read_bytes(), parse_float=Decimal), pins, runner=isolated_run)
    backend.source = args.incident / 'backup'
    backend.source_snapshot_sha256 = sha(backend.source / 'snapshot.json')
    assert sha(backend.source / 'database.dump') == 'e030e0a799f0ee323885403d2c09fc7b3d9b08e845971d8ff46d0dd2bbb08f23'
    phase = 'preflight'

    def interrupted(*_):
        signal.alarm(0)
        raise RuntimeError('isolated_rehearsal_deadline')

    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGALRM, interrupted)
    signal.alarm(2400)
    try:
        backend.check_live()
        with ReadySession(args.output, isolated_run) as session:
            phase = 'restore'
            print(json.dumps({'phase': phase}), flush=True)
            restored = backend.restore(session, backend.snapshot(session))
            for phase in ('old_before', 'candidate', 'old_after'):
                print(json.dumps({'phase': phase}), flush=True)
                result = backend.runtime(session, restored, phase)
                artifact = backend.frontend_artifacts[phase]
                result['frontend_contract_sha256'] = artifact['frontend_contract_sha256']
                verify_frontend(result, artifact['html'], artifact['asset'])
                (args.output / (phase + '.json')).write_text(json.dumps(result, indent=2))
                report['runs'].append({k: result[k] for k in ('label', 'image', 'ledger_count', 'catalog_equal_except_exact_delta', 'effective_false', 'frontend_sha256', 'asset_sha256')})
                report['runs'][-1]['frontend_contract_sha256'] = result['frontend_contract_sha256']
                report['runs'][-1]['rotating_nonce_verified'] = True
                report['runs'][-1]['pricing_sha256'] = hashlib.sha256(result['pricing'].encode()).hexdigest()
                (args.output / 'runtime-progress.json').write_text(json.dumps(report, indent=2))
        assert [r['ledger_count'] for r in report['runs']] == [290, 293, 293]
        assert len({r['pricing_sha256'] for r in report['runs']}) == 1
        report['status'] = 'passed'
    except Exception as exc:
        report.update(status='failed', phase=phase, error_type=type(exc).__name__, failure=probe.redacted_failure(exc, phase))
        print(json.dumps({'status': 'failed', 'phase': phase, 'error_type': type(exc).__name__}), flush=True)
    finally:
        signal.alarm(0)
        receipt = args.output / 'session.json'
        report['cleanup'] = json.loads(receipt.read_bytes()) if receipt.exists() else None
        report['elapsed_seconds'] = round(time.time() - report['started_unix'], 3)
        (args.output / 'runtime-result.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report), flush=True)
    assert report['status'] == 'passed' and report['cleanup']['status'] == 'cleaned'


if __name__ == '__main__':
    main()
