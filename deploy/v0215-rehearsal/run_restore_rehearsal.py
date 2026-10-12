"""Run only the restore stage on retained backup bytes, with no networking."""
import argparse
import json
import os
import pathlib
import shutil
import sys
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--bundle', type=pathlib.Path, required=True)
    p.add_argument('--incident', type=pathlib.Path, required=True)
    p.add_argument('--output', type=pathlib.Path, required=True)
    p.add_argument('--pg-image', required=True)
    p.add_argument('--expect-failure', action='store_true')
    args = p.parse_args()
    os.umask(0o077)
    sys.dont_write_bytecode = True
    sys.path.insert(0, str(args.bundle.resolve()))
    from v0215_deploy_backend import Deployment
    from v0215_live import Live
    from v0215_process import run
    from v0215_rollout import sha

    args.output.mkdir(mode=0o700)
    shutil.copytree(args.incident / 'backup', args.output / 'backup')
    for name in ('catalog_before.json', 'ledger_before.json'):
        shutil.copyfile(args.incident / name, args.output / name)
    load = lambda name: json.loads((args.bundle / name).read_bytes())
    discovery = load('v0215-live-discovery.json')
    discovery['pg_image'] = args.pg_image
    owned = set()

    def isolated_run(argv, **kwargs):
        if argv[0] != 'docker':
            raise RuntimeError('non_docker_command_forbidden')
        action = argv[1]
        if action == 'create':
            assert '--network=none' in argv and args.pg_image in argv
            assert '--read-only' in argv and '--cap-drop=ALL' in argv
            assert not any(x.startswith(('--volume', '--mount', '--publish', '--privileged')) or x in ('-v', '-p') for x in argv)
            name = argv[argv.index('--name') + 1]
            assert name.startswith('v0215-deployrestore-')
            owned.add(name)
            raw = run(argv, **kwargs)
            owned.add(raw.decode().strip())
            return raw
        if action in ('inspect', 'start', 'rm', 'exec'):
            target = next((item for item in argv[2:] if item in owned), None)
            if target is None:
                raise RuntimeError('unowned_container_forbidden')
            return run(argv, **kwargs)
        if action == 'ps' and argv[2:5] == ['-aq', '--no-trunc', '--filter']:
            permitted = set()
            for name in owned:
                if name.startswith('v0215-deployrestore-'):
                    permitted.add('name=^/' + name + '$')
                    permitted.add('label=org.sub2api.deploy.operation=' + name.rsplit('-', 1)[1])
            if len(argv) == 6 and argv[5] in permitted:
                return run(argv, **kwargs)
        raise RuntimeError('docker_action_forbidden:' + action)

    live = Live(args.output, discovery, load('v0215-migration-assessment.json'),
                load('v0215-live-pricing.json'), {}, runner=isolated_run)
    backend = Deployment(args.output, live, {'isolated_restore': True},
                         {'image_id': args.pg_image}, {}, runner=isolated_run)
    started = time.monotonic()
    result = {'backup_sha256': sha(args.output / 'backup/database.dump'),
              'network': 'none', 'production_access': False}
    try:
        result['restore'] = backend.restore()
        result['status'] = 'passed'
    except Exception as exc:
        result['status'] = 'failed'
        result['error_type'] = type(exc).__name__
        result['error'] = str(exc)
    result['elapsed_seconds'] = round(time.monotonic() - started, 3)
    receipt = args.output / 'restore-proof/session.json'
    result['cleanup'] = json.loads(receipt.read_bytes()) if receipt.exists() else None
    (args.output / 'rehearsal-result.json').write_text(json.dumps(result, indent=2))
    print(json.dumps(result), flush=True)
    clean = result['cleanup'] and result['cleanup']['status'] in ('cleaned', 'failed_cleaned')
    if args.expect_failure:
        assert result['status'] == 'failed' and result['error'] == 'restored_full_catalog' and clean
    else:
        assert result['status'] == 'passed' and clean


if __name__ == '__main__':
    main()
