"""Approval-gated Nginx handoff; never starts/stops containers or migrates data.

Run only after the approved preparation phase has produced a warm candidate and
a reviewed, private runtime plan. The process must run in a detached systemd
service so SSH loss cannot kill the independent recovery supervisor.
"""
import argparse
import fcntl
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from controller import Handoff, atomic_json, digest, require_approval
from supervisor import supervise


def command(argv, *, data=None, timeout=5):
    # Never include credentials or command stderr in user-facing exceptions.
    result = subprocess.run(argv, input=data, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('bounded_command_failed:' + argv[0])
    return result.stdout


def configs(original, release):
    needle = b'        proxy_pass http://host.docker.internal:18081;'
    if original.count(needle) != 1:
        raise ValueError('expected_single_original_gateway_upstream')
    if not release.isalnum():
        raise ValueError('invalid_release_marker')
    replacement = (b'        proxy_pass http://host.docker.internal:18082;\n'
                   b'        add_header X-Sub2API-Release ' + release.encode() + b' always;')
    return original, original.replace(needle, replacement)


class Adapter:
    def __init__(self, plan, blue, green, directory):
        self.plan, self.blue, self.green = plan, blue, green
        self.directory = pathlib.Path(directory)
        self.path = pathlib.Path(plan['nginx_config'])
        self.nginx = plan['nginx_id']

    def read(self):
        return self.path.read_bytes()

    def install(self, value):
        if value not in (self.blue, self.green) or self.read() not in (self.blue, self.green):
            raise RuntimeError('foreign_config_or_unapproved_payload')
        if self.path.is_symlink():
            raise RuntimeError('symlink_config_not_supported')
        info = self.path.stat()
        fd, temporary = tempfile.mkstemp(prefix='.bluegreen-', dir=self.path.parent)
        try:
            with os.fdopen(fd, 'wb') as stream:
                os.fchmod(stream.fileno(), info.st_mode & 0o777)
                os.fchown(stream.fileno(), info.st_uid, info.st_gid)
                stream.write(value); stream.flush(); os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            fd = os.open(self.path.parent, os.O_DIRECTORY)
            try: os.fsync(fd)
            finally: os.close(fd)
        finally:
            if os.path.exists(temporary): os.unlink(temporary)

    def validate(self):
        command(['docker', 'exec', self.nginx, 'nginx', '-t'])

    def reload(self):
        command(['docker', 'exec', self.nginx, 'nginx', '-s', 'reload'])

    def curl(self, target, path, *, authenticated=False, inference=False):
        argv = ['curl', '--silent', '--show-error', '--fail', '--noproxy', '*',
                '--connect-timeout', '1', '--max-time', '8' if inference else '1.5',
                '--header', 'Connection: close']
        if target == 'proxy':
            url = 'https://aiproxy.in.yunmostar.com'
            argv += ['--resolve', 'aiproxy.in.yunmostar.com:443:127.0.0.1']
        else:
            url = 'http://172.17.0.1:' + ('18081' if target == 'blue' else '18082')
        headers = self.directory / ('headers-' + str(os.getpid()))
        argv += ['--dump-header', str(headers)]
        if authenticated or inference:
            # This file contains only an Authorization header, mode 0600.
            argv += ['--header', '@' + self.plan['probe_authorization_file']]
        if inference:
            argv += ['--header', 'Content-Type: application/json', '--data-binary', '@' + self.plan['probe_payload_file']]
        output = command(argv + [url + path], timeout=10 if inference else 3)
        return output, headers.read_text()

    def ready(self, side):
        try:
            result, _ = self.curl(side, '/v1/chat/completions', inference=True)
            return bool(json.loads(result).get('choices'))
        except Exception:
            return False

    def probe(self, side):
        try:
            body, headers = self.curl('proxy', '/health')
            marker = 'x-sub2api-release: ' + self.plan['release_marker']
            if (marker in headers.lower()) != (side == 'green'):
                return False
            if json.loads(body).get('status') != 'ok': return False
            body, _ = self.curl('proxy', '/v1/models', authenticated=True)
            return isinstance(json.loads(body).get('data'), list)
        except Exception:
            return False


def preflight(plan):
    proof_path = pathlib.Path(plan['readiness_receipt_file'])
    if digest(proof_path.read_bytes()) != plan['readiness_receipt_sha256']:
        raise RuntimeError('readiness_evidence_changed')
    proof = json.loads(proof_path.read_bytes())
    required = {'online_backup_restore_verified', 'schema_post_ledger_verified',
                'functional_inference', 'sse', 'frontend', 'pricing_unchanged',
                'call_audit_ready', 'peer_slots_preserved', 'isolated_spool',
                'single_periodic_owner', 'no_new_feature_writes'}
    if (proof.get('green_id') != plan['green_id'] or
        not 0 <= time.time() - proof.get('verified_unix', 0) <= 300 or
        not all(proof.get('checks', {}).get(name) is True for name in required)):
        raise RuntimeError('fresh_complete_readiness_evidence_required')
    expected = [(plan['blue_id'], plan['blue_image'], plan['blue_started']),
                (plan['green_id'], plan['green_image'], plan['green_started']),
                (plan['nginx_id'], plan['nginx_image'], plan['nginx_started'])]
    for identity, image, started in expected:
        info = json.loads(command(['docker', 'inspect', identity]))[0]
        if info['Id'] != identity or info['Image'] != image or info['State']['StartedAt'] != started:
            raise RuntimeError('container_identity_changed')
        if not info['State']['Running'] or info['State'].get('Health', {}).get('Status') != 'healthy':
            raise RuntimeError('container_not_healthy')
        if identity == plan['blue_id'] and info['HostConfig']['RestartPolicy']['Name'] != 'no':
            # The unpatched old binary clears peers' Redis slots on startup.
            # Automatic restart must be suspended during the approved overlap.
            raise RuntimeError('old_binary_must_not_restart_during_overlap')
        if identity == plan['green_id']:
            env = dict(x.split('=', 1) for x in info['Config']['Env'])
            if env.get('SUB2API_MULTI_INSTANCE') != 'true' or env.get('SUB2API_BACKGROUND_JOBS_ENABLED') != 'false':
                raise RuntimeError('candidate_role_not_safe')
            if info['HostConfig']['PortBindings'].get('8080/tcp') != [{'HostIp': '172.17.0.1', 'HostPort': '18082'}]:
                raise RuntimeError('candidate_bind_not_reviewed')
            if any(m['Source'] == plan['blue_data_directory'] and m['RW'] for m in info['Mounts']):
                raise RuntimeError('shared_writable_spool')
    for field in ('probe_authorization_file', 'probe_payload_file'):
        p = pathlib.Path(plan[field])
        if p.is_symlink() or p.stat().st_mode & 0o077:
            raise RuntimeError('probe_file_not_private')
        if digest(p.read_bytes()) != plan[field + '_sha256']:
            raise RuntimeError('probe_file_changed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plan', type=pathlib.Path, required=True)
    parser.add_argument('--approved-plan', type=pathlib.Path, required=True)
    parser.add_argument('--approval', type=pathlib.Path, required=True)
    parser.add_argument('--operation', type=pathlib.Path, required=True)
    args = parser.parse_args()
    os.umask(0o077)
    plan = json.loads(args.plan.read_bytes())
    approved = json.loads(args.approved_plan.read_bytes())
    approval = json.loads(args.approval.read_bytes())
    # This gate is before all production calls, including authenticated probes.
    require_approval(approved, approval)
    # Runtime identities are resolved during the authorized warm-up. They must
    # remain within the immutable image/config/old-instance scope the user saw.
    scope = approved['baseline']
    for prefix in ('blue', 'nginx'):
        for field in ('id', 'image', 'started'):
            if plan[prefix + '_' + field] != scope[prefix][field]:
                raise RuntimeError('runtime_plan_outside_approved_baseline')
    if (plan['green_image'] != approved['candidate']['image'] or
        plan['blue_config_sha256'] != scope['nginx_config_sha256'] or
        plan['green_config_sha256'] != approved['proposed']['nginx_config_sha256'] or
        plan['nginx_config'] != '/home/yms/basic_services/nginx-reverse-proxy/nginx/conf.d/aiproxy.in.yunmostar.com.conf' or
        plan['blue_data_directory'] != '/home/yms/basic_services/sub2api/data'):
        raise RuntimeError('runtime_plan_outside_approved_scope')
    if not os.environ.get('INVOCATION_ID'):
        raise RuntimeError('detached_systemd_service_required')
    if plan.get('status') != 'prepared_for_confirmed_handoff':
        raise RuntimeError('preparation_not_complete')
    if digest(command(['docker', 'exec', plan['nginx_id'], 'nginx', '-T'])) != scope['nginx_full_config_sha256']:
        raise RuntimeError('full_nginx_configuration_drift')
    args.operation.mkdir(mode=0o700)
    with open('/run/lock/sub2api-bluegreen.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        blue = pathlib.Path(plan['blue_config_file']).read_bytes()
        green = pathlib.Path(plan['green_config_file']).read_bytes()
        if digest(blue) != plan['blue_config_sha256'] or digest(green) != plan['green_config_sha256']:
            raise RuntimeError('config_artifact_changed')
        if configs(blue, plan['release_marker'])[1] != green:
            raise RuntimeError('unreviewed_nginx_change')
        preflight(plan)
        atomic_json(args.operation / 'consent.json', approval)
        adapter = Adapter(plan, blue, green, args.operation)
        result = supervise(Handoff(adapter, args.operation, blue, green))
        atomic_json(args.operation / 'result.json', result)
        print(json.dumps(result))


if __name__ == '__main__': main()
