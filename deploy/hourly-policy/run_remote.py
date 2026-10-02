#!/usr/bin/env python3
"""Non-agent SSH runner. Source goes to stdin; no credentials leave VM."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import os
import selectors
import signal
import time
import re
import stat


class WrapperError(Exception):
    pass


class SSHResult(subprocess.CompletedProcess):
    ssh_error: str = 'unknown'


STDERR_CLASSIFICATION_LIMIT = 4096
SSH_ERROR_PATTERNS = {
    # Permanent security failures take precedence over transient wording.
    'auth_failed': (b'permission denied', b'authentication failed'),
    'host_key': (b'host key verification failed', b'remote host identification has changed'),
    'timeout': (b'timed out', b'timeout'),
    'refused': (b'connection refused',),
    'network_unreachable': (b'network is unreachable', b'no route to host'),
    'connection_closed': (b'connection closed', b'connection reset', b'broken pipe'),
}


def classify_ssh_stderr(stderr):
    """Heuristic only: inspect a bounded prefix, return local static codes only."""
    prefix = stderr[:STDERR_CLASSIFICATION_LIMIT].lower()
    for code, patterns in SSH_ERROR_PATTERNS.items():
        if any(pattern in prefix for pattern in patterns):
            return code
    return 'unknown'


def record_retry_failures(hour, failures, outcome):
    """Best-effort last-run record, never raw subprocess output or home fallback.

    Only called with locally constructed diagnostics. Replace rather than append;
    a clean first-attempt success preserves the last failure-bearing run.
    """
    directory = os.environ.get('HOURLY_POLICY_LOCAL_STATE_DIR')
    if not directory or not failures:
        return
    directory_fd = None
    temporary = None
    try:
        path = Path(directory)
        if not path.is_absolute() or path in (Path.home(), Path('/')):
            return
        directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        info = os.fstat(directory_fd)
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            return
        payload = (json.dumps({'hour': hour, 'outcome': outcome,
                               'failures': failures[:3]}) + '\n').encode('utf-8')
        if len(payload) > 4096:
            return
        # A fixed exclusive slot bounds disk use even after interrupted writers.
        # Never remove another writer's slot; stale slots require operator review.
        candidate = '.last-retry-failure.tmp'
        fd = os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=directory_fd)
        temporary = candidate
        with os.fdopen(fd, 'wb') as stream:
            stream.write(payload)
        os.replace(temporary, 'last-retry-failure.json',
                   src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        temporary = None  # The slot is free; a later writer may already own it.
    except (OSError, ValueError):
        pass  # Optional diagnostics must not turn recovered retries into alerts.
    finally:
        if directory_fd is not None:
            if temporary is not None:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)


def bounded_process(cmd, source, timeout, output_limit):
    """Stream stdin and both outputs concurrently, with a shared byte budget."""
    deadline = time.monotonic() + timeout
    output = bytearray()
    stderr_seen = False
    stderr_prefix = bytearray()
    count = offset = 0
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, bufsize=0, start_new_session=True)
    try:
        with selectors.DefaultSelector() as selector:
            for stream, event in ((proc.stdin, selectors.EVENT_WRITE),
                                  (proc.stdout, selectors.EVENT_READ),
                                  (proc.stderr, selectors.EVENT_READ)):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, event)
            while selector.get_map() or proc.poll() is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise WrapperError('timeout')
                for key, event in selector.select(min(remaining, .05)):
                    stream = key.fileobj
                    if event == selectors.EVENT_WRITE:
                        try:
                            sent = os.write(stream.fileno(), source[offset:offset + 65536])
                            offset += sent
                        except BrokenPipeError:
                            offset = len(source)
                        if offset == len(source):
                            selector.unregister(stream)
                            stream.close()
                    else:
                        chunk = os.read(stream.fileno(), 65536)
                        if not chunk:
                            selector.unregister(stream)
                            stream.close()
                            continue
                        count += len(chunk)
                        if count > output_limit:
                            raise WrapperError('output_limit')
                        if stream is proc.stdout:
                            output.extend(chunk)
                        else:
                            stderr_seen = True
                            stderr_prefix.extend(chunk[:max(0, STDERR_CLASSIFICATION_LIMIT - len(stderr_prefix))])
        result = SSHResult(cmd, proc.returncode, bytes(output), b'present' if stderr_seen else b'')
        result.ssh_error = classify_ssh_stderr(stderr_prefix)
        return result
    finally:
        for stream in (proc.stdin, proc.stdout, proc.stderr):
            stream.close()
        # Kill the local process group even if its leader already exited.
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            proc.wait(timeout=.2)
        except subprocess.TimeoutExpired:
            pass
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.wait(timeout=.2)


def command(mode, expected_hour=None):
    if mode not in ('--dry-run','--apply','--collect-only'):
        raise ValueError('unsupported mode')
    host = os.environ.get('HOURLY_POLICY_SSH_HOST', '')
    port = os.environ.get('HOURLY_POLICY_SSH_PORT', '22')
    if (not re.fullmatch(r'(?:[A-Za-z_][A-Za-z0-9_-]*@)?[A-Za-z0-9][A-Za-z0-9._-]*', host)
            or not re.fullmatch(r'[0-9]{1,5}', port) or not 1 <= int(port) <= 65535):
        raise ValueError('invalid SSH configuration')
    suffix = '' if expected_hour is None else ' --expected-hour ' + str(int(expected_hour))
    return ['ssh','-o','BatchMode=yes','-o','ConnectTimeout=15',
            '-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3',
            '-p',port,host,'sudo -n python3 - ' + mode + suffix]


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    modes=parser.add_mutually_exclusive_group()
    modes.add_argument('--dry-run',action='store_true')
    modes.add_argument('--collect-only',action='store_true')
    modes.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    mode='--apply' if args.apply else '--collect-only' if args.collect_only else '--dry-run'
    try:
        source=Path(__file__).with_name('controller.py').read_bytes()
    except OSError:
        print(json.dumps({'status':'error','error':'source_read_failed'}))
        return 1
    hour = int(time.time() // 3600) - 1
    started = time.monotonic()
    deadline = started + 900
    failures = []

    def fail(terminal):
        # Flush only on terminal failure, including hour/deadline guard exits.
        record_retry_failures(hour, failures, terminal['error'])
        for failure in failures:
            print(json.dumps(failure))
        print(json.dumps(terminal))
        return 1

    # Absolute offsets disperse even fast connection failures; slow attempts do
    # not accumulate backoff. Last slot retains a full 269s + cleanup budget.
    for attempt, offset in enumerate((0, 300, 630), 1):
        delay = max(0, started + offset - time.monotonic())
        if int(time.time() // 3600) - 1 != hour:
            return fail({'status':'error','error':'hour_rollover','hour':hour})
        remaining = min(deadline - time.monotonic(), (hour + 2)*3600 - time.time())
        if remaining <= delay + 1:
            return fail({'status':'error','error':'deadline_exhausted','hour':hour})
        if delay:
            time.sleep(delay)
        if int(time.time() // 3600) - 1 != hour:
            return fail({'status':'error','error':'hour_rollover','hour':hour})
        remaining = min(deadline - time.monotonic(), (hour + 2)*3600 - time.time())
        if remaining <= 1:
            return fail({'status':'error','error':'deadline_exhausted','hour':hour})
        try:
            # Reserve cleanup time within the 270s attempt / 900s total budgets.
            result = bounded_process(command(mode, hour), source, min(269, remaining - 1), 1048576)
            if result.returncode == 0 and not result.stderr:
                if result.stdout:
                    print(result.stdout.decode('utf-8'), end='')
                record_retry_failures(hour, failures, 'recovered')
                return 0
            failure = {'status':'error','error':'ssh_nonzero' if result.returncode else 'unexpected_stderr',
                       'exit_code':result.returncode,'attempt':attempt,'hour':hour}
            ssh_error = getattr(result, 'ssh_error', 'unknown')
            failure['ssh_error'] = ssh_error if ssh_error in SSH_ERROR_PATTERNS else 'unknown'
            if result.returncode == 1:
                try:
                    remote = json.loads(result.stdout)
                except (ValueError, UnicodeError, RecursionError):
                    remote = None
                # Emit only this local static literal, never arbitrary remote data.
                if (isinstance(remote, dict) and remote.get('status') == 'error'
                        and remote.get('error') == 'source channel configuration drift'):
                    failure['remote_error'] = 'source channel configuration drift'
            failures.append(failure)
            if result.returncode != 255 or failure['ssh_error'] not in (
                    'timeout', 'refused', 'network_unreachable', 'connection_closed'):
                return fail({'status':'error','error':'non_retryable_failure','hour':hour})
        except ValueError:
            return fail({'status':'error','error':'invalid_ssh_configuration'})
        except (WrapperError, subprocess.TimeoutExpired, OSError) as error:
            timed_out = (isinstance(error, subprocess.TimeoutExpired) or
                         isinstance(error, WrapperError) and error.args == ('timeout',))
            failures.append({'status':'error','error':'SSH wrapper timeout or execution failure',
                             'attempt':attempt,'hour':hour,
                             'ssh_error':'timeout' if timed_out else 'unknown'})
            # A local watchdog/IO/output-limit failure is not proof of a
            # transient SSH transport fault. Preserve pending state for review.
            return fail({'status':'error','error':'non_retryable_failure','hour':hour})
    return fail({'status':'error','error':'retries_exhausted','attempts':3,'hour':hour})


if __name__=='__main__':
    sys.exit(main())
