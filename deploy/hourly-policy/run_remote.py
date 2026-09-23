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


class WrapperError(Exception):
    pass


def bounded_process(cmd, source, timeout, output_limit):
    """Stream stdin and both outputs concurrently, with a shared byte budget."""
    deadline = time.monotonic() + timeout
    output = bytearray()
    stderr_seen = False
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
        return subprocess.CompletedProcess(cmd, proc.returncode, bytes(output), b'present' if stderr_seen else b'')
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
    deadline = time.monotonic() + 900
    for attempt, delay in enumerate((0, 5, 15), 1):
        if int(time.time() // 3600) - 1 != hour:
            print(json.dumps({'status':'error','error':'hour_rollover','hour':hour}))
            return 1
        remaining = min(deadline - time.monotonic(), (hour + 2)*3600 - time.time())
        if remaining <= delay + 1:
            print(json.dumps({'status':'error','error':'deadline_exhausted','hour':hour}))
            return 1
        if delay:
            time.sleep(delay)
        if int(time.time() // 3600) - 1 != hour:
            print(json.dumps({'status':'error','error':'hour_rollover','hour':hour}))
            return 1
        remaining = min(deadline - time.monotonic(), (hour + 2)*3600 - time.time())
        if remaining <= 1:
            print(json.dumps({'status':'error','error':'deadline_exhausted','hour':hour}))
            return 1
        try:
            # Reserve cleanup time within the 270s attempt / 900s total budgets.
            result = bounded_process(command(mode, hour), source, min(269, remaining - 1), 1048576)
            if result.returncode == 0 and not result.stderr:
                if result.stdout:
                    print(result.stdout.decode('utf-8'), end='')
                return 0
            failure = {'status':'error','error':'ssh_nonzero' if result.returncode else 'unexpected_stderr',
                       'exit_code':result.returncode,'attempt':attempt,'hour':hour}
            if result.returncode == 1:
                try:
                    remote = json.loads(result.stdout)
                except (ValueError, UnicodeError, RecursionError):
                    remote = None
                # Emit only this local static literal, never arbitrary remote data.
                if (isinstance(remote, dict) and remote.get('status') == 'error'
                        and remote.get('error') == 'source channel configuration drift'):
                    failure['remote_error'] = 'source channel configuration drift'
            print(json.dumps(failure))
        except ValueError:
            print(json.dumps({'status':'error','error':'invalid_ssh_configuration'}))
            return 1
        except (WrapperError, subprocess.TimeoutExpired, OSError):
            print(json.dumps({'status':'error','error':'SSH wrapper timeout or execution failure',
                              'attempt':attempt,'hour':hour}))
    print(json.dumps({'status':'error','error':'retries_exhausted','attempts':3,'hour':hour}))
    return 1


if __name__=='__main__':
    sys.exit(main())
