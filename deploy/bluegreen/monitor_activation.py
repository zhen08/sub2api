"""Bounded independent HTTPS sampling across an authorized traffic handoff."""
import json
import pathlib
import subprocess
import sys
import time

root = pathlib.Path(sys.argv[1])
samples = []
started = time.monotonic()
with (root / 'availability-samples.jsonl').open('x') as log:
    while time.monotonic() - started < 120:
        at = time.monotonic()
        try:
            r = subprocess.run(['curl', '--silent', '--show-error', '--noproxy', '*',
                '--resolve', 'aiproxy.in.yunmostar.com:443:127.0.0.1', '--connect-timeout', '1',
                '--max-time', '1.5', '--header', 'Connection: close', '--include',
                '--write-out', '\n%{http_code}', 'https://aiproxy.in.yunmostar.com/health'],
                capture_output=True, timeout=3)
            body = r.stdout.decode()
            sample = {'at': time.time(), 'elapsed': round(at - started, 3),
                      'seconds': round(time.monotonic() - at, 3),
                      'curl_exit_code': r.returncode,
                      'http_status': body.rsplit('\n', 1)[-1],
                      'failure_detail': r.stderr.decode(errors='replace')[:512] if r.returncode else '',
                      'ok': r.returncode == 0 and body.endswith('\n200') and '"status":"ok"' in body,
                      'green': 'x-sub2api-release: bg102b5cc0753e' in body.lower()}
        except subprocess.TimeoutExpired:
            sample = {'at': time.time(), 'elapsed': round(at - started, 3), 'ok': False, 'green': False}
        samples.append(sample)
        log.write(json.dumps(sample) + '\n'); log.flush()
        time.sleep(max(0, .2 - (time.monotonic() - at)))
result = {'samples': len(samples), 'failed_samples': sum(not s['ok'] for s in samples),
          'blue_samples': sum(s['ok'] and not s['green'] for s in samples),
          'green_samples': sum(s['ok'] and s['green'] for s in samples),
          'duration_seconds': round(time.monotonic() - started, 3),
          'maximum_sample_seconds': max(s.get('seconds', 3) for s in samples),
          'scope': 'origin_https_health_sampled_every_200ms; not_every_customer_request'}
(root / 'availability-result.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
