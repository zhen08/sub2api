"""Loopback-only fake upstream and traffic generator for the isolated lab.

Never use these credentials, synthetic responses or relaxed URL settings in
production. Run this module inside the lab's network-none namespace.
"""
import argparse
import concurrent.futures
import http.server
import json
import pathlib
import threading
import time
import urllib.error
import urllib.request
import uuid


STATS = {'started': 0, 'finished': 0, 'active': {}, 'max_active': {}, 'requests': []}
LOCK = threading.Lock()


class Upstream(http.server.BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *_): pass

    def do_GET(self):
        with LOCK:
            raw = json.dumps(STATS).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        data = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        content = str(data.get('messages', [{}])[-1].get('content', data.get('input', '')))
        parts = content.split(':')
        kind = parts[1] if len(parts) > 1 else 'unknown'
        delay = float(parts[2]) if len(parts) > 2 else 0
        delay = min(max(delay, 0), 180)
        response_id = 'chatcmpl-' + uuid.uuid4().hex
        usage = {'prompt_tokens': 1000, 'completion_tokens': 100, 'total_tokens': 1100}
        base = {'id': response_id, 'created': int(time.time()), 'model': 'gpt-4o', 'object': 'chat.completion'}
        with LOCK:
            STATS['started'] += 1
            STATS['active'][kind] = STATS['active'].get(kind, 0) + 1
            STATS['max_active'][kind] = max(STATS['max_active'].get(kind, 0), STATS['active'][kind])
            if kind in ('limited', 'startup_hold'):
                total = sum(STATS['active'].get(k, 0) for k in ('limited', 'startup_hold'))
                STATS['max_active']['limited_account_total'] = max(STATS['max_active'].get('limited_account_total', 0), total)
        completed = False
        try:
            if data.get('stream'):
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Cache-Control', 'no-cache')
                self.send_header('Connection', 'close')
                self.send_header('X-Request-Id', response_id)
                self.end_headers()
                base['object'] = 'chat.completion.chunk'
                def event(value):
                    self.wfile.write(b'data: ' + json.dumps(value).encode() + b'\n\n')
                    self.wfile.flush()
                event(dict(base, choices=[{'index': 0, 'delta': {'role': 'assistant', 'content': 'begin'}, 'finish_reason': None}]))
                deadline = time.monotonic() + delay
                while time.monotonic() < deadline:
                    time.sleep(min(1, max(0, deadline-time.monotonic())))
                    event(dict(base, choices=[{'index': 0, 'delta': {'content': '.'}, 'finish_reason': None}]))
                event(dict(base, choices=[{'index': 0, 'delta': {}, 'finish_reason': 'stop'}]))
                event(dict(base, choices=[], usage=usage))
                self.wfile.write(b'data: [DONE]\n\n'); self.wfile.flush()
                self.close_connection = True
            else:
                time.sleep(delay)
                raw = json.dumps(dict(base, choices=[{'index': 0, 'message': {'role': 'assistant', 'content': 'ok'}, 'finish_reason': 'stop'}], usage=usage)).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.send_header('X-Request-Id', response_id)
                self.end_headers(); self.wfile.write(raw); self.wfile.flush()
            completed = True
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with LOCK:
                STATS['active'][kind] -= 1
                STATS['finished'] += int(completed)
                STATS['requests'].append({'id': response_id, 'kind': kind, 'completed': completed})


def get(port, path='/health', key=None, timeout=2):
    headers = {'Connection': 'close'}
    if key: headers['Authorization'] = 'Bearer ' + key
    request = urllib.request.Request('http://127.0.0.1:' + str(port) + path, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, dict(response.headers), response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def request(port, key, kind='quick', delay=0, stream=False, started=None):
    payload = {'model': 'gpt-4o', 'messages': [{'role': 'user', 'content': 'bg:'+kind+':'+str(delay)+':'+uuid.uuid4().hex}],
               'max_tokens': 100, 'stream': stream}
    if stream: payload['stream_options'] = {'include_usage': True}
    req = urllib.request.Request('http://127.0.0.1:'+str(port)+'/v1/chat/completions', json.dumps(payload).encode(),
                                 {'Authorization': 'Bearer '+key, 'Content-Type': 'application/json', 'Connection': 'close'})
    begin = time.monotonic()
    try:
        with urllib.request.urlopen(req, timeout=190 if stream else 20) as response:
            if stream:
                content = bytearray()
                for line in response:
                    content.extend(line)
                    if started and b'data:' in line and not started.exists():
                        started.write_text(json.dumps({'started': time.monotonic(), 'side': response.headers.get('X-Lab-Release')}))
                complete = b'data: [DONE]' in content
            else:
                body = json.loads(response.read())
                complete = bool(body.get('choices'))
            return {'status': response.status, 'complete': complete, 'seconds': round(time.monotonic()-begin, 3),
                    'side': response.headers.get('X-Lab-Release'), 'kind': kind}
    except urllib.error.HTTPError as exc:
        # Only fake tenant errors can reach this file; no production credentials.
        return {'status': exc.code, 'complete': False, 'error': exc.read().decode()[:700], 'kind': kind}
    except Exception as exc:
        return {'status': 0, 'complete': False, 'error': type(exc).__name__, 'kind': kind}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['upstream', 'smoke', 'load', 'stream', 'hold', 'poll', 'api-poll'])
    parser.add_argument('--config', type=pathlib.Path, required=True)
    parser.add_argument('--output', type=pathlib.Path)
    args = parser.parse_args()
    cfg = json.loads(args.config.read_bytes())
    if args.mode == 'upstream':
        http.server.ThreadingHTTPServer(('127.0.0.1', 9099), Upstream).serve_forever()
    elif args.mode == 'smoke':
        result = [request(p, cfg['key']) for p in (8081, 8082)]
    elif args.mode == 'stream':
        result = request(8088, cfg['key'], 'long', 135, True, args.output.with_suffix('.started'))
    elif args.mode == 'hold':
        result = request(8081, cfg['limited_key'], 'startup_hold', 45, True, args.output.with_suffix('.started'))
    elif args.mode == 'api-poll':
        result=[]
        deadline=time.monotonic()+90
        while time.monotonic()<deadline and not args.output.with_suffix('.stop').exists():
            result.append(request(8088,cfg['key'],'cutover_probe'))
            time.sleep(.2)
    elif args.mode == 'load':
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            futures = [pool.submit(request, 8081 if n % 2 else 8082, cfg['key'], 'parallel', .15) for n in range(40)]
            result = {'parallel': [f.result() for f in futures]}
            futures = [pool.submit(request, 8081 if n % 2 else 8082, cfg['limited_key'], 'limited', 1) for n in range(12)]
            result['limited'] = [f.result() for f in futures]
        result['upstream'] = json.loads(get(9099, '/stats')[2])
    else:
        result = []
        deadline = time.monotonic() + 170
        while time.monotonic() < deadline and not args.output.with_suffix('.stop').exists():
            begin = time.monotonic()
            try:
                status, headers, _ = get(8088, timeout=.8)
                item = {'at': begin, 'status': status, 'side': headers.get('X-Lab-Release')}
            except Exception:
                item = {'at': begin, 'status': 0}
            result.append(item)
            time.sleep(.1)
    args.output.write_text(json.dumps(result, indent=2))
    if args.mode != 'upstream': print(json.dumps({'mode': args.mode, 'completed': True}), flush=True)


if __name__ == '__main__': main()
