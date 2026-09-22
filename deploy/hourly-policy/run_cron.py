"""Cron apply entrypoint: compact transitions; preserve diagnostic errors."""
from pathlib import Path
import contextlib
import io
import json
import sys
import run_remote

if __name__ == '__main__':
    sys.argv = [str(Path(__file__).with_name('run_remote.py')), '--apply']
    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            code = run_remote.main()
    except BaseException:
        sys.stdout.write(output.getvalue())
        raise
    text = output.getvalue()
    try:
        message = json.loads(text) if text.strip() else None
        if code == 0 and isinstance(message, dict) and message.get('status') == 'transitions':
            compact = [{key: target[key] for key in ('email', 'from', 'to')}
                       for target in message['targets']]
            text = ''.join(json.dumps(item, ensure_ascii=False) + '\n' for item in compact)
    except (ValueError, KeyError, TypeError):
        pass  # Never hide unexpected output or diagnostics.
    sys.stdout.write(text)
    raise SystemExit(code)
