"""Read-only channel guard projection; no credentials or unrelated fields emitted.

Run locally: python3 collect_channel_fixture.py
Uses the same VM/container API discovery as the controller and the read-only
preflight SSH destination. Does not execute controller.main or write remotely.
"""
import hashlib
import json
from pathlib import Path
import subprocess

FIELDS = ('id', 'group_ids', 'model_mapping', 'status', 'restrict_models',
          'billing_model_source', 'features', 'features_config', 'model_pricing',
          'apply_pricing_to_account_stats', 'account_stats_pricing_rules')


def main():
    source = Path(__file__).with_name('controller.py').read_text()
    source += '''
try:
    api = API.from_docker()
    rows = pages(api.get, '/channels')
    fields = FIELDS_PLACEHOLDER
    projection = [{k: row[k] for k in fields} for row in rows]
    print(json.dumps({'channels': projection,
                      'collected_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                      'source': 'GET /api/v1/admin/channels; exact guard field projection'}, sort_keys=True))
except Exception:
    print('{"error":"read_only_collection_failed_details_suppressed"}')
    sys.exit(1)
'''.replace('FIELDS_PLACEHOLDER', repr(FIELDS))
    # A different __name__ loads definitions without entering controller.main.
    script = "exec(" + repr(source) + ", {'__name__': 'channel_fixture_readonly'})"
    result = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15',
                             'vm', 'sudo -n python3 -'], input=script, text=True,
                            capture_output=True, timeout=120)
    if result.returncode or result.stderr:
        raise SystemExit('read-only SSH collection failed; details suppressed')
    data = json.loads(result.stdout)
    if 'error' in data:
        raise SystemExit(data['error'])
    target = Path(__file__).with_name('fixtures') / 'live_channels_20260923.json'
    target.parent.mkdir(exist_ok=True)
    target.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')
    print(json.dumps({'path': str(target), 'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
                      'channel_count': len(data['channels'])}))


if __name__ == '__main__':
    main()
