#!/usr/bin/env python3
"""Export local Call Audit health without exposing administrator credentials."""
from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

CONTAINER = "sub2api"
HEALTH_URL = "http://172.17.0.1:18081/api/v1/admin/call-audit/health"
DEFAULT_OUTPUT = "/var/lib/node_exporter/textfile_collector/sub2api_call_audit.prom"
PREFIX = "sub2api_call_audit_"
MAX_RESPONSE_BYTES = 64 * 1024
TIMEOUT_SECONDS = 5
MAX_INTEGER = 2**64 - 1

BOOL_FIELDS = ("enabled", "ready", "schema_ready", "storage_ready", "dead_letter_ready")
COUNTER_FIELDS = (
    "captured", "capture_failures", "worker_failures", "dead_lettered",
    "raw_capture_disabled", "finalize_saturated",
)
GAUGE_FIELDS = (
    "active_sessions", "backlog", "dead_letter_files", "oldest_pending_seconds",
    "disk_used_percent", "disk_available_bytes",
)
RECOVERY_BOOL_FIELDS = ("dead_letter_auto_retry_enabled",)
RECOVERY_GAUGE_FIELDS = (
    "dead_letter_recovering", "dead_letter_retryable", "dead_letter_exhausted",
    "dead_letter_permanent", "oldest_dead_letter_seconds",
)
RECOVERY_COUNTER_FIELDS = ("dead_letter_retries_total", "dead_letter_recovered_total")
RECOVERY_FIELDS = RECOVERY_BOOL_FIELDS + RECOVERY_GAUGE_FIELDS + RECOVERY_COUNTER_FIELDS


class CollectorError(Exception):
    """Only constant diagnostic codes may cross the logging boundary."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CollectorError("duplicate_json_key")
        result[key] = value
    return result


def decode_json(raw, reason):
    def reject_constant(value):
        raise CollectorError(reason)

    try:
        return json.loads(raw, object_pairs_hook=reject_duplicate_keys, parse_constant=reject_constant)
    except (ValueError, UnicodeDecodeError):
        raise CollectorError(reason) from None


def administrator_key():
    # Docker output (including all Env values) stays in this process only.
    # Do not print subprocess exceptions/stderr, which can contain local data.
    try:
        proc = subprocess.run(
            ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock", "inspect", "--type", "container", CONTAINER],
            capture_output=True, timeout=TIMEOUT_SECONDS, check=False,
            env={"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "DOCKER_CONFIG": "/nonexistent", "LC_ALL": "C"},
        )
    except subprocess.TimeoutExpired:
        raise CollectorError("docker_timeout") from None
    except OSError:
        raise CollectorError("docker_unavailable") from None
    if proc.returncode:
        raise CollectorError("docker_inspection_failed")
    data = decode_json(proc.stdout, "docker_invalid_json")
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise CollectorError("docker_invalid_schema")
    item = data[0]
    if not isinstance(item.get("State"), dict) or not isinstance(item.get("Config"), dict):
        raise CollectorError("docker_invalid_schema")
    if item.get("Name") != "/" + CONTAINER or item["State"].get("Running") is not True:
        raise CollectorError("application_not_running")
    entries = item["Config"].get("Env")
    if not isinstance(entries, list) or not all(isinstance(v, str) for v in entries):
        raise CollectorError("docker_invalid_schema")
    keys = [v.split("=", 1)[1] for v in entries if v.startswith("ADMIN_API_KEY=")]
    if len(keys) != 1 or not keys[0] or len(keys[0]) > 8192 or any(c in keys[0] for c in "\r\n\x00"):
        raise CollectorError("administrator_key_unavailable")
    return keys[0]


def fetch_snapshot(key):
    # Never honor host proxy variables or follow redirects with a privileged
    # header. The destination is deliberately not a CLI/configurable URL.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    request = urllib.request.Request(HEALTH_URL, headers={"x-api-key": key, "Accept": "application/json"})
    try:
        try:
            response = opener.open(request, timeout=TIMEOUT_SECONDS)
        except urllib.error.HTTPError as exc:
            if exc.code != 503:
                exc.close()
                raise CollectorError("health_http_status") from None
            response = exc
        with response:
            status_code = response.getcode()
            if status_code not in (200, 503):
                raise CollectorError("health_http_status")
            if response.headers.get_content_type() != "application/json":
                raise CollectorError("health_content_type")
            raw = response.read(MAX_RESPONSE_BYTES + 1)
    except CollectorError:
        raise
    except (OSError, TimeoutError, urllib.error.URLError):
        raise CollectorError("health_transport_failed") from None
    if len(raw) > MAX_RESPONSE_BYTES:
        raise CollectorError("health_response_too_large")
    return validate_snapshot(decode_json(raw, "health_invalid_json"), status_code)


def validate_snapshot(data, status_code):
    if not isinstance(data, dict):
        raise CollectorError("health_invalid_schema")
    values = {}
    recovery_present = set(RECOVERY_FIELDS).intersection(data)
    # An old complete snapshot is observable, but its unknown recovery state
    # is never fabricated as zeros. Partial upgrades/schema drift fail closed.
    if recovery_present and len(recovery_present) != len(RECOVERY_FIELDS):
        raise CollectorError("health_partial_recovery_schema")
    bool_fields = BOOL_FIELDS + (RECOVERY_BOOL_FIELDS if recovery_present else ())
    int_fields = COUNTER_FIELDS + GAUGE_FIELDS
    if recovery_present:
        int_fields += RECOVERY_GAUGE_FIELDS + RECOVERY_COUNTER_FIELDS
    for name in bool_fields:
        if name not in data or type(data[name]) is not bool:
            raise CollectorError("health_invalid_boolean")
        values[name] = data[name]
    for name in int_fields:
        if name not in data or type(data[name]) is not int or not 0 <= data[name] <= MAX_INTEGER:
            raise CollectorError("health_invalid_integer")
        values[name] = data[name]
    if values["disk_used_percent"] > 100:
        raise CollectorError("health_invalid_percentage")
    if values["ready"] and not all(values[k] for k in BOOL_FIELDS):
        raise CollectorError("health_inconsistent_readiness")
    expected_status = 503 if values["enabled"] and not values["ready"] else 200
    if status_code != expected_status:
        raise CollectorError("health_inconsistent_status")
    values["recovery_metrics_available"] = bool(recovery_present)
    return values


def previous_success(output):
    try:
        fd = os.open(output, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "r", encoding="ascii") as stream:
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                return 0
            text = stream.read(128 * 1024)
    except (OSError, UnicodeError):
        return 0
    matches = re.findall(r"^" + PREFIX + r"collector_last_success_unixtime ([0-9]+(?:\.[0-9]+)?)$", text, re.M)
    if len(matches) != 1:
        return 0
    value = float(matches[0])
    return value if math.isfinite(value) and 0 <= value <= time.time() + 5 else 0


def metric(name, value, kind="gauge"):
    full = PREFIX + name
    help_text = "Call Audit " + name.replace("_", " ") + "."
    return f"# HELP {full} {help_text}\n# TYPE {full} {kind}\n{full} {int(value) if isinstance(value, bool) else value}\n"


def render(snapshot, now, last_success):
    parts = [
        metric("collector_success", snapshot is not None),
        metric("collector_last_attempt_unixtime", now),
        metric("collector_last_success_unixtime", now if snapshot is not None else last_success),
    ]
    if snapshot is None:
        # Replace all former application series on collection failure. A stale
        # ready=1 or dead_letter_files=0 must not survive a failed sample.
        return "".join(parts)
    for field in BOOL_FIELDS + GAUGE_FIELDS + ("recovery_metrics_available",):
        parts.append(metric(field, snapshot[field]))
    for field in COUNTER_FIELDS:
        parts.append(metric(field + "_total", snapshot[field], "counter"))
    if snapshot["recovery_metrics_available"]:
        for field in RECOVERY_BOOL_FIELDS + RECOVERY_GAUGE_FIELDS:
            parts.append(metric(field, snapshot[field]))
        for field in RECOVERY_COUNTER_FIELDS:
            parts.append(metric(field, snapshot[field], "counter"))
    return "".join(parts)


def atomic_write(output, text):
    output = Path(output)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="ascii", prefix=".sub2api-audit-",
                                         suffix=".tmp", dir=output.parent, delete=False) as stream:
            temporary = stream.name
            os.fchmod(stream.fileno(), 0o644)
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        temporary = None
        fd = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass


def collect_once(output, now=None):
    now = time.time() if now is None else now
    last_success = previous_success(output)
    snapshot = None
    reason = "ok"
    try:
        key = administrator_key()
        try:
            snapshot = fetch_snapshot(key)
        finally:
            del key
    except CollectorError as exc:
        reason = str(exc)
    except Exception:
        # Never log raw exception text, tracebacks, Docker JSON or HTTP bodies.
        reason = "internal_error"
    try:
        atomic_write(output, render(snapshot, now, last_success))
    except Exception:
        # An unwritable filesystem can prevent any metric publication. The
        # independent stale/absent rule is required to detect that condition.
        return False, "metric_publication_failed"
    return snapshot is not None, reason


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(DEFAULT_OUTPUT))
    args = parser.parse_args(argv)
    success, reason = collect_once(args.output)
    print(json.dumps({"collector_success": int(success), "reason": reason}), file=sys.stdout if success else sys.stderr)
    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
