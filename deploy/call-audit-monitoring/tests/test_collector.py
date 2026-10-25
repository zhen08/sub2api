"""No Docker socket, HTTP service, credentials or third-party modules required."""
import contextlib
from email.message import Message
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest import mock
import urllib.error

MODULE_PATH = Path(__file__).resolve().parents[1] / "collector.py"
SPEC = importlib.util.spec_from_file_location("call_audit_collector", MODULE_PATH)
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)


def snapshot(*, recovery=True, ready=True):
    result = {name: True for name in collector.BOOL_FIELDS}
    result.update({name: 0 for name in collector.COUNTER_FIELDS + collector.GAUGE_FIELDS})
    result["ready"] = ready
    result["dead_letter_ready"] = ready
    if recovery:
        result.update({name: 0 for name in collector.RECOVERY_GAUGE_FIELDS + collector.RECOVERY_COUNTER_FIELDS})
        result["dead_letter_auto_retry_enabled"] = True
    return result


def headers(content_type="application/json"):
    result = Message()
    result["Content-Type"] = content_type
    return result


class Response(io.BytesIO):
    def __init__(self, data, status=200, content_type="application/json"):
        super().__init__(data)
        self.status = status
        self.headers = headers(content_type)

    def getcode(self):
        return self.status


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "audit.prom"

    def collect(self, data=None, *, status=200, error=None, now=200):
        with mock.patch.object(collector, "administrator_key", return_value="secret-never-exported"), \
                mock.patch.object(collector, "fetch_snapshot") as fetch:
            if error:
                fetch.side_effect = error
            else:
                fetch.return_value = collector.validate_snapshot(data or snapshot(), status)
            return collector.collect_once(self.output, now)

    def test_valid_sample_only_emits_whitelisted_metrics(self):
        data = snapshot()
        data.update({"readiness_error": "secret", "user_id": "sensitive", "request_body": "private"})
        self.assertEqual(self.collect(data), (True, "ok"))
        output = self.output.read_text()
        self.assertIn("sub2api_call_audit_collector_success 1\n", output)
        self.assertIn("sub2api_call_audit_recovery_metrics_available 1\n", output)
        self.assertIn("# TYPE sub2api_call_audit_capture_failures_total counter\n", output)
        for forbidden in ("secret", "sensitive", "private", "user_id", "request_body", "{"):
            self.assertNotIn(forbidden, output)

    def test_http_503_is_a_successful_degraded_sample(self):
        data = snapshot(ready=False)
        data["dead_letter_files"] = 21
        data["dead_letter_retryable"] = 21
        failure = urllib.error.HTTPError(collector.HEALTH_URL, 503, "Unavailable", headers(),
                                         io.BytesIO(json.dumps(data).encode()))
        opener = mock.Mock()
        opener.open.side_effect = failure
        with mock.patch.object(collector, "administrator_key", return_value="private-key"), \
                mock.patch.object(collector.urllib.request, "build_opener", return_value=opener):
            self.assertEqual(collector.collect_once(self.output, 200), (True, "ok"))
        output = self.output.read_text()
        self.assertIn("sub2api_call_audit_ready 0\n", output)
        self.assertIn("sub2api_call_audit_dead_letter_files 21\n", output)
        self.assertIn("sub2api_call_audit_collector_success 1\n", output)
        self.assertNotIn("private-key", output)

    def test_failure_replaces_stale_application_metrics_but_preserves_success_time(self):
        self.collect(now=100)
        for error in (collector.CollectorError("health_transport_failed"), TimeoutError("secret"),
                      ValueError("private response body")):
            with self.subTest(error=type(error).__name__):
                success, _ = self.collect(error=error, now=200)
                self.assertFalse(success)
                output = self.output.read_text()
                self.assertIn("sub2api_call_audit_collector_success 0\n", output)
                self.assertIn("sub2api_call_audit_collector_last_success_unixtime 100.0\n", output)
                self.assertNotIn("sub2api_call_audit_ready ", output)
                self.assertNotIn("sub2api_call_audit_dead_letter_files ", output)
                self.assertNotIn("secret", output)
                self.assertNotIn("private response", output)

    def test_first_failure_has_no_fabricated_success(self):
        self.assertFalse(self.collect(error=collector.CollectorError("docker_timeout"))[0])
        self.assertIn("sub2api_call_audit_collector_last_success_unixtime 0\n", self.output.read_text())

    def test_legacy_schema_does_not_fabricate_recovery_health(self):
        self.collect(snapshot(recovery=False))
        output = self.output.read_text()
        self.assertIn("sub2api_call_audit_recovery_metrics_available 0\n", output)
        for field in collector.RECOVERY_FIELDS:
            self.assertNotIn("sub2api_call_audit_" + field + " ", output)

    def test_partial_recovery_schema_is_rejected(self):
        data = snapshot()
        del data["oldest_dead_letter_seconds"]
        with self.assertRaisesRegex(collector.CollectorError, "partial_recovery_schema"):
            collector.validate_snapshot(data, 200)

    def test_missing_or_wrongly_typed_fields_are_rejected(self):
        for field in collector.BOOL_FIELDS + collector.RECOVERY_BOOL_FIELDS:
            for value in (0, 1, "true", None, []):
                with self.subTest(field=field, value=value):
                    data = snapshot()
                    data[field] = value
                    with self.assertRaises(collector.CollectorError):
                        collector.validate_snapshot(data, 200)
        for field in collector.COUNTER_FIELDS + collector.GAUGE_FIELDS + collector.RECOVERY_GAUGE_FIELDS + collector.RECOVERY_COUNTER_FIELDS:
            for value in (True, False, -1, 1.0, "1", None, float("nan"), 2**64):
                with self.subTest(field=field, value=value):
                    data = snapshot()
                    data[field] = value
                    with self.assertRaises(collector.CollectorError):
                        collector.validate_snapshot(data, 200)
        for field in collector.BOOL_FIELDS + collector.COUNTER_FIELDS + collector.GAUGE_FIELDS:
            data = snapshot()
            del data[field]
            with self.assertRaises(collector.CollectorError):
                collector.validate_snapshot(data, 200)

    def test_status_and_readiness_consistency(self):
        for data, code in ((snapshot(), 503), (snapshot(ready=False), 200)):
            with self.assertRaisesRegex(collector.CollectorError, "inconsistent_status"):
                collector.validate_snapshot(data, code)
        data = snapshot()
        data["schema_ready"] = False
        with self.assertRaisesRegex(collector.CollectorError, "inconsistent_readiness"):
            collector.validate_snapshot(data, 200)
        data = snapshot()
        data["disk_used_percent"] = 101
        with self.assertRaisesRegex(collector.CollectorError, "invalid_percentage"):
            collector.validate_snapshot(data, 200)

    def test_fetch_is_fixed_local_no_proxy_and_no_redirect(self):
        opener = mock.Mock()
        opener.open.return_value = Response(json.dumps(snapshot()).encode())
        with mock.patch.object(collector.urllib.request, "build_opener", return_value=opener) as build:
            collector.fetch_snapshot("private-key")
        handlers = build.call_args.args
        self.assertEqual(handlers[0].proxies, {})
        self.assertIsInstance(handlers[1], collector.NoRedirect)
        self.assertIsNone(handlers[1].redirect_request(None, None, 302, "", {}, "http://external.invalid"))
        request = opener.open.call_args.args[0]
        self.assertEqual(request.full_url, "http://172.17.0.1:18081/api/v1/admin/call-audit/health")
        self.assertEqual(request.get_header("X-api-key"), "private-key")
        self.assertEqual(opener.open.call_args.kwargs, {"timeout": 5})

    def test_fetch_rejects_bad_responses_and_timeout(self):
        cases = [
            Response(b"{}", status=201), Response(b"{}", content_type="text/html"),
            Response(b"x" * (collector.MAX_RESPONSE_BYTES + 1)), Response(b"not JSON secret"),
            Response(b'{"enabled":true,"enabled":false}'), Response(b"[]"),
            Response(b'{"ignored":NaN}'),
        ]
        for response in cases:
            with self.subTest(response=response):
                opener = mock.Mock()
                opener.open.return_value = response
                with mock.patch.object(collector.urllib.request, "build_opener", return_value=opener):
                    with self.assertRaises(collector.CollectorError):
                        collector.fetch_snapshot("private-key")
        for error in (TimeoutError("secret"), urllib.error.URLError("secret"),
                      urllib.error.HTTPError(collector.HEALTH_URL, 302, "secret", headers(), io.BytesIO())):
            opener = mock.Mock()
            opener.open.side_effect = error
            with mock.patch.object(collector.urllib.request, "build_opener", return_value=opener):
                with self.assertRaises(collector.CollectorError) as caught:
                    collector.fetch_snapshot("private-key")
                self.assertNotIn("secret", str(caught.exception))

    def test_docker_key_stays_in_memory(self):
        data = [{"Name": "/sub2api", "State": {"Running": True}, "Config": {"Env": ["ADMIN_API_KEY=private-key", "OTHER=secret"]}}]
        process = subprocess.CompletedProcess([], 0, json.dumps(data).encode(), b"")
        with mock.patch.object(collector.subprocess, "run", return_value=process) as run:
            self.assertEqual(collector.administrator_key(), "private-key")
        self.assertEqual(run.call_args.args[0], ["/usr/bin/docker", "--host", "unix:///var/run/docker.sock", "inspect", "--type", "container", "sub2api"])
        self.assertEqual(run.call_args.kwargs, {
            "capture_output": True, "timeout": 5, "check": False,
            "env": {"PATH": "/usr/bin:/bin", "HOME": "/nonexistent", "DOCKER_CONFIG": "/nonexistent", "LC_ALL": "C"},
        })

    def test_docker_invalid_key_or_schema_is_rejected(self):
        entries = [[], ["ADMIN_API_KEY="], ["ADMIN_API_KEY=a", "ADMIN_API_KEY=b"], ["ADMIN_API_KEY=a\nb"], [1]]
        for env in entries:
            data = [{"Name": "/sub2api", "State": {"Running": True}, "Config": {"Env": env}}]
            process = subprocess.CompletedProcess([], 0, json.dumps(data).encode(), b"secret")
            with mock.patch.object(collector.subprocess, "run", return_value=process):
                with self.assertRaises(collector.CollectorError):
                    collector.administrator_key()

    def test_logs_do_not_print_subprocess_or_http_error_data(self):
        process = subprocess.CompletedProcess([], 1, b"private-env", b"private-stderr")
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(collector.subprocess, "run", return_value=process), \
                contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(collector.main(["--output", str(self.output)]), 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(json.loads(stderr.getvalue()), {"collector_success": 0, "reason": "docker_inspection_failed"})
        for text in (stderr.getvalue(), self.output.read_text()):
            self.assertNotIn("private", text)

    def test_atomic_publication_and_permissions(self):
        self.output.write_text("old complete metrics\n")
        original_replace = os.replace

        def inspect_replace(source, destination):
            self.assertEqual(self.output.read_text(), "old complete metrics\n")
            self.assertEqual(Path(source).parent, self.output.parent)
            self.assertEqual(Path(source).suffix, ".tmp")
            self.assertEqual(Path(source).read_text(), "new complete metrics\n")
            original_replace(source, destination)

        with mock.patch.object(collector.os, "replace", side_effect=inspect_replace):
            collector.atomic_write(self.output, "new complete metrics\n")
        self.assertEqual(self.output.read_text(), "new complete metrics\n")
        self.assertEqual(stat.S_IMODE(self.output.stat().st_mode), 0o644)
        self.assertEqual(list(self.output.parent.iterdir()), [self.output])

    def test_failed_replace_is_reported_and_removes_temp_file(self):
        self.collect(now=100)
        old = self.output.read_bytes()
        with mock.patch.object(collector.os, "replace", side_effect=OSError("secret")):
            self.assertEqual(self.collect(now=200), (False, "metric_publication_failed"))
        self.assertEqual(self.output.read_bytes(), old)
        self.assertEqual(list(self.output.parent.iterdir()), [self.output])

    def test_old_success_rejects_symlink_duplicate_or_future_timestamp(self):
        target = self.output.parent / "secret"
        target.write_text("sub2api_call_audit_collector_last_success_unixtime 100\n")
        self.output.symlink_to(target)
        self.assertEqual(collector.previous_success(self.output), 0)
        self.output.unlink()
        self.output.write_text(target.read_text() * 2)
        self.assertEqual(collector.previous_success(self.output), 0)
        self.output.write_text("sub2api_call_audit_collector_last_success_unixtime 999999999999\n")
        self.assertEqual(collector.previous_success(self.output), 0)


if __name__ == "__main__":
    unittest.main()
