#!/usr/bin/env python3
"""Read-only CLI for the Sub2API Claude Relay-compatible audit-query HTTP API."""

from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from pathlib import Path
from typing import Any, BinaryIO, Dict, Iterable, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen


FILTER_FIELDS = (
    ("request_id", "requestId", "--request-id"),
    ("user_id", "userId", "--user-id"),
    ("user_username", "userUsername", "--user-username"),
    ("api_key_id", "apiKeyId", "--api-key-id"),
    ("api_key_name", "apiKeyName", "--api-key-name"),
    ("protocol", "protocol", "--protocol"),
    ("model", "model", "--model"),
    ("status", "status", "--status"),
    ("status_code", "statusCode", "--status-code"),
    ("capture_status", "captureStatus", "--capture-status"),
)


class ApiError(RuntimeError):
    """Represent an HTTP or response-shape error without exposing credentials."""


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def timeout_value(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def normalize_base_url(value: str) -> str:
    base_url = value.strip().rstrip("/")
    parsed = urlsplit(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ApiError(
            "AUDIT_QUERY_API_BASE_URL must be an absolute http(s) URL ending at the audit-query root"
        )
    if parsed.query or parsed.fragment:
        raise ApiError("AUDIT_QUERY_API_BASE_URL must not contain a query string or fragment")
    if parsed.path.rstrip("/").endswith("/v1/audit"):
        raise ApiError("AUDIT_QUERY_API_BASE_URL must not include /v1/audit")
    return base_url


class AuditQueryClient:
    def __init__(self, base_url: str, token: Optional[str], timeout: float) -> None:
        self.base_url = normalize_base_url(base_url)
        self.token = token
        self.timeout = timeout

    def _url(self, path: str, query: Optional[Dict[str, Any]] = None) -> str:
        url = f"{self.base_url}/{path.lstrip('/')}"
        if query:
            values = {key: value for key, value in query.items() if value is not None}
            if values:
                url = f"{url}?{urlencode(values, doseq=True)}"
        return url

    def open(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        authenticated: bool = True,
    ):
        headers = {"Accept": "application/json", "User-Agent": "query-audit-api-skill/1"}
        if authenticated:
            if not self.token:
                raise ApiError("AUDIT_QUERY_API_TOKEN is required for /v1/audit requests")
            headers["Authorization"] = f"Bearer {self.token}"

        data = None
        if body is not None:
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = Request(
            self._url(path, query),
            data=data,
            headers=headers,
            method=method,
        )
        try:
            return urlopen(request, timeout=self.timeout)
        except HTTPError as error:
            raw = error.read()
            request_id = error.headers.get("X-Request-Id")
            code = None
            message = None
            try:
                payload = json.loads(raw.decode("utf-8"))
                details = payload.get("error", {})
                code = details.get("code")
                message = details.get("message")
                request_id = details.get("requestId") or request_id
            except (UnicodeDecodeError, json.JSONDecodeError, AttributeError):
                pass
            suffix = ", ".join(
                part
                for part in (
                    f"code={code}" if code else None,
                    f"requestId={request_id}" if request_id else None,
                )
                if part
            )
            detail = message or error.reason or "request failed"
            raise ApiError(
                f"HTTP {error.code}: {detail}{f' ({suffix})' if suffix else ''}"
            ) from None
        except URLError as error:
            raise ApiError(f"connection failed: {error.reason}") from None

    def json(
        self,
        method: str,
        path: str,
        *,
        query: Optional[Dict[str, Any]] = None,
        body: Optional[Dict[str, Any]] = None,
        authenticated: bool = True,
    ) -> Dict[str, Any]:
        with self.open(
            method,
            path,
            query=query,
            body=body,
            authenticated=authenticated,
        ) as response:
            raw = response.read()
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ApiError(f"server returned invalid JSON: {error}") from None
        if not isinstance(payload, dict):
            raise ApiError("server returned a non-object JSON response")
        return payload


def add_filter_arguments(parser: argparse.ArgumentParser) -> None:
    for attribute, _api_name, flag in FILTER_FIELDS:
        kwargs: Dict[str, Any] = {"dest": attribute}
        if attribute == "status_code":
            kwargs["type"] = int
        parser.add_argument(flag, **kwargs)


def add_time_arguments(parser: argparse.ArgumentParser, required: bool = False) -> None:
    parser.add_argument("--from", dest="from_time", required=required, help="ISO-8601 start time")
    parser.add_argument("--to", dest="to_time", required=required, help="ISO-8601 end time")


def add_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", default="-", help="Output path, or - for stdout")
    parser.add_argument("--compact", action="store_true", help="Emit compact JSON")


def filters_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}
    for attribute, api_name, _flag in FILTER_FIELDS:
        value = getattr(args, attribute, None)
        if value is not None:
            filters[api_name] = value
    return filters


def unwrap_data(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("success") is not True or not isinstance(payload.get("data"), dict):
        raise ApiError("server returned an unexpected API envelope")
    return payload["data"]


def output_handle(path: str) -> tuple[BinaryIO, bool]:
    if path == "-":
        return sys.stdout.buffer, False
    destination = Path(path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    return destination.open("wb"), True


def emit_json(value: Any, path: str, compact: bool) -> None:
    handle, should_close = output_handle(path)
    try:
        if compact:
            data = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        else:
            data = json.dumps(value, ensure_ascii=False, indent=2)
        handle.write(data.encode("utf-8"))
        handle.write(b"\n")
    finally:
        if should_close:
            handle.close()


def list_calls(client: AuditQueryClient, args: argparse.Namespace) -> Dict[str, Any]:
    query = filters_from_args(args)
    if args.from_time:
        query["from"] = args.from_time
    if args.to_time:
        query["to"] = args.to_time
    if args.cursor:
        query["cursor"] = args.cursor

    if not args.all_pages:
        if args.limit:
            query["limit"] = args.limit
        return unwrap_data(client.json("GET", "/v1/audit/calls", query=query))

    calls = []
    pages = 0
    cursor = args.cursor
    has_more = False
    next_cursor = cursor
    while len(calls) < args.max_records:
        page_query = dict(query)
        page_query["limit"] = min(args.limit or 200, args.max_records - len(calls))
        if cursor:
            page_query["cursor"] = cursor
        else:
            page_query.pop("cursor", None)
        page = unwrap_data(client.json("GET", "/v1/audit/calls", query=page_query))
        page_calls = page.get("calls")
        if not isinstance(page_calls, list):
            raise ApiError("list response data.calls is not an array")
        calls.extend(page_calls)
        pages += 1
        has_more = page.get("hasMore") is True
        next_cursor = page.get("nextCursor")
        if not has_more:
            break
        if not next_cursor or next_cursor == cursor:
            raise ApiError("pagination stopped because nextCursor is missing or unchanged")
        cursor = str(next_cursor)

    return {
        "calls": calls,
        "hasMore": has_more,
        "nextCursor": next_cursor if has_more else None,
        "pages": pages,
        "truncatedByClient": has_more and len(calls) >= args.max_records,
    }


def export_records(client: AuditQueryClient, args: argparse.Namespace) -> Dict[str, Any]:
    body: Dict[str, Any] = {
        "from": args.from_time,
        "to": args.to_time,
        "filters": filters_from_args(args),
    }
    if args.cursor:
        body["cursor"] = args.cursor
    if args.limit:
        body["limit"] = args.limit
    if args.artifact_kind:
        body["artifactKinds"] = args.artifact_kind

    handle, should_close = output_handle(args.output)
    last_summary: Optional[Dict[str, Any]] = None
    record_count = 0
    try:
        with client.open("POST", "/v1/audit/exports/stream", body=body) as response:
            source: Iterable[bytes]
            if response.headers.get("Content-Encoding", "").lower() == "gzip":
                source = gzip.GzipFile(fileobj=response)
            else:
                source = response
            for raw_line in source:
                if not raw_line.strip():
                    continue
                try:
                    entry = json.loads(raw_line.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ApiError(f"export returned invalid NDJSON: {error}") from None
                handle.write(raw_line if raw_line.endswith(b"\n") else raw_line + b"\n")
                if entry.get("type") == "record":
                    record_count += 1
                if entry.get("type") == "summary":
                    last_summary = entry
    finally:
        if should_close:
            handle.close()

    if last_summary is None:
        raise ApiError("export ended without a summary record")
    print(
        "export summary: "
        f"records={record_count}, complete={last_summary.get('complete')}, "
        f"truncated={last_summary.get('truncated')}, "
        f"artifactFailures={last_summary.get('artifactFailures', 0)}, "
        f"nextCursor={last_summary.get('nextCursor')}",
        file=sys.stderr,
    )
    return last_summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        help="Audit-query root URL; defaults to AUDIT_QUERY_API_BASE_URL",
    )
    parser.add_argument(
        "--timeout",
        type=timeout_value,
        help="Socket timeout in seconds; defaults to AUDIT_QUERY_API_TIMEOUT_SECONDS or 1800",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("health", help="Check process liveness")
    subparsers.add_parser("ready", help="Check query service readiness")

    list_parser = subparsers.add_parser("list", help="List audit call metadata")
    add_time_arguments(list_parser)
    add_filter_arguments(list_parser)
    list_parser.add_argument("--cursor", help="Opaque pagination cursor")
    list_parser.add_argument("--limit", type=positive_int, help="Page size")
    list_parser.add_argument(
        "--all", dest="all_pages", action="store_true", help="Follow pagination automatically"
    )
    list_parser.add_argument(
        "--max-records",
        type=positive_int,
        default=1000,
        help="Client-side cap for --all (default: 1000)",
    )
    add_output_arguments(list_parser)

    call_parser = subparsers.add_parser("call", help="Get one call and artifact descriptors")
    call_parser.add_argument("request_id")
    add_output_arguments(call_parser)

    artifact_parser = subparsers.add_parser("artifact", help="Read one artifact by numeric ID")
    artifact_parser.add_argument("artifact_id")
    add_output_arguments(artifact_parser)

    export_parser = subparsers.add_parser("export", help="Export decompressed NDJSON")
    add_time_arguments(export_parser, required=True)
    add_filter_arguments(export_parser)
    export_parser.add_argument("--cursor", help="Opaque continuation cursor")
    export_parser.add_argument("--limit", type=positive_int)
    export_parser.add_argument(
        "--artifact-kind",
        action="append",
        choices=("client_request", "upstream_request", "response"),
        help="Artifact kind to include; repeat the flag for multiple kinds",
    )
    export_parser.add_argument("--output", default="-", help="NDJSON path, or - for stdout")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    base_url = args.base_url or os.environ.get("AUDIT_QUERY_API_BASE_URL", "")
    if not base_url:
        parser.error("set AUDIT_QUERY_API_BASE_URL or pass --base-url")

    timeout = args.timeout
    if timeout is None:
        try:
            timeout = timeout_value(os.environ.get("AUDIT_QUERY_API_TIMEOUT_SECONDS", "1800"))
        except (ValueError, argparse.ArgumentTypeError) as error:
            parser.error(f"invalid AUDIT_QUERY_API_TIMEOUT_SECONDS: {error}")

    token = os.environ.get("AUDIT_QUERY_API_TOKEN")
    client = AuditQueryClient(base_url, token, timeout)

    if args.command in {"health", "ready"}:
        payload = client.json("GET", f"/{args.command}z", authenticated=False)
        emit_json(payload, "-", False)
        return 0
    if not token:
        parser.error("set AUDIT_QUERY_API_TOKEN to the original Bearer token")

    if args.command == "list":
        emit_json(list_calls(client, args), args.output, args.compact)
        return 0
    if args.command == "call":
        payload = client.json("GET", f"/v1/audit/calls/{args.request_id}")
        emit_json(unwrap_data(payload), args.output, args.compact)
        return 0
    if args.command == "artifact":
        payload = client.json("GET", f"/v1/audit/artifacts/{args.artifact_id}")
        emit_json(unwrap_data(payload), args.output, args.compact)
        return 0
    if args.command == "export":
        summary = export_records(client, args)
        return 0 if summary.get("complete") is True else 3

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ApiError, OSError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(2) from None
