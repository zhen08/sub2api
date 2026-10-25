#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
fixture_root=$(mktemp -d "${TMPDIR:-/tmp}/sub2api-caddy-policy.XXXXXX")
cleanup() {
  rm -rf "$fixture_root"
}
trap cleanup EXIT HUP INT TERM

mkdir "$fixture_root/deploy"
cp "$repo_root/deploy/test-caddyfile-cache.sh" "$fixture_root/deploy/"
fixture="$fixture_root/deploy/Caddyfile"
check="$fixture_root/deploy/test-caddyfile-cache.sh"

expect_rejected() {
  scenario=$1
  if sh "$check" > "$fixture_root/output" 2>&1; then
    printf 'Caddy policy unexpectedly accepted %s\n' "$scenario" >&2
    exit 1
  fi
  if ! grep -q 'must leave flush_interval unset' "$fixture_root/output"; then
    cat "$fixture_root/output" >&2
    printf 'Caddy policy rejected %s for the wrong reason\n' "$scenario" >&2
    exit 1
  fi
}

# The actual local sidecar exception must pass without changing deployment files.
cp "$repo_root/deploy/Caddyfile" "$fixture"
sh "$check"

# A preceding allowed sidecar must not leak its exception to the main proxy.
for interval in -1 100ms; do
  awk -v interval="$interval" '
    { print }
    /^[[:space:]]*reverse_proxy localhost:8080[[:space:]]*\{/ {
      print "\t\t\tflush_interval " interval
    }
  ' "$repo_root/deploy/Caddyfile" > "$fixture"
  expect_rejected "main inference proxy flush_interval $interval"
done

sed 's@handle_path /audit-query/\*@handle_path /other/*@' \
  "$repo_root/deploy/Caddyfile" > "$fixture"
expect_rejected 'loopback proxy under another route'

sed 's/127\.0\.0\.1:3100/127.0.0.1:3101/' \
  "$repo_root/deploy/Caddyfile" > "$fixture"
expect_rejected 'audit route with another upstream'

sed 's/flush_interval -1/flush_interval 100ms/' \
  "$repo_root/deploy/Caddyfile" > "$fixture"
expect_rejected 'buffered audit export interval'

printf 'Caddy flush policy regression tests passed (5 rejected configurations)\n'
