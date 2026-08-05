#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)
cd "$repo_root"

fail() {
  printf 'docker target architecture test failed: %s\n' "$1" >&2
  exit 1
}

cross_compile_line='CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \'
cross_compile_count=$(grep -Fc "$cross_compile_line" Dockerfile || true)

[ "$cross_compile_count" -eq 2 ] || \
  fail "Dockerfile must cross-compile both sub2api and call-audit-migrate for TARGETARCH"

printf 'docker target architecture test passed\n'
