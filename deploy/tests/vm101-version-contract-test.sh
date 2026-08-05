#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_SCRIPT="${SCRIPT_DIR}/../build_vm101_image.sh"

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    exit 1
}

OUTPUT="$({
    BASE_VERSION=0.1.171 \
    YMS_BUILD_DATE=0805 \
    YMS_BUILD_SEQ=2 \
    IMAGE_REPOSITORY=zhen-yms/sub2api \
    VM101_BUILD_DRY_RUN=true \
        "${BUILD_SCRIPT}"
})"

printf '%s\n' "${OUTPUT}" | grep -Fxq 'VM101_VERSION=0.1.171-yms-0805-2' || \
    fail "the generated embedded version is not deterministic"
printf '%s\n' "${OUTPUT}" | grep -Fxq 'VM101_IMAGE=zhen-yms/sub2api:0.1.171-yms-0805-2' || \
    fail "the image tag and embedded version do not share one version source"

if BASE_VERSION=0.1.171 YMS_BUILD_DATE=0805 YMS_BUILD_SEQ=0 \
    VM101_BUILD_DRY_RUN=true "${BUILD_SCRIPT}" >/dev/null 2>&1; then
    fail "zero must not be accepted as a build sequence"
fi

if BASE_VERSION=0.1.171 YMS_BUILD_DATE=20260805 YMS_BUILD_SEQ=1 \
    VM101_BUILD_DRY_RUN=true "${BUILD_SCRIPT}" >/dev/null 2>&1; then
    fail "non-MMDD build dates must be rejected"
fi

printf 'PASS: VM101 image tag and embedded version use one validated source\n'
