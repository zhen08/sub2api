#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSION_FILE="${REPO_ROOT}/backend/cmd/server/VERSION"

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

BASE_VERSION="${BASE_VERSION:-$(tr -d '\r\n' < "${VERSION_FILE}")}"
YMS_BUILD_SEQ="${YMS_BUILD_SEQ:-}"
YMS_BUILD_DATE="${YMS_BUILD_DATE:-$(date -u +%m%d)}"
IMAGE_REPOSITORY="${IMAGE_REPOSITORY:-zhen-yms/sub2api}"

[[ "${BASE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || \
    fail "BASE_VERSION must be a semantic version such as 0.1.171 (got: ${BASE_VERSION})"
[[ "${YMS_BUILD_DATE}" =~ ^[0-9]{4}$ ]] || \
    fail "YMS_BUILD_DATE must use MMDD format (got: ${YMS_BUILD_DATE})"
[[ "${YMS_BUILD_SEQ}" =~ ^[1-9][0-9]*$ ]] || \
    fail "YMS_BUILD_SEQ is required and must be a positive integer"

FULL_VERSION="${BASE_VERSION}-yms-${YMS_BUILD_DATE}-${YMS_BUILD_SEQ}"
IMAGE="${IMAGE_REPOSITORY}:${FULL_VERSION}"

printf 'VM101_VERSION=%s\n' "${FULL_VERSION}"
printf 'VM101_IMAGE=%s\n' "${IMAGE}"

if [[ "${VM101_BUILD_DRY_RUN:-false}" == "true" ]]; then
    exit 0
fi

COMMIT="${COMMIT:-$(git -C "${REPO_ROOT}" rev-parse HEAD)}"
BUILD_DATE="${BUILD_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"

docker build \
    --platform linux/amd64 \
    -t "${IMAGE}" \
    --build-arg "VERSION=${FULL_VERSION}" \
    --build-arg "COMMIT=${COMMIT}" \
    --build-arg "DATE=${BUILD_DATE}" \
    -f "${REPO_ROOT}/Dockerfile" \
    "${REPO_ROOT}"

"${SCRIPT_DIR}/verify_vm101_image_version.sh" "${IMAGE}" "${FULL_VERSION}"
