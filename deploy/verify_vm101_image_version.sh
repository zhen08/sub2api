#!/usr/bin/env bash
set -euo pipefail

fail() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 1
}

if [[ "$#" -lt 1 || "$#" -gt 2 ]]; then
    fail "usage: $0 IMAGE[:TAG] [EXPECTED_VERSION]"
fi

IMAGE="$1"
TAG_VERSION="${IMAGE##*:}"
EXPECTED_VERSION="${2:-${TAG_VERSION}}"

[[ "${TAG_VERSION}" != "${IMAGE}" && "${TAG_VERSION}" != */* ]] || \
    fail "image must include an explicit version tag (got: ${IMAGE})"
[[ "${EXPECTED_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+-yms-[0-9]{4}-[1-9][0-9]*$ ]] || \
    fail "expected version must match X.Y.Z-yms-MMDD-N (got: ${EXPECTED_VERSION})"
[[ "${TAG_VERSION}" == "${EXPECTED_VERSION}" ]] || \
    fail "image tag ${TAG_VERSION} does not match expected version ${EXPECTED_VERSION}"

ARCHITECTURE="$(docker image inspect --format '{{.Architecture}}' "${IMAGE}")"
[[ "${ARCHITECTURE}" == "amd64" ]] || \
    fail "image architecture must be amd64 for VM101 (got: ${ARCHITECTURE})"

VERSION_OUTPUT="$(docker run --rm --platform linux/amd64 --entrypoint /app/sub2api "${IMAGE}" --version 2>&1)"
EMBEDDED_VERSION="$(printf '%s\n' "${VERSION_OUTPUT}" | sed -nE 's/.*Sub2API ([^[:space:]]+) \(commit:.*/\1/p' | tail -n 1)"

[[ -n "${EMBEDDED_VERSION}" ]] || \
    fail "could not read the embedded version from ${IMAGE}: ${VERSION_OUTPUT}"
[[ "${EMBEDDED_VERSION}" == "${EXPECTED_VERSION}" ]] || \
    fail "embedded version ${EMBEDDED_VERSION} does not match image tag ${TAG_VERSION}"

printf 'Verified VM101 image: %s (version=%s, architecture=%s)\n' \
    "${IMAGE}" "${EMBEDDED_VERSION}" "${ARCHITECTURE}"
