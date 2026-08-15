#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$(mktemp -d /tmp/qdl-phase3-rust.XXXXXX)"
PYTHON_IMAGE="${QDL_TEST_IMAGE:-data-layer:phase3-test}"
RUST_IMAGE="${QDL_RUST_IMAGE:-qdl-core:phase3-shadow}"

cleanup() {
  rm -rf "${STATE_DIR}"
}
trap cleanup EXIT
chmod 0777 "${STATE_DIR}"

docker run --rm -v "${ROOT_DIR}:/app:ro" -v "${STATE_DIR}:/state" -w /app \
  "${PYTHON_IMAGE}" python scripts/phase3_prepare_rust_smoke.py --state-dir /state

docker run --rm --entrypoint /usr/local/bin/qdl-binance-shadow \
  -v "${STATE_DIR}:/state" "${RUST_IMAGE}" /state/config.json

image_id="$(docker image inspect "${RUST_IMAGE}" --format '{{.Id}}')"
docker run --rm -v "${ROOT_DIR}:/app" -v "${STATE_DIR}:/state:ro" -w /app \
  "${PYTHON_IMAGE}" python scripts/phase3_verify_rust_wal.py \
  --state-dir /state \
  --output /app/upgrade/evidence/phase3-rust-binance-real-parity.json \
  --image-id "${image_id}"
