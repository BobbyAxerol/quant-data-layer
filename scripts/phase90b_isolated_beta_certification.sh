#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.phase7-beta.yml"
IMAGE_REF="${QDL_PHASE90B_IMAGE:?set QDL_PHASE90B_IMAGE}"
MATRIX_PROJECT="${QDL_PHASE90B_MATRIX_PROJECT:-qdl_phase90b_matrix}"
BRIDGE_PROJECT="${QDL_PHASE90B_BRIDGE_PROJECT:-qdl_phase90b_bridge}"
V1_CONTAINER="${QDL_V1_CONTAINER:-data_layer_service}"
PROD_REDIS_CONTAINER="${QDL_V1_REDIS_CONTAINER:-redis_marketdata}"
CAPACITY_OUTPUT="${QDL_PHASE90B_CAPACITY_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90b-capacity.json}"
SECURITY_OUTPUT="${QDL_PHASE90B_SECURITY_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90b-security-adversarial.json}"
PARITY_OUTPUT="${QDL_PHASE90B_PARITY_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90b-continuous-bridge.json}"
RESULT_OUTPUT="${QDL_PHASE90B_RESULT_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90b-isolated-v2-beta.json}"
REPORT_OUTPUT="${QDL_PHASE90B_REPORT_OUTPUT:-${ROOT_DIR}/upgrade/evidence/PHASE90B_ISOLATED_V2_BETA_REPORT.md}"
CHECKSUM_OUTPUT="${QDL_PHASE90B_CHECKSUM_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase90b-evidence.sha256}"
QUERY_PORT="${QDL_PHASE90B_QUERY_PORT:-18220}"
STREAM_A_HEALTH_PORT="${QDL_PHASE90B_STREAM_A_HEALTH_PORT:-18221}"
STREAM_B_HEALTH_PORT="${QDL_PHASE90B_STREAM_B_HEALTH_PORT:-18222}"
STREAM_A_GRPC_PORT="${QDL_PHASE90B_STREAM_A_GRPC_PORT:-18230}"
STREAM_B_GRPC_PORT="${QDL_PHASE90B_STREAM_B_GRPC_PORT:-18231}"

image_id="$(docker image inspect "${IMAGE_REF}" --format '{{.Id}}')"
redis_id="$(docker image inspect redis:7.2-alpine --format '{{.Id}}')"
random_hex() { python3 -c 'import secrets; print(secrets.token_hex(32))'; }
cursor_secret="$(random_hex)"
jwt_secret_1="$(random_hex)"
jwt_secret_2="$(random_hex)"
bridge_secret="$(random_hex)"
export QDL_BETA_IMAGE="${image_id}"
export QDL_BETA_REDIS_IMAGE="${redis_id}"
export QDL_BETA_INIT_IMAGE="${redis_id}"
export QDL_BETA_CURSOR_KEYS_JSON="{\"beta-k1\":\"${cursor_secret}\"}"
export QDL_BETA_CURSOR_ACTIVE_KEY_ID="beta-k1"
export QDL_BETA_JWT_KEYS_JSON="{\"beta-jwt-k1\":\"${jwt_secret_1}\",\"beta-jwt-k2\":\"${jwt_secret_2}\"}"
export QDL_BETA_INTERNAL_INGEST_SECRET="${bridge_secret}"
export QDL_BETA_JWT_ISSUER="https://identity.qdl.phase90b.invalid"
export QDL_BETA_JWT_AUDIENCE="qdl-v2-phase90b"
export QDL_BETA_SOURCE_BINDINGS="/app/config/phase7/canary-sources.yaml"
export QDL_BETA_CONSUMER_MANIFESTS="/app/consumers/beta/phase7-monitoring-binance.yaml:/app/consumers/beta/phase7-paper-alpha-binance.yaml:/app/consumers/beta/phase7-capacity-binance.yaml"
export QDL_BETA_AUTHORITY_REVISION="1"
export QDL_BETA_BRIDGE_RUN_ONCE="false"

temporary="$(mktemp -d)"
CERT_UID="${QDL_CERT_UID:-$(id -u)}"
CERT_GID="${QDL_CERT_GID:-$(id -g)}"
cleanup() {
  docker compose -p "${MATRIX_PROJECT}" -f "${COMPOSE_FILE}" \
    --profile phase7-beta down -v --remove-orphans >/dev/null 2>&1 || true
  docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" \
    --profile phase7-beta --profile phase7-canary \
    down -v --remove-orphans >/dev/null 2>&1 || true
  docker image rm "${IMAGE_REF}" >/dev/null 2>&1 || true
  rm -rf "${temporary}"
}
trap cleanup EXIT
trap 'printf "phase90b certification failed line=%s command=%s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

wait_http() {
  local url="$1" expected="$2" attempts="${3:-60}" code="000"
  for ((index=1; index<=attempts; index++)); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "${url}" || true)"
    [[ "${code}" == "${expected}" ]] && return 0
    sleep 1
  done
  printf 'timed out url=%s expected=%s actual=%s\n' "${url}" "${expected}" "${code}" >&2
  return 1
}

snapshot_v1() {
  docker inspect "${V1_CONTAINER}" | python3 "${ROOT_DIR}/scripts/phase73_topology_snapshot.py"
}

beta_keys_in_v1() {
  if ! docker inspect "${PROD_REDIS_CONTAINER}" >/dev/null 2>&1; then printf '0\n'; return; fi
  docker exec "${PROD_REDIS_CONTAINER}" redis-cli --scan --pattern 'qdl:beta:v2:*' | wc -l
}

query_beta() {
  local output="$1" token="$2"
  curl -sS --max-time 10 -o "${output}" -w '%{http_code}' \
    -H "Authorization: Bearer ${token}" \
    -H 'X-QDL-Consumer-ID: phase7-capacity-binance' \
    -H 'X-QDL-Purpose: INTERNAL_RESEARCH' \
    --get "http://127.0.0.1:${QUERY_PORT}/v2/market-data/a953e16e-7138-5562-b5e8-c337a44d0b65/warmup" \
    --data-urlencode 'feed=BAR' \
    --data-urlencode 'consumer_grade=RESEARCH' \
    --data-urlencode 'source_policy_id=alpha_crypto_primary_v1' \
    --data-urlencode 'interval=1m' \
    --data-urlencode 'limit=30' \
    --data-urlencode 'max_freshness_ms=180000' \
    --data-urlencode 'require_full_coverage=true' \
    --data-urlencode 'require_final_bars=true' \
    --data-urlencode 'stale_policy=BLOCK' \
    --data-urlencode 'gap_policy=BLOCK' \
    --data-urlencode 'recovery=SNAPSHOT_AND_REPLAY' \
    --data-urlencode 'bar_revision_policy=EMIT_REVISIONS'
}

snapshot_v1 >"${temporary}/v1-before.json"
openapi_before="$(curl -fsS --max-time 10 http://127.0.0.1:8100/openapi.json | sha256sum | cut -d' ' -f1)"
keys_before="$(beta_keys_in_v1)"

QDL_BETA_PROJECT="${MATRIX_PROJECT}" \
QDL_BETA_CONFIG_REVISION="phase90b-matrix-1" \
QDL_BETA_REDIS_PREFIX="qdl:beta:v2:paper:phase90b:matrix" \
QDL_BETA_CONSUMER_GROUP="qdl-v2-beta-phase90b-matrix" \
QDL_BETA_LEASE_SHARD_ID="stream-v2-phase90b-matrix" \
QDL_BETA_QUERY_HOST_PORT=18210 \
QDL_BETA_STREAM_A_HEALTH_PORT=18211 \
QDL_BETA_STREAM_B_HEALTH_PORT=18212 \
QDL_BETA_STREAM_A_GRPC_PORT=18213 \
QDL_BETA_STREAM_B_GRPC_PORT=18214 \
QDL_PHASE73_CAPACITY_OUTPUT="${CAPACITY_OUTPUT}" \
QDL_PHASE73_SECURITY_OUTPUT="${SECURITY_OUTPUT}" \
"${ROOT_DIR}/scripts/phase73_public_beta_certification.sh"

export QDL_BETA_CONFIG_REVISION="phase90b-continuous-1"
export QDL_BETA_REDIS_PREFIX="qdl:beta:v2:paper:phase90b:continuous"
export QDL_BETA_CONSUMER_GROUP="qdl-v2-beta-phase90b-continuous"
export QDL_BETA_LEASE_SHARD_ID="stream-v2-phase90b-continuous"
export QDL_BETA_QUERY_HOST_PORT="${QUERY_PORT}"
export QDL_BETA_STREAM_A_HEALTH_PORT="${STREAM_A_HEALTH_PORT}"
export QDL_BETA_STREAM_B_HEALTH_PORT="${STREAM_B_HEALTH_PORT}"
export QDL_BETA_STREAM_A_GRPC_PORT="${STREAM_A_GRPC_PORT}"
export QDL_BETA_STREAM_B_GRPC_PORT="${STREAM_B_GRPC_PORT}"

docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta --profile phase7-canary config --quiet
docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta --profile phase7-canary up -d
wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 200 60
wait_http "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/live" 200 30
wait_http "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/live" 200 30

status_a="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/ready" || true)"
status_b="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/ready" || true)"
if [[ "${status_a}:${status_b}" == "200:503" ]]; then
  active_grpc_port="${STREAM_A_GRPC_PORT}"
elif [[ "${status_a}:${status_b}" == "503:200" ]]; then
  active_grpc_port="${STREAM_B_GRPC_PORT}"
else
  printf 'expected exactly one active stream gateway, got A=%s B=%s\n' "${status_a}" "${status_b}" >&2
  exit 1
fi

bridge_id="$(docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta --profile phase7-canary ps -q qdl_beta_v1_bridge)"
[[ -n "${bridge_id}" ]]
docker inspect "${bridge_id}" >"${temporary}/bridge-inspect.json"
docker image inspect "${image_id}" >"${temporary}/image-inspect.json"

token="$(docker run --rm --network none \
  -e QDL_BETA_JWT_KEYS_JSON -e QDL_BETA_JWT_ISSUER -e QDL_BETA_JWT_AUDIENCE \
  "${image_id}" python /app/scripts/phase73_token.py)"
code="000"
for ((attempt=1; attempt<=60; attempt++)); do
  code="$(query_beta "${temporary}/v2-candidate.json" "${token}" || true)"
  if [[ "${code}" == "200" ]]; then
    count="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("count",0))' "${temporary}/v2-candidate.json")"
    [[ "${count}" -ge 30 ]] && break
  fi
  sleep 1
done
[[ "${code}" == "200" && "${count:-0}" -ge 30 ]]

previous_watermark=""
for ((attempt=1; attempt<=15; attempt++)); do
  sleep 2
  code="$(query_beta "${temporary}/v2-stable.json" "${token}" || true)"
  [[ "${code}" == "200" ]] || continue
  watermark="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["watermark_offset"])' "${temporary}/v2-stable.json")"
  if [[ -n "${previous_watermark}" && "${watermark}" == "${previous_watermark}" ]]; then
    cp "${temporary}/v2-stable.json" "${temporary}/v2-first.json"
    break
  fi
  previous_watermark="${watermark}"
done
[[ -f "${temporary}/v2-first.json" ]]
curl -fsS --max-time 10 'http://127.0.0.1:8100/v1/crypto/ohlcv/binance/BTCUSDT/1m?limit=120&market=usdm' >"${temporary}/v1-first.json"
sleep 12
[[ "$(query_beta "${temporary}/v2-second.json" "${token}")" == "200" ]]
curl -fsS --max-time 10 'http://127.0.0.1:8100/v1/crypto/ohlcv/binance/BTCUSDT/1m?limit=120&market=usdm' >"${temporary}/v1-second.json"

docker run --rm --network none --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --pids-limit 64 --memory 256m --cpus 0.5 \
  --user "${CERT_UID}:${CERT_GID}" --tmpfs "/tmp:rw,noexec,nosuid,nodev,size=16m,uid=${CERT_UID},gid=${CERT_GID}" \
  -v "${temporary}:/evidence" "${image_id}" \
  python /app/scripts/phase90b_bridge_parity.py \
  --source-bindings /app/config/phase7/canary-sources.yaml \
  --v1-first /evidence/v1-first.json --v2-first /evidence/v2-first.json \
  --v1-second /evidence/v1-second.json --v2-second /evidence/v2-second.json \
  --output /evidence/parity.json
cp "${temporary}/parity.json" "${PARITY_OUTPUT}"

mapfile -t beta_ids < <(docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta --profile phase7-canary ps -q)
: >"${temporary}/stats.jsonl"
docker stats --no-stream --format '{{json .}}' "${beta_ids[@]}" >>"${temporary}/stats.jsonl"

docker compose -p "${BRIDGE_PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta --profile phase7-canary down -v --remove-orphans
containers_after="$(docker ps -aq --filter label=com.docker.compose.project="${BRIDGE_PROJECT}" | wc -l)"
networks_after="$(docker network ls -q --filter label=com.docker.compose.project="${BRIDGE_PROJECT}" | wc -l)"
volumes_after="$(docker volume ls -q --filter label=com.docker.compose.project="${BRIDGE_PROJECT}" | wc -l)"
docker image rm "${IMAGE_REF}" >/dev/null
images_after="$(docker image ls -q "${IMAGE_REF}" | wc -l)"
snapshot_v1 >"${temporary}/v1-after.json"
openapi_after="$(curl -fsS --max-time 10 http://127.0.0.1:8100/openapi.json | sha256sum | cut -d' ' -f1)"
keys_after="$(beta_keys_in_v1)"

python3 "${ROOT_DIR}/scripts/phase90b_finalize_evidence.py" \
  --capacity "${CAPACITY_OUTPUT}" --security "${SECURITY_OUTPUT}" \
  --parity "${PARITY_OUTPUT}" --image-inspect "${temporary}/image-inspect.json" \
  --bridge-inspect "${temporary}/bridge-inspect.json" --stats "${temporary}/stats.jsonl" \
  --v1-before "${temporary}/v1-before.json" --v1-after "${temporary}/v1-after.json" \
  --openapi-before "${openapi_before}" --openapi-after "${openapi_after}" \
  --production-keys-before "${keys_before}" --production-keys-after "${keys_after}" \
  --containers-after "${containers_after}" --networks-after "${networks_after}" \
  --volumes-after "${volumes_after}" --images-after "${images_after}" \
  --output "${RESULT_OUTPUT}" --report "${REPORT_OUTPUT}"
capacity_rel="$(realpath --relative-to="${ROOT_DIR}" "${CAPACITY_OUTPUT}")"
security_rel="$(realpath --relative-to="${ROOT_DIR}" "${SECURITY_OUTPUT}")"
parity_rel="$(realpath --relative-to="${ROOT_DIR}" "${PARITY_OUTPUT}")"
result_rel="$(realpath --relative-to="${ROOT_DIR}" "${RESULT_OUTPUT}")"
report_rel="$(realpath --relative-to="${ROOT_DIR}" "${REPORT_OUTPUT}")"
(
  cd "${ROOT_DIR}"
  sha256sum "${capacity_rel}" "${security_rel}" "${parity_rel}" \
    "${result_rel}" "${report_rel}"
) >"${CHECKSUM_OUTPUT}"
(cd "${ROOT_DIR}" && sha256sum -c "${CHECKSUM_OUTPUT}")
