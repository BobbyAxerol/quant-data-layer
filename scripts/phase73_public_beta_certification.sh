#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.phase7-beta.yml"
PROJECT="${QDL_BETA_PROJECT:-qdl_phase73_certification}"
QUERY_PORT="${QDL_BETA_QUERY_HOST_PORT:-18100}"
STREAM_A_HEALTH_PORT="${QDL_BETA_STREAM_A_HEALTH_PORT:-18101}"
STREAM_B_HEALTH_PORT="${QDL_BETA_STREAM_B_HEALTH_PORT:-18102}"
STREAM_A_GRPC_PORT="${QDL_BETA_STREAM_A_GRPC_PORT:-18110}"
STREAM_B_GRPC_PORT="${QDL_BETA_STREAM_B_GRPC_PORT:-18111}"
PROD_REDIS_CONTAINER="${QDL_V1_REDIS_CONTAINER:-redis_marketdata}"
CAPACITY_OUTPUT="${QDL_PHASE73_CAPACITY_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase7-capacity.json}"
SECURITY_OUTPUT="${QDL_PHASE73_SECURITY_OUTPUT:-${ROOT_DIR}/upgrade/evidence/phase7-security-adversarial.json}"

: "${QDL_BETA_IMAGE:?set QDL_BETA_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_REDIS_IMAGE:?set QDL_BETA_REDIS_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_INIT_IMAGE:?set QDL_BETA_INIT_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_CURSOR_KEYS_JSON:?set isolated beta cursor keys}"
: "${QDL_BETA_JWT_KEYS_JSON:?set two isolated beta JWT keys}"
: "${QDL_BETA_INTERNAL_INGEST_SECRET:?set an isolated 32-byte bridge secret}"

export QDL_BETA_IMAGE QDL_BETA_REDIS_IMAGE QDL_BETA_INIT_IMAGE
export QDL_BETA_CURSOR_KEYS_JSON QDL_BETA_JWT_KEYS_JSON
export QDL_BETA_INTERNAL_INGEST_SECRET
export QDL_BETA_JWT_ISSUER="${QDL_BETA_JWT_ISSUER:-https://identity.qdl.beta.invalid}"
export QDL_BETA_JWT_AUDIENCE="${QDL_BETA_JWT_AUDIENCE:-qdl-v2-beta}"
export QDL_BETA_QUERY_HOST_PORT="${QUERY_PORT}"
export QDL_BETA_STREAM_A_HEALTH_PORT="${STREAM_A_HEALTH_PORT}"
export QDL_BETA_STREAM_B_HEALTH_PORT="${STREAM_B_HEALTH_PORT}"
export QDL_BETA_STREAM_A_GRPC_PORT="${STREAM_A_GRPC_PORT}"
export QDL_BETA_STREAM_B_GRPC_PORT="${STREAM_B_GRPC_PORT}"

temporary="$(mktemp -d)"
CERT_UID="${QDL_CERT_UID:-$(id -u)}"
CERT_GID="${QDL_CERT_GID:-$(id -g)}"
cleanup() {
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
    --profile phase7-beta down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${temporary}"
}
trap cleanup EXIT
trap 'printf "phase73 certification failed line=%s command=%s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

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
  local output="$1"
  mapfile -t ids < <(docker ps -aq --filter label=com.docker.compose.project=data_layer | sort)
  if ((${#ids[@]} == 0)); then printf '[]\n' >"${output}"; return; fi
  docker inspect "${ids[@]}" | python3 "${ROOT_DIR}/scripts/phase73_topology_snapshot.py" >"${output}"
}

beta_keys_in_v1() {
  if ! docker inspect "${PROD_REDIS_CONTAINER}" >/dev/null 2>&1; then printf '0\n'; return; fi
  docker exec "${PROD_REDIS_CONTAINER}" redis-cli --scan --pattern 'qdl:beta:v2:*' | wc -l
}

component_revision() {
  curl -fsS --max-time 2 "$1" | python3 -c '
import json,sys
name=sys.argv[1]
for item in json.load(sys.stdin).get("components",[]):
    if item.get("name")==name:
        print(item.get("revision") or "")
        raise SystemExit(0)
raise SystemExit(1)' "$2"
}

select_active_gateway() {
  local attempts="${1:-30}" status_a status_b
  for ((index=1; index<=attempts; index++)); do
    status_a="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/ready" || true)"
    status_b="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/ready" || true)"
    if [[ "${status_a}:${status_b}" == "200:503" ]]; then
      active_service=qdl_stream_v2_beta_a
      active_health_port="${STREAM_A_HEALTH_PORT}"
      active_grpc_port="${STREAM_A_GRPC_PORT}"
      passive_health_port="${STREAM_B_HEALTH_PORT}"
      return 0
    fi
    if [[ "${status_a}:${status_b}" == "503:200" ]]; then
      active_service=qdl_stream_v2_beta_b
      active_health_port="${STREAM_B_HEALTH_PORT}"
      active_grpc_port="${STREAM_B_GRPC_PORT}"
      passive_health_port="${STREAM_A_HEALTH_PORT}"
      return 0
    fi
    sleep 1
  done
  printf 'expected one active stream gateway, got A=%s B=%s\n' "${status_a}" "${status_b}" >&2
  return 1
}

snapshot_v1 "${temporary}/v1-before.json"
keys_before="$(beta_keys_in_v1)"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta config --quiet
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta up -d
wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 200
wait_http "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/live" 200
wait_http "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/live" 200

select_active_gateway 30
epoch_before="$(component_revision "http://127.0.0.1:${active_health_port}/health/dependencies" gateway_lease)"

redis_container="$(docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta ps -q qdl_beta_redis)"
redis_before="$(docker exec "${redis_container}" redis-cli INFO memory | awk -F: '/^used_memory:/{gsub(/\r/,"",$2); print $2}')"
durable_volume="${PROJECT}_qdl_beta_durable_state"
store_before_kib="$(docker run --rm -v "${durable_volume}:/data:ro" "${QDL_BETA_INIT_IMAGE}" sh -c 'du -sk /data | cut -f1')"
store_before="$((store_before_kib * 1024))"

mapfile -t beta_ids < <(docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta ps -q | sort)
: >"${temporary}/stats.jsonl"
docker run --rm --network host --read-only --cap-drop ALL \
  --security-opt no-new-privileges:true --pids-limit 256 --memory 512m --cpus 1 \
  --user "${CERT_UID}:${CERT_GID}" --tmpfs "/tmp:rw,noexec,nosuid,nodev,size=32m,uid=${CERT_UID},gid=${CERT_GID}" \
  -v "${durable_volume}:/var/lib/qdl-beta-durable" \
  -v "${temporary}:/evidence" \
  "${QDL_BETA_IMAGE}" python /app/scripts/phase73_beta_certification.py \
    --source-bindings /app/config/phase7/canary-sources.yaml \
    --monitoring-manifest /app/consumers/beta/phase7-monitoring-binance.yaml \
    --paper-manifest /app/consumers/beta/phase7-paper-alpha-binance.yaml \
    --capacity-manifest /app/consumers/beta/phase7-capacity-binance.yaml \
    --spool-path /var/lib/qdl-beta-durable/canonical-shadow.sqlite3 \
    --output /evidence/core.json --v1-base-url http://127.0.0.1:8100 \
    --query-url "http://127.0.0.1:${QUERY_PORT}" --grpc-target "127.0.0.1:${active_grpc_port}" \
    --ingest-urls-json "[\"http://127.0.0.1:${STREAM_A_HEALTH_PORT}\",\"http://127.0.0.1:${STREAM_B_HEALTH_PORT}\"]" \
    --ingest-secret "${QDL_BETA_INTERNAL_INGEST_SECRET}" \
    --jwt-keys-json "${QDL_BETA_JWT_KEYS_JSON}" \
    --cursor-keys-json "${QDL_BETA_CURSOR_KEYS_JSON}" \
    --issuer "${QDL_BETA_JWT_ISSUER}" --audience "${QDL_BETA_JWT_AUDIENCE}" &
cert_pid=$!
while kill -0 "${cert_pid}" >/dev/null 2>&1; do
  docker stats --no-stream --format '{{json .}}' "${beta_ids[@]}" >>"${temporary}/stats.jsonl" || true
  sleep 0.5
done
wait "${cert_pid}"

redis_after="$(docker exec "${redis_container}" redis-cli INFO memory | awk -F: '/^used_memory:/{gsub(/\r/,"",$2); print $2}')"
store_after_kib="$(docker run --rm -v "${durable_volume}:/data:ro" "${QDL_BETA_INIT_IMAGE}" sh -c 'du -sk /data | cut -f1')"
store_after="$((store_after_kib * 1024))"

capacity_token="$(docker run --rm --network none -e QDL_BETA_JWT_KEYS_JSON -e QDL_BETA_JWT_ISSUER -e QDL_BETA_JWT_AUDIENCE "${QDL_BETA_IMAGE}" python /app/scripts/phase73_token.py)"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta stop qdl_beta_redis >/dev/null
wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 503 30
outage_ready=503
outage_query="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 3 -H "Authorization: Bearer ${capacity_token}" -H 'X-QDL-Consumer-ID: phase7-capacity-binance' 'http://127.0.0.1:'"${QUERY_PORT}"'/v2/instruments' || true)"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta start qdl_beta_redis >/dev/null
wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 200 30
recovery_ready=200

select_active_gateway 30
epoch_before="$(component_revision "http://127.0.0.1:${active_health_port}/health/dependencies" gateway_lease)"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta stop "${active_service}" >/dev/null
wait_http "http://127.0.0.1:${passive_health_port}/health/ready" 200 30
epoch_after="$(component_revision "http://127.0.0.1:${passive_health_port}/health/dependencies" gateway_lease)"
((epoch_after > epoch_before))
v1_fallback="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 'http://127.0.0.1:8100/v1/crypto/ohlcv/binance/BTCUSDT/1m?limit=2&market=usdm')"

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" --profile phase7-beta down -v --remove-orphans
snapshot_v1 "${temporary}/v1-after.json"
diff -u "${temporary}/v1-before.json" "${temporary}/v1-after.json"
keys_after="$(beta_keys_in_v1)"
[[ "${keys_before}" == "0" && "${keys_after}" == "0" ]]
containers_after="$(docker ps -aq --filter label=com.docker.compose.project="${PROJECT}" | wc -l)"
networks_after="$(docker network ls -q --filter label=com.docker.compose.project="${PROJECT}" | wc -l)"
volumes_after="$(docker volume ls -q --filter label=com.docker.compose.project="${PROJECT}" | wc -l)"

python3 "${ROOT_DIR}/scripts/phase73_runtime_evidence.py" \
  --output "${temporary}/runtime.json" --image "${QDL_BETA_IMAGE}" \
  --store-before "${store_before}" --store-after "${store_after}" \
  --redis-before "${redis_before}" --redis-after "${redis_after}" \
  --outage-query "${outage_query}" --outage-ready "${outage_ready}" \
  --recovery-ready "${recovery_ready}" --epoch-before "${epoch_before}" \
  --epoch-after "${epoch_after}" --v1-fallback "${v1_fallback}" \
  --keys-after "${keys_after}" --containers-after "${containers_after}" \
  --networks-after "${networks_after}" --volumes-after "${volumes_after}"
python3 "${ROOT_DIR}/scripts/phase73_finalize_evidence.py" \
  --core "${temporary}/core.json" --stats "${temporary}/stats.jsonl" \
  --runtime "${temporary}/runtime.json" --output "${CAPACITY_OUTPUT}" \
  --security-output "${SECURITY_OUTPUT}"
