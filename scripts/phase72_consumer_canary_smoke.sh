#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.phase7-beta.yml"
PROJECT="${QDL_BETA_PROJECT:-qdl_phase72_canary}"
QUERY_PORT="${QDL_BETA_QUERY_HOST_PORT:-18100}"
STREAM_A_HEALTH_PORT="${QDL_BETA_STREAM_A_HEALTH_PORT:-18101}"
STREAM_B_HEALTH_PORT="${QDL_BETA_STREAM_B_HEALTH_PORT:-18102}"
STREAM_A_GRPC_PORT="${QDL_BETA_STREAM_A_GRPC_PORT:-18110}"
STREAM_B_GRPC_PORT="${QDL_BETA_STREAM_B_GRPC_PORT:-18111}"
PROD_REDIS_CONTAINER="${QDL_V1_REDIS_CONTAINER:-redis_marketdata}"
EVIDENCE_OUTPUT="${QDL_PHASE72_EVIDENCE_OUTPUT:-}"

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
chown 10001:10001 "${temporary}"
cleanup() {
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
    --profile phase7-beta down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${temporary}"
}
trap cleanup EXIT
trap 'printf "phase72 canary failed line=%s command=%s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

snapshot_v1() {
  local output="$1"
  mapfile -t ids < <(docker ps -aq --filter label=com.docker.compose.project=data_layer | sort)
  if ((${#ids[@]} == 0)); then
    printf '[]\n' >"${output}"
    return
  fi
  docker inspect "${ids[@]}" | python3 -c '
import json, sys
result = []
for item in json.load(sys.stdin):
    result.append({
        "Id": item["Id"],
        "Image": item["Image"],
        "Mounts": sorted([{
            "Destination": value.get("Destination"),
            "Mode": value.get("Mode"),
            "Name": value.get("Name"),
            "RW": value.get("RW"),
            "Source": value.get("Source"),
            "Type": value.get("Type"),
        } for value in item.get("Mounts", [])], key=lambda value: value["Destination"] or ""),
        "Name": item["Name"],
        "Networks": {
            name: {
                "EndpointID": value.get("EndpointID"),
                "IPAddress": value.get("IPAddress"),
                "NetworkID": value.get("NetworkID"),
            }
            for name, value in sorted(item["NetworkSettings"]["Networks"].items())
        },
        "RestartCount": item["RestartCount"],
    })
json.dump(sorted(result, key=lambda value: value["Id"]), sys.stdout,
          sort_keys=True, separators=(",", ":"))
print()
' >"${output}"
}

beta_keys_in_v1() {
  if ! docker inspect "${PROD_REDIS_CONTAINER}" >/dev/null 2>&1; then
    printf '0\n'
    return
  fi
  docker exec "${PROD_REDIS_CONTAINER}" redis-cli --scan \
    --pattern 'qdl:beta:v2:*' | wc -l
}

wait_http() {
  local url="$1" expected="$2" attempts="${3:-40}"
  local code="000"
  for ((index=1; index<=attempts; index++)); do
    code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 "${url}" || true)"
    if [[ "${code}" == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done
  printf 'timed out url=%s expected=%s actual=%s\n' "${url}" "${expected}" "${code}" >&2
  return 1
}

component_revision() {
  local url="$1" component="$2"
  curl -fsS --max-time 2 "${url}" | python3 -c '
import json, sys
name = sys.argv[1]
for item in json.load(sys.stdin).get("components", []):
    if item.get("name") == name:
        print(item.get("revision") or "")
        raise SystemExit(0)
raise SystemExit(1)
' "${component}"
}

run_canary() {
  local stage="$1" grpc_port="$2" output="$3" initial_result="${4:-}"
  local args=(
    "${stage}"
    --source-bindings /app/config/phase7/canary-sources.yaml
    --monitoring-manifest /app/consumers/beta/phase7-monitoring-binance.yaml
    --paper-manifest /app/consumers/beta/phase7-paper-alpha-binance.yaml
    --state-dir /evidence
    --output "/evidence/${output}"
    --v1-base-url http://127.0.0.1:8100
    --query-url "http://127.0.0.1:${QUERY_PORT}"
    --grpc-target "127.0.0.1:${grpc_port}"
    --ingest-urls-json "[\"http://127.0.0.1:${STREAM_A_HEALTH_PORT}\",\"http://127.0.0.1:${STREAM_B_HEALTH_PORT}\"]"
    --ingest-secret "${QDL_BETA_INTERNAL_INGEST_SECRET}"
    --jwt-keys-json "${QDL_BETA_JWT_KEYS_JSON}"
    --issuer "${QDL_BETA_JWT_ISSUER}"
    --audience "${QDL_BETA_JWT_AUDIENCE}"
  )
  if [[ -n "${initial_result}" ]]; then
    args+=(--initial-result "/evidence/${initial_result}")
  fi
  docker run --rm --network host --read-only --cap-drop ALL \
    --security-opt no-new-privileges:true --pids-limit 128 \
    --memory 384m --cpus 0.75 --user 10001:10001 \
    --tmpfs /tmp:rw,noexec,nosuid,nodev,size=16m,uid=10001,gid=10001 \
    -v "${temporary}:/evidence" \
    "${QDL_BETA_IMAGE}" python /app/scripts/phase72_consumer_canary.py "${args[@]}"
}

snapshot_v1 "${temporary}/v1-before.json"
keys_before="$(beta_keys_in_v1)"

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta config --quiet
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta up -d

wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 200 60
wait_http "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/live" 200 30
wait_http "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/live" 200 30

status_a="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
  "http://127.0.0.1:${STREAM_A_HEALTH_PORT}/health/ready" || true)"
status_b="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
  "http://127.0.0.1:${STREAM_B_HEALTH_PORT}/health/ready" || true)"
if [[ "${status_a}:${status_b}" == "200:503" ]]; then
  active_service="qdl_stream_v2_beta_a"
  active_health_port="${STREAM_A_HEALTH_PORT}"
  active_grpc_port="${STREAM_A_GRPC_PORT}"
  passive_health_port="${STREAM_B_HEALTH_PORT}"
  passive_grpc_port="${STREAM_B_GRPC_PORT}"
elif [[ "${status_a}:${status_b}" == "503:200" ]]; then
  active_service="qdl_stream_v2_beta_b"
  active_health_port="${STREAM_B_HEALTH_PORT}"
  active_grpc_port="${STREAM_B_GRPC_PORT}"
  passive_health_port="${STREAM_A_HEALTH_PORT}"
  passive_grpc_port="${STREAM_A_GRPC_PORT}"
else
  printf 'expected exactly one active beta stream, got A=%s B=%s\n' \
    "${status_a}" "${status_b}" >&2
  exit 1
fi

epoch_before="$(component_revision \
  "http://127.0.0.1:${active_health_port}/health/dependencies" gateway_lease)"
run_canary initial "${active_grpc_port}" initial.json

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta stop "${active_service}"
wait_http "http://127.0.0.1:${passive_health_port}/health/ready" 200 30
epoch_after="$(component_revision \
  "http://127.0.0.1:${passive_health_port}/health/dependencies" gateway_lease)"
((epoch_after > epoch_before))

run_canary post-failover "${passive_grpc_port}" failover.json initial.json

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta stop qdl_query_v2_beta
v1_fallback_code="$(curl -sS -o "${temporary}/v1-fallback.json" -w '%{http_code}' \
  --max-time 10 'http://127.0.0.1:8100/v1/crypto/ohlcv/binance/BTCUSDT/1m?limit=2&market=usdm')"
[[ "${v1_fallback_code}" == "200" ]]

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta down -v --remove-orphans
snapshot_v1 "${temporary}/v1-after.json"
keys_after="$(beta_keys_in_v1)"
diff -u "${temporary}/v1-before.json" "${temporary}/v1-after.json"
[[ "${keys_before}" == "0" && "${keys_after}" == "0" ]]

python3 - "${temporary}" "${epoch_before}" "${epoch_after}" \
  >"${temporary}/topology-result.json" <<'PY'
import json, pathlib, sys
root = pathlib.Path(sys.argv[1])
initial = json.loads((root / "initial.json").read_text())
failover = json.loads((root / "failover.json").read_text())
result = {
    "schema": "qdl.phase7.2.topology-canary.v1",
    "decision": "PASS",
    "authority": "V1_SHADOW_READ_ONLY",
    "real_provider_data": True,
    "monitoring_then_paper": True,
    "execution_dependency": initial["execution_dependency"],
    "v1_v2_mismatches": initial["v1_v2_mismatches"],
    "paper_restart_state_mismatch": failover["state_mismatch"],
    "gateway_epoch_before": int(sys.argv[2]),
    "gateway_epoch_after": int(sys.argv[3]),
    "v1_fallback_status": 200,
    "v1_topology_unchanged": True,
    "production_mutations": 0,
    "beta_keys_in_v1_after": 0,
}
print(json.dumps(result, sort_keys=True))
PY
cat "${temporary}/topology-result.json"
if [[ -n "${EVIDENCE_OUTPUT}" ]]; then
  mkdir -p "$(dirname "${EVIDENCE_OUTPUT}")"
  cp "${temporary}/topology-result.json" "${EVIDENCE_OUTPUT}"
fi
