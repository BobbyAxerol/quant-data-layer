#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.phase7-beta.yml"
PROJECT="${QDL_BETA_PROJECT:-qdl_phase71_beta}"
QUERY_PORT="${QDL_BETA_QUERY_HOST_PORT:-18100}"
STREAM_A_PORT="${QDL_BETA_STREAM_A_HEALTH_PORT:-18101}"
STREAM_B_PORT="${QDL_BETA_STREAM_B_HEALTH_PORT:-18102}"
PROD_REDIS_CONTAINER="${QDL_V1_REDIS_CONTAINER:-redis_marketdata}"

: "${QDL_BETA_IMAGE:?set QDL_BETA_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_REDIS_IMAGE:?set QDL_BETA_REDIS_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_INIT_IMAGE:?set QDL_BETA_INIT_IMAGE to an immutable image ID/digest}"
: "${QDL_BETA_CURSOR_KEYS_JSON:?set isolated beta cursor keys}"
: "${QDL_BETA_JWT_KEYS_JSON:?set isolated beta JWT keys}"

export QDL_BETA_IMAGE QDL_BETA_REDIS_IMAGE QDL_BETA_INIT_IMAGE
export QDL_BETA_CURSOR_KEYS_JSON QDL_BETA_JWT_KEYS_JSON
export QDL_BETA_JWT_ISSUER="${QDL_BETA_JWT_ISSUER:-https://identity.qdl.beta.invalid}"
export QDL_BETA_JWT_AUDIENCE="${QDL_BETA_JWT_AUDIENCE:-qdl-v2-beta}"
export QDL_BETA_QUERY_HOST_PORT="${QUERY_PORT}"
export QDL_BETA_STREAM_A_HEALTH_PORT="${STREAM_A_PORT}"
export QDL_BETA_STREAM_B_HEALTH_PORT="${STREAM_B_PORT}"

temporary="$(mktemp -d)"
cleanup() {
  docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
    --profile phase7-beta down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "${temporary}"
}
trap cleanup EXIT
trap 'printf "phase71 topology failed line=%s command=%s\n" "${LINENO}" "${BASH_COMMAND}" >&2' ERR

snapshot_v1() {
  local output="$1"
  mapfile -t ids < <(docker ps -aq --filter label=com.docker.compose.project=data_layer | sort)
  if ((${#ids[@]} == 0)); then
    printf '[]\n' >"${output}"
    return
  fi
  docker inspect "${ids[@]}" | python3 -c '
import json, sys
containers = []
for item in json.load(sys.stdin):
    networks = {}
    for name, value in item["NetworkSettings"]["Networks"].items():
        networks[name] = {
            "Aliases": sorted(value.get("Aliases") or []),
            "DNSNames": sorted(value.get("DNSNames") or []),
            "EndpointID": value.get("EndpointID"),
            "Gateway": value.get("Gateway"),
            "IPAddress": value.get("IPAddress"),
            "MacAddress": value.get("MacAddress"),
            "NetworkID": value.get("NetworkID"),
        }
    mounts = [{
        "Destination": value.get("Destination"),
        "Mode": value.get("Mode"),
        "Name": value.get("Name"),
        "RW": value.get("RW"),
        "Source": value.get("Source"),
        "Type": value.get("Type"),
    } for value in item.get("Mounts", [])]
    containers.append({
        "Id": item["Id"],
        "Image": item["Image"],
        "Mounts": sorted(mounts, key=lambda value: value["Destination"] or ""),
        "Name": item["Name"],
        "Networks": networks,
        "RestartCount": item["RestartCount"],
    })
json.dump(sorted(containers, key=lambda value: value["Id"]), sys.stdout,
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
payload = json.load(sys.stdin)
for item in payload.get("components", []):
    if item.get("name") == name:
        print(item.get("revision") or "")
        raise SystemExit(0)
raise SystemExit(1)
' "${component}"
}

snapshot_v1 "${temporary}/v1-before.txt"
keys_before="$(beta_keys_in_v1)"

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta config --quiet
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta up -d

wait_http "http://127.0.0.1:${QUERY_PORT}/health/ready" 200 60
wait_http "http://127.0.0.1:${QUERY_PORT}/v2/instruments" 401 10
wait_http "http://127.0.0.1:${STREAM_A_PORT}/health/live" 200 30
wait_http "http://127.0.0.1:${STREAM_B_PORT}/health/live" 200 30

token="$(docker run --rm --network none \
  -e QDL_BETA_JWT_KEYS_JSON \
  -e QDL_BETA_JWT_ISSUER \
  -e QDL_BETA_JWT_AUDIENCE \
  "${QDL_BETA_IMAGE}" python -c '
import json, os, time, uuid
import jwt
keys = json.loads(os.environ["QDL_BETA_JWT_KEYS_JSON"])
kid, secret = next(iter(keys.items()))
now = int(time.time())
print(jwt.encode({
    "sub": "spiffe://qdl/paper/alpha-okx-reference-shadow",
    "iss": os.environ["QDL_BETA_JWT_ISSUER"],
    "aud": os.environ["QDL_BETA_JWT_AUDIENCE"],
    "iat": now,
    "exp": now + 300,
    "jti": str(uuid.uuid4()),
    "environment": "paper",
    "roles": ["market_data_reader"],
    "consumer_manifest_revision": 1,
}, secret, algorithm="HS256", headers={"kid": kid}))
')"
auth_code="$(curl -sS -o "${temporary}/instruments.json" -w '%{http_code}' \
  --max-time 3 \
  -H "Authorization: Bearer ${token}" \
  -H 'X-QDL-Consumer-ID: alpha.okx.reference.shadow' \
  "http://127.0.0.1:${QUERY_PORT}/v2/instruments")"
if [[ "${auth_code}" != "200" ]]; then
  printf 'authenticated query failed status=%s response=' "${auth_code}" >&2
  cat "${temporary}/instruments.json" >&2
  printf '\n' >&2
fi
[[ "${auth_code}" == "200" ]]

status_a="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
  "http://127.0.0.1:${STREAM_A_PORT}/health/ready" || true)"
status_b="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 2 \
  "http://127.0.0.1:${STREAM_B_PORT}/health/ready" || true)"
if [[ "${status_a}:${status_b}" == "200:503" ]]; then
  active_service="qdl_stream_v2_beta_a"
  active_port="${STREAM_A_PORT}"
  passive_port="${STREAM_B_PORT}"
elif [[ "${status_a}:${status_b}" == "503:200" ]]; then
  active_service="qdl_stream_v2_beta_b"
  active_port="${STREAM_B_PORT}"
  passive_port="${STREAM_A_PORT}"
else
  printf 'expected exactly one active stream gateway, got A=%s B=%s\n' \
    "${status_a}" "${status_b}" >&2
  exit 1
fi

first_epoch="$(component_revision \
  "http://127.0.0.1:${active_port}/health/dependencies" gateway_lease)"
docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta stop "${active_service}"
wait_http "http://127.0.0.1:${passive_port}/health/ready" 200 30
second_epoch="$(component_revision \
  "http://127.0.0.1:${passive_port}/health/dependencies" gateway_lease)"
[[ "${second_epoch}" =~ ^[0-9]+$ ]]
[[ "${first_epoch}" =~ ^[0-9]+$ ]]
((second_epoch > first_epoch))

docker compose -p "${PROJECT}" -f "${COMPOSE_FILE}" \
  --profile phase7-beta down -v --remove-orphans
snapshot_v1 "${temporary}/v1-after.txt"
keys_after="$(beta_keys_in_v1)"
diff -u "${temporary}/v1-before.txt" "${temporary}/v1-after.txt"
[[ "${keys_before}" == "${keys_after}" ]]
[[ "${keys_after}" == "0" ]]

printf '{"authenticated_query_status":200,"beta_keys_in_v1":0,"failover_epoch_after":%s,"failover_epoch_before":%s,"v1_topology_unchanged":true}\n' \
  "${second_epoch}" "${first_epoch}"
