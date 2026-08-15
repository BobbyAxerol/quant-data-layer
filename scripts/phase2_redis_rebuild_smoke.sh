#!/usr/bin/env bash
set -euo pipefail

run_id="${QDL_PHASE2_RUN_ID:-$$}"
network="qdl_phase2_${run_id}"
redis_container="qdl_phase2_redis_${run_id}"
state_dir="$(mktemp -d /tmp/qdl-phase2-redis.XXXXXX)"
test_image="${QDL_TEST_IMAGE:-data-layer:v0.1.0}"

# The runtime image uses fixed UID 10001. This random, ephemeral directory holds
# test-only SQLite state and is removed by the EXIT trap.
chmod 0777 "${state_dir}"

cleanup() {
  docker rm -f "${redis_container}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  rm -rf "${state_dir}"
}
trap cleanup EXIT

docker network create "${network}" >/dev/null
docker run -d --name "${redis_container}" --network "${network}" \
  redis:7.2-alpine redis-server \
  --appendonly yes --appendfsync always --save "" \
  --maxmemory 64mb --maxmemory-policy noeviction >/dev/null

for _ in $(seq 1 30); do
  if docker exec "${redis_container}" redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.2
done
docker exec "${redis_container}" redis-cli ping | grep -q PONG

docker run --rm --network "${network}" \
  -e QDL_PHASE2_REDIS_URL="redis://${redis_container}:6379/15" \
  -v "$(pwd):/app" -w /app "${test_image}" \
  python -m unittest -v tests.test_fund_phase2_redis

docker run --rm --network "${network}" \
  -e QDL_PHASE2_REDIS_URL="redis://${redis_container}:6379/15" \
  -v "$(pwd):/app" -v "${state_dir}:/state" -w /app "${test_image}" \
  python scripts/phase2_redis_rebuild_probe.py seed --state-dir /state

docker restart "${redis_container}" >/dev/null
for _ in $(seq 1 30); do
  if docker exec "${redis_container}" redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.2
done

docker run --rm --network "${network}" \
  -e QDL_PHASE2_REDIS_URL="redis://${redis_container}:6379/15" \
  -v "$(pwd):/app" -v "${state_dir}:/state" -w /app "${test_image}" \
  python scripts/phase2_redis_rebuild_probe.py verify-rebuild --state-dir /state
