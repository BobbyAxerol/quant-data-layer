#!/usr/bin/env bash
# DL v2.4.0 runtime windows 1' / 2 / 1'': READ-ONLY gates for one recreated role.
# Usage: v240_window_gates.sh ROLE T0      ROLE = stream_kn_2 | query_kn_2 | query_kn_1 | stream_kn_1; T0 = UTC ISO
# Purpose: decide PASS/FAIL after the coordinator recreated ROLE (corrected order after the window 1 RCA,
# ROLLOUT_PLAN section 8). Gates: G-stream or G-query (role config), G-hot (Query -> Stream hot reads through
# each Query's own client, v240_hot_read_probe.py), G-query-refusals (no HOT_AUTHORITY_OR_PROTOCOL_REFUSED since
# T0), G-writer (TS writer slices, refusals, native probe routes). The stream subscriber count is informational
# only: the SDK keeps a failed-over subscription on the other replica (qdl_sdk transport.py 2.4.1:612-619).
# Boundary: docker inspect/logs/exec of read-only commands, Redis GET only (TS heartbeat). Mutates nothing.
# Exit 0 = all PASS, 1 = a gate FAILED (the coordinator runs the role's rollback block).
set -u -o pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
P=qdl_v2_stable_candidate; W=market_data_service_p184w; R=/home/bobby/.local/state/qdl-v2/releases
IMG_STREAM=sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98
IMG_QUERY=sha256:18ada571c72856cefc2844c8cb2c3809c3e30ac77927bc4c73160e7b2787cd18
BUNDLE=577470a63180bd8b7ac45e4beff7399bd8cf604fa3f3efdb8a855ea4f5191192
IMG_W=sha256:d1addc042756f6bef8a2648e2bed64a7384818f22b47ddea8f9f220f844474c0
role=${1:?role}; t0=${2:?T0}; F=0
g() { if [ "$2" = 1 ]; then echo "PASS $1: $3"; else echo "FAIL $1: $3"; F=1; fi; }
c=$P-$role-1
hbjson() { local id; id=$(docker inspect -f '{{.Id}}' $W | cut -c1-12); docker exec redis_service redis-cli get "service:heartbeat:market_data:$id:1"; }
mnt() { docker inspect $c --format '{{range .Mounts}}{{.Destination}}={{.Source}}:{{.RW}} {{end}}' | tr ' ' '\n'; }

# ---- role config
x=$(docker inspect -f '{{.Image}} {{.RestartCount}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' $c); set -- $x
want=$IMG_STREAM; case $role in query_kn_*) want=$IMG_QUERY;; esac
g image-rc-health $([ "$1" = $want ] && [ "$2" = 0 ] && [ "$3" = running ] && [ "${4:-}" = healthy ] && echo 1 || echo 0) "$x"
g realm-mount $(mnt | grep -qx "/consumer-realms=$R/v2.4.0-realm:false" && echo 1 || echo 0) "$(mnt | grep '^/consumer-realms=' | head -1)"
case $role in
stream_kn_*)
  g liveness-ro $(mnt | grep -q '^/session-liveness=.*:false$' && echo 1 || echo 0) "$(mnt | grep session-liveness)"
  ml=$(docker logs --since 1m $c 2>&1 | grep qdl_kn_gateway_metrics | tail -1)
  v=$(echo "$ml" | python3 -B -c 'import sys,json
try: d=json.loads(sys.stdin.read()); print(d["bundle_sha256"],d["hub_decode_failures"],d["hub_reader_errors"],d["refused"],d["hub_live_budget_refused"],d["replay_refused_capacity"],d["hub_subscribers"])
except Exception: print("NA")'); set -- $v
  g stream-bundle $([ "$1" = $BUNDLE ] && echo 1 || echo 0) "bundle_sha256=$1"
  g stream-hub $([ "${2:-x}" = 0 ] && [ "${3:-x}" = 0 ] && echo 1 || echo 0) "decode_failures=${2:-NA} reader_errors=${3:-NA}"
  g stream-refused $([ "${4:-x}" = 0 ] && [ "${5:-x}" = 0 ] && [ "${6:-x}" = 0 ] && echo 1 || echo 0) "refused=${4:-NA} live_budget=${5:-NA} replay_capacity=${6:-NA}"
  for s in stream_kn_1 stream_kn_2; do echo "INFO subscribers $s=$(docker logs --since 1m $P-$s-1 2>&1 | grep qdl_kn_gateway_metrics | tail -1 | python3 -B -c 'import sys,json;print(json.loads(sys.stdin.read())["hub_subscribers"])' 2>/dev/null)"; done;;
query_kn_*)
  envs=$(docker inspect $c --format '{{range .Config.Env}}{{println .}}{{end}}')
  ml=$(echo "$envs" | grep '^QDL_STABLE_CONSUMER_MANIFESTS=' | tr ':=' '\n\n')
  g query-manifests $(echo "$ml" | grep -qx /consumer-realms/alpha.okx.paper.stable-r17.yaml && echo "$ml" | grep -qx /consumer-realms/trading-system.paper.stable-r15.yaml && echo 1 || echo 0) "$(echo "$ml" | grep -E 'alpha.okx.paper|trading-system.paper' | tr '\n' ' ')"
  g query-hot-targets $(echo "$envs" | grep -qx 'QDL_KN_HOT_READ_TARGETS=qdl-v2-stream-b:8210,qdl-v2-stream-a:8210' && echo 1 || echo 0) "$(echo "$envs" | grep '^QDL_KN_HOT_READ_TARGETS=')"
  g query-runtime $(mnt | grep -qx "/runtime=$R/v2.3.1-stage-c/runtime:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime=')";;
esac

# ---- G-hot: each Query's own client (as configured) must read OK; per-target lines are diagnostic
for q in query_kn_1 query_kn_2; do
  out=$(timeout 60 docker exec -i $P-$q-1 python3 -B - < $HERE/v240_hot_read_probe.py 2>/dev/null)
  n_all=$(echo "$out" | grep -c '^ALL ')
  bad=$(echo "$out" | grep '^ALL ' | grep -vc ' OK$')
  g hot-read-$q $([ "$n_all" -ge 3 ] && [ "$bad" = 0 ] && echo 1 || echo 0) "configured client: $n_all reads, $bad not OK; $(echo "$out" | head -1)"
  echo "$out" | grep -v '^ALL \|^query catalog' | grep -v ' OK$' | sed "s/^/INFO $q per-target refusal: /"
done

# ---- G-query-refusals
for q in query_kn_1 query_kn_2; do
  n=$(docker logs --since $t0 $P-$q-1 2>&1 | grep '"qdl_refusal"' | grep -c HOT_AUTHORITY_OR_PROTOCOL_REFUSED)
  fb=$(docker logs --since $t0 $P-$q-1 2>&1 | grep -c '"qdl_hot_read_authority_fallback"')
  g query-refusals-$q $([ "$n" = 0 ] && echo 1 || echo 0) "HOT_AUTHORITY_OR_PROTOCOL_REFUSED since $t0 = $n (authority fallback log lines: $fb)"
done

# ---- G-writer: 246/246 within 120 s, or only the known quiet class; no HOT_AUTHORITY on native probe routes
ok=0; for i in $(seq 1 12); do
  v=$(hbjson | python3 -B -c 'import sys,json;d=json.load(sys.stdin)["details"]
other=sorted({x.get("reason_code") for x in d.get("not_ready",[])}-{"EXECUTION_CONTEXT_STALE","TRADE_EVENT_STALE"})
print(d["demanded_v2_slices"],d["ready_v2_slices"],d.get("execution_ready_v2_slices"),json.dumps(d.get("worker_retries"),separators=(",",":")),"|".join(other) or "-")'); set -- $v
  { [ "$1" = 246 ] && [ "$2" = 246 ]; } || [ "${5:-x}" = - ] && { ok=1; break; }; sleep 10; done
g writer-slices $ok "demanded/ready/exec=$1/$2/$3 other-reasons=${5:-NA} (246/246, or not-ready only EXECUTION_CONTEXT_STALE/TRADE_EVENT_STALE, within 120 s)"
g writer-retries $([ "${4:-x}" = "{}" ] && echo 1 || echo 0) "worker_retries=${4:-NA}"
since_s=$(( $(date -u +%s) - $(date -u -d "$t0" +%s) ))
hot=$(hbjson | python3 -B -c 'import sys,json;d=json.load(sys.stdin)["details"]
s='$since_s'; n=0
for w in (d.get("native_probe") or {}).values():
  for r in w.get("routes") or []:
    a=r.get("last_unavailable_age_seconds")
    if a is not None and a<=s and "HOT_AUTHORITY" in (r.get("last_unavailable_detail") or ""): n+=1
print(n)')
g writer-probe-authority $([ "$hot" = 0 ] && echo 1 || echo 0) "native probe routes with a HOT_AUTHORITY unavailable since T0 = $hot"
n=$(docker logs --since $t0 $W 2>&1 | grep -cE 'UNAUTHENTICATED|PERMISSION_DENIED|not bound to the active consumer manifest|quota is exhausted|CURSOR_EXPIRED|AUTHORITY_MISMATCH|RESUME_GENERATION_MISMATCH|RESTART_EXHAUSTED')
g writer-refusals $([ "$n" = 0 ] && echo 1 || echo 0) "count=$n since $t0"
g writer-untouched "$([ "$(docker inspect -f '{{.Image}} {{.RestartCount}}' $W)" = "$IMG_W 0" ] && echo 1 || echo 0)" "$(docker inspect -f '{{.Image}} rc={{.RestartCount}}' $W | cut -c1-19)"
for r in stream_kn_1 stream_kn_2 query_kn_1 query_kn_2 rust_core rust_core_2 rust_core_3 market_projector_1 market_projector_2 ingestor_binance_usdm ingestor_okx_swap binance_bar_edge; do
  [ $r = $role ] || echo "INFO $r $(docker inspect -f '{{.Image}} rc={{.RestartCount}} {{.State.StartedAt}}' $P-$r-1 | cut -c8-19,71-)"; done
[ $F = 0 ] && echo "WINDOW $role GATES PASS" || echo "WINDOW $role GATES FAIL"; exit $F
