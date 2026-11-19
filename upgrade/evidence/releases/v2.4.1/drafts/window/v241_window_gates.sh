#!/usr/bin/env bash
# DL v2.4.1 runtime windows (Query x2 overlay a, Query x2 overlay b, bar_edge): READ-ONLY gates for one recreated role.
# Usage: v241_window_gates.sh ROLE T0 PHASE   PHASE = a | b (Query) or bar (bar_edge). Derived from v240_window_gates.sh.
# Usage: v240_window_gates.sh ROLE T0      ROLE = stream_kn_2 | query_kn_2 | query_kn_1 | stream_kn_1 |
#                                                ingestor_binance_usdm | ingestor_okx_swap | binance_bar_edge; T0 = UTC ISO
# Windows 3/4 (stage C S10/S11 on v2.4.0): G-ingestor (started line, lanes flowing, 0 rejects, cores scope 0,
# session-liveness cadence >= 2.5 writes/s) and G-bar-edge (checkpoint crossing 14->16 / 22->24, heartbeat 16/24/0,
# running after 120 s; a "bounded bootstrap required" line is a HOLD, exit 3).
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
IMG_QUERY=sha256:929eeac2091ecbe916cf1f19fba9f13056e621a1c2c1947427b97a8a7c002082   # qdl-v2-python:2.4.1-985bd65
CANONICAL_SHA=$(python3 -c 'import json;print(json.load(open("/home/bobby/.local/state/qdl-v2/releases/v2.4.1-canonical/canonical-instrument-metadata.json"))["sha256"])')
BUNDLE=577470a63180bd8b7ac45e4beff7399bd8cf604fa3f3efdb8a855ea4f5191192
IMG_W=sha256:d1addc042756f6bef8a2648e2bed64a7384818f22b47ddea8f9f220f844474c0
IMG_INGESTOR=sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512   # unchanged in window 3
role=${1:?role}; t0=${2:?T0}; phase=${3:?phase a|b|bar}; F=0
g() { if [ "$2" = 1 ]; then echo "PASS $1: $3"; else echo "FAIL $1: $3"; F=1; fi; }
c=$P-$role-1
hbjson() { local id; id=$(docker inspect -f '{{.Id}}' $W | cut -c1-12); docker exec redis_service redis-cli get "service:heartbeat:market_data:$id:1"; }
mnt() { docker inspect $c --format '{{range .Mounts}}{{.Destination}}={{.Source}}:{{.RW}} {{end}}' | tr ' ' '\n'; }

# ---- role config
x=$(docker inspect -f '{{.Image}} {{.RestartCount}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' $c); set -- $x
want=$IMG_STREAM; case $role in query_kn_*|binance_bar_edge) want=$IMG_QUERY;; ingestor_*) want=$IMG_INGESTOR;; esac
g image-rc-health $([ "$1" = $want ] && [ "$2" = 0 ] && [ "$3" = running ] && [ "${4:-}" = healthy ] && echo 1 || echo 0) "$x"
case $role in stream_kn_*|query_kn_*)
g realm-mount $(mnt | grep -qx "/consumer-realms=$R/v2.4.0-realm:false" && echo 1 || echo 0) "$(mnt | grep '^/consumer-realms=' | head -1)";; esac
pyj() { python3 -B -c "import sys,json
try: d=json.loads(sys.stdin.read())
except Exception: print('NA'); sys.exit()
$1"; }
HOLD=0
case $role in
ingestor_*)
  cfg=${role}.json
  g ingestor-config $(mnt | grep -qx "/runtime/config.json=$R/v2.4.0-ingestor/$cfg:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime/config.json=')"
  st=$(docker logs --since $t0 $c 2>&1 | grep -m1 qdl_native_raw_ingestor_started)
  v=$(echo "$st" | pyj 'print(d["bindings"],d["lane_plan"])'); set -- $v
  wantb=118; [ $role = ingestor_okx_swap ] && wantb=465
  g ingestor-started $([ "$1" = $wantb ] && [ "${2:-}" = PINNED ] && echo 1 || echo 0) "bindings=$1 lane_plan=${2:-NA} (want $wantb PINNED)"
  p=$(docker logs --since 1m $c 2>&1 | grep qdl_native_raw_ingestor_progress | tail -1)
  if [ $role = ingestor_binance_usdm ]; then
    v=$(echo "$p" | pyj 'L={x["lane"]:x["frames"] for x in d["lanes"]};print(" ".join(f"{k}={L.get(k,-1)}" for k in ("binance-market-bar-001","binance-public-quote-001","binance-public-trade-001","binance-market-markindex-001","binance-public-book-001")))')
  else
    v=$(echo "$p" | pyj 'L=[(x["lane"],x["frames"]) for x in d["lanes"]];print(" ".join(f"{k}={n}" for k,n in L) if len(L)==7 else "lanes=%d" % len(L))')
  fi
  g ingestor-lanes-flowing "$(echo "$v" | grep -qE '=(-1|0)( |$)|NA|lanes=' && echo 0 || echo 1)" "frames per lane (10 s): $v"
  rj=$(docker logs --since $t0 $c 2>&1 | grep -ciE 'subscription.*reject|reject.*subscription|"event":"qdl_native_raw_ingestor_[a-z_]*(error|failed)"')
  g ingestor-rejects $([ "$rj" = 0 ] && echo 1 || echo 0) "rejected-subscription/error events=$rj"
  for cr in rust_core rust_core_2 rust_core_3; do
    v=$(docker logs --since 1m $P-$cr-1 2>&1 | grep qdl_realtime_core_progress | tail -1 | pyj 'print(d["scope_quarantines"],d["ignored_out_of_scope"])'); set -- $v
    g core-scope-$cr $([ "$1" = 0 ] && [ "${2:-}" = 0 ] && echo 1 || echo 0) "scope_q=$1 oos=${2:-NA}"; done
  venue=binance-usdm; [ $role = ingestor_okx_swap ] && venue=okx-swap
  cad=$(timeout 30 docker exec -i $P-query_kn_1-1 python3 -B - $venue < $HERE/v240_liveness_cadence.py 2>/dev/null)
  med=$(echo "$cad" | awk '{print $5}')
  g liveness-cadence $(awk "BEGIN{print (\"${med:-0}\"+0>=2.5)?1:0}") "$venue median writes/s=${med:-NA} (>= 2.5; 1000 ms baseline 1.17) $(echo "$cad" | cut -d' ' -f3)";;
binance_bar_edge)
  g bar-edge-runtime $(mnt | grep -qx "/runtime=$R/v2.3.1-stage-c/runtime:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime=')"
  cl=$(docker logs --since $t0 $c 2>&1 | grep -m1 'checkpoint crosses')
  g bar-edge-no-crossing $([ -z "$cl" ] && echo 1 || echo 0) "declaration unchanged (16/24): no crossing expected; got '${cl#*crosses }'"
  sc=$(docker exec $c sh -c 'ls /var/lib/qdl-kn/*.history-short.json 2>/dev/null; cat /var/lib/qdl-kn/*.history-short.json 2>/dev/null' | python3 -B -c 'import sys
t=sys.stdin.read().split("\n",1)
import json
try: d=json.loads(t[1]); print(t[0], len(d["proofs"]))
except Exception: print("absent 0")')
  echo "INFO bar_edge short-proof sidecar: $sc (the first v2.4.1 start re-proves and writes proofs; the next restart uses them)"
  cg=$(docker logs --since $t0 $c 2>&1 | grep -c 'cache generation changed; bounded bootstrap required')
  [ "$cg" != 0 ] && { echo "HOLD bar-edge: 'cache generation changed; bounded bootstrap required' x$cg - coordinator decides continue or rollback"; HOLD=1; }
  h=$(docker exec $c cat /var/lib/qdl-kn/heartbeat/bar-edge.json 2>/dev/null)
  v=$(echo "$h" | pyj 'r=d["declaration"]["revisions"];print(r["catalog"],r["acquisition"],d["declaration"]["image_files"])'); set -- $v
  g bar-edge-declaration $([ "$1" = 16 ] && [ "${2:-}" = 24 ] && [ "${3:-}" = 0 ] && echo 1 || echo 0) "catalog=$1 acquisition=${2:-NA} image_files=${3:-NA} (want 16/24/0; pre 14/22)"
  echo "INFO bar_edge heartbeat: $(echo "$h" | pyj 'print(d["detail"])')"
  x=$(docker inspect -f '{{.RestartCount}} {{.State.Status}}' $c); g bar-edge-running $([ "$x" = "0 running" ] && echo 1 || echo 0) "$x (run >= 120 s after T0)";;
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
  g query-runtime $(mnt | grep -qx "/runtime=$R/v2.3.1-stage-c/runtime:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime=')"
  g canonical-mount $(mnt | grep -qx "/canonical=$R/v2.4.1-canonical:false" && echo 1 || echo 0) "$(mnt | grep '^/canonical=')"
  cenv=$(echo "$envs" | grep '^QDL_CANONICAL_INSTRUMENT_METADATA=' | cut -d= -f2-)
  if [ "$phase" = a ]; then
    g canonical-disabled $([ -z "$cenv" ] && echo 1 || echo 0) "QDL_CANONICAL_INSTRUMENT_METADATA='$cenv' (readers first: empty)"
  else
    g canonical-enabled $([ "$cenv" = /canonical/canonical-instrument-metadata.json ] && echo 1 || echo 0) "QDL_CANONICAL_INSTRUMENT_METADATA='$cenv'"
    idx=$(timeout 60 docker exec $c python3 -B -c 'import os
from qdl.runtime.canonical_metadata import index_from_environment
i=index_from_environment(os.environ)
print(len(i), i.revision, i.sha256)' 2>&1 | tail -1)
    set -- $idx
    g canonical-index $([ "${1:-0}" = 527 ] && [ "${2:-}" = 1 ] && [ "${3:-}" = "$CANONICAL_SHA" ] && echo 1 || echo 0) "instruments=${1:-NA} revision=${2:-NA} sha256=${3:-NA} (want 527 / 1 / $CANONICAL_SHA)"
  fi
  boot=$(docker logs --since $t0 $c 2>&1 | grep -ciE 'CanonicalMetadataError|Traceback')
  g query-start-clean $([ "$boot" = 0 ] && echo 1 || echo 0) "CanonicalMetadataError/Traceback lines since T0 = $boot";;
esac

# ---- G-hot: each Query's own client (as configured) must read OK; per-target lines are diagnostic
for q in query_kn_1 query_kn_2; do
  out=$(timeout 60 docker exec -i $P-$q-1 python3 -B - < $HERE/v240_hot_read_probe.py 2>/dev/null)
  # Decides: no configured-client read may fail with an authority code (the RCA failure). A quiet product's
  # HOT_RECORD_MISSING or a transient (deadline/busy/unavailable, e.g. a fresh channel's first TLS read under host
  # load) is the pre-existing backup behaviour (Query then serves its primary cache) and is printed, not failed.
  n_all=$(echo "$out" | grep -c '^ALL ')
  n_ok=$(echo "$out" | grep '^ALL ' | grep -c ' OK$')
  auth=$(echo "$out" | grep '^ALL ' | grep -v ' OK$' | grep -cE 'HOT_AUTHORITY|UNAUTHENTICATED|PERMISSION_DENIED')
  g hot-read-$q $([ "$n_all" -ge 3 ] && [ "$n_ok" -ge 1 ] && [ "$auth" = 0 ] && echo 1 || echo 0) "configured client: $n_all reads, $n_ok OK, $auth authority/auth refusals; $(echo "$out" | head -1)"
  echo "$out" | grep '^ALL ' | grep -v ' OK$' | sed "s/^/INFO $q configured-client not OK: /"
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
[ $F = 0 ] && [ $HOLD = 1 ] && { echo "WINDOW $role HOLD"; exit 3; }
[ $F = 0 ] && echo "WINDOW $role GATES PASS" || echo "WINDOW $role GATES FAIL"; exit $F
