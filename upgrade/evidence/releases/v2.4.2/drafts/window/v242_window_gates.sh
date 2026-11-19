#!/usr/bin/env bash
# DL v2.4.2 r10 runtime windows: READ-ONLY gates for one recreated role. Derived from v241_window_gates.sh.
# Usage: v242_window_gates.sh ROLE T0      ROLE = stream_kn_2 | query_kn_2 | query_kn_1 | stream_kn_1 |
#        market_projector_1 | market_projector_2 | binance_bar_edge | ingestor_binance_usdm | ingestor_okx_swap
# Gates: role config (image, rc, health, mounts on releases/v2.4.2-r10 and releases/v2.4.2-realm), per role:
#   stream: r10 realm bundle sha, hub/refusal counters; query: manifests paper r18 / live r2 / sandbox r2 / TS r15,
#   canonical index, start clean, RVN typed refusal (v242_rvn_refusal_probe.py); projector: r10 kn bundle sha,
#   status errors; bar_edge: crossing 16->17 / 24->25 retained 1319 dropped 1 with the record, heartbeat 1319 bindings
#   17/25, instrument_unavailable=0; ingestor (optional W6): bindings, subscriptions_rejected=0, lanes, cores scope,
#   liveness cadence. Then G-hot, G-query-refusals and G-writer (246/246) as in v2.4.1.
# Boundary: docker inspect/logs/exec of read-only commands, Redis GET only (TS heartbeat). Mutates nothing.
# Fix 2026-11-19 (W7 deviation): the ingestor-rejects count excludes qdl_native_raw_ingestor_progress lines. From the
# v2.4.2 ingestor the progress line carries `subscriptions_rejected` (0), which the old `subscription.*reject` regex
# counted (12 false positives on ingestor_binance_usdm). Real rejections are gated by ingestor-subscriptions-rejected
# (field sum) and ingestor-rejection-events. As-run copy: v242_window_gates.as-installed.sh (sha-pinned install).
# Exit 0 = all PASS, 1 = a gate FAILED (run the role's rollback block), 3 = HOLD (coordinator decides).
set -u -o pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
P=qdl_v2_stable_candidate; W=market_data_service_p184w; R=/home/bobby/.local/state/qdl-v2/releases
IMG_STREAM=sha256:abafab2c98cbdd9d2fe86f2b26fd6fcf64800ccdd114a3dd5b92803de16a3e98
IMG_QUERY=sha256:5ea7a7a40ef2d1df920d0f7cf94f22431d53087f8002929e7dffdb4208f7dcc4   # qdl-v2-python:2.4.2-48137eb
IMG_PROJECTOR=sha256:58bdd28216139f0c1a40abeee3077834f1cf922cf6412e7fb1ce8845591a0512  # unchanged (bundle only)
KN_BUNDLE=1312c3bbf266478de842ab294c664a1ae8f03b1b0cce2a43dbc594aac37146b9             # r10 kn bundle field
KNDIR=$R/v2.4.2-r10; REALM=$R/v2.4.2-realm
CANONICAL_SHA=$(python3 -c 'import json;print(json.load(open("/home/bobby/.local/state/qdl-v2/releases/v2.4.1-canonical/canonical-instrument-metadata.json"))["sha256"])')
BUNDLE=016e68b4ed981bac7c83766f86d20b5af93f18e6f89e1ef7c13dd59e84749a6e   # r10 realm bundle field
IMG_W=sha256:d1addc042756f6bef8a2648e2bed64a7384818f22b47ddea8f9f220f844474c0
IMG_INGESTOR=sha256:e628fbe07afe94e8f716692633e77217a16223ea03abc7ea34edb959a48c72a7   # qdl-v2-rust:2.4.2-48137eb (W6)
role=${1:?role}; t0=${2:?T0}; F=0
g() { if [ "$2" = 1 ]; then echo "PASS $1: $3"; else echo "FAIL $1: $3"; F=1; fi; }
c=$P-$role-1
hbjson() { local id; id=$(docker inspect -f '{{.Id}}' $W | cut -c1-12); docker exec redis_service redis-cli get "service:heartbeat:market_data:$id:1"; }
mnt() { docker inspect $c --format '{{range .Mounts}}{{.Destination}}={{.Source}}:{{.RW}} {{end}}' | tr ' ' '\n'; }

# ---- role config
x=$(docker inspect -f '{{.Image}} {{.RestartCount}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' $c); set -- $x
want=$IMG_STREAM; case $role in query_kn_*|binance_bar_edge) want=$IMG_QUERY;; ingestor_*) want=$IMG_INGESTOR;; market_projector_*) want=$IMG_PROJECTOR;; esac
# binance_bar_edge is "unhealthy" during its silent restore (runbook): health is gated at its own end check.
hok=$([ "${4:-}" = healthy ] && echo 1 || echo 0); [ $role = binance_bar_edge ] && hok=1
g image-rc-health $([ "$1" = $want ] && [ "$2" = 0 ] && [ "$3" = running ] && [ $hok = 1 ] && echo 1 || echo 0) "$x"
case $role in stream_kn_*|query_kn_*)
g realm-mount $(mnt | grep -qx "/consumer-realms=$REALM:false" && echo 1 || echo 0) "$(mnt | grep '^/consumer-realms=' | head -1)";; esac
case $role in query_kn_*|stream_kn_*|market_projector_*|binance_bar_edge)
g kn-mount $(mnt | grep -qx "/kn=$KNDIR:false" && echo 1 || echo 0) "$(mnt | grep '^/kn=' | head -1)";; esac
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
  sr=$(echo "$p" | pyj 'print(sum(int(x.get("subscriptions_rejected",-1)) for x in d["lanes"]) if all("subscriptions_rejected" in x for x in d["lanes"]) else "MISSING")')
  g ingestor-subscriptions-rejected $([ "$sr" = 0 ] && echo 1 || echo 0) "sum subscriptions_rejected over lanes=$sr (v2.4.2 field; MISSING = old image)"
  rje=$(docker logs --since $t0 $c 2>&1 | grep -c '"qdl_native_subscription_rejected"')
  g ingestor-rejection-events $([ "$rje" = 0 ] && echo 1 || echo 0) "qdl_native_subscription_rejected lines since T0=$rje"
  rj=$(docker logs --since $t0 $c 2>&1 | grep -v qdl_native_raw_ingestor_progress | grep -ciE 'subscription.*reject|reject.*subscription|"event":"qdl_native_raw_ingestor_[a-z_]*(error|failed)"')
  g ingestor-rejects $([ "$rj" = 0 ] && echo 1 || echo 0) "rejected-subscription/error events=$rj"
  for cr in rust_core rust_core_2 rust_core_3; do
    v=$(docker logs --since 1m $P-$cr-1 2>&1 | grep qdl_realtime_core_progress | tail -1 | pyj 'print(d["scope_quarantines"],d["ignored_out_of_scope"])'); set -- $v
    g core-scope-$cr $([ "$1" = 0 ] && [ "${2:-}" = 0 ] && echo 1 || echo 0) "scope_q=$1 oos=${2:-NA}"; done
  venue=binance-usdm; [ $role = ingestor_okx_swap ] && venue=okx-swap
  cad=$(timeout 30 docker exec -i $P-query_kn_1-1 python3 -B - $venue < $HERE/v240_liveness_cadence.py 2>/dev/null)
  med=$(echo "$cad" | awk '{print $5}')
  g liveness-cadence $(awk "BEGIN{print (\"${med:-0}\"+0>=2.5)?1:0}") "$venue median writes/s=${med:-NA} (>= 2.5; 1000 ms baseline 1.17) $(echo "$cad" | cut -d' ' -f3)";;
binance_bar_edge)
  g bar-edge-runtime $(mnt | grep -qx "/runtime=$KNDIR/runtime:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime=')"
  me=$(docker inspect $c --format '{{range .Config.Env}}{{println .}}{{end}}' | grep '^QDL_STABLE_BAR_CHECKPOINT_MIGRATION=' | cut -d= -f2-)
  g bar-edge-migration-env $([ "$me" = /runtime/bar-edge-checkpoint-migration.json ] && echo 1 || echo 0) "QDL_STABLE_BAR_CHECKPOINT_MIGRATION=$me"
  rs=$(docker exec $c sha256sum /runtime/bar-edge-checkpoint-migration.json | cut -c1-64)
  g bar-edge-migration-record $([ "$rs" = 0f98b8f74c9b0dedbbf8f1ebc63d81701ce5e52a405e0b244de25cc36a429e24 ] && echo 1 || echo 0) "mounted record sha256=$rs (r10 record)"
  cl=$(docker logs --since $t0 $c 2>&1 | grep -m1 'checkpoint crosses')
  g bar-edge-crossing $(echo "$cl" | grep -q 'catalog 16->17 acquisition 24->25 crossed=True retained=1319 added=0 dropped=1 migration_record=True' && echo 1 || echo 0) "${cl#*crosses } (want 16->17 / 24->25 retained=1319 dropped=1 record)"
  cg=$(docker logs --since $t0 $c 2>&1 | grep -c 'cache generation changed; bounded bootstrap required')
  [ "$cg" != 0 ] && { echo "HOLD bar-edge: 'cache generation changed; bounded bootstrap required' x$cg - coordinator decides continue or rollback"; HOLD=1; }
  tb=$(docker logs --since $t0 $c 2>&1 | grep -c 'Traceback')
  g bar-edge-no-traceback $([ "$tb" = 0 ] && echo 1 || echo 0) "Traceback lines since T0=$tb"
  h=$(docker exec $c cat /var/lib/qdl-kn/heartbeat/bar-edge.json 2>/dev/null)
  hu=$(echo "$h" | pyj 'print(d.get("updated_at_ns",0))')
  if [ "${hu:-0}" != NA ] && [ "${hu:-0}" -gt $(( $(date -u -d "$t0" +%s) * 1000000000 )) ]; then
    v=$(echo "$h" | pyj 'r=d["declaration"]["revisions"];print(r["catalog"],r["acquisition"],d["declaration"]["image_files"])'); set -- $v
    g bar-edge-declaration $([ "$1" = 17 ] && [ "${2:-}" = 25 ] && [ "${3:-}" = 0 ] && echo 1 || echo 0) "catalog=$1 acquisition=${2:-NA} image_files=${3:-NA} (want 17/25/0; pre 16/24)"
    det=$(echo "$h" | pyj 'print(d["detail"])')
    nb=$(echo "$det" | grep -o ' bindings=[0-9]*' | tail -1 | cut -d= -f2); iu=$(echo "$det" | grep -o 'instrument_unavailable=[0-9]*' | cut -d= -f2)
    g bar-edge-bindings $([ "$nb" = 1319 ] && echo 1 || echo 0) "bindings=$nb (want 1319, was 1320)"
    g bar-edge-instrument-unavailable $([ "${iu:-x}" = 0 ] && echo 1 || echo 0) "instrument_unavailable=${iu:-MISSING} (RVN retired: want 0)"
    echo "INFO bar_edge heartbeat: $det"
    x=$(docker inspect -f '{{.RestartCount}} {{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' $c); g bar-edge-running-healthy $([ "$x" = "0 running healthy" ] && echo 1 || echo 0) "$x"
  else
    echo "HOLD bar-edge: no heartbeat since T0 yet (silent restore, runbook: 5-9 min expected, rollback if none in 20 min)"; HOLD=1
  fi;;
market_projector_*)
  st=$(docker logs --since $t0 $c 2>&1 | grep -m1 '"event":"started"')
  v=$(echo "$st" | pyj 'print(d["bundle_sha256"],d["stage_a"],d["stage_b"])'); set -- $v
  g projector-bundle $([ "$1" = $KN_BUNDLE ] && [ "${2:-}" = True ] && [ "${3:-}" = True ] && echo 1 || echo 0) "bundle_sha256=$1 stage_a=${2:-NA} stage_b=${3:-NA}"
  ps=$(docker logs --since 1m $c 2>&1 | grep '"schema":"qdl.kn3.projector-status.v1"' | tail -1)
  v=$(echo "$ps" | pyj 'print(json.dumps(d["errors"],separators=(",",":")),d["stage_a"]["poll_errors"],d["stage_b"]["cache_reconnects"],d["stage_b"]["last_rebuild_error"])'); set -- $v
  g projector-status $([ "$1" = "{}" ] && [ "${2:-}" = 0 ] && [ "${4:-}" = None ] && echo 1 || echo 0) "errors=$1 poll_errors=${2:-NA} cache_reconnects=${3:-NA} last_rebuild_error=${4:-NA}"
  tb=$(docker logs --since $t0 $c 2>&1 | grep -ciE 'panic|error' | head -1)
  echo "INFO projector panic/error lines since T0: $tb";;
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
  g query-manifests $(echo "$ml" | grep -qx /consumer-realms/alpha.okx.paper.stable-r18.yaml && echo "$ml" | grep -qx /consumer-realms/alpha.okx.live.stable-r2.yaml && echo "$ml" | grep -qx /consumer-realms/alpha.okx.sandbox.stable-r2.yaml && echo "$ml" | grep -qx /consumer-realms/trading-system.paper.stable-r15.yaml && echo 1 || echo 0) "$(echo "$ml" | grep -E 'alpha.okx|trading-system.paper' | tr '\n' ' ')"
  g query-hot-targets $(echo "$envs" | grep -qx 'QDL_KN_HOT_READ_TARGETS=qdl-v2-stream-b:8210,qdl-v2-stream-a:8210' && echo 1 || echo 0) "$(echo "$envs" | grep '^QDL_KN_HOT_READ_TARGETS=')"
  g query-runtime $(mnt | grep -qx "/runtime=$KNDIR/runtime:false" && echo 1 || echo 0) "$(mnt | grep '^/runtime=')"
  g canonical-mount $(mnt | grep -qx "/canonical=$R/v2.4.1-canonical:false" && echo 1 || echo 0) "$(mnt | grep '^/canonical=')"
  cenv=$(echo "$envs" | grep '^QDL_CANONICAL_INSTRUMENT_METADATA=' | cut -d= -f2-)
  g canonical-enabled $([ "$cenv" = /canonical/canonical-instrument-metadata.json ] && echo 1 || echo 0) "QDL_CANONICAL_INSTRUMENT_METADATA='$cenv'"
  idx=$(timeout 60 docker exec $c python3 -B -c 'import os
from qdl.runtime.canonical_metadata import index_from_environment
i=index_from_environment(os.environ)
print(len(i), i.revision, i.sha256)' 2>&1 | tail -1)
  set -- $idx
  g canonical-index $([ "${1:-0}" = 527 ] && [ "${2:-}" = 1 ] && [ "${3:-}" = "$CANONICAL_SHA" ] && echo 1 || echo 0) "instruments=${1:-NA} revision=${2:-NA} sha256=${3:-NA}"
  pr=$(timeout 120 docker exec -i $c python3 -B - < $HERE/v242_rvn_refusal_probe.py 2>&1); prc=$?
  echo "$pr" | sed 's/^/INFO rvn-probe: /'
  g query-rvn-typed-refusal $([ $prc = 0 ] && echo 1 || echo 0) "catalog/manifests carry no RVN; RVN snapshot -> INSTRUMENT_NOT_FOUND; mixed batch per-item (rc=$prc)"
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
