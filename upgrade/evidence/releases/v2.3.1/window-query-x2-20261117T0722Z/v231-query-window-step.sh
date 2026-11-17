#!/usr/bin/env bash
# DL v2.3.1 Query x2 window, one replica step (owner GO 2026-11-17): recreate ONE Query role on the R0 image, gate it,
# roll that role back by digest on any gate failure. Usage: v231-query-window-step.sh query_kn_2|query_kn_1
# Boundary: touches only the named role; reads env/secret files only as --env-file (never printed).
set -u
s=${1:?role}; case $s in query_kn_1|query_kn_2) ;; *) echo "REFUSED role $s"; exit 2;; esac
Q=/home/bobby/.local/state/qdl-v2; R=$Q/releases; P=qdl_v2_stable_candidate
CD=$R/v2.3.0-rc2-compose; CF=$CD/qdl-v2-stable-kn.yml
OLD="--env-file $CD/qdl-v2-stable-kn.v2.3.0-rc3.env --env-file $R/v2.3.0-stage-b-compose/stage-b.env"
NEW_ENV=$R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env
READER_TLS=$Q/tls/consumer-ca-20261111T1304Z/deployed-tls.env; SECRETS=$R/v2.2.5/kn-secrets.env
C="docker compose -p $P -f $CF"; W=market_data_service_p184w
OLD_IMG=sha256:a235c0a18cdeb112ea0b49627eaf02f2a68ab878298ebba07884461ee5709d0b
NEW_IMG=sha256:26c3b051ed1378aa83928230866cffcd6abcea87d248c06f850ee056994b3b47
WIN=$(cat $Q/V231_QUERY_WINDOW_CURRENT)
health() { timeout ${2:-180} bash -c "until [ \"\$(docker inspect -f '{{.State.Health.Status}}' $P-$1-1 2>/dev/null)\" = healthy ]; do sleep 3; done"; }
hb() { id=$(docker inspect -f '{{.Id}}' $W | cut -c1-12); docker exec redis_service redis-cli get "service:heartbeat:market_data:$id:1" | python3 -c 'import sys,json;d=json.load(sys.stdin)["details"];print(d["demanded_v2_slices"],d["ready_v2_slices"],d.get("execution_ready_v2_slices"))'; }
rollback() {
  echo "ROLLBACK $s: $1"
  $C $OLD --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s > $WIN/$s.rollback.log 2>&1
  health $s; echo "rolled back image=$(docker inspect -f '{{.Image}}' $P-$s-1) health=$(docker inspect -f '{{.State.Health.Status}}' $P-$s-1)"
  exit 1
}
t0=$(date -u +%FT%TZ); echo "$t0" > $WIN/T_$s.start
docker inspect $P-$s-1 --format '{{.Image}} {{.State.StartedAt}}' > $WIN/$s.pre
docker logs --since 30m $P-$s-1 > $WIN/$s.pre.log 2>&1; chmod 600 $WIN/$s.pre.log
$C --env-file $NEW_ENV --env-file $READER_TLS --env-file $SECRETS up -d --no-deps --force-recreate $s > $WIN/$s.up.log 2>&1 || rollback "compose up rc=$?"
health $s || rollback "not healthy in 180 s"
img=$(docker inspect -f '{{.Image}} {{.RestartCount}}' $P-$s-1); echo "$img" > $WIN/$s.post
[ "${img% *}" = "$NEW_IMG" ] && [ "${img#* }" = 0 ] || rollback "image/restart gate: $img"
docker exec $P-$s-1 sh -c 'grep -m1 -E "^\s+revision: 14" /consumer-realms/trading-system.paper.stable-r14.yaml' >/dev/null || rollback "manifest r14 not loaded"
lbl=b4-kn2; [ $s = query_kn_1 ] && lbl=b4-kn1
timeout 300 bash -c 'until awk "BEGIN{exit !($(cut -d" " -f1 /proc/loadavg) < 22)}"; do sleep 10; done' || rollback "load stayed >= 22, B4 smoke could not run"
sh $Q/v231-latency-q12/run_b4.sh $lbl > $WIN/$s.b4.out 2>&1
rep=$(ls -td $Q/v231-latency-q12/run-$lbl-* | head -1)/report.json
b4=$(python3 -B $Q/v231-latency-q12/b4_gate.py "$(dirname $rep)" 2>$WIN/$s.b4.bad)
echo "b4 $b4" | tee $WIN/$s.b4
set -- $b4; [ "${1:-0}" -gt 0 ] && [ "${1:-0}" = "${2:-x}" ] || rollback "B4 snapshot:batch gate: $b4"
sleep 120
auth=$(docker logs --since $t0 $W 2>&1 | grep -cE 'UNAUTHENTICATED|PERMISSION_DENIED|quota is exhausted|CURSOR_EXPIRED|AUTHORITY_MISMATCH|RESUME_GENERATION_MISMATCH')
h=$(hb); echo "writer auth_refusals=$auth hb=$h" | tee $WIN/$s.writer
[ "$auth" = 0 ] || rollback "writer refusals $auth"
# Slice gate: 246/246 at least once in 180 s; otherwise pass only if every not-ready entry is the pre-existing quiet-market
# class seen since 04:15Z (EXECUTION_CONTEXT_STALE / TRADE_EVENT_STALE). Any other reason rolls back.
ok=0; for i in $(seq 1 18); do set -- $(hb); [ "$1" = 246 ] && [ "$2" = 246 ] && { ok=1; break; }; sleep 10; done
if [ $ok = 0 ]; then
  id=$(docker inspect -f '{{.Id}}' $W | cut -c1-12)
  other=$(docker exec redis_service redis-cli get "service:heartbeat:market_data:$id:1" | python3 -c 'import sys,json;d=json.load(sys.stdin)["details"];print(sorted({x["reason_code"] for x in d.get("not_ready",[])}-{"EXECUTION_CONTEXT_STALE","TRADE_EVENT_STALE"}))')
  echo "slices not 246/246 in 180 s; other reasons=$other" | tee -a $WIN/$s.writer
  [ "$other" = "[]" ] || rollback "writer slices with new reason $other"
fi
echo "PASS $s $(date -u +%FT%TZ)" | tee $WIN/$s.PASS
