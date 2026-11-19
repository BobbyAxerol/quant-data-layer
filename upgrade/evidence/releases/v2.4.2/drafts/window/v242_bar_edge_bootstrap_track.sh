#!/bin/bash
# READ-ONLY DL v2.4.2 W6 bar_edge bootstrap tracker: one line per minute; ends after two consecutive samples with
# published=1319 pending=0 history_failed=5 instrument_unavailable=0 decl=17/25, or STOPPED, or the 3 h deadline.
# Then (only on BOOTSTRAP_DONE) waits 15 min and records D6b. docker exec cat / logs / inspect only.
C=qdl_v2_stable_candidate-binance_bar_edge-1; D=$(dirname "$0"); OUT=$D/be-track.log; SINCE=2026-11-19T16:10:47Z; ok=0; end=DEADLINE
for i in $(seq 1 180); do
  hb=$(docker exec $C cat /var/lib/qdl-kn/heartbeat/bar-edge.json 2>/dev/null)
  mt=$(docker exec $C stat -c %Y /var/lib/qdl-kn/heartbeat/bar-edge.json 2>/dev/null)
  det=$(echo "$hb" | python3 -B -c 'import sys,json
try:
  d=json.loads(sys.stdin.read()); r=d["declaration"]["revisions"]; print(d["detail"].replace(" ",","), "decl=%s/%s" % (r["catalog"],r["acquisition"]))
except Exception: print("NA")')
  st=$(docker inspect -f '{{.State.Health.Status}} rc={{.RestartCount}} {{.State.Status}}' $C)
  lg=$(docker logs --since $SINCE $C 2>&1 | grep -cE 'bounded bootstrap required|Traceback|qdl_bar_edge_instrument_unavailable')
  echo "$(date -u +%T) hb_age=$(( $(date +%s) - ${mt:-0} ))s $det | $st | traceback/unavailable/bounded=$lg" >> $OUT
  case "$det" in *published=1319,*pending=0,*history_failed=5,*instrument_unavailable=0,*decl=17/25*) ok=$((ok+1));; *) ok=0;; esac
  [ $ok -ge 2 ] && { end=BOOTSTRAP_DONE; break; }
  case "$st" in *exited*|*dead*) end=STOPPED; break;; esac
  sleep 60
done
echo "$(date -u +%T) $end" >> $OUT
[ $end = BOOTSTRAP_DONE ] || exit 1
sleep 900
docker logs --since 2026-11-19T16:10:51Z $C 2>&1 | grep -oE 'stable derived BAR gap (binding|healed binding|kept binding)=[^ ]+ open_ms=[0-9]+' > $D/d6b.txt
python3 -B -c "
import re
r=set();h=set();k=0
for l in open('$D/d6b.txt'):
  m=re.search(r'binding=(\S+) open_ms=(\d+)',l)
  if 'healed' in l: h.add(m.groups())
  elif 'kept' in l: k+=1
  else: r.add(m.groups())
print('D6B $(date -u +%FT%TZ) recorded',len(r),'healed',len(h),'kept',k,'outstanding',len(r-h))" >> $OUT
docker inspect $C --format 'bar_edge {{.Image}} rc={{.RestartCount}} {{.State.Health.Status}}' >> $OUT
docker exec $C sh -c 'cat /var/lib/qdl-kn/crypto-bar-checkpoint.json.history-short.json' | python3 -c 'import sys,json;print("sidecar proofs",len(json.load(sys.stdin)["proofs"]))' >> $OUT
echo "$(date -u +%T) TRACK_END" >> $OUT
