# KN maintenance window: reusable step template

Purpose: the fixed step order of a production KN window that restarts or recreates brokers or roles, kept separate from
the per-release packets (which are immutable evidence once a release is published). The worked example is
`upgrade/evidence/releases/v2.2.5/MAINTENANCE_WINDOW_PACKET.md` (W0-W12). A new packet copies these steps and fills in
its own commands, digests and receipts.

| Step | Action | Pass condition |
|---|---|---|
| W0 | Save rollback composes and receipts (W0.1-W0.5 of the v2.2.5 packet) | files written, 0600 where secrets are present |
| W1-W3 | Clean stops in dependency order (writer, readers/cores/ingestors, `stable_redis` + brokers) | all exited with code 0 |
| W4 | Brokers back (`kafka1 kafka2 kafka3`; `stable_redis` only if it was stopped) | brokers answer; URP 0, under-min-ISR 0 |
| **W4b** | **Preferred-leader election after any multi-broker restart (F2, 2026-11-13):** `python3 -B scripts/kafka_preferred_leader_election.py --apply --scope data --cert-dir <generation>/kafka --window-out $WIN/elect-data.jsonl --lock-owner <window owner>`, then `--scope coordinators` as a declared interruption (`--window-out $WIN/elect-coordinators.jsonl`) | `--observe` after: `blocked: 0`, 0 imbalanced, leaders about evenly split; coordinator describe matches the tool output; core group 3 members, no retry loop; **no rollback** (leadership is not data) |
| W5 | Topic-config compare (K6) | `topics` equal to the pre-window capture |
| W6-W11 | Cores (simultaneously), ingestors, projectors, Query, Stream, bar edge | each healthy before the next |
| W12 | TS writer: BOOTSTRAP then STRICT when the outage exceeds the 120 s execution-surface TTL | READY = demanded, then strict READY |

Why W4b: `auto.leader.rebalance.enable` is false on this cluster by design, so after a restart every partition and
coordinator stays led by whichever broker came back first. On 2026-11-13 all 166 leaders were on kafka3, which drew
about 1.8 vCPU against 0.2 on each of the others (RCA packet `KAFKA3_RCA.md`, plan anchor
`dl-v226-stage-a-okx-coverage-20261113`). Precedent: `elect --scope data` on 2026-10-16 (Kafka B roll).

## binance_bar_edge restart: the silent restore (lesson 2026-11-19, DL v2.4.0 W4 and v2.4.1 C)

**Behaviour.** After any bar_edge recreate the process first restores its checkpoint and checks that the durable
cache holds every restored binding's warmup window (`qdl/runtime/stable_bar_edge.py` `_restore_state` ->
`_checkpoint_history_gaps`). It writes its first heartbeat only after that, at the top of the run loop
(`write_heartbeat` in `run_forever`).
- The restore is silent and CPU-bound: about 75-80 % of one core.
- Measured durations:
  - v2.4.0 W4, 1047 bindings: 5 min 38 s from start to the first heartbeat;
  - v2.4.1 C, 1320 bindings at load 20-25: 8 min 54 s.
- During that time the heartbeat file is the *previous* container's, so a gate that reads it shows the old
  declaration.
- The healthcheck (heartbeat younger than 180 s, 3 x 30 s) turns `unhealthy`. This is expected. Nothing on this stack
  restarts an unhealthy container, and health recovers on the first heartbeat.

**Do not roll back for silence alone.** Roll back the bar_edge (checkpoint copy restore, previous image and compose)
only if any of these happens:
1. the container exits or restarts (RestartCount > 0);
2. a `Traceback` or `cache generation changed; bounded bootstrap required` line appears before the first heartbeat.
   The bounded-bootstrap line is a HOLD for the coordinator, not an automatic rollback;
3. no `stable BAR checkpoint restored` line appears within 20 min of the start (about 2x the measured worst case).

**What comes after the restore.** The bootstrap re-fetches what the check marked incomplete, and reports progress
in the heartbeat (`published`, `pending`).
- From DL v2.4.1, provider-short proofs persist in `<checkpoint>.history-short.json`, so only the first v2.4.1 start
  re-proves the short bindings (about 1 h 40 min at 1320 bindings).
- The end condition is published = bindings and pending = 0 on 2 consecutive samples, with only the known
  provider-failed bindings left (the five Binance 3d bindings). Then run D6b 15 min later.
