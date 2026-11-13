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
