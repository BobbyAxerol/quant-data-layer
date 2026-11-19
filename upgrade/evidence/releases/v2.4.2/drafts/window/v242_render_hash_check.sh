#!/usr/bin/env bash
# DL v2.4.2 prep, READ-ONLY: (1) every live runtime role's compose config hash, recomputed from the compose files and
# env files its own labels name, must equal its com.docker.compose.config-hash label (the files on disk still
# reproduce what runs); (2) for the r10 window roles, the hash the v2.4.2 layering would give (must differ: the
# intended change) and the image it names. Prints hashes only (docker compose reads the secrets env in-process).
# Mutates nothing: `docker compose config --hash` and `docker inspect` only.
set -u -o pipefail
P=qdl_v2_stable_candidate; R=/home/bobby/.local/state/qdl-v2/releases; C=$R/v2.4.2-compose; F=0
ROLES="kafka1 kafka2 kafka3 rust_core rust_core_2 rust_core_3 ingestor_binance_usdm ingestor_okx_swap market_projector_1 market_projector_2 stable_redis query_kn_1 query_kn_2 stream_kn_1 stream_kn_2 binance_bar_edge"
# market_cache is excluded: created 7 weeks ago from releases/v2.2.0-02cd827/compose.json (root-only, no env file), never
# part of a window and never recreated (ephemeral cache, CLAUDE.md 4.4); reported as INFO.
lab() { docker inspect $P-$1-1 --format "{{index .Config.Labels \"$2\"}}"; }
for r in $ROLES; do
  files=$(lab $r com.docker.compose.project.config_files); envs=$(lab $r com.docker.compose.project.environment_file)
  live=$(lab $r com.docker.compose.config-hash)
  args=(); IFS=, read -ra fa <<<"$files"; for f in "${fa[@]}"; do args+=(-f "$f"); done
  IFS=, read -ra ea <<<"$envs"; for e in "${ea[@]}"; do args+=(--env-file "$e"); done
  got=$(docker compose -p $P "${args[@]}" config --hash "$r" 2>/dev/null | awk '{print $2}')
  if [ "$got" = "$live" ]; then echo "PASS live-reproducible $r $live"; else echo "FAIL live-reproducible $r live=$live recomputed=${got:-ERROR}"; F=1; fi
done
echo "INFO market_cache untouched: image $(docker inspect -f '{{.Image}}' qdl_v2_stable_candidate-market_cache-1 | cut -c8-19) $(docker inspect -f '{{.State.Status}}' qdl_v2_stable_candidate-market_cache-1)"
# target layering per window role: base envs + v2.4.2 overlay(s), then the role's own TLS env + secrets (from labels)
target() { local r=$1; shift; local envs; envs=$(lab $r com.docker.compose.project.environment_file)
  local tls sec; tls=$(echo "$envs" | tr , '\n' | grep '/tls/'); sec=$(echo "$envs" | tr , '\n' | grep 'kn-secrets.env')
  local args=(-f $C/qdl-v2-stable-kn.v2.4.2.yml --env-file $R/v2.3.1-compose/qdl-v2-stable-kn.v2.3.1.env
              --env-file $R/v2.3.1-stage-c-compose/stage-c.env --env-file $C/v2.4.2.env)
  for o in "$@"; do args+=(--env-file "$o"); done; args+=(--env-file "$tls" --env-file "$sec")
  local h; h=$(docker compose -p $P "${args[@]}" config --hash "$r" 2>/dev/null | awk '{print $2}')
  local img; img=$(docker compose -p $P "${args[@]}" config --images "$r" 2>/dev/null | head -1)
  local live; live=$(lab $r com.docker.compose.config-hash)
  if [ -n "$h" ] && [ "$h" != "$live" ]; then echo "PASS target-changes $r -> $h image=$img (live $live)"; else echo "FAIL target $r hash=${h:-ERROR} live=$live"; F=1; fi
}
for r in stream_kn_2 query_kn_2 query_kn_1 stream_kn_1 market_projector_1 market_projector_2 binance_bar_edge; do target $r; done
for r in ingestor_binance_usdm ingestor_okx_swap; do target $r $C/v2.4.2-ingestor.env; done
[ $F = 0 ] && echo "RENDER-HASH CHECK PASS" || echo "RENDER-HASH CHECK FAIL"; exit $F
