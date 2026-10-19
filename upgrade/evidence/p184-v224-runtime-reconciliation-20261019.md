# Data Layer v2.2.4 runtime vs repository reconciliation - 2026-10-19

Observed 2026-10-19T08:13:50Z by docker ps / docker inspect / docker image inspect and file reads only; no container, Redis, Postgres or image mutation. Repository: origin/main b5ab3c2ab440 (tag v2.2.4 b5ab3c2ab440), origin/dev ba2bffeb07fe; dev = main + PR #34 (CI files and journal only); no runtime source differs. Release certificate: `upgrade/evidence/releases/v2.2.4/certificate.json`.

## Summary

- images: Every DL application image runs a source revision that is an ancestor of origin/main (9dab224, c7ce7d3, a4e7b3b, 31a5155, a388537; V1 2b0dcf7 with io.qdl.source-tree = 2b0dcf7^{tree}). Digests equal certificate.json release_images/runtime for all 18 certified rows. No code or image change is needed to make main describe the running binaries.
- post_revision_source_on_main: rust after 9dab224: cfg(test) only in qdl-native-raw-ingestor.rs; projector binary whole-file EQUAL in a 9dab224 build (RUSTLS_0.23.46_ROLL_PACKET.md); rc22 core kept by owner decision (0 rustls code, 5 CODEGEN functions differ); python after 31a5155: qdl/certification/reference_l2_acceptance.py only (apparatus).
- certificate_rows: All 18 certificate.json runtime rows equal the live containers in image id, cpus, memory and StartedAt (no restart or recreate since the certificate re-issue); restart count 0 on every role.
- caps: Live cpu/memory caps equal certificate.json runtime and docker-compose.v2-stable.yml on main for every role that the compose defines under the same or mapped name.

## Roles

| container | image (tag) | id | source rev | on main | cpus / MiB | create compose caps = live |
|---|---|---|---|---|---|---|
| data_layer_service | qdl-v1-fallback:v1.2.4-2b0dcf7 | dbfb57844977 | 2b0dcf74454c | yes | uncapped | n/a (unreadable) |
| market_data_service_p184w | local/trading-system:p184-writer-release-30f50a91 | 2deb90b0cc52 | 30f50a91dbd6 | n/a | uncapped | yes (uncapped in both) |
| binance_bar_edge-1 | qdl-v2-python:2.2.3-rc14-a388537 | 08f356a84151 | a388537c897d | yes | 0.75 / 512 | yes |
| ingestor_binance_usdm-1 | qdl-v2-rust:2.2.4-9dab224 | 27c79b53400e | 9dab22429ea4 | yes | 0.75 / 256 | yes |
| ingestor_okx_swap-1 | qdl-v2-rust:2.2.4-9dab224 | 27c79b53400e | 9dab22429ea4 | yes | 1.0 / 256 | no (file 0.5 cpu / 256 MiB) |
| kafka1-1 | apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9 | 9516fb7634ba | - | n/a | 2.25 / 1536 | no (file 1.75 cpu / 1536 MiB) |
| kafka2-1 | apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9 | 9516fb7634ba | - | n/a | 2.25 / 2048 | no (file 1.75 cpu / 2048 MiB) |
| kafka3-1 | apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9 | 9516fb7634ba | - | n/a | 2.25 / 1536 | no (file 1.75 cpu / 1536 MiB) |
| market_cache-1 | redis@sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8 | dfa18828cbc0 | - | n/a | 1.0 / 9216 | n/a (unreadable) |
| market_projector_1-1 | qdl-v2-rust:2.2.4-c7ce7d3 | 60f83a5106b0 | c7ce7d3087cb | yes | 0.75 / 512 | yes |
| market_projector_2-1 | qdl-v2-rust:2.2.4-c7ce7d3 | 60f83a5106b0 | c7ce7d3087cb | yes | 0.75 / 512 | yes |
| query_kn_1-1 | qdl-v2-python:2.2.4-31a5155 | 3e9e4d3add0f | 31a515522aff | yes | 2.0 / 2048 | no (file 1.5 cpu / 1536 MiB) |
| query_kn_2-1 | qdl-v2-python:2.2.4-31a5155 | 3e9e4d3add0f | 31a515522aff | yes | 2.0 / 2048 | no (file 1.5 cpu / 1536 MiB) |
| rust_core-1 | qdl-v2-rust:2.2.3-rc22-a4e7b3b | 7c848aadcb0d | a4e7b3b82475 | yes | 1.0 / 256 | yes |
| rust_core_2-1 | qdl-v2-rust:2.2.3-rc22-a4e7b3b | 7c848aadcb0d | a4e7b3b82475 | yes | 1.0 / 256 | yes |
| rust_core_3-1 | qdl-v2-rust:2.2.3-rc22-a4e7b3b | 7c848aadcb0d | a4e7b3b82475 | yes | 0.5 / 256 | yes |
| stable_redis-1 | redis@sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8 | dfa18828cbc0 | - | n/a | 0.5 / 160 | n/a (unreadable) |
| stream_kn_1-1 | qdl-v2-rust:2.2.4-9dab224 | 27c79b53400e | 9dab22429ea4 | yes | 0.75 / 512 | yes |
| stream_kn_2-1 | qdl-v2-rust:2.2.4-9dab224 | 27c79b53400e | 9dab22429ea4 | yes | 0.75 / 512 | yes |

V2 roles are compose project `qdl_v2_stable_candidate`; restart policy `unless-stopped` and restart count 0 on every role. `market_data_service_p184w` is the Trading System writer (ExecutorBroker revision, not this repository).

## Consumer manifests

| consumer id | runtime file | runtime rev | runtime sha256 | repository file (main) | repo rev | repo sha256 |
|---|---|---|---|---|---|---|
| alpha.binance.live.stable | realm `alpha.binance.live.stable.yaml` | 1 | e6b74f282045 | - | - | - |
| alpha.binance.paper.stable | realm `alpha.binance.paper.stable-r16.yaml` | 16 | 050d96dcc6f1 | consumers/stable/alpha-binance-paper.yaml | 16 | 050d96dcc6f1 |
| alpha.binance.sandbox.stable | realm `alpha.binance.sandbox.stable.yaml` | 1 | 514397f90bc9 | - | - | - |
| alpha.okx.live.stable | realm `alpha.okx.live.stable.yaml` | 1 | aa1096f95d11 | - | - | - |
| alpha.okx.paper.stable | realm `alpha.okx.paper.stable-r15.yaml` | 15 | 33b9cfd4ff19 | consumers/stable/alpha-okx-paper.yaml | 14 | e8fb3ea78305 |
| alpha.okx.sandbox.stable | realm `alpha.okx.sandbox.stable.yaml` | 1 | 4ec1d11b6c94 | - | - | - |
| trading-system.live.stable | realm `trading-system.live.stable.yaml` | 1 | 2d5790522e59 | - | - | - |
| trading-system.paper.stable | realm `trading-system.paper.stable-r11.yaml` | 11 | efd482fa4ffe | consumers/stable/trading-system-paper.yaml | 10 | 1e489237ad73 |
| trading-system.sandbox.stable | realm `trading-system.sandbox.stable.yaml` | 2 | fa7073f164ea | - | - | - |
| alpha.vn.paper.stable | baked `/app/consumers/stable/alpha-vn-paper.yaml` (image) | 1 | (as repository) | consumers/stable/alpha-vn-paper.yaml | 1 | be7331aaf264 |
| monitoring.multivenue.stable | baked `/app/consumers/stable/monitoring-multivenue.yaml` (image) | 2 | (as repository) | consumers/stable/monitoring-multivenue.yaml | 2 | 304a610d033a |
| qdl.crypto.reference-l2.stable | baked `/app/consumers/stable/reference-l2-stable.yaml` (image) | 7 | (as repository) | consumers/stable/reference-l2-stable.yaml | 7 | 5b220aba0afa |

Runtime realm directory: `~/.local/state/qdl-v2/p184-d3-capacity-20261009/reader-public-r16` (mounted read-only as /consumer-realms in query_kn_1/2 and stream_kn_1/2). Baked rows hold for query_kn (31a5155, identical consumers/ to main); binance_bar_edge bakes a388537 (see R3).

## Catalog

- Runtime gateway bundle: revision 12, 716 bindings, file sha256 e8d934e1c0c27644, bundle field sha256 63efd9ffb9a4cb9e.
- Repository `config/v2/stable-source-bindings.yaml`: revision 11, 712 bindings.
- Runtime-only bindings: okx-swap-btc-usd-swap-book_delta, okx-swap-btc-usd-swap-book_snapshot, okx-swap-btc-usd-swap-mark_index_price, okx-swap-btc-usd-swap-quote; repository-only: none.

## Drift

| id | severity | kind | fact | fix |
|---|---|---|---|---|
| R1 | medium | topology definition | docker-compose.v2-stable.yml on main defines the pre-KN roles query_v2_1/2, stream_v2_active/passive (python image, 2.0 vCPU), projector_v2..projector_v2_6 (python image, 1.0 vCPU / 768 MiB) and no market_cache. The running KN roles query_kn_1/2, stream_kn_1/2 (rust qdl-stream-gateway, 0.75 vCPU / 512 MiB), market_projector_1/2 (rust qdl-projector, 0.75 vCPU / 512 MiB) and market_cache (1.0 vCPU / 9 GiB) are defined only by per-role compose files under ~/.local/state/qdl-v2 generated from docker inspect (scripts/kn_production_packet.py service_from_inspect). | config slice for the next release: carry the KN roles into a versioned compose (CI5 full run); not docs-only |
| R2 | medium | consumer manifests / catalog (IDR-127) | trading-system.paper.stable runtime r11 (efd482fa) vs consumers/stable r10 (1e489237); alpha.okx.paper.stable runtime r15 (33b9cfd4, not in the repository by sha256) vs r14 (e8fb3ea7); runtime catalog r12 (716 bindings) vs config/v2/stable-source-bindings.yaml r11 (712); the 4 runtime-only bindings are OKX BTC-USD-SWAP book_delta, book_snapshot, mark_index_price, quote. The 6 live/sandbox realm manifests are not in the repository by sha256 (their generator was not checked). alpha.binance.paper.stable r16 is byte-identical to the repository. | code/config + tests slice (changes what the next python image bakes; bar edge checkpoint C.19/C.32): owner approval, next release |
| R3 | low | held role | binance_bar_edge runs qdl-v2-python:2.2.3-rc14-a388537 (held_scoped_out in the certificate); it bakes alpha-binance-paper r14 (main r16) and reference-l2-stable r6 (main r7). | none until the bar-edge roll that IDR-127 plans |
| R4 | low | stale create-compose snapshots | The OPT caps were applied by docker update, so the compose files that created kafka1-3 (cpus 1.75), query_kn_1/2 (1.5 vCPU / 1.5 GiB) and ingestor_okx_swap (0.5 vCPU) still hold the pre-OPT caps. Recreating from those files would silently revert the caps; the packet path (service_from_inspect) reads the live caps. | any recreate must build its compose from docker inspect, never reuse these files |
| R5 | low | create-compose provenance | stable_redis was created from a compose in the deleted worktree data-layer-dev-closure; market_cache from ~/.local/state/qdl-v2/releases/v2.2.0-02cd827/compose.json (root-only, not read); data_layer_service (V1) from docker-compose.yml + a deleted worktree copy of docker-compose.phase105c-v1-fallback.yml. | docker start only (CLAUDE.md section 1); no compose up |
| R6 | low | release tag | qdl-v1-fallback:v1.2.4-2b0dcf7 (V1 fallback) has no git tag v1.2.4 on origin (remote V1 tags: v1.2.1, v1.2.2); its source 2b0dcf7 is on main. | owner decision whether to tag; tags are release-only |
| R7 | info | certificate field | certificate.json publication_status NOT_TAGGED_CERTIFICATE_PASS_UNDER_POST_HOC_A5 predates the tag; the publication (tag v2.2.4 on b5ab3c2, release run 39214746630) is recorded in the plan journal. The published certificate is not edited. | none |
| R8 | info | cross-repo | market_data_service_p184w (TS writer, 2deb90b0) runs ExecutorBroker revision 30f50a91, which is on origin/feat/v2-rust-first-okx-demo only, not on the Trading System main. | Trading System reconciliation, outside this repository |

No container, image, compose file, Redis, Postgres or Kafka state was touched; the published v2.2.4 tag and its evidence are unchanged.

## Machine-readable record

```json
{
 "schema": "qdl.runtime-reconciliation.v1",
 "release": "v2.2.4",
 "observed_utc": "2026-10-19T08:13:50Z",
 "method": "docker ps / docker inspect / docker image inspect and file reads only; no container, Redis, Postgres or image mutation",
 "repository": {
  "origin_main": "b5ab3c2ab440f23636b18acdcc3fa1cf999dfdc5",
  "tag_v2_2_4": "b5ab3c2ab440f23636b18acdcc3fa1cf999dfdc5",
  "origin_dev": "ba2bffeb07fe370b00c6b0ec76b0e34cfa8364cc",
  "main_vs_dev_content": "dev = main + PR #34 (CI files and journal only); no runtime source differs"
 },
 "certificate": "upgrade/evidence/releases/v2.2.4/certificate.json",
 "roles": [
  {
   "container": "data_layer_service",
   "compose_project": "data_layer",
   "service": "data_layer",
   "image_id": "sha256:dbfb57844977513ae7ec0a4782e04da0213028a789753c6b991f26043b615d65",
   "repo_tags": [
    "qdl-v1-fallback:v1.2.4-2b0dcf7"
   ],
   "image_version_label": "v1.2.4",
   "source_revision": "2b0dcf74454c9f87c352d3c47389955aeb955804",
   "source_on_origin_main": true,
   "cpus": null,
   "memory_mib": null,
   "memory_swap_mib": null,
   "restart_policy": "unless-stopped",
   "created": "2026-08-30T08:27:00Z",
   "started": "2026-09-15T04:31:56Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/data_layer/docker-compose.yml,~/.worktrees/quant-data-layer-v2-authority/docker-compose.phase105c-v1-fallback.yml",
    "sha256": "UNREADABLE:FileNotFoundError"
   },
   "revision_env": null
  },
  {
   "container": "market_data_service_p184w",
   "compose_project": "trading-system-market-data",
   "service": "market_data_service_p184w",
   "image_id": "sha256:2deb90b0cc52eae7610fa44692f90df077e9db100ca259e38fd652a2dc1c5dc5",
   "repo_tags": [
    "local/trading-system:p184-writer-release-30f50a91"
   ],
   "image_version_label": "p184-writer-30f50a91",
   "source_revision": "30f50a91dbd67ce2a482480be598cf509b25333e",
   "source_on_origin_main": null,
   "cpus": null,
   "memory_mib": null,
   "memory_swap_mib": null,
   "restart_policy": "unless-stopped",
   "created": "2026-10-19T04:01:32Z",
   "started": "2026-10-19T04:36:21Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/trading-system/p184-20261005/runtime/p1/rc15/native-trace-roll/trace-20261019T040132Z.compose.json",
    "sha256": "26c689dab21fe937360a3165305bef1fb8f5ce9c4fde7cd38c59d3a68dbdaac9",
    "caps_equal_live": true,
    "note": "uncapped in both"
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-binance_bar_edge-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "binance_bar_edge",
   "image_id": "sha256:08f356a84151c6e865fbdc4a2efe6924df9336d15353254c2b2da684261794f5",
   "repo_tags": [
    "qdl-v2-python:2.2.3-rc14-a388537"
   ],
   "image_version_label": "2.2.3-rc14",
   "source_revision": "a388537c897de909257c7f45db44c9ac44a62ccc",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 512,
   "memory_swap_mib": 1024,
   "restart_policy": "unless-stopped",
   "created": "2026-10-12T04:56:50Z",
   "started": "2026-10-12T04:56:51Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc6/binance_bar_edge-rc6.compose.json",
    "sha256": "a246784a548b3cbfc2d2fa8e8b66d59276eef66737e778d830d1baed534b40ce",
    "caps_equal_live": true
   },
   "revision_env": {
    "QDL_STABLE_CONSUMER_MANIFESTS": [
     "/app/consumers/stable/monitoring-multivenue.yaml",
     "/app/consumers/stable/alpha-binance-paper.yaml",
     "/app/consumers/stable/alpha-okx-paper.yaml",
     "/app/consumers/stable/alpha-vn-paper.yaml",
     "/app/consumers/stable/trading-system-paper.yaml",
     "/app/consumers/stable/reference-l2-stable.yaml"
    ],
    "QDL_STABLE_AUTHORITY_REVISION": "1",
    "QDL_CONFIG_REVISION": "phasec36-reference-l2-r14"
   }
  },
  {
   "container": "qdl_v2_stable_candidate-ingestor_binance_usdm-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "ingestor_binance_usdm",
   "image_id": "sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-9dab224"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "9dab22429ea44566587f31d9f56f8b059933c795",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 256,
   "memory_swap_mib": 512,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T18:53:11Z",
   "started": "2026-10-18T18:53:12Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/ingestor_binance_usdm-rc17v2-20261018T185311Z.compose.json",
    "sha256": "5001b627b449978293970c22a90f4cb73e03286550338da636590b8a07551cd0",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-ingestor_okx_swap-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "ingestor_okx_swap",
   "image_id": "sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-9dab224"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "9dab22429ea44566587f31d9f56f8b059933c795",
   "source_on_origin_main": true,
   "cpus": 1.0,
   "memory_mib": 256,
   "memory_swap_mib": 512,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T18:46:18Z",
   "started": "2026-10-18T18:46:19Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/ingestor_okx_swap-rc17v2-20261018T184618Z.compose.json",
    "sha256": "f4ec5af26f62268b168add35bba4d3cbb82ec7d9afa4b32f29aa31180acf723c",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 0.5,
     "memory_bytes": 268435456
    }
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-kafka1-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "kafka1",
   "image_id": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
   "repo_tags": [
    "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
   ],
   "image_version_label": null,
   "source_revision": null,
   "source_on_origin_main": null,
   "cpus": 2.25,
   "memory_mib": 1536,
   "memory_swap_mib": 3072,
   "restart_policy": "unless-stopped",
   "created": "2026-10-16T14:14:09Z",
   "started": "2026-10-16T14:14:11Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/b-packet/candidate.compose.json",
    "sha256": "6769c1fa725494be3009ec426280ffcc9351fd3c5c9f1549802fdb9b46d2d6c5",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 1.75,
     "memory_bytes": 1610612736
    }
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-kafka2-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "kafka2",
   "image_id": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
   "repo_tags": [
    "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
   ],
   "image_version_label": null,
   "source_revision": null,
   "source_on_origin_main": null,
   "cpus": 2.25,
   "memory_mib": 2048,
   "memory_swap_mib": 4096,
   "restart_policy": "unless-stopped",
   "created": "2026-10-16T14:21:01Z",
   "started": "2026-10-16T14:21:03Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/b-packet/candidate.compose.json",
    "sha256": "6769c1fa725494be3009ec426280ffcc9351fd3c5c9f1549802fdb9b46d2d6c5",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 1.75,
     "memory_bytes": 2147483648
    }
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-kafka3-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "kafka3",
   "image_id": "sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9",
   "repo_tags": [
    "apache/kafka@sha256:9516fb7634bad307d17c33b589fde9023003b0cb761374f500002b980a3149b9"
   ],
   "image_version_label": null,
   "source_revision": null,
   "source_on_origin_main": null,
   "cpus": 2.25,
   "memory_mib": 1536,
   "memory_swap_mib": 3072,
   "restart_policy": "unless-stopped",
   "created": "2026-10-16T14:25:30Z",
   "started": "2026-10-16T14:25:32Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-g12-coordinator-load-20261016/b-packet/candidate.compose.json",
    "sha256": "6769c1fa725494be3009ec426280ffcc9351fd3c5c9f1549802fdb9b46d2d6c5",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 1.75,
     "memory_bytes": 1610612736
    }
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-market_cache-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "market_cache",
   "image_id": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
   "repo_tags": [
    "redis@sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8"
   ],
   "image_version_label": null,
   "source_revision": null,
   "source_on_origin_main": null,
   "cpus": 1.0,
   "memory_mib": 9216,
   "memory_swap_mib": 18432,
   "restart_policy": "unless-stopped",
   "created": "2026-09-27T04:50:38Z",
   "started": "2026-09-27T05:29:58Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.0-02cd827/compose.json",
    "sha256": "UNREADABLE:PermissionError"
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-market_projector_1-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "market_projector_1",
   "image_id": "sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-c7ce7d3"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "c7ce7d3087cb8a2daf82ba941982bd296d488f45",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 512,
   "memory_swap_mib": 1024,
   "restart_policy": "unless-stopped",
   "created": "2026-10-16T17:26:04Z",
   "started": "2026-10-16T17:26:05Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout/market_projector_1-rc17v2-20261016T172603Z.compose.json",
    "sha256": "4aa6dd2ab75cf5b8208388a6282afa8bf25d566efaacc05ff9cfc46d2f6649e5",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-market_projector_2-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "market_projector_2",
   "image_id": "sha256:60f83a5106b0baa33477cf64abee6fab471d80d9c1808981e02270516b11143e",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-c7ce7d3"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "c7ce7d3087cb8a2daf82ba941982bd296d488f45",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 512,
   "memory_swap_mib": 1024,
   "restart_policy": "unless-stopped",
   "created": "2026-10-16T17:32:21Z",
   "started": "2026-10-16T17:32:22Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout/market_projector_2-rc17v2-20261016T173221Z.compose.json",
    "sha256": "eecf2b733748e0b2c6d15962e15810e0a90f7c4c0c7523752e3138f7663f009d",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-query_kn_1-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "query_kn_1",
   "image_id": "sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff",
   "repo_tags": [
    "qdl-v2-python:2.2.4-31a5155"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "31a515522aff3f6e5e7791aa21500cfa570ccb41",
   "source_on_origin_main": true,
   "cpus": 2.0,
   "memory_mib": 2048,
   "memory_swap_mib": 4096,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T03:47:52Z",
   "started": "2026-10-18T03:47:53Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15/query_kn_1-rc15-20261018T034751Z.compose.json",
    "sha256": "04176cb599ba3b1bb73d5ed4abdf3f62cd21196ef272fdd40fba30d9658ec6f7",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 1.5,
     "memory_bytes": 1610612736
    }
   },
   "revision_env": {
    "QDL_STABLE_CONSUMER_MANIFESTS": [
     "/app/consumers/stable/monitoring-multivenue.yaml",
     "/consumer-realms/alpha.binance.paper.stable-r16.yaml",
     "/consumer-realms/alpha.okx.paper.stable-r15.yaml",
     "/app/consumers/stable/alpha-vn-paper.yaml",
     "/consumer-realms/trading-system.paper.stable-r11.yaml",
     "/app/consumers/stable/reference-l2-stable.yaml",
     "/consumer-realms/alpha.binance.live.stable.yaml",
     "/consumer-realms/alpha.binance.sandbox.stable.yaml",
     "/consumer-realms/alpha.okx.live.stable.yaml",
     "/consumer-realms/alpha.okx.sandbox.stable.yaml",
     "/consumer-realms/trading-system.live.stable.yaml",
     "/consumer-realms/trading-system.sandbox.stable.yaml"
    ],
    "QDL_KN_ROUTE_GENERATION": "v220-02cd827",
    "QDL_STABLE_AUTHORITY_REVISION": "1",
    "QDL_CONFIG_REVISION": "phasec36-reference-l2-r14"
   }
  },
  {
   "container": "qdl_v2_stable_candidate-query_kn_2-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "query_kn_2",
   "image_id": "sha256:3e9e4d3add0f35c5d4b4796c785a35b466c326d3328184f396182b5f51367fff",
   "repo_tags": [
    "qdl-v2-python:2.2.4-31a5155"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "31a515522aff3f6e5e7791aa21500cfa570ccb41",
   "source_on_origin_main": true,
   "cpus": 2.0,
   "memory_mib": 2048,
   "memory_swap_mib": 4096,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T03:58:40Z",
   "started": "2026-10-18T03:58:41Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc15/query_kn_2-rc15-20261018T035840Z.compose.json",
    "sha256": "c817154ad170f081d35bf66cd9059e68080ab0e60b88a5963f32d23c791577b6",
    "caps_equal_live": false,
    "file_caps": {
     "cpus": 1.5,
     "memory_bytes": 1610612736
    }
   },
   "revision_env": {
    "QDL_STABLE_AUTHORITY_REVISION": "1",
    "QDL_STABLE_CONSUMER_MANIFESTS": [
     "/app/consumers/stable/monitoring-multivenue.yaml",
     "/consumer-realms/alpha.binance.paper.stable-r16.yaml",
     "/consumer-realms/alpha.okx.paper.stable-r15.yaml",
     "/app/consumers/stable/alpha-vn-paper.yaml",
     "/consumer-realms/trading-system.paper.stable-r11.yaml",
     "/app/consumers/stable/reference-l2-stable.yaml",
     "/consumer-realms/alpha.binance.live.stable.yaml",
     "/consumer-realms/alpha.binance.sandbox.stable.yaml",
     "/consumer-realms/alpha.okx.live.stable.yaml",
     "/consumer-realms/alpha.okx.sandbox.stable.yaml",
     "/consumer-realms/trading-system.live.stable.yaml",
     "/consumer-realms/trading-system.sandbox.stable.yaml"
    ],
    "QDL_KN_ROUTE_GENERATION": "v220-02cd827",
    "QDL_CONFIG_REVISION": "phasec36-reference-l2-r14"
   }
  },
  {
   "container": "qdl_v2_stable_candidate-rust_core-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "rust_core",
   "image_id": "sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb",
   "repo_tags": [
    "qdl-v2-rust:2.2.3-rc22-a4e7b3b"
   ],
   "image_version_label": "2.2.3-rc22",
   "source_revision": "a4e7b3b82475b2c4359a27c0fa1144e4f3a2805e",
   "source_on_origin_main": true,
   "cpus": 1.0,
   "memory_mib": 256,
   "memory_swap_mib": 512,
   "restart_policy": "unless-stopped",
   "created": "2026-10-15T13:33:01Z",
   "started": "2026-10-15T13:33:01Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc22-simultaneous/rust_core-simultaneous-20261015T133222Z.compose.json",
    "sha256": "4af1fca25c736bda35d5e050624642e0422b4fac7c40cb892d24726fb9f2a303",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-rust_core_2-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "rust_core_2",
   "image_id": "sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb",
   "repo_tags": [
    "qdl-v2-rust:2.2.3-rc22-a4e7b3b"
   ],
   "image_version_label": "2.2.3-rc22",
   "source_revision": "a4e7b3b82475b2c4359a27c0fa1144e4f3a2805e",
   "source_on_origin_main": true,
   "cpus": 1.0,
   "memory_mib": 256,
   "memory_swap_mib": 512,
   "restart_policy": "unless-stopped",
   "created": "2026-10-15T13:33:01Z",
   "started": "2026-10-15T13:33:01Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc22-simultaneous/rust_core_2-simultaneous-20261015T133222Z.compose.json",
    "sha256": "cf355b25da0b53fd7a29ec7c7d7aaeaef59d7b7b367f20e0431fec54c6611f3b",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-rust_core_3-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "rust_core_3",
   "image_id": "sha256:7c848aadcb0dd28178bc6dc2b58c42df6c2c235de6d4bfd6602ef7c19fa64bfb",
   "repo_tags": [
    "qdl-v2-rust:2.2.3-rc22-a4e7b3b"
   ],
   "image_version_label": "2.2.3-rc22",
   "source_revision": "a4e7b3b82475b2c4359a27c0fa1144e4f3a2805e",
   "source_on_origin_main": true,
   "cpus": 0.5,
   "memory_mib": 256,
   "memory_swap_mib": 512,
   "restart_policy": "unless-stopped",
   "created": "2026-10-15T13:33:01Z",
   "started": "2026-10-15T13:33:01Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/rollout-rc22-simultaneous/rust_core_3-simultaneous-20261015T133222Z.compose.json",
    "sha256": "d51251b99d654d565d4520085d5a732aa8ada1e26b031351c490c7bada78e855",
    "caps_equal_live": true
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-stable_redis-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "stable_redis",
   "image_id": "sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8",
   "repo_tags": [
    "redis@sha256:dfa18828cbc07b3ae6a95ec7343f6c214fdee2d836197b4be8e9904420762cd8"
   ],
   "image_version_label": null,
   "source_revision": null,
   "source_on_origin_main": null,
   "cpus": 0.5,
   "memory_mib": 160,
   "memory_swap_mib": 320,
   "restart_policy": "unless-stopped",
   "created": "2026-09-03T12:36:16Z",
   "started": "2026-09-16T06:38:55Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.worktrees/data-layer-dev-closure/docker-compose.v2-stable.yml",
    "sha256": "UNREADABLE:FileNotFoundError"
   },
   "revision_env": null
  },
  {
   "container": "qdl_v2_stable_candidate-stream_kn_1-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "stream_kn_1",
   "image_id": "sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-9dab224"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "9dab22429ea44566587f31d9f56f8b059933c795",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 512,
   "memory_swap_mib": 1024,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T19:00:18Z",
   "started": "2026-10-18T19:00:22Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/stream_kn_1-rc17v2-20261018T190018Z.compose.json",
    "sha256": "7ff9cc44cbba89154fe0ed2fab100b81445b50c0c13511c92216838dd396dbe0",
    "caps_equal_live": true
   },
   "revision_env": {
    "QDL_KN_ROUTE_GENERATION": "v220-02cd827"
   }
  },
  {
   "container": "qdl_v2_stable_candidate-stream_kn_2-1",
   "compose_project": "qdl_v2_stable_candidate",
   "service": "stream_kn_2",
   "image_id": "sha256:27c79b53400eea5582713a3477034c8561f533755e09a94223e254bffadd5c92",
   "repo_tags": [
    "qdl-v2-rust:2.2.4-9dab224"
   ],
   "image_version_label": "2.2.4",
   "source_revision": "9dab22429ea44566587f31d9f56f8b059933c795",
   "source_on_origin_main": true,
   "cpus": 0.75,
   "memory_mib": 512,
   "memory_swap_mib": 1024,
   "restart_policy": "unless-stopped",
   "created": "2026-10-18T19:06:32Z",
   "started": "2026-10-18T19:07:18Z",
   "restart_count": 0,
   "create_compose": {
    "path": "~/.local/state/qdl-v2/releases/v2.2.4/rollout-rustls-0.23.46/stream_kn_2-rc17v2-20261018T190632Z.compose.json",
    "sha256": "6b50caf4a9d4069ff79d420e61340cb6cf9e69fe0b14e139a9ba2f026076a585",
    "caps_equal_live": true
   },
   "revision_env": {
    "QDL_KN_ROUTE_GENERATION": "v220-02cd827"
   }
  }
 ],
 "consumer_manifests": {
  "runtime_realm_dir": "~/.local/state/qdl-v2/p184-d3-capacity-20261009/reader-public-r16",
  "runtime": [
   {
    "mounted_file": "alpha.binance.live.stable.yaml",
    "consumer_id": "alpha.binance.live.stable",
    "revision": 1,
    "sha256": "e6b74f2820458a9336045bcf49f628ddfc0d83ca06c63726b58fa4b1f6cf37ca"
   },
   {
    "mounted_file": "alpha.binance.paper.stable-r16.yaml",
    "consumer_id": "alpha.binance.paper.stable",
    "revision": 16,
    "sha256": "050d96dcc6f1da5b42b373e05baae4e6d7965eb44d163e5a320302975ecd9269"
   },
   {
    "mounted_file": "alpha.binance.sandbox.stable.yaml",
    "consumer_id": "alpha.binance.sandbox.stable",
    "revision": 1,
    "sha256": "514397f90bc9411f08f5e0eaadc637ff9fa872925849639e3ffe3b41a689e048"
   },
   {
    "mounted_file": "alpha.okx.live.stable.yaml",
    "consumer_id": "alpha.okx.live.stable",
    "revision": 1,
    "sha256": "aa1096f95d118befa5e697bee81ddc68041cd03abd5fafa6546925329b1a2cd9"
   },
   {
    "mounted_file": "alpha.okx.paper.stable-r15.yaml",
    "consumer_id": "alpha.okx.paper.stable",
    "revision": 15,
    "sha256": "33b9cfd4ff19fee20560ee912192a880204789d99994dcb2dbfcd81ed60396fa"
   },
   {
    "mounted_file": "alpha.okx.sandbox.stable.yaml",
    "consumer_id": "alpha.okx.sandbox.stable",
    "revision": 1,
    "sha256": "4ec1d11b6c94055c81ce29c5d3bd73293f6e6d57621ddbd2bf8a03f4025b7afb"
   },
   {
    "mounted_file": "trading-system.live.stable.yaml",
    "consumer_id": "trading-system.live.stable",
    "revision": 1,
    "sha256": "2d5790522e59a2c5144ed453a8ce1740509eb3273278c58b757ca6e1d28977de"
   },
   {
    "mounted_file": "trading-system.paper.stable-r11.yaml",
    "consumer_id": "trading-system.paper.stable",
    "revision": 11,
    "sha256": "efd482fa4ffe7f9176387324f47d47d7e1c801bae6afb2c0747c3ce43652b8fc"
   },
   {
    "mounted_file": "trading-system.sandbox.stable.yaml",
    "consumer_id": "trading-system.sandbox.stable",
    "revision": 2,
    "sha256": "fa7073f164ea75232e035d2f620b172de1b65d635702ebfd026244ba66e63713"
   }
  ],
  "repository_origin_main": [
   {
    "path": "consumers/stable/alpha-binance-paper.yaml",
    "consumer_id": "alpha.binance.paper.stable",
    "revision": 16,
    "sha256": "050d96dcc6f1da5b42b373e05baae4e6d7965eb44d163e5a320302975ecd9269"
   },
   {
    "path": "consumers/stable/alpha-okx-paper.yaml",
    "consumer_id": "alpha.okx.paper.stable",
    "revision": 14,
    "sha256": "e8fb3ea783054effe6ee59356e1d1d2f9b7044ed1f40bae92cb94eee8710bd4c"
   },
   {
    "path": "consumers/stable/alpha-vn-paper.yaml",
    "consumer_id": "alpha.vn.paper.stable",
    "revision": 1,
    "sha256": "be7331aaf26479ec9d3e1208caf24fa93c467755553b9e7722b6944bfdec70d8"
   },
   {
    "path": "consumers/stable/monitoring-multivenue.yaml",
    "consumer_id": "monitoring.multivenue.stable",
    "revision": 2,
    "sha256": "304a610d033a458483b5bb0ea8ef170cdf042373c3f2c96f94d4f3754015c739"
   },
   {
    "path": "consumers/stable/reference-l2-stable.yaml",
    "consumer_id": "qdl.crypto.reference-l2.stable",
    "revision": 7,
    "sha256": "5b220aba0afa3c26ac4dfbc92a2d8768b3b77443d692fc4eaf9f94d5b67f84ad"
   },
   {
    "path": "consumers/stable/trading-system-paper.yaml",
    "consumer_id": "trading-system.paper.stable",
    "revision": 10,
    "sha256": "1e489237ad73fa3eeafdbd9decd0fbbbf9eefdee1a99a21c9b1409651fc6fe28"
   }
  ]
 },
 "catalog": {
  "runtime_bundle": {
   "file": "bundle.json",
   "sha256_file": "e8d934e1c0c2764495dab1373ac7171776e289c5c4a1e9cd709ce2f3b35044e2",
   "sha256_field": "63efd9ffb9a4cb9eca7a98bdd3189444fd1629674e3e0a883da557f939218c2c",
   "catalog_revision": 12,
   "bindings": 716
  },
  "repository_origin_main": {
   "path": "config/v2/stable-source-bindings.yaml",
   "catalog_revision": 11,
   "bindings": 712
  },
  "runtime_only_bindings": [
   "okx-swap-btc-usd-swap-book_delta",
   "okx-swap-btc-usd-swap-book_snapshot",
   "okx-swap-btc-usd-swap-mark_index_price",
   "okx-swap-btc-usd-swap-quote"
  ],
  "repository_only_bindings": []
 },
 "summary": {
  "images": "Every DL application image runs a source revision that is an ancestor of origin/main (9dab224, c7ce7d3, a4e7b3b, 31a5155, a388537; V1 2b0dcf7 with io.qdl.source-tree = 2b0dcf7^{tree}). Digests equal certificate.json release_images/runtime for all 18 certified rows. No code or image change is needed to make main describe the running binaries.",
  "post_revision_source_on_main": "rust after 9dab224: cfg(test) only in qdl-native-raw-ingestor.rs; projector binary whole-file EQUAL in a 9dab224 build (RUSTLS_0.23.46_ROLL_PACKET.md); rc22 core kept by owner decision (0 rustls code, 5 CODEGEN functions differ); python after 31a5155: qdl/certification/reference_l2_acceptance.py only (apparatus).",
  "caps": "Live cpu/memory caps equal certificate.json runtime and docker-compose.v2-stable.yml on main for every role that the compose defines under the same or mapped name.",
  "certificate_rows": "All 18 certificate.json runtime rows equal the live containers in image id, cpus, memory and StartedAt (no restart or recreate since the certificate re-issue); restart count 0 on every role."
 },
 "drift": [
  {
   "id": "R1",
   "severity": "medium",
   "kind": "topology definition",
   "fact": "docker-compose.v2-stable.yml on main defines the pre-KN roles query_v2_1/2, stream_v2_active/passive (python image, 2.0 vCPU), projector_v2..projector_v2_6 (python image, 1.0 vCPU / 768 MiB) and no market_cache. The running KN roles query_kn_1/2, stream_kn_1/2 (rust qdl-stream-gateway, 0.75 vCPU / 512 MiB), market_projector_1/2 (rust qdl-projector, 0.75 vCPU / 512 MiB) and market_cache (1.0 vCPU / 9 GiB) are defined only by per-role compose files under ~/.local/state/qdl-v2 generated from docker inspect (scripts/kn_production_packet.py service_from_inspect).",
   "repository_truth": "certificate.json runtime rows + this record",
   "fix": "config slice for the next release: carry the KN roles into a versioned compose (CI5 full run); not docs-only"
  },
  {
   "id": "R2",
   "severity": "medium",
   "kind": "consumer manifests / catalog (IDR-127)",
   "fact": "trading-system.paper.stable runtime r11 (efd482fa) vs consumers/stable r10 (1e489237); alpha.okx.paper.stable runtime r15 (33b9cfd4, not in the repository by sha256) vs r14 (e8fb3ea7); runtime catalog r12 (716 bindings) vs config/v2/stable-source-bindings.yaml r11 (712); the 4 runtime-only bindings are OKX BTC-USD-SWAP book_delta, book_snapshot, mark_index_price, quote. The 6 live/sandbox realm manifests are not in the repository by sha256 (their generator was not checked). alpha.binance.paper.stable r16 is byte-identical to the repository.",
   "repository_truth": "r11 byte copy in upgrade/evidence/releases/v2.2.4/; r15 and catalog r12 only by sha256 here",
   "fix": "code/config + tests slice (changes what the next python image bakes; bar edge checkpoint C.19/C.32): owner approval, next release"
  },
  {
   "id": "R3",
   "severity": "low",
   "kind": "held role",
   "fact": "binance_bar_edge runs qdl-v2-python:2.2.3-rc14-a388537 (held_scoped_out in the certificate); it bakes alpha-binance-paper r14 (main r16) and reference-l2-stable r6 (main r7).",
   "fix": "none until the bar-edge roll that IDR-127 plans"
  },
  {
   "id": "R4",
   "severity": "low",
   "kind": "stale create-compose snapshots",
   "fact": "The OPT caps were applied by docker update, so the compose files that created kafka1-3 (cpus 1.75), query_kn_1/2 (1.5 vCPU / 1.5 GiB) and ingestor_okx_swap (0.5 vCPU) still hold the pre-OPT caps. Recreating from those files would silently revert the caps; the packet path (service_from_inspect) reads the live caps.",
   "fix": "any recreate must build its compose from docker inspect, never reuse these files"
  },
  {
   "id": "R5",
   "severity": "low",
   "kind": "create-compose provenance",
   "fact": "stable_redis was created from a compose in the deleted worktree data-layer-dev-closure; market_cache from ~/.local/state/qdl-v2/releases/v2.2.0-02cd827/compose.json (root-only, not read); data_layer_service (V1) from docker-compose.yml + a deleted worktree copy of docker-compose.phase105c-v1-fallback.yml.",
   "fix": "docker start only (CLAUDE.md section 1); no compose up"
  },
  {
   "id": "R6",
   "severity": "low",
   "kind": "release tag",
   "fact": "qdl-v1-fallback:v1.2.4-2b0dcf7 (V1 fallback) has no git tag v1.2.4 on origin (remote V1 tags: v1.2.1, v1.2.2); its source 2b0dcf7 is on main.",
   "fix": "owner decision whether to tag; tags are release-only"
  },
  {
   "id": "R7",
   "severity": "info",
   "kind": "certificate field",
   "fact": "certificate.json publication_status NOT_TAGGED_CERTIFICATE_PASS_UNDER_POST_HOC_A5 predates the tag; the publication (tag v2.2.4 on b5ab3c2, release run 39214746630) is recorded in the plan journal. The published certificate is not edited.",
   "fix": "none"
  },
  {
   "id": "R8",
   "severity": "info",
   "kind": "cross-repo",
   "fact": "market_data_service_p184w (TS writer, 2deb90b0) runs ExecutorBroker revision 30f50a91, which is on origin/feat/v2-rust-first-okx-demo only, not on the Trading System main.",
   "fix": "Trading System reconciliation, outside this repository"
  }
 ],
 "not_changed": "No container, image, compose file, Redis, Postgres or Kafka state was touched; the published v2.2.4 tag and its evidence are unchanged."
}
```
