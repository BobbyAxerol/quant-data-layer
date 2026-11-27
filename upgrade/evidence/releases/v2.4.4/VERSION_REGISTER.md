# DL v2.4.4 version register

One row per image built, and per role rolled at runtime. Append-only; newest last. Runtime rows are added by whoever
runs the window, with its receipt.

## Images built

| Time (UTC) | Image | Digest (image id) | Source SHA | Tree | Receipt | R0 |
|---|---|---|---|---|---|---|
| 2026-11-27 10:47:06-10:53:22 | `qdl-v2-python:2.4.4-13850b1` | `sha256:0e14a5f4cb8bb732a4158d39eeb8cd5525cbfde9aaf708e143ceca846b2a69ce` | `13850b10fad9ca40f915dc26a8cc17b82e8c4b09` (fix/dl-def-o-reference-timeout) | `163d9ae2`-line + DEF-O (code tree = `4c7bf745` = `2af49510`) | `~/.local/state/qdl-v2/build-v2.4.4-13850b1/receipt.json` status PASS; python file verdicts OK vs Query `5ea7a7a4` (24 expected-different files) and bar_edge `396a4654` (22); pip freeze equal to both | trivy 0 fixable CRITICAL/HIGH; pip-audit: no known vulnerabilities |
| 2026-11-27 10:47:06-10:53:22 | `qdl-v2-rust:2.4.4-13850b1` | `sha256:8729292b06d4751de2730a36314536781c0eb2eb883b30cba892c82cb6c840d1` | same | same | same receipt: binary verdicts ok, 0 unexpected (stream gateway / ingestor / qdl-kafka / projector DIFFERENT, qdl-core + venue-core EQUAL vs `e628fbe0`), feature markers ok | **FAIL**: trivy 4 fixable HIGH, CVE-2025-6020 (libpam 1.5.2-6+deb12u2 → deb12u3) in the runtime base `b33ec0cb` (debian 12.15, built 2026-11-18); the running rust image `e628fbe0` has the same 4 (pre-existing). Base rebuild (`--build-base`, apt upgrade) needed before any rust role roll |

## Runtime rolls

| Time (UTC) | Role | From (rollback) | To | Config-hash before → after | Receipt |
|---|---|---|---|---|---|
| (pending) | query_kn_2 | `qdl-v2-python@sha256:5ea7a7a4…` (2.4.2-48137eb), hash `a0eff1c8…` | `sha256:0e14a5f4…` (2.4.4-13850b1) | `a0eff1c8…` → `a1c2d29e…` | `~/.local/state/qdl-v2/windows/v2.4.4-query/run.out` |
| (pending) | query_kn_1 | `5ea7a7a4…`, hash `535ec6cc…` | `0e14a5f4…` | `535ec6cc…` → `ac9bb831…` | same |

Next on this line: PROVIDER_CONFIRMATION (`feat/dl-provider-confirmation`), r18 declaration, the rust image after the base
rebuild (stream_kn x2, ingestor_okx_swap, optional ingestor_binance_usdm).
