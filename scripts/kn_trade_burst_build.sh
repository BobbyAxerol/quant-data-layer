#!/bin/sh
# Run inside the existing Rust builder with --cpus 2 --memory 2g --memory-swap 2g.
set -eu
export CARGO_BUILD_JOBS=2
rustfmt --edition 2021 rust/qdl-stream-gateway/examples/kn_trade_burst_gateway.rs
cargo test --locked -j 2 -p qdl-stream-gateway --lib --test native_stream -- --test-threads=2
cargo build --locked -j 2 -p qdl-stream-gateway --example kn_trade_burst_gateway
