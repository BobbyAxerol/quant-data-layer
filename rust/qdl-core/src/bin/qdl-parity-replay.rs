#![forbid(unsafe_code)]

use std::env;
use std::fs;
use std::time::Instant;

use qdl_core::canonical::{canonical_bytes, TradeFixture};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayBundle {
    fixtures: Vec<TradeFixture>,
    repeat: usize,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .ok_or("usage: qdl-parity-replay BUNDLE.json")?;
    let bundle: ReplayBundle = serde_json::from_slice(&fs::read(path)?)?;
    if bundle.fixtures.is_empty() || bundle.repeat == 0 || bundle.repeat > 10_000 {
        return Err("fixtures and bounded repeat are required".into());
    }
    let started = Instant::now();
    let mut aggregate = Sha256::new();
    let mut record_hashes = Vec::with_capacity(bundle.fixtures.len());
    let mut bytes = 0_u64;
    for iteration in 0..bundle.repeat {
        for fixture in &bundle.fixtures {
            let canonical = canonical_bytes(fixture)
                .map_err(|error| format!("canonicalize {}: {error}", fixture.provider_kind))?;
            let length = u64::try_from(canonical.len())?;
            aggregate.update(length.to_be_bytes());
            aggregate.update(&canonical);
            bytes = bytes.saturating_add(length);
            if iteration == 0 {
                record_hashes.push(format!("{:x}", Sha256::digest(&canonical)));
            }
        }
    }
    let elapsed = started.elapsed().as_secs_f64();
    let events = bundle.fixtures.len() * bundle.repeat;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "status": "PASS",
            "events": events,
            "bytes": bytes,
            "elapsed_seconds": elapsed,
            "events_per_second": events as f64 / elapsed.max(f64::EPSILON),
            "aggregate_sha256": format!("{:x}", aggregate.finalize()),
            "record_sha256": record_hashes,
        }))?
    );
    Ok(())
}
