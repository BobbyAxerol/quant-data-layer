#![forbid(unsafe_code)]

use std::env;
use std::fs;

use qdl_core::canonical::{canonical_bytes, TradeFixture};
use serde_json::json;
use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::args()
        .nth(1)
        .ok_or("usage: qdl-fixture-check FIXTURE.json")?;
    let fixture: TradeFixture = serde_json::from_slice(&fs::read(&path)?)?;
    let canonical = canonical_bytes(&fixture).map_err(|error| format!("canonical: {error}"))?;
    println!(
        "{}",
        json!({
            "canonical_bytes": canonical.len(),
            "provider_kind": fixture.provider_kind,
            "sha256": format!("{:x}", Sha256::digest(canonical)),
            "status": "PASS"
        })
    );
    Ok(())
}
