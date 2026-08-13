#![forbid(unsafe_code)]

use std::env;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use qdl_core::backoff::BackoffPolicy;
use qdl_core::binance::{
    canonicalize_frame, combined_url, decode_combined, exchange_info_has_active_symbol,
    ShadowConfig, WalRecord,
};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;

fn now_ns() -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64)
}

async fn durable_writer(
    path: String,
    mut receiver: mpsc::Receiver<WalRecord>,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    let mut written = 0;
    while let Some(record) = receiver.recv().await {
        file.write_all(&serde_json::to_vec(&record)?).await?;
        file.write_all(b"\n").await?;
        file.sync_data().await?;
        written += 1;
    }
    Ok(written)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "failed to install rustls ring crypto provider")?;
    let config_path = env::args()
        .nth(1)
        .ok_or("usage: qdl-binance-shadow CONFIG.json")?;
    let config: ShadowConfig = serde_json::from_slice(&tokio::fs::read(config_path).await?)?;
    if config.max_events == 0 {
        return Err("max_events must be positive".into());
    }
    let exchange_info: serde_json::Value =
        serde_json::from_slice(&tokio::fs::read(&config.exchange_info_path).await?)?;
    if !exchange_info_has_active_symbol(&exchange_info, &config.context.native_symbol) {
        return Err("configured instrument is not active in real Binance exchangeInfo".into());
    }
    let url = combined_url(&config.streams)?;
    let (sender, receiver) = mpsc::channel::<WalRecord>(512);
    let writer = tokio::spawn(durable_writer(config.wal_path.clone(), receiver));
    let context = Arc::new(config.context.clone());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(config.timeout_seconds);
    let backoff = BackoffPolicy {
        initial_ms: 250,
        maximum_ms: 5_000,
        multiplier: 2,
        jitter_bps: 2_000,
    }
    .validate()?;
    let mut accepted = 0usize;
    let mut attempt = 0u32;

    while accepted < config.max_events && tokio::time::Instant::now() < deadline {
        match connect_async(&url).await {
            Ok((socket, _)) => {
                attempt = 0;
                let (_, mut reader) = socket.split();
                while accepted < config.max_events {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let message = match tokio::time::timeout(remaining, reader.next()).await {
                        Ok(Some(Ok(message))) => message,
                        Ok(Some(Err(_))) | Ok(None) | Err(_) => break,
                    };
                    if !message.is_text() {
                        continue;
                    }
                    let frame = decode_combined(message.into_text()?.to_string())?;
                    let record = canonicalize_frame(
                        &frame,
                        (*context).clone(),
                        accepted as u64 + 1,
                        now_ns()?,
                    )?;
                    sender
                        .send(record)
                        .await
                        .map_err(|_| "durable writer stopped")?;
                    accepted += 1;
                }
            }
            Err(_) => {
                attempt = attempt.saturating_add(1);
                let delay_ms = backoff.delay_ms(attempt, attempt.min(10_000) as u16);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }
    drop(sender);
    let written = writer.await??;
    if written != accepted || accepted != config.max_events {
        return Err(format!("shadow WAL incomplete accepted={accepted} written={written}").into());
    }
    println!("{{\"status\":\"PASS\",\"events\":{written},\"production_writes\":0}}");
    Ok(())
}
