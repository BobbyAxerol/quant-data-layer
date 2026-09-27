//! Bars-topic cleaner (KN-3 K3.5 bounded cleaner, D11 amendment).
//!
//! Expiry tombstones every fact key it can see in the cache, but a fact can
//! reach the bars topic below its product's floor without the cache ever
//! holding it: a late fact refused as `STALE_BELOW_FLOOR`, a fact for an
//! expiring open written between the plan read and the floor, or any bug.
//! The cleaner treats the topic as the truth: per partition it reads the
//! committed records from the earliest offset up to the end offset captured
//! when the sweep starts, twice:
//!
//! * pass 1 keeps the last record of every `<lpk>|floor` key (a
//!   RETENTION_FLOOR frame, strictly decoded; a floor tombstone = no floor);
//! * pass 2 tracks, for every fact key `<lpk>|<open_ms>|...` whose open time
//!   is below its product's floor, whether its last record is a tombstone
//!   (memory is bounded by the below-floor keys only).
//!
//! Below-floor keys whose last record is not a tombstone are tombstoned on
//! the same partition in bounded transactions (`max_keys_per_txn`), at most
//! `max_tombstones` per sweep (the rest is reported pending). A key at or
//! above its floor, a floor key, and keys of a product without a floor are
//! never touched. A key that does not parse or a floor that does not decode
//! is a typed integrity error and nothing is published.
//!
//! Boundaries: an `assign()`-mode `read_committed` reader (its group id is
//! never joined nor committed, and is never the stage B group), and a transactional producer whose id
//! starts with [`CLEANER_TRANSACTIONAL_ID_PREFIX`] (under the
//! `kn-projector-v3-` ACL prefix). The market cache is never touched.

use crate::expiry::ExpirySink;
use crate::stage_b::StateInput;
use qdl_contracts::state_codec::{FrameKind, StateFrame};
use qdl_contracts::state_contract::LogicalProductKey;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{DefaultProducerContext, Producer, ThreadedProducer};
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

/// Transactional id prefix of the cleaner's producer (ACL `kn-projector-v3-`).
pub const CLEANER_TRANSACTIONAL_ID_PREFIX: &str = "kn-projector-v3-cleaner-";

/// The cleaner's transactional id for one replica.
pub fn cleaner_transactional_id(replica: &str) -> String {
    format!("{CLEANER_TRANSACTIONAL_ID_PREFIX}{replica}")
}

pub use crate::kafka_state::KafkaPartitionReader;
pub use crate::stage_b::PartitionReader;

#[derive(Clone, Debug)]
pub struct SweepLimits {
    pub max_keys_per_txn: usize,
    pub max_tombstones: usize,
    pub poll_batch: usize,
    pub poll_timeout: Duration,
    /// Deadline of one read pass (an open transaction holds the stable
    /// offset back: the pass then fails as incomplete, nothing published).
    pub read_timeout: Duration,
    pub transaction_timeout: Duration,
}

impl Default for SweepLimits {
    fn default() -> Self {
        Self {
            max_keys_per_txn: 500,
            max_tombstones: 10_000,
            poll_batch: 1_000,
            poll_timeout: Duration::from_millis(100),
            read_timeout: Duration::from_secs(120),
            transaction_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Records read over both passes.
    pub records_read: u64,
    /// Key + value bytes read over both passes.
    pub bytes_read: u64,
    /// Products with a floor on the partition.
    pub floors: u64,
    /// Distinct fact keys below their product's floor.
    pub below_floor_keys: u64,
    /// Of those, keys whose last record already is a tombstone.
    pub already_tombstoned: u64,
    pub tombstones_published: u64,
    pub transactions: u64,
    /// Below-floor live keys left for the next sweep (`max_tombstones`).
    pub pending: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CleanerError {
    /// A key that does not parse or a floor that does not decode: the sweep
    /// stops before publishing anything.
    Integrity {
        partition: i32,
        offset: i64,
        key: String,
        reason: String,
    },
    Source(String),
    /// A read pass did not reach the captured end offset in time.
    Incomplete {
        partition: i32,
        reached: i64,
        end: i64,
    },
    /// A tombstone transaction was not committed (it was aborted).
    Publish(String),
}

/// A bars-topic key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BarsKey {
    Floor { lpk: String },
    Fact { lpk: String, open_ms: u64 },
}

fn is_hex16(text: &str) -> bool {
    text.len() == 16
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_u64(text: &str) -> Option<u64> {
    let value: u64 = text.parse().ok()?;
    (value.to_string() == text).then_some(value)
}

/// Strict parse of a bars-topic key: `<lpk>|floor`, `<lpk>|<ms>|p` or
/// `<lpk>|<ms>|f<rev>|<sha16>` with a canonical BAR LPK.
pub fn parse_bars_key(key: &[u8]) -> Result<BarsKey, String> {
    let text = std::str::from_utf8(key).map_err(|_| "key is not UTF-8".to_owned())?;
    let parts: Vec<&str> = text.split('|').collect();
    if parts.len() < 8 {
        return Err("not a bars-topic key".into());
    }
    let lpk = parts[..7].join("|");
    let parsed = LogicalProductKey::parse(&lpk)?;
    if parsed.feed != "BAR" {
        return Err("not a BAR product".into());
    }
    match &parts[7..] {
        ["floor"] => Ok(BarsKey::Floor { lpk }),
        [open, "p"] => Ok(BarsKey::Fact {
            open_ms: canonical_u64(open).ok_or("open time is not canonical")?,
            lpk,
        }),
        [open, revision, sha]
            if revision.len() > 1
                && revision.starts_with('f')
                && revision[1..].parse::<u32>().is_ok()
                && is_hex16(sha) =>
        {
            Ok(BarsKey::Fact {
                open_ms: canonical_u64(open).ok_or("open time is not canonical")?,
                lpk,
            })
        }
        _ => Err("not a bars-topic key".into()),
    }
}

fn integrity(record: &StateInput, reason: impl Into<String>) -> CleanerError {
    CleanerError::Integrity {
        partition: record.partition,
        offset: record.offset,
        key: String::from_utf8_lossy(&record.key).into_owned(),
        reason: reason.into(),
    }
}

/// Read `[earliest, end)` of the partition once, calling `visit` per record.
fn read_pass<R: PartitionReader + ?Sized>(
    reader: &mut R,
    topic: &str,
    partition: i32,
    (earliest, end): (i64, i64),
    limits: &SweepLimits,
    report: &mut SweepReport,
    mut visit: impl FnMut(&StateInput) -> Result<(), CleanerError>,
) -> Result<(), CleanerError> {
    if end <= earliest {
        return Ok(());
    }
    reader
        .start(topic, partition, earliest)
        .map_err(CleanerError::Source)?;
    let deadline = Instant::now() + limits.read_timeout;
    let mut reached = earliest;
    loop {
        let batch = reader
            .poll(limits.poll_batch.max(1), limits.poll_timeout)
            .map_err(CleanerError::Source)?;
        for record in batch {
            if record.topic != topic || record.partition != partition || record.offset >= end {
                continue;
            }
            reached = reached.max(record.offset + 1);
            report.records_read += 1;
            report.bytes_read +=
                (record.key.len() + record.value.as_ref().map_or(0, Vec::len)) as u64;
            visit(&record)?;
        }
        if let Some(position) = reader
            .position(topic, partition)
            .map_err(CleanerError::Source)?
        {
            reached = reached.max(position);
        }
        if reached >= end {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(CleanerError::Incomplete {
                partition,
                reached,
                end,
            });
        }
    }
}

/// One sweep of one bars-topic partition (see the module header).
pub fn sweep_partition<R: PartitionReader + ?Sized, S: ExpirySink + ?Sized>(
    reader: &mut R,
    topic: &str,
    partition: i32,
    sink: &S,
    limits: &SweepLimits,
) -> Result<SweepReport, CleanerError> {
    let mut report = SweepReport::default();
    let bounds = reader
        .watermarks(topic, partition)
        .map_err(CleanerError::Source)?;

    // Pass 1: the last record of every floor key; every key must parse.
    let mut floors: HashMap<String, Option<u64>> = HashMap::new();
    read_pass(
        reader,
        topic,
        partition,
        bounds,
        limits,
        &mut report,
        |record| {
            let key = parse_bars_key(&record.key).map_err(|reason| integrity(record, reason))?;
            let BarsKey::Floor { lpk } = key else {
                return Ok(());
            };
            let floor = match &record.value {
                None => None,
                Some(value) => {
                    let frame = StateFrame::decode(value)
                        .map_err(|error| integrity(record, error.to_string()))?;
                    if frame.kind != FrameKind::RetentionFloor || frame.lpk.encode() != lpk {
                        return Err(integrity(record, "not this product's RETENTION_FLOOR"));
                    }
                    Some(
                        frame
                            .floor_open_time_ms
                            .ok_or_else(|| integrity(record, "floor without an open time"))?,
                    )
                }
            };
            floors.insert(lpk, floor);
            Ok(())
        },
    )?;
    let floors: HashMap<String, u64> = floors
        .into_iter()
        .filter_map(|(lpk, floor)| floor.map(|floor| (lpk, floor)))
        .collect();
    report.floors = floors.len() as u64;
    if floors.is_empty() {
        return Ok(report);
    }

    // Pass 2: below-floor fact keys -> is the last record a tombstone?
    let mut below: BTreeMap<Vec<u8>, bool> = BTreeMap::new();
    read_pass(
        reader,
        topic,
        partition,
        bounds,
        limits,
        &mut report,
        |record| {
            let key = parse_bars_key(&record.key).map_err(|reason| integrity(record, reason))?;
            if let BarsKey::Fact { lpk, open_ms } = key {
                if floors.get(&lpk).is_some_and(|floor| open_ms < *floor) {
                    below.insert(record.key.clone(), record.value.is_none());
                }
            }
            Ok(())
        },
    )?;
    report.below_floor_keys = below.len() as u64;
    report.already_tombstoned = below.values().filter(|dead| **dead).count() as u64;
    let live: Vec<Vec<u8>> = below
        .into_iter()
        .filter(|(_, dead)| !dead)
        .map(|(key, _)| key)
        .collect();
    let now = live.len().min(limits.max_tombstones);
    report.pending = (live.len() - now) as u64;

    for chunk in live[..now].chunks(limits.max_keys_per_txn.max(1)) {
        sink.begin().map_err(CleanerError::Publish)?;
        let sent = (|| {
            for key in chunk {
                let key = std::str::from_utf8(key).map_err(|error| error.to_string())?;
                sink.send_record(topic, partition, key, None, limits.transaction_timeout)?;
            }
            sink.commit(limits.transaction_timeout)
        })();
        if let Err(error) = sent {
            let abort = sink.abort(limits.transaction_timeout);
            return Err(CleanerError::Publish(match abort {
                Ok(()) => format!("cleaner transaction aborted: {error}"),
                Err(abort) => format!("cleaner transaction failed: {error}; abort failed: {abort}"),
            }));
        }
        report.transactions += 1;
        report.tombstones_published += chunk.len() as u64;
    }
    Ok(report)
}

// ------------------------------------------------------------ Kafka

#[derive(Clone, Debug)]
pub struct CleanerKafkaSettings {
    pub bootstrap: String,
    /// Replica name: client id `kn-projector-v3-cleaner-<replica>` and the
    /// transactional id [`cleaner_transactional_id`].
    pub replica: String,
    /// `(ca, certificate, key)` files; `None` = plaintext (isolated broker only).
    pub tls: Option<(String, String, String)>,
    pub transaction_timeout: Duration,
}

impl CleanerKafkaSettings {
    fn base(&self) -> ClientConfig {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", &self.bootstrap)
            .set("client.id", cleaner_transactional_id(&self.replica))
            .set("socket.timeout.ms", "10000");
        if let Some((ca, certificate, key)) = &self.tls {
            config
                .set("security.protocol", "ssl")
                .set("ssl.ca.location", ca)
                .set("ssl.certificate.location", certificate)
                .set("ssl.key.location", key)
                .set("ssl.endpoint.identification.algorithm", "https");
        }
        config
    }

    /// The transactional tombstone producer, transactions initialized
    /// (fences an older instance of the same replica).
    pub fn open_producer(&self) -> Result<ThreadedProducer<DefaultProducerContext>, String> {
        let producer: ThreadedProducer<DefaultProducerContext> = self
            .base()
            .set("transactional.id", cleaner_transactional_id(&self.replica))
            .set("enable.idempotence", "true")
            .set(
                "transaction.timeout.ms",
                self.transaction_timeout.as_millis().to_string(),
            )
            .create()
            .map_err(|error| format!("cleaner producer: {error}"))?;
        producer
            .init_transactions(self.transaction_timeout)
            .map_err(|error| format!("cleaner init_transactions: {error}"))?;
        Ok(producer)
    }

    pub fn open_reader(&self) -> Result<KafkaPartitionReader, String> {
        // The id stays under the projector's `kn-projector-v3-` prefix.
        KafkaPartitionReader::open(
            &self.bootstrap,
            &cleaner_transactional_id(&self.replica),
            self.tls.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lpk() -> String {
        LogicalProductKey::new("paper", "OKX", "SWAP", "u1", "BAR", Some("1m"))
            .unwrap()
            .encode()
    }

    #[test]
    fn bars_keys_parse_strictly() {
        let lpk = lpk();
        assert_eq!(
            parse_bars_key(format!("{lpk}|floor").as_bytes()),
            Ok(BarsKey::Floor { lpk: lpk.clone() })
        );
        assert_eq!(
            parse_bars_key(format!("{lpk}|60000|p").as_bytes()),
            Ok(BarsKey::Fact {
                lpk: lpk.clone(),
                open_ms: 60_000
            })
        );
        assert_eq!(
            parse_bars_key(format!("{lpk}|60000|f2|0123456789abcdef").as_bytes()),
            Ok(BarsKey::Fact {
                lpk: lpk.clone(),
                open_ms: 60_000
            })
        );
        let quote = LogicalProductKey::new("paper", "OKX", "SWAP", "u1", "QUOTE", None)
            .unwrap()
            .encode();
        for bad in [
            lpk.clone(),
            format!("{lpk}|060000|p"),
            format!("{lpk}|60000|x"),
            format!("{lpk}|60000|f|0123456789abcdef"),
            format!("{lpk}|60000|f1|0123456789ABCDEF"),
            format!("{lpk}|60000|f1|0123"),
            format!("{lpk}|60000|p|extra"),
            format!("{quote}|floor"),
            "garbage".into(),
        ] {
            assert!(parse_bars_key(bad.as_bytes()).is_err(), "{bad}");
        }
        assert!(parse_bars_key(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn the_transactional_id_stays_under_the_projector_acl_prefix() {
        assert!(cleaner_transactional_id("r0").starts_with("kn-projector-v3-"));
        assert_eq!(cleaner_transactional_id("r0"), "kn-projector-v3-cleaner-r0");
    }
}
