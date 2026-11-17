//! Provider-session liveness for Subscribe session-alive frames (DL v2.3.2).
//!
//! Purpose: a Subscribe stream for an event-recency OBSERVE QUOTE/TRADE
//! requirement may ask (`SubscribeRequest.session_alive_interval_ms`) for
//! typed evidence, at most every 250..500 ms, that the provider session
//! carrying its instrument is LIVE even when the instrument itself is quiet
//! (`qdl.stream.session-alive.v1`, `query.proto` `SessionAlive`).
//!
//! The evidence is the record each Rust ingestor lane already writes by
//! atomic rename into the shared `stable_state` volume
//! (`runtime/session-liveness/<venue>-<market>/<lane>.json`,
//! `qdl.provider-session-liveness.v1`, `qdl-native-raw-ingestor.rs`
//! `SessionLivenessWriter`). [`SessionLivenessReader::status`] is a port of
//! `qdl/runtime/session_liveness.py` `StableSessionLivenessReader.status` with
//! the same record validation, file-name rule, caps and failure flags, and the
//! same `(inode, mtime, size)` content cache (contents only, never a verdict).
//!
//! Boundary and deliberate differences, all fail closed:
//! * the lineage successor (`SOURCE_SESSION_CONTINUED`) is not ported: a frame
//!   names exactly the session and generation of the instrument's last
//!   delivered event, so a make-before-break successor gets frames only once
//!   the instrument's first event on it is delivered;
//! * [`session_alive`] adds the frame rules: state LIVE, transport age at most
//!   3 x the interval and within the requirement's session SLA.
//!
//! Read-only: lists and reads files under the configured root; never writes.

use crate::generated::marketdata_v2::EventEnvelope;
use crate::generated::query_v2 as query;
use std::collections::HashMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Schema name of the stream frame (`query.proto` `SessionAlive`).
pub const SESSION_ALIVE_SCHEMA: &str = "qdl.stream.session-alive.v1";
/// Schema of the ingestor record (`qdl/runtime/session_liveness.py`).
pub const SESSION_LIVENESS_SCHEMA: &str = "qdl.provider-session-liveness.v1";
/// Cadence bounds a server clamps `session_alive_interval_ms` to.
pub const MIN_SESSION_ALIVE_INTERVAL_MS: u32 = 250;
pub const MAX_SESSION_ALIVE_INTERVAL_MS: u32 = 500;
/// A session record whose transport time is older than this many intervals
/// proves nothing for a frame.
pub const MAX_RECORD_AGE_INTERVALS: u64 = 3;

const MAX_STATE_FILES: usize = 256;
const MAX_SESSION_ID_LENGTH: usize = 256;
const MAX_RECORD_BYTES: u64 = 4096;

/// `0` = not requested; otherwise the clamped cadence.
pub fn clamp_interval(requested_ms: u32) -> Option<Duration> {
    (requested_ms != 0).then(|| {
        Duration::from_millis(u64::from(
            requested_ms.clamp(MIN_SESSION_ALIVE_INTERVAL_MS, MAX_SESSION_ALIVE_INTERVAL_MS),
        ))
    })
}

/// Whether a requirement may ask for frames: feed QUOTE or TRADE with an
/// explicit event-recency OBSERVE route (which also carries a session SLA).
pub fn requirement_allows_frames(feed: &str, event_recency_policy: Option<&str>) -> bool {
    matches!(feed, "QUOTE" | "TRADE") && event_recency_policy == Some("OBSERVE")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionState {
    Live,
    Disconnected,
}

/// One validated `qdl.provider-session-liveness.v1` record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionRecord {
    pub source_session_id: String,
    pub connection_generation: u64,
    pub state: SessionState,
    pub last_transport_at_ns: i64,
    pub updated_at_ns: i64,
    pub config_revision: u64,
}

impl SessionRecord {
    /// `ProviderSessionLiveness.from_mapping` + `__post_init__`: exactly the
    /// seven keys, the schema, integer (never boolean or float) numbers and
    /// the same bounds.
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_slice(bytes).ok()?;
        let object = value.as_object()?;
        const KEYS: [&str; 7] = [
            "schema",
            "source_session_id",
            "connection_generation",
            "state",
            "last_transport_at_ns",
            "updated_at_ns",
            "config_revision",
        ];
        if object.len() != KEYS.len() || !KEYS.iter().all(|key| object.contains_key(*key)) {
            return None;
        }
        if object["schema"].as_str()? != SESSION_LIVENESS_SCHEMA {
            return None;
        }
        let session = object["source_session_id"].as_str()?.to_owned();
        let state = match object["state"].as_str()?.to_ascii_uppercase().as_str() {
            "LIVE" => SessionState::Live,
            "DISCONNECTED" => SessionState::Disconnected,
            _ => return None,
        };
        // serde_json keeps integers apart from floats and booleans.
        let integer = |key: &str| object[key].as_i64();
        let unsigned = |key: &str| object[key].as_u64();
        let record = Self {
            source_session_id: session,
            connection_generation: unsigned("connection_generation")?,
            state,
            last_transport_at_ns: integer("last_transport_at_ns")?,
            updated_at_ns: integer("updated_at_ns")?,
            config_revision: unsigned("config_revision")?,
        };
        let valid = !record.source_session_id.is_empty()
            && record.source_session_id.chars().count() <= MAX_SESSION_ID_LENGTH
            && record.connection_generation >= 1
            && record.last_transport_at_ns >= 1
            && record.updated_at_ns >= record.last_transport_at_ns
            && record.config_revision >= 1;
        valid.then_some(record)
    }
}

/// `StableSessionLivenessReader.status` result. `Known` carries the matched
/// record and its transport age judged after the read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionStatus {
    Known {
        record: SessionRecord,
        liveness_ms: u64,
    },
    /// The Python flag the lookup failed with (`SOURCE_SESSION_*`).
    Unknown(&'static str),
}

/// `_SAFE_FILE = ^[a-z0-9][a-z0-9._-]{0,119}\.json$`.
fn safe_file(name: &str) -> bool {
    let Some(stem) = name.strip_suffix(".json") else {
        return false;
    };
    let bytes = stem.as_bytes();
    (1..=120).contains(&bytes.len())
        && matches!(bytes[0], b'a'..=b'z' | b'0'..=b'9')
        && bytes[1..]
            .iter()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-'))
}

/// `_directory`: `<venue>-<market>`, each lower-cased `[a-z0-9]+`.
fn directory_name(venue: &str, market: &str) -> Option<String> {
    let venue = venue.trim().to_ascii_lowercase();
    let market = market.trim().to_ascii_lowercase();
    let ok = |value: &str| !value.is_empty() && value.bytes().all(|b| b.is_ascii_alphanumeric());
    (ok(&venue) && ok(&market)).then(|| format!("{venue}-{market}"))
}

type FileKey = (u64, i64, u64);
type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

fn wall_clock_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos() as i64)
        .unwrap_or_default()
}

/// Bounded, session-scoped liveness lookups under one root directory.
pub struct SessionLivenessReader {
    root: PathBuf,
    clock: Clock,
    /// Parsed contents by directory and file name, keyed by the file's
    /// `(inode, mtime_ns, size)`; only the files listed by the last scan.
    parsed: Mutex<HashMap<String, HashMap<String, (FileKey, Option<SessionRecord>)>>>,
}

impl std::fmt::Debug for SessionLivenessReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionLivenessReader")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl SessionLivenessReader {
    /// Records are judged at the wall clock sampled after the read.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_clock(root, Arc::new(wall_clock_ns))
    }

    /// Same, with the caller's clock (tests).
    pub fn with_clock(root: impl Into<PathBuf>, clock: Clock) -> Self {
        Self {
            root: root.into(),
            clock,
            parsed: Mutex::new(HashMap::new()),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The records of `names` in `directory`, each re-read only when its
    /// `(inode, mtime, size)` changed (the ingestor replaces a record by
    /// rename, so the key names one content).
    fn records(
        &self,
        directory: &Path,
        cache_key: &str,
        names: &[(String, FileKey)],
    ) -> Vec<Option<SessionRecord>> {
        let Ok(mut cache) = self.parsed.lock() else {
            return vec![None; names.len()];
        };
        let previous = cache.remove(cache_key).unwrap_or_default();
        let mut current = HashMap::with_capacity(names.len());
        let mut records = Vec::with_capacity(names.len());
        for (name, key) in names {
            let record = match previous.get(name) {
                Some((cached, record)) if cached == key => record.clone(),
                _ => {
                    let path = directory.join(name);
                    if key.2 > MAX_RECORD_BYTES {
                        None
                    } else {
                        std::fs::read(&path)
                            .ok()
                            .and_then(|bytes| SessionRecord::parse(&bytes))
                    }
                }
            };
            current.insert(name.clone(), (*key, record.clone()));
            records.push(record);
        }
        cache.insert(cache_key.to_owned(), current);
        records
    }

    /// Port of `StableSessionLivenessReader.status` (no lineage successor).
    pub fn status(
        &self,
        venue: &str,
        market: &str,
        source_session_id: &str,
        connection_generation: u64,
        config_revision: u64,
        now_ns: i64,
    ) -> SessionStatus {
        if source_session_id.is_empty()
            || source_session_id.chars().count() > MAX_SESSION_ID_LENGTH
            || connection_generation < 1
            || config_revision < 1
            || now_ns < 1
        {
            return SessionStatus::Unknown("SOURCE_SESSION_INVALID");
        }
        let Some(name) = directory_name(venue, market) else {
            return SessionStatus::Unknown("SOURCE_SESSION_INVALID");
        };
        let directory = self.root.join(&name);
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return SessionStatus::Unknown("SOURCE_SESSION_UNAVAILABLE")
            }
            Err(_) => return SessionStatus::Unknown("SOURCE_SESSION_UNREADABLE"),
        };
        let mut candidates: Vec<(String, FileKey)> = Vec::new();
        for entry in entries {
            let Ok(entry) = entry else {
                return SessionStatus::Unknown("SOURCE_SESSION_UNREADABLE");
            };
            let Ok(file_name) = entry.file_name().into_string() else {
                continue;
            };
            if !safe_file(&file_name) {
                continue;
            }
            // `DirEntry.is_file()` in Python follows symlinks; so does this.
            let Ok(meta) = std::fs::metadata(entry.path()) else {
                continue;
            };
            if !meta.is_file() {
                continue;
            }
            candidates.push((
                file_name,
                (
                    meta.ino(),
                    meta.mtime() * 1_000_000_000 + meta.mtime_nsec(),
                    meta.size(),
                ),
            ));
            if candidates.len() > MAX_STATE_FILES {
                return SessionStatus::Unknown("SOURCE_SESSION_UNAVAILABLE");
            }
        }
        candidates.sort();
        if candidates.is_empty() {
            if let Ok(mut cache) = self.parsed.lock() {
                cache.remove(&name);
            }
            return SessionStatus::Unknown("SOURCE_SESSION_UNAVAILABLE");
        }
        let mut matched: Option<SessionRecord> = None;
        let mut malformed = false;
        for record in self.records(&directory, &name, &candidates) {
            let Some(record) = record else {
                malformed = true;
                continue;
            };
            if record.source_session_id == source_session_id
                && record.connection_generation == connection_generation
            {
                if matched.is_some() {
                    return SessionStatus::Unknown("SOURCE_SESSION_AMBIGUOUS");
                }
                matched = Some(record);
            }
        }
        let Some(record) = matched else {
            return SessionStatus::Unknown(if malformed {
                "SOURCE_SESSION_MALFORMED"
            } else {
                "SOURCE_SESSION_UNAVAILABLE"
            });
        };
        // DL v2.3.0: an event carries its binding's pinned lineage revision,
        // which may be lower than the session's; only a session older than
        // its event is refused (session_liveness.py, W1 2026-11-13).
        if record.config_revision < config_revision {
            return SessionStatus::Unknown("SOURCE_SESSION_CONFIG_MISMATCH");
        }
        let evaluated = now_ns.max((self.clock)());
        if record.last_transport_at_ns > evaluated {
            return SessionStatus::Unknown("SOURCE_SESSION_CLOCK_SKEW");
        }
        let liveness_ms = ((evaluated - record.last_transport_at_ns) / 1_000_000) as u64;
        SessionStatus::Known {
            record,
            liveness_ms,
        }
    }
}

/// The session identity of the instrument's last delivered event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventSession {
    pub venue: String,
    pub market: String,
    pub source_session_id: String,
    pub connection_generation: u64,
    pub config_revision: u64,
}

impl EventSession {
    /// `None` for an envelope without a provider session (never vouched for).
    pub fn of(envelope: &EventEnvelope) -> Option<Self> {
        (!envelope.source_session_id.is_empty()
            && envelope.connection_generation >= 1
            && envelope.config_revision >= 1
            && u32::try_from(envelope.config_revision).is_ok())
        .then(|| Self {
            venue: envelope.venue.clone(),
            market: envelope.market.clone(),
            source_session_id: envelope.source_session_id.clone(),
            connection_generation: envelope.connection_generation,
            config_revision: envelope.config_revision,
        })
    }
}

/// Why no frame was built this tick (diagnostics only; silence is the signal).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Withheld {
    NoEvent,
    Session(&'static str),
    NotLive,
    Stale { liveness_ms: u64 },
}

/// The frame for this tick, or why there is none: the session of the last
/// delivered event must be LIVE, its transport at most
/// [`MAX_RECORD_AGE_INTERVALS`] x `interval` old and within the
/// requirement's session SLA (`max_session_liveness_ms`).
#[allow(clippy::too_many_arguments)]
pub fn session_alive(
    reader: &SessionLivenessReader,
    last: Option<&EventSession>,
    interval: Duration,
    max_session_liveness_ms: Option<u64>,
    gateway_epoch: u64,
    high_watermark: u64,
    now_ns: i64,
) -> Result<query::SessionAlive, Withheld> {
    let last = last.ok_or(Withheld::NoEvent)?;
    let (record, liveness_ms) = match reader.status(
        &last.venue,
        &last.market,
        &last.source_session_id,
        last.connection_generation,
        last.config_revision,
        now_ns,
    ) {
        SessionStatus::Known {
            record,
            liveness_ms,
        } => (record, liveness_ms),
        SessionStatus::Unknown(flag) => return Err(Withheld::Session(flag)),
    };
    if record.state != SessionState::Live {
        return Err(Withheld::NotLive);
    }
    let bound = (interval.as_millis() as u64) * MAX_RECORD_AGE_INTERVALS;
    let bound = max_session_liveness_ms.map_or(bound, |sla| bound.min(sla));
    if liveness_ms > bound {
        return Err(Withheld::Stale { liveness_ms });
    }
    Ok(query::SessionAlive {
        source_session_id: record.source_session_id,
        connection_generation: record.connection_generation,
        // EventSession::of admits only revisions that fit u32.
        config_revision: last.config_revision as u32,
        provider_last_transport_at_ns: record.last_transport_at_ns as u64,
        provider_session_liveness_ms: liveness_ms,
        gateway_epoch,
        high_watermark,
    })
}

/// The stream record of a frame: logical_offset 0 and the subscription's
/// current resume token, unchanged, so it can never move a cursor.
pub fn session_alive_record(frame: query::SessionAlive, resume_token: &str) -> query::StreamRecord {
    query::StreamRecord {
        logical_offset: 0,
        resume_token: resume_token.to_owned(),
        payload: Some(query::stream_record::Payload::SessionAlive(frame)),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    const NOW: i64 = 1_800_000_000_000_000_000;
    const MS: i64 = 1_000_000;

    struct Dir(PathBuf);

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn temp_root(tag: &str) -> Dir {
        let base = std::env::var_os("QDL_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let path = base.join(format!(
            "qdl-session-alive-{tag}-{}-{}",
            std::process::id(),
            wall_clock_ns()
        ));
        std::fs::create_dir_all(path.join("okx-swap")).unwrap();
        Dir(path)
    }

    fn record_json(
        session: &str,
        generation: u64,
        state: &str,
        transport: i64,
        revision: u64,
    ) -> String {
        serde_json::json!({
            "schema": SESSION_LIVENESS_SCHEMA,
            "source_session_id": session,
            "connection_generation": generation,
            "state": state,
            "last_transport_at_ns": transport,
            "updated_at_ns": transport,
            "config_revision": revision,
        })
        .to_string()
    }

    /// Atomic replace, as the ingestor writes (new inode each time).
    fn write(root: &Path, lane: &str, body: &str) {
        let dir = root.join("okx-swap");
        let tmp = dir.join(format!(".{lane}.tmp"));
        std::fs::write(&tmp, body).unwrap();
        std::fs::rename(&tmp, dir.join(format!("{lane}.json"))).unwrap();
    }

    fn event(revision: u64) -> EventSession {
        EventSession {
            venue: "OKX".into(),
            market: "SWAP".into(),
            source_session_id: "okx-public-1".into(),
            connection_generation: 7,
            config_revision: revision,
        }
    }

    fn reader(root: &Path, now: &Arc<AtomicI64>) -> SessionLivenessReader {
        let now = now.clone();
        SessionLivenessReader::with_clock(root, Arc::new(move || now.load(Ordering::SeqCst)))
    }

    fn frame(
        reader: &SessionLivenessReader,
        last: Option<&EventSession>,
    ) -> Result<query::SessionAlive, Withheld> {
        session_alive(
            reader,
            last,
            Duration::from_millis(500),
            Some(2_000),
            42,
            99,
            NOW,
        )
    }

    #[test]
    fn live_record_produces_a_frame_with_the_event_identity() {
        let root = temp_root("live");
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 300 * MS, 22),
        );
        let now = Arc::new(AtomicI64::new(NOW));
        let frame = frame(&reader(&root.0, &now), Some(&event(21))).expect("frame");
        assert_eq!(frame.source_session_id, "okx-public-1");
        assert_eq!(frame.connection_generation, 7);
        // The event's (pinned) revision, not the session's newer one.
        assert_eq!(frame.config_revision, 21);
        assert_eq!(frame.provider_last_transport_at_ns, (NOW - 300 * MS) as u64);
        assert_eq!(frame.provider_session_liveness_ms, 300);
        assert_eq!((frame.gateway_epoch, frame.high_watermark), (42, 99));
    }

    #[test]
    fn every_fail_closed_case_withholds_the_frame() {
        let root = temp_root("closed");
        let now = Arc::new(AtomicI64::new(NOW));
        let reader = reader(&root.0, &now);
        // Missing file.
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_UNAVAILABLE"))
        );
        // No delivered event yet.
        assert_eq!(frame(&reader, None), Err(Withheld::NoEvent));
        // DISCONNECTED.
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "DISCONNECTED", NOW - 10 * MS, 22),
        );
        assert_eq!(frame(&reader, Some(&event(21))), Err(Withheld::NotLive));
        // Stale: older than 3 x 500 ms.
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 1_501 * MS, 22),
        );
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Stale { liveness_ms: 1_501 })
        );
        // Wrong generation (a newer connection of the same lane).
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 8, "LIVE", NOW - 10 * MS, 22),
        );
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_UNAVAILABLE"))
        );
        // Wrong config revision: the session is older than its event.
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 10 * MS, 20),
        );
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_CONFIG_MISMATCH"))
        );
        // Transport in the future after the read: clock skew.
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW + 5 * MS, 22),
        );
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_CLOCK_SKEW"))
        );
        // Unreadable (malformed) record and no match.
        write(&root.0, "public", "{not json");
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_MALFORMED"))
        );
        // Two records of the same session/generation.
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 10 * MS, 22),
        );
        write(
            &root.0,
            "public-b",
            &record_json("okx-public-1", 7, "LIVE", NOW - 10 * MS, 22),
        );
        assert_eq!(
            frame(&reader, Some(&event(21))),
            Err(Withheld::Session("SOURCE_SESSION_AMBIGUOUS"))
        );
    }

    #[test]
    fn requirement_session_sla_bounds_the_age_too() {
        let root = temp_root("sla");
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 900 * MS, 22),
        );
        let now = Arc::new(AtomicI64::new(NOW));
        let result = session_alive(
            &reader(&root.0, &now),
            Some(&event(21)),
            Duration::from_millis(500),
            Some(800),
            1,
            1,
            NOW,
        );
        assert_eq!(result, Err(Withheld::Stale { liveness_ms: 900 }));
    }

    #[test]
    fn age_is_judged_after_the_read() {
        let root = temp_root("clock");
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 1_400 * MS, 22),
        );
        // The reader's clock is 200 ms past the caller's sample.
        let now = Arc::new(AtomicI64::new(NOW + 200 * MS));
        assert_eq!(
            frame(&reader(&root.0, &now), Some(&event(21))),
            Err(Withheld::Stale { liveness_ms: 1_600 })
        );
    }

    #[test]
    fn cache_rereads_a_replaced_file() {
        let root = temp_root("cache");
        let now = Arc::new(AtomicI64::new(NOW));
        let reader = reader(&root.0, &now);
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "LIVE", NOW - 10 * MS, 22),
        );
        assert!(frame(&reader, Some(&event(21))).is_ok());
        write(
            &root.0,
            "public",
            &record_json("okx-public-1", 7, "DISCONNECTED", NOW - 5 * MS, 22),
        );
        assert_eq!(frame(&reader, Some(&event(21))), Err(Withheld::NotLive));
    }

    #[test]
    fn record_validation_matches_the_python_reader() {
        let good = record_json("s", 1, "live", 10, 1);
        assert_eq!(
            SessionRecord::parse(good.as_bytes()).map(|r| r.state),
            Some(SessionState::Live)
        );
        let mut value: serde_json::Value = serde_json::from_str(&good).unwrap();
        value["extra"] = 1.into();
        assert!(SessionRecord::parse(value.to_string().as_bytes()).is_none());
        for (key, bad) in [
            ("schema", serde_json::json!("other")),
            ("connection_generation", serde_json::json!(0)),
            ("connection_generation", serde_json::json!(true)),
            ("last_transport_at_ns", serde_json::json!(1.5)),
            ("updated_at_ns", serde_json::json!(9)),
            ("config_revision", serde_json::json!(0)),
            ("state", serde_json::json!("RECONNECTING")),
            ("source_session_id", serde_json::json!("")),
        ] {
            let mut value: serde_json::Value = serde_json::from_str(&good).unwrap();
            value[key] = bad;
            assert!(
                SessionRecord::parse(value.to_string().as_bytes()).is_none(),
                "{key}"
            );
        }
    }

    #[test]
    fn file_names_and_directories_follow_the_python_rules() {
        assert!(safe_file("public.json"));
        assert!(safe_file("okx-public_2.a.json"));
        assert!(!safe_file(".public.json"));
        assert!(!safe_file("Public.json"));
        assert!(!safe_file(".json"));
        assert!(!safe_file(&format!("{}.json", "a".repeat(121))));
        assert!(safe_file(&format!("{}.json", "a".repeat(120))));
        assert_eq!(directory_name(" OKX ", "Swap").as_deref(), Some("okx-swap"));
        assert_eq!(directory_name("okx/..", "swap"), None);
        assert_eq!(directory_name("", "swap"), None);
    }

    #[test]
    fn interval_is_clamped_and_zero_means_off() {
        assert_eq!(clamp_interval(0), None);
        assert_eq!(clamp_interval(1), Some(Duration::from_millis(250)));
        assert_eq!(clamp_interval(400), Some(Duration::from_millis(400)));
        assert_eq!(clamp_interval(10_000), Some(Duration::from_millis(500)));
        assert!(requirement_allows_frames("QUOTE", Some("OBSERVE")));
        assert!(requirement_allows_frames("TRADE", Some("OBSERVE")));
        assert!(!requirement_allows_frames("QUOTE", Some("BLOCK")));
        assert!(!requirement_allows_frames("QUOTE", None));
        assert!(!requirement_allows_frames("BOOK_DELTA", Some("OBSERVE")));
    }

    #[test]
    fn frame_record_never_carries_a_cursor_position() {
        let record = session_alive_record(query::SessionAlive::default(), "token-7");
        assert_eq!(record.logical_offset, 0);
        assert_eq!(record.resume_token, "token-7");
        assert!(matches!(
            record.payload,
            Some(query::stream_record::Payload::SessionAlive(_))
        ));
    }

    #[test]
    fn envelopes_without_a_session_are_never_vouched_for() {
        let mut envelope = EventEnvelope {
            venue: "OKX".into(),
            market: "SWAP".into(),
            source_session_id: "s".into(),
            connection_generation: 1,
            config_revision: 3,
            ..Default::default()
        };
        assert!(EventSession::of(&envelope).is_some());
        envelope.connection_generation = 0;
        assert!(EventSession::of(&envelope).is_none());
        envelope.connection_generation = 1;
        envelope.config_revision = u64::from(u32::MAX) + 1;
        assert!(EventSession::of(&envelope).is_none());
    }
}
