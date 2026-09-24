//! Stage A product classification and BAR retained caps (KN-3 D14, D15).
//!
//! The one product-identity owner is the gateway bundle compiled from the
//! Python catalog, so the Stream and the projector cannot disagree on a
//! logical product key. A canonical record maps by `(physical key = Kafka
//! key, payload feed)` to exactly one binding: snapshot and delta of one book
//! are two products; MARK_INDEX_PRICE is one product whose envelope (both
//! component clocks) is carried unchanged. BAR becomes a `BAR_REVISION` frame
//! on the bars topic, every other feed a `LATEST` frame on the latest topic.
//! A record with no binding, another identity than its binding, or a payload
//! that does not decode is an integrity stop (never skipped), as in the
//! running projector.

use crate::stage_a::{InputRecord, OutputRecord, Transform, TransformError};
use prost::Message;
use qdl_contracts::gateway_bundle::Bundle;
use qdl_contracts::qdl::marketdata::v2::EventEnvelope;
use qdl_contracts::state_codec::{payload_feed, state_partition, StateFrame};
use qdl_contracts::state_contract::{LogicalProductKey, SourceCoordinate};
use std::collections::HashMap;

/// Public BAR window every product keeps (the spool keeps the same today).
pub const PUBLIC_BAR_WINDOW: u64 = 10_000;
/// Rows kept beyond the demanded window (repairs, late revisions).
pub const BAR_HEADROOM: u64 = 2_064;

/// Feeds sampled at an interval that the frozen KN-1 product check cannot
/// qualify (it qualifies only BAR by interval).
const SAMPLED_FEEDS: [&str; 4] = ["OPEN_INTEREST", "LONG_SHORT_RATIO", "TAKER_FLOW", "BASIS"];

#[derive(Clone, Debug)]
pub struct Product {
    pub lpk: LogicalProductKey,
    pub binding_id: String,
}

/// `(physical key, feed)` -> product, built from a verified bundle.
#[derive(Clone, Debug)]
pub struct ProductMap {
    pub environment: String,
    products: HashMap<(String, String), Product>,
}

impl ProductMap {
    pub fn from_bundle(bundle: &Bundle) -> Result<Self, String> {
        let mut products = HashMap::new();
        for binding in &bundle.bindings {
            if SAMPLED_FEEDS.contains(&binding.feed.as_str()) && binding.interval.is_some() {
                return Err(format!(
                    "binding {} is a sampled feed with an interval; the KN-1 product check cannot qualify it",
                    binding.binding_id
                ));
            }
            let interval =
                if binding.feed == "BAR" {
                    Some(binding.interval.as_deref().ok_or_else(|| {
                        format!("BAR binding {} has no interval", binding.binding_id)
                    })?)
                } else {
                    binding.interval.as_deref()
                };
            let lpk = LogicalProductKey::new(
                &bundle.environment,
                &binding.venue,
                &binding.market,
                &binding.instrument_uid,
                &binding.feed,
                interval,
            )?;
            if lpk.encode() != binding.product_key {
                return Err(format!(
                    "binding {} product key {} differs from the Rust derivation {}",
                    binding.binding_id,
                    binding.product_key,
                    lpk.encode()
                ));
            }
            let product = Product {
                lpk,
                binding_id: binding.binding_id.clone(),
            };
            if products
                .insert(
                    (binding.physical_key.clone(), binding.feed.clone()),
                    product,
                )
                .is_some()
            {
                return Err(format!(
                    "two bindings share physical key {} and feed {}",
                    binding.physical_key, binding.feed
                ));
            }
        }
        Ok(Self {
            environment: bundle.environment.clone(),
            products,
        })
    }

    pub fn product(&self, physical_key: &str, feed: &str) -> Option<&Product> {
        self.products
            .get(&(physical_key.to_owned(), feed.to_owned()))
    }

    pub fn len(&self) -> usize {
        self.products.len()
    }

    pub fn is_empty(&self) -> bool {
        self.products.is_empty()
    }

    pub fn bar_products(&self) -> impl Iterator<Item = &Product> {
        self.products
            .values()
            .filter(|product| product.lpk.feed == "BAR")
    }
}

/// D15: per BAR product, `max(public window, largest demanded warmup) +
/// headroom`, keyed by encoded LPK.
pub fn retained_caps(bundle: &Bundle, products: &ProductMap) -> HashMap<String, u64> {
    products
        .bar_products()
        .map(|product| {
            let demanded = bundle
                .manifests
                .iter()
                .filter(|manifest| {
                    manifest.requirements.iter().any(|requirement| {
                        requirement.instrument_uid == product.lpk.instrument_uid
                            && requirement.feed == "BAR"
                            && requirement.interval.as_deref()
                                == Some(product.lpk.qualifier.as_str())
                    })
                })
                .map(|manifest| manifest.quotas.max_warmup_rows)
                .max()
                .unwrap_or(0);
            (
                product.lpk.encode(),
                demanded.max(PUBLIC_BAR_WINDOW) + BAR_HEADROOM,
            )
        })
        .collect()
}

#[derive(Clone, Debug)]
pub struct TransformSettings {
    /// Kafka topic id of the canonical topic (the source coordinate names it).
    pub source_topic_id: String,
    pub latest_topic: String,
    pub bars_topic: String,
    pub latest_partitions: u32,
    pub bars_partitions: u32,
    pub materializer_epoch: u64,
}

pub struct ProductTransform {
    products: ProductMap,
    settings: TransformSettings,
}

impl ProductTransform {
    pub fn new(products: ProductMap, settings: TransformSettings) -> Self {
        Self { products, settings }
    }
}

fn integrity(reason: impl Into<String>) -> TransformError {
    TransformError::Integrity(reason.into())
}

impl Transform for ProductTransform {
    fn transform(&mut self, record: &InputRecord) -> Result<Vec<OutputRecord>, TransformError> {
        let physical_key =
            std::str::from_utf8(&record.key).map_err(|_| integrity("PHYSICAL_KEY_NOT_UTF8"))?;
        let envelope = EventEnvelope::decode(record.payload.as_slice())
            .map_err(|error| integrity(format!("CANONICAL_DECODE:{error}")))?;
        let feed = payload_feed(envelope.payload.as_ref());
        let product = self
            .products
            .product(physical_key, feed)
            .ok_or_else(|| integrity(format!("PRODUCT_NOT_IN_BUNDLE:{physical_key}:{feed}")))?;
        let source = SourceCoordinate {
            topic_id: self.settings.source_topic_id.clone(),
            partition: u32::try_from(record.partition)
                .map_err(|_| integrity("NEGATIVE_PARTITION"))?,
            offset: u64::try_from(record.offset).map_err(|_| integrity("NEGATIVE_OFFSET"))?,
        };
        let bar = feed == "BAR";
        let frame = if bar {
            StateFrame::bar_revision(
                &record.payload,
                &product.lpk,
                source,
                self.settings.materializer_epoch,
            )
        } else {
            StateFrame::latest(
                &record.payload,
                &product.lpk,
                source,
                self.settings.materializer_epoch,
            )
        }
        .map_err(|error| integrity(format!("{}:{error}", product.binding_id)))?;
        let (topic, partitions) = if bar {
            (&self.settings.bars_topic, self.settings.bars_partitions)
        } else {
            (&self.settings.latest_topic, self.settings.latest_partitions)
        };
        let partition = state_partition(&product.lpk, partitions)
            .map_err(|error| integrity(error.to_string()))?;
        Ok(vec![OutputRecord {
            topic: topic.clone(),
            partition: partition as i32,
            key: frame
                .key()
                .map_err(|error| integrity(error.to_string()))?
                .into_bytes(),
            value: Some(
                frame
                    .encode()
                    .map_err(|error| integrity(error.to_string()))?,
            ),
        }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdl_contracts::gateway_bundle::canonical_sha256;
    use qdl_contracts::qdl::marketdata::v2::event_envelope::Payload;
    use qdl_contracts::qdl::marketdata::v2::{
        Bar, BarLifecycle, MarkIndexPrice, OrderBookDelta, OrderBookSnapshot,
    };
    use qdl_contracts::state_codec::{FrameKind, StateFrame};
    use serde_json::{json, Value};
    use std::collections::BTreeMap;

    const UID: &str = "fb26214c-7b9b-5961-95b2-55154755af0f";
    const BOOK: &str = "fb26214c-7b9b-5961-95b2-55154755af0f/book/okx-swap-book";
    const BAR_KEY: &str = "fb26214c-7b9b-5961-95b2-55154755af0f/bar/okx-swap-bar-1m";
    const MARK: &str = "fb26214c-7b9b-5961-95b2-55154755af0f/mark_index_price/okx-swap-mark";

    fn binding(feed: &str, interval: Option<&str>, physical: &str) -> Value {
        let qualifier = interval.unwrap_or("-");
        json!({
            "binding_id": format!("okx-swap-{}", feed.to_lowercase()),
            "instrument_uid": UID, "venue": "OKX", "market": "SWAP",
            "feed": feed, "interval": interval,
            "source_policy_id": "crypto_primary_v2",
            "physical_key": physical, "source_id": "s", "stale_after_ms": 2000,
            "product_key": format!("lpk1|paper|OKX|SWAP|{UID}|{feed}|{qualifier}"),
        })
    }

    fn manifest(warmup: u64, interval: &str) -> Value {
        json!({
            "consumer_id": format!("c{warmup}"), "subject": "s", "environment": "paper",
            "manifest_revision": 1, "manifest_sha256": "x",
            "allowed_purposes": [], "allowed_permissions": [],
            "quotas": {"requests_per_minute": 1, "max_batch_items": 1,
                "max_warmup_rows": warmup, "max_streams": 1, "max_buffer_events": 1},
            "requirements": [{"instrument_uid": UID, "feed": "BAR", "interval": interval,
                "consumer_grade": "RESEARCH", "source_policy_id": "crypto_primary_v2",
                "event_recency_policy": null, "max_session_liveness_ms": null}],
        })
    }

    fn bundle_of(bindings: Vec<Value>, manifests: Vec<Value>) -> Bundle {
        let mut document = json!({
            "schema": qdl_contracts::gateway_bundle::SCHEMA,
            "environment": "paper",
            "catalog": {"catalog_revision": 1, "source_policy_revision": 1,
                "canonical_stream": "md.canonical.v2", "bindings": bindings},
            "manifests": manifests,
        });
        let sha = canonical_sha256(document.as_object().unwrap());
        document["sha256"] = json!(sha);
        Bundle::parse(&document.to_string()).unwrap()
    }

    fn catalog() -> Bundle {
        bundle_of(
            vec![
                binding("BOOK_SNAPSHOT", None, BOOK),
                binding("BOOK_DELTA", None, BOOK),
                binding("BAR", Some("1m"), BAR_KEY),
                binding("MARK_INDEX_PRICE", None, MARK),
            ],
            vec![],
        )
    }

    fn transform() -> ProductTransform {
        ProductTransform::new(
            ProductMap::from_bundle(&catalog()).unwrap(),
            TransformSettings {
                source_topic_id: "ljfjPYApRpWQd79McfTtZg".into(),
                latest_topic: "md.latest.v2".into(),
                bars_topic: "md.bars.v2".into(),
                latest_partitions: 6,
                bars_partitions: 6,
                materializer_epoch: 3,
            },
        )
    }

    fn encoded(uid: &str, payload: Payload) -> Vec<u8> {
        EventEnvelope {
            event_id: vec![7; 16],
            instrument_uid: uid.into(),
            venue: "OKX".into(),
            market: "SWAP".into(),
            payload: Some(payload),
            ..Default::default()
        }
        .encode_to_vec()
    }

    fn record(key: &str, payload: Vec<u8>, offset: i64) -> InputRecord {
        InputRecord {
            partition: 4,
            offset,
            key: key.as_bytes().to_vec(),
            payload,
        }
    }

    fn final_bar() -> Payload {
        Payload::Bar(Bar {
            interval: "1m".into(),
            open_time_ns: 1_758_700_800_000_000_000,
            close_time_ns: 1_758_700_859_999_999_999,
            is_final: true,
            lifecycle: BarLifecycle::Final as i32,
            ..Default::default()
        })
    }

    fn only(outputs: Vec<OutputRecord>) -> (OutputRecord, StateFrame) {
        assert_eq!(outputs.len(), 1);
        let output = outputs.into_iter().next().unwrap();
        let frame = StateFrame::decode(output.value.as_deref().unwrap()).unwrap();
        (output, frame)
    }

    #[test]
    fn book_snapshot_and_delta_on_one_physical_key_are_two_latest_products() {
        let mut transform = transform();
        let (snapshot, snapshot_frame) = only(
            transform
                .transform(&record(
                    BOOK,
                    encoded(UID, Payload::BookSnapshot(OrderBookSnapshot::default())),
                    10,
                ))
                .unwrap(),
        );
        let (delta, delta_frame) = only(
            transform
                .transform(&record(
                    BOOK,
                    encoded(UID, Payload::BookDelta(OrderBookDelta::default())),
                    11,
                ))
                .unwrap(),
        );
        assert_eq!(snapshot.topic, "md.latest.v2");
        assert_eq!(delta.topic, "md.latest.v2");
        assert_ne!(
            snapshot.key, delta.key,
            "a delta never replaces the snapshot product"
        );
        assert_eq!(snapshot_frame.kind, FrameKind::Latest);
        assert_eq!(snapshot_frame.lpk.feed, "BOOK_SNAPSHOT");
        assert_eq!(delta_frame.lpk.feed, "BOOK_DELTA");
        let source = delta_frame.source.unwrap();
        assert_eq!(
            (source.topic_id.as_str(), source.partition, source.offset),
            ("ljfjPYApRpWQd79McfTtZg", 4, 11)
        );
        assert_eq!(delta_frame.materializer_epoch, 3);
        assert_eq!(
            delta.partition as u32,
            state_partition(&delta_frame.lpk, 6).unwrap(),
            "routed by the LPK only"
        );
    }

    #[test]
    fn bar_goes_to_the_bars_topic_with_its_fact_key_and_envelope_unchanged() {
        let payload = encoded(UID, final_bar());
        let (output, frame) = only(
            transform()
                .transform(&record(BAR_KEY, payload.clone(), 99))
                .unwrap(),
        );
        assert_eq!(output.topic, "md.bars.v2");
        assert_eq!(frame.kind, FrameKind::BarRevision);
        assert_eq!(frame.lpk.qualifier, "1m");
        assert_eq!(frame.envelope, payload, "canonical bytes carried unchanged");
        let key = String::from_utf8(output.key).unwrap();
        assert!(
            key.starts_with(&format!(
                "lpk1|paper|OKX|SWAP|{UID}|BAR|1m|1758700800000|f0|"
            )),
            "{key}"
        );
    }

    #[test]
    fn mark_index_is_one_latest_product_carried_unchanged() {
        let payload = encoded(UID, Payload::MarkIndexPrice(MarkIndexPrice::default()));
        let (output, frame) = only(
            transform()
                .transform(&record(MARK, payload.clone(), 5))
                .unwrap(),
        );
        assert_eq!(output.topic, "md.latest.v2");
        assert_eq!(frame.lpk.feed, "MARK_INDEX_PRICE");
        assert_eq!(frame.envelope, payload);
    }

    #[test]
    fn unbound_foreign_or_corrupt_records_stop_stage_a() {
        let mut transform = transform();
        let cases = [
            // a feed the physical key has no binding for
            record(
                BOOK,
                encoded(UID, Payload::MarkIndexPrice(MarkIndexPrice::default())),
                1,
            ),
            // a physical key outside the bundle
            record("other/bar/x", encoded(UID, final_bar()), 2),
            // the binding's physical key but another instrument
            record(
                BAR_KEY,
                encoded("0d522cb3-d764-5066-9e96-19f1817e84d7", final_bar()),
                3,
            ),
            // not a canonical envelope
            record(BAR_KEY, vec![0xff, 0xff, 0xff], 4),
            // no payload
            record(BAR_KEY, EventEnvelope::default().encode_to_vec(), 5),
        ];
        for case in cases {
            match transform.transform(&case) {
                Err(TransformError::Integrity(reason)) => assert!(!reason.is_empty()),
                other => panic!("offset {} must stop: {other:?}", case.offset),
            }
        }
    }

    #[test]
    fn a_bundle_the_projector_cannot_verify_is_refused_at_load() {
        let sampled = bundle_of(
            vec![binding("OPEN_INTEREST", Some("5m"), "k/open_interest/s")],
            vec![],
        );
        assert!(ProductMap::from_bundle(&sampled)
            .unwrap_err()
            .contains("sampled feed"));
        let mut wrong = binding("TRADE", None, "k/trade/s");
        wrong["product_key"] = json!(format!("lpk1|live|OKX|SWAP|{UID}|TRADE|-"));
        assert!(ProductMap::from_bundle(&bundle_of(vec![wrong], vec![]))
            .unwrap_err()
            .contains("differs"));
        let twice = bundle_of(
            vec![
                binding("TRADE", None, "k/trade/s"),
                binding("TRADE", None, "k/trade/s"),
            ],
            vec![],
        );
        assert!(ProductMap::from_bundle(&twice)
            .unwrap_err()
            .contains("share"));
        // an unsampled OPEN_INTEREST binding is accepted
        let plain = bundle_of(
            vec![binding("OPEN_INTEREST", None, "k/open_interest/s")],
            vec![],
        );
        assert_eq!(ProductMap::from_bundle(&plain).unwrap().len(), 1);
    }

    /// Real canonical records of the KN-3 codec golden (spool physical key,
    /// LPK, source coordinate): the transform reproduces their keys,
    /// partitions and frames byte for byte.
    #[test]
    fn real_golden_records_transform_to_their_golden_keys_and_frames() {
        use base64::Engine as _;
        let golden: Value = serde_json::from_str(include_str!(
            "../../../contracts/golden/kn_v220/state_codec.json"
        ))
        .unwrap();
        let records: Vec<&Value> = golden["records"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|record| record["synthetic"] == json!(false))
            .collect();
        let decode = |text: &Value| {
            base64::engine::general_purpose::STANDARD
                .decode(text.as_str().unwrap())
                .unwrap()
        };
        let mut bindings = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for record in &records {
            let lpk = LogicalProductKey::parse(record["lpk"].as_str().unwrap()).unwrap();
            let physical = record["spool_partition_key"].as_str().unwrap();
            if seen.insert((physical.to_owned(), lpk.feed.clone())) {
                bindings.push(json!({
                    "binding_id": format!("golden-{}", record["name"].as_str().unwrap()),
                    "instrument_uid": lpk.instrument_uid, "venue": lpk.venue,
                    "market": lpk.market, "feed": lpk.feed,
                    "interval": if lpk.qualifier == "-" { Value::Null } else { json!(lpk.qualifier) },
                    "source_policy_id": "p", "physical_key": physical, "source_id": "s",
                    "stale_after_ms": 1, "product_key": lpk.encode(),
                }));
            }
        }
        let products = ProductMap::from_bundle(&bundle_of(bindings, vec![])).unwrap();
        let transform = |source: &Value, epoch: u64| {
            ProductTransform::new(
                products.clone(),
                TransformSettings {
                    source_topic_id: source["topic_id"].as_str().unwrap().into(),
                    latest_topic: "md.latest.v2".into(),
                    bars_topic: "md.bars.v2".into(),
                    latest_partitions: 6,
                    bars_partitions: 6,
                    materializer_epoch: epoch,
                },
            )
        };
        let frames: BTreeMap<(String, String), Vec<u8>> = golden["full_frames"]
            .as_array()
            .unwrap()
            .iter()
            .map(|frame| {
                (
                    (
                        frame["record"].as_str().unwrap().to_owned(),
                        frame["kind"].as_str().unwrap().to_owned(),
                    ),
                    decode(&frame["frame_b64"]),
                )
            })
            .collect();
        let mut compared = 0;
        for record in &records {
            let source = &record["source"];
            let outputs = transform(source, record["materializer_epoch"].as_u64().unwrap())
                .transform(&InputRecord {
                    partition: source["partition"].as_i64().unwrap() as i32,
                    offset: source["offset"].as_i64().unwrap(),
                    key: record["spool_partition_key"]
                        .as_str()
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                    payload: decode(&record["canonical_b64"]),
                })
                .unwrap();
            let (output, frame) = only(outputs);
            let bar = frame.lpk.feed == "BAR";
            let key_name = if bar { "bar" } else { "latest" };
            assert_eq!(
                String::from_utf8(output.key).unwrap(),
                record["keys"][key_name].as_str().unwrap()
            );
            assert_eq!(
                output.partition as u64,
                record["partitions"]["6"].as_u64().unwrap()
            );
            let kind = if bar { "BAR_REVISION" } else { "LATEST" };
            let name = record["name"].as_str().unwrap().to_owned();
            if let Some(expected) = frames.get(&(name, kind.to_owned())) {
                assert_eq!(output.value.as_deref().unwrap(), expected.as_slice());
                compared += 1;
            }
        }
        assert_eq!(records.len(), 24);
        assert_eq!(compared, 2, "the golden LATEST and BAR_REVISION frames");
    }

    #[test]
    fn retained_cap_is_the_public_window_or_the_largest_demand_plus_headroom() {
        let bars = vec![
            binding("BAR", Some("1m"), "k/bar/1m"),
            binding("BAR", Some("5m"), "k/bar/5m"),
            binding("BAR", Some("1h"), "k/bar/1h"),
            binding("TRADE", None, "k/trade/s"),
        ];
        let bundle = bundle_of(
            bars,
            vec![
                manifest(2_000, "1m"),
                manifest(10_000, "1m"),
                manifest(25_000, "5m"),
            ],
        );
        let caps = retained_caps(&bundle, &ProductMap::from_bundle(&bundle).unwrap());
        let cap = |interval: &str| caps[&format!("lpk1|paper|OKX|SWAP|{UID}|BAR|{interval}")];
        assert_eq!(caps.len(), 3, "BAR products only");
        assert_eq!(cap("1m"), 12_064);
        assert_eq!(cap("5m"), 27_064);
        assert_eq!(cap("1h"), 12_064, "no demand keeps the spool's window");
    }
}
