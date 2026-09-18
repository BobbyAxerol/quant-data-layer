use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::canonical::{canonical_bytes, TradeContext, TradeFixture};

pub const WS_COMBINED_BASE: &str = "wss://fstream.binance.com/public/stream?streams=";

#[derive(Clone, Debug, Deserialize)]
pub struct ShadowConfig {
    pub context: TradeContext,
    pub streams: Vec<String>,
    pub exchange_info_path: String,
    pub wal_path: String,
    pub max_events: usize,
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
}

fn default_timeout() -> u64 {
    30
}

#[derive(Clone, Debug)]
pub struct ProviderFrame {
    pub stream: String,
    pub data: Value,
    pub raw_frame: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WalRecord {
    pub schema: &'static str,
    pub provenance: &'static str,
    pub stream: String,
    pub raw_frame: String,
    pub canonical_hex: String,
    pub canonical_sha256: String,
}

pub fn validate_stream(stream: &str) -> Result<(), String> {
    let normalized = stream.trim();
    if normalized.is_empty()
        || normalized != normalized.to_ascii_lowercase() && !normalized.ends_with("@bookTicker")
    {
        // Native Binance event names retain documented casing; symbol must be lower-case.
        let symbol = normalized.split('@').next().unwrap_or_default();
        if symbol != symbol.to_ascii_lowercase() {
            return Err("Binance stream symbols must be lowercase".into());
        }
    }
    if !(normalized.ends_with("@trade")
        || normalized.ends_with("@bookTicker")
        || normalized.ends_with("@depth@100ms")
        || normalized.ends_with("@markPrice@1s")
        || normalized.contains("@kline_"))
    {
        return Err(format!("unsupported Binance USD-M stream: {normalized}"));
    }
    Ok(())
}

/// The routed WebSocket base a USD-M stream must be subscribed on.
///
/// Binance split `fstream.binance.com` into `/public`, `/market` and `/private`
/// and decommissioned the unrouted form on 2026-04-23. A connection without a
/// routed path receives the `/public` group only: channels under `/market`
/// acknowledge the subscription and then push nothing, which is how this
/// platform lost every Binance kline and mark price for five months while
/// `@depth`, `@bookTicker` and `@trade` kept working on the same socket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinanceRoute {
    Public,
    Market,
}

impl BinanceRoute {
    /// The path segment between the host and the control `/ws` endpoint.
    pub fn segment(self) -> &'static str {
        match self {
            BinanceRoute::Public => "public",
            BinanceRoute::Market => "market",
        }
    }
}

/// Which routed base one demanded stream belongs to.
///
/// `@aggTrade`, `@markPrice` and `@kline_*` are `/market` per Binance's change
/// notice. `@bookTicker` and the depth streams are `/public` per the same
/// notice. Raw `@trade` appears in neither of the notice's two tables; it was
/// measured on 2026-09-18 to deliver on an unrouted connection, which is the
/// `/public` group, and it is classified from that measurement rather than
/// from the document.
///
/// Every stream `validate_stream` accepts must be classifiable here, so a new
/// stream cannot be admitted without also being routed.
pub fn stream_route(stream: &str) -> Result<BinanceRoute, String> {
    let normalized = stream.trim();
    validate_stream(normalized)?;
    if normalized.ends_with("@markPrice@1s") || normalized.contains("@kline_") {
        return Ok(BinanceRoute::Market);
    }
    if normalized.ends_with("@trade")
        || normalized.ends_with("@bookTicker")
        || normalized.ends_with("@depth@100ms")
    {
        return Ok(BinanceRoute::Public);
    }
    Err(format!("unrouted Binance USD-M stream: {normalized}"))
}

/// Whether a configured base URL carries the routed path for `route`.
///
/// The ingestor subscribes dynamically, so it needs the control endpoint
/// (`…/ws`) rather than a combined-stream URL, and the routed segment must sit
/// immediately before it. `wss://fstream.binance.com/ws` - the decommissioned
/// form - fails here, which is the point: a config that still carries it is
/// refused instead of silently degrading to the `/public` group.
pub fn is_routed_control_url(url: &str, route: BinanceRoute) -> bool {
    let normalized = url.trim().trim_end_matches('/');
    normalized.starts_with("wss://")
        && normalized.ends_with(&format!("/{}/ws", route.segment()))
        && !normalized.contains('?')
}

pub fn combined_url(streams: &[String]) -> Result<String, String> {
    if streams.is_empty() {
        return Err("at least one demanded stream is required".into());
    }
    for stream in streams {
        validate_stream(stream)?;
    }
    Ok(format!("{WS_COMBINED_BASE}{}", streams.join("/")))
}

pub fn decode_combined(raw_frame: String) -> Result<ProviderFrame, String> {
    let decoded: Value = serde_json::from_str(&raw_frame).map_err(|error| error.to_string())?;
    let stream = decoded
        .get("stream")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "combined frame missing stream".to_owned())?
        .to_owned();
    let data = decoded
        .get("data")
        .filter(|value| value.is_object())
        .ok_or_else(|| "combined frame missing data object".to_owned())?
        .clone();
    Ok(ProviderFrame {
        stream,
        data,
        raw_frame,
    })
}

fn native_stream(decoded: &Value) -> Result<String, String> {
    let symbol = decoded
        .get("s")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Binance native frame missing symbol".to_owned())?
        .to_ascii_lowercase();
    let stream = match decoded.get("e").and_then(Value::as_str) {
        Some("trade") => format!("{symbol}@trade"),
        Some("bookTicker") => format!("{symbol}@bookTicker"),
        Some("markPriceUpdate") => format!("{symbol}@markPrice@1s"),
        // USD-M diff-depth frames normally carry the documented
        // `depthUpdate` discriminator. Map only the complete structural frame
        // to the explicitly subscribed `@depth@100ms` lane; the generic L2
        // state machine remains provider-neutral and will validate continuity.
        Some("depthUpdate") if is_direct_diff_depth(decoded) => {
            format!("{symbol}@depth@100ms")
        }
        Some("depthUpdate") => {
            return Err("Binance native depth update frame misses documented fields".into())
        }
        Some("kline") => {
            let interval = decoded
                .get("k")
                .and_then(Value::as_object)
                .and_then(|value| value.get("i"))
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| "Binance native kline frame missing interval".to_owned())?;
            format!("{symbol}@kline_{interval}")
        }
        Some(other) => return Err(format!("unsupported Binance native event: {other}")),
        // Binance direct `@bookTicker` frames may omit the generic event
        // discriminator. Infer only the documented full BBO shape; no
        // incomplete/control frame can be promoted to a quote subscription.
        None if is_direct_book_ticker(decoded) => format!("{symbol}@bookTicker"),
        // Diff-depth frames also omit the generic event discriminator on the
        // documented direct control socket.  Require the complete sequence and
        // side-update shape; a generic JSON/control frame cannot become an L2
        // subscription by inference.
        None if is_direct_diff_depth(decoded) => format!("{symbol}@depth@100ms"),
        None => return Err("Binance native frame missing event type".into()),
    };
    validate_stream(&stream)?;
    Ok(stream)
}

fn is_direct_book_ticker(decoded: &Value) -> bool {
    decoded.get("u").and_then(Value::as_u64).is_some()
        && ["b", "B", "a", "A"].iter().all(|field| {
            decoded
                .get(*field)
                .and_then(Value::as_str)
                .is_some_and(|value| !value.is_empty())
        })
}

fn is_direct_diff_depth(decoded: &Value) -> bool {
    decoded.get("U").and_then(Value::as_u64).is_some()
        && decoded.get("u").and_then(Value::as_u64).is_some()
        && decoded.get("b").and_then(Value::as_array).is_some()
        && decoded.get("a").and_then(Value::as_array).is_some()
}

/// Decode either a legacy combined stream frame or a direct frame received
/// after a control-protocol subscription. Keeping this at the venue boundary
/// lets the canonical core remain provider-envelope-only.
pub fn decode_subscribed(raw_frame: String) -> Result<ProviderFrame, String> {
    let decoded: Value = serde_json::from_str(&raw_frame).map_err(|error| error.to_string())?;
    if decoded.get("stream").is_some() || decoded.get("data").is_some() {
        return decode_combined(raw_frame);
    }
    let stream = native_stream(&decoded)?;
    Ok(ProviderFrame {
        stream,
        data: decoded,
        raw_frame,
    })
}

pub fn canonicalize_frame(
    frame: &ProviderFrame,
    mut context: TradeContext,
    partition_sequence: u64,
    received_at_ns: i64,
) -> Result<WalRecord, String> {
    context.partition_sequence = partition_sequence;
    context.received_at_ns = received_at_ns;
    context.normalized_at_ns = received_at_ns;
    context.published_at_ns = received_at_ns;
    let provider_kind = if frame.stream.ends_with("@trade") {
        "binance_usdm_agg_trade"
    } else if frame.stream.ends_with("@bookTicker") {
        "binance_usdm_bbo"
    } else if frame.stream.contains("@kline_") {
        "binance_usdm_bar"
    } else {
        return Err("unsupported provider frame".into());
    };
    let canonical = canonical_bytes(&TradeFixture {
        provider_kind: provider_kind.to_owned(),
        context,
        raw: frame.data.clone(),
    })?;
    use sha2::{Digest, Sha256};
    Ok(WalRecord {
        schema: "qdl.binance-shadow-wal.v1",
        provenance: "REAL_PROVIDER",
        stream: frame.stream.clone(),
        raw_frame: frame.raw_frame.clone(),
        canonical_hex: hex::encode(&canonical),
        canonical_sha256: format!("{:x}", Sha256::digest(&canonical)),
    })
}

pub fn exchange_info_has_active_symbol(payload: &Value, symbol: &str) -> bool {
    payload
        .get("symbols")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|item| {
            item.get("symbol").and_then(Value::as_str) == Some(symbol)
                && item.get("status").and_then(Value::as_str) == Some("TRADING")
        })
}

#[cfg(test)]
mod tests {
    use super::{
        combined_url, decode_combined, decode_subscribed, exchange_info_has_active_symbol,
        is_routed_control_url, stream_route, validate_stream, BinanceRoute,
    };
    use serde_json::json;

    /// Every stream the platform may subscribe, so the route table and the
    /// admission rule cannot drift apart.
    const ACCEPTED_STREAMS: [&str; 5] = [
        "btcusdt@trade",
        "btcusdt@bookTicker",
        "btcusdt@depth@100ms",
        "btcusdt@markPrice@1s",
        "btcusdt@kline_1m",
    ];

    #[test]
    fn market_group_streams_route_to_market() {
        assert_eq!(
            stream_route("btcusdt@markPrice@1s"),
            Ok(BinanceRoute::Market)
        );
        assert_eq!(stream_route("btcusdt@kline_1m"), Ok(BinanceRoute::Market));
        assert_eq!(stream_route("ethusdt@kline_15m"), Ok(BinanceRoute::Market));
    }

    #[test]
    fn public_group_streams_route_to_public() {
        assert_eq!(stream_route("btcusdt@trade"), Ok(BinanceRoute::Public));
        assert_eq!(stream_route("btcusdt@bookTicker"), Ok(BinanceRoute::Public));
        assert_eq!(
            stream_route("btcusdt@depth@100ms"),
            Ok(BinanceRoute::Public)
        );
    }

    #[test]
    fn every_admitted_stream_has_a_route() {
        for stream in ACCEPTED_STREAMS {
            assert!(validate_stream(stream).is_ok(), "{stream} must be admitted");
            assert!(
                stream_route(stream).is_ok(),
                "{stream} is admitted but has no routed base"
            );
        }
    }

    #[test]
    fn an_unsupported_stream_has_no_route() {
        assert!(stream_route("btcusdt@aggTrade").is_err());
        assert!(stream_route("btcusdt@forceOrder").is_err());
        assert!(stream_route("").is_err());
    }

    #[test]
    fn the_decommissioned_unrouted_base_is_not_a_routed_control_url() {
        // The exact form this platform ran until 2026-09-18.
        assert!(!is_routed_control_url(
            "wss://fstream.binance.com/ws",
            BinanceRoute::Public
        ));
        assert!(!is_routed_control_url(
            "wss://fstream.binance.com/ws",
            BinanceRoute::Market
        ));
    }

    #[test]
    fn routed_control_urls_are_recognised_per_group() {
        assert!(is_routed_control_url(
            "wss://fstream.binance.com/public/ws",
            BinanceRoute::Public
        ));
        assert!(is_routed_control_url(
            "wss://fstream.binance.com/market/ws",
            BinanceRoute::Market
        ));
        // A base is never accepted for the other group.
        assert!(!is_routed_control_url(
            "wss://fstream.binance.com/public/ws",
            BinanceRoute::Market
        ));
        assert!(!is_routed_control_url(
            "wss://fstream.binance.com/market/ws",
            BinanceRoute::Public
        ));
    }

    #[test]
    fn a_combined_stream_url_is_not_a_control_url() {
        // The ingestor subscribes dynamically and needs the control endpoint.
        assert!(!is_routed_control_url(
            "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m",
            BinanceRoute::Market
        ));
        assert!(!is_routed_control_url(
            "https://fstream.binance.com/market/ws",
            BinanceRoute::Market
        ));
    }

    #[test]
    fn demanded_streams_are_not_truncated() {
        let streams = vec![
            "btcusdt@trade".to_owned(),
            "btcusdt@bookTicker".to_owned(),
            "btcusdt@kline_1m".to_owned(),
        ];
        let url = combined_url(&streams).unwrap();
        assert!(streams.iter().all(|stream| url.contains(stream)));
    }

    #[test]
    fn malformed_frames_and_delisted_symbols_fail_closed() {
        assert!(decode_combined(r#"{"stream":"x"}"#.into()).is_err());
        let info = json!({"symbols": [{"symbol": "BTCUSDT", "status": "CLOSE"}]});
        assert!(!exchange_info_has_active_symbol(&info, "BTCUSDT"));
    }

    #[test]
    fn native_frames_keep_channel_identity() {
        let trade = decode_subscribed(
            r#"{"e":"trade","s":"BTCUSDT","t":1,"p":"1","q":"2","T":3,"m":false}"#.into(),
        )
        .unwrap();
        assert_eq!(trade.stream, "btcusdt@trade");
        // Direct Binance book-ticker payloads are structural and may omit
        // `e`; their full BBO shape maps only to the subscribed quote stream.
        let quote =
            decode_subscribed(r#"{"u":1,"s":"BTCUSDT","b":"1","B":"2","a":"3","A":"4"}"#.into())
                .unwrap();
        assert_eq!(quote.stream, "btcusdt@bookTicker");
        let bar =
            decode_subscribed(r#"{"e":"kline","s":"BTCUSDT","k":{"i":"1m","x":true}}"#.into())
                .unwrap();
        assert_eq!(bar.stream, "btcusdt@kline_1m");
        let book = decode_subscribed(
            r#"{"s":"BTCUSDT","U":10,"u":10,"pu":9,"b":[["1","2"]],"a":[["3","4"]]}"#.into(),
        )
        .unwrap();
        assert_eq!(book.stream, "btcusdt@depth@100ms");
        let depth_update = decode_subscribed(
            r#"{"e":"depthUpdate","s":"BTCUSDT","U":11,"u":11,"pu":10,"b":[["1","2"]],"a":[["3","4"]]}"#.into(),
        )
        .unwrap();
        assert_eq!(depth_update.stream, "btcusdt@depth@100ms");
        let mark_index = decode_subscribed(
            r#"{"e":"markPriceUpdate","E":3,"s":"BTCUSDT","p":"1","i":"1"}"#.into(),
        )
        .unwrap();
        assert_eq!(mark_index.stream, "btcusdt@markPrice@1s");
        assert!(decode_subscribed(
            r#"{"e":"depthUpdate","s":"BTCUSDT","U":12,"u":12,"b":[["1","2"]]}"#.into()
        )
        .is_err());
    }
}
