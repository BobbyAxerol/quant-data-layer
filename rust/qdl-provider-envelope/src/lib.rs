#![forbid(unsafe_code)]

use std::fmt::{Display, Formatter};

use prost::Message;
use qdl_contracts::qdl::provider::v1::{
    CaptureBoundary, RawProviderEnvelope, TransportCompression, TransportProtocol,
};
use sha2::{Digest, Sha256};

pub const RAW_SCHEMA_NAME: &str = "qdl.provider.raw";
pub const RAW_SCHEMA_MAJOR: u32 = 1;
pub const MAX_RAW_FRAME_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RawEnvelopeError {
    Missing(&'static str),
    Invalid(&'static str),
    Oversized(usize),
    HashMismatch,
}

impl Display for RawEnvelopeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Missing(field) => write!(formatter, "raw envelope missing {field}"),
            Self::Invalid(field) => write!(formatter, "raw envelope has invalid {field}"),
            Self::Oversized(size) => write!(formatter, "raw frame exceeds bound: {size}"),
            Self::HashMismatch => write!(formatter, "raw frame SHA-256 mismatch"),
        }
    }
}

impl std::error::Error for RawEnvelopeError {}

fn required(value: &str, field: &'static str) -> Result<(), RawEnvelopeError> {
    if value.trim().is_empty() {
        Err(RawEnvelopeError::Missing(field))
    } else {
        Ok(())
    }
}

pub fn validate(envelope: &RawProviderEnvelope) -> Result<(), RawEnvelopeError> {
    for (value, field) in [
        (&envelope.raw_schema_name, "raw_schema_name"),
        (&envelope.provider, "provider"),
        (&envelope.venue, "venue"),
        (&envelope.market, "market"),
        (&envelope.product_type, "product_type"),
        (&envelope.native_symbol, "native_symbol"),
        (&envelope.native_channel, "native_channel"),
        (&envelope.subscription_id, "subscription_id"),
        (&envelope.source_session_id, "source_session_id"),
        (&envelope.adapter_version, "adapter_version"),
        (&envelope.correlation_id, "correlation_id"),
    ] {
        required(value, field)?;
    }
    if envelope.raw_schema_name != RAW_SCHEMA_NAME || envelope.raw_schema_major != RAW_SCHEMA_MAJOR
    {
        return Err(RawEnvelopeError::Invalid("raw_schema_name/major"));
    }
    if envelope.capture_id.len() != 16 {
        return Err(RawEnvelopeError::Invalid("capture_id"));
    }
    if envelope.raw_frame_bytes.is_empty() {
        return Err(RawEnvelopeError::Missing("raw_frame_bytes"));
    }
    if envelope.raw_frame_bytes.len() > MAX_RAW_FRAME_BYTES {
        return Err(RawEnvelopeError::Oversized(envelope.raw_frame_bytes.len()));
    }
    if envelope.received_at_ns <= 0 {
        return Err(RawEnvelopeError::Invalid("received_at_ns"));
    }
    if envelope.connection_generation == 0
        || envelope.lease_epoch == 0
        || envelope.authority_revision == 0
        || envelope.partition_plan_epoch == 0
        || envelope.config_revision == 0
        || envelope.instrument_catalog_revision == 0
    {
        return Err(RawEnvelopeError::Invalid("revision/epoch"));
    }
    if TransportProtocol::try_from(envelope.transport_protocol)
        .unwrap_or(TransportProtocol::Unspecified)
        == TransportProtocol::Unspecified
        || TransportCompression::try_from(envelope.transport_compression)
            .unwrap_or(TransportCompression::Unspecified)
            == TransportCompression::Unspecified
        || CaptureBoundary::try_from(envelope.capture_boundary)
            .unwrap_or(CaptureBoundary::Unspecified)
            == CaptureBoundary::Unspecified
    {
        return Err(RawEnvelopeError::Invalid("transport/capture semantics"));
    }
    let actual = Sha256::digest(&envelope.raw_frame_bytes);
    if envelope.raw_frame_sha256.as_slice() != actual.as_slice() {
        return Err(RawEnvelopeError::HashMismatch);
    }
    Ok(())
}

pub fn deterministic_bytes(envelope: &RawProviderEnvelope) -> Result<Vec<u8>, RawEnvelopeError> {
    validate(envelope)?;
    Ok(envelope.encode_to_vec())
}

pub fn canonical_payload_hash(payload: &[u8]) -> [u8; 32] {
    Sha256::digest(payload).into()
}

#[cfg(test)]
mod tests {
    use super::{canonical_payload_hash, deterministic_bytes, validate, RawEnvelopeError};
    use qdl_contracts::qdl::provider::v1::{
        CaptureBoundary, RawProviderEnvelope, TransportCompression, TransportProtocol,
    };
    use sha2::{Digest, Sha256};

    fn valid() -> RawProviderEnvelope {
        let frame = br#"{"e":"aggTrade","s":"BTCUSDT","a":1}"#.to_vec();
        RawProviderEnvelope {
            raw_schema_name: "qdl.provider.raw".into(),
            raw_schema_major: 1,
            raw_schema_minor: 0,
            capture_id: (0_u8..16).collect(),
            provider: "BINANCE_DIRECT".into(),
            venue: "BINANCE".into(),
            market: "USDM".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: "BTCUSDT".into(),
            native_channel: "btcusdt@aggTrade".into(),
            subscription_id: "sub-1".into(),
            source_session_id: "session-1".into(),
            connection_generation: 2,
            lease_epoch: 3,
            authority_revision: 4,
            partition_plan_epoch: 5,
            received_at_ns: 1_000_000,
            transport_protocol: TransportProtocol::Websocket as i32,
            transport_compression: TransportCompression::None as i32,
            capture_boundary: CaptureBoundary::PostDecompression as i32,
            raw_frame_sha256: Sha256::digest(&frame).to_vec(),
            raw_frame_bytes: frame,
            adapter_version: "binance/2.0.0".into(),
            config_revision: 6,
            instrument_catalog_revision: 7,
            correlation_id: "corr-1".into(),
            test_provenance: true,
        }
    }

    #[test]
    fn exact_hash_and_golden_bytes_are_stable() {
        let envelope = valid();
        validate(&envelope).unwrap();
        let first = deterministic_bytes(&envelope).unwrap();
        let second = deterministic_bytes(&envelope).unwrap();
        assert_eq!(first, second);
        let golden = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/golden/phase8/raw-provider-envelope.bin"
        ));
        assert_eq!(first, golden);
        assert_eq!(canonical_payload_hash(b"canonical").len(), 32);
    }

    #[test]
    fn missing_semantics_and_hash_mismatch_fail_closed() {
        let mut envelope = valid();
        envelope.source_session_id.clear();
        assert_eq!(
            validate(&envelope),
            Err(RawEnvelopeError::Missing("source_session_id"))
        );
        let mut envelope = valid();
        envelope.raw_frame_sha256 = vec![0; 32];
        assert_eq!(validate(&envelope), Err(RawEnvelopeError::HashMismatch));
    }
}
