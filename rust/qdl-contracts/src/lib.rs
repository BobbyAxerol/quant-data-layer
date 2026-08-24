pub mod qdl {
    pub mod common {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/common/v1/qdl.common.v1.rs"
            ));
        }
    }

    pub mod demand {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/demand/v1/qdl.demand.v1.rs"
            ));
        }
    }

    pub mod instrument {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/instrument/v1/qdl.instrument.v1.rs"
            ));
        }
    }

    pub mod quality {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/quality/v1/qdl.quality.v1.rs"
            ));
        }
    }

    pub mod marketdata {
        pub mod v2 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/marketdata/v2/qdl.marketdata.v2.rs"
            ));
        }
    }

    pub mod query {
        // Prost keeps `oneof` wire semantics as a Rust enum. The canonical event
        // variant is intentionally larger than the lightweight control variant.
        #[allow(clippy::large_enum_variant)]
        pub mod v2 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/query/v2/qdl.query.v2.rs"
            ));
        }
    }

    pub mod provider {
        pub mod v1 {
            include!(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../../generated/rust/qdl/provider/v1/qdl.provider.v1.rs"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::qdl::common::v1::{
        decimal_value, AggressorSide, DecimalValue, QuantityUnit, SourceRole,
    };
    use super::qdl::demand::v1::{
        universe_selector, DataRequirement as DemandRequirement, DemandPurpose, ExplicitSymbols,
        FeedType as DemandFeedType, UniverseSelector,
    };
    use super::qdl::marketdata::v2::{event_envelope, EventEnvelope, Trade, TradeIdentityKind};
    use prost::Message;

    fn decimal(mantissa: i64, scale: i32, source_text: &str) -> DecimalValue {
        DecimalValue {
            scale,
            source_text: source_text.to_owned(),
            coefficient: Some(decimal_value::Coefficient::Mantissa(mantissa)),
        }
    }

    fn expected_trade() -> EventEnvelope {
        EventEnvelope {
            schema_name: "qdl.marketdata.trade".into(),
            schema_major: 2,
            schema_minor: 0,
            event_id: (0_u8..16).collect(),
            instrument_uid: "85ad7cb6-7ebf-5c81-9d82-12c4c10ca85c".into(),
            instrument_id: "BINANCE.USDM.PERPETUAL.BTC-USDT".into(),
            instrument_revision: 7,
            venue: "BINANCE".into(),
            market: "USDM".into(),
            product_type: "PERPETUAL".into(),
            native_symbol: "BTCUSDT".into(),
            provider: "BINANCE_DIRECT".into(),
            source_id: "binance-usdm-trade-003".into(),
            source_role: SourceRole::Primary as i32,
            lease_epoch: 42,
            source_event_time_ns: 1_786_352_400_123_000_000,
            received_at_ns: 1_786_352_400_123_456_000,
            normalized_at_ns: 1_786_352_400_123_500_000,
            published_at_ns: 1_786_352_400_123_700_000,
            source_sequence: "9876543210123456789".into(),
            partition_sequence: 1234,
            normalizer_version: "qdl-normalizer/2.0.0".into(),
            adapter_version: "binance/1.0.0".into(),
            quality_flags: vec![],
            raw_payload_hash: (16_u8..48).collect(),
            correlation_id: "phase1-golden-trade".into(),
            config_revision: 9,
            source_session_id: String::new(),
            connection_generation: 0,
            authority_revision: 0,
            partition_plan_epoch: 0,
            canonical_payload_hash: vec![],
            raw_capture_id: vec![],
            payload: Some(event_envelope::Payload::Trade(Trade {
                native_trade_id: "184467440737095516160".into(),
                price: Some(decimal(6_123_410, 2, "61234.10")),
                quantity: Some(decimal(125, 3, "0.125")),
                aggressor_side: AggressorSide::Buy as i32,
                is_block_trade: false,
                is_buyer_maker: false,
                quantity_unit: QuantityUnit::BaseAsset as i32,
                identity_kind: TradeIdentityKind::Native as i32,
            })),
        }
    }

    #[test]
    fn python_and_rust_share_exact_golden_bytes() {
        let golden = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/golden/canonical/trade-envelope.bin"
        ));
        let expected = expected_trade();
        assert_eq!(expected.encode_to_vec(), golden);

        let decoded = EventEnvelope::decode(golden.as_slice()).expect("decode golden envelope");
        assert_eq!(decoded, expected);
        assert_eq!(decoded.source_sequence, "9876543210123456789");
    }

    fn expected_demand_requirement() -> DemandRequirement {
        DemandRequirement {
            consumer_id: "alpha.grid".into(),
            purpose: DemandPurpose::Alpha as i32,
            universe: Some(UniverseSelector {
                selector_id: "binance-usdm-major".into(),
                venue: "BINANCE".into(),
                market: "USDM".into(),
                product_type: "PERPETUAL".into(),
                universe_ref: "".into(),
                selector: Some(universe_selector::Selector::ExplicitSymbols(
                    ExplicitSymbols {
                        native_symbols: vec!["BTCUSDT".into(), "ETHUSDT".into()],
                    },
                )),
                expected_universe_sha256: vec![],
            }),
            feed: DemandFeedType::Trade as i32,
            interval: "".into(),
            warmup_limit: 0,
            max_freshness_ms: 15_000,
            priority: 10,
            ttl_seconds: 180,
            require_final_bars: false,
            require_live: true,
            execution_grade: false,
            source_policy_id: "crypto_primary_v2".into(),
            depth_levels: 0,
            configuration_revision: 7,
        }
    }

    #[test]
    fn python_and_rust_share_exact_universal_demand_bytes() {
        let golden = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../contracts/golden/demand/universal-demand.bin"
        ));
        let expected = expected_demand_requirement();
        assert_eq!(expected.encode_to_vec(), golden);
        let decoded =
            DemandRequirement::decode(golden.as_slice()).expect("decode universal demand golden");
        assert_eq!(decoded, expected);
    }
}
