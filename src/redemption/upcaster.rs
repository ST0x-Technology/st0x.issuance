//! Event upcasters for Redemption aggregate events.
//!
//! Upcasters transform old event formats to new formats when events are loaded
//! from the event store. This allows schema evolution without data migration.

use cqrs_es::persist::{EventUpcaster, SemanticVersionEventUpcaster};
use serde_json::Value;

/// Creates the upcaster for TokensBurned v1.0 → v2.0.
///
/// v1.0 format had single `receipt_id` and `shares_burned` fields.
/// v2.0 format has `burns: Vec<BurnRecord>` for multi-receipt burns.
///
/// This upcaster transforms:
/// ```json
/// {
///   "TokensBurned": {
///     "receipt_id": "0x42",
///     "shares_burned": "0x100",
///     ...other fields...
///   }
/// }
/// ```
/// Into:
/// ```json
/// {
///   "TokensBurned": {
///     "burns": [{ "receipt_id": "0x42", "shares_burned": "0x100" }],
///     ...other fields...
///   }
/// }
/// ```
pub(crate) fn create_tokens_burned_upcaster() -> Box<dyn EventUpcaster> {
    Box::new(SemanticVersionEventUpcaster::new(
        "RedemptionEvent::TokensBurned",
        "2.0",
        Box::new(upcast_tokens_burned_v1_to_v2),
    ))
}

/// Transforms TokensBurned v1.0 payload to v2.0 format.
///
/// Extracts `receipt_id` and `shares_burned` from the top level and wraps
/// them in a `burns` array with a single element.
fn upcast_tokens_burned_v1_to_v2(mut payload: Value) -> Value {
    let Some(tokens_burned) = payload.get_mut("TokensBurned") else {
        return payload;
    };

    let Some(obj) = tokens_burned.as_object_mut() else {
        return payload;
    };

    let receipt_id = obj.remove("receipt_id");
    let shares_burned = obj.remove("shares_burned");

    if let (Some(receipt_id), Some(shares_burned)) = (receipt_id, shares_burned)
    {
        let burn_record = serde_json::json!({
            "receipt_id": receipt_id,
            "shares_burned": shares_burned
        });
        obj.insert("burns".to_string(), serde_json::json!([burn_record]));
    }

    payload
}

#[cfg(test)]
mod tests {
    use cqrs_es::persist::SerializedEvent;
    use proptest::prelude::*;
    use serde_json::{Value, json};

    use super::*;
    use crate::mint::tests::arb_issuer_request_id;

    /// Generates a hex string for U256 values (0x prefixed)
    fn u256_hex_string() -> impl Strategy<Value = String> {
        prop_oneof![
            Just("0x0".to_string()),
            Just("0x1".to_string()),
            Just("0x42".to_string()),
            Just("0xde0b6b3a7640000".to_string()),  // 1e18
            Just("0x56bc75e2d63100000".to_string()), // 100e18
            Just("0x8ac7230489e80000".to_string()), // 10e18
            Just("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".to_string()), // max U256
            "[0-9a-f]{1,64}".prop_map(|s| format!("0x{s}")),
        ]
    }

    prop_compose! {
        /// Generates a 64-char hex string for tx hashes (0x prefixed)
        fn tx_hash_string()(hex in "[0-9a-f]{64}") -> String {
            format!("0x{hex}")
        }
    }

    prop_compose! {
        /// Generates an ISO 8601 timestamp
        fn timestamp_string()(
            y in 2020u32..2030,
            m in 1u32..13,
            d in 1u32..29,
            h in 0u32..24,
            min in 0u32..60,
            s in 0u32..60
        ) -> String {
            format!("{y:04}-{m:02}-{d:02}T{h:02}:{min:02}:{s:02}Z")
        }
    }

    prop_compose! {
        /// Generates a valid v1.0 TokensBurned payload
        fn v1_payload_strategy()(
            issuer_id in arb_issuer_request_id(),
            tx_hash in tx_hash_string(),
            receipt_id in u256_hex_string(),
            shares_burned in u256_hex_string(),
            dust in u256_hex_string(),
            gas in 0u64..1_000_000,
            block in 1u64..100_000_000,
            burned_at in timestamp_string()
        ) -> Value {
            json!({
                "TokensBurned": {
                    "issuer_request_id": issuer_id,
                    "tx_hash": tx_hash,
                    "receipt_id": receipt_id,
                    "shares_burned": shares_burned,
                    "dust_returned": dust,
                    "gas_used": gas,
                    "block_number": block,
                    "burned_at": burned_at
                }
            })
        }
    }

    prop_compose! {
        /// Generates a valid v1.0 payload without dust_returned (older events)
        fn v1_payload_without_dust_strategy()(
            issuer_id in arb_issuer_request_id(),
            tx_hash in tx_hash_string(),
            receipt_id in u256_hex_string(),
            shares_burned in u256_hex_string(),
            gas in 0u64..1_000_000,
            block in 1u64..100_000_000,
            burned_at in timestamp_string()
        ) -> Value {
            json!({
                "TokensBurned": {
                    "issuer_request_id": issuer_id,
                    "tx_hash": tx_hash,
                    "receipt_id": receipt_id,
                    "shares_burned": shares_burned,
                    "gas_used": gas,
                    "block_number": block,
                    "burned_at": burned_at
                }
            })
        }
    }

    fn create_serialized_event(
        event_type: &str,
        event_version: &str,
        payload: Value,
    ) -> SerializedEvent {
        SerializedEvent::new(
            "red-test-aggregate".to_string(),
            1,
            "Redemption".to_string(),
            event_type.to_string(),
            event_version.to_string(),
            payload,
            Value::default(),
        )
    }

    proptest! {
        /// Any v1.x.y version should be upcastable
        #[test]
        fn can_upcast_any_v1_version(minor in 0u32..100, patch in 0u32..100) {
            let upcaster = create_tokens_burned_upcaster();
            let version = format!("1.{minor}.{patch}");

            prop_assert!(
                upcaster.can_upcast("RedemptionEvent::TokensBurned", &version),
                "Should upcast v1.{}.{}", minor, patch
            );
        }

        /// v2.0+ versions should NOT be upcastable
        #[test]
        fn cannot_upcast_v2_or_higher(major in 2u32..10, minor in 0u32..100, patch in 0u32..100) {
            let upcaster = create_tokens_burned_upcaster();
            let version = format!("{major}.{minor}.{patch}");

            prop_assert!(
                !upcaster.can_upcast("RedemptionEvent::TokensBurned", &version),
                "Should NOT upcast v{}.{}.{}", major, minor, patch
            );
        }

        /// Different event types should NOT be upcastable
        #[test]
        fn cannot_upcast_different_event_type(event_type in "[A-Z][a-z]+Event::[A-Z][a-z]+") {
            prop_assume!(event_type != "RedemptionEvent::TokensBurned");
            let upcaster = create_tokens_burned_upcaster();

            prop_assert!(
                !upcaster.can_upcast(&event_type, "1.0"),
                "Should NOT upcast {}", event_type
            );
        }
    }

    // Specific edge cases for can_upcast
    #[test]
    fn can_upcast_returns_false_for_invalid_version() {
        let upcaster = create_tokens_burned_upcaster();
        assert!(
            !upcaster.can_upcast("RedemptionEvent::TokensBurned", "invalid")
        );
    }

    #[test]
    fn can_upcast_returns_false_for_empty_version() {
        let upcaster = create_tokens_burned_upcaster();
        assert!(!upcaster.can_upcast("RedemptionEvent::TokensBurned", ""));
    }

    proptest! {
        /// Upcasting always produces a burns array with exactly one element
        #[test]
        fn upcast_always_creates_single_burn_record(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            let burns = tokens_burned.get("burns").unwrap().as_array().unwrap();
            prop_assert_eq!(burns.len(), 1, "burns array should have exactly 1 element");
        }

        /// Upcasting preserves receipt_id and shares_burned in burns array
        #[test]
        fn upcast_moves_values_to_burns_array(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original_receipt_id = payload["TokensBurned"]["receipt_id"].clone();
            let original_shares_burned = payload["TokensBurned"]["shares_burned"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            let burns = tokens_burned.get("burns").unwrap().as_array().unwrap();
            prop_assert_eq!(burns[0].get("receipt_id").unwrap(), &original_receipt_id);
            prop_assert_eq!(burns[0].get("shares_burned").unwrap(), &original_shares_burned);
        }

        /// Upcasting removes old receipt_id field from top level
        #[test]
        fn upcast_removes_receipt_id_from_top_level(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert!(
                tokens_burned.get("receipt_id").is_none(),
                "receipt_id should be removed from top level"
            );
        }

        /// Upcasting removes old shares_burned field from top level
        #[test]
        fn upcast_removes_shares_burned_from_top_level(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert!(
                tokens_burned.get("shares_burned").is_none(),
                "shares_burned should be removed from top level"
            );
        }

        /// Upcasting always sets event version to "2.0.0" (normalized semver)
        #[test]
        fn upcast_always_sets_version_2_0(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            // SemanticVersionEventUpcaster normalizes to full semver format
            prop_assert_eq!(upcasted.event_version, "2.0.0");
        }

        /// Upcasting preserves issuer_request_id
        #[test]
        fn upcast_preserves_issuer_request_id(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["issuer_request_id"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("issuer_request_id").unwrap(), &original);
        }

        /// Upcasting preserves tx_hash
        #[test]
        fn upcast_preserves_tx_hash(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["tx_hash"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("tx_hash").unwrap(), &original);
        }

        /// Upcasting preserves dust_returned
        #[test]
        fn upcast_preserves_dust_returned(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["dust_returned"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("dust_returned").unwrap(), &original);
        }

        /// Upcasting preserves gas_used
        #[test]
        fn upcast_preserves_gas_used(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["gas_used"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("gas_used").unwrap(), &original);
        }

        /// Upcasting preserves block_number
        #[test]
        fn upcast_preserves_block_number(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["block_number"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("block_number").unwrap(), &original);
        }

        /// Upcasting preserves burned_at
        #[test]
        fn upcast_preserves_burned_at(payload in v1_payload_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original = payload["TokensBurned"]["burned_at"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            prop_assert_eq!(tokens_burned.get("burned_at").unwrap(), &original);
        }

        /// Upcasting preserves aggregate_id
        #[test]
        fn upcast_preserves_aggregate_id(payload in v1_payload_strategy(), agg_id in "[a-z]{3}-[a-z0-9]{1,20}") {
            let upcaster = create_tokens_burned_upcaster();
            let event = SerializedEvent::new(
                agg_id.clone(),
                1,
                "Redemption".to_string(),
                "RedemptionEvent::TokensBurned".to_string(),
                "1.0".to_string(),
                payload,
                Value::default(),
            );

            let upcasted = upcaster.upcast(event);

            prop_assert_eq!(upcasted.aggregate_id, agg_id);
        }

        /// Upcasting preserves sequence number
        #[test]
        fn upcast_preserves_sequence(payload in v1_payload_strategy(), seq in 1usize..10000) {
            let upcaster = create_tokens_burned_upcaster();
            let event = SerializedEvent::new(
                "red-test".to_string(),
                seq,
                "Redemption".to_string(),
                "RedemptionEvent::TokensBurned".to_string(),
                "1.0".to_string(),
                payload,
                Value::default(),
            );

            let upcasted = upcaster.upcast(event);

            prop_assert_eq!(upcasted.sequence, seq);
        }

        /// Upcasting preserves metadata
        #[test]
        fn upcast_preserves_metadata(
            payload in v1_payload_strategy(),
            corr_id in "[a-z0-9]{8}",
            user in "[a-z]{4,8}"
        ) {
            let upcaster = create_tokens_burned_upcaster();
            let metadata = json!({"correlation_id": corr_id, "user": user});
            let event = SerializedEvent::new(
                "red-test".to_string(),
                1,
                "Redemption".to_string(),
                "RedemptionEvent::TokensBurned".to_string(),
                "1.0".to_string(),
                payload,
                metadata.clone(),
            );

            let upcasted = upcaster.upcast(event);

            prop_assert_eq!(upcasted.metadata, metadata);
        }

        /// Older v1.0 events without dust_returned field should still upcast
        #[test]
        fn upcast_handles_missing_dust_returned(payload in v1_payload_without_dust_strategy()) {
            let upcaster = create_tokens_burned_upcaster();
            let original_receipt_id = payload["TokensBurned"]["receipt_id"].clone();
            let original_shares_burned = payload["TokensBurned"]["shares_burned"].clone();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            // Core transformation should still work
            let tokens_burned = upcasted.payload.get("TokensBurned").unwrap();
            let burns = tokens_burned.get("burns").unwrap().as_array().unwrap();
            prop_assert_eq!(burns.len(), 1);
            prop_assert_eq!(burns[0].get("receipt_id").unwrap(), &original_receipt_id);
            prop_assert_eq!(burns[0].get("shares_burned").unwrap(), &original_shares_burned);
        }
    }

    proptest! {
        /// Any upcasted v1.0 payload should deserialize to the current RedemptionEvent
        #[test]
        fn upcasted_payload_deserializes_to_redemption_event(payload in v1_payload_strategy()) {
            use crate::redemption::RedemptionEvent;

            let upcaster = create_tokens_burned_upcaster();
            let event = create_serialized_event("RedemptionEvent::TokensBurned", "1.0", payload);

            let upcasted = upcaster.upcast(event);

            // The upcasted payload should deserialize to the current RedemptionEvent type
            let deserialized: RedemptionEvent =
                serde_json::from_value(upcasted.payload).unwrap();

            let RedemptionEvent::TokensBurned { burns, .. } = deserialized else {
                panic!("Expected TokensBurned variant");
            };

            prop_assert_eq!(burns.len(), 1);
        }
    }
}
