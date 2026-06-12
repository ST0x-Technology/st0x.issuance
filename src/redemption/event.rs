use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{BurnExternalTxId, IssuerRedemptionRequestId};
use crate::mint::{Quantity, TokenizationRequestId};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

/// A single burn operation within a multi-receipt burn.
///
/// Each burn targets a specific ERC-1155 receipt and burns a portion
/// (or all) of the shares associated with that receipt.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BurnRecord {
    /// The ERC-1155 receipt ID that was burned from
    pub(crate) receipt_id: U256,
    /// Number of shares burned from this receipt
    pub(crate) shares_burned: U256,
}

/// Payload of [`RedemptionEvent::TokensBurned`].
///
/// Wrapped in a newtype variant so its `Deserialize` can transparently accept
/// the legacy v1.0 on-disk shape — a single top-level `receipt_id` +
/// `shares_burned` — and normalize it into the v2.0 `burns` array. event-sorcery
/// has no upcaster layer (unlike the cqrs-es `SemanticVersionEventUpcaster` this
/// replaces), so the transformation that ran at load time is now performed during
/// deserialization, leaving the stored events byte-for-byte unchanged. See
/// [`TokensBurnedDataWire`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(try_from = "TokensBurnedDataWire")]
pub(crate) struct TokensBurnedData {
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    pub(crate) tx_hash: B256,
    /// All receipt burns performed in this transaction (v2.0 multi-burn shape).
    pub(crate) burns: Vec<BurnRecord>,
    /// Amount of dust returned to user (with 18 decimals). Dust recipient is
    /// always `metadata.wallet` from the `Detected` event. Defaults to zero for
    /// events prior to the dust handling feature.
    pub(crate) dust_returned: U256,
    pub(crate) gas_used: u64,
    pub(crate) block_number: u64,
    pub(crate) burned_at: DateTime<Utc>,
}

/// Wire shape for [`TokensBurnedData`] accepting both the v2.0 `burns` array and
/// the legacy v1.0 flat `receipt_id`/`shares_burned` pair. `TryFrom` reconstructs
/// the single-element `burns` array from the legacy fields when `burns` is absent.
#[derive(Deserialize)]
struct TokensBurnedDataWire {
    issuer_request_id: IssuerRedemptionRequestId,
    tx_hash: B256,
    #[serde(default)]
    burns: Vec<BurnRecord>,
    #[serde(default)]
    receipt_id: Option<U256>,
    #[serde(default)]
    shares_burned: Option<U256>,
    #[serde(default)]
    dust_returned: U256,
    gas_used: u64,
    block_number: u64,
    burned_at: DateTime<Utc>,
}

impl TryFrom<TokensBurnedDataWire> for TokensBurnedData {
    type Error = LegacyTokensBurnedError;

    fn try_from(wire: TokensBurnedDataWire) -> Result<Self, Self::Error> {
        let burns = match (
            wire.burns.is_empty(),
            wire.receipt_id,
            wire.shares_burned,
        ) {
            (false, _, _) => wire.burns,
            (true, Some(receipt_id), Some(shares_burned)) => {
                vec![BurnRecord { receipt_id, shares_burned }]
            }
            (true, _, _) => return Err(LegacyTokensBurnedError),
        };

        Ok(Self {
            issuer_request_id: wire.issuer_request_id,
            tx_hash: wire.tx_hash,
            burns,
            dust_returned: wire.dust_returned,
            gas_used: wire.gas_used,
            block_number: wire.block_number,
            burned_at: wire.burned_at,
        })
    }
}

#[derive(Debug, thiserror::Error)]
#[error(
    "TokensBurned event carries neither a `burns` array nor legacy \
     `receipt_id`/`shares_burned` fields"
)]
pub(crate) struct LegacyTokensBurnedError;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum RedemptionEvent {
    Detected {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
    },
    AlpacaCalled {
        issuer_request_id: IssuerRedemptionRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Quantity sent to Alpaca (truncated to 9 decimals).
        /// For events prior to dust handling feature: defaults to zero.
        #[serde(default)]
        alpaca_quantity: Quantity,
        /// Dust quantity to be returned to user (original - alpaca_quantity).
        /// For events prior to dust handling feature: defaults to zero.
        #[serde(default)]
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
    },
    AlpacaCallFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    AlpacaJournalCompleted {
        issuer_request_id: IssuerRedemptionRequestId,
        alpaca_journal_completed_at: DateTime<Utc>,
    },
    RedemptionFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    TokensBurned(TokensBurnedData),
    BurningFailed {
        issuer_request_id: IssuerRedemptionRequestId,
        error: String,
        failed_at: DateTime<Utc>,
        /// Fireblocks transaction ID, if the burn was submitted via Fireblocks.
        /// Absent for non-Fireblocks backends or pre-enrichment events.
        #[serde(default)]
        fireblocks_tx_id: Option<String>,
        /// Planned burns at the time of failure.
        /// Absent for pre-enrichment events.
        #[serde(default)]
        planned_burns: Vec<BurnRecord>,
    },
    /// Redemption reset to Detected state for reprocessing.
    /// Carries the original metadata so apply() can reconstruct Detected.
    Reprocessed {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        previous_state: String,
        reprocessed_at: DateTime<Utc>,
    },
    /// Burn transaction submitted to the signing backend.
    /// Persists the backend transaction ID so polling can resume after a restart.
    BurnFireblocksSubmitted {
        issuer_request_id: IssuerRedemptionRequestId,
        external_tx_id: BurnExternalTxId,
        fireblocks_tx_id: String,
        /// Planned burns at the time of submission (for recovery use).
        planned_burns: Vec<BurnRecord>,
        submitted_at: DateTime<Utc>,
    },

    /// Existing on-chain burn discovered during recovery via Fireblocks tx lookup.
    /// Mirrors mint's `ExistingMintRecovered` — the burn already landed on-chain
    /// but the bot failed to record it (e.g., Fireblocks polling timeout).
    ExistingBurnRecovered {
        issuer_request_id: IssuerRedemptionRequestId,
        fireblocks_tx_id: String,
        tx_hash: B256,
        burns: Vec<BurnRecord>,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
    /// Admin-closed redemption that cannot be automatically recovered.
    /// Terminal state — closed redemptions do not appear in stuck queries.
    RedemptionClosed {
        issuer_request_id: IssuerRedemptionRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },
    /// Admin-recorded terminal success for a redemption stuck in
    /// `Burning`/`BurnSubmitted` whose burn already landed on-chain but was
    /// never recorded (e.g. a crash between the burn and `TokensBurned`). The
    /// admin layer verifies `burn_tx_hash` on-chain before this event is
    /// emitted. Transitions to `Completed`, recording the proving tx hash for
    /// audit.
    BurnForceCompleted {
        issuer_request_id: IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        block_number: u64,
        reason: String,
        completed_at: DateTime<Utc>,
    },
    /// Post-Alpaca failed redemption resumed directly to Burning state.
    /// Used when Alpaca was already called and the journal eventually completed,
    /// but the bot had already timed out and marked the redemption as Failed.
    /// Carries all data needed to reconstruct the Burning state.
    BurnResumed {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        tokenization_request_id: TokenizationRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
        called_at: DateTime<Utc>,
        /// Alpaca's `updated_at` for the completed journal — the closest
        /// approximation we have to the actual journal completion time.
        alpaca_journal_completed_at: DateTime<Utc>,
        /// Optional deterministic Fireblocks `externalTxId` for the next burn
        /// submission. Old events did not carry this field.
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
        resumed_at: DateTime<Utc>,
    },
}

impl DomainEvent for RedemptionEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Detected { .. } => "RedemptionEvent::Detected".to_string(),
            Self::AlpacaCalled { .. } => {
                "RedemptionEvent::AlpacaCalled".to_string()
            }
            Self::AlpacaCallFailed { .. } => {
                "RedemptionEvent::AlpacaCallFailed".to_string()
            }
            Self::AlpacaJournalCompleted { .. } => {
                "RedemptionEvent::AlpacaJournalCompleted".to_string()
            }
            Self::RedemptionFailed { .. } => {
                "RedemptionEvent::RedemptionFailed".to_string()
            }
            Self::TokensBurned(_) => {
                "RedemptionEvent::TokensBurned".to_string()
            }
            Self::BurningFailed { .. } => {
                "RedemptionEvent::BurningFailed".to_string()
            }
            Self::Reprocessed { .. } => {
                "RedemptionEvent::Reprocessed".to_string()
            }
            Self::BurnResumed { .. } => {
                "RedemptionEvent::BurnResumed".to_string()
            }
            Self::BurnFireblocksSubmitted { .. } => {
                "RedemptionEvent::BurnFireblocksSubmitted".to_string()
            }
            Self::ExistingBurnRecovered { .. } => {
                "RedemptionEvent::ExistingBurnRecovered".to_string()
            }
            Self::RedemptionClosed { .. } => {
                "RedemptionEvent::RedemptionClosed".to_string()
            }
            Self::BurnForceCompleted { .. } => {
                "RedemptionEvent::BurnForceCompleted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        match self {
            Self::TokensBurned(_) => "2.0".to_string(),
            _ => "1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, b256, uint};
    use chrono::Utc;

    use super::*;

    fn test_redemption_id() -> IssuerRedemptionRequestId {
        IssuerRedemptionRequestId::new(b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        ))
    }

    #[test]
    fn test_alpaca_journal_completed_event_type() {
        let event = RedemptionEvent::AlpacaJournalCompleted {
            issuer_request_id: test_redemption_id(),
            alpaca_journal_completed_at: Utc::now(),
        };

        assert_eq!(
            event.event_type(),
            "RedemptionEvent::AlpacaJournalCompleted"
        );
    }

    #[test]
    fn test_tokens_burned_event_type() {
        let event = RedemptionEvent::TokensBurned(TokensBurnedData {
            issuer_request_id: test_redemption_id(),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            burns: vec![BurnRecord {
                receipt_id: uint!(42_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
            dust_returned: U256::ZERO,
            gas_used: 50000,
            block_number: 1000,
            burned_at: Utc::now(),
        });

        assert_eq!(event.event_type(), "RedemptionEvent::TokensBurned");
        assert_eq!(event.event_version(), "2.0");
    }

    #[test]
    fn test_burn_force_completed_event_type() {
        let event = RedemptionEvent::BurnForceCompleted {
            issuer_request_id: test_redemption_id(),
            burn_tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            block_number: 1000,
            reason: "admin recovery".to_string(),
            completed_at: Utc::now(),
        };

        assert_eq!(event.event_type(), "RedemptionEvent::BurnForceCompleted");
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn test_tokens_burned_serialization() {
        let event = RedemptionEvent::TokensBurned(TokensBurnedData {
            issuer_request_id: test_redemption_id(),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            burns: vec![BurnRecord {
                receipt_id: uint!(7_U256),
                shares_burned: uint!(250_500000000000000000_U256),
            }],
            dust_returned: U256::ZERO,
            gas_used: 75000,
            block_number: 2000,
            burned_at: Utc::now(),
        });

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn test_burning_failed_event_type() {
        let event = RedemptionEvent::BurningFailed {
            issuer_request_id: test_redemption_id(),
            error: "Blockchain error: timeout".to_string(),
            failed_at: Utc::now(),
            fireblocks_tx_id: None,
            planned_burns: vec![],
        };

        assert_eq!(event.event_type(), "RedemptionEvent::BurningFailed");
        assert_eq!(event.event_version(), "1.0");
    }

    #[test]
    fn test_burning_failed_serialization() {
        let event = RedemptionEvent::BurningFailed {
            issuer_request_id: test_redemption_id(),
            error: "Network timeout".to_string(),
            failed_at: Utc::now(),
            fireblocks_tx_id: Some("fb-tx-123".to_string()),
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(7_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
        };

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(event, deserialized);
    }

    /// Tests that old BurningFailed events without the new fields deserialize correctly.
    #[test]
    fn test_backwards_compat_burning_failed_without_enrichment_fields() {
        let json = r#"{
            "BurningFailed": {
                "issuer_request_id": "red-abcdef12",
                "error": "polling timeout",
                "failed_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::BurningFailed {
            fireblocks_tx_id,
            planned_burns,
            ..
        } = event
        else {
            panic!("Expected BurningFailed variant");
        };

        assert_eq!(fireblocks_tx_id, None);
        assert!(planned_burns.is_empty());
    }

    #[test]
    fn test_backwards_compat_alpaca_called_without_dust_fields() {
        let json = r#"{
            "AlpacaCalled": {
                "issuer_request_id": "red-abcdef12",
                "tokenization_request_id": "tok-old-123",
                "called_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::AlpacaCalled {
            alpaca_quantity,
            dust_quantity,
            ..
        } = event
        else {
            panic!("Expected AlpacaCalled variant");
        };

        assert_eq!(alpaca_quantity, Quantity::default());
        assert_eq!(dust_quantity, Quantity::default());
    }

    /// Tests that v2.0 TokensBurned events without dust_returned field default to zero.
    #[test]
    fn test_backwards_compat_tokens_burned_v2_without_dust_fields() {
        let json = r#"{
            "TokensBurned": {
                "issuer_request_id": "red-abcdef12",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "burns": [{"receipt_id": "0x42", "shares_burned": "0x56bc75e2d63100000"}],
                "gas_used": 50000,
                "block_number": 1000,
                "burned_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::TokensBurned(TokensBurnedData {
            dust_returned,
            burns,
            ..
        }) = event
        else {
            panic!("Expected TokensBurned variant");
        };

        assert_eq!(dust_returned, U256::ZERO);
        assert_eq!(burns.len(), 1);
        assert_eq!(burns[0].receipt_id, uint!(0x42_U256));
    }

    /// Tests that a legacy v1.0 `TokensBurned` event — a flat top-level
    /// `receipt_id` + `shares_burned` with no `burns` array — deserializes into
    /// a single-element `burns` array. This verifies the tolerant
    /// `TryFrom<TokensBurnedDataWire>` that replaces the cqrs-es upcaster.
    #[test]
    fn test_backwards_compat_tokens_burned_v1_flat_receipt() {
        let json = r#"{
            "TokensBurned": {
                "issuer_request_id": "red-abcdef12",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "receipt_id": "0x42",
                "shares_burned": "0x56bc75e2d63100000",
                "gas_used": 50000,
                "block_number": 1000,
                "burned_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::TokensBurned(TokensBurnedData {
            burns,
            dust_returned,
            ..
        }) = event
        else {
            panic!("Expected TokensBurned variant");
        };

        assert_eq!(burns.len(), 1);
        assert_eq!(
            burns[0],
            BurnRecord {
                receipt_id: uint!(0x42_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }
        );
        assert_eq!(dust_returned, U256::ZERO);
    }

    /// Tests that old BurnResumed events without external_tx_id default to None.
    #[test]
    fn test_backwards_compat_burn_resumed_without_external_tx_id() {
        let json = r#"{
            "BurnResumed": {
                "issuer_request_id": "red-abcdef12",
                "underlying": "AAPL",
                "token": "tAAPL",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "quantity": "1",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "block_number": 1,
                "detected_at": "2025-01-01T00:00:00Z",
                "tokenization_request_id": "tok-1",
                "alpaca_quantity": "1",
                "dust_quantity": "0",
                "called_at": "2025-01-01T00:00:00Z",
                "alpaca_journal_completed_at": "2025-01-01T00:00:00Z",
                "resumed_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::BurnResumed { external_tx_id, .. } = event else {
            panic!("Expected BurnResumed variant");
        };

        assert_eq!(external_tx_id, None);
    }
}
