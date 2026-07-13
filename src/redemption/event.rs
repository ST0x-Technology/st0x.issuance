use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{
    BurnExternalTxId, IssuerRedemptionRequestId, default_redemption_network,
};
use crate::config::VaultMode;
use crate::mint::{Quantity, TokenizationRequestId};
use crate::tokenized_asset::{Network, TokenSymbol, UnderlyingSymbol};
use crate::vault::{BurnRange, SendableTxWithHash, TxId};

/// Typed classification of an on-chain or pre-submit burn failure.
///
/// Retry-exclusion, log-level selection, and admin grouping key off this
/// typed field, never off parsing the free-text `error` string. Historical
/// `BurningFailed` events predate the field and replay as `Unclassified`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum BurnFailureClassification {
    #[default]
    Unclassified,
    /// The orchestrator's per-token receipt walk cannot cover the requested
    /// burn amount. Token-global anomaly; never auto-retried — recovery is a
    /// manual `EMERGENCY_ROLE` action followed by admin `ResumeBurn`.
    InsufficientReceipts { shortfall: U256 },
    /// The bot-side pre-submit `allowance(bot, orchestrator) >= amount` check
    /// failed. Never auto-retried — ops must grant the approval first.
    AllowanceInsufficient,
    /// The orchestrator reverted because the production vault beacon was
    /// upgraded ahead of its expectations. Environmental halt, not a burn
    /// defect; never advances retry counters.
    VaultLogicMismatch,
    /// Same halt condition as `VaultLogicMismatch`, for the receipt beacon.
    ReceiptLogicMismatch,
}

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
        #[serde(default = "default_redemption_network")]
        network: Network,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        /// The asset's resolved `VaultMode` at detection time. Anchors every
        /// later burn step's mode derivation — never re-resolved from live
        /// config. Historical events predate orchestrator mode and replay as
        /// `VaultDirect`.
        #[serde(default)]
        burn_mode: VaultMode,
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
    /// Redemption of a frozen asset parked before the Alpaca redeem call.
    /// Held is a deferral, never a drop — the tokens are already committed
    /// on-chain, so the redemption resumes (`Held -> AlpacaCalled`, reusing
    /// the existing `AlpacaCalled` event) once the asset unfreezes. Detection
    /// metadata stays in the aggregate from `Detected`, so this event carries
    /// only the hold timestamp.
    RedemptionHeld {
        issuer_request_id: IssuerRedemptionRequestId,
        held_at: DateTime<Utc>,
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
        #[serde(default, alias = "fireblocks_tx_id")]
        tx_id: Option<TxId>,
        /// Planned burns at the time of failure.
        /// Absent for pre-enrichment events.
        #[serde(default)]
        planned_burns: Vec<BurnRecord>,
        /// Typed failure classification. Absent for pre-orchestrator events,
        /// which replay as `Unclassified`.
        #[serde(default)]
        classification: BurnFailureClassification,
    },
    /// Redemption reset to Detected state for reprocessing.
    /// Carries the original metadata so apply() can reconstruct Detected.
    Reprocessed {
        issuer_request_id: IssuerRedemptionRequestId,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        #[serde(default = "default_redemption_network")]
        network: Network,
        wallet: Address,
        quantity: Quantity,
        tx_hash: B256,
        block_number: u64,
        detected_at: DateTime<Utc>,
        previous_state: String,
        reprocessed_at: DateTime<Utc>,
        /// Preserves the redemption's mode anchor across a reset to
        /// `Detected`. Absent on pre-orchestrator events (`VaultDirect`).
        #[serde(default)]
        burn_mode: VaultMode,
    },
    /// Burn transaction submitted to the signing backend.
    /// Persists the backend transaction ID so polling can resume after a restart.
    #[serde(alias = "BurnFireblocksSubmitted")]
    BurnTxSubmitted {
        issuer_request_id: IssuerRedemptionRequestId,
        external_tx_id: BurnExternalTxId,
        #[serde(alias = "fireblocks_tx_id")]
        tx_id: TxId,
        /// Planned burns at the time of submission (for recovery use).
        planned_burns: Vec<BurnRecord>,
        submitted_at: DateTime<Utc>,
    },

    /// Existing on-chain burn discovered during recovery via tx lookup.
    /// Mirrors mint's `ExistingMintRecovered` — the burn already landed on-chain
    /// but the bot failed to record it (e.g. polling timeout).
    ExistingBurnRecovered {
        issuer_request_id: IssuerRedemptionRequestId,
        #[serde(alias = "fireblocks_tx_id")]
        tx_id: TxId,
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
        #[serde(default)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
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
        #[serde(default)]
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
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
        #[serde(default = "default_redemption_network")]
        network: Network,
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
        /// Optional deterministic transaction `externalTxId` for the next burn
        /// submission. Old events did not carry this field.
        #[serde(default)]
        external_tx_id: Option<BurnExternalTxId>,
        resumed_at: DateTime<Utc>,
        /// Preserves the redemption's mode anchor across a resume to
        /// `Burning`. Absent on pre-orchestrator events (`VaultDirect`).
        #[serde(default)]
        burn_mode: VaultMode,
    },
    BurnIntended {
        issuer_request_id: IssuerRedemptionRequestId,
        sendable_tx: SendableTxWithHash,
        planned_burns: Vec<BurnRecord>,
    },
    /// Orchestrator-mode burn transaction broadcast (the counterpart of
    /// `BurnTxSubmitted`). Carries no `planned_burns` — there is no
    /// per-receipt plan to reserve; the orchestrator walks receipts on-chain.
    OrchestratorBurnSubmitted {
        issuer_request_id: IssuerRedemptionRequestId,
        external_tx_id: BurnExternalTxId,
        tx_id: TxId,
        submitted_at: DateTime<Utc>,
    },
    /// Orchestrator-mode burn succeeded on-chain, redemption complete
    /// (terminal success). Carries the consumed receipt pointer range from
    /// the orchestrator's `Burned` event instead of a per-receipt `burns`
    /// list.
    OrchestratorTokensBurned {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        shares_burned: U256,
        /// Consumed receipt-pointer range from the `Burned` event
        /// (`firstReceiptId` / `nextBurnReceiptIdAfter`), half-open.
        burn_range: BurnRange,
        /// Sub-10⁻⁹-token residue retained in the bot wallet, derived from
        /// this redemption's own persisted `AlpacaCalled.dust_quantity`
        /// converted to share-wei — the orchestrator has no multicall to
        /// atomically return dust through (SPEC Decision 6).
        dust_retained: U256,
        gas_used: u64,
        block_number: u64,
        burned_at: DateTime<Utc>,
    },
    /// Orchestrator-mode counterpart of `ExistingBurnRecovered`: an existing
    /// on-chain orchestrator burn discovered during recovery, decoded from the
    /// orchestrator's `Burned` event. Transitions to `Completed`. Carries
    /// `dust_retained` derived the same way as `OrchestratorTokensBurned`
    /// (from the redemption's own persisted `AlpacaCalled.dust_quantity`), so
    /// both paths to terminal success carry identical audit data.
    OrchestratorBurnRecovered {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        shares_burned: U256,
        burn_range: BurnRange,
        dust_retained: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
    BurnRecoveryAttempted {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        action: super::BurnRecoveryAction,
        attempted_at: DateTime<Utc>,
    },
    BurnPreparationRecoveryAttempted {
        issuer_request_id: IssuerRedemptionRequestId,
        attempt: u32,
        attempted_at: DateTime<Utc>,
    },
    BurnRecoveryExhausted {
        issuer_request_id: IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        attempts: u32,
        exhausted_at: DateTime<Utc>,
    },
    BurnPreparationRecoveryExhausted {
        issuer_request_id: IssuerRedemptionRequestId,
        attempts: u32,
        exhausted_at: DateTime<Utc>,
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
            Self::RedemptionHeld { .. } => {
                "RedemptionEvent::RedemptionHeld".to_string()
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
            Self::BurnTxSubmitted { .. } => {
                "RedemptionEvent::BurnTxSubmitted".to_string()
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
            Self::BurnIntended { .. } => {
                "RedemptionEvent::BurnIntended".to_string()
            }
            Self::OrchestratorBurnSubmitted { .. } => {
                "RedemptionEvent::OrchestratorBurnSubmitted".to_string()
            }
            Self::OrchestratorTokensBurned { .. } => {
                "RedemptionEvent::OrchestratorTokensBurned".to_string()
            }
            Self::OrchestratorBurnRecovered { .. } => {
                "RedemptionEvent::OrchestratorBurnRecovered".to_string()
            }
            Self::BurnRecoveryAttempted { .. } => {
                "RedemptionEvent::BurnRecoveryAttempted".to_string()
            }
            Self::BurnPreparationRecoveryAttempted { .. } => {
                "RedemptionEvent::BurnPreparationRecoveryAttempted".to_string()
            }
            Self::BurnRecoveryExhausted { .. } => {
                "RedemptionEvent::BurnRecoveryExhausted".to_string()
            }
            Self::BurnPreparationRecoveryExhausted { .. } => {
                "RedemptionEvent::BurnPreparationRecoveryExhausted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        match self {
            Self::TokensBurned(_)
            | Self::BurningFailed { .. }
            | Self::BurnTxSubmitted { .. }
            | Self::ExistingBurnRecovered { .. }
            | Self::RedemptionClosed { .. }
            | Self::BurnForceCompleted { .. } => "2.0".to_string(),
            _ => "1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, b256, uint};
    use chrono::Utc;
    use serde_json::{from_value, to_value};

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
            acknowledged_unresolved_burn_tx_hash: None,
            completed_at: Utc::now(),
        };

        assert_eq!(event.event_type(), "RedemptionEvent::BurnForceCompleted");
        assert_eq!(event.event_version(), "2.0");
    }

    #[test]
    fn terminal_admin_events_replay_without_acknowledgement_field() {
        let events = [
            RedemptionEvent::RedemptionClosed {
                issuer_request_id: test_redemption_id(),
                reason: "legacy close".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
                closed_at: Utc::now(),
            },
            RedemptionEvent::BurnForceCompleted {
                issuer_request_id: test_redemption_id(),
                burn_tx_hash: B256::random(),
                block_number: 1000,
                reason: "legacy force-complete".to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
                completed_at: Utc::now(),
            },
        ];

        for event in events {
            let mut value = to_value(&event).unwrap();
            let payload = value
                .as_object_mut()
                .unwrap()
                .values_mut()
                .next()
                .unwrap()
                .as_object_mut()
                .unwrap();
            payload.remove("acknowledged_unresolved_burn_tx_hash");

            let replayed: RedemptionEvent = from_value(value).unwrap();
            assert!(matches!(
                replayed,
                RedemptionEvent::RedemptionClosed {
                    acknowledged_unresolved_burn_tx_hash: None,
                    ..
                } | RedemptionEvent::BurnForceCompleted {
                    acknowledged_unresolved_burn_tx_hash: None,
                    ..
                }
            ));
        }
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
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: test_redemption_id(),
            error: "Blockchain error: timeout".to_string(),
            failed_at: Utc::now(),
            tx_id: None,
            planned_burns: vec![],
        };

        assert_eq!(event.event_type(), "RedemptionEvent::BurningFailed");
        assert_eq!(event.event_version(), "2.0");
    }

    #[test]
    fn submitted_and_recovered_burns_use_tagged_tx_id_event_version() {
        let submitted = RedemptionEvent::BurnTxSubmitted {
            issuer_request_id: test_redemption_id(),
            external_tx_id: BurnExternalTxId::from_string(
                "burn-abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    .to_string(),
            ),
            tx_id: TxId::random(),
            planned_burns: vec![],
            submitted_at: Utc::now(),
        };
        let recovered = RedemptionEvent::ExistingBurnRecovered {
            issuer_request_id: test_redemption_id(),
            tx_id: TxId::random(),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            burns: vec![],
            block_number: 1000,
            recovered_at: Utc::now(),
        };

        assert_eq!(submitted.event_version(), "2.0");
        assert_eq!(recovered.event_version(), "2.0");
    }

    #[test]
    fn test_burning_failed_serialization() {
        let event = RedemptionEvent::BurningFailed {
            classification: BurnFailureClassification::Unclassified,
            issuer_request_id: test_redemption_id(),
            error: "Network timeout".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::random()),
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
            tx_id,
            planned_burns,
            classification,
            ..
        } = event
        else {
            panic!("Expected BurningFailed variant");
        };

        assert_eq!(tx_id, None);
        assert!(planned_burns.is_empty());
        assert_eq!(classification, BurnFailureClassification::Unclassified);
    }

    /// Old BurningFailed events stored the transaction id under `fireblocks_tx_id`.
    /// The alias ensures these deserialize into `tx_id` without data loss, so
    /// `/admin/stuck` correctly reports them as awaiting confirmation rather than
    /// awaiting burn submission.
    #[test]
    fn test_backwards_compat_burning_failed_with_fireblocks_tx_id() {
        let json = r#"{
            "BurningFailed": {
                "issuer_request_id": "red-abcdef12",
                "error": "polling timeout",
                "failed_at": "2025-01-01T00:00:00Z",
                "fireblocks_tx_id": "fb-tx-999",
                "planned_burns": []
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::BurningFailed { tx_id, .. } = event else {
            panic!("Expected BurningFailed variant");
        };

        assert_eq!(tx_id, Some(TxId::Legacy("fb-tx-999".to_string())));
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

    /// Pre-multichain `Detected` payloads carry no `network` field; replaying
    /// them must default to Base via `default_redemption_network`, or every
    /// in-flight Base redemption would fail deserialization on upgrade.
    #[test]
    fn test_backwards_compat_detected_without_network_defaults_to_base() {
        let json = r#"{
            "Detected": {
                "issuer_request_id": "red-abcdef12",
                "underlying": "AAPL",
                "token": "tAAPL",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "quantity": "1",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "block_number": 1,
                "detected_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::Detected { network, .. } = event else {
            panic!("Expected Detected variant");
        };

        assert_eq!(network, Network::Base);
    }

    /// Historical `Detected` events predate orchestrator mode; an absent
    /// `burn_mode` must replay as `VaultDirect`.
    #[test]
    fn detected_without_burn_mode_replays_as_vault_direct() {
        let json = r#"{
            "Detected": {
                "issuer_request_id": "red-abcdef12",
                "underlying": "AAPL",
                "token": "tAAPL",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "quantity": "1",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "block_number": 1,
                "detected_at": "2025-01-01T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::Detected { burn_mode, .. } = event else {
            panic!("Expected Detected variant");
        };

        assert_eq!(burn_mode, VaultMode::VaultDirect);
    }

    /// Historical `Reprocessed` events predate orchestrator mode; an absent
    /// `burn_mode` must replay as `VaultDirect`.
    #[test]
    fn reprocessed_without_burn_mode_replays_as_vault_direct() {
        let json = r#"{
            "Reprocessed": {
                "issuer_request_id": "red-abcdef12",
                "underlying": "AAPL",
                "token": "tAAPL",
                "wallet": "0x1234567890abcdef1234567890abcdef12345678",
                "quantity": "1",
                "tx_hash": "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd",
                "block_number": 1,
                "detected_at": "2025-01-01T00:00:00Z",
                "previous_state": "Failed",
                "reprocessed_at": "2025-01-02T00:00:00Z"
            }
        }"#;

        let event: RedemptionEvent = serde_json::from_str(json).unwrap();

        let RedemptionEvent::Reprocessed { burn_mode, .. } = event else {
            panic!("Expected Reprocessed variant");
        };

        assert_eq!(burn_mode, VaultMode::VaultDirect);
    }

    /// Pins the persisted orchestrator wire format: the mode anchor including
    /// the orchestrator address must round-trip through the event store.
    #[test]
    fn detected_with_orchestrator_burn_mode_round_trips() {
        let event = RedemptionEvent::Detected {
            issuer_request_id: test_redemption_id(),
            underlying: UnderlyingSymbol::new("RKLB").unwrap(),
            token: TokenSymbol::new("tRKLB"),
            wallet: alloy::primitives::address!(
                "0x1234567890abcdef1234567890abcdef12345678"
            ),
            quantity: Quantity::default(),
            tx_hash: B256::random(),
            block_number: 7,
            detected_at: Utc::now(),
            burn_mode: VaultMode::Orchestrator {
                address: alloy::primitives::address!(
                    "0x00000000000000000000000000000000000000aa"
                ),
            },
            network: Network::Base,
        };

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();

        assert_eq!(event, deserialized);
    }

    #[test]
    fn orchestrator_event_types_and_round_trips() {
        let submitted = RedemptionEvent::OrchestratorBurnSubmitted {
            issuer_request_id: test_redemption_id(),
            external_tx_id: BurnExternalTxId::from_string(
                "burn-0xabcd".to_string(),
            ),
            tx_id: TxId::random(),
            submitted_at: Utc::now(),
        };
        let burned = RedemptionEvent::OrchestratorTokensBurned {
            issuer_request_id: test_redemption_id(),
            tx_hash: B256::random(),
            shares_burned: uint!(17_000000000000000000_U256),
            burn_range: BurnRange {
                first_receipt_id: uint!(3_U256),
                next_burn_receipt_id_after: uint!(6_U256),
            },
            dust_retained: uint!(1_000_000_000_U256),
            gas_used: 50_000,
            block_number: 45_000_100,
            burned_at: Utc::now(),
        };

        assert_eq!(
            submitted.event_type(),
            "RedemptionEvent::OrchestratorBurnSubmitted"
        );
        assert_eq!(
            burned.event_type(),
            "RedemptionEvent::OrchestratorTokensBurned"
        );
        assert_eq!(submitted.event_version(), "1.0");
        assert_eq!(burned.event_version(), "1.0");

        for event in [submitted, burned] {
            let serialized = serde_json::to_string(&event).unwrap();
            let deserialized: RedemptionEvent =
                serde_json::from_str(&serialized).unwrap();
            assert_eq!(event, deserialized);
        }
    }

    #[test]
    fn orchestrator_burn_recovered_event_type_and_round_trip() {
        let event = RedemptionEvent::OrchestratorBurnRecovered {
            issuer_request_id: test_redemption_id(),
            tx_hash: B256::random(),
            shares_burned: uint!(17_000000000000000000_U256),
            burn_range: BurnRange {
                first_receipt_id: uint!(0_U256),
                next_burn_receipt_id_after: uint!(3_U256),
            },
            dust_retained: uint!(1_000_000_000_U256),
            block_number: 45_000_100,
            recovered_at: Utc::now(),
        };

        assert_eq!(
            event.event_type(),
            "RedemptionEvent::OrchestratorBurnRecovered"
        );
        assert_eq!(event.event_version(), "1.0");

        let serialized = serde_json::to_string(&event).unwrap();
        let deserialized: RedemptionEvent =
            serde_json::from_str(&serialized).unwrap();
        assert_eq!(event, deserialized);
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

        let RedemptionEvent::BurnResumed {
            external_tx_id,
            network,
            burn_mode,
            ..
        } = event
        else {
            panic!("Expected BurnResumed variant");
        };

        assert_eq!(external_tx_id, None);
        assert_eq!(network, Network::Base);
        assert_eq!(burn_mode, VaultMode::VaultDirect);
    }
}
