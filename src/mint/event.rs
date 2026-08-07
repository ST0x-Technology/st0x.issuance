use alloy::primitives::{Address, B256, TxHash, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::vault::{PreparedMintTx, TxId};

use super::{
    ClientId, IssuerMintRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintEvent {
    Initiated {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
    },
    JournalConfirmed {
        issuer_request_id: IssuerMintRequestId,
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    MintingStarted {
        issuer_request_id: IssuerMintRequestId,
        started_at: DateTime<Utc>,
    },
    /// Exact signed transaction persisted before any broadcast.
    MintTxIntended {
        issuer_request_id: IssuerMintRequestId,
        prepared_tx: PreparedMintTx,
        intended_at: DateTime<Utc>,
    },
    TokensMinted {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerMintRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    MintCompleted {
        issuer_request_id: IssuerMintRequestId,
        completed_at: DateTime<Utc>,
    },
    /// Indicates that an existing on-chain mint was discovered during recovery.
    ExistingMintRecovered {
        issuer_request_id: IssuerMintRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
    /// Admin-closed mint that cannot be automatically recovered.
    /// Terminal state — closed mints do not appear in stuck queries.
    MintClosed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },

    /// Mint transaction submitted to the signing backend (Turnkey or local).
    /// Persists the backend transaction ID so that polling can resume after
    /// a restart without resubmitting (which would double-mint).
    #[serde(alias = "FireblocksSubmitted")]
    MintTxSubmitted {
        issuer_request_id: IssuerMintRequestId,
        external_tx_id: String,
        #[serde(alias = "fireblocks_tx_id")]
        tx_id: TxId,
        submitted_at: DateTime<Utc>,
    },

    /// Indicates that a mint retry has started during recovery.
    MintRetryStarted {
        issuer_request_id: IssuerMintRequestId,
        /// The on-chain tx hash that evidences the original mint succeeded.
        /// Present when recovery is triggered by receipt discovery, `None`
        /// when triggered by startup auto-recovery (which may retry the mint).
        #[serde(default)]
        tx_hash: Option<TxHash>,
        /// Present only for operator-authorized retries. Older and automatic
        /// retry events default to `None` during replay.
        #[serde(default)]
        manual_retry_id: Option<Uuid>,
        started_at: DateTime<Utc>,
    },
}

impl DomainEvent for MintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Initiated { .. } => "MintEvent::Initiated".to_string(),
            Self::JournalConfirmed { .. } => {
                "MintEvent::JournalConfirmed".to_string()
            }
            Self::JournalRejected { .. } => {
                "MintEvent::JournalRejected".to_string()
            }
            Self::MintingStarted { .. } => {
                "MintEvent::MintingStarted".to_string()
            }
            Self::MintTxIntended { .. } => {
                "MintEvent::MintTxIntended".to_string()
            }
            Self::TokensMinted { .. } => "MintEvent::TokensMinted".to_string(),
            Self::MintingFailed { .. } => {
                "MintEvent::MintingFailed".to_string()
            }
            Self::MintCompleted { .. } => {
                "MintEvent::MintCompleted".to_string()
            }
            Self::ExistingMintRecovered { .. } => {
                "MintEvent::ExistingMintRecovered".to_string()
            }
            Self::MintClosed { .. } => "MintEvent::MintClosed".to_string(),
            Self::MintTxSubmitted { .. } => {
                "MintEvent::MintTxSubmitted".to_string()
            }
            Self::MintRetryStarted { .. } => {
                "MintEvent::MintRetryStarted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        match self {
            Self::MintTxSubmitted { .. } => "2.0".to_string(),
            _ => "1.0".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn raw_fireblocks_submitted_event_replays_populated_legacy_tx_id() {
        let raw_event = json!({
            "FireblocksSubmitted": {
                "issuer_request_id": "550e8400-e29b-41d4-a716-446655440000",
                "external_tx_id": "mint-550e8400-e29b-41d4-a716-446655440000",
                "fireblocks_tx_id": "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f",
                "submitted_at": "2026-07-14T12:00:00Z"
            }
        });

        let event: MintEvent = serde_json::from_value(raw_event).unwrap();

        assert!(matches!(
            event,
            MintEvent::MintTxSubmitted {
                tx_id: TxId::Legacy(ref value),
                ref external_tx_id,
                ..
            } if value == "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f"
                && external_tx_id
                    == "mint-550e8400-e29b-41d4-a716-446655440000"
        ));
    }

    #[test]
    fn mint_tx_submitted_uses_tagged_tx_id_event_version() {
        let event = MintEvent::MintTxSubmitted {
            issuer_request_id: "550e8400-e29b-41d4-a716-446655440000"
                .parse()
                .unwrap(),
            external_tx_id: "mint-550e8400-e29b-41d4-a716-446655440000"
                .to_string(),
            tx_id: TxId::Legacy(
                "07bdef3c-5314-4d1d-94f7-f3f346cd4c2f".to_string(),
            ),
            submitted_at: Utc::now(),
        };

        assert_eq!(event.event_version(), "2.0");
    }
}
