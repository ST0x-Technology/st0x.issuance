use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{
    ClientId, IssuerRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintEvent {
    Initiated {
        issuer_request_id: IssuerRequestId,
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
        issuer_request_id: IssuerRequestId,
        confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    MintingStarted {
        issuer_request_id: IssuerRequestId,
        started_at: DateTime<Utc>,
    },
    TokensMinted {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerRequestId,
        error: String,
        failed_at: DateTime<Utc>,
    },
    MintCompleted {
        issuer_request_id: IssuerRequestId,
        completed_at: DateTime<Utc>,
    },
    /// Indicates that an existing on-chain mint was discovered during recovery.
    ExistingMintRecovered {
        issuer_request_id: IssuerRequestId,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        block_number: u64,
        recovered_at: DateTime<Utc>,
    },
    /// Indicates that a mint retry has started during recovery.
    MintRetryStarted {
        issuer_request_id: IssuerRequestId,
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
            Self::MintRetryStarted { .. } => {
                "MintEvent::MintRetryStarted".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
