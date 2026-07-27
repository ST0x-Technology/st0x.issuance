use alloy::primitives::{Address, Bytes, TxHash};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::{ReceiptId, Shares};
use crate::mint::IssuerMintRequestId;
use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
use crate::vault::ReceiptInformation;

/// Identifies how a receipt was created.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptSource {
    /// Receipt from Alpaca Instant Tokenization Network (ITN).
    /// The issuer_request_id links this receipt to the mint operation that created it.
    Itn { issuer_request_id: IssuerMintRequestId },
    /// External receipt for mints not performed by this service
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryEvent {
    Discovered {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
        #[serde(default)]
        receipt_info: Option<Box<ReceiptInformation>>,
        /// Original on-chain encoded bytes for the receipt information.
        /// Preserved so that redeem() passes back the exact bytes from deposit(),
        /// avoiding re-encoding legacy JSON receipts as CBOR.
        #[serde(default)]
        receipt_info_bytes: Option<Bytes>,
    },
    BalanceReconciled {
        receipt_id: ReceiptId,
        previous_balance: Shares,
        on_chain_balance: Shares,
    },
    Depleted {
        receipt_id: ReceiptId,
    },
    BurnReserved {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        burns: Vec<BurnRecord>,
    },
    /// A reservation was released without consuming on-chain shares (the burn
    /// failed definitively or was never submitted). Keyed only by redemption:
    /// `apply` clears the redemption's reservation wherever it is held.
    BurnReleased {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
    /// A submitted burn confirmed on-chain. The redemption's reservation is
    /// consumed: it is removed and the receipt's mirror balance is reduced by
    /// the reserved amount, reflecting the shares that left the vault. Keyed
    /// only by redemption; `apply` uses the stored reserved amounts.
    BurnSettled {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
    /// The wallet these balances belong to, recorded the first time a
    /// reconciliation pass confirms it holds the tracked receipts.
    ///
    /// Balances here are `balanceOf(holder, receipt_id)` readings, so the holder
    /// is part of what they mean. Recording it is what lets a later signer
    /// rotation be recognised as a rotation instead of read as every receipt
    /// having been spent at once. Emitted when the verified holder differs
    /// from the recorded one — the first confirmation, or a wallet verified to
    /// hold every tracked receipt after a rotation. Re-confirming the wallet
    /// already on record is a no-op, never an event per pass.
    CustodyConfirmed {
        holder: Address,
    },
    /// Custody of every tracked receipt moved to a replacement wallet.
    ///
    /// `from` is retained because it is where a rollback returns custody to, so
    /// reversing a migration needs no address from an operator.
    CustodyMigrated {
        from: Address,
        to: Address,
        tx_hash: TxHash,
    },
}

impl DomainEvent for ReceiptInventoryEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Discovered { .. } => {
                "ReceiptInventoryEvent::Discovered".to_string()
            }
            Self::BalanceReconciled { .. } => {
                "ReceiptInventoryEvent::BalanceReconciled".to_string()
            }
            Self::Depleted { .. } => {
                "ReceiptInventoryEvent::Depleted".to_string()
            }
            Self::BurnReserved { .. } => {
                "ReceiptInventoryEvent::BurnReserved".to_string()
            }
            Self::BurnReleased { .. } => {
                "ReceiptInventoryEvent::BurnReleased".to_string()
            }
            Self::BurnSettled { .. } => {
                "ReceiptInventoryEvent::BurnSettled".to_string()
            }
            Self::CustodyConfirmed { .. } => {
                "ReceiptInventoryEvent::CustodyConfirmed".to_string()
            }
            Self::CustodyMigrated { .. } => {
                "ReceiptInventoryEvent::CustodyMigrated".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
