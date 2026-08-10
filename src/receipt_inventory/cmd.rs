use alloy::primitives::{Address, Bytes, TxHash};
use serde::{Deserialize, Serialize};

use super::event::ReceiptSource;
use super::{ReceiptId, Shares};
use crate::mint::IssuerMintRequestId;
use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
use crate::vault::ReceiptInformation;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReceiptInventoryCommand {
    DiscoverReceipt {
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        source: ReceiptSource,
        receipt_info: Option<Box<ReceiptInformation>>,
        receipt_info_bytes: Option<Bytes>,
    },
    /// Apply an on-chain `balanceOf(observed_wallet, receipt_id)` reading.
    ///
    /// The reading only means anything relative to the wallet it was taken
    /// against, so the handler refuses it when that wallet is not the recorded
    /// custody holder — and refuses a destructive zero reading outright while
    /// custody has never been confirmed. This is the aggregate-level backstop
    /// no balance reader can bypass; see [`super::Custody`].
    ReconcileBalance {
        receipt_id: ReceiptId,
        on_chain_balance: Shares,
        observed_wallet: Address,
    },
    ReserveBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        burns: Vec<BurnRecord>,
    },
    ReleaseBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
    SettleBurn {
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    },
    /// Record the wallet these balances were read against, once a
    /// reconciliation pass has confirmed it holds the tracked receipts.
    ///
    /// Idempotent: re-confirming the wallet already held produces no event, so
    /// the periodic reconciler cannot grow the log a pass at a time.
    ConfirmCustody {
        holder: Address,
    },
    /// Record that custody of every tracked receipt moved to a new wallet.
    ///
    /// `tx_hash` is `None` when a completed move is re-observed without a
    /// transaction submitted by this execution, including historical moves
    /// signed by an external custodian. Idempotent and keyed on the destination
    /// alone: a move whose `to` is already the recorded holder produces no
    /// event, whatever `from` it claims.
    RecordCustodyMigration {
        from: Address,
        to: Address,
        tx_hash: Option<TxHash>,
    },
    /// Record a second ITN `Deposit` observed for an already-tracked request.
    ///
    /// Records the observation only; it never applies the discovery, so the
    /// 1:1 index is untouched. Idempotent on the duplicate's own identity so a
    /// re-scanned block range records it once.
    RecordConflictingItnDeposit {
        issuer_request_id: IssuerMintRequestId,
        discovered_receipt_id: ReceiptId,
        discovered_tx_hash: TxHash,
        discovered_block_number: u64,
    },
}
