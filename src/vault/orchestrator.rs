use alloy::primitives::{Address, B256, U256};
use serde::{Deserialize, Serialize};

use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};

/// Parameters for a single `ST0xOrchestrator.burn()` call.
///
/// Unlike the vault-direct [`super::MultiBurnParams`], there is no per-receipt
/// plan and no dust entry: the orchestrator walks its own receipts on-chain
/// and dust is retained in the bot wallet (recorded as `dust_retained` on the
/// confirming event, never returned on-chain).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrchestratorBurnParams {
    /// `ST0xOrchestrator` contract address, taken from the redemption's
    /// persisted `burn_mode` anchor — never from live config.
    pub(crate) orchestrator: Address,
    /// The vault contract, which is also the ERC-20 share token being burned.
    pub(crate) token: Address,
    /// Burn amount in 18-decimal share-wei (the redemption's
    /// `alpaca_quantity`; dust stays in the bot wallet).
    pub(crate) amount: U256,
    /// Bot wallet holding the shares the orchestrator pulls via
    /// `transferFrom`.
    pub(crate) owner: Address,
    /// Redemption's issuer request ID.
    pub(crate) issuer_request_id: IssuerRedemptionRequestId,
    /// Transfer that triggered this redemption, used for the deterministic
    /// `externalTxId` fallback.
    pub(crate) detected_tx_hash: B256,
    /// Optional deterministic `externalTxId` override for retry submissions.
    #[serde(default)]
    pub(crate) external_tx_id: Option<BurnExternalTxId>,
}

/// Result of a confirmed `ST0xOrchestrator.burn()`, parsed from the
/// orchestrator's `Burned` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrchestratorBurnResult {
    pub(crate) tx_hash: B256,
    /// `Burned.amount` — total shares pulled and burned.
    pub(crate) shares_burned: U256,
    /// Consumed receipt pointer range:
    /// `(Burned.firstReceiptId, Burned.nextBurnReceiptIdAfter)`.
    pub(crate) burn_range: (U256, U256),
    pub(crate) gas_used: u64,
    pub(crate) block_number: u64,
}

/// Outcome of the pre-submit orchestrator burn gates (SPEC "Failure States").
///
/// Evaluated allowance-first, then `vaultLogicIsExpected()`, then a burn
/// simulation: an allowance shortfall is an actionable ops failure and must
/// be reported even while the orchestrator is halted, and a deterministic
/// `InsufficientReceipts` revert must be classified here, before anything is
/// signed — without the simulation, the revert would only surface as an
/// unclassified failure of gas estimation during transaction preparation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrchestratorBurnReadiness {
    Ready,
    /// `token.allowance(owner, orchestrator) < amount` — the burn must not be
    /// submitted until ops grants the approval.
    AllowanceInsufficient {
        required: U256,
        current: U256,
    },
    /// `vaultLogicIsExpected()` returned `false` — the orchestrator is halted
    /// pending upgrade; defer without recording any failure.
    VaultLogicMismatch,
    /// The burn simulation reverted with
    /// `InsufficientReceipts(token, shortfall)` — the orchestrator's receipt
    /// walk cannot cover the amount. Token-global anomaly; never submitted
    /// and never auto-retried.
    InsufficientReceipts {
        shortfall: U256,
    },
}

/// Typed revert reason decoded from a mined-but-reverted orchestrator burn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OrchestratorRevertReason {
    /// `InsufficientReceipts(token, shortfall)` — the on-chain receipt walk
    /// cannot cover the burn amount.
    InsufficientReceipts {
        token: Address,
        shortfall: U256,
    },
    VaultLogicMismatch,
    ReceiptLogicMismatch,
    /// Revert data was unavailable or did not decode to one of the
    /// orchestrator's typed errors.
    Unknown,
}
