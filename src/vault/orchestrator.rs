use alloy::primitives::{Address, B256, Bytes, U256};
use serde::{Deserialize, Serialize};

use crate::VaultMode;
use crate::bindings::IST0xOrchestratorV1;
use crate::redemption::{BurnExternalTxId, IssuerRedemptionRequestId};
use crate::vault::ReceiptInformation;

/// A recipient-produced `MintAuthV1` authorizing one orchestrator mint.
///
/// The liquidity bot (the AP controlling the recipient wallet) picks a random
/// nonce and signs the EIP-712 `MintAuthV1` over `(token, to, amount, nonce)`
/// with the recipient wallet's key, then delivers it via the internal
/// mint-authorization call. Persisted verbatim on
/// `MintAuthorizationReceived`, so this serde shape is permanent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct MintAuthorization {
    /// Recipient-chosen random nonce; `(to, nonce)` is single-use on-chain.
    pub(crate) nonce: B256,
    /// Opaque recipient signature over the `MintAuthV1` digest. EMPTY is a
    /// valid input: the future Atomic Bridge recipient authorizes via the
    /// orchestrator's `authorizeMint` callback instead of a signature, so
    /// validation skips signer recovery for empty bytes (SPEC Decision 1).
    pub(crate) signature: Bytes,
}

impl MintAuthorization {
    pub(crate) fn to_binding(&self) -> IST0xOrchestratorV1::MintAuthV1 {
        IST0xOrchestratorV1::MintAuthV1 {
            nonce: self.nonce,
            signature: self.signature.clone(),
        }
    }
}

/// Parameters for a single `ST0xOrchestrator.mint()` call.
///
/// Unlike the vault-direct deposit multicall there is no `previewDeposit` and
/// no share transfer: the orchestrator asserts 1:1 on-chain
/// (`VaultAmountMismatch` otherwise), keeps the receipt, and forwards the
/// ERC-20 shares to `to`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct OrchestratorMintParams {
    /// `ST0xOrchestrator` contract address, taken from the mint's persisted
    /// `mint_mode` anchor — never from live config.
    pub(crate) orchestrator: Address,
    /// The vault contract, which is also the ERC-20 share token being minted.
    pub(crate) token: Address,
    /// Recipient wallet — the exact `to` the authorization was signed over.
    pub(crate) to: Address,
    /// Mint amount in 18-decimal share-wei — the exact `amount` the
    /// authorization was signed over.
    pub(crate) amount: U256,
    /// The liquidity bot's `MintAuthV1` for this mint; the on-chain call must
    /// use exactly the signed `(token, to, amount, nonce)`.
    pub(crate) authorization: MintAuthorization,
    /// Off-chain audit payload forwarded verbatim to `vault.mint`, keeping the
    /// CBOR audit-trail format identical to vault-direct deposits.
    pub(crate) receipt_info: ReceiptInformation,
    /// Optional deterministic `externalTxId` override for retry submissions.
    #[serde(default)]
    pub(crate) external_tx_id: Option<String>,
}

/// Result of a confirmed `ST0xOrchestrator.mint()`, parsed from the
/// orchestrator's `Minted` event. Carries the consumed `nonce` in place of
/// vault-direct's `receipt_id` — the orchestrator, not the bot, owns receipt
/// custody.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrchestratorMintResult {
    pub(crate) tx_hash: B256,
    /// `Minted.nonce` — the authorization nonce this mint consumed.
    pub(crate) nonce: B256,
    /// `Minted.amount` — shares minted and forwarded to the recipient.
    pub(crate) shares_minted: U256,
    pub(crate) gas_used: u64,
    pub(crate) block_number: u64,
}

/// Query identifying one specific mint's landing on-chain: the full-match
/// key `(to, nonce, token, amount)` plus the orchestrator whose `Minted`
/// logs are scanned. Named fields, because three of these are same-typed
/// addresses — positional arguments would let a call site transpose them
/// and compile cleanly while querying the wrong pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MintedLogQuery {
    pub(crate) orchestrator: Address,
    /// Recipient wallet the mint was signed over.
    pub(crate) to: Address,
    /// The authorization nonce — `(to, nonce)` is the on-chain uniqueness
    /// key.
    pub(crate) nonce: B256,
    /// The vault share token the mint was signed over.
    pub(crate) token: Address,
    /// Mint amount in 18-decimal share-wei.
    pub(crate) amount: U256,
    /// Override for the scan's backward window in blocks; `None` uses the
    /// default recovery-timeline window. Reconciliation of an inconclusive
    /// replay re-queries with a widened window (SPEC "Recipient
    /// Authorization" -> "Nonce"). Ignored by authorization validation,
    /// which reads on-chain state and never scans logs.
    pub(crate) lookback_blocks: Option<u64>,
}

/// Verdict of a `Minted`-log scan for one mint's landing. The on-chain
/// uniqueness key is only `(to, nonce)`, so the scan distinguishes THREE
/// outcomes — a bare hit is not proof the mint landed, and a bare miss is
/// not proof of absence (SPEC "Recipient Authorization" -> "Nonce": two
/// outcomes, never conflated).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MintedLogScan {
    /// A log at `(to, nonce)` whose `token` and `amount` equal the query's —
    /// THIS mint's landing.
    FullMatch(OrchestratorMintedLog),
    /// A log at `(to, nonce)` exists but its `token` or `amount` disagrees:
    /// affirmative proof a DIFFERENT mint consumed the pair, so this mint
    /// can never land.
    Mismatch,
    /// No log at `(to, nonce)` anywhere in the scanned window. When
    /// `nonceUsed` reports the nonce consumed, this is an inconclusive
    /// chain view (window too narrow, RPC or indexer lag) — an unknown
    /// outcome, never proof of anything.
    NotFound,
}

/// A landed orchestrator mint discovered via a `Minted`-log lookup that
/// full-matched `(to, nonce, token, amount)`. Unlike
/// [`OrchestratorMintResult`] this carries no `gas_used` — a bare log does
/// not expose it, and the recovery event it feeds
/// (`OrchestratorMintRecovered`) does not record it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrchestratorMintedLog {
    pub(crate) tx_hash: B256,
    pub(crate) nonce: B256,
    pub(crate) shares_minted: U256,
    pub(crate) block_number: u64,
}

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
    /// Burn amount in 18-decimal share-wei: the redemption's
    /// `alpaca_quantity` scaled via `Quantity::to_u256_with_18_decimals()`
    /// — never the raw Alpaca-precision decimal. Dust stays in the bot
    /// wallet and is excluded from this amount.
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

/// Consumed receipt-pointer range from the `Burned` event, half-open:
/// receipts strictly inside
/// `[first_receipt_id, next_burn_receipt_id_after)` were drained by the
/// burn. Named fields (not a tuple) because this is embedded in persisted
/// redemption events — the payload must self-describe which bound is which,
/// permanently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BurnRange {
    /// `Burned.firstReceiptId` — the pre-burn pointer value (may itself
    /// have been partially consumed by an earlier burn).
    pub(crate) first_receipt_id: U256,
    /// `Burned.nextBurnReceiptIdAfter` — the pointer's new value, the
    /// receipt the next burn resumes from (exclusive end).
    pub(crate) next_burn_receipt_id_after: U256,
}

/// Result of a confirmed `ST0xOrchestrator.burn()`, parsed from the
/// orchestrator's `Burned` event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct OrchestratorBurnResult {
    pub(crate) tx_hash: B256,
    /// `Burned.amount` — total shares pulled and burned.
    pub(crate) shares_burned: U256,
    pub(crate) burn_range: BurnRange,
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

/// Typed revert reason decoded from a mined-but-reverted orchestrator mint or
/// burn.
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
    /// `NonceReplayed(to, nonce)` — the `(to, nonce)` pair was already
    /// consumed by a successful mint. Recovery must full-match the on-chain
    /// `Minted` log against this mint's own `(to, nonce, token, amount)`
    /// before treating the earlier landing as this mint's.
    NonceReplayed {
        to: Address,
        nonce: B256,
    },
    /// `BadRecipientSignature()` — the recipient signature did not recover to
    /// `to`.
    BadRecipientSignature,
    /// `RecipientCallbackRejected(recipient)` — an `IMintRecipient` contract
    /// refused the mint via its `authorizeMint` callback (bridge path).
    RecipientCallbackRejected {
        recipient: Address,
    },
    /// `VaultAmountMismatch(expected, actual)` — the vault did not mint 1:1
    /// against the requested amount.
    VaultAmountMismatch {
        expected: U256,
        actual: U256,
    },
    /// Revert data was unavailable or did not decode to one of the
    /// orchestrator's typed errors.
    Unknown,
}

/// Which on-chain proof shape [`super::verify_burn_tx`] must accept when
/// verifying a burn transaction.
///
/// The two [`crate::VaultMode`]s emit structurally different burn proofs, and
/// a proof of one shape must never satisfy the other: a vault-direct
/// redemption's burn is never confirmed by an orchestrator-shaped
/// transaction, or vice versa. Callers always derive this from the
/// redemption's own persisted `burn_mode` (captured on `RedemptionDetected`),
/// never re-resolved from the asset's current `VaultMode` — that per-redemption
/// mode stays authoritative even while both modes are live side by side
/// during the incremental per-asset cutover (see `From<VaultMode>` below).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BurnProofKind {
    /// A single `Transfer(owner -> 0x0)` of vault shares, emitted directly by
    /// `ReceiptVault.withdraw()`.
    VaultDirect,
    /// Two legs in the same transaction: `Transfer(owner -> address)` (the
    /// orchestrator's `transferFrom` pull) followed by
    /// `Transfer(address -> 0x0)` (the orchestrator's burn). The pull total
    /// must equal the burn total, or the proof is rejected.
    Orchestrator {
        /// The `ST0xOrchestrator` contract address that performed the pull
        /// and burn.
        address: Address,
    },
}

impl From<VaultMode> for BurnProofKind {
    fn from(value: VaultMode) -> Self {
        match value {
            VaultMode::VaultDirect => Self::VaultDirect,
            VaultMode::Orchestrator { address } => {
                Self::Orchestrator { address }
            }
        }
    }
}
