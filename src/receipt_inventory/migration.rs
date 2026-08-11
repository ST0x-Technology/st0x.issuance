//! Moving receipt custody between signing wallets.
//!
//! The issuer burns against receipts held by its own signing address
//! (`balanceOf(bot_wallet, receipt_id)` in [`super::backfill`] and
//! [`super::reconcile`]), so rotating the signing backend strands every receipt
//! at the old address: the new wallet holds shares it cannot redeem because the
//! matching receipt sits elsewhere. Custody has to follow the rotation.
//!
//! The vendor-neutral engine's operator driver is `issuer move-receipts`
//! (`src/tokenized_asset/cli.rs`), which supplies the Turnkey signing
//! provider and the kind-corroborated destination, and performs the
//! outside-the-engine checks (deploy hold, gas readiness, confirmation).
//!
//! **The issuer service must be stopped before this runs.** Startup
//! reconciliation reads `balanceOf(bot_wallet)` for every tracked receipt
//! ([`super::reconcile`], reached from `run_startup_reconciliation`). Once
//! custody has moved but the service is still configured with the outgoing
//! signer, every one of those reads returns zero. The retained custody guard
//! refuses that pass without writing a depletion, but the deployment must
//! still be quiescent so mint and redemption workers cannot race the move.
//! Freezing the underlying does not provide that quiescence: a freeze rejects
//! new mints, it does not stop the service or serialize against it. The order
//! is stop the service, migrate, swap the signer configuration, start again.

use alloy::eips::BlockId;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::{PendingTransactionError, Provider};
use alloy::sol_types::SolCall;
use async_trait::async_trait;
use event_sorcery::StoreBuilder;
use itertools::izip;
use sqlx::{Pool, Sqlite};
use std::fmt;
use std::time::Duration;
use tracing::{debug, info};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares,
    SharesOverflow, load_inventory, send_receipt_inventory_command,
};
use crate::bindings::{
    IERC165, IERC1155Receiver, OffchainAssetReceiptVault, Receipt,
};
use crate::mint::{Mint, find_stuck as find_stuck_mints};
use crate::prepare_event_sourced_startup;
use crate::redemption::view::{
    find_stuck as find_stuck_redemptions, rebuild_redemption_view,
};
use crate::redemption::{
    IssuerRedemptionRequestId, Redemption, RedemptionEvent,
};
use crate::tokenized_asset::view::find_vault;
use crate::tokenized_asset::{Network, TokenizedAsset, UnderlyingSymbol};

/// Largest receipt batch proven to fit the retired production transfer path.
/// A 240-receipt batch exhausted gas while batches through 14 succeeded. Until
/// a future driver introduces resumable chunking, refusing above the proven
/// bound is safer than submitting an irreversible transaction with unverified
/// gas behaviour.
const MAX_RECEIPTS_PER_TRANSFER: usize = 14;

/// EIP-165 defines an interface's id as the XOR of all its function
/// selectors. Deriving it from the bound receiver-hook signatures (the very
/// functions an ERC-1155 transfer calls) means it can never drift from what
/// is actually probed — `0x4e2312e0` for `IERC1155Receiver`.
const ERC1155_RECEIVER_INTERFACE_ID: [u8; 4] = xor_selectors(
    IERC1155Receiver::onERC1155ReceivedCall::SELECTOR,
    IERC1155Receiver::onERC1155BatchReceivedCall::SELECTOR,
);

/// ERC-165's own interface id. `supportsInterface(bytes4)` is the
/// interface's only function, so the id IS its selector (`0x01ffc9a7`).
const ERC165_INTERFACE_ID: [u8; 4] = IERC165::supportsInterfaceCall::SELECTOR;

/// EIP-165 requires a compliant contract to answer `false` for
/// `0xffffffff`; answering `true` unmasks a fallback that affirms
/// everything, whose answers prove nothing.
const ERC165_INVALID_INTERFACE_ID: [u8; 4] = [0xff; 4];

const fn xor_selectors(first: [u8; 4], second: [u8; 4]) -> [u8; 4] {
    [
        first[0] ^ second[0],
        first[1] ^ second[1],
        first[2] ^ second[2],
        first[3] ^ second[3],
    ]
}

/// Refuses unless the deployment is quiescent: no burn reserved against this
/// vault's receipts, and no mint or redemption anywhere between initiation and
/// its terminal state.
///
/// Deliberately not a freeze check. The `Underlying` freeze means "corporate
/// action in progress" — a different fact with its own lifecycle — and a custody
/// migration must neither require declaring one nor end one that is real. What
/// the migration actually needs is that no work is in flight:
///
/// - A **reserved burn** is about to consume the very receipts being moved.
/// - A **non-terminal redemption** that holds no reservation yet (detected,
///   Alpaca called) resumes on restart and plans a burn against the *new*
///   wallet while the participant's money already moved — irreversibly, since
///   Alpaca is called before the burn.
/// - A **non-terminal mint** resumes by rebroadcasting a transaction signed by
///   the *old* wallet, depositing a fresh receipt at an address the migrated
///   deployment no longer watches.
///
/// The in-flight gates are scoped to the migrating asset: a stuck mint or
/// redemption only ever resumes against its own vault, so live work on one
/// asset proves nothing about another's receipts — and a permanently stuck
/// aggregate (a legacy shape awaiting its own recovery feature) must not
/// hold every other vault's migration hostage. Work that cannot be
/// attributed to an asset counts against every vault instead of none.
const fn require_quiescent(
    vault: Address,
    reserved: &[ReceiptId],
    redemptions_in_flight: usize,
    mints_in_flight: usize,
) -> Result<(), MigrationRefusal> {
    if !reserved.is_empty() {
        return Err(MigrationRefusal::BurnReserved {
            vault,
            receipts: reserved.len(),
        });
    }

    if redemptions_in_flight > 0 {
        return Err(MigrationRefusal::RedemptionsInFlight {
            count: redemptions_in_flight,
        });
    }

    if mints_in_flight > 0 {
        return Err(MigrationRefusal::MintsInFlight { count: mints_in_flight });
    }

    Ok(())
}

/// Custody of a vault's receipts, as the migration needs to see it.
///
/// The methods are one capability rather than several traits because a custody
/// move is only safe when the balances are known, the vault permits the
/// transfer, and the move itself lands. [`ReceiptCustody::transfer_custody`]
/// takes a [`TransferPermit`], which only
/// [`ReceiptCustody::transfer_permission`] can produce, so the ordering is
/// enforced by the type system rather than by a caller remembering it.
#[async_trait]
trait ReceiptCustody {
    /// On-chain balances held by `holder` for each identifier, in the order
    /// given. This is the authoritative answer that our tracked inventory is
    /// checked against.
    async fn held_balances(
        &self,
        vault: Address,
        holder: Address,
        receipt_ids: &[ReceiptId],
    ) -> Result<Vec<Shares>, ReceiptCustodyError>;

    /// Whether the vault currently permits a receipt transfer between this
    /// pair of addresses.
    ///
    /// Both refusal cases revert the transfer on-chain, so they are read
    /// immediately before submission rather than trusted from an earlier check:
    /// `OffchainAssetReceiptVault.authorizeReceiptTransfer3` runs
    /// `ownerFreezeCheckTransaction(from, to)` and then delegates to the
    /// authorizer, whose `TRANSFER_RECEIPT` branch reverts
    /// `CertificationExpired(from, to)` when certification has lapsed.
    async fn transfer_permission(
        &self,
        vault: Address,
        from: Address,
        to: Address,
    ) -> Result<TransferPermission, ReceiptCustodyError>;

    /// Move every holding to the permitted recipient in a single batch,
    /// returning the transaction hash.
    ///
    /// One batch per vault rather than one transfer per receipt: a single
    /// authorisation event, a single outcome, and no partially migrated vault
    /// to reason about on failure.
    ///
    /// The sender and recipient come from `permit`, not from separate
    /// arguments, so the addresses that were checked are necessarily the
    /// addresses that are used.
    async fn transfer_custody(
        &self,
        permit: &TransferPermit,
        holdings: &MigratableHoldings,
    ) -> Result<B256, ReceiptCustodyError>;
}

/// Whether a vault will accept a receipt transfer right now.
///
/// Modelled as the reason rather than a bool so the operator is told which gate
/// closed, and so a new gate cannot be silently folded into a false.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TransferPermission {
    Permitted(TransferPermit),
    /// The vault's certification has lapsed. Renewing it is held by whoever
    /// holds `CERTIFY` on the live vault, not by this service.
    CertificationExpired,
    /// An owner freeze covers this pair and neither address is exempt via the
    /// vault's always-allowed lists.
    OwnerFrozen {
        until: U256,
    },
}

/// Evidence that a vault accepted this exact sender and recipient at the moment
/// it was asked.
///
/// The fields are private and the only constructor is module-private, so a
/// permit cannot be conjured by a caller that skipped the check.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TransferPermit {
    vault: Address,
    from: Address,
    to: Address,
}

impl TransferPermit {
    const fn granted(vault: Address, from: Address, to: Address) -> Self {
        Self { vault, from, to }
    }

    const fn from(&self) -> Address {
        self.from
    }

    const fn to(&self) -> Address {
        self.to
    }

    const fn vault(&self) -> Address {
        self.vault
    }
}

/// Receipts confirmed to sit with the outgoing wallet, agreed between our
/// tracked inventory and the chain.
///
/// Existence implies agreement: the only constructor is
/// [`reconcile_holdings`], which refuses on any divergence. A caller therefore
/// cannot transfer a set of receipts we are not certain we hold.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MigratableHoldings {
    chain_id: u64,
    vault: Address,
    holder: Address,
    holdings: Vec<ReceiptHolding>,
}

/// A single receipt identifier and the balance held against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiptHolding {
    receipt_id: ReceiptId,
    balance: Shares,
}

/// What the outgoing wallet turned out to be holding.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SourceCustody {
    /// The outgoing wallet still holds the tracked receipts, so there is a
    /// migration to perform.
    Holds(MigratableHoldings),
    /// Every tracked identifier has already reached the incoming wallet (it
    /// holds at least each tracked balance, since a fresh run cannot know
    /// what the recipient held before the migration): a completed migration
    /// seen a second time.
    AlreadyMigrated { receipts: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecordedCustodyRoute {
    AtHolder,
    AtRecipient,
}

impl MigratableHoldings {
    const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    const fn vault(&self) -> Address {
        self.vault
    }

    const fn holder(&self) -> Address {
        self.holder
    }

    fn holdings(&self) -> &[ReceiptHolding] {
        &self.holdings
    }

    fn receipt_ids(&self) -> Vec<ReceiptId> {
        self.holdings.iter().map(|held| held.receipt_id).collect()
    }

    /// Identifiers and amounts as the parallel arrays
    /// `safeBatchTransferFrom(from, to, ids, amounts, data)` expects. Built
    /// together so the two can never fall out of step.
    fn batch_arguments(&self) -> (Vec<U256>, Vec<U256>) {
        self.holdings
            .iter()
            .map(|holding| {
                (holding.receipt_id.inner(), holding.balance.inner())
            })
            .unzip()
    }

    /// Total balance across every holding, used to assert the recipient gained
    /// exactly what the source gave up.
    fn total(&self) -> Result<Shares, SharesOverflow> {
        total_of(self.holdings.iter().map(|held| held.balance))
    }
}

/// Why a migration must not proceed.
///
/// Every variant is a stop, not a warning: this moves the backing that redeemed
/// tokens depend on, so an unclear picture is a reason to halt rather than to
/// proceed on a best guess.
#[derive(Debug, thiserror::Error)]
enum MigrationRefusal {
    #[error(
        "{count} redemption(s) are between detection and a terminal state; \
         they resume against the wrong wallet after a custody move. Drain them \
         to terminal (see /admin/stuck) before migrating"
    )]
    RedemptionsInFlight { count: usize },

    #[error(
        "{count} mint(s) are between initiation and a terminal state; recovery \
         rebroadcasts their old-wallet-signed transactions after a custody \
         move. Drain them to terminal (see /admin/stuck) before migrating"
    )]
    MintsInFlight { count: usize },

    #[error(
        "custody identity expects chain {expected}, but the provider reports \
         chain {actual}"
    )]
    ProviderChainMismatch { expected: u64, actual: u64 },

    #[error(
        "network {network} identifies chain {expected}, not requested chain \
         {requested}"
    )]
    NetworkChainMismatch { network: Network, expected: u64, requested: u64 },

    #[error(
        "no {network} listing exists for {underlying}; custody cannot be \
         scoped to a verified vault"
    )]
    ListingNotFound { underlying: UnderlyingSymbol, network: Network },

    #[error(
        "{underlying} on {network} is listed at vault {listed}, not the \
         requested vault {requested}"
    )]
    VaultListingMismatch {
        underlying: UnderlyingSymbol,
        network: Network,
        requested: Address,
        listed: Address,
    },

    #[error(
        "recipient was corroborated on chain {corroborated}, but custody is \
         moving on chain {migration}"
    )]
    RecipientChainMismatch { corroborated: u64, migration: u64 },

    #[error(
        "vault {vault} custody has never been confirmed; confirm the outgoing \
         holder before migrating"
    )]
    CustodyUnobserved { vault: Address },

    #[error(
        "vault {vault} records custody at {recorded}, which is neither the \
         requested holder {holder} nor destination {recipient}"
    )]
    CustodyRouteMismatch {
        vault: Address,
        recorded: Address,
        holder: Address,
        recipient: Address,
    },

    #[error(
        "vault {vault} records migration origin {recorded_origin:?}, not the \
         requested source {requested}"
    )]
    CustodyOriginMismatch {
        vault: Address,
        recorded_origin: Option<Address>,
        requested: Address,
    },

    #[error(
        "vault {vault} records custody at destination {recipient}, but the \
         requested source {holder} still holds tracked receipts"
    )]
    RecordedDestinationStillAtSource {
        vault: Address,
        holder: Address,
        recipient: Address,
    },

    #[error(transparent)]
    HolderMismatch(Box<HolderMismatch>),

    #[error(
        "vault {vault} has {receipts} receipt(s) reserved for an in-flight \
         redemption burn; that burn must settle or be released before custody \
         moves"
    )]
    BurnReserved { vault: Address, receipts: usize },

    #[error(
        "vault {vault} has no tracked receipts on chain {chain_id}; refusing \
         rather than reporting an unverified \"nothing to migrate\", since an \
         un-backfilled inventory is indistinguishable from an empty one"
    )]
    InventoryEmpty { vault: Address, chain_id: u64 },

    #[error(
        "vault {vault} certification is expired; receipt transfers revert \
         until whoever holds CERTIFY renews it"
    )]
    CertificationExpired { vault: Address },

    #[error("vault {vault} owner freeze blocks {from} -> {to} until {until}")]
    OwnerFrozen { vault: Address, from: Address, to: Address, until: U256 },

    #[error(
        "vault {vault} has {receipts} tracked receipts, exceeding the proven \
         single-transfer maximum of {maximum}; a resumable chunking driver is \
         required before custody can move"
    )]
    ReceiptBatchTooLarge { vault: Address, receipts: usize, maximum: usize },

    #[error(
        "receipt {receipt_id} on vault {vault} diverges: inventory tracks \
         {tracked}, chain holds {onchain}"
    )]
    InventoryDivergence {
        vault: Address,
        receipt_id: ReceiptId,
        tracked: Shares,
        onchain: Shares,
    },

    #[error(
        "receipt {receipt_id} of vault {vault} left the outgoing wallet but \
         the incoming wallet holds {at_recipient}, not the tracked \
         {tracked}; custody is in a state this migration cannot explain"
    )]
    RecipientBalanceMismatch {
        vault: Address,
        receipt_id: ReceiptId,
        tracked: Shares,
        at_recipient: Shares,
    },

    #[error(
        "vault {vault} returned {returned} balances for {requested} receipts"
    )]
    BalanceCountMismatch { vault: Address, requested: usize, returned: usize },

    #[error(
        "recipient {recipient} is the zero address; custody would be burned"
    )]
    RecipientIsZeroAddress { recipient: Address },

    #[error(
        "recipient {recipient} is already the current custody holder; a \
         self-transfer cannot migrate custody"
    )]
    RecipientIsHolder { recipient: Address },

    #[error(
        "chain {chain_id} has never seen recipient {recipient}: it has sent no \
         transaction and holds no native balance. That is what a mistyped \
         address looks like, and receipts moved to one cannot be recovered by \
         anyone. Check the address, and fund the incoming wallet for gas \
         before migrating."
    )]
    RecipientUnknownToChain { recipient: Address, chain_id: u64 },

    #[error(
        "recipient {recipient} is a contract that answers \
         supportsInterface(IERC1155Receiver) = false: it states it cannot \
         receive ERC-1155 transfers, so the receipt transfer would revert"
    )]
    RecipientContractRefusesReceipts { recipient: Address },

    #[error(
        "recipient {recipient} is a contract whose ERC-165 answers are \
         inconsistent (a compliant responder answers true for ERC-165 itself \
         and false for 0xffffffff), so its receiver-support claim proves \
         nothing; receipts only move to a contract whose support is proven"
    )]
    RecipientErc165Inconsistent { recipient: Address },

    #[error(
        "recipient {recipient} is a contract that does not answer ERC-165 \
         supportsInterface, so ERC-1155 receiver support cannot be proven \
         before an irreversible transfer"
    )]
    RecipientReceiverSupportUnproven {
        recipient: Address,
        #[source]
        source: Box<alloy::contract::Error>,
    },

    #[error(transparent)]
    SharesOverflow(#[from] SharesOverflow),

    #[error(transparent)]
    Custody(Box<ReceiptCustodyError>),
}

// The alloy error types these wrap are large, so box on conversion to keep the
// enum (and every `Result` carrying it) small, while `?` still works at the
// call site without a hand-rolled `.map_err(Box::new)`.
impl From<ReceiptCustodyError> for MigrationRefusal {
    fn from(error: ReceiptCustodyError) -> Self {
        Self::Custody(Box::new(error))
    }
}

/// Why the custody move itself could not complete.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptCustodyError {
    #[error(transparent)]
    Contract(Box<alloy::contract::Error>),

    #[error(transparent)]
    PendingTransaction(Box<PendingTransactionError>),

    #[error(transparent)]
    Rpc(Box<alloy::transports::TransportError>),

    #[error("chain reported no latest block")]
    NoLatestBlock,

    #[error(
        "custody was resolved for vault {expected} but asked about \
         {requested}"
    )]
    WrongVault { expected: Address, requested: Address },

    #[error(
        "permit authorises {permitted} as the sender but the holdings belong \
         to {holdings}"
    )]
    PermitHolderMismatch { permitted: Address, holdings: Address },

    #[error(
        "wallet {holder} has pending transactions (latest nonce {latest_nonce}, \
         pending nonce {pending_nonce}); refusing a custody transfer until \
         they settle or drop"
    )]
    PendingWalletTransactions {
        holder: Address,
        latest_nonce: LatestNonce,
        pending_nonce: PendingNonce,
    },

    #[error(
        "wallet {holder} returned pending nonce {pending_nonce} below latest \
         nonce {latest_nonce}"
    )]
    InvalidNonceOrder {
        holder: Address,
        latest_nonce: LatestNonce,
        pending_nonce: PendingNonce,
    },

    #[error("custody transfer {tx_hash} reverted on vault {vault}")]
    Reverted { vault: Address, tx_hash: B256 },

    #[error(transparent)]
    RecipientBalanceDecreased(Box<RecipientBalanceDecrease>),

    #[error(transparent)]
    PostConditionFailed(Box<PostConditionFailure>),

    #[error(
        "vault {vault} produced an empty transfer batch; nothing was \
         submitted"
    )]
    NothingToTransfer { vault: Address },
}

/// A wallet's nonce per `eth_getTransactionCount(holder, "latest")` — the
/// highest mined nonce. Kept distinct from [`PendingNonce`] so the two nonce
/// views can't be swapped at a call site without a type error.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LatestNonce(u64);

impl fmt::Display for LatestNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

/// A wallet's nonce per `eth_getTransactionCount(holder, "pending")` — mined
/// plus mempool-visible. Kept distinct from [`LatestNonce`] so the two nonce
/// views can't be swapped at a call site without a type error.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PendingNonce(u64);

impl fmt::Display for PendingNonce {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, formatter)
    }
}

/// A recipient balance that fell over a transfer that should only have raised
/// it — a corrupt observation, not a zero gain.
///
/// Boxed inside [`ReceiptCustodyError`] for the same size reason as
/// [`PostConditionFailure`].
#[derive(Debug, thiserror::Error)]
#[error(
    "transfer {tx_hash} on vault {vault}: receipt {receipt_id} balance at the \
     recipient fell from {before} to {after} over a transfer that should only \
     have increased it"
)]
pub(crate) struct RecipientBalanceDecrease {
    pub(crate) vault: Address,
    pub(crate) tx_hash: B256,
    pub(crate) receipt_id: ReceiptId,
    pub(crate) before: Shares,
    pub(crate) after: Shares,
}

impl From<RecipientBalanceDecrease> for ReceiptCustodyError {
    fn from(decrease: RecipientBalanceDecrease) -> Self {
        Self::RecipientBalanceDecreased(Box::new(decrease))
    }
}

/// A transfer that mined but left custody in a state the migration cannot
/// call success.
///
/// Boxed inside [`ReceiptCustodyError`] because five inline fields push every
/// `Result` in this module over clippy's large-error threshold.
#[derive(Debug, thiserror::Error)]
#[error(
    "transfer {tx_hash} on vault {vault} mined but post-conditions failed: \
     source retains {source_retained}, recipient gained {recipient_gained}, \
     expected {expected}"
)]
pub(crate) struct PostConditionFailure {
    pub(crate) vault: Address,
    /// The transfer that may have partially succeeded. An operator reconciling
    /// by hand needs it, so it belongs in the error rather than only in the log
    /// line that never gets written on this path.
    pub(crate) tx_hash: B256,
    pub(crate) source_retained: Shares,
    pub(crate) recipient_gained: Shares,
    pub(crate) expected: Shares,
}

impl From<PostConditionFailure> for ReceiptCustodyError {
    fn from(failure: PostConditionFailure) -> Self {
        Self::PostConditionFailed(Box::new(failure))
    }
}

/// A wallet whose on-chain balances do not match the tracked inventory, so it
/// cannot be confirmed as the custody holder.
///
/// Boxed inside [`MigrationRefusal`] because five inline fields push every
/// `Result` in this module over clippy's large-error threshold.
#[derive(Debug, thiserror::Error)]
#[error(
    "wallet {holder} does not hold vault {vault}'s tracked receipts (receipt \
     {receipt_id}: tracked {tracked}, held {held}); custody cannot be \
     confirmed against a wallet the chain says holds something else"
)]
pub(crate) struct HolderMismatch {
    pub(crate) vault: Address,
    pub(crate) holder: Address,
    pub(crate) receipt_id: ReceiptId,
    pub(crate) tracked: Shares,
    pub(crate) held: Shares,
}

impl From<HolderMismatch> for MigrationRefusal {
    fn from(mismatch: HolderMismatch) -> Self {
        Self::HolderMismatch(Box::new(mismatch))
    }
}

// Same boxing rationale as `MigrationRefusal::Custody`: these alloy errors are
// large enough that carrying them inline bloats every `Result` in the module.
impl From<alloy::contract::Error> for ReceiptCustodyError {
    fn from(error: alloy::contract::Error) -> Self {
        Self::Contract(Box::new(error))
    }
}

impl From<PendingTransactionError> for ReceiptCustodyError {
    fn from(error: PendingTransactionError) -> Self {
        Self::PendingTransaction(Box::new(error))
    }
}

impl From<alloy::transports::TransportError> for ReceiptCustodyError {
    fn from(error: alloy::transports::TransportError) -> Self {
        Self::Rpc(Box::new(error))
    }
}

/// What a completed migration did, so a re-run reads differently from a first
/// run rather than both printing the same success.
#[derive(Debug, PartialEq, Eq)]
pub enum MigrationOutcome {
    Migrated {
        transaction: B256,
        receipts: usize,
    },
    /// A completed migration observed again: the outgoing wallet holds nothing
    /// and the incoming wallet holds at least the tracked balances.
    AlreadyMigrated {
        receipts: usize,
    },
}

/// Which kind of address a destination was proven to be.
///
/// Deciding which corroboration it had to clear. Recorded on the witness so
/// the driver's confirmation prompt and the audit trail state what was proven
/// rather than assuming one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientKind {
    /// No deployed code; corroborated by on-chain history (transaction
    /// count or native balance).
    ExternallyOwned,
    /// Deployed code that proves ERC-1155 receiver support through a
    /// consistent ERC-165 responder.
    Erc1155Receiver,
}

impl fmt::Display for RecipientKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExternallyOwned => {
                write!(formatter, "externally owned account")
            }
            Self::Erc1155Receiver => {
                write!(formatter, "ERC-1155-receiving contract")
            }
        }
    }
}

/// A destination the chain itself has corroborated, paired with the caller's
/// stated current holder.
///
/// Existence proves only the destination and chain were corroborated by
/// [`CorroboratedRecipient::verify`] — with a corroboration as strong as the
/// kind of address the destination is (see [`RecipientKind`]). The stated
/// holder is checked against the inventory's recorded custody before any
/// transfer. An ERC-1155 transfer is final and has no counterparty to ask
/// for it back, so a mistyped destination is not a recoverable mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorroboratedRecipient {
    chain_id: u64,
    holder: Address,
    recipient: Address,
    kind: RecipientKind,
}

impl CorroboratedRecipient {
    /// Confirms `recipient` differs from `holder` and corroborates it by
    /// what the chain says it is.
    ///
    /// An address with no deployed code is an externally owned account and
    /// must have on-chain history: an address that has never sent a
    /// transaction and holds no native balance has no on-chain existence at
    /// all — precisely what a fat-fingered address looks like, since the
    /// odds of a typo landing on a used address are negligible. Both
    /// legitimate EOA destinations clear it: an incoming signing wallet has
    /// to be funded for gas before it can run the service, while a prior
    /// signing wallet has already been active on-chain.
    ///
    /// An address with deployed code clears the EOA evidence for the wrong
    /// reason — deployment proves nothing about receiving ERC-1155 — so a
    /// contract must instead prove receiver support up front, through a
    /// consistent ERC-165 responder affirming `IERC1155Receiver`. Receiver
    /// support is proven before submitting, never discovered by a revert.
    ///
    /// The error type is erased to `anyhow` for the same reason as
    /// [`migrate_vault_receipts`]: [`MigrationRefusal`] stays crate-internal.
    ///
    /// # Errors
    ///
    /// Returns an error for the zero address, the current holder, an EOA the
    /// chain has no record of, and a contract that does not prove ERC-1155
    /// receiver support.
    pub async fn verify<P: Provider>(
        provider: &P,
        holder: Address,
        recipient: Address,
    ) -> anyhow::Result<Self> {
        if recipient.is_zero() {
            return Err(
                MigrationRefusal::RecipientIsZeroAddress { recipient }.into()
            );
        }

        if recipient == holder {
            return Err(
                MigrationRefusal::RecipientIsHolder { recipient }.into()
            );
        }

        let chain_id =
            provider.get_chain_id().await.map_err(ReceiptCustodyError::from)?;

        let code = provider
            .get_code_at(recipient)
            .await
            .map_err(ReceiptCustodyError::from)?;

        let kind = if code.is_empty() {
            corroborate_externally_owned(provider, recipient, chain_id).await?
        } else {
            corroborate_erc1155_receiver(provider, recipient).await?
        };

        Ok(Self { chain_id, holder, recipient, kind })
    }

    const fn address(self) -> Address {
        self.recipient
    }

    const fn holder(self) -> Address {
        self.holder
    }

    const fn chain_id(self) -> u64 {
        self.chain_id
    }

    /// The kind of address the destination was proven to be.
    #[must_use]
    pub const fn kind(self) -> RecipientKind {
        self.kind
    }
}

/// The EOA corroboration: refused unless the chain has independent evidence
/// the address exists — transaction history or native balance.
async fn corroborate_externally_owned<P: Provider>(
    provider: &P,
    recipient: Address,
    chain_id: u64,
) -> anyhow::Result<RecipientKind> {
    let nonce = provider
        .get_transaction_count(recipient)
        .await
        .map_err(ReceiptCustodyError::from)?;

    let balance = provider
        .get_balance(recipient)
        .await
        .map_err(ReceiptCustodyError::from)?;

    if nonce == 0 && balance.is_zero() {
        return Err(MigrationRefusal::RecipientUnknownToChain {
            recipient,
            chain_id,
        }
        .into());
    }

    Ok(RecipientKind::ExternallyOwned)
}

/// The contract corroboration: a consistent ERC-165 responder affirming
/// `IERC1155Receiver`.
///
/// Consistency first (EIP-165's own detection procedure): the responder must
/// affirm ERC-165 itself and deny `0xffffffff` — a fallback that affirms
/// everything proves nothing — and only then is its answer for the receiver
/// interface trusted.
async fn corroborate_erc1155_receiver<P: Provider>(
    provider: &P,
    recipient: Address,
) -> anyhow::Result<RecipientKind> {
    let responder = IERC165::new(recipient, provider);

    let affirms_erc165 = responder
        .supportsInterface(ERC165_INTERFACE_ID.into())
        .call()
        .await
        .map_err(|source| {
            MigrationRefusal::RecipientReceiverSupportUnproven {
                recipient,
                source: Box::new(source),
            }
        })?;

    let affirms_invalid = responder
        .supportsInterface(ERC165_INVALID_INTERFACE_ID.into())
        .call()
        .await
        .map_err(|source| {
            MigrationRefusal::RecipientReceiverSupportUnproven {
                recipient,
                source: Box::new(source),
            }
        })?;

    if !affirms_erc165 || affirms_invalid {
        return Err(MigrationRefusal::RecipientErc165Inconsistent {
            recipient,
        }
        .into());
    }

    let supports_receiver = responder
        .supportsInterface(ERC1155_RECEIVER_INTERFACE_ID.into())
        .call()
        .await
        .map_err(|source| {
            MigrationRefusal::RecipientReceiverSupportUnproven {
                recipient,
                source: Box::new(source),
            }
        })?;

    if !supports_receiver {
        return Err(MigrationRefusal::RecipientContractRefusesReceipts {
            recipient,
        }
        .into());
    }

    Ok(RecipientKind::Erc1155Receiver)
}

impl std::fmt::Display for CorroboratedRecipient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.recipient)
    }
}

/// Migrates one vault's receipt custody to `recipient`, end to end.
///
/// Public so a future operator driver and the receipt-custody end-to-end test
/// can drive the same reusable engine.
///
/// `provider` must be signing as the holder paired with `recipient`, the wallet
/// whose custody moves — ERC-1155 only lets the holder or an approved operator
/// move a balance. The engine corroborates that stated holder against aggregate
/// custody before submission.
///
/// Re-running after a successful migration is safe: the outgoing wallet is
/// found empty with the incoming wallet holding the tracked balances, which
/// reports [`MigrationOutcome::AlreadyMigrated`] rather than submitting a
/// second transfer.
///
/// The error type is erased to `anyhow` at this boundary, matching the other
/// public orchestration boundary; the typed [`MigrationRefusal`] hierarchy is
/// preserved for every caller inside the crate.
///
/// # Errors
///
/// Returns an error if the store cannot be opened, any burn is reserved or any
/// mint/redemption is in flight, the vault has no tracked receipts, the tracked
/// inventory disagrees with the chain, the vault refuses the transfer, the
/// transfer reverts, or the post-conditions do not hold after submission.
pub async fn migrate_vault_receipts<P: Provider + Clone + Send + Sync>(
    pool: &Pool<Sqlite>,
    provider: P,
    identity: VaultIdentity<'_>,
    recipient: CorroboratedRecipient,
) -> anyhow::Result<MigrationOutcome> {
    prepare_custody_engine_state(pool).await?;
    identity.corroborate_provider(&provider).await?;
    identity.corroborate_listing(pool).await?;
    corroborate_recipient_chain(identity.chain_id, recipient.chain_id())?;

    let custody =
        OnchainReceiptCustody::resolve(provider, identity.vault).await?;

    execute_migration(pool, &custody, identity, recipient).await
}

/// Tracked receipts with balance for this vault, for the driver's
/// confirmation prompt.
///
/// Informational only: [`migrate_vault_receipts`]
/// re-derives its own holdings under the quiescence gates before anything
/// moves, so this count gates nothing.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or the inventory fails to
/// load.
pub async fn tracked_receipt_count(
    pool: &Pool<Sqlite>,
    chain_id: u64,
    vault: Address,
) -> anyhow::Result<usize> {
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;

    Ok(inventory.receipts_with_balance().len())
}

/// Verified identity of the vault a custody operation addresses.
///
/// Existence proves the provider reports `chain_id` and the tokenized-asset
/// listing binds `underlying` to `vault` on `network`. That binding is what
/// makes the asset-scoped in-flight quiescence gate safe.
#[derive(Debug, Clone, Copy)]
pub struct VaultIdentity<'a> {
    network: Network,
    chain_id: u64,
    vault: Address,
    underlying: &'a UnderlyingSymbol,
}

impl<'a> VaultIdentity<'a> {
    /// Corroborates a complete vault identity against the provider and the
    /// tokenized-asset listing.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider reports another chain, the listing
    /// does not exist, the listing names another vault, or its projection
    /// cannot be loaded.
    pub async fn verify<P: Provider>(
        pool: &Pool<Sqlite>,
        provider: &P,
        network: Network,
        chain_id: u64,
        vault: Address,
        underlying: &'a UnderlyingSymbol,
    ) -> anyhow::Result<Self> {
        rebuild_listing_projection(pool).await?;
        corroborate_network_chain(network, chain_id)?;

        let provider_chain_id =
            provider.get_chain_id().await.map_err(ReceiptCustodyError::from)?;
        let listed_vault = find_vault(pool, underlying, &network)
            .await?
            .ok_or_else(|| MigrationRefusal::ListingNotFound {
                underlying: underlying.clone(),
                network,
            })?;

        Self::from_observations(
            network,
            chain_id,
            provider_chain_id,
            vault,
            listed_vault,
            underlying,
        )
        .map_err(Into::into)
    }

    fn from_observations(
        network: Network,
        chain_id: u64,
        provider_chain_id: u64,
        vault: Address,
        listed_vault: Address,
        underlying: &'a UnderlyingSymbol,
    ) -> Result<Self, MigrationRefusal> {
        corroborate_network_chain(network, chain_id)?;
        corroborate_provider_chain(chain_id, provider_chain_id)?;

        corroborate_listed_vault(network, vault, listed_vault, underlying)?;

        Ok(Self { network, chain_id, vault, underlying })
    }

    async fn corroborate_provider<P: Provider>(
        self,
        provider: &P,
    ) -> Result<(), MigrationRefusal> {
        let provider_chain_id =
            provider.get_chain_id().await.map_err(ReceiptCustodyError::from)?;

        corroborate_provider_chain(self.chain_id, provider_chain_id)
    }

    async fn corroborate_listing(
        self,
        pool: &Pool<Sqlite>,
    ) -> anyhow::Result<()> {
        let listed_vault = find_vault(pool, self.underlying, &self.network)
            .await?
            .ok_or_else(|| MigrationRefusal::ListingNotFound {
                underlying: self.underlying.clone(),
                network: self.network,
            })?;

        corroborate_listed_vault(
            self.network,
            self.vault,
            listed_vault,
            self.underlying,
        )?;

        Ok(())
    }
}

const fn corroborate_network_chain(
    network: Network,
    requested: u64,
) -> Result<(), MigrationRefusal> {
    let expected = network.chain_id();
    if requested != expected {
        return Err(MigrationRefusal::NetworkChainMismatch {
            network,
            expected,
            requested,
        });
    }

    Ok(())
}

fn corroborate_listed_vault(
    network: Network,
    requested: Address,
    listed: Address,
    underlying: &UnderlyingSymbol,
) -> Result<(), MigrationRefusal> {
    if requested != listed {
        return Err(MigrationRefusal::VaultListingMismatch {
            underlying: underlying.clone(),
            network,
            requested,
            listed,
        });
    }

    Ok(())
}

const fn corroborate_provider_chain(
    expected: u64,
    actual: u64,
) -> Result<(), MigrationRefusal> {
    if expected != actual {
        return Err(MigrationRefusal::ProviderChainMismatch {
            expected,
            actual,
        });
    }

    Ok(())
}

const fn corroborate_recipient_chain(
    migration: u64,
    corroborated: u64,
) -> Result<(), MigrationRefusal> {
    if migration != corroborated {
        return Err(MigrationRefusal::RecipientChainMismatch {
            corroborated,
            migration,
        });
    }

    Ok(())
}

fn corroborate_recorded_custody_route(
    vault: Address,
    recorded: Option<Address>,
    recorded_origin: Option<Address>,
    holder: Address,
    recipient: Address,
) -> Result<RecordedCustodyRoute, MigrationRefusal> {
    match recorded {
        None => Err(MigrationRefusal::CustodyUnobserved { vault }),
        Some(recorded) if recorded == holder => {
            Ok(RecordedCustodyRoute::AtHolder)
        }
        Some(recorded)
            if recorded == recipient && recorded_origin == Some(holder) =>
        {
            Ok(RecordedCustodyRoute::AtRecipient)
        }
        Some(recorded) if recorded == recipient => {
            Err(MigrationRefusal::CustodyOriginMismatch {
                vault,
                recorded_origin,
                requested: holder,
            })
        }
        Some(recorded) => Err(MigrationRefusal::CustodyRouteMismatch {
            vault,
            recorded,
            holder,
            recipient,
        }),
    }
}

const fn corroborate_recorded_route_observation(
    route: RecordedCustodyRoute,
    observed: &SourceCustody,
    vault: Address,
    holder: Address,
    recipient: Address,
) -> Result<(), MigrationRefusal> {
    match (route, observed) {
        (RecordedCustodyRoute::AtRecipient, SourceCustody::Holds(_)) => {
            Err(MigrationRefusal::RecordedDestinationStillAtSource {
                vault,
                holder,
                recipient,
            })
        }
        (
            RecordedCustodyRoute::AtHolder,
            SourceCustody::Holds(_) | SourceCustody::AlreadyMigrated { .. },
        )
        | (
            RecordedCustodyRoute::AtRecipient,
            SourceCustody::AlreadyMigrated { .. },
        ) => Ok(()),
    }
}

async fn execute_migration(
    pool: &Pool<Sqlite>,
    custody: &(impl ReceiptCustody + Sync),
    identity: VaultIdentity<'_>,
    recipient: CorroboratedRecipient,
) -> anyhow::Result<MigrationOutcome> {
    let VaultIdentity { network: _, chain_id, vault, underlying } = identity;
    let holder = recipient.holder();
    let recipient = recipient.address();

    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;

    let inventory = load_inventory(&store, chain_id, &vault).await?;
    let route = corroborate_recorded_custody_route(
        vault,
        inventory.custody().holder(),
        inventory.custody().moved_from(),
        holder,
        recipient,
    )?;
    let tracked = quiescent_tracked_holdings(
        pool, &inventory, chain_id, vault, underlying,
    )
    .await?;

    let observed = reconcile_holdings(
        custody, chain_id, vault, holder, recipient, &tracked,
    )
    .await?;
    corroborate_recorded_route_observation(
        route, &observed, vault, holder, recipient,
    )?;

    match observed {
        SourceCustody::AlreadyMigrated { receipts } => {
            info!(
                target: "receipt_inventory",
                chain_id,
                %vault,
                %holder,
                %recipient,
                receipts,
                "Receipt custody already migrated"
            );

            // An already-completed move is still recorded idempotently so the
            // custody history reflects the on-chain state observed here.
            send_receipt_inventory_command(
                &store,
                chain_id,
                &vault,
                ReceiptInventoryCommand::RecordCustodyMigration {
                    from: holder,
                    to: recipient,
                    tx_hash: None,
                },
            )
            .await?;

            Ok(MigrationOutcome::AlreadyMigrated { receipts })
        }
        SourceCustody::Holds(holdings) => {
            let outcome =
                migrate_vault_custody(custody, &holdings, recipient).await?;

            // Recorded only after the move is verified, so the inventory's
            // custody history never claims a transfer that did not land. This
            // is what a later reverse migration reads its destination from,
            // instead of being handed an address.
            if let MigrationOutcome::Migrated { transaction, .. } = &outcome {
                send_receipt_inventory_command(
                    &store,
                    chain_id,
                    &vault,
                    ReceiptInventoryCommand::RecordCustodyMigration {
                        from: holder,
                        to: recipient,
                        tx_hash: Some(*transaction),
                    },
                )
                .await?;
            }

            Ok(outcome)
        }
    }
}

/// Confirms on-chain that `holder` holds exactly every tracked balance for
/// this vault, then records it as the inventory's custody holder.
///
/// This is the bootstrap for deployments whose events predate custody
/// tracking: the displacement guard treats unobserved custody as "a zero
/// balance means spent", so every vault's holder must be on record before any
/// service starts against a rotated wallet. The address is operator-supplied,
/// but it cannot be recorded wrongly: a mistyped wallet holds none of the
/// tracked receipts and is refused with the first mismatch.
///
/// Requires quiescence like the migration itself — recording custody while
/// work is in flight would capture a moving target.
///
/// # Errors
///
/// Returns an error if the store cannot be opened, work is in flight, the
/// vault has no tracked receipts, or `holder`'s on-chain balances do not match
/// the tracked inventory exactly.
pub async fn confirm_custody_holder<P: Provider + Clone + Send + Sync>(
    pool: &Pool<Sqlite>,
    provider: P,
    identity: VaultIdentity<'_>,
    holder: Address,
) -> anyhow::Result<usize> {
    prepare_custody_engine_state(pool).await?;
    identity.corroborate_provider(&provider).await?;
    identity.corroborate_listing(pool).await?;

    confirm_custody_holder_for_identity(pool, provider, identity, holder).await
}

/// Rebuilds every projection the stopped-service custody engine reads from the
/// event store. An empty or stale read model must never be interpreted as "no
/// listing" or "no work in flight" during an irreversible custody move.
async fn prepare_custody_engine_state(
    pool: &Pool<Sqlite>,
) -> anyhow::Result<()> {
    rebuild_listing_projection(pool).await?;

    prepare_event_sourced_startup::<Mint>(pool).await?;
    let (_mint_store, mint_projection) =
        StoreBuilder::<Mint>::new(pool.clone()).build(()).await?;
    mint_projection.rebuild_all().await?;

    prepare_event_sourced_startup::<Redemption>(pool).await?;
    rebuild_redemption_view(pool).await?;

    prepare_event_sourced_startup::<ReceiptInventory>(pool).await?;
    let _inventory_store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;

    debug!(
        target: "receipt",
        "Prepared custody engine projections from event history"
    );

    Ok(())
}

async fn rebuild_listing_projection(pool: &Pool<Sqlite>) -> anyhow::Result<()> {
    prepare_event_sourced_startup::<TokenizedAsset>(pool).await?;
    let (_listing_store, listing_projection) =
        StoreBuilder::<TokenizedAsset>::new(pool.clone()).build(()).await?;
    listing_projection.rebuild_all().await?;

    Ok(())
}

async fn confirm_custody_holder_for_identity<
    P: Provider + Clone + Send + Sync,
>(
    pool: &Pool<Sqlite>,
    provider: P,
    identity: VaultIdentity<'_>,
    holder: Address,
) -> anyhow::Result<usize> {
    let VaultIdentity { network: _, chain_id, vault, underlying } = identity;
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;

    let inventory = load_inventory(&store, chain_id, &vault).await?;
    let tracked = quiescent_tracked_holdings(
        pool, &inventory, chain_id, vault, underlying,
    )
    .await?;

    let custody = OnchainReceiptCustody::resolve(provider, vault).await?;
    let receipt_ids: Vec<ReceiptId> =
        tracked.iter().map(|held| held.receipt_id).collect();
    let held = custody.held_balances(vault, holder, &receipt_ids).await?;

    if held.len() != receipt_ids.len() {
        return Err(MigrationRefusal::BalanceCountMismatch {
            vault,
            requested: receipt_ids.len(),
            returned: held.len(),
        }
        .into());
    }

    for (holding, onchain) in izip!(&tracked, &held) {
        if holding.balance != *onchain {
            return Err(MigrationRefusal::from(HolderMismatch {
                vault,
                holder,
                receipt_id: holding.receipt_id,
                tracked: holding.balance,
                held: *onchain,
            })
            .into());
        }
    }

    send_receipt_inventory_command(
        &store,
        chain_id,
        &vault,
        ReceiptInventoryCommand::ConfirmCustody { holder },
    )
    .await?;

    Ok(tracked.len())
}

/// The wallet a reverse migration returns custody to, read from the recorded
/// migration.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or no custody migration was
/// ever recorded for this vault.
pub async fn recorded_migration_origin(
    pool: &Pool<Sqlite>,
    chain_id: u64,
    vault: Address,
) -> anyhow::Result<Address> {
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;

    inventory.custody().moved_from().ok_or_else(|| {
        anyhow::anyhow!(
            "vault {vault} on chain {chain_id} has no recorded custody \
             migration to reverse"
        )
    })
}

/// The wallet currently recorded as holding the vault's receipts.
///
/// The narrow read path for verifying custody state from outside the crate
/// (e.g. after a `confirm-custody` re-confirmation): the confirmation's
/// return value counts verified balances, while this reads the holder the
/// aggregate actually persisted.
///
/// # Errors
///
/// Returns an error if the store cannot be opened or custody has never been
/// observed for this vault.
pub async fn recorded_custody_holder(
    pool: &Pool<Sqlite>,
    chain_id: u64,
    vault: Address,
) -> anyhow::Result<Address> {
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;

    inventory.custody().holder().ok_or_else(|| {
        anyhow::anyhow!(
            "vault {vault} on chain {chain_id} has no recorded custody holder"
        )
    })
}

/// Loads the tracked holdings after proving the deployment is quiescent.
///
/// Shared by every operation that reads or moves custody: none of them may run
/// over in-flight work, and all of them are meaningless on an empty inventory.
async fn quiescent_tracked_holdings(
    pool: &Pool<Sqlite>,
    inventory: &ReceiptInventory,
    chain_id: u64,
    vault: Address,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<Vec<ReceiptHolding>> {
    let redemptions_in_flight = stuck_redemptions_for(pool, underlying).await?;
    let mints_in_flight = stuck_mints_for(pool, underlying).await?;

    require_quiescent(
        vault,
        &inventory.reserved_receipts(),
        redemptions_in_flight,
        mints_in_flight,
    )?;

    // A zero balance is nothing to move, and a vault whose every balance is
    // zero has nothing to migrate — treating it as migratable is how a
    // fully-spent vault could "verify" a move on two zero readings.
    let tracked: Vec<ReceiptHolding> = inventory
        .receipts_with_balance()
        .into_iter()
        .map(|receipt| ReceiptHolding {
            receipt_id: receipt.receipt_id,
            balance: receipt.available_balance,
        })
        .filter(|holding| !holding.balance.is_zero())
        .collect();

    if tracked.is_empty() {
        return Err(MigrationRefusal::InventoryEmpty { vault, chain_id }.into());
    }

    Ok(tracked)
}

/// Counts the stuck redemptions that could resume against `underlying`'s
/// vault.
///
/// Terminal-looking view shapes (`Failed`) no longer carry the asset, so
/// those are attributed from the aggregate's `Detected` event; a redemption
/// that cannot be attributed at all counts against every vault rather than
/// none.
async fn stuck_redemptions_for(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<usize> {
    let mut count = 0;

    for (issuer_request_id, view) in find_stuck_redemptions(pool).await? {
        let counts = match view.underlying() {
            Some(asset) => asset == underlying,
            None => detected_redemption_underlying(pool, &issuer_request_id)
                .await?
                .is_none_or(|asset| asset == *underlying),
        };

        if counts {
            count += 1;
        }
    }

    Ok(count)
}

/// Counts the stuck mints that could resume against `underlying`'s vault,
/// counting any unattributable mint against every vault.
async fn stuck_mints_for(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> anyhow::Result<usize> {
    Ok(find_stuck_mints(pool)
        .await?
        .iter()
        .filter(|(_, view)| {
            view.underlying().is_none_or(|asset| asset == underlying)
        })
        .count())
}

/// Recovers a redemption's asset from its `Detected` event when the view has
/// already dropped it.
async fn detected_redemption_underlying(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRedemptionRequestId,
) -> anyhow::Result<Option<UnderlyingSymbol>> {
    let aggregate_id = issuer_request_id.to_string();
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        if let RedemptionEvent::Detected { underlying, .. } =
            serde_json::from_str(&row.payload)?
        {
            return Ok(Some(underlying));
        }
    }

    Ok(None)
}

/// Cross-checks the tracked inventory against the chain, yielding the set to
/// move only when the two agree.
///
/// Both sources are needed and neither is sufficient. Our inventory can be
/// stale, so it cannot be trusted alone; the chain is authoritative but needs a
/// candidate identifier set to ask about, which only the inventory supplies
/// cheaply. Divergence means we do not know what we hold, which is a stop
/// rather than something to reconcile in flight.
///
/// One divergence is benign and is reported as
/// [`SourceCustody::AlreadyMigrated`]: the source holding nothing while the
/// recipient holds at least the tracked balances is a completed migration seen
/// again, not a corrupt inventory. The inventory is keyed by vault and chain
/// with no holder dimension, so it describes whichever wallet the service is
/// configured to sign as — which is why the service must not run against the
/// outgoing signer after custody moves (see the module docs).
async fn reconcile_holdings(
    custody: &(impl ReceiptCustody + Sync),
    chain_id: u64,
    vault: Address,
    holder: Address,
    recipient: Address,
    tracked: &[ReceiptHolding],
) -> Result<SourceCustody, MigrationRefusal> {
    let receipt_ids: Vec<ReceiptId> =
        tracked.iter().map(|held| held.receipt_id).collect();
    let onchain = custody.held_balances(vault, holder, &receipt_ids).await?;

    if onchain.len() != receipt_ids.len() {
        return Err(MigrationRefusal::BalanceCountMismatch {
            vault,
            requested: receipt_ids.len(),
            returned: onchain.len(),
        });
    }

    // Classification remains per identifier so a historical partial custody
    // move can resume. The current engine submits one atomic batch, but retired
    // execution paths could leave some identifiers with the recipient while
    // the rest remained with the holder; an all-or-nothing agreement check
    // would refuse that recoverable state as divergence forever.
    //
    // The recipient side is read unconditionally so all three balance views
    // zip positionally, with the length verified once up front. `>=`, not
    // `==`, on the recipient side: a fresh run cannot know what the
    // recipient held before the migration, and the forward path deliberately
    // allows a recipient with a pre-existing balance.
    let at_recipient =
        custody.held_balances(vault, recipient, &receipt_ids).await?;

    if at_recipient.len() != receipt_ids.len() {
        return Err(MigrationRefusal::BalanceCountMismatch {
            vault,
            requested: receipt_ids.len(),
            returned: at_recipient.len(),
        });
    }

    let mut unmoved: Vec<ReceiptHolding> = Vec::new();
    let mut migrated: usize = 0;
    for (held, onchain, moved_to_recipient) in
        izip!(tracked, &onchain, &at_recipient)
    {
        if *onchain == held.balance {
            // A zero tracked balance is nothing to move. Keeping it would
            // put an identifier in the batch that transfers nothing and
            // cannot be verified as having moved.
            if !held.balance.is_zero() {
                unmoved.push(*held);
            }
            continue;
        }

        // Reached only on disagreement. A zero tracked balance against a
        // nonzero on-chain one, or a partial on-chain balance, is an
        // inventory we cannot trust — not something to skip or resume.
        if held.balance.is_zero() || !onchain.is_zero() {
            return Err(MigrationRefusal::InventoryDivergence {
                vault,
                receipt_id: held.receipt_id,
                tracked: held.balance,
                onchain: *onchain,
            });
        }

        if *moved_to_recipient < held.balance {
            return Err(MigrationRefusal::RecipientBalanceMismatch {
                vault,
                receipt_id: held.receipt_id,
                tracked: held.balance,
                at_recipient: *moved_to_recipient,
            });
        }

        migrated += 1;
    }

    // Only positive tracked balances corroborated at the recipient prove a
    // completed migration. An inventory containing only zero balances has
    // nothing to move, but it is not evidence that any custody transfer ever
    // happened and must not record a false migration history entry.
    if unmoved.is_empty() && migrated > 0 {
        return Ok(SourceCustody::AlreadyMigrated { receipts: migrated });
    }

    // Canonical order, independent of the inventory's backing map, keeps
    // transfer construction deterministic across retries.
    unmoved.sort_by_key(|held| held.receipt_id.inner());

    debug!(
        target: "receipt_inventory",
        %vault,
        %holder,
        tracked = tracked.len(),
        migratable = unmoved.len(),
        "Reconciled receipt holdings against the chain"
    );

    Ok(SourceCustody::Holds(MigratableHoldings {
        chain_id,
        vault,
        holder,
        holdings: unmoved,
    }))
}

/// Moves one vault's receipts to the incoming wallet.
///
/// Re-reads the transfer permission immediately before submitting, because
/// certification is maintained outside this service and can lapse between an
/// earlier preflight and the transaction landing.
async fn migrate_vault_custody(
    custody: &(impl ReceiptCustody + Sync),
    holdings: &MigratableHoldings,
    recipient: Address,
) -> Result<MigrationOutcome, MigrationRefusal> {
    let vault = holdings.vault();
    if holdings.holdings().is_empty() {
        return Err(ReceiptCustodyError::NothingToTransfer { vault }.into());
    }

    let receipts = holdings.holdings().len();
    if receipts > MAX_RECEIPTS_PER_TRANSFER {
        return Err(MigrationRefusal::ReceiptBatchTooLarge {
            vault,
            receipts,
            maximum: MAX_RECEIPTS_PER_TRANSFER,
        });
    }

    let permit = match custody
        .transfer_permission(vault, holdings.holder(), recipient)
        .await?
    {
        TransferPermission::Permitted(permit) => permit,
        TransferPermission::CertificationExpired => {
            return Err(MigrationRefusal::CertificationExpired { vault });
        }
        TransferPermission::OwnerFrozen { until } => {
            return Err(MigrationRefusal::OwnerFrozen {
                vault,
                from: holdings.holder(),
                to: recipient,
                until,
            });
        }
    };

    // Captured before the transfer so the post-condition measures what this
    // transfer delivered, not the recipient's absolute balance — which would
    // wrongly pass if the recipient already held some of these identifiers.
    let receipt_ids = holdings.receipt_ids();
    let recipient_before =
        custody.held_balances(vault, recipient, &receipt_ids).await?;

    // Checked before submitting: a truncated response here would otherwise
    // only surface in `verify_custody_moved`, after the irreversible
    // transfer has already gone out.
    if recipient_before.len() != receipt_ids.len() {
        return Err(MigrationRefusal::BalanceCountMismatch {
            vault,
            requested: receipt_ids.len(),
            returned: recipient_before.len(),
        });
    }

    let expected = holdings.total()?;
    let transaction = custody.transfer_custody(&permit, holdings).await?;

    verify_custody_moved(
        custody,
        holdings,
        recipient,
        &recipient_before,
        expected,
        transaction,
    )
    .await?;

    info!(
        target: "receipt_inventory",
        chain_id = holdings.chain_id(),
        %vault,
        %transaction,
        receipts,
        holder = %holdings.holder(),
        %recipient,
        "Migrated receipt custody"
    );

    Ok(MigrationOutcome::Migrated { transaction, receipts })
}

/// Re-reads both sides after the transfer and refuses to report success unless
/// every identifier actually moved.
///
/// Checked per identifier rather than on the totals alone: equal totals can
/// hide two receipts swapping balances, which would leave the inventory
/// describing custody that does not exist. The recipient side is checked as a
/// delta against `recipient_before` so a pre-existing balance neither masks a
/// failed transfer nor is mistaken for one this migration delivered.
async fn verify_custody_moved(
    custody: &(impl ReceiptCustody + Sync),
    holdings: &MigratableHoldings,
    recipient: Address,
    recipient_before: &[Shares],
    expected: Shares,
    transaction: B256,
) -> Result<(), MigrationRefusal> {
    let vault = holdings.vault();
    let receipt_ids = holdings.receipt_ids();

    let source =
        custody.held_balances(vault, holdings.holder(), &receipt_ids).await?;
    let destination =
        custody.held_balances(vault, recipient, &receipt_ids).await?;

    // `izip!` stops at the shortest input, so a truncated balance response
    // would silently verify fewer identifiers than were transferred.
    let expected_len = holdings.holdings().len();
    if source.len() != expected_len
        || destination.len() != expected_len
        || recipient_before.len() != expected_len
    {
        return Err(MigrationRefusal::BalanceCountMismatch {
            vault,
            requested: expected_len,
            returned: source
                .len()
                .min(destination.len())
                .min(recipient_before.len()),
        });
    }

    let (moved, gains) =
        izip!(holdings.holdings(), &source, &destination, recipient_before)
            .try_fold(
                (true, Vec::with_capacity(expected_len)),
                |(moved, mut gains), (held, retained, after, before)| {
                    // A recipient balance that went *down* over a transfer meant to
                    // increase it is a corrupt observation, not a zero gain. Capping it
                    // would hide the anomaly, which the financial-integrity rule in
                    // AGENTS.md forbids.
                    let gained = after.checked_sub(*before).ok_or(
                        RecipientBalanceDecrease {
                            vault,
                            tx_hash: transaction,
                            receipt_id: held.receipt_id,
                            before: *before,
                            after: *after,
                        },
                    )?;
                    gains.push(gained);
                    Ok::<_, ReceiptCustodyError>((
                        moved && retained.is_zero() && gained == held.balance,
                        gains,
                    ))
                },
            )?;

    let source_retained = total_of(source.iter().copied())?;
    let recipient_gained = total_of(gains.iter().copied())?;

    if !moved || !source_retained.is_zero() || recipient_gained != expected {
        return Err(ReceiptCustodyError::from(PostConditionFailure {
            vault,
            tx_hash: transaction,
            source_retained,
            recipient_gained,
            expected,
        })
        .into());
    }

    Ok(())
}

/// Reads both nonce views for `holder` and rejects a non-quiescent wallet —
/// the single entry point for the read+check pair, so every custody transfer
/// applies the identical guard.
///
/// Relies on `eth_getTransactionCount(holder, "pending")` reflecting the
/// node's actual mempool contents for `holder` — i.e. that it is >= the
/// `"latest"` count, with equality meaning no in-flight transaction. This has
/// not been confirmed against Base's, Ethereum's, or HyperEVM's RPC providers
/// specifically. If a provider instead aliases `"pending"` to `"latest"` (or
/// otherwise ignores mempool contents), the two views always agree and this
/// guard silently never blocks, even with a genuinely in-flight transaction
/// from `holder` — allowing the double-submission this guard exists to
/// prevent.
async fn ensure_holder_quiescent<HolderProvider: Provider>(
    provider: &HolderProvider,
    holder: Address,
) -> Result<(), ReceiptCustodyError> {
    let (latest_count, pending_count) = tokio::try_join!(
        provider.get_transaction_count(holder).latest(),
        provider.get_transaction_count(holder).pending(),
    )?;
    let latest_nonce = LatestNonce(latest_count);
    let pending_nonce = PendingNonce(pending_count);
    ensure_wallet_nonce_quiescent(holder, latest_nonce, pending_nonce)
}

/// Rejects a wallet whose `pending` and `latest` nonce views disagree.
const fn ensure_wallet_nonce_quiescent(
    holder: Address,
    latest_nonce: LatestNonce,
    pending_nonce: PendingNonce,
) -> Result<(), ReceiptCustodyError> {
    if pending_nonce.0 < latest_nonce.0 {
        return Err(ReceiptCustodyError::InvalidNonceOrder {
            holder,
            latest_nonce,
            pending_nonce,
        });
    }
    if pending_nonce.0 > latest_nonce.0 {
        return Err(ReceiptCustodyError::PendingWalletTransactions {
            holder,
            latest_nonce,
            pending_nonce,
        });
    }

    Ok(())
}

/// Rejects a permit that does not authorise exactly these holdings.
///
/// A permit names a vault and a sender. Holdings for a different vault, or
/// belonging to a different wallet, were never the subject of the vault's
/// authorisation check, so submitting them would move custody the vault never
/// approved. Free-standing rather than inline so the guard is exercisable
/// without a chain.
fn ensure_permit_covers(
    permit: &TransferPermit,
    holdings: &MigratableHoldings,
) -> Result<(), ReceiptCustodyError> {
    if permit.vault() != holdings.vault() {
        return Err(ReceiptCustodyError::WrongVault {
            expected: permit.vault(),
            requested: holdings.vault(),
        });
    }

    if permit.from() != holdings.holder() {
        return Err(ReceiptCustodyError::PermitHolderMismatch {
            permitted: permit.from(),
            holdings: holdings.holder(),
        });
    }

    Ok(())
}

fn total_of(
    balances: impl IntoIterator<Item = Shares>,
) -> Result<Shares, SharesOverflow> {
    balances
        .into_iter()
        .try_fold(Shares::ZERO, |running, balance| running + balance)
}

/// [`ReceiptCustody`] against a live chain.
///
/// The provider must be signing as the outgoing wallet for
/// [`ReceiptCustody::transfer_custody`] to succeed, since ERC-1155 only lets
/// the holder or an approved operator move a balance.
pub(crate) struct OnchainReceiptCustody<P> {
    provider: P,
    vault: Address,
    receipt_contract: Address,
}

impl<P: Provider + Clone> OnchainReceiptCustody<P> {
    pub(crate) const fn new(
        provider: P,
        vault: Address,
        receipt_contract: Address,
    ) -> Self {
        Self { provider, vault, receipt_contract }
    }

    /// Resolves the vault's Receipt contract from the vault itself, so the
    /// caller never has to carry an address that could drift out of step with
    /// the vault it belongs to.
    pub(crate) async fn resolve(
        provider: P,
        vault: Address,
    ) -> Result<Self, ReceiptCustodyError> {
        let receipt_contract = OffchainAssetReceiptVault::new(vault, &provider)
            .receipt()
            .call()
            .await?;

        Ok(Self::new(provider, vault, receipt_contract))
    }

    /// Rejects a vault other than the one this instance resolved its Receipt
    /// contract from. Without it the `vault` argument would be silently
    /// ignored and balances would be read from the wrong contract.
    fn check_vault(&self, vault: Address) -> Result<(), ReceiptCustodyError> {
        if vault != self.vault {
            return Err(ReceiptCustodyError::WrongVault {
                expected: self.vault,
                requested: vault,
            });
        }

        Ok(())
    }
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync> ReceiptCustody
    for OnchainReceiptCustody<P>
{
    async fn held_balances(
        &self,
        vault: Address,
        holder: Address,
        receipt_ids: &[ReceiptId],
    ) -> Result<Vec<Shares>, ReceiptCustodyError> {
        self.check_vault(vault)?;

        let receipt = Receipt::new(self.receipt_contract, &self.provider);
        let ids: Vec<U256> = receipt_ids.iter().map(ReceiptId::inner).collect();
        let holders = vec![holder; ids.len()];

        let balances = receipt.balanceOfBatch(holders, ids).call().await?;

        Ok(balances.into_iter().map(Shares::from).collect())
    }

    async fn transfer_permission(
        &self,
        vault: Address,
        from: Address,
        to: Address,
    ) -> Result<TransferPermission, ReceiptCustodyError> {
        self.check_vault(vault)?;

        let contract = OffchainAssetReceiptVault::new(vault, &self.provider);

        if contract.isCertificationExpired().call().await? {
            return Ok(TransferPermission::CertificationExpired);
        }

        // Mirrors `ownerFreezeCheckTransaction`, which reverts only when the
        // freeze is live AND neither side is on an always-allowed list.
        let frozen_until = contract.ownerFrozenUntil().call().await?;
        let latest = self
            .provider
            .get_block(BlockId::latest())
            .await?
            .ok_or(ReceiptCustodyError::NoLatestBlock)?;
        let now = U256::from(latest.header.timestamp);

        if now <= frozen_until {
            let allowed_from =
                contract.ownerFreezeAlwaysAllowedFrom(from).call().await?;
            let allowed_to =
                contract.ownerFreezeAlwaysAllowedTo(to).call().await?;

            // The always-allowed values are timestamps, not flags: an entry
            // exempts its side only while `now` is within it, exactly as the
            // vault's `ownerFreezeCheckTransaction` evaluates them. A stale
            // (expired) entry is no exemption at all, so treating any
            // non-zero value as exempt would report `Permitted` for a pair
            // the transfer would revert on.
            if allowed_from < now && allowed_to < now {
                return Ok(TransferPermission::OwnerFrozen {
                    until: frozen_until,
                });
            }
        }

        Ok(TransferPermission::Permitted(TransferPermit::granted(
            vault, from, to,
        )))
    }

    async fn transfer_custody(
        &self,
        permit: &TransferPermit,
        holdings: &MigratableHoldings,
    ) -> Result<B256, ReceiptCustodyError> {
        self.check_vault(permit.vault())?;
        ensure_permit_covers(permit, holdings)?;

        // A previous transfer may have been broadcast just before the driver
        // crashed. While it remains pending, re-submitting would create a
        // second attempt without knowing the first one's outcome.
        // Once it drops, both nonce views converge and retrying the identical
        // transfer is safe; once it mines, the next reconciliation observes
        // moved custody and never reaches this submission path.
        ensure_holder_quiescent(&self.provider, permit.from()).await?;

        let receipt = Receipt::new(self.receipt_contract, &self.provider);
        let (ids, amounts) = holdings.batch_arguments();

        let pending = receipt
            .safeBatchTransferFrom(
                permit.from(),
                permit.to(),
                ids,
                amounts,
                Bytes::new(),
            )
            .send()
            .await?;

        // Without a deadline a stalled or underpriced transfer leaves the
        // operator's terminal blocked indefinitely with custody already
        // submitted. 120s matches the vault flow's mint and burn watches
        // (`crate::vault::service`); on timeout the error carries the hash so
        // the transfer can be reconciled or re-run.
        let confirmed = pending
            .with_timeout(Some(Duration::from_secs(120)))
            .get_receipt()
            .await?;
        let tx_hash = confirmed.transaction_hash;

        // A mined-but-reverted transfer moved nothing, so it is a definitive
        // failure rather than a hash to report as success.
        if !confirmed.status() {
            return Err(ReceiptCustodyError::Reverted {
                vault: permit.vault(),
                tx_hash,
            });
        }

        Ok(tx_hash)
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::logs_contain_at;

    const VAULT: Address = address!("00000000000000000000000000000000000000aa");
    const OUTGOING: Address =
        address!("00000000000000000000000000000000000000bb");
    const INCOMING: Address =
        address!("00000000000000000000000000000000000000cc");
    const CHAIN_ID: u64 = 8453;

    /// Fake custody that actually moves balances on transfer, so the
    /// post-condition assertions in `migrate_vault_custody` are exercised
    /// rather than trivially satisfied.
    struct FakeCustody {
        balances: Mutex<HashMap<(Address, ReceiptId), Shares>>,
        permission: Permission,
        transfers: Mutex<Vec<(Address, usize)>>,
        settle_transfer: bool,
        /// Return one fewer balance than asked for, but only when querying this
        /// holder, so a test can truncate the source and recipient reads
        /// independently.
        truncate_for: Option<Address>,
        /// Credit the recipient with the balances rotated across identifiers.
        /// The totals still match; only the per-identifier mapping is wrong.
        swap_on_settle: bool,
        /// Leave the recipient holding less than it did before the transfer,
        /// modelling a corrupt or reorganised observation.
        shrink_recipient_on_settle: bool,
    }

    /// The fake's scripted permission, kept separate from
    /// [`TransferPermission`] because a permit may only be minted by the code
    /// under test.
    #[derive(Clone, PartialEq, Eq)]
    enum Permission {
        Permitted,
        CertificationExpired,
        OwnerFrozen { until: U256 },
    }

    impl FakeCustody {
        fn holding(receipt_id: u64, balance: u64) -> ReceiptHolding {
            ReceiptHolding {
                receipt_id: ReceiptId::from(U256::from(receipt_id)),
                balance: Shares::new(U256::from(balance)),
            }
        }

        fn with_balances(held: &[(Address, u64, u64)]) -> Self {
            let balances = held
                .iter()
                .map(|(holder, receipt_id, balance)| {
                    (
                        (*holder, ReceiptId::from(U256::from(*receipt_id))),
                        Shares::new(U256::from(*balance)),
                    )
                })
                .collect();

            Self {
                balances: Mutex::new(balances),
                permission: Permission::Permitted,
                transfers: Mutex::new(Vec::new()),
                settle_transfer: true,
                truncate_for: None,
                swap_on_settle: false,
                shrink_recipient_on_settle: false,
            }
        }

        fn at_source(held: &[(u64, u64)]) -> Self {
            let owned: Vec<(Address, u64, u64)> = held
                .iter()
                .map(|(receipt_id, balance)| (OUTGOING, *receipt_id, *balance))
                .collect();
            Self::with_balances(&owned)
        }

        fn refusing(permission: Permission) -> Self {
            Self { permission, ..Self::at_source(&[(1, 100)]) }
        }

        fn transfer_count(&self) -> usize {
            self.transfers.lock().expect("transfers lock").len()
        }
    }

    #[async_trait]
    impl ReceiptCustody for FakeCustody {
        async fn held_balances(
            &self,
            _vault: Address,
            holder: Address,
            receipt_ids: &[ReceiptId],
        ) -> Result<Vec<Shares>, ReceiptCustodyError> {
            let balances = self.balances.lock().expect("balances lock");

            let mut found: Vec<Shares> = receipt_ids
                .iter()
                .map(|receipt_id| {
                    balances
                        .get(&(holder, *receipt_id))
                        .copied()
                        .unwrap_or(Shares::ZERO)
                })
                .collect();

            if self.truncate_for == Some(holder) {
                found.pop();
            }

            Ok(found)
        }

        async fn transfer_permission(
            &self,
            vault: Address,
            from: Address,
            to: Address,
        ) -> Result<TransferPermission, ReceiptCustodyError> {
            Ok(match self.permission {
                Permission::Permitted => TransferPermission::Permitted(
                    TransferPermit::granted(vault, from, to),
                ),
                Permission::CertificationExpired => {
                    TransferPermission::CertificationExpired
                }
                Permission::OwnerFrozen { until } => {
                    TransferPermission::OwnerFrozen { until }
                }
            })
        }

        async fn transfer_custody(
            &self,
            permit: &TransferPermit,
            holdings: &MigratableHoldings,
        ) -> Result<B256, ReceiptCustodyError> {
            self.transfers
                .lock()
                .expect("transfers lock")
                .push((permit.to(), holdings.holdings().len()));

            if self.settle_transfer {
                let mut balances = self.balances.lock().expect("balances lock");
                let moved = holdings.holdings();

                for (index, held) in moved.iter().enumerate() {
                    balances
                        .insert((permit.from(), held.receipt_id), Shares::ZERO);

                    if self.shrink_recipient_on_settle {
                        balances.insert(
                            (permit.to(), held.receipt_id),
                            Shares::ZERO,
                        );
                        continue;
                    }

                    // Rotating the amounts by one keeps the total identical
                    // while putting each amount against the wrong identifier.
                    let credited = if self.swap_on_settle {
                        moved[(index + 1) % moved.len()].balance
                    } else {
                        held.balance
                    };
                    let existing = balances
                        .get(&(permit.to(), held.receipt_id))
                        .copied()
                        .unwrap_or(Shares::ZERO);
                    balances.insert(
                        (permit.to(), held.receipt_id),
                        (existing + credited).expect("no overflow"),
                    );
                }
            }

            Ok(B256::repeat_byte(7))
        }
    }

    async fn reconcile(
        custody: &FakeCustody,
        tracked: &[ReceiptHolding],
    ) -> Result<SourceCustody, MigrationRefusal> {
        reconcile_holdings(
            custody, CHAIN_ID, VAULT, OUTGOING, INCOMING, tracked,
        )
        .await
    }

    fn post_condition_failure(
        refusal: &MigrationRefusal,
    ) -> &PostConditionFailure {
        match refusal {
            MigrationRefusal::Custody(boxed) => match &**boxed {
                ReceiptCustodyError::PostConditionFailed(failure) => failure,
                other => {
                    panic!("expected a post-condition failure, got {other:?}")
                }
            },
            other => panic!("expected a custody error, got {other:?}"),
        }
    }

    fn holdings_of(source: SourceCustody) -> MigratableHoldings {
        match source {
            SourceCustody::Holds(holdings) => holdings,
            SourceCustody::AlreadyMigrated { .. } => {
                panic!("expected the source to still hold its receipts")
            }
        }
    }

    #[test]
    fn quiescence_accepts_an_idle_deployment() {
        require_quiescent(VAULT, &[], 0, 0).unwrap();
    }

    #[test]
    fn quiescence_refuses_while_a_burn_is_reserved() {
        // In-flight redemptions can outlive any operational pause, so a
        // reservation is exactly the race that would burn against custody we
        // just moved.
        let reserved = [ReceiptId::from(U256::from(1))];

        let refusal = require_quiescent(VAULT, &reserved, 0, 0).unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::BurnReserved { vault, receipts }
                    if vault == VAULT && receipts == 1
            ),
            "a reserved burn must halt the migration, got {refusal:?}"
        );
    }

    /// A redemption between detection and terminal holds no reservation yet,
    /// but resumes on restart and plans a burn against the new wallet while
    /// the participant's money already moved. The gate must catch it even
    /// though the reservation gate cannot.
    #[test]
    fn quiescence_refuses_while_a_redemption_is_in_flight() {
        let refusal = require_quiescent(VAULT, &[], 2, 0).unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::RedemptionsInFlight { count: 2 }
            ),
            "in-flight redemptions must halt the migration, got {refusal:?}"
        );
    }

    /// A non-terminal mint's recovery rebroadcasts a transaction signed by the
    /// old wallet, depositing a fresh receipt at an address the migrated
    /// deployment no longer watches.
    #[test]
    fn quiescence_refuses_while_a_mint_is_in_flight() {
        let refusal = require_quiescent(VAULT, &[], 0, 1).unwrap_err();

        assert!(
            matches!(refusal, MigrationRefusal::MintsInFlight { count: 1 }),
            "in-flight mints must halt the migration, got {refusal:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn reconcile_accepts_inventory_agreeing_with_chain() {
        let custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];

        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        assert_eq!(holdings.holdings().len(), 2);
        assert_eq!(holdings.vault(), VAULT);
        assert_eq!(holdings.holder(), OUTGOING);
        assert_eq!(holdings.chain_id(), CHAIN_ID);
        assert_eq!(holdings.total().unwrap(), Shares::new(U256::from(350)));

        let (ids, amounts) = holdings.batch_arguments();
        assert_eq!(ids, vec![U256::from(1), U256::from(2)]);
        assert_eq!(amounts, vec![U256::from(100), U256::from(250)]);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Reconciled receipt holdings against the chain", "migratable=2"]
        ));
    }

    #[tokio::test]
    async fn reconcile_refuses_when_chain_disagrees_with_inventory() {
        let custody = FakeCustody::at_source(&[(1, 40)]);
        let tracked = [FakeCustody::holding(1, 100)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::InventoryDivergence {
                    tracked, onchain, ..
                } if tracked == Shares::new(U256::from(100))
                    && onchain == Shares::new(U256::from(40))
            ),
            "divergence must halt the migration, got {refusal:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_refuses_a_short_balance_response() {
        let mut custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        custody.truncate_for = Some(OUTGOING);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::BalanceCountMismatch {
                    requested, returned, ..
                } if requested == 2 && returned == 1
            ),
            "a short balance response must not be silently zero-padded, got \
             {refusal:?}"
        );
    }

    #[tokio::test]
    async fn reconcile_drops_receipts_the_wallet_no_longer_holds() {
        let custody = FakeCustody::at_source(&[(1, 100), (2, 0)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 0)];

        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        assert_eq!(
            holdings.holdings().len(),
            1,
            "a zero balance is nothing to move and must not enter the batch"
        );
    }

    #[tokio::test]
    async fn zero_only_inventory_is_not_reported_as_already_migrated() {
        let custody =
            FakeCustody::with_balances(&[(OUTGOING, 1, 0), (INCOMING, 1, 0)]);
        let tracked = [FakeCustody::holding(1, 0)];

        let source = reconcile(&custody, &tracked).await.unwrap();
        let SourceCustody::Holds(holdings) = source else {
            panic!(
                "zero balances cannot prove a completed migration, got {source:?}"
            );
        };

        assert!(
            holdings.holdings().is_empty(),
            "zero balances must not enter a transfer batch"
        );
    }

    /// A historical partial move can leave some identifiers already with the
    /// recipient and the rest still with the holder. A rerun must resume the
    /// remainder rather than refuse the moved identifiers as divergence.
    #[traced_test]
    #[tokio::test]
    async fn reconcile_resumes_a_partially_migrated_vault() {
        let custody = FakeCustody::with_balances(&[
            (OUTGOING, 1, 100),
            (INCOMING, 2, 250),
        ]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];

        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        assert_eq!(
            holdings.holdings().len(),
            1,
            "only the identifier still with the holder is left to migrate"
        );
        let (ids, amounts) = holdings.batch_arguments();
        assert_eq!(ids, vec![U256::from(1)]);
        assert_eq!(amounts, vec![U256::from(100)]);
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Reconciled receipt holdings against the chain", "migratable=1"]
        ));
    }

    /// A zero tracked balance means the wallet should hold nothing for that
    /// identifier; an on-chain balance appearing there is an inventory that
    /// cannot be trusted, not something to silently skip.
    #[tokio::test]
    async fn reconcile_refuses_an_untracked_onchain_balance() {
        let custody = FakeCustody::at_source(&[(1, 100), (2, 70)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 0)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::InventoryDivergence {
                    tracked, onchain, ..
                } if tracked == Shares::ZERO
                    && onchain == Shares::new(U256::from(70))
            ),
            "an untracked on-chain balance must refuse, got {refusal:?}"
        );
    }

    /// An identifier the holder no longer holds whose balance never reached
    /// the recipient is untraceable custody, not a resumable move.
    #[tokio::test]
    async fn reconcile_refuses_a_vanished_identifier() {
        let custody = FakeCustody::with_balances(&[(OUTGOING, 1, 100)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::RecipientBalanceMismatch {
                    tracked, at_recipient, ..
                } if tracked == Shares::new(U256::from(250))
                    && at_recipient == Shares::ZERO
            ),
            "a vanished balance must refuse, got {refusal:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn migrate_moves_every_receipt_and_verifies_post_conditions() {
        let custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let outcome =
            migrate_vault_custody(&custody, &holdings, INCOMING).await.unwrap();

        assert_eq!(
            outcome,
            MigrationOutcome::Migrated {
                transaction: B256::repeat_byte(7),
                receipts: 2
            }
        );
        assert_eq!(
            custody.transfers.lock().unwrap().as_slice(),
            [(INCOMING, 2)]
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Migrated receipt custody", "receipts=2"]
        ));
    }

    #[tokio::test]
    async fn migrate_refuses_more_receipts_than_the_proven_batch_limit() {
        let source: Vec<(u64, u64)> = (1..=15).map(|id| (id, 100)).collect();
        let custody = FakeCustody::at_source(&source);
        let tracked: Vec<ReceiptHolding> = source
            .iter()
            .map(|(receipt_id, balance)| {
                FakeCustody::holding(*receipt_id, *balance)
            })
            .collect();
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        assert!(matches!(
            refusal,
            MigrationRefusal::ReceiptBatchTooLarge {
                vault: VAULT,
                receipts: 15,
                maximum: 14,
            }
        ));
        assert_eq!(
            custody.transfer_count(),
            0,
            "an unproven batch size must be refused before submission"
        );
    }

    #[tokio::test]
    async fn migrate_measures_the_recipients_gain_not_its_balance() {
        // The recipient already holds receipt 1. A post-condition comparing the
        // absolute balance would accept a transfer that never moved anything.
        let custody = FakeCustody::with_balances(&[
            (OUTGOING, 1, 100),
            (INCOMING, 1, 100),
        ]);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let outcome =
            migrate_vault_custody(&custody, &holdings, INCOMING).await.unwrap();

        assert_eq!(
            outcome,
            MigrationOutcome::Migrated {
                transaction: B256::repeat_byte(7),
                receipts: 1
            },
            "the recipient's pre-existing balance must not mask the transfer"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn rerunning_a_completed_migration_is_a_no_op() {
        let custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());
        migrate_vault_custody(&custody, &holdings, INCOMING).await.unwrap();

        // The inventory is unchanged — it is keyed by vault, not by holder — so
        // a re-run sees tracked balances the source no longer has.
        let rerun = reconcile(&custody, &tracked).await.unwrap();

        assert_eq!(
            rerun,
            SourceCustody::AlreadyMigrated { receipts: 2 },
            "a completed migration must re-run as a no-op, not a divergence"
        );
        assert_eq!(
            custody.transfer_count(),
            1,
            "the re-run must not submit a second transfer"
        );
    }

    #[tokio::test]
    async fn migrate_refuses_while_certification_is_expired() {
        let custody = FakeCustody::refusing(Permission::CertificationExpired);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::CertificationExpired { vault } if vault == VAULT
            ),
            "expired certification must halt before submitting, got {refusal:?}"
        );
        assert_eq!(
            custody.transfer_count(),
            0,
            "no transaction may be submitted into a reverting gate"
        );
    }

    #[tokio::test]
    async fn migrate_refuses_while_the_owner_freeze_blocks_the_pair() {
        let custody = FakeCustody::refusing(Permission::OwnerFrozen {
            until: U256::from(1_800_000_000_u64),
        });
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::OwnerFrozen { from, to, .. }
                    if from == OUTGOING && to == INCOMING
            ),
            "an owner freeze must halt before submitting, got {refusal:?}"
        );
        assert_eq!(custody.transfer_count(), 0);
    }

    #[tokio::test]
    async fn migrate_fails_closed_when_receipts_land_on_the_wrong_identifier() {
        // The exact case per-identifier verification exists for: the recipient
        // ends up with the right total but each amount against the wrong
        // receipt. A totals-only check would call this a success.
        let mut custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        custody.swap_on_settle = true;
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        let failure = post_condition_failure(&refusal);
        assert_eq!(
            failure.recipient_gained, failure.expected,
            "the totals must match — this is exactly the case per-identifier \
             verification exists to catch"
        );
        assert!(
            matches!(
                refusal,
                MigrationRefusal::Custody(ref boxed)
                    if matches!(**boxed,
                        ReceiptCustodyError::PostConditionFailed(_))
            ),
            "matching totals must not rescue a per-identifier mismatch, got \
             {refusal:?}"
        );
    }

    #[tokio::test]
    async fn a_migration_to_a_recipient_holding_a_balance_still_reruns_cleanly()
    {
        // The forward path allows a recipient that already holds some of these
        // identifiers. If the already-migrated check demanded the recipient
        // hold *exactly* the tracked amount, that migration could never be
        // re-run: the recipient would hold pre-existing + migrated.
        let custody = FakeCustody::with_balances(&[
            (OUTGOING, 1, 100),
            (INCOMING, 1, 70),
        ]);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());
        migrate_vault_custody(&custody, &holdings, INCOMING).await.unwrap();

        let rerun = reconcile(&custody, &tracked).await.unwrap();

        assert_eq!(
            rerun,
            SourceCustody::AlreadyMigrated { receipts: 1 },
            "a completed migration must re-run as a no-op even when the \
             recipient held a balance beforehand"
        );
        assert_eq!(custody.transfer_count(), 1);
    }

    #[tokio::test]
    async fn a_short_recipient_response_is_not_read_as_already_migrated() {
        // Source empty, so the already-migrated path runs — but the recipient
        // read comes back short. Zip-style iteration would silently compare
        // fewer identifiers and declare the migration complete.
        let mut custody = FakeCustody::with_balances(&[
            (OUTGOING, 1, 0),
            (OUTGOING, 2, 0),
            (INCOMING, 1, 100),
            (INCOMING, 2, 250),
        ]);
        custody.truncate_for = Some(INCOMING);
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::BalanceCountMismatch {
                    requested, returned, ..
                } if requested == 2 && returned == 1
            ),
            "a short recipient response must halt, got {refusal:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_source_reports_the_recipient_balance_it_queried() {
        let custody =
            FakeCustody::with_balances(&[(OUTGOING, 1, 0), (INCOMING, 1, 5)]);
        let tracked = [FakeCustody::holding(1, 100)];

        let refusal = reconcile(&custody, &tracked).await.unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::RecipientBalanceMismatch {
                    tracked, at_recipient, ..
                } if tracked == Shares::new(U256::from(100))
                    && at_recipient == Shares::new(U256::from(5))
            ),
            "the refusal must name the recipient balance it actually read, \
             got {refusal:?}"
        );
    }

    #[test]
    fn a_pending_wallet_transaction_blocks_a_second_custody_submission() {
        let error = ensure_wallet_nonce_quiescent(
            OUTGOING,
            LatestNonce(7),
            PendingNonce(8),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ReceiptCustodyError::PendingWalletTransactions {
                holder: OUTGOING,
                latest_nonce: LatestNonce(7),
                pending_nonce: PendingNonce(8),
            }
        ));
        ensure_wallet_nonce_quiescent(
            OUTGOING,
            LatestNonce(8),
            PendingNonce(8),
        )
        .unwrap();
        assert!(matches!(
            ensure_wallet_nonce_quiescent(
                OUTGOING,
                LatestNonce(9),
                PendingNonce(8)
            )
            .unwrap_err(),
            ReceiptCustodyError::InvalidNonceOrder {
                holder: OUTGOING,
                latest_nonce: LatestNonce(9),
                pending_nonce: PendingNonce(8),
            }
        ));
    }

    #[tokio::test]
    async fn a_permit_for_another_vault_cannot_move_these_holdings() {
        let custody = FakeCustody::at_source(&[(1, 100)]);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());
        let other_vault = address!("00000000000000000000000000000000000000dd");
        let permit = TransferPermit::granted(other_vault, OUTGOING, INCOMING);

        let error = ensure_permit_covers(&permit, &holdings).unwrap_err();

        assert!(
            matches!(
                error,
                ReceiptCustodyError::WrongVault { expected, requested }
                    if expected == other_vault && requested == VAULT
            ),
            "a permit for another vault must not authorise this transfer, got \
             {error:?}"
        );
    }

    #[tokio::test]
    async fn a_permit_for_another_sender_cannot_move_these_holdings() {
        let custody = FakeCustody::at_source(&[(1, 100)]);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());
        let stranger = address!("00000000000000000000000000000000000000dd");
        let permit = TransferPermit::granted(VAULT, stranger, INCOMING);

        let error = ensure_permit_covers(&permit, &holdings).unwrap_err();

        assert!(
            matches!(
                error,
                ReceiptCustodyError::PermitHolderMismatch { permitted, holdings }
                    if permitted == stranger && holdings == OUTGOING
            ),
            "a permit naming another sender must not authorise this transfer, \
             got {error:?}"
        );
    }

    #[tokio::test]
    async fn a_matching_permit_covers_its_holdings() {
        let custody = FakeCustody::at_source(&[(1, 100)]);
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());
        let permit = TransferPermit::granted(VAULT, OUTGOING, INCOMING);

        assert!(ensure_permit_covers(&permit, &holdings).is_ok());
    }

    mod projection_rebuild {
        use alloy::primitives::{Address, B256};
        use rust_decimal::Decimal;
        use std::collections::HashMap;
        use tracing_test::traced_test;

        use super::*;
        use crate::config::VaultMode;
        use crate::mint::{
            ClientId, IssuerMintRequestId, MintCommand, Quantity,
            TokenizationRequestId,
        };
        use crate::redemption::{RedemptionCommand, RedemptionServices};
        use crate::test_utils::logs_contain_at;
        use crate::tokenized_asset::TokenSymbol;
        use crate::vault::NetworkVaultServices;

        #[traced_test]
        #[tokio::test]
        async fn custody_preparation_rebuilds_in_flight_work_views() {
            let pool = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();

            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            let token = TokenSymbol::new("tAAPL");
            let wallet = Address::repeat_byte(0x44);
            let quantity = Quantity::new(Decimal::ONE);

            let mint_id = IssuerMintRequestId::random();
            let (mint_store, _mint_projection) =
                StoreBuilder::<Mint>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();
            mint_store
                .send(
                    &mint_id,
                    MintCommand::Initiate {
                        issuer_request_id: mint_id.clone(),
                        tokenization_request_id: TokenizationRequestId::new(
                            "custody-prep-mint",
                        ),
                        quantity: quantity.clone(),
                        underlying: underlying.clone(),
                        token: token.clone(),
                        network: Network::Base,
                        client_id: ClientId::new(),
                        wallet,
                        mint_mode: VaultMode::VaultDirect,
                    },
                )
                .await
                .unwrap();

            let redemption_id = IssuerRedemptionRequestId::random();
            let redemption_services = RedemptionServices::new(
                NetworkVaultServices::new(HashMap::new()),
            );
            let redemption_store =
                StoreBuilder::<Redemption>::new(pool.clone())
                    .build(redemption_services)
                    .await
                    .unwrap();
            redemption_store
                .send(
                    &redemption_id,
                    RedemptionCommand::Detect {
                        issuer_request_id: redemption_id.clone(),
                        underlying: underlying.clone(),
                        token,
                        network: Network::Base,
                        wallet,
                        quantity,
                        tx_hash: B256::repeat_byte(0x55),
                        block_number: 1,
                        burn_mode: VaultMode::VaultDirect,
                    },
                )
                .await
                .unwrap();

            sqlx::query("DELETE FROM mint_view").execute(&pool).await.unwrap();
            sqlx::query("DELETE FROM redemption_view")
                .execute(&pool)
                .await
                .unwrap();

            prepare_custody_engine_state(&pool).await.unwrap();

            assert_eq!(find_stuck_mints(&pool).await.unwrap().len(), 1);
            assert_eq!(find_stuck_redemptions(&pool).await.unwrap().len(), 1);
            assert!(logs_contain_at!(
                tracing::Level::DEBUG,
                &["Prepared custody engine projections from event history"]
            ));
        }
    }

    #[tokio::test]
    async fn a_recipient_balance_that_falls_is_reported_not_capped() {
        // The recipient holds 70 before the transfer and 0 after. Capping the
        // delta at zero would report a plain post-condition failure and hide
        // that the observation itself is impossible.
        let mut custody = FakeCustody::with_balances(&[
            (OUTGOING, 1, 100),
            (INCOMING, 1, 70),
        ]);
        custody.shrink_recipient_on_settle = true;
        let tracked = [FakeCustody::holding(1, 100)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::Custody(ref boxed)
                    if matches!(**boxed,
                        ReceiptCustodyError::RecipientBalanceDecreased(ref decrease)
                            if decrease.before == Shares::new(U256::from(70))
                                && decrease.after == Shares::ZERO)
            ),
            "a falling recipient balance must be surfaced with the values \
             observed, got {refusal:?}"
        );
    }

    #[tokio::test]
    async fn migrate_fails_closed_when_custody_does_not_actually_move() {
        let mut custody = FakeCustody::at_source(&[(1, 100), (2, 250)]);
        custody.settle_transfer = false;
        let tracked =
            [FakeCustody::holding(1, 100), FakeCustody::holding(2, 250)];
        let holdings =
            holdings_of(reconcile(&custody, &tracked).await.unwrap());

        let refusal = migrate_vault_custody(&custody, &holdings, INCOMING)
            .await
            .unwrap_err();

        assert!(
            matches!(
                refusal,
                MigrationRefusal::Custody(ref boxed)
                    if matches!(**boxed,
                        ReceiptCustodyError::PostConditionFailed(_))
            ),
            "a transaction that did not move custody must not report success, \
             got {refusal:?}"
        );
    }

    /// An ERC-1155 transfer to a wrong address is final: no counterparty, no
    /// recovery, and the receipts back tokens that are still outstanding. These
    /// cover the last gate before the transfer, the only one that consults
    /// something other than what the operator typed.
    mod recipient_corroboration {
        use alloy::providers::ProviderBuilder;
        use alloy::providers::ext::AnvilApi;

        use super::*;
        use crate::test_utils::LocalEvm;

        /// Runtime bytecode returning 32 bytes ending in `0x01` for ANY
        /// call: a fallback that affirms every interface, which EIP-165's
        /// `0xffffffff` probe exists to unmask.
        const AFFIRMS_EVERYTHING: [u8; 10] =
            [0x60, 0x01, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3];

        /// Runtime bytecode that is the INVALID opcode: every call reverts,
        /// like a contract with no fallback and no ERC-165.
        const REVERTS_EVERY_CALL: [u8; 1] = [0xfe];

        #[tokio::test]
        async fn the_current_holder_cannot_be_its_own_destination() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                evm.wallet_address,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::RecipientIsHolder { recipient })
                        if *recipient == evm.wallet_address
                ),
                "custody cannot migrate to its current holder, got: {error:?}"
            );
        }

        #[tokio::test]
        async fn an_address_the_chain_has_never_seen_is_refused() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let typo = Address::random();
            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                typo,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::RecipientUnknownToChain {
                        recipient,
                        ..
                    }) if *recipient == typo
                ),
                "an unfunded, never-used address is what a typo looks like and \
                 must not be accepted, got: {error:?}"
            );
        }

        #[tokio::test]
        async fn the_zero_address_is_refused() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                Address::ZERO,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::RecipientIsZeroAddress { .. })
                ),
                "got: {error:?}"
            );
        }

        /// A funded destination distinct from the holder clears the gate,
        /// and the witness records what was proven: an externally owned
        /// account.
        #[tokio::test]
        async fn a_funded_wallet_is_corroborated() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let corroborated = CorroboratedRecipient::verify(
                &provider,
                Address::random(),
                evm.wallet_address,
            )
            .await
            .unwrap();

            assert_eq!(corroborated.address(), evm.wallet_address);
            assert_eq!(corroborated.kind(), RecipientKind::ExternallyOwned);
        }

        /// The orchestrator is the one contract destination the migration
        /// exists for: a consistent ERC-165 responder affirming
        /// `IERC1155Receiver`, corroborated as such.
        #[tokio::test]
        async fn the_orchestrator_is_corroborated_as_a_receiver() {
            let evm = LocalEvm::new().await.unwrap();
            let orchestrator = evm.deploy_orchestrator().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let corroborated = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                orchestrator,
            )
            .await
            .unwrap();

            assert_eq!(corroborated.address(), orchestrator);
            assert_eq!(corroborated.kind(), RecipientKind::Erc1155Receiver);
        }

        /// The vault's receipt contract is a genuine, consistent ERC-165
        /// responder (an ERC-1155 token) that is NOT an ERC-1155 receiver —
        /// transfers to it would revert, so it must be refused on its own
        /// answer, before any transaction exists.
        #[tokio::test]
        async fn a_contract_that_answers_false_for_receiver_support_is_refused()
        {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
            let receipt_contract: Address =
                OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                    .receipt()
                    .call()
                    .await
                    .unwrap()
                    .0
                    .into();

            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                receipt_contract,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::RecipientContractRefusesReceipts {
                        recipient,
                    }) if *recipient == receipt_contract
                ),
                "a non-receiver contract must be refused on its ERC-165 \
                 answer, got: {error:?}"
            );
        }

        /// A fallback answering true for everything claims `0xffffffff` too,
        /// which a compliant ERC-165 responder must deny — its answers prove
        /// nothing, so its receiver-support claim is not trusted.
        #[tokio::test]
        async fn a_contract_affirming_every_interface_is_refused() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
            let stub = Address::random();
            provider
                .anvil_set_code(stub, AFFIRMS_EVERYTHING.to_vec().into())
                .await
                .unwrap();

            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                stub,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::RecipientErc165Inconsistent {
                        recipient,
                    }) if *recipient == stub
                ),
                "an affirm-everything fallback proves nothing and must be \
                 refused, got: {error:?}"
            );
        }

        /// A contract that reverts every call (no ERC-165 at all) cannot
        /// prove receiver support, so it is refused before any transaction —
        /// never discovered by the transfer's own revert.
        #[tokio::test]
        async fn a_contract_without_erc165_is_refused_as_unproven() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();
            let stub = Address::random();
            provider
                .anvil_set_code(stub, REVERTS_EVERY_CALL.to_vec().into())
                .await
                .unwrap();

            let error = CorroboratedRecipient::verify(
                &provider,
                evm.wallet_address,
                stub,
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(
                        MigrationRefusal::RecipientReceiverSupportUnproven {
                            recipient,
                            ..
                        }
                    ) if *recipient == stub
                ),
                "receiver support must be proven before submitting, never \
                 discovered by a revert, got: {error:?}"
            );
        }

        #[test]
        fn a_recipient_corroborated_on_another_chain_is_refused() {
            let error = corroborate_recipient_chain(8453, 1).unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::RecipientChainMismatch {
                    corroborated: 1,
                    migration: 8453,
                }
            ));
        }
    }

    mod vault_identity_corroboration {
        use sqlx::sqlite::SqlitePoolOptions;

        use super::*;
        use crate::tokenized_asset::{
            AssetKey, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        };

        async fn pool_with_listing(vault: Address) -> Pool<Sqlite> {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            let (store, _) = StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            store
                .send(
                    &AssetKey::new(underlying.clone(), Network::Base),
                    TokenizedAssetCommand::Add {
                        underlying,
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::Base,
                        vault,
                    },
                )
                .await
                .unwrap();

            pool
        }

        async fn empty_pool() -> Pool<Sqlite> {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            pool
        }

        #[test]
        fn a_chain_not_named_by_the_network_is_refused() {
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            let error = VaultIdentity::from_observations(
                Network::Base,
                1,
                1,
                VAULT,
                VAULT,
                &underlying,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::NetworkChainMismatch {
                    network: Network::Base,
                    expected: 8453,
                    requested: 1,
                }
            ));
        }

        #[test]
        fn a_provider_on_another_chain_is_refused() {
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            let error = VaultIdentity::from_observations(
                Network::Base,
                8453,
                1,
                VAULT,
                VAULT,
                &underlying,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::ProviderChainMismatch {
                    expected: 8453,
                    actual: 1,
                }
            ));
        }

        #[test]
        fn a_vault_not_bound_to_the_underlying_listing_is_refused() {
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            let error = VaultIdentity::from_observations(
                Network::Base,
                8453,
                8453,
                VAULT,
                INCOMING,
                &underlying,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::VaultListingMismatch {
                    requested: VAULT,
                    listed: INCOMING,
                    ..
                }
            ));
        }

        #[tokio::test]
        async fn identity_is_recorroborated_against_the_execution_pool() {
            let verification_pool = pool_with_listing(VAULT).await;
            let execution_pool = pool_with_listing(INCOMING).await;
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();
            let identity = VaultIdentity::from_observations(
                Network::Base,
                CHAIN_ID,
                CHAIN_ID,
                VAULT,
                VAULT,
                &underlying,
            )
            .unwrap();
            identity.corroborate_listing(&verification_pool).await.unwrap();

            let error = identity
                .corroborate_listing(&execution_pool)
                .await
                .unwrap_err();

            assert!(matches!(
                error.downcast_ref::<MigrationRefusal>(),
                Some(MigrationRefusal::VaultListingMismatch {
                    requested: VAULT,
                    listed: INCOMING,
                    ..
                })
            ));
        }

        #[tokio::test]
        async fn verification_rebuilds_an_empty_listing_projection() {
            let pool = pool_with_listing(VAULT).await;
            sqlx::query("DELETE FROM tokenized_asset_view")
                .execute(&pool)
                .await
                .unwrap();
            let evm = crate::test_utils::LocalEvm::with_chain_id(CHAIN_ID)
                .await
                .unwrap();
            let provider = alloy::providers::ProviderBuilder::new()
                .connect(&evm.endpoint)
                .await
                .unwrap();
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();

            let identity = VaultIdentity::verify(
                &pool,
                &provider,
                Network::Base,
                CHAIN_ID,
                VAULT,
                &underlying,
            )
            .await
            .unwrap();

            assert_eq!(identity.vault, VAULT);
        }

        #[tokio::test]
        async fn verification_refuses_an_underlying_without_a_listing() {
            let pool = empty_pool().await;
            let evm = crate::test_utils::LocalEvm::with_chain_id(CHAIN_ID)
                .await
                .unwrap();
            let provider = alloy::providers::ProviderBuilder::new()
                .connect(&evm.endpoint)
                .await
                .unwrap();
            let underlying: UnderlyingSymbol = "AAPL".parse().unwrap();

            let error = VaultIdentity::verify(
                &pool,
                &provider,
                Network::Base,
                CHAIN_ID,
                VAULT,
                &underlying,
            )
            .await
            .unwrap_err();

            assert!(matches!(
                error.downcast_ref::<MigrationRefusal>(),
                Some(MigrationRefusal::ListingNotFound {
                    underlying: missing,
                    network: Network::Base,
                }) if missing == &underlying
            ));
        }
    }

    mod recorded_custody_route {
        use super::*;

        #[test]
        fn unobserved_custody_must_be_confirmed_before_migration() {
            let error = corroborate_recorded_custody_route(
                VAULT, None, None, OUTGOING, INCOMING,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::CustodyUnobserved { vault: VAULT }
            ));
        }

        #[test]
        fn an_unrelated_recorded_holder_refuses_the_route() {
            let unrelated =
                address!("00000000000000000000000000000000000000dd");
            let error = corroborate_recorded_custody_route(
                VAULT,
                Some(unrelated),
                None,
                OUTGOING,
                INCOMING,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::CustodyRouteMismatch {
                    recorded,
                    holder: OUTGOING,
                    recipient: INCOMING,
                    ..
                } if recorded == unrelated
            ));
        }

        #[test]
        fn a_recorded_destination_cannot_authorize_another_source_transfer() {
            let route = corroborate_recorded_custody_route(
                VAULT,
                Some(INCOMING),
                Some(OUTGOING),
                OUTGOING,
                INCOMING,
            )
            .unwrap();
            let observed = SourceCustody::Holds(MigratableHoldings {
                chain_id: CHAIN_ID,
                vault: VAULT,
                holder: OUTGOING,
                holdings: vec![FakeCustody::holding(1, 100)],
            });

            let error = corroborate_recorded_route_observation(
                route, &observed, VAULT, OUTGOING, INCOMING,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::RecordedDestinationStillAtSource {
                    vault: VAULT,
                    holder: OUTGOING,
                    recipient: INCOMING,
                }
            ));
        }

        #[test]
        fn a_recorded_destination_allows_an_idempotent_rerun() {
            let route = corroborate_recorded_custody_route(
                VAULT,
                Some(INCOMING),
                Some(OUTGOING),
                OUTGOING,
                INCOMING,
            )
            .unwrap();
            let observed = SourceCustody::AlreadyMigrated { receipts: 1 };

            assert!(
                corroborate_recorded_route_observation(
                    route, &observed, VAULT, OUTGOING, INCOMING,
                )
                .is_ok()
            );
        }

        #[test]
        fn a_recorded_destination_rejects_an_unrelated_claimed_source() {
            let unrelated =
                address!("00000000000000000000000000000000000000dd");

            let error = corroborate_recorded_custody_route(
                VAULT,
                Some(INCOMING),
                Some(OUTGOING),
                unrelated,
                INCOMING,
            )
            .unwrap_err();

            assert!(matches!(
                error,
                MigrationRefusal::CustodyOriginMismatch {
                    vault: VAULT,
                    recorded_origin: Some(OUTGOING),
                    requested,
                } if requested == unrelated
            ));
        }
    }

    /// The bootstrap gate that confirms custody before a migration.
    mod custody_bootstrap {
        use alloy::network::EthereumWallet;
        use alloy::primitives::TxHash;
        use alloy::providers::ProviderBuilder;
        use alloy::signers::local::PrivateKeySigner;
        use alloy::sol_types::SolEvent;
        use sqlx::sqlite::SqlitePoolOptions;

        use super::*;
        use crate::test_utils::LocalEvm;
        use crate::tokenized_asset::{
            AssetKey, TokenSymbol, TokenizedAssetCommand,
        };

        async fn pool_with_migrations() -> Pool<Sqlite> {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            pool
        }

        async fn seed_listing(
            pool: &Pool<Sqlite>,
            vault: Address,
            underlying: &UnderlyingSymbol,
        ) {
            let (store, _projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();
            store
                .send(
                    &AssetKey::new(underlying.clone(), Network::Base),
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new("tTSLA"),
                        network: Network::Base,
                        vault,
                    },
                )
                .await
                .unwrap();
        }

        /// Deposits one receipt at the EVM wallet and mirrors it into the
        /// inventory, returning the signing provider and the receipt's id and
        /// share balance.
        async fn seeded_vault(
            evm: &LocalEvm,
            pool: &Pool<Sqlite>,
        ) -> (impl Provider + Clone + use<>, U256, U256) {
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

            let signer =
                PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
            let provider = ProviderBuilder::new()
                .wallet(EthereumWallet::from(signer))
                .connect(&evm.endpoint)
                .await
                .unwrap();

            let vault = crate::bindings::OffchainAssetReceiptVault::new(
                evm.vault_address,
                &provider,
            );
            let shares = U256::from(25) * U256::from(10).pow(U256::from(18));
            let deposited = vault
                .deposit(
                    shares,
                    evm.wallet_address,
                    U256::from(10).pow(U256::from(18)),
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
            let receipt_id = deposited
                .inner
                .logs()
                .iter()
                .find_map(|log| {
                    crate::bindings::OffchainAssetReceiptVault::Deposit::decode_log(
                        &log.inner,
                    )
                    .ok()
                })
                .expect("deposit must emit a Deposit event")
                .id;

            let store = StoreBuilder::<ReceiptInventory>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            send_receipt_inventory_command(
                &store,
                Network::Base.chain_id(),
                &evm.vault_address,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(receipt_id),
                    balance: Shares::from(shares),
                    block_number: 1,
                    tx_hash: TxHash::ZERO,
                    source: crate::receipt_inventory::ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();

            (provider, receipt_id, shares)
        }

        async fn recorded_holder(
            pool: &Pool<Sqlite>,
            vault: Address,
        ) -> Option<Address> {
            let store = StoreBuilder::<ReceiptInventory>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            load_inventory(&store, Network::Base.chain_id(), &vault)
                .await
                .unwrap()
                .custody()
                .holder()
        }

        fn corroborated_identity<'a>(
            evm: &LocalEvm,
            underlying: &'a UnderlyingSymbol,
        ) -> VaultIdentity<'a> {
            let chain_id = Network::Base.chain_id();

            VaultIdentity::from_observations(
                Network::Base,
                chain_id,
                chain_id,
                evm.vault_address,
                evm.vault_address,
                underlying,
            )
            .unwrap()
        }

        /// The bootstrap only records a holder whose on-chain balances match
        /// the tracked inventory exactly.
        #[tokio::test]
        async fn a_holder_with_matching_balances_is_confirmed() {
            let evm = LocalEvm::with_chain_id(Network::Base.chain_id())
                .await
                .unwrap();
            let pool = pool_with_migrations().await;
            let (provider, _, _) = seeded_vault(&evm, &pool).await;
            let underlying: UnderlyingSymbol = "TSLA".parse().unwrap();
            seed_listing(&pool, evm.vault_address, &underlying).await;
            let identity = VaultIdentity::verify(
                &pool,
                &provider,
                Network::Base,
                Network::Base.chain_id(),
                evm.vault_address,
                &underlying,
            )
            .await
            .unwrap();

            let receipts = confirm_custody_holder(
                &pool,
                provider,
                identity,
                evm.wallet_address,
            )
            .await
            .unwrap();

            assert_eq!(receipts, 1);
            assert_eq!(
                recorded_holder(&pool, evm.vault_address).await,
                Some(evm.wallet_address),
                "the verified holder must be on record"
            );
        }

        /// A wallet that does not hold the tracked receipts is refused and
        /// nothing is recorded — a mistyped or wrong-workspace wallet cannot
        /// become the trusted custody holder.
        #[tokio::test]
        async fn a_holder_without_the_receipts_is_refused() {
            let evm = LocalEvm::with_chain_id(Network::Base.chain_id())
                .await
                .unwrap();
            let pool = pool_with_migrations().await;
            let (provider, _, _) = seeded_vault(&evm, &pool).await;
            let underlying: UnderlyingSymbol = "TSLA".parse().unwrap();

            let error = confirm_custody_holder_for_identity(
                &pool,
                provider,
                corroborated_identity(&evm, &underlying),
                Address::random(),
            )
            .await
            .unwrap_err();

            assert!(
                matches!(
                    error.downcast_ref::<MigrationRefusal>(),
                    Some(MigrationRefusal::HolderMismatch(_))
                ),
                "the refusal must be a holder mismatch, got: {error}"
            );
            assert_eq!(
                recorded_holder(&pool, evm.vault_address).await,
                None,
                "no custody may be recorded from a failed verification"
            );
        }
    }
}
