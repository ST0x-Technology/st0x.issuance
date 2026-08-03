//! Moving receipt custody from a retiring signing wallet to its replacement.
//!
//! The issuer burns against receipts held by its own signing address
//! (`balanceOf(bot_wallet, receipt_id)` in [`super::backfill`] and
//! [`super::reconcile`]), so rotating the signing backend strands every receipt
//! at the old address: the new wallet holds shares it cannot redeem because the
//! matching receipt sits elsewhere. Custody has to follow the rotation.
//!
//! This is a one-shot capability for the Turnkey cutover and is expected to be
//! removed once every vault has migrated.
//!
//! **The issuer service must be stopped before this runs.** Startup
//! reconciliation reads `balanceOf(bot_wallet)` for every tracked receipt
//! ([`super::reconcile`], reached from `run_startup_reconciliation`). Once
//! custody has moved but the service is still configured with the outgoing
//! signer, every one of those reads returns zero, which reconciles as a
//! depletion and drops the receipts from the aggregate outright. Freezing the
//! underlying does not prevent this: a freeze rejects new mints, it does not
//! stop the service or serialize against it. The order is stop the service,
//! migrate, swap the signer configuration, start again.

use alloy::eips::BlockId;
use alloy::network::{
    Ethereum, EthereumWallet, NetworkWallet, TransactionBuilder,
};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::{PendingTransactionError, Provider};
use alloy::rpc::types::TransactionRequest;
use async_trait::async_trait;
use event_sorcery::StoreBuilder;
use itertools::izip;
use sqlx::{Pool, Sqlite};
use std::fmt;
use std::num::NonZeroUsize;
use std::time::Duration;
use tracing::{debug, info};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand, Shares,
    SharesOverflow, load_inventory, send_receipt_inventory_command,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::fireblocks::{
    FireblocksConfig, FireblocksVaultError, FireblocksVaultService,
};
use crate::mint::find_stuck as find_stuck_mints;
use crate::redemption::view::find_stuck as find_stuck_redemptions;
use crate::redemption::{IssuerRedemptionRequestId, RedemptionEvent};
use crate::tokenized_asset::UnderlyingSymbol;

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
pub(crate) const fn require_quiescent(
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
pub(crate) trait ReceiptCustody {
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
pub(crate) enum TransferPermission {
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
pub(crate) struct TransferPermit {
    vault: Address,
    from: Address,
    to: Address,
}

impl TransferPermit {
    const fn granted(vault: Address, from: Address, to: Address) -> Self {
        Self { vault, from, to }
    }

    pub(crate) const fn from(&self) -> Address {
        self.from
    }

    pub(crate) const fn to(&self) -> Address {
        self.to
    }

    pub(crate) const fn vault(&self) -> Address {
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
pub(crate) struct MigratableHoldings {
    chain_id: u64,
    vault: Address,
    holder: Address,
    holdings: Vec<ReceiptHolding>,
    /// How many custody migrations the vault had recorded when these holdings
    /// were reconciled — the salt that gives a deliberate re-migration a
    /// fresh transfer identity while a retry keeps the original.
    migration_ordinal: u32,
}

/// A single receipt identifier and the balance held against it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReceiptHolding {
    pub(crate) receipt_id: ReceiptId,
    pub(crate) balance: Shares,
}

/// What the outgoing wallet turned out to be holding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SourceCustody {
    /// The outgoing wallet still holds the tracked receipts, so there is a
    /// migration to perform.
    Holds(MigratableHoldings),
    /// Every tracked identifier has already reached the incoming wallet (it
    /// holds at least each tracked balance, since a fresh run cannot know
    /// what the recipient held before the migration): a completed migration
    /// seen a second time.
    AlreadyMigrated { receipts: usize },
}

impl MigratableHoldings {
    pub(crate) const fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub(crate) const fn migration_ordinal(&self) -> u32 {
        self.migration_ordinal
    }

    pub(crate) const fn vault(&self) -> Address {
        self.vault
    }

    pub(crate) const fn holder(&self) -> Address {
        self.holder
    }

    pub(crate) fn holdings(&self) -> &[ReceiptHolding] {
        &self.holdings
    }

    pub(crate) fn receipt_ids(&self) -> Vec<ReceiptId> {
        self.holdings.iter().map(|held| held.receipt_id).collect()
    }

    /// Identifiers and amounts as the parallel arrays
    /// `safeBatchTransferFrom(from, to, ids, amounts, data)` expects. Built
    /// together so the two can never fall out of step.
    pub(crate) fn batch_arguments(&self) -> (Vec<U256>, Vec<U256>) {
        self.holdings
            .iter()
            .map(|holding| {
                (holding.receipt_id.inner(), holding.balance.inner())
            })
            .unzip()
    }

    /// Total balance across every holding, used to assert the recipient gained
    /// exactly what the source gave up.
    pub(crate) fn total(&self) -> Result<Shares, SharesOverflow> {
        total_of(self.holdings.iter().map(|held| held.balance))
    }
}

/// Why a migration must not proceed.
///
/// Every variant is a stop, not a warning: this moves the backing that redeemed
/// tokens depend on, so an unclear picture is a reason to halt rather than to
/// proceed on a best guess.
#[derive(Debug, thiserror::Error)]
pub(crate) enum MigrationRefusal {
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
        "chain {chain_id} has never seen recipient {recipient}: it has sent no \
         transaction and holds no native balance. That is what a mistyped \
         address looks like, and receipts moved to one cannot be recovered by \
         anyone. Check the address, and fund the incoming wallet for gas \
         before migrating."
    )]
    RecipientUnknownToChain { recipient: Address, chain_id: u64 },

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

    #[error(transparent)]
    Fireblocks(#[from] Box<FireblocksVaultError>),

    #[error("custody transfer {tx_hash} has no receipt after confirmation")]
    MissingReceipt { tx_hash: B256 },

    #[error(
        "vault {vault} produced an empty transfer batch; nothing was \
         submitted"
    )]
    NothingToTransfer { vault: Address },

    #[error(
        "vault {vault} certification lapsed partway through the chunked \
         migration; re-certify, then re-run — completed chunks resume"
    )]
    CertificationLapsed { vault: Address },

    #[error(
        "vault {vault} owner froze a transfer party until {until} partway \
         through the chunked migration; re-run once the freeze lifts — \
         completed chunks resume"
    )]
    OwnerFroze { vault: Address, until: U256 },
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

/// A destination the chain itself has already seen.
///
/// Existence implies corroboration: [`CorroboratedRecipient::verify`] is the
/// only constructor, so no caller can reach [`migrate_vault_receipts`] with an
/// address whose only evidence is that somebody typed it correctly. An ERC-1155
/// transfer is final and has no counterparty to ask for it back, so a mistyped
/// destination is not a recoverable mistake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CorroboratedRecipient(Address);

impl CorroboratedRecipient {
    /// Confirms the chain has independent evidence that `recipient` exists.
    ///
    /// An address that has never sent a transaction and holds no native balance
    /// has no on-chain existence at all — which is precisely what a
    /// fat-fingered address looks like, since the odds of a typo landing on a
    /// used address are negligible. Both legitimate destinations clear it: the
    /// incoming signing wallet has to be funded for gas before it can run the
    /// service, and the outgoing wallet a rollback returns custody to has been
    /// signing for months.
    ///
    /// The error type is erased to `anyhow` for the same reason as
    /// [`migrate_vault_receipts`]: [`MigrationRefusal`] stays crate-internal.
    ///
    /// # Errors
    ///
    /// Returns an error for the zero address, and when the chain has no record
    /// of the address at all.
    pub async fn verify<P: Provider>(
        provider: &P,
        recipient: Address,
    ) -> anyhow::Result<Self> {
        if recipient.is_zero() {
            return Err(
                MigrationRefusal::RecipientIsZeroAddress { recipient }.into()
            );
        }

        let chain_id =
            provider.get_chain_id().await.map_err(ReceiptCustodyError::from)?;

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

        Ok(Self(recipient))
    }

    pub(crate) const fn address(self) -> Address {
        self.0
    }
}

impl std::fmt::Display for CorroboratedRecipient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Migrates one vault's receipt custody to `recipient`, end to end.
///
/// Public so the operator CLI and the cutover end-to-end test drive the same
/// code: a test exercising a hand-rolled transfer would prove nothing about
/// what the operator actually runs.
///
/// `provider` must be signing as `holder`, the wallet whose custody moves —
/// ERC-1155 only lets the holder or an approved operator move a balance.
///
/// Re-running after a successful migration is safe: the outgoing wallet is
/// found empty with the incoming wallet holding the tracked balances, which
/// reports [`MigrationOutcome::AlreadyMigrated`] rather than submitting a
/// second transfer.
///
/// The error type is erased to `anyhow` at this boundary, matching the other
/// operator entry point in this crate ([`crate::run_issuer_cli`]); the typed
/// [`MigrationRefusal`] hierarchy is preserved for every caller inside the
/// crate.
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
    holder: Address,
    recipient: CorroboratedRecipient,
) -> anyhow::Result<MigrationOutcome> {
    let custody =
        OnchainReceiptCustody::resolve(provider, identity.vault).await?;

    execute_migration(pool, &custody, identity, holder, recipient).await
}

/// The vault a custody operation addresses: its chain, its address, and the
/// asset it belongs to. The asset scopes the in-flight quiescence gates —
/// stuck work only ever resumes against its own vault.
#[derive(Debug, Clone, Copy)]
pub struct VaultIdentity<'a> {
    pub chain_id: u64,
    pub vault: Address,
    pub underlying: &'a UnderlyingSymbol,
}

/// [`migrate_vault_receipts`] with the transfer submitted through the
/// Fireblocks API instead of a locally held key — the production forward leg.
///
/// Every gate is identical to the in-binary path: same quiescence checks, same
/// inventory/chain agreement, same certification and owner-freeze re-read,
/// same per-identifier post-condition deltas. Only the submission mechanism
/// differs, behind the same [`ReceiptCustody`] seam the tests exercise.
///
/// # Errors
///
/// As [`migrate_vault_receipts`], plus Fireblocks-side failures: authentication,
/// a non-whitelisted Receipt contract, TAP policy rejection, or a transaction
/// that reaches a terminal non-completed status.
pub async fn migrate_vault_receipts_via_fireblocks<
    P: Provider + Clone + Send + Sync,
>(
    pool: &Pool<Sqlite>,
    provider: P,
    fireblocks: &FireblocksConfig,
    identity: VaultIdentity<'_>,
    holder: Address,
    recipient: CorroboratedRecipient,
) -> anyhow::Result<MigrationOutcome> {
    let custody = FireblocksReceiptCustody::resolve(
        provider,
        fireblocks,
        identity.chain_id,
        identity.vault,
    )
    .await?;

    execute_migration(pool, &custody, identity, holder, recipient).await
}

async fn execute_migration(
    pool: &Pool<Sqlite>,
    custody: &(impl ReceiptCustody + Sync),
    identity: VaultIdentity<'_>,
    holder: Address,
    recipient: CorroboratedRecipient,
) -> anyhow::Result<MigrationOutcome> {
    let VaultIdentity { chain_id, vault, underlying } = identity;
    let recipient = recipient.address();

    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;

    let inventory = load_inventory(&store, chain_id, &vault).await?;
    let tracked = quiescent_tracked_holdings(
        pool, &inventory, chain_id, vault, underlying,
    )
    .await?;

    match reconcile_holdings(
        custody,
        chain_id,
        vault,
        holder,
        recipient,
        &tracked,
        inventory.migrations_recorded(),
    )
    .await?
    {
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

            // An already-completed move is still recorded (idempotently): the
            // production forward transfer is signed by the custodian itself,
            // outside this binary, so this observation is the only way the
            // event a rollback derives its destination from ever lands.
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
            // is what a later rollback reads its destination from, instead of
            // being handed an address.
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
    let VaultIdentity { chain_id, vault, underlying } = identity;
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

/// Proof that the Turnkey connection can sign the rollback, produced before
/// anything moves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RollbackSigningProof {
    /// Where the signed (never broadcast) rollback would send custody.
    pub destination: Address,
    /// How many tracked receipts the rollback transaction covers.
    pub receipts: usize,
    /// Native balance of the Turnkey wallet — it needs gas to actually
    /// broadcast a rollback and to operate the service afterwards.
    pub turnkey_gas: U256,
    /// Native balance of the wallet custody currently sits with.
    pub holder_gas: U256,
}

/// Signs the exact rollback-shaped transaction with Turnkey — **without
/// broadcasting it** — proving the connection end to end.
///
/// The transaction is a `safeBatchTransferFrom` of every tracked receipt from
/// the Turnkey wallet back to the current holder, the real shape a rollback
/// would submit.
///
/// This is the gate that keeps the forward move from being a one-way door: the
/// custodian signs the forward transfer outside this binary, and if the
/// Turnkey credentials, organization, address, or signing policy turn out
/// broken only *after* custody has moved, there is no way back and no service
/// that can run. A successful sign here proves the API credentials, the
/// organization, the address, and the policy against the real transaction
/// shape; the signer itself verifies the recovered signature matches the
/// Turnkey address before returning.
///
/// Gas and fee fields are fixed rather than estimated: estimating a transfer
/// of receipts the Turnkey wallet does not hold yet would revert, and the
/// signature's validity does not depend on them.
///
/// `destination` is the Fireblocks wallet the rollback would return custody
/// to, derived by the caller from the Fireblocks API — never typed.
///
/// # Errors
///
/// Returns an error if the store cannot be opened, the vault has no tracked
/// receipts, or Turnkey refuses or mis-signs the transaction.
pub async fn verify_rollback_signing<P: Provider>(
    pool: &Pool<Sqlite>,
    provider: P,
    wallet: &EthereumWallet,
    identity: VaultIdentity<'_>,
    destination: Address,
) -> anyhow::Result<RollbackSigningProof> {
    let turnkey = NetworkWallet::<Ethereum>::default_signer_address(wallet);
    let VaultIdentity { chain_id, vault, underlying } = identity;
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;

    // The same loader the migration itself uses: the signed shape is only
    // "exact" if it covers the identical holdings a real rollback would move,
    // gated by the identical quiescence checks.
    let tracked = quiescent_tracked_holdings(
        pool, &inventory, chain_id, vault, underlying,
    )
    .await?;

    let vault_contract = OffchainAssetReceiptVault::new(vault, &provider);
    let receipt_contract = Address::from(
        vault_contract
            .receipt()
            .call()
            .await
            .map_err(|error| ReceiptCustodyError::Contract(Box::new(error)))?
            .0,
    );

    let receipt = Receipt::new(receipt_contract, &provider);
    let ids: Vec<U256> =
        tracked.iter().map(|held| held.receipt_id.inner()).collect();
    let amounts: Vec<U256> =
        tracked.iter().map(|held| held.balance.inner()).collect();
    let calldata = receipt
        .safeBatchTransferFrom(turnkey, destination, ids, amounts, Bytes::new())
        .calldata()
        .clone();

    let nonce = provider
        .get_transaction_count(turnkey)
        .await
        .map_err(ReceiptCustodyError::from)?;

    let request = TransactionRequest::default()
        .with_from(turnkey)
        .with_to(receipt_contract)
        .with_input(calldata)
        .with_chain_id(chain_id)
        .with_nonce(nonce)
        .with_gas_limit(ROLLBACK_PROOF_GAS_LIMIT)
        .with_max_fee_per_gas(ROLLBACK_PROOF_MAX_FEE_PER_GAS)
        .with_max_priority_fee_per_gas(ROLLBACK_PROOF_MAX_PRIORITY_FEE);

    // Signing only: the transaction is built and signed but never submitted.
    // The Turnkey signer verifies the recovered signature matches its address
    // before returning, so success is proof of control, not just of an HTTP
    // 200.
    request.build(wallet).await?;

    let turnkey_gas = provider
        .get_balance(turnkey)
        .await
        .map_err(ReceiptCustodyError::from)?;
    let holder_gas = provider
        .get_balance(destination)
        .await
        .map_err(ReceiptCustodyError::from)?;

    Ok(RollbackSigningProof {
        destination,
        receipts: tracked.len(),
        turnkey_gas,
        holder_gas,
    })
}

/// Deliberately generous: the signature's validity does not depend on these,
/// and the transaction is never broadcast.
const ROLLBACK_PROOF_GAS_LIMIT: u64 = 1_000_000;
const ROLLBACK_PROOF_MAX_FEE_PER_GAS: u128 = 100_000_000_000;
const ROLLBACK_PROOF_MAX_PRIORITY_FEE: u128 = 1_000_000_000;

/// The minimum native balance the Turnkey wallet must hold before a forward
/// move becomes acceptable.
///
/// A bare non-zero check lets one wei pass while leaving a real rollback
/// unbroadcastable — a one-way door discovered only when the way back is
/// needed. Sized as the proof's gas limit at its priority fee (0.001 ether):
/// enough to broadcast a worst-case rollback batch under congested Base fees
/// with a wide margin, without demanding the proof's deliberately absurd
/// `max_fee` ceiling, which is two orders of magnitude above any real
/// broadcast cost.
pub(crate) fn rollback_gas_reserve() -> U256 {
    U256::from(ROLLBACK_PROOF_GAS_LIMIT)
        * U256::from(ROLLBACK_PROOF_MAX_PRIORITY_FEE)
}

/// The wallet a rollback returns custody to, read from the recorded migration.
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
             migration to roll back; record the forward move first (re-run \
             migrate-receipts once custody has actually moved)"
        )
    })
}

/// The recorded custody holder for this vault, if any.
///
/// The migrate CLI uses this to decide direction when the incoming wallet's
/// credentials are what is configured: a recorded holder that is not the
/// incoming wallet means the forward move happened (or must be verified)
/// out-of-band; a recorded holder that *is* the incoming wallet means the only
/// move left is a rollback.
///
/// # Errors
///
/// Returns an error if the store cannot be opened.
pub async fn recorded_custody_holder(
    pool: &Pool<Sqlite>,
    chain_id: u64,
    vault: Address,
) -> anyhow::Result<Option<Address>> {
    let store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    let inventory = load_inventory(&store, chain_id, &vault).await?;

    Ok(inventory.custody().holder())
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
pub(crate) async fn reconcile_holdings(
    custody: &(impl ReceiptCustody + Sync),
    chain_id: u64,
    vault: Address,
    holder: Address,
    recipient: Address,
    tracked: &[ReceiptHolding],
    migration_ordinal: u32,
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

    // Classification is per identifier so a failure between transfer chunks
    // resumes: a chunked transfer can leave some identifiers already with the
    // recipient while the rest still sit with the holder, and an all-or-
    // nothing agreement check would refuse that state as divergence forever.
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

    // Canonical order, independent of the inventory's backing map: chunk
    // membership and every chunk's deterministic externalTxId are derived
    // positionally from this sequence, so a rerun after a crash must
    // enumerate the same remaining identifiers in the same order to resume
    // its in-flight Fireblocks transactions instead of duplicating them.
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
        migration_ordinal,
    }))
}

/// Moves one vault's receipts to the incoming wallet.
///
/// Re-reads the transfer permission immediately before submitting, because
/// certification is maintained outside this service and can lapse between an
/// earlier preflight and the transaction landing.
pub(crate) async fn migrate_vault_custody(
    custody: &(impl ReceiptCustody + Sync),
    holdings: &MigratableHoldings,
    recipient: Address,
) -> Result<MigrationOutcome, MigrationRefusal> {
    let vault = holdings.vault();
    if holdings.holdings().is_empty() {
        return Err(ReceiptCustodyError::NothingToTransfer { vault }.into());
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

    let receipts = holdings.holdings().len();
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
/// the single entry point for the read+check pair, so every submission path
/// (custody transfers, chunked Fireblocks calls, the legacy-receipt sweeps)
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
pub(crate) async fn ensure_holder_quiescent<HolderProvider: Provider>(
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

        // A previous rollback may have been broadcast just before this CLI
        // crashed. While its transaction remains pending, re-submitting would
        // create a second attempt without knowing the first one's outcome.
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

/// Receipt custody whose transfer is submitted through the Fireblocks API —
/// the production forward leg, where the holder's key lives with the custodian
/// and this binary signs nothing.
///
/// Balance reads and the permission gates delegate to the on-chain
/// implementation; only [`ReceiptCustody::transfer_custody`] differs. The
/// `externalTxId` is deterministic over the batch content and the number of
/// custody migrations already recorded for the vault, so a crashed or retried
/// run resumes the original Fireblocks transaction instead of submitting a
/// second transfer, while a deliberate later re-migration (after a rollback)
/// carries a new ordinal and gets a fresh identity.
pub(crate) struct FireblocksReceiptCustody<P> {
    onchain: OnchainReceiptCustody<P>,
    client: FireblocksVaultService<P>,
    receipt_contract: Address,
    chain_id: u64,
    max_receipts_per_transfer: NonZeroUsize,
}

/// Upper bound on receipts per CONTRACT_CALL submitted through Fireblocks.
///
/// Fireblocks' transaction engine assigns its own gas limit to a
/// CONTRACT_CALL, and on large `safeBatchTransferFrom` batches that limit
/// exceeds Base's block gas limit, failing the transaction before it can
/// broadcast. Chunking keeps every submitted transfer small enough for a
/// sane engine gas limit; [`reconcile_holdings`]' per-identifier
/// classification makes a failure between chunks resumable.
///
/// The bound is empirical, not a documented Fireblocks limit: 240 receipts
/// is the only observed failure and batches up to 14 are proven to pass, so
/// the cap sits exactly at the proven bound rather than guessing anything
/// between — correctness does not depend on its value, only how many
/// transactions a large vault takes. Never change it between a failed run
/// and its re-run:
/// chunk boundaries derive from this value, so a resume under a different
/// cap computes different externalTxIds and submits fresh transactions
/// instead of resuming any still-pending ones (safe on-chain — an
/// overlapping duplicate reverts on insufficient balance — but noisy and
/// operator-confusing). Change it only once no Fireblocks transaction for
/// the vault is pending.
const MAX_RECEIPTS_PER_ENGINE_TRANSFER: NonZeroUsize =
    NonZeroUsize::MIN.saturating_add(13);

impl<P: Provider + Clone> FireblocksReceiptCustody<P> {
    pub(crate) async fn resolve(
        provider: P,
        config: &FireblocksConfig,
        chain_id: u64,
        vault: Address,
    ) -> Result<Self, ReceiptCustodyError> {
        let receipt_contract = OffchainAssetReceiptVault::new(vault, &provider)
            .receipt()
            .call()
            .await?;

        let client =
            FireblocksVaultService::new(config, provider.clone(), chain_id)
                .map_err(Box::new)?;

        Ok(Self {
            onchain: OnchainReceiptCustody::new(
                provider,
                vault,
                receipt_contract,
            ),
            client,
            receipt_contract,
            chain_id,
            max_receipts_per_transfer: MAX_RECEIPTS_PER_ENGINE_TRANSFER,
        })
    }
}

#[async_trait]
impl<P: Provider + Clone + Send + Sync> ReceiptCustody
    for FireblocksReceiptCustody<P>
{
    async fn held_balances(
        &self,
        vault: Address,
        holder: Address,
        receipt_ids: &[ReceiptId],
    ) -> Result<Vec<Shares>, ReceiptCustodyError> {
        self.onchain.held_balances(vault, holder, receipt_ids).await
    }

    async fn transfer_permission(
        &self,
        vault: Address,
        from: Address,
        to: Address,
    ) -> Result<TransferPermission, ReceiptCustodyError> {
        self.onchain.transfer_permission(vault, from, to).await
    }

    async fn transfer_custody(
        &self,
        permit: &TransferPermit,
        holdings: &MigratableHoldings,
    ) -> Result<B256, ReceiptCustodyError> {
        self.onchain.check_vault(permit.vault())?;
        ensure_permit_covers(permit, holdings)?;

        let receipt =
            Receipt::new(self.receipt_contract, &self.onchain.provider);
        let (ids, amounts) = holdings.batch_arguments();
        let note = format!(
            "receipt custody migration: vault {} -> {}",
            permit.vault(),
            permit.to()
        );
        let chunk_size = self.max_receipts_per_transfer.get();
        let chunk_count = ids.len().div_ceil(chunk_size);

        info!(target: "receipt_inventory",
            vault = %permit.vault(),
            receipt_contract = %self.receipt_contract,
            receipts = ids.len(),
            chunks = chunk_count,
            "submitting custody transfer to Fireblocks"
        );

        // Each chunk is a CONTRACT_CALL of its own, kept under the
        // per-transfer cap so Fireblocks' engine never assigns it a gas limit
        // above the network's block gas limit (which fails the transaction
        // before it can broadcast). A chunk is revert-checked before the next
        // one goes out; a failure between chunks is resumable because
        // `reconcile_holdings` classifies custody per identifier on rerun.
        let mut last_landed: Option<B256> = None;
        for (chunk_index, (chunk_ids, chunk_amounts)) in
            izip!(ids.chunks(chunk_size), amounts.chunks(chunk_size))
                .enumerate()
        {
            // The retiring Fireblocks wallet may be used outside this process.
            // Re-read both nonce views immediately before EVERY chunk: an
            // interleaved pending transaction means this migration no longer
            // owns a quiescent nonce domain and must stop before asking
            // Fireblocks for another CONTRACT_CALL. Exact retries remain
            // resumable through the deterministic externalTxId below.
            ensure_holder_quiescent(&self.onchain.provider, permit.from())
                .await?;

            // Certification is maintained outside this service and can lapse
            // while earlier chunks confirm; re-read before every chunk so a
            // transfer the vault would refuse is never submitted on stale
            // permission. The first chunk's permission was just checked by
            // the caller, but the read is cheap and uniformity beats a
            // special case.
            match self
                .onchain
                .transfer_permission(permit.vault(), permit.from(), permit.to())
                .await?
            {
                TransferPermission::Permitted(_) => {}
                TransferPermission::CertificationExpired => {
                    return Err(ReceiptCustodyError::CertificationLapsed {
                        vault: permit.vault(),
                    });
                }
                TransferPermission::OwnerFrozen { until } => {
                    return Err(ReceiptCustodyError::OwnerFroze {
                        vault: permit.vault(),
                        until,
                    });
                }
            }

            // Deterministic over this chunk's own content: a crashed or
            // retried run resumes the chunk's original Fireblocks
            // transaction instead of submitting a second transfer. Submission
            // walks to a fresh `-retry-N` id when a previous attempt failed
            // terminally, since Fireblocks spends an externalTxId forever.
            let external_tx_id = migration_external_tx_id(
                self.chain_id,
                permit.vault(),
                permit.to(),
                chunk_ids,
                chunk_amounts,
                holdings.migration_ordinal(),
            );
            let calldata = receipt
                .safeBatchTransferFrom(
                    permit.from(),
                    permit.to(),
                    chunk_ids.to_vec(),
                    chunk_amounts.to_vec(),
                    Bytes::new(),
                )
                .calldata()
                .clone();

            // Logged before the await: completion can block through console
            // approvals for minutes, and if this process dies or times out
            // the operator needs the id to look the transaction up.
            debug!(target: "receipt_inventory",
                vault = %permit.vault(),
                %external_tx_id,
                chunk = chunk_index + 1,
                chunks = chunk_count,
                chunk_receipts = chunk_ids.len(),
                "submitting custody transfer chunk to Fireblocks"
            );
            let tx_hash = self
                .client
                .submit_contract_call_to_completion(
                    self.receipt_contract,
                    &calldata,
                    &note,
                    &external_tx_id,
                )
                .await
                .map_err(Box::new)?;

            // Fireblocks can report `Completed` while the EVM transaction
            // itself reverted; a reverted transfer moved nothing, so it is a
            // definitive failure rather than a hash to report as success —
            // and no further chunk goes out on top of it.
            let confirmed = self
                .onchain
                .provider
                .get_transaction_receipt(tx_hash)
                .await?
                .ok_or(ReceiptCustodyError::MissingReceipt { tx_hash })?;

            if !confirmed.status() {
                return Err(ReceiptCustodyError::Reverted {
                    vault: permit.vault(),
                    tx_hash,
                });
            }

            last_landed = Some(tx_hash);
        }

        last_landed.ok_or(ReceiptCustodyError::NothingToTransfer {
            vault: permit.vault(),
        })
    }
}

/// Deterministic `externalTxId` for a custody migration batch.
///
/// Stable over the batch content — chain, vault, destination, identifiers,
/// amounts — so a retry of the same attempt deduplicates against the original
/// Fireblocks transaction (across restarts, crashes, and midnight), and
/// salted with the vault's migration ordinal so a deliberate re-migration
/// (the rehearsal's forward leg happens again at the real cutover, after the
/// rollback restored identical balances and recorded another migration) is a
/// new transaction rather than a stale dedup hit.
fn migration_external_tx_id(
    chain_id: u64,
    vault: Address,
    to: Address,
    ids: &[U256],
    amounts: &[U256],
    migration_ordinal: u32,
) -> String {
    let mut preimage = Vec::with_capacity(8 + 44 + (ids.len() * 64));
    preimage.extend_from_slice(&chain_id.to_be_bytes());
    preimage.extend_from_slice(vault.as_slice());
    preimage.extend_from_slice(to.as_slice());
    preimage.extend_from_slice(&migration_ordinal.to_be_bytes());
    for (id, amount) in izip!(ids, amounts) {
        preimage.extend_from_slice(&id.to_be_bytes::<32>());
        preimage.extend_from_slice(&amount.to_be_bytes::<32>());
    }

    let digest = alloy::primitives::keccak256(&preimage);

    format!("receipt-migration-{}", alloy::hex::encode(&digest[..16]))
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
            custody, CHAIN_ID, VAULT, OUTGOING, INCOMING, tracked, 0,
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

    /// A failure between transfer chunks leaves some identifiers already with
    /// the recipient and the rest still with the holder. A rerun must resume
    /// the remainder rather than refuse the moved identifiers as divergence.
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

        use super::*;
        use crate::test_utils::LocalEvm;

        #[tokio::test]
        async fn an_address_the_chain_has_never_seen_is_refused() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let typo = Address::random();
            let error = CorroboratedRecipient::verify(&provider, typo)
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

            let error = CorroboratedRecipient::verify(&provider, Address::ZERO)
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

        /// The legitimate destination in both directions clears the gate: the
        /// incoming wallet must be funded for gas before it can run the
        /// service, and the wallet a rollback returns custody to has been
        /// signing all along.
        #[tokio::test]
        async fn a_funded_wallet_is_corroborated() {
            let evm = LocalEvm::new().await.unwrap();
            let provider =
                ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

            let corroborated =
                CorroboratedRecipient::verify(&provider, evm.wallet_address)
                    .await
                    .unwrap();

            assert_eq!(corroborated.address(), evm.wallet_address);
        }
    }

    /// The idempotency identity for a Fireblocks-submitted migration must be
    /// stable over the batch content (so an in-flight retry deduplicates) and
    /// sensitive to it (so a different batch is a different transaction).
    mod migration_external_id {
        use super::*;

        #[test]
        fn identical_batches_share_an_identity() {
            let ids = vec![U256::from(1), U256::from(2)];
            let amounts = vec![U256::from(10), U256::from(20)];

            assert_eq!(
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 0),
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 0),
            );
        }

        #[test]
        fn different_batches_get_different_identities() {
            let ids = vec![U256::from(1)];
            let amounts = vec![U256::from(10)];
            let other_amounts = vec![U256::from(11)];

            assert_ne!(
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 0),
                migration_external_tx_id(
                    1,
                    VAULT,
                    INCOMING,
                    &ids,
                    &other_amounts,
                    0
                ),
                "a changed batch must not deduplicate against the old one"
            );
            assert_ne!(
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 0),
                migration_external_tx_id(1, VAULT, OUTGOING, &ids, &amounts, 0),
                "a changed destination must not deduplicate either"
            );
        }

        /// The rehearsal's exact hazard: rollback restores identical balances,
        /// so the real cutover re-submits an identical batch — possibly the
        /// same day. The migration ordinal is what makes it a new Fireblocks
        /// transaction instead of a stale dedup hit, while a retry of the
        /// same attempt (same ordinal) still deduplicates.
        #[test]
        fn a_re_migration_after_rollback_gets_a_fresh_identity() {
            let ids = vec![U256::from(1)];
            let amounts = vec![U256::from(10)];

            assert_ne!(
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 0),
                migration_external_tx_id(1, VAULT, INCOMING, &ids, &amounts, 2),
                "an identical batch at a later migration ordinal must be a \
                 new transaction identity"
            );
        }
    }

    /// The two operator gates that bootstrap and protect custody state:
    /// `confirm_custody_holder` (the only writer of trusted custody before a
    /// migration) and `verify_rollback_signing` (the proof that keeps the
    /// forward move from being a one-way door).
    mod custody_bootstrap {
        use alloy::network::EthereumWallet;
        use alloy::primitives::TxHash;
        use alloy::providers::ProviderBuilder;
        use alloy::signers::local::PrivateKeySigner;
        use alloy::sol_types::SolEvent;
        use sqlx::sqlite::SqlitePoolOptions;

        use super::*;
        use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm};

        async fn pool_with_migrations() -> Pool<Sqlite> {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .unwrap();
            sqlx::migrate!("./migrations").run(&pool).await.unwrap();
            pool
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
                ANVIL_CHAIN_ID,
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
            load_inventory(&store, ANVIL_CHAIN_ID, &vault)
                .await
                .unwrap()
                .custody()
                .holder()
        }

        /// The bootstrap only records a holder whose on-chain balances match
        /// the tracked inventory exactly.
        #[tokio::test]
        async fn a_holder_with_matching_balances_is_confirmed() {
            let evm = LocalEvm::new().await.unwrap();
            let pool = pool_with_migrations().await;
            let (provider, _, _) = seeded_vault(&evm, &pool).await;
            let underlying: UnderlyingSymbol = "TSLA".parse().unwrap();

            let receipts = confirm_custody_holder(
                &pool,
                provider,
                VaultIdentity {
                    chain_id: ANVIL_CHAIN_ID,
                    vault: evm.vault_address,
                    underlying: &underlying,
                },
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
            let evm = LocalEvm::new().await.unwrap();
            let pool = pool_with_migrations().await;
            let (provider, _, _) = seeded_vault(&evm, &pool).await;
            let underlying: UnderlyingSymbol = "TSLA".parse().unwrap();

            let error = confirm_custody_holder(
                &pool,
                provider,
                VaultIdentity {
                    chain_id: ANVIL_CHAIN_ID,
                    vault: evm.vault_address,
                    underlying: &underlying,
                },
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

        /// The rollback proof signs the exact tracked batch without
        /// broadcasting: the proof covers every tracked receipt, names the
        /// derived destination, and reports gas for both wallets.
        #[tokio::test]
        async fn rollback_signing_proof_covers_the_tracked_batch() {
            let evm = LocalEvm::new().await.unwrap();
            let pool = pool_with_migrations().await;
            let (provider, receipt_id, _) = seeded_vault(&evm, &pool).await;
            let underlying: UnderlyingSymbol = "TSLA".parse().unwrap();

            let signer =
                PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
            let wallet = EthereumWallet::from(signer);
            let destination = Address::random();

            let proof = verify_rollback_signing(
                &pool,
                &provider,
                &wallet,
                VaultIdentity {
                    chain_id: ANVIL_CHAIN_ID,
                    vault: evm.vault_address,
                    underlying: &underlying,
                },
                destination,
            )
            .await
            .unwrap();

            assert_eq!(proof.receipts, 1);
            assert_eq!(proof.destination, destination);
            assert!(
                !proof.turnkey_gas.is_zero(),
                "the signing wallet is funded on the local chain"
            );
            assert!(
                proof.holder_gas.is_zero(),
                "a random destination holds no gas yet"
            );

            // Signing proved control without moving anything: the receipt
            // still sits with its holder.
            let vault_contract =
                crate::bindings::OffchainAssetReceiptVault::new(
                    evm.vault_address,
                    &provider,
                );
            let receipt_contract =
                Address::from(vault_contract.receipt().call().await.unwrap().0);
            let receipt = Receipt::new(receipt_contract, &provider);
            assert!(
                !receipt
                    .balanceOf(evm.wallet_address, receipt_id)
                    .call()
                    .await
                    .unwrap()
                    .is_zero(),
                "the proof must not move custody"
            );
        }
    }

    /// The Fireblocks-submitted transfer path, end to end against a real vault
    /// with the Fireblocks API mocked: the custody impl builds the batch
    /// calldata, submits it as a `CONTRACT_CALL`, polls to completion, and
    /// verifies the returned transaction actually landed and did not revert.
    mod fireblocks_transfer {
        use alloy::network::EthereumWallet;
        use alloy::providers::ProviderBuilder;
        use alloy::signers::local::PrivateKeySigner;
        use alloy::sol_types::SolEvent;
        use fireblocks_sdk::{Client, ClientBuilder};
        use httpmock::MockServer;
        use rsa::RsaPrivateKey;
        use rsa::pkcs8::EncodePrivateKey;
        use std::sync::LazyLock;

        use super::*;
        use crate::fireblocks::parse_chain_asset_ids;
        use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm};

        static TEST_RSA_PEM: LazyLock<Vec<u8>> = LazyLock::new(|| {
            let mut rng = rand::thread_rng();
            let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
            key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
                .unwrap()
                .as_bytes()
                .to_vec()
        });

        fn mock_client(server: &MockServer) -> Client {
            ClientBuilder::new("test-api-user", &TEST_RSA_PEM)
                .with_url(&server.base_url())
                .build()
                .unwrap()
        }

        #[tokio::test]
        async fn transfer_custody_returns_the_hash_of_a_landed_transfer() {
            let evm = LocalEvm::new().await.unwrap();
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

            let holder = evm.wallet_address;
            let recipient = Address::random();
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
            let shares = U256::from(40) * U256::from(10).pow(U256::from(18));
            let deposited = vault
                .deposit(
                    shares,
                    holder,
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
            let receipt_contract =
                Address::from(vault.receipt().call().await.unwrap().0);

            // Holdings and permit are established while the holder still owns
            // the receipts, exactly as the engine does before submitting.
            let onchain = OnchainReceiptCustody::resolve(
                provider.clone(),
                evm.vault_address,
            )
            .await
            .unwrap();
            let tracked = [ReceiptHolding {
                receipt_id: ReceiptId::from(receipt_id),
                balance: Shares::from(shares),
            }];
            let SourceCustody::Holds(holdings) = reconcile_holdings(
                &onchain,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                holder,
                recipient,
                &tracked,
                0,
            )
            .await
            .unwrap() else {
                panic!("the holder must still own the receipts")
            };
            let TransferPermission::Permitted(permit) = onchain
                .transfer_permission(evm.vault_address, holder, recipient)
                .await
                .unwrap()
            else {
                panic!("a certified vault must permit the transfer")
            };

            // The "custodian" executes the batch on-chain; the mocked
            // Fireblocks API then reports that transaction as the completed
            // CONTRACT_CALL, exactly as production does once approvals clear.
            let receipt_instance = Receipt::new(receipt_contract, &provider);
            let landed = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_id],
                    vec![shares],
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
            let landed_hash = landed.transaction_hash;

            let server = MockServer::start();
            server.mock(|when, then| {
                when.method("GET").path("/contracts");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!([
                        {
                            "id": "contract-wallet-123",
                            "name": "Receipt",
                            "assets": [
                                {
                                    "id": "TESTCHAIN_ETH",
                                    "address": receipt_contract
                                        .to_string()
                                        .to_lowercase()
                                }
                            ]
                        }
                    ]));
            });
            let create_mock = server.mock(|when, then| {
                when.method("POST").path("/transactions");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({ "id": "fb-tx-1" }));
            });
            server.mock(|when, then| {
                when.method("GET").path("/transactions/fb-tx-1");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({
                        "id": "fb-tx-1",
                        "status": "COMPLETED",
                        "txHash": format!("{landed_hash:#x}"),
                    }));
            });

            let custody = FireblocksReceiptCustody {
                onchain,
                client: FireblocksVaultService::for_tests(
                    mock_client(&server),
                    provider.clone(),
                    ANVIL_CHAIN_ID,
                    parse_chain_asset_ids(&format!(
                        "{ANVIL_CHAIN_ID}:TESTCHAIN_ETH"
                    ))
                    .unwrap(),
                ),
                receipt_contract,
                chain_id: ANVIL_CHAIN_ID,
                max_receipts_per_transfer: MAX_RECEIPTS_PER_ENGINE_TRANSFER,
            };

            let returned =
                custody.transfer_custody(&permit, &holdings).await.unwrap();

            assert_eq!(
                returned, landed_hash,
                "the custody impl must report the hash of the transfer that \
                 actually landed"
            );
            assert_eq!(
                create_mock.calls_async().await,
                1,
                "exactly one CONTRACT_CALL must be submitted"
            );
        }

        /// Deposits `count` receipts of `shares` each into the vault,
        /// returning the receipt identifiers the Deposit events reported.
        async fn deposit_receipts<P: Provider>(
            vault: &crate::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance<P>,
            holder: Address,
            shares: U256,
            count: usize,
        ) -> Vec<U256> {
            let mut receipt_ids = Vec::new();
            for _ in 0..count {
                let deposited = vault
                    .deposit(
                        shares,
                        holder,
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
                receipt_ids.push(receipt_id);
            }
            receipt_ids
        }

        /// Mocks one engine CONTRACT_CALL round trip: a create matched on the
        /// chunk's externalTxId returning `fb_id`, and its poll reporting
        /// `COMPLETED` with `tx_hash`. Returns the create mock so the test
        /// can assert the submission happened.
        fn mock_engine_completion<'a>(
            server: &'a MockServer,
            external_tx_id: &str,
            fb_id: &str,
            tx_hash: B256,
        ) -> httpmock::Mock<'a> {
            let poll_body = serde_json::json!({
                "id": fb_id,
                "status": "COMPLETED",
                "txHash": format!("{tx_hash:#x}"),
            });
            let poll_path = format!("/transactions/{fb_id}");
            let create_body = serde_json::json!({ "id": fb_id });
            let external = external_tx_id.to_string();
            server.mock(move |when, then| {
                when.method("GET").path(poll_path.clone());
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(poll_body);
            });
            server.mock(move |when, then| {
                when.method("POST")
                    .path("/transactions")
                    .body_includes(external.clone());
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(create_body);
            })
        }

        /// A batch above the per-transfer cap must be submitted as several
        /// CONTRACT_CALLs, each under its own deterministic externalTxId and
        /// each revert-checked, with the last landed hash reported. One
        /// oversized submission is exactly what Fireblocks' engine rejects
        /// with a gas limit above the network's block gas limit.
        #[traced_test]
        #[tokio::test]
        async fn transfer_custody_chunks_batches_above_the_transfer_cap() {
            let evm = LocalEvm::new().await.unwrap();
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

            let holder = evm.wallet_address;
            let recipient = Address::random();
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
            let shares = U256::from(10) * U256::from(10).pow(U256::from(18));
            let mut receipt_ids =
                deposit_receipts(&vault, holder, shares, 3).await;
            receipt_ids.sort_unstable();
            let receipt_contract =
                Address::from(vault.receipt().call().await.unwrap().0);

            let onchain = OnchainReceiptCustody::resolve(
                provider.clone(),
                evm.vault_address,
            )
            .await
            .unwrap();
            // Deliberately oppose canonical receipt-id order: the chunks and
            // their deterministic externalTxIds must follow reconciliation's
            // sort, not the inventory fixture's iteration order.
            let tracked: Vec<ReceiptHolding> = receipt_ids
                .iter()
                .rev()
                .map(|receipt_id| ReceiptHolding {
                    receipt_id: ReceiptId::from(*receipt_id),
                    balance: Shares::from(shares),
                })
                .collect();
            let SourceCustody::Holds(holdings) = reconcile_holdings(
                &onchain,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                holder,
                recipient,
                &tracked,
                0,
            )
            .await
            .unwrap() else {
                panic!("the holder must still own the receipts")
            };
            let TransferPermission::Permitted(permit) = onchain
                .transfer_permission(evm.vault_address, holder, recipient)
                .await
                .unwrap()
            else {
                panic!("a certified vault must permit the transfer")
            };

            // The "custodian" lands each chunk on-chain up front; the mocked
            // Fireblocks API then reports each chunk's transaction as its
            // completed CONTRACT_CALL, exactly as production does.
            let receipt_instance = Receipt::new(receipt_contract, &provider);
            let first_chunk = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_ids[0], receipt_ids[1]],
                    vec![shares, shares],
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap()
                .transaction_hash;
            let second_chunk = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_ids[2]],
                    vec![shares],
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap()
                .transaction_hash;

            let first_id = migration_external_tx_id(
                ANVIL_CHAIN_ID,
                evm.vault_address,
                recipient,
                &[receipt_ids[0], receipt_ids[1]],
                &[shares, shares],
                0,
            );
            let second_id = migration_external_tx_id(
                ANVIL_CHAIN_ID,
                evm.vault_address,
                recipient,
                &[receipt_ids[2]],
                &[shares],
                0,
            );

            let server = MockServer::start();
            server.mock(|when, then| {
                when.method("GET").path("/contracts");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!([
                        {
                            "id": "contract-wallet-123",
                            "name": "Receipt",
                            "assets": [
                                {
                                    "id": "TESTCHAIN_ETH",
                                    "address": receipt_contract
                                        .to_string()
                                        .to_lowercase()
                                }
                            ]
                        }
                    ]));
            });
            let first_create = mock_engine_completion(
                &server,
                &first_id,
                "fb-chunk-1",
                first_chunk,
            );
            let second_create = mock_engine_completion(
                &server,
                &second_id,
                "fb-chunk-2",
                second_chunk,
            );

            let custody = FireblocksReceiptCustody {
                onchain,
                client: FireblocksVaultService::for_tests(
                    mock_client(&server),
                    provider.clone(),
                    ANVIL_CHAIN_ID,
                    parse_chain_asset_ids(&format!(
                        "{ANVIL_CHAIN_ID}:TESTCHAIN_ETH"
                    ))
                    .unwrap(),
                ),
                receipt_contract,
                chain_id: ANVIL_CHAIN_ID,
                max_receipts_per_transfer: NonZeroUsize::MIN.saturating_add(1),
            };

            let returned =
                custody.transfer_custody(&permit, &holdings).await.unwrap();

            assert_eq!(
                returned, second_chunk,
                "the reported hash must be the last landed chunk"
            );
            first_create.assert_calls(1);
            second_create.assert_calls(1);
            assert!(logs_contain_at!(
                tracing::Level::INFO,
                &["submitting custody transfer to Fireblocks", "chunks=2"]
            ));
            assert!(logs_contain_at!(
                tracing::Level::DEBUG,
                &["submitting custody transfer chunk to Fireblocks", "chunk=1"]
            ));
        }

        /// A chunk whose transaction reverted must stop the loop: no later
        /// chunk may be submitted on top of a definitively failed transfer.
        #[traced_test]
        #[tokio::test]
        async fn a_reverted_chunk_halts_the_remaining_chunks() {
            let evm = LocalEvm::new().await.unwrap();
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

            let holder = evm.wallet_address;
            let recipient = Address::random();
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
            let shares = U256::from(10) * U256::from(10).pow(U256::from(18));
            let mut receipt_ids =
                deposit_receipts(&vault, holder, shares, 3).await;
            receipt_ids.sort_unstable();
            let receipt_contract =
                Address::from(vault.receipt().call().await.unwrap().0);

            let onchain = OnchainReceiptCustody::resolve(
                provider.clone(),
                evm.vault_address,
            )
            .await
            .unwrap();
            let tracked: Vec<ReceiptHolding> = receipt_ids
                .iter()
                .rev()
                .map(|receipt_id| ReceiptHolding {
                    receipt_id: ReceiptId::from(*receipt_id),
                    balance: Shares::from(shares),
                })
                .collect();
            let SourceCustody::Holds(holdings) = reconcile_holdings(
                &onchain,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                holder,
                recipient,
                &tracked,
                0,
            )
            .await
            .unwrap() else {
                panic!("the holder must still own the receipts")
            };
            let TransferPermission::Permitted(permit) = onchain
                .transfer_permission(evm.vault_address, holder, recipient)
                .await
                .unwrap()
            else {
                panic!("a certified vault must permit the transfer")
            };

            // Chunk 1 lands successfully; chunk 2's reported transaction is
            // an oversized transfer that lands reverted (the explicit gas
            // limit skips estimation, which would otherwise refuse to
            // broadcast it).
            let receipt_instance = Receipt::new(receipt_contract, &provider);
            let first_chunk = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_ids[0]],
                    vec![shares],
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap()
                .transaction_hash;
            let reverted = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_ids[1]],
                    vec![shares + U256::from(1)],
                    Bytes::new(),
                )
                .gas(1_000_000)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
            assert!(
                !reverted.status(),
                "the oversized transfer must land reverted"
            );
            let reverted_hash = reverted.transaction_hash;

            let first_id = migration_external_tx_id(
                ANVIL_CHAIN_ID,
                evm.vault_address,
                recipient,
                &[receipt_ids[0]],
                &[shares],
                0,
            );
            let second_id = migration_external_tx_id(
                ANVIL_CHAIN_ID,
                evm.vault_address,
                recipient,
                &[receipt_ids[1]],
                &[shares],
                0,
            );
            let third_id = migration_external_tx_id(
                ANVIL_CHAIN_ID,
                evm.vault_address,
                recipient,
                &[receipt_ids[2]],
                &[shares],
                0,
            );

            let server = MockServer::start();
            server.mock(|when, then| {
                when.method("GET").path("/contracts");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!([
                        {
                            "id": "contract-wallet-123",
                            "name": "Receipt",
                            "assets": [
                                {
                                    "id": "TESTCHAIN_ETH",
                                    "address": receipt_contract
                                        .to_string()
                                        .to_lowercase()
                                }
                            ]
                        }
                    ]));
            });
            mock_engine_completion(
                &server,
                &first_id,
                "fb-halt-1",
                first_chunk,
            );
            mock_engine_completion(
                &server,
                &second_id,
                "fb-halt-2",
                reverted_hash,
            );
            let third_create = mock_engine_completion(
                &server,
                &third_id,
                "fb-halt-3",
                first_chunk,
            );

            let custody = FireblocksReceiptCustody {
                onchain,
                client: FireblocksVaultService::for_tests(
                    mock_client(&server),
                    provider.clone(),
                    ANVIL_CHAIN_ID,
                    parse_chain_asset_ids(&format!(
                        "{ANVIL_CHAIN_ID}:TESTCHAIN_ETH"
                    ))
                    .unwrap(),
                ),
                receipt_contract,
                chain_id: ANVIL_CHAIN_ID,
                max_receipts_per_transfer: NonZeroUsize::MIN,
            };

            let error =
                custody.transfer_custody(&permit, &holdings).await.unwrap_err();

            assert!(
                matches!(
                    error,
                    ReceiptCustodyError::Reverted { tx_hash, .. }
                        if tx_hash == reverted_hash
                ),
                "the reverted chunk must fail closed naming its hash, got \
                 {error}"
            );
            assert_eq!(
                third_create.calls_async().await,
                0,
                "no chunk may be submitted after a reverted one"
            );
            assert!(logs_contain_at!(
                tracing::Level::INFO,
                &["submitting custody transfer to Fireblocks", "chunks=3"]
            ));
            assert!(logs_contain_at!(
                tracing::Level::DEBUG,
                &["submitting custody transfer chunk to Fireblocks", "chunk=2"]
            ));
        }

        /// Fireblocks `Completed` is not proof the EVM transaction succeeded:
        /// a reverted transfer moved nothing and must fail closed rather than
        /// report the hash as a success.
        #[tokio::test]
        async fn transfer_custody_fails_closed_on_a_reverted_transfer() {
            let evm = LocalEvm::new().await.unwrap();
            evm.grant_deposit_role(evm.wallet_address).await.unwrap();
            evm.grant_certify_role(evm.wallet_address).await.unwrap();
            evm.certify_vault(U256::MAX).await.unwrap();

            let holder = evm.wallet_address;
            let recipient = Address::random();
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
            let shares = U256::from(40) * U256::from(10).pow(U256::from(18));
            let deposited = vault
                .deposit(
                    shares,
                    holder,
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
            let receipt_contract =
                Address::from(vault.receipt().call().await.unwrap().0);

            let onchain = OnchainReceiptCustody::resolve(
                provider.clone(),
                evm.vault_address,
            )
            .await
            .unwrap();
            let tracked = [ReceiptHolding {
                receipt_id: ReceiptId::from(receipt_id),
                balance: Shares::from(shares),
            }];
            let SourceCustody::Holds(holdings) = reconcile_holdings(
                &onchain,
                ANVIL_CHAIN_ID,
                evm.vault_address,
                holder,
                recipient,
                &tracked,
                0,
            )
            .await
            .unwrap() else {
                panic!("the holder must still own the receipts")
            };
            let TransferPermission::Permitted(permit) = onchain
                .transfer_permission(evm.vault_address, holder, recipient)
                .await
                .unwrap()
            else {
                panic!("a certified vault must permit the transfer")
            };

            // A transfer of more than the tracked balance lands but reverts;
            // the explicit gas limit skips estimation, which would otherwise
            // refuse to broadcast it. The mocked Fireblocks API then reports
            // exactly this reverted transaction as `Completed`.
            let receipt_instance = Receipt::new(receipt_contract, &provider);
            let reverted = receipt_instance
                .safeBatchTransferFrom(
                    holder,
                    recipient,
                    vec![receipt_id],
                    vec![shares + U256::from(1)],
                    Bytes::new(),
                )
                .gas(1_000_000)
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
            assert!(
                !reverted.status(),
                "the oversized transfer must land reverted"
            );
            let reverted_hash = reverted.transaction_hash;

            let server = MockServer::start();
            server.mock(|when, then| {
                when.method("GET").path("/contracts");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!([
                        {
                            "id": "contract-wallet-123",
                            "name": "Receipt",
                            "assets": [
                                {
                                    "id": "TESTCHAIN_ETH",
                                    "address": receipt_contract
                                        .to_string()
                                        .to_lowercase()
                                }
                            ]
                        }
                    ]));
            });
            server.mock(|when, then| {
                when.method("POST").path("/transactions");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({ "id": "fb-tx-1" }));
            });
            server.mock(|when, then| {
                when.method("GET").path("/transactions/fb-tx-1");
                then.status(200)
                    .header("content-type", "application/json")
                    .json_body(serde_json::json!({
                        "id": "fb-tx-1",
                        "status": "COMPLETED",
                        "txHash": format!("{reverted_hash:#x}"),
                    }));
            });

            let custody = FireblocksReceiptCustody {
                onchain,
                client: FireblocksVaultService::for_tests(
                    mock_client(&server),
                    provider.clone(),
                    ANVIL_CHAIN_ID,
                    parse_chain_asset_ids(&format!(
                        "{ANVIL_CHAIN_ID}:TESTCHAIN_ETH"
                    ))
                    .unwrap(),
                ),
                receipt_contract,
                chain_id: ANVIL_CHAIN_ID,
                max_receipts_per_transfer: MAX_RECEIPTS_PER_ENGINE_TRANSFER,
            };

            let error =
                custody.transfer_custody(&permit, &holdings).await.unwrap_err();

            assert!(
                matches!(
                    error,
                    ReceiptCustodyError::Reverted { tx_hash, .. }
                        if tx_hash == reverted_hash
                ),
                "a reverted transfer must fail closed naming the reverted \
                 hash, got: {error}"
            );
            assert_eq!(
                receipt_instance
                    .balanceOf(holder, receipt_id)
                    .call()
                    .await
                    .unwrap(),
                shares,
                "the reverted transfer must have moved nothing"
            );
        }
    }
}
