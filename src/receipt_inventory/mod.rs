pub(crate) mod backfill;
pub(crate) mod burn_tracking;
mod cmd;
mod event;
pub(crate) mod reconcile;
pub(crate) mod view;

use alloy::primitives::{Address, B256, Bytes, TxHash, U256};
use async_trait::async_trait;
use cqrs_es::AggregateError;
use event_sorcery::{EventSourced, LifecycleError, Nil, SendError, Store};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

use crate::mint::IssuerMintRequestId;
use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
use crate::vault::ReceiptInformation;
use crate::vault::rain_meta;
use backfill::BackfillError;
pub(crate) use backfill::ItnReceiptHandler;
use burn_tracking::plan_burn;
pub(crate) use burn_tracking::{
    BurnPlan, BurnTrackingError, ReceiptWithBalance,
};
pub(crate) use cmd::ReceiptInventoryCommand;
pub(crate) use event::{ReceiptInventoryEvent, ReceiptSource};
use reconcile::ReconcileError;

/// Receipt data recovered from the inventory for mint recovery.
#[derive(Debug, Clone)]
pub(crate) struct RecoveredReceipt {
    pub(crate) receipt_id: U256,
    pub(crate) tx_hash: B256,
    pub(crate) shares: U256,
    pub(crate) block_number: u64,
}

/// Errors that can occur when looking up a receipt.
#[derive(Debug, Error)]
pub(crate) enum ReceiptLookupError {
    #[error(transparent)]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
    #[error(
        "Receipt inventory inconsistent: index contains issuer_request_id {issuer_request_id} \
         -> receipt {receipt_id}, but receipt not found in receipts list"
    )]
    Inconsistent {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: ReceiptId,
    },
}

/// Unique identifier for an ERC-1155 receipt within a vault.
///
/// Each mint operation creates a receipt with a unique ID that tracks
/// the deposited assets. Receipts are burned during redemption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct ReceiptId(U256);

impl ReceiptId {
    pub(crate) const fn inner(&self) -> U256 {
        self.0
    }
}

impl From<U256> for ReceiptId {
    fn from(id: U256) -> Self {
        Self(id)
    }
}

impl std::fmt::Display for ReceiptId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Amount of vault shares (ERC-20 tokens with 18 decimals).
///
/// Shares represent ownership in the vault and are minted/burned
/// proportionally to deposited/withdrawn assets.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize,
)]
pub(crate) struct Shares(U256);

impl Shares {
    pub(crate) const ZERO: Self = Self(U256::ZERO);

    pub(crate) const fn new(amount: U256) -> Self {
        Self(amount)
    }

    pub(crate) const fn inner(&self) -> U256 {
        self.0
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.0.is_zero()
    }

    pub(crate) fn checked_sub(self, other: Self) -> Option<Self> {
        self.0.checked_sub(other.0).map(Self)
    }

    pub(crate) fn min(self, other: Self) -> Self {
        Self(self.0.min(other.0))
    }
}

impl From<U256> for Shares {
    fn from(amount: U256) -> Self {
        Self(amount)
    }
}

impl std::fmt::Display for Shares {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Overflow error for Shares arithmetic.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("Shares overflow: {lhs} + {rhs} exceeds U256::MAX")]
pub(crate) struct SharesOverflow {
    pub(crate) lhs: Shares,
    pub(crate) rhs: Shares,
}

impl std::ops::Add for Shares {
    type Output = Result<Self, SharesOverflow>;

    fn add(self, other: Self) -> Result<Self, SharesOverflow> {
        self.0
            .checked_add(other.0)
            .map(Self)
            .ok_or(SharesOverflow { lhs: self, rhs: other })
    }
}

/// Metadata about a receipt stored in the inventory.
///
/// `balance` mirrors the on-chain ERC-1155 balance the bot believes it holds.
/// `reserved` tracks shares committed to submitted-but-unconfirmed burns,
/// keyed by the redemption that submitted them. Available inventory for burn
/// planning is `balance - sum(reserved)`. Keeping the two separate is what lets
/// reconciliation compare against the true on-chain mirror without clobbering a
/// pending reservation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReceiptMetadata {
    balance: Shares,
    #[serde(default)]
    reserved: HashMap<IssuerRedemptionRequestId, Shares>,
    tx_hash: TxHash,
    block_number: u64,
    #[serde(default)]
    receipt_info: Option<ReceiptInformation>,
    #[serde(default)]
    receipt_info_bytes: Option<Bytes>,
}

impl ReceiptMetadata {
    /// Total shares reserved across all redemptions for this receipt.
    fn reserved_total(&self) -> Shares {
        Self::sum_reserved(self.reserved.values().copied())
    }

    /// Shares reserved by every redemption except `redemption`.
    ///
    /// Used when validating a reservation so a redemption re-reserving the same
    /// receipt (idempotent re-delivery) does not count its own prior amount.
    fn reserved_excluding(
        &self,
        redemption: &IssuerRedemptionRequestId,
    ) -> Shares {
        Self::sum_reserved(
            self.reserved
                .iter()
                .filter(|(id, _)| *id != redemption)
                .map(|(_, shares)| *shares),
        )
    }

    /// Shares available for new burn planning: mirror balance minus all
    /// outstanding reservations. Saturates to zero if reservations somehow
    /// exceed the mirror (e.g. after an external on-chain drain).
    fn available(&self) -> Shares {
        self.balance.checked_sub(self.reserved_total()).unwrap_or(Shares::ZERO)
    }

    /// Shares available to `redemption` for planning: mirror balance minus
    /// every *other* redemption's reservation. Excludes `redemption`'s own
    /// prior reservation so a retry can re-plan its full burn against the
    /// inventory it is entitled to; the atomic clear-and-reserve then replaces
    /// that prior reservation.
    fn available_excluding(
        &self,
        redemption: &IssuerRedemptionRequestId,
    ) -> Shares {
        self.balance
            .checked_sub(self.reserved_excluding(redemption))
            .unwrap_or(Shares::ZERO)
    }

    /// Sums reserved amounts. Each reservation was validated at `ReserveBurn`
    /// time to be `<=` this receipt's mirror balance (a real U256 share
    /// amount), so the sum cannot overflow U256 in practice; `saturating_add`
    /// is a non-panicking guard for that impossible case, not a silent cap over
    /// a real value.
    fn sum_reserved(reserved: impl Iterator<Item = Shares>) -> Shares {
        reserved.fold(Shares::ZERO, |acc, shares| {
            Shares::new(acc.inner().saturating_add(shares.inner()))
        })
    }
}

/// Parameters for registering a newly minted receipt.
pub(crate) struct MintedReceiptParams {
    pub(crate) vault: Address,
    pub(crate) receipt_id: ReceiptId,
    pub(crate) shares: Shares,
    pub(crate) block_number: u64,
    pub(crate) tx_hash: B256,
    pub(crate) receipt_info: ReceiptInformation,
    /// The exact encoded bytes passed to deposit() on-chain.
    pub(crate) receipt_info_bytes: Bytes,
}

/// Provides receipt inventory capabilities for command handlers.
///
/// Abstracts receipt discovery, querying, and burn planning.
/// Implementation uses the ReceiptInventory aggregate.
#[async_trait]
pub(crate) trait ReceiptService: Send + Sync {
    /// Registers a newly minted receipt in the inventory.
    ///
    /// Called after a successful on-chain deposit to make the receipt
    /// immediately available for burn planning. The monitor/backfill
    /// will also discover it eventually, but explicit registration
    /// avoids the timing gap.
    async fn register_minted_receipt(
        &self,
        params: MintedReceiptParams,
    ) -> Result<(), ReceiptRegistrationError>;

    /// Returns a burn plan: which receipts to burn and how much from each.
    ///
    /// The plan satisfies the required burn amount by selecting from available
    /// receipts in descending order of balance. Availability excludes
    /// `redemption_issuer_request_id`'s own prior reservation, so a retry plans
    /// against its full entitlement; the subsequent `reserve_burn` atomically
    /// replaces that reservation.
    async fn for_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: &IssuerRedemptionRequestId,
        shares_to_burn: Shares,
        dust: Shares,
    ) -> Result<BurnPlan, BurnTrackingError>;

    async fn reserve_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        burns: Vec<BurnRecord>,
    ) -> Result<(), ReceiptRegistrationError>;

    async fn release_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    ) -> Result<(), ReceiptRegistrationError>;

    async fn settle_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    ) -> Result<(), ReceiptRegistrationError>;

    /// Returns the redemptions that currently hold a reservation on the vault,
    /// for startup reservation recovery.
    async fn reserved_redemptions(
        &self,
        vault: Address,
    ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError>;

    /// Finds a receipt by its issuer_request_id (for ITN mints only).
    ///
    /// Used by mint recovery to check if a mint succeeded on-chain.
    /// Returns None if no receipt exists with this issuer_request_id.
    async fn find_by_issuer_request_id(
        &self,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError>;
}

#[derive(Debug, Error)]
pub(crate) enum ReceiptRegistrationError {
    #[error(transparent)]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
}

/// Implementation of ReceiptService using the ReceiptInventory aggregate.
pub(crate) struct CqrsReceiptService {
    store: Arc<Store<ReceiptInventory>>,
}

impl CqrsReceiptService {
    pub(crate) const fn new(store: Arc<Store<ReceiptInventory>>) -> Self {
        Self { store }
    }
}

/// Loads a vault's receipt inventory, treating an absent event stream as an
/// empty inventory.
///
/// This is the single home of that domain rule: an uninitialized vault
/// behaves exactly like a vault holding no receipts (`originate` is total for
/// the same reason). Every read of the aggregate must go through this helper
/// rather than re-encoding `load(..)?.unwrap_or_default()` at the call site,
/// so a future reader cannot silently diverge on the `None` case.
pub(crate) async fn load_inventory(
    store: &Store<ReceiptInventory>,
    vault: &Address,
) -> Result<ReceiptInventory, SendError<ReceiptInventory>> {
    Ok(store.load(vault).await?.unwrap_or_default())
}

/// Translates an `event_sorcery` send/load error back into the
/// `AggregateError<ReceiptInventoryError>` the service contract exposes.
///
/// Command-handler domain errors arrive as
/// `UserError(LifecycleError::Apply(domain))` and are unwrapped to the bare
/// domain error; `AggregateConflict` is preserved so `BurnManager`'s
/// optimistic-concurrency retry keeps matching it. Lifecycle errors that can
/// only arise from event-application corruption (never from a command) are
/// surfaced as `UnexpectedError`.
fn to_aggregate_error(
    error: SendError<ReceiptInventory>,
) -> AggregateError<ReceiptInventoryError> {
    match error {
        AggregateError::UserError(LifecycleError::Apply(domain)) => {
            AggregateError::UserError(domain)
        }
        AggregateError::UserError(lifecycle) => {
            AggregateError::UnexpectedError(Box::new(lifecycle))
        }
        AggregateError::AggregateConflict => AggregateError::AggregateConflict,
        AggregateError::DatabaseConnectionError(source) => {
            AggregateError::DatabaseConnectionError(source)
        }
        AggregateError::DeserializationError(source) => {
            AggregateError::DeserializationError(source)
        }
        AggregateError::UnexpectedError(source) => {
            AggregateError::UnexpectedError(source)
        }
    }
}

impl From<SendError<ReceiptInventory>> for ReceiptRegistrationError {
    fn from(error: SendError<ReceiptInventory>) -> Self {
        to_aggregate_error(error).into()
    }
}

impl From<SendError<ReceiptInventory>> for ReceiptLookupError {
    fn from(error: SendError<ReceiptInventory>) -> Self {
        to_aggregate_error(error).into()
    }
}

impl From<SendError<ReceiptInventory>> for BurnTrackingError {
    fn from(error: SendError<ReceiptInventory>) -> Self {
        to_aggregate_error(error).into()
    }
}

impl From<SendError<ReceiptInventory>> for BackfillError {
    fn from(error: SendError<ReceiptInventory>) -> Self {
        to_aggregate_error(error).into()
    }
}

impl From<SendError<ReceiptInventory>> for ReconcileError {
    fn from(error: SendError<ReceiptInventory>) -> Self {
        to_aggregate_error(error).into()
    }
}

#[async_trait]
impl ReceiptService for CqrsReceiptService {
    async fn register_minted_receipt(
        &self,
        params: MintedReceiptParams,
    ) -> Result<(), ReceiptRegistrationError> {
        let issuer_request_id = params.receipt_info.issuer_request_id.clone();

        self.store
            .send(
                &params.vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: params.receipt_id,
                    balance: params.shares,
                    block_number: params.block_number,
                    tx_hash: params.tx_hash,
                    source: ReceiptSource::Itn { issuer_request_id },
                    receipt_info: Some(Box::new(params.receipt_info)),
                    receipt_info_bytes: Some(params.receipt_info_bytes),
                },
            )
            .await?;

        Ok(())
    }

    async fn for_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: &IssuerRedemptionRequestId,
        shares_to_burn: Shares,
        dust: Shares,
    ) -> Result<BurnPlan, BurnTrackingError> {
        let inventory = load_inventory(&self.store, &vault).await?;

        let receipts = inventory
            .receipts_with_balance_excluding(redemption_issuer_request_id);
        plan_burn(receipts, shares_to_burn, dust)
    }

    async fn reserve_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
        burns: Vec<BurnRecord>,
    ) -> Result<(), ReceiptRegistrationError> {
        self.store
            .send(
                &vault,
                ReceiptInventoryCommand::ReserveBurn {
                    redemption_issuer_request_id,
                    burns,
                },
            )
            .await?;

        Ok(())
    }

    async fn release_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    ) -> Result<(), ReceiptRegistrationError> {
        self.store
            .send(
                &vault,
                ReceiptInventoryCommand::ReleaseBurn {
                    redemption_issuer_request_id,
                },
            )
            .await?;

        Ok(())
    }

    async fn settle_burn(
        &self,
        vault: Address,
        redemption_issuer_request_id: IssuerRedemptionRequestId,
    ) -> Result<(), ReceiptRegistrationError> {
        self.store
            .send(
                &vault,
                ReceiptInventoryCommand::SettleBurn {
                    redemption_issuer_request_id,
                },
            )
            .await?;

        Ok(())
    }

    async fn reserved_redemptions(
        &self,
        vault: Address,
    ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError> {
        let inventory = load_inventory(&self.store, &vault).await?;
        Ok(inventory.reserved_redemptions())
    }

    async fn find_by_issuer_request_id(
        &self,
        vault: &Address,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
        let receipt_inventory = load_inventory(&self.store, vault).await?;

        let Some(receipt_id) =
            receipt_inventory.find_by_issuer_request_id(issuer_request_id)
        else {
            return Ok(None);
        };

        let receipt = receipt_inventory
            .receipts_with_balance()
            .into_iter()
            .find(|r| r.receipt_id == receipt_id)
            .ok_or(ReceiptLookupError::Inconsistent {
                issuer_request_id: issuer_request_id.clone(),
                receipt_id,
            })?;

        Ok(Some(RecoveredReceipt {
            receipt_id: receipt_id.inner(),
            tx_hash: receipt.tx_hash,
            shares: receipt.available_balance.inner(),
            block_number: receipt.block_number,
        }))
    }
}

/// Tracks all receipts with available balance for a vault.
///
/// This aggregate maintains a map of receipt IDs to their available balances.
/// Receipts with zero balance are removed from state (events remain for audit).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct ReceiptInventory {
    receipts: HashMap<ReceiptId, ReceiptMetadata>,
    /// Maps issuer_request_id to receipt_id for ITN mints.
    /// Used by mint recovery to check if a mint succeeded on-chain.
    itn_receipts: HashMap<IssuerMintRequestId, ReceiptId>,
}

impl ReceiptInventory {
    pub(crate) fn receipts_with_balance(&self) -> Vec<ReceiptWithBalance> {
        self.receipts
            .iter()
            .map(|(receipt_id, metadata)| {
                Self::receipt_row(*receipt_id, metadata, metadata.available())
            })
            .collect()
    }

    /// Receipts with the availability `redemption` may plan against, treating
    /// its own prior reservation as not held (see
    /// `ReceiptMetadata::available_excluding`). Used by `for_burn` so a retry
    /// re-plans its full burn rather than being blocked by its own reservation.
    pub(crate) fn receipts_with_balance_excluding(
        &self,
        redemption: &IssuerRedemptionRequestId,
    ) -> Vec<ReceiptWithBalance> {
        self.receipts
            .iter()
            .map(|(receipt_id, metadata)| {
                Self::receipt_row(
                    *receipt_id,
                    metadata,
                    metadata.available_excluding(redemption),
                )
            })
            .collect()
    }

    fn receipt_row(
        receipt_id: ReceiptId,
        metadata: &ReceiptMetadata,
        available_balance: Shares,
    ) -> ReceiptWithBalance {
        ReceiptWithBalance {
            receipt_id,
            available_balance,
            tx_hash: metadata.tx_hash,
            block_number: metadata.block_number,
            receipt_info: metadata.receipt_info.clone(),
            receipt_info_bytes: metadata.receipt_info_bytes.clone(),
        }
    }

    /// Finds a receipt by its issuer_request_id (for ITN mints only).
    /// Returns None if no ITN receipt exists with this issuer_request_id.
    pub(crate) fn find_by_issuer_request_id(
        &self,
        issuer_request_id: &IssuerMintRequestId,
    ) -> Option<ReceiptId> {
        self.itn_receipts.get(issuer_request_id).copied()
    }

    /// Whether the given redemption holds a reservation on any receipt.
    fn holds_reservation(
        &self,
        redemption: &IssuerRedemptionRequestId,
    ) -> bool {
        self.receipts
            .values()
            .any(|metadata| metadata.reserved.contains_key(redemption))
    }

    /// Distinct redemptions that currently hold a reservation on any receipt.
    ///
    /// Used by startup reservation recovery to find reservations that were
    /// never settled or released (e.g. a crash between burn confirmation and
    /// settlement) and resolve them against the redemption's terminal state.
    pub(crate) fn reserved_redemptions(
        &self,
    ) -> Vec<IssuerRedemptionRequestId> {
        self.receipts
            .values()
            .flat_map(|metadata| metadata.reserved.keys().cloned())
            .unique()
            .collect()
    }

    /// `Depleted` events for receipts that clearing `redemption`'s reservation
    /// would leave at zero balance with no other reservation outstanding.
    ///
    /// `consume_balance` true models settlement (the mirror drops by the
    /// reserved amount); false models release (the mirror is unchanged). This
    /// also covers a receipt already at zero balance — preserved by the
    /// zero-drain reconcile guard — so it is depleted once its last reservation
    /// resolves rather than lingering empty forever.
    fn depletions_after_clearing(
        &self,
        redemption: &IssuerRedemptionRequestId,
        consume_balance: bool,
    ) -> Vec<ReceiptInventoryEvent> {
        self.receipts
            .iter()
            .filter_map(|(receipt_id, metadata)| {
                let reserved = metadata.reserved.get(redemption)?;

                // Another redemption still holds this receipt; leave it.
                if metadata.reserved.len() > 1 {
                    return None;
                }

                let post_balance = if consume_balance {
                    metadata
                        .balance
                        .checked_sub(*reserved)
                        .unwrap_or(Shares::ZERO)
                } else {
                    metadata.balance
                };

                post_balance.is_zero().then_some(
                    ReceiptInventoryEvent::Depleted { receipt_id: *receipt_id },
                )
            })
            .collect()
    }
}

/// Determines the source of a receipt by parsing the vault's receiptInformation.
///
/// Supports two formats:
/// 1. Rain metadata v1 CBOR (new format): starts with magic prefix `0xff0a89c674ee7874`,
///    contains deflated JSON payload with `OA_STRUCTURE` magic number
/// 2. Raw JSON (legacy format): plain JSON bytes from before the CBOR migration
///
/// If the receiptInformation contains a valid `issuer_request_id` in either format,
/// returns `ReceiptSource::Itn` with the parsed `ReceiptInformation`;
/// otherwise `ReceiptSource::External` with `None`.
pub(crate) fn determine_source(
    receipt_information: &Bytes,
) -> (ReceiptSource, Option<ReceiptInformation>) {
    if receipt_information.is_empty() {
        return (ReceiptSource::External, None);
    }

    let json_bytes = if rain_meta::is_rain_meta(receipt_information) {
        match rain_meta::decode_receipt_meta(receipt_information) {
            Ok(bytes) => bytes,
            Err(err) => {
                warn!(target: "receipt", error = %err,
                    data_len = receipt_information.len(),
                    data_prefix = %alloy::hex::encode(
                        &receipt_information[..receipt_information.len().min(32)]
                    ),
                    "Failed to decode Rain metadata from receipt information, \
                     classifying as External"
                );
                return (ReceiptSource::External, None);
            }
        }
    } else {
        receipt_information.to_vec()
    };

    serde_json::from_slice::<ReceiptInformation>(&json_bytes).ok().map_or(
        (ReceiptSource::External, None),
        |info| {
            let source = ReceiptSource::Itn {
                issuer_request_id: info.issuer_request_id.clone(),
            };
            (source, Some(info))
        },
    )
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, thiserror::Error)]
pub(crate) enum ReceiptInventoryError {
    #[error("Unknown receipt {receipt_id}")]
    UnknownReceipt { receipt_id: ReceiptId },
    #[error(transparent)]
    SharesOverflow(#[from] SharesOverflow),
    #[error(
        "Receipt {receipt_id} has insufficient balance: \
         available={available}, required={required}"
    )]
    InsufficientReceiptBalance {
        receipt_id: ReceiptId,
        available: Shares,
        required: Shares,
    },
}

impl ReceiptInventory {
    fn handle_command(
        &self,
        command: ReceiptInventoryCommand,
    ) -> Result<Vec<ReceiptInventoryEvent>, ReceiptInventoryError> {
        match command {
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id,
                balance,
                block_number,
                tx_hash,
                source,
                receipt_info,
                receipt_info_bytes,
            } => {
                if self.receipts.contains_key(&receipt_id) {
                    return Ok(vec![]);
                }

                Ok(vec![ReceiptInventoryEvent::Discovered {
                    receipt_id,
                    balance,
                    block_number,
                    tx_hash,
                    source,
                    receipt_info,
                    receipt_info_bytes,
                }])
            }

            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance,
            } => {
                let Some(metadata) = self.receipts.get(&receipt_id) else {
                    return Ok(vec![]);
                };

                let mirror = metadata.balance;
                let available = metadata.available();

                // While a submitted burn is pending, on-chain legitimately
                // sits between `available` (all reservations settled) and
                // `mirror` (none landed yet). Treat anything in that band as
                // "no external change" so reconciliation never clobbers a
                // reservation — settlement, not reconciliation, consumes it.
                if (available..=mirror).contains(&on_chain_balance) {
                    return Ok(if mirror.is_zero() {
                        depletion_events(receipt_id, metadata)
                    } else {
                        vec![]
                    });
                }

                let reconciled = ReceiptInventoryEvent::BalanceReconciled {
                    receipt_id,
                    previous_balance: mirror,
                    on_chain_balance,
                };

                let depletion = if on_chain_balance.is_zero() {
                    depletion_events(receipt_id, metadata)
                } else {
                    vec![]
                };

                Ok(std::iter::once(reconciled).chain(depletion).collect())
            }

            ReceiptInventoryCommand::ReserveBurn {
                redemption_issuer_request_id,
                burns,
            } => {
                let required_by_receipt = required_burns_by_receipt(&burns)?;

                for (receipt_id, required) in required_by_receipt {
                    let Some(metadata) = self.receipts.get(&receipt_id) else {
                        return Err(ReceiptInventoryError::UnknownReceipt {
                            receipt_id,
                        });
                    };

                    // Validate against availability excluding this redemption's
                    // own prior reservation, matching how `for_burn` plans, so a
                    // retry (or re-delivered ReserveBurn) does not count itself
                    // and over-reject. The atomic clear-and-reserve apply then
                    // replaces that prior reservation.
                    let available = metadata
                        .available_excluding(&redemption_issuer_request_id);

                    if available < required {
                        return Err(
                            ReceiptInventoryError::InsufficientReceiptBalance {
                                receipt_id,
                                available,
                                required,
                            },
                        );
                    }
                }

                Ok(vec![ReceiptInventoryEvent::BurnReserved {
                    redemption_issuer_request_id,
                    burns,
                }])
            }

            // Release restores a reservation without touching the mirror
            // balance (the burn never consumed any shares on-chain). It is a
            // no-op when the redemption holds no reservation, which makes it
            // safe to call defensively before re-planning a retry. Emits
            // Depleted for a zero-balance receipt whose last reservation this
            // release clears.
            ReceiptInventoryCommand::ReleaseBurn {
                redemption_issuer_request_id,
            } => {
                if !self.holds_reservation(&redemption_issuer_request_id) {
                    return Ok(vec![]);
                }

                let depletions = self.depletions_after_clearing(
                    &redemption_issuer_request_id,
                    false,
                );

                Ok(std::iter::once(ReceiptInventoryEvent::BurnReleased {
                    redemption_issuer_request_id,
                })
                .chain(depletions)
                .collect())
            }

            // Settle consumes a reservation after its burn confirmed on-chain:
            // the mirror balance drops by the reserved amount. Emits Depleted
            // for any receipt the settlement empties.
            ReceiptInventoryCommand::SettleBurn {
                redemption_issuer_request_id,
            } => {
                if !self.holds_reservation(&redemption_issuer_request_id) {
                    return Ok(vec![]);
                }

                let depletions = self.depletions_after_clearing(
                    &redemption_issuer_request_id,
                    true,
                );

                Ok(std::iter::once(ReceiptInventoryEvent::BurnSettled {
                    redemption_issuer_request_id,
                })
                .chain(depletions)
                .collect())
            }
        }
    }

    fn apply_event(&mut self, event: ReceiptInventoryEvent) {
        match event {
            ReceiptInventoryEvent::Discovered {
                receipt_id,
                balance,
                block_number,
                tx_hash,
                source,
                receipt_info,
                receipt_info_bytes,
            } => {
                self.receipts.insert(
                    receipt_id,
                    ReceiptMetadata {
                        balance,
                        reserved: HashMap::new(),
                        tx_hash,
                        block_number,
                        receipt_info: receipt_info.map(|boxed| *boxed),
                        receipt_info_bytes,
                    },
                );
                if let ReceiptSource::Itn { issuer_request_id } = source {
                    self.itn_receipts.insert(issuer_request_id, receipt_id);
                }
            }

            ReceiptInventoryEvent::BalanceReconciled {
                receipt_id,
                on_chain_balance,
                ..
            } => {
                // Remove the receipt only when on-chain hit zero AND no
                // reservation survives. A reserved receipt drained to zero is
                // preserved (mirror set to zero) so a pending settle/release
                // can still resolve the reservation rather than have it
                // silently discarded with the receipt.
                let has_reservation = self
                    .receipts
                    .get(&receipt_id)
                    .is_some_and(|metadata| !metadata.reserved.is_empty());

                if on_chain_balance.is_zero() && !has_reservation {
                    self.receipts.remove(&receipt_id);
                    self.itn_receipts.retain(|_, indexed_receipt_id| {
                        *indexed_receipt_id != receipt_id
                    });
                } else if let Some(metadata) =
                    self.receipts.get_mut(&receipt_id)
                {
                    metadata.balance = on_chain_balance;
                }
            }

            ReceiptInventoryEvent::Depleted { receipt_id } => {
                self.receipts.remove(&receipt_id);
                self.itn_receipts.retain(|_, indexed_receipt_id| {
                    *indexed_receipt_id != receipt_id
                });
            }

            // Atomic clear-and-reserve: a reservation is the redemption's whole
            // current allocation, so first drop any prior reservation it held
            // (e.g. from a retried attempt that planned different receipts),
            // then install the new one. This makes a retry's reserve replace the
            // redemption's reservation set in a single transaction — no stale
            // entry can survive to be over-consumed at settle, and the balance
            // is never returned to global availability between clear and
            // reserve.
            ReceiptInventoryEvent::BurnReserved {
                redemption_issuer_request_id,
                burns,
            } => {
                let reserved_by_receipt = match required_burns_by_receipt(
                    &burns,
                ) {
                    Ok(reserved_by_receipt) => reserved_by_receipt,
                    Err(err) => {
                        warn!(target: "receipt",
                            redemption_issuer_request_id = %redemption_issuer_request_id,
                            error = %err,
                            "BurnReserved burns failed to aggregate; reservation not applied"
                        );
                        return;
                    }
                };

                for metadata in self.receipts.values_mut() {
                    metadata.reserved.remove(&redemption_issuer_request_id);
                }

                for (receipt_id, reserved) in reserved_by_receipt {
                    let Some(metadata) = self.receipts.get_mut(&receipt_id)
                    else {
                        warn!(target: "receipt", receipt_id = %receipt_id,
                            redemption_issuer_request_id = %redemption_issuer_request_id,
                            "BurnReserved for unknown receipt; reservation skipped"
                        );
                        continue;
                    };

                    metadata
                        .reserved
                        .insert(redemption_issuer_request_id.clone(), reserved);
                }
            }

            // Release clears the redemption's reservation everywhere it is
            // held, restoring availability without touching the mirror balance.
            ReceiptInventoryEvent::BurnReleased {
                redemption_issuer_request_id,
            } => {
                for metadata in self.receipts.values_mut() {
                    metadata.reserved.remove(&redemption_issuer_request_id);
                }
            }

            // Settle consumes the reservation: drop it and reduce the mirror
            // balance by the reserved amount, since those shares left on-chain.
            // Idempotent — a receipt with no matching reservation is skipped.
            ReceiptInventoryEvent::BurnSettled {
                redemption_issuer_request_id,
            } => {
                for (receipt_id, metadata) in &mut self.receipts {
                    let Some(reserved) =
                        metadata.reserved.remove(&redemption_issuer_request_id)
                    else {
                        continue;
                    };

                    let Some(new_balance) =
                        metadata.balance.checked_sub(reserved)
                    else {
                        warn!(target: "receipt", receipt_id = %receipt_id,
                            redemption_issuer_request_id = %redemption_issuer_request_id,
                            balance = %metadata.balance,
                            reserved = %reserved,
                            "BurnSettled reservation exceeds mirror balance; clamping balance to zero"
                        );
                        metadata.balance = Shares::ZERO;
                        continue;
                    };

                    metadata.balance = new_balance;
                }
            }
        }
    }
}

#[async_trait]
impl EventSourced for ReceiptInventory {
    type Id = Address;
    type Event = ReceiptInventoryEvent;
    type Command = ReceiptInventoryCommand;
    type Error = ReceiptInventoryError;
    type Services = ();
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "ReceiptInventory";
    const PROJECTION: Nil = Nil;
    const SCHEMA_VERSION: u64 = 2;

    // Snapshots are disabled: the pre-migration wiring never wrote snapshots,
    // and event-sorcery hardwires snapshot-every-N with no off switch, so
    // usize::MAX makes the next-snapshot threshold unreachable. The proper
    // fix is for event-sorcery to take the snapshot policy explicitly from
    // the consumer, including the option to disable snapshotting entirely.
    //
    // Replay-cost note for whoever profiles a slow mint/redemption path:
    // without snapshots every send/load replays the vault's full event
    // stream, and `evolve` clones the whole inventory per event (its `&Self`
    // signature forces it), so loads cost O(events x receipts) and the
    // per-vault stream only ever grows.
    const SNAPSHOT_SIZE: usize = usize::MAX;

    /// Originate is total: the empty `Self::default()` is both the
    /// uninitialized seed and a valid live state, so any first event yields a
    /// valid inventory. `Discovered` inserts a receipt; any other genesis event
    /// applies as a no-op against the empty default (only `BurnReserved` with
    /// burns logs a WARN), matching the old infallible replay. Returning
    /// `None` here would make a stream whose first event isn't `Discovered`
    /// fail to load instead of yielding an empty inventory.
    fn originate(event: &Self::Event) -> Option<Self> {
        let mut inventory = Self::default();
        inventory.apply_event(event.clone());
        Some(inventory)
    }

    fn evolve(
        entity: &Self,
        event: &Self::Event,
    ) -> Result<Option<Self>, Self::Error> {
        let mut next = entity.clone();
        next.apply_event(event.clone());
        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Self::default().handle_command(command)
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        self.handle_command(command)
    }
}

/// Decides whether a receipt that has reached zero balance should be depleted.
///
/// A receipt that still holds a reservation is preserved (with a WARN) instead
/// of depleted: removing it would silently discard the reservation and turn a
/// later settle/release into a no-op. A pending settle/release or the startup
/// reservation recovery resolves it instead.
fn depletion_events(
    receipt_id: ReceiptId,
    metadata: &ReceiptMetadata,
) -> Vec<ReceiptInventoryEvent> {
    if metadata.reserved.is_empty() {
        return vec![ReceiptInventoryEvent::Depleted { receipt_id }];
    }

    warn!(target: "receipt", receipt_id = %receipt_id,
        "Receipt reached zero balance while still holding reservations; \
         preserving it so the reservation can resolve"
    );
    vec![]
}

fn required_burns_by_receipt(
    burns: &[BurnRecord],
) -> Result<HashMap<ReceiptId, Shares>, ReceiptInventoryError> {
    burns.iter().try_fold(HashMap::new(), |mut required, burn| {
        let receipt_id = ReceiptId::from(burn.receipt_id);
        let shares = Shares::from(burn.shares_burned);
        let current = required
            .remove(&receipt_id)
            .map_or(Ok(shares), |existing| existing + shares)?;

        required.insert(receipt_id, current);

        Ok(required)
    })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Bytes, TxHash, address, b256};
    use event_sorcery::{StoreBuilder, test_store};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::prepare_event_sourced_startup;
    use crate::test_utils::logs_contain_at;

    const TEST_OA_SCHEMA: &str =
        "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm";

    fn make_receipt_id(n: u64) -> ReceiptId {
        ReceiptId::from(U256::from(n))
    }

    fn make_shares(n: u64) -> Shares {
        Shares::new(U256::from(n))
    }

    /// A redemption distinct from any used to reserve in these tests, so
    /// `for_burn` planning counts (does not exclude) the reservations under
    /// test — i.e. the "another redemption is planning" viewpoint.
    fn planning_redemption() -> IssuerRedemptionRequestId {
        "red-00000000".parse().unwrap()
    }

    fn discover_receipt_cmd(
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
    ) -> ReceiptInventoryCommand {
        ReceiptInventoryCommand::DiscoverReceipt {
            receipt_id,
            balance,
            block_number,
            tx_hash,
            source: ReceiptSource::External,
            receipt_info: None,
            receipt_info_bytes: None,
        }
    }

    fn discover_itn_receipt_cmd(
        receipt_id: ReceiptId,
        balance: Shares,
        block_number: u64,
        tx_hash: TxHash,
        issuer_request_id: IssuerMintRequestId,
    ) -> ReceiptInventoryCommand {
        ReceiptInventoryCommand::DiscoverReceipt {
            receipt_id,
            balance,
            block_number,
            tx_hash,
            source: ReceiptSource::Itn { issuer_request_id },
            receipt_info: None,
            receipt_info_bytes: None,
        }
    }

    async fn setup_store() -> Arc<Store<ReceiptInventory>> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");
        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");
        Arc::new(test_store::<ReceiptInventory>(pool, ()))
    }

    /// Runs a command against the aggregate and applies the produced events,
    /// so test state is only ever built through the public command API.
    async fn drive(
        aggregate: &mut ReceiptInventory,
        command: ReceiptInventoryCommand,
    ) -> Vec<ReceiptInventoryEvent> {
        let events = aggregate.transition(command, &()).await.unwrap();
        for event in events.clone() {
            aggregate.apply_event(event);
        }
        events
    }

    #[tokio::test]
    async fn test_discover_receipt_emits_discovered_event() {
        let aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::Discovered {
                receipt_id,
                balance,
                block_number: 1000,
                ..
            } if *receipt_id == make_receipt_id(42) && *balance == make_shares(100)
        ));
    }

    #[tokio::test]
    async fn test_discover_already_discovered_is_idempotent() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // First discovery succeeds
        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply_event(event);
        }

        // Second discovery is idempotent - returns Ok with no events
        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(200),
                    2000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_discover_receipt_with_receipt_info_roundtrips() {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-roundtrip"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("TSLA"),
            Quantity(Decimal::new(5050, 2)),
            Utc::now(),
            Some("test notes".to_string()),
        );

        store
            .send(
                &vault,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: make_receipt_id(99),
                    balance: make_shares(500),
                    block_number: 2000,
                    tx_hash,
                    source: ReceiptSource::Itn { issuer_request_id },
                    receipt_info: Some(Box::new(receipt_info.clone())),
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();

        let inventory = load_inventory(&store, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].receipt_info,
            Some(receipt_info),
            "receipt_info should be preserved through command -> event -> state"
        );
    }

    #[tokio::test]
    async fn test_non_discovered_genesis_event_loads_as_empty_inventory() {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        // ReserveBurn with no burns aggregates to an empty required-by-receipt
        // map, so it skips the receipt-existence check and emits BurnReserved as
        // a genesis event — a non-Discovered first event on a fresh vault.
        // originate must treat the empty default as a valid live state and yield
        // an empty inventory, not fail the load with EventCantOriginate (every
        // read goes through `load_inventory`, where a load error propagates
        // instead of degrading to an empty inventory).
        store
            .send(
                &vault,
                ReceiptInventoryCommand::ReserveBurn {
                    redemption_issuer_request_id: "red-00000000"
                        .parse()
                        .unwrap(),
                    burns: vec![],
                },
            )
            .await
            .unwrap();

        let inventory = store
            .load(&vault)
            .await
            .expect("load must not fail on a non-Discovered genesis event")
            .expect("a persisted stream must materialize an inventory");

        assert!(
            inventory.receipts_with_balance().is_empty(),
            "an empty ReserveBurn must leave the inventory empty"
        );
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_receipt_when_itn_receipt_exists()
     {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();

        store
            .send(
                &vault,
                discover_itn_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                    issuer_request_id.clone(),
                ),
            )
            .await
            .unwrap();

        let inventory = load_inventory(&store, &vault).await.unwrap();
        let found = inventory.find_by_issuer_request_id(&issuer_request_id);
        assert_eq!(found, Some(make_receipt_id(42)));
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_when_not_exists() {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let issuer_request_id = IssuerMintRequestId::random();

        let inventory = load_inventory(&store, &vault).await.unwrap();
        let found = inventory.find_by_issuer_request_id(&issuer_request_id);
        assert_eq!(found, None);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_for_external_receipts()
    {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        // Discover an external receipt (no issuer_request_id)
        store
            .send(
                &vault,
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
            )
            .await
            .unwrap();

        let inventory = load_inventory(&store, &vault).await.unwrap();

        // External receipts should not be indexed by issuer_request_id
        let random_id = IssuerMintRequestId::random();
        assert_eq!(inventory.find_by_issuer_request_id(&random_id), None);

        // But the receipt itself should exist
        assert_eq!(inventory.receipts.len(), 1);
    }

    #[test]
    fn test_determine_source_returns_itn_when_receipt_info_has_issuer_request_id()
     {
        let expected_id = IssuerMintRequestId::random();
        let receipt_info = serde_json::json!({
            "tokenization_request_id": "tok-123",
            "issuer_request_id": expected_id.to_string(),
            "underlying": "AAPL",
            "quantity": "100.0",
            "operation_type": "Mint",
            "timestamp": "2024-01-01T00:00:00Z",
            "notes": null
        });

        let bytes = Bytes::from(serde_json::to_vec(&receipt_info).unwrap());
        let (source, info) = determine_source(&bytes);

        assert!(matches!(
            source,
            ReceiptSource::Itn { issuer_request_id } if issuer_request_id == expected_id
        ));
        assert!(info.is_some());
    }

    #[test]
    fn test_determine_source_returns_external_when_receipt_info_is_empty() {
        let bytes = Bytes::new();

        let (source, info) = determine_source(&bytes);

        assert!(matches!(source, ReceiptSource::External));
        assert!(info.is_none());
    }

    #[test]
    fn test_determine_source_returns_external_when_receipt_info_is_invalid_json()
     {
        let bytes = Bytes::from(vec![0x00, 0x01, 0x02]);

        let (source, info) = determine_source(&bytes);

        assert!(matches!(source, ReceiptSource::External));
        assert!(info.is_none());
    }

    #[test]
    fn test_determine_source_returns_itn_when_receipt_info_is_rain_cbor() {
        let expected_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-cbor-test"),
            expected_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(1005, 1)),
            Utc.timestamp_opt(1_704_067_200, 0).unwrap(),
            Some("cbor test".to_string()),
        );

        let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();
        assert!(
            rain_meta::is_rain_meta(&encoded),
            "encoded receipt should be rain meta format"
        );

        let (source, info) = determine_source(&encoded);

        assert!(
            matches!(
                &source,
                ReceiptSource::Itn { issuer_request_id } if issuer_request_id == &expected_id
            ),
            "Expected Itn source with matching issuer_request_id, got {source:?}"
        );
        assert!(info.is_some());
    }

    #[test]
    fn test_determine_source_returns_external_when_rain_meta_has_invalid_cbor()
    {
        let mut bytes = vec![0xff, 0x0a, 0x89, 0xc6, 0x74, 0xee, 0x78, 0x74];
        bytes.extend_from_slice(&[0x00, 0x01, 0x02]);
        let bytes = Bytes::from(bytes);

        let (source, info) = determine_source(&bytes);

        assert!(matches!(source, ReceiptSource::External));
        assert!(info.is_none());
    }

    async fn setup_receipt_service_with_receipts(
        receipts: Vec<(u64, u64)>,
    ) -> CqrsReceiptService {
        let store = setup_store().await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        for (i, (id, balance)) in receipts.into_iter().enumerate() {
            store
                .send(
                    &vault,
                    discover_receipt_cmd(
                        make_receipt_id(id),
                        make_shares(balance),
                        1000 + i as u64,
                        tx_hash,
                    ),
                )
                .await
                .unwrap();
        }

        CqrsReceiptService::new(store)
    }

    #[test]
    fn test_aggregate_conflict_survives_error_translation() {
        // BurnManager::reserve_with_conflict_retry matches on
        // ReceiptRegistrationError::Aggregate(AggregateError::AggregateConflict)
        // to retry optimistic-concurrency conflicts on the same vault. The store
        // surfaces those conflicts as SendError::AggregateConflict; this locks in
        // that the translation preserves the variant rather than collapsing it
        // into UnexpectedError, which would silently break the retry and fail
        // concurrent redemptions terminally.
        let translated = to_aggregate_error(
            SendError::<ReceiptInventory>::AggregateConflict,
        );
        assert!(
            matches!(translated, AggregateError::AggregateConflict),
            "AggregateConflict must pass through to_aggregate_error unchanged"
        );

        let registration_error = ReceiptRegistrationError::from(
            SendError::<ReceiptInventory>::AggregateConflict,
        );
        assert!(
            matches!(
                registration_error,
                ReceiptRegistrationError::Aggregate(
                    AggregateError::AggregateConflict
                )
            ),
            "the From<SendError> conversion BurnManager relies on must \
             preserve AggregateConflict"
        );
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_returns_plan_when_sufficient_balance()
     {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 200)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                &planning_redemption(),
                make_shares(150),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(150));
        assert!(!plan.allocations.is_empty());
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_includes_dust_in_plan() {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 200)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                &planning_redemption(),
                make_shares(100),
                make_shares(10),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(100));
        assert_eq!(plan.dust, make_shares(10));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_returns_error_when_insufficient_balance()
     {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 50), (2, 30)]).await;

        let result = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_with_empty_inventory_returns_error()
     {
        let service = setup_receipt_service_with_receipts(vec![]).await;

        let result = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_with_unknown_vault_returns_error()
     {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;

        let result = service
            .for_burn(
                address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
                &planning_redemption(),
                make_shares(50),
                make_shares(0),
            )
            .await;

        // Unknown vault has no receipts, so insufficient balance
        assert!(matches!(
            result,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));
    }

    #[tokio::test]
    async fn test_cqrs_receipt_service_for_burn_exactly_available_succeeds() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;

        let plan = service
            .for_burn(
                address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.total_burn, make_shares(100));
        assert_eq!(plan.allocations.len(), 1);
    }

    #[tokio::test]
    async fn test_reserve_burn_removes_receipt_from_future_plans() {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 50)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        service
            .reserve_burn(
                vault,
                "red-00000001".parse().unwrap(),
                vec![BurnRecord {
                    receipt_id: U256::from(1),
                    shares_burned: U256::from(100),
                }],
            )
            .await
            .unwrap();

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(50),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.allocations.len(), 1);
        assert_eq!(plan.allocations[0].receipt.receipt_id, make_receipt_id(2));
        assert_eq!(plan.allocations[0].burn_amount, make_shares(50));
    }

    #[tokio::test]
    async fn test_reserve_burn_reduces_partial_receipt_balance() {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 50)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        service
            .reserve_burn(
                vault,
                "red-00000001".parse().unwrap(),
                vec![BurnRecord {
                    receipt_id: U256::from(1),
                    shares_burned: U256::from(60),
                }],
            )
            .await
            .unwrap();

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(90),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.allocations.len(), 2);
        assert_eq!(plan.allocations[0].receipt.receipt_id, make_receipt_id(2));
        assert_eq!(plan.allocations[0].burn_amount, make_shares(50));
        assert_eq!(plan.allocations[1].receipt.receipt_id, make_receipt_id(1));
        assert_eq!(plan.allocations[1].burn_amount, make_shares(40));
    }

    #[tokio::test]
    async fn test_reserve_burn_rejects_duplicate_rows_exceeding_balance() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        let err = service
            .reserve_burn(
                vault,
                "red-00000001".parse().unwrap(),
                vec![
                    BurnRecord {
                        receipt_id: U256::from(1),
                        shares_burned: U256::from(60),
                    },
                    BurnRecord {
                        receipt_id: U256::from(1),
                        shares_burned: U256::from(60),
                    },
                ],
            )
            .await
            .expect_err("Duplicate receipt rows must be checked cumulatively");

        assert!(matches!(
            err,
            ReceiptRegistrationError::Aggregate(AggregateError::UserError(
                ReceiptInventoryError::InsufficientReceiptBalance {
                    available,
                    required,
                    ..
                }
            )) if available == make_shares(100) && required == make_shares(120)
        ));
    }

    #[tokio::test]
    async fn test_release_burn_restores_reserved_balance() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(
                vault,
                redemption_id.clone(),
                vec![BurnRecord {
                    receipt_id: U256::from(1),
                    shares_burned: U256::from(100),
                }],
            )
            .await
            .unwrap();

        let err = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(1),
                make_shares(0),
            )
            .await
            .expect_err("Fully reserved receipt should not be available");

        assert!(matches!(err, BurnTrackingError::InsufficientBalance { .. }));

        service.release_burn(vault, redemption_id).await.unwrap();

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await
            .unwrap();

        assert_eq!(plan.allocations.len(), 1);
        assert_eq!(plan.allocations[0].receipt.receipt_id, make_receipt_id(1));
        assert_eq!(plan.allocations[0].burn_amount, make_shares(100));
    }

    fn burn(receipt_id: u64, shares: u64) -> BurnRecord {
        BurnRecord {
            receipt_id: U256::from(receipt_id),
            shares_burned: U256::from(shares),
        }
    }

    /// Critical regression guard for the same-receipt burn race: while a
    /// submitted burn is pending (its shares have NOT left on-chain),
    /// reconciliation against the unchanged on-chain balance must NOT restore
    /// the reservation to available inventory.
    #[tokio::test]
    async fn test_reconcile_preserves_pending_reservation() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.apply_event(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1,
            tx_hash: TxHash::ZERO,
            source: ReceiptSource::External,
            receipt_info: None,
            receipt_info_bytes: None,
        });
        aggregate.apply_event(ReceiptInventoryEvent::BurnReserved {
            redemption_issuer_request_id: "red-00000001".parse().unwrap(),
            burns: vec![burn(42, 100)],
        });

        assert_eq!(
            aggregate.receipts_with_balance()[0].available_balance,
            make_shares(0),
            "fully reserved receipt must be unavailable"
        );

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(100),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "reconcile against unchanged on-chain balance must not wipe a \
             pending reservation, got {events:?}"
        );

        for event in events {
            aggregate.apply_event(event);
        }

        assert_eq!(
            aggregate.receipts_with_balance()[0].available_balance,
            make_shares(0),
            "reservation must remain in effect after reconcile"
        );
    }

    /// A burn landing on-chain (on-chain drops to the available level, below
    /// the mirror) is still inside the no-op band — settlement, not reconcile,
    /// consumes the reservation.
    #[tokio::test]
    async fn test_reconcile_no_op_for_landed_unsettled_burn() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.apply_event(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1,
            tx_hash: TxHash::ZERO,
            source: ReceiptSource::External,
            receipt_info: None,
            receipt_info_bytes: None,
        });
        aggregate.apply_event(ReceiptInventoryEvent::BurnReserved {
            redemption_issuer_request_id: "red-00000001".parse().unwrap(),
            burns: vec![burn(42, 60)],
        });

        // available = 40, mirror = 100; the burn of 60 lands -> on-chain 40.
        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(40),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "on-chain within [available, mirror] is not an external change, got {events:?}"
        );
    }

    /// An on-chain balance below the available level is a genuine external
    /// change (e.g. a transfer out) and must reconcile while leaving the
    /// pending reservation intact.
    #[tokio::test]
    async fn test_reconcile_external_drain_below_available_reconciles() {
        let mut aggregate = ReceiptInventory::default();
        aggregate.apply_event(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1,
            tx_hash: TxHash::ZERO,
            source: ReceiptSource::External,
            receipt_info: None,
            receipt_info_bytes: None,
        });
        aggregate.apply_event(ReceiptInventoryEvent::BurnReserved {
            redemption_issuer_request_id: "red-00000001".parse().unwrap(),
            burns: vec![burn(42, 30)],
        });

        // available = 70; on-chain 50 < 70 -> external drain.
        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(50),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled { on_chain_balance, .. }
                if *on_chain_balance == make_shares(50)
        ));
    }

    /// In the anomalous case of a receipt at zero mirror balance that still
    /// holds a reservation, an on-chain==0 reconcile must NOT deplete it: doing
    /// so would silently drop the reservation and turn a later settle/release
    /// into a no-op.
    #[traced_test]
    #[tokio::test]
    async fn test_reconcile_preserves_reservation_at_zero_mirror() {
        // Given is built from events (the canonical ES setup): an out-of-band
        // external drain (BalanceReconciled to zero on a reserved receipt)
        // legitimately produces a zero-mirror receipt that still holds a
        // reservation. Applying events directly — rather than driving the
        // commands — keeps the setup WARN-free, so the WARN assertion below is
        // attributable only to the command under test.
        let mut aggregate = ReceiptInventory::default();
        aggregate.apply_event(ReceiptInventoryEvent::Discovered {
            receipt_id: make_receipt_id(42),
            balance: make_shares(100),
            block_number: 1,
            tx_hash: TxHash::ZERO,
            source: ReceiptSource::External,
            receipt_info: None,
            receipt_info_bytes: None,
        });
        aggregate.apply_event(ReceiptInventoryEvent::BurnReserved {
            redemption_issuer_request_id: "red-00000001".parse().unwrap(),
            burns: vec![burn(42, 70)],
        });
        aggregate.apply_event(ReceiptInventoryEvent::BalanceReconciled {
            receipt_id: make_receipt_id(42),
            previous_balance: make_shares(100),
            on_chain_balance: make_shares(0),
        });

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "must not deplete a receipt that still holds a reservation"
        );
        assert!(
            aggregate.receipts.contains_key(&make_receipt_id(42)),
            "receipt must be preserved so its reservation can still resolve"
        );
        assert!(logs_contain_at!(Level::WARN, &["still holding reservations"]));
    }

    /// An external drain that pushes on-chain below `available` to zero while a
    /// reservation is outstanding must record the mirror change but NOT deplete
    /// the receipt — depleting would silently discard the reservation.
    #[traced_test]
    #[tokio::test]
    async fn test_reconcile_zero_drain_preserves_reservation() {
        let mut aggregate = ReceiptInventory::default();
        drive(
            &mut aggregate,
            discover_receipt_cmd(
                make_receipt_id(42),
                make_shares(100),
                1,
                TxHash::ZERO,
            ),
        )
        .await;
        drive(
            &mut aggregate,
            ReceiptInventoryCommand::ReserveBurn {
                redemption_issuer_request_id: "red-00000001".parse().unwrap(),
                burns: vec![burn(42, 70)],
            },
        )
        .await;

        // available = 30; an external drain to 0 is below it (out of band).
        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(
            events.len(),
            1,
            "must reconcile the mirror but not deplete, got {events:?}"
        );
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled { on_chain_balance, .. }
                if on_chain_balance.is_zero()
        ));

        for event in events {
            aggregate.apply_event(event);
        }

        assert!(
            aggregate.receipts.contains_key(&make_receipt_id(42)),
            "receipt with an outstanding reservation must be preserved"
        );
        assert!(
            !aggregate
                .receipts
                .get(&make_receipt_id(42))
                .unwrap()
                .reserved
                .is_empty(),
            "the reservation must survive the zero-drain reconcile"
        );
        assert!(logs_contain_at!(Level::WARN, &["still holding reservations"]));
    }

    #[tokio::test]
    async fn test_settle_burn_consumes_reservation_and_reduces_mirror() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 60)])
            .await
            .unwrap();

        service.settle_burn(vault, redemption_id).await.unwrap();

        // The mirror balance itself must drop 100 -> 40 and the reservation
        // must be cleared. Checking internals distinguishes a real settlement
        // from merely leaving the reservation in place (which would leave the
        // same 40 available but a stale 100 mirror).
        let inventory = load_inventory(&service.store, &vault).await.unwrap();
        let metadata = inventory
            .receipts
            .get(&make_receipt_id(1))
            .expect("receipt should remain after partial settlement");
        assert_eq!(metadata.balance, make_shares(40));
        assert!(
            metadata.reserved.is_empty(),
            "settlement must consume the reservation"
        );

        let err = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(41),
                make_shares(0),
            )
            .await
            .expect_err("settled shares must not be available");
        assert!(matches!(err, BurnTrackingError::InsufficientBalance { .. }));
    }

    #[tokio::test]
    async fn test_settle_burn_depletes_fully_consumed_receipt() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 100)])
            .await
            .unwrap();
        service.settle_burn(vault, redemption_id).await.unwrap();

        let inventory = load_inventory(&service.store, &vault).await.unwrap();
        assert!(
            inventory.receipts_with_balance().is_empty(),
            "a fully settled receipt must be depleted and removed"
        );
    }

    /// A receipt drained to zero on-chain while reserved is preserved by the
    /// reconcile guard; once its last reservation resolves (here via release),
    /// it must be depleted and removed rather than linger empty forever.
    #[tokio::test]
    async fn test_release_depletes_zero_drained_receipt() {
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();
        let mut aggregate = ReceiptInventory::default();
        drive(
            &mut aggregate,
            discover_receipt_cmd(
                make_receipt_id(1),
                make_shares(100),
                1,
                TxHash::ZERO,
            ),
        )
        .await;
        drive(
            &mut aggregate,
            ReceiptInventoryCommand::ReserveBurn {
                redemption_issuer_request_id: redemption_id.clone(),
                burns: vec![burn(1, 70)],
            },
        )
        .await;
        // Out-of-band drain to zero: the reconcile guard preserves the
        // reserved receipt at a zero mirror balance.
        drive(
            &mut aggregate,
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id: make_receipt_id(1),
                on_chain_balance: make_shares(0),
            },
        )
        .await;

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReleaseBurn {
                    redemption_issuer_request_id: redemption_id,
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.iter().any(|event| matches!(
                event,
                ReceiptInventoryEvent::Depleted { receipt_id }
                    if *receipt_id == make_receipt_id(1)
            )),
            "releasing the last reservation on a zero-balance receipt must deplete it, got {events:?}"
        );

        for event in events {
            aggregate.apply_event(event);
        }

        assert!(
            !aggregate.receipts.contains_key(&make_receipt_id(1)),
            "the zero-drained receipt must be removed after depletion"
        );
    }

    #[tokio::test]
    async fn test_release_burn_is_idempotent() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 100)])
            .await
            .unwrap();
        service.release_burn(vault, redemption_id.clone()).await.unwrap();
        // Second release must NOT over-credit the balance.
        service.release_burn(vault, redemption_id).await.unwrap();

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await
            .unwrap();
        assert_eq!(plan.allocations[0].burn_amount, make_shares(100));

        let err = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(101),
                make_shares(0),
            )
            .await
            .expect_err("double release must not inflate balance above 100");
        assert!(matches!(err, BurnTrackingError::InsufficientBalance { .. }));
    }

    #[tokio::test]
    async fn test_release_burn_without_reservation_is_noop() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        service
            .release_burn(vault, "red-00000001".parse().unwrap())
            .await
            .expect("releasing with no reservation must be a no-op");

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(100),
                make_shares(0),
            )
            .await
            .unwrap();
        assert_eq!(plan.allocations[0].burn_amount, make_shares(100));
    }

    #[tokio::test]
    async fn test_second_redemption_cannot_reserve_committed_balance() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        service
            .reserve_burn(
                vault,
                "red-00000001".parse().unwrap(),
                vec![burn(1, 60)],
            )
            .await
            .unwrap();

        let err = service
            .reserve_burn(
                vault,
                "red-00000002".parse().unwrap(),
                vec![burn(1, 60)],
            )
            .await
            .expect_err("second redemption must not reserve committed balance");
        assert!(matches!(
            err,
            ReceiptRegistrationError::Aggregate(AggregateError::UserError(
                ReceiptInventoryError::InsufficientReceiptBalance { .. }
            ))
        ));
    }

    #[tokio::test]
    async fn test_reserve_burn_is_idempotent_for_same_redemption() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 60)])
            .await
            .unwrap();
        // Re-delivery of the same reservation must not double-count itself.
        service
            .reserve_burn(vault, redemption_id, vec![burn(1, 60)])
            .await
            .expect("re-reserving the same redemption must succeed");

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(40),
                make_shares(0),
            )
            .await
            .unwrap();
        assert_eq!(plan.allocations[0].burn_amount, make_shares(40));
    }

    /// A retry that plans a DIFFERENT receipt must atomically replace the
    /// redemption's prior reservation, not accumulate it — otherwise settle
    /// (keyed only by redemption) would over-consume the stale receipt.
    #[tokio::test]
    async fn test_reserve_replaces_prior_reservation_atomically() {
        let service =
            setup_receipt_service_with_receipts(vec![(1, 100), (2, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        // First attempt reserves receipt 1; the retry reserves receipt 2.
        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 100)])
            .await
            .unwrap();
        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(2, 100)])
            .await
            .unwrap();

        service.settle_burn(vault, redemption_id).await.unwrap();

        let inventory = load_inventory(&service.store, &vault).await.unwrap();

        // Receipt 1's stale reservation was cleared by the retry, so settle
        // left it untouched; only receipt 2 was consumed (and depleted).
        let receipt_1 = inventory
            .receipts
            .get(&make_receipt_id(1))
            .expect("receipt 1 must remain untouched");
        assert_eq!(receipt_1.balance, make_shares(100));
        assert!(receipt_1.reserved.is_empty());
        assert!(
            !inventory.receipts.contains_key(&make_receipt_id(2)),
            "receipt 2 was settled to zero and depleted"
        );
    }

    /// `for_burn` excludes the planning redemption's own reservation (so a retry
    /// can re-plan its full burn) while still counting other redemptions'.
    #[tokio::test]
    async fn test_for_burn_excludes_own_reservation_on_retry() {
        let service = setup_receipt_service_with_receipts(vec![(1, 100)]).await;
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let redemption_id: IssuerRedemptionRequestId =
            "red-00000001".parse().unwrap();

        service
            .reserve_burn(vault, redemption_id.clone(), vec![burn(1, 100)])
            .await
            .unwrap();

        // Another redemption sees receipt 1 fully reserved.
        let other = service
            .for_burn(
                vault,
                &"red-00000002".parse().unwrap(),
                make_shares(100),
                make_shares(0),
            )
            .await;
        assert!(matches!(
            other,
            Err(BurnTrackingError::InsufficientBalance { .. })
        ));

        // The same redemption re-planning excludes its own reservation.
        let plan = service
            .for_burn(vault, &redemption_id, make_shares(100), make_shares(0))
            .await
            .unwrap();
        assert_eq!(plan.allocations[0].burn_amount, make_shares(100));
    }

    use chrono::{TimeZone, Utc};
    use proptest::prelude::*;
    use rust_decimal::Decimal;

    use crate::Quantity;
    use crate::mint::TokenizationRequestId;
    use crate::mint::tests::arb_issuer_request_id;
    use crate::test_utils::LocalEvm;
    use crate::tokenized_asset::UnderlyingSymbol;
    use crate::vault::ReceiptInformation;

    fn make_receipt_info(
        issuer_request_id: &IssuerMintRequestId,
    ) -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-test"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(100, 0)),
            Utc::now(),
            None,
        )
    }

    prop_compose! {
        fn arb_mint_receipt_information()(
            tok_id in "[a-zA-Z0-9_-]{1,64}",
            issuer_request_id in arb_issuer_request_id(),
            symbol in "[A-Z]{1,5}",
            qty in 0i64..1_000_000_000i64,
            timestamp_secs in 0i64..2_000_000_000i64,
            notes in proptest::option::of("[a-zA-Z0-9 ]{0,100}"),
        ) -> ReceiptInformation {
            ReceiptInformation::new(
                TokenizationRequestId::new(tok_id),
                issuer_request_id,
                UnderlyingSymbol::new(symbol),
                Quantity(Decimal::new(qty, 2)),
                Utc.timestamp_opt(timestamp_secs, 0).unwrap(),
                notes,
            )
        }
    }

    proptest! {
        #[test]
        fn encode_then_determine_source_preserves_issuer_request_id(
            receipt_info in arb_mint_receipt_information()
        ) {
            let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();
            let (source, parsed_info) = determine_source(&encoded);

            prop_assert!(
                matches!(&source, ReceiptSource::Itn { issuer_request_id }
                    if issuer_request_id == &receipt_info.issuer_request_id),
                "Expected Itn source with matching issuer_request_id, got {:?}", source
            );
            prop_assert_eq!(
                parsed_info,
                Some(receipt_info),
                "Decoded receipt information should round-trip through Rain metadata"
            );
        }
    }

    #[tokio::test]
    async fn encode_then_determine_source_roundtrips_through_anvil() {
        let evm = LocalEvm::new().await.unwrap();

        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();

        let original_issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-anvil-test"),
            original_issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity(Decimal::new(10050, 2)),
            chrono::Utc::now(),
            Some("Anvil integration test".to_string()),
        );

        let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();
        let amount = U256::from(100) * U256::from(10).pow(U256::from(18));

        let (_receipt_id, _shares, returned_info) = evm
            .mint_directly_with_info(
                amount,
                evm.wallet_address,
                encoded.clone(),
            )
            .await
            .unwrap();

        assert_eq!(
            returned_info, encoded,
            "Returned receiptInformation should match what was sent"
        );

        let (source, parsed_info) = determine_source(&returned_info);
        assert!(parsed_info.is_some());
        match source {
            ReceiptSource::Itn { issuer_request_id } => {
                assert_eq!(
                    issuer_request_id.to_string(),
                    original_issuer_request_id.to_string(),
                    "Extracted issuer_request_id should match original"
                );
            }
            ReceiptSource::External => {
                panic!("Expected Itn source, got External");
            }
        }
    }

    #[tokio::test]
    async fn test_reconcile_matching_balance_emits_no_events() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply_event(event);
        }

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(100),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "Reconcile with matching balance should emit no events, got {events:?}"
        );
    }

    #[tokio::test]
    async fn test_reconcile_decreased_balance_emits_balance_reconciled() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply_event(event);
        }

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(50),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled {
                receipt_id,
                previous_balance,
                on_chain_balance,
            } if *receipt_id == make_receipt_id(42)
                && *previous_balance == make_shares(100)
                && *on_chain_balance == make_shares(50)
        ));
    }

    #[tokio::test]
    async fn test_reconcile_zero_balance_emits_balance_reconciled_and_depleted()
    {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(100),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply_event(event);
        }

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled {
                on_chain_balance, ..
            } if on_chain_balance.is_zero()
        ));
        assert!(matches!(
            &events[1],
            ReceiptInventoryEvent::Depleted { receipt_id }
            if *receipt_id == make_receipt_id(42)
        ));
    }

    #[tokio::test]
    async fn test_reconcile_unknown_receipt_emits_no_events() {
        let aggregate = ReceiptInventory::default();

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(99),
                    on_chain_balance: make_shares(0),
                },
                &(),
            )
            .await
            .unwrap();

        assert!(
            events.is_empty(),
            "Reconcile for unknown receipt should emit no events, got {events:?}"
        );
    }

    #[tokio::test]
    async fn test_reconcile_increased_balance_emits_reconciled_event() {
        let mut aggregate = ReceiptInventory::default();
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let events = aggregate
            .transition(
                discover_receipt_cmd(
                    make_receipt_id(42),
                    make_shares(50),
                    1000,
                    tx_hash,
                ),
                &(),
            )
            .await
            .unwrap();

        for event in events {
            aggregate.apply_event(event);
        }

        let events = aggregate
            .transition(
                ReceiptInventoryCommand::ReconcileBalance {
                    receipt_id: make_receipt_id(42),
                    on_chain_balance: make_shares(100),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0],
            ReceiptInventoryEvent::BalanceReconciled {
                previous_balance,
                on_chain_balance,
                ..
            } if *previous_balance == make_shares(50) && *on_chain_balance == make_shares(100)
        ));

        for event in events {
            aggregate.apply_event(event);
        }

        let receipts = aggregate.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].available_balance, make_shares(100));
    }

    #[tokio::test]
    async fn test_register_minted_receipt_makes_receipt_available_for_burn() {
        let store = setup_store().await;
        let service = CqrsReceiptService::new(store.clone());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = make_receipt_info(&issuer_request_id);
        let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();

        service
            .register_minted_receipt(MintedReceiptParams {
                vault,
                receipt_id: make_receipt_id(1),
                shares: make_shares(100),
                block_number: 5000,
                tx_hash,
                receipt_info,
                receipt_info_bytes: encoded,
            })
            .await
            .expect("Registration should succeed");

        let plan = service
            .for_burn(
                vault,
                &planning_redemption(),
                make_shares(50),
                make_shares(0),
            )
            .await
            .expect("Burn planning should succeed with registered receipt");

        assert_eq!(plan.total_burn, make_shares(50));
        assert_eq!(plan.allocations.len(), 1);
    }

    #[tokio::test]
    async fn test_register_minted_receipt_is_findable_by_issuer_request_id() {
        let store = setup_store().await;
        let service = CqrsReceiptService::new(store.clone());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = make_receipt_info(&issuer_request_id);
        let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();

        service
            .register_minted_receipt(MintedReceiptParams {
                vault,
                receipt_id: make_receipt_id(42),
                shares: make_shares(200),
                block_number: 6000,
                tx_hash,
                receipt_info,
                receipt_info_bytes: encoded,
            })
            .await
            .expect("Registration should succeed");

        let found = service
            .find_by_issuer_request_id(&vault, &issuer_request_id)
            .await
            .expect("Lookup should succeed");

        let receipt =
            found.expect("Receipt should be found by issuer_request_id");
        assert_eq!(receipt.receipt_id, U256::from(42));
        assert_eq!(receipt.tx_hash, tx_hash);
        assert_eq!(receipt.shares, U256::from(200));
        assert_eq!(receipt.block_number, 6000);
    }

    #[tokio::test]
    async fn test_register_minted_receipt_is_idempotent() {
        let store = setup_store().await;
        let service = CqrsReceiptService::new(store.clone());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        for _ in 0..2 {
            let issuer_request_id = IssuerMintRequestId::random();
            let receipt_info = make_receipt_info(&issuer_request_id);
            let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();

            service
                .register_minted_receipt(MintedReceiptParams {
                    vault,
                    receipt_id: make_receipt_id(7),
                    shares: make_shares(500),
                    block_number: 7000,
                    tx_hash,
                    receipt_info,
                    receipt_info_bytes: encoded,
                })
                .await
                .expect("Registration should succeed (idempotent)");
        }

        let inventory = load_inventory(&store, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();

        assert_eq!(
            receipts.len(),
            1,
            "Duplicate registration should not create duplicate receipts"
        );
    }

    #[tokio::test]
    async fn test_register_minted_receipt_stores_receipt_info() {
        let store = setup_store().await;
        let service = CqrsReceiptService::new(store.clone());
        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let issuer_request_id = IssuerMintRequestId::random();
        let receipt_info = make_receipt_info(&issuer_request_id);
        let encoded = receipt_info.encode(Some(TEST_OA_SCHEMA)).unwrap();

        service
            .register_minted_receipt(MintedReceiptParams {
                vault,
                receipt_id: make_receipt_id(42),
                shares: make_shares(100),
                block_number: 5000,
                tx_hash,
                receipt_info: receipt_info.clone(),
                receipt_info_bytes: encoded,
            })
            .await
            .expect("Registration should succeed");

        let inventory = load_inventory(&store, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].receipt_info,
            Some(receipt_info),
            "receipt_info should be stored by register_minted_receipt"
        );
    }

    /// Regression: pre-event-sorcery snapshot payloads must be cleared before
    /// `StoreBuilder::build` projection catch-up.
    #[tokio::test]
    async fn pre_lifecycle_snapshot_cleared_before_store_build() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        let vault = address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let aggregate_id = format!("{vault:#x}");

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'SchemaRegistry',
                'schema',
                1,
                'SchemaRegistryEvent::VersionUpdated',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(
            serde_json::json!({
                "VersionUpdated": { "name": "ReceiptInventory", "version": 1 }
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "
            INSERT INTO snapshots (
                aggregate_type,
                aggregate_id,
                last_sequence,
                snapshot_version,
                payload,
                timestamp
            )
            VALUES (
                'ReceiptInventory',
                ?,
                1,
                0,
                ?,
                strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            )
            ",
        )
        .bind(aggregate_id.as_str())
        .bind(
            serde_json::json!({
                "receipts": {},
                "itn_receipts": {},
            })
            .to_string(),
        )
        .execute(&pool)
        .await
        .unwrap();

        prepare_event_sourced_startup::<ReceiptInventory>(&pool).await.unwrap();
        StoreBuilder::<ReceiptInventory>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let stale_snapshot_count: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM snapshots
            WHERE aggregate_type = 'ReceiptInventory'
              AND aggregate_id = ?
            ",
        )
        .bind(aggregate_id.as_str())
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            stale_snapshot_count, 0,
            "Startup must clear incompatible ReceiptInventory snapshots"
        );
    }
}
