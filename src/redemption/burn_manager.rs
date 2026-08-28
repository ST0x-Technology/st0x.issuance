use alloy::primitives::{Address, B256, U256};
use apalis_sqlite::SqlitePool;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::job::{ConfirmBurnJob, SubmitBurnJob};
use super::view::{
    RedemptionView, RedemptionViewError, find_burn_failed, find_burning,
};
use super::{
    BurnExternalTxId, BurnParams, BurnRecoveryAction, ExistingBurnProof,
    IssuerRedemptionRequestId, Redemption, RedemptionCommand, RedemptionError,
    RedemptionEvent, VaultFailure, next_burn_retry_external_tx_id_from_history,
    vault_error_to_redemption,
};
use crate::Quantity;
use crate::burn_excess::has_unresolved_excess_burn_intent;
use crate::config::{VaultMode, VaultModeKind};
use crate::jobs::{JobQueue, QueuePushError, job_type};
use crate::mint::QuantityConversionError;
use crate::mint::recovery::release_terminal_job;
use crate::receipt_inventory::{
    BurnPlan, BurnTrackingError, ReceiptRegistrationError, ReceiptService,
    Shares,
};
use crate::redemption::force_complete::{
    ForceCompleteRefusal, bind_verified_burns,
};
use crate::redemption::{
    BurnFailureClassification, BurnRecord, RedemptionMetadata,
    has_unresolved_signer_intent,
};
use crate::tokenized_asset::view::{TokenizedAssetViewError, find_vault};
use crate::tokenized_asset::{Network, UnderlyingSymbol};
use crate::vault::{
    BurnRequestOrigin, BurnTxStatus, BurnVerification, MultiBurnEntry,
    MultiBurnParams, NetworkVaultServices, OrchestratorBurnParams,
    OrchestratorBurnReadiness, SendableTxWithHash, TxId,
    UnconfiguredNetworkError, VaultError, VaultService,
};

pub(crate) const MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Copy)]
struct BurnRecoveryBudget {
    attempts: u32,
    exhausted: bool,
    last_transaction: Option<(B256, u64)>,
}

/// Shares the bot wallet must hold on-chain before a recovery-driven burn can
/// proceed. Vault-direct's multicall moves both the burned shares and the
/// dust in one transaction, so both must be present; the orchestrator burns
/// only `burn_shares` and never moves the dust (it stays in the bot wallet).
const fn required_recovery_shares(
    burn_mode: VaultMode,
    burn_shares: U256,
    total_with_dust: U256,
) -> U256 {
    match burn_mode {
        VaultMode::VaultDirect => total_with_dust,
        VaultMode::Orchestrator { .. } => burn_shares,
    }
}

fn recovery_burn_entries(planned_burns: &[BurnRecord]) -> Vec<MultiBurnEntry> {
    planned_burns
        .iter()
        .map(|burn| MultiBurnEntry {
            receipt_id: burn.receipt_id,
            burn_shares: burn.shares_burned,
            receipt_info: None,
            receipt_info_bytes: None,
        })
        .collect()
}

/// A definitive (release-eligible) burn-confirmation failure, grouped for
/// [`BurnManager::record_definitive_confirm_failure`].
struct DefinitiveConfirmFailure<'failure> {
    vault: Address,
    is_orchestrator: bool,
    classification: &'failure BurnFailureClassification,
    error: &'failure str,
    tx_id: &'failure TxId,
    planned_burns: &'failure [BurnRecord],
}

/// The persisted-burn recovery inputs pulled from a `BurnSubmitted` /
/// `BurnIntended` aggregate state, grouped for
/// [`BurnManager::recover_persisted_burn`].
struct PersistedBurnRecovery<'state> {
    metadata: &'state RedemptionMetadata,
    planned_burns: &'state [BurnRecord],
    sendable_tx: &'state SendableTxWithHash,
    external_tx_id: Option<BurnExternalTxId>,
    has_submitted: bool,
    alpaca_quantity: &'state Quantity,
}

/// Outcome of recovering a single redemption stuck in a burning state.
///
/// Recovery can legitimately finish without executing a burn — the redemption
/// may have already advanced, or the bot's on-chain balance may be too low to
/// burn safely. Callers must distinguish these no-ops from an actual burn so
/// they don't report success while the redemption is still unresolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// A fresh burn was submitted on-chain for a `Burning` redemption.
    Executed,
    /// Recovery enqueued a durable burn job (`SubmitBurnJob` or
    /// `ConfirmBurnJob`) that broadcasts or confirms the burn.
    EnqueuedBurnJob,
    /// Burn skipped: the bot's on-chain balance is insufficient, so the burn
    /// likely already landed but was never recorded. Needs manual review.
    SkippedManualIntervention,
    /// Orchestrator burn deferred: the wallet's balance is below the burn
    /// amount, and nothing was ever submitted (persist-before-broadcast), so
    /// a prior unrecorded burn is impossible. The redemption stays `Burning`
    /// and the next recovery pass retries automatically once the wallet is
    /// funded — an ops funding action, never the force-complete/close
    /// runbook.
    DeferredUnderfunded,
    /// The redemption already advanced past `Burning`/`BurnSubmitted`; there
    /// was nothing to burn.
    AlreadyAdvanced,
}

/// Orchestrates the on-chain burning process in response to
/// `AlpacaJournalCompleted` events.
///
/// The manager prepares and signs the burn, persists the intention, then
/// enqueues the durable `SubmitBurnJob` and `ConfirmBurnJob`. Those jobs call
/// back into `submit_intended_burn` and `confirm_submitted_burn`, which perform
/// the vault I/O outside any aggregate transition and record each outcome
/// through a pure command (`RecordBurnTxSubmitted`, `RecordBurnConfirmed`).
///
/// On burn failure, the manager issues a `RecordBurnFailure` command to record
/// the error.
#[derive(Clone)]
pub(crate) struct BurnManager {
    /// Per-network vault services and chain ids for recovery paths.
    vaults: NetworkVaultServices,
    view_pool: Pool<Sqlite>,
    store: Arc<Store<Redemption>>,
    receipt_service: Arc<dyn ReceiptService>,
    bot_wallet: Address,
    automatic_recovery_lock: Arc<Mutex<()>>,
    apalis_pool: SqlitePool,
}

impl BurnManager {
    /// Creates a new burn manager.
    ///
    /// # Arguments
    ///
    /// * `vaults` - Per-network vault services and chain ids for recovery paths
    /// * `view_pool` - Database pool for querying views
    /// * `store` - Event-sorcery store for dispatching commands and loading
    ///   aggregate state during recovery
    /// * `receipt_service` - Service for finding receipts to burn
    /// * `bot_wallet` - Bot's wallet address that owns both shares and receipts
    pub(crate) fn new(
        vaults: NetworkVaultServices,
        view_pool: Pool<Sqlite>,
        store: Arc<Store<Redemption>>,
        receipt_service: Arc<dyn ReceiptService>,
        bot_wallet: Address,
        apalis_pool: SqlitePool,
    ) -> Self {
        Self {
            vaults,
            view_pool,
            store,
            receipt_service,
            bot_wallet,
            apalis_pool,
            automatic_recovery_lock: Arc::new(Mutex::new(())),
        }
    }

    fn vault_for(
        &self,
        network: Network,
    ) -> Result<&Arc<dyn VaultService>, UnconfiguredNetworkError> {
        self.vaults.service(network)
    }

    fn chain_id_for(
        &self,
        network: Network,
    ) -> Result<u64, UnconfiguredNetworkError> {
        self.vaults.chain_id(network)
    }

    /// Terminalizes a redemption whose network has no configured vault
    /// service by recording a burn failure. Returns `Ok` once the failure is
    /// persisted — the redemption is handled (as `BurnFailed`), not stuck, so
    /// callers report the recovery as executed rather than failed.
    async fn record_burn_failure_for_unconfigured_network(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
        tx_id: Option<TxId>,
        planned_burns: Vec<super::BurnRecord>,
    ) -> Result<(), BurnManagerError> {
        let error =
            format!("No vault service configured for network {network}");

        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
            %network,
            "{error}"
        );

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error,
                    tx_id,
                    planned_burns,
                    classification: BurnFailureClassification::Unclassified,
                },
            )
            .await?;

        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn new_for_tests(
        vault_service: Arc<dyn VaultService>,
        view_pool: Pool<Sqlite>,
        store: Arc<Store<Redemption>>,
        receipt_service: Arc<dyn ReceiptService>,
        bot_wallet: Address,
        receipt_chain_id: u64,
        apalis_pool: SqlitePool,
    ) -> Self {
        Self {
            vaults: NetworkVaultServices::with_single_vault(
                Network::Base,
                receipt_chain_id,
                vault_service,
            ),
            view_pool,
            store,
            receipt_service,
            bot_wallet,
            apalis_pool,
            automatic_recovery_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Runs one complete automatic recovery pass for every unresolved burn
    /// state. Startup and the periodic reconciler share this entry point so a
    /// reverted transaction that advances from `Burning` to `BurnFailed` is
    /// retried in the same pass.
    pub(crate) async fn recover_unresolved_burns(&self) {
        self.recover_burning_redemptions().await;
        self.recover_burn_failed_redemptions().await;
    }

    /// Recovers redemptions stuck in the `Burning` state at startup.
    ///
    /// Queries the view for all redemptions in `Burning` state and resumes
    /// the burn process for each. This handles cases where the bot crashed
    /// after Alpaca journal completion but before burn was executed.
    async fn recover_burning_redemptions(&self) {
        let stuck_redemptions = match find_burning(&self.view_pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(target: "redemption", error = %err, "Failed to query for stuck Burning redemptions");
                return;
            }
        };

        if stuck_redemptions.is_empty() {
            debug!(target: "redemption", "No Burning redemptions to recover");
            return;
        }

        info!(target: "redemption", count = stuck_redemptions.len(),
            "Recovering stuck Burning redemptions"
        );

        for (issuer_request_id, _view) in stuck_redemptions {
            if let Err(err) =
                self.recover_single_burning(&issuer_request_id).await
            {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to recover Burning redemption"
                );
            }
        }
    }

    /// Recovers redemptions stuck in the `BurnFailed` state at startup.
    ///
    /// Queries the view for all redemptions where burn failed and retries
    /// the burn process for each using metadata preserved in the view.
    async fn recover_burn_failed_redemptions(&self) {
        let failed_redemptions = match find_burn_failed(&self.view_pool).await {
            Ok(redemptions) => redemptions,
            Err(err) => {
                error!(target: "redemption", error = %err, "Failed to query for BurnFailed redemptions");
                return;
            }
        };

        if failed_redemptions.is_empty() {
            debug!(target: "redemption", "No BurnFailed redemptions to recover");
            return;
        }

        info!(target: "redemption", count = failed_redemptions.len(),
            "Recovering BurnFailed redemptions"
        );

        for (issuer_request_id, view) in failed_redemptions {
            if let Err(err) =
                self.recover_single_burn_failed(&issuer_request_id, &view).await
            {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to recover BurnFailed redemption"
                );
            }
        }
    }

    pub(crate) async fn recover_burning_redemption(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        self.recover_single_burning(issuer_request_id).await
    }

    /// Admin-terminalizes a redemption stuck in
    /// `Burning`/`BurnIntended`/`BurnSubmitted` whose
    /// burn already landed on-chain but was never recorded (e.g. a crash
    /// between the burn and `TokensBurned`).
    ///
    /// Verifies the operator-supplied `burn_tx_hash` on-chain — the receipt
    /// must have succeeded and contain a real `Transfer(bot_wallet -> 0x0)` of
    /// the vault's shares — before recording the proving terminal event and
    /// transitioning the redemption to `Completed`. The held receipt
    /// reservation is then settled (mirror reduced), exactly as a normal burn
    /// completion would. Returns the on-chain verification so the caller can
    /// report the proven block number and burned shares.
    ///
    /// The supplied hash must match this redemption's non-empty persisted exact
    /// transaction before on-chain verification. Ambiguous or legacy states
    /// without that identity must be resolved via `CloseRedemption` after
    /// off-chain reconciliation — they are never silently force-completed.
    pub(crate) async fn force_complete_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        reason: String,
        acknowledged_unresolved_burn_tx_hash: Option<B256>,
    ) -> Result<BurnVerification, BurnManagerError> {
        let redemption =
            self.store.load(issuer_request_id).await?.ok_or_else(|| {
                BurnManagerError::InvalidAggregateState {
                    current_state: "Uninitialized".to_string(),
                }
            })?;

        let persisted_burn_tx = redemption.persisted_burn_tx()?;
        persisted_burn_tx.validate_for_owner(self.bot_wallet)?;
        let acknowledged_unresolved_burn_tx_hash = redemption
            .validate_force_complete_burn_hash(
                burn_tx_hash,
                acknowledged_unresolved_burn_tx_hash,
            )?;

        let (metadata, planned_burns, alpaca_quantity) = match &redemption {
            // Burning carries no persisted transaction, so the
            // `persisted_burn_tx` guard above already rejected it; the arm
            // exists only to keep the state match exhaustive.
            Redemption::Burning { metadata, alpaca_quantity, .. } => {
                (metadata, &[][..], alpaca_quantity)
            }
            Redemption::BurnIntended {
                metadata,
                planned_burns,
                alpaca_quantity,
                ..
            }
            | Redemption::BurnSubmitted {
                metadata,
                planned_burns,
                alpaca_quantity,
                ..
            } => (metadata, planned_burns.as_slice(), alpaca_quantity),
            other => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: aggregate_state_name(other).to_string(),
                });
            }
        };

        let vault = find_vault(
            &self.view_pool,
            &metadata.underlying,
            &metadata.network,
        )
        .await?
        .ok_or_else(|| BurnManagerError::AssetNotFound {
            network: metadata.network,
            underlying: metadata.underlying.clone(),
        })?;

        let verification = self
            .vault_for(metadata.network)?
            .verify_burn_tx(
                vault,
                self.bot_wallet,
                burn_tx_hash,
                metadata.burn_mode.into(),
            )
            .await?;

        // SPEC "ForceCompleteBurn": the proving transaction's signer nonce
        // must equal the persisted transaction's nonce — an alternate proof
        // must be the mined replacement at that exact nonce, ensuring the
        // acknowledged transaction can never land and another redemption's
        // same-vault burn can never be used as proof.
        if verification.nonce != persisted_burn_tx.nonce {
            return Err(BurnManagerError::ForceCompleteNonceMismatch {
                proof_nonce: verification.nonce,
                persisted_nonce: persisted_burn_tx.nonce,
            });
        }

        // Bind the proof to this redemption's persisted burn semantics: the
        // per-receipt plan for vault-direct (the same rule the offline CLI
        // enforces), and the burned amount plus transfer-free shape for
        // orchestrator mode, which has no receipt plan — the amount is its
        // only binding, and an orchestrator burn moves nothing besides the
        // pull-and-burn legs (dust is retained, never returned on-chain).
        match metadata.burn_mode {
            VaultMode::VaultDirect => {
                bind_verified_burns(planned_burns, &verification.burns)?;
            }
            VaultMode::Orchestrator { .. } => {
                let required_shares =
                    alpaca_quantity.to_u256_with_18_decimals()?;
                if verification.shares_burned != required_shares {
                    return Err(
                        BurnManagerError::ForceCompleteAmountMismatch {
                            proof_shares: verification.shares_burned,
                            required_shares,
                        },
                    );
                }
                if let Some(stray) = verification.share_transfers.first() {
                    return Err(BurnManagerError::ForceCompleteStrayTransfer {
                        recipient: stray.recipient,
                        shares: stray.shares,
                    });
                }
            }
        }

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            burn_tx_hash = ?burn_tx_hash,
            persisted_burn_tx_hash = ?persisted_burn_tx.hash,
            acknowledged_unresolved_burn_tx_hash = ?acknowledged_unresolved_burn_tx_hash,
            block_number = verification.block_number,
            shares_burned = %verification.shares_burned,
            "Force-completing stuck Burning redemption: burn verified on-chain"
        );

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::ForceCompleteBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    burn_tx_hash,
                    block_number: verification.block_number,
                    reason,
                    acknowledged_unresolved_burn_tx_hash,
                },
            )
            .await?;

        // Orchestrator redemptions never reserved receipts, so there is
        // nothing to settle and the receipt service must not be touched.
        let chain_id = self.chain_id_for(metadata.network)?;
        if matches!(metadata.burn_mode, VaultMode::VaultDirect) {
            self.settle_reserved_burn(chain_id, vault, issuer_request_id).await;
        }

        Ok(verification)
    }

    /// Resolves receipt reservations left dangling by a missed settlement —
    /// e.g. a crash between burn confirmation and settlement, which
    /// reconciliation cannot heal because a landed-but-unsettled burn sits
    /// inside the reconcile no-op band.
    ///
    /// Only a `Completed` redemption's reservation is settled (mirror reduced).
    /// Every other state is left in place: a definitive failure already released
    /// its reservation in the live/recovery paths, so a reservation surviving on
    /// a `Failed`/`Closed` redemption is from an *ambiguous* failure whose burn
    /// may still have landed — releasing it would over-credit inventory and risk
    /// a duplicate burn. In-flight redemptions are owned by the normal flow or
    /// burn recovery. Runs at startup after redemption recovery so in-flight
    /// `BurnSubmitted` reservations have already been confirmed-and-settled (or
    /// left for the ambiguous case) by then.
    pub(crate) async fn recover_stuck_reservations(
        &self,
        vaults: &[(u64, Address)],
    ) {
        let mut stuck: Vec<(u64, Address, IssuerRedemptionRequestId)> =
            Vec::new();

        for &(chain_id, vault) in vaults {
            match self
                .receipt_service
                .reserved_redemptions(chain_id, vault)
                .await
            {
                Ok(redemptions) => {
                    stuck.extend(
                        redemptions.into_iter().map(|id| (chain_id, vault, id)),
                    );
                }
                Err(error) => {
                    warn!(target: "redemption", %vault, %error,
                        "Failed to list reserved redemptions during reservation recovery"
                    );
                }
            }
        }

        if stuck.is_empty() {
            debug!(target: "redemption", "No reservations to recover");
            return;
        }

        info!(target: "redemption", count = stuck.len(),
            "Recovering reservations against redemption terminal state"
        );

        for (chain_id, vault, issuer_request_id) in stuck {
            if let Err(err) = self
                .resolve_stuck_reservation(chain_id, vault, &issuer_request_id)
                .await
            {
                warn!(target: "redemption", chain_id, vault = %vault,
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to resolve stuck reservation"
                );
            }
        }
    }

    async fn resolve_stuck_reservation(
        &self,
        chain_id: u64,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<(), BurnManagerError> {
        use Redemption::Completed;

        // A reservation for a redemption with no events is anomalous (e.g.
        // pruned history). Leave it for manual review rather than releasing
        // blindly against an unknown on-chain outcome.
        let Some(aggregate) = self.store.load(issuer_request_id).await? else {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Reservation held for an unknown redemption; left for manual review"
            );
            return Ok(());
        };

        match aggregate {
            // The burn confirmed on-chain but settlement was missed (e.g. a
            // crash in the confirm->settle window). Settle to reduce the mirror.
            Completed { .. } => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Settling reservation for completed redemption"
                );
                self.settle_reserved_burn(chain_id, vault, issuer_request_id)
                    .await;
            }
            // All other states are LEFT in place. A definitive failure already
            // released its reservation in the live/recovery paths (gated on
            // `should_release_reserved_burn`), so a reservation surviving on a
            // `Failed`/`Closed` redemption here is from an *ambiguous* failure
            // whose burn may still have landed. Releasing it would over-credit
            // inventory and risk a duplicate burn; leaving it keeps
            // availability conservatively correct until on-chain settlement or
            // manual intervention resolves it. In-flight states are owned by
            // the live flow / burn recovery.
            _ => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Leaving reservation for ambiguous or in-flight redemption"
                );
            }
        }

        Ok(())
    }

    async fn recover_single_burn_failed(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        view: &RedemptionView,
    ) -> Result<(), BurnManagerError> {
        let replacement_already_reserved =
            self.failed_replacement_already_reserved(issuer_request_id).await?;
        if !replacement_already_reserved
            && !self.recovery_budget_available(issuer_request_id, None).await?
        {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Skipping BurnFailed redemption with exhausted automatic recovery budget"
            );
            return Ok(());
        }

        let RedemptionView::BurnFailed {
            underlying,
            network,
            token,
            wallet,
            quantity,
            alpaca_quantity,
            dust_quantity,
            tx_hash,
            block_number,
            detected_at,
            called_at,
            alpaca_journal_completed_at,
            tokenization_request_id,
            tx_id,
            burn_mode,
            classification,
            ..
        } = view
        else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "View not in BurnFailed state, skipping"
            );
            return Ok(());
        };

        // Typed classifications are never auto-retried: InsufficientReceipts
        // and AllowanceInsufficient are deterministic until an operator acts,
        // and the logic-mismatch halt resolves environment-wide. Skip before
        // any budget bookkeeping so exhaustion accounting is untouched; the
        // admin re-drive (`ResumeBurn`) is the only way forward.
        if *classification != BurnFailureClassification::Unclassified {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                classification = ?classification,
                "Skipping non-retryable classified burn failure; manual \
                 recovery required"
            );
            return Ok(());
        }

        let vault = find_vault(&self.view_pool, underlying, network)
            .await?
            .ok_or_else(|| BurnManagerError::AssetNotFound {
                underlying: underlying.clone(),
                network: *network,
            })?;

        let vault_service = self.vault_for(*network)?;

        // If a tx was already submitted before failure, inspect it before
        // deciding whether to confirm, wait, or submit a replacement. The
        // confirm path is mode-scoped: each mode confirms via its own
        // on-chain shape and records its own success event.
        let retry_external_tx_id = if let Some(persisted_tx_id) = tx_id {
            return match burn_mode {
                VaultMode::VaultDirect => {
                    self.recover_burn_failed_with_existing_tx(
                        issuer_request_id,
                        network,
                        vault,
                        persisted_tx_id,
                        dust_quantity,
                    )
                    .await
                }
                VaultMode::Orchestrator { .. } => {
                    self.recover_orchestrator_burn_failed_with_existing_tx(
                        *network,
                        issuer_request_id,
                        persisted_tx_id,
                        alpaca_quantity,
                        dust_quantity,
                    )
                    .await
                }
            };
        } else {
            self.next_burn_retry_external_tx_id(issuer_request_id, tx_hash)
                .await?
        };

        let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        let total_shares = burn_shares
            .checked_add(dust_shares)
            .ok_or(BurnManagerError::SharesOverflow)?;

        // Check on-chain balance before attempting burn. If the bot has insufficient
        // shares, the burn likely already succeeded on-chain but we crashed before
        // recording it (e.g., RPC timeout via VaultError::PendingTransaction).
        // Skip this redemption to avoid double-burning. Manual intervention required.
        let on_chain_balance =
            vault_service.get_share_balance(vault, self.bot_wallet).await?;

        let required_shares =
            required_recovery_shares(*burn_mode, burn_shares, total_shares);

        if on_chain_balance < required_shares {
            match burn_mode {
                // Vault-direct reaches this path with no captured tx id, but a
                // broadcast that landed without recording its id is possible,
                // so a low balance is read as "the burn likely already
                // succeeded on-chain": auto-fail to avoid double-burning and
                // leave any reservation in place (releasing would over-credit
                // inventory against a stale-high mirror). Manual intervention
                // resolves it.
                VaultMode::VaultDirect => {
                    let reason = format!(
                        "On-chain balance insufficient for BurnFailed recovery: \
                         balance={on_chain_balance}, required={required_shares}"
                    );

                    info!(target: "redemption", issuer_request_id = %issuer_request_id,
                        on_chain_balance = %on_chain_balance,
                        required_shares = %required_shares,
                        "Auto-failing BurnFailed redemption with insufficient on-chain balance"
                    );

                    self.store
                        .send(
                            issuer_request_id,
                            RedemptionCommand::MarkFailed {
                                issuer_request_id: issuer_request_id.clone(),
                                reason,
                            },
                        )
                        .await?;

                    return Ok(());
                }
                // Orchestrator burns move shares only atomically inside
                // `burn()`, and this path runs with no submitted tx, so a
                // prior unrecorded burn for this redemption is impossible — a
                // low balance means the wallet is underfunded or its shares
                // were consumed elsewhere. Auto-failing would record a false
                // "burn landed" claim, so defer instead: the redemption stays
                // BurnFailed (visible in /admin/stuck) and the next pass
                // retries once the wallet is funded.
                VaultMode::Orchestrator { .. } => {
                    // ERROR: funding the wallet is an ops action — the retry
                    // loop cannot resolve this on its own.
                    error!(target: "redemption", issuer_request_id = %issuer_request_id,
                        on_chain_balance = %on_chain_balance,
                        burn_shares = %burn_shares,
                        "Insufficient wallet balance for orchestrator burn recovery; \
                         no burn was submitted, so a prior unrecorded burn is \
                         impossible — wallet underfunded or shares consumed \
                         elsewhere; deferring"
                    );

                    return Ok(());
                }
            }
        }

        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
            burn_shares = %burn_shares,
            dust_shares = %dust_shares,
            "Retrying burn for BurnFailed redemption"
        );

        // Step 1: Resume the burn (Failed → Burning) so the standard
        // two-step submit/confirm flow persists the tx ID.
        let metadata = super::RedemptionMetadata {
            issuer_request_id: issuer_request_id.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: *network,
            wallet: *wallet,
            quantity: quantity.clone(),
            detected_tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
            burn_mode: *burn_mode,
        };

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::ResumeBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    metadata,
                    tokenization_request_id: tokenization_request_id.clone(),
                    alpaca_quantity: alpaca_quantity.clone(),
                    dust_quantity: dust_quantity.clone(),
                    called_at: *called_at,
                    alpaca_journal_completed_at: *alpaca_journal_completed_at,
                    external_tx_id: retry_external_tx_id,
                },
            )
            .await?;

        // Step 2: Load the updated aggregate (now in Burning) and use
        // the standard submit → persist tx ID → confirm flow.
        let Some(aggregate) = self.store.load(issuer_request_id).await? else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: "Uninitialized".to_string(),
            });
        };

        self.handle_burning_started(issuer_request_id, &aggregate).await?;

        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
            "Successfully retried burn"
        );

        Ok(())
    }

    async fn failed_replacement_already_reserved(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<bool, BurnManagerError> {
        let aggregate_id = issuer_request_id.to_string();
        let payloads = sqlx::query_scalar::<_, String>(
            "
            SELECT payload
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
            ORDER BY sequence DESC
            LIMIT 2
            ",
        )
        .bind(aggregate_id)
        .fetch_all(&self.view_pool)
        .await?;
        let events = payloads
            .iter()
            .map(|payload| serde_json::from_str::<RedemptionEvent>(payload))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(matches!(
            events.as_slice(),
            [
                RedemptionEvent::BurningFailed { .. },
                RedemptionEvent::BurnRecoveryAttempted {
                    action: BurnRecoveryAction::Replace,
                    ..
                }
            ]
        ))
    }

    /// Recovers a BurnFailed redemption that has a previously submitted
    /// transaction. Tries to confirm the existing transaction rather than resubmitting.
    async fn recover_burn_failed_with_existing_tx(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        network: &Network,
        vault: Address,
        tx_id: &TxId,
        dust_quantity: &Quantity,
    ) -> Result<(), BurnManagerError> {
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            %tx_id,
            "BurnFailed recovery — confirming previously submitted transaction"
        );

        match self.vault_for(*network)?.confirm_burn(tx_id, dust_shares).await {
            Ok(result) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    tx_hash = %result.tx_hash,
                    "Previously submitted burn confirmed on-chain"
                );

                // Use actual on-chain burn data, not planned amounts —
                // the Rain contract's withdraw math may produce slightly
                // different values than planned (rounding in share ratios).
                let actual_burns: Vec<super::BurnRecord> = result
                    .burns
                    .iter()
                    .map(|burn| super::BurnRecord {
                        receipt_id: burn.receipt_id,
                        shares_burned: burn.shares_burned,
                    })
                    .collect();

                // Safe: BurningFailed always transitions the aggregate to
                // Failed, so RecordExistingBurn (which requires Failed) is valid.
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordExistingBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            tx_id: tx_id.clone(),
                            tx_hash: result.tx_hash,
                            proof: ExistingBurnProof::VaultDirect {
                                burns: actual_burns,
                            },
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                let chain_id = self.chain_id_for(*network)?;
                self.settle_reserved_burn(chain_id, vault, issuer_request_id)
                    .await;

                Ok(())
            }
            Err(err) => {
                if should_release_reserved_burn(&err) {
                    // Confirmed on-chain revert: the tx consumed no receipts,
                    // so it is safe to release the reservation and terminalize.
                    let chain_id = self.chain_id_for(*network)?;
                    self.release_reserved_burn(
                        chain_id,
                        vault,
                        issuer_request_id,
                    )
                    .await;

                    let reason = format!(
                        "Burn transaction confirmation failed for tx {tx_id}: {err}"
                    );

                    self.store
                        .send(
                            issuer_request_id,
                            RedemptionCommand::MarkFailed {
                                issuer_request_id: issuer_request_id.clone(),
                                reason,
                            },
                        )
                        .await?;

                    return Err(BurnManagerError::Vault(err));
                }

                // Non-terminal error (RPC blip, pending-receipt timeout, etc.)
                // — the tx may still be in-flight. Keep the redemption in
                // BurnFailed with tx_id intact so the next recovery pass
                // retries confirm_burn rather than submitting a replacement
                // while the original is still pending, which would risk
                // both transactions mining and double-burning.
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    %tx_id,
                    error = %err,
                    "Failed to confirm previously submitted burn"
                );
                Ok(())
            }
        }
    }

    /// Orchestrator-mode counterpart of `recover_burn_failed_with_existing_tx`.
    /// Confirms a previously submitted `orchestrator.burn()` and records the
    /// outcome as an `OrchestratorBurnRecovered` (via the `RecordExistingBurn`
    /// orchestrator proof). No reservation exists in orchestrator mode, so
    /// none is settled or released.
    async fn recover_orchestrator_burn_failed_with_existing_tx(
        &self,
        network: Network,
        issuer_request_id: &IssuerRedemptionRequestId,
        tx_id: &TxId,
        alpaca_quantity: &Quantity,
        dust_quantity: &Quantity,
    ) -> Result<(), BurnManagerError> {
        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            %tx_id,
            "BurnFailed recovery — confirming previously submitted orchestrator burn"
        );

        match self
            .vaults
            .service(network)?
            .confirm_orchestrator_burn(tx_id)
            .await
        {
            Ok(result) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    tx_hash = %result.tx_hash,
                    "Previously submitted orchestrator burn confirmed on-chain"
                );

                // Bind the confirmed economics to THIS redemption before any
                // terminal command: `shares_burned` must equal the persisted
                // alpaca_quantity in share-wei. (Token and caller are bound
                // transitively — `tx_id` is this redemption's own persisted
                // submission, and confirm cross-checks the `Burned` event
                // against that transaction's mined calldata.) A divergence
                // is an integrity anomaly: leave the redemption in BurnFailed
                // for the operator, never terminalize on it.
                let expected_shares =
                    alpaca_quantity.to_u256_with_18_decimals()?;
                if result.shares_burned != expected_shares {
                    error!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        tx_hash = %result.tx_hash,
                        expected_shares = %expected_shares,
                        shares_burned = %result.shares_burned,
                        "Confirmed orchestrator burn diverges from the \
                         persisted alpaca_quantity; refusing existing-burn \
                         recovery"
                    );
                    return Err(BurnManagerError::Redemption(
                        RedemptionError::OrchestratorAmountMismatch {
                            expected: expected_shares,
                            actual: result.shares_burned,
                        },
                    ));
                }

                // dust_retained is derived from the redemption's own persisted
                // dust_quantity — the orchestrator has no multicall to return
                // it on-chain — matching OrchestratorTokensBurned.
                let dust_retained = dust_quantity.to_u256_with_18_decimals()?;

                // Safe: BurningFailed always transitions the aggregate to
                // Failed, so RecordExistingBurn (which requires Failed) is valid.
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordExistingBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            tx_id: tx_id.clone(),
                            tx_hash: result.tx_hash,
                            proof: ExistingBurnProof::Orchestrator {
                                shares_burned: result.shares_burned,
                                burn_range: result.burn_range,
                                dust_retained,
                            },
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                Ok(())
            }
            Err(err) => {
                if should_release_reserved_burn(&err) {
                    // Definitive on-chain revert. No reservation exists in
                    // orchestrator mode, so there is nothing to release — just
                    // terminalize with the decoded reason.
                    let reason = format!(
                        "Orchestrator burn confirmation failed for tx {tx_id}: {err}"
                    );

                    self.store
                        .send(
                            issuer_request_id,
                            RedemptionCommand::MarkFailed {
                                issuer_request_id: issuer_request_id.clone(),
                                reason,
                            },
                        )
                        .await?;

                    return Err(BurnManagerError::Vault(err));
                }

                // Non-terminal error (RPC blip, pending-receipt timeout): the
                // tx may still be in-flight. Keep the redemption in BurnFailed
                // with tx_id intact so the next recovery pass retries
                // confirmation rather than submitting a replacement while the
                // original is still pending, which would risk double-burning.
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    %tx_id,
                    error = %err,
                    "Failed to confirm previously submitted orchestrator burn"
                );
                Ok(())
            }
        }
    }

    async fn recover_single_burning_shared_inner(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        metadata: &RedemptionMetadata,
        tx_id: &TxId,
        dust_shares: U256,
        planned_burns: &[BurnRecord],
        has_submitted: bool,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let vault = find_vault(
            &self.view_pool,
            &metadata.underlying,
            &metadata.network,
        )
        .await?
        .ok_or_else(|| BurnManagerError::AssetNotFound {
            underlying: metadata.underlying.clone(),
            network: metadata.network,
        })?;

        if has_submitted {
            info!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_id = %tx_id,
                "Recovering BurnSubmitted redemption - enqueuing confirm job"
            );
        } else {
            info!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_id = %tx_id,
                "Recovering BurnIntended redemption - enqueuing confirm job"
            );
        }

        let execution = Self::recovery_confirm_plan(
            metadata.network,
            vault,
            metadata.burn_mode,
            dust_shares,
            planned_burns,
        );
        self.enqueue_confirm_burn(issuer_request_id, execution, tx_id.clone())
            .await?;

        Ok(RecoveryOutcome::EnqueuedBurnJob)
    }

    /// Builds the mode-correct `BurnParams` for a recovery-driven command
    /// from the redemption's persisted anchor — never from live config.
    fn recovery_burn_params(
        &self,
        burn_mode: VaultMode,
        vault: Address,
        planned_burns: &[BurnRecord],
        dust_shares: U256,
        alpaca_quantity: &Quantity,
    ) -> Result<BurnParams, QuantityConversionError> {
        match burn_mode {
            VaultMode::VaultDirect => Ok(BurnParams::VaultDirect {
                vault,
                burns: recovery_burn_entries(planned_burns),
                dust_shares,
                owner: self.bot_wallet,
            }),
            VaultMode::Orchestrator { .. } => Ok(BurnParams::Orchestrator {
                token: vault,
                amount: alpaca_quantity.to_u256_with_18_decimals()?,
                owner: self.bot_wallet,
            }),
        }
    }

    async fn recover_persisted_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        recovery: PersistedBurnRecovery<'_>,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let PersistedBurnRecovery {
            metadata,
            planned_burns,
            sendable_tx,
            external_tx_id,
            has_submitted,
            alpaca_quantity,
        } = recovery;
        let vault_service = self.vault_for(metadata.network)?;
        let wallet_guard = vault_service.lock_wallet().await;
        if !self
            .recovery_budget_available(issuer_request_id, Some(sendable_tx))
            .await?
        {
            debug!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                tx_hash = %sendable_tx.hash,
                "Skipping burn with exhausted automatic recovery budget"
            );
            return Ok(RecoveryOutcome::SkippedManualIntervention);
        }

        let status = vault_service
            .classify_burn_tx(self.bot_wallet, sendable_tx)
            .await?;
        let tx_id = sendable_tx.hash.into();
        if matches!(status, BurnTxStatus::Mined | BurnTxStatus::Reverted) {
            return self
                .recover_single_burning_shared_inner(
                    issuer_request_id,
                    metadata,
                    &tx_id,
                    sendable_tx.dust_shares,
                    planned_burns,
                    has_submitted,
                )
                .await;
        }

        let action = match status {
            BurnTxStatus::StillMineable => BurnRecoveryAction::Rebroadcast,
            BurnTxStatus::ProvablyDead => BurnRecoveryAction::Replace,
            BurnTxStatus::Mined | BurnTxStatus::Reverted => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: "terminal burn classification".to_string(),
                });
            }
        };
        let vault = find_vault(
            &self.view_pool,
            &metadata.underlying,
            &metadata.network,
        )
        .await?
        .ok_or_else(|| BurnManagerError::AssetNotFound {
            underlying: metadata.underlying.clone(),
            network: metadata.network,
        })?;
        if status == BurnTxStatus::ProvablyDead {
            // Network-keyed reservation: one check covers competing burn AND
            // mint intents on this signer's nonce domain, excluding only this
            // redemption's own reservation. BurnExcess is not tracked in
            // `active_signer_intents`, so its intents need their own check.
            let unresolved_intent = has_unresolved_signer_intent(
                &self.view_pool,
                metadata.network,
                Some(issuer_request_id),
            )
            .await?;
            let unresolved_excess =
                has_unresolved_excess_burn_intent(&self.view_pool, None)
                    .await?;
            if unresolved_intent || unresolved_excess {
                drop(wallet_guard);
                debug!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %sendable_tx.hash,
                    unresolved_intent,
                    unresolved_excess,
                    "Deferring dead burn replacement behind another persisted wallet intent"
                );
                return Ok(RecoveryOutcome::SkippedManualIntervention);
            }
        }
        if !self
            .reserve_recovery_attempt(issuer_request_id, sendable_tx, action)
            .await?
        {
            drop(wallet_guard);
            return Ok(RecoveryOutcome::SkippedManualIntervention);
        }

        match status {
            BurnTxStatus::StillMineable => {
                let params = self.recovery_burn_params(
                    metadata.burn_mode,
                    vault,
                    planned_burns,
                    sendable_tx.dust_shares,
                    alpaca_quantity,
                )?;
                let execution = BurnExecutionPlan::from_recovery(
                    metadata.network,
                    params,
                    planned_burns.to_vec(),
                    external_tx_id,
                );
                self.enqueue_submit_burn(issuer_request_id, execution).await?;
            }
            BurnTxStatus::ProvablyDead => {
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::ReplaceDeadBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            owner: self.bot_wallet,
                        },
                    )
                    .await?;
            }
            BurnTxStatus::Mined | BurnTxStatus::Reverted => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: "terminal burn classification".to_string(),
                });
            }
        }

        if status == BurnTxStatus::ProvablyDead {
            self.submit_replacement_after_dead_burn(
                issuer_request_id,
                metadata.burn_mode,
                vault,
                alpaca_quantity,
            )
            .await?;
        }
        drop(wallet_guard);

        info!(target: "redemption",
            issuer_request_id = %issuer_request_id,
            tx_hash = %sendable_tx.hash,
            action = ?action,
            "Automatic burn recovery action accepted"
        );
        Ok(RecoveryOutcome::EnqueuedBurnJob)
    }

    /// Broadcasts the replacement transaction `ReplaceDeadBurn` just
    /// persisted: re-loads the aggregate to pick up the freshly intended
    /// replacement (its plan, signed bytes, and retry `externalTxId`) and
    /// submits it.
    async fn submit_replacement_after_dead_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        burn_mode: VaultMode,
        vault: Address,
        alpaca_quantity: &Quantity,
    ) -> Result<(), BurnManagerError> {
        let Some(Redemption::BurnIntended {
            planned_burns,
            sendable_tx,
            external_tx_id,
            metadata,
            ..
        }) = self.store.load(issuer_request_id).await?
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: "expected replacement BurnIntended".to_string(),
            });
        };

        let params = self.recovery_burn_params(
            burn_mode,
            vault,
            &planned_burns,
            sendable_tx.dust_shares,
            alpaca_quantity,
        )?;
        let execution = BurnExecutionPlan::from_recovery(
            metadata.network,
            params,
            planned_burns,
            external_tx_id,
        );
        self.enqueue_submit_burn(issuer_request_id, execution).await?;

        Ok(())
    }

    async fn recover_single_burning(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let Some(aggregate) = self.store.load(issuer_request_id).await? else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Redemption not found, skipping"
            );
            return Ok(RecoveryOutcome::AlreadyAdvanced);
        };

        match &aggregate {
            // Already submitted to tx but never confirmed — resume polling
            Redemption::BurnSubmitted {
                metadata,
                planned_burns,
                sendable_tx,
                external_tx_id,
                alpaca_quantity,
                ..
            } => {
                if sendable_tx == &SendableTxWithHash::default() {
                    let Redemption::BurnSubmitted {
                        tx_id, dust_quantity, ..
                    } = &aggregate
                    else {
                        return Err(BurnManagerError::InvalidAggregateState {
                            current_state: aggregate_state_name(&aggregate)
                                .to_string(),
                        });
                    };
                    return self
                        .recover_single_burning_shared_inner(
                            issuer_request_id,
                            metadata,
                            tx_id,
                            dust_quantity.to_u256_with_18_decimals()?,
                            planned_burns,
                            true,
                        )
                        .await;
                }
                self.recover_persisted_burn(
                    issuer_request_id,
                    PersistedBurnRecovery {
                        metadata,
                        planned_burns,
                        sendable_tx,
                        external_tx_id: Some(external_tx_id.clone()),
                        has_submitted: true,
                        alpaca_quantity,
                    },
                )
                .await
            }

            Redemption::BurnIntended {
                metadata,
                planned_burns,
                sendable_tx,
                external_tx_id,
                alpaca_quantity,
                ..
            } => {
                self.recover_persisted_burn(
                    issuer_request_id,
                    PersistedBurnRecovery {
                        metadata,
                        planned_burns,
                        sendable_tx,
                        external_tx_id: external_tx_id.clone(),
                        has_submitted: false,
                        alpaca_quantity,
                    },
                )
                .await
            }

            // Still in Burning state — needs full submit + confirm flow
            Redemption::Burning {
                metadata,
                alpaca_quantity,
                dust_quantity,
                external_tx_id,
                ..
            } => {
                let vault = find_vault(
                    &self.view_pool,
                    &metadata.underlying,
                    &metadata.network,
                )
                .await?
                .ok_or_else(|| {
                    BurnManagerError::AssetNotFound {
                        underlying: metadata.underlying.clone(),
                        network: metadata.network,
                    }
                })?;

                // We need to burn alpaca_quantity and transfer dust_quantity
                let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
                let dust_shares = dust_quantity.to_u256_with_18_decimals()?;
                let total_shares_needed = burn_shares
                    .checked_add(dust_shares)
                    .ok_or(BurnManagerError::SharesOverflow)?;

                // Check on-chain balance before attempting burn. If the bot has insufficient
                // shares, the burn likely already succeeded on-chain but we crashed before
                // recording it. Skip this redemption to avoid recording a false failure.
                // Resolve manually via the admin `force-complete` endpoint (records the
                // verified burn tx) for landed burns, or `close` for ambiguous cases.
                let vault_service = match self.vault_for(metadata.network) {
                    Ok(vault_service) => vault_service,
                    Err(UnconfiguredNetworkError { network }) => {
                        return self
                            .record_burn_failure_for_unconfigured_network(
                                issuer_request_id,
                                network,
                                None,
                                vec![],
                            )
                            .await
                            .map(|()| RecoveryOutcome::Executed);
                    }
                };

                let required_shares = required_recovery_shares(
                    metadata.burn_mode,
                    burn_shares,
                    total_shares_needed,
                );

                let on_chain_balance = vault_service
                    .get_share_balance(vault, self.bot_wallet)
                    .await?;

                if on_chain_balance < required_shares {
                    match metadata.burn_mode {
                        // Vault-direct: a crashed broadcast may have landed
                        // without recording, so a low balance is read as "the
                        // burn likely already succeeded". Skip to avoid a false
                        // failure; leave any reservation in place (releasing
                        // would over-credit the stale-high mirror). Resolve via
                        // the admin `force-complete` (landed) or `close`
                        // (ambiguous) endpoint.
                        VaultMode::VaultDirect => {
                            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                                on_chain_balance = %on_chain_balance,
                                burn_shares = %burn_shares,
                                dust_shares = %dust_shares,
                                required_shares = %required_shares,
                                "MANUAL INTERVENTION REQUIRED: On-chain balance insufficient for burn recovery. \
                                 Burn likely already succeeded but was not recorded. \
                                 Skipping to avoid recording false failure."
                            );
                            return Ok(
                                RecoveryOutcome::SkippedManualIntervention,
                            );
                        }
                        // Orchestrator burns move shares only atomically inside
                        // `burn()`, and this arm runs before anything is signed
                        // (persist-before-broadcast), so a prior unrecorded
                        // burn is impossible — a low balance means the wallet
                        // is underfunded or its shares were consumed elsewhere.
                        // Defer: the redemption stays Burning (visible in
                        // /admin/stuck as a stuck Burning entry once past the
                        // threshold; this ERROR log is the signal
                        // distinguishing underfunding from other stuck-Burning
                        // causes) and the next pass retries once the wallet is
                        // funded.
                        VaultMode::Orchestrator { .. } => {
                            error!(target: "redemption", issuer_request_id = %issuer_request_id,
                                on_chain_balance = %on_chain_balance,
                                burn_shares = %burn_shares,
                                "Insufficient wallet balance for orchestrator burn recovery; \
                                 no burn was submitted, so a prior unrecorded burn is \
                                 impossible — wallet underfunded or shares consumed \
                                 elsewhere; deferring"
                            );
                            return Ok(RecoveryOutcome::DeferredUnderfunded);
                        }
                    }
                }

                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    external_tx_id = ?external_tx_id,
                    "Recovering Burning redemption - resuming burn"
                );

                self.handle_burning_started(issuer_request_id, &aggregate)
                    .await?;

                Ok(RecoveryOutcome::EnqueuedBurnJob)
            }

            _ => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Redemption no longer in Burning or BurnSubmitted state, skipping"
                );
                Ok(RecoveryOutcome::AlreadyAdvanced)
            }
        }
    }

    /// Handles a `Burning` state by burning tokens on-chain.
    ///
    /// This method orchestrates the complete on-chain burning flow:
    /// 1. Validates the aggregate is in `Burning` state
    /// 2. Converts quantity to U256 with 18 decimals
    /// 3. Queries for a suitable receipt with sufficient balance
    /// 4. Calls blockchain service to burn tokens
    /// 5. Records success (`RecordBurnSuccess`) or failure (`RecordBurnFailure`) via commands
    ///
    /// # Arguments
    ///
    /// * `issuer_request_id` - ID of the redemption request
    /// * `aggregate` - Current state of the Redemption aggregate (must be `Burning`)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if burning succeeded and `RecordBurnSuccess` command was executed.
    /// Returns `Err(BurnManagerError::Vault)` if burning failed (`RecordBurnFailure`
    /// command is still executed to record the failure).
    ///
    /// # Errors
    ///
    /// * `BurnManagerError::InvalidAggregateState` - Aggregate is not in `Burning` state
    /// * `BurnManagerError::QuantityConversion` - Quantity cannot be converted to U256
    /// * `BurnManagerError::InsufficientBalance` - No receipt with sufficient balance found
    /// * `BurnManagerError::Vault` - Blockchain transaction failed
    /// * `BurnManagerError::Cqrs` - Command execution failed
    /// * `BurnManagerError::Sqlx` - Receipt query failed
    pub(crate) async fn handle_burning_started(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        aggregate: &Redemption,
    ) -> Result<(), BurnManagerError> {
        let (Redemption::Burning {
            metadata,
            alpaca_quantity,
            dust_quantity,
            external_tx_id,
            ..
        }
        | Redemption::BurnIntended {
            metadata,
            alpaca_quantity,
            dust_quantity,
            external_tx_id,
            ..
        }) = aggregate
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let Some(vault) = find_vault(
            &self.view_pool,
            &metadata.underlying,
            &metadata.network,
        )
        .await?
        else {
            let error_msg = format!(
                "No vault configured for underlying asset {} on network {}",
                metadata.underlying, metadata.network
            );

            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                underlying = %metadata.underlying,
                "{error_msg}"
            );

            self.store
                .send(
                    issuer_request_id,
                    RedemptionCommand::RecordBurnFailure {
                        classification: BurnFailureClassification::Unclassified,
                        issuer_request_id: issuer_request_id.clone(),
                        error: error_msg,
                        tx_id: None,
                        planned_burns: vec![],
                    },
                )
                .await?;

            return Err(BurnManagerError::AssetNotFound {
                underlying: metadata.underlying.clone(),
                network: metadata.network,
            });
        };

        // Convert quantities to U256 for on-chain operations
        let burn_shares = alpaca_quantity.to_u256_with_18_decimals()?;
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            underlying = %metadata.underlying,
            alpaca_quantity = %alpaca_quantity,
            dust_quantity = %dust_quantity,
            burn_shares = %burn_shares,
            dust_shares = %dust_shares,
            wallet = %metadata.wallet,
            vault = %vault,
            burn_mode = ?metadata.burn_mode,
            "Starting on-chain burning process with dust handling"
        );

        if let VaultMode::Orchestrator { address: orchestrator } =
            metadata.burn_mode
        {
            return self
                .execute_orchestrator_burn(
                    issuer_request_id,
                    metadata.network,
                    orchestrator,
                    vault,
                    burn_shares,
                    external_tx_id.clone(),
                )
                .await;
        }

        // A retry plans against availability that EXCLUDES this redemption's own
        // prior reservation (see `for_burn`), and `reserve_burn` atomically
        // replaces that reservation — so no separate release-before-plan is
        // needed, and the prior reservation is never returned to global
        // availability where a concurrent redemption could grab it.
        let plan = self
            .plan_burn(
                issuer_request_id,
                metadata.network,
                vault,
                &metadata.underlying,
                burn_shares,
                dust_shares,
            )
            .await?;

        let execution = BurnExecutionPlan::vault_direct(
            metadata.network,
            vault,
            &plan,
            self.bot_wallet,
            external_tx_id.clone(),
        );
        self.execute_burn_and_record_result(
            issuer_request_id,
            metadata.network,
            execution,
        )
        .await
    }

    /// Runs the orchestrator-mode pre-submit gates, then drives the shared
    /// intend→submit→confirm pipeline with no receipt plan. Skips the entire
    /// receipt reserve/settle/release lifecycle — the orchestrator custodies
    /// receipts and walks them on-chain.
    async fn execute_orchestrator_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
        orchestrator: Address,
        token: Address,
        amount: U256,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Result<(), BurnManagerError> {
        match self
            .vaults
            .service(network)?
            .check_orchestrator_burn_readiness(
                orchestrator,
                token,
                self.bot_wallet,
                amount,
            )
            .await?
        {
            OrchestratorBurnReadiness::Ready => {}
            OrchestratorBurnReadiness::AllowanceInsufficient {
                required,
                current,
            } => {
                // Deterministic until ops grants the approval — never
                // auto-retried, and the burn is never submitted.
                error!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    token = %token,
                    orchestrator = %orchestrator,
                    required = %required,
                    current = %current,
                    "Orchestrator burn allowance insufficient; ops must \
                     approve the orchestrator before this redemption can \
                     resume"
                );
                let error_msg = format!(
                    "Orchestrator allowance insufficient: required \
                     {required}, current {current}"
                );
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordBurnFailure {
                            classification:
                                BurnFailureClassification::AllowanceInsufficient,
                            issuer_request_id: issuer_request_id.clone(),
                            error: error_msg.clone(),
                            tx_id: None,
                            planned_burns: vec![],
                        },
                    )
                    .await?;
                return Err(BurnManagerError::Redemption(
                    RedemptionError::Vault {
                        message: error_msg,
                        release_reservation: false,
                        tx_id: None,
                        classification:
                            BurnFailureClassification::AllowanceInsufficient,
                    },
                ));
            }
            OrchestratorBurnReadiness::VaultLogicMismatch => {
                // Orchestrator-wide halt: no submission, no event, and no
                // retry counter advances. The redemption stays in `Burning`
                // and the next recovery pass re-checks health.
                warn!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    token = %token,
                    orchestrator = %orchestrator,
                    "Orchestrator halted (vault logic mismatch); deferring \
                     burn without recording failure"
                );
                return Ok(());
            }
            OrchestratorBurnReadiness::InsufficientReceipts { shortfall } => {
                // Token-global anomaly: the orchestrator's receipt walk
                // cannot cover this burn for ANY redemption of this token.
                // Never submitted, never auto-retried — recovery is a manual
                // EMERGENCY_ROLE action followed by admin ResumeBurn.
                error!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    token = %token,
                    orchestrator = %orchestrator,
                    shortfall = %shortfall,
                    "Orchestrator receipts insufficient to cover burn; \
                     manual EMERGENCY_ROLE recovery required"
                );
                let error_msg = format!(
                    "Orchestrator receipts insufficient: shortfall \
                     {shortfall}"
                );
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordBurnFailure {
                            classification:
                                BurnFailureClassification::InsufficientReceipts {
                                    shortfall,
                                },
                            issuer_request_id: issuer_request_id.clone(),
                            error: error_msg.clone(),
                            tx_id: None,
                            planned_burns: vec![],
                        },
                    )
                    .await?;
                return Err(BurnManagerError::Redemption(
                    RedemptionError::Vault {
                        message: error_msg,
                        release_reservation: false,
                        tx_id: None,
                        classification:
                            BurnFailureClassification::InsufficientReceipts {
                                shortfall,
                            },
                    },
                ));
            }
        }

        let execution = BurnExecutionPlan::orchestrator(
            network,
            token,
            amount,
            self.bot_wallet,
            external_tx_id,
        );
        self.execute_burn_and_record_result(
            issuer_request_id,
            network,
            execution,
        )
        .await
    }

    async fn next_burn_retry_external_tx_id(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        detected_tx_hash: &B256,
    ) -> Result<Option<BurnExternalTxId>, BurnManagerError> {
        let id_str = issuer_request_id.to_string();
        let rows = sqlx::query!(
            r#"
            SELECT payload as "payload!: String"
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
            ORDER BY sequence
            "#,
            id_str
        )
        .fetch_all(&self.view_pool)
        .await?;

        let events: Vec<RedemptionEvent> = rows
            .iter()
            .map(|row| serde_json::from_str(&row.payload))
            .collect::<Result<_, _>>()?;

        Ok(next_burn_retry_external_tx_id_from_history(
            detected_tx_hash,
            events.iter(),
        )?)
    }

    async fn burn_recovery_budget(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<BurnRecoveryBudget, BurnManagerError> {
        let aggregate_id = issuer_request_id.to_string();
        let payloads = sqlx::query_scalar::<_, String>(
            "
            SELECT payload
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type IN (
                  'RedemptionEvent::BurnRecoveryAttempted',
                  'RedemptionEvent::BurnRecoveryExhausted'
              )
            ORDER BY sequence
            ",
        )
        .bind(aggregate_id)
        .fetch_all(&self.view_pool)
        .await?;

        let mut budget = BurnRecoveryBudget {
            attempts: 0,
            exhausted: false,
            last_transaction: None,
        };
        for payload in payloads {
            match serde_json::from_str::<RedemptionEvent>(&payload)? {
                RedemptionEvent::BurnRecoveryAttempted {
                    tx_hash,
                    nonce,
                    ..
                } => {
                    budget.attempts =
                        budget.attempts.checked_add(1).ok_or_else(|| {
                            BurnManagerError::RecoveryAttemptOverflow {
                                issuer_request_id: issuer_request_id.clone(),
                            }
                        })?;
                    budget.last_transaction = Some((tx_hash, nonce));
                }
                RedemptionEvent::BurnRecoveryExhausted {
                    tx_hash,
                    nonce,
                    ..
                } => {
                    budget.exhausted = true;
                    budget.last_transaction = Some((tx_hash, nonce));
                }
                _ => {}
            }
        }

        Ok(budget)
    }

    async fn recovery_budget_available(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        current_transaction: Option<&SendableTxWithHash>,
    ) -> Result<bool, BurnManagerError> {
        let _guard = self.automatic_recovery_lock.lock().await;
        let budget = self.burn_recovery_budget(issuer_request_id).await?;
        if budget.exhausted {
            return Ok(false);
        }
        if budget.attempts < MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS {
            return Ok(true);
        }

        let (tx_hash, nonce) = current_transaction
            .map(|transaction| (transaction.hash, transaction.nonce))
            .or(budget.last_transaction)
            .ok_or_else(|| BurnManagerError::InvalidAggregateState {
                current_state:
                    "recovery budget reached without a transaction identity"
                        .to_string(),
            })?;
        self.persist_recovery_exhaustion(
            issuer_request_id,
            tx_hash,
            nonce,
            budget.attempts,
        )
        .await?;
        Ok(false)
    }

    async fn reserve_recovery_attempt(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        sendable_tx: &SendableTxWithHash,
        action: BurnRecoveryAction,
    ) -> Result<bool, BurnManagerError> {
        let _guard = self.automatic_recovery_lock.lock().await;
        let budget = self.burn_recovery_budget(issuer_request_id).await?;
        if budget.exhausted {
            return Ok(false);
        }
        if budget.attempts >= MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS {
            self.persist_recovery_exhaustion(
                issuer_request_id,
                sendable_tx.hash,
                sendable_tx.nonce,
                budget.attempts,
            )
            .await?;
            return Ok(false);
        }

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnRecoveryAttempt {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: sendable_tx.hash,
                    nonce: sendable_tx.nonce,
                    action,
                },
            )
            .await?;
        Ok(true)
    }

    async fn persist_recovery_exhaustion(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        tx_hash: B256,
        nonce: u64,
        attempts: u32,
    ) -> Result<(), BurnManagerError> {
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnRecoveryExhausted {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash,
                    nonce,
                    attempts,
                },
            )
            .await?;
        error!(target: "redemption",
            issuer_request_id = %issuer_request_id,
            tx_hash = %tx_hash,
            nonce,
            attempts,
            operator_action = "inspect the transaction; force-complete a verified landed burn, otherwise close after reconciliation",
            "Automatic burn recovery exhausted"
        );
        Ok(())
    }

    async fn plan_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
        vault: Address,
        underlying: &UnderlyingSymbol,
        burn_shares: U256,
        dust_shares: U256,
    ) -> Result<BurnPlan, BurnManagerError> {
        let chain_id = self.chain_id_for(network)?;
        let plan = self
            .receipt_service
            .for_burn(
                chain_id,
                vault,
                issuer_request_id,
                Shares::new(burn_shares),
                Shares::new(dust_shares),
            )
            .await;

        match plan {
            Ok(plan) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    num_receipts = plan.allocations.len(),
                    total_burn = %plan.total_burn,
                    dust = %plan.dust,
                    "Planned multi-receipt burn"
                );
                Ok(plan)
            }
            Err(BurnTrackingError::InsufficientBalance {
                required,
                available,
            }) => {
                self.handle_insufficient_balance(
                    issuer_request_id,
                    underlying,
                    required,
                    available,
                )
                .await
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn handle_insufficient_balance(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        underlying: &UnderlyingSymbol,
        required: Shares,
        available: Shares,
    ) -> Result<BurnPlan, BurnManagerError> {
        let error_msg = format!(
            "Insufficient balance for {underlying}: required {required}, available {available}"
        );

        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
            %required,
            %available,
            underlying = %underlying,
            "{error_msg}"
        );

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: error_msg.clone(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            "RecordBurnFailure command executed successfully"
        );

        Err(BurnManagerError::InsufficientBalance { required, available })
    }

    async fn execute_burn_and_record_result(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        network: Network,
        execution: BurnExecutionPlan,
    ) -> Result<(), BurnManagerError> {
        // Bounded to the 30 seconds SPEC promises: a live burn defers to
        // recovery rather than occupying the flow indefinitely behind an
        // intent that is not resolving.
        const MAX_WALLET_INTENT_WAIT_ATTEMPTS: u32 = 30;
        let mut wait_attempts = 0;
        let wallet_guard = loop {
            let wallet_guard = self.vault_for(network)?.lock_wallet().await;
            let unresolved_intent = has_unresolved_signer_intent(
                &self.view_pool,
                network,
                Some(issuer_request_id),
            )
            .await?;
            let unresolved_excess =
                has_unresolved_excess_burn_intent(&self.view_pool, None)
                    .await?;
            if !unresolved_intent && !unresolved_excess {
                break wallet_guard;
            }

            drop(wallet_guard);
            wait_attempts += 1;
            if wait_attempts >= MAX_WALLET_INTENT_WAIT_ATTEMPTS {
                warn!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    attempts = wait_attempts,
                    "Earlier wallet intent did not resolve within the wait \
                     budget; deferring this burn to recovery"
                );
                return Err(BurnManagerError::WalletIntentWaitExhausted {
                    issuer_request_id: issuer_request_id.clone(),
                });
            }
            debug!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                unresolved_intent,
                unresolved_excess,
                "Waiting for an earlier wallet intent before preparing burn"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        if !self.is_burn_execution_current(issuer_request_id).await? {
            return Ok(());
        }

        self.reserve_execution(issuer_request_id, &execution).await?;
        self.persist_burn_intention(issuer_request_id, &execution).await?;
        drop(wallet_guard);

        // Broadcast and confirm run in the durable SubmitBurnJob then
        // ConfirmBurnJob chain, so a crash between a vault call and its event
        // commit resumes from the persisted job.
        self.enqueue_submit_burn(issuer_request_id, execution).await
    }

    /// Enqueues the durable `SubmitBurnJob` under the redemption's idempotency
    /// key, freeing any terminal prior row so the push is not silently dropped.
    pub(crate) async fn enqueue_submit_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: BurnExecutionPlan,
    ) -> Result<(), BurnManagerError> {
        release_terminal_job(
            &self.view_pool,
            job_type::<SubmitBurnJob>(),
            &issuer_request_id.to_string(),
        )
        .await?;
        JobQueue::<SubmitBurnJob>::new(&self.apalis_pool)
            .push_with_idempotency_key(
                SubmitBurnJob {
                    issuer_request_id: issuer_request_id.clone(),
                    execution,
                },
                issuer_request_id.to_string(),
            )
            .await?;
        Ok(())
    }

    /// Idempotency key for a redemption's `ConfirmBurnJob`. Keyed by the
    /// transaction as well as the redemption so a `ReplaceDeadBurn` replacement
    /// enqueues its own confirm instead of collapsing onto the dead
    /// transaction's still-active job; a rerun for the same persisted `tx_id`
    /// still collapses.
    pub(crate) fn confirm_burn_idempotency_key(
        issuer_request_id: &IssuerRedemptionRequestId,
        tx_id: &TxId,
    ) -> String {
        format!("{issuer_request_id}:{tx_id}")
    }

    /// Enqueues the durable `ConfirmBurnJob` under the
    /// redemption-and-transaction idempotency key, freeing any terminal prior
    /// row so the push is not silently dropped.
    pub(crate) async fn enqueue_confirm_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: BurnConfirmPlan,
        tx_id: TxId,
    ) -> Result<(), BurnManagerError> {
        let idempotency_key =
            Self::confirm_burn_idempotency_key(issuer_request_id, &tx_id);
        release_terminal_job(
            &self.view_pool,
            job_type::<ConfirmBurnJob>(),
            &idempotency_key,
        )
        .await?;
        JobQueue::<ConfirmBurnJob>::new(&self.apalis_pool)
            .push_with_idempotency_key(
                ConfirmBurnJob {
                    issuer_request_id: issuer_request_id.clone(),
                    execution,
                    tx_id,
                },
                idempotency_key,
            )
            .await?;
        Ok(())
    }

    /// Builds the confirm-only plan a recovery-driven `ConfirmBurnJob` needs
    /// from the redemption's persisted anchor. Unlike the submit plan it carries
    /// no `BurnParams`, so no burn parameters are invented for a step that never
    /// reads them.
    fn recovery_confirm_plan(
        network: Network,
        vault: Address,
        burn_mode: VaultMode,
        dust_shares: U256,
        planned_burns: &[BurnRecord],
    ) -> BurnConfirmPlan {
        BurnConfirmPlan {
            network,
            vault,
            dust_shares,
            planned_burns: planned_burns.to_vec(),
            mode: match burn_mode {
                VaultMode::VaultDirect => VaultModeKind::VaultDirect,
                VaultMode::Orchestrator { .. } => VaultModeKind::Orchestrator,
            },
        }
    }

    async fn is_burn_execution_current(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<bool, BurnManagerError> {
        let Some(current) = self.store.load(issuer_request_id).await? else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: "Uninitialized".to_string(),
            });
        };
        if matches!(&current, Redemption::Burning { .. }) {
            return Ok(true);
        }

        debug!(target: "redemption",
            issuer_request_id = %issuer_request_id,
            state = aggregate_state_name(&current),
            "Skipping stale burn execution after acquiring wallet lock"
        );
        Ok(false)
    }

    async fn reserve_execution(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
    ) -> Result<(), BurnManagerError> {
        // Orchestrator burns have no bot-side inventory to reserve against.
        if execution.is_orchestrator() {
            return Ok(());
        }

        let result = self
            .reserve_with_conflict_retry(
                execution.network,
                execution.vault,
                issuer_request_id,
                execution.planned_burns.clone(),
            )
            .await;
        let Err(error) = result else {
            return Ok(());
        };

        error!(target: "redemption", issuer_request_id = %issuer_request_id,
            error = %error,
            "Failed to reserve receipts before burn submission; aborting to avoid double-spend"
        );
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.to_string(),
                    tx_id: None,
                    planned_burns: execution.planned_burns.clone(),
                },
            )
            .await?;

        Err(error)
    }

    async fn persist_burn_intention(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
    ) -> Result<(), BurnManagerError> {
        let result = self
            .store
            .send(
                issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: execution.params.clone(),
                    external_tx_id: execution.external_tx_id.clone(),
                },
            )
            .await;

        match result {
            Ok(()) => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "IntendBurn succeeded, submitting..."
                );
                Ok(())
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::PreparingBurnTxFailed { message },
            ))) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %message,
                    "Preparing signed burn tx failed"
                );

                // Always release because nothing was submitted on-chain.
                let chain_id = self.chain_id_for(execution.network)?;
                if !execution.is_orchestrator() {
                    self.release_reserved_burn(
                        chain_id,
                        execution.vault,
                        issuer_request_id,
                    )
                    .await;
                }

                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordBurnFailure {
                            classification:
                                BurnFailureClassification::Unclassified,
                            issuer_request_id: issuer_request_id.clone(),
                            error: message.clone(),
                            tx_id: None,
                            planned_burns: execution.planned_burns.clone(),
                        },
                    )
                    .await?;

                Err(BurnManagerError::Redemption(
                    RedemptionError::PreparingBurnTxFailed { message },
                ))
            }
            Err(error) => {
                // The append failed before anything reached the chain (the
                // event store rolls the write back atomically, including a
                // signer-intent trigger rejection), so the receipt
                // reservation must not outlive the attempt — a stranded
                // reservation blocks every later burn on the vault.
                warn!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    network = %execution.network,
                    error = %error,
                    "Burn intent append failed; releasing the receipt \
                     reservation before propagating"
                );
                match self.chain_id_for(execution.network) {
                    Ok(chain_id) => {
                        self.release_reserved_burn(
                            chain_id,
                            execution.vault,
                            issuer_request_id,
                        )
                        .await;
                    }
                    Err(chain_id_error) => {
                        warn!(target: "redemption",
                            issuer_request_id = %issuer_request_id,
                            network = %execution.network,
                            append_error = %error,
                            chain_id_error = %chain_id_error,
                            "Burn intent append failed and the receipt \
                             reservation could not be released"
                        );
                    }
                }
                Err(error.into())
            }
        }
    }

    pub(crate) async fn submit_intended_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
    ) -> Result<TxId, BurnManagerError> {
        // Performs the broadcast I/O and records the outcome through a pure
        // command, so no external vault call runs inside an aggregate
        // transition.
        let aggregate =
            self.store.load(issuer_request_id).await?.ok_or_else(|| {
                BurnManagerError::InvalidAggregateState {
                    current_state: "Uninitialized".to_string(),
                }
            })?;
        let (metadata, sendable_tx) = match &aggregate {
            Redemption::BurnIntended { metadata, sendable_tx, .. } => {
                (metadata.clone(), sendable_tx.clone())
            }
            // Already broadcast: a rerun returns the persisted id without
            // resending the transaction.
            Redemption::BurnSubmitted { tx_id, .. } => {
                return Ok(tx_id.clone());
            }
            other => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: aggregate_state_name(other).to_string(),
                });
            }
        };

        let vault_service = self.vault_for(execution.network)?;
        let submit_result = match &execution.params {
            BurnParams::VaultDirect { vault, burns, dust_shares, owner } => {
                let params = MultiBurnParams {
                    vault: *vault,
                    burns: burns.clone(),
                    dust_shares: *dust_shares,
                    owner: *owner,
                    user: metadata.wallet,
                    origin: BurnRequestOrigin::Redemption(
                        issuer_request_id.clone(),
                    ),
                    detected_tx_hash: metadata.detected_tx_hash,
                    external_tx_id: execution.external_tx_id.clone(),
                };
                vault_service.submit_burn(params, sendable_tx).await.map(
                    |submitted| RedemptionCommand::RecordBurnTxSubmitted {
                        issuer_request_id: issuer_request_id.clone(),
                        external_tx_id: BurnExternalTxId::from_string(
                            submitted.external_tx_id,
                        ),
                        tx_id: submitted.tx_id,
                        planned_burns: execution.planned_burns.clone(),
                    },
                )
            }
            BurnParams::Orchestrator { token, amount, owner } => {
                let VaultMode::Orchestrator { address: orchestrator } =
                    metadata.burn_mode
                else {
                    return Err(BurnManagerError::InvalidAggregateState {
                        current_state: "vault-direct burn_mode for an \
                                        orchestrator execution"
                            .to_string(),
                    });
                };
                let params = OrchestratorBurnParams {
                    orchestrator,
                    token: *token,
                    amount: *amount,
                    owner: *owner,
                    issuer_request_id: issuer_request_id.clone(),
                    detected_tx_hash: metadata.detected_tx_hash,
                    external_tx_id: execution.external_tx_id.clone(),
                };
                vault_service
                    .submit_orchestrator_burn(&params, &sendable_tx)
                    .await
                    .map(|submitted| {
                        RedemptionCommand::RecordOrchestratorBurnSubmitted {
                            issuer_request_id: issuer_request_id.clone(),
                            external_tx_id: BurnExternalTxId::from_string(
                                submitted.external_tx_id,
                            ),
                            tx_id: submitted.tx_id,
                        }
                    })
            }
        };

        let record_command = match submit_result {
            Ok(command) => command,
            Err(error) => {
                return self
                    .handle_broadcast_vault_error(
                        issuer_request_id,
                        execution,
                        error,
                    )
                    .await;
            }
        };

        self.store.send(issuer_request_id, record_command).await?;

        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
            "Burn submitted, confirming..."
        );
        self.load_submitted_tx_id(issuer_request_id).await
    }

    /// Handles a vault error from the broadcast: a failure eligible for release
    /// releases the reservation and records `RecordBurnFailure`; an ambiguous
    /// broadcast keeps the persisted transaction for recovery and propagates
    /// without recording.
    async fn handle_broadcast_vault_error(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
        error: VaultError,
    ) -> Result<TxId, BurnManagerError> {
        let VaultFailure {
            message,
            release_reservation,
            tx_id,
            classification,
        } = vault_error_to_redemption(&error);

        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
            error = %message,
            tx_id = ?tx_id,
            "Burn submission failed"
        );

        if release_reservation && !execution.is_orchestrator() {
            self.release_before_terminal_failure(
                execution.network,
                execution.vault,
                issuer_request_id,
            )
            .await?;
        }

        if !release_reservation {
            warn!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                "Burn broadcast outcome is ambiguous; keeping persisted transaction for recovery"
            );
            return Err(BurnManagerError::Redemption(RedemptionError::Vault {
                message,
                release_reservation: false,
                tx_id,
                classification,
            }));
        }

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: classification.clone(),
                    issuer_request_id: issuer_request_id.clone(),
                    error: message.clone(),
                    tx_id: tx_id.clone(),
                    planned_burns: execution.planned_burns.clone(),
                },
            )
            .await?;
        Err(BurnManagerError::Redemption(RedemptionError::Vault {
            message,
            release_reservation,
            tx_id,
            classification,
        }))
    }

    async fn load_submitted_tx_id(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<TxId, BurnManagerError> {
        let aggregate =
            self.store.load(issuer_request_id).await?.ok_or_else(|| {
                BurnManagerError::InvalidAggregateState {
                    current_state: "Uninitialized".to_string(),
                }
            })?;
        let Redemption::BurnSubmitted { tx_id, .. } = aggregate else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(&aggregate).to_string(),
            });
        };
        Ok(tx_id)
    }

    pub(crate) async fn confirm_submitted_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnConfirmPlan,
        tx_id: TxId,
    ) -> Result<(), BurnManagerError> {
        // Performs the confirmation I/O here, then records the outcome through
        // a pure command, so this step can later move to a durable job. A vault
        // error is the definitive failure or uncertain case; a command error is
        // a domain validation failure that propagates unchanged.
        let vault_service = self.vault_for(execution.network)?;
        let record_command = if execution.is_orchestrator() {
            match vault_service.confirm_orchestrator_burn(&tx_id).await {
                Ok(result) => {
                    RedemptionCommand::RecordOrchestratorBurnConfirmed {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_id: tx_id.clone(),
                        tx_hash: result.tx_hash,
                        shares_burned: result.shares_burned,
                        burn_range: result.burn_range,
                        gas_used: result.gas_used,
                        block_number: result.block_number,
                    }
                }
                Err(err) => {
                    return self
                        .handle_confirm_vault_error(
                            issuer_request_id,
                            execution,
                            &tx_id,
                            &err,
                        )
                        .await;
                }
            }
        } else {
            match vault_service
                .confirm_burn(&tx_id, execution.dust_shares)
                .await
            {
                Ok(result) => {
                    let burns = result
                        .burns
                        .into_iter()
                        .map(|burn| super::BurnRecord {
                            receipt_id: burn.receipt_id,
                            shares_burned: burn.shares_burned,
                        })
                        .collect();
                    RedemptionCommand::RecordBurnConfirmed {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_id: tx_id.clone(),
                        tx_hash: result.tx_hash,
                        burns,
                        dust_returned: result.dust_returned,
                        gas_used: result.gas_used,
                        block_number: result.block_number,
                    }
                }
                Err(err) => {
                    return self
                        .handle_confirm_vault_error(
                            issuer_request_id,
                            execution,
                            &tx_id,
                            &err,
                        )
                        .await;
                }
            }
        };

        // Records the confirmed burn through a pure command. Domain validation
        // errors, such as an orchestrator share mismatch, propagate unchanged.
        self.store.send(issuer_request_id, record_command).await?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            "Burn confirmed successfully"
        );

        // The burn landed on chain: consume the reservation so the mirror
        // balance drops to match.
        let chain_id = self.chain_id_for(execution.network)?;
        if !execution.is_orchestrator() {
            self.settle_reserved_burn(
                chain_id,
                execution.vault,
                issuer_request_id,
            )
            .await;
        }

        Ok(())
    }

    /// Handles a vault error from the inline confirm I/O, mirroring the old
    /// `ConfirmBurn` `Apply(RedemptionError::Vault { .. })` arm: a failure
    /// eligible for release terminalizes via
    /// `record_definitive_confirm_failure`, an uncertain one is left for
    /// periodic recovery, and either way the mapped error propagates with
    /// `tx_id: None`. A `BurnConfirmationPending` mapping propagates directly.
    async fn handle_confirm_vault_error(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnConfirmPlan,
        tx_id: &TxId,
        error: &VaultError,
    ) -> Result<(), BurnManagerError> {
        match super::map_confirm_burn_error(error, tx_id) {
            RedemptionError::Vault {
                message,
                release_reservation,
                classification,
                ..
            } => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %message,
                    classification = ?classification,
                    "Burn confirmation failed"
                );
                if release_reservation {
                    self.record_definitive_confirm_failure(
                        execution.network,
                        issuer_request_id,
                        DefinitiveConfirmFailure {
                            vault: execution.vault,
                            is_orchestrator: execution.is_orchestrator(),
                            classification: &classification,
                            error: &message,
                            tx_id,
                            planned_burns: &execution.planned_burns,
                        },
                    )
                    .await?;
                } else {
                    warn!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        tx_id = %tx_id,
                        "Burn confirmation remains uncertain; periodic recovery will retry"
                    );
                }

                Err(BurnManagerError::Redemption(RedemptionError::Vault {
                    message,
                    release_reservation,
                    tx_id: None,
                    classification,
                }))
            }
            other => Err(BurnManagerError::Redemption(other)),
        }
    }

    async fn release_before_terminal_failure(
        &self,
        network: Network,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<(), BurnManagerError> {
        // A crash after this idempotent release leaves the redemption in its
        // recoverable state, so the same transaction is checked again.
        // Recording failure first could strand the reservation because
        // burning-state recovery would no longer revisit the aggregate.
        let chain_id = self.chain_id_for(network)?;
        self.release_reserved_burn(chain_id, vault, issuer_request_id).await;
        Ok(())
    }

    /// Reserves the recovery action that may sign a fresh transaction after a
    /// proven revert. `None` means this is a legacy state without trustworthy
    /// persisted bytes, so automatic replacement must remain disabled.
    async fn reserve_replacement_after_revert(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<Option<bool>, BurnManagerError> {
        let Some(redemption) = self.store.load(issuer_request_id).await? else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: "Uninitialized".to_string(),
            });
        };
        let Ok(sendable_tx) = redemption.persisted_burn_tx() else {
            return Ok(None);
        };

        self.reserve_recovery_attempt(
            issuer_request_id,
            sendable_tx,
            BurnRecoveryAction::Replace,
        )
        .await
        .map(Some)
    }

    /// Reserves planned burns, retrying a bounded number of times on an
    /// optimistic-concurrency conflict.
    ///
    /// Concurrent redemptions for the same vault contend on the single
    /// `ReceiptInventory` aggregate; a lost commit race is transient and should
    /// be retried (each `execute` reloads), not treated as a terminal burn
    /// failure the way a genuine `InsufficientReceiptBalance` is.
    async fn reserve_with_conflict_retry(
        &self,
        network: Network,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
        planned_burns: Vec<super::BurnRecord>,
    ) -> Result<(), BurnManagerError> {
        const MAX_ATTEMPTS: usize = 3;
        let chain_id = self.chain_id_for(network)?;

        let mut attempt = 1;
        loop {
            match self
                .receipt_service
                .reserve_burn(
                    chain_id,
                    vault,
                    issuer_request_id.clone(),
                    planned_burns.clone(),
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(ReceiptRegistrationError::Aggregate(
                    AggregateError::AggregateConflict,
                )) if attempt < MAX_ATTEMPTS => {
                    warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                        attempt,
                        "Reserve hit an optimistic-concurrency conflict; retrying"
                    );
                    attempt += 1;
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    /// Releases a redemption's burn reservation, restoring available inventory.
    ///
    /// Best-effort: a failure is logged but not propagated. A stuck reservation
    /// is the failure mode the startup reservation recovery scan
    /// (`recover_stuck_reservations`) exists to clean up.
    async fn release_reserved_burn(
        &self,
        chain_id: u64,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        if let Err(err) = self
            .receipt_service
            .release_burn(chain_id, vault, issuer_request_id.clone())
            .await
        {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to release burn receipt reservation"
            );
        }
    }

    /// Settles a redemption's burn reservation after on-chain confirmation,
    /// reducing the mirror balance by the reserved amount.
    ///
    /// Best-effort: a failure is logged but not propagated. A reservation left
    /// unsettled here is recovered by the startup reservation recovery scan
    /// (`recover_stuck_reservations`); reconciliation cannot heal it because a
    /// landed-but-unsettled burn sits inside the reconcile no-op band.
    async fn settle_reserved_burn(
        &self,
        chain_id: u64,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        if let Err(err) = self
            .receipt_service
            .settle_burn(chain_id, vault, issuer_request_id.clone())
            .await
        {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to settle burn receipt reservation"
            );
        }
    }

    /// Handles a definitive (release-eligible) burn-confirmation failure,
    /// shared by the live confirm path and startup/periodic recovery so the
    /// recovery-eligibility rules for classified failures cannot diverge:
    /// releases the vault-direct reservation, reserves a budgeted replacement
    /// action only for `Unclassified` failures (typed classifications are
    /// never auto-retried — `InsufficientReceipts`/`AllowanceInsufficient`
    /// need manual action, and the logic-mismatch halt must not consume the
    /// retry budget), then records the classified `BurningFailed`, retaining
    /// the tx identity when no replacement action was reserved.
    ///
    /// Dropping the tx identity once a replacement action IS reserved is
    /// deliberate in both modes: this path only runs after a definitive
    /// mined revert, so the transaction can never land, and a tx-id-less
    /// `BurnFailed` routes the next recovery pass into the mode-aware
    /// preparation retry (`ResumeBurn` -> `handle_burning_started`). The
    /// complementary kept-tx-id case is handled in `recover_single_burn_failed`,
    /// which confirms the still-submitted transaction through the matching
    /// mode's confirm path (`recover_burn_failed_with_existing_tx` for
    /// vault-direct, `recover_orchestrator_burn_failed_with_existing_tx` for
    /// orchestrator).
    async fn record_definitive_confirm_failure(
        &self,
        network: Network,
        issuer_request_id: &IssuerRedemptionRequestId,
        failure: DefinitiveConfirmFailure<'_>,
    ) -> Result<(), BurnManagerError> {
        // The recovery-action decision runs BEFORE the release below so a
        // reserve error keeps the receipt reservation intact, matching the
        // pre-orchestrator ordering. Typed classifications are never
        // auto-retried (InsufficientReceipts / AllowanceInsufficient need
        // manual action; the logic-mismatch halt must not consume the retry
        // budget), so no recovery action is reserved for them.
        let exact_recovery = if *failure.classification
            == BurnFailureClassification::Unclassified
        {
            self.reserve_replacement_after_revert(issuer_request_id).await?
        } else {
            error!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                token = %failure.vault,
                orchestrator_mode = failure.is_orchestrator,
                classification = ?failure.classification,
                error = %failure.error,
                "Burn failed with a non-retryable classification; manual \
                 intervention required"
            );
            None
        };

        if !failure.is_orchestrator {
            self.release_before_terminal_failure(
                network,
                failure.vault,
                issuer_request_id,
            )
            .await?;
        }

        // `Some(false)` means the budget was exhausted and NOTHING was
        // reserved (only `BurnRecoveryExhausted` is persisted), so the tx
        // identity is retained for operator inspection exactly like the
        // no-persisted-bytes `None` case; only an actually reserved
        // replacement action (`Some(true)`) supersedes it.
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: failure.classification.clone(),
                    issuer_request_id: issuer_request_id.clone(),
                    error: failure.error.to_string(),
                    tx_id: (!matches!(exact_recovery, Some(true)))
                        .then(|| failure.tx_id.clone()),
                    planned_burns: failure.planned_burns.to_vec(),
                },
            )
            .await?;

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BurnExecutionPlan {
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) params: BurnParams,
    pub(crate) planned_burns: Vec<BurnRecord>,
    pub(crate) dust_shares: U256,
    pub(crate) external_tx_id: Option<BurnExternalTxId>,
}

impl BurnExecutionPlan {
    fn vault_direct(
        network: Network,
        vault: Address,
        plan: &BurnPlan,
        owner: Address,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Self {
        let burns: Vec<MultiBurnEntry> = plan
            .allocations
            .iter()
            .map(|allocation| MultiBurnEntry {
                receipt_id: allocation.receipt.receipt_id.inner(),
                burn_shares: allocation.burn_amount.inner(),
                receipt_info: allocation.receipt.receipt_info.clone(),
                receipt_info_bytes: allocation
                    .receipt
                    .receipt_info_bytes
                    .clone(),
            })
            .collect();
        let planned_burns = burns
            .iter()
            .map(|entry| BurnRecord {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();
        let dust_shares = plan.dust.inner();

        Self {
            network,
            vault,
            params: BurnParams::VaultDirect {
                vault,
                burns,
                dust_shares,
                owner,
            },
            planned_burns,
            dust_shares,
            external_tx_id,
        }
    }

    /// Orchestrator-mode execution: no receipt plan and no dust entry — the
    /// orchestrator walks receipts on-chain and dust stays in the bot wallet.
    const fn orchestrator(
        network: Network,
        token: Address,
        amount: U256,
        owner: Address,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Self {
        Self {
            network,
            vault: token,
            params: BurnParams::Orchestrator { token, amount, owner },
            planned_burns: vec![],
            dust_shares: U256::ZERO,
            external_tx_id,
        }
    }

    /// Builds a plan for a recovery-driven re-broadcast from the mode-correct
    /// `BurnParams` the reconciler reconstructed. `vault` and `dust_shares` are
    /// taken from the params so a later confirm reads the same anchor the hot
    /// path records.
    fn from_recovery(
        network: Network,
        params: BurnParams,
        planned_burns: Vec<BurnRecord>,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Self {
        let (vault, dust_shares, planned_burns) = match &params {
            BurnParams::VaultDirect { vault, dust_shares, .. } => {
                (*vault, *dust_shares, planned_burns)
            }
            BurnParams::Orchestrator { token, .. } => {
                (*token, U256::ZERO, vec![])
            }
        };
        Self {
            network,
            vault,
            params,
            planned_burns,
            dust_shares,
            external_tx_id,
        }
    }

    /// Whether this execution runs through the orchestrator, in which case
    /// the receipt-inventory reserve/settle/release lifecycle does not apply.
    pub(crate) const fn is_orchestrator(&self) -> bool {
        matches!(self.params, BurnParams::Orchestrator { .. })
    }

    /// Projects the confirm-only plan a `ConfirmBurnJob` carries, dropping the
    /// submit `BurnParams` the confirmation step never reads.
    pub(crate) fn confirm_plan(&self) -> BurnConfirmPlan {
        BurnConfirmPlan {
            network: self.network,
            vault: self.vault,
            dust_shares: self.dust_shares,
            planned_burns: self.planned_burns.clone(),
            mode: if self.is_orchestrator() {
                VaultModeKind::Orchestrator
            } else {
                VaultModeKind::VaultDirect
            },
        }
    }
}

/// The confirm-only projection of a burn plan. A `ConfirmBurnJob` carries this
/// instead of the full `BurnExecutionPlan` so its serialized row never holds
/// invented burn parameters: confirmation reads only the network, vault, dust,
/// planned burns, and mode.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BurnConfirmPlan {
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) dust_shares: U256,
    pub(crate) planned_burns: Vec<BurnRecord>,
    pub(crate) mode: VaultModeKind,
}

impl BurnConfirmPlan {
    /// Whether the burn runs through the orchestrator, in which case the
    /// receipt-inventory reserve/settle/release lifecycle does not apply.
    pub(crate) const fn is_orchestrator(&self) -> bool {
        matches!(self.mode, VaultModeKind::Orchestrator)
    }
}

const fn aggregate_state_name(aggregate: &Redemption) -> &'static str {
    match aggregate {
        Redemption::Detected { .. } => "Detected",
        Redemption::Held { .. } => "Held",
        Redemption::AlpacaCallClaimed { .. } => "AlpacaCallClaimed",
        Redemption::AlpacaCalled { .. } => "AlpacaCalled",
        Redemption::Burning { .. } => "Burning",
        Redemption::BurnSubmitted { .. } => "BurnSubmitted",
        Redemption::Failed { .. } => "Failed",
        Redemption::Completed { .. } => "Completed",
        Redemption::Closed { .. } => "Closed",
        Redemption::BurnIntended { .. } => "BurnIntended",
    }
}

pub(crate) const fn extract_tx_hash(error: &VaultError) -> Option<B256> {
    match error {
        VaultError::Reverted { tx_hash }
        | VaultError::EventNotFound { tx_hash }
        | VaultError::OrchestratorReverted { tx_hash, .. } => Some(*tx_hash),
        _ => None,
    }
}

/// Whether a failed burn confirmation definitively consumed no receipts, so its
/// inventory reservation must be released. Ambiguous pending tx statuses
/// keep the reservation (the transaction may still land on-chain). A decoded
/// orchestrator revert is definitive like a plain revert.
pub(crate) const fn should_release_reserved_burn(error: &VaultError) -> bool {
    matches!(
        error,
        VaultError::Reverted { .. } | VaultError::OrchestratorReverted { .. }
    )
}

pub(crate) const fn is_pending_burn_confirmation(error: &VaultError) -> bool {
    matches!(
        error,
        VaultError::ConfirmationPending { .. }
            | VaultError::PendingTransaction(_)
            | VaultError::Rpc(_)
    )
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnManagerError {
    #[error("Vault error: {0}")]
    Vault(#[from] VaultError),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<LifecycleError<Redemption>>),
    #[error("Redemption error: {0}")]
    Redemption(#[from] RedemptionError),
    #[error(transparent)]
    UnconfiguredNetwork(#[from] UnconfiguredNetworkError),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("Insufficient balance: required {required}, available {available}")]
    InsufficientBalance { required: Shares, available: Shares },
    #[error(transparent)]
    ForceCompleteRefusal(#[from] ForceCompleteRefusal),
    #[error(
        "Force-complete proof nonce {proof_nonce} does not match the \
         persisted burn transaction's nonce {persisted_nonce}; an alternate \
         proof must be the mined replacement at that exact nonce"
    )]
    ForceCompleteNonceMismatch { proof_nonce: u64, persisted_nonce: u64 },
    #[error(
        "Force-complete proof burned {proof_shares} share-wei but this \
         redemption requires exactly {required_shares}"
    )]
    ForceCompleteAmountMismatch { proof_shares: U256, required_shares: U256 },
    #[error(
        "Force-complete proof also transferred {shares} share-wei to \
         {recipient}; an orchestrator burn moves nothing besides the \
         pull-and-burn legs"
    )]
    ForceCompleteStrayTransfer { recipient: Address, shares: U256 },
    #[error("Receipt inventory error: {0}")]
    BurnTracking(#[from] BurnTrackingError),
    #[error("Redemption view error: {0}")]
    RedemptionView(#[from] RedemptionViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error(
        "Asset not found for underlying: {underlying} on network: {network}"
    )]
    AssetNotFound { underlying: UnderlyingSymbol, network: Network },
    #[error("Arithmetic overflow when computing total shares needed")]
    SharesOverflow,
    #[error("Receipt reservation error: {0}")]
    ReceiptRegistration(#[from] ReceiptRegistrationError),
    #[error("Burn recovery attempt counter overflowed for {issuer_request_id}")]
    RecoveryAttemptOverflow { issuer_request_id: IssuerRedemptionRequestId },
    #[error(
        "earlier wallet intent did not resolve within the wait budget for \
         {issuer_request_id}; deferred to recovery"
    )]
    WalletIntentWaitExhausted { issuer_request_id: IssuerRedemptionRequestId },
    #[error(transparent)]
    Enqueue(#[from] QueuePushError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256, address, b256, uint};
    use chrono::Utc;
    use cqrs_es::AggregateError;
    use event_sorcery::{Store, StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tracing_test::traced_test;

    use super::{
        BurnConfirmPlan, BurnExecutionPlan, BurnManager, BurnManagerError,
        DefinitiveConfirmFailure, MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        RecoveryOutcome, Redemption, RedemptionCommand,
        should_release_reserved_burn,
    };
    use crate::burn_excess::BurnExcessEvent;
    use crate::config::{VaultMode, VaultModeKind};
    use crate::mint::IssuerMintRequestId;
    use crate::mint::{Quantity, TokenizationRequestId};
    use crate::receipt_inventory::{
        BurnPlan, BurnTrackingError, CqrsReceiptService, MintedReceiptParams,
        ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
        ReceiptInventoryError, ReceiptLookupError, ReceiptRegistrationError,
        ReceiptService, ReceiptSource, ReceiptVaultKey, RecoveredReceipt,
        Shares,
    };
    use crate::redemption::BurnExternalTxId;
    use crate::redemption::RedemptionServices;
    use crate::redemption::view::{RedemptionViewReactor, find_burn_failed};
    use crate::redemption::{
        BurnFailureClassification, BurnParams, BurnRecord, BurnRecoveryAction,
        IssuerRedemptionRequestId, RedemptionError, RedemptionView,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, log_count_at, logs_contain_at};
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnRange, BurnRequestOrigin, BurnTxStatus, MultiBurnEntry,
        MultiBurnParams, NetworkVaultServices, OrchestratorBurnParams,
        OrchestratorBurnReadiness, OrchestratorBurnResult,
        OrchestratorRevertReason, ReceiptInformation, SendableTxWithHash, TxId,
        VaultError, VaultService, VerifiedBurn, VerifiedShareTransfer,
    };

    const TEST_WALLET: Address =
        address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

    fn transaction_failed() -> VaultError {
        VaultError::Reverted { tx_hash: B256::random() }
    }

    async fn insert_raw_event(
        pool: &SqlitePool,
        aggregate_type: &str,
        aggregate_id: &str,
        sequence: i64,
        event_type: &str,
        payload: &str,
    ) -> Result<(), sqlx::Error> {
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
            VALUES (?, ?, ?, ?, '1.0', ?, '{}')
            ",
        )
        .bind(aggregate_type)
        .bind(aggregate_id)
        .bind(sequence)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await?;

        Ok(())
    }

    #[test]
    fn should_release_on_terminal_failure() {
        assert!(
            should_release_reserved_burn(&transaction_failed()),
            "reverted tx should release the reservation"
        );
    }

    #[test]
    fn should_not_release_on_not_a_burn() {
        let not_a_burn = VaultError::NotABurn {
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
        };

        assert!(
            !should_release_reserved_burn(&not_a_burn),
            "a tx that is not a burn must not release the reservation"
        );
    }

    #[test]
    fn should_retain_reservation_on_non_definitive_errors() {
        let ambiguous = VaultError::EventNotFound {
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
        };

        assert!(
            !should_release_reserved_burn(&ambiguous),
            "ambiguous parse errors must not release the reservation"
        );
        assert!(!should_release_reserved_burn(&VaultError::InvalidReceipt));
    }

    struct TestHarness {
        store: Arc<Store<Redemption>>,
        receipt_service: Arc<dyn ReceiptService>,
        receipt_inventory_store: Arc<Store<ReceiptInventory>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        asset_store: Arc<Store<TokenizedAsset>>,
        apalis_pool: apalis_sqlite::SqlitePool,
        /// Temp directory backing the file database when the harness owns it;
        /// removed on drop. `None` when the caller supplied the pool.
        database_dir: Option<PathBuf>,
    }

    impl Drop for TestHarness {
        fn drop(&mut self) {
            // Remove the file database and its `-wal`/`-shm` sidecars when the
            // harness owns the directory. Best effort: the test is ending.
            if let Some(dir) = self.database_dir.take() {
                let _ = std::fs::remove_dir_all(dir);
            }
        }
    }

    struct SettleFailingReceiptService {
        inner: Arc<dyn ReceiptService>,
    }

    #[async_trait::async_trait]
    impl ReceiptService for SettleFailingReceiptService {
        async fn register_minted_receipt(
            &self,
            params: MintedReceiptParams,
        ) -> Result<(), ReceiptRegistrationError> {
            self.inner.register_minted_receipt(params).await
        }

        async fn for_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: &IssuerRedemptionRequestId,
            shares_to_burn: Shares,
            dust: Shares,
        ) -> Result<BurnPlan, BurnTrackingError> {
            self.inner
                .for_burn(
                    chain_id,
                    vault,
                    redemption_issuer_request_id,
                    shares_to_burn,
                    dust,
                )
                .await
        }

        async fn reserve_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: IssuerRedemptionRequestId,
            burns: Vec<BurnRecord>,
        ) -> Result<(), ReceiptRegistrationError> {
            self.inner
                .reserve_burn(
                    chain_id,
                    vault,
                    redemption_issuer_request_id,
                    burns,
                )
                .await
        }

        async fn release_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            self.inner
                .release_burn(chain_id, vault, redemption_issuer_request_id)
                .await
        }

        async fn settle_burn(
            &self,
            _chain_id: u64,
            _vault: Address,
            _redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            Err(ReceiptRegistrationError::Aggregate(AggregateError::UserError(
                ReceiptInventoryError::UnknownReceipt {
                    receipt_id: ReceiptId::from(U256::ZERO),
                },
            )))
        }

        async fn reserved_redemptions(
            &self,
            chain_id: u64,
            vault: Address,
        ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError>
        {
            self.inner.reserved_redemptions(chain_id, vault).await
        }

        async fn find_by_issuer_request_id(
            &self,
            chain_id: u64,
            vault: &Address,
            issuer_request_id: &IssuerMintRequestId,
        ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
            self.inner
                .find_by_issuer_request_id(chain_id, vault, issuer_request_id)
                .await
        }
    }

    impl TestHarness {
        async fn new() -> Self {
            Self::with_vault_mock(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn with_vault_mock(vault_mock: Arc<MockVaultService>) -> Self {
            // A shared file database lets the sqlx and apalis pools see the same
            // `Jobs` table; the directory is removed when the harness drops.
            let database_dir = std::env::temp_dir()
                .join(format!("st0x-burn-test-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&database_dir)
                .expect("test temp directory should be created");
            let database_url = format!(
                "sqlite:{}",
                database_dir.join("burn-test.db").display()
            );

            let options =
                sqlx::sqlite::SqliteConnectOptions::from_str(&database_url)
                    .expect("valid sqlite url")
                    .create_if_missing(true)
                    .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(5));
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(options)
                .await
                .expect("Failed to create test database");

            let apalis_options =
                apalis_sqlite::SqliteConnectOptions::from_str(&database_url)
                    .expect("valid sqlite url")
                    .pragma("journal_mode", "WAL")
                    .busy_timeout(Duration::from_secs(5));
            let apalis_pool =
                apalis_sqlite::SqlitePool::connect_with(apalis_options)
                    .await
                    .expect("Failed to create apalis test pool");

            let mut harness =
                Self::with_pool(vault_mock, pool, apalis_pool).await;
            harness.database_dir = Some(database_dir);
            harness
        }

        async fn with_pool(
            vault_mock: Arc<MockVaultService>,
            pool: SqlitePool,
            apalis_pool: apalis_sqlite::SqlitePool,
        ) -> Self {
            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let vault_service: Arc<dyn crate::vault::VaultService> =
                vault_mock.clone();

            // Redemption has no canonical Table projection; its query-facing
            // `redemption_view` is maintained by an explicit reactor, so the
            // test store wires the same reactor production uses to keep
            // `find_burning`/`find_burn_failed` populated during recovery.
            let store = StoreBuilder::<Redemption>::new(pool.clone())
                .with(Arc::new(RedemptionViewReactor::new(pool.clone())))
                .build(RedemptionServices::with_single_vault(
                    Network::Base,
                    vault_service,
                ))
                .await
                .expect("Failed to build redemption store");

            let receipt_inventory_store =
                Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

            let (asset_store, _asset_projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .expect("Failed to build tokenized asset store");

            let receipt_service = Arc::new(CqrsReceiptService::new(
                receipt_inventory_store.clone(),
            ));

            Self {
                store,
                receipt_service,
                receipt_inventory_store,
                pool,
                asset_store,
                apalis_pool,
                database_dir: None,
            }
        }

        async fn add_asset(
            &self,
            underlying: &UnderlyingSymbol,
            vault: Address,
        ) {
            self.asset_store
                .send(
                    &AssetKey::new(underlying.clone(), Network::Base),
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!(
                            "t{}",
                            underlying.as_str()
                        )),
                        network: Network::Base,
                        vault,
                    },
                )
                .await
                .expect("Failed to add tokenized asset");
        }

        async fn discover_receipt(
            &self,
            vault: Address,
            receipt_id: U256,
            balance: U256,
        ) {
            self.receipt_inventory_store
                .send(
                    &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                    ReceiptInventoryCommand::DiscoverReceipt {
                        receipt_id: ReceiptId::from(receipt_id),
                        balance: Shares::from(balance),
                        block_number: 1,
                        tx_hash: b256!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                        source: ReceiptSource::External,
                        receipt_info: None,
                        receipt_info_bytes: None,
                    },
                )
                .await
                .expect("Failed to discover receipt");
        }
    }

    async fn setup_test_environment() -> TestHarness {
        TestHarness::new().await
    }

    async fn create_test_redemption_in_burning_state(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burn-456");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        store
            .send(
                issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    network: Network::Base,
                    wallet,
                    quantity: quantity.clone(),
                    tx_hash,
                    block_number,
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::ClaimAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    alpaca_quantity: quantity,
                    dust_quantity: Quantity::new(Decimal::ZERO),
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    async fn load_aggregate(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        store.load(issuer_request_id).await.unwrap().unwrap()
    }

    /// Reconstructs the `BurnExecutionPlan` the enqueued `SubmitBurnJob` would
    /// carry from the persisted `BurnIntended` state, so a test can drive the
    /// submit and confirm steps inline (the harness runs no apalis worker).
    async fn intended_execution(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
    ) -> BurnExecutionPlan {
        let aggregate = load_aggregate(store, issuer_request_id).await;
        let Redemption::BurnIntended {
            metadata,
            alpaca_quantity,
            dust_quantity,
            planned_burns,
            external_tx_id,
            ..
        } = aggregate
        else {
            panic!("expected BurnIntended, got {aggregate:?}");
        };
        let dust_shares = dust_quantity.to_u256_with_18_decimals().unwrap();
        let params = match &metadata.burn_mode {
            VaultMode::VaultDirect => {
                let burns = planned_burns
                    .iter()
                    .map(|burn| MultiBurnEntry {
                        receipt_id: burn.receipt_id,
                        burn_shares: burn.shares_burned,
                        receipt_info: None,
                        receipt_info_bytes: None,
                    })
                    .collect();
                BurnParams::VaultDirect {
                    vault,
                    burns,
                    dust_shares,
                    owner: TEST_WALLET,
                }
            }
            VaultMode::Orchestrator { .. } => BurnParams::Orchestrator {
                token: vault,
                amount: alpaca_quantity.to_u256_with_18_decimals().unwrap(),
                owner: TEST_WALLET,
            },
        };
        BurnExecutionPlan {
            network: metadata.network,
            vault,
            params,
            planned_burns,
            dust_shares,
            external_tx_id,
        }
    }

    /// Drives the enqueued submit then confirm chain inline from `BurnIntended`
    /// to `Completed`, mirroring `SubmitBurnJob` then `ConfirmBurnJob`.
    async fn drive_intended_burn_to_completion(
        manager: &BurnManager,
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
    ) {
        let execution =
            intended_execution(store, issuer_request_id, vault).await;
        let tx_id = manager
            .submit_intended_burn(issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");
        manager
            .confirm_submitted_burn(
                issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await
            .expect("confirm_submitted_burn should record the confirmation");
    }

    /// Reconstructs the confirm-only plan a recovery `ConfirmBurnJob` carries
    /// from the persisted `BurnSubmitted` state, plus the submitted `tx_id`,
    /// mirroring `recovery_confirm_plan`.
    async fn submitted_confirm_execution(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
    ) -> (BurnConfirmPlan, TxId) {
        let aggregate = load_aggregate(store, issuer_request_id).await;
        let Redemption::BurnSubmitted {
            metadata,
            dust_quantity,
            tx_id,
            planned_burns,
            ..
        } = aggregate
        else {
            panic!("expected BurnSubmitted, got {aggregate:?}");
        };
        let dust_shares = dust_quantity.to_u256_with_18_decimals().unwrap();
        let plan = BurnConfirmPlan {
            network: metadata.network,
            vault,
            dust_shares,
            planned_burns,
            mode: match metadata.burn_mode {
                VaultMode::VaultDirect => VaultModeKind::VaultDirect,
                VaultMode::Orchestrator { .. } => VaultModeKind::Orchestrator,
            },
        };
        (plan, tx_id)
    }

    /// Drives the enqueued recovery `ConfirmBurnJob` inline from `BurnSubmitted`
    /// to its confirmation outcome.
    async fn drive_submitted_burn_confirm(
        manager: &BurnManager,
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
    ) {
        let (execution, tx_id) =
            submitted_confirm_execution(store, issuer_request_id, vault).await;
        manager
            .confirm_submitted_burn(issuer_request_id, &execution, tx_id)
            .await
            .expect("confirm_submitted_burn should record the confirmation");
    }

    async fn persist_test_burn_intent(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        owner: Address,
    ) {
        store
            .send(
                issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(42_U256),
                            burn_shares: uint!(17_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("persisted burn intent should succeed");
    }

    /// Counts every receipt-service call while delegating to the real
    /// service, so orchestrator-mode tests can prove the receipt lifecycle
    /// is never touched.
    struct RecordingReceiptService {
        inner: Arc<dyn ReceiptService>,
        calls: AtomicUsize,
    }

    impl RecordingReceiptService {
        fn new(inner: Arc<dyn ReceiptService>) -> Self {
            Self { inner, calls: AtomicUsize::new(0) }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }

        fn record(&self) {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl ReceiptService for RecordingReceiptService {
        async fn register_minted_receipt(
            &self,
            params: MintedReceiptParams,
        ) -> Result<(), ReceiptRegistrationError> {
            self.record();
            self.inner.register_minted_receipt(params).await
        }

        async fn for_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: &IssuerRedemptionRequestId,
            shares_to_burn: Shares,
            dust: Shares,
        ) -> Result<BurnPlan, BurnTrackingError> {
            self.record();
            self.inner
                .for_burn(
                    chain_id,
                    vault,
                    redemption_issuer_request_id,
                    shares_to_burn,
                    dust,
                )
                .await
        }

        async fn reserve_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: IssuerRedemptionRequestId,
            burns: Vec<BurnRecord>,
        ) -> Result<(), ReceiptRegistrationError> {
            self.record();
            self.inner
                .reserve_burn(
                    chain_id,
                    vault,
                    redemption_issuer_request_id,
                    burns,
                )
                .await
        }

        async fn release_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            self.record();
            self.inner
                .release_burn(chain_id, vault, redemption_issuer_request_id)
                .await
        }

        async fn settle_burn(
            &self,
            chain_id: u64,
            vault: Address,
            redemption_issuer_request_id: IssuerRedemptionRequestId,
        ) -> Result<(), ReceiptRegistrationError> {
            self.record();
            self.inner
                .settle_burn(chain_id, vault, redemption_issuer_request_id)
                .await
        }

        async fn reserved_redemptions(
            &self,
            chain_id: u64,
            vault: Address,
        ) -> Result<Vec<IssuerRedemptionRequestId>, ReceiptLookupError>
        {
            self.record();
            self.inner.reserved_redemptions(chain_id, vault).await
        }

        async fn find_by_issuer_request_id(
            &self,
            chain_id: u64,
            vault: &Address,
            issuer_request_id: &IssuerMintRequestId,
        ) -> Result<Option<RecoveredReceipt>, ReceiptLookupError> {
            self.record();
            self.inner
                .find_by_issuer_request_id(chain_id, vault, issuer_request_id)
                .await
        }
    }

    fn test_orchestrator_address() -> Address {
        address!("0x00000000000000000000000000000000000000aa")
    }

    /// Mirrors `create_test_redemption_in_burning_state` with the redemption
    /// anchored to orchestrator mode at detection.
    async fn create_orchestrator_redemption_in_burning_state(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        store
            .send(
                issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::Orchestrator {
                        address: test_orchestrator_address(),
                    },
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    tx_hash: b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                    block_number: 12345,
                    network: Network::Base,
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::ClaimAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-orch-456",
                    ),
                    alpaca_quantity: Quantity::new(Decimal::from(100)),
                    dust_quantity: Quantity::new(Decimal::ZERO),
                },
            )
            .await
            .unwrap();

        store
            .send(
                issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    struct OrchestratorTestSetup {
        harness: TestHarness,
        vault_mock: Arc<MockVaultService>,
        recording: Arc<RecordingReceiptService>,
        manager: BurnManager,
        issuer_request_id: IssuerRedemptionRequestId,
        aggregate: Redemption,
        vault: Address,
    }

    async fn setup_orchestrator_burning(
        vault_mock: Arc<MockVaultService>,
    ) -> OrchestratorTestSetup {
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let recording = Arc::new(RecordingReceiptService::new(
            harness.receipt_service.clone(),
        ));
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            harness.pool.clone(),
            harness.store.clone(),
            recording.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_orchestrator_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;

        OrchestratorTestSetup {
            harness,
            vault_mock,
            recording,
            manager,
            issuer_request_id,
            aggregate,
            vault,
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn orchestrator_burn_happy_path_skips_receipt_lifecycle() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success(),
        ))
        .await;

        setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await
            .expect("orchestrator burn should complete");

        drive_intended_burn_to_completion(
            &setup.manager,
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Completed { .. }),
            "expected Completed, got {aggregate:?}"
        );

        assert_eq!(
            setup.recording.call_count(),
            0,
            "the receipt reserve/settle/release lifecycle must never run in \
             orchestrator mode"
        );
        assert_eq!(setup.vault_mock.orchestrator_readiness_call_count(), 1);
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 1);

        let params = setup
            .vault_mock
            .last_orchestrator_burn_params()
            .expect("orchestrator submit must record its params");
        assert_eq!(params.orchestrator, test_orchestrator_address());
        assert_eq!(params.token, setup.vault);
        assert_eq!(params.amount, uint!(100_000000000000000000_U256));
        assert_eq!(params.owner, TEST_WALLET);

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Starting on-chain burning process", "Orchestrator"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Burn confirmed successfully"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn orchestrator_allowance_gate_records_classified_failure() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_readiness(
                OrchestratorBurnReadiness::AllowanceInsufficient {
                    required: uint!(100_000000000000000000_U256),
                    current: U256::ZERO,
                },
            ),
        ))
        .await;

        let result = setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await;
        assert!(result.is_err(), "allowance gate must fail the burn");

        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            0,
            "the burn must never be submitted without an approval"
        );

        let failed = find_burn_failed(&setup.harness.pool)
            .await
            .expect("burn-failed query should succeed");
        let (_, view) = failed
            .iter()
            .find(|(id, _)| *id == setup.issuer_request_id)
            .expect("redemption must be in BurnFailed view state");
        assert!(
            matches!(
                view,
                RedemptionView::BurnFailed {
                    classification:
                        BurnFailureClassification::AllowanceInsufficient,
                    ..
                }
            ),
            "expected AllowanceInsufficient classification, got {view:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &[
                "Orchestrator burn allowance insufficient",
                "required",
                "current",
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn orchestrator_health_gate_defers_without_event() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_readiness(
                OrchestratorBurnReadiness::VaultLogicMismatch,
            ),
        ))
        .await;

        setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await
            .expect("health-gate deferral is not an error");

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "a halted orchestrator must leave the redemption in Burning \
             (no event), got {aggregate:?}"
        );
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 0);

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Orchestrator halted", "deferring"]
        ));
    }

    /// The pre-submit shortfall gate (burn simulation reverting with
    /// `InsufficientReceipts`) records a classified failure without ever
    /// signing or submitting.
    #[traced_test]
    #[tokio::test]
    async fn orchestrator_shortfall_gate_records_classified_failure() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_readiness(
                OrchestratorBurnReadiness::InsufficientReceipts {
                    shortfall: uint!(250_U256),
                },
            ),
        ))
        .await;

        let result = setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await;
        assert!(result.is_err(), "the shortfall gate must fail the burn");
        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            0,
            "a shortfall burn must never be submitted"
        );

        let failed = find_burn_failed(&setup.harness.pool)
            .await
            .expect("burn-failed query should succeed");
        let (_, view) = failed
            .iter()
            .find(|(id, _)| *id == setup.issuer_request_id)
            .expect("redemption must be in BurnFailed view state");
        assert!(
            matches!(
                view,
                RedemptionView::BurnFailed {
                    classification:
                        BurnFailureClassification::InsufficientReceipts {
                            shortfall,
                        },
                    ..
                } if *shortfall == uint!(250_U256)
            ),
            "expected InsufficientReceipts classification, got {view:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &[
                "Orchestrator receipts insufficient",
                "manual EMERGENCY_ROLE recovery required",
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn orchestrator_insufficient_receipts_revert_classifies_failure() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_confirm_revert(
                OrchestratorRevertReason::InsufficientReceipts {
                    token: address!(
                        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ),
                    shortfall: uint!(250_U256),
                },
            ),
        ))
        .await;

        setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await
            .expect("orchestrator intent should persist");
        let execution = intended_execution(
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;
        let tx_id = setup
            .manager
            .submit_intended_burn(&setup.issuer_request_id, &execution)
            .await
            .expect("submit should broadcast the orchestrator burn");
        let result = setup
            .manager
            .confirm_submitted_burn(
                &setup.issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;
        assert!(result.is_err(), "revert must surface as an error");

        let failed = find_burn_failed(&setup.harness.pool)
            .await
            .expect("burn-failed query should succeed");
        let (_, view) = failed
            .iter()
            .find(|(id, _)| *id == setup.issuer_request_id)
            .expect("redemption must be in BurnFailed view state");
        assert!(
            matches!(
                view,
                RedemptionView::BurnFailed {
                    classification:
                        BurnFailureClassification::InsufficientReceipts {
                            shortfall,
                        },
                    ..
                } if *shortfall == uint!(250_U256)
            ),
            "expected InsufficientReceipts classification, got {view:?}"
        );

        // Non-retryable classifications must not consume the automatic
        // recovery budget.
        let recovery_events = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(setup.issuer_request_id.to_string())
        .fetch_one(&setup.harness.pool)
        .await
        .expect("recovery-event count should query");
        assert_eq!(
            recovery_events, 0,
            "a classified revert must not reserve a recovery action"
        );

        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["non-retryable classification", "InsufficientReceipts"]
        ));
    }

    /// A classified (non-retryable) burn failure must be skipped by the
    /// reconciler without consuming any recovery budget or resubmitting.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_skips_classified_burn_failures_without_budget() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_readiness(
                OrchestratorBurnReadiness::AllowanceInsufficient {
                    required: uint!(100_000000000000000000_U256),
                    current: U256::ZERO,
                },
            ),
        ))
        .await;

        // Drive to a classified BurnFailed state via the allowance gate.
        let _ = setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await;

        setup.manager.recover_unresolved_burns().await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "classified failure must stay Failed for manual recovery, \
             got {aggregate:?}"
        );
        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            0,
            "the reconciler must never resubmit a classified failure"
        );

        let budget_events = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type IN (
                  'RedemptionEvent::BurnRecoveryAttempted',
                  'RedemptionEvent::BurnPreparationRecoveryAttempted'
              )
            ",
        )
        .bind(setup.issuer_request_id.to_string())
        .fetch_one(&setup.harness.pool)
        .await
        .expect("budget-event count should query");
        assert_eq!(
            budget_events, 0,
            "skipping a classified failure must not consume recovery budget"
        );

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Skipping non-retryable classified burn failure",
                "AllowanceInsufficient",
            ]
        ));
    }

    /// A submitted orchestrator burn whose transaction already mined is
    /// confirmed by the reconciler, never rebroadcast or replaced.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_confirms_submitted_orchestrator_burn() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success(),
        ))
        .await;

        // Reach BurnSubmitted directly through the aggregate commands so the
        // pipeline's own confirm never runs.
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    params: BurnParams::Orchestrator {
                        token: setup.vault,
                        amount: uint!(100_000000000000000000_U256),
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("orchestrator intent should persist");
        let Redemption::BurnIntended { sendable_tx, .. } =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await
        else {
            panic!("expected BurnIntended");
        };
        let submitted = setup
            .vault_mock
            .submit_orchestrator_burn(
                &OrchestratorBurnParams {
                    orchestrator: setup.vault,
                    token: setup.vault,
                    amount: uint!(100_000000000000000000_U256),
                    owner: TEST_WALLET,
                    issuer_request_id: setup.issuer_request_id.clone(),
                    detected_tx_hash: B256::ZERO,
                    external_tx_id: None,
                },
                &sendable_tx,
            )
            .await
            .expect("mock orchestrator submit should succeed");
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::RecordOrchestratorBurnSubmitted {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    external_tx_id: BurnExternalTxId::from_string(
                        submitted.external_tx_id,
                    ),
                    tx_id: submitted.tx_id,
                },
            )
            .await
            .expect("orchestrator submission should persist");
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 1);

        setup.vault_mock.set_burn_tx_status(BurnTxStatus::Mined);
        setup.manager.recover_unresolved_burns().await;
        drive_submitted_burn_confirm(
            &setup.manager,
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Completed { .. }),
            "recovery must confirm the mined orchestrator burn, \
             got {aggregate:?}"
        );
        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            1,
            "a mined orchestrator burn must never be resubmitted"
        );
        assert_eq!(
            setup.recording.call_count(),
            0,
            "orchestrator recovery must not touch the receipt lifecycle"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Recovering BurnSubmitted redemption - enqueuing confirm job"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Burn confirmed successfully"]
        ));
    }

    /// Drives an orchestrator redemption to `BurnFailed` with the submitted
    /// transaction id retained on the `BurningFailed` event, so the reconciler
    /// takes the kept-tx-id confirm path.
    async fn drive_orchestrator_to_burn_failed_with_tx(
        setup: &OrchestratorTestSetup,
    ) {
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    params: BurnParams::Orchestrator {
                        token: setup.vault,
                        amount: uint!(100_000000000000000000_U256),
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("orchestrator intent should persist");
        let Redemption::BurnIntended { sendable_tx, .. } =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await
        else {
            panic!("expected BurnIntended");
        };
        let submitted = setup
            .vault_mock
            .submit_orchestrator_burn(
                &OrchestratorBurnParams {
                    orchestrator: setup.vault,
                    token: setup.vault,
                    amount: uint!(100_000000000000000000_U256),
                    owner: TEST_WALLET,
                    issuer_request_id: setup.issuer_request_id.clone(),
                    detected_tx_hash: B256::ZERO,
                    external_tx_id: None,
                },
                &sendable_tx,
            )
            .await
            .expect("mock orchestrator submit should succeed");
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::RecordOrchestratorBurnSubmitted {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    external_tx_id: BurnExternalTxId::from_string(
                        submitted.external_tx_id,
                    ),
                    tx_id: submitted.tx_id,
                },
            )
            .await
            .expect("orchestrator submission should persist");

        let Redemption::BurnSubmitted { tx_id, .. } =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await
        else {
            panic!("expected BurnSubmitted after BurnTokens");
        };
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: setup.issuer_request_id.clone(),
                    error: "confirmation ambiguity".to_string(),
                    tx_id: Some(tx_id),
                    planned_burns: vec![],
                },
            )
            .await
            .expect("burn failure should persist");
    }

    /// A landed orchestrator burn failure with a submitted transaction is now
    /// confirmed on-chain (no longer deferred): the reconciler records the
    /// existing burn and completes the redemption, without resubmitting or
    /// touching the receipt lifecycle.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_confirms_orchestrator_burn_failed_with_submitted_tx() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success(),
        ))
        .await;
        drive_orchestrator_to_burn_failed_with_tx(&setup).await;

        setup.manager.recover_unresolved_burns().await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Completed { .. }),
            "a mined orchestrator burn must be confirmed and completed, \
             got {aggregate:?}"
        );
        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            1,
            "recovery must confirm the existing burn, not resubmit"
        );
        assert_eq!(
            setup.recording.call_count(),
            0,
            "orchestrator recovery must not touch the receipt lifecycle"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Previously submitted orchestrator burn confirmed on-chain"]
        ));
    }

    /// An underfunded wallet defers an orchestrator `Burning` recovery with
    /// the DISTINCT `DeferredUnderfunded` outcome — funding the wallet lets
    /// the next pass retry automatically, so the operator must not be sent
    /// to the manual force-complete/close runbook
    /// (`SkippedManualIntervention`'s story).
    #[traced_test]
    #[tokio::test]
    async fn recover_burning_orchestrator_underfunded_defers() {
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                // Below the redemption's 100-token burn amount.
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let setup = setup_orchestrator_burning(vault_mock.clone()).await;

        let outcome = setup
            .manager
            .recover_single_burning(&setup.issuer_request_id)
            .await;

        assert!(
            matches!(outcome, Ok(RecoveryOutcome::DeferredUnderfunded)),
            "an underfunded orchestrator wallet must defer with the \
             distinct outcome, got {outcome:?}"
        );
        assert_eq!(
            vault_mock.orchestrator_submit_call_count(),
            0,
            "nothing may be submitted while the wallet is underfunded"
        );
        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "the deferral must leave the redemption in Burning, \
             got {aggregate:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &[
                "Insufficient wallet balance for orchestrator burn recovery",
                "deferring"
            ]
        ));
    }

    /// A confirmed orchestrator burn whose `Burned.amount` diverges from the
    /// redemption's own persisted `alpaca_quantity` must never terminalize:
    /// the recovery refuses before `RecordExistingBurn`, the redemption stays
    /// `Failed` for the operator, and the anomaly is logged at ERROR.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_refuses_diverging_confirmed_orchestrator_burn() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let setup = setup_orchestrator_burning(vault_mock.clone()).await;
        // Seed the confirm result BEFORE the drive: submit preserves a
        // pre-seeded result, so confirm reports one share-wei more than the
        // redemption's persisted quantity.
        vault_mock.seed_orchestrator_burn_result(OrchestratorBurnResult {
            tx_hash: B256::random(),
            shares_burned: uint!(100_000000000000000001_U256),
            burn_range: BurnRange {
                first_receipt_id: U256::ZERO,
                next_burn_receipt_id_after: U256::ONE,
            },
            gas_used: 50_000,
            block_number: 5_000,
        });
        drive_orchestrator_to_burn_failed_with_tx(&setup).await;

        setup.manager.recover_unresolved_burns().await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "a diverging confirmed burn must never terminalize, \
             got {aggregate:?}"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &[
                "diverges from the",
                "persisted alpaca_quantity",
                "refusing existing-burn",
                "recovery"
            ]
        ));
    }

    /// A pending (non-terminal) orchestrator confirmation keeps the redemption
    /// in `BurnFailed` with its tx id intact so the next reconciler pass
    /// retries confirmation rather than submitting a replacement.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_defers_pending_orchestrator_burn_failed() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_confirm_pending(),
        ))
        .await;
        drive_orchestrator_to_burn_failed_with_tx(&setup).await;

        setup.manager.recover_unresolved_burns().await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "a pending orchestrator confirmation must stay Failed, \
             got {aggregate:?}"
        );
        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            1,
            "a pending confirmation must not resubmit the burn"
        );
        assert_eq!(setup.recording.call_count(), 0);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Failed to confirm previously submitted orchestrator burn"]
        ));
    }

    /// A definitively reverted orchestrator confirmation terminalizes the
    /// redemption via `MarkFailed` (no reservation to release in orchestrator
    /// mode) and never resubmits or touches the receipt lifecycle.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_marks_reverted_orchestrator_burn_failed() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_confirm_revert(
                OrchestratorRevertReason::Unknown,
            ),
        ))
        .await;
        drive_orchestrator_to_burn_failed_with_tx(&setup).await;

        setup.manager.recover_unresolved_burns().await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "a reverted orchestrator burn stays Failed, got {aggregate:?}"
        );
        assert_eq!(
            setup.recording.call_count(),
            0,
            "orchestrator mode has no reservation to release"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Failed to recover BurnFailed redemption"]
        ));

        // MarkFailed moved the view out of BurnFailed, so a second reconciler
        // pass must not re-confirm the reverted transaction — the parked
        // redemption would otherwise loop confirm/MarkFailed on every pass.
        let confirms_after_first_pass =
            setup.vault_mock.orchestrator_confirm_call_count();
        setup.manager.recover_unresolved_burns().await;
        assert_eq!(
            setup.vault_mock.orchestrator_confirm_call_count(),
            confirms_after_first_pass,
            "a MarkFailed-parked redemption must leave the reconciler's scan"
        );
    }

    /// A definitively reverted orchestrator burn with an `Unclassified`
    /// reason goes through `record_definitive_confirm_failure` with a real
    /// persisted transaction: the replacement action is reserved, the dead
    /// transaction's id is dropped, and the next reconciler pass re-drives
    /// the burn through the mode-aware orchestrator path — never the
    /// vault-direct confirm-and-settle machinery.
    #[traced_test]
    #[tokio::test]
    async fn unclassified_orchestrator_revert_redrives_through_orchestrator() {
        let persisted_tx = SendableTxWithHash {
            tx: vec![1, 2, 3],
            hash: B256::random(),
            nonce: 7,
            signed_at: chrono::Utc::now(),
            dust_shares: U256::ZERO,
        };
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx)
                .with_orchestrator_confirm_revert(
                    OrchestratorRevertReason::Unknown,
                ),
        ))
        .await;

        setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await
            .expect("enqueuing the submit job is not an error");

        // No apalis worker drains the queue in unit tests, so drive the
        // enqueued submit then confirm inline. The sticky orchestrator revert
        // fails the confirm the same way the old inline path did.
        let execution = intended_execution(
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;
        let tx_id = setup
            .manager
            .submit_intended_burn(&setup.issuer_request_id, &execution)
            .await
            .expect(
                "submit_intended_burn should broadcast the orchestrator burn",
            );
        let result = setup
            .manager
            .confirm_submitted_burn(
                &setup.issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;
        assert!(result.is_err(), "the reverted burn must surface as an error");
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 1);

        // The dead transaction's id was dropped alongside the reserved
        // replacement action, so the recovery pass takes the preparation
        // retry (ResumeBurn -> handle_burning_started) rather than the
        // orchestrator-with-tx deferral or the vault-direct confirm path.
        setup.manager.recover_unresolved_burns().await;

        // The recovery pass re-enqueued the submit; drive it and the confirm
        // inline. The sticky revert fails the retry the same way.
        let execution = intended_execution(
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;
        let tx_id = setup
            .manager
            .submit_intended_burn(&setup.issuer_request_id, &execution)
            .await
            .expect(
                "submit_intended_burn should broadcast the orchestrator burn",
            );
        let _ = setup
            .manager
            .confirm_submitted_burn(
                &setup.issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;

        assert_eq!(
            setup.vault_mock.orchestrator_submit_call_count(),
            2,
            "recovery must re-drive the burn through the orchestrator path"
        );
        assert_eq!(
            setup.recording.call_count(),
            0,
            "the re-drive must never touch the vault-direct receipt \
             lifecycle"
        );
        // The sticky mock revert fails the retry the same way, so the
        // redemption parks in Failed again rather than completing.
        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "expected Failed after the reverted retry, got {aggregate:?}"
        );
    }

    /// With the recovery budget exhausted, a definitive confirm failure
    /// reserves NOTHING (`reserve_recovery_attempt` only persists
    /// `BurnRecoveryExhausted`), so the recorded `BurningFailed` must retain
    /// the dead transaction's id for operator inspection instead of dropping
    /// it as if a replacement action had been reserved.
    #[traced_test]
    #[tokio::test]
    async fn exhausted_budget_confirm_failure_retains_tx_id() {
        let persisted_tx = SendableTxWithHash {
            tx: vec![1, 2, 3],
            hash: B256::random(),
            nonce: 7,
            signed_at: chrono::Utc::now(),
            dust_shares: U256::ZERO,
        };
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx.clone()),
        ))
        .await;

        // Persist the real transaction, then spend the whole automatic
        // recovery budget against it.
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    params: BurnParams::Orchestrator {
                        token: setup.vault,
                        amount: uint!(100_000000000000000000_U256),
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("orchestrator intent should persist");
        let Redemption::BurnIntended { sendable_tx, metadata, .. } =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await
        else {
            panic!("expected BurnIntended");
        };
        setup
            .harness
            .store
            .send(
                &setup.issuer_request_id,
                RedemptionCommand::RecordOrchestratorBurnSubmitted {
                    issuer_request_id: setup.issuer_request_id.clone(),
                    external_tx_id: BurnExternalTxId::base(
                        &metadata.detected_tx_hash,
                    ),
                    tx_id: sendable_tx.hash.into(),
                },
            )
            .await
            .expect("orchestrator submission should persist");
        record_test_recovery_attempts(
            &setup.harness.store,
            &setup.issuer_request_id,
            &persisted_tx,
            BurnRecoveryAction::Rebroadcast,
            MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        )
        .await;

        setup
            .manager
            .record_definitive_confirm_failure(
                Network::Base,
                &setup.issuer_request_id,
                DefinitiveConfirmFailure {
                    vault: setup.vault,
                    is_orchestrator: true,
                    classification: &BurnFailureClassification::Unclassified,
                    error: "mined revert",
                    tx_id: &TxId::Hash(persisted_tx.hash),
                    planned_burns: &[],
                },
            )
            .await
            .expect("definitive failure should record");

        // The exhaustion marker proves the reservation was refused
        // (`Some(false)`), not skipped for missing persisted bytes.
        let exhausted_events = sqlx::query_scalar::<_, i64>(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryExhausted'
            ",
        )
        .bind(setup.issuer_request_id.to_string())
        .fetch_one(&setup.harness.pool)
        .await
        .expect("exhaustion count should query");
        assert_eq!(exhausted_events, 1, "the budget must have been exhausted");

        let failed = find_burn_failed(&setup.harness.pool)
            .await
            .expect("burn-failed query should succeed");
        let (_, view) = failed
            .iter()
            .find(|(id, _)| *id == setup.issuer_request_id)
            .expect("redemption must be in BurnFailed view state");
        assert!(
            matches!(
                view,
                RedemptionView::BurnFailed {
                    tx_id: Some(TxId::Hash(hash)),
                    ..
                } if *hash == persisted_tx.hash
            ),
            "an exhausted-budget failure must retain the tx identity, \
             got {view:?}"
        );
    }

    /// A pre-submit health-gate deferral leaves the redemption in `Burning`;
    /// the next reconciler pass re-checks health and completes the burn once
    /// the orchestrator is healthy again.
    #[traced_test]
    #[tokio::test]
    async fn reconciler_redrives_burn_after_orchestrator_recovers() {
        let setup = setup_orchestrator_burning(Arc::new(
            MockVaultService::new_success().with_orchestrator_readiness(
                OrchestratorBurnReadiness::VaultLogicMismatch,
            ),
        ))
        .await;

        setup
            .manager
            .handle_burning_started(&setup.issuer_request_id, &setup.aggregate)
            .await
            .expect("health-gate deferral is not an error");
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 0);

        setup
            .vault_mock
            .set_orchestrator_readiness(OrchestratorBurnReadiness::Ready);
        setup.manager.recover_unresolved_burns().await;

        drive_intended_burn_to_completion(
            &setup.manager,
            &setup.harness.store,
            &setup.issuer_request_id,
            setup.vault,
        )
        .await;

        let aggregate =
            load_aggregate(&setup.harness.store, &setup.issuer_request_id)
                .await;
        assert!(
            matches!(aggregate, Redemption::Completed { .. }),
            "the re-drive must complete once the orchestrator is healthy, \
             got {aggregate:?}"
        );
        assert_eq!(setup.vault_mock.orchestrator_submit_call_count(), 1);
        assert_eq!(
            setup.vault_mock.orchestrator_readiness_call_count(),
            2,
            "each attempt must re-check orchestrator health"
        );
    }

    async fn record_test_recovery_attempts(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        sendable_tx: &SendableTxWithHash,
        action: BurnRecoveryAction,
        count: u32,
    ) {
        for _ in 0..count {
            store
                .send(
                    issuer_request_id,
                    RedemptionCommand::RecordBurnRecoveryAttempt {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: sendable_tx.hash,
                        nonce: sendable_tx.nonce,
                        action,
                    },
                )
                .await
                .expect("recovery attempt should persist");
        }
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_success() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );

        // A successful burn must settle (consume) its reservation; if the
        // settle wiring were removed the reservation would linger here.
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "successful burn must leave no dangling reservation"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_force_complete_burn_records_verified_burn() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                // The proof must carry the persisted transaction's nonce and
                // the exact per-receipt plan — force-complete binds both.
                .with_verified_burns_and_total(
                    45_989_009,
                    persisted_tx.nonce,
                    uint!(17_U256),
                    vec![VerifiedBurn {
                        sender: owner,
                        receiver: owner,
                        receipt_id: uint!(42_U256),
                        shares_burned: uint!(17_U256),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        // Seed a held reservation so the test fails if force-complete stops
        // settling inventory after terminalizing the aggregate.
        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(42_U256),
                    shares_burned: uint!(17_U256),
                }],
            )
            .await
            .expect("seeding reservation should succeed");
        assert_eq!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap(),
            vec![issuer_request_id.clone()],
            "reservation should be held before force-complete"
        );
        persist_test_burn_intent(store, &issuer_request_id, vault, owner).await;

        let burn_tx_hash = persisted_tx.hash;

        let verification = manager
            .force_complete_burn(
                &issuer_request_id,
                burn_tx_hash,
                "burn confirmed on-chain".to_string(),
                None,
            )
            .await
            .expect("force-complete should succeed");

        // Block number and shares are taken from the on-chain verification,
        // not the operator.
        assert_eq!(verification.block_number, 45_989_009);
        assert_eq!(verification.shares_burned, uint!(17_U256));

        let updated = load_aggregate(store, &issuer_request_id).await;
        let Redemption::Completed { burn_tx_hash: recorded, .. } = updated
        else {
            panic!("Expected Completed state, got {updated:?}");
        };
        assert_eq!(recorded, burn_tx_hash);

        // Force-complete must settle (consume) the held reservation; if the
        // settle wiring were removed the reservation would linger here.
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "force-complete must leave no dangling reservation"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Force-completing stuck Burning redemption", "verified on-chain"]
        ));
    }

    /// Amount an orchestrator redemption seeded by
    /// [`create_orchestrator_redemption_in_burning_state`] burns:
    /// `alpaca_quantity` (100) converted to 18-decimal share-wei.
    const ORCHESTRATOR_BURN_AMOUNT: U256 = uint!(100_000000000000000000_U256);

    async fn persist_test_orchestrator_burn_intent(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        token: Address,
        owner: Address,
    ) {
        store
            .send(
                issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::Orchestrator {
                        token,
                        amount: ORCHESTRATOR_BURN_AMOUNT,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("orchestrator burn intent should persist");
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_orchestrator_persisted_hash_skips_receipt_lifecycle()
     {
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns_and_total(
                    45_989_009,
                    persisted_tx.nonce,
                    ORCHESTRATOR_BURN_AMOUNT,
                    vec![],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let recording = Arc::new(RecordingReceiptService::new(
            harness.receipt_service.clone(),
        ));
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            harness.pool.clone(),
            harness.store.clone(),
            recording.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_orchestrator_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        persist_test_orchestrator_burn_intent(
            &harness.store,
            &issuer_request_id,
            vault,
            owner,
        )
        .await;

        let verification = manager
            .force_complete_burn(
                &issuer_request_id,
                persisted_tx.hash,
                "orchestrator burn confirmed".to_string(),
                None,
            )
            .await
            .expect("orchestrator force-complete should succeed");

        assert_eq!(verification.shares_burned, ORCHESTRATOR_BURN_AMOUNT);
        assert!(matches!(
            load_aggregate(&harness.store, &issuer_request_id).await,
            Redemption::Completed { .. }
        ));
        assert_eq!(
            recording.call_count(),
            0,
            "orchestrator force-complete must never touch the receipt service"
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Force-completing stuck Burning redemption", "verified on-chain"]
        ));
    }

    #[tokio::test]
    async fn force_complete_orchestrator_accepts_matching_alternate_hash() {
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns_and_total(
                    45_989_009,
                    persisted_tx.nonce,
                    ORCHESTRATOR_BURN_AMOUNT,
                    vec![],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let recording = Arc::new(RecordingReceiptService::new(
            harness.receipt_service.clone(),
        ));
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            harness.pool.clone(),
            harness.store.clone(),
            recording.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_orchestrator_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        persist_test_orchestrator_burn_intent(
            &harness.store,
            &issuer_request_id,
            vault,
            owner,
        )
        .await;

        manager
            .force_complete_burn(
                &issuer_request_id,
                B256::random(),
                "alternate orchestrator burn".to_string(),
                Some(persisted_tx.hash),
            )
            .await
            .expect("matching alternate orchestrator burn should complete");

        assert!(matches!(
            load_aggregate(&harness.store, &issuer_request_id).await,
            Redemption::Completed { .. }
        ));
        assert_eq!(recording.call_count(), 0);
    }

    /// Force-complete binds an orchestrator proof to this redemption per
    /// SPEC "ForceCompleteBurn": a proof at the wrong nonce (could be
    /// another redemption's burn, and the acknowledged transaction could
    /// still land), with the wrong burned amount, or carrying stray share
    /// transfers (an orchestrator burn moves nothing besides the
    /// pull-and-burn legs) is rejected before any state change — and the
    /// receipt service is never touched either way.
    #[tokio::test]
    async fn force_complete_orchestrator_rejects_unbound_proofs() {
        let recipient = address!("0x1234567890abcdef1234567890abcdef12345678");
        for (scenario, verified_nonce, verified_shares, verified_transfers) in [
            ("nonce mismatch", 8, ORCHESTRATOR_BURN_AMOUNT, vec![]),
            ("amount mismatch", 7, uint!(99_000000000000000000_U256), vec![]),
            (
                "stray share transfer",
                7,
                ORCHESTRATOR_BURN_AMOUNT,
                vec![VerifiedShareTransfer {
                    recipient,
                    shares: uint!(1_U256),
                }],
            ),
        ] {
            let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            let persisted_tx = SendableTxWithHash::valid_for_test(
                7,
                vault,
                Bytes::from_static(&[0xde, 0xad]),
            );
            let owner = persisted_tx.signer_for_test();
            let vault_mock = Arc::new(
                MockVaultService::new_success()
                    .with_verified_burns_and_total(
                        45_989_009,
                        verified_nonce,
                        verified_shares,
                        vec![],
                        verified_transfers,
                    )
                    .with_prepared_tx(persisted_tx.clone()),
            );
            let harness =
                TestHarness::with_vault_mock(vault_mock.clone()).await;
            harness
                .add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault)
                .await;
            let recording = Arc::new(RecordingReceiptService::new(
                harness.receipt_service.clone(),
            ));
            let manager = BurnManager::new_for_tests(
                vault_mock.clone(),
                harness.pool.clone(),
                harness.store.clone(),
                recording.clone(),
                owner,
                ANVIL_CHAIN_ID,
                harness.apalis_pool.clone(),
            );

            let issuer_request_id = IssuerRedemptionRequestId::random();
            create_orchestrator_redemption_in_burning_state(
                &harness.store,
                &issuer_request_id,
            )
            .await;
            persist_test_orchestrator_burn_intent(
                &harness.store,
                &issuer_request_id,
                vault,
                owner,
            )
            .await;

            let err = manager
                .force_complete_burn(
                    &issuer_request_id,
                    B256::random(),
                    scenario.to_string(),
                    Some(persisted_tx.hash),
                )
                .await
                .unwrap_err();

            let rejected_as_expected = match scenario {
                "nonce mismatch" => matches!(
                    err,
                    BurnManagerError::ForceCompleteNonceMismatch {
                        proof_nonce: 8,
                        persisted_nonce: 7,
                    }
                ),
                "amount mismatch" => matches!(
                    err,
                    BurnManagerError::ForceCompleteAmountMismatch { .. }
                ),
                "stray share transfer" => matches!(
                    err,
                    BurnManagerError::ForceCompleteStrayTransfer { .. }
                ),
                _ => false,
            };
            assert!(
                rejected_as_expected,
                "scenario {scenario}: unexpected error {err:?}"
            );

            assert!(
                matches!(
                    load_aggregate(&harness.store, &issuer_request_id).await,
                    Redemption::BurnIntended { .. }
                ),
                "scenario {scenario} must leave the redemption BurnIntended"
            );
            assert_eq!(
                recording.call_count(),
                0,
                "scenario {scenario} touched the receipt service"
            );
        }
    }

    /// An orchestrator force-complete whose burn does not verify on-chain is
    /// rejected before any state change: `verify_burn_tx` fails, so the
    /// `ForceCompleteBurn` command is never sent, the redemption stays
    /// `BurnIntended`, and the receipt service is never touched.
    #[tokio::test]
    async fn force_complete_orchestrator_rejects_unverifiable_burn() {
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_unverifiable_burn()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let recording = Arc::new(RecordingReceiptService::new(
            harness.receipt_service.clone(),
        ));
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            harness.pool.clone(),
            harness.store.clone(),
            recording.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_orchestrator_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        persist_test_orchestrator_burn_intent(
            &harness.store,
            &issuer_request_id,
            vault,
            owner,
        )
        .await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                persisted_tx.hash,
                "operator hash is not a burn".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::Vault(VaultError::NotABurn { .. })
        ));
        assert!(
            matches!(
                load_aggregate(&harness.store, &issuer_request_id).await,
                Redemption::BurnIntended { .. }
            ),
            "an unverifiable burn must leave the redemption BurnIntended"
        );
        assert_eq!(
            recording.call_count(),
            0,
            "a rejected force-complete must not touch the receipt service"
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_unverifiable_burn() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_unverifiable_burn()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(store, &issuer_request_id, vault, owner).await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                persisted_tx.hash,
                "operator hash is not a burn".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::Vault(VaultError::NotABurn { .. })
        ));

        // An unverifiable hash must NOT terminalize — the redemption stays
        // BurnIntended for manual reconciliation.
        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::BurnIntended { .. }),
            "Expected BurnIntended state, got {updated:?}"
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_reverted_tx() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_reverted_burn()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(store, &issuer_request_id, vault, owner).await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                persisted_tx.hash,
                "operator hash reverted".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::Vault(VaultError::Reverted { .. })
        ));

        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::BurnIntended { .. }),
            "Expected BurnIntended state, got {updated:?}"
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_burning_without_persisted_hash() {
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burn(45_989_009, uint!(17_U256)),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let manager = BurnManager::new_for_tests(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                B256::random(),
                "another redemption's verified burn".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnManagerError::Redemption(
                RedemptionError::PersistedBurnHashUnavailable
            ))
        ));
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::Burning { .. }
        ));
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_non_burning_state() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        // An unknown redemption is Uninitialized — force-complete must refuse
        // before ever touching the chain.
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                b256!(
                    "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
                ),
                "wrong state".to_string(),
                None
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::InvalidAggregateState { .. }
        ));
        assert_eq!(vault_mock.get_multi_burn_call_count(), 0);
    }

    #[traced_test]
    #[tokio::test]
    async fn test_handle_burning_started_passes_retry_external_tx_id() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let retry_external_tx_id =
            BurnExternalTxId::from_string("burn-0xabc-retry-1".to_string());

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Thread the retry externalTxId through a real persisted BurnResumed
        // event (Failed → Burning) so we verify the full apply path, not an
        // in-memory mutation.
        let burning =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let Redemption::Burning {
            metadata,
            tokenization_request_id,
            alpaca_quantity,
            dust_quantity,
            called_at,
            alpaca_journal_completed_at,
            ..
        } = burning
        else {
            panic!("Expected Burning state, got {burning:?}");
        };

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "burn failed".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("RecordBurnFailure failed");

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ResumeBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    metadata,
                    tokenization_request_id,
                    alpaca_quantity,
                    dust_quantity,
                    called_at,
                    alpaca_journal_completed_at,
                    external_tx_id: Some(retry_external_tx_id.clone()),
                },
            )
            .await
            .expect("ResumeBurn failed");

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");

        let params = vault_mock
            .get_last_multi_burn_params()
            .expect("Expected multi_burn to have been called");

        assert_eq!(params.external_tx_id, Some(retry_external_tx_id));

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Starting on-chain burning process"]
        ));
    }

    #[tokio::test]
    async fn test_burn_preserves_receipt_info_in_multi_burn_entry() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            store,
            receipt_service,
            receipt_inventory_store,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-mint-99"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(Decimal::new(10000, 2)),
            Utc::now(),
            None,
        );

        receipt_inventory_store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(uint!(99_U256)),
                    balance: Shares::from(
                        uint!(100_000000000000000000_U256),
                    ),
                    block_number: 1,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::Itn {
                        issuer_request_id: IssuerMintRequestId::random(),
                    },
                    receipt_info: Some(Box::new(receipt_info.clone())),
                    receipt_info_bytes: None,
                },
            )
            .await
            .expect("Failed to discover receipt with receipt_info");

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let intended = load_aggregate(store, &issuer_request_id).await;
        let Redemption::BurnIntended {
            metadata,
            dust_quantity,
            planned_burns,
            external_tx_id,
            ..
        } = intended
        else {
            panic!("expected BurnIntended, got {intended:?}");
        };
        let dust_shares = dust_quantity.to_u256_with_18_decimals().unwrap();
        let burns: Vec<MultiBurnEntry> = planned_burns
            .iter()
            .map(|burn| MultiBurnEntry {
                receipt_id: burn.receipt_id,
                burn_shares: burn.shares_burned,
                receipt_info: Some(receipt_info.clone()),
                receipt_info_bytes: None,
            })
            .collect();
        let execution = BurnExecutionPlan {
            network: metadata.network,
            vault,
            params: BurnParams::VaultDirect {
                vault,
                burns,
                dust_shares,
                owner: TEST_WALLET,
            },
            planned_burns,
            dust_shares,
            external_tx_id,
        };
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");

        let params = vault_mock
            .get_last_multi_burn_params()
            .expect("Expected multi_burn to have been called");

        assert_eq!(params.burns.len(), 1);
        assert_eq!(
            params.burns[0].receipt_info.as_ref(),
            Some(&receipt_info),
            "MultiBurnEntry should preserve the original receipt_info"
        );
    }

    /// Verifies that when a receipt has `receipt_info_bytes` set, those exact
    /// bytes flow through to `MultiBurnParams` for the vault service to pass
    /// back to `redeem()` without re-encoding.
    #[traced_test]
    #[tokio::test]
    async fn test_receipt_info_bytes_flows_through_to_multi_burn_params() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness {
            store,
            receipt_service,
            receipt_inventory_store,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-bytes-test"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(Decimal::from(50)),
            Utc::now(),
            None,
        );

        let raw_bytes = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);

        receipt_inventory_store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(uint!(99_U256)),
                    balance: Shares::from(
                        uint!(100_000000000000000000_U256),
                    ),
                    block_number: 1,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::Itn {
                        issuer_request_id: IssuerMintRequestId::random(),
                    },
                    receipt_info: Some(Box::new(receipt_info.clone())),
                    receipt_info_bytes: Some(raw_bytes.clone()),
                },
            )
            .await
            .expect("Failed to discover receipt with receipt_info_bytes");

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let intended = load_aggregate(store, &issuer_request_id).await;
        let Redemption::BurnIntended {
            metadata,
            dust_quantity,
            planned_burns,
            external_tx_id,
            ..
        } = intended
        else {
            panic!("expected BurnIntended, got {intended:?}");
        };
        let dust_shares = dust_quantity.to_u256_with_18_decimals().unwrap();
        let burns: Vec<MultiBurnEntry> = planned_burns
            .iter()
            .map(|burn| MultiBurnEntry {
                receipt_id: burn.receipt_id,
                burn_shares: burn.shares_burned,
                receipt_info: Some(receipt_info.clone()),
                receipt_info_bytes: Some(raw_bytes.clone()),
            })
            .collect();
        let execution = BurnExecutionPlan {
            network: metadata.network,
            vault,
            params: BurnParams::VaultDirect {
                vault,
                burns,
                dust_shares,
                owner: TEST_WALLET,
            },
            planned_burns,
            dust_shares,
            external_tx_id,
        };
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");

        let params = vault_mock
            .get_last_multi_burn_params()
            .expect("Expected multi_burn to have been called");

        assert_eq!(params.burns.len(), 1);
        assert_eq!(
            params.burns[0].receipt_info_bytes.as_ref(),
            Some(&raw_bytes),
            "MultiBurnEntry should preserve the original receipt_info_bytes"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_ambiguous_failure_stays_recoverable()
     {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        let tx_id = manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit ok");
        let result = manager
            .confirm_submitted_burn(
                &issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::Redemption(
                    RedemptionError::Vault { .. }
                ))
            ),
            "Expected blockchain error, got {result:?}"
        );

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::BurnSubmitted { .. }),
            "ambiguous confirmation must remain recoverable, got {updated_aggregate:?}"
        );

        // The confirmation failed with an ambiguous (non-terminal) error, so
        // the reservation is intentionally RETAINED — the transaction may still
        // land on-chain, and releasing now could let a concurrent redemption
        // reuse shares that are about to be consumed.
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "ambiguous confirmation must retain the reservation"
        );
    }

    /// A burn SUBMISSION failure does not prove no transaction was created
    /// (e.g. a duplicate externalTxId whose lookup failed), so the reservation
    /// must be RETAINED — releasing could let a concurrent redemption reuse the
    /// balance and double-submit.
    #[tokio::test]
    async fn test_burn_submission_failure_retains_reservation() {
        let vault_mock = Arc::new(MockVaultService::new_submit_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        let result =
            manager.submit_intended_burn(&issuer_request_id, &execution).await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::Redemption(RedemptionError::Vault {
                    release_reservation: false,
                    ..
                }))
            ),
            "Expected submission failure that retains the reservation, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "an ambiguous submission failure must retain the reservation"
        );
    }

    /// A submission that fails with a DEFINITIVE on-chain revert
    /// (`VaultError::Reverted`, as the synchronous local backend produces)
    /// consumed no receipts, so the reservation must be RELEASED — unlike an
    /// ambiguous submit failure, which is retained.
    #[tokio::test]
    async fn test_submit_revert_releases_reservation() {
        let vault_mock = Arc::new(MockVaultService::new_submit_revert());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        let result =
            manager.submit_intended_burn(&issuer_request_id, &execution).await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::Redemption(RedemptionError::Vault {
                    release_reservation: true,
                    ..
                }))
            ),
            "Expected a revert submission failure that releases the reservation, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "a definitive on-chain revert at submit must release the reservation"
        );
    }

    /// A confirmation that fails with a DEFINITIVE on-chain revert
    /// (`VaultError::Reverted`) consumed no receipts, so the reservation must be
    /// RELEASED (exercises the `should_release_reserved_burn`-gated release in
    /// the confirm-failure path).
    #[tokio::test]
    async fn test_confirm_revert_releases_reservation() {
        let vault_mock = Arc::new(MockVaultService::new_confirm_revert());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started only enqueues now");

        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        let tx_id = manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit ok");
        let result = manager
            .confirm_submitted_burn(
                &issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::Redemption(RedemptionError::Vault {
                    release_reservation: true,
                    ..
                }))
            ),
            "Expected a revert confirmation failure that releases the reservation, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "a definitive on-chain revert must release the reservation"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_insufficient_balance() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::InsufficientBalance { .. })),
            "Expected InsufficientBalance error, got {result:?}"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Insufficient balance"),
            "Expected error message about insufficient balance, got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_wrong_state_fails() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("TSLA").unwrap();
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    network: Network::Base,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                },
            )
            .await
            .unwrap();

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(
                result,
                Err(BurnManagerError::InvalidAggregateState { .. })
            ),
            "Expected InvalidAggregateState error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_complete_redemption_with_burn() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_partial_burn_receipt_remains_active() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying_symbol = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying_symbol, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(43_U256),
                uint!(200_000000000000000000_U256),
            )
            .await;

        let tokenization_request_id =
            TokenizationRequestId::new("alp-partial-burn");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let block_number = 22222;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: underlying.clone(),
                    token,
                    network: Network::Base,
                    wallet,
                    quantity: quantity.clone(),
                    tx_hash,
                    block_number,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ClaimAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    alpaca_quantity: quantity,
                    dust_quantity: Quantity::new(Decimal::ZERO),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_depletes_receipt() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(44_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_with_multiple_receipts() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(45_U256),
                uint!(50_000000000000000000_U256),
            )
            .await;

        harness
            .discover_receipt(
                vault,
                uint!(46_U256),
                uint!(200_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_insufficient_balance_scenario() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::InsufficientBalance { .. })),
            "Expected InsufficientBalance error, got {result:?}"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Insufficient balance"),
            "Expected error message about insufficient balance, got: {reason}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_redemptions_empty() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        manager.recover_burning_redemptions().await;
    }

    #[tokio::test]
    async fn test_recover_burning_records_failure_when_network_not_configured()
    {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness
            .asset_store
            .send(
                &AssetKey::new(underlying.clone(), Network::Ethereum),
                TokenizedAssetCommand::Add {
                    underlying: underlying.clone(),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::Ethereum,
                    vault,
                },
            )
            .await
            .unwrap();

        let tokenization_request_id =
            TokenizationRequestId::new("alp-burn-eth");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::Detect {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    network: Network::Ethereum,
                    wallet,
                    quantity: quantity.clone(),
                    tx_hash,
                    block_number: 12345,
                    burn_mode: VaultMode::VaultDirect,
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ClaimAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id,
                    alpaca_quantity: quantity,
                    dust_quantity: Quantity::new(Decimal::ZERO),
                },
            )
            .await
            .unwrap();

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .unwrap();

        manager.recover_burning_redemptions().await;

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed after unconfigured-network recovery, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_redemptions_with_valid_redemption() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        manager.recover_burning_redemptions().await;

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_skips_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        // Create a redemption in Burning state (needs 100 shares)
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        // Recovery should skip this redemption without attempting burn
        manager.recover_burning_redemptions().await;

        // No burn should have been attempted
        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn when on-chain balance is insufficient"
        );

        // Redemption should stay in Burning state (not move to Failed)
        let aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "Expected Burning state unchanged when balance insufficient, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burning_skips_non_burning_state() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    underlying,
                    token,
                    network: Network::Base,
                    wallet,
                    quantity,
                    tx_hash,
                    block_number,
                },
            )
            .await
            .unwrap();

        manager.recover_burning_redemptions().await;

        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn for Detected state"
        );

        let aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(aggregate, Redemption::Detected { .. }),
            "Expected Detected state unchanged, got {aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burn_failed_redemptions() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Create redemption and progress to Burning state
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        // Record burn failure to transition to Failed/BurnFailed
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "Initial burn failed".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("Failed to record burn failure");

        // Verify aggregate is in Failed state
        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state, got {aggregate:?}"
        );

        // Recovery should find the BurnFailed view and retry
        manager.recover_burn_failed_redemptions().await;

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        // Burn should have been retried
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "Should have retried the burn"
        );

        // Aggregate should now be Completed
        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_recover_burn_failed_skips_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service =
            blockchain_service_mock.clone() as Arc<dyn VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "RPC timeout".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("Failed to record burn failure");

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Failed { .. }),
            "Expected Failed state before recovery, got {aggregate:?}"
        );

        // Recovery should skip due to insufficient balance
        manager.recover_burn_failed_redemptions().await;

        // No burn should have been attempted
        assert_eq!(
            blockchain_service_mock.get_multi_burn_call_count(),
            0,
            "Should not call burn when on-chain balance is insufficient"
        );

        // Aggregate should stay in Failed state (not re-fail or change)
        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Failed { .. }),
            "Expected Failed state unchanged when balance insufficient, got {updated_aggregate:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_marks_failed_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return 0 balance (burn already happened on-chain)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success().with_share_balance(uint!(0_U256)),
        );
        let blockchain_service =
            blockchain_service_mock.clone() as Arc<dyn VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "ERC1155: burn amount exceeds balance".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("Failed to record burn failure");

        // Recovery should auto-fail (MarkFailed) instead of just skipping
        manager.recover_burn_failed_redemptions().await;

        // The aggregate should have a new RedemptionFailed event
        // (from MarkFailed command, with reason about insufficient balance)
        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        let Redemption::Failed { reason, .. } = &updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains(
                "On-chain balance insufficient for BurnFailed recovery"
            ),
            "Expected auto-fail reason about insufficient balance, got: {reason}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Auto-failing BurnFailed redemption",
                "insufficient on-chain balance"
            ]
        ));
    }

    /// Seeds an orchestrator-mode redemption in `Burning` state with the given
    /// Alpaca and dust quantities, so balance-heuristic tests can vary the
    /// burn/dust split.
    async fn seed_orchestrator_burning_with_amounts(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        alpaca_quantity: Quantity,
        dust_quantity: Quantity,
    ) {
        store
            .send(
                issuer_request_id,
                RedemptionCommand::Detect {
                    burn_mode: VaultMode::Orchestrator {
                        address: test_orchestrator_address(),
                    },
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL").unwrap(),
                    token: TokenSymbol::new("tAAPL"),
                    wallet: address!(
                        "0x1234567890abcdef1234567890abcdef12345678"
                    ),
                    quantity: alpaca_quantity.clone(),
                    tx_hash: b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                    block_number: 12345,
                    network: Network::Base,
                },
            )
            .await
            .expect("Detect failed");
        store
            .send(
                issuer_request_id,
                RedemptionCommand::ClaimAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("ClaimAlpacaCall failed");
        store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-orch-bal",
                    ),
                    alpaca_quantity,
                    dust_quantity,
                },
            )
            .await
            .expect("RecordAlpacaCall failed");
        store
            .send(
                issuer_request_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("ConfirmAlpacaComplete failed");
    }

    /// An orchestrator `BurnFailed` redemption with no submitted tx and a
    /// balance below `burn_shares` is deferred (WARN, stays Failed), never
    /// auto-failed: no burn was submitted, so a prior unrecorded burn is
    /// impossible and a "burn landed" claim would be false.
    #[traced_test]
    #[tokio::test]
    async fn recover_burn_failed_orchestrator_defers_on_insufficient_balance() {
        let vault_mock = Arc::new(
            MockVaultService::new_success().with_share_balance(uint!(0_U256)),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_orchestrator_burning_with_amounts(
            store,
            &issuer_request_id,
            Quantity::new(Decimal::from(100)),
            Quantity::new(Decimal::ZERO),
        )
        .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "orchestrator burn preparation failed".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("RecordBurnFailure failed");

        manager.recover_burn_failed_redemptions().await;

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        let Redemption::Failed { reason, .. } = &aggregate else {
            panic!("expected Failed state, got {aggregate:?}");
        };
        assert!(
            !reason.contains("On-chain balance insufficient"),
            "orchestrator recovery must not auto-fail with a balance reason, \
             got: {reason}"
        );
        assert_eq!(
            vault_mock.orchestrator_submit_call_count(),
            0,
            "a deferred recovery must not submit a burn"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Insufficient wallet balance for orchestrator burn recovery"]
        ));
    }

    /// The orchestrator balance check ignores dust: a wallet holding exactly
    /// `burn_shares` (less than `burn_shares + dust`) still recovers, because
    /// the orchestrator never moves the dust on-chain.
    #[traced_test]
    #[tokio::test]
    async fn recover_burn_failed_orchestrator_ignores_dust_in_balance_check() {
        // Balance equals burn_shares (100e18) exactly — below burn + dust.
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(100_000000000000000000_U256)),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_orchestrator_burning_with_amounts(
            store,
            &issuer_request_id,
            Quantity::new(Decimal::from(100)),
            Quantity::new(Decimal::new(1, 9)),
        )
        .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "orchestrator burn preparation failed".to_string(),
                    tx_id: None,
                    planned_burns: vec![],
                },
            )
            .await
            .expect("RecordBurnFailure failed");

        manager.recover_burn_failed_redemptions().await;

        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Completed { .. }),
            "recovery must proceed when the wallet holds burn_shares, \
             got {aggregate:?}"
        );
        assert_eq!(vault_mock.orchestrator_submit_call_count(), 1);
    }

    /// An orchestrator redemption stuck in `Burning` with a balance below
    /// `burn_shares` defers (WARN, stays Burning) without submitting a burn —
    /// the persist-before-broadcast invariant means nothing was signed, so a
    /// low balance is an underfunded wallet, not a landed burn.
    #[traced_test]
    #[tokio::test]
    async fn recover_burning_orchestrator_defers_on_insufficient_balance() {
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_orchestrator_burning_with_amounts(
            store,
            &issuer_request_id,
            Quantity::new(Decimal::from(100)),
            Quantity::new(Decimal::ZERO),
        )
        .await;

        manager.recover_burning_redemptions().await;

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::Burning { .. }),
            "an underfunded orchestrator burn stays Burning, got {aggregate:?}"
        );
        assert_eq!(
            vault_mock.orchestrator_submit_call_count(),
            0,
            "a deferred recovery must not submit a burn"
        );
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Insufficient wallet balance for orchestrator burn recovery"]
        ));
    }

    /// A `BurnFailed` redemption with a `tx_id` means the signing backend
    /// already submitted a transaction before the failure was recorded.
    /// On recovery, `recover_burn_failed_with_existing_tx` must confirm the
    /// existing tx rather than re-submit, settle the reservation, and
    /// transition the aggregate to `Completed`.
    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_with_existing_tx_confirms_and_completes()
    {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("seeding reservation should succeed");

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "polling timeout".to_string(),
                    tx_id: Some(TxId::random()),
                    planned_burns: vec![BurnRecord {
                        receipt_id: uint!(99_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                },
            )
            .await
            .expect("RecordBurnFailure should succeed");

        manager.recover_burn_failed_redemptions().await;

        // confirm_burn was called; no fresh submit should happen
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            0,
            "existing-tx recovery must not submit a new burn"
        );

        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::Completed { .. }),
            "Expected Completed after confirming existing tx, got {updated:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "reservation must be settled after successful confirmation"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Previously submitted burn confirmed on-chain"]
        ));
    }

    /// When a `BurnFailed` redemption has a `tx_id` and the confirmation call
    /// returns `VaultError::Reverted` (the tx was definitively rejected),
    /// recovery must release the reservation — so inventory is not over-reserved
    /// — and mark the redemption as permanently failed.
    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_with_existing_tx_revert_releases_reservation_and_marks_failed()
     {
        let vault_mock = Arc::new(MockVaultService::new_confirm_revert());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let tx_id = TxId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("seeding reservation should succeed");

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "polling timeout".to_string(),
                    tx_id: Some(tx_id.clone()),
                    planned_burns: vec![BurnRecord {
                        receipt_id: uint!(99_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                },
            )
            .await
            .expect("RecordBurnFailure should succeed");

        manager.recover_burn_failed_redemptions().await;

        let updated = load_aggregate(store, &issuer_request_id).await;
        let Redemption::Failed { reason, .. } = &updated else {
            panic!("Expected Failed after revert, got {updated:?}");
        };
        assert!(
            reason.contains(&tx_id.to_string()),
            "failure reason must include the tx id, got: {reason}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "reservation must be released after a definitive on-chain revert"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["BurnFailed recovery", &tx_id.to_string()]
        ));
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Failed to recover BurnFailed redemption"]
        ));
    }

    /// When a `BurnFailed` redemption has a `tx_id` and `confirm_burn` fails
    /// with a non-terminal error (RPC blip, pending-receipt timeout — any
    /// error for which `should_release_reserved_burn` returns false), the
    /// recovery pass must leave the redemption in `BurnFailed` with `tx_id`
    /// intact and the reservation held so the next pass retries confirmation
    /// rather than submitting a replacement that could mine alongside the
    /// still-pending original.
    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_with_existing_tx_transient_error_keeps_polling()
     {
        // new_failure() makes confirm_burn return Err(VaultError::InvalidReceipt),
        // a non-terminal error (should_release_reserved_burn returns false).
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let tx_id = TxId::random();

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("seeding reservation should succeed");

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
                    classification: BurnFailureClassification::Unclassified,
                    issuer_request_id: issuer_request_id.clone(),
                    error: "rpc timeout".to_string(),
                    tx_id: Some(tx_id.clone()),
                    planned_burns: vec![BurnRecord {
                        receipt_id: uint!(99_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                },
            )
            .await
            .expect("RecordBurnFailure should succeed");

        manager.recover_burn_failed_redemptions().await;

        // A non-terminal error must not submit a replacement burn.
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            0,
            "transient confirm error must not submit a new burn"
        );

        // View must still show BurnFailed (not Failed) — tx_id intact,
        // so the next recovery pass retries confirm_burn rather than
        // treating the redemption as permanently done.
        let burn_failed_views = find_burn_failed(pool)
            .await
            .expect("find_burn_failed should succeed");
        assert!(
            burn_failed_views.iter().any(|(id, _)| id == &issuer_request_id),
            "redemption must still appear in BurnFailed view for next-pass retry"
        );

        // Reservation remains held — releasing would free shares that the
        // still-pending tx may yet consume.
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "reservation must be retained after a non-terminal confirm error"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["BurnFailed recovery", &tx_id.to_string()]
        ));
    }

    /// Exhausts the automatic recovery budget: each pass now enqueues a
    /// durable job (never broadcasts inline), and reaching the cap persists
    /// exhaustion, keeps the reservation, and signs no further transaction.
    async fn exhaust_restarted_recovery_budget(
        manager: &BurnManager,
        vault_mock: &MockVaultService,
        receipt_service: &Arc<dyn ReceiptService>,
        pool: &SqlitePool,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
        transactions: (SendableTxWithHash, &SendableTxWithHash),
    ) {
        let (prepared_tx, replacement_tx) = transactions;
        vault_mock.set_burn_tx_status(BurnTxStatus::StillMineable);

        // Recovery now enqueues a SubmitBurnJob per pass instead of
        // broadcasting inline; the broadcast count reflects only the two
        // drives the caller already performed (persisted tx + replacement).
        let broadcasts_before = vault_mock.get_multi_burn_call_count();

        for _ in 0..3 {
            assert!(matches!(
                manager.recover_single_burning(issuer_request_id).await,
                Ok(RecoveryOutcome::EnqueuedBurnJob)
            ));
        }
        let classifications_at_cap =
            vault_mock.burn_classification_call_count();
        let replacements_at_cap =
            vault_mock.replacement_preparation_call_count();
        assert!(matches!(
            manager.recover_single_burning(issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(
            vault_mock.burn_classification_call_count(),
            classifications_at_cap,
            "reaching the durable cap must persist exhaustion before classification"
        );
        assert_eq!(
            vault_mock.replacement_preparation_call_count(),
            replacements_at_cap,
            "reaching the durable cap must not sign another transaction"
        );
        assert!(matches!(
            manager.recover_single_burning(issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(
            vault_mock.burn_classification_call_count(),
            classifications_at_cap,
            "persisted exhaustion must skip classification RPCs"
        );
        assert_eq!(
            vault_mock.replacement_preparation_call_count(),
            replacements_at_cap,
            "persisted exhaustion must skip replacement signing"
        );
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            broadcasts_before,
            "enqueue-only recovery must not broadcast inline, and a \
             BurnSubmitted burn is never re-broadcast"
        );
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .expect("exhausted reservation query should succeed")
                .contains(issuer_request_id),
            "exhaustion must keep the receipt reservation held"
        );
        assert_eq!(
            vault_mock.submitted_burn_txs(),
            vec![prepared_tx, replacement_tx.clone()],
            "only the two driven broadcasts reached the chain"
        );
        let aggregate_id = issuer_request_id.to_string();
        let exhaustion_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryExhausted'
            ",
        )
        .bind(aggregate_id)
        .fetch_one(pool)
        .await
        .expect("exhaustion event count should load");
        assert_eq!(exhaustion_events, 1);
        assert_eq!(
            log_count_at!(
                tracing::Level::ERROR,
                &[
                    "Automatic burn recovery exhausted",
                    &replacement_tx.hash.to_string(),
                    "operator_action",
                    "force-complete",
                ]
            ),
            1,
            "recovery exhaustion must emit one actionable error"
        );
    }

    /// Discovers the funding receipt, reserves it, and drives `IntendBurn` so
    /// the redemption reaches `BurnIntended` with a persisted signed
    /// transaction, asserting the transaction was prepared exactly once.
    async fn seed_burn_intended(
        harness: &TestHarness,
        vault_mock: &MockVaultService,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
        recovery_owner: Address,
    ) {
        let TestHarness { store, receipt_service, .. } = harness;
        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(store, issuer_request_id).await;

        // Seed a reservation so the test verifies it is settled on confirm.
        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("seeding reservation should succeed");

        store
            .send(
                issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(99_U256),
                            burn_shares: uint!(100_000000000000000000_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: recovery_owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("IntendBurn should succeed");

        let aggregate = load_aggregate(store, issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::BurnIntended { .. }),
            "Expected BurnIntended with sendable_tx, got {aggregate:?}"
        );
        assert_eq!(
            vault_mock.burn_preparation_call_count(),
            1,
            "the signed transaction must be prepared before the restart"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_intended_rebroadcasts_persisted_transaction() {
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        let recovery_owner = prepared_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::StillMineable)
                .with_prepared_tx(prepared_tx.clone()),
        );
        let temp_dir =
            tempfile::tempdir().expect("temp directory should exist");
        let database_url = format!(
            "sqlite:{}?mode=rwc",
            temp_dir.path().join("burn-restart.db").display()
        );
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("file-backed database should connect");
        let apalis_options =
            apalis_sqlite::SqliteConnectOptions::from_str(&database_url)
                .expect("valid sqlite url")
                .pragma("journal_mode", "WAL")
                .busy_timeout(Duration::from_secs(5));
        let apalis_pool =
            apalis_sqlite::SqlitePool::connect_with(apalis_options)
                .await
                .expect("Failed to create apalis test pool");
        let harness =
            TestHarness::with_pool(vault_mock.clone(), pool, apalis_pool).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let issuer_request_id = IssuerRedemptionRequestId::random();

        seed_burn_intended(
            &harness,
            &vault_mock,
            vault,
            &issuer_request_id,
            recovery_owner,
        )
        .await;

        let restarted_pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("restart should reconnect to the same database");
        let restarted_store =
            StoreBuilder::<Redemption>::new(restarted_pool.clone())
                .with(Arc::new(RedemptionViewReactor::new(
                    restarted_pool.clone(),
                )))
                .build(RedemptionServices::with_single_vault(
                    Network::Base,
                    vault_mock.clone(),
                ))
                .await
                .expect("restart should rebuild the redemption store");
        let restarted_receipt_store = Arc::new(test_store::<ReceiptInventory>(
            restarted_pool.clone(),
            (),
        ));
        let restarted_receipt_service: Arc<dyn ReceiptService> =
            Arc::new(CqrsReceiptService::new(restarted_receipt_store));
        let replayed =
            load_aggregate(&restarted_store, &issuer_request_id).await;
        assert!(matches!(
            replayed,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == prepared_tx
        ));
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            restarted_pool.clone(),
            restarted_store.clone(),
            restarted_receipt_service.clone(),
            recovery_owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let result = manager.recover_single_burning(&issuer_request_id).await;

        // Recovery now enqueues a SubmitBurnJob rather than broadcasting
        // inline; drive the enqueued submit to broadcast the persisted tx.
        assert!(matches!(result, Ok(RecoveryOutcome::EnqueuedBurnJob)));
        let execution =
            intended_execution(&restarted_store, &issuer_request_id, vault)
                .await;
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("driving the enqueued submit should broadcast");

        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "driving recovery must broadcast the persisted transaction exactly once"
        );
        assert_eq!(
            vault_mock.burn_preparation_call_count(),
            1,
            "restart recovery must not prepare or re-sign the transaction"
        );
        assert_eq!(
            vault_mock.replacement_preparation_call_count(),
            0,
            "still-mineable recovery must not prepare a replacement"
        );
        assert_eq!(vault_mock.submitted_burn_txs(), vec![prepared_tx.clone()]);

        let updated =
            load_aggregate(&restarted_store, &issuer_request_id).await;
        assert!(matches!(updated, Redemption::BurnSubmitted { .. }));

        // Idempotency: re-running the submit job against the now-BurnSubmitted
        // redemption must NOT re-broadcast (submit_intended_burn short-circuits
        // on BurnSubmitted), so the broadcast count stays flat.
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("a rerun submit on a BurnSubmitted redemption is a no-op");
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "a BurnSubmitted redemption must never be re-broadcast"
        );

        assert!(
            restarted_receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "reservation remains held until a later pass observes a receipt"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Automatic burn recovery action accepted", "Rebroadcast"]
        ));

        let replacement_tx = SendableTxWithHash::valid_for_test(
            1,
            vault,
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        vault_mock.set_burn_tx_status(BurnTxStatus::ProvablyDead);
        vault_mock.set_prepared_tx(replacement_tx.clone());
        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::EnqueuedBurnJob)
        ));
        // The provably-dead replacement was signed and enqueued; drive its
        // submit so the fresh replacement actually broadcasts.
        let replacement_execution =
            intended_execution(&restarted_store, &issuer_request_id, vault)
                .await;
        manager
            .submit_intended_burn(&issuer_request_id, &replacement_execution)
            .await
            .expect("driving the enqueued replacement submit should broadcast");
        assert_eq!(
            vault_mock.submitted_burn_txs(),
            vec![prepared_tx.clone(), replacement_tx.clone()],
            "the replacement path must broadcast the fresh replacement"
        );
        assert!(
            restarted_receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .expect("restart reservation query should succeed")
                .contains(&issuer_request_id),
            "replacement must keep the persisted receipt reservation"
        );
        let contender = IssuerRedemptionRequestId::random();
        let availability = restarted_receipt_service
            .for_burn(
                ANVIL_CHAIN_ID,
                vault,
                &contender,
                Shares::new(uint!(1_U256)),
                Shares::ZERO,
            )
            .await;
        assert!(matches!(
            availability,
            Err(BurnTrackingError::InsufficientBalance { available, .. })
                if available == Shares::ZERO
        ));
        exhaust_restarted_recovery_budget(
            &manager,
            &vault_mock,
            &restarted_receipt_service,
            &restarted_pool,
            vault,
            &issuer_request_id,
            (prepared_tx, &replacement_tx),
        )
        .await;
    }

    #[traced_test]
    #[tokio::test]
    async fn concurrent_recovery_persists_exhaustion_once() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");
        for _ in 0..(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS - 1) {
            store
                .send(
                    &issuer_request_id,
                    RedemptionCommand::RecordBurnRecoveryAttempt {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: persisted_tx.hash,
                        nonce: persisted_tx.nonce,
                        action: BurnRecoveryAction::Rebroadcast,
                    },
                )
                .await
                .expect("recovery attempt should persist");
        }

        let first_manager = manager.clone();
        let second_manager = manager.clone();
        let (first, second) = tokio::join!(
            first_manager.reserve_recovery_attempt(
                &issuer_request_id,
                &persisted_tx,
                BurnRecoveryAction::Rebroadcast,
            ),
            second_manager.reserve_recovery_attempt(
                &issuer_request_id,
                &persisted_tx,
                BurnRecoveryAction::Rebroadcast,
            ),
        );

        assert!(matches!(
            (first, second),
            (Ok(true), Ok(false)) | (Ok(false), Ok(true))
        ));
        assert_eq!(vault_mock.burn_classification_call_count(), 0);
        let recovery_attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("recovery attempt count should load");
        assert_eq!(recovery_attempts, 5);
        let exhaustion_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryExhausted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("exhaustion event count should load");
        assert_eq!(exhaustion_events, 1);
        // Scope the log count to this redemption's own aggregate id:
        // `log_count_at!` reads a process-global buffer, so a concurrently
        // running test that also exhausts recovery budget would otherwise
        // inflate the count.
        let aggregate_id = issuer_request_id.to_string();
        assert_eq!(
            log_count_at!(
                tracing::Level::ERROR,
                &["Automatic burn recovery exhausted", aggregate_id.as_str()]
            ),
            1
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_provably_dead_burn_replaces_and_broadcasts_fresh_tx()
    {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let old_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let recovery_owner = old_tx.signer_for_test();
        let replacement_tx = SendableTxWithHash::valid_for_test(
            5,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(old_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            recovery_owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(99_U256),
                            burn_shares: uint!(100_000000000000000000_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: recovery_owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("old burn intent should persist");
        vault_mock.set_prepared_tx(replacement_tx.clone());

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::EnqueuedBurnJob)
        ));
        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the replacement");
        assert_eq!(
            vault_mock.submitted_burn_txs(),
            vec![replacement_tx.clone()]
        );
        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(matches!(
            aggregate,
            Redemption::BurnSubmitted { sendable_tx, .. }
                if sendable_tx == replacement_tx
        ));
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Automatic burn recovery action accepted", "Replace"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn provably_dead_replacement_waits_for_another_wallet_intent() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let old_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let owner = old_tx.signer_for_test();
        let replacement_tx = SendableTxWithHash::valid_for_test(
            5,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(old_tx),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

        // The durable uniqueness guard prevents this overlap now. Remove the
        // derived row for the current burn to model a historical pre-guard
        // database and retain coverage for the query-level defence in depth.
        let removed = sqlx::query(
            "
            DELETE FROM active_signer_intents
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
            ",
        )
        .bind(issuer_request_id.to_string())
        .execute(pool)
        .await
        .expect("test should remove the derived guard row");
        assert_eq!(
            removed.rows_affected(),
            1,
            "the reserve trigger must have created exactly the guard row this \
             test removes; a zero-row delete would silently stop modeling the \
             pre-guard database"
        );
        for (sequence, event_type, payload) in [
            (1, "MintEvent::Initiated", r#"{"Initiated":{"network":"base"}}"#),
            (2, "MintEvent::MintTxIntended", "{}"),
        ] {
            insert_raw_event(
                pool,
                "Mint",
                "blocking-mint",
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("blocking mint intent should seed");
        }

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 0);
        assert!(vault_mock.submitted_burn_txs().is_empty());
        let recovery_attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("recovery attempt count should load");
        assert_eq!(recovery_attempts, 0);
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Deferring dead burn replacement",
                &issuer_request_id.to_string(),
            ]
        ));

        insert_raw_event(
            pool,
            "Mint",
            "blocking-mint",
            3,
            "MintEvent::MintTxSubmitted",
            "{}",
        )
        .await
        .expect("blocking mint intent should resolve");

        for (sequence, event_type, payload) in [
            (
                1,
                "RedemptionEvent::Detected",
                r#"{"Detected":{"network":"base"}}"#,
            ),
            (2, "RedemptionEvent::BurnIntended", "{}"),
        ] {
            insert_raw_event(
                pool,
                "Redemption",
                "blocking-redemption",
                sequence,
                event_type,
                payload,
            )
            .await
            .expect("blocking burn intent should seed");
        }
        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 0);
        assert!(vault_mock.submitted_burn_txs().is_empty());

        insert_raw_event(
            pool,
            "Redemption",
            "blocking-redemption",
            3,
            "RedemptionEvent::BurnTxSubmitted",
            "{}",
        )
        .await
        .expect("blocking burn intent should resolve");
        vault_mock.set_prepared_tx(replacement_tx.clone());

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::EnqueuedBurnJob)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 1);
        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the replacement");
        assert_eq!(vault_mock.submitted_burn_txs(), vec![replacement_tx]);
    }

    /// Seeds an unresolved `BurnExcess` stream. The gate reads the event stream
    /// rather than `active_signer_intents` (`BurnExcess` reserves no row there),
    /// so only `event_type` matters and an empty payload is enough.
    async fn seed_unresolved_excess_burn(
        pool: &SqlitePool,
        event_type: &str,
    ) -> Result<(), sqlx::Error> {
        insert_raw_event(
            pool,
            "BurnExcess",
            "0x00000000000000000000000000000000000000000000000000000000000000e1",
            1,
            event_type,
            "{}",
        )
        .await
    }

    /// A dead burn may only be replaced when nothing else holds this wallet.
    /// `has_unresolved_signer_intent` cannot see an excess-burn recovery, so
    /// without the separate gate the replacement would sign over its nonce.
    #[traced_test]
    #[tokio::test]
    async fn provably_dead_replacement_waits_for_an_unresolved_excess_burn() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let old_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let owner = old_tx.signer_for_test();
        let replacement_tx = SendableTxWithHash::valid_for_test(
            5,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(old_tx),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    external_tx_id: None,
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                },
            )
            .await
            .expect("burn intent should persist");

        // `FundingExcluded` holds no signed transaction yet, and must still
        // block: the exclusion write is already permanent and the stream will
        // sign against the same issuer wallet.
        seed_unresolved_excess_burn(
            pool,
            BurnExcessEvent::FUNDING_EXCLUSION_RECORDED,
        )
        .await
        .expect("excess burn intent should seed");

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 0);
        assert!(vault_mock.submitted_burn_txs().is_empty());
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Deferring dead burn replacement",
                "unresolved_intent=false",
                "unresolved_excess=true",
            ]
        ));

        // Closing the excess stream must free the gate, or an abandoned
        // recovery would block burns forever.
        insert_raw_event(
            pool,
            "BurnExcess",
            "0x00000000000000000000000000000000000000000000000000000000000000e1",
            2,
            BurnExcessEvent::EXCESS_BURN_CLOSED,
            "{}",
        )
        .await
        .expect("excess burn intent should resolve");
        vault_mock.set_prepared_tx(replacement_tx.clone());

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::EnqueuedBurnJob)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 1);
        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the replacement");
        assert_eq!(vault_mock.submitted_burn_txs(), vec![replacement_tx]);
    }

    /// The live burn path waits behind an excess recovery rather than racing
    /// it for the nonce, and resumes once that stream resolves.
    ///
    /// Exhausting the full 30-attempt budget is deliberately not asserted here:
    /// the waits are real one-second sleeps, and paused time cannot stand in
    /// for them because advancing the clock 30 seconds also trips sqlx's
    /// 30-second pool acquire timeout.
    #[traced_test]
    #[tokio::test]
    async fn live_burn_waits_for_an_unresolved_excess_burn_then_proceeds() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;
        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        seed_unresolved_excess_burn(
            pool,
            BurnExcessEvent::EXCESS_BURN_INTENDED,
        )
        .await
        .expect("excess burn intent should seed");

        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        // Resolve the excess stream while the burn is parked in the wait loop,
        // so the test also proves the gate releases instead of only blocking.
        let releasing_pool = pool.clone();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1_500)).await;
            insert_raw_event(
                &releasing_pool,
                "BurnExcess",
                "0x00000000000000000000000000000000000000000000000000000000000000e1",
                2,
                BurnExcessEvent::EXCESS_BURN_CLOSED,
                "{}",
            )
            .await
            .expect("excess burn intent should resolve");
        });

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("the burn must proceed once the excess stream resolves");
        release.await.unwrap();
        drive_intended_burn_to_completion(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "the burn must reach the chain exactly once, after the wait"
        );
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::Completed { .. }
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &[
                "Waiting for an earlier wallet intent",
                "unresolved_intent=false",
                "unresolved_excess=true",
            ]
        ));
    }

    #[tokio::test]
    async fn test_force_complete_rejects_another_redemptions_burn_hash() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burn(45_989_009, uint!(17_U256))
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let manager = BurnManager::new_for_tests(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        harness.discover_receipt(vault, uint!(42_U256), uint!(17_U256)).await;
        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(42_U256),
                    shares_burned: uint!(17_U256),
                }],
            )
            .await
            .expect("reservation should seed");
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(42_U256),
                            burn_shares: uint!(17_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");
        let other_redemption_hash = B256::random();

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                other_redemption_hash,
                "wrong redemption".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnManagerError::Redemption(
                RedemptionError::UnresolvedBurnRequiresAcknowledgement { burn_tx_hash }
            )) if burn_tx_hash == persisted_tx.hash
        ));
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );
    }

    #[tokio::test]
    async fn force_complete_rejects_unvalidated_persisted_transaction_identity()
    {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let mut persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let owner = persisted_tx.signer_for_test();
        let decoded_hash = persisted_tx.hash;
        persisted_tx.hash = B256::random();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burn(45_989_009, uint!(17_U256))
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let manager = BurnManager::new_for_tests(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![],
                        dust_shares: U256::ZERO,
                        owner,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("malformed historical burn intent should replay");

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                persisted_tx.hash,
                "untrusted persisted identity".to_string(),
                None,
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnManagerError::Vault(
                VaultError::PreparedBurnHashMismatch { expected, decoded }
            )) if expected == persisted_tx.hash && decoded == decoded_hash
        ));
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recovery_classification_uncertainty_fails_closed() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_classification_failure()
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("reservation should seed");
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(99_U256),
                            burn_shares: uint!(100_000000000000000000_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

        manager.recover_burning_redemptions().await;

        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == persisted_tx
        ));
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );
        assert!(vault_mock.submitted_burn_txs().is_empty());
        let attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("attempt count should load");
        assert_eq!(attempts, 0);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Failed to recover Burning redemption"]
        ));
    }

    #[tokio::test]
    async fn test_missing_vault_does_not_consume_recovery_budget() {
        let persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::StillMineable)
                .with_prepared_tx(persisted_tx),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let manager = BurnManager::new_for_tests(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault: address!(
                            "0xcccccccccccccccccccccccccccccccccccccccc"
                        ),
                        burns: vec![],
                        dust_shares: U256::ZERO,
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Err(BurnManagerError::AssetNotFound { .. })
        ));
        let attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("attempt count should load");
        assert_eq!(attempts, 0);
    }

    #[traced_test]
    #[tokio::test]
    async fn reverted_persisted_burn_is_retried_by_the_failed_recovery_pass() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            4,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_confirm_revert()
                .with_burn_tx_status(BurnTxStatus::Reverted)
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        receipt_service
            .reserve_burn(
                ANVIL_CHAIN_ID,
                vault,
                issuer_request_id.clone(),
                vec![BurnRecord {
                    receipt_id: uint!(99_U256),
                    shares_burned: uint!(100_000000000000000000_U256),
                }],
            )
            .await
            .expect("reservation should seed");
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![MultiBurnEntry {
                            receipt_id: uint!(99_U256),
                            burn_shares: uint!(100_000000000000000000_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

        for _ in 0..(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS - 1) {
            store
                .send(
                    &issuer_request_id,
                    RedemptionCommand::RecordBurnRecoveryAttempt {
                        issuer_request_id: issuer_request_id.clone(),
                        tx_hash: persisted_tx.hash,
                        nonce: persisted_tx.nonce,
                        action: BurnRecoveryAction::Rebroadcast,
                    },
                )
                .await
                .expect("prior recovery attempt should persist");
        }

        let replacement_tx = SendableTxWithHash::valid_for_test(
            5,
            vault,
            Bytes::from_static(&[0xca, 0xfe]),
        );
        vault_mock.set_prepared_tx(replacement_tx.clone());
        // The recovery reconciler now enqueues durable jobs instead of
        // confirming/broadcasting inline; drive each enqueued step to
        // reproduce the full failed-recovery retry.
        //
        // Pass 1: the reverted persisted burn is routed to a ConfirmBurnJob.
        manager.recover_unresolved_burns().await;
        // Drive the confirm: the revert reserves the fifth (final) recovery
        // attempt as a Replace and releases the reservation.
        let reverted_confirm = BurnExecutionPlan {
            network: Network::Base,
            vault,
            params: BurnParams::VaultDirect {
                vault,
                burns: vec![],
                dust_shares: U256::ZERO,
                owner: TEST_WALLET,
            },
            planned_burns: vec![BurnRecord {
                receipt_id: uint!(99_U256),
                shares_burned: uint!(100_000000000000000000_U256),
            }],
            dust_shares: U256::ZERO,
            external_tx_id: None,
        };
        manager
            .confirm_submitted_burn(
                &issuer_request_id,
                &reverted_confirm.confirm_plan(),
                persisted_tx.hash.into(),
            )
            .await
            .expect_err("the reverted persisted burn confirmation must fail");
        // Pass 2: the failed-state pass resumes the burn and enqueues the
        // scheduled fresh replacement submit.
        manager.recover_unresolved_burns().await;
        let replacement_execution =
            intended_execution(store, &issuer_request_id, vault).await;
        manager
            .submit_intended_burn(&issuer_request_id, &replacement_execution)
            .await
            .expect("driving the replacement submit should broadcast");
        // Drive the replacement confirm: it reverts too, and with the budget
        // now at the cap this records exhaustion and terminalizes.
        let (replacement_confirm, replacement_tx_id) =
            submitted_confirm_execution(store, &issuer_request_id, vault).await;
        manager
            .confirm_submitted_burn(
                &issuer_request_id,
                &replacement_confirm,
                replacement_tx_id,
            )
            .await
            .expect_err("the reverted replacement confirmation must fail");

        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::Failed { .. }
        ));
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "the confirmed-reverted replacement must release its reservation"
        );
        let attempts_after_revert: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("attempt count should load");
        assert_eq!(
            attempts_after_revert,
            i64::from(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS)
        );
        let exhaustion_events: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption'
              AND aggregate_id = ?
              AND event_type = 'RedemptionEvent::BurnRecoveryExhausted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("exhaustion event count should load");
        assert_eq!(exhaustion_events, 1);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Burn confirmation failed"]
        ));

        assert_eq!(vault_mock.submitted_burn_txs(), vec![replacement_tx]);
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "the failed-state pass must submit the scheduled fresh replacement"
        );
    }

    /// An ambiguous local broadcast failure must retain both the persisted
    /// transaction and its receipt reservation. Signing a replacement could
    /// burn twice if the first broadcast actually reached the node.
    #[traced_test]
    #[tokio::test]
    async fn test_ambiguous_broadcast_failure_keeps_burn_intended() {
        let prepared_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let vault_mock =
            Arc::new(MockVaultService::new_submit_failure().with_prepared_tx(
                SendableTxWithHash {
                    tx: vec![1, 2, 3],
                    hash: prepared_hash,
                    nonce: 7,
                    signed_at: Utc::now(),
                    dust_shares: U256::ZERO,
                },
            ));
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started should enqueue the submit");
        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        assert!(
            manager
                .submit_intended_burn(&issuer_request_id, &execution)
                .await
                .is_err(),
            "ambiguous broadcast failure must be reported"
        );

        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(
                updated,
                Redemption::BurnIntended {
                    sendable_tx: SendableTxWithHash { hash, .. },
                    ..
                } if hash == prepared_hash
            ),
            "the exact persisted transaction must remain recoverable: {updated:?}"
        );
        assert_eq!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap(),
            vec![issuer_request_id],
            "ambiguous broadcast must retain the receipt reservation"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Burn broadcast outcome is ambiguous",
                "keeping persisted transaction for recovery",
            ]
        ));
    }

    /// A receipt timeout does not prove that a broadcast transaction failed.
    /// The submitted identity and reservation must remain recoverable so a
    /// later pass confirms the same transaction instead of signing a replacement.
    #[tokio::test]
    async fn test_confirmation_timeout_keeps_submitted_burn_and_reservation() {
        let prepared_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let vault_mock =
            Arc::new(MockVaultService::new_confirm_pending().with_prepared_tx(
                SendableTxWithHash {
                    tx: vec![1, 2, 3],
                    hash: prepared_hash,
                    nonce: 7,
                    signed_at: Utc::now(),
                    dust_shares: U256::ZERO,
                },
            ));
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .expect("handle_burning_started should enqueue the submit");
        let execution =
            intended_execution(store, &issuer_request_id, vault).await;
        let tx_id = manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");
        assert!(
            manager
                .confirm_submitted_burn(
                    &issuer_request_id,
                    &execution.confirm_plan(),
                    tx_id
                )
                .await
                .is_err(),
            "an unresolved confirmation must remain visible to the caller"
        );

        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(
                updated,
                Redemption::BurnSubmitted {
                    tx_id: TxId::Hash(hash),
                    ..
                } if hash == prepared_hash
            ),
            "the submitted transaction must remain recoverable: {updated:?}"
        );
        assert_eq!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap(),
            vec![issuer_request_id],
            "ambiguous confirmation must retain the receipt reservation"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_prepare_tx_failure_releases_reservation_and_records_burn_failure()
     {
        let vault_mock = Arc::new(MockVaultService::new_prepare_tx_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(
                result.unwrap_err(),
                BurnManagerError::Redemption(
                    RedemptionError::PreparingBurnTxFailed { .. }
                )
            ),
            "Expected PreparingBurnTxFailed error"
        );

        // No burn was submitted on-chain.
        assert_eq!(vault_mock.get_multi_burn_call_count(), 0);

        // Reservation must be released since nothing landed on-chain.
        assert!(
            receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "reservation must be released when prepare_tx fails"
        );

        // Aggregate must be in Failed state (RecordBurnFailure was called).
        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::Failed { .. }),
            "Expected Failed state after prepare_tx failure, got {updated:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Preparing signed burn tx failed"]
        ));
    }

    /// Tests that recovery from `BurnSubmitted` state (crash between submit
    /// and confirm) successfully confirms the existing transaction without
    /// submitting a new one.
    #[tokio::test]
    async fn test_recover_burn_submitted_confirms_existing_transaction() {
        let prepared_hash = b256!(
            "0x2323232323232323232323232323232323232323232323232323232323232323"
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::Mined)
                .with_prepared_tx(SendableTxWithHash {
                    tx: vec![1, 2, 3],
                    hash: prepared_hash,
                    nonce: 3,
                    signed_at: Utc::now(),
                    dust_shares: U256::ZERO,
                }),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Drive redemption to Burning state
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        // Issue IntendBurn then BurnTokens directly to stop at BurnSubmitted
        // (simulating a crash between submit and confirm). This calls submit_burn
        // on the mock and emits BurnTxSubmitted without confirming.
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    params: BurnParams::VaultDirect {
                        vault,
                        burns: vec![crate::vault::MultiBurnEntry {
                            receipt_id: uint!(99_U256),
                            burn_shares: uint!(100_000000000000000000_U256),
                            receipt_info: None,
                            receipt_info_bytes: None,
                        }],
                        dust_shares: U256::ZERO,
                        owner: TEST_WALLET,
                    },
                    external_tx_id: None,
                },
            )
            .await
            .expect("IntendBurn should succeed");

        let Redemption::BurnIntended { sendable_tx, .. } =
            load_aggregate(store, &issuer_request_id).await
        else {
            panic!("expected BurnIntended");
        };
        let submitted = vault_mock
            .submit_burn(
                MultiBurnParams {
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: uint!(99_U256),
                        burn_shares: uint!(100_000000000000000000_U256),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares: U256::ZERO,
                    owner: TEST_WALLET,
                    user: TEST_WALLET,
                    origin: BurnRequestOrigin::Redemption(
                        issuer_request_id.clone(),
                    ),
                    detected_tx_hash: B256::ZERO,
                    external_tx_id: None,
                },
                sendable_tx.clone(),
            )
            .await
            .expect("mock burn submit should succeed");
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnTxSubmitted {
                    issuer_request_id: issuer_request_id.clone(),
                    external_tx_id: BurnExternalTxId::from_string(
                        submitted.external_tx_id,
                    ),
                    tx_id: submitted.tx_id,
                    planned_burns: vec![BurnRecord {
                        receipt_id: uint!(99_U256),
                        shares_burned: uint!(100_000000000000000000_U256),
                    }],
                },
            )
            .await
            .expect("BurnTxSubmitted should persist");

        // Verify aggregate is in BurnSubmitted state
        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::BurnSubmitted { .. }),
            "Expected BurnSubmitted state, got {aggregate:?}"
        );

        // Record submit_burn call count before recovery
        let submits_before = vault_mock.get_multi_burn_call_count();

        // Recovery should find this redemption (view is Burning, aggregate
        // is BurnSubmitted) and confirm the existing transaction
        // without submitting a new burn.
        manager.recover_burning_redemptions().await;
        drive_submitted_burn_confirm(
            &manager,
            store,
            &issuer_request_id,
            vault,
        )
        .await;

        // Verify: no new submit_burn calls — recovery confirmed the existing tx
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            submits_before,
            "Recovery should confirm existing tx, not submit a new burn"
        );

        // Aggregate should now be Completed
        let recovered = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(recovered, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {recovered:?}"
        );
    }

    /// Reserves `shares` on `receipt_id` for `redemption`, simulating a
    /// reservation a missed settle/release left dangling.
    async fn seed_stuck_reservation(
        harness: &TestHarness,
        vault: Address,
        redemption: &IssuerRedemptionRequestId,
        receipt_id: U256,
        shares: U256,
    ) {
        harness
            .receipt_inventory_store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                ReceiptInventoryCommand::ReserveBurn {
                    redemption_issuer_request_id: redemption.clone(),
                    burns: vec![crate::redemption::BurnRecord {
                        receipt_id,
                        shares_burned: shares,
                    }],
                },
            )
            .await
            .expect("seeding a reservation should succeed");
    }

    #[tokio::test]
    async fn test_recover_stuck_reservations_settles_completed() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            harness.pool.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        // Receipt 1 funds the real burn; receipt 2 will hold the stuck
        // reservation we simulate after completion.
        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        harness
            .discover_receipt(
                vault,
                uint!(2_U256),
                uint!(50_000000000000000000_U256),
            )
            .await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .unwrap();
        drive_intended_burn_to_completion(
            &manager,
            &harness.store,
            &issuer_request_id,
            vault,
        )
        .await;

        seed_stuck_reservation(
            &harness,
            vault,
            &issuer_request_id,
            uint!(2_U256),
            uint!(50_000000000000000000_U256),
        )
        .await;
        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );

        manager.recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "GC must settle the reservation of a completed redemption"
        );
    }

    /// A vault mid-cutover holds both a vault-direct redemption (which reserved
    /// receipts) and an orchestrator redemption (which never reserves). The
    /// reservation sweep must still settle the vault-direct one while never
    /// surfacing the orchestrator one — proving it must keep running unchanged
    /// while any asset is vault-direct.
    #[traced_test]
    #[tokio::test]
    async fn recover_stuck_reservations_settles_vault_direct_ignoring_orchestrator()
     {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;
        let manager = BurnManager::new_for_tests(
            vault_mock.clone(),
            harness.pool.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        harness
            .discover_receipt(
                vault,
                uint!(2_U256),
                uint!(50_000000000000000000_U256),
            )
            .await;

        // Vault-direct redemption: complete it, then seed a dangling
        // reservation the sweep must settle.
        let vault_direct_id = IssuerRedemptionRequestId::random();
        let vault_direct_aggregate = create_test_redemption_in_burning_state(
            &harness.store,
            &vault_direct_id,
        )
        .await;
        manager
            .handle_burning_started(&vault_direct_id, &vault_direct_aggregate)
            .await
            .unwrap();
        drive_intended_burn_to_completion(
            &manager,
            &harness.store,
            &vault_direct_id,
            vault,
        )
        .await;
        seed_stuck_reservation(
            &harness,
            vault,
            &vault_direct_id,
            uint!(2_U256),
            uint!(50_000000000000000000_U256),
        )
        .await;

        // Orchestrator redemption on the same vault: completes without ever
        // reserving receipts.
        let orchestrator_id = IssuerRedemptionRequestId::random();
        let orchestrator_aggregate =
            create_orchestrator_redemption_in_burning_state(
                &harness.store,
                &orchestrator_id,
            )
            .await;
        manager
            .handle_burning_started(&orchestrator_id, &orchestrator_aggregate)
            .await
            .unwrap();
        drive_intended_burn_to_completion(
            &manager,
            &harness.store,
            &orchestrator_id,
            vault,
        )
        .await;

        let reserved_before = harness
            .receipt_service
            .reserved_redemptions(ANVIL_CHAIN_ID, vault)
            .await
            .unwrap();
        assert!(
            reserved_before.contains(&vault_direct_id),
            "the vault-direct redemption's reservation must be pending"
        );
        assert!(
            !reserved_before.contains(&orchestrator_id),
            "the orchestrator redemption must never reserve receipts"
        );

        manager.recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "the sweep must settle the vault-direct reservation"
        );
        assert!(matches!(
            load_aggregate(&harness.store, &vault_direct_id).await,
            Redemption::Completed { .. }
        ));
        assert!(matches!(
            load_aggregate(&harness.store, &orchestrator_id).await,
            Redemption::Completed { .. }
        ));
    }

    /// A settlement failure during reservation recovery must surface (logged)
    /// and leave the completed redemption's reservation in place, so the next
    /// recovery pass retries it rather than silently dropping the mirror
    /// reduction.
    #[traced_test]
    #[tokio::test]
    async fn test_recover_stuck_reservations_settle_failure_stays_recoverable()
    {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        // Drive the redemption to `Completed` through the real receipt service
        // so the burn settles cleanly; recovery below runs against a
        // settle-failing wrapper.
        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            NetworkVaultServices::with_single_vault(
                Network::Base,
                ANVIL_CHAIN_ID,
                blockchain_service,
            ),
            harness.pool.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
            harness.apalis_pool.clone(),
        );

        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;
        harness
            .discover_receipt(
                vault,
                uint!(2_U256),
                uint!(50_000000000000000000_U256),
            )
            .await;

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .unwrap();
        drive_intended_burn_to_completion(
            &manager,
            &harness.store,
            &issuer_request_id,
            vault,
        )
        .await;
        assert!(matches!(
            load_aggregate(&harness.store, &issuer_request_id).await,
            Redemption::Completed { .. }
        ));

        seed_stuck_reservation(
            &harness,
            vault,
            &issuer_request_id,
            uint!(2_U256),
            uint!(50_000000000000000000_U256),
        )
        .await;

        let settle_failing: Arc<dyn ReceiptService> =
            Arc::new(SettleFailingReceiptService {
                inner: harness.receipt_service.clone(),
            });
        let recovery_blockchain: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let failing_manager = BurnManager::new(
            NetworkVaultServices::with_single_vault(
                Network::Base,
                ANVIL_CHAIN_ID,
                recovery_blockchain,
            ),
            harness.pool.clone(),
            harness.store.clone(),
            settle_failing,
            TEST_WALLET,
            harness.apalis_pool.clone(),
        );

        failing_manager
            .recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)])
            .await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "a failed settlement must leave the reservation recoverable for the next pass"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Failed to settle burn receipt reservation",
                &issuer_request_id.to_string()
            ]
        ));

        manager.recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)]).await;
        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .is_empty(),
            "the next recovery pass must retry and settle the held reservation"
        );
    }

    /// A reservation surviving on a `Failed` redemption is from an *ambiguous*
    /// failure (definitive failures release in the live/recovery paths), so the
    /// burn may still have landed. The GC must LEAVE it rather than release it —
    /// releasing would over-credit inventory and risk a duplicate burn.
    #[tokio::test]
    async fn test_recover_stuck_reservations_leaves_ambiguous_submitted() {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            harness.pool.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Drive the redemption to an ambiguously submitted burn.
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_burning_state(
            &harness.store,
            &issuer_request_id,
        )
        .await;
        let _ = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;
        let execution =
            intended_execution(&harness.store, &issuer_request_id, vault).await;
        let tx_id = manager
            .submit_intended_burn(&issuer_request_id, &execution)
            .await
            .expect("submit_intended_burn should broadcast the persisted burn");
        let _ = manager
            .confirm_submitted_burn(
                &issuer_request_id,
                &execution.confirm_plan(),
                tx_id,
            )
            .await;
        assert!(matches!(
            load_aggregate(&harness.store, &issuer_request_id).await,
            Redemption::BurnSubmitted { .. }
        ));

        seed_stuck_reservation(
            &harness,
            vault,
            &issuer_request_id,
            uint!(1_U256),
            uint!(40_000000000000000000_U256),
        )
        .await;

        manager.recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "GC must leave an ambiguous failed redemption's reservation in place"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_stuck_reservations_warns_and_leaves_unknown() {
        let harness = TestHarness::new().await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL").unwrap(), vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new_for_tests(
            blockchain_service,
            harness.pool.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
            ANVIL_CHAIN_ID,
            harness.apalis_pool.clone(),
        );

        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // No redemption aggregate exists for this id (Uninitialized): the GC
        // must leave the reservation in place and surface a WARN rather than
        // releasing it blindly.
        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_stuck_reservation(
            &harness,
            vault,
            &issuer_request_id,
            uint!(1_U256),
            uint!(60_000000000000000000_U256),
        )
        .await;

        manager.recover_stuck_reservations(&[(ANVIL_CHAIN_ID, vault)]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(ANVIL_CHAIN_ID, vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "GC must leave an unknown redemption's reservation in place"
        );
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["unknown redemption"]
        ));
    }
}
