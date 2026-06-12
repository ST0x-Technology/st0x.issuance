use alloy::primitives::{Address, B256, U256};
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::view::{
    RedemptionView, RedemptionViewError, find_burn_failed, find_burning,
};
use super::{
    BurnExternalTxId, IssuerRedemptionRequestId, Redemption, RedemptionCommand,
    RedemptionError, next_burn_retry_external_tx_id_from_history,
};
use crate::fireblocks::{FireblocksVaultError, vault_service};
use crate::mint::QuantityConversionError;
use crate::receipt_inventory::{
    BurnPlan, BurnTrackingError, ReceiptRegistrationError, ReceiptService,
    Shares,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{
    BurnVerification, FireblocksTxStatus, MultiBurnEntry, VaultError,
    VaultService,
};

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
    /// A previously submitted Fireblocks burn was confirmed and recorded.
    ExistingBurnRecorded,
    /// Burn skipped: the bot's on-chain balance is insufficient, so the burn
    /// likely already landed but was never recorded. Needs manual review.
    SkippedManualIntervention,
    /// The redemption already advanced past `Burning`/`BurnSubmitted`; there
    /// was nothing to burn.
    AlreadyAdvanced,
}

/// Orchestrates the on-chain burning process in response to `AlpacaJournalCompleted` events.
///
/// The manager reacts to `AlpacaJournalCompleted` events by querying for a suitable receipt,
/// then issues a `BurnTokens` command to the Redemption aggregate. The aggregate's command
/// handler calls the vault service to perform the actual burn operation.
///
/// On burn failure, the manager issues a `RecordBurnFailure` command to record the error.
pub(crate) struct BurnManager<RedemptionStore>
where
    RedemptionStore: EventStore<Redemption>,
{
    /// Used only for balance queries during recovery (not for burns - those go through aggregate)
    vault_service: Arc<dyn VaultService>,
    view_pool: Pool<Sqlite>,
    cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
    store: Arc<RedemptionStore>,
    receipt_service: Arc<dyn ReceiptService>,
    bot_wallet: Address,
}

impl<RedemptionStore> BurnManager<RedemptionStore>
where
    RedemptionStore: EventStore<Redemption>,
{
    /// Creates a new burn manager.
    ///
    /// # Arguments
    ///
    /// * `vault_service` - Vault service for balance queries during recovery
    /// * `view_pool` - Database pool for querying views
    /// * `cqrs` - CQRS framework for executing commands on the Redemption aggregate
    /// * `store` - Event store for loading aggregate state during recovery
    /// * `receipt_service` - Service for finding receipts to burn
    /// * `bot_wallet` - Bot's wallet address that owns both shares and receipts
    pub(crate) fn new(
        vault_service: Arc<dyn VaultService>,
        view_pool: Pool<Sqlite>,
        cqrs: Arc<CqrsFramework<Redemption, RedemptionStore>>,
        store: Arc<RedemptionStore>,
        receipt_service: Arc<dyn ReceiptService>,
        bot_wallet: Address,
    ) -> Self {
        Self {
            vault_service,
            view_pool,
            cqrs,
            store,
            receipt_service,
            bot_wallet,
        }
    }

    /// Recovers redemptions stuck in the `Burning` state at startup.
    ///
    /// Queries the view for all redemptions in `Burning` state and resumes
    /// the burn process for each. This handles cases where the bot crashed
    /// after Alpaca journal completion but before burn was executed.
    pub(crate) async fn recover_burning_redemptions(&self) {
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
    pub(crate) async fn recover_burn_failed_redemptions(&self) {
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

    /// Admin-terminalizes a redemption stuck in `Burning`/`BurnSubmitted` whose
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
    /// Ambiguous cases with no verifiable on-chain burn fail here (`NotABurn`)
    /// and must be resolved via `CloseRedemption` (or off-chain reconciliation)
    /// instead — they are never silently force-completed.
    pub(crate) async fn force_complete_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        burn_tx_hash: B256,
        reason: String,
    ) -> Result<BurnVerification, BurnManagerError> {
        let mut context =
            self.store.load_aggregate(&issuer_request_id.to_string()).await?;

        let underlying = match context.aggregate() {
            Redemption::Burning { metadata, .. }
            | Redemption::BurnSubmitted { metadata, .. } => {
                metadata.underlying.clone()
            }
            other => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: aggregate_state_name(other).to_string(),
                });
            }
        };

        let vault = find_vault_by_underlying(&self.view_pool, &underlying)
            .await?
            .ok_or(BurnManagerError::AssetNotFound { underlying })?;

        // Verify the burn actually landed on-chain before recording a terminal
        // success — never trust the operator-supplied hash blindly.
        let verification = self
            .vault_service
            .verify_burn_tx(vault, self.bot_wallet, burn_tx_hash)
            .await?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            burn_tx_hash = ?burn_tx_hash,
            block_number = verification.block_number,
            shares_burned = %verification.shares_burned,
            "Force-completing stuck Burning redemption: burn verified on-chain"
        );

        self.cqrs
            .execute(
                &issuer_request_id.to_string(),
                RedemptionCommand::ForceCompleteBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    burn_tx_hash,
                    block_number: verification.block_number,
                    reason,
                },
            )
            .await?;

        self.settle_reserved_burn(vault, issuer_request_id).await;

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
    pub(crate) async fn recover_stuck_reservations(&self, vaults: &[Address]) {
        let mut stuck: Vec<(Address, IssuerRedemptionRequestId)> = Vec::new();

        for vault in vaults {
            match self.receipt_service.reserved_redemptions(*vault).await {
                Ok(redemptions) => {
                    stuck
                        .extend(redemptions.into_iter().map(|id| (*vault, id)));
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

        for (vault, issuer_request_id) in stuck {
            if let Err(err) =
                self.resolve_stuck_reservation(vault, &issuer_request_id).await
            {
                warn!(target: "redemption", vault = %vault,
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to resolve stuck reservation"
                );
            }
        }
    }

    async fn resolve_stuck_reservation(
        &self,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<(), BurnManagerError> {
        use Redemption::{Completed, Uninitialized};

        let mut context =
            self.store.load_aggregate(&issuer_request_id.to_string()).await?;

        match context.aggregate() {
            // The burn confirmed on-chain but settlement was missed (e.g. a
            // crash in the confirm->settle window). Settle to reduce the mirror.
            Completed { .. } => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Settling reservation for completed redemption"
                );
                self.settle_reserved_burn(vault, issuer_request_id).await;
            }
            // A reservation for a redemption with no events is anomalous (e.g.
            // pruned history). Leave it for manual review rather than releasing
            // blindly against an unknown on-chain outcome.
            Uninitialized => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Reservation held for an unknown redemption; left for manual review"
                );
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
        let RedemptionView::BurnFailed {
            underlying,
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
            fireblocks_tx_id,
            ..
        } = view
        else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "View not in BurnFailed state, skipping"
            );
            return Ok(());
        };

        let vault = find_vault_by_underlying(&self.view_pool, underlying)
            .await?
            .ok_or_else(|| BurnManagerError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        // If a Fireblocks tx was already submitted before failure, inspect it
        // before deciding whether to confirm, wait, or submit a replacement.
        let retry_external_tx_id = if let Some(fb_tx_id) = fireblocks_tx_id {
            match self.vault_service.check_fireblocks_tx(fb_tx_id).await? {
                // Completed: the tx landed on-chain. None: non-Fireblocks
                // backend with no status to inspect. Both confirm/record the
                // existing tx rather than resubmit.
                Some(FireblocksTxStatus::Completed { .. }) | None => {
                    return self
                        .recover_burn_failed_with_existing_tx(
                            issuer_request_id,
                            vault,
                            fb_tx_id,
                            dust_quantity,
                        )
                        .await;
                }
                Some(FireblocksTxStatus::Pending) => {
                    info!(target: "redemption", issuer_request_id = %issuer_request_id,
                        fireblocks_tx_id = %fb_tx_id,
                        "BurnFailed recovery found pending Fireblocks transaction; leaving for a later recovery pass"
                    );
                    return Ok(());
                }
                Some(FireblocksTxStatus::Failed { .. }) => {
                    let retry_external_tx_id = Some(
                        self.next_burn_retry_external_tx_id(
                            issuer_request_id,
                            tx_hash,
                        )
                        .await?
                        .unwrap_or_else(|| {
                            Redemption::retry_burn_external_tx_id_typed(
                                tx_hash, 1,
                            )
                        }),
                    );

                    info!(target: "redemption", issuer_request_id = %issuer_request_id,
                        fireblocks_tx_id = %fb_tx_id,
                        retry_external_tx_id = ?retry_external_tx_id,
                        "BurnFailed recovery found terminal failed Fireblocks transaction; submitting replacement burn"
                    );

                    retry_external_tx_id
                }
            }
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
        let on_chain_balance = self
            .vault_service
            .get_share_balance(vault, self.bot_wallet)
            .await?;

        if on_chain_balance < total_shares {
            let reason = format!(
                "On-chain balance insufficient for BurnFailed recovery: \
                 balance={on_chain_balance}, required={total_shares}"
            );

            info!(target: "redemption", issuer_request_id = %issuer_request_id,
                on_chain_balance = %on_chain_balance,
                total_shares = %total_shares,
                "Auto-failing BurnFailed redemption with insufficient on-chain balance"
            );

            let command = RedemptionCommand::MarkFailed {
                issuer_request_id: issuer_request_id.clone(),
                reason,
            };

            self.cqrs.execute(&issuer_request_id.to_string(), command).await?;

            // The burn likely already landed (on-chain balance is too low to
            // burn again), so any reservation from a prior attempt is LEFT in
            // place: releasing would over-credit inventory against a stale-high
            // mirror and risk a duplicate burn. It is resolved by on-chain
            // settlement or manual intervention.
            return Ok(());
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
            wallet: *wallet,
            quantity: quantity.clone(),
            detected_tx_hash: *tx_hash,
            block_number: *block_number,
            detected_at: *detected_at,
        };

        self.cqrs
            .execute(
                &issuer_request_id.to_string(),
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
        let mut context =
            self.store.load_aggregate(&issuer_request_id.to_string()).await?;

        self.handle_burning_started(issuer_request_id, context.aggregate())
            .await?;

        debug!(target: "redemption", issuer_request_id = %issuer_request_id,
            "Successfully retried burn"
        );

        Ok(())
    }

    /// Recovers a BurnFailed redemption that has a previously submitted Fireblocks
    /// transaction. Tries to confirm the existing transaction rather than resubmitting.
    async fn recover_burn_failed_with_existing_tx(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        fireblocks_tx_id: &str,
        dust_quantity: &crate::Quantity,
    ) -> Result<(), BurnManagerError> {
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            %fireblocks_tx_id,
            "BurnFailed recovery — confirming previously submitted Fireblocks transaction"
        );

        match self
            .vault_service
            .confirm_burn(fireblocks_tx_id, dust_shares)
            .await
        {
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
                self.cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::RecordExistingBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            fireblocks_tx_id: fireblocks_tx_id.to_string(),
                            tx_hash: result.tx_hash,
                            planned_burns: actual_burns,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                self.settle_reserved_burn(vault, issuer_request_id).await;

                Ok(())
            }
            Err(err) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    %fireblocks_tx_id,
                    error = %err,
                    "Failed to confirm previously submitted burn — \
                     marking as failed to prevent infinite retry loop"
                );

                if should_release_reserved_burn(&err) {
                    self.release_reserved_burn(vault, issuer_request_id).await;
                }

                // Re-mark as failed WITHOUT preserving the fireblocks_tx_id.
                // This moves the view from BurnFailed to Failed, so the next
                // recovery pass does not retry the same dead transaction.
                let reason = format!(
                    "Fireblocks burn confirmation failed for tx {fireblocks_tx_id}: {err}"
                );

                self.cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::MarkFailed {
                            issuer_request_id: issuer_request_id.clone(),
                            reason,
                        },
                    )
                    .await?;

                Err(BurnManagerError::Vault(err))
            }
        }
    }

    async fn recover_single_burning(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let mut context =
            self.store.load_aggregate(&issuer_request_id.to_string()).await?;

        let aggregate = context.aggregate();

        match aggregate {
            // Already submitted to Fireblocks but never confirmed — resume polling
            Redemption::BurnSubmitted {
                metadata,
                fireblocks_tx_id,
                dust_quantity,
                planned_burns,
                ..
            } => {
                let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

                let vault = find_vault_by_underlying(
                    &self.view_pool,
                    &metadata.underlying,
                )
                .await?
                .ok_or_else(|| {
                    BurnManagerError::AssetNotFound {
                        underlying: metadata.underlying.clone(),
                    }
                })?;

                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    fireblocks_tx_id = %fireblocks_tx_id,
                    "Recovering BurnSubmitted redemption - confirming existing transaction"
                );

                let confirm_result = self
                    .cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::ConfirmBurn {
                            issuer_request_id: issuer_request_id.clone(),
                            fireblocks_tx_id: fireblocks_tx_id.clone(),
                            dust_shares,
                        },
                    )
                    .await;

                match confirm_result {
                    Ok(()) => {
                        info!(target: "redemption", issuer_request_id = %issuer_request_id,
                            "Burn confirmed successfully during recovery"
                        );

                        self.settle_reserved_burn(vault, issuer_request_id)
                            .await;

                        Ok(RecoveryOutcome::ExistingBurnRecorded)
                    }
                    Err(AggregateError::UserError(RedemptionError::Vault(
                        err,
                    ))) => {
                        warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                            error = %err,
                            "Burn confirmation failed during recovery"
                        );

                        if should_release_reserved_burn(&err) {
                            self.release_reserved_burn(
                                vault,
                                issuer_request_id,
                            )
                            .await;
                        }

                        self.cqrs
                            .execute(
                                &issuer_request_id.to_string(),
                                RedemptionCommand::RecordBurnFailure {
                                    issuer_request_id: issuer_request_id
                                        .clone(),
                                    error: err.to_string(),
                                    fireblocks_tx_id: Some(
                                        fireblocks_tx_id.clone(),
                                    ),
                                    planned_burns: planned_burns.clone(),
                                },
                            )
                            .await?;

                        Err(BurnManagerError::Vault(err))
                    }
                    Err(err) => Err(err.into()),
                }
            }

            // Still in Burning state — needs full submit + confirm flow
            Redemption::Burning {
                metadata,
                alpaca_quantity,
                dust_quantity,
                external_tx_id,
                ..
            } => {
                let vault = find_vault_by_underlying(
                    &self.view_pool,
                    &metadata.underlying,
                )
                .await?
                .ok_or_else(|| {
                    BurnManagerError::AssetNotFound {
                        underlying: metadata.underlying.clone(),
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
                let on_chain_balance = self
                    .vault_service
                    .get_share_balance(vault, self.bot_wallet)
                    .await?;

                if on_chain_balance < total_shares_needed {
                    warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                        on_chain_balance = %on_chain_balance,
                        burn_shares = %burn_shares,
                        dust_shares = %dust_shares,
                        total_shares_needed = %total_shares_needed,
                        "MANUAL INTERVENTION REQUIRED: On-chain balance insufficient for burn recovery. \
                         Burn likely already succeeded but was not recorded. \
                         Skipping to avoid recording false failure."
                    );

                    // The redemption stays Burning for manual review. Any
                    // reservation from the crashed attempt is LEFT in place:
                    // the burn likely already landed, so releasing would
                    // over-credit inventory and risk a duplicate burn against
                    // the stale-high mirror. Leaving it keeps availability
                    // conservatively correct until manual intervention resolves
                    // the redemption.
                    return Ok(RecoveryOutcome::SkippedManualIntervention);
                }

                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    external_tx_id = ?external_tx_id,
                    "Recovering Burning redemption - resuming burn"
                );

                self.handle_burning_started(issuer_request_id, aggregate)
                    .await?;

                Ok(RecoveryOutcome::Executed)
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
        let Redemption::Burning {
            metadata,
            alpaca_quantity,
            dust_quantity,
            external_tx_id,
            ..
        } = aggregate
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(aggregate).to_string(),
            });
        };

        let Some(vault) =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
        else {
            let error_msg = format!(
                "No vault configured for underlying asset {}",
                metadata.underlying
            );

            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                underlying = %metadata.underlying,
                "{error_msg}"
            );

            self.cqrs
                .execute(
                    &issuer_request_id.to_string(),
                    RedemptionCommand::RecordBurnFailure {
                        issuer_request_id: issuer_request_id.clone(),
                        error: error_msg,
                        fireblocks_tx_id: None,
                        planned_burns: vec![],
                    },
                )
                .await?;

            return Err(BurnManagerError::AssetNotFound {
                underlying: metadata.underlying.clone(),
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
            "Starting on-chain burning process with dust handling"
        );

        // A retry plans against availability that EXCLUDES this redemption's own
        // prior reservation (see `for_burn`), and `reserve_burn` atomically
        // replaces that reservation — so no separate release-before-plan is
        // needed, and the prior reservation is never returned to global
        // availability where a concurrent redemption could grab it.
        let plan = self
            .plan_burn(
                issuer_request_id,
                vault,
                &metadata.underlying,
                burn_shares,
                dust_shares,
            )
            .await?;

        self.execute_burn_and_record_result(
            issuer_request_id,
            vault,
            plan,
            external_tx_id.clone(),
        )
        .await
    }

    async fn next_burn_retry_external_tx_id(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        detected_tx_hash: &B256,
    ) -> Result<Option<BurnExternalTxId>, BurnManagerError> {
        let events =
            self.store.load_events(&issuer_request_id.to_string()).await?;

        Ok(next_burn_retry_external_tx_id_from_history(
            detected_tx_hash,
            events.iter().map(|event| &event.payload),
        )?)
    }

    async fn plan_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        underlying: &UnderlyingSymbol,
        burn_shares: U256,
        dust_shares: U256,
    ) -> Result<BurnPlan, BurnManagerError> {
        let plan = self
            .receipt_service
            .for_burn(
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

        self.cqrs
            .execute(
                &issuer_request_id.to_string(),
                RedemptionCommand::RecordBurnFailure {
                    issuer_request_id: issuer_request_id.clone(),
                    error: error_msg.clone(),
                    fireblocks_tx_id: None,
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
        vault: Address,
        plan: BurnPlan,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Result<(), BurnManagerError> {
        let burns: Vec<MultiBurnEntry> = plan
            .allocations
            .into_iter()
            .map(|alloc| MultiBurnEntry {
                receipt_id: alloc.receipt.receipt_id.inner(),
                burn_shares: alloc.burn_amount.inner(),
                receipt_info: alloc.receipt.receipt_info,
                receipt_info_bytes: alloc.receipt.receipt_info_bytes,
            })
            .collect();

        // Capture planned burns before they are consumed by the command,
        // so we can include them in the BurningFailed event if the burn fails.
        let planned_burns: Vec<super::BurnRecord> = burns
            .iter()
            .map(|entry| super::BurnRecord {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();

        let dust_shares = plan.dust.inner();

        // Reserve the planned receipts BEFORE submitting to the signing
        // backend. The vault inventory is a single serialized aggregate, so a
        // concurrent redemption that already committed the same balance makes
        // this reservation fail — and we must not submit an unbacked burn that
        // would later revert on-chain with ERC1155InsufficientBalance.
        if let Err(err) = self
            .reserve_with_conflict_retry(
                vault,
                issuer_request_id,
                planned_burns.clone(),
            )
            .await
        {
            error!(target: "redemption", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to reserve receipts before burn submission; aborting to avoid double-spend"
            );

            self.cqrs
                .execute(
                    &issuer_request_id.to_string(),
                    RedemptionCommand::RecordBurnFailure {
                        issuer_request_id: issuer_request_id.clone(),
                        error: err.to_string(),
                        fireblocks_tx_id: None,
                        planned_burns,
                    },
                )
                .await?;

            return Err(err.into());
        }

        // Step 1: Submit the burn transaction (emits BurnFireblocksSubmitted)
        let submit_result = self
            .cqrs
            .execute(
                &issuer_request_id.to_string(),
                RedemptionCommand::BurnTokens {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns,
                    dust_shares,
                    owner: self.bot_wallet,
                    external_tx_id,
                },
            )
            .await;

        match submit_result {
            Ok(()) => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "BurnTokens submitted, confirming..."
                );
            }
            Err(AggregateError::UserError(RedemptionError::Vault(err))) => {
                let fireblocks_tx_id = extract_fireblocks_tx_id(&err);

                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    fireblocks_tx_id = ?fireblocks_tx_id,
                    "Burn submission failed"
                );

                // Release only on a DEFINITIVE failure that proves no shares
                // were consumed — a terminal Fireblocks status or an EVM revert
                // (the local backend submits synchronously, so a revert can
                // surface here). Otherwise RETAIN: a submit error does not prove
                // no transaction was created (a duplicate `externalTxId` whose
                // lookup failed, a missing transaction id, or an SDK/RPC error
                // after the request was sent all leave a possibly-in-flight
                // burn), and releasing could let a concurrent redemption reuse
                // the balance and double-submit. Recovery (via the preserved
                // `fireblocks_tx_id`) or manual intervention resolves retained
                // reservations.
                if should_release_reserved_burn(&err) {
                    self.release_reserved_burn(vault, issuer_request_id).await;
                }

                self.cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::RecordBurnFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: err.to_string(),
                            fireblocks_tx_id,
                            planned_burns,
                        },
                    )
                    .await?;

                return Err(BurnManagerError::Vault(err));
            }
            // Submission outcome is unknown (non-vault error): keep the
            // reservation so a concurrent redemption cannot reuse the balance.
            // A retry's atomic clear-and-reserve (ReserveBurn) replaces this
            // redemption's reservation in one step.
            Err(err) => return Err(err.into()),
        }

        // Step 2: Load aggregate to get the fireblocks_tx_id from BurnSubmitted state
        let mut context =
            self.store.load_aggregate(&issuer_request_id.to_string()).await?;

        let Redemption::BurnSubmitted { fireblocks_tx_id, .. } =
            context.aggregate()
        else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(context.aggregate())
                    .to_string(),
            });
        };

        let fireblocks_tx_id = fireblocks_tx_id.clone();

        // Step 3: Confirm the burn (emits TokensBurned or error)
        let confirm_result = self
            .cqrs
            .execute(
                &issuer_request_id.to_string(),
                RedemptionCommand::ConfirmBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    fireblocks_tx_id: fireblocks_tx_id.clone(),
                    dust_shares,
                },
            )
            .await;

        match confirm_result {
            Ok(()) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Burn confirmed successfully"
                );

                // The burn landed on-chain: consume the reservation so the
                // mirror balance drops to match.
                self.settle_reserved_burn(vault, issuer_request_id).await;

                Ok(())
            }
            Err(AggregateError::UserError(RedemptionError::Vault(err))) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Burn confirmation failed"
                );

                if should_release_reserved_burn(&err) {
                    self.release_reserved_burn(vault, issuer_request_id).await;
                }

                self.cqrs
                    .execute(
                        &issuer_request_id.to_string(),
                        RedemptionCommand::RecordBurnFailure {
                            issuer_request_id: issuer_request_id.clone(),
                            error: err.to_string(),
                            fireblocks_tx_id: Some(fireblocks_tx_id),
                            planned_burns,
                        },
                    )
                    .await?;

                Err(BurnManagerError::Vault(err))
            }
            Err(err) => Err(err.into()),
        }
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
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
        planned_burns: Vec<super::BurnRecord>,
    ) -> Result<(), ReceiptRegistrationError> {
        const MAX_ATTEMPTS: usize = 3;

        let mut attempt = 1;
        loop {
            match self
                .receipt_service
                .reserve_burn(
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
                Err(err) => return Err(err),
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
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        if let Err(err) = self
            .receipt_service
            .release_burn(vault, issuer_request_id.clone())
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
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        if let Err(err) = self
            .receipt_service
            .settle_burn(vault, issuer_request_id.clone())
            .await
        {
            warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                error = %err,
                "Failed to settle burn receipt reservation"
            );
        }
    }
}

const fn aggregate_state_name(aggregate: &Redemption) -> &'static str {
    match aggregate {
        Redemption::Uninitialized => "Uninitialized",
        Redemption::Detected { .. } => "Detected",
        Redemption::AlpacaCalled { .. } => "AlpacaCalled",
        Redemption::Burning { .. } => "Burning",
        Redemption::BurnSubmitted { .. } => "BurnSubmitted",
        Redemption::Failed { .. } => "Failed",
        Redemption::Completed { .. } => "Completed",
        Redemption::Closed { .. } => "Closed",
    }
}

/// Extracts the Fireblocks transaction ID from a `VaultError`, if the error
/// originated from a Fireblocks error that carries a transaction ID.
fn extract_fireblocks_tx_id(error: &VaultError) -> Option<String> {
    match error {
        VaultError::Fireblocks(
            crate::fireblocks::FireblocksVaultError::TransactionFailed {
                tx_id,
                ..
            }
            | crate::fireblocks::FireblocksVaultError::MissingTxHash { tx_id },
        ) => Some(tx_id.clone()),
        _ => None,
    }
}

/// Whether a failed burn confirmation definitively consumed no receipts, so its
/// inventory reservation must be released. Ambiguous pending Fireblocks statuses
/// keep the reservation (the transaction may still land on-chain).
const fn should_release_reserved_burn(error: &VaultError) -> bool {
    match error {
        VaultError::Fireblocks(FireblocksVaultError::TransactionFailed {
            status,
            ..
        }) => !vault_service::is_still_pending(*status),
        VaultError::Reverted { .. } => true,
        _ => false,
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BurnManagerError {
    #[error("Vault error: {0}")]
    Vault(#[from] VaultError),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("CQRS error: {0}")]
    Cqrs(#[from] AggregateError<RedemptionError>),
    #[error("Redemption error: {0}")]
    Redemption(#[from] RedemptionError),
    #[error("Invalid aggregate state: {current_state}")]
    InvalidAggregateState { current_state: String },
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("Insufficient balance: required {required}, available {available}")]
    InsufficientBalance { required: Shares, available: Shares },
    #[error("Receipt inventory error: {0}")]
    BurnTracking(#[from] BurnTrackingError),
    #[error("Redemption view error: {0}")]
    RedemptionView(#[from] RedemptionViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("Asset not found for underlying: {underlying}")]
    AssetNotFound { underlying: UnderlyingSymbol },
    #[error("Arithmetic overflow when computing total shares needed")]
    SharesOverflow,
    #[error("Receipt reservation error: {0}")]
    ReceiptRegistration(#[from] ReceiptRegistrationError),
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, Bytes, U256, address, b256, uint};
    use chrono::Utc;
    use cqrs_es::{
        AggregateContext, EventStore,
        persist::{GenericQuery, PersistedEventStore},
    };
    use event_sorcery::{Store, StoreBuilder, test_store};
    use fireblocks_sdk::models::TransactionStatus;
    use rust_decimal::Decimal;
    use sqlite_es::{
        SqliteCqrs, SqliteEventRepository, SqliteViewRepository, sqlite_cqrs,
    };
    use sqlx::sqlite::SqlitePoolOptions;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use crate::redemption::BurnExternalTxId;

    use super::{
        BurnManager, BurnManagerError, Redemption, RedemptionCommand,
        should_release_reserved_burn,
    };
    use crate::fireblocks::FireblocksVaultError;
    use crate::mint::IssuerMintRequestId;
    use crate::mint::{Network, Quantity, TokenizationRequestId};
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptService, ReceiptSource, Shares,
    };
    use crate::redemption::view::RedemptionView;
    use crate::redemption::{BurnRecord, IssuerRedemptionRequestId};
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        TokenSymbol, TokenizedAsset, TokenizedAssetCommand, UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        FireblocksTxStatus, MultiBurnEntry, ReceiptInformation, VaultError,
        VaultService,
    };

    const TEST_WALLET: Address =
        address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

    fn transaction_failed(status: TransactionStatus) -> VaultError {
        VaultError::Fireblocks(FireblocksVaultError::TransactionFailed {
            tx_id: "tx-123".to_string(),
            status,
        })
    }

    #[test]
    fn should_release_on_terminal_fireblocks_failure() {
        for status in [
            TransactionStatus::Failed,
            TransactionStatus::Rejected,
            TransactionStatus::Cancelled,
            TransactionStatus::Blocked,
        ] {
            assert!(
                should_release_reserved_burn(&transaction_failed(status)),
                "terminal status {status:?} should release the reservation"
            );
        }
    }

    #[test]
    fn should_retain_reservation_on_pending_fireblocks_status() {
        for status in [
            TransactionStatus::Broadcasting,
            TransactionStatus::Confirming,
            TransactionStatus::PendingEnrichment,
            TransactionStatus::Submitted,
        ] {
            assert!(
                !should_release_reserved_burn(&transaction_failed(status)),
                "pending status {status:?} must keep the reservation"
            );
        }
    }

    #[test]
    fn should_release_on_evm_revert() {
        let reverted = VaultError::Reverted {
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
        };

        assert!(
            should_release_reserved_burn(&reverted),
            "a reverted burn consumed no receipts and must release"
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

    type TestCqrs = SqliteCqrs<Redemption>;
    type TestStore = PersistedEventStore<SqliteEventRepository, Redemption>;

    struct TestHarness {
        cqrs: Arc<TestCqrs>,
        store: Arc<TestStore>,
        receipt_service: Arc<dyn ReceiptService>,
        receipt_inventory_store: Arc<Store<ReceiptInventory>>,
        pool: sqlx::Pool<sqlx::Sqlite>,
        asset_store: Arc<Store<TokenizedAsset>>,
    }

    impl TestHarness {
        async fn new() -> Self {
            Self::with_vault_mock(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn with_vault_mock(vault_mock: Arc<MockVaultService>) -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(5)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let redemption_view_repo = Arc::new(SqliteViewRepository::<
                RedemptionView,
                Redemption,
            >::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));

            let redemption_query = GenericQuery::new(redemption_view_repo);

            let vault_service: Arc<dyn crate::vault::VaultService> =
                vault_mock.clone();
            let cqrs = Arc::new(sqlite_cqrs(
                pool.clone(),
                vec![Box::new(redemption_query)],
                vault_service,
            ));

            let repo = SqliteEventRepository::new(pool.clone());
            let store = Arc::new(PersistedEventStore::new_event_store(repo));

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
                cqrs,
                store,
                receipt_service,
                receipt_inventory_store,
                pool,
                asset_store,
            }
        }

        async fn add_asset(
            &self,
            underlying: &UnderlyingSymbol,
            vault: Address,
        ) {
            self.asset_store
                .send(
                    underlying,
                    TokenizedAssetCommand::Add {
                        underlying: underlying.clone(),
                        token: TokenSymbol::new(format!("t{}", underlying.0)),
                        network: Network::new("base"),
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
                    &vault,
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
        cqrs: &TestCqrs,
        store: &TestStore,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        let tokenization_request_id =
            TokenizationRequestId::new("alp-burn-456");
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: quantity,
                dust_quantity: Quantity::new(Decimal::ZERO),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::ConfirmAlpacaComplete {
                issuer_request_id: issuer_request_id.clone(),
            },
        )
        .await
        .unwrap();

        load_aggregate(store, issuer_request_id).await
    }

    async fn load_aggregate(
        store: &TestStore,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Redemption {
        let mut context =
            store.load_aggregate(&issuer_request_id.to_string()).await.unwrap();
        context.aggregate().clone()
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_success() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

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
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .is_empty(),
            "successful burn must leave no dangling reservation"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_force_complete_burn_records_verified_burn() {
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burn(45_989_009, uint!(17_U256)),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
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
            receipt_service.reserved_redemptions(vault).await.unwrap(),
            vec![issuer_request_id.clone()],
            "reservation should be held before force-complete"
        );

        let burn_tx_hash = b256!(
            "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
        );

        let verification = manager
            .force_complete_burn(
                &issuer_request_id,
                burn_tx_hash,
                "burn confirmed on-chain".to_string(),
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
                .reserved_redemptions(vault)
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

    #[tokio::test]
    async fn test_force_complete_burn_rejects_unverifiable_burn() {
        let vault_mock =
            Arc::new(MockVaultService::new_success().with_unverifiable_burn());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                b256!(
                    "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
                ),
                "operator hash is not a burn".to_string(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::Vault(VaultError::NotABurn { .. })
        ));

        // An unverifiable hash must NOT terminalize — the redemption stays
        // Burning for manual reconciliation.
        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::Burning { .. }),
            "Expected Burning state, got {updated:?}"
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_reverted_tx() {
        let vault_mock =
            Arc::new(MockVaultService::new_success().with_reverted_burn());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                b256!(
                    "0x3601e281d321344b9569b44159996ae179c44e8d733cab7f81cb0424d0375ccf"
                ),
                "operator hash reverted".to_string(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            BurnManagerError::Vault(VaultError::Reverted { .. })
        ));

        let updated = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated, Redemption::Burning { .. }),
            "Expected Burning state, got {updated:?}"
        );
    }

    #[tokio::test]
    async fn test_force_complete_burn_rejects_non_burning_state() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let burning = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
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

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "burn failed".to_string(),
                fireblocks_tx_id: None,
                planned_burns: vec![],
            },
        )
        .await
        .expect("RecordBurnFailure failed");

        cqrs.execute(
            &issuer_request_id.to_string(),
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
            cqrs,
            store,
            receipt_service,
            receipt_inventory_store,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-mint-99"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::new(10000, 2)),
            Utc::now(),
            None,
        );

        receipt_inventory_store
            .send(
                &vault,
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

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

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
            cqrs,
            store,
            receipt_service,
            receipt_inventory_store,
            pool,
            ..
        } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-bytes-test"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::from(50)),
            Utc::now(),
            None,
        );

        let raw_bytes = Bytes::from(vec![0xde, 0xad, 0xbe, 0xef]);

        receipt_inventory_store
            .send(
                &vault,
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
                    receipt_info: Some(Box::new(receipt_info)),
                    receipt_info_bytes: Some(raw_bytes.clone()),
                },
            )
            .await
            .expect("Failed to discover receipt with receipt_info_bytes");

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

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
    async fn test_handle_burning_started_with_blockchain_failure() {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::Vault(_))),
            "Expected blockchain error, got {result:?}"
        );

        assert_eq!(vault_mock.get_multi_burn_call_count(), 1);

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        let Redemption::Failed { reason, .. } = updated_aggregate else {
            panic!("Expected Failed state, got {updated_aggregate:?}");
        };

        assert!(
            reason.contains("Invalid receipt"),
            "Expected error message to contain 'Invalid receipt', got: {reason}"
        );

        // The confirmation failed with an ambiguous (non-terminal) error, so
        // the reservation is intentionally RETAINED — the transaction may still
        // land on-chain, and releasing now could let a concurrent redemption
        // reuse shares that are about to be consumed.
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .contains(&issuer_request_id),
            "ambiguous burn failure must retain the reservation"
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::Vault(_))),
            "Expected submission failure, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(vault)
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::Vault(_))),
            "Expected a revert submission failure, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(vault)
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        harness
            .discover_receipt(
                vault,
                uint!(7_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(
            matches!(result, Err(BurnManagerError::Vault(_))),
            "Expected a revert confirmation failure, got {result:?}"
        );

        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .is_empty(),
            "a definitive on-chain revert must release the reservation"
        );
    }

    #[tokio::test]
    async fn test_handle_burning_started_with_insufficient_balance() {
        let harness = setup_test_environment().await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let wallet = address!("0x9876543210fedcba9876543210fedcba98765432");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0x1111111111111111111111111111111111111111111111111111111111111111"
        );
        let block_number = 54321;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying_symbol = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying_symbol, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(50));
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let block_number = 22222;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying: underlying.clone(),
                token,
                wallet,
                quantity: quantity.clone(),
                tx_hash,
                block_number,
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordAlpacaCall {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id,
                alpaca_quantity: quantity,
                dust_quantity: Quantity::new(Decimal::ZERO),
            },
        )
        .await
        .unwrap();

        cqrs.execute(
            &issuer_request_id.to_string(),
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

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_depletes_receipt() {
        let harness = setup_test_environment().await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(44_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_burn_with_multiple_receipts() {
        let harness = setup_test_environment().await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

        assert!(result.is_ok(), "Expected success, got error: {result:?}");

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;

        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state, got {updated_aggregate:?}"
        );
    }

    #[tokio::test]
    async fn test_insufficient_balance_scenario() {
        let harness = setup_test_environment().await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let aggregate = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;
        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        manager.recover_burning_redemptions().await;
    }

    #[tokio::test]
    async fn test_recover_burning_redemptions_with_valid_redemption() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        manager.recover_burning_redemptions().await;

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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        // Create a redemption in Burning state (needs 100 shares)
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;
        let blockchain_service_mock = Arc::new(MockVaultService::new_success());
        let blockchain_service = blockchain_service_mock.clone()
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let quantity = Quantity::new(Decimal::from(100));
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );
        let block_number = 12345;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::Detect {
                issuer_request_id: issuer_request_id.clone(),
                underlying,
                token,
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        // Record burn failure to transition to Failed/BurnFailed
        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Initial burn failed".to_string(),
                fireblocks_tx_id: None,
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

    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_retries_with_fresh_id_on_terminal_fireblocks_failure()
     {
        let vault_mock = Arc::new(
            MockVaultService::new_success().with_fireblocks_tx_status(
                FireblocksTxStatus::Failed {
                    detail: "FAILED".to_string(),
                    sub_status: Some("REJECTED_BY_BLOCKCHAIN".to_string()),
                    network_tx_hashes: vec![],
                },
            ),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let burning = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let Redemption::Burning { metadata, .. } = &burning else {
            panic!("Expected Burning state, got {burning:?}");
        };
        let detected_tx_hash = metadata.detected_tx_hash;

        // Fail with a recorded Fireblocks tx ID so recovery inspects it.
        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Terminal Fireblocks failure".to_string(),
                fireblocks_tx_id: Some("fb-failed-123".to_string()),
                planned_burns: vec![],
            },
        )
        .await
        .expect("Failed to record burn failure");

        manager.recover_burn_failed_redemptions().await;

        // The terminal Fireblocks failure must trigger a replacement burn.
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "Should submit a replacement burn on terminal Fireblocks failure"
        );

        // The replacement burn must carry a fresh deterministic retry id, since
        // Fireblocks rejects reusing the original externalTxId.
        let params = vault_mock
            .get_last_multi_burn_params()
            .expect("Expected replacement burn to have been submitted");

        assert_eq!(
            params.external_tx_id,
            Some(Redemption::retry_burn_external_tx_id_typed(
                &detected_tx_hash,
                1
            )),
            "Replacement burn must use retry-1 externalTxId"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["terminal failed Fireblocks transaction", "replacement burn"]
        ));
    }

    #[tokio::test]
    async fn test_recover_burn_failed_escalates_retry_id_from_prior_submission()
    {
        let vault_mock = Arc::new(
            MockVaultService::new_success().with_fireblocks_tx_status(
                FireblocksTxStatus::Failed {
                    detail: "FAILED".to_string(),
                    sub_status: Some("REJECTED_BY_BLOCKCHAIN".to_string()),
                    network_tx_hashes: vec![],
                },
            ),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let burning = create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        let Redemption::Burning { metadata, .. } = &burning else {
            panic!("Expected Burning state, got {burning:?}");
        };
        let detected_tx_hash = metadata.detected_tx_hash;
        let prior_retry_id =
            Redemption::retry_burn_external_tx_id_typed(&detected_tx_hash, 1);

        // Seed a prior replacement burn submission (retry-1) into history so
        // recovery must derive the next id by scanning history, not via the
        // no-prior-submission fallback (which would yield retry-1).
        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::BurnTokens {
                issuer_request_id: issuer_request_id.clone(),
                vault,
                burns: vec![MultiBurnEntry {
                    receipt_id: uint!(99_U256),
                    burn_shares: uint!(100_000000000000000000_U256),
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: uint!(0_U256),
                owner: TEST_WALLET,
                external_tx_id: Some(prior_retry_id),
            },
        )
        .await
        .expect("Failed to seed prior burn submission");

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Replacement burn terminally failed".to_string(),
                fireblocks_tx_id: Some("mock-fb-burn".to_string()),
                planned_burns: vec![],
            },
        )
        .await
        .expect("Failed to record burn failure");

        manager.recover_burn_failed_redemptions().await;

        // The next replacement burn must escalate to retry-2, proving the id is
        // derived from the recorded submission rather than the retry-1 fallback.
        let params = vault_mock
            .get_last_multi_burn_params()
            .expect("Expected replacement burn to have been submitted");

        assert_eq!(
            params.external_tx_id,
            Some(Redemption::retry_burn_external_tx_id_typed(
                &detected_tx_hash,
                2
            )),
            "Replacement burn must escalate to retry-2 from the prior retry-1 submission"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recovery, got {updated_aggregate:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_leaves_pending_fireblocks_tx_untouched() {
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_fireblocks_tx_status(FireblocksTxStatus::Pending),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Recorded failure while tx still pending".to_string(),
                fireblocks_tx_id: Some("fb-pending-123".to_string()),
                planned_burns: vec![],
            },
        )
        .await
        .expect("Failed to record burn failure");

        manager.recover_burn_failed_redemptions().await;

        // A pending Fireblocks tx might still land on-chain, so recovery must
        // not submit a replacement burn (would risk a double burn).
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            0,
            "Should not submit a burn while the Fireblocks tx is pending"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Failed { .. }),
            "Expected Failed state to be left unchanged, got {updated_aggregate:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["pending Fireblocks transaction", "later recovery pass"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn test_recover_burn_failed_records_existing_burn_on_completed_tx() {
        let vault_mock = Arc::new(
            MockVaultService::new_success().with_fireblocks_tx_status(
                FireblocksTxStatus::Completed {
                    tx_hash: b256!(
                        "0x4545454545454545454545454545454545454545454545454545454545454545"
                    ),
                    block_number: 5000,
                },
            ),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "Recorded failure though tx actually completed"
                    .to_string(),
                fireblocks_tx_id: Some("fb-completed-123".to_string()),
                planned_burns: vec![],
            },
        )
        .await
        .expect("Failed to record burn failure");

        manager.recover_burn_failed_redemptions().await;

        // A completed Fireblocks tx must be recorded, never resubmitted.
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            0,
            "Should not submit a replacement burn when the prior tx completed"
        );

        let updated_aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(updated_aggregate, Redemption::Completed { .. }),
            "Expected Completed state after recording existing burn, got {updated_aggregate:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["confirming previously submitted Fireblocks transaction"]
        ));
    }

    #[tokio::test]
    async fn test_recover_burn_failed_skips_when_balance_insufficient() {
        let harness = setup_test_environment().await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return balance less than required (100 shares = 100e18)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success()
                .with_share_balance(uint!(50_000000000000000000_U256)),
        );
        let blockchain_service =
            blockchain_service_mock.clone() as Arc<dyn VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        harness
            .discover_receipt(
                vault,
                uint!(99_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "RPC timeout".to_string(),
                fireblocks_tx_id: None,
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
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        // Configure mock to return 0 balance (burn already happened on-chain)
        let blockchain_service_mock = Arc::new(
            MockVaultService::new_success().with_share_balance(uint!(0_U256)),
        );
        let blockchain_service =
            blockchain_service_mock.clone() as Arc<dyn VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::RecordBurnFailure {
                issuer_request_id: issuer_request_id.clone(),
                error: "ERC1155: burn amount exceeds balance".to_string(),
                fireblocks_tx_id: None,
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

    /// Tests that recovery from `BurnSubmitted` state (crash between submit
    /// and confirm) successfully confirms the existing transaction without
    /// submitting a new one.
    #[tokio::test]
    async fn test_recover_burn_submitted_confirms_existing_transaction() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { cqrs, store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            cqrs.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        create_test_redemption_in_burning_state(
            cqrs,
            store,
            &issuer_request_id,
        )
        .await;

        // Issue BurnTokens directly to stop at BurnSubmitted (simulating
        // a crash between submit and confirm). This calls submit_burn on
        // the mock and emits BurnFireblocksSubmitted without confirming.
        cqrs.execute(
            &issuer_request_id.to_string(),
            RedemptionCommand::BurnTokens {
                issuer_request_id: issuer_request_id.clone(),
                vault,
                burns: vec![crate::vault::MultiBurnEntry {
                    receipt_id: uint!(99_U256),
                    burn_shares: uint!(100_000000000000000000_U256),
                    receipt_info: None,
                    receipt_info_bytes: None,
                }],
                dust_shares: U256::ZERO,
                owner: TEST_WALLET,
                external_tx_id: None,
            },
        )
        .await
        .expect("BurnTokens should succeed");

        // Verify aggregate is in BurnSubmitted state
        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::BurnSubmitted { .. }),
            "Expected BurnSubmitted state, got {aggregate:?}"
        );

        // Record submit_burn call count before recovery
        let submits_before = vault_mock.get_multi_burn_call_count();

        // Recovery should find this redemption (view is Burning, aggregate
        // is BurnSubmitted) and confirm the existing Fireblocks transaction
        // without submitting a new burn.
        manager.recover_burning_redemptions().await;

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
                &vault,
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            harness.pool.clone(),
            harness.cqrs.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
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
            &harness.cqrs,
            &harness.store,
            &issuer_request_id,
        )
        .await;
        manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await
            .unwrap();

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
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );

        manager.recover_stuck_reservations(&[vault]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .is_empty(),
            "GC must settle the reservation of a completed redemption"
        );
    }

    /// A reservation surviving on a `Failed` redemption is from an *ambiguous*
    /// failure (definitive failures release in the live/recovery paths), so the
    /// burn may still have landed. The GC must LEAVE it rather than release it —
    /// releasing would over-credit inventory and risk a duplicate burn.
    #[tokio::test]
    async fn test_recover_stuck_reservations_leaves_failed() {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            harness.pool.clone(),
            harness.cqrs.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
        );

        harness
            .discover_receipt(
                vault,
                uint!(1_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        // Drive the redemption to Failed (the failing mock fails the burn).
        let issuer_request_id = IssuerRedemptionRequestId::random();
        let aggregate = create_test_redemption_in_burning_state(
            &harness.cqrs,
            &harness.store,
            &issuer_request_id,
        )
        .await;
        let _ = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;
        assert!(matches!(
            load_aggregate(&harness.store, &issuer_request_id).await,
            Redemption::Failed { .. }
        ));

        seed_stuck_reservation(
            &harness,
            vault,
            &issuer_request_id,
            uint!(1_U256),
            uint!(40_000000000000000000_U256),
        )
        .await;

        manager.recover_stuck_reservations(&[vault]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(vault)
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            harness.pool.clone(),
            harness.cqrs.clone(),
            harness.store.clone(),
            harness.receipt_service.clone(),
            TEST_WALLET,
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

        manager.recover_stuck_reservations(&[vault]).await;

        assert!(
            harness
                .receipt_service
                .reserved_redemptions(vault)
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
