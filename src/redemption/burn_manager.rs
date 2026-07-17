use alloy::primitives::{Address, B256, U256};
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::view::{
    RedemptionView, RedemptionViewError, find_burn_failed, find_burning,
};
use super::{
    BurnExternalTxId, BurnRecoveryAction, IssuerRedemptionRequestId,
    Redemption, RedemptionCommand, RedemptionError, RedemptionEvent,
    next_burn_retry_external_tx_id_from_history,
};
use crate::mint::{QuantityConversionError, has_unresolved_mint_intent};
use crate::receipt_inventory::{
    BurnPlan, BurnTrackingError, ReceiptRegistrationError, ReceiptService,
    Shares,
};
use crate::redemption::{
    BurnRecord, RedemptionMetadata, has_unresolved_burn_intent,
};
use crate::tokenized_asset::UnderlyingSymbol;
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, find_vault_by_underlying,
};
use crate::vault::{
    BurnTxStatus, BurnVerification, MultiBurnEntry, SendableTxWithHash, TxId,
    VaultError, VaultService, VerifiedBurn, VerifiedShareTransfer,
};

const WALLET_INTENT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
pub(crate) const MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Copy)]
struct BurnRecoveryBudget {
    attempts: u32,
    exhausted: bool,
    last_transaction: Option<(B256, u64)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingBurnRecovery {
    Reserved(BurnRecoveryAction),
    ReplacementPrepared,
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

const fn recovery_action_for_status(
    status: BurnTxStatus,
) -> Option<BurnRecoveryAction> {
    match status {
        BurnTxStatus::StillMineable => Some(BurnRecoveryAction::Rebroadcast),
        BurnTxStatus::ProvablyDead => Some(BurnRecoveryAction::Replace),
        BurnTxStatus::Mined | BurnTxStatus::Reverted => None,
    }
}

fn terminal_classification_error() -> BurnManagerError {
    BurnManagerError::InvalidAggregateState {
        current_state: "terminal burn classification".to_string(),
    }
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
    /// A previously submitted tx burn was confirmed and recorded.
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
#[derive(Clone)]
pub(crate) struct BurnManager {
    /// Used only for balance queries during recovery (not for burns - those go through aggregate)
    vault_service: Arc<dyn VaultService>,
    view_pool: Pool<Sqlite>,
    store: Arc<Store<Redemption>>,
    receipt_service: Arc<dyn ReceiptService>,
    bot_wallet: Address,
    automatic_recovery_lock: Arc<Mutex<()>>,
}

impl BurnManager {
    /// Creates a new burn manager.
    ///
    /// # Arguments
    ///
    /// * `vault_service` - Vault service for balance queries during recovery
    /// * `view_pool` - Database pool for querying views
    /// * `store` - Event-sorcery store for dispatching commands and loading
    ///   aggregate state during recovery
    /// * `receipt_service` - Service for finding receipts to burn
    /// * `bot_wallet` - Bot's wallet address that owns both shares and receipts
    pub(crate) fn new(
        vault_service: Arc<dyn VaultService>,
        view_pool: Pool<Sqlite>,
        store: Arc<Store<Redemption>>,
        receipt_service: Arc<dyn ReceiptService>,
        bot_wallet: Address,
    ) -> Self {
        Self {
            vault_service,
            view_pool,
            store,
            receipt_service,
            bot_wallet,
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

        let (underlying, recipient, planned_burns) = match &redemption {
            Redemption::BurnIntended { metadata, planned_burns, .. }
            | Redemption::BurnSubmitted { metadata, planned_burns, .. } => {
                (metadata.underlying.clone(), metadata.wallet, planned_burns)
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
        if burn_tx_hash != persisted_burn_tx.hash
            && !burn_verification_matches_plan(
                &verification,
                planned_burns,
                self.bot_wallet,
                recipient,
                persisted_burn_tx.dust_shares,
                persisted_burn_tx.nonce,
            )
        {
            let expected_shares_burned = planned_burns
                .iter()
                .try_fold(U256::ZERO, |total, burn| {
                    total.checked_add(burn.shares_burned)
                })
                .ok_or(BurnManagerError::SharesOverflow)?;
            return Err(BurnManagerError::AlternateBurnSemanticsMismatch {
                expected_burns: planned_burns.clone(),
                expected_shares_burned,
                expected_recipient: recipient,
                expected_dust_shares: persisted_burn_tx.dust_shares,
                expected_nonce: persisted_burn_tx.nonce,
                verified_burns: verification.burns.clone(),
                verified_shares_burned: verification.shares_burned,
                verified_nonce: verification.nonce,
                verified_share_transfers: verification.share_transfers.clone(),
            });
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
                self.settle_reserved_burn(vault, issuer_request_id).await;
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
            tx_id,
            ..
        } = view
        else {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "View not in BurnFailed state, skipping"
            );
            return Ok(());
        };

        let replacement_already_reserved =
            self.failed_replacement_already_reserved(issuer_request_id).await?;
        let preparation_already_reserved =
            self.failed_preparation_already_reserved(issuer_request_id).await?;
        let recovery_allowed = if replacement_already_reserved
            || preparation_already_reserved
        {
            true
        } else if tx_id.is_none() {
            self.reserve_preparation_recovery_attempt(issuer_request_id).await?
        } else {
            self.recovery_budget_available(issuer_request_id, None).await?
        };
        if !recovery_allowed {
            debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                "Skipping BurnFailed redemption with exhausted automatic recovery budget"
            );
            return Ok(());
        }

        let vault = find_vault_by_underlying(&self.view_pool, underlying)
            .await?
            .ok_or_else(|| BurnManagerError::AssetNotFound {
                underlying: underlying.clone(),
            })?;

        // If a tx was already submitted before failure, inspect it
        // before deciding whether to confirm, wait, or submit a replacement.
        let retry_external_tx_id = if let Some(fb_tx_id) = tx_id {
            return self
                .recover_burn_failed_with_existing_tx(
                    issuer_request_id,
                    vault,
                    fb_tx_id,
                    dust_quantity,
                )
                .await;
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

            self.store.send(issuer_request_id, command).await?;

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

    async fn failed_preparation_already_reserved(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<bool, BurnManagerError> {
        let latest_event = sqlx::query_scalar::<_, String>(
            "
            SELECT event_type
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
            ORDER BY sequence DESC
            LIMIT 1
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_optional(&self.view_pool)
        .await?;

        Ok(latest_event.as_deref()
            == Some("RedemptionEvent::BurnPreparationRecoveryAttempted"))
    }

    /// Recovers a BurnFailed redemption that has a previously submitted
    /// transaction. Tries to confirm the existing transaction rather than resubmitting.
    async fn recover_burn_failed_with_existing_tx(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        tx_id: &TxId,
        dust_quantity: &crate::Quantity,
    ) -> Result<(), BurnManagerError> {
        let dust_shares = dust_quantity.to_u256_with_18_decimals()?;

        info!(target: "redemption", issuer_request_id = %issuer_request_id,
            %tx_id,
            "BurnFailed recovery — confirming previously submitted transaction"
        );

        match self.vault_service.confirm_burn(tx_id, dust_shares).await {
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
                            planned_burns: actual_burns,
                            block_number: result.block_number,
                        },
                    )
                    .await?;

                self.settle_reserved_burn(vault, issuer_request_id).await;

                Ok(())
            }
            Err(err) => {
                if should_release_reserved_burn(&err) {
                    // Confirmed on-chain revert: the tx consumed no receipts,
                    // so it is safe to release the reservation and terminalize.
                    self.release_reserved_burn(vault, issuer_request_id).await;

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

    async fn recover_single_burning_shared_inner(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        metadata: &RedemptionMetadata,
        tx_id: &TxId,
        dust_shares: U256,
        planned_burns: &[BurnRecord],
        has_submitted: bool,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let vault =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
                .ok_or_else(|| BurnManagerError::AssetNotFound {
                    underlying: metadata.underlying.clone(),
                })?;

        if has_submitted {
            info!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_id = %tx_id,
                "Recovering BurnSubmitted redemption - confirming existing transaction"
            );
        } else {
            info!(target: "redemption", issuer_request_id = %issuer_request_id,
                tx_id = %tx_id,
                "Recovering BurnIntended redemption - checking existing transaction"
            );
        }

        let confirm_result = self
            .store
            .send(
                issuer_request_id,
                RedemptionCommand::ConfirmBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_id: tx_id.clone(),
                    dust_shares,
                },
            )
            .await;

        match confirm_result {
            Ok(()) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Burn confirmed successfully during recovery"
                );

                self.settle_reserved_burn(vault, issuer_request_id).await;

                Ok(RecoveryOutcome::ExistingBurnRecorded)
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::Vault {
                    message: err,
                    release_reservation,
                    ..
                },
            ))) => {
                if has_submitted {
                    warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                        error = %err,
                        "Burn confirmation failed during recovery"
                    );
                } else {
                    warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                        error = %err,
                        "BurnIntended confirmation failed during recovery"
                    );
                }

                if release_reservation {
                    let exact_recovery = self
                        .reserve_replacement_after_revert(issuer_request_id)
                        .await?;
                    // Release before terminalizing the redemption. A crash
                    // after this idempotent release leaves the aggregate in
                    // its recoverable state, so the same transaction is
                    // checked again. Recording failure first could strand a
                    // reservation because burning-state recovery would no
                    // longer revisit the aggregate.
                    self.release_reserved_burn(vault, issuer_request_id).await;
                    self.store
                        .send(
                            issuer_request_id,
                            RedemptionCommand::RecordBurnFailure {
                                issuer_request_id: issuer_request_id.clone(),
                                error: err.clone(),
                                tx_id: exact_recovery
                                    .is_none()
                                    .then(|| tx_id.clone()),
                                planned_burns: planned_burns.to_vec(),
                            },
                        )
                        .await?;
                } else {
                    warn!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        tx_id = %tx_id,
                        "Burn confirmation remains uncertain; keeping persisted transaction recoverable"
                    );
                }

                Err(BurnManagerError::Redemption(RedemptionError::Vault {
                    message: err,
                    release_reservation,
                    tx_id: None,
                }))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn recover_persisted_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        metadata: &RedemptionMetadata,
        planned_burns: &[BurnRecord],
        sendable_tx: &SendableTxWithHash,
        external_tx_id: Option<BurnExternalTxId>,
        has_submitted: bool,
    ) -> Result<RecoveryOutcome, BurnManagerError> {
        let wallet_guard = self.vault_service.lock_wallet().await;
        let current = self.store.load(issuer_request_id).await?;
        let persisted_burn_matches = match &current {
            Some(Redemption::BurnIntended {
                sendable_tx: current_tx, ..
            }) => !has_submitted && current_tx == sendable_tx,
            Some(Redemption::BurnSubmitted {
                sendable_tx: current_tx, ..
            }) => has_submitted && current_tx == sendable_tx,
            _ => false,
        };
        if !persisted_burn_matches {
            debug!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                expected_tx_hash = %sendable_tx.hash,
                current_state = current
                    .as_ref()
                    .map_or("Missing", aggregate_state_name),
                "Skipping stale persisted burn recovery"
            );
            drop(wallet_guard);
            return Ok(RecoveryOutcome::AlreadyAdvanced);
        }

        let pending_recovery = self
            .pending_recovery_action(issuer_request_id, sendable_tx)
            .await?;
        if let Some(outcome) = self
            .exhausted_recovery_outcome(issuer_request_id, sendable_tx)
            .await?
        {
            drop(wallet_guard);
            return Ok(outcome);
        }

        let status = self
            .vault_service
            .classify_burn_tx(self.bot_wallet, sendable_tx)
            .await?;
        let tx_id = sendable_tx.hash.into();
        if matches!(status, BurnTxStatus::Mined | BurnTxStatus::Reverted) {
            drop(wallet_guard);
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
        if pending_recovery == Some(PendingBurnRecovery::ReplacementPrepared)
            && status == BurnTxStatus::StillMineable
        {
            self.submit_prepared_replacement(
                issuer_request_id,
                metadata,
                planned_burns,
                sendable_tx,
                external_tx_id,
            )
            .await?;
            drop(wallet_guard);
            info!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                tx_hash = %sendable_tx.hash,
                "Submitted persisted replacement from reserved recovery action"
            );
            return Ok(RecoveryOutcome::Executed);
        }

        let action = recovery_action_for_status(status)
            .ok_or_else(terminal_classification_error)?;
        let action_already_reserved =
            pending_recovery == Some(PendingBurnRecovery::Reserved(action));
        let vault =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
                .ok_or_else(|| BurnManagerError::AssetNotFound {
                    underlying: metadata.underlying.clone(),
                })?;
        if !action_already_reserved
            && !self
                .recovery_budget_available(issuer_request_id, Some(sendable_tx))
                .await?
        {
            drop(wallet_guard);
            return Ok(RecoveryOutcome::SkippedManualIntervention);
        }
        if status == BurnTxStatus::ProvablyDead {
            let unresolved_mint =
                has_unresolved_mint_intent(&self.view_pool, None).await?;
            let unresolved_burn = has_unresolved_burn_intent(
                &self.view_pool,
                Some(issuer_request_id),
            )
            .await?;
            if unresolved_mint || unresolved_burn {
                drop(wallet_guard);
                debug!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    tx_hash = %sendable_tx.hash,
                    "Deferring dead burn replacement behind another persisted wallet intent"
                );
                return Ok(RecoveryOutcome::SkippedManualIntervention);
            }
        }
        if !action_already_reserved
            && !self
                .reserve_recovery_attempt(
                    issuer_request_id,
                    sendable_tx,
                    action,
                )
                .await?
        {
            drop(wallet_guard);
            return Ok(RecoveryOutcome::SkippedManualIntervention);
        }
        if action_already_reserved {
            info!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                tx_hash = %sendable_tx.hash,
                action = ?action,
                "Resuming reserved burn recovery action"
            );
        }

        let burns = recovery_burn_entries(planned_burns);
        let command = match status {
            BurnTxStatus::StillMineable => RedemptionCommand::BurnTokens {
                issuer_request_id: issuer_request_id.clone(),
                vault,
                burns,
                dust_shares: sendable_tx.dust_shares,
                owner: self.bot_wallet,
                external_tx_id,
            },
            BurnTxStatus::ProvablyDead => RedemptionCommand::ReplaceDeadBurn {
                issuer_request_id: issuer_request_id.clone(),
                owner: self.bot_wallet,
            },
            BurnTxStatus::Mined | BurnTxStatus::Reverted => {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: "terminal burn classification".to_string(),
                });
            }
        };
        let command_result = self.store.send(issuer_request_id, command).await;
        if matches!(
            command_result,
            Err(AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::BurnReplacementPreparationFailed { .. }
            )))
        ) && action == BurnRecoveryAction::Replace
            && !self
                .recovery_budget_available(issuer_request_id, Some(sendable_tx))
                .await?
        {
            drop(wallet_guard);
            return Ok(RecoveryOutcome::SkippedManualIntervention);
        }
        command_result?;

        if status == BurnTxStatus::ProvablyDead {
            let Some(Redemption::BurnIntended {
                planned_burns,
                sendable_tx,
                external_tx_id,
                ..
            }) = self.store.load(issuer_request_id).await?
            else {
                return Err(BurnManagerError::InvalidAggregateState {
                    current_state: "expected replacement BurnIntended"
                        .to_string(),
                });
            };
            let replacement_burns = recovery_burn_entries(&planned_burns);
            self.store
                .send(
                    issuer_request_id,
                    RedemptionCommand::BurnTokens {
                        issuer_request_id: issuer_request_id.clone(),
                        vault,
                        burns: replacement_burns,
                        dust_shares: sendable_tx.dust_shares,
                        owner: self.bot_wallet,
                        external_tx_id,
                    },
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
        Ok(RecoveryOutcome::Executed)
    }

    async fn submit_prepared_replacement(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        metadata: &RedemptionMetadata,
        planned_burns: &[BurnRecord],
        sendable_tx: &SendableTxWithHash,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Result<(), BurnManagerError> {
        let vault =
            find_vault_by_underlying(&self.view_pool, &metadata.underlying)
                .await?
                .ok_or_else(|| BurnManagerError::AssetNotFound {
                    underlying: metadata.underlying.clone(),
                })?;
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::BurnTokens {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: recovery_burn_entries(planned_burns),
                    dust_shares: sendable_tx.dust_shares,
                    owner: self.bot_wallet,
                    external_tx_id,
                },
            )
            .await?;
        Ok(())
    }

    async fn exhausted_recovery_outcome(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<Option<RecoveryOutcome>, BurnManagerError> {
        if !self.burn_recovery_budget(issuer_request_id).await?.exhausted {
            return Ok(None);
        }
        debug!(target: "redemption",
            issuer_request_id = %issuer_request_id,
            tx_hash = %sendable_tx.hash,
            "Skipping burn with exhausted automatic recovery budget"
        );
        Ok(Some(RecoveryOutcome::SkippedManualIntervention))
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
                    metadata,
                    planned_burns,
                    sendable_tx,
                    Some(external_tx_id.clone()),
                    true,
                )
                .await
            }

            Redemption::BurnIntended {
                metadata,
                planned_burns,
                sendable_tx,
                external_tx_id,
                ..
            } => {
                self.recover_persisted_burn(
                    issuer_request_id,
                    metadata,
                    planned_burns,
                    sendable_tx,
                    external_tx_id.clone(),
                    false,
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

                self.handle_burning_started(issuer_request_id, &aggregate)
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

            self.store
                .send(
                    issuer_request_id,
                    RedemptionCommand::RecordBurnFailure {
                        issuer_request_id: issuer_request_id.clone(),
                        error: error_msg,
                        tx_id: None,
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
                  'RedemptionEvent::BurnPreparationRecoveryAttempted',
                  'RedemptionEvent::BurnRecoveryExhausted',
                  'RedemptionEvent::BurnPreparationRecoveryExhausted'
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
                RedemptionEvent::BurnPreparationRecoveryAttempted {
                    ..
                } => {
                    budget.attempts =
                        budget.attempts.checked_add(1).ok_or_else(|| {
                            BurnManagerError::RecoveryAttemptOverflow {
                                issuer_request_id: issuer_request_id.clone(),
                            }
                        })?;
                }
                RedemptionEvent::BurnRecoveryExhausted {
                    tx_hash,
                    nonce,
                    ..
                } => {
                    budget.exhausted = true;
                    budget.last_transaction = Some((tx_hash, nonce));
                }
                RedemptionEvent::BurnPreparationRecoveryExhausted {
                    ..
                } => {
                    budget.exhausted = true;
                }
                _ => {}
            }
        }

        Ok(budget)
    }

    async fn pending_recovery_action(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<Option<PendingBurnRecovery>, BurnManagerError> {
        let payloads = sqlx::query_scalar::<_, String>(
            "
            SELECT payload
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type IN (
                  'RedemptionEvent::BurnRecoveryAttempted',
                  'RedemptionEvent::BurnRecoveryExhausted',
                  'RedemptionEvent::BurnIntended',
                  'RedemptionEvent::BurnTxSubmitted',
                  'RedemptionEvent::BurningFailed'
              )
            ORDER BY sequence DESC
            LIMIT 2
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_all(&self.view_pool)
        .await?;
        let events = payloads
            .iter()
            .map(|payload| serde_json::from_str::<RedemptionEvent>(payload))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(match events.as_slice() {
            [
                RedemptionEvent::BurnRecoveryAttempted {
                    tx_hash,
                    nonce,
                    action,
                    ..
                },
                ..,
            ] if *tx_hash == sendable_tx.hash
                && *nonce == sendable_tx.nonce =>
            {
                Some(PendingBurnRecovery::Reserved(*action))
            }
            [
                RedemptionEvent::BurnIntended {
                    sendable_tx: replacement, ..
                },
                RedemptionEvent::BurnRecoveryAttempted {
                    action: BurnRecoveryAction::Replace,
                    ..
                },
            ] if replacement == sendable_tx => {
                Some(PendingBurnRecovery::ReplacementPrepared)
            }
            _ => None,
        })
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

        let transaction = current_transaction
            .map(|transaction| (transaction.hash, transaction.nonce))
            .or(budget.last_transaction);
        if let Some((tx_hash, nonce)) = transaction {
            self.persist_recovery_exhaustion(
                issuer_request_id,
                tx_hash,
                nonce,
                budget.attempts,
            )
            .await?;
        } else {
            self.persist_preparation_recovery_exhaustion(
                issuer_request_id,
                budget.attempts,
            )
            .await?;
        }
        Ok(false)
    }

    async fn reserve_preparation_recovery_attempt(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> Result<bool, BurnManagerError> {
        let _guard = self.automatic_recovery_lock.lock().await;
        let budget = self.burn_recovery_budget(issuer_request_id).await?;
        if budget.exhausted {
            return Ok(false);
        }
        if budget.attempts >= MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS {
            self.persist_preparation_recovery_exhaustion(
                issuer_request_id,
                budget.attempts,
            )
            .await?;
            return Ok(false);
        }
        let attempt = budget.attempts.checked_add(1).ok_or_else(|| {
            BurnManagerError::RecoveryAttemptOverflow {
                issuer_request_id: issuer_request_id.clone(),
            }
        })?;
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnPreparationRecoveryAttempt {
                    issuer_request_id: issuer_request_id.clone(),
                    attempt,
                },
            )
            .await?;
        Ok(true)
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

    async fn persist_preparation_recovery_exhaustion(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        attempts: u32,
    ) -> Result<(), BurnManagerError> {
        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnPreparationRecoveryExhausted {
                    issuer_request_id: issuer_request_id.clone(),
                    attempts,
                },
            )
            .await?;
        error!(target: "redemption",
            issuer_request_id = %issuer_request_id,
            attempts,
            operator_action = "inspect repeated burn preparation failures before manual recovery",
            "Automatic burn preparation recovery exhausted"
        );
        Ok(())
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

        self.store
            .send(
                issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
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
        vault: Address,
        plan: BurnPlan,
        external_tx_id: Option<BurnExternalTxId>,
    ) -> Result<(), BurnManagerError> {
        self.execute_burn_with_wallet_intent_timeout(
            issuer_request_id,
            vault,
            plan,
            external_tx_id,
            WALLET_INTENT_WAIT_TIMEOUT,
        )
        .await
    }

    async fn execute_burn_with_wallet_intent_timeout(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        plan: BurnPlan,
        external_tx_id: Option<BurnExternalTxId>,
        wait_timeout: Duration,
    ) -> Result<(), BurnManagerError> {
        let execution = BurnExecutionPlan::new(vault, &plan, external_tx_id);
        let wallet_guard = if let Ok(result) = tokio::time::timeout(wait_timeout, async {
            loop {
                let wallet_guard = self.vault_service.lock_wallet().await;
                let unresolved_mint =
                    has_unresolved_mint_intent(&self.view_pool, None).await?;
                let unresolved_burn = has_unresolved_burn_intent(
                    &self.view_pool,
                    Some(issuer_request_id),
                )
                .await?;
                if !unresolved_mint && !unresolved_burn {
                    return Ok::<_, BurnManagerError>(wallet_guard);
                }

                drop(wallet_guard);
                debug!(target: "redemption",
                    issuer_request_id = %issuer_request_id,
                    "Waiting for an earlier wallet intent before preparing burn"
                );
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await {
            result?
        } else {
            warn!(target: "redemption",
                issuer_request_id = %issuer_request_id,
                wait_ms = wait_timeout.as_millis(),
                "Deferring burn after wallet-intent wait deadline"
            );
            return Err(BurnManagerError::WalletIntentWaitTimeout {
                issuer_request_id: issuer_request_id.clone(),
            });
        };
        if !self.is_burn_execution_current(issuer_request_id).await? {
            return Ok(());
        }

        self.reserve_execution(issuer_request_id, &execution).await?;
        self.persist_burn_intention(issuer_request_id, &execution).await?;
        let tx_id =
            self.submit_intended_burn(issuer_request_id, &execution).await;
        drop(wallet_guard);
        let tx_id = tx_id?;

        self.confirm_submitted_burn(issuer_request_id, &execution, tx_id).await
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
        let result = self
            .reserve_with_conflict_retry(
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
                    issuer_request_id: issuer_request_id.clone(),
                    error: error.to_string(),
                    tx_id: None,
                    planned_burns: execution.planned_burns.clone(),
                },
            )
            .await?;

        Err(error.into())
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
                    vault: execution.vault,
                    burns: execution.burns.clone(),
                    dust_shares: execution.dust_shares,
                    owner: self.bot_wallet,
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
                self.release_reserved_burn(execution.vault, issuer_request_id)
                    .await;
                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordBurnFailure {
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
            Err(error) => Err(error.into()),
        }
    }

    async fn submit_intended_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
    ) -> Result<TxId, BurnManagerError> {
        let aggregate =
            self.store.load(issuer_request_id).await?.ok_or_else(|| {
                BurnManagerError::InvalidAggregateState {
                    current_state: "Uninitialized".to_string(),
                }
            })?;
        let Redemption::BurnIntended { .. } = &aggregate else {
            return Err(BurnManagerError::InvalidAggregateState {
                current_state: aggregate_state_name(&aggregate).to_string(),
            });
        };
        let result = self
            .store
            .send(
                issuer_request_id,
                RedemptionCommand::BurnTokens {
                    issuer_request_id: issuer_request_id.clone(),
                    vault: execution.vault,
                    burns: execution.burns.clone(),
                    dust_shares: execution.dust_shares,
                    owner: self.bot_wallet,
                    external_tx_id: execution.external_tx_id.clone(),
                },
            )
            .await;

        match result {
            Ok(()) => {
                debug!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "BurnTokens submitted, confirming..."
                );
                self.load_submitted_tx_id(issuer_request_id).await
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::Vault { message, release_reservation, tx_id },
            ))) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %message,
                    tx_id = ?tx_id,
                    "Burn submission failed"
                );
                if release_reservation {
                    self.release_before_terminal_failure(
                        execution.vault,
                        issuer_request_id,
                    )
                    .await;
                }

                if !release_reservation {
                    warn!(target: "redemption",
                        issuer_request_id = %issuer_request_id,
                        "Burn broadcast outcome is ambiguous; keeping persisted transaction for recovery"
                    );
                    return Err(BurnManagerError::Redemption(
                        RedemptionError::Vault {
                            message,
                            release_reservation: false,
                            tx_id,
                        },
                    ));
                }

                self.store
                    .send(
                        issuer_request_id,
                        RedemptionCommand::RecordBurnFailure {
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
                }))
            }
            Err(error) => Err(error.into()),
        }
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

    async fn confirm_submitted_burn(
        &self,
        issuer_request_id: &IssuerRedemptionRequestId,
        execution: &BurnExecutionPlan,
        tx_id: TxId,
    ) -> Result<(), BurnManagerError> {
        let result = self
            .store
            .send(
                issuer_request_id,
                RedemptionCommand::ConfirmBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_id: tx_id.clone(),
                    dust_shares: execution.dust_shares,
                },
            )
            .await;

        match result {
            Ok(()) => {
                info!(target: "redemption", issuer_request_id = %issuer_request_id,
                    "Burn confirmed successfully"
                );
                self.settle_reserved_burn(execution.vault, issuer_request_id)
                    .await;
                Ok(())
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                RedemptionError::Vault { message, release_reservation, .. },
            ))) => {
                warn!(target: "redemption", issuer_request_id = %issuer_request_id,
                    error = %message,
                    "Burn confirmation failed"
                );
                if release_reservation {
                    let exact_recovery = self
                        .reserve_replacement_after_revert(issuer_request_id)
                        .await?;
                    self.release_before_terminal_failure(
                        execution.vault,
                        issuer_request_id,
                    )
                    .await;
                    self.store
                        .send(
                            issuer_request_id,
                            RedemptionCommand::RecordBurnFailure {
                                issuer_request_id: issuer_request_id.clone(),
                                error: message.clone(),
                                tx_id: exact_recovery
                                    .is_none()
                                    .then(|| tx_id.clone()),
                                planned_burns: execution.planned_burns.clone(),
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
                }))
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn release_before_terminal_failure(
        &self,
        vault: Address,
        issuer_request_id: &IssuerRedemptionRequestId,
    ) {
        // A crash after this idempotent release leaves the redemption in its
        // recoverable state, so the same transaction is checked again.
        // Recording failure first could strand the reservation because
        // burning-state recovery would no longer revisit the aggregate.
        self.release_reserved_burn(vault, issuer_request_id).await;
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

struct BurnExecutionPlan {
    vault: Address,
    burns: Vec<MultiBurnEntry>,
    planned_burns: Vec<BurnRecord>,
    dust_shares: U256,
    external_tx_id: Option<BurnExternalTxId>,
}

impl BurnExecutionPlan {
    fn new(
        vault: Address,
        plan: &BurnPlan,
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

        Self {
            vault,
            burns,
            planned_burns,
            dust_shares: plan.dust.inner(),
            external_tx_id,
        }
    }
}

const fn aggregate_state_name(aggregate: &Redemption) -> &'static str {
    match aggregate {
        Redemption::Detected { .. } => "Detected",
        Redemption::AlpacaCalled { .. } => "AlpacaCalled",
        Redemption::Burning { .. } => "Burning",
        Redemption::BurnSubmitted { .. } => "BurnSubmitted",
        Redemption::Failed { .. } => "Failed",
        Redemption::Completed { .. } => "Completed",
        Redemption::Closed { .. } => "Closed",
        Redemption::BurnIntended { .. } => "BurnIntended",
    }
}

/// Extracts the transaction hash from a `VaultError`.
pub(crate) const fn extract_tx_hash(error: &VaultError) -> Option<B256> {
    match error {
        VaultError::Reverted { tx_hash }
        | VaultError::EventNotFound { tx_hash } => Some(*tx_hash),
        _ => None,
    }
}

/// Whether a failed burn confirmation definitively consumed no receipts, so its
/// inventory reservation must be released. Ambiguous pending tx statuses
/// keep the reservation (the transaction may still land on-chain).
pub(crate) const fn should_release_reserved_burn(error: &VaultError) -> bool {
    matches!(error, VaultError::Reverted { .. })
}

pub(crate) const fn is_pending_burn_confirmation(error: &VaultError) -> bool {
    matches!(
        error,
        VaultError::ConfirmationPending { .. }
            | VaultError::PendingTransaction(_)
            | VaultError::Rpc(_)
    )
}

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
    #[error(
        "Alternate proving transaction does not match the persisted burn semantics: expected nonce {expected_nonce}, total shares {expected_shares_burned}, burns {expected_burns:?}, recipient {expected_recipient}, dust {expected_dust_shares}; verified nonce {verified_nonce}, total shares {verified_shares_burned}, burns {verified_burns:?}, transfers {verified_share_transfers:?}"
    )]
    AlternateBurnSemanticsMismatch {
        expected_burns: Vec<BurnRecord>,
        expected_shares_burned: U256,
        expected_recipient: Address,
        expected_dust_shares: U256,
        expected_nonce: u64,
        verified_burns: Vec<VerifiedBurn>,
        verified_shares_burned: U256,
        verified_nonce: u64,
        verified_share_transfers: Vec<VerifiedShareTransfer>,
    },
    #[error("Receipt reservation error: {0}")]
    ReceiptRegistration(#[from] ReceiptRegistrationError),
    #[error(
        "Timed out waiting to prepare burn for redemption {issuer_request_id} behind an earlier wallet intent"
    )]
    WalletIntentWaitTimeout { issuer_request_id: IssuerRedemptionRequestId },
    #[error("Burn recovery attempt counter overflowed for {issuer_request_id}")]
    RecoveryAttemptOverflow { issuer_request_id: IssuerRedemptionRequestId },
}

fn burn_verification_matches_plan(
    verification: &BurnVerification,
    planned_burns: &[BurnRecord],
    owner: Address,
    recipient: Address,
    dust_shares: U256,
    expected_nonce: u64,
) -> bool {
    let Some(expected_shares_burned) =
        planned_burns.iter().try_fold(U256::ZERO, |total, burn| {
            total.checked_add(burn.shares_burned)
        })
    else {
        return false;
    };
    let mut expected = planned_burns
        .iter()
        .map(|burn| (burn.receipt_id, burn.shares_burned))
        .collect::<Vec<_>>();
    let mut verified = verification
        .burns
        .iter()
        .filter(|burn| burn.sender == owner && burn.receiver == recipient)
        .map(|burn| (burn.receipt_id, burn.shares_burned))
        .collect::<Vec<_>>();
    expected.sort_unstable();
    verified.sort_unstable();
    let expected_share_transfers = if dust_shares.is_zero() {
        vec![]
    } else {
        vec![(recipient, dust_shares)]
    };
    let verified_share_transfers = verification
        .share_transfers
        .iter()
        .map(|transfer| (transfer.recipient, transfer.shares))
        .collect::<Vec<_>>();

    verification.nonce == expected_nonce
        && verification.shares_burned == expected_shares_burned
        && expected == verified
        && verification.burns.len() == verified.len()
        && expected_share_transfers == verified_share_transfers
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, Bytes, U256, address, b256, uint};
    use chrono::Utc;
    use event_sorcery::{Store, StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::{SqlitePool, sqlite::SqlitePoolOptions};
    use std::sync::Arc;
    use std::time::Duration;
    use tracing_test::traced_test;

    use super::{
        BurnManager, BurnManagerError, MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        RecoveryOutcome, Redemption, RedemptionCommand,
        burn_verification_matches_plan, should_release_reserved_burn,
    };
    use crate::mint::IssuerMintRequestId;
    use crate::mint::{Network, Quantity, TokenizationRequestId};
    use crate::receipt_inventory::{
        BurnTrackingError, CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptService, ReceiptSource, Shares,
    };
    use crate::redemption::BurnExternalTxId;
    use crate::redemption::view::{RedemptionViewReactor, find_burn_failed};
    use crate::redemption::{
        BurnRecord, BurnRecoveryAction, IssuerRedemptionRequestId,
        RedemptionError,
    };
    use crate::test_utils::{log_count_at, logs_contain_at};
    use crate::tokenized_asset::{
        TokenSymbol, TokenizedAsset, TokenizedAssetCommand, UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnTxStatus, BurnVerification, MultiBurnEntry, ReceiptInformation,
        SendableTxWithHash, TxId, VaultError, VaultService, VerifiedBurn,
        VerifiedShareTransfer,
    };

    const TEST_WALLET: Address =
        address!("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266");

    fn transaction_failed() -> VaultError {
        VaultError::Reverted { tx_hash: B256::random() }
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

    #[test]
    fn alternate_burn_semantics_require_recipient_and_dust_transfer() {
        let owner = TEST_WALLET;
        let recipient = address!("0x1234567890abcdef1234567890abcdef12345678");
        let planned_burns = vec![BurnRecord {
            receipt_id: uint!(42_U256),
            shares_burned: uint!(17_U256),
        }];
        let verification = BurnVerification {
            block_number: 45_989_009,
            nonce: 7,
            shares_burned: uint!(17_U256),
            burns: vec![VerifiedBurn {
                sender: owner,
                receiver: recipient,
                receipt_id: uint!(42_U256),
                shares_burned: uint!(17_U256),
            }],
            share_transfers: vec![],
        };

        assert!(!burn_verification_matches_plan(
            &verification,
            &planned_burns,
            owner,
            recipient,
            uint!(3_U256),
            7,
        ));

        let verification = BurnVerification {
            share_transfers: vec![VerifiedShareTransfer {
                recipient,
                shares: uint!(3_U256),
            }],
            ..verification
        };
        assert!(burn_verification_matches_plan(
            &verification,
            &planned_burns,
            owner,
            recipient,
            uint!(3_U256),
            7,
        ));
        let mismatched_total = BurnVerification {
            shares_burned: uint!(18_U256),
            ..verification.clone()
        };
        assert!(!burn_verification_matches_plan(
            &mismatched_total,
            &planned_burns,
            owner,
            recipient,
            uint!(3_U256),
            7,
        ));
        let replayed_at_another_nonce =
            BurnVerification { nonce: 8, ..verification };
        assert!(!burn_verification_matches_plan(
            &replayed_at_another_nonce,
            &planned_burns,
            owner,
            recipient,
            uint!(3_U256),
            7,
        ));
    }

    struct TestHarness {
        store: Arc<Store<Redemption>>,
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

            Self::with_pool(vault_mock, pool).await
        }

        async fn with_pool(
            vault_mock: Arc<MockVaultService>,
            pool: SqlitePool,
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
                .build(vault_service)
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
        store: &Store<Redemption>,
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

        store
            .send(
                issuer_request_id,
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

    async fn persist_test_burn_intent(
        store: &Store<Redemption>,
        issuer_request_id: &IssuerRedemptionRequestId,
        vault: Address,
        owner: Address,
        dust_shares: U256,
    ) {
        store
            .send(
                issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: uint!(42_U256),
                        burn_shares: uint!(17_U256),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares,
                    owner,
                    external_tx_id: None,
                },
            )
            .await
            .expect("persisted burn intent should succeed");
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
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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
    async fn burn_wait_for_earlier_wallet_intent_is_bounded() {
        let vault_mock = Arc::new(MockVaultService::new_success());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;
        harness
            .discover_receipt(
                vault,
                uint!(42_U256),
                uint!(100_000000000000000000_U256),
            )
            .await;

        let unresolved_mint_id = IssuerMintRequestId::random().to_string();
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
                'Mint',
                ?,
                1,
                'MintEvent::MintTxIntended',
                '4.0',
                '{}',
                '{}'
            )
            ",
        )
        .bind(unresolved_mint_id)
        .execute(pool)
        .await
        .unwrap();

        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        let plan = manager
            .plan_burn(
                &issuer_request_id,
                vault,
                &underlying,
                uint!(100_000000000000000000_U256),
                U256::ZERO,
            )
            .await
            .unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            manager.execute_burn_with_wallet_intent_timeout(
                &issuer_request_id,
                vault,
                plan,
                None,
                Duration::from_millis(100),
            ),
        )
        .await
        .expect("wallet-intent wait must return before its deadline");

        assert!(matches!(
            result,
            Err(BurnManagerError::WalletIntentWaitTimeout {
                issuer_request_id: blocked_id,
            }) if blocked_id == issuer_request_id
        ));
        assert_eq!(vault_mock.get_multi_burn_call_count(), 0);
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &[
                "Deferring burn after wallet-intent wait deadline",
                &issuer_request_id.to_string(),
                "wait_ms=100",
            ]
        ));
        let aggregate = store
            .load(&issuer_request_id)
            .await
            .unwrap()
            .expect("redemption should still exist");
        assert!(matches!(aggregate, Redemption::Burning { .. }));
    }

    #[traced_test]
    #[tokio::test]
    async fn force_complete_acknowledged_alternate_burn_settles_reservation() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: owner,
                        receiver: address!(
                            "0x1234567890abcdef1234567890abcdef12345678"
                        ),
                        receipt_id: uint!(42_U256),
                        shares_burned: uint!(17_U256),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
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
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
        )
        .await;

        let burn_tx_hash = B256::random();

        let verification = manager
            .force_complete_burn(
                &issuer_request_id,
                burn_tx_hash,
                "burn confirmed on-chain".to_string(),
                Some(persisted_tx.hash),
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

        let proving_hash = format!("{burn_tx_hash:?}");
        let persisted_hash = format!("{:?}", persisted_tx.hash);
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Force-completing stuck Burning redemption",
                "verified on-chain",
                "acknowledged_unresolved_burn_tx_hash",
                &proving_hash,
                &persisted_hash,
            ]
        ));
    }

    #[tokio::test]
    async fn force_complete_rejects_alternate_burn_for_another_recipient() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            7,
            vault,
            Bytes::from_static(&[0xde, 0xad]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_verified_burns(
                    45_989_009,
                    persisted_tx.nonce,
                    vec![VerifiedBurn {
                        sender: owner,
                        receiver: address!(
                            "0x9999999999999999999999999999999999999999"
                        ),
                        receipt_id: uint!(42_U256),
                        shares_burned: uint!(17_U256),
                    }],
                    vec![],
                )
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        harness.discover_receipt(vault, uint!(42_U256), uint!(17_U256)).await;
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
            .expect("reservation should seed");
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
        )
        .await;

        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                B256::random(),
                "another redemption's burn".to_string(),
                Some(persisted_tx.hash),
            )
            .await;

        assert!(matches!(
            result,
            Err(BurnManagerError::AlternateBurnSemanticsMismatch { .. })
        ));
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );
    }

    #[tokio::test]
    async fn force_complete_forwards_persisted_dust_and_nonce_to_verification()
    {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let recipient = address!("0x1234567890abcdef1234567890abcdef12345678");
        let dust_shares = uint!(3_U256);

        for (scenario, verified_nonce, verified_share_transfers) in [
            ("missing dust transfer", 7, vec![]),
            (
                "different nonce",
                8,
                vec![VerifiedShareTransfer { recipient, shares: dust_shares }],
            ),
        ] {
            let mut persisted_tx = SendableTxWithHash::valid_for_test(
                7,
                vault,
                Bytes::from_static(&[0xde, 0xad]),
            );
            persisted_tx.dust_shares = dust_shares;
            let owner = persisted_tx.signer_for_test();
            let vault_mock = Arc::new(
                MockVaultService::new_success()
                    .with_verified_burns(
                        45_989_009,
                        verified_nonce,
                        vec![VerifiedBurn {
                            sender: owner,
                            receiver: recipient,
                            receipt_id: uint!(42_U256),
                            shares_burned: uint!(17_U256),
                        }],
                        verified_share_transfers,
                    )
                    .with_prepared_tx(persisted_tx.clone()),
            );
            let harness =
                TestHarness::with_vault_mock(vault_mock.clone()).await;
            let TestHarness { store, receipt_service, pool, .. } = &harness;
            harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
            let manager = BurnManager::new(
                vault_mock.clone(),
                pool.clone(),
                store.clone(),
                receipt_service.clone(),
                owner,
            );
            let issuer_request_id = IssuerRedemptionRequestId::random();
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;
            harness
                .discover_receipt(vault, uint!(42_U256), uint!(17_U256))
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
                .expect("reservation should seed");
            persist_test_burn_intent(
                store,
                &issuer_request_id,
                vault,
                owner,
                dust_shares,
            )
            .await;

            let result = manager
                .force_complete_burn(
                    &issuer_request_id,
                    B256::random(),
                    scenario.to_string(),
                    Some(persisted_tx.hash),
                )
                .await;

            assert!(
                matches!(
                    &result,
                    Err(BurnManagerError::AlternateBurnSemanticsMismatch {
                        expected_dust_shares,
                        expected_nonce,
                        ..
                    }) if *expected_dust_shares == dust_shares
                        && *expected_nonce == persisted_tx.nonce
                ),
                "scenario {scenario} unexpectedly succeeded: {result:?}"
            );
            assert!(
                matches!(
                    load_aggregate(store, &issuer_request_id).await,
                    Redemption::BurnIntended { .. }
                ),
                "scenario {scenario} changed the redemption"
            );
            assert_eq!(
                receipt_service.reserved_redemptions(vault).await.unwrap(),
                vec![issuer_request_id],
                "scenario {scenario} released the reservation"
            );
            assert_eq!(vault_mock.verify_burn_call_count(), 1);
        }
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

        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
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

        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
        )
        .await;

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
        let manager = BurnManager::new(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
                None,
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
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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
    async fn test_handle_burning_started_with_ambiguous_failure_stays_recoverable()
     {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
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
                .reserved_redemptions(vault)
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
            .await;

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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        let result = manager
            .handle_burning_started(&issuer_request_id, &aggregate)
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        store
            .send(
                &issuer_request_id,
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying_symbol = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying_symbol, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        store
            .send(
                &issuer_request_id,
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
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service = Arc::new(MockVaultService::new_success())
            as Arc<dyn crate::vault::VaultService>;
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        create_test_redemption_in_burning_state(store, &issuer_request_id)
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

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
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        store
            .send(
                &issuer_request_id,
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
        let TestHarness { store, receipt_service, pool, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        // Record burn failure to transition to Failed/BurnFailed
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
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

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
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
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let issuer_request_id = IssuerRedemptionRequestId::random();

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnFailure {
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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

        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;

        receipt_service
            .reserve_burn(
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
                .reserved_redemptions(vault)
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
                .reserved_redemptions(vault)
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
                .reserved_redemptions(vault)
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

    /// Recovery must re-broadcast the exact persisted transaction before
    /// confirming it, covering a crash after persistence but before broadcast.
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

        for _ in 0..3 {
            assert!(matches!(
                manager.recover_single_burning(issuer_request_id).await,
                Ok(RecoveryOutcome::Executed)
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
            classifications_at_cap + 1,
            "the fifth result must be classified before exhaustion is persisted"
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
        let classifications_after_exhaustion = classifications_at_cap + 1;
        assert_eq!(
            vault_mock.burn_classification_call_count(),
            classifications_after_exhaustion,
            "persisted exhaustion must skip classification RPCs"
        );
        assert_eq!(
            vault_mock.replacement_preparation_call_count(),
            replacements_at_cap,
            "persisted exhaustion must skip replacement signing"
        );
        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            5,
            "only five automatic recovery broadcasts are allowed"
        );
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .expect("exhausted reservation query should succeed")
                .contains(issuer_request_id),
            "exhaustion must keep the receipt reservation held"
        );
        assert_eq!(
            vault_mock.submitted_burn_txs(),
            vec![
                prepared_tx,
                replacement_tx.clone(),
                replacement_tx.clone(),
                replacement_tx.clone(),
                replacement_tx.clone(),
            ]
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

    #[traced_test]
    #[tokio::test]
    async fn persisted_burn_recovery_revalidates_state_under_wallet_lock() {
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_wallet_lock_blocked()
                .with_burn_tx_status(BurnTxStatus::StillMineable)
                .with_prepared_tx(prepared_tx),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            TEST_WALLET,
            U256::ZERO,
        )
        .await;

        let stale = load_aggregate(store, &issuer_request_id).await;
        let Redemption::BurnIntended {
            metadata,
            planned_burns,
            sendable_tx,
            external_tx_id,
            ..
        } = &stale
        else {
            panic!("expected persisted burn intent");
        };
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );
        let recovery_manager = manager.clone();
        let recovery_id = issuer_request_id.clone();
        let recovery_metadata = metadata.clone();
        let recovery_planned_burns = planned_burns.clone();
        let recovery_sendable_tx = sendable_tx.clone();
        let recovery_external_tx_id = external_tx_id.clone();
        let persisted_owner = sendable_tx.signer_for_test();
        let recovery = tokio::spawn(async move {
            recovery_manager
                .recover_persisted_burn(
                    &recovery_id,
                    &recovery_metadata,
                    &recovery_planned_burns,
                    &recovery_sendable_tx,
                    recovery_external_tx_id,
                    false,
                )
                .await
        });
        vault_mock.wait_for_wallet_lock_attempt().await;

        let replacement_tx = SendableTxWithHash::valid_for_test(
            1,
            vault,
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        vault_mock.set_burn_tx_status(BurnTxStatus::ProvablyDead);
        vault_mock.set_prepared_tx(replacement_tx.clone());
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ReplaceDeadBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    owner: persisted_owner,
                },
            )
            .await
            .expect("concurrent replacement should persist a new transaction");
        let classification_calls_after_replacement =
            vault_mock.burn_classification_call_count();
        vault_mock.release_wallet_lock();
        let outcome = recovery
            .await
            .expect("recovery task should join")
            .expect("stale recovery should be a safe no-op");

        assert_eq!(outcome, RecoveryOutcome::AlreadyAdvanced);
        assert_eq!(
            vault_mock.burn_classification_call_count(),
            classification_calls_after_replacement
        );
        assert!(vault_mock.submitted_burn_txs().is_empty());
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == replacement_tx
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Skipping stale persisted burn recovery", "BurnIntended"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn persisted_burn_confirmation_releases_wallet_lock() {
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            address!("0xcccccccccccccccccccccccccccccccccccccccc"),
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_confirm_pending_blocked()
                .with_burn_tx_status(BurnTxStatus::Mined)
                .with_prepared_tx(prepared_tx),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            TEST_WALLET,
            U256::ZERO,
        )
        .await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );
        let recovery_manager = manager.clone();
        let recovery_id = issuer_request_id.clone();
        let recovery = tokio::spawn(async move {
            recovery_manager.recover_single_burning(&recovery_id).await
        });

        vault_mock.wait_for_burn_confirmation().await;
        assert!(!recovery.is_finished());
        let wallet_guard = tokio::time::timeout(
            Duration::from_millis(50),
            vault_mock.lock_wallet(),
        )
        .await
        .expect("slow confirmation must not retain the wallet lock");
        drop(wallet_guard);
        vault_mock.release_burn_confirmation();

        assert!(recovery.await.expect("recovery task should join").is_err());
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &[
                "Recovering BurnIntended redemption",
                "checking existing transaction"
            ]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn restart_resumes_fifth_reserved_rebroadcast() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            vault,
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::StillMineable)
                .with_prepared_tx(prepared_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            TEST_WALLET,
            U256::ZERO,
        )
        .await;
        record_test_recovery_attempts(
            store,
            &issuer_request_id,
            &prepared_tx,
            BurnRecoveryAction::Rebroadcast,
            MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        )
        .await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        let outcome = manager
            .recover_single_burning(&issuer_request_id)
            .await
            .expect("reserved rebroadcast should resume");

        assert_eq!(outcome, RecoveryOutcome::Executed);
        assert_eq!(vault_mock.submitted_burn_txs(), vec![prepared_tx]);
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnSubmitted { .. }
        ));
        vault_mock.set_burn_tx_status(BurnTxStatus::Mined);
        let confirmation_outcome = manager
            .recover_single_burning(&issuer_request_id)
            .await
            .expect("submitted fifth action should still be confirmed");
        assert_eq!(confirmation_outcome, RecoveryOutcome::ExistingBurnRecorded);
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::Completed { .. }
        ));
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
        .expect("exhaustion count should load");
        assert_eq!(exhaustion_events, 0);
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Resuming reserved burn recovery action", "Rebroadcast"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn restart_submits_fifth_prepared_replacement_without_new_attempt() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            0,
            vault,
            Bytes::from_static(&[0x55, 0x66, 0x77]),
        );
        let replacement_tx = SendableTxWithHash::valid_for_test(
            1,
            vault,
            Bytes::from_static(&[0x55, 0x66, 0x77]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
        )
        .await;
        record_test_recovery_attempts(
            store,
            &issuer_request_id,
            &persisted_tx,
            BurnRecoveryAction::Replace,
            MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        )
        .await;
        vault_mock.set_prepared_tx(replacement_tx.clone());
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ReplaceDeadBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    owner,
                },
            )
            .await
            .expect("replacement intent should persist before simulated crash");
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == replacement_tx
        ));
        assert!(vault_mock.submitted_burn_txs().is_empty());
        vault_mock.set_burn_tx_status(BurnTxStatus::StillMineable);
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );

        let outcome = manager
            .recover_single_burning(&issuer_request_id)
            .await
            .expect("persisted replacement should submit after restart");

        assert_eq!(outcome, RecoveryOutcome::Executed);
        assert_eq!(vault_mock.submitted_burn_txs(), vec![replacement_tx]);
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
        assert_eq!(
            recovery_attempts,
            i64::from(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS)
        );
        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Submitted persisted replacement from reserved recovery action"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn fifth_prepared_replacement_consumed_while_down_is_exhausted() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let persisted_tx = SendableTxWithHash::valid_for_test(
            0,
            vault,
            Bytes::from_static(&[0x11, 0x22, 0x33]),
        );
        let replacement_tx = SendableTxWithHash::valid_for_test(
            1,
            vault,
            Bytes::from_static(&[0x11, 0x22, 0x33]),
        );
        let owner = persisted_tx.signer_for_test();
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(persisted_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            owner,
            U256::ZERO,
        )
        .await;
        record_test_recovery_attempts(
            store,
            &issuer_request_id,
            &persisted_tx,
            BurnRecoveryAction::Replace,
            MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        )
        .await;
        vault_mock.set_prepared_tx(replacement_tx.clone());
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::ReplaceDeadBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    owner,
                },
            )
            .await
            .expect("replacement intent should persist before simulated crash");
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );

        let outcome = manager
            .recover_single_burning(&issuer_request_id)
            .await
            .expect("consumed fifth replacement should exhaust recovery");

        assert_eq!(outcome, RecoveryOutcome::SkippedManualIntervention);
        assert!(vault_mock.submitted_burn_txs().is_empty());
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == replacement_tx
        ));
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
        .expect("exhaustion count should load");
        assert_eq!(exhaustion_events, 1);
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Automatic burn recovery exhausted", "attempts=5"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn fifth_replacement_preparation_failure_persists_exhaustion() {
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        let prepared_tx = SendableTxWithHash::valid_for_test(
            0,
            vault,
            Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]),
        );
        let vault_mock = Arc::new(
            MockVaultService::new_success()
                .with_burn_tx_status(BurnTxStatus::ProvablyDead)
                .with_prepared_tx(prepared_tx.clone()),
        );
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let TestHarness { store, receipt_service, pool, .. } = &harness;
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        persist_test_burn_intent(
            store,
            &issuer_request_id,
            vault,
            TEST_WALLET,
            U256::ZERO,
        )
        .await;
        record_test_recovery_attempts(
            store,
            &issuer_request_id,
            &prepared_tx,
            BurnRecoveryAction::Replace,
            MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS,
        )
        .await;
        vault_mock.reset();
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );

        assert_eq!(
            manager
                .recover_single_burning(&issuer_request_id)
                .await
                .expect("fifth preparation failure should exhaust recovery"),
            RecoveryOutcome::SkippedManualIntervention
        );
        assert_eq!(vault_mock.replacement_preparation_call_count(), 1);
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == prepared_tx
        ));
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
        .expect("exhaustion count should load");
        assert_eq!(exhaustion_events, 1);
        assert!(logs_contain_at!(
            tracing::Level::ERROR,
            &["Automatic burn recovery exhausted", "attempts=5"]
        ));
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
        let harness = TestHarness::with_pool(vault_mock.clone(), pool).await;
        let TestHarness { store, receipt_service, .. } = &harness;

        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

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

        // Seed a reservation so the test verifies it is settled on confirm.
        receipt_service
            .reserve_burn(
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
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: uint!(99_U256),
                        burn_shares: uint!(100_000000000000000000_U256),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares: U256::ZERO,
                    owner: recovery_owner,
                    external_tx_id: None,
                },
            )
            .await
            .expect("IntendBurn should succeed");

        let aggregate = load_aggregate(store, &issuer_request_id).await;
        assert!(
            matches!(aggregate, Redemption::BurnIntended { .. }),
            "Expected BurnIntended with sendable_tx, got {aggregate:?}"
        );
        assert_eq!(
            vault_mock.burn_preparation_call_count(),
            1,
            "the signed transaction must be prepared before the restart"
        );

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
                .build(vault_mock.clone())
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
        let manager = BurnManager::new(
            vault_mock.clone(),
            restarted_pool.clone(),
            restarted_store.clone(),
            restarted_receipt_service.clone(),
            recovery_owner,
        );
        let result = manager.recover_single_burning(&issuer_request_id).await;

        assert!(matches!(result, Ok(RecoveryOutcome::Executed)));

        assert_eq!(
            vault_mock.get_multi_burn_call_count(),
            1,
            "recovery must broadcast the persisted transaction exactly once"
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

        assert!(
            restarted_receipt_service
                .reserved_redemptions(vault)
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
            Ok(RecoveryOutcome::Executed)
        ));
        assert!(
            restarted_receipt_service
                .reserved_redemptions(vault)
                .await
                .expect("restart reservation query should succeed")
                .contains(&issuer_request_id),
            "replacement must keep the persisted receipt reservation"
        );
        let contender = IssuerRedemptionRequestId::random();
        let availability = restarted_receipt_service
            .for_burn(
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
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![],
                    dust_shares: U256::ZERO,
                    owner,
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
        let issuer_request_id_text = issuer_request_id.to_string();
        assert_eq!(
            log_count_at!(
                tracing::Level::ERROR,
                &["Automatic burn recovery exhausted", &issuer_request_id_text,]
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            recovery_owner,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: uint!(99_U256),
                        burn_shares: uint!(100_000000000000000000_U256),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares: U256::ZERO,
                    owner: recovery_owner,
                    external_tx_id: None,
                },
            )
            .await
            .expect("old burn intent should persist");
        vault_mock.set_prepared_tx(replacement_tx.clone());

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::Executed)
        ));
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![],
                    dust_shares: U256::ZERO,
                    owner,
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

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
                'Mint',
                'blocking-mint',
                1,
                'MintEvent::MintTxIntended',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(pool)
        .await
        .expect("blocking mint intent should seed");

        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
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
                'Mint',
                'blocking-mint',
                2,
                'MintEvent::MintTxSubmitted',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(pool)
        .await
        .expect("blocking mint intent should resolve");

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
                'Redemption',
                'blocking-redemption',
                1,
                'RedemptionEvent::BurnIntended',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(pool)
        .await
        .expect("blocking burn intent should seed");
        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::SkippedManualIntervention)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 0);
        assert!(vault_mock.submitted_burn_txs().is_empty());

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
                'Redemption',
                'blocking-redemption',
                2,
                'RedemptionEvent::BurnTxSubmitted',
                '1.0',
                '{}',
                '{}'
            )
            ",
        )
        .execute(pool)
        .await
        .expect("blocking burn intent should resolve");
        vault_mock.set_prepared_tx(replacement_tx.clone());

        assert!(matches!(
            manager.recover_single_burning(&issuer_request_id).await,
            Ok(RecoveryOutcome::Executed)
        ));
        assert_eq!(vault_mock.replacement_preparation_call_count(), 1);
        assert_eq!(vault_mock.submitted_burn_txs(), vec![replacement_tx]);
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        harness.discover_receipt(vault, uint!(42_U256), uint!(17_U256)).await;
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
            .expect("reservation should seed");
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![MultiBurnEntry {
                        receipt_id: uint!(42_U256),
                        burn_shares: uint!(17_U256),
                        receipt_info: None,
                        receipt_info_bytes: None,
                    }],
                    dust_shares: U256::ZERO,
                    owner,
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
                RedemptionError::UnresolvedBurnRequiresAcknowledgement {
                    burn_tx_hash,
                }
            )) if burn_tx_hash == persisted_tx.hash
        ));
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
                .await
                .unwrap()
                .contains(&issuer_request_id)
        );

        let wrong_acknowledgement = B256::random();
        let result = manager
            .force_complete_burn(
                &issuer_request_id,
                other_redemption_hash,
                "wrong acknowledgement".to_string(),
                Some(wrong_acknowledgement),
            )
            .await;
        assert!(matches!(
            result,
            Err(BurnManagerError::Redemption(
                RedemptionError::UnresolvedBurnAcknowledgementMismatch {
                    expected,
                    provided,
                }
            )) if expected == persisted_tx.hash
                && provided == wrong_acknowledgement
        ));
        assert_eq!(vault_mock.verify_burn_call_count(), 0);
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
        let manager = BurnManager::new(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            owner,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault,
                    burns: vec![],
                    dust_shares: U256::ZERO,
                    owner,
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
                    vault,
                    burns: vec![MultiBurnEntry {
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
            .expect("burn intent should persist");

        manager.recover_burning_redemptions().await;

        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { sendable_tx, .. }
                if sendable_tx == persisted_tx
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
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

    #[traced_test]
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
        let manager = BurnManager::new(
            vault_mock,
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
        );
        let issuer_request_id = IssuerRedemptionRequestId::random();
        create_test_redemption_in_burning_state(store, &issuer_request_id)
            .await;
        store
            .send(
                &issuer_request_id,
                RedemptionCommand::IntendBurn {
                    issuer_request_id: issuer_request_id.clone(),
                    vault: address!(
                        "0xcccccccccccccccccccccccccccccccccccccccc"
                    ),
                    burns: vec![],
                    dust_shares: U256::ZERO,
                    owner: TEST_WALLET,
                    external_tx_id: None,
                },
            )
            .await
            .expect("burn intent should persist");

        manager.recover_burning_redemptions().await;
        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::BurnIntended { .. }
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
        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Failed to recover Burning redemption", "Asset not found"]
        ));
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;
        let manager = BurnManager::new(
            vault_mock.clone(),
            pool.clone(),
            store.clone(),
            receipt_service.clone(),
            TEST_WALLET,
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
                    vault,
                    burns: vec![MultiBurnEntry {
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
        manager.recover_unresolved_burns().await;

        assert!(matches!(
            load_aggregate(store, &issuer_request_id).await,
            Redemption::Failed { .. }
        ));
        assert!(
            receipt_service
                .reserved_redemptions(vault)
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
            &["BurnIntended confirmation failed during recovery"]
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let manager = BurnManager::new(
            vault_mock,
            pool.clone(),
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
        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        assert!(
            manager
                .handle_burning_started(&issuer_request_id, &aggregate)
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
            receipt_service.reserved_redemptions(vault).await.unwrap(),
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let manager = BurnManager::new(
            vault_mock,
            pool.clone(),
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
        let aggregate =
            create_test_redemption_in_burning_state(store, &issuer_request_id)
                .await;

        assert!(
            manager
                .handle_burning_started(&issuer_request_id, &aggregate)
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
            receipt_service.reserved_redemptions(vault).await.unwrap(),
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
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn VaultService> = vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
                .reserved_redemptions(vault)
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

        store
            .send(
                &issuer_request_id,
                RedemptionCommand::RecordBurnPreparationRecoveryAttempt {
                    issuer_request_id: issuer_request_id.clone(),
                    attempt: 1,
                },
            )
            .await
            .expect("preparation attempt should survive a simulated restart");
        manager.recover_burn_failed_redemptions().await;
        let reserved_attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type =
                  'RedemptionEvent::BurnPreparationRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("reserved preparation attempt count should load");
        assert_eq!(
            reserved_attempts, 1,
            "restart must resume the reserved attempt without another slot"
        );

        for _ in 0..=MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS {
            manager.recover_burn_failed_redemptions().await;
        }

        assert_eq!(
            vault_mock.burn_preparation_call_count(),
            usize::try_from(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS).unwrap() + 1,
            "the live attempt plus five recovery attempts may prepare"
        );
        let preparation_attempts: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type =
                  'RedemptionEvent::BurnPreparationRecoveryAttempted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("preparation attempt count should load");
        let preparation_exhausted: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
              AND event_type =
                  'RedemptionEvent::BurnPreparationRecoveryExhausted'
            ",
        )
        .bind(issuer_request_id.to_string())
        .fetch_one(pool)
        .await
        .expect("preparation exhaustion count should load");
        assert_eq!(
            preparation_attempts,
            i64::from(MAX_AUTOMATIC_BURN_RECOVERY_ATTEMPTS)
        );
        assert_eq!(preparation_exhausted, 1);
        assert_eq!(
            log_count_at!(
                tracing::Level::ERROR,
                &["Automatic burn preparation recovery exhausted"]
            ),
            1,
            "preparation exhaustion must emit one actionable error"
        );
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
        let underlying = UnderlyingSymbol::new("AAPL");
        harness.add_asset(&underlying, vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            pool.clone(),
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
            .expect("IntendBurn should succeed");

        store
            .send(
                &issuer_request_id,
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
        // is BurnSubmitted) and confirm the existing transaction
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
    async fn test_recover_stuck_reservations_leaves_ambiguous_submitted() {
        let vault_mock = Arc::new(MockVaultService::new_failure());
        let harness = TestHarness::with_vault_mock(vault_mock.clone()).await;
        let vault = address!("0xcccccccccccccccccccccccccccccccccccccccc");
        harness.add_asset(&UnderlyingSymbol::new("AAPL"), vault).await;

        let blockchain_service: Arc<dyn crate::vault::VaultService> =
            vault_mock.clone();
        let manager = BurnManager::new(
            blockchain_service,
            harness.pool.clone(),
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
