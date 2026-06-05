use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::Utc;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::{
    AutomaticRetryDecision, IssuerMintRequestId, Mint, MintCommand, MintError,
    MintRecoveryMode,
};
use crate::receipt_inventory::ItnReceiptHandler;

/// Production handler that triggers mint recovery when an ITN receipt is
/// discovered by the receipt monitor.
#[derive(Clone)]
pub(crate) struct MintRecoveryHandler {
    mint_store: Arc<Store<Mint>>,
}

impl MintRecoveryHandler {
    pub(crate) const fn new(mint_store: Arc<Store<Mint>>) -> Self {
        Self { mint_store }
    }
}

#[async_trait]
impl ItnReceiptHandler for MintRecoveryHandler {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    ) {
        let mint_store = self.mint_store.clone();
        tokio::spawn(async move {
            drive_recovery(&mint_store, issuer_request_id, |id| {
                MintCommand::RecoverFromReceipt {
                    issuer_request_id: id,
                    tx_hash,
                }
            })
            .await;
        });
    }
}

/// Why a [`drive_recovery`] pass stopped. Lets the scheduled recovery loop
/// decide whether to wait, back off, or give up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DriveOutcome {
    /// Reached a terminal or non-recoverable state — no further work.
    Done,
    /// Paused until the next automatic retry window elapses.
    RetryNotDue,
    /// The previously submitted Fireblocks transaction is still pending.
    Pending,
    /// Automatic retry budget is exhausted.
    Exhausted,
    /// A command failed unexpectedly, or recovery did not converge.
    Failed,
}

/// Drives a mint through recovery to completion using `MintCommand::Recover`.
pub(crate) async fn recover_mint(
    mint_store: &Store<Mint>,
    issuer_request_id: IssuerMintRequestId,
) -> DriveOutcome {
    drive_recovery(mint_store, issuer_request_id, |id| MintCommand::Recover {
        issuer_request_id: id,
        mode: MintRecoveryMode::Automatic,
    })
    .await
}

/// Fixed backoff applied when a scheduled recovery pass cannot make progress —
/// the previously submitted Fireblocks tx is still pending, or a transient
/// error (e.g. RPC blip) occurred. Keeps the loop from spinning while waiting.
const SCHEDULED_RECOVERY_BACKOFF: Duration = Duration::from_secs(60);

/// Budget for retry-window wakeups (`Wait` / `RetryNotDue`). The automatic
/// schedule already terminates healthy retries via `Exhausted` after the
/// attempt cap; this bounds the degenerate case where a mint keeps re-failing
/// at the same attempt (e.g. submission errors before Fireblocks acceptance),
/// so the task gives up and the next restart re-picks it instead of looping.
const MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS: usize =
    (Mint::MAX_AUTOMATIC_MINT_RETRY_ATTEMPT as usize) * 2 + 4;

/// Budget for consecutive transient-failure backoffs (e.g. RPC blips) before
/// giving up. Small: a persistent error should surface for investigation, not
/// be hammered indefinitely. The next restart re-picks the mint.
const MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS: usize = 8;

/// Budget for polling a still-pending Fireblocks tx. A pending tx is a healthy
/// state (queued, awaiting policy approval, broadcasting, confirming) that we do
/// not control, so this is sized generously — ~6h at [`SCHEDULED_RECOVERY_BACKOFF`]
/// — separate from the transient-failure budget, so a slow finalization is not
/// abandoned prematurely.
const MAX_SCHEDULED_RECOVERY_PENDING_POLLS: usize = 360;

pub(crate) fn spawn_scheduled_mint_recovery(
    mint_store: Arc<Store<Mint>>,
    issuer_request_id: IssuerMintRequestId,
) {
    tokio::spawn(async move {
        recover_mint_until_automatic_budget_exhausted(
            mint_store.as_ref(),
            issuer_request_id,
            SCHEDULED_RECOVERY_BACKOFF,
            MAX_SCHEDULED_RECOVERY_PENDING_POLLS,
        )
        .await;
    });
}

async fn recover_mint_until_automatic_budget_exhausted(
    mint_store: &Store<Mint>,
    issuer_request_id: IssuerMintRequestId,
    backoff: Duration,
    max_pending_polls: usize,
) {
    let mut retry_wakeups = 0;
    let mut failure_backoffs = 0;
    let mut pending_polls = 0;
    let mut polled_tx_id: Option<String> = None;

    loop {
        let mint = match mint_store.load(&issuer_request_id).await {
            Ok(Some(mint)) => mint,
            // A missing mint cannot be recovered — the same outcome the old
            // default (`Uninitialized`) aggregate produced via
            // `AutomaticRetryDecision::NotRecoverable`.
            Ok(None) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    "Mint not found for scheduled recovery"
                );
                return;
            }
            Err(err) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Failed to load mint for scheduled recovery"
                );
                return;
            }
        };

        // Give each distinct Fireblocks transaction its own pending-poll
        // budget: when recovery advances to a new submitted tx, reset the
        // counter so a healthy retry is not abandoned because a prior tx
        // already spent the budget.
        let current_tx_id = mint.pending_fireblocks_tx_id();
        if current_tx_id != polled_tx_id {
            pending_polls = 0;
            polled_tx_id = current_tx_id;
        }

        match mint.automatic_retry_decision(Utc::now()) {
            AutomaticRetryDecision::Ready => {
                match recover_mint(mint_store, issuer_request_id.clone()).await
                {
                    DriveOutcome::Done | DriveOutcome::Exhausted => return,
                    // A still-pending Fireblocks tx is healthy; poll it on a
                    // generous budget separate from transient failures.
                    DriveOutcome::Pending => {
                        pending_polls += 1;
                        if pending_polls > max_pending_polls {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_pending_polls,
                                "Scheduled mint recovery stopped after maximum pending polls"
                            );
                            return;
                        }

                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            backoff_ms = backoff.as_millis(),
                            "Scheduled recovery waiting for pending Fireblocks transaction"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    // A transient error: back off and retry a bounded number of
                    // times so a persistent error surfaces rather than looping.
                    DriveOutcome::Failed => {
                        failure_backoffs += 1;
                        if failure_backoffs
                            > MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS
                        {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_failure_backoffs = MAX_SCHEDULED_RECOVERY_FAILURE_BACKOFFS,
                                "Scheduled mint recovery stopped after maximum failure backoffs"
                            );
                            return;
                        }

                        debug!(target: "mint", issuer_request_id = %issuer_request_id,
                            backoff_ms = backoff.as_millis(),
                            "Scheduled recovery backing off after a transient failure"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    // The retry window passed between the decision and the
                    // submit-time re-check; sleep so a clock race does not spin
                    // the wakeup budget, then re-evaluate (next decision Waits).
                    DriveOutcome::RetryNotDue => {
                        retry_wakeups += 1;
                        if retry_wakeups > MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS
                        {
                            warn!(target: "mint", issuer_request_id = %issuer_request_id,
                                max_retry_wakeups = MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS,
                                "Scheduled mint recovery stopped after maximum retry wakeups"
                            );
                            return;
                        }

                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            AutomaticRetryDecision::Wait(wait) => {
                retry_wakeups += 1;
                if retry_wakeups > MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS {
                    warn!(target: "mint", issuer_request_id = %issuer_request_id,
                        max_retry_wakeups = MAX_SCHEDULED_RECOVERY_RETRY_WAKEUPS,
                        "Scheduled mint recovery stopped after maximum retry wakeups"
                    );
                    return;
                }

                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    wait_ms = wait.as_millis(),
                    "Waiting for next automatic mint retry window"
                );
                tokio::time::sleep(wait).await;
            }
            AutomaticRetryDecision::Exhausted
            | AutomaticRetryDecision::NotRecoverable => return,
        }
    }
}

const MAX_RECOVERY_ATTEMPTS: usize = 10;

/// Drives a mint through recovery to completion by repeatedly sending
/// commands built by `make_command` until the mint reaches a terminal state.
///
/// A single recovery command advances the mint by one step (e.g.,
/// `MintingFailed` -> `CallbackPending`). This function loops until
/// `NotRecoverable` is returned, which means the mint has either
/// completed or reached a state that cannot be recovered from.
///
/// Bounded to [`MAX_RECOVERY_ATTEMPTS`] iterations to prevent infinite
/// spinning if a command returns `Ok(())` without advancing state.
async fn drive_recovery(
    mint_store: &Store<Mint>,
    issuer_request_id: IssuerMintRequestId,
    make_command: impl Fn(IssuerMintRequestId) -> MintCommand,
) -> DriveOutcome {
    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        let result = mint_store
            .send(&issuer_request_id, make_command(issuer_request_id.clone()))
            .await;

        match result {
            Ok(()) => {
                debug!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempt,
                    "Recovery step succeeded, continuing"
                );
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::NotRecoverable { current_state },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    current_state,
                    "Mint recovery complete"
                );
                return DriveOutcome::Done;
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::RetryNotDue { retry_at },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    %retry_at,
                    "Mint recovery paused until retry window"
                );
                return DriveOutcome::RetryNotDue;
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::AutomaticRetriesExhausted { attempts },
            ))) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    attempts,
                    "Automatic mint retries exhausted"
                );
                return DriveOutcome::Exhausted;
            }
            Err(AggregateError::UserError(LifecycleError::Apply(
                MintError::FireblocksTxStillPending { fireblocks_tx_id },
            ))) => {
                info!(target: "mint", issuer_request_id = %issuer_request_id,
                    fireblocks_tx_id,
                    "Mint recovery paused while Fireblocks transaction is pending"
                );
                return DriveOutcome::Pending;
            }
            Err(err) => {
                warn!(target: "mint", issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Mint recovery failed"
                );
                return DriveOutcome::Failed;
            }
        }
    }

    error!(target: "mint", issuer_request_id = %issuer_request_id,
        aggregate_id = %issuer_request_id,
        max_attempts = MAX_RECOVERY_ATTEMPTS,
        "Mint recovery exceeded maximum attempts without reaching terminal state"
    );

    DriveOutcome::Failed
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address, b256, uint};
    use chrono::Utc;
    use event_sorcery::{StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::tests::{BOT, VAULT};
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, MintServices, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptSource, Shares,
    };
    use crate::test_utils::log_count_at;
    use crate::tokenized_asset::{TokenizedAsset, TokenizedAssetCommand};
    use crate::vault::mock::MockVaultService;
    use crate::vault::{
        BurnVerification, FireblocksTxStatus, MintResult, MultiBurnParams,
        MultiBurnResult, ReceiptInformation, SubmittedTx, VaultError,
        VaultService,
    };

    /// Builds a real event-sorcery [`Store<Mint>`] backed by an in-memory
    /// SQLite pool, wired with the same services the production recovery flow
    /// uses (vault, Alpaca, receipt inventory). The [`TokenizedAsset`]
    /// projection is populated with the AAPL -> [`VAULT`] mapping so the
    /// recovery vault lookup (`find_vault_by_underlying`) resolves.
    struct MintRecoveryFixture {
        mint_store: Arc<Store<Mint>>,
        receipt_store: Arc<Store<ReceiptInventory>>,
        pool: sqlx::SqlitePool,
    }

    impl MintRecoveryFixture {
        async fn new() -> Self {
            Self::new_with_vault(Arc::new(MockVaultService::new_success()))
                .await
        }

        async fn new_with_vault(vault: Arc<dyn VaultService>) -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .unwrap();

            sqlx::migrate!("./migrations").run(&pool).await.unwrap();

            let (asset_store, _asset_projection) =
                StoreBuilder::<TokenizedAsset>::new(pool.clone())
                    .build(())
                    .await
                    .unwrap();

            asset_store
                .send(
                    &UnderlyingSymbol::new("AAPL"),
                    TokenizedAssetCommand::Add {
                        underlying: UnderlyingSymbol::new("AAPL"),
                        token: TokenSymbol::new("tAAPL"),
                        network: Network::new("base"),
                        vault: VAULT,
                    },
                )
                .await
                .unwrap();

            let receipt_store =
                Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

            let services = MintServices {
                vault,
                alpaca: Arc::new(MockAlpacaService::new_success()),
                receipts: Arc::new(CqrsReceiptService::new(
                    receipt_store.clone(),
                )),
                pool: pool.clone(),
                bot: BOT,
            };

            let mint_store =
                Arc::new(test_store::<Mint>(pool.clone(), services));

            Self { mint_store, receipt_store, pool }
        }

        /// Seeds the event store with raw `Mint` events, putting the aggregate
        /// directly into the desired lifecycle state. Mirrors the e2e
        /// setup-phase pattern of writing rows to the `events` table; the
        /// running service then reacts to them during the scenario.
        async fn seed_mint_events(
            &self,
            issuer_request_id: &IssuerMintRequestId,
            events: Vec<MintEvent>,
        ) {
            let aggregate_id = issuer_request_id.to_string();

            for (offset, event) in events.into_iter().enumerate() {
                let sequence = i64::try_from(offset).unwrap() + 1;
                let payload = serde_json::to_value(&event).unwrap();
                let variant = payload
                    .as_object()
                    .and_then(|map| map.keys().next())
                    .expect("MintEvent serializes as an externally-tagged enum")
                    .clone();
                let event_type = format!("MintEvent::{variant}");
                let payload_str = payload.to_string();

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
                    VALUES ('Mint', ?, ?, ?, '1.0', ?, '{}')
                    ",
                )
                .bind(&aggregate_id)
                .bind(sequence)
                .bind(&event_type)
                .bind(&payload_str)
                .execute(&self.pool)
                .await
                .unwrap();
            }
        }
    }

    /// Vault whose Fireblocks transaction never finalizes — every status check
    /// returns `Pending`. Used to exercise the scheduled-recovery backoff.
    struct PendingMintVault;

    #[async_trait]
    impl VaultService for PendingMintVault {
        async fn submit_mint(
            &self,
            _vault: Address,
            _assets: U256,
            _bot: Address,
            _user: Address,
            _receipt_info: ReceiptInformation,
            _external_tx_id: Option<String>,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_mint(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<MintResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn get_share_balance(
            &self,
            _vault: Address,
            _owner: Address,
        ) -> Result<U256, VaultError> {
            Ok(U256::ZERO)
        }

        async fn check_fireblocks_tx(
            &self,
            _fireblocks_tx_id: &str,
        ) -> Result<Option<FireblocksTxStatus>, VaultError> {
            Ok(Some(FireblocksTxStatus::Pending))
        }

        async fn submit_burn(
            &self,
            _params: MultiBurnParams,
        ) -> Result<SubmittedTx, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn confirm_burn(
            &self,
            _fireblocks_tx_id: &str,
            _expected_dust_shares: U256,
        ) -> Result<MultiBurnResult, VaultError> {
            Err(VaultError::InvalidReceipt)
        }

        async fn verify_burn_tx(
            &self,
            _vault: Address,
            _owner: Address,
            _tx_hash: B256,
        ) -> Result<BurnVerification, VaultError> {
            Err(VaultError::InvalidReceipt)
        }
    }

    fn fireblocks_failed_events(
        issuer_request_id: &IssuerMintRequestId,
        failed_at: chrono::DateTime<Utc>,
    ) -> Vec<MintEvent> {
        let mut events = fireblocks_submitted_events(issuer_request_id);
        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "terminal Fireblocks failure".to_string(),
            failed_at,
        });

        events
    }

    fn test_issuer_request_id() -> IssuerMintRequestId {
        IssuerMintRequestId::new(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
                .unwrap(),
        )
    }

    fn minting_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();

        vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
        ]
    }

    fn fireblocks_submitted_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::FireblocksSubmitted {
            issuer_request_id: issuer_request_id.clone(),
            external_tx_id: format!("mint-{issuer_request_id}"),
            fireblocks_tx_id: "fb-tx-123".to_string(),
            submitted_at: now,
        });

        events
    }

    fn minting_failed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_events(issuer_request_id);

        events.push(MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "timeout".to_string(),
            failed_at: now,
        });

        events
    }

    fn callback_pending_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_failed_events(issuer_request_id);

        events.push(MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 1000,
            recovered_at: now,
        });

        events
    }

    fn completed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = callback_pending_events(issuer_request_id);

        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: now,
        });

        events
    }

    async fn setup_with_receipt_and_events(
        events: Vec<MintEvent>,
    ) -> MintRecoveryFixture {
        let issuer_request_id = test_issuer_request_id();
        let fixture = MintRecoveryFixture::new().await;

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        );

        fixture
            .receipt_store
            .send(
                &VAULT,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(uint!(42_U256)),
                    balance: Shares::from(
                        uint!(100_000000000000000000_U256),
                    ),
                    block_number: 1000,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::Itn {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                    receipt_info: Some(Box::new(receipt_info)),
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();

        fixture.seed_mint_events(&issuer_request_id, events).await;

        fixture
    }

    #[traced_test]
    #[tokio::test]
    async fn minting_failed_with_receipt_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
            .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );
        let test = "minting_failed_with_receipt_recovers_to_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        // A single Recover command advances MintingFailed → CallbackPending
        // → Completed atomically via advance_through_callback, so the
        // drive_recovery loop only needs one successful step.
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn callback_pending_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = callback_pending_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
            .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );
        let test = "callback_pending_recovers_to_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
        assert_eq!(
            log_count_at!(Level::DEBUG, &[test, "Recovery step succeeded"]),
            1,
        );
    }

    /// A single `Recover` command on a mint stuck in `Minting` must reach
    /// `Completed` in one execution when an on-chain receipt is already
    /// present — the deposit event and the callback must be committed
    /// together so the aggregate never lingers in `CallbackPending`.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_minting_reaches_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test = "single_recover_command_from_minting_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Same invariant for the `MintingFailed` starting state.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_minting_failed_reaches_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test =
            "single_recover_command_from_minting_failed_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    /// Same invariant for the `FireblocksSubmitted` starting state — the
    /// mock vault's `confirm_mint` succeeds, so `TokensMinted` and
    /// `MintCompleted` must be emitted in one command.
    #[traced_test]
    #[tokio::test]
    async fn single_recover_command_from_fireblocks_submitted_reaches_completed()
     {
        let issuer_request_id = test_issuer_request_id();
        let events = fireblocks_submitted_events(&issuer_request_id);
        // No pre-existing receipt — the mock confirm path produces TokensMinted.
        let fixture = MintRecoveryFixture::new().await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        fixture
            .mint_store
            .send(
                &issuer_request_id,
                MintCommand::Recover {
                    issuer_request_id: issuer_request_id.clone(),
                    mode: MintRecoveryMode::Automatic,
                },
            )
            .await
            .unwrap();

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state after one Recover, got: {}",
            mint.state_name()
        );
        let test = "single_recover_command_from_fireblocks_submitted_reaches_completed";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Alpaca callback succeeded"]),
            1,
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn completed_mint_returns_cleanly() {
        let issuer_request_id = test_issuer_request_id();
        let events = completed_events(&issuer_request_id);
        let fixture = setup_with_receipt_and_events(events).await;

        recover_mint(fixture.mint_store.as_ref(), issuer_request_id.clone())
            .await;

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();

        assert!(
            matches!(mint, Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            mint.state_name()
        );

        let test = "completed_mint_returns_cleanly";
        assert_eq!(
            log_count_at!(Level::INFO, &[test, "Mint recovery complete"]),
            1,
        );
    }

    #[tokio::test]
    async fn recover_mint_returns_pending_while_fireblocks_tx_pending() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = fireblocks_failed_events(&issuer_request_id, failed_at);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome =
            recover_mint(fixture.mint_store.as_ref(), issuer_request_id).await;

        assert_eq!(outcome, DriveOutcome::Pending);
    }

    /// A pending Fireblocks transaction must make the scheduled loop back off
    /// between wakeups rather than spinning through its budget instantly. The
    /// loop sleeps the backoff on every pending wakeup, so total elapsed time
    /// must be at least one backoff per wakeup — a spinning loop would finish
    /// in microseconds.
    #[traced_test]
    #[tokio::test]
    async fn scheduled_recovery_backs_off_while_fireblocks_tx_pending() {
        let issuer_request_id = test_issuer_request_id();
        let failed_at = Utc::now() - chrono::Duration::minutes(2);
        let events = fireblocks_failed_events(&issuer_request_id, failed_at);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let backoff = Duration::from_millis(5);
        let max_pending_polls = 8;
        let start = tokio::time::Instant::now();
        recover_mint_until_automatic_budget_exhausted(
            fixture.mint_store.as_ref(),
            issuer_request_id.clone(),
            backoff,
            max_pending_polls,
        )
        .await;
        let elapsed = start.elapsed();

        assert!(
            elapsed
                >= backoff * (u32::try_from(max_pending_polls).unwrap() - 1),
            "Scheduled recovery must sleep the backoff on each pending \
             poll, elapsed={elapsed:?}"
        );

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::MintingFailed { .. }),
            "Mint stays MintingFailed while the tx remains pending, got: {}",
            mint.state_name()
        );

        let test = "scheduled_recovery_backs_off_while_fireblocks_tx_pending";
        assert!(
            log_count_at!(
                Level::WARN,
                &[test, "stopped after maximum pending polls"]
            ) >= 1,
            "Loop must stop with a warning once the pending-poll budget is spent"
        );
    }

    /// A mint already in `FireblocksSubmitted` whose tx is still pending must
    /// pause recovery (DriveOutcome::Pending) via the non-blocking pre-check
    /// rather than blocking in confirm_mint or flipping to MintingFailed.
    #[tokio::test]
    async fn fireblocks_submitted_pending_tx_pauses_recovery() {
        let issuer_request_id = test_issuer_request_id();
        let events = fireblocks_submitted_events(&issuer_request_id);
        let fixture =
            MintRecoveryFixture::new_with_vault(Arc::new(PendingMintVault))
                .await;
        fixture.seed_mint_events(&issuer_request_id, events).await;

        let outcome = recover_mint(
            fixture.mint_store.as_ref(),
            issuer_request_id.clone(),
        )
        .await;

        assert_eq!(outcome, DriveOutcome::Pending);

        let mint =
            fixture.mint_store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(mint, Mint::FireblocksSubmitted { .. }),
            "A pending tx must leave the mint in FireblocksSubmitted, got: {}",
            mint.state_name()
        );
    }
}
