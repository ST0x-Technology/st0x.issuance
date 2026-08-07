use alloy::primitives::Address;
use alloy::sol_types::SolEvent;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    IssuerRedemptionRequestId, Redemption, RedemptionCommand,
    burn_manager::BurnManager, journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
};
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::bindings;
use crate::burn_excess::exclusion::is_excluded_funding_log;
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAssetView, UnderlyingSymbol,
};
use crate::{Quantity, QuantityConversionError};

#[derive(Debug)]
pub(crate) enum TransferOutcome {
    Detected {
        issuer_request_id: IssuerRedemptionRequestId,
        client_id: ClientId,
        alpaca_account: AlpacaAccountNumber,
    },
    AlreadyDetected,
    SkippedMint,
    SkippedNoAccount,
    /// Path B burn-excess funding Transfer — not a real AP redemption.
    SkippedAdminRecovery,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferProcessingError {
    #[error("Failed to decode Transfer event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error(
        "Missing log index in log (required for funding-exclusion identity)"
    )]
    MissingLogIndex,
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("CQRS error: {0}")]
    Aggregate(Box<AggregateError<LifecycleError<Redemption>>>),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("No asset found for vault {vault}")]
    NoMatchingAsset { vault: Address },
    #[error(
        "Multiple enabled assets are bound to vault {vault}; refusing to \
         attribute its redemptions to an arbitrary underlying"
    )]
    AmbiguousVault { vault: Address },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

// `AggregateError<LifecycleError<Redemption>>` is large (it can carry a full
// Redemption aggregate), so it's boxed to keep `TransferProcessingError` small.
impl From<AggregateError<LifecycleError<Redemption>>>
    for TransferProcessingError
{
    fn from(error: AggregateError<LifecycleError<Redemption>>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

impl TransferProcessingError {
    /// Returns `true` for errors that will never succeed on retry (decode
    /// failures, missing log fields, no matching asset). These should be
    /// skipped rather than retried to avoid freezing the checkpoint.
    pub(crate) const fn is_non_transient(&self) -> bool {
        matches!(
            self,
            Self::SolTypes(_)
                | Self::MissingTxHash
                | Self::MissingBlockNumber
                | Self::MissingLogIndex
                | Self::QuantityConversion(_)
                | Self::NoMatchingAsset { .. }
        )
    }
}

/// Decodes a Transfer log, looks up the account view and the asset (from the
/// caller's per-pass snapshot, so attribution is consistent with the vault set
/// the pass was built from), and executes `RedemptionCommand::Detect`.
/// Idempotent — returns `AlreadyDetected` on duplicate.
pub(crate) async fn detect_transfer(
    log: &alloy::rpc::types::Log,
    vault: Address,
    network: Network,
    assets: &[TokenizedAssetView],
    store: &Store<Redemption>,
    pool: &Pool<Sqlite>,
) -> Result<TransferOutcome, TransferProcessingError> {
    let transfer_event =
        bindings::OffchainAssetReceiptVault::Transfer::decode_log(&log.inner)?;

    if transfer_event.from == Address::ZERO {
        debug!(target: "redemption", to = %transfer_event.to,
            value = %transfer_event.value,
            "Skipping mint event (from=0x0)"
        );
        return Ok(TransferOutcome::SkippedMint);
    }

    let tx_hash =
        log.transaction_hash.ok_or(TransferProcessingError::MissingTxHash)?;

    let block_number =
        log.block_number.ok_or(TransferProcessingError::MissingBlockNumber)?;

    // Fail closed: exclusion identity is (network, vault, tx_hash, log_index).
    // Without log_index we cannot prove the transfer is not an excluded Path B
    // funding log, so refuse Detect rather than open a Redemption by accident.
    let log_index =
        log.log_index.ok_or(TransferProcessingError::MissingLogIndex)?;

    if is_excluded_funding_log(pool, network, vault, tx_hash, log_index).await?
    {
        debug!(
            target: "redemption",
            %tx_hash,
            log_index,
            %vault,
            "Skipping admin recovery funding transfer"
        );
        return Ok(TransferOutcome::SkippedAdminRecovery);
    }

    let account_view = find_by_wallet(pool, &transfer_event.from).await?;

    let Some(AccountView::LinkedToAlpaca { client_id, alpaca_account, .. }) =
        account_view
    else {
        debug!(target: "redemption", from = %transfer_event.from,
            tx_hash = %tx_hash,
            "Skipping transfer from unknown/unlinked wallet"
        );
        return Ok(TransferOutcome::SkippedNoAccount);
    };

    let (underlying, token, network) =
        find_matching_asset(assets, vault, network)?;

    let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
    let quantity = Quantity::from_u256_with_18_decimals(transfer_event.value)?;

    let command = RedemptionCommand::Detect {
        issuer_request_id: issuer_request_id.clone(),
        underlying,
        token,
        network,
        wallet: transfer_event.from,
        quantity,
        tx_hash,
        block_number,
    };

    match store.send(&issuer_request_id, command).await {
        Ok(()) => {}
        Err(AggregateError::UserError(LifecycleError::Apply(_))) => {
            debug!(target: "redemption", %issuer_request_id,
                "Transfer already detected"
            );
            return Ok(TransferOutcome::AlreadyDetected);
        }
        Err(err) => return Err(err.into()),
    }

    info!(target: "redemption", %issuer_request_id,
        from = %transfer_event.from,
        "Redemption transfer detected"
    );

    Ok(TransferOutcome::Detected {
        issuer_request_id,
        client_id,
        alpaca_account,
    })
}

/// Dependencies for driving the post-detection redemption flow.
pub(crate) struct RedemptionFlowCtx {
    pub(crate) store: Arc<Store<Redemption>>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager>,
    pub(crate) journal_manager: Arc<JournalManager>,
    pub(crate) burn_manager: Arc<BurnManager>,
}

/// Drives the post-detection redemption flow: Alpaca call, journal polling,
/// and burn. Errors are logged but do not propagate — the detection is already
/// recorded.
pub(crate) async fn drive_redemption_flow(
    issuer_request_id: IssuerRedemptionRequestId,
    client_id: ClientId,
    alpaca_account: AlpacaAccountNumber,
    deps: RedemptionFlowCtx,
) {
    let redemption = match deps.store.load(&issuer_request_id).await {
        Ok(Some(redemption)) => redemption,
        Ok(None) => {
            warn!(target: "redemption", %issuer_request_id,
                "Redemption not found after detection"
            );
            return;
        }
        Err(err) => {
            warn!(target: "redemption", %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after detection"
            );
            return;
        }
    };

    if let Err(err) = deps
        .redeem_call_manager
        .handle_redemption_detected(
            &alpaca_account,
            &issuer_request_id,
            &redemption,
            client_id,
        )
        .await
    {
        warn!(target: "redemption", %issuer_request_id,
            error = ?err,
            "handle_redemption_detected failed"
        );
        return;
    }

    let redemption = match deps.store.load(&issuer_request_id).await {
        Ok(Some(redemption)) => redemption,
        Ok(None) => {
            warn!(target: "redemption", %issuer_request_id,
                "Redemption not found after redeem call"
            );
            return;
        }
        Err(err) => {
            warn!(target: "redemption", %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after redeem call"
            );
            return;
        }
    };

    let Redemption::AlpacaCalled { tokenization_request_id, .. } = &redemption
    else {
        debug!(target: "redemption", %issuer_request_id,
            aggregate_state = ?redemption,
            "Aggregate not in AlpacaCalled state after redeem call"
        );
        return;
    };

    let tokenization_request_id = tokenization_request_id.clone();

    // Awaited inline rather than spawned: this function already runs inside
    // the spawned task that `watch_redemption_flow` observes, and a detached
    // inner task would put the journal-polling/burn continuation outside that
    // watcher — a panic there would vanish silently, exactly what the watcher
    // exists to prevent.
    if let Err(err) = deps
        .journal_manager
        .handle_alpaca_called(
            &alpaca_account,
            issuer_request_id.clone(),
            tokenization_request_id,
        )
        .await
    {
        warn!(target: "redemption", %issuer_request_id,
            error = ?err,
            "handle_alpaca_called (journal polling) failed"
        );
        return;
    }

    let redemption = match deps.store.load(&issuer_request_id).await {
        Ok(Some(redemption)) => redemption,
        Ok(None) => {
            warn!(target: "redemption", %issuer_request_id,
                "Redemption not found after journal completion"
            );
            return;
        }
        Err(err) => {
            warn!(target: "redemption", %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after journal completion"
            );
            return;
        }
    };

    if matches!(redemption, Redemption::Burning { .. }) {
        if let Err(err) = deps
            .burn_manager
            .handle_burning_started(&issuer_request_id, &redemption)
            .await
        {
            warn!(target: "redemption", %issuer_request_id,
                error = ?err,
                "handle_burning_started failed"
            );
        }
    } else {
        debug!(target: "redemption", %issuer_request_id,
            aggregate_state = ?redemption,
            "Aggregate not in Burning state after journal completion"
        );
    }
}

/// Attributes a vault to its enabled asset from the caller's per-pass asset
/// snapshot.
fn find_matching_asset(
    assets: &[TokenizedAssetView],
    vault: Address,
    network: Network,
) -> Result<(UnderlyingSymbol, TokenSymbol, Network), TransferProcessingError> {
    let mut matching = assets
        .iter()
        .filter(|asset| asset.vault == vault && asset.network == network);

    let first = matching
        .next()
        .ok_or(TransferProcessingError::NoMatchingAsset { vault })?;

    // Two enabled assets bound to the same vault is a misconfiguration: neither
    // this lookup nor the per-vault checkpoint can disambiguate them, so picking
    // an arbitrary one would silently journal the redemption against the wrong
    // underlying at Alpaca. Fail loudly instead — `AmbiguousVault` is transient
    // (deliberately not in `is_non_transient`), so the vault's checkpoint
    // freezes and the failure recurs until an operator removes the duplicate,
    // rather than dropping or misrouting the redemption.
    if matching.next().is_some() {
        warn!(
            target: "redemption",
            %vault,
            "Two enabled assets share this vault; refusing to attribute the \
             redemption to an arbitrary underlying"
        );
        return Err(TransferProcessingError::AmbiguousVault { vault });
    }

    let TokenizedAssetView { underlying, token, network, .. } = first.clone();
    Ok((underlying, token, network))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address, b256};
    use event_sorcery::{Store, StoreBuilder, test_store};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{TransferOutcome, TransferProcessingError, detect_transfer};
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::redemption::Redemption;
    use crate::redemption::RedemptionServices;
    use crate::redemption::test_utils::{
        create_transfer_log, create_transfer_log_with_index,
        setup_test_db_with_asset,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::view::list_enabled_assets;
    use crate::tokenized_asset::{
        AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
        UnderlyingSymbol,
    };
    use crate::underlying::{Underlying, UnderlyingCommand};
    use crate::vault::mock::MockVaultService;

    fn setup_test_store(pool: &SqlitePool) -> Arc<Store<Redemption>> {
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());

        Arc::new(test_store::<Redemption>(
            pool.clone(),
            RedemptionServices::with_single_vault(Network::Base, vault_service),
        ))
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_success() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;
        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault, ap_wallet, bot_wallet, value, tx_hash, 12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        let outcome = result.expect("Expected success");
        assert!(
            matches!(outcome, TransferOutcome::Detected { .. }),
            "Expected Detected outcome"
        );

        if let TransferOutcome::Detected { issuer_request_id, .. } = outcome {
            let redemption =
                store.load(&issuer_request_id).await.unwrap().unwrap();
            assert!(
                matches!(redemption, Redemption::Detected { .. }),
                "Expected Detected state, got {redemption:?}"
            );
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Redemption transfer detected"]
        ));
    }

    // Freezing an asset gates new mints but must NOT stop redemption detection:
    // in-flight redemptions of a frozen asset still need to detect and complete.
    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_succeeds_for_frozen_asset() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;

        let underlying = UnderlyingSymbol::new("AAPL").unwrap();
        let (underlying_store, _projection) =
            StoreBuilder::<Underlying>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        underlying_store
            .send(
                &underlying,
                UnderlyingCommand::Freeze { underlying: underlying.clone() },
            )
            .await
            .expect("Failed to freeze underlying");

        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault, ap_wallet, bot_wallet, value, tx_hash, 12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        let outcome = result.expect("Expected success");
        assert!(
            matches!(outcome, TransferOutcome::Detected { .. }),
            "frozen asset must still detect redemptions, got {outcome:?}"
        );

        if let TransferOutcome::Detected { issuer_request_id, .. } = outcome {
            let redemption =
                store.load(&issuer_request_id).await.unwrap().unwrap();
            assert!(
                matches!(redemption, Redemption::Detected { .. }),
                "frozen asset redemption must persist Detected state, got {redemption:?}"
            );
        }

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Redemption transfer detected"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_skips_excluded_funding_log_only() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");
        let other_wallet =
            address!("0x8888888888888888888888888888888888888888");

        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;
        // Adjacent same-wallet transfer still redeems: also whitelist other.
        {
            use crate::account::{
                Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
            };
            use event_sorcery::StoreBuilder;

            let (account_store, _) = StoreBuilder::<Account>::new(pool.clone())
                .build(())
                .await
                .unwrap();
            let client_id = ClientId::new();
            account_store
                .send(
                    &client_id,
                    AccountCommand::Register {
                        client_id,
                        email: Email::new("other@example.com").unwrap(),
                    },
                )
                .await
                .unwrap();
            account_store
                .send(
                    &client_id,
                    AccountCommand::LinkToAlpaca {
                        alpaca_account: AlpacaAccountNumber(
                            "ALPACA999".to_string(),
                        ),
                    },
                )
                .await
                .unwrap();
            account_store
                .send(
                    &client_id,
                    AccountCommand::WhitelistWallet { wallet: other_wallet },
                )
                .await
                .unwrap();
        }

        let store = setup_test_store(&pool);
        let value = U256::from_str_radix("750000000000000000", 10).unwrap();
        let funding_tx = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        let neighbor_tx = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        let unrecorded_tx = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let funding = crate::burn_excess::FundingTransferId {
            network: Network::Base,
            vault,
            tx_hash: funding_tx,
            log_index: 2,
            from: ap_wallet,
            to: bot_wallet,
            amount: value,
        };
        crate::burn_excess::exclusion::record_funding_exclusion(
            &pool,
            &funding,
            b256!(
                "0x1bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
            ),
            chrono::Utc::now(),
        )
        .await
        .unwrap();

        let assets = list_enabled_assets(&pool).await.unwrap();

        let excluded = create_transfer_log_with_index(
            vault, ap_wallet, bot_wallet, value, funding_tx, 100, 2,
        );
        let excluded_outcome = detect_transfer(
            &excluded,
            vault,
            Network::Base,
            &assets,
            &store,
            &pool,
        )
        .await
        .unwrap();
        assert!(
            matches!(excluded_outcome, TransferOutcome::SkippedAdminRecovery),
            "excluded funding log must not open a redemption: {excluded_outcome:?}"
        );
        assert!(
            store
                .load(&IssuerRedemptionRequestId::new(funding_tx))
                .await
                .unwrap()
                .is_none(),
            "no Redemption aggregate for excluded funding log"
        );

        // Sibling log in the *same* transaction as the excluded one. The skip
        // key is (network, vault, tx_hash, log_index), so only log_index 2 is
        // excluded; log 3 must still redeem. This is what pins the exclusion to
        // one log rather than to the whole transaction.
        let sibling = create_transfer_log_with_index(
            vault, ap_wallet, bot_wallet, value, funding_tx, 100, 3,
        );
        let sibling_outcome = detect_transfer(
            &sibling,
            vault,
            Network::Base,
            &assets,
            &store,
            &pool,
        )
        .await
        .unwrap();
        assert!(
            matches!(sibling_outcome, TransferOutcome::Detected { .. }),
            "a sibling log in the excluded transaction must still redeem: \
             {sibling_outcome:?}"
        );

        // Neighbor transfer (different tx / log) still redeems; exclusion is
        // exact-identity only, not "skip all from this wallet".
        let neighbor = create_transfer_log_with_index(
            vault,
            other_wallet,
            bot_wallet,
            value,
            neighbor_tx,
            100,
            0,
        );
        let neighbor_outcome = detect_transfer(
            &neighbor,
            vault,
            Network::Base,
            &assets,
            &store,
            &pool,
        )
        .await
        .unwrap();
        assert!(
            matches!(neighbor_outcome, TransferOutcome::Detected { .. }),
            "neighbor transfer must still redeem: {neighbor_outcome:?}"
        );

        let unrecorded = create_transfer_log_with_index(
            vault,
            ap_wallet,
            bot_wallet,
            value,
            unrecorded_tx,
            101,
            0,
        );
        let unrecorded_outcome = detect_transfer(
            &unrecorded,
            vault,
            Network::Base,
            &assets,
            &store,
            &pool,
        )
        .await
        .unwrap();
        assert!(
            matches!(unrecorded_outcome, TransferOutcome::Detected { .. }),
            "unrecorded same-wallet transfer must still redeem: {unrecorded_outcome:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Skipping admin recovery funding transfer"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_skips_mint_events() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(vault, None).await;
        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault,
            Address::ZERO,
            bot_wallet,
            value,
            tx_hash,
            12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(result, Ok(TransferOutcome::SkippedMint)),
            "Expected SkippedMint, got {result:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Skipping mint event"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_missing_tx_hash() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(vault, None).await;
        let store = setup_test_store(&pool);

        let mut log = create_transfer_log(
            vault,
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );
        log.transaction_hash = None;

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(result, Err(TransferProcessingError::MissingTxHash)),
            "Expected MissingTxHash, got {result:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_missing_block_number() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(vault, None).await;
        let store = setup_test_store(&pool);

        let mut log = create_transfer_log(
            vault,
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );
        log.block_number = None;

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(result, Err(TransferProcessingError::MissingBlockNumber)),
            "Expected MissingBlockNumber, got {result:?}"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_missing_log_index_fails_closed() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        let pool = setup_test_db_with_asset(vault, None).await;
        let store = setup_test_store(&pool);

        let mut log = create_transfer_log(
            vault,
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );
        log.log_index = None;

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(result, Err(TransferProcessingError::MissingLogIndex)),
            "Expected MissingLogIndex, got {result:?}"
        );
        assert!(
            result.as_ref().unwrap_err().is_non_transient(),
            "missing log_index must not retry/freeze checkpoint indefinitely"
        );
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_no_matching_asset() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let wrong_vault =
            address!("0x9876543210fedcba9876543210fedcba98765432");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let pool = setup_test_db_with_asset(wrong_vault, Some(ap_wallet)).await;
        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();

        let log = create_transfer_log(
            vault,
            ap_wallet,
            bot_wallet,
            value,
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(
                result,
                Err(TransferProcessingError::NoMatchingAsset { .. })
            ),
            "Expected NoMatchingAsset, got {result:?}"
        );
    }

    /// Two enabled assets bound to the same vault is a misconfiguration that
    /// `find_matching_asset` must reject with `AmbiguousVault` rather than
    /// silently attributing the redemption to an arbitrary underlying at Alpaca.
    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_ambiguous_vault_when_two_assets_share_one() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");

        // `setup_test_db_with_asset` seeds AAPL on `vault`; add a SECOND asset
        // (TSLA) bound to the SAME vault to create the ambiguity.
        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;
        let store = setup_test_store(&pool);

        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        let tsla = UnderlyingSymbol::new("TSLA").unwrap();
        let key = AssetKey::new(tsla.clone(), Network::Base);
        asset_store
            .send(
                &key,
                TokenizedAssetCommand::Add {
                    underlying: tsla,
                    token: TokenSymbol::new("tTSLA"),
                    network: Network::Base,
                    vault,
                },
            )
            .await
            .unwrap();

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let log = create_transfer_log(
            vault,
            ap_wallet,
            bot_wallet,
            value,
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(
                result,
                Err(TransferProcessingError::AmbiguousVault { .. })
            ),
            "two assets sharing a vault must fail loudly, not misroute: \
             got {result:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::WARN,
            &["Two enabled assets share this vault"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_skips_unknown_wallet() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let unknown_wallet =
            address!("0x1111111111111111111111111111111111111111");

        let pool = setup_test_db_with_asset(vault, None).await;
        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();

        let log = create_transfer_log(
            vault,
            unknown_wallet,
            bot_wallet,
            value,
            b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let result =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;

        assert!(
            matches!(result, Ok(TransferOutcome::SkippedNoAccount)),
            "Expected SkippedNoAccount, got {result:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Skipping transfer from unknown/unlinked wallet"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_idempotent_on_duplicate() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;
        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault, ap_wallet, bot_wallet, value, tx_hash, 12345,
        );

        let assets = list_enabled_assets(&pool).await.unwrap();
        let first =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;
        assert!(
            matches!(first, Ok(TransferOutcome::Detected { .. })),
            "First detection should succeed, got {first:?}"
        );

        let second =
            detect_transfer(&log, vault, Network::Base, &assets, &store, &pool)
                .await;
        assert!(
            matches!(second, Ok(TransferOutcome::AlreadyDetected)),
            "Second detection should return AlreadyDetected, got {second:?}"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["Redemption transfer detected"]
        ));
        assert!(logs_contain_at!(
            tracing::Level::DEBUG,
            &["Transfer already detected"]
        ));
    }
}
