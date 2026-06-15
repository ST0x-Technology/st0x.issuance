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
use crate::tokenized_asset::view::{
    TokenizedAssetViewError, list_enabled_assets,
};
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
        network: Network,
    },
    AlreadyDetected,
    SkippedMint,
    SkippedNoAccount,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TransferProcessingError {
    #[error("Failed to decode Transfer event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Quantity conversion error: {0}")]
    QuantityConversion(#[from] QuantityConversionError),
    #[error("CQRS error: {0}")]
    Aggregate(Box<AggregateError<LifecycleError<Redemption>>>),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("No asset found for vault {vault}")]
    NoMatchingAsset { vault: Address },
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
                | Self::QuantityConversion(_)
                | Self::NoMatchingAsset { .. }
        )
    }
}

/// Decodes a Transfer log, looks up account and asset, and executes
/// `RedemptionCommand::Detect`. Idempotent — returns `AlreadyDetected` on
/// duplicate.
pub(crate) async fn detect_transfer(
    log: &alloy::rpc::types::Log,
    vault: Address,
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

    let (underlying, token, network) = find_matching_asset(pool, vault).await?;

    let issuer_request_id = IssuerRedemptionRequestId::new(tx_hash);
    let quantity = Quantity::from_u256_with_18_decimals(transfer_event.value)?;

    let command = RedemptionCommand::Detect {
        issuer_request_id: issuer_request_id.clone(),
        underlying,
        token,
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
        network,
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
    network: Network,
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
            network,
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

    tokio::spawn(async move {
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
    });
}

async fn find_matching_asset(
    pool: &Pool<Sqlite>,
    vault: Address,
) -> Result<(UnderlyingSymbol, TokenSymbol, Network), TransferProcessingError> {
    let assets = list_enabled_assets(pool).await?;

    assets
        .into_iter()
        .find_map(
            |TokenizedAssetView {
                 underlying,
                 token,
                 network,
                 vault: addr,
                 ..
             }| {
                (addr == vault).then_some((underlying, token, network))
            },
        )
        .ok_or(TransferProcessingError::NoMatchingAsset { vault })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address, b256};
    use event_sorcery::{Store, StoreBuilder, test_store};
    use sqlx::SqlitePool;
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{TransferOutcome, TransferProcessingError, detect_transfer};
    use crate::redemption::Redemption;
    use crate::redemption::test_utils::{
        create_transfer_log, setup_test_db_with_asset,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, UnderlyingSymbol,
    };
    use crate::vault::mock::MockVaultService;

    fn setup_test_store(pool: &SqlitePool) -> Arc<Store<Redemption>> {
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());

        Arc::new(test_store::<Redemption>(pool.clone(), vault_service))
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

        let result = detect_transfer(&log, vault, &store, &pool).await;

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

        let (asset_store, _projection) =
            StoreBuilder::<TokenizedAsset>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        asset_store
            .send(&UnderlyingSymbol::new("AAPL"), TokenizedAssetCommand::Freeze)
            .await
            .expect("Failed to freeze asset");

        let store = setup_test_store(&pool);

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            vault, ap_wallet, bot_wallet, value, tx_hash, 12345,
        );

        let result = detect_transfer(&log, vault, &store, &pool).await;

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

        let result = detect_transfer(&log, vault, &store, &pool).await;

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

        let result = detect_transfer(&log, vault, &store, &pool).await;

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

        let result = detect_transfer(&log, vault, &store, &pool).await;

        assert!(
            matches!(result, Err(TransferProcessingError::MissingBlockNumber)),
            "Expected MissingBlockNumber, got {result:?}"
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

        let result = detect_transfer(&log, vault, &store, &pool).await;

        assert!(
            matches!(
                result,
                Err(TransferProcessingError::NoMatchingAsset { .. })
            ),
            "Expected NoMatchingAsset, got {result:?}"
        );
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

        let result = detect_transfer(&log, vault, &store, &pool).await;

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

        let first = detect_transfer(&log, vault, &store, &pool).await;
        assert!(
            matches!(first, Ok(TransferOutcome::Detected { .. })),
            "First detection should succeed, got {first:?}"
        );

        let second = detect_transfer(&log, vault, &store, &pool).await;
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
