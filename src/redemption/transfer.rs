use alloy::primitives::Address;
use alloy::sol_types::SolEvent;
use cqrs_es::{AggregateContext, AggregateError, CqrsFramework, EventStore};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::{
    IssuerRedemptionRequestId, Redemption, RedemptionCommand, RedemptionError,
    burn_manager::BurnManager, journal_manager::JournalManager,
    redeem_call_manager::RedeemCallManager,
};
use crate::account::view::{AccountViewError, find_by_wallet};
use crate::account::{AccountView, AlpacaAccountNumber, ClientId};
use crate::bindings;
use crate::receipt_inventory::ReceiptInventory;
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
    Aggregate(#[from] cqrs_es::AggregateError<RedemptionError>),
    #[error("Account view error: {0}")]
    AccountView(#[from] AccountViewError),
    #[error("Tokenized asset view error: {0}")]
    TokenizedAssetView(#[from] TokenizedAssetViewError),
    #[error("No asset found for vault {vault}")]
    NoMatchingAsset { vault: Address },
}

/// Decodes a Transfer log, looks up account and asset, and executes
/// `RedemptionCommand::Detect`. Idempotent — returns `AlreadyDetected` on
/// duplicate.
pub(crate) async fn detect_transfer<RedemptionStore>(
    log: &alloy::rpc::types::Log,
    vault: Address,
    cqrs: &CqrsFramework<Redemption, RedemptionStore>,
    pool: &Pool<Sqlite>,
) -> Result<TransferOutcome, TransferProcessingError>
where
    RedemptionStore: EventStore<Redemption>,
{
    let transfer_event =
        bindings::OffchainAssetReceiptVault::Transfer::decode_log(&log.inner)?;

    if transfer_event.from == Address::ZERO {
        debug!(
            to = %transfer_event.to,
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
        debug!(
            from = %transfer_event.from,
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

    match cqrs.execute(&issuer_request_id.to_string(), command).await {
        Ok(()) => {}
        Err(AggregateError::UserError(_)) => {
            debug!(
                %issuer_request_id,
                "Transfer already detected"
            );
            return Ok(TransferOutcome::AlreadyDetected);
        }
        Err(err) => return Err(err.into()),
    }

    info!(
        %issuer_request_id,
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
pub(crate) struct RedemptionFlowCtx<RedemptionStore, ReceiptInventoryStore>
where
    RedemptionStore: EventStore<Redemption>,
    ReceiptInventoryStore: EventStore<ReceiptInventory>,
{
    pub(crate) event_store: Arc<RedemptionStore>,
    pub(crate) redeem_call_manager: Arc<RedeemCallManager<RedemptionStore>>,
    pub(crate) journal_manager: Arc<JournalManager<RedemptionStore>>,
    pub(crate) burn_manager:
        Arc<BurnManager<RedemptionStore, ReceiptInventoryStore>>,
}

/// Drives the post-detection redemption flow: Alpaca call, journal polling,
/// and burn. Errors are logged but do not propagate — the detection is already
/// recorded.
pub(crate) async fn drive_redemption_flow<
    RedemptionStore,
    ReceiptInventoryStore,
>(
    issuer_request_id: IssuerRedemptionRequestId,
    client_id: ClientId,
    alpaca_account: AlpacaAccountNumber,
    network: Network,
    deps: RedemptionFlowCtx<RedemptionStore, ReceiptInventoryStore>,
) where
    RedemptionStore: EventStore<Redemption> + 'static,
    ReceiptInventoryStore: EventStore<ReceiptInventory> + 'static,
    RedemptionStore::AC: Send,
    ReceiptInventoryStore::AC: Send,
{
    let aggregate_ctx = match deps
        .event_store
        .load_aggregate(&issuer_request_id.to_string())
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            warn!(
                %issuer_request_id,
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
            aggregate_ctx.aggregate(),
            client_id,
            network,
        )
        .await
    {
        warn!(
            %issuer_request_id,
            error = ?err,
            "handle_redemption_detected failed"
        );
        return;
    }

    let aggregate_ctx = match deps
        .event_store
        .load_aggregate(&issuer_request_id.to_string())
        .await
    {
        Ok(ctx) => ctx,
        Err(err) => {
            warn!(
                %issuer_request_id,
                error = ?err,
                "Failed to load aggregate after redeem call"
            );
            return;
        }
    };

    let Redemption::AlpacaCalled { tokenization_request_id, .. } =
        aggregate_ctx.aggregate()
    else {
        debug!(
            %issuer_request_id,
            aggregate_state = ?aggregate_ctx.aggregate(),
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
            warn!(
                %issuer_request_id,
                error = ?err,
                "handle_alpaca_called (journal polling) failed"
            );
            return;
        }

        let aggregate_ctx = match deps
            .event_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
        {
            Ok(ctx) => ctx,
            Err(err) => {
                warn!(
                    %issuer_request_id,
                    error = ?err,
                    "Failed to load aggregate after journal completion"
                );
                return;
            }
        };

        if matches!(aggregate_ctx.aggregate(), Redemption::Burning { .. }) {
            if let Err(err) = deps
                .burn_manager
                .handle_burning_started(
                    &issuer_request_id,
                    aggregate_ctx.aggregate(),
                )
                .await
            {
                warn!(
                    %issuer_request_id,
                    error = ?err,
                    "handle_burning_started failed"
                );
            }
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
        .find_map(|view| match view {
            TokenizedAssetView::Asset {
                underlying,
                token,
                network,
                vault: addr,
                ..
            } if addr == vault => Some((underlying, token, network)),
            _ => None,
        })
        .ok_or(TransferProcessingError::NoMatchingAsset { vault })
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{
        Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, address, b256,
    };
    use alloy::rpc::types::Log;
    use alloy::sol_types::SolEvent;
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{TransferOutcome, TransferProcessingError, detect_transfer};
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::redemption::Redemption;
    use crate::redemption::test_utils::setup_test_db_with_asset;
    use crate::test_utils::logs_contain_at;
    use crate::vault::mock::MockVaultService;

    type TestStore = MemStore<Redemption>;
    type TestCqrs = CqrsFramework<Redemption, TestStore>;

    fn setup_test_cqrs() -> (Arc<TestCqrs>, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let vault_service: Arc<dyn crate::vault::VaultService> =
            Arc::new(MockVaultService::new_success());
        let cqrs = Arc::new(CqrsFramework::new(
            (*store).clone(),
            vec![],
            vault_service,
        ));
        (cqrs, store)
    }

    fn create_transfer_log(
        from: Address,
        to: Address,
        value: U256,
        tx_hash: B256,
        block_number: u64,
    ) -> Log {
        let topics = vec![
            OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
            B256::left_padding_from(&from[..]),
            B256::left_padding_from(&to[..]),
        ];

        let data_bytes = value.to_be_bytes::<32>();

        Log {
            inner: PrimitiveLog {
                address: address!("0x0000000000000000000000000000000000000000"),
                data: LogData::new_unchecked(topics, Bytes::from(data_bytes)),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(block_number),
            block_timestamp: None,
            transaction_hash: Some(tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn detect_transfer_success() {
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");
        let bot_wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let ap_wallet = address!("0x9999999999999999999999999999999999999999");

        let (cqrs, store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log =
            create_transfer_log(ap_wallet, bot_wallet, value, tx_hash, 12345);

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

        let outcome = result.expect("Expected success");
        assert!(
            matches!(outcome, TransferOutcome::Detected { .. }),
            "Expected Detected outcome"
        );

        if let TransferOutcome::Detected { issuer_request_id, .. } = outcome {
            let context = store
                .load_aggregate(&issuer_request_id.to_string())
                .await
                .unwrap();
            assert!(
                matches!(context.aggregate(), Redemption::Detected { .. }),
                "Expected Detected state, got {:?}",
                context.aggregate()
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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, None).await;

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log = create_transfer_log(
            Address::ZERO,
            bot_wallet,
            value,
            tx_hash,
            12345,
        );

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, None).await;

        let mut log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );
        log.transaction_hash = None;

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, None).await;

        let mut log = create_transfer_log(
            address!("0x9999999999999999999999999999999999999999"),
            bot_wallet,
            U256::from(100),
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );
        log.block_number = None;

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(wrong_vault, Some(ap_wallet)).await;

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();

        let log = create_transfer_log(
            ap_wallet,
            bot_wallet,
            value,
            b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            12345,
        );

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, None).await;

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();

        let log = create_transfer_log(
            unknown_wallet,
            bot_wallet,
            value,
            b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            12345,
        );

        let result = detect_transfer(&log, vault, &cqrs, &pool).await;

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

        let (cqrs, _store) = setup_test_cqrs();
        let pool = setup_test_db_with_asset(vault, Some(ap_wallet)).await;

        let value = U256::from_str_radix("100000000000000000000", 10).unwrap();
        let tx_hash = b256!(
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        );

        let log =
            create_transfer_log(ap_wallet, bot_wallet, value, tx_hash, 12345);

        let first = detect_transfer(&log, vault, &cqrs, &pool).await;
        assert!(
            matches!(first, Ok(TransferOutcome::Detected { .. })),
            "First detection should succeed, got {first:?}"
        );

        let second = detect_transfer(&log, vault, &cqrs, &pool).await;
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
