use alloy::primitives::{
    Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, b256,
};
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use event_sorcery::{StoreBuilder, test_store};
use sqlx::SqlitePool;
use st0x_alpaca::issuer::mock::MockIssuerApi;
use std::sync::Arc;

use super::burn_manager::BurnManager;
use super::journal_manager::JournalManager;
use super::poller::{TransferPoller, TransferPollerConfig};
use super::redeem_call_manager::RedeemCallManager;
use super::{Redemption, RedemptionServices};
use crate::account::{
    Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
};
use crate::alpaca::AlpacaService;
use crate::bindings::OffchainAssetReceiptVault;
use crate::config::VaultModeConfig;
use crate::network_telemetry::NetworkTelemetry;
use crate::notifications::NoopLifecycleNotifier;
use crate::receipt_inventory::{
    CqrsReceiptService, ReceiptInventory, ReceiptService,
};
use crate::test_utils::ANVIL_CHAIN_ID;
use crate::tokenized_asset::{
    AssetKey, Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol,
};
use crate::vault::VaultService;
use crate::vault::mock::MockVaultService;

/// Creates an in-memory SQLite database with migrations applied, a seeded
/// tokenized asset (AAPL/tAAPL on base), and optionally a registered+linked
/// account with the given wallet whitelisted.
pub(crate) async fn setup_test_db_with_asset(
    vault: Address,
    ap_wallet: Option<Address>,
) -> SqlitePool {
    let pool = SqlitePool::connect(":memory:").await.unwrap();

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    let (asset_store, _asset_projection) =
        StoreBuilder::<TokenizedAsset>::new(pool.clone())
            .build(())
            .await
            .unwrap();

    let underlying = UnderlyingSymbol::new("AAPL").unwrap();
    let token = TokenSymbol::new("tAAPL");
    let network = Network::Base;

    asset_store
        .send(
            &AssetKey::new(underlying.clone(), network),
            TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token,
                network,
                vault,
            },
        )
        .await
        .unwrap();

    if let Some(wallet) = ap_wallet {
        link_ap_wallet(&pool, wallet).await;
    }

    pool
}

/// Registers an account, links it to Alpaca, and whitelists `wallet`, so a
/// Transfer from `wallet` into the redemption wallet reads as a redemption.
pub(crate) async fn link_ap_wallet(pool: &SqlitePool, wallet: Address) {
    let (account_store, _account_projection) =
        StoreBuilder::<Account>::new(pool.clone()).build(()).await.unwrap();

    let client_id = ClientId::new();
    let email = Email::new("test@example.com").unwrap();

    account_store
        .send(&client_id, AccountCommand::Register { client_id, email })
        .await
        .unwrap();

    account_store
        .send(
            &client_id,
            AccountCommand::LinkToAlpaca {
                alpaca_account: AlpacaAccountNumber("ALPACA123".to_string()),
            },
        )
        .await
        .unwrap();

    account_store
        .send(&client_id, AccountCommand::WhitelistWallet { wallet })
        .await
        .unwrap();
}

/// A transfer poller over `provider` whose redemption flow runs against
/// mocked Alpaca and vault services. `pool` must already have migrations
/// applied.
pub(crate) async fn transfer_poller_for_tests<P: Provider + Clone>(
    network: Network,
    provider: P,
    bot_wallet: Address,
    backfill_start_block: u64,
    pool: SqlitePool,
) -> TransferPoller<P> {
    let receipt_store =
        Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
    let receipt_service: Arc<dyn ReceiptService> =
        Arc::new(CqrsReceiptService::new(receipt_store));
    let vault_service: Arc<dyn VaultService> =
        Arc::new(MockVaultService::new_success());
    let store = Arc::new(test_store::<Redemption>(
        pool.clone(),
        RedemptionServices::with_single_vault(Network::Base, vault_service),
    ));

    let alpaca_service =
        Arc::new(MockIssuerApi::new_success()) as Arc<dyn AlpacaService>;
    let redeem_call_manager = Arc::new(RedeemCallManager::new(
        alpaca_service.clone(),
        store.clone(),
        pool.clone(),
        Arc::new(NoopLifecycleNotifier),
    ));
    let journal_manager = Arc::new(JournalManager::new(
        alpaca_service,
        store.clone(),
        pool.clone(),
    ));

    let apalis_pool = apalis_sqlite::SqlitePool::connect(":memory:")
        .await
        .expect("apalis test pool should connect");
    let burn_manager = Arc::new(BurnManager::new_for_tests(
        Arc::new(MockVaultService::new_success()),
        pool.clone(),
        store.clone(),
        receipt_service,
        bot_wallet,
        ANVIL_CHAIN_ID,
        apalis_pool,
    ));

    TransferPoller::new(TransferPollerConfig {
        network,
        provider,
        bot_wallet,
        backfill_start_block,
        store,
        pool,
        redeem_call_manager,
        journal_manager,
        burn_manager,
        vault_mode_config: VaultModeConfig::default(),
        telemetry: Arc::new(NetworkTelemetry::new([network])),
    })
}

pub(crate) fn create_transfer_log(
    vault_address: Address,
    from: Address,
    to: Address,
    value: U256,
    tx_hash: B256,
    block_number: u64,
) -> Log {
    create_transfer_log_with_index(
        vault_address,
        from,
        to,
        value,
        tx_hash,
        block_number,
        0,
    )
}

pub(crate) fn create_transfer_log_with_index(
    vault_address: Address,
    from: Address,
    to: Address,
    value: U256,
    tx_hash: B256,
    block_number: u64,
    log_index: u64,
) -> Log {
    let topics = vec![
        OffchainAssetReceiptVault::Transfer::SIGNATURE_HASH,
        B256::left_padding_from(&from[..]),
        B256::left_padding_from(&to[..]),
    ];

    let data_bytes = value.to_be_bytes::<32>();

    Log {
        inner: PrimitiveLog {
            address: vault_address,
            data: LogData::new_unchecked(topics, Bytes::from(data_bytes)),
        },
        block_hash: Some(b256!(
            "0x0000000000000000000000000000000000000000000000000000000000000001"
        )),
        block_number: Some(block_number),
        block_timestamp: None,
        transaction_hash: Some(tx_hash),
        transaction_index: Some(0),
        log_index: Some(log_index),
        removed: false,
    }
}
