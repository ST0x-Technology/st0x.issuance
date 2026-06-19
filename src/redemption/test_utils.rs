use alloy::primitives::{
    Address, B256, Bytes, Log as PrimitiveLog, LogData, U256, b256,
};
use alloy::rpc::types::Log;
use alloy::sol_types::SolEvent;
use event_sorcery::StoreBuilder;
use sqlx::SqlitePool;

use crate::account::{
    Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
};
use crate::bindings::OffchainAssetReceiptVault;
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol,
};

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

    let underlying = UnderlyingSymbol::new("AAPL");
    let token = TokenSymbol::new("tAAPL");
    let network = Network::Base;

    asset_store
        .send(
            &underlying,
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
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                },
            )
            .await
            .unwrap();

        account_store
            .send(&client_id, AccountCommand::WhitelistWallet { wallet })
            .await
            .unwrap();
    }

    pool
}

pub(crate) fn create_transfer_log(
    vault_address: Address,
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
        log_index: Some(0),
        removed: false,
    }
}
