use alloy::primitives::Address;
use cqrs_es::persist::GenericQuery;
use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
use sqlx::SqlitePool;
use std::sync::Arc;

use crate::account::view::AccountView;
use crate::account::{
    Account, AccountCommand, AlpacaAccountNumber, ClientId, Email,
};
use crate::tokenized_asset::{
    Network, TokenSymbol, TokenizedAsset, TokenizedAssetCommand,
    UnderlyingSymbol, view::TokenizedAssetView,
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

    let asset_view_repo = Arc::new(SqliteViewRepository::<
        TokenizedAssetView,
        TokenizedAsset,
    >::new(
        pool.clone(),
        "tokenized_asset_view".to_string(),
    ));
    let asset_query = GenericQuery::new(asset_view_repo);
    let asset_cqrs = sqlite_cqrs(pool.clone(), vec![Box::new(asset_query)], ());

    let underlying = UnderlyingSymbol::new("AAPL");
    let token = TokenSymbol::new("tAAPL");
    let network = Network::new("base");

    asset_cqrs
        .execute(
            &underlying.0,
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
        let account_view_repo =
            Arc::new(SqliteViewRepository::<AccountView, Account>::new(
                pool.clone(),
                "account_view".to_string(),
            ));

        let account_query = GenericQuery::new(account_view_repo);
        let account_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(account_query)], ());

        let client_id = ClientId::new();
        let email = Email::new("test@example.com".to_string()).unwrap();

        account_cqrs
            .execute(
                &client_id.to_string(),
                AccountCommand::Register { client_id, email },
            )
            .await
            .unwrap();

        account_cqrs
            .execute(
                &client_id.to_string(),
                AccountCommand::LinkToAlpaca {
                    alpaca_account: AlpacaAccountNumber(
                        "ALPACA123".to_string(),
                    ),
                },
            )
            .await
            .unwrap();

        account_cqrs
            .execute(
                &client_id.to_string(),
                AccountCommand::WhitelistWallet { wallet },
            )
            .await
            .unwrap();
    }

    pool
}
