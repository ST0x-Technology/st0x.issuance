#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use std::sync::{Arc, Mutex};

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{ETHEREUM_TEST_CHAIN_ID, Network, initialize_rocket};

/// Verifies redemption routing through `ChainRegistry`: a transfer to the bot
/// wallet on the Ethereum vault triggers Alpaca redeem with `network=ethereum`
/// and an on-chain burn on that chain, not Base.
#[tokio::test]
async fn test_multichain_redemption_routes_by_network()
-> Result<(), Box<dyn std::error::Error>> {
    let base_evm = LocalEvm::new().await?;
    let eth_evm = LocalEvm::with_chain_id(ETHEREUM_TEST_CHAIN_ID).await?;

    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let redemption_states: Arc<
        Mutex<Vec<harness::alpaca_mocks::RedemptionState>>,
    > = Arc::new(Mutex::new(Vec::new()));
    let (redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks_with_states(
            &mock_alpaca,
            Arc::clone(&redemption_states),
        );

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("multichain_redemption.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    harness::preseed_tokenized_asset_into_pool(
        &pool,
        base_evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;
    harness::preseed_tokenized_asset_into_pool_with_network(
        &pool,
        eth_evm.vault_address,
        "TSLA",
        "tTSLA",
        Network::Ethereum,
    )
    .await?;
    pool.close().await;

    let config = harness::create_multichain_config_with_db(
        &db_url,
        &mock_alpaca,
        &base_evm,
        &eth_evm,
    )?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();
    let bot_wallet = base_evm.wallet_address;

    harness::setup_roles(&base_evm, user_wallet, bot_wallet).await?;
    harness::setup_roles(&eth_evm, user_wallet, bot_wallet).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        harness::MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id: "alp-mint-eth-tsla-redeem",
            quantity: "10.0",
            underlying: "TSLA",
            token: "tTSLA",
            network: Network::Ethereum,
        },
    )
    .await?;
    harness::wait_for_mock_hit(&mint_callback_mock).await?;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());

    let eth_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance.clone())
        .connect(&eth_evm.endpoint)
        .await?;
    let eth_vault = OffchainAssetReceiptVaultInstance::new(
        eth_evm.vault_address,
        &eth_provider,
    );

    let base_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&base_evm.endpoint)
        .await?;
    let base_vault = OffchainAssetReceiptVaultInstance::new(
        base_evm.vault_address,
        &base_provider,
    );

    let eth_shares = harness::wait_for_shares(&eth_vault, user_wallet).await?;
    assert!(
        eth_shares > U256::ZERO,
        "Ethereum mint should credit shares on the Ethereum vault"
    );

    // Baseline captured before the redemption trigger: any wrong-chain mint
    // or burn during the redemption would move the Base vault's total supply.
    // (The bot wallet never holds Base-vault shares in this test, so a
    // balance check there would be trivially zero.)
    let base_supply_before = base_vault.totalSupply().call().await?;

    eth_vault
        .transfer(bot_wallet, eth_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hit(&redeem_mock).await?;
    harness::wait_for_burn(&eth_vault, bot_wallet).await?;

    let redeem_states = redemption_states.lock().unwrap().clone();
    assert_eq!(
        redeem_states.len(),
        1,
        "Expected exactly one Alpaca redeem call"
    );
    assert_eq!(
        redeem_states[0].network, "ethereum",
        "Alpaca redeem should carry the redemption aggregate's network"
    );
    assert_eq!(
        redeem_states[0].underlying_symbol, "TSLA",
        "Redeem should reference the Ethereum-network asset"
    );

    assert_eq!(
        base_vault.totalSupply().call().await?,
        base_supply_before,
        "Base vault total supply must be unchanged by the Ethereum redemption"
    );
    assert_eq!(
        eth_vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot wallet should hold no shares on the Ethereum vault after the \
         burn consumed the redeemed shares"
    );

    Ok(())
}
