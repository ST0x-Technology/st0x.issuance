#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{ETHEREUM_TEST_CHAIN_ID, Network, initialize_rocket};

/// Verifies mint routing through `ChainRegistry`: Base mints land on the Base
/// Anvil vault; Ethereum-network mints land on the second Anvil chain.
#[tokio::test]
async fn test_multichain_mint_routes_by_network()
-> Result<(), Box<dyn std::error::Error>> {
    let base_evm = LocalEvm::new().await?;
    let eth_evm = LocalEvm::with_chain_id(ETHEREUM_TEST_CHAIN_ID).await?;

    // Both Anvils deploy from the same key and nonce, so the default vault
    // addresses collide across chains. Receipt inventory is keyed by
    // `{chain_id}:{vault}`, so colliding addresses would not merge streams —
    // but twin addresses would make the routing assertions vacuous. Deploy a
    // distinct vault on the Ethereum chain so a wrong-chain call cannot
    // masquerade as the right one.
    let (eth_vault_address, eth_authorizer_address) =
        eth_evm.deploy_additional_vault().await?;
    assert_ne!(eth_vault_address, base_evm.vault_address);

    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("multichain_mint.db");
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
        eth_vault_address,
        "TSLA",
        "tTSLA",
        Network::Ethereum,
    )
    .await?;
    pool.close().await;

    let (config, _mock_subgraph) = harness::create_multichain_config_with_db(
        &db_url,
        &mock_alpaca,
        &base_evm,
        &eth_evm,
        eth_vault_address,
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
    harness::setup_roles_on_vault(
        &eth_evm,
        eth_authorizer_address,
        eth_vault_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    let user_wallet_instance = EthereumWallet::from(user_signer.clone());

    let base_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance.clone())
        .connect(&base_evm.endpoint)
        .await?;
    let base_vault = OffchainAssetReceiptVaultInstance::new(
        base_evm.vault_address,
        &base_provider,
    );

    let eth_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&eth_evm.endpoint)
        .await?;
    let eth_vault = OffchainAssetReceiptVaultInstance::new(
        eth_vault_address,
        &eth_provider,
    );

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        harness::MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id: "alp-mint-base-aapl",
            quantity: "25.0",
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await?;
    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    // Baseline captured before the Ethereum mint runs, so the final assertion
    // can detect an Ethereum mint erroneously landing on the Base vault.
    let base_shares_after_base_mint =
        harness::wait_for_shares(&base_vault, user_wallet).await?;
    assert!(
        base_shares_after_base_mint > U256::ZERO,
        "Base mint should credit shares on the Base vault"
    );

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        harness::MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id: "alp-mint-eth-tsla",
            quantity: "10.0",
            underlying: "TSLA",
            token: "tTSLA",
            network: Network::Ethereum,
        },
    )
    .await?;
    harness::wait_for_mock_hits(&mint_callback_mock, 2).await?;

    let eth_shares = harness::wait_for_shares(&eth_vault, user_wallet).await?;
    assert!(
        eth_shares > U256::ZERO,
        "Ethereum mint should credit shares on the Ethereum vault"
    );

    assert_eq!(
        base_vault.balanceOf(user_wallet).call().await?,
        base_shares_after_base_mint,
        "Ethereum mint must not change the Base vault balance"
    );

    Ok(())
}
