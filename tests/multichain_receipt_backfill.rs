mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{ETHEREUM_TEST_CHAIN_ID, Network, initialize_rocket};

/// Verifies receipt startup backfill discovers receipts through each asset's
/// own chain provider.
///
/// The receipt is minted BEFORE the service starts, on a vault deployed as an
/// additional instance on the Ethereum chain -- its address differs from
/// `base_evm.vault_address` and the contract exists ONLY on the Ethereum
/// chain, so a lookup through the wrong (Base) provider could never find it.
/// Because the mint pre-dates the service, live receipt monitoring never sees
/// the deposit; only the startup backfill, querying the Ethereum RPC, can put
/// the receipt into inventory. Burn planning draws exclusively on inventoried
/// receipts, so the redemption burn completing proves the backfill discovered
/// the receipt via the Ethereum provider.
#[tokio::test]
async fn test_multichain_receipt_backfill_uses_chain_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let base_evm = LocalEvm::new().await?;
    let eth_evm = LocalEvm::with_chain_id(ETHEREUM_TEST_CHAIN_ID).await?;
    let (eth_vault_address, eth_authorizer_address) =
        eth_evm.deploy_additional_vault().await?;
    assert_ne!(
        eth_vault_address, base_evm.vault_address,
        "test precondition: the Ethereum vault address must not collide with \
         the Base vault"
    );

    let mock_alpaca = MockServer::start();
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let redeem_mock =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();
    let bot_wallet = base_evm.wallet_address;

    harness::setup_roles_on_vault(
        &eth_evm,
        eth_authorizer_address,
        eth_vault_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    // Pre-start mint: receipt + shares land on the bot wallet. The shares
    // then move to the user so the post-start transfer back to the bot can
    // trigger a redemption, while the ERC-1155 receipt stays with the bot
    // for the burn.
    let mint_amount = U256::from(50) * U256::from(10).pow(U256::from(18));
    let (_receipt_id, shares) = eth_evm
        .mint_directly_on_vault(eth_vault_address, mint_amount, bot_wallet)
        .await?;

    let bot_signer = PrivateKeySigner::from_bytes(&eth_evm.private_key)?;
    let bot_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(bot_signer))
        .connect(&eth_evm.endpoint)
        .await?;
    let bot_vault_instance = OffchainAssetReceiptVaultInstance::new(
        eth_vault_address,
        &bot_provider,
    );
    bot_vault_instance
        .transfer(user_wallet, shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("multichain_receipt_backfill.db");
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

    let mut config = harness::create_multichain_config_with_db(
        &db_url,
        &mock_alpaca,
        &base_evm,
        &eth_evm,
    )?;
    // The periodic receipt backfill must never fire within this test's
    // lifetime: it could discover the pre-start receipt and rescue a broken
    // STARTUP backfill, which is the path this test exists to prove.
    config.receipt_poll_interval = Duration::from_secs(3600);

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::setup_account(&client, user_wallet).await;

    // Transferring the shares back to the bot triggers redemption detection;
    // the subsequent burn can only draw on the receipt that startup backfill
    // discovered through the Ethereum provider.
    let user_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&eth_evm.endpoint)
        .await?;
    let user_vault_instance = OffchainAssetReceiptVaultInstance::new(
        eth_vault_address,
        &user_provider,
    );

    // Baseline captured before the redemption trigger: any wrong-chain mint
    // or burn during the redemption would move the Base vault's total supply.
    // (The bot wallet never holds Base-vault shares in this test, so a
    // balance check there would be trivially zero.)
    let base_provider =
        ProviderBuilder::new().connect(&base_evm.endpoint).await?;
    let base_vault_instance = OffchainAssetReceiptVaultInstance::new(
        base_evm.vault_address,
        &base_provider,
    );
    let base_supply_before = base_vault_instance.totalSupply().call().await?;

    user_vault_instance
        .transfer(bot_wallet, shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hit(&redeem_mock).await?;
    harness::wait_for_burn(&user_vault_instance, bot_wallet).await?;

    assert_eq!(
        base_vault_instance.totalSupply().call().await?,
        base_supply_before,
        "Base vault total supply must be unchanged by the Ethereum-vault \
         redemption"
    );

    Ok(())
}
