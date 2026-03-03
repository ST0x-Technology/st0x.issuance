#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

/// Tests that adding a new tokenized asset at runtime triggers backfills and
/// starts monitors for the new vault, without requiring a service restart.
///
/// Scenario:
/// 1. Start service with AAPL pre-seeded
/// 2. Deploy a new vault for MSFT on Anvil
/// 3. POST /tokenized-assets to add MSFT
/// 4. Mint on MSFT vault — receipt monitor should detect the receipt
/// 5. Transfer shares to bot wallet — redemption detector should detect it
#[tokio::test]
async fn test_new_asset_triggers_backfills_and_monitors()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Deploy a second vault for MSFT
    let (vault2_address, vault2_authorizer) =
        evm.deploy_additional_vault().await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_dynamic_asset.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    // Pre-seed only AAPL — MSFT will be added at runtime via API
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Grant roles on vault2 (MSFT)
    harness::setup_roles_on_vault(
        &evm,
        vault2_authorizer,
        vault2_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    // Add MSFT via API at runtime — this should trigger backfills + monitors
    harness::seed_tokenized_asset_with(
        &client,
        vault2_address,
        "MSFT",
        "tMSFT",
    )
    .await;

    // Setup account
    let link_body = harness::setup_account(&client, user_wallet).await;

    // Mint on MSFT vault — this tests that:
    // 1. The receipt monitor is running for MSFT (mint completes via callback)
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-msft",
        "25.0",
        "MSFT",
        "tMSFT",
    )
    .await?;

    // Setup user provider for transfers
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault2 =
        OffchainAssetReceiptVaultInstance::new(vault2_address, &user_provider);

    // Wait for shares on MSFT vault
    let msft_shares = harness::wait_for_shares(&vault2, user_wallet).await?;
    assert!(msft_shares > U256::ZERO, "Should have MSFT shares after mint");

    // Transfer shares to bot wallet — tests that redemption detector is running
    vault2
        .transfer(bot_wallet, msft_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Wait for burn to complete — proves redemption detector is monitoring MSFT
    harness::wait_for_burn(&vault2, bot_wallet).await?;

    assert_eq!(
        vault2.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no MSFT shares after redemption"
    );

    Ok(())
}

/// Tests that changing a vault address at runtime restarts monitors, which
/// pick up the new address from the tokenized asset view.
///
/// Scenario:
/// 1. Start service with vault A for AAPL
/// 2. Deploy a new vault A' on Anvil
/// 3. POST /tokenized-assets with AAPL pointing to vault A'
/// 4. Mint on vault A' — receipt monitor should detect it
/// 5. Transfer shares on vault A' — redemption detector should detect it
#[tokio::test]
async fn test_address_change_restarts_monitors()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Deploy a second vault (this will become the "new" AAPL vault)
    let (vault_b_address, vault_b_authorizer) =
        evm.deploy_additional_vault().await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_addr_change.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    // Pre-seed AAPL with vault A (the original)
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Grant roles on vault B (the new AAPL vault)
    harness::setup_roles_on_vault(
        &evm,
        vault_b_authorizer,
        vault_b_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    // Update AAPL to point to vault B — this should restart monitors
    harness::seed_tokenized_asset_with(
        &client,
        vault_b_address,
        "AAPL",
        "tAAPL",
    )
    .await;

    // Setup account
    let link_body = harness::setup_account(&client, user_wallet).await;

    // Mint on vault B (the new AAPL vault) — tests that monitors picked up
    // the new address
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-aapl-new",
        "25.0",
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Setup user provider for transfers
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault_b =
        OffchainAssetReceiptVaultInstance::new(vault_b_address, &user_provider);

    // Wait for shares on vault B
    let shares = harness::wait_for_shares(&vault_b, user_wallet).await?;
    assert!(shares > U256::ZERO, "Should have shares on new vault");

    // Transfer shares to bot wallet — tests redemption detector on new vault
    vault_b.transfer(bot_wallet, shares).send().await?.get_receipt().await?;

    // Wait for burn — proves redemption detector is monitoring vault B
    harness::wait_for_burn(&vault_b, bot_wallet).await?;

    assert_eq!(
        vault_b.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no shares after redemption on new vault"
    );

    Ok(())
}
