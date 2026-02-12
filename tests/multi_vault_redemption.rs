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

/// Tests that redemptions are detected and completed on ALL vaults, not just one.
///
/// This test deploys two vaults (AAPL and TSLA), mints on both, transfers shares
/// back to the bot wallet on both (triggering redemptions), and asserts that both
/// redemptions complete (both burns succeed).
///
/// This test should FAIL with the current code because the service only monitors
/// a single vault (config.vault) for redemption transfers.
#[tokio::test]
async fn test_multi_vault_redemption_detects_on_all_vaults()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    // Deploy a second vault for TSLA
    let (vault2_address, vault2_authorizer) =
        evm.deploy_additional_vault().await?;

    // Use file-based database for persistence
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_multi_vault.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca);

    // Pre-seed assets BEFORE starting the service so that initialize_rocket
    // discovers both vaults and spawns redemption detectors for each.
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;
    harness::preseed_tokenized_asset(&db_url, vault2_address, "TSLA", "tTSLA")
        .await?;

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Grant roles on vault1 (AAPL)
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Grant roles on vault2 (TSLA)
    harness::setup_roles_on_vault(
        &evm,
        vault2_authorizer,
        vault2_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    // Setup account
    let link_body = harness::setup_account(&client, user_wallet).await;

    // Mint on vault1 (AAPL)
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-aapl",
        "50.0",
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Mint on vault2 (TSLA)
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-tsla",
        "30.0",
        "TSLA",
        "tTSLA",
    )
    .await?;

    // Setup user provider for transfers
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault1 = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let vault2 =
        OffchainAssetReceiptVaultInstance::new(vault2_address, &user_provider);

    // Wait for shares on both vaults
    let aapl_shares = harness::wait_for_shares(&vault1, user_wallet).await?;
    let tsla_shares = harness::wait_for_shares(&vault2, user_wallet).await?;

    assert!(aapl_shares > U256::ZERO, "Should have AAPL shares");
    assert!(tsla_shares > U256::ZERO, "Should have TSLA shares");

    // Transfer shares back to bot wallet on BOTH vaults (triggering redemptions)
    vault1
        .transfer(bot_wallet, aapl_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    vault2
        .transfer(bot_wallet, tsla_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Wait for burns on BOTH vaults
    harness::wait_for_burn(&vault1, bot_wallet).await?;
    harness::wait_for_burn(&vault2, bot_wallet).await?;

    // Verify all shares are burned on both vaults
    assert_eq!(
        vault1.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot should have no AAPL shares after burn"
    );
    assert_eq!(
        vault2.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot should have no TSLA shares after burn"
    );
    assert_eq!(
        vault1.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no AAPL shares after redemption"
    );
    assert_eq!(
        vault2.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no TSLA shares after redemption"
    );

    Ok(())
}

/// Tests that transfers occurring while the service is down are detected on restart.
///
/// The first service only pre-seeds AAPL (vault1), so it spawns a detector
/// only for vault1. TSLA (vault2) is added via API during phase 1 — no live
/// detector is spawned for it. After stopping the service, a transfer on vault2
/// can only be detected through backfilling at startup.
///
/// Flow:
/// 1. Pre-seed only AAPL. Start service. Add TSLA via API. Mint TSLA.
/// 2. Stop the service (first service's detector only covers vault1).
/// 3. Transfer TSLA shares to bot wallet while service is down.
/// 4. Restart the service — TSLA is now in DB from step 1.
/// 5. Assert the redemption on vault2 is detected via transfer backfill.
#[tokio::test]
async fn test_transfer_backfill_detects_transfers_while_down()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let (vault2_address, vault2_authorizer) =
        evm.deploy_additional_vault().await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_backfill.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca);

    // Only pre-seed AAPL — the first service will NOT spawn a detector for vault2.
    // TSLA gets added via API in phase 1 (after service starts), so no live
    // detector is ever spawned for it during the first service's lifetime.
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;

    // === Phase 1: Start service, add TSLA via API, mint on vault2 ===
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Add TSLA via API (no detector spawned — detectors are only created at startup)
    harness::seed_tokenized_asset_with(
        &client,
        vault2_address,
        "TSLA",
        "tTSLA",
    )
    .await;

    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;
    harness::setup_roles_on_vault(
        &evm,
        vault2_authorizer,
        vault2_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &link_body.client_id.to_string(),
        "alp-mint-tsla-backfill",
        "30.0",
        "TSLA",
        "tTSLA",
    )
    .await?;

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault2 =
        OffchainAssetReceiptVaultInstance::new(vault2_address, &user_provider);

    let tsla_shares = harness::wait_for_shares(&vault2, user_wallet).await?;
    assert!(tsla_shares > U256::ZERO, "Should have TSLA shares after mint");

    // === Phase 2: Stop service ===
    // The first service has no vault2 detector (TSLA was added via API after startup).
    drop(client);

    // === Phase 3: Transfer on vault2 while service is down ===
    vault2
        .transfer(bot_wallet, tsla_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    let bot_balance = vault2.balanceOf(bot_wallet).call().await?;
    assert!(
        bot_balance > U256::ZERO,
        "Bot should have TSLA shares from transfer"
    );

    // === Phase 4: Restart service ===
    // TSLA is now in the DB (added via API in phase 1). The second service
    // should detect it at startup and backfill the transfer.
    let config2 = harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket2 = initialize_rocket(config2).await?;
    let _client2 =
        rocket::local::asynchronous::Client::tracked(rocket2).await?;

    // === Phase 5: Assert redemption on vault2 completes via backfill ===
    harness::wait_for_burn(&vault2, bot_wallet).await?;

    assert_eq!(
        vault2.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "Bot should have no TSLA shares after backfill-triggered burn"
    );
    assert_eq!(
        vault2.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no TSLA shares after redemption"
    );

    Ok(())
}
