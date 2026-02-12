#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::mint::MintResponse;
use st0x_issuance::test_utils::LocalEvm;

async fn perform_mint_flow(
    client: &Client,
    evm: &LocalEvm,
    user_wallet: Address,
) -> Result<U256, Box<dyn std::error::Error>> {
    let link_body = harness::setup_account(client, user_wallet).await;

    let mint_response = client
        .post("/inkind/issuance")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            json!({
                "tokenization_request_id": "alp-mint-789",
                "qty": "50.0",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "network": "base",
                "client_id": link_body.client_id,
                "wallet_address": user_wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(mint_response.status(), rocket::http::Status::Ok);
    let mint_body: MintResponse = mint_response.into_json().await.unwrap();

    let confirm_response = client
        .post("/inkind/issuance/confirm")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            json!({
                "tokenization_request_id": "alp-mint-789",
                "issuer_request_id": mint_body.issuer_request_id,
                "status": "completed"
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(confirm_response.status(), rocket::http::Status::Ok);

    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    harness::wait_for_shares(&vault, user_wallet).await
}

#[tokio::test]
async fn test_tokenization_flow() -> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    // Use second Anvil test account for user (first account is bot)
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_tokenization.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

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

    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    let shares_minted = perform_mint_flow(&client, &evm, user_wallet).await?;
    mint_callback_mock.assert();

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_burn(&vault, bot_wallet).await?;

    redeem_mock.assert();
    poll_mock.assert();

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no shares after redemption"
    );

    Ok(())
}

/// Tests that MintManager and BurnManager share nonce state correctly.
///
/// This test reproduces the "nonce too low" bug that occurs when:
/// 1. MintManager and BurnManager have separate blockchain providers
/// 2. Each provider has its own nonce cache
/// 3. After a burn advances the on-chain nonce (via BurnManager's provider),
///    MintManager's provider still has its old cached nonce
/// 4. The next mint fails with "nonce too low"
///
/// The test performs: mint -> burn -> mint
/// With the bug present, the second mint fails.
/// After the fix (shared provider), all operations succeed.
#[tokio::test]
async fn test_mint_burn_mint_nonce_synchronization()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let _mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_nonce_sync.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

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

    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    // Step 1: First mint
    let link_body = harness::setup_account(&client, user_wallet).await;
    let client_id = link_body.client_id.to_string();

    harness::perform_mint_and_confirm(
        &client,
        user_wallet,
        &client_id,
        "alp-mint-1",
        "50.0",
    )
    .await?;
    let shares_minted = harness::wait_for_shares(&vault, user_wallet).await?;
    assert!(
        shares_minted > U256::ZERO,
        "User should have shares after first mint"
    );

    // Step 2: Burn (advances on-chain nonce)
    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;
    harness::wait_for_burn(&vault, bot_wallet).await?;
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "User should have no shares after burn"
    );

    // Step 3: Second mint (fails with stale nonce if bug is present)
    harness::perform_mint_and_confirm(
        &client,
        user_wallet,
        &client_id,
        "alp-mint-2",
        "25.0",
    )
    .await?;
    let final_balance = harness::wait_for_shares(&vault, user_wallet).await?;
    assert!(
        final_balance > U256::ZERO,
        "User should have shares after second mint"
    );

    Ok(())
}
