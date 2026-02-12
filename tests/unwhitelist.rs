#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use serde_json::json;
use url::Url;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{
    AlpacaConfig, AuthConfig, Config, IpWhitelist, initialize_rocket,
};

#[tokio::test]
async fn test_unwhitelist_wallet_blocks_mint_and_redemption()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let mint_callback_mock = harness::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) =
        harness::setup_redemption_mocks(&mock_alpaca, user_wallet);

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Url::parse(&evm.endpoint)?,
        chain_id: st0x_issuance::ANVIL_CHAIN_ID,
        signer: st0x_issuance::SignerConfig::Local(evm.private_key),
        vault: evm.vault_address,
        backfill_start_block: 0,
        auth: AuthConfig {
            issuer_api_key: "test-key-12345678901234567890123456"
                .parse()
                .expect("Valid API key"),
            alpaca_ip_ranges: IpWhitelist::single(
                "127.0.0.1/32".parse().expect("Valid IP range"),
            ),
            internal_ip_ranges: "127.0.0.0/8,::1/128"
                .parse()
                .expect("Valid IP ranges"),
        },
        log_level: st0x_issuance::LogLevel::Debug,
        hyperdx: None,
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    };

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::seed_tokenized_asset(&client, evm.vault_address).await;
    harness::setup_roles(&evm, user_wallet, bot_wallet).await?;

    // Step 1: Setup account with whitelisted wallet, perform a mint
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

    let user_wallet_instance = EthereumWallet::from(user_signer);
    let user_provider = ProviderBuilder::new()
        .wallet(user_wallet_instance)
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    let shares_minted = harness::wait_for_shares(&vault, user_wallet).await?;
    assert!(shares_minted > U256::ZERO, "First mint should succeed");
    mint_callback_mock.assert();

    // Step 2: Unwhitelist the wallet
    let unwhitelist_response = client
        .delete(format!("/accounts/{client_id}/wallets/{user_wallet}"))
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .dispatch()
        .await;

    assert_eq!(
        unwhitelist_response.status(),
        rocket::http::Status::Ok,
        "Unwhitelist should succeed"
    );

    // Step 3: Second mint should be rejected (wallet no longer whitelisted)
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
                "tokenization_request_id": "alp-mint-2",
                "qty": "25.0",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "network": "base",
                "client_id": client_id,
                "wallet_address": user_wallet
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(
        mint_response.status(),
        rocket::http::Status::BadRequest,
        "Mint should be rejected after wallet is unwhitelisted"
    );

    // Step 4: Redemption should fail — transfer shares to bot, wait, verify
    // redeem mock was NOT hit
    vault
        .transfer(bot_wallet, shares_minted)
        .send()
        .await?
        .get_receipt()
        .await?;

    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    assert_eq!(
        redeem_mock.calls_async().await,
        0,
        "Redeem should not be called for unwhitelisted wallet"
    );

    Ok(())
}
