#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{U256, b256};
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use std::time::Duration;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::{LocalEvm, ROLE_CERTIFY, ROLE_DEPOSIT};
use st0x_issuance::{Network, WrappedTokenConfig, WrappedTokenEntry};
use st0x_issuance_dto::UnderlyingSymbol;

use crate::harness::{
    authenticated_get_json, create_provider, initialize_rocket,
};

/// Polls `GET /admin/wrapped-transfers` until it lists at least `expected`
/// rows, failing after a timeout with the last observed body.
async fn wait_for_wrapped_transfers(
    client: &Client,
    expected: usize,
) -> Vec<serde_json::Value> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let body =
            authenticated_get_json(client, "/admin/wrapped-transfers").await;
        let transfers = body["transfers"]
            .as_array()
            .expect("/admin/wrapped-transfers must contain a transfers array")
            .clone();

        if transfers.len() >= expected {
            return transfers;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} wrapped transfer(s); last body: \
             {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// An ERC-20 transfer of a configured wrapped token into the issuer wallet is
/// not a redemption the service can process, so it must surface to the
/// operator: the running service records it once, and `GET
/// /admin/wrapped-transfers` lists it with the chain, asset, amount, sender
/// and transaction. A second vault stands in for the ERC-4626 wrapper: the
/// watcher only relies on the ERC-20 `Transfer` event, which both emit.
#[tokio::test]
async fn inbound_wrapped_token_transfer_is_listed_for_the_operator()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;

    let user_signer = PrivateKeySigner::from_bytes(&b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    ))?;
    let user_wallet = user_signer.address();

    let (wrapped_token, wrapped_authorizer) =
        evm.deploy_additional_vault().await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("wrapped_transfer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let mut config =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    config.wrapped_tokens = WrappedTokenConfig::new([WrappedTokenEntry {
        network: Network::Base,
        underlying: UnderlyingSymbol::new("AAPL")?,
        token: wrapped_token,
    }])?;
    config.wrapped_transfer_poll_interval = Duration::from_millis(200);

    let rocket = initialize_rocket(config).await?;
    let client = Client::tracked(rocket).await?;

    // The service is up with nothing detected yet.
    let body =
        authenticated_get_json(&client, "/admin/wrapped-transfers").await;
    assert_eq!(body["transfers"], json!([]));

    // Mint stand-in wrapped tokens to the user, who then sends them to the
    // issuer wallet instead of the vault token.
    evm.grant_role_on_authorizer(wrapped_authorizer, ROLE_DEPOSIT, bot_wallet)
        .await?;
    evm.grant_role_on_authorizer(wrapped_authorizer, ROLE_CERTIFY, bot_wallet)
        .await?;
    evm.certify_specific_vault(wrapped_token, U256::MAX).await?;
    let amount = harness::tokens(7);
    evm.mint_directly_on_vault(wrapped_token, amount, user_wallet).await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let token =
        OffchainAssetReceiptVaultInstance::new(wrapped_token, &user_provider);
    let receipt =
        token.transfer(bot_wallet, amount).send().await?.get_receipt().await?;

    let transfers = wait_for_wrapped_transfers(&client, 1).await;
    let row = &transfers[0];
    assert_eq!(row["network"], json!("base"));
    assert_eq!(row["underlying"], json!("AAPL"));
    assert_eq!(row["token"], json!(wrapped_token));
    assert_eq!(row["from"], json!(user_wallet));
    assert_eq!(row["amount"], json!(amount.to_string()));
    assert_eq!(row["tx_hash"], json!(receipt.transaction_hash));
    assert_eq!(row["block_number"], json!(receipt.block_number.unwrap()));
    assert!(row["log_index"].is_u64(), "log_index missing: {row}");
    assert!(row["detected_at"].is_string(), "detected_at missing: {row}");

    // Later passes re-read the chain past the checkpoint; the transfer must
    // not be recorded twice.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let transfers = wait_for_wrapped_transfers(&client, 1).await;
    assert_eq!(transfers.len(), 1, "duplicate rows: {transfers:?}");

    Ok(())
}
