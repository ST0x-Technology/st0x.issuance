use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, U256, address};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::{Mock, prelude::*};
use serde_json::json;
use url::Url;

use st0x_issuance::bindings::OffchainAssetReceiptVault;
use st0x_issuance::test_utils::{LocalEvm, test_alpaca_auth_header};
use st0x_issuance::{AlpacaConfig, Config, initialize_rocket};

fn setup_redemption_mocks(
    mock_alpaca: &MockServer,
    redemption_wallet: Address,
) -> (Mock, Mock) {
    let test_auth = test_alpaca_auth_header();

    let redeem_mock = mock_alpaca.mock(|when, then| {
        when.method(POST)
            .path("/v1/accounts/test-account/tokenization/redeem")
            .header("authorization", &test_auth);
        then.status(200).json_body(json!({
            "tokenization_request_id": "tok-redeem-123",
            "issuer_request_id": "red-test-789",
            "created_at": "2025-09-12T17:28:48.642437-04:00",
            "type": "redeem",
            "status": "pending",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "50",
            "issuer": "test-issuer",
            "network": "base",
            "wallet_address": redemption_wallet.to_string(),
            "tx_hash": "0x1234567890123456789012345678901234567890123456789012345678901234",
            "fees": "0.5"
        }));
    });

    let poll_mock = mock_alpaca.mock(|when, then| {
        when.method(GET)
            .path("/v1/accounts/test-account/tokenization/requests")
            .header("authorization", &test_auth);
        then.status(200).json_body(json!({
            "requests": [{
                "tokenization_request_id": "tok-redeem-123",
                "issuer_request_id": "red-test-789",
                "created_at": "2025-09-12T17:28:48.642437-04:00",
                "type": "redeem",
                "status": "completed",
                "underlying_symbol": "AAPL",
                "token_symbol": "tAAPL",
                "qty": "50",
                "issuer": "test-issuer",
                "network": "base",
                "wallet_address": redemption_wallet.to_string(),
                "tx_hash": "0x1234567890123456789012345678901234567890123456789012345678901234",
                "fees": "0.5"
            }]
        }));
    });

    (redeem_mock, poll_mock)
}

#[tokio::test]
async fn test_complete_redemption_flow_with_anvil()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let redemption_wallet =
        address!("0x9999999999999999999999999999999999999999");

    let (redeem_mock, poll_mock) =
        setup_redemption_mocks(&mock_alpaca, redemption_wallet);

    let config = Config {
        database_url: ":memory:".to_string(),
        database_max_connections: 5,
        rpc_url: Some(Url::parse(&evm.endpoint)?),
        private_key: Some(evm.private_key_hex()),
        vault_address: Some(evm.vault_address),
        redemption_wallet: Some(redemption_wallet),
        alpaca: AlpacaConfig {
            api_base_url: mock_alpaca.base_url(),
            account_id: "test-account".to_string(),
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            connect_timeout_secs: 10,
            request_timeout_secs: 30,
        },
    };

    let _rocket = initialize_rocket(config).await?;

    evm.grant_deposit_role(redemption_wallet).await?;

    let provider_signer = PrivateKeySigner::from_bytes(&evm.private_key)
        .map_err(|e| format!("Failed to create signer: {e}"))?;
    let wallet = EthereumWallet::from(provider_signer);
    let provider =
        ProviderBuilder::new().wallet(wallet).connect(&evm.endpoint).await?;

    let vault = OffchainAssetReceiptVault::new(evm.vault_address, &provider);

    let assets_to_mint = U256::from(50) * U256::from(10).pow(U256::from(18));
    let share_ratio = U256::from(10).pow(U256::from(18));
    let receipt_info = Bytes::from(b"test redemption");

    let mint_tx = vault
        .deposit(assets_to_mint, redemption_wallet, share_ratio, receipt_info)
        .send()
        .await?
        .get_receipt()
        .await?;

    let shares_minted = mint_tx
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            log.log_decode::<OffchainAssetReceiptVault::Deposit>()
                .ok()
                .map(|decoded| decoded.data().shares)
        })
        .ok_or("Deposit event not found")?;

    let initial_balance = vault.balanceOf(redemption_wallet).call().await?;
    assert_eq!(
        initial_balance, shares_minted,
        "Shares were not minted to redemption wallet"
    );

    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    let poll_interval = tokio::time::Duration::from_millis(100);

    let final_balance = loop {
        let balance = vault.balanceOf(redemption_wallet).call().await?;
        if balance < initial_balance {
            break balance;
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for balance to decrease after {}s (initial: {initial_balance}, current: {balance})",
                timeout.as_secs()
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    };

    redeem_mock.assert();
    poll_mock.assert();

    assert!(
        final_balance < initial_balance,
        "Expected shares to be burned, but balance didn't decrease"
    );

    Ok(())
}
