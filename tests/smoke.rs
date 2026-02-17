#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, b256};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

const UNDERLYING: &str = "TSLA";
const TOKEN: &str = "tTSLA";

/// Waits until the ERC-20 share balance of `wallet` strictly exceeds `threshold`.
async fn wait_for_balance_above<P>(
    vault: &OffchainAssetReceiptVaultInstance<P>,
    wallet: Address,
    threshold: U256,
) -> Result<U256, Box<dyn std::error::Error>>
where
    P: Provider,
{
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    let poll_interval = tokio::time::Duration::from_millis(100);

    loop {
        let balance = vault.balanceOf(wallet).call().await?;
        if balance > threshold {
            return Ok(balance);
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for balance > {threshold}. Current: {balance}"
            )
            .into());
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Copies the cleaned production database, replaces production addresses with
/// Anvil-compatible addresses, and verifies the service starts cleanly and
/// can perform three complete mint/redeem round trips.
///
/// Round 1: mint 1, redeem 1
/// Round 2: mint 1.5, mint 0.5, redeem 2
/// Round 3: mint 2, redeem 0.5, redeem 1.5
#[tokio::test]
async fn test_production_db_three_round_trips()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_private_key = b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    );
    let user_signer = PrivateKeySigner::from_bytes(&user_private_key)?;
    let user_wallet = user_signer.address();

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    // Copy the cleaned production database to a temp location
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("smoke.db");
    std::fs::copy("./issuance.db", &db_path)?;
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    // Replace production addresses with Anvil-compatible ones
    replace_production_addresses(&db_url, evm.vault_address, user_wallet)
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

    let client_id = get_client_id(&db_url).await?;

    // --- Round 1: mint 1 token, redeem 1 token ---

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &client_id,
        "smoke-r1-m1",
        "1.0",
        UNDERLYING,
        TOKEN,
    )
    .await?;
    let shares_r1 = harness::wait_for_shares(&vault, user_wallet).await?;

    vault.transfer(bot_wallet, shares_r1).send().await?.get_receipt().await?;
    harness::wait_for_burn(&vault, bot_wallet).await?;

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "Round 1: user should have 0 shares after full redeem"
    );

    // --- Round 2: mint 1.5, mint 0.5, redeem 2 ---

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &client_id,
        "smoke-r2-m1",
        "1.5",
        UNDERLYING,
        TOKEN,
    )
    .await?;
    let shares_after_first =
        harness::wait_for_shares(&vault, user_wallet).await?;

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &client_id,
        "smoke-r2-m2",
        "0.5",
        UNDERLYING,
        TOKEN,
    )
    .await?;
    let shares_r2 =
        wait_for_balance_above(&vault, user_wallet, shares_after_first).await?;

    vault.transfer(bot_wallet, shares_r2).send().await?.get_receipt().await?;
    harness::wait_for_burn(&vault, bot_wallet).await?;

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "Round 2: user should have 0 shares after full redeem"
    );

    // --- Round 3: mint 2, redeem 0.5, redeem 1.5 ---

    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        &client_id,
        "smoke-r3-m1",
        "2.0",
        UNDERLYING,
        TOKEN,
    )
    .await?;
    let shares_r3 = harness::wait_for_shares(&vault, user_wallet).await?;

    // Redeem 0.5 of 2.0 = 1/4 of total shares
    let quarter = shares_r3 / U256::from(4);
    let three_quarters = shares_r3 - quarter;

    vault.transfer(bot_wallet, quarter).send().await?.get_receipt().await?;
    harness::wait_for_burn(&vault, bot_wallet).await?;

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        three_quarters,
        "Round 3: user should have 3/4 shares after first partial redeem"
    );

    vault
        .transfer(bot_wallet, three_quarters)
        .send()
        .await?
        .get_receipt()
        .await?;
    harness::wait_for_burn(&vault, bot_wallet).await?;

    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "Round 3: user should have 0 shares after final redeem"
    );

    // Wait for all mint callbacks to complete — the last callback may still
    // be in flight after shares arrive on-chain.
    let expected_callbacks = 4;
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(5);
    loop {
        let calls = mint_callback_mock.calls_async().await;
        if calls >= expected_callbacks {
            break;
        }
        if start.elapsed() >= timeout {
            panic!(
                "Timed out waiting for {expected_callbacks} Alpaca mint callbacks. \
                 Got {calls}. The mint callback is sent after on-chain minting — \
                 if this consistently gets {calls}, the service may not be sending \
                 callbacks for all mints."
            );
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    Ok(())
}

/// Replaces production vault and wallet addresses in the copied database
/// with Anvil-compatible addresses.
async fn replace_production_addresses(
    db_url: &str,
    anvil_vault: Address,
    anvil_user_wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    use sqlx::sqlite::SqlitePoolOptions;

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    let vault_str = format!("{anvil_vault:#x}");
    let wallet_str = format!("{anvil_user_wallet:#x}");

    // Remove non-TSLA assets from the test copy — their production vault
    // addresses don't exist on Anvil and would cause startup failures.
    sqlx::query(
        "
        DELETE FROM events
        WHERE aggregate_type = 'TokenizedAsset'
          AND aggregate_id != 'TSLA'
        ",
    )
    .execute(&pool)
    .await?;

    sqlx::query(
        "
        UPDATE events
        SET payload = json_set(payload, '$.Added.vault', ?)
        WHERE aggregate_type = 'TokenizedAsset'
          AND event_type = 'TokenizedAssetEvent::Added'
        ",
    )
    .bind(&vault_str)
    .execute(&pool)
    .await?;

    sqlx::query(
        "
        UPDATE events
        SET payload = json_set(
            json_set(payload, '$.VaultAddressUpdated.vault', ?),
            '$.VaultAddressUpdated.previous_vault', ?
        )
        WHERE aggregate_type = 'TokenizedAsset'
          AND event_type = 'TokenizedAssetEvent::VaultAddressUpdated'
        ",
    )
    .bind(&vault_str)
    .bind(&vault_str)
    .execute(&pool)
    .await?;

    sqlx::query(
        "
        UPDATE events
        SET payload = json_set(payload, '$.WalletWhitelisted.wallet', ?)
        WHERE aggregate_type = 'Account'
          AND event_type = 'AccountEvent::WalletWhitelisted'
        ",
    )
    .bind(&wallet_str)
    .execute(&pool)
    .await?;

    pool.close().await;

    Ok(())
}

/// Retrieves one of the production account client_ids from the database.
async fn get_client_id(
    db_url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    use sqlx::sqlite::SqlitePoolOptions;

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    let row = sqlx::query_scalar::<_, String>(
        "
        SELECT json_extract(payload, '$.Registered.client_id')
        FROM events
        WHERE aggregate_type = 'Account'
          AND event_type = 'AccountEvent::Registered'
        LIMIT 1
        ",
    )
    .fetch_one(&pool)
    .await?;

    pool.close().await;

    Ok(row)
}
