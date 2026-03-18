#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, Bytes, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use sqlx::sqlite::SqlitePoolOptions;
use std::time::Duration;

use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::bindings::Receipt::ReceiptInstance;
use st0x_issuance::initialize_rocket;
use st0x_issuance::test_utils::LocalEvm;

/// Helper: set up a second wallet from Anvil's test accounts (account index 1).
fn second_wallet() -> (PrivateKeySigner, Address) {
    let signer = PrivateKeySigner::from_bytes(&b256!(
        "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d"
    ))
    .unwrap();
    let addr = signer.address();
    (signer, addr)
}

/// Helper: set up a third wallet from Anvil's test accounts (account index 2).
fn third_wallet() -> (PrivateKeySigner, Address) {
    let signer = PrivateKeySigner::from_bytes(&b256!(
        "0x5de4111afa1a4b94908f83103eb1f1706367c2e68ca870fc3fb9a804cdab365a"
    ))
    .unwrap();
    let addr = signer.address();
    (signer, addr)
}

/// Helper: query the events table for ReceiptInventory events.
async fn receipt_events(db_url: &str) -> Vec<(String, String)> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(db_url)
        .await
        .unwrap();

    sqlx::query_as(
        "SELECT event_type, payload FROM events \
         WHERE aggregate_type = 'ReceiptInventory' \
         ORDER BY sequence",
    )
    .fetch_all(&pool)
    .await
    .unwrap()
}

/// Happy path: An ERC-1155 receipt transferred TO the bot wallet via
/// `safeTransferFrom` is discovered by the live monitor.
///
/// 1. Mint a receipt to a second wallet (not bot) — bot has 0 balance
/// 2. Start the service (backfill finds nothing for this receipt)
/// 3. Second wallet transfers the receipt to bot wallet via safeTransferFrom
/// 4. Monitor detects TransferSingle → discovers receipt with on-chain balance
#[tokio::test]
async fn test_inbound_receipt_transfer_discovered_by_monitor()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;
    let (other_signer, other_wallet) = second_wallet();

    // Grant roles: other_wallet needs DEPOSIT to mint directly
    evm.grant_deposit_role(other_wallet).await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Mint a receipt to other_wallet (not bot)
    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(75) * one_share;

    // We need to mint as other_wallet, so use their provider
    let other_evm_wallet = EthereumWallet::from(other_signer.clone());
    let other_provider = ProviderBuilder::new()
        .wallet(other_evm_wallet.clone())
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &other_provider,
    );

    let share_ratio = U256::from(10).pow(U256::from(18));
    let tx_receipt = vault
        .deposit(amount, other_wallet, share_ratio, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    // Extract receipt_id from Deposit event
    let deposit_event = tx_receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            log.log_decode::<st0x_issuance::bindings::OffchainAssetReceiptVault::Deposit>()
                .ok()
                .map(|d| d.inner)
        })
        .expect("Deposit event emitted");
    let receipt_id = deposit_event.id;

    // Get receipt contract address
    let receipt_contract_addr = Address::from(vault.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &other_provider);

    // Verify: other_wallet has the receipt, bot has 0
    let other_balance =
        receipt_contract.balanceOf(other_wallet, receipt_id).call().await?;
    assert_eq!(
        other_balance, deposit_event.shares,
        "other_wallet should hold the minted shares"
    );
    let bot_balance_before =
        receipt_contract.balanceOf(bot_wallet, receipt_id).call().await?;
    assert_eq!(
        bot_balance_before,
        U256::ZERO,
        "bot should have 0 before transfer"
    );

    // Setup mocks and database
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_inbound_transfer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Start service — backfill runs, bot has no receipts yet
    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let _client = Client::tracked(rocket).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Transfer receipt from other_wallet to bot_wallet via ERC-1155 safeTransferFrom
    receipt_contract
        .safeTransferFrom(
            other_wallet,
            bot_wallet,
            receipt_id,
            other_balance,
            Bytes::new(),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // Give monitor time to detect TransferSingle and process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: receipt was discovered in the event store
    let events = receipt_events(&db_url).await;
    let discovered =
        events.iter().filter(|(t, _)| t.contains("Discovered")).count();

    assert_eq!(
        discovered,
        1,
        "Inbound transfer should trigger exactly 1 receipt discovery. Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    // Verify on-chain: bot now holds the receipt
    let bot_balance_after =
        receipt_contract.balanceOf(bot_wallet, receipt_id).call().await?;
    assert_eq!(bot_balance_after, other_balance);

    Ok(())
}

/// Happy path: An ERC-1155 receipt transferred OUT of the bot wallet is
/// detected by the live monitor, and the receipt inventory is reconciled.
///
/// 1. Mint a receipt to bot wallet
/// 2. Start service (backfill discovers the receipt)
/// 3. Bot transfers the receipt to another wallet via safeTransferFrom
/// 4. Monitor detects outbound TransferSingle → reconciles balance to 0
/// 5. Receipt is depleted in aggregate
#[tokio::test]
async fn test_outbound_receipt_transfer_reconciles_balance()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;
    let (_other_signer, other_wallet) = second_wallet();

    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Mint a receipt to bot wallet BEFORE service starts
    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(60) * one_share;
    let (receipt_id, _shares) = evm.mint_directly(amount, bot_wallet).await?;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_outbound_transfer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Start service — backfill discovers the receipt
    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let _client = Client::tracked(rocket).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify receipt was discovered by backfill
    let events_before = receipt_events(&db_url).await;
    let discovered_before =
        events_before.iter().filter(|(t, _)| t.contains("Discovered")).count();
    assert_eq!(
        discovered_before, 1,
        "Backfill should discover exactly 1 receipt"
    );

    // Bot transfers the receipt out to other_wallet
    let bot_signer = PrivateKeySigner::from_bytes(&evm.private_key)?;
    let bot_evm_wallet = EthereumWallet::from(bot_signer);
    let bot_provider = ProviderBuilder::new()
        .wallet(bot_evm_wallet)
        .connect(&evm.endpoint)
        .await?;

    let receipt_contract_addr = {
        let vault = OffchainAssetReceiptVaultInstance::new(
            evm.vault_address,
            &bot_provider,
        );
        Address::from(vault.receipt().call().await?.0)
    };
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &bot_provider);

    let bot_balance =
        receipt_contract.balanceOf(bot_wallet, receipt_id).call().await?;

    receipt_contract
        .safeTransferFrom(
            bot_wallet,
            other_wallet,
            receipt_id,
            bot_balance,
            Bytes::new(),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // Give monitor time to detect outbound TransferSingle and reconcile
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: balance was reconciled (should see BalanceReconciled + Depleted events)
    let events_after = receipt_events(&db_url).await;
    let reconciled = events_after
        .iter()
        .filter(|(t, _)| t.contains("BalanceReconciled"))
        .count();
    let depleted =
        events_after.iter().filter(|(t, _)| t.contains("Depleted")).count();

    assert_eq!(
        reconciled,
        1,
        "Outbound transfer should trigger exactly 1 BalanceReconciled. Events: {:?}",
        events_after.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );
    assert_eq!(
        depleted,
        1,
        "Full outbound transfer should trigger exactly 1 Depleted. Events: {:?}",
        events_after.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    // Verify on-chain: bot has 0 balance for this receipt
    let bot_balance_after =
        receipt_contract.balanceOf(bot_wallet, receipt_id).call().await?;
    assert_eq!(bot_balance_after, U256::ZERO);

    Ok(())
}

/// Unhappy path (filtered correctly): Mint transfers (from == address(0))
/// should NOT trigger duplicate discovery — they are already covered by
/// Deposit event handling.
///
/// 1. Start the service
/// 2. Mint a receipt to bot wallet (emits both Deposit AND TransferSingle with from=0)
/// 3. Verify only ONE Discovered event (from Deposit handler, not double-counted)
#[tokio::test]
async fn test_mint_transfer_not_double_counted()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;

    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_no_double_count.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Start service FIRST, then mint (so live monitor processes the events)
    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let _client = Client::tracked(rocket).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Mint while service is running — emits Deposit + TransferSingle(from=0x0)
    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(25) * one_share;
    let (receipt_id, shares) = evm.mint_directly(amount, bot_wallet).await?;

    // Give monitor time to process both events
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: exactly 1 Discovered event (not 2 from double-counting)
    let events = receipt_events(&db_url).await;
    let discovered =
        events.iter().filter(|(t, _)| t.contains("Discovered")).count();

    assert_eq!(
        discovered,
        1,
        "Mint should produce exactly 1 Discovered event (TransferSingle from 0x0 filtered). \
         Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    // Verify on-chain: bot holds the minted receipt with expected balance
    let provider = ProviderBuilder::new().connect(&evm.endpoint).await?;
    let vault_contract =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider);
    let receipt_contract_addr =
        Address::from(vault_contract.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &provider);
    let bot_balance =
        receipt_contract.balanceOf(bot_wallet, receipt_id).call().await?;
    assert_eq!(
        bot_balance, shares,
        "Bot should hold the minted shares for the receipt"
    );

    Ok(())
}

/// Happy path: Inbound transfer to bot wallet is discovered during backfill
/// (not just live monitoring).
///
/// 1. Mint receipt to other_wallet, transfer to bot_wallet — all BEFORE service starts
/// 2. Start service — backfill scans TransferSingle events, discovers the receipt
/// 3. Verify the receipt appears in the event store
#[tokio::test]
async fn test_inbound_transfer_discovered_by_backfill()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;
    let (other_signer, other_wallet) = second_wallet();

    evm.grant_deposit_role(other_wallet).await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Mint as other_wallet
    let other_evm_wallet = EthereumWallet::from(other_signer.clone());
    let other_provider = ProviderBuilder::new()
        .wallet(other_evm_wallet)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &other_provider,
    );

    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(40) * one_share;
    let share_ratio = U256::from(10).pow(U256::from(18));

    let tx_receipt = vault
        .deposit(amount, other_wallet, share_ratio, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let deposit_event = tx_receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            log.log_decode::<st0x_issuance::bindings::OffchainAssetReceiptVault::Deposit>()
                .ok()
                .map(|d| d.inner)
        })
        .expect("Deposit event");
    let receipt_id = deposit_event.id;

    // Transfer receipt to bot_wallet BEFORE service starts
    let receipt_contract_addr = Address::from(vault.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &other_provider);

    let balance =
        receipt_contract.balanceOf(other_wallet, receipt_id).call().await?;

    receipt_contract
        .safeTransferFrom(
            other_wallet,
            bot_wallet,
            receipt_id,
            balance,
            Bytes::new(),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // Now start the service — backfill should discover the receipt
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_backfill_transfer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let _rocket = initialize_rocket(config).await?;

    // Give backfill time
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify: receipt was discovered
    let events = receipt_events(&db_url).await;
    let discovered =
        events.iter().filter(|(t, _)| t.contains("Discovered")).count();

    assert_eq!(
        discovered,
        1,
        "Backfill should discover exactly 1 receipt transferred to bot wallet. Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    Ok(())
}

/// Unhappy path (filtered correctly): Burn transfers (to == address(0))
/// should NOT trigger duplicate reconciliation — they are already covered
/// by the Withdraw event handler.
///
/// 1. Mint receipt to bot wallet BEFORE service starts
/// 2. Start service (backfill discovers receipt)
/// 3. Bot burns the receipt via withdraw() — emits Withdraw + TransferSingle(to=0x0)
/// 4. Verify exactly 1 BalanceReconciled event (from Withdraw, not double-counted)
#[tokio::test]
async fn test_burn_transfer_not_double_reconciled()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;

    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Mint receipt to bot wallet BEFORE service starts
    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(30) * one_share;
    let (receipt_id, shares) = evm.mint_directly(amount, bot_wallet).await?;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_burn_no_double.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Start service — backfill discovers the receipt
    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let _client = Client::tracked(rocket).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Burn the receipt via withdraw — emits Withdraw AND TransferSingle(to=0x0)
    evm.withdraw_directly(receipt_id, shares, bot_wallet).await?;

    // Give monitor time to process both events
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: exactly 1 BalanceReconciled and 1 Depleted (from Withdraw handler only)
    let events = receipt_events(&db_url).await;
    let reconciled =
        events.iter().filter(|(t, _)| t.contains("BalanceReconciled")).count();
    let depleted =
        events.iter().filter(|(t, _)| t.contains("Depleted")).count();

    assert_eq!(
        reconciled,
        1,
        "Burn should produce exactly 1 BalanceReconciled (TransferSingle to 0x0 filtered). \
         Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );
    assert_eq!(
        depleted,
        1,
        "Full burn should produce exactly 1 Depleted event. Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    Ok(())
}

/// Unhappy path: Transfer between two unrelated wallets (neither is bot)
/// should not produce any inventory changes.
///
/// 1. Mint receipt to wallet A
/// 2. Start service
/// 3. Wallet A transfers receipt to wallet B (neither is bot_wallet)
/// 4. Verify: no Discovered events for that receipt ID
#[tokio::test]
async fn test_unrelated_transfer_ignored()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;
    let (wallet_a_signer, wallet_a) = second_wallet();
    let (_wallet_b_signer, wallet_b) = third_wallet();

    evm.grant_deposit_role(wallet_a).await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Mint receipt to wallet_a
    let wallet_a_evm = EthereumWallet::from(wallet_a_signer.clone());
    let wallet_a_provider = ProviderBuilder::new()
        .wallet(wallet_a_evm)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &wallet_a_provider,
    );

    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(20) * one_share;
    let share_ratio = U256::from(10).pow(U256::from(18));

    let tx_receipt = vault
        .deposit(amount, wallet_a, share_ratio, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let deposit_event = tx_receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            log.log_decode::<st0x_issuance::bindings::OffchainAssetReceiptVault::Deposit>()
                .ok()
                .map(|d| d.inner)
        })
        .expect("Deposit event");
    let receipt_id = deposit_event.id;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_unrelated_transfer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    // Start service
    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let rocket = initialize_rocket(config).await?;
    let _client = Client::tracked(rocket).await?;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Transfer receipt from wallet_a to wallet_b (neither is bot)
    let receipt_contract_addr = Address::from(vault.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &wallet_a_provider);

    let balance =
        receipt_contract.balanceOf(wallet_a, receipt_id).call().await?;

    receipt_contract
        .safeTransferFrom(wallet_a, wallet_b, receipt_id, balance, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    // Give monitor time to (not) process
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Verify: no Discovered events at all (the Deposit was for wallet_a, not bot,
    // and the transfer was between wallet_a and wallet_b, not involving bot)
    let events = receipt_events(&db_url).await;
    let discovered =
        events.iter().filter(|(t, _)| t.contains("Discovered")).count();

    assert_eq!(
        discovered,
        0,
        "Transfer between unrelated wallets should not produce any Discovered events. \
         Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    let reconciled =
        events.iter().filter(|(t, _)| t.contains("BalanceReconciled")).count();
    assert_eq!(
        reconciled,
        0,
        "Transfer between unrelated wallets should not produce any BalanceReconciled events. \
         Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    Ok(())
}

/// Unhappy path: Inbound transfer for a receipt that has zero on-chain balance
/// at query time should be skipped (not discovered).
///
/// 1. Other wallet mints receipt, transfers it to bot, then bot burns it — all
///    BEFORE service starts
/// 2. Start service — backfill sees the inbound TransferSingle but balanceOf
///    returns 0
/// 3. Verify: receipt is NOT discovered (zero balance skip)
#[tokio::test]
async fn test_inbound_transfer_with_zero_balance_skipped()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let bot_wallet = evm.wallet_address;
    let (other_signer, other_wallet) = second_wallet();

    evm.grant_deposit_role(other_wallet).await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_withdraw_role(bot_wallet).await?;
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;

    // Step 1: Other wallet mints receipt
    let other_evm_wallet = EthereumWallet::from(other_signer.clone());
    let other_provider = ProviderBuilder::new()
        .wallet(other_evm_wallet)
        .connect(&evm.endpoint)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &other_provider,
    );

    let one_share = U256::from(10).pow(U256::from(18));
    let amount = U256::from(50) * one_share;
    let share_ratio = U256::from(10).pow(U256::from(18));

    let tx_receipt = vault
        .deposit(amount, other_wallet, share_ratio, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let deposit_event = tx_receipt
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            log.log_decode::<st0x_issuance::bindings::OffchainAssetReceiptVault::Deposit>()
                .ok()
                .map(|d| d.inner)
        })
        .expect("Deposit event");
    let receipt_id = deposit_event.id;
    let shares = deposit_event.shares;

    // Step 2: Transfer receipt to bot
    let receipt_contract_addr = Address::from(vault.receipt().call().await?.0);
    let receipt_contract =
        ReceiptInstance::new(receipt_contract_addr, &other_provider);

    let receipt_balance =
        receipt_contract.balanceOf(other_wallet, receipt_id).call().await?;

    receipt_contract
        .safeTransferFrom(
            other_wallet,
            bot_wallet,
            receipt_id,
            receipt_balance,
            Bytes::new(),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    // Step 3: Other wallet transfers ERC-20 shares to bot so bot can withdraw
    vault.transfer(bot_wallet, shares).send().await?.get_receipt().await?;

    // Step 4: Bot burns the receipt (withdraw requires both shares + receipt)
    evm.withdraw_directly(receipt_id, shares, bot_wallet).await?;

    // Verify on-chain: bot has 0 receipt balance
    let bot_provider = ProviderBuilder::new()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let receipt_check =
        ReceiptInstance::new(receipt_contract_addr, &bot_provider);
    let bot_balance =
        receipt_check.balanceOf(bot_wallet, receipt_id).call().await?;
    assert_eq!(bot_balance, U256::ZERO, "Bot should have 0 after burn");

    // Step 5: Start service — backfill sees inbound TransferSingle but balanceOf=0
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("test_zero_balance_skip.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let (config, _mock_subgraph) =
        harness::create_config_with_db(&db_url, &mock_alpaca, &evm)?;
    let _rocket = initialize_rocket(config).await?;

    // Give backfill time
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Verify: NO Discovered events — the receipt had 0 balance when queried
    let events = receipt_events(&db_url).await;
    let discovered =
        events.iter().filter(|(t, _)| t.contains("Discovered")).count();

    assert_eq!(
        discovered,
        0,
        "Receipt with zero on-chain balance should not be discovered. Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    let reconciled =
        events.iter().filter(|(t, _)| t.contains("BalanceReconciled")).count();
    assert_eq!(
        reconciled,
        0,
        "Receipt with zero on-chain balance should not trigger reconciliation. Events: {:?}",
        events.iter().map(|(t, _)| t.as_str()).collect::<Vec<_>>()
    );

    Ok(())
}
