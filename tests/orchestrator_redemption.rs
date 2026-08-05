#![allow(clippy::unwrap_used)]

//! End-to-end orchestrator-mode redemption flows on Anvil.
//!
//! Assets whose configured `vault_mode` resolves to `Orchestrator` burn via a
//! single `ST0xOrchestrator.burn()` (the orchestrator walks its own receipts
//! on-chain) instead of the vault-direct multicall. These tests drive the
//! full HTTP service through the public API, with the orchestrator deployed
//! on Anvil and only Alpaca mocked.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256, b256};
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;

use st0x_issuance::bindings::IST0xOrchestratorV1::IST0xOrchestratorV1Instance;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{Network, initialize_rocket};

use crate::harness::{
    MintFlowRequest, authenticated_get_json, bot_provider, create_provider,
    fetch_stuck_entries, orchestrator_vault_modes, tokens,
};

const USER_PRIVATE_KEY: B256 =
    b256!("0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");

/// Polls `GET /admin/stuck` until a `BurnFailed` entry appears, returning it.
async fn wait_for_burn_failed_entry(
    client: &Client,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);

    loop {
        if let Some(entry) = fetch_stuck_entries(client)
            .await
            .into_iter()
            .find(|entry| entry["state"] == "BurnFailed")
        {
            return Ok(entry);
        }

        if start.elapsed() >= timeout {
            return Err("Timeout waiting for a BurnFailed stuck entry".into());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Polls `GET /admin/stuck` until one snapshot shows `control_id` present
/// and `resolved_id` absent.
///
/// The bare absence of `resolved_id` would be vacuous: a seed that never
/// replayed (or a broken view projection) also produces an empty stuck list.
/// The control redemption is seeded in a state recovery provably never
/// resolves (a classified `BurnFailed` — the reconciler skips typed
/// classifications), so its presence in the SAME snapshot proves the seeds
/// replay and the stuck surfacing works, making the target's absence positive
/// evidence that recovery resolved it. Requiring both in one snapshot also
/// sidesteps the startup race where recovery completes before the first poll
/// could ever observe the target.
async fn wait_for_recovery_to_resolve(
    client: &Client,
    resolved_id: &str,
    control_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);

    loop {
        let entries = fetch_stuck_entries(client).await;
        let control_present =
            entries.iter().any(|entry| entry["aggregate_id"] == control_id);
        let resolved_absent =
            !entries.iter().any(|entry| entry["aggregate_id"] == resolved_id);
        if control_present && resolved_absent {
            return Ok(());
        }

        if start.elapsed() >= timeout {
            return Err(format!(
                "Timeout waiting for recovery: control {control_id} \
                 present={control_present}, target {resolved_id} \
                 absent={resolved_absent}"
            )
            .into());
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Event stream for a control redemption parked in a classified
/// `BurnFailed` — a state recovery provably never resolves (the reconciler
/// skips typed classifications) — seeded alongside a recovery target so the
/// absence-based assertion in [`wait_for_recovery_to_resolve`] is
/// non-vacuous.
fn classified_control_events(
    control_id: &str,
    control_tx_hash: B256,
    orchestrator_address: Address,
    user_wallet: Address,
    old: &str,
) -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "RedemptionEvent::Detected",
            json!({
                "Detected": {
                    "issuer_request_id": control_id,
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "wallet": user_wallet,
                    "quantity": "5",
                    "tx_hash": control_tx_hash,
                    "block_number": 1,
                    "detected_at": old,
                    "burn_mode": {
                        "Orchestrator": { "address": orchestrator_address }
                    }
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaCalled",
            json!({
                "AlpacaCalled": {
                    "issuer_request_id": control_id,
                    "tokenization_request_id": "tok-recovery-control",
                    "alpaca_quantity": "5",
                    "dust_quantity": "0",
                    "called_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaJournalCompleted",
            json!({
                "AlpacaJournalCompleted": {
                    "issuer_request_id": control_id,
                    "alpaca_journal_completed_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::BurningFailed",
            json!({
                "BurningFailed": {
                    "issuer_request_id": control_id,
                    "error": "allowance missing (control fixture)",
                    "failed_at": old,
                    "tx_id": null,
                    "planned_burns": [],
                    "classification": "AllowanceInsufficient"
                }
            }),
        ),
    ]
}

/// Fetches `GET /admin/orchestrator-health`, failing loudly on a non-OK
/// status so a broken endpoint can never read as "healthy".
async fn fetch_orchestrator_health(client: &Client) -> serde_json::Value {
    authenticated_get_json(client, "/admin/orchestrator-health").await
}

/// A burn spanning multiple orchestrator-custodied receipts: three separate
/// orchestrator mints create three receipts, and one redemption consumes all
/// of them through the on-chain walk.
#[tokio::test]
async fn orchestrator_burn_walks_multiple_receipts()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    harness::approve_orchestrator(&evm, orchestrator_address).await?;
    for nonce_seed in 1..=3u8 {
        harness::orchestrator_mint_to(
            &evm,
            orchestrator_address,
            &user_signer,
            tokens(10),
            B256::with_last_byte(nonce_seed),
        )
        .await?;
    }

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_multi_receipt.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let (config, _mock_subgraph) = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::setup_account(&client, user_wallet).await;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    assert_eq!(vault.balanceOf(user_wallet).call().await?, tokens(30));
    vault.transfer(bot_wallet, tokens(30)).send().await?.get_receipt().await?;

    harness::wait_for_burn(&vault, bot_wallet).await?;

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);

    let burned_logs =
        orchestrator.Burned_filter().from_block(0).query().await?;
    assert_eq!(
        burned_logs.len(),
        1,
        "exactly one orchestrator burn must have landed"
    );
    let (burned, _log) = &burned_logs[0];
    assert_eq!(burned.token, evm.vault_address);
    assert_eq!(burned.amount, tokens(30));
    // A fresh vault's three deposits hold receipt IDs 1..=3; walking all
    // three advances the per-token pointer past the last consumed receipt.
    assert_eq!(
        burned.nextBurnReceiptIdAfter,
        U256::from(4u8),
        "the walk must have consumed all three orchestrator receipts"
    );
    assert_eq!(
        orchestrator.nextBurnReceiptId(evm.vault_address).call().await?,
        burned.nextBurnReceiptIdAfter,
        "the per-token burn pointer must have advanced past the walk"
    );

    Ok(())
}

/// A redemption larger than the orchestrator's receipt pool is classified as
/// `InsufficientReceipts` before any submission and parked for manual
/// recovery — never auto-retried.
#[tokio::test]
async fn orchestrator_shortfall_is_classified_without_submission()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    harness::approve_orchestrator(&evm, orchestrator_address).await?;
    // The orchestrator custodies only 10 tokens of receipts...
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(10),
        B256::with_last_byte(1),
    )
    .await?;
    // ...but the user holds 30: 20 more minted with a receipt the user
    // (not the orchestrator) owns, simulating a drained pool.
    evm.grant_deposit_role(user_wallet).await?;
    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    vault
        .deposit(tokens(20), user_wallet, tokens(1), Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_shortfall.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let (config, _mock_subgraph) = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::setup_account(&client, user_wallet).await;

    assert_eq!(vault.balanceOf(user_wallet).call().await?, tokens(30));
    vault.transfer(bot_wallet, tokens(30)).send().await?.get_receipt().await?;

    let stuck_entry = wait_for_burn_failed_entry(&client).await?;
    assert!(
        stuck_entry["detail"]
            .as_str()
            .unwrap()
            .contains("receipts insufficient"),
        "stuck detail must surface the shortfall, got {stuck_entry}"
    );

    // Let several reconciler passes elapse: the classified failure must not
    // be auto-retried. The harness config polls every 500ms
    // (`create_config_with_db`'s `receipt_poll_interval`), so 1.5s covers
    // three passes.
    tokio::time::sleep(tokio::time::Duration::from_millis(1_500)).await;

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let burned_logs =
        orchestrator.Burned_filter().from_block(0).query().await?;
    assert!(burned_logs.is_empty(), "a shortfall burn must never be submitted");
    assert_eq!(
        vault.balanceOf(bot_wallet).call().await?,
        tokens(30),
        "the bot must still hold the transferred shares"
    );

    Ok(())
}

/// Startup recovery of an orchestrator burn that already landed on-chain:
/// the reconciler confirms the existing transaction instead of submitting a
/// second burn.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn orchestrator_recovery_confirms_landed_burn_without_resubmitting()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    // The burn that "already landed before the crash": mint 10 to the bot
    // itself, then burn it through the orchestrator directly.
    let bot_signer = PrivateKeySigner::from_bytes(&evm.private_key)?;
    harness::approve_orchestrator(&evm, orchestrator_address).await?;
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &bot_signer,
        tokens(10),
        B256::with_last_byte(1),
    )
    .await?;
    let provider = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &provider);
    let burn_receipt = orchestrator
        .burn(evm.vault_address, tokens(10), Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;
    let burn_tx_hash = burn_receipt.transaction_hash;

    // Seed the crashed redemption's history (events table only, per the e2e
    // setup exception): anchored to orchestrator mode, submitted with the
    // real burn's hash, never confirmed. Timestamps predate STUCK_THRESHOLD
    // so a redemption stuck in Burning would be operator-visible.
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let detected_tx_hash = B256::random();
    let issuer_request_id = format!("{detected_tx_hash:#x}");
    let old = "2020-01-01T00:00:00Z";
    let user_wallet =
        PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?.address();

    let control_tx_hash = B256::random();
    let control_id = format!("{control_tx_hash:#x}");
    let control_events = classified_control_events(
        &control_id,
        control_tx_hash,
        orchestrator_address,
        user_wallet,
        old,
    );

    let events = [
        (
            "RedemptionEvent::Detected",
            json!({
                "Detected": {
                    "issuer_request_id": issuer_request_id,
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "wallet": user_wallet,
                    "quantity": "10",
                    "tx_hash": detected_tx_hash,
                    "block_number": 1,
                    "detected_at": old,
                    "burn_mode": {
                        "Orchestrator": { "address": orchestrator_address }
                    }
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaCalled",
            json!({
                "AlpacaCalled": {
                    "issuer_request_id": issuer_request_id,
                    "tokenization_request_id": "tok-recovery-1",
                    "alpaca_quantity": "10",
                    "dust_quantity": "0",
                    "called_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaJournalCompleted",
            json!({
                "AlpacaJournalCompleted": {
                    "issuer_request_id": issuer_request_id,
                    "alpaca_journal_completed_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::OrchestratorBurnSubmitted",
            json!({
                "OrchestratorBurnSubmitted": {
                    "issuer_request_id": issuer_request_id,
                    "external_tx_id": format!("burn-{detected_tx_hash:#x}"),
                    "tx_id": { "hash": burn_tx_hash },
                    "submitted_at": old
                }
            }),
        ),
    ];

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;
    for (aggregate_id, aggregate_events) in
        [(&issuer_request_id, &events[..]), (&control_id, &control_events[..])]
    {
        for (sequence, (event_type, payload)) in
            (1i64..).zip(aggregate_events.iter())
        {
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(aggregate_id)
            .bind(sequence)
            .bind(event_type)
            .bind(payload.to_string())
            .execute(&pool)
            .await?;
        }
    }
    pool.close().await;

    let (config, _mock_subgraph) = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Startup recovery confirms the landed burn: once recovered, the
    // redemption leaves the recoverable states, so it no longer surfaces in
    // /admin/stuck despite its ancient (past-threshold) timestamps — while
    // the unresolvable control redemption still does, proving the absence is
    // recovery's doing rather than a seed that never replayed.
    wait_for_recovery_to_resolve(&client, &issuer_request_id, &control_id)
        .await?;

    let burned_logs =
        orchestrator.Burned_filter().from_block(0).query().await?;
    assert_eq!(
        burned_logs.len(),
        1,
        "recovery must confirm the landed burn, never submit a second one"
    );
    assert_eq!(
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider)
            .balanceOf(bot_wallet)
            .call()
            .await?,
        U256::ZERO
    );

    Ok(())
}

/// Kill-and-restart recovery of an in-flight orchestrator burn: a redemption
/// whose orchestrator burn was submitted and landed on-chain but crashed as
/// `BurnFailed` with the tx id retained and never confirmed. On restart,
/// startup recovery takes the Step-4 orchestrator confirm path — recording the
/// existing burn without resubmitting — so the redemption completes and drains
/// from `/admin/stuck`, exactly one `Burned` log exists, and
/// `/admin/orchestrator-health` reports the live orchestrator healthy with an
/// advanced `nextBurnReceiptId`.
#[tokio::test]
async fn orchestrator_crash_recovery_confirms_in_flight_burn_failed()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    // The burn that "landed before the crash": mint 10 to the bot, then burn
    // it through the orchestrator directly, capturing the real tx hash and
    // consuming the single receipt (id 1).
    let bot_signer = PrivateKeySigner::from_bytes(&evm.private_key)?;
    harness::approve_orchestrator(&evm, orchestrator_address).await?;
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &bot_signer,
        tokens(10),
        B256::with_last_byte(1),
    )
    .await?;
    let provider = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &provider);
    let burn_receipt = orchestrator
        .burn(evm.vault_address, tokens(10), Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;
    let burn_tx_hash = burn_receipt.transaction_hash;

    // Seed the crashed redemption's history (events table only, per the e2e
    // setup exception): anchored to orchestrator mode, failed with the real
    // burn's tx id retained but never confirmed — the exact in-flight state
    // the Step-4 orchestrator recovery must resolve. Timestamps predate
    // STUCK_THRESHOLD, though a `BurnFailed` is operator-visible regardless.
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_burn_failed_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let detected_tx_hash = B256::random();
    let issuer_request_id = format!("{detected_tx_hash:#x}");
    let old = "2020-01-01T00:00:00Z";
    let user_wallet =
        PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?.address();

    // Control redemption making the absence assertion below non-vacuous —
    // see `wait_for_recovery_to_resolve`.
    let control_tx_hash = B256::random();
    let control_id = format!("{control_tx_hash:#x}");
    let control_events = classified_control_events(
        &control_id,
        control_tx_hash,
        orchestrator_address,
        user_wallet,
        old,
    );

    let events = [
        (
            "RedemptionEvent::Detected",
            json!({
                "Detected": {
                    "issuer_request_id": issuer_request_id,
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "wallet": user_wallet,
                    "quantity": "10",
                    "tx_hash": detected_tx_hash,
                    "block_number": 1,
                    "detected_at": old,
                    "burn_mode": {
                        "Orchestrator": { "address": orchestrator_address }
                    }
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaCalled",
            json!({
                "AlpacaCalled": {
                    "issuer_request_id": issuer_request_id,
                    "tokenization_request_id": "tok-burn-failed-1",
                    "alpaca_quantity": "10",
                    "dust_quantity": "0",
                    "called_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::AlpacaJournalCompleted",
            json!({
                "AlpacaJournalCompleted": {
                    "issuer_request_id": issuer_request_id,
                    "alpaca_journal_completed_at": old
                }
            }),
        ),
        (
            "RedemptionEvent::BurningFailed",
            json!({
                "BurningFailed": {
                    "issuer_request_id": issuer_request_id,
                    "error": "orchestrator burn confirmation lost before crash",
                    "failed_at": old,
                    "tx_id": { "hash": burn_tx_hash },
                    "planned_burns": [],
                    "classification": "Unclassified"
                }
            }),
        ),
    ];

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;
    for (aggregate_id, aggregate_events) in
        [(&issuer_request_id, &events[..]), (&control_id, &control_events[..])]
    {
        for (sequence, (event_type, payload)) in
            (1i64..).zip(aggregate_events.iter())
        {
            sqlx::query(
                "
                INSERT INTO events (
                    aggregate_type,
                    aggregate_id,
                    sequence,
                    event_type,
                    event_version,
                    payload,
                    metadata
                )
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(aggregate_id)
            .bind(sequence)
            .bind(event_type)
            .bind(payload.to_string())
            .execute(&pool)
            .await?;
        }
    }
    pool.close().await;

    let (config, _mock_subgraph) = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Startup recovery confirms the landed burn: the redemption leaves the
    // recoverable states and no longer surfaces in /admin/stuck — while the
    // unresolvable control redemption still does, proving the absence is
    // recovery's doing rather than a seed that never replayed.
    wait_for_recovery_to_resolve(&client, &issuer_request_id, &control_id)
        .await?;

    let burned_logs =
        orchestrator.Burned_filter().from_block(0).query().await?;
    assert_eq!(
        burned_logs.len(),
        1,
        "recovery must confirm the landed burn, never submit a second one"
    );
    assert_eq!(
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider)
            .balanceOf(bot_wallet)
            .call()
            .await?,
        U256::ZERO
    );

    // The operator health surface reads the live orchestrator: healthy, with
    // the per-token burn pointer advanced past the single consumed receipt.
    let health = fetch_orchestrator_health(&client).await;
    let expected_addr = serde_json::to_value(orchestrator_address)?;
    let orchestrators = health["orchestrators"].as_array().unwrap();
    assert_eq!(orchestrators.len(), 1);
    assert_eq!(
        orchestrators[0]["vault_logic"]["status"], "expected",
        "the deployed orchestrator must report healthy"
    );
    let aapl = health["assets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|asset| asset["underlying"] == "AAPL")
        .unwrap();
    assert_eq!(aapl["vault_mode"], "orchestrator");
    assert_eq!(
        aapl["orchestrator"], expected_addr,
        "asset must report the orchestrator address it burns through"
    );
    assert_eq!(
        aapl["next_burn_receipt_id"]["status"], "available",
        "a live orchestrator's receipt pointer must be readable"
    );
    assert_eq!(
        aapl["next_burn_receipt_id"]["value"], "2",
        "nextBurnReceiptId must have advanced past the consumed receipt"
    );

    Ok(())
}

/// Two assets in one deployment — one orchestrator-mode, one vault-direct —
/// each redemption takes its own burn path, side by side. This is the
/// single-asset-pilot configuration.
#[tokio::test]
async fn mixed_mode_assets_each_take_their_own_burn_path()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let bot_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    // Primary vault: RKLB, orchestrator mode.
    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;
    harness::approve_orchestrator(&evm, orchestrator_address).await?;
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(10),
        B256::with_last_byte(1),
    )
    .await?;

    // Second vault: AAPL, vault-direct with the full mint flow.
    let (vault2_address, vault2_authorizer) =
        evm.deploy_additional_vault().await?;
    harness::setup_roles_on_vault(
        &evm,
        vault2_authorizer,
        vault2_address,
        user_wallet,
        bot_wallet,
    )
    .await?;

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mixed_mode.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "RKLB",
        "tRKLB",
    )
    .await?;
    harness::preseed_tokenized_asset(&db_url, vault2_address, "AAPL", "tAAPL")
        .await?;

    let (config, _mock_subgraph) = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("RKLB", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id: "alp-mint-aapl-mixed",
            quantity: "50.0",
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let orchestrator_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let direct_vault =
        OffchainAssetReceiptVaultInstance::new(vault2_address, &user_provider);

    let aapl_shares =
        harness::wait_for_shares(&direct_vault, user_wallet).await?;

    // Redeem both concurrently: the orchestrator asset and the vault-direct
    // asset each go down their own path.
    orchestrator_vault
        .transfer(bot_wallet, tokens(10))
        .send()
        .await?
        .get_receipt()
        .await?;
    direct_vault
        .transfer(bot_wallet, aapl_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_burn(&orchestrator_vault, bot_wallet).await?;
    harness::wait_for_burn(&direct_vault, bot_wallet).await?;

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let burned_logs =
        orchestrator.Burned_filter().from_block(0).query().await?;
    assert_eq!(
        burned_logs.len(),
        1,
        "only the orchestrator asset may burn through the orchestrator"
    );
    let (burned, _log) = &burned_logs[0];
    assert_eq!(
        burned.token, evm.vault_address,
        "the orchestrator burn must be for the orchestrator-mode asset"
    );
    assert_eq!(burned.amount, tokens(10));

    // The vault-direct asset burned through the untouched multicall path:
    // its shares are gone without any orchestrator involvement.
    assert_eq!(
        direct_vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "vault-direct burn must have completed"
    );
    assert_eq!(
        orchestrator_vault.balanceOf(bot_wallet).call().await?,
        U256::ZERO,
        "orchestrator burn must have completed"
    );

    Ok(())
}
