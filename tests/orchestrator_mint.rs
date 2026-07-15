//! End-to-end orchestrator-mode mint flows on Anvil.
//!
//! Assets whose configured `vault_mode` resolves to `Orchestrator` mint via a
//! single `ST0xOrchestrator.mint()` gated on a recipient authorization — the
//! liquidity bot (stood in for here by the recipient's key) signs the
//! orchestrator's EIP-712 `MintAuthV1` digest and delivers it through the
//! internal authorization endpoint before the mint can submit. These tests
//! drive the full HTTP service through the public API, with the orchestrator
//! deployed on Anvil and only Alpaca mocked.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256, b256};
use alloy::providers::Provider;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use st0x_issuance::bindings::IST0xOrchestratorV1;
use st0x_issuance::bindings::IST0xOrchestratorV1::IST0xOrchestratorV1Instance;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::bindings::Receipt::ReceiptInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{Network, initialize_rocket};

use crate::harness::{
    MintFlowRequest, TEST_API_KEY, authenticated_get_json, bot_provider,
    confirm_mint_journal, create_provider, fetch_stuck_entries,
    initiate_mint_request, orchestrator_vault_modes, tokens,
};

const USER_PRIVATE_KEY: B256 =
    b256!("0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");

/// Signs the orchestrator's own `mintAuthDigest(token, to, amount, nonce)`
/// with the recipient's key — exactly what the liquidity bot does before
/// delivering the authorization.
async fn signed_mint_authorization(
    evm: &LocalEvm,
    orchestrator_address: Address,
    recipient_signer: &PrivateKeySigner,
    amount: U256,
    nonce: B256,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    let reader = bot_provider(evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let digest = orchestrator
        .mintAuthDigest(
            evm.vault_address,
            recipient_signer.address(),
            amount,
            nonce,
        )
        .call()
        .await?;
    let signature = recipient_signer.sign_hash_sync(&digest)?;
    Ok(Bytes::from(signature.as_bytes().to_vec()))
}

/// Delivers the liquidity bot's authorization via
/// `POST /internal/mints/<tokenization_request_id>/authorization`.
async fn deliver_mint_authorization(
    client: &Client,
    tokenization_request_id: &str,
    nonce: B256,
    signature: &Bytes,
) {
    let status = deliver_mint_authorization_response(
        client,
        tokenization_request_id,
        nonce,
        signature,
    )
    .await;

    assert_eq!(
        status,
        rocket::http::Status::Ok,
        "the mint authorization delivery must be accepted"
    );
}

/// The non-asserting delivery, for scenarios proving a rejection.
async fn deliver_mint_authorization_response(
    client: &Client,
    tokenization_request_id: &str,
    nonce: B256,
    signature: &Bytes,
) -> rocket::http::Status {
    client
        .post(format!(
            "/internal/mints/{tokenization_request_id}/authorization"
        ))
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new("X-API-KEY", TEST_API_KEY))
        .remote(
            "127.0.0.1:8000".parse().expect("test client address must parse"),
        )
        .body(json!({ "nonce": nonce, "signature": signature }).to_string())
        .dispatch()
        .await
        .status()
}

/// Polls `GET /admin/stuck` until a `MintingFailed` entry appears.
async fn wait_for_minting_failed_entry(
    client: &Client,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let start = tokio::time::Instant::now();
    let timeout = tokio::time::Duration::from_secs(15);

    loop {
        if let Some(entry) = fetch_stuck_entries(client)
            .await
            .into_iter()
            .find(|entry| entry["state"] == "MintingFailed")
        {
            return Ok(entry);
        }

        if start.elapsed() >= timeout {
            return Err(
                "Timeout waiting for a MintingFailed stuck entry".into()
            );
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }
}

/// Seeds a mint's event history directly into the `events` table — the e2e
/// setup exception: mid-flight states like `Minting`/`TxSubmitted` cannot be
/// induced and then frozen through the public API alone.
async fn seed_mint_events(
    db_url: &str,
    issuer_request_id: &str,
    events: &[(&str, &str, serde_json::Value)],
) -> Result<(), Box<dyn std::error::Error>> {
    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(db_url).await?;

    for (sequence, (event_type, event_version, payload)) in
        (1i64..).zip(events.iter())
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
            VALUES ('Mint', ?, ?, ?, ?, ?, '{}')
            ",
        )
        .bind(issuer_request_id)
        .bind(sequence)
        .bind(event_type)
        .bind(event_version)
        .bind(payload.to_string())
        .execute(&pool)
        .await?;
    }
    pool.close().await;

    Ok(())
}

/// The seeded history shared by the recovery scenarios: an orchestrator-mode
/// mint that got its authorization and started minting before the "crash".
/// Timestamps predate the stuck threshold so an unrecovered mint would be
/// operator-visible.
fn seeded_mint_history(
    issuer_request_id: &str,
    tokenization_request_id: &str,
    orchestrator_address: Address,
    user_wallet: Address,
    nonce: B256,
) -> Vec<(&'static str, &'static str, serde_json::Value)> {
    let old = "2020-01-01T00:00:00Z";
    let client_id = uuid::Uuid::new_v4();

    vec![
        (
            "MintEvent::Initiated",
            "1.0",
            json!({
                "Initiated": {
                    "issuer_request_id": issuer_request_id,
                    "tokenization_request_id": tokenization_request_id,
                    "quantity": "50",
                    "underlying": "AAPL",
                    "token": "tAAPL",
                    "network": "base",
                    "client_id": client_id,
                    "wallet": user_wallet,
                    "initiated_at": old,
                    "mint_mode": {
                        "Orchestrator": { "address": orchestrator_address }
                    }
                }
            }),
        ),
        (
            "MintEvent::MintAuthorizationReceived",
            "1.0",
            json!({
                "MintAuthorizationReceived": {
                    "issuer_request_id": issuer_request_id,
                    "mint_authorization": {
                        "nonce": nonce,
                        "signature": Bytes::from(vec![0x42u8; 65])
                    },
                    "received_at": old
                }
            }),
        ),
        (
            "MintEvent::JournalConfirmed",
            "1.0",
            json!({
                "JournalConfirmed": {
                    "issuer_request_id": issuer_request_id,
                    "confirmed_at": old
                }
            }),
        ),
        (
            "MintEvent::MintingStarted",
            "1.0",
            json!({
                "MintingStarted": {
                    "issuer_request_id": issuer_request_id,
                    "started_at": old
                }
            }),
        ),
    ]
}

/// Sends an orchestrator mint transaction that is doomed to revert (its
/// nonce is already consumed) with explicit gas, so it skips estimation,
/// mines, and reverts on-chain — reproducing the race where the nonce is
/// consumed between broadcast and mining.
async fn send_reverting_orchestrator_mint(
    evm: &LocalEvm,
    orchestrator_address: Address,
    recipient_signer: &PrivateKeySigner,
    amount: U256,
    nonce: B256,
) -> Result<B256, Box<dyn std::error::Error>> {
    let signature = signed_mint_authorization(
        evm,
        orchestrator_address,
        recipient_signer,
        amount,
        nonce,
    )
    .await?;

    let provider = bot_provider(evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &provider);
    let auth = IST0xOrchestratorV1::MintAuthV1 { nonce, signature };

    let receipt = orchestrator
        .mint(
            evm.vault_address,
            recipient_signer.address(),
            amount,
            auth,
            Bytes::new(),
        )
        .gas(1_000_000)
        .send()
        .await?
        .get_receipt()
        .await?;
    assert!(
        !receipt.status(),
        "the crafted collision transaction must revert on-chain"
    );

    Ok(receipt.transaction_hash)
}

/// The full orchestrator mint flow: Alpaca requests the mint, the recipient
/// key signs the `MintAuthV1` digest and delivers it through the internal
/// endpoint, the journal confirms, and the service mints through
/// `orchestrator.mint()` — shares land in the recipient wallet, the
/// orchestrator custodies the receipt, and the Alpaca callback fires.
#[tokio::test]
async fn orchestrator_mint_end_to_end() -> Result<(), Box<dyn std::error::Error>>
{
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_e2e.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    let tokenization_request_id = "alp-orch-mint-1";
    let issuer_request_id = initiate_mint_request(
        &client,
        user_wallet,
        &MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id,
            quantity: "50.0",
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await?;

    let nonce = B256::with_last_byte(1);
    let signature = signed_mint_authorization(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(50),
        nonce,
    )
    .await?;
    deliver_mint_authorization(
        &client,
        tokenization_request_id,
        nonce,
        &signature,
    )
    .await;

    confirm_mint_journal(&client, tokenization_request_id, &issuer_request_id)
        .await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );

    let shares = harness::wait_for_shares(&vault, user_wallet).await?;
    assert_eq!(
        shares,
        tokens(50),
        "the recipient wallet must receive the full minted amount"
    );

    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(minted_logs.len(), 1, "exactly one Minted log must exist");
    let (minted, _log) = &minted_logs[0];
    assert_eq!(minted.token, evm.vault_address);
    assert_eq!(minted.to, user_wallet);
    assert_eq!(minted.amount, tokens(50));
    assert_eq!(minted.nonce, nonce);

    // The orchestrator — not the bot or the recipient — custodies the
    // receipt backing the minted shares (a fresh vault's first deposit
    // holds receipt id 1).
    let receipt_address = Address::from(vault.receipt().call().await?.0);
    let receipt_contract = ReceiptInstance::new(receipt_address, &reader);
    assert_eq!(
        receipt_contract
            .balanceOf(orchestrator_address, U256::from(1u8))
            .call()
            .await?,
        tokens(50),
        "the orchestrator must custody the mint receipt"
    );
    assert_eq!(
        receipt_contract.balanceOf(user_wallet, U256::from(1u8)).call().await?,
        U256::ZERO,
        "the recipient must hold shares only, never the receipt"
    );

    // The liquidity bot's cue for delivering an authorization: the public
    // status endpoint reports the asset's live-config mode.
    let status =
        authenticated_get_json(&client, "/tokenized-assets/AAPL/status").await;
    assert_eq!(
        status["vault_mode"], "orchestrator",
        "the status endpoint must report the orchestrator mode"
    );

    Ok(())
}

/// The recipient-authorization boundary on the live HTTP path: a signature
/// by the wrong key over the mint's own digest is rejected at delivery
/// (422), the mint stays waiting for its authorization even after the
/// journal confirms, and nothing ever reaches the chain.
#[tokio::test]
async fn wrong_signer_authorization_is_rejected_and_nothing_mints()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_wrong_signer.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    let tokenization_request_id = "alp-orch-wrong-signer-1";
    let issuer_request_id = initiate_mint_request(
        &client,
        user_wallet,
        &MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id,
            quantity: "50.0",
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await?;

    // The wrong key signs the RIGHT digest — the orchestrator's own
    // `mintAuthDigest` for this mint's recipient and amount — so the only
    // thing wrong with the delivery is who signed it.
    let wrong_signer = PrivateKeySigner::random();
    let nonce = B256::with_last_byte(9);
    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let digest = orchestrator
        .mintAuthDigest(evm.vault_address, user_wallet, tokens(50), nonce)
        .call()
        .await?;
    let wrong_signature =
        Bytes::from(wrong_signer.sign_hash_sync(&digest)?.as_bytes().to_vec());

    let status = deliver_mint_authorization_response(
        &client,
        tokenization_request_id,
        nonce,
        &wrong_signature,
    )
    .await;
    assert_eq!(
        status,
        rocket::http::Status::UnprocessableEntity,
        "a wrong-signer authorization must be rejected at delivery"
    );

    // Even with the journal confirmed, the unauthorized mint must defer —
    // never fall back to vault-direct, never submit.
    confirm_mint_journal(&client, tokenization_request_id, &issuer_request_id)
        .await?;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert!(
        minted_logs.is_empty(),
        "no Minted log may exist after a rejected authorization"
    );

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        U256::ZERO,
        "the recipient must receive nothing from a rejected authorization"
    );

    Ok(())
}

/// Production ordering the happy path cannot prove: Alpaca confirms the
/// journal before the liquidity bot delivers the recipient authorization.
/// After those externally observable operations occur in that order, the mint
/// must still complete end-to-end with exactly one `Minted` log and one Alpaca
/// callback.
#[tokio::test]
async fn authorization_after_journal_confirmation_still_completes()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_deferred.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;

    let tokenization_request_id = "alp-orch-deferred-1";
    let issuer_request_id = initiate_mint_request(
        &client,
        user_wallet,
        &MintFlowRequest {
            client_id: &link_body.client_id.to_string(),
            tokenization_request_id,
            quantity: "50.0",
            underlying: "AAPL",
            token: "tAAPL",
            network: Network::Base,
        },
    )
    .await?;

    // Complete the journal-confirmation request before delivering the
    // authorization through the authorization-delivery service endpoint.
    confirm_mint_journal(&client, tokenization_request_id, &issuer_request_id)
        .await?;

    // The later authorization request must still lead to one completed mint.
    let nonce = B256::with_last_byte(2);
    let signature = signed_mint_authorization(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(50),
        nonce,
    )
    .await?;
    deliver_mint_authorization(
        &client,
        tokenization_request_id,
        nonce,
        &signature,
    )
    .await;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let shares = harness::wait_for_shares(&vault, user_wallet).await?;
    assert_eq!(
        shares,
        tokens(50),
        "the deferred mint must complete once the authorization arrives"
    );

    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(
        minted_logs.len(),
        1,
        "the resumed mint must submit exactly once"
    );
    let (minted, _log) = &minted_logs[0];
    assert_eq!(minted.to, user_wallet);
    assert_eq!(minted.amount, tokens(50));
    assert_eq!(minted.nonce, nonce);

    Ok(())
}

/// Kill-and-restart recovery of an orchestrator mint that already landed
/// on-chain: the seeded mint crashed in `Minting` after its authorization
/// arrived, while its transaction actually minted. Startup recovery
/// full-matches the landed `Minted` log and completes the mint without ever
/// submitting again — exactly one `Minted` log, the double-mint guard.
#[tokio::test]
async fn recovery_of_landed_orchestrator_mint_is_a_noop()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    // The mint that "already landed before the crash": consumes
    // (user, nonce 7) for exactly the seeded amount and token.
    let nonce = B256::with_last_byte(7);
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(50),
        nonce,
    )
    .await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_recovery.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let issuer_request_id = uuid::Uuid::new_v4().to_string();
    seed_mint_events(
        &db_url,
        &issuer_request_id,
        &seeded_mint_history(
            &issuer_request_id,
            "alp-orch-recovery-1",
            orchestrator_address,
            user_wallet,
            nonce,
        ),
    )
    .await?;

    let reader = bot_provider(&evm).await?;
    // Recovery's landed-log scan deliberately excludes the newest blocks as
    // a reorg buffer; mine past it so the landing is inside the window, as
    // it would be by the time real recovery re-drives a parked mint.
    reader
        .raw_request::<_, serde_json::Value>("anvil_mine".into(), (40u64,))
        .await?;
    // The bot's on-chain transaction count is the assertion that can
    // actually fail on a resubmission: a second `mint()` with the consumed
    // nonce would revert without emitting a log, so the Minted-log count
    // below stays 1 no matter what the bot does — only the account nonce
    // betrays a broadcast.
    let bot_nonce_before =
        reader.get_transaction_count(evm.wallet_address).await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let _client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    // Startup recovery completes the mint through the landed-log full match;
    // the Alpaca callback is the completion signal.
    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(
        minted_logs.len(),
        1,
        "recovery must record the landed mint, never submit a second one"
    );
    assert_eq!(
        reader.get_transaction_count(evm.wallet_address).await?,
        bot_nonce_before,
        "recovery must never broadcast anything — not even a doomed \
         resubmission that would revert"
    );

    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &reader);
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        tokens(50),
        "the recipient balance must be exactly the single landed mint"
    );

    Ok(())
}

/// A `(to, nonce)` collision with a different amount must never falsely
/// complete: the seeded mint's submitted transaction reverted with
/// `NonceReplayed` because another mint consumed the pair for a smaller
/// amount. Recovery full-matches the landed log, finds the amount differs,
/// and fails the mint as `NonceConsumedByOtherMint` for manual
/// reconciliation — visible in `/admin/stuck`, never auto-retried, no
/// callback.
#[tokio::test]
async fn nonce_collision_fails_for_manual_reconciliation()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    // A different mint consumes (user, nonce 9) first — for 10 tokens, not
    // the 50 the seeded mint is for.
    let nonce = B256::with_last_byte(9);
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(10),
        nonce,
    )
    .await?;

    // The seeded mint's own transaction: signed over 50 tokens with the now
    // consumed nonce, sent with explicit gas so it mines and reverts
    // on-chain (estimation would refuse to broadcast it).
    let reverted_tx_hash = send_reverting_orchestrator_mint(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(50),
        nonce,
    )
    .await?;

    // Move the other mint's landing past the reorg-confirmation buffer so
    // the confirm-side scan can SEE it — the proven-mismatch verdict
    // requires the log; an in-buffer landing would (correctly) read as the
    // inconclusive `NonceReplayUnresolved` instead.
    bot_provider(&evm)
        .await?
        .raw_request::<_, serde_json::Value>("anvil_mine".into(), (40u64,))
        .await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_collision.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let issuer_request_id = uuid::Uuid::new_v4().to_string();
    let mut events = seeded_mint_history(
        &issuer_request_id,
        "alp-orch-collision-1",
        orchestrator_address,
        user_wallet,
        nonce,
    );
    events.push((
        "MintEvent::MintTxSubmitted",
        "2.0",
        json!({
            "MintTxSubmitted": {
                "issuer_request_id": issuer_request_id,
                "external_tx_id": format!("mint-{issuer_request_id}"),
                "tx_id": { "hash": reverted_tx_hash },
                "submitted_at": "2020-01-01T00:00:00Z"
            }
        }),
    ));
    seed_mint_events(&db_url, &issuer_request_id, &events).await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let entry = wait_for_minting_failed_entry(&client).await?;
    assert_eq!(entry["aggregate_id"], issuer_request_id.as_str());
    let detail =
        entry["detail"].as_str().expect("the stuck detail must be a string");
    // The typed classification is the retry-exclusion signal — asserting it
    // (not just the revert text) distinguishes the never-auto-retried park
    // from a retryable `Unclassified` failure, which would pass the same
    // stuck-state check while meaning the opposite.
    assert!(
        detail.contains("NonceConsumedByOtherMint"),
        "the stuck detail must carry the typed classification, got: {detail}"
    );
    assert!(
        detail.contains("NonceReplayed"),
        "the stuck detail must carry the decoded revert, got: {detail}"
    );

    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(
        minted_logs.len(),
        1,
        "the collision must never be completed or resubmitted"
    );
    let (minted, _log) = &minted_logs[0];
    assert_eq!(
        minted.amount,
        tokens(10),
        "the only landed mint must be the other mint's"
    );

    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &reader);
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        tokens(10),
        "the seeded mint's 50 tokens must never have minted"
    );
    assert_eq!(
        mint_callback_mock.calls(),
        0,
        "a nonce-collision failure must never fire the Alpaca callback"
    );

    Ok(())
}

/// The inconclusive-replay lifecycle only e2e can prove, across a service
/// restart: the mint's own landing sits INSIDE the reorg-confirmation
/// buffer, so the pre-submit guard finds the nonce consumed with no visible
/// log and parks the mint as `NonceReplayUnresolved` (stuck-visible, no
/// callback, nothing broadcast). Once the chain moves past the buffer, the
/// next service start's recovery reconciles with the widened window,
/// full-matches the landing, and resolves the mint forward to its callback
/// — still without ever broadcasting anything.
#[tokio::test]
async fn unresolved_replay_parks_then_reconciles_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    evm.grant_certify_role(evm.wallet_address).await?;
    evm.certify_vault(U256::MAX).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;

    // The mint's OWN landing: consumes (user, nonce 7) for exactly the
    // seeded amount and token, moments before the "crash" — so it is still
    // inside the reorg-confirmation buffer when recovery first re-drives.
    let nonce = B256::with_last_byte(7);
    harness::orchestrator_mint_to(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(50),
        nonce,
    )
    .await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_unresolved.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    let issuer_request_id = uuid::Uuid::new_v4().to_string();
    seed_mint_events(
        &db_url,
        &issuer_request_id,
        &seeded_mint_history(
            &issuer_request_id,
            "alp-orch-unresolved-1",
            orchestrator_address,
            user_wallet,
            nonce,
        ),
    )
    .await?;

    let reader = bot_provider(&evm).await?;
    let bot_nonce_before =
        reader.get_transaction_count(evm.wallet_address).await?;

    // Phase 1: recovery re-drives the seeded mint; the guard finds the
    // nonce consumed with no log inside the buffered window and parks the
    // mint unresolved.
    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let entry = wait_for_minting_failed_entry(&client).await?;
    assert_eq!(entry["aggregate_id"], issuer_request_id.as_str());
    let detail =
        entry["detail"].as_str().expect("the stuck detail must be a string");
    assert!(
        detail.contains("NonceReplayUnresolved"),
        "the parked mint must carry the inconclusive classification, got: \
         {detail}"
    );
    assert_eq!(
        mint_callback_mock.calls(),
        0,
        "an unresolved replay must not fire the Alpaca callback"
    );

    // "Crash" the first service, then let the chain move past the
    // reorg-confirmation buffer — as it would within a minute on Base.
    drop(client);
    reader
        .raw_request::<_, serde_json::Value>("anvil_mine".into(), (40u64,))
        .await?;

    // Phase 2: the restarted service's recovery reconciles with the widened
    // window, full-matches the landing, and drives the mint to its
    // callback.
    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("AAPL", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let _client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    harness::wait_for_mock_hits(&mint_callback_mock, 1).await?;

    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(
        minted_logs.len(),
        1,
        "reconciliation must resolve the landed mint, never submit again"
    );
    assert_eq!(
        reader.get_transaction_count(evm.wallet_address).await?,
        bot_nonce_before,
        "neither phase may broadcast anything from the bot wallet"
    );

    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &reader);
    assert_eq!(
        vault.balanceOf(user_wallet).call().await?,
        tokens(50),
        "the recipient balance must be exactly the single landed mint"
    );

    Ok(())
}

/// Two assets in one deployment — one orchestrator-mode, one vault-direct —
/// each mint takes its own path, side by side. This is the
/// single-asset-pilot configuration.
#[tokio::test]
async fn mixed_mode_assets_each_take_their_own_mint_path()
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

    // Second vault: AAPL, vault-direct with the untouched mint flow.
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

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("orchestrator_mint_mixed.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());
    harness::preseed_tokenized_asset(
        &db_url,
        evm.vault_address,
        "RKLB",
        "tRKLB",
    )
    .await?;
    harness::preseed_tokenized_asset(&db_url, vault2_address, "AAPL", "tAAPL")
        .await?;

    let config = harness::create_config_with_vault_modes(
        &db_url,
        &mock_alpaca,
        &evm,
        orchestrator_vault_modes("RKLB", orchestrator_address),
    )?;
    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;
    let client_id = link_body.client_id.to_string();

    // Orchestrator-mode RKLB mint: initiate, deliver the authorization,
    // confirm.
    let rklb_tokenization_id = "alp-mint-rklb-mixed";
    let rklb_issuer_id = initiate_mint_request(
        &client,
        user_wallet,
        &MintFlowRequest {
            client_id: &client_id,
            tokenization_request_id: rklb_tokenization_id,
            quantity: "25.0",
            underlying: "RKLB",
            token: "tRKLB",
            network: Network::Base,
        },
    )
    .await?;
    let nonce = B256::with_last_byte(2);
    let signature = signed_mint_authorization(
        &evm,
        orchestrator_address,
        &user_signer,
        tokens(25),
        nonce,
    )
    .await?;
    deliver_mint_authorization(
        &client,
        rklb_tokenization_id,
        nonce,
        &signature,
    )
    .await;
    confirm_mint_journal(&client, rklb_tokenization_id, &rklb_issuer_id)
        .await?;

    // Vault-direct AAPL mint: the plain flow, no authorization anywhere.
    harness::perform_mint_and_confirm_with(
        &client,
        user_wallet,
        harness::MintFlowRequest {
            client_id: &client_id,
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

    assert_eq!(
        harness::wait_for_shares(&orchestrator_vault, user_wallet).await?,
        tokens(25),
        "the orchestrator-mode mint must land its full amount"
    );
    assert_eq!(
        harness::wait_for_shares(&direct_vault, user_wallet).await?,
        tokens(50),
        "the vault-direct mint must land its full amount"
    );
    harness::wait_for_mock_hits(&mint_callback_mock, 2).await?;

    // Only the orchestrator-mode asset minted through the orchestrator; the
    // vault-direct asset never touched it.
    let reader = bot_provider(&evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let minted_logs =
        orchestrator.Minted_filter().from_block(0).query().await?;
    assert_eq!(
        minted_logs.len(),
        1,
        "only the orchestrator asset may mint through the orchestrator"
    );
    let (minted, _log) = &minted_logs[0];
    assert_eq!(
        minted.token, evm.vault_address,
        "the orchestrator mint must be for the orchestrator-mode asset"
    );
    assert_eq!(minted.amount, tokens(25));
    assert_eq!(minted.to, user_wallet);

    // Each asset's status endpoint reports its own mode — the liquidity
    // bot's cue for which mints need an authorization.
    let rklb_status =
        authenticated_get_json(&client, "/tokenized-assets/RKLB/status").await;
    assert_eq!(rklb_status["vault_mode"], "orchestrator");
    let aapl_status =
        authenticated_get_json(&client, "/tokenized-assets/AAPL/status").await;
    assert_eq!(aapl_status["vault_mode"], "vault_direct");

    Ok(())
}
