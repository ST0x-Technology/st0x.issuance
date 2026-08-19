//! Multichain orchestrator routing on two Anvil chains.
//!
//! Each network carries its own orchestrator deployment at its own address
//! (`[orchestrator.addresses]` in the TOML config). This suite proves the
//! per-network keying end to end: two chains, two orchestrators at
//! deliberately different addresses, one orchestrator-mode mint AND one
//! redemption per chain — every operation must land on its own network's
//! orchestrator and leave the other chain untouched.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256, b256};
use alloy::providers::ProviderBuilder;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use st0x_issuance::bindings::IST0xOrchestratorV1::IST0xOrchestratorV1Instance;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::test_utils::{LocalEvm, ROLE_DEPOSIT, ROLE_WITHDRAW};
use st0x_issuance::{
    ETHEREUM_TEST_CHAIN_ID, Network, VaultModeConfig, VaultModeKind,
    initialize_rocket,
};
use std::collections::HashMap;

use crate::harness::{
    MintFlowRequest, TEST_API_KEY, confirm_mint_journal, initiate_mint_request,
    tokens,
};

const USER_PRIVATE_KEY: B256 =
    b256!("0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");

/// Signs the orchestrator's own `mintAuthDigest(token, to, amount, nonce)`
/// with the recipient's key, read from the given chain — the digest carries
/// that chain's EIP-712 domain (chainId + verifyingContract), so an
/// authorization is bound to one network's orchestrator.
async fn signed_mint_authorization(
    evm: &LocalEvm,
    orchestrator_address: Address,
    token: Address,
    recipient_signer: &PrivateKeySigner,
    amount: U256,
    nonce: B256,
) -> Result<Bytes, Box<dyn std::error::Error>> {
    let reader = harness::bot_provider(evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    let digest = orchestrator
        .mintAuthDigest(token, recipient_signer.address(), amount, nonce)
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
    let status = client
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
        .status();

    assert_eq!(
        status,
        rocket::http::Status::Ok,
        "the mint authorization delivery must be accepted"
    );
}

/// Fetches the decoded `Minted` logs at `orchestrator_address` on the given
/// chain.
async fn minted_logs(
    evm: &LocalEvm,
    orchestrator_address: Address,
) -> Result<
    Vec<st0x_issuance::bindings::IST0xOrchestratorV1::Minted>,
    Box<dyn std::error::Error>,
> {
    let reader = harness::bot_provider(evm).await?;
    let orchestrator =
        IST0xOrchestratorV1Instance::new(orchestrator_address, &reader);
    Ok(orchestrator
        .Minted_filter()
        .from_block(0)
        .query()
        .await?
        .into_iter()
        .map(|(minted, _log)| minted)
        .collect())
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn orchestrator_operations_route_to_each_networks_own_orchestrator()
-> Result<(), Box<dyn std::error::Error>> {
    let base_evm = LocalEvm::new().await?;
    let eth_evm = LocalEvm::with_chain_id(ETHEREUM_TEST_CHAIN_ID).await?;

    let user_signer = PrivateKeySigner::from_bytes(&USER_PRIVATE_KEY)?;
    let user_wallet = user_signer.address();

    base_evm.grant_certify_role(base_evm.wallet_address).await?;
    base_evm.certify_vault(U256::MAX).await?;
    let base_orchestrator = base_evm.deploy_orchestrator().await?;

    // Both Anvils deploy from the same key, so the default vault addresses
    // collide across chains — and the service refuses one vault address on
    // two networks. The Ethereum asset therefore lives on a freshly deployed
    // vault; its extra deployments also shift the deployer nonce, so the
    // Ethereum orchestrator lands at a genuinely different address and the
    // per-network keying under test cannot hold vacuously.
    let (eth_vault_address, eth_authorizer_address) =
        eth_evm.deploy_additional_vault().await?;
    assert_ne!(eth_vault_address, base_evm.vault_address);
    harness::setup_roles_on_vault(
        &eth_evm,
        eth_authorizer_address,
        eth_vault_address,
        user_wallet,
        eth_evm.wallet_address,
    )
    .await?;

    let eth_orchestrator = eth_evm.deploy_orchestrator().await?;
    assert_ne!(
        base_orchestrator, eth_orchestrator,
        "the two chains must carry orchestrators at different addresses"
    );
    // `deploy_orchestrator` wires DEPOSIT/WITHDRAW on the DEFAULT vault's
    // authorizer only; the Ethereum asset's vault needs the same grants.
    eth_evm
        .grant_role_on_authorizer(
            eth_authorizer_address,
            ROLE_DEPOSIT,
            eth_orchestrator,
        )
        .await?;
    eth_evm
        .grant_role_on_authorizer(
            eth_authorizer_address,
            ROLE_WITHDRAW,
            eth_orchestrator,
        )
        .await?;

    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (_redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("multichain_orchestrator.db");
    let db_url = format!("sqlite:{}?mode=rwc", db_path.display());

    let pool =
        SqlitePoolOptions::new().max_connections(1).connect(&db_url).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    harness::preseed_tokenized_asset_into_pool(
        &pool,
        base_evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;
    harness::preseed_tokenized_asset_into_pool_with_network(
        &pool,
        eth_vault_address,
        "TSLA",
        "tTSLA",
        Network::Ethereum,
    )
    .await?;
    pool.close().await;

    let mut config = harness::create_multichain_config_with_db(
        &db_url,
        &mock_alpaca,
        &base_evm,
        &eth_evm,
    )?;
    config.vault_mode_config = VaultModeConfig::new(
        HashMap::from([
            ("AAPL".to_string(), VaultModeKind::Orchestrator),
            ("TSLA".to_string(), VaultModeKind::Orchestrator),
        ]),
        VaultModeKind::VaultDirect,
        HashMap::from([
            (Network::Base, base_orchestrator),
            (Network::Ethereum, eth_orchestrator),
        ]),
    );

    let rocket = initialize_rocket(config).await?;
    let client = rocket::local::asynchronous::Client::tracked(rocket).await?;

    let link_body = harness::setup_account(&client, user_wallet).await;
    let client_id = link_body.client_id.to_string();

    for (evm, orchestrator, vault_address, request, amount, nonce) in [
        (
            &base_evm,
            base_orchestrator,
            base_evm.vault_address,
            MintFlowRequest {
                client_id: &client_id,
                tokenization_request_id: "alp-orch-base-aapl",
                quantity: "50.0",
                underlying: "AAPL",
                token: "tAAPL",
                network: Network::Base,
            },
            tokens(50),
            B256::with_last_byte(1),
        ),
        (
            &eth_evm,
            eth_orchestrator,
            eth_vault_address,
            MintFlowRequest {
                client_id: &client_id,
                tokenization_request_id: "alp-orch-eth-tsla",
                quantity: "10.0",
                underlying: "TSLA",
                token: "tTSLA",
                network: Network::Ethereum,
            },
            tokens(10),
            B256::with_last_byte(2),
        ),
    ] {
        let tokenization_request_id = request.tokenization_request_id;
        let issuer_request_id =
            initiate_mint_request(&client, user_wallet, &request).await?;

        let signature = signed_mint_authorization(
            evm,
            orchestrator,
            vault_address,
            &user_signer,
            amount,
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

        confirm_mint_journal(
            &client,
            tokenization_request_id,
            &issuer_request_id,
        )
        .await?;
    }

    harness::wait_for_mock_hits(&mint_callback_mock, 2).await?;

    for (evm, vault_address, expected_amount) in [
        (&base_evm, base_evm.vault_address, tokens(50)),
        (&eth_evm, eth_vault_address, tokens(10)),
    ] {
        let user_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(user_signer.clone()))
            .connect(&evm.endpoint)
            .await?;
        let vault = OffchainAssetReceiptVaultInstance::new(
            vault_address,
            &user_provider,
        );
        let shares = harness::wait_for_shares(&vault, user_wallet).await?;
        assert_eq!(
            shares, expected_amount,
            "each chain's mint must credit exactly its own quantity"
        );
    }

    // The keying proof: each chain's own orchestrator carries exactly one
    // Minted log, and that log's token/recipient/amount are the ones THIS
    // chain's mint requested — a swapped or shared address would change an
    // observed value here (the wrong chain's token or amount, or a second
    // log on one orchestrator).
    for (evm, orchestrator, expected_token, expected_amount) in [
        (&base_evm, base_orchestrator, base_evm.vault_address, tokens(50)),
        (&eth_evm, eth_orchestrator, eth_vault_address, tokens(10)),
    ] {
        let logs = minted_logs(evm, orchestrator).await?;
        assert_eq!(
            logs.len(),
            1,
            "each chain's orchestrator must carry exactly its own mint"
        );
        assert_eq!(
            logs[0].token, expected_token,
            "the Minted log must name this chain's vault token"
        );
        assert_eq!(
            logs[0].to, user_wallet,
            "the Minted log must name the requested recipient"
        );
        assert_eq!(
            logs[0].amount, expected_amount,
            "the Minted log must carry this chain's requested amount"
        );
    }

    // Redemption legs: the burn path resolves its mode (and orchestrator
    // address) from the DETECTED asset's network, so each chain's redemption
    // must burn through that chain's own orchestrator. The mints above left
    // the receipts in each orchestrator's custody, so the burn walk has
    // inventory on both chains.
    for (evm, orchestrator, vault_address, amount) in [
        (&base_evm, base_orchestrator, base_evm.vault_address, tokens(50)),
        (&eth_evm, eth_orchestrator, eth_vault_address, tokens(10)),
    ] {
        // The bot wallet approves THIS chain's orchestrator to pull the
        // shares it is about to receive.
        let bot = harness::bot_provider(evm).await?;
        OffchainAssetReceiptVaultInstance::new(vault_address, &bot)
            .approve(orchestrator, U256::MAX)
            .send()
            .await?
            .get_receipt()
            .await?;

        // The user sends the minted shares to the redemption (bot) wallet;
        // the running service detects, calls Alpaca, and burns.
        let user_provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(user_signer.clone()))
            .connect(&evm.endpoint)
            .await?;
        let vault = OffchainAssetReceiptVaultInstance::new(
            vault_address,
            &user_provider,
        );
        vault
            .transfer(evm.wallet_address, amount)
            .send()
            .await?
            .get_receipt()
            .await?;

        harness::wait_for_burn(&vault, evm.wallet_address).await?;
    }

    // Same non-vacuous shape as the mint proof: each chain's orchestrator
    // carries exactly one Burned log naming that chain's own token and
    // amount — a swapped or shared address would double a log or show the
    // wrong chain's facts.
    for (evm, orchestrator, expected_token, expected_amount) in [
        (&base_evm, base_orchestrator, base_evm.vault_address, tokens(50)),
        (&eth_evm, eth_orchestrator, eth_vault_address, tokens(10)),
    ] {
        let reader = harness::bot_provider(evm).await?;
        let contract = IST0xOrchestratorV1Instance::new(orchestrator, &reader);
        let burned_logs =
            contract.Burned_filter().from_block(0).query().await?;
        assert_eq!(
            burned_logs.len(),
            1,
            "each chain's orchestrator must carry exactly its own burn"
        );
        let (burned, _log) = &burned_logs[0];
        assert_eq!(
            burned.token, expected_token,
            "the Burned log must name this chain's vault token"
        );
        assert_eq!(
            burned.amount, expected_amount,
            "the Burned log must carry this chain's redeemed amount"
        );
    }

    Ok(())
}
