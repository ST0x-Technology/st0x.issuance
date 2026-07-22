#![allow(clippy::unwrap_used)]

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::providers::Provider;
use alloy::providers::ext::AnvilApi;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::SolEvent;
use httpmock::Mock;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use sqlx::sqlite::SqlitePoolOptions;
use st0x_issuance::bindings::OffchainAssetReceiptVault::{
    self, OffchainAssetReceiptVaultInstance,
};
use st0x_issuance::bindings::Receipt::ReceiptInstance;
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::{Config, SignerConfig, initialize_rocket};
use std::path::{Path, PathBuf};

use crate::harness::create_provider;

struct CutoverDatabases {
    fireblocks_url: String,
    turnkey_path: PathBuf,
    turnkey_url: String,
}

struct PreCutoverMint {
    client_id: String,
    shares: U256,
}

impl CutoverDatabases {
    fn in_directory(directory: &Path) -> Self {
        let fireblocks_path = directory.join("fireblocks-cutover-source.db");
        let turnkey_path = directory.join("turnkey-cutover.db");

        Self {
            fireblocks_url: format!(
                "sqlite:{}?mode=rwc",
                fireblocks_path.display()
            ),
            turnkey_url: format!("sqlite:{}?mode=rwc", turnkey_path.display()),
            turnkey_path,
        }
    }
}

async fn snapshot_database(
    source_database_url: &str,
    destination_path: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(source_database_url)
        .await?;

    sqlx::query("VACUUM INTO ?")
        .bind(destination_path.to_string_lossy().as_ref())
        .execute(&pool)
        .await?;
    pool.close().await;

    Ok(())
}

async fn mint_before_cutover(
    client: &Client,
    evm: &LocalEvm,
    user_signer: &PrivateKeySigner,
    user_wallet: Address,
    mint_callback_mock: &Mock<'_>,
) -> Result<PreCutoverMint, Box<dyn std::error::Error>> {
    let account = harness::setup_account(client, user_wallet).await;
    harness::perform_mint_and_confirm(
        client,
        user_wallet,
        &account.client_id.to_string(),
        "turnkey-cutover-mint",
        "50.0",
    )
    .await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer.clone()))
        .connect(&evm.endpoint)
        .await?;
    let user_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let minted_shares =
        harness::wait_for_shares(&user_vault, user_wallet).await?;
    harness::wait_for_mock_hits(mint_callback_mock, 1).await?;

    Ok(PreCutoverMint {
        client_id: account.client_id.to_string(),
        shares: minted_shares,
    })
}

async fn mint_after_cutover(
    client: &Client,
    user_wallet: Address,
    client_id: &str,
    mint_callback_mock: &Mock<'_>,
) -> Result<U256, Box<dyn std::error::Error>> {
    harness::perform_mint_and_confirm(
        client,
        user_wallet,
        client_id,
        "turnkey-cutover-canary",
        "25.0",
    )
    .await?;
    harness::wait_for_mock_hits(mint_callback_mock, 2).await?;

    Ok(U256::from(25) * U256::from(10).pow(U256::from(18)))
}

struct PostCutoverCanaries<'context, 'server> {
    client: &'context Client,
    evm: &'context LocalEvm,
    user_signer: &'context PrivateKeySigner,
    user_wallet: Address,
    turnkey_wallet: Address,
    pre_cutover_mint: &'context PreCutoverMint,
    mint_callback_mock: &'context Mock<'server>,
    redeem_mock: &'context Mock<'server>,
    poll_mock: &'context Mock<'server>,
}

impl PostCutoverCanaries<'_, '_> {
    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let user_provider = create_provider()
            .wallet(EthereumWallet::from(self.user_signer.clone()))
            .connect(&self.evm.endpoint)
            .await?;
        let user_vault = OffchainAssetReceiptVaultInstance::new(
            self.evm.vault_address,
            &user_provider,
        );
        let canary_shares = mint_after_cutover(
            self.client,
            self.user_wallet,
            &self.pre_cutover_mint.client_id,
            self.mint_callback_mock,
        )
        .await?;
        assert_eq!(
            user_vault.balanceOf(self.user_wallet).call().await?,
            self.pre_cutover_mint.shares + canary_shares,
            "the new wallet must complete exactly one canary mint"
        );

        user_vault
            .transfer(self.turnkey_wallet, self.pre_cutover_mint.shares)
            .send()
            .await?
            .get_receipt()
            .await?;

        harness::wait_for_mock_hits(self.redeem_mock, 1).await?;
        harness::wait_for_mock_hit(self.poll_mock).await?;
        harness::wait_for_burn(&user_vault, self.turnkey_wallet).await?;
        assert_eq!(
            user_vault.balanceOf(self.user_wallet).call().await?,
            canary_shares,
            "only the post-cutover canary shares must remain after redemption"
        );

        Ok(())
    }
}

async fn start_service(
    config: Config,
) -> Result<Client, Box<dyn std::error::Error>> {
    let rocket = initialize_rocket(config).await?;
    Ok(Client::tracked(rocket).await?)
}

async fn assert_additional_chains_disabled(client: &Client) {
    let response = client
        .post("/tokenized-assets")
        .header(rocket::http::ContentType::JSON)
        .header(rocket::http::Header::new(
            "X-API-KEY",
            "test-key-12345678901234567890123456",
        ))
        .remote("127.0.0.1:8000".parse().unwrap())
        .body(
            serde_json::json!({
                "underlying": "TSLA",
                "token": "tTSLA",
                "network": "ethereum",
                "vault": Address::random()
            })
            .to_string(),
        )
        .dispatch()
        .await;

    assert_eq!(response.status(), rocket::http::Status::UnprocessableEntity);
}

async fn setup_cutover_roles(
    evm: &LocalEvm,
    user_wallet: Address,
    fireblocks_wallet: Address,
    turnkey_wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_deposit_role(turnkey_wallet).await?;
    evm.grant_withdraw_role(fireblocks_wallet).await?;
    evm.grant_withdraw_role(turnkey_wallet).await?;
    evm.grant_certify_role(fireblocks_wallet).await?;
    evm.certify_vault(U256::MAX).await?;

    Ok(())
}

async fn single_deposit_for_owner(
    provider: &impl Provider,
    vault_address: Address,
    owner: Address,
) -> Result<(U256, U256, Bytes), Box<dyn std::error::Error>> {
    let vault = OffchainAssetReceiptVaultInstance::new(vault_address, provider);
    let deposit_logs =
        provider.get_logs(&vault.Deposit_filter().from_block(0).filter).await?;
    let deposits = deposit_logs
        .iter()
        .filter_map(|log| {
            OffchainAssetReceiptVault::Deposit::decode_log(&log.inner).ok()
        })
        .filter(|deposit| deposit.owner == owner)
        .collect::<Vec<_>>();

    assert_eq!(deposits.len(), 1, "the wallet must own exactly one deposit");

    Ok((
        deposits[0].id,
        deposits[0].shares,
        deposits[0].receiptInformation.clone(),
    ))
}

/// Proves the Base-only wallet-rotation sequence used by the Turnkey
/// migration runbook with multichain code present but additional chains disabled.
///
/// The local signers model the old Fireblocks-controlled address and the new
/// Turnkey-controlled address. Provider-specific credential and policy checks
/// remain staging gates; this scenario proves the application-owned custody,
/// persistence, restart, checkpoint, and transaction-idempotency invariants.
#[tokio::test]
async fn test_wallet_cutover_redeems_pre_cutover_receipt_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();

    let fireblocks_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let turnkey_signer = PrivateKeySigner::random();
    let turnkey_private_key: B256 = turnkey_signer.to_bytes();
    let turnkey_wallet = turnkey_signer.address();
    assert_ne!(
        fireblocks_wallet, turnkey_wallet,
        "the cutover must model migration to a new Turnkey address"
    );
    let fireblocks_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let test_gas_balance = U256::from(10) * U256::from(10).pow(U256::from(18));
    fireblocks_provider
        .anvil_set_balance(user_wallet, test_gas_balance)
        .await?;
    fireblocks_provider
        .anvil_set_balance(turnkey_wallet, test_gas_balance)
        .await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let databases = CutoverDatabases::in_directory(temp_dir.path());

    harness::preseed_tokenized_asset(
        &databases.fireblocks_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;

    setup_cutover_roles(&evm, user_wallet, fireblocks_wallet, turnkey_wallet)
        .await?;

    let (fireblocks_config, _fireblocks_subgraph) =
        harness::create_config_with_db(
            &databases.fireblocks_url,
            &mock_alpaca,
            &evm,
        )?;
    let fireblocks_client = start_service(fireblocks_config).await?;

    let pre_cutover_mint = mint_before_cutover(
        &fireblocks_client,
        &evm,
        &user_signer,
        user_wallet,
        &mint_callback_mock,
    )
    .await?;

    let fireblocks_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &fireblocks_provider,
    );
    let (minted_receipt_id, minted_receipt_shares, _) =
        single_deposit_for_owner(
            &fireblocks_provider,
            evm.vault_address,
            fireblocks_wallet,
        )
        .await?;

    let receipt_contract_address =
        fireblocks_vault.receipt().call().await?.0.into();
    let fireblocks_receipt =
        ReceiptInstance::new(receipt_contract_address, &fireblocks_provider);
    assert_eq!(
        fireblocks_receipt
            .balanceOf(fireblocks_wallet, minted_receipt_id)
            .call()
            .await?,
        minted_receipt_shares,
        "the old wallet must custody the historical mint receipt before cutover"
    );

    fireblocks_client.terminate().await;
    // Process exit kills the old service's detached workers in production. A
    // consistent snapshot gives the replacement service the same persisted
    // state without leaving those in-process test workers attached to its DB.
    snapshot_database(&databases.fireblocks_url, &databases.turnkey_path)
        .await?;

    fireblocks_receipt
        .safeTransferFrom(
            fireblocks_wallet,
            turnkey_wallet,
            minted_receipt_id,
            minted_receipt_shares,
            Bytes::new(),
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    assert_eq!(
        fireblocks_receipt
            .balanceOf(fireblocks_wallet, minted_receipt_id)
            .call()
            .await?,
        U256::ZERO,
        "the old wallet must not retain migrated receipt custody"
    );
    assert_eq!(
        fireblocks_receipt
            .balanceOf(turnkey_wallet, minted_receipt_id)
            .call()
            .await?,
        minted_receipt_shares,
        "the new wallet must receive the historical receipt before startup"
    );

    let (mut turnkey_config, _turnkey_subgraph) =
        harness::create_config_with_db(
            &databases.turnkey_url,
            &mock_alpaca,
            &evm,
        )?;
    turnkey_config.signer = SignerConfig::Local(turnkey_private_key);
    let turnkey_client = start_service(turnkey_config).await?;
    assert_additional_chains_disabled(&turnkey_client).await;

    PostCutoverCanaries {
        client: &turnkey_client,
        evm: &evm,
        user_signer: &user_signer,
        user_wallet,
        turnkey_wallet,
        pre_cutover_mint: &pre_cutover_mint,
        mint_callback_mock: &mint_callback_mock,
        redeem_mock: &redeem_mock,
        poll_mock: &poll_mock,
    }
    .run()
    .await?;
    assert_eq!(
        fireblocks_receipt
            .balanceOf(turnkey_wallet, minted_receipt_id)
            .call()
            .await?,
        U256::ZERO,
        "the historical receipt must be consumed by the new wallet"
    );
    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "restart plus one canary must produce exactly two mint callbacks"
    );
    assert_eq!(
        redeem_mock.calls_async().await,
        1,
        "cutover must create exactly one redemption"
    );

    let _turnkey_canary_receipt = single_deposit_for_owner(
        &fireblocks_provider,
        evm.vault_address,
        turnkey_wallet,
    )
    .await?;
    let withdraw_logs = fireblocks_provider
        .get_logs(&fireblocks_vault.Withdraw_filter().from_block(0).filter)
        .await?;
    let turnkey_withdrawals = withdraw_logs
        .iter()
        .filter_map(|log| {
            OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner).ok()
        })
        .filter(|withdrawal| withdrawal.owner == turnkey_wallet)
        .count();

    assert_eq!(
        turnkey_withdrawals, 1,
        "the new wallet must submit exactly one historical-receipt burn"
    );
    assert_eq!(
        fireblocks_provider.get_transaction_count(turnkey_wallet).await?,
        2,
        "the new wallet must sign exactly one canary mint and one historical-receipt burn"
    );

    turnkey_client.terminate().await;

    Ok(())
}

/// Proves what the running service does when the custody step is skipped, which
/// is the failure this whole cutover is sequenced to avoid.
///
/// The service is started against the un-migrated state and driven through the
/// public path, rather than the burn being called directly: a direct call would
/// only demonstrate that the vault reverts without a receipt, which is a
/// property of the vault contract and not of this codebase.
///
/// What it pins down is worse than a revert. The redemption flow calls Alpaca
/// before it burns, so the participant's shares are journaled back while the
/// tokens remain outstanding and unbacked, and `BurnFailed` recovery will not
/// auto-fail the redemption because the on-chain *share* balance is present —
/// it is the *receipt* that is missing. Nothing here recovers on its own. The
/// operator sequence, not the code, is what prevents this: freeze the asset so
/// no redemption arrives, and stop the service for the window.
#[tokio::test]
async fn test_cutover_without_receipt_transfer_cannot_burn_historical_shares()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CutoverDatabases::in_directory(temp_dir.path());
    let fireblocks_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let turnkey_signer = PrivateKeySigner::random();
    let turnkey_wallet = turnkey_signer.address();
    assert_ne!(fireblocks_wallet, turnkey_wallet);

    let fireblocks_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let test_gas_balance = U256::from(10) * U256::from(10).pow(U256::from(18));
    fireblocks_provider
        .anvil_set_balance(user_wallet, test_gas_balance)
        .await?;
    fireblocks_provider
        .anvil_set_balance(turnkey_wallet, test_gas_balance)
        .await?;

    harness::preseed_tokenized_asset(
        &databases.fireblocks_url,
        evm.vault_address,
        "AAPL",
        "tAAPL",
    )
    .await?;
    setup_cutover_roles(&evm, user_wallet, fireblocks_wallet, turnkey_wallet)
        .await?;
    let (fireblocks_config, _fireblocks_subgraph) =
        harness::create_config_with_db(
            &databases.fireblocks_url,
            &mock_alpaca,
            &evm,
        )?;
    let fireblocks_client = start_service(fireblocks_config).await?;
    let pre_cutover_mint = mint_before_cutover(
        &fireblocks_client,
        &evm,
        &user_signer,
        user_wallet,
        &mint_callback_mock,
    )
    .await?;
    let (receipt_id, receipt_shares, _receipt_information) =
        single_deposit_for_owner(
            &fireblocks_provider,
            evm.vault_address,
            fireblocks_wallet,
        )
        .await?;
    fireblocks_client.terminate().await;
    snapshot_database(&databases.fireblocks_url, &databases.turnkey_path)
        .await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer))
        .connect(&evm.endpoint)
        .await?;
    let user_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    user_vault
        .transfer(turnkey_wallet, pre_cutover_mint.shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Bring up the replacement service against the un-migrated state, exactly
    // as an operator who skipped the custody step would.
    let (mut turnkey_config, _turnkey_subgraph) =
        harness::create_config_with_db(
            &databases.turnkey_url,
            &mock_alpaca,
            &evm,
        )?;
    turnkey_config.signer =
        SignerConfig::Local(B256::from(turnkey_signer.to_bytes()));
    let turnkey_client = start_service(turnkey_config).await?;

    let turnkey_provider = create_provider()
        .wallet(EthereumWallet::from(turnkey_signer))
        .connect(&evm.endpoint)
        .await?;
    let turnkey_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &turnkey_provider,
    );

    // The service detects the transfer and calls Alpaca before it can burn, so
    // the redeem callback firing is the point of no return: shares are journaled
    // back to the participant while the tokens are still outstanding.
    harness::wait_for_mock_hits(&redeem_mock, 1).await?;

    // Give the burn every chance to land before asserting it did not. Without
    // the receipt it reverts, and `BurnFailed` recovery retries against an
    // on-chain share balance that is present, so it never auto-fails either.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let withdraw_logs = turnkey_provider
        .get_logs(&turnkey_vault.Withdraw_filter().from_block(0).filter)
        .await?;
    let turnkey_withdrawals = withdraw_logs
        .iter()
        .filter_map(|log| {
            OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner).ok()
        })
        .filter(|withdrawal| withdrawal.owner == turnkey_wallet)
        .count();
    assert_eq!(
        turnkey_withdrawals, 0,
        "no burn may succeed without receipt custody"
    );

    assert_eq!(
        turnkey_vault.balanceOf(turnkey_wallet).call().await?,
        pre_cutover_mint.shares,
        "the shares must remain outstanding and unburned"
    );

    let receipt_contract_address =
        turnkey_vault.receipt().call().await?.0.into();
    let receipt =
        ReceiptInstance::new(receipt_contract_address, &turnkey_provider);
    assert_eq!(
        receipt.balanceOf(fireblocks_wallet, receipt_id).call().await?,
        receipt_shares,
        "the historical receipt must remain in old-wallet custody"
    );
    assert_eq!(
        receipt.balanceOf(turnkey_wallet, receipt_id).call().await?,
        U256::ZERO,
        "the new wallet must still lack the historical receipt"
    );

    turnkey_client.terminate().await;

    Ok(())
}
