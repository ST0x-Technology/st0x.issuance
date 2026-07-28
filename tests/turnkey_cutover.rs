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
use st0x_issuance::receipt_inventory::migration::{
    CorroboratedRecipient, MigrationOutcome, VaultIdentity,
    migrate_vault_receipts,
};
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::underlying::UnderlyingSymbol;
use st0x_issuance::{Config, SignerConfig, initialize_rocket};
use std::path::{Path, PathBuf};

use crate::harness::create_provider;

/// The single asset every cutover scenario seeds and migrates. The migration
/// identity must name the seeded listing: quiescence is scoped by underlying,
/// so a mismatched symbol resolves against an asset absent from the database
/// and gates nothing.
const CUTOVER_UNDERLYING: &str = "AAPL";
const CUTOVER_TOKEN: &str = "tAAPL";

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

/// Waits until the running service has recorded the vault's receipt inventory.
///
/// The migration refuses outright on an empty inventory, so without this the
/// test would race the backfiller and fail for a reason unrelated to what it is
/// proving.
async fn wait_for_receipt_in_inventory(
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for _ in 0..100 {
        // Deliberately not filtered by aggregate id: reproducing the
        // `{chain_id}:{vault}` key here would couple this wait to an encoding
        // that has already been re-keyed once by migration. This scenario uses
        // a single vault, so the aggregate type alone identifies it — but the
        // event type matters: startup reconciliation records custody as soon
        // as the service boots, so "any inventory event" fires long before the
        // deposit this wait is actually for has been discovered.
        let recorded: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'ReceiptInventory'
              AND event_type = 'ReceiptInventoryEvent::Discovered'
            ",
        )
        .fetch_one(&pool)
        .await?;

        if recorded > 0 {
            pool.close().await;
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    pool.close().await;
    Err("no receipt ever entered the inventory".into())
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

/// Migrates custody against the outgoing service's database — driving the
/// migration engine the operator's tooling runs, exercised as an operator
/// would.
///
/// Note what this does and does not cover. The database snapshot handed to the
/// replacement service is taken *before* this runs, so the migration here acts
/// on the retiring store. That is deliberate and mirrors production: the
/// outgoing service is already stopped (see the module-level requirement in
/// `src/receipt_inventory/migration.rs`), and the quiescence gate checks real
/// persisted state — reservations and in-flight work — not service liveness.
///
/// The migration is run twice: the second run stands in for the operator losing
/// the terminal between the transaction confirming and success being recorded,
/// and must be a no-op rather than a second transfer or a divergence failure.
async fn run_operator_migration(
    database_url: &str,
    provider: &impl Provider,
    chain_id: u64,
    vault: Address,
    outgoing: Address,
    incoming: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let underlying = UnderlyingSymbol::new("AAPL")?;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    let identity = VaultIdentity { chain_id, vault, underlying: &underlying };
    let recipient = CorroboratedRecipient::verify(&provider, incoming).await?;

    let outcome =
        migrate_vault_receipts(&pool, provider, identity, outgoing, recipient)
            .await?;
    assert!(
        matches!(outcome, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the migration must report moving receipts, got {outcome:?}"
    );

    let rerun =
        migrate_vault_receipts(&pool, provider, identity, outgoing, recipient)
            .await?;
    assert!(
        matches!(rerun, MigrationOutcome::AlreadyMigrated { receipts } if receipts > 0),
        "re-running a completed migration must be a no-op, got {rerun:?}"
    );

    pool.close().await;

    Ok(())
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

/// Proves the Base-only wallet-rotation sequence with multichain code present
/// but additional chains disabled, driving the operator's real command: migrate
/// receipt custody over a stopped, quiescent deployment.
///
/// The local signers model the old Fireblocks-controlled address and the new
/// Turnkey-controlled address; the custodian's own credential and policy
/// checks remain staging gates outside this test's scope. This scenario proves
/// the application-owned custody, persistence, restart, checkpoint, and
/// transaction-idempotency invariants.
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
        CUTOVER_UNDERLYING,
        CUTOVER_TOKEN,
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

    run_operator_migration(
        &databases.fireblocks_url,
        &fireblocks_provider,
        evm.chain_id,
        evm.vault_address,
        fireblocks_wallet,
        turnkey_wallet,
    )
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

/// Proves receipt custody can be moved back after a completed migration, so an
/// aborted cutover is recoverable.
///
/// This matters because the two directions are authorized by different systems:
/// the outbound leg is signed by the retiring custodian, the inbound leg by the
/// replacement. If the reverse were not permitted on-chain, an aborted cutover
/// would strand custody with no way back. The vault's
/// `authorizeReceiptTransfer3` requires no role in either direction, only live
/// certification and no blocking owner freeze — this asserts that symmetry
/// against a real vault rather than trusting the reading.
///
/// No migration-specific code is exercised in reverse: the same
/// `migrate_vault_receipts` runs with the wallets swapped, which is the point.
#[tokio::test]
async fn test_receipt_custody_can_be_rolled_back_to_the_outgoing_wallet()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CutoverDatabases::in_directory(temp_dir.path());
    let fireblocks_wallet = evm.wallet_address;
    let turnkey_signer = PrivateKeySigner::random();
    let turnkey_wallet = turnkey_signer.address();
    assert_ne!(fireblocks_wallet, turnkey_wallet);

    let fireblocks_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let gas = U256::from(10) * U256::from(10).pow(U256::from(18));
    fireblocks_provider.anvil_set_balance(turnkey_wallet, gas).await?;

    harness::preseed_tokenized_asset(
        &databases.fireblocks_url,
        evm.vault_address,
        CUTOVER_UNDERLYING,
        CUTOVER_TOKEN,
    )
    .await?;
    evm.grant_deposit_role(fireblocks_wallet).await?;
    evm.grant_certify_role(fireblocks_wallet).await?;
    evm.certify_vault(U256::MAX).await?;

    // Run the service only long enough to discover the receipt into inventory,
    // which the migration cross-checks against the chain.
    let (config, _subgraph) = harness::create_config_with_db(
        &databases.fireblocks_url,
        &mock_alpaca,
        &evm,
    )?;
    let client = start_service(config).await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &fireblocks_provider,
    );
    let receipt_shares = U256::from(40) * U256::from(10).pow(U256::from(18));
    let share_ratio = U256::from(10).pow(U256::from(18));
    let deposit = vault
        .deposit(receipt_shares, fireblocks_wallet, share_ratio, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;
    let receipt_id = deposit
        .inner
        .logs()
        .iter()
        .find_map(|log| {
            OffchainAssetReceiptVault::Deposit::decode_log(&log.inner).ok()
        })
        .ok_or("deposit must emit a Deposit event")?
        .id;
    let receipt_contract: Address = vault.receipt().call().await?.0.into();
    let receipt = ReceiptInstance::new(receipt_contract, &fireblocks_provider);
    wait_for_receipt_in_inventory(&databases.fireblocks_url).await?;
    client.terminate().await;

    let underlying = UnderlyingSymbol::new("AAPL")?;
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&databases.fireblocks_url)
        .await?;

    let identity = VaultIdentity {
        chain_id: evm.chain_id,
        vault: evm.vault_address,
        underlying: &underlying,
    };
    let incoming =
        CorroboratedRecipient::verify(&fireblocks_provider, turnkey_wallet)
            .await?;
    let forward = migrate_vault_receipts(
        &pool,
        &fireblocks_provider,
        identity,
        fireblocks_wallet,
        incoming,
    )
    .await?;
    assert!(
        matches!(forward, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the forward migration must move receipts, got {forward:?}"
    );
    assert_eq!(
        receipt.balanceOf(turnkey_wallet, receipt_id).call().await?,
        receipt_shares,
        "the incoming wallet must hold the receipt after the forward move"
    );

    // The rollback: same command, signer and recipient swapped.
    let turnkey_provider = create_provider()
        .wallet(EthereumWallet::from(turnkey_signer))
        .connect(&evm.endpoint)
        .await?;
    let rollback_destination =
        CorroboratedRecipient::verify(&turnkey_provider, fireblocks_wallet)
            .await?;
    let rolled_back = migrate_vault_receipts(
        &pool,
        &turnkey_provider,
        identity,
        turnkey_wallet,
        rollback_destination,
    )
    .await?;

    assert!(
        matches!(rolled_back, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the rollback must move receipts back, got {rolled_back:?}"
    );
    assert_eq!(
        receipt.balanceOf(fireblocks_wallet, receipt_id).call().await?,
        receipt_shares,
        "custody must return to the outgoing wallet after rollback"
    );
    assert_eq!(
        receipt.balanceOf(turnkey_wallet, receipt_id).call().await?,
        U256::ZERO,
        "the incoming wallet must retain nothing after rollback"
    );

    pool.close().await;

    Ok(())
}

/// Proves the complete single-asset rehearsal, end to end, in the order the
/// operator will run it: cutover, operate, reverse, resume.
///
/// 1. The outgoing service mints a receipt (pre-cutover custody).
/// 2. Stop; custody migrates forward; the replacement service starts.
/// 3. The replacement service performs one canary redemption of the
///    pre-cutover receipt and one canary mint — the two directions of the flow
///    against the new custody.
/// 4. Stop; custody rolls back — including the canary mint's receipt, which
///    only exists because of step 3 and which the resumed service could not
///    burn against if it were left behind.
/// 5. The original service resumes on the same event history and redeems the
///    canary, proving the rehearsal ends with a fully operational deployment
///    on the outgoing wallet, not merely with balances returned.
///
/// The rollback deliberately carries the database forward rather than
/// restoring the pre-cutover backup: the replacement service performed real
/// writes (a redemption and a mint), and discarding them would fork history.
/// Only custody and the signer configuration reverse.
#[tokio::test]
async fn test_single_asset_rehearsal_operates_reverses_and_resumes()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;
    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CutoverDatabases::in_directory(temp_dir.path());
    let resumed_path = temp_dir.path().join("fireblocks-resumed.db");
    let resumed_url = format!("sqlite:{}?mode=rwc", resumed_path.display());

    let fireblocks_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let turnkey_signer = PrivateKeySigner::random();
    let turnkey_private_key: B256 = turnkey_signer.to_bytes();
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
        CUTOVER_UNDERLYING,
        CUTOVER_TOKEN,
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
    fireblocks_client.terminate().await;

    // Forward cutover. The migration runs against the same database the
    // replacement service will use, as it does in production, so the custody
    // events are part of the history the service starts from.
    snapshot_database(&databases.fireblocks_url, &databases.turnkey_path)
        .await?;
    run_operator_migration(
        &databases.turnkey_url,
        &fireblocks_provider,
        evm.chain_id,
        evm.vault_address,
        fireblocks_wallet,
        turnkey_wallet,
    )
    .await?;

    let (mut turnkey_config, _turnkey_subgraph) =
        harness::create_config_with_db(
            &databases.turnkey_url,
            &mock_alpaca,
            &evm,
        )?;
    turnkey_config.signer = SignerConfig::Local(turnkey_private_key);
    let turnkey_client = start_service(turnkey_config).await?;

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
    turnkey_client.terminate().await;

    // The reversal. The canary mint left its receipt with the incoming wallet,
    // so the rollback has real, newly created custody to return — not just the
    // original receipts.
    snapshot_database(&databases.turnkey_url, &resumed_path).await?;
    run_operator_migration(
        &resumed_url,
        &create_provider()
            .wallet(EthereumWallet::from(turnkey_signer))
            .connect(&evm.endpoint)
            .await?,
        evm.chain_id,
        evm.vault_address,
        turnkey_wallet,
        fireblocks_wallet,
    )
    .await?;

    resume_on_outgoing_wallet_and_redeem_canary(
        &resumed_url,
        &evm,
        &mock_alpaca,
        &user_signer,
        fireblocks_wallet,
        &redeem_mock,
        &poll_mock,
    )
    .await?;

    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "the rehearsal must produce exactly two mints: pre-cutover and canary"
    );
    assert_eq!(
        redeem_mock.calls_async().await,
        2,
        "the rehearsal must produce exactly two redemptions: canary and \
         post-rollback"
    );

    Ok(())
}

/// The rehearsal's final act: the outgoing wallet's service resumes on the
/// rolled-back state and redeems the canary receipt minted by the replacement,
/// proving the reversal restored an operational deployment.
async fn resume_on_outgoing_wallet_and_redeem_canary(
    database_url: &str,
    evm: &LocalEvm,
    mock_alpaca: &MockServer,
    user_signer: &PrivateKeySigner,
    fireblocks_wallet: Address,
    redeem_mock: &Mock<'_>,
    poll_mock: &Mock<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (resumed_config, _resumed_subgraph) =
        harness::create_config_with_db(database_url, mock_alpaca, evm)?;
    let resumed_client = start_service(resumed_config).await?;

    let user_provider = create_provider()
        .wallet(EthereumWallet::from(user_signer.clone()))
        .connect(&evm.endpoint)
        .await?;
    let user_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &user_provider,
    );
    let canary_shares =
        user_vault.balanceOf(user_signer.address()).call().await?;
    assert!(
        canary_shares > U256::ZERO,
        "the user must still hold the canary shares minted on the replacement"
    );

    user_vault
        .transfer(fireblocks_wallet, canary_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hits(redeem_mock, 2).await?;
    harness::wait_for_mock_hits(poll_mock, 2).await?;
    harness::wait_for_burn(&user_vault, fireblocks_wallet).await?;
    assert_eq!(
        user_vault.balanceOf(user_signer.address()).call().await?,
        U256::ZERO,
        "the resumed service must redeem the canary shares in full"
    );

    resumed_client.terminate().await;

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
/// operator sequence, not the code, is what prevents this: pause the authorized
/// participant's redemptions and stop the service for the window.
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
        CUTOVER_UNDERLYING,
        CUTOVER_TOKEN,
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
