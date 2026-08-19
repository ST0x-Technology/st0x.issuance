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
use st0x_issuance::bindings::ST0xOrchestrator;
use st0x_issuance::receipt_inventory::migration::{
    CorroboratedRecipient, MigrationOutcome, RecipientKind, VaultIdentity,
    confirm_custody_holder, migrate_vault_receipts, recorded_custody_holder,
    recorded_migration_origin,
};
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::tokenized_asset::UnderlyingSymbol;
use st0x_issuance::{Config, Network, SignerConfig, initialize_rocket};
use std::path::{Path, PathBuf};

use crate::harness::create_provider;

/// The single asset every migration scenario seeds and migrates. The migration
/// identity must name the seeded listing: quiescence is scoped by underlying,
/// so a mismatched symbol resolves against an asset absent from the database
/// and gates nothing.
const CUSTODY_UNDERLYING: &str = "AAPL";
const CUSTODY_TOKEN: &str = "tAAPL";

struct CustodyDatabases {
    outgoing_url: String,
    incoming_path: PathBuf,
    incoming_url: String,
}

struct HistoricalMint {
    issuer_request_id: String,
    client_id: String,
    shares: U256,
}

struct CanaryMint {
    issuer_request_id: String,
    shares: U256,
}

impl CustodyDatabases {
    fn in_directory(directory: &Path) -> Self {
        let outgoing_path = directory.join("outgoing-migration-source.db");
        let incoming_path = directory.join("incoming-migration.db");

        Self {
            outgoing_url: format!(
                "sqlite:{}?mode=rwc",
                outgoing_path.display()
            ),
            incoming_url: format!(
                "sqlite:{}?mode=rwc",
                incoming_path.display()
            ),
            incoming_path,
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
    wait_for_discovered_receipts(database_url, 1).await
}

/// Waits until the running service has discovered at least `minimum`
/// receipts into the vault's inventory — the multi-receipt counterpart of
/// [`wait_for_receipt_in_inventory`], for scenarios seeding more deposits
/// than one.
async fn wait_for_discovered_receipts(
    database_url: &str,
    minimum: i64,
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

        if recorded >= minimum {
            pool.close().await;
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    pool.close().await;
    Err(format!("fewer than {minimum} receipts ever entered the inventory")
        .into())
}

/// Waits until the running service has settled a redemption's burn
/// reservation. `wait_for_burn` only proves the burn landed on-chain; the
/// service records `BurnSettled` afterwards, and the migration's quiescence
/// gate refuses while a reservation is live — so stopping the service (or
/// snapshotting its database) before this wait races the settlement reactor.
async fn wait_for_settled_burn(
    database_url: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for _ in 0..100 {
        let settled: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE aggregate_type = 'ReceiptInventory'
              AND event_type = 'ReceiptInventoryEvent::BurnSettled'
            ",
        )
        .fetch_one(&pool)
        .await?;

        if settled > 0 {
            pool.close().await;
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    pool.close().await;
    Err("the redemption's burn reservation never settled".into())
}

/// Waits until the expected mint is terminal in the event store.
///
/// The callback mock observes the external request before `SendCallbackJob`
/// records `MintCompleted`. Stopping the service or running the migration after
/// the mock fires can therefore race the event commit and make a completed mint
/// appear in-flight to the quiescence gate.
async fn wait_for_completed_mint(
    database_url: &str,
    issuer_request_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await?;

    for _ in 0..100 {
        let completed: bool = sqlx::query_scalar(
            "
            SELECT EXISTS (
                SELECT 1
                FROM events
                WHERE aggregate_type = 'Mint'
                  AND aggregate_id = ?
                  AND event_type = 'MintEvent::MintCompleted'
            )
            ",
        )
        .bind(issuer_request_id)
        .fetch_one(&pool)
        .await?;

        if completed {
            pool.close().await;
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    pool.close().await;
    Err(format!("mint {issuer_request_id} never completed").into())
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

async fn mint_before_migration(
    client: &Client,
    evm: &LocalEvm,
    user_signer: &PrivateKeySigner,
    user_wallet: Address,
    mint_callback_mock: &Mock<'_>,
    db_url: &str,
) -> Result<HistoricalMint, Box<dyn std::error::Error>> {
    let account = harness::setup_account(client, user_wallet).await;
    let issuer_request_id = harness::perform_mint_and_confirm(
        client,
        user_wallet,
        &account.client_id.to_string(),
        "incoming-migration-mint",
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
    // The migration's quiescence gate counts non-terminal mints, so the
    // service must not shut down until the terminal event is COMMITTED —
    // the mock hit above only proves the callback request arrived.
    harness::wait_for_mint_completed(db_url, &issuer_request_id).await?;

    Ok(HistoricalMint {
        issuer_request_id,
        client_id: account.client_id.to_string(),
        shares: minted_shares,
    })
}

async fn mint_after_migration(
    client: &Client,
    user_wallet: Address,
    client_id: &str,
    mint_callback_mock: &Mock<'_>,
    db_url: &str,
) -> Result<CanaryMint, Box<dyn std::error::Error>> {
    const CANARY_QUANTITY: u64 = 25;

    let issuer_request_id = harness::perform_mint_and_confirm(
        client,
        user_wallet,
        client_id,
        "incoming-migration-canary",
        &format!("{CANARY_QUANTITY}.0"),
    )
    .await?;
    harness::wait_for_mock_hits(mint_callback_mock, 2).await?;
    // Same terminality wait as `mint_before_cutover`: the rehearsal
    // reverses the migration after the canaries, and its quiescence gate
    // must find this mint terminal, not racing its final event write.
    harness::wait_for_mint_completed(db_url, &issuer_request_id).await?;

    Ok(CanaryMint {
        issuer_request_id,
        shares: U256::from(CANARY_QUANTITY)
            * U256::from(10).pow(U256::from(18)),
    })
}

struct PostMigrationCanaries<'context, 'server> {
    client: &'context Client,
    evm: &'context LocalEvm,
    user_signer: &'context PrivateKeySigner,
    user_wallet: Address,
    incoming_wallet: Address,
    historical_mint: &'context HistoricalMint,
    mint_callback_mock: &'context Mock<'server>,
    redeem_mock: &'context Mock<'server>,
    poll_mock: &'context Mock<'server>,
    db_url: &'context str,
}

impl PostMigrationCanaries<'_, '_> {
    /// The canary redemption runs FIRST: it burns against a just-migrated
    /// historical receipt, which is the actual proof that custody moved and
    /// the inventory reconciled against the new holder. The canary mint only
    /// proves the new signer can sign fresh work, so it runs second.
    async fn run(&self) -> Result<CanaryMint, Box<dyn std::error::Error>> {
        let user_provider = create_provider()
            .wallet(EthereumWallet::from(self.user_signer.clone()))
            .connect(&self.evm.endpoint)
            .await?;
        let user_vault = OffchainAssetReceiptVaultInstance::new(
            self.evm.vault_address,
            &user_provider,
        );

        user_vault
            .transfer(self.incoming_wallet, self.historical_mint.shares)
            .send()
            .await?
            .get_receipt()
            .await?;

        harness::wait_for_mock_hits(self.redeem_mock, 1).await?;
        harness::wait_for_mock_hit(self.poll_mock).await?;
        harness::wait_for_burn(&user_vault, self.incoming_wallet).await?;
        assert_eq!(
            user_vault.balanceOf(self.user_wallet).call().await?,
            U256::ZERO,
            "the canary redemption must consume every historical share"
        );

        let canary_mint = mint_after_migration(
            self.client,
            self.user_wallet,
            &self.historical_mint.client_id,
            self.mint_callback_mock,
            self.db_url,
        )
        .await?;
        assert_eq!(
            user_vault.balanceOf(self.user_wallet).call().await?,
            canary_mint.shares,
            "the new wallet must complete exactly one canary mint"
        );

        Ok(canary_mint)
    }
}

/// Migrates custody against the given database through the vendor-neutral
/// engine. The migration deliberately touches no freeze state: freezing means
/// "corporate action in progress", and the window is controlled operationally
/// (liquidity rebalancing paused, service stopped) instead.
///
/// The migration is run twice: the second run stands in for the operator losing
/// the terminal between the transaction confirming and success being recorded,
/// and must be a no-op rather than a second transfer or a divergence failure.
async fn run_custody_migration(
    database_url: &str,
    provider: &impl Provider,
    chain_id: u64,
    vault: Address,
    outgoing: Address,
    incoming: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    let underlying: UnderlyingSymbol = CUSTODY_UNDERLYING.parse()?;
    let incoming =
        CorroboratedRecipient::verify(provider, outgoing, incoming).await?;

    let identity = VaultIdentity::verify(
        &pool,
        provider,
        Network::Base,
        chain_id,
        vault,
        &underlying,
    )
    .await?;
    // No bootstrap call is allowed here: success proves production startup
    // reconciliation already recorded the outgoing holder.
    let outcome =
        migrate_vault_receipts(&pool, provider, identity, incoming).await?;
    assert!(
        matches!(outcome, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the migration must report moving receipts, got {outcome:?}"
    );

    let rerun =
        migrate_vault_receipts(&pool, provider, identity, incoming).await?;
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

async fn setup_custody_roles(
    evm: &LocalEvm,
    user_wallet: Address,
    outgoing_wallet: Address,
    incoming_wallet: Address,
) -> Result<(), Box<dyn std::error::Error>> {
    evm.grant_deposit_role(user_wallet).await?;
    evm.grant_deposit_role(incoming_wallet).await?;
    evm.grant_withdraw_role(outgoing_wallet).await?;
    evm.grant_withdraw_role(incoming_wallet).await?;
    evm.grant_certify_role(outgoing_wallet).await?;
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
/// but additional chains disabled, driving the operator's real sequence:
/// stop the service, migrate receipt custody, start the replacement.
///
/// The local signers model outgoing and incoming custodians. Production
/// credential and policy checks remain operator and deployment concerns outside
/// this engine test.
/// This scenario proves the application-owned custody, persistence, restart,
/// checkpoint, and transaction-idempotency invariants.
#[tokio::test]
async fn test_receipt_custody_migration_redeems_historical_receipt_after_restart()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let mock_alpaca = MockServer::start();

    let outgoing_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let incoming_signer = PrivateKeySigner::random();
    let incoming_private_key: B256 = incoming_signer.to_bytes();
    let incoming_wallet = incoming_signer.address();
    assert_ne!(
        outgoing_wallet, incoming_wallet,
        "the migration must model migration to a new incoming address"
    );
    let outgoing_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let test_gas_balance = U256::from(10) * U256::from(10).pow(U256::from(18));
    outgoing_provider.anvil_set_balance(user_wallet, test_gas_balance).await?;
    outgoing_provider
        .anvil_set_balance(incoming_wallet, test_gas_balance)
        .await?;

    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);

    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;

    setup_custody_roles(&evm, user_wallet, outgoing_wallet, incoming_wallet)
        .await?;

    let outgoing_config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let outgoing_client = start_service(outgoing_config.clone()).await?;

    let historical_mint = mint_before_migration(
        &outgoing_client,
        &evm,
        &user_signer,
        user_wallet,
        &mint_callback_mock,
        &databases.outgoing_url,
    )
    .await?;

    let outgoing_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &outgoing_provider,
    );
    let (minted_receipt_id, minted_receipt_shares, _) =
        single_deposit_for_owner(
            &outgoing_provider,
            evm.vault_address,
            outgoing_wallet,
        )
        .await?;

    let receipt_contract_address =
        outgoing_vault.receipt().call().await?.0.into();
    let outgoing_receipt =
        ReceiptInstance::new(receipt_contract_address, &outgoing_provider);
    assert_eq!(
        outgoing_receipt
            .balanceOf(outgoing_wallet, minted_receipt_id)
            .call()
            .await?,
        minted_receipt_shares,
        "the old wallet must custody the historical mint receipt before migration"
    );

    wait_for_completed_mint(
        &databases.outgoing_url,
        &historical_mint.issuer_request_id,
    )
    .await?;
    outgoing_client.terminate().await;

    // The first startup had no receipt to corroborate. Restart after discovery
    // so the production startup-reconciliation path records custody; the
    // migration below must rely on that event rather than a test-only helper.
    let outgoing_client = start_service(outgoing_config).await?;
    outgoing_client.terminate().await;
    // Process exit kills the old service's detached workers in production. A
    // consistent snapshot gives the replacement service the same persisted
    // state without leaving those in-process test workers attached to its DB.
    snapshot_database(&databases.outgoing_url, &databases.incoming_path)
        .await?;

    run_custody_migration(
        &databases.incoming_url,
        &outgoing_provider,
        evm.chain_id,
        evm.vault_address,
        outgoing_wallet,
        incoming_wallet,
    )
    .await?;

    assert_eq!(
        outgoing_receipt
            .balanceOf(outgoing_wallet, minted_receipt_id)
            .call()
            .await?,
        U256::ZERO,
        "the old wallet must not retain migrated receipt custody"
    );
    assert_eq!(
        outgoing_receipt
            .balanceOf(incoming_wallet, minted_receipt_id)
            .call()
            .await?,
        minted_receipt_shares,
        "the new wallet must receive the historical receipt before startup"
    );

    let mut incoming_config = harness::create_config_with_db(
        &databases.incoming_url,
        &mock_alpaca,
        &evm,
    )?;
    incoming_config.signer = SignerConfig::Local(incoming_private_key);
    let incoming_client = start_service(incoming_config).await?;
    assert_additional_chains_disabled(&incoming_client).await;

    PostMigrationCanaries {
        client: &incoming_client,
        evm: &evm,
        user_signer: &user_signer,
        user_wallet,
        incoming_wallet,
        historical_mint: &historical_mint,
        mint_callback_mock: &mint_callback_mock,
        redeem_mock: &redeem_mock,
        poll_mock: &poll_mock,
        db_url: &databases.incoming_url,
    }
    .run()
    .await?;
    assert_eq!(
        outgoing_receipt
            .balanceOf(incoming_wallet, minted_receipt_id)
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
        "migration must create exactly one redemption"
    );

    let withdraw_logs = outgoing_provider
        .get_logs(&outgoing_vault.Withdraw_filter().from_block(0).filter)
        .await?;
    let incoming_withdrawals = withdraw_logs
        .iter()
        .filter_map(|log| {
            OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner).ok()
        })
        .filter(|withdrawal| withdrawal.owner == incoming_wallet)
        .count();

    assert_eq!(
        incoming_withdrawals, 1,
        "the new wallet must submit exactly one historical-receipt burn"
    );
    assert_eq!(
        outgoing_provider.get_transaction_count(incoming_wallet).await?,
        2,
        "the new wallet must sign exactly one canary mint and one historical-receipt burn"
    );

    incoming_client.terminate().await;

    Ok(())
}

/// Proves receipt custody can be moved back after a completed migration, so an
/// aborted migration is recoverable.
///
/// This matters because the two directions are authorized by different systems:
/// the outbound leg is signed by the retiring custodian, the inbound leg by the
/// replacement. If the reverse were not permitted on-chain, an aborted migration
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
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let mock_alpaca = MockServer::start();
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());
    let outgoing_wallet = evm.wallet_address;
    let incoming_signer = PrivateKeySigner::random();
    let incoming_wallet = incoming_signer.address();
    assert_ne!(outgoing_wallet, incoming_wallet);

    let outgoing_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let gas = U256::from(10) * U256::from(10).pow(U256::from(18));
    outgoing_provider.anvil_set_balance(incoming_wallet, gas).await?;

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;
    evm.grant_deposit_role(outgoing_wallet).await?;
    evm.grant_certify_role(outgoing_wallet).await?;
    evm.certify_vault(U256::MAX).await?;

    // Run the service only long enough to discover the receipt into inventory,
    // which the migration cross-checks against the chain.
    let config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let client = start_service(config.clone()).await?;
    let vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &outgoing_provider,
    );
    let receipt_shares = U256::from(40) * U256::from(10).pow(U256::from(18));
    let share_ratio = U256::from(10).pow(U256::from(18));
    let deposit = vault
        .deposit(receipt_shares, outgoing_wallet, share_ratio, Bytes::new())
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
    let receipt = ReceiptInstance::new(receipt_contract, &outgoing_provider);
    wait_for_receipt_in_inventory(&databases.outgoing_url).await?;
    client.terminate().await;

    // Restart with the discovered receipt so startup reconciliation records
    // the outgoing holder before the engine is allowed to migrate it.
    let client = start_service(config).await?;
    client.terminate().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&databases.outgoing_url)
        .await?;

    let underlying: UnderlyingSymbol = CUSTODY_UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        &pool,
        &outgoing_provider,
        Network::Base,
        evm.chain_id,
        evm.vault_address,
        &underlying,
    )
    .await?;
    // No bootstrap call is allowed here: the forward move must depend on the
    // custody event written by production startup reconciliation above.
    let forward = migrate_vault_receipts(
        &pool,
        &outgoing_provider,
        identity,
        CorroboratedRecipient::verify(
            &outgoing_provider,
            outgoing_wallet,
            incoming_wallet,
        )
        .await?,
    )
    .await?;
    assert!(
        matches!(forward, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the forward migration must move receipts, got {forward:?}"
    );
    assert_eq!(
        receipt.balanceOf(incoming_wallet, receipt_id).call().await?,
        receipt_shares,
        "the incoming wallet must hold the receipt after the forward move"
    );

    // The rollback: same engine with the wallets swapped, and its destination
    // derived from the recorded forward migration rather than named.
    let derived_destination =
        recorded_migration_origin(&pool, evm.chain_id, evm.vault_address)
            .await?;
    assert_eq!(
        derived_destination, outgoing_wallet,
        "the rollback destination must derive from the recorded migration"
    );

    let incoming_provider = create_provider()
        .wallet(EthereumWallet::from(incoming_signer))
        .connect(&evm.endpoint)
        .await?;
    let rolled_back = migrate_vault_receipts(
        &pool,
        &incoming_provider,
        identity,
        CorroboratedRecipient::verify(
            &incoming_provider,
            incoming_wallet,
            derived_destination,
        )
        .await?,
    )
    .await?;

    assert!(
        matches!(rolled_back, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the rollback must move receipts back, got {rolled_back:?}"
    );
    assert_eq!(
        receipt.balanceOf(outgoing_wallet, receipt_id).call().await?,
        receipt_shares,
        "custody must return to the outgoing wallet after rollback"
    );
    assert_eq!(
        receipt.balanceOf(incoming_wallet, receipt_id).call().await?,
        U256::ZERO,
        "the incoming wallet must retain nothing after rollback"
    );

    pool.close().await;

    Ok(())
}

/// Proves the complete single-asset rehearsal, end to end: migrate, operate,
/// reverse, and resume.
///
/// 1. The outgoing service mints a receipt (historical custody).
/// 2. Stop; custody migrates forward; the replacement service starts.
/// 3. The replacement service performs one canary redemption of the
///    historical receipt and one canary mint — the two directions of the flow
///    against the new custody.
/// 4. Stop; custody rolls back — including the canary mint's receipt, which
///    only exists because of step 3 and which the resumed service could not
///    burn against if it were left behind.
/// 5. The original service resumes on the same event history and redeems the
///    canary, proving the rehearsal ends with a fully operational deployment
///    on the outgoing wallet, not merely with balances returned.
///
/// The rollback deliberately carries the database forward rather than
/// restoring the historical backup: the replacement service performed real
/// writes (a redemption and a mint), and discarding them would fork history.
/// Only custody and the signer configuration reverse.
#[tokio::test]
async fn test_single_asset_rehearsal_operates_reverses_and_resumes()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());
    let resumed_path = temp_dir.path().join("outgoing-resumed.db");
    let resumed_url = format!("sqlite:{}?mode=rwc", resumed_path.display());

    let outgoing_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let incoming_signer = PrivateKeySigner::random();
    let incoming_private_key: B256 = incoming_signer.to_bytes();
    let incoming_wallet = incoming_signer.address();
    assert_ne!(outgoing_wallet, incoming_wallet);

    let outgoing_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let test_gas_balance = U256::from(10) * U256::from(10).pow(U256::from(18));
    outgoing_provider.anvil_set_balance(user_wallet, test_gas_balance).await?;
    outgoing_provider
        .anvil_set_balance(incoming_wallet, test_gas_balance)
        .await?;

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;
    setup_custody_roles(&evm, user_wallet, outgoing_wallet, incoming_wallet)
        .await?;

    let outgoing_config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let outgoing_client = start_service(outgoing_config.clone()).await?;
    let historical_mint = mint_before_migration(
        &outgoing_client,
        &evm,
        &user_signer,
        user_wallet,
        &mint_callback_mock,
        &databases.outgoing_url,
    )
    .await?;
    wait_for_completed_mint(
        &databases.outgoing_url,
        &historical_mint.issuer_request_id,
    )
    .await?;
    outgoing_client.terminate().await;

    // The initial startup preceded receipt discovery. A second production
    // startup corroborates and records outgoing custody for the migration.
    let outgoing_client = start_service(outgoing_config).await?;
    outgoing_client.terminate().await;

    // Forward migration. The migration runs against the same database the
    // replacement service will use, as it does in production, so the custody
    // events are part of the history the service starts from.
    snapshot_database(&databases.outgoing_url, &databases.incoming_path)
        .await?;
    run_custody_migration(
        &databases.incoming_url,
        &outgoing_provider,
        evm.chain_id,
        evm.vault_address,
        outgoing_wallet,
        incoming_wallet,
    )
    .await?;

    let mut incoming_config = harness::create_config_with_db(
        &databases.incoming_url,
        &mock_alpaca,
        &evm,
    )?;
    incoming_config.signer = SignerConfig::Local(incoming_private_key);
    let incoming_client = start_service(incoming_config).await?;

    let canary_mint = PostMigrationCanaries {
        client: &incoming_client,
        evm: &evm,
        user_signer: &user_signer,
        user_wallet,
        incoming_wallet,
        historical_mint: &historical_mint,
        mint_callback_mock: &mint_callback_mock,
        redeem_mock: &redeem_mock,
        poll_mock: &poll_mock,
        db_url: &databases.incoming_url,
    }
    .run()
    .await?;
    wait_for_settled_burn(&databases.incoming_url).await?;
    wait_for_completed_mint(
        &databases.incoming_url,
        &canary_mint.issuer_request_id,
    )
    .await?;
    incoming_client.terminate().await;

    // The reversal. The canary mint left its receipt with the incoming wallet,
    // so the rollback has real, newly created custody to return — not just the
    // original receipts.
    snapshot_database(&databases.incoming_url, &resumed_path).await?;
    run_custody_migration(
        &resumed_url,
        &create_provider()
            .wallet(EthereumWallet::from(incoming_signer))
            .connect(&evm.endpoint)
            .await?,
        evm.chain_id,
        evm.vault_address,
        incoming_wallet,
        outgoing_wallet,
    )
    .await?;

    resume_on_outgoing_wallet_and_redeem_canary(
        &resumed_url,
        &evm,
        &mock_alpaca,
        &user_signer,
        outgoing_wallet,
        &redeem_mock,
        &poll_mock,
    )
    .await?;

    assert_eq!(
        mint_callback_mock.calls_async().await,
        2,
        "the rehearsal must produce exactly two mints: historical and canary"
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
    outgoing_wallet: Address,
    redeem_mock: &Mock<'_>,
    poll_mock: &Mock<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let resumed_config =
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
        .transfer(outgoing_wallet, canary_shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    harness::wait_for_mock_hits(redeem_mock, 2).await?;
    harness::wait_for_mock_hits(poll_mock, 2).await?;
    harness::wait_for_burn(&user_vault, outgoing_wallet).await?;
    assert_eq!(
        user_vault.balanceOf(user_signer.address()).call().await?,
        U256::ZERO,
        "the resumed service must redeem the canary shares in full"
    );

    resumed_client.terminate().await;

    Ok(())
}

/// Proves what the running service does when the custody step is skipped, which
/// is the failure this whole migration is sequenced to avoid.
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
/// operator sequence, not the code, is what prevents this: pause rebalancing
/// so no redemption arrives (the sole participant is our own bot), and stop
/// the service for the window.
#[tokio::test]
async fn test_holder_rotation_without_receipt_transfer_cannot_burn_historical_shares()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let mock_alpaca = MockServer::start();
    let mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let (redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());
    let outgoing_wallet = evm.wallet_address;
    let user_signer = PrivateKeySigner::random();
    let user_wallet = user_signer.address();
    let incoming_signer = PrivateKeySigner::random();
    let incoming_wallet = incoming_signer.address();
    assert_ne!(outgoing_wallet, incoming_wallet);

    let outgoing_provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;
    let test_gas_balance = U256::from(10) * U256::from(10).pow(U256::from(18));
    outgoing_provider.anvil_set_balance(user_wallet, test_gas_balance).await?;
    outgoing_provider
        .anvil_set_balance(incoming_wallet, test_gas_balance)
        .await?;

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;
    setup_custody_roles(&evm, user_wallet, outgoing_wallet, incoming_wallet)
        .await?;
    let outgoing_config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let outgoing_client = start_service(outgoing_config).await?;
    let historical_mint = mint_before_migration(
        &outgoing_client,
        &evm,
        &user_signer,
        user_wallet,
        &mint_callback_mock,
        &databases.outgoing_url,
    )
    .await?;
    let (receipt_id, receipt_shares, _receipt_information) =
        single_deposit_for_owner(
            &outgoing_provider,
            evm.vault_address,
            outgoing_wallet,
        )
        .await?;
    wait_for_completed_mint(
        &databases.outgoing_url,
        &historical_mint.issuer_request_id,
    )
    .await?;
    outgoing_client.terminate().await;
    snapshot_database(&databases.outgoing_url, &databases.incoming_path)
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
        .transfer(incoming_wallet, historical_mint.shares)
        .send()
        .await?
        .get_receipt()
        .await?;

    // Bring up the replacement service against the un-migrated state, exactly
    // as an operator who skipped the custody step would.
    let mut incoming_config = harness::create_config_with_db(
        &databases.incoming_url,
        &mock_alpaca,
        &evm,
    )?;
    incoming_config.signer =
        SignerConfig::Local(B256::from(incoming_signer.to_bytes()));
    let incoming_client = start_service(incoming_config).await?;

    let incoming_provider = create_provider()
        .wallet(EthereumWallet::from(incoming_signer))
        .connect(&evm.endpoint)
        .await?;
    let incoming_vault = OffchainAssetReceiptVaultInstance::new(
        evm.vault_address,
        &incoming_provider,
    );

    // The service detects the transfer and calls Alpaca before it can burn, so
    // the redeem callback firing is the point of no return: shares are journaled
    // back to the participant while the tokens are still outstanding.
    harness::wait_for_mock_hits(&redeem_mock, 1).await?;

    // Give the burn every chance to land before asserting it did not. Without
    // the receipt it reverts, and `BurnFailed` recovery retries against an
    // on-chain share balance that is present, so it never auto-fails either.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    let withdraw_logs = incoming_provider
        .get_logs(&incoming_vault.Withdraw_filter().from_block(0).filter)
        .await?;
    let incoming_withdrawals = withdraw_logs
        .iter()
        .filter_map(|log| {
            OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner).ok()
        })
        .filter(|withdrawal| withdrawal.owner == incoming_wallet)
        .count();
    assert_eq!(
        incoming_withdrawals, 0,
        "no burn may succeed without receipt custody"
    );

    assert_eq!(
        incoming_vault.balanceOf(incoming_wallet).call().await?,
        historical_mint.shares,
        "the shares must remain outstanding and unburned"
    );

    let receipt_contract_address =
        incoming_vault.receipt().call().await?.0.into();
    let receipt =
        ReceiptInstance::new(receipt_contract_address, &incoming_provider);
    assert_eq!(
        receipt.balanceOf(outgoing_wallet, receipt_id).call().await?,
        receipt_shares,
        "the historical receipt must remain in old-wallet custody"
    );
    assert_eq!(
        receipt.balanceOf(incoming_wallet, receipt_id).call().await?,
        U256::ZERO,
        "the new wallet must still lack the historical receipt"
    );

    incoming_client.terminate().await;

    Ok(())
}

/// Proves the cutover's receipt move end to end against a REAL orchestrator
/// (RAI-1681): the destination is corroborated as an ERC-1155-receiving
/// contract via its own ERC-165 answers, the engine moves the receipts, the
/// orchestrator's burn pointer covers the transferred id (so the burn walk
/// can reach it without any manual `setBurnIndex`), the recorded origin
/// supports a later rollback, a re-run submits nothing — and the service
/// then starts cleanly against the migrated store with its custody record
/// intact (the expected-elsewhere reconciliation skip, until RAI-1223
/// retires the subsystem).
#[tokio::test]
async fn test_receipt_custody_migrates_into_the_orchestrator()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;
    let mock_alpaca = MockServer::start();
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());
    let bot_wallet = evm.wallet_address;

    let provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_certify_role(bot_wallet).await?;
    evm.certify_vault(U256::MAX).await?;

    // Run the service only long enough to discover the receipt into
    // inventory, then restart so production startup reconciliation records
    // the outgoing holder — the engine refuses unobserved custody.
    let config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let client = start_service(config.clone()).await?;
    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider);
    let receipt_shares = U256::from(40) * U256::from(10).pow(U256::from(18));
    let share_ratio = U256::from(10).pow(U256::from(18));
    let deposit = vault
        .deposit(receipt_shares, bot_wallet, share_ratio, Bytes::new())
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
    let receipt = ReceiptInstance::new(receipt_contract, &provider);
    wait_for_receipt_in_inventory(&databases.outgoing_url).await?;
    client.terminate().await;

    let client = start_service(config.clone()).await?;
    client.terminate().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&databases.outgoing_url)
        .await?;

    let underlying: UnderlyingSymbol = CUSTODY_UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        &pool,
        &provider,
        Network::Base,
        evm.chain_id,
        evm.vault_address,
        &underlying,
    )
    .await?;

    // The contract corroboration path: the orchestrator must prove ERC-1155
    // receiver support through its own ERC-165 answers.
    let destination = CorroboratedRecipient::verify(
        &provider,
        bot_wallet,
        orchestrator_address,
    )
    .await?;
    assert_eq!(
        destination.kind(),
        RecipientKind::Erc1155Receiver,
        "the orchestrator must corroborate as an ERC-1155-receiving contract"
    );

    let outcome =
        migrate_vault_receipts(&pool, &provider, identity, destination).await?;
    assert!(
        matches!(outcome, MigrationOutcome::Migrated { receipts, .. } if receipts > 0),
        "the migration must move receipts into the orchestrator, got \
         {outcome:?}"
    );

    assert_eq!(
        receipt.balanceOf(orchestrator_address, receipt_id).call().await?,
        receipt_shares,
        "the orchestrator must hold the full transferred receipt balance"
    );
    assert_eq!(
        receipt.balanceOf(bot_wallet, receipt_id).call().await?,
        U256::ZERO,
        "the bot wallet must retain nothing after the move"
    );

    // The cutover verification from the runbook: the burn pointer must sit
    // at or below the transferred id, so the transferred receipt is
    // reachable by the orchestrator's burn walk without manual intervention.
    let orchestrator = ST0xOrchestrator::new(orchestrator_address, &provider);
    let burn_pointer =
        orchestrator.nextBurnReceiptId(evm.vault_address).call().await?;
    assert!(
        burn_pointer <= receipt_id,
        "the burn pointer ({burn_pointer}) must cover the transferred \
         receipt ({receipt_id})"
    );

    // The rollback origin derives from the recorded migration, not a typed
    // address — an EMERGENCY_ROLE withdrawReceipt would return receipts
    // here.
    assert_eq!(
        recorded_migration_origin(&pool, evm.chain_id, evm.vault_address)
            .await?,
        bot_wallet,
        "the recorded origin must support a rollback to the bot wallet"
    );

    let rerun =
        migrate_vault_receipts(&pool, &provider, identity, destination).await?;
    assert!(
        matches!(rerun, MigrationOutcome::AlreadyMigrated { receipts } if receipts > 0),
        "re-running a completed move must submit nothing, got {rerun:?}"
    );

    pool.close().await;

    // The service starts cleanly on the migrated store: startup
    // reconciliation skips the vault whose custody a recorded migration
    // moved away (asserted at the log level in the reconciler's unit tests;
    // here the whole production startup path runs against the real store)
    // and the custody record survives untouched.
    let client = start_service(config.clone()).await?;
    client.terminate().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&databases.outgoing_url)
        .await?;
    assert_eq!(
        recorded_migration_origin(&pool, evm.chain_id, evm.vault_address)
            .await?,
        bot_wallet,
        "post-migration startup must not clobber the recorded custody"
    );
    pool.close().await;

    assert_eq!(
        receipt.balanceOf(orchestrator_address, receipt_id).call().await?,
        receipt_shares,
        "the orchestrator's receipts must survive the service restart \
         untouched"
    );

    // The rollback leg: an EMERGENCY_ROLE withdrawReceipt returns the
    // receipts to the bot wallet, and confirm-custody re-records the holder
    // so reconciliation resumes — the documented recovery a cutover
    // rollback depends on.
    let emergency_role = orchestrator.EMERGENCY_ROLE().call().await?;
    orchestrator
        .grantRole(emergency_role, bot_wallet)
        .send()
        .await?
        .get_receipt()
        .await?;
    orchestrator
        .withdrawReceipt(
            evm.vault_address,
            receipt_id,
            receipt_shares,
            bot_wallet,
        )
        .send()
        .await?
        .get_receipt()
        .await?;

    assert_eq!(
        receipt.balanceOf(bot_wallet, receipt_id).call().await?,
        receipt_shares,
        "the emergency withdrawal must return the receipt to the bot wallet"
    );
    assert_eq!(
        receipt.balanceOf(orchestrator_address, receipt_id).call().await?,
        U256::ZERO,
        "the orchestrator must retain nothing after the withdrawal"
    );

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&databases.outgoing_url)
        .await?;
    let confirmed =
        confirm_custody_holder(&pool, &provider, identity, bot_wallet).await?;
    assert_eq!(
        confirmed, 1,
        "re-confirmation must verify and record the returned receipt"
    );
    // The count above only proves verified balances; the persisted custody
    // holder is the fact reconciliation actually reads.
    assert_eq!(
        recorded_custody_holder(&pool, evm.chain_id, evm.vault_address).await?,
        bot_wallet,
        "re-confirmation must record the bot wallet as the current holder"
    );
    pool.close().await;

    // With custody re-recorded at the signing wallet, the service resumes
    // ordinary reconciliation on the same store.
    let client = start_service(config).await?;
    client.terminate().await;

    Ok(())
}

/// The chunked-migration scenario's tracked receipt count and the engine's
/// proven per-transaction bound: 17 receipts split into a full chunk of 14
/// plus a remainder of 3.
const CHUNKED_TRACKED_RECEIPTS: usize = 17;
const CHUNK_BOUND: usize = 14;

/// Everything the chunked-migration phases share, so each phase reads as one
/// focused step of the scenario.
struct ChunkedMigrationStage<'a, P: Provider> {
    pool: &'a sqlx::SqlitePool,
    provider: &'a P,
    identity: VaultIdentity<'a>,
    destination: CorroboratedRecipient,
    receipt_contract: Address,
    receipt_ids: &'a [U256],
    receipt_shares: U256,
    orchestrator_address: Address,
    bot_wallet: Address,
    chain_id: u64,
    vault_address: Address,
}

impl<P: Provider> ChunkedMigrationStage<'_, P> {
    /// Phase 1: the full move must cross the bound as multiple bounded batch
    /// transactions, each verified before the next, recording custody once —
    /// and a re-run must report `AlreadyMigrated`.
    async fn full_move_lands_in_bounded_batches(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let outcome = migrate_vault_receipts(
            self.pool,
            self.provider,
            self.identity,
            self.destination,
        )
        .await?;
        assert!(
            matches!(
                outcome,
                MigrationOutcome::Migrated { receipts, .. }
                    if receipts == CHUNKED_TRACKED_RECEIPTS
            ),
            "the migration must move all {CHUNKED_TRACKED_RECEIPTS} \
             receipts, got {outcome:?}"
        );

        let receipt =
            ReceiptInstance::new(self.receipt_contract, self.provider);
        let batches =
            receipt.TransferBatch_filter().from_block(0).query().await?;
        let engine_batches = batches
            .iter()
            .filter(|(event, _)| event.to == self.orchestrator_address)
            .count();
        assert_eq!(
            engine_batches, 2,
            "{CHUNKED_TRACKED_RECEIPTS} receipts must move as exactly two \
             bounded batch transactions"
        );

        self.assert_orchestrator_holds_everything(
            "the orchestrator must hold every receipt after the move",
        )
        .await?;
        for receipt_id in self.receipt_ids {
            assert_eq!(
                receipt.balanceOf(self.bot_wallet, *receipt_id).call().await?,
                U256::ZERO,
                "the bot wallet must retain nothing of receipt {receipt_id}"
            );
        }
        assert_eq!(
            recorded_migration_origin(
                self.pool,
                self.chain_id,
                self.vault_address,
            )
            .await?,
            self.bot_wallet,
            "custody must be recorded once, with the bot wallet as origin"
        );

        self.assert_rerun_reports_already_migrated(
            "re-running the completed move must submit nothing",
        )
        .await
    }

    /// Phase 2: return every receipt (the rollback leg) and re-confirm
    /// custody at the bot wallet, restoring the pre-migration state for the
    /// interrupted-run phase.
    async fn rollback_and_reconfirm_custody(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let orchestrator =
            ST0xOrchestrator::new(self.orchestrator_address, self.provider);
        let emergency_role = orchestrator.EMERGENCY_ROLE().call().await?;
        orchestrator
            .grantRole(emergency_role, self.bot_wallet)
            .send()
            .await?
            .get_receipt()
            .await?;
        for receipt_id in self.receipt_ids {
            orchestrator
                .withdrawReceipt(
                    self.vault_address,
                    *receipt_id,
                    self.receipt_shares,
                    self.bot_wallet,
                )
                .send()
                .await?
                .get_receipt()
                .await?;
        }

        let confirmed = confirm_custody_holder(
            self.pool,
            self.provider,
            self.identity,
            self.bot_wallet,
        )
        .await?;
        assert_eq!(
            confirmed, CHUNKED_TRACKED_RECEIPTS,
            "re-confirmation must verify every returned receipt"
        );
        // The count above only proves verified balances; the persisted
        // custody holder is the fact reconciliation actually reads.
        assert_eq!(
            recorded_custody_holder(
                self.pool,
                self.chain_id,
                self.vault_address,
            )
            .await?,
            self.bot_wallet,
            "re-confirmation must record the bot wallet as the current holder"
        );

        Ok(())
    }

    /// Phase 3: fabricate the exact state a crash between chunks leaves —
    /// the first bounded batch landed, the rest never submitted — then prove
    /// a plain re-run resumes with only the remainder and a further re-run
    /// reports `AlreadyMigrated`.
    async fn crash_resume_moves_only_remainder(
        &self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut sorted_ids = self.receipt_ids.to_vec();
        sorted_ids.sort();
        let first_chunk: Vec<U256> =
            sorted_ids.iter().take(CHUNK_BOUND).copied().collect();
        let receipt =
            ReceiptInstance::new(self.receipt_contract, self.provider);
        receipt
            .safeBatchTransferFrom(
                self.bot_wallet,
                self.orchestrator_address,
                first_chunk.clone(),
                vec![self.receipt_shares; first_chunk.len()],
                Bytes::new(),
            )
            .send()
            .await?
            .get_receipt()
            .await?;

        let resumed = migrate_vault_receipts(
            self.pool,
            self.provider,
            self.identity,
            self.destination,
        )
        .await?;
        assert!(
            matches!(
                resumed,
                MigrationOutcome::Migrated { receipts, .. }
                    if receipts == CHUNKED_TRACKED_RECEIPTS - CHUNK_BOUND
            ),
            "the resume must move only the remainder, got {resumed:?}"
        );
        self.assert_orchestrator_holds_everything(
            "every receipt must reach the orchestrator exactly once across \
             the interrupted run and its resume",
        )
        .await?;

        self.assert_rerun_reports_already_migrated(
            "a further re-run must report the completed migration",
        )
        .await
    }

    async fn assert_orchestrator_holds_everything(
        &self,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let receipt =
            ReceiptInstance::new(self.receipt_contract, self.provider);
        for receipt_id in self.receipt_ids {
            assert_eq!(
                receipt
                    .balanceOf(self.orchestrator_address, *receipt_id)
                    .call()
                    .await?,
                self.receipt_shares,
                "{message} (receipt {receipt_id})"
            );
        }
        Ok(())
    }

    async fn assert_rerun_reports_already_migrated(
        &self,
        message: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rerun = migrate_vault_receipts(
            self.pool,
            self.provider,
            self.identity,
            self.destination,
        )
        .await?;
        assert!(
            matches!(
                rerun,
                MigrationOutcome::AlreadyMigrated { receipts }
                    if receipts == CHUNKED_TRACKED_RECEIPTS
            ),
            "{message}, got {rerun:?}"
        );
        Ok(())
    }
}

/// RAI-1714: a vault above the proven 14-receipt single-transfer bound
/// migrates into the orchestrator as a sequence of bounded batch
/// transactions; a run interrupted between chunks resumes via a plain
/// re-run, moving only the remainder and recording custody once; a further
/// re-run reports `AlreadyMigrated` and submits nothing.
#[tokio::test]
async fn test_receipt_custody_chunked_migration_resumes_into_the_orchestrator()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::with_chain_id(Network::Base.chain_id()).await?;
    let orchestrator_address = evm.deploy_orchestrator().await?;
    let mock_alpaca = MockServer::start();
    let _mint_callback_mock =
        harness::alpaca_mocks::setup_mint_mocks(&mock_alpaca);
    let temp_dir = tempfile::tempdir()?;
    let databases = CustodyDatabases::in_directory(temp_dir.path());
    let bot_wallet = evm.wallet_address;

    let provider = create_provider()
        .wallet(EthereumWallet::from(PrivateKeySigner::from_bytes(
            &evm.private_key,
        )?))
        .connect(&evm.endpoint)
        .await?;

    harness::preseed_tokenized_asset(
        &databases.outgoing_url,
        evm.vault_address,
        CUSTODY_UNDERLYING,
        CUSTODY_TOKEN,
    )
    .await?;
    evm.grant_deposit_role(bot_wallet).await?;
    evm.grant_certify_role(bot_wallet).await?;
    evm.certify_vault(U256::MAX).await?;

    let config = harness::create_config_with_db(
        &databases.outgoing_url,
        &mock_alpaca,
        &evm,
    )?;
    let client = start_service(config.clone()).await?;

    let vault =
        OffchainAssetReceiptVaultInstance::new(evm.vault_address, &provider);
    let share_ratio = U256::from(10).pow(U256::from(18));
    let receipt_shares = U256::from(10) * share_ratio;
    let mut receipt_ids: Vec<U256> =
        Vec::with_capacity(CHUNKED_TRACKED_RECEIPTS);
    for _ in 0..CHUNKED_TRACKED_RECEIPTS {
        let deposit = vault
            .deposit(receipt_shares, bot_wallet, share_ratio, Bytes::new())
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
        receipt_ids.push(receipt_id);
    }
    let receipt_contract: Address = vault.receipt().call().await?.0.into();
    wait_for_discovered_receipts(
        &databases.outgoing_url,
        i64::try_from(CHUNKED_TRACKED_RECEIPTS)?,
    )
    .await?;
    client.terminate().await;

    // Startup reconciliation records the outgoing holder — the engine
    // refuses unobserved custody.
    let client = start_service(config.clone()).await?;
    client.terminate().await;

    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect(&databases.outgoing_url)
        .await?;
    let underlying: UnderlyingSymbol = CUSTODY_UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        &pool,
        &provider,
        Network::Base,
        evm.chain_id,
        evm.vault_address,
        &underlying,
    )
    .await?;
    let destination = CorroboratedRecipient::verify(
        &provider,
        bot_wallet,
        orchestrator_address,
    )
    .await?;

    let stage = ChunkedMigrationStage {
        pool: &pool,
        provider: &provider,
        identity,
        destination,
        receipt_contract,
        receipt_ids: &receipt_ids,
        receipt_shares,
        orchestrator_address,
        bot_wallet,
        chain_id: evm.chain_id,
        vault_address: evm.vault_address,
    };

    stage.full_move_lands_in_bounded_batches().await?;
    stage.rollback_and_reconfirm_custody().await?;
    stage.crash_resume_moves_only_remainder().await?;

    pool.close().await;

    Ok(())
}
