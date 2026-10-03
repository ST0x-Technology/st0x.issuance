//! Fork rehearsal of the RKLB orchestrator cutover against the real Base state.
//!
//! It runs only on request, because it needs a Base RPC that can serve state
//! at the fork block:
//!
//! ```sh
//! FORK_RPC_URL=... FORK_BLOCK=... \
//!   cargo test --test fork_rehearsal -- --ignored --nocapture
//! ```
//!
//! The fork keeps Base's chain ID, so a transaction signed on it is also valid
//! on Base. Nothing here signs with a real key: the service signs with a
//! random stand-in wallet, and every prod account the scenario needs is
//! impersonated on the fork only.
//!
//! The test keeps two kinds of setup apart. Real-state assertions check what
//! the pilot depends on and never patch it, so a failure there is a real pilot
//! blocker. Substitutions replace only what belongs to the prod wallet, and
//! each one is printed. The scenario follows the runbook
//! (`docs/runbooks/orchestrator-onboarding.md`): the cutover and the rollback
//! both run with the service stopped.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256, Bytes, U256, address, keccak256};
use alloy::providers::ext::AnvilApi;
use alloy::providers::{Provider, ProviderBuilder};
use alloy::rpc::types::TransactionReceipt;
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use httpmock::prelude::*;
use rocket::local::asynchronous::Client;
use serde_json::json;
use sqlx::sqlite::SqlitePoolOptions;
use st0x_issuance::bindings::IST0xOrchestratorV1::IST0xOrchestratorV1Instance;
use st0x_issuance::bindings::OffchainAssetReceiptVault::OffchainAssetReceiptVaultInstance;
use st0x_issuance::bindings::OffchainAssetReceiptVaultAuthorizerV1;
use st0x_issuance::bindings::Receipt::ReceiptInstance;
use st0x_issuance::bindings::ST0xOrchestrator;
use st0x_issuance::receipt_inventory::migration::{
    CorroboratedRecipient, CustodyAfterMove, MigrationOutcome, RecipientKind,
    VaultIdentity, confirm_custody_holder, migrate_vault_receipts,
    recorded_custody_holder, recorded_migration_origin, tracked_receipt_count,
};
use st0x_issuance::test_utils::LocalEvm;
use st0x_issuance::tokenized_asset::UnderlyingSymbol;
use st0x_issuance::{Config, Network};
use std::error::Error;
use std::future::Future;
use std::time::Duration;

use crate::harness::{
    MintFlowRequest, TEST_API_KEY, bot_provider, confirm_mint_journal,
    create_provider, initialize_rocket, initiate_mint_request,
    orchestrator_vault_modes, tokens,
};

/// The pinned `ST0xOrchestrator` address (`config.prod.toml`). It has code on
/// Base since block 50,563,983.
const ORCHESTRATOR: Address =
    address!("0x3A7387a484d87Aa8bBA45E98AAB401Ce4FBF03E2");

/// The RKLB vault on Base: the `tRKLB` share token itself. `wtRKLB` is its
/// ERC-4626 wrapper, not the vault.
const RKLB_VAULT: Address =
    address!("0xf6744fd94e27c2f58f6110aa9fdc77a87e41766b");

/// The prod bot wallet (the Turnkey key) that holds the RKLB receipts. The
/// test impersonates it only to move those receipts to the stand-in wallet.
const PROD_BOT_WALLET: Address =
    address!("0x3d0cd66efa66c05d86c3d4316b03eae87ab9e8ae");

/// Holds `DEFAULT_ADMIN_ROLE` on the orchestrator and the `*_ADMIN` roles on
/// the RKLB authorizer. Very likely a Safe; the fork can impersonate it.
const ADMIN: Address = address!("0xe70d821f3462a074e63b42d0aac6523faae1d611");

/// The real `EMERGENCY_ROLE` holder on the orchestrator. `None` until
/// governance grants the role on Base. Then set it here: the rollback runs as
/// that holder, and its role becomes a real-state assertion.
const EMERGENCY_HOLDER: Option<Address> = None;

/// Before this block the bot has no `MINT_ROLE` or `BURN_ROLE` on the
/// orchestrator, and the orchestrator has no roles on the RKLB authorizer.
const EARLIEST_FORK_BLOCK: u64 = 51_076_621;

/// `migrate_vault_receipts` moves at most this many receipts in one
/// transaction (`MAX_RECEIPTS_PER_TRANSFER` in `receipt_inventory::migration`).
const RECEIPTS_PER_TRANSFER: usize = 14;

const UNDERLYING: &str = "RKLB";
const TOKEN: &str = "tRKLB";

/// How long a step waits for the service. The fork reads state it has not
/// seen yet from the upstream RPC, so it is slower than a local chain.
const FORK_WAIT: Duration = Duration::from_secs(180);

/// A receipt id and the amount one holder has of it.
type Holding = (U256, U256);

struct Rehearsal {
    evm: LocalEvm,
    fork_block: u64,
    database_url: String,
    mock_alpaca: MockServer,
    /// Each receipt the prod wallet held at the fork block.
    snapshot: Vec<Holding>,
}

#[tokio::test]
#[ignore = "needs FORK_RPC_URL and FORK_BLOCK; see the module doc"]
async fn rklb_cutover_rehearsal_on_a_base_fork() -> Result<(), Box<dyn Error>> {
    let (rpc_url, fork_block) = fork_inputs()?;
    let evm = LocalEvm::fork(&rpc_url, fork_block, RKLB_VAULT).await?;
    let temp_dir = tempfile::tempdir()?;
    let database_url = format!(
        "sqlite:{}?mode=rwc",
        temp_dir.path().join("fork_rehearsal.db").display()
    );
    println!("fork block: {fork_block}");
    println!("stand-in bot wallet: {}", evm.wallet_address);

    let snapshot = assert_real_state(&evm).await?;
    let rehearsal = Rehearsal {
        evm,
        fork_block,
        database_url,
        mock_alpaca: MockServer::start(),
        snapshot,
    };
    Box::pin(substitute_prod_wallet(&rehearsal)).await?;
    harness::preseed_tokenized_asset(
        &rehearsal.database_url,
        RKLB_VAULT,
        UNDERLYING,
        TOKEN,
    )
    .await?;

    run_vault_direct_baseline(&rehearsal).await?;
    let chunks = cut_over(&rehearsal).await?;
    let operated = operate_in_orchestrator_mode(&rehearsal).await?;
    let returned = roll_back(&rehearsal).await?;
    rediscover_returned_receipts(&rehearsal, returned).await?;
    redeem_vault_direct(&rehearsal, &operated).await?;

    println!("--- fork rehearsal record ---");
    println!("fork block: {}", rehearsal.fork_block);
    println!("receipts moved at cutover: {}", rehearsal.snapshot.len());
    println!("cutover transactions (chunks): {chunks}");
    println!("orchestrator mint transaction: {}", operated.mint_tx);
    println!("orchestrator burn transaction: {}", operated.burn_tx);
    println!("receipts returned by the rollback: {returned}");

    Ok(())
}

/// Reads the RPC URL and the fork block from the environment. The URL can
/// carry an API key, so it is never printed.
fn fork_inputs() -> Result<(String, u64), Box<dyn Error>> {
    let rpc_url = std::env::var("FORK_RPC_URL").map_err(
        |_| "set FORK_RPC_URL to a Base RPC that can serve state at FORK_BLOCK",
    )?;
    let fork_block: u64 = std::env::var("FORK_BLOCK")
        .map_err(|_| "set FORK_BLOCK to the Base block to fork from")?
        .parse()?;
    if fork_block < EARLIEST_FORK_BLOCK {
        return Err(format!(
            "FORK_BLOCK {fork_block} is before {EARLIEST_FORK_BLOCK}, the \
             block where the bot and the orchestrator get their roles"
        )
        .into());
    }

    Ok((rpc_url, fork_block))
}

/// Checks the facts the pilot depends on, before the test changes anything,
/// and returns each receipt the prod wallet holds. Nothing here is patched:
/// a failure is a real pilot blocker.
async fn assert_real_state(
    evm: &LocalEvm,
) -> Result<Vec<Holding>, Box<dyn Error>> {
    let fork = fork_provider(evm).await?;
    assert!(
        !fork.get_code_at(ORCHESTRATOR).await?.is_empty(),
        "real state: no orchestrator code at {ORCHESTRATOR}"
    );
    let orchestrator = ST0xOrchestrator::new(ORCHESTRATOR, &fork);
    assert!(
        orchestrator.vaultLogicIsExpected().call().await?,
        "real state: vaultLogicIsExpected() is false"
    );

    let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
        evm.authorizer_address,
        &fork,
    );
    for role in ["DEPOSIT", "WITHDRAW"] {
        assert!(
            authorizer.hasRole(keccak256(role), ORCHESTRATOR).call().await?,
            "real state: the orchestrator lacks {role} on the RKLB authorizer"
        );
    }

    let vault = OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, &fork);
    assert!(
        !vault.isCertificationExpired().call().await?,
        "real state: the RKLB vault certification has expired, which blocks \
         orchestrator burns"
    );

    if let Some(holder) = EMERGENCY_HOLDER {
        let emergency_role = orchestrator.EMERGENCY_ROLE().call().await?;
        assert!(
            orchestrator.hasRole(emergency_role, holder).call().await?,
            "real state: {holder} does not hold EMERGENCY_ROLE"
        );
    }

    let receipt = receipt_contract(&fork).await?;
    let highwater = vault.highwaterId().call().await?;
    let snapshot = holdings(&receipt, PROD_BOT_WALLET, highwater).await?;
    assert!(
        !snapshot.is_empty(),
        "real state: the prod bot wallet holds no RKLB receipt"
    );
    assert!(
        holdings(&receipt, ORCHESTRATOR, highwater).await?.is_empty(),
        "real state: the orchestrator already holds RKLB receipts"
    );

    println!("real state: orchestrator code present, vault logic expected");
    println!("real state: orchestrator has DEPOSIT and WITHDRAW on the vault");
    println!("real state: vault certification valid");
    println!(
        "real state: prod bot wallet holds {} receipts, highwaterId {highwater}",
        snapshot.len()
    );
    println!(
        "real state: burn pointer {}",
        orchestrator.nextBurnReceiptId(RKLB_VAULT).call().await?
    );
    println!(
        "info: prod bot wallet tRKLB allowance to the orchestrator {}",
        vault.allowance(PROD_BOT_WALLET, ORCHESTRATOR).call().await?
    );

    Ok(snapshot)
}

/// Replaces what is tied to the prod wallet with the stand-in wallet, on the
/// fork only: its receipts, its roles, and its approval.
async fn substitute_prod_wallet(
    rehearsal: &Rehearsal,
) -> Result<(), Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let stand_in = evm.wallet_address;
    let fork = fork_provider(evm).await?;
    let receipt = receipt_contract(&fork).await?;

    impersonate(&fork, PROD_BOT_WALLET).await?;
    let (ids, amounts): (Vec<U256>, Vec<U256>) =
        rehearsal.snapshot.iter().copied().unzip();
    let moved = receipt
        .safeBatchTransferFrom(
            PROD_BOT_WALLET,
            stand_in,
            ids,
            amounts,
            Bytes::new(),
        )
        .from(PROD_BOT_WALLET)
        .send()
        .await?
        .get_receipt()
        .await?;
    require_success(&moved, "moving the prod receipts")?;
    for (id, amount) in &rehearsal.snapshot {
        assert_eq!(
            receipt.balanceOf(stand_in, *id).call().await?,
            *amount,
            "the stand-in wallet must hold receipt {id} at its prod balance"
        );
    }
    println!(
        "substitution: moved {} receipts from {PROD_BOT_WALLET} to the \
         stand-in wallet",
        rehearsal.snapshot.len()
    );

    impersonate(&fork, ADMIN).await?;
    let orchestrator = ST0xOrchestrator::new(ORCHESTRATOR, &fork);
    for role in [
        orchestrator.MINT_ROLE().call().await?,
        orchestrator.BURN_ROLE().call().await?,
    ] {
        let granted = orchestrator
            .grantRole(role, stand_in)
            .from(ADMIN)
            .send()
            .await?
            .get_receipt()
            .await?;
        require_success(&granted, "granting an orchestrator role")?;
    }

    let authorizer = OffchainAssetReceiptVaultAuthorizerV1::new(
        evm.authorizer_address,
        &fork,
    );
    for role in ["DEPOSIT", "WITHDRAW"] {
        let granted = authorizer
            .grantRole(keccak256(role), stand_in)
            .from(ADMIN)
            .send()
            .await?
            .get_receipt()
            .await?;
        require_success(&granted, "granting a vault role")?;
    }
    println!(
        "substitution: {ADMIN} granted the stand-in wallet MINT_ROLE and \
         BURN_ROLE on the orchestrator, DEPOSIT and WITHDRAW on the vault"
    );

    harness::approve_orchestrator(evm, ORCHESTRATOR).await?;
    println!("substitution: the stand-in wallet approved the orchestrator");

    Ok(())
}

/// Runbook state before step 7: the asset runs vault-direct, inventory
/// tracks every receipt, and startup reconciliation has recorded custody at
/// the stand-in wallet. Ends with the service stopped, as the deploy hold
/// before the move does.
async fn run_vault_direct_baseline(
    rehearsal: &Rehearsal,
) -> Result<(), Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let client = start_vault_direct(rehearsal).await?;
    wait_for_tracked_receipts(rehearsal, rehearsal.snapshot.len()).await?;
    client.terminate().await;

    let client = start_vault_direct(rehearsal).await?;
    client.terminate().await;

    let pool = connect(&rehearsal.database_url).await?;
    assert_eq!(
        recorded_custody_holder(&pool, evm.chain_id, RKLB_VAULT).await?,
        evm.wallet_address,
        "startup must record custody at the stand-in wallet"
    );
    pool.close().await;

    Ok(())
}

/// Runbook steps 10 and 11 with the service stopped: move every receipt to
/// the orchestrator in bounded chunks, then verify the move. Returns how many
/// transactions the move took.
async fn cut_over(rehearsal: &Rehearsal) -> Result<u64, Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let stand_in = evm.wallet_address;
    let provider = bot_provider(evm).await?;
    let pool = connect(&rehearsal.database_url).await?;
    let underlying: UnderlyingSymbol = UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        &pool,
        &provider,
        Network::Base,
        evm.chain_id,
        RKLB_VAULT,
        &underlying,
    )
    .await?;
    let destination =
        CorroboratedRecipient::verify(&provider, stand_in, ORCHESTRATOR)
            .await?;
    assert_eq!(
        destination.kind(),
        RecipientKind::Erc1155Receiver,
        "the orchestrator must corroborate as an ERC-1155 receiving contract"
    );

    let block_before = provider.get_block_number().await?;
    let outcome = migrate_vault_receipts(
        &pool,
        &provider,
        identity,
        destination,
        CustodyAfterMove::StaysWithHolder,
    )
    .await?;
    let chunks = provider.get_block_number().await? - block_before;
    assert!(
        matches!(
            outcome,
            MigrationOutcome::Migrated { receipts, .. }
                if receipts == rehearsal.snapshot.len()
        ),
        "the cutover must move every receipt, got {outcome:?}"
    );
    let expected_chunks = u64::try_from(
        rehearsal.snapshot.len().div_ceil(RECEIPTS_PER_TRANSFER),
    )?;
    assert_eq!(
        chunks, expected_chunks,
        "each chunk of at most {RECEIPTS_PER_TRANSFER} receipts is one \
         transaction"
    );

    let receipt = receipt_contract(&provider).await?;
    for (id, amount) in &rehearsal.snapshot {
        assert_eq!(
            receipt.balanceOf(ORCHESTRATOR, *id).call().await?,
            *amount,
            "the orchestrator must gain exactly receipt {id}'s balance"
        );
        assert_eq!(
            receipt.balanceOf(stand_in, *id).call().await?,
            U256::ZERO,
            "the stand-in wallet must keep nothing of receipt {id}"
        );
    }

    let lowest_moved = rehearsal
        .snapshot
        .iter()
        .map(|(id, _)| *id)
        .min()
        .ok_or("the snapshot is empty")?;
    let burn_pointer = ST0xOrchestrator::new(ORCHESTRATOR, &provider)
        .nextBurnReceiptId(RKLB_VAULT)
        .call()
        .await?;
    assert!(
        burn_pointer <= lowest_moved,
        "the burn pointer ({burn_pointer}) must cover the lowest moved \
         receipt ({lowest_moved})"
    );

    assert_cutover_records_no_custody(rehearsal, &pool, &provider, destination)
        .await?;
    pool.close().await;
    println!(
        "cutover: moved {} receipts in {chunks} transactions",
        rehearsal.snapshot.len()
    );

    Ok(chunks)
}

/// The cutover leaves custody at the stand-in wallet and records no
/// migration, and a re-run of the move submits nothing.
async fn assert_cutover_records_no_custody<P>(
    rehearsal: &Rehearsal,
    pool: &sqlx::SqlitePool,
    provider: &P,
    destination: CorroboratedRecipient,
) -> Result<(), Box<dyn Error>>
where
    P: Provider + Clone + Send + Sync,
{
    let evm = &rehearsal.evm;
    assert_eq!(
        recorded_custody_holder(pool, evm.chain_id, RKLB_VAULT).await?,
        evm.wallet_address,
        "the cutover must leave custody at the stand-in wallet"
    );
    assert!(
        recorded_migration_origin(pool, evm.chain_id, RKLB_VAULT)
            .await
            .is_err(),
        "the cutover must not record a custody migration"
    );

    let underlying: UnderlyingSymbol = UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        pool,
        provider,
        Network::Base,
        evm.chain_id,
        RKLB_VAULT,
        &underlying,
    )
    .await?;
    let rerun = migrate_vault_receipts(
        pool,
        provider,
        identity,
        destination,
        CustodyAfterMove::StaysWithHolder,
    )
    .await?;
    assert!(
        matches!(rerun, MigrationOutcome::AlreadyMigrated { receipts } if receipts > 0),
        "re-running a completed move must submit nothing, got {rerun:?}"
    );

    Ok(())
}

/// What the orchestrator-mode phase did, for the record and for the later
/// vault-direct redemption.
struct Operated {
    ap_signer: PrivateKeySigner,
    mint_tx: B256,
    burn_tx: B256,
}

/// Runbook step 12 and the pilot's validation: restart in orchestrator mode,
/// then one mint and one redemption through the service, with Alpaca mocked.
/// Inventory stays empty throughout.
async fn operate_in_orchestrator_mode(
    rehearsal: &Rehearsal,
) -> Result<Operated, Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let mint_callback =
        harness::alpaca_mocks::setup_mint_mocks(&rehearsal.mock_alpaca);
    let (redeem_mock, _poll_mock) =
        harness::alpaca_mocks::setup_redemption_mocks(&rehearsal.mock_alpaca);

    let client = start_orchestrator(rehearsal).await?;
    wait_for_tracked_receipts(rehearsal, 0).await?;

    let ap_signer = PrivateKeySigner::random();
    let ap_wallet = ap_signer.address();
    let fork = fork_provider(evm).await?;
    fork.anvil_set_balance(ap_wallet, tokens(1)).await?;
    let link = harness::setup_account(&client, ap_wallet).await;

    let minted = tokens(5);
    let tokenization_request_id = "fork-rehearsal-mint";
    let issuer_request_id = initiate_mint_request(
        &client,
        ap_wallet,
        &MintFlowRequest {
            client_id: &link.client_id.to_string(),
            tokenization_request_id,
            quantity: "5.0",
            underlying: UNDERLYING,
            token: TOKEN,
            network: Network::Base,
        },
    )
    .await?;
    let nonce = B256::with_last_byte(1);
    let signature =
        signed_mint_authorization(evm, &ap_signer, minted, nonce).await?;
    deliver_mint_authorization(
        &client,
        tokenization_request_id,
        nonce,
        &signature,
    )
    .await;
    confirm_mint_journal(&client, tokenization_request_id, &issuer_request_id)
        .await?;

    let vault = OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, &fork);
    wait_until("the AP to receive the minted shares", || async {
        Ok(vault.balanceOf(ap_wallet).call().await? == minted)
    })
    .await?;
    wait_until("the Alpaca mint callback", || async {
        Ok(mint_callback.calls() >= 1)
    })
    .await?;

    let orchestrator = IST0xOrchestratorV1Instance::new(ORCHESTRATOR, &fork);
    let minted_logs = orchestrator
        .Minted_filter()
        .from_block(rehearsal.fork_block + 1)
        .query()
        .await?;
    let [(minted_event, minted_log)] = minted_logs.as_slice() else {
        return Err(format!(
            "expected exactly one Minted log, got {}",
            minted_logs.len()
        )
        .into());
    };
    assert_eq!(minted_event.token, RKLB_VAULT);
    assert_eq!(minted_event.to, ap_wallet);
    assert_eq!(minted_event.amount, minted);
    let new_receipt_id = vault.highwaterId().call().await?;
    let receipt = receipt_contract(&fork).await?;
    assert_eq!(
        receipt.balanceOf(ORCHESTRATOR, new_receipt_id).call().await?,
        minted,
        "the orchestrator must hold the receipt of its own mint"
    );

    let redeemed = tokens(2);
    send_shares(evm, &ap_signer, redeemed).await?;
    wait_for_events(rehearsal, "RedemptionEvent::OrchestratorTokensBurned", 1)
        .await?;
    assert_eq!(
        vault.balanceOf(evm.wallet_address).call().await?,
        U256::ZERO,
        "the orchestrator burn must consume the redeemed shares"
    );
    assert!(redeem_mock.calls() >= 1, "the Alpaca redeem call must happen");
    let burned_logs = orchestrator
        .Burned_filter()
        .from_block(rehearsal.fork_block + 1)
        .query()
        .await?;
    let [(burned_event, burned_log)] = burned_logs.as_slice() else {
        return Err(format!(
            "expected exactly one Burned log, got {}",
            burned_logs.len()
        )
        .into());
    };
    assert_eq!(burned_event.token, RKLB_VAULT);
    assert_eq!(burned_event.amount, redeemed);

    client.terminate().await;
    let client = start_orchestrator(rehearsal).await?;
    wait_for_tracked_receipts(rehearsal, 0).await?;
    client.terminate().await;
    println!("operate: one orchestrator mint and one orchestrator burn");

    Ok(Operated {
        ap_signer,
        mint_tx: minted_log
            .transaction_hash
            .ok_or("the Minted log has no transaction hash")?,
        burn_tx: burned_log
            .transaction_hash
            .ok_or("the Burned log has no transaction hash")?,
    })
}

/// Runbook step 14 with the service stopped and the config back on
/// vault-direct: the `EMERGENCY_ROLE` holder withdraws every receipt the
/// orchestrator holds, at the amount it holds now. Returns how many receipts
/// came back.
async fn roll_back(rehearsal: &Rehearsal) -> Result<usize, Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let stand_in = evm.wallet_address;
    let fork = fork_provider(evm).await?;
    let holder = emergency_holder(evm, &fork).await?;
    let orchestrator = ST0xOrchestrator::new(ORCHESTRATOR, &fork);
    let receipt = receipt_contract(&fork).await?;
    let vault = OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, &fork);
    let highwater = vault.highwaterId().call().await?;

    let held = holdings(&receipt, ORCHESTRATOR, highwater).await?;
    for (id, amount) in &held {
        let before = receipt.balanceOf(stand_in, *id).call().await?;
        let withdrawn = orchestrator
            .withdrawReceipt(RKLB_VAULT, *id, *amount, stand_in)
            .from(holder)
            .send()
            .await?
            .get_receipt()
            .await?;
        require_success(&withdrawn, "withdrawReceipt")?;
        assert_eq!(
            receipt.balanceOf(stand_in, *id).call().await? - before,
            *amount,
            "the stand-in wallet must gain exactly receipt {id}'s amount"
        );
    }
    assert!(
        holdings(&receipt, ORCHESTRATOR, highwater).await?.is_empty(),
        "the orchestrator must hold nothing in 1..=highwaterId() after the \
         rollback"
    );
    println!("rollback: {holder} returned {} receipts", held.len());

    Ok(held.len())
}

/// The real `EMERGENCY_ROLE` holder when one exists; otherwise the admin
/// grants the role to the stand-in wallet on the fork. Either way, the holder
/// is impersonated, so the same calls work for a Safe and for the stand-in.
async fn emergency_holder<P: Provider>(
    evm: &LocalEvm,
    fork: &P,
) -> Result<Address, Box<dyn Error>> {
    if let Some(holder) = EMERGENCY_HOLDER {
        impersonate(fork, holder).await?;
        return Ok(holder);
    }

    impersonate(fork, ADMIN).await?;
    let orchestrator = ST0xOrchestrator::new(ORCHESTRATOR, fork);
    let emergency_role = orchestrator.EMERGENCY_ROLE().call().await?;
    let granted = orchestrator
        .grantRole(emergency_role, evm.wallet_address)
        .from(ADMIN)
        .send()
        .await?
        .get_receipt()
        .await?;
    require_success(&granted, "granting EMERGENCY_ROLE")?;
    impersonate(fork, evm.wallet_address).await?;
    println!(
        "substitution: no EMERGENCY_ROLE holder at the fork block, so {ADMIN} \
         granted it to the stand-in wallet"
    );

    Ok(evm.wallet_address)
}

/// Runbook step 14's restart and its completeness check: inventory tracks
/// every returned receipt at its on-chain balance, and custody never moved.
async fn rediscover_returned_receipts(
    rehearsal: &Rehearsal,
    returned: usize,
) -> Result<(), Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let client = start_vault_direct(rehearsal).await?;
    wait_for_tracked_receipts(rehearsal, returned).await?;
    client.terminate().await;

    let provider = bot_provider(evm).await?;
    let pool = connect(&rehearsal.database_url).await?;
    assert_eq!(
        recorded_custody_holder(&pool, evm.chain_id, RKLB_VAULT).await?,
        evm.wallet_address,
        "custody must still be at the stand-in wallet after the rollback"
    );
    assert!(
        recorded_migration_origin(&pool, evm.chain_id, RKLB_VAULT)
            .await
            .is_err(),
        "no custody migration may be recorded across cutover and rollback"
    );
    let underlying: UnderlyingSymbol = UNDERLYING.parse()?;
    let identity = VaultIdentity::verify(
        &pool,
        &provider,
        Network::Base,
        evm.chain_id,
        RKLB_VAULT,
        &underlying,
    )
    .await?;
    let verified =
        confirm_custody_holder(&pool, provider, identity, evm.wallet_address)
            .await?;
    assert_eq!(
        verified, returned,
        "inventory must track every returned receipt at its on-chain balance"
    );
    pool.close().await;
    println!("rediscovery: inventory tracks all {returned} returned receipts");

    Ok(())
}

/// One vault-direct redemption against the returned receipts.
async fn redeem_vault_direct(
    rehearsal: &Rehearsal,
    operated: &Operated,
) -> Result<(), Box<dyn Error>> {
    let evm = &rehearsal.evm;
    let client = start_vault_direct(rehearsal).await?;
    send_shares(evm, &operated.ap_signer, tokens(1)).await?;
    wait_for_events(rehearsal, "RedemptionEvent::TokensBurned", 1).await?;
    let fork = fork_provider(evm).await?;
    let vault = OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, &fork);
    assert_eq!(
        vault.balanceOf(evm.wallet_address).call().await?,
        U256::ZERO,
        "the vault-direct burn must consume the redeemed shares"
    );
    client.terminate().await;
    println!("vault-direct: one redemption burned against returned receipts");

    Ok(())
}

/// Signs the orchestrator's own `mintAuthDigest` with the AP's key, as the
/// liquidity bot does before it delivers the authorization.
async fn signed_mint_authorization(
    evm: &LocalEvm,
    recipient_signer: &PrivateKeySigner,
    amount: U256,
    nonce: B256,
) -> Result<Bytes, Box<dyn Error>> {
    let fork = fork_provider(evm).await?;
    let digest = IST0xOrchestratorV1Instance::new(ORCHESTRATOR, &fork)
        .mintAuthDigest(RKLB_VAULT, recipient_signer.address(), amount, nonce)
        .call()
        .await?;
    let signature = recipient_signer.sign_hash_sync(&digest)?;
    Ok(Bytes::from(signature.as_bytes().to_vec()))
}

/// Delivers the authorization through
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

/// The AP sends `amount` shares to the bot wallet, which starts a redemption.
async fn send_shares(
    evm: &LocalEvm,
    ap_signer: &PrivateKeySigner,
    amount: U256,
) -> Result<(), Box<dyn Error>> {
    let ap_provider = create_provider()
        .wallet(EthereumWallet::from(ap_signer.clone()))
        .connect(&evm.endpoint)
        .await?;
    let sent = OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, &ap_provider)
        .transfer(evm.wallet_address, amount)
        .send()
        .await?
        .get_receipt()
        .await?;
    require_success(&sent, "the AP's share transfer")
}

fn vault_direct_config(
    rehearsal: &Rehearsal,
) -> Result<Config, Box<dyn Error>> {
    let config = harness::create_config_with_db(
        &rehearsal.database_url,
        &rehearsal.mock_alpaca,
        &rehearsal.evm,
    )?;
    Ok(with_fork_backfill(config, rehearsal))
}

/// RKLB in orchestrator mode on Base, as runbook step 12 flips it.
fn orchestrator_config(
    rehearsal: &Rehearsal,
) -> Result<Config, Box<dyn Error>> {
    let config = harness::create_config_with_vault_modes(
        &rehearsal.database_url,
        &rehearsal.mock_alpaca,
        &rehearsal.evm,
        orchestrator_vault_modes(UNDERLYING, ORCHESTRATOR),
    )?;
    Ok(with_fork_backfill(config, rehearsal))
}

/// Starts every backfill at the fork block. The receipts reach the stand-in
/// wallet after it, and a scan from block 0 would read Base's whole history
/// through the upstream RPC.
fn with_fork_backfill(mut config: Config, rehearsal: &Rehearsal) -> Config {
    config.backfill_start_block = rehearsal.fork_block;
    for chain in &mut config.chains {
        chain.backfill_start_block = rehearsal.fork_block;
    }
    config
}

async fn start_vault_direct(
    rehearsal: &Rehearsal,
) -> Result<Client, Box<dyn Error>> {
    let config = vault_direct_config(rehearsal)?;
    start_service(config).await
}

async fn start_orchestrator(
    rehearsal: &Rehearsal,
) -> Result<Client, Box<dyn Error>> {
    let config = orchestrator_config(rehearsal)?;
    start_service(config).await
}

async fn start_service(config: Config) -> Result<Client, Box<dyn Error>> {
    let rocket = initialize_rocket(config).await?;
    Ok(Client::tracked(rocket).await?)
}

/// A provider without a wallet: transactions go out as
/// `eth_sendTransaction`, which the fork signs for impersonated accounts.
async fn fork_provider(
    evm: &LocalEvm,
) -> Result<impl Provider + Clone, Box<dyn Error>> {
    Ok(ProviderBuilder::new().connect(&evm.endpoint).await?)
}

async fn impersonate<P: Provider>(
    fork: &P,
    account: Address,
) -> Result<(), Box<dyn Error>> {
    fork.anvil_impersonate_account(account).await?;
    fork.anvil_set_balance(account, tokens(1)).await?;
    Ok(())
}

async fn receipt_contract<P: Provider>(
    provider: &P,
) -> Result<ReceiptInstance<&P>, Box<dyn Error>> {
    let receipt_address: Address =
        OffchainAssetReceiptVaultInstance::new(RKLB_VAULT, provider)
            .receipt()
            .call()
            .await?
            .0
            .into();
    Ok(ReceiptInstance::new(receipt_address, provider))
}

/// Every receipt id in `1..=highwater` that `holder` holds, with its amount.
async fn holdings<P: Provider>(
    receipt: &ReceiptInstance<P>,
    holder: Address,
    highwater: U256,
) -> Result<Vec<Holding>, Box<dyn Error>> {
    let mut held = Vec::new();
    let last = u64::try_from(highwater)?;
    for id in (1..=last).map(U256::from) {
        let amount = receipt.balanceOf(holder, id).call().await?;
        if !amount.is_zero() {
            held.push((id, amount));
        }
    }
    Ok(held)
}

fn require_success(
    receipt: &TransactionReceipt,
    what: &str,
) -> Result<(), Box<dyn Error>> {
    if receipt.status() {
        Ok(())
    } else {
        Err(format!("{what} reverted in {}", receipt.transaction_hash).into())
    }
}

async fn connect(
    database_url: &str,
) -> Result<sqlx::SqlitePool, Box<dyn Error>> {
    Ok(SqlitePoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?)
}

async fn wait_for_tracked_receipts(
    rehearsal: &Rehearsal,
    expected: usize,
) -> Result<(), Box<dyn Error>> {
    let pool = connect(&rehearsal.database_url).await?;
    let chain_id = rehearsal.evm.chain_id;
    wait_until(
        &format!("inventory to track exactly {expected} receipts"),
        || async {
            Ok(tracked_receipt_count(&pool, chain_id, RKLB_VAULT).await?
                == expected)
        },
    )
    .await?;
    pool.close().await;
    Ok(())
}

async fn wait_for_events(
    rehearsal: &Rehearsal,
    event_type: &str,
    expected: i64,
) -> Result<(), Box<dyn Error>> {
    let pool = connect(&rehearsal.database_url).await?;
    wait_until(&format!("{expected} {event_type} event(s)"), || async {
        let recorded: i64 = sqlx::query_scalar(
            "
            SELECT COUNT(*)
            FROM events
            WHERE event_type = ?
            ",
        )
        .bind(event_type)
        .fetch_one(&pool)
        .await?;
        Ok(recorded >= expected)
    })
    .await?;
    pool.close().await;
    Ok(())
}

/// Polls `check` until it answers true, for at most [`FORK_WAIT`].
async fn wait_until<Check, Answer>(
    what: &str,
    mut check: Check,
) -> Result<(), Box<dyn Error>>
where
    Check: FnMut() -> Answer,
    Answer: Future<Output = Result<bool, Box<dyn Error>>>,
{
    let deadline = tokio::time::Instant::now() + FORK_WAIT;
    loop {
        if check().await? {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for {what}").into());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
