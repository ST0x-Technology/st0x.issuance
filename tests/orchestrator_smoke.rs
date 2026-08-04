#![allow(clippy::unwrap_used)]

//! Smoke test for `ST0xOrchestrator` integration: deploy → mint → burn.
//!
//! Verifies that the test harness's `deploy_orchestrator()` correctly wires the
//! orchestrator to the vault and that a signed EIP-712 mint auth round-trips
//! through a full mint-then-burn cycle on Anvil.

mod harness;

use alloy::network::EthereumWallet;
use alloy::primitives::{B256, Bytes, U256};
use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use st0x_issuance::bindings::{IST0xOrchestratorV1, OffchainAssetReceiptVault};
use st0x_issuance::test_utils::LocalEvm;

use crate::harness::create_provider;

/// Full mint-then-burn cycle through the orchestrator.
///
/// 1. Deploy vault + orchestrator (grants MINT_ROLE / BURN_ROLE to deployer).
/// 2. Build a `MintAuthV1` digest, sign it with the deployer's key.
/// 3. Mint shares to the deployer via `orchestrator.mint()`.
/// 4. Approve the orchestrator allowance, then burn via `orchestrator.burn()`.
/// 5. Assert ERC-20 share balance returns to zero.
#[tokio::test]
async fn orchestrator_mint_burn_roundtrip()
-> Result<(), Box<dyn std::error::Error>> {
    let evm = LocalEvm::new().await?;

    // Certify the vault so deposits and withdrawals are enabled.
    evm.grant_certify_role(evm.wallet_address).await?;
    let far_future = U256::from(u64::MAX);
    evm.certify_vault(far_future).await?;

    let orchestrator_address = evm.deploy_orchestrator().await?;

    let signer = PrivateKeySigner::from_bytes(&evm.private_key)?;
    let wallet = EthereumWallet::from(signer.clone());

    let provider =
        create_provider().wallet(wallet).connect(&evm.endpoint).await?;

    let orchestrator =
        IST0xOrchestratorV1::new(orchestrator_address, &provider);
    let vault = OffchainAssetReceiptVault::new(evm.vault_address, &provider);

    let amount = U256::from(1_000_000u64);
    let nonce = B256::ZERO;

    // Obtain the EIP-712 digest the orchestrator expects us to sign.
    let digest = orchestrator
        .mintAuthDigest(evm.vault_address, evm.wallet_address, amount, nonce)
        .call()
        .await?;

    // Sign the digest off-chain — alloy's `sign_hash_sync` signs the raw bytes.
    let signature = signer.sign_hash_sync(&digest)?;

    let auth = IST0xOrchestratorV1::MintAuthV1 {
        nonce,
        signature: Bytes::from(signature.as_bytes().to_vec()),
    };

    orchestrator
        .mint(evm.vault_address, evm.wallet_address, amount, auth, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let balance_after_mint = vault.balanceOf(evm.wallet_address).call().await?;
    assert!(
        balance_after_mint > U256::ZERO,
        "expected positive share balance after mint, got {balance_after_mint}"
    );

    // Approve the orchestrator to pull shares, then burn.
    vault
        .approve(orchestrator_address, balance_after_mint)
        .send()
        .await?
        .get_receipt()
        .await?;

    orchestrator
        .burn(evm.vault_address, balance_after_mint, Bytes::new())
        .send()
        .await?
        .get_receipt()
        .await?;

    let balance_after_burn = vault.balanceOf(evm.wallet_address).call().await?;
    assert_eq!(
        balance_after_burn,
        U256::ZERO,
        "expected zero share balance after burn, got {balance_after_burn}"
    );

    Ok(())
}
