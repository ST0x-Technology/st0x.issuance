//! Pre-cutover orchestrator onboarding checks.
//!
//! Before an asset's `vault_mode` may flip to orchestrator, the ops checklist
//! requires — verified by on-chain reads — that the bot wallet holds
//! `MINT_ROLE` + `BURN_ROLE` on the orchestrator, that the orchestrator's
//! vault-logic version lock is healthy, that the orchestrator holds
//! `DEPOSIT` + `WITHDRAW` on each vault's authorizer, and that each asset's
//! one-time unlimited ERC-20 approval (bot → orchestrator, on the vault
//! share token) has been executed. This module is that verification:
//! read-only, generic over the provider, consumed by the issuer CLI's
//! preflight subcommand.

use alloy::network::{
    Ethereum, EthereumWallet, TransactionBuilder, TransactionBuilderError,
};
use alloy::primitives::{Address, B256, Bytes, U256, keccak256};
use alloy::providers::Provider;
use alloy::rpc::types::TransactionRequest;
use alloy::sol_types::SolCall;
use std::fmt::{self, Display, Formatter};
use tracing::{debug, info};

use crate::bindings::{
    IST0xOrchestratorV1, OffchainAssetReceiptVault,
    OffchainAssetReceiptVaultAuthorizerV1, Receipt, ST0xOrchestrator,
};
use crate::mint::UnderlyingSymbol;

/// Everything the pre-cutover gate checks, in one report. The role and
/// vault-logic facts are orchestrator-wide; approvals are per asset.
///
/// The three booleans are independent on-chain facts (each combination is a
/// real, reportable state), not one status split across flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrchestratorReadiness {
    pub(crate) orchestrator: Address,
    pub(crate) bot: Address,
    pub(crate) mint_role_granted: bool,
    pub(crate) burn_role_granted: bool,
    pub(crate) vault_logic_expected: bool,
    pub(crate) assets: Vec<AssetApprovalReadiness>,
}

/// One asset's ERC-20 allowance from the bot wallet to the orchestrator, read
/// from the vault contract (the vault IS the share token — the approval's
/// target contract is the vault, the spender is the orchestrator).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssetApprovalReadiness {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) vault: Address,
    pub(crate) allowance: U256,
    /// Whether the ORCHESTRATOR holds `DEPOSIT` on this vault's authorizer —
    /// without it the vault reverts the orchestrator's deposit inside
    /// `mint()`, a hard prerequisite no orchestrator-side read covers
    /// (`vaultLogicIsExpected()` is the implementation version lock, not an
    /// authorization check).
    pub(crate) deposit_role_granted: bool,
    /// Whether the orchestrator holds `WITHDRAW` on this vault's authorizer
    /// — the burn-side counterpart of `deposit_role_granted`.
    pub(crate) withdraw_role_granted: bool,
}

impl OrchestratorReadiness {
    /// The pre-cutover gate: both bot roles granted, vault logic healthy,
    /// and every checked asset approved unlimited with the orchestrator
    /// authorized on its vault's authorizer.
    pub(crate) fn is_ready(&self) -> bool {
        self.mint_role_granted
            && self.burn_role_granted
            && self.vault_logic_expected
            && self.assets.iter().all(AssetApprovalReadiness::is_ready)
    }
}

impl AssetApprovalReadiness {
    /// The approval strategy is a one-time UNLIMITED approval per token
    /// (SPEC Decision 5), so the pass criterion is exactly `U256::MAX` — a
    /// finite allowance means the one-time approval step has not run for
    /// this asset. Fail direction is safe: should an allowance ever read
    /// below `U256::MAX`, this reports NOT READY and the (idempotent)
    /// approval step is simply re-run; burns are additionally protected by
    /// their own per-burn allowance gate in
    /// [`super::VaultService::check_orchestrator_burn_readiness`].
    pub(crate) fn is_unlimited(&self) -> bool {
        self.allowance == U256::MAX
    }

    /// This asset is cutover-ready: approval unlimited and the orchestrator
    /// authorized for both vault operations on this vault's authorizer.
    pub(crate) fn is_ready(&self) -> bool {
        self.is_unlimited()
            && self.deposit_role_granted
            && self.withdraw_role_granted
    }
}

impl Display for OrchestratorReadiness {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        writeln!(formatter, "Orchestrator onboarding readiness")?;
        writeln!(formatter, "  orchestrator: {}", self.orchestrator)?;
        writeln!(formatter, "  bot wallet:   {}", self.bot)?;
        writeln!(
            formatter,
            "  [{}] MINT_ROLE granted to bot wallet",
            pass_fail(self.mint_role_granted)
        )?;
        writeln!(
            formatter,
            "  [{}] BURN_ROLE granted to bot wallet",
            pass_fail(self.burn_role_granted)
        )?;
        writeln!(
            formatter,
            "  [{}] vaultLogicIsExpected()",
            pass_fail(self.vault_logic_expected)
        )?;
        for asset in &self.assets {
            writeln!(
                formatter,
                "  [{}] {} (vault {}): allowance {}",
                pass_fail(asset.is_unlimited()),
                asset.underlying,
                asset.vault,
                if asset.is_unlimited() {
                    "unlimited".to_string()
                } else {
                    asset.allowance.to_string()
                }
            )?;
            writeln!(
                formatter,
                "  [{}] {} authorizer: DEPOSIT granted to orchestrator",
                pass_fail(asset.deposit_role_granted),
                asset.underlying,
            )?;
            writeln!(
                formatter,
                "  [{}] {} authorizer: WITHDRAW granted to orchestrator",
                pass_fail(asset.withdraw_role_granted),
                asset.underlying,
            )?;
        }
        write!(
            formatter,
            "Overall: {}",
            if self.is_ready() { "READY" } else { "NOT READY" }
        )
    }
}

/// Result of the idempotent per-asset approval step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApprovalOutcome {
    /// The allowance was already unlimited; no transaction was sent.
    AlreadyUnlimited,
    /// An `approve(orchestrator, U256::MAX)` landed and the re-read
    /// allowance is unlimited.
    Approved { tx_hash: B256 },
}

/// One transaction shape the Turnkey signing policy must allow. Target and
/// calldata are fixed at build time, so tests can decode exactly what would
/// be signed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SigningShape {
    pub(crate) label: &'static str,
    pub(crate) to: Address,
    pub(crate) calldata: Bytes,
}

/// Proof that one shape was signed — never broadcast.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SigningShapeProof {
    pub(crate) label: &'static str,
    pub(crate) to: Address,
    /// Hash of the signed, unbroadcast transaction.
    pub(crate) tx_hash: B256,
}

/// Errors from the on-chain readiness reads, the approval step, and the
/// sign-only policy proof.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OnboardingError {
    #[error(transparent)]
    Contract(#[from] alloy::contract::Error),
    #[error(transparent)]
    PendingTransaction(#[from] alloy::providers::PendingTransactionError),
    #[error(transparent)]
    Transport(#[from] alloy::transports::TransportError),
    #[error("approve transaction {tx_hash} reverted on-chain")]
    ApprovalReverted { tx_hash: B256 },
    #[error(
        "approve transaction {tx_hash} succeeded but the re-read allowance \
         is {allowance}, not unlimited"
    )]
    ApprovalNotEffective { tx_hash: B256, allowance: U256 },
    #[error(
        "failed to sign the {label} shape — a Turnkey signing-policy denial \
         surfaces here"
    )]
    SigningRejected {
        label: &'static str,
        #[source]
        source: Box<TransactionBuilderError<Ethereum>>,
    },
}

/// Reads the full pre-cutover readiness state: `hasRole(MINT_ROLE, bot)`,
/// `hasRole(BURN_ROLE, bot)`, `vaultLogicIsExpected()`, and per asset
/// `allowance(bot, orchestrator)` on the vault share token. Read-only — never
/// signs or submits anything.
///
/// # Errors
///
/// Returns an error if any on-chain read fails; a partially-checked state is
/// never reported as a result.
pub(crate) async fn check_orchestrator_readiness<P: Provider>(
    provider: &P,
    orchestrator: Address,
    bot: Address,
    assets: &[(UnderlyingSymbol, Address)],
) -> Result<OrchestratorReadiness, OnboardingError> {
    let orchestrator_contract = ST0xOrchestrator::new(orchestrator, provider);
    let mint_role = orchestrator_contract.MINT_ROLE().call().await?;
    let burn_role = orchestrator_contract.BURN_ROLE().call().await?;
    let mint_role_granted =
        orchestrator_contract.hasRole(mint_role, bot).call().await?;
    let burn_role_granted =
        orchestrator_contract.hasRole(burn_role, bot).call().await?;
    let vault_logic_expected =
        orchestrator_contract.vaultLogicIsExpected().call().await?;

    let mut asset_readiness = Vec::with_capacity(assets.len());
    for (underlying, vault) in assets {
        let vault_contract = OffchainAssetReceiptVault::new(*vault, provider);
        let allowance =
            vault_contract.allowance(bot, orchestrator).call().await?;
        // The vault authorizes deposits/withdrawals through its authorizer
        // contract, resolved on-chain — the orchestrator must hold both
        // roles there or the vault reverts its calls at the first live
        // mint/burn. `vaultLogicIsExpected()` above cannot detect this: it
        // is the implementation version lock, not an authorization check.
        let authorizer = vault_contract.authorizer().call().await?;
        let authorizer_contract = OffchainAssetReceiptVaultAuthorizerV1::new(
            Address::from(authorizer.0),
            provider,
        );
        let deposit_role_granted = authorizer_contract
            .hasRole(keccak256(b"DEPOSIT"), orchestrator)
            .call()
            .await?;
        let withdraw_role_granted = authorizer_contract
            .hasRole(keccak256(b"WITHDRAW"), orchestrator)
            .call()
            .await?;
        debug!(
            target: "vault",
            underlying = %underlying,
            vault = %vault,
            %allowance,
            deposit_role_granted,
            withdraw_role_granted,
            "Checked orchestrator allowance and authorizer grants"
        );
        asset_readiness.push(AssetApprovalReadiness {
            underlying: underlying.clone(),
            vault: *vault,
            allowance,
            deposit_role_granted,
            withdraw_role_granted,
        });
    }

    let readiness = OrchestratorReadiness {
        orchestrator,
        bot,
        mint_role_granted,
        burn_role_granted,
        vault_logic_expected,
        assets: asset_readiness,
    };
    info!(
        target: "vault",
        orchestrator = %orchestrator,
        bot = %bot,
        ready = readiness.is_ready(),
        asset_count = readiness.assets.len(),
        "Orchestrator readiness checked"
    );

    Ok(readiness)
}

/// Executes the one-time unlimited approval (SPEC Decision 5) of the vault
/// share token for the orchestrator, idempotently: an already-unlimited
/// allowance sends nothing, so re-runs and batch scripts are safe.
///
/// `signing_provider` must sign as `bot` — the allowance is read and
/// re-verified for `bot`, so a mismatched signer cannot silently approve from
/// the wrong wallet: the post-send re-read would still see a non-unlimited
/// allowance for `bot` and fail with [`OnboardingError::ApprovalNotEffective`].
/// Success is never inferred from the receipt alone; the allowance the burns
/// will rely on is re-read from the chain.
///
/// # Errors
///
/// Returns an error if any read or the send fails, if the approve
/// transaction reverts, or if the re-read allowance is not unlimited.
pub(crate) async fn ensure_unlimited_approval<P: Provider>(
    signing_provider: &P,
    vault: Address,
    orchestrator: Address,
    bot: Address,
) -> Result<ApprovalOutcome, OnboardingError> {
    let vault_contract =
        OffchainAssetReceiptVault::new(vault, signing_provider);
    let current = vault_contract.allowance(bot, orchestrator).call().await?;
    if current == U256::MAX {
        info!(
            target: "vault",
            %vault,
            %orchestrator,
            bot = %bot,
            "Orchestrator allowance already unlimited; nothing to send"
        );
        return Ok(ApprovalOutcome::AlreadyUnlimited);
    }

    let receipt = vault_contract
        .approve(orchestrator, U256::MAX)
        .send()
        .await?
        .get_receipt()
        .await?;
    let tx_hash = receipt.transaction_hash;
    if !receipt.status() {
        return Err(OnboardingError::ApprovalReverted { tx_hash });
    }

    let allowance = vault_contract.allowance(bot, orchestrator).call().await?;
    if allowance != U256::MAX {
        return Err(OnboardingError::ApprovalNotEffective {
            tx_hash,
            allowance,
        });
    }

    info!(
        target: "vault",
        %vault,
        %orchestrator,
        bot = %bot,
        %tx_hash,
        "Approved unlimited orchestrator allowance"
    );

    Ok(ApprovalOutcome::Approved { tx_hash })
}

/// Signs — without ever broadcasting — one transaction per shape the Turnkey
/// policy must allow before an asset's cutover: `mint` and `burn` on the
/// orchestrator, `approve` on the vault share token, and ERC-1155
/// `safeBatchTransferFrom` to the orchestrator on the vault's receipt
/// contract — the batch selector, because that is the shape every
/// Turnkey-signed receipt move in the migration tooling submits. A policy gap
/// found here costs nothing; found during the pilot's first live mint it
/// stalls a real customer flow.
///
/// The receipt contract is resolved on-chain from the vault, mirroring the
/// migration tooling — never typed.
///
/// # Errors
///
/// Returns an error if an on-chain read fails or a shape fails to sign;
/// [`OnboardingError::SigningRejected`] names the shape, so a policy denial
/// is attributable to the exact allowance that is missing.
pub(crate) async fn prove_signing_shapes<P: Provider>(
    provider: &P,
    wallet: &EthereumWallet,
    orchestrator: Address,
    vault: Address,
    bot: Address,
) -> Result<Vec<SigningShapeProof>, OnboardingError> {
    let receipt_contract = Address::from(
        OffchainAssetReceiptVault::new(vault, provider)
            .receipt()
            .call()
            .await?
            .0,
    );
    let chain_id = provider.get_chain_id().await?;
    let mut nonce = provider.get_transaction_count(bot).await?;

    let shapes =
        build_signing_shapes(orchestrator, vault, receipt_contract, bot);
    let mut proofs = Vec::with_capacity(shapes.len());
    for shape in shapes {
        let request = TransactionRequest::default()
            .with_from(bot)
            .with_to(shape.to)
            .with_input(shape.calldata)
            .with_chain_id(chain_id)
            .with_nonce(nonce)
            .with_gas_limit(SIGNING_PROOF_GAS_LIMIT)
            .with_max_fee_per_gas(SIGNING_PROOF_MAX_FEE_PER_GAS)
            .with_max_priority_fee_per_gas(SIGNING_PROOF_MAX_PRIORITY_FEE);
        nonce += 1;

        // Signing only: built and signed, never submitted. The Turnkey signer
        // verifies the recovered signature matches its address before
        // returning, so success proves control, not just an HTTP 200.
        let envelope = request.build(wallet).await.map_err(|source| {
            OnboardingError::SigningRejected {
                label: shape.label,
                source: Box::new(source),
            }
        })?;
        debug!(
            target: "vault",
            label = shape.label,
            to = %shape.to,
            tx_hash = %envelope.tx_hash(),
            "Signed policy-proof shape without broadcasting"
        );
        proofs.push(SigningShapeProof {
            label: shape.label,
            to: shape.to,
            tx_hash: *envelope.tx_hash(),
        });
    }

    info!(
        target: "vault",
        %orchestrator,
        %vault,
        shapes = proofs.len(),
        "Signed every policy-proof shape without broadcasting"
    );

    Ok(proofs)
}

/// The four transaction shapes, with harmless placeholder values (one wei,
/// zero nonce, empty signature/data): a signing policy evaluates the target
/// contract and calldata shape, the transactions are never broadcast, and gas
/// is fixed by the caller so nothing is ever estimated (estimating the
/// dummy-auth mint would revert).
fn build_signing_shapes(
    orchestrator: Address,
    vault: Address,
    receipt_contract: Address,
    bot: Address,
) -> Vec<SigningShape> {
    vec![
        SigningShape {
            label: "orchestrator.mint",
            to: orchestrator,
            calldata: IST0xOrchestratorV1::mintCall {
                token: vault,
                to: bot,
                amount: U256::ONE,
                auth: IST0xOrchestratorV1::MintAuthV1 {
                    nonce: B256::ZERO,
                    signature: Bytes::new(),
                },
                receiptInformation: Bytes::new(),
            }
            .abi_encode()
            .into(),
        },
        SigningShape {
            label: "orchestrator.burn",
            to: orchestrator,
            calldata: IST0xOrchestratorV1::burnCall {
                token: vault,
                amount: U256::ONE,
                burnInfo: Bytes::new(),
            }
            .abi_encode()
            .into(),
        },
        SigningShape {
            label: "vault.approve",
            to: vault,
            calldata: OffchainAssetReceiptVault::approveCall {
                spender: orchestrator,
                amount: U256::MAX,
            }
            .abi_encode()
            .into(),
        },
        // The BATCH form, not `safeTransferFrom`: every Turnkey-signed
        // receipt move in this repo is `safeBatchTransferFrom` (the custody
        // migration's rollback leg and `verify_rollback_signing` both build
        // it), and a Turnkey policy grants contract + selector — proving the
        // singular selector would pass while the real cutover migration is
        // denied.
        SigningShape {
            label: "receipt.safeBatchTransferFrom",
            to: receipt_contract,
            calldata: Receipt::safeBatchTransferFromCall {
                from: bot,
                to: orchestrator,
                ids: vec![U256::ONE],
                amounts: vec![U256::ONE],
                data: Bytes::new(),
            }
            .abi_encode()
            .into(),
        },
    ]
}

/// Deliberately generous and fixed rather than estimated: a signature's
/// validity does not depend on gas or fee values, the transactions are never
/// broadcast, and estimation would `eth_call` the dummy-auth mint and revert.
const SIGNING_PROOF_GAS_LIMIT: u64 = 1_000_000;
const SIGNING_PROOF_MAX_FEE_PER_GAS: u128 = 100_000_000_000;
const SIGNING_PROOF_MAX_PRIORITY_FEE: u128 = 1_000_000_000;

const fn pass_fail(passed: bool) -> &'static str {
    if passed { "PASS" } else { "FAIL" }
}

#[cfg(test)]
mod tests {
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::MockServer;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::test_utils::{LocalEvm, logs_contain_at};
    use crate::wallet::turnkey::test_wallet_against;

    fn rklb() -> UnderlyingSymbol {
        UnderlyingSymbol::new("RKLB").unwrap()
    }

    async fn approve(evm: &LocalEvm, orchestrator: Address, amount: U256) {
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();

        OffchainAssetReceiptVault::new(evm.vault_address, &provider)
            .approve(orchestrator, amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
    }

    async fn revoke_authorizer_role(
        evm: &LocalEvm,
        role_name: &[u8],
        from: Address,
    ) {
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();

        OffchainAssetReceiptVaultAuthorizerV1::new(
            evm.authorizer_address,
            &provider,
        )
        .revokeRole(keccak256(role_name), from)
        .send()
        .await
        .unwrap()
        .get_receipt()
        .await
        .unwrap();
    }

    async fn readiness(
        evm: &LocalEvm,
        orchestrator: Address,
        bot: Address,
    ) -> OrchestratorReadiness {
        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        check_orchestrator_readiness(
            &provider,
            orchestrator,
            bot,
            &[(rklb(), evm.vault_address)],
        )
        .await
        .unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn deployer_with_roles_but_no_approval_is_not_ready() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();

        let report = readiness(&evm, orchestrator, evm.wallet_address).await;

        assert!(report.mint_role_granted);
        assert!(report.burn_role_granted);
        assert!(report.vault_logic_expected);
        assert_eq!(report.assets.len(), 1);
        assert_eq!(report.assets[0].allowance, U256::ZERO);
        assert!(!report.is_ready());
        assert!(logs_contain_at!(
            Level::INFO,
            &["Orchestrator readiness checked", "ready=false"]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Checked orchestrator allowance", "RKLB"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn unlimited_approval_makes_deployer_ready() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        approve(&evm, orchestrator, U256::MAX).await;

        let report = readiness(&evm, orchestrator, evm.wallet_address).await;

        assert!(report.assets[0].is_unlimited());
        assert!(report.assets[0].deposit_role_granted);
        assert!(report.assets[0].withdraw_role_granted);
        assert!(report.is_ready());
        assert!(logs_contain_at!(
            Level::INFO,
            &["Orchestrator readiness checked", "ready=true"]
        ));
    }

    /// The authorizer grants are a hard prerequisite nothing
    /// orchestrator-side covers (`vaultLogicIsExpected()` is the version
    /// lock, not an authorization check): with WITHDRAW revoked from the
    /// orchestrator, the bot roles, vault logic, and allowance all pass
    /// while the report refuses READY — the state that would otherwise
    /// revert the first live burn inside the vault.
    #[traced_test]
    #[tokio::test]
    async fn revoked_authorizer_grant_is_not_ready() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        approve(&evm, orchestrator, U256::MAX).await;
        revoke_authorizer_role(&evm, b"WITHDRAW", orchestrator).await;

        let report = readiness(&evm, orchestrator, evm.wallet_address).await;

        assert!(report.mint_role_granted);
        assert!(report.burn_role_granted);
        assert!(report.vault_logic_expected);
        assert!(report.assets[0].is_unlimited());
        assert!(report.assets[0].deposit_role_granted);
        assert!(
            !report.assets[0].withdraw_role_granted,
            "the revoked grant must be reported"
        );
        assert!(
            !report.is_ready(),
            "a missing authorizer grant must refuse READY"
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Orchestrator readiness checked", "ready=false"]
        ));
    }

    #[tokio::test]
    async fn foreign_wallet_has_no_roles() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();

        let report = readiness(&evm, orchestrator, Address::random()).await;

        assert!(!report.mint_role_granted);
        assert!(!report.burn_role_granted);
        assert!(!report.is_ready());
    }

    #[tokio::test]
    async fn finite_allowance_is_not_unlimited() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        approve(&evm, orchestrator, U256::from(10).pow(U256::from(18))).await;

        let report = readiness(&evm, orchestrator, evm.wallet_address).await;

        assert!(!report.assets[0].is_unlimited());
        assert!(!report.is_ready());
    }

    async fn signing_provider(evm: &LocalEvm) -> impl Provider + use<> {
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap()
    }

    #[traced_test]
    #[tokio::test]
    async fn first_approval_sends_verifies_and_reports_the_tx() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        let provider = signing_provider(&evm).await;

        let outcome = ensure_unlimited_approval(
            &provider,
            evm.vault_address,
            orchestrator,
            evm.wallet_address,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ApprovalOutcome::Approved { .. }));
        let report = readiness(&evm, orchestrator, evm.wallet_address).await;
        assert!(report.assets[0].is_unlimited());
        assert!(logs_contain_at!(
            Level::INFO,
            &["Approved unlimited orchestrator allowance", "tx_hash"]
        ));
    }

    #[traced_test]
    #[tokio::test]
    async fn repeated_approval_is_a_no_op_without_a_transaction() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        let provider = signing_provider(&evm).await;

        ensure_unlimited_approval(
            &provider,
            evm.vault_address,
            orchestrator,
            evm.wallet_address,
        )
        .await
        .unwrap();
        let nonce_after_first =
            provider.get_transaction_count(evm.wallet_address).await.unwrap();

        let outcome = ensure_unlimited_approval(
            &provider,
            evm.vault_address,
            orchestrator,
            evm.wallet_address,
        )
        .await
        .unwrap();

        assert_eq!(outcome, ApprovalOutcome::AlreadyUnlimited);
        assert_eq!(
            provider.get_transaction_count(evm.wallet_address).await.unwrap(),
            nonce_after_first,
            "an already-unlimited allowance must not send a transaction"
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["already unlimited", "nothing to send"]
        ));
    }

    #[tokio::test]
    async fn finite_allowance_upgrades_to_unlimited() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        approve(&evm, orchestrator, U256::from(10).pow(U256::from(18))).await;
        let provider = signing_provider(&evm).await;

        let outcome = ensure_unlimited_approval(
            &provider,
            evm.vault_address,
            orchestrator,
            evm.wallet_address,
        )
        .await
        .unwrap();

        assert!(matches!(outcome, ApprovalOutcome::Approved { .. }));
        let report = readiness(&evm, orchestrator, evm.wallet_address).await;
        assert!(report.assets[0].is_unlimited());
    }

    #[test]
    fn signing_shapes_target_the_right_contracts_with_decodable_calldata() {
        let orchestrator = Address::repeat_byte(0x11);
        let vault = Address::repeat_byte(0x22);
        let receipt_contract = Address::repeat_byte(0x33);
        let bot = Address::repeat_byte(0x44);

        let shapes =
            build_signing_shapes(orchestrator, vault, receipt_contract, bot);
        let labels: Vec<&str> =
            shapes.iter().map(|shape| shape.label).collect();
        assert_eq!(
            labels,
            [
                "orchestrator.mint",
                "orchestrator.burn",
                "vault.approve",
                "receipt.safeBatchTransferFrom"
            ]
        );

        let mint =
            IST0xOrchestratorV1::mintCall::abi_decode(&shapes[0].calldata)
                .unwrap();
        assert_eq!(shapes[0].to, orchestrator);
        assert_eq!(mint.token, vault);
        assert_eq!(mint.to, bot);
        assert!(
            mint.auth.signature.is_empty(),
            "the placeholder auth must stay opaque and empty"
        );

        let burn =
            IST0xOrchestratorV1::burnCall::abi_decode(&shapes[1].calldata)
                .unwrap();
        assert_eq!(shapes[1].to, orchestrator);
        assert_eq!(burn.token, vault);

        let approve = OffchainAssetReceiptVault::approveCall::abi_decode(
            &shapes[2].calldata,
        )
        .unwrap();
        assert_eq!(shapes[2].to, vault);
        assert_eq!(approve.spender, orchestrator);
        assert_eq!(approve.amount, U256::MAX);

        // The batch selector — the shape the migration tooling actually
        // submits; the singular `safeTransferFrom` is a different selector
        // and therefore a different Turnkey policy grant.
        let transfer =
            Receipt::safeBatchTransferFromCall::abi_decode(&shapes[3].calldata)
                .unwrap();
        assert_eq!(shapes[3].to, receipt_contract);
        assert_eq!(transfer.from, bot);
        assert_eq!(transfer.to, orchestrator);
    }

    #[traced_test]
    #[tokio::test]
    async fn prove_signing_shapes_signs_everything_and_broadcasts_nothing() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let wallet = EthereumWallet::from(signer);
        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let block_before = provider.get_block_number().await.unwrap();
        let nonce_before =
            provider.get_transaction_count(evm.wallet_address).await.unwrap();

        let proofs = prove_signing_shapes(
            &provider,
            &wallet,
            orchestrator,
            evm.vault_address,
            evm.wallet_address,
        )
        .await
        .unwrap();

        assert_eq!(proofs.len(), 4);
        assert!(
            proofs
                .iter()
                .any(|proof| proof.label == "receipt.safeBatchTransferFrom"
                    && proof.to != evm.vault_address
                    && proof.to != orchestrator),
            "the transfer shape must target the resolved receipt contract"
        );

        assert_eq!(
            provider.get_block_number().await.unwrap(),
            block_before,
            "signing must not broadcast anything"
        );
        assert_eq!(
            provider.get_transaction_count(evm.wallet_address).await.unwrap(),
            nonce_before,
            "signing must not consume a nonce on-chain"
        );
        assert!(logs_contain_at!(
            Level::INFO,
            &["Signed every policy-proof shape", "shapes=4"]
        ));
        assert!(logs_contain_at!(
            Level::DEBUG,
            &["Signed policy-proof shape", "orchestrator.mint"]
        ));
    }

    /// A Turnkey-side refusal (mocked as the policy-denial case: the API
    /// rejects the sign request) must surface as `SigningRejected` naming the
    /// first refused shape — attribution is the whole point of the proof.
    #[tokio::test]
    async fn turnkey_refusal_names_the_rejected_shape() {
        let evm = LocalEvm::new().await.unwrap();
        let orchestrator = evm.deploy_orchestrator().await.unwrap();

        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/submit/sign_transaction");
            then.status(403)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "message": "request denied by policy"
                }));
        });
        let wallet = test_wallet_against(
            server.base_url(),
            evm.wallet_address,
            evm.chain_id,
        );
        let provider =
            ProviderBuilder::new().connect(&evm.endpoint).await.unwrap();

        let error = prove_signing_shapes(
            &provider,
            &wallet,
            orchestrator,
            evm.vault_address,
            evm.wallet_address,
        )
        .await
        .unwrap_err();

        assert!(
            matches!(
                error,
                OnboardingError::SigningRejected {
                    label: "orchestrator.mint",
                    ..
                }
            ),
            "the first shape's denial must be attributed to it, got {error}"
        );
    }

    #[test]
    fn display_reports_each_check_and_overall_verdict() {
        let not_ready = OrchestratorReadiness {
            orchestrator: Address::repeat_byte(0x11),
            bot: Address::repeat_byte(0x22),
            mint_role_granted: true,
            burn_role_granted: false,
            vault_logic_expected: true,
            assets: vec![AssetApprovalReadiness {
                underlying: rklb(),
                vault: Address::repeat_byte(0x33),
                allowance: U256::from(7),
                deposit_role_granted: true,
                withdraw_role_granted: false,
            }],
        };

        let rendered = not_ready.to_string();
        assert!(rendered.contains("[PASS] MINT_ROLE granted to bot wallet"));
        assert!(rendered.contains("[FAIL] BURN_ROLE granted to bot wallet"));
        assert!(rendered.contains("[PASS] vaultLogicIsExpected()"));
        assert!(rendered.contains("RKLB"));
        assert!(rendered.contains("allowance 7"));
        assert!(rendered.contains(
            "[PASS] RKLB authorizer: DEPOSIT granted to orchestrator"
        ));
        assert!(rendered.contains(
            "[FAIL] RKLB authorizer: WITHDRAW granted to orchestrator"
        ));
        assert!(rendered.contains("Overall: NOT READY"));

        let ready = OrchestratorReadiness {
            burn_role_granted: true,
            assets: vec![AssetApprovalReadiness {
                allowance: U256::MAX,
                withdraw_role_granted: true,
                ..not_ready.assets[0].clone()
            }],
            ..not_ready
        };

        let rendered = ready.to_string();
        assert!(rendered.contains("allowance unlimited"));
        assert!(rendered.contains("Overall: READY"));
    }
}
