use alloy::consensus::{
    SignableTransaction, Transaction, TxEnvelope, TxLegacy,
};
use alloy::eips::Encodable2718;
use alloy::primitives::{Address, B256, Bytes, Signature, TxKind, U256, b256};
#[cfg(test)]
use alloy::providers::{PendingTransactionError, WatchTxError};
use alloy::rpc::types::TransactionReceipt;
#[cfg(test)]
use alloy::transports::TransportErrorKind;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use tokio::sync::Notify;

use super::{
    BurnTxStatus, BurnVerification, MintResult, MintTxStatus, MultiBurnParams,
    MultiBurnResult, MultiBurnResultEntry, OrchestratorBurnParams,
    OrchestratorBurnReadiness, OrchestratorBurnResult, PreparedMintTx,
    ReceiptInformation, SubmittedTx, VaultError, VaultService,
    WalletNonceGuard,
};
#[cfg(test)]
use super::{OrchestratorRevertReason, VerifiedBurn, VerifiedShareTransfer};
use crate::redemption::BurnExternalTxId;
use crate::vault::{SendableTxWithHash, TxId};

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MintTokensCall {
    pub(crate) vault: Address,
    pub(crate) assets: U256,
    pub(crate) receiver: Address,
    pub(crate) receipt_info: ReceiptInformation,
}

/// Mock behavior for blockchain service.
///
/// This enum is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests)
/// needs it. However, the Failure variant IS behind `#[cfg(test)]` because E2E tests only
/// need the happy path and compile the library without `#[cfg(test)]` enabled.
enum MockBehavior {
    Success,
    #[cfg(test)]
    Failure,
    #[cfg(test)]
    SubmitFailure,
    /// Submit succeeds; confirm fails with a definitive on-chain revert
    /// (`VaultError::Reverted`) — exercises the terminal-failure release path.
    #[cfg(test)]
    ConfirmRevert,
    /// Confirmation timed out without proving whether the submitted
    /// transaction landed on-chain.
    #[cfg(test)]
    ConfirmPending,
    /// Wallet lock acquisition waits for an explicit test release.
    #[cfg(test)]
    WalletLockBlocked {
        attempted: Arc<Notify>,
        release: Arc<Notify>,
    },
    /// Burn confirmation waits for an explicit test release, then reports an
    /// uncertain pending result.
    #[cfg(test)]
    ConfirmPendingBlocked {
        started: Arc<Notify>,
        release: Arc<Notify>,
    },
    /// `submit_burn` fails with a definitive on-chain revert
    /// (`VaultError::Reverted`), as the synchronous local backend does when a
    /// burn mines but reverts — exercises the submit-failure release path.
    #[cfg(test)]
    SubmitRevert,
    /// `prepare_tx` returns an error, testing the crash-safe idempotency path
    /// where signed-tx preparation fails before the tx is stored in the event.
    #[cfg(test)]
    PrepareTxFails,
}

/// Configured outcome for `verify_burn_tx` in tests.
#[cfg(test)]
#[derive(Clone)]
enum MockVerifyBurn {
    /// The tx proves a burn: return this block number and shares burned.
    Verified {
        block_number: u64,
        nonce: u64,
        shares_burned: U256,
        burns: Vec<VerifiedBurn>,
        share_transfers: Vec<VerifiedShareTransfer>,
    },
    /// The tx succeeded but contains no matching burn.
    NotABurn,
    /// The tx reverted on-chain.
    Reverted,
}

#[cfg(test)]
#[derive(Clone, Default)]
enum MockCheckTxOutcome {
    Receipt(Box<TransactionReceipt>),
    /// The prior tx is still pending.
    Pending,
    /// The prior tx lookup failed at the RPC boundary.
    Rpc,
    /// The prior tx cannot be verified from its identifier or receipt.
    #[default]
    InvalidReceipt,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum MockBurnTxClassification {
    Status(BurnTxStatus),
    RpcError,
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum MockMintTxClassification {
    Status(MintTxStatus),
    RpcError,
}

#[cfg(test)]
impl Default for MockVerifyBurn {
    fn default() -> Self {
        Self::Verified {
            block_number: 45_989_009,
            nonce: 0,
            shares_burned: U256::from(1u8),
            burns: vec![],
            share_transfers: vec![],
        }
    }
}

/// Shared mock state for the orchestrator burn methods, grouped so every
/// constructor initializes it with a single field.
#[derive(Default)]
struct OrchestratorMockState {
    /// Cached result from `submit_orchestrator_burn` for retrieval in
    /// `confirm_orchestrator_burn`.
    pending_result: Mutex<Option<OrchestratorBurnResult>>,
    /// Readiness returned by `check_orchestrator_burn_readiness`;
    /// `None` means `Ready`.
    #[cfg(test)]
    readiness: Mutex<Option<OrchestratorBurnReadiness>>,
    /// When set, `confirm_orchestrator_burn` fails with
    /// `VaultError::OrchestratorReverted` carrying this reason.
    #[cfg(test)]
    confirm_revert: Mutex<Option<OrchestratorRevertReason>>,
    #[cfg(test)]
    last_params: Mutex<Option<OrchestratorBurnParams>>,
    #[cfg(test)]
    preparation_call_count: AtomicUsize,
    #[cfg(test)]
    submit_call_count: AtomicUsize,
    #[cfg(test)]
    readiness_call_count: AtomicUsize,
}

/// Mock blockchain service for testing.
///
/// This mock is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests
/// in `tests/`) needs to construct it. However, failure and delay support ARE behind
/// `#[cfg(test)]` because E2E tests only exercise the happy path and compile the library
/// without `#[cfg(test)]` enabled. Unit tests (inside the crate) can access `#[cfg(test)]`
/// code, so they get full mock functionality including failures and timing behavior.
pub(crate) struct MockVaultService {
    orchestrator: Arc<OrchestratorMockState>,
    behavior: MockBehavior,
    mint_delay_ms: u64,
    wallet_nonce_lock: Arc<tokio::sync::Mutex<()>>,
    #[cfg(test)]
    wallet_lock_call_count: Arc<AtomicUsize>,
    call_count: Arc<AtomicUsize>,
    multi_burn_call_count: Arc<AtomicUsize>,
    /// Cached MintResult from submit_mint for retrieval in confirm_mint.
    pending_mint_result: Arc<Mutex<Option<MintResult>>>,
    /// Cached MultiBurnResult from submit_burn for retrieval in confirm_burn.
    pending_burn_result: Arc<Mutex<Option<MultiBurnResult>>>,
    #[cfg(test)]
    last_call: Arc<Mutex<Option<MintTokensCall>>>,
    #[cfg(test)]
    share_balance: Arc<Mutex<U256>>,
    #[cfg(test)]
    last_multi_burn_params: Arc<Mutex<Option<MultiBurnParams>>>,
    /// Outcome returned by `verify_burn_tx`. Defaults to a successful
    /// verification, exercising the force-complete happy path.
    #[cfg(test)]
    verify_burn: Arc<Mutex<MockVerifyBurn>>,
    #[cfg(test)]
    verify_burn_call_count: Arc<AtomicUsize>,
    /// Signed tx returned by `prepare_tx` when local signing is configured.
    #[cfg(test)]
    prepared_tx: Arc<Mutex<Option<SendableTxWithHash>>>,
    #[cfg(test)]
    checked_tx_outcome: Arc<Mutex<MockCheckTxOutcome>>,
    #[cfg(test)]
    burn_tx_status: Arc<Mutex<MockBurnTxClassification>>,
    #[cfg(test)]
    mint_tx_status: Arc<Mutex<MockMintTxClassification>>,
    /// Optional per-call mint classification outcomes (FIFO). When non-empty,
    /// each `classify_mint_tx` pops the next entry; otherwise
    /// [`Self::mint_tx_status`] is used. Enables TOCTOU recheck tests.
    #[cfg(test)]
    mint_tx_status_sequence: Arc<Mutex<Vec<MockMintTxClassification>>>,
    #[cfg(test)]
    submitted_burn_txs: Arc<Mutex<Vec<SendableTxWithHash>>>,
    #[cfg(test)]
    burn_classification_call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    mint_classification_call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    burn_preparation_call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    replacement_preparation_call_count: Arc<AtomicUsize>,
    /// Optional sequence of confirm_mint outcomes; when non-empty, each call
    /// pops the next entry instead of using [`MockBehavior`].
    #[cfg(test)]
    confirm_mint_outcomes: Arc<Mutex<Vec<Result<MintResult, VaultError>>>>,
    /// Optional forced `submit_mint` error (e.g. uncertain broadcast).
    #[cfg(test)]
    submit_mint_error: Arc<Mutex<Option<VaultError>>>,
}

impl MockVaultService {
    #[must_use]
    pub(crate) fn new_success() -> Self {
        Self {
            behavior: MockBehavior::Success,
            mint_delay_ms: 0,
            orchestrator: Arc::new(OrchestratorMockState::default()),
            wallet_nonce_lock: Arc::new(tokio::sync::Mutex::new(())),
            #[cfg(test)]
            wallet_lock_call_count: Arc::new(AtomicUsize::new(0)),
            call_count: Arc::new(AtomicUsize::new(0)),
            multi_burn_call_count: Arc::new(AtomicUsize::new(0)),
            pending_mint_result: Arc::new(Mutex::new(None)),
            pending_burn_result: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            last_call: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            share_balance: Arc::new(Mutex::new(U256::MAX)),
            #[cfg(test)]
            last_multi_burn_params: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            verify_burn: Arc::new(Mutex::new(MockVerifyBurn::default())),
            #[cfg(test)]
            verify_burn_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            prepared_tx: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            checked_tx_outcome: Arc::new(Mutex::new(
                MockCheckTxOutcome::default(),
            )),
            #[cfg(test)]
            burn_tx_status: Arc::new(Mutex::new(
                MockBurnTxClassification::Status(BurnTxStatus::StillMineable),
            )),
            #[cfg(test)]
            mint_tx_status: Arc::new(Mutex::new(
                MockMintTxClassification::Status(MintTxStatus::StillMineable),
            )),
            #[cfg(test)]
            mint_tx_status_sequence: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            submitted_burn_txs: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            burn_classification_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            mint_classification_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            burn_preparation_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            replacement_preparation_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            confirm_mint_outcomes: Arc::new(Mutex::new(Vec::new())),
            #[cfg(test)]
            submit_mint_error: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failure() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::Failure;
        service
    }

    #[cfg(test)]
    pub(crate) fn new_submit_failure() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::SubmitFailure;
        service
    }

    #[cfg(test)]
    pub(crate) fn new_confirm_revert() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::ConfirmRevert;
        service
    }

    #[cfg(test)]
    pub(crate) fn new_confirm_pending() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::ConfirmPending;
        service
    }

    #[cfg(test)]
    pub(crate) fn new_wallet_lock_blocked() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::WalletLockBlocked {
            attempted: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        service
    }

    #[cfg(test)]
    pub(crate) fn new_confirm_pending_blocked() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::ConfirmPendingBlocked {
            started: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        };
        service
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_wallet_lock_attempt(&self) {
        let MockBehavior::WalletLockBlocked { attempted, .. } = &self.behavior
        else {
            panic!("mock does not block wallet lock acquisition");
        };
        attempted.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn release_wallet_lock(&self) {
        let MockBehavior::WalletLockBlocked { release, .. } = &self.behavior
        else {
            panic!("mock does not block wallet lock acquisition");
        };
        release.notify_one();
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_burn_confirmation(&self) {
        let MockBehavior::ConfirmPendingBlocked { started, .. } =
            &self.behavior
        else {
            panic!("mock does not block burn confirmation");
        };
        started.notified().await;
    }

    #[cfg(test)]
    pub(crate) fn release_burn_confirmation(&self) {
        let MockBehavior::ConfirmPendingBlocked { release, .. } =
            &self.behavior
        else {
            panic!("mock does not block burn confirmation");
        };
        release.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn new_submit_revert() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::SubmitRevert;
        service
    }

    #[cfg(test)]
    pub(crate) fn new_prepare_tx_failure() -> Self {
        let mut service = Self::new_success();
        service.behavior = MockBehavior::PrepareTxFails;
        service
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) const fn with_delay(mut self, delay_ms: u64) -> Self {
        self.mint_delay_ms = delay_ms;
        self
    }

    #[cfg(test)]
    pub(crate) fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_wallet_lock_call_count(&self) -> usize {
        self.wallet_lock_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_last_call(&self) -> Option<MintTokensCall> {
        self.last_call.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn get_multi_burn_call_count(&self) -> usize {
        self.multi_burn_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_last_multi_burn_params(&self) -> Option<MultiBurnParams> {
        self.last_multi_burn_params.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn submitted_burn_txs(&self) -> Vec<SendableTxWithHash> {
        self.submitted_burn_txs.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn burn_classification_call_count(&self) -> usize {
        self.burn_classification_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn burn_preparation_call_count(&self) -> usize {
        self.burn_preparation_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn replacement_preparation_call_count(&self) -> usize {
        self.replacement_preparation_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn verify_burn_call_count(&self) -> usize {
        self.verify_burn_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn reset(&self) {
        self.wallet_lock_call_count.store(0, Ordering::Relaxed);
        self.call_count.store(0, Ordering::Relaxed);
        self.multi_burn_call_count.store(0, Ordering::Relaxed);
        *self.last_call.lock().unwrap() = None;
        *self.last_multi_burn_params.lock().unwrap() = None;
        *self.pending_mint_result.lock().unwrap() = None;
        *self.pending_burn_result.lock().unwrap() = None;
        *self.prepared_tx.lock().unwrap() = None;
        self.submitted_burn_txs.lock().unwrap().clear();
        self.verify_burn_call_count.store(0, Ordering::Relaxed);
        self.burn_classification_call_count.store(0, Ordering::Relaxed);
        self.mint_classification_call_count.store(0, Ordering::Relaxed);
        self.burn_preparation_call_count.store(0, Ordering::Relaxed);
        self.replacement_preparation_call_count.store(0, Ordering::Relaxed);
        *self.mint_tx_status.lock().unwrap() =
            MockMintTxClassification::Status(MintTxStatus::StillMineable);
        self.mint_tx_status_sequence.lock().unwrap().clear();
        self.confirm_mint_outcomes.lock().unwrap().clear();
        *self.submit_mint_error.lock().unwrap() = None;
        *self.orchestrator.readiness.lock().unwrap() = None;
        *self.orchestrator.confirm_revert.lock().unwrap() = None;
        *self.orchestrator.last_params.lock().unwrap() = None;
        self.orchestrator.preparation_call_count.store(0, Ordering::Relaxed);
        self.orchestrator.submit_call_count.store(0, Ordering::Relaxed);
        self.orchestrator.readiness_call_count.store(0, Ordering::Relaxed);
        *self.orchestrator.pending_result.lock().unwrap() = None;
    }

    #[cfg(test)]
    pub(crate) fn with_share_balance(self, balance: U256) -> Self {
        self.set_share_balance(balance);
        self
    }

    /// Moves the balance on an already-shared mock, so a test can model the
    /// issuer's balance changing part-way through a run.
    #[cfg(test)]
    pub(crate) fn set_share_balance(&self, balance: U256) {
        *self.share_balance.lock().unwrap() = balance;
    }

    /// Configures `verify_burn_tx` to report the operator-supplied tx as a
    /// burn of `shares_burned` at `block_number`.
    #[cfg(test)]
    pub(crate) fn with_verified_burn(
        self,
        block_number: u64,
        shares_burned: U256,
    ) -> Self {
        *self.verify_burn.lock().unwrap() = MockVerifyBurn::Verified {
            block_number,
            nonce: 0,
            shares_burned,
            burns: vec![],
            share_transfers: vec![],
        };
        self
    }

    #[cfg(test)]
    pub(crate) fn with_verified_burns(
        self,
        block_number: u64,
        nonce: u64,
        burns: Vec<VerifiedBurn>,
        share_transfers: Vec<VerifiedShareTransfer>,
    ) -> Self {
        let shares_burned = burns
            .iter()
            .fold(U256::ZERO, |total, burn| total + burn.shares_burned);
        *self.verify_burn.lock().unwrap() = MockVerifyBurn::Verified {
            block_number,
            nonce,
            shares_burned,
            burns,
            share_transfers,
        };
        self
    }

    #[cfg(test)]
    pub(crate) fn with_verified_burns_and_total(
        self,
        block_number: u64,
        nonce: u64,
        shares_burned: U256,
        burns: Vec<VerifiedBurn>,
        share_transfers: Vec<VerifiedShareTransfer>,
    ) -> Self {
        *self.verify_burn.lock().unwrap() = MockVerifyBurn::Verified {
            block_number,
            nonce,
            shares_burned,
            burns,
            share_transfers,
        };
        self
    }

    /// Configures `verify_burn_tx` to report the operator-supplied tx as not a
    /// burn (succeeded on-chain but no matching `Transfer(owner -> 0x0)`).
    #[cfg(test)]
    pub(crate) fn with_unverifiable_burn(self) -> Self {
        *self.verify_burn.lock().unwrap() = MockVerifyBurn::NotABurn;
        self
    }

    /// Configures `verify_burn_tx` to report the operator-supplied tx as
    /// reverted on-chain.
    #[cfg(test)]
    pub(crate) fn with_reverted_burn(self) -> Self {
        *self.verify_burn.lock().unwrap() = MockVerifyBurn::Reverted;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_pending_checked_tx(self) -> Self {
        *self.checked_tx_outcome.lock().unwrap() = MockCheckTxOutcome::Pending;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_rpc_checked_tx_error(self) -> Self {
        *self.checked_tx_outcome.lock().unwrap() = MockCheckTxOutcome::Rpc;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_invalid_checked_tx(self) -> Self {
        *self.checked_tx_outcome.lock().unwrap() =
            MockCheckTxOutcome::InvalidReceipt;
        self
    }

    /// Configures `find_existing_burn` to return the given [`SubmittedTx`],
    /// simulating a burn that was already submitted on-chain before the current
    /// process started (crash-safe idempotency recovery path).
    #[cfg(test)]
    pub(crate) fn with_prepared_tx(
        self,
        sendable_tx: SendableTxWithHash,
    ) -> Self {
        *self.prepared_tx.lock().unwrap() = Some(sendable_tx);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_checked_tx_receipt(
        self,
        receipt: TransactionReceipt,
    ) -> Self {
        *self.checked_tx_outcome.lock().unwrap() =
            MockCheckTxOutcome::Receipt(Box::new(receipt));
        self
    }

    #[cfg(test)]
    pub(crate) fn with_burn_tx_status(self, status: BurnTxStatus) -> Self {
        *self.burn_tx_status.lock().unwrap() =
            MockBurnTxClassification::Status(status);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_burn_tx_classification_failure(self) -> Self {
        *self.burn_tx_status.lock().unwrap() =
            MockBurnTxClassification::RpcError;
        self
    }

    #[cfg(test)]
    pub(crate) fn set_burn_tx_status(&self, status: BurnTxStatus) {
        *self.burn_tx_status.lock().unwrap() =
            MockBurnTxClassification::Status(status);
    }

    #[cfg(test)]
    pub(crate) fn with_mint_tx_status(self, status: MintTxStatus) -> Self {
        *self.mint_tx_status.lock().unwrap() =
            MockMintTxClassification::Status(status);
        self
    }

    /// FIFO classification outcomes for successive `classify_mint_tx` calls.
    #[cfg(test)]
    pub(crate) fn with_mint_tx_status_sequence(
        self,
        statuses: Vec<MintTxStatus>,
    ) -> Self {
        *self.mint_tx_status_sequence.lock().unwrap() = statuses
            .into_iter()
            .map(MockMintTxClassification::Status)
            .collect();
        self
    }

    #[cfg(test)]
    pub(crate) fn with_mint_tx_classification_failure(self) -> Self {
        *self.mint_tx_status.lock().unwrap() =
            MockMintTxClassification::RpcError;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_confirm_mint_outcomes(
        self,
        outcomes: Vec<Result<MintResult, VaultError>>,
    ) -> Self {
        *self.confirm_mint_outcomes.lock().unwrap() = outcomes;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_submit_mint_error(self, error: VaultError) -> Self {
        *self.submit_mint_error.lock().unwrap() = Some(error);
        self
    }

    #[cfg(test)]
    pub(crate) fn mint_classification_call_count(&self) -> usize {
        self.mint_classification_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn set_prepared_tx(&self, sendable_tx: SendableTxWithHash) {
        *self.prepared_tx.lock().unwrap() = Some(sendable_tx);
    }

    /// Seeds the result returned by the next `confirm_burn` (resume paths that
    /// skip `submit_burn` otherwise fall back to default mock burn deltas).
    #[cfg(test)]
    pub(crate) fn with_pending_burn_result(
        self,
        result: MultiBurnResult,
    ) -> Self {
        *self.pending_burn_result.lock().unwrap() = Some(result);
        self
    }

    /// Configures `confirm_orchestrator_burn` to fail with
    /// `VaultError::OrchestratorReverted` carrying the given typed reason.
    #[cfg(test)]
    pub(crate) fn with_orchestrator_confirm_revert(
        self,
        reason: OrchestratorRevertReason,
    ) -> Self {
        *self.orchestrator.confirm_revert.lock().unwrap() = Some(reason);
        self
    }

    #[cfg(test)]
    pub(crate) fn with_orchestrator_readiness(
        self,
        readiness: OrchestratorBurnReadiness,
    ) -> Self {
        *self.orchestrator.readiness.lock().unwrap() = Some(readiness);
        self
    }

    #[cfg(test)]
    pub(crate) fn last_orchestrator_burn_params(
        &self,
    ) -> Option<OrchestratorBurnParams> {
        self.orchestrator.last_params.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn orchestrator_submit_call_count(&self) -> usize {
        self.orchestrator.submit_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn orchestrator_readiness_call_count(&self) -> usize {
        self.orchestrator.readiness_call_count.load(Ordering::Relaxed)
    }
}

const MOCK_MINT_TX_HASH: alloy::primitives::B256 =
    b256!("0x4242424242424242424242424242424242424242424242424242424242424242");

const MOCK_BURN_TX_HASH: alloy::primitives::B256 =
    b256!("0x4545454545454545454545454545454545454545454545454545454545454545");

fn default_orchestrator_burn_result() -> OrchestratorBurnResult {
    OrchestratorBurnResult {
        tx_hash: MOCK_BURN_TX_HASH,
        shares_burned: U256::from(100_000_000_000_000_000_000u128),
        burn_range: (U256::from(1u8), U256::from(2u8)),
        gas_used: 50000,
        block_number: 5000,
    }
}

fn default_multi_burn_result(dust_shares: U256) -> MultiBurnResult {
    MultiBurnResult {
        tx_hash: MOCK_BURN_TX_HASH,
        burns: vec![MultiBurnResultEntry {
            receipt_id: U256::from(99u64),
            shares_burned: U256::from(100_000_000_000_000_000_000u128),
        }],
        dust_returned: dust_shares,
        gas_used: 50000,
        block_number: 5000,
    }
}

#[async_trait]
impl VaultService for MockVaultService {
    #[cfg_attr(not(test), allow(unused_variables))]
    async fn prepare_mint_tx(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        _user: Address,
        receipt_info: ReceiptInformation,
        external_tx_id: Option<String>,
    ) -> Result<PreparedMintTx, VaultError> {
        if self.mint_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.mint_delay_ms,
            ))
            .await;
        }

        self.call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::PrepareTxFails) {
            return Err(VaultError::InvalidReceipt);
        }

        #[cfg(test)]
        {
            *self.last_call.lock().unwrap() = Some(MintTokensCall {
                vault,
                assets,
                receiver: bot,
                receipt_info: receipt_info.clone(),
            });
        }

        // Pre-compute the MintResult for confirm_mint to return.
        let receipt_info_bytes = match receipt_info.encode(Some(
            "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm",
        )) {
            Ok(bytes) => bytes,
            Err(err) => {
                return Err(VaultError::ReceiptEncode(err));
            }
        };

        let transaction = TxLegacy {
            chain_id: Some(1),
            nonce: 1,
            gas_price: 1,
            gas_limit: 21_000,
            to: TxKind::Call(vault),
            value: U256::ZERO,
            input: Bytes::new(),
        };
        let signature = Signature::new(U256::from(1), U256::from(1), false);
        let envelope = TxEnvelope::from(transaction.into_signed(signature));
        let tx_hash = *envelope.tx_hash();

        *self
            .pending_mint_result
            .lock()
            .expect("pending_mint_result mutex poisoned") = Some(MintResult {
            tx_hash,
            receipt_id: U256::from(1),
            shares_minted: assets,
            gas_used: 21000,
            block_number: 1000,
            receipt_info_bytes,
        });

        Ok(PreparedMintTx {
            tx: envelope.encoded_2718(),
            hash: tx_hash,
            nonce: envelope.nonce(),
            signed_at: chrono::Utc::now(),
            external_tx_id: external_tx_id
                .unwrap_or_else(|| "mock-mint".to_string()),
        })
    }

    async fn submit_mint(
        &self,
        prepared_tx: &PreparedMintTx,
    ) -> Result<SubmittedTx, VaultError> {
        #[cfg(test)]
        {
            let forced_error = self.submit_mint_error.lock().unwrap().take();
            if let Some(error) = forced_error {
                return Err(error);
            }
        }

        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitFailure) {
            return Err(VaultError::InvalidReceipt);
        }

        Ok(SubmittedTx {
            external_tx_id: prepared_tx.external_tx_id.clone(),
            tx_id: prepared_tx.hash.into(),
        })
    }

    async fn confirm_mint(
        &self,
        tx_id: &TxId,
    ) -> Result<MintResult, VaultError> {
        #[cfg(test)]
        {
            let mut outcomes = self
                .confirm_mint_outcomes
                .lock()
                .expect("confirm_mint_outcomes mutex poisoned");
            if !outcomes.is_empty() {
                return outcomes.remove(0);
            }
        }

        match &self.behavior {
            MockBehavior::Success => {
                let _ = tx_id;
                let result = self
                    .pending_mint_result
                    .lock()
                    .expect("pending_mint_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| MintResult {
                        tx_hash: MOCK_MINT_TX_HASH,
                        receipt_id: U256::from(1),
                        shares_minted: U256::ZERO,
                        gas_used: 21000,
                        block_number: 1000,
                        receipt_info_bytes: Bytes::new(),
                    });

                Ok(result)
            }
            #[cfg(test)]
            MockBehavior::Failure => Err(VaultError::InvalidReceipt),
            #[cfg(test)]
            MockBehavior::SubmitFailure => {
                // SubmitFailure only affects submit_*, not confirm_*.
                // If confirm is somehow called, return the cached result.
                let result = self
                    .pending_mint_result
                    .lock()
                    .expect("pending_mint_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| MintResult {
                        tx_hash: MOCK_MINT_TX_HASH,
                        receipt_id: U256::from(1),
                        shares_minted: U256::ZERO,
                        gas_used: 21000,
                        block_number: 1000,
                        receipt_info_bytes: Bytes::new(),
                    });
                Ok(result)
            }
            #[cfg(test)]
            MockBehavior::ConfirmRevert => {
                Err(VaultError::Reverted { tx_hash: MOCK_MINT_TX_HASH })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPending => {
                Err(VaultError::ConfirmationPending {
                    tx_id: tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPendingBlocked { started, release } => {
                started.notify_one();
                release.notified().await;
                Err(VaultError::ConfirmationPending {
                    tx_id: tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            #[cfg(test)]
            MockBehavior::WalletLockBlocked { .. }
            | MockBehavior::SubmitRevert
            | MockBehavior::PrepareTxFails => Err(VaultError::InvalidReceipt),
        }
    }

    async fn classify_mint_tx(
        &self,
        _owner: Address,
        _prepared_tx: &PreparedMintTx,
    ) -> Result<MintTxStatus, VaultError> {
        #[cfg(test)]
        {
            self.mint_classification_call_count.fetch_add(1, Ordering::Relaxed);
            // One lock at a time: holding the sequence guard while taking the
            // fallback guard is a nesting a future helper could invert.
            let queued = {
                let mut sequence = self.mint_tx_status_sequence.lock().unwrap();
                (!sequence.is_empty()).then(|| sequence.remove(0))
            };
            let classification =
                queued.unwrap_or_else(|| *self.mint_tx_status.lock().unwrap());
            return match classification {
                MockMintTxClassification::Status(status) => Ok(status),
                MockMintTxClassification::RpcError => {
                    Err(VaultError::InvalidReceipt)
                }
            };
        }
        #[cfg(not(test))]
        Ok(MintTxStatus::StillMineable)
    }

    async fn get_share_balance(
        &self,
        _vault: Address,
        _owner: Address,
    ) -> Result<U256, VaultError> {
        #[cfg(test)]
        {
            Ok(*self.share_balance.lock().unwrap())
        }
        #[cfg(not(test))]
        {
            Ok(U256::MAX)
        }
    }

    async fn submit_burn(
        &self,
        params: MultiBurnParams,
        prepared_tx: SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError> {
        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitFailure) {
            return Err(VaultError::InvalidReceipt);
        }

        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitRevert) {
            return Err(VaultError::Reverted { tx_hash: MOCK_BURN_TX_HASH });
        }

        self.multi_burn_call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_multi_burn_params.lock().unwrap() = Some(params.clone());
            self.submitted_burn_txs.lock().unwrap().push(prepared_tx.clone());
        }

        // Pre-compute the MultiBurnResult for confirm_burn to return.
        let burns = params
            .burns
            .into_iter()
            .map(|entry| MultiBurnResultEntry {
                receipt_id: entry.receipt_id,
                shares_burned: entry.burn_shares,
            })
            .collect();

        *self
            .pending_burn_result
            .lock()
            .expect("pending_burn_result mutex poisoned") =
            Some(MultiBurnResult {
                tx_hash: MOCK_BURN_TX_HASH,
                burns,
                dust_returned: params.dust_shares,
                gas_used: 50000,
                block_number: 5000,
            });

        Ok(SubmittedTx {
            external_tx_id: params.external_tx_id.map_or_else(
                || "mock-burn".to_string(),
                BurnExternalTxId::into_string,
            ),
            tx_id: prepared_tx.hash.into(),
        })
    }

    async fn confirm_burn(
        &self,
        _tx_id: &TxId,
        dust_shares: U256,
    ) -> Result<MultiBurnResult, VaultError> {
        match &self.behavior {
            MockBehavior::Success => {
                let result = self
                    .pending_burn_result
                    .lock()
                    .expect("pending_burn_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| default_multi_burn_result(dust_shares));

                Ok(result)
            }
            #[cfg(test)]
            MockBehavior::Failure => Err(VaultError::InvalidReceipt),
            #[cfg(test)]
            MockBehavior::SubmitFailure => {
                let result = self
                    .pending_burn_result
                    .lock()
                    .expect("pending_burn_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| default_multi_burn_result(dust_shares));
                Ok(result)
            }
            #[cfg(test)]
            MockBehavior::ConfirmRevert => {
                Err(VaultError::Reverted { tx_hash: MOCK_BURN_TX_HASH })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPending => {
                Err(VaultError::ConfirmationPending {
                    tx_id: _tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPendingBlocked { started, release } => {
                started.notify_one();
                release.notified().await;
                Err(VaultError::ConfirmationPending {
                    tx_id: _tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            // SubmitRevert fails at submit; if confirm is somehow reached,
            // return the cached result like the other submit-* behaviors.
            // PrepareTxFails never reaches confirm (fails before submit);
            // if somehow called, return the cached result.
            #[cfg(test)]
            MockBehavior::WalletLockBlocked { .. }
            | MockBehavior::SubmitRevert
            | MockBehavior::PrepareTxFails => {
                let result = self
                    .pending_burn_result
                    .lock()
                    .expect("pending_burn_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| default_multi_burn_result(dust_shares));
                Ok(result)
            }
        }
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    async fn verify_burn_tx(
        &self,
        _vault: Address,
        _owner: Address,
        tx_hash: B256,
    ) -> Result<BurnVerification, VaultError> {
        #[cfg(test)]
        {
            use MockVerifyBurn::{NotABurn, Reverted, Verified};

            self.verify_burn_call_count.fetch_add(1, Ordering::Relaxed);
            let outcome = self.verify_burn.lock().unwrap().clone();
            match outcome {
                Verified {
                    block_number,
                    nonce,
                    shares_burned,
                    burns,
                    share_transfers,
                } => Ok(BurnVerification {
                    block_number,
                    nonce,
                    shares_burned,
                    burns,
                    share_transfers,
                }),
                NotABurn => Err(VaultError::NotABurn { tx_hash }),
                Reverted => Err(VaultError::Reverted { tx_hash }),
            }
        }
        #[cfg(not(test))]
        {
            Ok(BurnVerification {
                block_number: 0,
                nonce: 0,
                shares_burned: U256::from(1u8),
                burns: vec![],
                share_transfers: vec![],
            })
        }
    }

    async fn prepare_burn_tx(
        &self,
        _params: &MultiBurnParams,
    ) -> Result<SendableTxWithHash, VaultError> {
        #[cfg(test)]
        {
            self.burn_preparation_call_count.fetch_add(1, Ordering::Relaxed);
            if matches!(self.behavior, MockBehavior::PrepareTxFails) {
                return Err(VaultError::InvalidReceipt);
            }
            // Use configured tx if present, otherwise fall back to default.
            // Cloned (not taken) so retries can re-use the same configured tx.
            let prepared = self.prepared_tx.lock().unwrap().clone();
            return Ok(prepared.unwrap_or_default());
        }
        #[cfg(not(test))]
        Ok(SendableTxWithHash::default())
    }

    async fn classify_burn_tx(
        &self,
        _owner: Address,
        _sendable_tx: &SendableTxWithHash,
    ) -> Result<BurnTxStatus, VaultError> {
        #[cfg(test)]
        {
            self.burn_classification_call_count.fetch_add(1, Ordering::Relaxed);
            let classification = *self.burn_tx_status.lock().unwrap();
            return match classification {
                MockBurnTxClassification::Status(status) => Ok(status),
                MockBurnTxClassification::RpcError => {
                    Err(VaultError::InvalidReceipt)
                }
            };
        }
        #[cfg(not(test))]
        Ok(BurnTxStatus::StillMineable)
    }

    async fn prepare_replacement_burn_tx(
        &self,
        _owner: Address,
        _sendable_tx: &SendableTxWithHash,
    ) -> Result<SendableTxWithHash, VaultError> {
        #[cfg(test)]
        {
            self.replacement_preparation_call_count
                .fetch_add(1, Ordering::Relaxed);
            return self
                .prepared_tx
                .lock()
                .unwrap()
                .clone()
                .ok_or(VaultError::InvalidReceipt);
        }
        #[cfg(not(test))]
        Ok(SendableTxWithHash::default())
    }

    async fn check_tx(
        &self,
        _tx_id: &TxId,
    ) -> Result<TransactionReceipt, VaultError> {
        #[cfg(test)]
        let checked_tx_outcome =
            self.checked_tx_outcome.lock().unwrap().clone();
        #[cfg(test)]
        {
            use super::classify_checked_receipt;

            match checked_tx_outcome {
                MockCheckTxOutcome::Receipt(receipt) => {
                    let tx_hash =
                        _tx_id.to_hash().ok_or(VaultError::InvalidReceipt)?;
                    return classify_checked_receipt(tx_hash, *receipt);
                }
                MockCheckTxOutcome::Pending => {
                    return Err(PendingTransactionError::TxWatcher(
                        WatchTxError::Timeout,
                    )
                    .into());
                }
                MockCheckTxOutcome::Rpc => {
                    return Err(VaultError::Rpc(
                        TransportErrorKind::custom_str("mock RPC failure"),
                    ));
                }
                MockCheckTxOutcome::InvalidReceipt => {
                    return Err(VaultError::InvalidReceipt);
                }
            }
        }
        #[cfg(not(test))]
        Err(VaultError::InvalidReceipt)
    }

    async fn lock_wallet(&self) -> WalletNonceGuard {
        #[cfg(test)]
        {
            self.wallet_lock_call_count.fetch_add(1, Ordering::Relaxed);
            if let MockBehavior::WalletLockBlocked { attempted, release } =
                &self.behavior
            {
                attempted.notify_one();
                release.notified().await;
            }
        }

        Some(self.wallet_nonce_lock.clone().lock_owned().await)
    }

    async fn check_orchestrator_burn_readiness(
        &self,
        _orchestrator: Address,
        _token: Address,
        _owner: Address,
        _amount: U256,
    ) -> Result<OrchestratorBurnReadiness, VaultError> {
        #[cfg(test)]
        {
            self.orchestrator
                .readiness_call_count
                .fetch_add(1, Ordering::Relaxed);
            let readiness_opt = *self.orchestrator.readiness.lock().unwrap();
            if let Some(readiness) = readiness_opt {
                return Ok(readiness);
            }
        }

        Ok(OrchestratorBurnReadiness::Ready)
    }

    async fn prepare_orchestrator_burn_tx(
        &self,
        _params: &OrchestratorBurnParams,
    ) -> Result<SendableTxWithHash, VaultError> {
        #[cfg(test)]
        {
            self.orchestrator
                .preparation_call_count
                .fetch_add(1, Ordering::Relaxed);
            if matches!(self.behavior, MockBehavior::PrepareTxFails) {
                return Err(VaultError::InvalidReceipt);
            }
            // Use configured tx if present, otherwise fall back to default.
            // Cloned (not taken) so retries can re-use the same configured tx.
            let prepared = self.prepared_tx.lock().unwrap().clone();
            return Ok(prepared.unwrap_or_default());
        }
        #[cfg(not(test))]
        Ok(SendableTxWithHash::default())
    }

    async fn submit_orchestrator_burn(
        &self,
        params: &OrchestratorBurnParams,
        sendable_tx: &SendableTxWithHash,
    ) -> Result<SubmittedTx, VaultError> {
        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitFailure) {
            return Err(VaultError::InvalidReceipt);
        }

        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitRevert) {
            return Err(VaultError::Reverted { tx_hash: MOCK_BURN_TX_HASH });
        }

        #[cfg(test)]
        {
            self.orchestrator.submit_call_count.fetch_add(1, Ordering::Relaxed);
            *self.orchestrator.last_params.lock().unwrap() =
                Some(params.clone());
        }

        // Pre-compute the OrchestratorBurnResult for confirm to return: the
        // orchestrator burns the full amount from a single-receipt walk.
        {
            let mut pending = self
                .orchestrator
                .pending_result
                .lock()
                .expect("orchestrator pending_result mutex poisoned");
            // Preserve a pre-seeded pending result configured by a test;
            // only fill the cache when empty.
            if pending.is_none() {
                *pending = Some(OrchestratorBurnResult {
                    tx_hash: MOCK_BURN_TX_HASH,
                    shares_burned: params.amount,
                    burn_range: (U256::from(1u8), U256::from(2u8)),
                    gas_used: 50000,
                    block_number: 5000,
                });
            }
        }

        Ok(SubmittedTx {
            external_tx_id: params
                .external_tx_id
                .clone()
                .unwrap_or_else(|| {
                    BurnExternalTxId::base(&params.detected_tx_hash)
                })
                .into_string(),
            tx_id: sendable_tx.hash.into(),
        })
    }

    async fn confirm_orchestrator_burn(
        &self,
        _tx_id: &TxId,
    ) -> Result<OrchestratorBurnResult, VaultError> {
        #[cfg(test)]
        let reason_opt = *self.orchestrator.confirm_revert.lock().unwrap();
        #[cfg(test)]
        if let Some(reason) = reason_opt {
            return Err(VaultError::OrchestratorReverted {
                tx_hash: MOCK_BURN_TX_HASH,
                reason,
            });
        }

        match &self.behavior {
            MockBehavior::Success => {
                let result = self
                    .orchestrator
                    .pending_result
                    .lock()
                    .expect("orchestrator pending_result mutex poisoned")
                    .take()
                    .unwrap_or_else(default_orchestrator_burn_result);

                Ok(result)
            }
            #[cfg(test)]
            MockBehavior::Failure => Err(VaultError::InvalidReceipt),
            #[cfg(test)]
            MockBehavior::ConfirmRevert => {
                Err(VaultError::Reverted { tx_hash: MOCK_BURN_TX_HASH })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPending => {
                Err(VaultError::ConfirmationPending {
                    tx_id: _tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            #[cfg(test)]
            MockBehavior::ConfirmPendingBlocked { started, release } => {
                started.notify_one();
                release.notified().await;
                Err(VaultError::ConfirmationPending {
                    tx_id: _tx_id.clone(),
                    message: "receipt polling timed out".to_string(),
                })
            }
            #[cfg(test)]
            MockBehavior::SubmitFailure
            | MockBehavior::WalletLockBlocked { .. }
            | MockBehavior::SubmitRevert
            | MockBehavior::PrepareTxFails => {
                let result = self
                    .orchestrator
                    .pending_result
                    .lock()
                    .expect("orchestrator pending_result mutex poisoned")
                    .take()
                    .unwrap_or_else(default_orchestrator_burn_result);
                Ok(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, B256, U256, address, b256};
    use chrono::Utc;
    use rust_decimal::Decimal;

    use super::MockVaultService;
    use crate::mint::{
        IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::vault::{
        BurnRequestOrigin, MultiBurnEntry, MultiBurnParams, ReceiptInformation,
        SendableTxWithHash, VaultError, VaultService,
    };

    fn test_sendable_tx() -> SendableTxWithHash {
        SendableTxWithHash::default()
    }

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL").unwrap(),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        )
    }

    fn test_receiver() -> Address {
        address!("0000000000000000000000000000000000000001")
    }

    fn test_vault() -> Address {
        address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    }

    #[tokio::test]
    async fn test_submit_and_confirm_mint_success() {
        let mock = MockVaultService::new_success();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        let prepared = mock
            .prepare_mint_tx(
                vault,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await
            .unwrap();
        let submitted = mock.submit_mint(&prepared).await.unwrap();

        assert_eq!(submitted.external_tx_id, "mock-mint");

        let result = mock.confirm_mint(&submitted.tx_id).await.unwrap();

        assert_eq!(result.receipt_id, U256::from(1));
        assert_eq!(result.shares_minted, assets);
        assert_eq!(result.gas_used, 21000);
        assert_eq!(result.block_number, 1000);
    }

    #[tokio::test]
    async fn test_confirm_mint_failure() {
        let mock = MockVaultService::new_failure();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        // Submit always succeeds
        let prepared = mock
            .prepare_mint_tx(
                vault,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
                None,
            )
            .await
            .unwrap();
        let submitted = mock.submit_mint(&prepared).await.unwrap();

        // Confirm returns the failure
        let result = mock.confirm_mint(&submitted.tx_id).await;
        assert!(matches!(result, Err(VaultError::InvalidReceipt)));
    }

    #[tokio::test]
    async fn test_get_call_count_increments() {
        let mock = MockVaultService::new_success();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");

        assert_eq!(mock.get_call_count(), 0);

        mock.prepare_mint_tx(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            test_receipt_info(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(mock.get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_get_last_call_captures_arguments() {
        let mock = MockVaultService::new_success();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        assert!(mock.get_last_call().is_none());

        mock.prepare_mint_tx(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info.clone(),
            None,
        )
        .await
        .unwrap();

        let last_call = mock.get_last_call();
        assert!(last_call.is_some());

        let call = last_call.unwrap();
        assert_eq!(call.vault, vault);
        assert_eq!(call.assets, assets);
        assert_eq!(call.receiver, bot_wallet);
        assert_eq!(
            call.receipt_info.issuer_request_id,
            receipt_info.issuer_request_id
        );
    }

    #[tokio::test]
    async fn test_with_delay_causes_delay() {
        let delay_ms = 50;
        let mock = MockVaultService::new_success().with_delay(delay_ms);
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        let start = tokio::time::Instant::now();
        mock.prepare_mint_tx(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info,
            None,
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() >= u128::from(delay_ms));
    }

    #[tokio::test]
    async fn test_reset_clears_state() {
        let mock = MockVaultService::new_success();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        mock.prepare_mint_tx(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info.clone(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(mock.get_call_count(), 1);
        assert!(mock.get_last_call().is_some());

        mock.reset();

        assert_eq!(mock.get_call_count(), 0);
        assert!(mock.get_last_call().is_none());
    }

    fn test_multi_burn_params() -> MultiBurnParams {
        let detected_tx_hash = b256!(
            "0xabababababababababababababababababababababababababababababababab"
        );
        MultiBurnParams {
            vault: test_vault(),
            burns: vec![MultiBurnEntry {
                receipt_id: U256::from(42),
                burn_shares: U256::from(500),
                receipt_info: Some(test_receipt_info()),
                receipt_info_bytes: None,
            }],
            dust_shares: U256::from(10),
            owner: test_receiver(),
            user: address!("0x2222222222222222222222222222222222222222"),
            origin: BurnRequestOrigin::Redemption(
                IssuerRedemptionRequestId::new(detected_tx_hash),
            ),
            detected_tx_hash,
            external_tx_id: None,
        }
    }

    #[tokio::test]
    async fn test_submit_and_confirm_burn_success() {
        let mock = MockVaultService::new_success();
        let params = test_multi_burn_params();
        let dust = params.dust_shares;

        let submitted =
            mock.submit_burn(params, test_sendable_tx()).await.unwrap();
        assert_eq!(submitted.external_tx_id, "mock-burn");

        let result = mock.confirm_burn(&submitted.tx_id, dust).await.unwrap();

        assert_eq!(result.burns.len(), 1);
        assert_eq!(result.dust_returned, dust);
    }

    #[tokio::test]
    async fn test_multi_burn_call_count_increments() {
        let mock = MockVaultService::new_success();

        assert_eq!(mock.get_multi_burn_call_count(), 0);

        mock.submit_burn(test_multi_burn_params(), test_sendable_tx())
            .await
            .unwrap();
        assert_eq!(mock.get_multi_burn_call_count(), 1);

        mock.submit_burn(test_multi_burn_params(), test_sendable_tx())
            .await
            .unwrap();
        assert_eq!(mock.get_multi_burn_call_count(), 2);
    }

    #[tokio::test]
    async fn test_reset_clears_multi_burn_state() {
        let params = test_multi_burn_params();
        let sendable_tx = test_sendable_tx();
        let mock = MockVaultService::new_success()
            .with_prepared_tx(sendable_tx.clone());

        mock.submit_burn(params.clone(), sendable_tx.clone()).await.unwrap();
        mock.submit_burn(params.clone(), sendable_tx.clone()).await.unwrap();
        mock.classify_burn_tx(params.owner, &sendable_tx).await.unwrap();
        mock.prepare_burn_tx(&params).await.unwrap();
        mock.prepare_replacement_burn_tx(params.owner, &sendable_tx)
            .await
            .unwrap();
        mock.verify_burn_tx(test_vault(), test_receiver(), B256::ZERO)
            .await
            .unwrap();

        assert_eq!(mock.get_multi_burn_call_count(), 2);
        assert!(mock.get_last_multi_burn_params().is_some());
        assert_eq!(mock.submitted_burn_txs().len(), 2);
        assert_eq!(mock.verify_burn_call_count(), 1);
        assert_eq!(mock.burn_classification_call_count(), 1);
        assert_eq!(mock.burn_preparation_call_count(), 1);
        assert_eq!(mock.replacement_preparation_call_count(), 1);

        mock.reset();

        assert_eq!(mock.get_multi_burn_call_count(), 0);
        assert!(mock.get_last_multi_burn_params().is_none());
        assert!(mock.submitted_burn_txs().is_empty());
        assert_eq!(mock.verify_burn_call_count(), 0);
        assert_eq!(mock.burn_classification_call_count(), 0);
        assert_eq!(mock.burn_preparation_call_count(), 0);
        assert_eq!(mock.replacement_preparation_call_count(), 0);
        assert_eq!(
            mock.prepare_burn_tx(&test_multi_burn_params()).await.unwrap(),
            SendableTxWithHash::default(),
        );
    }

    #[tokio::test]
    async fn test_submit_mint_failure() {
        let mock = MockVaultService::new_submit_failure();
        let prepared = mock
            .prepare_mint_tx(
                test_vault(),
                U256::from(1000),
                test_receiver(),
                address!("0x1111111111111111111111111111111111111111"),
                test_receipt_info(),
                None,
            )
            .await
            .unwrap();
        let result = mock.submit_mint(&prepared).await;

        assert!(
            matches!(result, Err(VaultError::InvalidReceipt)),
            "Expected InvalidReceipt, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_submit_burn_failure() {
        let mock = MockVaultService::new_submit_failure();
        let result = mock
            .submit_burn(test_multi_burn_params(), test_sendable_tx())
            .await;

        assert!(
            matches!(result, Err(VaultError::InvalidReceipt)),
            "Expected InvalidReceipt, got {result:?}"
        );
    }

    /// Keeps the Turnkey wallet-intent / alternate-burn mock helpers reachable
    /// while multichain burn-manager tests catch up to those call sites.
    #[tokio::test]
    async fn turnkey_mock_helpers_remain_wired() {
        use std::sync::Arc;

        use crate::vault::{TxId, VerifiedBurn, VerifiedShareTransfer};

        let wallet_blocked =
            Arc::new(MockVaultService::new_wallet_lock_blocked());
        let wait_lock = tokio::spawn({
            let mock = Arc::clone(&wallet_blocked);
            async move {
                mock.wait_for_wallet_lock_attempt().await;
            }
        });
        tokio::task::yield_now().await;
        let lock_task = tokio::spawn({
            let mock = Arc::clone(&wallet_blocked);
            async move {
                mock.lock_wallet().await;
            }
        });
        wait_lock.await.unwrap();
        wallet_blocked.release_wallet_lock();
        lock_task.await.unwrap();

        let confirm_blocked =
            Arc::new(MockVaultService::new_confirm_pending_blocked());
        let wait_confirm = tokio::spawn({
            let mock = Arc::clone(&confirm_blocked);
            async move {
                mock.wait_for_burn_confirmation().await;
            }
        });
        tokio::task::yield_now().await;
        let confirm_task = tokio::spawn({
            let mock = Arc::clone(&confirm_blocked);
            async move { mock.confirm_burn(&TxId::random(), U256::ZERO).await }
        });
        wait_confirm.await.unwrap();
        confirm_blocked.release_burn_confirmation();
        let confirm_result = confirm_task.await.unwrap();
        assert!(matches!(
            confirm_result,
            Err(VaultError::ConfirmationPending { .. })
        ));

        let burns = vec![VerifiedBurn {
            sender: test_receiver(),
            receiver: Address::ZERO,
            receipt_id: U256::from(1u64),
            shares_burned: U256::from(17u64),
        }];
        let transfers = vec![VerifiedShareTransfer {
            recipient: test_receiver(),
            shares: U256::from(1u64),
        }];
        let _ = MockVaultService::new_success().with_verified_burns(
            1,
            0,
            burns.clone(),
            transfers.clone(),
        );
        let _ = MockVaultService::new_success().with_verified_burns_and_total(
            1,
            0,
            U256::from(17u64),
            burns,
            transfers,
        );
    }
}
