use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    BurnResult, MintResult, ReceiptInformation, VaultError, VaultService,
};

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MintTokensCall {
    pub(crate) assets: U256,
    pub(crate) receiver: Address,
    pub(crate) receipt_info: ReceiptInformation,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct BurnTokensCall {
    pub(crate) shares: U256,
    pub(crate) receipt_id: U256,
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
    Failure {
        reason: String,
    },
}

/// Mock blockchain service for testing.
///
/// This mock is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests
/// in `tests/`) needs to construct it. However, failure and delay support ARE behind
/// `#[cfg(test)]` because E2E tests only exercise the happy path and compile the library
/// without `#[cfg(test)]` enabled. Unit tests (inside the crate) can access `#[cfg(test)]`
/// code, so they get full mock functionality including failures and timing behavior.
pub(crate) struct MockVaultService {
    behavior: MockBehavior,
    mint_delay_ms: u64,
    burn_delay_ms: u64,
    call_count: Arc<AtomicUsize>,
    burn_call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    last_call: Arc<Mutex<Option<MintTokensCall>>>,
    #[cfg(test)]
    last_burn_call: Arc<Mutex<Option<BurnTokensCall>>>,
}

impl MockVaultService {
    #[must_use]
    pub(crate) fn new_success() -> Self {
        Self {
            behavior: MockBehavior::Success,
            mint_delay_ms: 0,
            burn_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            burn_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            last_call: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            last_burn_call: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failure(reason: impl Into<String>) -> Self {
        Self {
            behavior: MockBehavior::Failure { reason: reason.into() },
            mint_delay_ms: 0,
            burn_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            burn_call_count: Arc::new(AtomicUsize::new(0)),
            last_call: Arc::new(Mutex::new(None)),
            last_burn_call: Arc::new(Mutex::new(None)),
        }
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
    pub(crate) fn get_last_call(&self) -> Option<MintTokensCall> {
        self.last_call.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn get_burn_call_count(&self) -> usize {
        self.burn_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_last_burn_call(&self) -> Option<BurnTokensCall> {
        self.last_burn_call.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn reset(&self) {
        self.call_count.store(0, Ordering::Relaxed);
        self.burn_call_count.store(0, Ordering::Relaxed);
        *self.last_call.lock().unwrap() = None;
        *self.last_burn_call.lock().unwrap() = None;
    }
}

#[async_trait]
impl VaultService for MockVaultService {
    #[cfg_attr(not(test), allow(unused_variables))]
    async fn mint_tokens(
        &self,
        assets: U256,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, VaultError> {
        if self.mint_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.mint_delay_ms,
            ))
            .await;
        }

        self.call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_call.lock().unwrap() = Some(MintTokensCall {
                assets,
                receiver,
                receipt_info: receipt_info.clone(),
            });
        }

        match &self.behavior {
            MockBehavior::Success => Ok(MintResult {
                tx_hash: B256::from([0x42; 32]),
                receipt_id: U256::from(1),
                shares_minted: assets,
                gas_used: 21000,
                block_number: 1000,
            }),
            #[cfg(test)]
            MockBehavior::Failure { reason } => {
                Err(VaultError::TransactionFailed { reason: reason.clone() })
            }
        }
    }

    #[cfg_attr(not(test), allow(unused_variables))]
    async fn burn_tokens(
        &self,
        shares: U256,
        receipt_id: U256,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<BurnResult, VaultError> {
        if self.burn_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.burn_delay_ms,
            ))
            .await;
        }

        self.burn_call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_burn_call.lock().unwrap() = Some(BurnTokensCall {
                shares,
                receipt_id,
                receiver,
                receipt_info: receipt_info.clone(),
            });
        }

        match &self.behavior {
            MockBehavior::Success => Ok(BurnResult {
                tx_hash: B256::from([0x43; 32]),
                receipt_id,
                shares_burned: shares,
                gas_used: 25000,
                block_number: 2000,
            }),
            #[cfg(test)]
            MockBehavior::Failure { reason } => {
                Err(VaultError::TransactionFailed { reason: reason.clone() })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address};
    use chrono::Utc;
    use rust_decimal::Decimal;

    use super::MockVaultService;
    use crate::mint::{
        IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::vault::{
        OperationType, ReceiptInformation, VaultError, VaultService,
    };

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation {
            tokenization_request_id: TokenizationRequestId::new("tok-123"),
            issuer_request_id: IssuerRequestId::new("iss-456"),
            underlying: UnderlyingSymbol::new("AAPL"),
            quantity: Quantity::new(Decimal::from(100)),
            operation_type: OperationType::Mint,
            timestamp: Utc::now(),
            notes: None,
        }
    }

    fn test_receiver() -> Address {
        address!("0000000000000000000000000000000000000001")
    }

    #[tokio::test]
    async fn test_new_success_returns_mint_result() {
        let mock = MockVaultService::new_success();
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let result = mock.mint_tokens(assets, receiver, receipt_info).await;

        assert!(result.is_ok());
        let mint_result = result.unwrap();
        assert_eq!(mint_result.receipt_id, U256::from(1));
        assert_eq!(mint_result.shares_minted, assets);
        assert_eq!(mint_result.gas_used, 21000);
        assert_eq!(mint_result.block_number, 1000);
    }

    #[tokio::test]
    async fn test_new_failure_returns_error() {
        let mock = MockVaultService::new_failure("network error");
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let result = mock.mint_tokens(assets, receiver, receipt_info).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VaultError::TransactionFailed { reason } if reason == "network error"
        ));
    }

    #[tokio::test]
    async fn test_get_call_count_increments() {
        let mock = MockVaultService::new_success();
        let assets = U256::from(1000);
        let receiver = test_receiver();

        assert_eq!(mock.get_call_count(), 0);

        mock.mint_tokens(assets, receiver, test_receipt_info()).await.unwrap();
        assert_eq!(mock.get_call_count(), 1);

        mock.mint_tokens(assets, receiver, test_receipt_info()).await.unwrap();
        assert_eq!(mock.get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_get_last_call_captures_arguments() {
        let mock = MockVaultService::new_success();
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        assert!(mock.get_last_call().is_none());

        mock.mint_tokens(assets, receiver, receipt_info.clone()).await.unwrap();

        let last_call = mock.get_last_call();
        assert!(last_call.is_some());

        let call = last_call.unwrap();
        assert_eq!(call.assets, assets);
        assert_eq!(call.receiver, receiver);
        assert_eq!(
            call.receipt_info.tokenization_request_id.0,
            receipt_info.tokenization_request_id.0
        );
    }

    #[tokio::test]
    async fn test_with_delay_causes_delay() {
        let delay_ms = 50;
        let mock = MockVaultService::new_success().with_delay(delay_ms);
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let start = tokio::time::Instant::now();
        mock.mint_tokens(assets, receiver, receipt_info).await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() >= u128::from(delay_ms));
    }

    #[tokio::test]
    async fn test_reset_clears_state() {
        let mock = MockVaultService::new_success();
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        mock.mint_tokens(assets, receiver, receipt_info.clone()).await.unwrap();
        mock.mint_tokens(assets, receiver, receipt_info).await.unwrap();

        assert_eq!(mock.get_call_count(), 2);
        assert!(mock.get_last_call().is_some());

        mock.reset();

        assert_eq!(mock.get_call_count(), 0);
        assert!(mock.get_last_call().is_none());
    }

    #[tokio::test]
    async fn test_burn_new_success_returns_burn_result() {
        let mock = MockVaultService::new_success();
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let result =
            mock.burn_tokens(shares, receipt_id, receiver, receipt_info).await;

        assert!(result.is_ok());
        let burn_result = result.unwrap();
        assert_eq!(burn_result.receipt_id, receipt_id);
        assert_eq!(burn_result.shares_burned, shares);
        assert_eq!(burn_result.gas_used, 25000);
        assert_eq!(burn_result.block_number, 2000);
    }

    #[tokio::test]
    async fn test_burn_new_failure_returns_error() {
        let mock = MockVaultService::new_failure("blockchain error");
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let result =
            mock.burn_tokens(shares, receipt_id, receiver, receipt_info).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            VaultError::TransactionFailed { reason } if reason == "blockchain error"
        ));
    }

    #[tokio::test]
    async fn test_burn_get_call_count_increments() {
        let mock = MockVaultService::new_success();
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();

        assert_eq!(mock.get_burn_call_count(), 0);

        mock.burn_tokens(shares, receipt_id, receiver, test_receipt_info())
            .await
            .unwrap();
        assert_eq!(mock.get_burn_call_count(), 1);

        mock.burn_tokens(shares, receipt_id, receiver, test_receipt_info())
            .await
            .unwrap();
        assert_eq!(mock.get_burn_call_count(), 2);
    }

    #[tokio::test]
    async fn test_burn_get_last_call_captures_arguments() {
        let mock = MockVaultService::new_success();
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        assert!(mock.get_last_burn_call().is_none());

        mock.burn_tokens(shares, receipt_id, receiver, receipt_info.clone())
            .await
            .unwrap();

        let last_call = mock.get_last_burn_call();
        assert!(last_call.is_some());

        let call = last_call.unwrap();
        assert_eq!(call.shares, shares);
        assert_eq!(call.receipt_id, receipt_id);
        assert_eq!(call.receiver, receiver);
        assert_eq!(
            call.receipt_info.tokenization_request_id.0,
            receipt_info.tokenization_request_id.0
        );
    }

    #[tokio::test]
    async fn test_reset_clears_burn_state() {
        let mock = MockVaultService::new_success();
        let shares = U256::from(500);
        let receipt_id = U256::from(42);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        mock.burn_tokens(shares, receipt_id, receiver, receipt_info.clone())
            .await
            .unwrap();
        mock.burn_tokens(shares, receipt_id, receiver, receipt_info)
            .await
            .unwrap();

        assert_eq!(mock.get_burn_call_count(), 2);
        assert!(mock.get_last_burn_call().is_some());

        mock.reset();

        assert_eq!(mock.get_burn_call_count(), 0);
        assert!(mock.get_last_burn_call().is_none());
    }
}
