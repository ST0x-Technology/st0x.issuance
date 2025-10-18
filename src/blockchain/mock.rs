use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use super::{
    BlockchainError, BlockchainService, MintResult, ReceiptInformation,
};

pub(crate) struct MockBlockchainService {
    should_succeed: bool,
    failure_reason: Option<String>,
    mint_delay_ms: u64,
    call_count: Arc<Mutex<usize>>,
    last_call: Arc<Mutex<Option<(U256, Address, ReceiptInformation)>>>,
}

impl MockBlockchainService {
    pub(crate) fn new_success() -> Self {
        Self {
            should_succeed: true,
            failure_reason: None,
            mint_delay_ms: 0,
            call_count: Arc::new(Mutex::new(0)),
            last_call: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn new_failure(reason: impl Into<String>) -> Self {
        Self {
            should_succeed: false,
            failure_reason: Some(reason.into()),
            mint_delay_ms: 0,
            call_count: Arc::new(Mutex::new(0)),
            last_call: Arc::new(Mutex::new(None)),
        }
    }

    pub(crate) fn with_delay(mut self, delay_ms: u64) -> Self {
        self.mint_delay_ms = delay_ms;
        self
    }

    pub(crate) fn get_call_count(&self) -> usize {
        *self.call_count.lock().unwrap()
    }

    pub(crate) fn get_last_call(
        &self,
    ) -> Option<(U256, Address, ReceiptInformation)> {
        self.last_call.lock().unwrap().clone()
    }

    pub(crate) fn reset(&self) {
        *self.call_count.lock().unwrap() = 0;
        *self.last_call.lock().unwrap() = None;
    }
}

#[async_trait]
impl BlockchainService for MockBlockchainService {
    async fn mint_tokens(
        &self,
        assets: U256,
        receiver: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<MintResult, BlockchainError> {
        if self.mint_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.mint_delay_ms,
            ))
            .await;
        }

        *self.call_count.lock().unwrap() += 1;

        *self.last_call.lock().unwrap() =
            Some((assets, receiver, receipt_info));

        if self.should_succeed {
            Ok(MintResult {
                tx_hash: B256::from([0x42; 32]),
                receipt_id: U256::from(1),
                shares_minted: assets,
                gas_used: 21000,
                block_number: 1000,
            })
        } else {
            Err(BlockchainError::TransactionFailed {
                reason: self
                    .failure_reason
                    .clone()
                    .unwrap_or_else(|| "Mock failure".to_string()),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address};
    use chrono::Utc;
    use rust_decimal::Decimal;

    use crate::{
        blockchain::{BlockchainService, OperationType, ReceiptInformation},
        mint::{
            IssuerRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
        },
    };

    use super::MockBlockchainService;

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
        let mock = MockBlockchainService::new_success();
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
        let mock = MockBlockchainService::new_failure("network error");
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let result = mock.mint_tokens(assets, receiver, receipt_info).await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::blockchain::BlockchainError::TransactionFailed { reason } if reason == "network error"
        ));
    }

    #[tokio::test]
    async fn test_get_call_count_increments() {
        let mock = MockBlockchainService::new_success();
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
        let mock = MockBlockchainService::new_success();
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        assert!(mock.get_last_call().is_none());

        mock.mint_tokens(assets, receiver, receipt_info.clone()).await.unwrap();

        let last_call = mock.get_last_call();
        assert!(last_call.is_some());

        let (captured_assets, captured_receiver, captured_info) =
            last_call.unwrap();
        assert_eq!(captured_assets, assets);
        assert_eq!(captured_receiver, receiver);
        assert_eq!(
            captured_info.tokenization_request_id.0,
            receipt_info.tokenization_request_id.0
        );
    }

    #[tokio::test]
    async fn test_with_delay_causes_delay() {
        let delay_ms = 50;
        let mock = MockBlockchainService::new_success().with_delay(delay_ms);
        let assets = U256::from(1000);
        let receiver = test_receiver();
        let receipt_info = test_receipt_info();

        let start = tokio::time::Instant::now();
        mock.mint_tokens(assets, receiver, receipt_info).await.unwrap();
        let elapsed = start.elapsed();

        assert!(elapsed.as_millis() >= delay_ms as u128);
    }

    #[tokio::test]
    async fn test_reset_clears_state() {
        let mock = MockBlockchainService::new_success();
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
}
