use async_trait::async_trait;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{Transfer, TransferError, TransferService};

/// Mock behavior for transfer service.
///
/// This enum is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests)
/// may need it. However, the Failure variant IS behind `#[cfg(test)]` because E2E tests only
/// need the happy path and compile the library without `#[cfg(test)]` enabled.
enum MockBehavior {
    Success {
        transfers: Vec<Transfer>,
    },
    #[cfg(test)]
    Failure {
        reason: String,
    },
}

/// Mock transfer service for testing.
///
/// This mock is NOT behind `#[cfg(test)]` because `setup_test_rocket()` (used by E2E tests
/// in `tests/`) may need to construct it. However, failure support IS behind `#[cfg(test)]`
/// because E2E tests only exercise the happy path and compile the library without
/// `#[cfg(test)]` enabled.
pub(crate) struct MockTransferService {
    behavior: MockBehavior,
    call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    last_call_timestamp: Arc<Mutex<Option<std::time::Instant>>>,
}

impl MockTransferService {
    #[must_use]
    pub(crate) fn new_success(transfers: Vec<Transfer>) -> Self {
        Self {
            behavior: MockBehavior::Success { transfers },
            call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            last_call_timestamp: Arc::new(Mutex::new(None)),
        }
    }

    #[must_use]
    pub(crate) fn new_empty() -> Self {
        Self::new_success(vec![])
    }

    #[cfg(test)]
    pub(crate) fn new_failure(reason: impl Into<String>) -> Self {
        Self {
            behavior: MockBehavior::Failure { reason: reason.into() },
            call_count: Arc::new(AtomicUsize::new(0)),
            last_call_timestamp: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn reset(&self) {
        self.call_count.store(0, Ordering::Relaxed);
        *self.last_call_timestamp.lock().unwrap() = None;
    }
}

#[async_trait]
impl TransferService for MockTransferService {
    async fn watch(&self) -> Result<Vec<Transfer>, TransferError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_call_timestamp.lock().unwrap() =
                Some(std::time::Instant::now());
        }

        match &self.behavior {
            MockBehavior::Success { transfers } => Ok(transfers.clone()),
            #[cfg(test)]
            MockBehavior::Failure { reason } => {
                Err(TransferError::FetchFailed(reason.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};

    use super::MockTransferService;
    use crate::mint::IssuerRequestId;
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::transfer::{Transfer, TransferService};

    fn create_test_transfer() -> Transfer {
        Transfer {
            issuer_request_id: IssuerRequestId::new("iss-123"),
            from: address!("0x1111111111111111111111111111111111111111"),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            amount: uint!(100_000000000000000000_U256),
            tx_hash: b256!(
                "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
            ),
            block_number: 1000,
        }
    }

    #[tokio::test]
    async fn test_new_success_returns_transfers() {
        let transfer = create_test_transfer();
        let mock = MockTransferService::new_success(vec![transfer.clone()]);

        let result = mock.watch().await;

        assert!(result.is_ok());
        let transfers = result.unwrap();
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].issuer_request_id, transfer.issuer_request_id);
        assert_eq!(transfers[0].from, transfer.from);
        assert_eq!(transfers[0].amount, transfer.amount);
    }

    #[tokio::test]
    async fn test_new_empty_returns_empty_vec() {
        let mock = MockTransferService::new_empty();

        let result = mock.watch().await;

        assert!(result.is_ok());
        let transfers = result.unwrap();
        assert_eq!(transfers.len(), 0);
    }

    #[tokio::test]
    async fn test_new_failure_returns_error() {
        let mock = MockTransferService::new_failure("connection timeout");

        let result = mock.watch().await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(
            err,
            crate::transfer::TransferError::FetchFailed(msg) if msg == "connection timeout"
        ));
    }

    #[tokio::test]
    async fn test_get_call_count_increments() {
        let mock = MockTransferService::new_empty();

        assert_eq!(mock.get_call_count(), 0);

        mock.watch().await.unwrap();
        assert_eq!(mock.get_call_count(), 1);

        mock.watch().await.unwrap();
        assert_eq!(mock.get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_reset_clears_state() {
        let mock = MockTransferService::new_empty();

        mock.watch().await.unwrap();
        mock.watch().await.unwrap();
        assert_eq!(mock.get_call_count(), 2);

        mock.reset();

        assert_eq!(mock.get_call_count(), 0);
    }

    #[tokio::test]
    async fn test_multiple_transfers() {
        let first_transfer = create_test_transfer();
        let mut second_transfer = create_test_transfer();
        second_transfer.issuer_request_id = IssuerRequestId::new("iss-456");
        second_transfer.amount = uint!(200_000000000000000000_U256);

        let mock = MockTransferService::new_success(vec![
            first_transfer.clone(),
            second_transfer.clone(),
        ]);

        let result = mock.watch().await;

        assert!(result.is_ok());
        let transfers = result.unwrap();
        assert_eq!(transfers.len(), 2);
        assert_eq!(
            transfers[0].issuer_request_id,
            first_transfer.issuer_request_id
        );
        assert_eq!(
            transfers[1].issuer_request_id,
            second_transfer.issuer_request_id
        );
        assert_eq!(transfers[0].amount, first_transfer.amount);
        assert_eq!(transfers[1].amount, second_transfer.amount);
    }
}
