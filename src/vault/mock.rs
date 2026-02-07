use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    BurnParams, BurnWithDustResult, MintResult, ReceiptInformation, VaultError,
    VaultService,
};

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct MintTokensCall {
    pub(crate) vault: Address,
    pub(crate) assets: U256,
    pub(crate) receiver: Address,
    pub(crate) receipt_info: ReceiptInformation,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub(crate) struct BurnWithDustCall {
    pub(crate) vault: Address,
    pub(crate) burn_shares: U256,
    pub(crate) dust_shares: U256,
    pub(crate) receipt_id: U256,
    pub(crate) owner: Address,
    pub(crate) user: Address,
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
    burn_with_dust_call_count: Arc<AtomicUsize>,
    #[cfg(test)]
    last_call: Arc<Mutex<Option<MintTokensCall>>>,
    #[cfg(test)]
    last_burn_with_dust_call: Arc<Mutex<Option<BurnWithDustCall>>>,
    #[cfg(test)]
    share_balance: Arc<Mutex<U256>>,
}

impl MockVaultService {
    #[must_use]
    pub(crate) fn new_success() -> Self {
        Self {
            behavior: MockBehavior::Success,
            mint_delay_ms: 0,
            burn_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            burn_with_dust_call_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(test)]
            last_call: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            last_burn_with_dust_call: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            share_balance: Arc::new(Mutex::new(U256::MAX)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failure() -> Self {
        Self {
            behavior: MockBehavior::Failure,
            mint_delay_ms: 0,
            burn_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            burn_with_dust_call_count: Arc::new(AtomicUsize::new(0)),
            last_call: Arc::new(Mutex::new(None)),
            last_burn_with_dust_call: Arc::new(Mutex::new(None)),
            share_balance: Arc::new(Mutex::new(U256::MAX)),
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
    pub(crate) fn get_burn_with_dust_call_count(&self) -> usize {
        self.burn_with_dust_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_last_burn_with_dust_call(
        &self,
    ) -> Option<BurnWithDustCall> {
        self.last_burn_with_dust_call.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn reset(&self) {
        self.call_count.store(0, Ordering::Relaxed);
        self.burn_with_dust_call_count.store(0, Ordering::Relaxed);
        *self.last_call.lock().unwrap() = None;
        *self.last_burn_with_dust_call.lock().unwrap() = None;
    }

    #[cfg(test)]
    pub(crate) fn with_share_balance(self, balance: U256) -> Self {
        *self.share_balance.lock().unwrap() = balance;
        self
    }
}

#[async_trait]
impl VaultService for MockVaultService {
    #[cfg_attr(not(test), allow(unused_variables))]
    async fn mint_and_transfer_shares(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        _user: Address,
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
                vault,
                assets,
                receiver: bot,
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
            MockBehavior::Failure => Err(VaultError::InvalidReceipt),
        }
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

    #[cfg_attr(not(test), allow(unused_variables))]
    async fn burn_and_return_dust(
        &self,
        params: BurnParams,
    ) -> Result<BurnWithDustResult, VaultError> {
        if self.burn_delay_ms > 0 {
            tokio::time::sleep(tokio::time::Duration::from_millis(
                self.burn_delay_ms,
            ))
            .await;
        }

        self.burn_with_dust_call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_burn_with_dust_call.lock().unwrap() =
                Some(BurnWithDustCall {
                    vault: params.vault,
                    burn_shares: params.burn_shares,
                    dust_shares: params.dust_shares,
                    receipt_id: params.receipt_id,
                    owner: params.owner,
                    user: params.user,
                    receipt_info: params.receipt_info.clone(),
                });
        }

        match &self.behavior {
            MockBehavior::Success => Ok(BurnWithDustResult {
                tx_hash: B256::from([0x44; 32]),
                receipt_id: params.receipt_id,
                shares_burned: params.burn_shares,
                dust_returned: params.dust_shares,
                gas_used: 30000,
                block_number: 3000,
            }),
            #[cfg(test)]
            MockBehavior::Failure => Err(VaultError::InvalidReceipt),
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
        BurnParams, OperationType, ReceiptInformation, VaultError, VaultService,
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

    fn test_vault() -> Address {
        address!("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
    }

    #[tokio::test]
    async fn test_new_success_returns_mint_result() {
        let mock = MockVaultService::new_success();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        let result = mock
            .mint_and_transfer_shares(
                vault,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
            )
            .await;

        assert!(result.is_ok());
        let mint_result = result.unwrap();
        assert_eq!(mint_result.receipt_id, U256::from(1));
        assert_eq!(mint_result.shares_minted, assets);
        assert_eq!(mint_result.gas_used, 21000);
        assert_eq!(mint_result.block_number, 1000);
    }

    #[tokio::test]
    async fn test_new_failure_returns_error() {
        let mock = MockVaultService::new_failure();
        let vault = test_vault();
        let assets = U256::from(1000);
        let bot_wallet = test_receiver();
        let user_wallet =
            address!("0x1111111111111111111111111111111111111111");
        let receipt_info = test_receipt_info();

        let result = mock
            .mint_and_transfer_shares(
                vault,
                assets,
                bot_wallet,
                user_wallet,
                receipt_info,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VaultError::InvalidReceipt));
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

        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            test_receipt_info(),
        )
        .await
        .unwrap();
        assert_eq!(mock.get_call_count(), 1);

        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            test_receipt_info(),
        )
        .await
        .unwrap();
        assert_eq!(mock.get_call_count(), 2);
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

        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info.clone(),
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
            call.receipt_info.tokenization_request_id.0,
            receipt_info.tokenization_request_id.0
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
        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info,
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

        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info.clone(),
        )
        .await
        .unwrap();
        mock.mint_and_transfer_shares(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info,
        )
        .await
        .unwrap();

        assert_eq!(mock.get_call_count(), 2);
        assert!(mock.get_last_call().is_some());

        mock.reset();

        assert_eq!(mock.get_call_count(), 0);
        assert!(mock.get_last_call().is_none());
    }

    #[tokio::test]
    async fn test_burn_and_return_dust_success() {
        let mock = MockVaultService::new_success();
        let burn_shares = U256::from(500);
        let dust_shares = U256::from(10);
        let receipt_id = U256::from(42);
        let params = BurnParams {
            vault: test_vault(),
            burn_shares,
            dust_shares,
            receipt_id,
            owner: test_receiver(),
            user: address!("0x2222222222222222222222222222222222222222"),
            receipt_info: test_receipt_info(),
        };

        let result = mock.burn_and_return_dust(params).await;

        assert!(result.is_ok());
        let burn_result = result.unwrap();
        assert_eq!(burn_result.receipt_id, receipt_id);
        assert_eq!(burn_result.shares_burned, burn_shares);
        assert_eq!(burn_result.dust_returned, dust_shares);
        assert_eq!(burn_result.gas_used, 30000);
        assert_eq!(burn_result.block_number, 3000);
    }

    #[tokio::test]
    async fn test_burn_and_return_dust_failure() {
        let mock = MockVaultService::new_failure();
        let params = BurnParams {
            vault: test_vault(),
            burn_shares: U256::from(500),
            dust_shares: U256::from(10),
            receipt_id: U256::from(42),
            owner: test_receiver(),
            user: address!("0x2222222222222222222222222222222222222222"),
            receipt_info: test_receipt_info(),
        };

        let result = mock.burn_and_return_dust(params).await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), VaultError::InvalidReceipt));
    }

    fn test_burn_params() -> BurnParams {
        BurnParams {
            vault: test_vault(),
            burn_shares: U256::from(500),
            dust_shares: U256::from(10),
            receipt_id: U256::from(42),
            owner: test_receiver(),
            user: address!("0x2222222222222222222222222222222222222222"),
            receipt_info: test_receipt_info(),
        }
    }

    #[tokio::test]
    async fn test_burn_with_dust_call_count_increments() {
        let mock = MockVaultService::new_success();

        assert_eq!(mock.get_burn_with_dust_call_count(), 0);

        mock.burn_and_return_dust(test_burn_params()).await.unwrap();
        assert_eq!(mock.get_burn_with_dust_call_count(), 1);

        mock.burn_and_return_dust(test_burn_params()).await.unwrap();
        assert_eq!(mock.get_burn_with_dust_call_count(), 2);
    }

    #[tokio::test]
    async fn test_burn_with_dust_last_call_captures_arguments() {
        let mock = MockVaultService::new_success();
        let params = test_burn_params();

        assert!(mock.get_last_burn_with_dust_call().is_none());

        mock.burn_and_return_dust(params.clone()).await.unwrap();

        let last_call = mock.get_last_burn_with_dust_call();
        assert!(last_call.is_some());

        let call = last_call.unwrap();
        assert_eq!(call.vault, params.vault);
        assert_eq!(call.burn_shares, params.burn_shares);
        assert_eq!(call.dust_shares, params.dust_shares);
        assert_eq!(call.receipt_id, params.receipt_id);
        assert_eq!(call.owner, params.owner);
        assert_eq!(call.user, params.user);
        assert_eq!(
            call.receipt_info.tokenization_request_id.0,
            params.receipt_info.tokenization_request_id.0
        );
    }

    #[tokio::test]
    async fn test_reset_clears_burn_with_dust_state() {
        let mock = MockVaultService::new_success();

        mock.burn_and_return_dust(test_burn_params()).await.unwrap();
        mock.burn_and_return_dust(test_burn_params()).await.unwrap();

        assert_eq!(mock.get_burn_with_dust_call_count(), 2);
        assert!(mock.get_last_burn_with_dust_call().is_some());

        mock.reset();

        assert_eq!(mock.get_burn_with_dust_call_count(), 0);
        assert!(mock.get_last_burn_with_dust_call().is_none());
    }
}
