use alloy::primitives::{Address, Bytes, U256, b256};
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::{
    MintResult, MultiBurnParams, MultiBurnResult, MultiBurnResultEntry,
    ReceiptInformation, SubmittedTx, VaultError, VaultService,
};

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
}

impl MockVaultService {
    #[must_use]
    pub(crate) fn new_success() -> Self {
        Self {
            behavior: MockBehavior::Success,
            mint_delay_ms: 0,
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
        }
    }

    #[cfg(test)]
    pub(crate) fn new_failure() -> Self {
        Self {
            behavior: MockBehavior::Failure,
            mint_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            multi_burn_call_count: Arc::new(AtomicUsize::new(0)),
            pending_mint_result: Arc::new(Mutex::new(None)),
            pending_burn_result: Arc::new(Mutex::new(None)),
            last_call: Arc::new(Mutex::new(None)),
            share_balance: Arc::new(Mutex::new(U256::MAX)),
            last_multi_burn_params: Arc::new(Mutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(crate) fn new_submit_failure() -> Self {
        Self {
            behavior: MockBehavior::SubmitFailure,
            mint_delay_ms: 0,
            call_count: Arc::new(AtomicUsize::new(0)),
            multi_burn_call_count: Arc::new(AtomicUsize::new(0)),
            pending_mint_result: Arc::new(Mutex::new(None)),
            pending_burn_result: Arc::new(Mutex::new(None)),
            last_call: Arc::new(Mutex::new(None)),
            share_balance: Arc::new(Mutex::new(U256::MAX)),
            last_multi_burn_params: Arc::new(Mutex::new(None)),
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
    pub(crate) fn get_multi_burn_call_count(&self) -> usize {
        self.multi_burn_call_count.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn get_last_multi_burn_params(&self) -> Option<MultiBurnParams> {
        self.last_multi_burn_params.lock().unwrap().clone()
    }

    #[cfg(test)]
    pub(crate) fn reset(&self) {
        self.call_count.store(0, Ordering::Relaxed);
        self.multi_burn_call_count.store(0, Ordering::Relaxed);
        *self.last_call.lock().unwrap() = None;
        *self.pending_mint_result.lock().unwrap() = None;
        *self.pending_burn_result.lock().unwrap() = None;
    }

    #[cfg(test)]
    pub(crate) fn with_share_balance(self, balance: U256) -> Self {
        *self.share_balance.lock().unwrap() = balance;
        self
    }
}

const MOCK_MINT_TX_HASH: alloy::primitives::B256 =
    b256!("0x4242424242424242424242424242424242424242424242424242424242424242");

const MOCK_BURN_TX_HASH: alloy::primitives::B256 =
    b256!("0x4545454545454545454545454545454545454545454545454545454545454545");

#[async_trait]
impl VaultService for MockVaultService {
    #[cfg_attr(not(test), allow(unused_variables))]
    async fn submit_mint(
        &self,
        vault: Address,
        assets: U256,
        bot: Address,
        _user: Address,
        receipt_info: ReceiptInformation,
    ) -> Result<SubmittedTx, VaultError> {
        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitFailure) {
            return Err(VaultError::InvalidReceipt);
        }

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

        // Pre-compute the MintResult for confirm_mint to return.
        let receipt_info_bytes = match receipt_info.encode(Some(
            "bafkreiahuttak2jvjzsd4r62xhf2fwvy7hbpbfdetxrieqxf4ivyxgpdm",
        )) {
            Ok(bytes) => bytes,
            Err(err) => {
                return Err(VaultError::ReceiptEncode(err));
            }
        };

        *self
            .pending_mint_result
            .lock()
            .expect("pending_mint_result mutex poisoned") = Some(MintResult {
            tx_hash: MOCK_MINT_TX_HASH,
            receipt_id: U256::from(1),
            shares_minted: assets,
            gas_used: 21000,
            block_number: 1000,
            receipt_info_bytes,
        });

        Ok(SubmittedTx {
            external_tx_id: "mock-mint".to_string(),
            fireblocks_tx_id: "mock-fb-mint".to_string(),
        })
    }

    async fn confirm_mint(
        &self,
        _fireblocks_tx_id: &str,
    ) -> Result<MintResult, VaultError> {
        match &self.behavior {
            MockBehavior::Success => {
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

    async fn submit_burn(
        &self,
        params: MultiBurnParams,
    ) -> Result<SubmittedTx, VaultError> {
        #[cfg(test)]
        if matches!(self.behavior, MockBehavior::SubmitFailure) {
            return Err(VaultError::InvalidReceipt);
        }

        self.multi_burn_call_count.fetch_add(1, Ordering::Relaxed);

        #[cfg(test)]
        {
            *self.last_multi_burn_params.lock().unwrap() = Some(params.clone());
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
            external_tx_id: "mock-burn".to_string(),
            fireblocks_tx_id: "mock-fb-burn".to_string(),
        })
    }

    async fn confirm_burn(
        &self,
        _fireblocks_tx_id: &str,
        dust_shares: U256,
    ) -> Result<MultiBurnResult, VaultError> {
        match &self.behavior {
            MockBehavior::Success => {
                let result = self
                    .pending_burn_result
                    .lock()
                    .expect("pending_burn_result mutex poisoned")
                    .take()
                    .unwrap_or_else(|| MultiBurnResult {
                        tx_hash: MOCK_BURN_TX_HASH,
                        burns: vec![],
                        dust_returned: dust_shares,
                        gas_used: 50000,
                        block_number: 5000,
                    });

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
                    .unwrap_or_else(|| MultiBurnResult {
                        tx_hash: MOCK_BURN_TX_HASH,
                        burns: vec![],
                        dust_returned: dust_shares,
                        gas_used: 50000,
                        block_number: 5000,
                    });
                Ok(result)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, U256, address, b256};
    use chrono::Utc;
    use rust_decimal::Decimal;

    use super::MockVaultService;
    use crate::mint::{
        IssuerMintRequestId, Quantity, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::redemption::IssuerRedemptionRequestId;
    use crate::vault::{
        MultiBurnEntry, MultiBurnParams, ReceiptInformation, VaultError,
        VaultService,
    };

    fn test_receipt_info() -> ReceiptInformation {
        ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            IssuerMintRequestId::random(),
            UnderlyingSymbol::new("AAPL"),
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

        let submitted = mock
            .submit_mint(vault, assets, bot_wallet, user_wallet, receipt_info)
            .await
            .unwrap();

        assert_eq!(submitted.external_tx_id, "mock-mint");

        let result =
            mock.confirm_mint(&submitted.fireblocks_tx_id).await.unwrap();

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
        let submitted = mock
            .submit_mint(vault, assets, bot_wallet, user_wallet, receipt_info)
            .await
            .unwrap();

        // Confirm returns the failure
        let result = mock.confirm_mint(&submitted.fireblocks_tx_id).await;
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

        mock.submit_mint(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            test_receipt_info(),
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

        mock.submit_mint(
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
        mock.submit_mint(vault, assets, bot_wallet, user_wallet, receipt_info)
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

        mock.submit_mint(
            vault,
            assets,
            bot_wallet,
            user_wallet,
            receipt_info.clone(),
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
            issuer_request_id: IssuerRedemptionRequestId::new(detected_tx_hash),
            detected_tx_hash,
        }
    }

    #[tokio::test]
    async fn test_submit_and_confirm_burn_success() {
        let mock = MockVaultService::new_success();
        let params = test_multi_burn_params();
        let dust = params.dust_shares;

        let submitted = mock.submit_burn(params).await.unwrap();
        assert_eq!(submitted.external_tx_id, "mock-burn");

        let result =
            mock.confirm_burn(&submitted.fireblocks_tx_id, dust).await.unwrap();

        assert_eq!(result.burns.len(), 1);
        assert_eq!(result.dust_returned, dust);
    }

    #[tokio::test]
    async fn test_multi_burn_call_count_increments() {
        let mock = MockVaultService::new_success();

        assert_eq!(mock.get_multi_burn_call_count(), 0);

        mock.submit_burn(test_multi_burn_params()).await.unwrap();
        assert_eq!(mock.get_multi_burn_call_count(), 1);

        mock.submit_burn(test_multi_burn_params()).await.unwrap();
        assert_eq!(mock.get_multi_burn_call_count(), 2);
    }

    #[tokio::test]
    async fn test_reset_clears_multi_burn_state() {
        let mock = MockVaultService::new_success();

        mock.submit_burn(test_multi_burn_params()).await.unwrap();
        mock.submit_burn(test_multi_burn_params()).await.unwrap();

        assert_eq!(mock.get_multi_burn_call_count(), 2);

        mock.reset();

        assert_eq!(mock.get_multi_burn_call_count(), 0);
    }

    #[tokio::test]
    async fn test_submit_mint_failure() {
        let mock = MockVaultService::new_submit_failure();
        let result = mock
            .submit_mint(
                test_vault(),
                U256::from(1000),
                test_receiver(),
                address!("0x1111111111111111111111111111111111111111"),
                test_receipt_info(),
            )
            .await;

        assert!(
            matches!(result, Err(VaultError::InvalidReceipt)),
            "Expected InvalidReceipt, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_submit_burn_failure() {
        let mock = MockVaultService::new_submit_failure();
        let result = mock.submit_burn(test_multi_burn_params()).await;

        assert!(
            matches!(result, Err(VaultError::InvalidReceipt)),
            "Expected InvalidReceipt, got {result:?}"
        );
    }
}
