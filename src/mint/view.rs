use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use super::{
    ClientId, IssuerRequestId, Mint, MintEvent, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintView {
    NotFound,
    Initiated {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
    },
    JournalConfirmed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
    },
    JournalRejected {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        reason: String,
        rejected_at: DateTime<Utc>,
    },
    CallbackPending {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    },
    MintingFailed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        error: String,
        failed_at: DateTime<Utc>,
    },
    Completed {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
        journal_confirmed_at: DateTime<Utc>,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
}

impl Default for MintView {
    fn default() -> Self {
        Self::NotFound
    }
}

impl MintView {
    fn handle_initiated(&mut self, event: &MintEvent) {
        let MintEvent::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = event
        else {
            return;
        };

        *self = Self::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
            wallet: *wallet,
            initiated_at: *initiated_at,
        };
    }

    fn handle_journal_confirmed(&mut self, confirmed_at: DateTime<Utc>) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at: confirmed_at,
        };
    }

    fn handle_journal_rejected(
        &mut self,
        reason: String,
        rejected_at: DateTime<Utc>,
    ) {
        let Self::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::JournalRejected {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            reason,
            rejected_at,
        };
    }

    fn handle_tokens_minted(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: u64,
        block_number: u64,
        minted_at: DateTime<Utc>,
    ) {
        let Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };
    }

    fn handle_minting_failed(
        &mut self,
        error: String,
        failed_at: DateTime<Utc>,
    ) {
        let Self::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error,
            failed_at,
        };
    }

    fn handle_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::CallbackPending {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        } = self.clone()
        else {
            return;
        };

        *self = Self::Completed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
            completed_at,
        };
    }
}

impl View<Mint> for MintView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
            MintEvent::Initiated { .. } => {
                self.handle_initiated(&event.payload);
            }
            MintEvent::JournalConfirmed { confirmed_at, .. } => {
                self.handle_journal_confirmed(*confirmed_at);
            }
            MintEvent::JournalRejected { reason, rejected_at, .. } => {
                self.handle_journal_rejected(reason.clone(), *rejected_at);
            }
            MintEvent::TokensMinted {
                tx_hash,
                receipt_id,
                shares_minted,
                gas_used,
                block_number,
                minted_at,
                ..
            } => {
                self.handle_tokens_minted(
                    *tx_hash,
                    *receipt_id,
                    *shares_minted,
                    *gas_used,
                    *block_number,
                    *minted_at,
                );
            }
            MintEvent::MintingFailed { error, failed_at, .. } => {
                self.handle_minting_failed(error.clone(), *failed_at);
            }
            MintEvent::MintCompleted { completed_at, .. } => {
                self.handle_mint_completed(*completed_at);
            }
        }
    }
}

pub(crate) async fn find_by_issuer_request_id(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRequestId,
) -> Result<Option<MintView>, MintViewError> {
    let issuer_request_id_str = &issuer_request_id.0;
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM mint_view
        WHERE view_id = ?
        "#,
        issuer_request_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(row) = row else {
        return Ok(None);
    };

    let view: MintView = serde_json::from_str(&row.payload)?;

    Ok(Some(view))
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use cqrs_es::EventEnvelope;
    use rust_decimal::Decimal;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;

    use super::*;
    use crate::mint::MintEvent;

    async fn setup_test_db() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .expect("Failed to create in-memory database");

        sqlx::migrate!("./migrations")
            .run(&pool)
            .await
            .expect("Failed to run migrations");

        pool
    }

    #[test]
    fn test_view_update_from_mint_initiated_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let event = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
            wallet,
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = MintView::default();

        assert!(matches!(view, MintView::NotFound));

        view.update(&envelope);

        let MintView::Initiated {
            issuer_request_id: view_issuer_id,
            tokenization_request_id: view_tokenization_id,
            quantity: view_quantity,
            underlying: view_underlying,
            token: view_token,
            network: view_network,
            client_id: view_client_id,
            wallet: view_wallet,
            initiated_at: view_initiated_at,
        } = view
        else {
            panic!("Expected Initiated variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_tokenization_id, tokenization_request_id);
        assert_eq!(view_quantity, quantity);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_network, network);
        assert_eq!(view_client_id, client_id);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_view() {
        let pool = setup_test_db().await;

        let issuer_request_id = IssuerRequestId::new("iss-999");
        let tokenization_request_id = TokenizationRequestId::new("alp-888");
        let quantity = Quantity::new(Decimal::from(50));
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let client_id = ClientId("client-777".to_string());
        let wallet = address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd");
        let initiated_at = Utc::now();

        let view = MintView::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id: client_id.clone(),
            wallet,
            initiated_at,
        };

        let payload =
            serde_json::to_string(&view).expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO mint_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            issuer_request_id.0,
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert view");

        let result = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_some());

        let MintView::Initiated {
            issuer_request_id: found_issuer_id,
            tokenization_request_id: found_tokenization_id,
            quantity: found_quantity,
            underlying: found_underlying,
            token: found_token,
            network: found_network,
            client_id: found_client_id,
            wallet: found_wallet,
            initiated_at: _,
        } = result.unwrap()
        else {
            panic!("Expected Initiated variant")
        };

        assert_eq!(found_issuer_id, issuer_request_id);
        assert_eq!(found_tokenization_id, tokenization_request_id);
        assert_eq!(found_quantity, quantity);
        assert_eq!(found_underlying, underlying);
        assert_eq!(found_token, token);
        assert_eq!(found_network, network);
        assert_eq!(found_client_id, client_id);
        assert_eq!(found_wallet, wallet);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let issuer_request_id = IssuerRequestId::new("nonexistent-id");

        let result = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[test]
    fn test_view_update_from_journal_confirmed_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let confirmed_at = Utc::now();

        let event = MintEvent::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            confirmed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::JournalConfirmed {
            issuer_request_id: view_issuer_id,
            journal_confirmed_at,
            ..
        } = view
        else {
            panic!("Expected JournalConfirmed variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(journal_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_view_update_from_journal_rejected_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };

        let rejected_at = Utc::now();
        let reason = "Insufficient funds".to_string();

        let event = MintEvent::JournalRejected {
            issuer_request_id: issuer_request_id.clone(),
            reason: reason.clone(),
            rejected_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::JournalRejected {
            issuer_request_id: view_issuer_id,
            reason: view_reason,
            rejected_at: view_rejected_at,
            ..
        } = view
        else {
            panic!("Expected JournalRejected variant")
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_reason, reason);
        assert_eq!(view_rejected_at, rejected_at);
    }

    #[test]
    fn test_view_update_from_tokens_minted_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut view = MintView::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying: underlying.clone(),
            token: token.clone(),
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        let event = MintEvent::TokensMinted {
            issuer_request_id: issuer_request_id.clone(),
            underlying,
            token,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::CallbackPending {
            issuer_request_id: view_issuer_id,
            tx_hash: view_tx_hash,
            receipt_id: view_receipt_id,
            shares_minted: view_shares_minted,
            gas_used: view_gas_used,
            block_number: view_block_number,
            minted_at: view_minted_at,
            ..
        } = view
        else {
            panic!("Expected CallbackPending variant, got {view:?}");
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_shares_minted, shares_minted);
        assert_eq!(view_gas_used, gas_used);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_minted_at, minted_at);
    }

    #[test]
    fn test_view_update_from_minting_failed_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut view = MintView::JournalConfirmed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };

        let error_message = "Transaction failed: insufficient gas";
        let failed_at = Utc::now();

        let event = MintEvent::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: error_message.to_string(),
            failed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 3,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::MintingFailed {
            issuer_request_id: view_issuer_id,
            error: view_error,
            failed_at: view_failed_at,
            ..
        } = view
        else {
            panic!("Expected MintingFailed variant, got {view:?}");
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_error, error_message);
        assert_eq!(view_failed_at, failed_at);
    }

    #[test]
    fn test_handle_initiated_with_wrong_event_type_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        let wrong_event = MintEvent::JournalConfirmed {
            issuer_request_id: IssuerRequestId::new("iss-123"),
            confirmed_at: Utc::now(),
        };

        view.handle_initiated(&wrong_event);

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_journal_confirmed_from_wrong_state_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        view.handle_journal_confirmed(Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_journal_confirmed_from_journal_confirmed_state_does_not_change()
     {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut view = MintView::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };
        let original_view = view.clone();

        view.handle_journal_confirmed(Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_journal_rejected_from_wrong_state_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        view.handle_journal_rejected("some error".to_string(), Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_journal_rejected_from_journal_confirmed_state_does_not_change()
     {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut view = MintView::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };
        let original_view = view.clone();

        view.handle_journal_rejected("some error".to_string(), Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_tokens_minted_from_wrong_state_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);

        view.handle_tokens_minted(
            tx_hash,
            receipt_id,
            shares_minted,
            50000,
            1000,
            Utc::now(),
        );

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_tokens_minted_from_initiated_state_does_not_change() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };
        let original_view = view.clone();

        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);

        view.handle_tokens_minted(
            tx_hash,
            receipt_id,
            shares_minted,
            50000,
            1000,
            Utc::now(),
        );

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_minting_failed_from_wrong_state_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        view.handle_minting_failed("error".to_string(), Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_minting_failed_from_initiated_state_does_not_change() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let mut view = MintView::Initiated {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
        };
        let original_view = view.clone();

        view.handle_minting_failed("error".to_string(), Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_view_update_from_mint_completed_event() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );
        let receipt_id = uint!(1_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let gas_used = 50000;
        let block_number = 1000;
        let minted_at = Utc::now();

        let mut view = MintView::CallbackPending {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            tx_hash,
            receipt_id,
            shares_minted,
            gas_used,
            block_number,
            minted_at,
        };

        let completed_at = Utc::now();

        let event = MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: issuer_request_id.0.clone(),
            sequence: 4,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let MintView::Completed {
            issuer_request_id: view_issuer_id,
            tx_hash: view_tx_hash,
            receipt_id: view_receipt_id,
            shares_minted: view_shares_minted,
            gas_used: view_gas_used,
            block_number: view_block_number,
            minted_at: view_minted_at,
            completed_at: view_completed_at,
            ..
        } = view
        else {
            panic!("Expected Completed variant, got {view:?}");
        };

        assert_eq!(view_issuer_id, issuer_request_id);
        assert_eq!(view_tx_hash, tx_hash);
        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_shares_minted, shares_minted);
        assert_eq!(view_gas_used, gas_used);
        assert_eq!(view_block_number, block_number);
        assert_eq!(view_minted_at, minted_at);
        assert_eq!(view_completed_at, completed_at);
    }

    #[test]
    fn test_handle_mint_completed_from_wrong_state_does_not_change_state() {
        let mut view = MintView::NotFound;
        let original_view = view.clone();

        view.handle_mint_completed(Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_mint_completed_from_journal_confirmed_state_does_not_change()
    {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();

        let mut view = MintView::JournalConfirmed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
        };
        let original_view = view.clone();

        view.handle_mint_completed(Utc::now());

        assert_eq!(view, original_view);
    }

    #[test]
    fn test_handle_mint_completed_from_minting_failed_state_does_not_change() {
        let issuer_request_id = IssuerRequestId::new("iss-123");
        let tokenization_request_id = TokenizationRequestId::new("alp-456");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId("client-789".to_string());
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let error = "Transaction failed: insufficient gas".to_string();
        let failed_at = Utc::now();

        let mut view = MintView::MintingFailed {
            issuer_request_id,
            tokenization_request_id,
            quantity,
            underlying,
            token,
            network,
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error,
            failed_at,
        };
        let original_view = view.clone();

        view.handle_mint_completed(Utc::now());

        assert_eq!(view, original_view);
    }
}
