use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use cqrs_es::persist::{GenericQuery, QueryReplay};
use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlite_es::{SqliteEventRepository, SqliteViewRepository};
use sqlx::{Pool, Sqlite};
use std::sync::Arc;

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

    #[error("Replay error: {0}")]
    Replay(#[from] cqrs_es::AggregateError<super::MintError>),
}

/// Replays all `Mint` events through the `mint_view`.
///
/// Uses `QueryReplay` to re-project the view from existing events in the event store.
/// This is used at startup to ensure the view is in sync with events after manual
/// event store modifications or schema changes.
pub async fn replay_mint_view(pool: Pool<Sqlite>) -> Result<(), MintViewError> {
    let view_repo = Arc::new(SqliteViewRepository::<MintView, Mint>::new(
        pool.clone(),
        "mint_view".to_string(),
    ));
    let query = GenericQuery::new(view_repo);

    let event_repo = SqliteEventRepository::new(pool);
    let replay = QueryReplay::new(event_repo, query);
    replay.replay_all().await?;

    Ok(())
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
    Minting {
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
        minting_started_at: DateTime<Utc>,
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
        gas_used: Option<u64>,
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
        gas_used: Option<u64>,
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
    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::CallbackPending { .. } => "CallbackPending",
            Self::MintingFailed { .. } => "MintingFailed",
            Self::Completed { .. } => "Completed",
        }
    }

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
            client_id: *client_id,
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

    fn handle_minting_started(&mut self, started_at: DateTime<Utc>) {
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

        *self = Self::Minting {
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
            minting_started_at: started_at,
        };
    }

    fn handle_tokens_minted(
        &mut self,
        tx_hash: B256,
        receipt_id: U256,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
    ) {
        let Self::Minting {
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
            ..
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
        let Self::Minting {
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
            ..
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

    fn handle_mint_retry_started(&mut self, started_at: DateTime<Utc>) {
        let Self::MintingFailed {
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
            ..
        } = self.clone()
        else {
            return;
        };

        *self = Self::Minting {
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
            minting_started_at: started_at,
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
            MintEvent::MintingStarted { started_at, .. } => {
                self.handle_minting_started(*started_at);
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
                    Some(*gas_used),
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
            MintEvent::ExistingMintRecovered {
                tx_hash,
                receipt_id,
                shares_minted,
                block_number,
                recovered_at,
                ..
            } => {
                self.handle_tokens_minted(
                    *tx_hash,
                    *receipt_id,
                    *shares_minted,
                    None,
                    *block_number,
                    *recovered_at,
                );
            }
            MintEvent::MintRetryStarted { started_at, .. } => {
                self.handle_mint_retry_started(*started_at);
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

/// Finds all mints in JournalConfirmed state (stuck after journal confirmation but before minting).
pub(crate) async fn find_journal_confirmed(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, MintView)>, MintViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload: String"
        FROM mint_view
        WHERE json_extract(payload, '$.JournalConfirmed') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: MintView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(row.view_id), view))
        })
        .collect()
}

/// Finds all mints in Minting or MintingFailed state (incomplete mints that need recovery).
pub(crate) async fn find_incomplete_mints(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, MintView)>, MintViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload: String"
        FROM mint_view
        WHERE json_extract(payload, '$.Minting') IS NOT NULL
           OR json_extract(payload, '$.MintingFailed') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: MintView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(row.view_id), view))
        })
        .collect()
}

/// Finds all mints in CallbackPending state (stuck after minting but before callback).
pub(crate) async fn find_callback_pending(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerRequestId, MintView)>, MintViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT view_id as "view_id!: String", payload as "payload: String"
        FROM mint_view
        WHERE json_extract(payload, '$.CallbackPending') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let view: MintView = serde_json::from_str(&row.payload)?;
            Ok((IssuerRequestId::new(row.view_id), view))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use cqrs_es::EventEnvelope;
    use cqrs_es::persist::GenericQuery;
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteCqrs, SqliteViewRepository, sqlite_cqrs};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::mint::{Mint, MintCommand, MintEvent};

    struct TestHarness {
        pool: Pool<Sqlite>,
        cqrs: SqliteCqrs<Mint>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect(":memory:")
                .await
                .expect("Failed to create in-memory database");

            sqlx::migrate!("./migrations")
                .run(&pool)
                .await
                .expect("Failed to run migrations");

            let view_repo =
                Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                    pool.clone(),
                    "mint_view".to_string(),
                ));
            let query = GenericQuery::new(view_repo);
            let cqrs = sqlite_cqrs(pool.clone(), vec![Box::new(query)], ());

            Self { pool, cqrs }
        }

        async fn initiate_mint(&self, issuer_request_id: &IssuerRequestId) {
            self.cqrs
                .execute(
                    &issuer_request_id.0,
                    MintCommand::Initiate {
                        issuer_request_id: issuer_request_id.clone(),
                        tokenization_request_id: TokenizationRequestId::new(
                            "alp-888",
                        ),
                        quantity: Quantity::new(Decimal::from(50)),
                        underlying: UnderlyingSymbol::new("TSLA"),
                        token: TokenSymbol::new("tTSLA"),
                        network: Network::new("base"),
                        client_id: ClientId::new(),
                        wallet: address!(
                            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                        ),
                    },
                )
                .await
                .expect("Failed to initiate mint");
        }
    }

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
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();

        let event = MintEvent::Initiated {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
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
        let harness = TestHarness::new().await;
        let TestHarness { pool, .. } = &harness;

        let issuer_request_id = IssuerRequestId::new("iss-999");

        harness.initiate_mint(&issuer_request_id).await;

        let result = find_by_issuer_request_id(pool, &issuer_request_id)
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
            ..
        } = result.unwrap()
        else {
            panic!("Expected Initiated variant")
        };

        assert_eq!(found_issuer_id, issuer_request_id);
        assert_eq!(
            found_tokenization_id,
            TokenizationRequestId::new("alp-888")
        );
        assert_eq!(found_quantity, Quantity::new(Decimal::from(50)));
        assert_eq!(found_underlying, UnderlyingSymbol::new("TSLA"));
        assert_eq!(found_token, TokenSymbol::new("tTSLA"));
        assert_eq!(found_network, Network::new("base"));
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
        let client_id = ClientId::new();
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
        let client_id = ClientId::new();
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
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut view = MintView::Minting {
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
            minting_started_at,
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
        assert_eq!(view_gas_used, Some(gas_used));
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
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut view = MintView::Minting {
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
            minting_started_at,
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
        let client_id = ClientId::new();
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
        let client_id = ClientId::new();
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
            Some(50000),
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
        let client_id = ClientId::new();
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
            Some(50000),
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
        let client_id = ClientId::new();
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
        let client_id = ClientId::new();
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
            gas_used: Some(gas_used),
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
        assert_eq!(view_gas_used, Some(gas_used));
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
        let client_id = ClientId::new();
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
        let client_id = ClientId::new();
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

    #[tokio::test]
    async fn test_find_journal_confirmed_returns_matching_mints() {
        let pool = setup_test_db().await;

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let iss_1 = IssuerRequestId::new("iss-jc-1");
        let iss_2 = IssuerRequestId::new("iss-jc-2");

        let init_cmd_1 = MintCommand::Initiate {
            issuer_request_id: iss_1.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-jc-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            client_id,
            wallet,
        };

        mint_cqrs.execute(&iss_1.0, init_cmd_1).await.unwrap();

        let confirm_cmd =
            MintCommand::ConfirmJournal { issuer_request_id: iss_1.clone() };

        mint_cqrs.execute(&iss_1.0, confirm_cmd).await.unwrap();

        let init_cmd_2 = MintCommand::Initiate {
            issuer_request_id: iss_2.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-jc-2"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            network: Network::new("base"),
            client_id,
            wallet,
        };

        mint_cqrs.execute(&iss_2.0, init_cmd_2).await.unwrap();

        let results = find_journal_confirmed(&pool).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.0, "iss-jc-1");
        assert!(matches!(results[0].1, MintView::JournalConfirmed { .. }));
    }

    #[tokio::test]
    async fn test_find_journal_confirmed_returns_empty_when_none() {
        let pool = setup_test_db().await;

        let results = find_journal_confirmed(&pool).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_find_callback_pending_returns_matching_mints() {
        let pool = setup_test_db().await;

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let tx_hash = b256!(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        );

        let iss_1 = IssuerRequestId::new("iss-cp-1");
        let iss_2 = IssuerRequestId::new("iss-cp-2");

        let init_cmd_1 = MintCommand::Initiate {
            issuer_request_id: iss_1.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-cp-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            client_id,
            wallet,
        };

        mint_cqrs.execute(&iss_1.0, init_cmd_1).await.unwrap();

        let confirm_cmd =
            MintCommand::ConfirmJournal { issuer_request_id: iss_1.clone() };

        mint_cqrs.execute(&iss_1.0, confirm_cmd).await.unwrap();

        let start_minting_cmd =
            MintCommand::StartMinting { issuer_request_id: iss_1.clone() };

        mint_cqrs.execute(&iss_1.0, start_minting_cmd).await.unwrap();

        let mint_success_cmd = MintCommand::CompleteMinting {
            issuer_request_id: iss_1.clone(),
            tx_hash,
            receipt_id: uint!(1_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: 50000,
            block_number: 1000,
        };

        mint_cqrs.execute(&iss_1.0, mint_success_cmd).await.unwrap();

        let init_cmd_2 = MintCommand::Initiate {
            issuer_request_id: iss_2.clone(),
            tokenization_request_id: TokenizationRequestId::new("alp-cp-2"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            network: Network::new("base"),
            client_id,
            wallet,
        };

        mint_cqrs.execute(&iss_2.0, init_cmd_2).await.unwrap();

        let confirm_cmd_2 =
            MintCommand::ConfirmJournal { issuer_request_id: iss_2.clone() };

        mint_cqrs.execute(&iss_2.0, confirm_cmd_2).await.unwrap();

        let results = find_callback_pending(&pool).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.0, "iss-cp-1");
        assert!(matches!(results[0].1, MintView::CallbackPending { .. }));
    }

    #[tokio::test]
    async fn test_find_callback_pending_returns_empty_when_none() {
        let pool = setup_test_db().await;

        let results = find_callback_pending(&pool).await.unwrap();

        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_find_journal_confirmed_returns_multiple_when_multiple_stuck()
    {
        let pool = setup_test_db().await;

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        for i in 1..=3 {
            let iss = IssuerRequestId::new(format!("iss-multi-{i}"));

            let init_cmd = MintCommand::Initiate {
                issuer_request_id: iss.clone(),
                tokenization_request_id: TokenizationRequestId::new(format!(
                    "alp-multi-{i}"
                )),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                client_id,
                wallet,
            };

            mint_cqrs.execute(&iss.0, init_cmd).await.unwrap();

            let confirm_cmd =
                MintCommand::ConfirmJournal { issuer_request_id: iss.clone() };

            mint_cqrs.execute(&iss.0, confirm_cmd).await.unwrap();
        }

        let results = find_journal_confirmed(&pool).await.unwrap();

        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_find_incomplete_mints_returns_minting_and_minting_failed() {
        let pool = setup_test_db().await;

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);
        let mint_cqrs =
            Arc::new(sqlite_cqrs(pool.clone(), vec![Box::new(mint_query)], ()));

        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        // Create a mint in Minting state
        let iss_minting = IssuerRequestId::new("iss-minting-1");
        mint_cqrs
            .execute(
                &iss_minting.0,
                MintCommand::Initiate {
                    issuer_request_id: iss_minting.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-minting-1",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    client_id,
                    wallet,
                },
            )
            .await
            .unwrap();
        mint_cqrs
            .execute(
                &iss_minting.0,
                MintCommand::ConfirmJournal {
                    issuer_request_id: iss_minting.clone(),
                },
            )
            .await
            .unwrap();
        mint_cqrs
            .execute(
                &iss_minting.0,
                MintCommand::StartMinting {
                    issuer_request_id: iss_minting.clone(),
                },
            )
            .await
            .unwrap();

        // Create a mint in MintingFailed state
        let iss_failed = IssuerRequestId::new("iss-failed-1");
        mint_cqrs
            .execute(
                &iss_failed.0,
                MintCommand::Initiate {
                    issuer_request_id: iss_failed.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-failed-1",
                    ),
                    quantity: Quantity::new(Decimal::from(50)),
                    underlying: UnderlyingSymbol::new("TSLA"),
                    token: TokenSymbol::new("tTSLA"),
                    network: Network::new("base"),
                    client_id,
                    wallet,
                },
            )
            .await
            .unwrap();
        mint_cqrs
            .execute(
                &iss_failed.0,
                MintCommand::ConfirmJournal {
                    issuer_request_id: iss_failed.clone(),
                },
            )
            .await
            .unwrap();
        mint_cqrs
            .execute(
                &iss_failed.0,
                MintCommand::StartMinting {
                    issuer_request_id: iss_failed.clone(),
                },
            )
            .await
            .unwrap();
        mint_cqrs
            .execute(
                &iss_failed.0,
                MintCommand::FailMinting {
                    issuer_request_id: iss_failed.clone(),
                    error: "Transaction reverted".to_string(),
                },
            )
            .await
            .unwrap();

        let results = find_incomplete_mints(&pool).await.unwrap();

        assert_eq!(results.len(), 2);

        let ids: Vec<_> = results.iter().map(|(id, _)| id.0.as_str()).collect();
        assert!(ids.contains(&"iss-minting-1"));
        assert!(ids.contains(&"iss-failed-1"));
    }

    #[tokio::test]
    async fn test_find_incomplete_mints_returns_empty_when_none() {
        let pool = setup_test_db().await;
        let results = find_incomplete_mints(&pool).await.unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_handle_mint_retry_started_transitions_minting_failed_to_minting() {
        let issuer_request_id = IssuerRequestId::new("iss-retry-1");
        let tokenization_request_id = TokenizationRequestId::new("alp-retry-1");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let failed_at = Utc::now();

        let mut view = MintView::MintingFailed {
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            quantity: quantity.clone(),
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            client_id,
            wallet,
            initiated_at,
            journal_confirmed_at,
            error: "Previous failure".to_string(),
            failed_at,
        };

        let retry_started_at = Utc::now();
        view.handle_mint_retry_started(retry_started_at);

        let MintView::Minting {
            issuer_request_id: view_iss,
            tokenization_request_id: view_tok,
            quantity: view_qty,
            underlying: view_und,
            token: view_tok_sym,
            network: view_net,
            client_id: view_client,
            wallet: view_wallet,
            initiated_at: view_init,
            journal_confirmed_at: view_jc,
            minting_started_at,
        } = view
        else {
            panic!("Expected Minting state, got {view:?}");
        };

        assert_eq!(view_iss, issuer_request_id);
        assert_eq!(view_tok, tokenization_request_id);
        assert_eq!(view_qty, quantity);
        assert_eq!(view_und, underlying);
        assert_eq!(view_tok_sym, token);
        assert_eq!(view_net, network);
        assert_eq!(view_client, client_id);
        assert_eq!(view_wallet, wallet);
        assert_eq!(view_init, initiated_at);
        assert_eq!(view_jc, journal_confirmed_at);
        assert_eq!(minting_started_at, retry_started_at);
    }

    #[test]
    fn test_handle_mint_retry_started_from_minting_state_does_not_change() {
        let issuer_request_id = IssuerRequestId::new("iss-retry-2");
        let tokenization_request_id = TokenizationRequestId::new("alp-retry-2");
        let quantity = Quantity::new(Decimal::from(100));
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let client_id = ClientId::new();
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");
        let initiated_at = Utc::now();
        let journal_confirmed_at = Utc::now();
        let minting_started_at = Utc::now();

        let mut view = MintView::Minting {
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
            minting_started_at,
        };
        let original_view = view.clone();

        view.handle_mint_retry_started(Utc::now());

        assert_eq!(view, original_view);
    }
}
