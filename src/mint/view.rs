use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use uuid::Uuid;

use super::{
    ClientId, IssuerMintRequestId, Network, Quantity, TokenSymbol,
    TokenizationRequestId, UnderlyingSymbol,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("UUID parse error: {0}")]
    Uuid(#[from] uuid::Error),
}

/// Read model for a mint, projected from the `mint_view` table.
///
/// `mint_view` stores the event-sorcery `Lifecycle<Mint>` payload
/// (`{"Live": {...}}`). The `find_*` queries extract the `$.Live` sub-object —
/// which is one of the live `Mint` variants — and deserialize it into this
/// enum, which mirrors those variants field-for-field. `NotFound` is the
/// query-miss sentinel and is never a deserialize target.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) enum MintView {
    #[default]
    NotFound,
    Initiated {
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
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
    FireblocksSubmitted {
        issuer_request_id: IssuerMintRequestId,
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
        external_tx_id: String,
        fireblocks_tx_id: String,
    },
    CallbackPending {
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
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
        issuer_request_id: IssuerMintRequestId,
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
    Closed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        closed_at: DateTime<Utc>,
    },
}

impl MintView {
    #[cfg(test)]
    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::FireblocksSubmitted { .. } => "FireblocksSubmitted",
            Self::CallbackPending { .. } => "CallbackPending",
            Self::MintingFailed { .. } => "MintingFailed",
            Self::Completed { .. } => "Completed",
            Self::Closed { .. } => "Closed",
        }
    }
}

pub(crate) async fn find_by_issuer_request_id(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerMintRequestId,
) -> Result<Option<MintView>, MintViewError> {
    let issuer_request_id_str = issuer_request_id.to_string();
    let row = sqlx::query!(
        r#"
        SELECT json_extract(payload, '$.Live') as "live: String"
        FROM mint_view
        WHERE view_id = ?
        "#,
        issuer_request_id_str
    )
    .fetch_optional(pool)
    .await?;

    let Some(live) = row.and_then(|row| row.live) else {
        return Ok(None);
    };

    let view: MintView = serde_json::from_str(&live)?;

    Ok(Some(view))
}

/// Finds all mints that need recovery (not in terminal states).
///
/// Returns mints in JournalConfirmed, Minting, MintingFailed, or CallbackPending states.
pub(crate) async fn find_all_recoverable_mints(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerMintRequestId, MintView)>, MintViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            view_id as "view_id!: String",
            json_extract(payload, '$.Live') as "live: String"
        FROM mint_view
        WHERE json_extract(payload, '$.Live.JournalConfirmed') IS NOT NULL
           OR json_extract(payload, '$.Live.Minting') IS NOT NULL
           OR json_extract(payload, '$.Live.FireblocksSubmitted') IS NOT NULL
           OR json_extract(payload, '$.Live.MintingFailed') IS NOT NULL
           OR json_extract(payload, '$.Live.CallbackPending') IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .filter_map(|row| row.live.map(|live| (row.view_id, live)))
        .map(|(view_id, live)| {
            let view: MintView = serde_json::from_str(&live)?;
            let id = Uuid::parse_str(&view_id)?;
            Ok((IssuerMintRequestId::new(id), view))
        })
        .collect()
}

/// Finds all mints that are not in a terminal state.
///
/// Returns every mint whose view sits in `Initiated`, `JournalConfirmed`,
/// `JournalRejected`, `Minting`, `CallbackPending`, or `MintingFailed` — i.e.
/// anything that hasn't reached `Completed` or `Closed`. Callers
/// (`/admin/stuck`) apply the staleness gate and decide which entries the
/// operator must act on. Differs from `find_all_recoverable_mints`, which is
/// limited to the states the automated recovery loop knows how to drive.
pub(crate) async fn find_stuck(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerMintRequestId, MintView)>, MintViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT
            view_id as "view_id!: String",
            json_extract(payload, '$.Live') as "live: String"
        FROM mint_view
        WHERE json_extract(payload, '$.Live.Initiated')           IS NOT NULL
           OR json_extract(payload, '$.Live.JournalConfirmed')    IS NOT NULL
           OR json_extract(payload, '$.Live.JournalRejected')     IS NOT NULL
           OR json_extract(payload, '$.Live.Minting')             IS NOT NULL
           OR json_extract(payload, '$.Live.FireblocksSubmitted') IS NOT NULL
           OR json_extract(payload, '$.Live.CallbackPending')     IS NOT NULL
           OR json_extract(payload, '$.Live.MintingFailed')       IS NOT NULL
        "#
    )
    .fetch_all(pool)
    .await?;

    rows.into_iter()
        .filter_map(|row| row.live.map(|live| (row.view_id, live)))
        .map(|(view_id, live)| {
            let view: MintView = serde_json::from_str(&live)?;
            let id = Uuid::parse_str(&view_id)?;
            Ok((IssuerMintRequestId::new(id), view))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use event_sorcery::{StoreBuilder, test_store};
    use rust_decimal::Decimal;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::sync::Arc;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::{Mint, MintCommand, MintServices};
    use crate::receipt_inventory::{CqrsReceiptService, ReceiptInventory};
    use crate::vault::mock::MockVaultService;

    /// Inserts a `Lifecycle<Mint>`-shaped row into `mint_view` for a given live
    /// `Mint` variant. The projection stores `{"Live": {"<Variant>": {...}}}`,
    /// which the `find_*` query helpers read via `$.Live`; serializing a
    /// `MintView` (which mirrors the live `Mint` variants field-for-field)
    /// produces the `{"<Variant>": {...}}` body, so wrapping it under `Live`
    /// reproduces exactly what the running projection would write.
    async fn insert_mint_view(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        view: &MintView,
    ) {
        let view_id = issuer_request_id.to_string();
        let live = serde_json::to_value(view).unwrap();
        let payload =
            serde_json::to_string(&serde_json::json!({ "Live": live }))
                .unwrap();
        sqlx::query!(
            "INSERT INTO mint_view (view_id, version, payload) VALUES (?, 1, ?)",
            view_id,
            payload,
        )
        .execute(pool)
        .await
        .unwrap();
    }

    fn mint_services(pool: Pool<Sqlite>) -> MintServices {
        let receipt_store =
            Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));

        MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(receipt_store)),
            pool,
            bot: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        }
    }

    struct TestMintFields {
        issuer_request_id: IssuerMintRequestId,
        tokenization_request_id: TokenizationRequestId,
        quantity: Quantity,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        client_id: ClientId,
        wallet: Address,
        initiated_at: DateTime<Utc>,
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

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_view() {
        let pool = setup_test_db().await;

        let (store, _projection) = StoreBuilder::<Mint>::new(pool.clone())
            .build(mint_services(pool.clone()))
            .await
            .expect("Failed to build mint store");

        let issuer_request_id = IssuerMintRequestId::random();

        store
            .send(
                &issuer_request_id,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-888",
                    ),
                    quantity: Quantity::new(Decimal::from(50)),
                    underlying: UnderlyingSymbol::new("TSLA"),
                    token: TokenSymbol::new("tTSLA"),
                    network: Network::Base,
                    client_id: ClientId::new(),
                    wallet: address!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                },
            )
            .await
            .expect("Failed to initiate mint");

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
        assert_eq!(found_network, Network::Base);
    }

    #[tokio::test]
    async fn test_find_by_issuer_request_id_returns_none_when_not_found() {
        let pool = setup_test_db().await;

        let issuer_request_id = IssuerMintRequestId::random();

        let result = find_by_issuer_request_id(&pool, &issuer_request_id)
            .await
            .expect("Query should succeed");

        assert!(result.is_none());
    }

    fn test_mint_fields() -> TestMintFields {
        TestMintFields {
            issuer_request_id: IssuerMintRequestId::random(),
            tokenization_request_id: TokenizationRequestId::new("alp-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
            initiated_at: Utc::now(),
        }
    }

    /// Seeds one view per recoverable state. `find_all_recoverable_mints` and
    /// `find_stuck` both match `FireblocksSubmitted`, so it is seeded here
    /// alongside the other recoverable states.
    async fn seed_recoverable_mint_views(
        pool: &Pool<Sqlite>,
    ) -> Vec<IssuerMintRequestId> {
        let now = Utc::now();
        let fields: Vec<_> = (0..5).map(|_| test_mint_fields()).collect();

        let views: Vec<MintView> = vec![
            MintView::JournalConfirmed {
                issuer_request_id: fields[0].issuer_request_id.clone(),
                tokenization_request_id: fields[0]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[0].quantity.clone(),
                underlying: fields[0].underlying.clone(),
                token: fields[0].token.clone(),
                network: fields[0].network.clone(),
                client_id: fields[0].client_id,
                wallet: fields[0].wallet,
                initiated_at: fields[0].initiated_at,
                journal_confirmed_at: now,
            },
            MintView::Minting {
                issuer_request_id: fields[1].issuer_request_id.clone(),
                tokenization_request_id: fields[1]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[1].quantity.clone(),
                underlying: fields[1].underlying.clone(),
                token: fields[1].token.clone(),
                network: fields[1].network.clone(),
                client_id: fields[1].client_id,
                wallet: fields[1].wallet,
                initiated_at: fields[1].initiated_at,
                journal_confirmed_at: now,
                minting_started_at: now,
            },
            MintView::FireblocksSubmitted {
                issuer_request_id: fields[2].issuer_request_id.clone(),
                tokenization_request_id: fields[2]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[2].quantity.clone(),
                underlying: fields[2].underlying.clone(),
                token: fields[2].token.clone(),
                network: fields[2].network.clone(),
                client_id: fields[2].client_id,
                wallet: fields[2].wallet,
                initiated_at: fields[2].initiated_at,
                journal_confirmed_at: now,
                minting_started_at: now,
                external_tx_id: "mint-base".to_string(),
                fireblocks_tx_id: "fb-1".to_string(),
            },
            MintView::MintingFailed {
                issuer_request_id: fields[3].issuer_request_id.clone(),
                tokenization_request_id: fields[3]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[3].quantity.clone(),
                underlying: fields[3].underlying.clone(),
                token: fields[3].token.clone(),
                network: fields[3].network.clone(),
                client_id: fields[3].client_id,
                wallet: fields[3].wallet,
                initiated_at: fields[3].initiated_at,
                journal_confirmed_at: now,
                error: "Transaction reverted".to_string(),
                failed_at: now,
            },
            MintView::CallbackPending {
                issuer_request_id: fields[4].issuer_request_id.clone(),
                tokenization_request_id: fields[4]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[4].quantity.clone(),
                underlying: fields[4].underlying.clone(),
                token: fields[4].token.clone(),
                network: fields[4].network.clone(),
                client_id: fields[4].client_id,
                wallet: fields[4].wallet,
                initiated_at: fields[4].initiated_at,
                journal_confirmed_at: now,
                tx_hash: b256!(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ),
                receipt_id: uint!(1_U256),
                shares_minted: uint!(100_000000000000000000_U256),
                gas_used: Some(50000),
                block_number: 1000,
                minted_at: now,
            },
        ];

        for (mint_fields, view) in fields.iter().zip(&views) {
            insert_mint_view(pool, &mint_fields.issuer_request_id, view).await;
        }

        fields.into_iter().map(|f| f.issuer_request_id).collect()
    }

    async fn seed_non_recoverable_mint_views(pool: &Pool<Sqlite>) {
        let now = Utc::now();

        let fields = test_mint_fields();
        insert_mint_view(
            pool,
            &fields.issuer_request_id,
            &MintView::Initiated {
                issuer_request_id: fields.issuer_request_id.clone(),
                tokenization_request_id: fields.tokenization_request_id,
                quantity: fields.quantity,
                underlying: fields.underlying,
                token: fields.token,
                network: fields.network,
                client_id: fields.client_id,
                wallet: fields.wallet,
                initiated_at: fields.initiated_at,
            },
        )
        .await;

        let fields = test_mint_fields();
        insert_mint_view(pool, &fields.issuer_request_id, &MintView::Completed {
            issuer_request_id: fields.issuer_request_id.clone(),
            tokenization_request_id: fields.tokenization_request_id,
            quantity: fields.quantity,
            underlying: fields.underlying,
            token: fields.token,
            network: fields.network,
            client_id: fields.client_id,
            wallet: fields.wallet,
            initiated_at: fields.initiated_at,
            journal_confirmed_at: now,
            tx_hash: b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
            receipt_id: uint!(2_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: Some(50000),
            block_number: 1001,
            minted_at: now,
            completed_at: now,
        }).await;
    }

    #[tokio::test]
    async fn test_find_all_recoverable_mints_returns_all_recoverable_states() {
        let pool = setup_test_db().await;
        let recoverable_ids = seed_recoverable_mint_views(&pool).await;
        seed_non_recoverable_mint_views(&pool).await;

        let results = find_all_recoverable_mints(&pool).await.unwrap();

        assert_eq!(results.len(), 5, "Expected 5 recoverable mints");

        let result_ids: Vec<_> =
            results.iter().map(|(id, _)| id.clone()).collect();
        for id in &recoverable_ids {
            assert!(
                result_ids.contains(id),
                "Should include recoverable mint {id}"
            );
        }

        let state_names: Vec<_> =
            results.iter().map(|(_, view)| view.state_name()).collect();
        assert!(state_names.contains(&"JournalConfirmed"));
        assert!(state_names.contains(&"Minting"));
        assert!(state_names.contains(&"FireblocksSubmitted"));
        assert!(state_names.contains(&"MintingFailed"));
        assert!(state_names.contains(&"CallbackPending"));
    }

    #[tokio::test]
    async fn test_find_all_recoverable_mints_returns_empty_when_none() {
        let pool = setup_test_db().await;
        let results = find_all_recoverable_mints(&pool).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_find_stuck_returns_all_non_terminal_variants() {
        let pool = setup_test_db().await;
        let now = Utc::now();

        // Seed the 5 recoverable variants (JournalConfirmed, Minting,
        // FireblocksSubmitted, MintingFailed, CallbackPending).
        let recoverable_ids = seed_recoverable_mint_views(&pool).await;

        // Seed Initiated and JournalRejected (non-terminal-but-not-recoverable).
        let initiated_fields = test_mint_fields();
        let initiated_id = initiated_fields.issuer_request_id.clone();
        insert_mint_view(
            &pool,
            &initiated_id,
            &MintView::Initiated {
                issuer_request_id: initiated_fields.issuer_request_id,
                tokenization_request_id: initiated_fields
                    .tokenization_request_id,
                quantity: initiated_fields.quantity,
                underlying: initiated_fields.underlying,
                token: initiated_fields.token,
                network: initiated_fields.network,
                client_id: initiated_fields.client_id,
                wallet: initiated_fields.wallet,
                initiated_at: initiated_fields.initiated_at,
            },
        )
        .await;

        let rejected_fields = test_mint_fields();
        let rejected_id = rejected_fields.issuer_request_id.clone();
        insert_mint_view(
            &pool,
            &rejected_id,
            &MintView::JournalRejected {
                issuer_request_id: rejected_fields.issuer_request_id,
                tokenization_request_id: rejected_fields
                    .tokenization_request_id,
                quantity: rejected_fields.quantity,
                underlying: rejected_fields.underlying,
                token: rejected_fields.token,
                network: rejected_fields.network,
                client_id: rejected_fields.client_id,
                wallet: rejected_fields.wallet,
                initiated_at: rejected_fields.initiated_at,
                reason: "Alpaca rejected the journal".to_string(),
                rejected_at: now,
            },
        )
        .await;

        // Seed a Completed mint that must NOT appear.
        let completed_fields = test_mint_fields();
        let completed_id = completed_fields.issuer_request_id.clone();
        insert_mint_view(&pool, &completed_id, &MintView::Completed {
            issuer_request_id: completed_fields.issuer_request_id,
            tokenization_request_id: completed_fields.tokenization_request_id,
            quantity: completed_fields.quantity,
            underlying: completed_fields.underlying,
            token: completed_fields.token,
            network: completed_fields.network,
            client_id: completed_fields.client_id,
            wallet: completed_fields.wallet,
            initiated_at: completed_fields.initiated_at,
            journal_confirmed_at: now,
            tx_hash: b256!("0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"),
            receipt_id: uint!(2_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            gas_used: Some(50000),
            block_number: 1001,
            minted_at: now,
            completed_at: now,
        }).await;

        // Seed a Closed mint that must NOT appear.
        let closed_fields = test_mint_fields();
        let closed_id = closed_fields.issuer_request_id.clone();
        insert_mint_view(
            &pool,
            &closed_id,
            &MintView::Closed {
                issuer_request_id: closed_fields.issuer_request_id,
                reason: "closed by admin".to_string(),
                closed_at: now,
            },
        )
        .await;

        let results = find_stuck(&pool).await.unwrap();
        let result_ids: Vec<_> =
            results.iter().map(|(id, _)| id.clone()).collect();

        assert_eq!(
            results.len(),
            7,
            "Expected 7 non-terminal mints, got ids: {result_ids:?}"
        );
        for id in &recoverable_ids {
            assert!(result_ids.contains(id), "Should include {id}");
        }
        assert!(result_ids.contains(&initiated_id), "Should include Initiated");
        assert!(
            result_ids.contains(&rejected_id),
            "Should include JournalRejected"
        );
        assert!(
            !result_ids.contains(&completed_id),
            "Completed must be filtered out"
        );
        assert!(
            !result_ids.contains(&closed_id),
            "Closed must be filtered out"
        );

        let state_names: Vec<_> =
            results.iter().map(|(_, view)| view.state_name()).collect();
        assert!(state_names.contains(&"Initiated"));
        assert!(state_names.contains(&"JournalConfirmed"));
        assert!(state_names.contains(&"JournalRejected"));
        assert!(state_names.contains(&"Minting"));
        assert!(state_names.contains(&"FireblocksSubmitted"));
        assert!(state_names.contains(&"MintingFailed"));
        assert!(state_names.contains(&"CallbackPending"));
    }

    #[tokio::test]
    async fn test_find_stuck_returns_empty_when_none() {
        let pool = setup_test_db().await;
        let results = find_stuck(&pool).await.unwrap();
        assert!(results.is_empty());
    }
}
