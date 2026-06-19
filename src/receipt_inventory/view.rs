use alloy::primitives::U256;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite, SqlitePool};
use tracing::{debug, warn};

use crate::mint::{IssuerMintRequestId, Mint, MintEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    #[error("Issuer request id parse error: {0}")]
    IssuerRequestIdParse(#[from] uuid::Error),
}

/// Rebuilds the `receipt_inventory_view` from the `Mint` event log.
///
/// event-sorcery reactors only observe newly committed events, so the historical
/// `Mint` streams are replayed through the reactor at startup. Preserves #167:
/// receipts activated through historical `MintEvent::ExistingMintRecovered`
/// events are rebuilt as `Active` rather than left stale in `Pending`.
pub(crate) async fn rebuild_receipt_inventory_view(
    pool: &Pool<Sqlite>,
) -> Result<(), ReceiptInventoryViewError> {
    debug!(target: "receipt", "Rebuilding receipt inventory view from events");

    sqlx::query!("DELETE FROM receipt_inventory_view").execute(pool).await?;

    let reactor = ReceiptInventoryViewReactor::new(pool.clone());

    let rows = sqlx::query!(
        r#"
        SELECT
            aggregate_id as "aggregate_id!: String",
            payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Mint'
        ORDER BY aggregate_id, sequence
        "#
    )
    .fetch_all(pool)
    .await?;

    for row in rows {
        let mint_id: IssuerMintRequestId = row.aggregate_id.parse()?;
        let event: MintEvent = serde_json::from_str(&row.payload)?;
        reactor.project(&mint_id, &event).await;
    }

    debug!(target: "receipt", "Receipt inventory view rebuild complete");

    Ok(())
}

/// Tracks the lifecycle of ERC-1155 receipt tokens from minting through burning.
///
/// This view accumulates data across multiple Mint events to track receipt state:
/// - `Unavailable`: No mint initiated yet (initial state)
/// - `Pending`: Mint initiated, waiting for on-chain minting (has underlying/token)
/// - `Active`: Tokens minted, receipt exists with available balance
/// - `Depleted`: Receipt fully burned (balance = 0)
///
/// `ReceiptInventoryViewReactor` projects this from the `Mint` event stream.
/// Currently only Mint events are handled; Redemption burn events will be added
/// in issue #25.
///
/// View ID: `issuer_request_id` (the Mint aggregate's aggregate_id)
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryView {
    #[default]
    Unavailable,
    Pending {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    },
    Active {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        current_balance: U256,
        minted_at: DateTime<Utc>,
    },
    Depleted {
        receipt_id: U256,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        initial_amount: U256,
        depleted_at: DateTime<Utc>,
    },
}

impl ReceiptInventoryView {
    fn with_initiated_data(
        self,
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
    ) -> Self {
        match self {
            Self::Unavailable => Self::Pending { underlying, token },
            other => other,
        }
    }

    fn with_tokens_minted(
        self,
        receipt_id: U256,
        shares_minted: U256,
        minted_at: DateTime<Utc>,
    ) -> Self {
        match self {
            Self::Pending { underlying, token } => Self::Active {
                receipt_id,
                underlying,
                token,
                initial_amount: shares_minted,
                current_balance: shares_minted,
                minted_at,
            },
            other => other,
        }
    }
}

deps!(ReceiptInventoryViewReactor, [Mint]);

/// Maintains the `receipt_inventory_view` table from the `Mint` event stream.
///
/// event-sorcery allows only one canonical `Table` projection per aggregate
/// (that is `mint_view`), so this secondary view of the Mint stream is kept up
/// to date by an explicit reactor rather than a second projection. It is
/// write-only today — nothing reads `receipt_inventory_view` in production; it
/// is retained for the planned dual-listening receipt lifecycle (issue #25).
pub(crate) struct ReceiptInventoryViewReactor {
    pool: SqlitePool,
}

impl ReceiptInventoryViewReactor {
    pub(crate) const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn project(&self, mint_id: &IssuerMintRequestId, event: &MintEvent) {
        // Only these events transition the view; skip the rest to avoid a
        // pointless read-write cycle.
        if !matches!(
            event,
            MintEvent::Initiated { .. }
                | MintEvent::TokensMinted { .. }
                | MintEvent::ExistingMintRecovered { .. }
        ) {
            return;
        }

        let view_id = mint_id.to_string();

        let current = match self.load(&view_id).await {
            Ok(view) => view,
            Err(error) => {
                warn!(target: "receipt", view_id, error = %error,
                    "Failed to load receipt inventory view; skipping update"
                );
                return;
            }
        };

        let updated = match event {
            MintEvent::Initiated { underlying, token, .. } => {
                current.with_initiated_data(underlying.clone(), token.clone())
            }
            MintEvent::TokensMinted {
                receipt_id,
                shares_minted,
                minted_at,
                ..
            } => current.with_tokens_minted(
                *receipt_id,
                *shares_minted,
                *minted_at,
            ),
            // Recovered mints (#167) must activate their receipt in inventory
            // too, otherwise recovery would leave the receipt inactive and
            // unspendable for redemption burn planning.
            MintEvent::ExistingMintRecovered {
                receipt_id,
                shares_minted,
                recovered_at,
                ..
            } => current.with_tokens_minted(
                *receipt_id,
                *shares_minted,
                *recovered_at,
            ),
            _ => return,
        };

        if let Err(error) = self.upsert(&view_id, &updated).await {
            warn!(target: "receipt", view_id, error = %error,
                "Failed to write receipt inventory view"
            );
        }
    }

    async fn load(
        &self,
        view_id: &str,
    ) -> Result<ReceiptInventoryView, ReceiptInventoryViewError> {
        let row = sqlx::query!(
            r#"
            SELECT payload as "payload!: String"
            FROM receipt_inventory_view
            WHERE view_id = ?
            "#,
            view_id
        )
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => Ok(serde_json::from_str(&row.payload)?),
            None => Ok(ReceiptInventoryView::Unavailable),
        }
    }

    async fn upsert(
        &self,
        view_id: &str,
        view: &ReceiptInventoryView,
    ) -> Result<(), ReceiptInventoryViewError> {
        let payload = serde_json::to_string(view)?;

        sqlx::query!(
            "
            INSERT INTO receipt_inventory_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ON CONFLICT(view_id) DO UPDATE SET
                version = version + 1,
                payload = ?
            ",
            view_id,
            payload,
            payload
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[async_trait]
impl Reactor for ReceiptInventoryViewReactor {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (mint_id, mint_event) = event.into_inner();
        self.project(&mint_id, &mint_event).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256, uint};
    use chrono::Utc;
    use event_sorcery::ReactorHarness;
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;

    use super::*;
    use crate::mint::{ClientId, Network, Quantity, TokenizationRequestId};

    async fn migrated_pool() -> SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
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

    /// Reads back the projected view for `mint_id`, deserializing the row the
    /// reactor wrote. Returns `Unavailable` when no row exists yet.
    async fn load_view(
        pool: &SqlitePool,
        mint_id: &IssuerMintRequestId,
    ) -> ReceiptInventoryView {
        let view_id = mint_id.to_string();
        let row = sqlx::query!(
            r#"
            SELECT payload as "payload!: String"
            FROM receipt_inventory_view
            WHERE view_id = ?
            "#,
            view_id
        )
        .fetch_optional(pool)
        .await
        .expect("Failed to query receipt inventory view");

        match row {
            Some(row) => serde_json::from_str(&row.payload)
                .expect("Failed to deserialize receipt inventory view"),
            None => ReceiptInventoryView::Unavailable,
        }
    }

    fn initiated_event(
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        tokenization_request_id: &str,
        wallet: Address,
    ) -> MintEvent {
        MintEvent::Initiated {
            issuer_request_id: IssuerMintRequestId::random(),
            tokenization_request_id: TokenizationRequestId::new(
                tokenization_request_id,
            ),
            quantity: Quantity::new(Decimal::from(100)),
            underlying,
            token,
            network: Network::Base,
            client_id: ClientId::new(),
            wallet,
            initiated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn test_unavailable_to_pending_on_mint_initiated() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let mint_id = IssuerMintRequestId::random();

        assert!(matches!(
            load_view(&pool, &mint_id).await,
            ReceiptInventoryView::Unavailable
        ));

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    underlying.clone(),
                    token.clone(),
                    "alp-456",
                    address!("0x1234567890abcdef1234567890abcdef12345678"),
                ),
            )
            .await
            .unwrap();

        let ReceiptInventoryView::Pending {
            underlying: view_underlying,
            token: view_token,
        } = load_view(&pool, &mint_id).await
        else {
            panic!("Expected Pending variant");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
    }

    #[tokio::test]
    async fn test_pending_to_active_on_tokens_minted() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let receipt_id = uint!(42_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let minted_at = Utc::now();
        let mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    underlying.clone(),
                    token.clone(),
                    "alp-456",
                    address!("0x1234567890abcdef1234567890abcdef12345678"),
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            load_view(&pool, &mint_id).await,
            ReceiptInventoryView::Pending { .. }
        ));

        harness
            .receive::<Mint>(
                mint_id.clone(),
                MintEvent::TokensMinted {
                    issuer_request_id: IssuerMintRequestId::random(),
                    tx_hash: b256!(
                        "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
                    ),
                    receipt_id,
                    shares_minted,
                    gas_used: 50000,
                    block_number: 1000,
                    minted_at,
                },
            )
            .await
            .unwrap();

        let ReceiptInventoryView::Active {
            receipt_id: view_receipt_id,
            underlying: view_underlying,
            token: view_token,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = load_view(&pool, &mint_id).await
        else {
            panic!("Expected Active variant");
        };

        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[tokio::test]
    async fn test_view_stores_underlying_and_token_from_initiated() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");
        let mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    underlying.clone(),
                    token.clone(),
                    "alp-999",
                    address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"),
                ),
            )
            .await
            .unwrap();

        let ReceiptInventoryView::Pending {
            underlying: stored_underlying,
            token: stored_token,
        } = load_view(&pool, &mint_id).await
        else {
            panic!("Expected Pending variant");
        };

        assert_eq!(stored_underlying, underlying);
        assert_eq!(stored_token, token);
    }

    #[tokio::test]
    async fn test_view_combines_initiated_data_with_tokens_minted() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let underlying = UnderlyingSymbol::new("AMD");
        let token = TokenSymbol::new("tAMD");
        let receipt_id = uint!(7_U256);
        let shares_minted = uint!(250_500000000000000000_U256);
        let minted_at = Utc::now();
        let mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    underlying.clone(),
                    token.clone(),
                    "alp-combo",
                    address!("0x3333333333333333333333333333333333333333"),
                ),
            )
            .await
            .unwrap();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                MintEvent::TokensMinted {
                    issuer_request_id: IssuerMintRequestId::random(),
                    tx_hash: b256!(
                        "0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
                    ),
                    receipt_id,
                    shares_minted,
                    gas_used: 75000,
                    block_number: 2000,
                    minted_at,
                },
            )
            .await
            .unwrap();

        let ReceiptInventoryView::Active {
            underlying: view_underlying,
            token: view_token,
            receipt_id: view_receipt_id,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = load_view(&pool, &mint_id).await
        else {
            panic!("Expected Active variant");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[tokio::test]
    async fn test_view_activates_receipt_on_existing_mint_recovered() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let underlying = UnderlyingSymbol::new("MSTR");
        let token = TokenSymbol::new("tMSTR");
        let receipt_id = uint!(136_U256);
        let shares_minted = uint!(4_028802626000000000_U256);
        let recovered_at = Utc::now();
        let mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    underlying.clone(),
                    token.clone(),
                    "alp-recovered",
                    address!("0x1234567890abcdef1234567890abcdef12345678"),
                ),
            )
            .await
            .unwrap();

        assert!(matches!(
            load_view(&pool, &mint_id).await,
            ReceiptInventoryView::Pending { .. }
        ));

        harness
            .receive::<Mint>(
                mint_id.clone(),
                MintEvent::ExistingMintRecovered {
                    issuer_request_id: IssuerMintRequestId::random(),
                    tx_hash: b256!(
                        "0xaff33e27ec0a4232eb2c172fac9a86f794d6551d969b301d734b9ca8511f1e07"
                    ),
                    receipt_id,
                    shares_minted,
                    block_number: 46_337_116,
                    recovered_at,
                },
            )
            .await
            .unwrap();

        let ReceiptInventoryView::Active {
            receipt_id: view_receipt_id,
            underlying: view_underlying,
            token: view_token,
            initial_amount,
            current_balance,
            minted_at,
        } = load_view(&pool, &mint_id).await
        else {
            panic!("Expected Active variant");
        };

        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(minted_at, recovered_at);
    }

    #[tokio::test]
    async fn test_multiple_mints_for_different_symbols() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let aapl_underlying = UnderlyingSymbol::new("AAPL");
        let tsla_underlying = UnderlyingSymbol::new("TSLA");
        let aapl_mint_id = IssuerMintRequestId::random();
        let tsla_mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                aapl_mint_id.clone(),
                initiated_event(
                    aapl_underlying.clone(),
                    TokenSymbol::new("tAAPL"),
                    "alp-aapl",
                    address!("0x1111111111111111111111111111111111111111"),
                ),
            )
            .await
            .unwrap();

        harness
            .receive::<Mint>(
                tsla_mint_id.clone(),
                initiated_event(
                    tsla_underlying.clone(),
                    TokenSymbol::new("tTSLA"),
                    "alp-tsla",
                    address!("0x2222222222222222222222222222222222222222"),
                ),
            )
            .await
            .unwrap();

        let aapl_view = load_view(&pool, &aapl_mint_id).await;
        let tsla_view = load_view(&pool, &tsla_mint_id).await;

        assert!(matches!(aapl_view, ReceiptInventoryView::Pending { .. }));
        assert!(matches!(tsla_view, ReceiptInventoryView::Pending { .. }));

        if let ReceiptInventoryView::Pending { underlying, .. } = &aapl_view {
            assert_eq!(*underlying, aapl_underlying);
        }

        if let ReceiptInventoryView::Pending { underlying, .. } = &tsla_view {
            assert_eq!(*underlying, tsla_underlying);
        }
    }

    #[tokio::test]
    async fn test_other_mint_events_do_not_change_state() {
        let pool = migrated_pool().await;
        let harness =
            ReactorHarness::new(ReceiptInventoryViewReactor::new(pool.clone()));

        let mint_id = IssuerMintRequestId::random();

        harness
            .receive::<Mint>(
                mint_id.clone(),
                initiated_event(
                    UnderlyingSymbol::new("AAPL"),
                    TokenSymbol::new("tAAPL"),
                    "alp-other",
                    address!("0x4444444444444444444444444444444444444444"),
                ),
            )
            .await
            .unwrap();

        let original_view = load_view(&pool, &mint_id).await;
        assert!(matches!(original_view, ReceiptInventoryView::Pending { .. }));

        let events = vec![
            MintEvent::JournalConfirmed {
                issuer_request_id: IssuerMintRequestId::random(),
                confirmed_at: Utc::now(),
            },
            MintEvent::JournalRejected {
                issuer_request_id: IssuerMintRequestId::random(),
                reason: "test".to_string(),
                rejected_at: Utc::now(),
            },
            MintEvent::MintingFailed {
                issuer_request_id: IssuerMintRequestId::random(),
                error: "test error".to_string(),
                failed_at: Utc::now(),
            },
            MintEvent::MintCompleted {
                issuer_request_id: IssuerMintRequestId::random(),
                completed_at: Utc::now(),
            },
        ];

        for event in events {
            harness.receive::<Mint>(mint_id.clone(), event).await.unwrap();

            assert_eq!(load_view(&pool, &mint_id).await, original_view);
        }
    }
}
