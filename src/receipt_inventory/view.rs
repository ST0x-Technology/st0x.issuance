use alloy::primitives::U256;
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use crate::mint::{Mint, MintEvent};
use crate::redemption::{Redemption, RedemptionEvent};
use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReceiptInventoryViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

/// Tracks the lifecycle of ERC-1155 receipt tokens from minting through burning.
///
/// This view accumulates data across multiple Mint events to track receipt state:
/// - `Unavailable`: No mint initiated yet (initial state)
/// - `Pending`: Mint initiated, waiting for on-chain minting (has underlying/token)
/// - `Active`: Tokens minted, receipt exists with available balance
/// - `Depleted`: Receipt fully burned (balance = 0)
///
/// The view uses cross-aggregate listening, implementing both `View<Mint>` and
/// `View<Redemption>` to track receipts through their complete lifecycle. Currently
/// only Mint events are fully implemented; Redemption burn events will be added in
/// issue #25.
///
/// View ID: `issuer_request_id` (the Mint aggregate's aggregate_id)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum ReceiptInventoryView {
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

impl Default for ReceiptInventoryView {
    fn default() -> Self {
        Self::Unavailable
    }
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

    fn with_tokens_burned(
        self,
        burn_receipt_id: U256,
        shares_burned: U256,
        burned_at: DateTime<Utc>,
    ) -> Self {
        match self {
            Self::Active {
                receipt_id,
                underlying,
                token,
                initial_amount,
                current_balance,
                minted_at,
            } if receipt_id == burn_receipt_id => {
                if current_balance > shares_burned {
                    Self::Active {
                        receipt_id,
                        underlying,
                        token,
                        initial_amount,
                        current_balance: current_balance - shares_burned,
                        minted_at,
                    }
                } else {
                    Self::Depleted {
                        receipt_id,
                        underlying,
                        token,
                        initial_amount,
                        depleted_at: burned_at,
                    }
                }
            }
            other => other,
        }
    }
}

impl View<Mint> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Mint>) {
        match &event.payload {
            MintEvent::Initiated { underlying, token, .. } => {
                *self = self
                    .clone()
                    .with_initiated_data(underlying.clone(), token.clone());
            }
            MintEvent::TokensMinted {
                receipt_id,
                shares_minted,
                minted_at,
                ..
            } => {
                *self = self.clone().with_tokens_minted(
                    *receipt_id,
                    *shares_minted,
                    *minted_at,
                );
            }
            MintEvent::JournalConfirmed { .. }
            | MintEvent::JournalRejected { .. }
            | MintEvent::MintingFailed { .. }
            | MintEvent::MintCompleted { .. } => {}
        }
    }
}

impl View<Redemption> for ReceiptInventoryView {
    fn update(&mut self, event: &EventEnvelope<Redemption>) {
        match &event.payload {
            RedemptionEvent::TokensBurned {
                receipt_id,
                shares_burned,
                burned_at,
                ..
            } => {
                *self = self.clone().with_tokens_burned(
                    *receipt_id,
                    *shares_burned,
                    *burned_at,
                );
            }
            RedemptionEvent::Detected { .. }
            | RedemptionEvent::AlpacaCalled { .. }
            | RedemptionEvent::AlpacaCallFailed { .. }
            | RedemptionEvent::AlpacaJournalCompleted { .. }
            | RedemptionEvent::RedemptionFailed { .. }
            | RedemptionEvent::BurningFailed { .. } => {}
        }
    }
}

/// Lists all Active receipts for a given underlying symbol.
///
/// Returns only receipts in the Active state (has available balance > 0).
/// Pending, Unavailable, and Depleted receipts are excluded.
///
/// # Use Case
/// View all available receipt inventory for a specific asset, useful for
/// operational dashboards and manual receipt selection.
pub(crate) async fn list_active_receipts(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
) -> Result<Vec<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let underlying_str = &underlying.0;
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE json_extract(payload, '$.Active.underlying') = ?
        "#,
        underlying_str
    )
    .fetch_all(pool)
    .await?;

    let views: Result<Vec<ReceiptInventoryView>, serde_json::Error> = rows
        .into_iter()
        .map(|row| serde_json::from_str(&row.payload))
        .collect();

    Ok(views?)
}

/// Finds a receipt with sufficient balance for a given underlying symbol.
///
/// Returns the receipt with the highest balance that meets or exceeds the
/// minimum balance requirement. Returns `None` if no receipt has sufficient
/// balance.
///
/// # Use Case
/// Primary method for selecting which receipt to burn from during redemption.
/// The burn logic will call this to find a suitable receipt, then use that
/// receipt's ID in the vault.withdraw() call.
pub(crate) async fn find_receipt_with_balance(
    pool: &Pool<Sqlite>,
    underlying: &UnderlyingSymbol,
    minimum_balance: U256,
) -> Result<Option<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let receipts = list_active_receipts(pool, underlying).await?;

    Ok(receipts
        .into_iter()
        .filter_map(|view| match view {
            ReceiptInventoryView::Active { current_balance, .. }
                if current_balance >= minimum_balance =>
            {
                Some((current_balance, view))
            }
            _ => None,
        })
        .sorted_by(|(a, _), (b, _)| b.cmp(a))
        .map(|(_, view)| view)
        .next())
}

/// Retrieves a single receipt by issuer_request_id (view_id).
///
/// Returns the receipt inventory view for the given issuer_request_id, which
/// serves as the view_id in the receipt_inventory_view table.
///
/// # Use Case
/// Used for querying the current state of a specific receipt in tests and
/// operational queries where the issuer_request_id is known.
#[cfg(test)]
pub(crate) async fn get_receipt(
    pool: &Pool<Sqlite>,
    issuer_request_id: &crate::mint::IssuerRequestId,
) -> Result<Option<ReceiptInventoryView>, ReceiptInventoryViewError> {
    let view_id = &issuer_request_id.0;
    let row = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM receipt_inventory_view
        WHERE view_id = ?
        "#,
        view_id
    )
    .fetch_optional(pool)
    .await?;

    match row {
        Some(row) => {
            let view: ReceiptInventoryView =
                serde_json::from_str(&row.payload)?;
            Ok(Some(view))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use chrono::Utc;
    use cqrs_es::persist::{GenericQuery, ViewContext, ViewRepository};
    use cqrs_es::{CqrsFramework, EventEnvelope, EventStore, Query};
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::error;

    use super::*;
    use crate::mint::{
        ClientId, IssuerRequestId, Mint, MintCommand, MintEvent, MintView,
        Network, Quantity, TokenizationRequestId,
    };
    use crate::redemption::{
        Redemption, RedemptionCommand, RedemptionEvent, RedemptionView,
    };

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

    /// Custom query for receipt inventory updates from Mint events.
    ///
    /// Unlike GenericQuery which uses aggregate_id as view_id, this query extracts
    /// the issuer_request_id from event payloads to use as the view_id. This allows
    /// both Mint and Redemption aggregates to update the same receipt inventory view.
    struct ReceiptInventoryMintQuery {
        view_repository: Arc<SqliteViewRepository<ReceiptInventoryView, Mint>>,
    }

    impl ReceiptInventoryMintQuery {
        const fn new(
            view_repository: Arc<
                SqliteViewRepository<ReceiptInventoryView, Mint>,
            >,
        ) -> Self {
            Self { view_repository }
        }
    }

    #[async_trait::async_trait]
    impl Query<Mint> for ReceiptInventoryMintQuery {
        async fn dispatch(
            &self,
            _aggregate_id: &str,
            events: &[EventEnvelope<Mint>],
        ) {
            for event in events {
                let view_id = match &event.payload {
                    MintEvent::Initiated { issuer_request_id, .. }
                    | MintEvent::TokensMinted { issuer_request_id, .. } => {
                        &issuer_request_id.0
                    }
                    _ => continue,
                };

                let result = async {
                    let (mut view, view_context) = self
                        .view_repository
                        .load_with_context(view_id)
                        .await?
                        .map_or_else(
                            || {
                                (
                                    ReceiptInventoryView::default(),
                                    ViewContext::new(view_id.to_string(), 0),
                                )
                            },
                            |pair| pair,
                        );

                    view.update(event);
                    self.view_repository.update_view(view, view_context).await
                }
                .await;

                if let Err(err) = result {
                    error!(
                        "Failed to update receipt inventory view for {view_id}: {err}"
                    );
                }
            }
        }
    }

    /// Custom query for receipt inventory updates from Redemption events.
    ///
    /// Unlike GenericQuery which uses aggregate_id as view_id, this query extracts
    /// the issuer_request_id from event payloads to use as the view_id. This allows
    /// both Mint and Redemption aggregates to update the same receipt inventory view.
    struct ReceiptInventoryRedemptionQuery {
        view_repository:
            Arc<SqliteViewRepository<ReceiptInventoryView, Redemption>>,
    }

    impl ReceiptInventoryRedemptionQuery {
        const fn new(
            view_repository: Arc<
                SqliteViewRepository<ReceiptInventoryView, Redemption>,
            >,
        ) -> Self {
            Self { view_repository }
        }
    }

    #[async_trait::async_trait]
    impl Query<Redemption> for ReceiptInventoryRedemptionQuery {
        async fn dispatch(
            &self,
            _aggregate_id: &str,
            events: &[EventEnvelope<Redemption>],
        ) {
            for event in events {
                let view_id = match &event.payload {
                    RedemptionEvent::TokensBurned {
                        issuer_request_id, ..
                    } => &issuer_request_id.0,
                    _ => continue,
                };

                let result = async {
                    let (mut view, view_context) = self
                        .view_repository
                        .load_with_context(view_id)
                        .await?
                        .map_or_else(
                            || {
                                (
                                    ReceiptInventoryView::default(),
                                    ViewContext::new(view_id.to_string(), 0),
                                )
                            },
                            |pair| pair,
                        );

                    view.update(event);
                    self.view_repository.update_view(view, view_context).await
                }
                .await;

                if let Err(err) = result {
                    error!(
                        "Failed to update receipt inventory view for {view_id}: {err}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_unavailable_to_pending_on_mint_initiated() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");

        let event = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new("iss-123"),
            tokenization_request_id: TokenizationRequestId::new("alp-456"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
            initiated_at: Utc::now(),
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = ReceiptInventoryView::default();

        assert!(matches!(view, ReceiptInventoryView::Unavailable));

        view.update(&envelope);

        let ReceiptInventoryView::Pending {
            underlying: view_underlying,
            token: view_token,
        } = view
        else {
            panic!("Expected Pending variant, got {view:?}");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
    }

    #[test]
    fn test_pending_to_active_on_tokens_minted() {
        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let receipt_id = uint!(42_U256);
        let shares_minted = uint!(100_000000000000000000_U256);
        let minted_at = Utc::now();

        let mut view = ReceiptInventoryView::Pending {
            underlying: underlying.clone(),
            token: token.clone(),
        };

        let event = MintEvent::TokensMinted {
            issuer_request_id: IssuerRequestId::new("iss-456"),
            tx_hash: b256!(
                "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd"
            ),
            receipt_id,
            shares_minted,
            gas_used: 50000,
            block_number: 1000,
            minted_at,
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-456".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Active {
            receipt_id: view_receipt_id,
            underlying: view_underlying,
            token: view_token,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = view
        else {
            panic!("Expected Active variant, got {view:?}");
        };

        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[test]
    fn test_view_stores_underlying_and_token_from_initiated() {
        let underlying = UnderlyingSymbol::new("NVDA");
        let token = TokenSymbol::new("tNVDA");

        let event = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new("iss-789"),
            tokenization_request_id: TokenizationRequestId::new("alp-999"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: underlying.clone(),
            token: token.clone(),
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc"),
            initiated_at: Utc::now(),
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-789".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = ReceiptInventoryView::default();
        view.update(&envelope);

        let ReceiptInventoryView::Pending {
            underlying: stored_underlying,
            token: stored_token,
        } = view
        else {
            panic!("Expected Pending variant, got {view:?}");
        };

        assert_eq!(stored_underlying, underlying);
        assert_eq!(stored_token, token);
    }

    #[test]
    fn test_view_combines_initiated_data_with_tokens_minted() {
        let underlying = UnderlyingSymbol::new("AMD");
        let token = TokenSymbol::new("tAMD");

        let mut view = ReceiptInventoryView::Pending {
            underlying: underlying.clone(),
            token: token.clone(),
        };

        let receipt_id = uint!(7_U256);
        let shares_minted = uint!(250_500000000000000000_U256);
        let minted_at = Utc::now();

        let event = MintEvent::TokensMinted {
            issuer_request_id: IssuerRequestId::new("iss-combo"),
            tx_hash: b256!(
                "0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff"
            ),
            receipt_id,
            shares_minted,
            gas_used: 75000,
            block_number: 2000,
            minted_at,
        };

        let envelope: EventEnvelope<Mint> = EventEnvelope {
            aggregate_id: "iss-combo".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Active {
            underlying: view_underlying,
            token: view_token,
            receipt_id: view_receipt_id,
            initial_amount,
            current_balance,
            minted_at: view_minted_at,
        } = view
        else {
            panic!("Expected Active variant, got {view:?}");
        };

        assert_eq!(view_underlying, underlying);
        assert_eq!(view_token, token);
        assert_eq!(view_receipt_id, receipt_id);
        assert_eq!(initial_amount, shares_minted);
        assert_eq!(current_balance, shares_minted);
        assert_eq!(view_minted_at, minted_at);
    }

    #[test]
    fn test_multiple_mints_for_different_symbols() {
        let aapl_underlying = UnderlyingSymbol::new("AAPL");
        let aapl_token = TokenSymbol::new("tAAPL");

        let tsla_underlying = UnderlyingSymbol::new("TSLA");
        let tsla_token = TokenSymbol::new("tTSLA");

        let mut aapl_view = ReceiptInventoryView::default();
        let mut tsla_view = ReceiptInventoryView::default();

        let aapl_initiated = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new("iss-aapl"),
            tokenization_request_id: TokenizationRequestId::new("alp-aapl"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: aapl_underlying.clone(),
            token: aapl_token,
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x1111111111111111111111111111111111111111"),
            initiated_at: Utc::now(),
        };

        let tsla_initiated = MintEvent::Initiated {
            issuer_request_id: IssuerRequestId::new("iss-tsla"),
            tokenization_request_id: TokenizationRequestId::new("alp-tsla"),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: tsla_underlying.clone(),
            token: tsla_token,
            network: Network::new("base"),
            client_id: ClientId::new(),
            wallet: address!("0x2222222222222222222222222222222222222222"),
            initiated_at: Utc::now(),
        };

        aapl_view.update(&EventEnvelope::<Mint> {
            aggregate_id: "iss-aapl".to_string(),
            sequence: 1,
            payload: aapl_initiated,
            metadata: HashMap::new(),
        });

        tsla_view.update(&EventEnvelope::<Mint> {
            aggregate_id: "iss-tsla".to_string(),
            sequence: 1,
            payload: tsla_initiated,
            metadata: HashMap::new(),
        });

        assert!(matches!(aapl_view, ReceiptInventoryView::Pending { .. }));
        assert!(matches!(tsla_view, ReceiptInventoryView::Pending { .. }));

        if let ReceiptInventoryView::Pending { underlying, .. } = &aapl_view {
            assert_eq!(*underlying, aapl_underlying);
        }

        if let ReceiptInventoryView::Pending { underlying, .. } = &tsla_view {
            assert_eq!(*underlying, tsla_underlying);
        }
    }

    #[test]
    fn test_with_tokens_burned_decrements_balance_on_partial_burn() {
        let view = ReceiptInventoryView::Active {
            receipt_id: uint!(42_U256),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let result = view.with_tokens_burned(
            uint!(42_U256),
            uint!(30_000000000000000000_U256),
            burned_at,
        );

        let ReceiptInventoryView::Active {
            receipt_id,
            current_balance,
            initial_amount,
            ..
        } = result
        else {
            panic!(
                "Expected Active variant after partial burn, got {result:?}"
            );
        };

        assert_eq!(receipt_id, uint!(42_U256));
        assert_eq!(current_balance, uint!(70_000000000000000000_U256));
        assert_eq!(initial_amount, uint!(100_000000000000000000_U256));
    }

    #[test]
    fn test_with_tokens_burned_transitions_to_depleted_on_exact_burn() {
        let view = ReceiptInventoryView::Active {
            receipt_id: uint!(7_U256),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            initial_amount: uint!(50_000000000000000000_U256),
            current_balance: uint!(50_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let result = view.with_tokens_burned(
            uint!(7_U256),
            uint!(50_000000000000000000_U256),
            burned_at,
        );

        let ReceiptInventoryView::Depleted {
            receipt_id,
            underlying,
            token,
            initial_amount,
            depleted_at,
        } = result
        else {
            panic!("Expected Depleted variant after full burn, got {result:?}");
        };

        assert_eq!(receipt_id, uint!(7_U256));
        assert_eq!(underlying, UnderlyingSymbol::new("TSLA"));
        assert_eq!(token, TokenSymbol::new("tTSLA"));
        assert_eq!(initial_amount, uint!(50_000000000000000000_U256));
        assert_eq!(depleted_at, burned_at);
    }

    #[test]
    fn test_with_tokens_burned_transitions_to_depleted_on_over_burn() {
        let view = ReceiptInventoryView::Active {
            receipt_id: uint!(10_U256),
            underlying: UnderlyingSymbol::new("NVDA"),
            token: TokenSymbol::new("tNVDA"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(30_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let result = view.with_tokens_burned(
            uint!(10_U256),
            uint!(50_000000000000000000_U256),
            burned_at,
        );

        assert!(
            matches!(result, ReceiptInventoryView::Depleted { .. }),
            "Expected Depleted variant when burning more than balance, got {result:?}"
        );
    }

    #[test]
    fn test_with_tokens_burned_leaves_active_unchanged_on_wrong_receipt_id() {
        let view = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: UnderlyingSymbol::new("GOOG"),
            token: TokenSymbol::new("tGOOG"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };
        let original_view = view.clone();

        let result = view.with_tokens_burned(
            uint!(99_U256),
            uint!(50_000000000000000000_U256),
            Utc::now(),
        );

        assert_eq!(
            result, original_view,
            "Burn with mismatched receipt ID should not change view"
        );
    }

    #[test]
    fn test_with_tokens_burned_leaves_pending_unchanged() {
        let view = ReceiptInventoryView::Pending {
            underlying: UnderlyingSymbol::new("AMD"),
            token: TokenSymbol::new("tAMD"),
        };
        let original_view = view.clone();

        let result = view.with_tokens_burned(
            uint!(1_U256),
            uint!(50_000000000000000000_U256),
            Utc::now(),
        );

        assert_eq!(
            result, original_view,
            "Burn in Pending state should not change view"
        );
    }

    #[test]
    fn test_with_tokens_burned_leaves_unavailable_unchanged() {
        let view = ReceiptInventoryView::Unavailable;
        let original_view = view.clone();

        let result = view.with_tokens_burned(
            uint!(1_U256),
            uint!(50_000000000000000000_U256),
            Utc::now(),
        );

        assert_eq!(
            result, original_view,
            "Burn in Unavailable state should not change view"
        );
    }

    #[test]
    fn test_with_tokens_burned_leaves_depleted_unchanged() {
        let view = ReceiptInventoryView::Depleted {
            receipt_id: uint!(5_U256),
            underlying: UnderlyingSymbol::new("META"),
            token: TokenSymbol::new("tMETA"),
            initial_amount: uint!(100_000000000000000000_U256),
            depleted_at: Utc::now(),
        };
        let original_view = view.clone();

        let result = view.with_tokens_burned(
            uint!(5_U256),
            uint!(50_000000000000000000_U256),
            Utc::now(),
        );

        assert_eq!(
            result, original_view,
            "Burn in Depleted state should not change view"
        );
    }

    #[test]
    fn test_active_receipt_balance_decrements_on_burn() {
        let mut view = ReceiptInventoryView::Active {
            receipt_id: uint!(42_U256),
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-123"),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_burned: uint!(30_000000000000000000_U256),
            gas_used: 50000,
            block_number: 2000,
            burned_at,
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Active { current_balance, .. } = view else {
            panic!("Expected Active variant after partial burn, got {view:?}");
        };

        assert_eq!(current_balance, uint!(70_000000000000000000_U256));
    }

    #[test]
    fn test_active_to_depleted_when_balance_reaches_zero() {
        let mut view = ReceiptInventoryView::Active {
            receipt_id: uint!(7_U256),
            underlying: UnderlyingSymbol::new("TSLA"),
            token: TokenSymbol::new("tTSLA"),
            initial_amount: uint!(50_000000000000000000_U256),
            current_balance: uint!(50_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-456"),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            receipt_id: uint!(7_U256),
            shares_burned: uint!(50_000000000000000000_U256),
            gas_used: 50000,
            block_number: 3000,
            burned_at,
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-456".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let ReceiptInventoryView::Depleted {
            receipt_id,
            underlying,
            token,
            initial_amount,
            depleted_at,
        } = view
        else {
            panic!("Expected Depleted variant after full burn, got {view:?}");
        };

        assert_eq!(receipt_id, uint!(7_U256));
        assert_eq!(underlying, UnderlyingSymbol::new("TSLA"));
        assert_eq!(token, TokenSymbol::new("tTSLA"));
        assert_eq!(initial_amount, uint!(50_000000000000000000_U256));
        assert_eq!(depleted_at, burned_at);
    }

    #[test]
    fn test_active_to_depleted_when_burning_more_than_balance() {
        let mut view = ReceiptInventoryView::Active {
            receipt_id: uint!(10_U256),
            underlying: UnderlyingSymbol::new("NVDA"),
            token: TokenSymbol::new("tNVDA"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(30_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let burned_at = Utc::now();
        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-789"),
            tx_hash: b256!(
                "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
            ),
            receipt_id: uint!(10_U256),
            shares_burned: uint!(50_000000000000000000_U256),
            gas_used: 50000,
            block_number: 4000,
            burned_at,
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-789".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(
            matches!(view, ReceiptInventoryView::Depleted { .. }),
            "Expected Depleted variant when burning more than balance, got {view:?}"
        );
    }

    #[test]
    fn test_burn_from_wrong_receipt_id_leaves_unchanged() {
        let mut view = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: UnderlyingSymbol::new("GOOG"),
            token: TokenSymbol::new("tGOOG"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };
        let original_view = view.clone();

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-wrong"),
            tx_hash: b256!(
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ),
            receipt_id: uint!(99_U256),
            shares_burned: uint!(50_000000000000000000_U256),
            gas_used: 50000,
            block_number: 5000,
            burned_at: Utc::now(),
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-wrong".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert_eq!(
            view, original_view,
            "Burn with mismatched receipt ID should not change view"
        );
    }

    #[test]
    fn test_burn_in_pending_state_leaves_unchanged() {
        let mut view = ReceiptInventoryView::Pending {
            underlying: UnderlyingSymbol::new("AMD"),
            token: TokenSymbol::new("tAMD"),
        };
        let original_view = view.clone();

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-pending"),
            tx_hash: b256!(
                "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            ),
            receipt_id: uint!(1_U256),
            shares_burned: uint!(50_000000000000000000_U256),
            gas_used: 50000,
            block_number: 6000,
            burned_at: Utc::now(),
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-pending".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert_eq!(
            view, original_view,
            "Burn in Pending state should not change view"
        );
    }

    #[test]
    fn test_burn_in_unavailable_state_leaves_unchanged() {
        let mut view = ReceiptInventoryView::Unavailable;
        let original_view = view.clone();

        let event = RedemptionEvent::TokensBurned {
            issuer_request_id: IssuerRequestId::new("red-unavail"),
            tx_hash: b256!(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            ),
            receipt_id: uint!(1_U256),
            shares_burned: uint!(50_000000000000000000_U256),
            gas_used: 50000,
            block_number: 7000,
            burned_at: Utc::now(),
        };

        let envelope: EventEnvelope<Redemption> = EventEnvelope {
            aggregate_id: "red-unavail".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert_eq!(
            view, original_view,
            "Burn in Unavailable state should not change view"
        );
    }

    #[test]
    fn test_other_redemption_events_leave_view_unchanged() {
        let mut view = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: UnderlyingSymbol::new("META"),
            token: TokenSymbol::new("tMETA"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };
        let original_view = view.clone();

        let events = vec![
            RedemptionEvent::Detected {
                issuer_request_id: IssuerRequestId::new("red-123"),
                underlying: UnderlyingSymbol::new("META"),
                token: TokenSymbol::new("tMETA"),
                wallet: address!("0x3333333333333333333333333333333333333333"),
                quantity: Quantity::new(Decimal::from(10)),
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                block_number: 5000,
                detected_at: Utc::now(),
            },
            RedemptionEvent::AlpacaCalled {
                issuer_request_id: IssuerRequestId::new("red-123"),
                tokenization_request_id: TokenizationRequestId::new("alp-456"),
                called_at: Utc::now(),
            },
            RedemptionEvent::AlpacaCallFailed {
                issuer_request_id: IssuerRequestId::new("red-123"),
                error: "test error".to_string(),
                failed_at: Utc::now(),
            },
            RedemptionEvent::AlpacaJournalCompleted {
                issuer_request_id: IssuerRequestId::new("red-123"),
                alpaca_journal_completed_at: Utc::now(),
            },
            RedemptionEvent::RedemptionFailed {
                issuer_request_id: IssuerRequestId::new("red-123"),
                reason: "test reason".to_string(),
                failed_at: Utc::now(),
            },
            RedemptionEvent::BurningFailed {
                issuer_request_id: IssuerRequestId::new("red-123"),
                error: "blockchain error".to_string(),
                failed_at: Utc::now(),
            },
        ];

        for event in events {
            let envelope: EventEnvelope<Redemption> = EventEnvelope {
                aggregate_id: "red-123".to_string(),
                sequence: 1,
                payload: event,
                metadata: HashMap::new(),
            };

            view.update(&envelope);

            assert_eq!(
                view, original_view,
                "Non-burn redemption events should not change view state"
            );
        }
    }

    #[test]
    fn test_other_mint_events_do_not_change_state() {
        let mut view = ReceiptInventoryView::Pending {
            underlying: UnderlyingSymbol::new("AAPL"),
            token: TokenSymbol::new("tAAPL"),
        };
        let original_view = view.clone();

        let events = vec![
            MintEvent::JournalConfirmed {
                issuer_request_id: IssuerRequestId::new("iss-123"),
                confirmed_at: Utc::now(),
            },
            MintEvent::JournalRejected {
                issuer_request_id: IssuerRequestId::new("iss-123"),
                reason: "test".to_string(),
                rejected_at: Utc::now(),
            },
            MintEvent::MintingFailed {
                issuer_request_id: IssuerRequestId::new("iss-123"),
                error: "test error".to_string(),
                failed_at: Utc::now(),
            },
            MintEvent::MintCompleted {
                issuer_request_id: IssuerRequestId::new("iss-123"),
                completed_at: Utc::now(),
            },
        ];

        for event in events {
            let envelope: EventEnvelope<Mint> = EventEnvelope {
                aggregate_id: "iss-123".to_string(),
                sequence: 2,
                payload: event,
                metadata: HashMap::new(),
            };

            view.update(&envelope);

            assert_eq!(view, original_view);
        }
    }

    #[tokio::test]
    async fn test_list_active_receipts_returns_only_active() {
        let pool = setup_test_db().await;

        let underlying = UnderlyingSymbol::new("GOOG");

        let active_view1 = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tGOOG"),
            initial_amount: uint!(100_000000000000000000_U256),
            current_balance: uint!(100_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let active_view2 = ReceiptInventoryView::Active {
            receipt_id: uint!(2_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tGOOG"),
            initial_amount: uint!(50_000000000000000000_U256),
            current_balance: uint!(50_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let pending_view = ReceiptInventoryView::Pending {
            underlying: underlying.clone(),
            token: TokenSymbol::new("tGOOG"),
        };

        let payload1 =
            serde_json::to_string(&active_view1).expect("Failed to serialize");
        let payload2 =
            serde_json::to_string(&active_view2).expect("Failed to serialize");
        let payload3 =
            serde_json::to_string(&pending_view).expect("Failed to serialize");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-1",
            payload1
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-2",
            payload2
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-3",
            payload3
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        let results = list_active_receipts(&pool, &underlying)
            .await
            .expect("Query should succeed");

        assert_eq!(results.len(), 2);
        assert!(
            results
                .iter()
                .all(|v| matches!(v, ReceiptInventoryView::Active { .. }))
        );
    }

    #[tokio::test]
    async fn test_find_receipt_with_balance_returns_highest() {
        let pool = setup_test_db().await;

        let underlying = UnderlyingSymbol::new("AMZN");

        let view1 = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tAMZN"),
            initial_amount: uint!(50_000000000000000000_U256),
            current_balance: uint!(50_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let view2 = ReceiptInventoryView::Active {
            receipt_id: uint!(2_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tAMZN"),
            initial_amount: uint!(150_000000000000000000_U256),
            current_balance: uint!(150_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let payload1 =
            serde_json::to_string(&view1).expect("Failed to serialize");
        let payload2 =
            serde_json::to_string(&view2).expect("Failed to serialize");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-1",
            payload1
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-2",
            payload2
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        let result = find_receipt_with_balance(
            &pool,
            &underlying,
            uint!(40_000000000000000000_U256),
        )
        .await
        .expect("Query should succeed");

        assert!(result.is_some());

        let ReceiptInventoryView::Active {
            receipt_id, current_balance, ..
        } = result.unwrap()
        else {
            panic!("Expected Active variant");
        };

        assert_eq!(receipt_id, uint!(2_U256));
        assert_eq!(current_balance, uint!(150_000000000000000000_U256));
    }

    #[tokio::test]
    async fn test_find_receipt_with_balance_returns_none_when_insufficient() {
        let pool = setup_test_db().await;

        let underlying = UnderlyingSymbol::new("META");

        let view = ReceiptInventoryView::Active {
            receipt_id: uint!(1_U256),
            underlying: underlying.clone(),
            token: TokenSymbol::new("tMETA"),
            initial_amount: uint!(50_000000000000000000_U256),
            current_balance: uint!(50_000000000000000000_U256),
            minted_at: Utc::now(),
        };

        let payload =
            serde_json::to_string(&view).expect("Failed to serialize");

        sqlx::query!(
            r"INSERT INTO receipt_inventory_view (view_id, version, payload) VALUES (?, 1, ?)",
            "iss-1",
            payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert");

        let result = find_receipt_with_balance(
            &pool,
            &underlying,
            uint!(100_000000000000000000_U256),
        )
        .await
        .expect("Query should succeed");

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cross_aggregate_burn_updates_mint_receipt() {
        let pool = setup_test_db().await;

        let mint_view_repo =
            Arc::new(SqliteViewRepository::<MintView, Mint>::new(
                pool.clone(),
                "mint_view".to_string(),
            ));
        let mint_query = GenericQuery::new(mint_view_repo);

        let receipt_inventory_mint_repo =
            Arc::new(SqliteViewRepository::<ReceiptInventoryView, Mint>::new(
                pool.clone(),
                "receipt_inventory_view".to_string(),
            ));
        let receipt_inventory_mint_query =
            ReceiptInventoryMintQuery::new(receipt_inventory_mint_repo);

        let mint_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![Box::new(mint_query), Box::new(receipt_inventory_mint_query)],
            (),
        ));

        let redemption_view_repo =
            Arc::new(SqliteViewRepository::<RedemptionView, Redemption>::new(
                pool.clone(),
                "redemption_view".to_string(),
            ));
        let redemption_query = GenericQuery::new(redemption_view_repo);

        let receipt_inventory_redemption_repo = Arc::new(
            SqliteViewRepository::<ReceiptInventoryView, Redemption>::new(
                pool.clone(),
                "receipt_inventory_view".to_string(),
            ),
        );
        let receipt_inventory_redemption_query =
            ReceiptInventoryRedemptionQuery::new(
                receipt_inventory_redemption_repo,
            );

        let redemption_cqrs = Arc::new(sqlite_cqrs(
            pool.clone(),
            vec![
                Box::new(redemption_query),
                Box::new(receipt_inventory_redemption_query),
            ],
            (),
        ));

        let issuer_request_id = IssuerRequestId::new("iss-mint-123");
        let receipt_id = uint!(42_U256);
        let initial_shares = uint!(100_000000000000000000_U256);

        execute_mint_flow(
            &mint_cqrs,
            &issuer_request_id,
            receipt_id,
            initial_shares,
        )
        .await;

        let balance_after_mint =
            verify_active_receipt(&pool, &issuer_request_id).await;
        assert_eq!(balance_after_mint, initial_shares);

        let shares_to_burn = uint!(30_000000000000000000_U256);

        execute_redemption_flow(
            &redemption_cqrs,
            &issuer_request_id,
            receipt_id,
            shares_to_burn,
        )
        .await;

        let balance_after_burn =
            verify_active_receipt(&pool, &issuer_request_id).await;

        let expected_balance = initial_shares - shares_to_burn;
        assert_eq!(
            balance_after_burn, expected_balance,
            "Receipt inventory at issuer_request_id should be updated by redemption burn. \
             Expected {expected_balance}, got {balance_after_burn}"
        );
    }

    async fn execute_mint_flow<ES>(
        mint_cqrs: &Arc<CqrsFramework<Mint, ES>>,
        issuer_request_id: &IssuerRequestId,
        receipt_id: U256,
        initial_shares: U256,
    ) where
        ES: EventStore<Mint>,
    {
        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::Initiate {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-456",
                    ),
                    quantity: Quantity::new(Decimal::from(100)),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    client_id: ClientId::new(),
                    wallet: address!(
                        "0x1111111111111111111111111111111111111111"
                    ),
                },
            )
            .await
            .expect("Initiate should succeed");

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::ConfirmJournal {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("ConfirmJournal should succeed");

        mint_cqrs
            .execute(
                &issuer_request_id.0,
                MintCommand::RecordMintSuccess {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    receipt_id,
                    shares_minted: initial_shares,
                    gas_used: 50000,
                    block_number: 1000,
                },
            )
            .await
            .expect("RecordMintSuccess should succeed");
    }

    async fn execute_redemption_flow<ES>(
        redemption_cqrs: &Arc<CqrsFramework<Redemption, ES>>,
        issuer_request_id: &IssuerRequestId,
        receipt_id: U256,
        shares_to_burn: U256,
    ) where
        ES: EventStore<Redemption>,
    {
        let redemption_aggregate_id = "red-redemption-456";

        redemption_cqrs
            .execute(
                redemption_aggregate_id,
                RedemptionCommand::Detect {
                    issuer_request_id: issuer_request_id.clone(),
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    wallet: address!("0x2222222222222222222222222222222222222222"),
                    quantity: Quantity::new(Decimal::from(30)),
                    tx_hash: b256!(
                        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    ),
                    block_number: 1500,
                },
            )
            .await
            .expect("Detect should succeed");

        redemption_cqrs
            .execute(
                redemption_aggregate_id,
                RedemptionCommand::RecordAlpacaCall {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-redeem-789",
                    ),
                },
            )
            .await
            .expect("RecordAlpacaCall should succeed");

        redemption_cqrs
            .execute(
                redemption_aggregate_id,
                RedemptionCommand::ConfirmAlpacaComplete {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("ConfirmAlpacaComplete should succeed");

        redemption_cqrs
            .execute(
                redemption_aggregate_id,
                RedemptionCommand::RecordBurnSuccess {
                    issuer_request_id: issuer_request_id.clone(),
                    tx_hash: b256!(
                        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                    ),
                    receipt_id,
                    shares_burned: shares_to_burn,
                    gas_used: 45000,
                    block_number: 2000,
                },
            )
            .await
            .expect("RecordBurnSuccess should succeed");
    }

    async fn verify_active_receipt(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerRequestId,
    ) -> U256 {
        let view = get_receipt(pool, issuer_request_id)
            .await
            .expect("Query should succeed")
            .expect("View should exist");

        let ReceiptInventoryView::Active { current_balance, .. } = view else {
            panic!("Expected Active view, got {view:?}");
        };

        current_balance
    }
}
