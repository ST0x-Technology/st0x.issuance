mod api;

use alloy::primitives::Address;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

pub(crate) use api::list_tokenized_assets;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct UnderlyingSymbol(pub(crate) String);

impl std::fmt::Display for UnderlyingSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl UnderlyingSymbol {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenSymbol(pub(crate) String);

impl std::fmt::Display for TokenSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TokenSymbol {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Network(pub(crate) String);

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Network {
    pub(crate) fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TokenizedAssetCommand {
    Add {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedAssetEvent {
    Added {
        underlying: UnderlyingSymbol,
        token: TokenSymbol,
        network: Network,
        vault: Address,
        added_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedAssetEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Added { .. } => "TokenizedAssetEvent::Added".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TokenizedAsset {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) token: TokenSymbol,
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) enabled: bool,
    pub(crate) added_at: DateTime<Utc>,
}

impl TokenizedAsset {
    pub(crate) fn from_event(event: &TokenizedAssetEvent) -> Self {
        match event {
            TokenizedAssetEvent::Added {
                underlying,
                token,
                network,
                vault,
                added_at,
            } => Self {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault: *vault,
                enabled: true,
                added_at: *added_at,
            },
        }
    }

    pub(crate) fn apply_transition(
        event: &TokenizedAssetEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        // TokenizedAsset has no transition events - Added is a genesis event
        Err(LifecycleError::Mismatch {
            state: format!("{current:?}"),
            event: event.event_type(),
        })
    }
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub(crate) enum TokenizedAssetError {
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError<Never>),
}

#[async_trait]
impl Aggregate for Lifecycle<TokenizedAsset, Never> {
    type Command = TokenizedAssetCommand;
    type Event = TokenizedAssetEvent;
    type Error = TokenizedAssetError;
    type Services = ();

    fn aggregate_type() -> String {
        "TokenizedAsset".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, TokenizedAsset::apply_transition)
            .or_initialize(&event, |e| Ok(TokenizedAsset::from_event(e)));
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedAssetCommand::Add {
                underlying,
                token,
                network,
                vault,
            } => {
                if self.live().is_ok() {
                    tracing::debug!(
                        underlying = %underlying.0,
                        "Asset already added, skipping"
                    );
                    return Ok(vec![]);
                }

                let now = Utc::now();

                Ok(vec![TokenizedAssetEvent::Added {
                    underlying,
                    token,
                    network,
                    vault,
                    added_at: now,
                }])
            }
        }
    }
}

impl View<Self> for Lifecycle<TokenizedAsset, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, TokenizedAsset::apply_transition)
            .or_initialize(&event.payload, |e| {
                Ok(TokenizedAsset::from_event(e))
            });
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedAssetViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

pub(crate) async fn list_enabled_assets(
    pool: &Pool<Sqlite>,
) -> Result<Vec<TokenizedAsset>, TokenizedAssetViewError> {
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload: String"
        FROM tokenized_asset_view
        WHERE json_extract(payload, '$.Live.enabled') = 1
        "#
    )
    .fetch_all(pool)
    .await?;

    let assets: Vec<TokenizedAsset> = rows
        .into_iter()
        .filter_map(|row| {
            let lifecycle: Lifecycle<TokenizedAsset, Never> =
                serde_json::from_str(&row.payload).ok()?;

            match lifecycle {
                Lifecycle::Live(asset) if asset.enabled => Some(asset),
                _ => None,
            }
        })
        .collect();

    Ok(assets)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::{Aggregate, test::TestFramework};
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};

    use super::*;

    type TokenizedAssetTestFramework =
        TestFramework<Lifecycle<TokenizedAsset, Never>>;

    #[test]
    fn test_add_asset_creates_new_asset() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        let validator = TokenizedAssetTestFramework::with(())
            .given_no_previous_events()
            .when(TokenizedAssetCommand::Add {
                underlying: underlying.clone(),
                token: token.clone(),
                network: network.clone(),
                vault,
            });

        let result = validator.inspect_result();

        match result {
            Ok(events) => {
                assert_eq!(events.len(), 1);

                match &events[0] {
                    TokenizedAssetEvent::Added {
                        underlying: event_underlying,
                        token: event_token,
                        network: event_network,
                        vault: event_vault,
                        added_at,
                    } => {
                        assert_eq!(event_underlying, &underlying);
                        assert_eq!(event_token, &token);
                        assert_eq!(event_network, &network);
                        assert_eq!(event_vault, &vault);
                        assert!(added_at.timestamp() > 0);
                    }
                }
            }
            Err(e) => panic!("Expected success, got error: {e}"),
        }
    }

    #[test]
    fn test_add_asset_when_already_added_is_idempotent() {
        let underlying = UnderlyingSymbol::new("AAPL");
        let token = TokenSymbol::new("tAAPL");
        let network = Network::new("base");
        let vault = address!("0x1234567890abcdef1234567890abcdef12345678");

        TokenizedAssetTestFramework::with(())
            .given(vec![TokenizedAssetEvent::Added {
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                vault: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
                added_at: chrono::Utc::now(),
            }])
            .when(TokenizedAssetCommand::Add {
                underlying,
                token,
                network,
                vault,
            })
            .then_expect_events(vec![]);
    }

    #[test]
    fn test_apply_asset_added_updates_state() {
        let mut asset: Lifecycle<TokenizedAsset, Never> = Lifecycle::default();

        assert!(matches!(asset, Lifecycle::Uninitialized));

        let underlying = UnderlyingSymbol::new("TSLA");
        let token = TokenSymbol::new("tTSLA");
        let network = Network::new("base");
        let vault = address!("0xfedcbafedcbafedcbafedcbafedcbafedcbafedc");
        let added_at = chrono::Utc::now();

        asset.apply(TokenizedAssetEvent::Added {
            underlying: underlying.clone(),
            token: token.clone(),
            network: network.clone(),
            vault,
            added_at,
        });

        let Lifecycle::Live(inner) = asset else {
            panic!("Expected Live state, got {asset:?}");
        };

        assert_eq!(inner.underlying, underlying);
        assert_eq!(inner.token, token);
        assert_eq!(inner.network, network);
        assert_eq!(inner.vault, vault);
        assert!(inner.enabled);
        assert_eq!(inner.added_at, added_at);
    }

    #[test]
    fn test_underlying_symbol_display() {
        let symbol = UnderlyingSymbol::new("AAPL");
        assert_eq!(format!("{symbol}"), "AAPL");
    }

    #[test]
    fn test_token_symbol_display() {
        let symbol = TokenSymbol::new("tAAPL");
        assert_eq!(format!("{symbol}"), "tAAPL");
    }

    #[test]
    fn test_network_display() {
        let network = Network::new("base");
        assert_eq!(format!("{network}"), "base");
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
    async fn test_list_enabled_assets_returns_only_enabled() {
        let pool = setup_test_db().await;

        let enabled_underlying = UnderlyingSymbol::new("AAPL");
        let enabled_asset = TokenizedAsset {
            underlying: enabled_underlying.clone(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::new("base"),
            vault: address!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            enabled: true,
            added_at: Utc::now(),
        };
        let enabled_lifecycle: Lifecycle<TokenizedAsset, Never> =
            Lifecycle::Live(enabled_asset.clone());

        let disabled_underlying = UnderlyingSymbol::new("TSLA");
        let disabled_asset = TokenizedAsset {
            underlying: disabled_underlying.clone(),
            token: TokenSymbol::new("tTSLA"),
            network: Network::new("base"),
            vault: address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
            enabled: false,
            added_at: Utc::now(),
        };
        let disabled_lifecycle: Lifecycle<TokenizedAsset, Never> =
            Lifecycle::Live(disabled_asset);

        let enabled_payload = serde_json::to_string(&enabled_lifecycle)
            .expect("Failed to serialize view");
        let disabled_payload = serde_json::to_string(&disabled_lifecycle)
            .expect("Failed to serialize view");

        sqlx::query!(
            r"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            enabled_underlying.0,
            enabled_payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert enabled view");

        sqlx::query!(
            r"
            INSERT INTO tokenized_asset_view (view_id, version, payload)
            VALUES (?, 1, ?)
            ",
            disabled_underlying.0,
            disabled_payload
        )
        .execute(&pool)
        .await
        .expect("Failed to insert disabled view");

        let result =
            list_enabled_assets(&pool).await.expect("Query should succeed");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].underlying, enabled_underlying);
        assert!(result[0].enabled);
    }

    #[tokio::test]
    async fn test_list_enabled_assets_returns_empty_when_none() {
        let pool = setup_test_db().await;

        let result =
            list_enabled_assets(&pool).await.expect("Query should succeed");

        assert!(result.is_empty());
    }
}
