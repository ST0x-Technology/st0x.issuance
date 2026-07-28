use alloy::primitives::{Address, B256, U256};
use chrono::{DateTime, Utc};
use event_sorcery::{Projection, ProjectionError};
use serde::{Deserialize, Serialize};
use sqlx::{Pool, Sqlite};
use uuid::Uuid;

use super::{
    ClientId, IssuerMintRequestId, Mint, MintFailureClassification, Network,
    Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
};
use crate::vault::{PreparedMintTx, TxId};

#[derive(Debug, thiserror::Error)]
pub(crate) enum MintViewError {
    #[error("Projection error: {0}")]
    Projection(#[from] ProjectionError<Mint>),
    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("Database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("mint view row id {view_id} is not a valid issuer request id")]
    InvalidViewId { view_id: String },
    #[error(
        "tokenization request {tokenization_request_id} matches {matches} mints; refusing ambiguous authorization routing"
    )]
    AmbiguousTokenizationRequest {
        tokenization_request_id: TokenizationRequestId,
        matches: usize,
    },
}

/// Query-oriented representation of a live `Mint` projection.
///
/// `NotFound` is the query-miss sentinel and is never a deserialize target.
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
    #[serde(alias = "TxIntended")]
    MintIntended {
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
        prepared_tx: PreparedMintTx,
    },
    #[serde(alias = "FireblocksSubmitted", alias = "TxSubmitted")]
    MintTxSubmitted {
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
        tx_id: TxId,
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
        /// Vault-direct audit data; `None` for orchestrator mints, whose
        /// receipts are custodied by the orchestrator.
        receipt_id: Option<U256>,
        /// The consumed authorization nonce; `None` for vault-direct mints.
        #[serde(default)]
        mint_nonce: Option<B256>,
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
        /// Typed failure classification mirrored from the aggregate state;
        /// defaults to `Unclassified` on rows written before it existed.
        #[serde(default)]
        classification: MintFailureClassification,
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
        /// Vault-direct audit data; `None` for orchestrator mints, whose
        /// receipts are custodied by the orchestrator.
        receipt_id: Option<U256>,
        /// The consumed authorization nonce; `None` for vault-direct mints.
        #[serde(default)]
        mint_nonce: Option<B256>,
        shares_minted: U256,
        gas_used: Option<u64>,
        block_number: u64,
        minted_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
    },
    Closed {
        issuer_request_id: IssuerMintRequestId,
        reason: String,
        #[serde(default)]
        acknowledged_unresolved_mint_tx_hash: Option<B256>,
        closed_at: DateTime<Utc>,
    },
}

impl MintView {
    fn from_mint(mint: Mint) -> Result<Self, serde_json::Error> {
        serde_json::from_value(serde_json::to_value(mint)?)
    }

    /// The asset this mint is for, when the state still carries it.
    pub(crate) const fn underlying(&self) -> Option<&UnderlyingSymbol> {
        match self {
            Self::Initiated { underlying, .. }
            | Self::JournalConfirmed { underlying, .. }
            | Self::JournalRejected { underlying, .. }
            | Self::Minting { underlying, .. }
            | Self::MintIntended { underlying, .. }
            | Self::MintTxSubmitted { underlying, .. }
            | Self::MintingFailed { underlying, .. }
            | Self::CallbackPending { underlying, .. } => Some(underlying),
            Self::NotFound | Self::Completed { .. } | Self::Closed { .. } => {
                None
            }
        }
    }

    #[cfg(test)]
    pub(crate) const fn state_name(&self) -> &'static str {
        match self {
            Self::NotFound => "NotFound",
            Self::Initiated { .. } => "Initiated",
            Self::JournalConfirmed { .. } => "JournalConfirmed",
            Self::JournalRejected { .. } => "JournalRejected",
            Self::Minting { .. } => "Minting",
            Self::MintIntended { .. } => "MintIntended",
            Self::MintTxSubmitted { .. } => "MintTxSubmitted",
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
    Projection::<Mint>::sqlite(pool.clone())
        .load(issuer_request_id)
        .await?
        .map(MintView::from_mint)
        .transpose()
        .map_err(Into::into)
}

/// Maps Alpaca's `tokenization_request_id` to our issuer id. The internal
/// mint-authorization call is keyed by the tokenization id — the only mint id
/// the liquidity bot shares with us; `IssuerMintRequestId` is minted here and
/// never leaves the Alpaca channel.
///
/// SQL only PRUNES candidate rows (served by the
/// `idx_mint_view_live_tokenization_request_id` expression index — the query
/// must keep the exact COALESCE expression the index is built on); every
/// candidate is then loaded through the type-safe projection and re-verified
/// against `Mint::tokenization_request_id()`, so no domain value is ever
/// parsed out of the view JSON here.
///
/// Nothing enforces tokenization-id uniqueness across mints, so matches are
/// partitioned by [`Mint::accepts_mint_authorization`]: exactly one
/// still-accepting mint wins regardless of stale same-id duplicates (a
/// completed mint from a reused id must never wedge the live one's
/// authorization), and a sole non-accepting match is still returned so late
/// deliveries keep their informative rejections (409 naming the state,
/// vault-direct 422). Anything genuinely ambiguous fails loudly rather than
/// routing an authorization to an arbitrary aggregate.
pub(crate) async fn find_issuer_id_by_tokenization_request_id(
    pool: &Pool<Sqlite>,
    tokenization_request_id: &TokenizationRequestId,
) -> Result<Option<IssuerMintRequestId>, MintViewError> {
    let candidate_ids: Vec<String> =
        sqlx::query_scalar(tokenization_id_candidate_query())
            .bind(&tokenization_request_id.0)
            .fetch_all(pool)
            .await?;

    let projection = Projection::<Mint>::sqlite(pool.clone());
    let mut accepting = Vec::new();
    let mut stale = Vec::new();
    for view_id in candidate_ids {
        let issuer_request_id = view_id
            .parse::<Uuid>()
            .map(IssuerMintRequestId::new)
            .map_err(|_| MintViewError::InvalidViewId { view_id })?;

        let Some(mint) = projection.load(&issuer_request_id).await? else {
            continue;
        };
        if mint.tokenization_request_id() != Some(tokenization_request_id) {
            continue;
        }

        if mint.accepts_mint_authorization() {
            accepting.push(issuer_request_id);
        } else {
            stale.push(issuer_request_id);
        }
    }

    // The ambiguity error reports every matching mint — accepting AND stale
    // — so the operator sees the true collision size (an `(accepting, _)`
    // binding would silently under-count when both kinds exist).
    let matches = accepting.len() + stale.len();
    match (accepting.len(), stale.len()) {
        (1, _) => Ok(accepting.pop()),
        (0, 0 | 1) => Ok(stale.pop()),
        _ => Err(MintViewError::AmbiguousTokenizationRequest {
            tokenization_request_id: tokenization_request_id.clone(),
            matches,
        }),
    }
}

/// The candidate-pruning query behind
/// [`find_issuer_id_by_tokenization_request_id`]. Its WHERE clause must
/// structurally match the COALESCE expression
/// `idx_mint_view_live_tokenization_request_id` is built on, or SQLite falls
/// back to a table scan — pinned by the query-plan test.
const fn tokenization_id_candidate_query() -> &'static str {
    "
    SELECT view_id
    FROM mint_view
    WHERE COALESCE(
        json_extract(payload, '$.Live.Initiated.tokenization_request_id'),
        json_extract(payload, '$.Live.JournalConfirmed.tokenization_request_id'),
        json_extract(payload, '$.Live.JournalRejected.tokenization_request_id'),
        json_extract(payload, '$.Live.Minting.tokenization_request_id'),
        json_extract(payload, '$.Live.TxIntended.tokenization_request_id'),
        json_extract(payload, '$.Live.TxSubmitted.tokenization_request_id'),
        json_extract(payload, '$.Live.CallbackPending.tokenization_request_id'),
        json_extract(payload, '$.Live.MintingFailed.tokenization_request_id'),
        json_extract(payload, '$.Live.Completed.tokenization_request_id')
    ) = ?
    "
}

/// Finds all mints that need recovery (not in terminal states).
///
/// Returns mints in JournalConfirmed, Minting, MintingFailed, or CallbackPending states.
pub(crate) async fn find_all_recoverable_mints(
    pool: &Pool<Sqlite>,
) -> Result<Vec<(IssuerMintRequestId, MintView)>, MintViewError> {
    Projection::<Mint>::sqlite(pool.clone())
        .load_all()
        .await?
        .into_iter()
        .map(|(issuer_request_id, mint)| {
            MintView::from_mint(mint)
                .map(|view| (issuer_request_id, view))
                .map_err(Into::into)
        })
        .filter(|result| {
            matches!(
                result,
                Ok((
                    _,
                    MintView::JournalConfirmed { .. }
                        | MintView::Minting { .. }
                        | MintView::MintIntended { .. }
                        | MintView::MintTxSubmitted { .. }
                        | MintView::MintingFailed { .. }
                        | MintView::CallbackPending { .. }
                ))
            ) || result.is_err()
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
    Projection::<Mint>::sqlite(pool.clone())
        .load_all()
        .await?
        .into_iter()
        .map(|(issuer_request_id, mint)| {
            MintView::from_mint(mint)
                .map(|view| (issuer_request_id, view))
                .map_err(Into::into)
        })
        .filter(|result| {
            matches!(
                result,
                Ok((
                    _,
                    MintView::Initiated { .. }
                        | MintView::JournalConfirmed { .. }
                        | MintView::JournalRejected { .. }
                        | MintView::Minting { .. }
                        | MintView::MintIntended { .. }
                        | MintView::MintTxSubmitted { .. }
                        | MintView::CallbackPending { .. }
                        | MintView::MintingFailed { .. }
                ))
            ) || result.is_err()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{address, b256, uint};
    use event_sorcery::StoreBuilder;
    use rust_decimal::Decimal;
    use sqlx::{Pool, Sqlite, sqlite::SqlitePoolOptions};

    use super::*;
    use crate::config::VaultMode;
    use crate::mint::{Mint, MintCommand};

    /// Inserts a `Lifecycle<Mint>`-shaped row into `mint_view` for a given
    /// query view. The adjusted variants reproduce the production `Mint`
    /// serialization rather than the query-oriented `MintView` names.
    async fn insert_mint_view(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerMintRequestId,
        view: &MintView,
    ) {
        let view_id = issuer_request_id.to_string();
        let mut live = serde_json::to_value(view).unwrap();
        let variants = live.as_object_mut().unwrap();

        if let Some(mut fields) = variants.remove("MintIntended") {
            variants.insert("TxIntended".to_owned(), fields.take());
        }

        if let Some(mut fields) = variants.remove("MintTxSubmitted") {
            fields
                .as_object_mut()
                .unwrap()
                .insert("prepared_tx".to_owned(), serde_json::Value::Null);
            variants.insert("TxSubmitted".to_owned(), fields.take());
        }

        if let Some(fields) = variants.get_mut("MintingFailed") {
            let fields = fields.as_object_mut().unwrap();
            let predecessor = serde_json::json!({
                "JournalConfirmed": {
                    "issuer_request_id": fields["issuer_request_id"],
                    "tokenization_request_id": fields["tokenization_request_id"],
                    "quantity": fields["quantity"],
                    "underlying": fields["underlying"],
                    "token": fields["token"],
                    "network": fields["network"],
                    "client_id": fields["client_id"],
                    "wallet": fields["wallet"],
                    "initiated_at": fields["initiated_at"],
                    "journal_confirmed_at": fields["journal_confirmed_at"],
                }
            });
            fields.insert("attempts".to_owned(), serde_json::json!(1));
            fields.insert("failed_from".to_owned(), predecessor);
        }

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
            .build(())
            .await
            .expect("Failed to build mint store");

        let issuer_request_id = IssuerMintRequestId::random();

        store
            .send(
                &issuer_request_id,
                MintCommand::Initiate {
                    mint_mode: VaultMode::VaultDirect,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: TokenizationRequestId::new(
                        "alp-888",
                    ),
                    quantity: Quantity::new(Decimal::from(50)),
                    underlying: UnderlyingSymbol::new("TSLA").unwrap(),
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
        assert_eq!(found_underlying, UnderlyingSymbol::new("TSLA").unwrap());
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

    fn initiate_command(
        issuer_request_id: &IssuerMintRequestId,
        tokenization_request_id: &str,
    ) -> MintCommand {
        MintCommand::Initiate {
            mint_mode: VaultMode::VaultDirect,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: TokenizationRequestId::new(
                tokenization_request_id,
            ),
            quantity: Quantity::new(Decimal::from(50)),
            underlying: UnderlyingSymbol::new("TSLA").unwrap(),
            token: TokenSymbol::new("tTSLA"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
        }
    }

    /// The tokenization-id lookup prunes candidates in SQL across every live
    /// state's payload path, then confirms through the projection — so it
    /// must keep finding a mint after it advances past `Initiated`, and must
    /// ignore other mints' ids.
    #[tokio::test]
    async fn find_issuer_id_by_tokenization_id_follows_state_changes() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Mint>::new(pool.clone())
            .build(())
            .await
            .expect("Failed to build mint store");

        let issuer_request_id = IssuerMintRequestId::random();
        store
            .send(
                &issuer_request_id,
                initiate_command(&issuer_request_id, "alp-tok-lookup"),
            )
            .await
            .expect("Failed to initiate mint");
        let other_id = IssuerMintRequestId::random();
        store
            .send(&other_id, initiate_command(&other_id, "alp-tok-other"))
            .await
            .expect("Failed to initiate other mint");

        let found = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-lookup"),
        )
        .await
        .expect("lookup must succeed");
        assert_eq!(found, Some(issuer_request_id.clone()));

        // Advance past Initiated: the lookup must match the
        // JournalConfirmed payload path too.
        store
            .send(
                &issuer_request_id,
                MintCommand::ConfirmJournal {
                    issuer_request_id: issuer_request_id.clone(),
                },
            )
            .await
            .expect("Failed to confirm journal");
        let found = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-lookup"),
        )
        .await
        .expect("lookup must succeed");
        assert_eq!(found, Some(issuer_request_id));

        let missing = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-unknown"),
        )
        .await
        .expect("lookup must succeed");
        assert_eq!(missing, None);
    }

    /// The candidate query must be served by
    /// `idx_mint_view_live_tokenization_request_id` — SQLite only uses an
    /// expression index when the WHERE clause structurally matches the
    /// indexed expression, so any drift between the migration and the query
    /// silently regresses to a table scan. Pin the query plan.
    #[tokio::test]
    async fn find_issuer_id_by_tokenization_id_lookup_uses_expression_index() {
        let pool = setup_test_db().await;

        let plan: Vec<(i64, i64, i64, String)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "EXPLAIN QUERY PLAN {}",
                super::tokenization_id_candidate_query()
            )))
            .bind("alp-tok-plan")
            .fetch_all(&pool)
            .await
            .expect("query plan must be explainable");

        assert!(
            plan.iter().any(|(_, _, _, detail)| detail
                .contains("idx_mint_view_live_tokenization_request_id")),
            "the lookup must be served by the expression index, got plan: \
             {plan:?}"
        );
    }

    /// A stale terminal mint from a reused tokenization id must never wedge
    /// the live one: the lookup prefers the single still-accepting mint, and
    /// still returns a sole stale match so late deliveries keep their
    /// informative rejections.
    #[tokio::test]
    async fn find_issuer_id_by_tokenization_id_prefers_live_over_stale() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Mint>::new(pool.clone())
            .build(())
            .await
            .expect("Failed to build mint store");

        // The stale duplicate: initiated with the same tokenization id, then
        // journal-rejected — terminal, can never accept an authorization.
        let stale_id = IssuerMintRequestId::random();
        store
            .send(&stale_id, initiate_command(&stale_id, "alp-tok-reused"))
            .await
            .expect("Failed to initiate stale mint");
        store
            .send(
                &stale_id,
                MintCommand::RejectJournal {
                    issuer_request_id: stale_id.clone(),
                    reason: "journal failed".to_string(),
                },
            )
            .await
            .expect("Failed to reject journal");

        // Only the stale mint exists yet: it must still be returned so a
        // late delivery gets its informative rejection instead of a 404.
        let found = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-reused"),
        )
        .await
        .expect("lookup must succeed");
        assert_eq!(found, Some(stale_id.clone()));

        // The live mint reusing the id: it must win over the stale one.
        let live_id = IssuerMintRequestId::random();
        store
            .send(&live_id, initiate_command(&live_id, "alp-tok-reused"))
            .await
            .expect("Failed to initiate live mint");

        let found = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-reused"),
        )
        .await
        .expect("lookup must succeed despite the stale duplicate");
        assert_eq!(
            found,
            Some(live_id),
            "the live mint must win over the stale terminal duplicate"
        );
    }

    /// Nothing enforces tokenization-id uniqueness across mints, so an
    /// ambiguous id must fail loudly instead of routing an authorization to
    /// an arbitrary aggregate.
    #[tokio::test]
    async fn find_issuer_id_by_tokenization_id_rejects_ambiguous_matches() {
        let pool = setup_test_db().await;
        let (store, _projection) = StoreBuilder::<Mint>::new(pool.clone())
            .build(())
            .await
            .expect("Failed to build mint store");

        for _ in 0..2 {
            let issuer_request_id = IssuerMintRequestId::random();
            store
                .send(
                    &issuer_request_id,
                    initiate_command(&issuer_request_id, "alp-tok-dup"),
                )
                .await
                .expect("Failed to initiate mint");
        }

        let result = find_issuer_id_by_tokenization_request_id(
            &pool,
            &TokenizationRequestId::new("alp-tok-dup"),
        )
        .await;

        assert!(
            matches!(
                result,
                Err(MintViewError::AmbiguousTokenizationRequest {
                    matches: 2,
                    ..
                })
            ),
            "a duplicated tokenization id must be rejected, got {result:?}"
        );
    }

    fn test_mint_fields() -> TestMintFields {
        TestMintFields {
            issuer_request_id: IssuerMintRequestId::random(),
            tokenization_request_id: TokenizationRequestId::new("alp-1"),
            quantity: Quantity::new(Decimal::from(100)),
            underlying: UnderlyingSymbol::new("AAPL").unwrap(),
            token: TokenSymbol::new("tAAPL"),
            network: Network::Base,
            client_id: ClientId::new(),
            wallet: address!("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"),
            initiated_at: Utc::now(),
        }
    }

    /// Seeds one view per recoverable state. `find_all_recoverable_mints` and
    /// `find_stuck` both match `TxSubmitted`, so it is seeded here
    /// alongside the other recoverable states.
    async fn seed_recoverable_mint_views(
        pool: &Pool<Sqlite>,
    ) -> Vec<IssuerMintRequestId> {
        let now = Utc::now();
        let fields: Vec<_> = (0..6).map(|_| test_mint_fields()).collect();

        let views: Vec<MintView> = vec![
            MintView::JournalConfirmed {
                issuer_request_id: fields[0].issuer_request_id.clone(),
                tokenization_request_id: fields[0]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[0].quantity.clone(),
                underlying: fields[0].underlying.clone(),
                token: fields[0].token.clone(),
                network: fields[0].network,
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
                network: fields[1].network,
                client_id: fields[1].client_id,
                wallet: fields[1].wallet,
                initiated_at: fields[1].initiated_at,
                journal_confirmed_at: now,
                minting_started_at: now,
            },
            MintView::MintIntended {
                issuer_request_id: fields[2].issuer_request_id.clone(),
                tokenization_request_id: fields[2]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[2].quantity.clone(),
                underlying: fields[2].underlying.clone(),
                token: fields[2].token.clone(),
                network: fields[2].network,
                client_id: fields[2].client_id,
                wallet: fields[2].wallet,
                initiated_at: fields[2].initiated_at,
                journal_confirmed_at: now,
                minting_started_at: now,
                prepared_tx: PreparedMintTx::default(),
            },
            MintView::MintTxSubmitted {
                issuer_request_id: fields[3].issuer_request_id.clone(),
                tokenization_request_id: fields[3]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[3].quantity.clone(),
                underlying: fields[3].underlying.clone(),
                token: fields[3].token.clone(),
                network: fields[3].network,
                client_id: fields[3].client_id,
                wallet: fields[3].wallet,
                initiated_at: fields[3].initiated_at,
                journal_confirmed_at: now,
                minting_started_at: now,
                external_tx_id: "mint-base".to_string(),
                tx_id: TxId::Legacy("fb-1".to_string()),
            },
            MintView::MintingFailed {
                issuer_request_id: fields[4].issuer_request_id.clone(),
                tokenization_request_id: fields[4]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[4].quantity.clone(),
                underlying: fields[4].underlying.clone(),
                token: fields[4].token.clone(),
                network: fields[4].network,
                client_id: fields[4].client_id,
                wallet: fields[4].wallet,
                initiated_at: fields[4].initiated_at,
                journal_confirmed_at: now,
                error: "Transaction reverted".to_string(),
                failed_at: now,
                classification: MintFailureClassification::Unclassified,
            },
            MintView::CallbackPending {
                issuer_request_id: fields[5].issuer_request_id.clone(),
                tokenization_request_id: fields[5]
                    .tokenization_request_id
                    .clone(),
                quantity: fields[5].quantity.clone(),
                underlying: fields[5].underlying.clone(),
                token: fields[5].token.clone(),
                network: fields[5].network,
                client_id: fields[5].client_id,
                wallet: fields[5].wallet,
                initiated_at: fields[5].initiated_at,
                journal_confirmed_at: now,
                tx_hash: b256!(
                    "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
                ),
                receipt_id: Some(uint!(1_U256)),
                mint_nonce: None,
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
            receipt_id: Some(uint!(2_U256)),
            mint_nonce: None,
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

        assert_eq!(results.len(), 6, "Expected 6 recoverable mints");

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
        assert!(state_names.contains(&"MintIntended"));
        assert!(state_names.contains(&"MintTxSubmitted"));
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
        // TxSubmitted, MintingFailed, CallbackPending).
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
            receipt_id: Some(uint!(2_U256)),
            mint_nonce: None,
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
                acknowledged_unresolved_mint_tx_hash: None,
                closed_at: now,
            },
        )
        .await;

        let results = find_stuck(&pool).await.unwrap();
        let result_ids: Vec<_> =
            results.iter().map(|(id, _)| id.clone()).collect();

        assert_eq!(
            results.len(),
            8,
            "Expected 8 non-terminal mints, got ids: {result_ids:?}"
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
        assert!(state_names.contains(&"MintIntended"));
        assert!(state_names.contains(&"MintTxSubmitted"));
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
