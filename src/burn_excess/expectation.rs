//! Durable SQL index of Path B funding Transfers a stream expects but has not
//! yet proven and excluded.
//!
//! The live route runs with the redemption poller up, and the funding
//! Transfer is mined before its hash can be proven, so the poller may read it
//! first. While a stream is `AwaitingFunding`, the poller holds a Transfer
//! matching its `(network, vault, from, to, amount)` instead of opening a
//! Redemption for it. The index is a derived read model of `FundingExpected`
//! events: the engine dual-writes it, [`FundingExpectationReactor`] keeps it
//! current on live commits, and [`rebuild_funding_expectation_index`] resets
//! it from the event store on startup and store open.

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use sqlx::{Pool, Sqlite, SqlitePool};
use tracing::{debug, info};

use super::exclusion::{address_key, hash_key};
use super::{BurnExcess, BurnExcessEvent, ExcessBurnBind};
use crate::tokenized_asset::Network;

/// Persist the funding Transfer `bind` expects so the poller holds it.
///
/// Idempotent on the stream's deposit transaction hash.
pub(crate) async fn record_funding_expectation(
    pool: &Pool<Sqlite>,
    bind: &ExcessBurnBind,
    expected_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        INSERT INTO burn_excess_funding_expectations (
            deposit_tx_hash,
            network,
            vault,
            from_address,
            to_address,
            amount,
            expected_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(deposit_tx_hash) DO NOTHING
        ",
    )
    .bind(hash_key(bind.deposit_tx_hash))
    .bind(bind.network.as_str())
    .bind(address_key(bind.vault))
    .bind(address_key(bind.original_recipient))
    .bind(address_key(bind.issuer_wallet))
    .bind(bind.shares.to_string())
    .bind(expected_at.to_rfc3339())
    .execute(pool)
    .await?;

    Ok(())
}

/// Drop the expectation of the stream for `deposit_tx_hash`, releasing any
/// Transfer the poller holds for it.
pub(crate) async fn clear_funding_expectation(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        DELETE FROM burn_excess_funding_expectations
        WHERE deposit_tx_hash = ?
        ",
    )
    .bind(hash_key(deposit_tx_hash))
    .execute(pool)
    .await?;

    Ok(())
}

/// Whether a stream expects exactly this Transfer as its funding.
pub(crate) async fn is_expected_funding(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    from: Address,
    to: Address,
    amount: U256,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM burn_excess_funding_expectations
            WHERE network = ?
              AND vault = ?
              AND from_address = ?
              AND to_address = ?
              AND amount = ?
        )
        ",
    )
    .bind(network.as_str())
    .bind(address_key(vault))
    .bind(address_key(from))
    .bind(address_key(to))
    .bind(amount.to_string())
    .fetch_one(pool)
    .await
}

/// Reset the index to exactly the streams whose latest event is
/// `FundingExpected`, i.e. still `AwaitingFunding`.
///
/// Unlike the exclusion index, which only ever grows, an expectation must
/// disappear once its stream moves on: a stale row would hold an unrelated
/// Transfer of the same shape. So this deletes and re-inserts in one
/// transaction rather than only inserting.
pub(crate) async fn rebuild_funding_expectation_index(
    pool: &Pool<Sqlite>,
) -> Result<usize, RebuildFundingExpectationError> {
    let mut transaction = pool.begin().await?;

    sqlx::query("DELETE FROM burn_excess_funding_expectations")
        .execute(&mut *transaction)
        .await?;

    let rows = sqlx::query_as::<_, (String, String)>(
        "
        SELECT
            expected.aggregate_id,
            expected.payload
        FROM events AS expected
        WHERE expected.aggregate_type = 'BurnExcess'
          AND expected.event_type = ?
          AND NOT EXISTS (
              SELECT 1
              FROM events AS later
              WHERE later.aggregate_type = expected.aggregate_type
                AND later.aggregate_id = expected.aggregate_id
                AND later.sequence > expected.sequence
          )
        ",
    )
    .bind(BurnExcessEvent::FUNDING_EXPECTED)
    .fetch_all(&mut *transaction)
    .await?;

    let mut recorded = 0usize;
    for (aggregate_id, payload) in rows {
        let event: BurnExcessEvent = serde_json::from_str(&payload)?;
        let BurnExcessEvent::FundingExpected { bind, expected_at, .. } = event
        else {
            // Per-row, so DEBUG. Only reachable when the `event_type` column
            // disagrees with the stored payload.
            debug!(
                target: "burn_excess",
                %aggregate_id,
                "Skipping non-expectation payload under FundingExpected \
                 event_type"
            );
            continue;
        };

        sqlx::query(
            "
            INSERT INTO burn_excess_funding_expectations (
                deposit_tx_hash,
                network,
                vault,
                from_address,
                to_address,
                amount,
                expected_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ",
        )
        .bind(hash_key(bind.deposit_tx_hash))
        .bind(bind.network.as_str())
        .bind(address_key(bind.vault))
        .bind(address_key(bind.original_recipient))
        .bind(address_key(bind.issuer_wallet))
        .bind(bind.shares.to_string())
        .bind(expected_at.to_rfc3339())
        .execute(&mut *transaction)
        .await?;
        recorded = recorded.saturating_add(1);
    }

    transaction.commit().await?;

    if recorded > 0 {
        info!(
            target: "burn_excess",
            recorded,
            "Rebuilt funding expectation index from FundingExpected events"
        );
    }

    Ok(recorded)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RebuildFundingExpectationError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

deps!(FundingExpectationReactor, [BurnExcess]);

/// Writes an expectation into the SQL index when a stream records it, and
/// drops it when the stream closes. Clearing on `FundingExclusionRecorded` is
/// [`super::exclusion::FundingExclusionReactor`]'s job, after it writes the
/// exclusion, so the poller is never left with neither.
///
/// Live-only, like the exclusion reactor: startup and store open call
/// [`rebuild_funding_expectation_index`].
pub(crate) struct FundingExpectationReactor {
    pool: SqlitePool,
}

impl FundingExpectationReactor {
    pub(crate) const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn on_event(
        &self,
        deposit_tx_hash: B256,
        event: &BurnExcessEvent,
    ) -> Result<(), sqlx::Error> {
        match event {
            BurnExcessEvent::FundingExpected { bind, expected_at, .. } => {
                record_funding_expectation(&self.pool, bind, *expected_at)
                    .await?;
                info!(
                    target: "burn_excess",
                    %deposit_tx_hash,
                    from = %bind.original_recipient,
                    amount = %bind.shares,
                    "Recorded funding expectation for admin recovery"
                );
            }
            BurnExcessEvent::ExcessBurnClosed { .. } => {
                clear_funding_expectation(&self.pool, deposit_tx_hash).await?;
            }
            BurnExcessEvent::FundingExclusionRecorded { .. }
            | BurnExcessEvent::ExcessBurnIntended { .. }
            | BurnExcessEvent::ExcessBurnSubmitted { .. }
            | BurnExcessEvent::ExcessBurnCompleted { .. } => {}
        }
        Ok(())
    }
}

#[async_trait]
impl Reactor for FundingExpectationReactor {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (aggregate_id, domain_event) = event.into_inner();
        // `Error = Never` cannot fail the command; the engine dual-writes the
        // row before answering, and the rebuild heals a lost write.
        if let Err(error) =
            self.on_event(aggregate_id.deposit_tx_hash(), &domain_event).await
        {
            tracing::error!(
                target: "burn_excess",
                deposit_tx_hash = %aggregate_id,
                error = %error,
                "Failed to update funding expectation index row"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address, b256};
    use event_sorcery::StoreBuilder;

    use super::*;
    use crate::burn_excess::{
        BurnExcessCommand, BurnExcessId, FundingTransferId,
    };
    use crate::mint::IssuerMintRequestId;

    async fn pool() -> Pool<Sqlite> {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn bind(deposit_tx_hash: B256) -> ExcessBurnBind {
        ExcessBurnBind {
            issuer_request_id: IssuerMintRequestId::random(),
            deposit_tx_hash,
            receipt_id: U256::from(7u64),
            shares: U256::from(750_000_000_000_000_000u64),
            original_recipient: address!(
                "0xA9C16673F65AE808688cB18952AFE3d9658C808f"
            ),
            vault: address!("0x1111111111111111111111111111111111111111"),
            network: Network::Base,
            issuer_wallet: address!(
                "0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"
            ),
        }
    }

    async fn expects(pool: &Pool<Sqlite>, bind: &ExcessBurnBind) -> bool {
        is_expected_funding(
            pool,
            bind.network,
            bind.vault,
            bind.original_recipient,
            bind.issuer_wallet,
            bind.shares,
        )
        .await
        .unwrap()
    }

    /// The hold covers only the Transfer the stream will burn: a different
    /// sender, recipient, amount, or vault is an ordinary redemption.
    #[tokio::test]
    async fn an_expectation_matches_only_its_exact_transfer() {
        let pool = pool().await;
        let expected = bind(B256::random());
        record_funding_expectation(&pool, &expected, Utc::now()).await.unwrap();

        assert!(expects(&pool, &expected).await);
        for other in [
            ExcessBurnBind {
                original_recipient: Address::random(),
                ..expected.clone()
            },
            ExcessBurnBind {
                issuer_wallet: Address::random(),
                ..expected.clone()
            },
            ExcessBurnBind { shares: U256::from(1u64), ..expected.clone() },
            ExcessBurnBind { vault: Address::random(), ..expected.clone() },
            ExcessBurnBind { network: Network::Ethereum, ..expected.clone() },
        ] {
            assert!(!expects(&pool, &other).await, "{other:?}");
        }

        clear_funding_expectation(&pool, expected.deposit_tx_hash)
            .await
            .unwrap();
        assert!(!expects(&pool, &expected).await);
    }

    /// The rebuild must restore a lost row for a stream still awaiting its
    /// funding, and drop a stale row for a stream that moved on, which would
    /// otherwise hold an unrelated Transfer of the same shape forever.
    #[tokio::test]
    async fn rebuild_keeps_only_streams_still_awaiting_funding() {
        let pool = pool().await;
        let store = StoreBuilder::<BurnExcess>::new(pool.clone())
            .build(())
            .await
            .unwrap();

        let awaiting = bind(B256::random());
        let excluded = ExcessBurnBind {
            original_recipient: Address::random(),
            ..bind(B256::random())
        };
        for stream in [&awaiting, &excluded] {
            store
                .send(
                    &BurnExcessId::new(stream.deposit_tx_hash),
                    BurnExcessCommand::ExpectFunding {
                        bind: stream.clone(),
                        reason: "duplicate mint".into(),
                        incident_id: None,
                    },
                )
                .await
                .unwrap();
        }
        store
            .send(
                &BurnExcessId::new(excluded.deposit_tx_hash),
                BurnExcessCommand::RecordFundingExclusion {
                    bind: excluded.clone(),
                    funding_log_id: FundingTransferId {
                        network: excluded.network,
                        vault: excluded.vault,
                        tx_hash: b256!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                        log_index: 3,
                        from: excluded.original_recipient,
                        to: excluded.issuer_wallet,
                        amount: excluded.shares,
                    },
                    reason: "duplicate mint".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        // The store above runs no reactors: the awaiting stream's row is
        // missing, and a stale row is left for the excluded one.
        record_funding_expectation(&pool, &excluded, Utc::now()).await.unwrap();

        assert_eq!(rebuild_funding_expectation_index(&pool).await.unwrap(), 1);
        assert!(expects(&pool, &awaiting).await);
        assert!(!expects(&pool, &excluded).await);
    }
}
