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
//! it from the event store at service startup, before the pollers spawn.

use alloy::primitives::{Address, B256, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use sqlx::{Executor, Pool, Sqlite, SqlitePool};
use tracing::{debug, info};

use super::exclusion::{address_key, hash_key, log_index_key};
use super::{BurnExcess, BurnExcessEvent, ExcessBurnBind, FundingTransferId};
use crate::tokenized_asset::Network;

/// Persist the funding Transfer `bind` expects so the poller holds it.
///
/// Idempotent on the stream's deposit transaction hash.
pub(crate) async fn record_funding_expectation(
    executor: impl Executor<'_, Database = Sqlite>,
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
    .execute(executor)
    .await?;

    Ok(())
}

/// Drop the expectation of the stream for `deposit_tx_hash`, releasing any
/// Transfer the poller holds for it. Returns whether a row was dropped.
pub(crate) async fn clear_funding_expectation(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(
        "
        DELETE FROM burn_excess_funding_expectations
        WHERE deposit_tx_hash = ?
        ",
    )
    .bind(hash_key(deposit_tx_hash))
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
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

/// Whether a stream other than `deposit_tx_hash` already expects its funding
/// Transfer on this vault. The issuer wallet can hold only one stream's
/// funding on a vault, because each `external` run needs its exact share
/// balance there.
pub(crate) async fn has_other_funding_expectation(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    deposit_tx_hash: B256,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM burn_excess_funding_expectations
            WHERE network = ?
              AND vault = ?
              AND deposit_tx_hash != ?
        )
        ",
    )
    .bind(network.as_str())
    .bind(address_key(vault))
    .bind(hash_key(deposit_tx_hash))
    .fetch_one(pool)
    .await
}

/// How the redemption poller must treat one Transfer log with respect to
/// Path B burn-excess funding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FundingTransferStatus {
    /// A stream proved and excluded this exact log: skip it.
    Excluded,
    /// A stream expects a Transfer of this shape and has not excluded it yet:
    /// hold it.
    Expected,
    /// No burn-excess stream claims it: an ordinary Transfer.
    Unrelated,
}

/// Classifies one Transfer log against the exclusion and expectation indexes
/// in a single statement, so it reads both from one SQLite snapshot. The
/// external route writes the exclusion and then clears the expectation; two
/// separate reads could fall between those writes, see neither, and let the
/// poller redeem the funding Transfer.
pub(crate) async fn classify_funding_transfer(
    pool: &Pool<Sqlite>,
    transfer: &FundingTransferId,
) -> Result<FundingTransferStatus, sqlx::Error> {
    let (excluded, expected) = sqlx::query_as::<_, (bool, bool)>(
        "
        SELECT
            EXISTS (
                SELECT 1
                FROM burn_excess_funding_exclusions
                WHERE network = ?
                  AND vault = ?
                  AND tx_hash = ?
                  AND log_index = ?
            ),
            EXISTS (
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
    .bind(transfer.network.as_str())
    .bind(address_key(transfer.vault))
    .bind(hash_key(transfer.tx_hash))
    .bind(log_index_key(transfer.log_index)?)
    .bind(transfer.network.as_str())
    .bind(address_key(transfer.vault))
    .bind(address_key(transfer.from))
    .bind(address_key(transfer.to))
    .bind(transfer.amount.to_string())
    .fetch_one(pool)
    .await?;

    Ok(if excluded {
        FundingTransferStatus::Excluded
    } else if expected {
        FundingTransferStatus::Expected
    } else {
        FundingTransferStatus::Unrelated
    })
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

        record_funding_expectation(&mut *transaction, &bind, expected_at)
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
/// Live-only, like the exclusion reactor: service startup calls
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
    use crate::burn_excess::exclusion::record_funding_exclusion;
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

    /// Each state the external route's handoff passes through (expectation
    /// only, then exclusion written, then expectation cleared) classifies the
    /// funding log as held or skipped, never as an ordinary Transfer, and the
    /// exclusion wins once it exists. That both indexes are read from one
    /// snapshot rests on `classify_funding_transfer` being one statement.
    #[tokio::test]
    async fn every_handoff_state_holds_or_skips_the_funding_log() {
        let pool = pool().await;
        let bind = bind(B256::random());
        let funding = FundingTransferId {
            network: bind.network,
            vault: bind.vault,
            tx_hash: B256::random(),
            log_index: 3,
            from: bind.original_recipient,
            to: bind.issuer_wallet,
            amount: bind.shares,
        };
        let classify = async |pool: &Pool<Sqlite>| {
            classify_funding_transfer(pool, &funding).await.unwrap()
        };

        assert_eq!(classify(&pool).await, FundingTransferStatus::Unrelated);

        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();
        assert_eq!(classify(&pool).await, FundingTransferStatus::Expected);

        record_funding_exclusion(
            &pool,
            &funding,
            bind.deposit_tx_hash,
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(classify(&pool).await, FundingTransferStatus::Excluded);

        clear_funding_expectation(&pool, bind.deposit_tx_hash).await.unwrap();
        assert_eq!(classify(&pool).await, FundingTransferStatus::Excluded);
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
