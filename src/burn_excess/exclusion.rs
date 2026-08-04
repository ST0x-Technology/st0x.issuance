//! Durable SQL index of Path B funding Transfer identities the poller must skip.
//!
//! The index is a derived read model of `FundingExclusionRecorded` events. Live
//! dispatch is best-effort (reactor `Error = Never`); durability for the CLI run
//! is the engine dual-write. Custom reactors on `Materialized = Nil` aggregates
//! are **not** catch_up'd by `StoreBuilder` — only live commits call
//! [`Reactor::react`]. Startup and CLI store open must call
//! [`rebuild_funding_exclusion_index`] so a restored DB (or a dual-write gap)
//! cannot leave the poller free to open a `Redemption` for an excluded funding
//! Transfer.

use alloy::primitives::{Address, B256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use sqlx::{Pool, Sqlite, SqlitePool};
use tracing::{debug, info};

use super::{BurnExcess, BurnExcessEvent, FundingTransferId};
use crate::tokenized_asset::Network;

fn address_key(address: Address) -> String {
    address.to_string().to_ascii_lowercase()
}

fn hash_key(hash: B256) -> String {
    format!("{hash:#x}")
}

fn log_index_key(log_index: u64) -> Result<i64, sqlx::Error> {
    // Encode, not Decode: this converts a value on its way into a bind
    // parameter, so a "decode" error would point an operator at the read path.
    i64::try_from(log_index).map_err(|error| {
        sqlx::Error::Encode(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )))
    })
}

/// Persist a verified funding log identity so the redemption poller skips it.
///
/// Idempotent on the primary key `(network, vault, tx_hash, log_index)`.
pub(crate) async fn record_funding_exclusion(
    pool: &Pool<Sqlite>,
    funding: &FundingTransferId,
    deposit_tx_hash: B256,
    excluded_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let network = funding.network.as_str();
    let vault = address_key(funding.vault);
    let tx_hash = hash_key(funding.tx_hash);
    let from_address = address_key(funding.from);
    let to_address = address_key(funding.to);
    let amount = funding.amount.to_string();
    let deposit = hash_key(deposit_tx_hash);
    let excluded_at = excluded_at.to_rfc3339();
    let log_index = log_index_key(funding.log_index)?;

    sqlx::query(
        "
        INSERT INTO burn_excess_funding_exclusions (
            network,
            vault,
            tx_hash,
            log_index,
            from_address,
            to_address,
            amount,
            deposit_tx_hash,
            excluded_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(network, vault, tx_hash, log_index) DO NOTHING
        ",
    )
    .bind(network)
    .bind(vault)
    .bind(tx_hash)
    .bind(log_index)
    .bind(from_address)
    .bind(to_address)
    .bind(amount)
    .bind(deposit)
    .bind(excluded_at)
    .execute(pool)
    .await?;

    Ok(())
}

/// Whether this exact Transfer log was recorded as an admin recovery funding move.
pub(crate) async fn is_excluded_funding_log(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    tx_hash: B256,
    log_index: u64,
) -> Result<bool, sqlx::Error> {
    let network = network.as_str();
    let vault = address_key(vault);
    let tx_hash = hash_key(tx_hash);
    let log_index = log_index_key(log_index)?;

    let exists = sqlx::query_scalar::<_, bool>(
        "
        SELECT EXISTS (
            SELECT 1
            FROM burn_excess_funding_exclusions
            WHERE network = ?
              AND vault = ?
              AND tx_hash = ?
              AND log_index = ?
        )
        ",
    )
    .bind(network)
    .bind(vault)
    .bind(tx_hash)
    .bind(log_index)
    .fetch_one(pool)
    .await?;

    Ok(exists)
}

/// Re-insert every `FundingExclusionRecorded` row from the event store.
///
/// Idempotent (`ON CONFLICT DO NOTHING`). Call on main-service startup and
/// before the burn-excess CLI opens its store. This is the recovery path when
/// the SQL index was lost (backup restore, table recreate) while events remain.
pub(crate) async fn rebuild_funding_exclusion_index(
    pool: &Pool<Sqlite>,
) -> Result<usize, RebuildFundingExclusionError> {
    let rows = sqlx::query_as::<_, (String, String)>(
        "
        SELECT
            aggregate_id,
            payload
        FROM events
        WHERE aggregate_type = 'BurnExcess'
          AND event_type = ?
        ORDER BY sequence
        ",
    )
    .bind(BurnExcessEvent::FUNDING_EXCLUSION_RECORDED)
    .fetch_all(pool)
    .await?;

    let mut recorded = 0usize;
    for (aggregate_id, payload) in rows {
        let deposit_tx_hash =
            aggregate_id.parse::<B256>().map_err(|error| {
                RebuildFundingExclusionError::AggregateId {
                    aggregate_id: aggregate_id.clone(),
                    message: error.to_string(),
                }
            })?;
        let event: BurnExcessEvent = serde_json::from_str(&payload)?;
        let BurnExcessEvent::FundingExclusionRecorded {
            funding_log_id,
            excluded_at,
            ..
        } = event
        else {
            // Per-row, so DEBUG; the summary after the loop is the INFO. Only
            // reachable when the `event_type` column disagrees with the stored
            // payload, since any other payload shape fails deserialization
            // above.
            debug!(
                target: "burn_excess",
                %aggregate_id,
                "Skipping non-exclusion payload under FundingExclusionRecorded \
                 event_type"
            );
            continue;
        };
        record_funding_exclusion(
            pool,
            &funding_log_id,
            deposit_tx_hash,
            excluded_at,
        )
        .await?;
        recorded = recorded.saturating_add(1);
    }

    if recorded > 0 {
        info!(
            target: "burn_excess",
            recorded,
            "Rebuilt funding exclusion index from FundingExclusionRecorded events"
        );
    }

    Ok(recorded)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RebuildFundingExclusionError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(
        "invalid BurnExcess aggregate_id {aggregate_id} while rebuilding \
         funding exclusion index: {message}"
    )]
    AggregateId { aggregate_id: String, message: String },
}

deps!(FundingExclusionReactor, [BurnExcess]);

/// Writes Path B funding identities into the SQL exclusion index when the
/// aggregate records them, so the transfer poller can skip without scanning
/// the event store.
///
/// Live-only: `StoreBuilder` does not catch_up custom reactors for
/// `Materialized = Nil` entities. Use [`rebuild_funding_exclusion_index`] on
/// startup for durability across process restarts and DB restores.
pub(crate) struct FundingExclusionReactor {
    pool: SqlitePool,
}

impl FundingExclusionReactor {
    pub(crate) const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn on_event(
        &self,
        deposit_tx_hash: B256,
        event: &BurnExcessEvent,
    ) -> Result<(), sqlx::Error> {
        if let BurnExcessEvent::FundingExclusionRecorded {
            funding_log_id,
            excluded_at,
            ..
        } = event
        {
            record_funding_exclusion(
                &self.pool,
                funding_log_id,
                deposit_tx_hash,
                *excluded_at,
            )
            .await?;
            info!(
                target: "burn_excess",
                %deposit_tx_hash,
                funding_tx = %format!("{:#x}", funding_log_id.tx_hash),
                log_index = funding_log_id.log_index,
                "Recorded funding exclusion for admin recovery"
            );
        }
        Ok(())
    }
}

#[async_trait]
impl Reactor for FundingExclusionReactor {
    type Error = Never;

    async fn react(
        &self,
        event: <Self::Dependencies as EntityList>::Event,
    ) -> Result<(), Self::Error> {
        let (aggregate_id, domain_event) = event.into_inner();
        // Reactor Error = Never cannot surface DB failures to the command path.
        // Engine dual-write owns durability for the active CLI run; resume
        // re-writes the row (ensure_path_b_exclusion_indexed). Live dispatch
        // failures and lost index tables are healed by
        // rebuild_funding_exclusion_index on next startup / CLI open.
        if let Err(error) =
            self.on_event(aggregate_id.deposit_tx_hash(), &domain_event).await
        {
            tracing::error!(
                target: "burn_excess",
                deposit_tx_hash = %aggregate_id,
                error = %error,
                "Failed to persist funding exclusion index row"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, U256, address, b256};
    use chrono::Utc;

    use super::*;
    use crate::tokenized_asset::Network;

    async fn pool() -> Pool<Sqlite> {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn sample_funding() -> FundingTransferId {
        FundingTransferId {
            network: Network::Base,
            vault: address!("0x1111111111111111111111111111111111111111"),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            log_index: 3,
            from: address!("0xA9C16673F65AE808688cB18952AFE3d9658C808f"),
            to: address!("0x3d0CD66EFA66c05d86c3d4316B03eAE87ab9E8aE"),
            amount: U256::from(750_000_000_000_000_000u64),
        }
    }

    fn sample_deposit() -> B256 {
        b256!(
            "0x1bb6afc590e58095099373a8fea2242017b31acc7940bcd0d6b68820ebeb8ebd"
        )
    }

    #[tokio::test]
    async fn record_and_lookup_exclusion_is_exact() {
        let pool = pool().await;
        let funding = sample_funding();
        let deposit = sample_deposit();

        record_funding_exclusion(&pool, &funding, deposit, Utc::now())
            .await
            .unwrap();

        assert!(
            is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap()
        );
        assert!(
            !is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index + 1,
            )
            .await
            .unwrap()
        );
        assert!(
            !is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                B256::ZERO,
                funding.log_index,
            )
            .await
            .unwrap()
        );
    }

    #[tokio::test]
    async fn rebuild_restores_index_from_events_after_row_loss() {
        let pool = pool().await;
        let funding = sample_funding();
        let deposit = sample_deposit();
        let excluded_at = Utc::now();

        let event = BurnExcessEvent::FundingExclusionRecorded {
            bind: super::super::ExcessBurnBind {
                issuer_request_id: crate::mint::IssuerMintRequestId::new(
                    uuid::Uuid::parse_str(
                        "d3042b2f-4845-4acd-9a67-92d743e4e58c",
                    )
                    .unwrap(),
                ),
                deposit_tx_hash: deposit,
                receipt_id: U256::from(7u64),
                shares: funding.amount,
                original_recipient: funding.from,
                vault: funding.vault,
                network: funding.network,
                issuer_wallet: funding.to,
            },
            funding_log_id: funding.clone(),
            reason: "duplicate mint".into(),
            incident_id: None,
            excluded_at,
        };
        let payload = serde_json::to_string(&event).unwrap();
        let aggregate_id = format!("{deposit:#x}");

        sqlx::query(
            "
            INSERT INTO events (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            )
            VALUES (
                'BurnExcess',
                ?,
                1,
                'BurnExcessEvent::FundingExclusionRecorded',
                '1.0',
                ?,
                '{}'
            )
            ",
        )
        .bind(&aggregate_id)
        .bind(&payload)
        .execute(&pool)
        .await
        .unwrap();

        assert!(
            !is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap(),
            "index empty before rebuild"
        );

        let recorded = rebuild_funding_exclusion_index(&pool).await.unwrap();
        assert_eq!(recorded, 1);
        assert!(
            is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap()
        );

        // Idempotent.
        assert_eq!(rebuild_funding_exclusion_index(&pool).await.unwrap(), 1);
        assert!(
            is_excluded_funding_log(
                &pool,
                funding.network,
                funding.vault,
                funding.tx_hash,
                funding.log_index,
            )
            .await
            .unwrap()
        );
    }
}
