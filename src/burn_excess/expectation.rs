//! Durable SQL index of Path B funding Transfers a live stream expects.
//!
//! The live route runs with the redemption poller up, and the funding
//! Transfer is mined before its hash can be proven, so the poller may read it
//! first. From `FundingExpected` until the stream completes or closes, the
//! poller holds a Transfer matching its `(network, vault, from, to, amount)`
//! instead of opening a Redemption for it, unless the stream has excluded that
//! exact log. The index is a derived read model of the stream's events: the
//! engine dual-writes it, [`FundingExpectationReactor`] keeps it current on
//! live commits, and [`rebuild_funding_expectation_index`] resets it from the
//! event store at service startup, before the pollers spawn.

use alloy::primitives::{Address, B256, U256};
use alloy::rpc::types::Log;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use event_sorcery::{EntityList, Never, Reactor, deps};
use sqlx::{Executor, Pool, Sqlite, SqlitePool, Transaction};
use tracing::{debug, info};

use super::exclusion::{
    address_key, hash_key, is_excluded_funding_log, log_index_key,
};
use super::{
    BurnExcess, BurnExcessEvent, ExcessBurnBind, FundingTransferId,
    HeldTransferRedemption,
};
use crate::bindings::OffchainAssetReceiptVault;
use crate::poll_checkpoint::{CheckpointError, rewind_transfer_poll_before};
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

/// Persist exact held-redemption attribution from the event stream.
///
/// Rows remain until detection persists the attribution in the Redemption
/// stream. Until then they let the poller scan a repointed vault and admit the
/// exact log even if the AP wallet is subsequently unlinked.
pub(crate) async fn record_held_redemptions(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
    held_redemptions: &[HeldTransferRedemption],
) -> Result<(), FundingTransferIndexError> {
    for held in held_redemptions {
        record_held_redemption(pool, deposit_tx_hash, held).await?;
        rewind_transfer_poll_before(
            pool,
            held.transfer.network,
            held.transfer.vault,
            held.block_number,
        )
        .await?;
    }
    Ok(())
}

/// Refuse a plan whose exact held log already has different durable
/// attribution. Call before appending the anchor event so a conflict cannot
/// poison event replay.
pub(crate) async fn ensure_held_redemptions_compatible(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
    held_redemptions: &[HeldTransferRedemption],
) -> Result<(), FundingTransferIndexError> {
    let anchored_events = sqlx::query_as::<_, (String, String)>(
        "
        SELECT aggregate_id, payload
        FROM events
        WHERE aggregate_type = 'BurnExcess'
          AND event_type = ?
        ",
    )
    .bind(BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED)
    .fetch_all(pool)
    .await?;
    for (aggregate_id, payload) in anchored_events {
        let event: BurnExcessEvent = serde_json::from_str(&payload)?;
        let BurnExcessEvent::HeldRedemptionsAnchored {
            held_redemptions: indexed,
            ..
        } = event
        else {
            continue;
        };
        let indexed_deposit = aggregate_id.parse()?;
        for requested in held_redemptions {
            for indexed in indexed.iter().filter(|indexed| {
                same_transfer_key(&indexed.transfer, &requested.transfer)
            }) {
                ensure_same_attribution(
                    indexed_deposit,
                    indexed,
                    deposit_tx_hash,
                    requested,
                )?;
            }
        }
    }

    for held in held_redemptions {
        let transfer = &held.transfer;
        let stored = sqlx::query_as::<_, (String, String)>(
            "
            SELECT deposit_tx_hash, attribution
            FROM burn_excess_held_redemptions
            WHERE network = ?
              AND vault = ?
              AND tx_hash = ?
              AND log_index = ?
            ",
        )
        .bind(transfer.network.as_str())
        .bind(address_key(transfer.vault))
        .bind(hash_key(transfer.tx_hash))
        .bind(log_index_key(transfer.log_index)?)
        .fetch_optional(pool)
        .await?;
        if let Some(stored) = stored {
            ensure_matching_attribution(deposit_tx_hash, held, stored)?;
        }
    }
    Ok(())
}

async fn record_held_redemption(
    executor: impl Executor<'_, Database = Sqlite>,
    deposit_tx_hash: B256,
    held: &HeldTransferRedemption,
) -> Result<(), FundingTransferIndexError> {
    let transfer = &held.transfer;
    let (stored_deposit, stored): (String, String) = sqlx::query_as(
        "
        INSERT INTO burn_excess_held_redemptions (
            network,
            vault,
            tx_hash,
            log_index,
            deposit_tx_hash,
            attribution
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(network, vault, tx_hash, log_index) DO UPDATE
            SET attribution = burn_excess_held_redemptions.attribution
        RETURNING deposit_tx_hash, attribution
        ",
    )
    .bind(transfer.network.as_str())
    .bind(address_key(transfer.vault))
    .bind(hash_key(transfer.tx_hash))
    .bind(log_index_key(transfer.log_index)?)
    .bind(hash_key(deposit_tx_hash))
    .bind(serde_json::to_string(held)?)
    .fetch_one(executor)
    .await?;
    ensure_matching_attribution(deposit_tx_hash, held, (stored_deposit, stored))
}

fn ensure_matching_attribution(
    requested_deposit: B256,
    requested: &HeldTransferRedemption,
    (stored_deposit, stored): (String, String),
) -> Result<(), FundingTransferIndexError> {
    ensure_same_attribution(
        stored_deposit.parse()?,
        &serde_json::from_str(&stored)?,
        requested_deposit,
        requested,
    )
}

fn ensure_same_attribution(
    indexed_deposit: B256,
    indexed: &HeldTransferRedemption,
    requested_deposit: B256,
    requested: &HeldTransferRedemption,
) -> Result<(), FundingTransferIndexError> {
    if indexed != requested {
        return Err(FundingTransferIndexError::HeldAttributionConflict {
            indexed_deposit,
            requested_deposit,
            indexed: Box::new(indexed.clone()),
            requested: Box::new(requested.clone()),
        });
    }
    Ok(())
}

fn same_transfer_key(
    left: &FundingTransferId,
    right: &FundingTransferId,
) -> bool {
    left.network == right.network
        && left.vault == right.vault
        && left.tx_hash == right.tx_hash
        && left.log_index == right.log_index
}

pub(crate) async fn clear_held_redemption(
    pool: &Pool<Sqlite>,
    transfer: &FundingTransferId,
) -> Result<(), FundingTransferIndexError> {
    sqlx::query(
        "
        DELETE FROM burn_excess_held_redemptions
        WHERE network = ?
          AND vault = ?
          AND tx_hash = ?
          AND log_index = ?
        ",
    )
    .bind(transfer.network.as_str())
    .bind(address_key(transfer.vault))
    .bind(hash_key(transfer.tx_hash))
    .bind(log_index_key(transfer.log_index)?)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn held_redemption_vaults(
    pool: &Pool<Sqlite>,
    network: Network,
) -> Result<Vec<Address>, FundingTransferIndexError> {
    let vaults = sqlx::query_scalar::<_, String>(
        "
        SELECT DISTINCT vault
        FROM burn_excess_held_redemptions
        WHERE network = ?
        ORDER BY vault
        ",
    )
    .bind(network.as_str())
    .fetch_all(pool)
    .await?;
    vaults.into_iter().map(|vault| vault.parse().map_err(Into::into)).collect()
}
pub(crate) async fn funding_expectation_vaults(
    pool: &Pool<Sqlite>,
    network: Network,
) -> Result<Vec<Address>, FundingTransferIndexError> {
    let vaults = sqlx::query_scalar::<_, String>(
        "
        SELECT DISTINCT vault
        FROM burn_excess_funding_expectations
        WHERE network = ?
        ORDER BY vault
        ",
    )
    .bind(network.as_str())
    .fetch_all(pool)
    .await?;
    vaults.into_iter().map(|vault| vault.parse().map_err(Into::into)).collect()
}

#[cfg(test)]
pub(crate) async fn has_released_funding_expectation(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar(
        "
        SELECT EXISTS (
            SELECT 1
            FROM burn_excess_funding_expectations
            WHERE network = ?
              AND vault = ?
              AND released = 1
        )
        ",
    )
    .bind(network.as_str())
    .bind(address_key(vault))
    .fetch_one(pool)
    .await
}

/// Release a terminal expectation while retaining its configuration interlock
/// until the poller proves it scanned through `release_through_block`.
pub(crate) async fn release_funding_expectation(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
    release_through_block: Option<u64>,
) -> Result<bool, FundingTransferIndexError> {
    release_funding_expectation_if_owned(
        pool,
        deposit_tx_hash,
        release_through_block,
        false,
    )
    .await
}

async fn release_funding_expectation_if_owned(
    pool: &Pool<Sqlite>,
    deposit_tx_hash: B256,
    release_through_block: Option<u64>,
    require_exclusion: bool,
) -> Result<bool, FundingTransferIndexError> {
    let release_through_signed =
        release_through_block.map(i64::try_from).transpose()?;
    let result = sqlx::query(
        "
        UPDATE burn_excess_funding_expectations AS expectation
        SET released = 1,
            release_through_block = CASE
                WHEN ? IS NULL
                 AND release_through_block IS NULL
                 AND (
                     SELECT block_number
                     FROM poll_checkpoints
                     WHERE name =
                         'transfer_poll_observed:'
                         || network
                         || ':'
                         || vault
                 ) IS NULL
                THEN NULL
                ELSE MAX(
                    COALESCE(?, release_through_block, 0),
                    COALESCE(
                        (
                            SELECT block_number
                            FROM poll_checkpoints
                            WHERE name =
                                'transfer_poll_observed:'
                                || network
                                || ':'
                                || vault
                        ),
                        0
                    )
                )
            END
        WHERE expectation.deposit_tx_hash = ?
          AND (
              NOT ?
              OR EXISTS (
                  SELECT 1
                  FROM burn_excess_funding_exclusions AS exclusion
                  WHERE exclusion.deposit_tx_hash =
                        expectation.deposit_tx_hash
                    AND exclusion.network = expectation.network
                    AND exclusion.vault = expectation.vault
                    AND exclusion.from_address =
                        expectation.from_address
                    AND exclusion.to_address = expectation.to_address
                    AND exclusion.amount = expectation.amount
              )
          )
        ",
    )
    .bind(release_through_signed)
    .bind(release_through_signed)
    .bind(hash_key(deposit_tx_hash))
    .bind(require_exclusion)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

pub(crate) async fn clear_released_funding_expectations(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    scanned_through_block: u64,
) -> Result<u64, FundingTransferIndexError> {
    let scanned_through_signed = i64::try_from(scanned_through_block)?;
    let network = network.as_str();
    let vault = address_key(vault);
    let mut transaction = pool.begin().await?;
    sqlx::query(
        "
        INSERT INTO burn_excess_expectation_catchups (
            deposit_tx_hash,
            network,
            vault,
            scanned_through_block,
            completed_at
        )
        SELECT
            expectation.deposit_tx_hash,
            expectation.network,
            expectation.vault,
            ?,
            strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        FROM burn_excess_funding_expectations AS expectation
        WHERE expectation.network = ?
          AND expectation.vault = ?
          AND expectation.released = 1
          AND expectation.release_through_block IS NOT NULL
          AND expectation.release_through_block <= ?
          AND COALESCE(
              (
                  SELECT block_number
                  FROM poll_checkpoints
                  WHERE name =
                      'transfer_poll_observed:'
                      || expectation.network
                      || ':'
                      || expectation.vault
              ),
              expectation.release_through_block
          ) <= ?
        ON CONFLICT(deposit_tx_hash) DO UPDATE
        SET scanned_through_block = MAX(
                burn_excess_expectation_catchups.scanned_through_block,
                excluded.scanned_through_block
            ),
            completed_at = excluded.completed_at
        ",
    )
    .bind(scanned_through_signed)
    .bind(network)
    .bind(&vault)
    .bind(scanned_through_signed)
    .bind(scanned_through_signed)
    .execute(&mut *transaction)
    .await?;
    let result = sqlx::query(
        "
        DELETE FROM burn_excess_funding_expectations
        WHERE network = ?
          AND vault = ?
          AND released = 1
          AND release_through_block IS NOT NULL
          AND release_through_block <= ?
          AND COALESCE(
              (
                  SELECT block_number
                  FROM poll_checkpoints
                  WHERE name =
                      'transfer_poll_observed:'
                      || network
                      || ':'
                      || vault
              ),
              release_through_block
          ) <= ?
        ",
    )
    .bind(network)
    .bind(vault)
    .bind(scanned_through_signed)
    .bind(scanned_through_signed)
    .execute(&mut *transaction)
    .await?;
    transaction.commit().await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
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
              AND released = 0
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FundingExpectationStatus {
    Active,
    Released,
}

pub(crate) async fn funding_expectation_status(
    pool: &Pool<Sqlite>,
    network: Network,
    vault: Address,
    from: Address,
    to: Address,
    amount: U256,
) -> Result<Option<FundingExpectationStatus>, sqlx::Error> {
    let released: Option<bool> = sqlx::query_scalar(
        "
        SELECT released
        FROM burn_excess_funding_expectations
        WHERE network = ?
          AND vault = ?
          AND from_address = ?
          AND to_address = ?
          AND amount = ?
        ",
    )
    .bind(network.as_str())
    .bind(address_key(vault))
    .bind(address_key(from))
    .bind(address_key(to))
    .bind(amount.to_string())
    .fetch_optional(pool)
    .await?;
    Ok(released.map(|released| {
        if released {
            FundingExpectationStatus::Released
        } else {
            FundingExpectationStatus::Active
        }
    }))
}
/// Whether `transfer`'s transaction carries another non-mint Transfer into
/// the issuer wallet, other than this stream's optional funding log or an
/// excluded log. Redemption identity is transaction-wide, while vault
/// configuration and wallet linkage are mutable, so admission must reject
/// every potential future competitor rather than only logs the poller would
/// accept today.
pub(crate) async fn competes_for_redemption_key(
    pool: &Pool<Sqlite>,
    receipt_logs: &[Log],
    transfer: &FundingTransferId,
    funding: Option<&FundingTransferId>,
) -> Result<bool, FundingTransferIndexError> {
    for log in receipt_logs {
        let Ok(decoded) =
            log.log_decode::<OffchainAssetReceiptVault::Transfer>()
        else {
            continue;
        };
        let data = decoded.data();
        if data.to != transfer.to || data.from == Address::ZERO {
            continue;
        }
        let Some(log_index) = log.log_index else {
            return Ok(true);
        };
        let vault = log.address();
        if vault == transfer.vault && log_index == transfer.log_index {
            continue;
        }
        let is_funding = funding.is_some_and(|funding| {
            vault == funding.vault
                && transfer.tx_hash == funding.tx_hash
                && log_index == funding.log_index
        });
        if is_funding {
            continue;
        }
        if !is_excluded_funding_log(
            pool,
            transfer.network,
            vault,
            transfer.tx_hash,
            log_index,
        )
        .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum FundingTransferIndexError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),

    #[error(
        "held-redemption attribution conflict: indexed deposit \
         {indexed_deposit:?} {indexed:?}, requested deposit \
         {requested_deposit:?} {requested:?}"
    )]
    HeldAttributionConflict {
        indexed_deposit: B256,
        requested_deposit: B256,
        indexed: Box<HeldTransferRedemption>,
        requested: Box<HeldTransferRedemption>,
    },

    #[error(transparent)]
    Hex(#[from] alloy::hex::FromHexError),
    #[error(transparent)]
    Range(#[from] std::num::TryFromIntError),
}
/// How the redemption poller must treat one Transfer log with respect to
/// Path B burn-excess funding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FundingTransferStatus {
    /// A stream proved and excluded this exact log: skip it.
    Excluded,
    /// A stream expects a Transfer of this shape and has not excluded it yet:
    /// hold it.
    Expected,
    /// A terminal expectation owns this shape, but this pass did not preflight
    /// it. Retry so the next pass can receipt-pin before detection.
    Released,
    /// The expectation released this exact genuine AP Transfer. Detect it
    /// with the account attribution anchored before the excess burn.
    Attributed(Box<HeldTransferRedemption>),
    /// No burn-excess stream claims it: an ordinary Transfer.
    Unrelated,
}

/// Classifies one Transfer log against the exclusion, expectation, and held
/// attribution indexes in one SQLite snapshot. Exact exclusion wins, followed
/// by active shape ownership, exact attribution, then released shape ownership.
/// Surfacing a released row lets a pass that missed the initial guard snapshot
/// retry before detection.
pub(crate) async fn classify_funding_transfer(
    pool: &Pool<Sqlite>,
    transfer: &FundingTransferId,
) -> Result<FundingTransferStatus, FundingTransferIndexError> {
    let (excluded, expected, released, attribution) =
        sqlx::query_as::<_, (bool, bool, bool, Option<String>)>(
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
                      AND released = 0
                ),
                EXISTS (
                    SELECT 1
                    FROM burn_excess_funding_expectations
                    WHERE network = ?
                      AND vault = ?
                      AND from_address = ?
                      AND to_address = ?
                      AND amount = ?
                      AND released = 1
                ),
                (
                    SELECT attribution
                    FROM burn_excess_held_redemptions
                    WHERE network = ?
                      AND vault = ?
                      AND tx_hash = ?
                      AND log_index = ?
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
        .bind(transfer.network.as_str())
        .bind(address_key(transfer.vault))
        .bind(address_key(transfer.from))
        .bind(address_key(transfer.to))
        .bind(transfer.amount.to_string())
        .bind(transfer.network.as_str())
        .bind(address_key(transfer.vault))
        .bind(hash_key(transfer.tx_hash))
        .bind(log_index_key(transfer.log_index)?)
        .fetch_one(pool)
        .await?;

    if excluded {
        return Ok(FundingTransferStatus::Excluded);
    }
    if expected {
        return Ok(FundingTransferStatus::Expected);
    }
    if let Some(payload) = attribution {
        return serde_json::from_str(&payload)
            .map(Box::new)
            .map(FundingTransferStatus::Attributed)
            .map_err(Into::into);
    }
    if released {
        return Ok(FundingTransferStatus::Released);
    }
    Ok(FundingTransferStatus::Unrelated)
}

/// Reset the active shape-expectation index and pending exact
/// held-redemption attribution index from BurnExcess events.
///
/// Active and terminal expectations remain indexed until a released catch-up
/// scan reaches the terminal event's chain boundary. Exact attribution is
/// rebuilt only until its Redemption has persisted detection. Run only at
/// startup before pollers spawn: both tables are deleted and rebuilt in one
/// transaction.
pub(crate) async fn rebuild_funding_expectation_index(
    pool: &Pool<Sqlite>,
) -> Result<usize, RebuildFundingExpectationError> {
    let mut transaction = pool.begin().await?;

    sqlx::query("DELETE FROM burn_excess_funding_expectations")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM burn_excess_held_redemptions")
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM burn_excess_expectation_guards")
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
          AND (
              NOT EXISTS (
                  SELECT 1
                  FROM events AS intended
                  WHERE intended.aggregate_type = expected.aggregate_type
                    AND intended.aggregate_id = expected.aggregate_id
                    AND intended.event_type = ?
              )
              OR EXISTS (
                  SELECT 1
                  FROM events AS anchored
                  WHERE anchored.aggregate_type = expected.aggregate_type
                    AND anchored.aggregate_id = expected.aggregate_id
                    AND anchored.event_type = ?
                    AND NOT EXISTS (
                        SELECT 1
                        FROM events AS prior_submission
                        WHERE prior_submission.aggregate_type =
                              anchored.aggregate_type
                          AND prior_submission.aggregate_id =
                              anchored.aggregate_id
                          AND prior_submission.event_type = ?
                          AND prior_submission.sequence < anchored.sequence
                    )
              )
          )
        ",
    )
    .bind(BurnExcessEvent::FUNDING_EXPECTED)
    .bind(BurnExcessEvent::EXCESS_BURN_INTENDED)
    .bind(BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED)
    .bind(BurnExcessEvent::EXCESS_BURN_SUBMITTED)
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
        let catchup_complete = sqlx::query_scalar::<_, bool>(
            "
            SELECT EXISTS (
                SELECT 1
                FROM burn_excess_expectation_catchups
                WHERE deposit_tx_hash = ?
            )
            ",
        )
        .bind(&aggregate_id)
        .fetch_one(&mut *transaction)
        .await?;
        if catchup_complete {
            continue;
        }

        record_funding_expectation(&mut *transaction, &bind, expected_at)
            .await?;
        recorded = recorded.saturating_add(1);
    }
    sqlx::query(
        "
        INSERT INTO burn_excess_expectation_guards (
            deposit_tx_hash,
            network,
            vault,
            from_address,
            to_address,
            amount
        )
        SELECT
            deposit_tx_hash,
            network,
            vault,
            from_address,
            to_address,
            amount
        FROM burn_excess_funding_expectations
        WHERE true
        ON CONFLICT(deposit_tx_hash) DO NOTHING
        ",
    )
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "
        UPDATE burn_excess_funding_expectations AS expectation
        SET released = 1,
            release_through_block = (
                SELECT json_extract(
                    closed.payload,
                    '$.ExcessBurnClosed.release_through_block'
                )
                FROM events AS closed
                WHERE closed.aggregate_type = 'BurnExcess'
                  AND closed.aggregate_id = expectation.deposit_tx_hash
                  AND closed.event_type = ?
                ORDER BY closed.sequence DESC
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM events AS closed
            WHERE closed.aggregate_type = 'BurnExcess'
              AND closed.aggregate_id = expectation.deposit_tx_hash
              AND closed.event_type = ?
        )
        ",
    )
    .bind(BurnExcessEvent::EXCESS_BURN_CLOSED)
    .bind(BurnExcessEvent::EXCESS_BURN_CLOSED)
    .execute(&mut *transaction)
    .await?;
    sqlx::query(
        "
        UPDATE burn_excess_funding_expectations AS expectation
        SET released = 1,
            release_through_block = (
                SELECT json_extract(
                    completed.payload,
                    '$.ExcessBurnCompleted.block_number'
                )
                FROM events AS completed
                WHERE completed.aggregate_type = 'BurnExcess'
                  AND completed.aggregate_id =
                      expectation.deposit_tx_hash
                  AND completed.event_type = ?
                ORDER BY completed.sequence DESC
                LIMIT 1
            )
        WHERE EXISTS (
            SELECT 1
            FROM events AS completed
            WHERE completed.aggregate_type = 'BurnExcess'
              AND completed.aggregate_id = expectation.deposit_tx_hash
              AND completed.event_type = ?
        )
        ",
    )
    .bind(BurnExcessEvent::EXCESS_BURN_COMPLETED)
    .bind(BurnExcessEvent::EXCESS_BURN_COMPLETED)
    .execute(&mut *transaction)
    .await?;
    raise_released_boundaries_to_observed(&mut transaction).await?;

    let (held_recorded, held_to_rewind) =
        rebuild_held_redemption_index(&mut transaction).await?;

    transaction.commit().await?;
    for (network, vault, block_number) in held_to_rewind {
        rewind_transfer_poll_before(pool, network, vault, block_number).await?;
    }

    if recorded > 0 {
        info!(
            target: "burn_excess",
            recorded,
            "Rebuilt funding expectation index from FundingExpected events"
        );
    }
    if held_recorded > 0 {
        info!(
            target: "burn_excess",
            held_recorded,
            "Rebuilt held-redemption attribution index from \
             HeldRedemptionsAnchored events"
        );
    }

    Ok(recorded)
}
async fn raise_released_boundaries_to_observed(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "
        UPDATE burn_excess_funding_expectations
        SET release_through_block = MAX(
            release_through_block,
            COALESCE(
                (
                    SELECT block_number
                    FROM poll_checkpoints
                    WHERE name =
                        'transfer_poll_observed:'
                        || network
                        || ':'
                        || vault
                ),
                release_through_block
            )
        )
        WHERE released = 1
          AND release_through_block IS NOT NULL
        ",
    )
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

async fn rebuild_held_redemption_index(
    transaction: &mut Transaction<'_, Sqlite>,
) -> Result<(usize, Vec<(Network, Address, u64)>), RebuildFundingExpectationError>
{
    let held_rows = sqlx::query_as::<_, (String, String)>(
        "
        SELECT aggregate_id, payload
        FROM events
        WHERE aggregate_type = 'BurnExcess'
          AND event_type = ?
        ",
    )
    .bind(BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED)
    .fetch_all(&mut **transaction)
    .await?;
    let mut held_recorded = 0usize;
    let mut held_to_rewind = Vec::new();
    for (aggregate_id, payload) in held_rows {
        let event: BurnExcessEvent = serde_json::from_str(&payload)?;
        let BurnExcessEvent::HeldRedemptionsAnchored {
            held_redemptions, ..
        } = event
        else {
            debug!(
                target: "burn_excess",
                %aggregate_id,
                "Skipping non-attribution payload under \
                 HeldRedemptionsAnchored event_type"
            );
            continue;
        };
        let deposit_tx_hash = aggregate_id.parse()?;
        for held in &held_redemptions {
            let excluded = sqlx::query_scalar::<_, bool>(
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
            .bind(held.transfer.network.as_str())
            .bind(address_key(held.transfer.vault))
            .bind(hash_key(held.transfer.tx_hash))
            .bind(log_index_key(held.transfer.log_index)?)
            .fetch_one(&mut **transaction)
            .await?;
            if excluded {
                continue;
            }
            let detected = sqlx::query_scalar::<_, bool>(
                "
                SELECT EXISTS (
                    SELECT 1
                    FROM events
                    WHERE aggregate_type = 'Redemption'
                      AND aggregate_id = ?
                      AND event_type = 'RedemptionEvent::Detected'
                )
                ",
            )
            .bind(held.transfer.tx_hash.to_string())
            .fetch_one(&mut **transaction)
            .await?;
            if detected {
                continue;
            }
            record_held_redemption(&mut **transaction, deposit_tx_hash, held)
                .await?;
            held_to_rewind.push((
                held.transfer.network,
                held.transfer.vault,
                held.block_number,
            ));
            held_recorded = held_recorded.saturating_add(1);
        }
    }

    Ok((held_recorded, held_to_rewind))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum RebuildFundingExpectationError {
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Hex(#[from] alloy::hex::FromHexError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),

    #[error(transparent)]
    Index(#[from] FundingTransferIndexError),
}

deps!(FundingExpectationReactor, [BurnExcess]);

/// Writes an expectation into the SQL index when a stream records it. A close
/// marks it released until one successful ordinary poll pass catches up;
/// completion drops it once the permanent funding exclusion exists. A stream
/// that has excluded its funding log keeps the row until then, so other
/// Transfers of the same shape stay held while its burn is pending.
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
    ) -> Result<(), FundingTransferIndexError> {
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
            BurnExcessEvent::HeldRedemptionsAnchored {
                held_redemptions,
                ..
            } => {
                record_held_redemptions(
                    &self.pool,
                    deposit_tx_hash,
                    held_redemptions,
                )
                .await?;
                if !held_redemptions.is_empty() {
                    info!(
                        target: "burn_excess",
                        %deposit_tx_hash,
                        held_redemptions = held_redemptions.len(),
                        "Anchored held-redemption account attribution before \
                         excess-burn broadcast"
                    );
                }
            }
            BurnExcessEvent::ExcessBurnCompleted { block_number, .. } => {
                let released = release_funding_expectation_if_owned(
                    &self.pool,
                    deposit_tx_hash,
                    Some(*block_number),
                    true,
                )
                .await?;
                log_release(deposit_tx_hash, "Completed", released);
            }
            BurnExcessEvent::ExcessBurnClosed {
                release_through_block, ..
            } => {
                let released = release_funding_expectation(
                    &self.pool,
                    deposit_tx_hash,
                    *release_through_block,
                )
                .await?;
                log_release(deposit_tx_hash, "Closed", released);
            }
            BurnExcessEvent::FundingExclusionRecorded { .. }
            | BurnExcessEvent::ExcessBurnIntended { .. }
            | BurnExcessEvent::ExcessBurnSubmitted { .. } => {}
        }
        Ok(())
    }
}

/// The release is what lets the poller detect the Transfers the stream held,
/// so it is the line tying their detection to this stream. Shared by the
/// reactor and the engine's terminal heal.
pub(crate) fn log_release(deposit_tx_hash: B256, state: &str, released: bool) {
    if released {
        info!(
            target: "burn_excess",
            %deposit_tx_hash,
            state,
            "Released the funding expectation of a terminal stream; Transfers \
             it held are now detected as ordinary redemptions unless excluded"
        );
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
    use event_sorcery::{Store, StoreBuilder};
    use tracing_test::traced_test;

    use super::*;
    use crate::account::{AlpacaAccountNumber, ClientId};
    use crate::burn_excess::exclusion::{
        rebuild_funding_exclusion_index, record_funding_exclusion,
    };
    use crate::burn_excess::{
        BurnExcessCloseProof, BurnExcessCommand, BurnExcessId, BurnExcessPath,
        FundingTransferId, HeldTransferRedemption,
    };
    use crate::config::VaultMode;
    use crate::mint::IssuerMintRequestId;
    use crate::poll_checkpoint::{
        advance_transfer_poll, advance_transfer_poll_observed,
        load_transfer_poll,
    };
    use crate::redemption::{IssuerRedemptionRequestId, RedemptionEvent};
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{TokenSymbol, UnderlyingSymbol};
    use crate::vault::{SendableTxWithHash, TxId};

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
            chain_id: 8453,
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

    #[tokio::test]
    async fn newer_observed_high_water_blocks_stale_release_cleanup() {
        let pool = pool().await;
        let expected = bind(B256::random());
        record_funding_expectation(&pool, &expected, Utc::now()).await.unwrap();
        assert!(
            release_funding_expectation(
                &pool,
                expected.deposit_tx_hash,
                Some(100),
            )
            .await
            .unwrap()
        );
        advance_transfer_poll_observed(
            &pool,
            expected.network,
            expected.vault,
            200,
        )
        .await
        .unwrap();

        assert_eq!(
            clear_released_funding_expectations(
                &pool,
                expected.network,
                expected.vault,
                100,
            )
            .await
            .unwrap(),
            0
        );
        assert_eq!(
            clear_released_funding_expectations(
                &pool,
                expected.network,
                expected.vault,
                200,
            )
            .await
            .unwrap(),
            1
        );
    }

    async fn seed_expectation_trigger_context(
        pool: &Pool<Sqlite>,
        binds: &[&ExcessBurnBind],
    ) {
        sqlx::query(
            "
            INSERT INTO tokenized_asset_vault_owners (
                network,
                vault,
                aggregate_id
            )
            VALUES (?, ?, 'PTY:base')
            ",
        )
        .bind(Network::Base.as_str())
        .bind(address_key(binds[0].vault))
        .execute(pool)
        .await
        .unwrap();
        for bind in binds {
            let account_id = uuid::Uuid::new_v4().to_string();
            let account_payload = serde_json::json!({
                "WalletWhitelisted": {
                    "wallet": format!("{:#x}", bind.original_recipient),
                    "whitelisted_at": Utc::now(),
                }
            });
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
                    'Account',
                    ?,
                    1,
                    'AccountEvent::WalletWhitelisted',
                    '1.0',
                    ?,
                    '{}'
                )
                ",
            )
            .bind(account_id)
            .bind(account_payload.to_string())
            .execute(pool)
            .await
            .unwrap();
            let mint_payload = serde_json::json!({
                "Initiated": {
                    "underlying": "PTY",
                    "network": "base",
                }
            });
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
                    'Mint',
                    ?,
                    1,
                    'MintEvent::Initiated',
                    '1.0',
                    ?,
                    '{}'
                )
                ",
            )
            .bind(bind.issuer_request_id.to_string())
            .bind(mint_payload.to_string())
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn one_transfer_shape_cannot_back_two_expectations() {
        let pool = pool().await;
        let expected = bind(B256::random());
        record_funding_expectation(&pool, &expected, Utc::now()).await.unwrap();
        let competing = ExcessBurnBind {
            issuer_request_id: IssuerMintRequestId::random(),
            deposit_tx_hash: B256::random(),
            ..expected
        };

        let error = record_funding_expectation(&pool, &competing, Utc::now())
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(_)));
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM burn_excess_funding_expectations",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 1);
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

    /// An exact held log stays attributable after its shape expectation is
    /// released, while the active expectation still wins and keeps it held.
    #[tokio::test]
    async fn held_redemption_attribution_survives_expectation_release() {
        let pool = pool().await;
        let bind = bind(B256::random());
        let held = HeldTransferRedemption {
            transfer: funding_log(&bind),
            block_number: 100,
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("account".into()),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            burn_mode: VaultMode::VaultDirect,
        };
        advance_transfer_poll(
            &pool,
            held.transfer.network,
            held.transfer.vault,
            200,
        )
        .await
        .unwrap();
        record_held_redemptions(
            &pool,
            bind.deposit_tx_hash,
            std::slice::from_ref(&held),
        )
        .await
        .unwrap();
        assert_eq!(
            load_transfer_poll(
                &pool,
                held.transfer.network,
                held.transfer.vault,
            )
            .await
            .unwrap(),
            Some(99),
            "an anchored log below the checkpoint must be replayed"
        );
        advance_transfer_poll(
            &pool,
            held.transfer.network,
            held.transfer.vault,
            150,
        )
        .await
        .unwrap();
        assert_eq!(
            load_transfer_poll(
                &pool,
                held.transfer.network,
                held.transfer.vault,
            )
            .await
            .unwrap(),
            Some(99),
            "a stale poll pass must not advance beyond the held-log floor"
        );
        let other_deposit = B256::random();
        record_held_redemptions(
            &pool,
            other_deposit,
            std::slice::from_ref(&held),
        )
        .await
        .unwrap();
        let conflicting = HeldTransferRedemption {
            client_id: ClientId::new(),
            ..held.clone()
        };
        assert!(matches!(
            record_held_redemptions(
                &pool,
                other_deposit,
                std::slice::from_ref(&conflicting),
            )
            .await
            .unwrap_err(),
            FundingTransferIndexError::HeldAttributionConflict {
                indexed_deposit,
                requested_deposit,
                ..
            } if indexed_deposit == bind.deposit_tx_hash
                && requested_deposit == other_deposit
        ));

        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Attributed(Box::new(held.clone()))
        );
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();
        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Expected
        );
        clear_funding_expectation(&pool, bind.deposit_tx_hash).await.unwrap();
        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Attributed(Box::new(held.clone()))
        );
        record_funding_exclusion(
            &pool,
            &held.transfer,
            B256::random(),
            Utc::now(),
        )
        .await
        .unwrap();
        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Excluded
        );
        assert!(
            held_redemption_vaults(&pool, held.transfer.network)
                .await
                .unwrap()
                .is_empty()
        );
    }

    fn funding_log(bind: &ExcessBurnBind) -> FundingTransferId {
        FundingTransferId {
            network: bind.network,
            vault: bind.vault,
            tx_hash: B256::random(),
            log_index: 3,
            from: bind.original_recipient,
            to: bind.issuer_wallet,
            amount: bind.shares,
        }
    }

    /// Drives a live stream through its whole Path B lifecycle to `Completed`.
    async fn complete_stream(
        store: &Store<BurnExcess>,
        bind: &ExcessBurnBind,
    ) -> HeldTransferRedemption {
        let id = BurnExcessId::new(bind.deposit_tx_hash);
        let funding = funding_log(bind);
        let burn_tx_hash = B256::random();
        let held = HeldTransferRedemption {
            transfer: FundingTransferId {
                tx_hash: B256::random(),
                log_index: 4,
                ..funding_log(bind)
            },
            block_number: 100,
            client_id: ClientId::new(),
            alpaca_account: AlpacaAccountNumber("account".into()),
            underlying: UnderlyingSymbol::new("PTY").unwrap(),
            token: TokenSymbol::new("tPTY"),
            burn_mode: VaultMode::VaultDirect,
        };
        let commands = [
            BurnExcessCommand::ExpectFunding {
                bind: bind.clone(),
                reason: "duplicate mint".into(),
                incident_id: None,
            },
            BurnExcessCommand::RecordFundingExclusion {
                bind: bind.clone(),
                funding_log_id: funding.clone(),
                reason: "duplicate mint".into(),
                incident_id: None,
            },
            BurnExcessCommand::IntendExcessBurn {
                bind: bind.clone(),
                path: BurnExcessPath::External,
                funding_log_id: Some(funding),
                reason: "duplicate mint".into(),
                incident_id: None,
                sendable_tx: SendableTxWithHash {
                    tx: vec![0xde, 0xad],
                    hash: burn_tx_hash,
                    nonce: 7,
                    signed_at: Utc::now(),
                    dust_shares: U256::ZERO,
                },
                held_redemptions: vec![held.clone()],
            },
            BurnExcessCommand::RecordExcessBurnSubmitted {
                tx_id: TxId::from(burn_tx_hash),
                burn_tx_hash,
            },
            BurnExcessCommand::CompleteExcessBurn {
                burn_tx_hash,
                block_number: 99,
            },
        ];
        for command in commands {
            store.send(&id, command).await.unwrap();
        }
        held
    }

    /// The rebuild restores active expectations and terminal handoff rows. A
    /// completed or closed stream is released at its persisted chain boundary,
    /// but keeps its configuration guard until the poller safely catches up.
    #[tokio::test]
    async fn rebuild_restores_active_and_terminal_handoffs() {
        let pool = pool().await;
        let store = StoreBuilder::<BurnExcess>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let other_sender = |deposit| ExcessBurnBind {
            original_recipient: Address::random(),
            ..bind(deposit)
        };

        let awaiting = bind(B256::random());
        let excluded = other_sender(B256::random());
        let closed = other_sender(B256::random());
        let completed = other_sender(B256::random());
        seed_expectation_trigger_context(
            &pool,
            [&awaiting, &excluded, &closed, &completed].as_slice(),
        )
        .await;
        for stream in [&awaiting, &excluded, &closed, &completed] {
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
        let completed_held = complete_stream(&store, &completed).await;
        store
            .send(
                &BurnExcessId::new(excluded.deposit_tx_hash),
                BurnExcessCommand::RecordFundingExclusion {
                    bind: excluded.clone(),
                    funding_log_id: FundingTransferId {
                        tx_hash: b256!(
                            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        ),
                        ..funding_log(&excluded)
                    },
                    reason: "duplicate mint".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &BurnExcessId::new(closed.deposit_tx_hash),
                BurnExcessCommand::CloseExcessBurn {
                    reason: "funding never sent".into(),
                    proof: BurnExcessCloseProof::Unsigned,
                    release_through_block: Some(100),
                },
            )
            .await
            .unwrap();

        // The store above runs no reactors: the pending streams' rows are
        // missing, and stale rows are left for the finished ones.
        for finished in [&closed, &completed] {
            record_funding_expectation(&pool, finished, Utc::now())
                .await
                .unwrap();
        }

        advance_transfer_poll_observed(
            &pool,
            completed.network,
            completed.vault,
            150,
        )
        .await
        .unwrap();
        assert_eq!(rebuild_funding_expectation_index(&pool).await.unwrap(), 4);
        assert!(expects(&pool, &awaiting).await);
        assert!(expects(&pool, &excluded).await);
        assert!(!expects(&pool, &closed).await);
        let (closed_released, release_through): (i64, Option<i64>) =
            sqlx::query_as(
                "
                SELECT released, release_through_block
                FROM burn_excess_funding_expectations
                WHERE deposit_tx_hash = ?
                ",
            )
            .bind(closed.deposit_tx_hash.to_string())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(closed_released, 1);
        assert_eq!(release_through, Some(150));
        let completed_release: Option<i64> = sqlx::query_scalar(
            "
            SELECT release_through_block
            FROM burn_excess_funding_expectations
            WHERE deposit_tx_hash = ?
            ",
        )
        .bind(completed.deposit_tx_hash.to_string())
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(completed_release, Some(150));
        assert!(!expects(&pool, &completed).await);
        assert_eq!(
            classify_funding_transfer(&pool, &completed_held.transfer)
                .await
                .unwrap(),
            FundingTransferStatus::Attributed(Box::new(completed_held.clone()))
        );
        let detected = RedemptionEvent::Detected {
            issuer_request_id: IssuerRedemptionRequestId::new(
                completed_held.transfer.tx_hash,
            ),
            underlying: completed_held.underlying.clone(),
            token: completed_held.token.clone(),
            network: completed_held.transfer.network,
            wallet: completed_held.transfer.from,
            quantity: crate::Quantity::from_u256_with_18_decimals(
                completed_held.transfer.amount,
            )
            .unwrap(),
            tx_hash: completed_held.transfer.tx_hash,
            block_number: 99,
            detected_at: Utc::now(),
            burn_mode: completed_held.burn_mode,
        };
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
            VALUES ('Redemption', ?, 1, 'RedemptionEvent::Detected', '1.0', ?, '{}')
            ",
        )
        .bind(completed_held.transfer.tx_hash.to_string())
        .bind(serde_json::to_string(&detected).unwrap())
        .execute(&pool)
        .await
        .unwrap();

        rebuild_funding_expectation_index(&pool).await.unwrap();
        assert_eq!(
            classify_funding_transfer(&pool, &completed_held.transfer)
                .await
                .unwrap(),
            FundingTransferStatus::Released
        );

        advance_transfer_poll(&pool, completed.network, completed.vault, 150)
            .await
            .unwrap();
        assert_eq!(
            clear_released_funding_expectations(
                &pool,
                completed.network,
                completed.vault,
                150,
            )
            .await
            .unwrap(),
            2
        );
        assert_eq!(rebuild_funding_expectation_index(&pool).await.unwrap(), 2);
        let replacement = ExcessBurnBind {
            deposit_tx_hash: B256::random(),
            receipt_id: U256::from(88u64),
            ..closed.clone()
        };
        store
            .send(
                &BurnExcessId::new(replacement.deposit_tx_hash),
                BurnExcessCommand::ExpectFunding {
                    bind: replacement.clone(),
                    reason: "replacement".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        advance_transfer_poll_observed(
            &pool,
            replacement.network,
            replacement.vault,
            200,
        )
        .await
        .unwrap();
        assert_eq!(rebuild_funding_expectation_index(&pool).await.unwrap(), 3);
        assert!(expects(&pool, &replacement).await);
    }

    #[tokio::test]
    async fn rebuild_omits_held_log_claimed_as_later_funding() {
        let pool = pool().await;
        let store = StoreBuilder::<BurnExcess>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let first = bind(B256::random());
        let second = ExcessBurnBind {
            issuer_request_id: first.issuer_request_id.clone(),
            deposit_tx_hash: B256::random(),
            receipt_id: U256::from(8u64),
            shares: first.shares,
            original_recipient: first.original_recipient,
            vault: first.vault,
            network: first.network,
            chain_id: 8453,
            issuer_wallet: first.issuer_wallet,
        };
        seed_expectation_trigger_context(&pool, &[&first]).await;
        let held = complete_stream(&store, &first).await;
        record_funding_expectation(&pool, &first, Utc::now()).await.unwrap();
        assert!(
            release_funding_expectation(
                &pool,
                first.deposit_tx_hash,
                Some(99),
            )
            .await
            .unwrap()
        );
        advance_transfer_poll_observed(&pool, first.network, first.vault, 100)
            .await
            .unwrap();
        assert_eq!(
            clear_released_funding_expectations(
                &pool,
                first.network,
                first.vault,
                100,
            )
            .await
            .unwrap(),
            1
        );
        let second_id = BurnExcessId::new(second.deposit_tx_hash);
        store
            .send(
                &second_id,
                BurnExcessCommand::ExpectFunding {
                    bind: second.clone(),
                    reason: "second stream".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &second_id,
                BurnExcessCommand::RecordFundingExclusion {
                    bind: second,
                    funding_log_id: held.transfer.clone(),
                    reason: "second stream".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();

        rebuild_funding_exclusion_index(&pool).await.unwrap();
        rebuild_funding_expectation_index(&pool).await.unwrap();

        assert_eq!(
            classify_funding_transfer(&pool, &held.transfer).await.unwrap(),
            FundingTransferStatus::Excluded
        );
        assert!(
            held_redemption_vaults(&pool, held.transfer.network)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rebuild_does_not_restore_legacy_post_intent_expectation() {
        let pool = pool().await;
        let store = StoreBuilder::<BurnExcess>::new(pool.clone())
            .build(())
            .await
            .unwrap();
        let bind = bind(B256::random());
        let id = BurnExcessId::new(bind.deposit_tx_hash);
        seed_expectation_trigger_context(&pool, &[&bind]).await;
        store
            .send(
                &id,
                BurnExcessCommand::ExpectFunding {
                    bind: bind.clone(),
                    reason: "duplicate mint".into(),
                    incident_id: None,
                },
            )
            .await
            .unwrap();
        let intended = BurnExcessEvent::ExcessBurnIntended {
            bind: bind.clone(),
            path: BurnExcessPath::External,
            funding_log_id: Some(funding_log(&bind)),
            reason: "legacy".into(),
            incident_id: None,
            sendable_tx: SendableTxWithHash::default(),
            intended_at: Utc::now(),
        };
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
            VALUES ('BurnExcess', ?, 2, ?, '1.0', ?, '{}')
            ",
        )
        .bind(id.to_string())
        .bind(BurnExcessEvent::EXCESS_BURN_INTENDED)
        .bind(serde_json::to_string(&intended).unwrap())
        .execute(&pool)
        .await
        .unwrap();
        let burn_tx_hash = B256::random();
        let legacy_repair_events = [
            (
                3_i64,
                BurnExcessEvent::EXCESS_BURN_SUBMITTED,
                BurnExcessEvent::ExcessBurnSubmitted {
                    tx_id: TxId::from(burn_tx_hash),
                    burn_tx_hash,
                    submitted_at: Utc::now(),
                },
            ),
            (
                4_i64,
                BurnExcessEvent::HELD_REDEMPTIONS_ANCHORED,
                BurnExcessEvent::HeldRedemptionsAnchored {
                    held_redemptions: Vec::new(),
                    anchored_at: Utc::now(),
                },
            ),
        ];
        for (sequence, event_type, event) in legacy_repair_events {
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
                VALUES ('BurnExcess', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(id.to_string())
            .bind(sequence)
            .bind(event_type)
            .bind(serde_json::to_string(&event).unwrap())
            .execute(&pool)
            .await
            .unwrap();
        }

        assert_eq!(rebuild_funding_expectation_index(&pool).await.unwrap(), 0);
        assert!(!expects(&pool, &bind).await);
    }

    /// The live reactor drops a completed stream's row only once its
    /// exclusion row is in, and says so: that line ties the held Transfers'
    /// detection to the stream that released them.
    #[traced_test]
    #[tokio::test]
    async fn the_reactor_releases_a_completed_stream_only_once_excluded() {
        let pool = pool().await;
        let bind = bind(B256::random());
        let reactor = FundingExpectationReactor::new(pool.clone());
        let completed = BurnExcessEvent::ExcessBurnCompleted {
            burn_tx_hash: B256::random(),
            block_number: 99,
            completed_at: Utc::now(),
        };
        record_funding_expectation(&pool, &bind, Utc::now()).await.unwrap();

        reactor.on_event(bind.deposit_tx_hash, &completed).await.unwrap();
        assert!(expects(&pool, &bind).await);
        let deposit_key = bind.deposit_tx_hash.to_string();
        let release = [
            "Released the funding expectation of a terminal stream",
            deposit_key.as_str(),
            "Completed",
        ];
        assert!(!logs_contain_at!(tracing::Level::INFO, &release));

        let unrelated_exclusion = FundingTransferId {
            tx_hash: B256::random(),
            amount: bind.shares + U256::from(1u64),
            ..funding_log(&bind)
        };
        record_funding_exclusion(
            &pool,
            &unrelated_exclusion,
            bind.deposit_tx_hash,
            Utc::now(),
        )
        .await
        .unwrap();
        reactor.on_event(bind.deposit_tx_hash, &completed).await.unwrap();
        assert!(
            expects(&pool, &bind).await,
            "a different exclusion owned by the deposit is not the funding log"
        );

        record_funding_exclusion(
            &pool,
            &funding_log(&bind),
            bind.deposit_tx_hash,
            Utc::now(),
        )
        .await
        .unwrap();
        reactor.on_event(bind.deposit_tx_hash, &completed).await.unwrap();
        assert!(!expects(&pool, &bind).await);
        assert!(logs_contain_at!(tracing::Level::INFO, &release));
    }
}
