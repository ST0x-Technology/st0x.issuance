use alloy::eips::BlockId;
use alloy::primitives::{Address, Bytes, TxHash, U256};
use alloy::rpc::types::{Filter, Log};
use alloy::sol_types::SolEvent;
use alloy::transports::{RpcError, TransportErrorKind};
use async_trait::async_trait;
use cqrs_es::AggregateError;
use event_sorcery::Store;
use futures::{StreamExt, TryStreamExt, stream};
use itertools::Itertools;
use sqlx::{Pool, Sqlite};
use std::sync::Arc;
use tracing::{error, info, trace, warn};

use super::{
    ReceiptId, ReceiptInventory, ReceiptInventoryCommand,
    ReceiptInventoryError, ReceiptSource, Shares, determine_source,
    load_inventory, send_receipt_inventory_command,
};
use crate::bindings::{OffchainAssetReceiptVault, Receipt};
use crate::mint::IssuerMintRequestId;
use crate::poll_checkpoint::{CheckpointError, advance_receipt_backfill};
use crate::tokenized_asset::Network;

/// Maximum number of blocks to query in a single get_logs call.
/// RPCs typically limit response sizes, so we chunk large ranges.
const BLOCK_CHUNK_SIZE: u64 = 2000;

/// Maximum concurrent RPC calls for balance checks.
/// Limits parallelism to avoid overwhelming the RPC provider.
const MAX_CONCURRENT_BALANCE_CHECKS: usize = 4;

/// Blocks the balance reads stay behind a fresh head when the chain moved on
/// during a long log scan. A pooled RPC can answer `eth_blockNumber` from a
/// backend a block or two ahead of the backend that then serves a read. The
/// reads never go below the block the logs reach, so after a short scan they
/// read at the pass head itself. A backend behind the pass head then fails
/// the pass, and the next pass, or the restart at startup, retries.
const READ_BLOCK_MARGIN: u64 = 2;

/// Generates inclusive block ranges of at most `chunk_size` blocks.
fn block_ranges(
    from: u64,
    to: u64,
    chunk_size: u64,
) -> impl Iterator<Item = (u64, u64)> {
    std::iter::successors(Some(from), move |&start| {
        let next = start + chunk_size;
        if next <= to { Some(next) } else { None }
    })
    .map(move |start| (start, (start + chunk_size - 1).min(to)))
}

/// Backfills the ReceiptInventory aggregate by scanning historic Deposit
/// events and ERC-1155 transfer events where the bot wallet received receipts.
///
/// This handles receipts minted outside our system (e.g., manual operations)
/// and receipts transferred to the bot wallet from other addresses.
pub(crate) struct ReceiptBackfiller<ProviderType, Handler>
where
    Handler: ItnReceiptHandler,
{
    provider: ProviderType,
    receipt_contract: Address,
    bot_wallet: Address,
    chain_id: u64,
    network: Network,
    vault: Address,
    store: Arc<Store<ReceiptInventory>>,
    pool: Pool<Sqlite>,
    handler: Handler,
}

/// Bundles [`ReceiptBackfiller`] construction inputs to stay within clippy's
/// argument limit.
pub(crate) struct ReceiptBackfillDeps<ProviderType, Handler> {
    pub(crate) provider: ProviderType,
    pub(crate) receipt_contract: Address,
    pub(crate) bot_wallet: Address,
    pub(crate) chain_id: u64,
    pub(crate) network: Network,
    pub(crate) vault: Address,
    pub(crate) store: Arc<Store<ReceiptInventory>>,
    pub(crate) pool: Pool<Sqlite>,
    pub(crate) handler: Handler,
}

#[derive(Debug)]
pub(crate) struct BackfillResult {
    pub(crate) processed_count: u64,
    pub(crate) skipped_zero_balance: u64,
    pub(crate) reconciled_count: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BackfillError {
    #[error("RPC error: {0}")]
    Rpc(#[from] RpcError<TransportErrorKind>),
    #[error("Failed to decode TransferSingle event: {0}")]
    SolTypes(#[from] alloy::sol_types::Error),
    #[error("Missing transaction hash in log")]
    MissingTxHash,
    #[error("Missing block number in log")]
    MissingBlockNumber,
    #[error("Integer conversion error: {0}")]
    IntConversion(#[from] std::num::TryFromIntError),
    #[error("Contract call error: {0}")]
    ContractCall(#[from] alloy::contract::Error),
    #[error("CQRS error: {0}")]
    Aggregate(#[from] AggregateError<ReceiptInventoryError>),
    #[error("Checkpoint error: {0}")]
    Checkpoint(#[from] CheckpointError),
    /// Inventory could not be loaded while checking an ITN Deposit for
    /// duplicate `issuer_request_id`. Fail closed before advancing the
    /// backfill checkpoint so the same block range is retried once inventory
    /// is readable again.
    #[error(
        "ITN inventory unreadable for issuer_request_id {issuer_request_id} \
         (receipt_id {receipt_id}); refusing discovery and checkpoint advance"
    )]
    ItnInventoryUnreadable {
        issuer_request_id: IssuerMintRequestId,
        receipt_id: ReceiptId,
    },
}

impl<ProviderType, Handler> ReceiptBackfiller<ProviderType, Handler>
where
    Handler: ItnReceiptHandler,
{
    pub(crate) fn new(
        deps: ReceiptBackfillDeps<ProviderType, Handler>,
    ) -> Self {
        let ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id,
            network,
            vault,
            store,
            pool,
            handler,
        } = deps;

        Self {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id,
            network,
            vault,
            store,
            pool,
            handler,
        }
    }
}

impl<ProviderType, Handler> ReceiptBackfiller<ProviderType, Handler>
where
    ProviderType: alloy::providers::Provider + Clone + Send + Sync,
    Handler: ItnReceiptHandler,
{
    /// Scans historic Deposit and ERC-1155 transfer events to discover
    /// receipts the bot owns.
    ///
    /// For each receipt discovered:
    /// 1. Checks the on-chain balance at the read block, never below
    ///    `head_block` (not just transfer amount, since receipts may have been
    ///    partially burned)
    /// 2. If balance > 0 and not already tracked, emits DiscoverReceipt command
    ///
    /// `from_block` is the block to start scanning from. On first run, pass
    /// the configured backfill start block. On subsequent runs, read the
    /// previous checkpoint via
    /// `load_receipt_backfill(pool, network, vault)`.
    ///
    /// `head_block` is the chain head to scan up to. The caller fetches it.
    ///
    /// Queries are chunked to avoid RPC response size limits.
    ///
    /// This is idempotent - running multiple times produces the same result.
    pub(crate) async fn backfill_receipts(
        &self,
        from_block: u64,
        head_block: u64,
    ) -> Result<BackfillResult, BackfillError> {
        let current_block = head_block;

        if from_block > current_block {
            info!(target: "receipt", from_block,
                current_block,
                "Backfill skipped: from_block is ahead of current block"
            );

            return Ok(BackfillResult {
                processed_count: 0,
                skipped_zero_balance: 0,
                reconciled_count: 0,
            });
        }

        trace!(target: "receipt", receipt_contract = %self.receipt_contract,
            bot_wallet = %self.bot_wallet,
            from_block,
            to_block = current_block,
            "Starting receipt backfill"
        );

        let mut all_discovery_logs: Vec<Log> = Vec::new();
        let mut all_reconciliation_ids: Vec<ReceiptId> = Vec::new();

        for (chunk_from, chunk_to) in
            block_ranges(from_block, current_block, BLOCK_CHUNK_SIZE)
        {
            let fetched =
                self.fetch_logs_for_range(chunk_from, chunk_to).await?;

            trace!(target: "receipt", chunk_from,
                chunk_to,
                discovery_logs = fetched.discovery_logs.len(),
                reconciliation_ids = fetched.reconciliation_receipt_ids.len(),
                "Processed block range"
            );

            all_discovery_logs.extend(fetched.discovery_logs);
            all_reconciliation_ids.extend(fetched.reconciliation_receipt_ids);
        }

        // Read balances at the block the logs reach, or, when the chain moved
        // on during the scan, at a fresh head less `READ_BLOCK_MARGIN`. A read
        // at `latest` can reach a node that is behind the node that served
        // the logs: a receipt that just arrived then reads zero, is skipped,
        // and the checkpoint moves past its block for good. An explicit block
        // fails on a node that does not have it yet, so the pass fails
        // without moving the checkpoint, and the next pass retries. At
        // startup, a failed pass stops startup, as any backfill RPC error
        // does, and the service restarts. After a long scan, the margin keeps
        // a backend that is a block or two behind the fresh head from failing
        // the pass; after a short scan, the reads stay at the pass head. The
        // fresh head keeps the reads at recent state, which a node that is
        // not an archive node still serves after a long scan.
        // It also makes the window in which a settled burn can outrun these
        // reads (see `reconcile_receipt`) the margin plus the time from this
        // call to the reads, not the whole scan. A pass with no logs reads
        // nothing, so it skips the call.
        let read_block = if all_discovery_logs.is_empty()
            && all_reconciliation_ids.is_empty()
        {
            current_block
        } else {
            self.provider
                .get_block_number()
                .await?
                .saturating_sub(READ_BLOCK_MARGIN)
                .max(current_block)
        };

        // Process discovery logs (Deposit + inbound transfers)
        let discoveries = all_discovery_logs
            .iter()
            .map(Self::parse_log)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .unique_by(|discovery| discovery.receipt_id);

        // Balance reads are network calls, so they run concurrently. Every
        // discovery then writes to this vault's one inventory aggregate, so
        // the writes run one at a time, in the order the pass collected the
        // logs: concurrent writers race on the aggregate's optimistic
        // concurrency check, and a pass with many discoveries can lose that
        // race on every retry and fail. A rollback's restart finds every
        // returned receipt in one pass.
        let readings: Vec<_> = stream::iter(discoveries)
            .map(|discovery| self.read_discovery_balance(discovery, read_block))
            .buffered(MAX_CONCURRENT_BALANCE_CHECKS)
            .try_collect()
            .await?;

        let mut processed_count = 0u64;
        let mut skipped_zero_balance = 0u64;
        for (discovery, current_balance) in readings {
            match self.process_discovery(discovery, current_balance).await? {
                ProcessOutcome::Processed => processed_count += 1,
                ProcessOutcome::ZeroBalance => skipped_zero_balance += 1,
            }
        }

        // Process reconciliation events (Withdraw + outbound transfers).
        // Once a recorded migration moved this vault's custody away from the
        // signing wallet, `balanceOf(bot_wallet)` readings mean nothing and
        // could only be refused as `CustodyDisplaced` — a wallet rotation's
        // own outbound transfers land here on the next catch-up, so they are
        // skipped at INFO until the subsystem retires (RAI-1223). The
        // orchestrator cutover records no migration, so it never lands here.
        // The discovery path above needs no gate: a zero balance returns
        // before any command is dispatched.
        let unique_reconciliation_ids: Vec<_> =
            all_reconciliation_ids.into_iter().unique().collect();

        let reconciled_count = if unique_reconciliation_ids.is_empty() {
            0
        } else if load_inventory(&self.store, self.chain_id, &self.vault)
            .await?
            .custody()
            .moved_away_from(self.bot_wallet)
        {
            info!(target: "receipt", vault = %self.vault,
                wallet = %self.bot_wallet,
                skipped = unique_reconciliation_ids.len(),
                "Custody recorded at a migrated destination; skipping \
                 outbound-transfer reconciliation"
            );
            0
        } else {
            let reconciled = u64::try_from(unique_reconciliation_ids.len())?;
            for receipt_id in unique_reconciliation_ids {
                self.reconcile_receipt(receipt_id, read_block).await?;
            }
            reconciled
        };

        advance_receipt_backfill(
            &self.pool,
            self.network,
            self.vault,
            current_block,
        )
        .await?;

        trace!(target: "receipt", processed_count,
            skipped_zero_balance,
            reconciled_count,
            checkpoint_block = current_block,
            read_block,
            "Receipt backfill complete"
        );

        Ok(BackfillResult {
            processed_count,
            skipped_zero_balance,
            reconciled_count,
        })
    }

    /// Fetches and categorizes logs for a block range into discovery logs
    /// (Deposit + inbound transfers) and reconciliation logs (Withdraw +
    /// outbound transfers).
    async fn fetch_logs_for_range(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> Result<FetchedLogs, BackfillError> {
        let vault_contract =
            OffchainAssetReceiptVault::new(self.vault, &self.provider);

        // --- Discovery logs: Deposit + inbound transfers ---

        // Deposit events (owner is not indexed, so we filter client-side).
        let deposit_filter = vault_contract
            .Deposit_filter()
            .from_block(from_block)
            .to_block(to_block)
            .filter;

        let deposit_logs = self.provider.get_logs(&deposit_filter).await?;

        let mut discovery_logs: Vec<Log> = deposit_logs
            .into_iter()
            .filter_map(|log| {
                match OffchainAssetReceiptVault::Deposit::decode_log(&log.inner)
                {
                    Ok(event) if event.owner == self.bot_wallet => {
                        Some(Ok(log))
                    }
                    Ok(_) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Inbound ERC-1155 transfers (topic3 = to = bot_wallet).
        let transfer_single_in = self
            .provider
            .get_logs(
                &transfer_single_filter(
                    self.receipt_contract,
                    self.bot_wallet,
                    TransferDirection::Inbound,
                )
                .from_block(from_block)
                .to_block(to_block),
            )
            .await?;

        let transfer_batch_in = self
            .provider
            .get_logs(
                &transfer_batch_filter(
                    self.receipt_contract,
                    self.bot_wallet,
                    TransferDirection::Inbound,
                )
                .from_block(from_block)
                .to_block(to_block),
            )
            .await?;

        for log in transfer_single_in {
            let event = Receipt::TransferSingle::decode_log(&log.inner)?;
            if event.to == self.bot_wallet && !event.from.is_zero() {
                discovery_logs.push(log);
            }
        }

        for log in transfer_batch_in {
            let event = Receipt::TransferBatch::decode_log(&log.inner)?;
            if event.to == self.bot_wallet && !event.from.is_zero() {
                discovery_logs.push(log);
            }
        }

        // --- Reconciliation logs: Withdraw + outbound transfers ---

        let mut reconciliation_receipt_ids: Vec<ReceiptId> = Vec::new();

        // Withdraw events (owner is not indexed, filter client-side).
        let withdraw_filter = vault_contract
            .Withdraw_filter()
            .from_block(from_block)
            .to_block(to_block)
            .filter;

        let withdraw_logs = self.provider.get_logs(&withdraw_filter).await?;

        for log in withdraw_logs {
            let event =
                OffchainAssetReceiptVault::Withdraw::decode_log(&log.inner)?;
            if event.owner == self.bot_wallet {
                reconciliation_receipt_ids.push(ReceiptId::from(event.id));
            }
        }

        // Outbound ERC-1155 transfers (topic2 = from = bot_wallet).
        let transfer_single_out = self
            .provider
            .get_logs(
                &transfer_single_filter(
                    self.receipt_contract,
                    self.bot_wallet,
                    TransferDirection::Outbound,
                )
                .from_block(from_block)
                .to_block(to_block),
            )
            .await?;

        let transfer_batch_out = self
            .provider
            .get_logs(
                &transfer_batch_filter(
                    self.receipt_contract,
                    self.bot_wallet,
                    TransferDirection::Outbound,
                )
                .from_block(from_block)
                .to_block(to_block),
            )
            .await?;

        for log in transfer_single_out {
            let event = Receipt::TransferSingle::decode_log(&log.inner)?;
            if event.from == self.bot_wallet && !event.to.is_zero() {
                reconciliation_receipt_ids.push(ReceiptId::from(event.id));
            }
        }

        for log in transfer_batch_out {
            let event = Receipt::TransferBatch::decode_log(&log.inner)?;
            if event.from == self.bot_wallet && !event.to.is_zero() {
                for receipt_id_raw in &event.ids {
                    reconciliation_receipt_ids
                        .push(ReceiptId::from(*receipt_id_raw));
                }
            }
        }

        Ok(FetchedLogs { discovery_logs, reconciliation_receipt_ids })
    }

    /// Parses a log into one or more `ReceiptDiscovery` entries.
    ///
    /// Deposit logs produce a single entry with receipt information.
    /// TransferSingle logs produce a single entry without receipt information.
    /// TransferBatch logs produce one entry per (id, value) pair.
    fn parse_log(log: &Log) -> Result<Vec<ReceiptDiscovery>, BackfillError> {
        let tx_hash =
            log.transaction_hash.ok_or(BackfillError::MissingTxHash)?;
        let block_number =
            log.block_number.ok_or(BackfillError::MissingBlockNumber)?;

        // Try Deposit first, then TransferSingle, then TransferBatch
        if let Ok(event) =
            OffchainAssetReceiptVault::Deposit::decode_log(&log.inner)
        {
            return Ok(vec![ReceiptDiscovery {
                receipt_id: ReceiptId::from(event.id),
                tx_hash,
                block_number,
                receipt_information: event.receiptInformation.clone(),
            }]);
        }

        if let Ok(event) = Receipt::TransferSingle::decode_log(&log.inner) {
            return Ok(vec![ReceiptDiscovery {
                receipt_id: ReceiptId::from(event.id),
                tx_hash,
                block_number,
                receipt_information: Bytes::new(),
            }]);
        }

        if let Ok(event) = Receipt::TransferBatch::decode_log(&log.inner) {
            return Ok(event
                .ids
                .iter()
                .map(|id| ReceiptDiscovery {
                    receipt_id: ReceiptId::from(*id),
                    tx_hash,
                    block_number,
                    receipt_information: Bytes::new(),
                })
                .collect());
        }

        Err(BackfillError::SolTypes(alloy::sol_types::Error::custom(
            "Log did not match Deposit, TransferSingle, or TransferBatch",
        )))
    }

    async fn read_discovery_balance(
        &self,
        discovery: ReceiptDiscovery,
        read_block: u64,
    ) -> Result<(ReceiptDiscovery, U256), BackfillError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        // Pinned to `read_block`, never below the block the logs reach (see
        // `backfill_receipts`). This reading also reconciles a receipt that
        // inventory already tracks, with the side effect `reconcile_receipt`
        // describes.
        let current_balance = receipt_contract
            .balanceOf(self.bot_wallet, discovery.receipt_id.inner())
            .block(BlockId::number(read_block))
            .call()
            .await?;

        Ok((discovery, current_balance))
    }

    async fn process_discovery(
        &self,
        discovery: ReceiptDiscovery,
        current_balance: U256,
    ) -> Result<ProcessOutcome, BackfillError> {
        if current_balance.is_zero() {
            return Ok(ProcessOutcome::ZeroBalance);
        }

        let (source, receipt_info) =
            determine_source(&discovery.receipt_information);
        let receipt_info_bytes = if discovery.receipt_information.is_empty() {
            None
        } else {
            Some(discovery.receipt_information.clone())
        };

        // Inventory is 1:1 on issuer_request_id for ITN mints. A second Deposit
        // with a different receipt_id / tx_hash for the same request is an
        // unrecoverable double-mint signal (operator: burn-excess / investigate).
        // Do not apply DiscoverReceipt — that would insert the duplicate and
        // overwrite `itn_receipts[issuer_request_id]`. Inventory load failure
        // also fails closed (skip discovery) so we never invent a second index.
        if let ReceiptSource::Itn { ref issuer_request_id } = source {
            match self
                .detect_duplicate_itn_deposit(
                    issuer_request_id,
                    discovery.receipt_id,
                    discovery.tx_hash,
                )
                .await
            {
                ItnDepositCheck::Clear => {}
                ItnDepositCheck::Conflict => {
                    // The checkpoint advances past this block once the pass
                    // completes and the range is never re-scanned, so the
                    // duplicate must be recorded now or it survives only as
                    // the log line above. `DiscoverReceipt` is still skipped:
                    // the event records the observation, never tracked balance.
                    send_receipt_inventory_command(
                        &self.store,
                        self.chain_id,
                        &self.vault,
                        ReceiptInventoryCommand::RecordConflictingItnDeposit {
                            issuer_request_id: issuer_request_id.clone(),
                            discovered_receipt_id: discovery.receipt_id,
                            discovered_tx_hash: discovery.tx_hash,
                            discovered_block_number: discovery.block_number,
                        },
                    )
                    .await?;

                    return Ok(ProcessOutcome::Processed);
                }
                ItnDepositCheck::Unreadable => {
                    // Transient inventory failure: stop the pass so the
                    // checkpoint is not advanced past this block range.
                    return Err(BackfillError::ItnInventoryUnreadable {
                        issuer_request_id: issuer_request_id.clone(),
                        receipt_id: discovery.receipt_id,
                    });
                }
            }
        }

        send_receipt_inventory_command(
            &self.store,
            self.chain_id,
            &self.vault,
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: discovery.receipt_id,
                balance: Shares::from(current_balance),
                block_number: discovery.block_number,
                tx_hash: discovery.tx_hash,
                source: source.clone(),
                receipt_info: receipt_info.map(Box::new),
                receipt_info_bytes,
            },
        )
        .await?;

        // For already-known receipts, DiscoverReceipt is a no-op. Reconcile
        // ensures the aggregate reflects the current on-chain balance (e.g.,
        // after an inbound transfer increases the balance).
        send_receipt_inventory_command(
            &self.store,
            self.chain_id,
            &self.vault,
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id: discovery.receipt_id,
                on_chain_balance: Shares::from(current_balance),
                observed_wallet: self.bot_wallet,
            },
        )
        .await?;

        trace!(target: "receipt", receipt_id = %discovery.receipt_id,
            balance = %current_balance,
            "Processed receipt"
        );

        if let ReceiptSource::Itn { issuer_request_id } = source {
            self.handler.on_itn_receipt_discovered(issuer_request_id).await;
        }

        Ok(ProcessOutcome::Processed)
    }

    /// Check whether a second Deposit/receipt for an already-tracked ITN mint
    /// id is being discovered. Inventory is 1:1 on `issuer_request_id`, so a
    /// conflicting receipt id or tx hash is operator-actionable double-mint
    /// evidence (use `issuer burn-excess` after proving the excess deposit).
    ///
    /// Inventory load failure is fail-closed (`Unreadable`): the caller must
    /// not apply `DiscoverReceipt` without a successful conflict check.
    async fn detect_duplicate_itn_deposit(
        &self,
        issuer_request_id: &IssuerMintRequestId,
        discovered_receipt_id: ReceiptId,
        discovered_tx_hash: TxHash,
    ) -> ItnDepositCheck {
        let inventory =
            match load_inventory(&self.store, self.chain_id, &self.vault).await
            {
                Ok(inventory) => inventory,
                Err(error) => {
                    warn!(
                        target: "receipt",
                        issuer_request_id = %issuer_request_id,
                        discovered_receipt_id = %discovered_receipt_id,
                        discovered_tx_hash = %discovered_tx_hash,
                        vault = %self.vault,
                        chain_id = self.chain_id,
                        error = %error,
                        "Failed to load inventory for ITN duplicate-deposit \
                         check; refusing discovery until inventory is readable"
                    );
                    return ItnDepositCheck::Unreadable;
                }
            };

        let Some(tracked) = inventory.conflicting_itn_deposit(
            issuer_request_id,
            discovered_receipt_id,
            discovered_tx_hash,
        ) else {
            return ItnDepositCheck::Clear;
        };

        error!(
            target: "receipt",
            issuer_request_id = %issuer_request_id,
            tracked = %tracked,
            existing_receipt_id = %tracked.receipt_id(),
            discovered_receipt_id = %discovered_receipt_id,
            discovered_tx_hash = %discovered_tx_hash,
            vault = %self.vault,
            chain_id = self.chain_id,
            "Duplicate Deposit/receipt for already-tracked issuer_request_id; \
             operator intervention required (do not mint again; consider burn-excess)"
        );

        ItnDepositCheck::Conflict
    }

    /// Queries the on-chain balance of a receipt at `read_block` and
    /// reconciles the aggregate.
    ///
    /// The read is pinned for the reason `backfill_receipts` gives. A
    /// burn of ours that lands after `read_block` can settle in inventory
    /// before this read, so the read can briefly restore the shares that
    /// burn consumed. The next pass scans the burn's block and reconciles
    /// the receipt again. `backfill_receipts` fetches `read_block` after the
    /// log scan, so this window is `READ_BLOCK_MARGIN` plus the time from that
    /// fetch to this read.
    async fn reconcile_receipt(
        &self,
        receipt_id: ReceiptId,
        read_block: u64,
    ) -> Result<(), BackfillError> {
        let receipt_contract =
            Receipt::new(self.receipt_contract, &self.provider);

        let on_chain_balance = receipt_contract
            .balanceOf(self.bot_wallet, receipt_id.inner())
            .block(BlockId::number(read_block))
            .call()
            .await?;

        send_receipt_inventory_command(
            &self.store,
            self.chain_id,
            &self.vault,
            ReceiptInventoryCommand::ReconcileBalance {
                receipt_id,
                on_chain_balance: Shares::from(on_chain_balance),
                observed_wallet: self.bot_wallet,
            },
        )
        .await?;

        trace!(target: "receipt", receipt_id = %receipt_id,
            on_chain_balance = %on_chain_balance,
            "Receipt balance reconciled"
        );

        Ok(())
    }
}

/// Outcome of the ITN 1:1 deposit conflict check before applying discovery.
enum ItnDepositCheck {
    /// No prior ITN receipt, or same identity rediscovery.
    Clear,
    /// Second deposit conflicts with inventory — do not DiscoverReceipt.
    Conflict,
    /// Inventory could not be loaded — fail closed (do not DiscoverReceipt).
    Unreadable,
}

struct FetchedLogs {
    discovery_logs: Vec<Log>,
    reconciliation_receipt_ids: Vec<ReceiptId>,
}

struct ReceiptDiscovery {
    receipt_id: ReceiptId,
    tx_hash: TxHash,
    block_number: u64,
    receipt_information: Bytes,
}

enum ProcessOutcome {
    Processed,
    ZeroBalance,
}

/// Called when the receipt backfiller discovers an ITN receipt (a receipt
/// with a valid `issuer_request_id` in its receiptInformation).
///
/// Invoked only after `DiscoverReceipt` has already persisted the receipt
/// (including `tx_hash`) to inventory. On-chain evidence therefore lives in
/// the inventory aggregate; this callback only needs the mint id so recovery
/// can re-drive via the per-state jobs, which load the receipt (and its
/// `tx_hash`) from inventory before recording or re-submitting.
#[async_trait]
pub(crate) trait ItnReceiptHandler: Send + Sync {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
    );
}

#[async_trait]
impl<T: ItnReceiptHandler + Sync> ItnReceiptHandler for &T {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
    ) {
        (*self).on_itn_receipt_discovered(issuer_request_id).await;
    }
}

/// No-op implementation of `ItnReceiptHandler` for contexts where ITN
/// receipt discovery does not need to trigger any action (e.g., startup
/// backfill, which runs before recovery handles stuck mints separately).
pub(crate) struct NoOpItnHandler;

#[async_trait]
impl ItnReceiptHandler for NoOpItnHandler {
    async fn on_itn_receipt_discovered(
        &self,
        _issuer_request_id: IssuerMintRequestId,
    ) {
    }
}

/// Direction of token transfer relative to the bot wallet.
///
/// ERC-1155 TransferSingle/TransferBatch events have indexed `from` (topic2)
/// and `to` (topic3) fields. Using directional filters reduces RPC payload
/// by filtering at the node level rather than client-side.
#[derive(Clone, Copy)]
pub(crate) enum TransferDirection {
    /// Tokens received: `to == bot_wallet` (topic3)
    Inbound,
    /// Tokens sent: `from == bot_wallet` (topic2)
    Outbound,
}

/// Builds a log filter for ERC-1155 TransferSingle events on the given
/// contract, filtered by indexed topic for the specified direction.
pub(crate) fn transfer_single_filter(
    receipt_contract: Address,
    bot_wallet: Address,
    direction: TransferDirection,
) -> Filter {
    let base = Filter::new()
        .address(receipt_contract)
        .event_signature(Receipt::TransferSingle::SIGNATURE_HASH);

    match direction {
        TransferDirection::Inbound => base.topic3(bot_wallet.into_word()),
        TransferDirection::Outbound => base.topic2(bot_wallet.into_word()),
    }
}

/// Builds a log filter for ERC-1155 TransferBatch events on the given
/// contract, filtered by indexed topic for the specified direction.
pub(crate) fn transfer_batch_filter(
    receipt_contract: Address,
    bot_wallet: Address,
    direction: TransferDirection,
) -> Filter {
    let base = Filter::new()
        .address(receipt_contract)
        .event_signature(Receipt::TransferBatch::SIGNATURE_HASH);

    match direction {
        TransferDirection::Inbound => base.topic3(bot_wallet.into_word()),
        TransferDirection::Outbound => base.topic2(bot_wallet.into_word()),
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, B256, Bytes, U256, address, b256};
    use alloy::providers::ext::AnvilApi;
    use alloy::providers::mock::Asserter;
    use alloy::providers::{Provider, ProviderBuilder};
    use alloy::rpc::types::Log;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use event_sorcery::{Store, test_store};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::{Pool, Sqlite};
    use std::sync::Arc;
    use tracing_test::traced_test;

    use super::{
        BackfillError, NoOpItnHandler, READ_BLOCK_MARGIN, ReceiptBackfillDeps,
        ReceiptBackfiller,
    };
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::poll_checkpoint::{
        load_checkpoint_block, receipt_backfill_name,
    };
    use crate::receipt_inventory::{
        ReceiptId, ReceiptInventory, ReceiptInventoryCommand, ReceiptSource,
        ReceiptVaultKey, Shares, load_inventory,
    };
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm, logs_contain_at};
    use crate::tokenized_asset::Network;

    async fn setup_test_pool() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    fn test_addresses() -> (Address, Address, Address) {
        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let vault = address!("0x3333333333333333333333333333333333333333");
        (receipt_contract, bot_wallet, vault)
    }

    struct DepositLogParams {
        vault: Address,
        sender: Address,
        owner: Address,
        assets: U256,
        shares: U256,
        id: U256,
        receipt_information: Bytes,
        tx_hash: B256,
        block_number: u64,
    }

    fn create_deposit_log(params: DepositLogParams) -> Log {
        let event = OffchainAssetReceiptVault::Deposit {
            sender: params.sender,
            owner: params.owner,
            assets: params.assets,
            shares: params.shares,
            id: params.id,
            receiptInformation: params.receipt_information,
        };

        Log {
            inner: alloy::primitives::Log {
                address: params.vault,
                data: event.encode_log_data(),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(params.block_number),
            block_timestamp: None,
            transaction_hash: Some(params.tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    async fn setup_store() -> (Arc<Store<ReceiptInventory>>, Pool<Sqlite>) {
        let pool = setup_test_pool().await;
        let store = Arc::new(test_store::<ReceiptInventory>(pool.clone(), ()));
        (store, pool)
    }

    /// Pushes empty responses for the inbound transfer, withdraw, and
    /// outbound transfer `get_logs` calls that the backfiller makes beyond
    /// the Deposit query. Order matches `fetch_logs_for_range`:
    /// inbound TransferSingle, inbound TransferBatch, Withdraw,
    /// outbound TransferSingle, outbound TransferBatch.
    fn push_empty_non_deposit_logs(asserter: &Asserter) {
        for _ in 0..5 {
            asserter.push_success(&Vec::<Log>::new());
        }
    }

    /// Pushes empty responses for the 3 reconciliation queries
    /// (Withdraw, outbound TransferSingle, outbound TransferBatch).
    fn push_empty_reconciliation_logs(asserter: &Asserter) {
        for _ in 0..3 {
            asserter.push_success(&Vec::<Log>::new());
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_discovers_receipt_from_historic_deposit() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf)
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store,
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(result.processed_count, 1);
        assert_eq!(result.skipped_zero_balance, 0);

        let test = "backfill_discovers_receipt_from_historic_deposit";
        assert!(
            logs_contain_at!(
                tracing::Level::TRACE,
                &[test, "Processed block range"]
            ),
            "Expected DEBUG log for block range processing"
        );
        assert!(
            logs_contain_at!(
                tracing::Level::TRACE,
                &[test, "Processed receipt"]
            ),
            "Expected DEBUG log for processed receipt"
        );
    }

    /// Every discovery in a pass writes to the vault's one inventory
    /// aggregate. Concurrent writers race on its optimistic concurrency check,
    /// and a pass this size could lose the race on every retry and fail, which
    /// at startup stops the service. The restart after a rollback finds every
    /// returned receipt in one pass, so the pass must never race itself.
    #[tokio::test]
    #[traced_test]
    async fn backfill_records_many_discoveries_without_racing_itself() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;
        let balance = U256::from(1000);
        let receipt_count = 65u64;

        let deposit_logs: Vec<Log> = (1..=receipt_count)
            .map(|receipt_id| {
                create_deposit_log(DepositLogParams {
                    vault,
                    sender: bot_wallet,
                    owner: bot_wallet,
                    assets: balance,
                    shares: balance,
                    id: U256::from(receipt_id),
                    receipt_information: Bytes::new(),
                    tx_hash: B256::from(U256::from(receipt_id)),
                    block_number: 100 + receipt_id,
                })
            })
            .collect();

        let asserter = Asserter::new();
        asserter.push_success(&deposit_logs);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(200u64));
        for _ in 0..receipt_count {
            asserter.push_success(&balance.to_be_bytes::<32>());
        }

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool,
            handler: NoOpItnHandler,
        });

        let result = backfiller.backfill_receipts(0, 200).await.unwrap();

        assert_eq!(result.processed_count, receipt_count);
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        assert_eq!(
            inventory.receipts_with_balance().len(),
            usize::try_from(receipt_count).unwrap(),
            "inventory must track every receipt the pass found"
        );
        assert!(
            !logs_contain("optimistic-concurrency conflict"),
            "one pass must never race itself on the inventory aggregate"
        );
    }

    #[tokio::test]
    async fn backfill_is_idempotent() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        let current_block = 200u64;

        // First run
        let asserter1 = Asserter::new();
        asserter1.push_success(&vec![deposit_log.clone()]);
        push_empty_non_deposit_logs(&asserter1);
        asserter1.push_success(&U256::from(current_block));
        asserter1.push_success(&balance.to_be_bytes::<32>());

        let provider1 = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter1);

        let backfiller1 = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider: provider1,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result1 =
            backfiller1.backfill_receipts(0, current_block).await.unwrap();
        assert_eq!(result1.processed_count, 1);

        // After the first run the aggregate has exactly one Discovered event
        // and the SQL checkpoint matches the chain head.
        let inventory_after_first =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        assert_eq!(
            inventory_after_first.receipts_with_balance().len(),
            1,
            "first run should produce exactly one receipt"
        );
        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, vault),
            )
            .await
            .unwrap(),
            Some(current_block),
            "first run should write the checkpoint to the SQL table"
        );

        // Second run with the same deposit log: command must be idempotent.
        let asserter2 = Asserter::new();
        asserter2.push_success(&vec![deposit_log]);
        push_empty_non_deposit_logs(&asserter2);
        asserter2.push_success(&U256::from(current_block));
        asserter2.push_success(&balance.to_be_bytes::<32>());

        let provider2 = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter2);

        let backfiller2 = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider: provider2,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result2 =
            backfiller2.backfill_receipts(0, current_block).await.unwrap();
        assert_eq!(result2.processed_count, 1);

        // After the second run the aggregate still has exactly one receipt
        // (no duplicate Discovered event applied) and the checkpoint is
        // unchanged.
        let inventory_after_second =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        assert_eq!(
            inventory_after_second.receipts_with_balance().len(),
            1,
            "second run must not produce a duplicate receipt"
        );
        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, vault),
            )
            .await
            .unwrap(),
            Some(current_block),
            "second run must not regress the checkpoint"
        );
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_skips_zero_balance_receipts() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(99);
        let deposit_amount = U256::from(500);
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: deposit_amount,
            shares: deposit_amount,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 200,
        });

        let asserter = Asserter::new();
        let current_block = 300u64;

        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf) - zero because receipt was fully burned
        asserter.push_success(&U256::ZERO.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store,
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(result.processed_count, 0);
        assert_eq!(result.skipped_zero_balance, 1);

        let test = "backfill_skips_zero_balance_receipts";
        assert!(
            logs_contain_at!(
                tracing::Level::TRACE,
                &[test, "Processed block range"]
            ),
            "Expected DEBUG log for block range processing"
        );
        assert!(
            !logs_contain_at!(
                tracing::Level::TRACE,
                &[test, "Processed receipt"]
            ),
            "Should NOT log processed receipt when all receipts have zero balance"
        );
    }

    #[tokio::test]
    async fn backfill_uses_current_onchain_balance_not_deposit_amount() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(77);
        let original_deposit = U256::from(1000);
        let current_balance = U256::from(300); // Partially burned
        let tx_hash = b256!(
            "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: original_deposit,
            shares: original_deposit,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 300,
        });

        let asserter = Asserter::new();
        let current_block = 400u64;

        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf) - returns current balance, not original deposit
        asserter.push_success(&current_balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(result.processed_count, 1);

        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].available_balance.inner(), current_balance);
    }

    #[tokio::test]
    async fn backfill_filters_deposits_by_owner() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");

        let balance = U256::from(1000);

        // Deposit to bot_wallet (should be processed)
        let deposit_for_bot = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: U256::from(1),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        // Deposit to other_wallet (should be filtered out)
        let deposit_for_other = create_deposit_log(DepositLogParams {
            vault,
            sender: other_wallet,
            owner: other_wallet,
            assets: balance,
            shares: balance,
            id: U256::from(2),
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            ),
            block_number: 101,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter) - both deposits returned, filtered client-side
        asserter.push_success(&vec![deposit_for_bot, deposit_for_other]);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf) - only called for bot's receipt
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store,
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(result.processed_count, 1);
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_emits_checkpoint_after_processing() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(42);
        let balance = U256::from(1000);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash,
            block_number: 100,
        });

        let asserter = Asserter::new();
        let current_block = 150u64;

        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        push_empty_non_deposit_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf)
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store,
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        backfiller.backfill_receipts(0, current_block).await.unwrap();

        let recorded = load_checkpoint_block(
            &pool,
            &receipt_backfill_name(Network::Base, vault),
        )
        .await
        .unwrap();
        assert_eq!(
            recorded,
            Some(current_block),
            "Backfill should checkpoint to current block after processing"
        );

        assert!(
            logs_contain_at!(
                tracing::Level::TRACE,
                &[
                    "backfill_emits_checkpoint_after_processing",
                    "Receipt backfill complete"
                ]
            ),
            "Expected INFO log for backfill completion"
        );
    }

    /// Verifies that `transfer_single_filter` and `transfer_batch_filter`
    /// bind `bot_wallet` to the correct ERC-1155 indexed topic slot:
    ///   Inbound  -> topic3 (to)
    ///   Outbound -> topic2 (from)
    ///
    /// A flipped index would silently miss all relevant transfer events
    /// while still compiling, so this regression test is load-bearing.
    #[test]
    fn transfer_filters_use_correct_topic_indices() {
        use alloy::primitives::address;
        use alloy::rpc::types::Filter;

        use super::{
            TransferDirection, transfer_batch_filter, transfer_single_filter,
        };
        use crate::bindings::Receipt;

        let receipt_contract =
            address!("0x1111111111111111111111111111111111111111");
        let bot_wallet = address!("0x2222222222222222222222222222222222222222");
        let wallet_word = bot_wallet.into_word();

        let assert_topic_placement =
            |filter: &Filter, expect_topic2: bool, label: &str| {
                let topics = &filter.topics;

                if expect_topic2 {
                    assert!(
                        topics[2].matches(&wallet_word),
                        "{label}: expected topic2 to contain bot_wallet"
                    );
                    assert!(
                        topics[3].is_empty(),
                        "{label}: expected topic3 to be empty"
                    );
                } else {
                    assert!(
                        topics[2].is_empty(),
                        "{label}: expected topic2 to be empty"
                    );
                    assert!(
                        topics[3].matches(&wallet_word),
                        "{label}: expected topic3 to contain bot_wallet"
                    );
                }

                assert!(
                    !topics[0].is_empty(),
                    "{label}: expected topic0 (event signature) to be set"
                );
            };

        let single_in = transfer_single_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Inbound,
        );
        assert_topic_placement(&single_in, false, "TransferSingle Inbound");

        let single_out = transfer_single_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Outbound,
        );
        assert_topic_placement(&single_out, true, "TransferSingle Outbound");

        let batch_in = transfer_batch_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Inbound,
        );
        assert_topic_placement(&batch_in, false, "TransferBatch Inbound");

        let batch_out = transfer_batch_filter(
            receipt_contract,
            bot_wallet,
            TransferDirection::Outbound,
        );
        assert_topic_placement(&batch_out, true, "TransferBatch Outbound");

        assert_ne!(
            single_in.topics[0], batch_in.topics[0],
            "TransferSingle and TransferBatch should have different signatures"
        );

        assert!(
            single_in.topics[0]
                .matches(&Receipt::TransferSingle::SIGNATURE_HASH),
            "TransferSingle filter should use TransferSingle signature"
        );
        assert!(
            batch_in.topics[0].matches(&Receipt::TransferBatch::SIGNATURE_HASH),
            "TransferBatch filter should use TransferBatch signature"
        );
    }

    #[test]
    fn block_ranges_single_chunk() {
        let ranges: Vec<_> = super::block_ranges(0, 100, 2000).collect();
        assert_eq!(ranges, vec![(0, 100)]);
    }

    #[test]
    fn block_ranges_exact_multiple() {
        let ranges: Vec<_> = super::block_ranges(0, 3999, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999)]);
    }

    #[test]
    fn block_ranges_with_remainder() {
        let ranges: Vec<_> = super::block_ranges(0, 5000, 2000).collect();
        assert_eq!(ranges, vec![(0, 1999), (2000, 3999), (4000, 5000)]);
    }

    #[test]
    fn block_ranges_from_nonzero() {
        let ranges: Vec<_> = super::block_ranges(1000, 4500, 2000).collect();
        assert_eq!(ranges, vec![(1000, 2999), (3000, 4500)]);
    }

    #[test]
    fn block_ranges_empty_when_from_equals_to() {
        let ranges: Vec<_> = super::block_ranges(100, 100, 2000).collect();
        assert_eq!(ranges, vec![(100, 100)]);
    }

    #[derive(Clone, Copy)]
    struct TransferSingleLogParams {
        receipt_contract: Address,
        operator: Address,
        from: Address,
        to: Address,
        id: U256,
        value: U256,
        tx_hash: B256,
        block_number: u64,
    }

    fn create_transfer_single_log(params: TransferSingleLogParams) -> Log {
        use crate::bindings::Receipt;

        let event = Receipt::TransferSingle {
            operator: params.operator,
            from: params.from,
            to: params.to,
            id: params.id,
            value: params.value,
        };

        Log {
            inner: alloy::primitives::Log {
                address: params.receipt_contract,
                data: event.encode_log_data(),
            },
            block_hash: Some(b256!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            block_number: Some(params.block_number),
            block_timestamp: None,
            transaction_hash: Some(params.tx_hash),
            transaction_index: Some(0),
            log_index: Some(0),
            removed: false,
        }
    }

    #[tokio::test]
    #[traced_test]
    async fn backfill_discovers_receipt_from_inbound_transfer() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(55);
        let balance = U256::from(750);
        let tx_hash = b256!(
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );

        let transfer_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash,
                block_number: 150,
            });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter) - no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) - one inbound transfer
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter) - no batch transfers
        asserter.push_success(&Vec::<Log>::new());
        push_empty_reconciliation_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf)
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(result.processed_count, 1);
        assert_eq!(result.skipped_zero_balance, 0);

        // Verify the receipt was actually discovered with correct ID and balance
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(receipts.len(), 1, "Should discover exactly one receipt");
        assert_eq!(
            receipts[0].receipt_id,
            ReceiptId::from(receipt_id),
            "Discovered receipt ID should match the transfer"
        );
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(balance),
            "Balance should match the on-chain balanceOf response"
        );
    }

    #[tokio::test]
    async fn backfill_filters_outbound_and_mint_transfers() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (store, pool) = setup_store().await;

        let tx_hash = b256!(
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );

        // Outbound transfer (from == bot_wallet) — should be filtered
        let outbound_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: bot_wallet,
                from: bot_wallet,
                to: other_wallet,
                id: U256::from(1),
                value: U256::from(100),
                tx_hash,
                block_number: 100,
            });

        // Mint transfer (from == address(0)) — should be filtered
        let mint_log = create_transfer_single_log(TransferSingleLogParams {
            receipt_contract,
            operator: bot_wallet,
            from: Address::ZERO,
            to: bot_wallet,
            id: U256::from(2),
            value: U256::from(200),
            tx_hash,
            block_number: 101,
        });

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter) - no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) - outbound + mint transfers
        asserter.push_success(&vec![outbound_log, mint_log]);
        // eth_getLogs (TransferBatch filter) - no batch transfers
        asserter.push_success(&Vec::<Log>::new());
        push_empty_reconciliation_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // No balanceOf calls since both transfers are filtered

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        assert_eq!(
            result.processed_count, 0,
            "Outbound and mint transfers should not be processed"
        );
        assert_eq!(result.skipped_zero_balance, 0);

        // Verify: aggregate has no receipts at all
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        assert!(
            inventory.receipts_with_balance().is_empty(),
            "No receipts should be discovered from outbound/mint transfers"
        );
    }

    /// Catch-up after a wallet rotation sees the rotation's own outbound
    /// transfers.
    /// Once recorded custody moved away from the signing wallet, those
    /// reconciliation readings are skipped at INFO instead of manufacturing
    /// `CustodyDisplaced` refusals — the mocked provider deliberately serves
    /// no `balanceOf` response, so a dispatched reading would fail the test.
    #[tokio::test]
    #[traced_test]
    async fn backfill_skips_outbound_reconciliation_for_a_migrated_vault() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let destination =
            address!("0x5555555555555555555555555555555555555555");
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(7);
        let tx_hash = b256!(
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );

        let key = ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault);
        store
            .send(
                &key,
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(receipt_id),
                    balance: Shares::from(U256::from(100)),
                    block_number: 1,
                    tx_hash,
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();
        store
            .send(
                &key,
                ReceiptInventoryCommand::ConfirmCustody { holder: bot_wallet },
            )
            .await
            .unwrap();
        store
            .send(
                &key,
                ReceiptInventoryCommand::RecordCustodyMigration {
                    from: bot_wallet,
                    to: destination,
                    tx_hash: None,
                },
            )
            .await
            .unwrap();

        // The migration's own outbound transfer, observed on catch-up.
        let outbound_log =
            create_transfer_single_log(TransferSingleLogParams {
                receipt_contract,
                operator: bot_wallet,
                from: bot_wallet,
                to: destination,
                id: receipt_id,
                value: U256::from(100),
                tx_hash,
                block_number: 100,
            });

        let asserter = Asserter::new();
        // Discovery queries: Deposit, inbound TransferSingle, inbound
        // TransferBatch — all empty.
        for _ in 0..3 {
            asserter.push_success(&Vec::<Log>::new());
        }
        // Reconciliation queries: Withdraw empty, outbound TransferSingle
        // carries the migration transfer, outbound TransferBatch empty.
        asserter.push_success(&Vec::<Log>::new());
        asserter.push_success(&vec![outbound_log]);
        asserter.push_success(&Vec::<Log>::new());
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(200u64));
        // Deliberately NO balanceOf response.

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result = backfiller.backfill_receipts(0, 200).await.unwrap();

        assert_eq!(
            result.reconciled_count, 0,
            "the migrated vault's outbound reconciliation must be skipped"
        );

        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        assert_eq!(
            inventory.receipts_with_balance().len(),
            1,
            "the migrated inventory must survive the pass intact"
        );
        assert_eq!(
            inventory.custody().holder(),
            Some(destination),
            "the skip must not touch recorded custody"
        );

        assert!(logs_contain_at!(
            tracing::Level::INFO,
            &["skipping", "outbound-transfer reconciliation"]
        ));
    }

    #[tokio::test]
    async fn backfill_deduplicates_deposit_and_transfer_for_same_receipt() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (store, pool) = setup_store().await;

        let receipt_id = U256::from(88);
        let balance = U256::from(500);

        // Same receipt discovered via both Deposit and TransferSingle
        let deposit_log = create_deposit_log(DepositLogParams {
            vault,
            sender: bot_wallet,
            owner: bot_wallet,
            assets: balance,
            shares: balance,
            id: receipt_id,
            receipt_information: Bytes::new(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            block_number: 100,
        });

        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id,
                value: balance,
                tx_hash: b256!(
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                ),
                block_number: 110,
            },
        );

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter)
        asserter.push_success(&vec![deposit_log]);
        // eth_getLogs (TransferSingle filter)
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter)
        asserter.push_success(&Vec::<Log>::new());
        push_empty_reconciliation_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf) — only one call since deduplicated
        asserter.push_success(&balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();

        // Should only process once despite appearing in both Deposit and Transfer
        assert_eq!(result.processed_count, 1);

        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(
            receipts.len(),
            1,
            "Should have exactly one receipt after deduplication"
        );
        assert_eq!(
            receipts[0].receipt_id,
            ReceiptId::from(receipt_id),
            "Receipt ID should match the deduplicated receipt"
        );
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(balance),
            "Balance should match the on-chain balanceOf response"
        );
    }

    /// Verifies that backfill reconciles an already-known receipt's balance
    /// upward when an inbound transfer increases the on-chain balance.
    #[tokio::test]
    async fn backfill_reconciles_known_receipt_balance_upward() {
        let (receipt_contract, bot_wallet, vault) = test_addresses();
        let other_wallet =
            address!("0x4444444444444444444444444444444444444444");
        let (store, pool) = setup_store().await;

        let receipt_id_raw = U256::from(55);
        let stale_balance = U256::from(500);
        let new_on_chain_balance = U256::from(750);

        // Seed aggregate with a receipt at a stale lower balance
        store
            .send(
                &ReceiptVaultKey::new(ANVIL_CHAIN_ID, vault),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(receipt_id_raw),
                    balance: Shares::from(stale_balance),
                    block_number: 50,
                    tx_hash: b256!(
                        "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                    ),
                    source: ReceiptSource::External,
                    receipt_info: None,
                    receipt_info_bytes: None,
                },
            )
            .await
            .unwrap();

        // Inbound transfer log (someone sent tokens to bot_wallet)
        let transfer_log = create_transfer_single_log(
            TransferSingleLogParams {
                receipt_contract,
                operator: other_wallet,
                from: other_wallet,
                to: bot_wallet,
                id: receipt_id_raw,
                value: U256::from(250),
                tx_hash: b256!(
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                ),
                block_number: 150,
            },
        );

        let asserter = Asserter::new();
        let current_block = 200u64;

        // eth_getLogs (Deposit filter) — no deposits
        asserter.push_success(&Vec::<Log>::new());
        // eth_getLogs (TransferSingle filter) — one inbound transfer
        asserter.push_success(&vec![transfer_log]);
        // eth_getLogs (TransferBatch filter) — none
        asserter.push_success(&Vec::<Log>::new());
        push_empty_reconciliation_logs(&asserter);
        // eth_blockNumber (the read block)
        asserter.push_success(&U256::from(current_block));
        // eth_call (balanceOf) — returns the new higher balance
        asserter.push_success(&new_on_chain_balance.to_be_bytes::<32>());

        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(PrivateKeySigner::random()))
            .connect_mocked_client(asserter);

        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet,
            chain_id: ANVIL_CHAIN_ID,
            network: Network::Base,
            vault,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let result =
            backfiller.backfill_receipts(0, current_block).await.unwrap();
        assert_eq!(result.processed_count, 1);

        // Verify balance was reconciled upward from 500 to 750
        let inventory =
            load_inventory(&store, ANVIL_CHAIN_ID, &vault).await.unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(new_on_chain_balance),
            "Backfill should reconcile known receipt balance upward"
        );
    }

    /// A `LocalEvm` whose wallet can deposit into and redeem from the vault.
    async fn local_evm_with_vault_roles() -> LocalEvm {
        let evm = LocalEvm::new().await.unwrap();
        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_withdraw_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();
        evm
    }

    async fn local_evm_wallet_provider(
        evm: &LocalEvm,
    ) -> impl Provider + Clone {
        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap()
    }

    /// Burns `shares` of the wallet's receipt through the vault's `redeem`,
    /// the call a vault-direct burn makes, in a new block.
    async fn redeem_on_chain(
        evm: &LocalEvm,
        provider: &impl Provider,
        receipt_id: U256,
        shares: U256,
    ) {
        let receipt =
            OffchainAssetReceiptVault::new(evm.vault_address, provider)
                .redeem(
                    shares,
                    evm.wallet_address,
                    evm.wallet_address,
                    receipt_id,
                    Bytes::new(),
                )
                .send()
                .await
                .unwrap()
                .get_receipt()
                .await
                .unwrap();
        assert!(receipt.status(), "the test redeem must succeed");
    }

    /// The logs can come from a node that is ahead of the node that answers
    /// the balance reads. A read at `latest` would then miss a receipt that
    /// just arrived, and the checkpoint would move past its block for good.
    /// Here the pass head is past Anvil's head, and Anvil's head is the fresh
    /// head: the read stays at the pass head, Anvil does not have that block
    /// yet, and the pass fails with the checkpoint unmoved.
    #[traced_test]
    #[tokio::test]
    async fn a_read_node_behind_the_logs_fails_the_pass() {
        let evm = local_evm_with_vault_roles().await;
        let deposited = U256::from(100) * U256::from(10).pow(U256::from(18));
        evm.mint_directly(deposited, evm.wallet_address).await.unwrap();
        let provider = local_evm_wallet_provider(&evm).await;
        let head_block = provider.get_block_number().await.unwrap() + 5;
        let receipt_contract = Address::from(
            OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                .receipt()
                .call()
                .await
                .unwrap()
                .0,
        );
        let (store, pool) = setup_store().await;
        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider,
            receipt_contract,
            bot_wallet: evm.wallet_address,
            chain_id: evm.chain_id,
            network: Network::Base,
            vault: evm.vault_address,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });

        let error =
            backfiller.backfill_receipts(0, head_block).await.unwrap_err();

        assert!(
            matches!(error, BackfillError::ContractCall(_)),
            "the balance read must fail, got {error:?}"
        );
        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, evm.vault_address),
            )
            .await
            .unwrap(),
            None,
            "a failed pass must not move the checkpoint"
        );
        let inventory =
            load_inventory(&store, evm.chain_id, &evm.vault_address)
                .await
                .unwrap();
        assert!(inventory.receipts_with_balance().is_empty());
        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &["Processed block range", "discovery_logs=1"]
        ));
    }

    /// A burn inside the pass range puts the receipt in the reconciliation
    /// set. A second burn lands after the pass head, during the scan, and
    /// more blocks follow it. The read goes to the fresh head less
    /// `READ_BLOCK_MARGIN`, which is the second burn's block here, so the
    /// reading includes the second burn and does not restore the shares it
    /// consumed. The checkpoint still stops at the pass head, so the next
    /// pass scans the second burn's block.
    #[traced_test]
    #[tokio::test]
    async fn reconciliation_reads_a_burn_after_the_pass_head() {
        let evm = local_evm_with_vault_roles().await;
        let deposited = U256::from(100) * U256::from(10).pow(U256::from(18));
        let (receipt_id, shares) =
            evm.mint_directly(deposited, evm.wallet_address).await.unwrap();
        let provider = local_evm_wallet_provider(&evm).await;
        let receipt_contract = Address::from(
            OffchainAssetReceiptVault::new(evm.vault_address, &provider)
                .receipt()
                .call()
                .await
                .unwrap()
                .0,
        );
        let (store, pool) = setup_store().await;
        let backfiller = ReceiptBackfiller::new(ReceiptBackfillDeps {
            provider: provider.clone(),
            receipt_contract,
            bot_wallet: evm.wallet_address,
            chain_id: evm.chain_id,
            network: Network::Base,
            vault: evm.vault_address,
            store: store.clone(),
            pool: pool.clone(),
            handler: NoOpItnHandler,
        });
        let discovery_head = provider.get_block_number().await.unwrap();
        backfiller.backfill_receipts(0, discovery_head).await.unwrap();

        let half = shares / U256::from(2);
        redeem_on_chain(&evm, &provider, receipt_id, half).await;
        let head_block = provider.get_block_number().await.unwrap();
        let quarter = shares / U256::from(4);
        redeem_on_chain(&evm, &provider, receipt_id, quarter).await;
        let second_burn_block = provider.get_block_number().await.unwrap();
        provider.anvil_mine(Some(READ_BLOCK_MARGIN), None).await.unwrap();

        let result = backfiller
            .backfill_receipts(discovery_head + 1, head_block)
            .await
            .unwrap();

        assert_eq!(result.reconciled_count, 1);
        let held_now = shares - half - quarter;
        let inventory =
            load_inventory(&store, evm.chain_id, &evm.vault_address)
                .await
                .unwrap();
        let receipts = inventory.receipts_with_balance();
        assert_eq!(receipts.len(), 1);
        assert_eq!(
            receipts[0].available_balance,
            Shares::from(held_now),
            "the reading must include the burn after the pass head"
        );
        assert_eq!(
            load_checkpoint_block(
                &pool,
                &receipt_backfill_name(Network::Base, evm.vault_address),
            )
            .await
            .unwrap(),
            Some(head_block),
            "the checkpoint must stop at the pass head"
        );
        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &[
                "Receipt balance reconciled",
                &ReceiptId::from(receipt_id).to_string(),
                &held_now.to_string(),
            ]
        ));
        assert!(logs_contain_at!(
            tracing::Level::TRACE,
            &[
                "Receipt backfill complete",
                &format!("checkpoint_block={head_block}"),
                &format!("read_block={second_burn_block}"),
            ]
        ));
    }
}
