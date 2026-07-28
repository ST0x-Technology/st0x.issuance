//! Terminalizes a `Failed` redemption whose burn already landed on-chain.
//!
//! The legacy custodian-era recovery gap: a burn submitted through the
//! retired Fireblocks integration is identified in event history only by a
//! backend transaction id the current signing backend cannot look up, so the
//! automated `BurnFailed` recovery finds the share balance already consumed
//! and strands the redemption in `Failed`. The operator supplies the on-chain
//! transaction hash instead; everything else is verified, never trusted: the
//! transaction must be a successful burn on this redemption's vault whose
//! per-receipt withdrawals match the persisted burn plan exactly, and no
//! other redemption's history may already claim it.

use alloy::consensus::Transaction;
use alloy::consensus::transaction::SignerRecoverable;
use alloy::primitives::{Address, B256, U256};
use alloy::providers::Provider;
use cqrs_es::AggregateError;
use event_sorcery::{LifecycleError, Store, StoreBuilder};
use sqlx::{Pool, Sqlite};
use std::collections::HashMap;
use std::sync::Arc;

use super::cmd::RedemptionCommand;
use super::view::RedemptionViewReactor;
use super::{
    BurnRecord, IssuerRedemptionRequestId, Redemption, RedemptionError,
    RedemptionEvent, RedemptionServices,
};
use crate::receipt_inventory::burn_tracking::ReceiptBurnsViewReactor;
use crate::receipt_inventory::{
    ReceiptInventory, ReceiptInventoryCommand, send_receipt_inventory_command,
};
use crate::tokenized_asset::{Network, UnderlyingSymbol};
use crate::vault::{
    BurnVerification, NetworkVaultServices, VerifiedBurn,
    verify_burn_in_receipt,
};

/// What event history proves about the redemption's burn, used to bind an
/// operator-supplied transaction to this redemption and no other.
#[derive(Debug)]
pub(crate) struct LandedBurnEvidence {
    pub(crate) underlying: UnderlyingSymbol,
    pub(crate) network: Network,
    /// The burn plan persisted by the latest `BurningFailed` event. The
    /// on-chain withdrawals of the proving transaction must match it exactly.
    pub(crate) planned_burns: Vec<BurnRecord>,
}

/// Why an operator-supplied transaction does not prove this redemption's
/// burn.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ForceCompleteRefusal {
    #[error(
        "redemption {issuer_request_id} has no event history in this database"
    )]
    UnknownRedemption { issuer_request_id: IssuerRedemptionRequestId },

    #[error(
        "redemption {issuer_request_id} has no persisted burn plan (no \
         BurningFailed event with planned burns); there is nothing to bind \
         the transaction against, so it cannot be force-completed — close it \
         after off-chain reconciliation instead"
    )]
    NoPersistedBurnPlan { issuer_request_id: IssuerRedemptionRequestId },

    #[error(
        "transaction {burn_tx_hash} is already claimed by redemption \
         {claimed_by}; one on-chain burn cannot complete two redemptions"
    )]
    BurnAlreadyClaimed {
        burn_tx_hash: B256,
        claimed_by: IssuerRedemptionRequestId,
    },

    #[error(
        "the transaction's withdrawals do not match the persisted burn plan: \
         planned {planned:?}, on-chain {onchain:?}; a transaction that burns \
         anything other than exactly the planned receipts is some other burn"
    )]
    BurnPlanMismatch { planned: Vec<(U256, U256)>, onchain: Vec<(U256, U256)> },
}

/// An operator-supplied transaction proven on-chain to be exactly the
/// redemption's planned burn.
#[derive(Debug)]
pub(crate) struct VerifiedLandedBurn {
    /// The wallet the transaction's signature recovers to — the burn's owner.
    pub(crate) owner: Address,
    pub(crate) verification: BurnVerification,
}

/// Fetches `burn_tx_hash` from the chain and proves it is the redemption's
/// landed burn: a successful transaction whose per-receipt withdrawals on
/// `vault` — owned by the wallet its own signature recovers to — match the
/// persisted burn plan exactly.
///
/// # Errors
///
/// Returns an error if the transaction or its receipt cannot be fetched, is
/// not a successful burn on `vault`, or its withdrawals do not match the
/// plan.
pub(crate) async fn verify_landed_burn<P: Provider>(
    provider: &P,
    vault: Address,
    burn_tx_hash: B256,
    planned_burns: &[BurnRecord],
) -> anyhow::Result<VerifiedLandedBurn> {
    let transaction = provider
        .get_transaction_by_hash(burn_tx_hash)
        .await?
        .ok_or_else(|| {
            anyhow::anyhow!("transaction {burn_tx_hash} is not on this chain")
        })?;
    let owner = transaction.inner.inner().recover_signer()?;
    let receipt =
        provider.get_transaction_receipt(burn_tx_hash).await?.ok_or_else(
            || anyhow::anyhow!("transaction {burn_tx_hash} has no receipt"),
        )?;

    let verification = verify_burn_in_receipt(
        &receipt,
        vault,
        owner,
        burn_tx_hash,
        transaction.nonce(),
    )?;
    bind_verified_burns(planned_burns, &verification.burns)?;

    Ok(VerifiedLandedBurn { owner, verification })
}

/// What the operator confirmed for the terminal event: the proven hash, the
/// block it landed in, the recorded reason, and any acknowledged stranded
/// signed transaction.
#[derive(Debug)]
pub(crate) struct VerifiedCompletion {
    pub(crate) burn_tx_hash: B256,
    pub(crate) block_number: u64,
    pub(crate) reason: String,
    pub(crate) acknowledged_unresolved_burn_tx_hash: Option<B256>,
}

/// Terminalizes the redemption with the verified burn and settles its receipt
/// reservation — the same settlement a live burn confirmation performs, so
/// the receipts the burn spent stop counting as available inventory.
///
/// # Errors
///
/// Returns an error if the stores cannot be built or either command dispatch
/// fails (including the aggregate refusing a state it cannot terminalize
/// from).
pub(crate) async fn terminalize_and_settle(
    pool: &Pool<Sqlite>,
    chain_id: u64,
    vault: Address,
    issuer_request_id: &IssuerRedemptionRequestId,
    completion: VerifiedCompletion,
) -> anyhow::Result<()> {
    let VerifiedCompletion {
        burn_tx_hash,
        block_number,
        reason,
        acknowledged_unresolved_burn_tx_hash,
    } = completion;

    let store = force_complete_store(pool.clone()).await?;
    let terminalization = store
        .send(
            issuer_request_id,
            RedemptionCommand::ForceCompleteBurn {
                issuer_request_id: issuer_request_id.clone(),
                burn_tx_hash,
                block_number,
                reason,
                acknowledged_unresolved_burn_tx_hash,
            },
        )
        .await;

    // A crash between the two dispatches leaves the redemption `Completed`
    // with its reservation still held. Treating the refusal as "already
    // terminalized, settlement still owed" makes re-running the command the
    // repair for that window instead of a dead end (settlement itself is
    // idempotent) — but only for the identical completion: a redemption
    // completed with some other transaction is a different burn, and settling
    // on its behalf here would act on a hash this run never proved.
    match terminalization {
        Ok(()) => {}
        Err(AggregateError::UserError(LifecycleError::Apply(
            RedemptionError::AlreadyCompleted { .. },
        ))) => {
            let redemption =
                store.load(issuer_request_id).await?.ok_or_else(|| {
                    anyhow::anyhow!(
                        "redemption {issuer_request_id} refused as already \
                         completed but has no loadable state"
                    )
                })?;
            let Redemption::Completed { burn_tx_hash: recorded, .. } =
                redemption
            else {
                anyhow::bail!(
                    "redemption {issuer_request_id} is terminal but not \
                     Completed; refusing to settle its reservation"
                );
            };
            if recorded != burn_tx_hash {
                anyhow::bail!(
                    "redemption {issuer_request_id} already completed with \
                     transaction {recorded}, not {burn_tx_hash}; refusing to \
                     settle on behalf of a completion this run did not prove"
                );
            }

            tracing::warn!(
                %issuer_request_id,
                %burn_tx_hash,
                "redemption already terminalized with this transaction; \
                 proceeding to settle its receipt reservation"
            );
        }
        Err(refusal) => return Err(refusal.into()),
    }

    let inventory_store =
        StoreBuilder::<ReceiptInventory>::new(pool.clone()).build(()).await?;
    send_receipt_inventory_command(
        &inventory_store,
        chain_id,
        &vault,
        ReceiptInventoryCommand::SettleBurn {
            redemption_issuer_request_id: issuer_request_id.clone(),
        },
    )
    .await?;

    Ok(())
}

/// Loads the evidence needed to bind an operator-supplied transaction to this
/// redemption, from the event store rather than the view: the view's `Failed`
/// shape drops the asset and the plan.
///
/// # Errors
///
/// Returns an error if the events cannot be read or deserialized, the
/// redemption has no history, or no burn plan was ever persisted.
pub(crate) async fn landed_burn_evidence(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRedemptionRequestId,
) -> anyhow::Result<LandedBurnEvidence> {
    let aggregate_id = issuer_request_id.to_string();
    let rows = sqlx::query!(
        r#"
        SELECT payload as "payload!: String"
        FROM events
        WHERE aggregate_type = 'Redemption' AND aggregate_id = ?
        ORDER BY sequence
        "#,
        aggregate_id
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        return Err(ForceCompleteRefusal::UnknownRedemption {
            issuer_request_id: issuer_request_id.clone(),
        }
        .into());
    }

    let mut asset = None;
    let mut planned_burns = Vec::new();

    for row in rows {
        match serde_json::from_str(&row.payload)? {
            RedemptionEvent::Detected { underlying, network, .. } => {
                if asset.is_none() {
                    asset = Some((underlying, network));
                }
            }
            RedemptionEvent::BurningFailed { planned_burns: plan, .. }
                if !plan.is_empty() =>
            {
                planned_burns = plan;
            }
            _ => {}
        }
    }

    let Some((underlying, network)) = asset else {
        return Err(ForceCompleteRefusal::UnknownRedemption {
            issuer_request_id: issuer_request_id.clone(),
        }
        .into());
    };

    if planned_burns.is_empty() {
        return Err(ForceCompleteRefusal::NoPersistedBurnPlan {
            issuer_request_id: issuer_request_id.clone(),
        }
        .into());
    }

    Ok(LandedBurnEvidence { underlying, network, planned_burns })
}

/// Refuses when any other redemption's history already mentions
/// `burn_tx_hash`.
///
/// Deliberately conservative: matching the hash anywhere in another
/// aggregate's payloads (not only in terminal events) treats "some other
/// redemption has ever seen this transaction" as a claim, because one
/// on-chain burn completing two redemptions would double-settle backing.
///
/// # Errors
///
/// Returns an error if the events cannot be read, or another redemption
/// mentions the hash.
pub(crate) async fn ensure_burn_unclaimed(
    pool: &Pool<Sqlite>,
    issuer_request_id: &IssuerRedemptionRequestId,
    burn_tx_hash: B256,
) -> anyhow::Result<()> {
    let aggregate_id = issuer_request_id.to_string();
    let hash_fragment = format!("%{burn_tx_hash}%");
    let claimed_by: Option<String> = sqlx::query_scalar(
        "
        SELECT aggregate_id
        FROM events
        WHERE aggregate_type = 'Redemption'
          AND aggregate_id != ?
          AND payload LIKE ?
        LIMIT 1
        ",
    )
    .bind(&aggregate_id)
    .bind(&hash_fragment)
    .fetch_optional(pool)
    .await?;

    let Some(claimed_by) = claimed_by else {
        return Ok(());
    };

    Err(ForceCompleteRefusal::BurnAlreadyClaimed {
        burn_tx_hash,
        claimed_by: claimed_by.parse()?,
    }
    .into())
}

/// Requires the transaction's per-receipt withdrawals to match the persisted
/// burn plan exactly — same receipt identifiers, same amounts, nothing
/// missing and nothing extra.
///
/// # Errors
///
/// Returns an error when the two sets differ in any way.
pub(crate) fn bind_verified_burns(
    planned: &[BurnRecord],
    onchain: &[VerifiedBurn],
) -> Result<(), ForceCompleteRefusal> {
    let mut planned_pairs: Vec<_> = planned
        .iter()
        .map(|record| (record.receipt_id, record.shares_burned))
        .collect();
    let mut onchain_pairs: Vec<_> = onchain
        .iter()
        .map(|burn| (burn.receipt_id, burn.shares_burned))
        .collect();
    planned_pairs.sort_unstable();
    onchain_pairs.sort_unstable();

    if planned_pairs != onchain_pairs {
        return Err(ForceCompleteRefusal::BurnPlanMismatch {
            planned: planned_pairs,
            onchain: onchain_pairs,
        });
    }

    Ok(())
}

/// Builds the redemption store the way production wires it — with the view
/// reactors attached, so the terminal event is reflected in
/// `redemption_view` and `/admin/stuck` stops reporting the redemption when
/// the service next starts.
///
/// The vault-service map is deliberately empty: dispatching
/// `ForceCompleteBurn` never resolves a signing backend, and any future
/// command that did would fail closed with an unconfigured-network error
/// rather than sign with the wrong backend.
///
/// # Errors
///
/// Returns an error if the store cannot be built.
pub(crate) async fn force_complete_store(
    pool: Pool<Sqlite>,
) -> anyhow::Result<Arc<Store<Redemption>>> {
    let store = StoreBuilder::<Redemption>::new(pool.clone())
        .with(Arc::new(RedemptionViewReactor::new(pool.clone())))
        .with(Arc::new(ReceiptBurnsViewReactor::new(pool)))
        .build(RedemptionServices::new(NetworkVaultServices::new(
            HashMap::new(),
        )))
        .await?;

    Ok(store)
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::{Address, Bytes, U256, address, b256, uint};
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use alloy::sol_types::SolEvent;
    use chrono::Utc;
    use cqrs_es::DomainEvent;
    use rust_decimal::Decimal;
    use sqlx::sqlite::SqlitePoolOptions;

    use super::*;
    use crate::bindings::OffchainAssetReceiptVault;
    use crate::receipt_inventory::{ReceiptSource, ReceiptVaultKey};
    use crate::redemption::TxId;
    use crate::test_utils::{ANVIL_CHAIN_ID, LocalEvm};
    use crate::tokenized_asset::TokenSymbol;
    use crate::{Network, Quantity};

    async fn pool_with_migrations() -> Pool<Sqlite> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(":memory:")
            .await
            .unwrap();
        sqlx::migrate!("./migrations").run(&pool).await.unwrap();
        pool
    }

    async fn seed_events(
        pool: &Pool<Sqlite>,
        issuer_request_id: &IssuerRedemptionRequestId,
        events: &[RedemptionEvent],
    ) {
        let aggregate_id = issuer_request_id.to_string();
        for (index, event) in events.iter().enumerate() {
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
                VALUES ('Redemption', ?, ?, ?, '1.0', ?, '{}')
                ",
            )
            .bind(&aggregate_id)
            .bind(i64::try_from(index).unwrap() + 1)
            .bind(event.event_type())
            .bind(serde_json::to_string(event).unwrap())
            .execute(pool)
            .await
            .unwrap();
        }
    }

    fn detected(
        issuer_request_id: &IssuerRedemptionRequestId,
    ) -> RedemptionEvent {
        RedemptionEvent::Detected {
            issuer_request_id: issuer_request_id.clone(),
            underlying: UnderlyingSymbol::new("NVDA").unwrap(),
            token: TokenSymbol::new("tNVDA"),
            network: Network::Base,
            wallet: address!("0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"),
            quantity: Quantity::new(Decimal::new(4, 2)),
            tx_hash: b256!(
                "0x1111111111111111111111111111111111111111111111111111111111111111"
            ),
            block_number: 30_000_000,
            detected_at: Utc::now(),
        }
    }

    fn burning_failed_with(
        issuer_request_id: &IssuerRedemptionRequestId,
        planned_burns: Vec<BurnRecord>,
    ) -> RedemptionEvent {
        RedemptionEvent::BurningFailed {
            issuer_request_id: issuer_request_id.clone(),
            error: "Fireblocks transaction polling timed out".to_string(),
            failed_at: Utc::now(),
            tx_id: Some(TxId::Legacy("fb-1417".to_string())),
            planned_burns,
        }
    }

    fn burning_failed(
        issuer_request_id: &IssuerRedemptionRequestId,
        plan: &[(u64, u64)],
    ) -> RedemptionEvent {
        burning_failed_with(issuer_request_id, planned(plan))
    }

    #[tokio::test]
    async fn evidence_recovers_the_asset_and_the_latest_plan() {
        let pool = pool_with_migrations().await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_events(
            &pool,
            &issuer_request_id,
            &[
                detected(&issuer_request_id),
                burning_failed(&issuer_request_id, &[(1, 100)]),
                burning_failed(&issuer_request_id, &[(3, 40)]),
            ],
        )
        .await;

        let evidence =
            landed_burn_evidence(&pool, &issuer_request_id).await.unwrap();

        assert_eq!(evidence.underlying, UnderlyingSymbol::new("NVDA").unwrap());
        assert_eq!(evidence.network, Network::Base);
        assert_eq!(evidence.planned_burns.len(), 1);
        assert_eq!(evidence.planned_burns[0].receipt_id, U256::from(3));
        assert_eq!(evidence.planned_burns[0].shares_burned, U256::from(40));
    }

    #[tokio::test]
    async fn evidence_refuses_a_redemption_without_a_persisted_plan() {
        let pool = pool_with_migrations().await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_events(&pool, &issuer_request_id, &[detected(&issuer_request_id)])
            .await;

        let error =
            landed_burn_evidence(&pool, &issuer_request_id).await.unwrap_err();

        assert!(matches!(
            error.downcast_ref::<ForceCompleteRefusal>(),
            Some(ForceCompleteRefusal::NoPersistedBurnPlan { .. })
        ));
    }

    #[tokio::test]
    async fn evidence_refuses_an_unknown_redemption() {
        let pool = pool_with_migrations().await;
        let issuer_request_id = IssuerRedemptionRequestId::random();

        let error =
            landed_burn_evidence(&pool, &issuer_request_id).await.unwrap_err();

        assert!(matches!(
            error.downcast_ref::<ForceCompleteRefusal>(),
            Some(ForceCompleteRefusal::UnknownRedemption { .. })
        ));
    }

    #[tokio::test]
    async fn a_hash_claimed_by_another_redemption_is_refused() {
        let pool = pool_with_migrations().await;
        let ours = IssuerRedemptionRequestId::random();
        let theirs = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x2222222222222222222222222222222222222222222222222222222222222222"
        );
        seed_events(
            &pool,
            &theirs,
            &[
                detected(&theirs),
                RedemptionEvent::BurnForceCompleted {
                    issuer_request_id: theirs.clone(),
                    burn_tx_hash,
                    block_number: 31_000_000,
                    reason: "verified".to_string(),
                    acknowledged_unresolved_burn_tx_hash: None,
                    completed_at: Utc::now(),
                },
            ],
        )
        .await;

        let error = ensure_burn_unclaimed(&pool, &ours, burn_tx_hash)
            .await
            .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<ForceCompleteRefusal>(),
            Some(ForceCompleteRefusal::BurnAlreadyClaimed { claimed_by, .. })
                if *claimed_by == theirs
        ));
    }

    struct LandedBurn<P> {
        evm: LocalEvm,
        provider: P,
        receipt_id: U256,
        shares: U256,
        burn_tx_hash: B256,
    }

    /// Deposits into a fresh local vault and burns the position — the
    /// on-chain half both anvil tests bind operator-supplied hashes against.
    /// The quantity is the NVDA amount from the production incident: 0.04
    /// shares.
    async fn landed_burn_on_local_vault() -> LandedBurn<impl Provider> {
        let evm = LocalEvm::new().await.unwrap();
        evm.grant_deposit_role(evm.wallet_address).await.unwrap();
        evm.grant_withdraw_role(evm.wallet_address).await.unwrap();
        evm.grant_certify_role(evm.wallet_address).await.unwrap();
        evm.certify_vault(U256::MAX).await.unwrap();

        let signer = PrivateKeySigner::from_bytes(&evm.private_key).unwrap();
        let provider = ProviderBuilder::new()
            .wallet(EthereumWallet::from(signer))
            .connect(&evm.endpoint)
            .await
            .unwrap();
        let vault =
            OffchainAssetReceiptVault::new(evm.vault_address, provider.clone());

        let shares = U256::from(40) * U256::from(10).pow(U256::from(15));
        let ratio = U256::from(10).pow(U256::from(18));
        let deposit = vault
            .deposit(shares, evm.wallet_address, ratio, Bytes::new())
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let receipt_id = deposit
            .inner
            .logs()
            .iter()
            .find_map(|log| {
                OffchainAssetReceiptVault::Deposit::decode_log(&log.inner).ok()
            })
            .unwrap()
            .id;

        let burn = vault
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

        LandedBurn {
            evm,
            provider,
            receipt_id,
            shares,
            burn_tx_hash: burn.transaction_hash,
        }
    }

    /// The realistic RAI-1417 recovery, end to end against a real vault: a
    /// receipt is deposited and burned on-chain (the legacy landed burn), a
    /// redemption is stranded in `Failed` with exactly that burn plan and a
    /// live receipt reservation, and the operator-supplied hash is verified,
    /// terminalized, and settled — the burned receipts stop counting as
    /// available inventory.
    #[tokio::test]
    async fn a_landed_burn_is_verified_and_terminalizes_the_redemption() {
        let LandedBurn { evm, provider, receipt_id, shares, burn_tx_hash } =
            landed_burn_on_local_vault().await;

        let pool = pool_with_migrations().await;
        let issuer_request_id = IssuerRedemptionRequestId::random();
        seed_events(
            &pool,
            &issuer_request_id,
            &[
                detected(&issuer_request_id),
                burning_failed_with(
                    &issuer_request_id,
                    vec![BurnRecord { receipt_id, shares_burned: shares }],
                ),
                RedemptionEvent::RedemptionFailed {
                    issuer_request_id: issuer_request_id.clone(),
                    reason: "On-chain balance insufficient for BurnFailed \
                             recovery: balance=0, \
                             required=40000000000000000"
                        .to_string(),
                    failed_at: Utc::now(),
                },
            ],
        )
        .await;

        let inventory_store =
            StoreBuilder::<ReceiptInventory>::new(pool.clone())
                .build(())
                .await
                .unwrap();
        send_receipt_inventory_command(
            &inventory_store,
            ANVIL_CHAIN_ID,
            &evm.vault_address,
            ReceiptInventoryCommand::DiscoverReceipt {
                receipt_id: receipt_id.into(),
                balance: shares.into(),
                block_number: 30_000_000,
                tx_hash: B256::random(),
                source: ReceiptSource::External,
                receipt_info: None,
                receipt_info_bytes: None,
            },
        )
        .await
        .unwrap();
        send_receipt_inventory_command(
            &inventory_store,
            ANVIL_CHAIN_ID,
            &evm.vault_address,
            ReceiptInventoryCommand::ReserveBurn {
                redemption_issuer_request_id: issuer_request_id.clone(),
                burns: vec![BurnRecord { receipt_id, shares_burned: shares }],
            },
        )
        .await
        .unwrap();

        let evidence =
            landed_burn_evidence(&pool, &issuer_request_id).await.unwrap();
        let landed = verify_landed_burn(
            &provider,
            evm.vault_address,
            burn_tx_hash,
            &evidence.planned_burns,
        )
        .await
        .unwrap();
        assert_eq!(landed.owner, evm.wallet_address);
        ensure_burn_unclaimed(&pool, &issuer_request_id, burn_tx_hash)
            .await
            .unwrap();

        terminalize_and_settle(
            &pool,
            ANVIL_CHAIN_ID,
            evm.vault_address,
            &issuer_request_id,
            VerifiedCompletion {
                burn_tx_hash,
                block_number: landed.verification.block_number,
                reason: "operator verified the landed burn on-chain"
                    .to_string(),
                acknowledged_unresolved_burn_tx_hash: None,
            },
        )
        .await
        .unwrap();

        let store = force_complete_store(pool.clone()).await.unwrap();
        let redemption = store.load(&issuer_request_id).await.unwrap().unwrap();
        assert!(
            matches!(redemption, Redemption::Completed { burn_tx_hash: recorded, .. } if recorded == burn_tx_hash),
            "the redemption must complete with the verified hash, got \
             {redemption:?}"
        );

        let inventory = inventory_store
            .load(&ReceiptVaultKey::new(ANVIL_CHAIN_ID, evm.vault_address))
            .await
            .unwrap()
            .unwrap();
        assert!(
            inventory.reserved_receipts().is_empty(),
            "settlement must clear the burn reservation, got {:?}",
            inventory.reserved_receipts()
        );
        assert!(
            inventory.receipts_with_balance().is_empty(),
            "the burned receipt must stop counting as available inventory, \
             got {:?}",
            inventory.receipts_with_balance()
        );
    }

    /// A real burn of some other receipt must not verify against this
    /// redemption's plan — the binding is what makes the hash operator-proof.
    #[tokio::test]
    async fn a_burn_of_a_different_receipt_is_refused() {
        let LandedBurn { evm, provider, receipt_id, shares, burn_tx_hash } =
            landed_burn_on_local_vault().await;

        // The plan names a different receipt than the one the transaction
        // actually burned.
        let planned_burns = vec![BurnRecord {
            receipt_id: receipt_id + U256::from(1),
            shares_burned: shares,
        }];

        let error = verify_landed_burn(
            &provider,
            evm.vault_address,
            burn_tx_hash,
            &planned_burns,
        )
        .await
        .unwrap_err();

        assert!(matches!(
            error.downcast_ref::<ForceCompleteRefusal>(),
            Some(ForceCompleteRefusal::BurnPlanMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn a_hash_mentioned_only_by_this_redemption_is_not_claimed() {
        let pool = pool_with_migrations().await;
        let ours = IssuerRedemptionRequestId::random();
        let burn_tx_hash = b256!(
            "0x3333333333333333333333333333333333333333333333333333333333333333"
        );
        seed_events(
            &pool,
            &ours,
            &[
                detected(&ours),
                RedemptionEvent::BurnForceCompleted {
                    issuer_request_id: ours.clone(),
                    burn_tx_hash,
                    block_number: 31_000_000,
                    reason: "verified".to_string(),
                    acknowledged_unresolved_burn_tx_hash: None,
                    completed_at: Utc::now(),
                },
            ],
        )
        .await;

        ensure_burn_unclaimed(&pool, &ours, burn_tx_hash).await.unwrap();
    }

    fn planned(pairs: &[(u64, u64)]) -> Vec<BurnRecord> {
        pairs
            .iter()
            .map(|&(receipt_id, shares)| BurnRecord {
                receipt_id: U256::from(receipt_id),
                shares_burned: U256::from(shares),
            })
            .collect()
    }

    fn onchain(pairs: &[(u64, u64)]) -> Vec<VerifiedBurn> {
        pairs
            .iter()
            .map(|&(receipt_id, shares)| VerifiedBurn {
                sender: Address::repeat_byte(1),
                receiver: Address::repeat_byte(2),
                receipt_id: U256::from(receipt_id),
                shares_burned: U256::from(shares),
            })
            .collect()
    }

    #[test]
    fn binding_accepts_an_exact_match_in_any_order() {
        bind_verified_burns(
            &planned(&[(1, 100), (2, 250)]),
            &onchain(&[(2, 250), (1, 100)]),
        )
        .unwrap();
    }

    #[test]
    fn binding_refuses_a_missing_withdrawal() {
        let refusal = bind_verified_burns(
            &planned(&[(1, 100), (2, 250)]),
            &onchain(&[(1, 100)]),
        )
        .unwrap_err();

        assert!(matches!(
            refusal,
            ForceCompleteRefusal::BurnPlanMismatch { .. }
        ));
    }

    #[test]
    fn binding_refuses_an_extra_withdrawal() {
        let refusal = bind_verified_burns(
            &planned(&[(1, 100)]),
            &onchain(&[(1, 100), (9, 5)]),
        )
        .unwrap_err();

        assert!(matches!(
            refusal,
            ForceCompleteRefusal::BurnPlanMismatch { .. }
        ));
    }

    #[test]
    fn binding_refuses_an_amount_mismatch() {
        let refusal =
            bind_verified_burns(&planned(&[(1, 100)]), &onchain(&[(1, 99)]))
                .unwrap_err();

        assert!(matches!(
            refusal,
            ForceCompleteRefusal::BurnPlanMismatch { .. }
        ));
    }

    #[test]
    fn binding_refuses_an_empty_transaction() {
        let refusal = bind_verified_burns(&planned(&[(1, 100)]), &onchain(&[]))
            .unwrap_err();

        assert!(matches!(
            refusal,
            ForceCompleteRefusal::BurnPlanMismatch { .. }
        ));
    }

    #[test]
    fn shares_conversion_covers_full_width() {
        let record = BurnRecord {
            receipt_id: uint!(3_U256),
            shares_burned: uint!(40_000000000000000_U256),
        };
        bind_verified_burns(
            std::slice::from_ref(&record),
            &[VerifiedBurn {
                sender: Address::repeat_byte(1),
                receiver: Address::repeat_byte(2),
                receipt_id: record.receipt_id,
                shares_burned: record.shares_burned,
            }],
        )
        .unwrap();
    }
}
