use alloy::primitives::TxHash;
use async_trait::async_trait;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use sqlite_es::SqliteCqrs;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::{IssuerMintRequestId, Mint, MintCommand, MintError};
use crate::receipt_inventory::ItnReceiptHandler;

/// Production handler that triggers mint recovery when an ITN receipt is
/// discovered by the receipt monitor.
#[derive(Clone)]
pub(crate) struct MintRecoveryHandler {
    mint_cqrs: Arc<SqliteCqrs<Mint>>,
}

impl MintRecoveryHandler {
    pub(crate) const fn new(mint_cqrs: Arc<SqliteCqrs<Mint>>) -> Self {
        Self { mint_cqrs }
    }
}

#[async_trait]
impl ItnReceiptHandler for MintRecoveryHandler {
    async fn on_itn_receipt_discovered(
        &self,
        issuer_request_id: IssuerMintRequestId,
        tx_hash: TxHash,
    ) {
        let mint_cqrs = self.mint_cqrs.clone();
        tokio::spawn(async move {
            drive_recovery(&mint_cqrs, issuer_request_id, |id| {
                MintCommand::RecoverFromReceipt {
                    issuer_request_id: id,
                    tx_hash,
                }
            })
            .await;
        });
    }
}

/// Drives a mint through recovery to completion using `MintCommand::Recover`.
pub(crate) async fn recover_mint<ES>(
    mint_cqrs: &CqrsFramework<Mint, ES>,
    issuer_request_id: IssuerMintRequestId,
) where
    ES: EventStore<Mint>,
{
    drive_recovery(mint_cqrs, issuer_request_id, |id| MintCommand::Recover {
        issuer_request_id: id,
    })
    .await;
}

const MAX_RECOVERY_ATTEMPTS: usize = 10;

/// Drives a mint through recovery to completion by repeatedly sending
/// commands built by `make_command` until the mint reaches a terminal state.
///
/// A single recovery command advances the mint by one step (e.g.,
/// `MintingFailed` -> `CallbackPending`). This function loops until
/// `NotRecoverable` is returned, which means the mint has either
/// completed or reached a state that cannot be recovered from.
///
/// Bounded to [`MAX_RECOVERY_ATTEMPTS`] iterations to prevent infinite
/// spinning if a command returns `Ok(())` without advancing state.
async fn drive_recovery<ES>(
    mint_cqrs: &CqrsFramework<Mint, ES>,
    issuer_request_id: IssuerMintRequestId,
    make_command: impl Fn(IssuerMintRequestId) -> MintCommand,
) where
    ES: EventStore<Mint>,
{
    let aggregate_id = issuer_request_id.to_string();

    for attempt in 1..=MAX_RECOVERY_ATTEMPTS {
        let result = mint_cqrs
            .execute(&aggregate_id, make_command(issuer_request_id.clone()))
            .await;

        match result {
            Ok(()) => {
                debug!(
                    issuer_request_id = %issuer_request_id,
                    attempt,
                    "Recovery step succeeded, continuing"
                );
            }
            Err(AggregateError::UserError(MintError::NotRecoverable {
                current_state,
            })) => {
                info!(
                    issuer_request_id = %issuer_request_id,
                    current_state,
                    "Mint recovery complete"
                );
                return;
            }
            Err(err) => {
                warn!(
                    issuer_request_id = %issuer_request_id,
                    error = %err,
                    "Mint recovery failed"
                );
                return;
            }
        }
    }

    error!(
        issuer_request_id = %issuer_request_id,
        aggregate_id,
        max_attempts = MAX_RECOVERY_ATTEMPTS,
        "Mint recovery exceeded maximum attempts without reaching terminal state"
    );
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, address, b256, uint};
    use chrono::Utc;
    use cqrs_es::mem_store::MemStore;
    use cqrs_es::persist::GenericQuery;
    use cqrs_es::{AggregateContext, CqrsFramework, EventStore};
    use rust_decimal::Decimal;
    use sqlite_es::{SqliteViewRepository, sqlite_cqrs};
    use sqlx::sqlite::SqlitePoolOptions;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tracing::Level;
    use tracing_test::traced_test;

    use super::*;
    use crate::alpaca::mock::MockAlpacaService;
    use crate::mint::{
        ClientId, IssuerMintRequestId, MintEvent, MintServices, Network,
        Quantity, TokenSymbol, TokenizationRequestId, UnderlyingSymbol,
    };
    use crate::receipt_inventory::{
        CqrsReceiptService, ReceiptId, ReceiptInventory,
        ReceiptInventoryCommand, ReceiptSource, Shares,
    };
    use crate::test_utils::logs_contain_at;
    use crate::tokenized_asset::{
        TokenizedAsset, TokenizedAssetCommand, view::TokenizedAssetView,
    };
    use crate::vault::ReceiptInformation;
    use crate::vault::mock::MockVaultService;

    type TestMintStore = MemStore<Mint>;

    const VAULT: Address =
        address!("0xcccccccccccccccccccccccccccccccccccccccc");
    const BOT: Address = address!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

    fn test_issuer_request_id() -> IssuerMintRequestId {
        IssuerMintRequestId::new(
            uuid::Uuid::parse_str("00000000-0000-0000-0000-000000000001")
                .unwrap(),
        )
    }

    fn minting_failed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();

        vec![
            MintEvent::Initiated {
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: TokenizationRequestId::new("tok-123"),
                quantity: Quantity::new(Decimal::from(100)),
                underlying: UnderlyingSymbol::new("AAPL"),
                token: TokenSymbol::new("tAAPL"),
                network: Network::new("base"),
                client_id: ClientId::new(),
                wallet: address!("0x1234567890abcdef1234567890abcdef12345678"),
                initiated_at: now,
            },
            MintEvent::JournalConfirmed {
                issuer_request_id: issuer_request_id.clone(),
                confirmed_at: now,
            },
            MintEvent::MintingStarted {
                issuer_request_id: issuer_request_id.clone(),
                started_at: now,
            },
            MintEvent::MintingFailed {
                issuer_request_id: issuer_request_id.clone(),
                error: "timeout".to_string(),
                failed_at: now,
            },
        ]
    }

    fn callback_pending_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = minting_failed_events(issuer_request_id);

        events.push(MintEvent::ExistingMintRecovered {
            issuer_request_id: issuer_request_id.clone(),
            tx_hash: b256!(
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ),
            receipt_id: uint!(42_U256),
            shares_minted: uint!(100_000000000000000000_U256),
            block_number: 1000,
            recovered_at: now,
        });

        events
    }

    fn completed_events(
        issuer_request_id: &IssuerMintRequestId,
    ) -> Vec<MintEvent> {
        let now = Utc::now();
        let mut events = callback_pending_events(issuer_request_id);

        events.push(MintEvent::MintCompleted {
            issuer_request_id: issuer_request_id.clone(),
            completed_at: now,
        });

        events
    }

    async fn setup_mint_cqrs_with_events(
        events: Vec<MintEvent>,
    ) -> (Arc<CqrsFramework<Mint, TestMintStore>>, Arc<TestMintStore>) {
        let issuer_request_id = test_issuer_request_id();

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(":memory:")
            .await
            .unwrap();

        sqlx::migrate!().run(&pool).await.unwrap();

        // Seed tokenized asset view into SQLite so find_vault_by_underlying works
        let asset_view_repo = Arc::new(SqliteViewRepository::<
            TokenizedAssetView,
            TokenizedAsset,
        >::new(
            pool.clone(),
            "tokenized_asset_view".to_string(),
        ));
        let asset_query = GenericQuery::new(asset_view_repo);
        let asset_cqrs =
            sqlite_cqrs(pool.clone(), vec![Box::new(asset_query)], ());
        asset_cqrs
            .execute(
                "AAPL",
                TokenizedAssetCommand::Add {
                    underlying: UnderlyingSymbol::new("AAPL"),
                    token: TokenSymbol::new("tAAPL"),
                    network: Network::new("base"),
                    vault: VAULT,
                },
            )
            .await
            .unwrap();

        // Set up receipt inventory with a receipt for the issuer_request_id
        let receipt_store = Arc::new(MemStore::<ReceiptInventory>::default());
        let receipt_cqrs =
            Arc::new(CqrsFramework::new((*receipt_store).clone(), vec![], ()));

        let receipt_info = ReceiptInformation::new(
            TokenizationRequestId::new("tok-123"),
            issuer_request_id.clone(),
            UnderlyingSymbol::new("AAPL"),
            Quantity::new(Decimal::from(100)),
            Utc::now(),
            None,
        );

        receipt_cqrs
            .execute(
                &VAULT.to_string(),
                ReceiptInventoryCommand::DiscoverReceipt {
                    receipt_id: ReceiptId::from(uint!(42_U256)),
                    balance: Shares::from(
                        uint!(100_000000000000000000_U256),
                    ),
                    block_number: 1000,
                    tx_hash: b256!(
                        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    ),
                    source: ReceiptSource::Itn {
                        issuer_request_id: issuer_request_id.clone(),
                    },
                    receipt_info: Some(receipt_info),
                },
            )
            .await
            .unwrap();

        let services = MintServices {
            vault: Arc::new(MockVaultService::new_success()),
            alpaca: Arc::new(MockAlpacaService::new_success()),
            receipts: Arc::new(CqrsReceiptService::new(
                receipt_store,
                receipt_cqrs,
            )),
            pool,
            bot: BOT,
        };

        let mint_store = Arc::new(MemStore::<Mint>::default());
        let mint_cqrs = Arc::new(CqrsFramework::new(
            (*mint_store).clone(),
            vec![],
            services,
        ));

        let aggregate_id = issuer_request_id.to_string();
        let context = mint_store.load_aggregate(&aggregate_id).await.unwrap();
        mint_store.commit(events, context, HashMap::default()).await.unwrap();

        (mint_cqrs, mint_store)
    }

    #[traced_test]
    #[tokio::test]
    async fn minting_failed_with_receipt_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = minting_failed_events(&issuer_request_id);
        let (mint_cqrs, mint_store) = setup_mint_cqrs_with_events(events).await;

        recover_mint(&mint_cqrs, issuer_request_id.clone()).await;

        let context = mint_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
            .unwrap();

        assert!(
            matches!(context.aggregate(), Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            context.aggregate().state_name()
        );
        assert!(logs_contain_at(Level::INFO, &["Mint recovery complete"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn callback_pending_recovers_to_completed() {
        let issuer_request_id = test_issuer_request_id();
        let events = callback_pending_events(&issuer_request_id);
        let (mint_cqrs, mint_store) = setup_mint_cqrs_with_events(events).await;

        recover_mint(&mint_cqrs, issuer_request_id.clone()).await;

        let context = mint_store
            .load_aggregate(&issuer_request_id.to_string())
            .await
            .unwrap();

        assert!(
            matches!(context.aggregate(), Mint::Completed { .. }),
            "Expected Completed state, got: {}",
            context.aggregate().state_name()
        );
        assert!(logs_contain_at(Level::INFO, &["Mint recovery complete"]));
    }

    #[traced_test]
    #[tokio::test]
    async fn completed_mint_returns_cleanly() {
        let issuer_request_id = test_issuer_request_id();
        let events = completed_events(&issuer_request_id);
        let (mint_cqrs, _mint_store) =
            setup_mint_cqrs_with_events(events).await;

        recover_mint(&mint_cqrs, issuer_request_id).await;

        assert!(logs_contain_at(Level::INFO, &["Mint recovery complete"]));
    }
}
