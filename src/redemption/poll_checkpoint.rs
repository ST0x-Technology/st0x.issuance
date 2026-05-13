use async_trait::async_trait;
use cqrs_es::{Aggregate, DomainEvent};
use serde::{Deserialize, Serialize};

/// Tracks how far the [`TransferPoller`](super::poller::TransferPoller) has
/// scanned. A single global instance (aggregate_id = `"transfer-poller"`)
/// records the last fully-processed block number.
///
/// Using a CQRS aggregate (instead of a raw SQL table) keeps the event store
/// as the single source of truth for all system state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct TransferPollCheckpoint {
    last_processed_block: Option<u64>,
}

impl TransferPollCheckpoint {
    pub(crate) const AGGREGATE_ID: &str = "transfer-poller";

    pub(crate) const fn last_processed_block(&self) -> Option<u64> {
        self.last_processed_block
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum TransferPollCheckpointCommand {
    Advance { block_number: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TransferPollCheckpointEvent {
    Advanced { block_number: u64 },
}

impl DomainEvent for TransferPollCheckpointEvent {
    fn event_type(&self) -> String {
        match self {
            Self::Advanced { .. } => {
                "TransferPollCheckpointEvent::Advanced".to_string()
            }
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Error type for the `TransferPollCheckpoint` aggregate.
///
/// No error variants needed: advancing to the same or older block is
/// silently ignored (returns empty events), and there are no other
/// failure modes for a monotonic counter.
#[derive(Debug)]
pub(crate) enum TransferPollCheckpointError {}

impl std::fmt::Display for TransferPollCheckpointError {
    fn fmt(
        &self,
        _formatter: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        Ok(())
    }
}

impl std::error::Error for TransferPollCheckpointError {}

#[async_trait]
impl Aggregate for TransferPollCheckpoint {
    type Command = TransferPollCheckpointCommand;
    type Event = TransferPollCheckpointEvent;
    type Error = TransferPollCheckpointError;
    type Services = ();

    fn aggregate_type() -> String {
        "TransferPollCheckpoint".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TransferPollCheckpointCommand::Advance { block_number } => {
                // Idempotent: ignore if not advancing past last checkpoint
                if self
                    .last_processed_block
                    .is_some_and(|last| block_number <= last)
                {
                    return Ok(vec![]);
                }

                Ok(vec![TransferPollCheckpointEvent::Advanced { block_number }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            TransferPollCheckpointEvent::Advanced { block_number } => {
                self.last_processed_block = Some(
                    self.last_processed_block
                        .map_or(block_number, |last| last.max(block_number)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::{
        AggregateContext, CqrsFramework, EventStore, mem_store::MemStore,
    };
    use std::sync::Arc;

    use super::*;

    type TestStore = MemStore<TransferPollCheckpoint>;
    type TestCqrs = Arc<CqrsFramework<TransferPollCheckpoint, TestStore>>;

    fn setup_cqrs() -> (TestCqrs, Arc<TestStore>) {
        let store = Arc::new(MemStore::default());
        let cqrs = Arc::new(CqrsFramework::new((*store).clone(), vec![], ()));
        (cqrs, store)
    }

    fn load_checkpoint(
        context: &impl AggregateContext<TransferPollCheckpoint>,
    ) -> Option<u64> {
        context.aggregate().last_processed_block()
    }

    #[tokio::test]
    async fn fresh_aggregate_has_no_checkpoint() {
        let (_cqrs, store) = setup_cqrs();

        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();

        assert_eq!(load_checkpoint(&context), None);
    }

    #[tokio::test]
    async fn advance_sets_checkpoint() {
        let (cqrs, store) = setup_cqrs();

        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 100 },
        )
        .await
        .unwrap();

        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();

        assert_eq!(load_checkpoint(&context), Some(100));
    }

    #[tokio::test]
    async fn advance_is_monotonic() {
        let (cqrs, store) = setup_cqrs();

        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 100 },
        )
        .await
        .unwrap();

        // Attempt to go backwards — should be a no-op
        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 80 },
        )
        .await
        .unwrap();

        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();

        assert_eq!(load_checkpoint(&context), Some(100));
    }

    #[tokio::test]
    async fn advance_is_idempotent() {
        let (cqrs, store) = setup_cqrs();

        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 100 },
        )
        .await
        .unwrap();

        // Same block number — should be a no-op
        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 100 },
        )
        .await
        .unwrap();

        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();

        assert_eq!(load_checkpoint(&context), Some(100));
        // Only one event should exist
        assert_eq!(context.current_sequence, 1);
    }

    #[tokio::test]
    async fn sequential_advances() {
        let (cqrs, store) = setup_cqrs();

        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 100 },
        )
        .await
        .unwrap();

        cqrs.execute(
            TransferPollCheckpoint::AGGREGATE_ID,
            TransferPollCheckpointCommand::Advance { block_number: 200 },
        )
        .await
        .unwrap();

        let context = store
            .load_aggregate(TransferPollCheckpoint::AGGREGATE_ID)
            .await
            .unwrap();

        assert_eq!(load_checkpoint(&context), Some(200));
    }
}
