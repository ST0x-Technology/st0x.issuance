use async_trait::async_trait;
use cqrs_es::Aggregate;
use cqrs_es::persist::{
    PersistedEventRepository, PersistenceError, ReplayStream, SerializedEvent,
    SerializedSnapshot,
};
use serde_json::Value;
use sqlx::sqlite::SqliteRow;
use sqlx::{Pool, Row, Sqlite};

use crate::sql_query::SqlQueryFactory;

/// Errors that can occur during SQLite event repository operations
#[derive(Debug, thiserror::Error)]
pub enum SqliteAggregateError {
    /// Optimistic locking conflict - the aggregate was modified concurrently
    #[error("Optimistic lock error: aggregate has been modified concurrently")]
    OptimisticLock,

    /// Database connection or query error
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    /// Event or snapshot deserialization error
    #[error("Event deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),

    /// Integer conversion error (e.g., sequence number overflow)
    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
}

impl From<SqliteAggregateError> for PersistenceError {
    fn from(err: SqliteAggregateError) -> Self {
        match err {
            SqliteAggregateError::OptimisticLock => Self::OptimisticLockError,
            SqliteAggregateError::Connection(e) => {
                Self::ConnectionError(Box::new(e))
            }
            SqliteAggregateError::Deserialization(e) => {
                Self::DeserializationError(Box::new(e))
            }
            SqliteAggregateError::TryFromInt(e) => {
                Self::UnknownError(Box::new(e))
            }
        }
    }
}

/// SQLite implementation of the `PersistedEventRepository` trait
///
/// Provides event sourcing persistence backed by SQLite, including:
/// - Event storage with optimistic locking
/// - Snapshot support for performance optimization
/// - Event streaming capabilities
pub struct SqliteEventRepository {
    pool: Pool<Sqlite>,
    query_factory: SqlQueryFactory,
    stream_channel_size: usize,
}

#[async_trait]
impl PersistedEventRepository for SqliteEventRepository {
    async fn get_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self.load_events::<A>(aggregate_id).await?)
    }

    async fn get_last_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, PersistenceError> {
        Ok(self.load_events_since::<A>(aggregate_id, last_sequence).await?)
    }

    async fn get_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, PersistenceError> {
        Ok(self.load_snapshot::<A>(aggregate_id).await?)
    }

    async fn persist<A: Aggregate>(
        &self,
        events: &[SerializedEvent],
        snapshot_update: Option<(String, Value, usize)>,
    ) -> Result<(), PersistenceError> {
        self.insert_events(events).await?;

        if let Some((aggregate_id, aggregate, current_sequence)) =
            snapshot_update
        {
            self.update_snapshot::<A>(
                &aggregate_id,
                current_sequence,
                0,
                aggregate,
            )
            .await?;
        }

        Ok(())
    }

    async fn stream_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(Some(aggregate_id)))
    }

    async fn stream_all_events<A: Aggregate>(
        &self,
    ) -> Result<ReplayStream, PersistenceError> {
        Ok(self.stream_events_impl::<A>(None))
    }
}

impl SqliteEventRepository {
    /// Creates a new `SqliteEventRepository` with default table names
    ///
    /// Uses "events" and "snapshots" as the table names with a default stream channel size of 1000.
    #[must_use]
    pub fn new(pool: Pool<Sqlite>) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new(
                "events".to_string(),
                "snapshots".to_string(),
            ),
            stream_channel_size: 1000,
        }
    }

    /// Creates a new `SqliteEventRepository` with custom table names
    ///
    /// Allows specifying custom names for the events and snapshots tables.
    #[must_use]
    pub const fn with_tables(
        pool: Pool<Sqlite>,
        events_table: String,
        snapshots_table: String,
    ) -> Self {
        Self {
            pool,
            query_factory: SqlQueryFactory::new(events_table, snapshots_table),
            stream_channel_size: 1000,
        }
    }

    /// Sets the channel size for event streaming
    ///
    /// The stream channel size determines how many events can be buffered during streaming operations.
    #[must_use]
    pub const fn with_stream_channel_size(
        mut self,
        stream_channel_size: usize,
    ) -> Self {
        self.stream_channel_size = stream_channel_size;
        self
    }

    async fn load_events<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Vec<SerializedEvent>, SqliteAggregateError> {
        let query = self.query_factory.select_events();
        let rows = sqlx::query(&query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_events_since<A: Aggregate>(
        &self,
        aggregate_id: &str,
        last_sequence: usize,
    ) -> Result<Vec<SerializedEvent>, SqliteAggregateError> {
        let query = self.query_factory.get_last_events();
        let last_sequence_i64 = i64::try_from(last_sequence)?;

        let rows = sqlx::query(&query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .bind(last_sequence_i64)
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(row_to_serialized_event).collect()
    }

    async fn load_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
    ) -> Result<Option<SerializedSnapshot>, SqliteAggregateError> {
        let query = self.query_factory.select_snapshot();
        let row = sqlx::query(&query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .fetch_optional(&self.pool)
            .await?;

        match row {
            None => Ok(None),
            Some(row) => Ok(Some(row_to_serialized_snapshot(&row)?)),
        }
    }

    async fn insert_events(
        &self,
        events: &[SerializedEvent],
    ) -> Result<(), SqliteAggregateError> {
        let insert_query = self.query_factory.insert_event();

        let mut tx = self.pool.begin().await?;

        for event in events {
            let sequence_i64 = i64::try_from(event.sequence)?;

            let result = sqlx::query(&insert_query)
                .bind(&event.aggregate_type)
                .bind(&event.aggregate_id)
                .bind(sequence_i64)
                .bind(&event.event_type)
                .bind(&event.event_version)
                .bind(&event.payload)
                .bind(&event.metadata)
                .execute(&mut *tx)
                .await;

            if let Err(e) = result {
                if is_optimistic_lock_error(&e) {
                    return Err(SqliteAggregateError::OptimisticLock);
                }
                return Err(SqliteAggregateError::Connection(e));
            }
        }

        tx.commit().await?;
        Ok(())
    }

    async fn update_snapshot<A: Aggregate>(
        &self,
        aggregate_id: &str,
        current_sequence: usize,
        _current_snapshot: usize,
        aggregate: Value,
    ) -> Result<(), SqliteAggregateError> {
        let query = self.query_factory.update_snapshot();
        let last_sequence_i64 = i64::try_from(current_sequence)?;

        let timestamp = chrono::Utc::now().to_rfc3339();

        sqlx::query(&query)
            .bind(A::aggregate_type())
            .bind(aggregate_id)
            .bind(last_sequence_i64)
            .bind(&aggregate)
            .bind(&timestamp)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    fn stream_events_impl<A: Aggregate>(
        &self,
        aggregate_id: Option<&str>,
    ) -> ReplayStream {
        let (mut feed, stream) = ReplayStream::new(self.stream_channel_size);

        let pool = self.pool.clone();
        let query = match aggregate_id {
            Some(_) => self.query_factory.select_events(),
            None => self.query_factory.all_events(),
        };
        let aggregate_type = A::aggregate_type();
        let aggregate_id = aggregate_id.map(String::from);

        tokio::spawn(async move {
            let rows = match &aggregate_id {
                Some(id) => {
                    sqlx::query(&query)
                        .bind(&aggregate_type)
                        .bind(id)
                        .fetch_all(&pool)
                        .await
                }
                None => {
                    sqlx::query(&query)
                        .bind(&aggregate_type)
                        .fetch_all(&pool)
                        .await
                }
            };

            let rows = match rows {
                Ok(rows) => rows,
                Err(e) => {
                    let _ = feed
                        .push(Err(PersistenceError::ConnectionError(Box::new(
                            e,
                        ))))
                        .await;
                    return;
                }
            };

            for row in &rows {
                let event = match row_to_serialized_event(row) {
                    Ok(event) => event,
                    Err(e) => {
                        let _ = feed
                            .push(Err(PersistenceError::DeserializationError(
                                Box::new(e),
                            )))
                            .await;
                        return;
                    }
                };

                if feed.push(Ok(event)).await.is_err() {
                    return;
                }
            }
        });

        stream
    }
}

fn row_to_serialized_event(
    row: &SqliteRow,
) -> Result<SerializedEvent, SqliteAggregateError> {
    let sequence_i64: i64 = row.try_get("sequence")?;
    let sequence = usize::try_from(sequence_i64)?;

    let payload_str: String = row.try_get("payload")?;
    let metadata_str: String = row.try_get("metadata")?;

    Ok(SerializedEvent {
        aggregate_type: row.try_get("aggregate_type")?,
        aggregate_id: row.try_get("aggregate_id")?,
        sequence,
        event_type: row.try_get("event_type")?,
        event_version: row.try_get("event_version")?,
        payload: serde_json::from_str(&payload_str)?,
        metadata: serde_json::from_str(&metadata_str)?,
    })
}

fn row_to_serialized_snapshot(
    row: &SqliteRow,
) -> Result<SerializedSnapshot, SqliteAggregateError> {
    let last_sequence_i64: i64 = row.try_get("last_sequence")?;
    let current_sequence = usize::try_from(last_sequence_i64)?;

    let payload_str: String = row.try_get("payload")?;

    Ok(SerializedSnapshot {
        aggregate_id: row.try_get("aggregate_id")?,
        aggregate: serde_json::from_str(&payload_str)?,
        current_sequence,
        current_snapshot: 0,
    })
}

fn is_optimistic_lock_error(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => {
            db_err.is_unique_violation()
                || db_err.message().contains("UNIQUE constraint failed")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::create_test_pool;
    use cqrs_es::DomainEvent;
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate {
        events: Vec<String>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    enum TestEvent {
        Created,
        Updated { value: String },
    }

    impl DomainEvent for TestEvent {
        fn event_type(&self) -> String {
            match self {
                Self::Created => "Created".to_string(),
                Self::Updated { .. } => "Updated".to_string(),
            }
        }

        fn event_version(&self) -> String {
            "1.0".to_string()
        }
    }

    #[derive(Debug)]
    struct TestError;

    impl Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "test error")
        }
    }

    impl std::error::Error for TestError {}

    #[async_trait]
    impl Aggregate for TestAggregate {
        type Command = ();
        type Event = TestEvent;
        type Error = TestError;
        type Services = ();

        fn aggregate_type() -> String {
            "TestAggregate".to_string()
        }

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }

        fn apply(&mut self, event: Self::Event) {
            self.events.push(event.event_type());
        }
    }

    #[tokio::test]
    async fn test_persist_and_load_events() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let event = SerializedEvent {
            aggregate_type: TestAggregate::aggregate_type(),
            aggregate_id: "test-123".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({"test": "data"}),
            metadata: serde_json::json!({}),
        };

        repo.insert_events(std::slice::from_ref(&event)).await.unwrap();

        let loaded =
            repo.load_events::<TestAggregate>("test-123").await.unwrap();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].aggregate_id, "test-123");
        assert_eq!(loaded[0].sequence, 1);
        assert_eq!(loaded[0].event_type, "TestEvent");
    }

    #[tokio::test]
    async fn test_optimistic_locking() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let event = SerializedEvent {
            aggregate_type: TestAggregate::aggregate_type(),
            aggregate_id: "test-456".to_string(),
            sequence: 1,
            event_type: "TestEvent".to_string(),
            event_version: "1.0".to_string(),
            payload: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };

        repo.insert_events(std::slice::from_ref(&event)).await.unwrap();

        let result = repo.insert_events(std::slice::from_ref(&event)).await;

        assert!(matches!(result, Err(SqliteAggregateError::OptimisticLock)));
    }

    #[tokio::test]
    async fn test_snapshot_operations() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let aggregate = serde_json::json!({"state": "data"});

        repo.update_snapshot::<TestAggregate>(
            "test-789",
            5,
            0,
            aggregate.clone(),
        )
        .await
        .unwrap();

        let loaded =
            repo.load_snapshot::<TestAggregate>("test-789").await.unwrap();

        assert!(loaded.is_some());
        let loaded = loaded.unwrap();
        assert_eq!(loaded.aggregate_id, "test-789");
        assert_eq!(loaded.current_sequence, 5);
        assert_eq!(loaded.aggregate, aggregate);
    }

    #[tokio::test]
    async fn test_load_events_since() {
        let pool = create_test_pool().await.unwrap();
        let repo = SqliteEventRepository::new(pool);

        let events = vec![
            SerializedEvent {
                aggregate_type: TestAggregate::aggregate_type(),
                aggregate_id: "test-abc".to_string(),
                sequence: 1,
                event_type: "Event1".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
            SerializedEvent {
                aggregate_type: TestAggregate::aggregate_type(),
                aggregate_id: "test-abc".to_string(),
                sequence: 2,
                event_type: "Event2".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
            SerializedEvent {
                aggregate_type: TestAggregate::aggregate_type(),
                aggregate_id: "test-abc".to_string(),
                sequence: 3,
                event_type: "Event3".to_string(),
                event_version: "1.0".to_string(),
                payload: serde_json::json!({}),
                metadata: serde_json::json!({}),
            },
        ];

        repo.insert_events(&events).await.unwrap();

        let loaded = repo
            .load_events_since::<TestAggregate>("test-abc", 1)
            .await
            .unwrap();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].sequence, 2);
        assert_eq!(loaded[1].sequence, 3);
    }
}
