use async_trait::async_trait;
use cqrs_es::persist::{PersistenceError, ViewContext, ViewRepository};
use cqrs_es::{Aggregate, View};
use sqlx::{Pool, Row, Sqlite};
use std::marker::PhantomData;

use crate::sql_query::SqlQueryFactory;

/// Errors that can occur during SQLite view repository operations
#[derive(Debug, thiserror::Error)]
pub enum SqliteViewError {
    /// Optimistic locking conflict - the view was modified concurrently
    #[error("Optimistic lock error: view has been modified concurrently")]
    OptimisticLock,

    /// Database connection or query error
    #[error("Database connection error: {0}")]
    Connection(#[from] sqlx::Error),

    /// View serialization error
    #[error("View serialization error: {0}")]
    Serialization(#[source] serde_json::Error),

    /// View deserialization error
    #[error("View deserialization error: {0}")]
    Deserialization(#[source] serde_json::Error),

    /// Integer conversion error (e.g., version number overflow)
    #[error("Integer conversion error: {0}")]
    TryFromInt(#[from] std::num::TryFromIntError),
}

impl From<SqliteViewError> for PersistenceError {
    fn from(err: SqliteViewError) -> Self {
        match err {
            SqliteViewError::OptimisticLock => Self::OptimisticLockError,
            SqliteViewError::Connection(e) => {
                Self::ConnectionError(Box::new(e))
            }
            SqliteViewError::Serialization(e) => {
                Self::UnknownError(Box::new(e))
            }
            SqliteViewError::Deserialization(e) => {
                Self::DeserializationError(Box::new(e))
            }
            SqliteViewError::TryFromInt(e) => Self::UnknownError(Box::new(e)),
        }
    }
}

/// SQLite implementation of the `ViewRepository` trait
///
/// Provides view persistence backed by SQLite for use with the `GenericQuery`
/// processor in the cqrs-es framework.
///
/// # Type Parameters
///
/// * `V` - The view type that implements `View<A>`
/// * `A` - The aggregate type that the view projects from
///
/// # Example
///
/// ```ignore
/// let pool = Pool::<Sqlite>::connect("sqlite::memory:").await?;
/// let view_repo = SqliteViewRepository::<MintView, Mint>::new(
///     pool,
///     "mint_view".to_string()
/// );
/// ```
pub struct SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    pool: Pool<Sqlite>,
    view_table: String,
    _phantom: PhantomData<(V, A)>,
}

impl<V, A> SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    /// Creates a new `SqliteViewRepository` for a specific view table
    ///
    /// # Arguments
    ///
    /// * `pool` - SQLite connection pool
    /// * `view_table` - Name of the table storing this view (e.g., "mint_view")
    #[must_use]
    pub const fn new(pool: Pool<Sqlite>, view_table: String) -> Self {
        Self { pool, view_table, _phantom: PhantomData }
    }
}

#[async_trait]
impl<V, A> ViewRepository<V, A> for SqliteViewRepository<V, A>
where
    V: View<A>,
    A: Aggregate,
{
    async fn load(&self, view_id: &str) -> Result<Option<V>, PersistenceError> {
        let query = SqlQueryFactory::select_view(&self.view_table);

        let row = sqlx::query(&query)
            .bind(view_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(SqliteViewError::Connection)?;

        match row {
            None => Ok(None),
            Some(row) => {
                let payload_str: String = row
                    .try_get("payload")
                    .map_err(SqliteViewError::Connection)?;

                let view: V = serde_json::from_str(&payload_str)
                    .map_err(SqliteViewError::Deserialization)?;

                Ok(Some(view))
            }
        }
    }

    async fn load_with_context(
        &self,
        _view_id: &str,
    ) -> Result<Option<(V, ViewContext)>, PersistenceError> {
        todo!()
    }

    async fn update_view(
        &self,
        _view: V,
        _context: ViewContext,
    ) -> Result<(), PersistenceError> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use cqrs_es::{DomainEvent, EventEnvelope};
    use serde::{Deserialize, Serialize};
    use std::fmt::{self, Display};

    use super::*;
    use crate::testing::create_test_pool;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
    struct TestAggregate;

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

        fn apply(&mut self, _event: Self::Event) {}
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq)]
    struct TestView {
        count: i64,
        values: Vec<String>,
    }

    impl View<TestAggregate> for TestView {
        fn update(&mut self, event: &EventEnvelope<TestAggregate>) {
            match &event.payload {
                TestEvent::Created => self.count += 1,
                TestEvent::Updated { value } => self.values.push(value.clone()),
            }
        }
    }

    #[tokio::test]
    async fn test_load_nonexistent_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool,
            "test_view".to_string(),
        );

        let result = repo.load("nonexistent").await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_load_view() {
        let pool = create_test_pool().await.unwrap();

        sqlx::query(
            "CREATE TABLE test_view (
                view_id TEXT PRIMARY KEY,
                version BIGINT NOT NULL,
                payload JSON NOT NULL
            )",
        )
        .execute(&pool)
        .await
        .unwrap();

        let test_view = TestView {
            count: 42,
            values: vec!["foo".to_string(), "bar".to_string()],
        };
        let payload = serde_json::to_string(&test_view).unwrap();

        sqlx::query(
            "INSERT INTO test_view (view_id, version, payload) VALUES (?, ?, ?)",
        )
        .bind("test-123")
        .bind(5_i64)
        .bind(&payload)
        .execute(&pool)
        .await
        .unwrap();

        let repo = SqliteViewRepository::<TestView, TestAggregate>::new(
            pool,
            "test_view".to_string(),
        );

        let result = repo.load("test-123").await.unwrap();

        assert!(result.is_some());
        let loaded_view = result.unwrap();
        assert_eq!(loaded_view.count, 42);
        assert_eq!(loaded_view.values, vec!["foo", "bar"]);
    }

    #[test]
    fn test_optimistic_lock_error_conversion() {
        let err = SqliteViewError::OptimisticLock;
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::OptimisticLockError
        ));
    }

    #[test]
    fn test_serialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_deserialization_error_conversion() {
        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(
            persistence_err,
            PersistenceError::DeserializationError(_)
        ));
    }

    #[test]
    fn test_try_from_int_error_conversion() {
        let int_err = i64::try_from(u64::MAX).unwrap_err();
        let err = SqliteViewError::TryFromInt(int_err);
        let persistence_err: PersistenceError = err.into();

        assert!(matches!(persistence_err, PersistenceError::UnknownError(_)));
    }

    #[test]
    fn test_error_display() {
        let err = SqliteViewError::OptimisticLock;
        assert_eq!(
            err.to_string(),
            "Optimistic lock error: view has been modified concurrently"
        );

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Serialization(json_err);
        assert!(err.to_string().contains("View serialization error"));

        let json_err = serde_json::from_str::<i32>("invalid").unwrap_err();
        let err = SqliteViewError::Deserialization(json_err);
        assert!(err.to_string().contains("View deserialization error"));
    }
}
