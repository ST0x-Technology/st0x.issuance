use cqrs_es::persist::PersistenceError;
use cqrs_es::{Aggregate, View};
use sqlx::{Pool, Sqlite};
use std::marker::PhantomData;

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

#[cfg(test)]
mod tests {
    use super::*;

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
