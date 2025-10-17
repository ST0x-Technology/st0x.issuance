use cqrs_es::persist::PersistenceError;

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
