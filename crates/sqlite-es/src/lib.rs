//! SQLite implementation of event sourcing persistence for cqrs-es
//!
//! This crate provides SQLite-backed event store and view repositories for use
//! with the cqrs-es framework. It follows the same pattern as postgres-es and
//! mysql-es.
//!
//! # Features
//!
//! - **Event Store**: Persistent storage for domain events with snapshot support
//! - **View Repository**: Generic view persistence with optimistic locking
//! - **SQLite Backend**: Lightweight, serverless database for event sourcing
//! - **JSON Storage**: Events and views stored as JSON for flexibility
//!
//! # Example Usage
//!
//! ## Event Repository
//!
//! ```ignore
//! use sqlite_es::SqliteEventRepository;
//! use sqlx::SqlitePool;
//!
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//! let repo = SqliteEventRepository::new(pool);
//!
//! // Use with CqrsFramework
//! let cqrs = CqrsFramework::new(store, vec![], Services::default());
//! ```
//!
//! ## View Repository
//!
//! ```ignore
//! use sqlite_es::SqliteViewRepository;
//! use sqlx::SqlitePool;
//! use cqrs_es::persist::GenericQuery;
//!
//! let pool = SqlitePool::connect("sqlite::memory:").await?;
//! let view_repo = SqliteViewRepository::<MyView, MyAggregate>::new(
//!     pool,
//!     "my_view".to_string()
//! );
//!
//! // Use with GenericQuery processor
//! let query = GenericQuery::new(view_repo);
//! ```

mod cqrs;
mod event_repository;
mod sql_query;
pub mod testing;
mod view_repository;

pub use cqrs::{SqliteCqrs, sqlite_cqrs};
pub use cqrs_es::persist::ViewContext;
pub use event_repository::{SqliteAggregateError, SqliteEventRepository};
pub use view_repository::{SqliteViewError, SqliteViewRepository};
