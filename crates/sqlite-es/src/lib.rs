//! SQLite implementation of PersistedEventRepository for cqrs-es
//!
//! This crate provides a SQLite-backed event store for use with the cqrs-es
//! framework. It follows the same pattern as postgres-es and mysql-es.

mod cqrs;
mod event_repository;
mod sql_query;
pub mod testing;
mod view_repository;

pub use cqrs::{SqliteCqrs, sqlite_cqrs};
pub use cqrs_es::persist::ViewContext;
pub use event_repository::{SqliteAggregateError, SqliteEventRepository};
pub use view_repository::{SqliteViewError, SqliteViewRepository};
