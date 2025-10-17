use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, CqrsFramework, Query};
use sqlx::{Pool, Sqlite};

use crate::event_repository::SqliteEventRepository;

/// Type alias for a CQRS framework backed by SQLite
pub type SqliteCqrs<A> =
    CqrsFramework<A, PersistedEventStore<SqliteEventRepository, A>>;

/// Creates a new CQRS framework with SQLite event storage
///
/// # Arguments
///
/// * `pool` - SQLite connection pool
/// * `query_processor` - Vector of query processors for building read models
/// * `services` - Services required by the aggregate
pub fn sqlite_cqrs<A>(
    pool: Pool<Sqlite>,
    query_processor: Vec<Box<dyn Query<A>>>,
    services: A::Services,
) -> SqliteCqrs<A>
where
    A: Aggregate,
{
    let repo = SqliteEventRepository::new(pool);
    let store = PersistedEventStore::new_event_store(repo);
    CqrsFramework::new(store, query_processor, services)
}
