use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, CqrsFramework, Query};
use sqlx::{Pool, Sqlite};

use crate::event_repository::SqliteEventRepository;

pub type SqliteCqrs<A> =
    CqrsFramework<A, PersistedEventStore<SqliteEventRepository, A>>;

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
