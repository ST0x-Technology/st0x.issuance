pub(crate) struct SqlQueryFactory {
    events_table: String,
    snapshots_table: String,
}

impl SqlQueryFactory {
    pub(crate) const fn new(
        events_table: String,
        snapshots_table: String,
    ) -> Self {
        Self { events_table, snapshots_table }
    }

    pub(crate) fn select_events(&self) -> String {
        format!(
            "SELECT
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
             FROM {}
             WHERE aggregate_type = ? AND aggregate_id = ?
             ORDER BY sequence",
            self.events_table
        )
    }

    pub(crate) fn insert_event(&self) -> String {
        format!(
            "INSERT INTO {} (
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.events_table
        )
    }

    pub(crate) fn all_events(&self) -> String {
        format!(
            "SELECT
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
             FROM {}
             WHERE aggregate_type = ?
             ORDER BY sequence",
            self.events_table
        )
    }

    pub(crate) fn get_last_events(&self) -> String {
        format!(
            "SELECT
                aggregate_type,
                aggregate_id,
                sequence,
                event_type,
                event_version,
                payload,
                metadata
             FROM {}
             WHERE aggregate_type = ? AND aggregate_id = ? AND sequence > ?
             ORDER BY sequence",
            self.events_table
        )
    }

    pub(crate) fn select_snapshot(&self) -> String {
        format!(
            "SELECT
                aggregate_type,
                aggregate_id,
                last_sequence,
                payload,
                timestamp
             FROM {}
             WHERE aggregate_type = ? AND aggregate_id = ?",
            self.snapshots_table
        )
    }

    pub(crate) fn update_snapshot(&self) -> String {
        format!(
            "INSERT OR REPLACE INTO {} (
                aggregate_type,
                aggregate_id,
                last_sequence,
                payload,
                timestamp
            ) VALUES (?, ?, ?, ?, ?)",
            self.snapshots_table
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_events_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.select_events();

        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM events"));
        assert!(
            query.contains("WHERE aggregate_type = ? AND aggregate_id = ?")
        );
        assert!(query.contains("ORDER BY sequence"));
    }

    #[test]
    fn test_insert_event_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.insert_event();

        assert!(query.contains("INSERT INTO events"));
        assert!(query.contains("VALUES (?, ?, ?, ?, ?, ?, ?)"));
    }

    #[test]
    fn test_all_events_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.all_events();

        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM events"));
        assert!(query.contains("WHERE aggregate_type = ?"));
        assert!(query.contains("ORDER BY sequence"));
    }

    #[test]
    fn test_get_last_events_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.get_last_events();

        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM events"));
        assert!(query.contains(
            "WHERE aggregate_type = ? AND aggregate_id = ? AND sequence > ?"
        ));
        assert!(query.contains("ORDER BY sequence"));
    }

    #[test]
    fn test_select_snapshot_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.select_snapshot();

        assert!(query.contains("SELECT"));
        assert!(query.contains("FROM snapshots"));
        assert!(
            query.contains("WHERE aggregate_type = ? AND aggregate_id = ?")
        );
    }

    #[test]
    fn test_update_snapshot_query() {
        let factory =
            SqlQueryFactory::new("events".to_string(), "snapshots".to_string());
        let query = factory.update_snapshot();

        assert!(query.contains("INSERT OR REPLACE INTO snapshots"));
        assert!(query.contains("VALUES (?, ?, ?, ?, ?)"));
    }

    #[test]
    fn test_custom_table_names() {
        let factory = SqlQueryFactory::new(
            "custom_events".to_string(),
            "custom_snapshots".to_string(),
        );

        assert!(factory.select_events().contains("FROM custom_events"));
        assert!(factory.select_snapshot().contains("FROM custom_snapshots"));
    }
}
