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

    pub(crate) fn select_view(view_table: &str) -> String {
        format!(
            "SELECT view_id, version, payload
             FROM {view_table}
             WHERE view_id = ?"
        )
    }

    pub(crate) fn insert_or_update_view(view_table: &str) -> String {
        format!(
            "INSERT OR REPLACE INTO {view_table} (
                view_id,
                version,
                payload
            ) VALUES (?, ?, ?)"
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

    #[test]
    fn test_select_view_query() {
        let query = SqlQueryFactory::select_view("test_view");

        assert!(query.contains("SELECT"));
        assert!(query.contains("view_id"));
        assert!(query.contains("version"));
        assert!(query.contains("payload"));
        assert!(query.contains("FROM test_view"));
        assert!(query.contains("WHERE view_id = ?"));
    }

    #[test]
    fn test_insert_or_update_view_query() {
        let query = SqlQueryFactory::insert_or_update_view("test_view");

        assert!(query.contains("INSERT OR REPLACE INTO test_view"));
        assert!(query.contains("view_id"));
        assert!(query.contains("version"));
        assert!(query.contains("payload"));
        assert!(query.contains("VALUES (?, ?, ?)"));
    }

    #[test]
    fn test_view_queries_with_different_table_names() {
        let mint_view_select = SqlQueryFactory::select_view("mint_view");
        let redemption_view_select =
            SqlQueryFactory::select_view("redemption_view");

        assert!(mint_view_select.contains("FROM mint_view"));
        assert!(redemption_view_select.contains("FROM redemption_view"));

        let mint_view_upsert =
            SqlQueryFactory::insert_or_update_view("mint_view");
        let redemption_view_upsert =
            SqlQueryFactory::insert_or_update_view("redemption_view");

        assert!(mint_view_upsert.contains("INTO mint_view"));
        assert!(redemption_view_upsert.contains("INTO redemption_view"));
    }
}
