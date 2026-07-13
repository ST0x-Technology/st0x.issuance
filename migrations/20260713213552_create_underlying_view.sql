-- Underlying view: per-underlying corporate-action freeze status, projected
-- from the `Underlying` aggregate (`{"Live": {"status": ...}}` lifecycle
-- payloads). An absent row means the underlying was never frozen (Enabled by
-- definition).
CREATE TABLE IF NOT EXISTS underlying_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);
