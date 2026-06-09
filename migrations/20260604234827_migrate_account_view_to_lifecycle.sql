-- account_view now stores the event-sorcery `Lifecycle<Account>` payload
-- (`{"Live": {...}}`) instead of the legacy cqrs-es `AccountView` payload. Drop
-- the old table (its stale `$.Account.*` generated columns and indexes) and
-- recreate it clean. `StoreBuilder::build` rebuilds every row from the event log
-- on startup via the projection's catch-up, so no data is lost.
DROP TABLE IF EXISTS account_view;

CREATE TABLE account_view (
    view_id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    payload JSON NOT NULL
);

-- Preserve the legacy duplicate-email index (migration 20251201225642),
-- re-pathed onto the Lifecycle payload. This guards the view against duplicate
-- rows only; it is not a 409 mechanism (the projection runs after the event
-- commits and swallows the resulting UNIQUE violation as a warning), so the
-- application-level find_by_email pre-check remains the registration guard.
CREATE UNIQUE INDEX idx_account_view_email_unique
    ON account_view(
        COALESCE(
            json_extract(payload, '$.Live.Registered.email'),
            json_extract(payload, '$.Live.LinkedToAlpaca.email')
        )
    )
    WHERE json_extract(payload, '$.Live.Registered.email') IS NOT NULL
       OR json_extract(payload, '$.Live.LinkedToAlpaca.email') IS NOT NULL;
