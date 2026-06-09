-- event-sorcery's snapshot store (used by the Account `Store`) reads and writes
-- a `snapshot_version` column on the shared `snapshots` table. The original
-- cqrs-es-era schema predates it, so add it with a default of 0 (matching
-- event-sorcery's canonical init schema). The remaining cqrs-es aggregates use
-- sqlite-es, whose snapshot writes omit this column entirely; DEFAULT 0 keeps
-- those INSERTs valid.
ALTER TABLE snapshots ADD COLUMN snapshot_version BIGINT NOT NULL DEFAULT 0;
