CREATE TABLE corporate_action_mutations (
    event_id TEXT PRIMARY KEY,
    action_id TEXT NOT NULL,
    mutation TEXT NOT NULL CHECK (mutation IN ('insert', 'update', 'delete')),
    underlying TEXT NOT NULL,
    ex_date TEXT NOT NULL,
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (action_id, event_id)
);

CREATE INDEX corporate_action_mutations_action_id
    ON corporate_action_mutations (action_id, event_id);

CREATE TABLE corporate_action_schedule (
    action_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    underlying TEXT NOT NULL,
    ex_date TEXT NOT NULL,
    deleted INTEGER NOT NULL CHECK (deleted IN (0, 1)),
    reconciled_event_id TEXT,
    revision INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    FOREIGN KEY (action_id, event_id)
        REFERENCES corporate_action_mutations (action_id, event_id),
    FOREIGN KEY (action_id, reconciled_event_id)
        REFERENCES corporate_action_mutations (action_id, event_id)
);

CREATE TABLE corporate_action_cursor (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    event_id TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE corporate_action_blocked_event (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    event_id TEXT,
    reason TEXT NOT NULL CHECK (reason IN ('cursor_regression', 'poison', 'replay_gap')),
    blocked_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    CHECK (reason = 'poison' OR event_id IS NOT NULL)
);
