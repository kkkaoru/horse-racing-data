ALTER TABLE sync_runs ADD COLUMN from_time TEXT;
ALTER TABLE sync_runs ADD COLUMN to_time TEXT;
ALTER TABLE sync_runs ADD COLUMN cursor_time TEXT;
ALTER TABLE sync_runs ADD COLUMN advance_cursor INTEGER NOT NULL DEFAULT 0;

CREATE TABLE provider_acquisition_cursors (
  provider TEXT PRIMARY KEY CHECK (provider IN ('jv', 'nv')),
  last_acquired_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
