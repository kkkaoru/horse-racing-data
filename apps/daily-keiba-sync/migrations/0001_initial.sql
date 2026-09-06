CREATE TABLE sync_runs (
  run_id TEXT PRIMARY KEY,
  dedupe_key TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL CHECK (provider IN ('jv', 'nv')),
  run_date TEXT NOT NULL,
  trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('daily', 'manual', 'monitor')),
  lookback_days INTEGER NOT NULL,
  status TEXT NOT NULL,
  staging_key TEXT,
  files INTEGER NOT NULL DEFAULT 0,
  records INTEGER NOT NULL DEFAULT 0,
  catalog_tables INTEGER NOT NULL DEFAULT 0,
  neon_tables INTEGER NOT NULL DEFAULT 0,
  error_stage TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
);

CREATE INDEX sync_runs_provider_date_idx
  ON sync_runs(provider, run_date, created_at DESC);

CREATE TABLE sync_run_tables (
  run_id TEXT NOT NULL REFERENCES sync_runs(run_id) ON DELETE CASCADE,
  table_name TEXT NOT NULL,
  staging_key TEXT NOT NULL,
  source_records INTEGER NOT NULL,
  catalog_status TEXT NOT NULL DEFAULT 'pending',
  catalog_snapshot_id TEXT,
  catalog_deleted_rows INTEGER NOT NULL DEFAULT 0,
  neon_status TEXT NOT NULL DEFAULT 'pending',
  error_stage TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, table_name)
);

CREATE INDEX sync_run_tables_catalog_idx
  ON sync_run_tables(run_id, catalog_status);
CREATE INDEX sync_run_tables_neon_idx
  ON sync_run_tables(run_id, neon_status);
