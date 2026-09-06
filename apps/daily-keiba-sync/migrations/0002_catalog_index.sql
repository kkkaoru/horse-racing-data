CREATE TABLE catalog_targets (
  table_name TEXT PRIMARY KEY,
  provider TEXT NOT NULL CHECK (provider IN ('jv', 'nv')),
  partition_field TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
  created_at TEXT NOT NULL
);

INSERT INTO catalog_targets (table_name, provider, partition_field, created_at) VALUES
  ('jvd_ra', 'jv', 'kaisai_nen', '2026-09-03T00:00:00.000Z'),
  ('jvd_se', 'jv', 'kaisai_nen', '2026-09-03T00:00:00.000Z'),
  ('nvd_ra', 'nv', 'kaisai_nen', '2026-09-03T00:00:00.000Z'),
  ('nvd_se', 'nv', 'kaisai_nen', '2026-09-03T00:00:00.000Z');

CREATE TABLE catalog_index_partitions (
  table_name TEXT NOT NULL REFERENCES catalog_targets(table_name),
  partition_value TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'planning', 'building', 'ready', 'failed')),
  catalog_snapshot_id TEXT,
  total_chunks INTEGER NOT NULL DEFAULT 0,
  completed_chunks INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (table_name, partition_value)
);

CREATE TABLE catalog_index_chunks (
  chunk_id TEXT PRIMARY KEY,
  table_name TEXT NOT NULL,
  partition_value TEXT NOT NULL,
  file_path TEXT NOT NULL,
  file_size INTEGER NOT NULL,
  row_start INTEGER NOT NULL,
  row_end INTEGER NOT NULL,
  snapshot_id TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending', 'succeeded', 'failed')),
  updated_at TEXT NOT NULL,
  UNIQUE (table_name, partition_value, file_path, row_start, row_end)
);

CREATE INDEX catalog_index_chunks_pending_idx
  ON catalog_index_chunks(table_name, partition_value, status);

CREATE TABLE catalog_row_index (
  table_name TEXT NOT NULL,
  row_key TEXT NOT NULL,
  partition_value TEXT NOT NULL,
  file_path TEXT NOT NULL,
  row_position INTEGER NOT NULL,
  catalog_snapshot_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (table_name, row_key)
);

CREATE INDEX catalog_row_index_partition_idx
  ON catalog_row_index(table_name, partition_value);

CREATE TABLE catalog_operations (
  run_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  base_snapshot_id TEXT NOT NULL,
  committed_snapshot_id TEXT,
  deleted_rows INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('planned', 'committed', 'indexed')),
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, table_name),
  FOREIGN KEY (run_id, table_name) REFERENCES sync_run_tables(run_id, table_name) ON DELETE CASCADE
);

CREATE TABLE catalog_table_leases (
  table_name TEXT PRIMARY KEY,
  owner_id TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE sync_run_table_partitions (
  run_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  partition_value TEXT NOT NULL,
  PRIMARY KEY (run_id, table_name, partition_value),
  FOREIGN KEY (run_id, table_name) REFERENCES sync_run_tables(run_id, table_name) ON DELETE CASCADE
);
