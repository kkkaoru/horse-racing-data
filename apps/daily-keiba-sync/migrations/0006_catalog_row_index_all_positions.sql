-- catalog_row_index was keyed by row_key alone, so it could remember only one physical position
-- per key. Once a write appended a second copy of a row (a retried catalog commit), the orphan
-- copy had no index entry and every later delete could remove at most the one indexed position,
-- which made the duplicate permanent. Key by physical position so the writer deletes every copy
-- of a staged key before appending.
CREATE TABLE catalog_row_index_all_positions (
  table_name TEXT NOT NULL,
  row_key TEXT NOT NULL,
  partition_value TEXT NOT NULL,
  file_path TEXT NOT NULL,
  row_position INTEGER NOT NULL,
  catalog_snapshot_id TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (table_name, row_key, file_path, row_position)
);

INSERT INTO catalog_row_index_all_positions
  (table_name, row_key, partition_value, file_path, row_position, catalog_snapshot_id, updated_at)
SELECT table_name, row_key, partition_value, file_path, row_position, catalog_snapshot_id, updated_at
FROM catalog_row_index;

DROP TABLE catalog_row_index;

ALTER TABLE catalog_row_index_all_positions RENAME TO catalog_row_index;

CREATE INDEX catalog_row_index_partition_idx
  ON catalog_row_index(table_name, partition_value);
