-- Newly registered horses reach JV-Link only through DIFN UM records; without
-- them jvd_um froze at the 2026-09 cutover and new runners lost their pedigree.
-- SQLite cannot alter a CHECK, and dropping sync_runs would cascade into
-- sync_run_tables, so the data_spec column is swapped in place instead.
ALTER TABLE sync_runs ADD COLUMN acquisition_spec TEXT NOT NULL DEFAULT 'RACE'
  CHECK (
    acquisition_spec IN ('RACE', 'COMM', 'RACECOMM', 'DIFN')
    AND (provider = 'jv' OR acquisition_spec = 'RACE')
    AND (acquisition_spec NOT IN ('COMM', 'DIFN') OR advance_cursor = 0)
  );
UPDATE sync_runs SET acquisition_spec = data_spec;
ALTER TABLE sync_runs DROP COLUMN data_spec;
ALTER TABLE sync_runs RENAME COLUMN acquisition_spec TO data_spec;

INSERT INTO catalog_targets (table_name, provider, partition_field, created_at)
VALUES ('jvd_um', 'jv', '__unpartitioned__', '2026-10-31T00:00:00.000Z');
