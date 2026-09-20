-- Pin acquisition semantics to each durable run, including all legacy RACE runs.
ALTER TABLE sync_runs ADD COLUMN data_spec TEXT NOT NULL DEFAULT 'RACE'
  CHECK (
    data_spec IN ('RACE', 'COMM', 'RACECOMM')
    AND (provider = 'jv' OR data_spec = 'RACE')
    AND (data_spec <> 'COMM' OR advance_cursor = 0)
  );
