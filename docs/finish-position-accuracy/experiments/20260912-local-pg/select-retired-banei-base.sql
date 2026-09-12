-- Run from repository root after the local-PG NAR base build for 2005--2006.
-- These are historical training venues, NOT additional current evaluation cells.
COPY (
  SELECT *
  FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-retired-v1/83/base/race_year=*/*.parquet')
  WHERE keibajo_code IN ('81','82','84')
) TO 'docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-retired-v1/83/retired-base'
(FORMAT PARQUET, PARTITION_BY (race_year), OVERWRITE_OR_IGNORE TRUE);
