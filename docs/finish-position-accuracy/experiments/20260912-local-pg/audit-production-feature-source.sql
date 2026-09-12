BEGIN READ ONLY;
SET LOCAL statement_timeout = '20s';
SELECT table_name, column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND (table_name LIKE '%finish_position%features%' OR table_name = 'race_entry_corner_features')
ORDER BY table_name, ordinal_position;
COMMIT;
