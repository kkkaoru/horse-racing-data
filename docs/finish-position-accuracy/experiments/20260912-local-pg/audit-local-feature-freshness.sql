BEGIN READ ONLY;
SET LOCAL statement_timeout = '45s';
SELECT category, keibajo_code, count(*) AS rows,
       min(race_date) AS first_date, max(race_date) AS last_date,
       count(*) FILTER (WHERE race_date > '20260518') AS post_banei_training_cutoff,
       count(*) FILTER (WHERE race_date >= '20260901') AS september_rows,
       count(*) FILTER (WHERE race_date >= '20260901' AND finish_position > 0) AS september_labeled,
       min(feature_schema_version) AS min_schema, max(feature_schema_version) AS max_schema
FROM race_finish_position_features
WHERE keibajo_code IN ('54','55','83')
GROUP BY category, keibajo_code
ORDER BY category, keibajo_code;
COMMIT;
