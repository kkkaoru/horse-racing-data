CREATE TEMP VIEW missing AS
SELECT h.* FROM read_parquet('/private/tmp/horse-nar-banei-0912/causal-features-raw2026.parquet') h
ANTI JOIN read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/race-cells-local-pg.parquet') m USING(race_id)
WHERE h.venue IN ('54','55','83');
SELECT json_object(
 'rows',(SELECT count(*) FROM missing),
 'labeled',(SELECT count(*) FROM missing WHERE finish>0),
 'races',(SELECT count(DISTINCT race_id) FROM missing),
 'by_year',(SELECT to_json(list(t ORDER BY year)) FROM (SELECT year,count(*) AS rows,count(*) FILTER(WHERE finish>0) AS labeled FROM missing GROUP BY year) t),
 'examples',(SELECT to_json(list(t ORDER BY race_id)) FROM (SELECT race_id,race_date,venue,count(*) AS rows,count(*) FILTER(WHERE finish>0) AS labeled FROM missing GROUP BY ALL ORDER BY race_id LIMIT 20) t)
);
