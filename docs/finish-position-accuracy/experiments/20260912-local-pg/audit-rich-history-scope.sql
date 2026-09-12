CREATE TEMP VIEW required AS
SELECT h.*
FROM read_parquet('/private/tmp/horse-nar-banei-0912/causal-features.parquet') h
JOIN read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/ablation-v1/83/2026/no-speed/scope-races.parquet') s USING(race_id)
WHERE h.race_date < '20260101';
CREATE TEMP VIEW rich AS
SELECT concat_ws('-',source,race_date,keibajo_code,race_bango) AS race_id,
       ketto_toroku_bango AS horse_id, finish_position AS finish
FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/rich-history-v1/83/base/race_year=*/*.parquet')
WHERE race_date < '20260101';
CREATE TEMP VIEW missing AS
SELECT q.race_id,q.horse_id,q.race_date,q.category,q.venue,q.finish
FROM required q ANTI JOIN rich r USING(race_id,horse_id);
SELECT json_object(
  'required_rows', (SELECT count(*) FROM required),
  'required_labeled', (SELECT count(*) FROM required WHERE finish>0),
  'rich_rows', (SELECT count(*) FROM rich),
  'missing_rows', (SELECT count(*) FROM missing),
  'missing_labeled', (SELECT count(*) FROM missing WHERE finish>0),
  'label_differences_on_present_rows', (SELECT count(*) FROM required q JOIN rich r USING(race_id,horse_id) WHERE q.finish IS DISTINCT FROM r.finish),
  'missing_by_year', (SELECT list(struct_pack(year:=year,rows:=rows,labeled:=labeled)) FROM (SELECT left(race_date,4) AS year,count(*) AS rows,count(*) FILTER(WHERE finish>0) AS labeled FROM missing GROUP BY year ORDER BY year)),
  'required_domains', (SELECT list(struct_pack(category:=category,venue:=venue,rows:=rows)) FROM (SELECT category,venue,count(*) AS rows FROM required GROUP BY category,venue ORDER BY category,venue))
);
