WITH runner_predictions AS (
  SELECT *, regexp_extract(filename, '/(no-speed|speed)/', 1) AS arm
  FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/ablation-v1/83/*/*/predictions.parquet', filename=true)
), race_hits AS (
  SELECT race_id, race_date, arm,
    max((predicted_rank=1 AND finish=1)::INTEGER) AS rank1,
    max((predicted_rank=2 AND finish=2)::INTEGER) AS rank2,
    max((predicted_rank=3 AND finish=3)::INTEGER) AS rank3,
    max((predicted_rank=4 AND finish=4)::INTEGER) AS rank4,
    max((predicted_rank=5 AND finish=5)::INTEGER) AS rank5
  FROM runner_predictions GROUP BY ALL
), paired AS (
  SELECT b.race_id,b.race_date, b.rank1 AS base_rank1,s.rank1 AS candidate_rank1,
    s.rank1-b.rank1 AS d1,s.rank2-b.rank2 AS d2,s.rank3-b.rank3 AS d3,
    s.rank4-b.rank4 AS d4,s.rank5-b.rank5 AS d5
  FROM race_hits b INNER JOIN race_hits s USING(race_id,race_date)
  WHERE b.arm='no-speed' AND s.arm='speed'
)
SELECT substr(p.race_date,1,4) AS year, m.class_label,m.distance_band,m.season,
  count(*) AS paired_races,avg(base_rank1) AS base_rank1,avg(candidate_rank1) AS candidate_rank1,
  avg(d1) AS delta_rank1,avg(d2) AS delta_rank2,avg(d3) AS delta_rank3,
  avg(d4) AS delta_rank4,avg(d5) AS delta_rank5
FROM paired p INNER JOIN read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/race-cells-local-pg.parquet') m USING(race_id)
WHERE m.season='autumn' AND m.class_label IN ('E','joken-000')
GROUP BY ALL ORDER BY year,m.class_label;
