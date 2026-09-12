-- Inference-only local-PG reconstruction: no future labels or model promotion.
CREATE TEMP VIEW upcoming AS SELECT * FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/production-upcoming-features/83/final/race_year=*/*.parquet');
CREATE TEMP VIEW long_values AS
SELECT unnest(['popularity_score','odds_score','weight_diff_from_avg','weight_avg_5','weather_normalized','track_condition_normalized','current_baba_condition','horse_baba_career_starts','horse_baba_win_rate','sire_baba_career_starts','sire_baba_win_rate','damsire_baba_career_starts','damsire_baba_win_rate','sire_horse_baba_combined_score','shusso_tosu']) AS feature,
       unnest([popularity_score,odds_score,weight_diff_from_avg,weight_avg_5,weather_normalized,track_condition_normalized,current_baba_condition,horse_baba_career_starts,horse_baba_win_rate,sire_baba_career_starts,sire_baba_win_rate,damsire_baba_career_starts,damsire_baba_win_rate,sire_horse_baba_combined_score,shusso_tosu]) AS value
FROM upcoming;
SELECT json_object(
  'entrants', (SELECT count(*) FROM upcoming),
  'races', (SELECT count(DISTINCT concat_ws('-',race_date,keibajo_code,race_bango)) FROM upcoming),
  'observed_labels', (SELECT count(*) FROM upcoming WHERE finish_position>0),
  'body_zero_encoding_consistent_rows', (SELECT count(*) FROM upcoming WHERE abs(weight_diff_from_avg+weight_avg_5)<0.000001),
  'features', (SELECT to_json(list(t ORDER BY feature)) FROM (
    SELECT feature,count(*) AS rows,count(value) AS known,count(DISTINCT value) AS distinct_values,min(value) AS minimum,max(value) AS maximum
    FROM long_values GROUP BY feature
  ) t)
);
