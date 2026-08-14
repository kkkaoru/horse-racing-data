-- Re-run to track 2026-08-15 finish-position generation.
-- Upcoming races are selected from the fixed D1-derived target list, so this
-- query does not need a result-finality predicate. If one is added, do not use
-- IS NOT NULL: require btrim(result_code) <> '' AND btrim(result_code) !~ '^0+$'.
WITH targets(category, source, keibajo_code, race_bango, expected_runners) AS (
  VALUES
    ('jra', 'jra', '04', '01', 9),
    ('jra', 'jra', '07', '01', 16),
    ('jra', 'jra', '01', '01', 14),
    ('jra', 'jra', '04', '02', 15),
    ('jra', 'jra', '07', '02', 15),
    ('jra', 'jra', '01', '02', 10),
    ('jra', 'jra', '04', '03', 12),
    ('jra', 'jra', '07', '03', 7),
    ('jra', 'jra', '01', '03', 12),
    ('jra', 'jra', '04', '04', 15),
    ('jra', 'jra', '07', '04', 14),
    ('jra', 'jra', '01', '04', 12),
    ('jra', 'jra', '04', '05', 18),
    ('jra', 'jra', '07', '05', 16),
    ('jra', 'jra', '01', '05', 12),
    ('jra', 'jra', '01', '06', 14),
    ('jra', 'jra', '01', '07', 15),
    ('jra', 'jra', '01', '08', 11),
    ('jra', 'jra', '01', '09', 8),
    ('ban-ei', 'nar', '83', '01', 10),
    ('jra', 'jra', '04', '06', 14),
    ('jra', 'jra', '01', '10', 16),
    ('jra', 'jra', '07', '06', 11),
    ('ban-ei', 'nar', '83', '02', 10),
    ('jra', 'jra', '04', '07', 15),
    ('jra', 'jra', '01', '11', 14),
    ('nar', 'nar', '44', '01', 12),
    ('jra', 'jra', '07', '07', 8),
    ('ban-ei', 'nar', '83', '03', 10),
    ('nar', 'nar', '55', '01', 6),
    ('jra', 'jra', '04', '08', 15),
    ('nar', 'nar', '44', '02', 14),
    ('jra', 'jra', '01', '12', 14),
    ('ban-ei', 'nar', '83', '04', 10),
    ('jra', 'jra', '07', '08', 10),
    ('nar', 'nar', '55', '02', 7),
    ('jra', 'jra', '04', '09', 11),
    ('nar', 'nar', '44', '03', 9),
    ('ban-ei', 'nar', '83', '05', 10),
    ('jra', 'jra', '07', '09', 16),
    ('nar', 'nar', '55', '03', 11),
    ('jra', 'jra', '04', '10', 15),
    ('nar', 'nar', '44', '04', 9),
    ('ban-ei', 'nar', '83', '06', 10),
    ('jra', 'jra', '07', '10', 16),
    ('nar', 'nar', '55', '04', 12),
    ('jra', 'jra', '04', '11', 18),
    ('nar', 'nar', '44', '05', 5),
    ('jra', 'jra', '07', '11', 15),
    ('ban-ei', 'nar', '83', '07', 10),
    ('nar', 'nar', '55', '05', 11),
    ('jra', 'jra', '04', '12', 18),
    ('nar', 'nar', '44', '06', 13),
    ('jra', 'jra', '07', '12', 16),
    ('ban-ei', 'nar', '83', '08', 10),
    ('nar', 'nar', '55', '06', 9),
    ('nar', 'nar', '44', '07', 16),
    ('ban-ei', 'nar', '83', '09', 10),
    ('nar', 'nar', '55', '07', 10),
    ('nar', 'nar', '44', '08', 11),
    ('ban-ei', 'nar', '83', '10', 10),
    ('nar', 'nar', '55', '08', 11),
    ('ban-ei', 'nar', '83', '11', 9),
    ('nar', 'nar', '44', '09', 14),
    ('nar', 'nar', '55', '09', 10),
    ('ban-ei', 'nar', '83', '12', 10),
    ('nar', 'nar', '44', '10', 14),
    ('nar', 'nar', '55', '10', 12)
), prediction_quality AS (
  SELECT
    p.source,
    p.keibajo_code,
    p.race_bango,
    count(DISTINCT p.ketto_toroku_bango)::int AS prediction_rows,
    count(DISTINCT p.predicted_rank)::int AS distinct_ranks,
    min(p.predicted_rank)::int AS min_rank,
    max(p.predicted_rank)::int AS max_rank,
    count(DISTINCT p.predicted_score)::int AS distinct_scores,
    bool_and(p.predicted_score = 0) AS all_scores_zero,
    bool_or(p.predicted_score = 'NaN'::numeric) AS has_nan_score,
    min(p.prediction_generated_at) AS first_generated_at,
    max(p.prediction_generated_at) AS last_generated_at
  FROM race_finish_position_model_predictions p
  WHERE p.kaisai_nen = '2026'
    AND p.kaisai_tsukihi = '0815'
  GROUP BY p.source, p.keibajo_code, p.race_bango
), checked AS (
  SELECT
    t.*,
    coalesce(p.prediction_rows, 0) AS prediction_rows,
    coalesce(p.distinct_ranks, 0) AS distinct_ranks,
    p.min_rank,
    p.max_rank,
    coalesce(p.distinct_scores, 0) AS distinct_scores,
    coalesce(p.all_scores_zero, false) AS all_scores_zero,
    coalesce(p.has_nan_score, false) AS has_nan_score,
    p.first_generated_at,
    p.last_generated_at,
    coalesce(p.prediction_rows, 0) = t.expected_runners AS runner_count_matches,
    coalesce(p.distinct_ranks, 0) = t.expected_runners
      AND p.min_rank = 1
      AND p.max_rank = t.expected_runners AS rank_sequence_valid,
    coalesce(p.distinct_scores, 0) > 1 AS scores_not_collapsed,
    p.first_generated_at >= timestamptz '2026-08-15 00:00:00+09'
      AND p.last_generated_at < timestamptz '2026-08-16 00:00:00+09' AS generated_today
  FROM targets t
  LEFT JOIN prediction_quality p
    ON p.source = t.source
   AND p.keibajo_code = t.keibajo_code
   AND p.race_bango = t.race_bango
), final AS (
  SELECT *,
    runner_count_matches
      AND rank_sequence_valid
      AND scores_not_collapsed
      AND NOT all_scores_zero
      AND NOT has_nan_score
      AND generated_today AS complete_and_healthy
  FROM checked
)
SELECT
  count(*) FILTER (WHERE complete_and_healthy) OVER () AS healthy_races_of_68,
  count(*) FILTER (WHERE prediction_rows > 0) OVER () AS races_with_any_prediction_of_68,
  count(*) FILTER (WHERE prediction_rows = 0) OVER () AS not_generated,
  count(*) FILTER (WHERE prediction_rows > 0 AND NOT runner_count_matches) OVER () AS runner_count_mismatches,
  count(*) FILTER (
    WHERE prediction_rows > 0
      AND (all_scores_zero OR has_nan_score OR NOT scores_not_collapsed)
  ) OVER () AS score_quality_failures,
  count(*) FILTER (WHERE prediction_rows > 0 AND NOT generated_today) OVER () AS stale_generation_failures,
  category,
  keibajo_code,
  race_bango,
  expected_runners,
  prediction_rows,
  distinct_scores,
  first_generated_at,
  last_generated_at,
  complete_and_healthy
FROM final
ORDER BY category, keibajo_code, race_bango;
