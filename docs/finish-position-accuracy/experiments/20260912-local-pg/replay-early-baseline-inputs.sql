-- A conservative no-observed-context replay, NOT a claim of historical snapshots.
-- Values validated on local upcoming 20260912: 120 entrants, 12 races (audit-upcoming-early-inputs.json).
-- Keep the frozen model's legacy historical body feature: correcting that silently
-- would change its input contract. Corrected body candidates are separately trained.
COPY (
 SELECT * REPLACE (
  0.5000::DOUBLE AS popularity_score, 0.5048::DOUBLE AS odds_score,
  NULL::DOUBLE AS weight_diff_from_avg,
  NULL::DOUBLE AS weather_normalized, NULL::DOUBLE AS track_condition_normalized,
  NULL::DOUBLE AS current_baba_condition,
  NULL::DOUBLE AS horse_baba_career_starts, NULL::DOUBLE AS horse_baba_win_rate,
  NULL::DOUBLE AS sire_baba_career_starts, NULL::DOUBLE AS sire_baba_win_rate,
  NULL::DOUBLE AS damsire_baba_career_starts, NULL::DOUBLE AS damsire_baba_win_rate,
  NULL::DOUBLE AS sire_horse_baba_combined_score
 ) FROM read_parquet('docs/finish-position-accuracy/experiments/20260912-local-pg/production-baseline-features/83/final/race_year=*/*.parquet')
) TO 'docs/finish-position-accuracy/experiments/20260912-local-pg/early-baseline-replay.parquet' (FORMAT PARQUET);
