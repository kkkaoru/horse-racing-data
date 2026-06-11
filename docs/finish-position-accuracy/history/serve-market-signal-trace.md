# Serve-Time Market-Signal Feature Trace

**Date:** 2026-06-11
**Investigator:** Claude (read-only code + parquet analysis)
**Trigger:** Follow-up to JRA timing fix (commit 8e46049) whose validation found 3 high-importance odds features zero-filled

---

## Verdict

**NO — production does NOT compute the market-signal features for upcoming races.**

The production serve pipeline (full 13-layer Docker chain) RUNS `add-market-signal-features.py` (Layer 2) but gets **all-NULL** for `inverse_odds_implied_prob`, `inverse_odds_market_share`, `odds_score_diff_from_race_avg`, and related features. This is a **second serve-skew gap**, independent of and compounding the JRA timing fix.

**Importance at stake: ~21.5%** (top-3 features: 8.87% + 7.67% + 4.96%)

---

## Evidence

### 1. Full Pipeline Chain Confirmed

`pipeline_args.LAYER_CHAIN["jra"]` includes `MARKET_SIGNAL_SCRIPT` at position 1 (of 13 layers). `pipeline_runner.build_pipeline()` executes all layers sequentially — `add-market-signal-features.py` IS called in production.

### 2. Market-Signal Layer's Data Source: `race_entry_corner_features`

`add-market-signal-features.py::stage_raw_odds()` executes:

```sql
CREATE OR REPLACE TEMP TABLE raw_odds AS
SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
       CAST(tansho_odds AS DOUBLE) AS tansho_odds_raw,
       CAST(tansho_ninkijun AS INT) AS tansho_ninkijun_raw
FROM pg.race_entry_corner_features
WHERE race_date BETWEEN '20100101' AND '20991231'
```

It does **not** read from the base-build parquet. It makes an independent PG query to `race_entry_corner_features`.

### 3. `race_entry_corner_features` Lags Behind Upcoming Races

`race_entry_corner_features` is built by `build-corner-feature-table.ts` — a historical rebuild tool last run 2026-05-19 (log: `tmp/build-corner-rebuild.log`). Coverage extends to approximately 2026-05-24 at the time of the 2026-06-07 validation run. **Today's upcoming race rows are absent from this table.**

The base build's `build_rec_select_sql()` explicitly acknowledges this: it uses the direct `jvd_se`/`nvd_se` UPCOMING path for target rows precisely _"for races not yet materialised into `race_entry_corner_features` (the derived table lags)"_. But this data never flows into the market-signal layer.

### 4. LEFT JOIN Misses All Upcoming Rows

`append_features_sql()` does:

```sql
JOIN base_v2 AS b
LEFT JOIN raw_odds r USING (source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
```

When today's race has no matching row in `raw_odds`, `tansho_odds_raw = NULL`, which causes:

- `inverse_odds_implied_prob = NULL` (requires `tansho_odds_raw > 0`)
- `inverse_odds_market_share = NULL` (same)
- `tansho_odds_raw = NULL`, `tansho_ninkijun_raw = NULL`
- `inverse_odds_rank_in_race = 1` for ALL horses (DuckDB RANK with NULLS LAST gives rank=1 to all tied nulls — not zero but wrong)
- `popularity_rank_in_race = 1` for ALL horses (same issue)
- `popularity_odds_disagreement = NULL`
- `popularity_score_diff_from_race_avg = NULL` (if `popularity_score` NULL)
- `odds_score_diff_from_race_avg = NULL` (if `odds_score` NULL)

### 5. Empirical Verification: Production Parquet

`tmp/today-finish-features-parquet/jra-final/` (full 13-layer pipeline output, race_date=2026-05-23, 549 rows):

| Feature                               | Non-null count | Notes                           |
| ------------------------------------- | -------------- | ------------------------------- |
| `inverse_odds_implied_prob`           | 0 / 549        | **ALL NULL**                    |
| `inverse_odds_market_share`           | 0 / 549        | **ALL NULL**                    |
| `odds_score_diff_from_race_avg`       | 0 / 549        | **ALL NULL**                    |
| `popularity_odds_disagreement`        | 0 / 549        | **ALL NULL**                    |
| `popularity_score_diff_from_race_avg` | 0 / 549        | **ALL NULL**                    |
| `tansho_odds_raw`                     | 0 / 549        | **ALL NULL**                    |
| `tansho_ninkijun_raw`                 | 0 / 549        | **ALL NULL**                    |
| `inverse_odds_rank_in_race`           | 549 / 549      | All = 1 (bogus — all tied-null) |
| `popularity_rank_in_race`             | 549 / 549      | All = 1 (bogus — all tied-null) |
| `odds_score`                          | 0 / 549        | NULL (pre-fix run)              |
| `popularity_score`                    | 0 / 549        | NULL (pre-fix run)              |

`tmp/today-finish-features-parquet-fixed-shusso/jra-final/` (186 cols, same date): same null pattern.

---

## Feature Classification: All-NULL in Production Pipeline

### Legitimately NULL (post-race features, unrun at prediction time)

These are correct and expected:

- `finish_position`, `finish_norm`
- `target_corner_1/3/4_norm`, `target_running_style_class`
- `speed_index_avg_5`, `speed_index_best_5`, `field_strength_avg_speed`, `field_strength_top3_speed` — derived from `time_sa` / `kohan_3f` which require race completion
- `last_race_margin_to_winner` — post-race timing signal

### Wrongly NULL (Gap 2: `race_entry_corner_features` lag — computable pre-race)

| Feature                               | SHAP importance | Root cause                                                 | Fix path                                                                                          |
| ------------------------------------- | --------------- | ---------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `inverse_odds_implied_prob`           | **8.87%**       | `tansho_odds_raw` NULL                                     | Pass realtime odds to market-signal layer                                                         |
| `inverse_odds_market_share`           | **7.67%**       | `tansho_odds_raw` NULL                                     | Same                                                                                              |
| `odds_score_diff_from_race_avg`       | **4.96%**       | Depends on `odds_score` from base; NULL pre-JRA-timing-fix | Fixed by JRA timing fix if `odds_score` is non-null; also needs `tansho_odds_raw` for full signal |
| `popularity_odds_disagreement`        | ~unknown        | Both sources NULL                                          | Same fix                                                                                          |
| `popularity_score_diff_from_race_avg` | ~unknown        | `popularity_score` NULL                                    | Covered by JRA timing fix                                                                         |
| `tansho_odds_raw`                     | ~unknown        | corner_features lag                                        | Same fix                                                                                          |
| `tansho_ninkijun_raw`                 | ~unknown        | corner_features lag                                        | Same fix                                                                                          |
| `inverse_odds_rank_in_race`           | ~unknown        | Computes rank=1 for all (wrong, not null)                  | Same fix — correct values require non-null tansho_odds_raw                                        |
| `popularity_rank_in_race`             | ~unknown        | Same                                                       | Same fix                                                                                          |

**Total importance of top-3 wrongly-null features: ~21.5%**

### Wrongly NULL (Other gaps — separate investigation needed)

| Feature                      | Observed in production parquet | Likely cause                                                                  |
| ---------------------------- | ------------------------------ | ----------------------------------------------------------------------------- |
| `shusso_tosu`                | ALL NULL (2026-05-23 run)      | Possible pipeline bug in shusso_tosu propagation for upcoming path            |
| `umaban_norm`                | ALL NULL                       | Depends on `shusso_tosu`                                                      |
| `umaban_x_nige_history`      | ALL NULL                       | Depends on `umaban_norm`                                                      |
| `track_condition_normalized` | ALL NULL                       | Depends on `babajotai_code_*` (should be available from `jvd_ra`)             |
| `weather_normalized`         | ALL NULL                       | Depends on `tenko_code` from `jvd_ra`/`nvd_ra` (should be available pre-race) |
| `weight_diff_from_avg`       | ALL NULL                       | Horse weight diff; requires `bataiju` realtime or SE field                    |

Note: `shusso_tosu` being all-null for the 2026-05-23 production run suggests a pre-fix pipeline bug or that the run was from an earlier pipeline version. Warrants separate investigation.

---

## Gap vs JRA Timing Fix

| Gap                                          | Description                                                                                                                                                                                 | Status                                                                                       |
| -------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Gap 1: JRA timing**                        | Realtime odds not fetched pre-prediction → `odds_score`, `popularity_score` = OOD median                                                                                                    | **FIXED** by commit 8e46049 (`realtime_odds_fetcher.py` + `--realtime-odds` base build flag) |
| **Gap 2: Market-signal corner-features lag** | `add-market-signal-features.py` reads `race_entry_corner_features` which doesn't contain upcoming rows → `inverse_odds_implied_prob`, `inverse_odds_market_share`, `tansho_odds_raw` = NULL | **OPEN**                                                                                     |

The two gaps are **independent and compound**:

- After Gap 1 fix: `odds_score` is correct → `odds_score_diff_from_race_avg` is correctly computed (uses only `b.odds_score` from base parquet)
- After Gap 1 fix: `inverse_odds_implied_prob` and `inverse_odds_market_share` remain NULL (require `tansho_odds_raw` from `race_entry_corner_features`)
- After both fixes: all odds-derived features computable → full ~21.5% importance recovered

---

## Fix Design

### Recommended: Option C — Use Input-Parquet `tansho_odds` in Market-Signal Layer

The base-build parquet **already contains `tansho_odds`** (from the UPCOMING path: `COALESCE(rt.tansho_odds_realtime, try_cast(nullif(trim(se.tansho_odds), '') AS DOUBLE) / 10)`). The market-signal layer should read `tansho_odds` from its `--input-dir` parquet for upcoming rows instead of (only) from `race_entry_corner_features`.

**Implementation sketch for `add-market-signal-features.py`:**

```python
# New: stage tansho_odds from input parquet for rows absent in corner_features
def stage_odds_from_parquet(con, input_glob):
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE odds_from_parquet AS
    SELECT source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
           CAST(tansho_odds AS DOUBLE) AS tansho_odds_raw,
           CAST(tansho_ninkijun AS INT) AS tansho_ninkijun_raw
    FROM read_parquet('{input_glob}', hive_partitioning=true)
    WHERE tansho_odds IS NOT NULL
    """)

# Modified join in append_features_sql: COALESCE(corner_features, parquet)
# raw_odds_coalesced.tansho_odds_raw = COALESCE(r.tansho_odds_raw, p.tansho_odds_raw)
```

**Files requiring change:**

- `apps/pc-keiba-viewer/src/scripts/finish-position-features/add-market-signal-features.py` — add `--use-parquet-odds` flag or auto-detect; stage parquet-odds table; COALESCE in join
- `apps/pc-keiba-viewer/tests/test_add_market_signal_features.py` — add tests for upcoming path with parquet odds (coverage gate >= 95%)
- `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py` — no change needed if auto-detect; or add `MARKET_SIGNAL_SCRIPT` to a new `SCRIPTS_WITH_PARQUET_ODDS` frozenset

### Alternative: Option B — Pass Realtime-Odds Parquet Path

Add `--realtime-odds <path>` to `add-market-signal-features.py`. `pipeline_args.build_layer_argv()` appends it for `MARKET_SIGNAL_SCRIPT` when `realtime_odds_path` is available. The layer COALESCEs realtime → corner_features for `tansho_odds_raw`.

**Files requiring change:** `add-market-signal-features.py`, `pipeline_args.py`, `pipeline_runner.py`, and their tests.

---

## Expected Recovery

| Scenario                       | Estimated top1 improvement                                                            |
| ------------------------------ | ------------------------------------------------------------------------------------- |
| JRA timing fix only (Gap 1)    | +8.65pp (I4 holdout estimate)                                                         |
| Market-signal fix only (Gap 2) | +3–6pp (rough: 21.5% total importance at stake; includes rank derangement correction) |
| Both fixes combined            | ≥ +8.65pp, likely +10–14pp (non-additive due to correlation)                          |

The 2026-06-07 concrete validation (+4.17pp) was measured with **both conditions having null market-signal features** (base-build-only parquet, 137 cols). The true fix benefit when both gaps are closed is expected to be materially higher than the observed +4.17pp and closer to the I4 estimate of +8.65pp for Gap 1 alone.

---

## Appendix: Empirical Parquet Column Counts

| Parquet                                                     | Conditions                                        | Cols | Market-signal present |
| ----------------------------------------------------------- | ------------------------------------------------- | ---- | --------------------- |
| `tmp/validate/june7-features-real/`                         | Base build only + real odds                       | 137  | No                    |
| `tmp/validate/june7-features-median/`                       | Base build only + median odds                     | 137  | No                    |
| `tmp/today-finish-features-parquet/jra-final/`              | Full 13-layer + no realtime (pre-fix)             | 182  | Present but all-null  |
| `tmp/today-finish-features-parquet-fixed-shusso/jra-final/` | Full 13-layer + no realtime (pre-fix)             | 186  | Present but all-null  |
| Production target (iter14)                                  | Full 13-layer + realtime odds + market-signal fix | 241  | Should be non-null    |
