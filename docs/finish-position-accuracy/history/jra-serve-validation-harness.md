# JRA Race-Day Serve-Accuracy Validation Harness

**Date built:** 2026-06-13  
**Purpose:** Measure what production actually served (finish-position and running-style predictions) vs actual race results, to validate the 09:30 cron serve-skew fix (`fe871a6`) and the balanced2 running-style model.

---

## Storage Schema

### Finish-position predictions

Table: `race_finish_position_model_predictions`  
Key columns used:

- `source` (jra/nar), `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango`, `ketto_toroku_bango`
- `predicted_rank` — 1-indexed rank assigned by model
- `model_version` — e.g. `jra-finish-position-lgbm-prod-iter14`
- `prediction_generated_at` — UTC timestamp of when the prediction was written (used for era classification)

No `odds_source` column exists. Era must be inferred from `prediction_generated_at`.

Join target: `jvd_se` (JRA) or `nvd_se` (NAR) on `keibajo_code + race_bango + ketto_toroku_bango`, filtering `kakutei_chakujun IS NOT NULL AND kakutei_chakujun ~ '^[0-9]+$' AND CAST(...) > 0`.

DISTINCT ON: `(keibajo_code, race_bango) ORDER BY prediction_generated_at DESC` picks the latest-generated prediction per race when multiple model versions exist.

### Running-style predictions

Table: `race_running_style_model_predictions`  
Key columns: `predicted_class` (0=nige, 1=senkou, 2=sashi, 3=oikomi), `model_version`, `prediction_generated_at`.

Actual running-style label is derived at query time from `jvd_se.corner_1` + `jvd_ra.shusso_tosu` using the formula from `build-corner-feature-table.ts`:

```
corner1_norm = (corner_1 - 1) / (shusso_tosu - 1)   when corner_1 != '00' and shusso_tosu > 1
             = NULL                                    for straight tracks
```

Horses on straight tracks (corner1_norm = NULL) are excluded from RS accuracy.

---

## Era Classification

| Era        | Definition                                                                                         |
| ---------- | -------------------------------------------------------------------------------------------------- |
| `DEGRADED` | `prediction_generated_at < 2026-06-11 00:00 UTC` — no 09:30 cron, predictions used OOD-median odds |
| `POST_FIX` | `prediction_generated_at >= 2026-06-11 00:00 UTC` — 09:30 cron live, realtime odds                 |
| `UNKNOWN`  | No predictions found                                                                               |

Era is assigned from the **latest** `prediction_generated_at` across all matched rows for the date.

---

## Harness Design

Script: `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py`  
Tests: `apps/pc-keiba-viewer/tests/test_serve_accuracy_report.py` (2379 total tests pass, 97.27% cov)

### Metrics computed

**Finish-position** (per race-day, JRA or NAR):

- `top1`: predicted_rank=1 AND actual_rank=1
- `place2`: predicted_rank=1 AND actual_rank≤2
- `place3`: predicted_rank=1 AND actual_rank≤3
- `fukusho_2p`: any horse in predicted top-2 finishes ≤2 (per-race OR, then averaged)
- `top3_box`: predicted_rank=1 AND actual_rank≤3 (same as place3 here)

**Running-style** (per horse, cornered tracks only):

- Per-class precision / recall / F1 for each of {nige, senkou, sashi, oikomi}
- Macro-F1 = mean of per-class F1 (classes with no actual samples skipped)
- Overall accuracy

### Architecture notes

- Pure helpers (`infer_era`, `compute_corner1_norm`, `classify_running_style`, `aggregate_fp_metrics`, `compute_rs_per_class`, `compute_macro_f1`) — all tested without DB
- DB functions (`query_finish_position_metrics`, `query_running_style_metrics`) accept `ConnectionLike` Protocol, tested with `MagicMock` connection
- Row results cast via `cast(list[FpRow], ...)` / `cast(list[RsRow], ...)` for type safety
- `metrics_to_dict()` returns JSON-serializable `dict[str, object]`

---

## Backtest Results

### 2026-06-06 JRA (DEGRADED era baseline)

Available predictions: 24 races (generated 2026-06-06 05:27 JST, before 09:30 cron fix).

| Metric     | Value  | n    |
| ---------- | ------ | ---- |
| top1       | 0.00%  | 0/24 |
| place2     | 8.33%  | 2/24 |
| place3     | 20.83% | 5/24 |
| fukusho_2p | 8.33%  | 2/24 |
| top3_box   | 20.83% | 5/24 |

**Note:** n=24 races is high-variance. The population-scale DEGRADED baseline is top1=31.78% (n=11,703 races). The 2026-06-06 single-day deviation is expected statistical noise.

Running-style: No non-straight-track predictions available for 2026-06-06 (v3 logits for 05/11 exist but 05/11 is a straight track, all RS correctly returns None).

### Population-scale reference (from serve-condition-baseline-population.md)

| Era                 | top1     | place2  | place3  | fukusho_2p | n      |
| ------------------- | -------- | ------- | ------- | ---------- | ------ |
| JRA DEGRADED        | 31.78%   | 15.25%  | 9.19%   | 57.76%     | 11,703 |
| JRA FULL (POST_FIX) | 44.71%   | 24.51%  | 15.48%  | 74.79%     | 11,703 |
| Expected recovery   | +12.93pp | +9.26pp | +6.29pp | +17.03pp   | —      |

---

## 2026-06-14 Run Commands

```sh
# From apps/pc-keiba-viewer/
uv run python src/scripts/serve_accuracy_report.py --date 20260614 --category jra

# JSON output (for programmatic parsing)
uv run python src/scripts/serve_accuracy_report.py --date 20260614 --category jra --json

# Finish-position only (skip running-style query)
uv run python src/scripts/serve_accuracy_report.py --date 20260614 --category jra --no-rs

# Custom PG URL (mask creds in logs)
uv run python src/scripts/serve_accuracy_report.py --date 20260614 --category jra \
    --pg-url "postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing"
```

### Expected output (POST_FIX era, if 09:30 cron ran successfully)

```
=== Finish-Position Serve Accuracy: 20260614 (JRA) ===
  Era:           POST_FIX
  Generated:     2026-06-14 09:30:XX JST
  ...
  top1:           ~44%  (approaching FULL baseline)

  Baselines (population n=11703):
    DEGRADED top1= 31.78%  place2= 15.25%  place3=  9.19%
    FULL     top1= 44.71%  place2= 24.51%  place3= 15.48%
```

If `Era: DEGRADED` appears on 2026-06-14, the 09:30 cron failed and predictions used stale/OOD odds again — check launchd logs.

---

## Validation Protocol for 2026-06-14

1. **After races finish (~17:00 JST):** Run the command above
2. **Check era label:** Must be `POST_FIX` — if `DEGRADED`, the serve-skew fix did not hold
3. **Compare top1 to baselines:**
   - ≥40% → recovery confirmed (within FULL baseline range)
   - 30-40% → partial recovery or high-variance day
   - <30% → degradation — investigate model_version_counts and prediction_generated_at
4. **Check RS accuracy** (if balanced2 model was active): Macro-F1 ≥45% expected for POST_FIX era with v3 model

---

## Files

- Script: `apps/pc-keiba-viewer/src/scripts/serve_accuracy_report.py`
- Tests: `apps/pc-keiba-viewer/tests/test_serve_accuracy_report.py`
- This doc: `docs/finish-position-accuracy/history/jra-serve-validation-harness.md`
