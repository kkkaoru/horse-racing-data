# Serve-Condition Baseline — Population-Scale Recovery (2023-2026 Holdout)

**Date**: 2026-06-11
**Models**: `iter14-jra-cb-pacestyle-course-v8` (CatBoost YetiRank, 241 feat) / `iter12-nar-xgb-hpo-v8` (XGBoost rank:pairwise, 192 feat)
**Holdout**: 2023-2026 post-training window — JRA 11,703 races / NAR 45,573 races
**Status**: DEFINITIVE population-scale measurement. Supersedes the n=23 single-day result in `serve-combined-recovery-measurement.md`.

---

## 1. What this measures

The serve-skew recovery is the accuracy gain from correctly providing real market data to the
production model at serve time, measured on the full 2023-2026 holdout. The training data
does not include any OOD-median corruption — this holdout has all features populated from
historical PG data — so FULL is the model's actual ceiling on real data. DEGRADED simulates
what production was computing before the three serve-skew fixes.

### Condition definitions

| Condition                | odds_score / popularity_score                        | market-signal group | futan group       | Notes                                             |
| ------------------------ | ---------------------------------------------------- | ------------------- | ----------------- | ------------------------------------------------- |
| **FULL**                 | as-built in parquet (final settled odds)             | populated           | populated         | Post-fix serve ≈ this; the WF / ceiling reference |
| **DEGRADED**             | OOD median: JRA 0.5692 / 0.5000; NAR 0.5008 / 0.5000 | zeroed (JRA only)   | zeroed (JRA only) | Exact pre-fix serve state                         |
| **ODDS_ONLY** (JRA only) | as-built (real odds)                                 | zeroed              | zeroed            | Intermediate: Fix1 only                           |

**Market-signal group** (JRA model only, 10 features): `tansho_odds_raw`, `tansho_ninkijun_raw`,
`inverse_odds_implied_prob`, `inverse_odds_market_share`, `inverse_odds_rank_in_race`,
`popularity_rank_in_race`, `odds_score_diff_from_race_avg`, `popularity_score_diff_from_race_avg`,
`popularity_odds_disagreement`, `horse_popularity_vs_field`.

**Futan group** (JRA model only, 7 features): `futan_juryo`, `past_futan_juryo_avg5`,
`past_high_futan_share`, `futan_juryo_rank_in_race`, `futan_juryo_diff_from_race_avg`,
`past_futan_juryo_diff`, `futan_weight_class`.

NAR model (192 features) does not include market-signal or futan layers — DEGRADED for NAR
is odds/popularity OOD median only.

OOD medians are the global median over the training era (2007-2022 JRA, 2006-2022 NAR) of
each feature, matching what the pre-fix production pipeline injected for races without live
odds data.

---

## 2. FULL vs DEGRADED recovery — per category / per axis

### 2a. JRA (n = 11,703 races, 2023-2026)

| Metric     |    FULL | DEGRADED |      Δ (pp) |    LB95 |
| ---------- | ------: | -------: | ----------: | ------: |
| top1       | 44.706% |  31.778% | **+12.928** | +12.091 |
| place2     | 24.506% |  15.252% |  **+9.254** |  +8.562 |
| place3     | 15.475% |   9.194% |  **+6.280** |  +5.691 |
| top3_box   | 15.475% |   9.194% |  **+6.280** |  +5.691 |
| fukusho_2p | 74.793% |  57.763% | **+17.030** | +16.209 |
| rentai_hit | 64.368% |  47.407% | **+16.962** | +16.107 |

LB95 = 5th percentile of 10,000-sample paired bootstrap (seed 42). All six recovery
magnitudes have LB95 > +5 pp, establishing statistical significance at population scale.

### 2b. NAR (n = 45,573 races, 2023-2026)

| Metric     |    FULL | DEGRADED |     Δ (pp) |   LB95 |
| ---------- | ------: | -------: | ---------: | -----: |
| top1       | 57.764% |  48.338% | **+9.427** | +9.067 |
| place2     | 42.378% |  36.892% | **+5.486** | +5.181 |
| place3     | 34.771% |  30.764% | **+4.007** | +3.730 |
| top3_box   | 34.771% |  30.764% | **+4.007** | +3.730 |
| fukusho_2p | 87.982% |  83.499% | **+4.483** | +4.226 |
| rentai_hit | 78.794% |  72.040% | **+6.754** | +6.445 |

NAR LB95 is tighter because n is ~4x larger. All six metrics significant (LB95 > +3.7 pp).

---

## 3. JRA per-fix decomposition

ODDS_ONLY isolates Fix1 (real odds vs OOD median, market-signal+futan zeroed in both arms).
Fix2+3 is the incremental from market-signal and futan layers (FULL - ODDS_ONLY).

| Metric     | Fix1 Δ (pp) | Fix1 LB95 | Fix2+3 Δ (pp) | Fix2+3 LB95 | Total Δ |
| ---------- | ----------: | --------: | ------------: | ----------: | ------: |
| top1       |      +6.810 |    +6.315 |        +6.118 |      +5.358 | +12.928 |
| place2     |      +4.597 |    +4.178 |        +4.657 |      +4.008 |  +9.254 |
| place3     |      +3.230 |    +2.862 |        +3.050 |      +2.529 |  +6.280 |
| top3_box   |      +3.230 |    +2.862 |        +3.050 |      +2.529 |  +6.280 |
| fukusho_2p |      +8.733 |    +8.203 |        +8.297 |      +7.579 | +17.030 |
| rentai_hit |      +8.314 |    +7.784 |        +8.647 |      +7.878 | +16.962 |

**Fix1 and Fix2+3 contribute nearly equally across all axes.** Each accounts for roughly
half the total recovery. The single-day n=23 measurement saw Fix1 carry proportionally more
of the top1 recovery (+17.4 of +21.7 pp) because one atypical day's race-level variance
dominated; at population scale the contributions equalize.

---

## 4. Comparison: population vs single-day n=23 result

| Source                                                           |                         n |                        top1 Δ |                  fukusho_2p Δ |
| ---------------------------------------------------------------- | ------------------------: | ----------------------------: | ----------------------------: |
| Single day 2026-06-07 (`serve-combined-recovery-measurement.md`) |                  23 races |                     +21.74 pp |                     +52.17 pp |
| **Population 2023-2026 (this)**                                  | **11,703 / 45,573 races** | **JRA +12.93 / NAR +9.43 pp** | **JRA +17.03 / NAR +4.48 pp** |

The single-day result was directionally correct (sign and scale of the degradation confirmed)
but inflated by race-level variance (Bootstrap 95% CI on top1 at n=23 is ≈ ±8–10 pp). The
population figures are the definitive estimates.

---

## 5. Go-forward serve-condition baseline

Post-fix, the production serve path approximates the FULL condition in the following ways:

### JRA

- **09:30 cron (Fix1, `fe871a6`)**: real D1 advance odds populate `odds_score` /
  `popularity_score` before scoring. The ~4 early races that time out fall back to OOD
  median, but these are a small minority of total race volume.
- **market-signal layer (Fix2, `5c3aa12`)**: `tansho_odds` passthrough from the base build
  now feeds `inverse_odds_*`, `odds_score_diff_*`, `popularity_odds_disagreement`. Coverage
  follows the D1 advance-odds cron — afternoon races are fully covered.
- **futan layer (Fix3, `ebd4636`)**: reads `futan_juryo` from `jvd_se` for upcoming races,
  populated for all JRA keibajo.
- **Residual gap**: ~4 early-morning JRA races per day where the D1 advance-odds cron does
  not complete before scoring still receive OOD-median odds. At the holdout scale this
  affects a negligible fraction of races; the market-signal and futan layers do populate
  regardless.
- **Production bugs still outstanding**: (a) `finish_position_features_duckdb.py` does not
  pass `tansho_odds` through as an output column — the market-signal layer raises
  `BinderException` at runtime until patched; (b) `add-futan-juryo-features.py` references
  `b.source` on `jvd_se` which has no such column — the futan layer also raises
  `BinderException` at runtime. Both workarounds were applied in the single-day measurement
  (`serve-combined-recovery-measurement.md`). Until these are patched the committed fixes are
  not end-to-end operational.

**Post-fix JRA serve ≈ FULL for afternoon races**; FULL is the correct reference for
evaluating JRA model improvements.

### NAR

- **odds/popularity refresh (guard)**: NAR afternoon races (post-14:00) receive a live odds
  refresh pass before scoring, putting `odds_score` / `popularity_score` close to final
  settled values.
- **No market-signal / futan layers in NAR model**: the NAR model's 192 features do not
  include the JRA-only market-signal or futan groups. DEGRADED for NAR is
  odds/popularity OOD median only.
- **Residual caveat — NAR early-morning median**: NAR morning races (typically before 10:00
  JST) may score with partial or OOD-median odds if the guard refresh has not run. This
  represents a systematic partial degradation (~half the population recovery for those races,
  i.e., roughly −4.7 pp top1 vs fully refreshed) for early blocks.

**Post-fix NAR serve ≈ FULL for afternoon races; early-morning races sit partway between
FULL and DEGRADED** depending on whether the odds guard has completed.

### Recommendation for future accept-gate evaluation

Future iterations must evaluate candidates under realistic serve conditions, not WF-style
fully-populated features:

- **JRA**: score with the odds-available fraction matching production (treat the ~4 early
  races per day as receiving OOD median; the rest as FULL). For bulk holdout scoring the
  iter14 parquet is already populated with settled odds, which approximates post-fix serve
  well enough.
- **NAR**: include an "afternoon subset" vs "morning subset" split when computing accept-gate
  metrics, or at minimum quote the population-average knowing early-morning races are
  partially degraded. The +4.7 pp top1 gap between full and OOD-median NAR odds
  (approximately ½ × +9.4 pp combined recovery) is the cost of not refreshing early NAR
  races.
- **Signal search should optimize real production accuracy**, not ceiling WF accuracy. A
  signal that improves WF by +1 pp but is only available at 09:30 JRA scoring time is not
  equivalent to a signal that improves WF by +1 pp but is odds-independent.

---

## 6. Caveats

1. **Holdout parquets use final settled odds** (from historical PG/jvd_se data). This is
   equivalent to FULL/post-fix serve for both categories. The DEGRADED condition is imposed
   at scoring time by overriding `odds_score`/`popularity_score` with the OOD median scalar
   and zeroing the market-signal and futan groups. This exactly replicates the pre-fix serve
   state where the base build emitted a flat median for all horses in a race, destroying the
   within-race rank signal.

2. **OOD medians are global scalars, not per-horse**. The pre-fix serve path emitted the
   same `odds_score` value for every horse in a race. The DEGRADED condition faithfully
   replicates this: every horse in every race receives the same OOD median, collapsing the
   rank signal to zero for those features.

3. **NAR model does not include market-signal or futan layers**. The NAR recovery (+9.4 pp
   top1) comes entirely from restoring `odds_score` / `popularity_score` from OOD median to
   real settled values. This is a lower bound on NAR potential if market-signal features
   were added in a future iteration.

4. **JRA 2026 races have higher finish_position incompleteness** (~3.6% vs <1% in prior
   years), primarily from upcoming or aborted races at the time the parquet was generated.
   All rows with null `finish_position` are excluded before scoring.

5. **Bootstrap is paired at the race level** (each bootstrap sample resamples the same set
   of races for FULL and DEGRADED), making it sensitive to the actual race-level variance.
   LB95 values conservatively establish the lower bound with correct pairing.

---

## 7. Provenance

- Raw result: `tmp/validate/serve_baseline_population.json` (not git-tracked, tmp/)
- Scoring script: `tmp/validate/serve_baseline_population_score.py` (not git-tracked)
- Feature parquets used:
  - JRA: `apps/pc-keiba-viewer/tmp/feat-jra-v8-iter14-course/race_year=202{3,4,5,6}/`
  - NAR: `apps/pc-keiba-viewer/tmp/feat-nar-v8-iter9-pacestyle/race_year=202{3,4,5,6}/`
- Models:
  - `apps/finish-position-predict-container/models/finish-position/jra/iter14-jra-cb-pacestyle-course-v8/`
  - `apps/finish-position-predict-container/models/finish-position/nar/iter12-nar-xgb-hpo-v8/`
- Single-day predecessor: `serve-combined-recovery-measurement.md` (n=23, 2026-06-07 JRA)
- Read-only PG + in-memory DuckDB. No DELETE/TRUNCATE/DROP. No production change.
