# JRA Season-Conditional Jockey/Trainer Form Family — Probe (2026-07-11)

- **Date**: 2026-07-11
- **Category**: JRA finish-position feature engineering
- **Source**: `apps/pc-keiba-viewer/tmp/frontier-scout/lever_bank.md` item 2
  ("Season-conditional jockey/trainer form family (store-built, never fed to
  champion)"), flagged as the one venue/jockey-adjacent win-rate family the
  2026-07-11 frontier scout verified as genuinely untested.
- **Baseline for dedup**: champion `jra-cb-v9-sim-2013`, clean 250-feature
  armB CatBoost YetiRank
  (`tmp/candidate-leak-clean-retrain/artifacts/jra-cb-v9-sim-2013-CLEAN/metadata.json`).

## Candidates (7 store-built, never-fed columns)

All computed in `src/scripts/finish_position_features_duckdb.py`
(`jockey_cte()` / `trainer_cte()`, lines ~1413-1500), strictly causal
(`h.race_date < t.race_date`, `HISTORY_LOOKBACK_YEARS=10`). **"Season" here
means calendar quarter** (`(month(history_race_dt)+9)%12//3 =
(month(target_race_dt)+9)%12//3`, Oct-shifted quarter buckets), aggregated
across the full 10-year lookback — i.e. "does this jockey/trainer perform
differently in this quarter-of-year, across years", not a single-year window.
This is a genuinely new axis: **no existing armB jockey/trainer feature
conditions on calendar season/month at all.**

| Column                                    | Construction                                                          |
| ----------------------------------------- | --------------------------------------------------------------------- |
| `jockey_season_win_rate`                  | jockey win rate, filtered to prior races in the same calendar quarter |
| `jockey_season_keibajo_win_rate`          | + same venue                                                          |
| `jockey_season_keibajo_distance_win_rate` | + same venue + distance ±200m                                         |
| `jockey_season_keibajo_distance_count`    | n-support for the above (count)                                       |
| `jockey_keibajo_distance_win_rate`        | venue + distance ±200m, no season                                     |
| `trainer_class_surface_season_win_rate`   | trainer win rate, same grade + surface (turf/dirt) + season           |
| `trainer_class_surface_season_count`      | n-support for the above (count)                                       |

## Dedup verification (per task instructions — cited, not re-derived)

Champion's existing jockey/trainer features (42 columns, confirmed via
`metadata.json` `feature_names`, identical set in both `jra-cb-v9-sim-2013-CLEAN`
and the `-jockey-pedigree269` variant): `jockey_career_win_rate`,
`jockey_recent_win_rate`, `jockey_keibajo_win_rate`, `jockey_distance_win_rate`,
`jockey_track_win_rate`, `jockey_grade_win_rate`, pair/style/corner rates,
`trainer_career_win_rate`, `trainer_keibajo_win_rate`, `trainer_distance_win_rate`,
`trainer_grade_win_rate`, `trainer_grade_top3_rate`, `trainer_target_race_*`,
`sim_jockey_*`, `sim_trainer_*`. **None condition on calendar season/quarter.**

Checked against every jockey/trainer REJECT on record — all differ in
construction, confirming these 7 are genuinely untested:

- `docs/probes/jra-jockey-triple-dynamic-subgroup-2026-06-19.md`: `jc_win_rate`
  = jockey × venue × distance-band × surface conditional rate, **expanding
  window (all prior races, no season term)**. Global REJECT (net -0.09pp),
  only 中山 (venue=06) subgroup ADOPT (+1.13pp, LB95>0, single-venue routing,
  not deployed as a global feature). No season dimension — different axis.
- `apps/pc-keiba-viewer/docs/probes/jra-jockey-winrate-clean-2026-07-04.md`
  (USER condition C, venue×class×dist×meeting-day×jockey win-rate,
  summer-venue focus): tested `jockey_venue_dist_win_eb`,
  `jockey_venue_dist_top3_eb` (EB-shrunk joint venue×dist-band cells),
  `jockey_meetphase_win_eb` (meeting-day phase), `jockey_summer_venue_top3_eb`/
  `_edge` (summer-circuit specialist). **REJECT, global and summer-restricted,
  DO-NOT-RETEST.** Different construction: EB-shrunk vs raw, meeting-day
  vs season/quarter — no calendar-season column tested.
- `docs/finish-position-prediction-system.md` §11 (jockey-track chemistry,
  jockey-switch delta, same-day jockey form, trainer-switch — all JRA/NAR/
  Ban-ei REJECT 2026-07-02): chemistry = subgroup rate **minus** career rate
  (relative diff, not raw conditional rate); switch = delta on jockey/trainer
  change (info absent from the row); same-day = today's ride count/rate
  (day-of-race, not season). All structurally distinct from a raw
  season-conditional rate.

No REJECT doc, §11 record, or `feature_registry_jra.duckdb` trial touches a
raw calendar-quarter-conditional jockey/trainer win rate. **Confirmed
UNTESTED-CANDIDATE**, consistent with the scout's finding.

## Method: odds-controlled partial-Spearman probe

- **Store**: `tmp/candidate-eval-jra/augmented` (per-race-year parquet,
  clean leak-free store), years 2023/2024/2025 (blind eras, matches campaign
  convention), n≈47-48k rows/year.
- **Coverage**: all 7 columns 96.4-99.9% non-null per year (checked
  2013/2018/2023/2024/2025). `jockey_season_keibajo_distance_count`
  n-support: median 85, only 7.4% of rows n<5, 12.7% n<10.
  `trainer_class_surface_season_count`: median 165, only 3.7% n<5, 6.5% n<10.
  Support is healthy — not a sparse-cell problem (the 10-year lookback +
  quarter-not-year granularity keeps cell sizes large).
- **Target**: `finish_position` (lower = better).
- **Controls**: `tansho_ninkijun` (odds/popularity rank) + the closest
  already-in-champion jockey/trainer feature per candidate (`jockey_career_win_rate`
  / `jockey_keibajo_win_rate` / `trainer_grade_win_rate` — mapped per column,
  see script). 2-control partial Spearman: rank-transform all variables,
  linearly residualize feature and target on `[1, rank(ninkijun), rank(control_feat)]`,
  Pearson of residuals.
- **Bar** (per task instructions): `|partial ρ| >= 0.02`, sign-stable across
  all 3 years (2023/2024/2025).
- Script: `tmp/venue-jockey-probe/probe_partial_rho.py`. Diagnostic follow-up
  (odds-only control, construction-overlap check):
  `tmp/venue-jockey-probe/probe_diagnostics.py`.

## Result: 0/7 pass

| Column                                    | raw ρ (2023) | partial ρ (odds+jockey-feat) 2023/24/25 | max\|partial ρ\| | sign-stable 3/3 | PASS                     |
| ----------------------------------------- | ------------ | --------------------------------------- | ---------------- | --------------- | ------------------------ |
| `jockey_season_win_rate`                  | -0.240       | +0.008 / -0.005 / -0.002                | 0.008            | No              | **REJECT**               |
| `jockey_season_keibajo_win_rate`          | -0.218       | +0.006 / -0.006 / -0.006                | 0.006            | No              | **REJECT**               |
| `jockey_season_keibajo_distance_win_rate` | -0.196       | +0.002 / -0.002 / -0.006                | 0.006            | No              | **REJECT**               |
| `jockey_season_keibajo_distance_count`    | -0.069       | -0.010 / -0.003 / -0.003                | 0.010            | Yes (all neg)   | **REJECT** (below floor) |
| `jockey_keibajo_distance_win_rate`        | -0.225       | -0.007 / +0.001 / -0.002                | 0.007            | No              | **REJECT**               |
| `trainer_class_surface_season_win_rate`   | -0.133       | +0.001 / -0.004 / -0.005                | 0.005            | No              | **REJECT**               |
| `trainer_class_surface_season_count`      | +0.027       | +0.016 / +0.009 / +0.018                | 0.018            | Yes (all pos)   | **REJECT** (below floor) |

Every column's raw Spearman is large and correctly signed (-0.07 to -0.25:
higher win rate correlates with a better/lower finish position, as
expected). After controlling for odds + the closest existing champion
feature, all 7 collapse to `|partial ρ| <= 0.018` — nowhere close to the
0.02 floor. Five of seven also flip sign year-to-year (only the two
n-support "count" columns are sign-stable, and both still miss the
magnitude bar). Full per-year table: `tmp/venue-jockey-probe/probe_result.json`.

### Diagnostic: mechanism is market pricing, not feature redundancy

Re-ran the probe controlling **only** for `tansho_ninkijun` (dropping the
jockey/trainer-feature control entirely) to separate "the market already
prices this" from "the existing champion feature already captures this":

| Column                                    | odds-only partial ρ (2023/24/25) |
| ----------------------------------------- | -------------------------------- |
| `jockey_season_win_rate`                  | +0.012 / +0.000 / +0.008         |
| `jockey_season_keibajo_win_rate`          | +0.006 / -0.004 / +0.005         |
| `jockey_season_keibajo_distance_win_rate` | +0.004 / -0.003 / +0.002         |
| `jockey_season_keibajo_distance_count`    | -0.009 / -0.004 / +0.000         |
| `jockey_keibajo_distance_win_rate`        | -0.001 / -0.001 / +0.006         |
| `trainer_class_surface_season_win_rate`   | +0.012 / +0.004 / +0.010         |
| `trainer_class_surface_season_count`      | +0.014 / +0.009 / +0.018         |

Essentially identical to the full 2-control result — adding the
jockey/trainer-feature control barely moves the numbers. **The market alone
already explains almost all of the raw correlation**; the candidates aren't
failing because CatBoost/existing features already reconstruct them, they're
failing because odds already price in whatever seasonal/venue/distance
jockey-trainer skill they encode. Same mechanism as every other REJECTed
jockey/trainer lever in this campaign (chemistry, switch, same-day form —
`docs/finish-position-prediction-system.md` §11).

Construction-overlap sanity check (raw Pearson r vs the chosen control
feature, 2023) confirms these are also substantially collinear with existing
features — `jockey_season_win_rate` r=+0.97 vs `jockey_career_win_rate`,
`jockey_season_keibajo_win_rate` r=+0.91 vs `jockey_keibajo_win_rate`,
`jockey_keibajo_distance_win_rate` r=+0.85 vs `jockey_keibajo_win_rate`,
`trainer_class_surface_season_win_rate` r=+0.62 vs `trainer_grade_win_rate` —
a second, compounding reason these add no increment, on top of market
pricing.

## Verdict: **REJECT at probe stage, no WF training run** (per task protocol step 4)

0 of 7 candidates clear the `|ρ|>=0.02` probe floor; no WF ablation
warranted. This closes the season-conditional jockey/trainer family
(`jockey_season_win_rate`, `jockey_season_keibajo_win_rate`,
`jockey_season_keibajo_distance_win_rate`,
`jockey_season_keibajo_distance_count`, `jockey_keibajo_distance_win_rate`,
`trainer_class_surface_season_win_rate`,
`trainer_class_surface_season_count`) — **DO-NOT-RETEST** this exact
7-column set. Extends the campaign's now-uniform finding across every
jockey/trainer-adjacent signal tried (raw conditional rate, EB-shrunk
interaction, chemistry diff, switch delta, same-day form): the JRA market
prices jockey/trainer skill, in all its conditional forms, essentially
completely.

## Artifacts

- `tmp/venue-jockey-probe/probe_partial_rho.py` — main probe script
- `tmp/venue-jockey-probe/probe_diagnostics.py` — odds-only + construction-overlap follow-up
- `tmp/venue-jockey-probe/probe_result.json` — full per-year numeric results
