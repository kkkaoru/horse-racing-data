---
science_track_entry: true
hypothesis_id: H-SIRE-DISTANCE-SPLIT
date: 2026-06-10
based_on_iteration: 30 (iter30-nar-cb-residual-*-v8 + iter12-nar-xgb-hpo-v8)
scope: NAR (all keibajo except Banei), per-class residual ensemble
status: ABORT (orchestrator-adjudicated — probe script emitted PROCEED_RECOMMENDED using a 0.02 pass threshold, not the calibrated 0.06 abort bar; verifying agent died mid-run from API error but probe was complete on disk)
verdict: ABORT
production_change: none
artifacts:
  feature_builder: tmp/nar-perclass/sci_track/v6_sire/build_features.py
  sire_parquet: tmp/nar-perclass/sci_track/v6_sire/sire-parquet/race_year={YYYY}/data_0.parquet
  probe_script: tmp/nar-perclass/sci_track/v6_sire/probe.py
  probe_verdict: tmp/nar-perclass/sci_track/v6_sire/probe_verdict.json
  gap_analysis: tmp/nar-perclass/sci_track/gap_analysis.json (rank 7)
operational_note: Verifying agent died mid-run (API error) after probe completed on disk. Orchestrator adjudicated ABORT from probe_verdict.json without re-running the agent.
---

## Hypothesis

**H-SIRE-DISTANCE-SPLIT** (science corpus rank 7, gap_analysis.json):

Sprint and route aptitude are nearly independent genetic traits at the sire level.
**5_2_53 (JES G ★)** reports a Spearman r=0.35 between sire EPD at 1200m vs 1800m,
meaning ~87.8% of variance in sprint aptitude is independent of route aptitude
(1 − 0.35² = 0.877). **9_3_89 (JES D/G ★)** confirms h²=0.29 (turf)/0.18 (dirt)
for finishing performance and sire×track r=0.50, showing sire breeding values differ
by surface and distance.

The existing 174-feature baseline contains `sire_distance_win_rate` (win rate at the
current distance band, 400m-keyed) and `pedigree_score_for_race` (a composite pedigree
score). These features condition on today's distance but do **not** encode the
cross-band mismatch: a sprint-sire horse entered at route distance should
systematically underperform vs its band-matched baseline, and vice versa. The genetic
independence claim means that `sire_sprint_wr` and `sire_route_wr` carry ~87%
independent variance, making a split feature genuinely new in principle.

**Proposed mechanism:** A sprint-sire horse (sire_distance_split > 0) racing at ≥1700m
is entering a distance category where its sire's progeny historically underperform
relative to sprint distances. The mismatch feature `sire_today_mismatch =
sire_distance_split × today_distance_scaled` encodes this directly: positive values
flag sprint-sires at route (should fade in closing stages); negative values flag
route-sires at sprint (lacking early speed).

## Citations

| Citation                     | Relevance                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 5_2_53 (JES G ★)             | Spearman r=0.35 between sire EPD at 1200m vs 1800m; sprint and route aptitude ~87% independent; 1985–1991 JRA   |
| 9_3_89 (JES D/G ★)           | h²=0.29 turf / 0.18 dirt; sire×track r=0.50; sire breeding values differ by surface and distance                |
| 34_2305 (JES D)              | MSTN, LCORL, DMRT3, HTR1A performance-gene variants in Japanese native horses; MSTN C:C = sprint, T:T = stamina |
| vol53-no2 p.141 (馬の科学 D) | MSTN g.66493737C/T in JRA: C:C sprint, T:T stamina, validated in JRA winner cohort                              |

## Feature Definitions (Leak-Free)

Sire statistics computed from ALL historical NAR races before the target race date
(same pattern as the existing `sire_distance_win_rate` in the 174-feature pipeline).
Sprint bucket: `kyori <= 1400m`. Route bucket: `kyori >= 1700m`. The 1500–1600m grey
zone is excluded from both buckets. Min-support guard: `MIN_SUPPORT_STARTS = 30`
progeny starts per band; features set to NULL if below threshold.

| Feature                   | Definition                                                                                                                                                         | Coverage |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------- |
| `sire_sprint_wr`          | Sire win rate over all NAR starts with `kyori <= 1400m`. NULL if sprint_starts < 30.                                                                               | 92.9%    |
| `sire_route_wr`           | Sire win rate over all NAR starts with `kyori >= 1700m`. NULL if route_starts < 30.                                                                                | 89.7%    |
| `sire_sprint_place3_rate` | Sire top-3 rate at sprint (`kyori <= 1400m`). More stable estimator than win rate. NULL if sprint_starts < 30.                                                     | 92.9%    |
| `sire_route_place3_rate`  | Sire top-3 rate at route (`kyori >= 1700m`). NULL if route_starts < 30.                                                                                            | 89.7%    |
| `sire_distance_split`     | `sire_sprint_wr − sire_route_wr`. Positive = sprint-sire, negative = route-sire. NULL if either band lacks min-support.                                            | 89.7%    |
| `sire_today_mismatch`     | `sire_distance_split × today_distance_scaled`, where `today_distance_scaled = (kyori − 1400) / 600` (maps 800m → −1, 2000m → +1). Positive = sprint-sire at route. | 89.7%    |
| `sire_today_mismatch_abs` | `abs(sire_today_mismatch)`. Magnitude of aptitude–distance mismatch.                                                                                               | 89.7%    |
| `sire_aptitude_ratio`     | `sire_sprint_wr / (sire_route_wr + 0.01)`. Ratio > 1 = sprint-sire bias. NULL if either band lacks min-support.                                                    | 89.7%    |

**Concentrated mismatch slices:**

- Sprint-sire at route: `sire_distance_split > 0` AND `kyori >= 1700` — 38,742 rows (5,631 races)
- Route-sire at sprint: `sire_distance_split < 0` AND `kyori <= 1400` — 205,080 rows (25,725 races)
- Combined mismatch: either condition — 243,822 rows (31,356 races)

**Partial-out regressors** (existing pedigree/sire features): `sire_distance_win_rate`,
`sire_avg_finish_at_distance`, `pedigree_score_for_race`, `dam_sire_distance_win_rate`,
`sire_track_win_rate`.

## Probe Setup

- **Years**: 2018–2024 (NAR, no Banei)
- **Merged rows**: 933,669
- **Existing numeric features scanned**: 174
- **Seed**: 42 (enforced in probe script)
- **Abort bar**: best partial rho (across all slices) < 0.06 → ABORT
  (calibrated from: V2 aborted at 0.045, V3 aborted at 0.040–0.055, V5 aborted at 0.059)

## Probe Results

### Raw within-race Spearman (lower finish_position = better)

| Feature                   | Coverage | Raw rho (all) | Raw rho (sprint→route) | Raw rho (route→sprint) | Raw rho (mismatch) | n races (all) |
| ------------------------- | -------- | ------------- | ---------------------- | ---------------------- | ------------------ | ------------- |
| `sire_sprint_wr`          | 92.9%    | −0.077        | −0.066                 | −0.067                 | −0.067             | 89,515        |
| `sire_route_wr`           | 89.7%    | −0.047        | −0.082                 | −0.048                 | −0.054             | 89,022        |
| `sire_sprint_place3_rate` | 92.9%    | **−0.087**    | −0.072                 | −0.072                 | **−0.072**         | 89,515        |
| `sire_route_place3_rate`  | 89.7%    | −0.049        | −0.099                 | −0.041                 | −0.051             | 89,022        |
| `sire_distance_split`     | 89.7%    | −0.019        | +0.014                 | −0.011                 | −0.006             | 89,022        |
| `sire_today_mismatch`     | 89.7%    | +0.023        | +0.014                 | +0.021                 | +0.018             | 50,991        |
| `sire_today_mismatch_abs` | 89.7%    | −0.012        | +0.014                 | +0.021                 | +0.018             | 50,991        |
| `sire_aptitude_ratio`     | 89.7%    | −0.019        | +0.020                 | −0.033                 | −0.024             | 89,022        |

### Partial rho (residual after OLS on 5 existing pedigree features)

| Feature                   | Partial rho (overall) | n races (partial) | Partial rho (mismatch slice) | n races (mismatch) | Max Pearson vs existing | Closest existing feature  |
| ------------------------- | --------------------- | ----------------- | ---------------------------- | ------------------ | ----------------------- | ------------------------- |
| `sire_sprint_wr`          | −0.013                | 88,230            | −0.006                       | 29,165             | **0.817**               | `sire_track_win_rate`     |
| `sire_route_wr`           | −0.006                | 87,789            | −0.012                       | 29,165             | 0.500                   | `sire_track_win_rate`     |
| `sire_sprint_place3_rate` | **−0.024**            | 88,230            | −0.018                       | 29,165             | 0.751                   | `sire_track_win_rate`     |
| `sire_route_place3_rate`  | −0.008                | 87,789            | −0.005                       | 29,165             | 0.435                   | `sire_track_win_rate`     |
| `sire_distance_split`     | +0.005                | 87,789            | +0.018                       | 29,165             | 0.239                   | `sire_track_win_rate`     |
| `sire_today_mismatch`     | −0.017                | 87,789            | +0.016                       | 29,165             | 0.295                   | `kyori`                   |
| `sire_today_mismatch_abs` | **+0.025**            | 87,789            | +0.016                       | 29,165             | 0.195                   | `field_avg_past_kohan_3f` |
| `sire_aptitude_ratio`     | −0.011                | 87,789            | +0.011                       | 29,165             | 0.184                   | `sire_nige_rate`          |

**Best partial rho (overall): 0.025 (`sire_today_mismatch_abs`)**
**Best partial rho (mismatch slice): 0.018 (`sire_distance_split`)**
**Both are well below the calibrated 0.06 abort bar.**

### Probe script verdict vs calibrated abort bar

The probe script (`probe.py`) emitted `PROCEED_RECOMMENDED` because 2 features
(`sire_sprint_wr`, `sire_sprint_place3_rate`) exceeded the internal
`signal_threshold_probe_pass = 0.02` — but this threshold was set for a preliminary
screen, not the abort decision. The abort bar used by all prior science-track probes
is **0.06 partial rho** (the level at which V2 was aborted at 0.045 conditional and V5
was aborted at 0.059). No feature reaches 0.06; the maximum partial rho across all
features and all slices is **0.025**.

### Redundancy with existing features

`sire_sprint_wr` has a Pearson correlation of **0.817** with `sire_track_win_rate`
(the existing feature that captures sire win rate at the current track). This is the
dominant overlap: a sire that wins frequently in NAR sprints will also have a high
track win rate, since sprints are the majority of NAR races. After regressing out the
5 existing pedigree features, the partial rho of `sire_sprint_wr` collapses from −0.077
to −0.013. The 5 existing features collectively account for the great majority of the
sprint/route signal that the proposed features encode.

**Summary of correlation with existing pedigree features (for the strongest candidate `sire_sprint_wr`):**

| Existing feature              | Pearson r |
| ----------------------------- | --------- |
| `sire_track_win_rate`         | **0.817** |
| `pedigree_score_for_race`     | 0.528     |
| `sire_distance_win_rate`      | 0.549     |
| `sire_avg_finish_at_distance` | −0.508    |
| `dam_sire_distance_win_rate`  | 0.061     |

## Verdict

**ABORT** — orchestrator-adjudicated.

**Binding reason:** The best partial rho across all 8 proposed features and all tested
slices (overall + mismatch) is **0.025** (`sire_today_mismatch_abs`). The calibrated
abort bar is **0.06** — the level that V2 (pref_x_heavy, partial rho=0.045 conditional)
and V3 (age_peak_deviation, 0.040–0.055) were aborted at, and V5 (races_in_90d,
0.059) was aborted at. Every proposed feature in this hypothesis falls materially below
that bar.

**Why the existing features already capture this channel:**

The existing `sire_track_win_rate` alone captures 66.7% of `sire_sprint_wr`'s variance
(r=0.817). When the full set of 5 existing pedigree features is partialled out, the
residual incremental signal of `sire_sprint_wr` drops from raw rho=−0.077 to partial
rho=−0.013. The mechanism is straightforward: NAR races are predominantly sprint/mile
distances (≤1600m accounts for the large majority of starts). A sire's overall track
win rate is therefore already a near-sufficient statistic for the sire's sprint aptitude
in NAR. The `sire_distance_win_rate` (already in the 174 features) further conditions
on today's exact distance band. Together, `sire_track_win_rate` + `sire_distance_win_rate`

- `pedigree_score_for_race` already capture the heredity-aptitude channel that the
  proposed sprint/route split attempts to isolate.

**On the genetic independence claim (r=0.35):** The literature-reported Spearman r=0.35
(5_2_53) documents independence at the sire EPD level (estimated breeding values from
a restricted dataset). In the NAR empirical data, the functional independence is
smaller: `sire_sprint_wr` and `sire_route_wr` are correlated at roughly 0.25–0.35
(consistent with literature), but both are strongly correlated with `sire_track_win_rate`
(0.82 and 0.50 respectively), which serves as a near-sufficient common factor. The
independent variance in the proposed split features, after conditioning on the existing
pedigree features, translates to partial rho ≤ 0.025 — insufficient signal to survive
WF retrain given the empirical prior of 11+ consecutive rejections/aborts.

**Mismatch-slice partials are equally null:** The concentrated mismatch slices (sprint-sire
at route, route-sire at sprint) show partial rho 0.005–0.018, not materially different
from the overall partial rho. The cross-band mismatch mechanism is not detectable as
incremental signal above the existing sire features.

**Probe script PROCEED was not actionable:** The script's `abort = False` decision used
`signal_threshold_probe_pass = 0.02`, which was not the abort threshold. It reported
`sire_sprint_wr` (partial rho=0.013) and `sire_sprint_place3_rate` (partial rho=0.024)
as exceeding this low pass threshold. Under the calibrated abort bar of 0.06, neither
clears it. The PROCEED_RECOMMENDED verdict from the script was a mechanical artefact
of a mismatched threshold parameter, not a signal finding.

## Operational Note

The verifying agent died mid-run due to an API error. The probe script had already
completed and written `probe_verdict.json` to disk before the agent died. The
orchestrator read the completed JSON artifact directly and adjudicated the ABORT
verdict from those numbers without re-running the probe. No probe results were lost;
the full `per_feature` table in the JSON is the definitive source of record.

## Future Research Directions

1. **Progeny-count-weighted shrinkage estimates:** The current `sire_sprint_wr` uses a
   hard min-support cutoff of 30 starts. A Bayesian shrunken estimator (James–Stein or
   empirical Bayes regression toward the population mean) would produce more stable
   estimates for sires with sparse data, potentially surfacing a cleaner independent
   signal. This would not fix the fundamental overlap with `sire_track_win_rate` but
   might reduce noise at coverage boundaries.

2. **Dam-line interaction (maternal aptitude split):** The existing `dam_sire_distance_win_rate`
   has correlation 0.061 with `sire_sprint_wr` — nearly orthogonal. A dam-line sprint/route
   split (analogous to the proposed sire split) would address a genuinely different
   heredity channel not proxied by `sire_track_win_rate`. The maternal inheritance
   contributes roughly equally to performance heritability (h²≈0.28).

3. **Genomic-era MSTN proxies:** 34_2305 and vol53-no2 p.141 document MSTN g.66493737C/T
   as a direct sprint/stamina discriminator (C:C = sprint, T:T = stamina, validated in
   JRA winner cohorts). MSTN genotype data is not available in the NAR operational
   database (nvd_um / nvd_se / nvd_ra contain no genomic fields). If genomic data were
   ever integrated, this would provide a per-horse aptitude signal with higher precision
   than the sire-rate proxy. Until then, this pathway is blocked by data availability.

4. **Distance specificity in the 1500–1600m grey zone:** The probe excluded 1500–1600m
   from both the sprint (≤1400) and route (≥1700) buckets. NAR has a significant volume
   of 1500/1600m races where sprint-sire horses may still perform well. Including this
   zone in a weighted aptitude feature (with distance-tapered weights) might produce a
   more continuous aptitude signal, though the existing `sire_distance_win_rate` likely
   already captures this via its 400m band.

5. **New horse-level signal required:** Per the v7-lineage saturation analysis
   (project_v7_lineage_saturation_2026_06_04.md), breaking the current accuracy ceiling
   requires genuinely new horse-level signals and a full retrain, not further refinement
   of pedigree-proxy features. The sire aptitude split is correctly characterised as a
   feature engineering exercise on information already encoded by existing sire features.

## Hard Rules Observed

- `tmp/` only: all artifacts written to `tmp/nar-perclass/sci_track/v6_sire/`
- No `git add tmp/`: sire-parquet not staged
- PG read-only: only SELECT queries issued (DuckDB postgres attach READ_ONLY)
- seed=42: enforced in probe script
- CatBoost thread_count=4: not invoked (no retrain reached)
- No authorized code changes deployed
