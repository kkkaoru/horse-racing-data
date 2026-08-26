# JRA continuous + categorical + quantile-bin embedding companion experiment

- Date: 2026-08-24
- Verdict: **ADOPT for dirt-small-005 only; deployed 2026-08-24**
- Scope: JRA finish-position; existing CatBoost/cell routes were retained as the baseline and the new model was evaluated only as a score-level companion.

## Final dropout-corrected production result

A serving-parity review found that the earlier MLX validation and OOF prediction paths had not called `model.eval()`, leaving dropout 0.1 active. All results in the later **Correction and second-pass result** and **Approved dirt-small-005 companion** sections are therefore retained only as superseded audit history.

The final implementation explicitly uses `train()` only for optimization and `eval()` for validation, OOF prediction, artifact fixture generation, and all-history export. Three complete 2023-2025 OOF seed sets were retrained after this correction.

The originally proposed equal three-seed mean selected companion weight 0.0 after correction and was rejected. Joint optimization on 2023-2024 only, requiring every seed to retain non-zero weight, selected:

```text
venue02_companion = 0.1 * seed1 + 0.1 * seed2 + 0.8 * seed3
served_score = 0.76 * current_prior_corner_z + 0.24 * venue02_companion_z
```

| period                |    rank1 |    rank2 |    rank3 |    rank4 |   rank5 | top3 box | objective |
| --------------------- | -------: | -------: | -------: | -------: | ------: | -------: | --------: |
| 2023 selection        |  0.000pp |  0.000pp |  0.000pp |  0.000pp | 0.000pp |  0.000pp |   0.000pp |
| 2024 selection        | +2.247pp | -2.247pp |  0.000pp | +1.124pp | 0.000pp |  0.000pp |  +0.449pp |
| 2025 blind (57 races) |  0.000pp | +1.754pp | +7.018pp |  0.000pp | 0.000pp |  0.000pp |  +1.754pp |

The production artifact is `jra-dirt-small-005-hybrid-v1`, trained afresh on 2013-2025 after each seed selected its epoch using 2013-2024 train / 2025 validation. It contains the complete 113-feature order, means, standard deviations, all bin boundaries, categorical vocabularies, architecture, route weights, and three immutable seed files.

NumPy serving parity on 32 races / 286 runners:

- input continuous max difference: `3.04e-7`;
- bin/category/race-category/umaban/mask: exact;
- per-seed rank flips: 0;
- weighted-ensemble rank flips: 0;
- final CatBoost+companion fused rank flips: 0.

Production integration is fail-closed: artifact load errors, leak-contract errors, structural feature gaps, or per-race scorer errors retain `jra-cb-v10-prior-corner274-2013`. Only the existing `prior_corner_dirt_smallfield_005` route invokes the hybrid.

Deployment completed through `finish-position-cron` with immediate container rollout:

- Worker version: `ec925f9a-dd23-42e2-9476-f457bd1a9a90`;
- container image tag: `ec925f9a`;
- R2 artifact hashes match `production-artifacts.json`;
- artifact integrity: `MATCH`, 44 selected keys;
- health endpoint: `{"cron":"0 18 * * *","name":"finish-position-cron","ok":true}`.

## Continued cell/weather search v5

A subsequent loop tested whether the corrected three-seed model could extend beyond `dirt-small-005` through fixed weather/range cells. This was a nested temporal evaluation: head and blend weight were selected on 2023, every exact-rank metric had to be nonnegative on the independent 2024 screen, and 2025 was then opened once as blind confirmation. The cells were fixed from track/physiology hypotheses rather than generated from outcomes: cold `<10°C`, hot `>=29.2°C`, very hot `>=33°C`, precipitation, heavy rain, high wind/gust, and their intersections with surface, route, and distance band.

Two cells passed the 2024 screen:

| cell                 | selected on 2023  | 2024 screen                                 | blind 2025                               |
| -------------------- | ----------------- | ------------------------------------------- | ---------------------------------------- |
| `class703+high-wind` | global head 0.10  | objective +0.116pp; all metrics nonnegative | objective -1.364pp; rank2/rank5 -4.545pp |
| `mile+hot`           | venue02 head 0.05 | objective +0.583pp; all metrics nonnegative | objective ~0; rank2 -0.662pp             |

Neither passed blind cell-level no-regression, so no additional production route was deployed. The result is consistent with model-selection variance: even temporally nested selection can surface false positives when many small cells are screened. Future iterations should use venue/month-normalized weather anomalies and multiplicity-aware hierarchical shrinkage rather than adding more raw threshold combinations.

Four-view review:

- **Statistics:** preserve 2023 selection / 2024 screening / 2025 blind separation; use date-cluster bootstrap and partial pooling before promotion.
- **Mathematics:** treat cell routing as a regularized mixture-of-experts problem with a sparse gate and shrink each cell toward its parent route, rather than independently maximizing noisy cell means.
- **Journal mechanism:** the heat-stress literature identifies temperature, humidity, wind, and workload/distance interactions. Temperature-only cells are an incomplete proxy because the current weather archive does not expose humidity/WBGT to the feature builder.
- **Computer science:** weather feature availability must be an explicit data contract. Current JRA production artifacts use only record-derived `weather_normalized`; the external `venue_*` columns are not silently assumed by the deployed model.

### Weather data-path audit and local repair

The 2023-2025 JRA weather store contains 10,365 races with zero missing values for temperature, precipitation, maximum wind, and gust in the audited race-level columns. The production `/weather` endpoint returned all 600 hourly rows for 2026-08-24. The 2026-08-25 endpoint was empty at 01:04 JST, correctly before its 01:30 forecast cron; at 01:44 JST it returned exactly 600 rows from KV, covering all 25 venues and every hour 00-23 once. A separate R2 SQL → fresh local DuckDB synchronization then inserted exactly the same 600 venue-hour keys: 25 venues, 24 distinct hours, range 00-23, and zero null temperature/precipitation/wind/gust rows. The scheduled production, R2 catalog, and local contracts are therefore complete rather than silently stale.

A genuine local parity defect was repaired in `generate_finish_position_features_local.py`: Phase A had always passed `venue_weather_dir=None`, so local feature generation discarded the maintained `apps/venue-weather/data` archive even though production full builds fetch weather. It now passes the repository weather archive path by default. Full Python validation passed: 4,949 tests and 97.54% coverage, with BasedPyright and ty clean.

Artifacts: `evaluation-weather-cells-v5.json` and `weather_cell_search.py` under the experiment directory.

### Venue/month-normalized anomaly cells v6

To avoid the raw wind-threshold distribution shift seen in 2025, v6 normalized temperature, rain, wind, and gust by a robust venue×calendar-month baseline. For each OOF fold, medians and `IQR / 1.349` scales were fitted using years strictly before that fold. This preserves physical locality and prevents the validation year from setting its own threshold.

Two cells passed the independent 2024 screen:

| cell                     | 2023 selection           | 2024 screen objective | blind 2025 objective | blind failure                  |
| ------------------------ | ------------------------ | --------------------: | -------------------: | ------------------------------ |
| `turf+temp-cold-anomaly` | venue02 head 0.20        |              +0.613pp |             +0.408pp | rank1/rank5 each -0.543pp      |
| `turf+gust-anomaly`      | dirt-small-005 head 0.30 |              +1.020pp |             -0.747pp | rank1 -1.042pp, rank3 -1.389pp |

Neither satisfies the frozen cell-level no-regression gate, so both are rejected and production remains unchanged. The cold anomaly result indicates useful ranking movement but not a deployable Pareto improvement; a future hierarchical model should learn the rank trade-off directly rather than selecting on a scalar objective.

Artifact: `evaluation-weather-anomaly-cells-v6.json`.

### Hierarchical exact-rank Pareto gate v7

A stricter mathematical formulation then searched every fixed and anomaly cell without collapsing rank metrics into a single selection objective. Each candidate had to keep exact rank1-5 and top3-box nonnegative separately in both 2023 and 2024. To reduce small-cell winner's curse, each cell delta was additionally shrunk toward its route or surface parent with a 200-race prior; all six shrunk deltas also had to remain nonnegative and the shrunk objective had to be positive in each year.

No head/weight/cell combination passed. This confirms that the apparent v5/v6 gains depended on rank trade-offs or unpooled small-cell noise, rather than a Pareto improvement. Since 2025 has now been inspected in multiple iterations, it is explicitly retired as a promotion holdout; future candidates require a new untouched forward window.

Artifact: `evaluation-hierarchical-pareto-cells-v7.json`.

### Causal humidity/wet-bulb and prior-day rain cells v8

Open-Meteo historical data for all 25 supported venues was backfilled into isolated evaluation-only v2 DuckDB files for 2023-2025. Coverage was complete: 8,760 hourly rows per venue in 2023/2025 and 8,784 in leap-year 2024. JRA race joins covered all 10,365 races with exactly three pre-post-time observations and zero missing humidity, wet-bulb, or completed prior-day precipitation values.

Fixed physiological/track cells included relative humidity >=80/90%, wet-bulb temperature >=24/26°C, humidity×wet-bulb, radiation×wet-bulb, and completed D-1 rain/heavy-rain with surface interactions. Using 2023 for head/weight selection and 2024 for an independent all-metric screen, **no JRA cell passed**. 2025 was not used for promotion because it has already been inspected repeatedly.

Artifact: `evaluation-causal-weather-v2-cells-v8.json`.

### Causal weather-window contract

The shared feature builder previously exposed only `venue_temperature_prior3` from the strictly pre-post-time window. Precipitation, wind, and gust were available only as 09:00–17:00 whole-day aggregates. Those are acceptable as pre-race forecast summaries in serving, but historical training uses observed actual weather and therefore includes hours after early races—a train/serve semantic mismatch and potential temporal leakage for a future weather model.

The builder now also emits, without changing any selected model's feature list:

- `venue_precipitation_prior3`;
- `venue_wind_speed_prior3_max`;
- `venue_wind_gusts_prior3_max`.

All use exactly the three complete hourly observations strictly before the scheduled post hour, matching the existing NAR research contract. Existing full-day columns remain for backward compatibility, but new weather candidates must use the prior-window fields. Tests verify that the post-hour observation and earlier out-of-window observations do not enter any of the four aggregates.

The causal weather fields were deployed in finish-position container image `8babdf00` / Worker version `8babdf00-1ed9-4a3b-9dd0-bd22af555043`. Artifact integrity remained `MATCH` and the health endpoint returned `ok: true`. No model selector or blend weight changed in this deployment.

Following the completed-day precipitation review, the builder also emits D-1, D-3, and D-7 precipitation totals ending at midnight before race day. The per-year loader includes the previous year's weather file, so a January 1 race receives December 31 context without future leakage. Humidity v2 adds nullable, strictly pre-post-time mean/max humidity, dew point, wet-bulb mean/max, and radiation mean fields. These additions were deployed in container `c338ec03` / Worker `c338ec03-0928-459d-9d11-905e9218c017`; current selected models ignore the new columns.

## Correction and second-pass result

The first-pass implementation below used the OOF fold year itself for early stopping. Its score therefore had checkpoint-selection leakage and must be treated as **superseded exploratory evidence**, not as the production decision result.

The corrected `multi-cell-v2` implementation makes two material changes:

1. Fold `Y` selects its epoch only on `Y-1`, then initializes a fresh model, trains that exact number of epochs on all data before `Y`, and predicts `Y` once. For example, blind 2025 uses 2024 for epoch selection and never uses a 2025 label during training or checkpoint selection.
2. One shared hybrid encoder learns five independently scored heads: `global`, `default`, `class703`, `dirt-small-005`, and `venue02`. Every head scores every race. A target cell can therefore combine results learned from multiple other cells, rather than being restricted to its own specialist.

The revised input also adds an explicit 113-dimensional missingness projection alongside continuous projection, categorical embeddings, and fold-local quantile-bin embeddings.

### Leakage-safe training checkpoints

| OOF fold | inner validation year | selected epochs | inner NDCG@3 |
| -------- | --------------------: | --------------: | -----------: |
| 2023     |                  2022 |               2 |      0.54823 |
| 2024     |                  2023 |               4 |      0.53751 |
| 2025     |                  2024 |               2 |      0.55598 |

### Cross-cell simplex weight search

Weights were searched over a non-negative 0.1 simplex across six scores: current production plus all five learned heads. Selection maximized the **minimum** objective delta across 2023 and 2024, so a gain in only one year could not compensate for a loss in the other.

| target cell    | selected mixture                                                             |
| -------------- | ---------------------------------------------------------------------------- |
| default        | current 0.3 + class703 head 0.1 + dirt-small-005 head 0.1 + venue02 head 0.5 |
| class703       | current 1.0                                                                  |
| dirt-small-005 | current 0.9 + venue02 head 0.1                                               |
| venue02        | current 1.0                                                                  |

This confirms that the requested cross-cell mechanism works: the default cell selected learning results from **three different specialist cells simultaneously**, while dirt-small-005 selected the venue02 specialist.

2023-2024 selection delta was:

|    rank1 |    rank2 |    rank3 |    rank4 |    rank5 | top3 box |
| -------: | -------: | -------: | -------: | -------: | -------: |
| +0.304pp | +0.130pp | +0.029pp | -0.043pp | +0.145pp | +0.029pp |

However, frozen-weight blind 2025 delta was:

|        rank1 |    rank2 |    rank3 |    rank4 |        rank5 |     top3 box |
| -----------: | -------: | -------: | -------: | -----------: | -----------: |
| **-0.289pp** | +0.116pp | +0.029pp | +0.203pp | **-0.318pp** | **-0.087pp** |

All bootstrap confidence intervals crossed zero. Most of the regression came from the default cell: rank1 -0.484pp and rank5 -0.581pp. The small dirt-small-005 cell improved rank2 +1.754pp, rank3 +3.509pp, and rank5 +1.754pp, but blind 2025 contains only 57 races in that cell, so this is not sufficient evidence for deployment.

### Weight-regularized production gate

A second selection policy capped total companion mass at 0.3 and required, independently in both 2023 and 2024:

- objective delta >= 0;
- rank1 delta >= 0;
- no individual metric below -0.25pp.

Only the unchanged current weight passed in every cell. Consequently the production-safe result is current 1.0 for all cells. The multi-cell implementation is retained as an experiment, but no learned head should be routed into production yet.

## Approved dirt-small-005 companion and continued review

The user approved adoption of the dirt-small-005 companion. The approved route is now:

```text
dirt-small-005 = 0.9 * current_z + 0.1 * venue02_specialist_z
```

The initial single-seed blind-2025 cell result (57 races) preserved rank1 and improved rank2 +1.754pp, rank3 +3.509pp, and rank5 +1.754pp.

### Overlapping range heads

`multi-range-v3` changed mutually exclusive specialists into nine overlapping learning ranges:

- dirt-all;
- field-le10-all;
- class005-all;
- dirt-class005;
- dirt-field-le13-all;
- dirt-field-le13-class005;
- dirt-field-le10-all;
- venue02-all;
- exact-dirt-small-005.

A race can contribute to every matching range simultaneously. Target-cell weights were constrained to include at least two distinct learned ranges, with at most 30% total new weight.

The search selected multiple ranges for default and class703, but both regressed on blind 2025. No multi-range addition improved the already-approved dirt-small-005 baseline consistently across both 2023 and 2024. A finer 0.05 grid also failed: the best two-range dirt addition had robust objective delta -0.393pp and reduced blind rank4/rank5 by 1.754pp.

### Hierarchical residual ranges

`multi-range-residual-v4` replaced independent specialist heads with `global_score + learned residual`, initialized with residual scale 0.1. This was intended to protect small cells from losing the global representation. Nevertheless, every two-range mixture had a negative robust selection delta. The approved baseline remained weight 1.0 in all target cells.

Thus the implementation supports multiple overlapping cell ranges, but forcing their adoption would reduce measured accuracy. They are not added to the approved dirt route.

### Three-seed stability check

The approved venue02 contribution was retrained with two additional leakage-safe seeds. Weight selection remained isolated to 2023-2024.

| venue02 specialist source | selected dirt weight | 2023 objective delta | 2024 objective delta | blind 2025 effect                                       |
| ------------------------- | -------------------: | -------------------: | -------------------: | ------------------------------------------------------- |
| seed 1                    |                 0.10 |              0.000pp |             +0.225pp | rank2 +1.754pp, rank3 +3.509pp, rank5 +1.754pp          |
| seed 2                    |                 0.00 |              0.000pp |              0.000pp | unchanged                                               |
| seed 3                    |                 0.10 |             +0.469pp |             +0.169pp | rank4 -1.754pp                                          |
| mean score of seeds 1-3   |             **0.10** |         **+0.156pp** |         **+0.281pp** | **rank3 +1.754pp; all other tracked metrics unchanged** |

The deploy candidate is therefore strengthened from one seed to the mean score of three seeds:

```text
dirt-small-005 = 0.9 * current_z + 0.1 * zscore(mean(venue02_head_seed1..3))
```

This keeps the user's approved route, is positive in both selection years, and removes the seed-specific blind rank4 regression. Confidence remains limited by the 57-race blind cell, so serving parity and fail-closed fallback are required before deployment.

## Superseded first-pass experiment

The remainder of this document records the original experiment for auditability. Its blind tables are not used for the final decision because of the early-stopping issue described above.

## Question

Test whether a runner representation combining:

1. continuous-value projection,
2. categorical embeddings, and
3. train-fold-only quantile-bin embeddings

can improve the current JRA routed prediction by tuning blend weights per existing production route cell. Accuracy must be measured at exact ranks 1-5, not only by a pooled summary.

## Leakage and evaluation contract

- Data: `tmp/feat-jra-v9-weather`, JRA 2013-2025.
- Candidate inputs: the 113 leak-free production-feature columns available consistently in that store. Current-race `target_corner_*`, `target_running_style_class`, `finish_position`, and `finish_norm` are denied.
- Walk-forward folds: blind 2023, 2024, and 2025; each model trains only on 2013 through `fold_year - 1`.
- Continuous mean/std, categorical vocabularies, and quantile boundaries are fitted separately on each fold's training rows. Validation data never determines a boundary or vocabulary.
- Current comparator: reconstructed OOF production routing over the identical 2023-2025 races:
  - `kyoso_joken_code=703` -> 269-feature jockey/pedigree model;
  - dirt + field <=10 + `kyoso_joken_code=005` -> 274-feature prior-corner model;
  - venue 02 -> 269-feature model;
  - otherwise -> 250-feature clean champion.
- Blend-weight selection uses 2023-2024 OOF only. Weights are frozen before the blind 2025 confirmation.
- Score fusion uses within-race z-normalization so CatBoost and Transformer score magnitudes are comparable.

The experiment is a **companion** test, not a replacement test: weight `0.0` exactly preserves the current route.

## Candidate architecture

Each horse is one race-set token:

```text
continuous 113-vector -> standardization -> linear projection
113 train-fold quantile-bin IDs -> per-feature embeddings -> normalized sum
track/grade/class/sex/month + venue/season + umaban -> categorical embeddings
sum with learned component scales -> LayerNorm -> race-set self-attention -> rank score
```

Quantile encoding has 16 bins plus a dedicated missing bin. Component scales are trainable rather than fixed.

Base candidate: dimensions 64, 2 attention layers, 4 heads. Loss is ListNet plus 0.25 pairwise loss. A larger dimensions-96 / 3-layer arm was also tested.

## Current cell accuracy before treatment

Current production routing reconstructed over 2023-2025 contains 10,365 races / 141,523 runners.

| Current route cell | races |   rank1 |   rank2 |   rank3 |   rank4 |   rank5 | top3 box |
| ------------------ | ----: | ------: | ------: | ------: | ------: | ------: | -------: |
| default            | 6,208 | 32.603% | 17.590% | 13.434% | 11.582% | 10.921% |   8.231% |
| class703           | 3,710 | 35.714% | 19.084% | 15.768% | 12.911% | 11.213% |  11.509% |
| dirt-small-005     |   210 | 42.857% | 21.429% | 17.143% | 17.143% | 13.333% |  16.667% |
| venue02            |   237 | 33.333% | 17.300% | 13.502% | 11.392% | 12.236% |   6.329% |

A separate full canonical-cell audit produced 639 surface × distance × class × season × venue cells in `tmp/candidate-jra-bin-embedding-2026-08-24/current-cell-accuracy-2023-2025.json`. The largest cell has only 152 races across all three years. Therefore independently fitting 639 cell weights would be selection overfit, not reliable optimization. Weight tuning was restricted to the four existing production route cells above.

## Trial 1: continuous + categorical control

The no-bin control selected the following 2023-2024 weights:

| cell           | companion weight |
| -------------- | ---------------: |
| default        |             0.25 |
| class703       |             0.25 |
| dirt-small-005 |             0.05 |
| venue02        |             0.65 |

Blind 2025 delta versus current:

|    rank1 |    rank2 |    rank3 |    rank4 |    rank5 | top3 box |
| -------: | -------: | -------: | -------: | -------: | -------: |
| -0.405pp | +0.260pp | -0.608pp | +0.289pp | -0.145pp | -0.260pp |

This arm is decisively unsuitable as a routed companion.

## Trial 2: continuous + categorical + bin embedding

The hybrid's learned scales at each fold's best checkpoint were:

| fold | continuous | categorical |   bin |
| ---- | ---------: | ----------: | ----: |
| 2023 |      0.985 |       0.959 | 1.137 |
| 2024 |      0.986 |       0.979 | 1.114 |
| 2025 |      0.993 |       0.985 | 1.071 |

The bin path was not ignored: its learned scale remained above the continuous and categorical scales in all three independent folds.

The 2023-2024 selection chose:

| cell           | companion weight |
| -------------- | ---------------: |
| default        |         **0.00** |
| class703       |         **0.30** |
| dirt-small-005 |         **0.00** |
| venue02        |         **0.00** |

Thus three of four current cells already preferred the unchanged production model during selection.

### Blind 2025 pooled result

3,455 races:

| metric   | current | candidate |    delta | bootstrap LB95 | bootstrap UB95 |
| -------- | ------: | --------: | -------: | -------------: | -------------: |
| rank1    | 33.430% |   33.314% | -0.116pp |       -0.434pp |       +0.203pp |
| rank2    | 18.350% |   18.466% | +0.116pp |       -0.347pp |       +0.608pp |
| rank3    | 13.864% |   13.980% | +0.116pp |       -0.318pp |       +0.550pp |
| rank4    | 11.983% |   12.185% | +0.203pp |       -0.232pp |       +0.666pp |
| rank5    | 11.288% |   11.114% | -0.174pp |       -0.550pp |       +0.203pp |
| top3 box |  9.609% |    9.638% | +0.029pp |       -0.174pp |       +0.232pp |

Every confidence interval crosses zero. Rank1 and rank5 regress in point estimate.

### Blind 2025 class703 cell

Only class703 had a non-zero selected companion weight. On its 1,252 blind races:

| metric   | current | candidate |    delta |     LB95 |     UB95 |
| -------- | ------: | --------: | -------: | -------: | -------: |
| rank1    | 34.505% |   34.185% | -0.319pp | -1.198pp | +0.559pp |
| rank2    | 19.010% |   19.329% | +0.319pp | -0.958pp | +1.597pp |
| rank3    | 14.377% |   14.696% | +0.319pp | -0.879pp | +1.518pp |
| rank4    | 13.179% |   13.738% | +0.559pp | -0.639pp | +1.837pp |
| rank5    | 11.262% |   10.783% | -0.479pp | -1.518pp | +0.559pp |
| top3 box | 10.942% |   11.022% | +0.080pp | -0.479pp | +0.639pp |

The apparent rank2-4 improvements are not robust and accompany rank1/rank5 regression.

## Trial 3: larger bin model

A dimensions-96 / 3-layer / 4-head version was trained on the same folds. Selection chose weights `default=0.60`, `class703=0.05`, `dirt-small-005=0.00`, `venue02=0.05`, but blind 2025 produced:

|    rank1 |    rank2 |    rank3 |    rank4 |    rank5 | top3 box |
| -------: | -------: | -------: | -------: | -------: | -------: |
| -1.013pp | +0.145pp | -0.203pp | -0.289pp | -0.058pp | -0.232pp |

This is a clear capacity/selection-overfit failure. Increasing model size does not rescue the architecture.

## Trial 4: rank-specific weights

To test whether one global ordering weight was hiding rank-specific value, separate weights for exact ranks 1-5 were greedily selected per production route cell on 2023-2024, with already-selected horses removed before choosing the next rank.

Blind 2025 result:

|    rank1 |    rank2 |    rank3 |    rank4 |    rank5 | top3 box |
| -------: | -------: | -------: | -------: | -------: | -------: |
| -0.521pp | +0.058pp | -0.463pp | +0.058pp | -0.666pp | -0.029pp |

Rank1 LB95 was -1.100pp and rank3 LB95 was -1.158pp. The highly variable selected weights did not generalize and demonstrate why per-rank/per-cell maximization on the same historical cells is unsafe.

## Bin ablation interpretation

Standalone blind-2025 scores show that bins add a small real change relative to the continuous+categorical control:

| arm                       |       rank1 |   rank2 |       rank3 |   rank4 |   rank5 |   top3 box |
| ------------------------- | ----------: | ------: | ----------: | ------: | ------: | ---------: |
| continuous+categorical    |     32.735% | 18.524% |     13.546% | 12.504% | 11.404% |     9.320% |
| + quantile-bin embeddings |     32.880% | 18.582% |     13.054% | 12.504% | 10.970% |     9.291% |
| current routed production | **33.430%** | 18.350% | **13.864%** | 11.983% | 11.288% | **9.609%** |

Bins improve the standalone arm by +0.145pp rank1 and +0.058pp rank2, but lose -0.492pp rank3, -0.434pp rank5, and -0.029pp top3 box. The representation is active, but it does not create a sufficiently strong or stable companion signal beyond the current routed models.

## Final decision

The broad JRA rollout remains **REJECTED**, but the user-approved dirt-small-005 exception advances as a three-seed deploy candidate:

- default, class703, venue02: production unchanged;
- dirt-small-005: current 0.9 + three-seed venue02-head mean 0.1;
- overlapping range additions: rejected because they did not pass the 2023/2024 stability gate.

Production routing must not change until an all-history artifact, NumPy serving parity, feature-contract fallback, and eval-through-serving gate are complete. This decision is based on leakage-safe results, not on the superseded first-pass fold scores.

Reasons:

1. Three of four production cells select weight zero in the best base-size hybrid.
2. The only selected non-zero cell fails blind confirmation and regresses rank1/rank5.
3. No pooled or cell-level blind improvement has LB95 above zero.
4. Larger capacity substantially overfits selection years.
5. Rank-specific cell tuning worsens blind rank1/rank3/rank5.
6. The result is consistent with the existing JRA finding that race-set Transformer companions are redundant against the efficient-market, listwise CatBoost route. Bin embeddings alter that model but do not overturn the result.

## Local artifacts

All experiment code and generated artifacts are under:

`apps/pc-keiba-viewer/tmp/candidate-jra-bin-embedding-2026-08-24/`

Key files:

- `experiment.py`
- `rank_weight_eval.py`
- `multi_cell_experiment.py`
- `multi_range_experiment.py`
- `multi_range_residual_experiment.py`
- `current-cell-accuracy-2023-2025.json`
- `evaluation-hybrid-bin.json`
- `evaluation-hybrid-bin-c2.json`
- `evaluation-continuous-cat.json`
- `evaluation-hybrid-bin-rank-specific.json`
- `evaluation-multi-cell-v2.json`
- `evaluation-multi-cell-v2-conservative.json`
- `evaluation-multi-range-v3.json`
- `evaluation-multi-range-residual-v4.json`
- `evaluation-dirt-multiseed.json`
- `blind-bootstrap-hybrid-bin.json`
- `blind-standalone-comparison.json`
- fold checkpoints, predictions, and training logs

No production code or configuration was changed by this experiment.
