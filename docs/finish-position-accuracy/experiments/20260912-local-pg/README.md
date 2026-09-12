# 2026-09-12 local-PG ranker experiments

Deadline: 09:00 JST. Training source: **local PostgreSQL only**.

## Final decision — 08:30 JST

**精度改善を確認できず、新モデルは不採用。本番は変更していません。** See [`final-decision.json`](final-decision.json) for the complete decision and unresolved gates. Captured serving replay matches120/120 ranks; that is reproducibility, not an accuracy improvement. Browser heatmap verification is blocked by Cloudflare Access authentication.

Final checks: viewer5,266 passed/97.39%; TimesFM172 passed/97.28%; replay25 passed/100%; package-scoped typing and changed replay lint/format pass. No commit, deployment or prediction regeneration. Both saved replay prediction frames remain exactly unchanged after the final conversion cleanup.

## Earlier status — 08:18 JST

The replay also matches all12 published sample standard deviations within `1.263e-15`. `audit-canonical-observations.json` records505,110 cross-source duplicate pairs and14 conflicting finish-label pairs; all14 saved observations follow the declared native NVD priority, with0 mismatches and0 same-day/future observations. Original training records and final selections remain unchanged.

**No promotion; production unchanged.** Captured final-cache replay now reproduces **120/120 published ranks across12 Ban-ei races** with the unchanged native model, production pandas ingestion and existing sim Prophet policy (`audit-serving-card.json`). This is serving reproduction, not an accuracy gain. Local PG feature parity and deployed-image byte attestation remain separate, unresolved questions.

The earlier null→zero assumption omitted production's pandas ingestion: numeric parquet NULL becomes NaN before scoring. The research replay now exposes an explicit `--frame-loader pandas` option without changing old defaults/artifacts. Earlier Polars frozen evaluations must not be presented as validated full production baselines. Updated replay tests24/24 and100% coverage; full viewer5,265/97.39%, full TimesFM172/97.28%, both package-scoped type checks clean. Frozen TimesFM2026 nominees remain rejected; no retuning from final results.

## Earlier status — 07:45 JST

**No promotion; production unchanged.** All five TimesFM adaptation families completed. The frozen2026 shortlist fails: Ban-ei joken-000 loses exact2nd place on33 races/3 dates;55/2YO is unchanged on3 races/3 dates. `final-family-selection-v1.json` records the prior nomination; `timesfm-all-cell-year-summary-v1.json` covers all70 cell/years, including zeros and per-rank supports. Final results are not used for retuning.

Read-only serving audits now include `cloudflare-r1-foundation.json`, `audit-captured-foundation.json` and `audit-captured-foundation-raw-sim.json`. These captured production inputs are **never training data**. Replaying production's raw-table similarity SQL against local PG repairs the zero target-entity statistics (raw120 versus derived-table0) and matches the captured R1 raw order10/10, but only6/10 published ranks. Full serving parity remains unproven. See the [ledger](../../history/nar-banei-20260912-improvement-ledger.md) for details and outstanding quality/UI gates.

## Earlier status — 06:55 JST

No candidate is promotion-eligible; production is unchanged. See the [current ledger](../../history/nar-banei-20260912-improvement-ledger.md) for superseding results and limitations.

- `timesfm-rustuna-plan.md`: official TimesFM-3 reuse, personal noncommercial license scope, twenty-year expanded training membership, and three declared adaptation families.
- `timesfm-input-v{1,2}/`, `timesfm-forecast-v{1,2}/`: retained scope/query/cache provenance; V2 adds completed-past-day speed contexts. All observations are local-PG-derived.
- `timesfm-rustuna-v{1,2,3}/`, `timesfm-paired-development-v{1,2,3}/`: exact Top1–Top5, earlier-year-only Rustuna trials and matched controls. V2's55/2YO has four positive point deltas and one unchanged rank, but all99% intervals cross zero. V3 innovation readouts retain mixed outcomes. No2026 readout results were consulted to design V2/V3.
- `audit-single-race-parity.json`, `audit-single-race-features.{sh,json}`: focused83:01 and whole-card ranks agree10/10, but model inputs agree only1,294/1,300; this is not production parity. Their CF rank agreement is1/10.
- `production-upcoming-prophet-v1/`, `audit-prophet-parity.sh`: inference-only check of the existing frozen post-score layer, separate from the unchanged booster inputs. It does not attest deployed policy/environment or acquire training observations.
- Latest focused tests:43 passed, exact/query cores100% statement/branch coverage. Full configured checks still need refreshing; earlier full-suite results below predate the TimesFM adapters.

## Reproducible code and outputs

- `apps/pc-keiba-viewer/src/scripts/learning/race_time.py`: raw MSSd decoder, matching the race detail page; 1234 -> 83.4 seconds.
- `learning/history_features.py`: strictly previous-day horse, jockey and trainer windows. Current result/clock columns are audit/label data, not predictors.
- `learning/cell_training_scope.py`: cell-specific 20-year seed window, expansion to every seed entrant's earlier races and all competitors, separate evaluation membership. NAR venue is mandatory.
- `learning/history_ablation.py`: CLI and fixed-budget QuerySoftMax trainer; 200 trees, depth 6, learning rate .05, seed 20260912, four threads, no evaluation-set early stopping. Explicit feature allowlist.
- `apps/pc-keiba-viewer/scripts/run-history-ablation.sh`: resumable 2020–2026, venue 83/54/55, speed/no-speed comparisons. Starts no new fits after 08:50 JST.
- `export-local-history.sql` and `export-recent-raw-local-pg.sql`: read-only source extraction, intended for `psql` COPY output. The first defines a broad envelope; every fold narrows its own seed window independently.
- `target-card-local-pg.csv`, `cell-race-metadata-local-pg.csv`, `race-cells-local-pg.parquet`, `target-cells.json`: saved card and cell provenance. Class/distance/season derivation reuses existing production helpers.
- `ablation-v1/`: model bytes, runner predictions, metrics, and scope race IDs. `september-confirmation-v1/`: frozen-model September confirmation.
- `banei-paired-evaluation.sql`: run from repository root; generates year/cell paired metrics. Saved result: `banei-paired-cell-evaluation.json`.

Run one trial from `apps/pc-keiba-viewer`:

```sh
PYTHONPATH=src/scripts uv run python -m learning.history_ablation \
  --features /absolute/path/to/existing-local-pg-causal-features.parquet \
  --output ../../docs/finish-position-accuracy/experiments/20260912-local-pg/ablation-v1/83/2020/speed \
  --venue 83 --year 2020 --include-speed
```

Resume the sweep with the existing local-PG feature input:

```sh
bash apps/pc-keiba-viewer/scripts/run-history-ablation.sh \
  /private/tmp/horse-nar-banei-0912/causal-features.parquet
```

That input was produced before the instruction to stop adding scratch files. It is now read-only input; subsequent outputs and scripts stay in repository paths not excluded from Git. The previous scratch-output sweep was interrupted at 02:04 JST and its completed artifacts moved here, without changing their contents. New output is written directly here, not via scratch-directory symlinks. No new scratch input copies are required to resume.

## Metric contract and release gate

`rank1`–`rank5` mean exact finishing-position matches, each divided by the number of evaluated races. They are **not** winner-in-TopK recall. All five are inspected, with counts and paired same-race deltas. Insufficient short-field support must be identified rather than inferred as evidence of accuracy.

These compact model ablations compare against their own no-speed baseline, **not the current production model**. Every report is non-promotion-eligible. Production release additionally requires matched out-of-time current-production comparison, all-five-rank checks, uncertainty/selection controls, inference-feature parity, selective target-cell regeneration, and verification of the jockey/trainer/pedigree win-rate heatmap contract. No production model has been changed.

## Data quality

Target: 34 races / 348 entries (Kochi 12/126, Saga 10/102, Ban-ei 12/120), grouped into ten basic venue/class/distance/season/surface cells. Raw scheduled field size is `00`; derive it from actual entries. Field/going-specific routing requires further refinement as those inputs become known.

The broad historical export contains 4,178,907 rows: NAR 2,607,843, JRA 1,571,064, with JRA histories back to 1993. NAR source history starts in 2005: early 2020 evaluation folds therefore have truncated 20-year source coverage, which is not treated as complete.

The derived local `race_entry_corner_features` table was stale for recent confirmed results. Original Ban-ei 2026 predictions ended on August 30 despite later unlabelled rows. The raw local `nvd_se`/`nvd_ra` source provides completed September 5–7 Ban-ei races. Raw-priority 2026 merge added/replaced rows, with zero duplicate race/horse keys; corrected features were kept separate from the original v1 inputs. The corrected source has 4,235,400 rows, including NAR through September 11. This must not be confused with all venues having every result through that date.

## Observed rejection

- Ban-ei 2020 full-year Top1: no-speed 22.07%, speed 24.33%. Other exact ranks include small regressions.
- Autumn `joken-000` Top1 improved in each 2020–2025 point estimate, but other exact ranks regressed in several years. `E` was unstable.
- **Frozen September 5–7 confirmation: 36 races / 350 classified finishers.**

| Exact rank | No speed |  Speed |
| ---------- | -------: | -----: |
| 1          |   27.78% | 22.22% |
| 2          |   16.67% | 25.00% |
| 3          |   13.89% |  8.33% |
| 4          |   13.89% |  5.56% |
| 5          |   13.89% |  8.33% |

**Reject this speed-only addition for production.** The September interval is now consumed evidence, not an untouched holdout for subsequent hypotheses.

## Predeclared Top5 objective experiment (02:15 JST)

The initial binary QuerySoftMax target is 1 only for the winner. Its loss has no ordinal target distinction between a second-place and fifth-place finisher. This is a poor direct match to the requested **five exact positions**, even when winner discrimination improves.

Test a fixed alternative, not an outcome-tuned search: `YetiRank:mode=NDCG;top=5;dcg_type=Base;dcg_denominator=LogPosition`, relevance `max(6-finish, 0)`, keeping the tree budget, seed and input features fixed. CatBoost's [ranking objective reference](https://catboost.ai/en/docs/concepts/loss-functions-ranking) documents non-Classic YetiRank `mode=NDCG`, `top`, gain type and denominator, with CPU-only support. NDCG is a surrogate; only observed exact rank1–5 changes can support the user's objective.

Start with Ban-ei 2024 and 2025 (both speed/no-speed), then extend chronologically. This is exploratory evidence, not a reset of consumed validation data or a production adoption decision.

```sh
ABLATION_OBJECTIVE=top5 ABLATION_VENUES=83 ABLATION_YEARS='2024 2025' \
  bash apps/pc-keiba-viewer/scripts/run-history-ablation.sh \
  /private/tmp/horse-nar-banei-0912/causal-features.parquet
```

Outputs: `top5-ablation-v1/`, a Git-eligible directory alongside the original binary-objective results. No new scratch feature copies.

## Relative-speed hypothesis (02:42 JST, before fitting)

Add three lagged contrasts: career, previous 365-day and previous 28-day means of within-race standardized speed `(metres/seconds - race mean) / race SD`. A common positive rescaling within a historical race cancels; this tests removal of race-level conditions, not an asserted causal adjustment for every handicap. Zero-variance/single-observation contrasts stay missing. Same-day and future outcomes are excluded. Current contrasts are never predictors.

`learning/relative_history_features.py` computes these features in memory over the existing local-PG input, before selecting the training scope. DuckDB disk spilling is disabled. No new scratch feature snapshots are written. Keep winner objective, tree budget and seed fixed and initially compare Ban-ei 2024/2025 against the same absolute-speed/no-speed arms. Previously inspected intervals remain consumed evidence.

```sh
ABLATION_RELATIVE_SPEED=1 ABLATION_VENUES=83 ABLATION_YEARS='2024 2025' \
  bash apps/pc-keiba-viewer/scripts/run-history-ablation.sh \
  /private/tmp/horse-nar-banei-0912/causal-features.parquet
```

Outputs: `relative-ablation-v1/`. Evaluate exact ranks, not just the training loss.

## Paired uncertainty and production provenance

`learning/paired_rank_evaluation.py` and `scripts/evaluate-history-objectives.sh` persist six original-objective comparisons in `paired-evaluation-v1/`. Identical runner identities, dates and outcomes are required; resample dates and divide summed hit deltas by summed race counts. Approximate percentile intervals use five-rank Bonferroni tails and abstain below twenty dates. They do not correct across the entire model/cell search.

`cloudflare-target-summary.request.json` / `.response.json` verified 34 races / 348 finite predicted ranks: Kochi/Saga use `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`, Ban-ei uses `banei-cb-v9-sim-2011`. The older `cloudflare-target-predictions.response.json` contains truncated text and is **not** a complete snapshot. Prediction state alone is not rendered win-rate heatmap verification.

The local-PG `race_finish_position_features` audit found only May 23–24 usable-category rows for these venues, no September features, plus old rows incorrectly tagged `jra` at NAR venues. Do not treat this table as a complete canonical baseline or silently zero-fill absent model predictors. Full verification after the objective/paired-evaluation additions: 5,166 tests passed, 97.56% coverage, global basedpyright zero errors; relative-speed changes require their own verification.

## Frozen production baseline reconstruction (03:14 JST)

`build-local-banei-production-base.sh` and `build-local-banei-production-layers.sh` rebuild the existing baseline recipe from local PG. The base uses the production builder's ten-year history; this is not a substitution for the candidate's configurable twenty-year seed scope. Six consumed layers reconstruct lineage, head-to-head, going/pedigree, carried-weight class, grade career and similar-race features. The final audit (`audit-banei-final-features.sql` / `.json`) finds **all 130 model inputs**, 5,527 rows, May 23–September 7. Source window/output dates are reported as observed, not assumed from the requested interval.

The native similar-race script previously overrode the common resource helper with a shared `/tmp` path. It now respects `PIPELINE_SPILL_TEMP_DIR`; the wrapper chooses a repository job-specific location, covered by an existing end-to-end test. No model feature mathematics changed in that repair.

`learning/frozen_production_evaluation.py` uses the actual serving numeric projection (`predict_lib.scorer.build_feature_matrix`, CatBoost path), including null-cell zero coercion. It rejects absent predictor columns. A model with named predictors must match metadata order; a model with canonical positional names `0..n-1` uses the same bundled metadata order as production's `_load_model_bundle`. This is explicitly reported as `positional-metadata`, not falsely described as an embedded-name match. The Ban-ei artifact uses this positional format.

From `apps/pc-keiba-viewer`:

```sh
PYTHONPATH=src/scripts:../finish-position-predict-container/src \
  uv run python -m learning.frozen_production_evaluation \
  --features '../../docs/finish-position-accuracy/experiments/20260912-local-pg/production-baseline-features/83/final/race_year=*/*.parquet' \
  --model-dir ../finish-position-predict-container/models/finish-position/ban-ei/banei-cb-v9-sim-2011 \
  --output ../../docs/finish-position-accuracy/experiments/20260912-local-pg/frozen-production-v1/83
```

Only outcomes after the declared training end (May 18) are allowed. Observed-finisher cohort completeness and **retrospective market-input availability versus the intended serving stage remain unverified**. This reconstruction alone is not an adoption gate or a claim of historically served predictions.

## Rich-history candidate preparation (03:24 JST)

Prepare the 130-feature native recipe over the locally available 2005–2026 Ban-ei observations, then test whether adding the corrected absolute/relative speed histories improves it. This is a richer comparator than the initial 14-feature research model. New models must still use chronological 2020–2026 cutoffs and the configured twenty-year seed-horse expansion; before fitting, audit that the rich feature rows cover the entire scope (including any cross-venue histories). No missing scoped races may be silently discarded.

```sh
export FEATURE_RUN=rich-history-v1 FROM_DATE=20050101 TO_DATE=20260911 HISTORY_FROM_DATE=20000101
# PG_URL must point to the authorized local horse_racing database.
bash apps/pc-keiba-viewer/scripts/build-local-banei-production-base.sh
bash apps/pc-keiba-viewer/scripts/build-local-banei-production-layers.sh
```

Outputs are separate from the frozen baseline reconstruction under `rich-history-v1/83/`. Feature preparation is not training completion, a production improvement claim or a release decision.

The cohort audit also found that old compact 2026 predictions are much sparser than the raw-source baseline: May 23–August 30 has 1,291 old candidate rows versus 4,708 baseline rows. Raw refreshed labels and native labels agree; the twenty native-absent rows are unlabelled. Re-score frozen compact models on refreshed input rather than comparing mismatched aggregates or dropping hundreds of unmatched races.

## Scope closure correction (03:41 JST)

`audit-rich-history-scope.sql` finds 340,020 required pre-2026 observations, including historical venues 81 (5,448), 82 (10,893), and 84 (6,035), in addition to 83. A current-venue-only native build is therefore insufficient: **22,089 labelled old-venue observations were missing**, all in 2005–2006. Labels agree on present observations. The remaining missing rows are unlabelled and must be classified, not silently treated as finishers.

`build-local-retired-banei-features.sh` rebuilds a local-PG NAR base for 2005–2006, filters it to 81/82/84 using `select-retired-banei-base.sql`, then applies the consumed feature layers. This recovers historical training venues without changing today's evaluation routing. Before any rich fit, re-audit the union of current-venue and retired-venue features against the full scope, including label consistency and any missing-feature histories. No silent inner join or all-zero fabricated rows.

The initial compact models have now been compared on identical 547 races / 5,178 observed finishers over 46 dates. The speed candidate's Top1 delta is -11.52 percentage points (five-rank-adjusted approximate interval [-18.06,-5.30]); all five point estimates are lower than the frozen production reconstruction. Reject these compact replacements. `compare-local-banei-baseline.sh` and `refreshed-evaluation-v1/` preserve the exact comparison and its remaining market-stage caveat.

The original three-venue × seven-year × two-arm sweep produced all 42 model/prediction/report triples. Its shell exited with a trailing parse error after the last result (the script had been edited while running); current `bash -n` passes. Retain the failure record, and do not edit scripts while their running shell is still reading them.

## Early-card experiment policy (04:08 JST)

The union audit (`audit-rich-union-scope.json`) now covers **all 333,761 labelled required pre-2026 rows**, with zero label disagreements and duplicate keys. 1,687 missing native observations are unlabelled; the rich experiment retains them in its scope rather than silently dropping their races. The retired-venue base succeeded, but its extension pipeline stopped at the explicit 83-only lineage guard. Thus the first rich experiment uses actual retired base features and explicitly missing extension cells; it does **not** claim complete retired-venue native context.

Before viewing any rich-model scores, the fixed early-card policy excludes current odds/popularity, weigh-in difference, observed weather/going, and every going-conditioned pedigree lookup. Historical weights, historical odds summaries and strictly previous-day history remain eligible. This leaves 117 native features, or 135 with the corrected absolute/relative and basic history features (also excluding current `going`). Both arms use the production recipe's fixed Classic YetiRank, 300 trees/depth 8, learning rate .05, L2 3, seed 20260519. `run-rich-history-ablation.sh` evaluates 2020–2026 and starts no fit after 08:50 JST.

Source inspection matters: upcoming missing market scores are **medians 0.5000/0.5048**, not blindly zero, in `finish_position_features_duckdb.py`'s `legacy_features`. Raw zero body weight may also propagate differently from a missing feature. A local upcoming-mode reconstruction is therefore required before asserting frozen-production early-stage parity. The prior 36.93% Top1 frozen baseline used retrospective observed context; compact candidates remain unqualified for promotion, but that comparison alone does **not** establish production superiority at the early-card stage.

## Body encoding and actual upcoming-input audit (04:24 JST)

`audit-upcoming-early-inputs.json` confirms **12 races / 120 entrants / zero observed labels**. All races have actual field size 10 despite raw card `00`. Market fallbacks are 0.5000/0.5048; current body difference, weather/going and all eight going-dependent fields are null. The suspected `-weight_avg_5` fallback did **not** occur for these entrants.

A separate historical issue was found: native `weight_avg_5` ranges from 3 to 1,507,575. The local DB itself has no ordinary numeric weight >2000 or positive <100. Its Ban-ei weight strings are hexadecimal: **`3E5` = 997 kg**, not scientific notation 300,000. `audit-local-banei-body-encoding.json` finds 15,013 scientific-notation lookalikes; the existing raw-feature helper independently documents hex kilograms and `FFF` missingness. Legacy base conversion does not apply that decoder. Preserve the original baseline and first rich experiment as legacy-parser diagnostics, not proven physical-feature parity.

`learning/body_history_features.py` builds a **separate** `corrected_weight_avg_5` from the previous five classified starts, counting missing-weight starts in the five-start window and excluding every same-day observation via strict as-of matching. Zero/FFF/invalid strings are missing. Original `weight_avg_5` is never overwritten. A separate research metadata contract selects the corrected column; `run-rich-body-ablation.sh` repeats the fixed two-arm, seven-year experiment in `rich-early-body-ablation-v1/`. Deploying such a model would require the same new predictor in serving, not a silent change to existing models.

`replay-early-baseline-inputs.sql` uses the verified no-observed-context values to score the frozen model conservatively. This is a **replay**, not evidence of archived historical 09:00 snapshots, and it retains the frozen model's legacy body input contract. Production artifact/rank parity and all release gates remain unresolved.

## Scientific rationale

See the companion ledger for `docs/journals` citations. Physically correct speed is distance/seconds; age, sex, distance and going are relevant contexts. Next hypothesis: past within-race relative speed can remove race-level context variation. It must still be lagged before the target race, and tested rather than assumed beneficial. The Ban-ei body-conformation paper's PCA 40% is morphometric variance, not explained performance variance.
