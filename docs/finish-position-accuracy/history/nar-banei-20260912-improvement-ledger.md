# NAR / Ban-ei 2026-09-12 improvement ledger

## Final decision — 08:30 JST

**Accuracy goal not achieved; retain existing production models and predictions.** `final-decision.json` records the failed frozen nominees, evidence, quality checks and remaining gaps. Five adaptation families and70 cell/year reports are complete; final2026 selection remains closed. No deployment, regeneration or commit was made.

Final replay checks passed25 tests/100% coverage, Ruff/format and viewer typing. Full viewer:5,266 passed,97.39%,27 existing runtime warnings. Full TimesFM remains172 passed/97.28%, whole-package typing clean. Both5,178-row prediction frames are exactly unchanged by the final row-conversion cleanup (`audit-pandas-row-conversion.json`). Repository ignore audit found0 ignored research artifact paths.

Captured-input replay reproduces120/120 published ranks and all12 sample standard deviations within1.263e-15, but this is not an accuracy gain. Local feature parity, complete native NAR reconstruction and deployed-image attestation remain incomplete. Rendered heatmap validation is blocked on normal Cloudflare Access authentication; no bypass or authentication change was attempted.

## Earlier checkpoint — 08:28 JST

- Browser navigation reached Cloudflare Access sign-in, not the race UI. Jockey/trainer/pedigree heatmaps and candidate before/after rendering remain **unverified**. No authentication attempt, Access change or regeneration request was made. Signed login query parameters were removed from retained navigation/snapshot artifacts; `browser-baseline-audit.json` records the blocker.
- Fixed-model, local-only dataframe sensitivity replays completed on the same547 races/5,178 classified finishers. Pandas early replay exact1–5 rates are[13.711,11.335,11.335,11.517,12.066]%; retrospective rates[36.746,21.207,17.185,14.442,13.894]%. These are not complete prerace cohorts, have no Prophet postprocessing, and do not establish production gains. `pandas-baseline-replay-plan.json` explicitly forbids using them to retune/reselect the closed TimesFM families. Old outputs are preserved.
- A final row-conversion cleanup removes a nested comprehension while preserving pandas missingness and metadata order. Its added test initially assumed string NULL stayed None; direct inspection showed this installed pandas string dtype yields NaN in both `to_dict(records)` and `itertuples`. The expectation was corrected to observed pandas semantics, not the production coercion changed. The failure log is retained. Full regression tests and exact comparison against both saved prediction frames are running before closure.

## Earlier checkpoint — 08:18 JST

- The full-card sim-policy replay also reproduces published **sample standard deviations**: maximum absolute difference across12 races is `1.2628786905111156e-15`. All120 ranks match. This remains captured-input inference reproduction, not local-feature parity, individual raw-score attestation, image-byte attestation, or improved accuracy.
- Canonical observation audit:4,235,400 local-PG export rows become3,730,290 physical-race/horse observations after505,110 cross-source duplicate pairs. There are14 pairs with differing positive finish labels, no conflicting positive clocks or field sizes, no source-priority ties, and no prefix/category priority inconsistencies. Existing saved V2 observations choose the declared native NVD value in all14 cases;0 mismatches. Original training records and completed studies are unchanged. See `audit-canonical-observations.json` for both source values and saved performance.
- The export spans1993-10-16 to2026-09-11, with0 observations on/after target day2026-09-12. Earliest overall data are JRA history, not evidence of complete older NAR coverage; source/venue ranges are reported separately. This physical-key audit is broader than the earlier rich-cohort union audit, whose zero-conflict statement must not be generalized to all source copies.
- Retained quality logs use `.txt` (`quality-refresh-0806.txt`, `serving-card-and-timesfm-types-0808.txt`) because repository `*.log` files are ignored. The retained logs and captured parquets are Git-eligible; no ignore rules were weakened.
- Read-only browser validation of the existing heatmap UI is being attempted; no rendered result has yet been verified and no candidate before/after comparison exists.

## Earlier checkpoint — 08:09 JST

- **Captured serving-input replay now matches all120 published ranks across all12 Ban-ei races** using the unchanged native model, `pandas.read_parquet(...).to_dict(records)` and the existing sim Prophet policy. The alternative base policy matches101/120; it is not selected per race. This is inference reproduction, not an accuracy improvement or candidate promotion. See `audit-serving-card.json` and retained read-only per-race caches.
- Important correction to earlier projection assumptions: production first loads parquet through pandas (`pipeline_runner.py:1279`, `_group_parquet_rows`). Numeric NULLs consequently reach the scorer as NaN, not None/zero. The eight numeric all-null Baba columns added after the day-base snapshot therefore matter. Polars/JSON-direct replay omitted this ingestion step. R1 captured raw pandas replay matches8/10; adding existing Prophet reaches10/10. Local PG raw-sim pandas ordering still differs from captured input, so local feature parity remains incomplete.
- Existing `score_frozen_catboost` defaults are retained for reproducibility, but now offers explicit `--frame-loader pandas`, documents the distinction, and reports its loader and absence of Prophet postprocessing. No old evaluation artifacts are overwritten. Earlier Polars frozen results remain research replays, **not validated full production baselines**. New numeric float/integer missingness tests and CLI coverage pass24/24 with100% coverage.
- R1 final-cache timestamp2026-09-11T00:15:48.442Z is approximately7 seconds after its prediction00:15:41.465Z; day-base source was written00:11:26.630Z. This ordering is consistent with asynchronous cache upload but is not cryptographic vintage/image attestation. All downloaded cache rows remain inference-audit-only, never training observations.
- Refreshed full viewer checks:5,265 tests pass,97.39% coverage,27 existing runtime warnings; viewer typing0. Full TimesFM:172 tests pass,97.28%; whole-package `basedpyright --project pyproject.toml src tests` now0 errors/warnings. Initial unscoped TimesFM typing scanned inherited viewer paths under the wrong package environment and reported249 errors; that invocation is retained, not suppressed. Initial viewer Ruff invocation failed because Ruff is not installed there; configured tool execution succeeded. Changed replay source/tests pass Ruff and formatting.
- Published score dispersion uses **sample** standard deviation (`prediction-kv-writer.ts`, denominator n−1), whereas the first diagnostic displayed population standard deviation. A follow-up report records both and verifies numerical differences; no score mathematics are being changed.
- Frozen final candidate decisions remain failed/closed. No production edits/deployment/regeneration, no new accuracy claims, and no rendered candidate heatmap verification.

## Earlier checkpoint — 07:45 JST

- Five TimesFM/Rustuna adaptation families completed. The development-only shortlist was frozen in `final-family-selection-v1.json` before TimesFM2026 readout review: V2 for55/2YO and V5 for83/joken-000. Other eight research controls remain unchanged. Neither nominated candidate passed the subsequent2026 audit; no production change or regeneration is justified.
- 2026 selected comparisons:83/joken-000,33 races/3 dates, baseline exact hits[9,6,3,5,5] versus candidate[10,4,3,6,5];55/2YO,3 races/3 dates, both[0,1,1,0,1]. Intervals are withheld below20 dates. Broader experiments already examined2026, so this is not a pristine campaign-wide holdout. No retuning from these final results.
- `timesfm-all-cell-year-summary-v1.json` now covers all70 cell/year combinations, preserving zero supports. In2026,54/3YO/extended_sprint,54/B/mile and55/A/mile have zero evaluation races. Each metric's all-race denominator and observed exact-finish supports are separately recorded. Development selected-family displays are not nested family-selection evidence.
- Obtained existing CF R1 day-base foundation via read-only Wrangler after the generic API adapter failed on HTTP200. These external serving inputs are **inference-audit-only and must never become training observations**. Original local versus captured matrices differ in295/1,300 values. Captured-input replay matches6/10 published ranks rather than1/10; model/source vintage and final late binding remain unattested.
- Identified a local similarity-path source mismatch: current target rows are120 in raw `nvd_se` but0 in `race_entry_corner_features`. The local CLI's derived-table path therefore produced zero current jockey/trainer/owner similarity statistics. Reusing the existing production raw-table SQL against local PG recovers120 target-entity rows and reproduces the captured foundation's entire R1 raw order10/10. Matrix equality rises to1,108/1,300; remaining historical aggregates differ. Published final-order agreement is still6/10, not serving parity.
- Source mathematics and production files were not changed for that probe; `replay-raw-similarity.sh` is an isolated research orchestrator. Native legacy body inputs remain unchanged. Earlier focused-versus-whole-card differences were only six floating-point values at most1.11e-16.
- Post-score Prophet sensitivity does not close the gap: earlier local sim-policy replay leaves all120 ranks unchanged; captured-input sim/base replays both remain6/10 on R1. Final per-race serving cache/provenance inspection is next.
- Latest focused checks:56 TimesFM tests, approximately99% combined coverage, exact/query cores100%; preparation17 tests100%; owned typing clean. The last full viewer run had5,258 passes,97.39% and27 existing warnings, but predates V4/V5. Full TimesFM typing has two unrelated concurrent `rustuna_optimization.py` unnecessary casts, not silently suppressed. Full checks need refreshing.
- No deployment, selective production regeneration or rendered before/after heatmap verification. Native NAR reconstruction and remaining source-provenance audits are incomplete.

## Earlier checkpoint — 06:55 JST

- No production changes, selective regeneration or rendered heatmap verification. Serving parity remains failed; research-control gains are not production gains.
- TimesFM-3 V1, V2 and V3 development evaluations are complete. Reused the existing official offline JRA forecaster and Rustuna API, with separate exact-position metrics and 70 twenty-year expanded cell/year scope manifests. All observations originate from local PG. Personal noncommercial production validation is explicitly authorized by the user; license acceptance/disclosures remain required.
- V2 adds completed-past-day speed normalization and magnitude-preserving performance readouts. Venue55/2YO improves four point metrics with the third unchanged: [+8.140,+4.651,0,+6.977,+3.488]pp on 86 races/65 dates. Every 99% date-cluster interval includes zero. Other cells remain mixed.
- V3 adds forecast-minus-last innovations to each horse's baseline. All ten cells still have at least one negative exact-position delta. Last-origin innovations are an explicit unchanged control. Each V2/V3 search uses 1,000 trials per eligible cell/year/origin; development years only. No 2026 readout metrics were inspected to design these adaptations; complete search multiplicity is not corrected.
- V3 focused quality: 43 tests passed, exact/query modules 100% statement/branch coverage; combined new modules approximately99%, typing zero errors/warnings. The full 5,238-test run below predates these additions and must be repeated.
- Focused race83:01 and whole-card reconstructions have identical ranks (10/10), but either agrees with production on only1/10. Across the original full card:13/120 ranks,3/12 winners,zero complete orders match. Matrix comparison finds1,294/1,300 identical inputs; six differences affect two pedigree aggregates, so rank agreement does not prove feature equality. Magnitude inspection is pending.
- Production code also applies a frozen Prophet post-score correction, omitted by the booster-only replay. An independent local-PG join of its existing lookup is prepared; final adjusted parity remains to be tested. The older local production worktree lacks the present policy/model paths, so it cannot attest the deployed image.
- Detailed outputs: `experiments/20260912-local-pg/timesfm-paired-development-v{1,2,3}/`, `audit-single-race-features.json`, and `timesfm-rustuna-plan.md`. Native NAR missing feature layers, final2026 evaluation, provenance and full quality checks remain open.

## Earlier checkpoint — 04:40 JST

- **No production modifications or regeneration.** Full current Python suite: 5,238 passed, 27 existing warnings, 97.60% coverage; root basedpyright zero errors.
- Original 42 compact fits complete. Rich early-card controls/candidates add native context and retain all 333,761 required labelled pre-2026 observations, including 22,089 recovered historical venue-81/82/84 observations. Retired extension statistics remain explicitly unavailable; no fabricated full-context claim.
- Local upcoming reconstruction: 12 Ban-ei races / 120 entrants, zero observed labels, actual field size 10. Missing market inputs map to medians .5000/.5048; 11 other early-unavailable native inputs are null.
- **Body parsing defect:** `3E5` is hexadecimal 997 kg, not exponent-form 300,000. 15,013 lookalikes found in local PG. New causal previous-five-classified-start `corrected_weight_avg_5` is a separate feature, preserving old model inputs. Corrected means range 665–1,252 kg; decoder/history tests 100% statement/branch coverage. Both 117- and 135-feature model recipes are tested chronologically; no blanket gains across every rank established.
- A no-observed-context frozen-model **replay** scores Top1 12.61% on 547 races / 5,178 finishers, not the 36.93% retrospective-context result. Neither result establishes actual early-serving quality without production parity. The body-corrected 117-feature candidate scores [30.53,17.92,17.00,12.80,10.97]% on that same cohort, but rank4/rank5 intervals still cross zero. Keep promotion disabled.
- Read-only production KV snapshot captured for all 120 Ban-ei entrants. Exact local-versus-production rank agreement is under audit; this is not training data or rendered heatmap verification.
- NAR fallback baseline reconstruction and refreshed 2026 scoring are now being prepared. Fine-cell scope manifests/evaluations for all ten target cells, full serving parity and rendered jockey/trainer/pedigree checks remain unfinished.

## Requested window and release contract

- Target: every cell represented on the 2026-09-12 NAR/Ban-ei card; stop at 09:00 JST.
- Evaluation years: 2020–2026, with 2026 outcomes strictly before prediction time.
- NAR evaluation identity must include venue. Evaluation routing and training membership are separate contracts.
- For each cell, retain a configurable 20-year seed-race window; expand through the seed entrants to all their historical races, including full competitor groups, not just the selected horses. Retain the evaluation race universe in the scope manifest but never put evaluation outcomes into a training fold.
- Historical fold membership and all features must be reconstructed as of each fold cutoff. Today's entrant information must not leak into historical model-selection folds.
- Only promote candidates with matched-race, out-of-time improvement against the current production recipe. Repeated searches require an untouched final holdout; report sample sizes, effect sizes and paired race/date-block uncertainty, not only a best point estimate.
- Promote only the improved routing cells and regenerate only their scheduled races. Preserve rollback model/routing identities.
- Before and after every release, check rendered race-detail jockey, trainer and pedigree win-rate heatmaps. API success alone does not establish visible UI correctness.

## Initial audit (from 01:02 JST)

- No model trained, selected, deployed or regenerated by this session yet.
- Initial multiline `/loop` was not parsed as an extension command (installed pi splits the command name at a space, not a newline). The subsequent explicit self-paced task activated the loop; wakeups now succeed. Deadline remains 09:00 JST.
- Working tree already contains many unrelated edits. Do not revert, overwrite or bundle those changes into a release.
- Existing adoption module: `apps/pc-keiba-viewer/src/scripts/learning/build_cell_models.py`. It provides cell identities, model provenance and multi-metric adoption gates; the architecture defaults are NAR XGBoost and Ban-ei CatBoost.
- Existing research and features: `apps/pc-keiba-viewer/tmp/candidate-prerace-weather-nar-banei-2026-08-24/`. These are historical research artifacts, not evidence of improvement on the requested current card.
- Race-detail decoder: `apps/pc-keiba-viewer/src/lib/race-time.ts`, `parseEncodedRaceTimeTenths`. It accepts trimmed 1–4 ASCII digits, pads to `MSSd`, rejects zero and seconds >=60, and returns `600*M + 10*SS + d` (tenths). Divide by ten only when the downstream unit is seconds. Raw integer division by ten is not valid across minute boundaries.
- Potential audit item, not yet a confirmed serving defect: `build-relationship-history-sql.ts` casts `history.soha_time` to numeric before speed aggregates. Trace the upstream source's units before changing it.

## Local implementation checkpoint (01:09 JST)

- Added `learning/race_time.py`: scalar and DuckDB raw MSSd-to-seconds decoders, without changing existing model feature values yet.
- Added `learning/cell_training_scope.py`: configurable seed horizon, mandatory NAR venue, pre-cutoff seed-horse expansion to all older races and full competitor rows, separate fold evaluation rows and union race manifest.
- Both new modules are covered by the existing `--cov=learning` package measurement.
- Focused verification: 39 tests passed, 100% statement/branch coverage on both modules; basedpyright reported zero errors and warnings.
- Final verification after formatting (01:12 JST): Ruff format/check, ty and basedpyright passed for all four added Python files. Full package `uv run pytest -n 4`: 5,134 passed, 27 warnings from `test_feature_explorer.py`, 97.52% total coverage, above the unchanged 95% threshold. Both new modules remain at 100%.
- A concrete raw conversion mismatch exists in `add-ban-ei-raw-features.py`: `1234 / 10 = 123.4`, whereas the detail decoder returns 83.4 seconds. It has not yet been changed or connected to new training.
- Public target-card request returned HTTP 401. Opening the target card in the Chrome integration redirected to Cloudflare Access login. User login is required before rendered production validation; access controls were not bypassed.
- Read-only local PostgreSQL card lookup failed with connection refused at localhost:5432. A working authorized source connection or complete current-card export is needed before target-cell enumeration and data completeness validation.
- No accuracy improvement, training completion, production promotion or rendered heatmap verification has been claimed.

## Data connection and causal-feature checkpoint (01:46 JST)

- User explicitly requires **local PostgreSQL for all training data**. Executor Cloudflare is reserved for production state, release operations and direct production verification.
- PostgreSQL is healthy in Apple Container `horse-racing-local-postgresql`. The host localhost:5432 listener is missing, but `container exec ... psql` works. No restart, replacement or data repair was needed.
- Local target card: Kochi 54 = 12 races / 126 entrants; Saga 55 = 10 / 102; Ban-ei 83 = 12 / 120. Total 34 races / 348 entrants. Raw `shusso_tosu` is `00` on this upcoming card: use actual current entry counts, not zero, for field-size features.
- Experiment artifacts: `/private/tmp/horse-nar-banei-0912/`. `target-card-local-pg.csv` preserves the raw card. `export-local-history.sql` is read-only and expands target-venue seed horses through ALL prior JRA/NAR races and full competitor groups.
- Initial export: 4,004,171 runner rows, 656,406,601 CSV bytes. JRA 1,445,684 rows (1993-10-16 through 2026-08-30); NAR 2,558,487 rows (2005-01-01 through 2026-09-08). Both CSV and Parquet were generated from local PG, not an external catalog.
- The broad seed envelope was subsequently widened to 2000-01-01 to cover 20-year seeds for the earliest 2020 validation fold. Each fold still applies its own cutoff and horizon. Regeneration is in progress; the initial counts above are not the final widened-envelope counts.
- Local NAR source history begins in 2005. Thus a full 20-year history is not available for the earliest evaluation folds; report this source truncation rather than claiming completeness. Features for target venues currently end September 5–6; latest raw-source versus derived-table completeness still requires reconciliation.
- Added `learning/history_features.py`: race-detail-equivalent seconds, past-only normalized speed, previous-year speed, career form and rider/trainer win rates. All windows end the previous day. Current clock is retained only as an audit column and is excluded from the explicit model predictor allowlist. Focused 42 tests passed at 100% coverage before adding the ablation harness.
- Added `learning/history_ablation.py`: fixed-budget QuerySoftMax, speed versus no-speed arms, no evaluation-set early stopping, explicit safe predictor columns including venue, dated split guard, exact rank1–5 metrics. This is a diagnostic model comparison, **not** the current-production adoption gate.

## Persisted experiments and production audit (02:32 JST)

- User requires no new Git-excluded scratch files. Completed models, predictions, scope IDs, SQL, card and cell metadata moved to `docs/finish-position-accuracy/experiments/20260912-local-pg/`; reproducible CLI and shell runners are under the viewer's `src/scripts/learning/` and `scripts/`. These paths are Git-eligible, not yet claimed committed. Existing large scratch inputs are read-only; new outputs go directly into the repository.
- Final widened original envelope: 4,178,907 rows. Raw-priority NAR 2026 refresh: 4,235,400 rows total, zero duplicate race/horse keys. The derived table lacked recent confirmed Ban-ei labels; raw local PostgreSQL restored September 5–7. Source extraction remains local-PG-only.
- Frozen September confirmation, 36 races / 350 finishers: speed/no-speed Top1 22.22%/27.78%, Top2 25.00%/16.67%, Top3 8.33%/13.89%, Top4 5.56%/13.89%, Top5 8.33%/13.89%. Reject the speed-only candidate. This interval is now consumed evidence, not a fresh holdout for later hypotheses.
- A fixed Top5 YetiRank NDCG objective with `max(6-finish,0)` relevance was predeclared and tested on Ban-ei 2024/2025. Same-feature paired comparisons do not justify replacing the winner objective: 2025 speed-arm Top1 delta -2.17 percentage points, approximate five-rank-adjusted date-bootstrap interval [-4.34,-0.06]. No production adoption.
- New `paired_rank_evaluation.py` requires exactly matching entrants/dates/labels and valid rank permutations. It reports short-field support and 10,000 date-cluster bootstrap draws, abstains with fewer than 20 dates, and uses 0.005/0.995 percentile tails across five ranks. These approximate intervals do not correct across the wider candidate/cell search. Related 13 tests passed, statement/branch coverage 100%, Ruff and basedpyright clean. Objective/CLI tests: 16 passed, 100% coverage.
- Direct Cloudflare KV audit found all 34 target races / 348 entries with finite normalized ranks and no read errors. **Actual NAR predictions use `iter12-nar-xgb-hpo-v8-stage1-marketfree-184`**, not the documented default blend. Ban-ei uses `banei-cb-v9-sim-2011`. Saved bounded API request/response: `cloudflare-target-summary.*.json`. The earlier full response was truncated and is not a complete snapshot.
- September KV enumeration retained only September 10–12 keys; it supplied no September 5–7 baseline. Local PG's prediction table also returned no target-venue September 1–11 records. Current model artifacts exist locally; their feature and training-cutoff provenance must be established before historical comparison.
- Ban-ei production metadata declares 130 predictors and training through 2026-05-18. It cannot be honestly evaluated as out-of-time on earlier years without refitting chronological production-recipe baselines. Post-May evaluation still requires canonical feature parity and local-PG feature provenance.
- No production writes, deployment, selective regeneration or rendered heatmap verification has occurred. Existing prediction `winProbability` fields are not evidence about separate jockey/trainer/pedigree heatmap availability.

## Scientific hypotheses and limits

- `docs/journals/papers/26_1506-age-racing-speed-thoroughbred-jra.md`: the abstract defines speed as distance in metres divided by final time in seconds; age/sex/distance/season stratification motivates a nonlinear age/context ranker and physically valid speed features. This is descriptive JRA evidence, not proof of a NAR or Ban-ei accuracy gain.
- `docs/journals/papers/12_1_1-body-size-conformation-racing-performance-banei.md`: the abstract and GLM tables support testing body-size/age context in Ban-ei. Its local prose incorrectly equates the first PCA component's ~40% **morphometric** variance with performance variance; do not use that numerical performance claim. Body weight is not present in the current extracted historical feature set and has not yet been tested.
- Statistical design: fix tree budget before examining each yearly outcome set; compare arms on identical races; subsequently require independent current-production comparison and an untouched confirmation interval before any release. Historical held-out outcomes must not choose iteration count or define training horse membership.

## Next steps

1. Complete remaining Kochi/Saga yearly ablations and fine-cell scope/uncertainty reports.
2. Establish local-PG canonical features for actual production models; compare only on genuinely out-of-time races, refitting the production recipe for older yearly folds when necessary.
3. Test context-relative historical speed/recency hypotheses with preserved rejected outcomes and explicit validation-consumption accounting.
4. Rerun the full package quality gates after the new CLI and evaluation changes.
5. Only promote matched-production improvements with all-rank evidence, serving parity, selective regeneration and heatmap verification; otherwise leave production unchanged.

## Evidence still required

Still missing: complete 20-year NAR source history for early folds, fine-cell manifests, decoder training/serving parity for any promoted model, matched historical current-production baseline, adequate unconsumed confirmation for adoption, and rendered production heatmap evidence. Target-card counts, current prediction model identities, diagnostic holdout results and cited mechanism limits are now recorded above.
