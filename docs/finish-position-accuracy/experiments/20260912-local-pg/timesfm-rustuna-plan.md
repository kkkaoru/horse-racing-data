# TimesFM / Rustuna cell adaptation plan

## User direction

Rustuna is the efficient hyperparameter-search engine, not a forecasting model or an accuracy ablation. A failed TimesFM configuration does not reject TimesFM for a cell. Continue testing different context lengths, normalization, predicted quantities and readouts; adopt demonstrated cell-level improvements only after the existing release gates.

## Reuse and important differences

- Reuse `apps/timesfm-finish-position/.../forecasting.py`'s pinned official TimesFM-3 evaluator and existing installed Rustuna 0.1.0. Do not create a new Rust crate.
- The JRA campaign files have become available in the main working tree. Preserve their concurrent changes.
- Its `rustuna_optimization.evaluate_surface` measures **winner recall within the first K predictions**. This campaign requires **exact position K**, K=1..5. Implement a separate exact-position evaluator, rather than silently changing JRA's metric contract.
- Its `build_entrant_history_scope` selects evaluation entrants. This campaign retains the separately configured twenty-year **seed-race** scope plus complete earlier horse races and competitor groups, using `learning.cell_training_scope`.
- JRA's market readout is not an appropriate early-card NAR/Ban-ei baseline. Use explicitly named, local-PG chronological model controls. The failed local-versus-production parity audit remains a release blocker, not proof of poor production accuracy.

## Initial hypothesis family and continued adaptation

1. Horse-level performance forecasts from strictly previous-day race history.
2. Multivariate performance plus correctly decoded, within-race relative physical speed.
3. Full available horse context versus an explicit short-context readout ablation; training-scope membership itself is never reduced.
4. Compare frozen pre-year contexts with rolling previous-day contexts. The latter may consume already observed earlier-year races, never same-day or future results.
5. Rustuna tunes readout/component weights, baseline mixing and sparse-history fallback using only earlier evaluation years. Retain an unchanged baseline candidate, record every trial, reject non-finite/invalid results, and report all five exact-rank supports and deltas separately.
6. If unsuccessful, continue with interval-aware contexts, body-history forecasts for validated Ban-ei hex kilograms, residual forecasts and cell/action-gain temporal controllers. Do not label a single failed profile as a universal rejection.

Cache expensive forecasts once; reuse them across Rustuna trials. Persist configs, scope manifests, forecast inputs' provenance, predictions, trial databases and reports in Git-eligible repository paths. Do not recreate old ignored scratch outputs. Horse-racing observations come exclusively from local PG or the already retained local-PG exports.

## Honest evaluation and license boundaries

Chronological outer years are 2020–2026. Selection uses earlier years; cold-start years use predeclared defaults, not their own labels. Repeated-search uncertainty and missing historical source coverage must remain explicit. Current baseline artifacts mostly score classified finishers; this does not establish complete pre-race serving-cohort parity.

The existing package identifies the pinned `google/timesfm-3.0-pytorch` weights as non-commercial. The user explicitly clarified that the deployed system is used only by them for personal verification. Treat this campaign, including any authorized deployment, as non-commercial personal validation; preserve the research/license disclosures. Commercial use remains unauthorized and would require a compatible license or separate rights. License availability is therefore not a blocker for the stated personal validation use; accuracy, serving parity and heatmap gates still apply.

## Fifth adaptation: recency-conditioned mixing (declared after V4 development)

V4 failed the all-five criterion in both Ban-ei cells. Before any 2026 readout review, test decay of the temporal mixing weight by elapsed days since the query's actual latest observation. This is recency-conditioned mixing, not a calendar-time TimesFM forecast. Use half-lives0(no decay),14,28,56,112,224 days; unknown histories have zero temporal influence. Reject known histories with missing, same-day or future timestamps. In particular, frozen-January profiles must use their cached latest observation, not a later serving-date history.

Reuse the six immutable V2 profiles across all ten cells, 1,000 trials per origin/eligible year and all three readout normalizations. Precompute decay arrays for efficient Rustuna search. Preserve no-decay defaults and original formula; do not change checkpoint, contexts, scope, cohorts or old artifacts. Evaluate2020–2025 before final2026. Larger hypothesis search and repeated development inspection remain limitations.

## Fourth adaptation: observed Ban-ei body histories (declared 07:05 JST)

V3 development remains mixed in all ten cells. Before inspecting 2026 readout outcomes, add two Ban-ei-only profiles: log decoded body kilograms, and performance plus log kilograms. Decode the retained local-PG raw-body export with the existing tested hexadecimal decoder (`3E5` =997kg); never substitute the lagged `corrected_weight_avg_5` for an actual observation. Join on canonical physical race/horse keys, reject duplicates, retain missing measurements and all original observations. Use log(kg/1000), a fixed unit shift so neutral zero imputation means1000kg rather than1kg; this is not a claim that1000kg is optimal. Past-only query construction and per-component observed counts remain mandatory.

Generate a separate V4 observation/cache namespace, with raw-source hashes. Preserve the six existing profiles as the CLI defaults; body profiles require explicit selection. Evaluate the two Ban-ei cells with the two new profiles, 1,000 trials per eligible year/origin and rank/centered/innovation readouts, 2020–2025 first. Larger forecast body mass and positive relative changes are hypotheses, not universal health/fitness claims. The usual baseline, persistence and mean5 controls remain. No new source observations, commercial authorization or serving-parity claim are implied.

## Third adaptation: forecast innovations (declared 06:50 JST)

V2 development aggregation completed: venue55/2YO has deltas [+8.140,+4.651,0,+6.977,+3.488]pp on 86 races/65 dates, but every 99% interval includes zero; other cells retain mixed rank changes. No production adoption is justified. No 2026 readout results were inspected for this decision.

Test an `innovation` readout using the existing immutable V2 forecasts. Instead of replacing incumbent ability with forecast levels, add forecast-minus-last-observation changes to each horse's baseline. Center performance innovations across eligible entrants; use centered average-tie CDF innovations for speed. Missing anchors or insufficient observed histories must fall back, never manufacture improvement. The last-value origin necessarily becomes the unchanged baseline under this transformation; mean5-minus-last supplies a regression-to-mean control. This is an adaptation of forecast use, not a newly trained checkpoint or proof of causal fitness changes.

Run innovation-only, 1,000 trials per origin/cell/year, years2020–2025 first. Keep original V1/V2 artifacts immutable; reuse the six V2 cache profiles and unchanged local-PG targets/scopes. Search budget is matched, but hypothesis families differ. All-five development feasibility, forward-year evaluation and uncertainty caveats remain mandatory. Evaluate 2026 only after development selection.

## Second adaptation family, declared after development-only review

The first four profiles and 300-trial searches completed. Review is restricted to 2021–2025; the generated 2026 readout accuracy has not been used to choose this extension. No cell has demonstrated improvement of all five exact ranks against its unchanged research control in that development comparison. This does not reject TimesFM as a whole.

Two concrete limitations motivate the next tests:

- Within-race speed standardization discards differences in race strength. Add `day_speed`: log physical speed centered over the completed historical venue/date/exact-distance/track-code cohort. Historical cross-race pace differences remain; grass and dirt are not mixed. It is an observed-history quantity and is never exposed for the prediction date itself. This is not claimed to remove every track-condition effect.
- Empirical rank-CDF readouts discard forecast spacing. Add a `centered` performance readout that preserves numeric forecast differences while matching the available horses' mean baseline score. Keep the original rank readout as a selectable control.

Add frozen-checkpoint profiles `day-speed-full` and `performance-day-speed-full`. Recompute all six profiles into a separate v2 cache, leaving v1 immutable, so every profile records actual observation counts separately for each component. A component filled entirely from a neutral value must not be reported as observed history. Retain matched last-value and five-start-mean controls and equal 1,000-trial Rustuna budgets across origins (v1 used 300; any between-version change is not solely attributable to normalization). Choose normalizations only on earlier years. Evaluate the second family on development years before examining final 2026 comparisons; development intervals remain exploratory and search multiplicity is not fully corrected.
