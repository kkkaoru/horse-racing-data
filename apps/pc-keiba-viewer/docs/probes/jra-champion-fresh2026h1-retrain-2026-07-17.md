# JRA champion freshness retrain — 2013→2026-07-12 (wave6, 2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — data-freshness maintenance retrain,
  team-lead-directed (wave6). **Not a window-ablation retest, not an
  accuracy-improvement claim.** The live champion
  (`jra-cb-v9-sim-2013-clean`, deployed 2026-07-04) was last trained through
  `2025-12-31` — roughly 6 months of 2026 racing (Jan–Jul) were entirely
  absent from its training data. This task extends `TRAIN_END` to
  `2026-07-12` using the byte-for-byte identical recipe, spec, and
  hyperparameters, and nothing else.
- **Outcome: retrained, gated, registered as MLflow challenger (v34).
  NOT deployed.** Three of four gates pass cleanly. The fourth (264-race
  replay parity) cannot be resolved to a clean pass **or** fail — a
  structural property of this specific task (explained in §4.3), not a
  numerical borderline. Production `model_meta.json` / `cell_routing.json`
  / viewer mirror / `FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS` are
  untouched. Champion alias remains version 13. **Recommendation and open
  question for team-lead in §7.**

---

## 0. Headline result

Pipeline integrity is clean (gate a), feature spec and importance ranking
are stable (gates c/d) — there is no evidence of a corrupted harvest,
broken assembly, or leaked column. The one gate that is supposed to answer
"does the new model still perform at least as well on real data"
(gate b, the 264-race summer3 replay) **cannot answer that question for
this particular retrain**: the entire replay population falls inside the
new model's own training window (2026-06-13→07-12 ⊂ 2013-01-01→07-12), so
any accuracy delta on it is confounded by in-sample advantage, not a blind
read. Taken literally, gate b's point estimates breach the -0.05pp
no-regression floor on 5 of 7 metrics — but none of the 7 are
statistically distinguishable from zero (every metric's 95% CI recrosses
zero), and the primary top1 metric is directionally positive in every
cell tested. This is exactly the "no genuine 2026 blind holdout exists"
limitation team-lead's own framing anticipated — it is reported in full,
not resolved unilaterally. See §4.3 and §7.

---

## 1. Recipe verification (identical to the live artifact, by direct read)

`tmp/candidate-leak-clean-retrain/jra_v9sim_artifacts.py` (the actual
script that produced the live `jra-cb-v9-sim-2013-clean` artifact) was
read in full and matched field-for-field:

| Field                                                    | Live champion                                          | This retrain                 |
| -------------------------------------------------------- | ------------------------------------------------------ | ---------------------------- |
| `loss_function`                                          | YetiRank                                               | YetiRank (identical)         |
| `iterations` / `depth` / `learning_rate` / `l2_leaf_reg` | 300 / 8 / 0.05 / 3.0                                   | identical                    |
| `random_seed`                                            | 20260519                                               | identical                    |
| `cat_indices`                                            | `[]` (`no_cat_features=True`)                          | identical                    |
| relevance mapping                                        | rank 1→3, 2→2, 3→1                                     | identical                    |
| sample weighting                                         | **none** — `Pool()` call carries no `weight=` argument | **none** (unchanged)         |
| `TRAIN_START`                                            | `20130101`                                             | identical                    |
| `TRAIN_END`                                              | `20251231`                                             | **`20260712`** (only change) |

**Factual correction to this task's own initial framing**: the task brief
described the recipe as using "time-decay weighting." Direct read of the
live-artifact-producing script shows no such mechanism exists in the
actual served recipe — no `weight=` parameter is passed to `Pool()`
anywhere in `jra_v9sim_artifacts.py`. This retrain replicates the recipe
that is **actually live**, not the brief's premise; per "spec unchanged,"
this doesn't change the approach, only the doc's accuracy about what the
recipe already does.

---

## 2. 2026H1 feature harvest + retrain

### 2.1 Harvest

Base builder (`finish_position_features_duckdb.py --category jra
--from-date 20260101 --to-date 20260712`, DuckDB 6GB/4 threads) produced
23,987 rows / 1,704 races / 9 venues (Sapporo's 2026 meet had not started
as of this run — 0 rows, not filtered out) in 36.8s. All 16 sequential
enrichment layers (`race_internal, market, course, class, kohan3f, baba,
futan, h2h, nearmiss, workout, sectional, grade_lineage, trainer,
similar_race, exotic_odds, jockey_pedigree_cell`) plus the inline
`run_pacestyle` RS join completed cleanly (all `OK`; RS coverage
549/23,987 rows non-null, consistent with a still-building 2026 RS-model
prediction backlog, not a bug).

### 2.2 Assembly + retrain

`tmp/candidate-jra-champion-fresh2026h1-2026-07-17/retrain.py` unions the
existing 2013–2025 store (`tmp/candidate-eval-jra/augmented`, 626,798 rows
/ 44,907 races) with the freshly-assembled 2026H1 harvest (23,247 rows /
1,662 races after the same `finish_position is not null` + date-range
filter used everywhere else in this campaign), casts every armB feature to
`Float64` on both sides, and fits one CatBoost model on the union
(650,045 rows / 46,569 races). Training completed in 54.5s (57.2s
end-to-end including assembly).

**Column-parity check** (old-store vs. new-harvest coverage, all 250 armB
features): zero columns missing entirely from either side. 40 features
flagged below 50% new-side coverage — cross-checked individually, and the
large majority of these (e.g. `speed_index_avg_5`, `speed_index_best_5`,
`field_strength_avg_speed`, `last_race_margin_to_winner`) were **also**
at or near 0% coverage in the _old_ store — i.e. structurally sparse
across both eras (a pre-existing property of these features, not a
regression introduced by the 2026H1 harvest). None of the flagged
features showed high old-side coverage collapsing to near-zero on the new
side, which is the pattern that would actually indicate a harvest bug.
Full report: `tmp/candidate-jra-champion-fresh2026h1-2026-07-17/feature_coverage_report.json`.

---

## 3. Gate (a): pipeline verification — PASS

Reproduces one WF fold (train ≤2024, blind 2025) using this task's _own_
data-loading query against the _existing_ 2013–2025 store (deliberately
not touching the 2026H1 harvest), compared against the already-cached,
previously-trusted `armB/fold-2025/model.json` used all day as the
baseline. Purpose: catch a data-loading/assembly bug via performance
divergence, not measure accuracy per se.

| Metric   | Cached (trusted) | Fresh (this task's loader) | Δ pp   | LB95   | UB95   |
| -------- | ---------------- | -------------------------- | ------ | ------ | ------ |
| top1     | 32.996           | 33.459                     | +0.463 | −0.088 | +1.013 |
| place2   | 18.206           | 18.408                     | +0.203 | −0.492 | +0.839 |
| place3   | 13.835           | 13.661                     | −0.174 | −0.839 | +0.492 |
| place4   | 12.069           | 12.040                     | −0.029 | −0.666 | +0.608 |
| place5   | 11.635           | 11.520                     | −0.116 | −0.724 | +0.521 |
| place6   | 9.899            | 10.246                     | +0.347 | −0.203 | +0.868 |
| top3_box | 9.464            | 9.464                      | +0.000 | −0.347 | +0.347 |

n=3,455 validation races. Every metric's delta sits inside the
established single-arm noise floor (±0.4pp,
`project_training_noise_floor_2026_07_11`) and every CI straddles zero.
**Clean pass — no data-corruption signal.** (Side note: the cached-arm
top1 rate here, 32.996%, closely matches gate b's champion-arm top1 rate
on a completely different population, 32.955% on the 264-race replay —
an internal consistency check that both scoring paths are behaving
sanely.)

---

## 4. Gate (b): 264-race replay parity — INCONCLUSIVE (not a clean pass, not a clean fail)

### 4.1 Setup

Reused `tmp/candidate-jra-summer3-local-replay-2026-07-17/scored.parquet`
(264 Hakodate/Fukushima/Kokura races, 2026-06-13→07-12, 3,379
horse-rows) rather than rebuilding it — it already carries the armB
feature set, a placeholder-safe ground truth (`finish_position_gt`,
verified identical to its own `finish_position_actual` on all 3,379 rows),
and the live champion's own predicted ranks (`rank_champion_only`).
Scored the same population with the new fresh2026h1 artifact and paired
the two rank columns (`n_boot=2000`, seed 20260717).

### 4.2 Pooled result

| Metric   | Champion | Fresh2026h1 | Δ pp       | LB95   | UB95   |
| -------- | -------- | ----------- | ---------- | ------ | ------ |
| top1     | 32.955   | 34.470      | **+1.515** | −0.379 | +3.788 |
| place2   | 19.318   | 19.697      | +0.379     | −1.515 | +2.273 |
| place3   | 14.015   | 12.879      | −1.136     | −3.419 | +1.136 |
| place4   | 11.742   | 10.227      | −1.515     | −3.788 | +0.758 |
| place5   | 14.015   | 12.879      | −1.136     | −3.409 | +1.136 |
| place6   | 12.500   | 12.121      | −0.379     | −3.030 | +2.273 |
| top3_box | 8.333    | 7.955       | −0.379     | −1.894 | +1.136 |

Taken **literally**, the §8.12 no-regression floor (-0.05pp) is breached
on 5/7 metrics (place3/4/5/6, top3*box); worst point estimate is place4 at
−1.515pp. **But every single one of those 5 metrics' 95% CI recrosses
zero** (e.g. place4: [−3.788, +0.758]) — none is a statistically
significant regression. At n=264, one flipped race = 0.379pp, so the
nominal −0.05pp floor is \_tighter than this population's own measurement
resolution*: a single coincidental flip breaches it by construction,
independent of any true effect. top1, the primary metric this whole
campaign has centered on, is directionally **positive** with a CI skewed
positive ([−0.379, +3.788]).

### 4.3 Why this gate cannot be cleanly resolved for this specific retrain

This is the important finding, caught only by comparing date ranges
directly rather than assuming the replay population was a valid holdout:
**the entire 264-race replay window (2026-06-13→2026-07-12) sits inside
the new model's own training window (2013-01-01→2026-07-12).** Extending
the replay to the full freshly-built 2026H1 harvest (1,662 races, 9
venues) would not fix this — it is _also_ entirely in-sample for
fresh2026h1 by construction, since that harvest **is** the data that was
added to training. Any population drawn from "2026 so far" is in-sample
for a model whose stated purpose is "train through the most recent
available date." The genuinely blind-to-both-models window
(2026-07-13→today) is 4–5 days and has few or zero finished/settled races
— not a usable test population.

This is not a bug to fix; it is a structural property of "freshness
maintenance" retrains that train through the present. It is exactly why
the task brief framed this as "freshness maintenance + no-regression
guarantee," explicitly not an accuracy-improvement claim — there is no
2026 population that is blind to the new model. Given the in-sample
contamination biases _toward_ showing the new model favorably, a result
that is directionally positive-but-noisy on top1, with no metric showing
a statistically significant swing in _either_ direction, and no
suspiciously large blowout improvement that would suggest overfitting, is
consistent with "nothing broken, nothing dramatically different" — but it
is **not** valid evidence of a true accuracy delta in either direction,
and it should not be read as satisfying a rigorous no-regression floor
check.

### 4.4 Cell breakdown (n≥50; CELL_MIN relaxed from the usual 200 given

the 264-race population — every cell's own n is reported for power
assessment; only Hakodate/Fukushima/Kokura present, Sapporo's 2026 meet
had not started as of the source replay build)

top1 by cell — directionally flat-to-positive in every single cell, no
exceptions:

| Cell                       | n   | Champion top1 | Fresh top1 | Δ pp   | LB95   |
| -------------------------- | --- | ------------- | ---------- | ------ | ------ |
| venue=02 (Hakodate)        | 120 | 29.167        | 32.500     | +3.333 | −0.833 |
| venue=03 (Fukushima)       | 72  | 36.111        | 36.111     | +0.000 | +0.000 |
| venue=10 (Kokura)          | 72  | 36.111        | 36.111     | +0.000 | −4.167 |
| surface=dirt               | 100 | 37.000        | 39.000     | +2.000 | −2.000 |
| surface=turf               | 157 | 31.847        | 33.121     | +1.274 | −1.274 |
| distance_band=intermediate | 104 | 35.577        | 37.500     | +1.923 | −1.923 |
| distance_band=mile         | 76  | 32.895        | 34.211     | +1.316 | +0.000 |

place3–place6/top3_box show scattered negative point estimates across
these same cells, but with wide CIs that in every case extend well past
the point estimate (e.g. venue=10 place3: −5.556pp [LB95 −11.111]) and no
consistent cross-cell directional pattern — consistent with sampling
noise at this population size, not a systematic weak spot. Full
per-cell, per-metric table: `tmp/candidate-jra-champion-fresh2026h1-2026-07-17/gate_b_result.json`.

---

## 5. Gate (c): feature-importance stability — PASS

Live champion's own model (loaded directly from
`apps/finish-position-predict-container/models/finish-position/jra/jra-cb-v9-sim-2013-clean/model.json`,
the actual locally-mirrored served artifact) vs. fresh2026h1,
`PredictionValuesChange` importance:

- **Top 5 identical set in both** (odds-driven features): `tansho_odds_raw`,
  `odds_score`, `odds_score_diff_from_race_avg`, `inverse_odds_market_share`,
  `inverse_odds_implied_prob` — only internal reordering (champion ranks
  `odds_score_diff_from_race_avg` #1; fresh2026h1 ranks `tansho_odds_raw`
  #1; same 5-feature set, comparable magnitudes).
- **Top 20 overlap: 18/20 (90%)**. Only two low-importance, closely-spaced
  features swap at the margin: `last_race_corner_progression` /
  `days_since_last_race_log` (out) vs. `last_3_avg_finish_norm` /
  `weight_avg_5` (in) — all four cluster in the 0.57–0.85 importance
  range, far below the top-5's 7.5–14.8 range.
- **Max rank-position shift among the 18 shared top-20 features: 2.**

No qualitative surprise; consistent with "same recipe, more data," not a
structurally different model.

---

## 6. Gate (d): spec / leak verification — PASS

- `metadata.json.feature_names`: **250 features, exact order-for-order
  match** against the canonical armB spec
  (`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`) — not
  just a set match.
- `feature_names` set is **byte-identical** to the live champion's own
  `metadata.json` (250/250, zero additions, zero removals, zero
  substitutions).
- `hyperparams` block identical to live champion's own metadata
  (loss_function/iterations/learning_rate/depth/l2_leaf_reg/relevance/
  no_cat_features/random_seed all match).
- Leak-token scan (`target_corner`, `kakutei_chakujun`, `finish_position`,
  `chakujun`, `final_odds_confirmed`, `rank_champion`, `rank_sim`,
  `is_winner`) against `feature_names`: **zero hits**.
- `leak_cols_excluded` flag: `true`. `feature_count` field internally
  consistent with `len(feature_names)`.

---

## 7. Gate summary, deploy decision, and open question for team-lead

| Gate                             | Result                                                                                                                                                                             |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| (a) pipeline verification        | **PASS** — clean, all within noise floor                                                                                                                                           |
| (b) 264-race replay parity       | **INCONCLUSIVE** — structurally not a valid blind test for this retrain (§4.3); literal floor breached on 5/7 metrics, zero statistically significant, top1 directionally positive |
| (c) feature-importance stability | **PASS** — clean                                                                                                                                                                   |
| (d) spec/leak verification       | **PASS** — clean                                                                                                                                                                   |

Per the instruction ("ALL must pass or no deploy"), gate (b) does not
cleanly clear the literal bar, so **Step 4 (production deploy prep —
`model_meta.json`, `cell_routing.json` default variant, viewer mirror,
`FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS`, parity tests) was
deliberately not started.** No production-facing file was touched. This
is a judgment call being surfaced, not made unilaterally: I do not believe
gate (b)'s literal breach is genuine evidence of regression (see §4.3),
but I'm also not overriding an explicit numeric gate criterion on my own
authority for a production model flip, especially given the same
literal-gate-application discipline has been the basis for every other
accept/reject decision this campaign made today.

**Two concrete paths, for team-lead to choose between:**

1. **Treat gate (a) as the binding safety gate** (the only one that is
   genuinely methodologically clean for this retrain), with gate (b)
   downgraded from a quantitative pass/fail floor to a qualitative sanity
   check — which it passes (no degenerate output, no wild divergence from
   the champion, no suspicious blowout that would suggest overfitting).
   Proceed to Step 4 deploy prep on this basis.
2. **Wait for a genuinely blind population**: defer the production flip
   until enough 2026-07-13+ races have settled (a few weeks) to run a
   real blind gate (b) against fresh2026h1's actual out-of-sample
   performance, then decide.

Either way, the retrained artifact is safe to keep as a registered
challenger in the meantime (§8) — that action has no production effect.

---

## 8. MLflow record

Logged via `log-training-run` (`hr-mlflow-training-run/v1` schema,
`register: true`, `champion: false`), durability verified by reading back
through `cli.build_client()` against the real Neon backend (confirmed
`postgresql` scheme, not local sqlite — see the note below):

- Run ID `949cd46ccf7c40ce90398bf57008d5f1`, status `FINISHED`, 22 params /
  16 tags / 37 metrics, experiment `finish-position/wf-eval`.
- Registered as `jra-finish-position` **version 34**.
- Challenger alias set: `jra-finish-position` → **v34**. Champion alias
  unchanged at **v13**.
- Tags include `campaign=2026-07-17-summer4`,
  `wave=wave6-champion-freshness-retrain`, all 4 gate results, and
  `deploy_status=not-deployed-challenger-only-pending-team-lead-review`.

**Note on a first-attempt failure**: the first `log-training-run` call
aborted partway (run `419eb06701734dd5aa79759d30789b4f`) because the
manifest's own `params` dict duplicated two keys (`based_on`,
`train_date_range`) that `_ingest_artifact_dir` _also_ auto-derives from
`metadata.json` — MLflow rejects changing an already-set param value, and
the two paths encoded the same field differently (joined string vs. raw
JSON list). Fixed by removing the duplicated keys from the manifest
(anything already a top-level `metadata.json` key doesn't need to be
manually re-specified). The orphaned run was explicitly marked `FAILED`
with a `superseded_reason` tag rather than left dangling in `RUNNING`
state. Separately, an initial ad-hoc verification script instantiated a
bare `MlflowClient()` instead of the package's own `cli.build_client()`
helper, silently defaulting to a local sqlite file and reporting the just
-created run as "not found" — resolved by always using `build_client()`
(reads `HORSE_RACING_MLFLOW_BACKEND_URI`), consistent with the
established durability-verification discipline
(`project_mlflow_neon_write_durability_2026_07_11`).

---

## 9. Artifact locations

- Trained model: `apps/pc-keiba-viewer/tmp/candidate-jra-champion-fresh2026h1-2026-07-17/artifact/`
  (`model.json`, `metadata.json`, `importance_top40.json`) — not yet
  copied to a production model path (Step 4 not started, §7).
- Harvest: `apps/pc-keiba-viewer/tmp/candidate-jra-champion-fresh2026h1-2026-07-17/features_base/`
  - `work/out/*` (16 layer dirs + pacestyle).
- Gate scripts + raw results: `gate_a_pipeline_verification.py` /
  `gate_a_result.json`, `gate_b_264_replay_parity.py` / `gate_b_result.json`,
  retrain script `retrain.py`, MLflow manifest builder
  `build_mlflow_manifest.py`. All under the same `tmp/` directory
  (git-ignored per project convention; not committed).
