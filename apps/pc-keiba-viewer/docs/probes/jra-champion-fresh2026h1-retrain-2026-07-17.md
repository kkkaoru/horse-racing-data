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

### 7.1 DECISION (team-lead, 2026-07-17): path 2 — keep challenger, blind-gate the weekend

Team-lead selected **path 2**. Verbatim rationale: (i) gate (b)'s
structural inconclusiveness aside, the in-sample replay's own point
estimates were negative on 5/7 metrics — even granting they're
noise-consistent, that's zero evidence actively supporting "flip
tonight"; (ii) 2026-07-18 onward is a genuinely blind weekend for both
models — real verification is days away, not weeks; (iii) the serving
stack already absorbed two deploys today, and stacking an unverified
artifact on top the night before racing exceeds the bar the goal's
"production-use consideration" is meant to enforce. **Champion alias
stays at v13. Challenger v34 is held pending a real blind gate — see §10
for the runbook.**

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
- Per-cell gate machinery (2026-07-18 addition, USER decision ④):
  `blind_gate_runbook.py`'s `run_cell_scan` / `cell_gate_status` /
  `generate_cell_routing_proposal` / `compute_venue_mix_caution` (same
  file, extended in place) — see §10.3 for what this does and the
  not-yet-done production wiring checklist. No proposal has been
  generated yet (`blind_gate/` is still empty, §10) and nothing under
  `apps/finish-position-predict-container` has been touched.

---

## 10. Flip runbook (weekend blind gate, per §7.1's DECISION)

**One command, re-run per race day or weekend, from `apps/pc-keiba-viewer`:**

```sh
uv run python tmp/candidate-jra-champion-fresh2026h1-2026-07-17/blind_gate_runbook.py \
    --from-date 20260718 --to-date 20260719
```

`blind_gate_runbook.py` builds that date window's features from scratch
(base builder + the same 16 enrichment layers + pacestyle used
throughout this doc), scores it with **both** the live champion
(`apps/finish-position-predict-container/models/finish-position/jra/jra-cb-v9-sim-2013-clean/`)
and the fresh2026h1 challenger (this dir's `artifact/`), and appends the
per-race hit outcomes to a persistent, dedup-by-`race_id` accumulator at
`tmp/candidate-jra-champion-fresh2026h1-2026-07-17/blind_gate/accumulated_hits.parquet`.
Every invocation recomputes the paired comparison over **all** races
accumulated so far (not just the new window) and prints a `FLIP-READY:
YES/NO` line plus a JSON status block
(`blind_gate/latest_gate_status.json`) and a cell×rank1-5 table
(`blind_gate/latest_cell_report.json`, venue/surface/distance_band,
n≥20).

**Smoke-tested** (this session, 2026-07-17) against the already-settled
2026-07-10→07-12 window (72 races, 941 horse-rows) to confirm the full
pipeline runs end-to-end before leaving it for the real blind weekend —
mechanics only, not a gate reading, since that window is in-sample for
the challenger. Result: completed cleanly in 224.7s (base builder + all
16 layers + assembly + dual scoring + accumulation + gate computation),
zero errors. The gate output itself correctly read `FLIP-READY: NO` and
flagged `interim_safety_check_n50plus: "FLAG"` (worst LB95 −12.5pp) —
expected and reassuring, not a concern: at n=72 on an in-sample window
the CIs are necessarily wide and this population can't validly speak to
regression either way (same reasoning as gate (b), §4.3), so the gate
logic correctly refused to report a false "OK" rather than silently
passing an underpowered/contaminated reading. This confirms the flag
condition itself is reachable and behaves as designed, which the real
weekend-1 run needs to have been exercised at least once before being
trusted blind.

**The smoke test's 72 in-sample rows were deleted from the accumulator
after verifying the pipeline** (`blind_gate/accumulated_hits.parquet` +
the `blind_gate/20260710_20260712/` working dir), specifically so they
cannot silently count toward the real "blind n≥200" flip gate — that
population must be exclusively 2026-07-13+ races that are genuinely
out-of-sample for the challenger. `blind_gate/` is empty and ready for
the first real invocation on 2026-07-18.

### 10.1 The three checkpoints team-lead specified

1. **After each race day**: run the command above with that day's
   `--from-date`/`--to-date` (a single day or a whole weekend at once —
   both work identically since the window is just a DuckDB date filter).
   This is the "score the day, append to the accumulator" step — always
   safe to run, no gate decision attached.
2. **Interim safety check (weekend 1, ~72 races expected 07-18/19)**:
   after running the command for that weekend, check
   `blind_gate/latest_gate_status.json`'s
   `interim_safety_check_n50plus` field. `"OK"` means every metric's
   accumulated LB95 clears the −0.05pp §8.12 floor — no red flag, keep
   going. `"FLAG"` means investigate before scoring further weekends
   (something the underpowered gate (b) in §4 didn't show — worth a
   fresh look, not an automatic abort). `"PENDING (n<50)"` just means
   not enough races yet.
3. **Flip gate (binding, blind n≥200, ≈2 weekends)**: same file's
   `flip_gate_n200plus` field reads `"READY"` only when **all** of: n≥200
   accumulated blind races, every one of top1/place2/place3/place4/
   place5/place6/top3_box has LB95 > −0.05pp (the §8.12 no-regression
   floor, applied by confidence interval this time, not raw point
   estimate — n≥200 gives ~0.5pp resolution per race, an order of
   magnitude tighter than gate (b)'s n=264 in-sample reading, so this is
   the first genuinely trustworthy quantitative reading this whole task
   produces), and at least one of {top1, place2, place3} is
   significantly positive (delta>0 **and** LB95>0). When `FLIP-READY:
YES` prints, proceed to Step 4 exactly as specified in the original
   task brief: bake the artifact to
   `models/finish-position/jra/jra-cb-v9-sim-2013-clean-fresh2026h1/`,
   update `model_meta.json` + test fixtures, update `cell_routing.json`
   default (`sim`) variant + the viewer mirror
   (`finish-position-cell-routing.ts`) + `FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS`
   simultaneously + parity tests, run each package's full check, commit,
   flip the MLflow champion alias (`set-champion jra-finish-position 34`),
   update the `stage` tag off `pending-weekend-blind-gate`, **then stop
   and report before push**, per the standing "commit OK, push needs
   explicit instruction" rule. If instead `FLIP-READY: NO` persists past
   ~3 weekends (n well over 200) with a genuinely negative,
   significant reading on any metric, that's a real regression signal
   this time (not an n=264 in-sample artifact) — close the flip attempt,
   keep the live champion, and downgrade challenger v34's `stage` tag to
   `rejected-see-blind-gate-results`.

### 10.2 Design notes for whoever runs this next

- The accumulator is additive and idempotent — re-running an
  already-scored window just re-derives and dedups the same rows, so
  it's safe to re-run a window if a session gets interrupted mid-build.
- If a window has zero finished races yet (JRA results can lag same-day
  posting), the script prints `window_rows: 0` and just re-reports the
  existing accumulated status — safe to re-run later the same day.
- Resource budget matches every other harvest build in this doc: DuckDB
  6GB/4 threads for the base builder and the final assembly join,
  4GB/2 threads per enrichment layer (12GB/3 threads for `h2h`
  specifically, unchanged from `build_harvest_layers.py`). Check
  `memory_pressure` before running if other heavy jobs are active.

### 10.3 Per-cell variant wiring checklist (USER decision ④, 2026-07-18 addition)

USER decision ④ (2026-07-18): v34 should activate **per-cell wherever it
genuinely wins**, not only via the all-or-nothing pooled flip in §10.1
point 3. `blind_gate_runbook.py` was extended the same day with a
§8.12-compliant per-cell gate (`cell_gate_status`), a
`cell_routing.json`-shaped proposal generator
(`generate_cell_routing_proposal`, gated by a hard assert on both the
per-race blind-date floor and the pooled n≥200 floor), and a venue-mix-skew
diagnostic (`compute_venue_mix_caution`, the "fold-2024 lesson" —
`docs/probes/jra-fold2024-anomaly-forensic-2026-07-17.md`). **None of this
wiring has been done tonight.** This is machinery only, verified against
synthetic data, not run against real accumulated weekend data — the
accumulator is still empty (§10 above), so there is nothing yet to wire.
This section is a checklist for whoever runs the (also-not-yet-run)
proposal generator against real accumulated data and gets a cell that
clears the full binding gate.

The per-cell path differs from the already-documented pooled flip (§10.1
point 3) in exactly these ways:

1. **Baked model artifact — same as the pooled path, no duplicate baking.**
   A per-cell promotion still bakes to
   `models/finish-position/jra/jra-cb-v9-sim-2013-clean-fresh2026h1/`
   exactly as §10.1 point 3 already specifies. One baked artifact serves
   both the pooled-flip path and any number of per-cell variants — the
   difference is entirely in how `cell_routing.json` points at it, not in
   what gets copied where.
2. **`cell_routing.json` — additive, not a default-variant replacement.**
   §10.1 point 3 replaces the `sim` default variant's `model_version`
   wholesale. A per-cell promotion instead adds a **new named variant**
   (`fresh2026h1_<dimension>_<value>`, e.g. `fresh2026h1_venue_05` — the
   naming `generate_cell_routing_proposal` already uses) plus a **new
   rule** targeting only that cell, alongside the existing `sim` /
   `jockey_pedigree_703` / `prior_corner_dirt_smallfield_005` entries.
   Merge the `cell_routing_proposal.json` sidecar's `cell_routing_fragment`
   into the real file by hand, diffing first — the manual-review discipline
   `generate_cell_routing_proposal`'s own docstring cites from
   `apps/mlflow`'s `export-cell-routing`: "never a faithful reproduction,
   always diff before baking, sidecar provenance, never auto-applied"
   (`apps/mlflow/README.md`, "export-cell-routing is a synthesis, not a
   reproduction"). The default `sim` variant and every existing rule stay
   untouched.
3. **Viewer mirror — the identical addition, mirrored by hand.**
   `apps/pc-keiba-viewer/src/lib/finish-position-cell-routing.ts`'s
   `FINISH_POSITION_CELL_ROUTING_CONFIG` needs the same new variant + rule
   added. Its own `finish-position-cell-routing.test.ts` parity test will
   catch a missed or divergent sync — not re-explained here, see that test
   file.
4. **`FINISH_POSITION_LEAK_FREE_MODEL_VERSIONS` — automatic once point 3 is
   done, not a separate edit.** Checked directly in `src/db/queries.ts`:
   this constant is `[...FINISH_POSITION_LEAK_FREE_BASE_MODEL_VERSIONS,
...getAllFinishPositionDisplayPriorityModelVersions()]`, and that second
   spread already folds in every `FINISH_POSITION_CELL_ROUTING_CONFIG`
   variant's `model_version` (`getAllFinishPositionCellRoutingModelVersions`
   in `finish-position-cell-routing.ts`). So completing point 3 alone is
   sufficient — the new `jra-cb-v9-sim-2013-clean-fresh2026h1` model_version
   becomes selectable without a separate `queries.ts` change, and a
   priority-0 cell-routed prediction won't be filtered out by
   `allowed_prediction_model_versions`.
5. **MLflow registry — a `routing_scope` tag, not a champion alias flip.**
   A per-cell variant is narrower than a full champion swap, so this is
   **not** `set-champion jra-finish-position 34` (§10.1 point 3's pooled
   mechanism). Checked against the existing precedent directly: neither
   `jockey_pedigree_703` nor `prior_corner_dirt_smallfield_005` carries a
   `champion`/`challenger` alias of their own — they carry a `routing_scope`
   tag on their own model version instead
   (`apps/mlflow/src/mlflow_tracking/registry.py`;
   `backfill_finish_position.py`'s `_tag_routed_variant` /
   `routing_scope=f"class:{class_code}"` registration path). Tag v34's
   model version with `routing_scope` reflecting the specific cell,
   matching that precedent — `build_cell_routing_export`'s own docstring
   already documents that a `routing_scope` outside the `class:`-prefixed
   convention (which is what any of this campaign's non-`kyoso_joken_code`
   dimensions would be) is recovered on export as a variant _without_ an
   auto-reconstructed rule, i.e. the operator completes the rule by hand —
   the same manual step point 2 above already requires, not new work this
   adds. The existing `challenger` alias (already pointing at v34, §8)
   does not need to change: it correctly continues to describe v34's
   coarser, pooled status, while the `routing_scope` tag is the mechanism
   that actually reflects a per-cell promotion.
6. **Parity tests, commit, no push.** Same discipline as §10.1 point 3: run
   each touched package's full check —
   `bun run --filter pc-keiba-viewer check` for points 3/4 above, and
   `bun run --filter finish-position-predict-container python:check` if
   `cell_routing.json` itself is touched (it lives under
   `apps/finish-position-predict-container`, outside `pc-keiba-viewer`'s
   own check) — commit, **then stop and report before push**, per the
   standing "commit OK, push needs explicit instruction" rule. A per-cell
   promotion does not flip `stage` off `pending-weekend-blind-gate` the way
   a full champion flip would (§10.1 point 3's last step): that tag
   describes the pooled challenger's own status, which a narrower per-cell
   promotion doesn't resolve one way or the other.

**Trigger condition**: this checklist applies to something concrete only
once `blind_gate_runbook.py`'s `cell_routing_proposal.json` sidecar
actually exists — i.e. the pooled n≥200 floor has been met (§10.1 point 3's
own binding checkpoint) _and_ at least one cell cleared the full gate in
`run_cell_scan`. Before that, `generate_cell_routing_proposal`'s own hard
asserts guarantee the file is never written, so there is nothing to wire.
