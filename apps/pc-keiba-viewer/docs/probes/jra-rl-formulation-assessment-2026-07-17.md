# JRA Finish-Position: Reinforcement-Learning Formulation Assessment (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position, design-first research probe (formulation
  survey, not a feature-engineering lever). Sibling to this session's
  vector-search angle on the same USER ask (handled by a different agent;
  see `jra-vector-knn-retrieval-2026-07-17.md` in this directory).
- **USER goal** (relayed via campaign orchestrator): JRA conditions A-D
  (venue x class x distance x meet-day / barrier / running-style / jockey
  win-rate / pedigree axes) should be examined not just via ordinary feature
  engineering but also via **reinforcement-learning** framings. Nobody had
  picked up the RL angle before this probe.
- **Condition A-D definitions** (confirmed directly against
  `docs/finish-position-prediction-system.md` §11 and the four cited probe
  docs — every citation below was opened and read, not taken on faith):
  - **A = 開催日目** (meeting-day / which day of a multi-day meet).
  - **B = 枠** (barrier / post-position draw, bundled with
    running-style/corner-passage in the one probe that tested it).
  - **C = 騎手勝率** (jockey win-rate).
  - **D = 血統系統** (pedigree lineage / sire-damsire).
  - §11 (`docs/finish-position-prediction-system.md:1267-1269`) literally
    labels these three past lever families **条件A+B**, **条件C**, and
    **条件D** in the JRA summer-venue campaign summary, which matches the
    task brief's mapping exactly. One precision worth noting: the probe
    docs' own "USER condition" header text for C reads "競馬場×class×距離×
    開催日数×騎手の勝率" (venue x class x distance x meeting-day x jockey
    win-rate) — i.e. the _label_ bundles meeting-day-count together with
    jockey win-rate as a compound condition, since conditions were
    originally specified as venue x class x distance x **the marginal
    axis under test**. The task brief's shorthand ("C = jockey win-rate")
    is the correct one-axis summary of what was actually engineered and
    tested under that label (`jockey_venue_dist_win_eb` etc., plus one
    `jockey_meetphase_win_eb` sub-column) — this is a labeling nuance, not
    a citation error.
  - A+B were tested together as a `meetingday×waku` interaction feature
    (`jra-meetingday-waku-clean-2026-07-04.md`, task #3), C as a
    jockey-winrate interaction (`jra-jockey-winrate-clean-2026-07-04.md`,
    task #4), D as a pedigree shrunk win-rate
    (`jra-pedigree-winrate-clean-2026-07-04.md`, task #5) — **all three
    REJECTED 2026-07-04**, verified below. D was later fully closed via a
    generalized seasonal-delta follow-up
    (`summer-oddsfree-pedigree-2026-07-04.md`, task #34, also REJECT).

This doc does not re-run any of that feature-engineering work. It asks a
different question: **is there a genuinely-distinct reinforcement-learning
formulation of this problem that hasn't already been tried under a
different name?** Three candidate framings were enumerated. Two close
without a fresh empirical test (a structural/mathematical argument for one,
an information-dominance argument for the other); the third is the one
candidate that survives dedup and gets a minimal, bounded, single-fold
empirical spot check.

## Dedup precedents verified

Every citation below was opened and read directly (not inferred from a
title or a summary) before being used in this doc's arguments.

| #   | Precedent                                      | What it actually is                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | Verified against |
| --- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| 1   | E-top2 place-preserving override               | Hard rank-swap rule: promote XGBoost's #1 pick to rank-1 **only** when it equals CatBoost's #2, leaving CatBoost's #3 fixed at rank-3 by construction. A static, deterministic, non-learned re-ranking rule — not a learned sequential policy of any kind. JRA version retired when sim-v9 replaced the underlying CatBoost (`project_etop2_place_preserving_win_2026_06_18` memory, confirmed "sim v9でretired" in `index_closed_probes.md`'s superseded index). NAR per-class routing variant separately retired from production after a base-selection artifact was found (`project_nar_etop2_perclass_routing_2026_06_19` memory: "訂正(2026-07-02): 上記ADOPTはbase-selection artifactだったことが判明...NAR E-top2はDO-NOT-RETEST", confirmed "per-class routing本番退役" in the index).                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| 2   | meet-momentum                                  | `jra-meet-momentum-2026-07-04.md` — verified its own header labels this **"USER condition: C"** and explicitly frames itself as testing a **"DYNAMIC within-meet momentum"** hypothesis, contrasted against the static career-rate construction. Candidate columns confirmed verbatim: `jockey_meet_momentum` / `jockey_meet_rides` / `jockey_meet_win_delta` / `trainer_meet_momentum` — a jockey's/trainer's very-recent same-meet form expressed as a delta from a career EB baseline, fed as a **static additive feature** into the same 250-feature CatBoost. Verdict confirmed verbatim: "The pooled positive drift on all 3 primaries...never reaches LB95>0...driven almost entirely by a single fold (2024, which alone clears the gate on all 3 primaries) and is not stable across seeds (top1 flips sign on seed101)." REJECT, DO-NOT-RETEST.                                                                                                                                                                                                                                                                                                                                                                                   |
| 3   | per-cell routing REJECT                        | `project_jra_rs_cell_routing_reject_2026_07_03` memory, confirmed: JRA **running-style** (a different prediction target than finish-position) per-cell model routing was tested and REJECTED — "fine cell key...最細cell keyでvalid race_count>=200に到達するcellは0/304" — the fresh serve-representative global model beat per-cell specialists at every aggregation level tried. **This is categorically distinct from `cell_routing.json`**, the finish-position model's own data-driven routing mechanism (`docs/finish-position-prediction-system.md` §6.3, verified directly) — that mechanism is **live in production today** (e.g. Ban-ei `grade_code==E` → base variant; JRA class-703 / dirt-smallfield-005 / venue-02 routing per §6.3's own "現行live例" list), gate-based and gated by gate `build_cell_models.py` accept criteria — not rejected, not related to this precedent beyond sharing the word "cell."                                                                                                                                                                                                                                                                                                              |
| 4   | confidence-shrinkage                           | `apps/pc-keiba-viewer/tmp/confidence-shrinkage/summary.json` (+ `gate_a_report.json` / `gate_b_report.json`), verified directly. Mechanism confirmed verbatim: `"shrink #1 pick's score toward within-race mean by factor k...score'_1 = mean_r + k*(score_1 - mean_r); all other horses' scores untouched; re-rank; applied ONLY in gated races."` Two gate definitions: gate_a = venue×grade (Sapporo/Hakodate × tokubetsu E-grade), gate_b = inner-draw pockets (wakuban<=2 at Sapporo/Fukushima/Kokura). Both verdicts confirmed verbatim: `"REJECT_no_k_clears_selection"`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 5   | score-level/calibration overlay lane closure   | `index_closed_probes.md` memory, confirmed **verbatim, word for word**: `"policy: score-level/calibration overlay lane は campaign 全体でクローズ — 同一60日窓の再スライスでの再開禁止、真に新しい falsifiable lead のみ"` — closing E-grade shrinkage, inner-waku overlay, and isotonic/monotone recalibration as a family, from a specific 60-day rolling-window construction.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| 6   | label reweighting                              | Cited only indirectly, exactly as instructed — no separately-findable doc exists for it. `jra-volatility-tiered-fusion-2026-07-11.md`'s own dedup paragraph was opened and confirmed to contain, verbatim: `"confirmed against the session's prior REJECTs: confidence-shrinkage, upset-gate cascade, label reweighting, odds-blind override — none of those blend two model scores conditioned on a continuous tier"`. It is cited here the same indirect way — no doc path is fabricated for it.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| 7   | **sameday_bias** (same-day dynamic track bias) | The single most load-bearing precedent for candidate (b) below, verified with extra care. `jra-masked-lever-clean-retest-2026-07-04.md`, "Lever #2" section (`docs/probes/jra-masked-lever-clean-retest-2026-07-04.md:75-140` in this repo's `apps/pc-keiba-viewer` copy). Candidate columns confirmed: `sameday_inside_bias` / `sameday_front_bias` / `sameday_prior_race_count`, built leak-free from strictly-earlier same-day races (`race_bango < current`, own race never included), residualized against a TRAIN-only per-venue baseline recomputed per WF fold. Fed as a static additive feature to the full 250-feature champion GBDT, full 3-seed x 3-fold WF, clean/leak-masked-confirmed (this was one of the levers re-tested specifically to rule out the `target_corner` leak having masked a real effect — it hadn't). Result confirmed verbatim from the doc's own table: pooled top1 **+0.119pp [LB95 -0.064]**, `primaries_passed=0/3`, `place3` and deeper ranks actually regress (worst `-0.241pp`, place5) → **REJECT**. Cross-checked against `apps/pc-keiba-viewer/tmp/candidate-masked-lever-retest/reports/sameday_bias.json` (exists on disk, 23,533 bytes) referenced by the doc as the authoritative artifact. |

One path-notation note for completeness: the task brief cited these files as
bare `docs/probes/...`; they in fact live at
`apps/pc-keiba-viewer/docs/probes/...` (this document's own directory). The
file contents match exactly as described in every case above — this is a
path-prefix shorthand, not a wrong citation.

## (a) Contextual bandit / offline RL for a single race's pick — NOT-DISTINCT, closed by proof sketch, no empirical test needed

Casting "predict this race's outcome" as a contextual bandit or single-step
offline RL problem means: state = pre-race features, action = a pick (or a
full ranking), reward = a hit indicator, and a policy π(a|x) to be learned
from logged historical data. This looks superficially like a new framing,
but it collapses to ordinary supervised ranking under one critical fact:
**horse racing is a full-information feedback problem, not a
bandit-feedback problem.**

A true bandit only reveals the reward of the action actually taken — an
ad-serving system learns whether the ad it showed was clicked, never what
would have happened with a different ad. Horse racing does not have this
property at all: every race reveals the complete finishing order of every
horse in the field, regardless of what the model predicted. The reward of
every _possible_ action (every possible pick, every possible full ranking)
is therefore directly computable from the single observed outcome, with no
missing counterfactuals. Offline contextual-bandit/RL machinery —
importance-weighting, inverse propensity scoring, doubly-robust estimators,
exploration-exploitation tradeoffs — exists specifically to correct for the
case where only the taken action's reward is observed and the logging
policy's action distribution has to be accounted for. None of that
machinery does anything useful when the reward is already fully known for
every action.

Under full information, the RL objective `max_π E[R(x, a=π(x))]` reduces
pointwise to `argmax_a R(x,a)`, which is exactly the ordinary
structured-prediction / learning-to-rank objective the champion's CatBoost
YetiRank loss already directly targets. This is precisely why the broader
ML literature does not reach for bandit/RL formulations in full-information
structured-prediction settings (ranking, translation, classification) — RL's
comparative advantage is specifically in partial-feedback or genuinely
sequential-credit-assignment settings, and a single race's win/place/show
outcome is neither partial-feedback nor sequential.

**Verdict: mathematically NOT-DISTINCT from the existing supervised ranking
model.** This is closed the same way isotonic/monotone recalibration was
permanently closed in this campaign (`index_closed_probes.md`'s
score-level/calibration-overlay policy line, precedent #5 above) — a
structural/mathematical argument, not an empirical one. No WF slot
warranted.

## (b) Sequential within-meet dynamic trust/calibration — dominated by existing evidence, not worth testing

This is the one candidate that maps onto condition A (開催日目) as a
literal RL time-step: state = meet-day progress, and a policy adjusts
per-cell trust or calibration for day t+1 based on results observed through
day t. Unlike (a), this candidate **is** mechanistically distinct in
principle from a static feature — a policy with explicit state/action/reward
updated online within a meet is a different computational object than a
fixed column fed once into an offline-trained GBDT.

But mechanism-distinctness is not the same as expected value. The
underlying information such a policy would need to exploit — "how has this
meet/day been going so far" — has already been tested **twice**,
independently, at two different granularities, through a channel that is
**strictly more expressive** than any constrained trust-adjustment policy
could realistically offer, and **both came back null**:

- **`sameday_bias`** (precedent #7 above, track-level, same-day): pooled
  top1 **+0.119pp [LB95 -0.064]**, 0/3 primaries, flat-to-negative on
  deeper ranks.
- **`jra-meet-momentum`** (precedent #2 above, jockey/trainer-level,
  within-meet): positive drift on all 3 primaries never reaches LB95>0,
  driven by a single fold (2024), sign-unstable across seeds (top1 flips
  sign on seed101).

Both fed the **same kind of signal** — recent within-meet realized outcomes
— into a 300-tree, depth-8, 250-feature CatBoost that is free to interact
that signal with everything else the model already knows. This is a strict
superset of what a low-dimensional bandit/RL trust-adjustment policy
(necessarily a much narrower function class, since such a policy exists
specifically to be simple, robust, and cheaply updatable online) could
extract from the identical information source. A constrained policy cannot
systematically out-extract signal from a variable that a more expressive,
more flexible learner already searched and found approximately zero signal
in.

This is an **information-dominance argument, not a mathematical identity**
— that distinction is stated here honestly, unlike candidate (a). This is
not a proof that RL provably cannot help; it is that the specific
informational premise this framing depends on has already been tested
twice and found empirically null, making a fresh test a low-expected-value
repeat of ground already covered by more-expressive prior art.

One more caution is worth naming explicitly: any "adjust calibration/trust
based on recent results" framing is adjacent to the score-level/calibration
overlay lane this campaign has already closed as a policy (precedent #5).
The time-granularity differs — within-meet, over days, not the closed
lane's 60-day rolling window — so this is **not literally the same
construction**, and this doc does not claim it is. But it inherits the same
general pattern (a trust/confidence adjustment layered on top of an
existing score, conditioned on recent context) that this campaign has
repeatedly found unproductive, and that pattern-level caution is a
legitimate reason for hesitation, not a closure argument on its own.

**Verdict: NOT WORTH TESTING given existing dominated evidence** — no WF
slot warranted. This is a judgment call under uncertainty, not a closed
proof, and is stated as such rather than overstated as a mathematical
closure.

## (c) Policy-gradient (REINFORCE) direct optimization of the exact §7.2 gate metrics — the ONE candidate that survives dedup, minimal empirical test warranted

### Why this is genuinely structurally distinct

Unlike (a) and (b), no prior REJECT in this entire campaign has touched the
**core training algorithm / loss function** of the ranking model itself.
Every prior lever in this campaign's history was about features, post-hoc
score combination/fusion (precedents #1, #4, #6, plus the meta-learner and
volatility-fusion campaigns), or offline routing (precedent #3, plus the
live `cell_routing.json` mechanism). None of them touched how the base
ranker's own parameters get optimized. A candidate that replaces or
augments the optimization algorithm itself is a genuinely new axis this
campaign has not yet closed.

### Why it is still very unlikely to help — stated honestly in both directions

LambdaRank-family losses, which CatBoost's YetiRank belongs to, are already
an **analytically exact form of policy gradient** for ranking metrics by
construction. The "lambda" gradient trick computes, in closed form, exactly
how a non-differentiable target ranking metric would change if a pair of
items were swapped — precisely the quantity a REINFORCE / score-function
estimator would otherwise have to approximate via noisy Monte Carlo
sampling. Because horse racing is full-information (candidate (a)'s
argument applies here too — the reward for every possible action is exactly
computable from the one observed outcome, no exploration required), a
stochastic policy-gradient estimator has no informational advantage: it can
only ever be a higher-variance, noisier way of arriving at a gradient
signal the existing analytical lambda-gradient already computes exactly.

This predicts a REINFORCE-tuned reranking should **not** beat the existing
champion score, and may plausibly be slightly worse given limited training
budget and gradient variance. But this is a claim about expected
optimization behavior, not a mathematical impossibility the way (a) is —
different local optima, or an unexpected inductive-bias effect, are
possible in practice. That is exactly why "measure, don't assume" applies
here and a minimal spot-check is worth the (small) cost, rather than
closing this candidate by argument alone the way (a) and (b) were closed.

## Minimal empirical test (candidate c)

### Design

Built under `apps/pc-keiba-viewer/tmp/ms-rl-formulation/` (new directory,
script `reinforce_rescale_wf.py`). No CatBoost model was retrained; the
cached champion "base" models were reused verbatim from
`tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`,
feature list = `armB` (250 features) from
`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`. Helper
functions (`load_store`, `predict_raw`, `zscore_within_race`,
`rank_from_score`, `per_race_hits`, `paired`, `gate`, `avg_hits`) were
copied byte-identical in logic from `tmp/ms-contender-meta/contender_meta_wf.py`
(itself copied from `tmp/ms-volatility-fusion/family_boost_train_and_fuse.py`),
same constants (`METRICS`, `PRIMARIES`, `GATE_MIN_DELTA=0.08`,
`GATE_NO_REG=-0.05`, `N_BOOT=2000`, `BOOT_SEED=20260519`). Only `z_base`,
`finish_position`, `race_id`, and the fold split were loaded from the
250-feature store — no `boost_raw`, no `volatility_score`, no cell
dimensions.

**Policy**: `score(w1, w2) = w1 * z_base + w2 * z_base^2` — 2 learnable
parameters only, a bounded proxy test of whether policy-gradient
optimization on top of the existing champion score does anything, not a
from-scratch policy-gradient GBDT. Per-race policy is a categorical/softmax
distribution over the field: `pi(horse i) = softmax(score_i)`.

**Reward**: `relevance_labels(df, 3, 2, 1)` value of the sampled horse (3 if
it actually finished 1st, 2 if 2nd, 1 if 3rd, 0 otherwise) — reused directly
from `finish_position_catboost.py` exactly as every precedent script does.
This ties the REINFORCE reward to the identical target the champion's own
YetiRank loss already uses, making this an apples-to-apples comparison of
optimization algorithm, not objective.

**Training**: for `seed_base in {42, 101, 2026}` independently, REINFORCE
with a moving-average baseline (EMA decay 0.99) fit on pooled `fold_year in
{2023, 2024}` OOS rows for that seed (one direct blind split — the task
spec explicitly does not require the 3-arm cross-fit design used by the
meta-learner precedent for this minimal, single-fold probe), starting from
`(w1=1.0, w2=0.0)` — the identity, i.e. exactly the champion's current
behavior. 30 epochs, race-level minibatches of 128 races, Adam optimizer
(lr=0.01) doing gradient **ascent** on expected reward. Final `(w1, w2)`
saved per seed.

**Evaluation**: the fitted `(w1, w2)` applied deterministically (no
sampling) to `fold_year==2025` rows, ranked by `score` descending, compared
via `paired()`/`gate()` against the baseline (`w1=1, w2=0`, i.e. plain
`z_base` — exactly what the champion already does) on all 8 metrics,
seed-averaged via `avg_hits()`.

Free memory was checked before the DuckDB store load
(`memory_pressure -Q` → 59-61% free, well above the 15% floor); DuckDB was
run with `memory_limit='6GB'`, `threads=4`. The REINFORCE training itself
processes ~94,000 horse-rows / ~6,910 races per seed in pure NumPy and
completed in under 5 seconds total for all 3 seeds — no memory check was
needed for that step per the task's own scoping.

### Sanity check: is the policy-gradient loop actually learning?

Per-epoch mean sampled reward for seed=42 (all 3 seeds are qualitatively
identical; full 30-epoch tables for all 3 seeds are saved in the JSON
artifact):

| Epoch | Mean reward | Std reward | w1    | w2    |
| ----- | ----------- | ---------- | ----- | ----- |
| 0     | 1.108       | 1.248      | 1.492 | 0.489 |
| 5     | 1.456       | 1.275      | 2.461 | 1.393 |
| 10    | 1.502       | 1.273      | 2.975 | 1.790 |
| 15    | 1.514       | 1.277      | 3.408 | 2.060 |
| 20    | 1.504       | 1.272      | 3.693 | 2.255 |
| 25    | 1.515       | 1.274      | 3.976 | 2.428 |
| 29    | 1.512       | 1.269      | 4.233 | 2.535 |

The trajectory is not flat-lined at a buggy constant (e.g. exactly 0, or a
fixed base rate with zero variance): mean sampled reward rises from 1.108
at epoch 0 to a plateau around 1.50-1.53 by epoch ~10-15, and both weights
move substantially and monotonically away from the `(1,0)` starting point
across all 30 epochs and all 3 seeds. This confirms the optimizer loop is
genuinely updating, not silently broken.

As a second, **deterministic** (zero-variance, no RNG) confirmation
independent of the noisy per-epoch sampling trajectory: the exact expected
reward under the softmax policy, `E[reward] = sum_i softmax(score)_i *
reward_i` averaged per race, was computed directly on the 2025 held-out set
for both the initial `(1,0)` policy and each seed's final fitted policy:

| Seed | E[reward \| init (1,0)] | E[reward \| fitted] | Delta   |
| ---- | ----------------------- | ------------------- | ------- |
| 42   | 0.8960                  | 1.5002              | +0.6042 |
| 101  | 0.8968                  | 1.5036              | +0.6069 |
| 2026 | 0.8962                  | 1.5026              | +0.6064 |

All three seeds show a genuine, substantial (~68% relative) increase in the
policy's own training objective on held-out data — unambiguous confirmation
that REINFORCE successfully optimized what it was asked to optimize. This
sets up the central empirical finding below: **the policy-gradient loop
worked exactly as intended, and that success still did not translate into a
better deterministic ranking.**

### Fitted `(w1, w2)` per seed

| Seed | w1     | w2     | Final EMA baseline | Fit races (2023+2024 pooled) |
| ---- | ------ | ------ | ------------------ | ---------------------------- |
| 42   | 4.2334 | 2.5350 | 1.505              | 6,910                        |
| 101  | 4.2484 | 2.6667 | 1.605              | 6,910                        |
| 2026 | 4.2970 | 2.5561 | 1.552              | 6,910                        |

All three seeds land far from `(1, 0)` — the policy gradient found plenty
to move toward (see "mechanism" note below for why), so this is not a
case of the optimizer trivially declining to move.

### Blind 2025 eval: fitted policy score vs baseline (`w1=1, w2=0`, seed-averaged, n=3,455 races)

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.140 | 33.140 | +0.000     | +0.000 |
| place2     | 18.360 | 18.273 | -0.087     | -0.193 |
| place3     | 13.768 | 13.632 | -0.135     | -0.299 |
| place4     | 12.234 | 12.031 | -0.203     | -0.598 |
| place5     | 11.394 | 10.121 | -1.274     | -1.939 |
| place6     | 10.005 | 9.108  | -0.897     | -1.698 |
| top3_box   | 9.426  | 9.320  | -0.106     | -0.241 |
| fukusho_2p | 74.771 | 74.636 | -0.135     | -0.251 |

**Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-1.274`
(place5, far outside the `-0.05` no-reg bound) → `ACCEPT_strict_gate=false`.
REJECT.**

### Interpretation

The result is not a simple "flat null" — it has real structure worth
naming precisely rather than glossing over:

- **top1 is _exactly_ unchanged** (`33.1404` vs `33.1404`, delta and LB95
  both round to `0.0000`). This was verified independently (not just
  trusted from the aggregate report) by directly comparing, race-by-race,
  the argmax-by-`z_base` horse against the argmax-by-fitted-score horse
  across all 3,455 seed-42 races: **exactly 1 race** out of 3,455 has a
  different argmax. In that one race, the champion's own top pick (a weak,
  low-confidence pick at `z=0.95`, close to the fitted parabola's vertex at
  `z≈-0.83`) finished 7th, and the parabola-preferred replacement (an
  extreme outlier at `z=-2.77`) finished 12th — both misses, so the one
  argmax flip changes nothing about the top1 hit/miss outcome. This
  confirms the exact-zero delta is a real, mechanistically-explained
  property of the fitted policy on this data, not a bug silently no-op'ing
  the ranking.
- **place2/place3 are mildly negative** and breach the `-0.05pp` no-reg
  floor but are small in magnitude — consistent with the theoretical
  prediction that REINFORCE finds nothing exploitable near the top of the
  ranking, where the champion's own YetiRank lambda-gradient already
  optimizes exactly.
- **place4-6 regress meaningfully** (place5 `-1.27pp`, place6 `-0.90pp`,
  both with LB95 comfortably negative). This has a clean mechanistic
  explanation, not just a numeric one: `relevance_labels(3,2,1)` assigns
  reward **0** to every finish position outside the top 3. REINFORCE
  therefore receives **zero gradient signal** about how to order the
  bottom of the field — nothing in the training objective penalizes
  scrambling place4-6. Meanwhile, the fitted `score(w1,w2) = w1*z +
w2*z^2` is a convex parabola with both `w1` and `w2` positive and a
  vertex at `z≈-0.83`: for `z` below the vertex (moderately-to-very
  below-average horses), the parabola's ordering is **partially inverted**
  relative to the champion's own linear ranking, since more-negative `z`
  produces a _higher_ score once the quadratic term dominates. This is
  visible directly in the deterministic diagnostic above (the one argmax
  flip involved exactly this mechanism) and explains why the damage
  concentrates in the undersupervised deep-rank metrics while leaving the
  supervised top-of-field metrics essentially untouched.
- **Why did REINFORCE push `w2` positive at all**, given this cost? A
  softmax policy with no entropy regularization has a well-known
  degenerate incentive: enlarging the parameters' overall magnitude
  sharpens (de-flattens) the sampling distribution, concentrating
  probability mass on the single highest-scoring horse and thereby raising
  _expected sampled reward_ under full information, where the model
  already knows which horse is most likely to be correct. The deterministic
  expected-reward check above confirms this directly — the policy's own
  objective rose by ~0.60 (a ~68% relative increase) purely by growing
  `(w1, w2)`, with the `z^2` term riding along as a comparatively cheap
  way to add sharpness rather than because it discovered any independently
  useful ranking information. This is a textbook REINFORCE
  variance-collapse dynamic, not evidence of a novel discovered signal.

This is exactly the outcome candidate (c)'s own theory section predicted:
no exploitable gain on the metrics the champion's YetiRank loss already
optimizes exactly, plus a clean, mechanistically-explained cost on the
metrics REINFORCE's reward construction leaves unsupervised. Per the task
scope, this result is reported as-is and not chased further: no additional
parameters, no additional folds, no reward-shaping iteration. A different
reward construction that also supervises place4-6 (e.g.
`relevance_labels` extended to rank 6) would be a **different candidate**,
not a retest of this one, and is explicitly out of scope here.

## Verdict

- **(a) Contextual bandit / offline RL for a single race's pick**:
  **NOT-DISTINCT, mathematically closed.** Full-information feedback
  collapses the RL objective pointwise to the existing supervised ranking
  objective. No empirical test warranted, same closure category as
  isotonic/monotone recalibration.
- **(b) Sequential within-meet dynamic trust/calibration**: **not worth
  testing, dominated by existing evidence.** Two independent, more
  expressive prior tests of the same underlying "recent within-meet form"
  signal (`sameday_bias`, `jra-meet-momentum`) both came back null. A
  judgment call under uncertainty, not a closed proof.
- **(c) Policy-gradient (REINFORCE) direct optimization**: **tested,
  REJECT.** `primaries_passed=0/3`, top1 exactly flat, place2/place3
  mildly negative, place4-6 regress up to `-1.27pp` — the theoretically
  predicted null on the supervised metrics, plus a mechanistically-clean
  cost on the metrics the REINFORCE reward doesn't supervise. The
  optimizer itself was independently confirmed to be working correctly
  (both a noisy per-epoch sampled-reward trajectory and a deterministic,
  zero-variance expected-reward check both show substantial, consistent
  learning across all 3 seeds) — this is a genuine null/negative result on
  a working implementation, not an artifact of a broken training loop.

## Scoped out for completeness (not tested, not part of this probe's mandate)

- **(d) RL for bet-sizing / staking.** Sizing how much to wager given a
  fixed prediction is a genuinely valid sequential-decision problem in this
  domain (bankroll evolves over successive bets, a real Kelly-criterion-style
  sequential-credit-assignment setting) — but it optimizes bankroll/ROI, a
  fundamentally different objective than this campaign's
  prediction-accuracy goal. Out of scope for this doc.
- **(e) Online bandit for choosing which cell-routing model variant to
  trust.** An online bandit that learns, over time, which of several
  competing model variants to route a given cell to is a different
  granularity of problem entirely — serving-ops / routing-arm selection,
  not a per-race condition-A-D feature question. It overlaps directly with
  the existing offline, gate-based `cell_routing.json` mechanism (already
  live in production, see precedent #3 above) and with this session's live
  `serve-defect-269` investigation (a different agent, active concurrently
  this session). Out of scope for this doc.

## 日本語まとめ

USERから中継された依頼は、JRA条件A-D(開催日目/枠/騎手勝率/血統系統)を通常の
特徴量エンジニアリングだけでなく強化学習(RL)の枠組みでも検討すること
だった。まず3つの候補フレーミングを列挙し、精密なdedup検証を行った。

**(a) 単レースpickへのcontextual bandit / offline RL化**は数学的に
non-distinctと判定しclose(実験不要)。競馬は「取った行動の報酬しか観測
できない」bandit-feedback問題ではなく、全馬の最終着順が常に完全観測される
full-information feedback問題であるため、RLの目的関数は既存のsupervised
ranking目的関数(YetiRank)へpointwiseに帰着する。

**(b) meet内の逐次的信頼度/較正調整**は条件A(開催日目)をRLのtime-stepとして
literal視できる唯一の候補で、メカニズム上は静的特徴量と原理的に異なるが、
同じ情報源(「このmeet/dayがここまでどう推移したか」)は既に2回、より表現力の
高いチャネル(250特徴量CatBoost全体)で独立にテスト済みで両方ともnull
(`sameday_bias`: pooled top1 +0.119pp[LB95-0.064]、`jra-meet-momentum`:
2024foldのみ駆動・seed101でtop1符号反転)——情報優位性の議論によりテスト
価値なしと判定(数学的証明ではなく期待値判断であることは明記)。

**(c) 既存YetiRank損失に代わる/加えたpolicy-gradient(REINFORCE)直接最適化**
のみdedupを生き残った——このcampaignでfeature/fusion/routingは全てREJECT
済みだが、ranking modelの学習アルゴリズム自体に触れたleverは一つもない。
理論的には、CatBoostのYetiRank(LambdaRank系)は既にranking指標に対する
解析的に厳密なpolicy gradientであり、full-information下ではREINFORCEの
score-function推定量はより高分散な劣化版にしかなり得ないと予測されるが、
これは数学的不可能性ではなく期待的な主張のため、最小限の実験で直接確認した。

**実験設計**: `score(w1,w2)=w1*z_base+w2*z_base^2`という2パラメータのみの
policy(champion既存z_baseへの最小限の再スケーリング)を、seed 42/101/2026
それぞれ独立にfold 2023+2024のpooled OOSデータでREINFORCE(EMA baseline
減衰0.99、Adam、30epoch)により学習し、blind fold 2025でbaseline(w1=1,w2=0
=champion現状)に対しpaired bootstrap評価した。学習ループが実際に機能して
いることを、(1) epoch別平均報酬trajectory(1.108→1.50-1.53へ上昇、flat-line
なし)と、(2) 決定論的な期待報酬の直接計算(初期方策0.896→学習後方策1.502、
全seedで約+0.60の一貫した上昇)の二重に確認した。

**結果: REJECT**。top1は完全に不変(delta/LB95とも0.0000——3,455レース中
argmaxが入れ替わったのは1レースのみで、その1レースでは元のpick・新しい
pickとも共に的中していない着順で結果に影響なしと個別検証済み)。place2/
place3は軽微な負、**place4-6は明確に悪化**(place5 -1.27pp、place6
-0.90pp)。これは理論予測通りの「supervisedされている指標では得るもの
なし」に加え、機序として説明可能な副作用も判明した:
`relevance_labels(3,2,1)`は4着以下に報酬0を与えるため、REINFORCEはfield
下位の並び順について一切の勾配シグナルを受け取らない。学習されたw1,w2は
共に正の凸放物線を作り、vertex(z≈-0.83)より低いzでは順位が部分的に逆転
する——これがplace4-6悪化の直接原因であり、実際に唯一のargmax反転事例で
現認された機序と一致する。REINFORCE自体が壊れていたのではなく、正しく
機能した上で「championの持つ厳密な解析的勾配を上回るものは何も見つからず、
教師されていない深い順位に実害を及ぼした」という結論。

**総括**: (a)数学的closed・(b)既存evidenceによりdominated・(c)実験実施し
REJECT。(d)賭け金サイジング用RL、(e)cell-routing variant選択用online
bandit の2件は目的・粒度が異なるため意図的に対象外とした。

## Artifacts

- Script: `apps/pc-keiba-viewer/tmp/ms-rl-formulation/reinforce_rescale_wf.py`
- Report (full 8-metric table, gate, fitted `(w1,w2)` per seed, full
  30-epoch trajectories for all 3 seeds):
  `apps/pc-keiba-viewer/tmp/ms-rl-formulation/reports/reinforce_rescale.json`
- Run log: `apps/pc-keiba-viewer/tmp/ms-rl-formulation/run.log`
- Cached assets reused (not retrained): champion base CatBoost models
  `apps/pc-keiba-viewer/tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`,
  feature list `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` (`armB`).
- MLflow: run logged to `finish-position/wf-eval`,
  `model_version=jra-rl-formulation-reinforce-rescale-2026-07-17`,
  `eval_regime=oos`, `campaign=2026-07-17-rl-formulation` (run id and
  read-back confirmation recorded alongside this doc's commit).

## DO-NOT-RETEST

- **(b) Sequential within-meet dynamic trust/calibration**: not formally
  REJECTed (no WF was run), but treated as closed pending genuinely new
  evidence — do not re-open by re-slicing `sameday_bias` or
  `jra-meet-momentum`'s existing data; a fresh test would need a materially
  different information source, not a different aggregation of the same
  "recent within-meet results" signal.
- **(c) REINFORCE rescaling of `z_base`** via
  `score(w1,w2) = w1*z_base + w2*z_base^2`, reward =
  `relevance_labels(3,2,1)` of the sampled horse: REJECT, DO-NOT-RETEST this
  exact construction. A genuinely different candidate — e.g. a reward that
  also supervises place4-6, or a policy class richer than a 2-parameter
  rescaling of a single existing score — would be a new hypothesis, not a
  retest, and is not evaluated by this doc.
