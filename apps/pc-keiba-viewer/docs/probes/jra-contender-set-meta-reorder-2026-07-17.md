# Contender-set meta-learner reorder — closing the 07-11 fusion doc's variants (a) and (c) (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position architecture lever. This probe executes
  the two variants `docs/probes/jra-volatility-tiered-fusion-2026-07-11.md`
  explicitly left open when it REJECTed a hand-tuned constant-weight
  volatility-gated fusion.
- **Precursor**: `docs/probes/jra-volatility-tiered-fusion-2026-07-11.md`
  (read in full before this probe). That doc fused a champion "base" score
  (250-feat CatBoost YetiRank) with a "boost" sub-score (78-feat CatBoost
  YetiRank trained only on `physical`/`style_pace`/`speed_time` columns) via
  a **hand-tuned constant weight** `z_base + w*z_boost`, gated by a
  volatility quintile. REJECT: 0/10 selection combos cleared gate, the blind
  2025 confirm failed all 3 primaries with a real place3 regression, and an
  exhaustive per-cell/per-tier scan found zero conditional-ADOPT cells. Its
  own close named three untested variants: (a) replace the hand-tuned weight
  with a genuinely trained stacked meta-learner, (b) decile instead of
  quintile tiering, (c) restrict reordering to a market/model-defined top-K
  contender set instead of a smooth race-wide nudge. **This probe tests (a)
  and (c) together; (b) remains untested — see the closing note.**
- **Script / report**: `tmp/ms-contender-meta/contender_meta_wf.py` →
  `tmp/ms-contender-meta/contender_meta_report.json` (every number below is
  sourced from that JSON at its own 4-decimal precision).

## Design under test

A small LightGBM lambdarank meta-learner combines the champion's `base`
score and the `boost` sub-score (both reused verbatim from cached models,
not retrained) plus rank/margin/volatility/market context into one
`meta_score` per horse, per race. Two reorder mechanisms consume the same
`meta_score`:

- **Variant A** — full within-race reorder by `meta_score` (the direct
  realization of open variant (a): a trained combiner instead of a hand-tuned
  weight).
- **Variant C_K** (K ∈ {4, 5, 6}) — reorder limited to the champion's own
  top-K contenders by `base_rank`; horses outside the top-K keep their
  champion `base_rank` unchanged (the realization of open variant (c): a
  contender-set-limited reorder instead of a smooth race-wide nudge — scoped
  to the champion's own top-K rather than the doc's literal "market's
  top-5/6" proposal, a deliberate, clearly-documented adaptation).

**Meta-learner spec** (fixed hyperparameters, deliberately no HPO — avoids
stacking a second selection-bias vector on top of the K/variant sweep):

| hyperparameter    | value      |
| ----------------- | ---------- |
| objective         | lambdarank |
| metric            | ndcg       |
| eval_at           | [3]        |
| num_leaves        | 15         |
| max_depth         | 4          |
| learning_rate     | 0.05       |
| n_estimators      | 150        |
| min_child_samples | 200        |
| reg_lambda        | 3.0        |
| verbosity         | -1         |

**11 meta-features**: `z_base, z_boost, base_rank, boost_rank, rank_diff,
base_margin_to_top, boost_margin_to_top, volatility_score, field_size,
ninkijun, ninkijun_norm`.

**Cached assets reused, not retrained**: champion "base" CatBoost models
(`tmp/candidate-masked-lever-retest/models/base/`, 250 feat, 3 seeds × 3
folds) and "boost" CatBoost models
(`tmp/ms-volatility-fusion/models/boost/`, 78 feat, 3 seeds × 3 folds) — the
exact same models the 07-11 precedent trained and evaluated. Only the meta-
learner (9 fits: 3 arms × 3 seeds) is new.

## Dedup — why this is not a repeat of any closed lever

This design is mechanistically different from every closed lever in this
campaign:

- **NOT** `docs/probes/jra-volatility-tiered-fusion-2026-07-11.md`'s
  hand-tuned constant-weight z-additive fusion (REJECT). That design had a
  fixed, manually-swept weight `w` applied only inside a volatility-quintile
  gate. This design has **no hand-tuned weight anywhere** — the combination
  of `z_base`/`z_boost`/rank/margin/volatility/market context is **learned**
  by a trained LightGBM lambdarank ranker, applied every race (not gated by
  a volatility tier), and the reorder mechanism itself (contender-set-K) is
  a structurally different intervention from a continuous z-score nudge.
- **NOT** the 2026-06-20 score-additive REJECT
  (`project_score_additive_draw_speed_reject_2026_06_20` in project memory)
  — same category of fixed-weight score blend this campaign has repeatedly
  closed; this design has no fixed weight to sweep.
- **NOT** confidence-shrinkage or odds-blind-override (both REJECTED the
  same 2026-07-11 session, `docs/probes/session-2026-07-11-campaign-summary.md`
  rows ~40-44) — confidence-shrinkage shrinks a single score toward the
  within-race mean; odds-blind-override nulls market columns on one model.
  Neither combines two model scores at all; this design's entire premise is
  combining two independently-trained model scores.
- **NOT** isotonic/monotone recalibration (permanently closed per project
  memory — within-race argmax invariant). This design explicitly **can**
  and does change the argmax, since `meta_score` is a learned nonlinear
  function of 11 features, not a monotone transform of one score. (In
  practice, per the feature-importance diagnostic below, it mostly doesn't
  change the argmax in this particular fit — but that is an empirical
  finding about this fit, not a structural constraint of the design the way
  it is for isotonic recalibration.)

## Methodology

**Config selection uses ONLY 2023/2024, never 2025** — two cross-fit
directions:

| arm | fit years | apply year (blind) |
| --- | --------- | ------------------ |
| S1  | 2023      | 2024               |
| S2  | 2024      | 2023               |

The winning config (`selected_config`) is frozen from S1+S2 alone, before
Arm Primary's 2025 report is even computed. `fit_meta()` carries a hard
assertion, checked unconditionally on every call across all three arms, that
`fold_year==2025` never appears in any `.fit()` call anywhere in the script
— 2025 is touched exclusively via `.predict()` inside Arm Primary.

**The headline blind confirm** — a third arm, run only after config
selection is frozen:

| arm     | fit years  | apply year (blind) |
| ------- | ---------- | ------------------ |
| Primary | 2023, 2024 | 2025               |

**Why three arms, not the precedent's single reused fold**: the 07-11 doc's
own untested-variant note asked only for "a genuinely trained stacked
meta-learner" and "a contender-set-limited reorder" as static design ideas —
it did not specify a cross-fit structure. This probe extends what was
literally asked by running three genuinely-independent, genuinely-blind
cross-fit directions (S1, S2, Primary) per seed, so that fold-consistency
(does the effect point the same way in all three, independently-blind years?)
can be checked honestly rather than reusing a single in-sample fold the way
the 07-11 precedent's own `pooled_3fold_context` did. Each arm is fit and
applied fully independently per `seed_base` (never mixing scores or
meta-fits across seeds), then seed-averaged (3 base-model seeds: 42, 101, 2026) before scoring.

**4 configs swept**: A, C_K4, C_K5, C_K6. Selection score per config =
`(min(n_primaries_passed across S1,S2), mean(top1_delta_pp across S1,S2))`,
lexicographic max. Sort-before-mask discipline (the historical
`retest_wf.py` bug precedent) is followed explicitly in the cell-scan code:
`paired()` re-sorts by `race_id` internally before joining and masking, so
every external mask array is built against an already-sorted frame.

## Results

### Selection sweep (2023/2024 only, 4 configs × 2 arms)

| config | arm                | top1 Δpp | top1 LB95pp | place2 Δpp | place2 LB95pp | place3 Δpp | place3 LB95pp | primaries passed |
| ------ | ------------------ | -------- | ----------- | ---------- | ------------- | ---------- | ------------- | ---------------- |
| A      | S1 (fit23→apply24) | 0.2509   | -0.3380     | 0.6659     | -0.0772       | -0.4343    | -1.1581       | 0/3              |
| A      | S2 (fit24→apply23) | -0.5208  | -1.1092     | 0.4726     | -0.2703       | -0.1350    | -0.8295       | 0/3              |
| C_K4   | S1 (fit23→apply24) | 0.2123   | -0.3667     | 0.6755     | -0.0678       | -0.2220    | -0.9265       | 0/3              |
| C_K4   | S2 (fit24→apply23) | -0.4726  | -1.0610     | 0.4340     | -0.2990       | -0.0772    | -0.7427       | 0/3              |
| C_K5   | S1 (fit23→apply24) | 0.2413   | -0.3474     | 0.7141     | -0.0386       | -0.3860    | -1.1098       | 0/3              |
| C_K5   | S2 (fit24→apply23) | -0.5015  | -1.0899     | 0.4823     | -0.2607       | -0.0675    | -0.7620       | 0/3              |
| C_K6   | S1 (fit23→apply24) | 0.2509   | -0.3380     | 0.6562     | -0.0871       | -0.4343    | -1.1581       | 0/3              |
| C_K6   | S2 (fit24→apply23) | -0.5208  | -1.1092     | 0.4726     | -0.2703       | -0.1157    | -0.7909       | 0/3              |

**8/8 selection evaluations failed the gate — 0/3 primaries pass in every
single (config, arm) cell.** Selection-score summary:

| config | min(n_primaries_passed, S1/S2) | mean top1 Δpp (S1/S2)  |
| ------ | ------------------------------ | ---------------------- |
| A      | 0                              | -0.1350                |
| C_K4   | 0                              | -0.1302                |
| C_K5   | 0                              | **-0.1301** (selected) |
| C_K6   | 0                              | -0.1350                |

`C_K5` was **not** selected because it won on any real metric — every config
tied at `min_n_primaries_passed=0`, so the tie-break fell to
"least-negative mean top1 delta," and `C_K5`'s -0.1301 happens to be
0.0001pp less negative than `C_K4`'s -0.1302 and 0.0049pp less negative than
`A`/`C_K6`'s -0.1350. This is **best of a flat/negative field, not a real
winner** — every candidate lost on the selection metric; C_K5 merely lost by
the smallest margin.

### Primary 2025 blind confirm (frozen C_K5, n=3455 races)

| metric     | base %  | cand %  | Δpp     | LB95pp  |
| ---------- | ------- | ------- | ------- | ------- |
| top1       | 33.1404 | 33.1404 | -0.0000 | -0.4727 |
| place2     | 18.3599 | 18.3599 | -0.0000 | -0.5885 |
| place3     | 13.7675 | 13.6517 | -0.1158 | -0.7429 |
| place4     | 12.2335 | 12.2528 | 0.0193  | -0.6078 |
| place5     | 11.3941 | 11.2108 | -0.1833 | -0.6271 |
| place6     | 10.0048 | 10.0048 | 0.0000  | 0.0000  |
| top3_box   | 9.4260  | 9.3102  | -0.1158 | -0.4631 |
| fukusho_2p | 74.7709 | 74.2595 | -0.5113 | -1.0326 |

Gate: `n_primaries_passed=0/3`, `primaries_lb95_positive={top1:false,
place2:false, place3:false}`, `place2_or_place3=false`,
`worst_delta_pp=-0.5113`, **`ACCEPT_strict_gate=false`**.

The top1 delta is literally `-0.0000pp` — base and candidate pick the
identical top1 horse at the aggregate level, not just a small nonzero
effect. The worst metric is `fukusho_2p` at -0.5113pp — a real-magnitude
(though not LB95-significant) regression on one of the two supplementary
metrics this campaign tracks per §7.1/`feedback_eval_rank_1_to_6`.

### Fold consistency (3 genuinely-independent blind arms, frozen C_K5)

| test year | arm                        | top1 Δpp | place2 Δpp | place3 Δpp |
| --------- | -------------------------- | -------- | ---------- | ---------- |
| 2023      | S2 (fit on 2024)           | -0.5015  | 0.4823     | -0.0675    |
| 2024      | S1 (fit on 2023)           | 0.2413   | 0.7141     | -0.3860    |
| 2025      | Primary (fit on 2023+2024) | -0.0000  | -0.0000    | -0.1158    |

`sign_consistency`: `top1=false`, `place2=false`, `place3=true`. top1 and
place2 sign-flip across the 3 folds — not a stable effect in either
direction. **place3 is sign-consistent — negative in all 3 independently-
blind years** (2023: -0.0675pp, 2024: -0.3860pp, 2025: -0.1158pp). This is a
small-but-consistent regression signal, not a non-finding, though none of
the three individually clears LB95 significance.

### Pooled genuinely-blind 3-arm context (n=10,365)

| metric     | base %  | cand %  | Δpp     | LB95pp  |
| ---------- | ------- | ------- | ------- | ------- |
| top1       | 33.7964 | 33.7096 | -0.0868 | -0.4182 |
| place2     | 18.1187 | 18.5174 | 0.3988  | 0.0096  |
| place3     | 14.1630 | 13.9733 | -0.1897 | -0.5950 |
| place4     | 12.1659 | 12.1370 | -0.0289 | -0.3988 |
| place5     | 11.0757 | 11.0564 | -0.0193 | -0.3023 |
| place6     | 10.4165 | 10.4165 | 0.0000  | 0.0000  |
| top3_box   | 9.4099  | 9.3391  | -0.0708 | -0.2863 |
| fukusho_2p | 74.9124 | 74.6133 | -0.2991 | -0.6303 |

Gate: `n_primaries_passed=1/3` (place2 only), `primaries_lb95_positive={top1:
false, place2:true, place3:false}`, `place2_or_place3=true`,
`worst_delta_pp=-0.2991`, **`ACCEPT_strict_gate=false`**.

This pool concatenates S2's 2023 (meta fit on 2024), S1's 2024 (meta fit on
2023), and Primary's 2025 (meta fit on 2023+2024) — disjoint years, every
race scored by an arm that never saw that race's own year during
meta-fitting. Per the report's own `methodological_note`, this is a genuine
methodological improvement over the 07-11 precedent's `pooled_3fold_context`,
whose selection sweep and pooled context drew from the same scored data
under one fixed config (2 of its 3 pooled years were also part of that
config's own selection) — this pool is fully out-of-sample in all three
years simultaneously.

**Report the place2 result honestly**: place2 is +0.3988pp with LB95
+0.0096pp — it clears zero, but by a razor-thin margin (0.0096pp), and top1
(-0.0868pp) and place3 (-0.1897pp) both fail. This is one primary passing
out of three, with the passing one barely distinguishable from the noise
floor below.

### Noise floor check (threshold 0.4pp top1)

| check                        | top1 Δpp | noise_suspect |
| ---------------------------- | -------- | ------------- |
| Primary 2025 blind confirm   | -0.0000  | true          |
| Pooled genuinely-blind 3-arm | -0.0868  | true          |

Both the primary-2025 and pooled-3-arm top1 deltas are far below the 0.4pp
pure-seed-noise floor (`project_training_noise_floor_2026_07_11`) — the
report flags `noise_suspect=true` on both.

### Cell scan (5 dims, n≥200, 2025 Primary-arm only, frozen C_K5)

#### keibajo_code (JRA venue code; 09 = Hanshin)

| venue | n   | top1 Δpp [LB95]   | place2 Δpp [LB95]   | place3 Δpp [LB95] | top3_box Δpp [LB95] | fukusho_2p Δpp [LB95] | clears gate? |
| ----- | --- | ----------------- | ------------------- | ----------------- | ------------------- | --------------------- | ------------ |
| 06    | 504 | 0.0661 [-1.2566]  | -1.4550 [-3.1746]   | -0.0661 [-1.7196] | -0.1984 [-0.8598]   | -0.5952 [-1.9841]     | no           |
| 08    | 468 | 0.3561 [-1.1396]  | -0.2849 [-1.7094]   | -0.4274 [-1.9961] | -0.7835 [-1.4957]   | -0.2849 [-1.4245]     | no           |
| 03    | 240 | 0.5556 [-0.9722]  | 0.4167 [-1.9444]    | -1.2500 [-4.0278] | -0.2778 [-1.6667]   | -0.5556 [-2.7778]     | no           |
| 10    | 240 | 0.9722 [-0.8333]  | 0.0000 [-2.0833]    | -1.1111 [-3.3333] | 1.8056 [0.5556]     | -0.1389 [-2.3611]     | no           |
| 09    | 468 | 0.1425 [-0.9972]  | **1.6382 [0.1425]** | -0.3561 [-1.9961] | 0.4274 [-0.4986]    | 0.2137 [-1.0684]      | **YES**      |
| 05    | 539 | -0.3711 [-1.4224] | 0.1237 [-1.6079]    | 0.8040 [-0.6803]  | -0.5566 [-1.6079]   | -0.9895 [-2.2263]     | no           |
| 04    | 288 | 0.2315 [-1.6204]  | 1.3889 [-0.4630]    | -0.2315 [-2.5463] | -0.4630 [-1.3918]   | -0.8102 [-2.8935]     | no           |
| 07    | 396 | -0.5892 [-2.0202] | -0.3367 [-2.0223]   | 0.4209 [-1.5993]  | -0.4209 [-1.5152]   | -1.0101 [-2.6936]     | no           |

#### kyori_band

| band | n    | top1 Δpp [LB95]   | place2 Δpp [LB95] | place3 Δpp [LB95] | top3_box Δpp [LB95] | fukusho_2p Δpp [LB95] | clears gate? |
| ---- | ---- | ----------------- | ----------------- | ----------------- | ------------------- | --------------------- | ------------ |
| 0    | 744  | 0.0448 [-0.9409]  | -0.3584 [-1.6577] | -0.0896 [-1.3900] | -0.3136 [-0.9857]   | -1.1201 [-2.3746]     | no           |
| 3    | 262  | -0.7634 [-2.6718] | 0.8906 [-1.0178]  | -0.3817 [-2.7990] | 0.1272 [-1.2723]    | 0.0000 [-1.3995]      | no           |
| 1    | 1110 | 0.1201 [-0.6907]  | 0.3303 [-0.8108]  | 0.1502 [-0.8709]  | -0.0601 [-0.6907]   | -0.3904 [-1.4114]     | no           |
| 2    | 1339 | 0.0249 [-0.8215]  | -0.2489 [-1.2447] | -0.2987 [-1.3200] | -0.0996 [-0.6721]   | -0.3734 [-1.1202]     | no           |

#### season_band

| band | n   | top1 Δpp [LB95]   | place2 Δpp [LB95] | place3 Δpp [LB95] | top3_box Δpp [LB95] | fukusho_2p Δpp [LB95] | clears gate? |
| ---- | --- | ----------------- | ----------------- | ----------------- | ------------------- | --------------------- | ------------ |
| 0    | 911 | -0.2195 [-1.1343] | 0.7684 [-0.2195]  | 0.4757 [-0.6952]  | -0.6220 [-1.2441]   | 0.6220 [-0.3659]      | no           |
| 3    | 792 | -0.2946 [-1.3047] | -0.7997 [-2.1886] | -0.2104 [-1.5572] | -0.0842 [-0.7155]   | -1.5152 [-2.6515]     | no           |
| 1    | 936 | -0.2137 [-1.0684] | -0.1068 [-1.1752] | -0.5698 [-1.9231] | 0.3917 [-0.2493]    | -0.7835 [-1.8519]     | no           |
| 2    | 816 | 0.7761 [-0.2859]  | 0.0408 [-1.3480]  | -0.1634 [-1.5931] | -0.1634 [-0.9804]   | -0.4902 [-1.5125]     | no           |

#### current_baba_condition

| condition | n    | top1 Δpp [LB95]   | place2 Δpp [LB95] | place3 Δpp [LB95] | top3_box Δpp [LB95] | fukusho_2p Δpp [LB95] | clears gate? |
| --------- | ---- | ----------------- | ----------------- | ----------------- | ------------------- | --------------------- | ------------ |
| 3         | 235  | 1.1348 [-0.4255]  | -0.8511 [-2.8369] | -0.0000 [-2.2695] | 0.2837 [-0.5674]    | -0.2837 [-2.1277]     | no           |
| 1         | 2529 | -0.1054 [-0.6722] | 0.0791 [-0.6330]  | -0.1977 [-1.0017] | -0.0264 [-0.4089]   | -0.5536 [-1.1862]     | no           |
| 2         | 621  | -0.0000 [-1.1809] | -0.0000 [-1.5043] | -0.1074 [-1.3956] | -0.4294 [-1.2346]   | -0.4294 [-1.7190]     | no           |

#### grade_code

| grade            | n    | top1 Δpp [LB95]  | place2 Δpp [LB95] | place3 Δpp [LB95] | top3_box Δpp [LB95] | fukusho_2p Δpp [LB95] | clears gate? |
| ---------------- | ---- | ---------------- | ----------------- | ----------------- | ------------------- | --------------------- | ------------ |
| (blank/ungraded) | 2526 | 0.0528 [-0.5018] | 0.0396 [-0.6994]  | -0.0132 [-0.7657] | -0.2375 [-0.6334]   | -0.4883 [-1.0953]     | no           |
| E                | 726  | 0.3673 [-0.7805] | -0.0000 [-1.1478] | 0.0918 [-1.1478]  | 0.4132 [-0.2755]    | -0.3673 [-1.4692]     | no           |

#### Summer 4-venue pooled (01/02/03/10, n=792)

top1 0.1263 [-0.7997], place2 -0.2946 [-1.5572], place3 -0.6734 [-2.1044],
top3_box 0.5892 [-0.1263], fukusho_2p -0.3367 [-1.5993] — **no clear**.

That is 8 + 4 + 4 + 3 + 2 = 21 individual cells plus the 1 summer-pooled
scope = **22 comparisons total**, no multiple-comparison correction applied
at the raw-flagging stage.

#### keibajo_code=09 is NOT a genuine conditional-ADOPT candidate

Exactly one cell — `keibajo_code=09` (Hanshin), n=468 — mechanically clears
the loose single-primary flag (place2 +1.6382pp, LB95 +0.1425). This is
**not** a real adoption signal, for three compounding reasons:

1. **The same cell has a real same-cell regression on another primary.**
   `keibajo_code=09`'s place3 is -0.3561pp with LB95 -1.9961 — a genuine
   regression that fails the multi-metric no-regression discipline
   (`feedback_multi_metric_accept_gate`), even though it wasn't separately
   gated in the raw per-cell flag (the flag only checks for ≥1 primary
   clearing, not for a simultaneous regression on another primary in the
   same cell).
2. **It is one hit among 22 unadjusted comparisons.** At typical
   false-positive rates this is consistent with chance — no
   multiple-comparison correction (e.g. Bonferroni) was applied at the
   raw-flagging stage, and one flag out of 22 comparisons is not a
   surprising count under the null.
3. **No fold-consistency confirmation, and no venue-level effect anywhere
   else in the scan.** This is a single blind year (2025 Primary-arm only)
   — there is no second independent blind year to check sign-consistency
   against for this specific cell. The other 7 venues in the same scan
   (06, 08, 03, 10, 05, 04, 07) all fail to clear, so there is no
   venue-level pattern corroborating Hanshin specifically. This is exactly
   the "weak cell = sampling-noise mirage" pattern this campaign's
   `project_accuracy_stagnation_root_cause_2026_07_11` root-cause
   investigation already characterized structurally, and precisely the
   discipline `feedback_cell_level_adoption_no_pooled_eval` requires (fold
   consistency + multiple-comparison awareness) before treating any single
   cell hit as adoptable.

### Feature-importance diagnostic (gain-based, per arm/seed)

| arm     | seed | z_base     | z_boost | base_rank | boost_rank | rank_diff | base_margin_to_top | boost_margin_to_top | volatility_score | field_size | ninkijun | ninkijun_norm |
| ------- | ---- | ---------- | ------- | --------- | ---------- | --------- | ------------------ | ------------------- | ---------------- | ---------- | -------- | ------------- |
| S1      | 42   | 51518.005  | 835.695 | 115.373   | 115.123    | 185.782   | 2190.251           | 659.533             | 872.346          | 236.274    | 69.914   | 443.723       |
| S1      | 101  | 53374.366  | 690.010 | 342.986   | 88.613     | 95.994    | 2801.097           | 422.841             | 794.121          | 212.770    | 79.369   | 519.015       |
| S1      | 2026 | 51807.325  | 587.015 | 89.390    | 67.108     | 215.310   | 2287.047           | 683.258             | 775.534          | 114.298    | 75.620   | 353.088       |
| S2      | 42   | 56206.007  | 808.119 | 146.302   | 104.717    | 158.478   | 1346.367           | 487.671             | 488.491          | 91.599     | 142.218  | 467.378       |
| S2      | 101  | 56004.684  | 746.601 | 158.328   | 106.291    | 168.348   | 1985.503           | 542.085             | 566.309          | 101.142    | 132.107  | 498.602       |
| S2      | 2026 | 56506.785  | 725.813 | 313.734   | 156.040    | 165.858   | 1632.706           | 524.510             | 595.845          | 103.132    | 56.499   | 402.961       |
| Primary | 42   | 110911.116 | 830.431 | 419.759   | 123.196    | 153.513   | 2394.970           | 641.893             | 897.963          | 204.702    | 145.017  | 686.653       |
| Primary | 101  | 109177.584 | 763.303 | 365.484   | 137.194    | 164.335   | 2236.560           | 718.149             | 1299.503         | 208.183    | 146.428  | 607.441       |
| Primary | 2026 | 109306.882 | 770.989 | 369.136   | 119.533    | 200.374   | 3791.047           | 571.698             | 760.859          | 155.709    | 114.866  | 460.912       |

`z_base` overwhelmingly dominates in every one of the 9 arm/seed cells
without exception, and `base_margin_to_top` is the 2nd-highest feature in
every single cell too, also without exception — the gap between them ranges
roughly 19×-49× depending on arm/seed. In the headline Primary arm
specifically (fit on pooled 2023+2024, ~94,026 rows — roughly double
S1/S2's ~47,000-row single-year fits, which is why its absolute gain sums
are roughly double as well), `z_base` sits at ~109,178-110,911 against
`base_margin_to_top`'s ~2,237-3,791 — a ~29×-50× gap.

This is a genuinely useful diagnostic, not just a formality: the
meta-learner essentially re-learned "rank by the champion's own score, with
a small correction for how far a horse sits from the top of the champion's
own ranking," rather than materially blending in the boost/context signal.
This is a coherent mechanistic explanation for why every effect size above
clusters near zero — neither a strong positive effect nor the sharp
place3-specific regression the 07-11 hand-tuned-weight design produced —
rather than a clean win or a clean failure in either direction. The
LightGBM ranker is not malfunctioning; it faithfully found that `z_base`
already carries almost all of the separable in-sample ranking signal, and
the 10 other features (including the entire `boost` sub-model) contribute
only marginal refinement.

## Verdict: REJECT

**Both tested variants are REJECTED**: variant (a) (stacked meta-learner,
full within-race reorder = Variant A) and variant (c) (contender-set-limited
reorder = Variant C_K, K ∈ {4, 5, 6}).

- **Selection phase**: 0/8 evaluations (4 configs × 2 arms) passed the gate.
  `C_K5` was selected purely by tie-break (least-negative mean top1 delta
  among four uniformly-failing candidates) — best of a flat/negative field,
  not a real winner.
- **Primary 2025 blind confirm** (frozen C_K5): 0/3 primaries pass,
  `ACCEPT_strict_gate=false`, top1 delta is literally `-0.0000pp` (base and
  candidate pick the identical top1 horse), worst metric `fukusho_2p` at
  -0.5113pp.
- **Fold consistency** (3 genuinely-independent blind arms): top1 and
  place2 sign-flip across folds (not stable); place3 is sign-consistently
  **negative** in all 3 independently-blind years, a small-but-real
  regression signal.
- **Pooled genuinely-blind 3-arm context** (methodologically stronger than
  the 07-11 precedent's own pooled context — every year here was scored by
  an arm that never saw that year during meta-fitting): 1/3 primaries pass
  (place2, LB95 +0.0096pp — a razor-thin margin, essentially indistinguishable
  from the noise floor), top1 and place3 both fail,
  `ACCEPT_strict_gate=false`.
- **Noise floor**: both the primary-2025 (-0.0000pp) and pooled-3-arm
  (-0.0868pp) top1 deltas are far below the 0.4pp pure-seed-noise floor —
  flagged `noise_suspect=true` on both.
- **Cell scan** (22 comparisons, no multiple-comparison correction): exactly
  1 cell (`keibajo_code=09`, Hanshin) mechanically clears the loose
  single-primary flag, and it is explicitly **not** a genuine conditional-
  ADOPT candidate — same-cell place3 regression, one hit among 22 unadjusted
  comparisons, no fold-consistency confirmation and no corroborating
  venue-level pattern elsewhere in the scan.
- **Mechanistic explanation**: gain-based feature importance shows `z_base`
  dominates by ~19×-49× over the next-highest feature in every arm/seed —
  the meta-learner essentially collapsed to "trust the champion," which
  coherently explains why every effect size above clusters near zero rather
  than showing a clean win or a clean, sharp failure.

This closes **both** open threads the 07-11 doc left explicitly untested.
**Variant (b) from that doc (decile instead of quintile tiering) remains
untested and out of scope for this probe** — it applies to the original
hand-tuned constant-weight fusion design, not to this stacked meta-learner
design, and no claim is made here about its status.

**DO-NOT-RETEST this exact design**: a stacked LightGBM lambdarank
meta-learner over base+boost scores plus rank/margin/volatility/market
context (the 11-feature spec above), trained via 3-arm genuinely-blind
cross-fit (S1/S2/Primary), with either a full within-race reorder (Variant
A) or a contender-set-limited reorder at K ∈ {4, 5, 6} (Variant C_K).

## 日本語まとめ

`jra-volatility-tiered-fusion-2026-07-11.md` が REJECT 時に明示的に残した
未検証の2案——(a) 手動チューニングした固定重みの代わりに本物の学習済み
メタラーナーで融合する、(c) レース全体への滑らかな加重ではなく、チャンピオン
自身の上位K頭という contender set 内に並べ替えを限定する——を、同一の
小型 LightGBM lambdarank メタラーナー(11特徴量: z_base/z_boost/順位・
マージン差分/volatility_score/人気関連)に両方乗せて検証した。ベース
(250特徴量チャンピオン)とブースト(78特徴量 physical/style_pace/speed_time
サブモデル)のスコアはいずれもキャッシュ済みモデルを再利用し、メタラーナー
のみ新規に学習(3アーム×3seed=9fit)。設定選択は2023/2024のみ(S1: 2023学習
→2024適用、S2: 2024学習→2023適用)で行い、2025年は fit_meta() 内の
無条件assertで一切の学習に混入しないことを保証した上で、Primaryアーム
(2023+2024学習→2025適用)による真のblind confirmを実施した。

4設定(A, C_K4, C_K5, C_K6)×2アームの選択スイープは **8/8 全滅**
(0/3 primaries pass)——`C_K5`が選ばれたのは実質的な勝者としてではなく、
「平均top1delta が最もマイナス幅の小さい」タイブレークの結果に過ぎない
(横並びで負けている4設定のうち、最も負け幅が小さかっただけ)。2025年
blind confirmでもtop1delta は文字通り**0.0000pp**(ベースと候補が集計
レベルで同一の1着馬を選択)、place3が-0.1158pp、fukusho_2pが-0.5113pp
とワースト。3アーム独立blind(2023/2024/2025)での符号一致性を見ると、
top1とplace2は符号が折れるが、**place3は3年連続で符号が負に一致**——
小さいが無視できない回帰シグナルとして正直に報告する。3年をプールした
「真にblindな」文脈(各年とも自分の年を学習で見ていないアームがスコアリング
——07-11先例のpooled文脈より方法論的に厳密)でも1/3 primaryのみ通過
(place2、LB95 +0.0096pp——ゼロをかろうじて上回るのみでノイズ床と実質
区別不能)、noise_floor_checkは両方の主要top1deltaを`noise_suspect=true`
と判定した。

cell scan(5次元・n≥200、計22比較、多重比較補正なし)では
`keibajo_code=09`(阪神)のみが緩いsingle-primaryフラグを機械的にクリア
(place2 +1.6382pp、LB95 +0.1425)したが、これは真の条件付きADOPT候補では
**ない**と明記する:同一セルでplace3が-0.3561pp[LB95 -1.9961]と実質的に
回帰しておりmulti-metric no-regression原則に反する、22比較中1件という
数は多重比較未補正下では偶然の範囲内、単一blind年のみでfold一貫性の
裏付けがなく他の7場でも同様の効果は一切見られない——本キャンペーンの
root-cause調査が構造的に特徴づけた「弱いcell=サンプリングノイズの蜃気楼」
パターンそのものである。feature importance(gain)を見ると、全9
アーム×seedの組み合わせで例外なく z_base が支配的(次点の
base_margin_to_topに対して概ね19〜49倍)——メタラーナーは事実上
「チャンピオン自身のスコアで順位付けする」ことを再学習しただけで、
boost/context側の情報を実質的にほとんど活用しておらず、これが効果量が
軒並みゼロ近傍に収束した(明確な正の効果でも07-11設計のような鋭い
place3回帰でもない)ことの一貫した機序的説明になっている。

**結論: REJECT**(variant (a) のメタラーナー版・variant (c) の
contender-set限定reorder版ともに不採用)。同一設計の再テストは禁止。
07-11ドキュメントのvariant (b)(decile分割)は本probeの対象外であり、
未検証のまま残る。

## Artifacts

- `tmp/ms-contender-meta/contender_meta_wf.py` — full script (module
  docstring documents the design/dedup reasoning in detail)
- `tmp/ms-contender-meta/contender_meta_report.json` — full verified report
  (every number in this doc is sourced from it)
- `tmp/ms-contender-meta/cell_report_for_mlflow.json` — flat 26-row
  reshape of the report for MLflow `cell_report` ingestion
- `tmp/ms-contender-meta/models/meta/{S1,S2,Primary}/seed{42,101,2026}/model.txt`
  — 9 saved LightGBM meta-learner models (3 arms × 3 seeds)
- `tmp/ms-contender-meta/run.log` — full training/eval run log
- MLflow run: `6237adb91b4e4e36aeffb4317ce9280a`
  (`finish-position/wf-eval`, `model_version=jra-contender-set-meta-reorder-c_k5-2026-07-17`,
  `verdict=reject-do-not-retest`)
