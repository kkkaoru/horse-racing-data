# JRA — Monotonic Constraints on Market Features (wave7 additional B) — WF Probe

- **Date**: 2026-07-17
- **Category**: JRA finish-position, architecture/regularization lever (not a
  new feature column, not a new loss function)
- **Source**: team-lead direct instruction ("追加調査B"), lever-bank-v2
  candidate #3 (`docs/probes/next-cycle-lever-bank-v2-2026-07-17.md` §3, full
  detail `tmp/frontier-scout/lever_bank_v2.md` lines 81-113) — assessed there
  as "UNTESTED-CANDIDATE, mechanistically distinct from everything closed,"
  prior probability "low-to-moderate."

## 0. Headline result

**REJECT, both arms.** The "full" arm (8 columns, the entire redundant
market-observable cluster constrained together) is a clean `0/3` primaries
pass in both the all-JRA pooled result and the summer-4-venue-restricted
result. The "narrow" arm (2 columns, the lever-bank scout's own minimal
proposal) nominally clears `place3` alone in the all-JRA pooled result
(`+0.29pp[LB95+0.07]`) but that alone is insufficient (`n_primaries_passed=1`,
gate requires ≥2), and decomposing it further makes the case for adoption
weaker, not stronger: the signal is **fold-2024-driven only** (fold-2023
`+0.10pp[LB95-0.31]`, fold-2025 `+0.22pp[LB95-0.15]`, neither significant;
only fold-2024 `+0.54pp[LB95+0.15]` clears LB95>0), and it **vanishes
entirely under summer-4-venue restriction** (`place3 +0.42pp[LB95-0.04]`,
narrowly missing significance, with `0/3` primaries passing overall) — i.e.
it fails specifically in the population this campaign's whole day is about.

CatBoost's `monotone_constraints` is confirmed genuinely functional for the
`YetiRank` loss (not a silent no-op — see §2), so this is a real negative
result about the hypothesis, not a tooling dead end.

## 1. Dedup — why this is genuinely untested, and distinct from a closed lane

- Grepped `finish_position_catboost.py`, `finish_position_xgboost.py`,
  `train_finish_position_catboost_walk_forward.py`, and the bespoke HPO
  tuner `tune_finish_position_jra_cb.py`: zero hits for `monotone_constraints`
  in any of them (re-confirmed independently, matching the lever-bank v2
  scout's own grep finding). The only production usage of this parameter
  today is in the non-champion `finish_position_lightgbm.py`, which has a
  working `resolve_monotone_constraints()` helper — a different model family
  entirely, never on the CatBoost champion path.
- **This is NOT the same as the already-closed "isotonic/monotone
  re-calibration" lane** (`index_closed_probes.md`: "isotonic/monotone
  再較正は数学的恒久クローズ (within-race argmax 不変)"). That closure is
  about a **post-hoc uniform monotonic transform applied to already-computed
  scores** — since the same transform is applied to every horse in a race,
  it provably cannot reorder them, hence provably cannot change
  top1/rank accuracy (this is a mathematical fact about monotonic functions,
  not an empirical finding). What this probe tests is a **training-time
  constraint on specific input features** that changes which splits CatBoost
  is allowed to choose while building trees — a constrained tree can pick a
  genuinely different split (different feature, different threshold) at a
  node than an unconstrained tree would, which **can** change within-race
  relative ordering. Different mechanism, different mathematics, correctly
  flagged by the lever-bank scout as distinct and confirmed here.
- An existing doc (`rootcause-i6-architecture.md`, 2026-06-11) asserted
  monotone constraints were already active on odds/popularity features. This
  is **not borne out by the champion code** (confirmed above) — a 29-day-old
  inaccuracy in that doc, not a reason to skip this candidate.

## 2. CatBoost `monotone_constraints` + `YetiRank`: genuinely functional, not a no-op

Given the earlier NAR-pooling arm this same day found `Pool(weight=...)` to
be a _silent_ no-op for `YetiRank` (no exception, just an easy-to-miss
runtime warning), this parameter combination was independently verified
empirically before trusting any WF result, rather than trusted from the
installed source inspection alone.

**Repro**: synthetic ranking data, groups of 5, with a deliberately
**U-shaped** (non-monotonic) true relationship between one feature (`X0`)
and relevance (`true_score = X0² + 0.3·X1 + noise`, converted to
within-group relevance ranks). Fit two `YetiRank` models on identical data:
one unconstrained, one with `monotone_constraints=[1, 0]` (force `X0`
non-decreasing). Sweeping `X0` from -3 to 3 at `X1=0`:

- **Unconstrained model**: correctly learned the U-shape — predictions
  decrease then increase (`10.62 → -7.46 → 10.62`), matching the true
  generative process. Confirms the model _can_ express non-monotonic
  relationships when nothing prevents it.
- **Constrained model**: genuinely monotonic non-decreasing prediction curve
  (`-0.70 → -0.70 → ... → 6.36`, flat where the true pattern wants to
  decrease, rising where it also wants to rise) — the constraint is _not_
  circumventable, it visibly reshapes the fitted function.
- `get_params()` on the constrained model correctly round-trips
  `monotone_constraints=[1, 0]`; the unconstrained model reports `None`.

Installed version: `catboost==1.2.10`. **Conclusion: this parameter has a
real, structural effect on `YetiRank` — proceed to the real WF test rather
than closing as NOT-APPLICABLE.**

## 3. Column selection — empirically verified direction, not guessed from names

Spearman correlation against `finish_position` (1=win=best, higher=worse) on
a 150K-row sample of `tmp/candidate-eval-jra/augmented/`. Convention:
CatBoost relevance labels are 3/2/1/0 for 1st/2nd/3rd/other (higher=better),
so a feature **positively** correlated with `finish_position` (higher value
→ worse finish) needs `monotone=-1`; **negatively** correlated needs `+1`.

| column                              | ρ vs finish_position | monotone |
| ----------------------------------- | -------------------- | -------- |
| `tansho_odds_raw`                   | +0.573               | -1       |
| `tansho_ninkijun_raw`               | +0.582               | -1       |
| `inverse_odds_implied_prob`         | -0.573               | +1       |
| `inverse_odds_market_share`         | -0.573               | +1       |
| `inverse_odds_rank_in_race`         | +0.582               | -1       |
| `popularity_rank_in_race`           | +0.582               | -1       |
| `popularity_score`                  | +0.525               | **-1**   |
| `odds_score`                        | +0.573               | **-1**   |
| `popularity_odds_disagreement`      | -0.226               | excluded |
| `horse_popularity_vs_field`         | +0.519               | excluded |
| `field_dominant_favorite_indicator` | +0.016               | excluded |

Two of these directions are **not** what the column names alone would
suggest: `popularity_score` and `odds_score` read like "higher = better
favored," but both are empirically `+0.52`-`+0.57` correlated with
_worse_ finish (same sign and similar magnitude as the raw
odds/ninkijun columns) — they are almost certainly rescaled encodings of the
raw odds/rank value itself, not inverted goodness scores. Trusting the
names would have built the constraint backwards for these two columns. This
is exactly why direction was verified empirically rather than assumed.

**Excluded** (checked and rejected as not "near-axiomatically monotonic," per
the lever-bank scout's own framing): `field_dominant_favorite_indicator`
(ρ≈0.016, a race-level "is there a clear favorite" flag, not a per-horse
signal); `popularity_odds_disagreement` (ρ≈-0.226, a mismatch metric, weaker
and not a pure favorite-ness direction); `horse_popularity_vs_field`,
`odds_score_diff_from_race_avg`, `popularity_score_diff_from_race_avg`
(relative/derived versions, deliberately left out of scope this round).

**Redundancy discovery, and why it forced a two-arm design**: pairwise
Spearman correlation among the 8 selected columns is **≥0.94 for every
pair**, several exactly `±1.0000` (`tansho_odds_raw` vs `odds_score` =
`1.0000`; `tansho_odds_raw` vs `inverse_odds_implied_prob` = `-1.0000`;
`tansho_ninkijun_raw` vs `inverse_odds_rank_in_race` vs
`popularity_rank_in_race` ≈ `0.9998`-`1.0000`). These are not 8 independent
market signals — they are **one underlying market-standing signal encoded
~8 different ways**. Constraining only 1-2 of them while leaving near-perfect
duplicates unconstrained would let CatBoost trivially route around the
constraint via the unconstrained twin, making a narrow constraint
potentially inert by construction rather than a genuine test of "does
monotonicity help." Hence two arms (not a strength sweep, per the
orchestrator's 1-2-config instruction):

- **"narrow"** (2 cols: `popularity_rank_in_race`, `inverse_odds_rank_in_race`,
  both `-1`) — the lever-bank scout's own original minimal proposal, tests
  the low-risk/low-effect hypothesis, but a null result here is inherently
  ambiguous (no effect, or routed around via a duplicate?).
- **"full"** (8 cols, all listed above with their verified signs) — the
  entire redundant cluster constrained together, the only design immune to
  the route-around escape hatch, i.e. the real test.

All other 242-244 armB features are unconstrained (`monotone=0`) in both
arms; baseline has no `monotone_constraints` key at all (not an all-zero
list — kept semantically distinct from "explicitly unconstrained").

## 4. Harness and spec

Adapted from `tmp/crosspool-odds-divergence/retest_wf.py` (same template used
for the NAR-pooling arm). New script: `tmp/monotonic-constraints/wf_monotonic.py`
(gitignored, evidence artifact only). CatBoost YetiRank, iterations=300,
depth=8, lr=0.05, l2=3.0, no early-stop, `cat_indices=[]`, base=armB-250,
`thread_count=4`. **3 seeds** `[42, 101, 2026]` × **3 folds** `{2023, 2024, 2025}`
(train 2013..Y-1, blind Y) — full WF this round, no phased screening (unlike
the same-day NAR-pooling arm, which used a 2-fold phase-1 screen). Gate
constants match the day's standard: `PRIMARIES=["top1","place2","place3"]`,
`GATE_MIN_DELTA=0.08`, `GATE_NO_REG=-0.05`, `N_BOOT=2000`, `BOOT_SEED=20260519`,
`CELL_DIMS=["keibajo_code","kyori_band","season_band","current_baba_condition"]`,
`CELL_MIN=200`. `SUMMER_VENUES = {"01","02","03","10"}` (Sapporo/Hakodate/
Fukushima/Kokura) — **note**: at least two other `tmp/` scripts in this repo
(`tmp/summer-venue-cells/score_summer_venue_cells.py:54`,
`tmp/candidate-data-inventory/probe_tozai_away.py:26`) define this set
without Sapporo (`{"02","03","10"}`), a live uncaught bug as of today; this
harness hardcodes the correct 4-venue set rather than copying either.

Baseline trained fresh in this harness (27 fits total: 9 baseline + 9 narrow

- 9 full) rather than reusing externally-cached artifacts, for
  self-contained provenance — cost is trivial at JRA-only row counts
  (single-fit timing: baseline 55.7s, 8-column-constrained candidate 83.8s for
  fold-2023/seed42, ~485K-620K rows; total measured wall-clock ≈33 min for all
  27 fits + evaluation). Sort-before-mask discipline (`.sort("race_id")`
  before `group_id`/before any boolean-mask cell filter) matches the pattern
  independently verified against this repo's installed Polars (1.42.0) during
  the NAR-pooling arm earlier today; this harness uses the identical
  join-after-sort construction, not a modified one.

## 5. Results

### All-JRA pooled (seed-avg, n=10,365 races)

| metric     | narrow Δpp [LB95]     | full Δpp [LB95]   |
| ---------- | --------------------- | ----------------- |
| top1       | +0.0708 [-0.1190]     | -0.0482 [-0.2219] |
| place2     | +0.1029 [-0.1126]     | +0.0032 [-0.2090] |
| place3     | **+0.2862 [+0.0675]** | +0.1737 [-0.0386] |
| top3_box   | +0.0354 [-0.0836]     | +0.0064 [-0.0997] |
| fukusho_2p | -0.0386 [-0.1995]     | -0.0193 [-0.1865] |

Gate: narrow `n_primaries_passed=1/3` (place3 only — top1/place2 both miss
LB95>0), `ACCEPT_strict_gate=false`. full `n_primaries_passed=0/3`,
`ACCEPT_strict_gate=false`.

### Summer-4-venue restricted (seed-avg, n=2,448 races)

| metric     | narrow Δpp [LB95] | full Δpp [LB95]   |
| ---------- | ----------------- | ----------------- |
| top1       | -0.0272 [-0.4221] | +0.0953 [-0.2859] |
| place2     | +0.1770 [-0.2859] | -0.1770 [-0.6264] |
| place3     | +0.4221 [-0.0408] | +0.1089 [-0.3544] |
| top3_box   | +0.0681 [-0.1498] | +0.1089 [-0.1225] |
| fukusho_2p | -0.0545 [-0.4221] | -0.0817 [-0.4493] |

Gate: **`n_primaries_passed=0/3` for both arms** — narrow's one pooled
"pass" (place3) drops to `+0.42pp[LB95-0.04]`, missing significance, in
exactly the population this campaign is about.

### Per-fold (seed-avg) — why the narrow-arm pooled "pass" doesn't hold up

| arm    | metric | fold2023          | fold2024              | fold2025          |
| ------ | ------ | ----------------- | --------------------- | ----------------- |
| narrow | place2 | -0.3472 [-0.7137] | **+0.5211 [+0.1544]** | +0.1351 [-0.2315] |
| narrow | place3 | +0.0965 [-0.3086] | **+0.5404 [+0.1544]** | +0.2219 [-0.1544] |
| full   | place2 | -0.3376 [-0.6947] | **+0.4922 [+0.0963]** | -0.1447 [-0.4826] |
| full   | place3 | +0.1929 [-0.1929] | **+0.4729 [+0.0869]** | -0.1447 [-0.5210] |

Both arms show the **identical pattern**: fold-2023 negative on place2,
fold-2025 flat-to-negative, and _only_ fold-2024 individually clears
LB95>0 — on both place2 and place3, in both arms. Per-seed decomposition of
the narrow arm's pooled place3 confirms the same fragility at the seed
level: only seed101 (`+0.46pp[LB95+0.13]`) individually clears
significance; seed42 (`+0.14pp[LB95-0.23]`) and seed2026
(`+0.25pp[LB95-0.14]`) do not. This is the same "single-fold/seed-driven,
doesn't replicate" pattern this campaign's history repeatedly treats as
disqualifying rather than promising (matches, e.g., the NAR-pooling arm's
own fold-2024-only place2 signal earlier today, the cross-source companion
column REJECT, and meet-momentum REJECT).

**Cross-lever observation, not chased further here**: fold-2024 producing an
anomalous positive signal that doesn't replicate in 2023 or 2025 has now
shown up independently in **two unrelated candidate constructions tested
today** (this arm and the NAR-pooling arm's place2 result). That's
suggestive of something about fold-2024 itself (small-sample validation-year
noise, or a specific champion weakness in that year) rather than either
lever being real — worth a future forensic look, not undertaken here (out
of this probe's scope and time budget).

### Cell scan (`cells_ge200`, sort-before-mask, LB95>0 hits only)

**narrow** (8 of 110 scanned cell×metric combinations): `keibajo_code=10`
top3_box +0.13[n=792]; `keibajo_code=08` top1 +0.11[n=1535];
`kyori_band=2` place3 +0.09[n=3988]; `season_band=3` place2 +0.16[n=2508];
`season_band=3` place3 +0.08[n=2508]; `season_band=2` top1 +0.09[n=2495];
`current_baba_condition=1` place3 +0.04[n=7488]; `current_baba_condition=3`
place3 +0.70[n=859].

**full** (4 of 110): `keibajo_code=01` top1 +0.20[n=504]; `keibajo_code=06`
place3 +0.27[n=1500]; `season_band=3` place3 +0.13[n=2508];
`current_baba_condition=3` place3 +0.23[n=859].

`season_band=3` (winter) place3 and `current_baba_condition=3` place3 are
positive in both arms — noted as an observation, not treated as a follow-up
lead: isolated/small-cluster hits inside a ~20-cell × 5-metric scan match
this campaign's established multiple-comparisons-noise pattern (same
treatment given to the NAR-pooling arm's single Sapporo-cell hit earlier
today), and there's no pre-registered mechanism reason to expect winter or
heavy-going conditions specifically to interact with market-feature
monotonicity.

## 6. Verdict

**REJECT, both arms.** No production model changed
(`jra-cb-v9-sim-2013-clean` untouched). The "full" arm — the only design
immune to the redundant-duplicate route-around problem, and therefore the
real test of the hypothesis — is unambiguous: 0/3 primaries in both the
all-JRA and summer-4-venue slices, no fold individually robust except the
same fold-2024 anomaly seen elsewhere today. The "narrow" arm's one nominal
pooled pass doesn't survive its own per-fold/per-seed decomposition or
summer-4-venue restriction, and is inherently ambiguous by design (a
2-column constraint inside an 8-column-redundant cluster can't distinguish
"no effect" from "routed around").

**Mechanism**: consistent with the lever-bank scout's own prior — "this
repo's single most repeated finding pattern... is that explicitly encoding
structure a depth-8 regularized tree ensemble already learns implicitly
produces null deltas." A ~635K-row, depth-8, 300-iteration CatBoost model
already has ample capacity and regularization (`l2_leaf_reg=3.0`) to learn a
near-monotonic relationship on these features where the data supports one;
forcing it structurally doesn't unlock new signal, and any variance
reduction in sparse tail populations (the scout's own hoped-for mechanism)
either doesn't materialize or is too small to clear this campaign's noise
floor (±0.4pp single-arm).

**DO-NOT-RETEST scope**: this exact construction — armB-250 baseline,
`monotone_constraints` on {the 2-column narrow set} or {the 8-column full
redundant-cluster set} with the verified signs above, CatBoost YetiRank armB
spec, JRA-only. **Left genuinely open**: monotone constraints on non-market
feature families (e.g. distance/speed-index continuous features, if any are
judged near-axiomatically monotonic — not attempted here, scope was
market-only per the orchestrator's brief); the `odds_score_diff_from_race_avg`
/ `popularity_score_diff_from_race_avg` / `horse_popularity_vs_field`
relative-encoding columns (deliberately excluded from both arms this round);
and the fold-2024 cross-lever anomaly noted in §5, which is a distinct
question from this probe's own hypothesis.

## 7. Artifacts

- Harness: `apps/pc-keiba-viewer/tmp/monotonic-constraints/wf_monotonic.py`
  (gitignored).
- Report: `apps/pc-keiba-viewer/tmp/monotonic-constraints/reports/wf_monotonic.json`.
- 27 trained models: `apps/pc-keiba-viewer/tmp/monotonic-constraints/models/{base,narrow,full}/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`.
- MLflow: `finish-position/wf-eval`, `model_version` =
  `jra-monotonic-narrow-candidate-2026-07-17` /
  `jra-monotonic-full-candidate-2026-07-17`, `eval_regime=wf`,
  `tags.gate_result=REJECT` for both, `register=False`, `champion=False`.
