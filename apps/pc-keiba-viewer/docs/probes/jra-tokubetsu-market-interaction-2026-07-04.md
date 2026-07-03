# JRA Market-Rank × Tokubetsu(E-grade) Interaction Feature Test (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **Trigger**: `jra-summer-upset-divergence-2026-07-04.md` found `grade_code=='E'`
  (特別/tokubetsu, featured non-graded stakes) races run **+5.5 to +12.1pp
  higher upset-winner rates than ordinary races at all 4 summer venues**
  (Sapporo/Hakodate/Fukushima/Kokura), worst pockets Fukushima×intermediate×E
  59.1% (n=23) and Kokura×sprint×E 51.4% (n=37). That doc explicitly did
  **not** recommend per-venue/per-segment routing (established REJECT
  territory — `project_venue_cell_round2_2026_06_20`,
  `project_jra_rs_cell_routing_reject_2026_07_03`) but flagged it as a
  candidate for either a calibration-layer fix or a genuinely global
  per-horse interaction feature (not routing) that lets the ranker learn,
  everywhere, to discount market-rank signal specifically in tokubetsu
  races. This doc tests the latter.
- **Regime-stability caution carried in from the task brief**: JRA
  odds-entropy field-difficulty features were REJECTed 2026-06-23 as
  regime-dependent (strong probe signal, 2024 backfired). `is_tokubetsu` is
  the categorical cousin of that continuous signal — the 3-fold WF below
  tests regime stability directly, and (spoiler) the concern was justified:
  see the per-fold breakdown.

## Method

Same harness, baseline, model spec, and (fixed) cell-mask code as
`jra-masked-lever-clean-retest-2026-07-04.md` — this doc reuses
`tmp/candidate-masked-lever-retest/retest_wf.py` (now with a `tokubetsu_mkt`
lever registered) and its already-trained, cached `armB` control models
(same 250-feat CLEAN baseline, same 3 folds × 3 seeds), so this test is
directly comparable to the #2/#4/#6 masked-lever results.

- **Baseline (control)**: CLEAN `armB`, 250 feat (same artifact as the
  masked-lever doc).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`,
  no early-stop, `cat_indices=[]`.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: `seed_base in {42, 101, 2026}`.
- **Metrics / significance / gate**: identical to the masked-lever doc
  (exact-ordinal top1/place2..place6, top3_box, fukusho_2p; primaries =
  `{top1, place2, place3}`; paired race-level bootstrap, 2000 iter, seed
  20260519; gate = ≥2/3 primaries `delta_pp>=+0.08` AND `LB95>0`, ≥1 of
  `{place2,place3}`, no metric `< -0.05pp`).
- **Cell eval**: the standard single-dim cut (`keibajo_code`/`kyori_band`/
  `season_band`/`current_baba_condition`, `n>=200`, using the already-fixed
  mask-alignment code) plus a purpose-built cross-cut
  (`tmp/candidate-masked-lever-retest/tokubetsu_crosscut.py`) for the
  grade-code / summer×grade cells the task specifically targets — `grade_code`
  and `keibajo_code` are both race-constant, so (unlike the draw/waku
  cross-cut) there's no per-horse anchoring ambiguity to worry about.

### Candidate columns

All 4 are pure per-row transforms of columns already present in the offline
store (`grade_code`, `tansho_ninkijun`, `shusso_tosu`, `keibajo_code`) — no
Postgres join, no window function, no leakage risk (all known at serve
time). Confirmed first that armB's existing `is_grade_race` flag is defined
as `grade_code in (A,B,C,D,G,H)` — **E is deliberately excluded from that
flag** (`finish_position_features_duckdb.py:2464`), so `is_tokubetsu` is a
genuinely new signal, not a duplicate of an existing armB feature.

- `is_tokubetsu` = `grade_code (trimmed) == 'E'` (132,417 / 626,798 store
  rows are E-grade, ~21%, plenty of training signal).
- `ninkijun_norm` (helper, not a standalone feature) = `(tansho_ninkijun-1) /
(shusso_tosu-1)`.
- `mkt_rank_x_tokubetsu` = `ninkijun_norm * is_tokubetsu`.
- `mkt_rank_x_summer_tokubetsu` = `ninkijun_norm * is_tokubetsu *
is_summer_venue(keibajo_code in 01/02/03/10)`.
- `favorite_x_tokubetsu` = `(tansho_ninkijun==1) * is_tokubetsu`.

Coverage: 100% in all 3 folds (all 4 ingredient columns are populated for
essentially every row — `tansho_ninkijun` null rate is 0.9% store-wide,
`grade_code`/`shusso_tosu` 0%).

## Result: **REJECT**

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.954 | +0.158     | -0.019 |
| place2     | 18.119 | 18.344 | +0.225     | +0.022 |
| place3     | 14.163 | 14.337 | +0.174     | -0.042 |
| place4     | 12.166 | 12.182 | +0.016     | -0.196 |
| place5     | 11.076 | 10.815 | -0.261     | -0.454 |
| place6     | 10.416 | 10.487 | +0.071     | -0.132 |
| top3_box   | 9.410  | 9.500  | +0.090     | -0.019 |
| fukusho_2p | 74.912 | 74.909 | -0.003     | -0.161 |

Gate: `primaries_passed={top1:false, place2:true, place3:false}`,
`n_primaries_passed=1/3`, `place2_or_place3=true`, `worst_delta=-0.261`
(place5, exceeds the `-0.05` no-reg bound) → **ACCEPT_strict_gate=false**.
This is the closest any of the 4 levers tested in this campaign (#2, #4, #6,
this one) has come to passing — `place2` genuinely clears `LB95>0` — but it
fails on two independent grounds: only 1/3 primaries (need ≥2), and place5
regresses well past the no-reg floor in every single fold (see below), so
even the campaign's more lenient "any primary improves + no regression"
policy (`feedback_incremental_gains_accept_gate`) doesn't clear it.

Per-fold (delta_pp [LB95]):

| Fold | top1           | place2             | place3             | place5             |
| ---- | -------------- | ------------------ | ------------------ | ------------------ |
| 2023 | +0.174[-0.135] | -0.077[-0.444]     | +0.164[-0.251]     | **-0.473[-0.849]** |
| 2024 | +0.232[-0.077] | **+0.714[+0.319]** | **+0.405[+0.029]** | -0.183[-0.492]     |
| 2025 | +0.068[-0.241] | +0.039[-0.309]     | -0.048[-0.405]     | -0.125[-0.482]     |

**Regime-stability check (the caution flagged in the task brief): confirmed
unstable.** Both primaries that cross LB95>0 in the pooled figure do so
_only_ in the 2024 fold — 2023 and 2025 are flat-to-negative on place2/place3.
This isn't a sign-flip like the odds-entropy feature's 2024 backfire, but it
is the same underlying problem: the pooled "signal" is one blind year
carrying the average, not a 3-fold-replicating effect. Per
`feedback_hpo_selection_bias_blind_holdout`, a signal this concentrated in a
single fold needs independent confirmation before being trusted, and here 2
of the 3 folds don't show it at all. Separately, **place5 regresses beyond
the `-0.05` no-reg bound in all 3 folds** (worst -0.473 in 2023) — a
consistent, not fold-noise, drawback.

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.367[+0.087] | +0.338[-0.039] | +0.270[-0.097] |
| 101  | +0.010[-0.289] | +0.232[-0.145] | +0.299[-0.077] |
| 2026 | +0.097[-0.212] | +0.106[-0.261] | -0.048[-0.444] |

Unlike #2/#4/#6, there's **no sign-flip across seeds** here — all 3 seeds
show positive top1 and place2, 2/3 show positive place3. This is a
meaningfully more consistent direction than the other 3 rejected levers, and
seed42 alone clears `top1 LB95>0`. Taken together with the per-fold result,
the honest read is: there is probably a small real effect here, but it's
concentrated in specific folds/seeds rather than uniformly present, and (see
below) it isn't the effect the task was looking for.

## The direct-target cell: does it fix the SUMMER × E-grade hot pocket?

This is the decisive result. Cross-cut script
`tmp/candidate-masked-lever-retest/tokubetsu_crosscut.py`, report
`tmp/candidate-masked-lever-retest/reports/tokubetsu_mkt_crosscut.json`
(global sanity-check row matches the pooled table above exactly).

| Cell                         | n    | Δtop1 [LB95]        | Δplace2 [LB95]      | Δplace3 [LB95]      |
| ---------------------------- | ---- | ------------------- | ------------------- | ------------------- |
| **SUMMER × E_tokubetsu**     | 577  | **-0.116 [-0.809]** | **-0.289 [-1.157]** | **-0.404 [-1.502]** |
| SUMMER × ordinary            | 1807 | +0.092 [-0.351]     | +0.314 [-0.185]     | +0.277 [-0.277]     |
| NON_SUMMER × E_tokubetsu     | 1609 | +0.352 [-0.104]     | +0.187 [-0.373]     | -0.062 [-0.601]     |
| NON_SUMMER × ordinary        | 5763 | +0.150 [-0.069]     | +0.295 [+0.017]     | +0.295 [-0.035]     |
| class=E_tokubetsu (JRA-wide) | 2186 | +0.229 [-0.122]     | +0.061 [-0.412]     | -0.153 [-0.640]     |
| class=ordinary (JRA-wide)    | 7570 | +0.137 [-0.062]     | **+0.299 [+0.057]** | **+0.291 [+0.044]** |

**`SUMMER × E_tokubetsu` — the exact cell this feature was designed to
help — is negative on all 3 primaries** (though at n=577 none individually
crosses LB95 significance, the direction is uniform and the opposite of the
hypothesis). Meanwhile **`class=ordinary` (JRA-wide, n=7,570, i.e.
_non_-tokubetsu races) is where both place2 and place3 actually clear
LB95>0** — the pooled "win" in the headline table is being driven almost
entirely by ordinary races and non-summer cells, not by tokubetsu races and
not by summer venues. This is essentially the inverse of the intended
mechanism: the hypothesis was "let the ranker discount market rank
specifically where the market is less reliable (tokubetsu, summer)" — what
actually happened is the model found some generic, unrelated
market-rank-conditional pattern in ordinary/non-summer races, while the
tokubetsu/summer segment it was built for got slightly worse.

## Standard cell cut (`keibajo_code`, all grades pooled)

The 4 summer venues are mixed, not uniformly positive: `01` Sapporo (n=504)
top1 **+1.124 [LB95 +0.265]** (significant, positive); `03` Fukushima
(n=720) place2 **+1.065 [LB95 +0.278]** (significant, positive); `10` Kokura
(n=792) flat/slightly negative (top1 -0.210[-0.800], place3
-0.168[-0.968]); `02` **Hakodate (n=432) place2 -1.698 [LB95 -2.857]** — a
large, clearly-significant **regression**, the worst single-cell number in
this whole campaign. One non-summer venue (`07`, n=1,128) also shows
place2/place3/top3_box all LB95>0. So even restricting to venue-level (not
grade-conditioned) cells, the picture is 2 summer venues up, 1 flat, 1 down
hard — not a clean "fixes the summer problem" story, and directly
contradicted once you condition on grade (previous section).

## Conclusion

**REJECT.** Three independent reasons, each sufficient on its own:

1. **Gate fails outright**: 1/3 primaries (not the required ≥2/3), and
   place5 regresses past the no-reg floor in all 3 folds.
2. **Regime instability, exactly the concern flagged in the task brief**:
   the pooled place2/place3 "signal" is carried entirely by the 2024 fold;
   2023 and 2025 don't show it. This is the categorical-feature analog of
   the odds-entropy feature's fold-dependent behavior that got it REJECTed
   on 2026-06-23 — same failure mode, not resolved by using a categorical
   instead of continuous formulation.
3. **Mechanism mismatch, the most important finding**: the direct-target
   cell (`SUMMER × E_tokubetsu`, n=577) trends negative on all 3 primaries —
   this feature does not fix the diagnosed hot pocket. Whatever positive
   signal exists pools mostly from `ordinary`-class and `NON_SUMMER` races,
   the _opposite_ segment from the one the feature was built to help, and
   one of the 4 target venues (Hakodate) shows a large, significant
   regression. The E-grade summer upset gap remains real (confirmed again
   here descriptively) but this feature construction does not close it —
   consistent with the originating doc's own framing that this looks like a
   market-inefficiency/calibration issue rather than something a training
   feature can fix, now with actual WF evidence rather than just a
   plausibility argument.

**DO-NOT-RETEST** this exact construction. If E-grade summer upsets are
revisited, the originating doc's alternative framing (a
display/calibration-layer confidence-shrinkage on the model's #1 pick,
specifically for E-grade races at summer venues, rather than a training
feature) remains the more promising unexplored angle — consistent with this
result, since a training-time interaction feature demonstrably didn't help
the target cell.

## Artifacts

- Harness (extended): `tmp/candidate-masked-lever-retest/retest_wf.py`
  (added `build_tokubetsu()`, `SUMMER_VENUE_CODES`, 3 new `extras` columns in
  `load_store()`, `tokubetsu_mkt` registered in `build_candidate()`)
- Cross-cut: `tmp/candidate-masked-lever-retest/tokubetsu_crosscut.py`
- Reports: `tmp/candidate-masked-lever-retest/reports/tokubetsu_mkt.json`,
  `tmp/candidate-masked-lever-retest/reports/tokubetsu_mkt_crosscut.json`
- Log: `tmp/candidate-masked-lever-retest/tokubetsu_mkt.log` (671.7s — base
  models reused from the masked-lever campaign's cache, only 9 new
  `tokubetsu_mkt` models trained)
- Related: `jra-summer-upset-divergence-2026-07-04.md` (originating
  diagnosis), `jra-masked-lever-clean-retest-2026-07-04.md` (shared harness +
  the cell-mask-alignment fix this test relies on)
