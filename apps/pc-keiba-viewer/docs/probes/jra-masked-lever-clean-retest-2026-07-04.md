# JRA Masked-Lever Clean-Baseline Re-Test (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **Trigger**: `target_corner_leak` discovery (see `project_target_corner_leak_2026_07_04` memory /
  commit history around 2026-07-04) — the deployed JRA/NAR baselines carried 4
  within-race leak columns (`target_corner_1_norm` / `target_corner_3_norm` /
  `target_corner_4_norm` / `target_running_style_class`, the horse's OWN
  in-race corner position / running-style FOR THE RACE BEING PREDICTED, always
  NULL at serve). Clean-retrain (leak cols excluded) improved serve accuracy
  JRA +0.85pp / NAR +4.4pp. Because the leak spuriously supplied the model the
  actual within-race corner order, any lever that supplies pace / draw /
  front-bias / running-position signal would have looked "redundant" when
  gated against the LEAKED baseline — its REJECT verdict is suspect. This doc
  re-verifies the fastest, backfill-free masked-lever candidates from
  `tmp/candidate-reject-list/REJECT-LIST.md` TOP-7 (#2, #4, #6) on the CLEAN
  baseline.

## Method

- **Harness**: `tmp/candidate-masked-lever-retest/retest_wf.py`
- **Baseline (control, both arms)**: CLEAN `armB` from
  `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` — 250 feat,
  the live `jra-cb-v9-sim-2013` spec (254→250: the 4 leak cols removed) minus
  9 cols missing from the eval store. No leak cols in either arm (both
  leak-free by construction — no null-at-serve masking needed, unlike the
  A_serve vs B_clean leak-removal comparison itself).
- **Treatment**: control + lever-specific candidate columns (additive).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`, no
  early-stop, `cat_indices=[]` (all-numeric), matches deployed
  `jra-cb-v9-sim-2013` exactly.
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}` (`seed = seed_base +
fold_year` per fold), pooled via per-race hit-rate averaging across seeds
  before the paired bootstrap (so the reported delta/LB95 is already
  seed-stable, not just single-seed noise).
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box` (set
  equality of predicted vs actual top-3), `fukusho_2p` (any predicted top-2
  finished ≤2). Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment − control`.
- **Accept gate** (`docs/finish-position-prediction-system.md` §7.2): ≥2 of 3
  primaries have `delta_pp >= +0.08` AND `LB95 > 0`; AND ≥1 of
  `{place2, place3}` passes; AND no metric regresses below `-0.05pp`
  (`GATE_NO_REG`).
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` / `kyori_band` /
  `season_band` / `current_baba_condition`, `n >= 200` per cell (multiple-
  comparison caution applied — a single positive cell among ~30+ tested is
  not itself adoption evidence).

### Methodology correction found during the Lever #6 pass (applies to all 3 levers)

While evaluating Lever #6's cell cut, a mask-alignment bug was found in
`retest_wf.py`'s cell loop: `paired()` internally does
`base_hits.sort("race_id")` before joining and applying the boolean `mask`
positionally, but the call sites built `mask` from `ba`/`ca` straight out of
`group_by("race_id")` — whose row order Polars does **not** guarantee sorted
(confirmed with a minimal repro: `df.group_by(...).agg(...)` reordered rows).
Any masked (per-cell) result was therefore silently applying the mask to the
wrong rows after `paired()`'s internal sort. **Pooled / per-fold / per-seed
numbers are unaffected** (those calls pass `mask=None`, so the only join key
is `race_id` itself, done correctly). Fix: sort `ba`/`ca` by `race_id`
immediately after the `dims` join, before computing any cell mask (see
`retest_wf.py`'s `cells_ge200_seedavg` block and
`summer_waku_crosscut.py`). All three levers' cell sections were
regenerated with the fix — predict-only against the already-trained models
(no retraining needed, ~15s each) — so every cell number in this doc
(including the corrected #2/#4 claims below) reflects the fixed harness.
The buggy run had produced a spurious "3 of 4 summer venues positive" pattern
for Lever #6 that vanished entirely once fixed (see Lever #6 below) — a
useful reminder that cell-level "signal" needs this kind of sanity check
before being trusted.

## Lever #2 — same-day dynamic track bias (`sameday_bias`)

**Original REJECT** (2026-07-02, LEAKED baseline): `sameday_inside_bias` /
`sameday_front_bias` (venue-baseline residual) / `sameday_prior_race_count`
from same-day `race_bango < N` races. JRA gate 0/3 + no-reg fail.
**masked_lever=Y** — `track_bias_front` = pace/front-running advantage,
`track_bias_inside` = draw advantage, both axes a corner-order leak would
spuriously duplicate.

**Candidate columns** (`tmp/candidate-masked-lever-retest/retest_wf.py::build_sameday`):
leak-free, strictly same-day `race_bango < current` races only (own race never
included); residualized against a TRAIN-only per-venue baseline (recomputed
per fold to avoid using future-fold venue means). Coverage ~91.8-91.9% across
folds (races 1 of the day are NULL, as expected).

### Clean-baseline result: **REJECT** (unchanged verdict)

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.915 | +0.119     | -0.064 |
| place2     | 18.119 | 18.164 | +0.045     | -0.167 |
| place3     | 14.163 | 14.031 | -0.132     | -0.367 |
| place4     | 12.166 | 11.983 | -0.183     | -0.412 |
| place5     | 11.076 | 10.835 | -0.241     | -0.444 |
| place6     | 10.416 | 10.487 | +0.071     | -0.125 |
| top3_box   | 9.410  | 9.342  | -0.068     | -0.174 |
| fukusho_2p | 74.912 | 74.864 | -0.048     | -0.212 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.241`
(place5, exceeds `-0.05` no-reg bound) → **ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | +0.029[-0.280] | -0.232[-0.579] | -0.145[-0.598] |
| 2024 | +0.251[-0.049] | +0.376[+0.010] | +0.116[-0.270] |
| 2025 | +0.077[-0.251] | -0.010[-0.357] | -0.367[-0.733] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.135[-0.164] | +0.145[-0.232] | -0.212[-0.608] |
| 101  | +0.125[-0.174] | +0.019[-0.328] | -0.125[-0.521] |
| 2026 | +0.097[-0.232] | -0.029[-0.396] | -0.058[-0.444] |

top1 is consistently weakly positive but never LB95>0; place3 is
consistently negative (2/3 folds, 3/3 seeds). No fold or seed flips the
verdict. **[corrected after the mask-alignment fix above]** Only cell with a
positive-LB95 primary: `current_baba_condition=4` (heavy/不良 going, n=347)
`place2 +0.961[LB95 +0.096]`, with `top1 +0.384[-0.288]` flat and `place3
-0.865[-2.690]` negative at that same cell — a single metric at a single
cell among ~22 tested cells, consistent with multiple-comparison noise per
the eval-rules caution, not cell-conditional adoption evidence. (The
originally-reported `keibajo_code=05` Tokyo cell was a mask-alignment
artifact and does not replicate post-fix: corrected `top1 +0.207[-0.249]`,
not significant.)

**Conclusion**: the clean baseline does NOT unmask this lever. Verdict is
unchanged from the leaked-baseline REJECT — CatBoost depth=8 already captures
same-day positional bias through its existing `track_bias_inside` /
`track_bias_front` (5-day window) + venue/weather/track_condition features,
independent of whether the target_corner leak was present. **DO-NOT-RETEST.**

## Lever #4 — horse-draw-affinity (`draw_affinity`)

**Original REJECT** (2026-07-02, probe-only, never reached full WF):
`draw_affinity_signal` / `horse_inside_edge` (per-horse conditional
inside-vs-outside top3-rate edge, `n_prior>=10`, coverage 49%).
**EARLY-REJECT at odds-controlled partial-Spearman probe**: `|ρ|<=0.0073`,
wrong-signed, mixed years. **masked_lever=Y** — draw predicts early-corner
position, which a corner-order leak would spuriously duplicate. This is the
first time this candidate reaches a full model-level WF gate (the original
probe never got promoted).

**Candidate columns** (`tmp/candidate-batch-probe/draw_affinity.parquet`,
`build_draw_affinity()`): leak-free, strictly-prior (`b.rdt < a.rdt` or
same-day earlier `race_bango`) per-horse inside-vs-outside top3-rate
difference, requiring `n_prior>=5, n_in>=2, n_out>=2`. Coverage 48.6-49.9%
across folds (new/thin-history horses NULL, as expected for a per-horse
statistic).

### Clean-baseline result: **REJECT**

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.861 | +0.064     | -0.116 |
| place2     | 18.119 | 18.096 | -0.023     | -0.235 |
| place3     | 14.163 | 14.134 | -0.029     | -0.244 |
| place4     | 12.166 | 12.188 | +0.023     | -0.206 |
| place5     | 11.076 | 11.037 | -0.039     | -0.241 |
| place6     | 10.416 | 10.526 | +0.109     | -0.090 |
| top3_box   | 9.410  | 9.439  | +0.029     | -0.077 |
| fukusho_2p | 74.912 | 74.912 | +0.000     | -0.161 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.039`
(within the `-0.05` no-reg bound, unlike sameday_bias — this lever is flat,
not harmful) → **ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.116[-0.444] | -0.328[-0.743] | -0.289[-0.714] |
| 2024 | +0.164[-0.154] | +0.405[+0.029] | +0.280[-0.058] |
| 2025 | +0.145[-0.154] | -0.145[-0.511] | -0.077[-0.453] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.164[-0.145] | +0.097[-0.309] | +0.029[-0.328] |
| 101  | -0.077[-0.396] | -0.039[-0.425] | -0.010[-0.367] |
| 2026 | +0.106[-0.212] | -0.125[-0.502] | -0.106[-0.492] |

Sign flips fold-to-fold and seed-to-seed (2023 negative across the board,
2024 positive across the board, 2025 mixed; seed42 all-positive, seed101
all-negative) — this is noise around zero, not a real effect.
**[corrected after the mask-alignment fix above]** Two cells reach a
positive-LB95 top1 (single metric each, `place2`/`place3` flat/negative at
both): `keibajo_code=05` (Tokyo, n=1,607) `top1 +0.498[LB95 +0.042]`, and
`keibajo_code=08` (n=1,535) `top1 +0.434[LB95 +0.022]`. Both LB95 margins are
thin (< 0.05pp above zero) and neither is corroborated by place2/place3 at
the same cell — consistent with multiple-comparison noise among ~22 tested
cells, not cell-conditional adoption evidence (originally this doc reported
"no cell had LB95>0", which was itself wrong due to the same bug — the
corrected picture is marginal single-metric noise, not zero, but still well
short of adoption).

**Conclusion**: clean baseline does NOT unmask this lever either — it
confirms the original probe's EARLY-REJECT verdict (draw fitness vanishes
after odds control) now at the full model-gate level too. The effect is
genuinely absent, not masked by the leak. **DO-NOT-RETEST.**

## Lever #6 — draw (枠) ablation (`draw_ablation`)

**Original REJECT** (2026-06-20, LEAKED baseline): `wakuban` + venue×dist draw
advantage + strength (see `tmp/candidate-reject-list/reject-list.json` rank
6). `top1 +0.077pp`, `fukusho_2p LB95 -0.193` → REJECT, reasoned "CatBoost
depth=8 already captures draw bias implicitly (umaban × keibajo_code × kyori
× track_code interaction)". A companion post-hoc score-additive draw+speed
override (rank 7, `project_score_additive_draw_speed_reject_2026_06_20`) also
REJECTed for an unrelated mechanistic reason (coarse post-hoc swap couldn't
beat the base #1) and is subsumed here per the reject-list's own
`masked_note` — this section is the GBDT-feature re-test for both #6 and #7.
**masked_lever=Y** — draw position is a primary determinant of early-corner
position, which a corner-order leak would spuriously duplicate, hiding any
independent draw signal.

**Candidate columns** (`tmp/candidate-masked-lever-retest/build_draw_ablation.py`,
rebuilt from the original 2026-06-20 probe doc since the original build
script was ephemeral/uncommitted): `wakuban_norm` (0-1 normalized gate
position within race), `draw_zone_venue_edge` (as-of top3-rate for this
draw-zone tertile at this venue, minus as-of venue-pooled top3-rate),
`draw_zone_venue_dist_edge` (same, venue × distance-band specific). All
strictly as-of (expanding window, own race excluded) — 100% coverage in
validation (v2 rebuild fixed a coverage bug in an earlier attempt that only
reached ~26%; see Artifacts). No horse-level self-join (the original v1
rebuild attempt was O(n²) per venue and drove host free memory from ~13GB to
~110MB before being killed — rewritten to race-level pre-aggregation +
expanding window functions, same pattern as `build_sameday`).

### Clean-baseline result: **REJECT** (unchanged verdict)

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.825 | +0.029     | -0.161 |
| place2     | 18.119 | 18.273 | +0.154     | -0.052 |
| place3     | 14.163 | 14.256 | +0.093     | -0.135 |
| place4     | 12.166 | 12.102 | -0.064     | -0.270 |
| place5     | 11.076 | 10.944 | -0.132     | -0.338 |
| place6     | 10.416 | 10.490 | +0.074     | -0.129 |
| top3_box   | 9.410  | 9.464  | +0.055     | -0.061 |
| fukusho_2p | 74.912 | 74.996 | +0.084     | -0.100 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.132`
(place5, exceeds the `-0.05` no-reg bound) → **ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | -0.251[-0.588] | -0.116[-0.511] | +0.068[-0.357] |
| 2024 | +0.222[-0.116] | +0.405[+0.019] | +0.405[+0.019] |
| 2025 | +0.116[-0.193] | +0.174[-0.193] | -0.193[-0.598] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.164[-0.154] | +0.415[+0.048] | +0.241[-0.154] |
| 101  | -0.222[-0.540] | -0.116[-0.492] | +0.068[-0.328] |
| 2026 | +0.145[-0.174] | +0.164[-0.212] | -0.029[-0.425] |

Same noise signature as #2/#4: sign flips fold-to-fold (2023 negative,
2024 uniformly positive with place2/place3 both crossing LB95>0 for that
fold alone, 2025 mixed) and seed-to-seed (seed42 all-positive including a
significant place2, seed101 mostly negative, seed2026 mixed) — no fold or
seed makes the pooled verdict flip, and the one seed/fold combination that
looks promising (seed42's place2, 2024's place2/place3) doesn't replicate
across the other two seeds/folds. This is the same pattern that sank
sameday_bias and draw_affinity.

**Cell cut** (post mask-alignment-fix, see methodology note above):
`keibajo_code` — **none of the 3 flagged summer venues reach LB95>0 on any
primary**: `01` Sapporo (n=504) top1 +0.60[-0.40] / place2 +0.07[-0.99] /
place3 +0.40[-0.79]; `03` Fukushima (n=720) top1 -0.23[-0.97] / place2
+0.28[-0.51] / place3 -0.28[-1.34]; `10` Kokura (n=792) top1 +0.04[-0.55] /
place2 +0.34[-0.42] / place3 -0.13[-1.01] — all flat/not-significant. Two
non-summer venues show a single-metric LB95>0 (`07` place3 +0.827[+0.207]
n=1,128; `08` place2 +0.586[+0.043] n=1,535, consistent with
multiple-comparison noise). The one cell worth flagging as a pattern (not
adoption evidence) is `season_band=3` (n=2,508): place2 +0.638[+0.172],
place3 +0.532[+0.067], top3_box +0.306[+0.079] — 3 of 5 metrics
simultaneously LB95>0, more than any other single cell in this campaign, but
still one cell among ~22 tested and those 3 metrics are highly correlated by
construction (not 3 independent confirmations) — logged for future reference
if a season-specific investigation is opened, not actioned here.

**Summer-venue × inside-waku targeted cross-cut** (does draw_ablation fix
the diagnosed "inside-waku (1-2) rank-1 overconfidence at Kokura/Sapporo/
Fukushima" defect from `jra-summer-venue-cell-focus-2026-07-04.md`?): script
`tmp/candidate-masked-lever-retest/summer_waku_crosscut.py`, report
`tmp/candidate-masked-lever-retest/reports/draw_ablation_summer_waku_crosscut.json`.
Method: fixed cohort = races where the **base arm's** own #1 pick is drawn
inner_1to2 (wakuban<=2) at a given venue (mirrors the diagnosis's own
race-level bucketing convention), then compares base vs cand hit-rates on
that identical race subset (paired, not re-anchored per arm — global sanity
check row matches the pooled result above exactly, confirming the join is
correct).

| Cohort                    | n    | top1 (base→cand) | Δtop1 (pp) [LB95] | Δplace2 [LB95]  | Δplace3 [LB95]  |
| ------------------------- | ---- | ---------------- | ----------------- | --------------- | --------------- |
| ALL_SUMMER × inner_1to2   | 522  | 26.76% → 26.63%  | -0.128 [-1.022]   | +0.128 [-0.894] | +0.319 [-0.766] |
| sapporo × inner_1to2      | 83   | 22.89% → 20.48%  | -2.410 [-5.221]   | +0.000 [-3.213] | -0.803 [-3.213] |
| fukushima × inner_1to2    | 170  | 27.45% → 27.84%  | +0.392 [-0.784]   | +0.980 [-0.588] | +1.373 [-0.392] |
| kokura × inner_1to2       | 171  | 24.56% → 24.76%  | +0.195 [-0.975]   | -0.195 [-1.754] | -0.195 [-2.534] |
| OTHER_VENUES × inner_1to2 | 1515 | 34.54% → 34.68%  | +0.132 [-0.374]   | -0.154 [-0.770] | -0.022 [-0.616] |

The base-only numbers confirm the diagnosed deficit is real (26.76% top1 at
ALL_SUMMER×inner_1to2 vs the 33.8% JRA-wide baseline, a ~7pp hole) — but
`draw_ablation` does **not** close it: no cohort reaches LB95>0 on any
primary, the pooled summer figure trends slightly negative on top1, and the
one venue with a small n (Sapporo, n=83) trends clearly negative (though too
noisy at that sample size to read as a real harm). Fukushima's inner-waku
subset is the closest to a positive signal (place3 +1.37pp) but still
doesn't cross LB95>0. **Verdict: this lever does not fix the summer-venue
inside-waku overconfidence problem** — that defect looks like genuine
model overconfidence (calibration), not a missing draw feature, consistent
with the diagnosis doc's own framing ("rank-1-pick overconfidence
conditional on venue... not a missing feature").

**Conclusion**: clean baseline does NOT unmask this lever — same verdict as
the leaked baseline (`top1 +0.077pp` there vs `+0.029pp` here, both far
below the +0.08pp gate floor and both without any LB95>0 primary), and the
targeted summer-venue cross-cut rules out a cell-conditional adoption case
too. CatBoost depth=8 continuing to capture draw bias implicitly (the
original 2026-06-20 reasoning) holds up post-leak-removal. **DO-NOT-RETEST**
(also closes out rank 7's score-additive variant, subsumed here).

## Overall conclusion

**All 3 re-tested levers (#2, #4, #6) REJECT on the clean baseline. None
show the "unmasking" pattern the `target_corner` leak hypothesis predicted.**

Masking quantification (leaked-baseline delta vs clean-baseline delta, top1
unless noted):

| #   | Lever         | Leaked-baseline result                                                                    | Clean-baseline result                                                           | Masked?                                                                                                                                                                   |
| --- | ------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 2   | sameday_bias  | Qualitative REJECT only (JRA gate 0/3 + no-reg fail; no `delta_pp` recorded)              | top1 +0.119pp [LB95 -0.064], 0/3 primaries                                      | **No** — both REJECT decisively; no `delta_pp` to compare numerically but neither shows adoption-level signal                                                             |
| 4   | draw_affinity | Never reached full WF gate (probe-only EARLY-REJECT at partial-Spearman, `\|ρ\|<=0.0073`) | top1 +0.064pp [LB95 -0.116], 0/3 primaries                                      | **No** — this is the _first_ full-model-gate test of this lever at all; it confirms the probe-level early-reject rather than reversing it                                 |
| 6   | draw_ablation | top1 +0.077pp, `fukusho_2p` LB95 -0.193 (full WF)                                         | top1 +0.029pp [LB95 -0.161], `fukusho_2p` +0.084pp [LB95 -0.100], 0/3 primaries | **No** — the clean-baseline effect is _smaller_, not larger; if the leak had been masking a real draw signal, removing it should have made the effect bigger, not smaller |

None of the three deltas moved in the direction the masking hypothesis
predicted (clean > leaked). All three remain firmly below the +0.08pp gate
floor with no LB95>0 primary in the pooled evaluation, across 3 folds × 3
seeds each (18 independent train/eval combinations total). The recurring
signature — small positive top1 that never clears LB95, place2/place3 that
flip sign fold-to-fold and seed-to-seed — is consistent with all three
levers measuring noise around a true effect of zero, not a real but masked
signal. **CatBoost depth=8's existing draw/pace/position feature set
(`wakuban`, `track_bias_inside`/`track_bias_front`, venue/distance/track
interactions already in the 250-feat armB) already captures whatever signal
these candidate columns would add**, independent of whether the
`target_corner` leak was present — the original 2026-06-20/07-02 REJECT
reasoning holds up post-leak-removal for all three.

The one place a genuinely new (not previously tested) question was asked —
"does draw_ablation specifically fix the diagnosed inside-waku
overconfidence at Kokura/Sapporo/Fukushima" — got a clean negative answer
via the targeted cross-cut above, closing that follow-up from the parallel
summer-venue diagnosis: the deficit is real but is a calibration/overconfidence
problem, not a missing-feature problem, so a feature-engineering lever
(inside-waku popularity/edge signal) was never going to fix it.

**Process note**: a real mask-alignment bug (see methodology section above)
was found and fixed mid-campaign; it had corrupted every cell-level number
produced before the fix (not the pooled/per-fold/per-seed numbers, which
were always correct). Two previously-written cell claims in this doc (#2's
Tokyo cell, #4's "no cell" claim) were retracted and replaced with the
corrected numbers. This doesn't change any lever's REJECT verdict (those
were always driven by the pooled gate, not cell results), but it's a
reminder to treat single-cell "signal" in this campaign skeptically until
cross-checked — exactly the multiple-comparison caution this doc already
applied is compounded by the risk of a silent computation bug, and the
"3 of 4 summer venues positive" pattern this session initially found (before
the fix) is a concrete example of how convincing-looking but spurious
cell-level signal can appear.

**Combined with Levers #2/#4 (also DO-NOT-RETEST)**: the JRA masked-lever
TOP-7 backlog now stands at 3/7 closed (all REJECT) via the fast,
backfill-free path. The remaining 4 — #1 (NAR H-RS-KEIBAJO-IMPUTE), #3
(pace-style × distance-band, JRA/NAR), #5 (relationship rs*p*_, JRA/NAR) —
require `rs*p*_` multi-year backfill or a NAR clean-baseline harness and
were out of scope for this pass (see Artifacts).

## Artifacts

- Harness: `tmp/candidate-masked-lever-retest/retest_wf.py`
- Draw-zone feature rebuild: `tmp/candidate-masked-lever-retest/build_draw_ablation.py`
  (rewritten mid-run: v1 used a horse-level self-join that is O(n²) per venue
  and drove host free memory from ~13GB to ~110MB before being killed; v2
  uses expanding window functions over pre-aggregated race-level rows — same
  pattern as `build_sameday` — 848,838 input rows → parquet in 0.7s, 100%
  coverage)
- Reports: `tmp/candidate-masked-lever-retest/reports/{sameday_bias,draw_affinity,draw_ablation}.json`
  (all 3 regenerated with the mask-alignment fix; pooled/per-fold/per-seed
  sections identical to the pre-fix runs, `cells_ge200_seedavg` corrected)
- Logs: `tmp/candidate-masked-lever-retest/{sameday_bias,draw_affinity,draw_ablation}.log`
  (original, pre-fix runs) and `*_fixed.log` (post-fix cell-only reruns,
  ~15s each, predict-only against the already-trained models)
- Summer-venue × inside-waku targeted cross-cut:
  `tmp/candidate-masked-lever-retest/summer_waku_crosscut.py`, report
  `tmp/candidate-masked-lever-retest/reports/draw_ablation_summer_waku_crosscut.json`
- Mask-alignment bug fix: both `retest_wf.py`'s `cells_ge200_seedavg` block
  and `summer_waku_crosscut.py` now `.sort("race_id")` immediately after the
  `dims` join, before any cell mask is computed (see methodology note above)
- Not re-verified in this pass (out of scope / lower priority per task
  instructions, backfill-dependent): #1 H-RS-KEIBAJO-IMPUTE (NAR),
  #3 pace-style×distance-band fit (rs*p*_ backfill), #5 relationship rs*p*_
  (rs*p*\_ backfill), #7 score-additive draw+speed (subsumed by #6 as a
  GBTfeature test per the reject-list's own `masked_note`). NAR same-day
  track-bias (original test covered both JRA/NAR) was also not re-run here —
  this harness is JRA-only (`tmp/candidate-eval-jra/augmented` store); a NAR
  clean baseline exists (`tmp/candidate-leak-clean-retrain/nar\__`) and could
  be retested with a parallel NAR harness if warranted.
