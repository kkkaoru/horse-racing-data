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
verdict. Only cell with a positive-LB95 primary: `keibajo_code=05` (Tokyo,
n=1,607) `top1 +0.622[LB95 +0.187]`, but `place2/place3` at that same cell are
flat/negative (`place2 -0.021[-0.581]`, `place3 +0.477[-0.083]`) — a single
metric at a single cell among ~30 tested cells, consistent with
multiple-comparison noise per the eval-rules caution, not cell-conditional
adoption evidence.

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
all-negative) — this is noise around zero, not a real effect. No cell
(`keibajo_code`/`kyori_band`/`season_band`/`current_baba_condition`, n≥200)
had any primary with LB95>0.

**Conclusion**: clean baseline does NOT unmask this lever either — it
confirms the original probe's EARLY-REJECT verdict (draw fitness vanishes
after odds control) now at the full model-gate level too. The effect is
genuinely absent, not masked by the leak. **DO-NOT-RETEST.**

## Lever #6 — draw (枠) ablation (`draw_ablation`)

TBD

## Overall conclusion

TBD

## Artifacts

- Harness: `tmp/candidate-masked-lever-retest/retest_wf.py`
- Draw-zone feature rebuild: `tmp/candidate-masked-lever-retest/build_draw_ablation.py`
  (rewritten mid-run: v1 used a horse-level self-join that is O(n²) per venue
  and drove host free memory from ~13GB to ~110MB before being killed; v2
  uses expanding window functions over pre-aggregated race-level rows — same
  pattern as `build_sameday` — 848,838 input rows → parquet in 0.7s, 100%
  coverage)
- Reports: `tmp/candidate-masked-lever-retest/reports/{sameday_bias,draw_affinity,draw_ablation}.json`
- Logs: `tmp/candidate-masked-lever-retest/{sameday_bias,draw_affinity,draw_ablation}.log`
- Not re-verified in this pass (out of scope / lower priority per task
  instructions, backfill-dependent): #1 H-RS-KEIBAJO-IMPUTE (NAR),
  #3 pace-style×distance-band fit (rs*p*_ backfill), #5 relationship rs*p*_
  (rs*p*\_ backfill), #7 score-additive draw+speed (subsumed by #6 as a
  GBTfeature test per the reject-list's own `masked_note`). NAR same-day
  track-bias (original test covered both JRA/NAR) was also not re-run here —
  this harness is JRA-only (`tmp/candidate-eval-jra/augmented` store); a NAR
  clean baseline exists (`tmp/candidate-leak-clean-retrain/nar\__`) and could
  be retested with a parallel NAR harness if warranted.
