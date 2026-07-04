# JRA Style-Rivalry / Lone-Escape Pace-Scenario Features (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position feature engineering
- **Task**: #16 (B-derived, venue × meetday × waku × running-style condition)
- **Hypothesis**: what matters is not a horse's own running style alone but its
  style's SCARCITY in the field — a lone front-runner (逃げ) gets an
  uncontested lead (real-world "single nige advantage" racing lore); many
  rivals sharing the same style split the trip advantage. Closers (差し)
  benefit from a hot, congested pace up front (pace-collapse mechanism),
  regardless of whether the horses actually contesting the pace are
  themselves sashi-type.

## Premise check (VERIFY step, before building candidates)

The assigning message's premise was: "the existing `field_*` cols include
self — that difference is the new information." **This is WRONG** — verified
against `src/scripts/finish-position-features/add-race-internal-features.py`
(the source of the `armB` 250-feat columns via
`tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`):

```sql
sum(coalesce(b.past_nige_rate_self, 0)) over race_partition - coalesce(b.past_nige_rate_self, 0)
  as field_nige_pressure,
```

`field_{nige,senkou,sashi,oikomi}_pressure` are **already sum-of-OTHERS**
(race-partition sum minus self) — self is already excluded. So the actual gap
vs. the existing 250-feat armB is narrower than the task framing suggested.
Auditing the existing columns (`self_nige_rate_minus_field_avg`,
`field_nige_candidate_count`, `field_has_pure_nige_horse`,
`past_style_x_field_pace_match`, `self_style_dominant_rate`,
`field_avg_style_concentration`, `field_style_diversity`, `rs_p_nige_x_field_pace`
— confirmed already present, all excluding self), the genuine remaining gaps
are:

1. No MEAN-of-others normalization exposed as a raw column (only used inside
   `self_nige_rate_minus_field_avg`, a _difference_, not the multiplicative
   lone-escape form below).
2. No rivalry count based on rivals' DOMINANT style (argmax over their own 4
   rates) — the existing `field_nige_candidate_count` only thresholds raw
   nige rate `> 0.4`, regardless of whether nige is that rival's dominant
   style, and has no senkou/sashi/oikomi analogue.
3. No CROSS-style front-congestion term — existing
   `past_style_x_field_pace_match` only sums SAME-style products
   (`self_nige*field_nige_pressure + self_senkou*field_senkou_pressure + ...`),
   missing e.g. `self_nige*field_senkou_pressure` (a nige horse cares about
   ALL front-type rivals, not just other nige horses).
4. No closer-benefits-from-hot-pace term paired against the correct causal
   driver (any front pressure), as opposed to the existing same-style-only
   `self_sashi*field_sashi_pressure` term.

Given this, the 4 candidates below are legitimately new signal, not exact
duplicates — but a smoke-test correlation check (below) shows non-trivial
overlap with existing columns, which tempers expectations.

## Candidate columns

All strictly-prior, per-horse, computed only from `past_*_rate_self` (self)
and race-partition aggregates of OTHER horses' `past_*_rate_self` (already
loaded `field_{nige,senkou}_pressure` armB columns, confirmed sum-of-others
above) — **no current-race outcome/corner data**, no race-level nige-count
constraint (forbidden per `feedback_no_race_level_nige_constraint` memory).

| Column                   | Formula                                                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| `lone_escape_score`      | `past_nige_rate_self * (1 - mean_of_OTHERS' past_nige_rate)`                                                                            |
| `style_rivalry_count`    | count of rivals whose DOMINANT past style (argmax of their own nige/senkou/sashi/oikomi rates) == self's dominant style (excludes self) |
| `front_congestion_x_own` | `(self nige+senkou rate) * (field nige+senkou pressure)` — cross-style front crowding                                                   |
| `sashi_room_score`       | `self sashi rate * (field nige+senkou pressure)` — closer benefits from others' hot front pace                                          |

Null handling: `past_{nige,senkou,sashi,oikomi}_rate_self` are perfectly
null-correlated (all 4 null together for ~40% of rows — first-time-out
horses with no prior starts) and sum to 1.0 (proper simplex) whenever
non-null (verified over 626,798 store rows, 0 mismatches). `lone_escape_score`
/ `style_rivalry_count` / `front_congestion_x_own` / `sashi_room_score` are
all NULL when self has no prior-style history (~39.5% coverage gap — CatBoost
treats NULL as informative-absent, standard for this store).

Dominant-style argmax tie-break order: nige > senkou > sashi > oikomi
(ties are effectively never hit with continuous rates; sanity-checked
`style_rivalry_count` is always in `[0, shusso_tosu]`, 0 violations).

Smoke-test correlation vs. nearest existing armB columns (377,267 complete
rows):

| Candidate                | vs `self_nige_rate_minus_field_avg` | vs `field_nige_candidate_count` | vs `past_style_x_field_pace_match` |
| ------------------------ | ----------------------------------- | ------------------------------- | ---------------------------------- |
| `lone_escape_score`      | **0.926**                           | -0.234                          | -0.220                             |
| `style_rivalry_count`    | -0.176                              | -0.032                          | **0.770**                          |
| `front_congestion_x_own` | 0.316                               | 0.273                           | 0.117                              |
| `sashi_room_score`       | -0.293                              | 0.152                           | 0.343                              |

`lone_escape_score` correlates strongly (0.926) with the existing
subtractive `self_nige_rate_minus_field_avg`, and `style_rivalry_count`
correlates moderately-strongly (0.770) with `past_style_x_field_pace_match`
— real overlap, tempering expectations that CatBoost depth=8 (which can
already approximate multiplicative interactions from the additive/difference
features it has) will find much incremental signal. `front_congestion_x_own`
and `sashi_room_score` are more clearly novel (|r| ≤ 0.34 vs. all 3 nearest
existing columns).

## Method

- **Harness**: `tmp/candidate-jra-style-rivalry/rivalry_wf.py`, a
  spec-parity clone of `tmp/candidate-masked-lever-retest/retest_wf.py`.
- **Baseline (control)**: CLEAN `armB` from
  `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json` — 250 feat,
  leak-free (matches the live `jra-cb-v9-sim-2013` spec).
- **Treatment**: control + 4 candidate columns (additive, 254 feat).
- **Model**: CatBoost YetiRank, `iterations=300, depth=8, lr=0.05, l2=3.0`,
  no early-stop, `cat_indices=[]` (all-numeric), `threads=4` (reduced from
  the sibling harness's default 6 to avoid CPU oversubscription — 4 other
  WF-test agents were running in parallel on the same host).
- **Folds**: 3 blind walk-forward folds, train `2013..Y-1` / test `Y`, for
  `Y in {2023, 2024, 2025}`.
- **Seeds**: multi-seed, `seed_base in {42, 101, 2026}`, pooled via per-race
  hit-rate averaging across seeds before the paired bootstrap.
- **Base-arm models REUSED** from
  `tmp/candidate-masked-lever-retest/models/base/` (identical feats / seeds /
  folds / CatBoost spec — confirmed by reading `retest_wf.py`) instead of
  retraining, saving ~half the wall-clock (9 candidate-arm models trained
  fresh vs. 18 total).
- **Metrics**: exact-ordinal `top1`/`place2`..`place6`, `top3_box`,
  `fukusho_2p`. Primaries = `{top1, place2, place3}`.
- **Significance**: paired race-level bootstrap, 2000 iterations, fixed seed
  20260519, `delta = treatment − control`.
- **Accept gate**: ≥2 of 3 primaries `delta_pp >= +0.08` AND `LB95 > 0`; AND
  ≥1 of `{place2, place3}` passes; AND no metric regresses below `-0.05pp`.
- **Primary target (per task)**: SUMMER-RESTRICTED cell (`keibajo_code` in
  `{01,02,03,10}`, pooled as `summer_flag`) needs its own primary with
  `delta>=+0.08` AND `LB95>0`, stable sign across multi-seed, plus
  global no-regression.
- **Cell eval**: pooled seed-avg re-cut by `keibajo_code` / `kyori_band` /
  `season_band` / `current_baba_condition` / `summer_flag` (pooled summer
  vs. other) / `grade_bucket` (E-grade tokubetsu vs. ordinary) /
  `shusso_band` (field-size tercile: ≤10 / 11-14 / ≥15), `n >= 200` per cell.

## Result: **REJECT**

Coverage: `lone_escape_score` 60.5-61.3% across folds (a touch lower than the
other 3 candidates, since it additionally requires `>=1` non-null rival, not
just self-history — consistent with the smoke-test's slightly-higher null
count for that column). Run: `tmp/candidate-jra-style-rivalry/rivalry_wf.py`,
707.1s wall-clock (9 candidate-arm models trained; 9 base-arm models reused
from `tmp/candidate-masked-lever-retest/models/base/`).

Pooled (seed-avg, n=10,365 races):

| Metric     | Base   | Cand   | Delta (pp) | LB95   |
| ---------- | ------ | ------ | ---------- | ------ |
| top1       | 33.796 | 33.886 | +0.090     | -0.103 |
| place2     | 18.119 | 18.196 | +0.077     | -0.135 |
| place3     | 14.163 | 14.317 | +0.154     | -0.058 |
| place4     | 12.166 | 12.079 | -0.087     | -0.306 |
| place5     | 11.076 | 11.095 | +0.019     | -0.174 |
| place6     | 10.416 | 10.584 | +0.167     | -0.029 |
| top3_box   | 9.410  | 9.400  | -0.010     | -0.122 |
| fukusho_2p | 74.912 | 74.893 | -0.019     | -0.183 |

Gate: `primaries_passed=0/3`, `lb95_positive=0/3`, `worst_delta=-0.087`
(place4, **breaches** the `-0.05pp` no-reg bound — a mild regression, not
just a flat null result) → **ACCEPT_strict_gate=false**.

Per-fold (top1/place2/place3 delta[LB95]):

| Fold | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 2023 | +0.029[-0.289] | -0.097[-0.473] | +0.087[-0.357] |
| 2024 | +0.241[-0.077] | +0.357[-0.039] | +0.386[+0.019] |
| 2025 | -0.000[-0.290] | -0.029[-0.415] | -0.010[-0.405] |

Per-seed (top1/place2/place3 delta[LB95]):

| Seed | top1           | place2         | place3         |
| ---- | -------------- | -------------- | -------------- |
| 42   | +0.251[-0.058] | +0.222[-0.164] | +0.212[-0.126] |
| 101  | -0.010[-0.328] | +0.039[-0.347] | +0.164[-0.193] |
| 2026 | +0.029[-0.290] | -0.029[-0.434] | +0.087[-0.309] |

Same signature as the sibling masked-lever campaign: 2024 alone looks
promising (place3 crosses LB95>0 for that fold in isolation) and seed42 alone
looks promising (all 3 primaries positive, though none crosses LB95>0 even
there) — but 2023/2025 and seed101/seed2026 don't replicate it, so the
pooled verdict (which is what matters) stays firmly in REJECT territory.

### Summer-restricted primary target (task's PRIMARY gate)

Pooled summer venues (`keibajo_code` in `{01,02,03,10}`, n=2,448) vs. other
venues (n=7,917):

| Cell   | n     | top1 delta[LB95] | place2 delta[LB95] | place3 delta[LB95] |
| ------ | ----- | ---------------- | ------------------ | ------------------ |
| summer | 2,448 | +0.055[-0.340]   | +0.041[-0.395]     | **+0.313**[-0.136] |
| other  | 7,917 | +0.101[-0.110]   | +0.088[-0.168]     | +0.105[-0.148]     |

`place3` in the summer cell has the largest raw delta of the whole campaign
(+0.31pp) but its LB95 is still solidly negative (-0.136) — well short of
the task's own bar (`delta>=+0.08 AND LB95>0`, stable multi-seed sign,
plus global no-regression). **Summer-restricted primary target: REJECT.**
The lever does not fix the previously-diagnosed summer-venue accuracy hole
(`jra-summer-venue-cell-focus-2026-07-04.md`) any more than the sibling
`draw_ablation` lever did.

### Additional cell cuts (`n>=200`, pooled seed-avg)

`keibajo_code`, `kyori_band`, `season_band`, `current_baba_condition`,
`grade_bucket` (E-grade tokubetsu vs. ordinary), `shusso_band` (field-size
tercile) — **no cell reaches `LB95>0` on more than a single primary metric**,
and only 2 of ~30 cells tested reach `LB95>0` on even one metric:

- `keibajo_code=05` (Tokyo, n=1,607): `top1 +0.622[LB95 +0.145]`, but
  `place2 -0.021[-0.664]` / `place3 -0.290[-0.913]` both flat/negative at the
  same cell.
- `kyori_band=1` (n=3,276): `top1 +0.346[LB95 +0.020]` (a thin margin, barely
  above zero), `place2 +0.305[-0.081]` flat, `place3 -0.153[-0.550]`
  negative.

Both are single-metric hits unconfirmed by the other 2 primaries at the same
cell, among ~30 tested cells — consistent with multiple-comparison noise,
the same caution the sibling `jra-masked-lever-clean-retest-2026-07-04.md`
campaign applied to its own single-cell hits. `grade_bucket=E` (tokubetsu,
n=2,186) is flat-to-negative (`place2 -0.366[-0.793]`), so this lever does
not help the E-grade/upset-driven races either (the task's own suggested
"natural home" for a closer-benefits lever). `shusso_band` shows no
field-size-conditional pattern (`small_le10` place3 +0.348[-0.155],
`large_ge15` place3 +0.058[-0.263] — same sign, neither significant, no
monotonic trend with field size that would support the "many rivals split
the trip" mechanism at large fields specifically).

## Conclusion

**REJECT, no cell-conditional adoption case.** The hypothesis (style
scarcity, not raw style, drives the front-running / closing advantage) is
directionally plausible and the pooled deltas are consistently
small-positive on `top1`/`place2`/`place3` across most folds/seeds — but
never clear the `LB95>0` bar anywhere, including the task's own designated
summer-restricted primary target (where `place3`'s raw delta is the largest
in the whole campaign yet still `LB95 -0.136`), and one metric (`place4`)
mildly regresses beyond the `-0.05pp` no-reg floor pooled. This matches the
smoke-test correlation finding: `lone_escape_score` (r=0.926 vs.
`self_nige_rate_minus_field_avg`) and `style_rivalry_count` (r=0.770 vs.
`past_style_x_field_pace_match`) are substantially reconstructable from
existing armB columns, so CatBoost depth=8 likely already captures most of
this multiplicative interaction from the additive/difference features it
has — consistent with the sibling masked-lever campaign's recurring
conclusion that the existing 250-feat pace/style feature set is already rich
enough to absorb this class of signal. **DO-NOT-RETEST** without a
materially different construction (e.g. a genuinely new data source, not a
recombination of `past_*_rate_self` + `field_*_pressure`).

## Artifacts

- Harness: `tmp/candidate-jra-style-rivalry/rivalry_wf.py`
- Report: `tmp/candidate-jra-style-rivalry/reports/style_rivalry.json`
- Log: `tmp/candidate-jra-style-rivalry/rivalry_wf.log`
- Candidate models: `tmp/candidate-jra-style-rivalry/models/style_rivalry/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`
- Base-arm models (reused, not retrained): `tmp/candidate-masked-lever-retest/models/base/seed{42,101,2026}/fold-{2023,2024,2025}/model.json`
- Premise-check source: `src/scripts/finish-position-features/add-race-internal-features.py`
  (confirms `field_*_pressure` already excludes self — the task's stated
  premise was incorrect, documented in the "Premise check" section above)
