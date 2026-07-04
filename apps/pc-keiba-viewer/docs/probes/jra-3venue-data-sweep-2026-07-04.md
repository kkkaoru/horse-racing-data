# JRA 3-Venue Race-Data Sweep — 函館/福島/小倉, 2023-2026 (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position, hypothesis generation (descriptive, not model-fit)
- **Trigger**: task #25 — large-scale exploratory sweep of race-related raw data
  (pace shape, within-day baba drift, agari structure, weight/condition, field
  composition) vs actual finish order at Hakodate (`02`) / Fukushima (`03`) /
  Kokura (`10`), 2024-2026, to generate **new** testable hypotheses. 2023 was
  added as an independent 4th replication year (cheap to pull, same schema).
- **Method**: raw `jvd_ra` (race) + `jvd_se` (per-horse) pulled directly from
  local PG (port 15432, read-only) via DuckDB's postgres scanner — not the
  offline feature store, so this sees columns the training pipeline may or
  may not have turned into features yet (lap splits, corner passing order,
  running-style judgment, weight change, baba condition per race). One
  materialized table, 32,016 horse-rows / 2,317 races, built by
  `tmp/candidate-3venue-sweep/build_analysis_table.py` →
  `tmp/candidate-3venue-sweep/analysis_table.parquet`. All analysis below is
  **pure descriptive statistics on actual outcomes** (no model, no WF) — the
  "replication" bar is: does the same-direction effect show up in each of
  2023/2024/2025/2026 **separately** for a given venue (not just pooled).
  DuckDB `memory_limit=4GB`, `threads=4`, no docker/DB writes, SELECT-only.
- **Coordination**: cross-checked every candidate against the same-day
  DO-NOT-RETEST catalogue (`jra-summer-venue-cell-focus`,
  `jra-summer-upset-divergence`, `jra-masked-lever-clean-retest`,
  `jra-meetingday-waku-clean`, `jra-unexploited-data-inventory`) to avoid
  relitigating same-day bias, draw affinity/ablation, straight×closer,
  meetingday×waku rail-wear, weather-12, barei, blinker, market×tokubetsu,
  pedigree, style-rivalry, layoff/summer-slot — all confirmed REJECT
  elsewhere today. Two of the six assigned families (weight/condition,
  field-composition×waku) turned out to substantially overlap those REJECTs;
  reported briefly for completeness, not re-litigated.

## Data coverage

| Venue          | 2023      | 2024 | 2025 | 2026 (partial year, thru ~Jul) |
| -------------- | --------- | ---- | ---- | ------------------------------ |
| Hakodate (02)  | 144 races | 144  | 144  | 84                             |
| Fukushima (03) | 240       | 240  | 240  | 108                            |
| Kokura (10)    | 264       | 288  | 240  | 181                            |

Hakodate runs one meet/year (Jun-Jul, purely summer). Fukushima runs 3
meets/year (Apr spring, Jun-Jul summer, Nov autumn) — genuinely multi-season.
Kokura runs 3 meets/year spanning Jan-Sep. This matters for family (d) below.

---

## Ranked hypothesis list

### 1. Pace-shape × distance-band interaction reverses direction — REPLICATES (mile), REPLICATES-with-caveats (intermediate)

**Finding**: `pace_index = kohan_3f_race − zenhan_3f_race` (race's last-3f time
minus first-3f time; positive = decelerating race). Bucketed into fixed
tercile cutoffs **per venue × distance-band** (pooled 2023-2026, not refit
per year, to avoid cutoff-fitting noise), then cross-tabbed against the
winner's `kyakushitsu_hantei` (running-style judgment: 1=逃げ/2=先行 →
"front", 3=差し/4=追込 → "closer").

A first pass pooling all distances per venue showed hot pace favoring
front-runners everywhere — this turned out to be a **pure distance
confound** (longer races mechanically show bigger zenhan/kohan gaps
regardless of tactical pace pressure; `intermediate`-band mean pace_index was
5.0-5.3 vs 0.5-1.6 for mile/long at all 3 venues). Re-cut within
venue×dist_band removes the confound and splits into two genuinely different,
opposite-direction laws:

**Mile (1200-1600m): hot early pace → closer wins (textbook, and it
replicates cleanly)**

| Venue     | 2023 Δ | 2024 Δ | 2025 Δ | 2026 Δ | Pooled Δ (front%, fast−slow) | Pooled LB95                        |
| --------- | ------ | ------ | ------ | ------ | ---------------------------- | ---------------------------------- |
| Hakodate  | −25.7  | −36.4  | −9.3   | −22.7  | −23.7pp (n=71/83)            | **−37.5 to −9.9**                  |
| Kokura    | −21.7  | −6.1   | −22.8  | +8.2   | −12.4pp (n=112/123)          | −24.3 to −0.5                      |
| Fukushima | −5.4   | −5.8   | −2.3   | −9.2   | −6.9pp (n=87/93)             | not computed, small but consistent |

Hakodate: 4/4 years negative. Fukushima: 4/4 years negative. Kokura: 3/4
negative (2026 partial-year, n=19, likely noise). This is the cleanest,
most-replicated finding in the sweep.

**Intermediate (1600-2000m): hot early pace → FRONT-runner wins
(reversed from textbook)**

| Venue     | 2023 Δ | 2024 Δ | 2025 Δ | 2026 Δ | Pooled Δ            | Pooled LB95       |
| --------- | ------ | ------ | ------ | ------ | ------------------- | ----------------- |
| Kokura    | +10.5  | +5.6   | +20.0  | +8.9   | +11.1pp (n=167/181) | **+2.1 to +20.1** |
| Hakodate  | +14.9  | +13.0  | −11.1  | +10.8  | +5.7pp (n=91/86)    | −6.4 to 17.8      |
| Fukushima | −1.9   | +8.7   | +5.6   | +23.5  | +7.2pp (n=138/138)  | −3.1 to 17.5      |

Kokura: 4/4 positive, and the only cell whose pooled LB95 clears 0 alone.
Hakodate/Fukushima: 3/4 positive each, but pooled CI touches zero — directionally
consistent, not individually significant. Sprint distance band showed no
consistent direction at any venue (small n, likely noise — not reported as a
finding).

**Interpretation**: at these 3 regional/minority-JRA venues specifically,
mile races behave like the general JRA population (pace pressure burns off
front-runners), but the 1700-1900m "intermediate" distance band — a common
class-race distance at all 3 tracks — inverts this. Plausible mechanism:
these venues have small fields and tight turns at this distance; a genuinely
honest/fast pace here exhausts the whole field roughly equally rather than
selectively punishing the leader, and the leader keeps a clean-air/no-traffic
tactical advantage that outweighs fatigue. This is speculative — the
descriptive pattern is what's established, not the mechanism.

**Novelty check**: not on the DO-NOT-RETEST list. **Closely adjacent** to it,
though: armB already carries race-level `field_nige_pressure` /
`field_sashi_pressure` / `field_senkou_pressure` / `field_oikomi_pressure`
(aggregate predicted-style pressure from the field, interacted with each
horse's own historical style rate — see
`src/scripts/finish-position-features/add-pacestyle-features.py:345-353`),
and `kyori` is already a feature, so a depth-8 CatBoost tree _could_ already
find a `kyori × field_pressure` split. The same "ingredients already exist,
should be tree-discoverable" argument was made for `straight×closer` earlier
today and it turned out **flat** (structurally saturated, DO-NOT-RETEST). Rate
this candidate **medium confidence**, not a presumed win.
**strictly-prior feasibility**: the realized `pace_index` used here (actual
zenhan/kohan split) is a POST-race outcome, **not serve-feasible as-is**. The
serve-feasible proxy is exactly the existing `field_nige_pressure` family
(built from entrants' historical style rates / RS-v3 predictions, already
computed pre-race). Recommended incremental feature: an explicit
`field_nige_pressure × distance_band_is_intermediate` cross term (and the mile
mirror), isolated to these venues or JRA-wide, cheap to build from existing
columns. **Expected WF cost**: low build cost (pure derived cross of existing
columns, no new data source), 1 fold×seed grid to check before wider rollout,
given the real saturation risk — do not skip straight to full 3×3 grid.

---

### 2. Mile races: the winner is rarely the single fastest closer (corroborates #1, not a separate feature)

Winner has the literally-fastest last-3f time in the race only **15.8-31.9%**
of the time at mile distance, all 3 venues, all 4 years (naive random baseline
for a ~12-13 horse field ≈ 7-8%, so agari does help, but far less than at
`intermediate`/`sprint` where the same metric runs 40-55%). This is
descriptive corroboration of finding #1 (mile races here are decided more by
tactical position/pace management than raw closing speed) — not proposed as
an independent feature; folding it into #1's WF test as supporting context.

---

### 3. Within-day baba-worsening → next race's winner skews front-runner — PARTIAL, high overlap with already-REJECTED `sameday_bias`

**Finding**: baba condition (track condition code) changes mid-card at all
3 venues (32.6% of Hakodate days, 18.8% of Fukushima days, 40.2% of Kokura
days have ≥2 distinct baba codes recorded that day). Comparing the race
immediately following a **worsening** transition (baba code increases,
e.g. rain cuts up the track) against same-day-stable races:

| Venue             | Just-after-worsening front% (n) | Stable-day front% (n) | Δ                        | Pooled 4yr direction                                                  |
| ----------------- | ------------------------------- | --------------------- | ------------------------ | --------------------------------------------------------------------- |
| Fukushima         | 96.0% (25)                      | 74.8% (765)           | **+21.2pp [LB95 +12.9]** | 2023 +16.3, 2024 +27.3, 2025 n=0 (no transitions that year), 2026 n=2 |
| Hakodate          | 86.8% (53)                      | 80.5% (405)           | +6.3pp [LB95 −3.6]       | +0.8, +21.7, +17.9, +21.5 (mostly tiny n)                             |
| Kokura            | 81.5% (81)                      | 76.1% (794)           | +5.4pp [LB95 −3.6]       | +7.0, +9.6, **−6.3 (reversal)**, +8.4                                 |
| Pooled (3 venues) | 85.5% (159)                     | 76.5% (1,964)         | **+9.0pp [LB95 +3.2]**   | —                                                                     |

Directionally consistent pooled and in most venue-years, statistically
significant pooled, but per-venue/per-year cells are small (many years have
single-digit n for the "just-after-worsening" bucket) and Kokura shows one
outright reversal (2025). Grading this **PARTIAL**, not REPLICATES.

**Critical overlap check**: task #26/other probes today already tested
`sameday_bias` (`sameday_front_bias`/`sameday_inside_bias`, a residualized
same-day running trend from `race_bango < N` races) as a full WF feature —
**REJECTED decisively** (0/3 primaries, JRA gate, `jra-masked-lever-clean-retest-2026-07-04.md`
Lever #2). A worsening-baba transition is _exactly_ the kind of event that
would produce a positive `sameday_front_bias` residual in the races right
after it — so this finding is very plausibly **the same signal already
tested and rejected**, just described via its trigger (weather/baba code
change) instead of via the fitted residual. The two are not identical
(mine is a discrete observable event, theirs is a continuous fitted
residual) but the mechanism overlap is high enough that I'm **not
recommending a fresh WF test** of this as a new lever — logging it mainly as
a plausible explanation for _why_ `sameday_bias` measures what it measures,
and as a caution against re-deriving the same signal a third way (the
masked-lever doc already flagged this exact pattern — draw_affinity,
draw_ablation, and sameday_bias all turned out to be things CatBoost's
existing feature set already captures).
**strictly-prior feasibility**: fully serve-feasible with zero modeling —
"is this race's officially-posted baba code worse than the previous race's,
same venue/day" is knowable before the race goes off. But given the overlap
above, this is a "cheap to build, likely redundant" item, not a priority.

---

### 4. Weight-change bands × season (summer負け proxy), Fukushima/Kokura off-season comparison — NOISE, confirms prior global REJECT

Fukushima and Kokura both run off-season meets (spring/winter) as well as
summer meets, so this is a genuine venue-specific retest opportunity for the
already-REJECTED `project_season_sex_weight_probe` (global JRA, 2026-06-20).
Result: no clean monotonic pattern. Fukushima summer: `big_loss` win rate
4.2% vs `gain` 6.3% vs `loss` **8.5%** (higher than a modest gain — direction
inconsistent with a simple "losing weight is bad" story). Kokura shows
similarly inconsistent ordering across bands/seasons. **Confirms the prior
REJECT holds even at venue-specific granularity — do not retest.**

---

### 5. Field size × waku, agari-structure sprint/position — overlap with already-REJECTED draw_affinity/draw_ablation and straight×closer

- Field-size-bucket × waku-group win-rate cross-tabs showed the expected
  large-field / outer-draw skew but nothing beyond what `draw_affinity`/
  `draw_ablation` (both REJECTED today, `jra-masked-lever-clean-retest-2026-07-04.md`)
  already tested at the full-model level.
- Sprint distance: winner's final-corner position ≤2 is very common
  (65-95% across venue/year) — this is the same short-straight-favors-front
  mechanism as `straight×closer` (self-rate × `course_final_straight_m`),
  independently WF-tested today via two methods and REJECTED as
  structurally saturated (`jra-summer-upset-divergence-2026-07-04.md`).
  **Not re-reported as new.**

---

### 6. (explored, inconclusive) mid-race repositioning by venue

Tried: winner's net position gain from corner_1→corner_4, by venue×dist_band,
as a proxy for "how much passing this course's geometry allows." Result:
1.3-2.5 positions gained across venues/bands, no clean venue-differentiating
signal, and the metric is structurally undefined for sprint/short-mile races
(corner_1/corner_2 are not recorded under ~1600m at these tracks — `00` in
raw JV-Data — so the comparison silently drops those distance bands). **NOISE
/ data-artifact-prone — dropping this angle**, noting it so it isn't
re-attempted the same way.

---

## Summary for prioritization

| #   | Hypothesis                                                             | Grade                                      | Serve-feasible as-is?                                 | Recommended next step                                                  |
| --- | ---------------------------------------------------------------------- | ------------------------------------------ | ----------------------------------------------------- | ---------------------------------------------------------------------- |
| 1   | Pace-shape × distance-band (mile=closer, intermediate=front, reversed) | REPLICATES (mile) / partial (intermediate) | No — needs `field_nige_pressure`-style pre-race proxy | Cheap incremental cross-feature + 1-fold WF check before wider rollout |
| 2   | Mile agari-rarely-decisive                                             | corroborates #1                            | n/a (descriptive only)                                | fold into #1's writeup, no separate feature                            |
| 3   | Baba-worsening → front-runner momentum                                 | PARTIAL                                    | Yes, zero-cost                                        | do not re-test — high overlap with REJECTED `sameday_bias`             |
| 4   | Weight×season venue retest                                             | NOISE                                      | —                                                     | DO-NOT-RETEST (confirms prior global REJECT)                           |
| 5   | Field-size×waku, sprint position-winner                                | overlap                                    | —                                                     | DO-NOT-RETEST (covered by draw_affinity/ablation, straight×closer)     |
| 6   | Mid-race repositioning                                                 | inconclusive/noise                         | —                                                     | drop                                                                   |

**Bottom line**: of six assigned families, one (#1, pace-shape × distance-band)
produced a genuinely new, multi-year-replicating, not-previously-tested
pattern worth a cautious incremental feature test — with the caveat that
several structurally similar ideas tested elsewhere today turned out to be
already-saturated in armB, so this should be validated cheaply before
investing in a full grid. The rest of the sweep mostly re-confirmed, at
finer (venue-specific) granularity, verdicts the team had already reached
JRA-wide today (weight/season, draw/waku, straight×closer, same-day bias) —
useful as independent replication of those REJECTs, not as new levers.

## WF follow-up (task #27, 2026-07-04): pace_reversal lever — REJECT

Per team-lead approval, finding #1 (pace-shape × distance-band reversal) was
tested as an explicit incremental feature via the **full standard 3-fold ×
3-seed WF harness** (not a 1-fold cheap pass — those have produced fold-
specific artifacts elsewhere today) on `tmp/candidate-masked-lever-retest/retest_wf.py`
(new `pace_reversal` lever branch), reusing the already-cached CLEAN armB
base models (9 fold×seed models, no retraining needed for control).

**Candidate columns** (pure per-row arithmetic on already-loaded armB store
columns, no PG join, no new data source — verified exact column names first:
`field_nige_pressure`/`field_senkou_pressure`/`past_nige_rate_self`/
`past_senkou_rate_self`/`past_sashi_rate_self`/`kyori` all already exist in
the 250-feat armB set):

- `dist_is_mile`, `dist_is_intermediate` — dummies from `kyori` (1200-1600 /
  1600-2000), matching the sweep's own distance-band cutoffs (the store's
  native `kyori_band` uses different edges — 1400-1700 / 1800-2200 — that
  straddle the sweep's mile/intermediate boundary, so it could not be reused
  directly for this test).
- `field_front_pressure_x_mile` / `field_front_pressure_x_intermediate` —
  `(field_nige_pressure + field_senkou_pressure) × dist_band` dummy.
- `own_front_x_field_pressure_x_intermediate` — own
  `(past_nige_rate_self + past_senkou_rate_self) × field_front_pressure ×
dist_is_intermediate` (the Kokura-reversal carrier).
- `own_sashi_x_field_pressure_x_mile` — own `past_sashi_rate_self ×
field_front_pressure × dist_is_mile` (the textbook-direction carrier).

**Spec**: CatBoost YetiRank, iterations=300, depth=8, lr=0.05, l2=3.0,
no-early-stop, `cat_indices=[]` — identical to the live `jra-cb-v9-sim-2013`
model. 3 blind folds (train 2013..Y-1, test Y) × 3 seeds (42/101/2026), paired
race-level bootstrap (2000 iters).

### Pooled result (seed-averaged, all JRA, n=10,365 races): REJECT

| Metric | base%  | cand%  | Δpp    | LB95   |
| ------ | ------ | ------ | ------ | ------ |
| top1   | 33.796 | 33.883 | +0.087 | −0.097 |
| place2 | 18.119 | 18.135 | +0.016 | −0.200 |
| place3 | 14.163 | 14.256 | +0.093 | −0.135 |

`0/3 primaries passed`, `ACCEPT_strict_gate=false`. Every metric's LB95
crosses zero.

### Target-cell cross-cut (the exact cells the sweep flagged)

Reused the trained models, re-predicted on the same validation folds, cut by
venue × distance-band (`tmp/candidate-pace-reversal-wf/target_cells_crosscut.py`
→ `tmp/candidate-masked-lever-retest/reports/pace_reversal_target_cells_crosscut.json`).
GLOBAL row matches the main report exactly (sanity check the cross-cut is
wired correctly):

| Cell                                           | n      | top1 Δ[LB95]    | place2 Δ[LB95]  | place3 Δ[LB95]      |
| ---------------------------------------------- | ------ | --------------- | --------------- | ------------------- |
| **TARGET** Kokura × intermediate               | 295    | +0.113 [−0.904] | +0.452 [−1.017] | **−1.243 [−2.712]** |
| **TARGET** Hakodate × mile                     | 131    | +0.000 [−1.781] | −0.255 [−1.781] | −1.272 [−3.308]     |
| **TARGET** Fukushima × mile                    | 165    | −0.202 [−2.222] | −1.010 [−2.828] | +0.202 [−1.616]     |
| contrast: Kokura × mile                        | 198    | −0.168 [−1.515] | −0.000 [−1.179] | −0.673 [−1.852]     |
| contrast: Hakodate × intermediate              | 171    | +0.975 [−0.585] | −0.975 [−3.704] | +0.975 [−0.780]     |
| contrast: Fukushima × intermediate             | 269    | −0.620 [−1.859] | +1.115 [−0.248] | +1.115 [−0.496]     |
| Summer-restricted primary (4 venues, all dist) | 2,448  | +0.191 [−0.232] | −0.041 [−0.517] | +0.272 [−0.218]     |
| Summer(4) × mile                               | 631    | +0.159 [−0.581] | −0.159 [−0.951] | −0.106 [−0.845]     |
| Summer(4) × intermediate                       | 950    | +0.351 [−0.351] | +0.210 [−0.702] | +0.105 [−0.737]     |
| Non-summer × mile (contrast)                   | 2,707  | +0.209 [−0.123] | +0.148 [−0.283] | −0.099 [−0.554]     |
| Non-summer × intermediate (contrast)           | 3,493  | −0.057 [−0.382] | +0.095 [−0.258] | +0.038 [−0.353]     |
| GLOBAL (sanity check)                          | 10,365 | +0.087 [−0.097] | +0.016 [−0.200] | +0.093 [−0.135]     |

**No cell clears LB95>0 on any primary — including the exact 3 cells the
descriptive sweep found the strongest 4/4-year replication in.** The
"contrast" cells (same venue, opposite distance band, where the effect
should be absent if the mechanism is real) are statistically
indistinguishable from the target cells, and the target-cell n (131-295) is
underpowered on its own, but the total absence of any positive-LB95 anywhere
— global, summer-restricted, or per-cell — is a clean, decisive signal, not
an underpowered null.

**Verdict: REJECT. DO-NOT-RETEST this construction.** This confirms the
saturation risk flagged in finding #1's "expected WF cost" note: CatBoost
depth=8 already extracts whatever signal exists from `kyori` +
`field_{nige,senkou,sashi,oikomi}_pressure` + `past_{nige,senkou,sashi}_rate_self`
on its own — explicitly crossing them with distance-band dummies added no
incremental information the tree hadn't already found. This is the same
outcome as `straight×closer`, `draw_affinity`, `draw_ablation`, and
`sameday_bias` earlier today: a descriptively real pattern in raw actuals
that the existing armB feature set already captures once given the raw
ingredients. The 3-venue sweep's remaining value is as **independent
replication of the "armB is saturated on pace/draw/position axes" finding**,
not as a source of new deployable levers.

Artifacts: `tmp/candidate-masked-lever-retest/retest_wf.py` (new
`build_pace_reversal` function + `pace_reversal` dispatch branch),
`tmp/candidate-masked-lever-retest/reports/pace_reversal.json` (main report),
`tmp/candidate-pace-reversal-wf/target_cells_crosscut.py` +
`tmp/candidate-masked-lever-retest/reports/pace_reversal_target_cells_crosscut.json`
(target-cell cross-cut), `tmp/candidate-pace-reversal-wf/smoke_test.py`
(pre-flight sanity check).

## Artifacts

- `tmp/candidate-3venue-sweep/build_analysis_table.py` — extraction script
  (PG → parquet, read-only).
- `tmp/candidate-3venue-sweep/analysis_table.parquet` — 32,016 horse-rows /
  2,317 races, one row per horse-race, typed columns (pace splits, corner
  positions, running-style judgment, weight change, baba code, etc.).
- `tmp/candidate-3venue-sweep/family_a_pace_shape.py` /
  `family_a_pace_shape_v2.py` — v1 (confounded, kept for the record showing
  why pooling distance bands was wrong) and v2 (distance-band-controlled,
  the version reported above).
