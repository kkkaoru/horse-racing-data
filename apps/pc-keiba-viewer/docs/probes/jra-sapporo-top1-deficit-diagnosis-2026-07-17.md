# Sapporo top1 deficit — mechanism diagnosis (2026-07-17)

- **Date**: 2026-07-17
- **Category**: JRA finish-position — mechanism diagnosis, not a lever-development
  probe. Task: explain _why_ the champion (`jra-cb-v9-sim-2013-clean` spec,
  armB 250 leak-free features) loses to market at Sapporo (venue `01`) top1
  specifically, per the 2026-07-17 summer-4-venue baseline
  (`docs/probes/jra-summer4-cell-baseline-2026-07-17.md`).
- **Starting fact (confirmed, not re-derived)**: WF blind backtest (3 seed ×
  3 fold, 2023-2025, `n=504`) finds Sapporo top1 **model 32.804% vs market
  35.714%, delta −2.910pp [LB95 −4.762, UB95 −1.124]** — the only
  venue/metric cell in the whole summer-4 ledger with UB95<0. Per-fold: 2023
  −2.976 [−5.759,−0.595], 2024 −2.778 [−5.952,+0.198], 2025 −2.976
  [−6.746,+0.397] — same sign/magnitude in all 3 independently-blind years.
  `place2`-`place6`/`top3_box` are **not** robust at Sapporo (all CIs cross
  zero) — this is a top1-specific weakness. Hakodate (02, same
  small-field/short-straight northern-turf profile) is the positive control:
  +1.698 [−0.156,+3.704].
- **Training performed**: none. Everything below is predict-only (9 cached
  CatBoost artifacts) or small from-scratch LightGBM family fits on a
  frozen, already-existing 2015-2022 train population (same convention as
  the 2026-07-04 family-decomposition precedent) — no candidate model was
  trained or evaluated against an ADOPT gate.
- **Scope discipline**: full 3-year WF (2023-2025), not a 60-day window.
  Structural facts below are measured directly from the store, not asserted
  from general course knowledge.

---

## 1. Loss anatomy — where exactly does the −2.9pp come from (task item 1)

Every race in the summer-4-venue population (2023-2025, clean-winner filter:
exactly one `finish_position==1` row) was classified into 5 mutually
exclusive buckets per (race, seed): `both_hit` (model pick = market pick =
winner), `model_only_hit`, `market_only_hit`, `both_miss_same_pick` (model
and market pick the identical wrong horse), `both_miss_diff_pick`. By
construction, `top1_delta = P(model_only_hit) − P(market_only_hit)`, so this
decomposition exactly explains the venue-level number (sanity check below).

Seed-pooled rates (race×seed pairs, n=1509/1293/2154/2367 respectively):

| Venue        | both_hit | model_only_hit | market_only_hit | both_miss_same | both_miss_diff |
| ------------ | -------- | -------------- | --------------- | -------------- | -------------- |
| 01 Sapporo   | 31.54%   | **1.33%**      | **4.24%**       | 56.79%         | 6.10%          |
| 02 Hakodate  | 31.86%   | 3.48%          | 1.78%           | 58.31%         | 4.56%          |
| 03 Fukushima | 27.99%   | 2.69%          | 1.95%           | 61.70%         | 5.66%          |
| 10 Kokura    | 28.47%   | 2.28%          | 2.58%           | 59.02%         | 7.65%          |

Bootstrap on the race-level (3-seed-averaged) delta reproduces the official
number almost exactly (tiny n diff from 1 dead-heat/multi-winner race per
venue dropped by the clean-winner filter): Sapporo −2.9158pp
[−4.7068,−1.1249] n=503, Hakodate +1.7015 [−0.3094,+3.6369] n=431, Fukushima
+0.7428 [−0.7892,+2.1356] n=718, Kokura −0.2957 [−1.6477,+1.1829] n=789.
Seed=42-only cross-check lands within 0.1pp of the pooled numbers on every
cell — not a seed artifact.

**This is a two-sided pattern, not one-sided**: `model_only_hit` at Sapporo
(1.33%) is less than half of every other venue's (2.28-3.48%), _and_
`market_only_hit` (4.24%) is 1.6-2.4x every other venue's (1.78-2.58%). Both
sides move together.

### 1.1 The mechanically-cleaner cut: model's hit rate conditional on the market already missing

`model_only_hit` is capped by how often the market itself misses (it's
impossible to log a `model_only_hit` in a race the market already won). To
remove that ceiling effect, restrict to races where `market_hit=false` and
ask: given the market missed, how often does the model still find the
winner? (`P(model_hit | market_miss)`, paired bootstrap n_boot=2000):

| Venue        | n (market-miss races, seed-pooled) | P(model hit \| market miss) | LB95  | UB95  |
| ------------ | ---------------------------------- | --------------------------- | ----- | ----- |
| 01 Sapporo   | 966                                | **2.070%**                  | 1.242 | 3.002 |
| 02 Hakodate  | 849                                | 5.300%                      | 3.769 | 6.949 |
| 03 Fukushima | 1497                               | 3.874%                      | 2.939 | 4.876 |
| 10 Kokura    | 1626                               | 3.321%                      | 2.460 | 4.244 |

Sapporo's CI (UB95 3.00) does not overlap Hakodate's (LB95 3.77). Seed=42-only
cross-check: Sapporo 2.174% (n=322), Hakodate 5.654% (n=283), Fukushima
2.806% (n=499), Kokura 3.690% (n=542) — same ordering, same magnitude. **Even
after removing the "market misses less often" ceiling effect, the model's
independent rescue skill is genuinely, robustly weaker at Sapporo than at
all 3 other summer venues** — this isn't just downstream arithmetic of the
market being sharper.

---

## 2. Model's anti-market boldness (task item 2)

Distribution of the model's own top1 pick's `tansho_ninkijun` (seed-pooled):

| Venue        | n    | mean pick ninkijun | pct pick is non-favorite (ninkijun≥2) |
| ------------ | ---- | ------------------ | ------------------------------------- |
| 01 Sapporo   | 1509 | 1.125              | 11.66%                                |
| 02 Hakodate  | 1293 | 1.113              | 9.82%                                 |
| 03 Fukushima | 2154 | 1.117              | 10.31%                                |
| 10 Kokura    | 2367 | 1.139              | 12.51%                                |

**Sapporo's model is not unusually contrarian in frequency** — 11.66% falls
squarely between Fukushima/Hakodate and Kokura, not an outlier.

The sharper question is success rate when it _is_ contrarian. Restricting to
races where the model's pick is literally a different horse from the
market's favorite (`model_pick_horse != market_pick_horse`), bootstrapped:

| Venue        | n (disagree) | hit rate when contrarian | LB95  | UB95  | (for reference) hit rate when agrees with market |
| ------------ | ------------ | ------------------------ | ----- | ----- | ------------------------------------------------ |
| 01 Sapporo   | 173          | **11.56%**               | 6.94  | 16.18 | 35.71% (n=1333)                                  |
| 02 Hakodate  | 118          | 38.14%                   | 29.66 | 47.46 | 35.33% (n=1166)                                  |
| 03 Fukushima | 210          | 27.62%                   | 21.90 | 33.33 | 31.21% (n=1932)                                  |
| 10 Kokura    | 290          | 18.62%                   | 14.14 | 23.10 | 32.54% (n=2071)                                  |

Sapporo's contrarian-pick success rate (11.56%) does not overlap Hakodate's
(CI gap: 16.18 vs 29.66) or Fukushima's (16.18 vs 21.90); it directionally
trails Kokura's too (CIs touch narrowly). At Hakodate, contrarian picks
succeed _more_ often than favorite-agreeing picks (38.14% vs 35.33%) — the
model's independent judgment adds value there. At Sapporo, contrarian picks
succeed at **less than a third** the rate of favorite-agreeing picks
(11.56% vs 35.71%).

**Direct answer to item 2's question**: this is "same degree of contrarianism,
uniquely unsuccessful at Sapporo" (the brief's second framing), not "more
aggressive contrarianism that happens to fail" (the first framing). This
reads as a calibration/information gap, not an aggression/risk-taking
pattern.

---

## 3. Common factors in Sapporo's model-alone-missed races (task item 3)

Races where the market's favorite won but the model's top1 pick did not
(majority-of-3-seeds classification): **Sapporo n=20/503, Hakodate n=8/431**
— both thin. A wide covariate sweep (field size, distance, current track
condition, field pace-pressure/nige-candidate-count, days-since-last-race,
layoff flag, weight z-score, jockey/trainer venue win-rate, model-pick vs
winner running-style class, going, meet-day-index) found **no covariate that
is both (a) meaningfully different between Sapporo's miss bucket and its own
control races, and (b) different in the same direction at Hakodate's miss
bucket** — i.e. no clean, cross-venue-replicated univariate marker at this
sample size. The one directionally-notable signal (`model_pick`'s own
`same_keibajo_win_rate` is lower in the miss bucket than control: 0.077 vs
0.191 at Sapporo) appears **in the same direction at Hakodate too** (0.071
vs 0.198) — a generic "harder races have less course-proven picks" marker,
not something that differentiates Sapporo. Categorical breakdowns (running
style class, going/baba condition, sex) show no distributional skew beyond
what n=20 noise would produce. **Item 3 comes back null at the available
sample size** — this itself is informative (rules out several tempting
single-factor stories) but is not, on its own, underpowered evidence against
a real effect existing; see §4 for a family-level (not single-covariate)
cut that does find something at this same small n.

### 3.1 Structural course-geometry facts (full population, not underpowered)

Measured directly from the store (not asserted), full 2023-2025 population:

| Venue        | n   | avg field size | avg kyori (m) | final straight (m) | dist. to 1st corner (m) | elevation Δ (m)       | avg baba | meet length (race-days) |
| ------------ | --- | -------------- | ------------- | ------------------ | ----------------------- | --------------------- | -------- | ----------------------- |
| 01 Sapporo   | 503 | 12.60          | 1641.6        | 264.0              | **289.9**               | 0.9 (0/503 null)      | 1.52     | 14                      |
| 02 Hakodate  | 431 | 12.31          | 1540.4        | 260.0              | **418.6**               | null (195/431, 45%\*) | 1.46     | 12                      |
| 03 Fukushima | 718 | 14.21          | 1681.5        | 295.7              | 366.7                   | 2.1                   | 1.20     | 20                      |
| 10 Kokura    | 789 | 14.07          | 1718.8        | 293.0              | 350.6                   | 2.96                  | 1.67     | 24                      |

_(\*Hakodate's course-elevation column has a genuine 45% coverage gap in this
store — a data-completeness note, not something bearing on Sapporo's own
number, which is fully populated.)_

Sapporo and Hakodate are the campaign's two small-field, short-meet, short-
final-straight "regional" summer venues — clearly distinct from
Fukushima/Kokura on those axes — which is exactly why Hakodate is the
natural contrast venue (per the task brief). On every one of those shared
axes (field size, straight length, meet length, going) Sapporo and Hakodate
are close to each other, so none of them explain why the _model-vs-market_
pattern diverges so sharply between the two. **The one large, distinctive
Sapporo-vs-Hakodate structural gap is distance to the first corner: 289.9m
at Sapporo vs 418.6m at Hakodate** — horses reach the first turn ~130m
sooner at Sapporo, leaving less time post-break to sort position. This is a
plausible (not proven) causal channel for why gate-craft/tactical
positioning skill — the kind of information jockey/trainer connections carry
more of than a static pre-race feature set does — could matter more at
Sapporo specifically. See §4.

---

## 4. Feature-family attack, restricted to Sapporo vs Hakodate (task item 4)

Delegated to a parallel sub-analysis reusing the 2026-07-04
family-decomposition method (`tmp/candidate-nonconform-decomp/families.py`,
8 families: market/recent*form/career_ability/speed_time/style_pace/
physical/connections/similarity), retrained from scratch on the same frozen
2015-2022 JRA-wide population (not reused from cache — the cached
`race_scores.parquet` doesn't carry pick \_identity*, only the winner's own
assigned rank, which item 4's diagnostic needs). Retrain verified two ways:
all 8 single-best-columns match `family_info.json` exactly; LightGBM top1
rates land within 0.2-1.8pp of the cached numbers (residual = LightGBM
multithread float non-determinism, not a methodology difference). Cross-
check against this doc's own numbers: recomputed champion top1 Sapporo
32.869% vs Hakodate 35.576% (vs this doc's §1 ledger: 32.804/35.494) and
market_raw top1 Sapporo 35.785% vs Hakodate 33.643% (vs baseline doc's
ledger: 35.714/33.796) — all within 0.1-0.2pp, confirms independent
reproduction from a from-scratch retrain.

### 4.1 Absolute family performance, Sapporo vs Hakodate (unconditional, LightGBM method)

| Family                | Sapporo top1 (n=503) | Hakodate top1 (n=431) | Sapporo − Hakodate |
| --------------------- | -------------------- | --------------------- | ------------------ |
| market (raw ninkijun) | 35.785               | 33.643                | **+2.14**          |
| champion (3-seed avg) | 32.869               | 35.576                | **−2.71**          |
| connections           | 22.266               | 20.186                | +2.08              |
| recent_form           | 22.068               | 23.202                | −1.13              |
| career_ability        | 21.272               | 20.186                | +1.09              |
| similarity            | 17.495               | 17.633                | −0.14              |
| speed_time            | 13.121               | 12.065                | +1.06              |
| style_pace            | 12.127               | 12.065                | +0.06              |
| physical              | 11.730               | 10.209                | +1.52              |

**None of the 7 non-market engineered families is uniquely weak at
Sapporo — several (connections, career*ability, speed_time, physical) are
actually slightly \_stronger* at Sapporo than Hakodate.** Only two rows show
a large, meaningful Sapporo-vs-Hakodate gap: raw market (+2.14pp, Sapporo
favor) and the full champion model (−2.71pp, Hakodate favor). This is the
clearest single piece of evidence that **the deficit is not "the model's
engineered signal is bad at Sapporo"** — every one of its component families
is fine-to-good there. It is specifically that (a) market is unusually sharp
at Sapporo and (b) the champion's full-model score, despite good
constituent-family performance, doesn't convert that into a matching edge
the way it does at Hakodate.

### 4.2 Which family would have rescued the model-alone-missed races

Model-alone-missed subset sizes (majority-seed classification, this
sub-analysis's own seed-by-seed count): **Sapporo 20-23/seed (of 503 races),
Hakodate 7-9/seed (of 431 races)** — Hakodate is genuinely too thin to be a
confident contrast on this specific cut; treat it as indicative only.
Recomputed directly from the sub-analysis's per-seed CSV, pooling raw counts
across the 3 seeds (42/101/2026) rather than averaging percentages:

**Agrees with the actual winner** (i.e. "would have picked correctly"):

| Family          | Sapporo (32/64, 19/64, ... pooled %) | Hakodate (pooled %, n=23) |
| --------------- | ------------------------------------ | ------------------------- |
| **connections** | **50.0%** (32/64)                    | 21.7% (5/23)              |
| market          | 29.7% (19/64)                        | 26.1% (6/23)              |
| career_ability  | 29.7% (19/64)                        | 30.4% (7/23)              |
| similarity      | 28.1% (18/64)                        | 21.7% (5/23)              |
| recent_form     | 23.4% (15/64)                        | 21.7% (5/23)              |
| speed_time      | 14.1% (9/64)                         | 13.0% (3/23)              |
| physical        | 10.9% (7/64)                         | 4.3% (1/23)               |
| style_pace      | 7.8% (5/64)                          | **34.8%** (8/23)          |

`connections` is Sapporo's clear, seed-stable leader — per-seed it's
47.6/47.8/55.0%, never displaced, ~18-25pp clear of the runner-up in every
single one of the 3 independently-trained seeds. At Hakodate no family shows
comparable stability (leader flips between seeds; connections itself swings
11-43% on the same 7-9 races) — consistent with Hakodate's n simply being
too small to read, not necessarily evidence the effect is genuinely absent
there.

**Agrees with the model's own wrong pick** (i.e. "what the champion appears
to be following into error"):

| Family         | Sapporo (pooled %) | Hakodate (pooled %) |
| -------------- | ------------------ | ------------------- |
| **market**     | **59.4%** (38/64)  | **73.9%** (17/23)   |
| recent_form    | 28.1%              | 21.7%               |
| speed_time     | 26.6%              | 13.0%               |
| career_ability | 25.0%              | 17.4%               |
| connections    | 12.5%              | 39.1%               |
| style_pace     | 7.8%               | 21.7%               |
| similarity     | 4.7%               | 26.1%               |
| physical       | 4.7%               | 8.7%                |

`market` dominates this side at **both** venues — when the champion deviates
from the raw favorite and is wrong, it's almost always because it leaned on
a market-adjacent alternative (a family LightGBM fit on market's own 15
columns), not because a non-market family pulled it astray. **This half of
the diagnostic is a clean null for a venue-specific mechanism** — the "what
leads the model astray" pattern is generic, not Sapporo-specific.

**Reading both halves together**: the Sapporo-specific signal is entirely on
the "what would have saved these races" side (connections), not the "what
misleads the model" side (market, same everywhere). That is a coherent,
non-contradictory pattern, but it rests on n=20-23 races and a single
(though 3x-seed-replicated) venue-vs-venue contrast — a lead, not a
confirmed mechanism.

---

## 5. Market's informational edge — is it real, and is it fillable? (task item 5)

### 5.1 Intraday odds-movement data does not exist for 2023-2025

Checked directly (local PG mirror, `jvd_o1`): every Sapporo 2024 race has
exactly one row per race at `data_kubun='5'` (confirmed final) with
`happyo_tsukihi_jifun='00000000'` (the intraday-announcement timestamp field
zeroed on confirmed rows). `data_kubun='9'` exists but only 102/114,436 rows
total across all JRA history (a rare non-movement marker, not snapshot
history). **There is no way to reconstruct whether Sapporo favorites are
formed early or move late for any historical WF year** — this specific
sub-question (task item 5's literal ask) is infeasible with available data,
reported honestly rather than worked around.

### 5.2 But a cheaper, decisive proxy check rules out the "just shorter favorites" explanation

Before trusting an "informational edge" story, the mechanical alternative
has to be ruled out: maybe Sapporo's favorites just look shorter-priced
(weaker/more lopsided fields), which would inflate market accuracy for
free, no "edge" required. Directly measured (full population):

| Venue        | n   | mean favorite odds | median favorite odds | pct favorite ≤1.5x | avg favorite-to-2nd-favorite odds gap (mean / median) |
| ------------ | --- | ------------------ | -------------------- | ------------------ | ----------------------------------------------------- |
| 01 Sapporo   | 503 | 2.668              | 2.6                  | 7.95%              | 1.741 / 1.3                                           |
| 02 Hakodate  | 429 | 2.645              | 2.6                  | 7.46%              | 1.715 / 1.4                                           |
| 03 Fukushima | 716 | 2.831              | 2.8                  | 5.87%              | 1.720 / 1.3                                           |
| 10 Kokura    | 790 | 2.827              | 2.8                  | 5.06%              | 1.679 / 1.4                                           |

**Sapporo and Hakodate are statistically indistinguishable on every measure
of favorite strength/market conviction** (mean odds 2.668 vs 2.645, median
identical at 2.6, favorite-to-2nd gap 1.741 vs 1.715) — yet Sapporo's
favorite wins ~2pp more often (35.7-35.8% vs 33.6-33.8%, §1/§4.1). Same
apparent conviction, higher accuracy. This is exactly the signature of a
**calibration edge** (the market is _right_ more often at a given confidence
level), not a **confidence-level artifact** (the market merely looks more
certain because the field is weaker). This is the single strongest piece of
evidence for a genuine informational edge, and it directly rules out the
most obvious alternative explanation.

### 5.3 The champion's absolute skill at Sapporo is not the problem

Cross-referencing the pre-v9-sim venue study
(`docs/probes/jra-venue-accuracy-investigation-2026-06-19.md`, different
model — `iter14-jra-cb-pacestyle-course-v8`, full 2007-2026 history, not
market-relative): Sapporo ranked **#2 of 10 JRA venues** by absolute top1
accuracy (42.64%, +2.34pp vs the 40.30% global average) — a genuinely
_good_ venue for the model in absolute terms, not a hard one. Combined with
§4.1's finding that every non-market feature family's absolute Sapporo
performance is comparable-to-better than Hakodate's, this rules out "Sapporo
is intrinsically a hard prediction problem" — the deficit is entirely
relative to Sapporo's own unusually sharp market, not an absolute skill gap.

---

## 6. Mechanism ranking

**A. Genuine, broad-based market/informational edge specific to Sapporo, not
captured by the current pre-race feature set — HIGH confidence, primary
finding.**
Support: (i) market's raw accuracy is elevated at Sapporo across every cut
checked in this doc and the baseline doc (dirt/turf/distance-band/class,
§4.1, and the original ledger's venue×surface/venue×distance_band/
venue×class_label rows); (ii) that elevation survives the strongest
available confound check — favorite-strength parity with Hakodate (§5.2);
(iii) the model's own absolute quality at Sapporo is fine-to-good (§5.3,
§4.1); (iv) loss anatomy is two-sided and broad (§1: both `model_only_hit`
suppressed and `market_only_hit` elevated, not concentrated in one narrow
sub-pattern; §1.1: the market-conditional rescue rate gap survives removing
the mechanical ceiling effect); (v) contrarian-pick success collapses
specifically at Sapporo, not contrarian-pick frequency (§2); (vi) item 3's
systematic covariate sweep found no legible single-factor story (§3),
consistent with — not proof of — an edge that lives outside anything a
static per-horse feature could encode (day-of paddock/track read, local
money). Caveat: (ii)-(iii) are strong ruling-out evidence, not direct proof
of "information" per se — no data source exists to observe bettor behavior
or paddock conditions directly (§5.1).

**B. Jockey/trainer/sire ("connections") signal specifically under-served
by the model at Sapporo — LOW-MODERATE confidence, thin, secondary lead.**
Support: §4.2's seed-stable (3/3 independently-trained seeds, ~18-25pp clear
margin every time) finding that `connections` is the standout
"would-have-saved-these-races" family at Sapporo, with no comparable
Hakodate signal; §3.1's structural fact that Sapporo has far less run-in to
the first corner (289.9m vs Hakodate 418.6m) than any other summer-4 venue,
a plausible channel for why gate-craft/tactical skill would matter more
there. Against: n=20-23 races is thin; Hakodate's own n=7-9 is too small to
be a confident contrast (could be a real Sapporo-specific effect, or could
be an artifact that would appear at any venue with a comparably-sized
model-alone-missed bucket — not distinguishable at this n); §3's covariate
sweep of `jockey_keibajo_win_rate`/`trainer_keibajo_win_rate` for the
model's picks showed the same-direction gap at Hakodate too (not
Sapporo-specific by that measure); the model already carries fairly granular
jockey×venue×season features (`jockey_keibajo_win_rate`,
`jockey_season_keibajo_win_rate`, `jockey_season_keibajo_distance_win_rate`/
`_count`) which a prior campaign
(`docs/probes/jra-venue-hypothesis-verification-2026-06-19.md` H5) already
found saturated ("残差 = elite jockey over-trust... 市場効率の壁"); and a
same-day precedent (§7) found that blending a _different_ secondary family
into the champion via a trained meta-learner collapses to "trust the
champion's own score" — a discouraging structural prior for any blend-based
attempt to act on this lead.

**C. Mechanical field-size/favorite-strength artifact — RULED OUT as a
standalone explanation.** Field size (§3.1: 12.60 vs 12.31) and favorite
strength (§5.2) are both close between Sapporo and Hakodate, so neither
explains why the two diverge so sharply on model-vs-market performance
despite sharing the "small regional summer venue" profile that separates
both of them from Fukushima/Kokura.

## Verdict

**Predominantly NOT fillable with the current pre-race, per-horse feature
paradigm.** The weight of evidence (§5.2's favorite-strength-parity result
especially) points to Sapporo's market carrying real information the
present feature set structurally cannot encode, not a correctable model
defect. This is reinforced by how much of the adjacent lever space has
already been tested and rejected across this campaign for reasons (GBDT
already captures venue/distance/track-bias interactions implicitly;
recalibration of running-style pick rates makes global accuracy worse;
score blends collapse to trusting the champion) that would plausibly
generalize to most Sapporo-specific variants of the same ideas (see §7
dedup). Hypothesis B (connections/gate-craft) remains a genuinely
non-duplicated, internally-reproducible thread worth a bigger-sample look
(§8) — but it is not, at n=20-23, distinguishable from "the kind of thing
that would show up in any small venue-restricted slice," and the closest
same-day precedent for actually _acting_ on a family-level lead (§7) is a
clean reject.

---

## 7. Lever proposals

Per task instructions (max 2, dedup required, proposal-only): **one
proposal offered, heavily gated; a second is explicitly declined with
reasons rather than manufactured to fill a quota.**

### 7.1 Proposed (gated): scale the connections-family "model-alone-missed"

diagnostic before considering any lever

**Not a lever spec — a precondition for one.** Extend §4.2's diagnostic
(family-restricted top1 pick vs actual winner, on the subset where champion
misses top1 and market hits it) from Sapporo-only (n=20-23) to either (a)
all JRA venues pooled, or (b) at minimum Hakodate/Fukushima/Kokura at
matched sample size (would need multiple WF years or a JRA-wide population
to get any single venue's model-alone-missed bucket past a few hundred
races). This resolves the open question §4.2 cannot: is `connections`
generally the best "rescue" family whenever the champion misses and market
hits (a JRA-wide pattern that just happens to be more visible at Sapporo's
higher base rate of market-hit races), or is it genuinely elevated at
Sapporo specifically (compatible with the run-to-first-corner story, §3.1)?
Only the second answer would justify any further lever design; a positive
JRA-wide-generic answer would fold this into the existing, closed
`jra-nonconforming-signal-decomposition-2026-07-04.md` findings (which
already found `connections` a middling-not-leading family in its own
JRA-wide S2 retention ranking — 0.324 retention, 5th of 7 non-market
families, `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md`
§"Headline result 1") rather than motivating anything Sapporo-specific.

**Dedup**: this is a diagnostic extension, not a re-test of any closed item.
It does not touch `project_venue_cell_round2_2026_06_20` (that campaign
tested _corrections_/routing at Tokyo/Hakodate/Hanshin, not a
family-vs-outcome diagnostic at Sapporo/Hakodate) or
`jra-nonconforming-signal-decomposition-2026-07-04.md` (JRA-wide,
severity-conditioned, never venue-conditioned).

### 7.2 Declined: no second lever proposed

Explicitly not proposing a blend/reorder/routing lever acting on the
connections lead now, for three compounding reasons, each independently
sufficient:

1. **n=20-23 is too thin to specify a defensible design** (train/apply
   split, regularization, gating threshold) with any confidence that the
   result reflects signal rather than which 20 races happened to fall in
   the bucket.
2. **A near-identical class of intervention was tested same-day and firmly
   rejected**: `docs/probes/jra-contender-set-meta-reorder-2026-07-17.md`
   trained a LightGBM meta-learner blending the champion's score with a
   secondary family sub-model (physical/style_pace/speed_time, not
   connections, but structurally the same "blend a secondary family score
   into the champion" design, including a contender-set-limited reorder
   variant structurally similar to "only touch disagreement races"). Result:
   0/8 selection-sweep passes, 2025 blind confirm 0/3 primaries with top1
   delta literally `-0.0000pp`, and a mechanistic explanation (`z_base`
   dominates the meta-learner's own gain by 19×-49× in every one of 9
   arm/seed fits) that would plausibly reproduce regardless of which
   secondary family is substituted in. That doc's own DO-NOT-RETEST covers
   "a stacked LightGBM lambdarank meta-learner over base+[secondary family]
   scores... with either a full within-race reorder or a contender-set-
   limited reorder" — swapping `connections` in for `physical/style_pace/
speed_time` would be the same design under that clause's spirit, not a
   genuinely distinct one, until §7.1's bigger-n check gives a reason to
   believe this specific family would behave differently.
3. **The adjacent lever-class REJECT history is extensive and points the
   same direction**: per-venue specialist training/routing structurally
   rejected (`project_venue_cell_round2_2026_06_20`, Ōi precedent cited
   there); track-bias/draw-bias correction rejected globally and at
   fine-grained per-class cells (`jra-track-bias-perclass-2026-06-19.md`,
   `jra-trackbias-dynamic-subgroup-2026-06-19.md`, ρ≤0.028 everywhere,
   0 robust cells); running-style/pace pick-rate calibration rejected
   (`jra-venue-hypothesis-verification-2026-06-19.md` H2 — demoting
   front-runners/promoting closers costs −4.19pp globally, the _opposite_
   direction from anything this doc's §2/§4 findings would motivate anyway,
   since Sapporo's problem is contrarian-pick failure, not front-runner
   over-selection); venue×distance interaction features rejected as
   redundant with existing `keibajo_code`+`kyori` splits (H4); jockey×venue
   features already saturated (H5); odds-derived field-difficulty features
   rejected as regime-dependent (`project_jra_field_difficulty_reject_2026_06_23`);
   isotonic/monotone recalibration permanently closed (argmax-invariant).

---

## 8. Caveats

- Item 3's null result is a "no signal found at n=20", not "proven no
  signal exists" — a genuinely small effect could be invisible at this
  sample size on any single covariate even if present.
- §4's family-attack retrain used LightGBM's default multithreaded
  nondeterminism; absolute numbers differ from the cached
  `race_scores.parquet` by up to ~1.8pp on some rows — directionally
  consistent, not byte-identical, and documented as such rather than
  silently reconciled.
- Hakodate is the natural contrast venue per task instructions, but it is
  one venue, not a full null distribution — §7.1 names the follow-up that
  would resolve this.
- No production or model change is proposed or attempted by this diagnosis;
  §7.1 is a diagnostic recommendation only, explicitly not queued for a
  WF/ADOPT gate.
- Sapporo's own 2026 meet has not started as of this writing (0 `jvd_ra`
  rows for `kaisai_nen=2026`, confirmed in the 2026-07-17 baseline doc) —
  nothing here extrapolates from 2026 serve data; all evidence is the
  2023-2025 WF population plus full-history absolute-accuracy context from
  the 2026-06-19 venue study.

---

## 9. 日本語まとめ

札幌(01)top1がmarketに頑健に負ける(−2.910pp、UB95<0、3年連続同方向)
機構を診断した。**結論: 主因は市場側の情報優位で、現行の pre-race 静的
特徴量では埋まらない可能性が高い。**

根拠: (1) loss anatomy が両側性 —model_only_hitが他場の半分未満、
market_only_hitが1.6-2.4倍—で、market_miss条件付きでもmodelの独立救出率
が札幌のみ有意に低い(2.07% vs 函館5.30%、CI非重複)。(2) 逆張りpick頻度は
札幌が突出していない(11.66%、他場と同水準)が、逆張りpick成功率は札幌のみ
崩壊(11.56% vs 函館38.14%、CI非重複)——「大胆さ」ではなく「同じ大胆さが
札幌だけ当たらない」という較正/情報ギャップ型。(3) 8家族分解で非市場7家族
いずれも札幌で弱くない(むしろconnections/career_ability/speed_time/physical
はやや強い)——モデルの絶対的な能力の問題ではない。(4) 決定打: 札幌と函館は
favorite強度(平均オッズ・中央値・2番人気との差)が統計的に区別不能なのに、
札幌のfavoriteは約2pt高い勝率——「単に混戦度が低いだけ」という機械的説明を
排除し、較正エッジ(同じ確信度でより当たる)という純粋な情報優位のシグネチャ
と整合。(5) 旧モデル(iter14)研究では札幌はJRA10場中2位の絶対精度——札幌が
「難しい場」なのではない。(6) 市場のタイミング情報(直前オッズ変動)は2023-25
年の historical jvd データに存在せず(単一confirmed snapshotのみ)、直接検証
は infeasible と正直に結論。

副次リード(低〜中確信度): connections家族(騎手・調教師・血統)が札幌の
「model単独外し」レース(n=20-23、薄い)で seed 安定的に最良の救出家族
(3/3 seed で47.6-55.0%、次点に18-25pt差)——札幌の第1コーナーまでの距離が
函館より約130m短い(289.9m vs 418.6m、実測)という構造的差異と整合的な
仮説だが、n不足かつ同日closeしたcontender-set meta-reorder probe(secondary
family blendはchampion score(z_base)が19-49倍支配し効果ゼロ収束)という
構造的に近い前例がREJECTされたばかりで、blend型レバーへの外挿には強い
慎重材料。

レバー提案は1件のみ(gated、レバーではなく次の診断ステップ): connections
家族診断をJRA全体または他場マッチドサンプルに拡張し、札幌固有か
JRA全体パターンかを判定してから初めてレバー化を検討すべき。2件目は
意図的に見送り(理由: n不足、同日REJECT前例との設計類似、隣接レバー
クラスの広範なREJECT履歴)。

---

## Artifacts

- `apps/pc-keiba-viewer/tmp/candidate-sapporo-deficit-diagnosis-2026-07-17/loss_anatomy.py`
  — items 1/2/3 script (predict-only, 9 cached champion artifacts)
- `.../loss_anatomy.log`, `.../loss_anatomy_summary.json`,
  `.../race_seed_categories.parquet` (race×seed pick/category table),
  `.../race_level_majority.parquet` (race-level covariates + majority
  classification), `.../covariate_report.json`, `.../categorical_report.json`
- `apps/pc-keiba-viewer/tmp/candidate-sapporo-family-attack-2026-07-17/` —
  item 4 sub-analysis: `family_scores.py`, `champion_scores.py`, `analyze.py`,
  `family_scores_horses.parquet`, `champion_scores_horses.parquet`,
  `family_refit_check.json` (retrain-vs-cache verification),
  `step3_aggregate.csv`, `step4_diagnostic.csv`,
  `step4_cross_seed_stability.csv`, `summary.json`
- Reused unchanged: `tmp/candidate-masked-lever-retest/models/base/**`,
  `tmp/candidate-leak-clean-retrain/jra_v9sim_feature_sets.json`,
  `tmp/candidate-eval-jra/augmented/**`,
  `tmp/candidate-nonconform-decomp/families.py`
- Local PG (`postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing`,
  read-only) — `jvd_o1` snapshot-availability check (§5.1)
- Precedents read and cited: `docs/probes/jra-summer4-cell-baseline-2026-07-17.md`,
  `docs/probes/jra-nonconforming-signal-decomposition-2026-07-04.md`,
  `docs/probes/jra-contender-set-meta-reorder-2026-07-17.md`,
  `docs/probes/jra-venue-accuracy-investigation-2026-06-19.md`,
  `docs/probes/jra-venue-hypothesis-verification-2026-06-19.md`,
  `docs/probes/jra-track-bias-perclass-2026-06-19.md`,
  `docs/probes/jra-trackbias-dynamic-subgroup-2026-06-19.md`,
  memory `index_closed_probes.md`,
  `project_venue_cell_round2_2026_06_20`,
  `project_jra_field_difficulty_reject_2026_06_23`
