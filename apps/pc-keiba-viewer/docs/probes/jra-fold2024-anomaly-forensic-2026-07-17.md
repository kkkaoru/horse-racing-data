# JRA — fold-2024 "favorable fold" anomaly forensic (bounded, ~45min)

- **Date**: 2026-07-17
- **Trigger**: two same-day, unrelated candidate constructions — the
  NAR-pooling arm (`jra-nar-pooling-arm-2026-07-17.md`) and the monotonic-
  constraints arm (`jra-monotonic-constraints-arm-2026-07-17.md`) — both
  showed their only individually-significant positive fold at **2024**, with
  2023 negative/flat and 2025 flat/negative in both cases. The orchestrator
  additionally noted the same-day HPO/arch-bakeoff sweep used fold-B (=2024)
  as its primary selection criterion, and its fold-B-selected top-10 configs
  went 0/10 on fold-A (2023) — a third independent data point pointing at
  2024 as an unusually easy fold to look good on. This is a bounded,
  read-only forensic into whether that's structural or coincidental. No
  training performed — analysis only, using stores and models already on
  disk from today's two arms.

## 1. Basic composition — 2023 vs 2024 vs 2025 (all JRA races, `finish_position` not null)

Race count, class-code mix, and season mix are **essentially identical**
across all three years — ruled out as an explanation:

| year | races | avg field size | season_band dist (spring/summer/autumn/winter) | top class code (`703`) races |
| ---- | ----- | -------------- | ---------------------------------------------- | ---------------------------- |
| 2023 | 3,456 | 13.68          | 864/864/840/888                                | 1,228                        |
| 2024 | 3,454 | 13.54          | 888/899/839/828                                | 1,230                        |
| 2025 | 3,455 | 13.75          | 911/936/816/792                                | 1,252                        |

**Venue composition is not identical — a real, verified anomaly**:

| keibajo_code | venue   | 2023                     | 2024    | 2025 |
| ------------ | ------- | ------------------------ | ------- | ---- |
| 08           | Kyoto   | 348                      | **719** | 468  |
| 09           | Hanshin | 552                      | **192** | 468  |
| (all others) | —       | within ±15% across years | —       | —    |

Kyoto races roughly **double** in 2024 relative to both neighboring years,
while Hanshin races **drop to a third** of 2023's level, before both
re-normalize toward a 2025 level that sits between the two. This lines up
with the real-world Kyoto racecourse multi-year renovation (meets displaced
mostly to Hanshin during construction, progressively returning to Kyoto
through 2023 and settling into full normal operation by 2024) — i.e. this
looks like a genuine one-off regime transition landing at the 2023→2024
boundary, not an ongoing multi-year trend or a data artifact. (Venue-code
mapping 08=Kyoto/09=Hanshin per the standard JRA numbering, cross-checked
for internal consistency against this repo's own `SUMMER_VENUES={01,02,03,10}`
= Sapporo/Hakodate/Fukushima/Kokura convention, which matches.)

## 2. Is champion baseline itself unusually weak in 2024? No.

Scored the already-trained baseline model (armB-250, seed42, no
monotone_constraints — from `tmp/monotonic-constraints/models/base/`)
against each fold's own validation set:

| fold | n races | top1        | place2 | place3 |
| ---- | ------- | ----------- | ------ | ------ |
| 2023 | 3,456   | 33.507%     | —      | —      |
| 2024 | 3,454   | **34.395%** | —      | —      |
| 2025 | 3,455   | 32.996%     | —      | —      |

If anything, baseline's raw top1 is _higher_ in 2024, not lower — this rules
out the "baseline underperforms in 2024, leaving more room for any
perturbation to look good" mechanism (a regression-to-the-mean story).

## 3. Is fold-2024 intrinsically noisier (wider single-arm noise floor)? No.

Directly tested using two **identical-spec** baseline models that differ
_only_ in random seed (42 vs 101 — pure noise, no construction difference at
all) — the same paired-bootstrap machinery used for every WF gate today,
applied to a known-null comparison:

| fold | top1 CI width | place2 CI width | place3 CI width |
| ---- | ------------- | --------------- | --------------- |
| 2023 | 1.042         | 1.331           | 1.273           |
| 2024 | 0.926         | 1.390           | 1.303           |
| 2025 | 1.129         | 1.360           | 1.187           |

CI widths (the direct measure of how much a fixed spec's outcome wobbles
from pure random-seed noise alone) are **statistically indistinguishable
across all three folds** — no evidence fold-2024 is a smaller-effective-
sample or higher-variance fold in general. This is worth stating plainly:
the campaign's standing ±0.4pp single-arm noise-floor convention applies
equally to all three folds, not more loosely to 2024.

## 4. Known 2024-specific data-quality incidents? None found in this store.

Grepped memory for 2024-tagged incidents/regens/ETL gaps — no hit
specifically implicating the `tmp/candidate-eval-jra/augmented/` store used
by every WF harness today. The one concrete historical incident type that
would plausibly show up this way (the `nvd_um`→`nvd_nu` pedigree-coverage
collapse, 2022=98%→2025=21% in the _raw_ JV-Data mirror, already fixed
2026-06-23) does **not** appear in this specific store: `sire_distance_win_rate`
NULL% is flat at 5.6-6.3% across 2022-2025 with no discontinuity at 2024,
confirming this store reflects the post-fix pedigree pipeline. No pipeline
defect found.

## 5. Statistical read: coincidence, or structural?

Under a naive null where each of the (NAR-pooling, monotonic-narrow,
monotonic-full) results independently and uniformly picks one of 3 folds to
be "the positive one," the probability all three land on the same fold by
chance is `(1/3)² ≈ 11%` — not overwhelming on its own, and these three
"trials" aren't really statistically independent (they're three different
perturbations of the _same_ champion model scored against the _same_
underlying fold-2024 population, so a shared cause would naturally produce
correlated outcomes across all of them without needing 3 independent lucky
draws).

Combined with §1's confirmed, real venue-composition anomaly, the more
specific and better-supported reading is: **fold-2024 isn't noisier or
weaker in general (§§2-3 rule that out) — it's compositionally unusual**
(Kyoto ~2x overrepresented, Hanshin ~⅓ its typical share), and this is a
genuine one-off regime transition rather than a representative "normal
year" sample. Suggestive corroboration: the monotonic-narrow arm's own
cell scan (pooled across all 3 folds and seeds) independently flagged
`keibajo_code=08` (Kyoto) as one of only 8 cells clearing LB95>0 for top1
— consistent with a Kyoto-specific soft spot in the champion model that a
Kyoto-overweighted fold would disproportionately expose to any perturbation,
in either direction. **This connection is plausible and evidence-consistent,
not proven** — confirming it properly would need a venue×fold interaction
table (score each arm's predictions split by venue _within_ each fold),
which is out of scope for this bounded forensic (would require new scoring
infrastructure, not just analysis of what's already on disk).

## 6. Recommendation for next-cycle evaluation convention

**One line**: when a candidate's only individually-significant fold is
2024, treat it as lower-confidence than the same pattern in 2023 or 2025 —
not because fold-2024 is generically noisier (it isn't, §3), but because it
sits on a known, real, one-off venue-composition transition (Kyoto
return-from-renovation), so a fold-2024-only signal should be checked
against the existing per-venue cell scan before being treated as a general
improvement — if the cell scan's positive hits concentrate at `keibajo_code=08`
specifically, that's corroboration the "fold-2024 win" is a venue-08 win
riding fold aggregation, not a broad-based gain.

## 7. Scope note

This forensic does not revise either arm's verdict — both NAR-pooling and
monotonic-constraints were already REJECTed on their own pooled/summer4/
per-fold gate results independent of this question (§5-6 of each doc already
correctly discounted the fold-2024-only signal before this forensic
existed). This is a standalone follow-up investigation into the underlying
"why," not a re-litigation of either arm's outcome.
