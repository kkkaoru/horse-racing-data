# JRA 2026 Summer Serve-Realistic Eval via R2 Feature Cache (2026-07-04)

- **Date**: 2026-07-04
- **Category**: JRA finish-position — 2026 actual-outcome validation
- **Goal**: `docs/probes/jra-summer-venue-cell-focus-2026-07-04.md` Part 2 could
  only report **raw 2026 actuals** (favorite win rate, field size), not
  model-scored accuracy, because the offline feature store has no 2026 rows
  and Neon's served-prediction log only had ~5 genuine clean-model races. This
  probe checks whether production's per-race R2 feature-parquet archive
  (features-R2-parquet migration) can supply real, serve-identical 2026
  feature vectors for the 4 summer venues so the clean model can be scored
  against real 2026 outcomes offline.

## Feasibility (Step 1) — R2 has TWO relevant prefixes, neither ideal alone

Bucket `pc-keiba-features-archive` (credentials: root `.env`, confirmed
reachable via DuckDB `httpfs` S3-compatible client, read-only, no writes
attempted):

| Prefix                                                      | Date coverage (summer venues, Jun-Jul 2026)                                                                                                     | Columns                                                                                                                                                                                              | Verdict                                                                                                                                                                                                                                     |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `features/by-race/{Y}/{M}/{D}/{cat}/{venue}/{race}.parquet` | **Broad**: 2035 objects across Jun-Jul 2026 alone; essentially every Hakodate(02)/Fukushima(03)/Kokura(10) race day has a file (9-12 races/day) | **45 raw columns** (odds, corner positions, weight, race metadata)                                                                                                                                   | Only **3 of the clean model's 250 `feature_names`** present (`umaban`, `kyori`, `futan_juryo`-ish raw fields) — NOT usable for scoring, no engineered features at all.                                                                      |
| `feat-cache/{cat}/{date}/features.parquet`                  | **Sparse**: only 3 JRA files exist in the ENTIRE bucket — `20260614`, `20260620`, `20260622`                                                    | **231-250 of 250** clean feature*names present (missing only the 19 `sim*\_`similar-race columns, which these June dates genuinely predate —`sim\_\_` was ADOPTed 2026-06-26, after all 3 snapshots) | Schema is good, but only 3 scattered whole-JRA-day snapshots exist anywhere, each covering `keibajo_code` in {02 Hakodate, 05 Tokyo, 09 Hanshin} — **no Fukushima(03), Kokura(10), or Sapporo(01) snapshot exists anywhere in the bucket.** |

**Neither prefix alone satisfies the "373 races, 4 venues" ask.** `feat-cache`
is the genuine serve-time feature cache (same pipeline predict_upcoming.py
uses, so scoring from it is legitimately serve-identical) but is clearly an
**ephemeral rescore cache**, not a durable per-day archive — it was not
populated for the vast majority of 2026 race days, summer or otherwise, and
carries zero Fukushima/Kokura/Sapporo history. Per the assigning instruction
("do not build a feature-reconstruction pipeline") this probe does **not**
attempt to reconstruct the missing 205 engineered columns for the
broadly-covered `features/by-race` archive — that would be exactly the
out-of-scope reconstruction work the instruction excludes.

**Decision**: proceed with a **reduced-scope** real eval using the 3 available
`feat-cache` snapshots (the only source with adequate schema), reporting
honestly how small and narrow the achievable sample is, rather than either
fabricating broader coverage or declaring a flat "infeasible" when a genuine
(if tiny) real-outcome sample IS available.

## What was actually scoreable (Step 2)

Of the 3 `feat-cache/jra/{date}/features.parquet` snapshots, each nominally
covering 12 Hakodate races (36 race-slots total):

- **`20260614`**: 152 rows, 12 races, `finish_position` populated for 151/152
  rows (1 null = a scratch) — a genuine **post-race** snapshot.
- **`20260620`** and **`20260622`**: `finish_position` is **NULL for every
  row** in both files (156/156 each) — these are **pre-race** snapshots (the
  feature vector as it stood before the race ran; `20260622`'s file even
  carries `kaisai_tsukihi='0620'` internally, i.e. it is a re-snapshot of the
  SAME 0620 card, not a different day — `feat-cache` is keyed by the
  container's run date, not necessarily the race date, and a rescore/backfill
  invocation on 0622 re-cached the 0620 card's PRE-race features).

**Net achievable real-outcome sample: 12 races, 1 date (2026-06-14), 1 venue
(Hakodate)** — far short of the ~373-race, 4-venue ask, and even short of the
36-race estimate this probe started with before checking `finish_position`
population.

**Scoring**: `apps/pc-keiba-viewer/tmp/candidate-leak-clean-retrain/artifacts/
jra-cb-v9-sim-2013-CLEAN/` (the live clean CatBoost, 250 features), the 19
missing `sim_*` columns filled NaN (CatBoost's native missing-value handling —
the same behavior any serve-time row with those columns absent would get).
Compared against a market-favorite baseline (`tansho_ninkijun_raw` rank).

## Result (n=12 races, Hakodate, 2026-06-14 — NOT statistically meaningful, descriptive only)

| Metric   | Clean model  | Market favorite |
| -------- | ------------ | --------------- |
| top1     | 16.7% (2/12) | 16.7% (2/12)    |
| place2   | 25.0% (3/12) | 16.7% (2/12)    |
| place3   | 0.0% (0/12)  | 8.3% (1/12)     |
| place4   | 8.3%         | 8.3%            |
| place5   | 8.3%         | 8.3%            |
| place6   | 33.3%        | 33.3%           |
| top3_box | 0.0%         | 0.0%            |

No LB95/bootstrap is computed here — **n=12 is far too small for any
confidence interval to be informative**, and this is explicitly a descriptive
snapshot, not a gated WF comparison. top1 ties the market baseline exactly (2
wins each); place3/top3_box are both 0% for the model at this n, which is
unremarkable noise at n=12 (place3 alone needs dozens of races before a 0%
reading is distinguishable from the ~14% baseline rate).

Cell cuts by class (`kyoso_joken_code`), distance-band, and meeting-day were
computed (`tmp/candidate-2026-summer-serve-eval/results.json`) but are not
reproduced here — every cell has `n<=6` races, which is not a reportable
statistic under this campaign's own `n>=200` (global) / `n>=100` (summer)
eval-rules threshold. A `waku_band` (draw-zone) cut was also computed but is
methodologically distinct from the other cuts (it slices by a per-HORSE
attribute, not a per-race one, so it measures "how did the model do
specifically for horses drawn in that zone" rather than partitioning races) —
flagged here rather than silently presented as equivalent to the other cells.

## Comparison to Part 2 (raw 2026 actuals)

Part 2 reported Hakodate 2026 favorite win rate 27.8% (n=72, ALL finished
2026 Hakodate races, unscored). This probe's n=12 subset's market-favorite
top1 rate is 16.7% — a much smaller, non-overlapping-in-spirit sample (12 of
those same 72 Hakodate races) that happens to read lower, consistent with
n=12 sampling noise around a ~28% true rate (binomial SD at n=12,p=0.28 is
~13pp, so 16.7% is well within 1 SD of 27.8%) — **not evidence of anything
venue-specific**, just illustrating how little n=12 can support.

## Overall conclusion: PARTIAL FEASIBILITY, not sufficient to answer the ask

1. **R2 is reachable and one prefix (`feat-cache`) has adequate schema
   coverage** (92%+ of clean-model features) — the underlying premise
   ("production features are stored as per-race R2 Parquet, serve-identical
   by construction") is TRUE for this prefix.
2. **But that prefix's actual population is sparse and accidental**: 3 JRA
   snapshots total in the whole bucket, all incidentally Hakodate/Tokyo/
   Hanshin, 2 of the 3 being pre-race (no outcome to score against). This
   looks like an ephemeral intra-day rescore cache that happens to persist
   rather than a maintained historical archive.
3. **The broadly-covered prefix (`features/by-race`) lacks the engineered
   features entirely** (45 of 250) — likely a different, earlier-stage raw
   snapshot (odds/entries), not the finish-position feature build.
4. **Net result**: a genuine, real, serve-identical n=12 sample was scored
   (better than Part 2's "not model-scored" caveat, strictly) but it does
   **not** close the gap Part 2 identified — 373 real 2026 summer races
   still have no model-scored comparison available via any existing R2 path.

**Recommendation** (not a decision, per instructions): if a real ~373-race
2026 summer serve-accuracy answer is wanted, it requires either (a) durably
writing the daily `feat-cache`-equivalent parquet for every JRA race day (an
infra change to `predict_upcoming.py`'s R2-write path, separate from today's
campaign and requiring USER sign-off on any code/deploy change), or (b)
waiting for `race_finish_position_model_predictions` in Neon to accumulate
more genuine clean-model-served days (Part 2's original suggestion, still the
lowest-risk path — no new infra, just elapsed time). Building a feature
reconstruction pipeline for `features/by-race`'s 45 raw columns was
explicitly out of scope for this pass and is not attempted here.

## Artifacts

- Script: `tmp/candidate-2026-summer-serve-eval/fetch_score_eval.py`
- Results: `tmp/candidate-2026-summer-serve-eval/results.json`
- R2 credentials: root `.env` (`R2_ACCOUNT_ID`/`R2_ACCESS_KEY_ID`/
  `R2_SECRET_ACCESS_KEY`/`R2_BUCKET`), read-only S3-compatible access via
  DuckDB `httpfs`; no writes attempted, no PG writes, no git ops.
- Does not edit `docs/probes/jra-summer-venue-cell-focus-2026-07-04.md`
  (owned by the summer-cells/meetday-waku probes).
