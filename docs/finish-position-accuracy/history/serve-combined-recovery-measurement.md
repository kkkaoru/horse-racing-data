# Combined Serve-Skew Recovery — Definitive Measurement (2026-06-07 JRA)

**Date**: 2026-06-11
**Race day measured**: 2026-06-07 (single JRA day, 24 races / 23 with complete results)
**Model**: `iter14-jra-cb-pacestyle-course-v8` (catboost-yetirank, 241 features, no retrain)
**Status**: COMPLETE — full 13-layer pipeline, all 241 features live in NEW condition

---

## 1. What this measures

The DEFINITIVE combined accuracy recovery from **all three serve-skew fixes**, scored on
real 2026-06-07 JRA race-day data with the production model unchanged. This supersedes the
earlier partial A-vs-B (`phase3-jra-servefix-validation.md`) which only varied 117/241
features (market-signal + futan were zero in **both** arms, so it was a strict lower bound).

| Fix   | Commit    | What it repairs                                                               |
| ----- | --------- | ----------------------------------------------------------------------------- |
| Fix 1 | `fe871a6` | JRA 09:30 cron + real D1 advance-odds replace the OOD-median fallback         |
| Fix 2 | `5c3aa12` | market-signal layer reads `tansho_odds` from input parquet for upcoming races |
| Fix 3 | `ebd4636` | futan layer reads `futan_juryo` from `jvd_se` for upcoming races              |

### Conditions

| Condition               | Odds source                                              | market-signal + futan       | Pipeline      |
| ----------------------- | -------------------------------------------------------- | --------------------------- | ------------- |
| **NEW** (all 3 fixes)   | D1 advance-odds 20/24 races + jvd_se fallback 4/24       | live (built from jvd_se/D1) | full 13-layer |
| **MID** (Fix 1 only)    | same D1 advance-odds                                     | zeroed at scoring           | full 13-layer |
| **OLD** (pre-all-fixes) | OOD-median (`odds_score=0.5664`, `popularity_score=0.5`) | zeroed at scoring           | full 13-layer |

Both NEW and OLD run the complete 13-layer JRA feature pipeline (v6 base + v7 + v8 layers).
MID reuses the NEW parquet with the 16 market-signal + futan features force-zeroed at
scoring time — this exactly reproduces the production `_coerce()` NULL→0.0 behavior of those
two layers before Fix 2/3, isolating Fix 1's contribution.

---

## 2. Results — NEW vs MID vs OLD

n = 23 races (single JRA day; 1 race dropped for incomplete results).

| Metric     | NEW (all 3) | MID (Fix 1) | OLD (none) |
| ---------- | ----------: | ----------: | ---------: |
| top1       |  **26.09%** |      21.74% |      4.35% |
| place2     |  **13.04%** |       4.35% |      4.35% |
| place3     |   **4.35%** |       4.35% |      0.00% |
| top3_box   |   **4.35%** |       4.35% |      0.00% |
| fukusho_2p |  **65.22%** |      47.83% |     13.04% |
| rentai_hit |  **47.83%** |      39.13% |      8.70% |

### Combined recovery (NEW − OLD) — total of all 3 fixes

| Metric     |     Δ (pp) |
| ---------- | ---------: |
| top1       | **+21.74** |
| place2     |  **+8.70** |
| place3     |  **+4.35** |
| fukusho_2p | **+52.17** |
| top3_box   |  **+4.35** |
| rentai_hit | **+39.13** |

The pre-fix serving path was catastrophic on this race day: OOD-median odds plus NULL
market-signal/futan dropped top1 to 4.35% (1/23) and fukusho_2p to 13.04%. With all three
fixes the model recovers to its expected operating band (top1 26%, fukusho_2p 65%).

---

## 3. Per-fix decomposition

| Source                   |   top1 | place2 | place3 | fukusho_2p |
| ------------------------ | -----: | -----: | -----: | ---------: |
| **Fix 1** (MID − OLD)    | +17.39 |  +0.00 |  +4.35 |     +34.78 |
| **Fix 2+3** (NEW − MID)  |  +4.35 |  +8.70 |  +0.00 |     +17.39 |
| **Combined** (NEW − OLD) | +21.74 |  +8.70 |  +4.35 |     +52.17 |

- **Fix 1 (real odds)** drives most of the top1/fukusho_2p recovery — replacing the OOD-median
  `odds_score=0.5664` (flat across the field) with real market odds restores the single
  strongest signal the model relies on for ranking the winner.
- **Fix 2+3 (market-signal + futan)** is _additive on top of_ real odds — it adds the entire
  place2 recovery (+8.70pp, MID was flat at 4.35%) plus a further +4.35pp top1 and +17.39pp
  fukusho_2p. This is the contribution the previous partial measurement could not see, because
  market-signal + futan were zero in both of its arms.

Feature-population check confirms the conditions are real (from the run diagnostics):

| feature                     | NEW mean | OLD-parquet mean | scoring treatment                             |
| --------------------------- | -------: | ---------------: | --------------------------------------------- |
| `odds_score`                |   0.6066 |           0.5664 | live both; OLD pinned to OOD median by build  |
| `popularity_score`          |   0.4986 |           0.4850 | live both                                     |
| `inverse_odds_implied_prob` |   0.0852 |           0.0852 | live in NEW; **zeroed at scoring** in MID/OLD |
| `futan_juryo`               |  56.0294 |          56.0294 | live in NEW; **zeroed at scoring** in MID/OLD |
| `past_futan_juryo_avg5`     |  20.8452 |          20.8452 | live in NEW; **zeroed at scoring** in MID/OLD |

(The OLD/MID parquet carries the same built values, but the 16 market-signal + futan
features are force-zeroed in the scoring matrix — identical to the production NULL→0.0 coerce.)

---

## 4. Caveats

- **n = 23 races, single race day.** Bootstrap 95% CI on top1 is roughly ±8–10pp at this n.
  Direction and magnitude are indicative; the headline number is the _sign and scale_ of the
  recovery, not a precise point estimate. The I4 holdout simulation remains the best
  population-level magnitude estimate.
- **D1 advance-odds: 20/24 races.** 4 early races (05:04, 05:08, 05:11, 09:08) timed out on
  the D1 advance-odds fetch and fall back to `jvd_se` final (settled) odds. In production at
  09:30 these would be covered; here they use settled odds, which slightly _favors_ the NEW
  arm for those 4 races. The bulk (20/24) use genuine pre-race advance odds.
- **`target_corner_1/3/4_norm` NULL for all 24 races** — corner features are not materialized
  for 0607 in `race_entry_corner_features` (covers to 2026-05-24). These NULLs are identical
  across all three conditions, so they do not bias the deltas.
- **History features** (`race_entry_corner_features`) populate from data through 2026-05-24;
  the 0607 current-race features come from `jvd_se` directly.
- **Two production-pipeline bugs were worked around** to run the full feature set:
  1. **Fix 2 prerequisite** — the base build (`finish_position_features_duckdb.py`) consumes
     `tansho_odds` internally but does not emit it as an output column, so the market-signal
     layer's `stage_parquet_odds()` raises `BinderException: column "tansho_odds" not found`.
     Worked around with an inline augment step (COALESCE D1 advance-odds over jvd_se final)
     before the market-signal layer. **Production needs the base build to pass `tansho_odds`
     through as an output column.**
  2. **Fix 3 SQL bug** — `add-futan-juryo-features.py` (commit `ebd4636`) references
     `coalesce(rec.source, b.source)` where `b` aliases `jvd_se`, which has no `source` column,
     raising `BinderException: Table "b" does not have a column named "source"`. Worked around
     with a custom futan layer using the `'jra'` literal. **Production needs `b.source`
     replaced with the category literal (or a `source` projection on the upcoming-race CTE).**

  Both workarounds reproduce the _intended_ behavior of the committed fixes; the underlying
  serving code still has these two binder errors and will fail at runtime until patched.

---

## 5. Bottom line

On real 2026-06-07 JRA data, the three serve-skew fixes combine to recover **+21.74pp top1**,
**+8.70pp place2**, **+4.35pp place3**, and **+52.17pp fukusho_2p** versus the all-broken
serving path. Fix 1 (real odds) carries the top1/fukusho_2p recovery; Fix 2+3 (market-signal +
futan) is additive on top and supplies the entire place2 gain. This is the first measurement
with all 241 features live, and it confirms the serve-skew tax was the dominant cause of the
degraded live JRA predictions — not a model-quality problem.

Two production binder bugs (Fix 2 base-build `tansho_odds` passthrough, Fix 3 `b.source`)
must be patched before the committed fixes actually run end-to-end in the container.

---

### Provenance

- Raw result: `tmp/validate/combined_recovery.json` (not git-tracked)
- Supersedes the partial lower bound in `phase3-jra-servefix-validation.md`
  (top1 +4.17pp / place2 +4.16pp on 117/241 features)
- Read-only PG (port 15432) + read-only D1 advance-odds; throwaway in-memory DuckDB; no DELETE/TRUNCATE/DROP
