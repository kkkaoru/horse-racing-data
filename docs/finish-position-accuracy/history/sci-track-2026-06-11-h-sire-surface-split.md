---
science_track_entry: true
hypothesis_id: H-SIRE-SURFACE-SPLIT
date: 2026-06-11
based_on_iteration: B3 (new-signal probing cycle, post v7-lineage saturation)
scope: JRA (turf code 1 vs dirt code 2)
status: ABORT
verdict: ABORT
production_change: none
artifacts:
  probe_script: tmp/jra-sire-surface/probe_b3_v2.py
  probe_verdict: tmp/jra-sire-surface/probe_verdict.json
---

## Hypothesis

**H-SIRE-SURFACE-SPLIT** (B3):

JES 9_3_89 reports that heritability of racing ability differs by surface
(h² turf = 0.29 vs dirt = 0.18) and the turf↔dirt genetic correlation is
only r ≈ 0.50. This means a sire strong on turf is NOT necessarily strong on
dirt — roughly 75% of turf aptitude variance is independent of dirt aptitude
(1 − 0.50² = 0.75).

**Distinction from H-SIRE-DISTANCE-SPLIT (ABORTED):** H-SIRE-DISTANCE-SPLIT
was sire × DISTANCE band (sprint vs route, 400m keyed). That was ABORTED
(2026-06-10). B3 is sire × SURFACE TYPE (turf code "1" vs dirt code "2") —
a different genetic axis per the h² × surface finding above.

**Proposed feature:** `sire_progeny_winrate_on_surface` = sire's cumulative
progeny win-rate on the CURRENT race's surface type (turf or dirt), computed
leak-free from races strictly before the target race date.

## Gap Confirmation

`sire_track_win_rate` already exists in `race_finish_position_features` and
is built by `build-pedigree-sql.ts::buildSireTrackStatsCte()` using
`surfaceCodeExpression` (left(track_code, 1)). However:

1. **Coverage: only ~2.5%** (18,995 of 746,125 JRA rows). The CTE is called
   inside a date-window UPDATE statement (`race_date BETWEEN $1 AND $2`) and
   aggregates from all historical rows without a strict time cutoff — it is
   therefore a leaky snapshot for the small window being regenerated each
   run, not a continuous feature.
2. When both old `sire_track_win_rate` and B3 are available (17,992 rows),
   the Spearman correlation is **r = 0.978** — they measure the same quantity.

B3 builds a true leak-free cumulative version via a DuckDB window with
`ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING`, lifting coverage from
2.5% to **87.8%**.

## Feature Definition (Leak-Free)

```sql
-- sire_surface_cumul: cumulative win-rate per sire × surface, strict lookback
SELECT sire_name, surface, race_date,
  SUM(starts) OVER w AS cum_starts,
  SUM(wins)   OVER w AS cum_wins
FROM sire_surface_daily
WINDOW w AS (
  PARTITION BY sire_name, surface
  ORDER BY race_date
  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
)
```

Feature value: `cum_wins / cum_starts` when `cum_starts >= 5`, else NULL.
Surface codes: `1` = turf, `2` = dirt (first char of `track_code`).
Obstacle (`5`) excluded (< 2% of JRA races).

**Control variable for partial rho:** `sire_total_wr` (sire cumulative win-rate
across all surfaces, same leak-free window). This is the appropriate control
for measuring the surface-specific incremental signal.

## Probe Results

### Coverage

| Metric                            | Value           |
| --------------------------------- | --------------- |
| Total JRA rows (turf + dirt)      | 729,716         |
| Rows with B3 feature              | 640,730         |
| Coverage                          | 87.8%           |
| Rows with sire_total_wr (control) | 643,072 (88.1%) |

Coverage is well above the 70% threshold. Surface split: 228,932 turf /
411,798 dirt (not surface-concentrated; both surfaces represented).

### Raw Spearman rho vs finish_norm

| Window            | n       | raw rho | p   |
| ----------------- | ------- | ------- | --- |
| Full period       | 640,730 | −0.1074 | p≈0 |
| Holdout 2023-2026 | 196,794 | −0.0976 | p≈0 |
| Turf only (full)  | 228,932 | −0.1545 | p≈0 |
| Dirt only (full)  | 411,798 | −0.0855 | p≈0 |

Negative sign is correct: higher sire win-rate → lower (better) finish position.

### Partial rho (controlling for sire_total_wr)

The partial rho measures the surface-specific incremental signal AFTER removing
the sire's general quality (total win-rate across all surfaces).

| Window            | n       | partial rho | bar  | Pass? |
| ----------------- | ------- | ----------- | ---- | ----- |
| Full period       | 640,730 | −0.0411     | 0.08 | NO    |
| Holdout 2023-2026 | 196,794 | −0.0362     | 0.08 | NO    |
| Turf only (full)  | 228,932 | −0.1053     | 0.08 | YES   |
| Dirt only (full)  | 411,798 | −0.0190     | 0.08 | NO    |

### Redundancy

| Feature         | Spearman corr with B3            |
| --------------- | -------------------------------- |
| `sire_total_wr` | 0.8105 (full) / 0.8366 (holdout) |

B3 is highly correlated (r ≈ 0.81) with the sire's all-surface win-rate.
The surface-specific residual accounts for only ~34% of B3's variance.

## Verdict

**ABORT**

**Deciding factor:** Holdout-window (2023-2026) partial rho = **−0.0362**,
well below the bar of **0.08**.

Full-period partial rho = −0.0411 also misses the bar.

### Why the surface-specific residual is weak

1. **B3 ≈ sire_total_wr:** Spearman r = 0.81 between B3 and the all-surface
   sire win-rate. The sire's general quality dominates the surface-specific
   signal. This mirrors the h-sire-distance-split finding where
   `sire_sprint_wr` was r = 0.82 with `sire_track_win_rate`.

2. **Turf partial rho passes (−0.105) but dirt fails (−0.019):** The
   incremental surface signal exists on turf (where h² is higher, 0.29)
   but is essentially absent on dirt (h² = 0.18, lower heritability means
   more environmental noise swamps the genetic channel). A turf-only feature
   would require surface-splitting the training data and is not actionable
   as a single retrain feature.

3. **Holdout decay:** Full partial rho = 0.041 → holdout partial rho = 0.036.
   Signal is modest and declines in the recent window, consistent with
   convergence of top sires' turf/dirt win-rates over time as modern sires
   are more versatile (deliberate breeding programs).

4. **JES 9_3_89 genetic independence (r ≈ 0.50) does not translate to
   predictive independence** once sire general quality (sire_total_wr) is
   controlled. The 50% genetic correlation at the sire EPD level translates
   to ~81% empirical Spearman correlation between the surface-specific
   win-rate and the all-surface win-rate because (a) elite sires tend to win
   on both surfaces, and (b) win-rate estimators share denominator structure.
   The incremental predictive signal after this overlap is removed is ~0.04
   partial rho, insufficient for retrain ROI.

## Hard Rules Observed

- `tmp/` only: all artifacts in `tmp/jra-sire-surface/`
- No `git add tmp/`
- PG read-only via DuckDB postgres attach READ_ONLY
- Threads ≤ 3, memory_limit = 6 GB
- No training run, no production change
- No DELETE / TRUNCATE / DROP issued
