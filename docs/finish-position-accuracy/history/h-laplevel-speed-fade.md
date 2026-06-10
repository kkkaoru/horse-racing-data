---
science_track_entry: true
hypothesis_id: H-LAPLEVEL-SPEED-FADE
date: 2026-06-11
based_on_iteration: iter30-nar-cb-residual-*-v8 (production baseline)
scope: NAR (all keibajo except Banei) + JRA
status: DATA-BLOCKED (NAR: venue-concentrated, only 4/14 venues have lap data; JRA: testable but out-of-scope for current NAR-centric model)
verdict: DATA-BLOCKED — not testable with current warehouse for NAR; JRA-only probe deferred
production_change: none (data availability audit only)
artifacts:
  availability_json: tmp/laptime/availability.json
---

## Motivation

The race-level speed-fade hypothesis (B4, `H-SPEED-FADE-INDEX`, ABORTed 2026-06-11) failed
for two reasons:

1. Partial ρ = 0.0725 < bar 0.08 (marginal signal)
2. Venue sign inconsistency: 8/14 NAR venues showed NEGATIVE rho, contradicting the stamina
   hypothesis direction

The ABORT analysis identified the root cause: `zenhan_3f / kohan_3f` are **race-level
aggregates** that conflate individual stamina with the pace scenario. In a fast-early race,
ALL horses show larger fade regardless of their own stamina capacity.

Refinement direction 3 from the ABORT record: use `nvd_ra.lap_time` / `jvd_ra.lap_time`
(per-200m furlong times) to build a per-horse **individual** deceleration index, normalized
to remove the race-level pace component. This would isolate stamina from race scenario.

## Data Availability Audit

### lap_time Column Format

Both `nvd_ra` (NAR) and `jvd_ra` (JRA) contain `lap_time` as a `varchar(75)` packed string:

- 25 × 3-char fields
- Each field = time for one 200m furlong in tenths of seconds (e.g. `125` = 12.5s)
- Trailing `000` fields = unused (race distance shorter than 25 furlongs)
- Coverage: per-race only (not per-horse) — would need to join to the race and use the
  race's lap curve for each horse in that race

Format verified against zenhan_3f/kohan_3f: sum of final 3 laps ≈ kohan_3f (within ±0.1s).

### NAR Coverage — VENUE-CONCENTRATED

**Critical finding: Only 4 of 14 NAR venues have lap_time data.**

| keibajo | Venue                | Total races (2019+) | With lap data | Coverage |
| ------- | -------------------- | ------------------- | ------------- | -------- |
| 42      | Morioka              | 5,027               | 4,984         | 99.1%    |
| 43      | Monbetsu             | 5,322               | 5,293         | 99.5%    |
| 44      | Ooi / Kawasaki / etc | 8,598               | 8,497         | 98.8%    |
| 45      | Urawa                | 5,638               | 5,568         | 98.8%    |
| 50      | Sonoda / Himeji      | 12,180              | 299           | 2.5%     |
| 30      | Sapporo (NAR)        | 7,074               | 0             | 0%       |
| 35      | Kanazawa             | 5,416               | 0             | 0%       |
| 36      | Kasamatsu            | 5,431               | 0             | 0%       |
| 46      | Kanazawa (main)      | 7,246               | 0             | 0%       |
| 47      | Morioka (NAR)        | 7,122               | 0             | 0%       |
| 48      | Funabashi            | 9,993               | 0             | 0%       |
| 51      | Himeji               | 2,085               | 0             | 0%       |
| 54      | Kochi                | 9,135               | 0             | 0%       |
| 55      | Saga                 | 9,543               | 0             | 0%       |

Overall NAR race coverage: **24.7%** (24,641 / 99,810 races 2019+).
Overall NAR horse-race row coverage: **27.7%** (283,672 / 1,023,462 rows 2019+).

This is **the same structural problem that caused B4's venue sign inconsistency**. Venues 54
(Kochi), 55 (Saga), 46 (Kanazawa), 48 (Funabashi), and 47 (Morioka) — which drove 8/14
negative signs in B4 — have **zero lap data**. A lap-level probe would be limited to venues
42-45, which are exactly the 4-6 venues that showed POSITIVE rho in B4. Any positive signal
found would be venue-selection artifact, not a generalizable stamina indicator.

### JRA Coverage — TESTABLE BUT OUT-OF-SCOPE

All 10 major JRA venues have good lap coverage (92-100%):

| keibajo | Venue     | Coverage |
| ------- | --------- | -------- |
| 01      | Sapporo   | 100.0%   |
| 02      | Hakodate  | 100.0%   |
| 03      | Fukushima | 92.1%    |
| 04      | Niigata   | 92.2%    |
| 05      | Tokyo     | 97.0%    |
| 06      | Nakayama  | 96.3%    |
| 07      | Chukyo    | 97.4%    |
| 08      | Kyoto     | 96.5%    |
| 09      | Hanshin   | 97.1%    |
| 10      | Kokura    | 91.9%    |

JRA race coverage: **57.5%** (24,758 / 43,054 races 2019+).  
JRA horse-race row coverage: **64.5%** (344,099 / 533,474 rows 2019+).

JRA lap data is dense and venue-balanced. However:

- The current production model and all active science-track probes are NAR-centric
- JRA feature parquet exists (`tmp/feat-v20-merged/jra/`) but B4 was not tested on JRA
- A JRA lap-level probe would be a separate, fresh hypothesis (not continuation of B4)

## Verdict

**DATA-BLOCKED for the primary target (NAR)**

The lap-level individual speed-fade hypothesis cannot be probed on NAR with the current
warehouse. The 4 venues with lap data (Morioka, Monbetsu, Ooi-group, Urawa) are exactly
the venues that showed positive rho in B4 — using them alone would produce a biased
positive-looking result that cannot generalize to the 10 venues with no lap data.

No probe was executed. The B4 `H-SPEED-FADE-INDEX` ABORT verdict stands unchanged.

**JRA probe is deferred** — the data exists and is dense, but requires:

1. A JRA-specific feature parquet refresh with lap-time features
2. Confirmed lap-time → individual deceleration construction (per-horse pace curve
   normalized by race-level pace)
3. Separate science-track entry targeting JRA model improvement

## Other Unused Candidate Columns (Future Probe Inventory)

Schema audit identified the following columns present in the warehouse but NOT consumed
by `finish_position_features_duckdb.py`:

### High-value candidates

| Column                  | Table              | Coverage                  | Notes                                                                                                                                                                                   |
| ----------------------- | ------------------ | ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kyakushitsu_hantei`    | `jvd_se`           | 98.3% JRA (2020+), 0% NAR | JRA-official per-horse running style (1=nige/front, 2=sentan/near-front, 3=sashi/mid, 4=oikomi/closer); fully dense for JRA; could validate/replace ML-predicted running style features |
| `wakuban`               | `nvd_se`, `jvd_se` | 100% NAR, 99.5% JRA       | Gate/barrier draw; fully dense; directional bias by gate position may affect pace scenario; worth probing as gate-number feature                                                        |
| `corner_tsuka_juni_1-4` | `nvd_ra`, `jvd_ra` | 100% all races            | Race-level position string at each corner (e.g. `315,8,4,...`); encodes pace order at each corner; could extract "front-runner density" or "position volatility" features at race level |
| `nyusen_juni`           | `nvd_se`           | 97.6% NAR                 | Official finish order; slightly different from `kakutei_chakujun` for dead heats; useful as probe ground-truth cross-check                                                              |
| `zenhan_3f`             | `nvd_ra`, `jvd_ra` | 24.7% NAR, 57.5% JRA      | Race-level first-half 3F time; SAME COVERAGE AS lap_time since sourced identically; useful for JRA "pace scenario" race feature (not horse feature)                                     |
| `kohan_4f`              | `nvd_ra`, `jvd_ra` | ~24-60%                   | Race-level final 4F time; wider than kohan_3f window; marginal over existing kohan_3f                                                                                                   |

### Low-value / too-sparse

| Column                | Table              | Coverage           | Notes                                                                                                       |
| --------------------- | ------------------ | ------------------ | ----------------------------------------------------------------------------------------------------------- |
| `mining_kubun`        | `jvd_se`           | 66.6% JRA          | JRA internal mining classification (3=classified, 0=unclassified); semantics unclear, likely administrative |
| `blinker_shiyo_kubun` | `jvd_se`           | 7.5% JRA           | Blinker use flag; only 7.5% populated, equipment signal but too sparse                                      |
| `ijo_kubun_code`      | `nvd_se`, `jvd_se` | 1.3% NAR, 1.0% JRA | Injury/scratch exception code; too sparse for feature use                                                   |
| `dochaku_tosu`        | `nvd_se`, `jvd_se` | 0.2%               | Dead-heat count; extremely rare                                                                             |
| `kyakushitsu_hantei`  | `nvd_se`           | 0% NAR             | NAR does not populate this field (all '0'); NAR-only running style must come from ML model                  |

### Priority ranking for future probes

1. **`kyakushitsu_hantei` (JRA)** — fully dense JRA running style ground-truth; could yield
   a `source_running_style` feature or be used to validate the ML-predicted `corner_1_norm`
   features; most actionable without additional engineering
2. **`wakuban` (gate number)** — 100% coverage both categories; gate bias in straight-side
   racing is a known effect (outer gates disadvantaged in short-distance dirt); simple feature
   with clear hypothesis direction
3. **`corner_tsuka_juni_1-4`** — 100% coverage; requires string parsing to extract race-level
   pace dynamics (e.g., number of horses within 2 lengths at corner 1); more complex
   engineering but high information content

## Comparison with Prior Science Track Entries

| Hypothesis                | Partial rho | Coverage                 | Verdict                              |
| ------------------------- | ----------- | ------------------------ | ------------------------------------ |
| V8 H-BABA-PAR-TIME        | 0.180       | 93%                      | PROCEED                              |
| B4 H-SPEED-FADE-INDEX     | 0.0725      | 96%                      | ABORT                                |
| **H-LAPLEVEL-SPEED-FADE** | **N/A**     | **27.7% NAR, 64.5% JRA** | **DATA-BLOCKED (NAR); JRA deferred** |

## Hard Rules Observed

- `tmp/laptime/` only: artifacts in `tmp/laptime/availability.json`
- No `git add tmp/`
- PG read-only: only SELECT queries through DuckDB postgres extension
- No DELETE/TRUNCATE/DROP issued
- No training or production change
- No re-test of the race-level B4 (ABORT verdict preserved)
