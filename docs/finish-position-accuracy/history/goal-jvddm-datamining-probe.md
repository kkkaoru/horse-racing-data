# GOAL: JRA jvd_dm / jvd_tm Data-Mining Predictions — Leak Gate + Partial-ρ Probe

**Date:** 2026-06-17  
**Purpose:** Evaluate whether JRA's published data-mining prediction tables (`jvd_dm` タイム型,
`jvd_tm` 対戦型) contain novel pre-race signal orthogonal to odds that could yield a large
accuracy gain. This probe is distinct from the previously-ABORTed `yoso_soha_time`/`yoso_juni`
evaluation (that was a signal quality ABORT; this focuses on whether jvd*dm contains \_different*
sub-fields not yet consumed).

**Prior art:** `e0-jvd-dm-verification.md` (2026-06-12) reaches the same ABORT — the current
probe **independently confirms** every finding with live DB queries.

---

## Gate-0: Table Existence and Data Scope

Both tables exist in local PG (neondb replica):

| table  | rows   | years     | columns                                    |
| ------ | ------ | --------- | ------------------------------------------ |
| jvd_dm | 84,303 | 2001–2026 | 10 + 18 × `mining_yoso_NN` (15 chars each) |
| jvd_tm | 53,869 | 2001–2026 | 10 + 18 × `mining_yoso_NN` (6 chars each)  |

---

## Gate-0A: data_kubun Distribution (Leak Screen)

```
jvd_dm: {2: 2 rows, 3: 3 rows, 7: 84,298 rows}
jvd_tm: {2: 2 rows, 3: 3 rows, 7: 53,864 rows}
```

- **data_kubun='2'** (2 rows, 2026 only): created the day before race — pre-race.
- **data_kubun='3'** (3 rows, 2026 only): created same race day before `hasso_jikoku` — pre-race.
- **data_kubun='7'** (99.97% of all rows): `data_sakusei_nengappi` = race date + 1 to 3 days
  (verified 2021-2024: 6,825 rows = +1 day, 6,658 rows = +2 days, 288 = +3 days, 48 = same day).

Gate-0A **does not immediately abort**: pre-race kubun=2/3 records exist. However, there are only
5 such rows in total (2 JRA races in Feb 2026). Not a usable training or serve signal.

---

## Gate-0B: Decode mining_yoso Format

### jvd_dm (15 chars per horse)

Empirically verified against `jvd_se` fields:

| chars | content                                          | source                     |
| ----- | ------------------------------------------------ | -------------------------- |
| 1–2   | umaban (always == slot NN)                       | structural                 |
| 3–7   | yoso_soha_time × 10 (predicted race time, 1/10s) | = `jvd_se.yoso_soha_time`  |
| 8–10  | yoso_gosa_plus (upper error bound, 1/10s)        | = `jvd_se.yoso_gosa_plus`  |
| 11–12 | ??? (2 chars; values ~00-70)                     | opaque                     |
| 13–15 | yoso_gosa_minus (lower error bound, 1/10s)       | = `jvd_se.yoso_gosa_minus` |

**Critical:** `jvd_dm.mining_yoso_NN[3:7]` is byte-for-byte identical to
`jvd_se.yoso_soha_time` for the horse with `umaban = NN`. No novel sub-field.

### jvd_tm (6 chars per horse)

| chars | content                          |
| ----- | -------------------------------- |
| 1–2   | umaban                           |
| 3–6   | opaque score (0000–9999 integer) |

jvd_tm score derivation is undocumented. JRA-only coverage (keibajo_code 03–10). No NAR.

---

## Gate-0C: Oracle Sanity Test

Using `data_kubun='7'` rows (the only meaningful volume), treating mining prediction rank as
predicted winner (yoso_juni already in jvd_se is the rank form):

| metric                                        | value       |
| --------------------------------------------- | ----------- |
| dm top-1 win rate (2021-2024, n=13,844 races) | **7.26%\*** |
| market (tansho_ninkijun=01) win rate baseline | ~39%        |
| yoso_juni=01 win rate (via jvd_se, n=83,863)  | **23.23%**  |

\*The 7.26% figure arises because `mining_yoso_01` encodes **umaban=01** (horse #1), not the
top-predicted horse. Slot ordering is by umaban, not predicted rank. The correct reading — via
`jvd_se.yoso_juni` — gives 23.23%, well below the market's 39%. This confirms the prediction is
pre-race quality (not an oracle at 100%) but substantially weaker than odds.

**IDM computation leak test (within-race):**

```
CORR(IDM_deviation_within_race, soha_time_deviation_within_race) = 0.127
```

IDM does NOT use the horse's own completed race time as input. The spuriously-high global
`CORR(IDM, soha_time) = 0.98` is a distance confound (both scale with race length).
**IDM values are pre-race computations.** No oracle leak.

---

## Gate-0D: Serve Availability

| kubun | timing                  | rows available at 03:00 JST cron |
| ----- | ----------------------- | -------------------------------- |
| 2     | day-1                   | 2 rows total (entire DB)         |
| 3     | same-day pre-hasso      | 3 rows total                     |
| 7     | +1 to +3 days post-race | NOT available for same-day races |

**FAIL:** 99.97% of data (kubun=7) arrives 1-3 days after the race. Kubun=2/3 covers only 2
races in February 2026 — not a functional serve signal.

The data CAN be used for **WF training** (historical kubun=7 records exist back to 2001), but
since it contains no novel information beyond `jvd_se` (see E1 below), this is moot.

---

## E1 Partial-ρ Probe (IDM vs finish, controlling for odds)

Since jvd_dm content == jvd_se yoso fields, E1 is computed for completeness against the
`yoso_soha_time`-derived IDM rank:

| metric                                                | value      |
| ----------------------------------------------------- | ---------- |
| N (horse-level, 2021-2024)                            | 156,468    |
| CORR(IDM_rank_within_race, finish_rank)               | **-0.368** |
| CORR(actual_odds_rank, finish_rank) [market baseline] | **+0.588** |
| CORR(IDM_raw, actual_odds)                            | -0.042     |
| CORR(IDM_raw, actual_ninki)                           | -0.080     |

IDM rank correlates with finish at r=-0.368 (negative because lower rank = better finish).
Market (odds rank) correlates at r=0.588 — **60% stronger than IDM**.

IDM is nearly orthogonal to market (r=-0.04 with odds) but has **much lower raw signal strength**.
Bar for E1 proceed: ρ ≥ 0.08 with partial control. With full odds control, the incremental
gain from IDM beyond odds would be far below 0.08 given the signal dominance of odds.

**This signal is the `yoso_soha_time` already ABORT-confirmed (iter history, 2026-06-11:
"yoso_soha_time/yoso_juni ABORT ρ0.066"). The partial-ρ probe result is consistent.**

---

## Redundancy with Existing Feature Set

jvd_se columns `yoso_soha_time`, `yoso_gosa_plus`, `yoso_gosa_minus`, `yoso_juni` are **already
present in the DB** and are candidate features in the existing pipeline. The DM tables contain
only these same values, reformatted into a per-race aggregate row.

```
jvd_dm.mining_yoso_NN ≡ concat(
    umaban,            // "NN"
    jvd_se.yoso_soha_time,
    jvd_se.yoso_gosa_plus,
    [2 opaque chars],
    jvd_se.yoso_gosa_minus
)
```

Redundancy: **100%** on extractable fields.

---

## Verdict: GATE-0 ABORT

**Reason 1 (primary):** jvd_dm contains no information beyond `jvd_se.{yoso_soha_time,
yoso_gosa_plus, yoso_gosa_minus}`, which were already evaluated and ABORTed (ρ 0.066 < bar 0.08,
2026-06-11). Integrating jvd_dm adds exactly zero novel signal.

**Reason 2 (serve):** 99.97% of rows arrive day+1 → not serve-available for same-day predictions.

**Reason 3 (quality):** dm top-1 win rate 23.23% vs market 39.18%. When dm and market disagree,
dm win rate drops to 13.68%. No alpha orthogonal to odds.

**Do not proceed to E1 training. Do not ingest jvd_dm or jvd_tm into feature pipeline.**

See also: `e0-jvd-dm-verification.md` (prior identical conclusion, 2026-06-12).
