# H: jvd_jg Scratch/Withdrawal History Probe — ABORT

**Date:** 2026-06-12
**Purpose:** Determine whether per-horse historical scratch/withdrawal frequency
(from `jvd_jg`, 競走馬除外情報) is a usable orthogonal pre-race signal for the
finish-position model.

**Hypothesis:** Horses that scratch often may have recurring health/soundness
issues → worse performance when they do run.

**Overall verdict: ABORT — holdout partial ρ = −0.003 (<<0.08 bar); the raw
correlation is entirely explained by popularity (market odds). No independent
signal.**

---

## Gate 0: jvd_jg Verification

### Schema (14 columns)

| column                                                       | content                                                                                                            |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------ |
| `ketto_toroku_bango`                                         | horse ID (matches jvd_se)                                                                                          |
| `kaisai_nen`, `kaisai_tsukihi`, `keibajo_code`, `race_bango` | race keys                                                                                                          |
| `shusso_kubun`                                               | start status: `1`=ran, `2`=取消 (withdrawal), `4`=除外 (excluded), `5`=中止 (non-finish), `6`=失格 (DQ), `9`=other |
| `jogai_jotai_kubun`                                          | exclusion state: `0`=normal entry, `1`=withdrawal, `2`=exclusion                                                   |
| `data_sakusei_nengappi`                                      | data creation date                                                                                                 |

### Row counts and shusso_kubun distribution

| shusso_kubun | meaning           | rows    |
| ------------ | ----------------- | ------- |
| `1`          | ran normally      | 731,414 |
| `2`          | withdrawal (取消) | 77,737  |
| `4`          | excluded (除外)   | 3,828   |
| `5`          | non-finish (中止) | 166     |
| `6`          | DQ (失格)         | 1,479   |
| `9`          | other             | 8,537   |

**Total rows: 823,161.** Critical: jvd_jg records ALL entry/exclusion events
including horses that ran (`shusso_kubun='1'`, 731k rows). True scratches are
only `shusso_kubun IN ('2', '4')`: **81,565 rows**.

### Coverage

- **Year range:** 2011–2026 (full history, ~50k rows/year)
- **Keibajo codes:** 01–10 only (JRA central venues)
- **NAR equivalent:** `nvd_jg` does NOT exist (`nvd_jc` exists but is jockey,
  not scratch). This signal is **JRA-only**.
- **Pre-race nature:** `shusso_kubun='2'/'4'` events are pre-race withdrawals
  recorded before hasso; they are historically loaded but the race context
  (kaisai keys) is valid for computing prior-race history features.

### Horse-level scratch distribution (true scratches only)

| scratches | horses |
| --------- | ------ |
| 1         | 17,169 |
| 2         | 9,101  |
| 3         | 4,537  |
| 4         | 2,234  |
| 5+        | 2,239  |

Median ≈ 1 scratch per horse that has any. Most JRA runners (59%) have **zero**
prior true scratches.

---

## Gate 1: Feature Design (Leak-Free)

Three features, all strictly using `race_date_int < current race_date_int`:

| feature                    | definition                                                                                          |
| -------------------------- | --------------------------------------------------------------------------------------------------- |
| `prior_true_scratch_count` | total `shusso_kubun IN ('2','4')` events for this horse before this race date                       |
| `true_scratch_last_year`   | same count, only in the prior 365-day window                                                        |
| `days_since_last_scratch`  | integer date distance to most recent scratch event (not computed — not needed once partial ρ fails) |

### Feature coverage (JRA 2016–2026, N=498,946 runner-starts)

| metric                     | value                         |
| -------------------------- | ----------------------------- |
| has_prior_true_scratch > 0 | 203,728 / 498,946 = **40.8%** |
| avg prior scratch count    | 0.90                          |
| max prior scratch count    | 29                            |

Coverage is acceptable (40.8% non-zero). Serve-availability is **OK** —
historical scratch records are in local PG and would be accessible at inference
time.

---

## Gate 2: Within-Race Variation

```
Races with variation in prior_true_scratch_count:
  32,216 / 36,054 = 89.35%
```

**PASSES.** Within-race variation is high: 89% of races have at least one
horse with a different prior-scratch count than another horse in the same race.

---

## Gate 3: Spearman ρ and Partial ρ

### Raw Spearman ρ (within-race rank correlation, scratch count vs finish position)

| window          | career ρ  | last-year ρ | N rows  | N races |
| --------------- | --------- | ----------- | ------- | ------- |
| All 2016–2026   | 0.099     | 0.078       | 498,946 | —       |
| Holdout 2023–26 | **0.101** | **0.075**   | 162,799 | 11,871  |

Raw ρ ~0.10 looks superficially promising. Per-year consistency (0.07–0.12)
is stable and not decaying.

### Partial ρ controlling for popularity (odds proxy)

| window          | partial ρ  | N       |
| --------------- | ---------- | ------- |
| Holdout 2023–26 | **−0.003** | 162,799 |

**The partial ρ collapses to −0.003.** The entire raw correlation is captured
by the popularity feature.

### Confound diagnosis

Scratch group vs avg popularity rank and avg finish (holdout 2023–26):

| scratch group | avg_popularity_rank | avg_finish |
| ------------- | ------------------- | ---------- |
| 0 scratches   | 7.27                | 7.44       |
| 1 scratch     | 8.01                | 7.87       |
| 2–4 scratches | 8.69                | 8.15       |
| 5+ scratches  | 10.48               | 8.94       |

Horses with more prior scratches have **higher popularity rank numbers** (i.e.
they are longer odds / less favored). This is expected: lower-quality horses
both scratch more often and are less fancied by the market. The market already
prices the soundness/quality signal embedded in scratch history.

### Within-popularity-quintile ρ breakdown (pop-controlled binning)

| Pop quintile      | ρ within bin |
| ----------------- | ------------ |
| Q1 (favorite)     | +0.046       |
| Q2                | +0.030       |
| Q3                | +0.016       |
| Q4                | −0.009       |
| Q5 (longest odds) | −0.040       |

The within-bin ρ is small (≤0.046) and **sign-flip across quintiles** — scratchy
favorites still run relatively well; scratchy outsiders are slightly worse
than expected. This is incoherent as a standalone signal and has zero
systematic direction after pop-adjustment.

### Avg finish by scratch status within same popularity rank

Holding popularity rank constant (ranks 1–5 within race, holdout 2023–26):

| pop_rank | no_scratch avg_finish | has_scratch avg_finish | delta |
| -------- | --------------------- | ---------------------- | ----- |
| 1 (fav)  | 3.408                 | 3.721                  | +0.31 |
| 2        | 4.341                 | 4.569                  | +0.23 |
| 3        | 4.993                 | 5.297                  | +0.30 |
| 4        | 5.582                 | 5.692                  | +0.11 |
| 5        | 6.186                 | 6.260                  | +0.07 |

The residual effect at the same popularity rank is 0.07–0.31 positions.
This tiny absolute delta disappears under partial ρ because odds already
partially embed this signal and it isn't orthogonal.

### Scratch vs popularity redundancy

```
Scratch rank vs popularity rank (within-race): ρ = 0.18
```

This confirms **moderate collinearity** (ρ=0.18) between scratch history and
market odds, consistent with the confound diagnosis.

---

## Gate 4: Serve Availability

- **Source table:** `jvd_jg` is in local PG, synced daily.
- **Timing:** Prior-race scratch history is fully computable at serve time —
  there is no kubun/timing issue (unlike jvd_dm). All `shusso_kubun='2'/'4'`
  events are pre-race by definition.
- **NAR availability:** `nvd_jg` does NOT exist → this feature would be
  JRA-only. NAR coverage = zero.

**PASSES for JRA. FAILS for NAR.**

---

## Summary of Gates

| gate                                   | result               | deciding number                               |
| -------------------------------------- | -------------------- | --------------------------------------------- |
| G0 Verification                        | PASS                 | 81,565 true scratch rows, 2011–2026, JRA-only |
| G1 Feature design (leak-free)          | PASS                 | strictly prior race_date                      |
| G2 Within-race variation               | PASS                 | 89.35% of races vary                          |
| G3 Raw Spearman ρ (holdout)            | PASS (superficially) | 0.101                                         |
| G3 Partial ρ (pop-controlled, holdout) | **FAIL**             | **−0.003** (bar: ≥0.08)                       |
| G4 Serve availability JRA              | PASS                 | historical PG, no timing issue                |
| G4 NAR coverage                        | FAIL                 | nvd_jg does not exist                         |

---

## Overall Verdict: ABORT

**Deciding number: holdout partial ρ = −0.003.**

The scratch-frequency signal is entirely subsumed by market popularity (odds).
Lower-quality horses both scratch more and trade at longer odds; the market has
already priced this in. After controlling for popularity, there is no
incremental information in scratch history (ρ ≈ 0).

The raw ρ=0.10 is real but is purely a proxy for the odds signal already in
the model. Adding scratch features would produce zero marginal gain, and the
model already routes through popularity optimally.

**Additional reasons not to proceed:**

1. NAR has no scratch-history table (`nvd_jg` does not exist), so this would
   be a JRA-only feature, adding asymmetry to the feature set.
2. Coverage is only 40.8% non-zero even for JRA — the majority of runners
   have no prior scratch, making this a sparse feature.
3. The within-popularity-quintile ρ sign-flip (positive in low odds quintiles,
   negative in high odds quintiles) indicates the residual is noise rather
   than a clean directional signal.

**Recommendation:** Do not integrate jvd_jg scratch features. The odds/popularity
signal already captures horse-quality variation including scratch propensity.
