---
document_type: cycle-summary
date: 2026-06-11
cycle_scope: NAR / JRA / Ban-ei — multi-class ensemble accuracy
hypotheses_tested: 8
adopted_changes: 1 (process fix — commit 03a5fd4)
new_baseline_recorded: Ban-ei holdout 2023-2026 (first ever)
overall_verdict: ALL 8 accuracy hypotheses REJECT or ABORT; v7-lineage saturation now extends to Ban-ei
---

## 1. Overview

**Objective.** Advance top1/place2/place3 accuracy across all three racing categories (NAR
per-class ensemble, JRA per-class ensemble, Ban-ei) via the science-track journal pipeline.
All 3 finishing positions were treated with equal priority; all SubAgent workstreams ran
in parallel.

**Outcome headline.** Eight accuracy hypotheses were tested — five probe-level (ABORT before
full retrain), two full-retrain judges (REJECT after powered holdout), and one combined
baseline+HPO+probe entry. Every hypothesis failed to clear the powered 4-axis accept gate.
V7-lineage saturation, previously confirmed for NAR (iter32–35) and JRA (iter18), now
extends to Ban-ei: HPO headroom is negligible (best HPO trial regresses top1 −0.201pp) and
the first-ever sectional/race-internal retrain also rejects.

The one concrete improvement was a **process fix** (commit 03a5fd4): double-staging of
`jra_um`/`jra_ra` tables removed and per-category odds/popularity median fallback added,
eliminating a train/serve distribution skew for UPCOMING races.

A first-ever formal Ban-ei holdout baseline was recorded (top1 0.34404 / place2 0.55890 /
place3 0.43173 / box 0.09237 on 5,976 races, 2023-2026), establishing the reference point
for all future Ban-ei experiments.

---

## 2. Results Table

| #           | Hypothesis / Item                                                                           | Category           | Deciding Number                                                                                                                                    | Verdict                          |
| ----------- | ------------------------------------------------------------------------------------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------- |
| V8-judge    | H-BABA-PAR-TIME judge (177 features, 7 NAR classes)                                         | NAR                | Class C narrowest miss: LB95 = −0.054pp; all 7 classes FAIL gate (best: Class C 3/4 axes positive, bar LB95 > 0)                                   | **REJECT**                       |
| B1          | H-PREV-BW-DROP (prev-race BW delta)                                                         | NAR + JRA          | Partial ρ NAR = +0.027, JRA = +0.016; bar = 0.08                                                                                                   | **ABORT**                        |
| B2          | H-AGE-SEX-BW-ZSCORE (growth-curve BW z-score)                                               | NAR                | Partial ρ = −0.0264; bar = 0.08                                                                                                                    | **ABORT**                        |
| B3          | H-SIRE-SURFACE-SPLIT (sire turf vs dirt win-rate)                                           | JRA                | Holdout (2023-2026) partial ρ = −0.0362; bar = 0.08                                                                                                | **ABORT**                        |
| B4          | H-SPEED-FADE-INDEX (late-vs-early 3F deviation)                                             | NAR                | Partial ρ = 0.0725 < bar 0.08; venue sign inconsistent (8/14 venues negative)                                                                      | **ABORT**                        |
| JRA-guard   | JRA lgb-lambdarank transfer guard (single-year)                                             | JRA                | Class 703 test-2023 Δtop1 +0.733pp; Class 005 test-2023 Δtop1 +0.428pp; no collapse → PROCEED to full retrain                                      | **PROCEED** (guard only)         |
| JRA-judge   | JRA lgb-lambdarank full judge (4-year holdout, nested HPO)                                  | JRA                | Class 703: all 4 axes negative (top1 −0.2365, LB95 top3_box = −0.4436pp); Class 005: all 4 axes negative (top1 −0.4766, LB95 top3_box = −0.8113pp) | **REJECT**                       |
| Ban-ei-HPO  | Ban-ei baseline + HPO (n_trials=40)                                                         | Ban-ei             | HPO best: top1 −0.201pp vs baseline (exceeds −0.05pp gate); futan-ratio probe partial ρ = −0.030 < 0.08                                            | **REJECT (HPO) / ABORT (probe)** |
| Ban-ei-sect | H-BANEI-SECTIONAL-RACEINTERNAL (soha_time z-scores + race-internal ranks)                   | Ban-ei             | Probe: `past_time_sa_dev_avg5` partial ρ = 0.0825 → PROCEED; Retrain LB95 top1 = −1.071pp                                                          | **REJECT**                       |
| ADOPTED     | Fix: remove double-staging jra_um/jra_ra + odds/popularity median fallback (commit 03a5fd4) | JRA + NAR + Ban-ei | Eliminates 2 redundant PG round-trips; COALESCE to per-category medians (JRA: odds 0.5664, pop 0.5000; NAR/Ban-ei: odds 0.5048, pop 0.5000)        | **ADOPTED**                      |
| BASELINE    | Ban-ei holdout baseline (first ever, 5,976 races 2023-2026)                                 | Ban-ei             | top1 0.34404 / place2 0.55890 / place3 0.43173 / top3_box 0.09237                                                                                  | **RECORDED**                     |

---

## 3. Methodological Lessons

### (a) Single-test-year transfer guards produce false positives — multi-year powered holdout is required for PROCEED

The JRA lgb-lambdarank transfer guard (2023 only) passed both powered classes convincingly:
Class 703 +0.733pp top1, Class 005 +0.428pp top1, no iter13 collapse pattern. The full
multi-year nested-HPO holdout (2023–2026 pooled) inverted the result entirely: 703 flipped
to −0.2365pp and 005 to −0.4766pp, with all 4 axes negative on both classes. The
single-year readings were sampling noise around a true negative effect. A single test year
(n≈930–1230 races) is too thin to distinguish a real residual gain from variance at
sub-percentage-point effect sizes. Future residual/blend experiments must be backed by a
multi-year expanding-window holdout with a powered judge (bootstrap LB95 + multi-axis
no-regression gate). The cheap guard's PROCEED means "not obviously broken, worth the full
retrain to find out" — never positive evidence of transfer.

### (b) Par-time / going normalization = race-level selection bias, not horse-level adaptation

H-BABA-PAR-TIME (V8) produced a genuine probe signal (partial ρ = 0.180, the highest of
any probe in the history). After full walk-forward retrain, all 7 NAR classes failed the
powered gate. Root cause: `baba_adj_centered` is a within-race deviation (horse adj speed
minus race avg adj speed). It captures relative speed on a going-adjusted basis but is
confounded by race-condition selection bias — horses with many heavy-going starts are
systematically different (lower grade, rural venues, different trainer strategies). The
par-time correction measures comparative going-day performance, NOT each horse's capacity
to ADAPT to different going types. A genuine horse-level going-adaptation signal would
require a history of performance variance across baba codes per horse, which is a distinct
feature not yet probed.

### (c) Probes must clear the bar in the HOLDOUT window (2023-2026), not just on the full period — full-pass / holdout-decay is a reliable abort signal

Three cases in this cycle exposed the danger of a full-period partial ρ that fails in the
holdout:

- **H-BABA-PAR-TIME** (V8-judge): the par-time table frozen at 2007–2017 loses calibration
  validity in the 2023–2026 holdout window; Class C's narrow miss (LB95 = −0.054pp just
  below zero) is consistent with the signal decaying in recent years.
- **H-BANEI-SECTIONAL** (Ban-ei): `past_time_sa_dev_avg5` full-period partial ρ = 0.0825
  (just above bar) decays to ≈ 0.047 in 2023–2026, far below the bar. The historical
  signal was entirely carried by 2007–2013 races; the retrain LB95 = −1.071pp confirmed
  the pattern.
- **H-SIRE-SURFACE-SPLIT** (B3): holdout (2023-2026) partial ρ = −0.036 vs full-period
  −0.041 — modest decay but the holdout number is still 2× below bar in the decision window.

For all future probes, the primary decision number is the holdout-window partial ρ. Where
a probe is reported only on the full period, a temporal breakdown is mandatory before
a PROCEED decision.

### (d) Within-race-rank layers (futan/bataiju/grade/career/pedigree) are near-worthless for Ban-ei; corner_1/3/4 hill-time columns are all-zero in the DB

The Ban-ei sectional/race-internal experiment revealed two important data facts:

1. **`grade_rank_rank_in_race` = constant**: all horses in a Ban-ei race share the same
   grade, so the within-race grade rank has zero variance. Any feature derived from a
   within-race ranking of grade is uninformative by construction.
2. **`futan_kg_rank_in_race` and `bataiju_kg_rank_in_race`**: partial ρ ≈ 0.01–0.03,
   far below bar. `current_futan_class` already captures futan_kg at bucket resolution;
   the exact-kg deviation adds only noise.
3. **`corner_1`, `corner_3`, `corner_4`**: all zero for all 324,491 Ban-ei rows (2005–2026).
   Ban-ei corner/hill checkpoint timing data is NOT present in this DB. Any feature
   engineering relying on these columns yields all-zero signals with zero predictive power.

The only sectional signal available is `soha_time` and `time_sa`, and even those are largely
collinear with the existing `speed_index_avg_5` (ρ = 0.72 in recent years).

---

## 4. Noted Data-Quality Issue

**`sire_track_win_rate` has only 2.5% coverage** (18,995 / 746,125 JRA rows). Root cause:
`build-pedigree-sql.ts::buildSireTrackStatsCte()` is called inside a date-window UPDATE
statement (`race_date BETWEEN $1 AND $2`) and aggregates without a strict time cutoff —
making it a leaky snapshot covering only the small regeneration window rather than a
continuous leak-free feature. (Confirmed in B3 probe: B3 rebuilt a true leak-free
version from scratch, lifting coverage to 87.8%.) The signal from `sire_track_win_rate` at
2.5% coverage is weak by construction; the issue is low-priority because the existing
sire quality features (`sire_total_wr` at 88% coverage, `pedigree_score_for_race`) already
capture most of the sire quality signal. Logged here for future codebase audit.

---

## 5. Forward Proposals

Each of the following requires more than a probe before it can reach a production decision.

### (i) Lap-level per-furlong individual speed-fade, IF the warehouse holds lap times

H-SPEED-FADE-INDEX (B4) was the closest ABORT in this cycle (partial ρ = 0.0725, just below
the 0.08 bar) and the feature is genuinely orthogonal to the existing kohan3f family (max
correlation 0.034). The current operationalisation uses race-level `zenhan_3f / kohan_3f`
aggregates, which conflate pace-scenario with individual stamina. A more granular late-
deceleration index from per-lap splits (`nvd_ra.lap_time`) could reduce this confound
substantially: if `lap_time` is populated in the warehouse with per-200m splits, computing
the deceleration in the FINAL 2 laps vs the horse's own historical distribution would
isolate the stamina signal from the pace-scenario. This investigation (does the warehouse
hold per-lap splits for NAR?) is currently in flight. If lap times are available, a
horse-level per-furlong fade feature is the highest-ranked probe candidate for rescuing
the B4 signal given its closeness to the bar and its genuine orthogonality.

### (ii) NAR class B lgb-lambdarank residual — ALREADY TESTED; further retrain is NOT a proven-recipe unused lever

A thorough review of docs history confirms that **NAR class B WAS given the lgb-lambdarank
residual treatment in H4 (oi-2026-06-10-wave1-h1-h5.md)**. The result:

- H4 class B: top1 +0.239pp, place2 +0.295pp (3/4 axes positive) but **place3 −0.421pp**
  (breaches −0.05pp floor) and top1 LB95 = −0.000983 < 0 (Holm-adj p = 0.1706). REJECT.

This was a full per-class residual blend with a fresh Optuna HPO winner (lightgbm:lambdarank
WF top1 0.56731 vs YetiRank baseline 0.56456), tuned on the correct judge slice, judged on
the powered full-NAR holdout (2023-2026) with bootstrap LB95 + multi-axis no-regression +
Holm. **NAR class B has therefore already received the same lgb-lambdarank treatment that
produced the NAR-C iter36 win — and REJECTED.** It is NOT an untested proven-recipe lever.
The failure mode is the same as the JRA transfer: the residual sharpens top1 at the cost of
place3 ordering (−0.421pp), which the multi-metric gate correctly rejects. Re-running with
the same recipe would reproduce the same result. Any future NAR-B advance requires either
a new signal (not yet probed) or a place-aware co-objective that does not sacrifice place3.

### (iii) Genuinely-new signals (odds drift, intra-day) are constrained by 21-year historical training data availability

Odds movement (drift, velocity of odds change before the off) and intra-day signals (same-
card weather, going update, late scratches) are the highest-potential new levers identified
across the NAR/JRA saturation analysis (Wave 1 / iter32-35 conclusion). A critical
constraint: the training corpus covers 2005–2022 (21-year window) and was built from
historical JV-Data snapshots. Intra-day odds drift in the historical data is either
absent or poorly structured for the pre-2015 window. Any new signal derived from live odds
snapshots is only learnable on the post-2015 portion of the training data (~8 years), which
may not provide enough label density for a per-class expanding-window retrain. Investigation
of what historical intra-day data is actually available in the warehouse is a prerequisite
before this lever can be assessed for feasibility.
