# Per-Class Accuracy Campaign — Master Roadmap

**Date created**: 2026-06-17  
**Campaign goal**: top1 ≥ 60% / place2 ≥ 50% / place3 ≥ 40% per class. Adopt ANY confirmed
per-class improvement even if absolute targets remain unmet.

---

## §1. Metric Definitions (canonical)

From `aggregate_bucket_eval_duckdb.py:341-350`:

```
top1    := per-race max(predicted_rank=1  AND finish_position=1)
place2  := per-race max(predicted_rank=2  AND finish_position=2)
place3  := per-race max(predicted_rank=3  AND finish_position=3)
top3_box := per-race cast(all of {rank≤3} have {finish≤3} = 3)
```

All four are **exact-ordinal** (place2=50% means the horse ranked 2nd by the model finished
exactly 2nd in 50% of races — not "within the top 3").

**Critical caveat on place2/place3 targets**: `goal-baseline-and-ceiling.md` proves the
exact-ordinal oracle ceiling is JRA place2 ~18% / NAR place2 ~23% — both far below 50%. The
targets are aspirational. The campaign rule is: **adopt any confirmed per-class improvement**.

---

## §2. Per-Class Baseline and Headroom Map

### JRA (holdout 2023-2026, iter19 production, n=11,703 total)

| Class     | Code | n races | top1% | place2% | place3% | Gap to 60% | Mdl−Mkt | Power  |
| --------- | ---- | ------: | ----: | ------: | ------: | :--------: | ------: | :----- |
| 未勝利    | 703  |   4,229 | 49.40 |   25.80 |   19.06 |  −10.60pp  |  +14.31 | HIGH   |
| 1勝クラス | 005  |   3,147 | 40.99 |   20.46 |   15.44 |  −19.01pp  |   +9.09 | HIGH   |
| 新馬      | 701  |     953 | 45.02 |   23.92 |   20.46 |  −14.98pp  |  +11.02 | MEDIUM |
| 障害      | 999  |   1,064 | 42.01 |   23.03 |   16.35 |  −17.99pp  |  +13.63 | MEDIUM |
| 2勝クラス | 010  |   1,583 | 43.02 |   20.78 |   14.66 |  −16.98pp  |   +7.83 | MEDIUM |
| 3勝クラス | 016  |     727 | 37.55 |   20.91 |   12.79 |  −22.45pp  |   +5.64 | LOW    |

**Headroom notes**:

- All classes: MODEL_EXCEEDS_ORACLE (model already beats Harville odds-based ceiling).
- 703 (未勝利): Largest n, highest top1, strongest Mdl−Mkt gap. **Best candidate for JRA experiments.**
- 016 (3勝クラス): Lowest top1, lowest Mdl−Mkt, smallest n. Most headroom in % gap; but low power.
- 999 (障害): Strong Mdl−Mkt gap (+13.63pp); jump-race dynamics differ from flat; unique signals possible.
- 701 (新馬): debut horses have zero prior race history — a structural feature gap that is untested.

### NAR (holdout 2023-2026, production per-class config, n=45,572 total)

| Class   | n races |  top1% | place2% | place3% | Gap to 60%  | Active ensemble                   | Power     |
| ------- | ------- | -----: | ------: | ------: | :---------: | --------------------------------- | :-------- |
| C       | ~26,060 | ~47-58 |  ~25-36 |  ~18-27 | −2 to −13pp | iter36-nar-lgb-ensemble-C-v8      | VERY HIGH |
| B       | ~7,124  | ~51.41 |  ~29.00 |  ~21.09 |   −8.59pp   | iter12-nar-xgb-hpo-v8 (fallback)  | HIGH      |
| 3YO     | 8,578   |  59.63 |   36.21 |   27.93 |   −0.37pp   | iter12 via other                  | HIGH      |
| other   | ~7,217  | ~52.48 |  ~29.90 |  ~18.38 |   −7.52pp   | iter30-nar-cb-ensemble-other-v8   | HIGH      |
| A       | ~2,812  |    ~56 |     ~33 |     ~26 |    ~−4pp    | iter30-nar-cb-ensemble-A-v8       | MEDIUM    |
| 2YO     | 2,176   |  60.85 |   37.27 |   31.30 |  +0.85pp ↑  | iter12 via other                  | MEDIUM    |
| OP      | ~1,231  |    ~63 |     ~38 |     ~29 |  ABOVE 60%  | iter30-nar-cb-ensemble-OP-v8      | MEDIUM    |
| NEW     | ~573    |      — |       — |       — |   unknown   | iter30-nar-cb-ensemble-NEW-v8     | LOW       |
| MUKATSU | ~556    |      — |       — |       — |   unknown   | iter30-nar-cb-ensemble-MUKATSU-v8 | LOW       |

**Headroom notes**:

- 2YO and OP already meet top1 ≥ 60% target. 3YO is at 59.63% (−0.37pp from target).
- C: most statistical power (~26k races); directional near-misses in Wave1 (top1 trading place).
- B: no ensemble registered; the only NAR class without a per-class model. Prime candidate.
- 3YO + 2YO: age-specific features not yet in the feature set; iter37 showed residual alone is insufficient — need new age-specific signals.
- NEW/MUKATSU: small n (< 600 races); low power for robust verdicts.

### Ban-ei (holdout 2023-2026, banei-cb-v7-lineage-wf-21y, n=5,928 total)

| Class     | n races | top1% | Status              | Notes                       |
| --------- | ------: | ----: | ------------------- | --------------------------- |
| E_general |  ~5,490 |   ~34 | SATURATED           | Bulk of races; no headroom  |
| E_named   |    ~438 |     — | unknown             | Named grade races           |
| QR_upper  |    ~123 |     — | unknown             | Q+R combined                |
| P         |     ~25 |     — | futan signal PASSES | Very small n; high variance |
| T         |     ~22 |     — | unknown             | Very small n                |

**Headroom notes**:

- Ban-ei global model SATURATED at market level (top1 +0.15pp vs market).
- Only P-class shows strong futan-load signal (partial ρ ≈ 0.18) but n=25 races is far too small.
- No viable short-term per-class path for Ban-ei unless a multi-year P-class dataset is assembled
  (~60-90 races over 3-5 years).

---

## §3. DO-NOT-RETEST Registry

The following approaches were rigorously rejected; running them again would waste compute
and risk false accepts (these are not borderline — they were correctly gated out):

1. **JRA per-class kNN/pgvector member** (`iter32-jra-vec-knn-{class}-v8`): all 5 JRA classes
   REJECTED. 005 near-miss (4/4 axes positive, LB95 −0.000953). Gate correctly rejected.
   Ref: `history/oi-2026-06-10-rounds-r2-r4-pgvector.md`.

2. **NAR kNN similarity member** (`iter32-nar-vec-knn-{class}-v8`): all 7 NAR classes REJECTED.
   Optimizer assigns ≈0 weight — orthogonal variance is target-noise, not target-signal.
   Ref: `history/oi-2026-06-10-r5-nar-similarity-member.md`.

3. **NAR field-relative / recency features (H1-H3)**: odds ordinal-rank, trajectory, meta-learner
   — all REJECTED. Features already present in baseline or carry sub-noise signal.
   Ref: `history/oi-2026-06-10-wave1-h1-h5.md`.

4. **NAR per-class HPO (H4)**: LightGBM LambdaRank beats YetiRank on inner CV→WF but fails
   powered holdout gate. Per-class models are NOT under-capacity.
   Ref: `history/oi-2026-06-10-wave1-h1-h5.md`.

5. **NAR 2YO/3YO per-class CB residual** (iter37): REJECTED for both. Residual adds no
   signal without age-specific features.
   Ref: `history/phase3-nar-2yo3yo-perclass.md`.

6. **JRA relationship features per-class**: all probes ABORT/REJECT (GBDT already captures
   nonlinear combinations).
   Ref: `history/jra-relationship-features-perclass.md`.

7. **NAR G1+F1 retrain** (bug fixes): fixing training bugs worsens serve metrics −0.63pp.
   NULL routing by GBDT is already optimal; "fixing" informative NULLs is counterproductive.
   Ref: `history/g1-f1-combined-nar-retrain-judge.md`.

---

## §4. Multi-Method Per-Class Hypothesis Roadmap

### Method Categories

| Tag   | Method                          |
| ----- | ------------------------------- |
| ML    | Per-class GBDT tuning/training  |
| FEAT  | New feature engineering         |
| VEC   | pgvector / embedding similarity |
| RL    | Reinforcement learning ranker   |
| CALIB | Calibration / post-processing   |
| CS    | Algorithmic / combinatorial     |

---

### TOP 5-8 CANDIDATES TO RUN FIRST

Ranked by (headroom × feasibility × statistical power × prior probability of success):

---

#### CANDIDATE 1: NAR B-class dedicated per-class model [ML, HIGH priority]

**Class**: NAR B  
**Method**: ML — dedicated per-class GBDT (CB YetiRank, same recipe as iter30)  
**Rationale**: B is the ONLY NAR class with no ensemble registered. It routes to the global
iter12 fallback. n ~7,124 is well-powered (MDE ~1.5pp at 80% power). If the other 6 NAR
classes gained from a per-class residual, B should too — it was not tried, not rejected.  
**Design**: standard iter30 recipe (iter12 anchor + CB residual, nested split 2018-22 + holdout
2023-26); strengthened gate (≥2 of 4 axes positive, ≥1 place positive, no axis < −0.05pp,
LB95 > 0).  
**Expected lift**: ~0.5-2pp top1 (similar to other NAR classes at iter30 activation).  
**Risk**: Same saturation diagnosis applies — residual over iter12 may find no signal beyond what
the global model already captures. BUT: this is the only untried NAR class.  
**File**: `nar/class-B.md`

---

#### CANDIDATE 2: NAR 3YO age-specific features + full retrain [FEAT + ML, HIGH priority]

**Class**: NAR 3YO  
**Method**: FEAT — add age-specific features not in the current 174-col set, then full retrain  
**Rationale**: iter37 proved the per-class CB residual adds no signal for 3YO on existing features.
The root cause diagnosis: feature set lacks age-specific signals. 3YO has n=8,578 (good power,
MDE ~1.3pp). The model is already at 59.63% top1 — a small lift clears the 60% target.  
**Candidate features** (not in current 174 cols):

- `career_race_count` as an age proxy (debut=0, 2nd race=1, etc.)
- `foal_month` (relative age within cohort — well-studied in thoroughbred racing)
- `days_since_first_race` (time-since-debut signal)
- `races_in_current_season` (within-season form freshness)
- Interaction: `foal_month × kyori` (some birth-month effects are distance-specific)

**Design**: partial-ρ probe (each candidate vs finish_pos, controlling for log(odds), in the
3YO subset) → gate ρ ≥ 0.08. Then incremental model retrain with surviving features + nested
holdout eval.  
**Why this differs from iter37**: iter37 used existing features; this adds new age-specific
signals. The residual approach failed not because the class is unpredictable but because there
was nothing new for the residual to learn.  
**File**: `nar/class-3YO.md`

---

#### CANDIDATE 3: JRA 016 (3勝クラス) — class-specific feature probe [FEAT + ML, MEDIUM priority]

**Class**: JRA 016 (3勝クラス)  
**Method**: FEAT — probe class-specific structural signals for 3勝クラス  
**Rationale**: 016 has the lowest Mdl−Mkt gap (+5.64pp) and lowest top1 (37.55%) — most
headroom in relative terms. The 3勝クラス is a transition class (horses graduating to OP/G3),
suggesting class-transition velocity features may be more informative here than in lower classes.  
**Candidate features**:

- `class_transition_velocity`: races-per-class-win (how quickly a horse climbed from 010 → 016)
- `days_at_current_class`: tenure at 3勝 level (fresh-promoted vs long-stalled horses)
- `rival_class_delta`: average class of rivals in previous races (relative strength scheduling)
- `win_margin_trend`: is this horse winning comfortably or just squeaking by?

**Design**: partial-ρ probe on JRA 016 subset, then per-class GBDT if ρ clears.  
**Note**: n=727 is low (MDE ~4pp). Probe is cheap; full retrain only if ρ ≥ 0.08 and effect
is sizeable.  
**File**: `jra/class-016.md`

---

#### CANDIDATE 4: pgvector — per-class RACE-CONDITION similarity (JRA 703, new design) [VEC, MEDIUM priority]

**Class**: JRA 703 (未勝利) — then extend to other JRA classes if it works  
**Method**: VEC — pgvector kNN, but with a DIFFERENT embedding and purpose than the prior rejected attempt  
**Why this differs from DO-NOT-RETEST**:  
The prior `iter32-jra-vec-knn-{class}` design embedded HORSE-HISTORY vectors and used kNN
on horse similarity. That was rejected because the feature space was redundant with existing
GBDT features. This new design embeds RACE-CONDITION similarity instead:

- Vector: `(kyori, track_code, baba_code, shusso_tosu, field_speed_index_mean, field_nige_rate_mean)`
  → this encodes the competitive environment, not individual horse ability.
- Query: find the K historically-closest races by race-condition vector.
- Feature: within those K races, compute historical top1/place2/place3 rates for each horse's
  profile (odds rank, running style, weight class).
- The signal being tested: "given a specific race condition vector, which horse types have
  historically outperformed their odds rank in races of this type?"

**Design**: probe ρ for the new race-condition kNN feature vs finish_pos, controlling for odds.
Must clear ρ ≥ 0.08 before spending retrain budget. Use DuckDB + L2 distance approximation
(no pg_vector install needed for probe).  
**Risk**: GBDT may already capture all race-condition information through existing features
(track_code, baba_code, field-level statistics). The probe answers this cheaply.  
**File**: `jra/class-703.md` (then extend to other JRA classes)

---

#### CANDIDATE 5: NAR C-class — fukusho-focused ranking objective (place2 preservation) [ML, MEDIUM priority]

**Class**: NAR C  
**Method**: ML — a ranking objective that explicitly optimizes place2 without trading top1  
**Rationale**: Wave1 H4 (LightGBM LambdaRank) showed top1 +0.342pp (LB95 +0.00119, Holm-significant)
but place3 −0.211pp breached the floor. The finding: LambdaRank sharpens the winner pick at
the cost of 2nd/3rd ordering. We need an objective that jointly optimizes top1 AND place2/place3.  
**Design**:

- Objective: NDCG@3 with a modified relevance schema that scores position-2 hits higher than
  the standard (graded-relevance schemes explored in `history/relevance-scheme-audit.md`).
- Alternatively: a multi-objective loss that combines `top1_loss + λ×place2_loss` where λ
  is tuned on the inner validation window to avoid place damage.
- Gate: strengthened multi-metric ≥2 of 4 axes positive AND no axis < −0.05pp AND LB95 > 0.

**Why this is different from prior attempts**: H4 used LambdaRank with standard top1 orientation.
This tests a place-aware loss modification specifically for the place-trading pattern seen in C.  
**File**: `nar/class-C.md`

---

#### CANDIDATE 6: RL policy-gradient ranker — NAR C or JRA 703 probe [RL, LOW-MEDIUM priority]

**Class**: NAR C or JRA 703 (largest n, best power)  
**Method**: RL — policy-gradient ranking (REINFORCE or policy-gradient LTR)  
**Rationale**: GBDT with standard LambdaRank/NDCG objectives optimizes an approximation of
ranking metrics. A RL ranker can directly optimize the exact per-race reward:

- Reward signal: `top1_hit × w1 + place2_hit × w2 + place3_hit × w3 + top3_box × w4`
  where weights are tunable per class.
- The advantage over GBDT: the RL policy directly maximizes the exact-ordinal multi-position
  reward, not a differentiable surrogate. This potentially captures the top1-vs-place tradeoff
  in a way that GBDT objectives cannot.

**Design (concrete, scoped)**:

1. Encode each race as a fixed-size feature matrix (one row per horse, pad to max field size).
2. Use a shallow policy network (3-layer MLP) with softmax output → ranking via gumbel-top-k.
3. REINFORCE with a per-race reward: `r = sum_position(w_pos × exact_hit_pos)`.
4. Train on per-class subset (NAR C: ~26k races, enough for RL stability with early stopping).
5. **Gate**: compare to existing iter30/iter36 ensemble on the nested holdout under the same
   strengthened gate.

**Honest assessment**: GBDT consistently beats RL on tabular ranking in published benchmarks.
The main value of this experiment is testing whether the exact reward signal helps where the
surrogate loss is binding (the top1/place trade observed in C and H4). Expected cost: ~1-2 days
of training + eval.  
**Risk**: RL on tabular data typically underperforms GBDT. Scope as a small per-class probe
before investing in a full multi-class RL system.  
**File**: `nar/class-C.md` (or `jra/class-703.md`)

---

#### CANDIDATE 7: JRA 999 (障害) dedicated class features [FEAT + ML, MEDIUM priority]

**Class**: JRA 999 (Jumps/Steeplechase)  
**Method**: FEAT — jump-race-specific signals  
**Rationale**: 999 has a strong Mdl−Mkt gap (+13.63pp) but the current feature set was
designed for flat races. Jump races have fundamentally different dynamics:

- Jumping ability (no existing feature)
- Hurdle/chase experience count (career obstacle-race count)
- Fall history (previous falls — captured in jvd_se nyusen_code?)
- Course layout specifics (number of obstacles, track elevation)

**Design**: audit jvd_se columns for jump-specific fields → build probe features → partial-ρ
gate. n=1,064 holdout is MEDIUM power (MDE ~3pp).  
**File**: `jra/class-999.md`

---

#### CANDIDATE 8: NAR 2YO age-specific features (foal month + debut-race profile) [FEAT + ML, LOW-MEDIUM priority]

**Class**: NAR 2YO  
**Method**: FEAT — debut/early-career features  
**Rationale**: 2YO already at 60.85% top1 (ABOVE the 60% target). The value here is improving
place2 (37.27%, gap to 50% = −12.73pp). place2 37.27% is the closest to 40% of any class.
Age-specific signals for 2YO (foal month, debut, early training volume) may be available in
the nvd_um / nvd_se feed.  
**Design**: check nvd_um for `foal_date` coverage → compute `foal_month` feature → partial-ρ
probe in 2YO subset.  
**Note**: n=2,176 (holdout) is borderline powered (MDE ~2.6pp). Probe cheap; model only if
large ρ.  
**File**: `nar/class-2YO.md`

---

## §5. Ranked Candidate Summary

| Rank | Candidate                                       | Class     | Method  | Priority   | Power     |
| ---- | ----------------------------------------------- | --------- | ------- | ---------- | --------- |
| 1    | NAR B dedicated per-class model                 | NAR B     | ML      | HIGH       | HIGH      |
| 2    | NAR 3YO age-specific features + retrain         | NAR 3YO   | FEAT+ML | HIGH       | HIGH      |
| 3    | JRA 016 class-transition feature probe          | JRA 016   | FEAT+ML | MEDIUM     | LOW       |
| 4    | pgvector race-condition similarity (new design) | JRA 703   | VEC     | MEDIUM     | HIGH      |
| 5    | NAR C place-preserving ranking objective        | NAR C     | ML      | MEDIUM     | VERY HIGH |
| 6    | RL policy-gradient ranker probe                 | NAR C/703 | RL      | LOW-MEDIUM | HIGH      |
| 7    | JRA 999 jump-race dedicated features            | JRA 999   | FEAT+ML | MEDIUM     | MEDIUM    |
| 8    | NAR 2YO foal-month / debut features             | NAR 2YO   | FEAT    | LOW-MEDIUM | MEDIUM    |

---

## §6. pgvector Design Constraint (DO-NOT-RETEST vs new design)

Any new pgvector experiment must satisfy ALL of the following to avoid being a re-test of the
rejected design:

1. **Different embedding space**: do NOT embed horse-history vectors (already tried). Use
   race-condition vectors, jockey-trainer-venue interaction vectors, or breeding-distance
   affinity vectors.
2. **Different purpose**: not an ensemble member blended at inference time (tried + rejected).
   Instead: a feature computed at training time and passed to the GBDT as an additional column.
3. **Probe first**: before any training, run a partial-ρ probe on the candidate kNN-derived
   feature vs finish_pos controlling for log(odds). Gate: |ρ| ≥ 0.08.
4. **Per-class scope**: evaluate only on a single well-powered class first (JRA 703 or NAR C).
5. **Document why the design differs** from prior rejected attempts in the class file before
   running.

---

## §7. RL Design Constraint (honest framing)

Any RL experiment must:

1. **Scope to a single well-powered class** (NAR C or JRA 703) before generalizing.
2. **Compare to the current per-class ensemble baseline** (not a weaker global fallback).
3. **Use the same strengthened accept gate** (multi-metric + LB95 + Holm).
4. **Acknowledge the GBDT benchmark**: tabular RL is likely to underperform GBDT. The test
   is whether the exact per-position reward signal unlocks something the surrogate loss misses —
   specifically the top1/place tradeoff.
5. **Training cost**: cap at 2 days compute; use the memory safety rules (heavy learning:
   same time only one process + DuckDB ≤6GB + free RAM ≥30%).

---

## §8. Forward Procedure

For each candidate, the lifecycle is:

1. **Class file update** (`per-class/{category}/class-{code}.md`): add hypothesis.
2. **Probe** (partial-ρ or equivalent cheap filter): gate ρ ≥ 0.08.
3. **Train** (if probe passes): follow the per-category recipe with nested split.
4. **Judge** (strengthened gate): ≥2 of 4 axes positive, ≥1 place positive, no axis < −0.05pp,
   top1 paired-bootstrap LB95 > 0, Holm if multi-class.
5. **Decision**: ADOPT (register in PER_CLASS_MODEL_VERSIONS + deploy) or REJECT.
6. **Class file update**: log the outcome in the Evaluation Log.
7. **ROADMAP.md update**: mark candidate status and add next priorities.

---

## §9. Current Status

| Candidate          | Status    | Started    | Verdict                                                     |
| ------------------ | --------- | ---------- | ----------------------------------------------------------- |
| NAR B ML           | COMPLETED | 2026-06-17 | ABORT — iter30 CB residual, no LB95≥0 on any primary metric |
| NAR 3YO FEAT       | pending   | —          | —                                                           |
| JRA 016 FEAT       | pending   | —          | —                                                           |
| pgvector race-cond | pending   | —          | —                                                           |
| NAR C place-obj    | pending   | —          | —                                                           |
| RL probe           | pending   | —          | —                                                           |
| JRA 999 FEAT       | pending   | —          | —                                                           |
| NAR 2YO FEAT       | pending   | —          | —                                                           |
