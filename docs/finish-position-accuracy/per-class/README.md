# Per-Class Accuracy Campaign — Convention Reference

Campaign started: 2026-06-17

## Purpose

This directory tracks hypothesis / verify / eval docs **per race class** across JRA, NAR, and
Ban-ei. The new goal is:

- top1 ≥ 60%
- place2 ≥ 50%
- place3 ≥ 40%

**Important caveat on place2/place3 targets (read once, never re-derive)**:  
`goal-baseline-and-ceiling.md` proves exact-ordinal place2 ≥ 40% is information-theoretically
infeasible (oracle ceiling JRA 18%, NAR 23% — both far below 40%). The targets are aspirational.
The operational rule is: **adopt any confirmed per-class improvement, even if the absolute
targets remain unmet**. Accept gate: multi-metric (top1 / place2 / place3 / top3_box), ≥2
positive AND ≥1 of {place2, place3} positive, no axis < −0.05pp, top1 paired-bootstrap
LB95 > 0, Holm correction across classes.

---

## Metric Definitions (canonical, from `aggregate_bucket_eval_duckdb.py:341-350`)

```
top1    := predicted_rank=1  AND finish_position=1  (per race: max over entries)
place2  := predicted_rank=2  AND finish_position=2  (per race: max over entries)
place3  := predicted_rank=3  AND finish_position=3  (per race: max over entries)
top3_box := (pred_rank≤3 ∩ finish≤3) has all 3 members correctly placed
```

All four are exact-ordinal — not "top-k set" metrics.

---

## Class Enumeration

### JRA

Routing field: `kyoso_joken_code` in jvd_se / feature parquet.

| Code  | Japanese       | Label        | Notes                                             |
| ----- | -------------- | ------------ | ------------------------------------------------- |
| 701   | 新馬           | Maiden debut | First-time starters                               |
| 703   | 未勝利         | Maiden       | No win yet, not debut                             |
| 005   | 1勝クラス      | 1win         | Formerly 500万下                                  |
| 010   | 2勝クラス      | 2win         | Formerly 1000万下                                 |
| 016   | 3勝クラス      | 3win         | Formerly 1600万下                                 |
| 703   | OP/L/G3-G1     | OP+          | code ≈ 703 OP; Grade coded via grade_code         |
| 999   | 障害           | Jumps        | Steeplechase/hurdle races                         |
| other | (unregistered) | other        | Folds to category global via normalize_class_code |

Named per-class codes in production: `005, 010, 016, 701, 703`. Code 999 (jumps) is not in
the named set and folds to "other" at inference. As of iter19 (2026-06-13) JRA routes
base-only (all per-class registry entries removed); the Phase B architecture is deployed but
empty.

### NAR

Routing field: `nar_subclass` — derived from `kyoso_joken_meisho` free-text via regex
in `finish_position_features_duckdb.py:nar_subclass_case_sql()`. Priority order: OP > 新馬 >
未勝利/未出走 > 2歳 > 3歳 > Ａ > Ｂ > Ｃ > else→other.

| Label   | Pattern        | Notes                                                                |
| ------- | -------------- | -------------------------------------------------------------------- |
| OP      | ＯＰ           | Open/graded races                                                    |
| NEW     | 新馬           | Debut races                                                          |
| MUKATSU | 未勝利\|未出走 | Maiden (no wins yet)                                                 |
| 2YO     | ２歳\|2歳      | 2-year-old races (added in aa2afd8; routes to other in per_class.py) |
| 3YO     | ３歳\|3歳      | 3-year-old races (added in aa2afd8; routes to other in per_class.py) |
| A       | Ａ             | A-class                                                              |
| B       | Ｂ             | B-class (routes to iter12 global fallback — no ensemble registered)  |
| C       | Ｃ             | C-class (most populous; has iter36 LGB ensemble)                     |
| other   | (default)      | Catch-all bucket                                                     |

Currently registered (PER_CLASS_MODEL_VERSIONS in per_class.py):
`NEW, MUKATSU, C (iter36 LGB), A, OP, other` — all iter30 CB ensembles except C.
Class `B` routes to global iter12 fallback (no ensemble registered).

### Ban-ei

No per-class routing today. All Ban-ei races route to category-global
`banei-cb-v7-lineage-wf-21y`. Class structure from `nvd_ra.grade_code`:

| grade_code  | Label     | n races (holdout 2023-26) | Notes                        |
| ----------- | --------- | ------------------------- | ---------------------------- |
| ` ` (space) | E_general | ~5,490                    | Ungraded (vast majority)     |
| E           | E_named   | ~438                      | Named E-grade races          |
| Q           | QR_upper  | ~75                       | Upper-tier (combined with R) |
| R           | QR_upper  | ~48                       | Upper-tier (combined with Q) |
| P           | P         | ~25                       | Highest grade (Banei Kinen)  |
| T           | T         | ~22                       | Special class                |

---

## Directory Convention

One subdirectory per category. Within each category, one file per class:

```
per-class/
  README.md             ← this file (convention + class enumeration + headroom index)
  ROADMAP.md            ← master plan: ranked candidates, multi-method roadmap
  jra/
    README.md           ← JRA-specific context + per-class baseline table
    class-701.md        ← 新馬 (Maiden debut)
    class-703.md        ← 未勝利 (Maiden)
    class-005.md        ← 1勝クラス
    class-010.md        ← 2勝クラス
    class-016.md        ← 3勝クラス
    class-999.md        ← 障害 (Jumps)
  nar/
    README.md           ← NAR-specific context + per-class baseline table
    class-C.md          ← C-class (most populous; has active ensemble)
    class-B.md          ← B-class (global fallback)
    class-A.md          ← A-class
    class-OP.md         ← Open/graded
    class-NEW.md        ← Debut (新馬)
    class-MUKATSU.md    ← Maiden (未勝利/未出走)
    class-2YO.md        ← 2-year-old
    class-3YO.md        ← 3-year-old
  ban-ei/
    README.md           ← Ban-ei context + per-class baseline table
    class-E_general.md  ← Ungraded (bulk of Ban-ei races)
    class-E_named.md    ← Named E-grade
    class-QR.md         ← Q+R tier combined
    class-P.md          ← P-grade (Banei Kinen tier)
    class-T.md          ← T-class
```

### Per-class file format

Each class file should contain:

1. **Frontmatter** (YAML): class label, category, n_races (holdout 2023-26), baseline metrics.
2. **Status**: active ensemble version, or "routes to global fallback."
3. **Headroom table**: model / market / oracle per top1/place2/place3/top3_box.
4. **Prior experiments** (brief ref to existing docs in history/).
5. **Active hypotheses**: one subsection per pending hypothesis with method tag and priority.
6. **Evaluation log**: outcomes of attempted improvements, with decision (ADOPT / REJECT).

---

## DO-NOT-RETEST Registry (pgvector / similarity)

The following pgvector/kNN approaches were rigorously rejected and must NOT be re-run:

1. **JRA per-class kNN member (iter32-jra-vec-knn-{class}-v8)**: all 5 JRA classes
   rejected (005 near-miss, top1 LB95 −0.000953); see `oi-2026-06-10-rounds-r2-r4-pgvector.md`.
2. **NAR kNN similarity member (iter32-nar-vec-knn-{class}-v8)**: all 7 NAR classes
   rejected; optimizer assigns ≈0 weight; see `oi-2026-06-10-r5-nar-similarity-member.md`.

A NEW pgvector hypothesis MUST differ structurally: different embedding space, different
similarity notion (e.g., per-class race-condition similarity vs horse-history similarity),
or different integration path (a feature, not an ensemble member). See ROADMAP.md §pgvector.

---

## Quick-reference headroom summary

(Full tables in ROADMAP.md §2. Short version for navigation.)

**JRA** (iter19 iter14 baseline, holdout 2023-26, n=11,703 races):

- All classes: MODEL_EXCEEDS_ORACLE. Top1 range: 38-50%. Model beats market by +6-14pp.
- Weakest class: 016 (3勝クラス) top1 37.55%, Mdl−Mkt only +5.64pp.
- Strongest class: 703 (未勝利) top1 49.40%, Mdl−Mkt +14.31pp.

**NAR** (iter12+ensembles, holdout 2023-26, n=45,572 races):

- All classes: MODEL_EXCEEDS_ORACLE. Top1 range: ~47-63%.
- 2YO top1 60.85%, 3YO top1 59.63% (age classes, high accuracy, n-limited).
- C-class largest holdout n (~26,060 for full; Ōi slice 1,928).

**Ban-ei** (banei-cb-v7-lineage-wf-21y, holdout n=5,928 races):

- SATURATED / ANTI-INFORMATIVE: top1 34.62% ≈ market 34.46%; rentai_hit −2.68pp vs market.
- Most headroom: P-class (n=25 — very small, high variance).
