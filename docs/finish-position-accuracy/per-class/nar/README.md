# NAR Per-Class Baseline and Context

Category: NAR  
Production model: `iter12-nar-xgb-hpo-v8` (global fallback, 174 features)  
Per-class ensembles: NEW / MUKATSU / C / A / OP / other — iter30 CB ensembles (except C which
uses iter36 LGB LambdaRank). Class B routes to global iter12 fallback (no ensemble registered).
2YO / 3YO: labeled separately since commit aa2afd8, but normalize to "other" in per_class.py.

---

## Baseline Table (holdout 2023-2026, production per-class config)

Sources: `oi-2026-06-10-r5-nar-similarity-member.md` (full-NAR holdout n per class, 2023-26);
`phase3-nar-2yo3yo-perclass.md` (2YO/3YO); `oi-2026-06-10-per-class-round.md` (Ōi slice);
`rootcause-i1-ceiling-market.md` (global NAR).

| Class   | n races (holdout 2023-26) |  top1% | place2% | place3% | top3_box% | Active ensemble                |
| ------- | ------------------------- | -----: | ------: | ------: | --------: | ------------------------------ |
| C       | ~26,060                   | ~47-58 |  ~25-36 |  ~18-27 |         — | iter36-nar-lgb-ensemble-C      |
| other   | ~7,217                    |    ~52 |     ~30 |     ~18 |       ~23 | iter30-nar-cb-ensemble-other   |
| B       | ~7,124                    |    ~51 |     ~29 |     ~21 |       ~22 | iter12 global fallback         |
| A       | ~2,812                    |      — |       — |       — |         — | iter30-nar-cb-ensemble-A       |
| OP      | ~1,231                    |      — |       — |       — |         — | iter30-nar-cb-ensemble-OP      |
| 2YO     | 2,176                     |  60.85 |   37.27 |   31.30 |     40.35 | iter12 via "other" fallback    |
| 3YO     | 8,578                     |  59.63 |   36.21 |   27.93 |     35.45 | iter12 via "other" fallback    |
| MUKATSU | ~556                      |      — |       — |       — |         — | iter30-nar-cb-ensemble-MUKATSU |
| NEW     | ~573                      |      — |       — |       — |         — | iter30-nar-cb-ensemble-NEW     |
| Global  | 45,572                    |  58.68 |   35.26 |   27.32 |     34.85 | (weighted over all classes)    |

Notes:

- C, other, B Ōi-slice values from `oi-2026-06-10-per-class-round.md` (Ōi = keibajo=44 slice):
  C: top1 47.35 / place2 25.62 / place3 18.36; other: 52.48 / 29.90 / 18.38; B: 51.41 / 29.00 / 21.09
- 2YO / 3YO from `phase3-nar-2yo3yo-perclass.md` (exact holdout counts 2,176 / 8,578)
- A, OP, NEW, MUKATSU n from R5 probe 2022 calibration year (approximate for full holdout)

---

## Gap to Targets

| Class  | top1 gap to 60% | place2 gap to 50% | place3 gap to 40% |
| ------ | --------------- | ----------------- | ----------------- |
| C      | ~+0 to −13pp    | ~−14 to −25pp     | ~−13 to −22pp     |
| other  | ~−8pp           | ~−20pp            | ~−22pp            |
| B      | ~−9pp           | ~−21pp            | ~−19pp            |
| 2YO    | +0.85pp (ABOVE) | −12.73pp          | −8.70pp           |
| 3YO    | −0.37pp         | −13.79pp          | −12.07pp          |
| Global | −1.32pp         | −14.74pp          | −12.68pp          |

2YO and 3YO are the only classes already meeting or near-meeting the top1 60% target.

---

## Prior Experiments (refs to history/)

- iter30 (NAR CB residual ensembles): `nar-schemeD-deploy-judge.md` — NEW/MUKATSU/C/A/OP/other
  ensembles activated.
- iter36 (C LGB LambdaRank residual): `oi-2026-06-10-iter36-lgb-lambdarank-residual-C-adopt.md`
  — C flipped from iter30 CB to iter36 LGB ensemble.
- iter37 (2YO/3YO age-class): `phase3-nar-2yo3yo-perclass.md` — REJECTED; residual adds no
  detectable signal on age-class races.
- Wave1 H1-H4: `oi-2026-06-10-wave1-h1-h5.md` — field-relative/recency/HPO all REJECTED; per-class
  models at genuine signal ceiling.
- NAR kNN similarity (R5): `oi-2026-06-10-r5-nar-similarity-member.md` — all 7 classes REJECTED;
  orthogonal feature variance is target-noise. DO-NOT-RETEST same design.
- Ōi-specialist levers: `oi-2026-06-10-per-class-round.md` — venue-targeted specialist REJECTED.
- G1+F1 retrain: `g1-f1-combined-nar-retrain-judge.md` — fixing training bugs worsens serve
  metrics (−0.63pp top1). iter12 maintained.
- Relationship features: `nar-relationship-features-perclass.md` — all ABORT/REJECT.

---

## Class-Specific Notes

**C (largest n ~26k)**: Most statistical power. The Ōi-slice C near-misses (H4: top1 +0.342pp,
LB95 +0.00119, Holm-significant, but place3 −0.211pp breaches floor) show there is a directional
signal for top1 in C that trades place ordering. A method that improves both simultaneously would
be adoptable.

**B (no ensemble, global fallback)**: n ~7,124 — well-powered. No per-class model has been
successfully trained for B. The class may have distinct structure justifying a dedicated approach.

**2YO (top1 already 60.85%)**: Meets the top1 target. place2 37.27% is the closest to 40% of
any class. Under-powered for detecting sub-2.6pp improvements. Age-specific features not yet
in the feature set.

**3YO (top1 59.63%)**: Very close to top1 target. n=8,578 well-powered. Same age-feature gap
as 2YO.

---

## Active Per-Class Hypotheses (NAR)

See individual class files and ROADMAP.md §4 for ranked candidates.
