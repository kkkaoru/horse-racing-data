# JRA Per-Class Baseline and Context

Category: JRA  
Production model: `iter19-jra-cb-kohan3f-going-v8` (244 features; base-only since 2026-06-13)  
Per-class routing: Phase B architecture deployed, registry currently empty — all classes
route to category-global fallback.

---

## Baseline Table (holdout 2023-2026, iter14/iter19 production)

Source: `rootcause-i1-ceiling-market.md` (iter14 baseline) + `iter19-deploy.md`.

| Class     | Code | n races | top1% | place2% | place3% | top3_box% | Mdl−Mkt top1 | Status               |
| --------- | ---- | ------: | ----: | ------: | ------: | --------: | -----------: | -------------------- |
| 未勝利    | 703  |   4,229 | 49.40 |   25.80 |   19.06 |         — |       +14.31 | MODEL_EXCEEDS_ORACLE |
| 1勝クラス | 005  |   3,147 | 40.99 |   20.46 |   15.44 |         — |        +9.09 | MODEL_EXCEEDS_ORACLE |
| 2勝クラス | 010  |   1,583 | 43.02 |   20.78 |   14.66 |         — |        +7.83 | MODEL_EXCEEDS_ORACLE |
| 3勝クラス | 016  |     727 | 37.55 |   20.91 |   12.79 |         — |        +5.64 | MODEL_EXCEEDS_ORACLE |
| 新馬      | 701  |     953 | 45.02 |   23.92 |   20.46 |         — |       +11.02 | MODEL_EXCEEDS_ORACLE |
| 障害      | 999  |   1,064 | 42.01 |   23.03 |   16.35 |         — |       +13.63 | MODEL_EXCEEDS_ORACLE |
| Global    | —    |  11,703 | 44.76 |   23.31 |   16.96 |     15.81 |       +11.41 | MODEL_EXCEEDS_ORACLE |

Note: Mdl−Mkt from `rootcause-i1-ceiling-market.md` §JRA Per Class. top3_box not
decomposed per-class in the source doc (global only = 15.81%).

---

## Gap to Targets

| Class | top1 gap to 60% | place2 gap to 50% | place3 gap to 40% |
| ----- | --------------- | ----------------- | ----------------- |
| 703   | −10.60pp        | −24.20pp          | −20.94pp          |
| 005   | −19.01pp        | −29.54pp          | −24.56pp          |
| 010   | −16.98pp        | −29.22pp          | −25.34pp          |
| 016   | −22.45pp        | −29.09pp          | −27.21pp          |
| 701   | −14.98pp        | −26.08pp          | −19.54pp          |
| 999   | −17.99pp        | −26.97pp          | −23.65pp          |

Reminder: place2/place3 targets are aspirational (oracle ceiling ~18% / ~14% — far below 50%/40%).

---

## Prior Experiments (refs to history/)

- iter19 (kohan3f): `iter19.md`, `iter19-deploy.md` — +0.48pp top1 global via 3 going-conditional
  上がり3F features; STAGED then deployed; JRA base-only.
- iter20 (per-class JRA candidates): `iter20.md` — all 6 per-class candidates lose to iter14
  on their own subsets. Registry stays empty.
- pgvector JRA member: `oi-2026-06-10-rounds-r2-r4-pgvector.md` — all 5 classes REJECTED;
  005 near-miss (4/4 axes positive, top1 LB95 −0.000953). DO-NOT-RETEST.
- HPO levers: `iter13.md`, `iter14.md` — CV gains from HPO failed to translate to WF gate.
- Window ablation: `goal-iter20-jra-2013-deploy.md` — 2013+ training start deployed in iter19.
- Relationship features: `jra-relationship-features-perclass.md` — all probes ABORT/REJECT.
- kohan3f verify: `jra-kohan3f-verify.md` — confirms +0.48pp top1 / +0.36pp fukusho_2p.

---

## Class-Specific Notes

**016 (3勝クラス)** — weakest top1 (37.55%), smallest Mdl−Mkt gap (+5.64pp), smallest n
(727). High-variance class with a competitive field structure. Most headroom on a percentage
basis but lowest statistical power.

**999 (障害 Jumps)** — second-strongest Mdl−Mkt gap (+13.63pp). Jump races have different
dynamics (obstacles, falls) not well-represented in flat-race features. High headroom,
unique signal source.

**701 (新馬)** — debut races; odds-correction analysis shows large maiden-popularity-echo
effect; see `maiden-popularity-echo-diagnosis.md`. The viewer odds-correction overlay was
set to default OFF to avoid double-counting (commit d8024468). Signal challenge: zero prior
race history for horses.

**703 (未勝利)** — largest class (n=4,229), highest top1 (49.40%), strongest Mdl−Mkt gap
(+14.31pp). Best statistical power for per-class experiments.

---

## Active Per-Class Hypotheses (JRA)

See individual class files and ROADMAP.md §4 for ranked candidates.
