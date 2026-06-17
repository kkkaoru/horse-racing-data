# Signal 4 NAR Sire×Keibajo: Serve-Coverage Gate (2026-06-17)

## Context

Signal 4 (NAR `sire_keibajo_win_rate` + `damsire_keibajo_win_rate`) passed the WF cheap-filter
(place2 +0.184pp, LB95 +0.191pp; see `goal-signal4-nar-sire-keibajo-verify.md`, commit 245d9ce).
Before authorising an expensive co-train (base retrain + feat-v15 rebuild + iter30/36
residual retrain), this gate checks whether serve coverage is adequate. The g1f1 trap
(`project_nar_g1f1_combined_adopt_2026_06_12`) teaches: if train coverage ≫ serve coverage,
the model adapts to informative-absence NULL-routing that doesn't match serve, and regression
at serve is predictable even when WF holdout looks positive.

The feature is sourced from `nvd_um` (N-Data horse master, `nar_um` in the pipeline) via
`ketto_joho_01b` (sire code) and `ketto_joho_05b` (damsire code), joined to race entries
(`nvd_se`) on `ketto_toroku_bango`.

---

## Coverage Query

Database: Neon PostgreSQL primary replica (`nvd_se` × `nvd_um`).
DuckDB `ATTACH` (read-only), `memory_limit='6GB'`, `threads=4`.
Ban-ei excluded (`keibajo_code != '83'`). Unique-horse and row-level coverage computed.

---

## Unique-Horse Coverage by Year

Number of distinct horses appearing in NAR race entries per year, and what fraction have
sire (`ketto_joho_01b`) and damsire (`ketto_joho_05b`) linkage in `nvd_um`.

| Year | Unique horses | In nvd_um | % in nvd_um | % sire | % damsire |
| ---: | ------------: | --------: | ----------: | -----: | --------: |
| 2005 |        16,899 |    16,412 |        97.1 |   97.1 |      97.1 |
| 2006 |        15,375 |    14,922 |        97.1 |   97.1 |      97.1 |
| 2007 |        13,714 |    13,332 |        97.2 |   97.2 |      97.2 |
| 2008 |        13,462 |    13,008 |        96.6 |   96.6 |      96.6 |
| 2009 |        13,310 |    12,893 |        96.9 |   96.9 |      96.9 |
| 2010 |        13,238 |    12,865 |        97.2 |   97.2 |      97.2 |
| 2011 |        13,017 |    12,653 |        97.2 |   97.2 |      97.2 |
| 2012 |        12,718 |    12,387 |        97.4 |   97.4 |      97.4 |
| 2013 |        12,495 |    12,209 |        97.7 |   97.7 |      97.7 |
| 2014 |        12,057 |    11,760 |        97.5 |   97.5 |      97.5 |
| 2015 |        11,789 |    11,541 |        97.9 |   97.9 |      97.9 |
| 2016 |        11,773 |    11,547 |        98.1 |   98.1 |      98.1 |
| 2017 |        12,037 |    11,811 |        98.1 |   98.1 |      98.1 |
| 2018 |        12,729 |    12,515 |        98.3 |   98.3 |      98.3 |
| 2019 |        13,337 |    13,095 |        98.2 |   98.2 |      98.2 |
| 2020 |        13,846 |    13,647 |        98.6 |   98.6 |      98.6 |
| 2021 |        14,361 |    14,119 |        98.3 |   98.3 |      98.3 |
| 2022 |        14,542 |    14,250 |        98.0 |   98.0 |      98.0 |
| 2023 |        15,110 |    12,355 |        81.8 |   81.8 |      81.8 |
| 2024 |        15,265 |     7,700 |        50.4 |   50.4 |      50.4 |
| 2025 |        15,576 |     4,560 |        29.3 |   29.3 |      29.3 |
| 2026 |        11,849 |     2,473 |        20.9 |   20.9 |      20.9 |

---

## Row-Level Coverage by Year

Race entry rows (not unique horses): what fraction of NAR race entries have sire/damsire
resolvable at feature-build time.

| Year | Total rows | Sire rows | % sire | % damsire |
| ---: | ---------: | --------: | -----: | --------: |
| 2005 |    162,215 |   161,394 |   99.5 |      99.5 |
| 2006 |    153,617 |   152,857 |   99.5 |      99.5 |
| 2007 |    137,300 |   136,652 |   99.5 |      99.5 |
| 2008 |    138,352 |   137,581 |   99.4 |      99.4 |
| 2009 |    141,990 |   141,282 |   99.5 |      99.5 |
| 2010 |    140,445 |   139,857 |   99.6 |      99.6 |
| 2011 |    135,655 |   135,058 |   99.6 |      99.6 |
| 2012 |    135,124 |   134,606 |   99.6 |      99.6 |
| 2013 |    130,748 |   130,279 |   99.6 |      99.6 |
| 2014 |    128,456 |   127,969 |   99.6 |      99.6 |
| 2015 |    126,506 |   126,107 |   99.7 |      99.7 |
| 2016 |    128,466 |   128,116 |   99.7 |      99.7 |
| 2017 |    130,575 |   130,217 |   99.7 |      99.7 |
| 2018 |    132,688 |   132,358 |   99.8 |      99.8 |
| 2019 |    135,064 |   134,736 |   99.8 |      99.8 |
| 2020 |    136,747 |   136,465 |   99.8 |      99.8 |
| 2021 |    133,706 |   133,351 |   99.7 |      99.7 |
| 2022 |    136,593 |   136,144 |   99.7 |      99.7 |
| 2023 |    139,363 |   129,562 |   93.0 |      93.0 |
| 2024 |    141,301 |    87,513 |   61.9 |      61.9 |
| 2025 |    140,487 |    51,289 |   36.5 |      36.5 |
| 2026 |     63,268 |    14,911 |   23.6 |      23.6 |

---

## Recent Race Days (2026-06, last 30 days)

Coverage for the most recent 30 NAR race dates (as of 2026-06-17):

|     Date | Horses | HasUM |  %UM |
| -------: | -----: | ----: | ---: |
| 20260619 |    375 |    80 | 21.3 |
| 20260618 |    501 |    98 | 19.6 |
| 20260617 |    514 |   112 | 21.8 |
| 20260616 |    501 |   112 | 22.4 |
| 20260615 |    258 |    74 | 28.7 |
| 20260614 |    322 |    79 | 24.5 |
| 20260613 |    227 |    50 | 22.0 |
| 20260612 |    382 |    91 | 23.8 |
| 20260611 |    518 |   109 | 21.0 |
| 20260610 |    518 |   106 | 20.5 |
| 20260609 |    501 |   127 | 25.3 |
| 20260608 |    343 |    57 | 16.6 |
| 20260607 |    361 |    66 | 18.3 |
| 20260606 |    223 |    40 | 17.9 |
| 20260605 |    381 |    87 | 22.8 |
| 20260604 |    525 |    92 | 17.5 |
| 20260603 |    514 |   115 | 22.4 |
| 20260602 |    510 |   135 | 26.5 |
| 20260601 |    350 |    72 | 20.6 |
| 20260531 |    433 |   117 | 27.0 |
| 20260530 |    217 |    52 | 24.0 |
| 20260529 |    382 |    96 | 25.1 |
| 20260528 |    487 |   100 | 20.5 |
| 20260527 |    503 |   138 | 27.4 |
| 20260526 |    484 |   130 | 26.9 |
| 20260525 |    489 |   101 | 20.7 |
| 20260524 |    351 |    69 | 19.7 |
| 20260523 |    218 |    40 | 18.3 |
| 20260522 |    387 |    80 | 20.7 |
| 20260521 |    523 |   113 | 21.6 |

**Current serve coverage: ~17–29% per day (median ~22%).**

---

## Train-vs-Serve Gap Analysis

| Window               | Row-level sire coverage | Notes                             |
| :------------------- | ----------------------: | :-------------------------------- |
| Train 2016–2022      |                   ~99.7 | Production WF train window        |
| Holdout 2023–2025    |                   ~62.0 | Used in cheap-filter WF test      |
| Serve 2025 (full yr) |                    36.5 | Real serve year 2025 row coverage |
| Serve 2026 YTD       |                    23.6 | Current serve year row coverage   |
| Most recent 30 days  |                  ~17–29 | Actual daily serve coverage       |

**Train/serve gap: 99.7% (train) vs ~22% (current serve) = -77.7pp.**

This is far larger than the g1f1 case (96% train vs 62% holdout, −34pp gap),
which alone caused −0.63pp top1 regression at serve. The Signal 4 gap is
**2.3× larger** than the already-regressive g1f1 gap.

Root cause: `nvd_um` is the JV-Data horse master (JRA-linkage registry). NAR-native
horses registered after ~2022 are either not in JV-Data at all, or have a registration
lag of months to years. Recent NAR horses (born 2022+) are predominantly absent from
`nvd_um`. This is the same structural gap noted in the cheap-filter doc: "Only 50% of
2024 NAR horses appear in nvd_um (JV-Data registered horses)."

The gap is **not a temporary data lag** — it reflects a permanent structural limitation:
NAR-only horses are registered in N-Data (`nvd_um`) as a JV-Data mirror but JV-Data
coverage of pure NAR horses is ~20-50% for recent cohorts and declining.

---

## Verdict: DEFER-SERVE-BLOCKED

**Signal 4 (NAR sire×keibajo) is SERVE-BLOCKED. Do NOT proceed to co-train.**

Rationale:

1. **Current serve coverage is ~22%** (median over last 30 race days). This means 78% of
   race entries at serve time receive NULL for `sire_keibajo_win_rate` and
   `damsire_keibajo_win_rate`.

2. **The train/serve gap is −77.7pp** (99.7% train vs 22% serve). This is 2.3× larger
   than the g1f1 trap that caused −0.63pp serve regression when WF showed a positive holdout.
   Expected magnitude of serve regression for Signal 4: likely −1 to −2pp top1 or worse.

3. **The WF holdout measured 62% coverage** (2023–2025), which is itself optimistic vs
   real serve (22–36%). The positive WF result (+0.184pp place2) was already described as
   conservative; the real serve distribution has ~3× less signal available than the holdout.

4. **The NULL-routing trap applies with full force.** LightGBM will adapt NULL-routing
   splits to ~1% NULL (train) → deploy those splits against 78% NULL → catastrophic
   split mismatch. The existing sire features (`sire_track_win_rate`, `sire_distance_win_rate`)
   have ~99.7% train coverage and thus are not affected by this pathology.

5. **No mitigation available today.** Unlike the g1f1 case where one could in principle
   train at capped coverage to match serve, here coverage is so low (~22%) that the feature
   has too little signal to measure incremental gains over noise even if capped.

**Blocker:** `nvd_um` NAR coverage must be resolved (see #254 nvd_um sync blocker).
Until `nvd_um` achieves ≥80% coverage for 2024–2026 NAR horses, Signal 4 remains
serve-blocked.

---

## If nvd_um Coverage Is Resolved

If `nvd_um` is backfilled or replaced with a higher-coverage N-Data source achieving ≥80%
for recent NAR horses, the cheap-filter WF result (+0.184pp place2, LB95 +0.191pp) still
stands. The recommended path at that point:

1. Re-verify serve coverage (run this gate again — target ≥80% for most recent 30 days).
2. Proceed to feature store rebuild + base retrain + iter30/36 co-retrain per weld-awareness
   checklist in `goal-signal4-nar-sire-keibajo-verify.md`.
3. Apply MUKATSU NULL-routing (place2 −1.495pp at MUKATSU; zero out new feature for
   MUKATSU subclass rows at feature build time).
