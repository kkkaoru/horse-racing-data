# Ban-ei Per-Class Baseline and Context

Category: Ban-ei  
Production model: `banei-cb-v7-lineage-wf-21y` (category-global, no per-class routing)  
Per-class routing: none (Ban-ei is excluded from PER_CLASS_ENABLED_CATEGORIES in per_class.py)

---

## Key Fact: Ban-ei Is Saturated at Market Level

From `rootcause-i1-ceiling-market.md` §Ban-ei:

| Metric     | Model% | Market% | Oracle% | Mdl−Mkt | Status               |
| ---------- | -----: | ------: | ------: | ------: | -------------------- |
| top1       |  34.62 |   34.46 |   34.62 |   +0.15 | SATURATED            |
| place2     |  20.65 |   20.72 |   20.65 |   −0.07 | SATURATED            |
| place3     |  15.54 |   15.55 |   15.69 |   −0.02 | SATURATED            |
| rentai_hit |  15.13 |   17.81 |   18.18 |   −2.68 | ANTI-INFORMATIVE     |
| fukusho_2p |  62.65 |   61.66 |   62.25 |   +1.00 | MODEL_EXCEEDS_ORACLE |

The Ban-ei model provides essentially zero lift over market ranking on exact-ordinal metrics.
rentai_hit is actively anti-informative (−2.68pp vs market).

---

## Ban-ei Class Structure (from `banei-relationship-features-perclass.md`)

Source: `nvd_ra.grade_code` joined to `nvd_se`.

| grade_code  | Label     | n rows (holdout 2023-26) | n races | Notes                       |
| ----------- | --------- | -----------------------: | ------: | --------------------------- |
| ` ` (space) | E_general |                   49,812 |  ~5,490 | Ungraded (vast majority)    |
| `E`         | E_named   |                    4,075 |    ~438 | Named E-grade races         |
| `Q`         | QR_upper  |     ~598 (combined w/ R) |     ~75 | Upper-tier graded           |
| `R`         | QR_upper  |               (combined) |     ~48 | Upper-tier graded           |
| `P`         | P         |                      227 |     ~25 | Highest grade (Banei Kinen) |
| `T`         | T         |                      206 |     ~22 | Special class               |

---

## Headroom by Class (from `banei-relationship-features-perclass.md` probe results)

The relationship-features probe found that within-race futan (weight load) features pass
ρ ≥ 0.08 partial Spearman gate ONLY for class P:

| Feature            | Class     | n rows | partial ρ | p-value | gate (0.08) | verdict |
| ------------------ | --------- | -----: | --------: | ------: | ----------- | ------- |
| futan_rank_in_race | E_general | 49,812 |   +0.0347 | <0.0001 | FAIL        | —       |
| futan_rank_in_race | E_named   |  4,075 |   +0.0190 |   0.225 | FAIL        | —       |
| futan_rank_in_race | P         |    227 |   +0.1869 |   0.005 | PASS        | signal  |
| futan_deviation    | P         |    227 |   +0.1761 |   0.008 | PASS        | signal  |

P-class (n=25 races in holdout) is the only class where futan-load features carry meaningful
signal beyond odds. However, n=25 races is extremely low — any model verdict will have very
high variance.

---

## Gap to Targets

| Class     | top1 (global 34.62%) | gap to 60% | Notes                              |
| --------- | -------------------: | ---------- | ---------------------------------- |
| E_general |                 ~34% | ~−26pp     | SATURATED — no meaningful headroom |
| P         |       unknown (n=25) | ~−26pp est | High variance; futan signal PASSES |
| E_named   |      unknown (n=438) | ~−26pp est | Unknown per-class model            |

---

## Prior Experiments (refs to history/)

- Banei baseline/HPO: `banei-baseline-hpo.md` — initial model performance.
- Sectional + race-internal features: `banei-sectional-raceinternal.md` — no gain.
- Exotic odds: `banei-exotic-extended-rejudge.md` — fukusho +1.69pp but top1 trade → REJECT.
- Relationship features (futan): `banei-relationship-features-perclass.md` — PARTIAL PROCEED
  (P-class only), but n=25 too small for robust evaluation.
- odds decoupling: `odds-decouple-banei.md` — odds removal worsens by −7.95pp top1 → REJECT.

---

## Active Per-Class Hypotheses (Ban-ei)

See individual class files and ROADMAP.md §4 for ranked candidates.

**Primary challenge**: Ban-ei is already saturated at the market level. The most viable path
is P-class (highest futan signal, ρ≈+0.18) but n=25 races in holdout is insufficient for
a robust model verdict (MDE would be ~10+ pp at 80% power).

The most tractable opportunity: collect more P-class data (over 3-5 years that's ~60-90 races
— borderline powered), then train P-specific features (futan_rank_in_race, futan_deviation as
confirmed signals).
