# Ban-ei Multi-Column Relationship Features — Per-Class Probe

**Date:** 2026-06-12
**Status:** PARTIAL PROCEED (P-class only)
**Dataset:** holdout 2023-2026, keibajo_code='83' (Obihiro Ban-ei)
**Probe type:** Partial Spearman ρ vs finish_pos, controlling for log(odds). Gate: |ρ| ≥ 0.08.

---

## Ban-ei Class Structure

Source: `nvd_ra.grade_code` joined to `nvd_se`.

| grade_code  | Label     | Holdout rows (2023-26) | Race count | Notes                                  |
| ----------- | --------- | ---------------------- | ---------- | -------------------------------------- |
| ` ` (space) | E_general | 49 812                 | ~5 490     | Ungraded general races (vast majority) |
| `E`         | E_named   | 4 075                  | 438        | Named E-grade races                    |
| `Q`         | QR_upper  | 598 (combined with R)  | ~75        | Upper-tier graded                      |
| `R`         | QR_upper  | (combined)             | ~48        | Upper-tier graded                      |
| `P`         | P         | 227                    | 25         | Highest grade (Banei Kinen tier)       |
| `T`         | T         | 206                    | 22         | Special class                          |

Production model: `banei-cb-v7-lineage-wf-21y`.
Existing features already include: futan-class career stats, grade-career stats (add-banei-futan-class-features.py, add-banei-grade-career-features.py). Within-race relative features are **not** in production.

---

## Candidate Features

All are multi-column relationship features not present in production:

| Feature            | Definition                                                   |
| ------------------ | ------------------------------------------------------------ |
| futan_rank_in_race | rank(futan) within race (1=lightest); futan hex→kg parsed    |
| futan_deviation    | horse_futan − race_mean_futan (0.1 kg units)                 |
| futan_per_bw       | futan_kg / bataiju_kg (both hex-encoded)                     |
| futan_vs_win_hist  | current_futan_raw − median_winning_futan (horse train hist)  |
| futan_rank_x_pop   | futan_rank_in_race × ninki (interaction)                     |
| form_rank_in_race  | rank(avg_train_finish_pos) within race (1=best recent form)  |
| futan_delta        | current_futan − previous_race_futan (class-transition delta) |
| futan_dev_x_baba   | futan_deviation × baba_code numeric (1-5 only, hex excluded) |
| futan_dev_x_tenko  | futan_deviation × tenko (weather) numeric (1-6)              |

---

## Per-Class Partial Spearman ρ Table

Control: log(odds). n = valid rows per class. Gate: |ρ| ≥ 0.08.

| Feature                | Class       |     n | ρ           | p-value     | w/in-race var | Gate     | Notes                                                                |
| ---------------------- | ----------- | ----: | ----------- | ----------- | ------------- | -------- | -------------------------------------------------------------------- |
| futan_rank_in_race     | E_general   | 49812 | +0.0347     | <0.0001     | 5.87          | FAIL     | below gate                                                           |
| futan_rank_in_race     | E_named     |  4075 | +0.0190     | 0.225       | 5.87          | FAIL     | below gate                                                           |
| **futan_rank_in_race** | **P**       |   227 | **+0.1869** | **0.005**   | 5.87          | **PASS** | p<0.01 significant                                                   |
| futan_rank_in_race     | QR_upper    |   598 | +0.0230     | 0.575       | 5.87          | FAIL     | below gate                                                           |
| futan_rank_in_race     | T           |   206 | −0.0898     | 0.199       | 5.87          | pass     | not significant (p=0.20)                                             |
| futan_deviation        | E_general   | 49812 | +0.0103     | 0.022       | 108.4         | FAIL     | below gate                                                           |
| futan_deviation        | E_named     |  4075 | +0.0009     | 0.956       | 108.4         | FAIL     | near-zero                                                            |
| **futan_deviation**    | **P**       |   227 | **+0.1761** | **0.008**   | 108.4         | **PASS** | p<0.01 significant                                                   |
| futan_deviation        | QR_upper    |   598 | −0.0255     | 0.534       | 108.4         | FAIL     | below gate                                                           |
| futan_deviation        | T           |   206 | −0.0980     | 0.161       | 108.4         | pass     | not significant (p=0.16)                                             |
| futan_per_bw           | E_general   | 20871 | +0.0267     | <0.0001     | 0.003         | FAIL     | below gate; w/in-race var small                                      |
| futan_per_bw           | E_named     |  3569 | +0.0224     | 0.181       | 0.003         | FAIL     | below gate                                                           |
| futan_per_bw           | P           |   186 | −0.0833     | 0.258       | 0.003         | pass     | NOT significant (p=0.26)                                             |
| futan_per_bw           | QR_upper    |   459 | −0.0382     | 0.414       | 0.003         | FAIL     | below gate                                                           |
| futan_per_bw           | T           |   186 | +0.0800     | 0.278       | 0.003         | FAIL     | barely below gate; ns                                                |
| futan_vs_win_hist      | E_general   | 24164 | +0.0094     | 0.146       | 1413          | FAIL     | below gate; coverage 44%                                             |
| futan_vs_win_hist      | E_named     |  3316 | −0.0187     | 0.282       | 1413          | FAIL     | below gate                                                           |
| futan_vs_win_hist      | P           |   151 | −0.0631     | 0.442       | 1413          | FAIL     | below gate; ns                                                       |
| futan_vs_win_hist      | QR_upper    |   355 | +0.0231     | 0.664       | 1413          | FAIL     | below gate                                                           |
| futan_vs_win_hist      | T           |   184 | +0.0246     | 0.741       | 1413          | FAIL     | below gate                                                           |
| futan_rank_x_pop       | E_general   | 49812 | +0.0453     | <0.0001     | 420.7         | FAIL     | below gate                                                           |
| futan_rank_x_pop       | E_named     |  4075 | +0.0388     | 0.013       | 420.7         | FAIL     | below gate                                                           |
| **futan_rank_x_pop**   | **P**       |   227 | **+0.2631** | **<0.0001** | 420.7         | **PASS** | strongest signal; largely captures ninki collinearity (see analysis) |
| futan_rank_x_pop       | QR_upper    |   598 | +0.0751     | 0.067       | 420.7         | FAIL     | borderline but below gate                                            |
| futan_rank_x_pop       | T           |   206 | −0.0546     | 0.436       | 420.7         | FAIL     | below gate                                                           |
| **form_rank_in_race**  | **E_named** |  3316 | **+0.0817** | **<0.0001** | 6.64          | **PASS** | significant; 81% coverage                                            |
| form_rank_in_race      | E_general   | 24979 | +0.0576     | <0.0001     | 6.64          | FAIL     | below gate                                                           |
| form_rank_in_race      | P           |   151 | +0.0405     | 0.621       | 6.64          | FAIL     | small n                                                              |
| form_rank_in_race      | QR_upper    |   355 | +0.0428     | 0.421       | 6.64          | FAIL     | below gate                                                           |
| form_rank_in_race      | T           |   184 | +0.0944     | 0.202       | 6.64          | pass     | not significant (p=0.20)                                             |
| futan_delta            | E_general   | 48308 | +0.0184     | <0.0001     | 763.4         | FAIL     | below gate                                                           |
| futan_delta            | E_named     |  3946 | −0.0001     | 0.993       | 763.4         | FAIL     | zero signal                                                          |
| futan_delta            | P           |   219 | +0.0264     | 0.698       | 763.4         | FAIL     | below gate; ns                                                       |
| futan_delta            | QR_upper    |   588 | +0.0445     | 0.281       | 763.4         | FAIL     | below gate                                                           |
| futan_delta            | T           |   201 | −0.0310     | 0.662       | 763.4         | FAIL     | below gate                                                           |
| futan_dev_x_baba       | E_general   | 16130 | +0.0207     | 0.009       | 1146.9        | FAIL     | below gate                                                           |
| futan_dev_x_baba       | E_named     |  1242 | −0.0110     | 0.697       | 1146.9        | FAIL     | below gate; selection bias                                           |
| futan_dev_x_baba       | P           |    96 | +0.1668     | 0.104       | 1146.9        | pass     | NOT significant (p=0.10)                                             |
| futan_dev_x_baba       | QR_upper    |   228 | −0.0599     | 0.368       | 1146.9        | FAIL     | below gate                                                           |
| futan_dev_x_baba       | T           |    65 | −0.3757     | 0.002       | 1146.9        | PASS     | **ARTIFACT — see below**                                             |
| futan_dev_x_tenko      | E_general   | 49812 | +0.0127     | 0.005       | 332.4         | FAIL     | below gate                                                           |
| futan_dev_x_tenko      | E_named     |  4075 | +0.0091     | 0.560       | 332.4         | FAIL     | below gate                                                           |
| **futan_dev_x_tenko**  | **P**       |   227 | **+0.1526** | **0.022**   | 332.4         | **PASS** | moderate; see analysis below                                         |
| futan_dev_x_tenko      | QR_upper    |   598 | −0.0296     | 0.470       | 332.4         | FAIL     | below gate                                                           |
| futan_dev_x_tenko      | T           |   206 | −0.0474     | 0.499       | 332.4         | FAIL     | below gate                                                           |

---

## Gate-Passing Features — Detailed Analysis

### P-class: futan_rank_in_race, futan_deviation

- Bivariate Spearman(futan_rank, finish_pos) = −0.003 (no raw correlation)
- After controlling for log(odds): ρ=+0.187 (p=0.005)
- After controlling for BOTH log(odds) + ninki: ρ=+0.178 (p=0.008) — signal survives double control
- **Interpretation:** Within P-class, horses carrying more-than-average weight for the race finish worse than odds predict. The heavier futan burden is NOT fully priced by the market — residual penalty exists.
- P-class futan range: 67–100 kg (bimodal: light 67-77kg vs heavy 89-100kg group, distinct weight classes within same grade)
- **Verdict: PROCEED for P-class.**

### P-class: futan_rank_x_pop

- Bivariate ρ = +0.607 (p<0.0001) — but this largely mirrors ninki
- Partial ρ (log_odds control only) = +0.263 (p<0.0001)
- futan_rank is correlated with ninki (ρ=−0.17) and log_odds (ρ=−0.21): heavier horses tend to be less popular
- The interaction captures compounding of heavy burden + poor market position
- futan_rank and futan_deviation are highly collinear (same information); the interaction adds multiplicative but not independent signal
- **Verdict: PROCEED for P-class, but futan_rank or futan_deviation alone is preferred; futan_rank_x_pop is redundant given collinearity.**

### P-class: futan_dev_x_tenko

- ρ=+0.153 (p=0.022), n=227
- Marginal statistical significance; tenko_code covers weather (1=sunny, 2=cloudy, 6=snow)
- Ban-ei runs in winter Hokkaido; snow conditions interact with heavy futan
- However, tenko is already partially captured by track condition proxies in the existing feature set
- **Verdict: CONDITIONAL PROCEED — test after futan_rank_in_race first; likely marginal after futan rank is included.**

### P-class: futan_per_bw

- ρ=−0.083 (p=0.258), n=186; not statistically significant
- within-race variance = 0.003 (very small, horses have similar body weight)
- **Verdict: ABORT.**

### E_named: form_rank_in_race

- ρ=+0.0817 (p<0.0001), n=3316; barely above gate but large-sample significant
- Coverage: 81% (horses debuted pre-2023 have training history; 19% are null)
- Interpretation: within E_named races, horses with historically better finishes tend to outperform their odds prediction
- Incremental over single-column form features? Likely partially, because it is a within-race relative rank not an absolute career avg
- **Verdict: MARGINAL PROCEED — probe incremental over existing form features before implementing.**

### T-class: futan_dev_x_baba (ρ=−0.38) — ARTIFACT

- n=65 out of 206 T-class rows (only 32% coverage)
- Ban-ei's `babajotai_code_dirt` is hex-encoded: values like '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'
- The baba_map `{1:1, 2:2, 3:3, 4:4, 5:5}` (treating as decimal chars) selects only 31% of rows — the subset where baba_code is literally '1'–'5'
- Selection bias: those 65 rows may not be representative of T-class
- T-class has only 22 races total (2023-2026); n=65 provides ~3 data points per race
- **Verdict: ABORT — artifact of biased baba code selection, insufficient n.**

### T-class: futan_rank_in_race, futan_deviation

- ρ=−0.090 and −0.098 (sign reversal vs P-class), p=0.20 — not significant
- T-class has 22 races, 9.4 avg field size; insufficient for stable estimates
- **Verdict: ABORT — insufficient n and not significant.**

---

## Summary Table

| Feature            | Class       | ρ      | p      | Verdict          | Reason                                                                 |
| ------------------ | ----------- | ------ | ------ | ---------------- | ---------------------------------------------------------------------- |
| futan_rank_in_race | **P**       | +0.187 | 0.005  | **PROCEED**      | Survives double control (log_odds + ninki); genuine incremental signal |
| futan_deviation    | **P**       | +0.176 | 0.008  | **PROCEED**      | Same signal as futan_rank; collinear but valid                         |
| futan_rank_x_pop   | P           | +0.263 | <0.001 | ABORT            | Redundant with futan_rank given ninki control                          |
| futan_dev_x_tenko  | P           | +0.153 | 0.022  | CONDITIONAL      | Test after futan_rank; likely marginal addition                        |
| futan_per_bw       | P           | −0.083 | 0.258  | ABORT            | Not significant; tiny w/in-race variance                               |
| form_rank_in_race  | **E_named** | +0.082 | <0.001 | MARGINAL PROCEED | Significant but needs incremental vs existing form                     |
| futan_dev_x_baba   | T           | −0.376 | 0.002  | ABORT            | Artifact: 32% selection bias in baba_code mapping                      |
| futan_delta        | all         | ≤0.045 | ns     | ABORT            | No signal across all classes                                           |
| futan_vs_win_hist  | all         | ≤0.063 | ns     | ABORT            | Coverage 44-66%; no signal                                             |
| futan_rank_in_race | E_general   | +0.035 | —      | ABORT            | Below gate; overwhelmed by odds                                        |
| futan_deviation    | E_general   | +0.010 | —      | ABORT            | Below gate                                                             |
| futan_dev_x_baba   | all (non-T) | ≤0.167 | ns     | ABORT            | Biased coverage; P-class ns                                            |
| futan_rank_in_race | T, QR       | ≤0.09  | ns     | ABORT            | Small n; not significant                                               |

---

## Key Findings

1. **P-class is the only class with genuine futan-relationship signal.** `futan_rank_in_race` and `futan_deviation` both pass the gate with p<0.01 and survive double control (log_odds + ninki). The signal reflects that heavier-weight horses within P-class finishes worse than market pricing accounts for.

2. **E_general (49 812 rows, 95% of data) has near-zero futan-relationship signal.** The market fully prices futan effects in standard races.

3. **P-class has only 25 races (227 rows) in holdout.** Even a significant ρ=0.19 may not translate to meaningful NDCG improvement given class rarity.

4. **futan_rank_x_pop dominates numerically (ρ=0.26) but is collinear with ninki.** After controlling both log_odds and ninki, futan_rank (ρ=0.18) is the cleaner signal.

5. **futan_dev_x_baba T-class result (ρ=−0.38) is an artifact.** baba_code is hex-encoded in nvd_ra; the '1'-'5' selection covers only 32% of rows.

6. **form_rank_in_race for E_named passes the gate barely (ρ=0.082, p<0.001, n=3316).** This is a within-race relative form rank vs absolute career stats already in the model. Incremental value needs verification vs existing features.

---

## Conclusion

**PROCEED pairs (2):**

- `(P, futan_rank_in_race)`: ρ=+0.187, p=0.005, robust to double control
- `(P, futan_deviation)`: ρ=+0.176, p=0.008, collinear with above — implement one, test other

**MARGINAL PROCEED (1):**

- `(E_named, form_rank_in_race)`: ρ=+0.082, p<0.001, large-n significant — needs incremental ablation

**ALL OTHER FEATURES: ABORT** (odds-absorbed in E_general; insufficient n in T/QR; artifacts; not significant).

### Expected impact

P-class covers only ~4 races/year at Obihiro's highest level (Banei Kinen and Kinen-equivalent). Even if futan_rank_in_race genuinely improves ranking within those races, the global metric impact will be negligible (< 0.05pp top1 across Ban-ei). The finding confirms Ban-ei is saturated at the system level — the market efficiently prices futan in 95% of races.

### Implementation note

If implementing `futan_rank_in_race` for P-class:

- Parse futan using `try_cast('0x' || trim(futan_juryo) as integer)` (existing DuckDB pattern from add-banei-futan-class-features.py)
- Compute within-race rank via window function `RANK() OVER (PARTITION BY race_partition ORDER BY futan_kg)`
- Train P-class sub-model only or use per-class ensemble gating
- Retrain on 21-year window; evaluate on 2023-2026 holdout vs current baseline
