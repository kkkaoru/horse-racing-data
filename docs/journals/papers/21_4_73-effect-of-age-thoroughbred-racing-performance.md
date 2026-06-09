# The Effect of Age on Thoroughbred Racing Performance

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                        |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 21(4): 73–78, 2010                                                                                                                                                                                                                            |
| docid                          | `21_4_73`                                                                                                                                                                                                                                                    |
| Article type                   | Original Article                                                                                                                                                                                                                                             |
| Authors                        | Marshall Gramm, Ryne Marksteiner                                                                                                                                                                                                                             |
| Affiliations                   | (1) Department of Economics and Business, Rhodes College, 2000 N. Parkway, Memphis, TN 38112-1690, USA; (2) Department of Economics, University of Wisconsin, William H. Sewell Social Science Building, 1180 Observatory Drive, Madison, WI 53706-1393, USA |
| Received / Accepted / Released | Accepted: September 17, 2010                                                                                                                                                                                                                                 |
| Keywords                       | age, peak performance, racing, speed figures, Thoroughbred                                                                                                                                                                                                   |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/21/4/21_4_73/_pdf/-char/en                                                                                                                                                                                          |

## Abstract (verbatim)

> Using a dataset of 274 male Thoroughbred racehorses in the United States, we study the effect of age on racing performance. Beyer speed figures, which are uniform measures of racing performance across distance and racing surface, are utilized in this study. A system of equations is estimated to determine quadratic improvement and decline in racing performance. We find that a typical horse's peak racing age is 4.45 years. The rate of improvement from age 2 to 4½ is greater than the rate of decline after age 4½. A typical horse will improve by 10 (horse) lengths in sprints (less than 1 mile) and 15 lengths in routes (one mile or greater) from age 2 to 4½. Over the next five years the typical decline is 6 lengths for sprints and 9½ lengths for routes.

## Relevance to finishing-position (着順) prediction

Feature family: **G — statistical modelling / age**.

This is a direct quantitative performance-modelling paper. The key output — peak racing age 4.45 years, quadratic improvement/decline trajectory, asymmetric improvement faster than decline — translates directly into actionable features for the finishing-position pipeline.

**Direct feature derivations:**

1. `actual_age_at_race` — horse's actual age in years (not the universal January 1 birth date age used in official classification) — the paper explicitly uses actual age; the NLS estimates (γ₁=−4.584, γ₂=−0.578, δ=4.45) provide the functional form
2. `age_deviation_from_peak` — |actual_age − 4.45| — continuous distance-from-peak feature; applies the paper's finding that both under-4.45 and over-4.45 horses underperform relative to their career mean
3. `age_quadratic_phase` — `(actual_age − 4.45)²` with separate coefficients for improvement phase (age < 4.45) and decline phase (age > 4.45) — the piecewise quadratic model; γ₁ (improvement) ≈ 7.9× larger in absolute value than γ₂ (decline), reflecting the asymmetric trajectory

**Distance interaction:** Routes (≥1 mile) show a 50% larger improvement arc (+15 lengths) and 58% larger decline (−9.5 lengths) than sprints (+10 lengths, −6 lengths). This justifies an `age_sq × distance_category` interaction term: young horses are especially disadvantaged in routes; the peak-age advantage is most pronounced in routes.

**Beyer figure scale reference:** The paper provides the conversion table — a 6-furlong race at 1:12 = speed figure 78; 1:09 = 120; beaten by 2 lengths = −5 points. This is analogous to JRA speed rating normalisation, validating the concept that a normalised speed figure corrected for track variant and distance can have the same age-curve correction applied.

## Background & objective

Northern-hemisphere racehorses are given a universal January 1 birthday, so official "age" differs from actual age by up to 12 months. When horses race outside their age cohort (especially in open-aged competitions from 3.25+ years), they face competitors spanning 7+ years of age. The paper adapts Fair's (2008) baseball aging model — a piecewise quadratic fixed-effects regression — to Thoroughbred racing to estimate: (1) the age of peak performance, (2) the rate of improvement from debut to peak, and (3) the rate of decline from peak to end of career. The use of horse fixed effects controls for innate ability, isolating the pure age effect.

## Materials & methods

**Dataset:**

- 274 male US Thoroughbred horses, all ≥6 years old at data cutoff, ≥45 career starts
- 16,783 individual race observations
- Source: Beyer speed figures from the Daily Racing Form

**Selection criteria rationale:** ≥45 starts and ≥6 years old ensures sufficient career length for reliable peak-age estimation. However, this excludes horses with early stallion retirement (typically the top performers retired to breeding by age 5 with 20–35 starts) — creating a **selection bias toward lower-ability, longer-racing geldings/non-breeding males**.

**Summary statistics (Table 3):**
| Variable | Mean | Min | Max |
|----------|------|-----|-----|
| Starts | 65.1 | 45 | 121 |
| Age at last start | 8.0 | 4.7 | 12.4 |
| Wins | 9 | 1 | 23 |
| Earnings | $178,914 | $28,699 | $1,174,781 |
| Avg Speed Figure | 59.1 | 31.0 | 88.7 |
| SD Speed Figure | 15.3 | 8.6 | 25.4 |
| Best Speed Figure | 84.5 | 50 | 108 |
| Avg Distance (furlongs) | 6.91 | 5.22 | 9.24 |

**Beyer speed figure system (Table 1 — speed to figure conversion, 3/4 mile / 1⅛ mile):**
| Time (3/4 mi) | Figure | Time (1⅛ mi) | Figure |
|--------------|--------|--------------|--------|
| 1:08 | 135 | 1:47 | 132 |
| 1:09 | 120 | 1:48 | 122 |
| 1:10 | 106 | 1:49 | 113 |
| 1:11 | 92 | 1:50 | 104 |
| 1:12 | 78 | 1:51 | 95 |
| 1:13 | 64 | 1:53 | 77 |
| 1:15 | 37 | 1:55 | 60 |

**Beaten lengths adjustment (Table 2):**
| Margin (lengths) | 3/4 mile pts | 1⅛ mile pts |
|-----------------|-------------|------------|
| 0.5 | 1 | 1 |
| 1 | 2 | 2 |
| 2 | 5 | 3 |
| 3 | 7 | 5 |
| 5 | 12 | 8 |
| 10 | 25 | 16 |

**Performance metric:** Beyer speed figure = normalised figure accounting for track variant and distance, but NOT weight carried. A high-calibre horse in top races scores 100–120.

**Econometric model (Fair 2008 adapted):**

- Piecewise quadratic horse fixed-effects model:
  - y_it = α_i + β₁x_it + γ₁x²_it + ε_it if x_it ≤ δ (improvement phase)
  - y_it = α_i + β₂x_it + γ₂x²_it + ε_it if x_it > δ (decline phase)
  - where y_it = speed figure, x_it = actual age, δ = peak age (estimated), α_i = horse fixed effect
  - Constraints ensure continuity and zero slope at peak: β₁ = −2γ₁δ; β₂ = −2γ₂δ
  - Estimated by non-linear least squares (NLS)
- Age categorisation (Table 4): 25 bins in quarter-year intervals for descriptive analysis

## Results (detailed — all numbers reproduced)

**Regression estimates (Table 5):**
| Parameter | Estimate | Standard Error |
|-----------|----------|---------------|
| γ₁ (improvement curvature) | **−4.584** | 0.127 |
| γ₂ (decline curvature) | **−0.578** | 0.021 |
| δ (peak age) | **4.45** | (not reported) |
| Observations | 16,783 | — |
| Horses | 274 | — |

Note: γ₁/γ₂ = −4.584/−0.578 ≈ 7.9× — the curvature of improvement is 7.9× steeper than the curvature of decline. Performance improves faster than it falls.

**Predicted performance differences from peak age (Table 5, Rk and Dk):**
| Age | R_k (points below peak) | D_k (% change per year) |
|-----|------------------------|------------------------|
| 2 | −26.32 | 37.43% per year improvement |
| 3 | −9.09 | 21.70% per year improvement |
| 4 | −1.02 | 5.98% per year improvement |
| 5 | −0.58 | −1.23% per year decline |
| 6 | −1.87 | −3.21% per year decline |
| 7 | −4.32 | −5.20% per year decline |
| 8 | −7.93 | −7.18% per year decline |

**Age-category analysis (Table 4, full data):**
| Age bracket | Starts | Starters | Diff (pts) | St Diff (SD) | % Career High SF |
|-------------|--------|---------|-----------|-------------|-----------------|
| <2.5 | 357 | 130 | **−17.2** | −1.14 | 0.36% |
| 2.5–2.75 | 439 | 165 | −12.4 | −0.81 | 0.55% |
| 2.75–3.0 | 486 | 177 | −5.9 | −0.39 | 2.19% |
| 3.0–3.25 | 698 | 213 | −3.1 | −0.21 | 3.16% |
| 3.25–3.5 | 770 | 222 | +0.9 | +0.06 | 8.45% |
| 3.5–3.75 | 704 | 220 | +1.9 | +0.13 | 7.12% |
| 3.75–4.0 | 665 | 209 | +3.3 | +0.22 | 4.32% |
| 4.0–4.25 | 847 | 238 | +3.6 | +0.23 | 9.88% |
| **4.25–4.5** | **944** | **249** | **+5.0** | **+0.33** | 8.36% |
| 4.5–4.75 | 798 | 236 | +4.9 | +0.32 | 7.79% |
| 4.75–5.0 | 715 | 218 | +4.5 | +0.29 | 6.87% |
| 5.0–5.25 | 849 | 245 | +3.7 | +0.24 | 5.90% |
| 5.25–5.5 | 918 | 247 | +3.3 | +0.21 | 7.91% |
| 5.5–5.75 | 794 | 235 | +1.7 | +0.11 | 4.68% |
| 5.75–6.0 | 632 | 198 | +0.6 | +0.04 | 2.74% |
| 6.0–6.25 | 863 | 242 | +1.5 | +0.10 | 4.53% |
| 6.25–6.5 | 914 | 247 | +0.1 | +0.01 | 2.95% |
| 6.5–6.75 | 591 | 189 | 0.0 | 0.00 | 1.82% |
| 6.75–7.0 | 437 | 148 | −0.5 | −0.03 | 1.46% |
| 7.0–7.25 | 551 | 162 | −1.0 | −0.06 | 1.82% |
| 7.25–7.5 | 592 | 162 | −0.9 | −0.06 | 2.55% |
| 7.5–8.0 | 611 | 126 | −2.8 | −0.18 | 1.09% |
| 8.0–8.5 | 717 | 118 | −3.8 | −0.25 | 2.37% |
| 8.5–9.0 | 326 | 73 | −6.1 | −0.39 | 0.18% |
| >9.0 | 565 | 55 | **−7.1** | **−0.46** | 0.91% |

**Career-best speed figure distribution by age:**

- Age 2: 3.0% (≈3.5% extrapolated from Table 4 <2.5 and 2.5–3 combined)
- **Age 3: 23.1%** of horses earned career-best figure
- **Age 4: 32.9%** — highest single-year proportion
- Age 5: 21.2%
- Cumulative through age 5: **80% of horses earned their career-best speed figure**
- Only 0.18% earned career best at age 8.5–9.0

**Age × distance interaction:**

- Age <2.5: run 0.75 furlongs shorter than career average distance (young horses race short sprints)
- No systematic distance shift with age beyond the initial 2-year-old sprint-only period
- Performance magnitude difference by distance: improvement sprint +10 lengths vs. route +15 lengths (age 2→peak); decline sprint −6 lengths vs. route −9.5 lengths (peak→9.5 yrs)

**Notable horses:** Fort Prado (only millionaire, $1,174,781 earnings, average figure 88.7, the dataset maximum); Cool N Collective (oldest at 12.4 years at last start); Ww Conquistador (most starts: 121; most wins: 23); Sunshine Bear ran career-best figure of 91 at age 9.3 (winning by 14 lengths).

## Discussion & interpretation

The piecewise quadratic fixed-effects model effectively isolates the age effect by controlling for innate horse ability (via horse-level intercepts α_i). The asymmetric improvement/decline trajectory (γ₁/γ₂ ≈ 7.9×) is consistent with biological development models: musculoskeletal maturation is rapid in the 2–5 year window, while degenerative decline is a slower process. The peak at 4.45 years aligns with Thoroughbred physiology — full skeletal ossification and muscle fibre development typically complete between ages 4 and 5.

The distance-specific magnitude difference (routes >sprints for both improvement and decline) likely reflects that route horses need more time to develop aerobic capacity and stamina (later-developing physiological traits) while sprint ability is more innate and earlier-expressed. Young horses run shorter races not because they prefer sprints but because trainers and race conditions restrict them to manageable distances early in training.

The selection bias (only long-career geldings/low-value males ≥45 starts) means the estimated peak age (4.45) may be slightly conservative for elite performers who peak earlier and retire to stud. However, for the purposes of a handicapping model applied to the population of all active racehorses (most of which are geldings or lower-value males), the 4.45 estimate and the quadratic trajectory are appropriate priors.

## Limitations

- US-only data; may not fully apply to Japanese Thoroughbreds (JRA/NAR) where surface, distances, and training protocols differ (Japanese turf racing is common; US flat racing on dirt dominates the dataset)
- Sample restricted to males ≥45 starts, ≥6 years old; excludes top stallions (retired early) and all females (different age curve possibly)
- 16,783 observations from 274 horses = average 61.3 starts/horse — career-heavy survivors with selection toward durability may have a different age profile than horses retired early due to injury
- Beyer figures not adjusted for weight carried (futan/斤量 in JRA terminology); weight adjustment matters more in longer routes where weight penalties accumulate across distance
- No health/injury information; horses who peaked early and declined sharply may have had unobserved health issues
- US track variant and Beyer figure system differ from JRA speed index methodology; recalibration of γ₁, γ₂, δ parameters may be needed for Japanese racing data

## Feature-engineering notes for the model

- `actual_age_years` — horse's precise age at race date (birth date to race date in years, e.g., 4.37 years); source: birth date (registration) + race date — use actual age, NOT universal Jan 1 age — JRA birth date is known from registration records
- `age_sq_improvement` — (actual_age − 4.45)² × I(actual_age ≤ 4.45) — quadratic improvement term; scaled by γ₁ prior (−4.584); use as initialisation for regularised regression
- `age_sq_decline` — (actual_age − 4.45)² × I(actual_age > 4.45) — quadratic decline term; scaled by γ₂ prior (−0.578)
- `age_deviation_from_peak` — |actual_age − 4.45| — simpler single-feature version; magnitude captures distance from prime; use with signed version to capture direction
- `age_x_route_flag` — actual_age × I(distance ≥ 1600 m for JRA; ≥1 mile) — interaction: age effect is 50% larger in routes than sprints; essential for distance-stratified models
- `is_debut_age_cohort` — I(actual_age < 2.5) — young debut horses run 17.2 pts below career mean; flag to mark the "young penalty" period
- `career_race_number` — sequence number of current race in horse's career — correlated with age but provides additional "experience" signal beyond raw chronological age; early-career horses benefit from experience accumulation
- `futan_age_interaction` — weight carried (futan/斤量) × actual_age — weight penalties typically increase with age (heavier for older horses in JRA age-weight scale); interaction may reveal age-weighted performance sensitivity
- **CAUTION:** The US γ₁ (−4.584) and γ₂ (−0.578) are US-track-specific estimates. Japanese turf racing may show different magnitudes; treat as priors with regularisation rather than fixed values. Validate against a JRA-specific age-performance regression using historical race data.

## Key references / follow-up leads

- Fair (2008) J. of Quan. Anal. in Sports 4: 1–39 — baseball aging model adapted here; piecewise quadratic fixed-effects model details
- Bailey et al. (1999) Vet. Rec. 145: 487–493 — injury/disease impacts on cohort of 2- and 3-year-old Thoroughbreds in training; age-specific injury rates that may confound the age-performance curve
- More (1999) Aust. Vet. J. 77: 105–112 — longitudinal study of racing Thoroughbreds: performance during first years of racing; Australian comparison
- Brown-Douglas et al. (2006) Proc. Kentucky Equine Res. Nutr. Conf. 15: 16–29 — Thoroughbred growth and future racing performance; physical development (weight, height as foals) predicting future racing success
- Sobczyńska (2007) Anim. Sci. Papers and Reports 25: 131–141 — factors affecting length of racing career in Polish Thoroughbred racehorses; career length as performance correlate
- Gaffney & Cunningham (1988) Nature 332: 722–723 — genetic trend in Thoroughbred racing performance; the performance plateau in English classic times
- Thompson (1995) J. Anim. Sci. 73: 2513–2517 — skeletal growth rates in weanling and yearling Thoroughbred horses; developmental basis for age-performance trajectory
