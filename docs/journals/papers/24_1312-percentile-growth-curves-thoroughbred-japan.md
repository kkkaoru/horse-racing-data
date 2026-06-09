# Empirical Percentile Growth Curves with Z-scores Considering Seasonal Compensatory Growths for Japanese Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 24(4): 63–69, 2013                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| docid                          | `24_1312`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Article type                   | Original Article                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| Authors                        | Tomoaki Onoda, Ryuta Yamamoto, Kyohei Sawamura, Harutaka Murase, Yasuo Nambo, Yoshinobu Inoue, Akira Matsui, Takeshi Miyake, Nobuhiro Hirai                                                                                                                                                                                                                                                                                                                                                                            |
| Affiliations                   | (1) Comparative Agricultural Sciences, Graduate School of Agriculture, Kyoto University, Kyoto 606-8502; (2) The Japan Bloodhorse Breeders' Association (JBBA), 4-5-4 Shinbashi, Minato-ku, Tokyo 105-0004; (3) JRA Facilities Co. Ltd., 4-5-4 Shinbashi, Minato-ku, Tokyo 105-0004; (4) Hidaka Training and Research Center, Japan Racing Association, 535-13 Nissha, Urakawa-cho, Hidaka, Hokkaido 057-0171; (5) Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya, Tochigi 320-0856 |
| Received / Accepted / Released | Received: September 13, 2013 / Accepted: October 30, 2013                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| Keywords                       | body weight, percentile growth curves, Thoroughbred horses, withers height                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/24/4/24_1312/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Abstract (verbatim)

> Percentile growth curves are often used as a clinical indicator to evaluate variations of children's growth status. In this study, we propose empirical percentile growth curves using Z-scores adapted for Japanese Thoroughbred horses, with considerations of the seasonal compensatory growth that is a typical characteristic of seasonal breeding animals. We previously developed new growth curve equations for Japanese Thoroughbreds adjusting for compensatory growth. Individual horses and residual effects were included as random effects in the growth curve equation model and their variance components were estimated. Based on the Z-scores of the estimated variance components, empirical percentile growth curves were constructed. A total of 5,594 and 5,680 body weight and age measurements of male and female Thoroughbreds, respectively, and 3,770 withers height and age measurements were used in the analyses. The developed empirical percentile growth curves using Z-scores are computationally feasible and useful for monitoring individual growth parameters of body weight and withers height of young Thoroughbred horses, especially during compensatory growth periods.

## Relevance to finishing-position (着順) prediction

Feature family: **F — conformation / body-size / growth trajectory**.

This paper establishes empirical percentile growth curves for body weight (BW) and withers height (WH) of Japanese Thoroughbreds from birth to ~1,100 days (~3 years), with explicit modelling of seasonal compensatory growth (CG) at 432 and 797 days. The key prediction-relevant output is the **Z-score framework**: each individual horse's BW or WH measurement at a specific age can be placed on a population-normalised scale, yielding a developmental-percentile feature that is free of mean-growth trends.

The variance components show that between-individual variation (σ²_a ≈ 1,431 for BW; σ²_a ≈ 16 for WH) dwarfs within-individual measurement noise (σ²_e ≈ 191–229 for BW; ≈ 3.5 for WH). This ratio (~7.5× for BW) means a single early-life BW measurement is a reliable, stable proxy for adult body size. Larger-for-age horses within the population may have a musculoskeletal development advantage at debut. The paper also documents that the 50–75th percentile band has been "ordinary" in recent Japanese Thoroughbred cohorts (the population has grown larger than the reference curves predict), meaning the reference Z-scores slightly underestimate modern horses — a systematic bias to account for if using historical reference values.

Practical feature derivations for the pipeline:

- `bw_zscore_at_entry` — horse's BW Z-score at yearling sale or first race entry, relative to the Onoda et al. population curves (computed from BW, age in days, sex, and the published σ²_a) — expected positive effect (heavier-for-age → better musculoskeletal capacity)
- `wh_zscore_at_yearling` — withers height Z-score at ~365 days — independent of BW and reflects skeletal frame; correlated with stride length and locomotor efficiency
- `birth_month_cg_timing` — months from birth to the first CG trough (centred at 432 days = ~14 months): horses born earlier in the year are older at the 432-day CG breakpoint, potentially yielding a developmentally smoother trajectory

## Background & objective

Japanese Thoroughbred foals in the Hidaka region (Hokkaido) experience pronounced seasonal CG: growth rate declines in winter and rebounds in spring, creating sigmoid inflections in the BW-age trajectory at approximately 432 days (first winter) and 797 days (second winter). Prior growth-curve work by the same group (Onoda et al., 2011, 2013) developed Richards growth-curve equations modified with sigmoid sub-functions f(t) and f'(t) to capture these CG periods, but had not translated those equations into clinically useful percentile reference charts with Z-scores. The present paper fills this gap by estimating random-effects variance components and constructing empirical percentile curves at 3, 10, 25, 50, 75, 90, and 97 percentiles — analogous to paediatric weight-for-age charts in human medicine.

## Materials & methods

**Data sources:** Hidaka Training and Research Center (JRA) and JBBA; collected 1999–2009.

**Body weight dataset:**

- Thoroughbred colts: 5,594 BW × age measurements from 271 horses
- Thoroughbred fillies: 5,680 BW × age measurements from 237 horses
- Age range: 0 to ~1,100 days (covers two winter CG periods before racing debut)

**Withers height dataset:**

- 3,770 WH × age measurements from 422 Thoroughbred colts and fillies combined
- Age range: 0 to ~800 days (covers first winter CG period only; second-CG WH data not available)

**Geographic context:** Hidaka region, Hokkaido — 815 stud farms, 82% of all Japanese racehorse stud farms (as of 2012 Hidaka Subprefectural Bureau data). Snow-covered winters; CG phenomenon pronounced.

**Growth curve models:** Based on Richards curve modified with sigmoid sub-functions:

For BW (colts and fillies separately):

- Equation 1 (colts): BW_male_ij = (575.0 + a_i) × [Richards equation] × f(t) × f'(t) + e_ij
- Equation 2 (fillies): analogous
- f(t): sigmoid sub-function centred on day 432 (first CG); f'(t): centred on day 797 (second CG)
- Maturity weight assumed: 575.0 kg; individual deviation a_i added to maturity weight

For WH (both sexes combined):

- Equation 3: WH_ij = (161.0 + a_i) × [Richards equation] × f(t) + e_ij
- Maturity WH assumed: 161.0 cm; only first CG period modelled (f'(t) not included due to data availability)

**Random effects:** Both individual horse effect (a_i) and residual (e_ij) included; population means E[a_i] = E[e_ij] = 0; independent. Variance components (σ²_a, σ²_e) estimated by **SAS NLMIXED procedure**.

**Empirical percentile curves using Z-scores:**

- Z-score assumption: data normally distributed around population mean
- Z-score formula: percentile_curve(t) = mean_growth(t) ± Z × sqrt(σ²_a + σ²_e)
- Where Z for percentiles: 3%ile: −1.881; 10%ile: −1.282; 25%ile: −0.675; 50%ile: 0.000; 75%ile: +0.675; 90%ile: +1.282; 97%ile: +1.881

**Validation:** Data percentages within each percentile interval counted at 180-day age intervals (0–180, 360, 540, 720, 900, and all days combined).

## Results (detailed — all numbers reproduced)

**Variance components:**

| Trait               | σ²_a     | σ²_e   | Significance  |
| ------------------- | -------- | ------ | ------------- |
| Colt BW (kg²)       | 1,431.14 | 191.51 | Both P<0.0001 |
| Filly BW (kg²)      | 1,431.01 | 228.95 | Both P<0.0001 |
| WH both sexes (cm²) | 16.14    | 3.52   | Both P<0.0001 |

Derived SDs: colt BW individual SD = √1,431 = ±37.8 kg; residual SD = √192 = ±13.9 kg. WH individual SD = √16.1 = ±4.0 cm; residual SD = √3.5 = ±1.9 cm.

The individual-to-residual variance ratio for BW: σ²_a/σ²_e ≈ 7.5× (colt) and 6.3× (filly). For WH: σ²_a/σ²_e ≈ 4.6×. This confirms BW and WH are dominated by stable individual-level differences rather than measurement noise.

**Validation (data distributions in percentile intervals):**

Colt BW data (selected rows from Table 1):
| Age period | 25–50%ile observed | 50–75%ile observed | Skew direction |
|------------|-------------------|--------------------|---------------|
| 0–180 days | 28.58% | 43.41% | Above 50th |
| All ages | 25.01% | 39.01% | Above 50th |

Filly BW data (Table 2):
| Age period | 25–50%ile observed | 50–75%ile observed |
|------------|-------------------|--------------------|
| 0–180 days | 34.78% | 42.88% |
| All ages | 31.37% | 40.81% |

WH both sexes (Table 3):
| Age period | 25–50%ile observed | 50–75%ile observed |
|------------|-------------------|--------------------|
| 0–180 days | 12.15% | 23.81% |
| All ages | 27.22% | 35.81% |

In all traits, the majority of observed data fall in or above the 50th percentile curve, confirming that the current Japanese Thoroughbred population (1999–2009) is **larger** than the reference equations (based on earlier standard values of mature BW 575 kg and WH 161 cm) predict. This upward shift is a known trend in modern Japanese breeding.

**CG patterns:** CG effects clearly visible in both BW and WH percentile plots around 432 days; second CG for BW at 797 days also confirmed. Authors confirmed CG presence in WH via formal model comparison (with vs. without f(t)).

**Sex differences in WH:** Less than 2 cm difference between sexes at most ages, justifying the combined-sex WH model.

**Colts vs. fillies BW σ²_a:** Virtually identical (1,431.14 vs. 1,431.01) — notable coincidence, suggesting the population variance structure is sex-invariant for BW.

## Discussion & interpretation

The authors note that the percentile curves are "currently underestimates" of modern body sizes — the 50–75th percentile window is where most recent Thoroughbreds fall, implying the true population median has shifted above the historical reference growth curve. Racehorse managers in the Hidaka region should interpret the curves with this in mind. The study's key practical value is providing a continuous, individual-horse Z-score computable from a single BW or WH measurement at any age: a foal weighing significantly above the 75th percentile at 432 days is developmentally advanced but may also face higher osteochondrosis risk during rapid rebound growth (Mohammed, 1990, Prev. Vet. Med.). The paper argues that Z-score-based percentile curves are preferable to purely data-driven smooth curves (LMS method, kernel regression, etc.) because they allow parametric hypothesis testing and extreme-value quantification.

The authors compare their approach to human paediatric growth standards (WHO, NCHS) as the closest analogy, emphasising that the mathematical framework (Richards equation + CG sub-functions + NLMIXED variance estimation) is the novelty for the horse literature.

## Limitations

- Reference population is exclusively Hidaka-region Hokkaido horses (1999–2009); applicability to Thoroughbreds from other prefectures or time periods may be limited.
- Maturity weight (575 kg) and WH (161 cm) are based on older JRA feeding standards and are now known to be underestimates for contemporary horses — the paper flags this explicitly.
- WH data limited to ~800 days; second CG period for WH is not characterised.
- No performance outcomes (race results) are linked; the developmental-percentile features are hypothesised to be predictive but not validated against racing performance in this paper.
- Normal distribution assumption for Z-scores may not hold perfectly, especially in tails (top-performing bloodlines may cluster at upper percentiles).
- Growth data collected 1999–2009; secular trends in body size mean more recent cohorts may require updated reference parameters.

## Feature-engineering notes for the model

- `bw_zscore_debut` — Z-score of horse's first recorded body weight relative to Onoda et al. population mean and SD at the same age (days) and sex; formula: Z = (BW_observed − BW_predicted(t)) / sqrt(σ²_a + σ²_e); source: 馬体重 from race entry records (debut race weight is recorded); expected positive effect on finishing position (larger-for-age → better musculoskeletal capacity)
- `bw_zscore_current` — same but using the most recent pre-race weight measurement — captures current developmental state vs. population norm — high availability (race-day 馬体重 always recorded in JRA/NAR)
- `bw_growth_rate_L90d` — (current_bw − bw_90days_ago) / 90 — rate of weight gain; fast growers near 432-day CG may be at elevated musculoskeletal risk — source: race history 馬体重 series
- `cg_period_flag` — binary: horse age at race falls within 420–444 days (first CG onset) or 787–807 days (second CG onset) — captures the developmentally turbulent CG window — derivable from birth date + race date
- `birth_month_adj_age` — actual age in days (not universal Jan 1 birthday age) at race — important for applying the Onoda growth curve correctly; universal age underestimates early-year-born horses — source: birth date from registration records
- **DO NOT** apply BW Z-score features for horses beyond ~3 years old (1,100 days); the growth curves are only validated up to that age range. For adult racing horses, raw 馬体重 and 馬体重変化 are more appropriate.
- **Interaction with season:** CG-period weight dips (around 432 and 797 days) may cause temporary apparent underweight that does not reflect true musculoskeletal development level; apply a seasonal CG correction before interpreting Z-scores during winter months.

## Key references / follow-up leads

- Onoda et al. (2011) J. Equine Sci. 22: 37–42 — empirical growth curve estimation with sigmoid CG sub-functions for male Thoroughbred BW (first paper in this series; provides the base equations used here)
- Onoda et al. (2013) J. Anim. Sci. (in press at time of publication) — extension to multiple CG periods for both colts and fillies
- Mohammed (1990) Prev. Vet. Med. 10: 63–71 — case-control study: fast-growing Thoroughbred yearlings at elevated osteochondrosis risk; growth-rate outliers (high BW Z-score) as injury/scratch risk signal
- Kocher & Staniar (2013) Livest. Sci. 154: 204–214 — foal birthdate effect on growth pattern in Thoroughbreds (cross-referenced as companion paper)
- Brown-Douglas & Pagan (2006) pp. 213–220 in Advances in Equine Nutrition Vol. IV — body weight and withers height growth rates in Thoroughbreds from US, England, Australia, NZ, India; international comparison reference
- Staniar et al. (2004) J. Anim. Sci. 82: 1007–1015 — Thoroughbred growth characterised by baseline and systematic deviation components; underpins the CG modelling approach
- Equine Research Institute (2004) Japanese Feeding Standard for Horses — provides the reference mature BW 575 kg and WH 161 cm values used in the Onoda equations
