# An Approach of Estimating Individual Growth Curves for Young Thoroughbred Horses Based on Their Birthdays

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                                                 |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 25(2): 29–35, 2014                                                                                                                                                                                                                                                                                                                                                                     |
| docid                          | `25_1402`                                                                                                                                                                                                                                                                                                                                                                                             |
| Article type                   | Original Article                                                                                                                                                                                                                                                                                                                                                                                      |
| Authors                        | Tomoaki ONODA, Ryuta YAMAMOTO, Kyohei SAWAMURA, Harutaka MURASE, Yasuo NAMBO, Yoshinobu INOUE, Akira MATSUI, Takeshi MIYAKE, Nobuhiro HIRAI                                                                                                                                                                                                                                                           |
| Affiliations                   | Comparative Agricultural Sciences, Graduate School of Agriculture, Kyoto University, Kyoto 606-8502, Japan; The Japan Bloodhorse Breeders' Association, Tokyo 105-0004, Japan; JRA Facilities Co., Ltd., Tokyo 105-0004, Japan; Hidaka Training and Research Center, Japan Racing Association, Hokkaido 057-0171, Japan; Equine Research Institute, Japan Racing Association, Tochigi 320-0856, Japan |
| Received / Accepted / Released | January 17, 2014 / February 25, 2014 / 2014                                                                                                                                                                                                                                                                                                                                                           |
| Keywords                       | birthday, body weight, individual growth curve, Thoroughbred, withers height                                                                                                                                                                                                                                                                                                                          |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/25/2/25_1402/_pdf/-char/en                                                                                                                                                                                                                                                                                                                                   |

## Abstract (verbatim)

> We propose an approach of estimating individual growth curves based on the birthday information of Japanese Thoroughbred horses, with considerations of the seasonal compensatory growth that is a typical characteristic of seasonal breeding animals. The compensatory growth patterns appear during only the winter and spring seasons in the life of growing horses, and the meeting point between winter and spring depends on the birthday of each horse. We previously developed new growth curve equations for Japanese Thoroughbreds adjusting for compensatory growth. Based on the equations, a parameter denoting the birthday information was added for the modeling of the individual growth curves for each horse by shifting the meeting points in the compensatory growth periods. A total of 5,594 and 5,680 body weight and age measurements of Thoroughbred colts and fillies, respectively, and 3,770 withers height and age measurements of both sexes were used in the analyses. The results of predicted error difference and Akaike Information Criterion showed that the individual growth curves using birthday information better fit to the body weight and withers height data than not using them. The individual growth curve for each horse would be a useful tool for the feeding managements of young Japanese Thoroughbreds in compensatory growth periods.

## Relevance to finishing-position (着順) prediction

Feature family **F (conformation/body-size)** with direct relevance to young-horse performance at JRA.

This paper is directly applicable to the target domain (Japanese Thoroughbreds, JRA/JBBA data). It establishes that relative birth date (RBD, parameter d = days from December 31) is a significant predictor of body weight at any given age for Thoroughbred foals in Hokkaido, due to the seasonal compensatory growth (CG) pattern in cold-winter regions. The practical implication: a horse born in June will meet its first CG period approximately 5 months earlier in its life than a January foal, causing a substantial body weight difference at the same chronological age or race date.

For the finishing-position model:

1. Body weight at race entry (馬体重, available in JRA race records) is already a direct feature. This paper provides the biological framework for why birth month should be included as a covariate when interpreting body weight: a June-born 2-year-old running in October is at a different growth-phase position than a January-born 2-year-old running in October.
2. A `bw_residual_from_growth_curve` feature (actual body weight minus birthday-adjusted expected body weight for that age) could flag underconditioning or developmental stress.
3. The birthday-adjusted growth curve is specifically fitted on Hokkaido-raised horses (Hidaka region), which is where the majority of JRA Thoroughbreds are bred and initially raised.

The AIC improvement from adding the birthday parameter is consistent across birth months for BW (all months favoured d-shifting for fillies; colts born in Jan, Feb, Mar, Jun showed P<0.01 improvement). For withers height, the effect is weaker (significant mainly for June-born horses and March–June on AIC).

## Background & objective

Thoroughbred foals are seasonal breeding animals born in spring; their growth declines in winter and rebounds rapidly in the following spring (compensatory growth, CG). The CG period timing depends on the birthday of each foal relative to the calendar winter. Previous work by the same group (Onoda et al. 2013) established population-average growth curve equations for Japanese Thoroughbreds with CG adjustments. This paper extends those equations to individual level by adding a parameter d that shifts the CG period centres (at days 432 and 797 of age) based on each foal's birthday. Objective: confirm that individual birthday-adjusted curves fit the data better than the population-average curve.

## Materials & methods

**Data:** Collected by Hidaka Training and Research Center (JRA) and Japan Bloodhorse Breeders' Association (JBBA) from the Hidaka region of Hokkaido, 1999–2009.

**Body weight (BW):**

- Colts: 5,594 BW × age measurements from 271 horses
- Fillies: 5,680 BW × age measurements from 237 horses

**Withers height (WH):**

- Both sexes combined: 3,770 WH × age measurements from 422 horses

**Birth month distribution (Table 1):**

| Birth month | BW Colts (N foals / N measurements) | BW Fillies (N foals / N measurements) | WH (N foals / N measurements) |
| ----------- | ----------------------------------- | ------------------------------------- | ----------------------------- |
| Jan         | 9 / 198                             | 8 / 96                                | 12 / 127                      |
| Feb         | 57 / 1,126                          | 57 / 1,123                            | 87 / 892                      |
| Mar         | 75 / 1,535                          | 67 / 1,307                            | 115 / 922                     |
| Apr         | 85 / 1,031                          | 57 / 1,167                            | 127 / 998                     |
| May         | 39 / 1,092                          | 38 / 813                              | 67 / 572                      |
| Jun         | 6 / 612                             | 10 / 674                              | 14 / 259                      |
| **Total**   | **271 / 5,594**                     | **237 / 5,680**                       | **422 / 3,770**               |

Modal birth month: March (largest N in colts and fillies BW; also WH).

**Birthday parameter d:** d = numerical date of birthday counted from December 31st. Examples: January 1 = d=1; February 15 = d=46; June 7 = d=158. Population average d=81 (≈March 21–22nd).

**Individual growth curve equations (BW colts Equation 1, BW fillies Equation 2):** Identical to Onoda et al. (2013) population-average equations except that "+ d − 81.0" is inserted in the sub-functions f(t) and f'(t). The sub-functions adjust for first CG (centred at day 432 of age) and second CG (centred at day 797 of age). If d=81, equations revert to population average.

**WH equation (Equation 3):** Same structure; same CG adjustment periods; "+ d − 81.0" in f(t) only (single CG period assumed for WH).

**Model comparison methods:**

1. Prediction Error Difference (PED) = e₁² − e₂² where e₁ = prediction error using own d, e₂ = prediction error using d=81. If mean PED < 0, individual d-shifting is better. Tested against zero by SAS MEANS t-test.
2. Akaike Information Criterion (AIC): computed per birth month group by SAS NLMIXED; lower AIC = better model.

## Results (detailed — reproduce ALL numbers)

**Prediction Error Difference (PED) results — significance levels by birth month:**

_Colt BW:_

- January: P<0.01 (d shifting better)
- February: P<0.01
- March: P<0.01
- April: NS (no significant effect)
- May: NS
- June: P<0.01

_Filly BW:_

- Generally negative PED for all birth months (d shifting better)
- February: P<0.01 (significant)
- May: P<0.01 (significant)
- Others: NS (but negative PED trend)

_WH:_

- June: significant effect of d shifting (P not specified but stated as detected)
- May: high positive PED (d=81 better, but insignificant)
- March–June: better on AIC (see below)

**AIC comparison (Table 2 of paper):**

| Birth month | Model           | BW Colts AIC | BW Fillies AIC | WH AIC     |
| ----------- | --------------- | ------------ | -------------- | ---------- |
| Jan         | d shifting      | **1,778**    | **770**        | 493        |
| Jan         | no shifting     | 1,793        | 799            | 492        |
| Feb         | d shifting      | **9,183**    | **9,418**      | **3,754**  |
| Feb         | no shifting     | 9,391        | 9,550          | 3,729      |
| Mar         | d shifting      | **12,430**   | **11,205**     | **3,976**  |
| Mar         | no shifting     | 12,506       | 11,229         | 3,978      |
| Apr         | d shifting      | 8,121        | **13,585**     | **4,695**  |
| Apr         | no shifting     | **8,058**    | 13,648         | 4,726      |
| May         | d shifting      | **9,093**    | **6,639**      | **2,585**  |
| May         | no shifting     | 9,013        | 6,837          | 2,616      |
| Jun         | d shifting      | **4,904**    | **5,462**      | **923**    |
| Jun         | no shifting     | 5,100        | 5,598          | 1,052      |
| **Total**   | **d shifting**  | **45,675**   | **47,157**     | **16,535** |
| **Total**   | **no shifting** | 46,068       | 47,707         | 16,673     |

Bold/italic = better fit. Total AIC improvement: BW Colts −393; BW Fillies −550; WH −138.

Most birth months show better AIC with d shifting for BW (exception: April colts, no shifting slightly better). All total AIC values favour d shifting.

**CG period shift example (Fig. 3 of paper):**

- Colt born June 7 (d=158): his individual growth curve with d=158 places the first CG centre at day 432 + (158 − 81) = day 509 of age; the curve describes his actual data profile more precisely during both CG periods than the d=81 standard curve.
- June foal meets first CG period ~5 months earlier in his life than a January foal.

**Seasonal shift pattern (Fig. 2 of paper):**
The BW scatterplots separated by birth month clearly show the CG troughs shifting leftward (earlier in age) as birth month moves from January to June. Withers height shows the same trend but less clearly.

## Discussion & interpretation

The key insight is that the CG phenomenon in Hokkaido Thoroughbreds is triggered by the calendar winter season, not by a fixed age of the foal. All foals experience the growth slowdown at approximately the same calendar period (winter), but because foals are born at different points in the year, this calendar-driven slowdown occurs at very different ages (in days) for January vs. June foals.

The d-shifting approach is a simple one-parameter extension of the population-average curve and is applicable by any practitioner: compute d from birthday, substitute into the equations, get an individual-specific growth curve. The improvement is greatest for foals at the extremes of the birth month distribution (January and June) because they deviate most from the March population average (d=81).

For WH, the effect is weaker than for BW. Authors and cited literature (Anderson and McIlwraith 2004; Kocher and Staniar 2013) suggest that withers height grows faster than BW early in life, partly completing its trajectory before the first CG period; hence CG exerts less influence on WH than on BW.

Practical management implication: a June-born foal that appears underweight for its chronological age (based on d=81 curve) may actually be appropriately growing for its developmental position in the CG cycle — using the standard curve would trigger unnecessary feeding interventions. Conversely, a January foal appearing on the standard curve may be above its individual expectation.

Race performance connection: Yamamoto et al. (cited) found that the season of growth stagnation is identical regardless of birth month — consistent with this paper's calendar-driven CG model. Pagan et al. (Kentucky Thoroughbreds) found growth rate affected by season in addition to age, and suggested birthday differences cannot be ignored for individual curves. Kocher and Staniar (2013, cited) confirmed calendar-season rather than birthday-season drives CG timing.

## Limitations

- Data from Hidaka region only (cold winter, pronounced CG); applicability to Thoroughbreds in Honshu training centres (Miho, Ritto) with milder winters may be reduced.
- The d parameter shifts CG period centres but does not modify CG amplitude — horses with the same d may differ in severity of growth slowdown based on nutrition, temperature, and individual variation.
- Dataset covers 1999–2009; if breeding season or farm management changed since, growth curves may shift.
- Withers height data (n=422) combined across sexes; sex-specific WH curves not provided separately, which may reduce precision.
- The population-average equations (Onoda et al. 2013) underlying this paper were fitted on the same dataset, so AIC improvement reflects within-sample fit; external validation on a held-out dataset not reported.

## Feature-engineering notes for the model

- `birth_month` — integer 1–6 (January–June) — source: birth records in JRA/JBBA data — expected effect: birth month proxies RBD; earlier months → first CG period encountered later in age, affecting body development at race debut — available in pedigree records
- `relative_birth_date_d` — d = day count from December 31 (Jan 1 = 1, Feb 1 = 32, Mar 1 = 60, etc.) — derivation: computed from birthday — expected effect: parameterises CG period timing; critical covariate for interpreting body weight at age ≤3 years
- `bw_residual_from_curve` — actual_bw (kg) − predicted_bw_from_birthday_adjusted_growth_curve(age_days, d) — derivation: use Equations 1/2 with horse's d and age_days; compute residual against measured 馬体重 — expected effect: negative residual (underweight for birthday-adjusted expectation) may indicate underconditioning, illness, or orthopaedic stress — available when 馬体重 is in race record (JRA records include body weight at race entry)
- `wh_residual_from_curve` — analogous for withers height if available — derivation: Equation 3 with horse's d — expected effect: significant mainly for extreme birth months (January, June); more robust for BW than WH — data availability: not routinely in race records; yearling sale data
- `age_at_debut_days` — exact age at first race in days — derivation: first_race_date − birth_date — expected effect: early debut (low age_at_debut_days) combined with late birth month (high d) indicates the horse was racing before its individual BMD/growth plateau was reached — captures orthopedic risk interaction
- `birth_month_x_age_group` — interaction: birth_month × (2yr / 3yr_early / 3yr_late) — derivation: combine birth_month with age-group feature — expected effect: June-born 2-year-olds are at different physiological position than January-born 2-year-olds; most predictively relevant for horses age ≤3

## Key references / follow-up leads

- Onoda T. et al. (2013) J. Anim. Sci. 91:5599–5604 — empirical growth curve with multiple seasonal CG for BW of Japanese Thoroughbred colts and fillies (population average; base equations for this paper)
- Onoda T. et al. (2013) J. Equine Sci. 24:63–69 — empirical percentile growth curves with Z-scores for Japanese Thoroughbreds
- Kocher A. and Staniar W.B. (2013) Livest. Sci. 154:204–214 — calendar season rather than birth date drives CG timing in Thoroughbreds
- Pagan J.D., Brown-Douglas C.G. and Caddel S. (2006) Advances in Equine Nutrition IV — growth rates and birthday effects in Kentucky Thoroughbreds
- Anderson T.M. and McIlwraith C.W. (2004) Equine Vet. J. 36:563–570 — longitudinal development of Thoroughbred conformation; WH grows faster than BW
- Mohammed H.O. (1990) Prev. Vet. Med. 10:63–71 — risk factors for osteochondrosis in horses; rapid/irregular growth associations
- 28_1726 (this corpus, if present) — GWAS for body weight in Japanese Thoroughbreds; genetic determinants of the same body weight phenotype
