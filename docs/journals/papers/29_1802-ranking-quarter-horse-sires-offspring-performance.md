# Ranking quarter horse sires via models of offspring performance

## Metadata

| Field                          | Value                                                               |
| ------------------------------ | ------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 29(3): 67–74, 2018                                   |
| docid                          | `29_1802`                                                           |
| Article type                   | Full Paper                                                          |
| Authors                        | Daniel T. Kasper, Rex F. Gandy                                      |
| Affiliations                   | Austin Peay State University, Tennessee 37044, U.S.A.               |
| Received / Accepted / Released | Received January 19, 2018; Accepted July 13, 2018                   |
| Keywords                       | horse, mixed models, racing performance, sire                       |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/29/3/29_1802/_pdf/-char/en |

## Abstract (verbatim)

> The 2016 Equibase data set of American Quarter Horse starts in North America was analyzed, with the purpose of ranking the sires of the racehorses. A speed z-score derived from the race times and distances was used as a racing performance measure. Mixed effects models were used on various subsets of the data based on race distance and sire offspring number. The sire categorical variable was considered as a random effect. Various statistical criteria were used to optimize the model. The constructed models were then varied in terms of the random and fixed effects included, and the conditional modes of the sire effects were extracted from these models. The benefit of the sire ranking that comes from this analysis is that it is controlled for track, jockey, trainer, weather, and several other variables that can impact speed. Sires are typically valued for high rankings for offspring earnings and winners. Yet a sire with a low stud fee may still produce offspring with a high ranking using our z-score model. The offspring of this bargain sire have the potential to produce fast offspring that could pay a dividend on a relatively low cost investment. The model sire ranking approach described in this paper is clearly bringing a new approach to the field of sire rankings.

## Relevance to finishing-position (着順) prediction

This paper falls squarely in feature family **G (statistical modelling)**. It builds linear mixed-effects models on 56,999 Quarter Horse race starts and decomposes performance variance into horse-individual, track, sire, trainer, jockey, and residual components. The variance table (Table 3) provides an empirical benchmark for the relative importance of these effects, directly informing which random-effect priors are worth including in a JRA finishing-position model.

The finding that horse-individual variance (0.305) dwarfs sire variance (0.083) supports horse-level embeddings over pure pedigree lookup in the prediction pipeline. For cold-start horses (few career starts), the sire BLUP provides a shrinkage prior. The jockey variance (0.022) is relatively small, consistent with keeping jockey as a lightweight feature rather than a high-cardinality random effect in JRA models.

The AIC-selected model retained weight carried as both linear and quadratic terms, confirming that futan/斤量 should be included non-linearly (e.g., as both `weight` and `weight²`, or via a spline) in any JRA regression. Track variance (0.157) — larger than sire — echoes JRA's course-level idiosyncrasy (turf vs. dirt, clockwise vs. counter-clockwise, track condition) and argues for explicit track random effects or rich course-feature encoding.

The speed z-score construction (distance ÷ time, z-scaled per distance group) is a clean continuous target variable suitable for multi-distance pooling in a JRA meta-model, analogous to using standardized speed figures.

## Background & objective

Racing performance of a horse is partly genetic, so the offspring of a sire gives indirect information about that sire's breeding value. However, comparing sires by raw earnings or win counts confounds sire genetics with the quality of tracks, jockeys, trainers, and environmental conditions. The research gap: no mixed-model approach for ranking American Quarter Horse sires that simultaneously controls for all major confounders. Hypothesis: a linear mixed-effects model treating sire as a random effect and controlling for track, jockey, trainer, weather, temperature, track condition, and weight will produce more accurate sire rankings than traditional ranking methods.

## Materials & methods

**Data:** 2016 Equibase proprietary dataset of American Quarter Horse race starts in the United States and Canada. Total raw starts: 56,999. After removing non-finite or extremely slow times (>4 SD below mean per distance): 56,181 race outcomes. Unique horses: 13,367; unique sires: 1,245; unique dams: 8,804; unique jockeys: 665; unique trainers: 1,593; unique tracks: 72.

**Response variable construction:** Mean speed per race = distance ÷ time. Then z-scale within each distance group: subtract mean, divide by SD, remove observations >4 SD below mean, re-scale again. Final speed z-scores pooled across distances.

**Data subsets:** 9 subsets formed by crossing 3 distance criteria (350 yards only, 300 yards only, all distances with ≥100 outcomes per distance) × 3 sire-subsetting criteria (all sires, sires with ≥3 offspring and ≥10 race outcomes, sires with ≥10 offspring). The 350-yard subset had 14,127 starts; 300-yard had 13,143.

**Variables constructed:** Age in estimated days (birthdate set to July 2 of birth year); month of race; crosswind, headwind, and tailwind speed variables (km/hr); parallel wind (signed); temperature in °C; track condition (categorical). All numeric variables z-scaled except wind (scaled by SD only, not re-centered, to preserve zero = no wind).

**Model forms (R lme4):** Three final model forms were selected:

- AIC-selected ("014"): `speed.z ~ 1 + (1|Dam) + (1|Sire) + (1|Horse) + (1|TRAINER) + (1|JOCKEY) + (1|Track.ID) + age.est.days.scaled + I(age.est.days.scaled^2) + sex.category + sex.category:age.est.days.scaled + sex.category:I(age.est.days.scaled^2) + headwind.scaled + tailwind.scaled + crosswind.scaled + weather + temperature.celsius.scaled + track.condition + weight.carried.scaled + I(weight.carried.scaled^2)`
- BIC-selected ("002.07.10"): same random effects; fixed effects drop sex×age interactions and temperature²; drops weather
- "Simple": same random effects; only age², sex, and wind variables as fixed effects

**Random-effects variants:** For each of 3 model forms × 9 data subsets, 5 versions removing dam, jockey, and/or trainer were fit (REML). Additionally, 2 ML versions for AIC and BIC model forms applied to all-distance data. Total: 17 ranking approaches applied across 9 datasets.

**Sire ranking:** `ranef()` extracts conditional modes of sire random effects. Sires ranked within each model. Median and mean ranks reported across models.

**Model comparison alternatives:** Sire total offspring earnings, average offspring earnings, average z-score, and number of winners were computed as four traditional benchmarks.

**Goodness-of-fit:** R²-like metric = R² of simple OLS regressing actual on fitted values (with random effects included in fitted). Range: 0.600–0.792 across models.

**Software:** R, lme4, ggplot2, openxlsx, installr.

## Results (detailed — reproduce ALL numbers)

**Random-effect variance components** (from "mixedmod.350yards.no.sire.subsetting.014.reml.nodam", the model with highest R²-like value):

| Groups   | Variance | Standard deviation |
| -------- | -------- | ------------------ |
| Horse    | 0.305    | 0.552              |
| Track ID | 0.157    | 0.396              |
| Sire     | 0.083    | 0.288              |
| Trainer  | 0.064    | 0.252              |
| Jockey   | 0.022    | 0.148              |
| Residual | 0.362    | 0.602              |

**Top sire rankings** — Table 1 (top 30 sires by 350-yard median model rank, showing 350y / 300y / all-distance median and mean model ranks alongside traditional ranking):

| Sire                 | 350y median | 350y mean | All-dist median | Total earnings rank | Avg earnings rank | Avg z-score rank | # winners rank |
| -------------------- | ----------- | --------- | --------------- | ------------------- | ----------------- | ---------------- | -------------- |
| Apollitical Jess     | 1           | 1.0       | 2               | 2                   | 2                 | 9                | 4              |
| Fantastic Corona Jr  | 2           | 2.0       | 1               | 42                  | 20                | 2                | 47             |
| One Sweet Jess       | 3           | 3.0       | 13              | 11                  | 22                | 67               | 5              |
| Mr Jess Perry        | 4           | 4.7       | 19              | 6                   | 13                | 26               | 13             |
| Corona Cartel        | 5           | 5.1       | 6               | 5                   | 9                 | 11               | 11             |
| Carters Cartel       | 6           | 6.1       | 8               | 15                  | 43                | 13               | 7              |
| Pyc Paint Your Wagon | 10          | 10.5      | 11              | 4                   | 30                | 12               | 1              |
| Walk Thru Fire       | 10          | 9.8       | 8               | 3                   | 14                | 7                | 3              |
| Ivory James          | 16          | 15.5      | 16              | 9                   | 49                | 14               | 2              |
| One Famous Eagle     | 21          | 21.8      | 12              | 1                   | 5                 | 36               | 6              |

Notable divergences cited in paper: One Sweet Jess model rank 13 vs. average z-score rank 67; Kiddy Up model rank 3 vs. number of winners rank 61; Dominyun model rank 4 vs. average earnings rank 75; Mighty B Valiant model rank 7 vs. total earnings rank 105.

**Table 2 — effect of sire subsetting on 350-yard rankings:** Three subsetting criteria (≥10 offspring, all offspring, ≥3 offspring + ≥10 race outcomes) produced very similar rankings (e.g., Apollitical Jess: 1st in all three; Mr Jess Perry: 4th, 7th, 5th). Small sire subsets (all offspring) differ slightly for minor sires with few offspring.

**R²-like values:** 0.600–0.792 across all fitted models. Highest value in the 350-yard no-dam model "014".

**Track variance largest among environmental confounders:** Track (0.157) > Sire (0.083), confirming track is the dominant non-horse systematic effect on race speed.

**Jockey removal:** No significant ranking change when jockey term removed, but track was indispensable. Removing track dramatically changed rankings and model fit.

**Model diagnostics:** Residuals slightly left-skewed (violation of normality assumption); horse and other random-effect estimates followed approximately normal distributions. Residual patterns on individual fixed-effect predictors showed no obvious systematic issues.

## Discussion & interpretation

The authors argue that traditional sire rankings by earnings conflate breeding value with extrinsic advantages (better tracks, stronger trainers, top jockeys). Their mixed-model approach controls for these confounders, and the large discrepancies between model rankings and traditional rankings (e.g., One Sweet Jess: model 13 vs. average z-score 67; Mighty B Valiant: model 7 vs. total earnings 105) confirm that the adjustment matters.

The large horse random-effect variance (0.305) relative to sire (0.083) means that a single horse's performance record already substantially outweighs its sire's contribution as a predictive feature. However, for first-start horses with no performance history, the sire term provides a useful Bayesian shrinkage prior.

The subsetting analysis shows that stable rankings emerge once a sire has ≥10 offspring, suggesting minimum sample requirements for reliable sire-level estimates.

The authors discuss structural data issues: if tracks form disconnected clusters with respect to horses/trainers/jockeys, mixed-model estimation may be compromised (analogous to non-identifiability in disconnected crossed random effects in animal models).

## Limitations

- Data limited to a single year (2016), which limits assessment of sire consistency over time.
- Quarter Horses race at short distances (300–350 yards) unlike Thoroughbreds; generalizability to longer-distance JRA races requires caution.
- Left-skewed residuals indicate mild violation of normality assumptions.
- Dam could not be reliably included due to low offspring-per-dam (many dams had only one offspring in 2016).
- No validation on held-out data; goodness-of-fit computed on training data.
- Structural crossing issues (track, horse, jockey partitioning) may affect estimates.

## Feature-engineering notes for the model

- `horse_individual_blup` — per-horse random-effect conditional mode from a mixed model trained on historical race z-scores — source: JRA race records — expected effect: largest single predictor (variance 0.305 in QH); JRA equivalent likely similar — include for all horses with ≥2 starts; for debuts use sire prior
- `sire_blup` — sire random-effect conditional mode — source: JBBA pedigree + JRA starts — expected effect: variance ~0.083 in QH; moderate positive for fast sire lines — use as debut prior; discount heavily once horse has ≥5 starts
- `track_effect` — track-level random effect or fixed track encoding — source: track/course ID in JRA data — expected effect: variance 0.157 in QH; JRA courses differ materially by surface/layout/footing — must include; recommend learnable embedding per course
- `weight_carried` — futan/斤量 linear term — source: JRA race entry — expected effect: positive up to optimum, negative beyond — include as linear + quadratic or spline
- `weight_carried_sq` — square of futan — source: derived from weight_carried — expected effect: AIC selection confirmed quadratic needed — pair with linear term; do NOT use alone
- `trainer_blup` — trainer random effect — source: JRA records — expected effect: variance 0.064 in QH; moderate — useful but smaller than track/horse
- `jockey_effect` — jockey random effect — source: JRA records — expected effect: variance only 0.022 in QH; weak — low priority; may not warrant high-cardinality encoding
- `age_days` — horse age in days at race date — source: birthday in JRA data — expected effect: quadratic in speed; peak at ~3–5 yr for Thoroughbreds — include as linear + quadratic; interact with sex
- `sex_age_interaction` — sex × age polynomial — source: derived — expected effect: sex differences in age trajectory confirmed by AIC selection — include sex × age.sq interaction for fillies/geldings vs. colts
- `headwind_speed`, `tailwind_speed`, `crosswind_speed` — km/hr — source: meteorological data (not routinely in JRA public records) — expected effect: tailwind positive on speed; crosswind slight negative — include if available
- `track_condition_encoded` — going/track surface condition — source: JRA race records — expected effect: firm/standard vs. heavy has large effect on speed — include as categorical
- `speed_zscore_target` — distance ÷ time, z-scaled per distance group — source: derived from race time and distance — use as continuous training target for auxiliary regression tasks; note: JRA finishing position is ordinal not continuous, but speed z-score can serve as auxiliary label for representation learning
- **CAUTION:** sire average z-score and total earnings rankings should NOT be used as sire features — they conflate genetics with extrinsic factors; use model-based BLUP instead

## Key references / follow-up leads

- Buttram ST. 1987. Genetics of Racing Performance in the American Quarter Horse. J. Anim. Sci. 66: 2800–2807.
- Villela LCV, Mota MDS, Oliveira HN. 2002. Genetic parameters of racing performance traits of Quarter horses in Brazil. J. Anim. Breed. Genet. 119: 229–234.
- Corrêa MJM, da Mota MDS. 2007. Genetic evaluation of performance traits in Brazilian Quarter Horse. J. Appl. Genet. 48: 145–151.
- Bates D, Maechler M, Bolker B, Walker S. 2015. Fitting linear mixed-effects models using lme4. J. Stat. Softw. 67: 1–48.
- Faraway JJ. 2016. Extending the Linear Model with R: Generalized Linear, Mixed Effects and Nonparametric Regression Models. 2nd ed. CRC Press.
