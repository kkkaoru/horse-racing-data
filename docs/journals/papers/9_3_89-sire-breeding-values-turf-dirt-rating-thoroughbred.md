# Relationship between Sire Breeding Values for the Rating Score on Turf and Dirt Racing Tracks in Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 9(3): 89–92, 1998                                                                                                    |
| docid                          | `9_3_89`                                                                                                                            |
| Article type                   | Original                                                                                                                            |
| Authors                        | Yasuyoshi Moritsu, Akiko Terai, Takumi Tashiro                                                                                      |
| Affiliations                   | Laboratory of Animal Breeding, Department of Dairy Science, Hokkaido College of Arts and Sciences, Ebetsu, Hokkaido 069-8501, Japan |
| Received / Accepted / Released | Submitted April 27, 1998; accepted August 6, 1998                                                                                   |
| Keywords                       | sire breeding value, sire model BLUP, turf racing tracks, dirt racing tracks, Thoroughbred racehorses                               |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/9/3/9_3_89/_pdf/-char/en                                                                   |

## Abstract (verbatim)

> This study was conducted to determine whether there is a relationship between the rankings of sires' breeding values, evaluated based on progeny rating scores, for turf and dirt racing tracks in Thoroughbred racehorses in Japan. Data for analysis were taken from the records of rating scores appearing in the Rating Magazine in May, 1997. Sires with at least 10 progeny having both turf and dirt rating scores were studied along with their progeny. The study populations were 116 sires and 2,754 progeny for turf racing tracks, and 116 sires and 2,797 progeny for dirt racing tracks. Sires' breeding values (EPD) were estimated with the BLUP sire model from progeny rating scores for either turf or dirt racing tracks. The fixed effects included in the BLUP sire model were sex, age of progeny and training center, whereas the sire and residuals were treated as random effects. The effect of the sire on rating scores was highly significant (P<0.001) for both kinds of racing tracks. Estimates of heritability for rating scores were 0.29 ± 0.04 at turf racing tracks and 0.18 ± 0.02 at dirt racing tracks. The correlation of a sire's EPD estimated for each kind of racing track clearly showed a positive trend, and the Spearman rank correlation was 0.502 (P<0.001). Based on this result, we conclude that most Thoroughbred stallions produce progeny suited to both turf and dirt racing tracks.

## Relevance to finishing-position (着順) prediction

Feature family **D (genetics / pedigree)**. This is a directly relevant quantitative genetics paper using BLUP sire-model analysis on JRA Thoroughbred rating score data. It provides two key empirical quantities for the pipeline:

1. **Heritability of rating score:** h² = 0.29 ± 0.04 on turf, 0.18 ± 0.02 on dirt. These are the fractions of rating score variance attributable to additive genetic (sire) effects in the JRA population. They justify encoding sire-based genetic features in a finishing-position model — roughly one-quarter to one-third of performance variance is genetically transmissible through the sire.

2. **Turf–dirt sire-EPD correlation (Spearman r = 0.502, P < 0.001):** Most sires are general-purpose (positive correlation), but ~50% of inter-sire rank variance is track-type specific. This justifies a sire × surface interaction feature: same sire's EPD predicts different relative performance on turf vs. dirt.

The lower heritability on dirt (0.18 vs. 0.29) likely reflects larger within-track environmental variance on JRA dirt courses (going variability, weather effects), diluting the genetic signal. A separate sire-effect feature indexed to surface type should be included, with lower predictive confidence for dirt races.

The fixed effects found to be highly significant (sex, age, training center) confirm that these standard JRA feature variables must be included to isolate the genetic sire component. The distance-aptitude confounding of the two outlier sires (medium-distance turf specialist vs. short-distance dirt specialist) provides a mechanistic explanation for the imperfect turf–dirt correlation and motivates a sire × distance interaction feature.

## Background & objective

In Japan, Thoroughbreds compete on both turf and dirt, unlike Europe (predominantly turf) or USA (predominantly dirt). The general belief among Japanese racing stakeholders is that individual horses are suited to one surface or the other, and that the sire contributes to this surface preference. Most JRA stallions are imported from Europe or USA, where they typically raced exclusively on one surface. The study asked whether a JRA stallion's EPD for turf performance is predictive of his EPD for dirt performance — testing whether surface-aptitude genetics are shared or independent. No prior study had estimated surface-specific heritabilities or EPD correlations in JRA.

## Materials & methods

**Data source:** Rating scores from _Rating Magazine_ (Geibunshiya Inc., Tokyo), May 1997 issue [reference 3]. Rating scores are published merit scores for racehorses registered with JRA, expressed as a dimensionless racing merit index. First published systematically from 1995.

**Population:**
| Track type | Number of sires | Number of progeny (female) | Number of progeny (male) | Total progeny |
|------------|----------------|--------------------------|--------------------------|--------------|
| Turf | 116 | 996 | 1,758 | 2,754 |
| Dirt | 116 | 1,071 | 1,726 | 2,797 |

Restriction: sires with ≥ 10 progeny having rating scores on **both** track types.

**Rating score summary statistics:**

| Track | Min  | Max  | Mean | SD  | CV (%) |
| ----- | ---- | ---- | ---- | --- | ------ |
| Turf  | 48.0 | 97.0 | 61.8 | 9.3 | 15.0   |
| Dirt  | 48.0 | 89.0 | 60.5 | 7.4 | 12.2   |

**Statistical model:** Linear mixed model (BLUP sire model):

> Yijklm = µ + Si + Aj + Bk + Cl + eijklm
>
> where Si = random effect of ith sire (i = 1,...,116), N(0, σ²s); Aj = fixed effect of jth progeny sex (j = 1, 2); Bk = fixed effect of kth progeny age group (k = 1 = 4-year-old, 2 = 5-year-old and older); Cl = fixed effect of lth training center (l = 1 = Miho, 2 = Ritto); eijklm = random residual, N(0, σ²e).

Interactions between fixed effects tested and found non-significant; omitted from final model.

**Procedures:** GLM (least-squares ANOVA) using SAS for fixed effects; VARCOMP with REML for variance components (sire and residual); MIXED procedure for BLUP sire breeding values. Heritability = 4 × intraclass correlation of paternal half-sibs [Swiger et al. 1964 formula for SE]. EPD = one-half of sire breeding value.

**Rank correlation:** Spearman rank correlation coefficient between turf EPD and dirt EPD, computed for all 116 sires.

## Results (detailed — reproduce ALL numbers)

**Analysis of variance results:** All four effects — sire, sex of progeny, age of progeny, training center — were **highly significant (P < 0.001)** for rating scores at **both** track types. The sire effect was described as "highly significant (P < 0.001)" and "above all" the most notable.

**Heritability estimates:**

| Track type | h² estimate | SE     |
| ---------- | ----------- | ------ |
| Turf       | 0.29        | ± 0.04 |
| Dirt       | 0.18        | ± 0.02 |

Both exceed prior Japanese estimates for racing time (Moritsu et al. 1994; Oki et al. 1995), which were slightly lower. Turf h² is similar to but lower than Gaffney & Cunningham (1988) Timeform rating h² = 0.50 ± 0.036 in Great Britain.

**Turf–dirt EPD correlation:**

- Spearman rank correlation coefficient: r = **0.502**, P < **0.001**.
- Scatterplot (Fig. 1) shows a clear positive trend overall, with two sires notably off the diagonal.
- Sire A: medium turf EPD, **low** dirt EPD — identified in the Thoroughbred stallion dictionary as having progeny suited to medium racing distances (turf range 1,000–3,600 m; dirt range 1,000–2,300 m).
- Sire B: average turf EPD, **high** dirt EPD — identified as having progeny suited to **short** racing distances.

Authors interpret the outlier pattern as distance-aptitude confounding: the shorter maximum dirt distance (2,300 m vs. 3,600 m for turf) means sprinters with high dirt EPD may appear surface-specialist when they are actually distance-specialist.

**Genetic correlations for racing distance (Oki et al. 1997 cited):**

- Genetic correlations in racing times between different distances: range 0.68–1.00 for turf; 0.53–1.00 for dirt.
- Lower floor on dirt (0.53) supports greater distance-specialisation on dirt.

## Discussion & interpretation

The moderately positive Spearman rank correlation (r = 0.502) between turf and dirt EPDs indicates that the majority of JRA Thoroughbred stallions produce progeny with generally consistent ability across track types. The authors conclude "most Thoroughbred stallions produce progeny suited to both turf and dirt racing tracks." The two outlier sires likely reflect distance-aptitude differences being conflated with surface-aptitude differences — JRA dirt distances are systematically shorter than turf distances, so a sprint-specialist sire appears dirt-specialist.

The lower heritability on dirt (0.18) vs. turf (0.29) may reflect greater measurement error or environmental variance on dirt: dirt course going varies more with weather and maintenance, introducing non-genetic noise. The turf h² = 0.29 is higher than racing time heritabilities in Japan (Moritsu et al. 1994; Oki et al. 1995) but lower than UK Timeform rating (h² = 0.50), possibly because the JRA rating was only recently introduced (since 1995) and may have lower reliability than the long-established Timeform system.

## Limitations

- Rating scores available only from 1995; limited number of generations and pedigree depth at time of analysis.
- Sire model only (not animal model); ignores dam contribution; h² estimates may be upward-biased if sires are genetically related (assumed unrelated in this model).
- EPD is "expected progeny difference" = half sire breeding value; does not account for dominance or epistatic effects.
- Distance-aptitude confounding is acknowledged but not formally controlled; separate analyses by distance class were not performed.
- Rating score as a performance criterion: not equivalent to finishing position directly; represents a merit index integrating multiple race results over a period.
- Only 116 sires with ≥ 10 progeny on both surfaces; excludes rare or recently imported sires.

## Feature-engineering notes for the model

- `sire_epd_turf` — sire's BLUP EPD for turf rating score — derivable from Rating Magazine or JRA genetic evaluation databases — expected positive effect on progeny turf finishing position; h² = 0.29 means substantial sire signal
- `sire_epd_dirt` — sire's BLUP EPD for dirt rating score — as above — expected positive effect on progeny dirt finishing position; h² = 0.18, lower precision than turf EPD
- `sire_surface_differential` — sire_epd_turf − sire_epd_dirt — derived feature — positive = turf specialist sire; negative = dirt specialist; r ≈ 0.50 between turf and dirt EPDs, so differential captures the ~50% variance that is surface-specific
- `sire_distance_aptitude` — sprint/classic/stayer category from stallion records — Thoroughbred stallion dictionary data — confounds with surface EPD; must be included as covariate to correctly isolate surface effect (see outlier Sires A and B)
- `progeny_sex` — male/female binary — race entry records — significant effect P < 0.001 on rating score at both surfaces; must be included as fixed effect
- `progeny_age_group` — 4-year-old vs. 5-year-old-and-older binary — race entry records — significant effect P < 0.001; use as fixed effect
- `training_center` — Miho vs. Ritto — race entry records — significant effect P < 0.001; must be included as fixed effect to isolate genetic sire signal from training environment
- **Interaction:** `sire_epd_dirt` × `surface` — sire genetic contribution differs by surface; separate sire EPD features per surface recommended over a single cross-surface EPD
- **Interaction:** `sire_epd` × `distance_category` — the Sire B (short-distance, high dirt EPD) outlier warns against using dirt EPD without conditioning on distance; at medium distances, sire EPDs should correlate more closely between surfaces

## Key references / follow-up leads

- Gaffney, B. and Cunningham, E.P. 1988. Estimation of genetic trend in racing performance of thoroughbred horses. _Nature_ 332: 722–723.
- Moritsu, Y., Funakoshi, H. and Ichikawa, S. 1994. Genetic evaluation of sires and environmental factors influencing best racing times of Thoroughbred horses in Japan. _J. Equine Sci._ 5: 53–58.
- Oki, H., Sasaki, Y. and Willham, R.L. 1995. Genetic parameter estimates for racing time by restricted maximum likelihood in the Thoroughbred horse of Japan. _J. Anim. Breed. Genet._ 112: 146–150.
- Oki, H., Sasaki, Y. and Willham, R.L. 1997. Estimation of genetic correlations between racing times recorded at different racing distances by restricted maximum likelihood in Thoroughbred racehorses. _J. Anim. Breed. Genet._ 114: 185–189.
- Henderson, C.R. 1973. Sire evaluation and genetic trend. _Proc. Anim. Breeding and Genetic Symposium in Honor of Dr. J.L. Lush_, pp. 10–41. ASAS and ADSA, Champaign, IL.
- Swiger, L.A., Harvey, W.R., Everson, D.O. and Gregory, K.E. 1964. The variance of intraclass correlation involving groups with one observation. _Biometrics_ 20: 818–826.
