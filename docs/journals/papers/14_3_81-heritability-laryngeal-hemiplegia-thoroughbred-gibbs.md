# Estimation of Heritability of Laryngeal Hemiplegia in the Thoroughbred Horse by Gibbs Sampling

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                              |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 14(3): 81–86, 2003                                                                                                                                                                                                                                                  |
| docid                          | `14_3_81`                                                                                                                                                                                                                                                                          |
| Article type                   | Original                                                                                                                                                                                                                                                                           |
| Authors                        | Takayuki IBI, Takeshi MIYAKE, Seiji HOBO, Hironori OKI, Nobushige ISHIDA, Yoshiyuki SASAKI                                                                                                                                                                                         |
| Affiliations                   | Division of Applied Bioscience, Graduate School of Agriculture, Kyoto University, Sakyo-ku, Kyoto 606-8502; Agura Farm, Sakitama 3-833, Kuroiso, Tochigi 325-0033; Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya City, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted July 18, 2003                                                                                                                                                                                                                                                             |
| Keywords                       | categorical trait, Bayesian analysis, Gibbs sampling, heritability, Laryngeal Hemiplegia                                                                                                                                                                                           |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/14/3/14_3_81/_pdf/-char/en                                                                                                                                                                                                                |

## Abstract (verbatim)

> Laryngeal Hemiplegia (LH) leads to a reduction in performance and because of it many promising racehorses have been forced to end their racing career. Therefore it is important for breeding good racehorses to estimate the heritability of LH. In this study, the computer program was developed based on the Bayesian analysis with Gibbs sampling for estimating the heritability of categorical traits assuming liability. A total of 706 records with LH-grade in Thoroughbreds aged 2 to 5 years were assigned for the genetic analysis. LH-grades consisted of five severity classes from 0 to 4. Racehorse breeders are often interested in whether the genetic effect controlling a complex disorder is present in the population. To answer this question, the binary trait analysis would be also useful. The heritability of LH-grade in the Thoroughbred horse was then also estimated as a binary or as a categorical trait. The mode values of the posterior distributions of heritability were 0.23 and 0.20 for the binary and the categorical trait, respectively. The fact that in the Thoroughbred population studied LH is at least partially controlled by genetic factors leads to the suggestion that when applying adequate breeding measures the prevalence of LH will be able to be reduced.

## Relevance to finishing-position (着順) prediction

Feature family: **B (respiratory / upper airway disorder) + D (genetics/pedigree)**. Laryngeal hemiplegia (LH, "roaring") is the most common upper respiratory tract disorder detected endoscopically in JRA racehorses (Hobo et al. 1995 reported prevalence data from JRA training centres). LH causes paralysis of the left arytenoid cartilage, obstructing the airway during inhalation and reducing aerobic capacity in affected horses — directly impairing race performance.

This paper establishes h² = 0.20–0.23 for LH in a JRA Thoroughbred population, confirming a partially genetic basis. The practical implications for a finishing-position model are:

1. **Pedigree-based risk feature**: Sire and dam LH history can be encoded as a pedigree risk feature. A sire with multiple affected offspring carries a liability breeding value above the population mean; this can be estimated from the JRA endoscopy database (706+ records). Feature: `sire_lh_prevalence_in_offspring` or `dam_lh_prevalence_in_offspring`.

2. **Sex interaction with LH**: Sex effect was significant (p<0.05) in LH-grade distribution. Male horses (specifically stallions and geldings) are more susceptible to LH. This justifies a `sex × lh_risk` interaction term; in practice, if endoscopic records are available, `horse_lh_grade` directly encodes airway obstruction severity.

3. **Performance degradation from respiratory obstruction**: Horses with LH grade ≥ 2 typically show reduced exercise capacity. The link between LH grade and racing performance is the key signal: if a horse's LH grade is recorded in veterinary examination data, it should be included as a performance-degradation feature. The h²=0.20 estimate implies ~20% of LH variance is genetic, with the remainder environmental/stochastic — meaning endoscopic findings in the horse itself (not just pedigree) are the primary data source.

4. **Career-end risk**: Many promising racehorses retire due to LH (per JRA 1996 report). A horse with progressive LH grade is a candidate for career-end encoding in survival analysis models.

The genetic component (h²=0.20–0.23) is moderate but non-trivial, indicating that selection against LH is feasible and that pedigree information adds information beyond chance. However, because h²<0.25, environmental and management factors (training intensity, stabling conditions affecting recurrent laryngeal neuropathy) also substantially contribute, and horse-level endoscopic data should take precedence over pedigree proxies when available.

## Background & objective

LH (roaring) in Thoroughbreds is caused by paralysis of the left arytenoid cartilage, producing a characteristic inspiratory wheeze. The obstruction reduces aerobic exercise capacity and forces premature retirement. LH was already suspected to have a genetic basis (family clustering, Cahill & Goulden 1987; preliminary genetic evidence, Poncet et al. 1989), but no rigorous heritability estimate with pedigree control had been published for the JRA population.

The statistical challenge is that LH-grade is a categorical (ordinal) trait — not a continuous measurement — which violates the normality assumption of standard variance component (REML) methods. The liability threshold model (Wright 1934), assuming an underlying continuous liability distribution with discrete thresholds, is the appropriate framework. Standard MAP estimation (Anderson & Aitkin 1985; Gianola & Foulley 1983) has bias problems in complex pedigrees. Bayesian Gibbs sampling (Sorensen et al. 1995; Hoeschele & Tier 1995) is theoretically less biased but had convergence problems for threshold parameters. This paper develops the GSTM program to address convergence and validates it via simulation before applying it to the JRA LH data.

## Materials & methods

**Genetic analysis data:**

- 706 records of LH-grade from Thoroughbred racehorses aged 2–5 years, recorded at the Racehorse Clinic at JRA's Ritto Training Center
- LH diagnosis: endoscopic examination using Hackett et al. (1991) 5-level classification (grades 0–4; grade 0 = unaffected, grade 4 = severely affected)
- Only stallions with ≥2 offspring with records used in the analysis
- If a horse had multiple lifetime diagnoses, only the most severe value retained
- Pedigree traced to great-grandparents

**Frequency distribution of LH-grade (Fig. 3 in paper):**
Binary trait (affected vs unaffected): approximately 30–40% affected (grade > 0) based on figure; categorical (0,1,2,3,4): distribution skewed toward grade 0, with grades 1 and 2 being next most common.

**Statistical model:**

_Categorical trait (5 classes)_:
L_ij = SEX_i + u_j + e_ij
Y_ij = 0 if L_ij ≤ t₁; = 1 if t₁ < L_ij ≤ t₂; = 2 if t₂ < L_ij ≤ t₃; = 3 if t₃ < L_ij ≤ t₄; = 4 if t₄ < L_ij

_Binary trait_:
L_ij = SEX_i + u_j + e_ij
Y_ij = 0 if L_ij ≤ t; = 1 if t < L_ij

Where: SEX_i = fixed sex effect (treated as covariate because only 2 levels); u_j = additive genetic breeding value (N(0, Aσ²_u)); e_ij = residual (N(0, Iσ²_e)); h² = σ²_u / (σ²_u + σ²_e)

**GSTM program:**

- Based on Sorensen et al. (1995) approach; extended to animal model (from sire model)
- Conditional posterior density functions from MAGGIC package (Janss 1998)
- Herd-year-season (HYS) treated as random effect (mean zero, does not distort liability distribution)
- Gibbs sampling: 10,000,000 total samples
- Burn-in and spacing determined by Raftery & Lewis (1996) convergence criterion
- Mode of posterior distribution used as point estimate; mean and median also reported

**Simulation validation:**

- 50 sires × 20 dams × 1 offspring = 2,050 animals; 3-class categorical trait
- True h² values 0.0, 0.1, 0.2, 0.3, 0.4, 0.5 (6 settings); 20 replicates each
- 210,000 Gibbs samples; first 10,000 burn-in; spacing every 100; 2,000 samples for posterior density

**Statistical significance test for heritability:** Mode is significant if density at mode is ≥20× density at zero (Janss et al. 1995 criterion; p<0.05 threshold).

**Preliminary analysis:** SAS CATMOD procedure confirmed sex effect significant (p<0.05) in LH-grade distribution before main Gibbs analysis.

## Results (detailed — reproduce ALL numbers)

**Simulation validation results (Table 1):**

| True h² | Estimated h² (mean ± SD of mode values, 20 replicates) |
| ------- | ------------------------------------------------------ |
| 0.0     | 0.02 ± 0.03                                            |
| 0.1     | 0.11 ± 0.06                                            |
| 0.2     | 0.18 ± 0.06                                            |
| 0.3     | 0.31 ± 0.07                                            |
| 0.4     | 0.41 ± 0.04                                            |
| 0.5     | 0.50 ± 0.06                                            |

All estimates unbiased (no systematic over- or under-estimation).

**Heritability estimates for LH-grade (Table 2):**

| Analysis          | Burn-in | Spacing | Samples used | Mean   | Mode   | Median |
| ----------------- | ------- | ------- | ------------ | ------ | ------ | ------ |
| Binary trait      | 4,496   | 17,960  | 557          | 0.3627 | 0.2326 | 0.3290 |
| Categorical trait | 2,979   | 23,863  | 419          | 0.3042 | 0.2040 | 0.2721 |

**Key point:** Mode values are 0.23 (binary) and 0.20 (categorical). These mode values are statistically significant:

- Binary trait: density at mode > 20× density at zero → p < 0.10
- Categorical trait: density at mode > 20× density at zero → p < 0.05

The posterior distributions are right-skewed (mean > mode), with a relatively flat region between h² = 0.2 and h² = 0.4 in the categorical analysis, suggesting the true heritability could be higher than the mode (possibly in the 0.2–0.4 range).

**Sex effect:** Significant (p<0.05) in preliminary SAS analysis; sex included as covariate in model. Sex-specific LH-grade distributions differ (males more susceptible than females; consistent with Yoshikawa et al. 2001 and 2002 findings on C-cell sex differences in TB racehorses).

## Discussion & interpretation

The h² estimates (mode 0.20–0.23, mean 0.30–0.36) confirm a moderate genetic component to LH in JRA Thoroughbreds. The flat posterior distribution between h²=0.2 and h²=0.4 cautions against treating the mode as a precise point estimate — the true h² could be up to 0.40, implying greater genetic control than the mode suggests.

The higher h² in binary vs categorical analysis is typical: when information is lost by collapsing to binary, the genetic variance relative to residual variance can appear inflated. The categorical analysis is methodologically more appropriate and the h²=0.20 estimate is the more conservative.

Practical breeding implications: selection programs based on endoscopic screening of breeding stallions and mares, combined with breeding value estimation, could reduce LH prevalence over generations. The paper recommends DNA sampling alongside endoscopy to eventually identify QTLs controlling LH.

The sex effect (males more susceptible) is consistent with the general finding that colts have higher injury/disorder rates than fillies in JRA racing, and may reflect either testosterone-related effects on recurrent laryngeal nerve pathology or body size differences.

## Limitations

- 706 records is moderate for quantitative genetic analysis with pedigree to great-grandparents; confidence intervals on h² are not reported but implied by the wide posterior distribution
- Only age 2–5 horses from one JRA training centre (Ritto); horses at Miho Training Center not included; selection bias possible (only horses reaching training centres are recorded)
- Endoscopic LH grading requires experienced examiner; inter-rater reliability of Hackett (1991) criteria acknowledged in the original Hackett paper as high but not perfect
- The liability threshold model assumes a normal liability distribution; if true liability is non-normal, estimates may be biased
- No racing performance data linked to LH grade; the performance consequence of each LH grade level is inferred from clinical knowledge, not measured in this study

## Feature-engineering notes for the model

- `horse_lh_grade` — Hackett (1991) endoscopic grade 0–4 at most recent examination — source: JRA veterinary examination records if available — grades 0–1 likely minimal performance impact; grade 2+ increasingly reduces aerobic capacity; encode as 0 (grade 0–1) vs 1 (grade 2+) or as ordinal score
- `horse_lh_affected` — binary flag (grade > 0) — simpler version of above; h²=0.23 justifies pedigree-based prediction when direct records unavailable
- `sire_lh_offspring_prevalence` — proportion of sire's offspring with recorded LH grade > 0 — source: JRA endoscopy registry or JBBA stallion records — proxy for sire LH breeding value; h²=0.20 implies ~20% of variance explained by additive genetics, so sire effects are real but modest
- `sex_lh_interaction` — sex × horse_lh_affected — males have higher susceptibility (p<0.05 sex effect); interaction captures differential impact of LH across sexes
- `sire_upper_airway_disorder_rate` — broader category combining LH + other upper airway disorders (soft palate dorsal displacement, epiglottic entrapment) if sire records available — wider signal than LH alone
- **Caution:** The h²=0.20 implies 80% of LH variance is non-genetic. Use pedigree-based LH features only as secondary signals, never as primary predictors; individual endoscopic findings take precedence. LH features should interact with distance (longer distances → more aerobic demand → greater performance penalty from airway obstruction).

## Key references / follow-up leads

- Hobo, S., Matsuda, Y., and Yoshida, K. 1995. Prevalence of upper respiratory tract disorders detected with a flexible videoendoscope in thoroughbred racehorses. _J. Vet. Med. Sci._ 57: 409–413. (JRA prevalence data by endoscopy; companion population description)
- Cahill, J.I. and Goulden, B.E. 1987. The pathogenesis of equine laryngeal hemiplegia — a review. _N. Z. Vet. J._ 35: 82–90. (pathogenesis review; recurrent laryngeal neuropathy mechanism)
- Poncet, P.A., Montavon, S., Gaillard, C., Barrelet, F., Straub, R., and Gerber, H. 1989. A preliminary report on the possible genetic basis of laryngeal hemiplegia. _Equine Vet. J._ 21: 137–138. (first genetic basis evidence)
- Hackett, R.P., Ducharme, N.G., and Fubini, S.L. 1991. The reliability of endoscopic examination in assessment of arytenoids cartilage movement in horses. _Vet. Surg._ 20: 174–179. (grading system used in this study)
- Sorensen, D.A., Anderson, S., Gianola, D., and Korsgaard, I. 1995. Bayesian inference in threshold models using Gibbs sampling. _Genet. Sel. Evol._ 27: 229–249. (core statistical method underpinning GSTM)
- JRA. 1996. Upper respiratory tract disorders of racing horses. The Racehorse Clinic at JRA's Ritto Training Center. (primary JRA epidemiological data on LH prevalence and career impact)
