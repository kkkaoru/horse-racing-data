# In Vitro Measurement of the Bone Mineral Density of the Third Metacarpal Bone by Dual Energy X-Ray Absorptiometry in Racehorses — Comparison with Single Photon Absorptiometry

## Metadata

| Field                          | Value                                                                                                                                                                                                     |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 7(4): 93–96, 1996                                                                                                                                                                          |
| docid                          | `7_4_93`                                                                                                                                                                                                  |
| Article type                   | Note                                                                                                                                                                                                      |
| Authors                        | Masa-aki Oikawa, Katsuhisa Shimazu                                                                                                                                                                        |
| Affiliations                   | Oikawa: Equine Research Institute, Japan Racing Association, 27-7 Tsurumaki 5-chome, Setagaya-ku, Tokyo 154, Japan; Shimazu: Nippon Veterinary and Animal Science University, Musashino, Tokyo 180, Japan |
| Received / Accepted / Released | Submitted April 26, 1996; accepted September 25, 1996                                                                                                                                                     |
| Keywords                       | DEXA, bone densitometry, bone mineral density, racehorses, third metacarpal bone                                                                                                                          |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/7/4/7_4_93/_pdf/-char/en                                                                                                                                         |

## Abstract (verbatim)

> Bone mineral density (BMD, g/cm²) in the distal portion of the third metacarpal bone (McIII) of racehorses was measured by a dual energy x-ray absorptiometry system (DEXA; DCS-3000, ALOKA Co, Ltd., Tokyo, Japan) in vitro, and compared with bone mineral content (BMC, g/cm²) evaluated by a single photon absorptiometry system (SPA: Model 278 A, Norland Co., Fort Atkinson, U.S.A.; radioactive source was ²⁴¹Am-45mCi). The BMD values measured by DEXA were positively correlated with the BMC values measured by SPA, which has been used to quantitatively assess BMC with high accuracy and reproducibility in racehorses, at 0.01–0.0001 level with a correlative coefficient (r) of 0.625–0.995 (n=30). The reproducibility of values measured by DEXA shows a satisfactory coefficient value (3%), almost same as that (2.7%) of SPA. These results suggest the potential applications of DEXA as a quantifying instrument for the measurement of bone mass of the McIII in racehorses.

## Relevance to finishing-position (着順) prediction

Feature family **A (injury/soundness)**. Longitudinal fractures of the distal McIII are the most common catastrophic injury in Thoroughbred racehorses, and they arise from subchondral cancellous bone sclerosis. This paper establishes that McIII BMD at the fracture-prone site (1 cm proximal to distal articular surface) correlates significantly with number of races run (r=0.785, p<0.05) and length of racing period (r=0.882, p<0.01), but not with age (r=0.247, NS) or body weight (r=0.633, NS). The clear finding that race count dominates over age in explaining BMD accumulation directly supports using `career_starts` as the primary bone-stress proxy — stronger than age alone — in any finishing-position pipeline that includes non-finish / catastrophic injury prediction.

The practical implication for the model: `career_starts` and `career_days` (racing period length) are accessible surrogates for the underlying pathophysiology of McIII sclerosis. The r=0.882 correlation for racing period is particularly strong; `career_days` should be included as a standalone feature. This complements the soft-tissue finding in companion paper 7_3_51 (same JRA group), where the same race-count axes drive degenerative lesion incidence.

## Background & objective

Longitudinal fractures of the distal McIII are suggested to arise from subchondral cancellous bone sclerosis (osteosclerosis), which in turn reflects accumulating bone mineral content. SPA (²⁴¹Am single photon absorptiometry) has been validated for equine McIII BMC measurement (Tomioka et al. 1985). DEXA uses dual-energy X-rays and does not require the limb to be immersed in tissue-equivalent medium; it provides spatial images that allow fixed region-of-interest placement. The study objective was to validate DEXA against the existing SPA gold standard, and to correlate McIII BMD with biological and management variables.

## Materials & methods

**Subjects:** 30 Thoroughbreds at autopsy, aged 28–67 months (2.3–5.6 years), body weight 428–500 kg; sex composition: 16 males, 13 females, 1 gelding. Left McIII forelimbs collected post-mortem.

**DEXA instrument:** DCS-3000, ALOKA Co. Ltd., Tokyo, Japan. Dual tightly-collimated X-ray beams alternating at 50 and 100 kVp; 90 CdTe semiconductor devices in array detector. BMD (g/cm²) = bone mineral content ÷ area (cm²). Region of interest: 1 × 2 cm.

**SPA instrument:** Model 278A, Norland Co., Fort Atkinson, USA. Source: ²⁴¹Am, 45 mCi at 60 keV. BMC (g/cm²) = bone mineral content ÷ bone width.

**Measurement sites** (at 1 cm proximal to distal articular surface of McIII):

- Site A (DEXA): medial region, dorsopalmar direction
- Site B (DEXA): lateral region, dorsopalmar direction
- Site C (DEXA): mediolateral direction — primary site for fracture zone
- Site D (SPA): dorsopalmar direction
- Site E (SPA): mediolateral direction

**Reproducibility:** BMD at site C measured 3 times on 15 of 30 bones; coefficient of variation (CV) calculated.

**Correlation analyses:** Pearson r between BMD (site C) and body weight, number of races run, period on races, and age. Statistical analysis: ANOVA-multiple method.

## Results (detailed — reproduce ALL numbers)

### Table 1: BMD and BMC values of McIII in 30 Thoroughbreds (mean ± SD)

| Site | Measurement                     | Value (g/cm²)     |
| ---- | ------------------------------- | ----------------- |
| A    | BMD (DEXA, medial dorsopalmar)  | 1.791 ± 0.185     |
| B    | BMD (DEXA, lateral dorsopalmar) | 1.798 ± 0.267     |
| C    | BMD (DEXA, mediolateral)        | **3.514 ± 0.386** |
| D    | BMC (SPA, dorsopalmar)          | 2.426 ± 0.300     |
| E    | BMC (SPA, mediolateral)         | 3.637 ± 0.443     |

Site C (mediolateral DEXA) is approximately twice as high as sites A and B (dorsopalmar DEXA), reflecting the greater bone mineral accumulation in the mediolateral plane at this fracture-prone site.

### Table 2: Correlations between BMD (DEXA) and BMC (SPA) across measurement sites (n=30)

| Site pair  | r         | Significance |
| ---------- | --------- | ------------ |
| A vs B     | 0.772     | P<0.001      |
| A vs C     | 0.723     | P<0.001      |
| A vs D     | 0.625     | P<0.01       |
| A vs E     | 0.686     | P<0.01       |
| B vs C     | 0.764     | P<0.001      |
| B vs D     | 0.772     | P<0.001      |
| B vs E     | 0.790     | P<0.001      |
| C vs D     | 0.908     | P<0.001      |
| **C vs E** | **0.955** | **P<0.001**  |
| D vs E     | 0.916     | P<0.001      |

Strongest correlation is between the two mediolateral sites (C and E): r=0.955. Site C DEXA is thus the best DEXA surrogate for the SPA gold standard.

### Table 3: Correlations of BMD (site C) with biological variables

| Variable                | r         | Significance |
| ----------------------- | --------- | ------------ |
| Body weight             | 0.633     | NS           |
| **Number of races run** | **0.785** | **P<0.05**   |
| **Period on races**     | **0.882** | **P<0.01**   |
| Age                     | 0.247     | NS           |

NS = not significant.

### Reproducibility

CV for repeated DEXA at site C: **3%** (vs 2.7% for SPA) — comparable reliability, validating DEXA as a field-applicable alternative.

## Discussion & interpretation

The very strong correlation of BMD with racing period (r=0.882, p<0.01) and moderate-strong correlation with race count (r=0.785, p<0.05), combined with non-significant correlation with age (r=0.247) and body weight (r=0.633), demonstrates that bone sclerosis at the fracture site is a function of cumulative racing workload rather than simply maturation or body size. This is physiologically coherent: each race imposes high-impact loading at the distal McIII, progressively stiffening the subchondral bone. The site C DEXA values (mediolateral, 3.514 g/cm²) are substantially higher than dorsopalmar values (1.791–1.798 g/cm²), consistent with the mediolateral direction bearing the greatest compressive forces during galloping.

The width of the McIII in the dorsopalmar direction varied little among horses (justifying area-based BMD calculation), while mediolateral width variation was also constrained, making site C most reliable for between-horse comparison.

## Limitations

- n=30 autopsy specimens; small sample constrains the power of correlation analyses
- Only left McIII measured; right-left asymmetry not assessed
- Autopsy population may not be representative of the living racing population (cause of death not described)
- Age range 28–67 months — concentrated in young adult horses; very old horses not represented
- In vitro measurements only; direct in vivo DEXA application in standing horses involves motion artefact and different positioning constraints

## Feature-engineering notes for the model

- `career_starts` — number of races run from debut — source: race history records — expected effect: positive correlation with McIII BMD and thus fracture risk; r=0.785 in this population — data availability: standard
- `career_days` — elapsed racing period in days — source: days from first race entry — expected effect: strongest single predictor of McIII sclerosis (r=0.882); should be preferred over `career_starts` alone when both are available — data availability: derivable from race date history
- `age_years` — horse age — expected effect: non-significant for bone sclerosis (r=0.247 NS) — note: do NOT use age as a primary bone-stress proxy; race history metrics dominate
- `body_weight_kg` — 馬体重 in kg — expected effect: non-significant for McIII BMD (r=0.633 NS) — note: weight alone does not drive bone sclerosis; workload does
- **Interaction note:** `career_days / age_years` (racing density proxy) is not explicitly tested in this paper but is implied; high race-per-year-of-life rates may accelerate sclerosis beyond either metric alone

## Key references / follow-up leads

- Tomioka Y., Kaneko M., Oikawa M., Kanemaru T., Yoshihara T., and Wada R. 1985. Bone mineral content of metacarpus in racehorses by photon absorption technique: In vitro measurement. _Bull. Equine Res. Inst._ 22: 22–29. (SPA gold standard for McIII BMC; original method paper)
- Kaneko M., Oikawa M., and Yoshihara T. 1993. Pathological analysis of bone fractures in race horses. _J. Vet. Med. Sci._ 55: 181–183. (bone fracture pathology companion paper — same JRA group)
- Yoshihara T., Kaneko M., Oikawa M., Wada R., and Tomioka Y. 1989. An application of the image analyzer to the soft radiogram of the third metacarpus in horses. _Jpn. J. Vet. Sci._ 51: 184–186. (radiographic image analysis of McIII sclerosis)
- Eyres K.S., Bell M.J., and Kanis J.H. 1993. New bone formation during leg lengthening evaluated by dual energy x-ray absorptiometry. _J. Bone Jt. Surg._ 75-B: 96–106. (DEXA validation reference)
