# Simple Lactate Measurement in Horses Using a Portable Lactate Analyzer with Lancet Skin Punctures under Field Conditions

## Metadata

| Field                          | Value                                                                                     |
| ------------------------------ | ----------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 18(1): 5–11, 2007                                                          |
| docid                          | `18_1_5`                                                                                  |
| Article type                   | Note                                                                                      |
| Authors                        | Mitsutoshi Kobayashi                                                                      |
| Affiliations                   | Blood Horse Training Center Horse Clinic, 141 Nishisya, Urakawa, Hokkaido 057-0171, Japan |
| Received / Accepted / Released | Accepted September 26, 2006                                                               |
| Keywords                       | horse, lactate measurement, portable analyzer                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/18/1/18_1_5/_pdf/-char/en                        |

## Abstract (verbatim)

> The following items were investigated to measure blood lactate in horses under field conditions using portable lactate analyzers recently developed for human athletes. 1) The precision of two types of portable lactate analyzer and the standard analyzer using the immobilized enzyme method, and correlation among the lactate concentrations measured by the three lactate analyzers. 2) The correlation between the lactate concentrations in peripheral blood collected by lancet puncture and jugular venous blood using Lactate-pro (L-pro). The study was performed in Thoroughbreds under training for races, using Accusport (AC) and L-pro as portable lactate analyzers and YSI1500 (YSI) for the standard analyzer using the immobilized enzyme method. The intra- and inter-assay coefficients of variation (CV) of L-pro was lower than AC at low and middle lactate concentrations. The precision of L-pro is better than AC when the blood lactate level is lower than 10 mmol/l, and hematocrit (Ht) is 57% or lower. The lactate concentration using L-pro in peripheral blood collected by lancet puncture through the neck skin was similar to the level in jugular venous blood. No infection or inflammatory symptom occurred after blood sampling and the examination is relatively safe. These findings suggest that blood lactate measurement using L-pro by lancet puncture through the neck skin under field conditions is simple and safe, requiring no blood sampling from the jugular vein, and it can be used for evaluation of aerobic capacity and intensity of exercise in daily submaximal training.

## Relevance to finishing-position (着順) prediction

Feature family: **C — exercise physiology / fitness**. Blood lactate is an established proxy for aerobic capacity and exercise intensity in Thoroughbreds; Evans et al. 1993 (_Equine Vet. J._ 25: 441–445) demonstrated correlations between racing performance and blood lactate + heart rate after exercise. This paper validates a simple, field-deployable measurement method (Lactate-Pro analyser with cervical lancet puncture, r = 0.99 vs. jugular YSI) that enables routine lactate data collection in JRA training facilities outside laboratory settings.

The direct modelling value is as an **enabler paper** rather than a paper generating new effect estimates: it confirms that Lactate-Pro cervical lancet measurements are equivalent to jugular vein gold-standard measurements, making any lactate values collected by this method reliable for use as training fitness features. The V200 (speed at blood lactate 2 mmol/l) and V4 (speed at 4 mmol/l) submaximal training indices — standard aerobic capacity tests in Thoroughbred racing — can be reliably computed from L-Pro measurements at the field level. These indices, if available from JRA training records, represent strong fitness proxies for pre-race form assessment.

A key practical constraint is the 10 mmol/l lactate ceiling above which L-Pro precision degrades: this is not a limitation for submaximal training monitoring (routine values 2–8 mmol/l) but does preclude accurate measurement of peak post-race lactate (~20–30 mmol/l). For the prediction pipeline, submaximal training lactate is more relevant than peak race lactate, so the 10 mmol/l limit is not binding.

## Background & objective

Previous portable lactate analysers (especially Accusport/AC) required 15–50 μl of blood via capillary collection — awkward for equine field use. The newer Lactate-Pro (L-Pro) requires only 5 μl by direct capillary action through the skin. Prior to this study, no report existed on L-Pro performance in horses, and concerns about safety of lancet puncture in horses were unaddressed. The study investigated: (1) precision and cross-analyser correlation among L-Pro, Accusport, and YSI1500; (2) whether cervical lancet peripheral blood lactate matches jugular venous blood lactate using L-Pro.

## Materials & methods

**Experiment 1 — Analyser precision and correlation:**

- Subjects: 28 healthy 2-year-old Thoroughbreds (26 males, 2 females) in active race training, without physical examination abnormalities.
- Analysers: YSI 1500 Sport (range 0–30 mmol/l), Accusport/AC (range 0.8–22 mmol/l), Lactate-Pro/L-Pro (range 0.8–23.3 mmol/l). All calibrated before testing; measurements at 18°C, 60% humidity.
- Blood: Jugular vein into sodium fluoride/potassium oxalate tube; stored ~4°C; measured after 1 hr. Whole blood for intra-assay CV; plasma (centrifuge 3,000 rpm, 20 min) for inter-assay CV.
- Horses exercised at various intensities to span the physiological lactate range.

_Intra-assay CV:_ 10 measurements each at low, middle, and high lactate concentrations (whole blood).

_Inter-assay CV:_ 3 measurements/day × 10 days = 30 measurements per concentration level (plasma).

_Correlation:_ 28 horses × 3 measurements each per analyser; linear regression and Pearson r among all three pairs. Subset analyses for: (a) lactate ≥ 10 mmol/l vs. < 10 mmol/l (influence of high lactate); (b) SD change as function of Ht (influence of hematocrit).

**Experiment 2 — Lancet vs. jugular comparison:**

- Subjects: 42 Thoroughbreds (34 two-year-olds, 6 three-year-olds, 2 four-year-olds; 30 males, 12 females) in active race training.
- Protocol: Blood collected simultaneously by (a) 21-G lancet puncture through left neck skin (hair shaved ~1 cm2 before exercise; sweat wiped after exercise; first blood wiped, second blood measured) and (b) jugular venipuncture — both collected 5 min after exercise.
- L-Pro used for both; immediate measurement.
- Post-puncture observation for infection (no anaesthesia used).
- Statistics: Pearson's product-moment correlation; JMP 5J (SAS Institute Japan); significance at P < 0.05 and P < 0.01.

## Results (detailed — reproduce ALL numbers)

**Intra-assay CV (whole blood, n=10 measurements each):**

| Concentration | Analyser | Mean (mmol/l) | SD (mmol/l) | CV (%) |
| ------------- | -------- | ------------- | ----------- | ------ |
| Low           | L-Pro    | 1.92          | 0.09        | 4.54   |
| Low           | AC       | 2.23          | 0.12        | 5.32   |
| Low           | YSI      | 1.99          | 0.04        | 1.78   |
| Middle        | L-Pro    | 8.74          | 0.28        | 3.20   |
| Middle        | AC       | 8.97          | 0.41        | 4.57   |
| Middle        | YSI      | 7.63          | 0.17        | 2.29   |
| High          | L-Pro    | 15.55         | 0.95        | 6.09   |
| High          | AC       | 13.82         | 1.10        | 7.98   |
| High          | YSI      | 17.50         | 0.49        | 2.78   |

Ranking: YSI lowest CV at all concentrations; L-Pro < AC at low and middle; CV increases with lactate concentration for both portable analysers.

**Inter-assay CV (plasma, n=10 days × 3/day):**

| Concentration | Analyser | Mean (mmol/l) | SD (mmol/l) | CV (%) |
| ------------- | -------- | ------------- | ----------- | ------ |
| Low           | L-Pro    | 2.67          | 0.09        | 3.37   |
| Low           | AC       | 2.22          | 0.10        | 4.41   |
| Low           | YSI      | 3.42          | 0.07        | 2.04   |
| Middle        | L-Pro    | 7.50          | 0.23        | 3.04   |
| Middle        | AC       | 6.93          | 0.39        | 5.59   |
| Middle        | YSI      | 6.10          | 0.13        | 2.11   |
| High          | L-Pro    | 12.58         | 0.46        | 3.62   |
| High          | AC       | 16.51         | 0.57        | 3.44   |
| High          | YSI      | 14.39         | 0.34        | 2.38   |

L-Pro inter-assay CV (3.0–3.6%) < AC at low and middle concentrations.

**Cross-analyser linear regression and correlation (n=28 horses × 3 measurements = 84 per pair):**

| Pair          | Regression         | r    | P      |
| ------------- | ------------------ | ---- | ------ |
| YSI vs. L-Pro | y = 0.874x − 0.147 | 0.96 | < 0.01 |
| YSI vs. AC    | y = 0.80x + 0.983  | 0.93 | < 0.01 |
| L-Pro vs. AC  | y = 0.889x + 1.447 | 0.95 | < 0.01 |

Highest correlation: YSI–L-Pro (r = 0.96).

**Influence of high lactate (≥10 mmol/l):**

- Below 10 mmol/l: all three correlations high and significant.
- ≥10 mmol/l: YSI–L-Pro correlation decreased; YSI–AC and L-Pro–AC correlations became **non-significant**.

**Influence of hematocrit (Ht):**

- Ht >54%: SD slightly increased for YSI and AC.
- Ht >57%: SD slightly increased for L-Pro.
- Practical limit: L-Pro reliable for Ht ≤ 57%.

**Lancet vs. jugular (Experiment 2, n = 42):**

- Regression: y = 0.953x + 0.08
- Correlation: **r = 0.99, P < 0.01**
- No infection or inflammatory symptom observed at any lancet puncture site.

## Discussion & interpretation

L-Pro demonstrates lower intra- and inter-assay CV than AC at physiologically relevant lactate concentrations (1.92–8.97 mmol/l range in training), making it the preferred portable analyser for submaximal training monitoring in horses. Its limitation at ≥10 mmol/l and Ht >57% matches prior AC limitations at slightly lower thresholds, confirming a general principle that electrochemical portable analysers degrade at extreme blood parameters. The near-perfect correlation between cervical lancet and jugular venous measurements (r = 0.99, y = 0.953x + 0.08) confirms clinical equivalence and enables routine field use without veterinary jugular puncture. The slight systematic offset (slope 0.953, intercept +0.08 mmol/l) is negligible for V200/V4 threshold estimation. The author notes that human-validated L-Pro (r = 0.99 vs. YSI in humans, Pyne et al. 2000) performs at equivalent level in horses.

## Limitations

- NOTE article — not designed to generate effect-size estimates; only validates a measurement method.
- Does not report lactate-to-race-result correlations; the modelling value is indirect (enabling valid data collection).
- Single site (Urakawa Blood Horse Training Center); training intensity distribution may not reflect all JRA training environments.
- Small n for Experiment 1 (28 horses); Experiment 2 covers a wider age range but still one facility.
- High-concentration limit (≥10 mmol/l) means post-race peak lactate cannot be accurately measured; only training submaximal values are reliable.

## Feature-engineering notes for the model

- `blood_lactate_mmol_l` — measured by L-Pro cervical lancet 5 min post-exercise — derivable from training log records if JRA training-centre data are accessible — expected effect: lower lactate at given submaximal speed → better aerobic capacity → positive finishing-position effect — data availability: not in public race records; requires training facility data linkage
- `v200_m_s` — treadmill speed at blood lactate 2 mmol/l (submaximal aerobic threshold) — derived from multiple lactate measurements at known speeds — expected effect: higher V200 → better aerobic base → positive finishing position — data availability: JRA standardised exercise tests if available
- `v4_m_s` — speed at 4 mmol/l lactate (anaerobic threshold estimate) — same derivation as V200 — expected effect: higher V4 → higher anaerobic threshold → better race-pace capacity — data availability: JRA exercise test records
- `training_lactate_trend` — slope of lactate at fixed submaximal speed over last N sessions — indicates improving fitness if slope negative — derivable from repeated L-Pro measurements at consistent training speed — data availability: training log required
- **Practical note:** The 10 mmol/l ceiling of L-Pro means post-race peak lactate (which can reach 20–30 mmol/l in a maximal race effort) cannot be measured with this device. Only pre-race or submaximal training lactate values are reliable for L-Pro.

## Key references / follow-up leads

- Evans, D.L., Harris, R.C., and Snow, D.H. 1993. "Correlation of racing performance with blood lactate and heart rate after exercise in thoroughbred horses." _Equine Vet. J._ 25: 441–445. [key performance-lactate correlation paper]
- Harkins, J.D. et al. 1993. "The correlation of running ability and physiological variables in Thoroughbred racehorses." _Equine Vet. J._ 25: 53–60.
- Pyne, D.B. et al. 2000. "Evaluation of the Lactate Pro blood lactate analyzer." _Eur. J. Appl. Physiol._ 82: 112–116. [human validation of same device]
- Evans, D.L., and Golland, L.C. 1996. "Accuracy of Accusport for measurement of lactate concentrations in equine blood and plasma." _Equine Vet. J._ 28: 398–402.
- Lindner, A. 1996. "Measurement of plasma lactate concentration with Accusport." _Equine Vet. J._ 28: 403–405.
- Seeherman, H.J., and Morris, E.A. 1990. "Methodology and repeatability of a standardized treadmill exercise test for clinical evaluation of fitness in horses." _Equine Vet. J. Suppl._ 9: 20–25.
