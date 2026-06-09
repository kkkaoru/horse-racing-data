# Usefulness of Plasma Fibrinogen Concentration Measurement in Diagnosis of Respiratory Disorders in Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                            |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 17(2): 27–32, 2006                                                                                                                                                                                                                                                                                |
| docid                          | `17_2_27`                                                                                                                                                                                                                                                                                                        |
| Article type                   | Full Paper                                                                                                                                                                                                                                                                                                       |
| Authors                        | Yoshimasa TAKIZAWA, Seiji HOBO                                                                                                                                                                                                                                                                                   |
| Affiliations                   | Racehorse clinic, Miho Training Center, Japan Racing Association, 2500–2 Mikoma, Miho-mura, Inashiki-gun, Ibaraki 300-0493; present address (Hobo): Microbiology Division, Epizootic Research Center, Equine Research Institute, Japan Racing Association, 1400–4 Shiba, Shimotsuke-shi, Tochigi 329-0412, Japan |
| Received / Accepted / Released | Accepted February 14, 2006                                                                                                                                                                                                                                                                                       |
| Keywords                       | diagnoses, fibrinogen, racehorse, respiratory disorders, thoroughbred                                                                                                                                                                                                                                            |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/17/2/17_2_27/_pdf/-char/en                                                                                                                                                                                                                                              |

## Abstract (verbatim)

> In this study, we determined a physiological standard for fibrinogen (Fbg) concentration in the plasma of clinically healthy thoroughbred racehorses trained under sufficient loads for running in races. Furthermore, we examined the usefulness of Fbg levels by comparing them with the white blood cell (WBC) counts in peripheral blood from thoroughbred racehorses diagnosed as suffering from colds or bacterial pneumonia. The Fbg level in healthy horses was lower (246.0 ± 33.1 mg/dl) than previously reported values. On the other hand, both the Fbg levels at the first medical examination and the maximum Fbg levels reflected the severity of the respiratory disorders; therefore, higher Fbg concentrations tended to be related to longer treatment periods. The results suggest that Fbg measurements, both initially and with the elapse of time, can enable the severity of clinical conditions of respiratory disorders in racehorses to be determined and an estimate to be made of the treatment period needed to cure the disease. Therefore, measurements of not only peripheral WBC counts, which have been previously examined, but also of Fbg concentrations permit a diagnosis, based on clinical conditions, of the severity of inflammation that can provide essential information on the pathological states of respiratory disorders, particularly bacterial pneumonia, in racehorses.

## Relevance to finishing-position (着順) prediction

Feature family: **B (respiratory disorders)**. Respiratory disease (colds and bacterial pneumonia) is a primary cause of rest and performance decline in Thoroughbred racehorses at JRA training centres. This paper provides: (1) a definitive reference range for plasma fibrinogen (Fbg) in race-fit Thoroughbreds at Miho Training Center (JRA) — 246.0 ± 33.1 mg/dl, lower than most literature values because horses were race-fit; (2) strong evidence that initial Fbg and maximum Fbg both predict treatment duration (r = 0.646 and 0.656, p < 0.01), making Fbg a quantitative severity-and-absence-duration predictor; (3) evidence that WBC count is essentially useless for predicting severity or treatment length (r = 0.163 / 0.094), so Fbg is the superior biomarker.

The practical feature engineering path: horses recovering from respiratory illness have elevated Fbg → longer rest → reduced training load → lower fitness → worse finishing position. A `days_since_respiratory_illness` or `recent_illness_flag` feature derived from training-centre medical records or identifiable gaps in workout data is the most available proxy. The Fbg threshold of ~300 mg/dl (≥ mean + 1.5 SD in healthy horses) can flag sub-clinical illness at race entry if blood data are available.

Interactions: `age` (mean age 2.5–2.6 yr in this sample — young horses disproportionately affected), `sex` (55M/49F in disease group, no significant sex effect reported), `days_to_next_race` (illness extends lay-off → long absence → form unknown), `running_style` (horses with high aerobic workload may have subclinical respiratory compromise affecting pacing ability).

## Background & objective

Respiratory disorders — especially colds and bacterial pneumonia — produce fever, nasal discharge, and prolonged rest requirements in racehorses at JRA training centres. Clinical diagnosis depends on physical examination, fever measurement, and WBC counts, but WBC is unreliable (often stays within normal range even in severe infection due to local neutrophil recruitment). Radiography and MRI are impractical for large horses. Inflammatory marker proteins (CRP, ceruloplasmin, haptoglobin, serum amyloid A, Fbg, α1-acid glycoprotein) are emerging as superior indicators in equine medicine. Fbg is easier to measure and has a simple salting-out assay. However, no JRA-specific reference range for race-fit Thoroughbreds existed, and no prior study had quantified the relationship between Fbg and treatment duration in racehorses. The objectives were: (1) establish a healthy racing Thoroughbred Fbg reference range; (2) compare Fbg vs. WBC as predictors of respiratory illness severity and treatment length.

## Materials & methods

**Healthy control group:**

- n = 96 clinically healthy Thoroughbred racehorses at Miho Training Center, JRA
- 55 males or geldings, 41 females; age 2.6 ± 0.9 yr (mean ± SD)
- Trained with sufficient load for racing
- Blood sampled from jugular vein at rest, ≥2 h after training completion
- Collection tubes: sodium citrate buffer (VT-050DK, Terumo) for Fbg; EDTA (VP-C050, Terumo) for WBC

**Disease group:**

- n = 104 Thoroughbred racehorses diagnosed with respiratory illness at Miho Training Center, JRA
- 55 males or geldings, 49 females; age 2.5 ± 0.8 yr
- **Group A** (mild cold): rectal temperature < 39.0°C at first examination; n = 37; no antimicrobial drugs; symptomatic treatment only
- **Group B** (severe / pneumonia): rectal temperature ≥ 39.0°C; n = 67; antimicrobial drugs (cephalothin sodium 10 g/head, 3×/day, Coaxin, Tobishi, Tokyo) until temperature and clinical signs normalised; 14/67 confirmed bacterial pneumonia by bronchoscopy (percolation liquid in bronchi)
- Blood sampled daily from first examination until clinical recovery

**Fbg assay:** Plasma separated by centrifugation (2,000 g, 10 min, 4°C). Fbg measured by salting-out method (Iatroset Fbg, Mitsubishi Kagaku Iatron, Tokyo) with human fibrinogen as standard.

- Intra-assay CV: 2.2% at 285 mg/dl, 1.7% at 520 mg/dl, 1.3% at 808 mg/dl (n=10 each)
- Inter-assay CV: 2.8% at 285 mg/dl, 1.9% at 520 mg/dl, 1.4% at 808 mg/dl (n=10 each)

**WBC count:** Automated blood cell counter (K-4500, Sysmex, Hyogo) on EDTA blood.

**Statistical methods:** Mann-Whitney U-test for group comparisons; Pearson correlation coefficient for Fbg/WBC vs. treatment duration. Significance: p < 0.05.

## Results (detailed — reproduce ALL numbers)

**Healthy horse reference values:**

| Parameter   | Value                              |
| ----------- | ---------------------------------- |
| Fbg (mg/dl) | 246.0 ± 33.1 (range 200–300)       |
| WBC (/mm³)  | 8,554 ± 1,006 (range 6,000–10,000) |

**Disease group Fbg levels:**

| Group                            | Fbg at first examination (mg/dl) | Maximum Fbg (mg/dl) |
| -------------------------------- | -------------------------------- | ------------------- |
| Healthy                          | 246.0 ± 33.1                     | —                   |
| Group A (mild cold, n=37)        | 302.1 ± 49.4                     | 415.0 ± 98.4 \*     |
| Group B (severe/pneumonia, n=67) | 530.0 ± 169.1 \*\*               | 668.8 ± 159.3 \*\*  |

\*p < 0.01 vs. healthy; \*\*p < 0.01 vs. healthy and vs. Group A.

Significant differences (p < 0.01) between:

- Healthy vs. Group B at first examination
- Healthy vs. Group A and Group B at maximum Fbg
- Group A vs. Group B at both first examination and maximum Fbg

**WBC counts:**

| Group   | WBC at first exam (/mm³) | Maximum WBC (/mm³) |
| ------- | ------------------------ | ------------------ |
| Healthy | 8,554 ± 1,006            | —                  |
| Group A | 10,885 ± 3,146           | 10,968 ± 3,035     |
| Group B | 11,797 ± 3,946           | 12,029 ± 3,832     |

No significant differences between healthy and Group A, healthy and Group B, or Group A and Group B for WBC at either timepoint.

**Correlations with treatment duration:**

| Predictor         | r (initial vs. treatment days) | r (maximum vs. treatment days) |
| ----------------- | ------------------------------ | ------------------------------ |
| Fbg concentration | **0.646** (p < 0.01)           | **0.656** (p < 0.01)           |
| WBC count         | 0.163 (not significant)        | 0.094 (not significant)        |

**Diagnostic thresholds inferred from the data:**

- Fbg ≥ 300 mg/dl: suggests at minimum mild cold (Group A level)
- Fbg ≥ 400 mg/dl: strongly suggests Group B severity / bacterial involvement
- Fbg ≥ 530 mg/dl: mean for severe Group B at first examination

**Comparative note on Fbg reference range:** The healthy JRA racing Thoroughbred Fbg (246 ± 33 mg/dl) is lower than most previously reported values (prior studies found maxima up to ~400 mg/dl). The authors attribute this to race-fit horses in good health, thorough training, and liver function adequacy (Fbg is a liver-produced acute-phase protein). The healthy WBC range (8,554 ± 1,006/mm³) matches prior reports.

## Discussion & interpretation

Fbg is a liver-produced acute-phase protein; its plasma concentration rises in response to tissue damage and inflammation. In respiratory disease, Fbg rises proportionally to bacterial burden and tissue involvement, with the highest values in confirmed bacterial pneumonia. The salting-out method used here correlates highly with the thrombin time method and has excellent precision (CV < 3%). The prior reference values (up to ~400 mg/dl "normal maximum") were inflated by inclusion of less fit horses; the JRA training-centre data (246 ± 33 mg/dl) is more appropriate for interpreting results in race-ready horses.

WBC's failure to predict treatment duration despite numerically higher counts in sicker horses is explained by the authors as "local neutrophilic recruitment in the presence of inflammation" — WBCs migrate out of peripheral blood into infected tissues, keeping peripheral counts paradoxically low or normal. This validates Fbg as a more reliable systemic inflammation marker.

The r ≈ 0.65 correlation of both initial and maximum Fbg with treatment duration is clinically useful: a horse with initial Fbg ~530 mg/dl faces a substantially longer rest than one with Fbg ~300 mg/dl. This maps directly onto training gap duration and expected performance degradation.

## Limitations

- Single centre (Miho Training Center, JRA); may not generalise to other training environments.
- n = 37 in Group A, n = 67 in Group B; the 14 bacteriologically confirmed pneumonia cases are a small subset.
- Treatment duration is not reported in absolute days (only correlation coefficient); the paper does not give mean treatment days per group.
- Fbg assay uses human fibrinogen as standard; may introduce a small systematic bias, though both intra- and inter-assay precision are excellent.
- No follow-up racing performance data after recovery; the link to finishing position is inferential.
- No sex or age subgroup analysis of Fbg levels.

## Feature-engineering notes for the model

- `respiratory_illness_days_since` — integer; days since most recent respiratory illness treatment concluded; source: JRA training centre veterinary records — expected effect: monotone negative (nearer illness → worse performance / higher scratch risk); interaction with `distance` (longer distance requires more aerobic capacity, more sensitive to respiratory compromise)
- `respiratory_illness_flag_recent` — binary (1 if illness within 60 days before race); source: veterinary logs or training gap — expected effect: significant negative predictor for finishing position
- `training_gap_days` — integer; number of days since last workout recorded in training DB; proxy for illness/rest periods — source: JRA training diary — expected effect: nonlinear (short gap = rest day ok; long gap = illness or injury flag)
- `plasma_fibrinogen_at_entry` — continuous (mg/dl); if blood records are available: reference 246 ± 33 mg/dl for healthy race-fit TB; threshold 300 = mild concern; 400 = serious concern — source: JRA/NAR blood monitoring at training centres — expected effect: strong negative — availability: research data, not standard race databases
- `wbc_count` — **do NOT use alone** as a respiratory illness severity predictor; r = 0.094–0.163 with treatment duration; WBC is not useful for this purpose even if available
- Interaction: `respiratory_illness_flag_recent × distance` — respiratory compromise disproportionately impairs longer-distance performance; encode as interaction term

## Key references / follow-up leads

- Hulten, C., Demmers, S. 2002. Serum amyloid A (SAA) as an aid in the management of infectious disease in the foal: comparison with total leucocyte count, neutrophil count and fibrinogen. _Equine Vet. J._ 34: 693–698. [SAA as even more sensitive marker than Fbg]
- Nunokawa, Y. et al. 1993. Evaluation of serum amyloid A protein as an acute-phase reactive protein in horses. _J. Vet. Med. Sci._ 55: 1011–1016. [SAA in Japanese horses]
- Hulten, C. et al. 1999. The acute phase protein serum amyloid A (SAA) as an inflammatory marker in equine influenza virus infection. _Acta Vet. Scand._ 40: 323–333.
- Giguere, S. et al. 2003. Evaluation of white blood cell concentration, plasma fibrinogen concentration, and agar gel immunodiffusion test for early identification of foals with Rhodococcus equi pneumonia. _J. Am. Vet. Med. Assoc._ 222: 775–781. [Combined markers in bacterial pneumonia]
- Rose, R.J., Hodgson, D.R. 1993. Lower respiratory tract diseases. In: _Manual of equine practice_, pp. 160–169. W.B. Saunders. [Standard reference for diagnosis and WBC use]
- Campbell, M.D., Bellamy, J.E., Searcy, G.P. 1981. Determination of plasma fibrinogen concentration in the horse. _Am. J. Vet. Res._ 42: 100–104. [Earlier equine Fbg reference values]
