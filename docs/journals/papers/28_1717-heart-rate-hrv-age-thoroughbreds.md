# Changes in heart rate and heart rate variability as a function of age in Thoroughbred horses

## Metadata

| Field                          | Value                                                                                                                                                          |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 28(3): 99–103, 2017                                                                                                                             |
| docid                          | `28_1717`                                                                                                                                                      |
| Article type                   | Full Paper                                                                                                                                                     |
| Authors                        | Hajime OHMURA, James H. JONES                                                                                                                                  |
| Affiliations                   | Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan; School of Veterinary Medicine, University of California, Davis, CA 95616, U.S.A. |
| Received / Accepted / Released | Received April 25, 2017; Accepted July 27, 2017                                                                                                                |
| Keywords                       | autonomic nervous activity, foal, old Thoroughbred                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/28/3/28_1717/_pdf/-char/en                                                                                            |

## Abstract (verbatim)

> We investigated changes in heart rate (HR) and HR variability as a function of age in newborn foals to old Thoroughbred horses. Experiments were performed on a total of 83 healthy and clinically normal Thoroughbred horses. Resting HR decreased with age from birth. The relationship between age and HR fit the equation Y=48.2X−0.129 (R²=0.705); the relationship between age and HR for horses 0–7 years old fit the equation Y=44.1X−0.179 (R²=0.882). Seven-day-old horses had the highest HR values (106 ± 10.3 beat/min). The low frequency (LF) and high frequency (HF) powers increased with age in newborn to old horses. These changes in HR and HR variability appear to result from the effects of ageing. Three- to seven-year-old race horses had the lowest HR values (32.9 ± 3.5 beat/min) and the highest LF and HF powers except for the HF powers in the oldest horses. Race training may have contributed to these changes. Horses of ages greater than 25 years old had the highest HF powers and the lowest LF/HF ratios. In individual horses, 8 of the 15 horses over 25 years old had LF/HF ratios of less than 1.0; their HR variability appears to be unique, and they may have a different autonomic balance than horses of younger age.

## Relevance to finishing-position (着順) prediction

Feature family **C (exercise physiology/fitness)** and **G (statistical modelling / age effects)**. This paper provides the quantitative age–HR relationship for JRA Thoroughbreds (from the JRA Equine Research Institute at Tochigi — the same population as most JRA racehorses). The equation Y = 48.2X^−0.129 (R² = 0.705) allows age-adjusted expected resting HR to be computed for any horse; the residual (actual HR minus expected HR for age) is a per-horse cardiovascular fitness indicator.

The most directly actionable finding: race-trained 3–7 year-old horses have the lowest resting HR of all age groups (32.9 ± 3.5 beat/min) — lower than untrained young horses (1–2 yr) and retired horses (15–24 yr, 41.2 ± 5.0 beat/min). This training-induced bradycardia is well established as a marker of cardiac adaptation and superior aerobic fitness. A horse with a resting HR below the age-expected value likely has better cardiovascular conditioning, consistent with better performance.

The LF/HF ratio (cardiac sympathovagal balance) peaks in race-trained horses (5.3 ± 2.9), reflecting the highest sympathetic nervous activity consistent with athletic training readiness. Departures from this pattern (e.g., elevated LF/HF indicating sympathetic dominance at rest, or depressed LF/HF indicating vagal dominance as seen in geriatric horses) could flag sub-optimal physiological state.

For JRA model features, resting HR before a race (if measurable) is the most accessible proxy from wearable sensors. The age-normed HR residual provides a fitness-relative-to-peers feature that controls for the natural HR decline with age.

## Background & objective

HR variability (HRV) is a non-invasive index of autonomic nervous activity: HF power reflects primarily parasympathetic activity; LF power reflects both sympathetic and parasympathetic contributions; LF/HF ratio encodes sympathovagal balance. HRV has been studied in adult Thoroughbreds (Kuwahara et al. 1996) and in training responses (Ohmura et al. 2002), but no prior study had measured HR and HRV from birth (newborn foals) through geriatric Thoroughbreds (>25 years), capturing the full lifespan.

Research gaps: (1) no normative data for foals and geriatric horses; (2) unknown whether the training-induced HR reduction observed in young horses during initial training is sustained and what the age trajectory looks like across the full lifespan; (3) unknown whether geriatric Thoroughbred autonomic balance differs from younger horses.

## Materials & methods

**Subjects:** 83 healthy and clinically normal Thoroughbred horses, 60 males and 23 females. Composed of:

- 7 foals measured longitudinally from within 24 hr of birth at 0 days, 7, 30, 90, and 200 days old
- 6 horses measured longitudinally at 1.5, 2.0, and 2.5 years old (no training under saddle during measurement period)
- 16 yearlings (1 year old)
- 27 race-trained horses (3–7 years old; all trained for racing and had raced)
- 12 retired horses (15–24 years old)
- 15 geriatric horses (>25 years old)

**ECG recording:** Base-apex Holter leads using SM-60 Holter ECG (Fukuda Denshi Co., Tokyo). Three electrodes affixed to thorax with foam tape; monitor in blanket pocket. Recordings ≥30 min in horse's own stall (familiar environment). All recordings 13:00–15:00 to control for diurnal variation. Horses free in stalls without restriction.

**HRV analysis:** Softron Co. ECG processor (previously validated for Thoroughbreds). R waves detected; RR-interval tachogram computed. Exclusion: horses with second-degree AV block in >10% of beats. Manual inspection to remove noise artefacts (values outside 75–125% of mean removed). Data resampled at 200 ms intervals; 512-point segments. Hamming window + Fast Fourier Transform → power spectrum. LF band: 0.01–0.07 Hz; HF band: 0.07–0.6 Hz.

**Regression:** Non-linear regression of HR vs. age. Significance: P ≤ 0.05.

**Data presentation:** Mean ± SD (not SE). Individual data for geriatric horses (>25 yr) in Table 2.

## Results (detailed — reproduce ALL numbers)

### Table 1 — HR and HRV by age group (mean ± SD; ranges in parentheses)

| Age group             | n   | Age                | BW (kg)            | HR (beat/min)              | LF power (msec²)                | HF power (msec²)               | LF/HF                   |
| --------------------- | --- | ------------------ | ------------------ | -------------------------- | ------------------------------- | ------------------------------ | ----------------------- |
| 0 days old            | 7   | —                  | 56.4 ± 5.1 (51–64) | 101.4 ± 14.5 (79–100)      | 158 ± 87 (71–299)               | 58 ± 35 (23–128)               | 2.9 ± 0.7 (1.6–3.7)     |
| 7 days old            | 7   | —                  | 72 ± 6.4 (62–84)   | 106.2 ± 10.3 (89–118)      | 171 ± 95 (73–381)               | 48 ± 26 (22–103)               | 3.8 ± 1.1 (2.4–5.6)     |
| 30 days old           | 7   | —                  | 115 ± 10 (96–128)  | 79.3 ± 6.1 (73–89)         | 364 ± 120 (170–538)             | 111 ± 83 (53–307)              | 4.0 ± 1.6 (1.8–7.1)     |
| 90 days old           | 7   | —                  | 183 ± 24 (152–222) | 60.1 ± 4.0 (52–65)         | 574 ± 280 (238–1,121)           | 155 ± 101 (48–383)             | 4.2 ± 1.5 (1.7–6.6)     |
| 200 days old          | 7   | —                  | 267 ± 28 (211–291) | 53.3 ± 6.2 (45–63)         | 1,157 ± 664 (539–2,493)         | 325 ± 146 (126–560)            | 3.9 ± 1.7 (1.4–6.6)     |
| 1 year                | 16  | —                  | —                  | 45.4 ± 2.4 (40.5–48.6)     | 1,184 ± 535 (373–2,189)         | 289 ± 196 (133–878)            | 4.9 ± 2.3 (1.1–10.3)    |
| 1.5 years             | 6   | —                  | 433 ± 11 (417–450) | 41.2 ± 3.0 (37–46)         | 1,926 ± 1,597 (444–5,356)       | 306 ± 157 (137–597)            | 6.0 ± 2.7 (2.8–9.0)     |
| 2 years               | 6   | —                  | 480 ± 31 (426–515) | 39.8 ± 5.6 (31–47)         | 1,744 ± 809 (805–3,338)         | 336 ± 113 (173–486)            | 5.7 ± 2.7 (2.4–10.4)    |
| 2.5 years             | 6   | —                  | 505 ± 34 (457–541) | 37.7 ± 2.1 (35–40)         | 1,853 ± 738 (1,050–3,062)       | 363 ± 96 (187–459)             | 5.4 ± 2.0 (2.3–8.1)     |
| 3–7 yr (race-trained) | 27  | 3.9 ± 1.2 (3–7)    | —                  | **32.9 ± 3.5** (25.0–40.0) | **3,138 ± 1,045** (1,575–5,488) | 844 ± 597 (213–2,545)          | 5.3 ± 2.9 (1.1–13.2)    |
| 15–24 yr (retired)    | 12  | 20.3 ± 2.5 (15–24) | —                  | 41.2 ± 5.0 (33.7–52.7)     | 1,371 ± 628 (623–2,391)         | 452 ± 185 (206–796)            | 3.1 ± 0.9 (1.7–5.1)     |
| >25 yr (geriatric)    | 15  | 27.1 ± 1.4 (25–30) | —                  | 39.4 ± 5.4 (28.0–46.0)     | 1,665 ± 1,425 (168–4,529)       | **2,679 ± 4,281** (183–13,050) | **1.5 ± 1.3** (0.3–4.4) |

### Age-HR regression equations

- All ages (0 days to 30+ years): **Y = 48.2X^−0.129** (R² = 0.705, P < 0.05), where X = age in years
- 0–7 years only: **Y = 44.1X^−0.179** (R² = 0.882, P < 0.05)

Note: X in years; for days-old foals, convert to fractional years (e.g., 7 days = 0.0192 years; 30 days = 0.0822 years).

### Table 2 — Individual geriatric horse data (>25 years old)

| Horse | Gender | Age (yr) | HR (beat/min) | LF power (msec²) | HF power (msec²) | LF/HF |
| ----- | ------ | -------- | ------------- | ---------------- | ---------------- | ----- |
| 1     | Male   | 26       | 39.7          | 3,234            | 12,900           | 0.25  |
| 2     | Male   | 26       | 43.7          | 4,308            | 13,050           | 0.33  |
| 3     | Female | 28       | 43.3          | 1,061            | 2,694            | 0.39  |
| 4     | Male   | 25       | 40.1          | 838              | 1,905            | 0.44  |
| 5     | Male   | 27       | 31            | 689              | 1,398            | 0.49  |
| 6     | Female | 30       | 46            | 168              | 254              | 0.66  |
| 7     | Male   | 27       | 38.4          | 608              | 629              | 0.97  |
| 8     | Male   | 29       | 42.9          | 533              | 536              | 0.99  |
| 9     | Male   | 27       | 28            | 4,529            | 3,302            | 1.37  |
| 10    | Female | 29       | 39.1          | 266              | 183              | 1.45  |
| 11    | Male   | 27       | 45.9          | 2,422            | 1,381            | 1.75  |
| 12    | Male   | 27       | 42            | 1,102            | 509              | 2.16  |
| 13    | Male   | 25       | 32.8          | 1,243            | 440              | 2.82  |
| 14    | Male   | 27       | 35.5          | 2,665            | 703              | 3.79  |
| 15    | Female | 26       | 42.6          | 1,309            | 300              | 4.37  |

8 of 15 geriatric horses had LF/HF < 1.0 (indicating HF > LF, i.e., parasympathetic dominance or unusual autonomic balance).

### Key quantitative findings summary

- 7-day-old foals had highest HR: 106.2 ± 10.3 beat/min
- Race-trained 3–7 yr horses had lowest HR: 32.9 ± 3.5 beat/min (range 25.0–40.0)
- Retired horses (15–24 yr) had HR 41.2 ± 5.0 beat/min — higher than race-trained; HR increases again after training cessation
- Geriatric horses (>25 yr) had HR 39.4 ± 5.4 beat/min — similar to retired, but with highest HF power (2,679 ± 4,281 msec²)
- LF power highest in race-trained horses: 3,138 ± 1,045 msec²
- LF/HF ratio highest at 1.5 years (6.0 ± 2.7) and race-trained (5.3 ± 2.9); lowest in geriatrics (1.5 ± 1.3)
- BW at 1.5 yr: 433 ± 11 kg; 2.0 yr: 480 ± 31 kg; 2.5 yr: 505 ± 34 kg (untrained young horses for longitudinal comparison)

## Discussion & interpretation

The power-law HR–age relationship (Y = 48.2X^−0.129) reflects allometric scaling: smaller/younger animals have higher metabolic rates and therefore higher HR to support cardiac output. The better fit in 0–7 year range (R² = 0.882) suggests the training effect in race horses (3–7 yr) produces an additional below-expected HR component on top of the general age-related decline.

Race-trained horses (3–7 yr) show the lowest resting HR and highest LF power among all age groups, likely due to both (a) natural HR decline with age and (b) training-induced bradycardia mediated by increased parasympathetic tone. The elevated LF/HF ratio in trained horses reflects augmented cardiac sympathovagal function consistent with athletic conditioning.

When horses retire from racing (15–24 yr), resting HR returns to ~41 beat/min (similar to untrained 2-year-olds), demonstrating that training-induced bradycardia is reversible and not permanent. This underscores HR as a dynamic fitness indicator rather than a fixed trait.

Geriatric horses (>25 yr) have a unique autonomic pattern: high HF power with low LF/HF ratio. The authors speculate this reflects increased cardiac vagal tone (parasympathetic dominance), which may represent age-related changes in autonomic regulation — potentially analogous to geriatric cardiovascular changes in humans. The extreme individual variability in geriatric HF power (183–13,050 msec²) suggests heterogeneous aging trajectories.

The prior training study (Ohmura et al. 2002) showed that handling and training of 1-year-olds increased parasympathetic activity and decreased HR within 7 months — consistent with the 1.5–2.5 yr longitudinal data showing HR declining from 41.2 to 37.7 beat/min without saddle training, attributable to natural growth rather than training per se.

## Limitations

- Cross-sectional comparison for most age groups (only foals 0–200 days and pre-training 1.5–2.5 yr were longitudinal); cannot distinguish age effects from cohort effects in other groups
- Race-trained group (3–7 yr) vs. retired (15–24 yr) confounds training status with age
- Sample sizes vary widely by group (7 foals, 27 race-trained, 12 retired, 15 geriatric); unequal statistical power
- All horses from JRA ERI facilities; selection bias toward elite JRA-bred Thoroughbreds
- Diurnal variation controlled (13:00–15:00), but seasonal or environmental variation not controlled across collection periods
- HR recorded at rest in familiar stall; does not capture exercise HR or pre-race stress HR

## Feature-engineering notes for the model

- `resting_hr_expected` — age-expected resting HR from equation Y = 48.2X^−0.129 (X in years) — source: derived from horse age at measurement — expected effect: decreases with age from birth to peak fitness; peak fitness age 3–7 yr gives lowest expected HR (~33–40 beat/min) — use as denominator for HR residual
- `resting_hr_residual` — actual resting HR minus age-expected HR — source: pre-race wearable HR data minus regression prediction — expected effect: negative residual (lower-than-expected HR) → better cardiovascular fitness for age → better finishing position — this is the most informative HR-derived feature if pre-race resting HR is available
- `resting_hr_absolute` — beats/min measured pre-race or at rest — source: wearable HR monitor in stall; not in public JRA race records — expected effect: negative (lower resting HR → better fitness); confounded by age — use as raw feature but always pair with age for context
- `lf_power_hf_power_ratio` — LF/HF ratio from Holter ECG — source: research-grade Holter recording; not in public JRA records — expected effect: higher LF/HF in trained horses (5.3 in race-trained vs. 3.1 in retired) → training readiness; monitoring for departures may flag overtraining — only available in ERI research setting
- `age_years` — horse age in years at race date — source: JRA race records (birth year publicly available) — expected effect: strong non-linear effect; prime racing age 3–5 yr; performance declines after 6–7 yr for most horses — include as continuous feature with non-linear encoding (e.g., spline or polynomial); also encode as `age_group` bucket
- `age_at_debut` — age in days at first race start — source: JRA records — expected effect: horses debuting very late (>1000 days) or very early (<600 days) may have developmental anomalies or poor training fit — use as a feature for debut context
- `years_in_training` — calendar years between first career start and current race — source: derived from JRA records — expected effect: proxy for cumulative training load and career wear; correlates with aging effect on HR — use alongside age for horses whose debut was atypical
- `training_bradycardia_flag` — binary: pre-race resting HR below age-expected value (HR_residual < −5 beat/min) — source: wearable HR — expected effect: positive (well-trained); flags peak fitness state — only applicable if real-time HR data available
- **CAUTION:** resting HR is lower in retired horses than in race-trained horses _at the same age range_, partly because trained horses are compared to untrained retired horses; do not conflate low HR with training effect without accounting for current training status
- **CAUTION:** LF/HF ratio interpretation in Thoroughbreds differs from humans (propranolol blockade does not change LF/HF in horses as in humans); standard human-derived HRV interpretations may not fully apply

## Key references / follow-up leads

- Ohmura H, Hiraga A, Aida H, Kuwahara M, Tsubone H. 2002. Effects of initial handling and training on autonomic nervous function in young Thoroughbreds. Am. J. Vet. Res. 63: 1488–1491. [Training effect on HRV in JRA yearlings]
- Kuwahara M et al. 1996. Assessment of autonomic nervous function by power spectral analysis of heart rate variability in the horse. J. Auton. Nerv. Syst. 60: 43–48. [Foundational HRV method in Thoroughbreds]
- Kamiya K, Ohmura H, Eto D et al. 2003. Heart size and heart rate variability of the top earning racehorse in Japan, T. M. Opera O. J. Equine Sci. 14: 97–100. [Elite individual horse case study]
- Ohmura H, Boscan PL, Solano AM et al. 2012. Changes in HR, HR variability, and AV block during withholding of food in Thoroughbreds. Am. J. Vet. Res. 73: 508–514. [Confounders: fasting effects on HR]
- Younes M, Robert C, Barrey E, Cottin F. 2016. Effects of age, exercise duration, and test conditions on heart rate variability in young endurance horses. Front. Physiol. 7: 155. [Parallel study in endurance breed]
- Hiraga A, Sugano S. 2017. Studies on exercise physiology of draft horses in Japan during the 1950s and 1960s. J. Equine Sci. 28(1): 1–12. [28_1623; foundational HR-VO₂ relationships]
