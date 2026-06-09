# The Japanese Experience with Tendonitis in Racehorses

## Metadata

| Field                          | Value                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 13(2): 41–56, 2002                                                                          |
| docid                          | `13_2_41`                                                                                                  |
| Article type                   | Review                                                                                                     |
| Authors                        | Masa-aki OIKAWA, Yoshinori KASASHIMA                                                                       |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted May 20, 2002                                                                                      |
| Keywords                       | tendonitis, racehorses, tendon injury                                                                      |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/13/2/13_2_41/_pdf/-char/en                                        |

## Abstract (verbatim)

> Superficial digital flexor tendon (SDFT) injury leading to tendonitis is the most frequent soft tissue injury in racehorses. As tendonitis has been noted to heal slowly and imperfectly, injured tendons require long periods of rest, and the rate of recurrence of tendonitis upon return to training is high. Between 1,100 and 1,200 of the 6,400 to 7,000 racehorses currently registered with the Japan Racing Association (JRA) suffer from tendonitis, and some 70% of these return to racing without making a comeback in a single race. In view of this statistic, the JRA has conducted investigational and clinical research from various perspectives. This paper will review the results of these studies, thus providing insight into practical methods of treating and preventing tendonitis. The study results are summarized under the following headings: 1) Introduction; 2) Impacts and loads on the lower limb at the track; 3) Occurrence of tendonitis at JRA training centers; 4) The hoof and tendonitis; 5) Ultrasonic diagnostic criteria; 6) Therapeutic regimens; 7) Conclusion. We concluded that tendon injuries represent irreversible structural alterations that are unlikely to be significantly altered by therapy, so that the best outcome may be achieved by minimizing the damage done by the original injury. From this viewpoint, research efforts will be directed toward prevention and early diagnosis, rather than therapy of established lesions.

## Relevance to finishing-position (着順) prediction

Feature family: **A (injury/soundness — soft-tissue injury, specifically SDFT tendonitis)**. This is the most comprehensively quantified injury study in this collection and the single most important paper for building JRA-specific scratch/layup risk features. The review synthesises ~10 years of JRA investigational data (1987–1997), covering epidemiology, biomechanics, risk factors, and prognosis. Key quantified signals for a finishing-position model:

1. **Track surface is a top-level injury risk modifier**: Woodchip track training reduces periostitis incidence by 60% vs dirt (34.3% vs 13.4%). Turf firm conditions increase injury; dirt wet/heavy conditions increase injury. This is the quantitative JRA basis for `track_surface × track_condition` features that affect not only injury risk but also pace and performance.

2. **Tendonitis incidence ≈ 11.3% per horse-year**: Mean incidence 11.31 ± 0.65% (7,321/64,787 registered horses) over 1992–1997. With 1,100–1,200 horses affected at any time, approximately 1 in 9 registered horses is currently affected. A horse entering or returning from a tendonitis layup carries this population-level baseline risk.

3. **70% non-return rate**: Approximately 70% of tendonitis horses fail to return to racing at all. Those that do return are at high recurrence risk (recurrence rate explicitly noted as high). This is the most important prognosis number: once SDFT tendonitis is diagnosed, probability of continuation at race level drops dramatically.

4. **Biomechanical risk markers observable from race records**:
   - Higher career days → higher SDFT modulus of elasticity (stiffer tendon → fracture risk): positive correlation, p significant
   - Higher total career distance → higher modulus of elasticity: positive correlation
   - Higher number of career layups → higher modulus: positive correlation
   - Higher frequency of works → lower modulus (more regular light work is protective)
   - Race times: tendonitis-affected horses ran significantly faster on woodchip and uphill training tracks before injury (Table 2: 16.7 ± 2.4 s vs 17.8 ± 1.5 s woodchip, p<0.037)

5. **Sex and body weight**: Males 64.5% of tendonitis cases in 1997; among 3-year-olds specifically (eliminating early retirement bias for fillies), 66.6% colts vs 34.0% fillies, while actual starter ratio was 59.7% colts : 40.2% fillies → colts over-represented. Affected colts weighed 477.8 kg (17.8 kg heavier than unaffected colts); affected fillies 460.0 kg (12.0 kg heavier than unaffected fillies); both differences statistically significant (p<0.05).

6. **MIZ-HYP% (maximum injury zone hypoechogenicity %) predicts comeback probability and layup duration**: Horses with MIZ-HYP% ≈ 17.3 ± 10.6% had high comeback probability; those with MIZ-HYP% ≈ 25.6 ± 15.2% low probability. Layup duration formula: months = 0.161 × MIZ-HYP% + 3.479 (r=0.581, p<0.05). This translates injury severity to expected days_since_last_race.

7. **SDFT cross-sectional area (CSA) ratio A/N > 1.2 predicts tendonitis development**: Horses whose affected-leg SDFT CSA was >20% larger than the contralateral leg subsequently developed ultrasonographically confirmed tendonitis; A/N ≤ 1.2 horses did not. This is a pre-injury screening criterion.

## Background & objective

Tendonitis (primarily SDFT) is the most frequent soft-tissue injury in JRA racehorses. In 1992, 1,145 of ~6,400 horses were affected; by 1994, 1,372. The JRA established the Committee for the Prevention of Accidents to Racehorses, which conducted surveys on risk factors (track surface, training protocols, shoeing) and clinical research (ultrasound diagnostics, therapy trials). This review summarises that body of work under 7 headings.

## Materials & methods

Review of JRA investigational studies 1987–1997. Key component studies:

**1. Ground-reaction force / biomechanics (Oikawa et al. 2000, Am. J. Vet. Res. 61:979–985; Kai et al. 1999, Equine Vet. J. Suppl. 30:214–217):**

- Instrument sandwiched between hoof and shoe measures vertical ground-reaction forces and 3D acceleration at walk, trot, and canter on woodchip, dirt, and turf tracks
- Rough track (footprint-pitted) surface: irregular impacts; leg stress far greater than smooth track

**2. Tendonitis incidence registry (longitudinal, Miho and Ritto Training Centers):**

- ~6,400–7,000 registered horses; ~4,000 admitted to training centres at any time
- Tendonitis cases 1987–1997 (Fig. 2): 937 (1987), 1,003 (1989), 1,155 (1991), 1,145 (1992), 1,372 (1994), 1,271 (1997)
- Catastrophic ruptures: ~24 (1987) → ~5/year after woodchip track construction
- Mean incidence 1992–1997: 11.31 ± 0.65% (7,321/64,787 horses); catastrophic rupture incidence: 0.07 ± 0.02% (44/64,747 horses) (Table 1)
- Catastrophic injuries during racing at Nakayama and Tokyo (1992–1995): 8 of 48,000 starters (0.017%); all 8 had prior locomotor injury history; 7 of 8 raced on dirt; 6 of 8 had low-ranking jockeys; all in races 5–10; 7 had 1–5 wins

**3. SDFT modulus of elasticity and CSA (Takahashi & Stover 2001, J. Equine Sci. 12:62):**

- Simple regression analysis: modulus of elasticity significantly higher with higher career days, total career distance, number of career layups, total layup time (all significant positive slopes)
- Modulus of elasticity significantly lower when frequency of works during career was high (protective effect of frequent lighter work)

**4. Hoof balance and tendonitis (Tanaka 1998, Am. Farriers J. May/June:74–76):**

- Irregular hoof shapes associated with SDFT tendonitis: long toe (Lt), low heel (Lh), under-run heel (Uh)
- Base-wide toe-in stance (fetlock internally rotated) frequent in tendonitis horses
- Radiographic assessment of hoof shape in three planes (dorsal, lateral, frontal) reveals differences between affected and healthy hooves

**5. Ultrasound diagnostics (Morii 2001, MS Thesis; Endo et al. 1999, J. Vet. Sci.):**

- PCA of 11 clinical/sonographic parameters from 146 tendonitis cases identified 5 significant prognostic parameters: MIZ-SA (cross-sectional area at maximum injury zone), MIZ-HYP% (hypoechoic %, maximum injury zone), T-SA (total SDFT CSA across all 7 zones), T-HYP (total hypoechoic CSA), %T-HYP (percentage hypoechoic)
- Clinical signs (swelling, heat, pain, lameness at walk/trot) not adequate indicators of severity

**6. Tendonitis lesion types and prognosis (Endo et al. 1999, chi-squared analysis):**

- Type A (core lesion, n=80): comeback rate 37.5%; recurrence 43.3%
- Type B (border/peripheral lesion, n=30): comeback rate 17.09%; recurrence 33.33%
- Type C (diffuse lesion, n=29): comeback rate 79.03%; recurrence 16.13%
- Type C significantly better comeback than A or B (chi-squared, p significant); Type C significantly lower recurrence

**7. MIZ-HYP% and layup duration regression (105 horses at JRA Hot Springs Sanatorium, 1993–1998):**

- High comeback probability associated with MIZ-HYP% = 17.3 ± 10.6%
- Low comeback probability: MIZ-HYP% = 25.6 ± 15.2%
- Layup duration (months) = 0.161 × MIZ-HYP% + 3.479 (P<0.05, r=0.581)

**8. SDFT CSA ratio for early detection (Korosue et al. 1999, n=26 horses with swollen tendons, no ultrasonographic abnormality):**

- Horses that later developed tendonitis: A/N ratio >1.2 (affected leg CSA > 20% larger than contralateral)
- Horses that did not develop tendonitis: A/N ≤1.2
- Statistically significant difference

**9. Body weight and sex (Oikawa 2000):**

- 1997 data: affected colts 477.8 kg, unaffected colts 460.0 kg (difference +17.8 kg, p<0.05); affected fillies 460.0 kg, unaffected fillies 448.0 kg (difference +12.0 kg, p<0.05)
- Sex distribution 1997: colts 64.5%, fillies 32.6%, geldings 2.9%

**10. Training track comparison (retrospective, Miho n=52 affected+175 healthy; Ritto n=76 affected+180 healthy):**

- Affected horses: less woodchip training (Miho); more dirt training (Ritto); less woodchip in trials (Ritto)
- Run times on woodchip: tendonitis horses 16.7 ± 2.4 s vs healthy 17.8 ± 1.5 s (final furlongs; p<0.037)
- Run times on uphill course: tendonitis horses 15.8 ± 3.2 s vs healthy 17.0 ± 1.8 s (p<0.059)

## Results (detailed — reproduce ALL numbers)

**Incidence data:**

| Year                            | Tendonitis cases                               |
| ------------------------------- | ---------------------------------------------- |
| 1987                            | 937                                            |
| 1989                            | 1,003                                          |
| 1991                            | 1,155                                          |
| 1992                            | 1,145                                          |
| 1994                            | 1,372                                          |
| 1997                            | 1,271                                          |
| 1992–1997 mean                  | 11.31 ± 0.65% (7,321/64,787 horses registered) |
| Catastrophic ruptures 1992–1997 | 0.07 ± 0.02% (44/64,747)                       |

Catastrophic injuries declined from ~24/year in 1987 to ~5/year after woodchip track construction.

**Track surface periostitis comparison:**

- Dirt track: 34.3% periostitis incidence
- Woodchip track: 13.4% periostitis incidence
- Ratio: dirt = 2.56× woodchip incidence

**Training run times at Ritto (Table 2):**

| Group                                  | n   | Woodchip (s, final furlongs) | Uphill (s) |
| -------------------------------------- | --- | ---------------------------- | ---------- |
| Horses that later developed tendonitis | 68  | 16.7 ± 2.4                   | 15.8 ± 3.2 |
| Healthy horses                         | 169 | 17.8 ± 1.5                   | 17.0 ± 1.8 |
| p-value                                | —   | 0.037                        | 0.059      |

**Trial times by year and surface (Table 3; G1 entry horses at Ritto, final 3 furlongs):**

| Year        | Dirt course (s) | Turf course (s) | Woodchip course (s) |
| ----------- | --------------- | --------------- | ------------------- |
| 1990        | 38.98 (n=154)   | 36.14 (n=18)    | 39.68 (n=9)         |
| 1997        | 38.72 (n=14)    | 34.20 (n=12)    | 38.23 (n=192)       |
| Improvement | −0.26 s         | −1.94 s         | −1.45 s             |

**Body weight comparison (1997 data):**

| Sex     | Tendonitis-affected (kg) | Unaffected (kg) | Difference (kg) | Significance |
| ------- | ------------------------ | --------------- | --------------- | ------------ |
| Colts   | 477.8                    | 460.0           | +17.8           | p<0.05       |
| Fillies | 460.0                    | 448.0           | +12.0           | p<0.05       |

**Limb side affected at Ritto Training Center (Fig. 4):** Right forelimb affected 10–20% more frequently than left forelimb, despite horses training both clockwise and anticlockwise.

**Lesion type vs prognosis (Table 4):**

| Lesion type      | n   | Comeback rate (%) | Recurrence rate (%) |
| ---------------- | --- | ----------------- | ------------------- |
| Type A (core)    | 80  | 37.5              | 43.3                |
| Type B (border)  | 30  | 17.09\*           | 33.33\*\*           |
| Type C (diffuse) | 29  | 79.03\*           | 16.13\*             |

\*Statistically significant difference from other types (chi-squared).

**MIZ-HYP% data from 105 Hot Springs Sanatorium horses:**

- High comeback group: MIZ-HYP% = 17.3 ± 10.6%
- Low comeback group: MIZ-HYP% = 25.6 ± 15.2%
- Regression: Layup months = 0.161 × MIZ-HYP% + 3.479 (r=0.581, p<0.05)
  - Example: MIZ-HYP% = 20% → estimated layup = 0.161 × 20 + 3.479 = 6.70 months
  - Example: MIZ-HYP% = 40% → estimated layup = 0.161 × 40 + 3.479 = 9.92 months

**SDFT temperature post-exercise:** SDFT bundle core temperature confirmed to exceed 42°C after a run; tenocyte survival rate decreases after 1 hr at 43°C. Thermal damage may be a predisposing lesion pathway (Wilson & Goodship 1994).

## Discussion & interpretation

The authors conclude that SDFT tendonitis is essentially an irreversible injury — healed tendons have inferior collagen fibre architecture that never fully restores original mechanical properties. The high recurrence rate confirms this. Prevention is therefore more important than cure.

Key preventive insights: woodchip and uphill tracks reduce catastrophic rupture but may increase chronic tendonitis from accumulation of micro-damage in cushion-layered tracks (paradox explained by deeper cushion layer increasing tendon displacement per stride). Horses trained predominantly on dirt at Ritto were more susceptible; horses that ran fastest on woodchip in training trials (16.7 vs 17.8 s) were already at higher risk — speed is a risk factor.

The regular work frequency protective effect is consistent with exercise physiology: frequent moderate exercise maintains tendon nutrition and structural integrity, whereas prolonged rest periods followed by high-intensity exercise create conditions for acute injury.

The right-forelimb predominance (10–20% higher incidence than left) despite bidirectional training is unexplained but consistently observed. This may reflect musculoskeletal asymmetry, rider-side handedness, or track curvature effects even in bidirectional training.

## Limitations

- Retrospective registry data; some incidence numbers reflect improved ultrasound diagnosis (more cases detected after ultrasound adoption, not necessarily more cases occurring)
- Lesion-type prognosis data (n=146 for PCA; n=105 for layup regression) are from a specialised sanatorium, not population-based; selection bias toward horses considered candidates for recovery
- MIZ-HYP% regression has r=0.581 (r²=0.34); substantial residual variance means the equation provides a rough estimate only
- Hoof shape data are qualitative/radiographic; not linked to specific injury outcomes with sample sizes
- BAPN-F and hyaluronic acid therapeutic data are preliminary (unpublished at time of review)

## Feature-engineering notes for the model

- `track_surface` — woodchip / dirt / turf — source: JRA/NAR race venue records — dirt (especially heavy/wet) and firm turf increase injury risk; woodchip reduces catastrophic rupture; encode as ordered risk categories; expected effect on pace (faster on firmer surfaces, more fatigue/injury risk)
- `track_condition_going` — fast / good / muddy / sloppy (dirt) or firm / good / soft / yielding (turf) — source: JRA/NAR race card data — interacts with surface; dirt heavy = highest injury risk; turf firm = high risk; include `surface × going` interaction
- `career_days` — number of days since first registered race — source: race records — positive correlation with SDFT modulus of elasticity (stiffer tendon → higher injury risk); expected positive association with layup probability
- `career_total_distance_m` — cumulative racing distance (metres) — source: race records — positive correlation with tendon modulus; use in combination with career_days
- `career_layup_count` — number of recorded rest periods (gaps > threshold days between races, e.g., >90 days) — source: race records — positive correlation with tendon modulus (each layup represents prior damage)
- `career_works_per_month` — frequency of registered work/trial times — source: JRA training time records if available — protective factor; higher frequency → lower modulus → lower injury risk
- `days_since_last_race` — gap since previous race (days) — source: race records — horses returning from long layups (consistent with prior tendonitis layup formula: layup_months = 0.161 × MIZ-HYP% + 3.479) may still have compromised tendons; >180 days absence flag for suspected tendonitis return
- `post_layup_flag` — binary: 1 if days_since_last_race > 90 days — proxy for return from tendonitis or other injury
- `body_weight_kg` — 馬体重 (kg) at race entry — heavier horses (colts +17.8 kg, fillies +12.0 kg) have significantly higher tendonitis incidence; higher weight → more load per stride → higher SDFT stress
- `sex_male` — male (colt/stallion/gelding) — males 64.5% of tendonitis in 1997 at slightly greater than their starting population share (~59%); colts at 3 yo: 66.6% vs 59.7% start ratio
- `training_trial_time_woodchip_rank` — percentile rank of final-furlong work time on woodchip vs peers in similar training period — faster pre-race woodchip times associated with tendonitis (p=0.037); this is a JRA training records feature; expected relationship: fastest workers at elevated injury risk
- `right_forelimb_injury_history` — flag for prior right forelimb injury — right forelimb 10–20% more affected than left; asymmetric risk feature
- `lesion_type_sdft` — Type A/B/C from ultrasound if available — comeback rates: C=79%, A=38%, B=17%; layup duration estimable from MIZ-HYP% if ultrasound data in veterinary records
- **Interaction term**: `career_total_distance_m × sex_male` — male horses carry heavier loads and are heavier, compounding both risk factors for SDFT; the interaction likely nonlinear
- **Caution**: The R² of the MIZ-HYP% layup regression is only 0.34; use only as rough order-of-magnitude estimate. Do not use to predict exact return dates.

## Key references / follow-up leads

- Oikawa, M. 2000. The Japanese experience with breakdowns. Proc. 13th Annu. Fall Sympo. Recent Adv. Clin. Vet. Med. University of California, Davis. pp. 63–67. (population injury rate epidemiology; parent source of this review's incidence figures)
- Takahashi, T. and Stover, S. 2001. The relationship of the modulus of elasticity and cross-sectional area of the superficial digital flexor tendon in racehorses to their exercise history. _J. Equine Sci._ 12: 62. (key study showing career days / total distance / layup count → tendon modulus; source for biomechanical risk features)
- Kai, M., Aoki, O., Hiraga, A., Oki, H., and Tokuriki, M. 2000. Use of an instrument sandwiched between the hoof and shoe to measure vertical ground reaction forces and three-dimensional acceleration at the walk, trot, and canter in horses. _Am. J. Vet. Res._ 61: 979–985. (JRA instrumented-shoe ground-reaction force data; biomechanical basis for surface-impact features)
- Wilson, A.M. and Goodship, A.E. 1994. Exercise-induced hyperthermia as a possible mechanism for tendon degeneration. _J. Biomech._ 27: 899–905. (42°C+ tendon temperature post-exercise → tenocyte damage; thermal predisposition to tendonitis)
- Genovese, R.L., Rantanen, N.W., and Simpson, B.S. 1990. Clinical experience with quantitative analysis of superficial digital flexor tendon injuries in thoroughbred and standardbred racehorses. _Vet. Clin. North Am. Equine Pract._ 6: 129–145. (ultrasound diagnostic criteria for MIZ-HYP%, T-SA, etc.; standard reference for sonographic grading)
- Kasashima, Y., Smith, R.K.W., Birch, H.L., Takahashi, T., Kusano, K., and Goodship, A.E. 2002. Exercise-induced tendon hypertrophy: cross-sectional area changes during growth are influenced by exercise. _Equine Vet. J. Suppl._ 34: 264–268. (CSA changes with exercise from birth; early training effects on tendon structure — cross-referenced in 14_2_51)
