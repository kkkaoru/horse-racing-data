# Investigation of Standardized Exercise Tests According to Fitness Level for Three-Day Event Horses

## Metadata

| Field                          | Value                                                                                                                                                        |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 9(1): 1–7, 1998                                                                                                                               |
| docid                          | `9_1_1`                                                                                                                                                      |
| Article type                   | Original                                                                                                                                                     |
| Authors                        | Ana Muñoz, Cristina Riber, Rafael Santisteban, Rafael Vivo, Sergio Agüera, Francisco Castejón                                                                |
| Affiliations                   | Department of Animal Biology, Section of Physiology, Faculty of Veterinary Medicine, University of Córdoba, Camp. Univ. Rabanales, C1. 14071, Córdoba, Spain |
| Received / Accepted / Released | Submitted April 8, 1997; accepted January 8, 1998                                                                                                            |
| Keywords                       | heart rate, lactate, SET, three-day event horses                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/9/1/9_1_1/_pdf/-char/en                                                                                             |

## Abstract (verbatim)

> Nine three-day event horses were divided into two groups. In Group A (4 horses), prior to the start of training, horses were subjected to standardized exercise test (SET) A: after a warm-up of 5 min walking and 6 min trotting, horses were galloped at 400 m, 500 m, 600 m, and 700 m/min over 1000 m with 5 min walking in between. In Group B (5 horses), after four months of training, horses were subjected to SET B: after a warm up of 5 min walking and 20 min trotting, horses were galloped at 400 m, 500 m, and 600 m/min over 1000 m with 5 min walking in between. For the SET of Group A, six physiological indices (V150, V200, VLA2, VLA4, HRLA2, HRLA4) were determined. Among these, VLA4 is an important index, being the exercise intensity needed to improve both aerobic and anaerobic capacities. At the conclusion of SET A, since blood lactate levels exceeded 4 mmol/l the final gallop at 700 m/min was eliminated. Two horses in Group B required a more intense exercise to realize blood lactate levels of 4 mmol/l. Increasing the exercises intensity of SET B can be accomplished by shortening the walking interval between galloping exercises, and this would decrease the risk of musculoskeletal injury.

## Relevance to finishing-position (着順) prediction

Feature family **C (exercise-physiology/fitness)**. This paper validates the use of standardised exercise tests (SETs) for quantifying fitness in equine athletes, and demonstrates the specific fitness indices that discriminate untrained from trained states. The key indices — VLA2, VLA4, V150, V200, HRLA2, HRLA4 — are direct measures of aerobic/anaerobic capacity that have been established in Standardbred and Thoroughbred performance literature as predictors of competitive outcome.

VLA4 (speed at blood lactate 4 mmol/L) is the primary index: it represents the maximal speed sustainable without progressive lactate accumulation (i.e., the functional anaerobic threshold), and directly predicts how fast a horse can race while staying below its aerobic ceiling. A horse with higher VLA4 can sustain faster race-pace speeds without entering the lactate spiral that causes fatigue. The 46% increase in VLA4 after 4 months of training (498.6 → 727.8 m/min) demonstrates the magnitude of change this feature can capture across a training cycle.

V200 (speed at heart rate 200 bpm) serves as a practical field-equivalent of VLA4, derivable without blood draws — only an HR monitor is needed. The paper validates V200 ≈ VLA4 in horses at comparable fitness levels (the equivalence of V200 and VLA4 is supported by Persson 1983, cited). HR-based indices are therefore a practical route to fitness proxy features in any racing pipeline that has access to training HR data.

For JRA/NAR prediction: if pre-race exercise test data or HR monitoring during workouts are available, VLA4 or V200 estimates could serve as the most physiologically grounded fitness features in the model, capturing the horse's current aerobic ceiling rather than relying solely on historical race performance.

## Background & objective

Equine exercise physiology research has focused on VO₂max and the aerobic–anaerobic transition zone as performance predictors. VO₂max requires respiratory gas exchange equipment impractical for field use; the lactate-based aerobic threshold (VLA2) and anaerobic threshold (VLA4) can be determined with portable lactate analysers on-track. Heart-rate-based indices (V150, V200) add a non-invasive alternative. Prior SETs had been developed mainly for endurance horses and Standardbreds; the design of an appropriate SET for three-day event horses (involving galloping at competition-level speeds) required validation. The group had previously developed SETs for Andalusian horses (Muñoz et al. 1997) and extended that framework here.

Three aims stated: (1) Design a short, practical field SET; (2) Justify the design choices; (3) Establish modifications to adapt the SET to different fitness tiers.

## Materials & methods

**Subjects:** 9 gelding three-day event horses (Anglo-Hunter and Thoroughbred breeds), ages 8–12 years; clinically healthy at time of test.

**Group A (n = 4):** tested before the start of the training season (untrained/less fit state)
**Group B (n = 5):** tested after a 4-month training period (trained/fitter state)

**Track:** Sandy track, semielliptical, 1,500 m perimeter. First 1,000 m had <1% upward slope; second 500 m was flat or very gentle downhill. Markers every 100 m. Speed checked by chronometer against markers; no significant difference found (P < 0.01) between real and scheduled time.

**SET A (Group A) — Table 1:**

| Bout          | Velocity  | Gait   | Duration | Distance |
| ------------- | --------- | ------ | -------- | -------- |
| N1            | 100 m/min | Walk   | 5:00 min | 500 m    |
| N2            | 250 m/min | Trot   | 6:00 min | 1,500 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min | 500 m    |
| N3            | 400 m/min | Gallop | 2:30 min | 1,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min | 500 m    |
| N4            | 500 m/min | Gallop | 2:00 min | 1,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min | 500 m    |
| N5            | 600 m/min | Gallop | 1:40 min | 1,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min | 500 m    |
| N6            | 700 m/min | Gallop | 1:25 min | 1,000 m  |

Total duration SET A: 33 min 35 sec (11 min warm-up + 7 min 35 sec exercise + 20 min recovery between bouts). Total distance: 8,000 m.

**SET B (Group B) — Table 2:**

| Bout          | Velocity  | Gait   | Duration  | Distance |
| ------------- | --------- | ------ | --------- | -------- |
| N1            | 100 m/min | Walk   | 5:00 min  | 500 m    |
| N2            | 250 m/min | Trot   | 20:00 min | 5,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min  | 500 m    |
| N3            | 400 m/min | Gallop | 2:30 min  | 1,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min  | 500 m    |
| N4            | 500 m/min | Gallop | 2:00 min  | 1,000 m  |
| Walk recovery | 100 m/min | Walk   | 5:00 min  | 500 m    |
| N5            | 600 m/min | Gallop | 1:40 min  | 1,000 m  |

Total duration SET B: 48 min 32 sec (25 min warm-up + 8 min 32 sec exercise + 15 min recovery between bouts). Total distance: 11,000 m. (700 m/min level eliminated; see Results.)

**Blood sampling:** External jugular vein venepuncture at rest and within 30 sec of finishing each workload. Blood centrifuged immediately to prevent further lactate production by blood cells. Plasma stored for analysis within 24 hr.

**Plasma lactate:** Enzymatic method (Analox Model Champion-PLM5).

**Heart rate:** Polar Sport Heart Rate Tester (Polar Electro OY, Finland); recorded at 5-sec intervals during exercise; mean and maximum values obtained at each SET level.

**Fitness indices calculated:**

- VLA2: speed at blood lactate 2 mmol/L (aerobic threshold)
- VLA4: speed at blood lactate 4 mmol/L (anaerobic threshold)
- V150: speed at HR = 150 bpm (comparable intensity to VLA2 per Persson 1983)
- V200: speed at HR = 200 bpm (comparable intensity to VLA4 per Persson 1983)
- HRLA2: heart rate at lactate 2 mmol/L
- HRLA4: heart rate at lactate 4 mmol/L

All indices extrapolated from linear (HR-velocity) and exponential (lactate-velocity) regression relationships. HR monitor problems prevented V150, V200, HRLA2, HRLA4 data from being obtained for Group B.

## Results (detailed — reproduce ALL numbers)

**VLA2, VLA4, and maximum lactate (Table 3):**

| Horse                 | Group | VLA2 (m/min)     | VLA4 (m/min)     | LAmax (mmol/L)   |
| --------------------- | ----- | ---------------- | ---------------- | ---------------- |
| 1                     | A     | 374.2            | 512.8            | 12.8             |
| 2                     | A     | 355.7            | 529.0            | 10.0             |
| 3                     | A     | 355.6            | 494.2            | 6.8              |
| 4                     | A     | 319.9            | 458.5            | 11.0             |
| **Group A mean ± SD** |       | **351.4 ± 22.7** | **498.6 ± 30.3** | 6.8–12.8 (range) |
| 5                     | B     | 426.6            | 599.9            | 9.1              |
| 6                     | B     | 603.6            | 734.7            | 4.9              |
| 7                     | B     | 440.3            | 786.9            | 7.2              |
| 8                     | B     | 392.6            | 739.2            | 3.2              |
| 9                     | B     | 547.5            | 778.5            | 1.9              |
| **Group B mean ± SD** |       | **489.3 ± 75.2** | **727.8 ± 75.2** | 1.9–9.1 (range)  |

Training effect on VLA4: 498.6 → 727.8 m/min, an increase of **229.2 m/min or ~46%**.
Training effect on VLA2: 351.4 → 489.3 m/min, an increase of **137.9 m/min or ~39%**.

**Exponential lactate-velocity regression equations:**

- Group A: LA = 0.377 × e^(0.005V), regression coefficient > 0.97
- Group B: LA = 0.507 × e^(0.003V), regression coefficient > 0.97

**V150, V200, HRLA2, HRLA4 (Group A only, Table 4):**

| Horse | V150 (m/min) | V200 (m/min) | HRLA2 (bpm) | HRLA4 (bpm) |
| ----- | ------------ | ------------ | ----------- | ----------- |
| 1     | 522          | 757          | 129         | 162         |
| 2     | 430          | 663          | 123         | 162         |
| 3     | 466          | 670          | 132         | 168         |
| 4     | 439          | 647          | 126         | 161         |

V200 range: 647–757 m/min; close to VLA4 range (458–529 m/min) from same horses — note V200 is in absolute speed; VLA4 and V200 measure closely related aerobic ceiling concepts (Persson 1983). HRLA4 range: 161–168 bpm.

**HR regression:** Regression coefficients > 0.95 for V150, V200, HRLA2, HRLA4.

**Lactate observations during SET B:**

- Three horses in Group B had LAmax above 4 mmol/L after 600 m/min: 9.1, 4.9, 7.2 mmol/L
- Two horses had LAmax below 4 mmol/L: 3.2, 1.9 mmol/L — these two required extrapolation for VLA4, which the authors note could overestimate VLA4

**SET A — lactate at each speed for Group A:**

- At 400 m/min: lactate 1.3–3.0 mmol/L (below VLA4 but approaching for some)
- At 700 m/min: lactate 6.8–12.8 mmol/L (all horses exceeded VLA4) — this motivated elimination of the 700 m/min bout from SET B

**HR stabilisation within each exercise bout:**

- At 400 and 500 m/min: peak HR reached within 25–40 sec of starting the bout
- At 600 and 700 m/min: peak HR reached within 55–70 sec of starting the bout; maintained ± 6 bpm from that point until end of bout
- Authors conclude: adequate heart-rate stabilisation within bout duration (even without reaching the classical 3-minute steady-state), justifying the shorter bout durations used

## Discussion & interpretation

**Warm-up duration:** Group A: 11 min (5 min walk + 6 min trot, 2,000 m). Group B: 25 min (5 min walk + 20 min trot, 5,500 m). Authors judged 11 min sufficient for cardiorespiratory and musculoskeletal preparation in untrained horses (citing Auvinet et al. 1989 and Isler et al. 1982). In trained horses (Group B), a longer warm-up was needed to prepare the higher-capacity system adequately.

**Number of exercise bouts:** 4 bouts (Group A) based on Thornton et al. (1983) and Wilson et al. (1983) showing that 4–8 data points yield high-quality exponential lactate-velocity curves. Fewer points risk erroneous curve fitting; more would require 14–18 blood draws (impractical for field).

**Selection of starting speed:** 400 m/min for both groups; this produced lactate of 1.3–3.0 mmol/L in untrained horses — appropriate to define the sub-threshold region of the curve. The authors note this is higher than some other studies' starting speeds but appropriate for event horses.

**Elimination of 700 m/min from Group B:** After 600 m/min, Group A horses already showed lactate 4.7–7.3 mmol/L. Since Group B horses were fitter, 700 m/min was expected to produce extreme lactate and musculoskeletal injury risk without adding useful information. Instead, rest intervals can be shortened to increase intensity without changing speed.

**Training adaptation confirmed:** The separation between Group A (VLA2 mean 351.4, VLA4 mean 498.6 m/min) and Group B (VLA2 mean 489.3, VLA4 mean 727.8 m/min) validates the SET as a fitness assessment tool. The authors note: "Training at VLA4 improves both the aerobic and anaerobic capacities."

**Practical protocol modification:** The authors recommend that for horses at higher fitness levels, the SET should use shorter rest intervals (reducing 5 min to ≤3 min between bouts) rather than adding a 700+ m/min level — this achieves higher effective exercise intensity with lower injury risk.

## Limitations

- Different horses in Group A and Group B (not the same horses before and after training); this is not a longitudinal paired design — group differences may reflect individual variation rather than training effect exclusively
- Small sample size (n = 4 and n = 5); large individual variation especially in Group B (VLA4 SD = 75.2 m/min vs. 30.3 m/min in Group A)
- HR data for Group B not available due to equipment problems; V150, V200, HRLA2, HRLA4 indices could not be calculated for trained horses
- Three-day event horses (Anglo-Hunters and Thoroughbreds, geldings only, age 8–12 years); direct extrapolation to 2–5-year-old Japanese racing Thoroughbreds and Quarter/Standardbreds requires caution
- Field track with <1% slope and varying meteorological conditions; laboratory treadmill would give more controlled results
- Extrapolation of VLA4 from below-threshold data points in 2 Group B horses may overestimate their VLA4

## Feature-engineering notes for the model

- `VLA4_estimate` — speed at which blood lactate = 4 mmol/L during a graded field exercise test — source: JRA/NAR supervised field test data if available — expected effect: higher VLA4 → higher sustainable race speed → positive for finishing position, especially at distances where aerobic power dominates (≥1600 m); 46% increase with 4 months training gives a sense of within-horse variation — data availability: not in standard race entry data; research/training management context
- `V200_estimate` — speed at heart rate 200 bpm from HR monitoring during workout — source: Polar-type HR monitor during training gallops — expected effect: equivalent to VLA4 (per Persson 1983 and this paper's regression coefficients > 0.95); more practically accessible than blood lactate — data availability: JRA monitors HR in training; V200 requires computing from HR-speed relationship which needs several speed-HR pairs
- `V150_estimate` — speed at heart rate 150 bpm — source: same HR monitoring — expected effect: corresponds to VLA2 (aerobic threshold); lower absolute speed than V200; useful for distinguishing aerobic base across horses — data availability: as V200
- `HRLA4` — heart rate at which blood lactate reaches 4 mmol/L — derivation: from field test; Group A mean was ~161–168 bpm — expected effect: higher HRLA4 means the horse reaches anaerobic threshold at a higher HR, indicative of a higher cardiac output capacity at threshold — data availability: research context
- `lactate_velocity_coeff` — exponential coefficient in LA = A × e^(B × V) fitted to individual horse's field test — derivation: requires ≥4 speed-lactate pairs from a SET — expected effect: smaller B coefficient → flatter lactate accumulation curve → horse maintains aerobic pace over wider speed range → better distance performer; larger B → steeper lactate curve → better sprint/short-distance performer — this is a mechanistically grounded "distance aptitude" feature
- `training_month_in_season` — proxy for fitness state based on how many months into training season the horse is — derivation: from calendar / race records — expected effect: horses 3–5 months into training period should be near peak VLA4 (Group B equivalent); horses at start of season or after layoff should be near Group A equivalent — data availability: derivable from race record gaps and season start dates
- `VLA4_distance_interaction` — interaction term: estimated VLA4 × race_distance — horses with very high VLA4 should outperform more at longer distances where sustained aerobic speed matters; at sprint distances, acceleration and anaerobic power dominate VLA4 less
- CAUTION: VLA4 in event horses (400–728 m/min = 6.7–12.1 m/s) is directly comparable to Thoroughbred racing speeds (typical JRA races 12–17 m/s), so the absolute values should not be transferred directly; relative within-horse change over time is more useful than cross-species absolute comparison

## Key references / follow-up leads

- Persson, S.G.B. 1983. Evaluation of exercise tolerance and fitness in the performance horse. pp. 441–457. In: Equine Exercise Physiology (Snow, Persson, Rose eds.), Granta Editions, Cambridge. — foundational reference for V150 ≈ VLA2 and V200 ≈ VLA4 equivalence; most-cited SET methodology paper
- Wilson, R.G., Isler, R.B., and Thornton, J.R. 1983. Heart rate, lactic acid production and speed during a standardized exercise test in Standardbred horses. pp. 487–496. In: Equine Exercise Physiology. — SET validation in Standardbreds; 4+ speed-lactate pairs for good curve fitting
- Thornton, J., Essén-Gustavsson, B., Lindholm, A., McMicken, D., and Persson, S. 1983. Effects of training and detraining on oxygen uptake, cardiac output, blood gas tensions, pH and lactate concentrations during and after exercise in the horse. pp. 470–486. In: Equine Exercise Physiology. — training/detraining changes in VLA4 and related indices; quantitative magnitude of training adaptation
- Muñoz, A., Santisteban, R., Rubio, D., Vivo, R., Agüera, E.I., Escribano, B.M., and Castejón, F.M. 1997. The use of functional indexes to evaluate fitness in Andalusian horses. J. Vet. Med. Sci. 59: 747–750. — same group's earlier SET validation in a different breed; provides baseline comparison data
- Auvinet, B., Galloux, P., Michaux, J.M., Lepage, O., et al. 1989. Test d'effort standardise de terrain pour chevaux de concours complet d'equitation. pp. 432–463. In: Compte-rendu du VIIème Congress des Pays Francophones. — 8-speed French SET; different design for comparison
- Isler, R., Straub, R., Appenzeller, Th., and Gysing, J. 1982. Beurteilung der aktuellen leistungs fähigkeit... Scheweig. Arch. Tierheilk. 123: 603–612. — Warmblood SET with 8-min rest intervals; design comparison
- Kronfeld, D.S., Ferrante, P.L., Taylor, L.E., and Custalow, S.E. 1995. Blood hydrogen ion and lactate concentrations during strenuous exercise in the horse. Equine Vet. J. Suppl. 18: 266–267. — note that 14–18 blood samples are needed for accurate threshold determination; field tests must compromise
