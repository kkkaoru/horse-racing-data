# Reliability of EquiPILOT® for Measuring Aerobic Fitness in Racehorses

## Metadata

| Field                          | Value                                                                                             |
| ------------------------------ | ------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 13(4): 117–121, 2002                                                               |
| docid                          | `13_4_117`                                                                                        |
| Article type                   | Note                                                                                              |
| Authors                        | Ryo Kusunose, Toshiyuki Takahashi                                                                 |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya 320-0856, Japan |
| Received / Accepted / Released | Accepted January 21, 2003                                                                         |
| Keywords                       | EquiPILOT, racehorses, aerobic fitness                                                            |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/13/4/13_4_117/_pdf/-char/en                              |

## Abstract (verbatim)

> In track testing of aerobic fitness in the racehorse, the measurement of heart rate and running speed is essential. In this study, we examined EquiPILOT®, an automated recording system which measures those two variables, for its reliability during the racehorse training. After some adjustment for fitting, it has been shown that the speed measured by this system is very accurate, and that its measurements of heart rate and running speed are stable. Based on our data, the EquiPILOT® has been proved reliable for assessing aerobic capacity of running racehorses on the track.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **C (exercise-physiology/fitness)** — specifically the sub-domain of aerobic fitness measurement. It validates the EquiPILOT GPS+HR system for measuring V200 (running velocity at heart rate 200 bpm), the standard field proxy for VO2max in Thoroughbreds, with speed accuracy R²=0.997 and HR accuracy R²=0.982 vs. a precision racetrack timing system. V200 is arguably the single most actionable aerobic fitness feature for finishing-position prediction because it is trainable, measurable in field conditions without treadmill equipment, and directly related to race speed at aerobic capacity.

The paper provides concrete V200 values from three horses: a 2-year-old pre-race horse (V200 = 10.86 m/s = 651.6 m/min), and two conditioned adult horses (G: 12.33/12.23 m/s = 739.8/733.8 m/min; R: 9.99/10.59 m/s = 599.4/635.4 m/min before/after 3-week training). This ~140 m/min range between horses (599.4 to 739.8 m/min) represents a substantial performance spread that would map directly to finishing position differences if available as a feature. Horse R gained 36 m/min (6%) in V200 over 3 weeks of conditioning, while Horse G (already at high fitness base) was unchanged.

For the JRA/NAR pipeline, V200 is not recorded in standard race databases but may be available in research or training-management systems at JRA training centres (notably JRA Equine Research Institute at Utsunomiya and the yearling training farms). Even without direct access to V200 data, the paper motivates use of proxy features: days-in-training (fitness trajectory proxy), recent workout pace (speed of recent gallops), and HR-based training load metrics if available.

The regression equations for Case 2 horses are particularly informative for the pipeline: V200 is computed from the HR-speed linear regression intercept/slope, so any system that records HR and speed at multiple exercise intensities can compute it. The HR-speed regression for Horse R shifted right (rightward = fitter) after training: Before: y = 0.246x + 41.5 (R²=0.930); After: y = 0.259x + 35.32 (R²=0.962).

## Background & objective

VO2max measurement is the gold standard for aerobic fitness evaluation in racehorses but requires treadmill equipment and maximal-intensity testing, limiting its practical use. V200 (velocity at HR=200 bpm) was proposed by Person (1983) as a field-measurable aerobic fitness index, validated against VO2max by Rose et al. (1990). V200 data have been accumulating in JRA research settings (Kobayashi et al. 1999, Omura et al. 2002). The measurement requires only simultaneous heart rate and speed recording at sub-maximal intensities during routine training — but until EquiPILOT, automated field recording of speed at racing speeds was not practical. Stopwatch timing was still standard. GPS technology (EquiPILOT, Fidelak GmbH, Germany) offers automated recording, but was developed for lower-speed cross-country/endurance use. This study evaluated whether EquiPILOT is accurate and stable at racehorse training speeds.

## Materials & methods

**Equipment evaluated:**

- EquiPILOT® (Fidelak GmbH, Germany): GPS-based main body (~500 g, 12×7.5×5 cm) + HR monitor transmitter (~100 g), worn on horse with sensor pair around trunk; main body placed near HR transmitter with upper surface skyward
- Reference: Racetrack Timing System at JRA Equine Research Institute — electric-field sensors every 200 m along track, automatic speed calculation and simultaneous HR recording

**Accuracy evaluation protocol:** EquiPILOT placed on Thoroughbreds running on racetrack at the Equine Research Institute (Tochigi). Simultaneous recording by both EquiPILOT and racetrack timing system. Regression analysis of paired speed and HR readings.

**Case 1 — 2-year-old pre-race horse:** Male Thoroughbred, 2 years and 2 months old, 430 kg, in training toward breeze-up sale. Exercise: walk to ground; 2 min warm-up trot; walk to track; 500 m canter avg 7.8 m/s (25 sec/furlong); 200 m walk; 600 m canter avg 10.0 m/s (20.1 sec/furlong); 200 m walk; 600 m canter avg 10.1 m/s (19.8 sec/furlong); slow to walk. V200 computed from HR-speed regression at steady-state trot and canter.

**Case 2 — adult horses before and after training:** Horse G: 8-year-old male, Horse R: 7-year-old male. Both deconditioned after >6 months at pasture. Training: 3,200 m/day gallop, 5 days/week for 3 weeks, intensity increased from 26 to 16 sec/furlong. Recording before and after training. Exercise at each recording: trot then 800 m gallop at 8.3 m/s (24 sec/furlong), followed by 800 m at 10.0 m/s (18 sec/furlong). V200 computed from regression of HR vs. speed at steady-state gallop segments. Body weight measured before and after training for both horses.

**V200 computation:** V200 = x-intercept of HR-speed regression line at HR=200 bpm (i.e., x = (200 − intercept) / slope).

## Results (detailed — reproduce ALL numbers)

**Accuracy vs. racetrack timing system:**

- Speed: regression y = 0.975x + 5.66, R² = 0.997; slope close to 1.0 → accurate
- Heart rate: regression y = 1.044x − 3.54, R² = 0.982; slope close to 1.0 → accurate
- Conclusion: EquiPILOT recordings reliable for both speed and HR at racehorse training speeds

**Case 1 (2-year-old pre-race, 430 kg):**

- V200 = 10.86 m/s (651.6 m/min)
- This represents fitness level of a 2-year-old in preparation for breeze-up sale

**Case 2 HR-speed regressions:**

Horse R (7-year-old, deconditioned):
| | Before 3-week training | After 3-week training |
|---|---|---|
| Regression equation | y = 0.246x + 41.5 | y = 0.259x + 35.32 |
| R² | 0.930 | 0.962 |
| V200 (m/s) | 9.99 | 10.59 |
| V200 (m/min) | 599.4 | 635.4 |
| Body weight (kg) | 508 | 490 |
| Delta V200 | +0.60 m/s (+6.0%) | |

Horse G (8-year-old, deconditioned):
| | Before 3-week training | After 3-week training |
|---|---|---|
| Regression equation | y = 0.194x + 56.6 | y = 0.194x + 57.4 |
| R² | 0.950 | 0.950 |
| V200 (m/s) | 12.33 | 12.23 |
| V200 (m/min) | 739.8 | 733.8 |
| Body weight (kg) | 490 | 485 |
| Delta V200 | −0.10 m/s (−0.8%, no change) | |

Horse G's regression slope (0.194) is notably lower than Horse R's (0.246/0.259), indicating that HR increases more steeply with speed in Horse R — consistent with lower aerobic capacity. The rightward shift of Horse R's HR-speed regression after training (same HR at higher speed) reflects improved aerobic capacity. Horse G's regression was essentially unchanged (slope and intercept nearly identical), consistent with his already high V200 (~740 m/min) — 3 weeks of mild training was insufficient to further improve his well-conditioned baseline.

Both horses lost weight during training (R: 508→490 kg, −3.5%; G: 490→485 kg, −1.0%), consistent with increased energy expenditure and lean-mass adaptation.

## Discussion & interpretation

EquiPILOT's high accuracy (R²=0.997 for speed, R²=0.982 for HR) confirms it is a reliable replacement for stopwatch timing and chest-strap HR monitors in field V200 assessment. The key adjustment required was modified placement (near HR transmitter, upper surface skyward) to ensure GPS signal quality at racing speeds; the manufacturer's recommended saddle-arm mounting was unsafe and obstructive at high speeds.

The individual horse results illustrate both the predictive power and limits of V200 as a training management tool. Horse R improved meaningfully (599 → 635 m/min, +36 m/min), consistent with the aerobic conditioning effect of systematic gallop training. Horse G, with pre-existing high fitness (740 m/min), showed no response — his training was likely too mild relative to his fitness base to produce an overload stimulus. This illustrates the principle that delta_V200 across training sessions is a meaningful fitness trajectory signal, but its interpretation depends on the current V200 level (a ceiling effect at high fitness).

The range of V200 observed across even this small sample (600–740 m/min) is a ~23% speed difference, which would translate directly to substantial differences in race-pace competitiveness. In a race at 600 m/min (a moderate gallop), a horse with V200=740 m/min is operating well below aerobic threshold while one with V200=600 m/min is at limit — a large sustainable-speed advantage for the fitter horse.

## Limitations

- Accuracy validation sample size not stated (number of horse-run pairs in the regression); presented as confirmation study without confidence intervals on regression parameters
- Case studies (2 or 3 horses total); not a population-level study of V200 distribution across Thoroughbreds
- Case 2 training was mild (pasture horses returning to light conditioning) — V200 response may differ for horses already in race training
- V200 computed from sub-maximal HR-speed regression; accuracy depends on the linear HR-speed assumption holding in the 100–200 bpm range (breaks down near VO2max)
- EquiPILOT was a commercial product by Fidelak GmbH (Germany); modern GPS trackers (Equimetre, EQUIMETRICS, Polar Equine, etc.) have substantially better accuracy and are the current standard

## Feature-engineering notes for the model

- `V200_mpm` — running velocity at HR=200 bpm (m/min) — computed from HR-speed regression using GPS+HR data from training sessions — strongest direct aerobic fitness feature; expected strong positive association with finishing position (faster horses finish higher); interaction with race distance (V200 more predictive for middle-long distances; sprint performance also depends on anaerobic capacity)
- `delta_V200_3wk` — change in V200 over 3-week training period (m/min) — derived from two V200 measurements 3 weeks apart — fitness trajectory; positive delta indicates horse improving; Horse R gained +36 m/min over 3 weeks; expected positive association with upcoming race performance
- `HR_speed_slope` — slope of HR-speed regression (bpm per m/s) — derived from multiple HR-speed pairs — steeper slope = less aerobic efficiency; Horse R slope 0.246–0.259 vs. Horse G 0.194; expected negative association with performance
- `HR_speed_intercept` — intercept of HR-speed regression (bpm at 0 speed) — same source — lower intercept after training (Horse R: 41.5 → 35.32) indicates rightward shift = improved fitness; expected negative association with performance (lower intercept = fitter)
- `BW_change_training_kg` — body weight change during training block (kg) — horse weight records — negative BW change in short training blocks reflects lean conditioning; Horse R lost 18 kg, Horse G lost 5 kg; moderate negative BW change may signal peak fitness but extreme loss signals health concern
- `days_at_gallop_training` — number of days in gallop-intensity training before race — training logs if available — proxy for fitness accumulation; diminishing returns expected after ~3–6 weeks in fit horses
- **Caution:** V200 and HR-speed data are not in standard JRA race databases. Feature is available only if training centres record GPS + HR data. For production pipeline, `recent_race_time_per_furlong` or `pace_percentile_vs_distance_class` are the nearest available proxies.

## Key references / follow-up leads

- Rose RJ, Hendrickson DK, Knight PK. 1990. Clinical exercise testing in the normal Thoroughbred racehorse. _Aust. Vet. J._ 67: 345–348. [V200 validation against VO2max]
- Kobayashi M, Kuribara K, Amada A. 1999. Application of V200 values for evaluation of training effects in the young Thoroughbred under field conditions. _Equine Vet. J. Suppl._ 30: 159–162. [JRA V200 field data accumulation]
- Omura H, Hiraga A, Matsui A, Aida H, Inoue Y, Asai Y, Sakamoto K, Tomita M. 2002. The change in V200 in yearlings. _Equine Science_ 39: 19–22. [Japanese; V200 changes in JRA yearlings]
- Person SGB. 1983. Evaluation of exercise tolerance and fitness in the performance horse. pp. 441–457. In: Equine Exercise Physiology (Snow DH, Person SGB, Rose RJ eds.), Granta Editions, Cambridge. [original V200 methodology proposal]
- Evans DL, Rose RJ. 1986. Method of investigation of the accuracy of four digitally-displaying heart rate meters suitable for use in the exercising horse. _Equine Vet. J._ 18: 129–132.
- Yamanobe A, Hiraga A, Kubo K. 1993. Evaluation of the training effect using daily heart rate during warming up in the horse. _Bull. Equine Res. Inst._ 30: 5–8.
