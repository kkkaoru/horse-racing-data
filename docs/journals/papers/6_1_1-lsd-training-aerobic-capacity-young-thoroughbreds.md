# The Effect of Long Slow Distance Training on Aerobic Work Capacity in Young Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 6(1): 1–6, 1995                                                                             |
| docid                          | `6_1_1`                                                                                                    |
| Article type                   | Original                                                                                                   |
| Authors                        | Atsushi Hiraga, Makoto Kai, Katsuyoshi Kubo, B. Kipp Erickson                                              |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 27-7 Tsurumaki 5-chome, Setagaya-ku, Tokyo 154, Japan |
| Received / Accepted / Released | Submitted January 13, 1995; accepted April 13, 1995                                                        |
| Keywords                       | aerobic work capacity, canter distance, Thoroughbred, training                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/6/1/6_1_1/_pdf/-char/en                                           |

## Abstract (verbatim)

> We investigated the effect of long slow distance training on aerobic work capacity in eleven 2-year-old Thoroughbred horses. Eight weeks of training (5 days/week) was performed on a track at a slow canter, 6 horses were cantered 3,200 m for 3 weeks, and then increased to 4,800 m for 5 weeks (Group L), while the other 5 horses were cantered 800 m throughout the same training period (Group S). Each horse performed an incremental exercise test on a treadmill before (Pre-test) and after (Post-test) the training period. In the Pre-test, there were no significant difference between Group L and Group S regarding VO₂peak (152.6±13.2 ml/kg/min in Group L vs 149.9±17.1 ml/kg/min in Group S) or the slope of the regression line for VO₂ and running speed. Peak heart rates (HRpeak; 221.8±6.7 beats/min in Group L vs 225.6±7.7 beats/min in Group S), and peak blood lactate concentrations (LApeak; 14.4±3.3 mmol/l in Group L vs 13.0±3.5 mmol/l in Group S) were not significantly different between the two groups. In the Post-test, there were no significant differences between groups in VO₂peak (156.1±11.5 ml/kg/min vs 152.6±5.4 ml/kg/min in Groups L and S, respectively), slope for VO₂ and speed, HRpeak (224.0±6.4 beats/min vs 227.2±6.8 beats/min in Group L and Group S, respectively), or LApeak (15.2±3.2 mmol/l in Group L vs 13.6±3.1 mmol/l in Group S). These results indicate that, in the case of low intensity cantering of 8-week training, differences in training distance do not significantly affect the degree of improvement in aerobic work capacity in the young Thoroughbred horse.

## Relevance to finishing-position (着順) prediction

Feature family **C (exercise-physiology/fitness)**. This study is a landmark null result for the most common early training intervention applied to JRA 2-year-olds: long slow distance (LSD) cantering. The finding that tripling canter distance (800 m vs. 4,800 m) produces no significant difference in VO₂peak, HRpeak, or blood lactate trajectory means that publicly available training-distance data (e.g., track log entries in JRA morning workout records) cannot be used as a direct aerobic fitness proxy unless the intensity (speed) is also known.

The practical implication for feature engineering: a simple "total cantering meters in past N weeks" feature derived from training logs is a weak predictor of aerobic fitness at the 2-year-old stage. The intensity component (speed at which the canter was performed — approximately 400 m/min in both groups here, corresponding to ~55–60% of HRpeak) is what matters. Where training speed data are available, features based on high-speed work fraction are more informative than volume alone.

The one significant post-training difference was in STEPmax (treadmill performance steps): Group L scored 9.4±0.3 vs Group S 8.6±0.4 (p<0.05), suggesting that higher training volume may improve musculoskeletal/structural fitness and overall performance capacity even when aerobic indices are unchanged. This is consistent with the authors' suggestion that longer cantering strengthens muscle, bone, tendon, and ligament, which is relevant to soundness and race-completion features.

## Background & objective

LSD training ("long slow distance") is standard practice for Japanese 2-year-old Thoroughbreds entering race preparation after breaking (typically January of the 2-year-old year). Erickson et al. (1987) and Sexton et al. (1987) named and studied this training method in Quarter horses but its effect on aerobic capacity in 2-year-old Thoroughbreds was uncharacterised. The study aimed to determine whether canter distance (volume) during LSD affects the degree of aerobic capacity improvement over 8 weeks.

## Materials & methods

**Subjects:** 11 two-year-old Thoroughbred horses. Broken in October of yearling year, pre-trained with trot and slow canter (≤800 m at 400 m/min) from October to December. From January (2-year-old year), assigned to:

- Group L (n=6): cantered 3,200 m for first 3 weeks, then 4,800 m for 5 weeks
- Group S (n=5): cantered 800 m throughout all 8 weeks
  Both groups trained 5 days/week at ~400 m/min on a dirt track. HR during training (week 8): 140–150 beats/min (~55–60% HRpeak) in both groups. Same feeding regime for both groups (estimated digestive energy: 31 Mcal). HR monitored with Polar PE-3000.

**Age and body weight** (Table 1):

- Group L: Pre: age 22.1±0.7 months, BW 444.3±22.3 kg; Post: age 24.2±0.7 months, BW 451.2±22.1 kg
- Group S: Pre: age 22.5±0.2 months, BW 465.4±20.1 kg; Post: age 24.5±0.2 months, BW 482.0±19.7 kg

**Exercise test:** Incremental treadmill test (Mustang-2200, Kagra, Switzerland) with 10% incline. Protocol: 5 min trotting at 4.0 m/s (warm-up), 5 min rest, then 1 min each at 1.8, 2.7, 3.4, 4.5, 5.4, 6.8, 9.0, 9.8, 10.8, 11.6 m/s. Terminated when horse could no longer maintain position.

**Measurements:**

- VO₂ and VCO₂: bias flow ventilatory system (Erickson et al. 1994 method), continuous during exercise
- HR: Polar monitor, calculated from last 30 s of each step
- Blood lactate (LA): jugular catheter, last 10 s each step; YSI-1500 Sport analyser
- PCV: microhaematocrit method
- STEPmax: number of completed steps + decimal fraction of time in final step

**Statistics:** Student t-test; data as mean ± SD. Asymptotic data from final 2–3 steps excluded from regression.

## Results (detailed — reproduce ALL numbers)

### Pre-test aerobic parameters (no significant intergroup differences)

| Parameter             | Group L      | Group S      |
| --------------------- | ------------ | ------------ |
| VO₂ peak (ml/kg/min)  | 152.6 ± 13.2 | 149.9 ± 17.1 |
| VCO₂ peak (ml/kg/min) | 184.4 ± 20.1 | 177.6 ± 9.1  |
| HRpeak (beats/min)    | 221.8 ± 6.7  | 225.6 ± 7.7  |
| LApeak (mmol/l)       | 14.4 ± 3.3   | 13.0 ± 3.5   |
| PCVpeak (%)           | 55.8 ± 2.7   | 54.8 ± 1.9   |
| STEPmax               | 8.8 ± 0.6    | 8.6 ± 0.8    |
| VO₂/speed slope       | 14.16        | 14.27        |
| HR/speed slope        | 14.23        | 14.38        |

### Post-test aerobic parameters (no significant intergroup differences except STEPmax)

| Parameter             | Group L       | Group S       | Significant? |
| --------------------- | ------------- | ------------- | ------------ |
| VO₂ peak (ml/kg/min)  | 156.1 ± 11.5  | 152.6 ± 5.4   | No           |
| VCO₂ peak (ml/kg/min) | 198.7 ± 11.6  | 189.6 ± 11.0  | No           |
| HRpeak (beats/min)    | 224.0 ± 6.4   | 227.2 ± 6.8   | No           |
| LApeak (mmol/l)       | 15.2 ± 3.2    | 13.6 ± 3.1    | No           |
| PCVpeak (%)           | 59.4 ± 3.0    | 57.6 ± 1.6    | No           |
| **STEPmax**           | **9.4 ± 0.3** | **8.6 ± 0.4** | **P<0.05**   |
| VO₂/speed slope       | 14.55         | 15.05         | No           |
| HR/speed slope        | 13.77         | 15.03         | No           |

### Recovery parameters (Table 2, mean ± SD, no intergroup differences)

| Parameter       | Time   | Group L Pre | Group S Pre | Group L Post | Group S Post |
| --------------- | ------ | ----------- | ----------- | ------------ | ------------ |
| VO₂ (ml/kg/min) | 1 min  | 45.4±19.6   | 47.9±22.8   | 43.9±4.0     | 39.5±6.5     |
| VO₂ (ml/kg/min) | 5 min  | 34.8±3.7    | 34.9±3.8    | 37.2±2.2     | 35.8±3.4     |
| VO₂ (ml/kg/min) | 15 min | 30.0±3.2    | 29.0±2.2    | 28.5±2.0     | 30.8±3.0     |
| HR (beats/min)  | 1 min  | 140.3±11.6  | 146.8±18.8  | 144.2±8.4    | 150.2±17.0   |
| HR (beats/min)  | 5 min  | 123.8±7.1   | 126.6±13.4  | 124.0±7.8    | 132.6±15.8   |
| LA (mmol/l)     | 1 min  | 14.0±3.0    | 12.7±3.0    | 14.7±2.9     | 13.0±2.8     |
| LA (mmol/l)     | 5 min  | 14.1±3.7    | 12.8±3.7    | 15.1±3.3     | 13.3±3.6     |
| PCV (%)         | 1 min  | 55.5±2.8    | 54.7±1.7    | 58.4±2.4     | 57.2±1.7     |

### Overall training effect (both groups combined, observed trends)

Both groups showed slight VO₂peak increases from Pre to Post (Group L: 152.6 → 156.1; Group S: 149.9 → 152.6 ml/kg/min) but neither change reached statistical significance. The 8-week LSD training produced minimal aerobic improvement overall, likely because pre-training (post-breaking exercise) had already captured the early adaptation window (Knight et al. 1991 found beneficial effects within 2 weeks of low-intensity training, with little additional improvement thereafter).

## Discussion & interpretation

The training intensity in both groups (HR 140–150 beats/min, ~55–60% HRpeak) is consistent with previously described LSD protocols for Quarter horses (Sexton et al. 1987). The absence of an aerobic advantage for Group L despite 6× greater canter distance strongly implies that, at this intensity level, additional distance provides no further aerobic stimulus. The suggestion is that aerobic capacity improvements are intensity-gated, not volume-gated, in young Thoroughbreds. However, Group L did achieve higher STEPmax, suggesting that structural (musculoskeletal) adaptations to volume are separable from cardiorespiratory adaptations.

The authors note that growth effects (these were 22–24 month olds) confound interpretation, and that Kubo et al. (unpublished) found similar age-matched results in field testing, supporting the generalisability of the null aerobic effect.

## Limitations

- Very small n (6 and 5 horses); inadequate statistical power to detect modest aerobic differences
- No randomisation described for group assignment
- Training intensity was held constant at ~400 m/min; the study only tests volume at fixed intensity, not intensity-volume interactions
- Breaking exercise prior to the study may have induced prior aerobic adaptation, compressing the training window
- Growth effects are confounded with training effects at this developmental stage
- Only JRA 2-year-olds; results may not generalise to older or NAR horses

## Feature-engineering notes for the model

- `training_volume_meters` (e.g., total canter meters in past 4 weeks from morning workout records) — expected effect: **weak** for aerobic fitness when intensity is unknown; this paper is a caution against volume-only training features
- `training_speed_high_fraction` — fraction of training sessions at speeds ≥9 m/s or equivalent sprint proportion — expected effect: stronger predictor of aerobic fitness state than volume — data availability: limited in public JRA training records
- `days_since_last_race` — proxy for detraining risk — expected effect: negative for fitness (longer gap → fitness decay) — data availability: standard feature derivable from race history
- `age_months_at_debut` — approximate developmental stage at first race — expected effect: horses debuting at 24–26 months (typical 2-year-old in Japan) had ~same aerobic capacity regardless of pre-training volume, implying debut fitness is largely intrinsic at this stage
- **Do NOT use:** raw training distance as a standalone aerobic proxy without speed context; this paper demonstrates null aerobic effect of 6× distance difference at constant intensity

## Key references / follow-up leads

- Erickson H.H., Sexton W.L., Erickson B.K., and Coffman J.R. 1987. Cardiopulmonary responses to exercise and detraining in the Quarter horse. pp. 41–51, _Equine Exercise Physiology 2_ (Gillespie & Robinson eds.), ICEEP, Davis CA. (LSD method in Quarter horses)
- Sexton W.L., Erickson H.H., and Coffman J.R. 1987. Cardiopulmonary and metabolic responses to exercise in the Quarter horse: Effect of training. pp. 77–91, _Equine Exercise Physiology 2_. (Quarter horse LSD training effects)
- Knight P.K., Sinha A.K., and Rose R.J. 1991. Effects of training intensity on maximum oxygen uptake. pp. 77–82, _Equine Exercise Physiology 3_ (Persson, Lindholm, Jeffcott eds.), ICEEP, Uppsala. (intensity effects on VO₂max — shows early plateau of low-intensity training effect)
- Seeherman H.J. and Morris E.A. 1990. Methodology and repeatability of a standardized treadmill exercise test for clinical evaluation of fitness in horses. _Equine Vet. J._ Suppl. 9: 20–25. (STEPmax methodology)
- Erickson B.K., Seaman J., Kubo K., Hiraga A., Kai M., Yamaya Y., and Wagner P.D. 1994. Mechanism of reduction in alveolar-arterial PO₂ difference by helium breathing in the exercising horse. _J. Appl. Physiol._ 76: 2794–2801. (respiratory gas measurement method used in this study)
- Persson S.G.B., Essen-Gustavsson B., Lindholm A., McMiken D., and Thornton J.R. 1983. Cardiorespiratory and metabolic effects of training of Standardbred yearling. pp. 458–469, _Equine Exercise Physiology_ (Snow, Persson, Rose eds.), Granta Editions, Cambridge. (yearling training reference)
