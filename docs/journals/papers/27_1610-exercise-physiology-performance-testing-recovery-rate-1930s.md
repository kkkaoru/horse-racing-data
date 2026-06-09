# Studies on exercise physiology and performance testing of racehorses performed in Japan during the 1930s using recovery rate as an index

## Metadata

| Field                          | Value                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 27(4): 131–142, 2016                                                                                                           |
| docid                          | `27_1610`                                                                                                                                     |
| Article type                   | Review Article                                                                                                                                |
| Authors                        | Atsushi HIRAGA, Shigeru SUGANO                                                                                                                |
| Affiliations                   | Hidaka Training and Research Center, Japan Racing Association, Hokkaido 057-0171; Professor Emeritus, The University of Tokyo, Tokyo 180-0004 |
| Received / Accepted / Released | (dates not stated in PDF; accepted 2016)                                                                                                      |
| Keywords                       | equine exercise physiology, performance testing, racehorse, recovery rate, training                                                           |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/27/4/27_1610/_pdf/-char/en                                                                           |

## Abstract (verbatim)

> The history of research on the exercise physiology of racehorses in Japan dates back to the 1930s. A research report entitled "Studies on exercise physiology and performance testing of the racehorse", published in 1933 by Shigeo Matsuba and Torao Shimamura of The University of Tokyo, was epoch-making and the most important study in the history of equine exercise physiology in Japan. Research results were reported from 92 Thoroughbred racehorses in a large-scale project during the period of 1928 to 1932 at the Shimofusa Imperial Farm and the Koiwai Farm, which were the two greatest racehorse farms at that time. A total of 20 physiological variables were measured to evaluate the fitness of Thoroughbred racehorses before exercise (Pre), just after exercise (Post), 1 hr after exercise (1 hr), 2 hr after exercise (2 hr), and 3 hr after exercise (3 hr) in order to calculate their recovery rates as an index of fitness and performance. The percentage of the Pre value at 1 hr, 2 hr, and 3 hr was calculated. When the percentage of a variable reached 95–105% of the Pre value, the variable was considered to be recovered. The percentage of the total number of variables that were recovered for each time period was calculated, and an overall average was calculated from them; Matsuba and Shimamura proposed calling this overall average the "recovery rate", which could then be applied to evaluate each horse. The effects of training on racehorses were subsequently evaluated by measuring the various physiological variables and the recovery rate.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **C (exercise physiology/fitness)** and **G (statistical modelling)**. It is the companion to 27_1604, covering the landmark 1933 Matsuba & Shimamura study in full detail, including the 20 physiological variables measured, the specific recovery-rate calculation method, the quantitative data from 92 Thoroughbreds across 5 measurement time points, and the correlation between recovery rate and winning percentage — making this the most explicit historical precedent for the concept of composite fitness scores predicting race performance.

The recovery rate concept — the percentage of 20 physiological variables returning to pre-exercise baseline at 1 hr, 2 hr, and 3 hr post-exercise, averaged into a single score — is a 1933-era multi-biomarker fitness index that directly anticipates modern multi-feature models. Crucially, Matsuba & Shimamura demonstrated that **recovery rate was positively correlated with winning percentage** across 25 horses with traceable racing careers — the first documented quantitative link between a physiological fitness index and actual race performance in Japanese Thoroughbreds.

For the current pipeline, the recovery-rate concept translates to:

1. **Post-gallop HR recovery rate** (time to return to baseline HR after a timed gallop) — the most accessible modern analogue of the 1933 recovery rate
2. **Post-exercise blood lactate clearance time** — time for lactate to return to baseline after standardised effort
3. **CK/AST normalisation speed** — time for muscle enzyme elevation (post-race or post-gallop) to return to pre-exercise range
4. **Composite recovery score** — a weighted combination of the above, analogous to the original RR index

The study also provides the exact definition of recovery rate (95–105% of Pre value = "recovered") and quantitative examples of the recovery calculation for individual horses (Table 1, 5, from the review), enabling direct operationalisation.

## Background & objective

This review focuses on the 1933 Matsuba & Shimamura report ("Studies on exercise physiology and performance testing of the racehorse") — a 294-page book — which Matsuba presented at the International Congress of Physiology in Moscow in 1930, attracting worldwide attention. The recovery rate concept was so influential it spawned subsequent studies of warhorses (Ogura 1942), racehorses in training (Matsuba 1939), and draft horses (post-WWII). This companion paper to 27_1604 provides the full detail of the original study methodology and results, plus the subsequent training-effect studies from the same group.

The broader context: a major shift from racehorse to warhorse to draft-horse research occurred across Japan in the 1930s–1960s, each period building on Matsuba's framework. After WWII, draft horse research dominated (1950s–1960s); post-war JRA racing resumed and exercise physiology of racehorses revived.

## Materials & methods

**Primary study (Matsuba & Shimamura 1928–1932, published 1933):**

- Subjects: 92 Thoroughbred racehorses (71 two-year-olds, 9 three-year-olds, 12 four-year-olds; 49 colts, 28 fillies, 15 geldings) at Shimofusa Imperial Farm and Koiwai Farm (2 greatest Japanese racehorse farms of the era)
- Duration: large-scale project, 1928–1932; 9 surveys over 5 years; 61 total survey days
- Exercise protocol (standardised from 3rd year onward, surveys 5–9): Trot 800 m → Canter 1,600 m → Gallop 800 m
- Measurement time points: Pre (before exercise), Post (just after exercise), 1 hr, 2 hr, 3 hr after exercise
- Variables measured: 20 physiological variables (14 measured in year 1, all 20 in year 4, 10 in year 5)

**The 20 physiological variables:**

| Category                 | Variables                                                                                                                                                                                                                                                                                                   |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Clinical (variables 1–7) | Body temperature; respiratory frequency; pulse rate; blood pressure; cardiac function (auscultation); state of conjunctiva; state of perspiration                                                                                                                                                           |
| Blood (variables 8–20)   | Specific gravity of blood; erythrocyte count; leukocyte count; haemoglobin concentration; packed cell volume (PCV); viscosity of blood; blood glucose concentration; plasma CO₂ concentration; plasma pH; serum freezing point depression; serum refractive index; erythrocyte fragility; serum spectrogram |

**Recovery rate calculation:**

1. For each variable at 1 hr, 2 hr, 3 hr: compute percentage of Pre value
2. If percentage reaches 95–105% of Pre → variable is "recovered"
3. Recovery rate at time T = (number of recovered variables / total variables) × 100
4. Overall recovery rate (RR) = (RR@1hr + RR@2hr + RR@3hr) / 3
5. For body temperature and blood specific gravity: raw value minus offset (30°C; 1.000) used before percentage calculation (to handle small absolute changes)

**Training effect study (Matsuba 1939, published in 5 papers in J. Central Society for Veterinary Medicine 1938):**

- Conducted at Nakayama Race Club (now Nakayama Racecourse, JRA) over 4 years (1933–1934, 1936–1937)
- Commissioned by Teikoku Keiba Kyokai (Imperial Racing Association)
- 492-page book (A5 size)
- Evaluated changes in blood characteristics and recovery rate with training

## Results (detailed — reproduce ALL numbers)

### Key quantitative findings from the 7th survey (July 29, 1931, Koiwai Farm)

**Pulse rate before and during recovery (Table 1 of review, individual horses):**

| Horse name      | Pre | Post | 1 hr | 2 hr | 3 hr | Pre% | Post% | 1hr%  | 2hr%  | 3hr%  |
| --------------- | --- | ---- | ---- | ---- | ---- | ---- | ----- | ----- | ----- | ----- |
| Dai 4 Rachidia  | 34  | 106  | 40   | 34   | 34   | 100  | 310.3 | 117.7 | 100.0 | 100.0 |
| Shining star    | 33  | 98   | 41   | 35   | 36   | 100  | 297.0 | 123.4 | 106.1 | 109.1 |
| Dai 6 Shian Mor | 34  | 112  | 39   | 36   | 37   | 100  | 329.4 | 109.5 | 105.9 | 108.8 |
| Dai 4 Shian Mor | 36  | 116  | 42   | 38   | 36   | 100  | 322.2 | 113.8 | 105.5 | 100.0 |
| Dai 2 Shian Mor | 34  | 94   | 40   | 39   | 37   | 100  | 276.4 | 117.7 | 114.7 | 108.8 |
| Dai 3 Shian Mor | 30  | 102  | 37   | 34   | 34   | 100  | 340.0 | 123.3 | 113.3 | 113.3 |
| Dai 5 Shian Mor | 34  | 104  | 44   | 36   | 34   | 100  | 305.8 | 129.4 | 105.9 | 100.0 |
| Dai 7 Shian Mor | 36  | 98   | 36   | 32   | 32   | 100  | 272.2 | 100.0 | 88.1  | 88.1  |
| Dai 19 Koiwai   | 30  | 94   | 37   | 33   | 30   | 100  | 313.3 | 123.3 | 110.0 | 100.0 |

(Pulse rate in beats/min. Recovered = 95–105% of Pre value.)

**Mean values across all horses (7th survey, averaged from 7 surveys' tables as graphed by review authors):**

- Body temperature: Pre 38.1°C → Post 39.5°C → returns to Pre within 2 hr
- Respiratory frequency: Pre 12.4 br/min → Post 71.3 br/min → 1 hr 21.4 br/min → 3 hr 14.7 br/min
- Pulse rate: Pre 33.4 bpm → Post 102.7 bpm → 1 hr 39.6 bpm → 3 hr 34.4 bpm
- Erythrocyte count: increased post-exercise; returned toward baseline by 3 hr (exact values from Figure 6 of review)
- Haemoglobin (Hb): Pre 14.2 g/dl → Post 20.0 g/dl → 1 hr 16.0 g/dl → 3 hr 14.6 g/dl
- Blood viscosity (relative to water = 1.000): rest 5.0–6.0; Pre 6.4 → Post 10.6 → returned to Pre by 1 hr
- Blood pressure (systolic, radial artery): Pre 156 mmHg → Post 238 mmHg → 1 hr 176 mmHg → 3 hr 158 mmHg

### Example recovery rate calculation (horse "Yamaougi", 5th survey March 1930, Table 3 of review):

Recovery at 1 hr: 3/11 variables recovered = **RR@1hr = 0.27 (27%)**
Recovery at 2 hr: 7/11 variables recovered = **RR@2hr = 0.64 (64%)**
Recovery at 3 hr: 5/11 variables recovered = **RR@3hr = 0.45 (45%)**
Overall RR = (0.27 + 0.64 + 0.45) / 3 = **0.45 (45%)**

### Summary of 5th survey results (Table 5 of review)

Multiple horses showing variability in recovery rates, with fitter horses (those with better racing records) showing higher recovery rates.

### Performance correlation (25 horses with full career records)

**Key result:** Recovery rate was positively correlated with winning percentage (Fig. 8 of review). However, Matsuba & Shimamura cautioned: recovery rate alone cannot perfectly predict performance because numerous other factors influence racing outcome. They proposed both talent AND training as important factors.

### "Pulse pressure ratio" (d/p ratio)

Additional derived index proposed by Matsuba & Shimamura:

- d = blood pressure difference (Pre → Post)
- p = pulse rate difference (Pre → Post)
- d/p ratio tends to increase as training advances (fitter horses: moderate HR rise but larger BP rise)
- Named "pulse pressure ratio"; considered a non-invasive fitness index alongside recovery rate

### Training effect study (Matsuba 1939, Nakayama Race Club)

Series of experiments on racehorses already in active training (not just farm horses). Key finding: blood characteristics and recovery rate improved with training — validating the recovery rate as an index sensitive to training-induced fitness gains.

### Post-WWII draft horse studies (1950s–1960s)

Multiple researchers (unnamed in review summary) confirmed similar physiological adaptation patterns during conditioning in draft horses, validating the generalisability of the recovery-rate framework across different horse types.

## Discussion & interpretation

The review provides historical context explaining why Matsuba & Shimamura's work was considered epoch-making:

1. **Comprehensive multi-variable approach:** Previous exercise physiology studies measured only 1–2 variables. Measuring 20 variables simultaneously — clinical, haematological, biochemical — provided a holistic view of the exercise response.

2. **Recovery rate as a composite score:** The aggregation of variable-specific recovery rates into a single index prefigures modern approaches to multi-biomarker fitness scoring. The 95–105% "recovered" threshold anticipates modern reference interval methodology.

3. **Individual vs. population performance:** The correlation between recovery rate and winning percentage across 25 horses is the earliest documented quantitative link between exercise physiology and racing performance in Japan.

4. **Limitations acknowledged by Matsuba:** The recovery rate index was acknowledged to be incomplete — it cannot capture all factors determining race outcome (tactics, jockey skill, draw, etc.). This honest caveat by the 1930s authors is entirely consistent with the pipeline's current approach: fitness features are one component of a multi-factor model.

The d/p ratio (pulse pressure ratio) is particularly interesting as it captures the ratio of cardiovascular response to exercise load — an early operationalisation of what modern physiology calls "cardiac efficiency." Horses with high d/p show larger BP responses relative to HR increase, suggesting stronger cardiac output per beat.

## Limitations

- Primary data from 1928–1932 at two farms using late-19th/early-20th century physiological measurement methods (auscultation, colorimetry, sedimentation).
- Horses were 2–4yo at Shimofusa and Koiwai farms, not yet raced under competitive conditions; the training effect studies (1933–1937) used Nakayama Race Club horses but results are summarised rather than tabulated in full.
- The correlation between recovery rate and winning percentage (n=25 horses) is the key performance-link result but sample size is very small.
- Matsuba explicitly warned against using recovery rate as the sole predictor, acknowledging multifactorial causation.
- Survey-era physiological variables include several that are no longer measured (serum spectrogram, serum refractive index, blood specific gravity); modern equivalents are not identical.

## Feature-engineering notes for the model

- `post_exercise_hr_recovery_rate` — percentage of HR variables returned to 95–105% of pre-exercise value at 1 hr post-exercise — source: post-gallop HR monitoring (JRA training centre records if available) — expected effect: higher % → fitter horse → positive performance predictor; direct operationalisation of Matsuba's recovery rate concept for HR specifically
- `hr_pre_to_post_ratio` — post-exercise HR / pre-exercise HR — source: HR monitoring — expected effect: lower ratio at same exercise intensity → better cardiac efficiency; values from 1930s data: ~2.3× (102.7/33.4 = 3.1 at post-exercise; returning to 1.2× at 1 hr)
- `blood_pressure_pulse_ratio_dp` — (post_BP − pre_BP) / (post_PR − pre_PR) — source: if BP and HR both measured — expected effect: higher d/p → better cardiac efficiency; Matsuba's "pulse pressure ratio" — data availability very limited
- `post_exercise_hb_recovery` — Hb normalisation rate post-exercise — source: training centre bloodwork — expected effect: Hb from 14.2 → 20.0 → 14.6 g/dl typical; faster return = better cardiovascular recovery
- `composite_fitness_score` — custom: weighted combination of HR recovery, lactate clearance, CK normalisation — source: training centre monitoring — expected effect: positive correlation with winning probability (analogous to original recovery rate vs. winning %); **the key multi-feature fitness index recommended by this paper**
- `training_load_proxy` — cumulative race starts + gallop sessions in prior 60 days — source: race records + training logs — expected effect: moderate training load → improved recovery rate; very high load → reduced recovery rate (overtraining); captures the training effect on the fitness index
- `days_since_last_race` — continuous — source: race records — expected effect: 30–60 days since last race = optimal recovery window; very short or very long intervals deviate from peak condition

## Key references / follow-up leads

- Matsuba S, Shimamura T. 1933. Studies on exercise physiology and performance testing of the racehorse. Book, 294 pages. University of Tokyo. (the primary source for this entire review)
- Matsuba S. 1939. Studies on the performance and training of the racehorse. Book, 492 pages. Published as 5 papers in J. Central Society for Veterinary Medicine 1938. (training effect study)
- Ogura. 1942. Performance testing of the horse. 100-page report, Imperial University of Taipei. (warhorse application of recovery rate concept)
- Kimata (student of Matsuba). [Multiple interpretive articles on exercise physiology in Japanese veterinary journals, 1930s–40s, describing recovery rate and training effects in detail]
- Companion paper: Hiraga A, Sugano S. 2016. 27_1604, J. Equine Sci. 27(2): 37–48. (respiration, HR, blood characteristics — same authors, companion review covering 1940s–1970s)
- Tatsumi et al. [Showed close relationship between O₂ consumption by Douglas bag method and post-exercise pulse rate — validating pulse rate as proxy for exercise intensity]
- Nomura et al. 1964. First field ECG in horses (cited in 27_1604) — showed direct HR measurement became possible, superseding pulse rate auscultation
