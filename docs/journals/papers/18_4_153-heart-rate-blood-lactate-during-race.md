# Heart Rates and Blood Lactate Response in Thoroughbred Horses during a Race

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 18(4): 153–160, 2007                                                                                                                                                                                                                                       |
| docid                          | `18_4_153`                                                                                                                                                                                                                                                                |
| Article type                   | Full Paper                                                                                                                                                                                                                                                                |
| Authors                        | Kazutaka MUKAI, Toshiyuki TAKAHASHI, Daisuke ETO, Hajime OHMURA, Hirokazu TSUBONE, Atsushi HIRAGA                                                                                                                                                                         |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya, Tochigi 320-0856; Department of Comparative Pathophysiology, Graduate School of Agricultural and Life Sciences, The University of Tokyo, 1–1–1 Yayoi, Bunkyo-ku, Tokyo 113-8657, Japan |
| Received / Accepted / Released | Accepted September 28, 2007                                                                                                                                                                                                                                               |
| Keywords                       | exercise, heart rate, horse, lactate, race                                                                                                                                                                                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/18/4/18_4_153/_pdf/-char/en                                                                                                                                                                                                      |

## Abstract (verbatim)

> We measured the heart rate before, during and after a 1200-m race, and blood lactate concentration at 10 min after a race in 23 Thoroughbred horses. The heart rate increased when horses were walking through the underground passage and after the jockeys mounted them in the paddock, even though there was no increase in exercise intensity. During the canter from the exit of the underground passage to the starting stalls, the heart rate increased to a peak value of 194.0 ± 2.0 (mean ± SE) beat/min, which was equivalent to 91.1 ± 0.8% of the maximal racing heart rate. In the starting stalls immediately before the start of the race, the heart rate was 171.4 ± 5.3 beat/min. The peak value of the heart rate (HRpeak) during the race was 213.6 ± 1.7 beat/min, and the time taken to reach 95% of HRpeak was 12.4 ± 1.8 sec. The blood lactate concentration after the race was 22.5 ± 0.6 mmol/l. The mean speed of the horses was 15.9 ± 0.0 m/s, which was similar to those of 3-year-olds in maiden races. These data could be useful for planning training strategies for Thoroughbred horses.

## Relevance to finishing-position (着順) prediction

Feature family: **C (exercise-physiology / fitness)**. This is one of the most directly applicable papers in the collection: it provides quantitative ground-truth exercise physiology benchmarks from actual simulated JRA races (Nakayama Racecourse, 1,200 m dirt). The data establish reference ranges for heart rate at each pre-race stage, peak HR during the race, the time to reach 95% of HRpeak, and post-race blood lactate — all key indicators of cardiorespiratory fitness and aerobic/anaerobic capacity.

Key engineered features motivated by this paper: (1) **V200** (speed at HR = 200 bpm, a standard field fitness index) — if JRA training records include HR-monitoring data, V200 quantifies aerobic fitness directly; (2) **post-race lactate** — a proxy for aerobic efficiency: horses producing lower lactate at the same speed have superior aerobic metabolism; (3) **pre-race HR / arousal index** — elevated starting-stall HR (171 bpm) is documented here as a psychological arousal signal that Hada et al. (2003) link to enhanced performance (reduced lactate accumulation via increased aerobic contribution). If pre-race paddock behaviour is observable, it encodes this arousal signal.

Interactions with pipeline features: race distance (1,200 m sprint confirms >70% aerobic + heavy anaerobic contribution), surface (dirt), sex (8M/1G/14F in sample), 馬体重 (451.9 ± 6.6 kg average), running-style/脚質 (arousal-related HR differences between front-runners and come-from-behind horses), muscle fibre composition (type II >80% in gluteus medius — explains the slow VO2 component and high lactate).

## Background & objective

Understanding physiological demands of actual Thoroughbred races is essential for training strategy but has been technically difficult. More than 70% of energy in 1–3 min intense exercise in horses comes from the aerobic pathway (Eaton et al. 1995). VO2max is the gold-standard aerobic capacity index but requires laboratory treadmill testing that trainers rarely permit. Heart rate correlates with oxygen consumption and can be measured in field conditions (Kobayashi et al. 1999 V200 methodology). Psychological state and arousal may influence athletic performance (inverted-U hypothesis); pre-race HR elevation provides a measurable proxy. Blood lactate after a race quantifies anaerobic contribution. The objective was to simultaneously characterise HR throughout the entire pre-race/race/post-race sequence and post-race lactate under actual JRA race conditions.

## Materials & methods

**Subjects:** 23 Thoroughbred racehorses (8 males, 1 gelding, 14 females; age 4.0 ± 0.2 yr, body weight 451.9 ± 6.6 kg). Trained 6 days/week on a dirt track at JRA Horse Racing School. All confirmed clinically healthy and sound at veterinary inspection before the race.

**Race protocol:** Three simulated races of 1,200 m on a dirt surface at Nakayama Racecourse, JRA (8, 10, and 5 horses per race, clockwise direction). Horses saddled with HR monitor ~50 min before start. Then walked 15 min around the stable → underground passage → 30-min paddock walk (15 min groom-led, 15 min jockey-mounted) → underground passage again with jockey → canter warm-up to starting stalls (odd-numbered horses loaded first, then even). Post-race: unsaddled, walked 10 min, then blood sampled.

**Heart rate measurement:** Polar S810 monitor, 5-s interval averaging. One electrode under the saddle sponge; transmitter and receiver on the elastic girth.

**Blood lactate:** Jugular vein sample at 10 min post-race, collected into sodium fluoride + EDTA vacuum tubes, kept on ice. Measured with YSI-2300 STAT Plus automatic lactate analyser (Yellow Springs Instruments, USA).

**Race timing:** Automatic infrared timing device + video analysis for speed.

**Statistical analysis:** Repeated measures ANOVA on HR stage data; pairwise Tukey's test. P < 0.05 threshold. JMP v.5.0.1a.

Note: HR data from 2 horses not recorded from onset; 2 others lost data after gathering at the starting point.

## Results (detailed — reproduce ALL numbers)

**Horse characteristics (Table 1a):**

| Measurement                      | Value (mean ± SE) |
| -------------------------------- | ----------------- |
| Race time                        | 75.6 ± 0.2 s      |
| Running speed                    | 15.9 ± 0.0 m/s    |
| Body weight                      | 451.9 ± 6.6 kg    |
| Blood lactate (10 min post-race) | 22.5 ± 0.6 mmol/l |
| t95% (time to reach 95% HRpeak)  | 12.4 ± 1.8 s      |

**Heart rate at each stage (Table 1b):**

| Stage                                       | Heart rate (beat/min, mean ± SE) | Significance group |
| ------------------------------------------- | -------------------------------- | ------------------ |
| Walking around stable                       | 73.3 ± 2.9                       | a                  |
| Underground passage (groom-led)             | 88.0 ± 4.2                       | b                  |
| Paddock (groom-led)                         | 71.1 ± 2.8                       | a                  |
| Paddock with jockey mounted                 | 97.3 ± 4.6                       | b                  |
| Underground passage with jockey             | 114.3 ± 5.4                      | c                  |
| Around starting stalls (walking)            | 97.7 ± 3.5                       | b                  |
| In starting stalls (just before start)      | 171.4 ± 5.3                      | d                  |
| Galloping to starting stalls (warm-up peak) | 194.0 ± 2.0                      | —                  |
| During race (HRpeak)                        | 213.6 ± 1.7                      | —                  |

Different letter superscripts indicate significant differences between values (P < 0.05).

**Derived values:**

- Warm-up peak as % of HRpeak: 91.1 ± 0.8%
- Mean race speed (15.9 m/s) is similar to 3-year-old maiden races at Nakayama Racecourse
- Post-race lactate (22.5 mmol/l whole blood) ≈ 30% lower than plasma values in prior studies, consistent with known whole-blood/plasma lactate ratio (Poso 2002; Rose & Hodgson 1994)
- t95% HRpeak = 12.4 ± 1.8 s vs. t95% VO2max ≈ 60 s (Rose et al. 1988); HR rises much faster than VO2

**Three distinct HR patterns during the race (Figs. 3a–3c):**

1. **Fig. 3a:** HR rises rapidly to peak, remains stable throughout race — interpreted as constant running speed.
2. **Fig. 3b:** HR constant in early stages, gradually increases in latter periods — consistent with VO2 slow component during heavy exercise causing sustained HR rise.
3. **Fig. 3c:** HR reaches peak then gradually decreases throughout race — interpreted as either decreasing running speed or cardiac overshoot (maximal HR reached in ~30–45 s can overshoot before plateauing, depending on temperament and warm-up; Persson 1967).

**Energy system contribution:** At 125% VO2max (equivalent to a ~1,000 m sprint), 70% of energy is aerobic (Eaton et al. 1992, 1995). The high post-race lactate (22.5 mmol/l) confirms heavy anaerobic contribution even in a 1,200 m sprint.

**Effect of jockey mounting on HR:** Walking HR with jockey (97.3 bpm) significantly higher than groom-led paddock HR (71.1 bpm), but Sloet van Oldruitenborgh-Oosterbaan et al. (1995) found that adding a 90 kg rider or 90 kg lead did NOT significantly affect HR during walking on a treadmill. Therefore the HR increase after mounting is attributed to psychological factors, not physical load.

**Warm-up intensity and recovery:** Peak warm-up HR 194.0 bpm = 91.1% HRpeak. Warm-up duration < 1 min, distance < 600 m. Post-warm-up recovery time ≈15 min before race; based on unpublished data, blood lactate from warm-up decreases to < 2 mmol/l in this 15-min recovery period — the warm-up is not expected to cause residual fatigue.

**Starting stall HR and performance link:** Krzywanek et al. (1970) reported lower pre-race stall HR; the higher stall HR in this study (171 bpm) suggests more excited horses. Hada et al. (2003) demonstrated that psychological stress before intense exercise reduced blood lactate accumulation and enhanced running performance in Thoroughbreds, suggesting increased aerobic contribution due to higher pre-exercise HR and haematocrit.

**Type II muscle fibre connection:** >80% type II fibres in the middle gluteal muscle of Thoroughbreds (Snow 1983; Eto et al. 2003, 2004; Yamano et al. 2002, 2005) are consistent with the VO2 slow component (Poole et al. 1994), explaining pattern 3b; and with the high anaerobic capacity producing 22.5 mmol/l lactate.

## Discussion & interpretation

The mean race time (75.6 ± 0.2 s) and speed (15.9 ± 0.0 m/s) match 3-year-old maiden races at Nakayama, confirming the races were representative of standard JRA quality. The pre-race HR elevation sequence (stable 73 → underground 88 → paddock 71 [calmer] → jockey 97 → passage with jockey 114 → stalls 171 bpm) documents a progressive arousal cascade driven by environmental novelty and anticipation. This arousal itself may prime aerobic pathways by increasing pre-race HR and haematocrit (Hada et al. 2003). The starting stall HR (171 bpm = 80.2% of HRpeak) is particularly significant: it represents a substantial cardiac pre-activation that shortens the cardiovascular "lag" at race start. The very rapid t95% HRpeak of 12.4 s (vs. ~60 s for VO2 to reach 95% of max) confirms that HR is not a real-time proxy for VO2 at race onset but quickly reaches maximum.

The three HR race patterns reflect genuine physiological variation in pace strategy, VO2 kinetics, and temperament. Trainers can use these data to identify horses whose HR pattern matches poor pacing (pattern 3c = early overshoot or speed fade) vs. controlled effort (3a = stable).

## Limitations

- Only 23 horses in 3 simulated races; not genuine competition racing.
- 1,200 m dirt only; no turf, no longer distances.
- All races at same venue (Nakayama); no between-venue or between-going variation.
- 2 of 23 horses had incomplete HR records.
- Post-race lactate measured at 10 min only; no multi-timepoint lactate kinetics.
- No individual-level finishing position data reported; this is an average physiology study, not a horse-level performance prediction study.
- Blood lactate in whole blood; plasma values would be ~30% higher, complicating comparison with some prior studies.

## Feature-engineering notes for the model

- `V200` — speed (m/s) at HR = 200 bpm; derived from training HR-speed records; standard aerobic fitness index — source: JRA training HR monitoring data — expected effect: strong positive for finishing position (higher V200 → better aerobic capacity) — availability: available at JRA training centres for monitored horses
- `HR_peak_race` — maximal HR during race; 213.6 ± 1.7 bpm is the reference for a standard maiden-quality 1,200 m dirt race; deviation from this range may indicate fitness issue — source: wearable HR monitors — availability: not in standard race databases
- `t95_HR` — time (seconds) to reach 95% of HRpeak; 12.4 ± 1.8 s benchmark; faster onset may indicate better pre-activation — source: HR monitor — availability: research only
- `post_race_lactate` — blood lactate at 10 min post-race; 22.5 ± 0.6 mmol/l is the reference for 1,200 m dirt sprint; horses producing lower lactate at same speed have superior aerobic efficiency — source: post-race blood test — availability: JRA research, not standard databases
- `pre_race_arousal_HR` — HR in starting stalls (171.4 ± 5.3 bpm reference); elevated arousal linked to enhanced performance (Hada et al. 2003); proxy: paddock behaviour score or jockey-reported demeanour — source: subjective observation or HR monitor — expected effect: moderate positive (up to a threshold; over-excitement negative)
- `distance_energy_system` — interaction term: distance × surface affects aerobic/anaerobic split; 1,200 m = ~70%+ aerobic despite heavy anaerobic contribution; model should include `distance × surface` interaction — source: race records — availability: standard
- **Do NOT use** HR features derived from post-race measures as race-entry predictors directly; they are outcome measures. Use them to construct training-window fitness indices via time-series aggregation.

## Key references / follow-up leads

- Evans, D.L., Rose, R.J. 1987. Maximum oxygen uptake in racehorses: changes with training state and prediction from submaximal cardiorespiratory measurements. _Equine Exercise Physiology 2_, ICEEP Publications. [V200 concept and VO2max in TBs]
- Hada, T., Onaka, T., Takahashi, T., Hiraga, A., Yagi, K. 2003. Effects of novelty stress on neuroendocrine activities and running performance in thoroughbred horses. _J. Neuroendocrinol._ 15: 638–648. [Psychological stress → reduced lactate → enhanced performance]
- Kobayashi, M., Kuribara, K., Amada, A. 1999. Application of V200 values for evaluation of training effects in the young thoroughbred under field conditions. _Equine Vet. J. Suppl._ 30: 159–162. [V200 field application]
- Mukai, K., Takahashi, T., Hada, T., Eto, D. et al. 2003. Influence of gender and racing performance on heart rates during submaximal exercise in Thoroughbred racehorses. _J. Equine Sci._ 14: 93–96. [Sex and performance effects on submaximal HR]
- Eaton, M.D., Evans, D.L., Hodgson, D.R., Rose, R.J. 1995. Maximal accumulated oxygen deficit in thoroughbred horses. _J. Appl. Physiol._ 78: 1564–1568. [Aerobic/anaerobic energy split in TBs]
- Krzywanek, H., Wittke, G., Bayer, A., Borman, P. 1970. The heart rates of Thoroughbred horses during a race. _Equine Vet. J._ 2: 115–117. [Earlier race HR study for comparison]
- Eto, D. et al. 2003. Effect of controlled exercise on middle gluteal muscle fibre composition in Thoroughbred foals. _Equine Vet. J._ 35: 676–680. [Type II fibre proportion in JRA TBs]
