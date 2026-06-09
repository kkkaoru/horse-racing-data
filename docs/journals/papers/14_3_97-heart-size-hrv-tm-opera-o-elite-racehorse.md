# Heart Size and Heart Rate Variability of the Top Earning Racehorse in Japan, T. M. Opera O

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 14(3): 97–100, 2003                                                                                                                                                                                                                                                                                                                                                  |
| docid                          | `14_3_97`                                                                                                                                                                                                                                                                                                                                                                           |
| Article type                   | Case Report                                                                                                                                                                                                                                                                                                                                                                         |
| Authors                        | Kazuhiro KAMIYA, Hajime OHMURA, Daisuke ETO, Kazutaka MUKAI, Shigeto USHIYA, Atsushi HIRAGA, Sadao YOKOTA                                                                                                                                                                                                                                                                           |
| Affiliations                   | Racehorse Clinic, Ritto Training Center, Japan Racing Association, 1028 Misono, Ritto-shi, Shiga 520-3085; Equine Science Division, Hidaka Training and Research Center, Japan Racing Association, 535–13 Aza-Nishicha, Urakawa-cho, Urakawa-gun, Hokkaido 057-0171; Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya-shi, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted July 30, 2003                                                                                                                                                                                                                                                                                                                                                              |
| Keywords                       | athletic performance, heart rate variability, left ventricular mass                                                                                                                                                                                                                                                                                                                 |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/14/3/14_3_97/_pdf/-char/en                                                                                                                                                                                                                                                                                                                 |

## Abstract (verbatim)

> T. M. Opera O is considered as one of the greatest racehorses in the history of horseracing in Japan. Only a very few data is available on circulatory function of elite Thoroughbred racehorses. Recently, we obtained data on circulatory parameters of T. M. Opera O. The left ventricular mass of T. M. Opera O, as measured on an echocardiogram, was 4.60 kg. The resting heart rate (HR) of T. M. Opera O was 25 beat/min. The low-frequency and high-frequency power, as determined by an analysis of HR variability, were 13,900 and 5,963 ms², respectively, which were considerably higher than those of the other racehorses. These results suggest that a large heart, formed by genetic factors and training, markedly enhanced the parasympathetic nervous activity and reduced the resting HR of T. M. Opera O. The data we obtained from T. M. Opera O is invaluable for understanding the physical fitness of elite Thoroughbreds and for further developing their athletic performance.

## Relevance to finishing-position (着順) prediction

Feature family: **C (exercise-physiology/fitness) with sub-theme of cardiac structure and autonomic function as elite performance indicators**. This case report provides the most extreme upper-bound cardiac measurements ever reported for a JRA racehorse — T. M. Opera O, Japan's all-time top earner (¥1,835,189,000 career earnings; 14 wins in 26 starts including 7 G1 races). The paper establishes reference values for elite vs average cardiac parameters:

| Parameter        | T. M. Opera O | Other racehorses (n=15; mean ± SE)                    | Elite ratio |
| ---------------- | ------------- | ----------------------------------------------------- | ----------- |
| Resting HR (bpm) | 25            | 30.3 ± 1.2                                            | −17%        |
| LV mass (kg)     | 4.60          | 3.37 ± 0.34 (2-yr-olds); 3.366 ± 0.822 (Young et al.) | +37%        |
| LF power (ms²)   | 13,900        | 3,819 ± 459                                           | +264%       |
| HF power (ms²)   | 5,963         | 1,248 ± 321                                           | +378%       |
| LF:HF ratio      | 2.3           | 4.1 ± 0.5                                             | −44%        |

These measurements establish three actionable cardiac features for the model pipeline:

1. **Left ventricular mass (LV mass)**: Young et al. (2002) showed a positive correlation between LV mass and VO₂max in Thoroughbreds — the horse with the largest heart has the highest aerobic capacity. T. M. Opera O's LV mass of 4.60 kg is 37% above the 3.37 kg mean, corresponding to exceptional aerobic ceiling. If echocardiographic data is available from pre-race veterinary examinations, `lv_mass_kg` is a direct predictor.

2. **Resting heart rate (HR_rest)**: T. M. Opera O's resting HR of 25 bpm vs 30.3 bpm average is consistent with the well-known "athlete's bradycardia" — enhanced parasympathetic tone from sustained training. Lower resting HR is strongly associated with greater stroke volume and cardiac output reserve. `resting_hr_bpm` (from veterinary monitoring or stall-based telemetry) is a practical feature if available.

3. **Heart rate variability (HRV) — HF power**: HF power (0.07–0.6 Hz, parasympathetic index) of 5,963 ms² vs 1,248 ms² average represents a ~4.8-fold difference. Prior work by Ohmura et al. (2002) showed that 7 months of training significantly increased both LF and HF power in young Thoroughbreds, establishing HRV as a training-adaptation marker. The exceptionally high HF power in T. M. Opera O reflects deep parasympathetic dominance — a hallmark of elite aerobic conditioning.

4. **LF:HF ratio**: T. M. Opera O's ratio of 2.3 vs 4.1 average indicates parasympathetic dominance at rest, in contrast to the sympathetic-leaning ratio seen in less fit horses. Lower LF:HF ratio → higher autonomic fitness → expected better race performance.

For the JRA/NAR finishing-position pipeline, these cardiac features are not routinely available in race records. However, they define the physiological basis for why `resting_hr` (if obtainable from veterinary logs) and derived fitness indices (V200, HR recovery rate) are meaningful predictors.

## Background & objective

Thoroughbred horses have extraordinary cardiopulmonary function: heart weight ~1.4% of body weight (vs 0.6% in average mammals); maximal HR 210–230 bpm; stroke volume ~1.6 L; VO₂max >160 mL/kg/min (vs ~80 mL/kg/min in human marathon runners). Previous cardiovascular data mostly came from non-elite racehorses or horses not in active racing condition. T. M. Opera O's owner and trainer cooperated to allow data collection during the 2001 racing season (5 years old), when the horse competed in 7 races (5 G1, 2 G2). The paper presents the first detailed cardiac profile of Japan's greatest-earning racehorse.

## Materials & methods

**Subject:** T. M. Opera O; 5-year-old Thoroughbred stallion; racing career 1998–2002 (26 races, 14 wins including 7 G1, career earnings ¥1,835,189,000 as of end-2002, highest in JRA history at time of publication).

**Comparison group:**

- HR/HRV: 15 other racehorses at Ritto Training Center (14 males, 1 female; age 3.7 ± 0.2 yr)
- LV mass: well-trained 2-year-old Thoroughbreds (JRA dataset, n not specified); and Young et al. 2002 data (n not specified; mean 3.366 ± 0.822 kg)

**All data collected in 2001 (7 races studied).**

**Work-out HR and blood lactate measurement:**

- Measured on days 11 and 4 before each of the 7 races (14 data points × 2 sessions = up to 14 measurements per parameter)
- HR monitor: Polar Electro Oy
- Work-out programme: warm-up + 1,800 m on oval woodchip course, speed gradually increasing from ~1,200 m from finish; peak speed at final 400–600 m
- Blood lactate: jugular vein sample at 10 min post-gallop; YSI 2300 STAT Plus analyser

**ECG/HRV recording:**

- 3-hour Holter recording with Fukuda Denshi SM-60; base-apex lead; own stall
- ECG processor: Softron Inc., Tokyo (as in Kuwahara et al. 1996)
- R waves detected; R-R interval tachogram computed; artefacts eliminated manually or by 75–125% mean criterion
- Data resampled at 200 ms intervals; 512-point datasets
- Hamming window applied; Fast Fourier Transform computed
- LF power: 0.01–0.07 Hz (combines sympathetic + parasympathetic)
- HF power: 0.07–0.6 Hz (primarily parasympathetic/vagal)
- LF:HF ratio computed

**Echocardiography:**

- EUB-6000 ultrasound unit (Hitachi); transducer placed in left 5th–6th intercostal space
- M-mode measurements in diastole: LVIDd (left ventricular internal diameter), IVSd (interventricular septal thickness), LVFWd (left ventricular free-wall thickness)
- LV mass formula (Devereux & Reichek 1977):
  LV mass = 1.04 × [(LVIDd + LVFWd + IVSd)³ − LVIDd³] − 13.6

**Statistics:** Paired t-test for day 11 vs day 4 comparisons; p<0.05 significance.

## Results (detailed — reproduce ALL numbers)

**Work-out HR and blood lactate:**

| Session            | Mean speed (last 1,200 m; m/min) | Mean speed (last 600 m; m/min) | Peak HR (beats/min) | Blood lactate at 10 min post (mmol/L) |
| ------------------ | -------------------------------- | ------------------------------ | ------------------- | ------------------------------------- |
| Day 11 before race | 868 ± 8.8                        | 926 ± 10.2                     | 216 ± 5.4           | 13.0 ± 2.5                            |
| Day 4 before race  | 888 ± 18.7                       | 932 ± 20.2                     | 212 ± 4.5           | 16.0 ± 1.9                            |
| Significance       | n.s.                             | n.s.                           | n.s.                | p < 0.05                              |

Speed did not differ significantly between day 11 and day 4. Peak HR did not differ significantly. Blood lactate was significantly higher on day 4 (16.0 vs 13.0 mmol/L, p<0.05), likely reflecting a slight and unmeasured increase in exercise intensity/duration close to the race date.

**Resting HR and HRV (Table 1):**

| Parameter              | T. M. Opera O | 15 other racehorses (mean ± SE) |
| ---------------------- | ------------- | ------------------------------- |
| Resting HR (beats/min) | 25            | 30.3 ± 1.2                      |
| LF power (ms²)         | 13,900        | 3,819 ± 459                     |
| HF power (ms²)         | 5,963         | 1,248 ± 321                     |
| LF:HF ratio            | 2.3           | 4.1 ± 0.5                       |

**Echocardiographic measurements (T. M. Opera O):**

- LVIDd: 11.5 cm
- IVSd: 2.7 cm
- LVFWd: 3.9 cm
- Computed LV mass: 1.04 × [(11.5 + 2.7 + 3.9)³ − 11.5³] − 13.6 = 4.60 kg

**Reference LV mass values:**

- JRA well-trained 2-year-olds (comparison group): 3.37 ± 0.34 (SE) kg
- Young et al. 2002 Thoroughbred population: 3.366 ± 0.822 (SD) kg
- T. M. Opera O: 4.60 kg (+37% above mean; >2 SD above Young et al. population)

**Pre-training Thoroughbred resting HR (from Ohmura et al. 2002 cited in this paper):** 41.5 ± 0.8 bpm → post-7-months-training: 38.7 ± 0.4 bpm (significant decrease, p<0.05), with concurrent significant increases in LF and HF power.

## Discussion & interpretation

The authors attribute T. M. Opera O's exceptional cardiac profile to combined genetic endowment (large inherited heart) and training-induced adaptation. Large LV mass → larger stroke volume → higher cardiac output per unit heart rate → higher VO₂max → superior aerobic performance capacity. The horse's 14/26 win rate in G1/G2 company is the performance validation.

The markedly elevated HF and LF power — both 3–4× higher than the 15-horse comparison group — reflect extreme parasympathetic dominance at rest. The low LF:HF ratio (2.3 vs 4.1) confirms this: when parasympathetic tone is very high, the LF component (which has both sympathetic and parasympathetic contributions) is proportionally smaller relative to HF. This autonomic profile is consistent with deeply trained, elite aerobic athletes in human sport physiology.

The blood lactate increase from day 11 to day 4 (13.0 → 16.0 mmol/L, p<0.05) despite similar exercise speed and HR is consistent with the standard pre-race taper: horses are typically worked harder (greater cumulative distance or higher peak speed in later intervals) in the final work before a race. The slightly faster mean speed on day 4 (888 vs 868 m/min, n.s. but directionally higher) likely explains the lactate increase through a nonlinear lactate-speed relationship near lactate threshold.

## Limitations

- Single-subject case report; no statistical comparison of T. M. Opera O against matched controls possible. All findings are descriptive.
- The 15-horse comparison group for HR/HRV is younger (3.7 yr) than T. M. Opera O (5 yr); age differences may partly explain lower HR/HRV differences if older horses have higher parasympathetic tone (though the paper notes evidence is sparse for HR_max-age effects in the 3–5 yr range)
- LV mass comparison uses 2-year-old JRA horses as the benchmark — younger, less mature hearts. A comparison with elite 5-year-old well-trained horses is absent.
- The HRV measurement is from a 3-hour Holter in a stall; circadian variation, stall activity level, feeding timing, and other environmental variables could affect HRV measures significantly. Not controlled.
- No direct VO₂max measurement for T. M. Opera O; the link between LV mass and aerobic capacity is from Young et al. 2002's population correlation, not direct measurement.

## Feature-engineering notes for the model

- `lv_mass_kg` — left ventricular mass in kg from echocardiography — source: JRA/NAR pre-race veterinary examination records if echocardiography is routine — strong positive predictor of aerobic capacity and performance; reference: >4 kg is elite, ~3.37 kg is typical well-trained Thoroughbred; not routinely available in standard race records
- `resting_hr_bpm` — resting heart rate (beats/min) from stall monitoring or Holter — source: veterinary monitoring systems — lower HR (≤25 bpm = elite level; ~30 bpm = average well-trained TB) indicates higher stroke volume and parasympathetic tone; expected negative correlation with finishing position (lower HR → better performance)
- `hf_power_ms2` — HRV high-frequency power (0.07–0.6 Hz), parasympathetic index — source: Holter ECG if available — T. M. Opera O at 5,963 vs average 1,248 ms²; >3,000 ms² indicates elite autonomic fitness
- `lf_hf_ratio` — LF:HF ratio from HRV analysis — source: Holter ECG — lower ratio → higher parasympathetic dominance → better training adaptation; T. M. Opera O at 2.3 vs 4.1 average
- `pre_race_day4_blood_lactate` — blood lactate at 10 min post-gallop on day 4 before race — source: veterinary training logs — reflects work intensity in final preparation; day4 lactate (16.0 mmol/L) significantly higher than day11 (13.0 mmol/L) for T. M. Opera O, consistent with normal taper protocol; very high day4 lactate may indicate over-training risk
- `days_before_race_work_speed` — average speed (m/min) during primary work session on day 4 and day 11 before race — source: training logs — combines with lactate to construct "taper state" composite feature
- **Caution:** None of the cardiac features (LV mass, resting HR, HRV) are available from standard JRA/NAR public race records. They require access to JRA veterinary examination databases or custom telemetry. For publicly available modelling, these features inform the _interpretation_ of proxies: V200, resting HR trends from training logs, and 馬体重 changes serve as indirect surrogates.

## Key references / follow-up leads

- Young, L.E., Marlin, D.J., Deaton, C., Brown-Feltner, H., Roberts, C.A., and Wood, J.L. 2002. Heart size estimated by echocardiography correlates with maximal oxygen uptake. _Equine Vet. J. Suppl._ 34: 467–471. (LV mass–VO₂max correlation in Thoroughbreds; population study underpinning TM Opera O interpretation)
- Young, L.E. 1999. Cardiac responses to training in 2-year-old thoroughbreds: an echocardiographic study. _Equine Vet. J. Suppl._ 30: 195–198. (LV mass increase with 18 weeks training in 2-year-olds)
- Ohmura, H., Hiraga, A., Aida, H., Kuwahara, M., and Tsubone, H. 2002. Effects of initial handling and training on autonomic nervous function in young Thoroughbreds. _Am. J. Vet. Res._ 63: 1488–1491. (HRV changes with 7-month training in young TBs; resting HR decreases, LF and HF power increase)
- Kuwahara, M., Hashimoto, S., Ishii, K., Yagi, Y., Hada, T., Hiraga, A., Kai, M., Kubo, K., Oki, H., Tsubone, H., and Sugano, S. 1996. Assessment of autonomic nervous function by power spectral analysis of heart rate variability in the horse. _J. Auton. Nerv. Syst._ 60: 43–48. (HRV frequency band definitions and methodology in horses; basis of LF=0.01–0.07 Hz, HF=0.07–0.6 Hz bands used here)
- Devereux, R.B. and Reichek, N. 1977. Echocardiographic determination of left ventricular mass in man. Anatomic validation of the method. _Circulation_ 55: 613–618. (LV mass formula used; validated in humans, applied to horses)
- Kubo, K., Senta, T., and Sugimoto, O. 1974. Relationship between training and heart in the Thoroughbred racehorse. _Exp. Rep. Equine Health Lab._ 11: 87–93. (heart-to-body-weight ratio ~1.4% in trained TBs; cited for context)
