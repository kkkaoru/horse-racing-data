# Risk factors for superficial digital flexor tendinopathy in Thoroughbred racing horses in Japan

## Metadata

| Field                          | Value                                                                                                                                       |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 30(4): 93–98, 2019                                                                                                           |
| docid                          | `30_1909`                                                                                                                                   |
| Article type                   | Full Paper                                                                                                                                  |
| Authors                        | Yoko Ikeda, Akikazu Ishihara, Masahiko Nakajima, Kazutaka Yamada                                                                            |
| Affiliations                   | Department of Veterinary Medicine, Azabu University, Kanagawa 252-5201, Japan; Tokyo Metropolitan Racing Association, Tokyo 140-0012, Japan |
| Received / Accepted / Released | April 4, 2019 / November 7, 2019 / 2019                                                                                                     |
| Keywords                       | racing horse, risk factor, superficial flexor tendinopathy, Thoroughbred                                                                    |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/30/4/30_1909/_pdf/-char/en                                                                         |

## Abstract (verbatim)

> Thoroughbred racehorses are commonly affected with superficial digital flexor (SDF) tendinopathy. This study aimed to identify risk factors for SDF tendinopathy in racing horses. The authors selected racehorses (n=292) with SDF tendinopathy from the medical records of a racetrack. As a risk factor associated with track-related variables, the SDF tendinopathy odds ratio (OR) was significantly high for a sloppy track surface compared with a standard track surface. Regarding risk factors associated with race-related variables, the SDF tendinopathy OR was significantly high in the following cases: when the order of arrival was worse than or equal to the 10th place; when the racehorses started to run a short race and when the racehorses' favourites were worse than or equal to the 8th place. Regarding risk factors associated with racehorse-related variables, the body weight of racehorses with SDF tendinopathy was significantly heavier than that of control horses. When there was a decrease in body weight since previous racing, the SDF tendinopathy OR was significantly high. Regarding risk factors associated with race career-related variables, when the charge in the race distance was short, the SDF tendinopathy OR was significantly high. As a countermeasure to prevent SDF tendinopathy, a sloppy track surface should be avoided during the race by guiding the horse toward to more solid track surface. Selecting long-distance races with slow speed, if possible, could reduce the risk of SDF tendinopathy.

## Relevance to finishing-position (着順) prediction

Feature family: **A — injury/soundness**. This is the most directly actionable paper in the collection for the finishing-position model. Every significant risk factor identified for SDF tendinopathy (the single leading cause of career-ending orthopedic retirement in Thoroughbreds, accounting for 46% of orthopedic retirements) is a feature already present in standard race databases:

- **Track condition (sloppy vs. standard):** OR=1.69 — races run on sloppy dirt tracks at NAR have 69% higher odds of SDF tendinopathy. For the prediction model, sloppy track condition should be included as a feature that increases injury-probability prior.
- **Finishing position (≥10th place):** OR=2.50 — the strongest single risk factor. Important caveat: this is likely reverse causality (a horse already suffering subclinical tendon damage finishes poorly, then the damage becomes clinically apparent within 2 days). This creates a useful retrospective signal: a horse with a run of unexpectedly poor finishes may already have subclinical tendinopathy.
- **Race distance (<1,300 m):** OR=1.45 — short, fast races cause more SDF microdamage. JRA also runs short races (<1,300 m); this risk factor is relevant across both JRA and NAR.
- **Favourite rank (≥8th):** OR=1.49 — poorly fancied horses (low market confidence) have higher injury risk; this may reflect trainer awareness of subclinical problems.
- **Body weight (≥470 kg):** OR=1.55 — heavier horses load tendons more. JRA 馬体重 is recorded at weigh-in; use as a continuous or thresholded feature.
- **Body weight decrease ≥5 kg from previous race:** OR=1.59 — rapid weight loss may indicate excessive training load, muscle catabolism, or GI issues, all of which reduce musculoskeletal resilience.
- **Sex (male vs. female):** OR=1.35; **gelding vs. female:** OR=3.09 — geldings are at highest risk. Sex is a standard race database field.
- **Cumulative race distance, last 3 races <4,000 m:** OR=1.40; **last 6 races <8,000 m:** OR=1.47 — insufficient accumulated distance before a race (low recent workload) is associated with higher injury risk, possibly because tendons are not conditioned.
- **Days since previous race ≥90:** OR=1.75 — long inter-race intervals signal either subclinical injury or deconditioning; both elevate risk.

All of these features are derivable from publicly available JRA/NAR race records (netkeiba, JRA official), making this paper the highest-priority source for concrete feature engineering.

## Background & objective

SDF tendinopathy accounts for 46% of orthopedic retirements in Thoroughbreds; prevalence 6% in 2-year-olds, 20% in 3-year-olds, 17% in 4-year-olds, 12% in 5+year-olds (average 14%). Injury rate: 0.94 injuries per 1,000 race starts on flat dirt tracks; 0.58 per 1,000 on turf. Risk factors identified in international studies (old age, female sex, long-distance fast races, heavy body weight, firm track, long career) may not generalise to Japan, where non-JRA (NAR) tracks run shorter distances (1,000–2,880 m) on dirt only. This study fills the Japan-specific gap using Oi Racecourse (Tokyo Metropolitan Racing Association) records.

## Materials & methods

**Study design:** retrospective matched case-control study.

**Cases:** 292 Thoroughbred racehorses diagnosed with SDF tendinopathy via ultrasonography within 2 days of racing at Oi Racecourse (Tokyo Metropolitan Racing Association, NAR), 2011–2015. Ultrasonography confirmed all diagnoses.

**Controls:** 2 control horses per case (584 controls total). Matching criteria: same trainer, same age (or nearest age if no exact match), raced within 6 months of case horse's injury date. Each control horse could be selected only once. Control horses selected from horses not in the case group.

**Race record data source:** www.netkeiba.com, covering all race records prior to injury (cases) or selection date (controls).

**Variables analysed:**

- Track condition (standard, good, muddy, sloppy) and weather at race time (clear, cloudy, raining/snowy)
- Order of arrival at the injury race
- Race distance (m) at the injury race
- Favourite rank at the injury race
- Body weight at race time (kg); change in body weight from previous race (kg)
- Sex and age
- Total race distance (excluding injury race) and mean race distance
- Cumulative race distance in last 3 races (excluding injury race) — "charge in running distance since last 3 races"
- Cumulative race distance in last 6 races (excluding injury race)
- Number of days since previous race
- History of previous diseases

**Exclusions:** horses with no previous race excluded from body weight change and days-since-last-race analyses. Horses with ≤2 or ≤5 previous starts excluded from last-3-races or last-6-races analyses, respectively. Dirt track conditions only (turf data not used).

**Statistics:** Student's t-test (continuous variables); chi-square test and Fisher's exact test (categorical variables); odds ratios calculated at VassarStats.net. Significance: P<0.05. Multiple logistic regression not performed due to small sample size.

**Case and control characteristics (Table 1):**
| Variable | Cases (n=292) | Controls (n=584) | P |
|----------|---------------|-----------------|---|
| Age (y) | 4.0 ± 1.4 (range 2–9) | 3.8 ± 1.3 (range 2–18) | NS |
| No. of previous races | 18.5 ± 17.2 (range 0–101) | 18.3 ± 15.1 (range 0–110) | NS |
| Total race distance (m) | 26,446 ± 25,651 (range 0–148,000) | 26,401 ± 22,510 (range 0–166,000) | NS |
| Mean race distance (m) | 1,378 ± 193 (range 0–2,034) | 1,412 ± 257 (range 0–2,632) | NS |

## Results (detailed — reproduce ALL numbers)

### Track/weather-related risk factors (Table 2)

| Track condition      | Cases n (%)  | Controls n (%) | OR       | P         | 95% CI        |
| -------------------- | ------------ | -------------- | -------- | --------- | ------------- |
| Standard (reference) | 137 (47%)    | 327 (56%)      | 1.00     | —         | —             |
| Good                 | 53 (18%)     | 99 (17%)       | 1.28     | NS        | 0.87–1.88     |
| Muddy                | 44 (15%)     | 76 (13%)       | 1.38     | NS        | 0.91–2.11     |
| **Sloppy**           | **58 (20%)** | **82 (14%)**   | **1.69** | **<0.01** | **1.14–2.50** |

| Weather          | Cases n (%) | Controls n (%) | OR   | P   | 95% CI    |
| ---------------- | ----------- | -------------- | ---- | --- | --------- |
| Fine (reference) | 155 (53%)   | 315 (54%)      | 1.00 | —   | —         |
| Cloudy           | 96 (33%)    | 199 (34%)      | 0.98 | NS  | 0.72–1.34 |
| Raining/snowy    | 41 (14%)    | 70 (12%)       | 1.24 | NS  | 0.77–1.83 |

### Race-related risk factors (Table 3)

| Variable          | Cases mean ± SD | Controls mean ± SD | OR       | P         | 95% CI        |
| ----------------- | --------------- | ------------------ | -------- | --------- | ------------- |
| Order of arrival  | 9.3 ± 3.9       | 7.2 ± 4.3          | —        | <0.01     | —             |
| ≥10th place       | 158/292 (54%)   | 187/584 (32%)      | **2.50** | **<0.01** | **1.88–3.34** |
| Race distance (m) | 1,385 ± 222     | 1,447 ± 290        | —        | <0.01     | —             |
| <1,300 m          | 131/292 (45%)   | 210/584 (36%)      | **1.45** | **<0.05** | **1.09–1.93** |
| Favourite         | 7.6 ± 3.4       | 7.0 ± 4.8          | —        | <0.05     | —             |
| ≥8th favourite    | 146/292 (50%)   | 234/584 (40%)      | **1.49** | **<0.05** | **1.13–1.98** |

### Racehorse-related risk factors (Table 4)

| Variable                    | Cases mean ± SD | Controls mean ± SD | OR       | P         | 95% CI        |
| --------------------------- | --------------- | ------------------ | -------- | --------- | ------------- |
| Body weight (kg)            | **477.8 ± 29**  | **469.7 ± 33**     | —        | **<0.01** | —             |
| ≥470 kg                     | 178/292 (60%)   | 293/584 (50%)      | **1.55** | **<0.01** | **1.17–2.06** |
| Body weight change (kg)     | 0.06 ± 7.57     | 0.59 ± 5.97        | —        | NS        | —             |
| ≥5 kg decrease              | 53/292 (18%)    | 70/584 (12%)       | **1.59** | **<0.05** | **1.08–2.34** |
| **Sex: female (reference)** | 99 (34%)        | 245 (42%)          | 1.00     | —         | —             |
| **Male**                    | 178 (61%)       | 327 (56%)          | **1.35** | **<0.05** | **1.00–1.81** |
| **Gelding**                 | 15 (5%)         | 12 (2%)            | **3.09** | **<0.01** | **1.40–6.84** |
| Age                         | 4.0 ± 1.4       | 3.8 ± 1.3          | —        | NS        | —             |

### Race career-related risk factors (Table 5)

| Variable                                  | Cases mean ± SD | Controls mean ± SD | OR       | P         | 95% CI        |
| ----------------------------------------- | --------------- | ------------------ | -------- | --------- | ------------- |
| Cumulative race distance last 3 races (m) | 4,195 ± 535     | 4,375 ± 737        | —        | <0.01     | —             |
| <4,000 m (last 3 races)                   | 93/255 (36%)    | 129/444 (29%)      | **1.40** | **<0.05** | **1.01–1.94** |
| Cumulative race distance last 6 races (m) | 8,379 ± 1,379   | 8,878 ± 3,052      | —        | <0.05     | —             |
| <8,000 m (last 6 races)                   | 83/216 (38%)    | 117/392 (30%)      | **1.47** | **<0.05** | **1.03–2.08** |
| Days since previous race                  | 41.8 ± 61.6     | 33.3 ± 48.7        | —        | <0.05     | —             |
| ≥90 days                                  | 30/289 (10%)    | 35/519 (6%)        | **1.75** | **<0.05** | **1.05–2.92** |

### Previous disease risk factors (Table 6)

| Variable                 | Cases n (%) | Controls n (%) | OR   | P   | 95% CI      |
| ------------------------ | ----------- | -------------- | ---- | --- | ----------- |
| History of tendinitis    | 2 (0.69%)   | 0 (0%)         | ∞    | N/A | —           |
| History of other disease | 1 (0.34%)   | 11 (1.84%)     | 0.17 | NS  | 0.023–1.393 |

## Discussion & interpretation

The key finding — OR=2.50 for order of arrival ≥10th — likely reflects reverse causality: horses that finish poorly may have been already suffering subclinical tendinopathy that caused both poor performance and the impending clinical injury. This is confirmed by the authors' suggestion that poor favourites (OR=1.49) may already be known to trainers as unsound. The sloppy track result (OR=1.69) is paradoxical to prior international literature (which found firm tracks riskier) and may reflect the specific clay composition of Japanese dirt tracks versus sand-based tracks abroad. Short-distance race risk (OR=1.45) is also the inverse of overseas findings; the authors attribute this to the narrow distance range at Oi (1,000–2,880 m) versus international tracks (≥4,000 m), where the direction reverses.

The gelding vs. female OR of 3.09 is the largest in the study and is consistent with prior reports that sex hormones affect tendon mechanical properties (females may benefit from oestrogen-mediated tendon resilience). Body weight ≥470 kg (OR=1.55) confirms biomechanical loading; mean case weight was 477.8 ± 29 kg vs. control 469.7 ± 33 kg, an 8.1 kg difference.

The authors propose that short cumulative race distance in the past 3–6 races (low recent racing volume) predisposes to injury because tendons are not adequately conditioned by regular race loads. Combined with ≥90-day inter-race interval (OR=1.75), this suggests that deconditioning and/or subclinical injury-driven rest both elevate subsequent injury risk.

## Limitations

- Single NAR venue (Oi Racecourse, dirt only, short distances 1,000–2,880 m); results may not generalise to JRA turf/dirt or longer-distance tracks.
- Univariable analysis only; no multivariable logistic regression due to small sample size; ORs may be confounded.
- Reverse causality for order-of-arrival and favourite rank risk factors cannot be excluded.
- 2.69% of controls were selected at the same racetrack as cases, potentially creating ascertainment bias.
- Missing history data for SDF tendinopathy recurrence in controls (could not be determined from records).
- No data on training load or work rate between races.

## Feature-engineering notes for the model

- `track_condition_sloppy` — binary flag (1 = sloppy dirt); OR 1.69 for injury, and also directly affects race pace and going preference interactions. Source: race records track_condition field. JRA codes: 重 (heavy) ≈ muddy/sloppy, 不良 (bad) = sloppy.
- `last_race_finish_position` — finishing rank in the most recent race; ≥10th OR=2.50 for subsequent SDF tendinopathy. Source: race results. Use as lagged feature with n_runners denominator (i.e., finish_rate = position/runners).
- `race_distance` — race distance in metres; <1,300 m OR=1.45 for injury at NAR. Source: race records.
- `favourite_rank` — starting odds rank in the race; ≥8th OR=1.49. Source: odds data.
- `body_weight_kg` — horse body weight at weigh-in; ≥470 kg OR=1.55. JRA and NAR record 馬体重 officially. Use as continuous feature; threshold at 470 kg.
- `body_weight_change_kg` — body weight at current race minus body weight at previous race (can be negative); decrease ≥5 kg OR=1.59. Source: successive weigh-in records.
- `sex` — female=0, male=1, gelding=2 (ordinal risk encoding); gelding OR=3.09 vs. female baseline.
- `cumulative_dist_last_3` — sum of race distances in the 3 races preceding the current race (excluding current); <4,000 m OR=1.40. Source: computed from race distance history.
- `cumulative_dist_last_6` — sum of race distances in the 6 races preceding the current race; <8,000 m OR=1.47. Source: computed.
- `days_since_last_race` — calendar days from previous race to current race; ≥90 OR=1.75. Source: race date fields.
- **Interaction terms:**
  - `body_weight_kg × cumulative_dist_last_6`: heavy decondititioned horse has compounded risk.
  - `sex_gelding × body_weight_kg`: geldings with high body weight may be extreme outliers.
  - `days_since_last_race × last_race_finish_position`: long rest after a very poor finish strongly suggests subclinical injury.
- **Caution on reverse causality:** `last_race_finish_position` and `favourite_rank` may not be independent causes of injury but instead proxies for pre-existing subclinical damage. Use these features as informative priors for injury probability, not direct performance predictors.

## Key references / follow-up leads

- Takahashi T, Kasashima Y, Ueno Y. 2004. Association between race history and risk of superficial digital flexor tendon injury in Thoroughbred racehorses. J. Am. Vet. Med. Assoc. 225: 90–93. — Japanese Thoroughbred SDF tendon injury vs. race history; foundational for this paper.
- Kasashima Y, Takahashi T, Smith RKW et al. 2004. Prevalence of superficial digital flexor tendonitis and suspensory desmitis in Japanese Thoroughbred flat racehorses in 1999. Equine Vet. J. 36: 346–350. — JRA Thoroughbred prevalence 1999; cited as follow-up in 30_1905 also.
- Lam KKH, Parkin TDH, Riggs CM, Morgan KL. 2007. Evaluation of detailed training data to identify risk factors for retirement because of tendon injuries in Thoroughbred racehorses. Am. J. Vet. Res. 68: 1188–1197. — training data-based risk factors.
- Perkins NR, Reid SW, Morris RS. 2005. Risk factors for injury to the superficial digital flexor tendon and suspensory apparatus in Thoroughbred racehorses in New Zealand. N. Z. Vet. J. 53: 184–192. — international comparison dataset.
- Reardon RJM et al. 2012. Risk factors for superficial digital flexor tendinopathy in Thoroughbred racehorses in hurdle starts in the UK (2001–2009). Equine Vet. J. 44: 564–569. — UK comparison dataset.
