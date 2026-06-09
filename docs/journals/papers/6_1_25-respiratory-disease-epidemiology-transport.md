# Some Epidemiological Aspects of Equine Respiratory Disease Associated with Transport

## Metadata

| Field                          | Value                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 6(1): 25–29, 1995                                                                     |
| docid                          | `6_1_25`                                                                                             |
| Article type                   | Note                                                                                                 |
| Authors                        | Masa-aki Oikawa, Ryo Kusunose                                                                        |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 5-27-7 Tsurumaki, Setagaya-ku, Tokyo 154, Japan |
| Received / Accepted / Released | Submitted September 30, 1994; accepted May 26, 1995                                                  |
| Keywords                       | horse, respiratory disease, transport                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/6/1/6_1_25/_pdf/-char/en                                    |

## Abstract (verbatim)

> In order to obtain the epidemiologic characteristics of equine respiratory disease associated with transport, a total of 29 Anglo-Arab and Thoroughbred horses were transported by truck for 1,708 km. The horses were examined to determine whether a clinical relationship existed between respiratory disease, and the following intrinsic and extrinsic factors; breed, sex, position in vehicle, travelling time, weight loss during transit, food and water intake throughout the transport, nervous excitability and anaerobic efficiency. As travelling time increased, the number of horses which developed respiratory disease increased. However, no significant relationship existed when other factors were compared to the onset of equine respiratory disease.

## Relevance to finishing-position (着順) prediction

Feature family **B (respiratory)**. Transport-induced respiratory disease (shipping fever / transport pneumonia) causes fever, cough, nasal discharge, and lethargy, directly impairing race-day performance and increasing scratch probability. This epidemiological study isolates travelling time as the single statistically associated factor among eight candidates; all other tested variables (breed, sex, vehicle position, nervous excitability, anaerobic efficiency, weight loss, hay/water intake) were non-significant.

For the prediction pipeline: a `transport_hours_to_race` feature (hours in transit from home stable to race venue) is a candidate signal for performance degradation and scratch risk. The onset pattern in this study (disease appearing from 14 hours post-departure, rising steeply at 21–24 hours) suggests a non-linear threshold effect. A binary `long_transport` flag at a 14-hour threshold, or a spline/binned encoding of transport time, would be more appropriate than a linear feature. The JRA haichi (stable allocation) system means some horses travel >10 hours to distant racecourses (e.g., Sapporo, Kokura from Kanto/Kansai); these long-haul entries are identifiable from stable address × racecourse combinations.

Since breed (Thoroughbred vs. Anglo-Arab) and sex were non-significant, these variables need not be interacted with transport time in the model.

## Background & objective

Equine respiratory disease is a known consequence of transport. One study cited in the paper found 24.4% of affected horses had been transported >500 miles; another found 8 of 11 affected horses had been transported within the prior 24 hours (Raphel and Beech 1982; Mair and Lane 1989). While pathophysiology (stress, immune suppression, altered mucociliary clearance), microbiology (Streptococcus equi subsp. zooepidemicus, Pasteurella), and pathology have been studied, analytical epidemiology identifying specific risk factors was absent. This study aimed to quantify the relationship between specific intrinsic and extrinsic factors and disease onset.

## Materials & methods

**Subjects and transport:** 29 horses aged 23–27 months transported 1,708 km by road truck on two occasions:

- Group 1 (n=13): Anglo-Arab horses (10 male, 3 female); transported April 10–12, 1993
- Group 2 (n=16): Thoroughbred horses (8 male, 8 female); transported April 23–25, 1993
  Total travelling time: 36 hours each. Vehicles carried 4 horses each (2 front, 2 rear). After each 4–5-hour travel period, horses rested for 1 hour.

**Disease definition:** Rectal temperature ≥38.6°C, with or without coughing, nasal discharge, and/or lethargy. Only febrile horses (≥38.6°C) classified as affected; horses with other signs but normal temperature classified as unaffected.

**Variables examined:**

1. Breed (Anglo-Arab vs. Thoroughbred)
2. Sex (male vs. female)
3. Position in vehicle (front vs. rear)
4. Nervous excitability: ECG telemetry during 5-min exposure to horse-sized cloth silhouette in paddock, measured 1 week before transport. Index: maximum heart rate (beats/min) and time to decrease to <100/min.
5. Anaerobic efficiency: 800 m run at ~700 m/min, 2 weeks before transport; blood lactate measured pre- and 5 min post-exercise by enzymatic method. Index: lactate increment (post minus pre).
6. Weight loss during transport (% of initial body weight)
7. Hay intake (kg) during transport
8. Water intake (L) during transport
9. Travelling time (hours since departure)

**Measurements within trucks:** Ambient temperature, air dust concentration (mg/m³), ammonia concentration (ppm) measured throughout 36 hours.

**Statistics:** Student t-test; significance at P<0.05.

## Results (detailed — reproduce ALL numbers)

### Overall disease prevalence

- Group 1: 6 of 13 horses (46%) developed respiratory disease during transport
- Group 2: 7 of 16 horses (44%) developed respiratory disease during transport
- Total: 13 of 29 horses (44.8%) affected

### Table 1: Prevalence by breed, sex, vehicle position (no significant differences)

| Factor           | Subgroup A       | n   | Disease (n) | Prevalence (%) | Subgroup B     | n   | Disease (n) | Prevalence (%) | A vs B |
| ---------------- | ---------------- | --- | ----------- | -------------- | -------------- | --- | ----------- | -------------- | ------ |
| Breed            | Thoroughbred (A) | 16  | 7           | 44%            | Anglo-Arab (B) | 13  | 6           | 46%            | NS     |
| Sex              | Male (A)         | 18  | 7           | 39%            | Female (B)     | 11  | 6           | 55%            | NS     |
| Vehicle position | Front (A)        | 12  | 4           | 33%            | Rear (B)       | 15  | 9           | 60%            | NS     |

### Nervous excitability indices (no significant intergroup differences between Groups 1 and 2)

- Group 1 (n=11): Max HR after silhouette = 139.2±21.27 beats/min; time to decrease to <100/min = 1.53±0.64 min
- Group 2 (n=16): Max HR after silhouette = 134.3±29.53 beats/min; time to decrease to <100/min = 0.89±0.82 min

### Anaerobic efficiency (lactate increment, no significant intergroup difference)

- Group 1: lactate pre-exercise = 5.27±2.45 mg/%; post-exercise = 6.78±1.68 mg/%
- Group 2: lactate pre-exercise = 4.25±1.05 mg/%; post-exercise = 5.33±2.46 mg/%

### Table 2: Affected vs. non-affected horse comparison (all horses combined, n=29)

| Factor                           | Affected (A) n | Mean ± SD      | Non-affected (B) n | Mean ± SD      | A vs B |
| -------------------------------- | -------------- | -------------- | ------------------ | -------------- | ------ |
| Max HR at silhouette (beats/min) | 13             | 140.20 ± 21.40 | 14                 | 132.60 ± 30.22 | NS     |
| Time to <100/min (min)           | 13             | 1.28 ± 0.72    | 13                 | 1.04 ± 0.84    | NS     |
| Lactate increment (mg%)          | 10             | 1.90 ± 2.34    | 12                 | 1.58 ± 1.25    | NS     |
| Body weight loss (% BW)          | 3              | 3.13 ± 0.45    | 4                  | 4.30 ± 0.87    | NS     |
| Hay intake (kg)                  | 3              | 19.67 ± 4.50   | 5                  | 19.20 ± 2.71   | NS     |
| Water intake (L)                 | 3              | 58.33 ± 22.48  | 5                  | 54.40 ± 15.78  | NS     |

### Travelling time and disease onset (Fig. 3)

- First respiratory disease onset: as early as 14 hours post-departure
- Thereafter: cumulative number of affected horses increased with travelling time
- Marked increase in prevalence observed at 21–24 hours post-departure
- Relationship is monotonically increasing (graphical data; no statistical test reported for this specific association)

### Environmental conditions within transport vehicles

- Group 1 experienced lower ambient temperatures and dust concentrations than Group 2 throughout 36 hours
- Group 1 experienced higher ammonia concentrations than Group 2 during first 18 hours; reverse for final 18 hours
- Despite these environmental differences, no significant difference in disease incidence between groups
- Ammonia concentrations, temperature, and dust therefore do not appear to be primary determinants in this range

## Discussion & interpretation

The paper identifies travelling time as the key variable, consistent with prior reports linking long-distance transport to higher respiratory disease incidence (Raphel and Beech 1982; Mair and Lane 1989). The authors note that body position restriction, inability to lower the head (important for mucociliary clearance), immune suppression from prolonged stress, and accumulating inhalation of environmental contaminants over time (rather than peak concentration) likely contribute to the time-dependent risk. The small sample sizes for weight loss, hay/water data (n=3–5) severely limit conclusions from those comparisons.

The environmental data showed that Group 1 and Group 2 had different ammonia/temperature/dust profiles, yet similar disease rates, further suggesting that time exposure rather than acute environmental concentrations is the primary driver.

## Limitations

- Very small sample (n=29); particularly small subgroups for weight/hay/water comparisons (n=3–5)
- Only two transport occasions, limiting generalisability to other routes, seasons, or vehicle configurations
- Disease defined solely by rectal temperature ≥38.6°C; subclinical respiratory effects are missed
- No control (non-transported) group
- Travelling time is confounded with distance and duration of stress — cannot distinguish pure time from cumulative distance or head-position restriction
- No post-transport follow-up to assess performance degradation at subsequent race; outcome measured is disease incidence, not finishing position directly

## Feature-engineering notes for the model

- `transport_hours_to_race` — estimated hours in transit to reach the racecourse, derived from home stable address × racecourse location — expected effect: positive (longer transport → higher respiratory disease and performance degradation risk); threshold around 14 hours, sharp increase at 21–24 hours — data availability: derivable from stud farm / stable address and racecourse metadata; not always in standard race records
- `long_distance_transport_flag` — binary: 1 if estimated transport >14 hours, 0 otherwise — expected effect: positive for underperformance/scratch risk — data availability: same as above
- `days_since_transport` — if race occurs ≥5 days post-transport, recovery may be complete; within 1–3 days post-long-transport the risk is highest — note: requires race scheduling data relative to horse relocation
- `breed` (TB vs. Anglo-Arab) — **null effect** for transport-related disease in this study; do not include as transport-risk modifier
- `sex` — **null effect** for transport-related disease; do not interact with transport feature
- **Do NOT use:** single-point environmental measurements (ammonia, temperature, dust) as proxies for transport disease risk; this paper shows these are non-predictive compared to travel time

## Key references / follow-up leads

- Raphel C.F. and Beech J. 1982. Pleuritis secondary to pneumonia or lung abscessation in 90 horses. _J. Am. Vet. Med. Assoc._ 181: 808–810. (24.4% of affected horses had recent long-distance transport)
- Mair T.S. and Lane J.G. 1989. Pneumonia, lung abscesses and pleuritis in adult horses: a review of 51 cases. _Equine Vet. J._ 21: 175–180. (8/11 affected horses transported within 24 hours)
- Oikawa M., Kamada M., Yoshikawa Y., and Yoshikawa T. 1994. Pathology of equine pneumonia associated with transport, and isolation of _Streptococcus equi_ subsp. _zooepidemicus_. _J. Comp. Path._ 111: 205–212. (pathological companion — same JRA author)
- Oikawa M., Takagi S., Anzai R., Yoshikawa Y., and Yoshikawa T. 1995. Pathology of equine respiratory disease occurring in association with transport. _J. Comp. Path._ 112 (in press at time of publication). (sequel pathology paper)
- Traub-Dargatz J.L., McKinnon A.O., Bruyninckx W.J., Thrall M.A., Jones R.L., and Blancquaert A-M.B. 1988. Effect of transportation stress on bronchoalveolar lavage fluid analysis in female horses. _Am. J. Vet. Res._ 49: 1026–1029. (physiological mechanism — BAL changes)
- Yayou K., Kusunose R., Matsui K., Uzawa O., Matias J.M., and Sugano S. 1991. The responses of pregnant mares to visual stimulation with special reference to the changes in behavior and electrocardiogram. _Jpn. J. Equine Sci._ 2: 41–48. (nervous excitability assessment method used here)
