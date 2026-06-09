# Epidemiology of racing injuries in Thoroughbred racehorses with special reference to bone fractures: Japanese experience from the 1980s to 2000s

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                           |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 27(3): 81–97, 2016                                                                                                                                                                                                                                                                               |
| docid                          | `27_1514`                                                                                                                                                                                                                                                                                                       |
| Article type                   | Review Article                                                                                                                                                                                                                                                                                                  |
| Authors                        | Yousuke MAEDA, Michiko HANADA, Masa-aki OIKAWA                                                                                                                                                                                                                                                                  |
| Affiliations                   | Laboratory of Clinical Veterinary Medicine for Large Animal, School of Veterinary Medicine, Kitasato University, Aomori 034-8628; Faculty of Animal Health Technology, Department of Animal Health Technology, Yamazaki Gakuen University, Tokyo 150-0046; Japanese Society of Equine Science, Tochigi 329-0412 |
| Received / Accepted / Released | May 1, 2015 / August 5, 2016 / 2016                                                                                                                                                                                                                                                                             |
| Keywords                       | bone fracture, epidemiology, racing injury, Thoroughbred racehorse                                                                                                                                                                                                                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/27/3/27_1514/_pdf/-char/en                                                                                                                                                                                                                                             |

## Abstract (verbatim)

> This report describes the descriptive epidemiology of racing fractures that occurred from the 1980s to 2000s on racetracks of the Japan Racing Association (JRA). The incidence of racehorse fractures during flat racing was approximately 1–2%. Fractures occurring during a race are more likely to occur in a forelimb. Fractures mostly occur at the third and fourth corners of oval tracks and on the home stretch. They also occur more frequently at the time of changing the leading limb. Comparison of the incidence of racing fracture between before and after reconstruction of the geometrical configuration of a racetrack revealed that there was an outstanding reduction in the number of serious fractures in the year before and after reconstruction. It was postulated that the improvement in racing time, possibly influenced by reconstructing the geometrical configuration of the racetrack, were connected to the reduction in the number of fractures. Of non-biological race- and course-related factors, type of course (dirt or turf), track surface condition, differences between racecourses, and racing distance significantly influence racing time. By using an instrumented shoe, vertical ground reaction forces (VGRFs) on the forelimb during galloping and the relationships between a rough dirt and woodchip track surface and a smooth dirt and woodchip surface were measured. Relating the incidence of racing fractures with track conditions in general showed that track surface has significant effects on the incidence of fracture, with the incidence of fractures increasing as track conditions on dirt worsen and a tendency for the incidence of fractures to decrease as track conditions on turf worsen. It seems probable that track condition in general may affect the incidence of fracture. The incidence of fracture in horses during both racing and training decreased as the years progressed.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **A (injury/soundness)** at the population level, overlapping with **G (statistical modelling)** through its racing-time regression results. It is the most comprehensive JRA-specific epidemiological review covering the 1980s–2000s, providing concrete incidence numbers, risk-factor profiles, and quantitative data on how **track type, going, racecourse, and racing distance** independently affect both fracture incidence and racing time — both of which are direct signals for the finishing-position model.

Four feature families are validated by this paper for the JRA domain:

1. **Track surface type (dirt vs. turf):** Overall fracture incidence dirt (1.90%, 95% CI 1.84–1.95) vs. turf (1.77%, 95% CI 1.73–1.82) over 14 years; dirt courses also have higher racing times. Both surface type and going interact non-linearly: on JRA dirt, wetter conditions increase fracture risk (opposite to NYRA); on JRA turf, wetter conditions decrease fracture risk. This JRA-specific dirt track behaviour (sand composition vs. clay/silt in NYRA) must be encoded separately from non-JRA models.

2. **Track going (surface condition):** Going significantly affects racing time at both Nakayama and Kyoto racecourses. On turf, Firm races at 1,200 m at Nakayama average 70.8 ± 1.4 s vs. 73.6 ± 1.6 s on Soft (P<0.01 difference), with intermediate values for Good (71.6) and Yielding (72.1). On dirt at Nakayama 1,200 m, Fast: 75.1 ± 0.9 s, Good: 74.1 ± 1.2 s, Muddy: 73.8 ± 1.6 s, Sloppy: 74.1 ± 1.2 s. These quantitative going effects are critical for time-normalisation in performance models.

3. **Racecourse identity:** Racecourse independently and significantly affects racing time even after removing track condition and distance effects. The 10 JRA racecourses (Sapporo, Hakodate, Fukushima, Niigata, Tokyo, Chukyo, Hanshin, Kokura, Nakayama, Kyoto) should each have their own fixed effects in the model.

4. **Race distance:** Racing distance significantly influences racing time and fracture incidence. Longer distances are associated with different fracture profiles and different performance curves.

## Background & objective

The JRA formed the Japanese Committee on the Prevention of Accidents to both jockeys and racehorses in April 1983. This committee investigated fracture occurrence on JRA tracks and training centres and implemented countermeasures specific to Japan. Non-biological risk factors in Japan differ substantially from Western countries due to differences in track shape (clockwise vs. anticlockwise), track surface components (JRA dirt = fine river sand; NYRA dirt = clay/silt/sand), race distance ranges, number of runners, horseshoe types, and climate. The objective is to review and synthesise these JRA-specific investigations to make the Japanese data accessible internationally.

**JRA structure (as reviewed):**

- 10 racecourses, predominantly oval, races held on weekends (288 racing days/year)
- 8 clockwise, 2 anticlockwise
- 51.5% flat races on turf, 48.5% on dirt (during period of review)
- ~4,400 racehorses stabled at 2 training centres (Miho: eastern Japan; Ritto: western Japan)
- Racehorse management and all data (medical, race records) under centralised JRA control

**Track surface definitions:**

- _JRA Dirt:_ 7–8 cm cushion sand (fine river sand, narrow particle size distribution) over hard substrate. Conditions: Fast (dry), Good (some residual moisture), Muddy (very moist), Sloppy (slippery/excessive water).
- _JRA Turf:_ 10–12 cm Japanese lawn grass over 30–50 cm sandy soil. Conditions: Firm (dry/slight moisture), Good, Yielding (very wet/slower times), Soft (water-logged/very slow times).
- **JRA hardness range on non-race days: 80–130 G** (deceleration impact). Saint Cloud and Chantilly (France) measured harder; Keeneland, Arlington, Hollywood Park (USA), Epsom, Newmarket (UK), Longchamp (France) measured softer.

## Materials & methods

**Data source:** Annual JRA report on racehorse hygiene (JRA Annual Racehorse Hygiene Report); Racehorse Information Management System (computerised database: M880/180 mainframe, WS3050RX terminals, Hitachi Ltd.). Data covers fractures during flat racing at 10 racecourses and training at 2 training centres.

**Case definition:** Any horse with an acute bone fracture during a race that then failed to race for ≥3 months, was permanently retired, euthanised, or died. Confirmed by radiography and/or ultrasound. Clinical-only diagnoses excluded.

**Racehorse fracture data:** 556,705 starters in flat racing at 10 JRA racecourses from 1987 to 2000. Fracture analysis covers 1987–2000 (racing) and 1981–1997 (training).

**Racing time analysis (Tables 2 & 3):** 183,465 Thoroughbred racehorses (99,803 turf starters, 83,662 dirt starters) in 16,765 flat races at 10 JRA racecourses, 2000–2004. Statistical method: Student's t-test following one-way ANOVA for between-group comparisons.

**Fracture location analysis (video):** 259 catastrophic fractures during races at 10 JRA racecourses over 3 years (1983–1986), analysed from patrol video cameras.

**VGRF measurements (instrumented shoe):** Device sandwiched between hoof and shoe, measuring vertical ground reaction forces during cantering and galloping. Used to compare rough vs. harrowed track surfaces.

**Track hardness survey:** JRA track hardness measuring vehicle (piezoelectric accelerometer, 6 kg weight, 1 m arm; measurements at 5 m intervals) at 9 overseas racecourses in Europe and North America, 1992–1997.

## Results (detailed — reproduce ALL numbers)

### Fracture incidence during flat racing

- **Overall (1987–2000):** 10,203 fractures in 556,705 starters = **1.83%** (95% CI 1.80–1.87)
- **Turf courses:** 1.77% (95% CI 1.73–1.82)
- **Dirt courses:** 1.90% (95% CI 1.84–1.95)
- **Catastrophic fractures (direct euthanasia, 1985–1994):** 0.32 ± 0.07%
- Trend: fracture incidence declined progressively from 1987 to 2000 (secular improvement)

### Sites of locomotor injuries (1987–1996)

| Setting  | Total n | Fracture in Limb | Forelimb | Hind limb | Other (skull/spine) | Soft tissue injury |
| -------- | ------- | ---------------- | -------- | --------- | ------------------- | ------------------ |
| Training | 10,710  | 97.3%            | 71.9%    | 25.4%     | 0.8%                | 1.9%               |
| Racing   | 8,484   | 90.6%            | 78.1%    | 12.5%     | 0.3%                | 9.1%               |

Training fracture incidence (1981–1997): 16,884 fractures in 18,884,329 horse entries = **0.089%**.

### Most common fracture types (JRA racing context)

1. Distal condylar fracture of McIII/MtIII (most common catastrophic fracture)
2. Fractures and injuries of the suspensory apparatus comprising PSBs and suspensory interosseous ligament
3. Carpal fractures; phalangeal fractures

### Fracture location on track (patrol video, n=259 catastrophic fractures, 1983–1986)

- **Most frequent locations:** third and fourth corners; home stretch
- **Also frequent:** at time of leading-limb change
- On straights and when exiting a turn onto a straight: leading leg and injured leg highly correlated
- During turns and when entering a turn: low correlation between leading leg and injured leg
- **Many accidents occurred just after a lead change** (confirmed also in UK)

### Effect of track reconstruction (Hanshin Racetrack)

- Pre-remodel: tight 3rd–4th corners with straights between, requiring frequent lead changes and speed reductions
- Post-remodel: more gradual corners, increased radius of curvature, straights converted to gentle curves
- Result: **outstanding reduction in serious fractures** (more reduction in serious than slight fractures)
- Average racing time was **2.37 seconds slower** after reconstruction on turf and dirt for all distances

### Relationship between track condition and fracture incidence

- **Dirt:** Fracture risk increased significantly as conditions worsened: Fast → Good → Muddy → Sloppy (P<0.001, Cochran-Armitage test)
- **Turf:** Fracture risk tended to **decrease** as conditions worsened (wetter/softer): P=0.0711 (Cochran-Armitage test; trend)
- Note: This is **opposite** to NYRA dirt (where muddy = lower risk than fast) — attributed to JRA fine-particle river sand vs. NYRA clay/silt/sand absorbing moisture differently

### Effect of track condition on racing time (Tables 2 & 3)

**Nakayama Racetrack (1990–1994), Turf:**

| Distance (m) | Firm (s)    | Good (s)    | Yielding (s) | Soft (s)    |
| ------------ | ----------- | ----------- | ------------ | ----------- |
| 1,200        | 70.8 ± 1.4  | 71.6 ± 1.4  | 72.1 ± 1.0   | 73.6 ± 1.6  |
| 1,600        | 96.9 ± 1.1  | 97.7 ± 1.4  | 98.9 ± 0.9   | 99.9 ± 1.1  |
| 1,800        | 110.9 ± 1.4 | 112.0 ± 1.8 | 111.5 ± 1.6  | 115.1 ± 1.8 |
| 2,200        | 136.3 ± 1.5 | 137.2 ± 1.3 | 138.3 ± 1.1  | 140.6 ± 1.2 |
| 2,500        | 155.5 ± 1.6 | 156.7 ± 1.2 | 160.2 ± 1.4  | 160.7 ± 1.0 |

Numbers in parentheses in original: (winners/runners). Significant differences marked A,B (P<0.01) or a,b (P<0.05) between groups.

**Nakayama Racetrack (1990–1994), Dirt:**

| Distance (m) | Fast (s)    | Good (s)    | Muddy (s)   | Sloppy (s)  |
| ------------ | ----------- | ----------- | ----------- | ----------- |
| 1,200        | 75.1 ± 0.9  | 74.1 ± 1.2  | 73.8 ± 1.6  | 74.1 ± 1.2  |
| 1,800        | 117.3 ± 1.4 | 116.1 ± 2.0 | 115.4 ± 1.4 | 115.8 ± 1.5 |

Note: On dirt, Muddy and Sloppy produce **faster** average times than Fast — inverse of turf. Statistically significant differences exist (P<0.01) between Fast and other conditions for 1,200 m and 1,800 m.

**Kyoto Racetrack (1990–1994), Turf (partial Table 3):**

| Distance (m) | Firm (s)   | Good (s)   | Yielding (s) | Soft (s)   |
| ------------ | ---------- | ---------- | ------------ | ---------- |
| 1,200        | 71.6 ± 1.2 | 72.1 ± 0.9 | 72.0 ± 1.1   | 72.2 ± 1.0 |
| 1,400        | 84.5 ± 1.2 | 85.4 ± 1.2 | 84.8 ± 1.0   | 86.0 ± 1.3 |
| 1,600        | 97.4 ± 1.4 | 97.2 ± 1.0 | 97.3 ± 1.1   | 99.5 ± 1.5 |

### VGRF and track surface

- Rough/uneven track surface: higher variance in peak VGRF on trailing forelimb during galloping
- Harrowed (smooth) surface: significantly decreased variance in peak VGRF (P significant)
- Implication: surface unevenness increases asymmetric loading → injury risk; regular harrowing reduces risk

### Bucked shins incidence in 2yo racehorses

- JRA: **66% in 2yo Thoroughbreds during first 8 months of training** (vs. 24–25% UK/USA, 65% Australia)
- No difference in incidence between groups exercising once vs. twice per week at maximal speed

### Jockey falls (1998–2000)

- JRA flat racing: 1.62 falls per 1,000 rides (comparable to California 1.62, Australia 1.43)
- Jockey injury rate per fall: data not yet available for Japan at time of publication

## Discussion & interpretation

The paper identifies several JRA-specific findings that differ from international data:

1. **Higher overall fracture incidence (1.83%) vs. NYRA (0.35–0.73% non-catastrophic; 0.08–0.185% catastrophic):** Explained by more comprehensive case ascertainment by JRA (all horses on one system; includes minor chip fractures), not necessarily higher true incidence.

2. **JRA dirt track fracture pattern (wetter = more fractures):** Opposite to NYRA (wetter = fewer fractures). Attributed to JRA fine river sand, which becomes slippery when wet rather than cohesive, increasing sliding/shear forces.

3. **Hanshin track geometry reconstruction:** One of the clearest natural experiments demonstrating that racetrack geometry (corner radius, number of lead changes) causally reduces catastrophic fractures. This is directly relevant for designing risk features based on track geometry.

4. **Track hardness context:** JRA standard 80–130 G (non-race days). JRA turf has been characterised as hard by international observers; the paper shows Saint Cloud and Chantilly are actually harder. Nevertheless, the hardness distribution across the track (measured at 5 m intervals) affects loading patterns.

The paper emphasises multifactorial causation: biological factors (subclinical bone lesions, conformation, age, sex) interact with non-biological factors (track condition, geometry, surface composition, speed, training load). For risk models, biological and non-biological factors should be modelled as main effects AND interactions.

## Limitations

- Data from 1980s–2000s; track conditions, surfaces, and veterinary practices have evolved since.
- Fracture case definition (≥3 months absence) excludes horses that competed with mild chip fractures or recovered quickly; true incidence may differ.
- Descriptive epidemiology; no multivariate risk-factor model with individual horse data — cannot estimate independent effects of each variable.
- Patrol video analysis (259 catastrophic fractures, 1983–1986) is a small, old sample; fracture location patterns may differ with newer track geometry.
- Training fracture incidence (0.089%) is based on total "horse entries" to training, not standardised by exercise exposure; denominator is imprecise.
- Bucked shins incidence comparison (JRA 66% vs. UK 24–65%) may reflect differences in study design, age range, and diagnostic criteria rather than true geographic differences.

## Feature-engineering notes for the model

- `track_surface_type` — categorical: dirt/turf — source: JRA race records — expected effect: dirt = faster times on muddy/sloppy (opposite to turf); dirt = higher fracture risk on heavy conditions; encode with going interaction
- `going_grade_turf` — ordinal: Firm=0, Good=1, Yielding=2, Soft=3 — source: JRA race records — expected effect: monotone increase in race time (70.8 → 73.6 s for 1,200 m Nakayama turf); positive coefficient on race time → negative predictor for horses suited to fast conditions
- `going_grade_dirt` — ordinal: Fast=0, Good=1, Muddy=2, Sloppy=3 — source: JRA race records — expected effect: non-monotone; Muddy/Sloppy slightly FASTER than Fast for JRA dirt (74.1 → 73.8 s for 1,200 m Nakayama dirt) — use surface × going interaction, NOT the same encoding as turf
- `racecourse_id` — categorical (10 JRA racecourses) — source: JRA race records — expected effect: significant independent effect on racing time; fixed effects required per venue
- `race_distance_m` — continuous (metres) — source: JRA race records — expected effect: strong positive predictor of race time; required as denominator for time normalisation
- `track_going_injury_risk` — derived binary: going = Muddy/Sloppy on dirt — expected effect: elevated fracture/scratch risk; positive predictor of non-completion
- `corner_count` — number of turns in race — source: JRA course layout data — expected effect: more corners → more lead changes → higher injury risk; Hanshin remodel showed causal effect
- `secular_year` — race year (1987–2000 trend: decreasing incidence) — source: race records — expected effect: captures systematic safety improvements; include as a confound or time trend
- `leading_limb_change_zone` — binary: does course contain tight corners requiring lead changes? — source: JRA course geometry — expected effect: higher fracture risk per race
- `bucked_shins_history_2yo` — binary: documented bucked shins in 2yo training period (JRA 66% base rate) — source: medical records — expected effect: positive predictor of absence/scratch; 66% base rate means most 2yo Thoroughbreds have experienced this

## Key references / follow-up leads

- Oikawa M, Kusunose R. 2005. Vet. J. [Racing time, racecourse, and track condition analysis at JRA, 2000–2004; source of Tables 2 and 3 in this paper]
- Kaneko M et al. 1980. [First paper emphasising pre-existing focal osteochondral sclerosis as precursor of condylar fractures; landmark JRA finding]
- Kai M et al. [JRA instrumented shoe for VGRF measurement during galloping; device description and VGRF results on rough vs. smooth surfaces]
- Oikawa M et al. [Patrol video analysis of 259 catastrophic fractures on 10 JRA racecourses, 1983–1986; fracture location at corners and lead-change points]
- Hill TW. 2003. Survey of injuries in Thoroughbred at New York Racing Association tracks. Clin. Tech. Equine Prac. 2: 323–328. [NYRA comparison data: 0.35–0.73% non-catastrophic; 0.08–0.185% catastrophic]
- Verheyen KLP, Wood JLN. [UK fracture epidemiology; international comparator]
- Parkin TDH. [Fracture epidemiology reviews; international comparator]
- Companion papers: 27_1529 (radiographic risk at sales), 28_1702 (PSB fracture in foals)
