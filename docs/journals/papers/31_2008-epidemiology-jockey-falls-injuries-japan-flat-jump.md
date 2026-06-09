# Epidemiology of jockey falls and injuries in flat and jump races in Japan (2003–2017)

## Metadata

| Field                          | Value                                                                                                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 31(4): 101–104, 2020                                                                                                                                                                                                           |
| docid                          | `31_2008`                                                                                                                                                                                                                                     |
| Article type                   | Note                                                                                                                                                                                                                                          |
| Authors                        | Fumiaki Mizobe, Yuji Takahashi, Kanichi Kusano                                                                                                                                                                                                |
| Affiliations                   | Racehorse Hospital, Miho Training Center, Japan Racing Association, Ibaraki 300-0493, Japan; Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan; Equine Department, Japan Racing Association, Tokyo 106-8401, Japan |
| Received / Accepted / Released | April 13, 2020 / October 5, 2020 / not stated                                                                                                                                                                                                 |
| Keywords                       | epidemiology, horse racing, jockey fall, jockey injury                                                                                                                                                                                        |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/31/4/31_2008/_pdf/-char/en                                                                                                                                                                           |

## Abstract (verbatim)

> Jockey safety is of paramount importance from the standpoint of welfare and public perception. Thus, an understanding of the epidemiology and associated risk factors is necessary to implement measures to reduce the jockey falls (JFs) and jockey injuries (JIs). This descriptive epidemiological study investigated the occurrence of JFs and JIs in 715,210 and 25,183 rides in flat and jump races, respectively, from 2003 to 2017. In flat races, the incidence rates of JFs and JIs were 1.4 and 0.6 per 1,000 rides, respectively. In jump races, they were 44.4 and 18.1 per 1,000 rides, respectively. In flat races, 56.8% of JFs at corners resulted in JIs. In jump races, the major causes of JFs and JIs were lost balance and hampered by a fallen horse at an obstacle. Our findings provide a basis to design a future study analyzing risk factors for JFs.

## Relevance to finishing-position (着順) prediction

This paper belongs to the **race-event-risk feature family** (cross-cutting A=injury/soundness and G=statistical-modelling). It provides 15-year JRA incidence data (n=715,210 flat rides; n=25,183 jump rides) for jockey falls (JF) and jockey injuries (JI), stratified by race type, location within the race, and cause. Jockey falls cause immediate DNF for the horse, creating systematic bias in finishing-position records.

Key implications for finishing-position prediction:

1. **Race type is the dominant binary risk factor**: jump races carry 31.7× higher JF rate (44.4 vs. 1.4/1,000 rides), making race type the single most important DNF-risk discriminator. Jump-race finishing distributions are fundamentally different from flat — all jump-race horse entries should carry an elevated prior probability of not finishing (or finishing poorly due to obstacle difficulty).

2. **Within flat races**: corners are highest-severity locations (56.8% JF→JI conversion vs. 19.8% shortly-after-start). Short-distance turf races at tight oval courses have more corner exposure per unit distance, raising field-interaction risk. Field size drives corner crowding.

3. **Horse fatality as JF cause**: 53.1% of horse-fatality-caused JFs resulted in JI — the highest rate among falls caused by biomechanical horse failure (vs. 20.7% for stumbling falls). Horse health/vet-check features therefore flow through to jockey safety risk and to race-level completion risk.

4. **Cause distribution at obstacles in jump races**: "lost balance" at steeplechase fences accounts for the vast majority of JFs (787/972 total lost-balance JFs) but has lower JI conversion (40.5%) vs. "lost balance at hurdles" (91/167, 54.5%); hurdle JFs are relatively more severe.

This is a descriptive study; no multivariate risk model is presented. The JRA data match the prediction target population (JRA flat and jump, 10+8 racecourses, turf + dirt, 1,000–4,260 m).

## Background & objective

Jockey falls are the primary mechanism by which horse health events translate into human injury and publicly visible racing accidents. Prior JRA-specific data (Oikawa 2004) covered 1998–2000 and reported raw JF counts (211/130,433 flat rides; 282/4,220 jump rides) but did not fully characterise JI rates. International comparisons were also needed given differences in race regulations, fence types, and distances across countries (France, GB, Ireland, Australia, USA).

The study aimed to: (1) determine JF and JI incidence rates in JRA races 2003–2017; (2) characterise causes and locations of JFs and JIs separately for flat and jump; (3) compare JRA rates with international benchmarks.

Definitions used:

- **JF (jockey fall)**: jockey dislodged from horse during a race (start-to-finish-line interval only; pre-start and post-finish excluded)
- **JI (jockey injury)**: JF that resulted in an orthopedic disorder confirmed by medical certificate from racecourse clinic or hospital
- Incidence rate = (number of events / number of rides) × 1,000

## Materials & methods

**Data source:** Official accident reports compiled by race stewards (Stewards' reports), Japan Racing Association, January 1, 2003 to December 31, 2017.

**Coverage:**

- Flat races: 715,210 rides at 10 racecourses; turf and dirt; 1,000–3,600 m
- Jump races: 25,183 rides at 8 racecourses; hurdles (120–130 cm, mobile brush-topped obstacles) and steeplechase fences (120–160 cm, immobile hedges ± solid base); 2,750–4,260 m; minimum 8 obstacles per race

**Categorisation of JF causes:**

- Flat: stumbled, clipped heels, horse fatality, hampered by fallen horse, shifted ground abruptly, horse behavior, others
- Jump: lost balance, hampered by fallen horse, clipped heels, horse fatality, stumbled, others

**Categorisation of JF locations:**

- Flat: shortly after start, corner, stretch
- Jump: shortly after start, corner, stretch, slope, hurdle, steeplechase fence

**Analysis:** Descriptive statistics only. Conversion rate = (number of JIs / number of JFs) × 100%. No multivariate statistical modelling.

## Results (detailed — reproduce ALL numbers)

**Flat races: overall (n=715,210 rides):**

- Total JFs: 992; JF rate: 1.4/1,000 rides
- Total JIs: 399; JI rate: 0.6/1,000 rides
- JF → JI conversion rate: 40.2%

**Flat races — JFs and JIs by location and cause (full table):**

| Cause              | Shortly after start | Corner            | Stretch           | Total             |
| ------------------ | ------------------- | ----------------- | ----------------- | ----------------- |
|                    | JF / JI / JI%       | JF / JI / JI%     | JF / JI / JI%     | JF / JI / JI%     |
| Stumbled           | 310/57/18.4%        | 8/7/87.5%         | 6/3/50.0%         | 324/67/20.7%      |
| Clipped heels      | 28/12/42.9%         | 115/72/62.6%      | 61/36/59.0%       | 204/120/58.8%     |
| Horse fatality     | 3/0/0%              | 85/50/58.8%       | 91/45/49.5%       | 179/95/53.1%      |
| Hampered by fallen | 4/2/50.0%           | 103/50/48.5%      | 43/19/44.2%       | 150/71/47.3%      |
| Shifted ground     | 35/6/17.1%          | 15/8/53.3%        | 24/18/75.0%       | 74/32/43.2%       |
| Horse behavior     | 33/5/15.2%          | 3/1/33.3%         | 2/1/50.0%         | 38/7/18.4%        |
| Others             | 11/2/18.2%          | 4/1/25.0%         | 8/4/50.0%         | 23/7/30.4%        |
| **Total**          | **424/84/19.8%**    | **333/189/56.8%** | **235/126/53.6%** | **992/399/40.2%** |

**Notable flat-race findings:**

- Largest JF count: stumbled shortly after start (310/324 stumbled falls = 95.7% at start); but lowest JI rate (18.4%) because lower speed at start
- Highest JI conversion cause: "shifted ground abruptly" at stretch (75.0%) and at corner (53.3%)
- Horse fatality and clipped heels: highest JI conversion overall (53.1% and 58.8%)
- Corner: 333 JFs, 189 JIs, 56.8% conversion — highest injury severity location
- Stretch: 235 JFs, 126 JIs, 53.6% — high speed = high injury force

**Jump races: overall (n=25,183 rides):**

- Total JFs: 1,117; JF rate: 44.4/1,000 rides
- Total JIs: 458; JI rate: 18.1/1,000 rides
- JF → JI conversion rate: 41.0%
- JF rate ratio (jump vs. flat): 44.4/1.4 = 31.7×

**Jump races — JFs and JIs by location and cause (full table):**

| Cause           | Shortly after start | Corner         | Stretch       | Slope          | Hurdle           | S-chase fence     | Total               |
| --------------- | ------------------- | -------------- | ------------- | -------------- | ---------------- | ----------------- | ------------------- |
|                 | JF/JI/%             | JF/JI/%        | JF/JI/%       | JF/JI/%        | JF/JI/%          | JF/JI/%           | JF/JI/%             |
| Lost balance    | 1/0/0%              | 8/0/0%         | 0/0/—         | 9/6/66.7%      | 167/91/54.5%     | 787/319/40.5%     | 972/416/42.8%       |
| Hampered fallen | 1/0/0%              | 3/1/33.3%      | 1/0/0%        | 1/1/100%       | 14/6/42.9%       | 76/24/31.6%       | 96/32/33.3%         |
| Clipped heels   | 0/—/—               | 4/2/50.0%      | 0/—/—         | 0/—/—          | 6/0/0%           | 17/4/23.5%        | 27/6/22.2%          |
| Horse fatality  | 0/—/—               | 2/0/0%         | 5/1/20.0%     | 1/0/0%         | 0/—/—            | 2/0/0%            | 10/1/10.0%          |
| Stumbled        | 6/1/16.7%           | 0/—/—          | 1/0/0%        | 2/1/50.0%      | 0/—/—            | 0/—/—             | 9/2/22.2%           |
| Others          | 1/0/0%              | 0/—/—          | 0/—/—         | 0/—/—          | 0/—/—            | 2/1/50.0%         | 3/1/33.3%           |
| **Total**       | **9/1/11.1%**       | **17/3/17.6%** | **7/1/14.3%** | **13/8/61.5%** | **187/97/51.9%** | **884/348/39.4%** | **1,117/458/41.0%** |

**Notable jump-race findings:**

- Steeplechase fence: largest absolute number of JFs (884) and JIs (348); 39.4% conversion
- Hurdle: 187 JFs, 97 JIs, 51.9% conversion — higher injury rate per fall than steeplechase fences despite lower fence height; authors speculate faster approach speed at lower hurdles
- Slope: highest JI/JF conversion (61.5%) despite low absolute n (13 JFs)
- Lost balance at steeplechase fence: 787 JFs (70.4% of all jump JFs), 319 JIs (40.5% conversion)

**International comparison (per 1,000 rides):**

|         | Japan | Australia | France | GB   | Ireland | USA |
| ------- | ----- | --------- | ------ | ---- | ------- | --- |
| Flat JF | 1.4   | 1.4       | 3.1    | 4.4  | 3.8     | 1.6 |
| Flat JI | 0.6   | 0.7       | 1.2    | 1.8  | 1.4     | 0.8 |
| Jump JF | 44.4  | 52.6      | 91.4   | 67.7 | 49.5    | —   |
| Jump JI | 18.1  | 5.1       | 11.9   | 12.2 | 10.1    | —   |

**Notable:** JRA jump JI rate (18.1) is substantially higher than all international comparisons (5.1–12.2) despite lower JF rate than France/GB. The reasons for the higher JI conversion in Japan remain unclear from this descriptive study.

## Discussion & interpretation

JRA flat JF rates are comparable to Australia (1.4) and USA (1.6) but lower than France (3.1), GB (4.4), and Ireland (3.8). Part of the difference is methodological: France, GB, and Ireland include JFs from parade ring mount to dismount after race, whereas the JRA definition is start-to-finish-line only.

Corners are the highest-severity location in flat races (56.8% JI conversion). At corners in large fields, horses run in close lateral proximity; a fallen jockey is highly likely to be struck by subsequent horses. The severity at the stretch (53.6%) reflects high running speed at impact.

Stumbling at the start accounts for the most JFs (324) but the lowest JI rate (20.7%) because horses are accelerating from low speed; the fall kinetic energy is lower. Clipped heels (one horse stepping on the hind feet of the horse in front) has 58.8% JI conversion — among the highest of any cause — as it typically causes sudden, uncontrolled falls.

Horse fatality is a significant JI-causing mechanism in flat races: 179 JFs from horse fatalities, 95 JIs (53.1%). This links horse health surveillance (vet scratches, lameness history, cardiac screening) to jockey safety outcomes — a pathway linking injury-prevention and finishing-position modelling.

In jump races, the higher JI rate vs. international comparisons is unexplained; potential contributors include Japan's hurdle height/approach angle design, jockey skill distribution, ground hardness, or differences in medical certificate thresholds. This warrants the analytic follow-up the authors explicitly call for.

## Limitations

- **Descriptive only**: no risk-factor modelling; causes are recorded by stewards' judgment rather than objective measurement, introducing classification uncertainty
- **JF definition (start-to-finish only)**: underestimates true incidence vs. France/GB/Ireland studies that include parade ring period; international comparisons should be interpreted cautiously
- **JI definition varies internationally**: orthopedic medical certificate criteria differ across countries; JI rates are not directly comparable
- **No horse-level or jockey-level covariates** analysed: age, weight, field size, track condition, race distance, jockey experience are all known or suspected risk factors (cited from Australian literature) but not examined here
- **Temporal trends**: 15-year pooled data; secular trends in safety equipment, fence design, or reporting practices are not assessed
- **Jump races at 8 specific courses**: venue-specific obstacle designs differ; pooled results mask course-level heterogeneity

## Feature-engineering notes for the model

- `race_type_jump_flag` — binary (jump/flat); jump races carry 31.7× higher JF rate → categorical DNF risk prior; expected massive downward adjustment to expected finishing position for all jump-race entrants
- `race_type_jump_dnf_prior` — parametric: flat DNF-from-JF rate ≈ 0.14% (1.4/1,000 rides); jump DNF-from-JF rate ≈ 4.4% (44.4/1,000 rides); add to horse-specific health/vet prior for total DNF risk
- `field_size` — larger fields → higher corner density → higher clipped-heels and hampered-by-fallen risk; source: race entry data; expected positive association with both JF risk and finish-position variance
- `corner_count_per_distance` — proxy: turf oval short courses vs. dirt long courses vs. straight turf courses; courses with tight corners or frequent corner sequences per km raise corner-interaction risk; derivable from racecourse geometry data
- `horse_fatality_risk_flag` — horse fatality is a leading cause of JF with high JI conversion (53.1%); veterinary health flags, lameness history, and stress-fracture risk markers proxy this; expected: horse_fatality_risk ↑ → all horses in the race have higher DNF risk (hazard propagation effect)
- `race_distance_m` — shorter flat distances have more stumble-at-start events but lower JI conversion; longer distances have more stretch/corner falls with higher JI conversion; non-linear U-shaped safety profile
- `race_distance_jump_obstacle_count` — for jump races, number of obstacles ≈ distance/obstacle_interval; more obstacles = more JF opportunities; not directly encoded in standard race data but can be approximated from JRA jump-race distance
- `jockey_corner_fall_rate_career` — jockey-level feature: career proportion of flat falls occurring at corners vs. start; high corner-fall rate jockeys may be systematically lower-quality in tight field situations; source: JRA jockey accident records (not public)
- **Do NOT use** this study's JI conversion rates directly as horse finishing-position predictors — JI is a rider injury metric; the relevant downstream variable is horse DNF probability

## Key references / follow-up leads

- Hitchens PL, Blizzard CL, Jones G, Day LM, Fell J. 2009. The incidence of race-day jockey falls in Australia, 2002–2006. Med. J. Aust. 190: 83–86. — comparative Australian flat/jump rates
- Hitchens PL, Blizzard CL, et al. 2010. Predictors of race-day jockey falls in flat racing in Australia. Occup. Environ. Med. 67: 693–698. — multivariate risk-factor model (race grade, distance, field size) — the analytic follow-up this JRA study calls for
- McCrory P, Turner M, et al. 2006. An analysis of injuries resulting from professional horse racing in France during 1991–2001: a comparison with GB 1992–2001. Br. J. Sports Med. 40: 614–618. — France/GB data used in international comparison
- O'Connor S, Warrington G, et al. 2017. Epidemiology of injury due to race-day jockey falls in professional flat and jump horse racing in Ireland, 2011–2015. J. Athl. Train. 52: 1140–1146. — Ireland data
- Pinchbeck GL, Clegg PD, et al. 2003. Case-control study to investigate risk factors for horse falls in hurdle racing in England and Wales. Vet. Rec. 152: 583–587. — risk-factor model for jump races (fence type, horse/jockey experience)
- Wylie CE, McManus P, et al. 2017. Thoroughbred fatality and associated jockey falls and injuries in races in New South Wales and the Australian Capital Territory, Australia: 2009–2014. Vet. J. 227: 1–7. — horse fatality → jockey safety linkage
