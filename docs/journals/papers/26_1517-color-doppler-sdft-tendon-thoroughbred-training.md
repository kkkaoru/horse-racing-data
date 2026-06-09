# A Study of the Distribution of Color Doppler Flows in the Superficial Digital Flexor Tendon of Young Thoroughbreds During Their Training Periods

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 26(4): 99–104, 2015                                                                                                                                                                                                                                                                                                            |
| docid                          | `26_1517`                                                                                                                                                                                                                                                                                                                                     |
| Article type                   | Original Article                                                                                                                                                                                                                                                                                                                              |
| Authors                        | Takashi HATAZOE, Yoshiro ENDO, Yohei IWAMOTO, Kenji KOROSUE, Taisuke KURODA, Saemi INOUE, Daiki MURATA, Seiji HOBO, Kazuhiro MISUMI                                                                                                                                                                                                           |
| Affiliations                   | Kyusyu Stallion Station, The Japan Bloodhorse Breeders' Association, Kagoshima 899-8313, Japan; Japan Racing Association Miyazaki Training Farm, Miyazaki 880-0036, Japan; Japan Racing Association Equine Research Institute, Tochigi 320-0856, Japan; Joint Faculty of Veterinary Medicine, Kagoshima University, Kagoshima 890-0065, Japan |
| Received / Accepted / Released | June 30, 2015 / August 21, 2015 / 2015                                                                                                                                                                                                                                                                                                        |
| Keywords                       | blood flow, color Doppler, tendon, Thoroughbred, training                                                                                                                                                                                                                                                                                     |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/26/4/26_1517/_pdf/-char/en                                                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> Aim of this study was to evaluate the relationships of exercise and tendon injury with Doppler flows appearing in the superficial digital flexor tendon (SDFT) of young Thoroughbreds during training periods. The forelimb SDFTs of 24 one- to two-year-old Thoroughbreds clinically free of any orthopaedic disorders were evaluated using grey-scale (GS) and color Doppler (CD) images during two training periods between December 2013 to April 2015. Twelve horses per year were examined in December, February, and April in training periods that began in September and ended in April. The SDFT was evaluated in 3 longitudinal images of equal lengths (labelled 1, 2, 3 in order from proximal to distal), and 6 transversal images separated by equal lengths (labelled 1A, 1B, 2A, 2B, 3A and 3B in order from proximal to distal) of the metacarpus using both GS and CD. The running (canter and gallop) distance for 1 month before the date of the ultrasonographic examinations was increased in December, February, and April in both of the two training periods. CD flows defined as rhythmically blinking or pulsatory colored signals were found in 56 of 864 (6.4%) transversal CD images, in 28, 12, 13, and 3 images of 1A, 1B, 2A and 2B, respectively, and in 7, 14, and 35 images captured in December, February, and April, respectively. There were no longitudinal or transversal GS images indicating injury in the SDFTs in either of the two training periods. The increase of CD flows in the proximal regions of the SDFT are possibly related to the increase of the running distance during the training periods of the one- to two-year-old Thoroughbreds. Because no injury was diagnosed in the SDFTs by GS images during the training periods, the increase of CD flows in the proximal parts of SDFT is not necessarily predictive of tendon injury in the near future during the training period of young Thoroughbreds.

## Relevance to finishing-position (着順) prediction

Feature family **A (injury/soundness — soft tissue)**. SDFT injury is the most common career-threatening soft tissue injury in Thoroughbred racehorses; horses missing races due to SDFT tendinopathy or withdrawal risk from impending injury are a major source of scratch/DNF events.

This study's primary finding is paradoxically useful: intra-tendinous blood flow detected by colour Doppler (CD) increases with training load in healthy horses, without any macroscopic injury. This means that training-load ramp-up features (rapid increase in cumulative running distance per month) inherently produce a CD signal that, in the pre-clinical literature, was associated with early injury — but this study shows the signal is a normal physiological response to exercise load in young horses. For a race model, this means:

1. Training density features (km/month, races/month, ramp rate) correlate with tendon vascular loading but NOT with near-term macroscopic injury in healthy young horses;
2. The meaningful injury signal is a grey-scale (macroscopic) ultrasonography finding, not a CD finding alone;
3. Acute ramp-ups in training load remain valid proxies for injury risk at the population level (epidemiological data), even though this cohort showed no injury at 9.8–54.0 km/month.

For the model pipeline, the primary usable features are: `races_last_30d`, `km_last_30d` (if training logs available), and `rest_days_since_last_race` as proxies for cumulative tendon load.

## Background & objective

SDFT injury is intractable and recurrence-prone in Thoroughbreds; understanding the pre-clinical phase is critical for prevention. CD ultrasonography detects intra-tendinous blood flow that may signal early vascular response before macroscopic damage. Previous studies in human Achilles tendon found CD flow associated with training load (badminton hours, weekly running) independent of injury. An equine study by the same group found histologically increased blood vessel count in SDFT of horses with a history of rupture. The aim was to evaluate whether CD flows in SDFT of young healthy Thoroughbreds during training relate to exercise load, and whether CD flows precede injury.

## Materials & methods

**Animals:** 24 one- to two-year-old Thoroughbreds (12 males, 12 females); clinically free of all orthopaedic disorders and history. Split into two consecutive cohorts of 12 (6 male, 6 female each):

- Cohort 1: December 2013 – April 2014
- Cohort 2: December 2014 – April 2015

Breaking and training began in September each year and ended in April.

**Running distance (canter + gallop, km/month, mean ± SD):**

| Month    | 2013/Cohort 1 (km/month) | 2014/Cohort 2 (km/month) |
| -------- | ------------------------ | ------------------------ |
| December | 9.6 ± 6.3                | 9.8 ± 5.2                |
| February | 47.7 ± 1.8               | 34.0 ± 9.6               |
| April    | 54.0 ± 18.1              | 45.6 ± 7.6               |

**Ultrasonography:** iU22 (Philips Medical Systems, Tokyo); linear array transducer, 5–12 MHz broadband, 50 mm effective aperture. Horses sedated with medetomidine (2 mg/head IV). After complete clipping of palmar metacarpus, transducer placed directly on skin with abundant gel. Maximum image depth 3.5 cm; focus at SDFT depth.

**Imaging zones:** Each SDFT examined at:

- 3 longitudinal images (zones 1–3, proximal to distal, equal thirds of metacarpus length)
- 6 transversal images (zones 1A, 1B, 2A, 2B, 3A, 3B, proximal to distal, separated by equal distances from proximal end at accessory carpal bone to distal end at proximal sesamoid bone)

Zones 1A and 1B are proximal; 3A and 3B are distal.

**Image recording:** GS images at 45 Hz for 15 s; CD images at 6–11 Hz for 30 s. Both forelimbs examined each session. Total: 6 images × 2 forelimbs × 24 horses × 3 examinations = 864 transversal CD images.

**CD flow grading (previously established grading system):**

- Grade 1: tiny to small colour activities rhythmically blinking at a site; flow rates not measurable by pulsed Doppler.
- Grade 2: unequivocal pulsatile dot colour activities; flow rates measurable by pulsed Doppler.
- Grade 3: unequivocal linear colour activities as blood streams with regular periodic direction changes. (None observed in this study.)

CD gain set uniformly at 77–85% of maximal gain (just below noise level) for all tendons.

**Macroscopic injury diagnosis:** GS images; hypoechoic/anechoic areas = injury criteria.

## Results (detailed — reproduce ALL numbers)

**Grey-scale (macroscopic injury):**

- Zero longitudinal or transversal GS images indicating injury in either training period across all 24 horses and 3 examinations.

**Color Doppler (total positive images):**

- 56 of 864 transversal CD images (6.4%) showed positive CD flows.
- Grade 1 (rhythmically blinking): 32 images.
- Grade 2 (pulsatile): 23 images.
- Grade 3 (linear streams): 0 images.
- (Note: 56 images but 32+23=55; one image category unspecified in text.)

**CD flow distribution by zone:**

| Zone               | Positive images |
| ------------------ | --------------- |
| 1A (most proximal) | 28              |
| 1B                 | 12              |
| 2A                 | 13              |
| 2B                 | 3               |
| 3A                 | 0               |
| 3B                 | 0               |

Zones 3A and 3B had zero positive images. The proximal zones (1A, 1B, 2A) accounted for 53/56 (94.6%) of all positive images.

**CD flow temporal distribution (by examination month):**

| Month    | Positive images | Horses with any CD flow |
| -------- | --------------- | ----------------------- |
| December | 7               | 7/24                    |
| February | 14              | 8/24                    |
| April    | 35              | 15/24                   |

CD flow increased monotonically with training ramp-up (December: ~9.7 km/month → April: ~49.8 km/month pooled average). Some horses showed positive CD flows that disappeared and reappeared between examinations (transient signal).

**Individual horse data (Table 2 of paper, both cohorts):** Per-horse, per-zone CD grade recorded for December, February, April. Horse-level data available in paper; most positive flows are grade 1 or 2 in zones 1A, 1B, 2A; none in 3A or 3B for any horse in either cohort.

## Discussion & interpretation

Normal tendons are hypovascular; angiogenesis is typically associated with injury and healing. The authors hypothesise that CD flows in healthy young Thoroughbreds represent transient increases in inherent vessel blood flow in response to exercise-induced hypoxia and/or hyperthermia of tendon tissue — a demand-driven vascular response rather than neovascularization from damage. Analogous findings from human Achilles tendon studies support this: Boesen et al. showed CD flow in healthy runners post-exercise; Andersson et al. found power Doppler flow in symptomatic and asymptomatic running subjects.

The proximal concentration of CD flows is consistent with blood supply anatomy: the primary intrinsic blood supply enters at the myotendinous junction proximally (perimysial vessels extending from the superficial digital flexor muscle into the proximal third of the tendon). This perimysial supply is the most responsive to exercise load.

The fact that CD flows were transient (appearing and disappearing) and not associated with macroscopic injury argues against their use as a clinical early-warning sign in healthy training horses. In contrast, a previous study by Murata et al. (2012) found CD flows in horses with active chronic tendinopathy (post-macroscopic injury), and Genovese et al. established that quantitative grey-scale analysis can predict prognosis after macroscopic SDFT injury.

Key difference from human Achilles tendon findings: One human study (Andersson et al.) found power Doppler in asymptomatic runners predictive of subsequent Achilles tendinopathy development. The present study does not follow horses after April to assess whether any CD-positive horses developed macroscopic tendinopathy. This is the primary limitation — the "no injury" conclusion is temporally limited to the 5-month training ramp-up period.

## Limitations

- Cohort size: 24 horses over two training seasons; no injury observed, so the sensitivity of CD for predicting injury cannot be estimated.
- Follow-up limited to April; horses were not tracked after the training season ended — eventual SDFT injuries may have developed in CD-positive horses.
- No control comparison (CD flows in non-training horses); the baseline vascularity in pre-training state is assumed to be low but not measured.
- Sedation with medetomidine may affect vasomotor tone and potentially alter CD signals, though this is not discussed.
- Transversal CD images used for analysis; longitudinal CD scans were inferior at detecting small grade 1/2 signals, so proximal zone distribution may be underestimated.
- The grading system (grades 1–3) is semi-quantitative; inter-rater reliability not reported.

## Feature-engineering notes for the model

- `races_last_30d` — integer count of races in 30 days preceding target race — derivation: race history records — expected effect: positive correlation with tendon load; indirect scratch-risk proxy — available in JRA race records
- `km_last_30d_proxy` — derived running distance: approximate from races × average race distance + training logs (if available) — derivation: race distance sum as training-load proxy — expected effect: mirrors CD flow ramp pattern; higher load = higher but transient vascular response, not direct injury predictor
- `rest_days_since_last_race` — integer — derivation: race history — expected effect: longer rest reduces cumulative load; very short intervals (<14 days) may indicate stressed tendon — available in JRA records
- `training_load_ramp_rate` — 30-day km minus prior 30-day km — derivation: training logs or race-interval proxy — expected effect: acute large increases indicate risk even without macroscopic injury signal; analogous to the December-to-April ramp in this study
- `sdft_injury_history_flag` — binary: horse has documented SDFT tendinopathy or tendon rupture in past — derivation: JRA veterinary records — expected effect: strong positive on future SDFT reinjury risk (SDFT tendinopathy is recurrence-prone; histology shows increased vessel count in ruptured tendon tissue) — data availability: restricted to JRA internal records
- `proximal_sdft_cd_flow` — if CD ultrasonography data become available: count of proximal-zone positive images — derivation: veterinary ultrasound records — expected effect: tracks training load (not direct injury predictor in young healthy horses) — data availability: not in public race records
- Caution: do NOT interpret CD flow presence alone as an injury signal; the key pathological signal is macroscopic grey-scale hypoechoic lesion, not CD flow.

## Key references / follow-up leads

- Murata D. et al. (2012) J. Vet. Med. Sci. 74:1639–1642 — CD ultrasonography in active SDFT tendinopathy; hyperaemia signal in injured horses
- Genovese R.L. et al. (1990) Vet. Clin. North Am. Equine Pract. — quantitative grey-scale analysis of SDFT injuries; prognosis after macroscopic injury
- Boesen M.I. et al. (2006) — CD flow in human Achilles tendons post-running exercise (symptomatic and non-symptomatic)
- Andersson G. et al. (2011) — power Doppler as prognostic factor for Achilles tendinopathy in asymptomatic runners
- Birch H.L., Rutter G.A. and Goodship A.E. (1997) Res. Vet. Sci. 62:93–97 — oxidative energy metabolism in equine tendon cells; hypoxia response
- 25_1315 (this corpus) — Wnt/β-catenin regulation of tenomodulin in equine BMSCs; repair pathway biology
- 25_1316 (this corpus) — SL peak force 11,957 N, SDFT 4,615 N, DDFT 5,076 N at trot; biomechanical loading context
