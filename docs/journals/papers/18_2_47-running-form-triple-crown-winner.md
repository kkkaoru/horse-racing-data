# Running Form Characteristics of the Triple Crown Winner in Japan

## Metadata

| Field                          | Value                                                                                                                                                                                                                         |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 18(2): 47–53, 2007                                                                                                                                                                                             |
| docid                          | `18_2_47`                                                                                                                                                                                                                     |
| Article type                   | Note                                                                                                                                                                                                                          |
| Authors                        | Toshiyuki Takahashi, Osamu Aoki, Atsushi Hiraga                                                                                                                                                                               |
| Affiliations                   | Sport Science Division, Equine Research Institute, Japan Racing Association, 321–4 Tokami-cho, Utsunomiya, Tochigi 320-0856; Education Center of Japan Farriers Association, 1829–2 Tsuruta-cho, Utsunomiya, Tochigi 320-0851 |
| Received / Accepted / Released | Accepted January 17, 2007                                                                                                                                                                                                     |
| Keywords                       | diagonal step length, overlap time, running form, stride frequency, stride length                                                                                                                                             |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/18/2/18_2_47/_pdf/-char/en                                                                                                                                                           |

## Abstract (verbatim)

> The purpose of this study was to describe the characteristics of fast race horses by analyzing the running form of Deep Impact, the undefeated Japanese triple crown winner in 2005. A high-speed video data of the Kikuka Syo race (Japanese St. Leger, JPN G1, 3,000 m, turf) was taken at a rate of 250 frames/sec. The high-speed video system was set in a left lateral position about 100 m before the finishing post with a field view width of about 16 m. The speed of Deep Impact, 17.8 m/sec, was the fastest of all horses measured (average 16.1 m/sec), the stride frequency, 2.36 strides/sec, was the third largest (average 2.28 strides/sec), and the stride length, 7.54 m, was the longest (average 7.08 m). The diagonal and airborne step lengths of Deep Impact were longer than the average values. The overlap time of Deep Impact was shorter than the average value. The ratio of overlap time to stride duration of Deep Impact was 8.5 %, whereas the average value was 16.9 %. A shorter overlap time was also observed on a common characteristic of Secretariat, the famous elite race horse in USA and correlated to running speed. Thus, these characteristics may be related to effective running form in elite horses.

## Relevance to finishing-position (着順) prediction

Feature family: **F — conformation / body size / gait**. This paper provides the most detailed high-speed kinematic analysis of an elite JRA racehorse (Deep Impact, undefeated 2005 Japanese Triple Crown winner) in a real JRA G1 race, establishing quantitative biomechanical benchmarks for superior Thoroughbred performance under actual race conditions.

The critical finding for the prediction pipeline is the negative correlation between **overlap time** (the period both paired limbs share ground contact) and running speed across all 13 measured horses: r = −0.8813, P < 0.0001. This correlation is not merely a single-horse observation — it holds across the field at the finish of a 3,000 m G1. Because overlap time increases with fatigue (Leach & Sprigings 1979) and on slippery track surfaces, it can serve as a late-race performance quality indicator: horses maintaining shorter overlap time late in races demonstrate superior fatigue resistance and biomechanical efficiency.

The pipeline cannot directly measure overlap time from race records, but the correlation motivates two proxy feature classes. First, **late-race speed maintenance** (final 200 m sectional time vs. peak sectional time) approximates the ability to maintain short overlap time under fatigue — horses that slow dramatically in the final furlong likely have increasing overlap times. Second, **stride length** was the strongest single differentiator (Deep Impact: 7.54 m vs. field mean 7.08 m, significant); stride length at given speed can be estimated from sectional-time/body-length ratios or pedigree-derived speed metrics. The confirmation that Deep Impact's airborne duration (0.124 s) was shorter than the field mean (0.134 ± 0.016 s) — not longer as popularly assumed — is a useful negative result preventing misguided feature engineering (jockey's "flying" sensation was actually from longer airborne **distance** at higher speed, not longer airborne time).

Track surface interacts strongly: preliminary data cited show overlap time increases on slippery turf and is larger on dirt (consistent with Secretariat's larger overlap time on dirt than Deep Impact's on firm turf). This supports including `going × speed_capacity` interactions in any surface-stratified model.

## Background & objective

Deep Impact won the 2005 Japanese Triple Crown (Satsuki Sho, Tokyo Yushun, Kikuka Sho) undefeated; his jockey described him as "running as though flying." Two prior observations motivated the study: (1) Deep Impact's hindlimb shoes showed less natural wear than typical strong horses, suggesting efficient hoof-ground propulsion; (2) the jockey's sensation of flying. The only comparable elite-horse gait study was Pratt & O'Connor 1978 on Secretariat. The aim was to characterise Deep Impact's running form using high-speed video and compare to fellow starters in a real JRA G1.

## Materials & methods

**Race:** Kikuka Sho (Japanese St. Leger, JPN G1, Kyoto Racecourse, Oct. 23, 2005; distance 3,000 m; track type: turf; condition: firm; restricted to 3-year-old colts and fillies). Finish time: 3:04.6. Last 800 m lap: 47.8 sec; last 600 m lap: 35.7 sec. Winner: Deep Impact (2-length margin to Admire Japan).

**Video system:** High-speed camera (HSV-500C3, nac Image Technology) at **250 frames/sec**, positioned laterally on the left side approximately **100 m before the finishing post**, field view width approximately **16 m**. At least two complete strides captured per horse in the field of view; one complete stride used for analysis.

**Measurable horses:** 13 of 16 starters (3 excluded because hooves not visible behind other horses).

**Temporal variables measured from frame timing:**

- Stride duration; airborne duration; stride frequency (strides/sec)
- Stance-phase durations of each limb (4 limbs: trailing hind, leading hind, trailing fore, leading fore)
- **Overlap time**: period when two legs are simultaneously in contact with ground
- Contact = frame before fetlock hyperextension; liftoff = frame after fetlock returns straight

**Spatial variables** (measured via 93-cm calibration bars placed at near/mid/far distances from camera; nearest bar to each horse used as standard; ImageJ software):

- Hind step length, diagonal step length, fore step length, airborne step length, stride length
- Limb angles (palmer side of metacarpal/metatarsal bone to ground surface) at ground contact and liftoff, for all 4 limbs

**Speed calculation:** stride length × stride frequency.

**Statistical method:** Shapiro-Wilk W-test confirmed normal distribution for all variables except stride length (one outlier). T-scores (adjusted deviation scores): T = 50 + (X − mean)/SD × 10. T-score < 40 or > 60 = significant (corresponds to ±15.87% from mean, i.e., outside ±1 SD in the T-score scale). Values reported as mean ± SD for the 13-horse field.

## Results (detailed — reproduce ALL numbers)

**Speed and stride kinematics:**

| Variable                           | Deep Impact | Field mean ± SD | T-score / significance         |
| ---------------------------------- | ----------- | --------------- | ------------------------------ |
| Speed (m/s)                        | 17.8        | 16.1            | Significantly fastest          |
| Stride frequency (strides/s)       | 2.36        | 2.28            | 3rd largest (not significant)  |
| Stride length (m)                  | 7.54        | 7.08            | Significantly longest (T > 60) |
| Airborne duration (s)              | 0.124       | 0.134 ± 0.016   | Not longer than average        |
| Overlap time / stride duration (%) | 8.5%        | 16.9%           | Significantly shorter (T < 40) |

**Step lengths:**

| Variable             | Deep Impact          | Field mean | Note                                                        |
| -------------------- | -------------------- | ---------- | ----------------------------------------------------------- |
| Airborne step length | Significantly longer | —          | T-score not stated numerically but described as significant |
| Diagonal step length | Longer (T = 59.5)    | —          | Tendency, just below significance threshold (T > 60)        |
| Hind step length     | Not different        | —          | —                                                           |
| Fore step length     | Not different        | —          | —                                                           |

**Stance durations (Fig. 2 data):** All limbs of Deep Impact had significantly shorter stance durations **except** the leading forelimb (which was not significantly different).

**Overlap time detail:** Especially notable shortening of overlap time between hindlimbs and between leading hindlimb and trailing forelimb. The time of contact of the trailing forelimb with the ground "tended to be later" (T-score = 59.7 — just below significance threshold), allowing extension of body and longer diagonal step.

**Limb angles at contact and liftoff (Table 1):**

| Limb          | Variable               | Deep Impact | Field mean ± SD | Significant? |
| ------------- | ---------------------- | ----------- | --------------- | ------------ |
| Trailing Hind | Angle at contact (deg) | 52.4        | 53.6 ± 5.1      | No           |
| Trailing Hind | Angle at liftoff (deg) | 130.5       | 129.0 ± 3.0     | No           |
| Trailing Hind | Range of motion (deg)  | 78.1        | 75.3 ± 6.4      | No           |
| Leading Hind  | Angle at contact (deg) | 50.2        | 47.7 ± 4.1      | No           |
| Leading Hind  | Angle at liftoff (deg) | **132.7\*** | 127.8 ± 3.3     | Yes (T > 60) |
| Leading Hind  | Range of motion (deg)  | 82.5        | 80.1 ± 6.3      | No           |
| Trailing Fore | Angle at contact (deg) | 59.3        | 58.8 ± 3.2      | No           |
| Trailing Fore | Angle at liftoff (deg) | 139.4       | 137.5 ± 2.9     | No           |
| Trailing Fore | Range of motion (deg)  | 80.1        | 78.8 ± 4.1      | No           |
| Leading Fore  | Angle at contact (deg) | 51.9        | 55.6 ± 4.6      | No           |
| Leading Fore  | Angle at liftoff (deg) | **143.1\*** | 137.0 ± 2.6     | Yes (T > 60) |
| Leading Fore  | Range of motion (deg)  | **91.2\***  | 81.3 ± 6.4      | Yes (T > 60) |

\*Significant (T-score > 60 or < 40).

**Speed–overlap time correlation (across all 13 horses):**

- r = −0.8813, P < 0.0001
- Faster horses had shorter overlap time; slower/more fatigued horses had longer overlap time.

**Secretariat comparison (from Pratt & O'Connor 1978):**

- Secretariat overlap time: 0.081 s; ratio-to-stride duration: 18.6%
- Deep Impact overlap time ratio: 8.5% (smaller than Secretariat's ratio)
- Secretariat stride length: 7.38 m; Deep Impact: 7.54 m; ordinary Thoroughbred mean: 6.66–6.83 m (Ishii et al. 1989) or range 6.1–7.7 m (Pratt 1983)
- Deep Impact's overlap time is smaller than Secretariat's; authors attribute this to surface differences (firm turf in Japan vs. soft cushion dirt in USA — Secretariat likely had larger overlap on dirt).

**Stride frequency upper limits:** Reported maximum in prior literature ~2.5 strides/sec (Hiraga et al. 1994; Ishii et al. 1989); maximum in this race 2.4 strides/sec — near the physiological ceiling. Speed gains must therefore come primarily from **stride length**, not frequency.

**Hindlimb shoe wear note:** Deep Impact's hindlimb shoes showed less natural wear than typical strong horses. Authors interpret this as efficient propulsion without hoof-ground sliding.

## Discussion & interpretation

Deep Impact's superior speed (17.8 m/s at the finish of 3,000 m) was achieved through the longest stride length in the field, not exceptional stride frequency. The airborne duration (0.124 s) was actually slightly shorter than average — the popular "flying" impression came from longer airborne distance (function of speed × airborne duration) despite shorter airborne time. The shortened overlap time — especially between hindlimbs and between leading hindlimb and trailing forelimb — enables body extension and longer diagonal step, a mechanism shared with Secretariat. The enlarged leading forelimb liftoff angle and range of motion (91.2° vs. 81.3°) indicate more forward-bending at liftoff, reducing vertical displacement and minimising energy lost in up-down body motion. On slippery conditions or dirt, overlap time and ratio-to-stride duration increased even for Deep Impact (unpublished data cited), but the negative correlation with speed persisted, confirming that short overlap time is speed-enabling regardless of surface.

## Limitations

- Single race (one G1, one date); no longitudinal or multiple-race comparison for Deep Impact.
- 13 of 16 starters measurable; 3 excluded (could not observe hooves).
- One stride analysed per horse; at one point in the race (~100 m from finish); earlier in the race or mid-race kinematics not examined (Leach et al. 1987 used different timing).
- Calibration bars at three distances introduce spatial measurement error depending on horse position relative to camera.
- Angle measurements from 2D lateral projection; 3D components not captured.
- No conformation or physical measurements of individual horses; cannot relate kinematics to morphology.

## Feature-engineering notes for the model

- `late_race_speed_drop` — (peak 200m sectional speed − final 200m sectional speed) / peak 200m speed — derivable from JRA sectional timing data — expected effect: proxy for overlap time increase under fatigue; larger drop → poorer fatigue resistance → lower finishing position — data availability: requires sectional times (available for some JRA races)
- `stride_length_proxy` — approximate stride length from sectional time / body-length norm (body length ~2.5 m for Thoroughbreds), or pedigree sprint-distance aptitude index — data availability: only indirectly derivable
- `surface_overlap_interaction` — surface type (turf/dirt/sand) × fitness score — motivated by significant overlap time difference across surfaces — derivable from race condition records — data availability: fully available (track surface in race records)
- `track_going` — firm/good/soft/heavy for turf; standard/heavy for dirt — interacts with late-race speed capacity — data availability: available in JRA race records
- **Do NOT engineer** features based on airborne duration as a proxy for "flying" or superior speed — this paper shows Deep Impact's airborne duration was not longer than average; the intuition is incorrect.
- `distance_category` — 1,200 m (sprint) / 1,400–1,800 m (middle) / 2,000–2,400 m (classic) / 2,400+ m (staying) — stride frequency approaches its ceiling (~2.5 strides/s) at all distances; speed at finishing distances is primarily stride-length limited — motivates separate models by distance category

## Key references / follow-up leads

- Pratt, G.W. Jr., and O'Connor, J.T. Jr. 1978. "A relationship between gait and breakdown in the horse." _Am. J. Vet. Res._ 39: 249–253. [Secretariat kinematic analysis]
- Leach, D.H., and Sprigings, E. 1979. "Gait fatigue in the racing Thoroughbred." _J. Equine Med. Surg._ 3: 436–443. [fatigue → overlap time increase]
- Leach, D.H. et al. 1987. "Multivariate statistical analysis of stride-timing measurements of nonfatigued racing Thoroughbreds." _Am. J. Vet. Res._ 48: 880–888.
- Ishii, K. et al. 1989. "Kinematic analysis of horse gait." _Bull. Equine Res. Inst._ 26: 1–9. [stride frequency ceiling]
- Hiraga, A. et al. 1994. "Relationships between stride length, stride frequency, step length and velocity at the start dash in a racehorse." _J. Equine Sci._ 5: 127–130.
- Kai, M. et al. 1997. "Comparison of stride characteristics in a cantering horse on a flat and inclined treadmill." _Equine Vet. J. Suppl._ 23: 76–79.
