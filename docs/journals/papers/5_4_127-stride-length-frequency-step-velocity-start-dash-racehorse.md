# Relationships between Stride Length, Stride Frequency, Step Length and Velocity at the Start Dash in a Racehorse

## Metadata

| Field                          | Value                                                                                                      |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 5(4): 127–130, 1994                                                                         |
| docid                          | `5_4_127`                                                                                                  |
| Article type                   | Note (Research Note)                                                                                       |
| Authors                        | Atsushi Hiraga, Akira Yamanobe, Katsuyoshi Kubo                                                            |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 27–7 Tsurumaki 5-chome, Setagaya-ku, Tokyo 154, Japan |
| Received / Accepted / Released | Submitted October 20, 1994; accepted March 23, 1995                                                        |
| Keywords                       | horse, step length, stride length, stride frequency                                                        |
| PDF                            | https://www.jstage.jst.go.jp/article/jes1994/5/4/5_4_127/_pdf/-char/en                                     |

## Abstract (verbatim)

> The present study was undertaken in effort to describe the relationships among each step length, stride length, stride frequency, and velocity during the start dash. Stride frequency reached its peak immediately after the start, while stride length required about 25–30 strides to reach its maximal level. Although substantial increase in velocity was attained by an increase in stride length, it appears that acceleration during the start dash is achieved by maximal increase in stride frequency. Mid step length reached a maximum level following several strides, however airborne step length required more time to reach its maximum. Therefore acceleration during the start dash is achieved by a maximal increase in mid step length since it is not possible to quickly increase airborne step length.

## Relevance to finishing-position (着順) prediction

Feature family: **F (conformation/body-size/gait)**.

This paper provides the biomechanical profile of the acceleration phase from the starting gate — the phase most determinative of early race positioning, which is among the strongest determinants of finishing position especially at sprint distances (≤1,200 m). The central finding is a mechanical decoupling between the two components of speed: **stride frequency** reaches its maximum (≈2.5/s) essentially at the first stride and is locked in by the 8th stride, while **stride length** takes 25–30 strides to reach its maximum (7.2 m) and accounts for the bulk of velocity increase from mid-start to peak velocity.

The practical implication for gate-break prediction: a horse's initial advantage comes entirely from stride frequency, not stride length. By the 8th stride the horse has stabilised its gait at transverse gallop (after transitioning through half-bounds and rotary gallop), meaning horses that complete the half-bounds → rotary → transverse gait transition quickly will achieve effective acceleration sooner. Peak velocity is not attained until 25–30 strides after the start, corresponding to roughly 175–210 m of distance. For races of 1,000–1,200 m, this acceleration phase represents ~15–20% of total race distance — a disproportionately large portion of the race.

For the finishing-position model, features derived from race records (e.g., gate-break time, early position at first corner, 200 m split time) serve as empirical proxies for the individual horse's acceleration capacity characterised by this paper. The individual variation in stride length (Fig. 4: clear between-horse differences in stride length at strides 1–30 across 3 horses) confirms that stride length at race pace is an individual-level ability feature. Since stride length is correlated with body size and conformation (conformation proxy via body weight `馬体重` and height), existing features partially encode stride-length variation.

## Background & objective

Prior work by Dusek et al. (1970) [ref 1] and Yamanobe et al. (1992) [ref 5] showed that both stride length and stride frequency increase with running speed in horses, but that at high velocities the rate of increase in stride length levels off while stride frequency continues to rise. Among the four step-length components (fore, mid, hind, airborne), mid and airborne step lengths increase more rapidly with speed than fore and hind. Kai & Kubo (1993) [ref 2] observed that stride frequency peaks nearly immediately after the gate start while stride length takes longer. However, the specific dynamics of each step-length component during the acceleration phase was unknown.

**Objective:** Describe the within-stride and across-stride evolution of stride frequency, stride length, all four step lengths (fore, mid, hind, airborne), and velocity during the start dash from a gate to peak speed.

## Materials & methods

**Subjects:** Three female Thoroughbreds (No. 1: 5 years old; No. 2: 6 years old; No. 3: 6 years old). All ridden by the same skilled rider. Note: very small n=3, all female.

**Track:** Straight flat dirt track; horses ran approximately 300 m from the gate at full speed.

**Reference race comparison:** Actual racing Thoroughbreds cover the first 200 m in 12–13 s. In this experiment, horse No. 1 completed the first 200 m in 13.8 s — approximately 6% slower than race pace, considered comparable for study purposes.

**Instrumentation:**

- Piezoelectric accelerometers (Model 501 ST, TEAL Corporation, Japan) attached to the cannons of all four limbs
- Acceleration curves recorded continuously (Fig. 1 shows a typical cannon accelerometer tracing)
- Stride duration measured from tracings using a slide caliper (time from one hoof impact to the next for the same limb)
- Stride frequency = reciprocal of stride duration
- **Limitation:** complete acceleration curve traces obtained for horse No. 1 only; therefore stride frequency data available for horse No. 1 only

**Stride and step length measurement:**

- Stride length and four step lengths measured from hoof prints (impression intervals on the dirt surface) per the Yamanobe et al. method [ref 5]
- Velocity for each stride computed as stride length × stride frequency
- Hoof prints measured with a tape and ruler

**Four step-length definitions (Fig. 3 schema):**

- **Fore step length:** distance from hind-limb hoof impact to same-side fore-limb hoof impact
- **Mid step length:** distance from fore-limb hoof impact to contralateral hind-limb hoof impact (the longest of the four)
- **Hind step length:** distance from fore-limb hoof impact to same-side hind-limb hoof impact
- **Airborne step length:** distance from hind-limb hoof impact to contralateral fore-limb hoof impact (corresponds to the suspension phase)

## Results (detailed — reproduce ALL numbers)

**Overall velocity and stride dynamics (Fig. 2 — horse No. 1):**

- Peak running velocity reached at **25–30 strides** after the start
- Peak stride length: **7.2 m**, attained at 25–30 strides, remained constant thereafter
- Stride frequency (horse No. 1): approximately **2.5/s immediately after start**; varied slightly during the first several strides; became constant after the **8th stride** (coinciding with gait stabilisation in transverse gallop)
- All three horses showed qualitatively the same pattern of change in stride length (Fig. 4) and step lengths, despite individual differences in absolute values

**Step-length dynamics (Fig. 3 — horse No. 1):**

| Step component | Value at start                 | Maximum value | Stride to maximum                          | Pattern                                         |
| -------------- | ------------------------------ | ------------- | ------------------------------------------ | ----------------------------------------------- |
| Mid step       | ~2 m (near-max from start)     | ~2 m          | within first few strides                   | Essentially flat; slight variations             |
| Airborne step  | short at start                 | ~2.4 m        | ~20th stride then gradual further increase | Linear increase through stride 20, then gradual |
| Fore step      | short initially                | ~1.2 m        | ~20th stride                               | Slight increase then plateau                    |
| Hind step      | very short (few first strides) | ~1.0 m        | 20–30th stride                             | Slow increase from very low initial value       |

**Gait transitions (Fig. 5 — all three horses):**

- Strides 1–several: **half-bounds** (both hind limbs landing almost simultaneously)
- Horses No. 1 and No. 2: half-bounds → **rotary gallop** → **transverse gallop** (transverse gallop established by approximately the 8th stride)
- Horse No. 3: half-bounds → **transverse gallop** directly (rotary gallop phase absent or very brief); same order of limb landing throughout
- Transverse gallop is the normal racing gait for maximum sustained speed; rotary gallop is characteristic of animals with flexible spines (cats, cheetahs) and is apparently used as an intermediate gait by most horses during acceleration

**Key mechanistic interpretation (from text):**

1. Stride frequency reached its maximum (2.5/s) from the very start — the value at race pace is reached by the first stride
2. Velocity increase from low start velocity to peak is therefore attributable to stride length increasing (not frequency, which is already maximal)
3. But immediate acceleration in the first few strides is due to stride frequency because stride length cannot increase rapidly in the early strides — the body's inertia limits how quickly airborne step length can grow
4. Mid step length is near-maximal from the beginning (propelling force from hind limbs effective immediately); hind step length is kept short initially to maximise propelling force (generating mid step), then normalises as speed builds
5. Airborne step length grows proportional to body velocity (inertia-driven) — explains why it is the last component to reach maximum

**Individual differences (Fig. 4):**
Three horses showed measurably different absolute stride lengths at equivalent strides, confirming stride length is an individual-level characteristic even at full speed (strides 25+).

## Discussion & interpretation

The finding that stride frequency reaches its peak immediately after the start (2.5/s) is consistent with Kai & Kubo (1993) [ref 2]. The fact that 2.5/s is the race-pace stride frequency means the horse is immediately operating at maximum cadence — further velocity gain must come from stride length extension. This is mechanically constrained by the requirement that airborne step length (the suspension component) can only grow as body momentum builds, making it inertia-limited.

The finding that mid step length is effectively at maximum from the start is attributed to the hind limbs generating maximum propelling force (short hind step length = maximum push-off, producing large mid step length per Yamanobe et al. [ref 5]). As velocity builds, the hind step normalises and the airborne component takes over as the dominant variable stride-length contributor.

The three gait phases (half-bounds → rotary → transverse gallop) have mechanical interpretations. Half-bounds allow asymmetric loading during very low speed. Rotary gallop (present in 2/3 horses) represents a transitional gait characteristic of flexible-spine quadrupeds that provides high cadence at the cost of some efficiency. Transverse gallop is the most mechanically efficient gait for sustained high speed and represents the final stable racing gait. Horse No. 3 (the 6-year-old female who skipped rotary gallop) may not have been running at maximum effort, or individual variation in gate technique explains the difference — the authors acknowledge this uncertainty. They note that actual race-start gait transitions likely depend on limb position just before the start, achieved velocity, and jockey action.

The practical conclusion is that **not all horses necessarily run by rotary gallop during a racing gate start** — this is a source of individual variability in gate acceleration that may partly explain "gate performance" differences recorded in practice.

## Limitations

- Very small sample: n=3 horses, all female, ages 5–6 years old
- Complete accelerometer traces for stride frequency obtained for horse No. 1 only; stride frequency conclusions are from a single animal
- Whether horse No. 3 was running at maximum effort is uncertain; the absence of a rotary gallop phase in that horse may reflect non-maximal effort rather than a genuine individual biomechanical difference
- 5-year-old vs. 6-year-old horses may not represent all ages; 2-year-old starters (the prime JRA debut cohort) may have different acceleration profiles
- Track conditions (flat dirt, specific surface firmness not characterised) and horse fitness level not standardised
- No measurement of jockey weight, riding technique quantification, or starting gate type

## Feature-engineering notes for the model

- `gate_rank` or `start_rank` — horse's historical ranking in gate-break performance (early position at first 100–200 m, relative to field) — source: JRA race records (corner position at first timing point) — expected effect: stride frequency is at peak within the first stride; horses that "arrive first" at the first corner have an inherent advantage, especially at ≤1,200 m where the start phase is ~15–20% of race distance; positive correlation with finishing position at sprint distances
- `body_weight_kg` — 馬体重 — source: `race_entry.body_weight` — expected effect: proxy for stride length (taller/heavier horses tend to have longer strides); individual stride length variation confirmed across 3 horses; interaction with distance (long stride more valuable at longer distances)
- `stride_length_proxy` — if workout gait analysis data available: stride length at race pace (strides 25+); otherwise use body height as proxy — source: workout records or body measurement — expected effect: longer stride length = higher peak velocity; stride length variability is individual-level (not distance-dependent in the same way as frequency)
- `race_distance_start_fraction` — proportion of race that falls within the ~175–210 m acceleration zone (25–30 strides × ~7 m/stride) — source: `race.distance` — expected effect: for 1,000 m races the first 210 m = 21% of the race; for 3,000 m it is only 7%; gate-break features should receive higher weight for shorter races
- `surface_at_start` — dirt vs. turf starting surface — source: `race.surface` — expected effect: the study was on dirt; hoof print dynamics and gait transitions may differ on turf; surface interaction with gate-break performance is untested but plausible
- `debut_flag` — first-career race indicator — source: derived from race sequence number — expected effect: young horses may not yet have mastered the transverse gallop transition by debut; gate technique improves with experience; interaction with `age_months`
- **NOTE**: Gait components in this study (stride freq, mid/airborne step lengths) are not directly observable from JRA race records but early corner position (if recorded) and 200 m split times (if available) serve as outcome proxies for the acceleration profile described

## Key references / follow-up leads

- Yamanobe, A., Hiraga, A., and Kubo, K. 1992. Relationships between stride frequency, stride length, step length and velocity with asymmetric gaits in the Thoroughbred horse. _Jpn. J. Equine Sci._ 3(2): 143–148 — direct precursor paper; velocity-dependent stride relationships at steady state (not start dash)
- Kai, M., and Kubo, K. 1993. Respiration pause and gait after the start dash in a horse. _Bull. Equine Res. Inst._ 30: 26–29 — confirms stride frequency peaks immediately after start; also examines respiration and start-dash gait transitions
- Leach, D.H., Sprigings, E.J., and Laverty, W.H. 1987. Multivariate statistical analysis of stride timing measurements of nonfatigued racing Thoroughbreds. _Am. J. Vet. Res._ 48: 880–888 — stride timing at race pace in racing Thoroughbreds; provides steady-state context for start-dash values
- Dusek, J., Ehrlein, H.J., von Engelhardt, W., and Hornicke, H. 1970. Beziehungen zwischen trittlange, trittfrequenz and geschwindigkeit bei Pferden. _Z. Tierzuchtg. Zuchtgsbiol._ 87: 177–188 — foundational stride length × frequency × speed relationship in horses (German)
- Leach, D.H., and Dagg, A.I. 1983. A review of research on equine locomotion and biomechanics. _Equine Vet. J._ 15: 93–102 — review including rotary gallop during acceleration from standing
