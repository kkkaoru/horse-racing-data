# Circadian Variations in Plasma Adrenaline and Noradrenaline in the Thoroughbred Horse

## Metadata

| Field                          | Value                                                                                                                          |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 8(3): 81–88, 1997                                                                                               |
| docid                          | `8_3_81`                                                                                                                       |
| Article type                   | Original                                                                                                                       |
| Authors                        | Masahiko Kurosawa, Fujie Takeda, Shunichi Nagata, Kyosuke Mima                                                                 |
| Affiliations                   | Research Department, Laboratory of Racing Chemistry, 4-37-6 Kamiyoga, Setagaya-ku, Tokyo 158, Japan (Japan Racing Association) |
| Received / Accepted / Released | Submitted May 25, 1997; accepted August 22, 1997                                                                               |
| Keywords                       | adrenaline, circadian variations, circulating levels, noradrenaline, Thoroughbred                                              |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/8/3/8_3_81/_pdf/-char/en                                                              |

## Abstract (verbatim)

> Circadian variations in catecholamines (CA) in the Thoroughbred horse were investigated by determining plasma adrenaline (Ad) and noradrenaline (NA) concentrations every 2 hr by high-performance liquid chromatography with an electrochemical detector (HPLC-ED). The HPLC-ED method in the present study was sufficient for determining plasma CA concentrations. The elution of CA from alumina with a mixture of acetic acid and methanol (1:50, v/v) gave a good recovery. The detection limit for both Ad and NA was 10 pg/ml. A significant variation during the 24 hr was observed for plasma NA concentrations (P<0.01), but not for plasma Ad concentrations. From 14:00 hr to 18:00 hr, plasma NA concentrations were significantly higher than those observed from 02:00 hr to 04:00 hr (P<0.01). As a result of the cosinor analysis for individuals, a circadian rhythm of plasma concentrations of Ad and NA was demonstrated (mean ± SE, 26.3 ± 4.6 pg/ml and 66.3 ± 3.8 pg/ml; rhythm amplitude, 9.4 ± 1.4 pg/ml and 12.7 ± 1.2 pg/ml; time of trough, 02:34 hr:min ± 9.7 min and 02:54 hr:min ± 33 min for Ad and NA, respectively). The 6 hr continuous tie-stall-type restraint decreased plasma concentrations of Ad and NA and delayed their peaks by 4 hr and 2 hr, respectively. There was a significant correlation (n=48, r=0.562, P<0.001) between the circadian variations in plasma Ad and NA. In conclusion, it was found that circadian variations in plasma CA in the Thoroughbred horse were similar to those in humans. In addition, the present study suggests that the sustained stress may influence circadian variations in plasma CA.

## Relevance to finishing-position (着順) prediction

This paper belongs to **Feature family E (environment/stress)**. It establishes the first quantitative 24-hour baseline profiles of plasma adrenaline (Ad) and noradrenaline (NA) in the Thoroughbred at rest, using catheter-based sampling (avoiding venepuncture artefacts) and HPLC-ED assay. The JRA's Laboratory of Racing Chemistry conducted this study specifically to understand equine sympatho-adrenal rhythms — directly relevant to interpreting racehorse responses.

For finishing-position prediction, the relevance operates at three levels:

1. **Race post-time effect:** NA is significantly highest 14:00–18:00 (82.0 pg/ml peak at 16:00) and lowest at 02:00–04:00 (51.3 pg/ml). Afternoon/evening races coincide with naturally elevated sympatho-adrenal tone in horses. The `race_start_hour` is already a standard feature candidate; this paper provides the physiological prior: afternoon races occur when horses are in a naturally more aroused sympatho-adrenal state, which may interact with performance (especially in shorter, more explosive races where sympatho-adrenal priming matters).

2. **Pre-race confinement/travel stress:** The tie-stall restraint experiment shows that 6 hr of sustained restraint suppresses NA and delays its peak by 2 hr (two-way ANOVA p < 0.001). Pre-race transportation and stall confinement can similarly suppress and shift catecholamine rhythms, potentially affecting race-day arousal. If a horse normally peaks at 16:00 but was restrained/transported from 09:00–15:00, NA may be suppressed for the race itself.

3. **Training timing:** Early-morning training at 06:00–08:00 corresponds to naturally maintained elevated CA levels (plasma NA 69.9 pg/ml at 08:00) — supporting the practice of JRA morning training as physiologically appropriate. If training time is recorded, it may interact with CA rhythm as a covariate.

Plasma catecholamines are not routinely reported in JRA/NAR race databases; however, race post-time (`race_start_hour`) and pre-race transport information are available and serve as indirect proxies for these physiological states.

## Background & objective

Catecholamines (Ad and NA) are the primary sympatho-adrenal hormones. Ad is secreted by the adrenal medulla; NA is the major neurotransmitter of sympathetic terminals. In humans, circadian rhythms in plasma CA are well established: lower at night, higher during the day. Time-of-day effects on stress-induced physiological reactivity have been reported in humans (Hickey et al. 1993). However, at the time of this paper, no study had characterised 24-hour circadian patterns in plasma CA in horses. The few published equine plasma CA reports used venepuncture (which artefactually elevates CA by ~30–50% due to excitement). This study was designed to: (1) validate an HPLC-ED method for equine plasma CA; (2) characterise 24-hour circadian rhythms by catheter sampling; (3) test the effect of tie-stall restraint on CA rhythms.

## Materials & methods

**Subjects:**

- Circadian experiment: 5 Thoroughbreds (3 stallions, 2 mares, age 3–5 years, body weight 435–490 kg)
- Tie-stall restraint experiment: 4 of the same 5 horses
- All clinically normal; no medications for 1 month prior; housed in individual stables with straw bedding

**Experimental design:**

- Fed mixed grain rations twice daily: 07:00 and 15:30 hr
- No exercise during experiment
- Circadian experiment: horses moved freely in stables; grass hay and water ad libitum
- 16-gauge angiocatheter inserted in right jugular vein on day of experiment; fixed with adhesive tape; heparinised (1,000 U/ml); 90-min acclimatisation before sampling
- Blood (15 ml) sampled every 2 hr from 08:00 for 24 hr (circadian) or 14 hr (restraint)
- Tie-stall restraint: horses cross-tied 09:00–15:00 (6 hr); could reach hay and water but could not lie down
- Blood chilled in EDTA-2K tubes; plasma separated at 3,500 × g, 4°C, 10 min; stored at −40°C until analysis

**HPLC-ED method (modified from Ganhao et al. 1991):**

- System: Shimadzu LC-10A + integrator C-R7A + Shiseido Nanospace SI-1 2005 ED
- Column: CAPCELL PAK C18 UG-120 (5 µm, 150 × 4.6 mm i.d.) + guard column
- Mobile phase: 0.1 M phosphate buffer pH 2.5 (2 mM octanesulfonic acid + EDTA-2Na 10 mg/ml) + acetonitrile (98.5:1.5); flow rate 1.0 ml/min; column 40°C; applied potential +650 mV vs Ag/AgCl
- Extraction: Sepakol minicolumn + activated alumina; eluted with methanol:acetic acid (98:2, v/v, pH 2.8) — key modification from Ganhao et al. (who used 0.1 M perchloric acid, which co-eluted more endogenous substances); internal standard: 0.5 ng methyldopamine
- Recoveries (Table 1): Ad 68.8–75.8%; NA 62.4–70.3%; dopamine 67.3–68.1% (all mean ± SE, n=5)
- CV for precision (n=5 at 100 pg/ml): Ad 1.6%; NA 1.9%; dopamine 1.4%
- Linear range: 10–2000 pg/ml; r for regression: Ad 0.9999; NA 0.9996; dopamine 0.9987
- Detection limit: 10 pg/ml for both Ad and NA

**Statistics:** One-way ANOVA for repeated measures (circadian variation). Two-way ANOVA (restraint vs. free conditions). Tukey post hoc. Significance p < 0.05. Cosinor analysis for individual circadian rhythms (Nelson et al. 1979), tested against null of amplitude = 0. Pearson correlation between Ad and NA over 24 hr.

## Results (detailed — reproduce ALL numbers)

**Circadian variation in plasma Ad (Fig. 2):**

- Horse No. 1 excluded from Ad analysis (nocturnal values below 10 pg/ml detection limit)
- One-way ANOVA: variation with time NOT significant (p > 0.05)
- Pattern: higher afternoon, lower night
- Mean at 08:00: 30.8 ± 4.9 pg/ml
- Gradually reduced to 12:00
- Peak at 14:00: 36.2 ± 8.3 pg/ml
- Nadir at 04:00: 18.7 ± 3.8 pg/ml (note: paper prints "ng/ml" here but context makes clear this is pg/ml — probable typographic error in original)
- 24-hr mean (cosinor): 26.3 ± 4.6 pg/ml (mean ± SE)
- Cosinor amplitude: 9.4 ± 1.4 pg/ml
- Time of trough: 02:34 hr:min ± 9.7 min
- All 4 horses had significant individual circadian rhythms (p < 0.005 to p < 0.05, Table 2)

**Cosinor analysis — individual Ad (Table 2):**

| Horse | Mean (pg/ml) | Amplitude (pg/ml) | Trough time | Significance |
| ----- | ------------ | ----------------- | ----------- | ------------ |
| No. 2 | 41.6         | 13.1              | 02:10       | p < 0.005    |
| No. 3 | 21.1         | 8.2               | 02:46       | p < 0.05     |
| No. 4 | 21.2         | 6.6               | 02:27       | p < 0.005    |
| No. 5 | 22.5         | 9.8               | 02:53       | p < 0.005    |

**Circadian variation in plasma NA (Fig. 2):**

- One-way ANOVA: significant variation with time (p < 0.0001)
- Mean at 08:00: 69.9 ± 4.8 pg/ml
- Peak at 16:00: 82.0 ± 4.6 pg/ml
- Nadir at 02:00: 51.3 ± 3.3 pg/ml
- Significantly higher 14:00–18:00 vs. 02:00–04:00 (p < 0.01)
- 24-hr mean (cosinor): 66.3 ± 3.8 pg/ml
- Cosinor amplitude: 12.7 ± 1.2 pg/ml
- Time of trough: 02:54 hr:min ± 33 min
- All 5 horses had significant individual circadian rhythms

**Cosinor analysis — individual NA (Table 2):**

| Horse | Mean (pg/ml) | Amplitude (pg/ml) | Trough time | Significance |
| ----- | ------------ | ----------------- | ----------- | ------------ |
| No. 1 | 64.9         | 11.2              | 01:36       | p < 0.005    |
| No. 2 | 74.2         | 13.9              | 04:19       | p < 0.01     |
| No. 3 | 68.1         | 10.5              | 04:09       | p < 0.05     |
| No. 4 | 71.7         | 17.0              | 02:08       | p < 0.005    |
| No. 5 | 52.4         | 10.9              | 02:20       | p < 0.005    |

**Ad–NA correlation:**

- Pearson r = 0.562 (n = 48, p < 0.001) — significant positive correlation throughout 24-hr period (Fig. 4)
- NA concentrations ~2.5× those of Ad throughout the day (consistent with human pattern)

**Tie-stall restraint effects (Fig. 3):**

- Ad: one-way ANOVA during 14-hr restraint period significant (p < 0.01); highest value 39.7 ± 2.7 pg/ml at 18:00 vs. lowest 20.0 ± 4.0 pg/ml at 12:00 (p < 0.01); two-way ANOVA (restraint vs. free) NOT significant; peak delayed by 4 hr
- NA: one-way ANOVA during restraint period NOT significant; highest 100.8 ± 23.5 pg/ml at 18:00, lowest 45.6 ± 6.7 pg/ml at 12:00; two-way ANOVA (restraint vs. free) SIGNIFICANT (p < 0.001); NA at 16:00 under restraint significantly lower than free condition (p < 0.05); peak delayed by 2 hr
- Overall: sustained tie-stall restraint reduced and delayed both Ad and NA rhythms

**Recovery data (Table 1 — HPLC validation):**

| Catecholamine | Recovery at 0.1 ng/ml | Recovery at 1.0 ng/ml |
| ------------- | --------------------- | --------------------- |
| Adrenaline    | 75.8 ± 2.3%           | 68.8 ± 0.7%           |
| Noradrenaline | 62.4 ± 1.5%           | 70.3 ± 1.5%           |
| Dopamine      | 68.1 ± 1.0%           | 67.3 ± 0.9%           |

## Discussion & interpretation

**Catheter vs. venepuncture:** Resting plasma Ad values in this study (26.3 pg/ml mean) are notably lower than prior equine reports (Beadle et al. 1982; Fujii et al. 1987), which likely used venepuncture. The excitement from venepuncture activates the sympatho-adrenal axis; catheter sampling gives true resting baseline. The current values are similar to Lauderdale et al. (1975).

**NA as the more reliable circadian marker:** NA showed significant ANOVA-level circadian variation (p < 0.0001); Ad did not at the ANOVA level (4 horses, N too small after excluding Horse 1), but cosinor analysis confirmed individual rhythms in all 4 horses. The difference is mechanistic: in humans, plasma NA reflects posture and locomotor activity (exogenous rhythm), while plasma Ad has a more intrinsic (endogenous) rhythm. Horses sleep standing; rare recumbency meant posture was not a confound, so NA variation reflects activity level and feeding response.

**Post-feeding peak:** NA peaked near 15:30–16:00 — shortly after the afternoon feeding. The authors attribute this to increased locomotor activity after feeding (horses moving around stables) rather than a direct food-intake effect. This is consistent with activity-dependent sympathetic NA release.

**Early-morning training window:** CA concentrations at 06:00–08:00 (JRA training hours) are at approximately 70–80% of their daily peak. The authors note this suggests early morning training is "reasonable and should be effective" since horses are in a moderately activated sympatho-adrenal state.

**Tie-stall suppression mechanism:** Horses initially became drowsy for 2–3 hr in tie-stall, then somewhat restless beyond 3 hr. The suppression of NA suggests locomotor restriction reduces sympathetic output. The delayed peak indicates that when released, normal locomotion resumes the circadian pattern but shifted. For race-day logistics, prolonged pre-race stall confinement could suppress sympatho-adrenal readiness.

**Comparison with human pattern:** Both Ad and NA follow the same day-high/night-low pattern as in humans. Resting CA concentrations are lower in horses than humans, but the exercise response is greater in horses (Snow et al. 1992).

## Limitations

- Very small n (5 horses for circadian; 4 for restraint); limited statistical power
- Only 3 stallions and 2 mares; no age range tested (3–5 years)
- No exercise during experiment — resting state only; exercise-induced CA responses not measured here
- Horse No. 1 excluded from Ad analysis (below detection limit nocturnally); reduces effective n to 4 for Ad
- 2-hr sampling interval may miss short-duration peaks (e.g., peri-feeding spikes)
- Only 6 hr of restraint tested; longer confinement effects unknown
- Not directly applicable to race-day conditions (race excitement is a much larger CA stimulus than any circadian variation)

## Feature-engineering notes for the model

- `race_start_hour` — hour of race post time (0–23); NA peaks 14:00–18:00 (82.0 pg/ml) and troughs at 02:00–04:00 (51.3 pg/ml); afternoon races align with naturally higher sympatho-adrenal state; **directly available** in JRA/NAR race records; expected effect: curvilinear — afternoon race start may slightly favour horses with high-NA phenotype (arousal-dependent performance), though the effect is physiologically modest (amplitude only ~12.7 pg/ml out of 66.3 pg/ml mean); already in most feature sets
- `pretransport_confinement_hr` — hours of tie-stall/truck confinement from stable departure to race start; tie-stall suppresses NA by ~2 hr delay; longer confinement → greater pre-race sympatho-adrenal suppression; **available from logistics records** at JRA but not in public race data
- `transport_distance_km` — proxy for confinement duration; longer transports imply longer periods of NA suppression; **available from training-centre-to-racecourse records**
- `race_start_hour × distance_m` interaction — sympatho-adrenal activation matters most for short-distance (sprint) events where explosive, adrenaline-dependent power is critical; less relevant for long-distance aerobic events
- `feeding_time_gap_hr` — hours between last feeding and race post-time; NA peaks post-feeding (15:30 feeding → NA peak ~16:00); if a horse is fed at 15:30 and races at 15:30, optimal; if raced at 08:00 without morning feeding, pre-feeding nadir state
- **Practical limitation:** Plasma CA concentrations are not measured routinely and cannot be included as direct features. All CA-derived features are physiological priors that justify including `race_start_hour` and transport-related features as proxies, with these physiological mechanisms as the rationale.
- **Do NOT use** resting plasma NA or Ad values from venepuncture samples as training features if they exist — venepuncture-induced excitement artefact makes them non-comparable to catheter-based values

## Key references / follow-up leads

- Snow, D.H., Harris, R.C., McDonald, I.A., Forster, C.D. and Marlin, D.J. 1992. Effects of high-intensity exercise on plasma catecholamines in the Thoroughbred horse. _Equine Vet. J._ 24: 462–467. **[CA during exercise — quantifies the CA surge during actual racing conditions]**
- Dela, F., Mikines, K.J., Linstow, M.V. and Galbo, H. 1992. Heart rate and plasma catecholamines during 24 h of everyday life in trained and untrained men. _J. Appl. Physiol._ 73: 2389–2395.
- Hickey, M.S., Costill, D.L., Vukovich, M.D., Kryzmenski, K. and Widrick, J.J. 1993. Time of day effects on sympathoadrenal and pressor reactivity to exercise in healthy man. _Eur. J. Appl. Physiol._ 67: 159–163. **[Time-of-day effect on sympathoadrenal responses to exercise — supports race_start_hour feature]**
- Linsell, C.R., Lightman, S.L., Mullen, P.E., Brown, M.J. and Causon, R.C. 1985. Circadian rhythms of epinephrine and norepinephrine in man. _J. Clin. Endocrinol. Metab._ 60: 1210–1215. **[Human baseline for comparison]**
- Galbo, H. 1986. Autonomic neuroendocrine responses to exercise. _Scand. J. Sports Sci._ 8: 3–17.
- Beadle, R.E., Norwood, G.L. and Brencick, V.A. 1982. Summertime plasma catecholamine concentrations in healthy and anhidrotic horses in Louisiana. _Am. J. Vet. Res._ 43: 1446–1448. **[Prior equine CA baseline — likely venepuncture-inflated]**
