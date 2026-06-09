# Frequency Spectral Evaluation about Hemoglobin Kinetics of Pedal Circulation Attend to Laminar Layer with Near Infrared Spectroscopy in Sound Horse

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                             |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 18(4): 137–144, 2007                                                                                                                                                                                                                               |
| docid                          | `18_4_137`                                                                                                                                                                                                                                                        |
| Article type                   | Original                                                                                                                                                                                                                                                          |
| Authors                        | Osamu SASAKI, Atsutoshi KUWANO, Naoki SASAKI, Toyohiko YOSHIHARA, Shigeo HARA                                                                                                                                                                                     |
| Affiliations                   | Department of Veterinary Surgery, Faculty of Agriculture, Iwate University, Ueda, Morioka-shi, Iwate 020-8550; Clinical and Pathobiology Division, Equine Research Institute, Japan Racing Association, 321-4 Tokami-cho, Utsunomiya-shi, Tochigi 320-0856, Japan |
| Received / Accepted / Released | Accepted July 27, 2007                                                                                                                                                                                                                                            |
| Keywords                       | frequency spectral analysis, hemodynamics, horse, laminar layer, near infrared spectroscopy                                                                                                                                                                       |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/18/4/18_4_137/_pdf/-char/en                                                                                                                                                                                              |

## Abstract (verbatim)

> This study observed the fluctuation of total hemoglobin (tHb) and reduced hemoglobin (dHb) concentrations as a direct reflection of hemodynamics under the hoof wall with near infrared spectroscopy (NIRS). NIRS observation of the laminar layer under the pigmented hoof walls was performed without sedation or invasion on five clinically normal horses. NIRS indexes relating to the concentration change of tHb (tHbindex) and dHb (dHbindex) in tissue were recorded at 2 Hz and evaluated over 0.004 Hz in spectral power. Each NIRS index fluctuated spontaneously and continuously from 0.01 Hz to 0.05 Hz. Over 10 times difference of total power was shown in fluctuations of each NIRS index. Nevertheless, a significant positive correlation was observed between NIRS indexes. In the geometrical distribution of the power spectra from 0.0078 Hz to 0.5 Hz, the sub-band power of the dHbindex dominated the tHbindex in sub-bands with central frequencies of 0.011 Hz and 0.022 Hz. This was interpreted as characteristic of the hemoglobin oxidation-reduction (redox) fluctuation common to forelegs of both sides. NIRS observation based on the second time scale and adapted by spectral analysis was able to evaluate qualitatively the characteristics of spontaneous kinetics of hemoglobin redox regardless of a lack of quantity, suggesting the ability to provide virginal vital signs under hooves.

## Relevance to finishing-position (着順) prediction

Feature family: **A (injury/soundness)**. Laminitis is one of the most severe and economically costly racehorse diseases, causing sudden performance collapse and often permanent withdrawal from racing. It arises from disruption of the laminar-layer microcirculation under the hoof wall. This paper establishes baseline NIRS spectral fingerprints for healthy pedal hemodynamics — spontaneous oscillations of tHb and dHb in the 0.01–0.05 Hz band — providing the reference against which pathological deviations can be detected. The dominance of dHb over tHb power at 0.011 and 0.022 Hz (the low-frequency peripheral band, LFp) is attributed to sympathetic regulation of peripheral flow, and any disruption of this pattern would flag incipient laminar ischemia before clinical lameness appears.

The direct link to race finishing position is indirect at this stage: the paper studies only five normal horses and measures no performance outcomes. However, the practical significance is that if NIRS monitoring were deployed at JRA/NAR training centres, the spectral ratio dHb/tHb in LFp could serve as an early-warning feature for pre-clinical laminitis, which causes unexplained sudden deterioration in finishing position and eventual scratch. The paper is a foundational methodology study; the engineered feature it motivates is a binary `laminitis_risk_nirs_flag` derived from deviations from the normal 0.01–0.05 Hz spectral envelope.

Interactions with existing pipeline features: this signal would primarily interact with `days_since_last_race` (horses with prolonged absences may have had subclinical laminitis), `body_weight_change` (laminitic horses lose condition), and `going` / surface type (hard or frozen ground increases digital pressure and laminitis risk).

## Background & objective

The laminar layer — the interlocking epidermal and dermal laminae coupling the third phalanx to the hoof wall — transmits the horse's entire bodyweight. Laminitis destroys this structure, causing palmar rotation of the third phalanx and severe pain. The developmental (pre-clinical) phase of laminitis is characterised by markedly decreased capillary circulation in the laminar layer, despite an apparent paradox of increased total foot blood flow (via arteriovenous shunting). Existing clinical evaluation techniques (angiography, scintigraphy) depend on plasma tracers rather than directly imaging red-cell hemoglobin; laser Doppler flowmetry requires drilling the hoof wall. NIRS offers non-invasive, continuous, direct monitoring of hemoglobin redox. The objective was to characterise the spectral properties of spontaneous hemoglobin oscillations under the hoof wall in clinically normal horses, as a prerequisite for using NIRS to detect early pathological change.

## Materials & methods

**Subjects:** Five adult Thoroughbred females, 4 years old, 463–538 kg; acclimated to 25°C for 1 h; confirmed normal by infrared thermography (TVS610, Nippon-Avionics) for hoof wall surface temperature symmetry.

**Equipment:** Two independent NIRS oxygen monitors (HEO200, OMRON, Japan), one per foreleg, applied simultaneously. HEO200 provides relative-change NIRS indexes for tHb+myoglobin (tHbindex) and reduced Hb+myoglobin (dHbindex) using a two-wavelength method. Probe separation 35 mm horizontal, fixed with self-union tape (No.525K, Nitto) and Elasticon (Johnson & Johnson) on the median plane of each hoof wall. All hooves were pigmented. Horse stood unsedated in a stall.

**Recording:** tHbindex and dHbindex sampled at Fs = 2 Hz for 256 s each (512 points per channel). Artifacts common to both forelegs were excluded.

**Frequency analysis:** Hamming window + FFT (DFT Analyser, GPL software). Peak frequency defined as maximum power spectral component. Total power (totalPowertHbindex, totalPowerdHbindex) = sum of all spectral components (expressed as common log values). Six geometrical sub-bands constructed at 1/2ⁿ Hz intervals from 0.0078 Hz to 0.5 Hz. Minimum resolution component = 1/256 = 0.0039 Hz. Components >0.5 Hz excluded due to Nyquist limit at 1 Hz; components at heart-rate frequency (~1 Hz) are outside the analysis window.

**Hoof anatomy reference (Table 2, n=21–34 light-breed Japanese horses):**

| Parameter                    | Mean ± SD         |
| ---------------------------- | ----------------- |
| Body weight (kg)             | 489 ± 28 (n=26)   |
| Hoof wall thickness (mm)     | 9.4 ± 1.4 (n=34)  |
| Laminar layer thickness (mm) | 5.8 ± 1.0 (n=21)  |
| Hoof to laminar layer (mm)   | 15.4 ± 1.8 (n=21) |

Probe depth estimated as ½ × 35 mm = 17.5 mm, consistent with reaching the laminar layer.

**Statistical methods:** Two-factor factorial ANOVA (foreleg side × NIRS index) for peak frequency and sub-band power. Fisher's z-transformation for correlation of total powers. Two-way repeated measures ANOVA (foreleg side × NIRS index, sub-bands as repeated factor) for spectral distribution. Post-hoc Scheffe's F-test where ANOVA significant. α = 0.05.

## Results (detailed — reproduce ALL numbers)

**Peak frequency (Table 1):**

| Side  | dHbindex peak freq. (Hz)          | tHbindex peak freq. (Hz)          |
| ----- | --------------------------------- | --------------------------------- |
| Left  | 0.012–0.055 [mean 0.023 ± 0.0021] | 0.016–0.027 [mean 0.021 ± 0.0023] |
| Right | 0.016–0.027 [mean 0.027 ± 0.0075] | 0.016–0.027 [mean 0.023 ± 0.0021] |

Total power range: tHbindex 3.2×10⁻¹² – 3.0×10⁻¹¹ %² (right), 1.2×10⁻¹¹ – 5.1×10⁻¹¹ %² (left); dHbindex 1.0×10⁻¹¹ – 1.1×10⁻¹⁰ %² (right), 2.1×10⁻¹¹ – 1.4×10⁻¹⁰ %² (left).

- No significant differences in peak frequency between left/right forelegs or between NIRS indexes (two-factor ANOVA).
- Total power varied >10-fold between individuals within each NIRS index and foreleg.
- Significant positive correlation between totalPowertHbindex and totalPowerdHbindex: **R² = 0.903, p < 0.0001** (n=10 observations across 5 horses × 2 forelegs).

**Sub-band power distribution:**

- Two-way repeated measures ANOVA: significant interaction between NIRS indexes and foreleg for spectral distribution. Significant main effect of foreleg on overall power spread.
- Left foreleg spectral power significantly more dominant than right foreleg in the high-frequency peripheral band (HFp: 0.063–0.25 Hz), specifically at 0.088 Hz and 0.117 (0.177) Hz sub-bands.
- dHbindex sub-band power significantly dominated tHbindex at 0.011 Hz and 0.022 Hz (the low-frequency peripheral band, LFp: 0.0078–0.031 Hz).
- No interaction between foreleg side and NIRS indexes in LFp, confirming this dHb/tHb power asymmetry is a bilateral systemic characteristic of peripheral hemoglobin redox, not a laterality artifact.
- No significant differences between NIRS indexes at frequencies >0.25 Hz.

**Interpretation of LFp dominance of dHb:** The 0.01–0.07 Hz band is the sympathetic nervous system low-frequency band in horses (Kuwahara et al. 1996). The dominance of dHb (reduced Hb) power over tHb power in this band reflects sympathetic regulation of peripheral vasoconstriction/dilation cycles. The absence of spectral components around 0.15 Hz (respiratory band) in pedal circulation is consistent with central circulation findings.

## Discussion & interpretation

The peak frequency of pedal hemoglobin oscillations (0.01–0.05 Hz) matches the 0.03 Hz autonomic nervous system low-frequency component in central circulation (Kuwahara et al. 1996), suggesting continuity between central and peripheral haemodynamic regulation in the horse foot. The lack of precise phase coherence between forelegs, and frequency modulation variability, indicates both whole-body (blood pressure, sympathetic tone) and local/segmental factors modulate pedal flow.

The over-10-fold dispersion in total power is attributed to melanin pigmentation variation in hoof walls interfering with the absolute NIRS optical pathlength (Pringle et al. 1999). However, the R² = 0.903 correlation between tHb and dHb total powers confirms that the relative spectral ratio (dHb/tHb redox) is robust even when absolute power varies. This ratio principle — not absolute power — is what makes NIRS useful for clinical comparison in pigmented hooves.

The authors propose that the frequency spectral characteristics of hemoglobin redox under the hoof wall can serve as a general index for non-invasive pedal hemodynamic monitoring. Future work must link abnormal LFp spectral patterns to actual laminitis episodes (acute vs. developmental phase) to validate clinical utility.

## Limitations

- n = 5 horses, all female, all 4-year-old Thoroughbreds; no age, breed, or sex variation.
- Normal animals only — no laminitic or at-risk horses; no performance outcomes measured.
- Pigmented hooves limit absolute quantification of hemoglobin concentration (pigmentation interferes with NIRS optical pathlength).
- 35 mm probe spacing estimated to reach laminar layer based on anatomical average; individual hoof wall thickness not measured per animal.
- No aliasing filter in HEO200 hardware; spectral components above Fs/2 = 1 Hz could in principle alias into the analysis window.
- Single 256-s recording per horse; no test-retest reliability data.
- No validated clinical application to laminitis detection in this paper; the link to performance loss is inferential.

## Feature-engineering notes for the model

- `laminitis_risk_flag` — binary; derived from training logs showing sudden lameness or hoof treatment; source: JRA/NAR veterinary treatment records — expected effect: strong positive predictor of DNF / poor finishing position — data availability: JRA training centre medical records (not in standard race databases)
- `days_since_hoof_treatment` — integer; days between most recent hoof pathology treatment and race date; source: veterinary logs — expected effect: monotone negative (longer recovery → lower risk) — availability: veterinary records only
- `going_hardness` — categorical or numeric; hard/firm going increases digital pressure and laminitis risk — source: official course going reports — availability: present in JRA/NAR race records; interaction with `laminitis_risk_flag`
- `body_weight_delta_recent` — change in 馬体重 over last 30 days; rapid weight loss may indicate systemic disease including laminitis prodrome — source: 馬体重 records at race entry — availability: standard
- **Do NOT use** raw NIRS spectral values directly — they are only available from research monitoring, not from standard race/training databases.

## Key references / follow-up leads

- Hinckley, K.A., Fearn, S., Howard, B.R., Henderson, I.W. 1995. Near infrared spectroscopy of haemodynamics and oxygenation in normal and laminitic horses. _Equine Vet. J._ 27: 465–470. [Direct comparison of normal vs. laminitic NIRS profiles]
- Adair, H.S. III, Goble, D.O., Shires, G., Sanders, W.L. 1994. Evaluation of laser doppler flowmetry for measuring coronary band and laminar microcirculatory blood flow in clinically normal horses. _Am. J. Vet. Res._ 55: 445–449.
- Kuwahara, M. et al. 1996. Assessment of autonomic nervous function by power spectral analysis of heart rate variability in the horse. _J. Auton. Nerv. Syst._ 60: 43–48. [Central autonomic spectral bands in horses]
- Pringle, J., Roberts, C., Kohl, M., Lekeux, P. 1999. Near infrared spectroscopy in large animals: optical pathlength and influence of hair covering and epidermal pigmentation. _Vet. J._ 158: 48–52.
- Hood, D.M. et al. 1993. The role of vascular mechanisms in the development of acute equine laminitis. _J. Vet. Int. Med._ 7: 228–234.
