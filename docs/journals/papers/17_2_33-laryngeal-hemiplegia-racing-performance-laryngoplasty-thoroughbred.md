# Effect of Laryngeal Hemiplegia of Thoroughbred Racehorses on Racing Performance and the Usefulness of Laryngoplasty Performed on Horses with Laryngeal Hemiplegia

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                   |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 17(2): 33–37, 2006                                                                                                                                                                                                                                       |
| docid                          | `17_2_33`                                                                                                                                                                                                                                                               |
| Article type                   | Original                                                                                                                                                                                                                                                                |
| Authors                        | Atsushi KIKUTA, Seiji HOBO, Yoshimasa TAKIZAWA, Yoshikazu KAWASAKI                                                                                                                                                                                                      |
| Affiliations                   | Racehorse clinic, Ritto Training Center, Japan Racing Association, 1028 Misono, Ritto-shi, Shiga 520-3085; Microbiology Division, Epizootic Research Center, Equine Research Institute, Japan Racing Association, 1400–4 Shiba, Shimotsuke-shi, Tochigi 329-0412, Japan |
| Received / Accepted / Released | Accepted March 13, 2006                                                                                                                                                                                                                                                 |
| Keywords                       | laryngeal hemiplegia, laryngoplasty, racing performance, roaring, thoroughbred racehorse                                                                                                                                                                                |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/17/2/17_2_33/_pdf/-char/en                                                                                                                                                                                                     |

## Abstract (verbatim)

> We evaluated the effect of laryngeal hemiplegia (LH) on racing performance in thoroughbred racehorses as well as the effects of laryngoplasty on racing performance. A total of 294 racehorses (LH group) were diagnosed with LH were analyzed for this study. A total of 294 racehorses (no-LH group) not suffering from LH extracted at random from the Japan Racing Association population were used for comparison of sex and body weight. The epidemiological investigation evaluated relationships between the occurrence of LH and sex, body weight, the number of the horses that were late for the time limit, the rate of the horse that returned to racing after diagnosis, the number of races run, and race results. Evaluations of the effect of laryngoplasty on racing performance were performed on the rate of the horse that returned to racing after laryngoplasty or no-laryngoplasty, the number of races run, and race results. The ration of males or geldings to female in the LH group was higher than it of the no-LH group. Body weight of the LH group was significantly heavy as compared with it of the no-LH group. It became clear that there is a tendency to suffer the male with heavy weight from LH compared with female. In Grade III, the racing performance was significantly lower than Grade I and Grade II. The racing performance of the laryngoplasty group was higher than the no-laryngoplasty group. It became clear that the horses that have severe LH were poor performance, and laryngoplasty was the useful surgical technique for LH treatment.

## Relevance to finishing-position (着順) prediction

Feature family: **B (respiratory — upper airway obstruction)**. Laryngeal hemiplegia (LH, "roaring") is an upper-airway obstruction condition affecting 2–8% of JRA Thoroughbreds, caused by recurrent laryngeal nerve damage producing arytenoid cartilage adduction during exercise. This paper provides the largest JRA-population dataset on LH, documenting a dose-dependent, statistically significant performance degradation by endoscopic grade (I < II < III). Grade III horses have: 37.2% late-for-time-limit rate (vs. 7.5% Grade I), only 46.5% returning to racing at all, averaging only 1.57 races run after diagnosis, and earning only 0.42 points/race (vs. 1.08 Grade I).

Key engineered features: (1) `lh_grade` — ordinal 0/1/2/3 from endoscopic records; (2) `laryngoplasty_history` — binary; Grade III horses with surgery show dramatically better outcomes (75% return vs. 40%, 4.06 races vs. 1.00, 0.75 pts vs. 0.09 pts). Even without endoscopy data, male sex and heavier body weight implicitly carry part of the LH risk signal (LH group 3.1:1 male:female vs. 1.3:1 in controls; LH body weight 479.9 ± 31.2 kg vs. 460.6 ± 26.6 kg, p < 0.01).

Interactions: `sex` (males 2-3× more likely to develop LH — strongest interaction), `body_weight` (heavier horses more at risk), `distance` (LH impairs performance more severely at longer distances because sustained high airflow is required), `surface` (dirt late-for-time limit definitions differ from turf: ≥5 s vs. ≥4 s).

## Background & objective

Roaring (abnormal inspiratory noise) is caused by upper-airway obstruction, most commonly LH, and is associated with poor racing performance. LH involves collapse of the left arytenoid cartilage during inspiration when the recurrent laryngeal nerve is damaged. Severity is graded by endoscopy at rest using Rakestraw et al. criteria (0–IV). Prior reports established LH prevalence at 2–8% in JRA Thoroughbreds and documented airflow mechanics improvements after laryngoplasty on treadmill exercise tests. However, epidemiological validation of actual racing performance effects — by grade — had not been done at JRA scale. The laryngoplasty technique (Marks et al. 1970: prosthetic suture of the paralysed cricoarytenoideus dorsalis muscle to fix the arytenoid in abduction) had physiological support but no large-scale racing-outcome data. The objective was to: (1) quantify racing performance degradation by LH grade in JRA horses; (2) determine whether laryngoplasty improves actual racing outcomes in Grade III horses.

## Materials & methods

**LH group:** 294 Thoroughbred racehorses diagnosed with LH at Ritto Training Center, JRA. Endoscopic examination performed at rest using flexible videoendoscope (Hobo et al. 1995). LH graded by Rakestraw et al. (1991) criteria:

- Grade 0: synchronous full abduction/adduction of both arytenoids
- Grade I: asynchronous movement (hesitation, flutter, adductor weakness) of left arytenoid; full abduction inducible by nasal occlusion or swallowing
- Grade II: asynchronous movement of left arytenoid; full abduction NOT inducible or maintainable by nasal occlusion or swallowing
- Grade III: marked laryngeal asymmetry; no substantial movement of left arytenoid
- Grade IV: other asynchrony/asymmetry not fitting Grades I–III

**Age and sex distribution of LH group (Table 1):**

| Age (yr)  | Males/Geldings | Females                                                                      |
| --------- | -------------- | ---------------------------------------------------------------------------- |
| 2         | 6              | 5                                                                            |
| 3         | 9              | 6                                                                            |
| 4         | 4              | 4                                                                            |
| 4         | 8              | 1 (note: two "4" rows in original table, representing 44 and 59 ages listed) |
| 5         | 9              | 4                                                                            |
| 6+        | 4              | 0                                                                            |
| **Total** | **222**        | **72**                                                                       |

**No-LH control group:** 294 Thoroughbred racehorses randomly extracted from the JRA population with no LH. Used for sex ratio and body weight comparison only.

**Performance metrics:**

- "Late for time limit": horse finishing ≥5 s behind winner on dirt, or ≥4 s on turf — recorded pre-diagnosis
- "Return to racing rate": proportion returning to racing after LH diagnosis
- "Races run": number of post-diagnosis race starts
- "Race result points": custom scoring — 1st=5, 2nd=4, 3rd=3, 4th=2, 5th=1, ≥6th=0; average per horse

**Laryngoplasty subgroup (Grade III only):** 16 horses underwent surgery (Marks et al. method, general anaesthesia by isoflurane inhalation); 70 Grade III horses did not.

**Statistical analysis:** Kruskal-Wallis test and Mann-Whitney test. Significance at p < 0.05. Data expressed as mean ± SD.

## Results (detailed — reproduce ALL numbers)

**Sex distribution:**

| Group         | Male/Gelding : Female ratio |
| ------------- | --------------------------- |
| LH group      | 3.1 : 1.0 (222M/G : 72F)    |
| No-LH control | 1.3 : 1.0                   |

**Body weight:**

| Group         | Body weight (kg) |
| ------------- | ---------------- |
| LH group      | 479.9 ± 31.2     |
| No-LH control | 460.6 ± 26.6     |
| Significance  | p < 0.01         |

**Performance by LH grade (Table 2):**

| Metric                | Grade I (n=106) | Grade II (n=102)  | Grade III (n=86)         |
| --------------------- | --------------- | ----------------- | ------------------------ |
| Late-for-time rate    | 7.5% (8 horses) | 16.7% (17 horses) | **37.2% (32 horses)** ᵃᵇ |
| Return-to-racing rate | 73.6%           | 68.6%             | **46.5%** ᵃᵇ             |
| Races run (mean ± SD) | 5.65 ± 6.40     | 4.93 ± 4.01       | **1.57 ± 2.12** ᵃᵇ       |
| Race result points    | 1.08 ± 1.13     | 0.74 ± 0.63       | **0.42 ± 0.15** ᵃᵇ       |

ᵃ p < 0.01 vs. Grade I; ᵇ p < 0.01 vs. Grade II

All four metrics show significant degradation at Grade III vs. Grades I and II. No significant difference reported between Grades I and II (though numerically Grade II is worse). The authors note that "some horses showed remarkable performance decline even in Grade II" — suggesting that Grade II endoscopy at rest is insufficient to predict performance; treadmill endoscopy may be needed for Grade II cases.

**Effect of laryngoplasty in Grade III (Table 3):**

| Metric                | Laryngoplasty (n=16) | No Laryngoplasty (n=70) | Significance |
| --------------------- | -------------------- | ----------------------- | ------------ |
| Return-to-racing rate | **75.0%**            | 40.0%                   | p < 0.01     |
| Races run (mean ± SD) | **4.06 ± 3.07**      | 1.00 ± 1.32             | p < 0.01     |
| Race result points    | **0.75 ± 0.65**      | 0.09 ± 0.32             | p < 0.01     |

Laryngoplasty restores Grade III performance to approximately Grade I levels (return rate 75% vs. Grade I 73.6%; races run 4.06 vs. Grade I 5.65; points 0.75 vs. Grade I 1.08).

## Discussion & interpretation

The male predominance in LH (3.1:1 vs. 1.3:1) and the heavier body weight of LH horses (479.9 vs. 460.6 kg, p < 0.01) support a hereditary component and a size effect on the recurrent laryngeal nerve (longer nerve path in larger horses subject to greater mechanical stress). The authors note LH tends to develop by damage to the recurrent laryngeal nerve, with a possible hereditary factor (Poncet et al. 1989).

Grade I LH shows no clinically severe impact (7.5% late rate, 73.6% return), consistent with the airway still being able to fully abduct under nasal occlusion. Grade III, where the arytenoid is essentially fixed in the partially adducted position, causes dramatic airway obstruction at exercise, resulting in 37.2% late rates and only 46.5% returning to racing at all.

Laryngoplasty works by prosthetically fixing the paralysed arytenoid in abduction, improving intratracheal pressure and ventilation volume during exercise (Derksen et al. 1986; Hawkins et al. 1997). This study is the first epidemiological confirmation that laryngoplasty improves actual JRA race outcomes at population scale: the 75% return-to-racing rate vs. 40% and the 8-fold improvement in race points (0.75 vs. 0.09) are clinically decisive.

## Limitations

- Retrospective design; no randomisation of surgical decision in Grade III.
- Laryngoplasty group n = 16 is small; selection bias likely (surgery preferentially offered to horses with better prognosis or owner consent).
- No dynamic (treadmill) endoscopy data; Grade II performance prediction from rest endoscopy is unreliable.
- No control for confounders (trainer quality, class level, post-surgery complications).
- Race result point metric is a custom 5-point scale; not a standard finishing-position measure.
- No follow-up beyond races run/return; long-term durability of laryngoplasty effects not assessed.
- Population restricted to Ritto Training Center (JRA); may differ from Miho or NAR populations.

## Feature-engineering notes for the model

- `lh_grade` — ordinal integer 0–3 from endoscopy at rest; source: JRA veterinary records — expected effect: monotone negative (higher grade → more performance penalty, especially at Grade III); effect is multiplicative on scratch probability and points per race — availability: JRA veterinary records only; not in standard race databases
- `laryngoplasty_history` — binary; 1 if horse has undergone laryngoplasty; source: surgical records — expected effect: strong positive for Grade III horses (restores ~Grade I performance); interaction with `lh_grade` — availability: JRA surgical records
- `lh_late_rate_proxy` — derived: proportion of horse's past races where it finished ≥5 s (dirt) or ≥4 s (turf) behind winner; serves as a revealed LH severity proxy without endoscopy — source: past race records — expected effect: negative — availability: fully available from race DB
- `sex_male` × `body_weight_heavy` — interaction proxy for LH risk; males are 2.4× over-represented in LH; heavy horses (≥480 kg) more at risk — source: sex and 馬体重 from race entry — availability: standard; use as a latent LH-risk score
- `return_to_racing_after_gap` — binary; if horse has had a long lay-off following diagnosis of upper-airway issue and has now returned, flag for potential under-performance in first races back — source: race entry gap analysis
- `distance` interaction: LH performance penalty is expected to increase with distance (longer sustained exercise = more ventilation demand); encode `lh_grade × distance_m` interaction term if LH grade is available

## Key references / follow-up leads

- Rakestraw, P.C., Hackett, R.P., Ducharme, N.G., Nielan, G.J., Erb, H.N. 1991. Arytenoid cartilage movement in resting and exercising horses. _Vet. Surg._ 20: 122–127. [Grading criteria used in this paper]
- Hobo, S., Matsuda, Y., Yoshida, K. 1995. Prevalence of upper respiratory tract disorders detected with a flexible videoendoscope in thoroughbred racehorses. _J. Vet. Med. Sci._ 57: 409–413. [Prevalence data for JRA TBs]
- Derksen, F.J., Stick, J.A., Scott, E.A., Robinson, N.E., Slocombe, R.F. 1986. Effects of laryngeal hemiplegia and laryngoplasty on airway flow mechanics in exercising horses. _Am. J. Vet. Res._ 47: 16–20. [Physiological basis of laryngoplasty benefit]
- Hawkins, J.F. et al. 1997. Laryngoplasty with or without ventriculectomy for treatment of left laryngeal hemiplegia in 230 racehorses. _Vet. Surg._ 26: 484–491.
- Morris, E.A., Seeherman, H.J. 1990. Evaluation of upper respiratory tract function during strenuous exercise in racehorses. _J. Am. Vet. Med. Assoc._ 196: 431–438. [Exercise endoscopy methodology]
- Poncet, P.A. et al. 1989. A preliminary report on the possible genetic basis of laryngeal hemiplegia. _Equine Vet. J._ 21: 137–138. [Hereditary component]
- Marks, D. et al. 1970. Use of a prosthetic device for surgical correction of laryngeal hemiplegia in horses. _J. Am. Vet. Med. Assoc._ 157: 157–163. [Original laryngoplasty technique]
