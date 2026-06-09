# Characteristic of Inflammatory Airway Disease in Japanese Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                              |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Journal                        | J. Equine Sci. 19(2): 25–29, 2008                                                                                                                                                                                                                                  |
| docid                          | `19_2_25`                                                                                                                                                                                                                                                          |
| Article type                   | Original                                                                                                                                                                                                                                                           |
| Authors                        | Kanichi Kusano, Yuhiro Ishikawa, Kazuhiro Seki, Ryo Kusunose                                                                                                                                                                                                       |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 321–4 Tokami-Cho, Utsunomiya City, Tochigi 320-0856; Ritto Training Center, JRA, 1028 Misono, Ritto City, Shiga 520-3085; Miho Training Center, JRA, 2500–2 Mikoma, Miho-mura, Inashiki-gun, Ibaraki 300-0493 |
| Received / Accepted / Released | Accepted February 7, 2008                                                                                                                                                                                                                                          |
| Keywords                       | etiology, inflammatory airway disease, poor performance, thoroughbred racehorse                                                                                                                                                                                    |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/19/2/19_2_25/_pdf/-char/en                                                                                                                                                                                                |

## Abstract (verbatim)

> Inflammatory airway disease (IAD) is a common cause of poor performance, interruption of training and premature retirement in racehorses. It is also reported that up to 80% of horses are affected at some point in the first years of training in UK and Australia. However, no studies with regard to the information on occurrence of IAD in Japanese Thoroughbred racehorses have been reported. To investigate the occurrence and the characteristics of IAD, epidemic research including endoscopic examination of the airway tract and trachea wash was conducted for Thoroughbred racehorses presenting coughs or poor performance which airway tract disease was suspected stalled in training facility managed by Japan Racing Association. Fifty-six out of 76 Thoroughbred racehorses (73.7%) presenting coughing or poor performance were diagnosed as IAD. Mean incidence rate of IAD was 0.3% and it has been confirmed that constant number of IAD exists in Japan. Up to 35.7% of IAD horses showed upper airway abnormalities in some extent. There was a trend for IAD horses to use wood shavings for bedding and fed hay from the ground compared with the control group. Therefore, improvement of stabling environment may aid in preventing IAD. This study demonstrated that Japanese Thoroughbred racehorses are affected by IAD likewise other countries as well as demonstrated the characteristics of IAD which may contribute to the clarification of the pathogenesis of IAD.

## Relevance to finishing-position (着順) prediction

Feature family: **B — respiratory disease**. Inflammatory airway disease (IAD) is explicitly documented here as a cause of poor performance, training interruption, and premature retirement in JRA Thoroughbreds. This study establishes Japanese-specific prevalence (73.7% IAD rate among symptomatic horses at JRA training centres) and risk factors (wood-shaving bedding, ground-level hay feeding), enabling construction of respiratory-burden features for performance-degradation and scratch-risk models.

For the finishing-position pipeline, IAD functions as a performance-degrading condition with three modelling pathways. First, a `recent_respiratory_event` binary flag (derived from veterinary visit records or training interruption) acts as a direct form-impairment signal. Second, stable-management features (bedding type, hay position) can proxy IAD risk probability when individual veterinary records are absent. Third, the mean age of 2.39 ± 0.5 yr in the symptomatic group identifies young horses in early training as the primary risk cohort, supporting an `age × respiratory_risk` interaction term for maiden/early-career horses.

The 0.3%/month population incidence at JRA training centres is low at baseline but highly concentrated among underperformers — consistent with treating LRTD/IAD history as a strong negative performance prior rather than a broad population covariate. The companion paper (docid 19_4_97, Kusano et al. 2008) provides additional granularity including blood biomarkers and career-start data from the same JRA population.

## Background & objective

IAD is recognised in Thoroughbred (Burrell et al. 1996), Standardbred, sport horse, and National Hunt populations internationally, with up to 80% of horses in UK/Australian training yards affected at some point in early training years. Aetiology is multifactorial: viral infection, bacterial infection, environmental dust/endotoxin load, and dysregulated inflammation are all implicated. No Japanese-specific data existed prior to this study. The aim was to establish occurrence rate, characterise IAD horses by upper airway endoscopy and tracheal cytology, and identify environmental risk factors (bedding, hay feeding) in JRA training facilities.

## Materials & methods

**Setting:** JRA Racehorse Hospitals at Ritto Training Center (Shiga) and Miho Training Center (Ibaraki), July–December 2001.

**Subjects:** 76 Thoroughbred racehorses (mean age 2.39 ± 0.5 yr) presenting to hospital with coughing or poor performance as the chief complaint, suspected of airway tract disease. For environmental questionnaire, 232 healthy stabled horses in JRA training facilities served as controls.

**Endoscopy:** Performed in-stall with nose twitch; tracheal wash collected transendoscopically by instilling 30 ml of sterile saline into the distal trachea and immediately aspirating. Samples preserved in equal volume of Shandon Cytospin Collection Fluid.

**Cytology:** Cytocentrifugation (Shandon Cytospin3); staining with Hemacolor (MERCK). Total nucleated cell count by haemocytometer; differential count of 300 cells per slide for neutrophils, lymphocytes, eosinophils, macrophages, and epithelial cells. **IAD diagnosis criterion: >20% neutrophils** in tracheal aspirate.

**Upper airway scoring:** Scored at endoscopy as none (1), mild (2), or moderate/marked (3) for: epiglottis abnormality (EA), pharyngeal lymphoid hyperplasia (PLH), laryngeal hemiplegia (LH), and dorsal displacement of soft palate (DDSP).

**Incidence calculation:** Monthly incidence rate = number of IAD-diagnosed horses ÷ mean total number of registered horses in training centre that month.

**Questionnaire:** Completed by trainer or stable staff at time of endoscopy: coughing during rest/training, poor performance, bedding material (rice straw vs. wood shavings), hay feeding position (ground vs. basket at head level). Fisher's exact test for IAD vs. control comparisons; Pearson's χ2 for incidence seasonality. Significance P < 0.05.

## Results (detailed — reproduce ALL numbers)

**IAD diagnosis:**

- 56/76 (73.7%) diagnosed as IAD (>20% neutrophils and increased tracheal mucus)

**Tracheal aspirate differential (mean %):**

| Cell type   | IAD group (n=56) | Non-IAD group (n=20) |
| ----------- | ---------------- | -------------------- |
| Neutrophils | 55.7 ± 26.3%     | 5.5 ± 4.5%           |
| Macrophages | 33.0 ± 20.7%     | 85.1 ± 7.4%          |
| Lymphocytes | 11.1 ± 8.1%      | 9.4 ± 5.6%           |
| Eosinophils | 0.1 ± 0.4%       | 0%                   |

**Upper airway endoscopic scores (mean ± SD):**

| Finding                                | IAD group (n=56) | Non-IAD group (n=20) | Significance |
| -------------------------------------- | ---------------- | -------------------- | ------------ |
| Epiglottis abnormality (EA)            | 1.2 ± 0.5        | 1.1 ± 0.3            | ns           |
| Pharyngeal lymphoid hyperplasia (PLH)  | 1.5 ± 0.7        | 1.4 ± 0.7            | ns           |
| Laryngeal hemiplegia (LH)              | 1.0 ± 0.3        | 1.0 ± 0              | ns           |
| Dorsal displacement soft palate (DDSP) | 1.1 ± 0.3        | 1.3 ± 0.4            | ns           |

No significant difference between IAD and Non-IAD groups on any upper airway measure. Up to **35.7% of IAD horses** showed upper airway abnormalities to some extent.

**Monthly incidence (July–December 2001):**

| Month     | IAD cases | Registered horses | Incidence rate |
| --------- | --------- | ----------------- | -------------- |
| July      | 19        | 1,992             | 1.0%           |
| August    | 4         | 1,800             | 0.2%           |
| September | 10        | 2,712             | 0.4%           |
| October   | 9         | 3,401             | 0.3%           |
| November  | 6         | 3,820             | 0.2%           |
| December  | 8         | 4,089             | 0.2%           |
| **Total** | **56**    | **17,814**        | **0.3%**       |

No significant seasonal variation (Pearson's χ2).

**Environmental risk factors:**

| Factor                           | IAD (n=56)    | Controls (n=232) | P-value (Fisher's) |
| -------------------------------- | ------------- | ---------------- | ------------------ |
| Hay fed from ground              | 62.5% (35/56) | 45.3% (105/232)  | P < 0.05           |
| Hay fed from basket (head level) | 37.5% (21/56) | 54.7% (127/232)  | —                  |
| Wood shavings bedding            | 42.9% (24/56) | 18.5% (43/232)   | P < 0.05           |
| Rice straw bedding               | 57.1% (32/56) | 81.5% (189/232)  | —                  |

Both wood-shaving bedding and ground-level hay feeding were significantly more prevalent in IAD horses than in controls.

**Interpretation of unexpected direction:** Conventionally, rice straw and elevated head position were considered higher-risk (more endotoxins; head elevation impairs postural drainage). Authors note that respirable endotoxins and organic dust content may differ between straw and rice straw (rice straw possibly lower); and that in JRA's well-managed facilities, bedding/hay effects may be attenuated. The study results contradict the expected direction, suggesting local environmental context matters.

## Discussion & interpretation

The 73.7% IAD rate among symptomatic horses confirms that IAD is the dominant respiratory diagnosis in young JRA Thoroughbreds presenting with cough/poor performance, consistent with international studies. The mean incidence of 0.3%/month is lower than Burrell et al. 1996 (12% monthly prevalence, 10 cases/100 horses/month) because clinically apparent cases only were studied, and JRA's well-maintained facilities may suppress incidence. The absence of seasonality contrasts with some international reports. Upper airway endoscopy did not differentiate IAD from Non-IAD, supporting the view that IAD is a lower (not upper) respiratory tract condition. The environmental risk factors (wood shavings, ground feeding) were significant, though the direction contradicts classic dust-endotoxin theory, possibly reflecting differences in Japanese bedding materials or well-controlled stable hygiene.

## Limitations

- Case series of symptomatic horses only; no random sampling → cannot derive population prevalence; incidence figures rely on clinical presentation, not screening.
- Control group for environmental survey (n=232) was healthy horses, not age-matched; potential confounding by management practices at Ritto vs. Miho.
- No virological/bacteriological workup; aetiology not determined.
- Single 6-month period (July–December 2001); no multi-year replication.
- No follow-up on racing outcomes of IAD horses after treatment.

## Feature-engineering notes for the model

- `iad_flag` — binary: IAD diagnosed in this training stint (from veterinary records) — derivable from JRA clinical records if accessible — expected effect: strong negative predictor of finishing position; strong positive predictor of race-absence/scratch — data availability: unlikely in public JRA race data; would need clinical record linkage
- `recent_respiratory_visit_days` — days since last veterinary visit for respiratory complaint — derivable from JRA health records — expected effect: negative with recency (recent visit → current respiratory burden) — data availability: requires clinical DB access
- `bedding_type` — categorical: rice straw / wood shavings — source: stable management records — expected effect: wood shavings → higher IAD risk → indirect performance decrement — data availability: not in standard race databases
- `hay_feed_position` — binary: ground vs. basket — source: stable management records — expected effect: ground feeding → higher IAD risk — data availability: not in standard race databases
- `horse_age_yr` — continuous; mean symptomatic age 2.39 ± 0.5 yr — derivable from date of birth in race records — expected interaction: young horses (2–3 yr) × respiratory-event → stronger negative effect — data availability: fully available
- **Do NOT use** upper airway endoscopy scores as IAD proxies — no significant difference between IAD and Non-IAD in this study.

## Key references / follow-up leads

- Burrell, M.H. et al. 1996. "Respiratory disease in Thoroughbred horses in training: the relationship between disease and viruses, bacteria and environment." _Vet. Rec._ 139: 308–313.
- Newton, J.R. et al. 2003. "A case control study of factors and infections associated with clinically apparent respiratory disease in UK Thoroughbred racehorses." _Prev. Vet. Med._ 60: 107–132.
- Bailey, C.J. et al. 1999. "Impact of injuries and disease on a cohort of two-and three-year-old Thoroughbreds in training." _Vet. Rec._ 145: 487–493.
- Robinson, N.E. 2003. "Inflammatory airway disease: defining the syndrome. Conclusions of the Havemeyer Workshop." _Equine Vet. Educ._ 15: 61–63.
- McGorum, B.C. et al. 1998. "Total and respirable airborne dust endotoxin concentrations in three equine management systems." _Equine Vet. J._ 30: 430–434.
- Kusano, K. et al. 2008. _J. Equine Sci._ 19: 97–102 (docid 19_4_97) — companion paper with blood biomarkers + JARIS career data.
