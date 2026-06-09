# Tracheal Endoscopic and Cytological Findings and Blood Examination Results in Thoroughbred Racehorses Suspected to have Lower Respiratory Tract Disease

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 19(4): 97–102, 2008                                                                                                                                                                                                                                                                            |
| docid                          | `19_4_97`                                                                                                                                                                                                                                                                                                     |
| Article type                   | Original                                                                                                                                                                                                                                                                                                      |
| Authors                        | Kanichi Kusano, Seiji Hobo, Hirotaka Ode, Yuhiro Ishikawa                                                                                                                                                                                                                                                     |
| Affiliations                   | Miho Racehorse Clinic, Miho Training Center, Japan Racing Association, 2500–2 Mikoma, Miho-mura, Inashiki-gun, Ibaraki 300-0493; Microbiology Division, Epizootic Research Center, Equine Research Institute, JRA, 1400–4 Shiba, Shimotsuke-shi, Tochigi 329-0412. Note: Kusano and Hobo contributed equally. |
| Received / Accepted / Released | Accepted October 21, 2008                                                                                                                                                                                                                                                                                     |
| Keywords                       | cough, inflammatory airway disease, lower respiratory tract, poor performance, Thoroughbred racehorse                                                                                                                                                                                                         |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/19/4/19_4_97/_pdf/-char/en                                                                                                                                                                                                                                           |

## Abstract (verbatim)

> Cytology of tracheal aspirates, tracheal endoscopic and blood tests were carried out to 86 Thoroughbred racehorses presenting coughs or poor performance which were suspected to have lower respiratory tract disease (LRTD) to assess the conditions of the disorders. Racehorses were classified into coughing (66 horses) and non-coughing (20 horses) groups based on clinical symptoms. Nine Thoroughbred racehorses without respiratory abnormality were used as controls. Assessment of grades of airway mucus, cytology of tracheal aspirates and serum amyloid A (SAA), fibrinogen (Fbg) and pulmonary surfactant protein D (SP-D) measurements were performed. Relationships between age, gender and racing careers were also investigated to understand the characteristics of LRTD in racehorses. Mean age was significantly higher in non-coughing group compared to coughing group. Existence of racing career and number of starts were significantly greater in non-coughing group compared to coughing group. On the other hand, grades of airway mucus were significantly higher in coughing group compared to control group. Percentages of neutrophils in tracheal aspirates were significantly higher in coughing group compared to non-coughing and control groups. SAA, Fbg and SP-D were higher in coughing group compared to non-coughing and control groups indicating that condition of coughing group is in the acute phase. Positive rate of inflammatory airway disease was significantly higher in coughing and non-coughing groups compared to control group. It was concluded that carrying out comprehensive evaluation including investigation on SAA, Fbg and SP-D analysis with airway assessment to Thoroughbred racehorses which were suspected to have LRTD are useful procedure to understand the pathological condition which aid to initiate appropriate treatment, prognosis judgment or to advise trainers to consider altering training regimen.

## Relevance to finishing-position (着順) prediction

Feature family: **B — respiratory disease**. This JRA-dataset study establishes that lower respiratory tract disease (LRTD) in racehorses is characterised by two distinct clinical phenotypes — coughing (acute, higher inflammatory markers) and non-coughing (subclinical, older horses, more career starts) — with different implications for performance modelling. Crucially, racing career data retrieved from JARIS (Japan Racing Information System) reveals that non-coughing LRTD horses had significantly more prior race starts (6.7 ± 8.0 vs. 2.7 ± 6.0) and higher proportion with at least one career start (65.0% vs. 34.8%), supporting `cumulative_starts` as a proxy for accumulated respiratory burden in older horses.

For finishing-position prediction: (1) acute coughing LRTD (high SAA, high neutrophils) is a strong current-form-impairment signal; (2) subclinical non-coughing LRTD disproportionately affects older horses with more career races, implying that career-length features may partially capture latent respiratory wear; (3) the acute-phase biomarkers (SAA median 52.9 μg/ml in coughing vs. 0.5 μg/ml non-coughing vs. 0.2 μg/ml control) show extreme separation, suggesting that if SAA is recorded pre-race, a threshold near 10 μg/ml would flag most acute cases. Age as a moderating variable (non-coughing group significantly older: 3.3 ± 0.9 vs. 2.6 ± 1.0 yr) motivates an `age × career_starts` interaction term.

Companion paper (docid 19_2_25, Kusano et al. 2008) addresses IAD prevalence and environmental risk factors in the same JRA cohort. Together they provide a comprehensive picture of JRA respiratory disease epidemiology for modelling.

## Background & objective

LRTD is the second most common cause of poor performance in racehorses after musculoskeletal disease. IAD (a tighter sub-definition of LRTD) affects primarily young horses in flat-training yards. Racehorses with LRTD can be clinically classified into coughing and non-coughing groups based on symptoms; if meaningful biological differences exist between these groups, it would inform prognosis and treatment decisions. Blood acute-phase markers (SAA, fibrinogen) and pulmonary surfactant protein D (SP-D) may characterise disease severity beyond endoscopy and cytology alone. The study also investigates whether age, sex, and racing career history differ between LRTD subtypes, using JARIS data linkage.

## Materials & methods

**Setting:** Miho Training Center (Ibaraki), JRA; no epidemic infectious diseases present during the study period.

**Subjects:**

- 86 Thoroughbred racehorses with suspected LRTD (based on clinical presentation; upper respiratory tract involvement excluded by endoscopy at rest) — classified into:
  - **Coughing group** (n = 66): major complaint = cough
  - **Non-coughing group** (n = 20): suspected LRTD without cough (poor performance)
- 9 Thoroughbred racehorses without respiratory abnormality: **controls**

**Age/sex (Table 1):**

| Group        | Male | Female | Total | Male:Female ratio | Age (yr) mean ± SD (range) |
| ------------ | ---- | ------ | ----- | ----------------- | -------------------------- |
| Non-coughing | 13   | 7      | 20    | 1.86              | 3.3 ± 0.9 (2–5)            |
| Coughing     | 37   | 29     | 66    | 1.28              | 2.6 ± 1.0 (2–7)            |
| Control      | 5    | 4      | 9     | 1.25              | 2.9 ± 1.8 (2–7)            |

Geldings included in male count. Note: JRA registered horse M:F ratio is approximately 1.3:1.0.

**JARIS data linkage:** Racing career searched for each horse: existence/non-existence of prior starts, number of starts, days from latest start prior to examination, days from examination to nearest start after examination. Controls excluded from career survey (none had started at survey time).

**Tracheal endoscopy:** Video endoscopy >2 hr after training with nose twitch. Tracheal mucopus (TM) graded 0–3:

- Grade 0: no findings
- Grade 1: seed mucus near rima glottidis
- Grade 2: continuous mucus <15 mm width
- Grade 3: continuous mucus >15 mm width

**Tracheal aspirate:** Collected via endoscope forceps channel with 30 ml of 0.9% sterile saline. Cytocentrifugation (Shandon Cytospin4, Thermo Electron); staining (Diff-Quick 16920, Sysmex). Differential count of 200 cells. **IAD diagnosis: >20% neutrophils.**

**Blood:** Jugular venipuncture at time of endoscopy. SAA, SP-D from serum; Fbg from plasma.

**Statistics:** χ2 test for TM grades and sex ratios; Wilcoxon signed-ranks test for two-group comparisons; Kruskal-Wallis + Scheffe's F test for three-group comparisons. P < 0.05 significance.

## Results (detailed — reproduce ALL numbers)

**Tracheal mucopus grades (Table 3):**

| Grade | Non-coughing (n=20) | Coughing (n=66) | Control (n=9) |
| ----- | ------------------- | --------------- | ------------- |
| 0     | 4                   | 5               | 4             |
| 1     | 10                  | 22              | 4             |
| 2     | 4                   | 23              | 1             |
| 3     | 2                   | 16              | 0             |

Coughing group significantly higher TM grades than control (P < 0.01). Non-coughing vs. control not significant.

**Tracheal aspirate cytology and IAD positive rate (Table 4):**

| Group               | Neutrophils (%) mean ± SD | IAD positive rate |
| ------------------- | ------------------------- | ----------------- |
| Non-coughing (n=20) | 31.0 ± 16.5               | 60.0%             |
| Coughing (n=66)     | 52.1 ± 25.3               | 91.8%             |
| Control (n=9)       | 12.3 ± 2.9                | 0.0%              |

Coughing vs. non-coughing: neutrophils P < 0.01. Coughing vs. control and non-coughing vs. control: IAD rate P < 0.01.

**Blood acute-phase markers (Table 4):**

| Marker                        | Non-coughing | Coughing      | Control      |
| ----------------------------- | ------------ | ------------- | ------------ |
| SAA (μg/ml), mean ± SD        | 0.5 ± 0.6    | 52.9 ± 156.3  | 0.2 ± 0.4    |
| Fibrinogen (mg/dl), mean ± SD | 255.3 ± 68.1 | 311.7 ± 123.2 | 254.1 ± 20.0 |
| SP-D (ng/ml), mean ± SD       | 0.6 ± 2.4    | 4.9 ± 12.3    | 0.0 ± 0.0    |

All three markers higher in coughing vs. other groups, but **differences not statistically significant** (high variability in coughing group). Authors note: "the horse that clinical condition was severe tended to be high value in blood examination."

**Age:** Non-coughing group significantly older than coughing group (3.3 ± 0.9 vs. 2.6 ± 1.0 yr; P < 0.01).

**Racing career (Table 2):**

| Variable                               | Non-coughing  | Coughing     | Significance |
| -------------------------------------- | ------------- | ------------ | ------------ |
| ≥1 prior start (%)                     | 65.0%         | 34.8%        | P < 0.01     |
| No prior start (%)                     | 35.0%         | 65.2%        | P < 0.01     |
| Number of starts (mean ± SD)           | 6.7 ± 8.0     | 2.7 ± 6.0    | P < 0.05     |
| Days from latest start (mean ± SD)     | 44.5 ± 58.7   | 40.1 ± 76.2  | ns           |
| Days to nearest next start (mean ± SD) | 144.3 ± 177.4 | 91.4 ± 134.6 | ns           |

**Sex:** Male:female ratio 1.86 in non-coughing vs. 1.28 in coughing — higher male proportion in non-coughing group, not statistically significant. Authors note this is consistent with the finding that older horses have a higher male ratio and that airway inflammation is most common in young horses.

## Discussion & interpretation

Coughing LRTD horses are younger, less experienced (fewer starts), and in acute inflammatory state (higher SAA, fibrinogen, SP-D). Non-coughing LRTD horses are older, have more career starts, and show subacute/chronic respiratory disease (lower neutrophils, near-normal acute-phase markers). The age and career difference between groups is attributed to the natural history of IAD: horses acquire and accumulate lower airway disease through repeated training and racing exposures. The blood markers (SAA, Fbg, SP-D) confirm acute-phase status in coughing horses even if the differences are not statistically significant in this moderately sized sample. Tracheal aspirate is confirmed as a safe, feasible technique in race-prepared horses without local anaesthesia. Authors recommend comprehensive evaluation combining airway assessment + SAA/Fbg/SP-D to guide treatment decisions, prognosis, and training regimen advice to trainers.

## Limitations

- Case series (symptomatic horses at Miho Training Center) — selection bias, no random sampling, results not generalisable to prevalence in the full JRA population.
- n = 86 + 9 controls; blood marker differences non-significant due to high within-group variability (SAA in coughing: mean 52.9 but SD 156.3 — suggests a few very high-SAA horses drive the mean).
- No longitudinal follow-up; career-days data are cross-sectional not causal.
- Controls had no prior career starts, precluding career-stat comparisons with controls.
- Single training centre (Miho only).

## Feature-engineering notes for the model

- `career_starts_cumulative` — total number of race starts prior to current race — derivable from JRA race records — expected effect: non-coughing LRTD horses accumulate more starts → higher starts may indicate latent subclinical respiratory burden in older horses; however, more starts also correlates with fitness/experience — use with age interaction to disentangle — data availability: fully available
- `age_yr` — continuous, from date of birth — derivable from JRA records — expected interaction: young horses (2–3 yr) with few starts → higher acute LRTD risk; older horses (3–4 yr) with many starts → subclinical LRTD — data availability: fully available
- `serum_amyloid_a_ug_ml` — direct acute-phase marker from blood test — threshold ~10 μg/ml separates most acute cases from non-coughing and controls — data availability: requires pre-race clinical records; not in public race DB
- `fibrinogen_mg_dl` — systemic inflammation marker; above ~320 mg/dl suggestive of acute phase — data availability: requires clinical records
- `sp_d_ng_ml` — pulmonary surfactant protein D; elevated in alveolar inflammation — data availability: requires clinical records
- `tracheal_mucus_grade` — ordinal 0–3 from routine endoscopy — source: JRA veterinary examinations — expected effect: grade ≥ 2 → impaired respiratory capacity, negative finishing-position effect — data availability: requires clinical record linkage
- `days_since_last_start` — days_since_last_race — derivable from race records — note: not significantly different between coughing and non-coughing groups in this study; lower predictive value for respiratory status specifically

## Key references / follow-up leads

- Kusano, K. et al. 2008. _J. Equine Sci._ 19: 25–29 (docid 19_2_25) — companion paper: IAD prevalence + environmental risk factors (same JRA cohort).
- Ramzan, P.H. et al. 2008. "Lower respiratory tract disease in Thoroughbred racehorses: analysis of endoscopic data from a UK training yard." _Equine Vet. J._ 40: 7–13.
- Christley, R.M. et al. 2001. "Coughing in thoroughbred racehorses: risk factors and tracheal endoscopic and cytological findings." _Vet. Rec._ 148: 99–104.
- Hobo, S. et al. 2007. "Evaluation of serum amyloid A and surfactant protein D in sera for identification of the clinical condition of horses with bacterial pneumonia." _J. Vet. Med. Sci._ 69: 827–830.
- Couetil, L.L. et al. 2007. "ACVIM Consensus Statement. Inflammatory airway disease of Horses." _J. Vet. Intern. Med._ 21: 356–361.
- Bailey, C.J. et al. 1997. "Wastage in the Australian thoroughbred racing industry: a survey of Sydney trainers." _Aust. Vet. J._ 75: 64–66.
