# Association between exercise-induced pulmonary hemorrhage and inflammatory airway disease in polo ponies

## Metadata

| Field                          | Value                                                                                                                                                                      |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 28(2): 55–59, 2017                                                                                                                                          |
| docid                          | `28_1701`                                                                                                                                                                  |
| Article type                   | Full Paper                                                                                                                                                                 |
| Authors                        | Katia Moreira da SILVA, Juliana Nabuco Pereira OTAKA, Carlos Alexandre Paula GONÇALVES, Estevão Grossi Aguiar SILVA, Nayro Xavier de ALENCAR, Daniel Augusto Barroso LESSA |
| Affiliations                   | Federal Fluminense University, Rio de Janeiro 24.230-340, Brazil                                                                                                           |
| Received / Accepted / Released | Received January 4, 2017; Accepted April 21, 2017                                                                                                                          |
| Keywords                       | EIPH, IAD, neutrophils, tracheal wash                                                                                                                                      |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/28/2/28_1701/_pdf/-char/en                                                                                                        |

## Abstract (verbatim)

> The respiratory system is essential for health and high athletic performance in horses. Respiratory diseases have been recognized as having a major impact on training equine animals and are commonly cited as the second most common cause of wasted training time. Inflammatory airway disease (IAD) is an important cause of poor performance in young racehorses. Exercise-induced pulmonary hemorrhage (EIPH) is considered a major issue for the equine industry because of its high prevalence and association with reduced athletic performance. In Brazil, polo is a growing equestrian sport, but studies on it are still scarce. The aim of this study was to evaluate the occurrence of EIPH, the association between EIPH and IAD, and EIPH influence on the tracheal cytological profile of polo ponies. Thirty-seven horses regularly used for polo were included in this study. Endoscopic examination was performed every 30 to 90 min after practice, and tracheal lavage was performed after 18 to 24 hr. Sixteen animals (43.2%) presented a score of 0 for mucus in endoscopy; twelve animals (32.4%) presented a score for 1 and nine animals (24.3%) presented score 2 of mucus. IAD was characterized by tracheal cytology in 12 animals (32.4%). The occurrence of EIPH in this study was 29.7% (11/37). No significant difference was found in the cell types in tracheal cytology when EIPH-positive and EIPH-negative horses were compared. Polo ponies are affected by IAD and EIPH in relevant proportions, but there was no association between EIPH and tracheal cytological profile.

## Relevance to finishing-position (着順) prediction

Feature family **B (respiratory health)**. EIPH and IAD are the two most prevalent respiratory diseases in athletic horses and both impair performance. EIPH prevalence in Thoroughbred racehorses ranges from 15% to 90% depending on diagnosis method; IAD is the second most common cause of lost training time. A horse suffering from active EIPH or IAD on race day would be expected to perform below its baseline level, making respiratory disease history a relevant negative performance predictor.

This paper's key finding — that EIPH and IAD **co-occur at meaningful rates (~30% each) but are statistically independent** (no significant difference in tracheal cytology between EIPH+ and EIPH− horses, P > 0.05 for all cell types) — directly informs feature design. EIPH and IAD should be modelled as **separate binary/count features** rather than collapsed into a single "respiratory disease" composite. A horse may have EIPH without IAD, IAD without EIPH, or both; the performance impacts may differ.

The tracheal mucus score (0–4 at endoscopy, present in 56.7% of horses in this study) is a simple clinical marker that correlates with upper airway health and could serve as an intermediate feature if pre-race endoscopy data were available. JRA does conduct endoscopic examinations in Thoroughbreds; if those records are accessible, mucus score and EIPH grade would be valuable features.

## Background & objective

IAD is defined as failure of inflammatory cell homeostasis in airways, presenting variable signs without overt systemic disease. It is characterised by increased neutrophils (≥20%) and/or eosinophils (≥1%) in tracheal lavage. EIPH refers to presence of blood in airways after intense exercise, scored 0–4 endoscopically. Two proposed mechanistic links between EIPH and IAD: (1) bronchoconstriction from airway inflammation increases intrapleural pressure → promotes bleeding; (2) blood in alveoli from EIPH causes secondary inflammation and fibrosis. However, prior studies in racehorses were inconclusive.

The research gap: no study of EIPH–IAD association in polo ponies (a growing sport population in Brazil). The specific question: do EIPH-positive polo ponies have a different tracheal cytological profile (more neutrophils/eosinophils) than EIPH-negative horses?

## Materials & methods

**Subjects:** 37 polo horses regularly used for polo practice in Rio de Janeiro, Brazil (21 females, 16 males; age 3–22 years; BW 380–480 kg). All clinically healthy, no active disease or medication during study. Vaccinated against tetanus, influenza, eastern/western encephalomyelitis, and rabies. Housed in 12 m² stalls under identical bedding conditions, fed commercial food + oat + alfalfa hay + mineral salt supplement. Approved by Committee for Animal Experimentation of Fluminense Federal University (study number 0068-08).

**Exercise regime:** Each horse participated in one polo chukka (7.5 min per chukka, 3-min rest between chukkas), twice per week. On other days, only light exercise.

**Endoscopic examination:** Performed 30–90 min post-exercise. Blood in airways scored 0–4 (EIPH scale). Tracheal mucus scored 0–4. EIPH grade definitions: 0 = no blood; 1 = one or more streaks of blood not covering >1/3 of tracheal surface; 2 = multiple streams covering >1/3 but not >90%; 3 = confluent streams covering >90%; 4 = trachea flooded.

**Tracheal lavage (TW):** Performed 12–16 hr post-exercise (most cited as 18–24 hr in abstract; protocol described as 12–16 hr in methods). Centrifuged at 110 g × 5 min (model 206R; Fanem). Cell pellets for linear smear slides; fixed in methanol, stained with Giemsa. 300-cell differential count per slide.

**IAD definition:** Neutrophils ≥20% AND/OR eosinophils ≥1% in TW = IAD positive (following Robinson 2003 and Kusano et al. 2008).

**Statistical analysis:** IBM SPSS Statistics. Kolmogorov-Smirnov for normality. Mann-Whitney U test comparing EIPH-positive vs. EIPH-negative cell-type proportions. Significance: P < 0.05.

## Results (detailed — reproduce ALL numbers)

**Endoscopic mucus scoring:**

| Mucus score | n   | %     |
| ----------- | --- | ----- |
| 0           | 16  | 43.2% |
| 1           | 12  | 32.4% |
| 2           | 9   | 24.3% |
| ≥3          | 0   | 0%    |

**EIPH prevalence and grades:**

- EIPH positive: 11/37 horses = **29.7%**
- EIPH grade 1: 7 horses
- EIPH grade 2: 1 horse
- EIPH grade 3: 2 horses
- EIPH grade 4: 0 horses
- (Note: 11 horses listed but 7+1+2 = 10; paper states 11/37)

**IAD prevalence and cytological findings:**

- IAD positive: 12/37 horses = **32.4%**
- Neutrophilia (≥20% neutrophils in TW): 9/37 = 24.3%
- Eosinophilia (≥1% eosinophils): 3/37 = 8.1%

**Table 1 — TW differential cell counts by IAD status (mean ± SD):**

| Cell type            | Healthy (n=25) | IAD (n=12)    |
| -------------------- | -------------- | ------------- |
| Neutrophils (%)      | 7.39 ± 6.31    | 57.76 ± 28.82 |
| Lymphocytes (%)      | 7.80 ± 7.28    | 4.36 ± 1.95   |
| Macrophages (%)      | 36.63 ± 22.59  | 24.71 ± 20.01 |
| Eosinophils (%)      | 0.36 ± 0.76    | 0.10 ± 0.21   |
| Epithelial cells (%) | 47.68 ± 29.48  | 13.03 ± 18.05 |

IAD horses had dramatically elevated neutrophils (57.76 vs. 7.39%), confirming neutrophilic inflammation as the defining cytological finding.

**Table 2 — TW differential cell counts by EIPH status (mean ± SD):**

| Cell type            | No EIPH (n=26) | EIPH (n=11)   | P-value |
| -------------------- | -------------- | ------------- | ------- |
| Neutrophils (%)      | 19.26 ± 24.61  | 20.50 ± 31.36 | P=0.431 |
| Lymphocytes (%)      | 7.71 ± 7.35    | 5.16 ± 3.76   | P=0.743 |
| Macrophages (%)      | 31.05 ± 19.25  | 40.05 ± 28.41 | P=0.418 |
| Eosinophils (%)      | 0.26 ± 0.64    | 0.38 ± 0.77   | P=0.183 |
| Epithelial cells (%) | 41.6 ± 29.71   | 33.7 ± 34.18  | P=0.540 |

**No significant difference** in any cell type between EIPH-positive and EIPH-negative horses (Mann-Whitney, all P > 0.05). Same superscript letters denote no statistically significant differences.

**Prevalence comparison with prior polo studies:**

- Previous report from same group (Otaka et al. 2014, same population): EIPH 34.09% — consistent with 29.7% found here
- Chile polo ponies (Moran et al. 2003): 46% EIPH prevalence — higher
- Brazil polo/riding horses (Lessa et al. 2005): lower EIPH rate — different population
- Racehorses: 15–75% by endoscopy, up to 90% by BAL (McKane et al. 1993)
- IAD occurrence in this study: similar to army horses (Sad et al. 2013), lower than racehorses in training

## Discussion & interpretation

The lack of significant association between EIPH grade and tracheal cytological profile supports the **capillary stress failure hypothesis** for EIPH: pulmonary capillary bleeding is caused by high intrapleural pressures during intense exercise, independent of whether airway inflammation is present. EIPH can occur without pulmonary inflammation as a prerequisite.

The time window between EIPH episode and tracheal lavage (12–16 hr) may have been insufficient to capture a secondary inflammatory response to blood in the airways. Prior studies showing EIPH-associated neutrophilia may have used different timing, allowing more time for inflammatory cells to accumulate.

The IAD prevalence (32.4%) despite all animals appearing clinically healthy and physically active highlights that subclinical airway inflammation is common in athletic horses. This supports screening programs rather than symptom-based diagnosis.

Bronchoalveolar lavage (BAL) is the gold standard for IAD diagnosis and would allow site-specific sampling from the caudal lung lobe (the primary EIPH bleed site). However, BAL requires ≥24 hr rest post-procedure, making it impractical for sporting animals; tracheal wash is the practical compromise.

The 12–16 hr post-exercise lavage timing and pooled sampling from both lungs (diluted compared to BAL) are procedural factors that could attenuate detection of EIPH-associated cytological changes.

## Limitations

- Small sample (n=37); limited statistical power, especially for detecting moderate associations
- Polo ponies (Brazil) may differ in EIPH/IAD biology from Japanese Thoroughbreds; exercise intensity in a 7.5-min polo chukka differs from a 1–3 min Thoroughbred flat race
- TW rather than BAL used; TW has poorer sensitivity and is subject to interoperator and timing variability
- Time between EIPH and lavage (12–16 hr) may miss early post-hemorrhage inflammatory response
- No BAL to confirm IAD diagnosis; possibility of underdiagnosis
- No longitudinal tracking of EIPH episodes; unknown chronicity
- Endoscopic EIPH scoring has inter-observer variability; single-observer not specified as blind

## Feature-engineering notes for the model

- `eiph_history_grade` — highest EIPH grade observed in last N races (0–4 scale) — source: JRA pre/post-race endoscopy records if available — expected effect: negative (higher EIPH grade → more lung impairment → worse performance); grades 3–4 particularly predictive of dropout/retirement — encode as ordinal or treat grade 0 separately
- `eiph_recent_binary` — any EIPH (grade ≥1) in last 30 days — source: veterinary records — expected effect: negative on next race performance; EIPH within 3–7 days is risk factor for more severe next episode
- `iad_positive` — binary, neutrophils ≥20% or eosinophils ≥1% in TW — source: JRA veterinary records — expected effect: negative on performance; horses with active IAD show poor performance — NOT the same as EIPH; model separately
- `mucus_score_endoscopy` — 0–4 score at post-exercise endoscopy — source: JRA endoscopic examination records — expected effect: negative (higher mucus → worse upper airway health → worse performance); may precede overt IAD — include if available
- `respiratory_tx_flag` — binary: horse receiving furosemide or other respiratory treatment — source: JRA medication records — expected effect: furosemide treats EIPH acutely but flags pre-existing problem — include as treatment history feature (NB: furosemide banned in Japan so this feature would encode historical treatment in other jurisdictions)
- `days_since_last_eiph` — days elapsed since last confirmed EIPH episode — source: veterinary records — expected effect: negative early post-EIPH (higher risk of recurrence within 3–7 days); recovers after several weeks — exponential decay encoding recommended
- **CAUTION:** EIPH and IAD should NOT be combined into a single "respiratory disease" feature — this paper confirms they are independent conditions with potentially different performance impacts; treat separately
- **CAUTION:** tracheal mucus score alone is not sufficient to diagnose IAD — neutrophil count in lavage is required; do not conflate

## Key references / follow-up leads

- Kusano K, Ishikawa Y, Seki K, Kusunose R. 2008. Characteristic of inflammatory airway disease in Japanese thoroughbred racehorses. J. Equine Sci. 19: 25–29. [JRA-specific EIPH/IAD data]
- Couëtil LL et al. 2016. Inflammatory airway disease of horses—Revised Consensus Statement. J. Vet. Intern. Med. 30: 503–515. [Current IAD diagnostic criteria]
- McKane SA, Canfield PJ, Rose RJ. 1993. Equine bronchoalveolar lavage cytology: survey of thoroughbred racehorses in training. Aust. Vet. J. 70: 401–404. [Up to 90% EIPH in racehorses]
- Newton JR, Wood JL. 2002. Evidence of an association between inflammatory airway disease and EIPH in young Thoroughbreds during training. Equine Vet. J. Suppl. 34: 417–424.
- Sullivan S, Hinchcliff K. 2015. Update on exercise-induced pulmonary hemorrhage. Vet. Clin. North Am. Equine Pract. 31: 187–198.
- West JB, Mathieu-Costello O. 1994. Stress failure of pulmonary capillaries as a mechanism for exercise-induced pulmonary haemorrhage in the horse. Equine Vet. J. 26: 441–447.
