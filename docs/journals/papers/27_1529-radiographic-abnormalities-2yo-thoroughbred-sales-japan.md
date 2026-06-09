# A retrospective study of radiographic abnormalities in the repositories of 2-year-old Thoroughbred in-training sales in Japan

## Metadata

| Field                          | Value                                                                                                                                                                                         |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 27(2): 67–76, 2016                                                                                                                                                             |
| docid                          | `27_1529`                                                                                                                                                                                     |
| Article type                   | Original Article                                                                                                                                                                              |
| Authors                        | Daisuke MIYAKOSHI, Hiroyuki SENBA, Mitsumori SHIKICHI, Masaya MAEDA, Ryo SHIBATA, Kazuhiro MISUMI                                                                                             |
| Affiliations                   | Hidaka Horse Breeders Association, Hokkaido 056-0002; The Japan Bloodhorse Breeders' Association, Hokkaido 056-0144; Faculty of Veterinary Medicine, Kagoshima University, Kagoshima 890-0065 |
| Received / Accepted / Released | October 23, 2015 / April 23, 2016 / 2016                                                                                                                                                      |
| Keywords                       | prognosis, radiography, repository, sales, Thoroughbred                                                                                                                                       |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/27/2/27_1529/_pdf/-char/en                                                                                                                           |

## Abstract (verbatim)

> This study aimed to evaluate the influence of radiographic abnormalities of 2-year-old Thoroughbred horses that were listed at in-training sales in Japan, on whether they started to race or not at 2–3 years of age. Radiographs of 850 2-year-old Thoroughbreds in the in-training sales repository from 2007 to 2010 were reviewed, and 26 categories of radiographic abnormalities were found. Forty-three horses (5.1%, 43/850) did not start a race at 2–3 years of age. In accordance with the racing results for this age category, as determined by Fisher's exact test and multiple logistic regression analysis, none of the radiographic abnormalities were significantly related to failure to start a race. At 2 years of age, 198 horses (23.3%, 198/850) did not start a race. Horses with enlargement of the proximal sesamoid bones in the fore (9 of 19 horses) and hind limbs (5 of 9 horses) did not start a race at the age of 2 years, and fewer of these horses (fore, P=0.021; hind, P=0.030) started a race at the age of 2 years compared with the population of horses without these radiographic abnormalities. These results suggest that identification of radiographic enlargement of the proximal sesamoid bones during training sales could derail the racing debut of horses at the age of 2 years. However, this might not necessarily indicate a poor prognosis and resulting in retirement from racing at 2–3 years of age.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **A (injury/soundness)**, specifically the sub-family of pre-race structural risk at time of sales. It provides the first and most comprehensive Japanese-specific data linking radiographic abnormalities at 2-year-old in-training sales to racing debut probability, covering 850 horses across four consecutive sales years (2007–2010).

The key finding — that proximal sesamoid bone (PSB) enlargement in fore (P=0.021) and hind (P=0.030) limbs significantly delays racing debut at 2 years — is directly applicable as a **debut-risk feature** in models predicting whether a horse will start its first race at age 2. Horses that delay debut represent a distinct population (shorter career exposure at 2yo, potentially different physical profile) that should be modelled differently from horses with standard 2yo debuts.

Critically, the paper distinguishes short-term (2yo debut delay) from longer-term effects (2–3yo career failure), finding that PSB enlargement does NOT predict 2–3yo retirement. This non-linearity is important: the same feature that predicts debut delay is NOT a valid predictor of long-term career failure, which must be modelled by other mechanisms (training load, cumulative race starts, injury accumulation). For the finishing-position pipeline, this means PSB enlargement should be used as a debut-gap indicator (interaction with age at first race) but not as a direct performance predictor for established horses.

The study also establishes baseline prevalence rates for 26 categories of radiographic abnormalities in Japanese 2yo sales Thoroughbreds — useful as prior probabilities for Bayesian approaches to injury risk.

## Background & objective

Radiographic repositories for limbs became mandatory for 2-year-old Thoroughbreds listed at in-training sales in Japan in 2006. These repositories — sets of radiographs submitted to sales companies before each sale and available to buyers — affect purchase price and subsequent management decisions. International studies from the UK, USA, and Australia had documented various radiographic risk factors at yearling and 2yo sales, but no study had evaluated Japanese-specific findings. This paper is the **first Japanese study** linking 2yo in-training sales radiographic abnormalities to actual racing outcomes.

## Materials & methods

**Study design:** Retrospective cohort study.

**Subjects:** 850 2-year-old Thoroughbreds listed at in-training sales in Japan, from 2007 to 2010.

- Year distribution: 171 (2007), 304 (2008), 217 (2009), 158 (2010)

**Radiographic evaluation:**

- Joints covered: metacarpophalangeal joints (fore fetlocks), metatarsophalangeal joints (hind fetlocks), and carpi
- Views for carpi: lateromedial; dorsal 45° lateral-palmar medial oblique; dorsal 35° medial-palmar lateral oblique (3 views)
- Views for fetlocks: dorsal 30° proximal-palmar distal oblique; lateromedial; dorsal 15° proximal 45° lateral-palmar distal medial oblique; dorsal 15° proximal 45° medial-palmar distal lateral oblique (4 views)
- Evaluators: 4 experienced equine practitioners for carpi; 3 for fetlocks. Discrepancies re-evaluated by first author.
- Exclusion: non-diagnostic views in any joint series led to exclusion of that joint series.
- Evaluable series: 636 carpus, 691 fore fetlock, 660 hind fetlock (out of 850 total)

**26 categories of radiographic abnormalities** classified, covering:

- _Carpus (5 categories):_ dorsal medial intercarpal joint disease, fragments, osteophytes, circular lucencies, accessory carpal bone fracture
- _Fore fetlock (10 categories):_ proximal dorsal P1 fragments, proximal palmar P1 fragments, subchondral cystic lesions (SCLs) at distal MC3/proximal P1, fragments at dorsal aspect of distal MC3, irregular lucencies at dorsal aspect of distal MC3, osteophytes at proximal dorsal MC3, enthesophytes at palmar P1, PSB enlargement, PSB fractures, PSB modelling
- _Hind fetlock (8 categories):_ proximal dorsal P1 fragments, proximal palmar P1 fragments, SCLs at distal MT3/proximal P1, fragments at dorsal aspect of distal MT3, irregular lucencies at dorsal aspect of distal MT3, enthesophytes at plantar P1, PSB enlargement, PSB fractures, PSB modelling

**Outcome measures:**

- Primary: failure to start any race at 2–3 years of age (vs. started ≥1 race)
- Secondary: failure to start any race specifically at 2 years of age (vs. started ≥1 race at 2yo)
- Data source: Japan Bloodstock Information System (JBIS, Japan Bloodhorse Breeders' Association)

**Statistical methods:**

- Univariate: Fisher's exact test (P<0.05 significant)
- Multivariable: single logistic regression for screening (P<0.20 inclusion threshold), followed by multiple logistic regression (backward elimination to P<0.20; final significance P<0.05)
- Software: JMP version 7.0 (SAS Institute)

## Results (detailed — reproduce ALL numbers)

### Overall outcomes

- Starters at 2–3 years: 807/850 (94.9%)
- Non-starters at 2–3 years: 43/850 (5.1%)
- Starters at 2 years specifically: 652/850 (76.7%)
- Non-starters at 2 years: 198/850 (23.3%)

### Prevalence of radiographic abnormalities

**Carpus (n=636 evaluable series):**

| Abnormality                             | n   | Prevalence |
| --------------------------------------- | --- | ---------- |
| Osteophytes at carpal bone              | 35  | 5.5%       |
| Circular lucencies at carpal bone       | 8   | 1.3%       |
| Dorsal medial intercarpal joint disease | 14  | 2.2%       |
| Fragments at carpal bones               | 7   | 1.1%       |
| Accessory carpal bone fracture          | 2   | 0.3%       |

**Fore fetlock (n=691 evaluable series):**

| Abnormality                                      | n   | Prevalence |
| ------------------------------------------------ | --- | ---------- |
| Modelling in proximal sesamoid bones             | 37  | 5.4%       |
| Osteophytes at proximal dorsal MC3               | 25  | 3.6%       |
| Proximal dorsal fragments at P1                  | 21  | 3.0%       |
| Enthesophytes at palmar P1                       | 17  | 2.5%       |
| PSB enlargement                                  | 19  | 2.7%       |
| Fragments at dorsal distal MC3                   | 11  | 1.6%       |
| PSB fractures                                    | 3   | 0.4%       |
| Proximal palmar P1 fragments                     | 3   | 0.4%       |
| SCLs at distal MC3 or proximal P1                | 3   | 0.4%       |
| Irregular lucencies at sagittal ridge distal MC3 | 2   | 0.3%       |

**Hind fetlock (n=660 evaluable series):**

| Abnormality                               | n   | Prevalence |
| ----------------------------------------- | --- | ---------- |
| Proximal palmar P1 fragments              | 40  | 6.1%       |
| PSB modelling                             | 22  | 3.3%       |
| Proximal dorsal P1 fragments              | 18  | 2.7%       |
| Enthesophytes at plantar P1               | 16  | 2.4%       |
| PSB enlargement                           | 9   | 1.4%       |
| PSB fractures                             | 8   | 1.2%       |
| Fragments at dorsal distal MT3            | 4   | 0.6%       |
| Irregular lucencies at sagittal ridge MT3 | 4   | 0.6%       |
| SCLs at distal MT3 or proximal P1         | 0   | 0%         |

### Statistical results: 2–3 year starter vs. non-starter

**Carpus (Table 1 — 2–3yr outcome):** No radiographic abnormality was significantly associated with failure to start at 2–3 years (all P≥0.10 by Fisher's exact test). Notable OR but wide CIs: accessory carpal bone fracture OR=18.9 (95% CI 1.2–308.7), P=0.10.

**Fore fetlock (Table 3 — 2–3yr outcome):** No radiographic abnormality was significantly associated with failure to start at 2–3 years. PSB fracture: OR=11.0 (95% CI 1.0–124.3), P=0.13.

**Hind fetlock (Table 5 — 2–3yr outcome, shown in PDF pages 5–6):** No significant association.

**Multivariable analysis (2–3yr outcome):** No radiographic abnormality reached significance.

### Statistical results: 2-year starter vs. non-starter

**Carpus (Table 2 — 2yr outcome):** No significant associations.

**Fore fetlock (Table 4 — 2yr outcome):**

| Abnormality                     | Starters (n=533) | Non-starters (n=158) | OR  | 95% CI  | P (Fisher) |
| ------------------------------- | ---------------- | -------------------- | --- | ------- | ---------- |
| PSB enlargement (fore)          | 10/19 (52.6%)    | 9/19 (47.4%)         | 3.2 | 1.3–7.9 | **0.021**  |
| PSB modelling (fore)            | 24/37 (64.9%)    | 13/37 (35.1%)        | 1.9 | 0.9–3.8 | 0.072      |
| Enthesophytes palmar P1         | 11/17 (64.7%)    | 6/17 (35.3%)         | 1.9 | 0.7–5.1 | 0.24       |
| Fragments dorsal distal MC3     | 7/11 (63.6%)     | 4/11 (36.4%)         | 2.0 | 0.7–6.8 | 0.28       |
| Osteophytes proximal dorsal MC3 | 23/25 (92.0%)    | 2/25 (8.0%)          | 0.3 | 0.1–1.2 | 0.088      |

**Hind fetlock (Table 6 — 2yr outcome):**

| Abnormality            | Starters    | Non-starters | OR  | 95% CI | P (Fisher) |
| ---------------------- | ----------- | ------------ | --- | ------ | ---------- |
| PSB enlargement (hind) | 4/9 (44.4%) | 5/9 (55.6%)  | —   | —      | **0.030**  |

**Multivariable logistic regression (2yr outcome):**

- PSB enlargement (fore): P=**0.013** (significant)
- PSB enlargement (hind): P=**0.031** (significant)
- No other variables reached significance after backward elimination

### Summary of key OR and CI from Tables 2 and 4 for carpus (2yr outcome, Table 2):

| Abnormality                      | OR  | 95% CI   | P    |
| -------------------------------- | --- | -------- | ---- |
| Dorsal medial intercarpal lesion | 0.9 | 0.3–4.0  | 1.0  |
| Osteophytes carpal               | 1.3 | 0.6–2.9  | 0.41 |
| Accessory carpal bone fracture   | 3.3 | 0.2–53.5 | 0.41 |

## Discussion & interpretation

The study makes two main conclusions:

1. **26 radiographic abnormality categories found; these affect <7% of the 2yo sales population.** Most radiographic findings are not clinically significant for racing debut prediction.

2. **Only PSB enlargement (fore and hind) delays 2yo debut; no radiographic finding predicts 2–3yo career failure.** This distinction is critical: the prognostic horizon matters. Most horses with PSB enlargement eventually start (10/19 fore, 4/9 hind did start at 2yo), confirming that the effect is a delay rather than a career-ending risk factor.

Comparisons with international studies: osteophytes at carpal bones (5.5% in this study vs. 2.7% Jackson et al., 1.7% Kane et al. at yearling sales) and proximal dorsal P1 fragments (3.0% here vs. 0.7–1.6% in yearling studies) were higher, likely reflecting the older age (2yo vs. 1yo) at examination. Previous USA study (Meagher et al.) found PSB fracture, sesamoiditis, and wedge-shaped tarsal bones as risk factors at 2yo in-training sales — the Japanese study did not replicate PSB fractures as significant (only 3 fore / 8 hind cases observed).

The dorsal medial intercarpal joint disease finding was not significant in this study (2.2%), despite a previous yearling-sales study (Kane et al.) finding 63% of affected yearlings failed to start. Possible explanation: 2yo horses may have more advanced healing of subchondral lesions at the point of training sales.

## Limitations

- Retrospective design; radiographic repository was designed for clinical/commercial purposes, not standardised for research.
- n=850 covers only 4 sales years (2007–2010) from Hidaka sales; may not represent all Japanese Thoroughbred breeding regions.
- Multiple testing: 26 categories × 2 outcomes = 52 comparisons; not corrected for multiple comparisons. The P=0.021 and P=0.030 findings should be interpreted cautiously.
- Multivariable analysis required exclusion of 214/850 (carpus), 159/850 (fore fetlock), and 190/850 (hind fetlock) horses with non-diagnostic views — introduces potential selection bias.
- Outcome is binary (started/did not start) and does not capture performance quality, race earnings, or finishing position. No modelling of number of starts, career earnings, or performance level.
- No follow-up beyond age 3; horses may have started later (age 4+) and been classified as "non-starters" in this study.

## Feature-engineering notes for the model

- `psb_enlargement_fore` — binary: presence of PSB enlargement in forelimb at 2yo in-training sales — source: JBBA/HHBA sales repository data — expected effect: OR=3.2 for failure to debut at 2yo; positive predictor of debut delay; NOT a predictor of 2–3yo retirement — multivariable P=0.013
- `psb_enlargement_hind` — binary: presence of PSB enlargement in hindlimb at 2yo in-training sales — source: same — expected effect: OR not reported but P=0.031 in multivariable; positive predictor of debut delay
- `psb_enlargement_combined` — binary OR score (fore OR hind) — stronger combined signal; 19/850 = 2.7% fore, 9/850 = 1.4% hind — interaction with `age_at_first_race`: if first race at age 3+, PSB enlargement at sales no longer predictive
- `radiographic_finding_count_2yo` — count of abnormal categories at 2yo sales — source: same — expected effect: weak overall risk proxy, but no individual category (other than PSB enlargement) is independently significant
- `age_at_first_race` — continuous (months) — source: JRA/JBR race records — expected effect: interacts with PSB enlargement; horses with PSB enlargement who debut late are "delayed but not inferior" — do not penalise this group in performance models once started
- **Do NOT use** PSB enlargement as a predictor of race performance for horses that have already established a racing record — its effect is restricted to debut timing, not performance quality

## Key references / follow-up leads

- Meagher DM et al. 2006. [2yo in-training sales radiographic abnormalities and racing outcomes, USA]. (direct international comparator; found PSB fracture, sesamoiditis, wedge-shaped tarsal bones as risk factors)
- Kane AJ et al. [Yearling sales radiographic abnormalities and racing outcomes]. (dorsal medial intercarpal joint disease: 63% of affected yearlings failed to start)
- Jackson BF et al. [Yearling sales radiographic abnormalities in the UK]. (prevalence comparator)
- Verwilghen D et al. [In-training sales radiographic repositories in Europe]. (international comparator)
- Companion papers: 27_1514 (JRA fracture epidemiology) and 28_1702 (PSB apical fractures in foals — histopathological mechanism for PSB pathology)
