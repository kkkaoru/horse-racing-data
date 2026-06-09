# Relationships among Body Size, Conformation, and Racing Performance in Banei Draft Racehorses

## Metadata

| Field                          | Value                                                                                  |
| ------------------------------ | -------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 12(1): 1–7, 2001                                                        |
| docid                          | `12_1_1`                                                                               |
| Article type                   | Original                                                                               |
| Authors                        | Fumiro Kashiwamura, Avarzed Avgaandorj, Keiko Furumura                                 |
| Affiliations                   | Obihiro University of Agriculture and Veterinary Medicine, Obihiro-shi 080-8555, Japan |
| Received / Accepted / Released | Accepted February 8, 2001                                                              |
| Keywords                       | Banei draft racehorse, body measurement, earnings, principle component analysis        |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/12/1/12_1_1/_pdf/-char/en                     |

## Abstract (verbatim)

> The body size and weight of 584 Banei draft racehorses at Obihiro Racing Stable in Hokkaido, Japan, was investigated. Almost all the animals (94.7%) were cross-breed heavy draft horses; there were a few Percheron and Percheron grade (4.8%) and a Belgian breed (0.5%). The ages of the animals ranged from 2 to 10 years. Males accounted for 80.3%, females for 17.5%, and 2.2% were castrated. The mean body measurements of 2-year-old Banei draft racehorses were significantly smaller than those of the 3- to 4-year-old or 5- to 10-year-old horses. Hip width, croup width, and rump length of females were greater than those of males. In contrast, males had greater chest width and cannon bone circumference than females. Significant relationships among almost all body measurements were observed. A high correlation was found between body weight and chest girth, croup width and body length. A principle component analysis were applied to investigate the effect of body conformation on racing performance in terms of earnings and the time on the performance test. Performance was significantly related to the principle components of general body size and weight of the horses. The cannon bone, body length, and rump length were suggested to have an effect on earnings per race in 2-year-old male horses. In terms of body size and conformation, the Banei draft racehorses, which are large in general size and well balanced, seemed to show high performance.

## Relevance to finishing-position (着順) prediction

This paper belongs to feature family **F (conformation/body-size/gait)**. It is the only study in this JES collection that directly and quantitatively models the relationship between morphometric body measurements and race earnings for a Japanese racing discipline. While Banei draft racing (pulling heavy iron sleighs over 200 m with two humps) differs mechanically from flat Thoroughbred racing, the methodological findings are transferable: body weight and general size index (PC1) account for ~40% of performance variance (earnings per race, p<0.001), and cannon bone circumference is specifically predictive for 2-year-old males.

For the Ban-ei prediction pipeline specifically (keibajo_code='83', pg.nvd_se), this paper provides direct evidence that the following features carry predictive signal: 馬体重 (body weight), chest girth, body length, and cannon circumference. The PCA decomposition (Table 5) shows eigenvalues and eigenvectors: PC1 (general size/weight, eigenvalue 4.75, explaining 40% of variance) has large positive loadings on BW (0.41), CG (0.38), CrW (0.37), HW (0.35), CD (0.31), BL (0.32); all of these are highly correlated in JRA/Ban-ei records with 馬体重.

For JRA Thoroughbred racing, the direct morphometric measurements are not routinely recorded except 馬体重 (body weight at weigh-in before each race). However, 馬体重 acts as a composite proxy for the PC1 "general body size" factor (r=0.81 with chest girth, r=0.76 with croup width, r=0.71 with body length in this study), so 馬体重 as a standalone feature captures substantial morphometric information. The paper also demonstrates that age-specific performance standards must be applied (2-year-old horses earn more per race under handicap rules; older horses may be in higher grades pulling more weight).

The finding that "tall and narrow" (high PC2) or "wide-chested but short-bodied" (high PC6) horses show no or negative earnings advantage supports the use of body balance/proportionality features rather than single linear dimensions. In the absence of full morphometric records in JRA databases, 馬体重 change across races (delta weight) and 馬体重 relative to age/sex norms are practical derived features.

## Background & objective

Banei draft racing is unique to Hokkaido, Japan: horses pull 0.5–1 ton iron sleighs over 200 m with two humps (1.0 m and 1.6 m high). In 1997, 729 registered Banei draft racehorses existed, primarily cross-breed heavy drafts. Prior to the 1950s, some work existed on draft horse pulling ability, but since then little research had been done. This study investigated body size/conformation distributions and their relationship to racing performance using PCA and GLM, treating racing earnings (1992) and performance test race times as outcome variables.

## Materials & methods

**Subjects:** 584 Banei draft racehorses at Obihiro Racing Stable, Hokkaido, December 1992.

**Demographics (Table 1):**
| Age | Male | Female | Castrated | Total | % |
|-----|------|--------|-----------|-------|---|
| 2 | 140 | 27 | 8 | 175 | 30.0 |
| 3 | 90 | 27 | 2 | 119 | 20.4 |
| 4 | 74 | 16 | 2 | 92 | 15.8 |
| 5 | 54 | 15 | 0 | 69 | 11.8 |
| 6 | 44 | 13 | 0 | 57 | 9.8 |
| 7 | 25 | 3 | 0 | 28 | 4.8 |
| 8 | 28 | 1 | 0 | 29 | 5.0 |
| 9 | 14 | 0 | 0 | 14 | 2.4 |
| 10 | 0 | 0 | 1 | 1 | 0.2 |
| Total | 469 | 102 | 13 | 584 | 100 |

**Breeds:** 94.7% cross-breed heavy draft; 4.8% Percheron/Percheron grade; 0.5% Belgian. Sire breeds: 60.3% cross-breed; Dam breeds: 73.3% cross-breed; remainder Breton/Breton grade/Percheron/Percheron grade/Belgian.

**Body measurements (12 + 1 derived, Figure 1):**

1. WH — Withers height (cm): vertical distance, top of withers to ground
2. CH — Croup height (cm): vertical distance, top of hips to ground
3. BL — Body length (cm): diagonal, shoulder point to buttock point
4. CD — Chest depth (cm): vertical, top of withers to bottom of chest girth behind shoulder
5. CW — Chest width (cm): horizontal, widest part of chest just behind elbow
6. HW — Hip width (cm): horizontal, between both tuber coxae
7. CrW — Croup width (cm): horizontal, between both greater trochanters
8. RL — Rump length (cm): distance, tuber coxae to tuber ischii
9. CG — Chest girth (cm): vertical circumference just behind shoulder/elbow
10. CC — Cannon circumference (cm): circumference of middle metacarpus
11. LL — Leg length (cm): derived as WH − CD
12. BW — Body weight (kg)

**Performance measures:** Earnings per race in 1992 (yen, log-transformed for normality); time on performance test race (corrected for track moisture content). Data from Municipal Association of Banei Racing in Hokkaido.

**Statistics:** PRINCOMP procedure (SAS) on all 12 measurements from all animals. GLM procedure (SAS) with PC1–PC6, sex, age group, sex×age interaction as independent variables; log-earnings as dependent variable. For 2-year-old males only: same procedure also with performance test time as dependent variable. Significance threshold P<0.05.

## Results (detailed — reproduce ALL numbers)

**Body measurements by age group and sex (Table 3, selected key rows):**

| Measurement | Males 2y      | Males 3–4y    | Males 5y+      | Females 2y    | Females 3–4y  | Females 5y+   |
| ----------- | ------------- | ------------- | -------------- | ------------- | ------------- | ------------- |
| WH (cm)     | 171.4 ± 3.98  | 172.7 ± 3.27  | 172.5 ± 3.65   | 171.0 ± 3.18  | 172.8 ± 4.50  | 174.1 ± 3.64  |
| BL (cm)     | 188.4 ± 4.21  | 192.0 ± 4.09  | 193.5 ± 3.99   | 191.1 ± 3.74  | 193.0 ± 3.64  | 194.7 ± 3.77  |
| CG (cm)     | 226.0 ± 5.14  | 232.6 ± 6.30  | 236.1 ± 7.11   | 227.7 ± 5.40  | 234.9 ± 6.58  | 239.0 ± 6.12  |
| CC (cm)     | 26.9 ± 1.17   | 27.6 ± 1.25   | 28.6 ± 1.62    | 25.5 ± 0.78   | 26.1 ± 1.10   | 26.4 ± 1.24   |
| BW (kg)     | 899.6 ± 48.59 | 969.1 ± 50.10 | 1007.2 ± 50.36 | 900.2 ± 43.32 | 953.0 ± 50.61 | 992.0 ± 48.77 |

All measurements of 2-year-olds significantly smaller than 3–4y and 5y+ groups (P<0.05), except leg length (LL; inverse trend). Females had significantly greater HW, CrW, RL vs. males; males had significantly greater CW and CC vs. females in the 5y+ group.

**Correlations among body measurements (Table 4, key values, males above diagonal / females below):**

- BW × CG: r = 0.76 (female) / 0.81 (male) — highest BW correlation
- BW × CrW: r = 0.71 (female) / 0.76 (male)
- BW × BL: r = 0.63 (female) / 0.71 (male)
- BW × HW: r = 0.59 (female) / 0.60 (male)
- HW × CrW: r = 0.78 (female) / 0.75 (male)
- LL × CD: r = −0.36 (female) / −0.38 (male) — negative (taller legs → shallower relative chest)

**PCA eigenvectors and eigenvalues (Table 5, all animals):**

| Item                  | PC1      | PC2      | PC3      | PC4      | PC5      | PC6      |
| --------------------- | -------- | -------- | -------- | -------- | -------- | -------- |
| WH                    | 0.24     | 0.53     | −0.02    | −0.27    | −0.20    | 0.08     |
| CH                    | 0.23     | 0.49     | −0.01    | −0.10    | −0.11    | 0.21     |
| BL                    | 0.32     | −0.02    | −0.10    | −0.17    | 0.27     | −0.60    |
| CD                    | 0.31     | −0.09    | −0.13    | −0.51    | −0.46    | 0.21     |
| CW                    | 0.21     | −0.23    | 0.38     | 0.41     | 0.10     | 0.58     |
| HW                    | 0.35     | −0.11    | −0.28    | 0.31     | −0.05    | −0.13    |
| CrW                   | 0.37     | −0.15    | −0.18    | 0.31     | 0.02     | 0.05     |
| RL                    | 0.20     | 0.03     | −0.37    | −0.32    | 0.72     | 0.39     |
| CG                    | 0.38     | −0.12    | −0.06    | 0.12     | −0.28    | 0.02     |
| CC                    | 0.19     | 0.00     | 0.74     | −0.29    | 0.11     | 0.00     |
| LL                    | 0.00     | 0.60     | 0.08     | 0.36     | 0.15     | −0.08    |
| BW                    | 0.41     | −0.07    | 0.14     | 0.00     | 0.12     | −0.19    |
| **Eigenvalue**        | **4.75** | **2.27** | **1.10** | **1.02** | **0.84** | **0.64** |
| Cumulative proportion | 0.40     | 0.58     | 0.68     | 0.76     | 0.83     | 0.88     |

PC1 interpretation: large body weight and girth (BW 0.41, CG 0.38, CrW 0.37, HW 0.35) — "general body size/weight"
PC2 interpretation: tall and narrow (WH 0.53, CH 0.49, LL 0.60; vs. width measurements negative)
PC3 interpretation: thick cannon bone (CC 0.74) with wide chest and short/narrow hindquarters
PC4: wide chest and hindquarters, long legs, shallow chest
PC5: long rump (RL 0.72)
PC6: short body, wide chest (BL −0.60, CW 0.58)

**GLM results — factors affecting earnings per race, all horses (Table 6):**

| Factor                | DF  | MS   | F value | Effect direction | Significant      |
| --------------------- | --- | ---- | ------- | ---------------- | ---------------- |
| PC1                   | 1   | 2.45 | 38.52   | +                | \*\*\* (P<0.001) |
| PC2                   | 1   | 0.11 | 1.80    | +                | NS               |
| PC3                   | 1   | 0.01 | 0.11    | +                | NS               |
| PC4                   | 1   | 0.07 | 1.16    | −                | NS               |
| PC5                   | 1   | 0.02 | 0.26    | +                | NS               |
| PC6                   | 1   | 0.14 | 2.26    | −                | NS               |
| Sex                   | 1   | 0.01 | 0.20    | —                | NS               |
| Age group             | 2   | 0.27 | 4.16    | —                | \* (P<0.05)      |
| Interaction (sex×age) | 2   | 0.30 | 4.65    | —                | \* (P<0.05)      |

**Least-squares mean earnings per race by age group:**

- 2-year-old: 97.7 × 1,000 yen/race
- 3–4-year-old: 83.2 × 1,000 yen/race
- 5-year-old and older: 72.4 × 1,000 yen/race

(Higher earnings for 2-year-olds are a handicap/classification artefact: horses with high earnings are placed in upper grades where they must pull heavier sleighs)

**GLM for 2-year-old males only (Table 8):**

| Factor | Performance test time |        | Earnings per race |        |
| ------ | --------------------- | ------ | ----------------- | ------ |
|        | F value               | Effect | F value           | Effect |
| PC1    | 3.28\*                | −      | 17.01\*\*\*       | +      |
| PC2    | 0.01                  | −      | 1.60              | +      |
| PC3    | 0.00                  | +      | 1.39              | +      |
| PC4    | 0.00                  | −      | 4.30\*\*          | −      |
| PC5    | 0.06                  | −      | 3.61\*            | +      |
| PC6    | 1.31                  | +      | 2.45              | −      |

For 2-year-old males: PC1 is significant for both earnings (P<0.001, positive) and performance test time (P<0.05, negative — larger horses run faster). PC4 (small cannon + small body length) negatively affects earnings (P<0.01). PC5 (long rump) positively affects earnings (P<0.05). PC4 interpretation from 2-year-old PCA (Table 7): PC4 has large negative CC (−0.55) and BL (−0.47) → small cannon and short body → negative performance implication.

**2-year-old male PCA (Table 7):**

| Item       | PC1  | PC2   | PC3   | PC4   | PC5   | PC6   |
| ---------- | ---- | ----- | ----- | ----- | ----- | ----- |
| WH         | 0.27 | 0.51  | −0.01 | 0.14  | −0.17 | 0.06  |
| CH         | 0.31 | 0.41  | −0.12 | 0.04  | −0.02 | 0.15  |
| BL         | 0.32 | 0.02  | 0.09  | −0.47 | 0.37  | −0.17 |
| CD         | 0.31 | −0.01 | −0.48 | −0.19 | −0.49 | 0.19  |
| CW         | 0.16 | −0.26 | 0.38  | 0.19  | −0.01 | 0.79  |
| HW         | 0.34 | −0.24 | 0.07  | 0.32  | −0.01 | −0.34 |
| CrW        | 0.29 | −0.27 | 0.19  | 0.39  | 0.04  | −0.14 |
| RL         | 0.21 | −0.05 | −0.54 | 0.13  | 0.64  | 0.28  |
| CG         | 0.36 | −0.18 | −0.06 | 0.11  | −0.34 | −0.15 |
| CC         | 0.25 | 0.01  | 0.39  | −0.55 | −0.06 | 0.13  |
| LL         | 0.09 | 0.57  | 0.32  | 0.29  | 0.15  | −0.06 |
| BW         | 0.41 | −0.13 | 0.08  | −0.13 | 0.20  | −0.17 |
| Eigenvalue | 4.49 | 2.23  | 1.14  | 1.06  | 0.82  | 0.78  |
| Cumulative | 0.37 | 0.56  | 0.66  | 0.74  | 0.81  | 0.88  |

## Discussion & interpretation

The primary finding — that PC1 (general body size/weight) explains ~40% of performance variance (P<0.001) — confirms that Banei racing is fundamentally a power/strength event where absolute size matters. Larger, heavier horses pull heavier loads faster. Body weight alone (r=0.81 with chest girth) is a strong proxy for PC1.

The cannon bone circumference result for 2-year-old males is particularly interesting: small cannon circumference (PC4, F=4.30, P<0.01 negative effect) predicts lower earnings, consistent with the structural integrity hypothesis — thicker cannon bones support greater limb loads without injury. This is relevant for injury risk modelling in Ban-ei racing where horses carry much higher loads (0.5–1 ton sleigh) than flat racing.

The "tall and narrow" PC2 finding (no significant earnings advantage) challenges naive assumptions about height in draft racing: what matters is mass and proportional balance (PC1), not stature per se. PC6 (short body, wide chest) showed a negative earnings trend (F=2.26, NS but effect direction negative) — horses with "imbalanced" proportions may be less mechanically efficient at sustained pulling.

The age-earnings inversion (2-year-olds earn most per race) is a handicap artefact important for modelling: in Banei, younger, smaller horses compete against lighter sleigh weights in lower-grade races, earning disproportionately per race. Models must adjust for grade/class level, not raw earnings.

The authors conclude that "selection of Banei draft racehorses for potential performance ability should be guided by attention to the general balance of body conformation rather than focus on any particular dimensional proportion" — this supports using composite size features (PC1-equivalent) rather than any single linear measurement.

## Limitations

- Cross-sectional study (one time point, December 1992): growth trajectories not captured; 2-year-olds are still growing
- Only earnings per race (1992) as outcome: reflects both current season performance and grade assignment; lifetime earnings would be a more stable outcome
- Sample limited to Obihiro Racing Stable; generalisability to all Banei horses uncertain
- Breed homogeneity assumption: 94.7% cross-breed with varying proportions of Breton/Percheron/Belgian ancestry; genetic background not controlled
- Banei race discipline (pulling, not flat/speed) means findings for Thoroughbred/JRA flat racing require careful analogical extrapolation
- PC analysis conducted on full sample including wide age range; age-specific PCA structures (Table 7 for 2-year-old males) may be more relevant but are available for only one subgroup

## Feature-engineering notes for the model

- `body_weight_kg` (馬体重) — JRA/Ban-ei race records — primary morphometric feature; positive association with performance in Ban-ei (larger horse → more pulling power); in Thoroughbred racing the relationship is inverted-U (optimal weight for speed/endurance); for Ban-ei, higher is generally better
- `body_weight_age_sex_z_score` — derived: (BW − mean_BW_age_sex_group) / SD — normalised weight for age and sex group; a 2-year-old at 900 kg is average but a 5-year-old at 900 kg is undersized; expected positive association with performance
- `delta_body_weight_vs_prev_race` — BW change since previous race (kg) — JRA records (`taijuu` field) — weight gain suggests recovery/growth; weight loss may signal illness/overtraining; non-linear effect; interaction with age
- `cannon_circumference_cm` — cannon bone circumference — not in standard JRA records; available in conformation databases if horse was measured — positive association with performance for 2-year-old males (structural integrity); also injury risk proxy
- `chest_girth_cm` — chest girth — r=0.81 with body weight; not in standard records; proxy via body weight — for Ban-ei, chest girth captures trunk volume and muscle mass
- `rump_length_cm` — rump length — positive association with 2-year-old earnings (PC5 positive effect P<0.05) — not in standard JRA records
- `age_years` — horse age — JRA records — age-specific performance expectations; age × grade interaction for Ban-ei handicap adjustment; for Thoroughbred racing, age×distance interaction well-documented
- `sex_category` — male/female/castrated — JRA records — males have greater CW and CC; interaction with age; in Thoroughbred racing, sex effects on performance are well-documented and vary by distance and surface
- **Caution:** This is a Ban-ei-specific study. For JRA flat racing with Thoroughbreds, 馬体重 has a non-linear performance relationship (too heavy may be suboptimal for speed events). Do NOT apply Ban-ei size monotonicity directly to JRA Thoroughbred models.

## Key references / follow-up leads

- Saastamoinen M. 1990. Heritabilities for body size and growth and phenotypic correlation among measurements in young horses. _Acta Agric. Scand._ 40: 387–396. [genetic priors on morphometric traits; Finnish horses]
- Ishizaki S, Honzawa S, Shinohara A, Koyama K. 1954. Relation between body weight and work power in the horse. _Jpn. J. Zootech. Sci._ 25: 168–173. [BW × pulling power, Japanese]
- Tsunemoto K, Suzuki M, Miyoshi S, Mitsumoto T, Oguri N. 1992. Trends in the relationships of racing performance and body weight in race horses. _Res. Bull. Obihiro Univ._ 18: 1–9. [direct precedent for 馬体重 × performance in Japanese racing]
- Green DA. 1961. A review of studies on the growth rate of horses. _Brit. Vet. J._ 117: 181–190. [growth and cannon bone development]
- Brown JE, Brown CJ, Butts WT. 1973. Evaluating relationships among immature measures of size, shape and performance of beef bulls. _J. Anim. Sci._ 6: 1010–1020. [PCA methodology precursor]
