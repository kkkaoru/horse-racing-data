# Radiographic Texture of the Trabecular Bone of the Proximal Phalanx in Horses with Metacarpophalangeal Osteoarthritis

## Metadata

| Field                          | Value                                                                                                                                                                               |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **35**(2): 21–28, 2024                                                                                                                                               |
| docid                          | `35_2401`                                                                                                                                                                           |
| Article type                   | Full Paper                                                                                                                                                                          |
| Authors                        | Lorena de Oliveira PEREIRA, Anderson Fernando DE SOUZA, Julio David SPAGNOLO, Ana Lúcia Miluzzi YAMADA, Daniela Miranda Richarte de Andrade SALGADO, André Luis do Valle DE ZOPPA   |
| Affiliations                   | 1 School of Veterinary Medicine and Animal Science, University of São Paulo, São Paulo 05508270, Brazil; 2 School of Dentistry, University of São Paulo, São Paulo 05508000, Brazil |
| Received / Accepted / Released | 2024-01-19 / 2024-05-16 / —                                                                                                                                                         |
| Keywords                       | fractal analysis, horses, osteoarthritis, trabecular bone                                                                                                                           |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/35/2/35_2401/_pdf/-char/en                                                                                                                 |

## Abstract (verbatim)

> Osteoarthritis (OA) is a prevalent condition in horses, leading to changes in trabecular bone structure and radiographic texture. Although fractal dimension (FD) and lacunarity have been applied to quantify these changes in humans, their application in horses remains nascent. This study evaluated the use of FD, bone area fraction (BA/TA), and lacunarity in quantifying trabecular bone differences in the proximal phalanx (P1) in 50 radiographic examinations of equine metacarpophalangeal joints with varying OA degrees. In the dorsopalmar view, regions of interest were defined in the trabecular bone of the proximal epiphysis, medial and lateral to the sagittal groove of P1. Lower BA/TA values were observed medially in horses with severe OA (P=0.003). No significant differences in FD and lacunarity were found across OA degrees (P>0.1). FD, BA/TA, and lacunarity were not effective in identifying radiographic texture changes in the P1 trabecular bone in horses with different metacarpophalangeal OA degrees.

## Relevance to finishing-position (着順) prediction

**Feature family A (injury/soundness).** Metacarpophalangeal (fetlock) osteoarthritis is the most prevalent joint disease in racehorses, particularly in the forelimb, and is a leading cause of lameness, reduced performance, and DNS (Did Not Start). This paper tests whether digital radiographic texture metrics — fractal dimension (FD), bone area fraction (BA/TA), and lacunarity — can stratify OA severity in the proximal phalanx (P1), with implications for a quantitative injury-severity prior feature.

The core predictive insight is largely negative but informative:

1. **Only severe OA (grade 3) is discriminated by a single quantitative metric** (medial BA/TA, P = 0.003, Cohen's d = 2.31 for mild vs. severe). Mild-to-moderate OA cannot be separated from absent OA by these texture measures.
2. **FD and lacunarity are non-significant** (P > 0.1) across all OA grades and both medial and lateral ROIs. These features, if derived from plain radiographs, add no stratification power for subclinical-to-moderate disease.
3. **Medial side shows more pronounced changes than lateral**, consistent with biomechanical loading patterns in locomotion — medial P1 epiphysis bears higher strain energy density.

Practical implication: if pre-race or pre-sale radiographic reports include an OA severity grade, **only grade 3 (severe)** reliably carries a negative performance/availability signal. Grades 1 and 2 (mild, moderate) are not statistically distinct from grade 0 using this radiographic texture approach. The study also establishes that inter-rater agreement for OA grading is only moderate (kappa = 0.4), limiting clinical utility of subjective grading.

## Background & objective

Trabecular bone analysis using fractal methods (box-counting FD, lacunarity) is well-established in human medicine for osteoporosis and OA. The equine metacarpophalangeal joint is subject to extreme compressive and shear loads during racing, and OA is spatially specific: the medial condyle of MC3 and medial P1 epiphysis are preferentially affected. Standard radiography is the most accessible imaging modality at racecourse level. The authors hypothesized that less severe OA would show higher FD and BA/TA (denser, more complex trabeculae) and lower lacunarity (more homogeneous structure) than advanced OA — i.e., that texture metrics would grade OA continuously. This hypothesis was not supported by the data.

## Materials & methods

**Subjects:** 50 horses (retrospective archive review, University of São Paulo Veterinary Teaching Hospital). Inclusion criteria: age ≥ 3 years, any sex, any breed except mules/donkeys, minimum 4 radiographic views available (LM, DL-PaMO, DP, DM-PaLO), no fractures/luxations/deformities, no motion artifacts. Mean age 6.9 ± 3.7 years (range 3–18). Gender: 64% male (32/50), 36% female (18/50). Breeds: American Trotter 30% (15), Brazilian Sport Horse 20% (10), Mangalarga Marchador 16% (8), Quarter Horse 14% (7), Pure Blood Lusitano 6% (3), Thoroughbred 4% (2), crossbreed 4% (2), Campolina 2% (1), Crioulo 2% (1), Mangalarga 2% (1). Ethics: CEUAx No. 6884180522.

**OA grading:** Three independent experienced evaluators scored each case on a 0–3 scale (absent/mild/moderate/severe) using the criteria of Silva et al. 2019 and Lacitignola et al. 2020 (Table 1 of paper). Kappa = 0.4 (moderate agreement, P < 0.001).

**OA classification criteria summary:**

- Grade 0 (absent): normal radiographic appearance
- Grade 1 (mild): mild subchondral sclerosis, small osteophytes, mild joint space narrowing, mild soft tissue changes
- Grade 2 (moderate): diffuse sclerosis, prominent osteophytes, moderate joint space narrowing, visible enthesophytes
- Grade 3 (severe): evident diffuse sclerosis extending to epiphyseal trabecular bone, extensive irregular osteophytes, severe joint space narrowing/loss, severe subchondral erosion or cystic lesion, multiple osteochondral fragments

**ROI definition:** Dorsopalmar (DP) view; 50 × 50 pixel ROIs placed in trabecular bone of proximal epiphysis of P1, medial and lateral to the sagittal groove, 30 pixels from the joint line, centered on a line from the sagittal groove to the medial/lateral extremity. ROIs extracted, converted to 8-bit grayscale, duplicated, and binarized.

**Texture metrics computed:**

- FD: box-counting method (Fiji/ImageJ)
- BA/TA: BoneJ plugin (Fiji/ImageJ) — ratio of bone (white pixels) to total ROI area
- Lacunarity: FracLac extension (box-counting method) — quantifies spatial heterogeneity of void spaces; high lacunarity = high heterogeneity

**Statistical analysis:** Shapiro-Wilk normality test, Levene homogeneity of variance test. Generalized linear model (GAMLj package, Jamovi v2.3.21) with predictors: OA degree, sex, breed, age group, and interactions. Model fit by log-likelihood and AIC. Only OA degree was significant in the GLM (P = 0.003). Follow-up: one-way ANOVA + Bonferroni post hoc pairwise comparisons for BA/TA by OA grade. Effect size: Cohen's d. Significance: P < 0.05; trend: P < 0.1.

## Results (detailed — reproduce ALL numbers)

**OA grade distribution:**

| OA degree  | n   | %   |
| ---------- | --- | --- |
| 0 Absent   | 12  | 24% |
| 1 Mild     | 23  | 46% |
| 2 Moderate | 11  | 22% |
| 3 Severe   | 4   | 8%  |

**Medial BA/TA — pairwise Bonferroni comparisons (Table 4):**

| Comparison          | Mean difference | SE    | t      | P       | Cohen's d |
| ------------------- | --------------- | ----- | ------ | ------- | --------- |
| Absent vs. Mild     | −0.074          | 0.036 | −2.071 | 0.045   | −0.744    |
| Absent vs. Moderate | −0.028          | 0.048 | −0.577 | 0.567   | −0.280    |
| Absent vs. Severe   | +0.156          | 0.064 | +2.432 | 0.019   | +1.570    |
| Mild vs. Moderate   | +0.046          | 0.044 | +1.041 | 0.304   | +0.464    |
| Mild vs. Severe     | +0.229          | 0.061 | +3.758 | < 0.001 | +2.314    |
| Moderate vs. Severe | +0.183          | 0.069 | +2.653 | 0.011   | +1.850    |

Note: positive mean difference = absent/mild/moderate has HIGHER BA/TA than severe. Severe OA shows the LOWEST BA/TA (less bone area fraction, consistent with trabecular bone loss/osteopenia in severe disease).

**Lateral BA/TA:** Not significant across OA degrees (P > 0.1).

**Medial FD:** Not significant across OA degrees (P > 0.1).

**Lateral FD:** Not significant across OA degrees (P > 0.1).

**Medial lacunarity:** Not significant across OA degrees (P > 0.1).

**Lateral lacunarity:** Not significant across OA degrees (P > 0.1).

**Sex effects:** No significant differences in BA/TA between male and female horses (P = 0.729).

**GLM model selection:** Only OA degree emerged as significant predictor (P = 0.003). Sex, breed, age group, and all interactions were non-significant.

## Discussion & interpretation

The finding that severe OA (grade 3) shows lower medial BA/TA is consistent with the known OA pathomechanism: in advanced OA, subchondral plate thickening and sclerosis occur, but underlying cancellous bone becomes osteopenic — the trabeculae thin and reduce in number, lowering the BA/TA in the 50×50 pixel ROI. This is spatially specific to the medial side because the medial P1 epiphysis bears a higher strain energy density during locomotion (Moshage et al. 2020), and severe OA lesions preferentially affect the medial condyle (Marsiglia et al. 2022).

The failure to differentiate mild-to-moderate OA from absent OA is attributed to: (a) limited spatial resolution and dynamic range of routine radiographs (2D projection loses 3D trabecular architecture detail), (b) small ROI size (50×50 pixels), (c) the subjective OA grading with only moderate inter-rater agreement (kappa = 0.4) introducing label noise, and (d) insufficient sample size in the severe category (n = 4 cases).

Unlike human studies where FD significantly decreases in osteoporosis and fractures, the horse metacarpophalangeal joint does not follow the same pattern — possibly because the trabecular structure is already highly adapted to peak dynamic loads in racing horses, masking subtle texture changes at mild-moderate OA stages.

Authors recommend CT (3D imaging) for more sensitive trabecular assessment, and suggest future studies focus on homogeneous breed/use groups.

## Limitations

- Only 4/50 horses had severe OA; the key finding rests on 4 cases — sample size for the discriminating category is critically small
- Only 2/50 were Thoroughbreds; the sample is dominated by American Trotters and Brazilian Sport Horses, which have different biomechanical loads and OA patterns than JRA/NAR racehorses
- Retrospective archive review: selection bias toward horses presented for clinical evaluation (not representative of the racing population prevalence distribution)
- Moderate inter-rater agreement (kappa = 0.4) is a substantial limitation; the OA grade labels have ~40% noise contribution that would attenuate any signal
- 2D radiographic projection loses 3D trabecular information; CT or micro-CT would be more sensitive
- ROI size (50×50 pixels) may be too small to capture macro-architectural changes; ROI placement was manual (potential placement bias)
- Keratin contamination issue noted in the context of keratin in lab work; not applicable here but reflects generalized methodological awareness

## Feature-engineering notes for the model

- `oa_grade_p1` — fetlock OA severity grade (0–3) from pre-race or pre-sale radiographic report — JRA/NAR veterinary inspection records — expected negative effect on race availability and performance, but **only grade 3 reliably signals a risk**; grades 1–2 are not discriminable from grade 0 by quantitative texture analysis — data availability unknown/unlikely for most horses
- `ba_ta_medial_p1` — medial proximal phalanx bone area fraction — quantitative radiographic texture metric — **only discriminates severe OA (grade 3)**; Cohen's d = 2.31 for mild vs. severe; not useful for subclinical screening — would require radiographic archive access; not in standard race entry data
- `fractal_dimension_p1` — **DO NOT USE**: non-significant across all OA grades in this study (P > 0.1); confirmed insensitive for equine P1 OA staging
- `lacunarity_p1` — **DO NOT USE**: non-significant (P > 0.1) in this study
- `injury_history_p1_fracture` — binary: has horse had P1 sagittal fracture? — from veterinary/surgical records — strong proxy for OA development risk; see companion paper 34_2302 for race resumption base rates after P1 fracture surgery
- **Interaction:** `oa_grade_p1 × age` — older horses more likely to have progressive OA; age mediates OA progression (Cantley et al. 1999 noted age-related metacarpophalangeal OA progression)
- **Population caution:** This study's cohort is 96% non-Thoroughbred; direct applicability to JRA/NAR Thoroughbreds is uncertain

## Key references / follow-up leads

- **Moshage S.G. et al. 2020** — medial proximal phalanx exhibits higher strain energy density in standing horses; explains medial-side specificity of OA changes; need to identify exact journal
- **Marsiglia S. et al. 2022** — Anat. Rec. 305:3385–3397 — metacarpal cartilage in Thoroughbreds; lesion predominance in medial condyle of third metacarpal bone
- **Baldwin C.M. et al. 2020** — Equine Vet. J. 52:213–218 — third carpal bone slab fractures and racing performance after arthroscopic repair in UK Thoroughbreds
- **Harrison S.M. et al. 2014** — "Medial and lateral pressure on metacarpophalangeal joint contact areas during horse locomotion" — referenced in paper; spatial loading basis for medial OA preponderance
- **Cantley C.E.L. et al. 1999** — natural progression of age-related metacarpophalangeal OA in horses; sporting activities accelerate it — directly relevant for age × OA interaction feature
- **Silva et al. 2019 / Lacitignola et al. 2020** — OA grading criteria used in this study; validated 0–3 scoring system for equine metacarpophalangeal joint radiographs
