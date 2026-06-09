# Clinical Usefulness of the Measurement of Bone Mineral Content by Radiographic Absorptiometry in the Young Thoroughbred

## Metadata

| Field                          | Value                                                                                                                                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 18(3): 99–106, 2007                                                                                                                                                                       |
| docid                          | `18_3_99`                                                                                                                                                                                                |
| Article type                   | Note                                                                                                                                                                                                     |
| Authors                        | Mitsutoshi Kobayashi, Kunihide Ando, Mikihiro Kaneko, Yoshinobu Inoue, Yo Asai, Hiroyuki Taniyama                                                                                                        |
| Affiliations                   | Blood Horse Training Center, 141 Nishisya, Urakawa, Hokkaido 057-0171; Hidaka Training and Research Center, Japan Racing Association, Urakawa 057-0171; Rakuno Gakuen University, Ebetsu 069-8501, Japan |
| Received / Accepted / Released | Accepted June 14, 2007                                                                                                                                                                                   |
| Keywords                       | bmc, bone disorder, bone metabolism marker, horse                                                                                                                                                        |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/18/3/18_3_99/_pdf/-char/en                                                                                                                                      |

## Abstract (verbatim)

> The purpose of this investigation was to evaluate the clinical usefulness of the total radiographic bone aluminum equivalency (RBAE) method for the measurement of bone mineral content (BMC) in the third metacarpal bone (McIII) of the young Thoroughbred by examining: a) changes in BMC and bone metabolism marker levels in young Thoroughbreds being trained in the growth period; b) correlation of BMC and bone metabolism marker levels; and c) relationships of BMC and bone metabolism marker levels with the state of occurrence of bone disorders. The total RBAE method by the ortho system was chosen for this study. Ninety-one 2-year-old Thoroughbreds that showed no abnormalities on physical examination before the study were evaluated for BMC, and 39 were selected at random for evaluation of the bone metabolism marker levels. BMC of the normal horses showed no change between Days 0 and 90 of the study, but significantly increased on Day 180 (p<0.01). BMC showed no correlations with bone metabolism markers. In addition, there was a high incidence of bone disorders in Days 0–90 of the study when BMC did not change though training increased. These results suggest that the measurement of BMC by the total RBAE method in young Thoroughbreds is useful for providing information about skeletal development, and also for obtaining information on the predisposing period for bone disorders.

## Relevance to finishing-position (着順) prediction

Feature family: **A — injury/soundness (bone disorder / developmental orthopedic disease)**. Bone disorders (bucked shins, splints, developmental orthopedic diseases [DOD], sesamoiditis) in 2-year-old Thoroughbreds during the first months of race training represent a major source of training interruption and race-absence events. This study quantifies the temporal risk window: **56% of bone disorders occurred in Days 0–90 of race training**, when BMC stagnated despite increasing training load — a direct consequence of bone remodelling reversal (resorption activated before formation).

For the finishing-position pipeline, the most actionable implication is a **career-phase risk prior**: the first ~90 days of race training at age ~20–23 months corresponds to a high bone-disorder incidence period. In the JRA race record context, this maps to a horse's first season (2-year-old summer–autumn). Features encoding `career_phase` (maiden/early-career, number of career races) and `horse_age_months` around 20–29 months provide proxy information for bone-health risk phase. The sex difference found (males significantly higher BMC than females at Day 270, mean age 29 months) supports a `sex × career_age` interaction.

The null result on bone metabolism markers (OC, PICP, ICTP) as predictors of individual bone disorder occurrence is also important: these serum markers are **not useful individual-level risk predictors** at the measurement intervals used (90-day intervals), and their derivation from race records is impossible anyway. This reduces model development scope to timing-based and BMC-based features only. The companion paper (docid 19_4_83, Inoue et al. 2008) provides intensity-dependent marker responses in a controlled protocol that extends the interpretation.

## Background & objective

Racehorses begin race training at approximately 20 months of age; training intensity increases rapidly. During growth, both bone resorption and formation are enhanced, but the balance shifts with training load. The RBAE (radiographic bone aluminium equivalency) method measures BMC in the third metacarpal bone (McIII) non-invasively, without anaesthesia or sedation, and the total RBAE variant accounts for volumetric changes. BMC below the nutritional foramen of McIII was previously linked to limb bone disorders. Prior studies reported BMC decreases during 50–100 days after training onset, attributed to activation of bone remodelling. No study had simultaneously measured BMC, bone metabolism markers (OC, PICP, ICTP), and bone disorder incidence in the same longitudinal cohort of young Japanese Thoroughbreds.

## Materials & methods

**Subjects:** 91 two-year-old Thoroughbred horses (52 males, 39 females), mean age 20 ± 0.8 months at study start, no abnormalities on physical examination. All trained at the Blood Horse Training Center, Urakawa.

**Bone metabolism markers subset:** 39 randomly selected horses (24 males, 15 females) from the 91.

**Training and management:** Horses broken-in ~60 days before study start; management changed from pasture to stabling (with daily paddock release 3–5 hr in 5 m × 5 m paddock). Training 6 days/week on sand track. Training intensity increased progressively; running distance and velocity recorded as exercise intensity indices.

**Exercise intensity (mean ± SD):**

| Period                    | Running distance (m) | Running velocity (m/min) |
| ------------------------- | -------------------- | ------------------------ |
| Days −90 to 0 (pre-study) | 434 ± 670            | 427 ± 111                |
| Days 0–90                 | 1,776 ± 811          | 563 ± 89                 |
| Days 90–180               | 2,354 ± 536          | 712 ± 114                |
| Days 180–270              | 2,270 ± 423          | 676 ± 115                |

Both distance and velocity increased significantly (P < 0.01) from pre-study to Days 90–180; no change between Days 90–180 and 180–270.

**BMC measurement:** Total RBAE method (modified RBAE accounting for volumetric changes; Kobayashi et al. 2006, J. Equine Sci. 17: 105–112). Instrument: portable X-ray (FPS-X, Flat Co.); ortho-type intensifying screen/film; aluminium step wedges (11-step, X-ray Marketing Associates); dorsopalmar views; developed with Kodak X-Omat 1000 Processor; analysed with GS700 photodensitometer (BioRad). Site: BMC immediately below nutritional foramen of **left McIII**. Measured at Days 0, 90, 180, 270.

**Exclusions from BMC analysis:** 22 horses requiring ≥1 week rest due to injury or locomotive disease were excluded from healthy horse BMC analysis. Horses that left to race also excluded. Final healthy BMC sample sizes: n = 69 (Day 0), 68 (Day 90), 54 (Day 180), 46 (Day 270).

**Bone metabolism markers:** Blood samples from jugular vein at **same hour on Days 90 and 270** (diurnal variation controlled). Centrifuged 2,300 g × 20 min; serum stored at −80°C. Assays by RIA (double-antibody):

- OC (osteocalcin, bone formation): DiaSorin Inc. commercial kit; intra-assay CV 5.2%, inter-assay CV 5.9%; range 0.78–25 ng/ml.
- PICP (type I procollagen carboxyterminal propeptide, bone formation): Orion Diagnostica; intra-assay CV 4.5%, inter-assay CV 6.6%; range 6.25–500 ng/ml.
- ICTP (type I collagen cross-linked C-telopeptide, bone resorption): Orion Diagnostica; intra-assay CV 4.2%, inter-assay CV 5.4%; range 0.25–50 ng/ml.

**Statistics:** Tukey's HSD test for monthly BMC and marker changes; Spearman's rank correlation (rs) for BMC vs. markers; t-test (homoscedastic) or Welch's test (non-homoscedastic) for sex comparisons and injured vs. uninjured marker comparisons. Significance P < 0.05 and P < 0.01.

## Results (detailed — reproduce ALL numbers)

**BMC of healthy horses (McIII below nutritional foramen, mm²Al):**

| Sex                   | Day 0     | Day 90    | Day 180       | Day 270       |
| --------------------- | --------- | --------- | ------------- | ------------- |
| Males (n→69→68→54→46) | 984 ± 246 | 974 ± 186 | 1,146 ± 222\* | 1,128 ± 141\* |
| Females (same ns)     | 898 ± 226 | 897 ± 151 | 1,106 ± 218\* | 1,026 ± 157   |

\*Significantly higher than Days 0 and 90 (P < 0.01).

- No significant change between Days 0 and 90 (both sexes).
- Significant increase at Day 180 (males and females) vs. Days 0 and 90 (P < 0.01).
- No significant change between Days 180 and 270.
- **Males significantly higher BMC than females at Day 270** (P < 0.05, mean age ~29 months).

**Interpretation of Days 0–90 stagnation:** Authors attribute the flat BMC period to competing effects — BMC decrease from management change (pasture → stable, Hoekstra et al. 1999) and increased bone resorption at training onset, offset by training-stimulated formation. A "reversal period" lasting ~90 days before net BMC increase occurs.

**Bone metabolism markers (healthy horses; values in ng/ml, mean ± SD):**

| Marker | Males Day 90 (n=20) | Males Day 270 (n=20) | Females Day 90 (n=10) | Females Day 270 (n=10) |
| ------ | ------------------- | -------------------- | --------------------- | ---------------------- |
| OC     | 16.42 ± 3.27        | 13.38 ± 1.85\*       | 15.59 ± 2.74          | 13.19 ± 2.64 (ns)      |
| PICP   | 704.45 ± 397.5      | 340.6 ± 52.62\*      | 771.3 ± 435.81        | 336.4 ± 60.46\*        |
| ICTP   | 15.38 ± 1.72        | 12.74 ± 1.65\*       | 16.06 ± 1.28          | 13.4 ± 2.09\*          |

\*Significantly lower than Day 90 (P < 0.01). OC in females not significant. All markers fall within previously reported ranges for Thoroughbreds of this age (Price et al. 2001).

**Correlation of BMC with bone metabolism markers (Spearman rs, n=30 per comparison):**

| Timepoint | Marker pair  | n   | rs    | P    |
| --------- | ------------ | --- | ----- | ---- |
| Day 90    | BMC vs. OC   | 30  | 0.004 | 0.98 |
| Day 90    | BMC vs. PICP | 30  | −0.27 | 0.15 |
| Day 90    | BMC vs. ICTP | 30  | −0.15 | 0.44 |
| Day 270   | BMC vs. OC   | 30  | −0.03 | 0.89 |
| Day 270   | BMC vs. PICP | 30  | 0.22  | 0.25 |
| Day 270   | BMC vs. ICTP | 30  | 0.08  | 0.66 |

**No significant correlation between BMC and any bone metabolism marker at either timepoint.**

**Bone disorders (Table 3):**

| Disorder type | Days 0–90   | Days 90–180 | Days 180–270 | Total         |
| ------------- | ----------- | ----------- | ------------ | ------------- |
| Splint        | 3           | 2           | 3            | 8             |
| Bucked shin   | 2           | 2           | 0            | 4             |
| DOD           | 3           | 0           | 0            | 3             |
| Sesamoiditis  | 1           | 0           | 0            | 1             |
| **Total**     | **9 (56%)** | **4 (25%)** | **3 (19%)**  | **16 (100%)** |

Numbers in parentheses in original table indicate n with bone metabolism markers measured: (5), (3), (1) across periods.

Of 91 horses: 22 developed disorders requiring ≥1 week rest; of those, **16 had bone disorders specifically** (6 others had non-bone locomotive issues).

**Bone metabolism markers: injured vs. uninjured (Table 4):**

| Marker       | Day 90 Uninjured (n=30) | Day 90 Bone-injury (n=9) | Day 270 Uninjured (n=30) | Day 270 Bone-injury (n=9) |
| ------------ | ----------------------- | ------------------------ | ------------------------ | ------------------------- |
| OC (ng/ml)   | 16.15 ± 3.08            | 17.18 ± 4.04             | 13.31 ± 2.20\*\*         | 13.83 ± 2.81\*            |
| PICP (ng/ml) | 726.73 ± 434.14         | 599.22 ± 363.41          | 339.2 ± 54.34\*\*        | 333.22 ± 59.06\*          |
| ICTP (ng/ml) | 15.61 ± 1.60            | 15.64 ± 2.07             | 12.96 ± 1.80\*\*         | 13.38 ± 2.60\*            |

Both groups decreased over time (P < 0.01 or P < 0.05 vs. Day 90); **no significant difference between injured and uninjured horses at either timepoint for any marker.**

**Quarter Horse parallel:** Nielsen et al. 1997 reported 15/53 (28%) Quarter Horse bone disorders with 50% in the reduced-BMC period — consistent with the present results.

## Discussion & interpretation

The Days 0–90 BMC plateau (despite escalating training load) is interpreted as a reversal period: bone resorption is activated first at the onset of training (Wolff's law adaptation), and the simultaneous management change from pasture to stabling further reduces BMC. Net BMC is unchanged because resorption and formation effects approximately cancel. From Day 90 onward, formation dominates and BMC rises significantly to Day 180. The pattern mirrors Quarter Horse data (BMC decrease 60–120 days after training onset, Nielsen 1997/1998). The high bone-disorder incidence during Days 0–90 (56% of all bone disorders) — precisely when BMC is stagnating despite increasing mechanical load — is mechanistically consistent: bone is being remodelled and has temporarily reduced mineral density while demanding higher stress loads. The failure of bone metabolism markers to differentiate future-injured from non-injured horses is attributed to the 180-day measurement interval being too long to capture acute changes, the mildness of disorders (allowing short rest and training resumption), and the absence of severe fractures in this cohort. Authors recommend shorter measurement intervals in future to detect individual risk. Males developed higher BMC than females by Day 270, consistent with human bone biology (sex hormone-driven periosteal osteogenesis continues longer in males).

## Limitations

- NOTE article with modest n for some subanalyses (n = 9 bone-disorder horses for marker comparisons; n = 30 for BMC-marker correlations).
- 180-day marker measurement intervals too wide to capture acute bone metabolism dynamics preceding individual injuries.
- RBAE method has lower spatial resolution than DEXA or CT; total RBAE improves volumetric correction but is not a gold standard.
- Only left McIII measured; right-left asymmetry and other bone sites not assessed.
- 22-horse exclusion from BMC healthy analysis (injured horses) may create survivorship bias in BMC growth curves.
- One training centre; management protocol specific to Urakawa Blood Horse Training Center.

## Feature-engineering notes for the model

- `career_phase_risk_window` — binary: horse is in first 90 days of race training (approximate age 20–23 months) — estimated from `debut_date − first_training_start_date` or from age at first race start — expected effect: positive predictor of bone disorder / scratch risk during this window — data availability: debut date derivable from JRA race records; first training date requires farm records
- `horse_age_months_at_race` — continuous, from date of birth — derivable from JRA records — expected interaction: 20–23 months × training_load → elevated bone-disorder risk — data availability: fully available
- `sex` — male / female (geldings with males) — derivable from JRA records — expected effect: females may have lower BMC robustness at age 29 months (P < 0.05 sex difference at Day 270); possible interaction with career-phase bone risk — data availability: fully available
- `cumulative_race_starts_age_2` — number of starts as a 2-year-old — proxy for total training/racing load during the bone-consolidation period — derivable from JRA race records — expected effect: very high cumulative load as 2-year-old → elevated bone wear prior to 3-year-old season — data availability: fully available
- **Do NOT use** OC, PICP, or ICTP as individual bone disorder risk predictors — not significant in this study; not derivable from race records anyway.
- **Do NOT use** BMC alone as an injury predictor without temporal context — BMC stagnation matters relative to training load trajectory, not as an absolute value.

## Key references / follow-up leads

- Nielsen, B.D. et al. 1997. "Changes in the third metacarpal bone and frequency of bone injuries in young Quarter Horses during race training." _J. Equine Vet. Sci._ 17: 541–549. [key parallel dataset]
- Nielsen, B.D. et al. 1998. "Characterization of changes related to mineral balance and bone metabolism in the young racing Quarter Horse." _J. Equine Vet. Sci._ 18: 190–200.
- Jackson, B.F. et al. 2003. "Gender differences in bone turnover in 2-year-old Thoroughbreds." _Equine Vet. J._ 35: 702–706. [OC/ICTP higher in bucked-shin horses]
- Jackson, B.F. et al. 2005. "Biochemical markers of bone metabolism and risk of dorsal metacarpal disease in 2-year-old Thoroughbreds." _Equine Vet. J._ 37: 87–91.
- Kobayashi, M. et al. 2006. "Measurement of equine bone mineral content by radiographic absorptiometry using CR and ortho system." _J. Equine Sci._ 17: 105–112. [total RBAE method validation]
- Inoue, Y. et al. 2006. "Changes in serum biochemical markers of bone cell activity in growing Thoroughbred horses." _Asian-Aust. J. Anim. Sci._ 19: 1632–1637. [background for present marker values]
- Hoekstra, K.E. et al. 1999. "Comparison of bone mineral content and biochemical markers of bone metabolism in stall- vs. pasture-reared horses." _Equine Vet. J. Suppl._ 30: 601–604.
- Price, J.S. et al. 2001. "Biochemical markers of bone metabolism in growing thoroughbreds: a longitudinal study." _Res. Vet. Sci._ 71: 37–44.
