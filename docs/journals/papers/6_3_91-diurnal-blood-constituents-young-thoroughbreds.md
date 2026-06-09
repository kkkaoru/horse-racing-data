# Diurnal Variations of Blood Constituents in Young Thoroughbred Horses

## Metadata

| Field                          | Value                                                                                                |
| ------------------------------ | ---------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 6(3): 91–97, 1995                                                                     |
| docid                          | `6_3_91`                                                                                             |
| Article type                   | Original                                                                                             |
| Authors                        | Kazushige Yashiki, Ryo Kusunose, Shigeyoshi Takagi                                                   |
| Affiliations                   | Equine Research Institute, Japan Racing Association, 5–27–7 Tsurumaki, Setagaya-ku, Tokyo 154, Japan |
| Received / Accepted / Released | Submitted September 27, 1995; accepted November 24, 1995                                             |
| Keywords                       | diurnal variation, blood constituents, resting day, exercise day                                     |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/6/3/6_3_91/_pdf/-char/en                                    |

## Abstract (verbatim)

> To delineate points to be attended to in utilizing blood constituents as parameters for diagnostic examination of race horses, serial changes of blood constituents were followed on a day of resting and a day of physical exercise in five 2-year-old Thoroughbred horses (2 males and 3 females) currently under a course of training equivalent to that for young prospective race horses. Of 31 blood constituent parameters assessed which are commonly measured in the clinical setting, 27 parameters revealed significant within-day variations (P<0.05). Variations by 20% or more which are presumed to be of practical value from their nature were observed for 14 parameters: total leukocyte counts, neutrophil counts, lymphocyte counts, eosinophil counts, and serum levels of lactate dehydrogenase, glutamic dehydrogenase, total bilirubin, uric acid, glucose, triglyceride, potassium, inorganic phosphorus, triiodothyronine and thyroxine. Whilst the principal source of variations was physical exercise, there were several parameters which were inferred to be affected by such factors as feeding and daytime/night. Thus the present data indicated importance of due consideration of feeding conditions including quality and amount of physical exercise and of blood sampling time, for more accurate interpretation of data of hematologic and blood chemical tests.

## Relevance to finishing-position (着順) prediction

Feature family: **C (exercise-physiology/fitness)** and **A (injury/soundness/clinical diagnosis)**.

This paper establishes the magnitude and time-course of within-day variation for 31 blood parameters in 2-year-old Thoroughbreds under JRA training conditions. For any finishing-position model that ingests blood biomarker features (lactate, glucose, electrolytes, enzymes, thyroid hormones), the sampling time relative to feeding and the most recent exercise session is a critical hidden confounder. The study's key practical contribution is the classification of parameters by degree of variation: 14 parameters swing ≥20% within a single day — a magnitude that can easily exceed a true biological "fitness" signal unless sampling conditions are standardised.

The exercise-day vs. rest-day distinction is directly relevant to race-day physiology: horses in active training (equivalent to pre-race weeks) have a different post-exercise biochemical profile from resting horses. Parameters like WBC (+25%), neutrophils (+40%), LDH, GLDH, uric acid, and glucose are acutely responsive to exercise, making them noisy features unless the time between last exercise and blood draw is recorded. Parameters like serum triglyceride (+10–20%) and T4 are dominated by feeding rhythms, while lymphocyte counts show a primarily nocturnal rhythm unrelated to exercise — useful as a negative control for training effect.

For the existing pipeline, this paper argues that any planned use of blood markers as horse-level fitness features must be accompanied by: (1) time-of-draw standardisation (pre-exercise morning window), (2) knowledge of whether the sampling day was an exercise or rest day, and (3) caution that only parameters with variation <10% (ALP, TP, Alb, T-Cho, Na, Cl, Ca, RBC, PCV) approach the stability needed for cross-horse comparison without time-matching.

## Background & objective

Race horses are blood-sampled at irregular times — at the most convenient moment under a given feeding condition, or in emergencies. The paper notes that horses possess a long alimentary tract and intracaecal digestion that strongly perturbs many blood parameters. While individual parameter diurnal variation has been studied in humans, it was uncharacterised in young Thoroughbreds under active training. The objective was to document serial within-day variation of 31 clinically measured blood parameters on both a resting day and an exercise day, to guide valid interpretation of clinical laboratory results.

## Materials & methods

**Subjects:** Five healthy 2-year-old Thoroughbreds (2 colts, 3 fillies) on a training programme at the Race Horse School of the Japan Racing Association.

**Study design:** Cross-over within a single August 1992 experiment. Each horse was observed on one resting day (stall confinement) and one exercise day. The exercise consisted of approximately one hour of running at full speed along a 1,000-metre dirt course beginning at 10:00 a.m.; horses remained in stalls for the rest of the day.

**Sampling schedule:** Jugular venipuncture at 4-hour intervals over 24 hours, beginning at 7:30 a.m. (7 time-points: 7:30, 11:30, 15:30, 19:30, 23:30, 3:30, 7:30). Blood collected into EDTA dipotassium tubes and plain evacuated tubes. Feeding: four times daily at 6:30, 11:20, 16:20, and 19:20; the 11:20 ration was largest; the 19:20 ration was hay only.

**Instruments:** Automated blood cell counter (Erythrocytes/WBC/PCV); Tatai's eosinophil counting plate (eosinophils); differential counts for neutrophils and lymphocytes; Hitachi 736 autoanalyser for: CPK, γ-GTP, AST, GLDH, LDH, ChE, ALP, TP, Cre, Alb, TB, BUN, UA, Glu, T-Cho, TG, PL, Na, K, Cl, Ca, Fe, IP; enzyme immunoassay for T3 and T4.

**Statistical analysis:** GLM procedure of SAS; two-way analysis of variance; significance threshold P<0.05.

**Parameters:** 31 total — haematological (RBC, WBC, PCV, Eo, Ne, Ly) and biochemical (CPK, γ-GTP, AST, GLDH, LDH, ChE, ALP, TP, Cre, Alb, TB, BUN, UA, Glu, T-Cho, TG, PL, Na, K, Cl, Ca, Fe, IP, T3, T4).

## Results (detailed — reproduce ALL numbers)

**Overall significance (Table 1):** 27 of 31 parameters showed statistically significant within-day changes (P<0.05). The 4 non-significant parameters were RBC, PCV, CPK, and Fe (on both days) and Eo (rest day only), ChE (rest day), Ca (exercise day), T3 (rest day), TG (exercise day) — see table below.

**Table 1 — Parameters showing significant changes with time (P-values as printed):**

| Parameter | Rest day | Exercise day  |
| --------- | -------- | ------------- |
| RBC       | N.S.     | N.S.          |
| WBC       | N.S.     | \*\* (P<0.01) |
| PCV       | N.S.     | N.S.          |
| Eo        | N.S.     | \* (P<0.05)   |
| Ne        | N.S.     | \*\*          |
| Ly        | \*\*     | \*\*          |
| CPK       | N.S.     | N.S.          |
| γ-GTP     | N.S.     | \*\*          |
| AST       | N.S.     | \*\*          |
| GLDH      | \*\*     | \*\*          |
| LDH       | \*\*     | \*\*          |
| ChE       | \*\*     | N.S.          |
| ALP       | \*\*     | \*\*          |
| TP        | \*\*     | \*\*          |
| Cre       | \*\*     | \*\*          |
| Alb       | \*\*     | \*\*          |
| TB        | \*       | \*\*          |
| UA        | \*\*     | \*\*          |
| BUN       | \*       | \*\*          |
| Glu       | \*\*     | \*\*          |
| T-Cho     | \*\*     | \*\*          |
| TG        | \*\*     | N.S.          |
| PL        | \*\*     | \*\*          |
| Na        | \*\*     | \*\*          |
| Ca        | \*       | N.S.          |
| K         | \*\*     | \*\*          |
| Fe        | N.S.     | N.S.          |
| Cl        | N.S.     | \*\*          |
| IP        | \*\*     | \*\*          |
| T3        | N.S.     | \*\*          |
| T4        | \*\*     | \*\*          |

**Table 2 — Classification by magnitude of variation:**

| Variation class | Parameters                                                               |
| --------------- | ------------------------------------------------------------------------ |
| <10%            | ALP, TP, Alb, Cre*, T-Cho*, Na, Cl\*, Ca\*\*                             |
| 10% to <20%     | AST*, LDH*, γ-GTP*, ChE**, TB**, BUN, Cre**, T-Cho**, PL, T3*            |
| ≥20%            | WBC*, Ne*, Ly, Eo*, LDH\*\*, GLDH, UA, TB*, Glu, TG\*_, K, IP, T3_, T4\* |

\* Only on exercise day. \*\* Only on rest day.

**Haematological parameters (Fig. 1 — described from figure):**

- WBC (exercise day): pre-exercise ~9,000–10,000/μL → peak ~12,000/μL at ~15:30 (+~25–30%) → returns to ~9,000–10,000/μL by 23:30–7:30. On rest day: stable at ~9,000/μL throughout.
- Ne (exercise day): pre-exercise ~4,500–5,000/μL → peak ~7,000–7,500/μL at ~15:30 (+~40%) → declines. Rest day: stable ~4,000–4,500/μL.
- Ly: increased by 20–30% nocturnally (both rest and exercise days), peak ~3:00–5:00, nadir ~19:00–21:00; exercise had minimal additional effect.
- Eo (exercise day): nadir ~5 hours post-exercise, consistent with exercise-stress eosinopenia.

**Serum biochemical parameters (Fig. 2):**

- LDH (IU/L): exercise day: pre-exercise ~420 → relatively stable ~420–490 throughout (no acute peak). Rest day: substantially higher morning value ~600 (IU/L), declining to ~430 by 15:30 — rest day showed a greater change than exercise day. Authors suggest delayed peak as response to previous day's exercise or secondary kinetogenic haemolysis.
- GLDH (IU/L): exercise day: 07:30 baseline ~3 → peak ~15 at ~11:30 (approximately 4–5× increase) → declining to ~4–5 by 19:30. Rest day: stable and low ~3–4 throughout. GLDH liberated rapidly from skeletal muscle; peak 5 h post-exercise.
- UA (mg/dL): exercise day: baseline ~0.3–0.4 → sharp peak ~1.5 at 11:30 (approximately 4× increase) → rapidly returns to ~0.3 by 15:30 and stays there. Rest day: flat at ~0.3–0.4. UA is liberated from skeletal muscle and eliminated in urine rapidly.
- TB (mg/dL): both days show variation in the range ~1.2–2.2; exercise day shows lower values (nadir ~1.2–1.5) then rising gradually. Rest day shows higher more variable values.
- Glu (mg/dL): post-prandial morning peak (~105–110 mg/dL on exercise day at 7:30) → trough ~70–80 at 11:30 → partial recovery ~80–85 then nocturnal rise ~90–115 by 3:30–7:30. Exercise day and rest day broadly similar in pattern.
- TG (mg/dL): rest day shows pronounced postprandial increase, sustained >10 hours after feeding (~25 → peak ~35 → maintained elevated). Exercise day: no such sustained increase (TG was not significant on exercise day) — suggesting enhanced lipid metabolism during exercise.

**Electrolytes/minerals (Fig. 3):**

- K (mmol/L): highly variable on both days, range ~3.0–4.3. Peak observed at night ~23:30–3:30. Large within-day swings of ≥20%.
- IP (mg/dL): range ~3.0–5.5 on rest day (with nocturnal rise). Exercise day: nadir at ~11:30 (~3.0) then gradual rise to ~4.5 by night.

**Thyroid hormones (Fig. 3):**

- T3 (ng/dL): exercise day only significant; peak ~1.4 at 11:30, baseline ~1.0–1.1. Rest day: flat ~1.0–1.2 (N.S.).
- T4 (μg/dL): rest day: ~2.5–2.7 (stable, significant). Exercise day: lower values starting ~1.9 at 7:30, rising to ~2.2 by 15:30 then declining. Both days show >20% variation.

**Parameters with <10% variation (stable for clinical use without time-matching):**
ALP, TP, Alb, T-Cho (rest day), Na, Cl (rest day), Ca (rest day) — these are the safest parameters for cross-horse comparisons without strict sampling-time control.

**Summary of practically significant (≥20%) diurnal fluctuators:**
WBC, neutrophils, lymphocytes, eosinophils, LDH, GLDH, total bilirubin, uric acid, glucose, triglyceride, potassium, inorganic phosphorus, T3, T4 — 14 parameters in total.

## Discussion & interpretation

The principal driver of the ≥20% variations is physical exercise, which acutely elevates WBC (+25%), neutrophils (+40%), GLDH (~4–5×), and UA (~4×). The post-exercise WBC/neutrophil elevation is attributed to three mechanisms: (1) drawing of leukocytes from bone marrow/spleen/lungs due to increased circulating blood volume, (2) shift of leukocytes from the marginal pool driven by plasma catecholamine elevation, and (3) stimulation of neutrophil production by cytokines such as interleukin-1 (mechanism 3 considered most accountable given the response timing). Eosinopenia is a well-established consequence of exercise stress.

GLDH and uric acid are liberated from skeletal muscle rapidly after exercise [ref 4, 10]; GLDH remains in bloodstream longer than uric acid, which is eliminated in urine quickly. The authors note that CPK, classically a sensitive marker of muscle damage, showed NO appreciable change even on the exercise day — suggesting the exercise was not vigorous enough to cause acute muscle damage at this training stage.

The higher rest-day LDH (greater change than exercise day) may reflect a delayed LDH peak as a response to previous exercise sessions, or secondary kinetogenic haemolysis (supported by elevated total bilirubin), warranting further investigation.

Lymphocyte nocturnal increase (20–30%, both days, unrelated to exercise) reflects the known maximal response of cellular immunity in the inactive phase, as also described in humans.

Rest-day serum triglyceride elevation after feeding (sustained >10 hours) contrasts with its absence on the exercise day, indicating enhanced energy metabolism during exercise consumes circulating lipids. This finding emphasises that not only feeding time but also the nature of exercise that day must be considered when interpreting TG data.

T3 and T4 varied ≥20% on exercise day only (T3) and on both days (T4); the variation is described as modest in magnitude but present and statistically significant.

The authors conclude that physiological parameters requiring careful attention during clinical laboratory assessment include: post-exercise total leukocyte counts, neutrophil counts, serum GLDH, serum uric acid, morning postprandial serum glucose, and rest-day serum LDH and triglycerides.

## Limitations

- Very small sample: n=5 horses only, all 2-year-olds at a single training school in August 1992. Seasonal and developmental (age) effects cannot be separated.
- Single time-point for the exercise day (one bout of exercise at 10:00); exercise intensity relative to race intensity was likely lower (no significant CPK increase).
- No individual-horse data shown, only means; within-animal CV could be large.
- The 4-hour sampling interval means acute responses (peaking within 1–2 hours) may be undersampled.
- Results may not generalise to older horses, high-intensity race-day conditions, or different exercise modalities.

## Feature-engineering notes for the model

- `wbc_sampling_hours_post_exercise` — time elapsed since last exercise when WBC was measured — source: workout logs × draw time — expected effect: WBC inflated by ~25–40% within 5 h post-exercise; use only as covariate, not raw feature
- `neutrophil_count_normalised` — absolute neutrophil count adjusted for time-of-draw and exercise-day flag — source: clinical blood records — expected effect: positive for immune challenge; large diurnal confound makes raw value unreliable
- `ldh_rest_day_morning` — LDH measured on a rest day, morning sample — source: clinical chemistry records — expected effect: elevated LDH on rest day may indicate residual muscle damage from prior heavy training; more informative than post-exercise LDH
- `gldh_post_exercise_peak` — GLDH ~11:30 (1.5 h post-exercise) — source: clinical chemistry — expected effect: acute muscle stress biomarker; ~4–5× elevation vs. rest; do NOT compare across exercise vs. non-exercise sampling conditions without adjustment
- `uric_acid_post_exercise` — UA at ~11:30 on exercise day — source: chemistry — expected effect: ~4× spike, normalises within 4 h; sampling-time confound is severe; not useful cross-horse without time-matching
- `glucose_postprandial` — Glu at 7:30 (post first feeding) — source: chemistry — expected effect: postprandial peak ~105–110 mg/dL; trough at 11:30 ~70 mg/dL; feeding time must be known
- `triglyceride_rest_day` — TG on rest day, post-afternoon-feeding — source: chemistry — expected effect: sustained rise >10 h post-feed on rest day only; exercise day flat; useful for metabolic status if day-type known
- `potassium_nocturnal` — K peak at 23:30–3:30 (≥20% swing) — source: electrolytes — expected effect: muscle-fatigue relevant; nocturnal peak is physiological rhythm not pathology; do NOT use single sample without time-of-draw
- `t3_t4_morning_baseline` — thyroid hormones from pre-exercise morning sample only — source: endocrine panel — expected effect: T4 varies ≥20% within day; valid only from standardised morning baseline
- **DO NOT USE** raw WBC, Ne, LDH, GLDH, UA, Glu, TG, K, IP, T3, T4 as features without a paired sampling-time covariate or strict standardisation window

## Key references / follow-up leads

- Genba, A. 1986. Sampling time and blood constituents. _J. M. Tec._ 30: 806–809 (Japanese) — foundational reference on sampling time effects
- Keenan, D.M. 1979. Changes of blood metabolites in horses after racing, with particular reference to uric acid. _Aust. Vet. J._ 55: 54–57 — post-race UA dynamics
- Murakami, M. et al. 1976. Swimming exercises in horses. _Exp. Rep. Equine Hlth Lab._ 13: 27–49 — eosinopenia with exercise stress in horses
- Yashiki, K., and Takagi, S. 1991. Post-exercise changes in blood chemical features as assessed using automated biochemical analyzer. _Equine Science_ 28: 51–58 (Japanese) — direct precursor study by same authors
- Yamaoka, S. et al. 1978. Clinical and enzymological findings of tying-up syndrome in thoroughbred racehorses in Japan. _Exp. Rep. Equine Hlth Lab._ 15: 62–77 — context for LDH/GLDH in muscle injury
