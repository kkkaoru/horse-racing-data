# Electrocardiogram from the Longest-Living Racehorse in Japan: Legendary Racehorse Shinzan

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                                                                                                   |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 8(2): 39–42, 1997                                                                                                                                                                                                                                                                                                        |
| docid                          | `8_2_39`                                                                                                                                                                                                                                                                                                                                |
| Article type                   | Note                                                                                                                                                                                                                                                                                                                                    |
| Authors                        | Masayoshi Kuwahara, Atsushi Hiraga, Tsurayuki Nishimura, Hirokazu Tsubone, Shigeru Sugano                                                                                                                                                                                                                                               |
| Affiliations                   | Department of Comparative Pathophysiology, Graduate School of Agriculture and Life Sciences, The University of Tokyo, 1-1-1 Yayoi, Bunkyo-ku, Tokyo 113; Equine Research Institute, Japan Racing Association, 321-4 Togami-cho, Utsunomiya, Tochigi 320; Hidaka Nosai, 2-5-5 Urakawa-cho, Sakaimachi Higashi, Urakawa-gun, Hokkaido 057 |
| Received / Accepted / Released | Submitted February 28, 1997; accepted April 8, 1997                                                                                                                                                                                                                                                                                     |
| Keywords                       | atrial fibrillation, electrocardiogram, horses                                                                                                                                                                                                                                                                                          |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/8/2/8_2_39/_pdf/-char/en                                                                                                                                                                                                                                                                       |

## Abstract (verbatim)

> The paper describes the ECG obtained from the longest-living racehorse in Japan. The horse named Shinzan became the first postwar Japanese triple crown champion. Although the horse died of heart failure on July 13, 1996 (35 years and 3 months old), we had recorded ECGs from the horse at clinical examination from time to time since December 30, 1994. The first ECG was recorded from the horse on December 30, 1994 and the diagnosis was atrial fibrillation. Atrial fibrillation was still present in the final ECG recording obtained with a Holter ECG recorder on May 17, 1996. It was therefore considered that the atrial fibrillation was of a persistent and not a paroxysmal type.

## Relevance to finishing-position (着順) prediction

Feature family **B (cardiac arrhythmia as performance risk)**. Atrial fibrillation (AF) is the most common arrhythmia affecting racing performance in horses. This case study on Shinzan serves primarily as a clinical narrative introducing AF in horses for a Japanese audience, but includes a systematic literature review of AF epidemiology, clinical features, and racing prognosis that provides key inputs for model feature design.

The most directly relevant finding from the reviewed literature: horses with **paroxysmal AF** (spontaneous reversion to sinus rhythm within 24 h) achieve similar race results after return to racing compared with pre-AF races (Deem & Fregin 1982, 106 cases; Stewart et al. 1990). This establishes a critical distinction: paroxysmal AF is not a permanent performance impairment, but persistent AF (as in Shinzan) is associated with declining cardiac function. For a finishing-position model, the relevant features are: (1) history of AF (any type), (2) distinction between paroxysmal and persistent, and (3) age and sex priors.

The literature review documents that AF incidence is highest in 3–4-year-olds and in male horses (Reef et al. 1988), a risk window that coincides with JRA Classic generation races. This intersection of high-stakes races and peak AF incidence risk means that a `history_af` feature combined with `age_years` and `sex` interactions has real predictive value for unexpected underperformance in young male horses.

Paroxysmal AF during a race may be missed because the horse returns to sinus rhythm within 24 h — the true incidence is likely underestimated in historical records. This creates a systematic false-negative bias in any historical AF-flag feature derived from clinical records.

## Background & objective

Shinzan (シンザン) was the first postwar Japanese Triple Crown champion (1964) and, at 35 years 3 months, the longest-living racehorse in Japan at his death. The paper reports ECG recordings taken from December 1994 to May 1996 documenting persistent AF. The clinical description is accompanied by a review of the veterinary literature on AF in horses, addressing incidence, predisposing factors, types (paroxysmal vs. persistent), treatment (quinidine), and prognosis for return to racing.

## Materials & methods

**Case:** Thoroughbred stallion Shinzan; born ~April 1961; died July 13, 1996 (age 35 years 3 months) at Tanigawa Ranch, Hokkaido, of heart failure.

**Clinical history:** Serious physical problems first noted February 16, 1994; irregular cardiac rhythm detected by auscultation February 17, 1994. ECG recording unavailable at that time. No acute deterioration signs (no pulmonary or peripheral oedema) during the monitoring period.

**ECG recordings:**

1. Conventional ECG recorder (apex-base lead): December 30, 1994 — first confirmed ECG, diagnosis AF; February 15, 1995 — detailed AF ECG recorded (Fig. 1)
2. Holter ECG recorder (apex-base lead): May 17, 1996 — final recording (Fig. 2, compressed 12-min and extended views)

**Measurements:**

- Ventricular rate (beats/min): averaged over defined recording periods
- RR interval range (msec): longest and shortest RR intervals
- QRS duration range (msec)
- Fibrillatory (f) wave rate (per min)

**No formal statistical methods** (n = 1 case).

## Results (detailed — reproduce ALL numbers)

### ECG findings from Shinzan

**February 15, 1995 (conventional ECG):**
| Parameter | Value |
|-----------|-------|
| Cardiac rhythm | Atrial fibrillation |
| Ventricular rate | ~42 beats/min (averaged over 2 min) |
| Longest RR interval | 2,840 msec |
| Shortest RR interval | 840 msec |
| QRS duration range | 100–140 msec |
| f wave rate | ~300/min |
| f wave character | Very coarse |

**May 17, 1996 (Holter ECG, 14-hr recording):**
| Parameter | Value |
|-----------|-------|
| Cardiac rhythm | Atrial fibrillation (persistent) |
| Ventricular rate | ~48 beats/min (averaged over 14 hr) |
| Longest RR interval | 4,120 msec |
| Shortest RR interval | 620 msec |
| QRS duration range | 90–160 msec |
| f wave rate | ~270/min |

Note: Ventricular rate increased slightly (42 → 48 beats/min) between February 1995 and May 1996, consistent with progression. The range of f wave rates reported in literature is 270–500/min; Shinzan's 270–300/min was on the low end.

**AF classification:** Persistent type (not paroxysmal) — AF present at every ECG recording over the 18-month monitoring period, never reverted to sinus rhythm spontaneously or with treatment (no treatment was given, as the horse was at rest and in no acute distress).

### Literature review findings reproduced from the paper

**Predisposing factors for AF in horses:**

- Large atrial myocardial mass (large heart)
- High vagal tone
- Atrial premature contractions (initiating trigger)
- Rapid atrial stimulation capable of initiating fibrillation
- Reentry circuits through circuits not requiring anatomical obstacles (secondary wavelets of depolarisation in repolarised myocardium)
- Focal atrial myocardial fibrosis, microvascular alterations, cardiac nerve abnormalities (Else & Holmes 1971: histopathological findings in 39 AF horses; none < 5 years old; 53% > 20 years old)

**Epidemiology from reviewed sources:**

- AF is "the most common arrhythmia affecting performance in horses" (multiple citations).
- Amada & Kurita (1975): 5 cases paroxysmal AF in racehorses; all reverted spontaneously to sinus rhythm.
- Paroxysmal cases typically revert within **24 h**; if horse examined 2–3 days later, found in sinus rhythm — true incidence underestimated.
- Reef et al. (1988): AF most frequent in **3- and 4-year-olds** and in **male horses**; though authors note this may reflect case loads at referral centres.
- Holmes et al. (1969): AF most common in **older draft horses**.
- Deem & Fregin (1982, 106 cases): paroxysmal-AF horses that returned to racing achieved **similar race results** to pre-AF.
- Stewart et al. (1990): idiopathic AF in a champion Standardbred — detailed case with return to racing.

**Quinidine treatment:**

- Quinidine sulfate: most frequently used; increases effective refractory period in atrial and ventricular myocardium; increases fibrillation threshold.
- Horses with **longer duration** AF, other cardiac disease, or structural changes: less likely to convert; require larger total quinidine doses; higher recurrence rate.
- High incidence of quinidine toxicosis: nasal mucosal oedema, epistaxis, dyspnoea, diarrhoea, laminitis.
- Reef et al. (1995) new treatment perspectives: alternative techniques available.
- Treatment outcome (Reef et al. 1988 factors): longer suspected AF duration and concurrent cardiac disease reduce conversion probability.

## Discussion & interpretation

Shinzan's persistent AF was likely age-related structural (myocardial fibrosis) rather than vagally-mediated functional AF typical of younger racehorses. The 35-year-old age context makes this case an extreme outlier unsuitable for direct generalisation to racing-age horses. However, the embedded literature review is the paper's primary contribution: it documents that AF is (1) the most performance-relevant equine arrhythmia, (2) predominantly paroxysmal and recoverable in young horses, (3) associated with male sex and age 3–4 in the racing context, (4) clinically underdetected due to spontaneous reversion, and (5) treatable with quinidine but with significant toxicity risks.

The authors note they provided no treatment to Shinzan because the horse was elderly, at rest, and showed no acute distress — a clinically reasonable decision. The cause of death (heart failure) could not definitively be linked to the AF without histopathological studies that were not performed.

## Limitations

- n = 1 case; the Shinzan ECG data have no generalisability for inferring racing-age AF features.
- Monitoring began only in December 1994 after problem onset in February 1994 — the exact onset and timeline of AF are uncertain.
- Histopathology not performed; no definitive mechanism for AF or cause of death.
- Holter monitor limited to apex-base lead (single lead); precise QRS morphology assessment limited.
- The literature review draws on studies from multiple countries and horse breeds (Thoroughbred, Standardbred, draft horses) with different AF epidemiology.
- Reef et al. (1988) data on age/sex distribution may reflect referral bias (not population prevalence).

## Feature-engineering notes for the model

- `history_af_any` — binary (1 = any AF ever documented, 0 = no record) — JRA/NAR veterinary records and scratch records — positive association with unexpected underperformance; paroxysmal AF during races causes acute performance drop; persistent AF indicates structural cardiac disease
- `history_af_paroxysmal` — binary (1 = paroxysmal AF, i.e., spontaneous reversion documented; 0 = no paroxysmal AF record) — clinical withdrawal codes — lower long-term performance risk than persistent AF; paroxysmal horses can return to similar race results (Deem & Fregin 1982); risk window ~24–72 h post-episode
- `history_af_persistent` — binary (1 = persistent AF documented, i.e., no reversion, or quinidine treatment required) — clinical records — higher performance impact; associated with structural disease; effectively disqualifying for future racing
- `days_since_af_episode` — integer (days since last documented AF episode) — derived from clinical timestamps — performance risk decreases as days from episode increase; flag as high-risk within ~7 days
- `quinidine_treatment_record` — binary or count — clinical/withdrawal records — horses that required quinidine had longer/more complex AF; higher recurrence risk; potential drug interaction features
- `sex` — male/gelding vs. female — race entry records — male predilection for AF at ages 3–4 (Reef et al. 1988); sex × age interaction warranted
- `age_years` — numeric — race records — peak AF incidence window 3–4 years for racehorses; age × sex × `history_af` interaction
- **Underdetection caveat:** Paroxysmal AF reverts within 24 h; horses examined ≥2–3 days post-race will appear normal. Any AF-flag feature derived from clinical records will have **systematic false-negative bias** — actual AF prevalence in race horses is likely higher than documented. This weakens the feature's recall but does not invalidate its precision for identified cases.
- **JRA data linkage:** Race withdrawal reasons (病気/cardiac) and veterinary examination records post-race could provide the AF signal. Sudden unexpected poor performance in an otherwise consistent horse — especially a 3–4-year-old male — may warrant retroactive AF hypothesis flagging.

## Key references / follow-up leads

- Deem, D.A. and Fregin, G.F. 1982. Atrial fibrillation in horses: a review of 106 clinical cases, with consideration of prevalence, clinical signs, and prognosis. _J. Am. Vet. Med. Assoc._ 180: 261–265.
- Reef, V.B., Levitan, C.W. and Spencer, P.A. 1988. Factors affecting prognosis and conversion in equine atrial fibrillation. _J. Vet. Intern. Med._ 2: 1–6.
- Reef, V.B., Reimer, J.M. and Spencer, P.A. 1995. Treatment of atrial fibrillation in horses: new perspectives. _J. Vet. Intern. Med._ 9: 57–67.
- Amada, A. and Kurita, H. 1975. Five cases of paroxysmal atrial fibrillation in the racehorse. _Exp. Rep. Equine Health Lab._ 12: 89–100.
- Amada, A. and Kurita, H. 1978. Treatment of atrial fibrillation with quinidine sulfate in the racehorse. _Exp. Rep. Equine Health Lab._ 15: 47–61.
- Else, R.W. and Holmes, J.R. 1971. Pathological changes in atrial fibrillation in the horse. _Equine Vet. J._ 3: 56–64.
- Stewart, G.A., Fulton, L.J. and McKellar, C.D. 1990. Idiopathic atrial fibrillation in a champion Standardbred racehorse. _Aust. Vet. J._ 67: 187–191.
- Holmes, J.R., Drake, P.G.G. and Else, R.W. 1969. Atrial fibrillation in the horse. _Equine Vet. J._ 1: 211–222.
- Yamaya, Y., Kubo, K. and Amada, A. 1997. Relationship between atrio-ventricular conduction and hemodynamics during atrial pacing in horses. _J. Equine Sci._ 8: 35–38. [companion paper, docid 8_2_35]
