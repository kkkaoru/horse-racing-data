# Surgical corrections and postsurgical complications of epiglottic entrapment in Thoroughbreds: 12 cases (2009–2015)

## Metadata

| Field                          | Value                                                                                                                                                                     |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 30(2): 41–45, 2019                                                                                                                                         |
| docid                          | `30_1905`                                                                                                                                                                 |
| Article type                   | Note                                                                                                                                                                      |
| Authors                        | Sang-Kyu Lee, Inhyung Lee                                                                                                                                                 |
| Affiliations                   | College of Veterinary Medicine, Seoul National University, Seoul 08826, Republic of Korea; KRA Equine Hospital, Korea Racing Authority, Gwacheon 13822, Republic of Korea |
| Received / Accepted / Released | March 12, 2019 / May 9, 2019 / 2019                                                                                                                                       |
| Keywords                       | axial division, epiglottic entrapment, shielded hook bistoury, Thoroughbred                                                                                               |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/30/2/30_1905/_pdf/-char/en                                                                                                       |

## Abstract (verbatim)

> Twelve Thoroughbred racehorses were diagnosed as epiglottic entrapment at the Korea Racing Authority equine hospital. Four different surgical correction techniques were used to treat epiglottic entrapment: the transnasal hook bistoury, transoral unshielded hook bistoury, transnasal shielded hook bistoury, and transendoscopic laser techniques. Eleven cases were surgically resolved eventually, with one case of recurrence. Five complications related to surgical correction occurred: a severe nasal passage laceration and bleeding (n=1), epiglottic laceration (n=1), epiglottis tip burns (n=2), and moderate nasal passage laceration (n=1). Intraoperative complications occurred in approximately 41.7% of cases. Thus, the possibility of surgical complication should be considered thoroughly when choosing a surgical technique for correction of epiglottic entrapment.

## Relevance to finishing-position (着順) prediction

Feature family: **B — respiratory disorder**. Epiglottic entrapment is an upper airway disorder in which an oedematous or hypertrophied aryepiglottic fold envelops the epiglottis dorsally, causing exercise-induced abnormal respiratory noise and exercise intolerance. All 12 affected horses in this study showed reduced race performance before surgery (mean rank rate 0.8, range 0.3–1.0, where rank/runners closer to 1.0 = worse), and all 11 surgically resolved horses improved post-surgery (mean rank rate 0.6, range 0.2–1.0).

The key modelling implications are:

1. **Performance gap + young Thoroughbred + pre-gap poor performance → respiratory surgery signal.** Surgery-to-first-race intervals ranged from 69–115 days by technique. A prolonged absence (≥69 days) in a 2–6 year-old Thoroughbred whose last few races showed declining performance relative to ability is consistent with respiratory (or musculoskeletal) intervention and should trigger a "post-surgery bounce" prior.
2. **Post-surgery improvement magnitude:** mean rank rate improved from 0.8 to 0.6 (a 0.2 improvement on a 0–1 scale). If race rank rate is defined as finishing position / field size, this corresponds to, e.g., going from ~8th/10 to ~6th/10 — a meaningful but not dramatic improvement. The true ability ceiling is only revealed in subsequent races, not the first post-gap race.
3. **Incidence rate:** 1.2% (12/975 endoscoped horses) at a racing authority facility is a lower bound; not all horses undergo endoscopy. Combined with other upper airway disorders, respiratory dysfunction affects a non-trivial proportion of Thoroughbreds in active training.

This feature family interacts with `days_since_last_race`, `age`, and pre-gap pace/ranking trajectory features.

## Background & objective

Epiglottic entrapment in horses was first described in 1978. It occurs when the normal vascular pattern and notched edge of the epiglottis cannot be visualised due to a retroverted aryepiglottic fold enveloping the epiglottis. It can be persistent or intermittent and primarily affects athletic horses. Presentation: abnormal respiratory noise and exercise intolerance. Multiple surgical techniques exist with varying recurrence rates: transnasal axial division with unshielded/shielded bistoury (5–15%), transoral axial division (10%), laser (4%), transendoscopic electrosurgical (40%), laryngotomy (36%). The shielded bistoury was introduced in 2011; this study is among the first to compare all four low-recurrence-rate techniques including the shielded instrument.

## Materials & methods

**Setting:** KRA Equine Hospital, Korea Racing Authority, Gwacheon, South Korea.

**Population:** 975 horses underwent respiratory endoscopic examination between April 2009 and April 2015; 12 were diagnosed with epiglottic entrapment (incidence 1.2%).

**Cases:** 12 Thoroughbred racehorses; 8 male (stallions and geldings), 4 female; age 2–6 years (mean 3.8 years); ulcerations of entrapped mucous membrane observed in 5/12 (41.7%) horses.

**Diagnostic criteria:** at least two presurgical resting endoscopic examinations of the upper respiratory tract.

**Surgical techniques used:**

- Transnasal hook bistoury (TnHB): 2 cases (including 1 uncompleted due to severe nasal laceration)
- Transoral hook bistoury (ToHB): 1 case (general anaesthesia, lateral recumbency, ketamine 2.20 mg/kg)
- Transnasal shielded hook bistoury (TSHB): 4 cases (including 1 re-surgery), plus 1 initial TSHB that was uncompleted and converted to TL
- Transendoscopic laser (TL): 5 cases (including 1 re-surgery and 1 converted from failed TSHB); Ceralas D25 diode laser (Biolitec AG), 15–18 W contact fashion

**Standing sedation for most cases:** detomidine 0.02 mg/kg + butorphanol 0.10 mg/kg; topical 2% lidocaine 50 ml.

**Postoperative care:** procaine penicillin G 20,000 IU/kg IM + flunixin meglumine 1.10 mg/kg IV for 5 days; endoscopic re-examination at 2 weeks post-op.

**Performance data:** elapsed days from surgery to first race; rank in last race before surgery and first race after surgery. Source: KRA online database (studbook.kra.co.kr). Race rank rate = finishing rank / number of runners.

## Results (detailed — reproduce ALL numbers)

### Complications (Table 1)

| Technique | n (total surgical trials)                                                                                                                               | Complication                                                                                                             |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| TnHB      | 2                                                                                                                                                       | 1 severe nasal passage laceration + bleeding (RBC 4.1×10⁶/µl, PCV 16%); surgery not completed; owner declined re-surgery |
| ToHB      | 1                                                                                                                                                       | Partial epiglottis laceration (swallowing during axial division); healed by secondary healing ~2 months                  |
| TSHB      | 4 (including 1 re-entrapment requiring re-surgery with TSHB again) + 1 initial attempt converted to TL (moderate nasal laceration from unclosed shield) | 1 re-entrapment post-TSHB; 1 moderate nasal laceration                                                                   |
| TL        | 5 (including 1 re-surgery from failed TSHB)                                                                                                             | 2 epiglottis tip burns (2/5, 40%); 1 additional epiglottis tip burn in the converted TSHB→TL case                        |

- Overall intraoperative complication rate: 41.7% (5/12 horses)
- Major complications (severe nasal laceration, partial epiglottis laceration): 2/12 (16.7%)
- Minor complications (moderate nasal laceration, epiglottis tip burns): 3/12 total involving this category (2 burns + 1 moderate laceration)
- Re-entrapment rate: 9.1% (1/11 completed surgeries)
- Surgical resolution eventually achieved in 11/12 horses

### Time from surgery to first race start (Table 2)

| Technique                                | n (horses who completed surgery + raced) | Mean days to 1st race | Minimum | Maximum |
| ---------------------------------------- | ---------------------------------------- | --------------------- | ------- | ------- |
| Transoral hook bistoury (ToHB)           | 1                                        | 115 days              | 115     | 115     |
| Transnasal hook bistoury (TnHB)          | 1                                        | 74 days               | 74      | 74      |
| Transnasal shielded hook bistoury (TSHB) | 4                                        | 69 days               | 52      | 89      |
| Transendoscopic laser (TL)               | 5                                        | 109 days              | 65      | 189\*   |

\*Note: Table in paper shows maximum 189 days for TL, minimum 65 days (values appear to be swapped in the printed table; the minimum of 65 and maximum of 189 appear for TL).

### Pre- and post-surgery race rank rates (Table 3)

| Measure                       | Last race pre-surgery | First race post-surgery |
| ----------------------------- | --------------------- | ----------------------- |
| n horses                      | 11                    | 11                      |
| Mean rank rate (rank/runners) | 0.8                   | 0.6                     |
| Best (lowest) rank rate       | 0.3                   | 0.2                     |
| Worst (highest) rank rate     | 1.0                   | 1.0                     |

- Performance improved in all 11 surgically resolved horses (100%).
- 1 horse excluded from performance analysis (uncompleted TnHB, owner refused re-surgery).

## Discussion & interpretation

The mean rank rate improvement from 0.8 to 0.6 post-surgery confirms that surgical correction of epiglottic entrapment restores at least partial aerobic capacity and race competitiveness. The transnasal shielded bistoury had the shortest mean time to first race (69 days) and is recommended as the preferred technique when available. The laser technique required 109 days mean due to granulation tissue formation from thermal damage. The 41.7% intraoperative complication rate across all techniques is high and underscores the surgical difficulty. The recommended postoperative rest of 4–6 weeks appears adequate to prevent re-entrapment in most cases, given that the minimum rest was 52 days before return to racing.

The authors note that DDSP (dorsal displacement of soft palate) is a known sequela to epiglottic entrapment correction (up to 20% of cases by some reports), though no DDSP occurred in this study.

## Limitations

- n=12; single institution (KRA, Korea). Korean Racing Authority Thoroughbreds may differ from JRA horses in training regime and racing schedule.
- No control group; pre-surgery rank rates reflect the horses' condition at the time of diagnosis, not matched historical performance.
- Race record analysis limited: only last pre-surgery race and first post-surgery race were examined; the trajectory (number of races before and after) is unknown.
- Many factors influence race ranking beyond respiratory function; the authors explicitly caution against overgeneralisation.
- Selection bias: only endoscoped horses are in the sample; the true prevalence of epiglottic entrapment may differ from the 1.2% figure.
- Population: Korean Thoroughbreds; applicability to JRA and NAR horses requires separate validation.

## Feature-engineering notes for the model

- `days_since_last_race` — gap ≥69 days in a horse aged 2–6 is consistent with respiratory or musculoskeletal surgery. Informative prior for post-surgery performance bounce. Source: race database date fields.
- `pre_gap_rank_rate_mean` — mean rank/runners over the 3 races before the gap. Values approaching 1.0 (last place) combined with long gap signal likely intervention. Source: computed from race records.
- `post_gap_race_number` — first post-gap race has mean rank rate 0.6 vs. pre-gap 0.8; subsequent races likely better as horse adapts. A "recovery trajectory" feature (e.g., post_gap_race_1, post_gap_race_2) captures this.
- `horse_age_at_gap` — respiratory surgery most common in 2–6 year-olds; interaction with gap feature.
- `incidence_rate_respiratory` — 1.2% base rate per endoscoped horse at training age; use as a Bayesian prior weight when constructing injury probability features.
- **Interaction:** `days_since_last_race ≥69 × age_2_6 × recent_rank_rate_poor` — composite respiratory-surgery-indicator feature.
- **Caution:** this feature cannot distinguish respiratory surgery from musculoskeletal surgery using race records alone; both produce similar gap-performance patterns. The signal is nonetheless useful as a combined "post-intervention bounce" prior.

## Key references / follow-up leads

- Kasashima Y et al. 2004. Prevalence of superficial digital flexor tendonitis and suspensory desmitis in Japanese Thoroughbred flat racehorses in 1999. Equine Vet. J. 36: 346–350. — JRA Thoroughbred musculoskeletal injury prevalence.
- Lacourt M, Marcoux M. 2011. Treatment of epiglottic entrapment by transnasal axial division in standing sedated horses using a shielded hook bistoury. Vet. Surg. 40: 299–304. — original description of the shielded bistoury technique.
- Ahern BJ, Parente EJ. 2008. Surgical complications of the equine upper respiratory tract. Vet. Clin. North Am. Equine Pract. 24: 465–484. — complication rates for all techniques.
- Sweeney CR, Maxson AD, Soma LR. 1991. Endoscopic findings in the upper respiratory tract of 678 Thoroughbred racehorses. J. Am. Vet. Med. Assoc. 198: 1037–1038. — prevalence study in Thoroughbreds.
- Raphel CF. 1982. Endoscopic findings in the upper respiratory tract of 479 horses. J. Am. Vet. Med. Assoc. 181: 470–473. — earlier prevalence study for comparison.
