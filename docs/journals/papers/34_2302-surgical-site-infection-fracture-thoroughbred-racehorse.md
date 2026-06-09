# Incidence of Surgical Site Infection after Internal Fixation of the First Phalangeal Bone and the Third Metacarpal/Metatarsal Bone Fractures in Thoroughbred Racehorses

## Metadata

| Field                          | Value                                                                                                                                                                                                                        |
| ------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **34**(3): 61–66, 2023                                                                                                                                                                                        |
| docid                          | `34_2302`                                                                                                                                                                                                                    |
| Article type                   | Full Paper                                                                                                                                                                                                                   |
| Authors                        | Hiroshi MITA, Taisuke KURODA, Hidekazu NIWA, Norihisa TAMURA, Kentaro FUKUDA, Minoru OHTA                                                                                                                                    |
| Affiliations                   | 1 Clinical Veterinary Medicine Division, Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan; 2 Microbiology Division, Equine Research Institute, Japan Racing Association, Tochigi 329-0412, Japan |
| Received / Accepted / Released | 2023-01-06 / 2023-04-19 / —                                                                                                                                                                                                  |
| Keywords                       | implant surgery, simple fracture, surgical site infection                                                                                                                                                                    |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/34/3/34_2302/_pdf/-char/en                                                                                                                                                          |

## Abstract (verbatim)

> Surgical site infection (SSI) is one of the major complications of equine fracture surgery. The purpose of this study was to investigate the incidence of and risk factors for SSI after internal fixation of the first phalangeal bone (P1) and the third metacarpal/metatarsal bone (MC3/MT3) fractures in Thoroughbred racehorses. Between 2011 and 2020, 451 cases underwent surgery with screws or a locking compression plate (LCP) for sagittal fractures of P1 or condylar fractures of MC3/MT3. Overall, 2.9% (13/451) of the cases developed an SSI. The incidence was significantly higher in plate fixation (21.4%) than in screw fixation (2.3%). There was no significant association with other variables, such as sex, age, number of screws, experience of surgeon, or prophylactic antimicrobials. The median duration of hospitalization for screw fixation was 14 days without an SSI and 20 days with an SSI, and those for plate fixation were 26 and 25–88 days, respectively, indicating that the development of SSI prolongs the duration of hospitalization. On the other hand, there were no significant differences in discharge and race resumption rates between cases with and without an SSI. These data indicate that the incidence of SSI in this study was low and that it was higher following plate fixation than screw fixation.

## Relevance to finishing-position (着順) prediction

**Feature family A (injury/soundness).** This paper provides the most directly actionable quantitative priors for post-fracture career trajectory modelling in JRA Thoroughbreds:

1. **Race resumption base rate after fracture surgery:** 61.4% of screw-fixation horses (n = 427) returned to racing after P1/MC3/MT3 fracture surgery; only 45.5% of plate-fixation horses (n = 11) did so. These figures — based on 451 JRA cases over 10 years — are the empirical Bayesian priors for whether a horse that sustained a condylar/sagittal fracture will re-appear in race entries at all. Combined with injury records, these rates feed directly into a horse-level availability model.

2. **Fixation type as a stratification variable:** Plate fixation (LCP) carries a 2.6× higher non-resumption rate (54.5% vs. 38.6% non-resumption) and an OR = 11.65 for SSI (the only statistically significant risk factor). If surgical records are accessible, fixation type is a binary high-value feature.

3. **Hospital discharge rate as a censoring indicator:** 98.0% overall discharge rate (442/451) means very few horses die or are euthanised during hospitalisation for these fracture types. The 8 non-discharge cases include 4 fatal fractures during anaesthesia recovery — not SSI-related.

4. **SSI does not itself impair race resumption** (once a horse is discharged): resumption rate was 60% with SSI vs. 61.4% without SSI in screw cases (not significant), and 33.3% vs. 45.5% in plate cases (not significant). The damage is done by the fracture type and fixation type, not by whether an SSI develops. SSI mainly prolongs hospitalisation (20 vs. 14 days for screw; 25–88 vs. 26 days for plate).

## Background & objective

Condylar fractures of MC3/MT3 and sagittal fractures of P1 are the most common simple fractures in Japanese Thoroughbred racehorses, typically occurring at the articular surface with minimal fragment displacement. They are treated with closed minimally-invasive screw fixation in Japan (small-volume implants), unlike more severe fractures managed elsewhere with plates. Prior SSI literature (Curtiss et al. 2019: 5% for screw; ~16% for LCP; MacDonald et al. 1994: 8.1–31% for various techniques) covered more severe fractures and mixed populations. This study fills the gap for clean, simple, JRA-specific fracture surgeries, and evaluates prophylactic antimicrobial efficacy.

## Materials & methods

**Study design:** Retrospective electronic medical record review of all JRA Thoroughbred racehorses undergoing screw or LCP surgery for sagittal P1 fractures or condylar MC3/MT3 fractures at JRA hospitals (Hakodate, Miho, Ritto), 2011–2020. Ethics: JRA Research Committee permit 2022-3296-04.

**n = 451 cases** (437 screw fixation, 14 plate/LCP fixation). Median age 3 years (IQR 2–3).

**Surgery details:**

- General anaesthesia or standing sedation with local anaesthesia
- Site prep: clippers + povidone-iodine scrub + isopropanol + 10% povidone-iodine
- Ioban antimicrobial incise drape (3M)
- Screws: 4.5-mm cortical bone screws; median 2 screws (IQR 2–4)
- Plate: LCP with 10–13 holes; minimally invasive technique via stub incision
- Screw fixation time: median 55 min (IQR 40–70 min)
- Plate fixation time: median 160 min (IQR 123–191 min)
- 33 surgeons

**Prophylactic antimicrobials used (by frequency):**

- Cephalothin systemic (iv, 10 g/horse, q12h): n = 146 (duration median 4 days, IQR 4–5)
- Cephalothin systemic + amikacin RLP (1.0 g, 1 day): n = 101
- Kanamycin im (5.0 g/horse, q24h): n = 64 (duration median 3 days, IQR 2.3–3)
- Amikacin RLP alone: n = 54 (duration median 1 day)
- Others (Mycillin, polymyxin, marbofloxacin, combinations): n = 51
- None: n = 35

**SSI definition (Curtiss et al. criteria):** Positive bacterial culture from surgical site or drainage PLUS change in comfort or unexplained fever (> 38.6°C).

**Outcome measures:** SSI development (binary), discharge from hospital (binary), race resumption (participation in ≥ 1 race post-surgery), duration of hospitalisation (days).

**Variables tested for association with SSI:** Hospital (Hakodate/Miho/Ritto), sex, age, affected bone (P1 hind, P1 fore, MC3, MT3), surgical time, implant type (screw vs. plate), number of screws, surgeon experience (years), prophylactic antimicrobial choice, anaesthesia method (general vs. standing).

**Statistics:** Data as median (IQR). Fisher's exact test, chi-squared test, or Wilcoxon rank-sum test. Benjamin–Hochberg correction for multiple comparisons. BH-adjusted P < 0.05 = significant. JMP v16 and EZR/R.

## Results (detailed — reproduce ALL numbers)

**Overall SSI incidence:** 2.9% (13/451)

**Table 1: Association of variables with SSI development:**

| Variable                                 | SSI (n) | No SSI (n) | Odds ratio | P value    |
| ---------------------------------------- | ------- | ---------- | ---------- | ---------- |
| Hospital: Hakodate                       | 0       | 17         | —          | 1.00       |
| Hospital: Miho                           | 5       | 229        | Ref        | —          |
| Hospital: Ritto                          | 8       | 192        | 1.90       | 0.82       |
| Sex: Gelding                             | 0       | 17         | —          | —          |
| Sex: Female                              | 1       | 146        | Ref        | —          |
| Sex: Male                                | 12      | 275        | 6.40       | 0.21       |
| Age: OR per 1-year increase              | —       | —          | 0.89       | 0.51       |
| Affected bone: P1 hind                   | 1       | 75         | Ref        | —          |
| Affected bone: P1 fore                   | 3       | 113        | 2.05       | 1.00       |
| Affected bone: MC3                       | 7       | 198        | 2.65       | 1.00       |
| Affected bone: MT3                       | 2       | 55         | 2.73       | 1.00       |
| Surgical time: < 30 min                  | 0       | 33         | —          | 1.00       |
| Surgical time: 31–60 min                 | 5       | 172        | Ref        | —          |
| Surgical time: > 60 min                  | 6       | 132        | 1.56       | 0.90       |
| **Implant: Screw only**                  | **10**  | **427**    | Ref        | —          |
| **Implant: Screw + Plate**               | **3**   | **11**     | **11.65**  | **< 0.01** |
| Screws: 1–2                              | 6       | 224        | Ref        | —          |
| Screws: 3–4                              | 1       | 157        | 0.24       | 0.25       |
| Screws: > 5                              | 3       | 43         | 2.60       | 0.25       |
| Surgeon experience: 3–5 y                | 2       | 84         | Ref        | —          |
| Surgeon experience: 6–10 y               | 5       | 185        | 1.49       | 1.00       |
| Surgeon experience: > 11 y               | 6       | 169        | 1.14       | 1.00       |
| Prophylactic: Kanamycin                  | 0       | 64         | —          | 1.00       |
| Prophylactic: Cephalothin                | 2       | 144        | Ref        | —          |
| Prophylactic: Cephalothin + amikacin RLP | 2       | 99         | 1.45       | 1.00       |
| Prophylactic: Amikacin RLP alone         | 4       | 50         | 5.76       | 0.23       |
| Prophylactic: Others                     | 3       | 48         | 4.50       | —          |
| Prophylactic: None                       | 2       | 33         | 4.36       | 0.37       |
| Anaesthesia: General                     | 13      | 418        | —          | —          |
| Anaesthesia: Standing sedation           | 0       | 20         | —          | 1.00       |

**Only statistically significant predictor: Implant type (plate vs. screw), OR = 11.65, adjusted P < 0.01.**

**Screw fixation outcomes (n = 437):**

- SSI incidence: 10/437 = 2.3%
- Discharge rate: 100% (10/10) with SSI; 98.1% (419/427) without SSI — not significant
- Race resumption: 60.0% (6/10) with SSI; 61.4% (262/427) without SSI — not significant
- Hospitalisation duration: 20 days (IQR 15–33.5) with SSI vs. 14 days (IQR 13–18) without SSI — P < 0.01
- Non-discharge (8 cases): 4 fatal fractures during recovery from anaesthesia or hospitalisation, 2 gastrointestinal disease, 2 laminitis

**Plate fixation outcomes (n = 14):**

- SSI incidence: 3/14 = 21.4%
- Discharge rate: 66.7% (2/3) with SSI vs. 100% (11/11) without SSI — not significant
- Race resumption: 33.3% (1/3) with SSI vs. 45.5% (5/11) without SSI — not significant
- Hospitalisation duration: 25 and 88 days for the 2 SSI cases that were discharged; 26 days (IQR 18–44) for non-SSI cases
- One SSI case not discharged due to anaphylactic shock; plate removed 2 months post-surgery in another MRSA case

**SSI onset:** Median postoperative day 7 (IQR 4.5–10). 12/13 were superficial SSIs. 1 deep case at MT3 plate site on day 3; resolved after plate removal at 2 months.

**Bacterial isolates (from 5/13 SSI cases tested, 11 isolates total):**

- MRSA: 2 isolates
- MSSA (methicillin-sensitive S. aureus): 1 isolate
- Mammaliicoccus sciuri (formerly S. sciuri): 1
- Streptococcus equisimilis: 1
- Aerococcus sp.: 1
- Aerobic Gram-positive rod: 1
- Escherichia coli: 1
- Pseudomonas aeruginosa: 2
- Elizabethkingia sp.: 1
- Multiple species in 2 of 5 cases

MRSA was resistant to beta-lactams; other isolates susceptible to routinely used antimicrobials.

## Discussion & interpretation

The 2.9% overall SSI rate is lower than prior literature (5–31%), which the authors attribute to: (a) exclusively clean, closed, simple fractures — no open wounds or contaminated sites, (b) minimally invasive technique reducing dead space, and (c) closed technique reducing SSI risk compared to open reduction. The key finding — that plate fixation (OR 11.65) is the only significant SSI predictor — is explained by the much greater soft-tissue and bone-tissue damage from LCP insertion and the large dead space around the plate relative to screw fixation.

The absence of a significant association between operative time and SSI is attributed to small sample size (particularly the plate group: n = 14), which limited statistical power.

The finding that SSI does not significantly affect race resumption rate (once discharged) implies that the fracture type and surgical approach, not the infectious complication, determine career prognosis. MRSA cases were the main management challenge: standard beta-lactam prophylaxis failed, and treatment required removing implants or using specialist antibiotics.

The temporal selection bias in antimicrobial choice (kanamycin was preferred during a period of C. difficile colitis cases at one hospital) reflects real-world clinical practice variation and is an acknowledged confound.

## Limitations

- Retrospective design; selection bias and unmeasured confounders possible
- Small plate group (n = 14); statistical power for subgroup analyses is limited
- SSI follow-up limited to hospitalisation period (median 14 days); SSIs developing post-discharge may have been missed (human literature notes up to 88% of SSIs detected by 30 days, but only 40% by 10 days)
- Some horses treated post-discharge by private veterinarians; race resumption data may be incomplete
- No control group for antimicrobial prophylaxis (no randomised trial); selection bias in antimicrobial choice by surgeon preference
- All cases from JRA hospitals; racing performance data available, but the paper does not analyse performance metrics (only race resumption binary)
- Plate group is too small for a reliable OR estimate; the OR = 11.65 confidence interval is not reported and is likely wide

## Feature-engineering notes for the model

- `fracture_surgery_type` — screw vs. plate fixation — JRA veterinary/surgical records — binary feature; plate fixation associated with ~2× higher non-resumption rate (54.5% vs. 38.6%) — data available within JRA veterinary databases; unlikely in public race entry data
- `fracture_bone_type` — P1 sagittal vs. MC3/MT3 condylar — from veterinary records — affects prognosis differently based on severity and loading patterns
- `fracture_race_resumption_prior` — has horse returned to racing after P1 or MC3/MT3 fracture surgery? — binary from race history — 61.4% (screw) or 45.5% (plate) expected return rate; if a horse has a surgical history and then re-enters, this is a positive selection signal
- `post_fracture_entry_flag` — first race entry after fracture surgery — from race history gap analysis — expected performance degradation in first few starts post-injury
- `days_since_fracture_surgery` — days elapsed from surgical record to race entry — derived from medical + race records — recovery trajectory feature; too short an interval may indicate incomplete healing
- **Race resumption as Bayesian prior:** Use 61.4% (screw) or 45.5% (plate) as horse-level prior probability of returning to racing post-fracture surgery when constructing training data splits
- **Do NOT use** SSI occurrence as a direct predictor of race performance: this study shows SSI did not independently affect resumption rate; the fracture/implant type drives outcomes

## Key references / follow-up leads

- **Mita H. et al. 2022** — Vet. Rec. 190:e1482 — "Incidence of carpal fractures and risk factors for recurrent fractures after arthroscopic removal of carpal chip fracture fragments in Thoroughbred racehorses" — companion study on a less severe fracture class (chip fractures); provides parallel return-to-race data
- **Curtiss A.L. et al. 2019** — Vet. Surg. 48:685–693 — SSI in equine orthopedic internal fixation, 155 cases (2008–2016); comparison benchmark (5% screw, 16% LCP in a mixed fracture population)
- **Ahern B.J. et al. 2010** — Vet. Surg. 39:588–593 — orthopedic infections in equine long bone fractures and arthrodeses, 192 cases (1990–2006); key reference for fracture type-SSI associations
- **Kuroda T. et al. 2021** — Equine Vet. J. 53:1239–1249 — pharmacokinetics and pharmacodynamics of cephalothin in healthy horses; context for prophylactic antimicrobial dosing
- **Nomura M. et al. 2020** — Vet. Rec. 187:e14 — Clostridioides difficile colitis in Japanese Thoroughbreds; explains the temporal antimicrobial selection bias in this study
- **Ahern B.J. & Richardson D.W. 2010** — long bone fractures, orthopedic infection; reference for prior SSI rates in equine fracture surgery
