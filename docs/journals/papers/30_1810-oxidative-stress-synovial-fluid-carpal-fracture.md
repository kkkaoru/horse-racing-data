# Markers for oxidative stress in the synovial fluid of Thoroughbred horses with carpal bone fracture

## Metadata

| Field                          | Value                                                                                                                                                                            |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. 30(1): 13–16, 2019                                                                                                                                                |
| docid                          | `30_1810`                                                                                                                                                                        |
| Article type                   | Note                                                                                                                                                                             |
| Authors                        | Nao Tsuzuki, Yoshinori Kanbayashi, Kanichi Kusano                                                                                                                                |
| Affiliations                   | Obihiro University of Agriculture and Veterinary Medicine, Hokkaido 080-8555, Japan; Racehorse Hospital, Miho Training Center, Japan Racing Association, Ibaraki 300-0493, Japan |
| Received / Accepted / Released | February 19, 2018 / November 26, 2018 / 2019                                                                                                                                     |
| Keywords                       | arthritis, horse, oxidative stress, synovial fluid                                                                                                                               |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/30/1/30_1810/_pdf/-char/en                                                                                                              |

## Abstract (verbatim)

> Arthritis is thought to cause oxidative stress in synovial fluid in humans, but there have been few reports in horses. To evaluate oxidative stress in synovial fluid in horses, this study used 19 horses with unilateral fracture of the carpal joint bone. Synovial fluid was collected from the carpal joint on the fracture (arthritis group) and contralateral (control group) sides. Diacron-reactive oxygen metabolites (d-ROMs) and biological antioxidant potential (BAP) were then measured, and the oxidative stress index (OSI) was calculated. d-ROMs and OSI of the arthritis group were significantly higher than the control group. BAP of the arthritis group was significantly lower than the control group. Thus, this study revealed that oxidative stress develops in the synovial fluid of horses during arthritis.

## Relevance to finishing-position (着順) prediction

Feature family: **A — injury/soundness**. The paper confirms that active carpal joint arthritis (arising from fracture) generates measurable oxidative stress in equine synovial fluid. The three markers — d-ROMs (reactive oxygen metabolites), BAP (biological antioxidant potential), and OSI (d-ROMs/BAP × 100) — can also be measured from plasma samples (as established in Kusano et al. 2016 J. Equine Sci. 27:125–129 using the same Free Carpe Diem analyzer). Elevated plasma OSI in JRA horses prior to a race therefore signals active joint inflammation and is a potential engineered feature for injury-risk or performance-impairment scoring.

The principal modelling relevance is indirect: carpal fracture is one of the commonest reasons for JRA Thoroughbred retirement or prolonged absence, and subclinical inflammation detectable by plasma biomarkers may precede the clinically apparent fracture. Horses whose recent blood panels (if available) show elevated OSI or elevated d-ROMs may be at heightened risk for DNF or poor performance relative to ability. The within-horse paired design makes this study's causal claim especially strong.

Interaction with pipeline features: elevated body weight (馬体重) at race time was separately identified as a risk factor for SDF tendinopathy (30_1909), and both injury types may co-occur in the same horse demographic; combining oxidative stress biomarker data with body weight deviation features could improve injury-probability priors. This feature is only practicable if JRA blood sampling data are accessible.

## Background & objective

Oxidative stress — imbalance between reactive oxygen species (ROS) and antioxidants favouring ROS — is implicated in joint disease pathology in humans: inflamed joints activate neutrophils that release ROS; arachidonic acid generated during inflammation also drives ROS production; articular cartilage chondrocytes are particularly vulnerable and are killed on exposure to ROS. Prior horse studies compared oxidative stress markers between different individuals (confounded by diet and exercise), so within-horse intra-individual comparison had never been performed. This study fills that gap using unilateral carpal bone fractures as a natural experiment.

## Materials & methods

**Subjects:** 19 Thoroughbred racehorses (11 males, 7 females, 1 gelding; mean ± SD age 3.2 ± 0.6 years) from the JRA Miho Training Center with radiographically confirmed unilateral fracture of the carpal joint bone.

**Fracture locations:** distal end of radius (n=14), radial carpal bone (n=2), intermediate carpal bone (n=1), distal end of radius + intermediate carpal bone (n=1), third carpal bone (n=1). Right side in 13 cases, left in 6.

**Timing:** median 7 days from fracture to surgery (range 3–12 days). All horses received NSAIDs (Diclofenac 1 mg/kg p.o.) until the day before surgery.

**Sample collection:** 1 ml synovial fluid aspirated from both carpal joints just before arthroscopic incision under general inhaled anaesthesia. Fractured-joint fluid = arthritis group; contralateral joint fluid = control group. Samples stored at −80°C.

**Assay:** d-ROMs and BAP measured on Free Carpe Diem analyzer (Diacron International, Grosseto, Italy). OSI = d-ROMs/BAP × 100.

**Statistics:** paired t-test (Statcel3, OMS Publishing). Normality confirmed; data reported as mean ± SD. Significance threshold P<0.05.

## Results (detailed — reproduce ALL numbers)

All three markers showed statistically significant differences between the arthritis and control joints within the same horse:

| Marker          | Arthritis group (mean ± SD) | Control group (mean ± SD) | P value |
| --------------- | --------------------------- | ------------------------- | ------- |
| d-ROMs (U.CARR) | 87.2 ± 15.0                 | 76.6 ± 16.3               | 0.02    |
| BAP (µM/l)      | 1,661.5 ± 440.3             | 2,050.0 ± 377.0           | 0.0002  |
| OSI             | 5.6 ± 1.8                   | 3.9 ± 1.1                 | 0.0002  |

- d-ROMs were significantly higher in the arthritis group (P=0.02), indicating excessive ROS production.
- BAP was significantly lower in the arthritis group (P=0.0002), indicating consumption of antioxidants.
- OSI was significantly higher in the arthritis group (P=0.0002), indicating net oxidative stress.
- The simultaneous increase in ROS production AND decrease in antioxidant capacity confirms that both mechanisms contribute to the elevated OSI in acute arthritis (not merely antioxidant depletion alone).
- All 19 horses showed palpable joint swelling and heat in the affected joint, confirming clinical arthritis.
- The study notes that chronic cases may show an adaptive increase in antioxidant capacity, so the acute-vs-chronic distinction matters for interpretation.

## Discussion & interpretation

The elevated OSI in the fractured joint relative to the contralateral healthy joint of the same horse demonstrates intra-individual oxidative stress, removing confounders (diet, exercise level) that plagued prior cross-sectional studies. The dual mechanism — increased ROS production (high d-ROMs) AND antioxidant consumption (low BAP) — is analogous to human osteoarthritis and temporomandibular joint disease. Articular cartilage is particularly vulnerable because cultured chondrocytes die when exposed to ROS; thus oxidative stress may damage cartilage at sites remote from the fracture focus. The authors propose that intra-articular antioxidants (vitamin C, vitamin E, astaxanthin, low-molecular-weight hyaluronic acid) might reduce oxidative stress and help preserve joint structure.

The comparator paper by Müller et al. (2010) showed decreased antioxidant capacity in acute equine metacarpophalangeal arthritis; that result (between individuals) now aligns with the within-individual result here.

## Limitations

- n=19 from a single training center (Miho); no JRA Ritto or other venues.
- All cases are acute (median 7 days post-fracture); chronic arthritis may show different patterns (adaptive antioxidant upregulation).
- Multiple fracture sites pooled; site-specific differences not analysed.
- No pre-race blood values available; synovial fluid measurement requires arthroscopy, so direct pre-race screening is not feasible. Blood/plasma proxies (Kusano et al. 2016) are the more practical feature source.
- Small sample size precludes multivariable modelling.

## Feature-engineering notes for the model

- `plasma_dROMs` — diacron-reactive oxygen metabolites measured from pre-race blood sample (U.CARR); elevated values flag excessive ROS production associated with joint inflammation. Source: JRA blood panel records (if available). Expected effect: positive association with injury probability, negative association with race-day performance. Data availability: research/veterinary records only, not standard race database.
- `plasma_BAP` — biological antioxidant potential (µM/l); decreased values flag antioxidant depletion consistent with active inflammation. Source: same blood panel. Expected effect: negative association with injury probability.
- `plasma_OSI` — derived feature: d-ROMs / BAP × 100; composite inflammation index. Expected effect: positive association with injury/DNF risk.
- Reference range (Kusano et al. 2016 JRA Thoroughbreds age 2–5): required to normalise OSI values relative to horse age and sex.
- **Caution:** these biomarkers require blood sampling close to the race and are not present in standard public race databases (netkeiba, JRA official records). Use only if internal JRA veterinary data are available.
- **Interaction term:** `plasma_OSI × days_since_last_race` — long inter-race intervals with elevated OSI may signal ongoing subclinical joint damage.

## Key references / follow-up leads

- Kusano K et al. 2016. Reference range of blood biomarkers for oxidative stress in Thoroughbred racehorses (2–5 years old). J. Equine Sci. 27: 125–129. — **directly actionable**: reference ranges for d-ROMs and BAP in JRA racehorses by age.
- Celi P, Sullivan M, Evans D. 2010. The stability of the reactive oxygen metabolites (d-ROMs) and biological antioxidant potential (BAP) tests on stored horse blood. Vet. J. 183: 217–218. — assay validation in horses.
- Dimock AN, Siciliano PD, McIlwraith CW. 2000. Evidence supporting an increased presence of reactive oxygen species in the diseased equine joint. Equine Vet. J. 32: 439–443. — prior equine joint ROS study (different individuals).
- Müller AJ et al. 2010. Comparison of the antioxidant effects of synovial fluid from equine metacarpophalangeal joints with those of hyaluronic acid and chondroitin sulfate. Am. J. Vet. Res. 71: 399–404. — acute equine arthritis antioxidant depletion.
- Villasante A et al. 2010. Antioxidant capacity and oxidative damage determination in synovial fluid of chronically damaged equine metacarpophalangeal joint. Vet. Res. Commun. 34: 133–141. — chronic equine arthritis (different pattern from acute).
