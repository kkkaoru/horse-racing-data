# Processing log — collection cursor & decisions

Newest-first. Each article examined is recorded here with a keep/skip decision so the
autonomous loop is resumable. `kept` papers have a file in `papers/`.

**Cursor**: **COMPLETE** — all Vols **5–37** screened (Waves 1–4). Vols 1–4 are not on J-STAGE (HTTP 500; the `jes` archive starts at Vol 5, 1994). Kept total: **119** of ~490 examined.

Helper tool ready: `scripts/journals/jstage.py` (+ `test_jstage.py`, 26 tests / 99% cov) — stdlib J-STAGE contents parser + PDF downloader. PDFs cached under `scripts/journals/cache/`.

Legend — decision: `kept` / `skip`. reason codes: `repro` (reproduction), `genetics-cons`
(conservation/breed genetics), `method` (lab/assay method), `dental` (age estimation),
`vet-nonperf` (veterinary, no performance link), `perf` (performance-relevant → kept).

## Volume 37 (2026)

### Issue 1 — released 2026-03-14

| docid   | title                                                                                                                               | type       | decision | reason                                                                                                                |
| ------- | ----------------------------------------------------------------------------------------------------------------------------------- | ---------- | -------- | --------------------------------------------------------------------------------------------------------------------- |
| 37_2511 | Dynamics of plasma anti-Müllerian hormone concentrations before and after ovum pick-up in pure and crossbred Hokkaido native ponies | Full Paper | skip     | repro                                                                                                                 |
| 37_2513 | Comparative study of dental age estimation methods against known chronological age in Nigerian local horses                         | Full Paper | skip     | dental — age-estimation method, non-racing population, no performance data                                            |
| 37_2509 | Trends in the prevalence of Gidoh at Japan Racing Association Training Centers in 2020                                              | Full Paper | skip     | vet-nonperf — JRA racehorses & hoof soundness, but abstract states no link between Gidoh and performance/race outcome |
| 37_2515 | Genetic background and phenotypic features of the endangered Miyako horse                                                           | Full Paper | skip     | genetics-cons                                                                                                         |
| 37_2514 | Direct single-nucleotide polymorphism genotyping from whole blood without DNA extraction                                            | Note       | skip     | method                                                                                                                |

Vol 37 Issue 1: 5 examined, 0 kept.

## Volume 36 (2025)

### Issue 4 — released 2025-12-16

| docid   | title                                                                      | type   | decision | reason                                       |
| ------- | -------------------------------------------------------------------------- | ------ | -------- | -------------------------------------------- |
| 36_2512 | Tibetan wild ass, _Equus kiang_, in the literature: a comprehensive review | Review | skip     | genetics-cons — wildlife/non-racing          |
| 36_2510 | First documented case of equine brucellosis in Libya: a case report        | Note   | skip     | vet-nonperf — infectious-disease case report |

### Issue 3 — released 2025-09-17

| docid   | title                                                                                                                        | type       | decision | reason                                       |
| ------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------- | -------- | -------------------------------------------- |
| 36_2421 | Evaluation of maturation-related changes in maxillary sinus diameter and cheek teeth positioning in the Dareshuri horse      | Full Paper | skip     | vet-nonperf — anatomy, non-racing breed      |
| 36_2504 | Correlation of hindgut microbiome and fermentation properties with a history of gas/impaction colic in Japanese draft horses | Full Paper | skip     | vet-nonperf — colic/microbiome, draft horses |
| 36_2506 | Misconceptions and misuse: caregivers' knowledge/attitudes/practices re dexamethasone, Ibadan, Nigeria                       | Full Paper | skip     | vet-nonperf — KAP survey                     |

### Issue 2 — released 2025-06-12

| docid   | title                                                                                                                | type       | decision | reason                                                                                                                                        |
| ------- | -------------------------------------------------------------------------------------------------------------------- | ---------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| 36_2417 | Diagnostic performance of oxidative stress biomarkers, acute phase proteins, trace elements in equine colic severity | Full Paper | skip     | vet-nonperf — colic diagnostics                                                                                                               |
| 36_2420 | Metabolic, pathological, and genetic analyses of neonatal foals that died in Noma horses                             | Full Paper | skip     | vet-nonperf — neonatal mortality, conservation breed                                                                                          |
| 36_2501 | Reduction in endemic EHV-1/EHV-4 infection among Thoroughbred yearlings through an updated vaccination program       | Full Paper | skip     | vet-nonperf — borderline (Thoroughbred respiratory infection) but vaccination-efficacy epidemiology, no predictive feature/performance signal |
| 36_2410 | Sudden death in a Thoroughbred stallion: cardiac tamponade due to transverse aortic rupture                          | Note       | skip     | vet-nonperf — single case report                                                                                                              |

### Issue 1 — released 2025-03-19

| docid   | title                                                                                                                 | type       | decision | reason                                                                |
| ------- | --------------------------------------------------------------------------------------------------------------------- | ---------- | -------- | --------------------------------------------------------------------- |
| 36_2415 | Differences in serum iron concentrations between summer and winter in Noma horses                                     | Full Paper | skip     | vet-nonperf — seasonal physiology, non-racing breed                   |
| 36_2419 | Genomic regions and candidate genes associated with forehead whorl positioning in Thoroughbred horses                 | Full Paper | skip     | genetics — whorl-position GWAS, no link to performance in this paper  |
| 36_2418 | Effects of pre-exercise cooling in hot environments on performance and physiological responses in Thoroughbred horses | Full Paper | **kept** | perf — Thoroughbred performance + heat/body-weight/thermal physiology |
| 36_2411 | A case of a newborn Kiso native pony with median hard cleft palate and urachal hypoplasia                             | Note       | skip     | vet-nonperf — congenital case report                                  |
| 36_2413 | Preoperative CT imaging for equine cranial disorders: two case reports of congenital malformations                    | Note       | skip     | vet-nonperf — imaging case reports                                    |
| 36_2414 | Isolation and molecular identification of Lactobacillaceae and Bifidobacterium from horse feces                       | Note       | skip     | method — microbiology isolation                                       |

Vol 36: 16 examined, 1 kept (36_2418).

## Volume 35 (2024) — 4 examined-issues, kept 3

| docid      | iss | decision | reason                                            | title                                                                                 |
| ---------- | --- | -------- | ------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 35_JES2311 | 1   | **kept** | perf — conformation measures                      | 3D imaging and body measurement of riding horses using four scanners simultaneously   |
| 35_JES2315 | 1   | skip     | vet-nonperf — BCS estimation, Nigerian non-racing | Can Nigerian horse owners effectively estimate body condition and cresty neck scores? |
| 35_2308    | 1   | skip     | repro — testicular arteritis, draft               | Non-suppurative and necrotizing testicular arteritis in a heavy draft horse           |
| 35_2401    | 2   | **kept** | perf — fetlock OA severity marker                 | Radiographic texture of trabecular bone of proximal phalanx in MCP osteoarthritis     |
| 35_2322    | 2   | skip     | vet-nonperf — Noma serum amino acids              | Serum amino acid profiles in clinically normal Noma horses                            |
| 35_24009   | 3   | skip     | vet-nonperf — castration anaesthesia              | TIVA propofol–ketamine–xylazine ± remifentanil in castration                          |
| 35_2406    | 3   | skip     | other — tapeworm DNA barcoding                    | DNA barcoding of Anoplocephala perfoliata from a Ban'ei horse                         |
| 35_2404    | 4   | skip     | vet-nonperf — activity meter, Kiso, low accuracy  | Assessment of horse behavior using a cat/dog activity device                          |
| 35_2407    | 4   | **kept** | perf — pedigree inbreeding/sire blood             | Rising trends of inbreeding in Japanese Thoroughbred horses                           |

## Volume 34 (2023) — 18 examined, kept 2

| docid   | iss | decision | reason                                           | title                                                                                           |
| ------- | --- | -------- | ------------------------------------------------ | ----------------------------------------------------------------------------------------------- |
| 34_2302 | 3   | **kept** | perf — fracture SSI + return-to-racing           | Incidence of SSI after internal fixation of P1 and MC3/MT3 fractures in Thoroughbred racehorses |
| 34_2305 | 4   | **kept** | perf — MSTN/LCORL/DMRT3/HTR1A                    | Genetic characterization of Japanese native horse breeds by genotyping trait variants           |
| 34_2223 | 1   | skip     | genetics-cons — Miyako diversity                 | Genetic diversity using 31 microsatellites in Miyako horses                                     |
| 34_2218 | 1   | skip     | vet-nonperf — CEM qPCR detection                 | Enhanced detection of Taylorella equigenitalis by qPCR                                          |
| 34_2222 | 1   | skip     | vet-nonperf — coronavirus serosurvey             | Equine coronavirus virus-neutralizing antibodies in riding stables                              |
| 34_19   | 1   | skip     | other — errata                                   | ERRATA: Taishu horses                                                                           |
| 34_2232 | 2   | skip     | method — gene-doping plasma storage              | Storage/use of plasma samples for gene doping tests                                             |
| 34_2227 | 2   | skip     | vet-nonperf — equine metabolic syndrome, Nigeria | EMS occurrence and risk factors in Nigeria                                                      |
| 34_2214 | 2   | skip     | vet-nonperf — granulosa cell tumour IHC          | Immunohistochemical markers for granulosa cell tumors                                           |
| 34_2226 | 2   | skip     | vet-nonperf — transfusion antibodies             | Anti-erythrocyte antibodies in donor horses                                                     |
| 34_2229 | 2   | skip     | other — hay feeder efficiency                    | Round bale feeders: Tombstone vs Hay Saver                                                      |
| 34_2230 | 2   | skip     | vet-nonperf — HRV in Criollo (non-racing)        | HRV in Criollo horses                                                                           |
| 34_2301 | 3   | skip     | vet-nonperf — resveratrol & MSC cell cycle       | Resveratrol effects on equine MSC cell cycle                                                    |
| 34_2304 | 3   | skip     | vet-nonperf — colic oxidative stress             | Severity of colic vs oxidative stress/APP/trace elements                                        |
| 34_2310 | 3   | skip     | method — INDEL ID panel                          | Individual identification panel using INDEL markers                                             |
| 34_2231 | 3   | skip     | vet-nonperf — piroplasmosis case                 | Equine piroplasmosis at Tokyo 2020                                                              |
| 34_2317 | 4   | skip     | other — draft microbiome/nutrition               | Concentrate levels vs intestinal fermentation in draft horses                                   |
| 34_2312 | 4   | skip     | method — cephalothin PK                          | Cephalothin PK/PD after IM administration                                                       |

## Volume 33 (2022) — 12 examined, kept 1

| docid   | iss | decision | reason                                            | title                                                                                 |
| ------- | --- | -------- | ------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 33_2202 | 1   | **kept** | perf — transport HR/HRV stress                    | Effect of restraint inside the transport vehicle on HR and HRV in Thoroughbred horses |
| 33_1    | 1   | skip     | repro — transitional-mare ovulation               | Artificial light + progesterone device for first ovulation                            |
| 33_2131 | 1   | skip     | vet-nonperf — neck adipose haemorrhage, slaughter | Crest adipose haemorrhage in slaughtered heavy horses                                 |
| 33_2203 | 2   | skip     | method — in vitro tendon differentiation          | BMP12+KY02111 tendon differentiation in MSCs                                          |
| 33_2114 | 2   | skip     | genetics-cons — Noma age vs blood/size            | Age vs blood/body size in Noma horses                                                 |
| 33_2210 | 3   | skip     | vet-nonperf — antivenom hyperimmunisation         | Serum proteins in crotalid venom hyperimmunisation                                    |
| 33_2201 | 3   | skip     | vet-nonperf — Arabian skin dermoscopy             | Videodermoscopy of Arabian skin in summer                                             |
| 33_2204 | 3   | skip     | vet-nonperf — UV vitamin D3                       | Undetectable vitamin D3 in UV-irradiated equine skin                                  |
| 33_2207 | 3   | skip     | vet-nonperf — cephalothin tissue levels           | Cephalothin in body fluids/tissues                                                    |
| 33_2215 | 4   | skip     | other — donkey packing HR/RR                      | Packing effects on donkey diurnal HR/RR                                               |
| 33_2221 | 4   | skip     | genetics-cons — Taishu parentage                  | Taishu horse diversity/parentage, 31 microsatellites                                  |
| 33_2211 | 4   | skip     | vet-nonperf — EPE foal prevalence                 | Equine proliferative enteropathy in Hidaka foals                                      |

## Volume 32 (2021) — 22 examined, kept 3

| docid   | iss | decision | reason                                       | title                                                                        |
| ------- | --- | -------- | -------------------------------------------- | ---------------------------------------------------------------------------- |
| 32_2029 | 1   | **kept** | perf — transport respiratory biomarkers      | Serum proteomes: healthy vs respiratory-disease-with-transport Thoroughbreds |
| 32_2016 | 2   | **kept** | perf — exercise GH/prolactin                 | Exercise & emotional stress on prolactin and GH secretion                    |
| 32_2113 | 4   | **kept** | perf — carpal fracture racing outcome        | Arthroscopic repair of 4th carpal bone slab fracture                         |
| 32_2106 | 1   | skip     | vet-nonperf — rotavirus review               | Equine rotavirus infection                                                   |
| 32_2026 | 1   | skip     | vet-nonperf — draft blood typing             | Erythrocyte antigen frequencies in draft donors                              |
| 32_2032 | 1   | skip     | vet-nonperf — Noma haematology               | Season/sex effects on Noma blood parameters                                  |
| 32_2101 | 2   | skip     | vet-nonperf — gastric ulcer rice extract     | Rice fermented extract on gastric ulcers                                     |
| 32_2105 | 2   | skip     | repro — broodmare age                        | Advancing age vs broodmare reproductive performance                          |
| 32_1931 | 2   | skip     | repro — gestation activin A                  | Circulating activin A in gestation                                           |
| 32_2024 | 2   | skip     | vet-nonperf — navicular farriery case        | Modified Z-bar shoe for navicular syndrome                                   |
| 32_2031 | 2   | skip     | vet-nonperf — Staph colonisation Libya       | Nasal Staph colonisation/resistance                                          |
| 32_2108 | 2   | skip     | repro — granulosa tumour case                | 17 kg ovarian granulosa cell tumor in draft mare                             |
| 32_2033 | 3   | skip     | method — tablet 3D measurement               | Body measurement with tablet 3D scanner                                      |
| 32_2035 | 3   | skip     | vet-nonperf — hippotherapy spinal kinematics | Spinal kinematics under asymmetric riding                                    |
| 32_2027 | 3   | skip     | other — Mongolian mare lactation             | Lactation vs metabolic parameters, grazing mares                             |
| 32_2112 | 3   | skip     | vet-nonperf — EHV-1 vaccine antibodies       | EHV-1 vaccine antibody persistence                                           |
| 32_2122 | 4   | skip     | vet-nonperf — head/neck US injection review  | Ultrasound-guided head/neck injections                                       |
| 32_117  | 4   | skip     | repro — PGF2α uterine neutrophils            | PGF2α on uterine PMN counts in draft horses                                  |
| 32_2125 | 4   | skip     | method — gene-doping reference materials     | Reference materials for qPCR gene-doping tests                               |
| 32_2123 | 4   | skip     | vet-nonperf — MCP angle vs SDFT echo (null)  | MCP joint angle vs SDFT/SL echogenicity in gaited horses                     |
| 32_2127 | 4   | skip     | repro — abnormal ovary AMH case              | Non-neoplastic abnormal ovary with high AMH                                  |
| 32_2118 | 4   | skip     | other — mule cortisol                        | Serum cortisol in working mules                                              |

## Volume 31 (2020) — 18 examined, kept 4

| docid   | iss | decision | reason                                   | title                                                                                 |
| ------- | --- | -------- | ---------------------------------------- | ------------------------------------------------------------------------------------- |
| 31_1923 | 2   | **kept** | perf — conformation↔injury               | Morphometric measurements and musculoskeletal disorders in jumping Thoroughbreds      |
| 31_2020 | 3   | **kept** | perf(uncertain) — MSTN/DMRT3             | Kushum horse genetics: mtDNA/Y + trait genes                                          |
| 31_2012 | 4   | **kept** | perf — VO₂max/lactate/SaO₂               | Cardiopulmonary function during supramaximal exercise (hypoxia/normoxia/hyperoxia)    |
| 31_2008 | 4   | **kept** | perf — jockey fall/DNF epidemiology      | Epidemiology of jockey falls and injuries in flat and jump races in Japan (2003–2017) |
| 31_1922 | 1   | skip     | repro — mule foal passive immunity       | Passive immunity in mule foals                                                        |
| 31_1929 | 1   | skip     | vet-nonperf — donkey brucellosis         | Brucella seroprevalence in Nigerian donkeys                                           |
| 31_1921 | 1   | skip     | vet-nonperf — foal salacinol supplement  | Salacinol supplement in neonatal foals                                                |
| 31_1902 | 2   | skip     | other — donkey foot morphometry          | Morphometric measurements of working donkey feet                                      |
| 31_1906 | 2   | skip     | repro — draft colostral IgG              | Colostral/foal IgG and perinatal abnormalities                                        |
| 31_2001 | 3   | skip     | vet-nonperf — neurofibroma lameness case | Subcutaneous neurofibroma causing lameness                                            |
| 31_1924 | 3   | skip     | method — MALDI-TOF ID                    | MALDI-TOF identification of equine isolates                                           |
| 31_2015 | 3   | skip     | other — coat-colour registration         | Inconsistencies in coat colour registration                                           |
| 31_2005 | 3   | skip     | vet-nonperf — West Nile serosurvey       | West Nile virus in horses/mosquitoes, Nigeria                                         |
| 31_2025 | 4   | skip     | method — horsehair WGR gene-doping       | Whole-genome resequencing from horsehair roots                                        |
| 31_2021 | 4   | skip     | vet-nonperf — E. coli AMR                | AMR profiles of E. coli in racehorses                                                 |
| 31_2013 | 4   | skip     | vet-nonperf — Yonaguni nuchal ligament   | Nuchal ligament anatomy in Yonaguni ponies                                            |
| 31_2018 | 4   | skip     | repro — abortion/uterine prolapse        | Abortion and uterine prolapse in a mare                                               |
| 31_2014 | 4   | skip     | method — PRP double-spin                 | Optimal double-spin for equine PRP                                                    |

## Volume 30 (2019) — 17 examined, kept 7

| docid   | iss | decision | reason                                   | title                                                                      |
| ------- | --- | -------- | ---------------------------------------- | -------------------------------------------------------------------------- |
| 30_1816 | 1   | **kept** | perf — condylar fracture return          | Unicortical palmar lateral condylar fracture, return 14 days post-surgery  |
| 30_1810 | 1   | **kept** | perf — fracture oxidative-stress markers | Oxidative-stress markers in synovial fluid with carpal bone fracture       |
| 30_1901 | 2   | **kept** | perf — EHI × WBGT × racecourse           | Post-race exertional heat illness prevalence and racecourse climate, Japan |
| 30_1905 | 2   | **kept** | perf — epiglottic entrapment             | Epiglottic entrapment surgery outcomes in Thoroughbreds                    |
| 30_1916 | 3   | **kept** | perf — post-race metabolomics            | Metabolomic plasma changes after racing (LC-HRMS)                          |
| 30_1918 | 4   | **kept** | perf — VO₂max reference                  | VO₂ measurement with flow-through calorimeter                              |
| 30_1909 | 4   | **kept** | perf — SDF tendinopathy risk factors ★   | Risk factors for SDF tendinopathy in Thoroughbred racing, Japan            |
| 30_1822 | 1   | skip     | vet-nonperf — nemabiome                  | Equine intestinal nemabiome NGS                                            |
| 30_1903 | 2   | skip     | vet-nonperf — anaesthesia induction      | Thiopental/guaifenesin vs propofol/ketamine induction                      |
| 30_1910 | 2   | skip     | genetics-cons — Taishu conservation      | Genetic analysis of Taishu horses                                          |
| 30_1914 | 3   | skip     | vet-nonperf — ESBL E. coli               | ESBL E. coli in racehorses                                                 |
| 30_1912 | 3   | skip     | vet-nonperf — EPE serum protein          | Total serum protein as EPE diagnostic                                      |
| 30_1904 | 3   | skip     | other — Noma haematology reference       | Noma horse haematology/biochemistry reference                              |
| 30_1908 | 4   | skip     | repro — draft uterine fluid/bacteria     | Intrauterine fluid vs cervical bacteria in draft mares                     |
| 30_1820 | 4   | skip     | vet-nonperf — temporohyoid OA (riding)   | Temporohyoid osteoarthropathy & crib-biting                                |
| 30_1913 | 4   | skip     | vet-nonperf — penile sarcoid             | Surgical management of penile sarcoid                                      |
| 30_1920 | 4   | skip     | vet-nonperf — salacinol safety           | Effect and safety of salacinol                                             |

## Volume 29 (2018) — 19 examined, kept 2

| docid   | iss | decision | reason                                     | title                                                             |
| ------- | --- | -------- | ------------------------------------------ | ----------------------------------------------------------------- |
| 29_1730 | 1   | **kept** | perf — MSTN distance aptitude              | Selection for eventing on the MSTN gene in Brazilian sport horses |
| 29_1802 | 3   | **kept** | perf — mixed-effects speed model ★         | Ranking quarter horse sires via models of offspring performance   |
| 29_1723 | 1   | skip     | repro — uterus/placenta thickness          | Combined uterus/placenta thickness in draft pregnancy             |
| 29_1728 | 1   | skip     | vet-nonperf — enterolithiasis CT           | Large colon displacement with enterolithiasis                     |
| 29_1732 | 1   | skip     | vet-nonperf — colitis glucose marker       | Blood glucose in acute colitis/SIRS                               |
| 29_1731 | 1   | skip     | vet-nonperf — Bordetella isolates          | Bordetella bronchiseptica from horses                             |
| 29_1739 | 2   | skip     | repro — uteroplacental activin receptors   | Activin receptors in uteroplacental tissue                        |
| 29_1807 | 2   | skip     | genetics-cons — Y-chromosome natives       | Y-chromosomal haplotypes in Japanese native horses                |
| 29_1736 | 2   | skip     | vet-nonperf — branchial cyst surgery       | Marsupialisation of branchial remnant cyst                        |
| 29_1801 | 2   | skip     | vet-nonperf — fenbendazole microbiome      | Fenbendazole effect on hindgut microbiome                         |
| 29_1806 | 3   | skip     | method — albumin removal proteomics        | Albumin removal for gastric-ulcer proteomics                      |
| 29_1811 | 3   | skip     | other — foal lying behaviour               | Lying behaviour in Thoroughbred foals                             |
| 29_1805 | 3   | skip     | vet-nonperf — propofol oxidative stress    | Oxidative stress under propofol protocols                         |
| 29_1812 | 3   | skip     | vet-nonperf — colitis coagulopathy         | Coagulopathy in acute colitis severity                            |
| 29_1817 | 4   | skip     | genetics-cons — Miyako/Yonaguni            | Genetic relationship of Miyako & Yonaguni horses                  |
| 29_1738 | 4   | skip     | other — Turkoman conformation heritability | Body conformation traits of Iranian Turkoman horses               |
| 29_1819 | 4   | skip     | genetics-cons — Tokara conservation        | Tokara horse conservation, 31 microsatellites                     |
| 29_1814 | 4   | skip     | method — scintigraphy bedding              | Water-dispersed paper bedding for scintigraphy                    |
| 29_1815 | 4   | skip     | vet-nonperf — EAV ELISA                    | ELISAs for equine arteritis virus antibodies                      |
| 29_1808 | 4   | skip     | vet-nonperf — synovium MSC therapy         | Synovium-derived MSC implantation pilot                           |

## Volume 28 (2017) — 24 examined, kept 8

| docid   | iss | decision | reason                                   | title                                                            |
| ------- | --- | -------- | ---------------------------------------- | ---------------------------------------------------------------- |
| 28_1623 | 1   | **kept** | perf — exercise physiology review        | Exercise physiology of draft horses in Japan (1950s–60s)         |
| 28_1626 | 2   | **kept** | perf — hypoxic training VO₂max           | Hypoxic training increases VO₂max in Thoroughbreds               |
| 28_1703 | 2   | **kept** | perf — MHC muscle isoforms               | PRP & gene expression/morphology of Thoroughbred skeletal muscle |
| 28_1701 | 2   | **kept** | perf — EIPH/IAD prevalence               | EIPH and inflammatory airway disease in polo ponies              |
| 28_1712 | 3   | **kept** | perf — suspensory MRI → return           | Standing MRI of suspensory-origin osseous injury                 |
| 28_1717 | 3   | **kept** | perf — HR/HRV vs age                     | HR and HRV as a function of age in Thoroughbreds                 |
| 28_1726 | 4   | **kept** | perf — GWAS body weight (MSTN/LCORL)     | GWAS for body weight in Japanese Thoroughbred racehorses         |
| 28_1702 | 3   | **kept** | perf — foal sesamoid fracture            | Histopathology of apical PSB fracture in young foals             |
| 28_1620 | 1   | skip     | other — herd GPS distances               | Wearable GPS interindividual distances                           |
| 28_1624 | 1   | skip     | vet-nonperf — Gidoh keratolytic bacteria | Isolation of keratolytic bacteria in hoof-wall cavity            |
| 28_1608 | 1   | skip     | vet-nonperf — dystocia foal blood gas    | Dystocia effects on draft newborn blood gas                      |
| 28_1707 | 2   | skip     | repro — aborted fetal sizes              | Aborted fetal sizes in Thoroughbreds                             |
| 28_1705 | 2   | skip     | genetics-cons — coat colour loci         | Gray/Extension loci in Thoroughbred coat colour                  |
| 28_1721 | 3   | skip     | other — transport behaviour review       | Minimising transport-related problem behaviour                   |
| 28_1718 | 3   | skip     | other — yearling growth N/S climate      | Body growth & endocrine N vs S Japan                             |
| 28_1714 | 3   | skip     | vet-nonperf — chronic laminitis imaging  | MRI/CT of severe chronic laminitis                               |
| 28_1704 | 3   | skip     | vet-nonperf — anaesthesia case           | Sevoflurane+alfaxalone-medetomidine anaesthesia                  |
| 28_1710 | 3   | skip     | vet-nonperf — temporohyoid OA foal       | CT/MRI of early temporohyoid OA in a foal                        |
| 28_1729 | 4   | skip     | method — RNA extraction kits             | RNA kits & qPCR mixes for gastric epithelium                     |
| 28_1706 | 4   | skip     | vet-nonperf — fentanyl/medetomidine CRI  | Fentanyl+medetomidine under sevoflurane                          |
| 28_1716 | 4   | skip     | vet-nonperf — ocular mast cell tumour    | Equine ocular mast cell tumour                                   |
| 28_1722 | 4   | skip     | other — peripheral blood stem cells      | PB stem cells from a native horse                                |
| 28_1715 | 4   | skip     | repro — rebreeding foaling rate          | Foaling rate after pregnancy loss                                |
| 28_1725 | 4   | skip     | repro — uterine torsion prognosis        | Factors affecting uterine torsion prognosis                      |

Wave 1 (Vols 35–28): 132 examined, 30 kept.

## Volume 27 (2016) — 22 examined, kept 7

| docid   | iss | decision | reason                                     | title                                                                        |
| ------- | --- | -------- | ------------------------------------------ | ---------------------------------------------------------------------------- |
| 27_1604 | 2   | **kept** | perf — exercise HR/ESR review              | Exercise physiology of the racehorse, Japan 1930s–70s (respiration/HR/blood) |
| 27_1529 | 2   | **kept** | perf — radiographs → debut delay           | Radiographic abnormalities in 2yo in-training sales repositories, Japan      |
| 27_1514 | 3   | **kept** | perf — JRA injury/fracture epidemiology ★  | Epidemiology of racing injuries (fractures), Japan 1980s–2000s               |
| 27_1611 | 3   | **kept** | perf — LCORL body composition              | LCORL BIEC2-808543 variants & body composition under training                |
| 27_1606 | 3   | **kept** | perf — oxidative-stress reference          | Reference range of blood oxidative-stress biomarkers (2–5 yo)                |
| 27_1610 | 4   | **kept** | perf — recovery-rate index                 | Exercise physiology & performance testing via recovery rate (1930s)          |
| 27_1616 | 4   | **kept** | perf — exercise transcriptome (SAA)        | Exercise-induced transcripts in peripheral blood                             |
| 27_1530 | 1   | skip     | vet-nonperf — brucellosis serosurvey       | Horse brucellosis seroprevalence, Nigeria                                    |
| 27_1532 | 1   | skip     | vet-nonperf — TIVA anaesthesia             | Alfaxalone/butorphanol/medetomidine cardiorespiratory effects                |
| 27_1523 | 1   | skip     | vet-nonperf — postpartum fever             | Postpartum fever haematology in draft mares                                  |
| 27_1525 | 1   | skip     | vet-nonperf — filariasis antigens          | Immunodiagnostic antigens for cerebrospinal filariasis                       |
| 27_1522 | 1   | skip     | vet-nonperf — joint supplement             | Resveratrol+HA supplement in aged lame horses                                |
| 27_1509 | 2   | skip     | vet-nonperf — charcoal hindgut             | Activated charcoal supplementation in vitro                                  |
| 27_1527 | 2   | skip     | other — dedifferentiated fat cells         | Equine dedifferentiated fat cell multipotency                                |
| 27_1526 | 2   | skip     | vet-nonperf — castration oxidative stress  | Oxidative stress after castration                                            |
| 27_1602 | 3   | skip     | vet-nonperf — influenza serosurvey         | Equine influenza serology, Nigeria                                           |
| 27_1605 | 3   | skip     | vet-nonperf — cardiac tamponade foal       | Autopsy imaging for cardiac tamponade in a foal                              |
| 27_1531 | 3   | skip     | vet-nonperf — motor neuron disease         | Four cases of equine motor neuron disease                                    |
| 27_1601 | 4   | skip     | vet-nonperf — cervical cord CT myelography | Cervical cord compression CT myelography in foals                            |
| 27_1615 | 4   | skip     | repro — buserelin ovulation                | GnRH analog buserelin in draft mares                                         |
| 27_1612 | 4   | skip     | method — housekeeping genes qPCR           | Housekeeping genes for kidney qPCR                                           |
| 27_1609 | 4   | skip     | vet-nonperf — bispectral index             | BIS as anaesthetic depth indicator                                           |

## Volume 26 (2015) — 20 examined, kept 4

| docid   | iss | decision | reason                                     | title                                                      |
| ------- | --- | -------- | ------------------------------------------ | ---------------------------------------------------------- |
| 26_1414 | 1   | **kept** | perf — ECG/AF & exercise HR                | History of racehorse electrocardiography research in Japan |
| 26_1506 | 2   | **kept** | perf — age × racing speed (JRA) ★          | The effect of age on the racing speed of Thoroughbreds     |
| 26_1513 | 3   | **kept** | perf — bone mineral density/fracture risk  | Bone mineral density in Thoroughbreds via QCT              |
| 26_1517 | 4   | **kept** | perf — SDFT Doppler & training load        | Color Doppler flow in SDFT during training                 |
| 26_1502 | 1   | skip     | repro — AMH cryptorchid                    | AMH as hemi-castrated cryptorchid indicator                |
| 26_1413 | 1   | skip     | vet-nonperf — Rhodococcus antibiogram      | Rhodococcus equi prevalence/antibiogram, India             |
| 26_1408 | 1   | skip     | method — LAMP Taylorella                   | LAMP detection of Taylorella spp.                          |
| 26_1503 | 2   | skip     | repro — yearling endocrine/climate         | Growth & endocrine of yearlings, different climates        |
| 26_1510 | 2   | skip     | repro — photoperiod gonadal                | Extended photoperiod on gonadal function/hair coat         |
| 26_1505 | 2   | skip     | vet-nonperf — hinged shoes contracted feet | Aluminum hinged shoes for contracted feet                  |
| 26_1516 | 3   | skip     | vet-nonperf — MSC glycosaminoglycan        | MSC GAG secretion in vitro                                 |
| 26_1508 | 3   | skip     | repro — IVP twins                          | Monozygotic twins from IVP embryo transfer                 |
| 26_1501 | 3   | skip     | vet-nonperf — MRSA keratitis               | MRSA ulcerative keratitis case                             |
| 26_1511 | 4   | skip     | other — donkey behaviour                   | Seasonal donkey behavioural activities                     |
| 26_1512 | 4   | skip     | repro — photoperiod yearling               | Photoperiod treatment on yearlings                         |
| 26_1519 | 4   | skip     | vet-nonperf — Cushing's case               | Cushing's disease in a stallion                            |
| 26_1515 | 4   | skip     | vet-nonperf — fungal placentitis           | Fungal placentitis case                                    |
| 26_1518 | 4   | skip     | repro — training & progesterone            | Progesterone in racehorses during training (null)          |
| 26_1521 | 4   | skip     | vet-nonperf — xylazine plasma protein      | Plasma protein after xylazine                              |
| 26_1528 | 4   | skip     | repro — photoperiod weanling               | Photoperiod effects on weanlings                           |

## Volume 25 (2014) — 13 examined, kept 5

| docid   | iss | decision | reason                              | title                                                    |
| ------- | --- | -------- | ----------------------------------- | -------------------------------------------------------- |
| 25_1315 | 1   | **kept** | perf — SDFT repair biology          | Tenomodulin via Wnt/β-catenin in equine BMSCs            |
| 25_1316 | 1   | **kept** | perf — tendon/ligament forces       | In vivo flexor tendon & SL forces during trotting        |
| 25_1402 | 2   | **kept** | perf — birthday growth curves       | Individual growth curves by birthday                     |
| 25_1410 | 4   | **kept** | perf — incline EMG conditioning     | Treadmill inclination EMG of fore/hindlimb               |
| 25_1412 | 4   | **kept** | perf — transportation fever         | Transportation fever in 2yo (marbofloxacin)              |
| 25_1313 | 1   | skip     | vet-nonperf — pack donkey thermoreg | Diurnal/seasonal donkey rectal temp/HR/RR                |
| 25_1314 | 1   | skip     | repro — deslorelin conception       | Deslorelin post-breeding LH/progesterone                 |
| 25_1401 | 2   | skip     | vet-nonperf — gut Lactobacilli      | Commensal Lactobacilli/Bifidobacteria intestinal barrier |
| 25_1403 | 2   | skip     | repro — endometrosis                | Endometrosis histopathology in mares                     |
| 25_1404 | 2   | skip     | vet-nonperf — S. equi capsule       | Antiphagocytic SeM requires capsule                      |
| 25_1405 | 3   | skip     | other — pawing stereotypy           | Pawing in Standardbred racehorses                        |
| 25_1407 | 3   | skip     | vet-nonperf — gastrocnemius rupture | Gastrocnemius rupture in neonatal foals                  |
| 25_1409 | 4   | skip     | vet-nonperf — probiotics feces      | Fermented probiotics & fecal condition                   |

## Volume 24 (2013) — 11 examined, kept 2

| docid   | iss | decision | reason                                 | title                                                        |
| ------- | --- | -------- | -------------------------------------- | ------------------------------------------------------------ |
| 24_1    | 1   | **kept** | perf — oxidative stress biomarkers     | Treadmill exercise & hydrogen water on oxidative metabolites |
| 24_1312 | 4   | **kept** | perf — percentile growth curves        | Percentile growth curves with Z-scores (seasonal CG)         |
| 24_17   | 2   | skip     | vet-nonperf — tenascin-C in vitro      | Tenascin-C in tendon-derived cells                           |
| 24_25   | 2   | skip     | repro — placental retention            | Placental retention vs foal-heat reproduction                |
| 24_31   | 3   | skip     | genetics-noperf — DRD4 temperament     | DRD4 gene breed differences                                  |
| 24_37   | 3   | skip     | other — Tanzania equines               | Domestic equines in Tanzania                                 |
| 24_47   | 3   | skip     | repro — progesterone assay             | PATHFAST progesterone immunoassay                            |
| 24_53   | 3   | skip     | vet-nonperf — Streptococcus detection  | S. equi detection in equines, India                          |
| 24_1308 | 4   | skip     | vet-nonperf — carbonic anhydrase feces | Fecal carbonic anhydrase as occult-blood marker              |
| 24_1310 | 4   | skip     | method — EHV-1 CF test                 | Complement-fixation test improvement for EHV-1               |
| 24_1311 | 4   | skip     | genetics-cons — Kiso reference values  | Haematological reference values, Kiso horse                  |

## Volume 23 (2012) — 5 examined, kept 0 (thin volume)

| docid | iss | decision | reason                            | title                                            |
| ----- | --- | -------- | --------------------------------- | ------------------------------------------------ |
| 23_1  | 1   | skip     | vet-nonperf — parasites           | Parasites of horses, Nigeria                     |
| 23_17 | 2   | skip     | vet-nonperf — influenza pathology | Pathological changes after influenza inoculation |
| 23_35 | 3   | skip     | repro — endometrial glands        | Endometrial gland density in mares               |
| 23_41 | 3   | skip     | vet-nonperf — hoof canker         | Treponeme canker maggot therapy                  |
| 23_47 | 4   | skip     | other — Turkey mules              | Livestock resources/mules in Turkey              |

## Volume 22 (2011) — 10 examined, kept 1

| docid   | iss | decision | reason                                 | title                                                        |
| ------- | --- | -------- | -------------------------------------- | ------------------------------------------------------------ |
| 22_2_21 | 2   | **kept** | perf — muscle free-radical/fibre shift | Free radical formation after intensive exercise in TB muscle |
| 22_1_1  | 1   | skip     | vet-nonperf — fetal ferritin proteins  | Ferritin-binding proteins in fetal plasma                    |
| 22_1_9  | 1   | skip     | repro — colt reproductive hormones     | Post-natal FSH/LH/etc. in colts                              |
| 22_1_17 | 1   | skip     | vet-nonperf — brucellosis              | Brucellosis serology, Nigeria                                |
| 22_2_29 | 2   | skip     | repro — stallion/gelding hormones      | Annual reproductive hormone changes                          |
| 22_2_37 | 2   | skip     | other — BW growth curve                | Growth curve sigmoid sub-functions (male BW)                 |
| 22_3_53 | 3   | skip     | method — EHV-1 PCR                     | PCR assay for EHV-1 respiratory disease                      |
| 22_3_57 | 3   | skip     | vet-nonperf — tendon cell biology      | Injury changes in tendon-recovered cells                     |
| 22_4_67 | 4   | skip     | genetics-cons — Kiso population        | Population statistics of endangered Kiso horse               |
| 22_4_73 | 4   | skip     | vet-nonperf — foal ferritin            | Ferritin-binding activity in foal serum                      |

## Volume 21 (2010) — 10 examined, kept 4

| docid   | iss | decision | reason                               | title                                                            |
| ------- | --- | -------- | ------------------------------------ | ---------------------------------------------------------------- |
| 21_2_11 | 2   | **kept** | perf — breeding structure/inbreeding | Demographic analysis of Japanese Thoroughbred breeding structure |
| 21_3_39 | 3   | **kept** | perf — gene-dropping pedigree        | Gene-dropping ancestral contributions & allele survival          |
| 21_4_59 | 4   | **kept** | perf — muscle fibre recruitment      | Muscle fibre recruitment, continuous vs interval exercise        |
| 21_4_73 | 4   | **kept** | perf — age × performance ★           | The effect of age on Thoroughbred racing performance             |
| 21_1_1  | 1   | skip     | repro — β-carotene mare              | β-carotene on trotter mare peripartum                            |
| 21_1_7  | 1   | skip     | vet-nonperf — ossifying fibroma      | Ossifying fibroma in racehorse maxilla                           |
| 21_2_17 | 2   | skip     | vet-nonperf — hip dislocation foal   | Hip dislocation from umbilical infection                         |
| 21_2_E1 | 2   | skip     | other — erratum                      | Erratum for 21_1_7                                               |
| 21_3_33 | 3   | skip     | vet-nonperf — strangles vaccination  | Strangles vaccine antibody levels                                |
| 21_4_67 | 4   | skip     | other — buyer segmentation           | Horse-buyer market segmentation                                  |

## Volume 20 (2009) — 10 examined, kept 2

| docid   | iss | decision | reason                                   | title                                              |
| ------- | --- | -------- | ---------------------------------------- | -------------------------------------------------- |
| 20_2_15 | 2   | **kept** | perf — clubfoot incidence/scratch risk   | Clubfoot (DDFTC) survey in Hidaka breeding area    |
| 20_3_33 | 3   | **kept** | perf — glycogen recovery 24h             | Glycogen & SR Ca²⁺-ATPase after intensive exercise |
| 20_1_1  | 1   | skip     | repro — sperm morphology                 | Sperm morphology in stallions                      |
| 20_1_7  | 1   | skip     | method — 13C breath test                 | 13C-phenylalanine/dipeptide breath tests           |
| 20_2_11 | 2   | skip     | method — cell line propagation           | Equine cell line for herpesvirus culture           |
| 20_2_19 | 2   | skip     | vet-nonperf — laryngeal hemiplegia draft | CCL repair system for laryngeal hemiplegia (draft) |
| 20_3_41 | 3   | skip     | repro — uterine hematoma                 | Postpartum broad-ligament hematoma                 |
| 20_4_59 | 4   | skip     | vet-nonperf — hematopoietic neoplasia    | Myelo/lymphoproliferative disorders review         |
| 20_4_73 | 4   | skip     | genetics-noperf — Palomino colour        | Parental coat colour & prize-winning palominos     |
| 20_4_79 | 4   | skip     | other — equine-assisted therapy          | Horseback riding for children with PDD             |

Wave 2 (Vols 27–20): 101 examined, 25 kept.

## Volume 19 (2008) — 12 examined, kept 3

| docid    | dec      | reason                                        | title                                                              |
| -------- | -------- | --------------------------------------------- | ------------------------------------------------------------------ |
| 19_2_25  | **kept** | perf — IAD prevalence/risk                    | Characteristics of inflammatory airway disease in JP Thoroughbreds |
| 19_4_83  | **kept** | perf — bone-turnover markers vs exercise      | Bone-metabolism markers vs exercise intensity                      |
| 19_4_97  | **kept** | perf — LRTD blood/endoscopy                   | Tracheal endoscopy & blood in LRTD-suspect racehorses              |
| 19_1_1   | skip     | repro/genetics-cons — Przewalski              | Reproduction/development of released Przewalski's horses           |
| 19_1_9   | skip     | vet-nonperf — therapeutic-riding conformation | Conformation vs rider oscillation                                  |
| 19_1_19  | skip     | other — foal handling behaviour               | Flight distance/avoidance in foals/yearlings                       |
| 19_2_31  | skip     | repro — follicular steroids                   | Intrafollicular steroid hormones in mares                          |
| 19_2_35  | skip     | repro — PMSG/hCG ovarian                      | PMSG/hCG for ovarian quiescence                                    |
| 19_3_53  | skip     | other — bedding behaviour                     | Bedding material & lying behaviour                                 |
| 19_3_57  | skip     | vet-nonperf — Skyros pony fracture case       | Distal epiphyseal metacarpal fracture (Skyros pony)                |
| 19_3_63  | skip     | other — influenza morbidity models            | Morbidity estimation models for influenza                          |
| 19_4_91  | skip     | vet-nonperf — interferon shipping fever       | Interferon-α on shipping fever                                     |
| 19_4_103 | skip     | method — collagen mRNA laminitis              | Collagen mRNA in regenerated laminae                               |

## Volume 18 (2007) — 18 examined, kept 5

| docid    | dec      | reason                                     | title                                                  |
| -------- | -------- | ------------------------------------------ | ------------------------------------------------------ |
| 18_1_5   | **kept** | perf — portable lactate (field)            | Simple lactate measurement, portable analyzer          |
| 18_2_47  | **kept** | perf — gait/stride biomechanics            | Running form of the Triple Crown winner (Deep Impact)  |
| 18_3_99  | **kept** | perf — young-TB bone mineral content       | BMC by radiographic absorptiometry, young Thoroughbred |
| 18_4_137 | **kept** | perf — NIRS pedal hemodynamics (weak)      | NIRS hemoglobin kinetics of pedal circulation          |
| 18_4_153 | **kept** | perf — HR/lactate during race              | Heart rates & blood lactate during a race              |
| 18_1_1   | skip     | vet-nonperf — lymphangitis mules           | Epizootic lymphangitis in cart mules                   |
| 18_1_13  | skip     | other — neuroanatomy                       | Nissl staining of amygdala                             |
| 18_1_27  | skip     | repro — neonatal ovaries                   | Inhibin/activin in neonatal ovaries                    |
| 18_1_33  | skip     | method — MDCT                              | Basic MDCT assessment                                  |
| 18_2_39  | skip     | other — pack-donkey shade                  | Shade provision for pack donkeys                       |
| 18_2_55  | skip     | vet-nonperf — melanoma MDCT                | MDCT of melanoma                                       |
| 18_2_59  | skip     | vet-nonperf — aortic thrombosis            | Aortic-iliac thrombosis pathology                      |
| 18_3_85  | skip     | repro — filly endocrine                    | FSH/LH/etc. in fillies 0–6 mo                          |
| 18_3_93  | skip     | repro — fetal adrenal                      | Inhibin/activin in fetal adrenals                      |
| 18_3_107 | skip     | repro — stallion hCG                       | hCG on testicular hormones                             |
| 18_3_117 | skip     | vet-nonperf — influenza vaccine            | Antibody responses to flu vaccine                      |
| 18_4_145 | skip     | genetics-cons — Mongolian markings         | Primitive markings in Mongolian/Przewalski             |
| 18_4_161 | skip     | vet-nonperf — transport pneumonia cytology | Tracheal aspirate cytology, transport pneumonia        |

## Volume 17 (2006) — 13 examined, kept 2

| docid    | dec      | reason                                    | title                                           |
| -------- | -------- | ----------------------------------------- | ----------------------------------------------- |
| 17_2_27  | **kept** | perf — fibrinogen respiratory severity    | Plasma fibrinogen in respiratory disorders      |
| 17_2_33  | **kept** | perf — laryngeal hemiplegia → performance | LH effect on racing performance + laryngoplasty |
| 17_1_1   | skip     | vet-nonperf — P-glycoprotein EMND         | P-glycoprotein in motor neuron disease          |
| 17_1_9   | skip     | other — jumping-horse digestibility       | Diet digestibility for jumping horses           |
| 17_1_17  | skip     | vet-nonperf — colic survey                | Acute abdomen in a breeding region              |
| 17_1_23  | skip     | vet-nonperf — EHV-1 neonatal              | EHV-1 DNA in dead foal organs                   |
| 17_2_39  | skip     | vet-nonperf — breath test GI              | H₂/CH₄ breath test, GI disease                  |
| 17_3_67  | skip     | vet-nonperf — uveitis IOP                 | Intraocular pressure in uveitis                 |
| 17_3_75  | skip     | repro — IGF mares                         | IGF around parturition                          |
| 17_4_97  | skip     | repro — S. zooepidemicus metritis         | Szp gene PCR-RFLP, metritis                     |
| 17_4_101 | skip     | method — S. equi PCR                      | Shuttle PCR for S. equi SeM                     |
| 17_4_105 | skip     | method — BMC absorptiometry               | BMC by radiographic absorptiometry (method)     |
| 17_4_113 | skip     | method — anti-ferritin autoantibody       | Horse anti-ferritin autoantibody                |

## Volume 16 (2005) — 14 examined, kept 1

| docid    | dec      | reason                                       | title                                                 |
| -------- | -------- | -------------------------------------------- | ----------------------------------------------------- |
| 16_1_1   | **kept** | perf — exercise blood markers (trotter)      | Rehydrating supplement & exercise markers in trotters |
| 16_1_11  | skip     | vet-nonperf — thyroid tumour                 | Thyroid tumour immunohistochemistry                   |
| 16_1_19  | skip     | other — yearling energy/region               | Breeding-region energy intake & growth                |
| 16_1_27  | skip     | vet-nonperf — disease survey                 | Equine disease prevalence, Nigeria                    |
| 16_2_29  | skip     | method — R. equi genotyping                  | Virulent Rhodococcus equi genotyping                  |
| 16_2_35  | skip     | vet-nonperf — rotavirus vaccine              | Inactivated rotavirus vaccine field study             |
| 16_2_45  | skip     | repro — granulosa tumour                     | Inhibin in granulosa-theca tumours                    |
| 16_3_67  | skip     | repro — xylazine ovulation                   | Tranquilization & ovulation timing                    |
| 16_3_73  | skip     | vet-nonperf — malignant hyperthermia case    | Halothane malignant-hyperthermia muscle pathology     |
| 16_3_79  | skip     | vet-nonperf — Ca/P ponies                    | Dietary Ca/P in ponies/donkeys                        |
| 16_4_99  | skip     | vet-nonperf — R. equi susceptibility         | Antimicrobial susceptibility, R. equi                 |
| 16_4_105 | skip     | vet-nonperf — ivermectin                     | Ivermectin efficacy vs GI parasites                   |
| 16_4_111 | skip     | vet-nonperf — Salmonella plasmid             | Salmonella Abortusequi virulence plasmid              |
| 16_4_117 | skip     | vet-nonperf — transport respiratory cytology | Tracheobronchial aspirate cytology, transport         |

## Volume 15 (2004) — 12 examined, kept 3

| docid    | dec      | reason                                  | title                                     |
| -------- | -------- | --------------------------------------- | ----------------------------------------- |
| 15_1_7   | **kept** | perf — erythrocyte KCC/haemolysis + SNP | K-Cl cotransport & erythrocyte fragility  |
| 15_3_61  | **kept** | perf — blood lactate intervals          | Lactate in repeated severe exercises      |
| 15_4_103 | **kept** | perf — bone density/sesamoid fracture   | Bone density & PSB fracture, Cyprus track |
| 15_1_1   | skip     | repro — semen extender                  | Semen preservation extender               |
| 15_2_31  | skip     | method — SP-A ELISA                     | Surfactant protein A ELISA                |
| 15_2_37  | skip     | vet-nonperf — anthelmintic              | Bithionol vs tapeworms                    |
| 15_3_67  | skip     | other — bedding behaviour               | Used bedding & lying behaviour            |
| 15_3_75  | skip     | vet-nonperf — R. equi soil              | Virulent R. equi in soil                  |
| 15_3_81  | skip     | vet-nonperf — mosapride GI              | Mosapride on digestive tract              |
| 15_4_85  | skip     | vet-nonperf — electrointestinography    | Electrointestinography for GI motility    |
| 15_4_93  | skip     | other — plasma amino acids              | Plasma AA vs dietary protein              |
| 15_4_99  | skip     | vet-nonperf — transport stress          | Stress responses to road transport        |

## Volume 14 (2003) — 14 examined, kept 6

| docid    | dec      | reason                                | title                                               |
| -------- | -------- | ------------------------------------- | --------------------------------------------------- |
| 14_1_1   | **kept** | perf — sweating-rate thermoregulation | Total vs neck sweating rate during exercise         |
| 14_1_13  | **kept** | perf — plasma K fitness biomarker     | Plasma K & systemic adaptation to effort            |
| 14_2_51  | **kept** | perf — early-training bone/endocrine  | Exercise on thyroid/parathyroid/bone in foals       |
| 14_3_81  | **kept** | perf — heritability of LH             | Heritability of laryngeal hemiplegia (Gibbs)        |
| 14_3_93  | **kept** | perf — submaximal HR → class/sex      | HR during submaximal exercise vs gender/performance |
| 14_3_97  | **kept** | perf — elite cardiac (T.M. Opera O)   | Heart size & HRV of top earner                      |
| 14_1_5   | skip     | vet-nonperf — endotoxemia             | Experimental endotoxemia                            |
| 14_2_37  | skip     | vet-nonperf — GI prokinetics          | Medicinal treatment of GI dysfunction               |
| 14_3_75  | skip     | other — equine diaper ammonia         | "Equine diaper" ammonia reduction                   |
| 14_3_87  | skip     | vet-nonperf — Salmonella foals        | Salmonella Typhimurium in foals                     |
| 14_4_101 | skip     | other — milk amino acids              | Diurnal milk AA                                     |
| 14_4_111 | skip     | method — influenza RT-PCR             | Real-time PCR for equine influenza                  |
| 14_4_119 | skip     | other — feed digestibility            | Digestibility of cereals/hays                       |
| 14_4_125 | skip     | other — treadmill rhythm method       | Movement rhythm analysis (horse+rider)              |

## Volume 13 (2002) — 18 examined, kept 6

| docid    | dec      | reason                                  | title                                        |
| -------- | -------- | --------------------------------------- | -------------------------------------------- |
| 13_1_9   | **kept** | perf — fracture × career starts/age/sex | Thyroid C cells at fractures (fracture risk) |
| 13_2_41  | **kept** | perf — JRA tendonitis review ★          | The Japanese experience with tendonitis      |
| 13_3_89  | **kept** | perf — haptoglobin/spleen → max speed   | Plasma haptoglobin & exercise (splenectomy)  |
| 13_4_109 | **kept** | perf — sweating/mineral loss (heat)     | Total sweating rate & mineral loss           |
| 13_4_113 | **kept** | perf — neck sweat = total-SR proxy      | Unit-area sweating rate across body areas    |
| 13_4_117 | **kept** | perf — V200 aerobic fitness (GPS+HR)    | Reliability of EquiPILOT for aerobic fitness |
| 13_1_1   | skip     | vet-nonperf — laminitis case            | Laminitis recovery case                      |
| 13_1_19  | skip     | other — digestibility method            | Chromic-oxide digestibility method           |
| 13_1_23  | skip     | other — roughage digestibility          | Roughage digestibility                       |
| 13_1_29  | skip     | vet-nonperf — pulmonary edema case      | Pulmonary edema case                         |
| 13_2_57  | skip     | genetics-noperf — DRD4                  | DRD4 sequence variability across Equus       |
| 13_2_63  | skip     | vet-nonperf — R. equi antibodies        | Isotype antibody responses to R. equi        |
| 13_2_71  | skip     | vet-nonperf — rotavirus serosurvey      | Rotavirus sero-epidemiology                  |
| 13_3_75  | skip     | repro — granulosa tumour removal        | Ovary removal in granulosa-theca tumour      |
| 13_3_83  | skip     | repro — pony ovulation hormones         | LH/FSH/etc. at ovulation control             |
| 13_3_93  | skip     | vet-nonperf — stringhalt                | Stringhalt outbreak                          |
| 13_4_101 | skip     | repro — oxytocin placental              | Oxytocin & placental retention               |
| 13_4_123 | skip     | vet-nonperf — thiopental anaesthesia    | Thiopental cardiovascular effects            |

## Volume 12 (2001) — 15 examined, kept 4

| docid    | dec      | reason                                         | title                                                    |
| -------- | -------- | ---------------------------------------------- | -------------------------------------------------------- |
| 12_1_1   | **kept** | perf — Banei body size/conformation → earnings | Body size, conformation & racing performance (Ban-ei)    |
| 12_1_17  | **kept** | perf — haematological periodicity              | Periodicities of blood parameters, athletic vs sedentary |
| 12_3_85  | **kept** | perf — SDFT hyperthermia/injury                | Exercise-induced SDFT hyperthermia & cooling             |
| 12_4_139 | **kept** | perf — bucked shin complex / gallop count      | Exercise intensity & bucked shin complex                 |
| 12_1_9   | skip     | repro — testicular fibrosis                    | Myoid cells in testicular fibrosis                       |
| 12_1_25  | skip     | repro — feto-placental enzymes                 | Steroidogenic enzymes in feto-placental unit             |
| 12_2_33  | skip     | vet-nonperf — stress hormones                  | Neuroendocrine response to novel stimuli                 |
| 12_2_39  | skip     | vet-nonperf — thyroid C cells                  | C-cell distribution vs age/sex                           |
| 12_2_47  | skip     | method — sugar fermentation                    | Microplate assay for S. equi                             |
| 12_2_51  | skip     | vet-nonperf — guttural pouch                   | Guttural pouch flush for abscesses                       |
| 12_3_77  | skip     | repro — ovarian follicle enzymes               | Inhibin/activin/aromatase in follicles                   |
| 12_4_119 | skip     | vet-nonperf — pituitary adenoma                | Pars intermedia adenomas (aged)                          |
| 12_4_127 | skip     | vet-nonperf — staphylococci                    | Staphylococci in nares/skin                              |
| 12_4_135 | skip     | vet-nonperf — transport serology               | Viral serology after transport                           |
| 12_4_145 | skip     | other — hay digestibility                      | Digestibility/chewing, native vs half-bred               |

Wave 3 (Vols 19–12): 116 examined, 30 kept.

## Volume 11 (2000) — 16 examined, kept 1

| docid    | dec      | reason                               | title                                             |
| -------- | -------- | ------------------------------------ | ------------------------------------------------- |
| 11_3_51  | **kept** | perf — erythrocyte fragility/fitness | Osmotic fragility of erythrocytes during exercise |
| 11_1_1   | skip     | vet-nonperf — R. equi foal deaths    | Dead foals with Rhodococcus equi pneumonia        |
| 11_1_7   | skip     | vet-nonperf — R. equi epidemiology   | Epidemiology of R. equi in foals                  |
| 11_1_15  | skip     | method — NO production cells         | NO production in cultured equine cells            |
| 11_1_19  | skip     | vet-nonperf — stable fungi           | Emericella nidulans in stables                    |
| 11_2_23  | skip     | vet-nonperf — R. equi soil           | Virulence plasmids in soil R. equi (Argentina)    |
| 11_2_29  | skip     | vet-nonperf — EHV-1 donkeys          | EHV-1 experimental infection in donkeys           |
| 11_2_35  | skip     | repro — granulosa tumour             | Endocrinology of granulosa-theca tumour           |
| 11_2_45  | skip     | other — native horse gut bacteria    | Fecal bacteria in Hokkaido native horses          |
| 11_3_63  | skip     | vet-nonperf — GI motility sedatives  | Jejunum/cecum reactivity to xylazine              |
| 11_3_69  | skip     | other — pasture intake               | Voluntary intake in woodland pasture              |
| 11_3_75  | skip     | other — melanoma cell lines          | Melanoma culture lines, gray horses               |
| 11_4_83  | skip     | other — rider biomechanics           | Head movement/EMG of riders                       |
| 11_4_91  | skip     | other — zebra behaviour              | Scent-marking in zebras                           |
| 11_4_99  | skip     | method — CYP17 gene structure        | Genomic organization of CYP17                     |
| 11_4_107 | skip     | vet-nonperf — AHS virus mice         | African horsesickness in mice                     |

## Volume 10 (1999) — 14 examined, kept 4

| docid     | dec      | reason                               | title                                                    |
| --------- | -------- | ------------------------------------ | -------------------------------------------------------- |
| 10_2_45   | **kept** | perf — uric acid/allantoin oxidative | Uric acid & allantoin after exhaustive exercise          |
| 10_3,4_53 | **kept** | perf — research-agenda review        | A view of equine science development in the 21st century |
| 10_3,4_61 | **kept** | perf — splenectomy → −14% speed      | Splenectomy & RBC fragility during exercise              |
| 10_3,4_67 | **kept** | perf — exercise RBC density/PCV      | Exercise on RBC density (normal vs splenectomized)       |
| 10_1_1    | skip     | vet-nonperf — white line disease     | White line disease, low Zn/Cu diet                       |
| 10_1_7    | skip     | vet-nonperf — immune-organ cells     | Immunocompetent cells in immuno-organs                   |
| 10_1_13   | skip     | genetics-cons — ELA-DRB              | ELA-DRB exon 2 polymorphism                              |
| 10_1_17   | skip     | vet-nonperf — Cushing's pony         | Cushing's disease in a pony                              |
| 10_1_21   | skip     | repro — placental proline            | Proline uptake in placental vesicles                     |
| 10_2_27   | skip     | vet-nonperf — lymphoid cells         | Immunocompetent cells in lymphoid tissue                 |
| 10_2_33   | skip     | vet-nonperf — feed deprivation       | Feed deprivation biochemistry in equids                  |
| 10_2_39   | skip     | vet-nonperf — lymphocyte cycle       | Lymphocyte cell division cycles                          |
| 10_2_49   | skip     | method — guttural pouch survey       | Guttural pouch lavage microbiology                       |
| 10_3,4_73 | skip     | method — S. aureus PFGE              | Molecular epidemiology of S. aureus                      |

## Volume 9 (1998) — 20 examined, kept 6

| docid   | dec      | reason                                 | title                                             |
| ------- | -------- | -------------------------------------- | ------------------------------------------------- |
| 9_1_1   | **kept** | perf — SET fitness indices (VLA4/V200) | Standardized exercise tests by fitness level      |
| 9_1_9   | **kept** | perf — catecholamine/ACTH vs exercise  | CA/ACTH/cortisol responses to treadmill exercise  |
| 9_2_33  | **kept** | perf — caffeine VO₂max                 | Caffeine effects on performance/cardiorespiratory |
| 9_3_77  | **kept** | perf — chronic stress/overtraining     | Stress in the racing horse: coping vs not coping  |
| 9_3_89  | **kept** | perf — sire BV turf/dirt ★             | Sire breeding values for rating, turf vs dirt     |
| 9_4_107 | **kept** | perf — splenic RBC/lactate fragility   | Splenic RBC & lactate vs osmotic fragility        |
| 9_1_19  | skip     | vet-nonperf — EAV epitope              | EAV GL protein epitope                            |
| 9_1_25  | skip     | repro — EHV-1 mouse parturition        | EHV-1 on mouse parturition                        |
| 9_1_29  | skip     | other — hay digestibility              | Exercise vs hay energy digestibility              |
| 9_2_45  | skip     | repro — 3β-HSD cloning                 | Testicular 3β-HSD mRNA                            |
| 9_2_53  | skip     | genetics-cons — phylogenetics          | Japanese native/alien horse protein polymorphisms |
| 9_2_71  | skip     | vet-nonperf — tibial fracture case     | Double DCP fixation, spiral tibial fracture       |
| 9_3_83  | skip     | repro — mare fertility                 | Managing the mare for fertility                   |
| 9_3_93  | skip     | vet-nonperf — glanders                 | Clinico-microbiology of glanders                  |
| 9_3_97  | skip     | method — satellite cells               | Isolation of muscle satellite cells               |
| 9_4_101 | skip     | repro — acrosomal reaction             | Percoll/GAG on stallion acrosome                  |
| 9_4_113 | skip     | method — transferrin subgroups         | Transferrin subgrouping by mAbs                   |
| 9_4_119 | skip     | vet-nonperf — leptospirosis            | Equine leptospirosis prevalence                   |
| 9_4_125 | skip     | repro — insemination timing            | Insemination timing via estrone-sulphate          |

## Volume 8 (1997) — 20 examined, kept 8

| docid   | dec      | reason                              | title                                                  |
| ------- | -------- | ----------------------------------- | ------------------------------------------------------ |
| 8_1_21  | **kept** | perf — breaking-period VO₂max       | Low-intensity breaking-period cardiopulmonary function |
| 8_2_35  | **kept** | perf — AV conduction/hemodynamics   | AV conduction & hemodynamics during atrial pacing      |
| 8_2_39  | **kept** | perf — atrial fibrillation          | ECG of legendary racehorse Shinzan (AF review)         |
| 8_2_43  | **kept** | perf — breed forelimb kinematics    | Forelimb kinematics, Warmblood vs Andalusian           |
| 8_2_49  | **kept** | perf — sinker laminitis post-race   | Sinker laminitis after race & transport                |
| 8_3_75  | **kept** | perf — training intensity VO₂max    | Training intensity & cardiopulmonary function (2yo)    |
| 8_3_81  | **kept** | perf — circadian catecholamines     | Circadian adrenaline/noradrenaline                     |
| 8_4_113 | **kept** | perf — leukocyte glycolytic enzymes | Glycolytic enzymes in leukocytes during training       |
| 8_1_1   | skip     | vet-nonperf — influenza vaccine     | Influenza vaccine with recent strain                   |
| 8_1_7   | skip     | vet-nonperf — EAV antigen           | Neutralization sites in EAV GL protein                 |
| 8_1_13  | skip     | method — c-ski gene                 | c-ski gene expression in tissues                       |
| 8_1_25  | skip     | method — JEV ELISA                  | ELISA for Japanese encephalitis virus                  |
| 8_2_29  | skip     | method — EAV RT-PCR                 | RT-PCR for equine arteritis virus                      |
| 8_3_57  | skip     | method — EHV-1 ELISA                | ELISA for EHV-1 antibody                               |
| 8_3_63  | skip     | method — IgG nephelometry           | Nephelometry for horse IgG                             |
| 8_3_69  | skip     | method — blood-group mAbs           | mAbs to blood group antigens                           |
| 8_4_89  | skip     | vet-nonperf — dermatosis isolation  | Fungal/bacterial isolation, dermatosis                 |
| 8_4_95  | skip     | method — cell culture               | Primary cell culture method                            |
| 8_4_101 | skip     | vet-nonperf — priapism case         | Priapism from spinal nematodiasis                      |
| 8_4_109 | skip     | repro — superoxide scavenging       | Superoxide scavenging in mares/foals at delivery       |

## Volume 7 (1996) — 11 examined, kept 3

| docid  | dec      | reason                              | title                                             |
| ------ | -------- | ----------------------------------- | ------------------------------------------------- |
| 7_2_35 | **kept** | perf — erythrocyte/plasma lactate   | Erythrocyte vs plasma lactate during exercise     |
| 7_3_51 | **kept** | perf — soft-tissue lesions × career | Soft-tissue morbid anatomy in fractured forelimbs |
| 7_4_93 | **kept** | perf — McIII BMD × career           | McIII bone mineral density by DEXA                |
| 7_1_1  | skip     | repro — transport LH/FSH            | Transport stress on LH/FSH in mares               |
| 7_1_7  | skip     | vet-nonperf — MRSA metritis         | MRSA from mares with metritis                     |
| 7_1_13 | skip     | vet-nonperf — fetal pneumonia       | Interstitial pneumonia in aborted fetus           |
| 7_1_17 | skip     | method — rotavirus VP7              | Rotavirus VP7 gene                                |
| 7_2_21 | skip     | other — transport behaviour         | Behaviour of untethered horses in transport       |
| 7_2_27 | skip     | method — hoof histology stain       | Hoof ground-section staining                      |
| 7_2_43 | skip     | vet-nonperf — NGF shipping fever    | NGF in shipping-fever horses                      |
| 7_2_47 | skip     | vet-nonperf — anaesthesia case      | Sevoflurane for fracture fixation                 |
| 7_3_55 | skip     | vet-nonperf — soft palate cysts     | Rostral soft palate abnormalities                 |
| 7_3_59 | skip     | repro — ovarian artery              | Intraovarian artery morphology                    |
| 7_3_63 | skip     | method — MTT blastogenesis          | MTT lymphocyte blastogenesis assay                |
| 7_4_67 | skip     | vet-nonperf — piroplasmosis         | Equine piroplasmosis review                       |
| 7_4_79 | skip     | method — EHV-1 ORF                  | EHV-1 ORF nucleotide sequences                    |
| 7_4_89 | skip     | repro — hysteroscopy                | Hysteroscopy in subfertile mares                  |

## Volume 6 (1995) — 17 examined, kept 8

| docid   | dec      | reason                                 | title                                          |
| ------- | -------- | -------------------------------------- | ---------------------------------------------- |
| 6_1_1   | **kept** | perf — LSD training aerobic            | Long slow distance training & aerobic capacity |
| 6_1_25  | **kept** | perf — transport → respiratory disease | Epidemiology of transport respiratory disease  |
| 6_2_31  | **kept** | perf(uncertain) — DNA-analysis review  | Recent studies on DNA analysis in the horse    |
| 6_2_55  | **kept** | perf — incline cardiopulmonary         | Effects of incline on cardiopulmonary function |
| 6_2_61  | **kept** | perf — RBC density/fitness markers     | Density-separated Thoroughbred erythrocytes    |
| 6_3_79  | **kept** | perf — ECG training adaptation         | ECG changes induced by training (Andalusian)   |
| 6_3_91  | **kept** | perf — diurnal blood variation         | Diurnal variations of blood constituents       |
| 6_4_135 | **kept** | perf — transport respiratory rate      | Respiratory changes during transport           |
| 6_1_7   | skip     | repro — PGF2α estrus                   | PGF2α estrus induction                         |
| 6_1_15  | skip     | method — EIA ELISA                     | ELISA screening for equine infectious anaemia  |
| 6_1_21  | skip     | other — colostrum minerals             | Colostrum mineral concentrations               |
| 6_2_67  | skip     | other — literary analysis              | "Why did Alan blind six horses in Equus?"      |
| 6_3_73  | skip     | vet-nonperf — influenza antigens       | H3 influenza antigenic relationships           |
| 6_3_99  | skip     | vet-nonperf — ammonia trachea          | Atmospheric ammonia tracheal effects           |
| 6_4_105 | skip     | vet-nonperf — R. equi foals            | Rhodococcus equi infection in foals            |
| 6_4_121 | skip     | other — native horse nutrition         | Sasa nipponica intake in Hokkaido horses       |
| 6_4_127 | skip     | other — riding garment posture         | Human body posture for riding garments         |

## Volume 5 (1994) — 22 examined, kept 3 (oldest volume; Vols 1–4 absent from J-STAGE)

| docid   | dec      | reason                                      | title                                                                    |
| ------- | -------- | ------------------------------------------- | ------------------------------------------------------------------------ |
| 5_2_53  | **kept** | perf — JRA racing-times genetic+env model ★ | Genetic evaluation of sires & environmental factors on best racing times |
| 5_3_83  | **kept** | perf — RR-interval/HRV vs training          | Diurnal rhythms of R-R interval in young Thoroughbreds                   |
| 5_4_127 | **kept** | perf — start-dash stride mechanics ★        | Stride length/frequency/velocity at the start dash                       |
| 5_1_1   | skip     | vet-nonperf — EIA review                    | Equine infectious anaemia                                                |
| 5_1_21  | skip     | vet-nonperf — R. equi plasmids              | R. equi virulence plasmids on farms                                      |
| 5_1_27  | skip     | vet-nonperf — antibody titres               | JE/Getah/influenza antibody surveillance                                 |
| 5_1_33  | skip     | method — PCR cloning                        | PCR cloning/sequencing technique                                         |
| 5_1_37  | skip     | method — hoof histology                     | Resin-embedded hoof sections                                             |
| 5_1_41  | skip     | repro — uterine histopath                   | Uterine histopathology, arteritis virus                                  |
| 5_1_45  | skip     | vet-nonperf — EVA carrier                   | Equine viral arteritis carrier state                                     |
| 5_1_49  | skip     | vet-nonperf — parasites necropsy            | Internal parasite prevalence at necropsy                                 |
| 5_1_e1  | skip     | other — erratum                             | Erratum                                                                  |
| 5_2_59  | skip     | vet-nonperf — EHV-1 epizootiology           | EHV-1 sero/molecular study                                               |
| 5_2_69  | skip     | method — HR data-logger                     | Ambulatory pasture HR logger                                             |
| 5_2_73  | skip     | vet-nonperf — subepiglottic cyst            | Subepiglottic cyst removal case                                          |
| 5_3_77  | skip     | repro — IVF breed effects                   | In vitro fertilization breed effects                                     |
| 5_3_87  | skip     | vet-nonperf — dobutamine                    | Dobutamine in anaesthetized horses                                       |
| 5_3_95  | skip     | other — cervical vertebra morphometry       | Cervical vertebrae in foals (anatomy)                                    |
| 5_4_101 | skip     | vet-nonperf — EVA review                    | Equine viral arteritis control                                           |
| 5_4_115 | skip     | genetics-cons — DNA fingerprinting          | Breed differentiation of native horses                                   |
| 5_4_121 | skip     | vet-nonperf — R. equi antibodies            | Antibody response to R. equi                                             |
| 5_4_131 | skip     | other — RR-interval feeding                 | Resting R-R interval & feeding behaviour                                 |

Wave 4 (Vols 11–5): 120 examined, 33 kept.

---

**FINAL TOTAL: ~490 articles examined across Vols 5–37 (1994–2026); 119 kept.** Vols 1–4 do not exist on J-STAGE.

---

## Monitoring log (checks for newly-published papers)

Back-catalog is complete; subsequent runs only add genuinely new articles. Newest processed = Vol 37 Issue 1 (released 2026-03-14).

| date (JST) | newest issue found | Vol 38? | advance-pub? | new kept | note                                                     |
| ---------- | ------------------ | ------- | ------------ | -------- | -------------------------------------------------------- |
| 2026-06-10 | Vol 37 Issue 1     | no      | none         | 0        | Vol 37 Issue 2 → HTTP 500 (not yet published); no change |
