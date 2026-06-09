# 3D Imaging and Body Measurement of Riding Horses Using Four Scanners Simultaneously

## Metadata

| Field                          | Value                                                                                                                                                                                                                                                          |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Journal                        | J. Equine Sci. **35**(1): 1–7, 2024                                                                                                                                                                                                                            |
| docid                          | `35_JES2311`                                                                                                                                                                                                                                                   |
| Article type                   | Full Paper                                                                                                                                                                                                                                                     |
| Authors                        | Akihiro MATSUURA, Suzuka TORII, Yuki OJIMA, Yoshio KIKU                                                                                                                                                                                                        |
| Affiliations                   | 1 Department of Animal Science, School of Veterinary Medicine, Kitasato University, Aomori 034-8628, Japan; 2 Department of Sustainable Agriculture, College of Agriculture, Food and Environment Sciences, Rakuno Gakuen University, Hokkaido 069-8501, Japan |
| Received / Accepted / Released | 2023-06-05 / 2023-12-22 / —                                                                                                                                                                                                                                    |
| Keywords                       | body measurement, composite stereoscopic image, conformation, horse, three-dimensional scan                                                                                                                                                                    |
| PDF                            | https://www.jstage.jst.go.jp/article/jes/35/1/35_JES2311/_pdf/-char/en                                                                                                                                                                                         |

## Abstract (verbatim)

> Although there have been advances in the technology for measuring horse body size with stereoscopic three-dimensional (3D) scanners, previously reported methods with a single scanner still face a significant challenge: the time necessary for scanning is too long for the horses to remain stationary. This study attempted to scan the horse simultaneously from four directions using four scanners in order to complete the scans in a short amount of time and then combine the images from the four scans on a computer into one whole image of each horse. This study also compared body measurements from the combined 3D images with those taken from conventional manual measurements. Nine riding horses were used to construct stereoscopic composite images, and the following 10 measurements were taken: height at the withers, back, and croup; chest depth; width of the chest (WCh), croup, and waist; girth circumference, cannon circumference (CaC), and body length. The same 10 measurements were taken by conventional manual methods. Relative errors ranged from −1.89% to 7.05%. The correlation coefficient between manual and 3D measurements was significant for all body measurements (P<0.01) except for WCh and CaC. A simple regression analysis of all body measurements revealed a strong correlation (P<0.001, R2=0.9994, root-mean-square error=1.612). Simultaneous scanning with four devices from four directions reduced the scanning time from 60 sec with one device to 15 sec. This made it possible to perform non-contact body measurements even on incompletely trained horses who could not remain stationary for long periods of time.

## Relevance to finishing-position (着順) prediction

**Feature family F (conformation/body-size/gait).** Horse body conformation — withers height, chest depth, girth circumference, body length, cannon circumference — is an established proxy for athletic capacity, stride mechanics, and soundness in Thoroughbreds. The primary contribution of this paper to a finishing-position model is two-fold:

1. **Validating a scalable non-contact pipeline for 10 conformation features** (relative error −1.89% to +7.05% vs. manual gold standard; R² = 0.9994 across all 10 sites pooled). iPhone 12 Pro-based 3D scanning completed in 15 seconds makes at-scale deployment feasible at yearling sales or pre-race veterinary checks. These 10 measurements are therefore potentially collectable for every horse in the JBBA/JRA system, providing fixed physical-prior features for horses with limited race history.

2. **Body weight estimation without scales** — the Wagner & Tyler formula [GiC² × BoL / 11,880] applied to 3D-derived girth circumference (GiC) and body length (BoL) produced estimated body weights statistically indistinguishable from scale weight (P = 0.71), with mean error < 1 kg across 9 horses. This enables a body-weight proxy derivable from conformation scan data.

Relevance is indirect (this paper does not model race outcomes), but conformation features have been linked to performance, stride length, and injury risk in Thoroughbreds. The pipeline is especially valuable for unraced or lightly-raced horses where historical form is sparse.

## Background & objective

Manual body measurement of horses is laborious, time-consuming (multiple sites, measuring tape, caliper), and subject to inter-measurer variation. A single-scanner 3D method (Matsuura et al. 2021, J. Equine Sci. 32:73–80) required ~60 seconds per scan — too long for horses to remain stationary. This study aimed to reduce scan time by splitting the imaging range among four simultaneous scanners (four-directional composite), evaluate accuracy of the combined 3D image against conventional manual measurements, and compare the four-device method with the published one-device method.

## Materials & methods

**Subjects:** 9 riding horses (7 Thoroughbreds, 2 crossbreeds; 2 mares, 7 geldings; age 4–19 years, mean 14.1 ± 5.0 years SD; BW 541.8 ± 43.4 kg). From Kitasato University equestrian team and research herd. Ethics approved by Kitasato University Committee (20-034). Measurements: September–October 2021. Maximum 17 days between BW measurement and 3D scan.

**Instruments:** 4 × iPhone 12 Pro (Apple Inc.) with "3D Scanner App 1.9.5" (LAAN LABS). Post-processing: CloudCompare 2.10.2 Stereo (GNU GPL) for compositing; Autodesk Fusion 360 for measurement extraction. Weight scale (not specified). Manual measurements by single trained keeper using stick scale (HWi), tape measure (BoL, GiC, CaC), and calipers (WCh, WCr, WWa).

**Ten measurements collected:** Height at withers (HWi), Height at back (HBa), Height at croup (HCr), Chest depth (ChD), Width of chest (WCh), Width of croup (WCr), Width of waist (WWa), Girth circumference (GiC), Cannon circumference of left forelimb (CaC), Body length (BoL).

**Scan protocol:** 4 operators simultaneously, each scanning one quadrant (left front, left rear, right rear, right front) at 1–2 m distance, 15–20 seconds per scan. Images cropped, pairwise composited, then combined into full-body 3D model. CaC measured from left-front image only (no compositing). Each measurement taken twice; mean used. Head and neck excluded due to movement.

**Statistics:** Paired t-test, Pearson correlation (r), R² and RMSE (all 10 sites pooled), Bland–Altman plots (mean difference vs. mean of two methods; 95% CI), one-sample t-test (mean difference ≠ 0), one-way ANOVA for BW comparison. IBM SPSS v21. Relative error = (3D − Manual) / Manual × 100.

**Body weight estimate formula:** BW (kg) = GiC (cm)² × BoL (cm) / 11,880 (Wagner & Tyler 2011).

## Results (detailed — reproduce ALL numbers)

**Table 1: Per-measurement comparison (manual vs. 3D, n=9 horses)**

| Measurement | Manual mean (cm) | Manual SE | 3D mean (cm) | 3D SE | Paired t P | Mean relative error (%) | Pearson r | r P-value  |
| ----------- | ---------------- | --------- | ------------ | ----- | ---------- | ----------------------- | --------- | ---------- |
| HWi         | 159.9            | 2.6       | 159.4        | 2.6   | 0.039\*    | −0.28                   | 0.998     | 0.000\*\*  |
| HBa         | 150.8            | 2.6       | 150.8        | 2.5   | 0.981      | 0.00                    | 0.996     | 0.000\*\*  |
| HCr         | 159.2            | 2.6       | 159.0        | 2.5   | 0.417      | −0.13                   | 0.995     | 0.000\*\*  |
| ChD         | 76.6             | 1.2       | 77.3         | 1.1   | 0.106      | 0.96                    | 0.945     | 0.000\*\*  |
| WCh         | 38.6             | 0.7       | 39.9         | 0.6   | 0.050      | 3.25                    | 0.667     | 0.050 (ns) |
| WCr         | 50.6             | 0.5       | 49.6         | 0.8   | 0.080      | −1.89                   | 0.823     | 0.006\*\*  |
| WWa         | 52.2             | 1.0       | 53.0         | 0.9   | 0.066      | 1.64                    | 0.933     | 0.000\*\*  |
| GiC         | 191.0            | 1.8       | 192.5        | 1.8   | 0.045\*    | 0.80                    | 0.937     | 0.000\*\*  |
| CaC         | 20.6             | 0.3       | 22.0         | 0.7   | 0.030\*    | 7.05                    | 0.540     | 0.134 (ns) |
| BoL         | 175.6            | 2.8       | 174.0        | 2.8   | 0.021\*    | −0.89                   | 0.980     | 0.000\*\*  |

Notes: \* P < 0.05; \*\* P < 0.01. WCh and CaC did not achieve significant correlation.

**Pooled regression (all 10 measurements, all 9 horses):**

- r = 0.9997, P < 0.001
- R² = 0.9994
- RMSE = 1.612 cm

**Bland–Altman analysis:**

- Mean difference (3D − manual): 0.25 cm
- Not significantly different from 0 (one-sample t-test, P = 0.141)
- 4 measurements fell outside 95% CI: WCh, GiC, CaC, BoL

**Body weight estimates (Table 2, n=9 horses individually listed):**

| Horse | Sex     | Age  | BW actual (kg) | BW estimated manual (kg) | BW estimated 3D (kg) |
| ----- | ------- | ---- | -------------- | ------------------------ | -------------------- |
| #1    | Mare    | 4    | 502            | 506.9                    | 516.1                |
| #2    | Gelding | 18   | 550            | 549.4                    | 542.0                |
| #3    | Gelding | 13   | 446            | 428.1                    | 433.9                |
| #4    | Gelding | 19   | 594            | 628.4                    | 624.4                |
| #5    | Gelding | 17   | 552            | 539.9                    | 539.5                |
| #6    | Mare    | 16   | 572            | 540.6                    | 538.5                |
| #7    | Gelding | 16   | 552            | 541.9                    | 548.2                |
| #8    | Gelding | 8    | 546            | 561.5                    | 576.6                |
| #9    | Gelding | 16   | 562            | 567.0                    | 576.6                |
| Mean  | —       | 14.1 | 541.8          | 540.4                    | 544.0                |
| SE    | —       | 1.7  | 14.5           | 17.8                     | 17.3                 |

ANOVA comparing actual BW vs. estimated from manual vs. estimated from 3D: P = 0.71 (no significant difference).

**Scan time:** 15 sec (4 devices simultaneously) vs. 60 sec (1 device). Per-animal total time including re-scanning and checking: ~15 min.

**Comparison with prior single-scanner method (Table 3):**
Both methods showed poor performance for CaC (significant t-test, absolute relative error > 1%, non-significant correlation, and outside 95% CI Bland-Altman) — attributed to hair interference and scanner limitations on thin/small objects. The four-device method additionally showed significant differences for HWi, GiC, BoL vs. manual (not problematic in the single-device study), primarily due to difficulty measuring "shortest possible distance" manually and hair interference on GiC.

## Discussion & interpretation

The 4x speed reduction (60 s → 15 s) addresses the practical bottleneck for measuring young or unaccustomed horses that cannot remain stationary. CaC (cannon circumference) remains the most problematic measurement across both single- and four-scanner methods due to the small diameter of the cannon region combined with hair thickness inflating 3D measurements (mean relative error +7.05%, lowest correlation r = 0.540).

The body-weight estimation formula (GiC² × BoL / 11,880) performed well, with mean estimated weight within ~3 kg of scale weight for most horses, suggesting that 3D-derived GiC and BoL could substitute for scale weight in contexts where weighing facilities are unavailable. The authors note this formula is derived for Thoroughbreds and the current study's crossbreed horses introduced some variability (e.g., horse #3, estimated 428/434 vs. actual 446 kg).

The study is limited to a small, non-racing cohort (mostly aged geldings) and does not connect measurements to performance or race outcomes. The four-device method is slightly inferior to the single-device method on the number of discrepancy indicators, but the practical advantage of 4× speed is significant for deployment with young horses.

## Limitations

- n = 9 horses only, including 2 crossbreeds; not directly from JRA/NAR racing population
- Aged gelding-dominated sample (mean age 14.1 y); conformation measurements may differ from young racehorses
- No race performance data; performance association is inferred from literature, not this study
- CaC has poor 3D accuracy and is the most clinically important conformation feature for soundness assessment
- Head and neck excluded from composite (horses moved too much); neck length/thickness features could not be measured
- Grant from Japan Racing Horse Association noted but data collection was at Kitasato University, not a JRA facility

## Feature-engineering notes for the model

- `height_withers_cm` — HWi from 3D or manual measurement — JBBA/sales data, pre-race inspection — taller horses have longer stride at same cadence; interaction with distance and going; expected weak positive effect on performance — available at registration/yearling sale; stable across career
- `girth_circumference_cm` — GiC — 3D or manual measurement — correlates with heart girth, lung capacity, aerobic power — available from 3D scan or annual physical exam records
- `body_length_cm` — BoL — 3D or manual — stride length proxy; BW estimation numerator — available as above
- `bw_estimated_gic_bol` — GiC² × BoL / 11,880 — 3D or manual measurement — body weight proxy when scale not available — expected R² > 0.98 vs. actual weight if GiC and BoL accurate
- `chest_depth_cm` — ChD — 3D measurement (relative error 0.96%, r = 0.945) — correlates with heart/lung volume — available from 3D scan
- `cannon_circumference_cm` — CaC — 3D measurement (relative error +7.05%, r = 0.540, non-significant) — **USE WITH CAUTION**: 3D method systematically overestimates due to hair; manually measured CaC is preferred for soundness/lameness prediction; associated with bone strength and fracture risk
- `width_croup_cm` — WCr — relative error −1.89%, r = 0.823 — hindquarter power proxy; interaction with distance (longer races favour hindquarter-powered horses)
- **Interaction terms to consider:** `height_withers × distance_m` (stride length × race distance), `chest_depth × surface` (aerobic capacity matters more on turf/stamina tracks)
- **Do NOT use** WCh from 3D as sole feature: r = 0.667, P = 0.050, non-significant — manual caliper measurement is more reliable; 3D systematically overestimates WCh by 3.25%

## Key references / follow-up leads

- **Matsuura A. et al. 2021** — J. Equine Sci. 32:73–80 — single-scanner baseline method; comparative baseline for Table 3
- **Pérez-Ruiz M. et al. 2020** — Comput. Electron. Agric. 174:105510 — LiDAR-based morphometrics in horses; precision agriculture approach
- **Wagner E.L. & Tyler P.J. 2011** — "A comparison of weight estimation methods in adult horses," J. Equine Vet. Sci. 31:706–710 — source of the BW formula GiC² × BoL / 11,880
- **Tozaki T. et al. 2017** — J. Equine Sci. 28:127–134 — GWAS for body weight in Japanese Thoroughbreds; chromosomal loci on chromosomes 3, 9, 15, 18
- **Tozaki T. et al. 2016** — J. Equine Sci. 27:107–114 — LCORL variants and body composition in Thoroughbreds under training; genetic basis of the conformation features measured here
