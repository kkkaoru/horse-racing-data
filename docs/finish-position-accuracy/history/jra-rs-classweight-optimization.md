# JRA Running-Style Class-Weight Optimization

Date: 2026-06-13

## Objective

Nige is OVER-predicted (precision ~0.33, 3700 predicted vs 3093 true in 2024).
Goal: find per-class weight multipliers that improve macro-F1 without sacrificing
overall accuracy by more than 0.3pp.

## Setup

- Train/eval splits: tune (2006-2021 / 2022), full (2006-2023 / 2024, 2006-2024 / 2025)
- Features: 157 (production feature set minus 14 missing columns)
- Hyperparameters: production v3 (num_leaves=63, lr=0.05, num_iter=3000, ES=100)
- Leak guard: `rs_p_*` excluded

## Step 1-2: Tuning Results (2022 holdout)

| Weight Vector         | macro-F1 | Accuracy | nige-P | nige-R | nige-F1 |
| --------------------- | -------- | -------- | ------ | ------ | ------- |
| balanced2             | 0.4837   | 0.4973   | 0.382  | 0.446  | 0.411   |
| uniform               | 0.4796   | 0.5130   | 0.499  | 0.262  | 0.344   |
| nige_0.5              | 0.4786   | 0.4981   | 0.414  | 0.384  | 0.398   |
| nige_0.65             | 0.4779   | 0.4940   | 0.380  | 0.436  | 0.406   |
| oikomi_0.8            | 0.4762   | 0.4883   | 0.349  | 0.488  | 0.407   |
| inverse_freq_baseline | 0.4760   | 0.4907   | 0.350  | 0.490  | 0.409   |
| nige_0.8              | 0.4754   | 0.4909   | 0.360  | 0.465  | 0.406   |
| nige_1.5              | 0.4652   | 0.4782   | 0.314  | 0.574  | 0.406   |

Baseline (inverse_freq) macro-F1 on tuning split: 0.4760

Top candidates selected for full eval: ['inverse_freq_baseline', 'balanced2', 'uniform']

## Step 3: Full Multi-Year Evaluation

### inverse_freq_baseline

#### 2024 (train 2006-2023)

- Accuracy: 0.4890 (Δ+0.0000 vs baseline)
- macro-F1: 0.4738 (Δ+0.0000 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.342     | 0.497  | 0.405 | +0.000          |
| senkou | 0.456     | 0.467  | 0.462 | +0.000          |
| sashi  | 0.497     | 0.374  | 0.427 | +0.000          |
| oikomi | 0.564     | 0.645  | 0.602 | +0.000          |

#### 2025 (train 2006-2024)

- Accuracy: 0.4828 (Δ+0.0000 vs baseline)
- macro-F1: 0.4668 (Δ+0.0000 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.319     | 0.508  | 0.392 | +0.000          |
| senkou | 0.453     | 0.466  | 0.459 | +0.000          |
| sashi  | 0.493     | 0.360  | 0.416 | +0.000          |
| oikomi | 0.565     | 0.640  | 0.600 | +0.000          |

### balanced2

#### 2024 (train 2006-2023)

- Accuracy: 0.4973 (Δ+0.0083 vs baseline)
- macro-F1: 0.4831 (Δ+0.0093 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.375     | 0.447  | 0.408 | +0.003          |
| senkou | 0.453     | 0.521  | 0.485 | +0.023          |
| sashi  | 0.492     | 0.405  | 0.444 | +0.017          |
| oikomi | 0.589     | 0.602  | 0.595 | -0.006          |

#### 2025 (train 2006-2024)

- Accuracy: 0.4976 (Δ+0.0149 vs baseline)
- macro-F1: 0.4796 (Δ+0.0127 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.373     | 0.400  | 0.386 | -0.006          |
| senkou | 0.454     | 0.540  | 0.493 | +0.034          |
| sashi  | 0.489     | 0.414  | 0.449 | +0.033          |
| oikomi | 0.591     | 0.590  | 0.591 | -0.010          |

### uniform

#### 2024 (train 2006-2023)

- Accuracy: 0.5136 (Δ+0.0246 vs baseline)
- macro-F1: 0.4785 (Δ+0.0047 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.500     | 0.254  | 0.336 | -0.068          |
| senkou | 0.491     | 0.467  | 0.479 | +0.017          |
| sashi  | 0.473     | 0.542  | 0.505 | +0.078          |
| oikomi | 0.590     | 0.598  | 0.594 | -0.008          |

#### 2025 (train 2006-2024)

- Accuracy: 0.5101 (Δ+0.0273 vs baseline)
- macro-F1: 0.4695 (Δ+0.0027 vs baseline)

| Class  | Precision | Recall | F1    | ΔF1 vs baseline |
| ------ | --------- | ------ | ----- | --------------- |
| nige   | 0.469     | 0.226  | 0.305 | -0.087          |
| senkou | 0.485     | 0.476  | 0.480 | +0.021          |
| sashi  | 0.471     | 0.537  | 0.502 | +0.086          |
| oikomi | 0.592     | 0.589  | 0.591 | -0.010          |

## Step 4: Calibration Comparison

Best candidate for calibration analysis: **balanced2**

### 2025 — new weights + new calibrators (fitted on 2024 preds)

| Class  | Precision | Recall | F1    |
| ------ | --------- | ------ | ----- |
| nige   | 0.444     | 0.276  | 0.340 |
| senkou | 0.473     | 0.498  | 0.485 |
| sashi  | 0.466     | 0.531  | 0.496 |
| oikomi | 0.608     | 0.542  | 0.573 |

macro-F1 (new weights + new calib): 0.4738 Accuracy: 0.5040

### 2025 — baseline weights + production calibrators (reference)

| Class  | Precision | Recall | F1    |
| ------ | --------- | ------ | ----- |
| nige   | 0.383     | 0.378  | 0.380 |
| senkou | 0.468     | 0.470  | 0.469 |
| sashi  | 0.471     | 0.505  | 0.487 |
| oikomi | 0.597     | 0.546  | 0.570 |

macro-F1 (baseline + prod calib): 0.4767 Accuracy: 0.4969

## Gate Evaluation

### balanced2

- macro-F1 improved 2024: True
- macro-F1 improved 2025: True
- accuracy ok 2024 (≥-0.3pp): True
- accuracy ok 2025 (≥-0.3pp): True
- no class F1 drop >2pp 2024: True
- no class F1 drop >2pp 2025: True
- **ADOPT-READY: True**

### uniform

- macro-F1 improved 2024: True
- macro-F1 improved 2025: True
- accuracy ok 2024 (≥-0.3pp): True
- accuracy ok 2025 (≥-0.3pp): True
- no class F1 drop >2pp 2024: False
- no class F1 drop >2pp 2025: False
- **ADOPT-READY: False**

## Verdict

**ADOPT-READY**

Best weight vector: **balanced2** (mults: {'nige': 0.65, 'senkou': 1.0, 'sashi': 1.0, 'oikomi': 0.85})

2024: macro-F1 0.4831 (Δ+0.0093) accuracy 0.4973 (Δ+0.0083)
2025: macro-F1 0.4796 (Δ+0.0127) accuracy 0.4976 (Δ+0.0149)

Per-class nige deltas (2024 / 2025):

- nige precision: 0.375 / 0.373 (baseline: 0.342 / 0.319)
- nige recall: 0.447 / 0.400 (baseline: 0.497 / 0.508)
- nige F1: 0.408 / 0.386 (baseline: 0.405 / 0.392)

### Calibrated serve-path caveat (must resolve before production flip)

The gate above compares RAW (uncalibrated) argmax, but production serves
post-isotonic-calibration probabilities. Comparing the two full serve paths on 2025:

| Serve path                                         | Accuracy | macro-F1 | nige-P | nige-R |
| -------------------------------------------------- | -------- | -------- | ------ | ------ |
| baseline weights + prod calibrators (current prod) | 0.4969   | 0.4767   | 0.383  | 0.378  |
| balanced2 + new calibrators                        | 0.5040   | 0.4738   | 0.444  | 0.276  |
| balanced2 raw (no calibration)                     | 0.4976   | 0.4796   | 0.373  | 0.400  |

- Calibrated-vs-calibrated: balanced2 gives **+0.71pp accuracy** and **+6.1pp nige
  precision** but **−0.29pp macro-F1** and **−10.2pp nige recall** vs current prod.
- balanced2 WITHOUT calibration beats both on macro-F1 (0.4796). Isotonic calibration
  shifts argmax toward accuracy at the cost of nige recall.
- Known project failure mode: WF gains that invert under the serve path
  (cf. serve-skew 2026-06-11, NAR G-1+F1 2026-06-12). The raw-gate PASS is therefore
  necessary but NOT sufficient for a production flip.

**Recommendation**: ADOPT-READY per the specified gate, but a production flip must pick
the serving configuration explicitly: (a) balanced2 + new calibrators if accuracy /
nige-precision is the priority (downstream `rs_p_*` probability consumers also benefit
from better-calibrated probabilities), or (b) balanced2 uncalibrated argmax for
class-label display if macro-F1 is the priority. Do NOT pair balanced2 with the OLD
production calibrators (fitted on inverse-freq output distribution — mismatched).
