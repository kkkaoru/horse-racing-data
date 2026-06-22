# JRA RS x venue x dist x track — per-class / per-keibajo partial-rho probe (2026-06-19)

Feature: LOYO (leave-one-year-out) cell mean of `finish_norm` over `(keibajo_code, track_code, kyori_band, predicted_style)`, sign-flipped so higher = better finish. `predicted_style` = argmax of `past_{nige,senkou,sashi,oikomi}_rate_self`.

Partial rho controls for `odds_score`. Bootstrap 95% CI: n_boot=5000, seed=42. Years 2019-2025. PASS = LB95 > 0.

**Global reference**: n=330,826 rho=-0.0081 LB95=-0.0116 UB95=-0.0048 (FAIL)

## Per-class (grade_code)

| grade_code | meaning    | n       | partial rho | LB95    | UB95    | verdict     |
| ---------- | ---------- | ------- | ----------- | ------- | ------- | ----------- |
| ` `        | ?          | 243,842 | -0.0129     | -0.0170 | -0.0090 | FAIL        |
| `E`        | 新馬       | 66,857  | -0.0091     | -0.0165 | -0.0015 | FAIL        |
| `C`        | 3勝        | 6,973   | -0.0063     | -0.0300 | +0.0183 | FAIL        |
| `L`        | リステッド | 5,956   | -0.0099     | -0.0352 | +0.0145 | FAIL        |
| `B`        | 2勝        | 3,627   | -0.0010     | -0.0330 | +0.0322 | FAIL        |
| `A`        | OP         | 2,775   | -0.0168     | -0.0530 | +0.0203 | FAIL        |
| `H`        | 重賞H      | 399     | -0.0792     | -0.1758 | +0.0207 | FAIL        |
| `G`        | 重賞G      | 202     | +0.1571     | +0.0145 | +0.2918 | PASS        |
| `F`        | 重賞F      | 146     | +0.0783     | -0.0910 | +0.2494 | FAIL        |
| `D`        | ?          | 49      | -           | -       | -       | SKIP(n<100) |

## Per-keibajo (keibajo_code)

| keibajo_code | name | n      | partial rho | LB95    | UB95    | verdict |
| ------------ | ---- | ------ | ----------- | ------- | ------- | ------- |
| `01`         | 札幌 | 14,403 | -0.0339     | -0.0500 | -0.0175 | FAIL    |
| `02`         | 函館 | 12,345 | +0.0002     | -0.0182 | +0.0182 | FAIL    |
| `03`         | 福島 | 22,484 | -0.0122     | -0.0257 | +0.0007 | FAIL    |
| `04`         | 新潟 | 32,580 | -0.0100     | -0.0209 | +0.0006 | FAIL    |
| `05`         | 東京 | 52,766 | +0.0035     | -0.0051 | +0.0119 | FAIL    |
| `06`         | 中山 | 50,635 | -0.0116     | -0.0203 | -0.0029 | FAIL    |
| `07`         | 中京 | 37,944 | -0.0074     | -0.0176 | +0.0028 | FAIL    |
| `08`         | 京都 | 33,164 | -0.0025     | -0.0133 | +0.0082 | FAIL    |
| `09`         | 阪神 | 46,912 | -0.0129     | -0.0222 | -0.0035 | FAIL    |
| `10`         | 小倉 | 27,593 | -0.0196     | -0.0312 | -0.0076 | FAIL    |

## PASS summary (LB95 > 0)

- **Classes**: `G`
- **Keibajo**: NONE
