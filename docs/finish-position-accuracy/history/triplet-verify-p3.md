# Triplet Verification P3 — Rank 3 & Rank 8 (2026-06-12)

## Context

Per-class probe on two triplets from `triplet-ideation-ranked.md`. Analysis is READ-ONLY on
the local PG mirror (postgresql://horse_racing:\*\*\*@127.0.0.1:15432/horse_racing). Holdout:
2023-01-01 to 2026-06-12. Gate: partial Spearman ρ ≥ 0.08 after odds-control.

---

## Rank 3: `tansho_odds` × `corner1_norm` × `finish_position`

### Hypothesis

Horses that run from the front (low corner1_pos) but consistently underperform their market odds
may carry a "trajectory" signal — the historical correlation between corner position and
market-adjusted finish residual could predict future finish independently of current odds.

### Column names confirmed

- `corner_1` (varchar 2) — absolute corner-1 position (e.g. "02", "09")
- `tansho_odds` (varchar 4) — stored as int×10 (e.g. "0038" = 3.8x; "0000" = unknown/post)
- `kakutei_chakujun` (varchar 2) — confirmed finish position

`corner1_norm` is computed as `TRIM(corner_1)::int / race_size` where `race_size = COUNT(*)
OVER (PARTITION BY race_id)`. `inv_odds = 10.0 / tansho_odds_numeric` (implied-prob proxy).

### Feature construction

```sql
-- Per past race (last ≤ 5, strictly race_date_str < target_date):
--   market_residual = finish_norm - (1 - inv_odds)
-- Aggregate over last 5:
--   corr_corner1_mresidual: CORR(corner1_norm, market_residual)
--   front_vs_offpace_diff:  avg(market_residual | c1n ≤ 0.33) - avg(... | c1n > 0.33)
-- Target: finish_norm of target race, partial_ρ controlling for target inv_odds
```

All joins use strict `p.race_date_str < h.race_date_str`; no leakage.

### Holdout results (2023–2026)

| Class | N horses×races | Feature                  | ρ (raw) | p-value | partial ρ (after inv_odds ctrl) | partial p |
| ----- | -------------- | ------------------------ | ------- | ------- | ------------------------------- | --------- |
| JRA   | 2 607          | `corr_corner1_mresidual` | -0.0041 | 0.8725  | -0.0042                         | 0.8686    |
| JRA   | 1 235          | `front_vs_offpace_diff`  | -0.0180 | 0.5283  | -0.0178                         | 0.5320    |
| NAR   | 969            | `corr_corner1_mresidual` | -0.0652 | 0.1358  | -0.0654                         | 0.1347    |
| NAR   | 412            | `front_vs_offpace_diff`  | +0.0419 | 0.3962  | +0.0405                         | 0.4119    |

Notes:

- Row counts smaller than total `N horses×races` reflect cases where corner1 IS NOT NULL AND
  at least 3 past races with corner1 data exist.
- The NAR `corr_corner1_mresidual` ρ = −0.065 with p = 0.14 is the highest signal seen but
  still far below the gate and not significant.

### Redundancy check

The existing feature `past_corner_1_norm_avg_5` (simple average of past corner1_norm) already
achieves Pearson r ≈ +0.072 on JRA (raw correlation, no odds control), indicating plain running
position history carries more signal than the odds-residual conditioning. The triplet's
conditioning step (subtracting expected-rank-from-odds before correlating with corner position)
adds noise rather than refinement — the residual is dominated by odds-model estimation error.

The `inverse_odds_implied_prob` / `odds_score` features already capture the inv_odds dimension
at race time. Conditioning historical running style on past odds residuals loses degrees of
freedom (requires BOTH corner1 AND valid odds for each past race) and produces a sparser, noisier
feature.

### VERDICT: ABORT

**Reason:** All partial ρ values after odds-control are ≤ |0.065|, far below the gate of 0.08.
p-values are uniformly non-significant (0.13–0.87). The hypothesis that historical
corner-position × market-residual trajectories carry incremental predictive signal beyond current
odds is empirically rejected. The plain `past_corner_1_norm_avg_5` analog already outperforms the
triplet feature on raw correlation without odds conditioning; the added complexity of residual
conditioning degrades rather than improves signal.

---

## Rank 8: `zogen_sa` × `tansho_odds` × `finish_position`

### Hypothesis

Horses whose past weight changes (zogen_sa) are consistently mispriced by the market — i.e.,
the market does not fully adjust odds for weight changes — may expose an underpriced signal.
Both the historical corr(zogen, finish) trajectory and the direct current-race zogen_signed are
tested.

### Column names confirmed

- `zogen_fugo` (varchar 1) — sign: `'+'` or `'-'`
- `zogen_sa` (varchar 3) — magnitude in 0.1 kg units (stored as "002" = 0.2 kg; "017" = 1.7 kg)
- Null rate for 2023+: 49 239 / 237 011 = 20.8% NULL on JRA (first run at course / no prior weight)

`zogen_signed` = +zogen_sa::float when fugo='+', -zogen_sa::float when fugo='-'.

### Feature construction

```sql
-- Per past race (last ≤ 5, strictly past < target):
--   zogen_signed, finish_norm, inv_odds
-- Aggregate over last 5:
--   corr_zogen_finish:   CORR(past_zogen_signed, past_finish_norm)
--   corr_zogen_invodds:  CORR(past_zogen_signed, past_inv_odds)
--   weightup_vs_down:    avg(finish_norm | zogen > 0) - avg(... | zogen < 0)
-- Also: current target_zogen_signed vs target_finish_norm (direct, 1-race)
```

### Holdout results (2023–2026)

| Class | N horses×races | Feature                  | ρ (raw) | p-value | partial ρ (after inv_odds ctrl) | partial p |
| ----- | -------------- | ------------------------ | ------- | ------- | ------------------------------- | --------- |
| JRA   | 1 838          | `corr_zogen_finish_hist` | -0.0205 | 0.3816  | -0.0194                         | 0.4088    |
| JRA   | 1 606          | `current_zogen_direct`   | +0.0201 | 0.4199  | +0.0160                         | 0.5222    |
| JRA   | 1 590          | `weightup_vs_down_diff`  | -0.0196 | 0.4354  | -0.0183                         | 0.4666    |
| NAR   | 851            | `corr_zogen_finish_hist` | -0.0005 | 0.9882  | -0.0023                         | 0.9459    |
| NAR   | 766            | `current_zogen_direct`   | -0.0050 | 0.8900  | -0.0040                         | 0.9119    |
| NAR   | 758            | `weightup_vs_down_diff`  | -0.0274 | 0.4508  | -0.0290                         | 0.4253    |

Notes:

- Existing features `weight_diff_from_avg` / `weight_trend_5` / `bataiju` already encode weight
  change information. Direct Pearson r of current zogen_signed vs finish_norm ≈ −0.002 on JRA,
  consistent with near-zero ρ here.
- All partial ρ are ≤ |0.029|; sign is inconsistent across features within NAR.

### Redundancy check

The existing `weight_diff_from_avg` and `weight_trend_5` features already represent weight
condition change. The triplet's proposed "market responsiveness" angle (does the market price
in zogen correctly?) reduces to asking whether past corr(zogen, finish) predicts future finish —
which it does not (ρ ≈ 0). The null rate of ~21% further reduces usable N. No incremental
signal is observed beyond what plain weight features already (poorly) capture.

Baseline odds signal strength (Pearson r of inv_odds vs finish_norm at race level, JRA 2023+):
approximately −0.017 to +0.021, reflecting that within-race Spearman of CURRENT odds vs finish
is positive but small when measured globally rather than within-race. This confirms the
odds-control denominator is non-trivial, making the partial ρ the stringent acid test.

### VERDICT: ABORT

**Reason:** All partial ρ values are ≤ |0.029| with p > 0.40 on JRA and effectively zero on NAR.
The odds frontier prior is confirmed: weight change information (zogen_sa) is already known to
the market and fully priced into tansho_odds. Historical corr(zogen, finish) trajectories carry
no incremental information after odds control. The 20.8% null rate on JRA would further inflate
production imputation noise. This triplet is structurally redundant with existing weight features
and provides no incremental value.

---

## Summary

| Rank | Triplet                                        | Best partial ρ (any class/feature) | Gate (≥0.08) | Verdict   |
| ---- | ---------------------------------------------- | ---------------------------------- | ------------ | --------- |
| 3    | `tansho_odds × corner1_norm × finish_position` | −0.065 (NAR, corr_c1_mresid)       | FAIL         | **ABORT** |
| 8    | `zogen_sa × tansho_odds × finish_position`     | −0.029 (NAR, weightup_vs_down)     | FAIL         | **ABORT** |

Both triplets are dominated by the odds frontier. Partial ρ after odds-control is effectively
zero across all constructions. Feature engineering complexity (trajectory / residual conditioning
/ running-style bucketing) adds noise without recovering signal. These angles are closed.
