# JRA Draw × Distance-Speed × Momentum Feature Ablation

- **Date**: 2026-06-20
- **Category**: JRA finish-position feature engineering
- **Verdict**: ALL REJECT — 3 feature groups × 3-fold walk-forward CatBoost ablation

## Background

- Production: `iter22-jra-etop2` (CatBoost iter20 + XGBoost E-top2, 244 features)
- User focus: 馬の時系列成績 / 馬ごとの距離と時計 / 競馬場×距離ごとの枠有利不利
- Feature store: 142 columns. Draw coverage: only 2 features (`umaban` / `umaban_norm`). Gap identified.
- Walk-forward: 3-fold (train 2013-{2022,2023,2024}, test {2023,2024,2025})
- CatBoost YetiRank: `iter=500`, `depth=8`, `lr=0.05`, `l2=3.0`, `seed=42`

## Ablation 1: Draw Features (REJECT)

New features: `wakuban`, `draw_advantage_venue_dist`, `draw_advantage_strength` (100% non-NULL)

SQL investigation found venue×distance-specific draw bias from +2.62pp (函館 long turf inner) to -2.13pp (京都 sprint turf outer). Direction reverses by venue.

Pooled (n=10,365 races):

| Metric     | Delta (pp) | LB95   |
| ---------- | ---------- | ------ |
| top1       | +0.077     | -0.183 |
| place2     | -0.096     | -0.453 |
| place3     | -0.280     | -0.627 |
| fukusho_2p | +0.077     | -0.193 |

Gate: `fukusho_2p` LB95>0 FALSE, positive axes 1/4, veto place3 -0.280 FAIL.

Root cause: CatBoost depth=8 already learns `umaban` × `keibajo_code` × `kyori` × `track_code` interactions implicitly.

## Ablation 2: Distance-Specific Speed (REJECT)

New features: `dist_speed_index_avg`, `dist_run_count`, `dist_speed_index_best` (35% NULL — first-time-at-distance)

Built from PG source (`soha_time` → speed_index conversion). Note: feature store `speed_index` columns are 100% NULL (v8 store built with empty `time_sa`).

Pooled:

| Metric     | Delta (pp) | LB95   |
| ---------- | ---------- | ------ |
| top1       | +0.116     | -0.135 |
| place2     | -0.106     | -0.453 |
| place3     | -0.164     | -0.511 |
| fukusho_2p | -0.077     | -0.347 |

Gate: all FAIL. 35% NULL disrupts GBDT routing (D-phase lesson confirmed again).

Market pricing: record-rank signal 85% already in odds. Residual +3.78pp only in favorite band.

## Ablation 3: Horse Momentum (REJECT)

New features: `win_streak` (6.9% >0), `loss_streak` (82.2% >0), `finish_improvement_rate` (44.2% NULL)

Pooled:

| Metric     | Delta (pp) | LB95   |
| ---------- | ---------- | ------ |
| top1       | -0.029     | -0.309 |
| place2     | -0.260     | -0.618 |
| place3     | -0.425     | -0.820 |
| fukusho_2p | -0.135     | -0.444 |

Gate: all FAIL. Worst of 3 ablations. `finish_trend_5` + `recent_win_count_5` + `recent_finish` + `last_race_finish_norm` (existing 48 time-series features) already saturate this signal.

## Root Cause Analysis

1. **Draw**: CatBoost captures venue×distance draw bias implicitly through multi-way tree splits.
2. **Distance speed**: 85% priced into odds + 35% NULL routing disruption.
3. **Momentum**: 48 existing time-series features already saturate streak/trajectory signal.

## Conclusion

All 3 user focus areas (draw, distance×time, horse trajectory) are already captured by the 142-feature model. Explicit feature additions are redundant or harmful (NULL routing disruption). This is consistent with the prior frontier findings.
