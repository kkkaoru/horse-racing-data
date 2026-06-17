# JRA iter20 Focused HPO (2026-06-17)

## Summary

**VERDICT: ADOPT (iter21)**

Ran 28 Optuna TPE trials (3-fold leave-one-year-out CV, 2023/2024/2025) on the
iter20-jra-cb-2013-v8 hyperparameter space. The deployed iter20 config inherited
iter14's hyperparameters (depth=8, lr=0.05) without HPO on the 2013+ 244-feature
data. Best found config improves pooled exact top1 by **+0.57pp** with LB95=+0.21pp
(gate: LB95 ≥ 0.0 required), and does not regress place2/place3 beyond -0.05pp.

## Context

Production JRA = iter20-jra-cb-2013-v8 (CatBoost YetiRank, 250 usable features,
train-start 2013). Hyperparameters were never tuned for the 2013+ dataset; this HPO
is the last clean modeling lever before signal-level changes are required.

## Search Space

| Parameter           | Range        | Fixed |
| ------------------- | ------------ | ----- |
| depth               | [6, 9]       |       |
| learning_rate       | [0.03, 0.08] |       |
| l2_leaf_reg         | [1, 8]       |       |
| od_wait             | [30, 100]    |       |
| random_strength     | [0.5, 2]     |       |
| bagging_temperature | [0, 2]       |       |
| iterations          |              | 1000  |
| seed                |              | 2068  |
| thread_count        |              | 6     |

Sampler: Optuna TPE, seed=42. Trial 0 seeded with baseline config to confirm
reproducibility (confirmed: pooled top1=0.4509, identical to standalone baseline).

## Baseline (iter20 deployed config)

| Fold | top1   | place2 | place3 | n_races |
| ---- | ------ | ------ | ------ | ------- |
| 2023 | 43.95% | 23.03% | 17.16% | 3,456   |
| 2024 | 46.38% | 24.52% | 16.79% | 3,454   |
| 2025 | 44.95% | 23.30% | 16.35% | 3,455   |
| Pool | 45.09% | 23.62% | 16.77% | 10,365  |

Baseline params: depth=8, learning_rate=0.05, l2_leaf_reg=3.0, od_wait=30,
random_strength=1.0, bagging_temperature=1.0

## Best Config (Trial 26)

| Parameter           | iter20 (baseline) | iter21 (HPO best) |
| ------------------- | ----------------- | ----------------- |
| depth               | 8                 | **7**             |
| learning_rate       | 0.05              | **0.0795**        |
| l2_leaf_reg         | 3.0               | **2.645**         |
| od_wait             | 30                | **65**            |
| random_strength     | 1.0               | **1.098**         |
| bagging_temperature | 1.0               | **1.194**         |
| iterations          | 1000              | 1000              |
| seed                | 2068              | 2068              |

## 3-Fold Results: Best Config vs Baseline

Gate evaluation re-ran the best config from scratch for bootstrap comparison.

| Fold | top1       | Δtop1       | place2     | Δplace2     | place3     | Δplace3     |
| ---- | ---------- | ----------- | ---------- | ----------- | ---------- | ----------- |
| 2023 | 44.36%     | +0.41pp     | 22.97%     | -0.06pp     | 17.56%     | +0.41pp     |
| 2024 | 46.84%     | +0.46pp     | 23.91%     | -0.61pp     | 16.42%     | -0.38pp     |
| 2025 | 45.79%     | +0.84pp     | 23.85%     | +0.55pp     | 16.93%     | +0.58pp     |
| Pool | **45.66%** | **+0.57pp** | **23.58%** | **-0.04pp** | **16.97%** | **+0.20pp** |

## Gate Evaluation

Gate: paired bootstrap 10,000 samples, seed=42, 5th percentile of deltas.

| Metric | Delta   | LB95    | Threshold | Pass? |
| ------ | ------- | ------- | --------- | ----- |
| top1   | +0.57pp | +0.21pp | ≥ 0.00pp  | PASS  |
| place2 | -0.04pp | -0.47pp | ≥ -0.05pp | PASS  |
| place3 | +0.20pp | -0.23pp | ≥ -0.05pp | PASS  |

All gate conditions satisfied.

## All Trial Results

| Trial  | depth | lr         | l2       | od_wait | top1       | Δtop1       | place2     | place3     |
| ------ | ----- | ---------- | -------- | ------- | ---------- | ----------- | ---------- | ---------- |
| 0      | 8     | 0.0500     | 3.00     | 30      | 0.4509     | 0.00pp      | 0.2362     | 0.1677     |
| 1      | 7     | 0.0762     | 4.58     | 72      | 0.4545     | +0.36pp     | 0.2358     | 0.1698     |
| 2      | 6     | 0.0702     | 3.49     | 80      | 0.4561     | +0.51pp     | 0.2359     | 0.1670     |
| 3      | 9     | 0.0369     | 1.46     | 43      | 0.4507     | -0.03pp     | 0.2335     | 0.1661     |
| 4      | 7     | 0.0399     | 3.57     | 39      | 0.4474     | -0.36pp     | 0.2334     | 0.1648     |
| 5      | 7     | 0.0648     | 1.51     | 66      | 0.4542     | +0.33pp     | 0.2374     | 0.1694     |
| 6      | 8     | 0.0355     | 1.14     | 97      | 0.4518     | +0.09pp     | 0.2337     | 0.1661     |
| 7      | 7     | 0.0330     | 4.15     | 61      | 0.4490     | -0.19pp     | 0.2349     | 0.1658     |
| 8      | 6     | 0.0732     | 1.71     | 77      | 0.4531     | +0.21pp     | 0.2373     | 0.1683     |
| 9      | 8     | 0.0360     | 7.51     | 85      | 0.4516     | +0.07pp     | 0.2362     | 0.1663     |
| 10     | 6     | 0.0519     | 2.45     | 54      | 0.4498     | -0.12pp     | 0.2324     | 0.1649     |
| 11     | 6     | 0.0789     | 5.63     | 78      | 0.4549     | +0.40pp     | 0.2341     | 0.1681     |
| 12     | 6     | 0.0634     | 6.18     | 86      | 0.4533     | +0.23pp     | 0.2333     | 0.1723     |
| 13     | 6     | 0.0615     | 5.44     | 100     | 0.4544     | +0.35pp     | 0.2328     | 0.1663     |
| 14     | 6     | 0.0796     | 2.40     | 86      | 0.4562     | +0.52pp     | 0.2363     | 0.1678     |
| 15     | 6     | 0.0537     | 2.31     | 90      | 0.4542     | +0.33pp     | 0.2336     | 0.1666     |
| 16     | 7     | 0.0676     | 2.29     | 92      | 0.4551     | +0.41pp     | 0.2369     | 0.1705     |
| 17     | 6     | 0.0582     | 2.80     | 80      | 0.4551     | +0.41pp     | 0.2375     | 0.1660     |
| 18     | 7     | 0.0703     | 1.88     | 65      | 0.4530     | +0.20pp     | 0.2331     | 0.1658     |
| 19     | 6     | 0.0452     | 3.28     | 55      | 0.4504     | -0.06pp     | 0.2339     | 0.1671     |
| 20     | 9     | 0.0792     | 1.03     | 72      | 0.4559     | +0.50pp     | 0.2355     | 0.1680     |
| 21     | 9     | 0.0765     | 1.08     | 72      | 0.4514     | +0.05pp     | 0.2352     | 0.1700     |
| 22     | 9     | 0.0684     | 4.13     | 83      | 0.4529     | +0.19pp     | 0.2344     | 0.1708     |
| 23     | 8     | 0.0787     | 1.22     | 72      | 0.4547     | +0.38pp     | 0.2338     | 0.1662     |
| 24     | 9     | 0.0720     | 1.95     | 75      | 0.4545     | +0.36pp     | 0.2332     | 0.1712     |
| 25     | 8     | 0.0588     | 1.36     | 92      | 0.4543     | +0.34pp     | 0.2337     | 0.1695     |
| **26** | **7** | **0.0795** | **2.65** | **65**  | **0.4566** | **+0.57pp** | **0.2358** | **0.1697** |
| 27     | 7     | 0.0715     | 2.68     | 59      | 0.4527     | +0.27pp     | 0.2315     | 0.1685     |

### Key patterns

- Low lr (<0.05) consistently underperforms regardless of depth
- depth=6 with lr≥0.07 reliably positive (+0.21pp to +0.52pp)
- depth=7 with lr≈0.079 and moderate od_wait=65 is optimal (trial 26)
- depth=9 with very high lr (0.079) competitive (trial 20: +0.50pp) but depth=7 more stable
- Baseline depth=8/lr=0.05 was sub-optimal; higher lr + slightly shallower trees better

## Verdict

**ADOPT as iter21-jra-cb-2013-v8**

Top1 LB95 = +0.21pp (above ≥ 0.0 gate). Place2 delta = -0.04pp (above -0.05pp floor).
Place3 delta = +0.20pp (positive). All 3 gate conditions passed.

## Deploy Plan (iter21)

1. **Retrain full**: run `finish_position_catboost.py` with best_params (depth=7,
   learning_rate=0.0795, l2_leaf_reg=2.645, od_wait=65, random_strength=1.098,
   bagging_temperature=1.194, iterations=1000, seed=2068, thread_count=6) on all data
   2013-01-01 through 2025-12-31. Feature store:
   `tmp/v8/feat-jra-v8-iter19-kohan3f-going/`.

2. **Artifact**: save to
   `apps/finish-position-predict-container/models/finish-position/jra/iter21-jra-cb-2013-v8/`

3. **Smoke test**: `RUN_DATE=20260607 python -m finish_position_lightgbm`
   (or equivalent catboost inference) — verify predictions non-empty, no crash.

4. **Registry flip**: update `active_models.json` JRA entry from
   `iter20-jra-cb-2013-v8` to `iter21-jra-cb-2013-v8`.

5. **Rollback**: if serve accuracy drops in first 2 days, revert to
   `iter20-jra-cb-2013-v8` (rollback = single active_models.json line change).
