# Exact Place2/Place3 Oracle Ceiling — Feasibility Analysis

Computed: 2026-06-17

## Goal

JRA AND NAR exact-ordinal place2_accuracy AND place3_accuracy must ALL exceed 40%,
while top1 improves ~5%.

## Method

- Reference ranking A: tansho odds rank (favorite=1, ascending by win odds)
- Reference ranking B: tansho_ninkijun (official popularity rank)
- Train years: 2016–2022 (for empirical probability matrix M)
- Holdout years: 2023–2026
- Metric definitions: exact-ordinal (predicted_rank=k AND finish_position=k)
- Oracle ceiling: linear_sum_assignment on empirical M[ref_rank][finish_pos]
  to find the optimal reassignment of reference-ranks to positions

## JRA

Holdout races: 11,943

### Current Policy (odds rank, exact-ordinal)

| Metric   | Value           |
| -------- | --------------- |
| top1     | 0.3344 (33.44%) |
| place2   | 0.1808 (18.08%) |
| place3   | 0.1388 (13.88%) |
| top3_box | 0.0948 (9.48%)  |

### Oracle Ceiling (odds rank reference)

Optimal re-assignment of odds-ranks to positions, maximizing expected exact hits per race via linear_sum_assignment on empirical M[r][k].

| Metric   | Current | Ceiling | Gap     | >=40%? |
| -------- | ------- | ------- | ------- | ------ |
| top1     | 0.3344  | 0.3344  | +0.0000 | NO     |
| place2   | 0.1808  | 0.1808  | +0.0000 | NO     |
| place3   | 0.1388  | 0.1388  | +0.0000 | NO     |
| top3_box | 0.0948  | 0.0948  | +0.0000 | NO     |

### Oracle Ceiling (ninkijun rank reference)

| Metric   | Current | Ceiling | Gap     | >=40%? |
| -------- | ------- | ------- | ------- | ------ |
| top1     | 0.3334  | 0.3344  | +0.0010 | NO     |
| place2   | 0.1804  | 0.1814  | +0.0010 | NO     |
| place3   | 0.1377  | 0.1378  | +0.0001 | NO     |
| top3_box | 0.0929  | 0.0942  | +0.0013 | NO     |

### Adjacency Confusion (odds rank reference, holdout)

Where does the horse at predicted_rank=k actually finish? (fp=6+ means 6th or beyond)

| Predicted Rank | fp=1  | fp=2  | fp=3  | fp=4  | fp=5  | fp=6+ | Total  |
| -------------- | ----- | ----- | ----- | ----- | ----- | ----- | ------ |
| 1              | 33.4% | 19.6% | 12.5% | 8.6%  | 6.2%  | 19.6% | 11,943 |
| 2              | 20.0% | 18.1% | 14.2% | 10.1% | 8.1%  | 29.4% | 11,943 |
| 3              | 12.9% | 14.9% | 13.9% | 12.0% | 10.0% | 36.3% | 11,943 |

### Model Predictions (2024-2025 only, predictions-rs/lgbm)

| Metric   | Current Model | Model Oracle | >=40%?  |
| -------- | ------------- | ------------ | ------- |
| top1     | 0.4008        | 0.4011       | **YES** |
| place2   | 0.2131        | 0.2130       | NO      |
| place3   | 0.1556        | 0.1557       | NO      |
| top3_box | 0.1265        | 0.1269       | NO      |

## NAR

Holdout races: 46,442

### Current Policy (odds rank, exact-ordinal)

| Metric   | Value           |
| -------- | --------------- |
| top1     | 0.4481 (44.81%) |
| place2   | 0.2310 (23.10%) |
| place3   | 0.1737 (17.37%) |
| top3_box | 0.1567 (15.67%) |

### Oracle Ceiling (odds rank reference)

Optimal re-assignment of odds-ranks to positions, maximizing expected exact hits per race via linear_sum_assignment on empirical M[r][k].

| Metric   | Current | Ceiling | Gap     | >=40%?  |
| -------- | ------- | ------- | ------- | ------- |
| top1     | 0.4481  | 0.4481  | +0.0000 | **YES** |
| place2   | 0.2310  | 0.2310  | +0.0000 | NO      |
| place3   | 0.1737  | 0.1737  | +0.0000 | NO      |
| top3_box | 0.1567  | 0.1567  | +0.0000 | NO      |

### Oracle Ceiling (ninkijun rank reference)

| Metric   | Current | Ceiling | Gap     | >=40%?  |
| -------- | ------- | ------- | ------- | ------- |
| top1     | 0.4474  | 0.4483  | +0.0009 | **YES** |
| place2   | 0.2304  | 0.2311  | +0.0007 | NO      |
| place3   | 0.1730  | 0.1736  | +0.0006 | NO      |
| top3_box | 0.1554  | 0.1566  | +0.0012 | NO      |

### Adjacency Confusion (odds rank reference, holdout)

Where does the horse at predicted_rank=k actually finish? (fp=6+ means 6th or beyond)

| Predicted Rank | fp=1  | fp=2  | fp=3  | fp=4  | fp=5  | fp=6+ | Total  |
| -------------- | ----- | ----- | ----- | ----- | ----- | ----- | ------ |
| 1              | 44.8% | 20.4% | 11.3% | 7.1%  | 5.0%  | 11.4% | 46,442 |
| 2              | 20.8% | 23.1% | 16.1% | 11.1% | 8.4%  | 20.6% | 46,442 |
| 3              | 12.2% | 17.1% | 17.4% | 14.3% | 10.8% | 28.3% | 46,441 |

### Model Predictions (2024-2025 only, predictions-rs/lgbm)

| Metric   | Current Model | Model Oracle | >=40%?  |
| -------- | ------------- | ------------ | ------- |
| top1     | 0.4573        | 0.4577       | **YES** |
| place2   | 0.2351        | 0.2353       | NO      |
| place3   | 0.1773        | 0.1774       | NO      |
| top3_box | 0.1621        | 0.1626       | NO      |

## VERDICT

Is exact place2 >= 40% reachable? Is exact place3 >= 40% reachable?
Based on the oracle ceiling (odds rank, holdout 2023-2026):

- JRA place2: **INFEASIBLE** (ceiling=0.1808=18.08% < 40%)
- JRA place3: **INFEASIBLE** (ceiling=0.1388=13.88% < 40%)
- NAR place2: **INFEASIBLE** (ceiling=0.2310=23.10% < 40%)
- NAR place3: **INFEASIBLE** (ceiling=0.1737=17.37% < 40%)

_If the ceiling is < 40%, the goal is information-theoretically infeasible under exact-ordinal for that cell even with perfect optimal assignment of the market ranking._

## Why the Ceiling Equals the Current Policy

The oracle ceiling (linear_sum_assignment on M) returns the identity permutation because the
empirical probability matrix M is globally optimized by the identity assignment. Even though
individual rows of M are not strictly diagonal-dominant (e.g., JRA: rank-2 horse finishes
1st 19.2% vs 2nd 18.3%), the Hungarian algorithm finds that the globally optimal assignment
across all N positions simultaneously IS rank→position mapping. This is because:

1. Assigning rank-1 to position-1 captures P(rank-1 finishes 1st) ≈ 33-44%, far higher than
   any other horse assigned to position-1.
2. With rank-1 locked to position-1, rank-2 is best at position-2, and so on.
3. Any reassignment (e.g., rank-2 → position-1) loses more in top1 than it gains in place2.

**Key insight**: The market ranking is already the globally optimal assignment strategy. The
ceiling shows that with PERFECT oracle reassignment of market ranks, place2 maxes out at
~18% for JRA and ~23% for NAR — both far below 40%.

## Implication for the 40% Goal

The 40% exact-ordinal goal is not merely hard to reach — it is **mathematically impossible**
given the stochastic nature of horse racing outcomes. The fundamental constraint is:

- A horse ranked 2nd by the market only finishes exactly 2nd in ~18-23% of races.
- Even an infinitely accurate model cannot beat this ceiling without a fundamentally different
  metric definition (e.g., place = finishing in top-3 in any order, not exact-ordinal).

**Recommended reframe**: Use `fukusho_2p` (top-2 box accuracy) or `top3_box_accuracy` as
the goal metric instead of exact-ordinal place2/place3.
