# Why `pedigree_score_for_race_rank_in_race` differs on 04/12 (2026-08-16)

One column only. No PUT. Production HIT `04/12` / `07/01` not overwritten.

## Split (hypothesis 1 vs 2)

On the same 15 horses, **raw `pedigree_score_for_race` is not equal**.

- score equal: **0 / 15**
- score max abs: **0.09606**
- rank equal: **1 / 15**

So this is **not** “same score, different rank population”. Hypotheses **1
and 4 are out** as the primary cause.

## Numbers (umaban order)

| umaban | ketto      | score R2 | rank R2 | score local | rank local |
| -----: | ---------- | -------: | ------: | ----------: | ---------: |
|      1 | 2023100999 |      NaN |       8 |     0.06093 |         11 |
|      2 | 2023102069 |      0.0 |       1 |     0.04838 |         13 |
|      3 | 2023106889 |      NaN |       8 |     0.10155 |          2 |
|      4 | 2023105254 |      NaN |       8 |     0.08013 |          6 |
|      5 | 2023101129 |      NaN |       8 |     0.05783 |         12 |
|      6 | 2023103419 |      0.0 |       1 |     0.06780 |          9 |
|      7 | 2023100204 |      0.0 |       1 |     0.04306 |         14 |
|      8 | 2023100225 |      NaN |       8 |     0.10284 |          1 |
|      9 | 2023102042 |      NaN |       8 |     0.02894 |         15 |
|     10 | 2023100739 |      0.0 |       1 |     0.06716 |         10 |
|     11 | 2023100578 |      0.0 |       1 |     0.09606 |          3 |
|     12 | 2023103624 |      NaN |       8 |     0.07054 |          8 |
|     13 | 2023103252 |      0.0 |       1 |     0.07325 |          7 |
|     14 | 2023105572 |      NaN |       8 |     0.08570 |          4 |
|     15 | 2023107440 |      0.0 |       1 |     0.08361 |          5 |

R2 score nulls: **8 / 15**. Local score nulls: **0 / 15**.
R2 ranks are only `{1, 8}` (seven 1s = score 0.0, eight 8s = NaN).
Local ranks are **1..15**, each once.

## Rank recomputes (to kill hypothesis 1)

- Local rank == `score.rank(min, desc)` **inside the 15**: **15 / 15**
- That same recompute == R2 rank: **1 / 15**
- Local 04/12 horses ranked among the **490-row day**: ranks 90..479.
  R2 rank == day rank: **0 / 15**

Local rank is race-internal. R2 rank is **not** a day-wide rank of the
local scores. The gap starts **before** rank: the score column itself.

R2’s own ranks match `score.rank(min)` on the **non-null** 0.0 rows
(7/15). The eight NaN rows are stored as rank **8**, which is not
pandas `rank` on NaN (that is NaN). So R2 also has a **null→8** fill
after a two-value score (`0.0` vs null). That is a **secondary** rank
rule, not the reason local ≠ R2: local never sees those nulls.

## Hypothesis 3 (time)

- R2 HIT Last-Modified: **2026-08-15 19:09:35 GMT = 04:09:35 JST**
- Host `feat-jra-layer-16`: **05:02 JST**
- Built-in split final: **06:26–06:59 JST**

Production object is **~1–3 h earlier**. Local split vs local layer-16
on this column: score **15/15 equal**, rank **15/15 equal**. The two
local paths agree with each other and both disagree with the earlier
R2 object.

That is consistent with **different score inputs / pipeline time**, not
with “we ranked the same scores over the wrong set”. Which source table
or layer produced R2’s 0.0/NaN vs local’s dense positives is **not**
identified (out of scope for this one-column split).

## What is established

1. **Rejected:** rank population (full day vs race) as the cause.
2. **Rejected:** local rank bug — it is exactly in-race rank of local scores.
3. **Established:** `pedigree_score_for_race` already differs (0/15 equal).
4. **Established:** R2 scores are degenerate (0.0 or NaN); local scores are
   a full 15-way order.
5. **Open:** whether R2’s 0.0/NaN is an older snapshot, a missing lineage
   join at 04:09, or a different writer path.
