# Pedigree score spread on production feat-cache (2026-08-16 07:12 JST)

No PUT. Existing HIT objects not overwritten.

Degenerate = `pedigree_score_for_race` is only `0.0` and/or NaN (no positive).

## 1. All 10 production HITs on 20260816

| cat    | race  |   n | null | zero | pos | uniq | degenerate |
| ------ | ----- | --: | ---: | ---: | --: | ---: | ---------- |
| jra    | 04/12 |  15 |    8 |    7 |   0 |    1 | **yes**    |
| jra    | 07/01 |  10 |   10 |    0 |   0 |    0 | **yes**    |
| nar    | 35/05 |  20 |    2 |    6 |  12 |   13 | no         |
| nar    | 35/06 |  18 |    4 |    6 |   8 |    8 | no         |
| nar    | 35/07 |  19 |    3 |    5 |  11 |   12 | no         |
| nar    | 44/10 |  26 |    0 |    7 |  19 |   20 | no         |
| ban-ei | 83/03 |   9 |    2 |    7 |   0 |    1 | **yes**    |
| ban-ei | 83/06 |  10 |    0 |    0 |  10 |   10 | no         |
| ban-ei | 83/07 |  10 |    0 |    0 |  10 |    9 | no         |
| ban-ei | 83/10 |  10 |    0 |    0 |  10 |   10 | no         |

**0816 HIT: degenerate 3 / 10.** JRA HITs **2 / 2**. NAR HITs **0 / 4**.
Ban-ei HITs **1 / 4** (83/03 only).

## 2. Other `pedigree_*` columns on those 10

JRA / NAR objects have 7 `pedigree_*` cols; Ban-ei 5.

When the score is degenerate, **5 of 7** JRA cols (or **4 of 5** Ban-ei)
are also degenerate. Two JRA interaction cols stay live:
`pedigree_venue_x_horse_venue`, `pedigree_distance_x_horse_distance`.

When the score is live (all 4 NAR + 3 Ban-ei), **all** `pedigree_*` cols
are live.

## 3. Past-day cache (priority)

HEAD scan of common venues: **21 HIT** (08-14 NAR 9; 08-15 JRA 5 + Ban-ei 7).
**No 08-14 JRA HIT** in that scan. GET of 10 of those 21:

| ymd  | cat    | race  |   n | null | zero | pos | degenerate |
| ---- | ------ | ----- | --: | ---: | ---: | --: | ---------- |
| 0815 | jra    | 01/04 |  12 |    5 |    7 |   0 | **yes**    |
| 0815 | jra    | 04/02 |  15 |   15 |    0 |   0 | **yes**    |
| 0815 | jra    | 04/09 |  11 |    2 |    0 |   9 | no         |
| 0815 | jra    | 04/10 |  15 |    7 |    8 |   0 | **yes**    |
| 0815 | jra    | 04/11 |  18 |   10 |    8 |   0 | **yes**    |
| 0815 | ban-ei | 83/01 |  10 |    0 |    0 |  10 | no         |
| 0815 | ban-ei | 83/03 |  10 |    5 |    5 |   0 | **yes**    |
| 0815 | ban-ei | 83/07 |  10 |    0 |    0 |  10 | no         |
| 0814 | nar    | 44/01 |  36 |    3 |   17 |  16 | no         |
| 0814 | nar    | 47/12 |  10 |    0 |    0 |  10 | no         |

**08-15 JRA sampled: degenerate 4 / 5** (04/09 is the live one).
Not today-only. Not every JRA object (04/09 has 9 positive scores).
NAR past samples live. Ban-ei mixed, same as 0816.
