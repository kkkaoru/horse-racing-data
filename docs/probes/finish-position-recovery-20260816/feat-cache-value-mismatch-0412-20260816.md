# feat-cache value mismatch vs production HIT 04/12 (2026-08-16 07:07 JST)

No R2 PUT. Existing HIT objects (`04/12`, `07/01`) were not overwritten.
04/01 HEAD at 07:06:23 still **404** (no late land).

## Gate

Advisor: names matching is not enough. Compare **values** on a production
HIT race before any PUT. Rank-changing diffs are a hard stop.

## What was compared

- Production HIT GET: `feat-cache/catalog-v1/jra/20260816/04/12/features.parquet`
  (15×390, Last-Modified Sat 15 Aug 2026 19:09:35 GMT)
- Local A: `/tmp/fp-builtin-split/feat-jra-v7-final` sliced to 04/12 (15×391)
- Local B: `/tmp/predict-upcoming/feat-jra-layer-16` sliced to 04/12 (15×391)

Join key: `ketto_toroku_bango` + `umaban`. All 15 horses matched.

## Result: **do not PUT**

| check                                  | split_final vs R2                | layer-16 vs R2             |
| -------------------------------------- | -------------------------------- | -------------------------- |
| extra local col                        | `race_year` only                 | `race_year` only           |
| mismatch cols (rtol 1e-5)              | **27**                           | **47**                     |
| `pedigree_score_for_race_rank_in_race` | **14/15 horses**, max abs **13** | same 14/15, max abs **13** |

Examples (split_final, all 15 horses differ): sire win/style rates,
`pedigree_score_for_race`, `sire_x_field_pace_score` (max abs 2.08).

A HIT on either local object would score a **different rank vector** than
the production writer. MISS is safer.

## Other notes

- 01/02 admin focused-full at 22:06:16Z returned HTTP 200 `status=busy`
  (`racesPredicted: 0`). Not accepted. Not retried.
- Safe writer remains HTTP focused-full → Worker `FEATURES_CACHE.put`.
  That path is currently **busy** and still has the 04/01 overtake problem.
