# NAR / Ban-ei weight window (read-only, 2026-08-16 08:25 JST)

Deadlines: NAR first post **12:35** (weight earliest ~12:04). Ban-ei
**14:25** (~13:54). Do not wait until after JRA 09:40 to start watching
these.

## 1. Does weight-rescore-trigger cover NAR / Ban-ei?

Yes. Two different gates, measured from committed `wrangler.jsonc` +
code + yesterday's D1:

| path                                             | gate                                                                             | JRA | NAR                          | Ban-ei |
| ------------------------------------------------ | -------------------------------------------------------------------------------- | --- | ---------------------------- | ------ |
| Coordinator cron T-25                            | `COORDINATOR_ENABLED` (live **"0"**) and `RESCORE_CATEGORIES` (**"nar,ban-ei"**) | off | off (coordinator itself off) | off    |
| Weight write → POST `/api/internal/rescore-race` | `RESCORE_ENABLED` (**"1"**). **Does not read** `RESCORE_CATEGORIES`              | on  | on                           | on     |

`parseRescoreTriggerRequest` maps `jra:*` → jra, `nar:*:83:*` / `65` →
ban-ei, other `nar:*` → nar.

D1 `weight-rescore-trigger` **yesterday** (08-15): jra ok=36, nar ok=20,
ban-ei ok=12. The path already fired for all three. **Today 00:00 JST
onward: 0 rows** — weights have not written yet (`fetch-weights` still
`skip:weights-empty` at 08:02).

## 2. Where is NAR/Ban-ei weight, and how many today?

Column: `nvd_se.bataiju` (plus `zogen_sa` / `zogen_fugo`). Same blank
sentinel as the viewer (`'   '`).

0816 `nvd_se` at 08:25 JST:

| keibajo | races | rows | rows with a real `bataiju` |
| ------- | ----: | ---: | -------------------------: |
| 35      |    12 |  110 |                      **0** |
| 44      |    10 |  112 |                      **0** |
| 55      |    10 |  111 |                      **0** |
| 83      |    12 |  117 |                      **0** |

35/01 sample: every `bataiju` / `zogen_sa` is three spaces. Same state
as 08:00 local-PG check (0/450). Not published yet.

## 3. Are NAR/Ban-ei in the feat-cache seed?

Yes, **44/44 HIT**. Not the earlier "10 HIT / 63 MISS" inventory
(that was 05:50, JRA-heavy). Signed HEAD at 08:25:

- nar 35×12 + 44×10 + 55×10 = 32 HIT. Last-Modified **22:49–22:50 UTC
  15 Aug = 07:49–07:50 JST today**.
- ban-ei 83×12 = 12 HIT. Last-Modified **22:48–22:49 UTC = 07:48–07:49
  JST**.

These objects are **after** the 05:31/05:50 host one-shot and after
the 04:09 JRA HIT. A later weight rescore that HITs them will score
**this** 07:49 parquet, not the 05:31 host `v7-final`. Same
verification duty as JRA (pedigree / schema), owned with optimize on
HIT quality.

## 4. Can per-race pedigree collapse happen on NAR/Ban-ei?

JRA collapse: `--target-race` + `rec INNER JOIN jra_um` +
`PEDIGREE_MIN_RACES=5`.

NAR/Ban-ei `pedigree_rec_um_subquery` is **not** that join. It is
`rec LEFT JOIN nar_um LEFT JOIN nar_nu` filtered to rows that still
have a sire id. History population is not `jra_um`. So the **exact
JRA mechanism does not apply**.

Not proven tonight: that a `--target-race` NAR base still reaches
`MIN_RACES=5` on `sire_*` stats. Optimize's 08-16 HIT sample was NAR
0/4 live on cache; that is not a generation experiment. Do not claim
NAR is immune — claim only **the JRA join is absent**.

## Watch after 12:04 / 13:54

Same three questions as JRA 09:09: trigger rows for `nar:` / `nar:…:83:`,
Neon `prediction_generated_at` vs 05:31/05:50 host stamps, coverage
still 32+12. Plus: if rescore HITs the 07:49 cache, compare pedigree
on that object (optimize) before trusting the overwrite.
