# First dead column for JRA pedigree_score (2026-08-16)

No PUT / deploy. Existing HIT objects not overwritten.

## Where the score is built

`finish_position_features_duckdb.py` `base_features_select_sql`:
`pedigree_score_for_race` is the mean of **five already-gated components**
(`PEDIGREE_MIN_RACES = 5`):

- `sire_distance_win_rate`
- `dam_sire_distance_win_rate`
- `sire_track_win_rate`
- `sire_keibajo_win_rate`
- `damsire_keibajo_win_rate`

If every component is NULL, `nullif(divisor, 0)` makes the score **NULL**.
If a component is present but **0.0**, coalesce turns the score into **0.0**.
The score column is not an independent computation.

## First dead place (measured)

On production HIT `jra 04/12` (degenerate score):

| column                      | null | zero |   pos |
| --------------------------- | ---: | ---: | ----: |
| sire_distance_win_rate      |   12 |    3 | **0** |
| dam_sire_distance_win_rate  |   14 |    1 | **0** |
| sire_track_win_rate         |    8 |    7 | **0** |
| sire_keibajo_win_rate       |   15 |    0 | **0** |
| damsire_keibajo_win_rate    |   15 |    0 | **0** |
| **pedigree_score_for_race** |    8 |    7 | **0** |

Same horses on **local split** (live score): all five components have
positives (14–15/15). So the score dies **because the five `sire_*` /
`damsire_*` outputs are already dead**, not because of the rank or the
mean expression.

`jra 07/01` HIT: all five components **10/10 NULL** → score 10/10 NULL.
That is the same first place, fully empty.

NAR HIT `44/10` (control): those five columns have positives; score is live.

08-15 JRA `04/09` (the one live past JRA HIT): score is live **because
`sire_track_win_rate` is live** (9 pos). Distance components are still
all-null. So “JRA always kills every component” is **false**; the first
dead family is still those five, and which of them survive varies.

## Why interaction cols can live

`pedigree_venue_x_horse_venue` / `pedigree_distance_x_horse_distance` are
`coalesce(gated_sire_rate, 0) * horse_rate`. A dead sire rate becomes **0**,
not NULL, so the product can still be non-zero from the horse side. That
matches “5 of 7 `pedigree_*` dead, 2 interactions live”.

## JRA-only join (code, not a guess about tonight’s writer)

`pedigree_rec_um_subquery("jra")` is
`rec INNER JOIN jra_um` (sire/damsire ids).
NAR is `rec LEFT JOIN nar_um / nar_nu` with a not-null sire filter.

`jra_um` / `rec` history both receive `horse_filter` when `--target-race`
is set: **only the target race’s `ketto_toroku_bango` list**. Sire
aggregates then see only those horses’ own past rows, not the sire’s
other progeny. Full-day local builds pass `target_race=None` → empty
filter → full `jra_um` / full `rec`. That is the code difference that
can empty JRA components while leaving NAR (different um tables / filter)
alive. **Whether production HIT used `--target-race` is not proven from
container logs** (none). It is consistent with: local full-day live,
production per-race HIT dead, NAR HIT live.

`--target-race` as the cause is **rejected by local artifacts**.
This morning's split passed `target_race=04:01` to all five RACE_CHAIN
layers. In that output, 04/01 (the target) still has positives on the
five components (score 13/13 live) and 04/12 (not the target) is 15/15
live. Day-base was `target_race=None`; 04/12 scores are identical before
and after RACE_CHAIN (15/15). So attaching `--target-race` to RACE_CHAIN
does not kill the components. Production HIT death is still unexplained.

Not claimed: that production HIT used this filter, or that 08-15 `04/09`
survived because it was a day-wide build.
