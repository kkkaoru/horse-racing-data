# JRA track-bias draw-interaction probe (2026-06-19)

**Verdict: ABORT — all candidates fail partial-ρ gate (≥0.08).** No track-bias
draw-interaction signal survives the existing controls.

## Task

Maximize track-bias information for the JRA finish-position model. Test whether
draw × condition / distance / existing-bias **interactions** add signal beyond
the controls already in production (`odds_score + track_bias_inside +
umaban_norm`).

Gate: partial Spearman ρ ≥ 0.08 vs `finish_norm`.

## Data

- Feature store: `tmp/v8/feat-jra-v8-iter19-kohan3f-going/race_year=*/` (20 yrs,
  941,970 rows after non-null filter on y + controls + baba + kyori).
- DuckDB 1.5.3, `memory_limit=4GB`, `threads=4` (env: `apps/pc-keiba-viewer/.venv`).
- PG container `horse-racing-local-postgresql` (user/db = `horse_racing`).

## PG raw distribution (draw_group × baba × surface)

Schema note: `jvd_se` has **no `source`/`race_id`** col; join is the composite
`(kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime,
race_bango)`. Baba is split into `babajotai_code_shiba` / `_dirt`; surface from
`track_code` (turf 10–22, dirt 23–29). baba code 1=良 2=稍重 3=重 4=不良.

avg_finish by draw_group (inner ≤4 / mid ≤8 / outer >8):

| surface | baba | inner | mid  | outer |
| ------- | ---- | ----- | ---- | ----- |
| turf    | 1    | 7.02  | 7.12 | 8.23  |
| turf    | 2    | 6.98  | 7.11 | 8.22  |
| turf    | 3    | 6.78  | 7.05 | 8.17  |
| turf    | 4    | 6.45  | 6.78 | 8.03  |
| dirt    | 1    | 6.30  | 6.33 | 7.37  |
| dirt    | 4    | 5.94  | 6.04 | 7.12  |

The draw effect is **large but nearly additive**: outer is ~1.2 (turf) / ~0.9
(dirt) worse across _every_ condition. The condition-dependent _change_ in that
gap (the interaction) is small — turf inner improves 7.02→6.45 as track worsens
while outer holds ~8.0–8.2, a ~0.5pp swing.

## Partial ρ (residual-on-residual Spearman, controls regressed out)

| candidate                                                | partial ρ | raw Spearman | gate |
| -------------------------------------------------------- | --------- | ------------ | ---- |
| a. umaban_x_baba_condition (un × current_baba_condition) | −0.0036   | −0.0094      | fail |
| b. umaban_x_distance_band (un × kyori)                   | +0.0067   | +0.0092      | fail |
| d. umaban_x_track_bias_inside (un × track_bias_inside)   | −0.0107   | −0.0092      | fail |

Surface/condition-sliced (raw products, same controls) — strongest slice still
fails by 3×:

| slice                    | a(baba) | b(dist) | d(tbi)  |
| ------------------------ | ------- | ------- | ------- |
| ALL (n=941,970)          | −0.0047 | +0.0129 | −0.0208 |
| TURF (n=450,432)         | +0.0044 | +0.0120 | −0.0136 |
| DIRT (n=462,567)         | −0.0049 | +0.0081 | −0.0296 |
| HEAVY baba≥3 (n=116,326) | −0.0020 | +0.0190 | −0.0188 |
| TURF & baba≥3 (n=30,621) | −0.0008 | +0.0236 | −0.0052 |

Diagnostic: `tbi_rho ≈ 0.000` and `un_rho ≈ +0.018` (turf +0.037) in every
slice — both controls carry only weak _monotone_ signal (GBDT uses them
non-linearly), so the near-zero **raw** Spearman of the products confirms this is
not control-masking. The interactions genuinely carry no rank signal.

## c. track_switch_flag — NOT TESTABLE from store

The feature store has **no prev-race surface column** (`last_race_*` covers
finish/margin/corner/class/distance but not shiba↔dirt). Building
`track_switch_flag` requires an upstream PG prev-race surface join and a store
rebuild — out of scope for this parquet-based probe. Given a/b/d all collapse,
not recommended as a standalone follow-up.

## Conclusion

ABORT. Draw is already captured as a weak additive main effect by `umaban_norm`

- `track_bias_inside`; the condition/distance/bias **interactions** add ~0
  incremental rank signal (worst-case slice 3× under gate). Consistent with the
  standing JRA-frontier finding that GBDT non-linearly absorbs available
  track/draw structure. gate-draw remains the binding constraint; this confirms
  its _interaction_ extensions are also exhausted.
