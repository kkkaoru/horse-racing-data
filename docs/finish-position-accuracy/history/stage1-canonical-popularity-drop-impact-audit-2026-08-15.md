# Stage-1 canonical popularity drop — production impact audit (2026-08-15)

**Status:** root cause and impact confirmed; investigation only. **No code change, migration, deploy, or production setting change was made.**

## Executive summary

`add-near-miss-features.py` drops the canonical `tansho_ninkijun` column from its output because of a duplicate-column join followed by `EXCLUDE`. The Stage-1 freshness gate added on 2026-07-22 reads exactly that canonical column. Therefore every **full feature build** that passes through the near-miss layer appears odds-missing to the gate, even when realtime odds were fetched and the market layer populated its raw/derived market columns.

This is not a model-training feature mismatch: neither champion uses canonical `tansho_ninkijun` as a model feature. The same final feature pipeline used for training also drops it. The defect is in the **serve-only routing signal** added later.

Production persistence confirms material impact, not merely theoretical reachability:

- JRA: 216 race keys dated 2026-07-25 through 2026-08-09 (within the audit window ending 2026-08-14) have Stage-1 rows and **zero** JRA champion rows in Neon. For those race dates, the persisted model is therefore market-free for all 216 JRA races.
- NAR: 764 race keys have Stage-1 rows; only 136 have iter40 Stage-2 rows. Of 801 unique NAR race keys having either model, 665 are Stage-1-only; among the 99 with both, Stage-1 is newer for 48 and Stage-2 is newer for 51. A recency-equivalent estimate is therefore 713/801 (89.0%) Stage-1 versus 88/801 (11.0%) Stage-2. Viewer priority and historical cache state can affect the exact page impression count, so these are persisted prediction/race counts, not analytics impression counts.
- Ban-ei is unaffected by this routing defect because `stage1_routing.json` has no Ban-ei entry.

Healthy-regime top1 opportunity cost from the accepted blind-WF probes:

- JRA: champion 33.63% versus Stage-1 28.89% = **-4.75pp** (10,365 blind races; documented paired delta -4.75pp, LB95 -5.56).
- NAR: healthy champion 44.71% versus Stage-1 38.64% = **-6.07pp** (40,710 blind races). The NAR probe's accepted purpose was incident recovery, where Stage-1 beats a market-collapsed champion by +12.18pp; it was never intended as the healthy primary.

## 1. Exact data path and root cause

### Odds fetch and canonical base columns are correct

For upcoming races, `finish_position_features_duckdb.py:754-761` emits:

- `tansho_ninkijun = coalesce(rt.ninkijun_realtime, source fallback)`
- `tansho_odds = coalesce(rt.tansho_odds_realtime, source fallback)`

The fetched realtime parquet is staged at `finish_position_features_duckdb.py:1209-1253`. This path can populate the canonical columns locally and is not the point where `tansho_ninkijun` disappears.

### JRA market layer successfully consumes the canonical values

`add-market-signal-features.py:117-145` copies canonical odds/rank from the input parquet into `tansho_odds_raw` and `tansho_ninkijun_raw`. Lines 186-222 retain the input with `b.*` and derive the market features. JRA `RACE_CHAIN` order is `MARKET_SIGNAL_SCRIPT` then `NEAR_MISS_SCRIPT` (`pipeline_args.py:245-249`), so non-null raw market values prove that the canonical values existed before near-miss processing.

### Near-miss drops canonical rank

`add-near-miss-features.py` performs:

1. line 753: `select b.*, rh.kishumei_ryakusho, rh.tansho_ninkijun, rh.shusso_tosu`
2. DuckDB suffixes the duplicate history columns (`tansho_ninkijun_1`, `shusso_tosu_1`).
3. line 786: `b.* exclude (kishumei_ryakusho, tansho_ninkijun, shusso_tosu)`

That removes the original canonical `tansho_ninkijun`. The history copy remains only under its suffixed name and is not a replacement for the current realtime board. No canonical rank is re-emitted.

The adjacent `shusso_tosu` handling proves this collision behavior was already known: lines 787-802 explain the suffix trap and explicitly re-emit a canonical all-NULL `shusso_tosu` for trained-schema compatibility. Equivalent handling was omitted for `tansho_ninkijun`.

NAR has the same failure because its `RACE_CHAIN` begins with `NEAR_MISS_SCRIPT` (`pipeline_args.py:252-256`). NAR does not need the JRA market layer for the column to be dropped: the base canonical rank enters near-miss and is removed there.

### Gate consumes only the dropped name

`stage1_routing.py:230-254` implements `race_has_fresh_odds()` by reading `entry.get(TANSHO_NINKIJUN_FIELD)`, where `late_binding.py:81` defines the field as canonical `tansho_ninkijun`. It does not inspect `tansho_ninkijun_raw` or `tansho_ninkijun_1`.

Consequently the full-build sequence is:

```text
realtime fetch succeeds
  -> base canonical tansho_ninkijun/tansho_odds populated
  -> JRA market layer can populate *_raw and derived market columns
  -> near-miss drops canonical tansho_ninkijun
  -> score_races sees no canonical rank
  -> freshness gate returns odds-missing
  -> Stage-1 market-free model replaces Stage-2 output
```

This identifies the exact cause as a **rename/collision + later-layer projection bug**, not fetch failure, key mismatch, or freshness expiry.

## 2. Timeline

- **2026-05-20 21:28 JST — `cc6e7f6f`**: introduced the near-miss `base_with_meta` join and `EXCLUDE`, including removal of canonical `tansho_ninkijun`.
- **2026-06-06 — `6b21e03f1`**: explicitly documented and repaired the analogous `shusso_tosu` collision for model schema compatibility, but did not restore `tansho_ninkijun`.
- **2026-07-22 08:03 JST — `10e921de`**: added and wired the JRA Stage-1 gate. The JRA probe records that it was built and deployed later that day.
- **2026-07-22 09:15 JST — `cd1f2d98`**: initially wired NAR Stage-1 config.
- **2026-07-22 13:12 JST — `bda43d7d`**: temporarily reverted NAR config.
- **2026-07-22 13:54 JST — `e1aa78df`**: re-enabled/deployed NAR Stage-1 after review.

Thus the near-miss projection defect existed for about two months without affecting model choice. It became a silent routing defect when the gate began consuming the already-removed column on 2026-07-22.

## 3. Was every production race routed to Stage-1?

### Structural answer

- Every JRA/NAR **mode=full** output after gate enablement is routed to Stage-1, regardless of whether fetch and market derivation succeeded, because the final entries lack canonical `tansho_ninkijun`.
- Not every later **mode=rescore** output must be Stage-1. Python late binding writes fresh canonical `tansho_ninkijun` back (`late_binding.py:223`), so a successful fresh rescore can pass the gate and emit Stage-2.
- The 2026-08-15 queue pause prevented the expected rescore lifecycle, which is why the distinction matters operationally.

### Production persistence evidence

A read-only Neon audit grouped predictions by race key/model for race dates 2026-07-22 through 2026-08-14:

| source | Stage-1 race keys | Stage-2 race keys | both | Stage-1 only | Stage-1 newer among both | Stage-2 newer among both |
| ------ | ----------------: | ----------------: | ---: | -----------: | -----------------------: | -----------------------: |
| JRA    |               216 |                 0 |    0 |          216 |                        0 |                        0 |
| NAR    |               764 |               136 |   99 |          665 |                       48 |                       51 |

For NAR, union(Stage-1, iter40) is 801 race keys. Treating the newer candidate as the recency winner yields 713 Stage-1 and 88 Stage-2. The viewer additionally applies model allowlisting and priority in `queries.ts`; therefore this table establishes model-output prevalence but should not be misreported as exact UI impressions.

Historical backfills can have generation timestamps after the race date. Counts above intentionally answer which model outputs persist per race; a separate request/impression log would be required to prove what each user saw at each moment.

## 4. Measured accuracy impact

### JRA

From `jra-stage1-market-free-fallback-probe-2026-07-22.md`:

| arm                           | blind-WF top1 |
| ----------------------------- | ------------: |
| healthy champion (`B_pop`)    |        33.63% |
| market-free Stage-1 (`C_pop`) |        28.89% |
| collapsed champion (`B_null`) |         9.44% |

Healthy odds + accidental Stage-1 routing costs **4.75pp top1** (paired LB95 -5.56). Stage-1 is still much safer than truly missing-market Stage-2, which is why the correct fix must preserve the gate rather than merely disable it.

### NAR

The accepted NAR probe (ctx session `897bce47-be3f-7a77-b785-47e43e47bc18`, event `34e1dff8-914c-7ede-8b77-c9b326637c7d`) records 40,710 blind races:

| arm                           | blind-WF top1 |
| ----------------------------- | ------------: |
| healthy champion (`B_pop`)    |        44.71% |
| market-free Stage-1 (`C_pop`) |        38.64% |
| collapsed champion (`B_null`) |        26.46% |

Healthy odds + accidental Stage-1 routing costs **6.07pp top1**. In the intended incident regime, Stage-1 recovers **+12.18pp [LB95 +11.69]** over the collapsed champion.

### Ban-ei

No Stage-1 config exists for Ban-ei, so this missing routing-signal column cannot route Ban-ei to a market-free fallback. Ban-ei's odds feature quality is a separate concern, not part of this gate incident.

## 5. Training/serving alignment

The canonical rank is **not a model feature**:

- JRA champion metadata contains `tansho_ninkijun_raw`, not `tansho_ninkijun` or `tansho_ninkijun_1`.
- NAR champion metadata contains neither canonical nor suffixed rank; it consumes derived market features.
- Both Stage-1 artifacts intentionally contain no market fields.

The champions were built from the same layer family after `cc6e7f6f`; canonical rank's absence from final training parquet therefore matches full-build serving parquet. Although model projection is name-based, changing the parquet schema is unnecessarily risky because this pipeline has known schema/order-sensitive legacy contracts (`shusso_tosu` is the concrete example). **The rejected first fix is therefore to re-emit canonical `tansho_ninkijun` from near-miss.** The first fix must stay in the routing layer and leave feature parquet unchanged.

The best existing cross-category gate candidate found in this audit is canonical `tansho_odds`: near-miss does not exclude it, the base builder fills it from the same realtime snapshot, and both JRA and NAR carry it. `tansho_ninkijun_1` is not suitable for upcoming races because `stage_race_history()` is settled-only (`finish_position is not null`); `tansho_ninkijun_raw` is not suitable as a cross-category signal because only JRA runs the market layer that adds it. Any routing change must validate `tansho_odds > 0` parity against the original realtime rank signal for full, rescore, partial-board, and scratch cases before adoption.

The meaningful train/serve mismatch is elsewhere: historical training rows have populated derived market features, while morning full builds may legitimately have NULL/median market fields before odds publication. That is the incident the gate was designed to insure against.

## 6. Additional rescore contract findings

These are separate from the canonical-drop root cause but must be addressed before a safe correction is deployed:

1. **Python runner-count skew.** Near-miss intentionally emits canonical `shusso_tosu` as NULL and keeps the real value in `shusso_tosu_1`. Python `late_binding.py:219` reads only canonical `shusso_tosu`, so rescore recomputes `popularity_score` to the 0.5 median even when fresh rank exists. The TypeScript twin already works around this by deriving runner count from live odds. Training/full-build behavior is confirmed different: `finish_position_features_duckdb.py:2387-2405` computes `popularity_score` before near-miss from `rec.shusso_tosu`, and the upcoming base source populates field size from race metadata or a race-partition count. Thus Python rescore's constant-median behavior is serve skew, not the trained distribution.
2. **Only 2 of the champion's market features are refreshed.** Python rescore updates canonical odds/rank, `odds_score`, `popularity_score`, and weight diff. It does not recompute JRA's other 13 market features (`*_raw`, inverse-odds features, ranks/diffs/disagreement, field dominance, horse popularity, similar-odds correlations). If the morning full cache predates odds publication, those remain NULL/stale while canonical rank makes the gate declare Stage-2 healthy. NAR likewise leaves two of its four market features (`field_dominant_favorite_indicator`, `horse_popularity_vs_field`) stale.

Accordingly, correcting the gate alone fixes false Stage-1 selection on a full build whose market features are already populated, but it does not establish healthy Stage-2 feature parity on rescore. Expanding rescore's update set is a separate evaluation project, not part of the first routing fix.

## 7. Safe post-meeting validation plan (not executed)

1. Leave feature parquet unchanged. Change only the gate's freshness input after validating a preserved, cross-category signal. The leading candidate is canonical `tansho_odds > 0`; do not use settled-only `tansho_ninkijun_1`, and do not assume JRA-only `tansho_ninkijun_raw` covers NAR.
2. Add an integration assertion spanning base -> market (JRA) -> near-miss -> gate, proving a fetched board routes to Stage-2 while an absent board routes to Stage-1. Include NAR without the market layer.
3. Reject re-emitting canonical `tansho_ninkijun` as the first fix: it changes final parquet schema in a pipeline with known trained-schema legacy constraints, while a routing-only correction can avoid that risk.
4. Fix Python runner-count sourcing with the same live-field-size contract already used by TypeScript; first lock parity against the builder's confirmed pre-near-miss `rec.shusso_tosu` calculation.
5. Treat recomputation of all champion market-derived features during rescore as a separate evaluated change. Do not bundle it with the gate correction.
6. Evaluate current behavior versus corrected behavior on blind race/cell cohorts; specifically guard the expected +4.75pp JRA / +6.07pp NAR healthy-regime recovery and incident fallback correctness.
7. Shadow-log gate inputs/reason/model choice before changing production selection.
8. Deploy only after 2026-08-15 meetings end and independently verify fresh, missing, partial, stale, and scratch cases.

### Prepared dark patch

Commit `5cabe484` implements the routing-only candidate without activating it:

- unset/any value other than `1`: current canonical-rank-only behavior;
- `STAGE1_PRESERVED_ODDS_GATE_ENABLED=1`: canonical rank **or** positive canonical odds marks the board fresh;
- `finish-position-cron` forwards the optional flag to the container;
- no feature parquet or model-vector schema changes;
- no deploy or production flag change as part of this audit.

Post-meeting canary sequence:

1. Deploy with the flag absent/off and verify identical Stage-1 behavior.
2. In a controlled future window where one selected upcoming race has a populated board, ensure no broad scheduler/recovery batch can run, enable the flag, and trigger focused full for only that race. Do not use a completed race: the full builder's upcoming-source filter would not exercise the target path.
3. Confirm logs report `reason=fresh`, the champion artifact loads, row count/rank uniqueness are complete, score spread is non-degenerate, and no unexpected model version is selected.
4. Repeat with a deliberately odds-absent fixture/request and confirm `reason=odds-missing` still selects Stage-1.
5. Disable the flag immediately on any mismatch and destroy the canary container before broader cell-by-cell evaluation.

## Reproduction queries

The impact counts used read-only SQL equivalent to:

```sql
with r as (
  select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
         bool_or(model_version = 'jra-cb-stage1-marketfree235-2013') as jra_s1,
         bool_or(model_version = 'jra-cb-v9-sim-2013-clean') as jra_s2,
         bool_or(model_version = 'iter12-nar-xgb-hpo-v8-stage1-marketfree-184') as nar_s1,
         bool_or(model_version = 'iter40-nar-settransformer-blend-v1') as nar_s2,
         max(prediction_generated_at) filter (where model_version like '%stage1-marketfree%') as s1_at,
         max(prediction_generated_at) filter (
           where model_version in ('jra-cb-v9-sim-2013-clean', 'iter40-nar-settransformer-blend-v1')
         ) as s2_at
  from race_finish_position_model_predictions
  where kaisai_nen || kaisai_tsukihi between '20260722' and '20260814'
  group by 1,2,3,4,5
)
select source, ... from r;
```

No credentials or connection strings are recorded in this document.
