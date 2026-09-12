# JRA exact-cell campaign — 2026-09-12

## Historical evidence qualification

This is a retained campaign record, not a fresh validation or deployment authorization. Some reported winner-hit deltas exceed the stated exact-race counts (including the reciprocal-rank Radio Nippon row). Their aggregation denominators have not been reconciled with the original reports. Preserve these observations as unresolved evidence; do not interpret the affected tables as validated per-race accuracy or use them to approve a model. Deployment and test statements below describe the dated campaign, not the current checkout.

## Scope contract

- Target card: all 24 JRA races at Nakayama (`06`) and Hanshin (`09`).
- Canonical evaluation cells: 23; Nakayama 1R and 7R share one canonical cell.
- Evaluation years: 2020–2026, grouped by complete race and strictly chronological.
- Training history: up to 20 years before each fold cutoff. Entrants in the evaluation races seed the cohort, then every prior race of those horses is eligible regardless of venue.
- Venue remains an explicit evaluation-cell dimension. Open class (`999`) additionally requires a normalized named-race identity; a generic `999` cell is forbidden.
- Model metrics and promotion guards cover winner inclusion at Top 1, 2, 3, 4, and 5.

Dedicated named cells:

- Nakayama 11R Radio Nippon Sho: `jra-cell-c9b3b62022b2b368`, `venue=06;distance=1200;season=autumn;surface=dirt;class=999;race=name:ラジオ日本賞`.
- Hanshin 11R Challenge Cup: `jra-cell-8c1fd54d5a4ba6e3`, `venue=09;distance=2000;season=autumn;surface=turf;class=999;race=name:チャレンジカップ`.

## Race-time correction

PC-KEIBA `soha_time` is packed `MSSd`, not ordinary elapsed tenths. All speed-feature calculations decode it at the raw-data boundary used by the calculation:

```text
1008 -> 1 minute, 00 seconds, 8 tenths -> 608 tenths -> 1:00.8
```

The decoder rejects zero, nonnumeric values, and impossible seconds (`>= 60`). It is not applied to `kohan_3f`, `kohan_4f`, or `time_sa`.

The relationship feature join also deduplicates source snapshots before joining. The corrected historical output has 1,304,722 rows and 1,304,722 unique `(race_id, horse_id)` identities; the pre-fix join incorrectly added 2,188 duplicate rows.

Retained immutable inputs:

- Cell authority manifest SHA-256: `331428e2282c6e7cd48cfb7581d498824930aae68e46da914f2407406db61329`.
- Production plan SHA-256: `17be1e25c174fa4ee1ebef79458d0645e65d8c9c5e8e97cd399d3faeec54fcc7`.
- All-history decoded sectional feature tree SHA-256: `20c4dc29d6f7250085a801dc1fc33bcf5274d0ad89064d558e43ada0b94892ec`.
- Deduplicated relationship-R1 feature tree SHA-256: `345f4e565fcdaa646dd3063c5eecc789f9632483a7c11c200628917916541604`.
- Current Prophet v6.1 comparator SHA-256: `a2ca67dddd8c46ffa08c96abf053caf30e068ec6f61f79286d93277322176452`.

## Reproducible evaluation entry point

`optimize_priority_jra_cells.py` accepts the production plan directly, selecting one representative target race per unique cell (while the production plan retains all target races in a shared cell):

```bash
cd apps/finish-position-predict-container
ROOT="$HOME/.local/share/horse-racing-data-production/jra-cell-v1"
: "${PG_URL:?Set PG_URL to an authorized read-only database connection}"
uv run python src/optimize_priority_jra_cells.py \
  --pg-url "$PG_URL" \
  --features-root "$ROOT/FEATURE_SNAPSHOT" \
  --production-plan "$ROOT/cell-authority-cohort-v8-20260912/production-20260912.json" \
  --target-date 2026-09-12 \
  --output "$ROOT/optimization.json" \
  --checkpoint-dir "$ROOT/checkpoints" \
  --iterations 100 --thread-count 8 \
  --input-revision 'authority=<sha256>;features=<parquet-tree-sha256>'
```

Compare the untouched holdout predictions with the current Prophet v6.1 graph on exact race/runner identities:

```bash
uv run python src/compare_priority_jra_to_current.py \
  --optimization "$ROOT/optimization.json" \
  --current "$ROOT/current-v61-predictions-2024-through-20260830-authoritative-union.parquet" \
  --output "$ROOT/comparison.json"
```

Promotion remains fail-closed unless the development guard, annual Top2–Top5 non-regression guard, complete same-identity current comparison, positive Top1 delta, PIT/cohort checks, target feature parity, Python/Worker score parity, and R2 byte attestation all pass. A literature-motivated feature is only a hypothesis until it passes these measurements.

The current-v6.1 replay contains zero class-`701` races (versus 759 observed 2024–2026 races in the feature source), while the global fallback evaluation covers 754. This is an incumbent-comparator coverage limitation, not a zero-delta result. Class-`701` target cells therefore remain fail-closed unless a candidate first passes the independent market guard; no tested candidate has done so. The unrelated global evaluation is not relabelled as deployed-current evidence.

## Relationship-R1 result (Top3 relevance)

Optimization SHA-256: `0eaa04dd640b49c95720020789211da436b6aa61f4c168c5e5642fc9ad16f392`. Comparison SHA-256: `2ce42ec21b1bf2e31ff7b7547748ccfaaa102257e3093dd2be72466d4c19287a`.

Three cells passed development selection, but none passed the untouched 2024–2026 market guard and none passed the complete same-identity current-v6.1 promotion gate. Deltas below are Top1/Top2/Top3/Top4/Top5 winner-capture hit counts. `Exact/current races` distinguishes exact holdout races from complete comparator intersections; `—` means the current export had no complete comparable race and therefore fails closed.

| Target                     | Cell                            | Selected candidate        | Exact/current races |          vs market |    vs current v6.1 | Gate                                    |
| -------------------------- | ------------------------------- | ------------------------- | ------------------: | -----------------: | -----------------: | --------------------------------------- |
| 06-06                      | `jra-cell-b3995877bbf23997`     | entrant-history, depth 8  |               6 / 0 |     +0/+0/+0/+0/+0 |                  — | REJECT                                  |
| 06-03                      | `jra-cell-09f7b5fcbd5851d8`     | related-distance, depth 6 |             14 / 14 |     +0/+0/+0/-1/+0 |     +0/+0/+0/+0/+0 | REJECT                                  |
| **06-11 Radio Nippon Sho** | **`jra-cell-c9b3b62022b2b368`** | entrant-history, depth 6  |           **1 / 1** | **+0/+0/+1/+0/+0** | **+0/+0/+0/+0/+1** | **REJECT: Top1 tie and one exact race** |
| 06-09                      | `jra-cell-836f5464e32dfccd`     | entrant-history, depth 6  |               4 / 4 |     +0/+0/+1/+2/+0 |     -1/-1/+0/+0/-1 | REJECT                                  |
| 06-05                      | `jra-cell-01b841423408e590`     | entrant-history, depth 8  |               9 / 0 |     +0/+0/+0/+0/+0 |                  — | REJECT                                  |
| 06-02                      | `jra-cell-3d9a76f2a918ffca`     | related-distance, depth 8 |             10 / 10 |     +0/+0/+0/-1/+0 |     +0/+0/+0/-1/+0 | REJECT                                  |
| 06-12                      | `jra-cell-378b8d1c6a3270ae`     | entrant-history, depth 6  |             17 / 17 |     +0/+0/+1/+0/+0 |     -3/-2/-1/-1/+0 | REJECT                                  |
| 06-01, 06-07               | `jra-cell-63c427c45d615ab3`     | related-distance, depth 8 |             16 / 16 |     +0/+0/+0/+0/+0 |     -3/+0/+0/+0/-1 | REJECT                                  |
| 06-10                      | `jra-cell-d29eba2329ff7ad3`     | venue-surface, depth 8    |               1 / 1 |     -1/+1/+0/+0/+0 |     +0/+0/+0/-1/+0 | REJECT                                  |
| 06-04                      | `jra-cell-d8550f3010c95308`     | entrant-history, depth 6  |               9 / 9 |     +0/+0/+0/+0/+0 |     +0/+0/+0/+0/+0 | REJECT                                  |
| 06-08                      | `jra-cell-04adb033dda53986`     | entrant-history, depth 6  |               2 / 2 |     -1/+0/+0/+1/+0 |     +0/+0/+0/+0/+0 | REJECT                                  |
| 09-12                      | `jra-cell-e00b892f47618e2c`     | entrant-history, depth 8  |               2 / 2 |     +1/+1/+0/-1/-1 |     +0/+0/+0/+0/+1 | REJECT                                  |
| 09-07                      | `jra-cell-7215c648f304972b`     | related-distance, depth 6 |               5 / 5 |     +0/+0/+0/+1/+1 |     +0/+1/+0/+0/+0 | REJECT                                  |
| 09-09                      | `jra-cell-4f8b5a913eab62ff`     | entrant-history, depth 6  |               1 / 1 |     -1/+0/+0/+0/+2 |     +0/+0/+0/+0/+0 | REJECT                                  |
| 09-05                      | `jra-cell-dbfbddf8c9fa76cc`     | related-distance, depth 8 |               3 / 0 |     -1/+1/-1/+0/+0 |                  — | REJECT                                  |
| 09-10                      | `jra-cell-a2ad90dd734aff22`     | entrant-history, depth 8  |               2 / 2 |     +0/+1/+1/+2/+4 |     +0/+0/+0/+0/+0 | REJECT                                  |
| 09-06                      | `jra-cell-cd2717996f11eb23`     | entrant-history, depth 8  |               3 / 0 |     +0/+2/+0/+0/+3 |                  — | REJECT                                  |
| 09-03                      | `jra-cell-1215b4b224302766`     | entrant-history, depth 8  |               9 / 9 |     -1/+1/+1/+1/+2 |     -3/+0/-1/+0/+0 | REJECT                                  |
| 09-08                      | `jra-cell-44cf73d7d12eb712`     | entrant-history, depth 6  |               2 / 2 |     +0/+1/+0/+0/+0 |     -1/+0/+0/+0/+0 | REJECT                                  |
| 09-04                      | `jra-cell-bf557078384d1ae5`     | entrant-history, depth 8  |               3 / 3 |     +0/+1/-1/+0/+0 |     +0/+0/+0/+0/-1 | REJECT                                  |
| 09-02                      | `jra-cell-76003f5e501c6f68`     | entrant-history, depth 8  |               4 / 4 |     +1/+1/-1/-2/+0 |     +0/+0/+0/-1/+0 | REJECT                                  |
| **09-11 Challenge Cup**    | **`jra-cell-8c1fd54d5a4ba6e3`** | entrant-history, depth 6  |           **1 / 1** | **+0/+1/+1/+1/+1** | **+0/+0/+0/+0/+0** | **REJECT: Top1 tie and one exact race** |
| 09-01                      | `jra-cell-cb75e5e481c1310e`     | entrant-history, depth 6  |               3 / 3 |     +0/+0/+0/+0/+0 |     -2/-1/+0/+0/+0 | REJECT                                  |

No relationship-R1 artifact is authorized for publication or target rescore. Rejected fold checkpoints were removed; the full JSON report was retained as gzip (reversible to the recorded uncompressed SHA-256) together with the 32 KB comparison report.

## Sectional-only result (Top3 relevance)

Optimization SHA-256: `3b9383b9680540151f264517635867ceba2b1ed917164cdfcdc555ceb1f5a639`. Comparison SHA-256: `99a67b28b21d37dd377640fe3eef30624681ff9ad92853ba8d8c0234efb77617`.

Only one cell passed development selection; no cell passed the untouched holdout market guard or current-v6.1 promotion gate.

| Target                     | Cell                            | Exact/current races | vs market Top1/2/3/4/5 | vs current Top1/2/3/4/5 | Gate       |
| -------------------------- | ------------------------------- | ------------------: | ---------------------: | ----------------------: | ---------- |
| 06-06                      | `jra-cell-b3995877bbf23997`     |               6 / 0 |         +0/+0/+0/+0/+0 |                       — | REJECT     |
| 06-03                      | `jra-cell-09f7b5fcbd5851d8`     |             14 / 14 |         +0/-1/+0/-1/+0 |          +0/-1/+0/+0/+0 | REJECT     |
| **06-11 Radio Nippon Sho** | **`jra-cell-c9b3b62022b2b368`** |           **1 / 1** |     **-1/+0/+0/+0/+0** |      **+0/+0/+0/+0/+1** | **REJECT** |
| 06-09                      | `jra-cell-836f5464e32dfccd`     |               4 / 4 |         +0/+1/+1/+2/+0 |          -1/+0/+0/+0/+0 | REJECT     |
| 06-05                      | `jra-cell-01b841423408e590`     |               9 / 0 |         +0/+0/+0/+0/+0 |                       — | REJECT     |
| 06-02                      | `jra-cell-3d9a76f2a918ffca`     |             10 / 10 |         +0/+0/+0/-1/+0 |          +0/+0/+0/-1/+0 | REJECT     |
| 06-12                      | `jra-cell-378b8d1c6a3270ae`     |             17 / 17 |         +0/+0/+0/-1/+0 |          -3/-2/-2/-2/+0 | REJECT     |
| 06-01, 06-07               | `jra-cell-63c427c45d615ab3`     |             16 / 16 |         +0/+0/+0/+0/+0 |          -3/+0/+0/+0/-1 | REJECT     |
| 06-10                      | `jra-cell-d29eba2329ff7ad3`     |               1 / 1 |         +0/+2/-1/+3/+0 |          +0/+0/+0/+0/+0 | REJECT     |
| 06-04                      | `jra-cell-d8550f3010c95308`     |               9 / 9 |         +0/+0/-1/+0/+0 |          +0/+0/-1/+0/+0 | REJECT     |
| 06-08                      | `jra-cell-04adb033dda53986`     |               2 / 2 |         +0/+0/+0/+1/+0 |          +0/+0/+0/+0/+0 | REJECT     |
| 09-12                      | `jra-cell-e00b892f47618e2c`     |               2 / 2 |         +0/+0/+0/+0/-2 |          +0/+0/+0/+0/+0 | REJECT     |
| 09-07                      | `jra-cell-7215c648f304972b`     |               5 / 5 |         +0/+0/+0/+1/+2 |          +0/+1/+0/+0/+0 | REJECT     |
| 09-09                      | `jra-cell-4f8b5a913eab62ff`     |               1 / 1 |         -1/+0/+1/+1/+0 |          +0/+0/+0/+0/+0 | REJECT     |
| 09-05                      | `jra-cell-dbfbddf8c9fa76cc`     |               3 / 0 |         -1/+1/-1/-1/+0 |                       — | REJECT     |
| 09-10                      | `jra-cell-a2ad90dd734aff22`     |               2 / 2 |         +0/+1/+1/+2/+5 |          +0/+0/+0/+0/+0 | REJECT     |
| 09-06                      | `jra-cell-cd2717996f11eb23`     |               3 / 0 |         -1/+1/+1/+3/+3 |                       — | REJECT     |
| 09-03                      | `jra-cell-1215b4b224302766`     |               9 / 9 |         -1/+1/+0/+0/+3 |          -3/+0/-1/+0/+0 | REJECT     |
| 09-08                      | `jra-cell-44cf73d7d12eb712`     |               2 / 2 |         -1/+1/+0/-1/-1 |          -1/+0/+0/+0/+0 | REJECT     |
| 09-04                      | `jra-cell-bf557078384d1ae5`     |               3 / 3 |         +0/+1/-1/+0/+0 |          +0/+0/+0/+0/-1 | REJECT     |
| 09-02                      | `jra-cell-76003f5e501c6f68`     |               4 / 4 |         +0/+1/-1/+0/-1 |          +0/+0/+1/+0/+0 | REJECT     |
| **09-11 Challenge Cup**    | **`jra-cell-8c1fd54d5a4ba6e3`** |           **1 / 1** |     **+0/+0/-1/-1/+1** |      **+0/+0/+0/+0/+0** | **REJECT** |
| 09-01                      | `jra-cell-cb75e5e481c1310e`     |               3 / 3 |         +0/+0/+0/+0/+0 |          -2/-1/+0/+0/+0 | REJECT     |

No sectional-only artifact is authorized for publication or target rescore. Rejected checkpoints were removed and the full report was retained in reversible gzip form.

## Relationship-R1 result (reciprocal-rank relevance)

Optimization SHA-256: `038660558c6f1d2d20a4dc9bbf70b9921f7b44052d71b9d6731bd87c04fc5354`. Comparison SHA-256: `282ba8fd2a088cb2a1ef843ec48dcea7808d7880f31a11a2f8ff32c8dee7e9f9`.

Four cells passed development selection and one (`09-06`, class `701`) passed the market holdout guard. That cell has zero complete current-v6.1 comparison races, so it remains fail-closed rather than treating absent incumbent evidence as improvement. No cell passed the current promotion gate.

| Target                     | Cell                            | Exact/current races | vs market Top1/2/3/4/5 | vs current Top1/2/3/4/5 | Gate                                               |
| -------------------------- | ------------------------------- | ------------------: | ---------------------: | ----------------------: | -------------------------------------------------- |
| 06-06                      | `jra-cell-b3995877bbf23997`     |               6 / 0 |         +0/+0/+1/+0/+0 |                       — | REJECT                                             |
| 06-03                      | `jra-cell-09f7b5fcbd5851d8`     |             14 / 14 |         +0/+0/+0/-1/+0 |          +0/+0/+0/+0/+0 | REJECT                                             |
| **06-11 Radio Nippon Sho** | **`jra-cell-c9b3b62022b2b368`** |           **1 / 1** |     **-2/+0/+0/+1/+0** |      **+0/+0/+0/+0/+0** | **REJECT**                                         |
| 06-09                      | `jra-cell-836f5464e32dfccd`     |               4 / 4 |         +0/+0/+1/+1/-1 |          -1/-1/+0/+0/-1 | REJECT                                             |
| 06-05                      | `jra-cell-01b841423408e590`     |               9 / 0 |         +0/+0/+0/+0/+0 |                       — | REJECT                                             |
| 06-02                      | `jra-cell-3d9a76f2a918ffca`     |             10 / 10 |         -1/+0/+0/-1/+0 |          -1/+0/+0/-1/+0 | REJECT                                             |
| 06-12                      | `jra-cell-378b8d1c6a3270ae`     |             17 / 17 |         +0/+1/+1/-1/+0 |          -3/-1/-1/-2/+0 | REJECT                                             |
| 06-01, 06-07               | `jra-cell-63c427c45d615ab3`     |             16 / 16 |         +0/+0/+0/+0/+1 |          -3/+0/+0/+0/+0 | REJECT                                             |
| 06-10                      | `jra-cell-d29eba2329ff7ad3`     |               1 / 1 |         +0/+1/+0/+1/+0 |          +0/+0/+0/+0/+0 | REJECT                                             |
| 06-04                      | `jra-cell-d8550f3010c95308`     |               9 / 9 |         +0/+0/+0/+0/+0 |          +0/+0/+0/+0/+0 | REJECT                                             |
| 06-08                      | `jra-cell-04adb033dda53986`     |               2 / 2 |         +0/+0/+0/+0/+0 |          +0/+0/+0/+0/+0 | REJECT                                             |
| 09-12                      | `jra-cell-e00b892f47618e2c`     |               2 / 2 |         +1/+1/-3/+0/-1 |          +0/+0/+0/+0/+1 | REJECT                                             |
| 09-07                      | `jra-cell-7215c648f304972b`     |               5 / 5 |         +0/+0/-1/-1/+1 |          +0/+1/+0/+0/+0 | REJECT                                             |
| 09-09                      | `jra-cell-4f8b5a913eab62ff`     |               1 / 1 |         -1/+1/-1/+1/+1 |          +0/+1/+0/+0/+0 | REJECT                                             |
| 09-05                      | `jra-cell-dbfbddf8c9fa76cc`     |               3 / 0 |         -1/+1/-1/+0/-1 |                       — | REJECT                                             |
| 09-10                      | `jra-cell-a2ad90dd734aff22`     |               2 / 2 |         +0/+2/+1/+1/+5 |          +0/+0/+0/+0/+0 | REJECT                                             |
| **09-06**                  | **`jra-cell-cd2717996f11eb23`** |           **3 / 0** |     **+1/+2/+0/+1/+5** |                   **—** | **REJECT: market pass, current comparison absent** |
| 09-03                      | `jra-cell-1215b4b224302766`     |               9 / 9 |         -1/+1/-1/+0/+1 |          -3/+0/-2/+0/+0 | REJECT                                             |
| 09-08                      | `jra-cell-44cf73d7d12eb712`     |               2 / 2 |         -1/+2/+0/+0/+0 |          -1/+0/+0/+0/+0 | REJECT                                             |
| 09-04                      | `jra-cell-bf557078384d1ae5`     |               3 / 3 |         +0/+1/-1/+0/+0 |          +0/+0/+0/+0/-1 | REJECT                                             |
| 09-02                      | `jra-cell-76003f5e501c6f68`     |               4 / 4 |         +1/+1/-1/+1/-1 |          +0/+0/+0/+0/+0 | REJECT                                             |
| **09-11 Challenge Cup**    | **`jra-cell-8c1fd54d5a4ba6e3`** |           **1 / 1** |     **+1/+0/-1/-2/+0** |      **+0/+0/+0/+0/+0** | **REJECT**                                         |
| 09-01                      | `jra-cell-cb75e5e481c1310e`     |               3 / 3 |         +0/+0/+0/+0/+0 |          -2/-1/+0/+0/+0 | REJECT                                             |

No reciprocal-rank artifact is authorized for publication or target rescore.

## Production/UI preflight

At 01:46 JST, authenticated read-only requests for both dedicated 11R pages returned all three sections (`finish-prediction`, `condition`, and `win-rate-heatmap`) with 16 runners each. Each heatmap contained jockey, trainer, jockey-frame, and seven pedigree categories. Recheck the same sections after every accepted target-only rescore; HTTP success alone is not score or rendering parity.

## TimesFM-3 cell-custom follow-up

At the owner's request, TimesFM-3 is being reconsidered with cell-specific adaptation rather than a single global policy. The immutable JRA history export contains 1,295,370 unique `(race_id, horse_id)` rows over 95,611 races from 2000-01-05 through 2026-09-06; SHA-256 `9f12304770710ae59af91d662279656534330b5457be23eb3d6e5a91f17948e9`. Every fold begins with all strictly prior races of its evaluation entrants across all venues. Related-distance and venue-surface scopes may add peers but cannot remove entrant history.

Profiles vary structurally for newcomer, sprint, turf-route, balanced, and normalized named-open cells. Expensive TimesFM forecasts are cached once; Rustuna 0.1.0 then runs resumable Rust TPE over profile/scope selection, market blend, and performance/speed/final-3F/pace readout weights (10,000 trials per cell by default). Development remains 2020–2023 and holdout remains 2024–2026. The owner confirmed the deployed Cloudflare installation is self-only and non-commercial. Rustuna is search infrastructure and does not alter inference accuracy by itself; it remains enabled to make large local HPO practical. TimesFM is adopted independently and proactively for any cell that measurably improves untouched Top1 while preserving annual Top2–Top5 and passing the same market/current/PIT/parity/attestation gates. Failure of one profile/readout rejects only that adaptation, not TimesFM for the cell.

The first 9-cell checkpoint (diagnostic SHA-256 `40e6f413e74162ba796c15b0a51294361f1d1ed7d11239fc1d0b2798d8bf7e15`) exposed two concrete evaluation/adaptation defects. For Radio Nippon Sho, forecasting absolute performance levels perturbed strong market ordering without a stable market-relative signal, producing negative development and holdout deltas. Also, the current-v6.1 comparator ends on 2026-08-30 while fold evidence extends through 2026-09-06; requiring comparator identities after its attested cutoff incorrectly made otherwise covered cells incomplete. The diagnostic run was stopped and is ineligible for promotion.

The corrected adaptation forecasts each horse's historical performance percentile residual over its race-local pre-race market percentile. Rustuna now has a deterministic exact-market fallback and an explicit objective penalty for every annual Top2–Top5 violation, so an unhelpful or depth-regressing temporal readout cannot be reported as better than the zero-delta baseline. Current comparison is strict through its attested 2026-08-30 boundary, while later holdout races remain in the market gate. Rejected cell IDs can be rerun independently with `--cell-id`; follow-up tuning remains development-fold-only and is reported separately to expose sequential model-selection evidence.

A second development-only diagnostic is sparse horse history: Radio Nippon proxy folds had only about 40–42% history availability in 2020–2023. Applying a common fallback temporal value to the remaining runners can move otherwise correct market ranks. The next readout therefore exposes `minimum_history_count` values 1/2/3/5/8 to Rustuna and leaves runners below the selected threshold exactly on their market score. This keeps all-venue history in scope while making TimesFM influence conditional on adequate horse-specific context.

The next predeclared representation addresses cross-horse scale calibration without removing any history: each horse's pre-cutoff residual context is standardized using only that horse's fitting rows, TimesFM forecasts the normalized trajectory, and the forecast is mapped back with the same pre-cutoff location and scale before race-local ranking. This tests whether foundation-model dynamics add value after preserving each horse's empirical performance level.

Because race ranks are piecewise constant, a continuous blend sampler can miss an improvement that exists only at the exact pure-TimesFM boundary. The follow-up therefore searches an explicit market-weight grid containing both exact endpoints `0.0` and `1.0` plus dense near-endpoint and interior values. The initial 8-cell minimum-history attempt used the continuous sampler, produced only baseline selections, and was stopped as an HPO diagnostic rather than retained as eligible evidence. The corrected follow-up combines exact-grid blending, history thresholds, and horse-normalized residual profiles.

A further predeclared PIT-safe adaptation replaces the coarse outer-year recursive horizon with rolling one-step origins. For each evaluation race it consumes every actual all-venue start strictly before that race date, including earlier starts in the same evaluation year, while excluding same-date and future outcomes. This addresses stale January-only contexts without weakening chronological isolation; outer-year forecasting remains as an explicit comparator profile.

Per-trial diagnostics then exposed a selection-adapter defect rather than a Rustuna accuracy issue: some cells had thousands of feasible objective-positive TimesFM trials, while the adapter still consumed `study.best_trial` and emitted the market fallback. The adapter now independently audits all completed trials after Rustuna optimization and selects the maximum feasible TimesFM objective before applying the deterministic baseline comparison. A regression test requires a known improving surface to return a positive, non-market best result. The affected partial run was discarded and restarted; it is not promotion evidence.

### TimesFM level/residual round 1 result

Report SHA-256: `6c3896d180b3c2b2fdda6c3b0e2b1074b75aa77465f6f96154d275727181d084`. All 23 cells selected the deterministic market-only fallback: 0 development passes, 0 market passes, 12 complete current comparisons, and 0 production passes. `current` compares the selected market fallback to current v6.1 only through its attested 2026-08-30 cutoff. Deltas are Top1/Top2/Top3/Top4/Top5 hit counts.

| Target                 | Cell                        | Selected surface        |  Dev vs market | Holdout vs market |        Current | Decision          |
| ---------------------- | --------------------------- | ----------------------- | -------------: | ----------------: | -------------: | ----------------- |
| 06-06                  | `jra-cell-b3995877bbf23997` | newcomer-pace           | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 06-03                  | `jra-cell-09f7b5fcbd5851d8` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+1/+0 | reject adaptation |
| 06-11 Radio Nippon Sho | `jra-cell-c9b3b62022b2b368` | named-open-complete     | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 06-09                  | `jra-cell-836f5464e32dfccd` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+1/-1/-1/+2 | reject adaptation |
| 06-05                  | `jra-cell-01b841423408e590` | newcomer-pace           | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 06-02                  | `jra-cell-3d9a76f2a918ffca` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+0/+0 | reject adaptation |
| 06-12                  | `jra-cell-378b8d1c6a3270ae` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -3/-2/-2/-1/+0 | reject adaptation |
| 06-01                  | `jra-cell-63c427c45d615ab3` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -3/+0/+0/+0/-1 | reject adaptation |
| 06-10                  | `jra-cell-d29eba2329ff7ad3` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 06-04                  | `jra-cell-d8550f3010c95308` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+0/+0 | reject adaptation |
| 06-08                  | `jra-cell-04adb033dda53986` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +2/-2/-1/-1/+0 | reject adaptation |
| 09-12                  | `jra-cell-e00b892f47618e2c` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-07                  | `jra-cell-7215c648f304972b` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -3/-1/+0/+0/-2 | reject adaptation |
| 09-09                  | `jra-cell-4f8b5a913eab62ff` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-05                  | `jra-cell-dbfbddf8c9fa76cc` | newcomer-pace           | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-10                  | `jra-cell-a2ad90dd734aff22` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-06                  | `jra-cell-cd2717996f11eb23` | newcomer-pace           | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-03                  | `jra-cell-1215b4b224302766` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -4/-4/-2/-1/-3 | reject adaptation |
| 09-08                  | `jra-cell-44cf73d7d12eb712` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-04                  | `jra-cell-bf557078384d1ae5` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -5/-6/-4/-3/-1 | reject adaptation |
| 09-02                  | `jra-cell-76003f5e501c6f68` | performance-over-market | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -5/-4/-3/-3/+0 | reject adaptation |
| 09-11 Challenge Cup    | `jra-cell-8c1fd54d5a4ba6e3` | named-open-complete     | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation |
| 09-01                  | `jra-cell-cb75e5e481c1310e` | balanced                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -1/+1/+0/-1/+0 | reject adaptation |

### TimesFM exact-grid/history-gated round 2 result

Report SHA-256: `91c781cf3bc59daa358303e6d9f7b1934feccb48d6df620c83561fcdb9d4aec1`. Exact-grid blending, minimum history, and horse-normalized residuals produced 7 development passes, 0 annual market passes, 12 complete current comparisons, and 0 production passes. Deltas are Top1/Top2/Top3/Top4/Top5 hits.

| Target                 | Cell                        | Selected surface               |  Dev vs market | Holdout vs market |        Current | Decision                 |
| ---------------------- | --------------------------- | ------------------------------ | -------------: | ----------------: | -------------: | ------------------------ |
| 06-06                  | `jra-cell-b3995877bbf23997` | newcomer-pace                  | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation        |
| 06-03                  | `jra-cell-09f7b5fcbd5851d8` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+1/+0 | reject adaptation        |
| 06-11 Radio Nippon Sho | `jra-cell-c9b3b62022b2b368` | residual, min 5, market .99    | +2/+1/+0/+1/+0 |    -1/+0/+0/-1/-1 |     incomplete | reject adaptation        |
| 06-09                  | `jra-cell-836f5464e32dfccd` | balanced, min 5, market .95    | +1/+1/+0/+0/+0 |    +1/+0/+2/+2/+0 | +1/+1/+1/+1/+2 | reject: annual 2026 Top2 |
| 06-05                  | `jra-cell-01b841423408e590` | newcomer-pace                  | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation        |
| 06-02                  | `jra-cell-3d9a76f2a918ffca` | balanced                       | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+0/+0 | reject adaptation        |
| 06-12                  | `jra-cell-378b8d1c6a3270ae` | residual expanded, min 5       | +0/+2/+3/+3/+0 |    +1/+2/+3/+1/+0 | -2/-1/+1/+0/+0 | reject: dev Top1         |
| 06-01                  | `jra-cell-63c427c45d615ab3` | balanced                       | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -3/+0/+0/+0/-1 | reject adaptation        |
| 06-10                  | `jra-cell-d29eba2329ff7ad3` | normalized residual, min 5     | +2/+2/+1/+3/+4 |    +2/+0/+2/+5/+0 |     incomplete | reject: annual 2024 Top2 |
| 06-04                  | `jra-cell-d8550f3010c95308` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +0/+0/+0/+0/+0 | reject adaptation        |
| 06-08                  | `jra-cell-04adb033dda53986` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | +2/-2/-1/-1/+0 | reject adaptation        |
| 09-12                  | `jra-cell-e00b892f47618e2c` | performance expanded, min 1    | +0/+1/+0/+0/+0 |    +5/+2/+2/+1/+0 |     incomplete | reject: dev Top1         |
| 09-07                  | `jra-cell-7215c648f304972b` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -3/-1/+0/+0/-2 | reject adaptation        |
| 09-09                  | `jra-cell-4f8b5a913eab62ff` | sprint-turf expanded, min 2    | +2/+1/+1/+0/+0 |    -1/-2/-3/+1/+0 |     incomplete | reject adaptation        |
| 09-05                  | `jra-cell-dbfbddf8c9fa76cc` | newcomer-pace                  | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation        |
| 09-10                  | `jra-cell-a2ad90dd734aff22` | balanced expanded, min 3       | +1/+0/+1/+1/+0 |    -2/+4/-2/+2/+1 |     incomplete | reject adaptation        |
| 09-06                  | `jra-cell-cd2717996f11eb23` | newcomer-pace                  | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation        |
| 09-03                  | `jra-cell-1215b4b224302766` | balanced                       | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -4/-4/-2/-1/-3 | reject adaptation        |
| 09-08                  | `jra-cell-44cf73d7d12eb712` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |     incomplete | reject adaptation        |
| 09-04                  | `jra-cell-bf557078384d1ae5` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -5/-6/-4/-3/-1 | reject adaptation        |
| 09-02                  | `jra-cell-76003f5e501c6f68` | normalized residual            | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 | -5/-4/-3/-3/+0 | reject adaptation        |
| 09-11 Challenge Cup    | `jra-cell-8c1fd54d5a4ba6e3` | named complete expanded, min 1 | +1/+3/+0/+2/+0 |    +3/+0/-2/-4/+0 |     incomplete | reject adaptation        |
| 09-01                  | `jra-cell-cb75e5e481c1310e` | balanced expanded, min 8       | +5/+3/+1/+0/+0 |    -1/-3/-1/+1/+1 | -2/-2/-1/+0/+1 | reject adaptation        |

### TimesFM rolling-origin round 3 result

Report SHA-256: `0cb9fbaddfb521e63003d73051993db1983bf85e46b78519c81b898c0ad2c9a7`. The PIT-safe same-year entrant-history expansion and rolling candidate produced 8 development passes, 0 annual market passes, 12 complete current comparisons, 3 selected rolling profiles, and 0 production passes. No candidate was published, activated, or rescored.

| Target                 | Cell                        | Selected surface             |  Dev vs market | Holdout vs market |          Current | Decision                 |
| ---------------------- | --------------------------- | ---------------------------- | -------------: | ----------------: | ---------------: | ------------------------ |
| 06-06                  | `jra-cell-b3995877bbf23997` | newcomer-pace                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |       incomplete | reject adaptation        |
| 06-03                  | `jra-cell-09f7b5fcbd5851d8` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   +0/+0/+0/+1/+0 | reject adaptation        |
| 06-11 Radio Nippon Sho | `jra-cell-c9b3b62022b2b368` | normalized residual expanded | +2/+1/+0/+1/+0 |    -1/+0/+0/-1/-1 |       incomplete | reject adaptation        |
| 06-09                  | `jra-cell-836f5464e32dfccd` | balanced                     | +1/+2/+1/+1/+0 |    -1/-5/-4/-1/-5 |   -1/-3/-5/-4/-4 | reject adaptation        |
| 06-05                  | `jra-cell-01b841423408e590` | newcomer-pace                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |       incomplete | reject adaptation        |
| 06-02                  | `jra-cell-3d9a76f2a918ffca` | rolling residual             | +2/+1/+1/+1/+0 |    +0/+0/+0/-1/+0 |   +0/+0/+0/-1/+0 | reject: holdout Top4     |
| 06-12                  | `jra-cell-378b8d1c6a3270ae` | normalized residual expanded | +0/+2/+3/+3/+0 |    +1/+2/+3/+1/+0 |   -2/-1/+1/+0/+0 | reject: dev Top1         |
| 06-01                  | `jra-cell-63c427c45d615ab3` | balanced                     | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   -3/+0/+0/+0/-1 | reject adaptation        |
| 06-10                  | `jra-cell-d29eba2329ff7ad3` | residual related-distance    | +2/+2/+1/+3/+4 |    +2/+0/+2/+5/+0 |       incomplete | reject: annual 2024 Top2 |
| 06-04                  | `jra-cell-d8550f3010c95308` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   +0/+0/+0/+0/+0 | reject adaptation        |
| 06-08                  | `jra-cell-04adb033dda53986` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   +2/-2/-1/-1/+0 | reject adaptation        |
| 09-12                  | `jra-cell-e00b892f47618e2c` | performance expanded         | +0/+1/+0/+0/+0 |    +5/+2/+2/+1/+0 |       incomplete | reject: dev Top1         |
| 09-07                  | `jra-cell-7215c648f304972b` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   -3/-1/+0/+0/-2 | reject adaptation        |
| 09-09                  | `jra-cell-4f8b5a913eab62ff` | sprint-turf                  | +2/+1/+1/+0/+0 |    -2/-2/-3/+1/+0 |       incomplete | reject adaptation        |
| 09-05                  | `jra-cell-dbfbddf8c9fa76cc` | newcomer-pace                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |       incomplete | reject adaptation        |
| 09-10                  | `jra-cell-a2ad90dd734aff22` | balanced                     | +2/+0/+0/+1/+0 |    -1/+4/+1/+1/+0 |       incomplete | reject adaptation        |
| 09-06                  | `jra-cell-cd2717996f11eb23` | newcomer-pace                | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |       incomplete | reject adaptation        |
| 09-03                  | `jra-cell-1215b4b224302766` | rolling residual             | +0/+0/+1/+0/+0 |  -5/-5/-8/-13/-13 | -8/-7/-8/-13/-15 | reject adaptation        |
| 09-08                  | `jra-cell-44cf73d7d12eb712` | rolling residual             | +1/+2/+3/+1/+0 |    -6/-5/-1/+0/-4 |       incomplete | reject adaptation        |
| 09-04                  | `jra-cell-bf557078384d1ae5` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   -5/-6/-4/-3/-1 | reject adaptation        |
| 09-02                  | `jra-cell-76003f5e501c6f68` | normalized residual          | +0/+0/+0/+0/+0 |    +0/+0/+0/+0/+0 |   -5/-4/-3/-3/+0 | reject adaptation        |
| 09-11 Challenge Cup    | `jra-cell-8c1fd54d5a4ba6e3` | named-open-finish expanded   | +0/+2/+1/+1/+0 |    +0/-2/-1/-2/-1 |       incomplete | reject adaptation        |
| 09-01                  | `jra-cell-cb75e5e481c1310e` | balanced                     | +5/+3/+1/+0/+0 |    -2/-3/-1/+1/+1 |   -3/-2/-1/+0/+1 | reject adaptation        |

### Final pre-race attestation

At 2026-09-12 07:51 JST and again at 08:50 JST, both named 11Rs exposed 16 card runners, 16 finish-prediction feature rows, 16 condition runners, 16 horse-rate rows, and 224 pedigree heatmap rows. Local `jvd_se` also contained 16 runners for each race with zero non-normal `ijo_kubun_code` scratch flags. Radio Nippon Sho predictions were generated at `2026-09-11T18:40:33.109Z` and Challenge Cup predictions at `2026-09-11T19:00:41.976Z`; all 32 rows retained incumbent model `jra-cb-stage1-marketfree235-2013`. The JRA day-base key `feat-daybase/catalog-v1/jra/20260912/features.parquet` remained absent in R2, although incumbent predictions and all preserved UI/heatmap sections were serving. The locally available viewer token was not the cron `TRIGGER_TOKEN`, so the unauthorized admin prewarm was not bypassed.

Final TimesFM package gates passed with Ruff and basedpyright clean, 172 tests, and 97.28% coverage. Durable implementation commit `68605c47` and the following documentation commits are on `agent/jra-0912-cell-loop`; no campaign worktree was created for these commits. No TimesFM candidate was deployed because all three adaptation rounds produced zero annual market passes and zero production passes.

## Correctness deployment

The MSSd/dedup correctness commits were cherry-picked onto latest main and deployed with both prediction queues drained. Worker version `f584b569-1587-448a-99ba-0086b809c85d` is at 100%; both finish-position Container applications report `ready` on image `f584b569`. Prediction delivery was resumed and rejected-model rescoring was intentionally skipped.

## Campaign status

Two early sectional trial snapshots were terminated because one retained duplicate suffixed weight columns and the next staged clock history only from 2006 instead of the full 2000–2026 evaluation source. Their checkpoints were deleted and are not eligible evidence. The retained all-history dataset has one row per runner/race and production-compatible feature names. Top3-relevance relationship-R1, sectional-only, and reciprocal-rank relationship-R1 candidates were rejected for every cell. The reciprocal run produced one market-guard pass, but its class-`701` incumbent comparison was absent and therefore failed closed. No 2026-09-12 model was published, activated, or rescored from these experiments.
