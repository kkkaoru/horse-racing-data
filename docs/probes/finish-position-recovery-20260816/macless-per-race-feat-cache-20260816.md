# Mac-less per-race feat-cache HIT (code facts + 8/17 change)

Two separate problems on 8/16:

1. **No object** — 63 races 404. Writer not reached.
2. **Degenerate object** — 10 HITs written with dead JRA pedigree.

## Does CacheMiss fallback PUT?

**Yes**, before this change.

`iter_predict_chunks` (`serve.py`): after a scoped `mode=rescore`
`CacheMissError`, it rebuilds via `predict_fn` under
`activate_scoped_rescore_cache_miss_fallback()` (`LAYER_CHAIN` +
`--target-race`). Then, still on the same success path, it always called
`per_race_parquet_payload_fn` (`params.mode` is still `"rescore"`). The
Worker NDJSON proxy `FEATURES_CACHE.put`s `perRaceParquets`.

That is how a first-of-day weight rescore could **lock in** a dead
pedigree vector. Enabling `DAY_BASE_SPLIT_ENABLED` does not help: the
fallback **skips** split (`predict_upcoming.py` ~1176–1191).

`mode=full` focused-full is a **different** writer: detached thread →
in-process store → `GET /focused-full-cache` pickup. Pickup used to run
only on the “Neon already complete, skip /predict” path, not on the
HTTP `success` ack after a real run. That is problem (1) for morning
focused-full.

## Change (this commit)

- Do **not** embed `perRaceParquets` when
  `is_scoped_rescore_cache_miss_fallback()` is true.
- After a focused-full HTTP `success` or container `already-complete`,
  call `pickUpFocusedFullCache` (same as the Neon-complete skip path).

Morning `mode=full` can then PUT a cache. Weight `mode=rescore` HITs it
and only overlays odds/bataiju. A CacheMiss rebuild still predicts, but
does not poison R2.

Still required for live pedigree on the **first** `mode=full` of a day:
day-base without `--target-race` (`DAY_BASE_SPLIT` + working PREWARM /
`ensure_day_base`). Not flipped tonight.

Commit `6793ad7f` also contains unrelated A8 probe files
(`a8-early-market-board-0712-20260816.md`, `a8-main-generation-20260816.md`,
`generate_a8_main.py`, `test_generate_a8_main.py`). They were not part of
this change. Do not amend.

## 09:09 weight observation (confound)

`04/01` Neon `prediction_generated_at` is **07:07:10Z** (production
focused-full overwrite, likely dead pedigree). `07/01` is still
**05:04:08Z** (tonight’s full-day host generate). Rank movement on 04/01
alone cannot separate “cache seed restored pedigree” from “reverted the
07:07 overwrite”. Observe **both** 04/01 and 07/01 after weight rescore.
Baselines:

- `neon-0401-ranks-before-weight-20260816.tsv` (gen 07:07:10Z)
- `neon-0701-ranks-before-weight-20260816.tsv` (gen 05:04:08Z)
- `neon-0101-ranks-before-weight-20260816.tsv` (gen 05:03:55Z)

Also record, on the **same** rescore row: did `tansho_odds` / `odds_score`
change, or only `weight_diff_from_avg` / bataiju? Advisor local PG at 08:41
had JRA odds **0/500** as well as weights. `apply_fresh_snapshots` refreshes
both. If odds are still empty at 09:09, the rank delta is weight-only (or
pedigree restore on 04/01). If both fields move in one `generated_at`, do
not attribute the rank move to weight alone — write that they are not
separable without a second snapshot.
