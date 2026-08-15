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
