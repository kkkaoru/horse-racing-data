# 04/12 feat-cache provenance (read-only, 2026-08-16 07:12 JST)

No PUT, no delete, no deploy. HEAD + Neon SELECT only.

## 1. When was the R2 object written?

Key (code, `build_r2_per_race_feat_cache_key`):

`feat-cache/catalog-v1/jra/20260816/04/12/features.parquet`

Signed HEAD at 07:12 JST:

| field          | value                                                |
| -------------- | ---------------------------------------------------- |
| Last-Modified  | **Sat, 15 Aug 2026 19:09:35 GMT** = **04:09:35 JST** |
| Content-Length | 87178                                                |
| Content-Type   | application/octet-stream                             |

Day-level key `feat-cache/catalog-v1/jra/20260816/features.parquet` is
**404**. Tonight's object is per-race only.

Relative to tonight's clocks:

- host JRA one-shot score/flush finished **20:04:07 UTC = 05:04 JST**.
  That is **after** this R2 object.
- 03:44 JST host start / 03:44:49 earlier Neon gen (advisor) is
  **before** this R2 object if that stamp is JST. The **current** Neon
  row for 04/12 is not 03:44.

So the cache is **not** the 05:04 host one-shot. It is also **not** a
post-05:04 overwrite. It sits between the early container gens and the
later host UPSERT.

## 2. Same execution as current Neon 04/12?

Neon now (`kaisai_nen=2026`, `kaisai_tsukihi=0816`, `04/12`):

| field                     | value                                                |
| ------------------------- | ---------------------------------------------------- |
| rows                      | 15                                                   |
| `prediction_generated_at` | **2026-08-15 20:04:07.973351+00** = **05:04:07 JST** |
| `model_version`           | `jra-cb-stage1-marketfree235-2013`                   |
| distinct gen timestamps   | 1                                                    |

R2 Last-Modified 04:09:35 JST vs Neon 05:04:07 JST: **~55 minutes
apart**. Not the same write.

What the 04:09 object _is_ cannot be proven from HEAD alone. What the
code allows at that hour:

- Per-race key is seeded by **focused-full**
  (`_seed_focused_full_per_race_payloads`) via later
  `pickUpFocusedFullCache` (GET `/focused-full-cache` after Neon
  completion). Live NDJSON on focused-full returns `accepted` first, so
  the HTTP stream itself does not PUT.
- Day-batch / blocking `/predict` can embed `parquetBase64` on the live
  NDJSON last line; that path writes the **day-level** key
  (`build_r2_feat_cache_key`), which is 404 tonight, so that is not
  this object.
- `mode=rescore` **reads** this key; it does not seed it.

Allowed conclusion: this object was written by a **focused-full pickup**
(or an equivalent per-race seed), **not** by the 05:04 host one-shot
and **not** by a whole-day `/predict` PUT. Which race-night job at
04:09 produced it is **not in the object headers**.

## 3. Does the writer recompute features?

No. Both working PUT paths copy bytes:

1. Container reads local parquet (`read_bytes`), `base64.b64encode`.
2. Worker `atob` → `Uint8Array` → `FEATURES_CACHE.put` (`container-ndjson-proxy.ts`).
   Direct SigV4 PUT was removed in `41adee27`.

There is no Python/TS feature recompute on the PUT path. `atob` /
`b64encode` can theoretically corrupt bytes if the payload is truncated;
that would not systematically turn one rank column into another integer
across 14/15 horses. A dtype rewrite (float64 → int64) is **not** in
this path.

## What this can and cannot explain

Can explain 27-column / 47-column diffs vs tonight's local split or
`layer-16`: **different write times, different jobs**. Local 05:04
scored `feat-jra-layer-16`. R2 04:09 is an earlier container
focused-full seed. They are not required to match.

Cannot, from this HEAD, prove which of those two is "what production
would serve at 09:40". Current Neon 04/12 **is** the 05:04 host write.
A later rescore that HITs this 04:09 object would score the older
parquet, not the 05:04 rows. That risk is why cache PUT was stopped.

Do not treat the 04:09 HIT as tonight's source of truth, and do not
treat the mismatch as proven train/serve skew until a same-job local
vs container pair is compared.
