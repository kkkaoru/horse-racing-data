# Weight-rescore vs post (2026-08-16)

One sheet for tomorrow. Fill NAR / Ban-ei when those windows close.
Times are JST unless marked Z. Cache Last-Modified is seed window
07:48–07:52 unless a later write appears.

Shared JRA trigger: weights **09:10:29**, `triggerRescoreAfterWeights`
**09:10**, queue depth 2. Last-Modified still seed means **no PUT after
07:49**, not “unread”. GET does not change Last-Modified. HIT vs MISS
is not stored (see below).

| race         | post  | trigger→post | cache Last-Modified                | Neon gen (distinct=1) | in time? |
| ------------ | ----- | -----------: | ---------------------------------- | --------------------- | -------- |
| JRA 04/01    | 09:40 |       30 min | **07:49:20** (81153B) no overwrite | **07:07:10Z**         | **no**   |
| JRA 07/01    | 09:50 |       40 min | **07:49:35** (73697B) no overwrite | **05:04:08Z**         | **no**   |
| JRA 01/01    | 10:00 |       50 min | **07:49:05** (84451B) no overwrite | **05:03:55Z**         | **no**   |
| NAR 35/01    | 12:04 |            — | (seed; fill at window)             | 05:31:18Z baseline    |          |
| NAR 44/01    | 12:04 |            — |                                    | 05:31:24Z baseline    |          |
| NAR 55/01    | 12:04 |            — |                                    | 05:31:30Z baseline    |          |
| Ban-ei 83/01 | 13:54 |            — |                                    | 05:50:12Z baseline    |          |

Contrast races (same two facts if a gen moves): 35/06, 44/06, 55/06,
83/06. TSV: `nar-banei-weight-baselines-20260816.md`.

Three JRA R1s: slack 30 / 40 / 50 min, Neon unchanged, objects not
overwritten. Not a short-window accident. Whether the consumer **read**
the seed is unknown.

**HIT vs MISS cannot be told from durable stores we can query.** No R2
GET access log on the object. Neon has no cache flag. D1
`finish_position_predict_retry_errors` only stores the exception after
a consumer `catch`, not `rescore-fallback-to-full`. Worker GraphQL
_might_ show that progress stage if a delivery started and
`parseNdjsonStream` logged it — we do not have those lines here.
Without that, HIT (seconds) vs MISS (9.9 min held fallback) is
indistinguishable. Seed HIT / rank / odds-vs-weight **not** tested.
