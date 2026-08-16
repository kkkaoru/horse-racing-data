# Weight-rescore vs post (2026-08-16)

One sheet for tomorrow. Fill NAR / Ban-ei when those windows close.
Times are JST unless marked Z. Cache Last-Modified is seed window
07:48–07:52 unless a later write appears.

Shared JRA trigger: weights **09:10:29**, `triggerRescoreAfterWeights`
**09:10**, queue depth 2. Cache unread = Last-Modified still seed.

| race         | post  | trigger→post | cache Last-Modified          | Neon gen (distinct=1) | in time? |
| ------------ | ----- | -----------: | ---------------------------- | --------------------- | -------- |
| JRA 04/01    | 09:40 |       30 min | **07:49:20** (81153B) unread | **07:07:10Z**         | **no**   |
| JRA 07/01    | 09:50 |       40 min | **07:49:35** (73697B) unread | **05:04:08Z**         | **no**   |
| JRA 01/01    | 10:00 |       50 min | **07:49:05** (84451B) unread | **05:03:55Z**         | **no**   |
| NAR 35/01    | 12:04 |            — | (seed; fill at window)       | 05:31:18Z baseline    |          |
| NAR 44/01    | 12:04 |            — |                              | 05:31:24Z baseline    |          |
| NAR 55/01    | 12:04 |            — |                              | 05:31:30Z baseline    |          |
| Ban-ei 83/01 | 13:54 |            — |                              | 05:50:12Z baseline    |          |

Contrast races (same two facts if a gen moves): 35/06, 44/06, 55/06,
83/06. TSV: `nar-banei-weight-baselines-20260816.md`.

Three JRA R1s: slack 30 / 40 / 50 min, all unread + Neon unchanged.
Not a short-window accident. Seed HIT / rank / odds-vs-weight **not**
tested on JRA today.
