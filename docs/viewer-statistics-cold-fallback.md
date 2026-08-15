# Viewer statistics cold-fallback safety

## Failure pattern

A warm section cache can hide an unsafe request-time fallback path. On 2026-08-15,
`similar` returned in roughly 0.07 seconds while warm, but an overseas cache-version
change exposed the cold path and returned `503 section_unavailable` after 107 seconds.
The same path took 22.268 seconds for a JRA race and 12.368 seconds for a NAR race
when the section cache was deliberately bypassed.

The cause was not the overseas person-statistics snapshot. The snapshot returned the
expected 20 rows, including A. O'Brien's 2,015 starts and 508 wins. The section also
calculates bloodline statistics. When coverage was insufficient, rate-stat candidates
were evaluated in successive batches until one reached the coverage threshold. Warm
operation hid that unbounded fallback.

Do not treat “warming fixed it” as a root-cause resolution. Before changing a section
cache version:

1. Bypass the section cache and measure the changed section against its production
   database.
2. Measure the changed race itself, not only unchanged regression races.
3. Use a version-specific preview URL before promotion only when the Worker supports
   preview URLs. `pc-keiba-viewer` exports Durable Objects (`PaddockRoom` and
   `RaceTrendRoom`), so Cloudflare does not generate preview URLs for it. For this
   Worker, exhaustively validate the integrated build against the production database
   locally, deploy only in an approved maintenance window, and keep immediate rollback
   ready.
4. Compare cold and warm responses, including semantic fields and response hashes.
5. Roll back immediately on a timeout, `503`, or unintended body difference.

## Bounded deterministic fallback

Rate-stat fallback candidates are evaluated concurrently. The request waits at most
six seconds for the complete candidate set. It uses the first qualifying result in
the original candidate order only when the complete set finishes within the budget.
If the deadline expires, all partial results are discarded. This prevents completion
order or transient load from selecting a different response that is then retained in
KV.

The response can expose `bloodlineStatsIncomplete` when coverage is unavailable. The
UI explains that the affected bloodline score remains blank instead of silently
omitting it.

Validation covered all 68 races on 2026-08-15 plus the A8 overseas race. Compared
with the existing warm responses, bloodline rows and selected settings had zero
mismatches. A fresh-process cold generation of Ban-ei 83/01 was repeated five times;
all bodies had length 647,223 and hash `b7f851d0c98371a3`.

## Why Ban-ei does not default to exact race-title matching

Ban-ei race titles frequently describe one-off sponsorships, visits, birthdays,
weddings, or memorials rather than a repeatable race category. The class condition
is the stable statistical boundary.

For the 12 Ban-ei races on 2026-08-15, ten title-token races had names such as a
first visit, birthday, wedding, or memorial race. Every one of those ten exact titles
appeared only once in the preceding ten-year population. Exact-title filtering
therefore removes the historical population instead of selecting a meaningful peer
group. The automatic Ban-ei scope consequently starts with `includeRaceTitle=false`;
an explicit user selection can still request title filtering.
