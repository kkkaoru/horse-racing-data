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
If the deadline expires, all partial results are discarded and the section computation
fails. The route may serve a valid stale body; otherwise it returns `503` without
caching a load-dependent partial body. An exhausted search is different from a timed-out
search: only an exhausted search may return an incomplete-data disclosure and legitimate
zero rows. This prevents completion order or transient load from selecting a different
response that is then retained in KV.

The distinction matters on a truly cold database path. An initial JRA 01/10 generation
took 9.048 seconds and hit the six-second fallback deadline; three immediately repeated
runs took 2.706, 2.424, and 2.256 seconds and completed. Earlier 68-race validation had
run against a warm database and therefore reported zero bloodline mismatches. Warm-only
validation does not prove that a deadline-dependent cold response is safe.

The response can expose `bloodlineStatsIncomplete` when a completed candidate search
finds insufficient coverage. The UI explains that the affected bloodline score remains
blank instead of silently omitting it. A fresh-process cold generation of Ban-ei 83/01
was repeated five times; all bodies had length 647,223 and hash
`b7f851d0c98371a3`.

## Explicit all-conditions-off remains a known heavy path

The `similar` controls allow a user to turn off every condition and select a ten-year
or all-time window. Unlike automatic fallback, this explicit state can also disable
venue filtering. For NAR this creates an all-venue scan. The legacy query itself can
exceed the 15-second statement timeout in that state; repeated local probes of 55/10
and 44/10 timed out. This is a user-reachable existing issue, but it is separate from
the normal automatic fallback, which preserves venue filtering for NAR. Do not use the
explicit all-conditions-off timing as a proxy for default request performance.

Possible later fixes include retaining the venue when clearing conditions or limiting
the selectable period. This path was intentionally not changed in the 2026-08-15
fallback deployment work.

## Cross-database ordering comparisons

Local PostgreSQL and Neon can use different collations. A person-row query ordered by
rate, starts, and name was stable across five baseline and five candidate executions
per race on the same local database, while the local and Neon captures ordered tied
names differently. Compare order-dependent output in the same database environment;
do not use local row order as a byte-for-byte proxy for Neon row order. Compare keys,
metrics, and detail multisets separately when validating across those environments.

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
