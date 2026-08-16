# Handoff: weight rescore (work sheet)

User one-pager is `user-report-20260816.md`. This page is for the
next person who has to **measure**. Live image `0c76062e`. No deploy
09:40–20:50 unless the user opens a window.

**Read first:** `rejected-hypotheses-20260816.md`. The first thing a
new person does is invent a cause. Those 11 are already dead (Last-Modified
≠ unread, 15-min max ≠ regen, cron minutes are UTC, 20 min ≠ fail).
Pass/fail and the elapsed distribution are for **after** a deploy, not
before the first hypothesis.

## (1) One line

Weight trigger fires and the queue consumer runs; R1 predictions
almost never UPSERT before post. One late race did.

## (2) Known / unknown

**Known**

- Trigger exists (`RESCORE_ENABLED=1`). 09:10 JRA; 11:44 NAR 35/01.
- Four R1 misses: 04/01, 07/01, 01/01, 35/01 (slack 30–51 min, JRA+NAR).
- One landing: 01/02 at **10:54** = post+24. `odds_score` per-horse;
  `model_version` still marketfree. retry_errors 10:19 / 10:20.
- `mode=rescore` **holds** NDJSON; focused-full **detaches**.
- Last-Modified still 07:49 seed = **no PUT**, not unread.
- HIT vs MISS is **not** in Neon / D1 / R2 metadata.
- Seed 80/80 (JRA+NAR+Ban-ei) still those objects at 11:51 (44/44 NAR/Ban-ei).
- Nine git fixes do **not** include a root-cause stall fix
  (`5480ed4d`). Only `85bfba82` / `67440b8b` even sit on the path.
- **0817 also stopped at 07:16** (same second as 0816’s last write).
  Advisor 15:11: **8/32** already on Neon (overnight 02:30–07:16); venue
  **46 is 0/10**. A stopped job has **no pace** — do not extrapolate
  “full by 06:00”. Fill the missing venues; do not DELETE the 8.

**Unknown**

- Why consume usually does not UPSERT before post.
- HIT or MISS on the seed.
- Why 01/02 only, and why it had retry_errors.
- Whether 9/10 instances at 09:37 caused the 10:20 503 (no count at 09:10).

## (3) Measure next

Pass/fail after a deploy: `post-deploy-weight-rescore-pass-fail-20260816.md`
(`f8456705`). Short version:

- Baseline **before** that day’s trigger.
- 3 R1s; post−5 = betting fail; +80 = “did it finish”.
- `containers list` at **trigger** and at **post−5**.
- On a landing: odds spread + `model_version` (not just UPSERT).
- Ban-ei 83/01 tonight: same two facts + append the asymmetry table.
- **Why 0006:** 07/02 **landed** (13:39, odds spread) with **no** D1
  lifecycle row and **no** `retry_errors` row. Trigger time is gone.
  “No record ≠ did not run.” Next race day, apply 0006 or we will
  lose the next 07/02.

## (4) Do not

- Re-raise the 11 dropped claims: `rejected-hypotheses-20260816.md`.
- Treat Last-Modified as unread. Treat 20 min empty as fail.
- Count a post+24 UPSERT as fixed. Ship nine commits as the stall fix.
- DELETE R2 / Neon. Flip `DAY_BASE_SPLIT` / `COORDINATOR_ENABLED`.
- Re-POST focused-full 04/01. Seed from `v7-final` without row counts.
- Invent a 12th JRA cause tonight.

## Map

Entrance is **three files**: `user-report-20260816.md`, this handoff,
`overnight-fp-index.md`. Other clocks (`jra-weight-09*`, `nar-3501-*`)
are a time series — same stall, later timestamps. Do not open them first.
