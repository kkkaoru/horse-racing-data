# Pass/fail after a later deploy (weight rescore)

Not a commit ranking. After an image is live, use this to say
**fixed** or **not fixed**. Tonight’s cause is still unknown.

**This sheet does not certify the tonight pile.** Fix-dev’s split:
only `85bfba82` (writable txn, **if** consume reached the container)
and `67440b8b` (DuckDB 1.5.5, **MISS only**) even sit on today’s path.
The other seven do not move this stamp. Shipping all nine and then
reading **not fixed** here means we never deployed a root-cause fix —
not that those nine “failed to cure” the stall. Do not treat a fail
verdict as evidence against `6793ad7f` / PREWARM / split / odds-board.

## When to take the baseline

Take Neon ranks **before** that day’s weight trigger, on the **new**
image’s morning scores.

| clock                                          | action                                                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------ |
| after first morning UPSERT, **before** weights | dump R1 + one contrast per venue (same shape as `neon-*-ranks-before-weight-20260816.tsv`) |
| same moment                                    | HEAD feat-cache Last-Modified so a later PUT is visible                                    |
| do **not** wait until 09:09                    | today’s 09:10 snapshot was already post-seed; we could not compare pre-seed ranks          |

If host seed and container morning gen can differ, label which writer
produced the baseline.

## What to observe (next race day)

Today’s two facts are **necessary, not sufficient**.

| #   | fact                                             | enough for?                   |
| --- | ------------------------------------------------ | ----------------------------- |
| 1   | trigger fired (D1 / sync log)                    | enqueue happened              |
| 2   | Neon `min/max/distinct generated_at` vs baseline | **UPSERT yes/no**             |
| 3   | Last-Modified vs morning HEAD                    | PUT yes/no only. Not HIT/MISS |

**Add:**

| #   | fact                                                                                                                            | why today was blind                                                                                                                                                                       |
| --- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 4   | ranks / `odds_score` vs baseline **if** (2) moved. Also: is `odds_score` **per-horse** (min≠max) or still the morning constant? | landing ≠ useful rescore. Spread (01/02 0.11–0.65 vs R1 0.5664) ⇒ real odds entered. Flat ⇒ UPSERT without new market. If both weight-ish ranks and odds move in one gen, say inseparable |
| 5   | Worker GraphQL for **that** `mode=rescore` delivery: success vs `internalError`, and if present `rescore-fallback-to-full`      | HIT vs MISS is not in Neon/D1/R2. Only NDJSON/GraphQL can show the stage                                                                                                                  |
| 6   | D1 `finish_position_predict_retry_errors` for that race                                                                         | `catch` after start. Empty ≠ success                                                                                                                                                      |

Without (5), “fixed” can only mean **UPSERT before post**. It cannot
mean “HIT path works”.

## How many races (revised after 5 watches)

Today: 04/01, 07/01, 01/01 missed; 35/01 pending at +26–36 min; **01/02
only** UPSERT, and that was **post+24**. Three JRA R1s already rule out
a short window. They do **not** rule out “one late stray lands”.

**Minimum to say fixed:** still **3 R1s**, all UPSERT **before post−5**.
If 35/01 also misses, require those 3 across **≥2 categories** (shared
stall). One R2 landing like 01/02 **does not count** toward fixed.

**3 is enough to fail** (any required R1 misses the clock). **3 is not
enough to claim the path is stable** if only 1 of 5 watches landed
today — after a “fixed” morning, still log the next 2 R1s the same way
(no extra deploy gate).

Contrasts (R6) optional; they do not count unless an R1 is scratched.

## Fail clock (still post−5 / +80)

Today vs those two lines:

| watch                                                        | vs post−5                         | vs +80 from trigger                                  |
| ------------------------------------------------------------ | --------------------------------- | ---------------------------------------------------- |
| 04/01 30 min slack, no UPSERT                                | fail                              | would also fail at post                              |
| 07/01 40 min slack; landed **12:57** (+227 min from trigger) | fail at post                      | +80 would have failed; **still landed**              |
| 01/01 50 min                                                 | fail                              | same                                                 |
| 35/01 +36 min still baseline, post 12:35                     | will fail at 12:30 if still empty | +80 is after post — **post−5 binds first**           |
| 01/02 landed post+24                                         | **fail** (too late)               | trigger→land ~104 min; +80 would already have failed |

**Keep both clocks.** post−5 is the betting clock (today’s 01/02 would
still be fail if we only used +80). +80 is the “did it ever finish”
clock when post is far (today’s 77 min JRA wait; focused-full +76).
20 min stays **not yet**, not fail — 01/02 proves a later landing exists.

**Not fixed:** any required R1 hits post−5 or +80 empty. 1-of-3 is not
fixed. A post+24 UPSERT is not fixed.

**Fixed:** all required R1s UPSERT **before post−5**, and (4) recorded
(odds spread vs flat; model_version vs morning). Rank delta 0 is ok
if written.

**Unknown / do not claim HIT:** (2) moved but (5) has no stage line.

**Fail at +80 / post−5, but keep watching.** 07/01 landed at **+227 min**.
A fail verdict must **not** stop recording later UPSERTs (same shape as
judging 04/01 at +20 min). Queued ≠ dead. Betting fail and “never
finishes” are different facts.

Do not wait past the fail clock to _declare_ fixed. Do keep a later
row on the asymmetry table if a gen moves after fail.

## LIVE INSTANCES (new; today we only had after-the-fact)

Today: 9/10 at **11:20–11:30**, `LAST MODIFIED` **09:37** — after the
09:10 trigger, not at it. 10:20 Container 503 has **no** contemporaneous
count.

Take `wrangler containers list` **twice** (read-only):

| when                                                   | why                                            |
| ------------------------------------------------------ | ---------------------------------------------- |
| **weight arrival / trigger** (same minute if possible) | count at enqueue. Today’s missing clock        |
| **post−5** on the first required R1                    | count when we declare operational fail or pass |

A third list at landing (if any) is optional. Do not use a list from
hours later as “slots at 09:10”.
