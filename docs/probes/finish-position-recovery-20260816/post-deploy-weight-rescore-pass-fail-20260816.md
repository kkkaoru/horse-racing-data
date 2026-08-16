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

## How many races

Today 3 JRA R1s (slack 30 / 40 / 50 min) ruled out a short window.
That bar stays.

**Minimum to say fixed:** **3 R1s**, at least **2 categories** if
NAR/Ban-ei also missed tonight (shared structure). If tonight NAR
lands and JRA does not, next day needs **3 JRA R1s** (JRA-specific).

Contrasts (R6) are optional; they do not count toward the 3 unless an
R1 is scratched.

## Fail clock (revise today’s 20 min)

Today: 20 min was the first alarm; **77 / 77 / 66 min** still no
UPSERT. Focused-full once landed at **+76 min**, so 20 min is only
“not yet”, not fail.

| clock after trigger                  | if Neon gen still baseline                                                    |
| ------------------------------------ | ----------------------------------------------------------------------------- |
| **20 min**                           | not yet (same as today)                                                       |
| **post − 5 min**                     | **operational fail** for that race (too late to bet)                          |
| **+80 min** or post, whichever first | **not fixed** for that race. 80 > today’s 77 and the +76 focused-full landing |

**Not fixed (deploy):** any of the 3 required R1s hits the +80 / post
line. One success and two misses is not fixed.

**Fixed (deploy):** all 3 required R1s UPSERT **before post − 5 min**,
and (4) is recorded (even if rank delta is 0 — then write “landed,
ranks unchanged”).

**Unknown / do not claim HIT:** (2) moved but (5) has no fallback
line and no HIT log. Say UPSERT-only.

Do not wait past the fail clock hoping for a 76-minute landing.
That is not a working weight path.
