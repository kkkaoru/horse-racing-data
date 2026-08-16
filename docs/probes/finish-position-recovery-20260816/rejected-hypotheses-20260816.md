# Hypotheses we raised tonight and then dropped

Not a scorecard. Same _shape_ of error should not recur tomorrow.
Cause of the stall is still **unknown**.

| #   | who              | claim                                                            | why it looked true                               | what killed it                                                                     | how to avoid next time                                    |
| --- | ---------------- | ---------------------------------------------------------------- | ------------------------------------------------ | ---------------------------------------------------------------------------------- | --------------------------------------------------------- |
| 1   | advisor          | Last-Modified still 07:49 ⇒ cache **unread** / job never started | Only visible R2 clock; 09:31 asked this one HEAD | GET does not change Last-Modified. Spec of HEAD                                    | Read the object verb before inferring “read”              |
| 2   | advisor          | 08:45 / 08:49 Neon gens ⇒ seed already used                      | `where generated_at > now()-15min` then `max`    | primary: 04/08 and 07/02 still 05:04, distinct=1                                   | Always print **count + min + max + distinct**             |
| 3   | advisor          | GraphQL `internalError` 09:00–09:55 = finish-position-cron tick  | Errors sit next to cron minutes                  | Cron expression is UTC; those minutes have **no** fp-cron                          | Convert cron to JST **and** list ticks in the window      |
| 4   | advisor          | Consumer waits 9.9 min ⇒ Worker wall clock ⇒ `internalError`     | CacheMiss fallback is held `mode=rescore`        | Paths split: rescore **waits**, focused-full **accepted**. Errors ≠ proven timeout | Name `mode` before blaming the wait                       |
| 5   | advisor          | 20 min no UPSERT = fail / “does not complete”                    | First alarm; focused-full once +20 min           | 01/02 landed **+24 min after post** (10:54). Completes, too late to bet            | Fail clock = **post−5 min**, not “no row at +20”          |
| 6   | advisor ~07:00   | Split ~25% cheaper ⇒ too small to enable                         | RACE_CHAIN 447 s vs 9.9 min p50                  | Wrong axis. JRA `--target-race` **kills pedigree**. 447 s is **whole-day** RACE    | Do not treat a layer timer as 1-race; quality ≠ speed     |
| 7   | optimize         | `--target-race` on host = 1-race RACE cost                       | Flag name                                        | Output still 490×36; PG history shrinks, parquet does not                          | Count rows after the run                                  |
| 8   | optimize         | Seed vs old HIT **values** must match before PUT                 | 04/12 rank max abs 13                            | Old HIT was the **dead** vector; host full-day was healthy                         | Compare to a known-good writer, not to prod contamination |
| 9   | optimize (early) | Last-Modified seed + no Neon ⇒ unread (same as #1)               | Repeated advisor’s HEAD ask                      | Retracted `ee5fcfa6`                                                               | Do not promote a teammate’s shortcut without the spec     |
| 10  | fix-dev          | Cron / adjacent success explains the error window                | Time proximity                                   | Advisor withdrew after UTC cron check (`713dcc38`)                                 | Same as #3                                                |
| 11  | team (implicit)  | Queue is FIFO / near-post first                                  | Intuition                                        | `max_concurrency=9`, shards, `retry(delaySeconds)` overtakes                       | Read consumer + wrangler before “order”                   |

**Still standing (not in this table):** trigger fired; consumer GraphQL
ran; most R1s no UPSERT before post; 01/02 late landing with odds
spread; HIT/MISS unseen; 9/10 instances at 09:37 **after** the fact.

Do not add a 12th JRA cause tonight. NAR 12:04 is the next fact.
