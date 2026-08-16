# Weight-rescore observations side by side (no cause)

Filled 12:11 JST. Add rows later. Do **not** infer why 01/02 landed.

Slack = trigger → post for R1s. 01/02 slack is post → landing
(+24 min after 10:30). 35/01 trigger 11:44:58, post 12:35; at 12:11
elapsed 26 min, still baseline.

| race      |   n | trigger (JST)                | post  | slack / elapsed                    | Neon gen at check        | landed before post−5? | retry_errors (D1)                                      | odds_score              | model_version                                 |
| --------- | --: | ---------------------------- | ----- | ---------------------------------- | ------------------------ | --------------------- | ------------------------------------------------------ | ----------------------- | --------------------------------------------- |
| JRA 04/01 |  13 | 09:10                        | 09:40 | 30 min                             | **07:07:10Z** (baseline) | no                    | none reported                                          | morning flat 0.5664     | `jra-cb-stage1-marketfree235-2013`            |
| JRA 07/01 |  10 | 09:10                        | 09:50 | 40 min                             | **05:04:08Z**            | no                    | none reported                                          | morning flat 0.5664     | same family                                   |
| JRA 01/01 |  14 | 09:10                        | 10:00 | 50 min (advisor 39)                | **05:03:55Z**            | no                    | none reported                                          | morning flat 0.5664     | same family                                   |
| NAR 35/01 |   9 | 11:44:58                     | 12:35 | **26 min elapsed**, 24 min to post | **05:31:18Z** (still)    | pending               | —                                                      | morning flat 0.5048     | `iter12-nar-xgb-hpo-v8-stage1-marketfree-184` |
| JRA 01/02 |   8 | (same 09:10 wave; not an R1) | 10:30 | landed **10:54:34** = post+24      | **01:54:34Z**            | **no** (too late)     | **10:19 Network connection lost; 10:20 Container 503** | **per-horse 0.11–0.65** | still `jra-cb-stage1-marketfree235-2013`      |

Same on all five (so far): seed Last-Modified **unchanged** (no PUT);
feat-cache still 07:48–07:52 objects. HIT/MISS not stored.

Same on the three JRA R1s + 35/01 so far: no UPSERT, no retry_errors
row, odds still the morning **constant**.

Different on 01/02 only: UPSERT exists; retry_errors exist; odds
**spread**; smallest field (8). Still market-free model. Still fail
on the post−5 min clock.

Do not turn “smallest field” or “has retry_errors” into a cause.
Tomorrow: append NAR 44/01, 55/01, Ban-ei 83/01 the same way.
