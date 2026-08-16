# Weight-rescore observations side by side (no cause)

Updated 13:01 JST. Add Ban-ei later. Do **not** infer why 01/02 or 07/01 landed late.

Slack = trigger → post for R1s. 01/02 slack is post → landing
(+24 min after 10:30). 35/01 trigger 11:44:58, post 12:35; still
baseline at +51 min (advisor 12:41).

| race         |   n | trigger (JST)                                      | post  | slack / elapsed                                | Neon gen at check        | landed before post−5? | retry_errors (D1)                                       | odds_score              | model_version                                 |
| ------------ | --: | -------------------------------------------------- | ----- | ---------------------------------------------- | ------------------------ | --------------------- | ------------------------------------------------------- | ----------------------- | --------------------------------------------- |
| JRA 04/01    |  13 | 09:10                                              | 09:40 | 30 min                                         | **07:07:10Z** (baseline) | no                    | none reported                                           | morning flat 0.5664     | `jra-cb-stage1-marketfree235-2013`            |
| JRA 07/01    |  10 | 09:10                                              | 09:50 | landed **12:57:05** = post+3h07 / trigger+3h47 | **03:57:05Z**            | **no** (too late)     | GraphQL **12:55 Container 503**; then land ~2 min later | **per-horse 0.20–1.00** | still `jra-cb-stage1-marketfree235-2013`      |
| JRA 01/01    |  14 | 09:10                                              | 10:00 | 50 min (advisor 39)                            | **05:03:55Z**            | no                    | none reported                                           | morning flat 0.5664     | same family                                   |
| NAR 35/01    |   9 | 11:44:58                                           | 12:35 | **51 min**, past post                          | **05:31:18Z**            | **no**                | none reported                                           | morning flat 0.5048     | `iter12-nar-xgb-hpo-v8-stage1-marketfree-184` |
| JRA 01/02    |   8 | (same 09:10 wave; not an R1)                       | 10:30 | landed **10:54:34** = post+24                  | **01:54:34Z**            | **no** (too late)     | **10:19 Network connection lost; 10:20 Container 503**  | **per-horse 0.11–0.65** | still `jra-cb-stage1-marketfree235-2013`      |
| JRA 07/02    |  12 | **unknown** (no D1 lifecycle; no retry_errors row) | 10:20 | landed **13:39:34** = post+3h19                | **04:39:34Z**            | **no** (too late)     | none                                                    | **per-horse 0.12–0.97** | still `jra-cb-stage1-marketfree235-2013`      |
| Ban-ei 83/01 |  10 | 13:42:52                                           | 14:25 | **>18 min** at 14:01 (42 min slack)            | **05:50:12Z**            | pending               | —                                                       | morning (see TSV)       | morning                                       |

Same on all five (so far): seed Last-Modified **unchanged** (no PUT);
feat-cache still 07:48–07:52 objects. HIT/MISS not stored.

Still morning at 13:41: 04/01, 01/01, 35/01. Late UPSERT: 01/02,
07/01, **07/02**. All three odds **spread**; all still marketfree.
Betting in-time count stays **0**.

**Observation, not a cause:** on both landings, odds entered and
`model_version` stayed `jra-cb-stage1-marketfree235-2013`.

**Also observation, not a cause (n=2):** 01/02 and 07/01 each sat
**after** a Container 503. 07/02 has **no** retry_errors row.

**Also observation, not a cause (n=2 same venue):** 07/01 12:57 then
07/02 13:39 = **42 min** gap. If 07/03 lands, append the next gap.

Do not turn “smallest field” or “has retry_errors” into a cause.
Tomorrow: append NAR 44/01, 55/01, Ban-ei 83/01 the same way.
