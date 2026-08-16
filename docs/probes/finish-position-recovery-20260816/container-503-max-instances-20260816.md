# Container 503 at 10:20 is max instances, not boot wait (2026-08-16)

Read-only. Clock **2026-08-16 11:20 JST**. No deploy. No stop.

`01/02` retry_errors:

| recorded_at UTC | JST      | error                                                                                                                                                                                  |
| --------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 01:19:56        | 10:19:56 | `Network connection lost.`                                                                                                                                                             |
| 01:20:37        | 10:20:37 | `Container DO returned 503: There is no Container instance available at this time. This is likely because you have reached your max concurrent instance count (set in wrangler config` |

That second line is the platform text for **the configured cap**,
not “Python is still starting”.

## What 503 means here

`wrangler.jsonc` `containers[0].max_instances` is **10**.
Repo note (`07-official-spec-sizing.md`, Cloudflare wrangler
containers): start above the **running** cap errors. Stopped does
not count. Skill `containers/gotchas.md` splits two phrases:

- “Max instances reached” → all `max_instances` slots in use
- “No container instance available” → account capacity

The stored excerpt is the first: **app cap**, then truncated
`(set in wrangler config`.

`container-class.ts` `catch` around `containerFetch` returns
**502** `"Container start failed"`. The 503 string is therefore
**not** that catch. It is the runtime refusing a start because
10 were already live.

`sleepAfter` is **45m**. Docs
(`finish-position-prediction-system.md`): a sleeping instance
**still counts** in the live count. 45m idle is not “free slot”.

## What was visible at 10:19–10:21

GraphQL `finish-position-cron` 01:15–01:25 UTC:

| UTC   | JST   | success req/sub | internalError |
| ----- | ----- | --------------- | ------------- |
| 01:19 | 10:19 | 1 / 8           | 0             |
| 01:20 | 10:20 | 8 / 25          | **2**         |
| 01:21 | 10:21 | 1 / 1           | **3**         |

Aligns with the two `01/02` catch rows. Does not name the DO.

`wrangler containers list` **now** (11:20): app
`finish-position-cron-finishpositionpredictcontainer`
**LIVE INSTANCES 9**, last modified 00:37 UTC = 09:37 JST.

`wrangler containers instances` (same clock):

| name                                             | state    | created                                 |
| ------------------------------------------------ | -------- | --------------------------------------- |
| `predict-jra-0`                                  | running  | **2026-08-16 00:26:06Z = 09:26:06 JST** |
| `predict-jra-1`                                  | running  | 08-14                                   |
| `predict-jra-2`                                  | running  | 08-14                                   |
| `predict-nar-0/1/2`                              | running  | 08-11                                   |
| `predict-ban-ei-0`                               | running  | 08-15                                   |
| `predict-ban-ei-1`                               | running  | 08-11                                   |
| `predict-ban-ei-2`                               | running  | 08-14                                   |
| `predict-jra` / `predict-nar` / `predict-ban-ei` | inactive | 00:30 UTC today                         |

Nine running = 3 categories × 3 shards. That is the designed
full shard set. One slot left under 10. Historical count at
10:20 is **not** stored. A 503 then means the cap was full
**then**; we cannot replay which tenth name was refused.

`predict-jra-0` created at **09:26** matches the morning
GraphQL burst. We cannot see CPU / held-fetch / idle inside it.

## What this does **not** say

Advisor sketch: 503 = boot wait → retry → success, so the three
R1s never started.

**Not established.**

- 503 text is the **cap**, not boot.
- `01/02` later wrote Neon at 10:54. That only shows a later
  attempt got a slot or reused a live DO. Not that 10:20 was a
  start handshake.
- `04/01` `07/01` `01/01` still have **no** retry row. Missing
  catch ≠ never invoked (said before). Missing catch also ≠
  “never started a container”.

NAR 11:20: `nvd_se` weights still **0** on 35/44/55/83.
Baselines unchanged. First three JRA gens unchanged.

No production change.
