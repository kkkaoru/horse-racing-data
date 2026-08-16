# JRA 04/01 weight path missed post (2026-08-16)

No deploy. No DELETE. No focused-full POST.

| clock (JST) | fact                                                                                                               |
| ----------- | ------------------------------------------------------------------------------------------------------------------ |
| 09:10:29    | weights arrived; `triggerRescoreAfterWeights` fired (advisor / D1)                                                 |
| 09:10       | queue depth 2                                                                                                      |
| 09:31       | R2 `04/01` Last-Modified still **07:49:20** (seed, 81153B). `07/01` 07:49:35, `01/01` 07:49:05                     |
| 09:39       | Neon primary still `04/01` **07:07:10Z**, `07/01` **05:04:08Z**, `01/01` **05:03:55Z**. No JRA gen after 09:00 JST |
| 09:40       | first post                                                                                                         |
| 09:41       | Neon still unchanged                                                                                               |

Cache unread after the trigger ⇒ dequeue-or-start, not a mid-job Neon
lag. Same shape as focused-full `04/01` accept+20min (queue, not
compute). Seed HIT / rank / odds-vs-weight were **not** tested on JRA
today.

Carry the same checks to NAR 12:04 / Ban-ei 13:54. Baselines:
`nar-banei-weight-baselines-20260816.md` (`3e41ca7f`).
