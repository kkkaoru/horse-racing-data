# Deploy windows after today’s card (2026-08-16)

Read-only. Last domestic posts: Ban-ei **20:45**, NAR 44/55 **20:50**.
A8 `A8/04` post **22:50**. A8 **main generation is not this
container** (`a8-main-generation-20260816.md`: isolated oversea
Python, no Neon / R2 / container). Racing-hours ban is still
**09:40–20:50**. Default remains **tomorrow morning**.

12:21 Neon: `35/01` still **05:31**. Trigger +36 min. Post 12:35
not yet.

## Three windows

| window                       | clock                       | ship?                                                | verify today’s stall?                                                                         |
| ---------------------------- | --------------------------- | ---------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| **(1)** last post → A8 start | **20:50–21:00** (10 min)    | No. Image deploy + list + one probe does not fit     | No                                                                                            |
| **(2)** after A8 generate    | **22:00–** (A8 posts 22:50) | **Yes for this Worker.** A8 generate does not use it | **No.** Next weight clock is tomorrow ~09:10. Shipping here only proves the new version boots |
| **(3)** tomorrow morning     | before first JRA weight     | **Yes.** Safest                                      | **Yes.** First `fetch-weights` ok is the stall test                                           |

Correction to “22:00 is limited because A8 posts 22:50”: that limit
is for **A8 serving / oversea files**, not for
`finish-position-cron` image. Do not deploy **viewer** in (2) if A8
HTML is still being read. Container rollback target stays
`0c76062e`.

## What each window can take

**(1)** nothing except maybe a D1 `0006` apply if someone insists.
Still a bad idea: canary then injects every 5 min into the same
queue we cannot label. Skip.

**(2)** may ship `85bfba82` (+ `67440b8b` if we accept an untested
MISS). `41676f7c` only if we want empty layer-timing until tomorrow.
Do **not** judge stall-fixed from a 22:00 boot. `0006` apply here
starts canaries overnight; still no weight `tracking_id`. Split stays
off.

**(3)** same commits, then watch the first weight trigger → Neon
`generated_at`. That is the only clock that can fail the stall
closed. Per-message outcome is still **not** in tonight’s nine;
without it a miss tomorrow is the same unknown.

User picks (2) ship-only vs (3) ship+test. Default (3).
