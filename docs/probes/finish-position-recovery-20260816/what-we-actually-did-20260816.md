# What we actually did (2026-08-16)

Not “five root causes”. Not “the stall is fixed in git”.

## Did

- Counted Neon. Host one-shot wrote **80/80** by 05:50. That is
  not the live container.
- Watched the weight path. Triggers fired. Some Neon stamps moved
  **after** post (01/02 +65 min, 07/01 +227 min, 07/02 +~3 h with
  **no** D1 row). Betting clock so far: **0 before post**.
- Ruled things **out**: paused queue; cron in the error minutes;
  Last-Modified = unread; NAR-only branch; cap full at the NAR
  clock; settings that add to 227 min; A8 using this path.
- Wrote how to look tomorrow (`observe-commands`, morning
  runbook, 08:00 host deadline). 0006 is tables. It does not
  label a weight message.

## Did not

- Find the reason consume usually does not UPSERT before post.
- Land a weight rescore before any R1 we watched (14:11: 83/01
  still 05:50, post 14:25 not yet).
- Ship a fix. Nine commits do not contain one.
- Make tomorrow’s 80 automatic.

14:10:48: `83/01` 05:50, `35/01` 05:31, `04/01` 07:07,
`01/01` 05:03. Coverage 80/940. Max `07/02` 13:39.
