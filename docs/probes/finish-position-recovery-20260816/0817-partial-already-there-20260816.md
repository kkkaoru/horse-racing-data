# 0817 is not “starting now” (read 2026-08-16 15:00)

Neon already has **8 races / 76 rows** for `kaisai_tsukihi='0817'`.
Card is NAR only: 35=12, 44=10, 46=10 → **32**. So **8/32**.

These stamps are **last night**, not this afternoon:

| race  | gen (JST) | model                 |
| ----- | --------- | --------------------- |
| 35/05 | 02:30     | iter12-nar marketfree |
| 35/01 | 02:35     | same                  |
| 35/09 | 03:22     | same                  |
| 35/06 | 03:41     | same                  |
| 35/07 | 04:10     | same                  |
| 44/10 | 04:41     | same                  |
| 44/04 | 05:25     | same                  |
| 35/04 | 07:16     | same                  |

Same family as today’s 0816 host rows. **No 0817 row after 07:16
JST.** Nothing new is filling the other 24 right now. Do not
extrapolate a finish time. 46 has **0**.

Path is unproven (no 0817 retry row). Treat as leftover / early
container or host, not a live 15:00 job.

## Change the 08:00 gate

“0 then host” is wrong. 8 leftover rows can sit all night.

At 08:00 count distinct 0817 races:

- **32** with sensible runner counts → skip host; watch weights
- **<32** (including today’s 8) → host NAR for the **missing**
  venues/races. Do **not** wipe the 8. UPSERT. Prefer whole
  missing venues (46 is empty; 35 is 6/12; 44 is 2/10)
- Do not wait for “the rest to catch up”. They are not moving

First post still **35/01 11:45**. Deadline unchanged:
notice 08:00, start by 08:15 if under 32.
`tomorrow-host-fallback-deadline-20260817.md`.
