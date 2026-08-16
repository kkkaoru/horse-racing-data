# Tomorrow host fallback deadline (2026-08-17)

08-17 **has no JRA card** (`jvd_ra` 0). First post is NAR **35/01
11:45** (`hasso_jikoku=1145`). Then 46/01 15:10, 44/01 15:40.
Do not plan around 09:40.

Tonight’s host wall (measured, one category at a time):

| block             | clock       | elapsed                                                |
| ----------------- | ----------- | ------------------------------------------------------ |
| notice stall      | 02:35       | —                                                      |
| start work        | 03:44       | **+69 min** to notice                                  |
| JRA score on Neon | 05:04       | **80 min** after start (layers already partly on disk) |
| NAR flush         | 05:31       | +27 min                                                |
| Ban-ei flush      | 05:50       | +19 min                                                |
| start → 80/80     | 03:44–05:50 | **126 min**                                            |
| playbook header   | 03:00–05:40 | same job                                               |

Cold JRA 17-layer from empty disk is **longer** than tonight’s
resume. Playbook: do not restart mid-category; FORCE 8/4; DuckDB
1.5.5; no parallel categories. Container per-race is **not** the
backup (~8 races / 90 min).

08-17 only needs NAR (32 races if 35+44+46; 46 is a new venue).
NAR tonight was **~27 min** after JRA was already done. A cold
NAR from empty disk: budget **90 min**, not 27.

## Notice / start / latest finish

Need rows **before 11:45**. Margin 15 min → finish by **11:30**.

| if you notice by | start host by | NAR 90 min done | in time for 11:45? |
| ---------------- | ------------- | --------------- | ------------------ |
| **08:00**        | **08:15**     | 09:45           | yes                |
| 09:00            | 09:10         | 10:40           | yes                |
| 10:00            | 10:10         | 11:40           | **tight**          |
| 10:30            | 10:40         | 12:10           | **no** for 35/01   |

**Alarm 08:00.** Count Neon `kaisai_tsukihi='0817'` (not D1
`completed`). Expected after a good generate: 35=12, 44=10, 46=10.
Zero at 08:00 → start the NAR host one-shot **by 08:15**.
`local-oneshot-recovery-playbook.md`.

Do **not** wait for the 09:10 weight clock. That tests rescore,
not first-row existence.

If 08:00 already has 32/32, skip host. Watch weights only
(`observe-commands-20260816.md`, DATE=0817).

If you wake after **10:00** with Neon still 0: start anyway for
the 15:10/15:40 cards; **say 35/01 will miss**. Do not start a
second category in parallel to “catch up”.

Notice lag tonight was 69 min. The 08:00 alarm is what removes
that hour. The 90 min is the generate. Both are required.

No production change tonight.
