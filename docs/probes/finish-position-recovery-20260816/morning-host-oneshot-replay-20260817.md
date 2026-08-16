# If tomorrow’s container writes nothing: replay tonight’s host one-shot

Not the clock for “when to notice” (fix-dev). This is **what changes**
if we run the same Mac recovery again, half-awake.

Tonight wall **~03:00–05:50 JST ≈ 170 min** to Neon 80/80. That included
DuckDB version hunt, a 2.7GiB OOM, and **not** restarting mid-JRA
(~40 min of base–layer-6 kept). Compute alone, once FORCE 8/4 and
1.5.5 are set: JRA layers 7–16 **~19 min** after that resume; NAR+Ban-ei
filled 05:04→05:50 UTC wait… latest stamps 05:04 / 05:31 / 05:50 JST
= **~46 min** for NAR+Ban-ei after JRA flush. So a **clean** three-category
run is on the order of **~70–90 min**, not 170. Saving is the hunt and
the OOM, not a faster DuckDB.

Playbook already has: `local-oneshot-recovery-playbook.md`.
Seed after (optional): `feat-cache-seed-runbook-20260816.md` (`73e53b7c`).

## Already written (do not rediscover)

| item                                                         | where                                                          |
| ------------------------------------------------------------ | -------------------------------------------------------------- |
| DuckDB **1.5.5** (`uv run --with 'duckdb==1.5.5'`)           | playbook §versions + launch template                           |
| `PIPELINE_FORCE_MEMORY_GB=8` `PIPELINE_FORCE_THREADS=4`      | launch template. Auto/`MAX` cannot raise                       |
| Category order **jra → nar → ban-ei**, never parallel        | §What worked                                                   |
| Layer counts 17 / 10 / 7                                     | §Layer counts (`LAYER_CHAIN`)                                  |
| Do **not** restart `predict_upcoming.py` mid-category        | dedicated section. Resume layer N+1 + `_score_and_flush_races` |
| `colima stop`; Apple local PG **stays**                      | §Memory. Concurrent colima → 2.7GiB OOM                        |
| Neon `BEGIN; SET TRANSACTION READ WRITE` then **count rows** | §Neon. Ignore D1 `completed`                                   |
| `feat-*-v7-final` name lies (JRA 1×8)                        | §Memory / seed runbook                                         |
| `PIPELINE_DIR` = viewer scripts                              | launch template (`2139645b`)                                   |

## Still missing for a groggy morning

1. **One copy-paste block with tomorrow’s `RUN_DATE`** and the exact
   `cd` + `uv` cwd (`apps/finish-position-predict-container`). Playbook
   template uses `$REPO` / `YYYYMMDD` — easy to run from the wrong dir.
2. **Preflight 2 minutes:** `duckdb --version` or `uv run --with` print;
   `colima status` (must be stopped); `memory_pressure` free %; confirm
   Apple PG still up. Not a numbered checklist at the top of the playbook.
3. **JRA first-post slack.** Tonight first post 09:40. A 70–90 min
   clean run started at 07:30 finishes ~08:40–09:00 — **before** 09:40
   if notice is early. Started at 08:30 it **misses** 04/01. Notice
   clock is fix-dev’s sheet; this only says compute needs **≥90 min**
   before first post if we want R1 rows.
4. **FORCE is host-only.** Container `envVars` still does not forward
   it (`built-not-used` #7). Morning one-shot ≠ “the image will use 8/4”.
5. **Do not seed until Neon counts match** and last-layer rows/cols
   are counted. Seed runbook is a **second** job after 80/80.
6. **Ban-ei 83/09–10 v8** is `grade_code=E`, not a write fail. Do not
   rerun Ban-ei for that.

## What will not shrink

- Iceberg base + 17 JRA layers still take most of the 70–90 min.
- Host must stay **single-category**. No “three terminals”.
- If we OOM again and **restart** the orchestrator, we pay tonight’s
  40 min again. The playbook already forbids that — follow it.

No deploy. No host generate from this page tonight.
