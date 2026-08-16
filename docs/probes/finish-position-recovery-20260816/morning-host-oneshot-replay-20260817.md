# If 08-17 container writes nothing: NAR-only host one-shot

**08-17 is Monday: JRA 0, NAR 32** (advisor local PG). No 09:40 JST
clock. Weight-rescore check is the **NAR** window (~12:04), not 09:10.
First post ~**12:35** (confirm on the day). Ban-ei not on this card
unless someone counts 83 tomorrow.

Not the “when to notice” clock (fix-dev). This is compute if we replay
the Mac path half-awake.

## Time (NAR only)

Tonight 170 min was JRA hunt + OOM + 17 layers + NAR + Ban-ei.
NAR flush tonight: **05:04 → 05:31 JST ≈ 27 min** after JRA was already
on disk (10 layers, 32 races). Head-to-head NAR peaked ~3.2GB (JRA 6.95).

**Clean NAR-only estimate: ~30–45 min** (Iceberg base + 10 layers +
score/flush + one mistake). Not 70–90 (that was JRA-inclusive).
Start by **11:05** if first post is 12:35 and we want R1 rows with slack
(`12:35 − 90 min` is conservative; 45 min compute + 15 min preflight
⇒ **11:35** is the tight line). Prefer 11:05.

Playbook: `local-oneshot-recovery-playbook.md`.
Seed after (optional): `feat-cache-seed-runbook-20260816.md`.

## Already written

| item                                                      | where                                      |
| --------------------------------------------------------- | ------------------------------------------ |
| DuckDB **1.5.5**                                          | playbook + `uv run --with 'duckdb==1.5.5'` |
| `PIPELINE_FORCE_MEMORY_GB=8` `THREADS=4`                  | launch template. Auto/`MAX` cannot raise   |
| **One category.** Tomorrow: `PREDICT_CATEGORIES=nar` only | do not start jra                           |
| NAR **10** layers                                         | `LAYER_CHAIN`                              |
| Do not restart mid-category                               | resume layer N+1                           |
| `colima stop`; Apple PG stays                             | concurrent colima → 2.7GiB OOM             |
| Neon READ WRITE then **count**                            | expect **32** NAR races, not 80            |
| `feat-nar-v7-final` can be the real body                  | still **count** rows/cols                  |
| `PIPELINE_DIR` = viewer scripts                           | `2139645b`                                 |

## Still missing (groggy morning) — still valid

1. Copy-paste with `RUN_DATE=20260817` and cwd
   `apps/finish-position-predict-container`.
2. **2 min preflight:** DuckDB 1.5.5 print; `colima status` stopped;
   `memory_pressure`; Apple PG up.
3. Start early enough for **12:35**, not 09:40 (**11:05** / tight 11:35).
4. FORCE is **host-only** (container `envVars` does not forward it).
5. Seed only after Neon **32** and last-layer counts. Not “80/80”.
6. Do not run Ban-ei “to be safe” on a NAR-only day.

## Launch (fill cwd)

```sh
cd "$REPO/apps/finish-position-predict-container"
export PIPELINE_DIR="$REPO/apps/pc-keiba-viewer/src/scripts"
export SOURCE_DATABASE_URL='r2-catalog://pc-keiba'
export RUN_DATE=20260817
export PREDICT_DAYS_AHEAD=0
export PREDICT_CATEGORIES=nar
export PIPELINE_FORCE_MEMORY_GB=8
export PIPELINE_FORCE_THREADS=4
unset PREDICT_SERVE_MODE
uv run --with 'duckdb==1.5.5' python src/predict_upcoming.py
```

No deploy. No host generate from this page tonight.
