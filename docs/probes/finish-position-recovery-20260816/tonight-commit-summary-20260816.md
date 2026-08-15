# Tonight's commits vs production (2026-08-16 07:51 JST)

Production is unchanged. Commits below are **local `main` only**.
Nothing in this list has been deployed, secret-put, or queue-edited.
If a later reader thinks "we already fixed that", they are looking at
git, not at Cloudflare.

Live finish-position-cron: **`0c76062e-03c6-4b8b-9a25-a501a4f4c9cc`**
(100% since 2026-08-14T15:00:58Z). Rollback: `953d086b`.
Viewer remains `06fd3c24`. Do not deploy 09:40–20:50 JST.

Index: `overnight-fp-index.md`.

## What is in production tonight

- Neon **80/80** (940 rows) from the **host** one-shot, not from the
  container image. JRA 36 / NAR 32 / Ban-ei 12. Generated
  `target_race`-free, so JRA pedigree on those rows is healthy.
- All 0816 scores are **market-free** (`odds-missing`).
- Ban-ei 83/09 and 83/10 use v8 because `grade_code == E`.
- Queue delivery is ACTIVE. PREWARM cron still fires at 09:30 JST.
- `DAY_BASE_SPLIT_ENABLED` key exists; value unread; restore message
  is `keep split off`. Never recorded as enabled.

## Code fixes committed, **not deployed** (5)

These change runtime behavior. Production still runs the old code.

| #   | commit     | what it would change                                      | why not live           |
| --- | ---------- | --------------------------------------------------------- | ---------------------- |
| 1   | `85bfba82` | Neon DML: `BEGIN; SET TRANSACTION READ WRITE`             | image                  |
| 2   | `2139645b` | local `PIPELINE_DIR` override                             | image (host-only help) |
| 3   | `cd90cb73` | PREWARM payload miss/throw → `status=error`               | image                  |
| 4   | `3cd71358` | Worker: `success` without `parquetKey` → failed           | Worker                 |
| 5   | `41676f7c` | layer-timing: drop debug gate, writable txn, return False | image                  |

Related, also not in the running image:

| commit     | what                 | note                                                               |
| ---------- | -------------------- | ------------------------------------------------------------------ |
| `67440b8b` | pin DuckDB **1.5.5** | local image built, not shipped                                     |
| `9007a6e6` | `PIPELINE_FORCE_*`   | **viewer** scripts. Container DO `envVars` does not forward it yet |

`8228ce00` (`sync-failed` instead of `completed`) and `0abd880c`
(guard retry) are also undeployed. They are not finish-position image
fixes; they still are not in production.

**Five finish-position runtime fixes + the DuckDB pin are git-only.**
A 09:40 race will not see them.

## Docs only (no runtime change)

Playbook and clocks: `aaf533c6` `c7df1b8b` `a2c459f4` `dd800b31`
`d0357541`.

PREWARM / split lineage: `a0f56e20` `885453be` `e9514d80`.

Feat-cache / pedigree: `c51ab570` `ae3bd052` `43670d44` `8c8b38b3`.

Observability design (no impl): `bdf9fe58`.

Layer-time measurements (optimize): `243ec816` `3372cae9`.

## What we did **not** fix

- **JRA pedigree collapse on production per-race / feat-cache HITs.**
  Mechanism (optimize): `--target-race` shrinks `jra_um` so
  `MIN_RACES=5` dies. Host 80/80 avoided this because it was whole-day.
  No code change tonight. Do not seed cache from those HITs.
- **Market features** still missing (`odds-missing` → stage1 market-free).
- **`DAY_BASE_SPLIT_ENABLED`** not set. Needs team-lead. 07-17 said
  do not flip as-is; 07-18 watermark still requires that check. Speed
  is secondary; quality is the reason to consider it later.
- **Container logs** still invisible from `wrangler tail`. Design only
  (`bdf9fe58`). `_debug_finish_position_layer_timing` still empty in
  production because `41676f7c` is not deployed.
- **PREWARM R2 objects** still 404. Cron time still 09:30 JST.
- **04/01 focused-full** accepted 05:51, still MISS at +20 min.
- **Viewer** `detail-section-data.ts` dirty tree is **not ours**.
- **Queue priority / weight trigger** — proposals only.
- **A8 overlay** — oversea track; not this summary.

## Who must confirm before a later change

| change                                         | who                  | window                                       |
| ---------------------------------------------- | -------------------- | -------------------------------------------- |
| Container / cron deploy of the 5 fixes + 1.5.5 | user                 | not 09:40–20:50; default tomorrow morning    |
| `DAY_BASE_SPLIT_ENABLED=jra`                   | team-lead, then user | after deploy of watermark image; not tonight |
| feat-cache PUT / seed                          | nobody tonight       | HIT pedigree is degenerate                   |
| DELETE / TRUNCATE Neon                         | never                | UPSERT only                                  |
| Push these commits                             | user                 | not requested                                |

## How to tell git from production

```sh
cd apps/finish-position-cron
bunx wrangler deployments list --name finish-position-cron
# live 100% must still be 0c76062e until a confirmed window
```

Neon 80/80 after 05:50 does **not** mean the new image ran. It means
the Mac host one-shot wrote rows. The next container-generated race
will still use `0c76062e` and debug-gated layer timing.
