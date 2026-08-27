# local-postgresql

Local PostgreSQL for development. It runs with Apple Container CLI
(`https://github.com/apple/container`) and exposes PostgreSQL on
`0.0.0.0:5432` by default so a Parallels Desktop Windows VM can connect to
the Mac host.

## Prerequisites

- `bun`
- Apple Container CLI (`container`)

```sh
container system start
```

Local PostgreSQL always runs on Apple Container CLI. `compose.yml` is a
legacy Docker Compose reference only — do not start it with colima/docker.
Cloudflare Containers deploy (`wrangler deploy` in `finish-position-cron` /
`mlflow-ui-proxy`) still needs a Docker API and uses colima via
`scripts/ensure-docker-compat.sh`. Those package scripts pass the deploy or
dev command through the helper, so Colima is stopped automatically when the
Docker-backed command exits if this invocation started it.

If another PostgreSQL is already listening on port `5432`, stop it or change
`POSTGRES_PORT` in `apps/local-postgresql/.env`.

## Setup And Start

```sh
cp apps/local-postgresql/.env.example apps/local-postgresql/.env
bun --cwd apps/local-postgresql start
```

PostgreSQL data is stored on the Mac filesystem at:

```text
apps/local-postgresql/data/postgres
```

The `data/` directory is ignored by Git so large local datasets stay on the
Mac filesystem instead of inside the container writable layer.

## Commands

```sh
bun --cwd apps/local-postgresql start
bun --cwd apps/local-postgresql stop
bun --cwd apps/local-postgresql pc-keiba:update
bun run --cwd apps/local-postgresql pc-keiba:update-and-sync
bun run --cwd apps/local-postgresql scrape:netkeiba-training
bun --cwd apps/local-postgresql logs
bun --cwd apps/local-postgresql psql
bun --cwd apps/local-postgresql status

# Index corruption guard (amcheck + REINDEX, never DROP INDEX)
bun --cwd apps/local-postgresql indexes:check:quick
bun --cwd apps/local-postgresql indexes:repair:quick
bun --cwd apps/local-postgresql indexes:check
bun --cwd apps/local-postgresql indexes:repair
```

`start` runs `indexes:repair:quick` after PostgreSQL is healthy.
`replica:push` also runs `indexes:repair:quick` before R2/Neon sync so XX002
corruption cannot silently break ingest or push.

`pc-keiba:update` starts local PostgreSQL, boots the configured Parallels VM
(`PARALLELS_VM_NAME`, default `Windows 11`), and waits for the Windows-side
PC-KEIBA update to finish. After a successful update it gracefully shuts down
the VM and waits for the `stopped` state, releasing the Windows VM memory.
When the update fails or times out, the VM is intentionally left running for
inspection.

The Windows-side updater must already be installed with
`scripts/install-pc-keiba-auto-update.ps1`. The host command invokes
`py -3.12 ... --wait --close-when-done`; override the Python launcher with
`PARALLELS_PYTHON_COMMAND` when the Windows installation uses another command.
Set `LOCAL_POSTGRES_AUTO_START=0` when PostgreSQL is already managed by another
process.

`pc-keiba:update-and-sync` is the end-to-end orchestrator. It keeps the
individual commands independent and runs them in this strict order:

1. `pc-keiba:update`
2. verify that the Parallels VM is `stopped`
3. materialize local corner features for today through the seven-day publication horizon
4. `scrape:netkeiba-training` for JRA runners without an official `jvd_hc` or
   `jvd_wc` workout in the preceding 14 days
5. `replica:push` (R2 Catalog, then Neon)
6. refresh the selected `race_entity_history_v1` year and atomically publish the
   direct-Catalog Parquet manifest
7. enqueue authenticated `sync-realtime-data` jobs for JST today and tomorrow;
   after each `discover-urls` enqueue, poll the read-only discovery status until
   D1 reaches the Neon JRA race count, then enqueue
   `plan-premium-race-data-fetches`

The entity-history stage runs `sync:entity-history-serving` with the same JST
run year, so the API manifest cannot lag the raw R2 Catalog replica after a
successful end-to-end update.

The orchestrator forces `PARALLELS_STOP_AFTER_SUCCESS=1`. If the Windows update
fails, it does not start replica synchronization. If the VM is not confirmed
stopped, it also refuses to sync. A replica failure is returned as a non-zero
exit without repeating the already-completed Windows update. The final queue
orchestration also fails closed: authorization and permanent HTTP failures stop
the orchestrator, while network errors and HTTP 408/429/5xx responses are
retried up to three times. Discovery runs outside the request lifetime in the
Worker queue. The host polls `/api/internal/discovery-status` every 10 seconds
for at most 15 minutes and does not enqueue premium planning until D1 has all
Neon JRA races for that date. This prevents a successful replica push from
being reported as a fully completed pipeline before newly synchronized races
reach D1.

The inline API uses `REALTIME_ADMIN_TOKEN` from the process environment, or
falls back to `apps/sync-realtime-data/.dev.vars`. Override the production base
URL with `SYNC_REALTIME_DATA_BASE_URL`. `SYNC_REALTIME_DATA_DATE=YYYYMMDD`
overrides the JST base date; the following date is always derived and processed
as well.

`scrape:netkeiba-training` is also independently runnable. It idempotently
applies `sql/20260822000000_create_netkeiba_training_workouts.sql`, queries JST
today and tomorrow from local `jvd_ra`/`jvd_se`, and calls the authenticated
`/api/internal/netkeiba-training-workouts` endpoint once per qualifying race.
It joins response horse numbers to local `umaban` and `ketto_toroku_bango`, then
upserts each race in one PostgreSQL transaction. Invalid authentication,
invalid payloads, and upstream HTTP errors fail closed without deleting existing
rows. An unpublished race with no workouts, or a non-empty response containing
workouts only for runners that already have official data, is safely skipped
and processing continues with the next race. Use the same
`REALTIME_ADMIN_TOKEN`, `SYNC_REALTIME_DATA_BASE_URL`, and
`SYNC_REALTIME_DATA_DATE` overrides as the post-sync discovery step.

For a one-time cleanup of a Colima VM left by an older command, first confirm
that no Docker workload is needed (`docker ps`), then run `colima stop` and
verify with `colima status`. Do not stop a VM that owns another active Docker
workload.

### New tables require separate Neon DDL

`replica:push:neon` synchronizes rows only. It enumerates local tables with
primary keys, but it does **not** create or alter destination tables. Therefore,
adding a local migration that creates a table also requires this deployment
checklist before the next scheduled push:

1. Apply the same committed `CREATE TABLE`, constraints, comments, and indexes
   to Neon. Do not add destination-only foreign keys or other schema changes.
2. Compare local and Neon `information_schema.columns` output, including column
   order, types, nullability, defaults, and numeric precision/scale.
3. Compare primary/check/foreign-key constraints, indexes, and column comments.
4. Run a focused push with `REPLICA_SYNC_TABLES=<new_table>` and verify row count
   and a deterministic row fingerprint on both databases.
5. Run the normal `bun --cwd apps/local-postgresql replica:push:neon` command to
   prove the complete synchronization path still succeeds.
6. Compare the full local/Neon table inventories. Local-only training/log tables
   intentionally excluded by `push-neon-sync.ts` are allowed; every other
   local primary-key table must exist in Neon.

A missing destination DDL fails during the pre-copy fingerprint query with
`relation "public.<table>" does not exist`; successful local migration tests do
not detect this operational gap.

### Index corruption background

PC-KEIBA ingest has repeatedly hit PostgreSQL B-tree corruption (`XX002`,
amcheck invariant violations) on indexes such as `nvd_bn_pk` / `jvd_ra_*`.
Symptoms include unique indexes that stop enforcing uniqueness. The guard:

1. Scans btree indexes with `amcheck`
2. Removes heap duplicates that slipped past a corrupted unique index
3. Rebuilds broken indexes with `REINDEX INDEX` (in-place rebuild; **never**
   `DROP INDEX`)
4. Cleans corrupt orphan temp catalog rows that block autovacuum

Run `indexes:repair:quick` before retrying PC-KEIBA 通常データ登録 after any
`XX002` / Npgsql index error.

Default Mac connection string:

```text
postgresql://horse_racing:horse_racing@localhost:5432/horse_racing
```

From Parallels Desktop Windows, connect to the Mac host IP instead of
`localhost`.

For Parallels Shared Network, the Mac host is often:

```text
postgresql://horse_racing:horse_racing@10.211.55.2:5432/horse_racing
```

You can check the current Mac-side Parallels IP with:

```sh
ipconfig getifaddr bridge100
```

Connection settings:

```text
Server: 10.211.55.2
Port: 5432
Database: horse_racing
User: horse_racing
Password: horse_racing
```
