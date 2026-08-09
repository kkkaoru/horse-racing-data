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
`scripts/ensure-docker-compat.sh`.

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
