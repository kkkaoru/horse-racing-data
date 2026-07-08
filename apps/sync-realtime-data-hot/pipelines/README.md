# Odds R2 Catalog Runbook

Realtime odds are written to R2 first. D1 is no longer the odds payload store.

## Runtime Store

- Latest object: `odds-live/v1/{source}/{yyyymmdd}/{raceKey}/payload.json`
- Raw snapshot object: `odds-snapshots/v1/{source}/{yyyymmdd}/{raceKey}/{fetchedAt}.json`
- Catalog staging object: `odds-catalog-staging/v1/kaisai_yyyymmdd={yyyymmdd}/{raceKey}/{fetchedAt}.ndjson`
- KV pointer: `odds:r2:payload:{raceKey}`, TTL `ODDS_R2_POINTER_KV_TTL_SECONDS`
- Edge Cache: `/api/odds/:raceKey`, TTL `ODDS_EDGE_CACHE_TTL_SECONDS`

The Worker read path is Edge Cache -> R2 latest -> Durable Object fallback -> KV mirror -> empty payload.
`?fresh=1` skips Edge Cache and Durable Object and reads R2 latest first.

## Catalog Resources

- R2 bucket: `pc-keiba-odds-archive`
- R2 Data Catalog warehouse: `78109ec18c7c85b194b19fb32e3bb149_pc-keiba-odds-archive`
- Pipeline stream: `odds_snapshots_hot_stream`
- Sink to create: `odds_snapshots_hot_sink`
- Pipeline to create: `odds_snapshots_hot_pipeline`
- Namespace/table: `odds.snapshots_hot`

Official references:

- R2 Data Catalog sink: https://developers.cloudflare.com/pipelines/sinks/available-sinks/r2-data-catalog/
- R2 SQL query: https://developers.cloudflare.com/r2-sql/query-data/
- R2 Data Catalog management: https://developers.cloudflare.com/r2/data-catalog/manage-catalogs/
- R2 API token permissions: https://developers.cloudflare.com/r2/api/tokens/

## Token Requirements

Use an API token authorized for this account and bucket with R2 Data Catalog, R2 storage, and R2 SQL permissions.
The existing `.env` `R2_API_TOKEN` was tested on 2026-07-08 and failed:

- `wrangler pipelines sinks create ...` -> `code: 1012`
- `wrangler r2 sql query ...` -> `80013: Unauthorized`

Do not treat that token as a valid catalog or R2 SQL token.

## Create Sink And Pipeline

Use the idempotent provisioning command first:

```sh
ODDS_R2_CATALOG_TOKEN="$TOKEN" \
bun run --filter sync-realtime-data-hot provision:odds-r2-catalog
```

It skips existing resources and creates only the missing sink or pipeline.

Manual equivalent:

```sh
set -a
source .env
set +a
TOKEN="${WRANGLER_R2_SQL_AUTH_TOKEN:-$R2_API_TOKEN}"
TOKEN="${TOKEN%\"}"
TOKEN="${TOKEN#\"}"

bunx wrangler pipelines sinks create odds_snapshots_hot_sink \
  --type r2-data-catalog \
  --bucket pc-keiba-odds-archive \
  --namespace odds \
  --table snapshots_hot \
  --catalog-token "$TOKEN" \
  --roll-interval 60

bunx wrangler pipelines create odds_snapshots_hot_pipeline \
  --sql-file apps/sync-realtime-data-hot/pipelines/odds-catalog-pipeline.sql
```

## R2 SQL Smoke Query

```sh
WRANGLER_R2_SQL_AUTH_TOKEN="$TOKEN" bunx wrangler r2 sql query \
  78109ec18c7c85b194b19fb32e3bb149_pc-keiba-odds-archive \
  "SELECT race_key, odds_type, count(*) AS rows FROM odds.snapshots_hot GROUP BY race_key, odds_type ORDER BY rows DESC LIMIT 20"
```

## Full Cutover Verification

```sh
ODDS_R2_VERIFY_RACE_KEYS=nar:2026:0708:30:11,nar:2026:0708:45:12 \
WRANGLER_R2_SQL_AUTH_TOKEN="$TOKEN" \
bun run --filter sync-realtime-data-hot verify:odds-r2-cutover
```

This checks the hot Worker `fresh=1` read path, the legacy D1 odds table absence
(or zero post-cutover writes if the table still exists), R2 Data Catalog status,
stream/sink/pipeline presence, and R2 SQL namespace visibility.

## Cutover Checks

```sh
bunx wrangler r2 object get pc-keiba-odds-archive/odds-live/v1/nar/20260708/nar:2026:0708:30:10/payload.json --remote -

bunx wrangler d1 execute sync-realtime-data-hot-v2 --remote --command \
  "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'odds_snapshots'"
```

The D1 query should return no rows after migration `0006_drop_odds_snapshots.sql`.
