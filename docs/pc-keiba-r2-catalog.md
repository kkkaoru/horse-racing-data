# PC-KEIBA R2 Catalog architecture

Last updated: 2026-07-15

## Source authority

The only source allowed to publish tables into `pc-keiba-r2-catalog` is the
local PC-KEIBA PostgreSQL instance on a loopback address. The publisher rejects
remote PostgreSQL URLs. Neon, D1, feature archives, previously generated
Parquet, and processed `daily_race_entries` are never seed, repair, or fallback
inputs.

The published Iceberg tables preserve the local PostgreSQL table names and
primary keys in namespace `pc_keiba`. Date-keyed tables use identity partitions.
The source-separated `oversea_*` tables are also published as small, full-table
snapshots so overseas identities, histories, pedigrees, and auditable person
statistics are available from the Catalog without treating Neon as a source.
Each write verifies row count, primary-key uniqueness, and a deterministic
fingerprint before recording a snapshot manifest.

## Runtime ownership

| Workload                                       | Runtime source                                                     |
| ---------------------------------------------- | ------------------------------------------------------------------ |
| Viewer page display                            | Hyperdrive to Neon, lightweight bounded reads only                 |
| Realtime race keys and base feature generation | `pc-keiba-r2-catalog` Worker, fixed R2 SQL over raw Iceberg tables |
| Running-style feature aggregation              | R2 Catalog / R2 SQL; no PostgreSQL or D1 feature fallback          |
| Finish-position Container feature layers       | DuckDB read-only Iceberg REST attachment                           |
| Prediction result projection                   | Neon writes; viewer reads the projected rows                       |

KV and Cache API contain only short-lived results derived from the current raw
Iceberg generation. Generated R2 feature Parquet uses a versioned `catalog-v1`
namespace. Running-style prediction output uses `raw-iceberg-v1`. Existing
unversioned feature or prediction objects are not accepted as inputs.

## Synchronization

```sh
bun run --filter local-postgresql replica:push:r2-catalog
bun run --filter local-postgresql replica:push:neon
```

`replica:push` runs Catalog publication first, followed by the lightweight
viewer projection sync to Neon. For a focused partition verification:

```sh
bun run --cwd apps/pc-keiba-r2-catalog sync --date 20260715
```

Overseas supplemental tables are intentionally master-style snapshots and must
be selected explicitly in full mode after local PostgreSQL is updated:

```sh
bun run --cwd apps/pc-keiba-r2-catalog sync --full --tables \
  oversea_runner_identity,oversea_runner_source_id,oversea_horse_race_history,oversea_person_race_history,oversea_horse_pedigree,oversea_person_win_rate_stats
```

Required Catalog credentials are `R2_CATALOG_TOKEN`, `R2_CATALOG_URI`, and
`R2_CATALOG_WAREHOUSE`. Secrets must be Worker secrets or ignored local env
values and must never be committed.

## Failure behavior

Catalog read failures stop feature generation. Production batch code must not
fall back to local PostgreSQL, Neon, Hyperdrive, D1 feature rows, or an older
feature Parquet generation. This fail-closed behavior prevents a successful but
unverifiable prediction build from hiding source drift.
