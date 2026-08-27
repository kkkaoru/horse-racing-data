# PostgreSQL to R2 Data Catalog sync

`sync_r2_catalog.py` reads the local PostgreSQL replica through DuckDB's
read-only `postgres` attachment and writes Iceberg snapshots with PyIceberg.
The source URL is restricted to `localhost`, `127.0.0.1`, or `::1`. Generic
`SOURCE_DATABASE_URL` and `NEON_DATABASE_URL` variables are intentionally
ignored.

Existing feature Parquet files, feature archives, Neon, D1, processed feature
tables, and existing Catalog rows are never seed, completion, merge, or
fallback sources. The primary transfer path is an allowlisted raw local
PostgreSQL table to the same-named Iceberg table. Prediction-derived tables such
as `race_entry_corner_features` and `race_running_style_model_predictions` are
not part of the inventory. This sync
CLI does not provide or configure a prediction-runtime PostgreSQL dependency;
readers must use the raw catalog tables and derive features at query time.

The Worker exposes the derived JRA training view as
`GET /v1/race-trainings?date=YYYYMMDD&keibajoCode=NN&raceBango=NN`. The
response is `{ "rows": [...] }` with Viewer-compatible `Training` fields and a
`trainingDataSource` of `jra` or `netkeiba`. It joins the race's `jvd_se`
runners to the preceding 14 days of `jvd_hc` and `jvd_wc`, unions the exact
race's `netkeiba_training_workouts`, prefers the official row only when the
complete workout signature is duplicated, and retains a placeholder for each
runner without a real workout. Exact-race `/admin/purge` requests purge this
view from Cache API and KV together with the race-feature entry.

Required for writes:

```sh
export R2_CATALOG_TOKEN='...'
```

The token must have R2 Data Catalog and R2 object read/write permissions.
`CLOUDFLARE_DEBUG_TOKEN` and `WRANGLER_R2_SQL_AUTH_TOKEN` are supported as
fallbacks. Override the checked-in account/bucket defaults when needed:

```sh
export R2_CATALOG_URI='https://catalog.cloudflarestorage.com/<account>/<bucket>'
export R2_CATALOG_WAREHOUSE='<account>_<bucket>'
export PC_KEIBA_SOURCE_DATABASE_URL='postgresql://...@127.0.0.1:15432/horse_racing'
```

Examples:

```sh
uv run sync_r2_catalog.py --date 20260715 --dry-run
uv run sync_r2_catalog.py --date 20260715 --tables nvd_se,nvd_ra
uv run sync_r2_catalog.py --date 20260715 --tables jvd_se,jvd_hc,jvd_wc,netkeiba_training_workouts
uv run sync_r2_catalog.py --full --tables jvd_um,nvd_um,nvd_nu,jvd_hn,jvd_bt
uv run sync_r2_catalog.py --full --tables jvd_ra --year-scope 2010-2014
uv run sync_r2_catalog.py --full --force
uv run sync_r2_catalog.py --full --tables oversea_runner_identity,oversea_runner_source_id,oversea_horse_race_history,oversea_person_race_history,oversea_horse_pedigree,oversea_person_win_rate_stats
uv run test_sync_r2_catalog.py
uv run sync_entity_history.py --full
uv run sync_entity_history.py --year 2026
uv run publish_entity_catalog_manifest.py --skip-upload
uv run refresh_entity_history_serving.py --year 2026
uv run test_sync_entity_history.py
uv run test_publish_entity_catalog_manifest.py
uv run test_refresh_entity_history_serving.py
```

`sync_entity_history.py` is the sole materialized serving-table exception. It
expands each completed local JRA/NAR runner into horse, jockey, trainer, and
owner rows in `race_entity_history_v1`. The table is identity-partitioned by
entity type, source, one-hex MD5 bucket, and year. This lets the Worker prune to
a small entity/year partition without imposing a two-year history ceiling.
`--full` builds a temporary table and swaps it into service only after every
year succeeds; `--year` atomically refreshes one year and should run after the
corresponding raw `jvd_ra/jvd_se/nvd_ra/nvd_se` refresh.

The latency-sensitive API reads those same Catalog-managed Parquet files
directly through the native R2 binding. Target resolution reads the selected
year from the raw `jvd_ra/jvd_se/nvd_ra/nvd_se` snapshots; history reads only
the entity/source/hash/year partitions from `race_entity_history_v1`.
`publish_entity_catalog_manifest.py` derives a compact index from the five
current Iceberg snapshot IDs and data-file paths. It publishes only the small
`entity-catalog-serving-v1/manifest.json` pointer; it never copies table data.
The manifest is written last, so every API request observes one complete set of
immutable Catalog files.

```bash
uv run refresh_entity_history_serving.py --year 2026 \
  --output tmp/entity-catalog-manifest.json
```

The orchestrator is fail-fast. It refreshes the raw Catalog tables, atomically
refreshes the selected history year, and then publishes the snapshot-derived
manifest. Use `--skip-upload` to build and validate the manifest locally without
changing the API pointer. The Worker preserves the existing signed cursor and
future-leakage filters while avoiding R2 SQL scheduling and any duplicate
`history.pack` or `target.pack` dataset.

The `oversea_*` tables are small source-separated raw tables. They are treated
as explicit full-table snapshots: local PostgreSQL remains the sole authority,
while Neon and existing Catalog objects are never used to complete or repair
them.

`--year-scope` narrows a `--full` run to one five-year scope and therefore also
suppresses stale-scope deletion for that run, since every unexamined scope would
otherwise look stale. It fails rather than exiting successfully when the scope
matches nothing in the selected tables. `--force` restores the unconditional
rewrite described below.

Date-keyed tables no longer create one Iceberg partition per day. Tables with
`kaisai_nen`/`kaisai_tsukihi` retain both raw columns but partition only by
identity `kaisai_nen`. `jvd_hc` and `jvd_wc` partition by `truncate(4,
chokyo_nengappi)`. `netkeiba_training_workouts` follows the race-date year
partition and uses `updated_at` for its source marker. Date mode uses the requested date only to select its calendar
year, extracts that entire year from local PostgreSQL, and atomically rewrites
the year partition. It never reads Catalog rows to seed or merge that write.
Master tables are skipped and require `--full`.

Full mode maps source and target year sets to stable five-year scopes aligned
to years divisible by five (for example `1995-1999`). Each scope is extracted
once from local PostgreSQL, fully overwritten once, then read back and verified
as one fingerprint/row-count unit. Target-only scopes are deleted. The Iceberg
partition spec remains yearly. A five-year chunk is capped at 5,000,000 rows or
2 GiB of Arrow buffers. Master tables are streamed as RecordBatches and fail
before writing if either
`R2_SYNC_MASTER_MAX_ROWS` (default 2,000,000) or
`R2_SYNC_MASTER_MAX_BYTES` (default 1 GiB of Arrow buffers) is exceeded.
Each successful or unchanged table writes a row to
`pc_keiba._sync_manifest`. All of a run's manifest rows are appended in a single
Iceberg commit at the end of the run, and a run that fails part way through
still flushes the rows it completed. The data snapshot also records the run ID,
row count, source fingerprint, mode, and partition date or year in snapshot
properties.

## Skipping unchanged slices

Two layered proofs, both stored as Iceberg table properties after a verified
write:

1. `sync.source-fingerprint.<slice>` — a PostgreSQL aggregate marker
   (`count(*)`, min/max of the table's marker range column, and
   `bit_xor(hashtextextended)` over marker extras + primary key).
   JV/NAR tables use `data_sakusei_nengappi` + `record_id`; `oversea_*`
   tables use `updated_at` because they have neither JV column.
   Computed via `postgres_query` without `SELECT *`. When it matches, the
   run reports `"status": "skipped"` and performs no extract, no Arrow
   fingerprint, no write, and no read-back.
2. `sync.fingerprint.<slice>` — the Arrow IPC SHA-256 used to verify that R2
   actually holds those bytes. Used when the source marker is absent (first run
   after a writer change) so a matching extract can still skip the R2 PUT.

Slice keys are one per five-year scope, per date, or `__full__` for a
whole-table master rewrite. Iceberg `load_table` is cached per table within a
run so a 15-scope plan does not pay 15 catalog round trips.

`--force` still rewrites every slice. Source-marker values are prefixed with
`SOURCE_MARKER_FORMAT_VERSION`; bump that when the PG SQL changes. Arrow values
are prefixed with `FINGERPRINT_FORMAT_VERSION`; bump that when the writer
serialization changes. Anything unrecognized — including a bare, unversioned
hash — is not trusted and triggers a rewrite.

DuckDB is capped at `memory_limit=6GB` and `threads=4`. Phase timings are
emitted as `phase_timing` JSON lines (`source_marker`, `load_table`,
`extract_source`, `arrow_fingerprint`, `r2_put`, `r2_verify`, `skip_extract`,
`skip_write`) plus a final `run_summary`.
