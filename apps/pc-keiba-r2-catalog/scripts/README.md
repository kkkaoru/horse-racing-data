# PostgreSQL to R2 Data Catalog sync

`sync_r2_catalog.py` reads the local PostgreSQL replica through DuckDB's
read-only `postgres` attachment and writes Iceberg snapshots with PyIceberg.
The source URL is restricted to `localhost`, `127.0.0.1`, or `::1`. Generic
`SOURCE_DATABASE_URL` and `NEON_DATABASE_URL` variables are intentionally
ignored.

Existing feature Parquet files, feature archives, Neon, D1, processed feature
tables, and existing Catalog rows are never seed, completion, merge, or
fallback sources. The only transfer path is an allowlisted raw local PostgreSQL
table to the same-named Iceberg table. Derived tables such as
`race_entry_corner_features` and
`race_running_style_model_predictions` are not part of the inventory. This sync
CLI does not provide or configure a prediction-runtime PostgreSQL dependency;
readers must use the raw catalog tables and derive features at query time.

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
uv run sync_r2_catalog.py --full --tables jvd_um,nvd_um,nvd_nu,jvd_hn,jvd_bt
uv run sync_r2_catalog.py --full --tables jvd_ra --year-scope 2010-2014
uv run sync_r2_catalog.py --full --force
uv run test_sync_r2_catalog.py
```

`--year-scope` narrows a `--full` run to one five-year scope and therefore also
suppresses stale-scope deletion for that run, since every unexamined scope would
otherwise look stale. It fails rather than exiting successfully when the scope
matches nothing in the selected tables. `--force` restores the unconditional
rewrite described below.

Date-keyed tables no longer create one Iceberg partition per day. Tables with
`kaisai_nen`/`kaisai_tsukihi` retain both raw columns but partition only by
identity `kaisai_nen`. `jvd_hc` partitions by `truncate(4,
chokyo_nengappi)`. Date mode uses the requested date only to select its calendar
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

After a write is read back and verified, the slice's fingerprint is stored on
the Iceberg table as a `sync.fingerprint.<slice>` property — one key per
five-year scope, per date, or `__full__` for a whole-table master rewrite. The
next run extracts and fingerprints the source as usual, and when the fingerprint
matches the stored one it reports `"status": "skipped"` and performs no write,
no read-back, and no property commit. Because the property is only written after
verification, a skip is a proof that R2 already holds exactly these bytes rather
than an assumption that nothing changed.

Stored values are prefixed with `FINGERPRINT_FORMAT_VERSION`. The source
fingerprint cannot notice a change in how the _writer_ serializes data, so bump
that constant whenever the conforming rules, the partition spec, the timestamp
normalization, or the fingerprint algorithm changes; every stored value is then
invalidated and every slice is rewritten once. Anything unrecognized — including
a bare, unversioned hash — is not trusted and triggers a rewrite.

Measured on `jvd_ra` `2010-2014` (18,084 rows), a rewrite is ~24.6s wall clock
and the subsequent skip is ~8.9s. The marginal cost of an additional skipped
scope within one run is only the ~1s Iceberg `load_table`; the remainder is
per-run fixed cost (catalog handshake and the single manifest commit).
