# Local DuckDB 1.5.3 cannot IcebergScan R2 Catalog (2026-08-16)

Recorded 2026-08-16 03:38 JST by `oversea-horse-race` from advisor's
local one-shot failure. This is an emergency-recovery note, not a pin
change.

## Failure

Local `predict_upcoming.py` one-shot died in the DuckDB feature builder:

```text
_duckdb.NotImplementedException: Not implemented Error: IcebergScan serialization not implemented
```

Stack (advisor report):

- `finish_position_features_duckdb.py:437` `run_staged_sql` → `con.execute(sql)`
- `stage_rec_table` / `stage_source_tables` / `stage_source`

That is the R2 Catalog / Iceberg read path, not the Neon upsert path.

## Version gap

| Location                                                | DuckDB constraint / resolved version                 |
| ------------------------------------------------------- | ---------------------------------------------------- |
| Local runtime (advisor)                                 | 1.5.3                                                |
| `apps/finish-position-predict-container/uv.lock`        | resolves `duckdb==1.5.3`                             |
| `apps/finish-position-predict-container/pyproject.toml` | `"duckdb>=1.1.0"` only, no upper bound               |
| `apps/finish-position-predict-container/Dockerfile`     | `"duckdb>=1.1.0"` only                               |
| Production container image                              | **not confirmed tonight**; baked at last image build |

`production-artifacts.json` records model files, not the image DuckDB
version. Do not assume the running Cloudflare image is 1.5.3 just
because the current lockfile is.

The open lower bound means a later `uv lock` / image rebuild can pick a
newer DuckDB than the last deployed image. Local emergency generation
then fails on Iceberg even when production containers still work.

## What not to change tonight

- Do not raise or pin DuckDB in `pyproject.toml` during this recovery.
- Do not treat option B (local PG feature regen) as proven; it is
  disk-I/O bound and needs an explicit switch investigation.
- Keep the production container path running as insurance.

## Follow-up (not tonight)

Pin an upper bound or lock the image DuckDB version so local one-shot
and the Cloudflare image cannot drift. Confirm the live image version
before any local downgrade attempt.
