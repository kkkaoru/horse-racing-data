# 2026-08-16 finish-position recovery

**~76 files.** Read three: `user-report-20260816.md` (decide),
`handoff-weight-rescore-20260816.md` (measure), `overnight-fp-index.md`
(categories). Do not start from this README’s older links.
Container deploy window / rollback ids: `container-deploy-window-20260816.md`.
DAY_BASE_SPLIT vs 08-12 outage: `day-base-split-0812-lineage-20260816.md`.
JRA pedigree collapse + 04/12 cache clock: `feat-cache-0412-provenance-20260816.md`.
Container observability design (no impl): `container-observability-design-20260816.md`.
Tonight git vs production: `tonight-commit-summary-20260816.md`.

Canary input: `races-canary-0401.tsv` (`jra 04/01`, post 09:40 JST,
expected 13 runners).

See `neon-primary-session-readonly-20260816.md` for the 03:18 JST
read-only diagnosis: Neon primary `default_transaction_read_only=on`
is `source=session` (pooler reuse), not a database/role default. The
correct per-write fix is `SET TRANSACTION READ WRITE`, matching
`sync-realtime-data` commit `171ed4d2`. Do not `secret put`, swap
endpoints, or `ALTER ROLE` / `ALTER DATABASE` tonight.

See `per-race-latency-phase1-20260816.md` for the 04:25–04:37 JST
read-only per-race latency baseline: 08-15 local p50, the 03:44→04:09
gap classification, and why container internals are currently unobservable.
See `per-race-latency-phase2-bottleneck-20260816.md` and
`per-race-latency-phase3-log-verify-20260816.md` for the ranked bottlenecks
and the log-only verification contract (no image yet).

Status 06:13 JST: 80/80 predictions on Neon; feat-cache still sparse.
jra 04/01 focused-full accepted 05:51, still MISS at +20 min (overtaken).
Proposal: `weight-trigger-queue-proposal-20260816.md`.
No deploy. Remaining MISS not expanded.

A8 isolated main generation (no production writes): `a8-main-generation-20260816.md`.

See `local-oneshot-recovery-playbook.md` for the host recovery that
reached Neon 80/80 at 05:50 JST (JRA 36 + NAR 32 + Ban-ei 12). Layer
counts are JRA 17 / NAR 10 / Ban-ei 7. Do not restart
`predict_upcoming.py` mid-category: `build_pipeline()` deletes every
layer dir. Force `PIPELINE_FORCE_MEMORY_GB=8` / `PIPELINE_FORCE_THREADS=4`.
JRA head-to-head needed RSS 6.95GB.

See `local-duckdb-icebergscan-gap-20260816.md` for the 03:38 JST local
one-shot failure: DuckDB `1.5.3` raises `IcebergScan serialization not
implemented` against R2 Catalog. Host recovery used `duckdb==1.5.5`.
Container deploy stays blocked during racing hours.
