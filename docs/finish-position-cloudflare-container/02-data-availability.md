# 02 — Data availability on a Cloudflare Worker Container

**Question:** Can a Cloudflare Worker Container obtain ALL the data the
finish-position prediction pipeline needs?

**Scope:** read-only investigation of the pipeline code + every `wrangler.*`
config in the repo + Cloudflare Containers/Hyperdrive/R2 docs (MCP). No code
changed.

**Verdict (short):** **YES — every data source is reachable from a CF Worker
Container, and crucially the pipeline already runs on one today** (the
`finish-position-cron` Worker + `FinishPositionPredictContainer` Durable Object
in `apps/finish-position-cron/wrangler.jsonc`). Data access is **not** the
blocker. The container is started with `enableInternet: true`
(`apps/finish-position-cron/src/dispatch.ts:18`) and gets the Neon URL as an
env var, so it reaches Postgres directly over TCP, R2 over HTTPS, and the
realtime workers over HTTPS — exactly as it does on the Mac docker host. The
only blocker was/is **compute lifetime** (the ~90-110 s reap vs the ~10 min
DuckDB build), which is a separate investigation (see 01 / 03), not data
availability.

The one thing that does **not** work is binding-based DB access: DuckDB's
`postgres` extension is a native libpq client and **cannot** use Hyperdrive or
the Workers `connect()` API. But it does not need to — direct egress to Neon
already covers it. R2 is reached via DuckDB `httpfs` + the R2 S3-compatible
endpoint (HTTPS), which also needs no binding. So the access model is "container
egress over the public internet", and that is already wired and proven.

---

## 1. What data the pipeline needs

Source of truth: `apps/finish-position-predict-container/src/predict_upcoming.py`,
`pipeline_runner.py`, `realtime_odds_fetcher.py`, the DuckDB base builder
`apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`, and the
layer chain in `apps/pc-keiba-viewer/src/scripts/finish-position-features/*.py`.

| #   | Data                                                                                                                                           | Concrete source                                                                                                                                                                                     | Access pattern                                                                                                                                                                               | Size / volume                                                                                           |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| A   | **Race + horse base rows** (JRA `jvd_se`/`jvd_ra`, NAR/Ban-ei `nvd_se`/`nvd_ra`)                                                               | Neon Postgres                                                                                                                                                                                       | DuckDB `postgres` ext `ATTACH … (TYPE postgres, READ_ONLY)` then heavy multi-CTE `postgres_scanner` over a **21-year history window** (`h.race_date < t.race_date`)                          | Largest input; tens of millions of `se` rows scanned per category. This is the ~10 min cost.            |
| B   | **Finish-position feature store** (`race_entry_corner_features`)                                                                               | Neon Postgres                                                                                                                                                                                       | Same ATTACH; the base feature parquet is built from this derived table; `--target-date` mode also pulls today's rows straight from `jvd_se`/`nvd_se` when the derived table hasn't refreshed | Derived per-entry table, 21y. Read by ~13 layer scripts.                                                |
| C   | **Tansho odds + popularity (historical)**                                                                                                      | Neon Postgres (`tansho_odds`, `tansho_ninkijun` columns on `se` / corner-features)                                                                                                                  | Part of the ATTACH scan (B)                                                                                                                                                                  | Included in B.                                                                                          |
| D   | **Tansho odds + bataiju (realtime, today's races)**                                                                                            | `sync-realtime-data-hot` worker `GET /api/odds/{raceKey}` and `sync-realtime-data` worker `GET /api/horse-weight/{raceKey}`                                                                         | Plain HTTPS GET per upcoming race, 5 s timeout, retry+backoff, graceful NULL fallback (`realtime_odds_fetcher.py`)                                                                           | One small JSON per race (~hundreds of races/day max). WAF needs a non-empty `User-Agent` (already set). |
| E   | **Running-style ("脚質") predictions** (`rs_p_nige/senkou/sashi/oikomi`, `rs_predicted_class`)                                                 | **Dual-source** (auto): R2 parquet `s3://pc-keiba-features-archive/running-style/predictions/by-day/{YYYY}/{MM}/{DD}/{category}/*.parquet` **or** Neon table `race_running_style_model_predictions` | `add-pacestyle-features.py`: R2 path via DuckDB `httpfs` + S3 secret to `{account}.r2.cloudflarestorage.com` (HTTPS); PG path via the ATTACH. `auto` falls back PG→ when R2 creds absent     | Per-day per-category parquet shard; small.                                                              |
| F   | **Pedigree** (sire / damsire)                                                                                                                  | Neon `jvd_um` (JRA) + `nvd_um` (NAR/Ban-ei), staged into a `horse_pedigree` temp by `pedigree_staging.py`                                                                                           | Part of the ATTACH scan; used by `add-baba-pedigree-affinity-features.py` and the base builder's pedigree CTEs                                                                               | `um` horse-registration tables, millions of rows (full scan, deduped JRA-wins).                         |
| G   | **Workout / handicap** (`jvd_hc`)                                                                                                              | Neon Postgres                                                                                                                                                                                       | ATTACH scan in `add-workout-features.py` (JRA only)                                                                                                                                          | JRA workout rows, 21y.                                                                                  |
| H   | **Exotic odds** (umaren/wide/sanrenpuku `nvd_o2/o3/o5`) — optional layers                                                                      | Neon Postgres                                                                                                                                                                                       | ATTACH scan in `add_exotic_odds_features.py`                                                                                                                                                 | Currently not in the production LAYER_CHAIN (exotic REJECTED per memory); listed for completeness.      |
| I   | **Model artifacts** (CatBoost `.cbm`/`model.json`, XGBoost `model.json`, per-class ensemble members + manifest, `metadata.json` feature_names) | R2 bucket `pc-keiba-finish-position-models` — **but baked into the Docker image today** (`/models`, `Dockerfile:75`); no runtime R2 read                                                            | Loaded from local image path at startup. JRA ~3.8–7 MB, NAR ~2.8 MB, Ban-ei ~4.1 MB + per-class members.                                                                                     |
| J   | **Static course lookup** (119-row course-numerical parquet)                                                                                    | Baked into image `/app/lookups` (`Dockerfile:55`)                                                                                                                                                   | Local file read by the iter14 course layer                                                                                                                                                   | 119 rows.                                                                                               |
| K   | **Output: predictions + audit**                                                                                                                | Neon `race_finish_position_model_predictions` (UPSERT) + `finish_position_cron_executions` (audit)                                                                                                  | psycopg over TCP (`db_driver.py`), chunked idempotent UPSERT                                                                                                                                 | Hundreds–thousands of rows/day write.                                                                   |

Distinct Postgres tables the build reads (via the DuckDB `pg` ATTACH alias):
`race_entry_corner_features`, `jvd_se`, `jvd_ra`, `jvd_um`, `jvd_hc`, `nvd_se`,
`nvd_ra`, `nvd_um`, `race_running_style_model_predictions`, and (only if the
exotic layers are wired) `nvd_o2`/`nvd_o3`/`nvd_o5`.

---

## 2. What's reachable from a CF Worker Container, and how

A CF Container has three independent ways to obtain data; this pipeline uses the
first two and bakes the third into the image:

### (a) Direct outbound internet (`enableInternet: true`) — PRIMARY, already wired

`apps/finish-position-cron/src/dispatch.ts:18` already starts the container with
`enableInternet: true` and passes `NEON_DATABASE_URL` (+ `RUN_DATE`,
`PREDICT_DAYS_AHEAD`, etc.) as `envVars`. Inside the container:

- **Postgres (Neon)** — DuckDB `postgres` extension `ATTACH`'s the Neon URL and
  opens its own libpq/TCP connection
  (`finish_position_features_duckdb.py:305-320`). The predictions UPSERT + audit
  use psycopg over TCP. Both work over plain egress; no binding involved. This
  is the model the Mac docker host uses and it is identical on a CF Container —
  the only difference is who sets the env var (launchd vs the Worker `start()`).
- **R2 (running-style parquet)** — DuckDB `httpfs` + an S3 secret pointed at
  `{account_id}.r2.cloudflarestorage.com` (`add-pacestyle-features.py:186-198`).
  Pure HTTPS to R2's S3-compatible endpoint; needs R2 S3 API tokens
  (`R2_ACCOUNT_ID`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`) as env vars, not a
  binding.
- **Realtime odds / weight workers** — HTTPS GET to
  `sync-realtime-data-hot.kkk4oru.com` and `sync-realtime-data.kkk4oru.com`
  (`realtime_odds_fetcher.py:51-52`). Note: `global_fetch_strictly_public` is
  set on those workers, and the custom domains resolve publicly, so an external
  container reaches them the same way the Mac host does.

### (b) Worker bindings via the outbound Worker / `containerFetch` — AVAILABLE, optional

CF Containers can reach Worker bindings (R2, D1, KV, DO state) without
credentials by routing an in-container HTTP request through the Worker's
outbound handler (`MyContainer.outboundByHost = { "my.r2": … env.BUCKET.get() }`
— Cloudflare changelog 2026-03-26, `@cloudflare/containers` ≥ 0.2.0). The Worker
already has the R2 bucket available (the viewer binds `FINISH_POSITION_MODELS`
→ `pc-keiba-finish-position-models`; the cron Worker would add the same). This
is the **credential-free** alternative to (a) for R2/D1/KV. It is _not_ required
for this pipeline because (a) already covers everything, but it is the clean way
to drop R2 S3 tokens if desired (model artifacts, or running-style parquet).

### (c) Baked into the image — model artifacts + lookup (current)

Model artifacts (I) and the course lookup (J) are COPYed into the image at build
time (`Dockerfile:55,75`). No runtime fetch. This is fine on a CF Container too
— it is just a bigger image. (Switching to runtime R2 read via (a) or (b) is
possible but optional.)

### Hyperdrive — NOT usable by this pipeline (and not needed)

Other workers bind Hyperdrive (viewer `002197…`, hot `1820ef…`, features
`3c70be…`, old `d0041a…`). **Hyperdrive does not help here:** it is a
Worker-isolate construct — Postgres is reached through the Workers `connect()`
API / `env.HYPERDRIVE.connectionString` from inside JS, with the documented
`localConnectionString` only for `wrangler dev`. DuckDB's `postgres` extension is
a **native libpq client running in the container**, not in the Worker isolate, so
it cannot consume a Hyperdrive binding. Heavy CTEs over a Hyperdrive-pooled
connection would also fight Hyperdrive's pooling/caching model. **Conclusion:
keep DuckDB's direct Neon ATTACH over `enableInternet` egress.** Hyperdrive is a
red herring for the container's feature build (it is the right tool for the JS
workers, which is why they bind it).

---

## 3. Gaps vs the LOCAL environment

Things the Mac docker run uses that a CF Container would **not** have for free,
and the migration each needs:

| Local-only today                                                                                                                 | Why it matters                                                                                                                                                                                                      | CF Container resolution                                                                                                                                                                                                                                                                                                                                                                                             |
| -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`SOURCE_DATABASE_URL` = local Colima PG** (`127.0.0.1:15432`) for the long DuckDB ATTACH (`predict_upcoming.py:89`, DEPLOY.md) | The Mac run builds features against the **local logical replica** to dodge Neon's SSL idle-eviction during the ~10 min scan, and only UPSERTs to Neon. A CF Container has no local mirror reachable at `127.0.0.1`. | Point the feature build at **Neon directly** (drop `SOURCE_DATABASE_URL` → it falls back to `NEON_DATABASE_URL`, `conn_url.resolve_source_url`). Risk: Neon idle-timeout on a 10 min ATTACH and Neon compute cost (memory `project_local_pg_regen_disk_bound`, `project_serve_skew_root_cause`). Mitigation belongs to the compute/perf design (01/03), not data access — but it is the one real behavioral change. |
| **Models on local disk via image bake**                                                                                          | Already image-baked, works identically on CF.                                                                                                                                                                       | None — or move to runtime R2 read via §2(b).                                                                                                                                                                                                                                                                                                                                                                        |
| **R2 S3 API tokens** (`R2_ACCOUNT_ID`/`R2_ACCESS_KEY_ID`/`R2_SECRET_ACCESS_KEY`) for the running-style R2 path                   | Currently the Mac run may use `--rs-source=auto` and fall back to PG when tokens are absent.                                                                                                                        | Provide the 3 R2 tokens as Worker secrets → `envVars`, OR switch running-style read to PG `race_running_style_model_predictions` (already supported, no new creds), OR route via §2(b) outbound Worker R2 binding (credential-free). All three reach the same data.                                                                                                                                                 |
| **No DuckDB local-disk scratch concerns**                                                                                        | The build writes temp parquet to `/tmp/predict-upcoming` and DuckDB spills to disk under memory pressure. CF `standard-4` = 20 GB disk / 12 GiB RAM.                                                                | Disk OK for the parquet (small); the 21y ATTACH scan + DuckDB spill must fit 12 GiB RAM / 20 GB disk. Validate in compute design (01/03); not a data-reachability gap.                                                                                                                                                                                                                                              |
| **launchd-set `RUN_DATE`**                                                                                                       | The Worker already computes JST run date (`time.ts`) and passes it.                                                                                                                                                 | None — Worker path already exists.                                                                                                                                                                                                                                                                                                                                                                                  |

There is **no data source that is structurally unavailable** to a CF Container.
Everything Neon-resident is reachable over TCP egress; everything R2-resident is
reachable over HTTPS (S3 endpoint or outbound-Worker binding); the realtime
workers are public HTTPS; models/lookup are image-baked. The only _change_ is
substituting the local-replica `SOURCE_DATABASE_URL` with Neon (or a
cloud-reachable replica), which shifts load onto Neon — a cost/perf decision, not
an availability blocker.

---

## 4. Existing bindings inventory (what's already wired vs new)

From every `wrangler.*` in the repo:

| Worker                      | Hyperdrive | R2                                                                                                           | D1                                | Service bindings                                           | KV                      | Queues        | DO                                  | Container                                                                         |
| --------------------------- | ---------- | ------------------------------------------------------------------------------------------------------------ | --------------------------------- | ---------------------------------------------------------- | ----------------------- | ------------- | ----------------------------------- | --------------------------------------------------------------------------------- |
| **finish-position-cron**    | —          | —                                                                                                            | `FINISH_POSITION_CRON_DB` (audit) | —                                                          | —                       | —             | `FINISH_POSITION_PREDICT_CONTAINER` | `FinishPositionPredictContainer` standard-4, `max_instances:1`, **cron disabled** |
| pc-keiba-viewer             | `002197…`  | `FINISH_POSITION_MODELS` (`pc-keiba-finish-position-models`)                                                 | 3× (realtime / hot / features)    | `REALTIME_DATA`, `REALTIME_FEATURES`, `REALTIME_HOT`, self | 2×                      | 1             | PADDOCK_ROOM, RACE_TREND_ROOM       | —                                                                                 |
| sync-realtime-data-hot      | `1820ef…`  | `ODDS_ARCHIVE`                                                                                               | `REALTIME_HOT_DB`                 | `PC_KEIBA_VIEWER`                                          | `ODDS_HOT_KV`           | hot-jobs      | OddsCacheHot                        | —                                                                                 |
| sync-realtime-data          | `d0041a…`  | `RUNNING_STYLE_MODELS` (`pc-keiba-finish-position-models`), `FEATURES_ARCHIVE` (`pc-keiba-features-archive`) | `REALTIME_DB`                     | viewer, hot, features                                      | DETAIL_SECTION_CACHE_KV | 4×            | 5 DOs                               | —                                                                                 |
| sync-realtime-data-features | `3c70be…`  | `FEATURES_ARCHIVE`, `MODELS`                                                                                 | `REALTIME_FEATURES_DB`            | `REALTIME_OLD`                                             | `FEATURES_KV`           | features-jobs | —                                   | —                                                                                 |

**Already wired for the container path:** `enableInternet`,
`NEON_DATABASE_URL` secret, the container + DO + migration, the D1 audit DB. The
realtime endpoints are public custom domains.

**New (only if you choose a binding-based path instead of pure egress):**

- R2 bucket binding `pc-keiba-finish-position-models` on the **cron** Worker (it
  is bound on the viewer/features/old workers but not on cron) — needed only if
  you move model artifacts or running-style read to §2(b) outbound-Worker R2.
- R2 S3 API tokens as secrets — needed only if you keep the running-style
  `httpfs` S3 path on the container.
- Nothing else. No Hyperdrive binding is useful for the container.

---

## 5. Data-access feasibility verdict

**FEASIBLE — no data-access blocker.** A CF Worker Container can obtain every
input the finish-position pipeline needs, and the wiring already exists and has
been deployed:

- Postgres (Neon): direct DuckDB ATTACH + psycopg over `enableInternet` TCP. ✔
- Feature store / pedigree / workout / odds-historical: same ATTACH scan. ✔
- Realtime odds + weight: public HTTPS to the hot/main workers (UA already set). ✔
- Running-style predictions: R2 `httpfs` (S3 endpoint, HTTPS) **or** Neon table
  **or** outbound-Worker R2 binding — three working routes. ✔
- Model artifacts + lookup: image-baked (no runtime dependency). ✔
- Output UPSERT + audit: psycopg over TCP. ✔

The single behavioral change from the local setup is that the heavy DuckDB
feature build must read **Neon directly** instead of the local Colima replica
(no `127.0.0.1` inside CF). That moves a ~10 min, 21-year, multi-table scan onto
Neon — raising **Neon compute cost / idle-timeout risk**, and it interacts with
the **~90-110 s container reap** that is the historical reason the CF cron was
disabled (DEPLOY.md, `feedback_cloudflare_containers_90s_reap`). **Those are
compute-lifetime and cost problems, owned by the mechanism/architecture design
(docs 01 / 03), not data availability.** From the data-access angle alone, the
answer is an unambiguous yes.
