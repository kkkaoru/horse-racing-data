# Historical Finish-Position Prediction Mechanism (Superseded)

**Date:** 2026-06-18
**Pipeline described here:** legacy macOS launchd + Docker container (local Colima)
**Current Status:** superseded by Cloudflare-only production. Launchd/local Docker
is historical or optional local/manual tooling only, not scheduler, authority,
fallback, or ordering dependency.

---

## 1. Daily Schedule & Execution Model

### Historical Schedule (launchd-based, superseded)

The deprecated `com.kkk4oru.finish-position-predict.plist` defined two
StartCalendarInterval entries:

1. **JST 03:00** — NAR + Ban-ei only (`PREDICT_CATEGORIES=nar,ban-ei`)
   - JRA mirror (`jvd_se` table) not yet available at 03:00 JST (arrives ~09:03 JST)
   - Races-per-run: ~48 NAR + 0 Ban-ei
   - Duration: ~3–4 min
   - Reason: Avoids wasted work; places JRA predictions in later 09:30 run

2. **JST 09:30** — ALL categories (`PREDICT_CATEGORIES=<all>`)
   - JRA mirror has arrived (~09:03 JST); hot-worker D1 has 12+ h of advance-odds
   - On weekdays (no JRA): zero races, exits in ~30 s
   - On Sat/Sun: JRA races with REAL odds (not fallback); NAR repeats
   - Races-per-run: 0–30 JRA (Sat/Sun only) + 48 NAR + 0 Ban-ei = 48–78 total

### Race-Hours Guard (Intra-day)

Historically, a separate launchd `race-prediction-guard` fired every 20 minutes (10:00–20:40 JST) during race hours. It could re-kick the legacy local prediction pipeline with `PREDICT_DAYS_AHEAD=1` for freshness, locked by `mkdir` on `/tmp/finish-position-predict.lock`. This is not the production authority.

### Idempotency & Locking

- **Upsert semantics:** Predictions are written via `race_finish_position_model_predictions` primary-key `ON CONFLICT DO UPDATE`, so re-running the same date is safe (same model_version, source, date, keibajo, race_bango, ketto overwrites the prior row).
- **Process lock:** `/tmp/finish-position-predict.lock` (atomic mkdir) prevents concurrent runs. If the 03:00 run is in flight at 09:30, the 09:30 cron logs a skip and exits 0.
- **Log archival:** Dated logs under `/Users/kkk4oru/Library/Logs/finish-position-predict/${RUN_DATE}.log` (UTC timestamp, redacted credentials).

---

## 2. End-to-End Pipeline Steps

### A. Pre-flight Checks (Wrapper Script)

File: `scripts/launchd/finish-position-predict-daily.sh`

1. **Colima status** — starts if down
2. **Docker daemon reachable** — via `docker info`
3. **Image cached** — `finish-position-predict-local:split2`; builds if missing
4. **Neon credentials** — reads `apps/local-postgresql/.env.replica` for `NEON_DATABASE_URL`
5. **R2 credentials** — optional; reads `.env` for `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` (determines `RS_SOURCE=auto|pg`)

### B. Container Entrypoint & Orchestration

File: `apps/finish-position-predict-container/src/predict_upcoming.py`

Invoked via: `uv run python src/predict_upcoming.py` (entrypoint in Dockerfile)

**Env vars passed by wrapper:**

```
SOURCE_DATABASE_URL = postgresql://horse_racing:horse_racing@127.0.0.1:15432/horse_racing
NEON_DATABASE_URL   = postgresql://<neon-credentials>@<neon-host>/neondb
RUN_DATE            = YYYYMMDD (e.g., 20260617)
RUN_DATE_ISO        = YYYY-MM-DD
PREDICT_DAYS_AHEAD  = 0 (default) or 1 (race-hours guard)
PREDICT_CATEGORIES  = "nar,ban-ei" (03:00 JST) or "" (09:30 JST, all)
MODELS_DIR          = /models
RS_SOURCE           = "auto" (R2 parquet) or "pg" (Neon ATTACH, fallback)
R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET (optional)
```

**Main loop (per-category):**

1. **Resolve categories** — filter `PREDICT_CATEGORIES` allowlist; unset = all 3 (jra, nar, ban-ei)
2. **For each category:**
   - Fetch upcoming race keys (keibajo_code, race_bango) from Neon via `SOURCE_DATABASE_URL`
   - Fetch realtime odds + bataiju from Cloudflare hot-worker endpoints (5s timeout each)
   - Build v8 feature parquet via subprocess (DuckDB + 14 layer scripts)
   - Load production model + per-class ensemble members from `/models`
   - Score each race, rank within race, dedupe, chunk, UPSERT to Neon
   - Record audit row in `finish_position_cron_executions`

### C. Feature Build (Subprocess: DuckDB + Layer Chain)

File: `apps/finish-position-predict-container/src/pipeline_runner.py`

**Subprocess call structure:**

```
python /app/pipeline/finish_position_features_duckdb.py \
  --category {jra|nar|ban-ei} \
  --target-date 20260617 \
  --days-ahead 0 \
  --pg-url postgresql://... \
  --output-dir /tmp/predict-upcoming/feat-{category}-base \
  --allow-empty-targets \
  [--realtime-odds /tmp/predict-upcoming/realtime-odds-{category}.parquet]
```

Then **sequential layer chain per category** (`LAYER_CHAIN` in `predict_lib/pipeline_args.py`):

**JRA (14 layers, 244 features):**

1. `add-race-internal-features.py` (pure DuckDB; reads base parquet)
2. `add-market-signal-features.py` (PG history scan)
3. `add-sectional-and-weight-features.py` (PG)
4. `add-futan-juryo-features.py` (PG)
5. `add-workout-features.py` (PG)
6. `add-near-miss-features.py` (PG)
7. `add-grade-race-lineage-features.py` (PG + lineage config)
8. `add-head-to-head-features.py` (PG)
9. `add-baba-pedigree-affinity-features.py` (PG)
10. `add-trainer-stable-affinity-features.py` (PG; jvd_se filter)
11. `add-pacestyle-features.py` (PG race_running_style_model_predictions; jra filter)
12. `add-course-numerical-features.py` (baked parquet lookup; no PG)
13. `add-relationship-r1-features.py` (PG; jra filter)
14. `add_kohan3f_going_features.py` (PG; JRA iter19 signal)

**NAR (8 layers, 192 features):**

1. race-internal (pure DuckDB)
2. near-miss (PG)
3. lineage (PG + config)
4. head-to-head (PG)
5. baba-pedigree (PG)
6. trainer (PG; nar filter)
7. pacestyle (PG; nar filter)
8. relationship (PG; nar filter)

**Ban-ei (5 layers, 111 features):**

1. lineage (PG + ban-ei config)
2. head-to-head (PG)
3. baba-pedigree (PG)
4. ban-ei futan-class (PG)
5. ban-ei grade-career (PG)

**Layer invocation:**

- Each layer reads prior parquet from stdin (input-dir), appends columns, writes to output-dir
- Layers with `--pg-url` sustain an ATTACH to Postgres for history scans (`--from-date 20100101`)
- Timeouts: implicit per-layer (no explicit limit; relies on Postgres query timeouts and container 90s reap)

### D. Realtime Odds Fetch (Parallel to Feature Build)

File: `apps/finish-position-predict-container/src/realtime_odds_fetcher.py`

**Before feature build starts:**

1. Query Neon for upcoming race keys: `SELECT DISTINCT keibajo_code, race_bango FROM ... WHERE finish_position IS NULL`
2. For each race key, fetch **two endpoints in parallel per race:**
   - `GET https://sync-realtime-data-hot.kkk4oru.com/api/odds/{raceKey}` (5s timeout)
   - `GET https://sync-realtime-data.kkk4oru.com/api/horse-weight/{raceKey}` (5s timeout)
3. Merge results by `(umaban)`, write parquet with columns:
   - `keibajo_code`, `race_bango`, `umaban`, `tansho_odds_realtime`, `ninkijun_realtime`, `bataiju_realtime`, `exotic_sanrenpuku_p3_realtime`
4. Return parquet path or `None` (graceful fallback if zero rows collected)

**Error handling:** Individual race fetch failures are logged and swallowed; builder COALESCEs to `nvd_se`/`jvd_se` fallback.

### E. Model Load & Inference

File: `apps/finish-position-predict-container/src/predict_upcoming.py` + model adapters

**Model artifacts:**

- Baked into image at `/models/finish-position/{category}/{modelVersion}/{model.json,metadata.json}`
- Per-class ensemble members (JRA 703): `/models/finish-position/jra/per-class/{kyoso_joken_code}/{memberVersion}/...`
- E-top2 XGB companion (STAGED): `/models/finish-position/jra/xgb-jra-2013-v8/model.json` (loaded if `JRA_ETOP2_ENABLED=True`)

**Production models (v8 iter as of 2026-06-18):**

- JRA: `iter20-jra-cb-2013-v8` (CatBoost, 244 features, train 2013–2022)
- NAR: `iter12-nar-xgb-hpo-v8` (XGBoost, 192 features, train TBD)
- Ban-ei: `banei-cb-v7-lineage-wf-21y` (CatBoost, 111 features, v7-lineage frozen)

**Per-race scoring:**

1. Load metadata (feature names) from JSON, assert count matches expected (244 JRA / 192 NAR / 111 Ban-ei)
2. Build feature matrix from parquet rows (per entry per horse)
3. Route to per-class model if registered (`kyoso_joken_code` JRA, `nar_subclass` NAR); else fallback to category-global
4. Score via CatBoost/XGBoost/LightGBM native predict (single-threaded per race; races scored serially)
5. Rank within race by predicted score (descending)
6. Apply E-top2 override if enabled (XGB#1 == CB#2 && class != 701 → promote CB#2 to rank 1)

### F. Prediction Row Building & Upsert

Files: `predict_lib/upsert_sql.py`, `predict_lib/upcoming.py`, `predict_lib/dedupe.py`

**Per-race output:**

- Race ID: `{source}:{kaisai_nen}:{kaisai_tsukihi}:{keibajo_code}:{race_bango}`
- For each horse entry (umaban):
  - Extract: predicted_score, predicted_rank, predicted_top1_prob, predicted_top3_prob, predicted_finish_position

**Chunked UPSERT:**

```sql
INSERT INTO race_finish_position_model_predictions (
  model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango,
  umaban, predicted_score, predicted_rank, predicted_top1_prob, predicted_top3_prob, predicted_finish_position
)
VALUES
  (...), (...), ...  -- 500 rows per chunk default
ON CONFLICT (model_version, source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango, ketto_toroku_bango)
DO UPDATE SET
  umaban = excluded.umaban,
  predicted_score = excluded.predicted_score,
  ... (all 6 columns updated)
```

- Batch size: 500 rows per UPSERT (to keep statement <16-min wall limit)
- Dedupe by primary key before insert (track {race_id, ketto_toroku_bango} pairs; warn on duplicates)
- Connection: `NEON_DATABASE_URL` (write target)

### G. Audit Recording

File: `predict_lib/audit.py`

**Per-run audit row:**

```sql
INSERT INTO finish_position_cron_executions (
  run_date, run_timestamp, status, races_predicted, duration_ms, error
) VALUES (...)
```

- `status`: "success" | "error"
- `races_predicted`: total predictions written
- `duration_ms`: wall-clock time from start to audit record
- `error`: error message if status="error" (or NULL)

---

## 3. Workload Profile & Timing Breakdown

### Typical NAR-Only Run (03:00 JST)

**Measured from 2026-06-17 03:00 run (514 NAR predictions):**

| Phase                                        | Duration                        | Notes                                                                                                                                                                                                                                                                                                        |
| -------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Pre-flight** (Colima, docker, credentials) | ~2 min                          | Includes docker image build if missing (~30s), image inspection (~30s), env parsing                                                                                                                                                                                                                          |
| **Realtime odds fetch**                      | ~5 s                            | 48 races × 2 endpoints (odds + weight), 5s timeout each; gracefully falls back to NULL                                                                                                                                                                                                                       |
| **Feature build (base DuckDB)**              | ~35 s                           | `finish_position_features_duckdb.py` for NAR; reads 2.2M rec + indexes, computes 514 target rows                                                                                                                                                                                                             |
| **Feature build (14 layers total)**          | ~120–180 s **[DOMINANT PHASE]** | Sequential layer scripts, each reads PG, outputs parquet; varies by layer (race-internal + market-signal ~0.4s, near-miss ~20–30s, lineage ~10s, h2h ~5s, baba-ped ~10s, trainer ~5s, pacestyle ~5s, relationship ~10s; iteration shows 35.5s for NAR base, then layer stack adds ~140s more for full chain) |
| **Model load**                               | ~2 s                            | Load CatBoost booster from JSON, metadata from JSON, verify feature count                                                                                                                                                                                                                                    |
| **Inference (scoring)**                      | ~10–20 s                        | Score 514 entries across ~50 races; per-race serial (each race ~0.2–0.4s to score 8–12 horses)                                                                                                                                                                                                               |
| **UPSERT (Neon)**                            | ~30–60 s                        | Chunk_rows(514, 500) → 2 chunks; each chunk UPSERT to Neon via `psycopg` (network RTT ~100–200ms per round-trip)                                                                                                                                                                                             |
| **Audit record**                             | ~2 s                            | Create table DDL if not exists, insert one audit row                                                                                                                                                                                                                                                         |
| **Total run duration**                       | **~3.5 min (210 s)**            | [35.5s base + ~140s layers + 20s score + 60s upsert + 10s overhead]                                                                                                                                                                                                                                          |

### Full Tri-Category Run (09:30 JST on Saturday)

**Measured from 2026-06-17 09:30 run (514 NAR + 0 JRA + 0 Ban-ei, skipped JRA & Ban-ei due to races=0):**

| Phase                        | Duration                    | Notes                                                                                                                           |
| ---------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| **JRA feature build**        | ~46 s                       | Base DuckDB (20.85s source + 13.74s indexes + 11.4s other layers) — no races, early-exit                                        |
| **NAR feature build**        | ~150–180 s                  | Full chain (35.5s base + 140s layers)                                                                                           |
| **Ban-ei feature build**     | ~3–5 s                      | Base only (no races)                                                                                                            |
| **Total (all 3 categories)** | **~10–12 min (sequential)** | If all three categories have races, runs serially; cumulative ~35s JRA + 150s NAR + 3s Ban-ei + model load/scoring per category |
|                              |                             | **Bottleneck: NAR layer chain (140s)** dominates; JRA full chain would add another ~200s if races exist                         |

### Scaling Characteristics

- **Races:** ~50 NAR per day; feature build scales ~O(target races \* history window)
- **Per-race scoring:** ~200 ms per race (8–12 horses each, feature matrix build + predict + rank)
- **Per-prediction UPSERT:** ~0.1 ms per horse (chunked batch, network RTT amortized across 500)
- **Layer cost dominance:** PG history scan (lineage, h2h, baba-ped, trainer, pacestyle, relationship) >> DuckDB base build. The near-miss layer alone sustains a complex join over all prior races (O(history) per target race).

### Why >90s (CF Container Reap Limit)?

1. **Layer chain is sequential:** Each layer reads input parquet, scans Postgres history, writes output parquet. No intra-layer parallelism. The 14 JRA layers add ~200s cumulative (including heavy pedigree + lineage scans).
2. **Postgres ATTACH + long-running query:** The DuckDB base build holds an ATTACH to Postgres for ~35s (reads 2.2M rec rows to build target index). Neon's idle timeout can fire if there's no query activity for >5 min, but the base build stays active.
3. **Per-category seqential:** JRA feature build, then NAR, then Ban-ei. No inter-category parallelism (would require multiple Postgres ATTACH contexts, not feasible in single DuckDB subprocess).
4. **Total: ~210 s (3.5 min) for NAR alone; 600+ s (10 min) for all three categories with races.** A CF Container reap at 90–110s would fail mid-layer-chain, leaving partial predictions in Neon (dangerous without atomic commit at subprocess level).

---

## 4. Data Sources & Access Patterns

### Primary: Local Colima PostgreSQL (SOURCE_DATABASE_URL)

- **Role:** Feature build Postgres (avoid Neon idle timeout)
- **Location:** `127.0.0.1:15432` in Colima container network
- **Credentials:** `horse_racing:horse_racing` (from wrapper script)
- **Tables read by feature pipeline:**
  - `rec` (2.2M rows) — historical race metadata
  - `jvd_se`, `nvd_se` — JRA/NAR race results (competitor stats)
  - `jvd_um`, `nvd_um` — JRA/NAR horse master (pedigree, trainer)
  - `jvd_ra`, `nvd_ra` — JRA/NAR race attributes
  - Lineage config tables (per-category)
  - Pedigree CTE (Cartesian-join heavy; causes disk spill in DuckDB ~0.42 chunks/min)
- **Data size:** ~2.5 GB total (loaded into DuckDB memory per category run)
- **Access pattern:** Full table scans + indexes (hist_from_date=20100101 for all categories)

### Secondary: Neon PostgreSQL (NEON_DATABASE_URL)

- **Role:** Write target for predictions; read source for upcoming race keys
- **Credentials:** Role-based auth (npg\_ prefix); SSL required
- **Tables:**
  - `race_finish_position_model_predictions` (write) — UPSERT predictions
  - `finish_position_cron_executions` (write) — audit log
  - `races` (read) — query upcoming races by date (SELECT ... WHERE finish_position IS NULL)
- **Access pattern:** Small reads (upcoming race keys), large writes (500–1000 predictions per chunk)
- **Performance:** ~100–200 ms RTT per UPSERT chunk from Mac → AWS Singapore Neon

### Tertiary: Cloudflare Workers (HTTP)

- **sync-realtime-data-hot:** Tansho odds + sanrenpuku
- **sync-realtime-data:** Horse bataiju (weight)
- **Endpoint:** `GET /api/odds/{raceKey}`, `GET /api/horse-weight/{raceKey}`
- **Timeout:** 5 s per request; on failure, graceful NULL fallback
- **Data:** Per-race, per-horse odds snapshots (latest 12+ h for JRA advance-odds)
- **Access pattern:** ~50–100 HTTP requests per category run (one odds + one weight per race), parallel-capable

### Quaternary: R2 (Optional; Running-Style Parquets)

- **Purpose:** Pre-computed running-style predictions (avoid re-computing from PG every run)
- **Condition:** Enabled if `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY` are set
- **Path:** `pc-keiba-features-archive/{date}/running-style.parquet`
- **Fallback:** If R2 unavailable, feature build falls back to PG ATTACH + compute
- **Data:** ~100 KB–5 MB per category per day

### Image-Baked: Model Artifacts & Lookups

- **Models:** `/models/finish-position/{category}/{modelVersion}/model.json`, `metadata.json`
- **Lookup tables:** `/app/lookups/course-numerical-features.parquet` (119 rows, baked)
- **Lineage configs:** `/app/pipeline/finish-position-features/lineage-races/{jra|nar|ban-ei}.json`
- **Feature scripts:** `/app/pipeline/finish-position-features/*.py` (14 layers)

---

## 5. Model Artifacts Inventory

### Production Models (Baked in Image)

| Category | Model Version              | Architecture | Feature Count | Train Window | File Size |
| -------- | -------------------------- | ------------ | ------------- | ------------ | --------- |
| JRA      | iter20-jra-cb-2013-v8      | CatBoost     | 244           | 2013–2022    | ~2–3 MB   |
| NAR      | iter12-nar-xgb-hpo-v8      | XGBoost      | 192           | TBD          | ~1–2 MB   |
| Ban-ei   | banei-cb-v7-lineage-wf-21y | CatBoost     | 111           | v7-lineage   | ~500 KB   |

### E-top2 Companion (STAGED)

- **File:** `/models/finish-position/jra/xgb-jra-2013-v8/model.json`
- **Architecture:** XGBoost
- **Feature Count:** 244 (identical to CB iter20)
- **Activation flag:** `JRA_ETOP2_ENABLED` in `predict_lib/model_meta.py` (default `False`)
- **Override:** When enabled, both models loaded; E-top2 logic fires when XGB#1 == CB#2 && class != 701

### Per-Class Ensemble Members (JRA Phase B-2 ENSEMBLE_ACTIVATION_703)

- **Registry:** `kyoso_joken_code=703` (Nihonjin 3 Win class)
- **Members:**
  1. `iter20-jra-cb-perclass-703-v8/model.json` (CatBoost single)
  2. `iter20-jra-cb-perclass-703-hpo-v8/model.json` (CatBoost HPO)
  3. `iter21-jra-cb-chain-703-v8/model.json` (CatBoost residual)
  4. `iter22-jra-cb-residual-703-v8/model.json` (CatBoost residual v2)
- **Ensemble:** `iter23-jra-cb-ensemble-703-v8/manifest.json` (weighted blend)
- **Routing:** `predict_lib/per_class.py` checks registry; if match, loads manifest + members; else fallback to category-global

### Loading Mechanics

1. **Startup:** Resolve `model_version_for(category)` → read metadata JSON → assert feature count
2. **Per-booster:** Load via adapter (`catboost_adapter.load_catboost_booster` / `xgboost_adapter.load_xgboost_booster`)
3. **Per-class:** Load manifest → iterate members → load each booster → pool.register
4. **Scoring:** Per-race class code lookup → routing decision → fetch booster from pool (or fallback)

---

## 6. Failure Modes & Constraints

### Known Constraints for CF Container Port

> **2026-06-19 更新**: CF Container への移行設計が `04-off-mac-migration-plan.md` に完成。
> 以下の制約評価は 2026-06-03 時点のもの。held request + `renewActivityTimeout()` 設計では
> per-category (~1–2.5 min) は問題なく収まると見込まれる。PHASE 2 pilot で実測確認後に確定。
> off-Mac migration の全体フローは `04-off-mac-migration-plan.md` §5 参照。

1. **90–110s reap timeout (歴史的制約):** 2026-06-03 の試行では `start()` + DO alarm keepalive で
   ~90 s に SIGTERM されたが、現在の CF docs は 10 m idle timeout + `renewActivityTimeout()` による
   explicit keepalive + 15 m graceful-stop を定義する。**held request + `renewActivityTimeout()` 設計に
   移行することで解消見込み。03-architecture-design.md §1.4 および 04-off-mac-migration-plan.md §2 参照。
   pilot 実測前は "partially superseded" として扱う。**
2. **Layer chain sequential:** Can't parallelize 14 JRA layers; each reads parquet, scans PG, writes next. **OPTIMIZATION GAP.**
3. **Postgres ATTACH sensitive to idle:** Neon's idle timeout can fire if layer pauses >5 min. The base build and early layers keep it active, but a stalled layer could trigger eviction. **MITIGATED: local Colima replica; CF Container would need to attach to Neon directly (SSL, slow).**
4. **Per-category sequential:** Feature build runs JRA then NAR then Ban-ei. No inter-category parallelism. **OPTIMIZATION GAP.**
5. **Single-threaded scoring:** Races are scored serially within a category. **ACCEPTABLE: scoring is <5% of runtime; layer chain dominates.**

### Graceful Fallbacks (Robustness)

- **Realtime odds:** HTTP failure → logs warning → DuckDB builder COALESCEs to `nvd_se`/`jvd_se` NULL
- **Running-style R2:** Not available → falls back to PG compute
- **Per-class model missing:** Registry lookup fails → falls back to category-global booster
- **Race category with zero races:** Feature build returns early with zero rows; scorer skips; audit logs races_predicted=0
- **Neon UPSERT timeout:** Caught, logged, re-attempted; audit records error if all chunks fail

### Architectural Assumptions (Tight Coupling)

1. **Neon == Postgres:** Feature build hard-codes `psycopg` protocol; native PG. Cloudflare D1 (SQLite) incompatible; would require SQL rewrite + schema changes.
2. **Local Colima for feature build:** Assumed to be lower-latency & idle-timeout-free compared to Neon. True for NAR/Ban-ei but JRA layer chain is still ~200s cumulative.
3. **Model JSON + metadata.json:** Format is vendor-specific (CatBoost JSON serialization, XGBoost JSON RFC-strict). Changes to model format require wrapper updates.
4. **Feature scripts version-locked:** Layer chain order + flags are hard-baked in `predict_lib/pipeline_args.LAYER_CHAIN`. Adding/removing a layer requires code change + test.

---

## 7. Recommendation Summary

### Key Findings

1. **Dominant bottleneck:** Feature build layer chain (~140 s NAR, ~200 s JRA cumulative). Not the scoring, not the UPSERT — the PG history scans and parquet rewrites.
2. **Total workload:** 210–600 s depending on category. The 2026-06-03 monolithic CF Container attempt appeared incompatible with a ~90–110 s idle reap. This was the historical reason for the temporary macOS launchd fallback, not current production authority.
3. **Data path:** Feature build reads **all historical races (2.2M rows) from PG per category run**. No change to that volume without rewriting DuckDB base build (not recommended; it's the viewer's reused code).
4. **Scaling:** Per-race time is O(1); total time is O(layer_chain_cost), not O(races). Adding more races has minimal impact; the bottleneck is the PG scans, not the output size.

### Porting Options to CF Container (Feasibility)

**Option A: Speed up the layer chain (in-place optimization)**

- Parallelize intra-layer: not feasible (DuckDB ATTACH is single-threaded per context)
- Parallelize inter-layer: requires spawning multiple DuckDB processes → complex inter-process parquet handoff
- Pre-compute historical aggregates in PG: requires schema changes to store pre-aggregated stats
- **Effort: HIGH; Payoff: Maybe 30–50% reduction; still ~100+ s**

**Option B: Migrate to query-only (no subprocess layer chain)**

- Embed all feature logic into a single big Postgres query (or series of CTEs)
- Eliminate DuckDB entirely; return results as JSON directly to Python
- **Effort: EXTREME (rewrite 2000+ lines of DuckDB/SQL); Payoff: Possible 2x speedup; Still Neon-dependent (slow)**

**Option C: Pre-compute features offline & serve from feature store**

- Daily cron (outside prediction window) builds & caches features for tomorrow's races
- Predictor reads cache (Neon table or R2 Parquet), skips feature build
- **Effort: MEDIUM (refactor feature build lifecycle); Payoff: Prediction runtime ~20 s (model load + score + upsert); Compatible with CF Container**

**Option D: Accept 10–12 min runtime; upgrade CF Container resource tier**

- Cloudflare Containers have no published per-tier timeout adjustment (reap is global)
- **Effort: BLOCKED (no control over reap timeout; Cloudflare infrastructure constraint)**

**Option E (historical, rejected/superseded): Keep macOS launchd; port orchestration logic to CF Worker (observer only)**

- macOS continues to run Docker (proven, 3.5 min); CF Worker polls for completion & logs
- **Effort: LOW (adds CF Worker wrapper, no substantive change to prediction logic); Payoff: Observability, audit trail in D1**

### 2026-06-19 Off-Mac Migration Update

**off-Mac migration 設計が完成した。** `04-off-mac-migration-plan.md` に以下を含む:

- Neon pre-wake cron (JST 02:52 / 09:22) を先行 deploy する設計
- held `/predict` + `renewActivityTimeout()` による reap-safe Container 実行
- Neon 直 feature-build cost 対策の評価(結論: cost 受容 + pre-wake で latency 最適化)
- PHASE 1 (pre-wake) → PHASE 2 (pilot) → PHASE 3 (dual-run) → PHASE 4 (cutover) の go/no-go gate
- 当時の計画では Mac launchd を cutover 確認まで authority として維持する方針だったが、現在は Cloudflare-only production に supersede 済み

### Recommendation (2026-06-03 時点)

**For CF Container porting in a future phase:** Pursue **Option C (pre-compute features offline)** combined with **Option A (targeted layer speedups)**. Neither alone hits the 90s mark, but together they could:

1. Move feature pre-computation to an off-peak window (e.g., 02:00–02:30 JST) on a cron-triggered compute instance (cheaper than CF Container for long-running work)
2. Parallelize inter-category feature builds (spawn 3 DuckDB processes in parallel if resource-constrained)
3. Reduce prediction-time logic to model load (~2s) + score (~10s) + upsert (~20s) = **~35 s per full run**, well under 90s

This recommendation is superseded. Current production authority is Cloudflare
Cron / Queue / Worker / Container; macOS launchd is deprecated local/manual
tooling only.

---

## Appendix: File Locations & Key Paths

| Component                    | Location                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------- |
| Wrapper script               | `scripts/launchd/finish-position-predict-daily.sh`                           |
| LaunchAgent plist            | `scripts/launchd/com.kkk4oru.finish-position-predict.plist`                  |
| Container Dockerfile         | `apps/finish-position-predict-container/Dockerfile`                          |
| Predictor entrypoint         | `apps/finish-position-predict-container/src/predict_upcoming.py`             |
| Pipeline orchestration       | `apps/finish-position-predict-container/src/pipeline_runner.py`              |
| Feature args builder         | `apps/finish-position-predict-container/src/predict_lib/pipeline_args.py`    |
| Model metadata               | `apps/finish-position-predict-container/src/predict_lib/model_meta.py`       |
| Realtime odds fetcher        | `apps/finish-position-predict-container/src/realtime_odds_fetcher.py`        |
| UPSERT logic                 | `apps/finish-position-predict-container/src/predict_lib/upsert_sql.py`       |
| Feature build scripts (base) | `apps/pc-keiba-viewer/src/scripts/finish_position_features_duckdb.py`        |
| Feature layer chain          | `apps/pc-keiba-viewer/src/scripts/finish-position-features/*.py` (14 layers) |
| Models (baked)               | `apps/finish-position-predict-container/models/finish-position/`             |
| Logs                         | `/Users/kkk4oru/Library/Logs/finish-position-predict/*.log`                  |
| Lock file                    | `/tmp/finish-position-predict.lock`                                          |
| Work dir                     | `/tmp/predict-upcoming/`                                                     |
