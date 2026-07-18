// Run with bun. Type definitions for the finish-position-cron Worker.

import type { Container } from "@cloudflare/containers";
import type { PredictRunCoordinator } from "./predict-run-coordinator";

export type PredictCategory = "jra" | "nar" | "ban-ei";

// "full" = full DuckDB feature build + score + write features to R2 cache.
// "rescore" = read cached features from R2 + latest odds + re-score only (no 21y Neon scan).
export type PredictMode = "full" | "rescore";

export interface CatalogServiceBinding {
  fetch(request: Request): Promise<Response>;
}

export interface Env {
  FINISH_POSITION_PREDICT_CONTAINER: DurableObjectNamespace<Container<Env>>;
  FINISH_POSITION_CRON_DB: D1Database;
  // Read-only D1 binding to the sync-realtime-data DB. The per-race coordinator
  // reads realtime_race_sources.race_start_at_jst (JST ISO post-time) from here
  // to gate which races are within their T-X rescore window. Same source the
  // running-style coordinator and deprecated local guard use; finish-position
  // only ever SELECTs from it.
  REALTIME_DB: D1Database;
  NEON_DATABASE_URL: string;
  // Backward window (days) the corner-features-refresh.ts evening/morning
  // crons additionally sweep, so a day whose settlement columns were still
  // NULL on a prior visit gets re-upserted instead of staying permanently
  // stuck (docs/probes/corner-features-settlement-backfill-heal-2026-07-17.md).
  // Optional so existing callers/tests need not set it; unset/unparseable
  // falls back to 0 (no backward window, matching the pre-lookback behavior).
  CORNER_FEATURES_LOOKBACK_DAYS?: string;
  PC_KEIBA_R2_CATALOG?: CatalogServiceBinding;
  // Read/source connection for the heavy DuckDB feature build. Production sets
  // r2-catalog://pc-keiba; an unset value is forwarded empty and the container
  // fails closed instead of falling back to Neon.
  SOURCE_DATABASE_URL?: string;
  PREDICT_DAYS_AHEAD: string;
  TRIGGER_TOKEN: string;
  // Feature flag for the per-race rescore coordinator. "1" enables enqueueing;
  // any other value (including unset) keeps it in shadow — the cron still fires
  // but enqueues nothing, so deploying the coordinator does not change
  // production predictions. Optional so existing callers/tests need not set it.
  COORDINATOR_ENABLED?: string;
  // Comma-separated PredictCategory list gating which categories the per-race
  // coordinator enqueues rescore for (e.g. "jra" or "jra,nar"). Unset/empty or
  // a list with no recognized category falls back to JRA-only — NAR/Ban-ei
  // rescore stays off the container per-race path so it does not contend with
  // the morning full-pass container slot for those categories. See
  // race-coordinator.ts resolveRescoreCategories. Optional so existing
  // callers/tests need not set it.
  RESCORE_CATEGORIES?: string;
  // Feature flag for event-driven per-race rescore requests from
  // sync-realtime-data. "1" enables the internal rescore endpoint; any other
  // value accepts the request as a no-op so full generation can drain first.
  RESCORE_ENABLED?: string;
  // Feature flag forwarded into the container env: unset enables the NAR
  // clean Set-Transformer x ensemble score-z blend (iter40); "0", "false", or
  // "off" rolls the container back to the pure iter12 clean188 base. Set via
  // `wrangler secret put NAR_TRANSFORMER_BLEND_ENABLED`. Optional so existing
  // callers/tests need not set it.
  NAR_TRANSFORMER_BLEND_ENABLED?: string;
  // Feature flag forwarded into the container env: gates whether the Python
  // container's day-base feature split path (day-stable layers cached once
  // per category per day via GET /prewarm-day-base, reused by per-race full
  // builds) is active. A comma-separated PredictCategory allowlist (e.g.
  // "jra" or "jra,nar"); empty/unset disables the split for every category.
  // The Worker does not interpret the value itself -- it only forwards it
  // into the container env (see container-class.ts); the Python side reads
  // it via is_day_base_split_enabled(category). Set via
  // `wrangler secret put DAY_BASE_SPLIT_ENABLED`. Optional so existing
  // callers/tests need not set it.
  DAY_BASE_SPLIT_ENABLED?: string;
  // KV namespace (id: d984fba531804927ac1b551200d4b3cb) is orphaned — binding removed.
  // DO-backed strong-consistency coordinator replaces KV for run dedup/state.
  PREDICT_RUN_COORDINATOR: DurableObjectNamespace<PredictRunCoordinator>;
  PREDICT_QUEUE: Queue<PredictQueueMessage>;
  // R2 binding for per-run feature parquet cache (full→put, rescore→get).
  FEATURES_CACHE: R2Bucket;
  // R2 S3 credentials forwarded into the container env so the Python rescore path
  // (predict_upcoming.py::_load_r2_config) can GET the cached feature parquet via
  // S3 SigV4. R2_ACCOUNT_ID / R2_ACCESS_KEY_ID / R2_SECRET_ACCESS_KEY are Worker
  // secrets; R2_BUCKET is a plain var. All optional — _load_r2_config treats an
  // absent/empty value as "skip R2".
  R2_ACCOUNT_ID?: string;
  R2_ACCESS_KEY_ID?: string;
  R2_SECRET_ACCESS_KEY?: string;
  R2_BUCKET?: string;
  // Read-only Cloudflare Iceberg REST Catalog credentials for the heavy
  // feature-build source. The token is a Worker secret; URI/warehouse are
  // plain vars. Web display and prediction output can still use Neon, but the
  // batch input path must resolve through this catalog.
  R2_CATALOG_TOKEN?: string;
  R2_CATALOG_URI?: string;
  R2_CATALOG_WAREHOUSE?: string;
  // venue-weather Worker base URL forwarded into the container env so the Python
  // prediction path can fetch venue weather data over HTTP. Plain var; optional
  // so existing callers/tests need not set it.
  VENUE_WEATHER_URL?: string;
  // Feature flag (USER decision 11, docs/finish-position-prediction-system.md
  // §5.4): "1" switches every Container DO name lookup for a race-scoped
  // predict request (queue-consumer.ts, focused-full-cache-pickup.ts,
  // worker.ts's admin run-focused-full-race) from one DO per category to a
  // hash-sharded DO per category (see predict-do-shard.ts), so races spread
  // across independent container filesystems instead of all serializing
  // through the one process-wide pipeline slot each category DO holds today.
  // Unset/any other value keeps the pre-sharding `predict-{category}` name --
  // the instant-rollback path. Optional so existing callers/tests need not
  // set it.
  RACE_SHARDED_DO?: string;
  // Shard count per category when RACE_SHARDED_DO is enabled, parsed as a
  // positive integer; unset/non-positive/non-integer falls back to 3 inside
  // predict-do-shard.ts. wrangler.jsonc's FinishPositionPredictContainer caps
  // at max_instances: 10 shared across jra/nar/ban-ei, so 3 keeps the
  // worst-case fully-active 3-category x N-shard total (3x3=9) under that
  // limit -- raising it must be checked against max_instances before deploy.
  // Optional so existing callers/tests need not set it.
  RACE_SHARD_MAX_CONCURRENT?: string;
}

export type CronAuditStatus = "started" | "success" | "error";

export interface CronAuditRecord {
  runDate: string;
  status: CronAuditStatus;
  racesPredicted: number;
  durationMs: number;
  error: string | null;
}

export interface PredictStartOptions {
  entrypoint: string[];
  envVars: Record<string, string>;
  enableInternet: boolean;
}

export interface RunDates {
  runDate: string;
  runYmd: string;
}

export interface PredictQueueMessage {
  runDate: string;
  runDateIso: string;
  runYmd: string;
  category: PredictCategory;
  daysAhead: number;
  mode: PredictMode;
  // Per-race targeting. keibajoCode/raceBango are 2-digit zero-padded strings
  // matching realtime_race_sources. Absent on the legacy per-category messages,
  // so the existing consumer is unaffected.
  keibajoCode?: string;
  raceBango?: string;
  // Backward-compatible field for older queued messages. Focused per-race full
  // builds intentionally ignore it and use the stable race-scoped DO name.
  requestId?: string;
  // Gates event-driven full-build bypasses: when sync-realtime-data finishes
  // running-style, it can trigger POST /run with skipDedup=true so the queue
  // consumer skips the per-category claimRun dedup gate. Absent/false keeps the
  // normal dedup path.
  skipDedup?: boolean;
  // Number of times this focused per-race full message has been re-enqueued
  // because the container's single per-process pipeline slot was busy with a
  // DIFFERENT race. Each busy re-enqueue creates a fresh message (resetting the
  // Cloudflare retry attempt count) with this incremented, bounded by
  // MAX_BUSY_REQUEUES in queue-consumer.ts. Absent on the first send.
  busyRequeueCount?: number;
  // Number of times this message has been re-enqueued by dlq-consumer.ts
  // after landing in the dead-letter queue (finish-position-predict-dlq),
  // having exhausted the primary queue's max_retries. Bounded by
  // MAX_DLQ_REDRIVES in dlq-consumer.ts so a poison-pill message is redriven
  // at most once instead of bouncing between the two queues forever. Absent
  // on every message that has not yet been dead-lettered.
  dlqRedriveCount?: number;
  // Enables verbose diagnostic logs for this message and the downstream
  // Container /predict request. Absent/false keeps production logs quiet.
  debug?: boolean;
  // Explicit operator bypass for three independent guards: (1) the old-date
  // guard (old-date-guard.ts) -- skips the runYmd staleness check entirely;
  // (2) the focused-full completion guard (ackIfFocusedFullAlreadyComplete,
  // backed by isFocusedFullPredictionComplete in
  // focused-full-completion.ts) -- skips the "Neon already has every row
  // for this model_version" short-circuit; (3) the DO-backed
  // claimFocusedFullRace terminal-status gate (predict-run-coordinator.ts)
  // -- lets the claim through even when a prior attempt for this exact
  // race already reached status:"success" in Durable Object storage, which
  // otherwise blocks every future claim for that (runYmd, category,
  // keibajoCode, raceBango) key permanently (see
  // apps/pc-keiba-viewer/docs/probes/jra-serving-audit-jun-jul-2026-07-17.md
  // Defect F). Guards (2) and (3) can both reach "complete"/"success" from
  // a row-count-only check, so neither can tell a genuinely finished race
  // from one whose rows are present but wrong (e.g. the 2026-07-12
  // degenerate-score batch -- see focused-full-completion.test.ts's
  // "documented limitation" test); force lets an operator deliberately
  // re-trigger such a race so the normal full-build pipeline runs and its
  // per-(model_version, horse) UPSERT overwrites the bad row in place. All
  // three bypasses dispatch to the Container as normal. Absent/false keeps
  // all three guards active.
  force?: boolean;
}

export interface PredictRunState {
  status: "started" | "success" | "error";
  startedAt: string;
  racesPredicted?: number;
  error?: string;
}
