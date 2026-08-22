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
  // Shared with pc-keiba-viewer / sync-realtime-data. Optional so existing
  // callers/tests need not set it -- publishFinishPositionPredictionCache
  // no-ops with skipped-no-kv when the binding is absent.
  DETAIL_SECTION_CACHE_KV?: KVNamespace;
  // Same secret value as sync-realtime-data's PC_KEIBA_VIEWER_INTERNAL_TOKEN
  // (viewer's PC_KEIBA_INTERNAL_TOKEN). Used to POST
  // /api/internal/prediction-cache-bust after a weight-rescore KV overwrite.
  // Set via `wrangler secret put PC_KEIBA_VIEWER_INTERNAL_TOKEN`. Optional so
  // existing callers/tests need not set it -- bust is skipped when unset.
  PC_KEIBA_VIEWER_INTERNAL_TOKEN?: string;
  // Optional viewer origin override for prediction-cache-bust. Unset/blank
  // falls back to https://pc-keiba-viewer.kkk4oru.com.
  PC_KEIBA_VIEWER_ORIGIN?: string;
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
  // Bearer token for sync-realtime-data's POST /api/jobs endpoint, used by
  // running-style-kick.ts to enqueue plan-running-style-predictions for the
  // JST windows sync-realtime-data's own native running-style crons do not
  // cover. Same secret value scripts/launchd/race-prediction-guard.sh already
  // reads from apps/sync-realtime-data/.dev.vars locally; set here via
  // `wrangler secret put REALTIME_ADMIN_TOKEN`. Optional so existing
  // callers/tests need not set it -- an unset value fails the kick's bearer
  // auth against sync-realtime-data (logged, never thrown).
  REALTIME_ADMIN_TOKEN?: string;
  // Legacy/manual feature flag for the per-race time coordinator. Production
  // has no coordinator cron: the second prediction is exclusively triggered
  // after sync-realtime-data successfully persists horse weights.
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
  // Default-off rollout flag for the Stage-1 freshness-gate repair. Exactly
  // "1" lets the Python gate accept the canonical tansho_odds column that
  // survives near-miss projection when canonical tansho_ninkijun is absent.
  // Any other/unset value preserves the current rank-only gate for immediate
  // rollback. The Worker only forwards this value into the container.
  STAGE1_PRESERVED_ODDS_GATE_ENABLED?: string;
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
  PREDICT_QUEUE: Queue<PredictQueueBody>;
  CONTAINER_CONTROL_QUEUE?: Queue<ContainerControlMessage>;
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
  // predict-do-shard.ts. Applies to focused-full only -- per-race rescore
  // always uses the unsharded predict-{category} DO so five JRA/NAR shards
  // cannot pack max_instances. wrangler.jsonc's FinishPositionPredictContainer
  // ceiling is 12; container-slot-cap.ts keeps general starts <= 10 and
  // reserves 2 for Ban-ei focused-full / day-base. Raising this must be
  // checked against both that software cap and max_instances before deploy.
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

export interface DeliveryCanaryMessage {
  type: "delivery-canary";
  id: string;
  enqueuedAt: string;
}

export interface DayBasePickupMessage {
  type: "day-base-pickup";
  category: PredictCategory;
  runYmd: string;
  attempt: number;
  // True only when the source-day running-style barrier requested the first
  // prediction pass. Scheduled prewarms leave this false/absent and only warm
  // the artifact; the pickup fans out after it proves the fresh object landed.
  generatePredictionsAfterHit?: boolean;
}

export interface DayBasePrewarmMessage {
  type: "day-base-prewarm";
  category: PredictCategory;
  runYmd: string;
  daysAhead: number;
  requestedAt: string;
  generatePredictionsAfterHit?: boolean;
}

export interface ContainerControlMessage {
  type: "container-stop";
  force?: boolean;
  name: string;
  requestedAt: string;
  workKey?: string;
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
  // Scheduled post time from realtime_race_sources. Day-base fanout includes
  // it so downstream queue ordering can prioritize imminent races. Optional
  // for backward compatibility with manual/admin and already queued messages.
  raceStartAtJst?: string;
  // Backward-compatible field for older queued messages. Focused per-race full
  // builds intentionally ignore it and use the stable race-scoped DO name.
  requestId?: string;
  // Gates event-driven full-build bypasses. After the fresh day-base HIT is
  // proven, the fanout uses skipDedup=true so each race enters the focused-full
  // coordinator instead of the legacy per-category claim. Absent/false keeps
  // the normal dedup path.
  skipDedup?: boolean;
  // Durable lifecycle ID for self-heal detection -> enqueue -> consume ->
  // prediction completion accounting. Absent on unrelated legacy messages.
  deliveryTrackingId?: string;
  // Number of times this focused per-race full message has been re-enqueued
  // because the container's single per-process pipeline slot was busy with a
  // DIFFERENT race. Each busy re-enqueue creates a fresh message (resetting the
  // Cloudflare retry attempt count) with this incremented, bounded by
  // MAX_BUSY_REQUEUES in queue-consumer.ts. Absent on the first send.
  busyRequeueCount?: number;
  // First time a horse-weight rescore was deferred behind its initial full
  // prediction/cache. Preserved across fresh Queue messages so deferral stays
  // cheap and bounded without falling into immediate retry + DLQ storms.
  rescoreDeferredAt?: string;
  // Number of times this message has been re-enqueued by dlq-consumer.ts
  // after landing in the dead-letter queue (finish-position-predict-dlq),
  // having exhausted the primary queue's max_retries. Bounded by
  // MAX_DLQ_REDRIVES in dlq-consumer.ts so a poison-pill message is redriven
  // at most once instead of bouncing between the two queues forever. Absent
  // on every message that has not yet been dead-lettered.
  dlqRedriveCount?: number;
  // Last predict/container failure snapshot. Cloudflare Queues
  // message.retry() cannot mutate the original body, so the primary consumer
  // normally persists this to finish_position_predict_retry_errors instead.
  // Present when a fresh send() (busy requeue / DLQ redrive) carries the
  // snapshot forward so the DLQ consumer can copy it without a D1 lookup.
  lastFailure?: {
    errorName?: string | null;
    errorMessage?: string | null;
    errorStack?: string | null;
    httpStatus?: number | null;
    httpBodyExcerpt?: string | null;
  };
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

export type PredictQueueBody =
  | PredictQueueMessage
  | DeliveryCanaryMessage
  | DayBasePickupMessage
  | DayBasePrewarmMessage;

export interface PredictRunState {
  status: "started" | "success" | "error";
  startedAt: string;
  racesPredicted?: number;
  error?: string;
}
