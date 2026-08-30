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

export interface RunningStylePlanJobMessage {
  date: string;
  type: "plan-running-style-predictions";
}

export interface Env {
  FINISH_POSITION_PREDICT_CONTAINER: DurableObjectNamespace<Container<Env>>;
  // Default-off canary binding for focused race-chain work. It is optional in
  // the type so local/test environments and a partially rolled-back Worker
  // fail closed to FINISH_POSITION_PREDICT_CONTAINER.
  FINISH_POSITION_RACE_CHAIN_CONTAINER?: DurableObjectNamespace<Container<Env>>;
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
  // Production prediction cache busts use this direct Worker-to-Worker route
  // so Cloudflare Access on the public viewer hostname cannot reject them.
  // Optional to preserve the public-origin fallback in local/test runtimes.
  PC_KEIBA_VIEWER?: { fetch: typeof fetch };
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
  // Direct Cloudflare service binding for live odds warm-up. Avoids a public
  // hostname/WAF round trip before materializing the market-signal R2 artifact.
  REALTIME_HOT?: { fetch: typeof fetch };
  // Bearer credential for the catalog's fresh race-entry attestation endpoint.
  // Rescore retries fail closed before Container dispatch when this is absent.
  FINISH_POSITION_ATTESTATION_TOKEN?: string;
  // Read/source connection for the heavy DuckDB feature build. Production sets
  // r2-catalog://pc-keiba; an unset value is forwarded empty and the container
  // fails closed instead of falling back to Neon.
  SOURCE_DATABASE_URL?: string;
  PREDICT_DAYS_AHEAD: string;
  PIPELINE_TOTAL_TIMEOUT_SECONDS?: string;
  TRIGGER_TOKEN: string;
  // Direct producer route to sync-realtime-data's general job consumer.
  // This avoids public HTTP, Access, and bearer-token failure modes. Optional
  // only for rolling-deploy and local fixture compatibility; the kick fails
  // closed when it is absent.
  RUNNING_STYLE_PLAN_JOBS?: Queue<RunningStylePlanJobMessage>;
  // Deprecated credential retained in the environment type during migration.
  // running-style-kick.ts never reads it after direct Queue delivery lands.
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
  // "1" attempts JRA horse-weight rescore inside the Worker from the final
  // per-race feature cache. Any miss or scoring/write failure falls back to
  // the existing Container path, making this an instant config rollback.
  JRA_WORKER_RESCORE_ENABLED?: string;
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
  // Default-off race-chain Container canary. Both the exact "1" flag and a
  // comma-separated category allowlist entry are required. Routing also
  // requires a metadata-bearing R2 day-base object at dispatch time.
  RACE_CHAIN_CONTAINER_ENABLED?: string;
  RACE_CHAIN_CONTAINER_CATEGORIES?: string;
  // Default-off Worker producer for the attested per-race market-signal
  // foundation. A miss keeps the legacy Container layer active.
  WORKER_MARKET_SIGNAL_FOUNDATION_ENABLED?: string;
  // Run remaining race-chain layer entrypoints in one killable Python/DuckDB
  // process. Unset keeps the legacy one-subprocess-per-layer fallback.
  RACE_CHAIN_FUSED_ENABLED?: string;
  // Short successful-run idle grace that lets the next same-shard race reuse
  // the warm process. Delayed stop remains workKey-fenced and is rejected once
  // a new owner claims the slot.
  CONTAINER_REUSE_IDLE_SECONDS?: string;
  // Default-off rollout gate for Durable Object scheduled completion watches.
  // Exactly "1" lets a focused-full accepted response acknowledge its source
  // Queue delivery after the Container DO durably registers the watch.
  FOCUSED_FULL_WATCH_ENABLED?: string;
  // Public Worker endpoint used only with a per-watch HMAC-signed callback URL.
  // The Container sends a terminal nudge; the Worker still polls authoritative
  // Container status before finalizing, and the delayed watch remains fallback.
  FOCUSED_FULL_CALLBACK_URL?: string;
  // KV namespace (id: d984fba531804927ac1b551200d4b3cb) is orphaned — binding removed.
  // DO-backed strong-consistency coordinator replaces KV for run dedup/state.
  PREDICT_RUN_COORDINATOR: DurableObjectNamespace<PredictRunCoordinator>;
  PREDICT_QUEUE: Queue<PredictQueueBody>;
  // Terminal-only lane used by Container DO completion watches. Optional so a
  // rolling deploy without the new binding keeps the legacy Queue polling path.
  FOCUSED_FULL_COMPLETION_QUEUE?: Queue<FocusedFullCompletionMessage | FocusedFullWatchTickMessage>;
  // Dedicated low-latency lane for the one post-weight rescore pass. Optional
  // during rolling deploys; the internal weight endpoint falls back to the
  // primary queue until this binding is present.
  WEIGHT_RESCORE_QUEUE?: Queue<PredictQueueMessage>;
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
  // Stable for every pickup/redelivery belonging to one logical prewarm.
  // Missing only on messages queued before generation-scoped ownership.
  generationId?: string;
  // True only when the source-day running-style barrier requested the first
  // prediction pass. Scheduled prewarms leave this false/absent and only warm
  // the artifact; the pickup fans out after it proves the fresh object landed.
  generatePredictionsAfterHit?: boolean;
  // Set only by an authenticated admin historical prewarm. Automatic work
  // never sets this and is discarded after its JST race day.
  force?: boolean;
}

export interface DayBasePrewarmMessage {
  type: "day-base-prewarm";
  category: PredictCategory;
  runYmd: string;
  daysAhead: number;
  requestedAt: string;
  // Created once by the producer and retained by every Queue redelivery.
  // Missing only on messages queued before generation-scoped ownership.
  generationId?: string;
  generatePredictionsAfterHit?: boolean;
  force?: boolean;
}

export interface ContainerControlMessage {
  type: "container-stop";
  // A lifecycle may atomically transfer a Container lease between a small,
  // explicit set of owners before its terminal stop is consumed. The stop
  // consumer accepts exactly one currently-active owner from this list and
  // still destroys the DO only once.
  acceptableWorkKeys?: string[];
  // A reusable Container releases its capacity lease before the delayed idle
  // stop. The stop may proceed only while no newer owner has claimed the DO.
  allowUnowned?: boolean;
  force?: boolean;
  name: string;
  requestedAt: string;
  // Missing means the legacy binding for already-queued control messages.
  role?: "legacy" | "race-chain";
  workKey?: string;
}

export interface ContainerCleanupMessage {
  type: "container-cleanup";
  acceptableWorkKeys?: string[];
  attempt: number;
  name: string;
  role: "legacy" | "race-chain";
  workKey: string;
}

export interface PredictionCacheRepairMessage {
  type: "prediction-cache-repair";
  category: PredictCategory;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

export type FocusedFullWatchOutcome = "error" | "missing" | "success" | "timeout";

export interface FocusedFullWatchPayload {
  body: PredictQueueMessage;
  doName: string;
  role: "legacy" | "race-chain";
  // Stable for every redelivery of one logical Queue generation. The Worker
  // derives this from workKey + source message id before calling Container.
  watchId: string;
  workKey: string;
}

export interface FocusedFullCompletionMessage extends FocusedFullWatchPayload {
  type: "focused-full-completion";
  error?: string;
  outcome: FocusedFullWatchOutcome;
}

export interface FocusedFullWatchTickMessage extends FocusedFullWatchPayload {
  type: "focused-full-watch-tick";
  deadlineAtMs: number;
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
  // Immutable identity of the horse-weight snapshot that caused this rescore.
  // Required by event-driven rescore producers and preserved across every
  // Queue retry/redrive so a newer or older non-empty snapshot cannot be
  // mistaken for the requested post-weight generation.
  weightSnapshotCount?: number;
  weightSnapshotFetchedAt?: string;
  weightSnapshotHash?: string;
  activeHorseNumbers?: number[];
  excludedHorseNumbers?: number[];
  entrySnapshotFetchedAt?: string;
  entrySnapshotHash?: string;
  // Backward-compatible field for older queued messages. Focused per-race full
  // builds intentionally ignore it and use the stable race-scoped DO name.
  requestId?: string;
  // Gates event-driven full-build bypasses. After the fresh day-base HIT is
  // proven, the fanout uses skipDedup=true so each race enters the focused-full
  // coordinator instead of the legacy per-category claim. Absent/false keeps
  // the normal dedup path.
  skipDedup?: boolean;
  // Set only after the lightweight race-chain Container reports
  // DAY_BASE_REQUIRED. Queue retries cannot mutate a body, so the consumer
  // sends one replacement carrying this durable fallback marker. Once set,
  // routing must stay on the day-base-capable legacy binding.
  forceLegacyContainer?: boolean;
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
  // Stable lower bound for a forced generation's completion check. Queue
  // redeliveries must only accept rows written after this request, otherwise
  // pre-existing same-day rows can stop the detached Container prematurely.
  forceRequestedAt?: string;
}

export type PredictQueueBody =
  | PredictQueueMessage
  | DeliveryCanaryMessage
  | DayBasePickupMessage
  | DayBasePrewarmMessage
  | ContainerCleanupMessage
  | FocusedFullCompletionMessage
  | FocusedFullWatchTickMessage
  | PredictionCacheRepairMessage;

export interface PredictRunState {
  status: "started" | "success" | "error";
  startedAt: string;
  racesPredicted?: number;
  error?: string;
}
