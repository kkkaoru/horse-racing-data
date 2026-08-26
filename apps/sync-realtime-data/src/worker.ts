import {
  BABA_CODE_TO_LOCAL_KEIBAJO,
  buildRaceListUrl,
  buildRaceResultUrl,
  buildRaceKey,
  extractOddsLinks,
  FetchStatusError,
  fetchRaceLinksFromRaceList,
  fetchRaceListPageHtml,
  fetchRacePage,
  fetchTodayRaceListUrls,
  isRaceResultDisabledOnRaceList,
  parseRaceMetadata,
  parseRaceEntries,
  parseHorseWeights,
  parseRaceEntryHorseNumbers,
  parseRaceResultExcludedHorseNumbers,
  parseRaceResults,
  parseRaceResultHorseWeights,
  parseRaceResultTanshoOdds,
  type KeibaGoRaceLink,
  type RaceResultTanshoOddsRow,
} from "./keiba-go";
import {
  FETCH_LOG_SUCCESS,
  PLAN_RESULT_FETCHES_SUMMARY_STATUS,
  SKIP_STATUS,
} from "./fetchLogStatuses";
import { formatError, formatErrorLogLine } from "./format-error";
import { QUEUE_HANDLER_TIMEOUT_MS, withHandlerTimeout } from "./handler-timeout";
import { mergeJsonHeaders } from "./http";
import {
  buildJraEntryUrlFromRace,
  buildJraResultUrlFromRaceSource,
  fetchJraResultHtmlWithFallback,
  isJraScratchStatus,
  parseJraRaceResultExcludedHorseNumbers,
  parseJraRaceResults,
  parseJraHorseWeights,
  parseJraRaceEntries,
  sanitizeJraRaceEntriesWithOdds,
} from "./jra";
import { fetchJraTrackConditionWithPlaywright } from "./jra-track-condition";
import { putPremiumDataTopCache } from "./premium-data-top-cache";
import {
  claimRealtimePlanRecovery,
  releaseRealtimePlanRecovery,
} from "./realtime-plan-recovery-claim";
import {
  BAN_EI_KEIBAJO_CODE,
  buildPremiumUrl,
  buildPremiumRaceLinkFromRace,
  detectPremiumLoginPrompt,
  discoverPremiumRaceLinks,
  fetchPremiumHtml,
  fetchPremiumHtmlAttempts,
  getPremiumRaceConfig,
  hasPremiumRaceFetchConfig,
  isPremiumDataTopHtmlAuthorized,
  isPremiumRaceDataTarget,
  isPremiumStableCommentHtmlAuthorized,
  matchPremiumLinkToRace,
  parsePremiumDataTopHorses,
  parsePremiumPaddockBulletins,
  parsePremiumStableComments,
  parsePremiumStateMessage,
  parsePremiumTrainingReviews,
  parseNetkeibaTrainingWorkouts,
  mergeNetkeibaTrainingWorkouts,
  summarizePremiumStableCommentHtml,
  type PremiumDataTopHorse,
  type PremiumPaddockBulletin,
  type PremiumStableComment,
  type PremiumTrainingReview,
} from "./premium-race";
import {
  clearCachedPremiumPaddock,
  readCachedPremiumPaddock,
  writeCachedPremiumPaddock,
} from "./premium-paddock-cache";
import { fetchJraRacesByDate, fetchNarRacesByDate } from "./postgres";
import { buildRealtimeRaceKey, raceKeyFromRealtimePath, type RealtimeSource } from "./race-key";
import {
  buildRealtimePayload,
  claimPremiumPaddockNotificationSend,
  claimResultCacheBust,
  claimResultFetch,
  claimReservedWeightFetch,
  claimTrackConditionFetch,
  claimWeightFetch,
  completeResultFetch,
  completeResultCacheBust,
  completeTrackConditionFetch,
  countJraRaceSourcesMissingRaceDateFieldsByDate,
  countJraRaceSourcesByDate,
  countRaceSourcesByDate,
  failTrackConditionFetch,
  failResultFetch,
  getPremiumRaceLink,
  getPremiumRacePayload,
  getPremiumPaddockFetchState,
  getPremiumPaddockNotificationState,
  getPremiumRaceDataFetchState,
  getQueueHealthMetrics,
  getLatestHorseWeights,
  getLatestRaceEntries,
  getRaceSource,
  deleteDailyRaceEntriesChunk,
  deleteOddsSnapshotsChunk,
  deleteRaceRunningStylesChunk,
  getLatestTrackConditionForRace,
  getSameDayVenueJockeyWins,
  incrementEmptyResultAttempts,
  insertRaceEntrySnapshot,
  insertRaceResultSnapshot,
  insertHorseWeightSnapshot,
  insertJraTrackConditionSnapshot,
  markEmptyResultGiveUp,
  listJraVenueTrackConditionSchedulesByDate,
  listOddsSnapshotsForExport,
  listPremiumRaceDataFetchCandidatesByDate,
  listPendingResultCacheBustRaceKeys,
  listRaceKeysByDateFromHyperdrive,
  listRaceSourceKeibajoCodesByDate,
  listRaceSourcesForSeed,
  listSchedulableRaceSourcesByDate,
  logFetch,
  markPremiumPaddockQueued,
  markPremiumRaceDataQueued,
  markResultFetchQueued,
  markTrackConditionQueued,
  recordPartialResultFetch,
  recordPremiumPaddockNotificationEvent,
  registerResultCacheBust,
  replacePremiumRaceData,
  resetEmptyResultAttempts,
  runD1Retention,
  toHorseTrends,
  toOddsTrendsByType,
  updateLastFetch,
  updatePremiumRaceDataFetchState,
  updatePremiumPaddockFetchState,
  updatePremiumPaddockNotificationState,
  upsertJraRaceSource,
  upsertNarRaceSource,
  upsertPremiumRaceLink,
  type HotOddsPayload,
  type LocalRaceRow,
  type PremiumRacePayload,
  type ResultCacheBustClaim,
  type SchedulableRaceSource,
} from "./storage";
import {
  RUNNING_STYLE_INFERENCE_CRON,
  RUNNING_STYLE_PREWARM_CRON,
  exportRunningStyleParquetsForDate,
  formatTomorrowYYYYMMDDInJst,
  planRunningStylePredictionsForDate,
  refreshViewerRunningStyleCachesForDate,
  resolveRunningStyleCronDates,
  runRunningStyleCronTick,
} from "./running-style-cron";
import { materializeRunningStyleFeatureParquetsForDate } from "./running-style-feature-materialize";
import { handleRunningStylePredictionJob } from "./running-style-queue";
import { DAILY_FEATURE_BUILD_CRON } from "./daily-feature-build";
import { WIN5_DISCOVER_CRON, logWin5CronResult } from "./win5-cron";
import { handleWin5PredictionJob } from "./win5-queue";
import { probeNeonWritePool } from "./neon-write-pool-probe";
import {
  parseRunningStylePostgresVerificationParams,
  runRunningStyleWorkerPostgresVerification,
} from "./running-style-verification";
import { readCachedTrackCondition, writeCachedTrackCondition } from "./track-condition-cache";
import {
  proxyHorseWeightLatestFromStub,
  proxyHorseWeightStreamFromStub,
  writeHorseWeightSnapshotToStub,
  type HorseWeightSnapshot,
} from "./durable-objects/horse-weight-do";
export { HorseWeightDO } from "./durable-objects/horse-weight-do";
import {
  buildRaceTrendDailyTrackDoIdName,
  fetchRaceTrendDailyTrackRacesFromStub,
  pushRaceTrendDailyTrackRowToStub,
} from "./durable-objects/race-trend-daily-track-do";
export { RaceTrendDailyTrackDO } from "./durable-objects/race-trend-daily-track-do";
import type { RaceTrendDailyTrackRow } from "horse-racing-realtime/race-trend-daily-track-types";
import { buildTrendBustFromRaceContext, requestTrendCacheBust } from "./viewer-trend-cache-bust";
import { triggerRaceCacheBust } from "./viewer-race-cache-bust";
import {
  getJraAdvanceOddsFetchSlotAt,
  getJstDateParts,
  getNarOddsFetchSlotAt,
  getNarOddsSaleStartAt,
  getOddsFetchSlotAt,
  getTodayJst,
  isJstPollingWindow,
  parseRaceStartJst,
  toJstIsoString,
} from "./time";
import type {
  Env,
  HorseWeight,
  Job,
  NarRaceSource,
  OddsType,
  RaceEntry,
  RaceResult,
  RealtimeRacePayload,
  WeightSnapshotGeneration,
} from "./types";

const QUEUE_SEND_BATCH_SIZE = 100;
// /api/internal/queue-health treats races whose last result-fetch was longer
// than this threshold ago AND that never completed as "stuck". 30 minutes is
// the operational tolerance: the planner re-claims every 2 minutes and the
// upstream rarely publishes results > 25 minutes after post time, so a row
// stale > 30 min is a signal something is wrong, not a normal late publish.
const QUEUE_HEALTH_STUCK_THRESHOLD_MINUTES = 30;
const NEON_WRITE_POOL_HEALTH_CACHE_CONTROL = "private, no-store";

type NeonWritePoolSource = "DATABASE_URL_NEON" | "NEON_DATABASE_URL";
type NeonWritePoolQueryErrorClass = "auth" | "network" | "read_only" | "unknown";

interface NeonWritePoolProbeSuccess {
  canInsertFinishPosition: boolean;
  canInsertRunningStyle: boolean;
  canUpsertFinishPosition: boolean;
  canUpsertRunningStyle: boolean;
  defaultTransactionReadOnly: boolean;
  defaultTransactionReadOnlySource: string;
  fpTablePresent: boolean;
  inRecovery: boolean;
  ok: true;
  rsTablePresent: boolean;
  source: NeonWritePoolSource;
  transactionReadOnly: boolean;
  writablePrimary: boolean;
}

interface NeonWritePoolUnconfiguredResult {
  errorClass: "unconfigured";
  ok: false;
}

interface NeonWritePoolQueryFailure {
  errorClass: NeonWritePoolQueryErrorClass;
  ok: false;
  source: NeonWritePoolSource;
}

type NeonWritePoolProbeResult =
  | NeonWritePoolProbeSuccess
  | NeonWritePoolUnconfiguredResult
  | NeonWritePoolQueryFailure;

interface NeonWritePoolHealthOkResult {
  canInsertFinishPosition: boolean;
  canInsertRunningStyle: boolean;
  canUpsertFinishPosition: boolean;
  canUpsertRunningStyle: boolean;
  defaultTransactionReadOnly: boolean;
  defaultTransactionReadOnlySource: string;
  fpTablePresent: boolean;
  inRecovery: boolean;
  rsTablePresent: boolean;
  source: NeonWritePoolSource;
  transactionReadOnly: boolean;
  writablePrimary: boolean;
}

interface NeonWritePoolHealthOkBody {
  result: NeonWritePoolHealthOkResult;
  status: "ok";
}

interface NeonWritePoolHealthQueryErrorBody {
  result: { source: NeonWritePoolSource };
  status: NeonWritePoolQueryErrorClass;
}

interface NeonWritePoolHealthUnconfiguredBody {
  status: "unconfigured";
}

interface RealtimePlanRecoveryOptions {
  delaySeconds: number;
  failureClass: "connection_pressure" | "other";
  now: Date;
  stage: "queue.enqueue-single-recovery" | "scheduled.enqueue-single-recovery";
}

interface LoggedPlannerStageInput {
  env: Env;
  jobType: "drain-result-cache-busts" | "plan-premium-paddock" | "plan-result-fetches";
  plan: () => Promise<number>;
}

interface RunResultCacheBustsInput {
  claim: ResultCacheBustClaim | null;
  env: Env;
  race: NarRaceSource;
}

interface ResultCacheBustOutcomes {
  raceDelivered: boolean;
  trendDelivered: boolean;
}

interface PremiumRaceDataSections {
  dataTopHorses?: PremiumDataTopHorse[];
  stableComments?: PremiumStableComment[];
  trainingReviews?: PremiumTrainingReview[];
}

// True at most once per hour so the discover-urls fallback only fires off
// the :02 result-poller tick of each JST hour. Without this guard the
// cron would re-discover every 2 minutes, which is wasteful.
const HOURLY_DISCOVERY_RECOVERY_MINUTE = 2;
const RESULT_FETCH_LOCK_MINUTES = 10;
const RESULT_CACHE_BUST_LEASE_SECONDS = 90;
const RESULT_CACHE_BUST_DRAIN_LIMIT = 12;
// 2026-06-02: NAR keiba.go.jp upstream publishes results progressively — top-3
// finishers first, then the remaining horses several minutes later. Without a
// short retry lock the default RESULT_FETCH_LOCK_MINUTES would block re-fetch
// for the entire 10 min window, leaving the viewer stuck on top-3 only. When
// we detect a partial result (inserted < expectedHorseCount) we shorten the
// lock to this value so the next result-poll cron tick (every 2 min) can
// re-claim and pick up the freshly-published remaining rows.
// 2026-06-05: replaces the old NAR_RESULT_COMPLETION_BACKSTOP_MINUTES
// force-complete path with a progressive retry — the lock interval grows as
// the gap since race start grows (short / medium / long) and the race only
// gets force-completed after RESULT_FETCH_GIVE_UP_HOURS. The previous 60-min
// backstop force-completed races that the upstream eventually publishes
// minutes-to-hours later, so the missing finishers were permanently dropped
// from the D1 race-result snapshot.
const RESULT_FETCH_RETRY_LOCK_MINUTES = 2;
// 2026-06-05: medium-phase retry lock used between RESULT_FETCH_RETRY_MEDIUM_THRESHOLD_MINUTES
// and RESULT_FETCH_RETRY_LONG_THRESHOLD_MINUTES after race start. Reduces D1
// + upstream HTTP load while still re-fetching often enough to land late
// publishes inside the same hour.
const RESULT_FETCH_RETRY_MEDIUM_LOCK_MINUTES = 5;
// 2026-06-05: long-phase retry lock used between RESULT_FETCH_RETRY_LONG_THRESHOLD_MINUTES
// and RESULT_FETCH_GIVE_UP_HOURS after race start. Long enough that we are
// not hammering the upstream after the obvious publish window but short
// enough to catch the rare multi-hour late publishes that the previous
// 60-min backstop discarded.
const RESULT_FETCH_RETRY_LONG_LOCK_MINUTES = 15;
// 2026-06-05: boundary between short and medium retry phases. Within this
// window keiba.go.jp typically publishes the remaining finishers within one
// or two cron ticks, so a 2-minute lock is appropriate.
const RESULT_FETCH_RETRY_MEDIUM_THRESHOLD_MINUTES = 10;
// 2026-06-05: boundary between medium and long retry phases.
const RESULT_FETCH_RETRY_LONG_THRESHOLD_MINUTES = 60;
// 2026-06-05: max age (since race start) we keep retrying a partial result
// fetch. After this point we mark the race complete with whatever has been
// saved so far so the planner stops re-enqueuing forever. 24h covers every
// observed real-world late-publish gap on keiba.go.jp / JRA.
const RESULT_FETCH_GIVE_UP_HOURS = 24;
// 2026-06-30: empty-result circuit breaker threshold. When the upstream
// result HTML parses to zero rows for the same race this many times in a
// row AND enough wall-clock time has elapsed past the publish window,
// fetchAndStoreResults force-completes the race with `result_complete_at`
// (logged as `empty_giveup:race_count_exceeded`). Raised from 20 to 40 in
// tandem with the don't-throw fix (queue-retry storm previously inflated
// 1 cron tick into 4 increments, so the old 20 effectively gave up after
// only 5 ticks ~10 min — before NAR publishes results at +10-15 min). With
// the don't-throw fix, 1 tick = 1 increment, so 40 attempts = ~80 minutes
// of cron ticks, well past the realistic NAR publish window. A non-empty
// fetch resets the counter (storage.resetEmptyResultAttempts).
const RESULT_FETCH_EMPTY_GIVEUP_COUNT = 40;
// 2026-06-30: minimum minutes after the official race start before the
// empty-result counter is even allowed to start ticking. NAR (keiba.go.jp)
// typically does NOT publish results until ~10-15 minutes after the race
// finishes, so any empty-result observation before then is the normal
// awaiting-publish window, not a real failure. Logged as
// `skip:awaiting-publish` (one row per race per dedupe window) instead of
// incrementing the counter, so an operator can confirm the planner is
// still polling without inflating fetch_logs or the circuit breaker.
const NAR_RESULT_PUBLISH_DELAY_MINUTES = 10;
// 2026-06-30: defence-in-depth giveup floor. Even if the per-race counter
// somehow exceeds RESULT_FETCH_EMPTY_GIVEUP_COUNT (clock skew, multi-
// consumer drift, retry storm against a not-yet-fixed upstream), refuse
// to give up unless the wall-clock has also passed this many minutes
// since the official race start. Prevents the 2026-06-28 NAR-result-lost
// failure mode where the counter tripped ~5-6 minutes after race start,
// well before the upstream had even published, and locked the race row
// forever.
const RESULT_FETCH_EMPTY_GIVEUP_MIN_MINUTES_AFTER_START = 60;
// 2026-07-24 incident (Oi 5R/6R): both races took the full 60-minute floor
// above before force-completing with zero result rows, because keiba.go.jp
// never returns anything the parser can tell apart from "not published yet"
// on the per-race result page itself. Its RaceList page (the same page
// fetchTodayRaceListUrls reads) is more informative: it disables a race's
// 成績 link only once and never re-enables it once a result truly will not
// land (confirmed hours later for both races that day, while every other
// race that day flipped to enabled within its normal publish window). This
// floor lets a NAR race give up once that upstream signal is checked and
// confirms "disabled", well before the full RESULT_FETCH_EMPTY_GIVEUP_COUNT
// / RESULT_FETCH_EMPTY_GIVEUP_MIN_MINUTES_AFTER_START floor would otherwise
// require — still comfortably past NAR_RESULT_PUBLISH_DELAY_MINUTES so a
// race that is merely running late keeps its normal grace period first.
const NAR_RESULT_VOID_CHECK_MIN_MINUTES_AFTER_START = 30;
// Distinguishes a RaceList-confirmed void give-up from the plain attempt-
// count/time-floor circuit breaker in fetch_logs telemetry.
export const EMPTY_RESULT_VOID_LOG_STATUS = "empty_giveup:race_list_disabled";
// 2026-05-31: lowered from 3 to 2 in tandem with the result-poll cron drop
// from "*/5" to "*/2". With the previous 5-minute cron + 3-minute throttle
// 11R results landed in D1 up to ~5 minutes after JRA published them, and
// the 12R detail view's race-trend panel showed only 1R-10R for that whole
// window. Each result-poll tick is one cheap SELECT against
// realtime_race_sources so D1 still has plenty of CPU headroom.
const RESULT_FETCH_INTERVAL_MINUTES = 2;
// 2026-06-07: re-enqueue threshold for races whose `last_result_queued_at`
// stayed set without ever being cleared by `completeResultFetch` /
// `failResultFetch` / `recordPartialResultFetch`. When a `fetch-results` job
// is dequeued but takes an early-return path (claim failed, race not finished
// yet, transient skip) the queued_at column is never reset, so the planner
// permanently skips that race even after the lock has expired. This stale
// threshold MUST be strictly larger than the longest retry lock window
// (`RESULT_FETCH_RETRY_LONG_LOCK_MINUTES` = 15) plus a small grace, so we do
// not race the in-flight job. 20 minutes = 15 + 5 grace.
const RESULT_FETCH_QUEUE_STALE_MINUTES = 20;
const RESULT_FETCH_INLINE_JRA_MAX_PER_TICK = 1;
const RESULT_FETCH_INLINE_NAR_MAX_PER_TICK = 4;
// JST 09-22 (= UTC 00-13) is the race-day result-poller cron. Distinct from
// the hourly "0 0-13 * * *" plan-realtime-fetches cron so we only run the
// result poller without re-triggering the heavier hourly work. Tightened to
// every 2 minutes (was every 5) so a freshly-finished race appears in the
// merged race-trend payload within one or two ticks instead of up to five.
export const RESULT_POLL_CRON = "*/2 0-13 * * *";
const TRACK_CONDITION_FETCH_LOCK_MINUTES = 15;
const QUEUE_RETRY_DELAY_SECONDS = 60;
const RUNNING_STYLE_QUEUE_LOG_MARKER = "queue=running-style-predictions";
const PREMIUM_RACE_DATA_RETRY_DELAY_SECONDS = 20 * 60;
// Proxy session expiries auto-recover within minutes; keep the auth re-queue
// gap short so a flaky session does not block a whole race-day window. Cap
// total attempts so a permanently broken upstream cannot loop forever — past
// the cap we keep the row in `auth_required` but back off to an hourly retry
// so the next session-recovery window still picks the race up.
const PREMIUM_RACE_DATA_AUTH_RETRY_DELAY_SECONDS = 5 * 60;
const PREMIUM_RACE_DATA_AUTH_RETRY_BACKOFF_SECONDS = 60 * 60;
const PREMIUM_RACE_DATA_AUTH_RETRY_MAX_ATTEMPTS = 5;
const PREMIUM_PADDOCK_RETRY_DELAY_SECONDS = 120;
const PREMIUM_PADDOCK_RETRY_DELAY_HOT_SECONDS = 15;
const PREMIUM_PADDOCK_RETRY_DELAY_WARM_SECONDS = 30;
const PREMIUM_PADDOCK_HOT_WINDOW_MINUTES = 20;
const PREMIUM_PADDOCK_WARM_WINDOW_MINUTES = 40;
const PREMIUM_PADDOCK_RECHECK_MINUTES = 1;
const PREMIUM_PADDOCK_WINDOW_BEFORE_MINUTES = 120;
const PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES = 2;
const REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS = 60;
const REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS = 90;
// 2026-06-07: D1 overload error markers used to detect when a plan-realtime
// run failed because the D1 binding was throttled rather than because of a
// real bug. When seen, we open a circuit breaker that suppresses the next
// few cron ticks + queue retries so the queue does not multiply itself into
// thousands of identical jobs that all hit the same overloaded D1 instance.
const D1_OVERLOAD_MARKERS: readonly string[] = ["D1 DB is overloaded", "Too many requests queued"];
const PLAN_REALTIME_CIRCUIT_BREAKER_KV_KEY = "plan-realtime-fetches:circuit-breaker";
const PLAN_REALTIME_CIRCUIT_BREAKER_KV_VALUE = "open";
const PLAN_REALTIME_CIRCUIT_BREAKER_TTL_SECONDS = 120;
// Queue-side retry delay used only when the failure was caused by D1 overload.
// 60s base + random 0..120s jitter prevents the next retry wave from landing
// on the same second across all batched plan-realtime jobs.
const PLAN_REALTIME_OVERLOAD_RETRY_DELAY_BASE_SECONDS = 60;
const PLAN_REALTIME_OVERLOAD_RETRY_DELAY_JITTER_SECONDS = 120;
const CONNECTION_PRESSURE_MARKERS: readonly string[] = [
  "cannot perform i/o on behalf of a different request",
  "connection pool exhausted",
  "connection terminated unexpectedly",
  "failed to acquire connection",
  "failed to acquire permit to connect to the database",
  "maxclientsinsessionmode",
  "remaining connection slots are reserved",
  "timed out while creating a new server connection",
  "too many connections",
];
const CONNECTION_PRESSURE_RECOVERY_DELAY_BASE_SECONDS = 120;
const CONNECTION_PRESSURE_RECOVERY_DELAY_JITTER_SECONDS = 120;
const REALTIME_PLAN_RECOVERY_CLAIM_TTL_SECONDS = 5 * 60;
const REALTIME_PLAN_RECOVERY_CLAIM_KEY_PREFIX = "plan-realtime-fetches-recovery";
const DEFAULT_PREMIUM_RACE_QUEUE_DELAY_SECONDS = 15;
const DEFAULT_PREMIUM_PADDOCK_DISCORD_BOT_NAME = "外部パドック速報";
const DEFAULT_DETAIL_ORIGIN = "https://pc-keiba-viewer.kkk4oru.com";
const PREMIUM_PADDOCK_NOTIFICATION_FORMAT_VERSION = "2026-05-16-v2";
const PREMIUM_PADDOCK_NOTIFICATION_LOCK_SECONDS = 90;
// JRA horse-weight fetch scheduling priority (Tokyo/Kyoto 5R and 11R first,
// then 5R-onward JRA, then 1R-4R). `race_bango` is stored zero-padded in
// realtime_race_sources.race_bango (see storage.ts toRaceSource: row.race_bango
// is written via padStart(2,"0") in upsertNarRaceSource / upsertJraRaceSource),
// so we compare against "05" not "5".
const JRA_PRIORITY_VENUE_CODES = ["05", "08"] satisfies readonly string[];
const JRA_PRIORITY_RACE_BANGOS = ["05", "11"] satisfies readonly string[];
// 2026-06-06: raised from 90 to 180 so the 15-minute weight-plan cron has
// 12 attempts per race instead of 6, recovering from any single Hyperdrive
// read timeout that leaves planRealtimeFetches with an empty race list.
// Paired with the cron change from "0 0-13 * * *" (hourly) to
// "*/15 0-14 * * *" (15-min) and with WEIGHT_FETCH_SAME_DAY_COOLDOWN_MINUTES
// so a single in-day failure no longer locks out re-fetch for 24 hours.
const WEIGHT_FETCH_LEAD_MINUTES = 180;
const WEIGHT_FETCH_PRIORITY_TIER_HIGH = 0;
const WEIGHT_FETCH_PRIORITY_TIER_MID = 1;
const WEIGHT_FETCH_PRIORITY_TIER_LOW = 2;
const WEIGHT_FETCH_PRIORITY_TIER_NAR = 3;
const WEIGHT_FETCH_BANGO_PRIORITY_THRESHOLD = 5;
// Once a weight fetch succeeds on a different JST date than the race we wait
// 24h before re-fetching. When the previous fetch is on the same JST date as
// the race we only wait 1h, so any partial-page failure has many retries
// before post time instead of being silently locked out for the whole day.
const WEIGHT_FETCH_INTERVAL_MINUTES = 24 * 60;
const WEIGHT_FETCH_SAME_DAY_COOLDOWN_MINUTES = 60;
// Near-race cooldown override: when the race is within
// WEIGHT_FETCH_NEAR_RACE_THRESHOLD_MINUTES of post time (and not too far
// past it), shorten the cooldown to 10 minutes so a recent partial / empty
// snapshot does not lock out re-fetch for the entire 60-minute same-day
// window when post time is imminent.
const WEIGHT_FETCH_NEAR_RACE_COOLDOWN_MINUTES = 10;
const WEIGHT_FETCH_NEAR_RACE_THRESHOLD_MINUTES = 30;
const WEIGHT_FETCH_NEAR_RACE_POST_LIMIT_MINUTES = 10;
const WEIGHT_FETCH_EMPTY_RETRY_DELAY_SECONDS = 10 * 60;
const WEIGHT_FETCH_EMPTY_RETRY_NEAR_RACE_DELAY_SECONDS = 5 * 60;
// Longer than the 25s queue-handler timeout, but shorter than the Queue's 60s
// retry delay. This blocks already-queued duplicates while ensuring the first
// retry after a failed scrape can claim immediately.
const WEIGHT_FETCH_LEASE_SECONDS = 45;
// Running-style feature assembly may legitimately take about a minute when
// Catalog has to build a race parquet.  It must still have a finite bound so
// one unavailable Catalog request cannot hold the single-concurrency queue
// forever and starve later race dates.
const MILLISECONDS_PER_MINUTE = 60_000;
// KV TTL for the weight-race-list fallback (used when Hyperdrive returns an
// empty result so the plan still has something to enqueue). 24h keeps the
// fallback alive across the entire race day.
const WEIGHT_RACE_LIST_KV_TTL_SECONDS = 24 * 60 * 60;
const WEIGHT_RACE_LIST_KV_PREFIX = "realtime:weight-race-list:";
// Sparse-row guard for horse weight fetches: if parser returns 1 row only,
// skip the write entirely so existing snapshots are preserved. The next cron
// will re-fetch.
const MIN_HORSE_WEIGHT_ROWS_PER_RACE = 2;
// Event-driven per-race rescore trigger fired right after a fetch-weights job
// writes weights to D1. Posts to finish-position-cron's internal endpoint over
// the FINISH_POSITION_CRON service binding so the race is re-scored with fresh
// weights immediately, without waiting for the 5-min coordinator cron poll.
// The internal URL host is arbitrary — service bindings ignore it — but the
// path must match finish-position-cron's INTERNAL_RESCORE_RACE_PATH.
const FINISH_POSITION_CRON_INTERNAL_RESCORE_RACE_URL =
  "https://finish-position-cron.internal/api/internal/rescore-race";
const FINISH_POSITION_CRON_INTERNAL_READINESS_URL =
  "https://finish-position-cron.internal/api/internal/prediction-readiness";
const WEIGHT_RESCORE_TRIGGER_LOG_KIND = "weight-rescore-trigger";
// Ban-ei rows live under the nar source with keibajo_code 65 / 83 (帯広);
// the finish-position-cron predict pipeline produces ban-ei as its own
// category so the trigger maps those two codes to "ban-ei" instead of "nar".
const BAN_EI_KEIBAJO_CODES: ReadonlySet<string> = new Set(["65", "83"]);
const RACE_KEY_PART_COUNT = 5;
// JST hours at which planRealtimeFetches fires `discover-premium-races`.
// 20:00 prepares tomorrow's premium race links. 09:00 is the recovery slot
// for the previous 20:00 tick when D1 overload or Hyperdrive timeout left
// the discovery step incomplete, so today's paddock pipeline still has
// fresh links instead of running empty until the next 20:00 tick.
const PREMIUM_RACE_DISCOVERY_HOURS_JST = [9, 20] satisfies readonly number[];
// Weight watchdog: the sole automatic scheduler for horse-weight jobs. It
// bypasses the
// heavier plan-realtime-fetches code path so a circuit-breaker open state
// (D1 saturation) does not silently skip weight enqueueing for upcoming
// races. The watchdog inspects realtime_race_sources directly and enqueues
// fetch-weights jobs only while no successful weight snapshot has been
// recorded. The general planner and the fetch handler never self-enqueue
// weight jobs, so each retry cadence has one owner.
export const WEIGHT_WATCHDOG_CRON = "1-59/2 * * * *";
const WEIGHT_WATCHDOG_LOOKAHEAD_MINUTES = 180;
const WEIGHT_WATCHDOG_LOOKBACK_MINUTES = 10;
const WEIGHT_WATCHDOG_FAR_ATTEMPT_BACKOFF_MINUTES = 15;
const WEIGHT_WATCHDOG_NEAR_ATTEMPT_BACKOFF_MINUTES = 5;
const WEIGHT_WATCHDOG_NEAR_RACE_THRESHOLD_MINUTES = 30;
const WEIGHT_WATCHDOG_ADAPTIVE_RACE_THRESHOLD_MINUTES = 90;
// Reservations are fenced by moving last_weight_fetch_attempt_at one second
// past the cron timestamp. A one-minute eligibility cutoff therefore makes a
// completed pending observation eligible on the next two-minute cron tick without
// weakening the per-race atomic reservation.
const WEIGHT_WATCHDOG_ADAPTIVE_ATTEMPT_BACKOFF_MINUTES = 1;
const WEIGHT_WATCHDOG_MAX_PER_TICK = 24;
const WEIGHT_WATCHDOG_HEARTBEAT_DEDUPE_TTL_SECONDS = 60 * 60;
const JRA_KEIBAJO_NAMES: Record<string, string> = {
  "01": "札幌",
  "02": "函館",
  "03": "福島",
  "04": "新潟",
  "05": "東京",
  "06": "中山",
  "07": "中京",
  "08": "京都",
  "09": "阪神",
  "10": "小倉",
};

const getNow = (env: Env): Date => {
  if (!env.REALTIME_TEST_NOW) {
    return new Date();
  }
  const date = new Date(env.REALTIME_TEST_NOW);
  return Number.isNaN(date.getTime()) ? new Date() : date;
};

const json = (body: unknown, init?: ResponseInit): Response =>
  new Response(JSON.stringify(body), {
    headers: (() => {
      const headers = mergeJsonHeaders(init);
      headers.set("access-control-allow-origin", "*");
      if (!headers.has("cache-control")) {
        headers.set("cache-control", "public, max-age=0");
      }
      return headers;
    })(),
    status: init?.status ?? 200,
  });

const buildNeonWritePoolHealthResponse = (probe: NeonWritePoolProbeResult): Response => {
  if (probe.ok) {
    const body: NeonWritePoolHealthOkBody = {
      result: {
        canInsertFinishPosition: probe.canInsertFinishPosition,
        canInsertRunningStyle: probe.canInsertRunningStyle,
        canUpsertFinishPosition: probe.canUpsertFinishPosition,
        canUpsertRunningStyle: probe.canUpsertRunningStyle,
        defaultTransactionReadOnly: probe.defaultTransactionReadOnly,
        defaultTransactionReadOnlySource: probe.defaultTransactionReadOnlySource,
        fpTablePresent: probe.fpTablePresent,
        inRecovery: probe.inRecovery,
        rsTablePresent: probe.rsTablePresent,
        source: probe.source,
        transactionReadOnly: probe.transactionReadOnly,
        writablePrimary: probe.writablePrimary,
      },
      status: "ok",
    };
    return json(body, {
      headers: { "cache-control": NEON_WRITE_POOL_HEALTH_CACHE_CONTROL },
    });
  }
  if (probe.errorClass === "unconfigured") {
    const body: NeonWritePoolHealthUnconfiguredBody = { status: "unconfigured" };
    return json(body, {
      headers: { "cache-control": NEON_WRITE_POOL_HEALTH_CACHE_CONTROL },
      status: 503,
    });
  }
  const body: NeonWritePoolHealthQueryErrorBody = {
    result: { source: probe.source },
    status: probe.errorClass,
  };
  return json(body, {
    headers: { "cache-control": NEON_WRITE_POOL_HEALTH_CACHE_CONTROL },
    status: 502,
  });
};

const HOT_WORKER_ORIGIN = "https://sync-realtime-data-hot.kkk4oru.com";
const FEATURES_WORKER_ORIGIN = "https://sync-realtime-data-features.kkk4oru.com";
const FORWARD_RESPONSE_BODY_MAX_LENGTH = 200;
// 2026-06-13: bound the wall-time of the fire-and-forget features-worker POST.
// Without this, a hung or Hyperdrive-timeout features worker keeps the queue
// consumer slot (`max_concurrency: 3`) occupied long enough to starve other
// plan-realtime-fetches jobs. The enqueue-recompute endpoint must durably send
// the build job before acknowledging; production observed a request reaching
// the old 5s boundary, so keep the bound finite while allowing normal queue
// latency headroom.
const FORWARD_RACE_FEATURES_TIMEOUT_MS = 15000;
const FORWARD_RACE_FEATURES_TIMEOUT_MESSAGE_PREFIX = "timeout";

// Per-race D1 upsert retry tuning. The discover-urls job historically failed
// atomically on a single `D1_ERROR: Internal error in D1 DB storage caused
// object to be reset` or `Idle connection closed`, so the entire date's races
// were left unseen by downstream cron. The fix is per-race try / catch with
// bounded exponential backoff so one transient D1 error only loses that one
// race — the rest of the date is still ingested.
const DISCOVER_UPSERT_MAX_ATTEMPTS = 3;
const DISCOVER_UPSERT_BASE_DELAY_MS = 200;
const DISCOVER_UPSERT_BACKOFF_MULTIPLIER = 4;
const DISCOVER_UPSERT_FAILED_RACE_KEYS_MAX = 50;

const readForwardResponseBody = async (response: Response): Promise<string> => {
  try {
    const text = await response.text();
    return text.slice(0, FORWARD_RESPONSE_BODY_MAX_LENGTH);
  } catch {
    return "";
  }
};

interface WeightFetchPriorityInput {
  source: string;
  keibajoCode: string;
  raceBango: string;
}

interface WeightCandidate {
  race: SchedulableRaceSource;
  minutes: number;
}

interface ForwardRaceSourceArgs {
  source: "jra" | "nar";
  raceKey: string;
  raceStartAtJst: string;
  debaUrl: string;
  oddsLinksJson: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

interface ForwardNarTanshoOddsArgs {
  fetchedAt: string;
  raceKey: string;
  rows: RaceResultTanshoOddsRow[];
}

interface ImportOddsSnapshotRowPayload {
  average_odds: number | null;
  combination: string;
  fetched_at: string;
  max_odds: number | null;
  min_odds: number | null;
  odds: number;
  odds_type: string;
  race_key: string;
  rank: number;
}

const NAR_TANSHO_ODDS_TYPE = "tansho";

interface ForwardRaceForFeaturesArgs {
  source: "jra" | "nar";
  raceKey: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
}

type DiscoverUpsertOutcome = "inserted" | "retried" | "failed";

interface DiscoverUpsertResult {
  raceKey: string;
  outcome: DiscoverUpsertOutcome;
}

interface DiscoverUpsertCounters {
  inserted: number;
  retried: number;
  failed: number;
  failedRaceKeys: readonly string[];
}

interface RetryUpsertArgs {
  raceKey: string;
  attempt: (attempt: number) => Promise<void>;
  sleep: (ms: number) => Promise<void>;
}

const buildRaceStartAtJst = (
  kaisaiNen: string,
  kaisaiTsukihi: string,
  hassoJikoku: string | null,
): string =>
  hassoJikoku
    ? `${kaisaiNen}-${kaisaiTsukihi.slice(0, 2)}-${kaisaiTsukihi.slice(2, 4)}T${hassoJikoku.slice(0, 2)}:${hassoJikoku.slice(2, 4)}:00+09:00`
    : "";

export const forwardRaceSourceToHot = async (
  env: Env,
  args: ForwardRaceSourceArgs,
): Promise<void> => {
  if (!env.REALTIME_HOT || !env.PC_KEIBA_VIEWER_INTERNAL_TOKEN) {
    return;
  }
  try {
    const response = await env.REALTIME_HOT.fetch(
      `${HOT_WORKER_ORIGIN}/api/internal/odds-fetch-state`,
      {
        body: JSON.stringify(args),
        headers: {
          "content-type": "application/json",
          "x-pc-keiba-internal-token": env.PC_KEIBA_VIEWER_INTERNAL_TOKEN,
        },
        method: "POST",
      },
    );
    if (!response.ok) {
      const body = await readForwardResponseBody(response);
      await logFetch(
        env.REALTIME_DB,
        "forward-race-source-to-hot",
        "error",
        args.raceKey,
        `status=${response.status} body=${body.slice(0, FORWARD_RESPONSE_BODY_MAX_LENGTH)}`,
      ).catch(() => undefined);
    }
  } catch (error) {
    // Forwarding to the hot worker is best-effort: never block discovery on it.
    await logFetch(
      env.REALTIME_DB,
      "forward-race-source-to-hot",
      "error",
      args.raceKey,
      formatError(error),
    ).catch(() => undefined);
  }
};

// Fire-and-forget POST that re-upserts NAR tansho odds + popularity into the
// hot worker's odds_snapshots D1 after a successful result fetch. This closes
// the loop when live odds polling missed the final pre-race snapshot — the
// result page (RaceMarkTable) already carries the final 単勝オッズ / 人気
// per row, so the same payload can backfill prior odds rows via the existing
// /api/internal/import-odds-chunk endpoint (ON CONFLICT DO UPDATE keeps the
// re-POST idempotent against earlier live polls).
export const forwardNarTanshoOddsToHot = async (
  env: Env,
  args: ForwardNarTanshoOddsArgs,
): Promise<void> => {
  if (!env.REALTIME_HOT || !env.PC_KEIBA_VIEWER_INTERNAL_TOKEN) {
    return;
  }
  if (args.rows.length === 0) {
    return;
  }
  const importRows = args.rows.map(
    (row): ImportOddsSnapshotRowPayload => ({
      average_odds: null,
      combination: row.horseNumber,
      fetched_at: args.fetchedAt,
      max_odds: null,
      min_odds: null,
      odds: row.tanshoOdds,
      odds_type: NAR_TANSHO_ODDS_TYPE,
      race_key: args.raceKey,
      rank: row.popularity,
    }),
  );
  try {
    const response = await env.REALTIME_HOT.fetch(
      `${HOT_WORKER_ORIGIN}/api/internal/import-odds-chunk`,
      {
        body: JSON.stringify({ rows: importRows }),
        headers: {
          "content-type": "application/json",
          "x-pc-keiba-internal-token": env.PC_KEIBA_VIEWER_INTERNAL_TOKEN,
        },
        method: "POST",
      },
    );
    if (!response.ok) {
      const body = await readForwardResponseBody(response);
      await logFetch(
        env.REALTIME_DB,
        "forward-nar-tansho-odds-to-hot",
        "error",
        args.raceKey,
        `status=${response.status} body=${body.slice(0, FORWARD_RESPONSE_BODY_MAX_LENGTH)}`,
      ).catch(() => undefined);
    }
  } catch (error) {
    // Forwarding to the hot worker is best-effort: never block the result
    // fetch on it (the result snapshot has already been persisted upstream).
    await logFetch(
      env.REALTIME_DB,
      "forward-nar-tansho-odds-to-hot",
      "error",
      args.raceKey,
      formatError(error),
    ).catch(() => undefined);
  }
};

const isAbortError = (error: unknown): boolean =>
  error instanceof Error && error.name === "AbortError";

const formatForwardRaceFeaturesError = (error: unknown): string =>
  isAbortError(error)
    ? `${FORWARD_RACE_FEATURES_TIMEOUT_MESSAGE_PREFIX} after ${FORWARD_RACE_FEATURES_TIMEOUT_MS}ms`
    : formatError(error);

// Fire-and-forget POST to the new features worker so the new R2 Parquet build
// + new D1 inference pipeline can pick up the race the moment we discover it.
// fail-soft: any error is logged but the upstream race upsert is never blocked.
// 2026-06-13: bounded with an AbortController-driven timeout so a hung features
// worker cannot tie up the queue consumer slot for the whole queue retry budget.
export const forwardRaceForFeatures = async (
  env: Env,
  args: ForwardRaceForFeaturesArgs,
): Promise<void> => {
  if (!env.REALTIME_FEATURES || !env.PC_KEIBA_VIEWER_INTERNAL_TOKEN) {
    return;
  }
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), FORWARD_RACE_FEATURES_TIMEOUT_MS);
  try {
    const response = await env.REALTIME_FEATURES.fetch(
      `${FEATURES_WORKER_ORIGIN}/api/internal/enqueue-recompute`,
      {
        body: JSON.stringify(args),
        headers: {
          "content-type": "application/json",
          "x-pc-keiba-internal-token": env.PC_KEIBA_VIEWER_INTERNAL_TOKEN,
        },
        method: "POST",
        signal: controller.signal,
      },
    );
    if (!response.ok) {
      const body = await readForwardResponseBody(response);
      await logFetch(
        env.REALTIME_DB,
        "forward-race-for-features",
        "error",
        args.raceKey,
        `status=${response.status} body=${body.slice(0, FORWARD_RESPONSE_BODY_MAX_LENGTH)}`,
      ).catch(() => undefined);
    }
  } catch (error) {
    // Forwarding to the features worker is best-effort: never block discovery on it.
    await logFetch(
      env.REALTIME_DB,
      "forward-race-for-features",
      "error",
      args.raceKey,
      formatForwardRaceFeaturesError(error),
    ).catch(() => undefined);
  } finally {
    clearTimeout(timeoutId);
  }
};

export const fetchHotOddsPayload = async (
  env: Env,
  raceKey: string,
): Promise<HotOddsPayload | null> => {
  if (!env.REALTIME_HOT) {
    return null;
  }
  try {
    const response = await env.REALTIME_HOT.fetch(`${HOT_WORKER_ORIGIN}/api/odds/${raceKey}`);
    if (!response.ok) {
      return null;
    }
    const body = (await response.json()) as HotOddsPayload | null;
    return body ?? null;
  } catch {
    return null;
  }
};

// REALTIME_DB is still recovering from the historical odds polling load and can
// throw `D1_ERROR: D1 DB exceeded its CPU time limit and was reset.` for even a
// single SELECT. Without this guard the /realtime endpoint propagates the
// exception as Cloudflare worker error 1101 and the viewer's odds chart goes
// blank, even though the hot worker has the odds payload ready. When D1 fails
// we still serve the hot odds plus a stub for the D1-derived fields so the
// chart keeps rendering.
export const buildDegradedRealtimePayload = (
  raceKey: string,
  hotOdds: HotOddsPayload | null,
): RealtimeRacePayload => ({
  horseWeights: null,
  odds: hotOdds
    ? {
        fetchedAt: hotOdds.fetchedAt,
        history: hotOdds.history,
        historyByType: hotOdds.historyByType,
        horseTrends: toHorseTrends(hotOdds.history),
        latest: hotOdds.latest,
        trendsByType: toOddsTrendsByType(hotOdds.historyByType),
      }
    : null,
  raceEntries: null,
  raceKey,
  raceResults: null,
  source: null,
  trackCondition: null,
});

export const buildRealtimeRouteResponse = async (
  env: Env,
  raceKey: string,
): Promise<RealtimeRacePayload> => {
  const hotOdds = await fetchHotOddsPayload(env, raceKey);
  try {
    const [source, cachedTrackCondition] = await Promise.all([
      getRaceSource(env.REALTIME_DB, raceKey),
      readCachedTrackCondition(env, raceKey),
    ]);
    const trackCondition =
      cachedTrackCondition ?? (await getLatestTrackConditionForRace(env.REALTIME_DB, raceKey));
    return await buildRealtimePayload(env.REALTIME_DB, raceKey, source, hotOdds, trackCondition);
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "realtime-route",
      "error",
      raceKey,
      formatError(error),
      env.DETAIL_SECTION_CACHE_KV,
    ).catch(() => undefined);
    return buildDegradedRealtimePayload(raceKey, hotOdds);
  }
};

export const addDaysToYyyymmdd = (yyyymmdd: string, days: number): string => {
  const date = new Date(
    `${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`,
  );
  date.setUTCDate(date.getUTCDate() + days);
  return toJstIsoString(date).slice(0, 10).replace(/-/g, "");
};

const JRA_PREMIUM_LINK_CRONS = new Set(["0 4 * * 5", "0 4 * * 6"]);
const JRA_PREMIUM_DATA_CRONS = new Set(["0 5 * * 5", "0 5 * * 6"]);
// 03:30 JST (= 18:30 UTC) — off-peak slot for D1 retention sweeps.
const D1_RETENTION_CRON = "30 18 * * *";
// 20:05 JST (= 11:05 UTC) — nightly prep for next 1-3 days.
export const MULTI_DAY_PREP_CRON = "5 11 * * *";
// 09:10 JST (= 00:10 UTC) — morning fallback for today.
export const TODAY_BACKFILL_CRON = "10 0 * * *";
// Keep the shared FIFO inference Queue reserved for the next race day.  The
// previous +1/+2/+3 fan-out put later dates behind the same consumer; when a
// next-day cold Catalog query retried, its replacement was queued behind two
// full future race cards.  Preparing only +1 preserves the useful day-ahead
// warm-up without allowing later dates to delay tomorrow's predictions.
const MULTI_DAY_PREP_OFFSET_DAYS: readonly number[] = [1];
export const getCronJob = (cron: string, now = new Date()): Job => {
  const today = getTodayJst(now);
  if (JRA_PREMIUM_LINK_CRONS.has(cron)) {
    return { date: addDaysToYyyymmdd(today, 1), type: "discover-premium-race-links" };
  }
  if (JRA_PREMIUM_DATA_CRONS.has(cron)) {
    return { date: addDaysToYyyymmdd(today, 1), type: "plan-premium-race-data-fetches" };
  }
  if (cron === "5 0 * * *") {
    return { date: today, type: "discover-urls" };
  }
  return { date: today, type: "plan-realtime-fetches" };
};

const logRunningStylePlanResult = async (
  env: Env,
  scheduledAt: Date,
  ctx?: ExecutionContext,
): Promise<void> => {
  for (const date of resolveRunningStyleCronDates(scheduledAt)) {
    const materializeResult = await materializeRunningStyleFeatureParquetsForDate(env, date);
    await logFetch(
      env.REALTIME_DB,
      "materialize-running-style-features",
      resolveMaterializeLogStatus(materializeResult),
      null,
      JSON.stringify({ ...materializeResult, mode: "inference-cron" }),
    );
    if (materializeResult.materializeError !== undefined) {
      throw new Error(materializeResult.materializeError);
    }
  }
  await runRunningStyleCronTick(env, scheduledAt, ctx)
    .then((summary) =>
      logFetch(
        env.REALTIME_DB,
        "plan-running-style-predictions",
        "ok",
        null,
        JSON.stringify(summary),
      ),
    )
    .catch((error: unknown) =>
      logFetch(
        env.REALTIME_DB,
        "plan-running-style-predictions",
        "error",
        null,
        formatError(error),
      ),
    );
};

export const buildFallbackRaceRow = (
  targetDate: string,
  link: KeibaGoRaceLink,
  html: string,
): LocalRaceRow | null => {
  const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
  if (!keibajoCode) {
    return null;
  }
  const metadata = parseRaceMetadata(html);
  if (!metadata.startTime) {
    return null;
  }
  return {
    hasso_jikoku: metadata.startTime,
    kaisai_nen: targetDate.slice(0, 4),
    kaisai_tsukihi: targetDate.slice(4, 8),
    keibajo_code: keibajoCode,
    kyosomei_hondai: metadata.raceName,
    race_bango: link.raceNumber,
  };
};

const defaultDiscoverSleep = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms));

// Single attempt of `attempt(N)` with bounded exponential backoff retry on
// throw. Backoff = base * multiplier^(attempt-1) so default = 200ms / 800ms /
// 3200ms. Returns the outcome of the per-race upsert; never re-throws because
// the caller wants to continue processing the rest of the date's races.
const runUpsertWithRetry = async (args: RetryUpsertArgs): Promise<DiscoverUpsertOutcome> => {
  const attemptOnce = async (attempt: number): Promise<DiscoverUpsertOutcome> => {
    try {
      await args.attempt(attempt);
      return attempt === 1 ? "inserted" : "retried";
    } catch (error) {
      if (attempt >= DISCOVER_UPSERT_MAX_ATTEMPTS) {
        return "failed";
      }
      const delay =
        DISCOVER_UPSERT_BASE_DELAY_MS * DISCOVER_UPSERT_BACKOFF_MULTIPLIER ** (attempt - 1);
      // Log every retry so partial-progress is visible in tail -f.
      console.error(
        `discover-urls upsert retry raceKey=${args.raceKey} attempt=${attempt} error=${formatError(error)} nextDelayMs=${delay}`,
      );
      await args.sleep(delay);
      return attemptOnce(attempt + 1);
    }
  };
  return attemptOnce(1);
};

interface UpsertOneJraArgs {
  env: Env;
  race: LocalRaceRow;
  sleep: (ms: number) => Promise<void>;
}

const upsertOneJraRaceWithRetry = async (
  args: UpsertOneJraArgs,
): Promise<DiscoverUpsertResult | null> => {
  const entryUrl = buildJraEntryUrlFromRace(args.race);
  if (!entryUrl) {
    return null;
  }
  const raceBango = args.race.race_bango.padStart(2, "0");
  const jraRaceKey = buildRealtimeRaceKey(
    "jra",
    args.race.kaisai_nen,
    args.race.kaisai_tsukihi,
    args.race.keibajo_code,
    raceBango,
  );
  const outcome = await runUpsertWithRetry({
    attempt: async () => {
      await upsertJraRaceSource(args.env.REALTIME_DB, args.race, entryUrl);
    },
    raceKey: jraRaceKey,
    sleep: args.sleep,
  });
  if (outcome === "failed") {
    return { outcome, raceKey: jraRaceKey };
  }
  await forwardRaceSourceToHot(args.env, {
    debaUrl: entryUrl,
    kaisaiNen: args.race.kaisai_nen,
    kaisaiTsukihi: args.race.kaisai_tsukihi,
    keibajoCode: args.race.keibajo_code,
    oddsLinksJson: "{}",
    raceBango,
    raceKey: jraRaceKey,
    raceStartAtJst: buildRaceStartAtJst(
      args.race.kaisai_nen,
      args.race.kaisai_tsukihi,
      args.race.hasso_jikoku,
    ),
    source: "jra",
  });
  await forwardRaceForFeatures(args.env, {
    kaisaiNen: args.race.kaisai_nen,
    kaisaiTsukihi: args.race.kaisai_tsukihi,
    keibajoCode: args.race.keibajo_code,
    raceBango,
    raceKey: jraRaceKey,
    source: "jra",
  });
  return { outcome, raceKey: jraRaceKey };
};

interface UpsertOneNarArgs {
  env: Env;
  link: KeibaGoRaceLink;
  race: LocalRaceRow;
  keibajoCode: string;
  oddsLinks: Partial<Record<OddsType, string>>;
  sleep: (ms: number) => Promise<void>;
}

const upsertOneNarRaceWithRetry = async (args: UpsertOneNarArgs): Promise<DiscoverUpsertResult> => {
  const raceBango = args.race.race_bango.padStart(2, "0");
  const narRaceKey = buildRealtimeRaceKey(
    "nar",
    args.race.kaisai_nen,
    args.race.kaisai_tsukihi,
    args.keibajoCode,
    raceBango,
  );
  const outcome = await runUpsertWithRetry({
    attempt: async () => {
      await upsertNarRaceSource(args.env.REALTIME_DB, args.link, args.race, args.oddsLinks);
    },
    raceKey: narRaceKey,
    sleep: args.sleep,
  });
  if (outcome === "failed") {
    return { outcome, raceKey: narRaceKey };
  }
  await forwardRaceSourceToHot(args.env, {
    debaUrl: args.link.url,
    kaisaiNen: args.race.kaisai_nen,
    kaisaiTsukihi: args.race.kaisai_tsukihi,
    keibajoCode: args.keibajoCode,
    oddsLinksJson: JSON.stringify(args.oddsLinks),
    raceBango,
    raceKey: narRaceKey,
    raceStartAtJst: buildRaceStartAtJst(
      args.race.kaisai_nen,
      args.race.kaisai_tsukihi,
      args.race.hasso_jikoku,
    ),
    source: "nar",
  });
  await forwardRaceForFeatures(args.env, {
    kaisaiNen: args.race.kaisai_nen,
    kaisaiTsukihi: args.race.kaisai_tsukihi,
    keibajoCode: args.keibajoCode,
    raceBango,
    raceKey: narRaceKey,
    source: "nar",
  });
  return { outcome, raceKey: narRaceKey };
};

const accumulateOutcome = (
  counters: DiscoverUpsertCounters,
  result: DiscoverUpsertResult | null,
): DiscoverUpsertCounters => {
  if (!result) {
    return counters;
  }
  if (result.outcome === "inserted") {
    return { ...counters, inserted: counters.inserted + 1 };
  }
  if (result.outcome === "retried") {
    return { ...counters, retried: counters.retried + 1 };
  }
  const truncated = counters.failedRaceKeys.length < DISCOVER_UPSERT_FAILED_RACE_KEYS_MAX;
  return {
    ...counters,
    failed: counters.failed + 1,
    failedRaceKeys: truncated
      ? [...counters.failedRaceKeys, result.raceKey]
      : counters.failedRaceKeys,
  };
};

const INITIAL_DISCOVER_COUNTERS: DiscoverUpsertCounters = {
  failed: 0,
  failedRaceKeys: [],
  inserted: 0,
  retried: 0,
};

interface UpsertDiscoveredUrlsOptions {
  sleep: (ms: number) => Promise<void>;
}

export const upsertDiscoveredUrls = async (
  env: Env,
  targetDate: string,
  options: UpsertDiscoveredUrlsOptions,
): Promise<{
  fallbackRaceListCount: number;
  failed: number;
  failedRaceKeys: readonly string[];
  inserted: number;
  jraRaceCount: number;
  localRaceCount: number;
  retried: number;
  topRaceListCount: number;
  upserted: number;
}> => {
  const raceListUrls = await fetchTodayRaceListUrls(targetDate);
  const localRaces = await fetchNarRacesByDate(env, targetDate);
  const jraRaces = await fetchJraRacesByDate(env, targetDate);
  const fallbackRaceListUrls = Array.from(
    new Set(
      localRaces
        .map(
          (race) =>
            Object.entries(BABA_CODE_TO_LOCAL_KEIBAJO).find(
              ([, code]) => code === race.keibajo_code,
            )?.[0],
        )
        .filter((babaCode): babaCode is string => Boolean(babaCode)),
    ),
  ).map((babaCode) => buildRaceListUrl(targetDate, babaCode));
  const targetRaceListUrls = Array.from(
    new Map(
      [...raceListUrls, ...fallbackRaceListUrls].map((item) => [item.babaCode, item]),
    ).values(),
  );
  const localRaceMap = new Map(
    localRaces.map((race) => [
      buildRaceKey(race.kaisai_nen, race.kaisai_tsukihi, race.keibajo_code, race.race_bango),
      race,
    ]),
  );

  const jraResults: (DiscoverUpsertResult | null)[] = [];
  for (const race of jraRaces) {
    const result = await upsertOneJraRaceWithRetry({ env, race, sleep: options.sleep });
    jraResults.push(result);
  }
  const narResults: DiscoverUpsertResult[] = [];
  for (const raceList of targetRaceListUrls) {
    const links = await fetchRaceLinksFromRaceList(raceList.url);
    for (const link of links) {
      const keibajoCode = BABA_CODE_TO_LOCAL_KEIBAJO[link.babaCode];
      if (!keibajoCode) {
        continue;
      }
      const raceKey = buildRaceKey(
        targetDate.slice(0, 4),
        targetDate.slice(4, 8),
        keibajoCode,
        link.raceNumber,
      );
      const racePageHtml = await fetchRacePage(link.url);
      const race =
        localRaceMap.get(raceKey) ?? buildFallbackRaceRow(targetDate, link, racePageHtml);
      if (!race) {
        continue;
      }
      const oddsLinks = extractOddsLinks(racePageHtml, link.url);
      const result = await upsertOneNarRaceWithRetry({
        env,
        keibajoCode,
        link,
        oddsLinks,
        race,
        sleep: options.sleep,
      });
      narResults.push(result);
    }
  }
  const counters = [...jraResults, ...narResults].reduce(
    accumulateOutcome,
    INITIAL_DISCOVER_COUNTERS,
  );
  return {
    fallbackRaceListCount: fallbackRaceListUrls.length,
    failed: counters.failed,
    failedRaceKeys: counters.failedRaceKeys,
    inserted: counters.inserted,
    jraRaceCount: jraRaces.length,
    localRaceCount: localRaces.length,
    retried: counters.retried,
    topRaceListCount: raceListUrls.length,
    upserted: counters.inserted + counters.retried,
  };
};

const ensureJraRaceSourcesAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  const [d1RaceCount, missingRaceDateFieldCount] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    countJraRaceSourcesMissingRaceDateFieldsByDate(env.REALTIME_DB, targetDate),
  ]);
  const jraRaces = await fetchJraRacesByDate(env, targetDate);
  if (jraRaces.length === 0) {
    return;
  }
  if (d1RaceCount >= jraRaces.length && missingRaceDateFieldCount === 0) {
    const discoveredKeibajoCodes = new Set(
      await listRaceSourceKeibajoCodesByDate(env.REALTIME_DB, targetDate),
    );
    const expectedJraVenueCodes = Array.from(new Set(jraRaces.map((race) => race.keibajo_code)));
    if (expectedJraVenueCodes.every((keibajoCode) => discoveredKeibajoCodes.has(keibajoCode))) {
      return;
    }
  }
  for (const race of jraRaces) {
    const entryUrl = buildJraEntryUrlFromRace(race);
    if (!entryUrl) {
      continue;
    }
    await upsertJraRaceSource(env.REALTIME_DB, race, entryUrl);
    const raceBango = race.race_bango.padStart(2, "0");
    const jraRaceKey = buildRealtimeRaceKey(
      "jra",
      race.kaisai_nen,
      race.kaisai_tsukihi,
      race.keibajo_code,
      raceBango,
    );
    await forwardRaceSourceToHot(env, {
      debaUrl: entryUrl,
      kaisaiNen: race.kaisai_nen,
      kaisaiTsukihi: race.kaisai_tsukihi,
      keibajoCode: race.keibajo_code,
      oddsLinksJson: "{}",
      raceBango,
      raceKey: jraRaceKey,
      raceStartAtJst: buildRaceStartAtJst(race.kaisai_nen, race.kaisai_tsukihi, race.hasso_jikoku),
      source: "jra",
    });
    await forwardRaceForFeatures(env, {
      kaisaiNen: race.kaisai_nen,
      kaisaiTsukihi: race.kaisai_tsukihi,
      keibajoCode: race.keibajo_code,
      raceBango,
      raceKey: jraRaceKey,
      source: "jra",
    });
  }
};

const linkPremiumRacesFromHtml = async (
  env: Env,
  html: string,
  races: NarRaceSource[],
  config: ReturnType<typeof getPremiumRaceConfig>,
): Promise<number> => {
  const links = discoverPremiumRaceLinks(html, config);
  let linked = 0;
  for (const race of races.filter(isPremiumRaceDataTarget)) {
    const link = matchPremiumLinkToRace(links, race);
    if (!link) {
      continue;
    }
    await upsertPremiumRaceLink(env.REALTIME_DB, race.raceKey, link);
    linked += 1;
  }
  return linked;
};

const discoverPremiumRacesForDate = async (
  env: Env,
  targetDate: string,
): Promise<{ configured: boolean; discovered: number; linked: number }> => {
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config) || !config.topPathTemplate) {
    return { configured: false, discovered: 0, linked: 0 };
  }
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  let discovered = 0;
  let linked = 0;
  const topUrl = buildPremiumUrl(config, config.topPathTemplate, { date: targetDate });
  if (topUrl) {
    const html = await fetchPremiumHtml(config, topUrl);
    const links = discoverPremiumRaceLinks(html, config);
    discovered += links.length;
    linked += await linkPremiumRacesFromHtml(
      env,
      html,
      races.filter((race) => race.source === "jra"),
      config,
    );
  }
  if (config.narTopPathTemplate) {
    const narTopUrl = buildPremiumUrl(config, config.narTopPathTemplate, { date: targetDate });
    if (narTopUrl) {
      const html = await fetchPremiumHtml(config, narTopUrl);
      const links = discoverPremiumRaceLinks(html, config);
      discovered += links.length;
      linked += await linkPremiumRacesFromHtml(
        env,
        html,
        races.filter((race) => race.source === "nar"),
        config,
      );
    }
  }
  return { configured: true, discovered, linked };
};

const ensurePremiumRaceLink = async (
  env: Env,
  race: NarRaceSource,
): Promise<Awaited<ReturnType<typeof getPremiumRaceLink>>> => {
  const existing = await getPremiumRaceLink(env.REALTIME_DB, race.raceKey);
  if (existing) {
    return existing;
  }
  const targetDate = `${race.kaisaiNen}${race.kaisaiTsukihi}`;
  await discoverPremiumRacesForDate(env, targetDate);
  const discovered = await getPremiumRaceLink(env.REALTIME_DB, race.raceKey);
  if (discovered) {
    return discovered;
  }
  const fallbackLink = buildPremiumRaceLinkFromRace(race, getPremiumRaceConfig(env));
  if (!fallbackLink) {
    return null;
  }
  await upsertPremiumRaceLink(env.REALTIME_DB, race.raceKey, fallbackLink);
  return fallbackLink;
};

export const getRaceStart = (race: NarRaceSource): Date | null =>
  parseRaceStartJst(
    race.kaisaiNen,
    race.kaisaiTsukihi,
    race.raceStartAtJst.slice(11, 16).replace(":", ""),
  );

export const minutesUntilRace = (race: NarRaceSource, now = new Date()): number | null => {
  const raceStart = getRaceStart(race);
  if (!raceStart) {
    return null;
  }
  return (raceStart.getTime() - now.getTime()) / 60_000;
};

export const getNarVenueMeetingKey = (
  race: Pick<NarRaceSource, "kaisaiNen" | "kaisaiTsukihi" | "keibajoCode" | "source">,
): string => `${race.source}:${race.kaisaiNen}${race.kaisaiTsukihi}:${race.keibajoCode}`;

export const getNarVenueLastRaceStartAtMap = (races: NarRaceSource[]): Map<string, string> => {
  const result = new Map<string, string>();
  for (const race of races) {
    if (race.source !== "nar") {
      continue;
    }
    const key = getNarVenueMeetingKey(race);
    const current = result.get(key);
    if (!current || new Date(race.raceStartAtJst).getTime() > new Date(current).getTime()) {
      result.set(key, race.raceStartAtJst);
    }
  }
  return result;
};

export const getNarOddsSaleStartForRace = (
  race: NarRaceSource,
  venueLastRaceStartAtJst: string | null | undefined,
): Date | null => {
  if (race.source !== "nar") {
    return null;
  }
  return getNarOddsSaleStartAt({
    keibajoCode: race.keibajoCode,
    raceStartAtJst: race.raceStartAtJst,
    venueLastRaceStartAtJst,
  });
};

export const getCurrentOddsSlotAt = (
  race: NarRaceSource,
  now: Date,
  options: { venueLastRaceStartAtJst?: string | null } = {},
): string | null => {
  const raceStart = getRaceStart(race);
  if (!raceStart) {
    return null;
  }
  if (race.source === "jra") {
    return getJraAdvanceOddsFetchSlotAt(raceStart, now) ?? getOddsFetchSlotAt(raceStart, now);
  }
  return getNarOddsFetchSlotAt(
    raceStart,
    now,
    getNarOddsSaleStartForRace(race, options.venueLastRaceStartAtJst),
  );
};

export const isDue = (
  lastFetchedAt: string | null,
  intervalMinutes: number,
  now = new Date(),
): boolean => {
  if (!lastFetchedAt) {
    return true;
  }
  const last = new Date(lastFetchedAt).getTime();
  return Number.isNaN(last) || now.getTime() - last >= intervalMinutes * 60_000;
};

export const isSlotDue = (lastActivityAt: string | null, slotAt: string): boolean => {
  if (!lastActivityAt) {
    return true;
  }
  return new Date(lastActivityAt).getTime() < new Date(slotAt).getTime();
};

export const latestTimestamp = (...timestamps: (string | null)[]): string | null => {
  const latest = timestamps
    .map((timestamp) => (timestamp ? new Date(timestamp).getTime() : Number.NaN))
    .filter((timestamp) => !Number.isNaN(timestamp))
    .sort((left, right) => right - left)[0];
  return latest === undefined ? null : new Date(latest).toISOString();
};

export const isThreeMinuteTick = (date: Date): boolean => date.getUTCMinutes() % 3 === 0;

// JST yyyy-mm-dd slice from an ISO-with-offset string. Returns empty string
// when the input cannot be parsed so the caller can treat that as "no date
// match" and fall back to the 24h cooldown.
export const extractJstDate = (value: string | null): string => {
  if (!value) return "";
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? "" : toJstIsoString(parsed).slice(0, 10);
};

// 1h cooldown when the previous fetch landed on the same JST date as the
// race, 24h cooldown otherwise. The same-day path keeps retries flowing for
// any in-day failure (partial parse, transient origin error). The 24h path
// kicks in once we successfully fetched yesterday's weights so a stale row
// from a previous calendar day does not block today's first attempt.
// When `now` is provided and the race is within
// WEIGHT_FETCH_NEAR_RACE_THRESHOLD_MINUTES of post time (and not more than
// WEIGHT_FETCH_NEAR_RACE_POST_LIMIT_MINUTES after it), shorten the cooldown
// to WEIGHT_FETCH_NEAR_RACE_COOLDOWN_MINUTES so the watchdog has many fast
// retries before the race ends.
export interface WeightFetchCooldownInput {
  lastFetchAt: string | null;
  now?: Date;
  raceStartAtJst: string;
}

const isNearRace = (raceStartAtJst: string, now: Date): boolean => {
  const raceStartMs = new Date(raceStartAtJst).getTime();
  if (Number.isNaN(raceStartMs)) return false;
  const minutesUntil = (raceStartMs - now.getTime()) / MILLISECONDS_PER_MINUTE;
  return (
    minutesUntil < WEIGHT_FETCH_NEAR_RACE_THRESHOLD_MINUTES &&
    minutesUntil > -WEIGHT_FETCH_NEAR_RACE_POST_LIMIT_MINUTES
  );
};

export const resolveWeightFetchCooldownMinutes = (input: WeightFetchCooldownInput): number => {
  if (input.now && isNearRace(input.raceStartAtJst, input.now)) {
    return WEIGHT_FETCH_NEAR_RACE_COOLDOWN_MINUTES;
  }
  if (!input.lastFetchAt) return WEIGHT_FETCH_INTERVAL_MINUTES;
  const raceDate = input.raceStartAtJst.slice(0, 10);
  const fetchDate = extractJstDate(input.lastFetchAt);
  return raceDate && raceDate === fetchDate
    ? WEIGHT_FETCH_SAME_DAY_COOLDOWN_MINUTES
    : WEIGHT_FETCH_INTERVAL_MINUTES;
};

interface WeightRaceListKvEntry {
  raceKey: string;
  source: "jra" | "nar";
}

const buildWeightRaceListKvKey = (date: string): string => `${WEIGHT_RACE_LIST_KV_PREFIX}${date}`;

// KV-backed fallback so a Hyperdrive read timeout (which surfaces as an empty
// SchedulableRaceSource list) does not silently skip weight planning. We write
// the minimal {raceKey, source} list on every successful plan and read it back
// when the live query returns empty.
export const readWeightRaceListFallbackFromKv = async (
  env: Env,
  date: string,
): Promise<WeightRaceListKvEntry[]> => {
  if (!env.DETAIL_SECTION_CACHE_KV) return [];
  const raw = await env.DETAIL_SECTION_CACHE_KV.get(buildWeightRaceListKvKey(date));
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? (parsed as WeightRaceListKvEntry[]) : [];
  } catch {
    return [];
  }
};

export const writeWeightRaceListFallbackToKv = async (
  env: Env,
  date: string,
  entries: WeightRaceListKvEntry[],
): Promise<void> => {
  if (!env.DETAIL_SECTION_CACHE_KV) return;
  await env.DETAIL_SECTION_CACHE_KV.put(buildWeightRaceListKvKey(date), JSON.stringify(entries), {
    expirationTtl: WEIGHT_RACE_LIST_KV_TTL_SECONDS,
  });
};

export interface StaleWeightFetchRace {
  lastWeightFetchAt: string | null;
  lastWeightFetchAttemptAt: string | null;
  lastWeightFetchSoftMissAt: string | null;
  raceKey: string;
  raceStartAtJst: string;
}

interface PredictionReadinessRace {
  preWeight: { complete: boolean };
  raceKey: string;
}

const predictionReadinessRaceKey = (realtimeRaceKey: string): string | null => {
  const parts = realtimeRaceKey.split(":");
  if (parts.length !== RACE_KEY_PART_COUNT) return null;
  const source = parts[0];
  const keibajoCode = parts[3];
  const raceBango = parts[4];
  if (!source || !keibajoCode || !raceBango) return null;
  return `${source}:${keibajoCode.padStart(2, "0")}:${raceBango.padStart(2, "0")}`;
};

const parsePredictionReadinessRaces = (value: unknown): readonly PredictionReadinessRace[] => {
  if (!isObjectRecord(value) || !Array.isArray(value.races)) {
    throw new Error("finish-position prediction readiness returned an invalid envelope");
  }
  return value.races.map((race): PredictionReadinessRace => {
    if (
      !isObjectRecord(race) ||
      typeof race.raceKey !== "string" ||
      !isObjectRecord(race.preWeight) ||
      typeof race.preWeight.complete !== "boolean"
    ) {
      throw new Error("finish-position prediction readiness returned an invalid race");
    }
    return { preWeight: { complete: race.preWeight.complete }, raceKey: race.raceKey };
  });
};

// Horse-weight acquisition is the next stage after the initial prediction,
// not an independent clock-only lane. Query the finish-position Worker once
// per race day over the existing Service Binding and admit only races whose
// complete initial generation is present in both Neon and the display KV.
// Missing bindings, auth, malformed responses, and upstream failures throw so
// the watchdog fails closed and retries on its next cron instead of violating
// the pipeline order.
export const filterPreweightReadyWeightRaces = async (
  env: Env,
  races: readonly StaleWeightFetchRace[],
): Promise<readonly StaleWeightFetchRace[]> => {
  if (races.length === 0) return [];
  const binding = env.FINISH_POSITION_CRON;
  const token = env.TRIGGER_TOKEN;
  if (!binding) throw new Error("FINISH_POSITION_CRON binding is required for weight readiness");
  if (!token) throw new Error("TRIGGER_TOKEN is required for weight readiness");
  const runYmds = [...new Set(races.map((race) => raceKeyDateYmd(race.raceKey)))];
  if (runYmds.some((runYmd) => runYmd === null)) {
    throw new Error("weight readiness received an invalid race key");
  }
  const validRunYmds = runYmds.filter((runYmd): runYmd is string => runYmd !== null);
  const readyKeys = new Set<string>();
  await Promise.all(
    validRunYmds.map(async (runYmd) => {
      const url = new URL(FINISH_POSITION_CRON_INTERNAL_READINESS_URL);
      url.searchParams.set("runYmd", runYmd);
      const response = await binding.fetch(
        new Request(url, { headers: { Authorization: `Bearer ${token}` } }),
      );
      if (!response.ok) {
        throw new Error(`finish-position prediction readiness failed with HTTP ${response.status}`);
      }
      const readiness = parsePredictionReadinessRaces(await response.json());
      for (const race of readiness) {
        if (race.preWeight.complete) readyKeys.add(race.raceKey);
      }
    }),
  );
  return races.filter((race) => {
    const key = predictionReadinessRaceKey(race.raceKey);
    return key !== null && readyKeys.has(key);
  });
};

interface StaleWeightFetchRaceRow {
  last_weight_fetch_at: string | null;
  last_weight_fetch_attempt_at: string | null;
  last_weight_fetch_soft_miss_at: string | null;
  race_key: string;
  race_start_at_jst: string;
}

const selectInlineRealtimeJobs = <T extends { raceKey: string }>(
  jobs: readonly T[],
  limits: { jra: number; nar: number },
): T[] => [
  ...jobs.filter((candidate) => candidate.raceKey.startsWith("jra:")).slice(0, limits.jra),
  ...jobs.filter((candidate) => candidate.raceKey.startsWith("nar:")).slice(0, limits.nar),
];

const buildTanshoOddsRowsFromHotOdds = (
  hotOdds: HotOddsPayload | null,
): RaceResultTanshoOddsRow[] =>
  (hotOdds?.latest.tansho ?? [])
    .map((row): RaceResultTanshoOddsRow | null => {
      if (row.odds === undefined || row.rank === undefined) return null;
      if (!Number.isFinite(row.odds) || row.odds <= 0) return null;
      if (!Number.isFinite(row.rank) || row.rank <= 0) return null;
      const horseNumber = row.combination.trim();
      if (horseNumber.length === 0) return null;
      return { horseNumber, popularity: row.rank, tanshoOdds: row.odds };
    })
    .filter((row): row is RaceResultTanshoOddsRow => row !== null);

// Direct D1 query for races whose post time falls inside the watchdog
// lookahead window and whose last weight fetch is null. Keeps the watchdog
// independent of the heavier
// plan-realtime-fetches code path so a circuit-breaker open state does not
// silently skip weight enqueueing.
export const findStaleWeightFetchRaces = async (
  db: D1Database,
  now: Date,
): Promise<readonly StaleWeightFetchRace[]> => {
  // race_start_at_jst / last_weight_fetch_at are stored as JST strings such as
  // "2026-06-13T11:05:00+09:00". D1 (SQLite) compares strings lexically, so
  // these bounds MUST also be JST strings. UTC ISO strings from `toISOString()`
  // (e.g. "2026-06-13T02:05:00.000Z") would sort wrongly against JST values:
  // the lex compare hits at position 11, where JST hour "1" > UTC hour "0",
  // making the watchdog never see today's stale rows.
  const lookAheadJst = toJstIsoString(
    new Date(now.getTime() + WEIGHT_WATCHDOG_LOOKAHEAD_MINUTES * MILLISECONDS_PER_MINUTE),
  );
  const lookBackJst = toJstIsoString(
    new Date(now.getTime() - WEIGHT_WATCHDOG_LOOKBACK_MINUTES * MILLISECONDS_PER_MINUTE),
  );
  const nearRaceJst = toJstIsoString(
    new Date(now.getTime() + WEIGHT_WATCHDOG_NEAR_RACE_THRESHOLD_MINUTES * MILLISECONDS_PER_MINUTE),
  );
  const adaptiveRaceJst = toJstIsoString(
    new Date(
      now.getTime() + WEIGHT_WATCHDOG_ADAPTIVE_RACE_THRESHOLD_MINUTES * MILLISECONDS_PER_MINUTE,
    ),
  );
  const adaptiveAttemptBackoffJst = toJstIsoString(
    new Date(
      now.getTime() - WEIGHT_WATCHDOG_ADAPTIVE_ATTEMPT_BACKOFF_MINUTES * MILLISECONDS_PER_MINUTE,
    ),
  );
  const farAttemptBackoffJst = toJstIsoString(
    new Date(now.getTime() - WEIGHT_WATCHDOG_FAR_ATTEMPT_BACKOFF_MINUTES * MILLISECONDS_PER_MINUTE),
  );
  const nearAttemptBackoffJst = toJstIsoString(
    new Date(
      now.getTime() - WEIGHT_WATCHDOG_NEAR_ATTEMPT_BACKOFF_MINUTES * MILLISECONDS_PER_MINUTE,
    ),
  );
  const result = await db
    .prepare(
      `
        select race_key, race_start_at_jst, last_weight_fetch_at,
          last_weight_fetch_attempt_at, last_weight_fetch_soft_miss_at
        from realtime_race_sources
        where race_start_at_jst > ?
          and race_start_at_jst <= ?
          and last_weight_fetch_at is null
          and (
            last_weight_fetch_attempt_at is null
            or last_weight_fetch_attempt_at < case
              when last_weight_fetch_soft_miss_at = last_weight_fetch_attempt_at
                and race_start_at_jst <= ? then ?
              when race_start_at_jst <= ? then ?
              else ?
            end
          )
        order by race_start_at_jst
        limit ?
      `,
    )
    .bind(
      lookBackJst,
      lookAheadJst,
      adaptiveRaceJst,
      adaptiveAttemptBackoffJst,
      nearRaceJst,
      nearAttemptBackoffJst,
      farAttemptBackoffJst,
      WEIGHT_WATCHDOG_MAX_PER_TICK,
    )
    .all<StaleWeightFetchRaceRow>();
  return result.results.map((row) => ({
    lastWeightFetchAt: row.last_weight_fetch_at,
    lastWeightFetchAttemptAt: row.last_weight_fetch_attempt_at,
    lastWeightFetchSoftMissAt: row.last_weight_fetch_soft_miss_at ?? null,
    raceKey: row.race_key,
    raceStartAtJst: row.race_start_at_jst,
  }));
};

// Dedicated weight watchdog tick. Runs every two minutes as the only automatic
// horse-weight scheduler. The watchdog only touches a single D1 read and
// the queue, so a Hyperdrive saturation that opens the planner circuit
// breaker still leaves weight fetches flowing here.
//
// 2026-06-28 (D1 cost optimization): every logFetch call here passes the
// shared KV namespace so storage.shouldSkipFetchLog dedupes identical rows
// (same jobType + status + raceKey + message hash) within
// default 60-second window. The quiet "no stale weight races" heartbeat uses
// a dedicated one-hour TTL, while enqueued counts and errors retain the
// default TTL so distinct operational outcomes remain visible immediately.
// Each candidate is
// atomically reserved in D1 before enqueueing, which prevents a queue backlog
// from allowing the next watchdog tick to enqueue the same race again.
export const runWeightWatchdog = async (env: Env, now: Date): Promise<void> => {
  try {
    const stale = await findStaleWeightFetchRaces(env.REALTIME_DB, now);
    if (stale.length === 0) {
      await logFetch(
        env.REALTIME_DB,
        "weight-watchdog",
        "ok",
        null,
        "no stale weight races",
        env.DETAIL_SECTION_CACHE_KV,
        WEIGHT_WATCHDOG_HEARTBEAT_DEDUPE_TTL_SECONDS,
      );
      return;
    }
    const ready = await filterPreweightReadyWeightRaces(env, stale);
    if (ready.length === 0) {
      await logFetch(
        env.REALTIME_DB,
        "weight-watchdog",
        "ok",
        null,
        JSON.stringify({ deferredUntilPreweight: stale.length, enqueued: 0 }),
        env.DETAIL_SECTION_CACHE_KV,
      );
      return;
    }
    const reservedAt = toJstIsoString(now);
    const reservations = await Promise.all(
      ready.map(async (race): Promise<FetchWeightsJobShape | null> => {
        const minutesUntil =
          (new Date(race.raceStartAtJst).getTime() - now.getTime()) / MILLISECONDS_PER_MINUTE;
        const backoffMinutes =
          race.lastWeightFetchSoftMissAt === race.lastWeightFetchAttemptAt &&
          race.lastWeightFetchSoftMissAt !== null &&
          minutesUntil <= WEIGHT_WATCHDOG_ADAPTIVE_RACE_THRESHOLD_MINUTES
            ? WEIGHT_WATCHDOG_ADAPTIVE_ATTEMPT_BACKOFF_MINUTES
            : minutesUntil <= WEIGHT_WATCHDOG_NEAR_RACE_THRESHOLD_MINUTES
              ? WEIGHT_WATCHDOG_NEAR_ATTEMPT_BACKOFF_MINUTES
              : WEIGHT_WATCHDOG_FAR_ATTEMPT_BACKOFF_MINUTES;
        const retryBefore = toJstIsoString(
          new Date(now.getTime() - backoffMinutes * MILLISECONDS_PER_MINUTE),
        );
        const claimed = await claimWeightFetch(
          env.REALTIME_DB,
          race.raceKey,
          reservedAt,
          retryBefore,
        );
        return claimed
          ? { raceKey: race.raceKey, type: "fetch-weights", watchdogReservedAt: reservedAt }
          : null;
      }),
    );
    const jobs = reservations.filter((job): job is FetchWeightsJobShape => job !== null);
    await enqueueJobs(env, jobs);
    await logFetch(
      env.REALTIME_DB,
      "weight-watchdog",
      "ok",
      null,
      JSON.stringify({ enqueued: jobs.length }),
      env.DETAIL_SECTION_CACHE_KV,
    );
  } catch (error: unknown) {
    await logFetch(
      env.REALTIME_DB,
      "weight-watchdog",
      "error",
      null,
      formatError(error),
      env.DETAIL_SECTION_CACHE_KV,
    );
  }
};

const isPriorityJraVenue = (keibajoCode: string): boolean =>
  JRA_PRIORITY_VENUE_CODES.includes(keibajoCode);

const isPriorityRaceBango = (input: WeightFetchPriorityInput): boolean =>
  input.source === "jra" &&
  isPriorityJraVenue(input.keibajoCode) &&
  JRA_PRIORITY_RACE_BANGOS.includes(input.raceBango);

const isLateJraRace = (input: WeightFetchPriorityInput): boolean =>
  input.source === "jra" &&
  Number.parseInt(input.raceBango, 10) >= WEIGHT_FETCH_BANGO_PRIORITY_THRESHOLD;

export const weightFetchPriorityTier = (input: WeightFetchPriorityInput): number => {
  if (input.source !== "jra") return WEIGHT_FETCH_PRIORITY_TIER_NAR;
  if (isPriorityRaceBango(input)) return WEIGHT_FETCH_PRIORITY_TIER_HIGH;
  if (isLateJraRace(input)) return WEIGHT_FETCH_PRIORITY_TIER_MID;
  return WEIGHT_FETCH_PRIORITY_TIER_LOW;
};

export const compareWeightCandidates = (a: WeightCandidate, b: WeightCandidate): number => {
  const ta = weightFetchPriorityTier({
    source: a.race.source,
    keibajoCode: a.race.keibajoCode,
    raceBango: a.race.raceBango,
  });
  const tb = weightFetchPriorityTier({
    source: b.race.source,
    keibajoCode: b.race.keibajoCode,
    raceBango: b.race.raceBango,
  });
  if (ta !== tb) return ta - tb;
  return a.minutes - b.minutes;
};

export const isPremiumRaceDiscoveryTick = (date: Date): boolean => {
  const hour = Number(getJstDateParts(date).hour);
  return PREMIUM_RACE_DISCOVERY_HOURS_JST.includes(hour);
};

// 2026-06-07: D1 retry-loop saturation guard. plan-realtime-fetches is the
// fan-out job that the queue auto-retried when D1 was throttled, which in
// turn re-fired the entire fan-out and amplified the load instead of letting
// D1 cool down. These helpers let both the cron path and the queue path
// short-circuit until the breaker expires.
export const isD1OverloadError = (error: unknown): boolean => {
  if (!(error instanceof Error)) return false;
  return D1_OVERLOAD_MARKERS.some((marker) => error.message.includes(marker));
};

export const isConnectionPressureError = (error: unknown): boolean => {
  if (!(error instanceof Error) || isD1OverloadError(error)) return false;
  const message = error.message.toLowerCase();
  return CONNECTION_PRESSURE_MARKERS.some((marker) => message.includes(marker));
};

export const isPlanRealtimeCircuitBreakerOpen = async (env: Env): Promise<boolean> => {
  if (!env.DETAIL_SECTION_CACHE_KV) return false;
  const value = await env.DETAIL_SECTION_CACHE_KV.get(PLAN_REALTIME_CIRCUIT_BREAKER_KV_KEY);
  return value === PLAN_REALTIME_CIRCUIT_BREAKER_KV_VALUE;
};

export const tripPlanRealtimeCircuitBreaker = async (env: Env): Promise<void> => {
  if (!env.DETAIL_SECTION_CACHE_KV) return;
  await env.DETAIL_SECTION_CACHE_KV.put(
    PLAN_REALTIME_CIRCUIT_BREAKER_KV_KEY,
    PLAN_REALTIME_CIRCUIT_BREAKER_KV_VALUE,
    { expirationTtl: PLAN_REALTIME_CIRCUIT_BREAKER_TTL_SECONDS },
  );
};

export const buildPlanRealtimeOverloadRetryDelaySeconds = (): number =>
  PLAN_REALTIME_OVERLOAD_RETRY_DELAY_BASE_SECONDS +
  Math.floor(Math.random() * PLAN_REALTIME_OVERLOAD_RETRY_DELAY_JITTER_SECONDS);

export const buildConnectionPressureRecoveryDelaySeconds = (): number =>
  CONNECTION_PRESSURE_RECOVERY_DELAY_BASE_SECONDS +
  Math.floor(Math.random() * CONNECTION_PRESSURE_RECOVERY_DELAY_JITTER_SECONDS);

const getLatestSuccessfulRealtimePlanAt = async (env: Env): Promise<string | null> => {
  const row = await env.REALTIME_DB.prepare(
    `
      select created_at
      from fetch_logs
      where job_type in ('plan-realtime-fetches', 'plan-realtime-fetches-self')
        and status = 'ok'
      order by created_at desc
      limit 1
    `,
  ).first<{ created_at: string }>();
  return row?.created_at ?? null;
};

const enqueueSelfRealtimePlanIfStale = async (
  env: Env,
  date: string,
  options: RealtimePlanRecoveryOptions,
): Promise<void> => {
  if (!isJstPollingWindow(options.now)) {
    return;
  }
  if (await isPlanRealtimeCircuitBreakerOpen(env)) {
    return;
  }
  const latest = await getLatestSuccessfulRealtimePlanAt(env);
  if (
    latest &&
    new Date(latest).getTime() >
      options.now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS * 1000
  ) {
    return;
  }
  const claimKey = `${REALTIME_PLAN_RECOVERY_CLAIM_KEY_PREFIX}:${date}`;
  const ownerToken = crypto.randomUUID();
  const claimedAt = toJstIsoString(options.now);
  const expiresAt = toJstIsoString(
    new Date(options.now.getTime() + REALTIME_PLAN_RECOVERY_CLAIM_TTL_SECONDS * 1000),
  );
  const claimed = await claimRealtimePlanRecovery({
    claimedAt,
    claimKey,
    db: env.REALTIME_DB,
    expiresAt,
    ownerToken,
  });
  if (!claimed) {
    return;
  }
  try {
    await env.REALTIME_JOBS.send(
      { date, selfSchedule: true, type: "plan-realtime-fetches" },
      { delaySeconds: options.delaySeconds },
    );
  } catch (error) {
    await releaseRealtimePlanRecovery({
      claimKey,
      db: env.REALTIME_DB,
      ownerToken,
    }).catch((releaseError: unknown) =>
      console.error(
        formatErrorLogLine(
          "Realtime planner recovery claim rollback failed",
          { claimKey, stage: "recovery-claim.rollback" },
          releaseError,
        ),
      ),
    );
    throw error;
  }
  await logFetch(env.REALTIME_DB, "plan-realtime-fetches-self", "queued", null, date);
  await logFetch(
    env.REALTIME_DB,
    "plan-realtime-fetches-recovery",
    "queued",
    null,
    JSON.stringify({
      delaySeconds: options.delaySeconds,
      failureClass: options.failureClass,
      stage: options.stage,
    }),
    env.DETAIL_SECTION_CACHE_KV,
  );
};

const runScheduledRealtimePlanWithRecovery = async (env: Env, date: string): Promise<void> => {
  try {
    await handleJob(env, { date, type: "plan-realtime-fetches" });
  } catch (error) {
    try {
      await enqueueSelfRealtimePlanIfStale(env, date, {
        delaySeconds: isConnectionPressureError(error)
          ? buildConnectionPressureRecoveryDelaySeconds()
          : REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS,
        failureClass: isConnectionPressureError(error) ? "connection_pressure" : "other",
        now: getNow(env),
        stage: "scheduled.enqueue-single-recovery",
      });
    } catch (recoveryError) {
      console.error(
        formatErrorLogLine(
          "Scheduled realtime planner self-recovery failed",
          { date },
          recoveryError,
        ),
      );
    }
    throw error;
  }
};

export const getJstDayStart = (targetDate: string): Date =>
  new Date(
    `${targetDate.slice(0, 4)}-${targetDate.slice(4, 6)}-${targetDate.slice(6, 8)}T00:00:00+09:00`,
  );

export const toJstSlotIso = (targetDate: string, hhmm: string): string =>
  `${targetDate.slice(0, 4)}-${targetDate.slice(4, 6)}-${targetDate.slice(6, 8)}T${hhmm.slice(0, 2)}:${hhmm.slice(2, 4)}:00+09:00`;

export const floorToHalfHourJstSlot = (now: Date): string => {
  const current = toJstIsoString(now);
  const minute = Number(current.slice(14, 16));
  const flooredMinute = minute >= 30 ? "30" : "00";
  return `${current.slice(0, 14)}${flooredMinute}:00+09:00`;
};

export const isTrackConditionDue = (
  schedule: {
    firstRaceStartAtJst: string;
    lastFetchAt: string | null;
    lastQueuedAt: string | null;
    lastRaceStartAtJst: string;
  },
  targetDate: string,
  now: Date,
): { due: boolean; slotAt: string | null } => {
  const today = getTodayJst(now);
  const dayBefore = addDaysToYyyymmdd(targetDate, -1);
  const nowMs = now.getTime();
  const lastActivity = latestTimestamp(schedule.lastFetchAt, schedule.lastQueuedAt);

  if (today === dayBefore) {
    const slotAt = toJstSlotIso(dayBefore, "1000");
    const dayBeforeSlot = new Date(getJstDayStart(targetDate).getTime() - 14 * 60 * 60_000);
    return {
      due: nowMs >= dayBeforeSlot.getTime() && isSlotDue(lastActivity, slotAt),
      slotAt,
    };
  }

  if (today !== targetDate) {
    return { due: false, slotAt: null };
  }

  if (nowMs < new Date(schedule.firstRaceStartAtJst).getTime()) {
    const slotAt = ["0600", "0700", "0900"]
      .map((hhmm) => toJstSlotIso(targetDate, hhmm))
      .filter((candidate) => nowMs >= new Date(candidate).getTime())
      .toSorted((left, right) => new Date(right).getTime() - new Date(left).getTime())[0];
    if (slotAt) {
      return { due: isSlotDue(lastActivity, slotAt), slotAt };
    }
  }

  const firstRaceMs = new Date(schedule.firstRaceStartAtJst).getTime();
  const lastRaceMs = new Date(schedule.lastRaceStartAtJst).getTime();
  if (nowMs >= firstRaceMs && nowMs <= lastRaceMs) {
    const slotAt = floorToHalfHourJstSlot(now);
    return { due: isSlotDue(lastActivity, slotAt), slotAt };
  }

  return { due: false, slotAt: null };
};

export const isRaceFinished = (race: NarRaceSource, now: Date): boolean => {
  const minutes = minutesUntilRace(race, now);
  return minutes !== null && minutes <= 0;
};

const ensureDiscoveredUrlsAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  const d1RaceCount = await countRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const localRaces = await fetchNarRacesByDate(env, targetDate);
  const jraRaces = await fetchJraRacesByDate(env, targetDate);
  const raceListUrls = await fetchTodayRaceListUrls(targetDate);
  const expectedKeibajoCodes = raceListUrls
    .map((raceList) => BABA_CODE_TO_LOCAL_KEIBAJO[raceList.babaCode])
    .filter((keibajoCode): keibajoCode is string => Boolean(keibajoCode));
  const discoveredKeibajoCodes = new Set(
    await listRaceSourceKeibajoCodesByDate(env.REALTIME_DB, targetDate),
  );
  const hasAllExpectedKeibajoCodes = expectedKeibajoCodes.every((keibajoCode) =>
    discoveredKeibajoCodes.has(keibajoCode),
  );
  if (d1RaceCount >= localRaces.length + jraRaces.length && hasAllExpectedKeibajoCodes) {
    return;
  }
  await upsertDiscoveredUrls(env, targetDate, { sleep: defaultDiscoverSleep });
};

export const enqueueJobs = async (env: Env, jobs: Job[]): Promise<void> => {
  const premiumDelaySeconds = Math.max(
    1,
    Number(env.PREMIUM_RACE_QUEUE_DELAY_SECONDS ?? DEFAULT_PREMIUM_RACE_QUEUE_DELAY_SECONDS),
  );
  const orderedJobs = jobs.toSorted((left, right) => {
    if (left.type === "fetch-premium-paddock" && right.type !== "fetch-premium-paddock") {
      return -1;
    }
    if (left.type !== "fetch-premium-paddock" && right.type === "fetch-premium-paddock") {
      return 1;
    }
    return 0;
  });
  let premiumJobIndex = 0;
  for (let index = 0; index < orderedJobs.length; index += QUEUE_SEND_BATCH_SIZE) {
    const chunk = orderedJobs.slice(index, index + QUEUE_SEND_BATCH_SIZE);
    if (chunk.some(isPremiumRaceJob)) {
      for (const job of chunk) {
        if (isPremiumRaceJob(job)) {
          await (env.PREMIUM_RACE_JOBS ?? env.REALTIME_JOBS).send(job, {
            delaySeconds:
              job.type === "fetch-premium-paddock"
                ? premiumJobIndex
                : premiumJobIndex * premiumDelaySeconds,
          });
          premiumJobIndex += 1;
        } else {
          await env.REALTIME_JOBS.send(job);
        }
      }
      continue;
    }
    if (chunk.length === 1) {
      await env.REALTIME_JOBS.send(chunk[0] as Job);
      continue;
    }
    await env.REALTIME_JOBS.sendBatch(chunk.map((body) => ({ body })));
  }
};

export const isPremiumRaceJob = (job: Job): boolean =>
  job.type === "discover-premium-race-links" ||
  job.type === "discover-premium-races" ||
  job.type === "plan-premium-race-data-fetches" ||
  job.type === "fetch-premium-race-data" ||
  job.type === "fetch-premium-paddock";

export const planTrackConditionFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const schedules = await listJraVenueTrackConditionSchedulesByDate(env.REALTIME_DB, targetDate);
  return schedules.flatMap((schedule) => {
    const due = isTrackConditionDue(schedule, targetDate, now);
    return due.due
      ? [{ date: targetDate, keibajoCode: schedule.keibajoCode, type: "fetch-jra-track-condition" }]
      : [];
  });
};

export const planPremiumPaddockFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  if (!hasPremiumRaceFetchConfig(getPremiumRaceConfig(env))) {
    return [];
  }
  await ensureJraRaceSourcesAreCurrent(env, targetDate);
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const jobs: Job[] = [];
  for (const race of races) {
    if (race.source !== "jra") {
      continue;
    }
    const minutes = minutesUntilRace(race, now);
    if (minutes === null || minutes > PREMIUM_PADDOCK_WINDOW_BEFORE_MINUTES) {
      continue;
    }
    const state = await getPremiumPaddockFetchState(env.REALTIME_DB, race.raceKey);
    if (minutes < -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) {
      continue;
    }
    if (state?.retryAfter && new Date(state.retryAfter).getTime() > now.getTime()) {
      continue;
    }
    if (
      state?.lastQueuedAt &&
      new Date(state.lastQueuedAt).getTime() >
        now.getTime() - PREMIUM_PADDOCK_RECHECK_MINUTES * 60_000
    ) {
      continue;
    }
    if (
      state?.lastFetchAt &&
      new Date(state.lastFetchAt).getTime() >
        now.getTime() - PREMIUM_PADDOCK_RECHECK_MINUTES * 60_000
    ) {
      continue;
    }
    jobs.push({ raceKey: race.raceKey, type: "fetch-premium-paddock" });
  }
  return jobs;
};

export const planPremiumRaceDataFetchesForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
): Promise<Job[]> => {
  if (!hasPremiumRaceFetchConfig(getPremiumRaceConfig(env))) {
    return [];
  }
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  await Promise.all(
    races.filter(isPremiumRaceDataTarget).map((race) => ensurePremiumRaceLink(env, race)),
  );
  const candidates = await listPremiumRaceDataFetchCandidatesByDate(
    env.REALTIME_DB,
    targetDate,
    toJstIsoString(now),
  );
  return candidates.map((candidate) => ({
    raceKey: candidate.raceKey,
    type: "fetch-premium-race-data",
  }));
};

const tryEnsureDiscoveredUrlsAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  try {
    await ensureDiscoveredUrlsAreCurrent(env, targetDate);
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "discover-urls",
      "error",
      null,
      formatError(error),
      env.DETAIL_SECTION_CACHE_KV,
    );
  }
};

const tryDiscoverUrlsForDate = async (env: Env, targetDate: string, mode: string) => {
  try {
    const result = await upsertDiscoveredUrls(env, targetDate, { sleep: defaultDiscoverSleep });
    await logFetch(
      env.REALTIME_DB,
      "discover-urls",
      "ok",
      null,
      JSON.stringify({ ...result, mode }),
    );
    return result;
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "discover-urls",
      "error",
      null,
      formatError(error),
      env.DETAIL_SECTION_CACHE_KV,
    );
    return null;
  }
};

const runMaterializeWhenReady = async (env: Env, targetDate: string) =>
  materializeRunningStyleFeatureParquetsForDate(env, targetDate);

const resolveMaterializeLogStatus = (result: {
  materializeError?: string;
  scanned: number;
}): string => {
  if (result.materializeError === undefined) return "ok";
  if (result.scanned === 0) return "skipped";
  return "error";
};

const prewarmRunningStylePredictionsForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
  ctx?: ExecutionContext,
) => {
  const discoveryResult = await tryDiscoverUrlsForDate(env, targetDate, "running-style-prewarm");
  const materializeResult = await runMaterializeWhenReady(env, targetDate);
  await logFetch(
    env.REALTIME_DB,
    "materialize-running-style-features",
    resolveMaterializeLogStatus(materializeResult),
    null,
    JSON.stringify({ ...materializeResult, mode: "prewarm" }),
  );
  if (materializeResult.materializeError !== undefined) {
    throw new Error(materializeResult.materializeError);
  }
  const runningStyleResult = await planRunningStylePredictionsForDate(env, targetDate, now);
  const cacheRefreshResult = await refreshViewerRunningStyleCachesForDate(env, targetDate, ctx);
  await logFetch(
    env.REALTIME_DB,
    "plan-running-style-predictions",
    "ok",
    null,
    JSON.stringify({ ...runningStyleResult, cacheRefresh: cacheRefreshResult, mode: "prewarm" }),
  );
  return {
    cacheRefresh: cacheRefreshResult,
    date: targetDate,
    discovery: discoveryResult,
    materialize: materializeResult,
    runningStyle: runningStyleResult,
  };
};

const prewarmRaceDataForDate = async (
  env: Env,
  targetDate: string,
  now: Date,
  ctx?: ExecutionContext,
  mode = "scheduled-prep",
) => {
  const discoveryResult = await tryDiscoverUrlsForDate(env, targetDate, mode);
  const materializeResult = await runMaterializeWhenReady(env, targetDate);
  await logFetch(
    env.REALTIME_DB,
    "materialize-running-style-features",
    resolveMaterializeLogStatus(materializeResult),
    null,
    JSON.stringify({ ...materializeResult, mode }),
  );
  if (materializeResult.materializeError !== undefined) {
    throw new Error(materializeResult.materializeError);
  }
  const runningStyleResult = await planRunningStylePredictionsForDate(env, targetDate, now);
  const cacheRefreshResult = await refreshViewerRunningStyleCachesForDate(env, targetDate, ctx);
  await logFetch(
    env.REALTIME_DB,
    "plan-running-style-predictions",
    "ok",
    null,
    JSON.stringify({
      ...runningStyleResult,
      cacheRefresh: cacheRefreshResult,
      mode,
      target: targetDate,
    }),
  );
  return {
    cacheRefresh: cacheRefreshResult,
    date: targetDate,
    discovery: discoveryResult,
    materialize: materializeResult,
    runningStyle: runningStyleResult,
  };
};

const prewarmRaceDataForDates = async (
  env: Env,
  dates: ReadonlyArray<string>,
  now: Date,
  ctx?: ExecutionContext,
  mode = "scheduled-prep",
): Promise<void> => {
  for (const date of dates) {
    await prewarmRaceDataForDate(env, date, now, ctx, mode).catch((error: unknown) =>
      logFetch(
        env.REALTIME_DB,
        "plan-running-style-predictions",
        "error",
        null,
        formatError(error),
      ),
    );
  }
};

export const truncate = (value: string, maxLength: number): string =>
  value.length <= maxLength ? value : `${value.slice(0, Math.max(0, maxLength - 1))}…`;

export const buildPremiumPaddockSignature = async (
  bulletins: readonly PremiumPaddockBulletin[],
): Promise<string> => {
  const signaturePayload = {
    formatVersion: PREMIUM_PADDOCK_NOTIFICATION_FORMAT_VERSION,
    rows: bulletins
      .map((row) => ({
        commentText: row.commentText ?? "",
        evaluationText: row.evaluationText ?? "",
        frameNumber: row.frameNumber ?? "",
        groupKey: row.groupKey,
        horseName: row.horseName ?? "",
        horseNumber: row.horseNumber,
      }))
      .toSorted((left, right) =>
        `${left.groupKey}:${left.horseNumber}`.localeCompare(
          `${right.groupKey}:${right.horseNumber}`,
        ),
      ),
  };
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(JSON.stringify(signaturePayload)),
  );
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

export const formatPremiumPaddockBulletinLine = (row: PremiumPaddockBulletin): string =>
  [
    `**${row.horseNumber} 番 ${truncate(row.horseName ?? "-", 32)}**　${row.groupKey === "value" ? "穴馬" : "人気馬"} / ${row.evaluationText ?? "-"}`,
    row.commentText ? `> ${truncate(row.commentText, 140)}` : "> コメントなし",
  ].join("\n");

export const buildDetailUrl = (race: NarRaceSource): string => {
  const origin = DEFAULT_DETAIL_ORIGIN;
  return `${origin}/races/${race.kaisaiNen}/${race.kaisaiTsukihi.slice(0, 2)}/${race.kaisaiTsukihi.slice(2, 4)}/${race.keibajoCode}/${race.raceBango}`;
};

export const formatRaceStartForDiscord = (raceStartAtJst: string): string =>
  new Intl.DateTimeFormat("ja-JP", {
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    month: "long",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).format(new Date(raceStartAtJst));

export const formatMinutesUntilRace = (raceStartAtJst: string, now: Date): string => {
  const diffMinutes = Math.ceil((new Date(raceStartAtJst).getTime() - now.getTime()) / 60_000);
  if (diffMinutes > 0) {
    return `発走まで残り${diffMinutes}分`;
  }
  if (diffMinutes === 0) {
    return "まもなく発走";
  }
  return `発走から${Math.abs(diffMinutes)}分経過`;
};

const hasRaceStarted = (raceStartAtJst: string, now: Date): boolean =>
  new Date(raceStartAtJst).getTime() <= now.getTime();

export const notifyPremiumPaddockIfNeeded = async (
  env: Env,
  race: NarRaceSource,
  bulletins: readonly PremiumPaddockBulletin[],
  fetchedAt: string,
): Promise<void> => {
  const payloadSignature = await buildPremiumPaddockSignature(bulletins);
  const currentNotification = await getPremiumPaddockNotificationState(
    env.REALTIME_DB,
    race.raceKey,
  );
  const alreadyNotified = currentNotification?.lastNotifiedAt != null;
  if (hasRaceStarted(race.raceStartAtJst, getNow(env))) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "race already started",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "race_started",
      status: "skipped_started",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "race already started",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "race_started",
      status: "skipped_started",
    });
    return;
  }
  if (bulletins.length === 0) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "premium paddock rows are empty",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "premium paddock rows are empty",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    return;
  }
  if (!env.PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: "discord webhook is not configured",
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "webhook_not_configured",
      status: "skipped_unconfigured",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "discord webhook is not configured",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "webhook_not_configured",
      status: "skipped_unconfigured",
    });
    return;
  }
  if (alreadyNotified) {
    if (currentNotification?.lastNotifiedAt !== fetchedAt) {
      await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
        fetchedAt,
        payloadSignature,
        raceKey: race.raceKey,
        skipReason: "already_notified",
        status: "skipped_duplicate",
      });
    }
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: "premium paddock notification was already sent for this race",
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      skipReason: "already_notified",
      status: "skipped_duplicate",
    });
    return;
  }

  const sendAttemptAt = toJstIsoString(getNow(env));
  const lockBefore = toJstIsoString(
    new Date(getNow(env).getTime() - PREMIUM_PADDOCK_NOTIFICATION_LOCK_SECONDS * 1000),
  );
  const claimed = await claimPremiumPaddockNotificationSend(env.REALTIME_DB, {
    lockBefore,
    payloadFetchedAt: fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sendAttemptAt,
  });
  if (!claimed) {
    return;
  }

  const raceNumberLabel = `${Number(race.raceBango)}R`;
  const raceOrderLabel = `${Number(race.raceBango)}番目`;
  const racePlace = JRA_KEIBAJO_NAMES[race.keibajoCode] ?? `競馬場 ${race.keibajoCode}`;
  const raceName = race.raceName ?? "レース名未取得";
  const startLabel = `${formatRaceStartForDiscord(race.raceStartAtJst)}発走（JST）`;
  const remainingLabel = formatMinutesUntilRace(race.raceStartAtJst, getNow(env));
  const response = await fetch(env.PREMIUM_PADDOCK_DISCORD_WEBHOOK_URL, {
    body: JSON.stringify({
      embeds: [
        {
          author: { name: "External Paddock Feed" },
          color: 0xf97316,
          description: [
            `🏟️ **${racePlace} ${raceNumberLabel}（${raceOrderLabel}のレース）**`,
            `🏷️ **${truncate(raceName, 120)}**`,
            `🕒 ${startLabel}`,
            `⏳ ${remainingLabel}`,
            `[レース詳細を開く](${buildDetailUrl(race)})`,
            "",
            truncate(
              bulletins.map(formatPremiumPaddockBulletinLine).join("\n────────────\n"),
              1400,
            ),
          ].join("\n"),
          footer: {
            text: `外部速報 ${bulletins.length}件 / 取得 ${fetchedAt}`,
          },
          timestamp: new Date().toISOString(),
          title: "🚨 外部パドック速報",
        },
      ],
      username: env.PREMIUM_PADDOCK_DISCORD_BOT_NAME ?? DEFAULT_PREMIUM_PADDOCK_DISCORD_BOT_NAME,
    }),
    headers: { "Content-Type": "application/json" },
    method: "POST",
  });

  if (!response.ok) {
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `discord webhook failed: ${response.status}`,
      payloadSignature,
      raceKey: race.raceKey,
      sentAt: sendAttemptAt,
      status: "failed",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `discord webhook failed: ${response.status}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey: race.raceKey,
      sendAttemptAt,
      skipReason: null,
      status: "failed",
    });
    throw new Error(`premium paddock notification failed: ${response.status}`);
  }

  await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
    fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sentAt: sendAttemptAt,
    status: "ok",
  });
  await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
    message: null,
    notifiedAt: fetchedAt,
    payloadFetchedAt: fetchedAt,
    payloadSignature,
    raceKey: race.raceKey,
    sendAttemptAt,
    skipReason: null,
    status: "ok",
  });
};

export const getPremiumPaddockRetryDelaySeconds = (
  race: NarRaceSource,
  now = new Date(),
): number => {
  const minutes = minutesUntilRace(race, now);
  if (minutes === null) return PREMIUM_PADDOCK_RETRY_DELAY_SECONDS;
  if (minutes < -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) return PREMIUM_PADDOCK_RETRY_DELAY_SECONDS;
  if (minutes <= PREMIUM_PADDOCK_HOT_WINDOW_MINUTES) return PREMIUM_PADDOCK_RETRY_DELAY_HOT_SECONDS;
  if (minutes <= PREMIUM_PADDOCK_WARM_WINDOW_MINUTES)
    return PREMIUM_PADDOCK_RETRY_DELAY_WARM_SECONDS;
  return PREMIUM_PADDOCK_RETRY_DELAY_SECONDS;
};

export const getPremiumPaddockRetryAfter = (env: Env, race: NarRaceSource): string =>
  toJstIsoString(
    new Date(getNow(env).getTime() + getPremiumPaddockRetryDelaySeconds(race, getNow(env)) * 1000),
  );

const retryPremiumPaddockWhileInWindow = async (env: Env, race: NarRaceSource): Promise<void> => {
  const minutes = minutesUntilRace(race, getNow(env));
  if (minutes === null || minutes < -PREMIUM_PADDOCK_WINDOW_AFTER_MINUTES) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { raceKey: race.raceKey, type: "fetch-premium-paddock" },
    { delaySeconds: getPremiumPaddockRetryDelaySeconds(race, getNow(env)) },
  );
};

export const getEmptyWeightRetryDelaySeconds = (race: NarRaceSource, now: Date): number | null => {
  const minutes = minutesUntilRace(race, now);
  if (minutes === null) return null;
  if (minutes > WEIGHT_FETCH_LEAD_MINUTES) return null;
  if (minutes < -WEIGHT_FETCH_NEAR_RACE_POST_LIMIT_MINUTES) return null;
  return minutes <= WEIGHT_FETCH_NEAR_RACE_THRESHOLD_MINUTES
    ? WEIGHT_FETCH_EMPTY_RETRY_NEAR_RACE_DELAY_SECONDS
    : WEIGHT_FETCH_EMPTY_RETRY_DELAY_SECONDS;
};

// Returns active (non-scratched) entry horse numbers missing from a non-empty
// weight set. Empty weights intentionally return [] so the empty-retry path
// owns count=0 rather than the sparse path.
export const getMissingJraHorseWeightNumbers = (
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): string[] => {
  if (weights.length === 0) {
    return [];
  }
  const expectedHorseNumbers = new Set(
    entries
      .filter((entry) => !entry.status || !isJraScratchStatus(entry.status))
      .map((entry) => entry.horseNumber),
  );
  const actualHorseNumbers = new Set(weights.map((weight) => weight.horseNumber));
  return Array.from(expectedHorseNumbers).filter(
    (horseNumber) => !actualHorseNumbers.has(horseNumber),
  );
};

export const assertJraHorseWeightsComplete = (
  raceKey: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): void => {
  const missingHorseNumbers = getMissingJraHorseWeightNumbers(entries, weights);
  if (missingHorseNumbers.length > 0) {
    throw new Error(
      `JRA horse weight rows are sparse: ${raceKey} missing=${missingHorseNumbers.join(",")}`,
    );
  }
};

// NAR entry status is null for active runners; any non-null value comes from
// the keiba.go ENTRY_STATUS_LABELS list (出場停止 / 出走取消 / 取消 / 競走除外 /
// 除外) and means scratched. Without this, a partial weight scrape (e.g. when
// the official site has only posted 7 of 8 horses) would mark
// last_weight_fetch_at and the 24h cooldown blocks the retry that would pick
// up the late-posted horse.
export const getMissingNarHorseWeightNumbers = (
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): string[] => {
  if (weights.length === 0) {
    return [];
  }
  const expectedHorseNumbers = new Set(
    entries.filter((entry) => !entry.status).map((entry) => entry.horseNumber),
  );
  const actualHorseNumbers = new Set(weights.map((weight) => weight.horseNumber));
  return Array.from(expectedHorseNumbers).filter(
    (horseNumber) => !actualHorseNumbers.has(horseNumber),
  );
};

export const assertNarHorseWeightsComplete = (
  raceKey: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): void => {
  const missingHorseNumbers = getMissingNarHorseWeightNumbers(entries, weights);
  if (missingHorseNumbers.length > 0) {
    throw new Error(
      `NAR horse weight rows are sparse: ${raceKey} missing=${missingHorseNumbers.join(",")}`,
    );
  }
};

const buildSparseWeightsLogDetail = (
  weights: HorseWeight[],
  missingHorseNumbers: readonly string[],
): string =>
  missingHorseNumbers.length > 0
    ? `count=${weights.length} missing=${missingHorseNumbers.join(",")}`
    : `count=${weights.length}`;

const shouldRunHourlyDiscoveryRecovery = (now: Date): boolean => {
  const { minute } = getJstDateParts(now);
  return Number(minute) === HOURLY_DISCOVERY_RECOVERY_MINUTE;
};

export type ResultFetchEligibility = "due" | "too-recent" | "ineligible";

// 2026-06-28 observability: the planner needs to distinguish "this race is in
// principle a fetch-results candidate but its cooldown is still active"
// (too-recent) from "this race is structurally not a candidate" (ineligible:
// future-dated race, completed race, currently locked, currently queued).
// Without that split the plan-result-fetches-summary row cannot tell the
// difference between "happy path — nothing to do" and "every race is silently
// suppressed by the cooldown".
export const classifyResultFetchEligibility = (
  race: SchedulableRaceSource,
  now: Date,
): ResultFetchEligibility => {
  const minutes = minutesUntilRace(race, now);
  if (minutes === null) {
    return "ineligible";
  }
  if (minutes > 0) {
    return "ineligible";
  }
  if (race.source !== "nar" && race.source !== "jra") {
    return "ineligible";
  }
  if (race.resultCompleteAt) {
    return "ineligible";
  }
  const resultLockUntil = race.resultFetchLockUntil
    ? new Date(race.resultFetchLockUntil).getTime()
    : Number.NaN;
  if (!Number.isNaN(resultLockUntil) && resultLockUntil > now.getTime()) {
    return "ineligible";
  }
  const queuedAtMs = race.lastResultQueuedAt
    ? new Date(race.lastResultQueuedAt).getTime()
    : Number.NaN;
  const queuedTooLongAgo =
    !Number.isNaN(queuedAtMs) &&
    now.getTime() - queuedAtMs > RESULT_FETCH_QUEUE_STALE_MINUTES * 60_000;
  if (race.lastResultQueuedAt && !queuedTooLongAgo) {
    return "ineligible";
  }
  return isDue(race.lastResultFetchAt, RESULT_FETCH_INTERVAL_MINUTES, now) ? "due" : "too-recent";
};

const buildResultFetchJobIfDue = (
  race: SchedulableRaceSource,
  now: Date,
): Extract<Job, { type: "fetch-results" }> | null =>
  classifyResultFetchEligibility(race, now) === "due"
    ? { raceKey: race.raceKey, type: "fetch-results" }
    : null;

interface ResultFetchEligibilityBreakdown {
  eligible: number;
  skippedTooRecent: number;
}

export const summariseResultFetchEligibility = (
  races: readonly SchedulableRaceSource[],
  now: Date,
): ResultFetchEligibilityBreakdown =>
  races.reduce<ResultFetchEligibilityBreakdown>(
    (acc, race) => {
      const eligibility = classifyResultFetchEligibility(race, now);
      if (eligibility === "due") {
        return { eligible: acc.eligible + 1, skippedTooRecent: acc.skippedTooRecent };
      }
      if (eligibility === "too-recent") {
        return { eligible: acc.eligible, skippedTooRecent: acc.skippedTooRecent + 1 };
      }
      return acc;
    },
    { eligible: 0, skippedTooRecent: 0 },
  );

// Result-poller-only planner. Used by the "*/2 0-13 * * *" cron so the
// race-result `fetch-results` jobs fire every 2 minutes without re-running
// the heavier work that the hourly "0 0-13 * * *" cron already performs
// (track-condition, premium paddock, weights, discovery refresh). 2026-05-31:
// also re-runs discovery once per hour off this lightweight cron so a missed
// hourly discover-urls tick does not leave today's races invisible to the
// result poller (= the "11R confirmed but viewer never sees 1R-11R" failure
// mode the new DO + cache-bust path is meant to fix upstream).
interface PlanResultFetchesOptions {
  discoveryRecovery?: boolean;
}

export const planResultFetchesOnly = async (
  env: Env,
  targetDate: string,
  options: PlanResultFetchesOptions = {},
): Promise<number> => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return 0;
  }
  if (options.discoveryRecovery !== false && shouldRunHourlyDiscoveryRecovery(now)) {
    await tryEnsureDiscoveredUrlsAreCurrent(env, targetDate);
  }
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const jobs: Extract<Job, { type: "fetch-results" }>[] = races
    .map((race) => buildResultFetchJobIfDue(race, now))
    .filter((job): job is Extract<Job, { type: "fetch-results" }> => job !== null);
  const inlineJobs = selectInlineRealtimeJobs(jobs, {
    jra: RESULT_FETCH_INLINE_JRA_MAX_PER_TICK,
    nar: RESULT_FETCH_INLINE_NAR_MAX_PER_TICK,
  });
  const inlineRaceKeys = new Set(inlineJobs.map((job) => job.raceKey));
  const queuedJobs = jobs.filter((job) => !inlineRaceKeys.has(job.raceKey));
  await enqueueJobs(env, queuedJobs);
  let inlineAttempted = 0;
  let inlineError = 0;
  const inlineFallbackJobs: Extract<Job, { type: "fetch-results" }>[] = [];
  for (const job of inlineJobs) {
    inlineAttempted += 1;
    try {
      await handleJob(env, job);
    } catch (error: unknown) {
      inlineError += 1;
      inlineFallbackJobs.push(job);
      await logFetch(
        env.REALTIME_DB,
        "plan-result-fetches-inline",
        "error",
        job.raceKey,
        formatError(error),
        env.DETAIL_SECTION_CACHE_KV,
      );
    }
  }
  await enqueueJobs(env, inlineFallbackJobs);
  const enqueuedJobs = [...queuedJobs, ...inlineFallbackJobs];
  const breakdown = summariseResultFetchEligibility(races, now);
  await logFetch(
    env.REALTIME_DB,
    "plan-result-fetches",
    PLAN_RESULT_FETCHES_SUMMARY_STATUS,
    null,
    inlineAttempted === 0
      ? JSON.stringify({
          enqueued: enqueuedJobs.length,
          eligible: breakdown.eligible,
          skipped_too_recent: breakdown.skippedTooRecent,
        })
      : JSON.stringify({
          enqueued: enqueuedJobs.length,
          eligible: breakdown.eligible,
          inlineAttempted,
          inlineError,
          skipped_too_recent: breakdown.skippedTooRecent,
        }),
    // 2026-06-28 (D1 cost optimization): the */2 cron emits a summary row
    // every 2 min. Identical summary payloads (same enqueued / eligible /
    // skipped triple) repeat through quiet windows; KV dedupe collapses
    // those to 1 row / LOG_DEDUPE_TTL_SECONDS while still landing rows on
    // first transition to a new payload.
    env.DETAIL_SECTION_CACHE_KV,
  );
  await markResultFetchQueued(
    env.REALTIME_DB,
    enqueuedJobs.map((job) => job.raceKey),
    toJstIsoString(now),
  );
  return jobs.length;
};

// Premium-paddock-only planner. Used by the "*/2 0-13 * * *" cron so paddock
// detection effectively polls every 2 minutes (paired with
// PREMIUM_PADDOCK_RECHECK_MINUTES = 1 to allow re-enqueue between hourly
// ticks). The hourly "0 0-13 * * *" cron still drives the heavier
// planRealtimeFetches path; this lightweight job only fans out paddock
// candidates so we catch early publications without re-running track-condition
// / weights / discovery work.
export const planPremiumPaddockFetchesOnly = async (
  env: Env,
  targetDate: string,
): Promise<number> => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return 0;
  }
  const todayJobs = await planPremiumPaddockFetchesForDate(env, targetDate, now);
  const nextDay = addDaysToYyyymmdd(targetDate, 1);
  const tomorrowJobs = await planPremiumPaddockFetchesForDate(env, nextDay, now);
  const jobs: Job[] = [...todayJobs, ...tomorrowJobs];
  await enqueueJobs(env, jobs);
  await markPremiumPaddockQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-premium-paddock" ? [job.raceKey] : [])),
    toJstIsoString(now),
  );
  return jobs.length;
};

const runLoggedPlannerStage = async (input: LoggedPlannerStageInput): Promise<void> => {
  try {
    const count = await input.plan();
    await logFetch(input.env.REALTIME_DB, input.jobType, "ok", null, `${count} jobs queued`).catch(
      (logError: unknown) =>
        console.error(
          formatErrorLogLine(
            "Scheduled planner success log failed",
            { jobType: input.jobType, stage: `${input.jobType}.log-ok` },
            logError,
          ),
        ),
    );
  } catch (planError) {
    await logFetch(
      input.env.REALTIME_DB,
      input.jobType,
      "error",
      null,
      formatError(planError),
      input.env.DETAIL_SECTION_CACHE_KV,
    ).catch((logError: unknown) =>
      console.error(
        formatErrorLogLine(
          "Scheduled planner error log failed",
          {
            jobType: input.jobType,
            planError: formatError(planError),
            stage: `${input.jobType}.log-error`,
          },
          logError,
        ),
      ),
    );
  }
};

const runResultAndPaddockPlans = async (env: Env, targetDate: string): Promise<void> => {
  await runLoggedPlannerStage({
    env,
    jobType: "drain-result-cache-busts",
    plan: () => drainPendingResultCacheBusts(env),
  });
  await runLoggedPlannerStage({
    env,
    jobType: "plan-result-fetches",
    plan: async () => {
      const todayCount = await planResultFetchesOnly(env, targetDate);
      const yesterdayCount = await planResultFetchesOnly(env, addDaysToYyyymmdd(targetDate, -1), {
        discoveryRecovery: false,
      });
      return todayCount + yesterdayCount;
    },
  });
  await runLoggedPlannerStage({
    env,
    jobType: "plan-premium-paddock",
    plan: () => planPremiumPaddockFetchesOnly(env, targetDate),
  });
};

export const planRealtimeFetches = async (env: Env, targetDate: string): Promise<number> => {
  const now = getNow(env);
  const jobs: Job[] = [];
  // Outside the JST polling window (22:00-05:59) there are no races to
  // observe — results, track condition, and premium scraping all rely on
  // race timing that has either already finished for the day or hasn't been
  // published yet. Skip everything to keep the every-minute cron from
  // hammering D1 with planning queries while there's nothing to do. Odds
  // polling is owned by the sync-realtime-data-hot worker.
  if (!isJstPollingWindow(now)) {
    return 0;
  }
  jobs.push(...(await planTrackConditionFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planTrackConditionFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  {
    await tryEnsureDiscoveredUrlsAreCurrent(env, targetDate);
    const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
    for (const race of races) {
      const minutes = minutesUntilRace(race, now);
      if (minutes === null) {
        continue;
      }

      const resultLockUntil = race.resultFetchLockUntil
        ? new Date(race.resultFetchLockUntil).getTime()
        : Number.NaN;
      if (
        minutes <= 0 &&
        (race.source === "nar" || race.source === "jra") &&
        !race.resultCompleteAt &&
        isDue(race.lastResultFetchAt, RESULT_FETCH_INTERVAL_MINUTES, now) &&
        (Number.isNaN(resultLockUntil) || resultLockUntil <= now.getTime()) &&
        !race.lastResultQueuedAt
      ) {
        jobs.push({ raceKey: race.raceKey, type: "fetch-results" });
      }
    }
  }
  if (isPremiumRaceDiscoveryTick(now)) {
    jobs.push({ date: targetDate, type: "discover-premium-races" });
    jobs.push({ date: addDaysToYyyymmdd(targetDate, 1), type: "discover-premium-races" });
  }
  jobs.push(...(await planPremiumRaceDataFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planPremiumRaceDataFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  jobs.push(...(await planPremiumPaddockFetchesForDate(env, targetDate, now)));
  jobs.push(
    ...(await planPremiumPaddockFetchesForDate(env, addDaysToYyyymmdd(targetDate, 1), now)),
  );
  await enqueueJobs(env, jobs);
  const queuedAt = toJstIsoString(now);
  await markResultFetchQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-results" ? [job.raceKey] : [])),
    queuedAt,
  );
  await markTrackConditionQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) =>
      job.type === "fetch-jra-track-condition"
        ? [{ date: job.date, keibajoCode: job.keibajoCode }]
        : [],
    ),
    queuedAt,
  );
  await markPremiumPaddockQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-premium-paddock" ? [job.raceKey] : [])),
    queuedAt,
  );
  await markPremiumRaceDataQueued(
    env.REALTIME_DB,
    jobs.flatMap((job) => (job.type === "fetch-premium-race-data" ? [job.raceKey] : [])),
    queuedAt,
  );
  return jobs.length;
};

interface FetchWeightsBatchInput {
  date: string;
  force: boolean;
  source: "all" | "jra" | "nar";
}

const FETCH_WEIGHTS_BATCH_DATE_PATTERN = /^\d{4}-\d{2}-\d{2}$/u;
const FETCH_WEIGHTS_BATCH_SOURCE_NORMALIZER: ReadonlyMap<string, "all" | "jra" | "nar"> = new Map([
  ["all", "all"],
  ["jra", "jra"],
  ["nar", "nar"],
]);

// Validates the manual force-trigger POST body. Accepts `YYYY-MM-DD` date,
// optional `source` (jra/nar/all default jra), and optional `force` boolean
// that bypasses the per-race cooldown so an operator can re-fetch a race
// whose previous attempt only stored a partial / empty bataiju snapshot.
export const parseFetchWeightsBatchBody = (
  body: { date?: string; force?: boolean; source?: string } | null,
): FetchWeightsBatchInput | null => {
  if (!body || typeof body.date !== "string") return null;
  if (!FETCH_WEIGHTS_BATCH_DATE_PATTERN.test(body.date)) return null;
  const rawSource = body.source ?? "jra";
  const source = FETCH_WEIGHTS_BATCH_SOURCE_NORMALIZER.get(rawSource);
  if (!source) return null;
  return {
    date: body.date,
    force: body.force === true,
    source,
  };
};

// Bulk-enqueues fetch-weights jobs for every schedulable race on `date`
// matching `source`. When `force` is false the per-race cooldown still
// applies; when true every matching race is enqueued unconditionally. The
// 15-min cron + 180-min lead time covers the happy path, this endpoint
// exists for operator-driven backfill after a Hyperdrive outage.
export const enqueueFetchWeightsBatch = async (
  env: Env,
  input: FetchWeightsBatchInput,
): Promise<number> => {
  const targetDate = input.date.replace(/-/gu, "");
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const now = getNow(env);
  const matchingRaces = races.filter(
    (race) => input.source === "all" || race.source === input.source,
  );
  const dueRaces = input.force
    ? matchingRaces
    : matchingRaces.filter((race) =>
        isDue(
          race.lastWeightFetchAt,
          resolveWeightFetchCooldownMinutes({
            lastFetchAt: race.lastWeightFetchAt,
            now,
            raceStartAtJst: race.raceStartAtJst,
          }),
          now,
        ),
      );
  const jobs: Job[] = dueRaces.map((race) => ({ raceKey: race.raceKey, type: "fetch-weights" }));
  await enqueueJobs(env, jobs);
  return jobs.length;
};

// Prefer the more complete of primary entry-page weights vs race-result-page
// weights. Incomplete primary HTML (partial late post / scraper miss) used to
// short-circuit as soon as primaryWeights.length > 0 and never tried the
// result page — leaving races stuck on hard sparse errors (2026-07-21 NAR).
export const parseHorseWeightsForRace = async (
  race: NarRaceSource,
  html: string,
  entries: Omit<RaceEntry, "fetchedAt">[] = [],
): Promise<HorseWeight[]> => {
  if (race.source === "jra") {
    return parseJraHorseWeights(html);
  }
  const primaryWeights = parseHorseWeights(html);
  const primaryMissing =
    primaryWeights.length > 0 ? getMissingNarHorseWeightNumbers(entries, primaryWeights) : [];
  const shouldTryResultPage =
    primaryWeights.length === 0 || (entries.length > 0 && primaryMissing.length > 0);
  if (!shouldTryResultPage) {
    return primaryWeights;
  }
  const resultHtml = await fetchRacePage(buildRaceResultUrl(race.debaUrl));
  const resultWeights = parseRaceResultHorseWeights(resultHtml);
  if (primaryWeights.length === 0) {
    return resultWeights;
  }
  if (resultWeights.length === 0) {
    return primaryWeights;
  }
  const resultMissing = getMissingNarHorseWeightNumbers(entries, resultWeights);
  if (resultMissing.length < primaryMissing.length) {
    return resultWeights;
  }
  if (
    resultMissing.length === primaryMissing.length &&
    resultWeights.length > primaryWeights.length
  ) {
    return resultWeights;
  }
  return primaryWeights;
};

const fetchAndStoreWeights = async (
  env: Env,
  raceKey: string,
  watchdogReservedAt: string | null,
  requestedGeneration: WeightSnapshotGeneration | null,
): Promise<boolean> => {
  // Only a generation-bound repair delivery may reuse a stored snapshot. A
  // normal watchdog/manual job must scrape again: otherwise any old non-empty
  // snapshot could be promoted to post-weight success without proving it is
  // the generation that caused this rescore request.
  const alreadyStored = await getLatestHorseWeights(env.REALTIME_DB, raceKey);
  if (alreadyStored !== null && alreadyStored.horses.length > 0 && requestedGeneration !== null) {
    const storedRace = await getRaceSource(env.REALTIME_DB, raceKey);
    if (!storedRace) {
      throw new Error(`race source not found: ${raceKey}`);
    }
    const storedGeneration = await buildWeightSnapshotGeneration(
      alreadyStored.fetchedAt,
      alreadyStored.horses,
    );
    if (!isSameWeightGeneration(storedGeneration, requestedGeneration)) {
      throw new Error(`weight snapshot repair generation mismatch: ${raceKey}`);
    }
    await triggerRescoreAfterWeights({
      env,
      generation: requestedGeneration,
      raceKey,
      raceStartAtJst: storedRace.raceStartAtJst,
    });
    await updateLastFetch(
      env.REALTIME_DB,
      raceKey,
      "last_weight_fetch_at",
      alreadyStored.fetchedAt,
    );
    await logFetch(
      env.REALTIME_DB,
      "fetch-weights",
      SKIP_STATUS.weightsAlreadyStored,
      raceKey,
      null,
    );
    return true;
  }
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    throw new Error(`race source not found: ${raceKey}`);
  }
  const now = getNow(env);
  const fetchedAt = toJstIsoString(now);
  const claimedAt =
    watchdogReservedAt === fetchedAt ? toJstIsoString(new Date(now.getTime() + 1000)) : fetchedAt;
  const leaseExpiredBefore = toJstIsoString(
    new Date(now.getTime() - WEIGHT_FETCH_LEASE_SECONDS * 1000),
  );
  const reservationClaimed =
    watchdogReservedAt === null
      ? false
      : await claimReservedWeightFetch(env.REALTIME_DB, raceKey, watchdogReservedAt, claimedAt);
  const claimed =
    reservationClaimed ||
    (await claimWeightFetch(env.REALTIME_DB, raceKey, claimedAt, leaseExpiredBefore));
  if (!claimed) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-weights",
      SKIP_STATUS.lockHeld,
      raceKey,
      `leaseSeconds=${WEIGHT_FETCH_LEASE_SECONDS}`,
    );
    return false;
  }
  const html = await fetchRacePage(race.debaUrl);
  const latestOdds = race.source === "jra" ? await fetchHotOddsPayload(env, raceKey) : null;
  const latestTanshoOdds = buildTanshoOddsRowsFromHotOdds(latestOdds);
  const entries =
    race.source === "jra"
      ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(html), latestOdds?.latest)
      : parseRaceEntries(html);
  await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
  // Entry-only push so the race-trend DO knows about the venue's pre-race
  // siblings without waiting for either the alarm self-pull (60s cadence)
  // or the first result-fetch push. Before this a viewer hitting race 11R
  // between race 10R's start and race 10R's first result fetch saw a
  // sibling list of 1R-9R only — the DO had no row for 10R yet, so the
  // viewer's "today sibling rows" filter dropped it. Field-level merge in
  // the DO preserves any odds / weight a later push or self-pull lands.
  if (entries.length > 0) {
    await pushResultsToRaceTrendDO(
      env,
      buildRaceTrendDailyTrackRow({
        entries,
        fetchedAt,
        isComplete: false,
        race,
        results: [],
        tanshoOdds: latestTanshoOdds,
      }),
      race,
    );
  }
  const weights = await parseHorseWeightsForRace(race, html, entries);
  // Fail-closed pending path: an empty or incomplete upstream response must
  // not throw and must not write last_weight_fetch_at / snapshot (a success
  // cooldown would block recovery). The watchdog schedules the next single
  // fetch while the provider is still publishing the table. 2026-07-19..21:
  // assert* throws turned sparse NAR/JRA parses into hard `error` rows with no
  // recovery (e.g. nar:2026:0721:35:04).
  if (weights.length === 0) {
    await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_soft_miss_at", claimedAt);
    // An empty upstream response means the provider has not published the
    // weight table yet. This is an expected pending state, not a fetch error
    // and must not enter Queue retry/DLQ accounting. The watchdog owns the
    // next single fetch reservation.
    await logFetch(
      env.REALTIME_DB,
      "fetch-weights",
      SKIP_STATUS.weightsPending,
      raceKey,
      "count=0 upstream-not-published",
    );
    return false;
  }
  const missingHorseNumbers =
    race.source === "jra"
      ? getMissingJraHorseWeightNumbers(entries, weights)
      : race.source === "nar"
        ? getMissingNarHorseWeightNumbers(entries, weights)
        : [];
  const entryGeneration = await buildEntrySnapshotGeneration({
    entries,
    fetchedAt,
    source: race.source,
  });
  const unexpectedHorseNumbers =
    entryGeneration.activeHorseNumbers.length === 0
      ? []
      : getUnexpectedWeightHorseNumbers(entryGeneration.activeHorseNumbers, weights);
  const tooFewRows = weights.length < MIN_HORSE_WEIGHT_ROWS_PER_RACE;
  if (
    missingHorseNumbers.length > 0 ||
    unexpectedHorseNumbers.length > 0 ||
    entryGeneration.activeHorseNumbers.length === 0 ||
    tooFewRows
  ) {
    await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_soft_miss_at", claimedAt);
    await logFetch(
      env.REALTIME_DB,
      "fetch-weights",
      SKIP_STATUS.weightsIncomplete,
      raceKey,
      `${buildSparseWeightsLogDetail(weights, missingHorseNumbers)} upstream-incomplete${
        unexpectedHorseNumbers.length > 0 ? ` unexpected=${unexpectedHorseNumbers.join(",")}` : ""
      }`,
    );
    return false;
  }
  await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
  await broadcastHorseWeightsToDO(env, raceKey, fetchedAt, weights);
  const generation: WeightSnapshotGeneration = {
    ...(await buildWeightSnapshotGeneration(fetchedAt, weights)),
    ...entryGeneration,
  };
  await triggerOrEnqueueWeightGenerationRepair({
    env,
    generation,
    raceKey,
    raceStartAtJst: race.raceStartAtJst,
  });
  await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
  if (entries.length > 0) {
    await pushResultsToRaceTrendDO(
      env,
      buildRaceTrendDailyTrackRow({
        entries,
        fetchedAt,
        isComplete: false,
        race,
        results: [],
        tanshoOdds: latestTanshoOdds,
        weights,
      }),
      race,
    );
  }
  await runResultCacheBusts({ claim: null, env, race });
  return true;
};

const toHorseWeightSnapshot = (fetchedAt: string, weights: HorseWeight[]): HorseWeightSnapshot => ({
  fetchedAt,
  horses: weights.map((entry) => ({
    changeAmount: entry.changeAmount,
    changeSign: entry.changeSign,
    horseName: entry.horseName,
    horseNumber: entry.horseNumber,
    weight: entry.weight,
  })),
});

// Pushes the freshly persisted weights to the Durable Object that fan-outs to
// any active SSE subscribers. Failures are swallowed and logged so the queue
// consumer does not retry the (already successful) D1 write on transient DO
// errors. The next weight fetch will resync the DO state.
const broadcastHorseWeightsToDO = async (
  env: Env,
  raceKey: string,
  fetchedAt: string,
  weights: HorseWeight[],
): Promise<void> => {
  try {
    const stub = env.HORSE_WEIGHT_DO.get(env.HORSE_WEIGHT_DO.idFromName(raceKey));
    await writeHorseWeightSnapshotToStub({
      snapshot: toHorseWeightSnapshot(fetchedAt, weights),
      stub,
    });
  } catch (error) {
    await logFetch(env.REALTIME_DB, "horse-weight-do-write", "error", raceKey, formatError(error));
  }
};

interface RescoreTriggerRequest extends WeightSnapshotGeneration {
  category: "ban-ei" | "jra" | "nar";
  keibajoCode: string;
  raceBango: string;
  raceStartAtJst?: string;
  runYmd: string;
}

interface FailedRescoreTriggerOutcome {
  detail: string;
  status: string;
  succeeded: false;
}

interface SuccessfulRescoreTriggerOutcome {
  detail: null;
  status: string;
  succeeded: true;
}

type RescoreTriggerOutcome = FailedRescoreTriggerOutcome | SuccessfulRescoreTriggerOutcome;

interface TriggerRescoreAfterWeightsInput {
  env: Env;
  raceKey: string;
  raceStartAtJst?: string;
  generation: WeightSnapshotGeneration;
}

interface CanonicalWeightRow {
  horseNumber: number;
  weight: number;
}

interface EntrySnapshotGeneration {
  activeHorseNumbers: number[];
  entrySnapshotFetchedAt: string;
  entrySnapshotHash: string;
  excludedHorseNumbers: number[];
}

interface CanonicalEntrySnapshot {
  activeHorseNumbers: number[];
  excludedHorseNumbers: number[];
}

interface BuildEntrySnapshotGenerationInput {
  entries: ReadonlyArray<Omit<RaceEntry, "fetchedAt">>;
  fetchedAt: string;
  source: "jra" | "nar";
}

const compareCanonicalWeightRows = (left: CanonicalWeightRow, right: CanonicalWeightRow): number =>
  left.horseNumber - right.horseNumber;

const canonicalWeightRows = (weights: ReadonlyArray<HorseWeight>): CanonicalWeightRow[] =>
  weights
    .map((entry): CanonicalWeightRow => {
      const horseNumber = Number(entry.horseNumber);
      if (!Number.isInteger(horseNumber) || horseNumber <= 0) {
        throw new Error(`invalid weight snapshot horse number: ${entry.horseNumber}`);
      }
      if (
        typeof entry.weight !== "number" ||
        !Number.isInteger(entry.weight) ||
        entry.weight <= 0
      ) {
        throw new Error(`invalid weight snapshot value: ${entry.horseNumber}`);
      }
      return { horseNumber, weight: entry.weight };
    })
    .sort(compareCanonicalWeightRows);

const sha256Hex = async (value: string): Promise<string> => {
  const digest = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join("");
};

const compareHorseNumbers = (left: number, right: number): number => left - right;

const canonicalEntrySnapshot = (
  source: "jra" | "nar",
  entries: ReadonlyArray<Omit<RaceEntry, "fetchedAt">>,
): CanonicalEntrySnapshot => {
  const activeHorseNumbers = new Set<number>();
  const excludedHorseNumbers = new Set<number>();
  for (const entry of entries) {
    const horseNumber = Number(entry.horseNumber);
    if (!Number.isInteger(horseNumber) || horseNumber <= 0) {
      throw new Error(`invalid entry snapshot horse number: ${entry.horseNumber}`);
    }
    const excluded =
      entry.status !== null && (source === "nar" || isJraScratchStatus(entry.status));
    if (excluded) {
      excludedHorseNumbers.add(horseNumber);
      activeHorseNumbers.delete(horseNumber);
    } else if (!excludedHorseNumbers.has(horseNumber)) {
      activeHorseNumbers.add(horseNumber);
    }
  }
  return {
    activeHorseNumbers: Array.from(activeHorseNumbers).sort(compareHorseNumbers),
    excludedHorseNumbers: Array.from(excludedHorseNumbers).sort(compareHorseNumbers),
  };
};

export const buildEntrySnapshotGeneration = async (
  input: BuildEntrySnapshotGenerationInput,
): Promise<EntrySnapshotGeneration> => {
  const snapshot = canonicalEntrySnapshot(input.source, input.entries);
  return {
    ...snapshot,
    entrySnapshotFetchedAt: input.fetchedAt,
    entrySnapshotHash: await sha256Hex(JSON.stringify(snapshot)),
  };
};

const getUnexpectedWeightHorseNumbers = (
  activeHorseNumbers: ReadonlyArray<number>,
  weights: ReadonlyArray<HorseWeight>,
): string[] => {
  const expected = new Set(activeHorseNumbers.map(String));
  return weights
    .map((weight) => weight.horseNumber)
    .filter((horseNumber) => !expected.has(horseNumber));
};

export const buildWeightSnapshotGeneration = async (
  fetchedAt: string,
  weights: ReadonlyArray<HorseWeight>,
): Promise<WeightSnapshotGeneration> => {
  const rows = canonicalWeightRows(weights);
  return {
    weightSnapshotCount: rows.length,
    weightSnapshotFetchedAt: fetchedAt,
    weightSnapshotHash: await sha256Hex(JSON.stringify(rows)),
  };
};

const isSameWeightGeneration = (
  left: WeightSnapshotGeneration,
  right: WeightSnapshotGeneration,
): boolean =>
  left.weightSnapshotCount === right.weightSnapshotCount &&
  left.weightSnapshotFetchedAt === right.weightSnapshotFetchedAt &&
  left.weightSnapshotHash === right.weightSnapshotHash;

const triggerOrEnqueueWeightGenerationRepair = async (
  input: TriggerRescoreAfterWeightsInput,
): Promise<void> => {
  try {
    await triggerRescoreAfterWeights(input);
  } catch {
    await input.env.REALTIME_JOBS.send({
      raceKey: input.raceKey,
      type: "fetch-weights",
      weightGeneration: input.generation,
    });
  }
};

const resolveRescoreCategory = (
  source: string,
  keibajoCode: string,
): "ban-ei" | "jra" | "nar" | null => {
  if (source === "jra") return "jra";
  if (source !== "nar") return null;
  return BAN_EI_KEIBAJO_CODES.has(keibajoCode) ? "ban-ei" : "nar";
};

// raceKey shape: "<source>:<year>:<mmdd>:<keibajoCode>:<raceBango>" (see
// buildRealtimeRaceKey in race-key.ts). Returns null when the shape does not
// match or the source maps to no predict category (defensive — every D1 row
// today is jra or nar).
export const parseRescoreTriggerRequest = (
  raceKey: string,
  generation: WeightSnapshotGeneration,
  raceStartAtJst?: string,
): RescoreTriggerRequest | null => {
  const parts = raceKey.split(":");
  if (parts.length !== RACE_KEY_PART_COUNT) return null;
  const source = parts[0]!;
  const year = parts[1]!;
  const mmdd = parts[2]!;
  const keibajoCode = parts[3]!;
  const raceBango = parts[4]!;
  if (!source || !year || !mmdd || !keibajoCode || !raceBango) return null;
  const category = resolveRescoreCategory(source, keibajoCode);
  if (!category) return null;
  return {
    category,
    keibajoCode,
    raceBango,
    ...(raceStartAtJst === undefined ? {} : { raceStartAtJst }),
    runYmd: `${year}${mmdd}`,
    ...generation,
  };
};

const postRescoreTriggerRequest = async (
  binding: { fetch: typeof fetch },
  token: string,
  target: RescoreTriggerRequest,
): Promise<Response> =>
  binding.fetch(
    new Request(FINISH_POSITION_CRON_INTERNAL_RESCORE_RACE_URL, {
      body: JSON.stringify(target),
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      method: "POST",
    }),
  );

// Map a finish-position-cron /api/internal/rescore-race response onto the
// fetch_logs status that actually describes what happened. The endpoint answers
// 401 on token mismatch, 200 {rescoreEnabled:false} when RESCORE_ENABLED != "1",
// 200 {claimed:false} on a DO claim collision, and 202 {claimed:true} when the
// per-race rescore message was really enqueued.
const readRescoreTriggerOutcome = async (response: Response): Promise<RescoreTriggerOutcome> => {
  if (!response.ok) return { detail: `http ${response.status}`, status: "error", succeeded: false };
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    return { detail: "unparsable response body", status: "error", succeeded: false };
  }
  if (!isObjectRecord(body))
    return { detail: "unparsable response body", status: "error", succeeded: false };
  if (body.rescoreEnabled === false)
    return {
      detail: "rescore disabled",
      status: SKIP_STATUS.rescoreDisabled,
      succeeded: false,
    };
  if (body.claimed !== true)
    return { detail: null, status: SKIP_STATUS.rescoreNotClaimed, succeeded: true };
  return { detail: null, status: FETCH_LOG_SUCCESS.okStatus, succeeded: true };
};

const throwRescoreTriggerFailure = async (
  env: Env,
  raceKey: string,
  detail: string,
): Promise<never> => {
  await logFetch(env.REALTIME_DB, WEIGHT_RESCORE_TRIGGER_LOG_KIND, "error", raceKey, detail);
  throw new Error(`weight rescore trigger failed: ${detail}`);
};

// Trigger immediately after a successful horse-weight write, and also on a
// queue redelivery that finds the snapshot already stored. The finish-position
// coordinator owns idempotency; this caller must fail closed until it receives
// a confirmed enqueue or an idempotent already-claimed response. Throwing after
// a transport/config/protocol failure lets the Queue retry the trigger without
// scraping or rewriting the already-persisted horse weights.
export const triggerRescoreAfterWeights = async (
  input: TriggerRescoreAfterWeightsInput,
): Promise<void> => {
  const { env, generation, raceKey, raceStartAtJst } = input;
  const binding = env.FINISH_POSITION_CRON;
  const token = env.TRIGGER_TOKEN;
  if (!binding) {
    return throwRescoreTriggerFailure(env, raceKey, "missing FINISH_POSITION_CRON binding");
  }
  if (!token) {
    return throwRescoreTriggerFailure(env, raceKey, "missing TRIGGER_TOKEN");
  }
  const target = parseRescoreTriggerRequest(raceKey, generation, raceStartAtJst);
  if (!target) {
    return throwRescoreTriggerFailure(env, raceKey, "invalid race key shape");
  }
  const response = await postRescoreTriggerRequest(binding, token, target).catch(
    async (error: unknown): Promise<never> =>
      throwRescoreTriggerFailure(env, raceKey, formatError(error)),
  );
  const outcome = await readRescoreTriggerOutcome(response);
  if (!outcome.succeeded) {
    await throwRescoreTriggerFailure(env, raceKey, outcome.detail);
  }
  await logFetch(
    env.REALTIME_DB,
    WEIGHT_RESCORE_TRIGGER_LOG_KIND,
    outcome.status,
    raceKey,
    outcome.detail,
  );
};

interface BuildRaceTrendRowArgs {
  entries: Omit<RaceEntry, "fetchedAt">[];
  fetchedAt: string;
  isComplete: boolean;
  race: NarRaceSource;
  results: Omit<RaceResult, "fetchedAt">[];
  tanshoOdds?: RaceResultTanshoOddsRow[];
  weights?: HorseWeight[];
}

const formatHassoJikokuFromRaceStart = (raceStartAtJst: string): string | null => {
  if (raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

const normalizeHorseNumberKey = (value: string): string => {
  const trimmed = value.trim();
  const parsed = Number.parseInt(trimmed, 10);
  return Number.isFinite(parsed) ? String(parsed) : trimmed;
};

// Build a push-row whose starter list covers the FULL field (one row per
// entry), not just the horses present in the result HTML. The previous
// 3-result push permanently shrank race-trend DO state from 12 entries to
// 3 result rows for any partial-result NAR card; the field-level DO merge
// (mergeStarterRowLists) further preserves bataiju / odds / popularity the
// alarm self-pull had already populated. Horses with a result row pick up
// finishPosition + sohaTime; horses without one stay at finishPosition=0 so
// the viewer still renders the row with "-" until the next push fills it in.
const buildRaceTrendDailyTrackRow = ({
  entries,
  fetchedAt,
  isComplete,
  race,
  results,
  tanshoOdds = [],
  weights = [],
}: BuildRaceTrendRowArgs): RaceTrendDailyTrackRow => {
  const resultByHorseNumber = new Map(
    results.map((result) => [normalizeHorseNumberKey(result.horseNumber), result]),
  );
  const oddsByHorseNumber = new Map(
    tanshoOdds.map((odds) => [normalizeHorseNumberKey(odds.horseNumber), odds]),
  );
  const weightByHorseNumber = new Map(
    weights.map((weight) => [normalizeHorseNumberKey(weight.horseNumber), weight]),
  );
  const starterRows = entries.map((entry) => {
    const horseNumberKey = normalizeHorseNumberKey(entry.horseNumber);
    const result = resultByHorseNumber.get(horseNumberKey);
    const odds = oddsByHorseNumber.get(horseNumberKey);
    const weight = weightByHorseNumber.get(horseNumberKey);
    const finishPosition = result
      ? Number.parseInt(result.finishPosition.replace(/\s+/gu, ""), 10) || 0
      : 0;
    return {
      bamei: entry.horseName,
      bataiju:
        weight?.weight === undefined || weight.weight === null ? null : String(weight.weight),
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      finishPosition,
      hassoJikoku: formatHassoJikokuFromRaceStart(race.raceStartAtJst),
      jockeyName: entry.jockeyName,
      kaisaiNen: race.kaisaiNen,
      kaisaiTsukihi: race.kaisaiTsukihi,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      raceName: race.raceName,
      runnerCount: null,
      sohaTime: result?.time ?? null,
      source: race.source,
      tanshoOdds:
        odds === undefined ? null : String(Math.round(odds.tanshoOdds * 10)).padStart(4, "0"),
      tanshoPopularity: odds === undefined ? null : String(odds.popularity).padStart(2, "0"),
      umaban: entry.horseNumber,
      wakuban: null,
      zogenFugo: weight?.changeSign ?? null,
      zogenSa:
        weight?.changeAmount === undefined || weight.changeAmount === null
          ? null
          : String(weight.changeAmount),
    };
  });
  return {
    fetchedAt,
    finishedAt: isComplete ? fetchedAt : null,
    isComplete,
    raceBango: race.raceBango,
    raceKey: race.raceKey,
    runningStyles: [],
    starterRows,
  };
};

const buildRaceEntriesFromResults = (
  results: ReadonlyArray<Omit<RaceResult, "fetchedAt">>,
): Omit<RaceEntry, "fetchedAt">[] =>
  results.map((result) => ({
    horseName: result.horseName,
    horseNumber: result.horseNumber,
    jockeyName: null,
    status: null,
  }));

type RaceTrendEntriesSource = "parsed" | "stored" | "results";

interface ResolvedRaceTrendEntries {
  entries: Omit<RaceEntry, "fetchedAt">[];
  source: RaceTrendEntriesSource;
}

interface ResultEntryPage {
  html: string;
  notFound: boolean;
  storedEntries: Omit<RaceEntry, "fetchedAt">[] | null;
}

interface ResultPage {
  html: string;
  notFound: boolean;
}

const fetchNarEntryPageForResult = async (
  env: Env,
  race: NarRaceSource,
): Promise<ResultEntryPage> => {
  try {
    return { html: await fetchRacePage(race.debaUrl), notFound: false, storedEntries: null };
  } catch (error: unknown) {
    if (!(error instanceof FetchStatusError) || error.status !== 404) {
      throw error;
    }
    const stored = await getLatestRaceEntries(env.REALTIME_DB, race.raceKey);
    if (stored === null || stored.horses.length === 0) {
      return { html: "", notFound: true, storedEntries: null };
    }
    const storedEntries = stored.horses.map((entry) => ({
      horseName: entry.horseName,
      horseNumber: entry.horseNumber,
      jockeyName: entry.jockeyName,
      status: entry.status,
    }));
    await logFetch(
      env.REALTIME_DB,
      "fetch-results",
      "fallback:stored-entry-after-404",
      race.raceKey,
      `fetched_at=${stored.fetchedAt} horses=${storedEntries.length}`,
      env.DETAIL_SECTION_CACHE_KV,
    );
    return { html: "", notFound: true, storedEntries };
  }
};

const fetchNarResultPage = async (url: string): Promise<ResultPage> => {
  try {
    return { html: await fetchRacePage(url), notFound: false };
  } catch (error: unknown) {
    if (!(error instanceof FetchStatusError) || error.status !== 404) {
      throw error;
    }
    return { html: "", notFound: true };
  }
};

const resolveRaceTrendEntries = async (
  env: Env,
  raceKey: string,
  parsedEntries: Omit<RaceEntry, "fetchedAt">[],
  results: Omit<RaceResult, "fetchedAt">[],
): Promise<ResolvedRaceTrendEntries> => {
  if (parsedEntries.length > 0) return { entries: parsedEntries, source: "parsed" };
  const stored = await getLatestRaceEntries(env.REALTIME_DB, raceKey);
  if (stored !== null && stored.horses.length > 0) {
    return {
      entries: stored.horses.map((entry) => ({
        horseName: entry.horseName,
        horseNumber: entry.horseNumber,
        jockeyName: entry.jockeyName,
        status: entry.status,
      })),
      source: "stored",
    };
  }
  return { entries: buildRaceEntriesFromResults(results), source: "results" };
};

// Observe `requestTrendCacheBust` outcome by status:
//   - "ok"       : silent (the happy path needs no log entry).
//   - "error"    : 5xx / network failure / retry exhausted — record at error
//                  level so the standard fetch_logs telemetry catches it.
//   - "skipped"  : viewer internal token not configured (or other
//                  environmental skip). Lower severity than error but still
//                  worth surfacing because a long "skipped" streak silently
//                  disables the bust signal across the entire card.
const runTrendCacheBust = async (
  env: Env,
  raceKey: string,
  race: NarRaceSource,
): Promise<boolean> => {
  const outcome = await requestTrendCacheBust(env, buildTrendBustFromRaceContext(race));
  if (outcome.status === "error") {
    await logFetch(env.REALTIME_DB, "trend-cache-bust", "error", raceKey, outcome.message);
    return false;
  }
  if (outcome.status === "skipped") {
    await logFetch(env.REALTIME_DB, "trend-cache-bust", "skipped", raceKey, outcome.message);
    return false;
  }
  return true;
};

// Per-race cache-bust signal. Fires alongside the day-level trend bust so
// the viewer drops both the main and stale-tier KV entries for every
// detail-section variant of the finished race plus bumps the generation
// counter that defeats the Cache API tier. Failures are logged and returned
// to the persistent outbox without aborting result persistence.
const runRaceCacheBust = async (
  env: Env,
  raceKey: string,
  race: NarRaceSource,
): Promise<boolean> => {
  const outcome = await triggerRaceCacheBust(env, {
    keibajoCode: race.keibajoCode,
    mmdd: race.kaisaiTsukihi,
    raceBango: race.raceBango,
    source: race.source,
    year: race.kaisaiNen,
  });
  if (outcome.status === "error") {
    await logFetch(env.REALTIME_DB, "race-cache-bust", "error", raceKey, outcome.message);
    return false;
  }
  if (outcome.status === "skipped") {
    await logFetch(env.REALTIME_DB, "race-cache-bust", "skipped", raceKey, outcome.message);
    return false;
  }
  return true;
};

// Result persistence is the Queue message's success boundary. Viewer cache
// invalidation is best-effort: run the day and race busts concurrently, and
// contain even an unexpected rejection (including a D1 telemetry failure)
// so it cannot turn a completed result write into a Queue redelivery.
const runResultCacheBusts = async (
  input: RunResultCacheBustsInput,
): Promise<ResultCacheBustOutcomes> => {
  const raceKey = input.race.raceKey;
  const [trendOutcome, raceOutcome] = await Promise.allSettled([
    input.claim === null || input.claim.needsTrendBust
      ? runTrendCacheBust(input.env, raceKey, input.race)
      : Promise.resolve(true),
    input.claim === null || input.claim.needsRaceBust
      ? runRaceCacheBust(input.env, raceKey, input.race)
      : Promise.resolve(true),
  ]);
  if (trendOutcome.status === "rejected") {
    console.warn(
      formatErrorLogLine(
        "Result cache bust side effect failed",
        { kind: "trend", raceKey },
        trendOutcome.reason,
      ),
    );
  }
  if (raceOutcome.status === "rejected") {
    console.warn(
      formatErrorLogLine(
        "Result cache bust side effect failed",
        { kind: "race", raceKey },
        raceOutcome.reason,
      ),
    );
  }
  return {
    raceDelivered: raceOutcome.status === "fulfilled" && raceOutcome.value,
    trendDelivered: trendOutcome.status === "fulfilled" && trendOutcome.value,
  };
};

const processPendingResultCacheBust = async (env: Env, race: NarRaceSource): Promise<boolean> => {
  const now = getNow(env);
  const nowIso = toJstIsoString(now);
  const claim = await claimResultCacheBust({
    db: env.REALTIME_DB,
    leaseUntil: toJstIsoString(new Date(now.getTime() + RESULT_CACHE_BUST_LEASE_SECONDS * 1_000)),
    now: nowIso,
    raceKey: race.raceKey,
  });
  if (claim === null) {
    return false;
  }
  const outcomes = await runResultCacheBusts({ claim, env, race });
  await completeResultCacheBust({
    claim,
    db: env.REALTIME_DB,
    now: nowIso,
    ...outcomes,
  });
  return outcomes.raceDelivered && outcomes.trendDelivered;
};

export const drainPendingResultCacheBusts = async (env: Env): Promise<number> => {
  const raceKeys = await listPendingResultCacheBustRaceKeys(
    env.REALTIME_DB,
    toJstIsoString(getNow(env)),
    RESULT_CACHE_BUST_DRAIN_LIMIT,
  );
  const outcomes = await Promise.all(
    raceKeys.map(async (raceKey) => {
      const race = await getRaceSource(env.REALTIME_DB, raceKey);
      return race === null ? false : processPendingResultCacheBust(env, race);
    }),
  );
  return outcomes.filter(Boolean).length;
};

// The DO push is intentionally fire-and-forget for the surrounding
// `fetchAndStoreResults` flow — a 5xx from the DO must not abort result
// persistence or trigger a `failResultFetch` rollback. But silently
// discarding the Response (the pre-fix behavior) meant 5xx pushes never
// surfaced anywhere observable, so a DO that stayed unhealthy across a
// whole card looked exactly like a healthy one. Surface non-2xx via
// logFetch so the standard fetch_logs telemetry catches it without
// changing the fire-and-forget semantics.
const pushResultsToRaceTrendDO = async (
  env: Env,
  row: RaceTrendDailyTrackRow,
  race: NarRaceSource,
): Promise<void> => {
  try {
    const idName = buildRaceTrendDailyTrackDoIdName({
      keibajoCode: race.keibajoCode,
      source: race.source,
      targetYmd: `${race.kaisaiNen}${race.kaisaiTsukihi}`,
    });
    const stub = env.RACE_TREND_DAILY_TRACK_DO.get(
      env.RACE_TREND_DAILY_TRACK_DO.idFromName(idName),
    );
    const response = await pushRaceTrendDailyTrackRowToStub({ row, stub });
    if (!response.ok) {
      await logFetch(
        env.REALTIME_DB,
        "race-trend-daily-track-do-push",
        "non-2xx",
        race.raceKey,
        `HTTP ${response.status}`,
      );
    }
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "race-trend-daily-track-do-push",
      "error",
      race.raceKey,
      formatError(error),
    );
  }
};

export type ResultFetchOutcome =
  | "complete"
  | "retry-short"
  | "retry-medium"
  | "retry-long"
  | "give-up";

interface ResolveResultFetchOutcomeInput {
  expectedHorseCount: number;
  inserted: number;
  minutesAfterRaceStart: number | null;
  source: NarRaceSource["source"];
}

// 2026-06-05: replaces the old NAR-only completion backstop + partial-retry
// helper pair with a single resolver that returns the routing decision for
// fetchAndStoreResults. NAR partial results route through a progressive retry
// (retry-short / retry-medium / retry-long) up to RESULT_FETCH_GIVE_UP_HOURS,
// after which "give-up" force-completes with whatever rows have been saved.
// This eliminates the previous 60-min backstop force-complete window that
// permanently dropped finishers the upstream eventually published.
// 2026-06-20: JRA no longer auto-completes on partial. Two production traps
// (jra:2026:0620:05:01 = locked at top-5, jra:2026:0620:02:02 = saved 5/14)
// proved that JRA Playwright sometimes returns a partial result HTML even
// though the upstream publishes the full field atomically. The previous code
// also returned "complete" when expectedHorseCount === 0 (entry HTML parse
// failure) regardless of how many result rows landed, which locked the race
// at the partial snapshot forever. The new logic:
//   - JRA with expected===0 AND inserted>0 → retry-short (cannot disambiguate
//     "true empty entry list" from "entry parser failed but result has rows")
//   - JRA partial (expected>0 AND inserted<expected) flows through the same
//     retry phases as NAR up to the 24h give-up window
//   - NAR with expected===0 still returns "complete" (cancelled-race case)
export const resolveResultFetchOutcome = (
  input: ResolveResultFetchOutcomeInput,
): ResultFetchOutcome => {
  if (input.expectedHorseCount <= 0) {
    return input.source === "jra" && input.inserted > 0 ? "retry-short" : "complete";
  }
  if (input.inserted >= input.expectedHorseCount) {
    return "complete";
  }
  if (input.minutesAfterRaceStart === null) {
    return "complete";
  }
  if (input.minutesAfterRaceStart >= RESULT_FETCH_GIVE_UP_HOURS * 60) {
    return "give-up";
  }
  if (input.minutesAfterRaceStart < RESULT_FETCH_RETRY_MEDIUM_THRESHOLD_MINUTES) {
    return "retry-short";
  }
  return input.minutesAfterRaceStart < RESULT_FETCH_RETRY_LONG_THRESHOLD_MINUTES
    ? "retry-medium"
    : "retry-long";
};

interface ResolveResultFetchIsCompleteInput {
  expectedHorseCount: number;
  inserted: number;
  outcome: ResultFetchOutcome;
  source: NarRaceSource["source"];
}

// 2026-06-20: Pure helper extracted from handleCompleteResultFetch so the
// isComplete decision can be unit-tested in isolation. The rules are:
//   - give-up always finalizes (force-complete after 24h)
//   - matched fields (inserted >= expected && expected > 0) finalize
//   - NAR with expected===0 finalizes (cancelled-race / true-empty case)
//   - JRA with expected===0 does NOT finalize — resolveResultFetchOutcome
//     reroutes that case to retry-short when inserted>0, so reaching this
//     helper means inserted===0 (transient parse failure) and we want the
//     planner to keep re-enqueuing instead of locking the race forever.
export const resolveResultFetchIsComplete = (input: ResolveResultFetchIsCompleteInput): boolean => {
  if (input.outcome === "give-up") {
    return true;
  }
  if (input.expectedHorseCount > 0 && input.inserted >= input.expectedHorseCount) {
    return true;
  }
  return input.expectedHorseCount === 0 && input.source === "nar";
};

interface ResolveEmptyResultGiveupInput {
  attemptCount: number;
  minutesAfterRaceStart: number;
}

interface HandleEmptyResultFetchInput {
  env: Env;
  now: Date;
  race: NarRaceSource;
  raceKey: string;
}

export type EmptyResultFetchOutcome = "give-up" | "silent-return" | "awaiting-publish";

// 2026-06-28: logFetch status emitted when the empty-result circuit breaker
// trips. Exported so callers can grep telemetry by a single constant string
// and unit tests can assert it without re-typing the literal.
export const EMPTY_RESULT_GIVEUP_LOG_STATUS = "empty_giveup:race_count_exceeded";

// 2026-06-30: pure helper computing minutes elapsed since the race started.
// Returns 0 when minutesUntilRace is null (race start unparseable) so the
// caller does not have to special-case null — a 0-minutes-after-start race
// is treated as still inside the awaiting-publish window, which is the
// safest default for a malformed row.
export const computeMinutesAfterRaceStart = (race: NarRaceSource, now: Date): number => {
  const minutes = minutesUntilRace(race, now);
  return minutes === null ? 0 : -minutes;
};

// 2026-06-30: pure helper for the empty-result circuit breaker. Returns true
// only when BOTH the per-race attempt counter has hit
// RESULT_FETCH_EMPTY_GIVEUP_COUNT AND enough wall-clock has elapsed since
// the race started (RESULT_FETCH_EMPTY_GIVEUP_MIN_MINUTES_AFTER_START). The
// AND gate prevents a clock-skew or retry-storm from tripping the breaker
// before the upstream publish window has even closed — the 2026-06-28
// failure mode where the counter ran out ~5-6 min after race start.
export const resolveEmptyResultGiveup = (input: ResolveEmptyResultGiveupInput): boolean =>
  input.attemptCount >= RESULT_FETCH_EMPTY_GIVEUP_COUNT &&
  input.minutesAfterRaceStart >= RESULT_FETCH_EMPTY_GIVEUP_MIN_MINUTES_AFTER_START;

const reverseBabaCode = (keibajoCode: string): string | null =>
  Object.entries(BABA_CODE_TO_LOCAL_KEIBAJO).find(([, code]) => code === keibajoCode)?.[0] ?? null;

// 2026-07-24: best-effort upstream cross-check for the empty-result circuit
// breaker (see NAR_RESULT_VOID_CHECK_MIN_MINUTES_AFTER_START above). JRA is
// out of scope -- it has no keiba.go.jp RaceList page at all -- and any
// fetch/parse failure fails CLOSED (returns false) so an ambiguous or
// unreachable RaceList page can never force an early give-up; the plain
// attempt-count/time-floor breaker remains the backstop in that case.
export const isNarRaceConfirmedVoidOnRaceList = async (
  env: Env,
  race: NarRaceSource,
): Promise<boolean> => {
  if (race.source !== "nar") return false;
  const babaCode = reverseBabaCode(race.keibajoCode);
  if (babaCode === null) return false;
  try {
    const { url } = buildRaceListUrl(`${race.kaisaiNen}${race.kaisaiTsukihi}`, babaCode);
    const html = await fetchRaceListPageHtml(url);
    if (html === null) return false;
    return isRaceResultDisabledOnRaceList(html, race.raceBango) === true;
  } catch {
    return false;
  }
};

// 2026-06-30: integration helper for the empty-result circuit breaker.
// Four-way outcome:
//   - "awaiting-publish": race finished but the upstream publish window
//     has not opened yet (now - race_start < NAR_RESULT_PUBLISH_DELAY_
//     MINUTES). Counter NOT incremented, lock cleared via failResultFetch
//     so the planner re-enqueues on the next cron tick, single
//     skip:awaiting-publish row written (KV-deduped).
//   - "silent-return": publish window open, counter incremented but the
//     giveup threshold, time floor, AND (for NAR, past its own later floor)
//     the RaceList void cross-check all still say "keep waiting". Lock
//     cleared via failResultFetch so the planner re-enqueues next tick. No
//     log row (the eventual giveup row carries the attempt count; per-tick
//     logging would blow up D1).
//   - "give-up": either the count/time gates passed, or (NAR only, past
//     NAR_RESULT_VOID_CHECK_MIN_MINUTES_AFTER_START) the RaceList page
//     confirms this race's result link is disabled → markEmptyResultGiveUp
//     force-completes the race + a give-up log row (EMPTY_RESULT_GIVEUP_
//     LOG_STATUS for the count/time path, EMPTY_RESULT_VOID_LOG_STATUS for
//     the RaceList-confirmed path).
// Critically: NONE of these outcomes throw. The caller silently returns
// (acks the queue message) so the previous 1-tick = 4-increment retry
// storm cannot reappear.
export const handleEmptyResultFetch = async (
  input: HandleEmptyResultFetchInput,
): Promise<EmptyResultFetchOutcome> => {
  const minutesAfterRaceStart = computeMinutesAfterRaceStart(input.race, input.now);
  if (minutesAfterRaceStart < NAR_RESULT_PUBLISH_DELAY_MINUTES) {
    await failResultFetch(input.env.REALTIME_DB, input.raceKey);
    await logFetch(
      input.env.REALTIME_DB,
      "fetch-results",
      SKIP_STATUS.awaitingPublish,
      input.raceKey,
      null,
      input.env.DETAIL_SECTION_CACHE_KV,
    );
    return "awaiting-publish";
  }
  const attemptCount = await incrementEmptyResultAttempts(input.env.REALTIME_DB, input.raceKey);
  const countGiveup = resolveEmptyResultGiveup({ attemptCount, minutesAfterRaceStart });
  const confirmedVoid =
    !countGiveup &&
    minutesAfterRaceStart >= NAR_RESULT_VOID_CHECK_MIN_MINUTES_AFTER_START &&
    (await isNarRaceConfirmedVoidOnRaceList(input.env, input.race));
  if (!countGiveup && !confirmedVoid) {
    await failResultFetch(input.env.REALTIME_DB, input.raceKey);
    return "silent-return";
  }
  await markEmptyResultGiveUp(input.env.REALTIME_DB, input.raceKey, toJstIsoString(input.now));
  await logFetch(
    input.env.REALTIME_DB,
    "fetch-results",
    confirmedVoid ? EMPTY_RESULT_VOID_LOG_STATUS : EMPTY_RESULT_GIVEUP_LOG_STATUS,
    input.raceKey,
    `attempts=${attemptCount}`,
  );
  return "give-up";
};

const RETRY_LOCK_MINUTES_BY_OUTCOME: ReadonlyMap<ResultFetchOutcome, number> = new Map([
  ["retry-short", RESULT_FETCH_RETRY_LOCK_MINUTES],
  ["retry-medium", RESULT_FETCH_RETRY_MEDIUM_LOCK_MINUTES],
  ["retry-long", RESULT_FETCH_RETRY_LONG_LOCK_MINUTES],
]);

// 2026-06-05: Returns the partial-result lock duration (minutes) the caller
// should apply to recordPartialResultFetch for a given retry-phase outcome.
// Non-retry outcomes throw because the caller is expected to branch on
// outcome before reaching this helper.
export const resolveRetryLockMinutes = (outcome: ResultFetchOutcome): number => {
  const minutes = RETRY_LOCK_MINUTES_BY_OUTCOME.get(outcome);
  if (minutes === undefined) {
    throw new Error(`resolveRetryLockMinutes called with non-retry outcome: ${outcome}`);
  }
  return minutes;
};

interface DispatchResultFetchOutcomeInput {
  entries: Omit<RaceEntry, "fetchedAt">[];
  env: Env;
  expectedHorseCount: number;
  fetchedAt: string;
  inserted: number;
  now: Date;
  outcome: ResultFetchOutcome;
  race: NarRaceSource;
  raceKey: string;
  results: Omit<RaceResult, "fetchedAt">[];
  tanshoOdds: RaceResultTanshoOddsRow[];
  weights: HorseWeight[];
}

interface TriggerFeaturesRebuildAfterResultLandedArgs {
  env: Env;
  inserted: number;
  race: NarRaceSource;
}

// 2026-06-30: Fire-and-forget features-worker R2 parquet rebuild trigger.
// Before this, fetchAndStoreResults wrote race_result_snapshots and never
// notified the features worker, so the per-race parquet stayed pre-result
// until the daily-feature-build cron ticked — which could be hours away or
// miss the date entirely. The viewer race-trend section then served the
// stale parquet so finishes / odds / popularity that landed mid-day were
// invisible until the next cron run (on 2026-06-30 we hand-backfilled 128
// NAR races spanning 06-26..06-28 to recover from this gap). Closing the gap
// structurally here means every successful result row drives an immediate
// parquet rebuild. forwardRaceForFeatures already wraps the underlying fetch
// in a 5s timeout + try / catch + logFetch, so the queue ack is never blocked
// by a hung features worker and a transient error is logged rather than
// kicked back into the queue retry loop.
export const triggerFeaturesRebuildAfterResultLanded = async (
  args: TriggerFeaturesRebuildAfterResultLandedArgs,
): Promise<void> => {
  if (args.inserted <= 0) {
    return;
  }
  await forwardRaceForFeatures(args.env, {
    kaisaiNen: args.race.kaisaiNen,
    kaisaiTsukihi: args.race.kaisaiTsukihi,
    keibajoCode: args.race.keibajoCode,
    raceBango: args.race.raceBango,
    raceKey: args.race.raceKey,
    source: args.race.source,
  });
};

// 2026-06-05: Routes the resolveResultFetchOutcome decision to the right
// storage write + side effects (DO push + viewer trend cache bust). Split
// out of fetchAndStoreResults so the per-outcome branching stays at one
// level of indentation and the helper itself is unit-testable in isolation.
// 2026-06-30: After either branch finishes, fire the features-worker R2
// parquet rebuild so finishes land in the per-race parquet within seconds
// instead of waiting for the next daily-feature-build cron tick.
const dispatchResultFetchOutcome = async (
  input: DispatchResultFetchOutcomeInput,
): Promise<void> => {
  const isRetry =
    input.outcome === "retry-short" ||
    input.outcome === "retry-medium" ||
    input.outcome === "retry-long";
  await (isRetry ? handleRetryResultFetch(input) : handleCompleteResultFetch(input));
  await triggerFeaturesRebuildAfterResultLanded({
    env: input.env,
    inserted: input.inserted,
    race: input.race,
  });
};

const handleRetryResultFetch = async (input: DispatchResultFetchOutcomeInput): Promise<void> => {
  const retryLockMinutes = resolveRetryLockMinutes(input.outcome);
  const retryLockUntil = toJstIsoString(new Date(input.now.getTime() + retryLockMinutes * 60_000));
  await registerResultCacheBust({
    db: input.env.REALTIME_DB,
    fetchedAt: input.fetchedAt,
    isComplete: false,
    raceKey: input.raceKey,
    results: input.results,
  });
  await recordPartialResultFetch(
    input.env.REALTIME_DB,
    input.raceKey,
    input.fetchedAt,
    retryLockUntil,
    {
      expectedHorseCount: input.expectedHorseCount,
      savedHorseCount: input.inserted,
    },
  );
  await pushResultsToRaceTrendDO(
    input.env,
    buildRaceTrendDailyTrackRow({
      entries: input.entries,
      fetchedAt: input.fetchedAt,
      isComplete: false,
      race: input.race,
      results: input.results,
      tanshoOdds: input.tanshoOdds,
      weights: input.weights,
    }),
    input.race,
  );
  await processPendingResultCacheBust(input.env, input.race);
  await logFetch(
    input.env.REALTIME_DB,
    "fetch-results",
    "partial",
    input.raceKey,
    `inserted=${input.inserted} expected=${input.expectedHorseCount} retry-lock-minutes=${retryLockMinutes}`,
  );
};

const handleCompleteResultFetch = async (input: DispatchResultFetchOutcomeInput): Promise<void> => {
  const isComplete = resolveResultFetchIsComplete({
    expectedHorseCount: input.expectedHorseCount,
    inserted: input.inserted,
    outcome: input.outcome,
    source: input.race.source,
  });
  await registerResultCacheBust({
    db: input.env.REALTIME_DB,
    fetchedAt: input.fetchedAt,
    isComplete,
    raceKey: input.raceKey,
    results: input.results,
  });
  await completeResultFetch(input.env.REALTIME_DB, input.raceKey, input.fetchedAt, {
    expectedHorseCount: input.expectedHorseCount,
    isComplete,
    savedHorseCount: input.inserted,
  });
  await pushResultsToRaceTrendDO(
    input.env,
    buildRaceTrendDailyTrackRow({
      entries: input.entries,
      fetchedAt: input.fetchedAt,
      isComplete,
      race: input.race,
      results: input.results,
      tanshoOdds: input.tanshoOdds,
      weights: input.weights,
    }),
    input.race,
  );
  // The outbox only claims a changed result signature or a completion-state
  // transition. Each cache tier advances independently, so retrying one
  // failed tier does not repeat the already-delivered day-wide invalidation.
  await processPendingResultCacheBust(input.env, input.race);
  // Force-completion (24h give-up) is the highest-severity silent finish: the
  // planner stops re-enqueuing forever, so an operator MUST be able to see
  // that this race finalised with fewer horses than the field had.
  if (input.outcome === "give-up") {
    await logFetch(
      input.env.REALTIME_DB,
      "fetch-results",
      SKIP_STATUS.giveUp,
      input.raceKey,
      `inserted=${input.inserted} expected=${input.expectedHorseCount}`,
    );
  }
};

const fetchAndStoreResults = async (env: Env, raceKey: string): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(new Date(now.getTime() + RESULT_FETCH_LOCK_MINUTES * 60_000));
  const claimed = await claimResultFetch(env.REALTIME_DB, raceKey, lockUntil, toJstIsoString(now));
  if (!claimed) {
    // 2026-06-28 (D1 cost optimization): fetch-results runs every 2 min and
    // claim-failed fires every tick a race is locked by another consumer.
    // Forward the KV namespace so identical (raceKey + skip:claim-failed)
    // pairs dedupe within LOG_DEDUPE_TTL_SECONDS while still landing one
    // row per distinct race so an operator can still see lock contention.
    await logFetch(
      env.REALTIME_DB,
      "fetch-results",
      SKIP_STATUS.claimFailed,
      raceKey,
      null,
      env.DETAIL_SECTION_CACHE_KV,
    );
    return;
  }
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    throw new Error(`race source not found: ${raceKey}`);
  }
  if (!isRaceFinished(race, now)) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    await logFetch(
      env.REALTIME_DB,
      "fetch-results",
      SKIP_STATUS.notFinished,
      raceKey,
      null,
      env.DETAIL_SECTION_CACHE_KV,
    );
    return;
  }

  try {
    const fetchedAt = toJstIsoString();
    const resultUrl =
      race.source === "jra"
        ? buildJraResultUrlFromRaceSource(race)
        : buildRaceResultUrl(race.debaUrl);
    if (!resultUrl) {
      throw new Error(`race result url is unavailable: ${raceKey}`);
    }
    const [entryPage, resultPage] = await Promise.all([
      race.source === "jra"
        ? fetchJraResultHtmlWithFallback({
            browserBinding: env.JRA_BROWSER,
            needsParse: (html) => parseJraRaceEntries(html).length > 0,
            url: race.debaUrl,
          }).then((html): ResultEntryPage => ({ html, notFound: false, storedEntries: null }))
        : fetchNarEntryPageForResult(env, race),
      race.source === "jra"
        ? fetchJraResultHtmlWithFallback({
            browserBinding: env.JRA_BROWSER,
            needsParse: (html) =>
              parseJraRaceResults(html).length > 0 ||
              parseJraRaceResultExcludedHorseNumbers(html).length > 0,
            url: resultUrl,
          }).then((html): ResultPage => ({ html, notFound: false }))
        : fetchNarResultPage(resultUrl),
    ]);
    const resultHtml = resultPage.html;
    const parsedEntries =
      race.source === "jra"
        ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(entryPage.html), null)
        : parseRaceEntries(entryPage.html);
    const entries = entryPage.storedEntries ?? parsedEntries;
    if (entryPage.storedEntries === null && !entryPage.notFound) {
      await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
    }
    const parsedEntryHorseNumbers =
      entryPage.storedEntries !== null || race.source === "jra"
        ? entries.map((entry) => entry.horseNumber)
        : parseRaceEntryHorseNumbers(entryPage.html);
    const excludedHorseNumbers = new Set(
      race.source === "jra"
        ? [
            ...entries
              .filter((entry) => entry.status && isJraScratchStatus(entry.status))
              .map((entry) => entry.horseNumber),
            ...parseJraRaceResultExcludedHorseNumbers(resultHtml),
          ]
        : parseRaceResultExcludedHorseNumbers(resultHtml),
    );
    const results =
      race.source === "jra" ? parseJraRaceResults(resultHtml) : parseRaceResults(resultHtml);
    // A NAR 404 explicitly means that the page is not published, so route it
    // through the empty-result state machine and acknowledge this queue job.
    // If neither page returned that explicit signal, zero parsed rows remain a
    // transient parser/upstream failure and must keep the Queue retry behavior.
    if (
      parsedEntryHorseNumbers.length === 0 &&
      results.length === 0 &&
      race.source === "nar" &&
      (entryPage.notFound || resultPage.notFound)
    ) {
      await handleEmptyResultFetch({ env, now, race, raceKey });
      return;
    }
    if (parsedEntryHorseNumbers.length === 0 && results.length === 0) {
      throw new Error(`race entry rows are empty: ${raceKey}`);
    }
    const resolvedTrendEntries = await resolveRaceTrendEntries(env, raceKey, entries, results);
    const effectiveEntryHorseNumbers =
      parsedEntryHorseNumbers.length > 0
        ? parsedEntryHorseNumbers
        : resolvedTrendEntries.entries.map((entry) => entry.horseNumber);
    const expectedHorseCount =
      resolvedTrendEntries.source === "results" && race.source === "nar"
        ? results.length + 1
        : effectiveEntryHorseNumbers.filter((horseNumber) => !excludedHorseNumbers.has(horseNumber))
            .length;
    if (expectedHorseCount > 0 && results.length === 0) {
      // 2026-06-30: NEVER throw on empty result. Throwing triggers the
      // Cloudflare queue auto-retry (default max_retries=3 in
      // wrangler.jsonc) which previously turned 1 cron tick into 4
      // empty_result_attempts increments — the breaker tripped ~5-6 min
      // after race start, BEFORE the NAR upstream's typical +10-15 min
      // publish window opened. handleEmptyResultFetch now owns the full
      // state transition (failResultFetch / markEmptyResultGiveUp) and
      // returns one of three outcomes, all of which silently ack the
      // queue message so the planner re-enqueues on the next 2-min tick.
      await handleEmptyResultFetch({ env, now, race, raceKey });
      return;
    }
    await resetEmptyResultAttempts(env.REALTIME_DB, raceKey);
    const inserted = await insertRaceResultSnapshot(env.REALTIME_DB, raceKey, fetchedAt, results);
    const [latestWeights, latestOdds] = await Promise.all([
      getLatestHorseWeights(env.REALTIME_DB, raceKey),
      race.source === "jra" ? fetchHotOddsPayload(env, raceKey) : Promise.resolve(null),
    ]);
    const tanshoOdds =
      race.source === "nar" && results.length > 0
        ? parseRaceResultTanshoOdds(resultHtml)
        : buildTanshoOddsRowsFromHotOdds(latestOdds);
    const weightsForTrend = latestWeights?.horses ?? [];
    if (race.source === "nar") {
      // Backfill the hot worker's odds_snapshots with the final 単勝オッズ /
      // 人気 parsed from the same result HTML. This is a safety net for races
      // where live odds polling missed the final pre-race snapshot (e.g. the
      // 2026-06-29 trend section showing blanks for sibling rows).
      await forwardNarTanshoOddsToHot(env, {
        fetchedAt,
        raceKey,
        rows: tanshoOdds,
      });
    }
    // isRaceFinished above guarantees minutesUntilRace(race, now) is non-null and
    // <= 0, so the non-null assertion here is provably safe (same pattern as the
    // `match[1]!` regex captures elsewhere in this file). Keeping it as `!`
    // avoids a defensive `?? null` arm that v8 would mark as a dead branch.
    const minutesAfterRaceStart = -minutesUntilRace(race, now)!;
    // 2026-06-05: replaces the old baseComplete + NAR-backstop + partial-retry
    // chain with a single outcome resolver. NAR partial results progressively
    // retry (retry-short / retry-medium / retry-long) up to
    // RESULT_FETCH_GIVE_UP_HOURS (24h) before falling through to a forced
    // completion. JRA always lands on "complete" because the JRA result HTML
    // publishes the full field atomically (no progressive publish window to
    // retry through).
    const outcome = resolveResultFetchOutcome({
      expectedHorseCount,
      inserted,
      minutesAfterRaceStart,
      source: race.source,
    });
    await dispatchResultFetchOutcome({
      entries: resolvedTrendEntries.entries,
      env,
      expectedHorseCount,
      fetchedAt,
      inserted,
      now,
      outcome,
      race,
      raceKey,
      results,
      tanshoOdds,
      weights: weightsForTrend,
    });
  } catch (error) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    throw error;
  }
};

const fetchAndStoreJraTrackCondition = async (
  env: Env,
  params: { date: string; keibajoCode: string },
): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(
    new Date(now.getTime() + TRACK_CONDITION_FETCH_LOCK_MINUTES * 60_000),
  );
  const claimed = await claimTrackConditionFetch(env.REALTIME_DB, {
    date: params.date,
    keibajoCode: params.keibajoCode,
    lockUntil,
    now: toJstIsoString(now),
  });
  if (!claimed) {
    return;
  }

  try {
    await ensureJraRaceSourcesAreCurrent(env, params.date);
    const fetchedAt = toJstIsoString();
    const condition = await fetchJraTrackConditionWithPlaywright(env.JRA_BROWSER, {
      kaisaiNen: params.date.slice(0, 4),
      keibajoCode: params.keibajoCode,
    });
    const payload = { ...condition, fetchedAt };
    const races = await insertJraTrackConditionSnapshot(env.REALTIME_DB, {
      condition: payload,
      date: params.date,
      fetchedAt,
      keibajoCode: params.keibajoCode,
    });
    await completeTrackConditionFetch(env.REALTIME_DB, {
      date: params.date,
      fetchedAt,
      keibajoCode: params.keibajoCode,
    });
    await Promise.all(
      races
        .filter((race) => new Date(fetchedAt).getTime() <= new Date(race.raceStartAtJst).getTime())
        .map((race) => writeCachedTrackCondition(env, race.raceKey, payload)),
    );
  } catch (error) {
    await failTrackConditionFetch(env.REALTIME_DB, params);
    throw error;
  }
};

// Mirrors isPremiumRaceDataTarget's source/keibajoCode check but operates on
// the raw raceKey string (shape "<source>:<year>:<mmdd>:<keibajoCode>:<raceBango>")
// so the queue handler's defensive pre-fetch guard does not need a race lookup.
const isPremiumRaceDataQueueRaceKey = (raceKey: string): boolean => {
  const parts = raceKey.split(":");
  return parts[0] === "jra" || (parts[0] === "nar" && parts[3] !== BAN_EI_KEIBAJO_CODE);
};

const normalizePremiumTrainingReview = (row: PremiumTrainingReview): PremiumTrainingReview => ({
  commentText: row.commentText,
  evaluationGrade: row.evaluationGrade,
  evaluationText: row.evaluationText,
  horseName: row.horseName,
  horseNumber: row.horseNumber,
  riderName: row.riderName,
  trainingDate: row.trainingDate,
});

const normalizePremiumStableComment = (row: PremiumStableComment): PremiumStableComment => ({
  commentText: row.commentText,
  evaluationGrade: row.evaluationGrade,
  evaluationText: row.evaluationText,
  frameNumber: row.frameNumber,
  horseName: row.horseName,
  horseNumber: row.horseNumber,
});

const normalizePremiumDataTopHorse = (row: PremiumDataTopHorse): PremiumDataTopHorse => ({
  horseName: row.horseName,
  horseNumber: row.horseNumber,
  rank: row.rank,
  reasons: row.reasons,
});

const comparePremiumTrainingReviews = (
  left: PremiumTrainingReview,
  right: PremiumTrainingReview,
): number =>
  `${left.horseNumber}:${left.trainingDate}`.localeCompare(
    `${right.horseNumber}:${right.trainingDate}`,
  );

const comparePremiumStableComments = (
  left: PremiumStableComment,
  right: PremiumStableComment,
): number => left.horseNumber.localeCompare(right.horseNumber);

const comparePremiumDataTopHorses = (
  left: PremiumDataTopHorse,
  right: PremiumDataTopHorse,
): number => left.rank - right.rank;

const premiumTrainingReviewsSignature = (rows: readonly PremiumTrainingReview[]): string =>
  JSON.stringify(rows.map(normalizePremiumTrainingReview).toSorted(comparePremiumTrainingReviews));

const premiumStableCommentsSignature = (rows: readonly PremiumStableComment[]): string =>
  JSON.stringify(rows.map(normalizePremiumStableComment).toSorted(comparePremiumStableComments));

const premiumDataTopHorsesSignature = (rows: readonly PremiumDataTopHorse[]): string =>
  JSON.stringify(rows.map(normalizePremiumDataTopHorse).toSorted(comparePremiumDataTopHorses));

const hasPremiumRaceDataChanged = (
  previous: PremiumRacePayload | null,
  next: PremiumRaceDataSections,
): boolean => {
  if (previous === null) {
    return (
      (next.trainingReviews?.length ?? 0) > 0 ||
      (next.stableComments?.length ?? 0) > 0 ||
      (next.dataTopHorses?.length ?? 0) > 0
    );
  }
  return (
    (next.trainingReviews !== undefined &&
      premiumTrainingReviewsSignature(next.trainingReviews) !==
        premiumTrainingReviewsSignature(previous.trainingReviews)) ||
    (next.stableComments !== undefined &&
      premiumStableCommentsSignature(next.stableComments) !==
        premiumStableCommentsSignature(previous.stableComments)) ||
    (next.dataTopHorses !== undefined &&
      premiumDataTopHorsesSignature(next.dataTopHorses) !==
        premiumDataTopHorsesSignature(previous.dataTopHorses))
  );
};

const fetchAndStorePremiumRaceData = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || !isPremiumRaceDataTarget(race)) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-race-data",
      SKIP_STATUS.raceNotFound,
      raceKey,
      null,
    );
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-race-data",
      SKIP_STATUS.configMissing,
      raceKey,
      null,
    );
    return;
  }
  const link = await ensurePremiumRaceLink(env, race);
  if (!link) {
    throw new Error(`premium race link not found: ${raceKey}`);
  }
  const [workUrl, commentUrl, dataTopUrl] = [
    race.source === "jra"
      ? buildPremiumUrl(
          config,
          config.workPathTemplate,
          { sourceRaceId: link.sourceRaceId },
          { source: race.source },
        )
      : null,
    race.source === "jra"
      ? buildPremiumUrl(
          config,
          config.commentPathTemplate,
          { sourceRaceId: link.sourceRaceId },
          { source: race.source },
        )
      : null,
    buildPremiumUrl(
      config,
      config.dataTopPathTemplate,
      { sourceRaceId: link.sourceRaceId },
      { source: race.source },
    ),
  ];
  const fetchedAt = toJstIsoString();
  const [workResult, commentResult, dataTopResult] = await Promise.allSettled([
    workUrl ? fetchPremiumHtml(config, workUrl) : Promise.resolve(""),
    commentUrl ? fetchPremiumHtml(config, commentUrl) : Promise.resolve(""),
    dataTopUrl ? fetchPremiumHtml(config, dataTopUrl) : Promise.resolve(""),
  ]);
  const workHtml = workResult.status === "fulfilled" ? workResult.value : "";
  const commentHtml = commentResult.status === "fulfilled" ? commentResult.value : "";
  const dataTopHtml = dataTopResult.status === "fulfilled" ? dataTopResult.value : "";
  if (!workHtml && !commentHtml && !dataTopHtml) {
    const retryAfter = toJstIsoString(
      new Date(getNow(env).getTime() + PREMIUM_RACE_DATA_RETRY_DELAY_SECONDS * 1000),
    );
    await updatePremiumRaceDataFetchState(env.REALTIME_DB, {
      message: [workResult, commentResult, dataTopResult]
        .flatMap((result) =>
          result.status === "rejected"
            ? [result.reason instanceof Error ? result.reason.message : String(result.reason)]
            : [],
        )
        .join("; "),
      raceKey,
      retryAfter,
      status: "failed",
    });
    throw new Error(`premium race data fetch failed: ${raceKey}`);
  }
  const trainingReviews = workHtml ? parsePremiumTrainingReviews(workHtml, env) : undefined;
  const parsedStableComments = commentHtml
    ? parsePremiumStableComments(commentHtml, env)
    : undefined;
  const dataTopHorses = dataTopHtml ? parsePremiumDataTopHorses(dataTopHtml, env) : undefined;
  const commentAuthorized = commentHtml ? isPremiumStableCommentHtmlAuthorized(commentHtml) : false;
  // netkeiba's unauthenticated data-top teaser page structurally contains
  // exactly one <dl> block matching parsePremiumDataTopHorses's pattern, so a
  // naive parse silently "succeeds" with one fabricated horse pick instead of
  // the real 2-3. detectPremiumLoginPrompt below is tuned to a different gate
  // string that this teaser page does not contain (verified in production:
  // 1000/1000 unauthenticated NAR fetches 2026-05-31..2026-06-28 carried the
  // teaser markers but never tripped loginPromptDetected), so data-top needs
  // its own authenticity check rather than relying on that shared detector.
  const dataTopAuthorized = dataTopHtml ? isPremiumDataTopHtmlAuthorized(dataTopHtml) : false;
  const dataTopAuthRequired = Boolean(dataTopHtml) && !dataTopAuthorized;
  // Gate text also appears in authenticated page chrome. Treat it as an auth
  // failure only when that specific page lacks its authoritative positive
  // signal/content. This preserves login-only detection without retrying a
  // fully fetched race forever because a sibling page contains an upsell.
  const workLoginPromptDetected =
    detectPremiumLoginPrompt(workHtml) && (trainingReviews?.length ?? 0) === 0;
  const commentLoginPromptDetected = detectPremiumLoginPrompt(commentHtml) && !commentAuthorized;
  const dataTopLoginPromptDetected = detectPremiumLoginPrompt(dataTopHtml) && !dataTopAuthorized;
  const loginPromptDetected =
    workLoginPromptDetected || commentLoginPromptDetected || dataTopLoginPromptDetected;
  // Suppress the stable-comment replace when the proxy returned the preview
  // (unauthenticated) page: otherwise the unauth response (typically 3 rows)
  // would overwrite a previously stored authenticated snapshot (full field).
  // The fetch state below still records `commentAuthRequired: true` so the
  // planner re-queues the race.
  const stableComments = commentHtml && !commentAuthorized ? undefined : parsedStableComments;
  // Suppress data_top replace only when the data-top page itself fails its
  // own authenticity check (dataTopAuthRequired, backed by
  // isPremiumDataTopHtmlAuthorized). That gate is authoritative for this page
  // and strictly stronger than the shared loginPromptDetected check: the
  // shared detector is known to misfire on upsell furniture present on the
  // sibling work/comment pages even when the data-top page itself is fully
  // authenticated, so it must not be used to suppress data-top persistence
  // (it wrongly wiped valid, already-fetched data-top data in production).
  const dataTopHorsesForReplace = dataTopAuthRequired ? undefined : dataTopHorses;
  const previousPayload = await getPremiumRacePayload(env.REALTIME_DB, raceKey).catch(() => null);
  const premiumRaceDataChanged = hasPremiumRaceDataChanged(previousPayload, {
    dataTopHorses: dataTopHorsesForReplace,
    stableComments,
    trainingReviews,
  });
  await replacePremiumRaceData(env.REALTIME_DB, {
    dataTopHorses: dataTopHorsesForReplace,
    fetchedAt,
    link,
    raceKey,
    stableComments,
    trainingReviews,
  });
  if (dataTopHorsesForReplace && dataTopHorsesForReplace.length > 0) {
    await putPremiumDataTopCache({
      env,
      race,
      rows: dataTopHorsesForReplace.map((row) => ({ ...row, fetchedAt })),
    });
  }
  const hasAnyData =
    (trainingReviews?.length ?? 0) > 0 ||
    (parsedStableComments?.length ?? 0) > 0 ||
    (dataTopHorses?.length ?? 0) > 0;
  // The premium queue polls hot races repeatedly. Compare semantic content
  // against the durable D1 snapshot (ignoring fetchedAt) so an unchanged poll
  // does not fan out another Viewer invalidation + warm. A first insert,
  // repaired section, or authorized removal still busts immediately.
  if (premiumRaceDataChanged) {
    await runRaceCacheBust(env, raceKey, race);
  }
  const commentAuthRequired = Boolean(commentHtml) && !commentAuthorized;
  // Data-top's own authenticity failure feeds the same shared auth-retry
  // backoff as the login-prompt detector: both mean "this race needs a
  // re-fetch once the session is authenticated again", not "fetch failed".
  const anyAuthIssueDetected = loginPromptDetected || dataTopAuthRequired;
  const previousState = await getPremiumRaceDataFetchState(env.REALTIME_DB, raceKey);
  const previousMessage = parsePremiumStateMessage(previousState?.message ?? null);
  const nextAuthRetryCount = anyAuthIssueDetected ? previousMessage.authRetryCount + 1 : 0;
  const authRetryExhausted = nextAuthRetryCount > PREMIUM_RACE_DATA_AUTH_RETRY_MAX_ATTEMPTS;
  const authRetryAfter = anyAuthIssueDetected
    ? toJstIsoString(
        new Date(getNow(env).getTime() + resolveAuthRetryDelaySeconds(authRetryExhausted) * 1000),
      )
    : null;
  const resolvedStatus = resolvePremiumRaceDataStatus({
    commentAuthRequired,
    dataTopAuthRequired,
    hasAnyData,
    loginPromptDetected,
  });
  await updatePremiumRaceDataFetchState(env.REALTIME_DB, {
    fetchedAt,
    message: JSON.stringify({
      authRetryCount: nextAuthRetryCount,
      commentAuthRequired,
      commentError:
        commentResult.status === "rejected"
          ? commentResult.reason instanceof Error
            ? commentResult.reason.message
            : String(commentResult.reason)
          : null,
      commentHtmlLength: commentHtml.length,
      dataTopCount: dataTopHorses?.length ?? null,
      dataTopError:
        dataTopResult.status === "rejected"
          ? dataTopResult.reason instanceof Error
            ? dataTopResult.reason.message
            : String(dataTopResult.reason)
          : null,
      dataTopHtmlLength: dataTopHtml.length,
      dataTopAuthorized: dataTopHtml ? dataTopAuthorized : null,
      dataTopPersisted: dataTopHorsesForReplace !== undefined,
      dataTopHasIconAccount: dataTopHtml ? dataTopHtml.includes("Icon_Account") : null,
      dataTopHasDummyBox: dataTopHtml ? dataTopHtml.includes("DummyBox") : null,
      dataTopHasPremiumRegist: dataTopHtml ? dataTopHtml.includes("Premium_Regist_Box") : null,
      dataTopHasIconLogin: dataTopHtml ? dataTopHtml.includes("Icon_Login") : null,
      dataTopHasLogout: dataTopHtml ? dataTopHtml.includes("ログアウト") : null,
      dataTopDlBlockCount: dataTopHtml ? (dataTopHtml.match(/<dl\b/giu)?.length ?? 0) : null,
      loginPromptDetected,
      stableCommentCount: parsedStableComments?.length ?? null,
      stableCommentPersisted: stableComments !== undefined,
      stableCommentSample:
        commentHtml && (parsedStableComments?.length ?? 0) === 0
          ? summarizePremiumStableCommentHtml(commentHtml)
          : null,
      trainingReviewCount: trainingReviews?.length ?? null,
      workError:
        workResult.status === "rejected"
          ? workResult.reason instanceof Error
            ? workResult.reason.message
            : String(workResult.reason)
          : null,
      workHtmlLength: workHtml.length,
    }),
    raceKey,
    retryAfter: authRetryAfter,
    status: resolvedStatus,
  });
};

interface ResolvePremiumStatusInput {
  commentAuthRequired: boolean;
  dataTopAuthRequired: boolean;
  hasAnyData: boolean;
  loginPromptDetected: boolean;
}

const resolvePremiumRaceDataStatus = (input: ResolvePremiumStatusInput): string => {
  if (input.loginPromptDetected || input.commentAuthRequired || input.dataTopAuthRequired) {
    return "auth_required";
  }
  return input.hasAnyData ? "ok" : "empty";
};

const resolveAuthRetryDelaySeconds = (exhausted: boolean): number =>
  exhausted
    ? PREMIUM_RACE_DATA_AUTH_RETRY_BACKOFF_SECONDS
    : PREMIUM_RACE_DATA_AUTH_RETRY_DELAY_SECONDS;

const isPresentString = (value: string | null): value is string => value !== null;

interface PremiumPaddockProxyCachePurgeConfig {
  proxyBearer: string;
  proxyUrl: string;
  proxyUserId: string;
}

const hasPremiumPaddockProxyCachePurgeConfig = (
  config: ReturnType<typeof getPremiumRaceConfig>,
): config is ReturnType<typeof getPremiumRaceConfig> & PremiumPaddockProxyCachePurgeConfig =>
  config.proxyBearer !== null && config.proxyUrl !== null && config.proxyUserId !== null;

export const buildPremiumPaddockProxyCachePurgeRequest = (
  config: ReturnType<typeof getPremiumRaceConfig>,
  targetUrl: string,
): Request | null => {
  if (!hasPremiumPaddockProxyCachePurgeConfig(config)) {
    return null;
  }
  const requestUrl = new URL(config.proxyUrl);
  if (requestUrl.searchParams.get("cache") === "0") {
    return null;
  }
  requestUrl.searchParams.set("url", targetUrl);
  requestUrl.searchParams.set("user_id", config.proxyUserId);
  return new Request(requestUrl.toString(), {
    headers: { Authorization: `Bearer ${config.proxyBearer}` },
    method: "DELETE",
  });
};

const purgePremiumPaddockProxyCache = async (
  config: ReturnType<typeof getPremiumRaceConfig>,
  targetUrl: string,
): Promise<void> => {
  const request = buildPremiumPaddockProxyCachePurgeRequest(config, targetUrl);
  if (!request) {
    return;
  }
  await fetch(request).catch(() => undefined);
};

export const buildPremiumPaddockUrls = (
  config: ReturnType<typeof getPremiumRaceConfig>,
  sourceRaceId: string,
): string[] =>
  [
    buildPremiumUrl(config, config.paddockPathTemplate, { sourceRaceId }),
    buildPremiumUrl(config, config.paddockFallbackPathTemplate, { sourceRaceId }),
  ].filter(isPresentString);

const fetchPremiumPaddockHtmlAttempts = async (
  config: ReturnType<typeof getPremiumRaceConfig>,
  urls: readonly string[],
  sourceRaceId: string,
): Promise<Awaited<ReturnType<typeof fetchPremiumHtmlAttempts>>> => {
  if (urls.length === 0) {
    throw new Error(`premium paddock url not found: ${sourceRaceId}`);
  }
  const results = await Promise.allSettled(
    urls.map(async (url) => {
      await purgePremiumPaddockProxyCache(config, url);
      return fetchPremiumHtmlAttempts(config, url);
    }),
  );
  const fulfilledAttempts = results.flatMap((result) =>
    result.status === "fulfilled" ? [result.value] : [],
  );
  const allAttempts = fulfilledAttempts.flat();
  if (allAttempts.length > 0) {
    return allAttempts;
  }
  const failure = results.find((result) => result.status === "rejected");
  throw failure?.reason ?? new Error(`premium paddock fetch returned no attempts: ${sourceRaceId}`);
};

const fetchAndStorePremiumPaddock = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || race.source !== "jra") {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-paddock",
      SKIP_STATUS.raceNotFound,
      raceKey,
      null,
    );
    return;
  }
  const currentState = await getPremiumPaddockFetchState(env.REALTIME_DB, raceKey);
  if (
    currentState?.retryAfter &&
    new Date(currentState.retryAfter).getTime() > getNow(env).getTime()
  ) {
    // 2026-06-28 (D1 cost optimization): paddock fetch retries every */2 min
    // while a race is in cooldown; without dedupe each retry writes a
    // distinct lock-held row even though the underlying state has not
    // changed. KV dedupe keeps one row per (raceKey + status) window.
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-paddock",
      SKIP_STATUS.lockHeld,
      raceKey,
      null,
      env.DETAIL_SECTION_CACHE_KV,
    );
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-paddock",
      SKIP_STATUS.configMissing,
      raceKey,
      null,
      env.DETAIL_SECTION_CACHE_KV,
    );
    return;
  }
  const link = await ensurePremiumRaceLink(env, race);
  if (!link) {
    throw new Error(`premium race link not found: ${raceKey}`);
  }
  const paddockUrls = buildPremiumPaddockUrls(config, link.sourceRaceId);
  if (paddockUrls.length === 0) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-paddock",
      SKIP_STATUS.paddockUrlMissing,
      raceKey,
      null,
    );
    return;
  }
  const attempts = await fetchPremiumPaddockHtmlAttempts(
    config,
    paddockUrls,
    link.sourceRaceId,
  ).catch(async (error: unknown) => {
    const existingPayload = await getPremiumRacePayload(env.REALTIME_DB, raceKey).catch(() => null);
    if (existingPayload && existingPayload.paddockBulletins.length > 0) {
      const latestFetchedAt = existingPayload.paddockBulletins.reduce<string | null>(
        (latest, row) => (latest && latest > row.fetchedAt ? latest : row.fetchedAt),
        null,
      );
      await updatePremiumPaddockFetchState(env.REALTIME_DB, {
        fetchedAt: latestFetchedAt,
        message: null,
        raceKey,
        status: "ok",
      });
      return;
    }
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      message: formatError(error),
      raceKey,
      retryAfter,
      status: "failed",
    });
    throw error;
  });
  if (!attempts) {
    return;
  }
  const parsedAttempts = attempts.map((attempt) => ({
    mode: attempt.mode,
    parsed: parsePremiumPaddockBulletins(attempt.html, env),
  }));
  const selectedAttempt =
    parsedAttempts.find((attempt) => attempt.parsed.bulletins.length > 0) ??
    parsedAttempts.find((attempt) => attempt.mode === "proxy" && attempt.parsed.authRequired) ??
    parsedAttempts.find((attempt) => attempt.parsed.pending) ??
    parsedAttempts[0];
  if (!selectedAttempt) {
    throw new Error(`premium paddock fetch returned no attempts: ${raceKey}`);
  }
  const parsed = selectedAttempt.parsed;
  const fetchedAt = toJstIsoString();
  if (parsed.authRequired) {
    await logFetch(
      env.REALTIME_DB,
      "fetch-premium-paddock",
      SKIP_STATUS.authRequired,
      raceKey,
      selectedAttempt.mode,
    );
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `auth_required:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "auth_required",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock auth required: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "auth_required",
      status: "skipped_auth_required",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock auth required: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "auth_required",
      status: "skipped_auth_required",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.unavailable) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `unavailable:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "unavailable",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock is unavailable: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "unavailable",
      status: "skipped_unavailable",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock is unavailable: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "unavailable",
      status: "skipped_unavailable",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.pending) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `pending:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "pending",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock is pending: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "pending",
      status: "skipped_pending",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock is pending: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "pending",
      status: "skipped_pending",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  if (parsed.bulletins.length === 0) {
    await clearCachedPremiumPaddock(env, raceKey);
    const retryAfter = getPremiumPaddockRetryAfter(env, race);
    const payloadSignature = await buildPremiumPaddockSignature([]);
    await updatePremiumPaddockFetchState(env.REALTIME_DB, {
      fetchedAt,
      message: `empty:${selectedAttempt.mode}`,
      raceKey,
      retryAfter,
      status: "empty",
    });
    await recordPremiumPaddockNotificationEvent(env.REALTIME_DB, {
      fetchedAt,
      message: `premium paddock rows are empty: ${selectedAttempt.mode}`,
      payloadSignature,
      raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await updatePremiumPaddockNotificationState(env.REALTIME_DB, {
      message: `premium paddock rows are empty: ${selectedAttempt.mode}`,
      payloadFetchedAt: fetchedAt,
      payloadSignature,
      raceKey,
      skipReason: "empty",
      status: "skipped_empty",
    });
    await retryPremiumPaddockWhileInWindow(env, race);
    return;
  }
  await replacePremiumRaceData(env.REALTIME_DB, {
    fetchedAt,
    link,
    paddockBulletins: parsed.bulletins,
    raceKey,
  });
  const payload = await getPremiumRacePayload(env.REALTIME_DB, raceKey);
  await writeCachedPremiumPaddock(env, raceKey, {
    fetchedAt,
    paddockBulletins: payload.paddockBulletins,
  });
  // parsed.bulletins.length > 0 is guaranteed here (the empty path returned above).
  await updatePremiumPaddockFetchState(env.REALTIME_DB, {
    fetchedAt,
    message: null,
    raceKey,
    status: "ok",
  });
  await notifyPremiumPaddockIfNeeded(env, race, parsed.bulletins, fetchedAt);
};

// 2026-06-28: queue-stall preventive guard. Previously the tail of
// `handleJob` fell through to `fetchAndStoreWeights(env, job.raceKey)` for any
// `Job.type` not matched above. Adding a future variant without a handler
// would silently route it into the NAR weight scrape path. Be explicit:
// fetch-weights goes through the timeout wrapper; anything else logs as an
// unknown type and returns. The argument is intentionally typed `unknown` so
// TypeScript's exhaustive narrowing in `handleJob` does not collapse this to
// `never` -- this guard is a runtime safety net against malformed messages,
// not a type-level fallthrough handler.
const handleFetchWeightsOrUnknown = async (env: Env, job: unknown): Promise<void> => {
  const fetchWeightsJob = pickFetchWeightsJob(job);
  if (!fetchWeightsJob) {
    await logFetch(
      env.REALTIME_DB,
      "unknown-job-type",
      "error",
      pickRaceKey(job),
      JSON.stringify({ type: pickJobType(job) }),
    );
    return;
  }
  if (await skipStaleLiveRealtimeJob(env, fetchWeightsJob.type, fetchWeightsJob.raceKey)) {
    return;
  }
  // 2026-06-28: NAR weight scrapes can hang on dead or rate-limited NAR
  // upstreams. Same 30s runtime cancel concern as fetch-results above.
  const stored = await withHandlerTimeout({
    label: "fetch-weights",
    ms: QUEUE_HANDLER_TIMEOUT_MS,
    task: fetchAndStoreWeights(
      env,
      fetchWeightsJob.raceKey,
      fetchWeightsJob.watchdogReservedAt ?? null,
      fetchWeightsJob.weightGeneration ?? null,
    ),
  });
  if (!stored) return;
  await logFetch(env.REALTIME_DB, fetchWeightsJob.type, "ok", fetchWeightsJob.raceKey, null);
};

interface FetchWeightsJobShape {
  raceKey: string;
  type: "fetch-weights";
  watchdogReservedAt?: string;
  weightGeneration?: WeightSnapshotGeneration;
}

const raceKeyDateYmd = (raceKey: string): string | null => {
  const parts = raceKey.split(":");
  if (parts.length !== RACE_KEY_PART_COUNT) return null;
  const year = parts[1];
  const monthDay = parts[2];
  if (!year || !monthDay || !/^\d{4}$/u.test(year) || !/^\d{4}$/u.test(monthDay)) return null;
  return `${year}${monthDay}`;
};

const skipStaleLiveRealtimeJob = async (
  env: Env,
  jobType: "fetch-results" | "fetch-weights",
  raceKey: string,
): Promise<boolean> => {
  const raceDate = raceKeyDateYmd(raceKey);
  if (!raceDate) return false;
  const today = getTodayJst(getNow(env));
  const staleBefore = jobType === "fetch-results" ? addDaysToYyyymmdd(today, -1) : today;
  if (raceDate >= staleBefore) return false;
  await logFetch(
    env.REALTIME_DB,
    jobType,
    "skip:stale-live-job",
    raceKey,
    JSON.stringify({ raceDate, today }),
    env.DETAIL_SECTION_CACHE_KV,
  );
  return true;
};

const isObjectRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isCanonicalHorseNumberArray = (value: unknown): value is number[] =>
  Array.isArray(value) &&
  value.every(
    (horseNumber, index) =>
      Number.isInteger(horseNumber) &&
      horseNumber > 0 &&
      (index === 0 || value[index - 1] < horseNumber),
  );

const hasRunnerSnapshotFields = (value: Record<string, unknown>): boolean =>
  value.activeHorseNumbers !== undefined ||
  value.entrySnapshotFetchedAt !== undefined ||
  value.entrySnapshotHash !== undefined ||
  value.excludedHorseNumbers !== undefined;

export const pickWeightGeneration = (value: unknown): WeightSnapshotGeneration | null => {
  if (!isObjectRecord(value)) return null;
  if (!Number.isInteger(value.weightSnapshotCount) || Number(value.weightSnapshotCount) <= 0)
    return null;
  if (typeof value.weightSnapshotFetchedAt !== "string") return null;
  if (typeof value.weightSnapshotHash !== "string") return null;
  const generation: WeightSnapshotGeneration = {
    weightSnapshotCount: Number(value.weightSnapshotCount),
    weightSnapshotFetchedAt: value.weightSnapshotFetchedAt,
    weightSnapshotHash: value.weightSnapshotHash,
  };
  if (!hasRunnerSnapshotFields(value)) return generation;
  if (!isCanonicalHorseNumberArray(value.activeHorseNumbers)) return null;
  if (value.activeHorseNumbers.length === 0) return null;
  if (!isCanonicalHorseNumberArray(value.excludedHorseNumbers)) return null;
  if (typeof value.entrySnapshotFetchedAt !== "string") return null;
  if (typeof value.entrySnapshotHash !== "string") return null;
  const active = new Set(value.activeHorseNumbers);
  if (value.excludedHorseNumbers.some((horseNumber) => active.has(horseNumber))) return null;
  return {
    ...generation,
    activeHorseNumbers: value.activeHorseNumbers,
    entrySnapshotFetchedAt: value.entrySnapshotFetchedAt,
    entrySnapshotHash: value.entrySnapshotHash,
    excludedHorseNumbers: value.excludedHorseNumbers,
  };
};

const pickFetchWeightsJob = (job: unknown): FetchWeightsJobShape | null => {
  if (!isObjectRecord(job)) return null;
  if (job.type !== "fetch-weights") return null;
  if (typeof job.raceKey !== "string") return null;
  const weightGeneration = pickWeightGeneration(job.weightGeneration);
  if (job.weightGeneration !== undefined && weightGeneration === null) return null;
  return {
    raceKey: job.raceKey,
    type: "fetch-weights",
    ...(typeof job.watchdogReservedAt === "string"
      ? { watchdogReservedAt: job.watchdogReservedAt }
      : {}),
    ...(weightGeneration === null ? {} : { weightGeneration }),
  };
};

const pickRaceKey = (job: unknown): string | null => {
  if (!isObjectRecord(job)) return null;
  return typeof job.raceKey === "string" ? job.raceKey : null;
};

const pickJobType = (job: unknown): string | null => {
  if (!isObjectRecord(job)) return null;
  return typeof job.type === "string" ? job.type : null;
};

export const handleJob = async (env: Env, job: Job): Promise<void> => {
  try {
    if (job.type === "discover-urls") {
      const result = await upsertDiscoveredUrls(env, job.date, { sleep: defaultDiscoverSleep });
      const premiumResult = await discoverPremiumRacesForDate(env, job.date);
      const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, job.date);
      await enqueueJobs(
        env,
        races
          .filter(isPremiumRaceDataTarget)
          .map((race) => ({ raceKey: race.raceKey, type: "fetch-premium-race-data" })),
      );
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      await logFetch(
        env.REALTIME_DB,
        "discover-premium-races",
        "ok",
        null,
        JSON.stringify(premiumResult),
      );
      return;
    }
    if (job.type === "plan-realtime-fetches") {
      if (await isPlanRealtimeCircuitBreakerOpen(env)) {
        await logFetch(env.REALTIME_DB, job.type, "skipped", null, "circuit breaker open").catch(
          () => {},
        );
        return;
      }
      const count = await planRealtimeFetches(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, `${count} jobs queued`);
      if (job.selfSchedule) {
        await logFetch(
          env.REALTIME_DB,
          "plan-realtime-fetches-self",
          "ok",
          null,
          `${count} jobs queued`,
        );
      }
      return;
    }
    if (job.type === "discover-premium-races") {
      const result = await discoverPremiumRacesForDate(env, job.date);
      const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, job.date);
      await enqueueJobs(
        env,
        races
          .filter(isPremiumRaceDataTarget)
          .map((race) => ({ raceKey: race.raceKey, type: "fetch-premium-race-data" })),
      );
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    if (job.type === "discover-premium-race-links") {
      const result = await discoverPremiumRacesForDate(env, job.date);
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    if (job.type === "plan-premium-race-data-fetches") {
      const premiumResult = await discoverPremiumRacesForDate(env, job.date);
      const jobs = await planPremiumRaceDataFetchesForDate(env, job.date, getNow(env));
      await enqueueJobs(env, jobs);
      await markPremiumRaceDataQueued(
        env.REALTIME_DB,
        jobs.flatMap((queuedJob) =>
          queuedJob.type === "fetch-premium-race-data" ? [queuedJob.raceKey] : [],
        ),
        toJstIsoString(getNow(env)),
      );
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ premiumResult, queued: jobs.length }),
      );
      return;
    }
    if (job.type === "fetch-premium-race-data") {
      // Defensive: isPremiumRaceDataTarget already gates planning + discovery to
      // jra + non-Ban-ei nar (restored 2026-07-04, see isPremiumRaceDataTarget's
      // comment), but legacy queue entries enqueued before this gate or a future
      // manual enqueue could land a Ban-ei job here. Skip it before
      // fetchAndStorePremiumRaceData does an HTTP fetch / D1 write loop.
      if (!isPremiumRaceDataQueueRaceKey(job.raceKey)) {
        await logFetch(env.REALTIME_DB, job.type, "skip:non-jra", job.raceKey, null);
        return;
      }
      await fetchAndStorePremiumRaceData(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-premium-paddock") {
      // Defensive: planPremiumPaddockFetchesForDate already gates to source==="jra",
      // but a manual enqueue or future code path could land a non-JRA job here. Skip
      // it before fetchAndStorePremiumPaddock does an HTTP fetch / D1 write loop.
      if (!job.raceKey.startsWith("jra:")) {
        await logFetch(env.REALTIME_DB, job.type, "skip:non-jra", job.raceKey, null);
        return;
      }
      await fetchAndStorePremiumPaddock(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-results") {
      if (await skipStaleLiveRealtimeJob(env, job.type, job.raceKey)) {
        return;
      }
      // 2026-06-28: wrap the JRA Playwright path so a hung Browser binding
      // (10 concurrent / 10-minute paid-plan caps) does not let the Workers
      // runtime cancel the whole handler at ~30s, which previously left the
      // message to silently retry without a fetch_logs entry. The thrown
      // HandlerTimeoutError is caught below and logged + retried explicitly.
      await withHandlerTimeout({
        label: "fetch-results",
        ms: QUEUE_HANDLER_TIMEOUT_MS,
        task: fetchAndStoreResults(env, job.raceKey),
      });
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-jra-track-condition") {
      await fetchAndStoreJraTrackCondition(env, job);
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ date: job.date, keibajoCode: job.keibajoCode }),
      );
      return;
    }
    if (job.type === "plan-running-style-predictions") {
      const materialize = await materializeRunningStyleFeatureParquetsForDate(env, job.date);
      if (materialize.materializeError !== undefined) {
        throw new Error(materialize.materializeError);
      }
      const planSummary = await planRunningStylePredictionsForDate(
        env,
        job.date,
        getNow(env),
      ).catch((error: unknown) => ({
        error: formatError(error),
      }));
      const cacheRefresh = await refreshViewerRunningStyleCachesForDate(env, job.date).catch(
        (error: unknown) => ({
          error: formatError(error),
        }),
      );
      const parquetExport = await exportRunningStyleParquetsForDate(env, job.date).catch(
        (error: unknown) => ({ error: formatError(error) }),
      );
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ cacheRefresh, materialize, parquetExport, plan: planSummary }),
      );
      return;
    }
    if (job.type === "materialize-running-style-features") {
      const summary = await materializeRunningStyleFeatureParquetsForDate(env, job.date).catch(
        (error: unknown) => ({ error: formatError(error) }),
      );
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(summary));
      return;
    }
    if (job.type === "generate-running-style-predictions") {
      // The running-style handler owns bounded Catalog (120s) and PostgreSQL
      // (20s) deadlines. Do not wrap it in Promise.race: a queue-level timeout
      // cannot cancel a Service Binding request, so it would acknowledge a
      // retry while the original inference kept running and duplicated R2 SQL.
      const summary = await handleRunningStylePredictionJob(env, job);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, JSON.stringify(summary));
      return;
    }
    if (job.type === "discover-win5-schedules") {
      await logWin5CronResult(env, getNow(env));
      await logFetch(env.REALTIME_DB, job.type, "ok", null, job.date);
      return;
    }
    if (job.type === "generate-win5-predictions") {
      const summary = await handleWin5PredictionJob(env, job);
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        `${job.kaisaiNen}${job.kaisaiTsukihi}`,
        JSON.stringify(summary),
      );
      return;
    }
    if (job.type === "build-daily-features") {
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "disabled",
        null,
        "Catalog service owns realtime feature builds",
      );
      return;
    }
    await handleFetchWeightsOrUnknown(env, job);
  } catch (error) {
    if (job.type === "plan-realtime-fetches" && isD1OverloadError(error)) {
      await tripPlanRealtimeCircuitBreaker(env).catch(() => {});
    }
    if (job.type !== "generate-running-style-predictions") {
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "error",
        "raceKey" in job ? job.raceKey : null,
        formatError(error),
        env.DETAIL_SECTION_CACHE_KV,
      );
    }
    throw error;
  }
};

export const raceKeyFromRequest = (url: URL): string | null => {
  return raceKeyFromRealtimePath(url.pathname);
};

// Flat horse-weight endpoint: GET /api/horse-weight/{percent-encoded raceKey}
// Used by the finish-position-predict container to read bataiju for upcoming
// races (available in D1 ~30-40 min before post time via the weight watchdog).
// Returns the HorseWeightSnapshot JSON from HORSE_WEIGHT_DO, or D1 fallback
// rows when the DO has not been hydrated yet. Race key format mirrors
// /api/odds/{raceKey}:
//   {source}:{YYYY}:{MMDD}:{keibajo_code}:{race_bango}  (percent-encoded)
// e.g. /api/horse-weight/nar%3A2026%3A0610%3A44%3A01
export const horseWeightRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(/^\/api\/horse-weight\/(.+)$/u);
  if (!match?.[1]) return null;
  const decoded = decodeURIComponent(match[1]);
  // Validate race key shape: {jra|nar}:{YYYY}:{MMDD}:{KK}:{RR}
  if (!/^(jra|nar):\d{4}:\d{4}:[0-9A-Z]{2}:\d{2}$/u.test(decoded)) return null;
  return decoded;
};

const horseWeightsStreamPathRegex =
  /^\/api\/(jra|nar)\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/horse-weights-stream$/u;
const horseWeightsLatestPathRegex =
  /^\/api\/(jra|nar)\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/horse-weights-latest$/u;

const horseWeightsRaceKeyFromMatch = (match: RegExpMatchArray): string =>
  buildRealtimeRaceKey(
    match[1] as RealtimeSource,
    match[2]!,
    `${match[3]!}${match[4]!}`,
    match[5]!,
    match[6]!,
  );

export const horseWeightsStreamRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(horseWeightsStreamPathRegex);
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) return null;
  return horseWeightsRaceKeyFromMatch(match);
};

export const horseWeightsLatestRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(horseWeightsLatestPathRegex);
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) return null;
  return horseWeightsRaceKeyFromMatch(match);
};

type HorseWeightStub = {
  fetch: (input: string, init?: RequestInit) => Promise<Response>;
};

const horseWeightD1Snapshot = async (
  env: Env,
  raceKey: string,
): Promise<HorseWeightSnapshot | null> => {
  const latest = await getLatestHorseWeights(env.REALTIME_DB, raceKey);
  if (latest === null) return null;
  return toHorseWeightSnapshot(latest.fetchedAt, latest.horses);
};

const writeHorseWeightSnapshotToStubSafe = async (
  env: Env,
  raceKey: string,
  stub: HorseWeightStub,
  snapshot: HorseWeightSnapshot,
): Promise<void> => {
  try {
    await writeHorseWeightSnapshotToStub({ snapshot, stub });
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      "horse-weight-do-hydrate",
      "error",
      raceKey,
      formatError(error),
    );
  }
};

const hydrateHorseWeightStubFromD1 = async (
  env: Env,
  raceKey: string,
  stub: HorseWeightStub,
): Promise<HorseWeightSnapshot | null> => {
  const snapshot = await horseWeightD1Snapshot(env, raceKey);
  if (snapshot === null) return null;
  await writeHorseWeightSnapshotToStubSafe(env, raceKey, stub, snapshot);
  return snapshot;
};

const proxyHorseWeightLatestWithD1Fallback = async (
  env: Env,
  raceKey: string,
  stub: HorseWeightStub,
): Promise<Response> => {
  const response = await proxyHorseWeightLatestFromStub(stub);
  if (response.status !== 204) return response;
  const snapshot = await hydrateHorseWeightStubFromD1(env, raceKey, stub);
  return snapshot === null ? response : json(snapshot);
};

export const premiumRaceKeyFromRequest = (url: URL): string | null => {
  const match = url.pathname.match(
    /^\/api\/(jra|nar)\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/premium$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5] || !match[6]) {
    return null;
  }
  return buildRealtimeRaceKey(
    match[1] as RealtimeSource,
    match[2],
    `${match[3]}${match[4]}`,
    match[5],
    match[6],
  );
};

interface RaceTrendDailyTrackQueryParams {
  beforeRaceBango: string;
  keibajoCode: string;
  source: "jra" | "nar";
  targetYmd: string;
}

const isYyyymmdd = (value: string): boolean => /^\d{8}$/u.test(value);
const isRaceBango = (value: string): boolean => /^\d{1,2}$/u.test(value);
const isKeibajoCode = (value: string): boolean => /^[0-9A-Z]{2}$/u.test(value);
const isTrendSource = (value: string | null): value is "jra" | "nar" =>
  value === "jra" || value === "nar";

const NETKEIBA_TRAINING_WORKOUTS_PATH_TEMPLATE = "/race/oikiri.html?race_id={sourceRaceId}";
const NETKEIBA_SOURCE_RACE_ID_PATTERN = /^\d{12}$/u;
const NETKEIBA_INTERMEDIATE_WORKOUT_TYPE = "1";

const withNetkeibaIntermediateWorkoutType = (url: string): string => {
  const parsed = new URL(url);
  if (parsed.searchParams.get("type") === NETKEIBA_INTERMEDIATE_WORKOUT_TYPE) return url;
  parsed.searchParams.set("type", NETKEIBA_INTERMEDIATE_WORKOUT_TYPE);
  return parsed.toString();
};

interface NetkeibaTrainingWorkoutsRequestBody {
  raceDate: string;
  sourceRaceId: string;
}

const parseNetkeibaTrainingWorkoutsRequestBody = (
  value: unknown,
): NetkeibaTrainingWorkoutsRequestBody | null => {
  if (!isObjectRecord(value)) return null;
  if (typeof value.raceDate !== "string" || !isYyyymmdd(value.raceDate)) return null;
  if (
    typeof value.sourceRaceId !== "string" ||
    !NETKEIBA_SOURCE_RACE_ID_PATTERN.test(value.sourceRaceId)
  ) {
    return null;
  }
  return { raceDate: value.raceDate, sourceRaceId: value.sourceRaceId };
};

const fetchNetkeibaTrainingWorkouts = async (
  env: Env,
  body: NetkeibaTrainingWorkoutsRequestBody,
): Promise<Response> => {
  const config = getPremiumRaceConfig(env);
  if (!config.origin) {
    return json({ error: "premium_fetch_not_configured" }, { status: 503 });
  }
  try {
    const workPathTemplate = config.workPathTemplate ?? NETKEIBA_TRAINING_WORKOUTS_PATH_TEMPLATE;
    const workUrl = new URL(
      workPathTemplate.replaceAll("{sourceRaceId}", body.sourceRaceId),
      config.origin,
    ).toString();
    const html = await fetchPremiumHtml(config, workUrl);
    const finalWorkouts = parseNetkeibaTrainingWorkouts(html, body.raceDate);
    const intermediateUrl = withNetkeibaIntermediateWorkoutType(workUrl);
    if (intermediateUrl === workUrl) {
      return json({ workouts: finalWorkouts });
    }
    try {
      const intermediateHtml = await fetchPremiumHtml(config, intermediateUrl);
      return json({
        workouts: mergeNetkeibaTrainingWorkouts([
          finalWorkouts,
          parseNetkeibaTrainingWorkouts(intermediateHtml, body.raceDate),
        ]),
      });
    } catch {
      return json({ workouts: finalWorkouts });
    }
  } catch {
    return json({ error: "premium_fetch_failed" }, { status: 502 });
  }
};

export const raceTrendDailyTrackQueryFromRequest = (
  url: URL,
): RaceTrendDailyTrackQueryParams | null => {
  if (url.pathname !== "/internal/race-trend-daily-track") return null;
  const source = url.searchParams.get("source");
  const targetYmd = url.searchParams.get("ymd");
  const keibajoCode = url.searchParams.get("keibajo");
  const beforeRaceBango = url.searchParams.get("beforeRaceBango");
  if (!isTrendSource(source)) return null;
  if (!targetYmd || !isYyyymmdd(targetYmd)) return null;
  if (!keibajoCode || !isKeibajoCode(keibajoCode)) return null;
  if (!beforeRaceBango || !isRaceBango(beforeRaceBango)) return null;
  return { beforeRaceBango, keibajoCode, source, targetYmd };
};

export const sameDayVenueJockeyWinsFromRequest = (
  url: URL,
): {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
} | null => {
  const match = url.pathname.match(
    /^\/api\/nar\/races\/(\d{4})\/(\d{2})\/(\d{2})\/([0-9A-Z]{2})\/(\d{2})\/jockey-wins$/u,
  );
  if (!match?.[1] || !match[2] || !match[3] || !match[4] || !match[5]) {
    return null;
  }
  return {
    day: match[3],
    keibajoCode: match[4],
    month: match[2],
    raceNumber: match[5],
    year: match[1],
  };
};

export default {
  async fetch(request, env, _ctx): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === "OPTIONS") {
      return new Response(null, {
        headers: {
          "access-control-allow-headers": "content-type",
          "access-control-allow-methods": "GET, OPTIONS, POST",
          "access-control-allow-origin": "*",
        },
      });
    }

    if (url.pathname === "/health") {
      return json({ ok: true });
    }

    if (url.pathname === "/api/internal/netkeiba-training-workouts" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = parseNetkeibaTrainingWorkoutsRequestBody(await request.json().catch(() => null));
      if (!body) {
        return json({ error: "invalid body" }, { status: 400 });
      }
      return fetchNetkeibaTrainingWorkouts(env, body);
    }

    if (url.pathname === "/api/internal/discovery-status" && request.method === "GET") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const date = url.searchParams.get("date");
      if (date === null || !/^\d{8}$/u.test(date)) {
        return json({ error: "invalid date" }, { status: 400 });
      }
      const [jraRaces, d1JraRaceCount] = await Promise.all([
        fetchJraRacesByDate(env, date),
        countJraRaceSourcesByDate(env.REALTIME_DB, date),
      ]);
      const complete = d1JraRaceCount >= jraRaces.length;
      return json(
        {
          complete,
          d1JraRaceCount,
          date,
          neonJraRaceCount: jraRaces.length,
        },
        {
          headers: complete
            ? { "cache-control": "private, no-store" }
            : { "cache-control": "private, no-store", "retry-after": "10" },
        },
      );
    }

    if (url.pathname === "/api/internal/queue-health" && request.method === "GET") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const now = getNow(env);
      const todayYmd = getTodayJst(now);
      const thirtyMinutesAgoIso = toJstIsoString(
        new Date(now.getTime() - QUEUE_HEALTH_STUCK_THRESHOLD_MINUTES * 60_000),
      );
      const metrics = await getQueueHealthMetrics(env.REALTIME_DB, {
        thirtyMinutesAgoIso,
        todayYmd,
        yesterdayYmd: addDaysToYyyymmdd(todayYmd, -1),
      });
      return json(metrics);
    }

    if (url.pathname === "/api/internal/neon-write-pool-health" && request.method === "GET") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      return buildNeonWritePoolHealthResponse(await probeNeonWritePool(env));
    }

    if (url.pathname === "/api/internal/export-odds-chunk" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as {
        since_id: number;
        batch_size: number;
        after_fetched_at?: string;
      };
      const rows = await listOddsSnapshotsForExport(env.REALTIME_DB, {
        afterFetchedAt: body.after_fetched_at,
        batchSize: body.batch_size,
        sinceId: body.since_id,
      });
      const nextSinceId = rows.length > 0 ? rows.at(-1)!.id : body.since_id;
      return json({
        done: rows.length < body.batch_size,
        next_since_id: nextSinceId,
        rows,
      });
    }

    if (
      url.pathname === "/api/internal/list-race-keys-by-date-from-hyperdrive" &&
      request.method === "POST"
    ) {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as { kaisaiNen: string; kaisaiTsukihi: string };
      const rows = await listRaceKeysByDateFromHyperdrive(env.REALTIME_DB, {
        kaisaiNen: body.kaisaiNen,
        kaisaiTsukihi: body.kaisaiTsukihi,
      });
      return json({ rows });
    }

    if (url.pathname === "/api/internal/export-race-sources-chunk" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as { since_id: number; batch_size: number };
      const rows = await listRaceSourcesForSeed(env.REALTIME_DB, {
        batchSize: body.batch_size,
        sinceId: body.since_id,
      });
      const nextSinceId = rows.length > 0 ? rows.at(-1)!.rowid : body.since_id;
      return json({
        done: rows.length < body.batch_size,
        next_since_id: nextSinceId,
        rows,
      });
    }

    if (url.pathname === "/api/internal/delete-odds-chunk" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as {
        since_id: number;
        batch_size: number;
        upper_bound_id: number;
      };
      const result = await deleteOddsSnapshotsChunk(env.REALTIME_DB, {
        batchSize: body.batch_size,
        sinceId: body.since_id,
        upperBoundId: body.upper_bound_id,
      });
      return json(result);
    }

    if (
      url.pathname === "/api/internal/delete-daily-race-entries-chunk" &&
      request.method === "POST"
    ) {
      const expectedToken = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN;
      if (!expectedToken || request.headers.get("x-pc-keiba-internal-token") !== expectedToken) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as { chunk_size: number; since_rowid: number };
      const result = await deleteDailyRaceEntriesChunk(env.REALTIME_DB, {
        chunkSize: body.chunk_size,
        sinceRowid: body.since_rowid,
      });
      return json(result);
    }

    if (
      url.pathname === "/api/internal/delete-race-running-styles-chunk" &&
      request.method === "POST"
    ) {
      const expectedToken = env.PC_KEIBA_VIEWER_INTERNAL_TOKEN;
      if (!expectedToken || request.headers.get("x-pc-keiba-internal-token") !== expectedToken) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json()) as { chunk_size: number; since_rowid: number };
      const result = await deleteRaceRunningStylesChunk(env.REALTIME_DB, {
        chunkSize: body.chunk_size,
        sinceRowid: body.since_rowid,
      });
      return json(result);
    }

    if (url.pathname === "/api/jobs" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const job = (await request.json()) as Job;
      await enqueueJobs(env, [job]);
      return json({ ok: true });
    }

    if (url.pathname === "/api/jobs/run-inline" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const job = (await request.json()) as Job;
      await handleJob(env, job);
      return json({ ok: true });
    }

    if (url.pathname === "/api/jobs/fetch-weights" && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const body = (await request.json().catch(() => null)) as {
        date?: string;
        force?: boolean;
        source?: string;
      } | null;
      const validBody = parseFetchWeightsBatchBody(body);
      if (!validBody) {
        return json({ error: "invalid body" }, { status: 400 });
      }
      const enqueued = await enqueueFetchWeightsBatch(env, validBody);
      return json({ enqueued, ok: true });
    }

    const runningStylePostgresVerificationParams = parseRunningStylePostgresVerificationParams(url);
    if (runningStylePostgresVerificationParams && request.method === "POST") {
      const expectedToken = env.REALTIME_ADMIN_TOKEN;
      if (!expectedToken || request.headers.get("authorization") !== `Bearer ${expectedToken}`) {
        return json({ error: "forbidden" }, { status: 403 });
      }
      const summary = await runRunningStyleWorkerPostgresVerification(
        env,
        runningStylePostgresVerificationParams,
      );
      return json({ ok: true, ...summary });
    }

    const premiumRaceKey = premiumRaceKeyFromRequest(url);
    if (premiumRaceKey && request.method === "GET") {
      const [payload, cachedPaddock] = await Promise.all([
        getPremiumRacePayload(env.REALTIME_DB, premiumRaceKey),
        readCachedPremiumPaddock(env, premiumRaceKey),
      ]);
      return json(
        payload.paddockBulletins.length > 0 && cachedPaddock && typeof cachedPaddock === "object"
          ? { ...payload, ...cachedPaddock }
          : payload,
        {
          headers: {
            "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
          },
        },
      );
    }

    const raceKey = raceKeyFromRequest(url);
    if (raceKey && request.method === "GET") {
      const payload = await buildRealtimeRouteResponse(env, raceKey);
      if (payload.odds && payload.odds.horseTrends.length === 0) {
        payload.odds.horseTrends = toHorseTrends(payload.odds.history);
      }
      if (payload.odds?.historyByType && !payload.odds.trendsByType) {
        payload.odds.trendsByType = toOddsTrendsByType(payload.odds.historyByType);
      }
      return json(payload, {
        headers: {
          "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
        },
      });
    }

    const sameDayVenueJockeyWins = sameDayVenueJockeyWinsFromRequest(url);
    if (sameDayVenueJockeyWins && request.method === "GET") {
      return json(
        {
          jockeyWins: await getSameDayVenueJockeyWins(env.REALTIME_DB, {
            beforeRaceBango: sameDayVenueJockeyWins.raceNumber,
            kaisaiNen: sameDayVenueJockeyWins.year,
            kaisaiTsukihi: `${sameDayVenueJockeyWins.month}${sameDayVenueJockeyWins.day}`,
            keibajoCode: sameDayVenueJockeyWins.keibajoCode,
          }),
        },
        {
          headers: {
            "cache-control": `public, max-age=${Number(env.REALTIME_API_CACHE_SECONDS ?? "20")}`,
          },
        },
      );
    }

    const horseWeightsStreamRaceKey = horseWeightsStreamRaceKeyFromRequest(url);
    if (horseWeightsStreamRaceKey && request.method === "GET") {
      const streamStub = env.HORSE_WEIGHT_DO.get(
        env.HORSE_WEIGHT_DO.idFromName(horseWeightsStreamRaceKey),
      );
      await hydrateHorseWeightStubFromD1(env, horseWeightsStreamRaceKey, streamStub);
      return proxyHorseWeightStreamFromStub(streamStub);
    }

    const horseWeightsLatestRaceKey = horseWeightsLatestRaceKeyFromRequest(url);
    if (horseWeightsLatestRaceKey && request.method === "GET") {
      const latestStub = env.HORSE_WEIGHT_DO.get(
        env.HORSE_WEIGHT_DO.idFromName(horseWeightsLatestRaceKey),
      );
      return proxyHorseWeightLatestWithD1Fallback(env, horseWeightsLatestRaceKey, latestStub);
    }

    const horseWeightFlatRaceKey = horseWeightRaceKeyFromRequest(url);
    if (horseWeightFlatRaceKey && request.method === "GET") {
      const flatStub = env.HORSE_WEIGHT_DO.get(
        env.HORSE_WEIGHT_DO.idFromName(horseWeightFlatRaceKey),
      );
      return proxyHorseWeightLatestWithD1Fallback(env, horseWeightFlatRaceKey, flatStub);
    }

    const raceTrendQuery = raceTrendDailyTrackQueryFromRequest(url);
    if (raceTrendQuery && request.method === "GET") {
      const idName = buildRaceTrendDailyTrackDoIdName({
        keibajoCode: raceTrendQuery.keibajoCode,
        source: raceTrendQuery.source,
        targetYmd: raceTrendQuery.targetYmd,
      });
      const stub = env.RACE_TREND_DAILY_TRACK_DO.get(
        env.RACE_TREND_DAILY_TRACK_DO.idFromName(idName),
      );
      return fetchRaceTrendDailyTrackRacesFromStub({
        beforeRaceBango: raceTrendQuery.beforeRaceBango,
        context: {
          keibajoCode: raceTrendQuery.keibajoCode,
          source: raceTrendQuery.source,
          targetYmd: raceTrendQuery.targetYmd,
        },
        stub,
      });
    }

    return json({ error: "not found" }, { status: 404 });
  },

  async scheduled(controller, env, ctx): Promise<void> {
    const scheduledAt =
      typeof controller.scheduledTime === "number"
        ? new Date(controller.scheduledTime)
        : new Date();
    if (controller.cron === RESULT_POLL_CRON) {
      const targetDate = getTodayJst(scheduledAt);
      ctx.waitUntil(runResultAndPaddockPlans(env, targetDate));
      return;
    }
    if (controller.cron === RUNNING_STYLE_INFERENCE_CRON) {
      ctx.waitUntil(logRunningStylePlanResult(env, scheduledAt, ctx));
      return;
    }
    if (controller.cron === RUNNING_STYLE_PREWARM_CRON) {
      const targetDate = formatTomorrowYYYYMMDDInJst(scheduledAt);
      ctx.waitUntil(
        prewarmRunningStylePredictionsForDate(env, targetDate, scheduledAt, ctx)
          .catch((error: unknown) =>
            logFetch(
              env.REALTIME_DB,
              "plan-running-style-predictions",
              "error",
              null,
              formatError(error),
              env.DETAIL_SECTION_CACHE_KV,
            ),
          )
          .then(() => undefined),
      );
      return;
    }
    if (controller.cron === WIN5_DISCOVER_CRON) {
      ctx.waitUntil(logWin5CronResult(env, scheduledAt));
      return;
    }
    if (controller.cron === DAILY_FEATURE_BUILD_CRON) {
      ctx.waitUntil(
        logFetch(
          env.REALTIME_DB,
          "build-daily-features",
          "disabled",
          null,
          "Catalog service owns realtime feature builds",
        ),
      );
      return;
    }
    if (controller.cron === D1_RETENTION_CRON) {
      ctx.waitUntil(
        runD1Retention(env.REALTIME_DB, scheduledAt)
          .then((result) =>
            logFetch(env.REALTIME_DB, "d1-retention", "ok", null, JSON.stringify(result)),
          )
          .catch((error: unknown) =>
            logFetch(
              env.REALTIME_DB,
              "d1-retention",
              "error",
              null,
              formatError(error),
              env.DETAIL_SECTION_CACHE_KV,
            ),
          ),
      );
      return;
    }
    if (controller.cron === MULTI_DAY_PREP_CRON) {
      const today = getTodayJst(scheduledAt);
      const dates = MULTI_DAY_PREP_OFFSET_DAYS.map((offset) => addDaysToYyyymmdd(today, offset));
      ctx.waitUntil(prewarmRaceDataForDates(env, dates, scheduledAt, ctx, "multi-day-prep"));
      return;
    }
    if (controller.cron === TODAY_BACKFILL_CRON) {
      const today = getTodayJst(scheduledAt);
      ctx.waitUntil(prewarmRaceDataForDate(env, today, scheduledAt, ctx, "today-backfill"));
      return;
    }
    if (controller.cron === WEIGHT_WATCHDOG_CRON) {
      ctx.waitUntil(runWeightWatchdog(env, scheduledAt));
      return;
    }
    const job = getCronJob(controller.cron, scheduledAt);
    if (job.type === "plan-realtime-fetches") {
      ctx.waitUntil(runScheduledRealtimePlanWithRecovery(env, job.date));
      return;
    }
    ctx.waitUntil(handleJob(env, job));
  },

  async queue(batch, env): Promise<void> {
    for (const message of batch.messages) {
      const runningStyleRaceKey =
        message.body.type === "generate-running-style-predictions" ? message.body.raceKey : null;
      if (runningStyleRaceKey !== null) {
        await logFetch(
          env.REALTIME_DB,
          message.body.type,
          "started",
          runningStyleRaceKey,
          `${RUNNING_STYLE_QUEUE_LOG_MARKER} attempts=${message.attempts}`,
          env.DETAIL_SECTION_CACHE_KV,
        ).catch((logError: unknown) =>
          console.warn(
            formatErrorLogLine(
              "Running-style queue start log failed",
              {
                attempts: String(message.attempts),
                raceKey: runningStyleRaceKey,
              },
              logError,
            ),
          ),
        );
      }
      try {
        await handleJob(env, message.body);
        message.ack();
      } catch (error) {
        if (runningStyleRaceKey !== null) {
          await logFetch(
            env.REALTIME_DB,
            message.body.type,
            "error",
            runningStyleRaceKey,
            `${RUNNING_STYLE_QUEUE_LOG_MARKER} attempts=${message.attempts} error=${formatError(error)}`,
            env.DETAIL_SECTION_CACHE_KV,
          ).catch((logError: unknown) =>
            console.warn(
              formatErrorLogLine(
                "Running-style queue error log failed",
                {
                  attempts: String(message.attempts),
                  raceKey: runningStyleRaceKey,
                },
                logError,
              ),
            ),
          );
        }
        console.error(
          formatErrorLogLine(
            "Queue job failed",
            runningStyleRaceKey === null
              ? { type: message.body.type }
              : {
                  attempts: String(message.attempts),
                  raceKey: runningStyleRaceKey,
                  type: message.body.type,
                },
            error,
          ),
        );
        if (message.body.type === "plan-realtime-fetches" && isConnectionPressureError(error)) {
          const realtimePlanJob = message.body;
          if (!realtimePlanJob.selfSchedule) {
            await enqueueSelfRealtimePlanIfStale(env, realtimePlanJob.date, {
              delaySeconds: buildConnectionPressureRecoveryDelaySeconds(),
              failureClass: "connection_pressure",
              now: getNow(env),
              stage: "queue.enqueue-single-recovery",
            }).catch((recoveryError: unknown) =>
              console.error(
                formatErrorLogLine(
                  "Queue realtime planner single recovery enqueue failed",
                  { date: realtimePlanJob.date, stage: "queue.enqueue-single-recovery" },
                  recoveryError,
                ),
              ),
            );
          } else {
            await logFetch(
              env.REALTIME_DB,
              "plan-realtime-fetches-recovery",
              "exhausted",
              null,
              JSON.stringify({
                failureClass: "connection_pressure",
                stage: "queue.single-recovery-exhausted",
              }),
              env.DETAIL_SECTION_CACHE_KV,
            ).catch((logError: unknown) =>
              console.error(
                formatErrorLogLine(
                  "Queue realtime planner recovery exhaustion log failed",
                  { date: realtimePlanJob.date, stage: "queue.single-recovery-exhausted" },
                  logError,
                ),
              ),
            );
          }
          message.ack();
          continue;
        }
        const delaySeconds = isD1OverloadError(error)
          ? buildPlanRealtimeOverloadRetryDelaySeconds()
          : QUEUE_RETRY_DELAY_SECONDS;
        try {
          message.retry({ delaySeconds });
        } catch (retryError) {
          console.error(
            formatErrorLogLine(
              "Queue delayed retry failed",
              runningStyleRaceKey === null
                ? { type: message.body.type }
                : {
                    attempts: String(message.attempts),
                    raceKey: runningStyleRaceKey,
                    type: message.body.type,
                  },
              retryError,
            ),
          );
          message.retry();
        }
      }
    }
  },
} satisfies ExportedHandler<Env, Job>;
