import {
  BABA_CODE_TO_LOCAL_KEIBAJO,
  buildRaceListUrl,
  buildRaceResultUrl,
  buildRaceKey,
  extractOddsLinks,
  fetchRaceLinksFromRaceList,
  fetchRacePage,
  fetchTodayRaceListUrls,
  parseRaceMetadata,
  parseRaceEntries,
  parseHorseWeights,
  parseRaceEntryHorseNumbers,
  parseRaceResultExcludedHorseNumbers,
  parseRaceResults,
  parseRaceResultHorseWeights,
  type KeibaGoRaceLink,
} from "./keiba-go";
import { formatError } from "./format-error";
import { mergeJsonHeaders } from "./http";
import {
  buildJraEntryUrlFromRace,
  buildJraResultUrlFromRaceSource,
  fetchJraResultHtmlWithPlaywright,
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
  buildPremiumUrl,
  buildPremiumRaceLinkFromRace,
  discoverPremiumRaceLinks,
  fetchPremiumHtml,
  fetchPremiumHtmlAttempts,
  getPremiumRaceConfig,
  hasPremiumRaceFetchConfig,
  isPremiumRaceDataTarget,
  isPremiumStableCommentHtmlAuthorized,
  matchPremiumLinkToRace,
  parsePremiumDataTopHorses,
  parsePremiumPaddockBulletins,
  parsePremiumStableComments,
  parsePremiumTrainingReviews,
  summarizePremiumStableCommentHtml,
  type PremiumPaddockBulletin,
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
  claimResultFetch,
  claimTrackConditionFetch,
  completeResultFetch,
  completeTrackConditionFetch,
  countJraRaceSourcesMissingRaceDateFieldsByDate,
  countRaceSourcesByDate,
  failTrackConditionFetch,
  failResultFetch,
  getPremiumRaceLink,
  getPremiumRacePayload,
  getPremiumPaddockFetchState,
  getPremiumPaddockNotificationState,
  getRaceSource,
  deleteDailyRaceEntriesChunk,
  deleteOddsSnapshotsChunk,
  deleteRaceRunningStylesChunk,
  getLatestTrackConditionForRace,
  getSameDayVenueJockeyWins,
  insertRaceEntrySnapshot,
  insertRaceResultSnapshot,
  insertHorseWeightSnapshot,
  insertJraTrackConditionSnapshot,
  listJraVenueTrackConditionSchedulesByDate,
  listOddsSnapshotsForExport,
  listPremiumRaceDataFetchCandidatesByDate,
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
  replacePremiumRaceData,
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
  type SchedulableRaceSource,
} from "./storage";
import {
  RUNNING_STYLE_INFERENCE_CRON,
  RUNNING_STYLE_PREWARM_CRON,
  formatTomorrowYYYYMMDDInJst,
  planRunningStylePredictionsForDate,
  refreshViewerRunningStyleCachesForDate,
  runRunningStyleCronTick,
} from "./running-style-cron";
import { materializeRunningStyleFeatureParquetsForDate } from "./running-style-feature-materialize";
import { handleRunningStylePredictionJob } from "./running-style-queue";
import {
  DAILY_FEATURE_BUILD_CRON,
  probeDailyRaceEntriesFreshness,
  runDailyFeatureBuildForEnv,
} from "./daily-feature-build";
import { WIN5_DISCOVER_CRON, logWin5CronResult } from "./win5-cron";
import { handleWin5PredictionJob } from "./win5-queue";
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
  RaceEntry,
  RaceResult,
  RealtimeRacePayload,
} from "./types";

const QUEUE_SEND_BATCH_SIZE = 100;
// True at most once per hour so the discover-urls fallback only fires off
// the first result-poller tick of each JST hour. Without this guard the
// cron would re-discover every 2 minutes, which is wasteful.
const HOURLY_RECOVERY_MINUTE_THRESHOLD = 2;
const RESULT_FETCH_LOCK_MINUTES = 10;
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
// 2026-05-31: lowered from 3 to 2 in tandem with the result-poll cron drop
// from "*/5" to "*/2". With the previous 5-minute cron + 3-minute throttle
// 11R results landed in D1 up to ~5 minutes after JRA published them, and
// the 12R detail view's race-trend panel showed only 1R-10R for that whole
// window. Each result-poll tick is one cheap SELECT against
// realtime_race_sources so D1 still has plenty of CPU headroom.
const RESULT_FETCH_INTERVAL_MINUTES = 2;
// JST 09-22 (= UTC 00-13) is the race-day result-poller cron. Distinct from
// the hourly "0 0-13 * * *" plan-realtime-fetches cron so we only run the
// result poller without re-triggering the heavier hourly work. Tightened to
// every 2 minutes (was every 5) so a freshly-finished race appears in the
// merged race-trend payload within one or two ticks instead of up to five.
export const RESULT_POLL_CRON = "*/2 0-13 * * *";
const TRACK_CONDITION_FETCH_LOCK_MINUTES = 15;
const QUEUE_RETRY_DELAY_SECONDS = 60;
const PREMIUM_RACE_DATA_RETRY_DELAY_SECONDS = 20 * 60;
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
const MILLISECONDS_PER_MINUTE = 60_000;
// KV TTL for the weight-race-list fallback (used when Hyperdrive returns an
// empty result so the plan still has something to enqueue). 24h keeps the
// fallback alive across the entire race day.
const WEIGHT_RACE_LIST_KV_TTL_SECONDS = 24 * 60 * 60;
const WEIGHT_RACE_LIST_KV_PREFIX = "realtime:weight-race-list:";
// Weight watchdog: a dedicated every-minute cron path that bypasses the
// heavier plan-realtime-fetches code path so a circuit-breaker open state
// (D1 saturation) does not silently skip weight enqueueing for upcoming
// races. The watchdog inspects realtime_race_sources directly and enqueues
// fetch-weights jobs for races within the lookahead window whose last
// weight fetch is null or older than the stale threshold.
export const WEIGHT_WATCHDOG_CRON = "* * * * *";
const WEIGHT_WATCHDOG_LOOKAHEAD_MINUTES = 180;
const WEIGHT_WATCHDOG_LOOKBACK_MINUTES = 30;
const WEIGHT_WATCHDOG_STALE_THRESHOLD_MINUTES = 5;
const WEIGHT_WATCHDOG_MAX_PER_TICK = 8;
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

const HOT_WORKER_ORIGIN = "https://sync-realtime-data-hot.kkk4oru.com";
const FEATURES_WORKER_ORIGIN = "https://sync-realtime-data-features.kkk4oru.com";
const FORWARD_RESPONSE_BODY_MAX_LENGTH = 200;

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

interface WeightCandidatePair {
  race: SchedulableRaceSource;
  minutes: number | null;
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

interface ForwardRaceForFeaturesArgs {
  source: "jra" | "nar";
  raceKey: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
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

// Fire-and-forget POST to the new features worker so the new R2 Parquet build
// + new D1 inference pipeline can pick up the race the moment we discover it.
// fail-soft: any error is logged but the upstream race upsert is never blocked.
export const forwardRaceForFeatures = async (
  env: Env,
  args: ForwardRaceForFeaturesArgs,
): Promise<void> => {
  if (!env.REALTIME_FEATURES || !env.PC_KEIBA_VIEWER_INTERNAL_TOKEN) {
    return;
  }
  try {
    const response = await env.REALTIME_FEATURES.fetch(
      `${FEATURES_WORKER_ORIGIN}/api/internal/recompute-and-build-parquet`,
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
      formatError(error),
    ).catch(() => undefined);
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
    await logFetch(env.REALTIME_DB, "realtime-route", "error", raceKey, formatError(error)).catch(
      () => undefined,
    );
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
// 20:05 JST (= 11:05 UTC) — nightly prep for next 1-3 days (features + running-style).
export const MULTI_DAY_PREP_CRON = "5 11 * * *";
// 09:10 JST (= 00:10 UTC) — morning fallback for today (features + running-style).
export const TODAY_BACKFILL_CRON = "10 0 * * *";
const MULTI_DAY_PREP_OFFSET_DAYS: readonly number[] = [1, 2, 3];
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

const upsertDiscoveredUrls = async (
  env: Env,
  targetDate: string,
): Promise<{
  fallbackRaceListCount: number;
  jraRaceCount: number;
  localRaceCount: number;
  topRaceListCount: number;
  upserted: number;
}> => {
  const [raceListUrls, localRaces, jraRaces] = await Promise.all([
    fetchTodayRaceListUrls(targetDate),
    fetchNarRacesByDate(env, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
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

  let upserted = 0;
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
    upserted += 1;
  }
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
      await upsertNarRaceSource(env.REALTIME_DB, link, race, oddsLinks);
      const raceBango = race.race_bango.padStart(2, "0");
      const narRaceKey = buildRealtimeRaceKey(
        "nar",
        race.kaisai_nen,
        race.kaisai_tsukihi,
        keibajoCode,
        raceBango,
      );
      await forwardRaceSourceToHot(env, {
        debaUrl: link.url,
        kaisaiNen: race.kaisai_nen,
        kaisaiTsukihi: race.kaisai_tsukihi,
        keibajoCode,
        oddsLinksJson: JSON.stringify(oddsLinks),
        raceBango,
        raceKey: narRaceKey,
        raceStartAtJst: buildRaceStartAtJst(
          race.kaisai_nen,
          race.kaisai_tsukihi,
          race.hasso_jikoku,
        ),
        source: "nar",
      });
      await forwardRaceForFeatures(env, {
        kaisaiNen: race.kaisai_nen,
        kaisaiTsukihi: race.kaisai_tsukihi,
        keibajoCode,
        raceBango,
        raceKey: narRaceKey,
        source: "nar",
      });
      upserted += 1;
    }
  }
  return {
    fallbackRaceListCount: fallbackRaceListUrls.length,
    jraRaceCount: jraRaces.length,
    localRaceCount: localRaces.length,
    topRaceListCount: raceListUrls.length,
    upserted,
  };
};

const ensureJraRaceSourcesAreCurrent = async (env: Env, targetDate: string): Promise<void> => {
  const [d1RaceCount, missingRaceDateFieldCount, jraRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    countJraRaceSourcesMissingRaceDateFieldsByDate(env.REALTIME_DB, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
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
  raceKey: string;
  raceStartAtJst: string;
}

interface StaleWeightFetchRaceRow {
  last_weight_fetch_at: string | null;
  race_key: string;
  race_start_at_jst: string;
}

// Direct D1 query for races whose post time falls inside the watchdog
// lookahead window and whose last weight fetch is null or older than the
// stale threshold. Keeps the watchdog independent of the heavier
// plan-realtime-fetches code path so a circuit-breaker open state does not
// silently skip weight enqueueing.
export const findStaleWeightFetchRaces = async (
  db: D1Database,
  now: Date,
): Promise<readonly StaleWeightFetchRace[]> => {
  const lookAheadIso = new Date(
    now.getTime() + WEIGHT_WATCHDOG_LOOKAHEAD_MINUTES * MILLISECONDS_PER_MINUTE,
  ).toISOString();
  const lookBackIso = new Date(
    now.getTime() - WEIGHT_WATCHDOG_LOOKBACK_MINUTES * MILLISECONDS_PER_MINUTE,
  ).toISOString();
  const staleIso = new Date(
    now.getTime() - WEIGHT_WATCHDOG_STALE_THRESHOLD_MINUTES * MILLISECONDS_PER_MINUTE,
  ).toISOString();
  const result = await db
    .prepare(
      `
        select race_key, race_start_at_jst, last_weight_fetch_at
        from realtime_race_sources
        where race_start_at_jst > ?
          and race_start_at_jst < ?
          and (last_weight_fetch_at is null or last_weight_fetch_at < ?)
        order by race_start_at_jst
        limit ?
      `,
    )
    .bind(lookBackIso, lookAheadIso, staleIso, WEIGHT_WATCHDOG_MAX_PER_TICK)
    .all<StaleWeightFetchRaceRow>();
  return result.results.map((row) => ({
    lastWeightFetchAt: row.last_weight_fetch_at,
    raceKey: row.race_key,
    raceStartAtJst: row.race_start_at_jst,
  }));
};

// Dedicated weight watchdog tick. Runs every minute alongside the existing
// "*/15 0-14" weight plan cron and the every-minute plan-realtime-fetches
// path on the hot worker. The watchdog only touches a single D1 read and
// the queue, so a Hyperdrive saturation that opens the planner circuit
// breaker still leaves weight fetches flowing here.
export const runWeightWatchdog = async (env: Env, now: Date): Promise<void> => {
  try {
    const stale = await findStaleWeightFetchRaces(env.REALTIME_DB, now);
    if (stale.length === 0) {
      await logFetch(env.REALTIME_DB, "weight-watchdog", "ok", null, "no stale weight races");
      return;
    }
    const jobs: Job[] = stale.map((race) => ({
      raceKey: race.raceKey,
      type: "fetch-weights",
    }));
    await enqueueJobs(env, jobs);
    await logFetch(
      env.REALTIME_DB,
      "weight-watchdog",
      "ok",
      null,
      JSON.stringify({ enqueued: jobs.length }),
    );
  } catch (error: unknown) {
    await logFetch(env.REALTIME_DB, "weight-watchdog", "error", null, formatError(error));
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

const isWeightCandidate = (pair: WeightCandidatePair): pair is WeightCandidate =>
  pair.minutes !== null;

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
  const jst = toJstIsoString(date);
  return jst.slice(11, 16) === "20:00";
};

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

const getLatestQueuedSelfRealtimePlanAt = async (env: Env): Promise<string | null> => {
  const row = await env.REALTIME_DB.prepare(
    `
      select created_at
      from fetch_logs
      where job_type = 'plan-realtime-fetches-self'
        and status = 'queued'
      order by created_at desc
      limit 1
    `,
  ).first<{ created_at: string }>();
  return row?.created_at ?? null;
};

const enqueueSelfRealtimePlanIfStale = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  const latest = await getLatestSuccessfulRealtimePlanAt(env);
  if (
    latest &&
    new Date(latest).getTime() > now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS * 1000
  ) {
    return;
  }
  const latestQueued = await getLatestQueuedSelfRealtimePlanAt(env);
  if (
    latestQueued &&
    new Date(latestQueued).getTime() >
      now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS * 1000
  ) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { date, selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS },
  );
  await logFetch(env.REALTIME_DB, "plan-realtime-fetches-self", "queued", null, date);
};

const enqueueNextSelfRealtimePlan = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  await env.REALTIME_JOBS.send(
    { date, selfSchedule: true, type: "plan-realtime-fetches" },
    { delaySeconds: REALTIME_PLAN_SELF_SCHEDULE_DELAY_SECONDS },
  );
};

const runRealtimePlannerWatchdogIfStale = async (env: Env, date: string, now = getNow(env)) => {
  if (!isJstPollingWindow(now)) {
    return;
  }
  const latest = await getLatestSuccessfulRealtimePlanAt(env);
  if (
    latest &&
    new Date(latest).getTime() > now.getTime() - REALTIME_PLAN_SELF_SCHEDULE_STALE_SECONDS * 1000
  ) {
    return;
  }
  await handleJob(env, { date, selfSchedule: true, type: "plan-realtime-fetches" });
};

const seedRealtimePlannerWatchdog = (env: Env, ctx: ExecutionContext): void => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return;
  }
  ctx.waitUntil(runRealtimePlannerWatchdogIfStale(env, getTodayJst(now), now));
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
  const [d1RaceCount, localRaces, jraRaces] = await Promise.all([
    countRaceSourcesByDate(env.REALTIME_DB, targetDate),
    fetchNarRacesByDate(env, targetDate),
    fetchJraRacesByDate(env, targetDate),
  ]);
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
  await upsertDiscoveredUrls(env, targetDate);
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
    await logFetch(env.REALTIME_DB, "discover-urls", "error", null, formatError(error));
  }
};

const tryBuildDailyFeaturesForDate = async (env: Env, targetDate: string, mode: string) => {
  try {
    const result = await runDailyFeatureBuildForEnv(env, {
      fromDate: targetDate,
      toDate: targetDate,
    });
    await logFetch(
      env.REALTIME_DB,
      "build-daily-features",
      "ok",
      null,
      JSON.stringify({ ...result, mode }),
    );
    return result;
  } catch (error) {
    await logFetch(env.REALTIME_DB, "build-daily-features", "error", null, formatError(error));
    return null;
  }
};

const tryDiscoverUrlsForDate = async (env: Env, targetDate: string, mode: string) => {
  try {
    const result = await upsertDiscoveredUrls(env, targetDate);
    await logFetch(
      env.REALTIME_DB,
      "discover-urls",
      "ok",
      null,
      JSON.stringify({ ...result, mode }),
    );
    return result;
  } catch (error) {
    await logFetch(env.REALTIME_DB, "discover-urls", "error", null, formatError(error));
    return null;
  }
};

// Skip materialize when build-daily-features did not (yet) populate D1.
// 2026-06-04 incident: keibajo 30 (門別) races never materialized because
// prewarm fired ahead of the PG → D1 replication for that venue. Returning a
// zero-row summary lets the running-style-cron */10 tick pick the work up on
// the next pass without poisoning the materialize log with a per-race error.
interface MaterializeSkipResult {
  date: string;
  materialized: number;
  materializeError: string;
  scanned: number;
  skipped: number;
}

const buildSkippedMaterializeResult = (
  targetDate: string,
  rowCount: number,
): MaterializeSkipResult => ({
  date: targetDate,
  materialized: 0,
  materializeError: `build-daily-features produced ${rowCount} D1 rows for ${targetDate}; deferring materialize to next cron tick`,
  scanned: 0,
  skipped: 0,
});

const runMaterializeWhenReady = async (env: Env, targetDate: string) => {
  const probe = await probeDailyRaceEntriesFreshness(env.REALTIME_DB, targetDate, targetDate);
  if (probe.rowCount > 0) {
    return materializeRunningStyleFeatureParquetsForDate(env, targetDate);
  }
  return buildSkippedMaterializeResult(targetDate, probe.rowCount);
};

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
  const featureResult = await tryBuildDailyFeaturesForDate(
    env,
    targetDate,
    "running-style-prewarm",
  );
  const materializeResult = await runMaterializeWhenReady(env, targetDate);
  await logFetch(
    env.REALTIME_DB,
    "materialize-running-style-features",
    resolveMaterializeLogStatus(materializeResult),
    null,
    JSON.stringify({ ...materializeResult, mode: "prewarm" }),
  );
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
    features: featureResult,
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
  const featureResult = await tryBuildDailyFeaturesForDate(env, targetDate, mode);
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
    features: featureResult,
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

export const notifyPremiumPaddockIfNeeded = async (
  env: Env,
  race: NarRaceSource,
  bulletins: readonly PremiumPaddockBulletin[],
  fetchedAt: string,
): Promise<void> => {
  const payloadSignature = await buildPremiumPaddockSignature(bulletins);
  if (new Date(race.raceStartAtJst).getTime() <= getNow(env).getTime()) {
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
  const currentNotification = await getPremiumPaddockNotificationState(
    env.REALTIME_DB,
    race.raceKey,
  );
  if (currentNotification?.lastNotifiedAt) {
    if (currentNotification.lastNotifiedAt !== fetchedAt) {
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

export const assertJraHorseWeightsComplete = (
  raceKey: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): void => {
  if (weights.length === 0) {
    return;
  }
  const expectedHorseNumbers = new Set(
    entries
      .filter((entry) => !entry.status || !isJraScratchStatus(entry.status))
      .map((entry) => entry.horseNumber),
  );
  const actualHorseNumbers = new Set(weights.map((weight) => weight.horseNumber));
  const missingHorseNumbers = Array.from(expectedHorseNumbers).filter(
    (horseNumber) => !actualHorseNumbers.has(horseNumber),
  );
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
export const assertNarHorseWeightsComplete = (
  raceKey: string,
  entries: Omit<RaceEntry, "fetchedAt">[],
  weights: HorseWeight[],
): void => {
  if (weights.length === 0) {
    return;
  }
  const expectedHorseNumbers = new Set(
    entries.filter((entry) => !entry.status).map((entry) => entry.horseNumber),
  );
  const actualHorseNumbers = new Set(weights.map((weight) => weight.horseNumber));
  const missingHorseNumbers = Array.from(expectedHorseNumbers).filter(
    (horseNumber) => !actualHorseNumbers.has(horseNumber),
  );
  if (missingHorseNumbers.length > 0) {
    throw new Error(
      `NAR horse weight rows are sparse: ${raceKey} missing=${missingHorseNumbers.join(",")}`,
    );
  }
};

const shouldRunHourlyDiscoveryRecovery = (now: Date): boolean => {
  const { minute } = getJstDateParts(now);
  return Number(minute) < HOURLY_RECOVERY_MINUTE_THRESHOLD;
};

const buildResultFetchJobIfDue = (
  race: SchedulableRaceSource,
  now: Date,
): Extract<Job, { type: "fetch-results" }> | null => {
  const minutes = minutesUntilRace(race, now);
  if (minutes === null) {
    return null;
  }
  const resultLockUntil = race.resultFetchLockUntil
    ? new Date(race.resultFetchLockUntil).getTime()
    : Number.NaN;
  const isResultFetchEligible =
    minutes <= 0 &&
    (race.source === "nar" || race.source === "jra") &&
    !race.resultCompleteAt &&
    isDue(race.lastResultFetchAt, RESULT_FETCH_INTERVAL_MINUTES, now) &&
    (Number.isNaN(resultLockUntil) || resultLockUntil <= now.getTime()) &&
    !race.lastResultQueuedAt;
  return isResultFetchEligible ? { raceKey: race.raceKey, type: "fetch-results" } : null;
};

// Result-poller-only planner. Used by the "*/2 0-13 * * *" cron so the
// race-result `fetch-results` jobs fire every 2 minutes without re-running
// the heavier work that the hourly "0 0-13 * * *" cron already performs
// (track-condition, premium paddock, weights, discovery refresh). 2026-05-31:
// also re-runs discovery once per hour off this lightweight cron so a missed
// hourly discover-urls tick does not leave today's races invisible to the
// result poller (= the "11R confirmed but viewer never sees 1R-11R" failure
// mode the new DO + cache-bust path is meant to fix upstream).
export const planResultFetchesOnly = async (env: Env, targetDate: string): Promise<number> => {
  const now = getNow(env);
  if (!isJstPollingWindow(now)) {
    return 0;
  }
  if (shouldRunHourlyDiscoveryRecovery(now)) {
    await tryEnsureDiscoveredUrlsAreCurrent(env, targetDate);
  }
  const races = await listSchedulableRaceSourcesByDate(env.REALTIME_DB, targetDate);
  const jobs: Extract<Job, { type: "fetch-results" }>[] = races
    .map((race) => buildResultFetchJobIfDue(race, now))
    .filter((job): job is Extract<Job, { type: "fetch-results" }> => job !== null);
  await enqueueJobs(env, jobs);
  await markResultFetchQueued(
    env.REALTIME_DB,
    jobs.map((job) => job.raceKey),
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
    const weightCandidates: WeightCandidate[] = races
      .map((race): WeightCandidatePair => ({ race, minutes: minutesUntilRace(race, now) }))
      .filter(isWeightCandidate)
      .filter(
        (c) =>
          c.minutes <= WEIGHT_FETCH_LEAD_MINUTES &&
          isDue(
            c.race.lastWeightFetchAt,
            resolveWeightFetchCooldownMinutes({
              lastFetchAt: c.race.lastWeightFetchAt,
              now,
              raceStartAtJst: c.race.raceStartAtJst,
            }),
            now,
          ),
      );
    [...weightCandidates].sort(compareWeightCandidates).forEach((c) => {
      jobs.push({ raceKey: c.race.raceKey, type: "fetch-weights" });
    });
    if (races.length > 0) {
      await writeWeightRaceListFallbackToKv(
        env,
        targetDate,
        races.map(
          (race): WeightRaceListKvEntry => ({ raceKey: race.raceKey, source: race.source }),
        ),
      );
    } else {
      const fallback = await readWeightRaceListFallbackFromKv(env, targetDate);
      fallback.forEach((entry) => {
        jobs.push({ raceKey: entry.raceKey, type: "fetch-weights" });
      });
    }
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

const fetchAndStoreWeights = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    throw new Error(`race source not found: ${raceKey}`);
  }
  const fetchedAt = toJstIsoString();
  const html = await fetchRacePage(race.debaUrl);
  const latestOdds = race.source === "jra" ? await fetchHotOddsPayload(env, raceKey) : null;
  const entries =
    race.source === "jra"
      ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(html), latestOdds?.latest)
      : parseRaceEntries(html);
  await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
  let weights = race.source === "jra" ? parseJraHorseWeights(html) : parseHorseWeights(html);
  if (race.source === "nar" && weights.length === 0) {
    const resultHtml = await fetchRacePage(buildRaceResultUrl(race.debaUrl));
    weights = parseRaceResultHorseWeights(resultHtml);
  }
  if (race.source === "jra") {
    assertJraHorseWeightsComplete(raceKey, entries, weights);
  }
  if (race.source === "nar") {
    assertNarHorseWeightsComplete(raceKey, entries, weights);
  }
  if (weights.length > 0 && weights.length < 2) {
    await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, []);
    throw new Error(`horse weight rows are unexpectedly sparse: ${weights.length}`);
  }
  await insertHorseWeightSnapshot(env.REALTIME_DB, raceKey, fetchedAt, weights);
  if (weights.length > 0) {
    await updateLastFetch(env.REALTIME_DB, raceKey, "last_weight_fetch_at", fetchedAt);
    await broadcastHorseWeightsToDO(env, raceKey, fetchedAt, weights);
  }
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

interface BuildRaceTrendRowArgs {
  entries: Omit<RaceEntry, "fetchedAt">[];
  fetchedAt: string;
  isComplete: boolean;
  race: NarRaceSource;
  results: Omit<RaceResult, "fetchedAt">[];
}

const formatHassoJikokuFromRaceStart = (raceStartAtJst: string): string | null => {
  if (raceStartAtJst.length < 16) return null;
  return `${raceStartAtJst.slice(11, 13)}${raceStartAtJst.slice(14, 16)}`;
};

const buildRaceTrendDailyTrackRow = ({
  entries,
  fetchedAt,
  isComplete,
  race,
  results,
}: BuildRaceTrendRowArgs): RaceTrendDailyTrackRow => {
  const entryByHorseNumber = new Map(entries.map((entry) => [entry.horseNumber, entry]));
  const starterRows = results.map((result) => {
    const entry = entryByHorseNumber.get(result.horseNumber);
    return {
      bamei: entry?.horseName ?? result.horseName,
      bataiju: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      finishPosition: Number.parseInt(result.finishPosition.replace(/\s+/gu, ""), 10) || 0,
      hassoJikoku: formatHassoJikokuFromRaceStart(race.raceStartAtJst),
      jockeyName: entry?.jockeyName ?? null,
      kaisaiNen: race.kaisaiNen,
      kaisaiTsukihi: race.kaisaiTsukihi,
      keibajoCode: race.keibajoCode,
      raceBango: race.raceBango,
      raceName: race.raceName,
      runnerCount: null,
      sohaTime: result.time,
      source: race.source,
      tanshoOdds: null,
      tanshoPopularity: null,
      umaban: result.horseNumber,
      wakuban: null,
      zogenFugo: null,
      zogenSa: null,
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

// Observe `requestTrendCacheBust` outcome by status:
//   - "ok"       : silent (the happy path needs no log entry).
//   - "error"    : 5xx / network failure / retry exhausted — record at error
//                  level so the standard fetch_logs telemetry catches it.
//   - "skipped"  : viewer internal token not configured (or other
//                  environmental skip). Lower severity than error but still
//                  worth surfacing because a long "skipped" streak silently
//                  disables the bust signal across the entire card.
const runTrendCacheBust = async (env: Env, raceKey: string, race: NarRaceSource): Promise<void> => {
  const outcome = await requestTrendCacheBust(env, buildTrendBustFromRaceContext(race));
  if (outcome.status === "error") {
    await logFetch(env.REALTIME_DB, "trend-cache-bust", "error", raceKey, outcome.message);
    return;
  }
  if (outcome.status === "skipped") {
    await logFetch(env.REALTIME_DB, "trend-cache-bust", "skipped", raceKey, outcome.message);
  }
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
// fetchAndStoreResults. JRA always returns "complete" because the JRA result
// HTML publishes the full field atomically (no progressive publish) — same
// behavior as before. NAR partial results route through a progressive retry
// (retry-short / retry-medium / retry-long) up to RESULT_FETCH_GIVE_UP_HOURS,
// after which "give-up" force-completes with whatever rows have been saved.
// This eliminates the previous 60-min backstop force-complete window that
// permanently dropped finishers the upstream eventually published.
export const resolveResultFetchOutcome = (
  input: ResolveResultFetchOutcomeInput,
): ResultFetchOutcome => {
  if (input.expectedHorseCount <= 0) {
    return "complete";
  }
  if (input.inserted >= input.expectedHorseCount) {
    return "complete";
  }
  if (input.source === "jra") {
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
}

// 2026-06-05: Routes the resolveResultFetchOutcome decision to the right
// storage write + side effects (DO push + viewer trend cache bust). Split
// out of fetchAndStoreResults so the per-outcome branching stays at one
// level of indentation and the helper itself is unit-testable in isolation.
const dispatchResultFetchOutcome = async (
  input: DispatchResultFetchOutcomeInput,
): Promise<void> => {
  const isRetry =
    input.outcome === "retry-short" ||
    input.outcome === "retry-medium" ||
    input.outcome === "retry-long";
  if (isRetry) {
    await handleRetryResultFetch(input);
    return;
  }
  await handleCompleteResultFetch(input);
};

const handleRetryResultFetch = async (input: DispatchResultFetchOutcomeInput): Promise<void> => {
  const retryLockMinutes = resolveRetryLockMinutes(input.outcome);
  const retryLockUntil = toJstIsoString(new Date(input.now.getTime() + retryLockMinutes * 60_000));
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
    }),
    input.race,
  );
  if (input.inserted > 0) {
    await runTrendCacheBust(input.env, input.raceKey, input.race);
  }
  await logFetch(
    input.env.REALTIME_DB,
    "fetch-results",
    "partial",
    input.raceKey,
    `inserted=${input.inserted} expected=${input.expectedHorseCount} retry-lock-minutes=${retryLockMinutes}`,
  );
};

const handleCompleteResultFetch = async (input: DispatchResultFetchOutcomeInput): Promise<void> => {
  // baseComplete + give-up both finalize the race; both set isComplete=true so
  // the planner stops re-enqueuing. JRA partial (saved<expected) is the one
  // case where we complete with isComplete=false — preserves the pre-2026-06-05
  // JRA behavior because JRA result HTML publishes the full field atomically
  // so a saved<expected on JRA means the parser missed a row (different
  // failure mode from the NAR progressive-publish gap).
  const isComplete =
    input.outcome === "give-up" ||
    (input.expectedHorseCount > 0 && input.inserted >= input.expectedHorseCount) ||
    input.expectedHorseCount === 0;
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
    }),
    input.race,
  );
  // Always bust the viewer trend cache when ANY result row landed (not just
  // when the full field is parsed). JRA / NAR sometimes publish results in
  // multiple stages — for example a long objection delay can hold the last
  // 1-2 horses while the leading horses are already on the result page. If
  // we only bust on `isComplete` the merged race-trend payload keeps the
  // pre-race "no result yet" cache for that race until natural TTL expiry,
  // which is exactly the "1R-11R confirmed but 12R detail still shows them
  // as unfinished" failure mode this commit targets.
  if (input.inserted > 0) {
    await runTrendCacheBust(input.env, input.raceKey, input.race);
  }
};

const fetchAndStoreResults = async (env: Env, raceKey: string): Promise<void> => {
  const now = getNow(env);
  const lockUntil = toJstIsoString(new Date(now.getTime() + RESULT_FETCH_LOCK_MINUTES * 60_000));
  const claimed = await claimResultFetch(env.REALTIME_DB, raceKey, lockUntil, toJstIsoString(now));
  if (!claimed) {
    return;
  }
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race) {
    await failResultFetch(env.REALTIME_DB, raceKey);
    throw new Error(`race source not found: ${raceKey}`);
  }
  if (!isRaceFinished(race, now)) {
    await failResultFetch(env.REALTIME_DB, raceKey);
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
    const [entryHtml, resultHtml] = await Promise.all([
      race.source === "jra"
        ? fetchJraResultHtmlWithPlaywright(env.JRA_BROWSER, race.debaUrl)
        : fetchRacePage(race.debaUrl),
      race.source === "jra"
        ? fetchJraResultHtmlWithPlaywright(env.JRA_BROWSER, resultUrl)
        : fetchRacePage(resultUrl),
    ]);
    const entries =
      race.source === "jra"
        ? sanitizeJraRaceEntriesWithOdds(parseJraRaceEntries(entryHtml), null)
        : parseRaceEntries(entryHtml);
    await insertRaceEntrySnapshot(env.REALTIME_DB, raceKey, fetchedAt, entries);
    const entryHorseNumbers =
      race.source === "jra"
        ? entries.map((entry) => entry.horseNumber)
        : parseRaceEntryHorseNumbers(entryHtml);
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
    const expectedHorseCount = entryHorseNumbers.filter(
      (horseNumber) => !excludedHorseNumbers.has(horseNumber),
    ).length;
    const results =
      race.source === "jra" ? parseJraRaceResults(resultHtml) : parseRaceResults(resultHtml);
    if (expectedHorseCount > 0 && results.length === 0) {
      throw new Error(`race result rows are empty: ${raceKey}`);
    }
    const inserted = await insertRaceResultSnapshot(env.REALTIME_DB, raceKey, fetchedAt, results);
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
      entries,
      env,
      expectedHorseCount,
      fetchedAt,
      inserted,
      now,
      outcome,
      race,
      raceKey,
      results,
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

const fetchAndStorePremiumRaceData = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || !isPremiumRaceDataTarget(race)) {
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
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
  // Suppress the stable-comment replace when the proxy returned the preview
  // (unauthenticated) page: otherwise the unauth response (typically 3 rows)
  // would overwrite a previously stored authenticated snapshot (full field).
  // The fetch state below still records `commentAuthRequired: true` so the
  // planner re-queues the race.
  const stableComments = commentHtml && !commentAuthorized ? undefined : parsedStableComments;
  await replacePremiumRaceData(env.REALTIME_DB, {
    dataTopHorses,
    fetchedAt,
    link,
    raceKey,
    stableComments,
    trainingReviews,
  });
  if (dataTopHorses && dataTopHorses.length > 0) {
    await putPremiumDataTopCache({
      env,
      race,
      rows: dataTopHorses.map((row) => ({ ...row, fetchedAt })),
    });
  }
  const hasAnyData =
    (trainingReviews?.length ?? 0) > 0 ||
    (parsedStableComments?.length ?? 0) > 0 ||
    (dataTopHorses?.length ?? 0) > 0;
  const commentAuthRequired = Boolean(commentHtml) && !commentAuthorized;
  await updatePremiumRaceDataFetchState(env.REALTIME_DB, {
    fetchedAt,
    message: JSON.stringify({
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
      dataTopHasIconAccount: dataTopHtml ? dataTopHtml.includes("Icon_Account") : null,
      dataTopHasDummyBox: dataTopHtml ? dataTopHtml.includes("DummyBox") : null,
      dataTopHasPremiumRegist: dataTopHtml ? dataTopHtml.includes("Premium_Regist_Box") : null,
      dataTopHasIconLogin: dataTopHtml ? dataTopHtml.includes("Icon_Login") : null,
      dataTopHasLogout: dataTopHtml ? dataTopHtml.includes("ログアウト") : null,
      dataTopDlBlockCount: dataTopHtml ? (dataTopHtml.match(/<dl\b/giu)?.length ?? 0) : null,
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
    status: commentAuthRequired ? "auth_required" : hasAnyData ? "ok" : "empty",
  });
};

const fetchAndStorePremiumPaddock = async (env: Env, raceKey: string): Promise<void> => {
  const race = await getRaceSource(env.REALTIME_DB, raceKey);
  if (!race || race.source !== "jra") {
    return;
  }
  const currentState = await getPremiumPaddockFetchState(env.REALTIME_DB, raceKey);
  if (
    currentState?.retryAfter &&
    new Date(currentState.retryAfter).getTime() > getNow(env).getTime()
  ) {
    return;
  }
  const config = getPremiumRaceConfig(env);
  if (!hasPremiumRaceFetchConfig(config)) {
    return;
  }
  const link = await ensurePremiumRaceLink(env, race);
  if (!link) {
    throw new Error(`premium race link not found: ${raceKey}`);
  }
  const paddockUrl = buildPremiumUrl(config, config.paddockPathTemplate, {
    sourceRaceId: link.sourceRaceId,
  });
  if (!paddockUrl) {
    return;
  }
  let attempts: Awaited<ReturnType<typeof fetchPremiumHtmlAttempts>>;
  try {
    attempts = await fetchPremiumHtmlAttempts(config, paddockUrl);
  } catch (error: unknown) {
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

export const handleJob = async (env: Env, job: Job): Promise<void> => {
  try {
    if (job.type === "discover-urls") {
      const [result, premiumResult] = await Promise.all([
        upsertDiscoveredUrls(env, job.date),
        discoverPremiumRacesForDate(env, job.date),
      ]);
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
        await enqueueNextSelfRealtimePlan(env, job.date);
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
      await fetchAndStorePremiumRaceData(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-premium-paddock") {
      await fetchAndStorePremiumPaddock(env, job.raceKey);
      await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
      return;
    }
    if (job.type === "fetch-results") {
      await fetchAndStoreResults(env, job.raceKey);
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
      await logFetch(
        env.REALTIME_DB,
        job.type,
        "ok",
        null,
        JSON.stringify({ cacheRefresh, plan: planSummary }),
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
      const result = await runDailyFeatureBuildForEnv(env, {
        forceRefresh: job.forceRefresh ?? false,
        fromDate: job.date,
        sourceScope: job.sourceScope ?? "all",
        toDate: job.date,
      });
      await logFetch(env.REALTIME_DB, job.type, "ok", null, JSON.stringify(result));
      return;
    }
    await fetchAndStoreWeights(env, job.raceKey);
    await logFetch(env.REALTIME_DB, job.type, "ok", job.raceKey, null);
  } catch (error) {
    await logFetch(
      env.REALTIME_DB,
      job.type,
      "error",
      "raceKey" in job ? job.raceKey : null,
      formatError(error),
    );
    throw error;
  }
};

export const raceKeyFromRequest = (url: URL): string | null => {
  return raceKeyFromRealtimePath(url.pathname);
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
  async fetch(request, env, ctx): Promise<Response> {
    const url = new URL(request.url);
    if (request.method !== "OPTIONS") {
      seedRealtimePlannerWatchdog(env, ctx);
    }
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
      return proxyHorseWeightStreamFromStub(streamStub);
    }

    const horseWeightsLatestRaceKey = horseWeightsLatestRaceKeyFromRequest(url);
    if (horseWeightsLatestRaceKey && request.method === "GET") {
      const latestStub = env.HORSE_WEIGHT_DO.get(
        env.HORSE_WEIGHT_DO.idFromName(horseWeightsLatestRaceKey),
      );
      return proxyHorseWeightLatestFromStub(latestStub);
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
      ctx.waitUntil(
        planResultFetchesOnly(env, targetDate)
          .then((count) =>
            logFetch(env.REALTIME_DB, "plan-result-fetches", "ok", null, `${count} jobs queued`),
          )
          .catch((error: unknown) =>
            logFetch(env.REALTIME_DB, "plan-result-fetches", "error", null, formatError(error)),
          ),
      );
      ctx.waitUntil(
        planPremiumPaddockFetchesOnly(env, targetDate)
          .then((count) =>
            logFetch(env.REALTIME_DB, "plan-premium-paddock", "ok", null, `${count} jobs queued`),
          )
          .catch((error: unknown) =>
            logFetch(env.REALTIME_DB, "plan-premium-paddock", "error", null, formatError(error)),
          ),
      );
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
      // Enqueue rather than run inline. The queue consumer caps concurrency
      // and the new freshness guard short-circuits when daily_race_entries
      // is already populated within the last hour, so back-to-back hourly
      // ticks no longer pile direct D1 writes from the cron handler.
      const targetDate = getTodayJst(scheduledAt);
      ctx.waitUntil(
        enqueueJobs(env, [{ date: targetDate, sourceScope: "all", type: "build-daily-features" }])
          .then(() =>
            logFetch(
              env.REALTIME_DB,
              "build-daily-features",
              "queued",
              null,
              JSON.stringify({ date: targetDate, sourceScope: "all" }),
            ),
          )
          .catch((error: unknown) =>
            logFetch(env.REALTIME_DB, "build-daily-features", "error", null, formatError(error)),
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
            logFetch(env.REALTIME_DB, "d1-retention", "error", null, formatError(error)),
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
    ctx.waitUntil(handleJob(env, job));
    if (job.type === "plan-realtime-fetches") {
      ctx.waitUntil(enqueueSelfRealtimePlanIfStale(env, job.date));
    }
  },

  async queue(batch, env): Promise<void> {
    for (const message of batch.messages) {
      try {
        await handleJob(env, message.body);
        message.ack();
      } catch {
        message.retry({ delaySeconds: QUEUE_RETRY_DELAY_SECONDS });
      }
    }
  },
} satisfies ExportedHandler<Env, Job>;
