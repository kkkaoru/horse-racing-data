import { drainResponseBody } from "../lib/bounded-response-drain";
import {
  markRaceCacheWarmGeneration,
  readRaceCacheWarmGeneration,
} from "../lib/race-cache-warm-generation";
import {
  DETAIL_SECTION_CACHE_WARM_PARAM,
  buildDetailSectionApiPath,
  type DetailSectionCacheWarmMessage,
  type RaceDetailSsrCacheWarmMessage,
} from "../lib/race-detail-section-cache";
import { buildRaceTrendApiPath, type RaceTrendCacheWarmMessage } from "../lib/race-trend-cache";
import { formatJstDate, formatTodayJstDate } from "./jst-date";

const INTERNAL_ORIGIN = "https://pc-keiba-viewer.local";
const SCHEDULE_PATH = "/api/cache-warm/race-detail-sections";
const RACE_TREND_SCHEDULE_PATH = "/api/cache-warm/race-trends";
const RACE_DETAIL_SSR_SCHEDULE_PATH = "/api/cache-warm/race-detail-ssr";
const WIN_RATE_HEATMAP_SCHEDULE_PATH = "/api/cache-warm/win-rate-heatmaps";
const WARM_IN_BATCH_CONCURRENCY = 2;
// A hung self request held a consumer slot for up to 15 minutes (overall-score
// canceled at 930s on 2026-09-23). Abort it and let the queue retry instead.
const QUEUE_WARM_REQUEST_TIMEOUT_MS = 120_000;

type CacheWarmMessage =
  | DetailSectionCacheWarmMessage
  | RaceDetailSsrCacheWarmMessage
  | RaceTrendCacheWarmMessage;

interface CacheWarmBatchEntry {
  date: string;
  kind: string;
  ms: number;
  result: "ack" | "retry";
}

interface TimedWarmParams {
  ctx: PcKeibaExecutionContext;
  env: CloudflareEnv;
  openNextWorker: OpenNextWorker;
}

interface QueueWarmItem {
  ack(): void;
  body: CacheWarmMessage;
  retry(): void;
  // Set by the Queues runtime (when the message was sent).
  timestamp?: Date;
}

type OpenNextWorker = {
  fetch(request: Request, env: CloudflareEnv, ctx: PcKeibaExecutionContext): Promise<Response>;
};

export interface ScheduleTodayRaceDetailSectionCacheParams {
  ctx: PcKeibaExecutionContext;
  env: CloudflareEnv;
  openNextWorker: OpenNextWorker;
  todayJstYmd: string;
}

const fetchSelf = (
  openNextWorker: OpenNextWorker,
  request: Request,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<Response> =>
  env.WORKER_SELF_REFERENCE?.fetch(request) ?? openNextWorker.fetch(request, env, ctx);

export const scheduleTomorrowRaceDetailSectionCache = async (
  openNextWorker: OpenNextWorker,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const response = await fetchSelf(
    openNextWorker,
    new Request(`${INTERNAL_ORIGIN}${SCHEDULE_PATH}`, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "scheduled",
      },
      method: "POST",
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race detail cache schedule failed: ${response.status}`);
  }
};

export const scheduleTodayRaceDetailSectionCache = async (
  params: ScheduleTodayRaceDetailSectionCacheParams,
): Promise<void> => {
  const url = new URL(SCHEDULE_PATH, INTERNAL_ORIGIN);
  url.searchParams.set("date", params.todayJstYmd);
  const response = await fetchSelf(
    params.openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "scheduled",
      },
      method: "POST",
    }),
    params.env,
    params.ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race detail today cache schedule failed: ${response.status}`);
  }
};

export const scheduleDueRaceTrendCache = async (
  openNextWorker: OpenNextWorker,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const response = await fetchSelf(
    openNextWorker,
    new Request(`${INTERNAL_ORIGIN}${RACE_TREND_SCHEDULE_PATH}`, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "scheduled",
      },
      method: "POST",
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race trend cache schedule failed: ${response.status}`);
  }
};

export const scheduleRaceDetailSsrCacheWarm = async (
  openNextWorker: OpenNextWorker,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
  options: { date?: string } = {},
): Promise<void> => {
  const url = new URL(RACE_DETAIL_SSR_SCHEDULE_PATH, INTERNAL_ORIGIN);
  if (options.date) {
    url.searchParams.set("date", options.date);
  }
  const response = await fetchSelf(
    openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "scheduled",
      },
      method: "POST",
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race detail SSR cache warm failed: ${response.status}`);
  }
};

export const scheduleTodayWinRateHeatmapWarm = async (
  params: ScheduleTodayRaceDetailSectionCacheParams,
): Promise<void> => {
  const url = new URL(WIN_RATE_HEATMAP_SCHEDULE_PATH, INTERNAL_ORIGIN);
  url.searchParams.set("date", params.todayJstYmd);
  const response = await fetchSelf(
    params.openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "scheduled",
      },
      method: "POST",
    }),
    params.env,
    params.ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`win rate heatmap warm schedule failed: ${response.status}`);
  }
};

const warmDetailSection = async (
  openNextWorker: OpenNextWorker,
  message: DetailSectionCacheWarmMessage,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const url = new URL(buildDetailSectionApiPath(message), INTERNAL_ORIGIN);
  url.searchParams.set(DETAIL_SECTION_CACHE_WARM_PARAM, "1");
  const response = await fetchSelf(
    openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "queue",
      },
      signal: AbortSignal.timeout(QUEUE_WARM_REQUEST_TIMEOUT_MS),
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race detail cache warm failed: ${response.status} ${url.pathname}`);
  }
};

const warmRaceTrend = async (
  openNextWorker: OpenNextWorker,
  message: RaceTrendCacheWarmMessage,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const url = new URL(buildRaceTrendApiPath(message), INTERNAL_ORIGIN);
  const response = await fetchSelf(
    openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "queue",
      },
      signal: AbortSignal.timeout(QUEUE_WARM_REQUEST_TIMEOUT_MS),
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race trend cache warm failed: ${response.status} ${url.pathname}`);
  }
};

const warmRaceDetailSsr = async (
  openNextWorker: OpenNextWorker,
  message: RaceDetailSsrCacheWarmMessage,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const url = new URL(RACE_DETAIL_SSR_SCHEDULE_PATH, INTERNAL_ORIGIN);
  url.searchParams.set("date", `${message.year}-${message.month}-${message.day}`);
  url.searchParams.set("keibajo", message.keibajoCode);
  url.searchParams.set("race", message.raceNumber);
  const response = await fetchSelf(
    openNextWorker,
    new Request(url, {
      headers: { "X-PC-Keiba-Cache-Warm": "queue" },
      method: "POST",
      signal: AbortSignal.timeout(QUEUE_WARM_REQUEST_TIMEOUT_MS),
    }),
    env,
    ctx,
  ).then(drainResponseBody);
  if (!response.ok) {
    throw new Error(`race detail SSR cache warm failed: ${response.status} ${url.pathname}`);
  }
};

const isRaceTrendCacheWarmMessage = (
  message: CacheWarmMessage,
): message is RaceTrendCacheWarmMessage => "kind" in message && message.kind === "race-trend";

const isRaceDetailSsrCacheWarmMessage = (
  message: CacheWarmMessage,
): message is RaceDetailSsrCacheWarmMessage =>
  "kind" in message && message.kind === "race-detail-ssr";

const isHeatmapWarmMessage = (message: CacheWarmMessage): boolean =>
  !isRaceTrendCacheWarmMessage(message) &&
  !isRaceDetailSsrCacheWarmMessage(message) &&
  message.section === "win-rate-heatmap";

const isPastRaceMessage = (message: CacheWarmMessage, todayJstYmd: string): boolean =>
  `${message.year}-${message.month}-${message.day}` < todayJstYmd;

// Only stale past-race work is dropped: a past race enqueued today (e.g. a
// result correction from trend-cache-bust) is still warmed.
const isStalePastRaceItem = (item: QueueWarmItem, todayJstYmd: string): boolean =>
  isPastRaceMessage(item.body, todayJstYmd) &&
  (item.timestamp === undefined || formatJstDate(item.timestamp) < todayJstYmd);

const ackMessage = (message: QueueWarmItem): void => {
  message.ack();
};

const warmQueueMessage = async (
  openNextWorker: OpenNextWorker,
  message: QueueWarmItem,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  try {
    if (isRaceDetailSsrCacheWarmMessage(message.body)) {
      const race = {
        keibajoCode: message.body.keibajoCode,
        mmdd: `${message.body.month}${message.body.day}`,
        raceBango: message.body.raceNumber,
        source: message.body.source,
        year: message.body.year,
      };
      const state = await readRaceCacheWarmGeneration({
        kind: "race-detail-ssr",
        kv: env.DETAIL_SECTION_CACHE_KV,
        race,
      });
      if (state?.valid) {
        message.ack();
        return;
      }
      await warmRaceDetailSsr(openNextWorker, message.body, env, ctx);
      const marked = await markRaceCacheWarmGeneration({
        generation: state?.generation ?? "0",
        kind: "race-detail-ssr",
        kv: env.DETAIL_SECTION_CACHE_KV,
        race,
      });
      if (state !== null && !marked) {
        throw new Error("race detail SSR cache generation changed during warm");
      }
      message.ack();
      return;
    }
    if (isRaceTrendCacheWarmMessage(message.body)) {
      const race = {
        keibajoCode: message.body.keibajoCode,
        mmdd: `${message.body.month}${message.body.day}`,
        raceBango: message.body.raceNumber,
        source: message.body.source,
        year: message.body.year,
      };
      const state = await readRaceCacheWarmGeneration({
        kind: "race-trend",
        kv: env.DETAIL_SECTION_CACHE_KV,
        race,
      });
      if (state?.valid || (state !== null && state.generation !== message.body.cacheGeneration)) {
        message.ack();
        return;
      }
      await warmRaceTrend(openNextWorker, message.body, env, ctx);
      const marked = await markRaceCacheWarmGeneration({
        generation: message.body.cacheGeneration,
        kind: "race-trend",
        kv: env.DETAIL_SECTION_CACHE_KV,
        race,
      });
      if (state !== null && !marked) {
        throw new Error("race trend cache generation changed during warm");
      }
      message.ack();
      return;
    }
    await warmDetailSection(openNextWorker, message.body, env, ctx);
    message.ack();
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.error("[pc-keiba-viewer] race detail cache warm failed", detail);
    message.retry();
  }
};

const warmKindOf = (message: CacheWarmMessage): string =>
  isRaceTrendCacheWarmMessage(message) || isRaceDetailSsrCacheWarmMessage(message)
    ? message.kind
    : message.section;

// Runs one warm and reports which of ack/retry it chose and how long it took,
// so batch logs show where consumer time goes.
const timedWarm = async (
  params: TimedWarmParams,
  message: QueueWarmItem,
): Promise<CacheWarmBatchEntry> => {
  const startedAt = Date.now();
  const outcome: { result: "ack" | "retry" } = { result: "ack" };
  await warmQueueMessage(
    params.openNextWorker,
    {
      ack: () => message.ack(),
      body: message.body,
      retry: () => {
        outcome.result = "retry";
        message.retry();
      },
    },
    params.env,
    params.ctx,
  );
  return {
    date: `${message.body.year}${message.body.month}${message.body.day}`,
    kind: warmKindOf(message.body),
    ms: Date.now() - startedAt,
    result: outcome.result,
  };
};

const mapInChunksCollect = async <T, R>(
  items: readonly T[],
  chunkSize: number,
  mapper: (item: T) => Promise<R>,
): Promise<R[]> =>
  items.length === 0
    ? []
    : [
        ...(await Promise.all(items.slice(0, chunkSize).map(mapper))),
        ...(await mapInChunksCollect(items.slice(chunkSize), chunkSize, mapper)),
      ];

export const handleRaceDetailSectionCacheQueue = async (
  openNextWorker: OpenNextWorker,
  batch: PcKeibaMessageBatch<CacheWarmMessage>,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  // Heatmaps are warmed by HeatmapWarmWorkflow, and races before today (JST)
  // are over: warming them only delays today's races behind a backlog (on
  // 2026-09-23 about half of the consumer's warms were 9/21-9/22 races). Ack
  // both without warming; past races still compute on demand.
  const todayJstYmd = formatTodayJstDate(new Date());
  const heatmaps = batch.messages.filter((message) => isHeatmapWarmMessage(message.body));
  const past = batch.messages.filter(
    (message) => !isHeatmapWarmMessage(message.body) && isStalePastRaceItem(message, todayJstYmd),
  );
  [...heatmaps, ...past].forEach(ackMessage);
  const warms = await mapInChunksCollect(
    batch.messages.filter((message) => !heatmaps.includes(message) && !past.includes(message)),
    WARM_IN_BATCH_CONCURRENCY,
    (message) => timedWarm({ ctx, env, openNextWorker }, message),
  );
  console.log(
    JSON.stringify({
      event: "cache_warm_batch",
      size: batch.messages.length,
      skippedHeatmap: heatmaps.length,
      skippedPast: past.length,
      warms,
    }),
  );
};
