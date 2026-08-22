import {
  DETAIL_SECTION_CACHE_WARM_PARAM,
  buildDetailSectionApiPath,
  type DetailSectionCacheWarmMessage,
} from "../lib/race-detail-section-cache";
import { buildRaceTrendApiPath, type RaceTrendCacheWarmMessage } from "../lib/race-trend-cache";

const INTERNAL_ORIGIN = "https://pc-keiba-viewer.local";
const SCHEDULE_PATH = "/api/cache-warm/race-detail-sections";
const RACE_TREND_SCHEDULE_PATH = "/api/cache-warm/race-trends";
const RACE_DETAIL_SSR_SCHEDULE_PATH = "/api/cache-warm/race-detail-ssr";
const WARM_IN_BATCH_CONCURRENCY = 2;
const HEATMAP_WARM_CONCURRENCY = 1;
const HEATMAP_STORED_HEADERS: ReadonlyArray<string> = ["HIT", "MISS-STORED"];

type CacheWarmMessage = DetailSectionCacheWarmMessage | RaceTrendCacheWarmMessage;

interface QueueWarmItem {
  ack(): void;
  body: CacheWarmMessage;
  retry(): void;
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
  );
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
  );
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
  );
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
  );
  if (!response.ok) {
    throw new Error(`race detail SSR cache warm failed: ${response.status}`);
  }
};

const warmDetailSection = async (
  openNextWorker: OpenNextWorker,
  message: DetailSectionCacheWarmMessage,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<Response> => {
  const url = new URL(buildDetailSectionApiPath(message), INTERNAL_ORIGIN);
  url.searchParams.set(DETAIL_SECTION_CACHE_WARM_PARAM, "1");
  const response = await fetchSelf(
    openNextWorker,
    new Request(url, {
      headers: {
        "X-PC-Keiba-Cache-Warm": "queue",
      },
    }),
    env,
    ctx,
  );
  if (!response.ok) {
    throw new Error(`race detail cache warm failed: ${response.status} ${url.pathname}`);
  }
  return response;
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
    }),
    env,
    ctx,
  );
  if (!response.ok) {
    throw new Error(`race trend cache warm failed: ${response.status} ${url.pathname}`);
  }
};

const isRaceTrendCacheWarmMessage = (
  message: CacheWarmMessage,
): message is RaceTrendCacheWarmMessage => "kind" in message && message.kind === "race-trend";

const isHeatmapWarmMessage = (message: CacheWarmMessage): boolean =>
  !isRaceTrendCacheWarmMessage(message) && message.section === "win-rate-heatmap";

const assertHeatmapCacheStored = (response: Response): void => {
  const header = response.headers.get("X-Win-Rate-Heatmap-Cache");
  if (header !== null && HEATMAP_STORED_HEADERS.some((value) => value === header)) {
    return;
  }
  throw new Error(`heatmap cache was not stored: ${header ?? "missing"}`);
};

const mapInChunks = async <T>(
  items: readonly T[],
  chunkSize: number,
  mapper: (item: T) => Promise<void>,
): Promise<void> => {
  if (items.length === 0) {
    return;
  }
  await Promise.all(items.slice(0, chunkSize).map(mapper));
  await mapInChunks(items.slice(chunkSize), chunkSize, mapper);
};

const warmQueueMessage = async (
  openNextWorker: OpenNextWorker,
  message: QueueWarmItem,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  try {
    if (isRaceTrendCacheWarmMessage(message.body)) {
      await warmRaceTrend(openNextWorker, message.body, env, ctx);
      message.ack();
      return;
    }
    const response = await warmDetailSection(openNextWorker, message.body, env, ctx);
    if (message.body.section === "win-rate-heatmap") {
      assertHeatmapCacheStored(response);
    }
    message.ack();
  } catch (error) {
    const detail = error instanceof Error ? error.message : String(error);
    console.error("[pc-keiba-viewer] race detail cache warm failed", detail);
    message.retry();
  }
};

export const handleRaceDetailSectionCacheQueue = async (
  openNextWorker: OpenNextWorker,
  batch: PcKeibaMessageBatch<CacheWarmMessage>,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  const heatmapMessages = batch.messages.filter((message) => isHeatmapWarmMessage(message.body));
  const otherMessages = batch.messages.filter((message) => !isHeatmapWarmMessage(message.body));
  await mapInChunks(otherMessages, WARM_IN_BATCH_CONCURRENCY, (message) =>
    warmQueueMessage(openNextWorker, message, env, ctx),
  );
  await mapInChunks(heatmapMessages, HEATMAP_WARM_CONCURRENCY, (message) =>
    warmQueueMessage(openNextWorker, message, env, ctx),
  );
};
