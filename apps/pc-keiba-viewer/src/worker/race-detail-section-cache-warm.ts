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

type CacheWarmMessage = DetailSectionCacheWarmMessage | RaceTrendCacheWarmMessage;

type OpenNextWorker = {
  fetch(request: Request, env: CloudflareEnv, ctx: PcKeibaExecutionContext): Promise<Response>;
};

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
): Promise<void> => {
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

export const handleRaceDetailSectionCacheQueue = async (
  openNextWorker: OpenNextWorker,
  batch: PcKeibaMessageBatch<CacheWarmMessage>,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  await Promise.all(
    batch.messages.map(async (message) => {
      try {
        if (isRaceTrendCacheWarmMessage(message.body)) {
          await warmRaceTrend(openNextWorker, message.body, env, ctx);
        } else {
          await warmDetailSection(openNextWorker, message.body, env, ctx);
        }
        message.ack();
      } catch {
        message.retry();
      }
    }),
  );
};
