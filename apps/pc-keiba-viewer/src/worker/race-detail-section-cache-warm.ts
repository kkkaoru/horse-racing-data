import {
  DETAIL_SECTION_CACHE_WARM_PARAM,
  buildDetailSectionApiPath,
  type DetailSectionCacheWarmMessage,
} from "../lib/race-detail-section-cache";

const INTERNAL_ORIGIN = "https://pc-keiba-viewer.local";
const SCHEDULE_PATH = "/api/cache-warm/race-detail-sections";

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

export const handleRaceDetailSectionCacheQueue = async (
  openNextWorker: OpenNextWorker,
  batch: PcKeibaMessageBatch<DetailSectionCacheWarmMessage>,
  env: CloudflareEnv,
  ctx: PcKeibaExecutionContext,
): Promise<void> => {
  await Promise.all(
    batch.messages.map(async (message) => {
      try {
        await warmDetailSection(openNextWorker, message.body, env, ctx);
        message.ack();
      } catch {
        message.retry();
      }
    }),
  );
};
