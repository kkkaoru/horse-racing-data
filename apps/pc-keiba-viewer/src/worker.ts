export { PaddockRoom } from "./worker/paddock-room";
export { RaceTrendRoom } from "./worker/race-trend-room";
// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import openNextWorker from "../.open-next/worker.js";
import type { DetailSectionCacheWarmMessage } from "./lib/race-detail-section-cache";
import type { RaceTrendCacheWarmMessage } from "./lib/race-trend-cache";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleDueRaceTrendCache,
  scheduleRaceDetailSsrCacheWarm,
  scheduleTomorrowRaceDetailSectionCache,
  scheduleTopRacesCacheWarm,
} from "./worker/race-detail-section-cache-warm";

const formatTomorrowJstDate = (now: Date): string => {
  const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
  const parts = new Intl.DateTimeFormat("en-CA", {
    day: "2-digit",
    month: "2-digit",
    timeZone: "Asia/Tokyo",
    year: "numeric",
  }).formatToParts(tomorrow);
  const lookup = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${lookup.year ?? "1970"}-${lookup.month ?? "01"}-${lookup.day ?? "01"}`;
};

export default {
  ...openNextWorker,
  fetch: openNextWorker.fetch,
  queue(
    batch: PcKeibaMessageBatch<DetailSectionCacheWarmMessage | RaceTrendCacheWarmMessage>,
    env: CloudflareEnv,
    ctx: PcKeibaExecutionContext,
  ) {
    return handleRaceDetailSectionCacheQueue(openNextWorker, batch, env, ctx);
  },
  scheduled(controller: { cron?: string }, env: CloudflareEnv, ctx: PcKeibaExecutionContext) {
    if (controller.cron === "0 12 * * *") {
      ctx.waitUntil(scheduleTomorrowRaceDetailSectionCache(openNextWorker, env, ctx));
      ctx.waitUntil(
        scheduleRaceDetailSsrCacheWarm(openNextWorker, env, ctx, {
          date: formatTomorrowJstDate(new Date()),
        }),
      );
    }
    if (controller.cron === "*/5 * * * *") {
      ctx.waitUntil(scheduleDueRaceTrendCache(openNextWorker, env, ctx));
      ctx.waitUntil(scheduleTopRacesCacheWarm(openNextWorker, env, ctx));
    }
    if (controller.cron === "*/15 * * * *") {
      ctx.waitUntil(scheduleRaceDetailSsrCacheWarm(openNextWorker, env, ctx));
    }
  },
};
