export { PaddockRoom } from "./worker/paddock-room";
export { RaceTrendRoom } from "./worker/race-trend-room";
// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import openNextWorker from "../.open-next/worker.js";
import type { DetailSectionCacheWarmMessage } from "./lib/race-detail-section-cache";
import type { RaceTrendCacheWarmMessage } from "./lib/race-trend-cache";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleDueRaceTrendCache,
  scheduleTomorrowRaceDetailSectionCache,
} from "./worker/race-detail-section-cache-warm";

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
    }
    if (controller.cron === "*/5 * * * *") {
      ctx.waitUntil(scheduleDueRaceTrendCache(openNextWorker, env, ctx));
    }
  },
};
