export { PaddockRoom } from "./worker/paddock-room";
import type { DetailSectionCacheWarmMessage } from "./lib/race-detail-section-cache";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleTomorrowRaceDetailSectionCache,
} from "./worker/race-detail-section-cache-warm";

// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import openNextWorker from "../.open-next/worker.js";

export default {
  ...openNextWorker,
  fetch: openNextWorker.fetch,
  queue(
    batch: PcKeibaMessageBatch<DetailSectionCacheWarmMessage>,
    env: CloudflareEnv,
    ctx: PcKeibaExecutionContext,
  ) {
    return handleRaceDetailSectionCacheQueue(openNextWorker, batch, env, ctx);
  },
  scheduled(_controller: unknown, env: CloudflareEnv, ctx: PcKeibaExecutionContext) {
    ctx.waitUntil(scheduleTomorrowRaceDetailSectionCache(openNextWorker, env, ctx));
  },
};
