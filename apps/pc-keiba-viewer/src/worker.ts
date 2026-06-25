export { PaddockRoom } from "./worker/paddock-room";
export { RaceTrendRoom } from "./worker/race-trend-room";
// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import openNextWorker from "../.open-next/worker.js";
import type { DetailSectionCacheWarmMessage } from "./lib/race-detail-section-cache";
import type { RaceTrendCacheWarmMessage } from "./lib/race-trend-cache";
import { routeWebSocketUpgradeToDurableObject } from "./lib/websocket-do-router";
import { formatTodayJstDate, formatTomorrowJstDate } from "./worker/jst-date";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleDueRaceTrendCache,
  scheduleRaceDetailSsrCacheWarm,
  scheduleTodayRaceDetailSectionCache,
  scheduleTomorrowRaceDetailSectionCache,
} from "./worker/race-detail-section-cache-warm";

export default {
  ...openNextWorker,
  fetch(request: Request, env: CloudflareEnv, ctx: PcKeibaExecutionContext): Promise<Response> {
    return (
      routeWebSocketUpgradeToDurableObject(request, env) ?? openNextWorker.fetch(request, env, ctx)
    );
  },
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
    if (controller.cron === "0 21 * * *") {
      const todayJst = formatTodayJstDate(new Date());
      ctx.waitUntil(
        scheduleTodayRaceDetailSectionCache({
          ctx,
          env,
          openNextWorker,
          todayJstYmd: todayJst,
        }),
      );
      ctx.waitUntil(scheduleRaceDetailSsrCacheWarm(openNextWorker, env, ctx, { date: todayJst }));
    }
    if (controller.cron === "*/5 0-14 * * *") {
      ctx.waitUntil(scheduleDueRaceTrendCache(openNextWorker, env, ctx));
    }
    if (controller.cron === "*/15 0-14 * * *") {
      ctx.waitUntil(scheduleRaceDetailSsrCacheWarm(openNextWorker, env, ctx));
    }
  },
};
