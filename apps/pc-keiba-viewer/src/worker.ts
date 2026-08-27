export { PaddockRoom } from "./worker/paddock-room";
export { RaceTrendRoom } from "./worker/race-trend-room";
// @ts-ignore OpenNext generates this file before Wrangler bundles the Worker.
import openNextWorker from "../.open-next/worker.js";
import { handleMcpOauthHttp } from "./lib/mcp-oauth-http";
import { createKvOauthStore } from "./lib/mcp-oauth-store";
import {
  handlePcKeibaMcpRequest,
  readMcpAuthToken,
  readMcpOauthSigningKey,
} from "./lib/mcp-request";
import type {
  DetailSectionCacheWarmMessage,
  RaceDetailSsrCacheWarmMessage,
} from "./lib/race-detail-section-cache";
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
  async fetch(
    request: Request,
    env: CloudflareEnv,
    ctx: PcKeibaExecutionContext,
  ): Promise<Response> {
    const oauthStore = env.MCP_OAUTH_KV === undefined ? null : createKvOauthStore(env.MCP_OAUTH_KV);
    if (oauthStore !== null) {
      const oauthResponse = await handleMcpOauthHttp({
        fetchImpl: fetch,
        nowSeconds: Math.floor(Date.now() / 1000),
        request,
        signingKey: readMcpOauthSigningKey(env),
        store: oauthStore,
      });
      if (oauthResponse !== null) {
        return oauthResponse;
      }
    }
    const mcpResponse = await handlePcKeibaMcpRequest({
      fetchSite: (pathWithQuery: string, signal?: AbortSignal) =>
        openNextWorker.fetch(
          new Request(new URL(pathWithQuery, request.url), { method: "GET", signal }),
          env,
          ctx,
        ),
      mcpAuthToken: readMcpAuthToken(env),
      nowSeconds: Math.floor(Date.now() / 1000),
      oauthSigningKey: readMcpOauthSigningKey(env),
      request,
    });
    if (mcpResponse !== null) {
      return mcpResponse;
    }
    return (
      routeWebSocketUpgradeToDurableObject(request, env) ?? openNextWorker.fetch(request, env, ctx)
    );
  },
  queue(
    batch: PcKeibaMessageBatch<
      DetailSectionCacheWarmMessage | RaceDetailSsrCacheWarmMessage | RaceTrendCacheWarmMessage
    >,
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
