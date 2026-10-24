export { HeatmapExecutionOwner } from "./worker/heatmap-execution-owner";
export { HeatmapWarmWorkflow } from "./worker/heatmap-warm-workflow";
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
import type { McpSiteFetchInit } from "./lib/mcp-tools";
import type {
  DetailSectionCacheWarmMessage,
  RaceDetailSsrCacheWarmMessage,
} from "./lib/race-detail-section-cache";
import type { RaceTrendCacheWarmMessage } from "./lib/race-trend-cache";
import { resolveSharedPageRequest } from "./lib/shared-page-access";
import { routeWebSocketUpgradeToDurableObject } from "./lib/websocket-do-router";
import { HEATMAP_BUILD_QUEUE_NAME, handleHeatmapBuildQueue } from "./worker/heatmap-build-queue";
import { createSelfFetch } from "./worker/heatmap-warm-workflow";
import { formatTodayJstDate, formatTomorrowJstDate } from "./worker/jst-date";
import {
  handleRaceDetailSectionCacheQueue,
  scheduleDueRaceTrendCache,
  scheduleRaceDetailSsrCacheWarm,
  scheduleTodayRaceDetailSectionCache,
  scheduleTodayWinRateHeatmapWarm,
  scheduleTomorrowRaceDetailSectionCache,
} from "./worker/race-detail-section-cache-warm";

const CLEF_RERANK_PREFIX: string = "/api/clef-rerank/";

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
      // The clef-flash bet Worker owns /api/clef-rerank/*; reach it over its service binding.
      fetchSite: (pathWithQuery: string, init?: McpSiteFetchInit) =>
        pathWithQuery.startsWith(CLEF_RERANK_PREFIX) && env.CLEF_RERANK !== undefined
          ? env.CLEF_RERANK.fetch(new Request(new URL(pathWithQuery, request.url)))
          : openNextWorker.fetch(
              new Request(new URL(pathWithQuery, request.url), {
                body: init?.body,
                headers:
                  init?.body === undefined ? undefined : { "content-type": "application/json" },
                method: init?.method === undefined ? "GET" : init.method,
                signal: init?.signal,
              }),
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
    const sharedRequest = await resolveSharedPageRequest({
      access:
        env.ACCESS_AUD === undefined || env.ACCESS_TEAM_DOMAIN === undefined
          ? undefined
          : { aud: env.ACCESS_AUD, fetchImpl: fetch, teamDomain: env.ACCESS_TEAM_DOMAIN },
      kv: env.MCP_OAUTH_KV,
      nowMs: Date.now(),
      request,
    });
    if (sharedRequest instanceof Response) {
      return sharedRequest;
    }
    return (
      routeWebSocketUpgradeToDurableObject(sharedRequest, env) ??
      openNextWorker.fetch(sharedRequest, env, ctx)
    );
  },
  queue(
    batch: PcKeibaMessageBatch<
      DetailSectionCacheWarmMessage | RaceDetailSsrCacheWarmMessage | RaceTrendCacheWarmMessage
    >,
    env: CloudflareEnv,
    ctx: PcKeibaExecutionContext,
  ) {
    // The detail-section handler acks heatmap-shaped messages without warming
    // them, so owned builds must be routed before reaching it.
    return batch.queue === HEATMAP_BUILD_QUEUE_NAME
      ? handleHeatmapBuildQueue(batch, createSelfFetch(env))
      : handleRaceDetailSectionCacheQueue(openNextWorker, batch, env, ctx);
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
          skipHeatmap: false,
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
      // Race-day sweep: fill any of today's detail sections missing since the 06:00 warm.
      ctx.waitUntil(
        scheduleTodayRaceDetailSectionCache({
          ctx,
          env,
          openNextWorker,
          skipHeatmap: true,
          todayJstYmd: formatTodayJstDate(new Date()),
        }),
      );
    }
    if (controller.cron === "*/15 0-12 * * *") {
      // Race-hours heatmap sweep (JST 09:00-21:59); the 06:00 JST pass covers
      // overnight runner updates. HIT races only need a cache probe.
      ctx.waitUntil(
        scheduleTodayWinRateHeatmapWarm({
          ctx,
          env,
          openNextWorker,
          todayJstYmd: formatTodayJstDate(new Date()),
        }),
      );
    }
  },
};
