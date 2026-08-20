// Run with bun. Durable-Object-backed Container class for the predictor image.
// Queue/admin callers hit stub.fetch("/predict?...") and the DO proxies through
// containerFetch. Focused full returns "accepted" quickly while the Python
// process continues in a detached thread; sleepAfter keeps the category
// container alive long enough for Neon completion polling to observe the write.
// container-class.ts is excluded from the coverage gate (see vitest.config.ts).
//
// SLEEP_AFTER is "20m": first-day Ban-ei day-base + race-chain can take
// 10–15m after a detached "accepted". 5m expired the instance mid-build
// (Activity expired). 20m is well below the old 90m slot hold.

import { Container } from "@cloudflare/containers";
import { proxyParquetFromNdjson } from "./container-ndjson-proxy";
import type { Env } from "./types";

const DEFAULT_PORT = 8080;
// 20m covers a detached first-day day-base build (10–15m) plus race-chain.
const SLEEP_AFTER = "20m";
const MODELS_DIR_DEFAULT = "/models";
const EMPTY_ENV_VALUE = "";
const ADMIN_STOP_PATH = "/__admin/stop-container";
const AUTH_HEADER = "authorization";
const BEARER_PREFIX = "Bearer ";

const isDebugRequest = (url: URL): boolean => {
  const value = url.searchParams.get("debug");
  if (value === null) return false;
  return ["1", "true", "yes", "on", "debug"].includes(value.trim().toLowerCase());
};

const describePredictRequest = (url: URL): string => {
  const searchParams = url.searchParams;
  return [
    `path=${url.pathname}`,
    `category=${searchParams.get("category") ?? "-"}`,
    `runDate=${searchParams.get("runDate") ?? "-"}`,
    `mode=${searchParams.get("mode") ?? "-"}`,
    `keibajo=${searchParams.get("keibajoCode") ?? "-"}`,
    `race=${searchParams.get("raceBango") ?? "-"}`,
  ].join(" ");
};

const isAuthorizedAdmin = (request: Request, token: string): boolean => {
  const header = request.headers.get(AUTH_HEADER);
  if (!header?.startsWith(BEARER_PREFIX)) return false;
  return header.slice(BEARER_PREFIX.length) === token;
};

export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  override async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const requestSummary = describePredictRequest(url);
    if (url.pathname === ADMIN_STOP_PATH) {
      if (!isAuthorizedAdmin(request, this.env.TRIGGER_TOKEN)) {
        console.warn(`[predict-container-do] admin-stop unauthorized ${requestSummary}`);
        return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
      }
      console.warn(`[predict-container-do] admin-stop requested ${requestSummary}`);
      await this.destroy();
      console.warn(`[predict-container-do] admin-stop destroyed ${requestSummary}`);
      return Response.json({ ok: true });
    }
    this.envVars = {
      MODELS_DIR: MODELS_DIR_DEFAULT,
      NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
      PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
      PREDICT_SERVE_MODE: "http",
      NAR_TRANSFORMER_BLEND_ENABLED: this.env.NAR_TRANSFORMER_BLEND_ENABLED ?? EMPTY_ENV_VALUE,
      STAGE1_PRESERVED_ODDS_GATE_ENABLED:
        this.env.STAGE1_PRESERVED_ODDS_GATE_ENABLED ?? EMPTY_ENV_VALUE,
      DAY_BASE_SPLIT_ENABLED: this.env.DAY_BASE_SPLIT_ENABLED ?? EMPTY_ENV_VALUE,
      SOURCE_DATABASE_URL: this.env.SOURCE_DATABASE_URL ?? EMPTY_ENV_VALUE,
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
      R2_BUCKET: this.env.R2_BUCKET ?? EMPTY_ENV_VALUE,
      R2_CATALOG_TOKEN: this.env.R2_CATALOG_TOKEN ?? EMPTY_ENV_VALUE,
      R2_CATALOG_URI: this.env.R2_CATALOG_URI ?? EMPTY_ENV_VALUE,
      R2_CATALOG_WAREHOUSE: this.env.R2_CATALOG_WAREHOUSE ?? EMPTY_ENV_VALUE,
      VENUE_WEATHER_URL: this.env.VENUE_WEATHER_URL ?? EMPTY_ENV_VALUE,
      PYTHONUNBUFFERED: "1",
    };
    const startedAt = Date.now();
    const debug = isDebugRequest(url);
    try {
      if (debug) console.log(`[predict-container-do] fetch start ${requestSummary}`);
      const response = await this.containerFetch(request);
      if (debug) {
        console.log(
          `[predict-container-do] fetch response ${requestSummary} status=${response.status} durationMs=${
            Date.now() - startedAt
          }`,
        );
      }
      return proxyParquetFromNdjson(
        response,
        this.env,
        this.ctx.waitUntil.bind(this.ctx),
        this.renewActivityTimeout.bind(this),
        debug,
      );
    } catch (err) {
      console.error(
        `[predict-container-do] fetch failed ${requestSummary} durationMs=${
          Date.now() - startedAt
        }: ${String(err)}`,
      );
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    }
  }
}
