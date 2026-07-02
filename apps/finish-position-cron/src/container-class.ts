// Run with bun. Durable-Object-backed Container class for the predictor image.
// Held-fetch design: the queue consumer calls stub.fetch("/predict?...") which
// the DO proxies via containerFetch — the in-flight containerFetch keeps the
// container alive without any keepalive loop. sleepAfter resets automatically
// per CF docs while the HTTP request is in-flight. container-class.ts is
// excluded from the coverage gate (see vitest.config.ts).
//
// SLEEP_AFTER is "30m" (was "30s") because of the focused-full fire-and-forget
// fix: for a focused per-race full request (mode=full with both
// keibajoCode/raceBango set), the container's /predict endpoint now returns
// almost instantly after launching the real DuckDB+layer+scoring+Neon
// pipeline on a background thread (see queue-consumer.ts's
// FOCUSED_FULL_ACCEPTED_STATUS handling), rather than holding the response
// open for the whole 10-20+ minute run. That means the in-flight
// containerFetch no longer keeps sleepAfter reset for the pipeline's full
// duration -- the idle timer starts counting down from the moment the fast
// response is sent, not from when the background pipeline actually finishes.
// A short sleepAfter would risk the container being SIGTERM'd (default
// onActivityExpired behavior, not overridden here) mid-pipeline, killing the
// in-flight background thread before it ever reaches Neon.

import { Container } from "@cloudflare/containers";
import { proxyParquetFromNdjson } from "./container-ndjson-proxy";
import type { Env } from "./types";

const DEFAULT_PORT = 8080;
// 30m matches the same total retry-budget reasoning as
// FOCUSED_FULL_RETRY_DELAY_SECONDS * max_retries in queue-consumer.ts /
// wrangler.jsonc (2.5min x 12 retries = 30min), so the container reliably
// outlives a single worst-case background pipeline run even if, for some
// reason, no redelivery arrives in time to reset the timer via a new request.
// This value applies to ALL request types on this container class (day-batch,
// rescore, focused-full): day-batch/rescore already hold their own connection
// open continuously during their (currently unchanged, still-blocking)
// execution, so this mostly extends their post-completion idle tail -- an
// acceptable, deliberate cost/safety tradeoff for this fix. sleepAfter is not
// conditional per-request-type; that's not supported by the Container base
// class.
const SLEEP_AFTER = "30m";
const MODELS_DIR_DEFAULT = "/models";
const EMPTY_ENV_VALUE = "";
const ADMIN_STOP_PATH = "/__admin/stop-container";
const AUTH_HEADER = "authorization";
const BEARER_PREFIX = "Bearer ";

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
    if (url.pathname === ADMIN_STOP_PATH) {
      if (!isAuthorizedAdmin(request, this.env.TRIGGER_TOKEN)) {
        return Response.json({ error: "unauthorized", ok: false }, { status: 401 });
      }
      await this.destroy();
      return Response.json({ ok: true });
    }
    this.envVars = {
      MODELS_DIR: MODELS_DIR_DEFAULT,
      NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
      PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
      PREDICT_SERVE_MODE: "http",
      NAR_TRANSFORMER_BLEND_ENABLED: this.env.NAR_TRANSFORMER_BLEND_ENABLED ?? EMPTY_ENV_VALUE,
      SOURCE_DATABASE_URL: this.env.SOURCE_DATABASE_URL ?? EMPTY_ENV_VALUE,
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
      R2_BUCKET: this.env.R2_BUCKET ?? EMPTY_ENV_VALUE,
      VENUE_WEATHER_URL: this.env.VENUE_WEATHER_URL ?? EMPTY_ENV_VALUE,
    };
    try {
      const response = await this.containerFetch(request);
      return proxyParquetFromNdjson(
        response,
        this.env,
        this.ctx.waitUntil.bind(this.ctx),
        this.renewActivityTimeout.bind(this),
      );
    } catch (err) {
      console.error(`[container-class] containerFetch failed: ${String(err)}`);
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    }
  }
}
