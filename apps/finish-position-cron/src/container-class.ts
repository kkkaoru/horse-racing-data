// Run with bun. Durable-Object-backed Container class for the predictor image.
// Held-fetch design: the queue consumer calls stub.fetch("/predict?...") which
// the DO proxies via containerFetch — the in-flight containerFetch keeps the
// container alive without any keepalive loop. sleepAfter resets automatically
// per CF docs while the HTTP request is in-flight. container-class.ts is
// excluded from the coverage gate (see vitest.config.ts).

import { Container } from "@cloudflare/containers";
import { proxyParquetFromNdjson } from "./container-ndjson-proxy";
import type { Env } from "./types";

const DEFAULT_PORT = 8080;
const SLEEP_AFTER = "15m";
const MODELS_DIR_DEFAULT = "/models";
const EMPTY_ENV_VALUE = "";

export class FinishPositionPredictContainer extends Container<Env> {
  override defaultPort = DEFAULT_PORT;
  override sleepAfter = SLEEP_AFTER;
  override enableInternet = true;

  override async fetch(request: Request): Promise<Response> {
    this.envVars = {
      MODELS_DIR: MODELS_DIR_DEFAULT,
      NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
      PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
      SOURCE_DATABASE_URL: this.env.SOURCE_DATABASE_URL ?? EMPTY_ENV_VALUE,
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
      R2_BUCKET: this.env.R2_BUCKET ?? EMPTY_ENV_VALUE,
      VENUE_WEATHER_URL: this.env.VENUE_WEATHER_URL ?? EMPTY_ENV_VALUE,
    };
    try {
      const response = await this.containerFetch(request);
      return proxyParquetFromNdjson(response, this.env, this.ctx.waitUntil.bind(this.ctx));
    } catch (err) {
      console.error(`[container-class] containerFetch failed: ${String(err)}`);
      return Response.json(
        { error: "Container start failed", detail: String(err) },
        { status: 502 },
      );
    }
  }
}
