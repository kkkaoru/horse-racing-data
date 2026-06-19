// Run with bun. Durable-Object-backed Container class for the predictor image.
// Held-fetch design: the queue consumer calls stub.fetch("/predict?...") which
// the DO proxies via containerFetch — the in-flight containerFetch keeps the
// container alive without any keepalive loop. sleepAfter resets automatically
// per CF docs while the HTTP request is in-flight. container-class.ts is
// excluded from the coverage gate (see vitest.config.ts).

import { Container } from "@cloudflare/containers";
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
    // Inject Worker runtime secrets into container env before containerFetch
    // starts the container. The Dockerfile default PREDICT_SERVE_MODE=http
    // activates the HTTP /predict server mode. NEON_DATABASE_URL is required
    // at container bootstrap time.
    this.envVars = {
      MODELS_DIR: MODELS_DIR_DEFAULT,
      NEON_DATABASE_URL: this.env.NEON_DATABASE_URL,
      PREDICT_DAYS_AHEAD: this.env.PREDICT_DAYS_AHEAD,
      // R2 S3 credentials for the Python rescore path. envVars is
      // Record<string,string>, so coerce an undefined secret/var to "" — an empty
      // value is exactly what _load_r2_config treats as absent ("skip R2").
      R2_ACCOUNT_ID: this.env.R2_ACCOUNT_ID ?? EMPTY_ENV_VALUE,
      R2_ACCESS_KEY_ID: this.env.R2_ACCESS_KEY_ID ?? EMPTY_ENV_VALUE,
      R2_SECRET_ACCESS_KEY: this.env.R2_SECRET_ACCESS_KEY ?? EMPTY_ENV_VALUE,
      R2_BUCKET: this.env.R2_BUCKET ?? EMPTY_ENV_VALUE,
    };
    return this.containerFetch(request);
  }
}
