// Run with bun. Pure builder for the container start() options.

import type { Env, PredictStartOptions } from "./types";

const PREDICT_ENTRYPOINT = ["python", "/app/src/predict_upcoming.py"];

interface BuildStartOptionsInput {
  env: Env;
  runDate: string;
  runYmd: string;
  category?: string;
}

const categoryEnvVars = (input: BuildStartOptionsInput): Record<string, string> =>
  input.category
    ? {
        MODELS_DIR: "/models",
        PREDICT_SERVE_MODE: "http",
        RS_SOURCE: "pg",
        SOURCE_DATABASE_URL: input.env.NEON_DATABASE_URL,
        category: input.category,
      }
    : {};

// Build the per-run container start configuration. Secrets (NEON_DATABASE_URL)
// and the run window are passed as container env vars; outbound internet is
// required because the container talks to Neon (TCP) and R2 (HTTPS). Returning
// a plain object keeps this unit-testable without any Container binding.
export const buildPredictStartOptions = (input: BuildStartOptionsInput): PredictStartOptions => ({
  enableInternet: true,
  entrypoint: PREDICT_ENTRYPOINT,
  envVars: {
    NEON_DATABASE_URL: input.env.NEON_DATABASE_URL,
    PREDICT_DAYS_AHEAD: input.env.PREDICT_DAYS_AHEAD,
    RUN_DATE: input.runYmd,
    RUN_DATE_ISO: input.runDate,
    ...categoryEnvVars(input),
  },
});
