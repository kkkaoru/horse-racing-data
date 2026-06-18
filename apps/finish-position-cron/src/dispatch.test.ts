// Run with bun. Tests for the container start-options builder.

import { expect, test } from "vitest";
import { buildPredictStartOptions } from "./dispatch";
import type { Env } from "./types";

const makeEnv = (overrides: Partial<Env>): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_STATE: {} as unknown as KVNamespace,
  TRIGGER_TOKEN: "test-token",
  ...overrides,
});

test("buildPredictStartOptions enables internet for Neon + R2 egress", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({}),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.enableInternet).toBe(true);
});

test("buildPredictStartOptions sets the python predict entrypoint", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({}),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.entrypoint).toStrictEqual(["python", "/app/src/predict_upcoming.py"]);
});

test("buildPredictStartOptions passes the Neon secret as an env var", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({ NEON_DATABASE_URL: "postgres://secret-host/db" }),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.envVars.NEON_DATABASE_URL).toBe("postgres://secret-host/db");
});

test("buildPredictStartOptions passes the run window env vars", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({ PREDICT_DAYS_AHEAD: "3" }),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.envVars.PREDICT_DAYS_AHEAD).toBe("3");
  expect(options.envVars.RUN_DATE).toBe("20260603");
  expect(options.envVars.RUN_DATE_ISO).toBe("2026-06-03");
});

test("buildPredictStartOptions includes category env vars when category is provided", () => {
  const options = buildPredictStartOptions({
    category: "jra",
    env: makeEnv({}),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.envVars.category).toBe("jra");
  expect(options.envVars.PREDICT_SERVE_MODE).toBe("http");
  expect(options.envVars.RS_SOURCE).toBe("pg");
});

test("buildPredictStartOptions does not include category env vars when category is omitted", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({}),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(options.envVars.category).toBeUndefined();
  expect(options.envVars.PREDICT_SERVE_MODE).toBeUndefined();
});
