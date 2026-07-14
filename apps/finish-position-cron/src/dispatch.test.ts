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
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
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

test("buildPredictStartOptions keeps Neon as output and passes Catalog as feature source", () => {
  const options = buildPredictStartOptions({
    category: "nar",
    env: makeEnv({
      NEON_DATABASE_URL: "postgres://write-output/db",
      R2_CATALOG_TOKEN: "catalog-token",
      R2_CATALOG_URI: "https://catalog.example.test/bucket",
      R2_CATALOG_WAREHOUSE: "account_bucket",
      SOURCE_DATABASE_URL: "r2-catalog://pc-keiba",
    }),
    runDate: "2026-07-15",
    runYmd: "20260715",
  });
  expect(options.envVars).toMatchObject({
    NEON_DATABASE_URL: "postgres://write-output/db",
    R2_CATALOG_TOKEN: "catalog-token",
    R2_CATALOG_URI: "https://catalog.example.test/bucket",
    R2_CATALOG_WAREHOUSE: "account_bucket",
    SOURCE_DATABASE_URL: "r2-catalog://pc-keiba",
  });
});

test("buildPredictStartOptions fails closed with an empty Catalog source", () => {
  const options = buildPredictStartOptions({
    env: makeEnv({ SOURCE_DATABASE_URL: undefined }),
    runDate: "2026-07-15",
    runYmd: "20260715",
  });
  expect(options.envVars.SOURCE_DATABASE_URL).toBe("");
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
