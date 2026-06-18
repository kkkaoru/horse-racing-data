// Run with bun. Tests for the KV state helpers.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import { buildKvKey, isAlreadyRunning, readRunState, writeRunState } from "./kv-state";

const putMock = vi.fn(async () => undefined);
const getMock = vi.fn(async () => null as string | null);

const makeEnv = (): Env => ({
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: {} as unknown as Env["PREDICT_QUEUE"],
  PREDICT_STATE: { get: getMock, put: putMock } as unknown as KVNamespace,
  TRIGGER_TOKEN: "secret-token",
});

beforeEach(() => {
  putMock.mockClear();
  getMock.mockReset();
  getMock.mockResolvedValue(null);
});

test("buildKvKey returns correct format", () => {
  expect(buildKvKey({ category: "jra", runYmd: "20260603" })).toBe("predict:20260603:jra");
});

test("writeRunState puts JSON with correct key and 24h TTL", async () => {
  const env = makeEnv();
  await writeRunState({
    category: "jra",
    env,
    runYmd: "20260603",
    state: { startedAt: "2026-06-03T01:00:00.000Z", status: "started" },
  });
  expect(putMock).toHaveBeenCalledWith(
    "predict:20260603:jra",
    JSON.stringify({ startedAt: "2026-06-03T01:00:00.000Z", status: "started" }),
    { expirationTtl: 86400 },
  );
});

test("readRunState returns null when KV has no value", async () => {
  getMock.mockResolvedValue(null);
  const result = await readRunState({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toBe(null);
});

test("readRunState returns parsed state when found", async () => {
  getMock.mockResolvedValue(
    JSON.stringify({
      startedAt: "2026-06-03T01:00:00.000Z",
      status: "success",
      racesPredicted: 10,
    }),
  );
  const result = await readRunState({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toStrictEqual({
    startedAt: "2026-06-03T01:00:00.000Z",
    status: "success",
    racesPredicted: 10,
  });
});

test("isAlreadyRunning returns false when KV has no value", async () => {
  getMock.mockResolvedValue(null);
  const result = await isAlreadyRunning({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toBe(false);
});

test("isAlreadyRunning returns true when status is started", async () => {
  getMock.mockResolvedValue(
    JSON.stringify({ startedAt: "2026-06-03T01:00:00.000Z", status: "started" }),
  );
  const result = await isAlreadyRunning({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toBe(true);
});

test("isAlreadyRunning returns false when status is success", async () => {
  getMock.mockResolvedValue(
    JSON.stringify({ startedAt: "2026-06-03T01:00:00.000Z", status: "success", racesPredicted: 5 }),
  );
  const result = await isAlreadyRunning({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toBe(false);
});

test("isAlreadyRunning returns false when status is error", async () => {
  getMock.mockResolvedValue(
    JSON.stringify({
      startedAt: "2026-06-03T01:00:00.000Z",
      status: "error",
      error: "something failed",
    }),
  );
  const result = await isAlreadyRunning({ category: "jra", env: makeEnv(), runYmd: "20260603" });
  expect(result).toBe(false);
});
