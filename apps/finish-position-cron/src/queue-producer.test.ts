// Run with bun. Tests for the queue producer.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import { enqueuePredict } from "./queue-producer";

const sendMock = vi.fn(async () => undefined);

const makeEnv = (): Env => ({
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_STATE: {} as unknown as KVNamespace,
  TRIGGER_TOKEN: "secret-token",
});

beforeEach(() => {
  sendMock.mockClear();
});

test("enqueuePredict sends all 3 categories when category is omitted", async () => {
  const categories = await enqueuePredict({
    daysAhead: 2,
    env: makeEnv(),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(sendMock).toHaveBeenCalledTimes(3);
  expect(categories).toStrictEqual(["jra", "nar", "ban-ei"]);
});

test("enqueuePredict sends only the specified category when category is provided", async () => {
  const categories = await enqueuePredict({
    category: "nar",
    daysAhead: 2,
    env: makeEnv(),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(categories).toStrictEqual(["nar"]);
});

test("enqueuePredict returns the array of categories that were enqueued", async () => {
  const categories = await enqueuePredict({
    category: "ban-ei",
    daysAhead: 3,
    env: makeEnv(),
    runDate: "2026-06-04",
    runYmd: "20260604",
  });
  expect(categories).toStrictEqual(["ban-ei"]);
});

test("the message payload has all required fields", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    runDate: "2026-06-03",
    runYmd: "20260603",
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});
