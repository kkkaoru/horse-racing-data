// Run with bun. Tests for the queue producer.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import { enqueuePredict } from "./queue-producer";
import { PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";

const sendMock = vi.fn(async () => undefined);

const makeEnv = (): Env => ({
  FEATURES_CACHE: {} as unknown as R2Bucket,
  FINISH_POSITION_CRON_DB: {} as unknown as D1Database,
  FINISH_POSITION_PREDICT_CONTAINER: {} as unknown as Env["FINISH_POSITION_PREDICT_CONTAINER"],
  NEON_DATABASE_URL: "postgres://example",
  PREDICT_DAYS_AHEAD: "2",
  PREDICT_QUEUE: { send: sendMock } as unknown as Env["PREDICT_QUEUE"],
  PREDICT_RUN_COORDINATOR: {} as unknown as Env["PREDICT_RUN_COORDINATOR"],
  REALTIME_DB: {} as unknown as D1Database,
  TRIGGER_TOKEN: "secret-token",
});

const basePerRace = {
  keibajoCode: "05",
  raceBango: "11",
} as const;

beforeEach(() => {
  sendMock.mockClear();
});

test("enqueuePredict sends all 3 categories when category is omitted", async () => {
  const categories = await enqueuePredict({
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledTimes(3);
  expect(categories).toStrictEqual(["jra", "nar", "ban-ei"]);
});

test("enqueuePredict sends only the specified category when category is provided", async () => {
  const categories = await enqueuePredict({
    category: "nar",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(categories).toStrictEqual(["nar"]);
});

test("enqueuePredict returns the array of categories that were enqueued", async () => {
  const categories = await enqueuePredict({
    category: "ban-ei",
    daysAhead: 3,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-04",
    runYmd: "20260604",
    ...basePerRace,
  });
  expect(categories).toStrictEqual(["ban-ei"]);
});

test("the message payload has all required fields with mode full", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict sends rescore mode when mode is rescore", async () => {
  await enqueuePredict({
    category: "nar",
    daysAhead: 0,
    env: makeEnv(),
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
});

test("enqueuePredict attaches keibajoCode and raceBango for a per-race rescore", async () => {
  const categories = await enqueuePredict({
    category: "nar",
    daysAhead: 0,
    env: makeEnv(),
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runYmd: "20260619",
  });
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(categories).toStrictEqual(["nar"]);
  expect(sendMock).toHaveBeenCalledWith({
    category: "nar",
    daysAhead: 0,
    keibajoCode: "45",
    mode: "rescore",
    raceBango: "12",
    runDate: "2026-06-19",
    runDateIso: "2026-06-19",
    runYmd: "20260619",
  });
});

test("enqueuePredict attaches keibajoCode and raceBango for a per-race full build", async () => {
  const categories = await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-28",
    runYmd: "20260628",
  });
  expect(categories).toStrictEqual(["jra"]);
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-28",
    runDateIso: "2026-06-28",
    runYmd: "20260628",
  });
});

test("enqueuePredict preserves downstream full per-race trigger fields with skipDedup without requestId", async () => {
  const randomUuidSpy = vi
    .spyOn(crypto, "randomUUID")
    .mockReturnValue("00000000-0000-4000-8000-000000000001");
  try {
    const categories = await enqueuePredict({
      category: "jra",
      daysAhead: 2,
      env: makeEnv(),
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-06-28",
      runYmd: "20260628",
      skipDedup: true,
    });
    expect(sendMock).toHaveBeenCalledTimes(1);
    expect(categories).toStrictEqual(["jra"]);
    expect(sendMock).toHaveBeenCalledWith({
      category: "jra",
      daysAhead: 2,
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-06-28",
      runDateIso: "2026-06-28",
      runYmd: "20260628",
      skipDedup: true,
    });
    expect(randomUuidSpy).not.toHaveBeenCalled();
  } finally {
    randomUuidSpy.mockRestore();
  }
});

test("enqueuePredict rejects day-scoped enqueue without keibajoCode/raceBango", async () => {
  await expect(
    enqueuePredict({
      category: "jra",
      daysAhead: 2,
      env: makeEnv(),
      mode: "full",
      runDate: "2026-06-03",
      runYmd: "20260603",
      skipDedup: true,
    }),
  ).rejects.toThrow(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(sendMock).not.toHaveBeenCalled();
});

test("enqueuePredict rejects when only keibajoCode is provided", async () => {
  await expect(
    enqueuePredict({
      category: "nar",
      daysAhead: 0,
      env: makeEnv(),
      keibajoCode: "45",
      mode: "rescore",
      runDate: "2026-06-19",
      runYmd: "20260619",
    }),
  ).rejects.toThrow(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(sendMock).not.toHaveBeenCalled();
});

test("enqueuePredict rejects when only raceBango is provided", async () => {
  await expect(
    enqueuePredict({
      category: "nar",
      daysAhead: 0,
      env: makeEnv(),
      mode: "rescore",
      raceBango: "12",
      runDate: "2026-06-19",
      runYmd: "20260619",
    }),
  ).rejects.toThrow(PER_RACE_SCOPE_REQUIRED_ERROR);
  expect(sendMock).not.toHaveBeenCalled();
});

test("enqueuePredict attaches skipDedup when skipDedup is true", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    skipDedup: true,
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
    skipDedup: true,
  });
});

test("enqueuePredict attaches debug when debug is true", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    debug: true,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    debug: true,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict attaches force when force is true", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    force: true,
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    force: true,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict omits force when force is false", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    force: false,
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict omits force when force is undefined", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict omits skipDedup when skipDedup is false", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    skipDedup: false,
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict omits skipDedup when skipDedup is undefined", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});

test("enqueuePredict multi-category path still requires per-race fields", async () => {
  await enqueuePredict({
    daysAhead: 2,
    env: makeEnv(),
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledTimes(3);
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-06-03",
    runDateIso: "2026-06-03",
    runYmd: "20260603",
  });
});
