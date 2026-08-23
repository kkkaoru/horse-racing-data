// Run with bun. Tests for the queue producer.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";
import { enqueuePredict } from "./queue-producer";
import { PER_RACE_SCOPE_REQUIRED_ERROR } from "./per-race-scope-guard";

const {
  completeFocusedFullRaceMock,
  failFocusedFullRaceEnqueueMock,
  featureHeadMock,
  reserveFocusedFullRaceEnqueueMock,
} = vi.hoisted(() => ({
  completeFocusedFullRaceMock: vi.fn(async () => undefined),
  failFocusedFullRaceEnqueueMock: vi.fn(async () => undefined),
  featureHeadMock: vi.fn(async (): Promise<R2Object | null> => null),
  reserveFocusedFullRaceEnqueueMock: vi.fn(
    async (): Promise<{ proceed: boolean; state?: string }> => ({ proceed: true }),
  ),
}));

vi.mock("./do-state", () => ({
  completeFocusedFullRace: completeFocusedFullRaceMock,
  failFocusedFullRaceEnqueue: failFocusedFullRaceEnqueueMock,
  reserveFocusedFullRaceEnqueue: reserveFocusedFullRaceEnqueueMock,
}));

const sendMock = vi.fn(async () => undefined);

const makeEnv = (): Env => ({
  FEATURES_CACHE: { head: featureHeadMock } as unknown as R2Bucket,
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
  sendMock.mockReset();
  sendMock.mockResolvedValue(undefined);
  failFocusedFullRaceEnqueueMock.mockClear();
  completeFocusedFullRaceMock.mockClear();
  featureHeadMock.mockReset();
  featureHeadMock.mockResolvedValue(null);
  reserveFocusedFullRaceEnqueueMock.mockReset();
  reserveFocusedFullRaceEnqueueMock.mockResolvedValue({ proceed: true });
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

test("enqueuePredict attaches raceStartAtJst when day-base fanout provides it", async () => {
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    keibajoCode: "07",
    mode: "full",
    raceBango: "11",
    raceStartAtJst: "2026-08-22T17:50:00+09:00",
    runDate: "2026-08-22",
    runYmd: "20260822",
  });
  expect(sendMock).toHaveBeenCalledWith({
    category: "jra",
    daysAhead: 2,
    keibajoCode: "07",
    mode: "full",
    raceBango: "11",
    raceStartAtJst: "2026-08-22T17:50:00+09:00",
    runDate: "2026-08-22",
    runDateIso: "2026-08-22",
    runYmd: "20260822",
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
    expect(randomUuidSpy).toHaveBeenCalledTimes(1);
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
  vi.useFakeTimers();
  vi.setSystemTime(new Date("2026-06-03T00:00:00.123Z"));
  try {
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
      forceRequestedAt: "2026-06-03T00:00:00.123Z",
      keibajoCode: "05",
      mode: "full",
      raceBango: "11",
      runDate: "2026-06-03",
      runDateIso: "2026-06-03",
      runYmd: "20260603",
    });
  } finally {
    vi.useRealTimers();
  }
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

test("enqueuePredict records tracked self-heal lifecycle around queue send", async () => {
  const runMock = vi.fn(async () => undefined);
  const prepareMock = vi.fn(() => ({ bind: vi.fn(() => ({ run: runMock })) }));
  const env = {
    ...makeEnv(),
    FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  };
  await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    deliveryTrackingId: "tracking-id",
    env,
    mode: "full",
    runDate: "2026-06-03",
    runYmd: "20260603",
    ...basePerRace,
  });
  expect(sendMock).toHaveBeenCalledWith(
    expect.objectContaining({ deliveryTrackingId: "tracking-id" }),
  );
  expect(prepareMock).toHaveBeenCalledTimes(2);
});

test("enqueuePredict keeps its reservation after Queue send when enqueued telemetry fails", async () => {
  const runMock = vi
    .fn(async () => undefined)
    .mockResolvedValueOnce(undefined)
    .mockRejectedValueOnce(new Error("D1 unavailable"));
  const prepareMock = vi.fn(() => ({ bind: vi.fn(() => ({ run: runMock })) }));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  const env = {
    ...makeEnv(),
    FINISH_POSITION_CRON_DB: { prepare: prepareMock } as unknown as D1Database,
  };
  await expect(
    enqueuePredict({
      category: "jra",
      daysAhead: 2,
      deliveryTrackingId: "tracking-id",
      env,
      mode: "full",
      runDate: "2026-08-23",
      runYmd: "20260823",
      skipDedup: true,
      ...basePerRace,
    }),
  ).resolves.toStrictEqual(["jra"]);
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(failFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
  expect(errorSpy).toHaveBeenCalledWith(
    "Failed to record enqueued prediction delivery:",
    "Error: D1 unavailable",
  );
  errorSpy.mockRestore();
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

test("enqueuePredict suppresses a duplicate full enqueue reserved by another producer", async () => {
  reserveFocusedFullRaceEnqueueMock.mockResolvedValue({ proceed: false, state: "enqueued" });
  const categories = await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-08-23",
    runYmd: "20260823",
    skipDedup: true,
  });
  expect(categories).toStrictEqual([]);
  expect(sendMock).not.toHaveBeenCalled();
  expect(failFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
});

test("enqueuePredict keeps a completed race suppressed when its exact feature cache exists", async () => {
  reserveFocusedFullRaceEnqueueMock.mockResolvedValue({ proceed: false, state: "success" });
  featureHeadMock.mockResolvedValue({} as R2Object);

  await expect(
    enqueuePredict({
      category: "ban-ei",
      daysAhead: 2,
      env: makeEnv(),
      keibajoCode: "83",
      mode: "full",
      raceBango: "01",
      runDate: "2026-08-24",
      runYmd: "20260824",
      skipDedup: true,
    }),
  ).resolves.toStrictEqual([]);

  expect(featureHeadMock).toHaveBeenCalledWith(
    "feat-cache/catalog-v1/ban-ei/20260824/83/01/features.parquet",
  );
  expect(completeFocusedFullRaceMock).not.toHaveBeenCalled();
  expect(sendMock).not.toHaveBeenCalled();
});

test("enqueuePredict reopens only a cacheless completed race and reserves its repair", async () => {
  reserveFocusedFullRaceEnqueueMock
    .mockResolvedValueOnce({ proceed: false, state: "success" })
    .mockResolvedValueOnce({ proceed: true });
  const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => undefined);

  await expect(
    enqueuePredict({
      category: "ban-ei",
      daysAhead: 2,
      env: makeEnv(),
      keibajoCode: "83",
      mode: "full",
      raceBango: "01",
      runDate: "2026-08-24",
      runYmd: "20260824",
      skipDedup: true,
    }),
  ).resolves.toStrictEqual(["ban-ei"]);

  expect(completeFocusedFullRaceMock).toHaveBeenCalledWith({
    category: "ban-ei",
    env: expect.anything(),
    keibajoCode: "83",
    raceBango: "01",
    runYmd: "20260824",
    status: "error",
  });
  expect(reserveFocusedFullRaceEnqueueMock).toHaveBeenCalledTimes(2);
  expect(sendMock).toHaveBeenCalledTimes(1);
  expect(warnSpy).toHaveBeenCalledWith(
    "[predict-producer] reopened cacheless focused-full success category=ban-ei runYmd=20260824 keibajo=83 race=01",
  );
  warnSpy.mockRestore();
});

test("enqueuePredict releases its full enqueue reservation when Queue send fails", async () => {
  const randomUuidSpy = vi
    .spyOn(crypto, "randomUUID")
    .mockReturnValue("00000000-0000-4000-8000-000000000002");
  sendMock.mockRejectedValueOnce(new Error("queue unavailable"));
  await expect(
    enqueuePredict({
      category: "nar",
      daysAhead: 2,
      env: makeEnv(),
      keibajoCode: "44",
      mode: "full",
      raceBango: "03",
      runDate: "2026-08-23",
      runYmd: "20260823",
      skipDedup: true,
    }),
  ).rejects.toThrow("queue unavailable");
  expect(failFocusedFullRaceEnqueueMock).toHaveBeenCalledWith({
    category: "nar",
    env: expect.anything(),
    keibajoCode: "44",
    raceBango: "03",
    reservationId: "00000000-0000-4000-8000-000000000002",
    runYmd: "20260823",
  });
  randomUuidSpy.mockRestore();
});

test("enqueuePredict preserves Queue failure when reservation release also fails", async () => {
  sendMock.mockRejectedValueOnce(new Error("queue unavailable"));
  failFocusedFullRaceEnqueueMock.mockRejectedValueOnce(new Error("coordinator unavailable"));
  const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
  await expect(
    enqueuePredict({
      category: "nar",
      daysAhead: 2,
      env: makeEnv(),
      keibajoCode: "44",
      mode: "full",
      raceBango: "03",
      runDate: "2026-08-23",
      runYmd: "20260823",
      skipDedup: true,
    }),
  ).rejects.toThrow("queue unavailable");
  expect(errorSpy).toHaveBeenCalledWith(
    "Failed to release focused-full enqueue reservation:",
    "Error: coordinator unavailable",
  );
  errorSpy.mockRestore();
});

test("enqueuePredict force full bypasses semantic reservation", async () => {
  const categories = await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    force: true,
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-08-23",
    runYmd: "20260823",
    skipDedup: true,
  });
  expect(categories).toStrictEqual(["jra"]);
  expect(reserveFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
});

test("enqueuePredict legacy full without skipDedup keeps category-run dedup and bypasses focused reservation", async () => {
  const categories = await enqueuePredict({
    category: "jra",
    daysAhead: 2,
    env: makeEnv(),
    keibajoCode: "05",
    mode: "full",
    raceBango: "11",
    runDate: "2026-08-23",
    runYmd: "20260823",
  });
  expect(categories).toStrictEqual(["jra"]);
  expect(reserveFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
});

test("enqueuePredict rescore bypasses focused-full semantic reservation", async () => {
  const categories = await enqueuePredict({
    category: "jra",
    daysAhead: 0,
    env: makeEnv(),
    keibajoCode: "05",
    mode: "rescore",
    raceBango: "11",
    runDate: "2026-08-23",
    runYmd: "20260823",
  });
  expect(categories).toStrictEqual(["jra"]);
  expect(reserveFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
});

test("enqueuePredict rescore Queue failure does not release a focused-full reservation", async () => {
  sendMock.mockRejectedValueOnce(new Error("rescore queue unavailable"));
  await expect(
    enqueuePredict({
      category: "jra",
      daysAhead: 0,
      env: makeEnv(),
      keibajoCode: "05",
      mode: "rescore",
      raceBango: "11",
      runDate: "2026-08-23",
      runYmd: "20260823",
    }),
  ).rejects.toThrow("rescore queue unavailable");
  expect(failFocusedFullRaceEnqueueMock).not.toHaveBeenCalled();
});
