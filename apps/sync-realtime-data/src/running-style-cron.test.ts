// Run with bun test apps/sync-realtime-data/src/running-style-cron.test.ts
import { expect, test, vi } from "vitest";

import {
  RUNNING_STYLE_INFERENCE_CRON,
  RUNNING_STYLE_PREWARM_CRON,
  addDaysToYYYYMMDDInJst,
  formatYYYYMMDDInJst,
  formatTomorrowYYYYMMDDInJst,
  resolveRunningStyleCronDates,
  runRunningStyleCronTick,
  selectRacesNeedingRunningStyleInference,
  type RegisteredRaceRow,
} from "./running-style-cron";
import type { Env } from "./types";

const RACE: RegisteredRaceRow = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0519",
  keibajo_code: "46",
  race_bango: "12",
  source: "nar",
};

const buildDbWithRegisteredRaces = (races: RegisteredRaceRow[]): D1Database =>
  ({
    prepare: vi.fn(() => ({
      all: vi.fn(async () => ({ results: races })),
      bind: vi.fn().mockReturnThis(),
    })),
  }) as unknown as D1Database;

test("RUNNING_STYLE_INFERENCE_CRON runs every 10 min during JST race hours", () => {
  expect(RUNNING_STYLE_INFERENCE_CRON).toBe("*/10 0-14 * * *");
});

test("RUNNING_STYLE_PREWARM_CRON runs at 21:00 JST", () => {
  expect(RUNNING_STYLE_PREWARM_CRON).toBe("0 12 * * *");
});

test("formatYYYYMMDDInJst formats a UTC instant as JST date", () => {
  expect(formatYYYYMMDDInJst(new Date("2026-05-18T16:00:00Z"))).toBe("20260519");
});

test("formatTomorrowYYYYMMDDInJst returns the next JST date", () => {
  expect(formatTomorrowYYYYMMDDInJst(new Date("2026-05-20T12:00:00Z"))).toBe("20260521");
  expect(addDaysToYYYYMMDDInJst("20260228", 1)).toBe("20260301");
});

test("runRunningStyleCronTick does not touch Postgres or enqueue when disabled", async () => {
  const send = vi.fn();
  const env = {
    REALTIME_DB: buildDbWithRegisteredRaces([RACE]),
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
    RUNNING_STYLE_D1_WRITE_ENABLED: "0",
  } as unknown as Env;

  const result = await runRunningStyleCronTick(env, new Date("2026-05-19T00:00:00Z"));

  expect(result).toMatchObject({ enqueued: 0, scanned: 1 });
  expect(send).not.toHaveBeenCalled();
});

test("selectRacesNeedingRunningStyleInference queues races with incomplete predictions", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 10]]),
    new Map(),
  );

  expect(selected.needed).toHaveLength(1);
  expect(selected.needed[0]).toMatchObject({
    expectedHorseCount: 14,
    existingHorseCount: 10,
    raceKey: "nar:20260519:46:12",
  });
});

test("selectRacesNeedingRunningStyleInference treats active-only coverage as completed", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 12]]),
    new Map([["nar:20260519:46:12", 12]]),
    new Map(),
  );

  expect(selected.needed).toHaveLength(0);
  expect(selected.completed).toBe(1);
});

test("selectRacesNeedingRunningStyleInference skips active queued state", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 10]]),
    new Map([
      [
        "nar:20260519:46:12",
        {
          attemptedAt: new Date().toISOString(),
          cellModelKey: null,
          cellVariantId: null,
          completedAt: null,
          expectedHorseCount: null,
          featuresR2Key: null,
          modelVersion: null,
          raceKey: "nar:20260519:46:12",
          status: "pending",
          writtenHorseCount: null,
        },
      ],
    ]),
  );

  expect(selected.needed).toHaveLength(0);
  expect(selected.alreadyQueued).toBe(1);
});

test("selectRacesNeedingRunningStyleInference counts featureReady=0 as missingFeatures", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map(),
    new Map(),
    new Map(),
    new Map(),
  );
  expect(selected.missingFeatures).toBe(1);
  expect(selected.featureReady).toBe(0);
});

test("selectRacesNeedingRunningStyleInference treats stale active state as needing rerun", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 0]]),
    new Map([
      [
        "nar:20260519:46:12",
        {
          attemptedAt: "2026-05-19T00:00:00.000Z",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: null,
          expectedHorseCount: null,
          featuresR2Key: null,
          modelVersion: null,
          raceKey: "nar:20260519:46:12",
          status: "processing",
          writtenHorseCount: null,
        },
      ],
    ]),
    new Date("2026-05-19T01:00:00.000Z"),
  );
  expect(selected.needed).toHaveLength(1);
});

test("selectRacesNeedingRunningStyleInference treats state with attemptedAt=null as active and skips it", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 0]]),
    new Map([
      [
        "nar:20260519:46:12",
        {
          attemptedAt: null,
          cellModelKey: null,
          cellVariantId: null,
          completedAt: null,
          expectedHorseCount: null,
          featuresR2Key: null,
          modelVersion: null,
          raceKey: "nar:20260519:46:12",
          status: "pending",
          writtenHorseCount: null,
        },
      ],
    ]),
  );
  expect(selected.alreadyQueued).toBe(1);
  expect(selected.needed).toHaveLength(0);
});

test("selectRacesNeedingRunningStyleInference treats malformed attemptedAt as active", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 14]]),
    new Map([["nar:20260519:46:12", 0]]),
    new Map([
      [
        "nar:20260519:46:12",
        {
          attemptedAt: "not-a-date",
          cellModelKey: null,
          cellVariantId: null,
          completedAt: null,
          expectedHorseCount: null,
          featuresR2Key: null,
          modelVersion: null,
          raceKey: "nar:20260519:46:12",
          status: "processing",
          writtenHorseCount: null,
        },
      ],
    ]),
  );
  expect(selected.needed.length).toBe(1);
});

test("resolveRunningStyleCronDates returns only today outside the sweep window", () => {
  // 2026-06-04T06:00:00Z = 2026-06-04T15:00:00 JST (hour 15, outside 0-6)
  expect(resolveRunningStyleCronDates(new Date("2026-06-04T06:00:00Z"))).toStrictEqual([
    "20260604",
  ]);
});

test("resolveRunningStyleCronDates sweeps yesterday when JST hour is 0-5", () => {
  // 2026-06-03T16:30:00Z = 2026-06-04T01:30:00 JST (hour 1, inside 0-6 window)
  expect(resolveRunningStyleCronDates(new Date("2026-06-03T16:30:00Z"))).toStrictEqual([
    "20260603",
    "20260604",
  ]);
});

test("resolveRunningStyleCronDates sweeps yesterday at the boundary JST hour 5:59", () => {
  // 2026-06-03T20:59:00Z = 2026-06-04T05:59:00 JST (hour 5, inside window)
  expect(resolveRunningStyleCronDates(new Date("2026-06-03T20:59:00Z"))).toStrictEqual([
    "20260603",
    "20260604",
  ]);
});

test("resolveRunningStyleCronDates exits sweep window at JST hour 6", () => {
  // 2026-06-03T21:00:00Z = 2026-06-04T06:00:00 JST (hour 6, OUTSIDE window)
  expect(resolveRunningStyleCronDates(new Date("2026-06-03T21:00:00Z"))).toStrictEqual([
    "20260604",
  ]);
});

test("runRunningStyleCronTick scans yesterday + today inside the sweep window", async () => {
  const send = vi.fn();
  const env = {
    REALTIME_DB: buildDbWithRegisteredRaces([RACE]),
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
    RUNNING_STYLE_D1_WRITE_ENABLED: "0",
  } as unknown as Env;
  // JST 2026-06-04T01:30:00 → sweep window
  const result = await runRunningStyleCronTick(env, new Date("2026-06-03T16:30:00Z"));
  expect(result.scanned).toBe(2);
  expect(result.cacheRefresh?.date).toBe("20260604");
});

test("runRunningStyleCronTick scans only today outside the sweep window", async () => {
  const send = vi.fn();
  const env = {
    REALTIME_DB: buildDbWithRegisteredRaces([RACE]),
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
    RUNNING_STYLE_D1_WRITE_ENABLED: "0",
  } as unknown as Env;
  // JST 2026-06-04T15:00:00 → outside window
  const result = await runRunningStyleCronTick(env, new Date("2026-06-04T06:00:00Z"));
  expect(result.scanned).toBe(1);
  expect(result.cacheRefresh?.date).toBe("20260604");
});

test("runRunningStyleCronTick captures planError when planRunningStylePredictionsForDate throws", async () => {
  const env = {
    REALTIME_DB: {
      prepare: vi.fn(() => {
        throw new Error("d1 boom");
      }),
    },
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
  } as unknown as Env;
  const result = await runRunningStyleCronTick(env, new Date("2026-06-04T06:00:00Z"));
  expect(result.planError).toBe("d1 boom");
  expect(result.cacheRefresh?.refreshError).toBe("d1 boom");
});

test("runRunningStyleCronTick aggregates planError + refreshError across two dates", async () => {
  const env = {
    REALTIME_DB: {
      prepare: vi.fn(() => {
        throw new Error("multi-day boom");
      }),
    },
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
  } as unknown as Env;
  // sweep window: 2 dates processed, both throw — merge path exercised
  const result = await runRunningStyleCronTick(env, new Date("2026-06-03T16:30:00Z"));
  expect(result.planError).toBe("multi-day boom");
  expect(result.cacheRefresh?.refreshError).toBe("multi-day boom");
});

test("runRunningStyleCronTick records parquetExport summary when FEATURES_ARCHIVE is bound", async () => {
  const send = vi.fn();
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  // D1.prepare returns a chain where bind().all() yields empty results so the
  // export quickly returns the "no rows for day" skipped summary.
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ all, bind }));
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
    RUNNING_STYLE_D1_WRITE_ENABLED: "1",
  } as unknown as Env;
  const result = await runRunningStyleCronTick(env, new Date("2026-06-04T06:00:00Z"));
  expect(result.parquetExport?.fileCount).toBe(0);
  expect(result.parquetExport?.rowCount).toBe(0);
  expect(result.parquetExport?.bytesWritten).toBe(0);
});

test("runRunningStyleCronTick omits parquetExport when inference flag disabled", async () => {
  const send = vi.fn();
  const put = vi.fn(async (_key: string, _body: Uint8Array, _options?: unknown) => undefined);
  const all = vi.fn(async () => ({ results: [] }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ all, bind }));
  const env = {
    FEATURES_ARCHIVE: { put } as unknown as R2Bucket,
    REALTIME_DB: { prepare } as unknown as D1Database,
    REALTIME_JOBS: { send, sendBatch: vi.fn() },
    RUNNING_STYLE_D1_WRITE_ENABLED: "0",
  } as unknown as Env;
  const result = await runRunningStyleCronTick(env, new Date("2026-06-04T06:00:00Z"));
  expect(result.parquetExport).toBeUndefined();
  expect(put).not.toHaveBeenCalled();
});
