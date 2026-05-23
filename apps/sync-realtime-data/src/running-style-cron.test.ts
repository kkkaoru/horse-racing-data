// Run with bun test apps/sync-realtime-data/src/running-style-cron.test.ts
import { expect, test, vi } from "vitest";

import {
  RUNNING_STYLE_INFERENCE_CRON,
  RUNNING_STYLE_PREWARM_CRON,
  addDaysToYYYYMMDDInJst,
  formatYYYYMMDDInJst,
  formatTomorrowYYYYMMDDInJst,
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

test("RUNNING_STYLE_INFERENCE_CRON is */10 schedule", () => {
  expect(RUNNING_STYLE_INFERENCE_CRON).toBe("*/10 * * * *");
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
          raceKey: "nar:20260519:46:12",
          status: "pending",
        },
      ],
    ]),
  );

  expect(selected.needed).toHaveLength(0);
  expect(selected.alreadyQueued).toBe(1);
});
