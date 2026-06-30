// Run with bun test apps/sync-realtime-data/test/running-style-cron.test.ts
import { expect, test } from "vitest";

import {
  selectRacesNeedingRunningStyleInference,
  type RegisteredRaceRow,
} from "../src/running-style-cron";
import type { RunningStyleInferenceStateDetail } from "../src/running-style-d1";

const RACE: RegisteredRaceRow = {
  kaisai_nen: "2026",
  kaisai_tsukihi: "0525",
  keibajo_code: "46",
  race_bango: "12",
  source: "nar",
};

const RACE_KEY = "nar:20260525:46:12";

const buildState = (
  overrides: Partial<RunningStyleInferenceStateDetail> = {},
): RunningStyleInferenceStateDetail => ({
  attemptedAt: new Date().toISOString(),
  cellModelKey: null,
  cellVariantId: null,
  completedAt: null,
  expectedHorseCount: null,
  featuresR2Key: null,
  modelVersion: null,
  raceKey: RACE_KEY,
  status: "pending",
  writtenHorseCount: null,
  ...overrides,
});

test("enqueues a race when prediction counts are below the expected horse count", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 10]]),
    new Map(),
  );

  expect(selected.needed).toHaveLength(1);
  expect(selected.completed).toBe(0);
  expect(selected.alreadyQueued).toBe(0);
  expect(selected.featureReady).toBe(1);
  expect(selected.missingFeatures).toBe(0);
  expect(selected.needed[0]?.expectedHorseCount).toBe(14);
  expect(selected.needed[0]?.existingHorseCount).toBe(10);
});

test("marks a race completed when prediction count reaches the expected horse count", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 12]]),
    new Map([[RACE_KEY, 12]]),
    new Map(),
  );

  expect(selected.needed).toHaveLength(0);
  expect(selected.completed).toBe(1);
});

test("treats inference state status=completed with full horse count as completed even when expectedHorseCount is unknown", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map(),
    new Map(),
    new Map([[RACE_KEY, 12]]),
    new Map([
      [
        RACE_KEY,
        buildState({ expectedHorseCount: 12, status: "completed", writtenHorseCount: 12 }),
      ],
    ]),
  );

  expect(selected.completed).toBe(1);
  expect(selected.needed).toHaveLength(0);
});

test("counts a race as alreadyQueued when its state is active (pending) without prediction coverage", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 10]]),
    new Map([[RACE_KEY, buildState({ status: "pending" })]]),
  );

  expect(selected.alreadyQueued).toBe(1);
  expect(selected.needed).toHaveLength(0);
});

test("enqueues a race even when feature count is zero (planner no longer gates on race_entry_corner_features)", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map(),
    new Map(),
    new Map(),
    new Map(),
  );

  expect(selected.featureReady).toBe(0);
  expect(selected.missingFeatures).toBe(1);
  expect(selected.needed).toHaveLength(1);
  expect(selected.needed[0]?.raceKey).toBe(RACE_KEY);
});

test("classifies feature-ready vs missing without skipping any registered race", () => {
  const otherRace: RegisteredRaceRow = {
    kaisai_nen: "2026",
    kaisai_tsukihi: "0525",
    keibajo_code: "83",
    race_bango: "01",
    source: "nar",
  };
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE, otherRace],
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 14]]),
    new Map(),
    new Map(),
  );

  expect(selected.featureReady).toBe(1);
  expect(selected.missingFeatures).toBe(1);
  expect(selected.needed).toHaveLength(2);
});

test("does not mark a race completed when only existing predictions exist but state lacks completion details", () => {
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map(),
    new Map(),
    new Map([[RACE_KEY, 5]]),
    new Map(),
  );

  expect(selected.completed).toBe(0);
  expect(selected.needed).toHaveLength(1);
});

test("treats stale active state (attemptedAt older than 5 minutes) as not active", () => {
  const oldAttempt = new Date(Date.now() - 6 * 60 * 1000).toISOString();
  const selected = selectRacesNeedingRunningStyleInference(
    [RACE],
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 14]]),
    new Map([[RACE_KEY, 10]]),
    new Map([[RACE_KEY, buildState({ attemptedAt: oldAttempt, status: "pending" })]]),
  );

  expect(selected.alreadyQueued).toBe(0);
  expect(selected.needed).toHaveLength(1);
});
