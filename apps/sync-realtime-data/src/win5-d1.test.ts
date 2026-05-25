// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  getWin5Prediction,
  getWin5Schedule,
  listWin5SchedulesByYear,
  markWin5InferenceState,
  parseWin5PredictionRow,
  parseWin5ScheduleRow,
  serializeWin5Schedule,
  upsertWin5Prediction,
  upsertWin5Schedule,
} from "./win5-d1";
import type { Win5PredictionPayload, Win5Schedule } from "../../pc-keiba-viewer/src/lib/win5/types";

const SCHEDULE: Win5Schedule = {
  fetchedAt: "2026-05-10T09:00:00+09:00",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0511",
  legs: [
    {
      kaisaiKai: "02",
      kaisaiNichime: "06",
      keibajoCode: "05",
      legIndex: 1,
      raceBango: "09",
    },
  ],
  saleDeadline: "2026-05-11T14:00:00+09:00",
  source: "jra_web",
};

const PREDICTION: Win5PredictionPayload = {
  defaultBudgetYen: 2000,
  kaisaiNen: "2026",
  kaisaiTsukihi: "0511",
  legs: [],
  modelVersion: "win5-xgb-v7-lineage-v1",
  plans: {},
  predictedAt: "2026-05-11T11:55:00+09:00",
  recommendedBudgetYen: 3000,
};

it("serializeWin5Schedule produces JSON containing schedule fields", () => {
  expect(serializeWin5Schedule(SCHEDULE)).toBe(JSON.stringify(SCHEDULE));
});

it("parseWin5ScheduleRow rebuilds schedule when source is jvd_wf", () => {
  const result = parseWin5ScheduleRow({
    fetched_at: "2026-05-10T09:00:00+09:00",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    legs_json: JSON.stringify({ legs: SCHEDULE.legs }),
    sale_deadline: "2026-05-11T14:00:00+09:00",
    source: "jvd_wf",
  });
  expect(result).toStrictEqual({
    fetchedAt: "2026-05-10T09:00:00+09:00",
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    legs: SCHEDULE.legs,
    saleDeadline: "2026-05-11T14:00:00+09:00",
    source: "jvd_wf",
  });
});

it("parseWin5ScheduleRow defaults non-jvd_wf source to jra_web", () => {
  const result = parseWin5ScheduleRow({
    fetched_at: "2026-05-10T09:00:00+09:00",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    legs_json: JSON.stringify({ legs: SCHEDULE.legs }),
    sale_deadline: null,
    source: "some_other",
  });
  expect(result.source).toBe("jra_web");
  expect(result.saleDeadline).toBeNull();
});

it("parseWin5PredictionRow parses the prediction JSON column", () => {
  const result = parseWin5PredictionRow({
    default_budget_yen: 2000,
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    model_version: "win5-xgb-v7-lineage-v1",
    predicted_at: "2026-05-11T11:55:00+09:00",
    prediction_json: JSON.stringify(PREDICTION),
    recommended_budget_yen: 3000,
  });
  expect(result).toStrictEqual(PREDICTION);
});

it("upsertWin5Schedule binds normalized fields and runs the insert", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertWin5Schedule(db, SCHEDULE);
  expect(prepare).toHaveBeenCalledTimes(1);
  expect(bind.mock.calls[0]).toStrictEqual([
    "2026",
    "0511",
    "2026-05-11T14:00:00+09:00",
    "jra_web",
    JSON.stringify(SCHEDULE),
    "2026-05-10T09:00:00+09:00",
  ]);
});

it("upsertWin5Schedule passes null when saleDeadline is missing", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn((..._args: unknown[]) => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertWin5Schedule(db, { ...SCHEDULE, saleDeadline: undefined });
  expect(bind.mock.calls[0]![2]).toBeNull();
});

it("upsertWin5Prediction binds prediction fields and runs the insert", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await upsertWin5Prediction(db, PREDICTION);
  expect(bind.mock.calls[0]).toStrictEqual([
    "2026",
    "0511",
    "win5-xgb-v7-lineage-v1",
    3000,
    2000,
    JSON.stringify(PREDICTION),
    "2026-05-11T11:55:00+09:00",
  ]);
});

it("getWin5Schedule returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getWin5Schedule(db, "2026", "0511");
  expect(result).toBeNull();
});

it("getWin5Schedule parses the row when present", async () => {
  const first = vi.fn(async () => ({
    fetched_at: "2026-05-10T09:00:00+09:00",
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    legs_json: JSON.stringify({ legs: SCHEDULE.legs }),
    sale_deadline: "2026-05-11T14:00:00+09:00",
    source: "jra_web",
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getWin5Schedule(db, "2026", "0511");
  expect(result).toStrictEqual(SCHEDULE);
});

it("getWin5Prediction returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getWin5Prediction(db, "2026", "0511", "win5-xgb-v7-lineage-v1");
  expect(result).toBeNull();
});

it("getWin5Prediction parses the row when present", async () => {
  const first = vi.fn(async () => ({
    default_budget_yen: 2000,
    kaisai_nen: "2026",
    kaisai_tsukihi: "0511",
    model_version: "win5-xgb-v7-lineage-v1",
    predicted_at: "2026-05-11T11:55:00+09:00",
    prediction_json: JSON.stringify(PREDICTION),
    recommended_budget_yen: 3000,
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getWin5Prediction(db, "2026", "0511", "win5-xgb-v7-lineage-v1");
  expect(result).toStrictEqual(PREDICTION);
});

it("listWin5SchedulesByYear returns mapped schedules", async () => {
  const all = vi.fn(async () => ({
    results: [
      {
        fetched_at: "2026-05-10T09:00:00+09:00",
        kaisai_nen: "2026",
        kaisai_tsukihi: "0511",
        legs_json: JSON.stringify({ legs: SCHEDULE.legs }),
        sale_deadline: null,
        source: "jra_web",
      },
    ],
  }));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listWin5SchedulesByYear(db, "2026");
  expect(result).toStrictEqual([{ ...SCHEDULE, saleDeadline: null }]);
});

it("listWin5SchedulesByYear handles missing results array", async () => {
  const all = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ all }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await listWin5SchedulesByYear(db, "2026");
  expect(result).toStrictEqual([]);
});

it("markWin5InferenceState binds completed status with attemptIncrement=false", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await markWin5InferenceState(db, {
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    status: "completed",
    updatedAt: "2026-05-11T12:00:00+09:00",
  });
  expect(bind.mock.calls[0]).toStrictEqual([
    "2026",
    "0511",
    "completed",
    0,
    null,
    "2026-05-11T12:00:00+09:00",
  ]);
});

it("markWin5InferenceState binds processing status with increment=true and lastError", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  await markWin5InferenceState(db, {
    incrementAttempt: true,
    kaisaiNen: "2026",
    kaisaiTsukihi: "0511",
    lastError: "boom",
    status: "processing",
    updatedAt: "2026-05-11T12:01:00+09:00",
  });
  expect(bind.mock.calls[0]).toStrictEqual([
    "2026",
    "0511",
    "processing",
    1,
    "boom",
    "2026-05-11T12:01:00+09:00",
  ]);
});
