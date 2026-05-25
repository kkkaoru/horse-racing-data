// run with: bun run test
import { expect, it, vi } from "vitest";
import {
  getFinishPositionInferenceState,
  markFinishPositionFeaturesCached,
} from "./finish-position-d1";

const RACE = {
  kaisaiNen: "2026",
  kaisaiTsukihi: "0512",
  keibajoCode: "08",
  raceBango: "01",
  raceKey: "jra:2026:0512:08:01",
  source: "jra",
} as const;

it("markFinishPositionFeaturesCached binds race + inputs and runs the insert", async () => {
  const run = vi.fn(async () => ({}));
  const bind = vi.fn(() => ({ run }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;

  await markFinishPositionFeaturesCached(db, RACE, {
    attemptedAt: "2026-05-12T11:30:00+09:00",
    completedAt: "2026-05-12T11:31:00+09:00",
    featuresR2Key: "running-style/jra/2026/0512/jra:2026:0512:08:01.parquet",
    modelVersion: "v7-lineage",
  });

  expect(prepare).toHaveBeenCalledTimes(1);
  expect(bind).toHaveBeenCalledTimes(1);
  expect(bind.mock.calls[0]).toStrictEqual([
    "jra:2026:0512:08:01",
    "jra",
    "2026",
    "0512",
    "08",
    "01",
    "completed",
    "running-style/jra/2026/0512/jra:2026:0512:08:01.parquet",
    "v7-lineage",
    "2026-05-12T11:30:00+09:00",
    "2026-05-12T11:31:00+09:00",
    null,
  ]);
  expect(run).toHaveBeenCalledTimes(1);
});

it("getFinishPositionInferenceState returns null when no row found", async () => {
  const first = vi.fn(async () => null);
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getFinishPositionInferenceState(db, "jra:2026:0512:08:01");
  expect(result).toBeNull();
});

it("getFinishPositionInferenceState maps row columns to camelCase fields", async () => {
  const first = vi.fn(async () => ({
    attempted_at: "2026-05-12T11:30:00+09:00",
    completed_at: "2026-05-12T11:31:00+09:00",
    model_version: "v7-lineage",
    predictions_r2_key: "key1",
    race_key: "jra:2026:0512:08:01",
    status: "completed",
  }));
  const bind = vi.fn(() => ({ first }));
  const prepare = vi.fn(() => ({ bind }));
  const db = { prepare } as unknown as D1Database;
  const result = await getFinishPositionInferenceState(db, "jra:2026:0512:08:01");
  expect(result).toStrictEqual({
    attemptedAt: "2026-05-12T11:30:00+09:00",
    completedAt: "2026-05-12T11:31:00+09:00",
    featuresR2Key: "key1",
    modelVersion: "v7-lineage",
    raceKey: "jra:2026:0512:08:01",
    status: "completed",
  });
});
