// Run with bun test apps/sync-realtime-data/src/running-style-cron.test.ts
import { expect, test, vi } from "vitest";

import { RUNNING_STYLE_INFERENCE_CRON, runRunningStyleCronTick } from "./running-style-cron";
import type { Env } from "./types";

const buildEnabledEnv = (): Env => ({
  REALTIME_DB: { batch: vi.fn(async () => []) } as unknown as D1Database,
  RUNNING_STYLE_D1_WRITE_ENABLED: "1",
  RUNNING_STYLE_MODELS: {
    get: vi.fn(async () => null),
    head: vi.fn(async () => null),
  } as unknown as R2Bucket,
} as unknown as Env);

const buildDisabledEnv = (): Env => ({
  REALTIME_DB: { batch: vi.fn(async () => []) } as unknown as D1Database,
  RUNNING_STYLE_D1_WRITE_ENABLED: "0",
  RUNNING_STYLE_MODELS: {
    get: vi.fn(async () => null),
    head: vi.fn(async () => null),
  } as unknown as R2Bucket,
} as unknown as Env);

test("runRunningStyleCronTick marks both sources skipped when flag is 0", async () => {
  const result = await runRunningStyleCronTick(buildDisabledEnv(), new Date("2026-05-18T10:00:00Z"));
  expect(result.every((entry) => entry.status === "skipped")).toBe(true);
});

test("runRunningStyleCronTick reports two source entries", async () => {
  const result = await runRunningStyleCronTick(buildDisabledEnv(), new Date("2026-05-18T10:00:00Z"));
  expect(result.length).toBe(2);
});

test("runRunningStyleCronTick reports failed status when R2 lookup throws", async () => {
  const result = await runRunningStyleCronTick(buildEnabledEnv(), new Date("2026-05-18T10:00:00Z"));
  expect(result.every((entry) => entry.status === "failed")).toBe(true);
});

test("RUNNING_STYLE_INFERENCE_CRON is */10 schedule", () => {
  expect(RUNNING_STYLE_INFERENCE_CRON).toBe("*/10 * * * *");
});
