// Run with: bun run --filter pipeline-health-monitor test
import { expect, it, vi } from "vitest";

import { getFailureCount, incrementFailureCounter, resetFailureCounter } from "./alert-state";
import type { Env } from "./types";

interface KvMocks {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
  delete: ReturnType<typeof vi.fn>;
}

const buildKvMocks = (initial: string | null = null): KvMocks => ({
  get: vi.fn(async () => initial),
  put: vi.fn(async () => undefined),
  delete: vi.fn(async () => undefined),
});

const buildEnv = (mocks: KvMocks): Env =>
  ({
    STATE_KV: {
      get: mocks.get,
      put: mocks.put,
      delete: mocks.delete,
    },
  }) as unknown as Env;

it("getFailureCount returns zero when KV has no value", async () => {
  const mocks = buildKvMocks(null);
  const env = buildEnv(mocks);
  const count = await getFailureCount(env, "check-a");
  expect(count).toBe(0);
  expect(mocks.get).toHaveBeenCalledWith("failures:check-a");
});

it("getFailureCount returns the parsed integer when KV has a numeric string", async () => {
  const env = buildEnv(buildKvMocks("7"));
  const count = await getFailureCount(env, "check-a");
  expect(count).toBe(7);
});

it("getFailureCount returns zero when KV has a non-numeric string", async () => {
  const env = buildEnv(buildKvMocks("not-a-number"));
  const count = await getFailureCount(env, "check-a");
  expect(count).toBe(0);
});

it("incrementFailureCounter increments from zero to one and writes with a 4h TTL", async () => {
  const mocks = buildKvMocks(null);
  const env = buildEnv(mocks);
  const next = await incrementFailureCounter(env, "check-b");
  expect(next).toBe(1);
  expect(mocks.put).toHaveBeenCalledWith("failures:check-b", "1", { expirationTtl: 14400 });
});

it("incrementFailureCounter increments from three to four", async () => {
  const mocks = buildKvMocks("3");
  const env = buildEnv(mocks);
  const next = await incrementFailureCounter(env, "check-b");
  expect(next).toBe(4);
  expect(mocks.put).toHaveBeenCalledWith("failures:check-b", "4", { expirationTtl: 14400 });
});

it("resetFailureCounter calls KV delete with the prefixed key", async () => {
  const mocks = buildKvMocks();
  const env = buildEnv(mocks);
  await resetFailureCounter(env, "check-c");
  expect(mocks.delete).toHaveBeenCalledWith("failures:check-c");
});
