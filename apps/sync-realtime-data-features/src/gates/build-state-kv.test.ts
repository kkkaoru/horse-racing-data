// Run with: bun run --filter sync-realtime-data-features test
import { expect, it, vi } from "vitest";

import {
  getBuildStateFromKv,
  isBuildStateFresh,
  putBuildStateToKv,
  shouldSkipBuild,
} from "./build-state-kv";
import type { Env } from "../types";

interface MockKv {
  get: ReturnType<typeof vi.fn>;
  put: ReturnType<typeof vi.fn>;
}

const buildEnv = (): { env: Env; kv: MockKv } => {
  const kv: MockKv = { get: vi.fn(), put: vi.fn() };
  return {
    env: { FEATURES_KV: kv as unknown as KVNamespace } as unknown as Env,
    kv,
  };
};

it("returns null when KV miss", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(null);
  await expect(getBuildStateFromKv(env, "r")).resolves.toBeNull();
});

it("returns parsed record when KV hit", async () => {
  const { env, kv } = buildEnv();
  kv.get.mockResolvedValueOnce(
    JSON.stringify({ lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 14 }),
  );
  await expect(getBuildStateFromKv(env, "r")).resolves.toStrictEqual({
    lastBuiltAt: "2026-05-29T00:00:00Z",
    rowCount: 14,
  });
});

it("writes record with TTL", async () => {
  const { env, kv } = buildEnv();
  await putBuildStateToKv(env, "r", { lastBuiltAt: "now", rowCount: 1 });
  expect(kv.put).toHaveBeenCalledWith(
    "features:build-state:r",
    '{"lastBuiltAt":"now","rowCount":1}',
    { expirationTtl: 86_400 },
  );
});

it("does not skip when state is null", () => {
  expect(
    shouldSkipBuild({
      freshnessThresholdMs: 60_000,
      now: new Date("2026-05-29T00:00:00Z"),
      state: null,
    }),
  ).toBe(false);
});

it("does not skip when lastBuiltAt is unparseable", () => {
  expect(
    shouldSkipBuild({
      freshnessThresholdMs: 60_000,
      now: new Date("2026-05-29T00:00:00Z"),
      state: { lastBuiltAt: "not-a-date", rowCount: 1 },
    }),
  ).toBe(false);
});

it("skips when state is fresh", () => {
  expect(
    shouldSkipBuild({
      freshnessThresholdMs: 60_000,
      now: new Date("2026-05-29T00:00:30Z"),
      state: { lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 1 },
    }),
  ).toBe(true);
});

it("does not skip when state is older than threshold", () => {
  expect(
    shouldSkipBuild({
      freshnessThresholdMs: 60_000,
      now: new Date("2026-05-29T00:02:00Z"),
      state: { lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 1 },
    }),
  ).toBe(false);
});

it("isBuildStateFresh returns false when state is null", () => {
  expect(isBuildStateFresh(null, 60_000, new Date("2026-05-29T00:00:00Z"))).toBe(false);
});

it("isBuildStateFresh returns false when rowCount is zero", () => {
  expect(
    isBuildStateFresh(
      { lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 0 },
      60_000,
      new Date("2026-05-29T00:00:30Z"),
    ),
  ).toBe(false);
});

it("isBuildStateFresh returns false when lastBuiltAt is unparseable", () => {
  expect(
    isBuildStateFresh(
      { lastBuiltAt: "not-a-date", rowCount: 5 },
      60_000,
      new Date("2026-05-29T00:00:30Z"),
    ),
  ).toBe(false);
});

it("isBuildStateFresh returns true when within freshness window", () => {
  expect(
    isBuildStateFresh(
      { lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 5 },
      60_000,
      new Date("2026-05-29T00:00:30Z"),
    ),
  ).toBe(true);
});

it("isBuildStateFresh returns false when older than freshness window", () => {
  expect(
    isBuildStateFresh(
      { lastBuiltAt: "2026-05-29T00:00:00Z", rowCount: 5 },
      60_000,
      new Date("2026-05-29T00:02:00Z"),
    ),
  ).toBe(false);
});
