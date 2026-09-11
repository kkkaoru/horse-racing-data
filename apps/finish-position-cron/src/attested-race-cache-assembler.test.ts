// Run with bun. Tests for fail-closed Worker assembly of per-race feature caches.

import { beforeEach, expect, test, vi } from "vitest";
import type { Env } from "./types";

const { materializeMock, readinessMock, snapshotsMock } = vi.hoisted(() => ({
  materializeMock: vi.fn(async () => ({
    featureHash: "feature-hash",
    manifestKey: "manifest-key",
    raceCount: 2,
    rowCount: 24,
    status: "materialized",
  })),
  readinessMock: vi.fn(async () => ({ ready: true, reason: "ready" })),
  snapshotsMock: vi.fn(async () => new Map()),
}));

vi.mock("./day-base-race-materializer", () => ({
  materializeDayBasePerRaceCache: materializeMock,
}));

vi.mock("./focused-full-day-base-readiness", () => ({
  getFocusedFullDayBaseReadiness: readinessMock,
}));

vi.mock("./race-source-snapshot", () => ({
  fetchCatalogRaceSourceSnapshots: snapshotsMock,
}));

import { assembleAttestedRaceCaches } from "./attested-race-cache-assembler";

const env = {
  PC_KEIBA_R2_CATALOG: { fetch: vi.fn() },
} as unknown as Env;

beforeEach(() => {
  materializeMock.mockClear();
  materializeMock.mockResolvedValue({
    featureHash: "feature-hash",
    manifestKey: "manifest-key",
    raceCount: 2,
    rowCount: 24,
    status: "materialized",
  });
  readinessMock.mockReset();
  readinessMock.mockResolvedValue({ ready: true, reason: "ready" });
  snapshotsMock.mockReset();
  snapshotsMock.mockResolvedValue(new Map());
});

test("assembles attested race caches between two freshness checks", async () => {
  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, force: true, runYmd: "20260910" }),
  ).resolves.toStrictEqual({
    featureHash: "feature-hash",
    manifestKey: "manifest-key",
    raceCount: 2,
    rowCount: 24,
    status: "materialized",
  });
  expect(readinessMock).toHaveBeenCalledTimes(2);
  expect(snapshotsMock).toHaveBeenCalledWith({
    catalog: env.PC_KEIBA_R2_CATALOG,
    category: "nar",
    runYmd: "20260910",
  });
  expect(materializeMock).toHaveBeenCalledWith({
    category: "nar",
    env,
    force: true,
    runYmd: "20260910",
    sourceSnapshots: expect.any(Map),
  });
});

test("omits an undefined force option", async () => {
  await assembleAttestedRaceCaches({ category: "jra", env, runYmd: "20260910" });

  expect(materializeMock).toHaveBeenCalledWith({
    category: "jra",
    env,
    runYmd: "20260910",
    sourceSnapshots: expect.any(Map),
  });
});

test("rejects an unavailable Catalog binding", async () => {
  await expect(
    assembleAttestedRaceCaches({
      category: "nar",
      env: {} as unknown as Env,
      runYmd: "20260910",
    }),
  ).resolves.toStrictEqual({ reason: "catalog-binding-unavailable", status: "fallback" });
  expect(readinessMock).not.toHaveBeenCalled();
});

test.each([
  "rs-predicted-at-max-mismatch",
  "rs-row-count-454-of-455",
  "running-style-race-count-46-of-47",
])("assembles when only category-level running-style metadata is stale: %s", async (reason) => {
  readinessMock.mockResolvedValue({ ready: false, reason });

  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, runYmd: "20260910" }),
  ).resolves.toMatchObject({ status: "materialized" });
  expect(readinessMock).toHaveBeenCalledTimes(2);
  expect(snapshotsMock).toHaveBeenCalledTimes(1);
  expect(materializeMock).toHaveBeenCalledTimes(1);
});

test("rejects a stale preaggregate before loading source rows", async () => {
  readinessMock.mockResolvedValueOnce({ ready: false, reason: "source-watermark-mismatch" });

  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, runYmd: "20260910" }),
  ).resolves.toStrictEqual({
    reason: "preaggregate-not-ready:source-watermark-mismatch",
    status: "fallback",
  });
  expect(snapshotsMock).not.toHaveBeenCalled();
  expect(materializeMock).not.toHaveBeenCalled();
});

test("rejects a source change during assembly", async () => {
  readinessMock
    .mockResolvedValueOnce({ ready: true, reason: "ready" })
    .mockResolvedValueOnce({ ready: false, reason: "source-row-count-20-of-21" });

  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, runYmd: "20260910" }),
  ).resolves.toStrictEqual({
    reason: "source-changed-during-assembly:source-row-count-20-of-21",
    status: "fallback",
  });
  expect(materializeMock).not.toHaveBeenCalled();
});

test("returns an opaque source loading failure as a stable fallback", async () => {
  snapshotsMock.mockRejectedValueOnce("unavailable");

  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, runYmd: "20260910" }),
  ).resolves.toStrictEqual({ reason: "race-cache-assembly-failed", status: "fallback" });
  expect(materializeMock).not.toHaveBeenCalled();
});

test("returns bounded source loading failures as fallback", async () => {
  snapshotsMock.mockRejectedValueOnce(new Error("catalog unavailable"));

  await expect(
    assembleAttestedRaceCaches({ category: "nar", env, runYmd: "20260910" }),
  ).resolves.toStrictEqual({ reason: "catalog unavailable", status: "fallback" });
  expect(materializeMock).not.toHaveBeenCalled();
});
