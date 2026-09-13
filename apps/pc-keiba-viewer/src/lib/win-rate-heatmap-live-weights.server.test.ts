// Run with bun. Warm-only weight loading shares the realtime source policy.
import { beforeEach, expect, it, vi } from "vitest";

import { fetchHeatmapLiveWeights } from "./win-rate-heatmap-live-weights.server";
const mocks = vi.hoisted(() => ({
  env: vi.fn<() => Promise<unknown>>(),
  latest: vi.fn<() => Promise<unknown>>(),
  resolve: vi.fn<() => Promise<unknown>>(),
}));
vi.mock("./cloudflare-context.server", () => ({ safeGetCloudflareEnv: mocks.env }));
vi.mock("./realtime-payload.server", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./realtime-payload.server")>()),
  fetchHorseWeightsLatest: mocks.latest,
  resolveHorseWeights: mocks.resolve,
}));
beforeEach(() => {
  vi.resetAllMocks();
  mocks.latest.mockResolvedValue(null);
  mocks.resolve.mockResolvedValue(null);
});
it("loads only live weights and strips unrelated snapshot fields", async () => {
  const realtimeData = { fetch: vi.fn<typeof fetch>() };
  const db = {};
  const snapshot = {
    fetchedAt: "2026-09-13T09:10:03+09:00",
    horses: [
      { horseNumber: "01", weight: 488, horseName: "horse", changeAmount: 2, changeSign: "+" },
    ],
  };
  mocks.env.mockResolvedValue({ REALTIME_DATA: realtimeData, REALTIME_DB: db });
  mocks.latest.mockResolvedValue(snapshot);
  mocks.resolve.mockResolvedValue(snapshot);
  expect(
    await fetchHeatmapLiveWeights({
      day: "13",
      month: "09",
      year: "2026",
      keibajoCode: "06",
      raceNumber: "01",
      source: "jra",
    }),
  ).toStrictEqual([{ horseNumber: "01", weight: 488 }]);
  expect(mocks.resolve).toHaveBeenCalledWith({
    db,
    fromDO: snapshot,
    raceKey: "jra:2026:0913:06:01",
  });
  expect(mocks.latest).toHaveBeenCalledOnce();
});
it("uses D1 fallback when the live binding is absent", async () => {
  mocks.env.mockResolvedValue({ REALTIME_DB: {} });
  mocks.resolve.mockResolvedValue({ horses: [{ horseNumber: "1", weight: 510 }] });
  expect(
    await fetchHeatmapLiveWeights({
      day: "13",
      month: "09",
      year: "2026",
      keibajoCode: "36",
      raceNumber: "1",
      source: "nar",
    }),
  ).toStrictEqual([{ horseNumber: "1", weight: 510 }]);
  expect(mocks.latest).not.toHaveBeenCalled();
  expect(mocks.resolve).toHaveBeenCalledWith({
    db: {},
    fromDO: null,
    raceKey: "nar:2026:0913:36:01",
  });
});
it("retains the realtime route's unavailable snapshot policy", async () => {
  mocks.env.mockResolvedValue(null);
  expect(
    await fetchHeatmapLiveWeights({
      day: "13",
      month: "09",
      year: "2026",
      keibajoCode: "06",
      raceNumber: "01",
      source: "jra",
    }),
  ).toStrictEqual([]);
  expect(mocks.latest).not.toHaveBeenCalled();
});
it("falls back after the DO reader reports an unavailable snapshot", async () => {
  mocks.env.mockResolvedValue({ REALTIME_DATA: { fetch: vi.fn<typeof fetch>() }, REALTIME_DB: {} });
  mocks.resolve.mockResolvedValue({ horses: [{ horseNumber: "1", weight: null }] });
  expect(
    await fetchHeatmapLiveWeights({
      day: "13",
      month: "09",
      year: "2026",
      keibajoCode: "06",
      raceNumber: "01",
      source: "jra",
    }),
  ).toStrictEqual([{ horseNumber: "1", weight: null }]);
  expect(mocks.resolve).toHaveBeenCalledWith({
    db: {},
    fromDO: null,
    raceKey: "jra:2026:0913:06:01",
  });
});
