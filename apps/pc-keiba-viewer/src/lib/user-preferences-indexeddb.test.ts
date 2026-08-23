// bun で実行する (bunx vitest)
import "fake-indexeddb/auto";
import { beforeEach, expect, it, vi } from "vitest";

import { saveFavorites } from "./favorites-indexeddb";
import { getOrCreateUserId, setUserId } from "./user-identity-indexeddb";
import {
  getConditionFinishChart,
  getConditionPayoutChart,
  getHeatmapShowStarts,
  getHeatmapSplitBloodlineLines,
  getResultsChart,
  getTrainingChart,
  getTrainingScatterAllWorkouts,
  loadConditionFinishChartForCurrentUser,
  loadConditionPayoutChartForCurrentUser,
  loadHeatmapShowStartsForCurrentUser,
  loadHeatmapSplitBloodlineLinesForCurrentUser,
  loadResultsChartForCurrentUser,
  loadTrainingChartForCurrentUser,
  loadTrainingScatterAllWorkoutsForCurrentUser,
  persistConditionFinishChartForCurrentUser,
  persistConditionPayoutChartForCurrentUser,
  persistHeatmapShowStartsForCurrentUser,
  persistHeatmapSplitBloodlineLinesForCurrentUser,
  persistResultsChartForCurrentUser,
  persistTrainingChartForCurrentUser,
  persistTrainingScatterAllWorkoutsForCurrentUser,
  setConditionFinishChart,
  setConditionPayoutChart,
  setHeatmapShowStarts,
  setHeatmapSplitBloodlineLines,
  setResultsChart,
  setTrainingChart,
  setTrainingScatterAllWorkouts,
} from "./user-preferences-indexeddb";

const DB_NAME = "pc-keiba-viewer";

const resetIndexedDb = (): Promise<void> =>
  new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase(DB_NAME);
    request.addEventListener("success", () => {
      resolve();
    });
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("blocked", () => {
      reject(new Error("indexedDB deleteDatabase blocked"));
    });
  });

beforeEach(async () => {
  await resetIndexedDb();
  vi.restoreAllMocks();
});

it("returns the default off state when a user has no heatmap preference", async () => {
  expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
});

it("stores heatmap race-count visibility for one user", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(true);
});

it("keeps heatmap race-count preferences isolated per user", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  await setHeatmapShowStarts("viewer-b", false);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(true);
  expect(await getHeatmapShowStarts("viewer-b")).toBe(false);
});

it("overwrites the last heatmap race-count choice for the same user", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  await setHeatmapShowStarts("viewer-a", false);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
});

it("ignores a corrupt heatmap preference row", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  const dbRequest = indexedDB.open(DB_NAME, 3);
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    dbRequest.addEventListener("success", () => {
      resolve(dbRequest.result);
    });
    dbRequest.addEventListener("error", () => {
      reject(dbRequest.error);
    });
  });
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction("userPreferences", "readwrite");
    tx.objectStore("userPreferences").put({
      showHeatmapStarts: "yes",
      updatedAt: "2026-08-22T00:00:00.000Z",
      userId: "viewer-a",
    });
    tx.addEventListener("complete", () => {
      resolve();
    });
    tx.addEventListener("error", () => {
      reject(tx.error);
    });
  });
  db.close();
  expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
});

it("loads and persists the current user heatmap preference", async () => {
  await setUserId("current-viewer");
  expect(await loadHeatmapShowStartsForCurrentUser()).toBe(false);
  await persistHeatmapShowStartsForCurrentUser(true);
  expect(await loadHeatmapShowStartsForCurrentUser()).toBe(true);
  expect(await getHeatmapShowStarts("current-viewer")).toBe(true);
});

it("creates a user when persisting heatmap preference without an existing identity", async () => {
  const randomSpy = vi.spyOn(crypto, "randomUUID");
  randomSpy.mockReturnValue("33333333-3333-4333-8333-333333333333");
  await persistHeatmapShowStartsForCurrentUser(true);
  expect(await getOrCreateUserId()).toBe("33333333-3333-4333-8333-333333333333");
  expect(await getHeatmapShowStarts("33333333-3333-4333-8333-333333333333")).toBe(true);
});

it("returns the default off state for an empty user id", async () => {
  expect(await getHeatmapShowStarts("")).toBe(false);
  await setHeatmapShowStarts("", true);
  expect(await getHeatmapShowStarts("")).toBe(false);
  expect(await getHeatmapSplitBloodlineLines("")).toBe(true);
  await setHeatmapSplitBloodlineLines("", false);
  expect(await getHeatmapSplitBloodlineLines("")).toBe(true);
  expect(await getConditionPayoutChart("")).toBe(true);
  await setConditionPayoutChart("", false);
  expect(await getConditionPayoutChart("")).toBe(true);
  expect(await getConditionFinishChart("")).toBe(true);
  await setConditionFinishChart("", false);
  expect(await getConditionFinishChart("")).toBe(true);
  expect(await getTrainingChart("")).toBe(true);
  await setTrainingChart("", false);
  expect(await getTrainingChart("")).toBe(true);
  expect(await getTrainingScatterAllWorkouts("")).toBe(false);
  await setTrainingScatterAllWorkouts("", true);
  expect(await getTrainingScatterAllWorkouts("")).toBe(false);
  expect(await getResultsChart("")).toBe(true);
  await setResultsChart("", false);
  expect(await getResultsChart("")).toBe(true);
});

it("stores condition analysis chart visibility independently of heatmap flags", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  expect(await getConditionPayoutChart("viewer-a")).toBe(true);
  expect(await getConditionFinishChart("viewer-a")).toBe(true);
  await setConditionPayoutChart("viewer-a", false);
  await setConditionFinishChart("viewer-a", false);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(true);
  expect(await getConditionPayoutChart("viewer-a")).toBe(false);
  expect(await getConditionFinishChart("viewer-a")).toBe(false);
  await setHeatmapShowStarts("viewer-a", false);
  expect(await getConditionPayoutChart("viewer-a")).toBe(false);
  expect(await getConditionFinishChart("viewer-a")).toBe(false);
});

it("treats a missing condition chart flag as chart-on", async () => {
  await setHeatmapShowStarts("viewer-legacy-chart", true);
  expect(await getConditionPayoutChart("viewer-legacy-chart")).toBe(true);
  expect(await getConditionFinishChart("viewer-legacy-chart")).toBe(true);
});

it("loads and persists the current user condition chart preferences", async () => {
  await setUserId("current-viewer-charts");
  expect(await loadConditionPayoutChartForCurrentUser()).toBe(true);
  expect(await loadConditionFinishChartForCurrentUser()).toBe(true);
  await persistConditionPayoutChartForCurrentUser(false);
  await persistConditionFinishChartForCurrentUser(false);
  expect(await loadConditionPayoutChartForCurrentUser()).toBe(false);
  expect(await loadConditionFinishChartForCurrentUser()).toBe(false);
});

it("stores bloodline split visibility independently from race-count visibility", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  expect(await getHeatmapSplitBloodlineLines("viewer-a")).toBe(true);
  await setHeatmapSplitBloodlineLines("viewer-a", false);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(true);
  expect(await getHeatmapSplitBloodlineLines("viewer-a")).toBe(false);
  await setHeatmapShowStarts("viewer-a", false);
  expect(await getHeatmapSplitBloodlineLines("viewer-a")).toBe(false);
});

it("treats a missing bloodline split flag as split-on", async () => {
  await setHeatmapShowStarts("viewer-legacy", true);
  const dbRequest = indexedDB.open(DB_NAME, 3);
  const db = await new Promise<IDBDatabase>((resolve, reject) => {
    dbRequest.addEventListener("success", () => {
      resolve(dbRequest.result);
    });
    dbRequest.addEventListener("error", () => {
      reject(dbRequest.error);
    });
  });
  await new Promise<void>((resolve, reject) => {
    const tx = db.transaction("userPreferences", "readwrite");
    tx.objectStore("userPreferences").put({
      showHeatmapStarts: true,
      updatedAt: "2026-08-22T00:00:00.000Z",
      userId: "viewer-legacy",
    });
    tx.addEventListener("complete", () => {
      resolve();
    });
    tx.addEventListener("error", () => {
      reject(tx.error);
    });
  });
  db.close();
  expect(await getHeatmapShowStarts("viewer-legacy")).toBe(true);
  expect(await getHeatmapSplitBloodlineLines("viewer-legacy")).toBe(true);
});

it("stores training chart visibility independently of heatmap and condition flags", async () => {
  await setHeatmapShowStarts("viewer-a", true);
  await setConditionPayoutChart("viewer-a", false);
  expect(await getTrainingChart("viewer-a")).toBe(true);
  await setTrainingChart("viewer-a", false);
  expect(await getHeatmapShowStarts("viewer-a")).toBe(true);
  expect(await getConditionPayoutChart("viewer-a")).toBe(false);
  expect(await getTrainingChart("viewer-a")).toBe(false);
  await setTrainingChart("viewer-a", true);
  expect(await getTrainingChart("viewer-a")).toBe(true);
  await setTrainingChart("viewer-a", false);
  expect(await getTrainingChart("viewer-a")).toBe(false);
});

it("treats a missing training chart flag as chart-on", async () => {
  await setHeatmapShowStarts("viewer-legacy-training", true);
  expect(await getTrainingChart("viewer-legacy-training")).toBe(true);
});

it("loads and persists the current user training chart preference", async () => {
  await setUserId("current-viewer-training");
  expect(await loadTrainingChartForCurrentUser()).toBe(true);
  await persistTrainingChartForCurrentUser(false);
  expect(await loadTrainingChartForCurrentUser()).toBe(false);
  expect(await getTrainingChart("current-viewer-training")).toBe(false);
});

it("stores all-workout scatter independently of training chart visibility", async () => {
  await setTrainingChart("viewer-scatter", false);
  expect(await getTrainingScatterAllWorkouts("viewer-scatter")).toBe(false);
  await setTrainingScatterAllWorkouts("viewer-scatter", true);
  expect(await getTrainingChart("viewer-scatter")).toBe(false);
  expect(await getTrainingScatterAllWorkouts("viewer-scatter")).toBe(true);
  await setTrainingScatterAllWorkouts("viewer-scatter", false);
  expect(await getTrainingScatterAllWorkouts("viewer-scatter")).toBe(false);
});

it("treats a missing all-workout scatter flag as latest-only", async () => {
  await setTrainingChart("viewer-legacy-scatter", false);
  expect(await getTrainingScatterAllWorkouts("viewer-legacy-scatter")).toBe(false);
});

it("stores results chart visibility independently of training chart flags", async () => {
  await setTrainingChart("viewer-results", false);
  expect(await getResultsChart("viewer-results")).toBe(true);
  await setResultsChart("viewer-results", false);
  expect(await getTrainingChart("viewer-results")).toBe(false);
  expect(await getResultsChart("viewer-results")).toBe(false);
  await setResultsChart("viewer-results", true);
  expect(await getResultsChart("viewer-results")).toBe(true);
});

it("treats a missing results chart flag as chart-on", async () => {
  await setTrainingChart("viewer-legacy-results", false);
  expect(await getResultsChart("viewer-legacy-results")).toBe(true);
});

it("loads and persists the current user results chart preference", async () => {
  await setUserId("current-viewer-results");
  expect(await loadResultsChartForCurrentUser()).toBe(true);
  await persistResultsChartForCurrentUser(false);
  expect(await loadResultsChartForCurrentUser()).toBe(false);
  expect(await getResultsChart("current-viewer-results")).toBe(false);
});

it("loads and persists the current user all-workout scatter preference", async () => {
  await setUserId("current-viewer-scatter");
  expect(await loadTrainingScatterAllWorkoutsForCurrentUser()).toBe(false);
  await persistTrainingScatterAllWorkoutsForCurrentUser(true);
  expect(await loadTrainingScatterAllWorkoutsForCurrentUser()).toBe(true);
  expect(await getTrainingScatterAllWorkouts("current-viewer-scatter")).toBe(true);
});

it("loads and persists the current user bloodline split preference", async () => {
  await setUserId("current-viewer-split");
  expect(await loadHeatmapSplitBloodlineLinesForCurrentUser()).toBe(true);
  await persistHeatmapSplitBloodlineLinesForCurrentUser(false);
  expect(await loadHeatmapSplitBloodlineLinesForCurrentUser()).toBe(false);
  expect(await getHeatmapSplitBloodlineLines("current-viewer-split")).toBe(false);
});

it("returns fallbacks when window is undefined", async () => {
  const target = globalThis as Record<string, unknown>;
  const originalWindow = target.window;
  target.window = undefined;
  try {
    expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
    await setHeatmapShowStarts("viewer-a", true);
    expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
    expect(await getHeatmapSplitBloodlineLines("viewer-a")).toBe(true);
    expect(await getConditionPayoutChart("viewer-a")).toBe(true);
    expect(await getConditionFinishChart("viewer-a")).toBe(true);
    expect(await getTrainingChart("viewer-a")).toBe(true);
    expect(await getTrainingScatterAllWorkouts("viewer-a")).toBe(false);
    expect(await getResultsChart("viewer-a")).toBe(true);
    await persistHeatmapShowStartsForCurrentUser(true);
    expect(await loadHeatmapShowStartsForCurrentUser()).toBe(false);
  } finally {
    target.window = originalWindow;
  }
});

it("keeps favorites and identity working after the preferences store is added", async () => {
  await saveFavorites([{ id: "h1", kind: "horse", label: "alpha" }]);
  await setUserId("viewer-after-preferences");
  await persistHeatmapShowStartsForCurrentUser(true);
  expect(await getHeatmapShowStarts("viewer-after-preferences")).toBe(true);
});

it("creates the preferences store when upgrading from a v2 database", async () => {
  await new Promise<void>((resolve, reject) => {
    const v2Request = indexedDB.open(DB_NAME, 2);
    v2Request.addEventListener("upgradeneeded", () => {
      v2Request.result.createObjectStore("favorites", { keyPath: "key" });
      v2Request.result.createObjectStore("userIdentity", { keyPath: "key" });
    });
    v2Request.addEventListener("success", () => {
      v2Request.result.close();
      resolve();
    });
    v2Request.addEventListener("error", () => {
      reject(v2Request.error);
    });
  });
  await setHeatmapShowStarts("migrated-viewer", true);
  expect(await getHeatmapShowStarts("migrated-viewer")).toBe(true);
});
