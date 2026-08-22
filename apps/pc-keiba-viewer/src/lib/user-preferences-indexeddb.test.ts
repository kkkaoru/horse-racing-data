// bun で実行する (bunx vitest)
import "fake-indexeddb/auto";
import { beforeEach, expect, it, vi } from "vitest";

import { saveFavorites } from "./favorites-indexeddb";
import { getOrCreateUserId, setUserId } from "./user-identity-indexeddb";
import {
  getHeatmapShowStarts,
  loadHeatmapShowStartsForCurrentUser,
  persistHeatmapShowStartsForCurrentUser,
  setHeatmapShowStarts,
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
});

it("returns fallbacks when window is undefined", async () => {
  const target = globalThis as Record<string, unknown>;
  const originalWindow = target.window;
  target.window = undefined;
  try {
    expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
    await setHeatmapShowStarts("viewer-a", true);
    expect(await getHeatmapShowStarts("viewer-a")).toBe(false);
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
