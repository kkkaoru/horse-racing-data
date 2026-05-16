import "fake-indexeddb/auto";
import { beforeEach, describe, expect, it } from "vitest";

import { getFavorites, isFavorite, saveFavorites, toggleFavorite } from "./favorites-indexeddb";

const resetIndexedDb = async (): Promise<void> => {
  await new Promise<void>((resolve, reject) => {
    const request = indexedDB.deleteDatabase("pc-keiba-viewer");
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
};

describe("favorites indexeddb", () => {
  beforeEach(async () => {
    await resetIndexedDb();
  });

  it("returns an empty list when nothing has been saved", async () => {
    expect(await getFavorites()).toEqual([]);
  });

  it("saves and retrieves favorites", async () => {
    await saveFavorites([
      { id: "h1", kind: "horse", label: "alpha" },
      { id: "h2", kind: "horse", label: "beta" },
    ]);
    expect(await getFavorites()).toEqual([
      { id: "h1", kind: "horse", label: "alpha" },
      { id: "h2", kind: "horse", label: "beta" },
    ]);
  });

  it("clears previous favorites on save", async () => {
    await saveFavorites([{ id: "h1", kind: "horse", label: "alpha" }]);
    await saveFavorites([{ id: "h2", kind: "horse", label: "beta" }]);
    expect(await getFavorites()).toEqual([{ id: "h2", kind: "horse", label: "beta" }]);
  });

  it("reports favorite membership", async () => {
    await saveFavorites([{ id: "h1", kind: "horse", label: "alpha" }]);
    expect(await isFavorite({ id: "h1", kind: "horse", label: "alpha" })).toBe(true);
    expect(await isFavorite({ id: "h2", kind: "horse", label: "beta" })).toBe(false);
  });

  it("adds an entry through toggleFavorite", async () => {
    const added = await toggleFavorite({ id: "h1", kind: "horse", label: "alpha" });
    expect(added).toBe(true);
    expect(await isFavorite({ id: "h1", kind: "horse", label: "alpha" })).toBe(true);
  });

  it("removes an entry through toggleFavorite", async () => {
    await saveFavorites([{ id: "h1", kind: "horse", label: "alpha" }]);
    const stillFavorite = await toggleFavorite({ id: "h1", kind: "horse", label: "alpha" });
    expect(stillFavorite).toBe(false);
    expect(await isFavorite({ id: "h1", kind: "horse", label: "alpha" })).toBe(false);
  });
});
