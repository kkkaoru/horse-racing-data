"use client";

// bun で実行する (bunx oxlint / bunx oxfmt / bunx vitest 経由)

import { getOrCreateUserId } from "./user-identity-indexeddb";
import { DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS } from "./win-rate-heatmap";

const DB_NAME = "pc-keiba-viewer";
const DB_VERSION = 3;
const FAVORITES_STORE = "favorites";
const USER_IDENTITY_STORE = "userIdentity";
const USER_PREFERENCES_STORE = "userPreferences";

interface UserPreferencesRow {
  showHeatmapStarts: boolean;
  updatedAt: string;
  userId: string;
}

const isBrowser = (): boolean => typeof window !== "undefined" && typeof indexedDB !== "undefined";

const ensureStores = (db: IDBDatabase): void => {
  if (!db.objectStoreNames.contains(FAVORITES_STORE)) {
    db.createObjectStore(FAVORITES_STORE, { keyPath: "key" });
  }
  if (!db.objectStoreNames.contains(USER_IDENTITY_STORE)) {
    db.createObjectStore(USER_IDENTITY_STORE, { keyPath: "key" });
  }
  if (!db.objectStoreNames.contains(USER_PREFERENCES_STORE)) {
    db.createObjectStore(USER_PREFERENCES_STORE, { keyPath: "userId" });
  }
};

const openPreferencesDb = (): Promise<IDBDatabase> =>
  new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("upgradeneeded", () => {
      ensureStores(request.result);
    });
    request.addEventListener("success", () => {
      resolve(request.result);
    });
  });

const withStore = async <T>(
  mode: IDBTransactionMode,
  callback: (store: IDBObjectStore) => IDBRequest<T> | void,
): Promise<T | undefined> => {
  const db = await openPreferencesDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(USER_PREFERENCES_STORE, mode);
    const store = transaction.objectStore(USER_PREFERENCES_STORE);
    const request = callback(store);
    const holder: { result: T | undefined } = { result: undefined };
    if (request) {
      request.addEventListener("success", () => {
        holder.result = request.result;
      });
      request.addEventListener("error", () => {
        reject(request.error);
      });
    }
    transaction.addEventListener("complete", () => {
      db.close();
      resolve(holder.result);
    });
    transaction.addEventListener("error", () => {
      db.close();
      reject(transaction.error);
    });
  });
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isUserPreferencesRow = (value: unknown): value is UserPreferencesRow =>
  isRecord(value) &&
  typeof value.userId === "string" &&
  typeof value.updatedAt === "string" &&
  typeof value.showHeatmapStarts === "boolean";

export const getHeatmapShowStarts = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return isUserPreferencesRow(row) ? row.showHeatmapStarts : DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS;
};

export const setHeatmapShowStarts = async (userId: string, showStarts: boolean): Promise<void> => {
  if (!isBrowser() || userId.length === 0) {
    return;
  }
  await withStore("readwrite", (store) => {
    store.put({
      showHeatmapStarts: showStarts,
      updatedAt: new Date().toISOString(),
      userId,
    });
  });
};

export const loadHeatmapShowStartsForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getHeatmapShowStarts(userId);
};

export const persistHeatmapShowStartsForCurrentUser = async (
  showStarts: boolean,
): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setHeatmapShowStarts(userId, showStarts);
};
