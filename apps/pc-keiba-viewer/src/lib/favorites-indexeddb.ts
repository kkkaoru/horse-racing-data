"use client";

import { dedupeFavorites, favoriteKey, type FavoriteItem } from "./favorites";

const DB_NAME = "pc-keiba-viewer";
const DB_VERSION = 1;
const STORE_NAME = "favorites";

const openFavoritesDb = (): Promise<IDBDatabase> =>
  new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("upgradeneeded", () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(STORE_NAME)) {
        db.createObjectStore(STORE_NAME, { keyPath: "key" });
      }
    });
    request.addEventListener("success", () => {
      resolve(request.result);
    });
  });

const withStore = async <T>(
  mode: IDBTransactionMode,
  callback: (store: IDBObjectStore) => IDBRequest<T> | void,
): Promise<T | undefined> => {
  const db = await openFavoritesDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(STORE_NAME, mode);
    const store = transaction.objectStore(STORE_NAME);
    const request = callback(store);
    let result: T | undefined;
    if (request) {
      request.addEventListener("success", () => {
        result = request.result;
      });
      request.addEventListener("error", () => {
        reject(request.error);
      });
    }
    transaction.addEventListener("complete", () => {
      db.close();
      resolve(result);
    });
    transaction.addEventListener("error", () => {
      db.close();
      reject(transaction.error);
    });
  });
};

interface StoredFavorite extends FavoriteItem {
  key: string;
  updatedAt: string;
}

export const getFavorites = async (): Promise<FavoriteItem[]> => {
  const rows = (await withStore<StoredFavorite[]>("readonly", (store) => store.getAll())) ?? [];
  return dedupeFavorites(rows.map(({ id, kind, label }) => ({ id, kind, label })));
};

export const saveFavorites = async (items: FavoriteItem[]): Promise<void> => {
  await withStore("readwrite", (store) => {
    store.clear();
    for (const item of dedupeFavorites(items)) {
      store.put({
        ...item,
        key: favoriteKey(item),
        updatedAt: new Date().toISOString(),
      } satisfies StoredFavorite);
    }
  });
};

export const isFavorite = async (item: FavoriteItem): Promise<boolean> => {
  const existing = await withStore<StoredFavorite>("readonly", (store) =>
    store.get(favoriteKey(item)),
  );
  return Boolean(existing);
};

export const toggleFavorite = async (item: FavoriteItem): Promise<boolean> => {
  const key = favoriteKey(item);
  const exists = await isFavorite(item);
  await withStore("readwrite", (store) => {
    if (exists) {
      store.delete(key);
      return;
    }
    store.put({
      ...item,
      key,
      updatedAt: new Date().toISOString(),
    } satisfies StoredFavorite);
  });
  return !exists;
};
