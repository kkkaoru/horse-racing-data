"use client";

// Run with: bunx vitest run src/lib/user-identity-indexeddb.test.ts

const DB_NAME = "pc-keiba-viewer";
const DB_VERSION = 2;
const FAVORITES_STORE = "favorites";
const USER_IDENTITY_STORE = "userIdentity";
const USER_IDENTITY_KEY = "singleton";

export interface StoredUserIdentity {
  userId: string;
  createdAt: string;
  updatedAt: string;
}

const isBrowser = (): boolean => typeof window !== "undefined" && typeof indexedDB !== "undefined";

const ensureStores = (db: IDBDatabase): void => {
  if (!db.objectStoreNames.contains(FAVORITES_STORE)) {
    db.createObjectStore(FAVORITES_STORE, { keyPath: "key" });
  }
  if (!db.objectStoreNames.contains(USER_IDENTITY_STORE)) {
    db.createObjectStore(USER_IDENTITY_STORE, { keyPath: "key" });
  }
};

const openUserIdentityDb = (): Promise<IDBDatabase> =>
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
  const db = await openUserIdentityDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(USER_IDENTITY_STORE, mode);
    const store = transaction.objectStore(USER_IDENTITY_STORE);
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

interface UserIdentityRow extends StoredUserIdentity {
  key: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isUserIdentityRow = (value: unknown): value is UserIdentityRow =>
  isRecord(value) &&
  typeof value.userId === "string" &&
  typeof value.createdAt === "string" &&
  typeof value.updatedAt === "string";

const readRow = async (): Promise<UserIdentityRow | null> => {
  const row = await withStore<unknown>("readonly", (store) => store.get(USER_IDENTITY_KEY));
  return isUserIdentityRow(row) ? row : null;
};

const writeRow = async (row: UserIdentityRow): Promise<void> => {
  await withStore("readwrite", (store) => {
    store.put(row);
  });
};

export const getUserId = async (): Promise<string | null> => {
  if (!isBrowser()) {
    return null;
  }
  const row = await readRow();
  return row ? row.userId : null;
};

export const getOrCreateUserId = async (): Promise<string> => {
  if (!isBrowser()) {
    return "";
  }
  const existing = await readRow();
  if (existing) {
    return existing.userId;
  }
  const timestamp = new Date().toISOString();
  const newUserId = crypto.randomUUID();
  await writeRow({
    key: USER_IDENTITY_KEY,
    userId: newUserId,
    createdAt: timestamp,
    updatedAt: timestamp,
  });
  return newUserId;
};

export const setUserId = async (id: string): Promise<void> => {
  if (!isBrowser()) {
    return;
  }
  const existing = await readRow();
  const timestamp = new Date().toISOString();
  await writeRow({
    key: USER_IDENTITY_KEY,
    userId: id,
    createdAt: existing ? existing.createdAt : timestamp,
    updatedAt: timestamp,
  });
};
