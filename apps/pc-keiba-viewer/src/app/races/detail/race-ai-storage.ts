"use client";

const DB_NAME = "pc-keiba-race-ai";
const DB_VERSION = 1;
const LOG_STORE = "raceLogs";
const MODEL_STORE = "modelCache";

export type RaceAiMessageRole = "assistant" | "user";

export interface RaceAiMessage {
  content: string;
  createdAt: string;
  id: string;
  role: RaceAiMessageRole;
}

export interface RaceAiThoughtLog {
  content: string;
  createdAt: string;
  dataFingerprint: string;
  id: string;
  modelVersion: string;
  trigger: string;
}

interface StoredRaceAiLog {
  messages: RaceAiMessage[];
  raceKey: string;
  thoughtLogs: RaceAiThoughtLog[];
  updatedAt: string;
}

interface StoredModel {
  blob: Blob;
  cachedAt: string;
  key: string;
  modelVersion: string;
  sourceUrl: string;
}

const openRaceAiDb = (): Promise<IDBDatabase> =>
  new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("upgradeneeded", () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(LOG_STORE)) {
        db.createObjectStore(LOG_STORE, { keyPath: "raceKey" });
      }
      if (!db.objectStoreNames.contains(MODEL_STORE)) {
        db.createObjectStore(MODEL_STORE, { keyPath: "key" });
      }
    });
    request.addEventListener("success", () => {
      resolve(request.result);
    });
  });

const withStore = async <T>(
  storeName: string,
  mode: IDBTransactionMode,
  callback: (store: IDBObjectStore) => IDBRequest<T> | void,
): Promise<T | undefined> => {
  const db = await openRaceAiDb();
  return new Promise((resolve, reject) => {
    const transaction = db.transaction(storeName, mode);
    const store = transaction.objectStore(storeName);
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

const emptyLog = (raceKey: string): StoredRaceAiLog => ({
  messages: [],
  raceKey,
  thoughtLogs: [],
  updatedAt: new Date().toISOString(),
});

export const getRaceAiLog = async (raceKey: string): Promise<StoredRaceAiLog> =>
  (await withStore<StoredRaceAiLog>(LOG_STORE, "readonly", (store) => store.get(raceKey))) ??
  emptyLog(raceKey);

export const saveRaceAiLog = async (log: StoredRaceAiLog): Promise<void> => {
  await withStore(LOG_STORE, "readwrite", (store) => {
    store.put({
      ...log,
      updatedAt: new Date().toISOString(),
    } satisfies StoredRaceAiLog);
  });
};

export const loadCachedModel = async (key: string): Promise<ArrayBuffer | null> => {
  const row = await withStore<StoredModel>(MODEL_STORE, "readonly", (store) => store.get(key));
  return row ? row.blob.arrayBuffer() : null;
};

export const saveCachedModel = async ({
  buffer,
  key,
  modelVersion,
  sourceUrl,
}: {
  buffer: ArrayBuffer;
  key: string;
  modelVersion: string;
  sourceUrl: string;
}): Promise<void> => {
  await withStore(MODEL_STORE, "readwrite", (store) => {
    store.put({
      blob: new Blob([buffer], { type: "application/octet-stream" }),
      cachedAt: new Date().toISOString(),
      key,
      modelVersion,
      sourceUrl,
    } satisfies StoredModel);
  });
};
