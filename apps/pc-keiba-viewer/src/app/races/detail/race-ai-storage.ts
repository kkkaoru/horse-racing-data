"use client";

const DB_NAME = "pc-keiba-race-ai";
const DB_VERSION = 3;
const LOG_STORE = "raceLogs";
const MODEL_PART_DATA_STORE = "modelPartDataCache";
const MODEL_PART_STORE = "modelPartCache";
const MODEL_STORE = "modelCache";
const SETTINGS_STORAGE_KEY = "pc-keiba-race-ai-settings-v1";
const SETTINGS_EVENT_NAME = "pc-keiba-race-ai-settings";

export type RaceAiConsent = "denied" | "granted" | "unanswered";

export type RaceAiMessageRole = "assistant" | "system" | "user";

export interface RaceAiSettings {
  autoStart: boolean;
  consent: RaceAiConsent;
  showSystemMessages: boolean;
  updatedAt: string;
}

export interface RaceAiMessage {
  content: string;
  createdAt: string;
  id: string;
  prediction?: {
    confidence: number | null;
    horseName: string;
    horseNumber: string;
    jockeyName: string;
    rank: number;
    reason: string;
  }[];
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
  size?: number;
  sourceUrl: string;
}

interface StoredModelPart {
  cachedAt: string;
  end: number;
  key: string;
  modelCacheKey: string;
  modelVersion: string;
  partIndex: number;
  size?: number;
  sourceUrl: string;
  start: number;
  totalBytes: number;
}

interface StoredModelPartData {
  data: ArrayBuffer;
  key: string;
}

export interface RaceAiCachedModelInfo {
  cachedAt: string;
  key: string;
  modelVersion: string;
  size: number | null;
  sourceUrl: string;
}

export interface RaceAiCachedModelPartInfo {
  cachedAt: string;
  end: number;
  key: string;
  modelCacheKey: string;
  modelVersion: string;
  partIndex: number;
  size: number | null;
  sourceUrl: string;
  start: number;
  totalBytes: number;
}

export const RACE_AI_USAGE_CONFIRM_MESSAGE =
  "このブラウザでアーモンドAI予想を利用しますか？\n\nはいを選ぶとAI利用を許可し、AI利用の自動開始をオンにします。必要なAIモデルはブラウザ内に保存されます。";

export const RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE =
  "AIモデルをこのブラウザにダウンロードしますか？\n\nモデルは大きいため、通信量と保存容量を使用します。";

const emptySettings = (): RaceAiSettings => ({
  autoStart: false,
  consent: "unanswered",
  showSystemMessages: false,
  updatedAt: new Date().toISOString(),
});

const openRaceAiDb = (): Promise<IDBDatabase> =>
  new Promise((resolve, reject) => {
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.addEventListener("error", () => {
      reject(request.error);
    });
    request.addEventListener("upgradeneeded", (event) => {
      const db = request.result;
      if (event.oldVersion < 3) {
        if (db.objectStoreNames.contains(MODEL_STORE)) {
          db.deleteObjectStore(MODEL_STORE);
        }
        if (db.objectStoreNames.contains(MODEL_PART_STORE)) {
          db.deleteObjectStore(MODEL_PART_STORE);
        }
        if (db.objectStoreNames.contains(MODEL_PART_DATA_STORE)) {
          db.deleteObjectStore(MODEL_PART_DATA_STORE);
        }
      }
      if (!db.objectStoreNames.contains(LOG_STORE)) {
        db.createObjectStore(LOG_STORE, { keyPath: "raceKey" });
      }
      if (!db.objectStoreNames.contains(MODEL_STORE)) {
        db.createObjectStore(MODEL_STORE, { keyPath: "key" });
      }
      if (!db.objectStoreNames.contains(MODEL_PART_STORE)) {
        db.createObjectStore(MODEL_PART_STORE, { keyPath: "key" });
      }
      if (!db.objectStoreNames.contains(MODEL_PART_DATA_STORE)) {
        db.createObjectStore(MODEL_PART_DATA_STORE, { keyPath: "key" });
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

export const deleteRaceAiLog = async (raceKey: string): Promise<void> => {
  await withStore(LOG_STORE, "readwrite", (store) => {
    store.delete(raceKey);
  });
};

export const getRaceAiSettings = (): RaceAiSettings => {
  if (typeof window === "undefined") {
    return emptySettings();
  }
  const raw = window.localStorage.getItem(SETTINGS_STORAGE_KEY);
  if (!raw) {
    return emptySettings();
  }
  try {
    const parsed: unknown = JSON.parse(raw);
    if (typeof parsed !== "object" || parsed === null) {
      return emptySettings();
    }
    const consent = Reflect.get(parsed, "consent");
    const autoStart = Reflect.get(parsed, "autoStart");
    const showSystemMessages = Reflect.get(parsed, "showSystemMessages");
    const updatedAt = Reflect.get(parsed, "updatedAt");
    if (consent !== "granted" && consent !== "denied" && consent !== "unanswered") {
      return emptySettings();
    }
    return {
      autoStart: typeof autoStart === "boolean" ? autoStart : false,
      consent,
      showSystemMessages: typeof showSystemMessages === "boolean" ? showSystemMessages : false,
      updatedAt: typeof updatedAt === "string" ? updatedAt : new Date().toISOString(),
    };
  } catch {
    return emptySettings();
  }
};

export const saveRaceAiSettings = (settings: Omit<RaceAiSettings, "updatedAt">): RaceAiSettings => {
  const nextSettings: RaceAiSettings = {
    ...settings,
    updatedAt: new Date().toISOString(),
  };
  window.localStorage.setItem(SETTINGS_STORAGE_KEY, JSON.stringify(nextSettings));
  window.dispatchEvent(
    new CustomEvent<RaceAiSettings>(SETTINGS_EVENT_NAME, { detail: nextSettings }),
  );
  return nextSettings;
};

export const subscribeRaceAiSettings = (listener: (settings: RaceAiSettings) => void) => {
  const handleStorage = (event: StorageEvent) => {
    if (event.key === SETTINGS_STORAGE_KEY) {
      listener(getRaceAiSettings());
    }
  };
  const handleCustom = (event: Event) => {
    listener(event instanceof CustomEvent ? event.detail : getRaceAiSettings());
  };
  window.addEventListener("storage", handleStorage);
  window.addEventListener(SETTINGS_EVENT_NAME, handleCustom);
  return () => {
    window.removeEventListener("storage", handleStorage);
    window.removeEventListener(SETTINGS_EVENT_NAME, handleCustom);
  };
};

export const requestRaceAiConsent = (): RaceAiSettings => {
  const allowed = window.confirm(RACE_AI_USAGE_CONFIRM_MESSAGE);
  return saveRaceAiSettings({
    autoStart: allowed,
    consent: allowed ? "granted" : "denied",
    showSystemMessages: false,
  });
};

export const loadCachedModel = async (key: string): Promise<ArrayBuffer | null> => {
  const row = await withStore<StoredModel>(MODEL_STORE, "readonly", (store) => store.get(key));
  return row ? row.blob.arrayBuffer() : null;
};

const storedBlobSize = (row: { blob: Blob; size?: number }): number | null => {
  if (typeof row.size === "number" && Number.isFinite(row.size) && row.size > 0) {
    return row.size;
  }
  return row.blob.size || null;
};

const storedModelPartSize = (row: StoredModelPart): number | null => {
  if (typeof row.size === "number" && Number.isFinite(row.size) && row.size > 0) {
    return row.size;
  }
  return null;
};

const toArrayBufferCopy = (buffer: Uint8Array): ArrayBuffer => {
  const copy = new ArrayBuffer(buffer.byteLength);
  new Uint8Array(copy).set(buffer);
  return copy;
};

export const getCachedModelInfo = async (key: string): Promise<RaceAiCachedModelInfo | null> => {
  const row = await withStore<StoredModel>(MODEL_STORE, "readonly", (store) => store.get(key));
  return row
    ? {
        cachedAt: row.cachedAt,
        key: row.key,
        modelVersion: row.modelVersion,
        size: storedBlobSize(row),
        sourceUrl: row.sourceUrl,
      }
    : null;
};

export const listCachedModelInfos = async (): Promise<RaceAiCachedModelInfo[]> => {
  const rows =
    (await withStore<StoredModel[]>(MODEL_STORE, "readonly", (store) => store.getAll())) ?? [];
  return rows.map((row) => ({
    cachedAt: row.cachedAt,
    key: row.key,
    modelVersion: row.modelVersion,
    size: storedBlobSize(row),
    sourceUrl: row.sourceUrl,
  }));
};

export const modelPartCacheKey = ({
  end,
  modelCacheKey,
  partIndex,
  start,
}: {
  end: number;
  modelCacheKey: string;
  partIndex: number;
  start: number;
}): string => `${modelCacheKey}:part:${partIndex}:${start}-${end}`;

const modelPartInfo = (row: StoredModelPart): RaceAiCachedModelPartInfo => ({
  cachedAt: row.cachedAt,
  end: row.end,
  key: row.key,
  modelCacheKey: row.modelCacheKey,
  modelVersion: row.modelVersion,
  partIndex: row.partIndex,
  size: storedModelPartSize(row),
  sourceUrl: row.sourceUrl,
  start: row.start,
  totalBytes: row.totalBytes,
});

export const listCachedModelPartInfos = async (
  modelCacheKey: string,
): Promise<RaceAiCachedModelPartInfo[]> => {
  const rows =
    (await withStore<StoredModelPart[]>(MODEL_PART_STORE, "readonly", (store) => store.getAll())) ??
    [];
  return rows
    .filter((row) => row.modelCacheKey === modelCacheKey)
    .map(modelPartInfo)
    .toSorted((left, right) => left.partIndex - right.partIndex);
};

export const listAllCachedModelPartInfos = async (): Promise<RaceAiCachedModelPartInfo[]> => {
  const rows =
    (await withStore<StoredModelPart[]>(MODEL_PART_STORE, "readonly", (store) => store.getAll())) ??
    [];
  return rows.map(modelPartInfo).toSorted((left, right) => {
    const cacheKeyOrder = left.modelCacheKey.localeCompare(right.modelCacheKey);
    return cacheKeyOrder === 0 ? left.partIndex - right.partIndex : cacheKeyOrder;
  });
};

export const loadCachedModelPart = async (key: string): Promise<ArrayBuffer | null> => {
  const row = await withStore<StoredModelPartData>(MODEL_PART_DATA_STORE, "readonly", (store) =>
    store.get(key),
  );
  if (!row) {
    return null;
  }
  return row.data.slice(0);
};

export const saveCachedModelPart = async ({
  buffer,
  end,
  key,
  modelCacheKey,
  modelVersion,
  partIndex,
  sourceUrl,
  start,
  totalBytes,
}: {
  buffer: Uint8Array;
  end: number;
  key: string;
  modelCacheKey: string;
  modelVersion: string;
  partIndex: number;
  sourceUrl: string;
  start: number;
  totalBytes: number;
}): Promise<void> => {
  const db = await openRaceAiDb();
  await new Promise<void>((resolve, reject) => {
    const transaction = db.transaction([MODEL_PART_STORE, MODEL_PART_DATA_STORE], "readwrite");
    const partStore = transaction.objectStore(MODEL_PART_STORE);
    const dataStore = transaction.objectStore(MODEL_PART_DATA_STORE);
    transaction.addEventListener("complete", () => {
      db.close();
      resolve();
    });
    transaction.addEventListener("error", () => {
      db.close();
      reject(transaction.error);
    });
    dataStore.put({
      data: toArrayBufferCopy(buffer),
      key,
    } satisfies StoredModelPartData);
    partStore.put({
      cachedAt: new Date().toISOString(),
      end,
      key,
      modelCacheKey,
      modelVersion,
      partIndex,
      size: buffer.byteLength,
      sourceUrl,
      start,
      totalBytes,
    } satisfies StoredModelPart);
  });
};

export const deleteCachedModelParts = async (modelCacheKey: string): Promise<void> => {
  const parts = await listCachedModelPartInfos(modelCacheKey);
  if (parts.length === 0) {
    return;
  }
  const db = await openRaceAiDb();
  await new Promise<void>((resolve, reject) => {
    const transaction = db.transaction([MODEL_PART_STORE, MODEL_PART_DATA_STORE], "readwrite");
    const partStore = transaction.objectStore(MODEL_PART_STORE);
    const dataStore = transaction.objectStore(MODEL_PART_DATA_STORE);
    transaction.addEventListener("complete", () => {
      db.close();
      resolve();
    });
    transaction.addEventListener("error", () => {
      db.close();
      reject(transaction.error);
    });
    for (const part of parts) {
      partStore.delete(part.key);
      dataStore.delete(part.key);
    }
  });
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
      size: buffer.byteLength,
      sourceUrl,
    } satisfies StoredModel);
  });
};

export const deleteCachedModel = async (key: string): Promise<void> => {
  await withStore(MODEL_STORE, "readwrite", (store) => {
    store.delete(key);
  });
};
