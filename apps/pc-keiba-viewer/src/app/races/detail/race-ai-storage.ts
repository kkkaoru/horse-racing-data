"use client";

const DB_NAME = "pc-keiba-race-ai";
const DB_VERSION = 1;
const LOG_STORE = "raceLogs";
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

export interface RaceAiCachedModelInfo {
  cachedAt: string;
  key: string;
  modelVersion: string;
  size: number | null;
  sourceUrl: string;
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

export const getCachedModelInfo = async (key: string): Promise<RaceAiCachedModelInfo | null> => {
  const row = await withStore<StoredModel>(MODEL_STORE, "readonly", (store) => store.get(key));
  return row
    ? {
        cachedAt: row.cachedAt,
        key: row.key,
        modelVersion: row.modelVersion,
        size: row.blob.size || null,
        sourceUrl: row.sourceUrl,
      }
    : null;
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

export const deleteCachedModel = async (key: string): Promise<void> => {
  await withStore(MODEL_STORE, "readwrite", (store) => {
    store.delete(key);
  });
};
