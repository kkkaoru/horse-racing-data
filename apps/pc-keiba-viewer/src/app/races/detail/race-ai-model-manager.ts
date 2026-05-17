"use client";

import {
  deleteCachedModel,
  getCachedModelInfo,
  loadCachedModel,
  RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE,
  saveCachedModel,
  type RaceAiCachedModelInfo,
} from "./race-ai-storage";

export interface RaceAiModelDefinition {
  cacheKey: string;
  fileName: string;
  id: string;
  isLatest: boolean;
  name: string;
  sha256: string;
  sizeBytes: number;
  url: string;
  version: string;
}

export type RaceAiModelStatus = "downloaded" | "downloading" | "not-downloaded";

export interface RaceAiModelState {
  cachedAt: string | null;
  downloadedBytes: number;
  error: string | null;
  model: RaceAiModelDefinition;
  progress: number | null;
  status: RaceAiModelStatus;
  totalBytes: number | null;
}

interface ActiveDownload {
  controller: AbortController;
  downloadedBytes: number;
  error: string | null;
  progress: number | null;
  promise: Promise<ArrayBuffer>;
  totalBytes: number | null;
}

const MODEL_VERSION = "v20260518";
const MODEL_FILE_NAME = "gemma-4-E2B-it-web.task";
const MODEL_CACHE_KEY = `gemma-4-e2b:${MODEL_VERSION}:${MODEL_FILE_NAME}`;
const MODEL_URL = `/api/models/gemma-4-e2b/${MODEL_VERSION}/${MODEL_FILE_NAME}`;
const MODEL_SIZE_BYTES = 2_003_697_664;
const MODEL_SHA256 = "2cbff161177a4d51c9d04360016185976f504517ba5758cd10c1564e5421c5a5";

export const RACE_AI_MODELS: readonly RaceAiModelDefinition[] = [
  {
    cacheKey: MODEL_CACHE_KEY,
    fileName: MODEL_FILE_NAME,
    id: "gemma-4-e2b-v20260518",
    isLatest: true,
    name: "Gemma 4 E2B Web",
    sha256: MODEL_SHA256,
    sizeBytes: MODEL_SIZE_BYTES,
    url: MODEL_URL,
    version: MODEL_VERSION,
  },
];

const getLatestRaceAiModel = (): RaceAiModelDefinition => {
  const model = RACE_AI_MODELS.find((item) => item.isLatest) ?? RACE_AI_MODELS[0];
  if (!model) {
    throw new Error("AIモデルが定義されていません。");
  }
  return model;
};

export const LATEST_RACE_AI_MODEL = getLatestRaceAiModel();

const activeDownloads = new Map<string, ActiveDownload>();
const listeners = new Set<() => void>();

const notify = () => {
  for (const listener of listeners) {
    listener();
  }
};

export const subscribeRaceAiModelDownloads = (listener: () => void) => {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
};

const toDownloadedState = (
  model: RaceAiModelDefinition,
  info: RaceAiCachedModelInfo,
): RaceAiModelState => ({
  cachedAt: info.cachedAt,
  downloadedBytes: info.size ?? model.sizeBytes,
  error: null,
  model,
  progress: 1,
  status: "downloaded",
  totalBytes: info.size ?? model.sizeBytes,
});

export const getRaceAiModelState = async (
  model: RaceAiModelDefinition,
): Promise<RaceAiModelState> => {
  const active = activeDownloads.get(model.id);
  if (active) {
    return {
      cachedAt: null,
      downloadedBytes: active.downloadedBytes,
      error: active.error,
      model,
      progress: active.progress,
      status: "downloading",
      totalBytes: active.totalBytes,
    };
  }
  const cached = await getCachedModelInfo(model.cacheKey);
  if (cached) {
    return toDownloadedState(model, cached);
  }
  return {
    cachedAt: null,
    downloadedBytes: 0,
    error: null,
    model,
    progress: null,
    status: "not-downloaded",
    totalBytes: model.sizeBytes,
  };
};

export const getRaceAiModelStates = async (): Promise<RaceAiModelState[]> =>
  Promise.all(RACE_AI_MODELS.map((model) => getRaceAiModelState(model)));

const updateActiveDownload = (
  model: RaceAiModelDefinition,
  patch: Partial<Pick<ActiveDownload, "downloadedBytes" | "error" | "progress" | "totalBytes">>,
) => {
  const active = activeDownloads.get(model.id);
  if (!active) {
    return;
  }
  activeDownloads.set(model.id, {
    ...active,
    ...patch,
  });
  notify();
};

const fetchModelBuffer = async (
  model: RaceAiModelDefinition,
  controller: AbortController,
): Promise<ArrayBuffer> => {
  const response = await fetch(model.url, { signal: controller.signal });
  if (!response.ok) {
    throw new Error(`model api ${response.status}`);
  }
  const total = Number(response.headers.get("content-length"));
  const totalBytes = Number.isFinite(total) && total > 0 ? total : model.sizeBytes;
  updateActiveDownload(model, { totalBytes });
  if (!response.body) {
    const buffer = await response.arrayBuffer();
    updateActiveDownload(model, {
      downloadedBytes: buffer.byteLength,
      progress: 1,
      totalBytes: buffer.byteLength,
    });
    return buffer;
  }

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let downloadedBytes = 0;

  for (;;) {
    // eslint-disable-next-line no-await-in-loop -- model bytes must be read sequentially from the response stream.
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    if (!value) {
      continue;
    }
    chunks.push(value);
    downloadedBytes += value.byteLength;
    updateActiveDownload(model, {
      downloadedBytes,
      progress: totalBytes > 0 ? downloadedBytes / totalBytes : null,
      totalBytes,
    });
  }

  const buffer = new Uint8Array(downloadedBytes);
  let offset = 0;
  for (const chunk of chunks) {
    buffer.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return buffer.buffer;
};

export const downloadRaceAiModel = async (
  model: RaceAiModelDefinition = LATEST_RACE_AI_MODEL,
): Promise<ArrayBuffer> => {
  const cached = await loadCachedModel(model.cacheKey);
  if (cached) {
    notify();
    return cached;
  }

  const active = activeDownloads.get(model.id);
  if (active) {
    return active.promise;
  }

  const controller = new AbortController();
  const promise = (async () => {
    try {
      const buffer = await fetchModelBuffer(model, controller);
      await saveCachedModel({
        buffer,
        key: model.cacheKey,
        modelVersion: model.version,
        sourceUrl: model.url,
      });
      return buffer;
    } catch (error) {
      updateActiveDownload(model, {
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    } finally {
      activeDownloads.delete(model.id);
      notify();
    }
  })();

  activeDownloads.set(model.id, {
    controller,
    downloadedBytes: 0,
    error: null,
    progress: 0,
    promise,
    totalBytes: model.sizeBytes,
  });
  notify();
  return promise;
};

export const ensureRaceAiModelBuffer = async ({
  confirmDownload,
  model = LATEST_RACE_AI_MODEL,
}: {
  confirmDownload: boolean;
  model?: RaceAiModelDefinition;
}): Promise<ArrayBuffer> => {
  const cached = await loadCachedModel(model.cacheKey);
  if (cached) {
    notify();
    return cached;
  }
  const active = activeDownloads.get(model.id);
  if (active) {
    return active.promise;
  }
  if (confirmDownload && !window.confirm(RACE_AI_MODEL_DOWNLOAD_CONFIRM_MESSAGE)) {
    throw new Error("AIモデルのダウンロードがキャンセルされました。");
  }
  return downloadRaceAiModel(model);
};

export const abortRaceAiModelDownload = (model: RaceAiModelDefinition): void => {
  activeDownloads.get(model.id)?.controller.abort();
};

export const deleteRaceAiModel = async (model: RaceAiModelDefinition): Promise<void> => {
  abortRaceAiModelDownload(model);
  await deleteCachedModel(model.cacheKey);
  notify();
};

export const formatRaceAiModelSize = (bytes: number | null): string => {
  if (!bytes || bytes <= 0) {
    return "-";
  }
  const gib = bytes / 1024 / 1024 / 1024;
  return `${gib.toFixed(2)} GiB`;
};
