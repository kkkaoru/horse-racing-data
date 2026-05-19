"use client";

import {
  deleteCachedModel,
  getCachedModelInfo,
  listCachedModelInfos,
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
  attempt: number | null;
  cachedAt: string | null;
  downloadedBytes: number;
  error: string | null;
  maxAttempts: number | null;
  model: RaceAiModelDefinition;
  progress: number | null;
  status: RaceAiModelStatus;
  totalBytes: number | null;
}

interface ActiveDownload {
  attempt: number | null;
  controller: AbortController;
  downloadedBytes: number;
  error: string | null;
  maxAttempts: number;
  progress: number | null;
  promise: Promise<ArrayBuffer>;
  totalBytes: number | null;
}

const MODEL_VERSION = "v20260518";
const MODEL_FILE_NAME = "gemma-4-E2B-it-web.task";
const MODEL_CACHE_KEY = `gemma-4-e2b:${MODEL_VERSION}:${MODEL_FILE_NAME}`;
const MODEL_SIZE_BYTES = 2_003_697_664;
const MODEL_URL = `/api/models/gemma-4-e2b/${MODEL_VERSION}/${MODEL_FILE_NAME}?expectedSize=${MODEL_SIZE_BYTES}`;
const MODEL_SHA256 = "2cbff161177a4d51c9d04360016185976f504517ba5758cd10c1564e5421c5a5";
const MODEL_SIZE_TOLERANCE_BYTES = Math.max(Math.round(MODEL_SIZE_BYTES * 0.02), 32 * 1024 * 1024);
const MODEL_DOWNLOAD_RANGE_BYTES = 32 * 1024 * 1024;
const MODEL_DOWNLOAD_MAX_ATTEMPTS = 3;
const MODEL_DOWNLOAD_RANGE_MAX_ATTEMPTS = 4;
const MODEL_DOWNLOAD_RETRY_DELAY_MS = 1_200;

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
const lastModelErrors = new Map<string, string>();
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
  attempt: null,
  cachedAt: info.cachedAt,
  downloadedBytes: info.size ?? model.sizeBytes,
  error: null,
  maxAttempts: null,
  model,
  progress: 1,
  status: "downloaded",
  totalBytes: model.sizeBytes,
});

const isCompleteCachedModelInfo = (
  model: RaceAiModelDefinition,
  info: RaceAiCachedModelInfo,
): boolean => isAcceptableModelSize(model, info.size);

const isAcceptableModelSize = (
  model: RaceAiModelDefinition,
  actualBytes: number | null | undefined,
): actualBytes is number =>
  typeof actualBytes === "number" &&
  Number.isFinite(actualBytes) &&
  actualBytes > 0 &&
  Math.abs(actualBytes - model.sizeBytes) <= MODEL_SIZE_TOLERANCE_BYTES;

const markIncompleteCachedModel = (
  model: RaceAiModelDefinition,
  info?: RaceAiCachedModelInfo | null,
): void => {
  lastModelErrors.set(
    model.id,
    info?.size
      ? `保存済みモデルのサイズが想定と異なるためAI利用には使いません (${info.size}/${model.sizeBytes})。削除または再ダウンロードしてください。`
      : "保存済みモデルのサイズを確認できませんでした。削除または再ダウンロードしてください。",
  );
};

const getCompleteCachedModelInfo = async (
  model: RaceAiModelDefinition,
): Promise<RaceAiCachedModelInfo | null> => {
  const cached = await getCachedModelInfo(model.cacheKey);
  if (!cached) {
    return null;
  }
  if (!isCompleteCachedModelInfo(model, cached)) {
    markIncompleteCachedModel(model, cached);
    return null;
  }
  return cached;
};

const toIncompleteCachedState = (
  model: RaceAiModelDefinition,
  info: RaceAiCachedModelInfo,
): RaceAiModelState => ({
  attempt: null,
  cachedAt: info.cachedAt,
  downloadedBytes: info.size ?? 0,
  error:
    info.size === null
      ? "保存済みモデルのサイズを確認できませんでした。削除または再ダウンロードしてください。"
      : `保存済みモデルのサイズが想定と異なるためAI利用には使いません (${info.size}/${model.sizeBytes})。削除または再ダウンロードしてください。`,
  maxAttempts: null,
  model,
  progress: null,
  status: "not-downloaded",
  totalBytes: model.sizeBytes,
});

const loadCompleteCachedModel = async (
  model: RaceAiModelDefinition,
): Promise<ArrayBuffer | null> => {
  const cachedInfo = await getCompleteCachedModelInfo(model);
  if (!cachedInfo) {
    return null;
  }
  const cached = await loadCachedModel(model.cacheKey);
  if (!cached) {
    return null;
  }
  if (!isAcceptableModelSize(model, cached.byteLength)) {
    markIncompleteCachedModel(model, cachedInfo);
    return null;
  }
  return cached;
};

export const getRaceAiModelState = async (
  model: RaceAiModelDefinition,
): Promise<RaceAiModelState> => {
  const active = activeDownloads.get(model.id);
  if (active) {
    return {
      attempt: active.attempt,
      cachedAt: null,
      downloadedBytes: active.downloadedBytes,
      error: active.error,
      maxAttempts: active.maxAttempts,
      model,
      progress: active.progress,
      status: "downloading",
      totalBytes: active.totalBytes,
    };
  }
  const cached = await getCachedModelInfo(model.cacheKey);
  if (cached) {
    return isCompleteCachedModelInfo(model, cached)
      ? toDownloadedState(model, cached)
      : toIncompleteCachedState(model, cached);
  }
  return {
    attempt: null,
    cachedAt: null,
    downloadedBytes: 0,
    error: lastModelErrors.get(model.id) ?? null,
    maxAttempts: null,
    model,
    progress: null,
    status: "not-downloaded",
    totalBytes: model.sizeBytes,
  };
};

const readExpectedSizeFromSourceUrl = (sourceUrl: string): number | null => {
  try {
    const url = new URL(sourceUrl, "https://pc-keiba.local");
    const expectedSize = Number(url.searchParams.get("expectedSize"));
    return Number.isFinite(expectedSize) && expectedSize > 0 ? expectedSize : null;
  } catch {
    return null;
  }
};

const readFileNameFromSourceUrl = (sourceUrl: string, fallback: string): string => {
  try {
    const url = new URL(sourceUrl, "https://pc-keiba.local");
    const segments = url.pathname.split("/").filter(Boolean);
    return segments.at(-1) || fallback;
  } catch {
    return fallback;
  }
};

const toStoredOnlyModel = (info: RaceAiCachedModelInfo): RaceAiModelDefinition => {
  const fileName = readFileNameFromSourceUrl(info.sourceUrl, info.key);
  return {
    cacheKey: info.key,
    fileName,
    id: `cached-${encodeURIComponent(info.key)}`,
    isLatest: false,
    name:
      info.modelVersion === MODEL_VERSION && fileName === MODEL_FILE_NAME
        ? "Gemma 4 E2B Web (保存済み)"
        : `${fileName} (保存済み)`,
    sha256: "",
    sizeBytes: readExpectedSizeFromSourceUrl(info.sourceUrl) ?? info.size ?? 0,
    url: info.sourceUrl,
    version: info.modelVersion || "cached",
  };
};

const toStoredOnlyState = (info: RaceAiCachedModelInfo): RaceAiModelState => {
  const model = toStoredOnlyModel(info);
  const usable = model.sizeBytes > 0 && isAcceptableModelSize(model, info.size);
  return {
    attempt: null,
    cachedAt: info.cachedAt,
    downloadedBytes: info.size ?? 0,
    error: usable
      ? null
      : "この保存済みモデルは現在のAI利用対象ではありません。不要であれば削除してください。",
    maxAttempts: null,
    model,
    progress: usable ? 1 : null,
    status: usable ? "downloaded" : "not-downloaded",
    totalBytes: model.sizeBytes || info.size,
  };
};

export const getRaceAiModelStates = async (): Promise<RaceAiModelState[]> => {
  const knownStates = await Promise.all(RACE_AI_MODELS.map((model) => getRaceAiModelState(model)));
  const knownCacheKeys = new Set(RACE_AI_MODELS.map((model) => model.cacheKey));
  const storedOnlyStates = (await listCachedModelInfos())
    .filter((info) => !knownCacheKeys.has(info.key))
    .toSorted((left, right) => right.cachedAt.localeCompare(left.cachedAt))
    .map(toStoredOnlyState);
  return [...knownStates, ...storedOnlyStates];
};

const updateActiveDownload = (
  model: RaceAiModelDefinition,
  patch: Partial<
    Pick<ActiveDownload, "attempt" | "downloadedBytes" | "error" | "progress" | "totalBytes">
  >,
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

const abortError = (): DOMException =>
  new DOMException("AIモデルのダウンロードが中止されました。", "AbortError");

const isAbortError = (error: unknown): boolean =>
  error instanceof DOMException
    ? error.name === "AbortError"
    : error instanceof Error && error.name === "AbortError";

const waitForRetry = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(abortError());
      return;
    }
    let timeout = 0;
    const onAbort = () => {
      window.clearTimeout(timeout);
      reject(abortError());
    };
    timeout = window.setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });

const errorMessage = (error: unknown): string =>
  error instanceof Error ? error.message : String(error);

const createModelSizeMismatchError = (
  model: RaceAiModelDefinition,
  actualBytes: number,
  source: string,
): Error =>
  new Error(
    `model size mismatch (${source}): expected about ${model.sizeBytes}, got ${actualBytes}`,
  );

const buildModelRequestUrl = (model: RaceAiModelDefinition, attempt: number): string => {
  const url = new URL(model.url, window.location.origin);
  url.searchParams.set("expectedSize", String(model.sizeBytes));
  url.searchParams.set("downloadAttempt", String(attempt));
  url.searchParams.set("downloadStartedAt", String(Date.now()));
  return `${url.pathname}${url.search}`;
};

const fetchRemoteModelSize = async (
  model: RaceAiModelDefinition,
  controller: AbortController,
  attempt: number,
): Promise<number> => {
  const response = await fetch(buildModelRequestUrl(model, attempt), {
    cache: "no-store",
    headers: { accept: "application/octet-stream" },
    method: "HEAD",
    signal: controller.signal,
  });
  if (!response.ok) {
    throw new Error(`model api ${response.status}`);
  }
  const total = Number(response.headers.get("content-length"));
  if (!isAcceptableModelSize(model, total)) {
    throw createModelSizeMismatchError(model, Number.isFinite(total) ? total : 0, "content-length");
  }
  return total;
};

const readRangeBuffer = async (response: Response, expectedBytes: number): Promise<Uint8Array> => {
  const buffer = new Uint8Array(await response.arrayBuffer());
  if (buffer.byteLength !== expectedBytes) {
    throw new Error(
      `model range size mismatch: expected ${expectedBytes}, got ${buffer.byteLength}`,
    );
  }
  return buffer;
};

const fetchModelRangeWithRetries = async ({
  attempt,
  controller,
  end,
  model,
  rangeIndex,
  start,
}: {
  attempt: number;
  controller: AbortController;
  end: number;
  model: RaceAiModelDefinition;
  rangeIndex: number;
  start: number;
}): Promise<Uint8Array> => {
  const expectedBytes = end - start + 1;
  let lastError: unknown = null;
  for (let rangeAttempt = 1; rangeAttempt <= MODEL_DOWNLOAD_RANGE_MAX_ATTEMPTS; rangeAttempt += 1) {
    try {
      const requestUrl = `${buildModelRequestUrl(model, attempt)}&downloadRange=${rangeIndex}&rangeAttempt=${rangeAttempt}`;
      // eslint-disable-next-line no-await-in-loop -- range retries must be sequential.
      const response = await fetch(requestUrl, {
        cache: "no-store",
        headers: {
          accept: "application/octet-stream",
          range: `bytes=${start}-${end}`,
        },
        signal: controller.signal,
      });
      if (response.status !== 206) {
        throw new Error(`model range api ${response.status}`);
      }
      // eslint-disable-next-line no-await-in-loop -- the body belongs to this range attempt.
      return await readRangeBuffer(response, expectedBytes);
    } catch (error) {
      lastError = error;
      if (isAbortError(error) || rangeAttempt >= MODEL_DOWNLOAD_RANGE_MAX_ATTEMPTS) {
        break;
      }
      updateActiveDownload(model, {
        error: `範囲 ${rangeIndex + 1} の取得に失敗しました。再試行します (${rangeAttempt + 1}/${MODEL_DOWNLOAD_RANGE_MAX_ATTEMPTS})`,
      });
      // eslint-disable-next-line no-await-in-loop -- retry backoff must happen before the next range request.
      await waitForRetry(500 * rangeAttempt, controller.signal);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(errorMessage(lastError));
};

const fetchModelBuffer = async (
  model: RaceAiModelDefinition,
  controller: AbortController,
  attempt: number,
): Promise<ArrayBuffer> => {
  const totalBytes = await fetchRemoteModelSize(model, controller, attempt);
  updateActiveDownload(model, { error: null, totalBytes });
  const buffer = new Uint8Array(totalBytes);
  let downloadedBytes = 0;
  let rangeIndex = 0;

  for (let start = 0; start < totalBytes; start += MODEL_DOWNLOAD_RANGE_BYTES) {
    const end = Math.min(start + MODEL_DOWNLOAD_RANGE_BYTES - 1, totalBytes - 1);
    // eslint-disable-next-line no-await-in-loop -- model ranges must be written in sequence.
    const rangeBuffer = await fetchModelRangeWithRetries({
      attempt,
      controller,
      end,
      model,
      rangeIndex,
      start,
    });
    buffer.set(rangeBuffer, start);
    downloadedBytes += rangeBuffer.byteLength;
    updateActiveDownload(model, {
      downloadedBytes,
      progress: Math.min(downloadedBytes / totalBytes, 1),
      totalBytes,
    });
    rangeIndex += 1;
  }

  if (!isAcceptableModelSize(model, downloadedBytes)) {
    throw createModelSizeMismatchError(model, downloadedBytes, "downloaded bytes");
  }
  return buffer.buffer;
};

const fetchModelBufferWithRetries = async (
  model: RaceAiModelDefinition,
  controller: AbortController,
): Promise<ArrayBuffer> => {
  let lastError: unknown = null;
  for (let attempt = 1; attempt <= MODEL_DOWNLOAD_MAX_ATTEMPTS; attempt += 1) {
    updateActiveDownload(model, {
      attempt,
      downloadedBytes: 0,
      error: attempt === 1 ? null : `再試行しています (${attempt}/${MODEL_DOWNLOAD_MAX_ATTEMPTS})`,
      progress: 0,
      totalBytes: model.sizeBytes,
    });
    try {
      // eslint-disable-next-line no-await-in-loop -- retry attempts must run sequentially.
      return await fetchModelBuffer(model, controller, attempt);
    } catch (error) {
      lastError = error;
      if (isAbortError(error) || attempt >= MODEL_DOWNLOAD_MAX_ATTEMPTS) {
        updateActiveDownload(model, { error: errorMessage(error) });
        break;
      }
      updateActiveDownload(model, {
        error: `${errorMessage(error)}。再試行します (${attempt + 1}/${MODEL_DOWNLOAD_MAX_ATTEMPTS})`,
      });
      // eslint-disable-next-line no-await-in-loop -- retry backoff must happen before the next request.
      await waitForRetry(MODEL_DOWNLOAD_RETRY_DELAY_MS * attempt, controller.signal);
    }
  }
  throw lastError instanceof Error ? lastError : new Error(errorMessage(lastError));
};

export const downloadRaceAiModel = async (
  model: RaceAiModelDefinition = LATEST_RACE_AI_MODEL,
): Promise<ArrayBuffer> => {
  const cached = await loadCompleteCachedModel(model);
  if (cached) {
    notify();
    return cached;
  }

  const active = activeDownloads.get(model.id);
  if (active) {
    return active.promise;
  }

  lastModelErrors.delete(model.id);
  const controller = new AbortController();
  const promise = (async () => {
    await Promise.resolve();
    try {
      const buffer = await fetchModelBufferWithRetries(model, controller);
      await saveCachedModel({
        buffer,
        key: model.cacheKey,
        modelVersion: model.version,
        sourceUrl: model.url,
      });
      const saved = await getCompleteCachedModelInfo(model);
      if (!saved) {
        throw new Error("保存後のAIモデルサイズを確認できませんでした。");
      }
      lastModelErrors.delete(model.id);
      return buffer;
    } catch (error) {
      const message = errorMessage(error);
      if (!isAbortError(error)) {
        lastModelErrors.set(model.id, message);
      }
      updateActiveDownload(model, { error: message });
      throw error;
    } finally {
      activeDownloads.delete(model.id);
      notify();
    }
  })();

  activeDownloads.set(model.id, {
    attempt: null,
    controller,
    downloadedBytes: 0,
    error: null,
    maxAttempts: MODEL_DOWNLOAD_MAX_ATTEMPTS,
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
  const cached = await loadCompleteCachedModel(model);
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
  lastModelErrors.delete(model.id);
  notify();
};

export const formatRaceAiModelSize = (bytes: number | null): string => {
  if (!bytes || bytes <= 0) {
    return "-";
  }
  const gib = bytes / 1024 / 1024 / 1024;
  return `${gib.toFixed(2)} GiB`;
};
