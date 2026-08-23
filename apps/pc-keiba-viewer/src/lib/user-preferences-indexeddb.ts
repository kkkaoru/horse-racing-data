"use client";

// This file runs with bun.

import {
  DEFAULT_CONDITION_FINISH_CHART,
  DEFAULT_CONDITION_PAYOUT_CHART,
} from "./condition-analysis-charts";
import { DEFAULT_SHOW_RESULTS_CHART } from "./horse-race-time-charts";
import { DEFAULT_SHOW_ALL_TRAINING_WORKOUTS, DEFAULT_SHOW_TRAINING_CHART } from "./training-charts";
import { getOrCreateUserId } from "./user-identity-indexeddb";
import {
  DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
  DEFAULT_WIN_RATE_HEATMAP_SPLIT_BLOODLINE_LINES,
} from "./win-rate-heatmap";

const DB_NAME = "pc-keiba-viewer";
const DB_VERSION = 3;
const FAVORITES_STORE = "favorites";
const USER_IDENTITY_STORE = "userIdentity";
const USER_PREFERENCES_STORE = "userPreferences";

interface UserPreferencesRow {
  showAllTrainingWorkouts: boolean;
  showConditionFinishChart: boolean;
  showConditionPayoutChart: boolean;
  showHeatmapStarts: boolean;
  showResultsChart: boolean;
  showTrainingChart: boolean;
  splitHeatmapBloodlineLines: boolean;
  updatedAt: string;
  userId: string;
}

interface UserPreferencePatch {
  showAllTrainingWorkouts?: boolean;
  showConditionFinishChart?: boolean;
  showConditionPayoutChart?: boolean;
  showHeatmapStarts?: boolean;
  showResultsChart?: boolean;
  showTrainingChart?: boolean;
  splitHeatmapBloodlineLines?: boolean;
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

const readStoredBoolean = (value: unknown, fallback: boolean): boolean =>
  typeof value === "boolean" ? value : fallback;

const readUserPreferences = (value: unknown): UserPreferencesRow => {
  if (
    !isRecord(value) ||
    typeof value.userId !== "string" ||
    typeof value.updatedAt !== "string" ||
    typeof value.showHeatmapStarts !== "boolean"
  ) {
    return {
      showAllTrainingWorkouts: DEFAULT_SHOW_ALL_TRAINING_WORKOUTS,
      showConditionFinishChart: DEFAULT_CONDITION_FINISH_CHART,
      showConditionPayoutChart: DEFAULT_CONDITION_PAYOUT_CHART,
      showHeatmapStarts: DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS,
      showResultsChart: DEFAULT_SHOW_RESULTS_CHART,
      showTrainingChart: DEFAULT_SHOW_TRAINING_CHART,
      splitHeatmapBloodlineLines: DEFAULT_WIN_RATE_HEATMAP_SPLIT_BLOODLINE_LINES,
      updatedAt: "",
      userId: "",
    };
  }
  return {
    showAllTrainingWorkouts: readStoredBoolean(
      value.showAllTrainingWorkouts,
      DEFAULT_SHOW_ALL_TRAINING_WORKOUTS,
    ),
    showConditionFinishChart: readStoredBoolean(
      value.showConditionFinishChart,
      DEFAULT_CONDITION_FINISH_CHART,
    ),
    showConditionPayoutChart: readStoredBoolean(
      value.showConditionPayoutChart,
      DEFAULT_CONDITION_PAYOUT_CHART,
    ),
    showHeatmapStarts: value.showHeatmapStarts,
    showResultsChart: readStoredBoolean(value.showResultsChart, DEFAULT_SHOW_RESULTS_CHART),
    showTrainingChart: readStoredBoolean(value.showTrainingChart, DEFAULT_SHOW_TRAINING_CHART),
    splitHeatmapBloodlineLines: readStoredBoolean(
      value.splitHeatmapBloodlineLines,
      DEFAULT_WIN_RATE_HEATMAP_SPLIT_BLOODLINE_LINES,
    ),
    updatedAt: value.updatedAt,
    userId: value.userId,
  };
};

const putUserPreferences = async (userId: string, patch: UserPreferencePatch): Promise<void> => {
  if (!isBrowser() || userId.length === 0) {
    return;
  }
  const existing = await withStore<unknown>("readonly", (store) => store.get(userId));
  const current = readUserPreferences(existing);
  const showHeatmapStarts =
    patch.showHeatmapStarts === undefined ? current.showHeatmapStarts : patch.showHeatmapStarts;
  const splitHeatmapBloodlineLines =
    patch.splitHeatmapBloodlineLines === undefined
      ? current.splitHeatmapBloodlineLines
      : patch.splitHeatmapBloodlineLines;
  const showConditionPayoutChart =
    patch.showConditionPayoutChart === undefined
      ? current.showConditionPayoutChart
      : patch.showConditionPayoutChart;
  const showConditionFinishChart =
    patch.showConditionFinishChart === undefined
      ? current.showConditionFinishChart
      : patch.showConditionFinishChart;
  const showTrainingChart =
    patch.showTrainingChart === undefined ? current.showTrainingChart : patch.showTrainingChart;
  const showAllTrainingWorkouts =
    patch.showAllTrainingWorkouts === undefined
      ? current.showAllTrainingWorkouts
      : patch.showAllTrainingWorkouts;
  const showResultsChart =
    patch.showResultsChart === undefined ? current.showResultsChart : patch.showResultsChart;
  await withStore("readwrite", (store) => {
    store.put({
      showAllTrainingWorkouts,
      showConditionFinishChart,
      showConditionPayoutChart,
      showHeatmapStarts,
      showResultsChart,
      showTrainingChart,
      splitHeatmapBloodlineLines,
      updatedAt: new Date().toISOString(),
      userId,
    });
  });
};

export const getHeatmapShowStarts = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_WIN_RATE_HEATMAP_SHOW_STARTS;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showHeatmapStarts;
};

export const setHeatmapShowStarts = async (userId: string, showStarts: boolean): Promise<void> => {
  await putUserPreferences(userId, { showHeatmapStarts: showStarts });
};

export const getHeatmapSplitBloodlineLines = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_WIN_RATE_HEATMAP_SPLIT_BLOODLINE_LINES;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).splitHeatmapBloodlineLines;
};

export const setHeatmapSplitBloodlineLines = async (
  userId: string,
  splitBloodlineLines: boolean,
): Promise<void> => {
  await putUserPreferences(userId, { splitHeatmapBloodlineLines: splitBloodlineLines });
};

export const getConditionPayoutChart = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_CONDITION_PAYOUT_CHART;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showConditionPayoutChart;
};

export const setConditionPayoutChart = async (
  userId: string,
  showChart: boolean,
): Promise<void> => {
  await putUserPreferences(userId, { showConditionPayoutChart: showChart });
};

export const getConditionFinishChart = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_CONDITION_FINISH_CHART;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showConditionFinishChart;
};

export const setConditionFinishChart = async (
  userId: string,
  showChart: boolean,
): Promise<void> => {
  await putUserPreferences(userId, { showConditionFinishChart: showChart });
};

export const getTrainingChart = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_SHOW_TRAINING_CHART;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showTrainingChart;
};

export const setTrainingChart = async (userId: string, showChart: boolean): Promise<void> => {
  await putUserPreferences(userId, { showTrainingChart: showChart });
};

export const getTrainingScatterAllWorkouts = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_SHOW_ALL_TRAINING_WORKOUTS;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showAllTrainingWorkouts;
};

export const setTrainingScatterAllWorkouts = async (
  userId: string,
  showAllWorkouts: boolean,
): Promise<void> => {
  await putUserPreferences(userId, { showAllTrainingWorkouts: showAllWorkouts });
};

export const getResultsChart = async (userId: string): Promise<boolean> => {
  if (!isBrowser() || userId.length === 0) {
    return DEFAULT_SHOW_RESULTS_CHART;
  }
  const row = await withStore<unknown>("readonly", (store) => store.get(userId));
  return readUserPreferences(row).showResultsChart;
};

export const setResultsChart = async (userId: string, showChart: boolean): Promise<void> => {
  await putUserPreferences(userId, { showResultsChart: showChart });
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

export const loadHeatmapSplitBloodlineLinesForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getHeatmapSplitBloodlineLines(userId);
};

export const persistHeatmapSplitBloodlineLinesForCurrentUser = async (
  splitBloodlineLines: boolean,
): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setHeatmapSplitBloodlineLines(userId, splitBloodlineLines);
};

export const loadConditionPayoutChartForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getConditionPayoutChart(userId);
};

export const persistConditionPayoutChartForCurrentUser = async (
  showChart: boolean,
): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setConditionPayoutChart(userId, showChart);
};

export const loadConditionFinishChartForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getConditionFinishChart(userId);
};

export const persistConditionFinishChartForCurrentUser = async (
  showChart: boolean,
): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setConditionFinishChart(userId, showChart);
};

export const loadTrainingChartForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getTrainingChart(userId);
};

export const persistTrainingChartForCurrentUser = async (showChart: boolean): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setTrainingChart(userId, showChart);
};

export const loadTrainingScatterAllWorkoutsForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getTrainingScatterAllWorkouts(userId);
};

export const persistTrainingScatterAllWorkoutsForCurrentUser = async (
  showAllWorkouts: boolean,
): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setTrainingScatterAllWorkouts(userId, showAllWorkouts);
};

export const loadResultsChartForCurrentUser = async (): Promise<boolean> => {
  const userId = await getOrCreateUserId();
  return getResultsChart(userId);
};

export const persistResultsChartForCurrentUser = async (showChart: boolean): Promise<void> => {
  const userId = await getOrCreateUserId();
  await setResultsChart(userId, showChart);
};
