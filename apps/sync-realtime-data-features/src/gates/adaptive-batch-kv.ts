// Run with bun. Phase F: adaptive batch size for the scheduled enqueue.
// Tracks the last N recompute outcomes in a single KV key and grows / shrinks
// the per-tick enqueue cap based on success rate. The state is best-effort:
// concurrent ticks may race and the latest write wins. That's acceptable
// because the window is just a heuristic, not a correctness gate.

import type { Env } from "../types";

const WINDOW_KEY = "features:metrics:recompute:window:v1";
const WINDOW_TTL_SECONDS = 86_400;
const WINDOW_SIZE = 50;
const WINDOW_MIN_SAMPLES = 10;
const BATCH_MIN = 3;
const BATCH_MAX = 30;
const BATCH_START = 5;
const PERCENT_BASE = 100;
const SUCCESS_INC_THRESHOLD = 80;
const SUCCESS_DEC_THRESHOLD = 50;

interface WindowState {
  recent: boolean[];
  batchSize: number;
}

const isBooleanArray = (value: unknown): value is boolean[] =>
  Array.isArray(value) && value.every((entry) => typeof entry === "boolean");

const isWindowState = (value: unknown): value is WindowState => {
  if (value === null || typeof value !== "object") {
    return false;
  }
  const candidate = value as { recent?: unknown; batchSize?: unknown };
  return isBooleanArray(candidate.recent) && typeof candidate.batchSize === "number";
};

const tryParseWindowState = (json: string): WindowState | null => {
  try {
    const parsed: unknown = JSON.parse(json);
    return isWindowState(parsed) ? parsed : null;
  } catch {
    return null;
  }
};

const safeKvGet = async (env: Env): Promise<string | null> => {
  try {
    return await env.FEATURES_KV.get(WINDOW_KEY);
  } catch {
    return null;
  }
};

const safeKvPut = async (env: Env, state: WindowState): Promise<void> => {
  try {
    await env.FEATURES_KV.put(WINDOW_KEY, JSON.stringify(state), {
      expirationTtl: WINDOW_TTL_SECONDS,
    });
  } catch {
    // best-effort; metrics window write failure should not block enqueue
  }
};

const readWindowState = async (env: Env): Promise<WindowState | null> => {
  const json = await safeKvGet(env);
  if (!json) {
    return null;
  }
  return tryParseWindowState(json);
};

const computeSuccessRate = (recent: boolean[]): number => {
  if (recent.length === 0) {
    return 0;
  }
  const successCount = recent.filter((entry) => entry).length;
  return (successCount * PERCENT_BASE) / recent.length;
};

const adjustBatchSize = (current: number, successRate: number): number => {
  if (successRate >= SUCCESS_INC_THRESHOLD) {
    return Math.min(BATCH_MAX, current + 1);
  }
  if (successRate < SUCCESS_DEC_THRESHOLD) {
    return Math.max(BATCH_MIN, current - 1);
  }
  return current;
};

const appendOutcomeToRecent = (recent: boolean[], success: boolean): boolean[] => {
  const next = [success, ...recent];
  return next.slice(0, WINDOW_SIZE);
};

const buildSeedState = (success: boolean): WindowState => ({
  batchSize: BATCH_START,
  recent: [success],
});

export const recordRecomputeOutcome = async (env: Env, success: boolean): Promise<void> => {
  const existing = await readWindowState(env);
  if (!existing) {
    await safeKvPut(env, buildSeedState(success));
    return;
  }
  const nextRecent = appendOutcomeToRecent(existing.recent, success);
  const nextBatchSize =
    nextRecent.length < WINDOW_MIN_SAMPLES
      ? existing.batchSize
      : adjustBatchSize(existing.batchSize, computeSuccessRate(nextRecent));
  await safeKvPut(env, { batchSize: nextBatchSize, recent: nextRecent });
};

export const readNextBatchSize = async (env: Env): Promise<number> => {
  const state = await readWindowState(env);
  if (!state) {
    return BATCH_START;
  }
  if (state.recent.length < WINDOW_MIN_SAMPLES) {
    return state.batchSize;
  }
  return adjustBatchSize(state.batchSize, computeSuccessRate(state.recent));
};
