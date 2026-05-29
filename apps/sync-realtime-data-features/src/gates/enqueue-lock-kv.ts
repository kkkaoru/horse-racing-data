// Run with bun. Gate 3: KV-based enqueue dedupe. Default 60s TTL.

import type { Env } from "../types";

const ENQUEUE_LOCK_KEY_PREFIX = "features:enqueue-lock";
const DEFAULT_ENQUEUE_LOCK_TTL_SECONDS = 60;

const buildEnqueueLockKey = (raceKey: string, jobType: string): string =>
  `${ENQUEUE_LOCK_KEY_PREFIX}:${jobType}:${raceKey}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.FEATURES_ENQUEUE_LOCK_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_ENQUEUE_LOCK_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_ENQUEUE_LOCK_TTL_SECONDS;
};

export const isEnqueueLocked = async (
  env: Env,
  raceKey: string,
  jobType: string,
): Promise<boolean> => {
  const locked = await env.FEATURES_KV.get(buildEnqueueLockKey(raceKey, jobType));
  return locked !== null;
};

export const acquireEnqueueLock = async (
  env: Env,
  raceKey: string,
  jobType: string,
): Promise<void> => {
  await env.FEATURES_KV.put(buildEnqueueLockKey(raceKey, jobType), "1", {
    expirationTtl: resolveTtlSeconds(env),
  });
};
