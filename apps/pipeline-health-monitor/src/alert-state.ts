// Run with bun.
import type { Env } from "./types";

const FAILURE_KV_TTL_SECONDS = 14400;
const KV_PREFIX = "failures:";
const ZERO_DEFAULT = 0;
const INCREMENT_STEP = 1;
const DECIMAL_RADIX = 10;

const buildKvKey = (checkName: string): string => `${KV_PREFIX}${checkName}`;

const parseCounter = (raw: string | null): number => {
  if (raw === null) {
    return ZERO_DEFAULT;
  }
  const parsed = Number.parseInt(raw, DECIMAL_RADIX);
  return Number.isNaN(parsed) ? ZERO_DEFAULT : parsed;
};

export const getFailureCount = async (env: Env, checkName: string): Promise<number> => {
  const raw = await env.STATE_KV.get(buildKvKey(checkName));
  return parseCounter(raw);
};

export const incrementFailureCounter = async (env: Env, checkName: string): Promise<number> => {
  const current = await getFailureCount(env, checkName);
  const next = current + INCREMENT_STEP;
  await env.STATE_KV.put(buildKvKey(checkName), String(next), {
    expirationTtl: FAILURE_KV_TTL_SECONDS,
  });
  return next;
};

export const resetFailureCounter = async (env: Env, checkName: string): Promise<void> => {
  await env.STATE_KV.delete(buildKvKey(checkName));
};
