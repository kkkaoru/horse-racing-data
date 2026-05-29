// Run with bun. Gate 6: KV cache for R2 list results so repeated list calls hit KV instead.

import type { Env } from "../types";

const R2_LIST_KV_KEY_PREFIX = "features:r2-list:v1";
const DEFAULT_R2_LIST_CACHE_TTL_SECONDS = 600;

const buildR2ListKey = (prefix: string): string => `${R2_LIST_KV_KEY_PREFIX}:${prefix}`;

const resolveTtlSeconds = (env: Env): number => {
  const raw = env.FEATURES_R2_LIST_CACHE_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_R2_LIST_CACHE_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_R2_LIST_CACHE_TTL_SECONDS;
};

export const readR2ListFromKv = async (env: Env, prefix: string): Promise<string[] | null> => {
  const json = await env.FEATURES_KV.get(buildR2ListKey(prefix));
  return json ? (JSON.parse(json) as string[]) : null;
};

export const writeR2ListToKv = async (env: Env, prefix: string, keys: string[]): Promise<void> => {
  await env.FEATURES_KV.put(buildR2ListKey(prefix), JSON.stringify(keys), {
    expirationTtl: resolveTtlSeconds(env),
  });
};
