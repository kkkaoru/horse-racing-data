// Run with bun. Gate 7: colo-local Cache API for R2 GET Parquet bytes, default 5 min TTL.

import type { Env } from "../types";

const PARQUET_CACHE_BASE = "https://internal/features/parquet-bytes/v1";
const DEFAULT_TTL_SECONDS = 300;
const OCTET_STREAM = "application/octet-stream";

const resolveTtl = (env: Env): number => {
  const raw = env.FEATURES_PARQUET_BYTES_CACHE_TTL_SECONDS;
  if (!raw) {
    return DEFAULT_TTL_SECONDS;
  }
  const parsed = Number(raw);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_TTL_SECONDS;
};

const buildKey = (r2Key: string): Request =>
  new Request(`${PARQUET_CACHE_BASE}/${encodeURIComponent(r2Key)}`, { method: "GET" });

export const readParquetBytesFromCache = async (r2Key: string): Promise<ArrayBuffer | null> => {
  const cached = await caches.default.match(buildKey(r2Key));
  if (!cached) {
    return null;
  }
  return await cached.arrayBuffer();
};

export const writeParquetBytesToCache = async (
  r2Key: string,
  bytes: ArrayBuffer,
  env: Env,
): Promise<void> => {
  const ttl = resolveTtl(env);
  const response = new Response(bytes, {
    headers: {
      "Cache-Control": `public, max-age=${ttl}, s-maxage=${ttl}`,
      "Content-Type": OCTET_STREAM,
    },
  });
  await caches.default.put(buildKey(r2Key), response);
};
