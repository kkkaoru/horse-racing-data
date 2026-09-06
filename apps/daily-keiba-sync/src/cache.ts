import type { Provider } from "./types";

const CACHE_API_TTL_SECONDS = 30;
const KV_TTL_SECONDS = 300;
const CACHE_ORIGIN = "https://daily-keiba-sync-cache.invalid";

interface CursorCache {
  delete(request: Request): Promise<boolean>;
  match(request: Request): Promise<Response | undefined>;
  put(request: Request, response: Response): Promise<void>;
}

interface CursorKv {
  delete(key: string): Promise<void>;
  get(key: string, type: "json"): Promise<unknown>;
  put(key: string, value: string, options: { expirationTtl: number }): Promise<void>;
}

interface CursorCacheValue {
  cursor: string | null;
  version: 1;
}

const cursorKey = (provider: Provider): string => `acquisition-cursor:v1:${provider}`;

const cursorRequest = (provider: Provider): Request =>
  new Request(`${CACHE_ORIGIN}/acquisition-cursor/v1/${provider}`);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseCursorValue = (value: unknown): CursorCacheValue | null => {
  if (!isRecord(value)) return null;
  const candidate = value;
  if (candidate.version !== 1) return null;
  if (
    candidate.cursor !== null &&
    (typeof candidate.cursor !== "string" || !/^20\d{12}$/.test(candidate.cursor))
  )
    return null;
  return { cursor: candidate.cursor, version: 1 };
};

const parseResponse = async (response: Response | undefined): Promise<CursorCacheValue | null> => {
  if (response === undefined) return null;
  try {
    return parseCursorValue(await response.json());
  } catch {
    return null;
  }
};

const putCursor = async (
  kv: CursorKv,
  cache: CursorCache,
  provider: Provider,
  value: CursorCacheValue,
): Promise<void> => {
  const body = JSON.stringify(value);
  await kv.put(cursorKey(provider), body, { expirationTtl: KV_TTL_SECONDS });
  await cache.put(
    cursorRequest(provider),
    new Response(body, {
      headers: {
        "Cache-Control": `max-age=${CACHE_API_TTL_SECONDS}`,
        "Content-Type": "application/json",
      },
    }),
  );
};

export const getCachedProviderCursor = async (
  kv: CursorKv,
  provider: Provider,
  load: () => Promise<string | null>,
  cache?: CursorCache,
): Promise<string | null> => {
  const targetCache = cache ?? (await caches.open("daily-keiba-sync-v1"));
  const local = await parseResponse(await targetCache.match(cursorRequest(provider)));
  if (local !== null) return local.cursor;

  const stored = parseCursorValue(await kv.get(cursorKey(provider), "json"));
  if (stored !== null) {
    await targetCache.put(
      cursorRequest(provider),
      new Response(JSON.stringify(stored), {
        headers: {
          "Cache-Control": `max-age=${CACHE_API_TTL_SECONDS}`,
          "Content-Type": "application/json",
        },
      }),
    );
    return stored.cursor;
  }

  const cursor = await load();
  await putCursor(kv, targetCache, provider, { cursor, version: 1 });
  return cursor;
};

export const purgeProviderCursorCache = async (
  kv: CursorKv,
  provider: Provider,
  cache?: CursorCache,
): Promise<void> => {
  const targetCache = cache ?? (await caches.open("daily-keiba-sync-v1"));
  await Promise.all([kv.delete(cursorKey(provider)), targetCache.delete(cursorRequest(provider))]);
};

export const purgeAllProviderCursorCaches = async (
  kv: CursorKv,
  cache?: CursorCache,
): Promise<void> => {
  const targetCache = cache ?? (await caches.open("daily-keiba-sync-v1"));
  await Promise.all([
    purgeProviderCursorCache(kv, "jv", targetCache),
    purgeProviderCursorCache(kv, "nv", targetCache),
  ]);
};
