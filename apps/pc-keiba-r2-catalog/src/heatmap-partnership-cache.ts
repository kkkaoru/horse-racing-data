// Run with bun (bunx vitest).
import { partnershipCacheKey } from "./heatmap-partnership";
import {
  buildPartnershipCountsQuery,
  buildPartnershipTargetRacesQuery,
  type PartnershipQueryInput,
} from "./heatmap-partnership-sql";
import type { CacheStore, KvStore } from "./types";

export interface PartnershipCohort {
  targetRaces: Record<string, unknown>[];
  values: Record<string, unknown>[];
}

export interface PartnershipCohortRequest {
  cache: CacheStore;
  execute: (sql: string) => Promise<Record<string, unknown>[]>;
  kv: KvStore;
  query: PartnershipQueryInput;
  warm: boolean;
}

interface MemoryEntry {
  body: string;
  expires: number;
}

interface CachedQueryRequest {
  key: string;
  request: PartnershipCohortRequest;
  sql: () => string;
}

export interface PartnershipCohortCache {
  load: (request: PartnershipCohortRequest) => Promise<PartnershipCohort | null>;
}

const TTL_SECONDS: number = 36 * 60 * 60;
const MEMORY_TTL_MS: number = 60_000;
const MEMORY_ENTRIES: number = 64;
const MEMORY_BODY_LIMIT: number = 256 * 1024;
const CACHE_ORIGIN: string = "https://pc-keiba-r2-catalog-cache.internal/partnership/";

const parseRows = (text: string): Record<string, unknown>[] | null => {
  try {
    const value: unknown = JSON.parse(text);
    return Array.isArray(value) &&
      value.every(
        (row): row is Record<string, unknown> =>
          typeof row === "object" && row !== null && !Array.isArray(row),
      )
      ? value
      : null;
  } catch {
    return null;
  }
};

const cacheResponse = (body: string): Response =>
  new Response(body, {
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": `public, max-age=${TTL_SECONDS}`,
    },
  });

// Each isolate owns a bounded hot cache and singleflight map. KV supplies the
// globally shared copy; Cache API supplies the regional copy. Read requests
// never execute SQL, even when every cache tier misses.
export const createPartnershipCohortCache = (): PartnershipCohortCache => {
  const memory: Map<string, MemoryEntry> = new Map();
  const pending: Map<string, Promise<Record<string, unknown>[] | null>> = new Map();
  const remember = (key: string, body: string): void => {
    if (body.length > MEMORY_BODY_LIMIT) return;
    memory.delete(key);
    memory.set(key, { body, expires: Date.now() + MEMORY_TTL_MS });
    if (memory.size > MEMORY_ENTRIES) {
      const oldest: string | undefined = memory.keys().next().value;
      if (oldest !== undefined) memory.delete(oldest);
    }
  };
  const loadOne = async (input: CachedQueryRequest): Promise<Record<string, unknown>[] | null> => {
    const hot: MemoryEntry | undefined = memory.get(input.key);
    if (hot !== undefined && hot.expires > Date.now()) return parseRows(hot.body);
    memory.delete(input.key);
    const cacheRequest: Request = new Request(`${CACHE_ORIGIN}${encodeURIComponent(input.key)}`);
    const regional: Response | undefined = await input.request.cache.match(cacheRequest);
    if (regional?.ok) {
      const body: string = await regional.text();
      const parsed: Record<string, unknown>[] | null = parseRows(body);
      if (parsed !== null) {
        remember(input.key, body);
        return parsed;
      }
      await input.request.cache.delete(cacheRequest);
    }
    const durable: string | null = await input.request.kv.get(input.key);
    if (durable !== null) {
      const parsed: Record<string, unknown>[] | null = parseRows(durable);
      if (parsed !== null) {
        remember(input.key, durable);
        await input.request.cache.put(cacheRequest, cacheResponse(durable)).catch(() => undefined);
        return parsed;
      }
    }
    if (!input.request.warm) return null;
    const rows: Record<string, unknown>[] = await input.request.execute(input.sql());
    const body: string = JSON.stringify(rows);
    await input.request.kv.put(input.key, body, { expirationTtl: TTL_SECONDS });
    await input.request.cache.put(cacheRequest, cacheResponse(body)).catch(() => undefined);
    remember(input.key, body);
    return rows;
  };
  const shared = (input: CachedQueryRequest): Promise<Record<string, unknown>[] | null> => {
    const flightKey: string = `${input.key}:${input.request.warm}`;
    const existing: Promise<Record<string, unknown>[] | null> | undefined = pending.get(flightKey);
    if (existing !== undefined) return existing;
    const work: Promise<Record<string, unknown>[] | null> = loadOne(input).finally(() => {
      pending.delete(flightKey);
    });
    pending.set(flightKey, work);
    return work;
  };
  return {
    load: async (request) => {
      const horses: string =
        request.query.scope.kind === "horseJockey"
          ? [...new Set(request.query.horseIds)].toSorted().join(",")
          : "";
      const key: string = [
        request.query.config.R2_SQL_ACCOUNT_ID,
        request.query.config.R2_SQL_NAMESPACE,
        partnershipCacheKey(request.query.scope),
        horses,
      ]
        .map(encodeURIComponent)
        .join(":");
      const [targetRaces, values] = await Promise.all([
        shared({
          key: `${key}:targets`,
          request,
          sql: () => buildPartnershipTargetRacesQuery(request.query),
        }),
        shared({
          key: `${key}:values`,
          request,
          sql: () => buildPartnershipCountsQuery(request.query),
        }),
      ]);
      return targetRaces === null || values === null ? null : { targetRaces, values };
    },
  };
};
