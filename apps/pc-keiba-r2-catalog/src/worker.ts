import {
  cacheRequestFor,
  featureDescriptor,
  jsonRowsResponse,
  kvKeyFor,
  parsePositiveSeconds,
  populateCacheApi,
  populateCaches,
  purgeDescriptors,
  readKvRows,
  type CacheDescriptor,
} from "./cache";
import { coalesce } from "./inflight";
import { normaliseCatalogRaceKeyRow, normaliseDailyRaceEntryRow } from "./normalise";
import {
  buildRaceFeaturesQuery,
  buildRaceKeysQuery,
  executeR2Sql,
  R2SqlQueryError,
} from "./r2-sql";
import { normaliseRunningStyleRows, numberOrNull } from "./running-style-response";
import { buildRunningStyleFeaturesQuery } from "./running-style-sql";
import type {
  Env,
  RaceFeatureFilters,
  RunningStyleFeatureFilters,
  RunningStyleSourceScope,
  SourceScope,
  WorkerDependencies,
} from "./types";

const DATE_PATTERN = /^\d{8}$/u;
const CODE_PATTERN = /^\d{1,2}$/u;
const FEATURE_SOURCES: ReadonlyArray<SourceScope> = ["all", "jra", "nar", "ban-ei"];
// R2 SQL error code for "query expression too deep: nesting depth exceeds
// the protocol's limit" -- see running-style-feature-ctes.ts's
// includeOrderBy docstring for why this happens and only for large-enough
// source data volumes.
const R2_SQL_EXPRESSION_TOO_DEEP_CODE = 40018;

// Operator-facing description of a failed upstream call. `code` is the
// Cloudflare R2 SQL error code when one was returned, null otherwise.
interface CatalogFailure {
  code: number | string | null;
  detail: string;
}

const jsonResponse = (value: unknown, status = 200): Response =>
  Response.json(value, {
    headers: { "Cache-Control": "no-store" },
    status,
  });

const requireDate = (url: URL): string => {
  const date = url.searchParams.get("date") ?? "";
  if (!DATE_PATTERN.test(date)) throw new Error("date must match YYYYMMDD");
  return date;
};

const parseSource = (url: URL, required: boolean): SourceScope | undefined => {
  const value = url.searchParams.get("source");
  if (value === null && !required) return undefined;
  if (value === "all" || value === "jra" || value === "nar" || value === "ban-ei") return value;
  throw new Error("source must be jra, nar, ban-ei, or all");
};

const parseCode = (url: URL, name: string): string | undefined => {
  const value = url.searchParams.get(name);
  if (value === null) return undefined;
  if (!CODE_PATTERN.test(value)) throw new Error(`${name} must contain one or two digits`);
  return value.padStart(2, "0");
};

const parseFeatureFilters = (url: URL): RaceFeatureFilters => {
  const source = parseSource(url, true);
  if (source === undefined) throw new Error("source is required");
  return {
    date: requireDate(url),
    keibajoCode: parseCode(url, "keibajoCode"),
    raceBango: parseCode(url, "raceBango"),
    source,
  };
};

const parseRunningStyleSource = (url: URL): RunningStyleSourceScope => {
  const value = url.searchParams.get("source");
  if (value === "jra" || value === "nar" || value === "ban-ei") return value;
  throw new Error("source must be jra, nar, or ban-ei");
};

const requireCode = (url: URL, name: string): string => {
  const code = parseCode(url, name);
  if (code === undefined) throw new Error(`${name} is required`);
  return code;
};

// raceBango is optional: omitting it builds every race at the venue in one
// pass. The decade-wide history CTEs depend only on date + source, so a
// venue-level build pays that scan once instead of once per race.
const parseRunningStyleFilters = (url: URL): RunningStyleFeatureFilters => ({
  date: requireDate(url),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: parseCode(url, "raceBango"),
  source: parseRunningStyleSource(url),
});

const runningStyleCoalesceKey = (filters: RunningStyleFeatureFilters): string =>
  `running-style:${filters.source}:${filters.date}:${filters.keibajoCode}:${filters.raceBango ?? "all"}`;

const cachedArrayResponse = async (
  descriptor: CacheDescriptor,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response | null> => {
  const request = cacheRequestFor(descriptor);
  const cached = await dependencies.cache.match(request);
  if (cached) {
    const response = new Response(cached.body, cached);
    response.headers.set("X-Catalog-Cache", "cache-api");
    return response;
  }
  const kvBody = await readKvRows(env.CATALOG_KV, kvKeyFor(descriptor));
  if (kvBody === null) return null;
  const cacheTtl = parsePositiveSeconds(env.CACHE_TTL_SECONDS, 60);
  await Promise.allSettled([populateCacheApi(dependencies.cache, request, kvBody, cacheTtl)]);
  return jsonRowsResponse(kvBody, "kv");
};

const queryAndCache = async (
  descriptor: CacheDescriptor,
  env: Env,
  dependencies: WorkerDependencies,
  query: string,
  normalise: (row: Record<string, unknown>) => unknown,
): Promise<Response> => {
  const cached = await cachedArrayResponse(descriptor, env, dependencies);
  if (cached) return cached;
  const rows = await executeR2Sql(env, query, dependencies.fetchImpl);
  const body = JSON.stringify({ rows: rows.map(normalise) });
  await populateCaches(
    dependencies.cache,
    env.CATALOG_KV,
    descriptor,
    body,
    parsePositiveSeconds(env.CACHE_TTL_SECONDS, 60),
    parsePositiveSeconds(env.KV_TTL_SECONDS, 600),
  );
  return jsonRowsResponse(body, "r2-sql");
};

const handleRaceKeys = (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const date = requireDate(url);
  const descriptor: CacheDescriptor = { date, kind: "race-keys" };
  return queryAndCache(
    descriptor,
    env,
    dependencies,
    buildRaceKeysQuery(env, date),
    normaliseCatalogRaceKeyRow,
  );
};

const handleRaceFeatures = (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseFeatureFilters(url);
  return queryAndCache(
    featureDescriptor(filters),
    env,
    dependencies,
    buildRaceFeaturesQuery(env, filters),
    normaliseDailyRaceEntryRow,
  );
};

const isExpressionTooDeepError = (error: unknown): boolean =>
  error instanceof R2SqlQueryError && error.code === R2_SQL_EXPRESSION_TOO_DEEP_CODE;

const failureCode = (error: unknown): number | string | null => {
  if (!(error instanceof R2SqlQueryError)) return null;
  return error.code ?? null;
};

const describeFailure = (error: unknown): CatalogFailure => ({
  code: failureCode(error),
  detail: error instanceof Error ? error.message : String(error),
});

// R2 SQL may project race_bango as a JSON number rather than a zero-padded
// string. Collapsing every numeric value to "" would put all races in one
// bucket and interleave them by umaban, so normalise both shapes to the same
// zero-padded string form before comparing.
const raceBangoKey = (row: Record<string, unknown>): string => {
  const value = row.race_bango;
  if (typeof value === "string" || typeof value === "number") return String(value).padStart(2, "0");
  return "";
};

// Matches the server-side "race_bango, umaban" ordering. For a single-race
// build every race_bango is identical, so this degrades to umaban order.
const compareByRaceThenUmaban = (
  left: Record<string, unknown>,
  right: Record<string, unknown>,
): number => {
  const byRace = raceBangoKey(left).localeCompare(raceBangoKey(right));
  if (byRace !== 0) return byRace;
  return (numberOrNull(left.umaban) ?? 0) - (numberOrNull(right.umaban) ?? 0);
};

const runningStyleBody = async (
  env: Env,
  dependencies: WorkerDependencies,
  filters: RunningStyleFeatureFilters,
): Promise<string> => {
  try {
    const rows = await executeR2Sql(
      env,
      buildRunningStyleFeaturesQuery(env, filters, true),
      dependencies.fetchImpl,
    );
    return JSON.stringify(normaliseRunningStyleRows(rows));
  } catch (error) {
    if (!isExpressionTooDeepError(error)) throw error;
    // R2 SQL's distributed Top-K sort (ORDER BY + LIMIT together) can exceed
    // the plan-depth protocol limit for a data volume as large as JRA's --
    // retry once without ORDER BY (still LIMIT) and sort the small result
    // here instead. Identical row set and values; only where the sort
    // happens changes.
    const rows = await executeR2Sql(
      env,
      buildRunningStyleFeaturesQuery(env, filters, false),
      dependencies.fetchImpl,
    );
    return JSON.stringify(normaliseRunningStyleRows([...rows].sort(compareByRaceThenUmaban)));
  }
};

const handleRunningStyleFeatures = async (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseRunningStyleFilters(url);
  const body = await coalesce(runningStyleCoalesceKey(filters), () =>
    runningStyleBody(env, dependencies, filters),
  );
  return new Response(body, {
    headers: { "Cache-Control": "no-store", "Content-Type": "application/json; charset=utf-8" },
  });
};

const purgeTargets = (url: URL): CacheDescriptor[] => {
  const date = requireDate(url);
  const source = parseSource(url, false);
  const keibajoCode = parseCode(url, "keibajoCode");
  const raceBango = parseCode(url, "raceBango");
  const featureSources = source === undefined ? FEATURE_SOURCES : [source];
  const features = featureSources.map(
    (featureSource): CacheDescriptor => ({
      date,
      keibajoCode,
      kind: "race-features",
      raceBango,
      source: featureSource,
    }),
  );
  return source === undefined && keibajoCode === undefined && raceBango === undefined
    ? [{ date, kind: "race-keys" }, ...features]
    : features;
};

const handlePurge = async (
  request: Request,
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const expected = env.ADMIN_TOKEN;
  if (!expected || request.headers.get("Authorization") !== `Bearer ${expected}`) {
    return jsonResponse({ error: "unauthorized" }, 401);
  }
  const purged = await purgeDescriptors(dependencies.cache, env.CATALOG_KV, purgeTargets(url));
  return jsonResponse({ ok: true, purged });
};

export const handleRequest = async (
  request: Request,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const url = new URL(request.url);
  try {
    if (request.method === "GET" && url.pathname === "/health") {
      return jsonResponse({ name: "pc-keiba-r2-catalog", ok: true });
    }
    if (request.method === "GET" && url.pathname === "/v1/race-keys") {
      return await handleRaceKeys(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/race-features") {
      return await handleRaceFeatures(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/running-style-features") {
      return await handleRunningStyleFeatures(url, env, dependencies);
    }
    if (
      (request.method === "POST" || request.method === "DELETE") &&
      url.pathname === "/admin/purge"
    ) {
      return await handlePurge(request, url, env, dependencies);
    }
    return jsonResponse({ error: "not_found" }, 404);
  } catch (error) {
    if (
      error instanceof Error &&
      (error.message.includes("must") || error.message.includes("required"))
    ) {
      return jsonResponse({ error: error.message }, 400);
    }
    // Surface the underlying R2 SQL failure. Without this the only operator
    // signal is "HTTP 502", which is what made the 2026-08-08 running-style
    // stall undiagnosable. Neither field can carry credentials: `detail` is
    // built from Cloudflare's own error payload plus this Worker's own
    // messages, and the bearer token is never interpolated into either
    // (see r2-sql.ts::executeR2Sql).
    const failure = describeFailure(error);
    console.error(
      "[pc-keiba-r2-catalog] request failed",
      JSON.stringify({
        code: failure.code,
        detail: failure.detail,
        path: url.pathname,
        search: url.search,
      }),
    );
    return jsonResponse(
      { code: failure.code, detail: failure.detail, error: "r2_sql_unavailable" },
      502,
    );
  }
};

const worker = {
  fetch(request: Request, env: Env): Promise<Response> {
    return handleRequest(request, env, { cache: caches.default, fetchImpl: fetch });
  },
};

export default worker;
