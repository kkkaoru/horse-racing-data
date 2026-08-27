import {
  cacheRequestFor,
  conditionHistoryStatsDescriptor,
  featureDescriptor,
  heatmapStatsDescriptor,
  horseRaceResultsDescriptor,
  jsonRowsResponse,
  kvKeyFor,
  parsePositiveSeconds,
  populateCacheApi,
  populateCaches,
  purgeDescriptors,
  raceEntityRecentResultsDescriptor,
  readKvConditionHistoryStats,
  readKvHeatmapStats,
  readKvRaceEntityPage,
  readKvRows,
  trainingDescriptor,
  type CacheDescriptor,
} from "./cache";
import { coalesce } from "./inflight";
import { normaliseCatalogRaceKeyRow, normaliseDailyRaceEntryRow } from "./normalise";
import {
  buildBulkFreshRaceEntriesQuery,
  buildFreshRaceEntriesQuery,
  buildRaceFeaturesQuery,
  buildRaceKeysQuery,
  executeR2Sql,
  normaliseBulkFreshRaceEntries,
  normaliseFreshRaceEntries,
  R2SqlQueryError,
} from "./r2-sql";
import { normaliseRunningStyleRows, numberOrNull } from "./running-style-response";
import { buildRaceTrainingsQuery, normaliseRaceTrainingRow } from "./race-training";
import { buildRunningStyleFeaturesQuery } from "./running-style-sql";
import {
  readEntityCatalogHistory,
  readEntityCatalogManifest,
  readEntityCatalogTarget,
} from "./race-entity-catalog-store";
import {
  buildRaceEntityHistoryQuery,
  buildRaceEntityPage,
  buildRaceEntityTargetQuery,
  normaliseRaceEntityHistoryRow,
  normaliseRaceEntityTarget,
  parseRaceEntityCursor,
} from "./race-entity-recent-results";
import {
  buildHorseRaceResultsQuery,
  normaliseHorseRaceResultRow,
  uniqueHorseRaceResults,
} from "./horse-race-results";
import {
  buildConditionFinishPositionStatsQuery,
  buildConditionFrameStatsQuery,
  buildConditionRaceTimeStatsQuery,
  buildConditionTargetRacesQuery,
  buildConditionWeightClassStatsQuery,
  isBanEiKeibajo,
  normaliseConditionHistoryStatsPayload,
} from "./condition-history-stats";
import {
  buildWinRateHeatmapBloodlineQuery,
  buildWinRateHeatmapSimilarQuery,
  normaliseWinRateHeatmapStatsPayload,
} from "./win-rate-heatmap-stats";
import type {
  BulkFreshRaceEntryFilters,
  CatalogSource,
  Env,
  FreshRaceEntryFilters,
  HorseRaceResultsFilters,
  HorseRaceResultsSourceScope,
  KvStore,
  RaceFeatureFilters,
  RaceEntityRecentResultsFilters,
  RaceEntityType,
  RaceTrainingFilters,
  RunningStyleFeatureFilters,
  RunningStyleSourceScope,
  SourceScope,
  WinRateHeatmapStatsFilters,
  WorkerDependencies,
} from "./types";

const DATE_PATTERN = /^\d{8}$/u;
const CODE_PATTERN = /^\d{1,2}$/u;
const YEAR_PATTERN: RegExp = /^\d{4}$/u;
const ENTITY_BUCKET_PATTERN: RegExp = /^[0-9a-f]$/u;
const FEATURE_SOURCES: ReadonlyArray<SourceScope> = ["all", "jra", "nar", "ban-ei"];
const DEFAULT_STATS_YEARS: number = 10;
const MIN_STATS_YEARS: number = 1;
const MAX_STATS_YEARS: number = 50;
const RACE_ENTITY_TYPES: ReadonlySet<string> = new Set(["horse", "jockey", "trainer", "owner"]);
const RACE_ENTITY_DEFAULT_LIMITS: ReadonlyMap<RaceEntityType, number> = new Map([
  ["horse", 5],
  ["jockey", 10],
  ["trainer", 10],
  ["owner", 10],
]);
const RACE_ENTITY_MAX_LIMITS: ReadonlyMap<RaceEntityType, number> = new Map([
  ["horse", 20],
  ["jockey", 30],
  ["trainer", 30],
  ["owner", 30],
]);
const RACE_ENTITY_CACHE_API_TTL_SECONDS: number = 60 * 60;
const RACE_ENTITY_KV_TTL_SECONDS: number = 6 * 60 * 60;
const HEATMAP_CACHE_API_TTL_SECONDS = 36 * 60 * 60;
const HEATMAP_KV_TTL_SECONDS = 36 * 60 * 60;
// R2 SQL error code for "query expression too deep: nesting depth exceeds
// the protocol's limit" -- see running-style-feature-ctes.ts's
// includeOrderBy docstring for why this happens and only for large-enough
// source data volumes.
const R2_SQL_EXPRESSION_TOO_DEEP_CODE = 40018;
// R2 SQL returns 70200 when a valid distributed query exhausts an internal
// execution resource. A single target horse uses the same feature contract
// with a much smaller final join, so only this code is eligible for the
// bounded per-horse recovery below.
const R2_SQL_EXECUTION_RESOURCE_CODE = 70200;
// R2 SQL can surface a race-specific query-plan rejection as HTTP 422/60104.
// The per-horse form has a materially smaller plan and is safe to retry.
const R2_SQL_QUERY_PLAN_REJECTED_CODE = 60104;
const RUNNING_STYLE_RACE_QUERY_TIMEOUT_MS = 30_000;
const RUNNING_STYLE_HORSE_QUERY_TIMEOUT_MS = 60_000;
const RUNNING_STYLE_UMABAN_BATCHES: ReadonlyArray<ReadonlyArray<number>> = [
  [1, 2, 3, 4, 5, 6],
  [7, 8, 9, 10, 11, 12],
  [13, 14, 15, 16, 17, 18],
];
const FRESH_RACE_ENTRIES_PATH: string = "/v1/internal/fresh-race-entries";
const BULK_FRESH_RACE_ENTRIES_PATH: string = "/v1/internal/fresh-race-entries-bulk";
const AUTHORIZATION_HEADER: string = "Authorization";
const BEARER_PREFIX: string = "Bearer ";
const SHA_256_ALGORITHM: string = "SHA-256";

// Operator-facing description of a failed upstream call. `code` is the
// Cloudflare R2 SQL error code when one was returned, null otherwise.
interface CatalogFailure {
  code: number | string | null;
  detail: string;
}

interface RunningStyleQueryParams {
  dependencies: WorkerDependencies;
  env: Env;
  filters: RunningStyleFeatureFilters;
}

interface RunningStyleHorseBatchParams extends RunningStyleQueryParams {
  umabans: ReadonlyArray<number>;
}

class RaceEntityRequestError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status: number) {
    super(message);
    this.code = code;
    this.status = status;
  }
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

const parseUmaban = (url: URL): number | undefined => {
  const value = url.searchParams.get("umaban");
  if (value === null) return undefined;
  const umaban = Number(value);
  if (!Number.isInteger(umaban) || umaban < 1 || umaban > 18) {
    throw new Error("umaban must be an integer from 1 to 18");
  }
  return umaban;
};

// raceBango is optional: omitting it builds every race at the venue in one
// pass. The decade-wide history CTEs depend only on date + source, so a
// venue-level build pays that scan once instead of once per race.
const parseRunningStyleFilters = (url: URL): RunningStyleFeatureFilters => ({
  date: requireDate(url),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: parseCode(url, "raceBango"),
  source: parseRunningStyleSource(url),
  umaban: parseUmaban(url),
  gradeCode: url.searchParams.get("gradeCode"),
});

const parseRaceTrainingFilters = (url: URL): RaceTrainingFilters => ({
  date: requireDate(url),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: requireCode(url, "raceBango"),
});

const parseFreshRaceEntryFilters = (url: URL): FreshRaceEntryFilters => ({
  date: requireDate(url),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: requireCode(url, "raceBango"),
  source: parseRunningStyleSource(url),
});

const parseBulkFreshRaceEntryFilters = (url: URL): BulkFreshRaceEntryFilters => ({
  date: requireDate(url),
  source: parseRunningStyleSource(url),
});

const parseBearerToken = (request: Request): string | null => {
  const authorization = request.headers.get(AUTHORIZATION_HEADER);
  return authorization?.startsWith(BEARER_PREFIX) === true
    ? authorization.slice(BEARER_PREFIX.length)
    : null;
};

const digestSecret = (value: string): Promise<ArrayBuffer> =>
  crypto.subtle.digest(SHA_256_ALGORITHM, new TextEncoder().encode(value));

const timingSafeEqualDigests = (left: ArrayBuffer, right: ArrayBuffer): boolean => {
  // Both inputs are fixed-width SHA-256 digests, so this reduction compares
  // every byte without leaking either secret's original length.
  const rightBytes = new Uint8Array(right);
  const mismatch = new Uint8Array(left).reduce(
    (difference, byte, index) => difference | (byte ^ (rightBytes[index] ?? 0)),
    0,
  );
  return mismatch === 0;
};

const isFreshRaceEntriesAuthorized = async (request: Request, env: Env): Promise<boolean> => {
  const expected = env.FINISH_POSITION_ATTESTATION_TOKEN;
  const provided = parseBearerToken(request);
  if (expected === undefined || expected.length === 0 || provided === null || provided.length === 0)
    return false;
  const [providedDigest, expectedDigest] = await Promise.all([
    digestSecret(provided),
    digestSecret(expected),
  ]);
  return timingSafeEqualDigests(providedDigest, expectedDigest);
};

const compactUtcDate = (timestamp: number): string =>
  new Date(timestamp).toISOString().slice(0, 10).replaceAll("-", "");

const parseHeatmapDate = (url: URL): string => {
  const year = url.searchParams.get("year") ?? "";
  const month = url.searchParams.get("month") ?? "";
  const day = url.searchParams.get("day") ?? "";
  if (!YEAR_PATTERN.test(year)) throw new Error("year must match YYYY");
  if (!CODE_PATTERN.test(month)) throw new Error("month must contain one or two digits");
  if (!CODE_PATTERN.test(day)) throw new Error("day must contain one or two digits");
  const date = `${year}${month.padStart(2, "0")}${day.padStart(2, "0")}`;
  const timestamp = Date.UTC(
    Number(date.slice(0, 4)),
    Number(date.slice(4, 6)) - 1,
    Number(date.slice(6, 8)),
  );
  if (compactUtcDate(timestamp) !== date) {
    throw new Error("year, month, and day must be a valid calendar date");
  }
  return date;
};

const parseHeatmapSource = (url: URL): CatalogSource => {
  const value = url.searchParams.get("source");
  if (value === null) throw new Error("source is required");
  if (value === "jra" || value === "nar") return value;
  throw new Error("source must be jra or nar");
};

const parseIncludeFlag = (url: URL, name: string): boolean => {
  const value = url.searchParams.get(name);
  if (value === null || value === "1") return true;
  if (value === "0") return false;
  throw new Error(`${name} must be 0 or 1`);
};

const parseYears = (url: URL): number => {
  const value = url.searchParams.get("years");
  if (value === null) return DEFAULT_STATS_YEARS;
  const years = Number(value);
  if (!Number.isInteger(years) || years < MIN_STATS_YEARS || years > MAX_STATS_YEARS) {
    throw new Error("years must be an integer from 1 to 50");
  }
  return years;
};

const parseOptionalIncludeFlag = (url: URL, name: string): boolean => {
  const value = url.searchParams.get(name);
  if (value === null || value === "0") return false;
  if (value === "1") return true;
  throw new Error(`${name} must be 0 or 1`);
};

const parseWinRateHeatmapFilters = (url: URL): WinRateHeatmapStatsFilters => ({
  date: parseHeatmapDate(url),
  includeAge: parseOptionalIncludeFlag(url, "includeAge"),
  includeClass: parseOptionalIncludeFlag(url, "includeClass"),
  includeConditionKey: parseOptionalIncludeFlag(url, "includeConditionKey"),
  includeDistance: parseIncludeFlag(url, "includeDistance"),
  includeGrade: parseOptionalIncludeFlag(url, "includeGrade"),
  includeJockeyFrame: parseOptionalIncludeFlag(url, "includeJockeyFrame"),
  includeOwner: parseOptionalIncludeFlag(url, "includeOwner"),
  includeRaceTitle: parseOptionalIncludeFlag(url, "includeRaceTitle"),
  includeSurface: parseIncludeFlag(url, "includeSurface"),
  includeTrackCode: parseOptionalIncludeFlag(url, "includeTrackCode"),
  includeTurn: parseIncludeFlag(url, "includeTurn"),
  includeVenue: parseIncludeFlag(url, "includeVenue"),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: requireCode(url, "raceNumber"),
  source: parseHeatmapSource(url),
  years: parseYears(url),
});

const parseRaceBangoOrNumber = (url: URL): string => {
  if (url.searchParams.get("raceBango") !== null) return requireCode(url, "raceBango");
  if (url.searchParams.get("raceNumber") !== null) return requireCode(url, "raceNumber");
  throw new Error("raceBango is required");
};

const parseHorseRaceResultsSourceScope = (url: URL): HorseRaceResultsSourceScope => {
  const value = url.searchParams.get("sourceScope");
  if (value === null || value === "all") return "all";
  if (value === "jra" || value === "nar") return value;
  throw new Error("sourceScope must be all, jra, or nar");
};

const parseHorseRaceResultsFilters = (url: URL): HorseRaceResultsFilters => ({
  date: requireDate(url),
  keibajoCode: requireCode(url, "keibajoCode"),
  raceBango: parseRaceBangoOrNumber(url),
  source: parseHeatmapSource(url),
  sourceScope: parseHorseRaceResultsSourceScope(url),
});

const isRaceEntityType = (value: string): value is RaceEntityType => RACE_ENTITY_TYPES.has(value);

const parseRaceEntityType = (url: URL): RaceEntityType => {
  const value = url.searchParams.get("entityType");
  if (value === null || !isRaceEntityType(value)) {
    throw new RaceEntityRequestError(
      "INVALID_ENTITY_TYPE",
      "entityType must be horse, jockey, trainer, or owner.",
      400,
    );
  }
  return value;
};

const raceEntityLimit = (url: URL, entityType: RaceEntityType): number => {
  const defaultLimit = RACE_ENTITY_DEFAULT_LIMITS.get(entityType);
  const maxLimit = RACE_ENTITY_MAX_LIMITS.get(entityType);
  if (defaultLimit === undefined || maxLimit === undefined) {
    throw new RaceEntityRequestError("INVALID_ENTITY_TYPE", "Entity limit policy is missing.", 400);
  }
  const raw = url.searchParams.get("limit");
  if (raw === null) return defaultLimit;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 1 || parsed > maxLimit) {
    throw new RaceEntityRequestError(
      "INVALID_LIMIT",
      `limit must be an integer from 1 to ${String(maxLimit)} for ${entityType}.`,
      400,
    );
  }
  return parsed;
};

const parseRaceEntityFilters = (url: URL): RaceEntityRecentResultsFilters => {
  const entityType = parseRaceEntityType(url);
  const horseNumber = requireCode(url, "horseNumber");
  const cursor = url.searchParams.get("cursor");
  return {
    cursor: cursor === null || cursor.length === 0 ? null : cursor,
    date: requireDate(url),
    entityType,
    horseNumber,
    keibajoCode: requireCode(url, "keibajoCode"),
    limit: raceEntityLimit(url, entityType),
    raceBango: parseRaceBangoOrNumber(url),
    source: parseHeatmapSource(url),
  };
};

const runningStyleCoalesceKey = (filters: RunningStyleFeatureFilters): string =>
  `running-style:${filters.source}:${filters.date}:${filters.keibajoCode}:${filters.raceBango ?? "all"}:${filters.umaban === undefined ? "all" : String(filters.umaban)}`;

const cachedCatalogResponse = async (
  descriptor: CacheDescriptor,
  env: Env,
  dependencies: WorkerDependencies,
  readKv: (kv: KvStore, key: string) => Promise<string | null>,
): Promise<Response | null> => {
  const request = cacheRequestFor(descriptor);
  const cached = await dependencies.cache.match(request);
  if (cached) {
    const response = new Response(cached.body, cached);
    response.headers.set("X-Catalog-Cache", "cache-api");
    return response;
  }
  const kvBody = await readKv(env.CATALOG_KV, kvKeyFor(descriptor));
  if (kvBody === null) return null;
  const cacheTtl = parsePositiveSeconds(env.CACHE_TTL_SECONDS, 60);
  await Promise.allSettled([populateCacheApi(dependencies.cache, request, kvBody, cacheTtl)]);
  return jsonRowsResponse(kvBody, "kv");
};

const heatmapCoalesceKey = (filters: WinRateHeatmapStatsFilters): string =>
  [
    "heatmap",
    filters.source,
    filters.date,
    filters.keibajoCode,
    filters.raceBango,
    String(filters.years),
    filters.includeVenue ? "1" : "0",
    filters.includeDistance ? "1" : "0",
    filters.includeSurface ? "1" : "0",
    filters.includeTurn ? "1" : "0",
    filters.includeOwner === true ? "1" : "0",
    filters.includeJockeyFrame === true ? "1" : "0",
    filters.includeGrade === true ? "1" : "0",
    filters.includeTrackCode === true ? "1" : "0",
    filters.includeAge === true ? "1" : "0",
    filters.includeClass === true ? "1" : "0",
    filters.includeConditionKey === true ? "1" : "0",
    filters.includeRaceTitle === true ? "1" : "0",
  ].join(":");

const conditionHistoryCoalesceKey = (filters: WinRateHeatmapStatsFilters): string =>
  [
    "condition-history",
    filters.source,
    filters.date,
    filters.keibajoCode,
    filters.raceBango,
    String(filters.years),
    filters.includeVenue ? "1" : "0",
    filters.includeDistance ? "1" : "0",
    filters.includeSurface ? "1" : "0",
    filters.includeTurn ? "1" : "0",
    filters.includeGrade === true ? "1" : "0",
    filters.includeTrackCode === true ? "1" : "0",
    filters.includeAge === true ? "1" : "0",
    filters.includeClass === true ? "1" : "0",
    filters.includeConditionKey === true ? "1" : "0",
    filters.includeRaceTitle === true ? "1" : "0",
  ].join(":");

const heatmapStatsBody = async (
  env: Env,
  dependencies: WorkerDependencies,
  filters: WinRateHeatmapStatsFilters,
): Promise<string> => {
  const [bloodlineRows, similarRows] = await Promise.all([
    executeR2Sql(env, buildWinRateHeatmapBloodlineQuery(env, filters), dependencies.fetchImpl),
    executeR2Sql(env, buildWinRateHeatmapSimilarQuery(env, filters), dependencies.fetchImpl),
  ]);
  return JSON.stringify(normaliseWinRateHeatmapStatsPayload({ bloodlineRows, similarRows }));
};

const handleWinRateHeatmapStats = async (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseWinRateHeatmapFilters(url);
  const descriptor = heatmapStatsDescriptor(filters);
  const cached = await cachedCatalogResponse(descriptor, env, dependencies, readKvHeatmapStats);
  if (cached) return cached;
  const body = await coalesce(heatmapCoalesceKey(filters), () =>
    heatmapStatsBody(env, dependencies, filters),
  );
  await populateCaches(
    dependencies.cache,
    env.CATALOG_KV,
    descriptor,
    body,
    HEATMAP_CACHE_API_TTL_SECONDS,
    HEATMAP_KV_TTL_SECONDS,
  );
  return jsonRowsResponse(body, "r2-sql");
};

const queryAndCacheRows = async (
  descriptor: CacheDescriptor,
  env: Env,
  dependencies: WorkerDependencies,
  query: string,
  toRows: (rows: ReadonlyArray<Record<string, unknown>>) => unknown[],
): Promise<Response> => {
  const cached = await cachedCatalogResponse(descriptor, env, dependencies, readKvRows);
  if (cached) return cached;
  const rows = await executeR2Sql(env, query, dependencies.fetchImpl);
  const body = JSON.stringify({ rows: toRows(rows) });
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

const queryAndCache = async (
  descriptor: CacheDescriptor,
  env: Env,
  dependencies: WorkerDependencies,
  query: string,
  normalise: (row: Record<string, unknown>) => unknown,
): Promise<Response> =>
  queryAndCacheRows(descriptor, env, dependencies, query, (rows) => rows.map(normalise));

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

const handleFreshRaceEntries = async (
  request: Request,
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  if (!(await isFreshRaceEntriesAuthorized(request, env))) {
    return jsonResponse({ error: "unauthorized" }, 401);
  }
  const filters = parseFreshRaceEntryFilters(url);
  const rows = await executeR2Sql(
    env,
    buildFreshRaceEntriesQuery(env, filters),
    dependencies.fetchImpl,
  );
  const entries = normaliseFreshRaceEntries(rows);
  return jsonResponse({ ...filters, entries });
};

const handleBulkFreshRaceEntries = async (
  request: Request,
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  if (!(await isFreshRaceEntriesAuthorized(request, env))) {
    return jsonResponse({ error: "unauthorized" }, 401);
  }
  const filters = parseBulkFreshRaceEntryFilters(url);
  const rows = await executeR2Sql(
    env,
    buildBulkFreshRaceEntriesQuery(env, filters),
    dependencies.fetchImpl,
  );
  const entries = normaliseBulkFreshRaceEntries(rows, filters.source);
  return jsonResponse({ ...filters, entries });
};

const handleRaceTrainings = (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseRaceTrainingFilters(url);
  return queryAndCache(
    trainingDescriptor(filters),
    env,
    dependencies,
    buildRaceTrainingsQuery(env, filters),
    normaliseRaceTrainingRow,
  );
};

const handleHorseRaceResults = (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseHorseRaceResultsFilters(url);
  return queryAndCacheRows(
    horseRaceResultsDescriptor(filters),
    env,
    dependencies,
    buildHorseRaceResultsQuery(env, filters),
    (rows) => uniqueHorseRaceResults(rows.map(normaliseHorseRaceResultRow)),
  );
};

const entityNotFoundCode = (entityType: RaceEntityType): string =>
  `${entityType.toUpperCase()}_NOT_FOUND`;

const raceEntityCursorSecret = (env: Env): string => {
  const secret = env.RACE_ENTITY_CURSOR_SECRET;
  if (secret === undefined || secret.length < 32) {
    throw new RaceEntityRequestError(
      "UPSTREAM_ERROR",
      "The race entity cursor signing secret is unavailable.",
      502,
    );
  }
  return secret;
};

const handleRaceEntityRecentResults = async (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseRaceEntityFilters(url);
  const descriptor = raceEntityRecentResultsDescriptor(filters);
  const cached = await cachedCatalogResponse(descriptor, env, dependencies, readKvRaceEntityPage);
  if (cached) return cached;
  const cursorSecret = raceEntityCursorSecret(env);
  const catalogManifest =
    env.CATALOG_OBJECTS === undefined ? null : await readEntityCatalogManifest(env.CATALOG_OBJECTS);
  const catalogTarget =
    env.CATALOG_OBJECTS === undefined || catalogManifest === null
      ? null
      : await readEntityCatalogTarget(env.CATALOG_OBJECTS, catalogManifest, filters);
  const targetRows =
    catalogManifest === null
      ? await executeR2Sql(env, buildRaceEntityTargetQuery(env, filters), dependencies.fetchImpl)
      : [];
  const targetRow = targetRows[0];
  if (catalogTarget === null && targetRow === undefined) {
    throw new RaceEntityRequestError("RACE_NOT_FOUND", "The target race was not found.", 404);
  }
  const target = catalogTarget ?? normaliseRaceEntityTarget(targetRow ?? {});
  if (!target.runnerFound) {
    throw new RaceEntityRequestError("RUNNER_NOT_FOUND", "The target runner was not found.", 404);
  }
  if (target.horseId === null || /^0+$/u.test(target.horseId)) {
    throw new RaceEntityRequestError("HORSE_NOT_FOUND", "The runner horse ID is unavailable.", 404);
  }
  if (target.entityId === null || /^0+$/u.test(target.entityId)) {
    throw new RaceEntityRequestError(
      "ENTITY_ID_NOT_AVAILABLE",
      `A canonical ${filters.entityType} ID is not available for this runner.`,
      422,
    );
  }
  if (target.entityBucket === null || !ENTITY_BUCKET_PATTERN.test(target.entityBucket)) {
    throw new RaceEntityRequestError(
      "MALFORMED_TARGET_DATA",
      "The resolved entity history partition is malformed.",
      502,
    );
  }
  if (target.entityName === null) {
    throw new RaceEntityRequestError(
      entityNotFoundCode(filters.entityType),
      `The resolved ${filters.entityType} was not found.`,
      404,
    );
  }
  const resolvedTarget = {
    ...target,
    entityBucket: target.entityBucket,
    entityId: target.entityId,
  };
  const cursor = await parseRaceEntityCursor(filters, resolvedTarget.entityId, cursorSecret);
  if (cursor === "invalid") {
    throw new RaceEntityRequestError(
      "INVALID_CURSOR",
      "The cursor does not match the target race, entity, filter, or sort order.",
      400,
    );
  }
  const rows =
    env.CATALOG_OBJECTS !== undefined && catalogManifest !== null
      ? await readEntityCatalogHistory(
          env.CATALOG_OBJECTS,
          catalogManifest,
          filters,
          resolvedTarget,
          cursor,
        )
      : (
          await executeR2Sql(
            env,
            buildRaceEntityHistoryQuery(env, filters, resolvedTarget, cursor),
            dependencies.fetchImpl,
          )
        ).map((row) => {
          try {
            return normaliseRaceEntityHistoryRow(row);
          } catch {
            throw new RaceEntityRequestError(
              "MALFORMED_HISTORY_DATA",
              "The R2 Catalog history row is malformed.",
              502,
            );
          }
        });
  const body = JSON.stringify(
    await buildRaceEntityPage(filters, resolvedTarget, rows, cursorSecret),
  );
  await populateCaches(
    dependencies.cache,
    env.CATALOG_KV,
    descriptor,
    body,
    RACE_ENTITY_CACHE_API_TTL_SECONDS,
    RACE_ENTITY_KV_TTL_SECONDS,
  );
  return jsonRowsResponse(body, catalogManifest === null ? "r2-sql" : "r2-catalog-parquet");
};

const conditionHistoryStatsBody = async (
  env: Env,
  dependencies: WorkerDependencies,
  filters: WinRateHeatmapStatsFilters,
): Promise<string> => {
  // Workers outbound TCP cap is 6. Run R2 SQL in pairs, like heatmap
  // bloodline then similar, so a miss cannot stampede five sockets at once.
  const [frameRows, weightRows] = await Promise.all([
    executeR2Sql(env, buildConditionFrameStatsQuery(env, filters), dependencies.fetchImpl),
    executeR2Sql(
      env,
      buildConditionWeightClassStatsQuery({ env, filters, kind: "body" }),
      dependencies.fetchImpl,
    ),
  ]);
  const [carriedRows, finishRows] = await Promise.all([
    isBanEiKeibajo(filters.keibajoCode)
      ? Promise.resolve<Record<string, unknown>[]>([])
      : executeR2Sql(
          env,
          buildConditionWeightClassStatsQuery({ env, filters, kind: "carried" }),
          dependencies.fetchImpl,
        ),
    executeR2Sql(env, buildConditionFinishPositionStatsQuery(env, filters), dependencies.fetchImpl),
  ]);
  const [raceTimeRows, targetRaceRows] = await Promise.all([
    executeR2Sql(env, buildConditionRaceTimeStatsQuery(env, filters), dependencies.fetchImpl),
    executeR2Sql(env, buildConditionTargetRacesQuery(env, filters), dependencies.fetchImpl),
  ]);
  return JSON.stringify(
    normaliseConditionHistoryStatsPayload({
      carriedRows,
      finishRows,
      frameRows,
      raceTimeRows,
      targetRaceRows,
      weightRows,
    }),
  );
};

const handleConditionHistoryStats = async (
  url: URL,
  env: Env,
  dependencies: WorkerDependencies,
): Promise<Response> => {
  const filters = parseWinRateHeatmapFilters(url);
  const descriptor = conditionHistoryStatsDescriptor(filters);
  const cached = await cachedCatalogResponse(
    descriptor,
    env,
    dependencies,
    readKvConditionHistoryStats,
  );
  if (cached) return cached;
  const body = await coalesce(conditionHistoryCoalesceKey(filters), () =>
    conditionHistoryStatsBody(env, dependencies, filters),
  );
  await populateCaches(
    dependencies.cache,
    env.CATALOG_KV,
    descriptor,
    body,
    HEATMAP_CACHE_API_TTL_SECONDS,
    HEATMAP_KV_TTL_SECONDS,
  );
  return jsonRowsResponse(body, "r2-sql");
};

const isExpressionTooDeepError = (error: unknown): boolean =>
  error instanceof R2SqlQueryError && error.code === R2_SQL_EXPRESSION_TOO_DEEP_CODE;

const isExecutionResourceError = (error: unknown): boolean =>
  error instanceof R2SqlQueryError &&
  (error.code === R2_SQL_EXECUTION_RESOURCE_CODE || error.code === R2_SQL_QUERY_PLAN_REJECTED_CODE);

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

const executeRunningStyleRaceQuery = async ({
  dependencies,
  env,
  filters,
}: RunningStyleQueryParams): Promise<Record<string, unknown>[]> => {
  try {
    return await Promise.race([
      executeR2Sql(env, buildRunningStyleFeaturesQuery(env, filters, true), dependencies.fetchImpl),
      new Promise<never>((_, reject) =>
        setTimeout(
          () => reject(new Error("running-style race query timed out")),
          RUNNING_STYLE_RACE_QUERY_TIMEOUT_MS,
        ),
      ),
    ]);
  } catch (error) {
    if (
      !isExpressionTooDeepError(error) &&
      !isExecutionResourceError(error) &&
      !(error instanceof Error && error.message === "running-style race query timed out")
    ) {
      throw error;
    }
    if (
      canSplitRunningStyleRace(filters) &&
      (isExecutionResourceError(error) ||
        (error instanceof Error && error.message === "running-style race query timed out"))
    ) {
      return executeRunningStylePerHorse({ dependencies, env, filters }).then((rows) =>
        [...rows].sort(compareByRaceThenUmaban),
      );
    }
    if (!isExpressionTooDeepError(error)) throw error;
    // R2 SQL's distributed Top-K sort (ORDER BY + LIMIT together) can exceed
    // the plan-depth protocol limit for a data volume as large as JRA's --
    // retry once without ORDER BY (still LIMIT) and sort the small result in
    // this Worker instead.
    const rows = await executeR2Sql(
      env,
      buildRunningStyleFeaturesQuery(env, filters, false),
      dependencies.fetchImpl,
    );
    return [...rows].sort(compareByRaceThenUmaban);
  }
};

const executeRunningStyleHorseBatch = ({
  dependencies,
  env,
  filters,
  umabans,
}: RunningStyleHorseBatchParams): Promise<Record<string, unknown>[][]> =>
  Promise.all(
    umabans.map((umaban) =>
      Promise.race([
        executeR2Sql(
          env,
          buildRunningStyleFeaturesQuery(env, { ...filters, umaban }, false),
          dependencies.fetchImpl,
        ),
        new Promise<never>((_, reject) =>
          setTimeout(
            () =>
              reject(new Error(`running-style horse query timed out for umaban=${String(umaban)}`)),
            RUNNING_STYLE_HORSE_QUERY_TIMEOUT_MS,
          ),
        ),
      ]),
    ),
  );

const executeRunningStylePerHorse = ({
  dependencies,
  env,
  filters,
}: RunningStyleQueryParams): Promise<Record<string, unknown>[]> =>
  RUNNING_STYLE_UMABAN_BATCHES.reduce<Promise<Record<string, unknown>[]>>(
    async (rowsPromise, umabans) => {
      const rows = await rowsPromise;
      const batchRows = await executeRunningStyleHorseBatch({
        dependencies,
        env,
        filters,
        umabans,
      });
      return [...rows, ...batchRows.flat()];
    },
    Promise.resolve([]),
  );

const canSplitRunningStyleRace = (filters: RunningStyleFeatureFilters): boolean =>
  filters.raceBango !== undefined && filters.umaban === undefined;

const runningStyleBody = async (
  env: Env,
  dependencies: WorkerDependencies,
  filters: RunningStyleFeatureFilters,
): Promise<string> => {
  try {
    const rows = await executeRunningStyleRaceQuery({ dependencies, env, filters });
    return JSON.stringify(normaliseRunningStyleRows(rows));
  } catch (error) {
    if (!isExecutionResourceError(error) || !canSplitRunningStyleRace(filters)) throw error;
    // The fixed 1..18 domain keeps subrequest count finite. Six concurrent
    // queries stay within the Worker's outbound-connection budget, while
    // sequential batches avoid turning one resource failure into an R2 SQL
    // request burst. Promise.all is intentional: any horse failure rejects
    // the whole response, so callers retry rather than persisting a partial
    // field.
    const rows = await executeRunningStylePerHorse({ dependencies, env, filters });
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
  const trainings =
    keibajoCode === undefined || raceBango === undefined
      ? []
      : [trainingDescriptor({ date, keibajoCode, raceBango })];
  return source === undefined && keibajoCode === undefined && raceBango === undefined
    ? [{ date, kind: "race-keys" }, ...features]
    : [...features, ...trainings];
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
    if (request.method === "GET" && url.pathname === FRESH_RACE_ENTRIES_PATH) {
      return await handleFreshRaceEntries(request, url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === BULK_FRESH_RACE_ENTRIES_PATH) {
      return await handleBulkFreshRaceEntries(request, url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/race-trainings") {
      return await handleRaceTrainings(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/running-style-features") {
      return await handleRunningStyleFeatures(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/win-rate-heatmap-stats") {
      return await handleWinRateHeatmapStats(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/horse-race-results") {
      return await handleHorseRaceResults(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/condition-history-stats") {
      return await handleConditionHistoryStats(url, env, dependencies);
    }
    if (request.method === "GET" && url.pathname === "/v1/race-entity-recent-results") {
      return await handleRaceEntityRecentResults(url, env, dependencies);
    }
    if (
      (request.method === "POST" || request.method === "DELETE") &&
      url.pathname === "/admin/purge"
    ) {
      return await handlePurge(request, url, env, dependencies);
    }
    return jsonResponse({ error: "not_found" }, 404);
  } catch (error) {
    if (error instanceof RaceEntityRequestError) {
      return jsonResponse({ error: { code: error.code, message: error.message } }, error.status);
    }
    if (url.pathname === "/v1/race-entity-recent-results") {
      const message = error instanceof Error ? error.message : "";
      const validation = message.includes("must") || message.includes("required");
      if (validation) {
        return jsonResponse(
          {
            error: {
              code: message.includes("horseNumber") ? "RUNNER_NOT_FOUND" : "RACE_NOT_FOUND",
              message,
            },
          },
          400,
        );
      }
      const timeout = message.toLowerCase().includes("timeout");
      console.error(
        JSON.stringify({
          code: failureCode(error),
          detail: message,
          event: "race_entity_recent_results_failed",
          path: url.pathname,
        }),
      );
      return jsonResponse(
        {
          error: {
            code: timeout ? "TIMEOUT" : "UPSTREAM_ERROR",
            message: timeout
              ? "The R2 Catalog history query timed out."
              : "The R2 Catalog history query failed.",
          },
        },
        timeout ? 504 : 502,
      );
    }
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
      {
        code: failure.code,
        detail: failure.detail,
        error:
          url.pathname === FRESH_RACE_ENTRIES_PATH || url.pathname === BULK_FRESH_RACE_ENTRIES_PATH
            ? "fresh_race_entries_unavailable"
            : "r2_sql_unavailable",
      },
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
