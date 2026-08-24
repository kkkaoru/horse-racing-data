// Run with bun. Best-effort viewer prediction cache warming for the cron Worker.
//
// After predictions land in Neon and pred:fp KV is written, the cron Worker
// warms the viewer's finish-prediction section, race-detail SSR snapshot,
// and race-detail page so colo Cache API + DETAIL_SECTION_CACHE_KV are hot.
// The normal path first busts the viewer's stale Cache API / finish-inputs
// copies, then always warms with __predictionRefresh. Cache API deletion does
// not make KV propagation synchronous, so a normal warm could otherwise read
// an old static KV snapshot back into cache after a successful bust. Failures
// are logged and never thrown so they can never block prediction.

import { isOverseasKeibajoCode } from "./cron-decision";
import { publishFinishPositionPredictionCache } from "./prediction-kv-writer";
import type { Env, PredictCategory } from "./types";

const VIEWER_BASE_URL: string = "https://pc-keiba-viewer.kkk4oru.com";
const SECTION_PATH: string = "finish-prediction";
const RACE_DETAIL_SSR_PATH: string = "/api/cache-warm/race-detail-ssr";
const CACHE_WARM_HEADER_NAME: string = "X-PC-Keiba-Cache-Warm";
const CACHE_WARM_HEADER_VALUE: string = "scheduled";
const INTERNAL_TOKEN_HEADER_NAME: string = "x-pc-keiba-internal-token";
const HTTP_GET_METHOD: string = "GET";
const HTTP_POST_METHOD: string = "POST";
const PREDICTION_REFRESH_PARAM = "__predictionRefresh";
const PREDICTION_REFRESH_VALUE = "1";
const EXPECTED_PREDICTION_GENERATED_AT_PARAM = "expectedPredictionGeneratedAt";
const WARM_TIMEOUT_MS = 20_000;
const RUN_DATE_YEAR_START = 0;
const RUN_DATE_YEAR_END = 4;
const RUN_DATE_MONTH_START = 5;
const RUN_DATE_MONTH_END = 7;
const RUN_DATE_DAY_START = 8;
const RUN_DATE_DAY_END = 10;
const RUN_YMD_NEN_START = 0;
const RUN_YMD_NEN_END = 4;
const RUN_YMD_TSUKIHI_START = 4;
const RUN_YMD_TSUKIHI_END = 8;
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
// Ban-ei rows live under the nar source with keibajo_code 83. The warm path
// keeps its own routing table so it can mirror the predict pipeline without
// coupling to the coordinator internals.
const BAN_EI_KEIBAJO_CODES = ["83"] as const;
const POPULATE_MAX_ATTEMPTS = 8;
const POPULATE_RETRY_DELAY_MS = 10_000;
const YMD_LENGTH = 8;
const MAX_WARM_RESPONSE_BYTES = 1024 * 1024;
const WARM_RESPONSE_LIMIT_ERROR = "viewer warm response exceeded byte limit";
interface CategoryRaceFilter {
  keibajoCodes: ReadonlyArray<string>;
  keibajoMode: "all" | "exclude" | "include";
  sources: ReadonlyArray<string>;
}

const CATEGORY_RACE_FILTERS: Readonly<Record<PredictCategory, CategoryRaceFilter>> = {
  "ban-ei": {
    keibajoCodes: BAN_EI_KEIBAJO_CODES,
    keibajoMode: "include",
    sources: ["nar"],
  },
  jra: {
    keibajoCodes: [],
    keibajoMode: "all",
    sources: ["jra"],
  },
  nar: {
    keibajoCodes: BAN_EI_KEIBAJO_CODES,
    keibajoMode: "exclude",
    sources: ["nar"],
  },
};

interface WarmRaceParams {
  day: string;
  expectedGeneratedAt?: string;
  internalToken?: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  refresh?: boolean;
  viewer?: Env["PC_KEIBA_VIEWER"];
  year: string;
}

interface WarmFetchInit {
  headers?: Readonly<Record<string, string>>;
  method?: string;
}

interface WarmCategoryParams {
  category: PredictCategory;
  env: Env;
  runDate: string;
  runYmd: string;
}

interface PopulateViewerDisplayParams {
  category: PredictCategory;
  env: Env;
  keibajoCode: string;
  raceBango: string;
  runYmd: string;
}

interface RaceWarmRow {
  keibajo_code: string;
  race_bango: string;
}

const pad = (value: string, width: number): string => value.padStart(width, "0");

const sleep = (ms: number): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export const buildWarmRaceParamsFromYmd = (
  runYmd: string,
  keibajoCode: string,
  raceBango: string,
): WarmRaceParams => ({
  day: runYmd.slice(RUN_YMD_TSUKIHI_START + 2, RUN_YMD_TSUKIHI_END),
  keibajoCode: pad(keibajoCode, KEIBAJO_PAD_WIDTH),
  month: runYmd.slice(RUN_YMD_TSUKIHI_START, RUN_YMD_TSUKIHI_START + 2),
  raceNumber: pad(raceBango, RACE_BANGO_PAD_WIDTH),
  year: runYmd.slice(RUN_YMD_NEN_START, RUN_YMD_NEN_END),
});

const buildPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(", ");

const buildKeibajoFilter = (
  filter: CategoryRaceFilter,
): { binds: ReadonlyArray<string>; sql: string } => {
  if (filter.keibajoMode === "all") {
    return { binds: [], sql: "" };
  }
  const operator = filter.keibajoMode === "include" ? "in" : "not in";
  return {
    binds: filter.keibajoCodes,
    sql: `\n        and keibajo_code ${operator} (${buildPlaceholders(filter.keibajoCodes.length)})`,
  };
};

const buildSectionUrl = (params: WarmRaceParams): string => {
  const base = `${VIEWER_BASE_URL}/api/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${params.raceNumber}/sections/${SECTION_PATH}`;
  if (params.refresh !== true) return base;
  const query = new URLSearchParams({ [PREDICTION_REFRESH_PARAM]: PREDICTION_REFRESH_VALUE });
  if (params.expectedGeneratedAt !== undefined) {
    query.set(EXPECTED_PREDICTION_GENERATED_AT_PARAM, params.expectedGeneratedAt);
  }
  return `${base}?${query.toString()}`;
};

const buildRaceDetailPageUrl = (params: WarmRaceParams): string =>
  `${VIEWER_BASE_URL}/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${params.raceNumber}`;

const buildRaceDetailSsrWarmUrl = (params: WarmRaceParams): string =>
  `${VIEWER_BASE_URL}${RACE_DETAIL_SSR_PATH}?date=${params.year}-${params.month}-${params.day}&keibajo=${params.keibajoCode}&race=${params.raceNumber}`;

const resolveViewerFetcher = (viewer: Env["PC_KEIBA_VIEWER"]): typeof fetch =>
  viewer ? (input, init) => viewer.fetch(input, init) : fetch;

const drainWarmResponseReader = async (
  reader: ReadableStreamDefaultReader<Uint8Array>,
  bytesRead: number,
): Promise<void> => {
  const chunk = await reader.read();
  if (chunk.done) return;
  const nextBytesRead = bytesRead + chunk.value.byteLength;
  if (nextBytesRead > MAX_WARM_RESPONSE_BYTES) {
    await reader.cancel(WARM_RESPONSE_LIMIT_ERROR);
    throw new Error(WARM_RESPONSE_LIMIT_ERROR);
  }
  await drainWarmResponseReader(reader, nextBytesRead);
};

const drainWarmResponseBody = async (response: Response): Promise<void> => {
  if (response.body === null) return;
  await drainWarmResponseReader(response.body.getReader(), 0);
};

const fetchWithTimeout = async (
  url: string,
  viewer: Env["PC_KEIBA_VIEWER"],
  init?: WarmFetchInit,
): Promise<boolean> => {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), WARM_TIMEOUT_MS);
  try {
    const response = await resolveViewerFetcher(viewer)(url, {
      ...(init === undefined ? {} : init),
      signal: controller.signal,
    });
    await drainWarmResponseBody(response);
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeoutId);
  }
};

// Fire-and-forget warm of one race's viewer section. Returns true on a 2xx
// response; any non-2xx, timeout, or network error returns false (never throws).
export const warmPredictionCacheForRace = async (params: WarmRaceParams): Promise<boolean> => {
  if (params.expectedGeneratedAt === undefined) {
    return fetchWithTimeout(buildSectionUrl(params), params.viewer);
  }
  const internalToken = params.internalToken?.trim();
  if (internalToken === undefined || internalToken.length === 0) return false;
  return fetchWithTimeout(buildSectionUrl(params), params.viewer, {
    headers: { [INTERNAL_TOKEN_HEADER_NAME]: internalToken },
    method: HTTP_GET_METHOD,
  });
};

export const warmRaceDetailPage = async (params: WarmRaceParams): Promise<boolean> =>
  fetchWithTimeout(buildRaceDetailPageUrl(params), params.viewer);

export const warmRaceDetailSsrSnapshot = async (params: WarmRaceParams): Promise<boolean> =>
  fetchWithTimeout(buildRaceDetailSsrWarmUrl(params), params.viewer, {
    headers: { [CACHE_WARM_HEADER_NAME]: CACHE_WARM_HEADER_VALUE },
    method: HTTP_POST_METHOD,
  });

export const warmViewerDisplayForRace = async (params: WarmRaceParams): Promise<boolean> => {
  if (!(await warmPredictionCacheForRace(params))) return false;
  if (!(await warmRaceDetailSsrSnapshot(params))) return false;
  return warmRaceDetailPage(params);
};

const listRacesForCategory = async (params: WarmCategoryParams): Promise<RaceWarmRow[]> => {
  const filter = CATEGORY_RACE_FILTERS[params.category];
  const keibajoFilter = buildKeibajoFilter(filter);
  const nen = params.runYmd.slice(RUN_YMD_NEN_START, RUN_YMD_NEN_END);
  const tsukihi = params.runYmd.slice(RUN_YMD_TSUKIHI_START, RUN_YMD_TSUKIHI_END);
  const sql = `select keibajo_code, race_bango
       from realtime_race_sources
      where source in (${buildPlaceholders(filter.sources.length)})
        and kaisai_nen = ?
        and kaisai_tsukihi = ?${keibajoFilter.sql}
      order by keibajo_code, race_bango`;
  const result = await params.env.REALTIME_DB.prepare(sql)
    .bind(...filter.sources, nen, tsukihi, ...keibajoFilter.binds)
    .all<RaceWarmRow>();
  if (params.category !== "jra") {
    return result.results;
  }
  return result.results.filter((row) => !isOverseasKeibajoCode(row.keibajo_code));
};

// Warm every race in the category for the run date. Queries realtime_race_sources
// (same D1 table the coordinator uses) and fires one viewer warm per race. Best
// effort: row-level failures only affect that race's boolean and are not thrown.
// Returns the count of races that warmed successfully (2xx).
export const warmPredictionCacheForCategory = async (
  params: WarmCategoryParams,
): Promise<number> => {
  const year = params.runDate.slice(RUN_DATE_YEAR_START, RUN_DATE_YEAR_END);
  const month = params.runDate.slice(RUN_DATE_MONTH_START, RUN_DATE_MONTH_END);
  const day = params.runDate.slice(RUN_DATE_DAY_START, RUN_DATE_DAY_END);
  const rows = await listRacesForCategory(params);
  const warmed = await Promise.all(
    rows.map((row) =>
      warmPredictionCacheForRace({
        day,
        keibajoCode: pad(row.keibajo_code, KEIBAJO_PAD_WIDTH),
        month,
        raceNumber: pad(row.race_bango, RACE_BANGO_PAD_WIDTH),
        viewer: params.env.PC_KEIBA_VIEWER,
        year,
      }),
    ),
  );
  return warmed.filter((ok) => ok).length;
};

export const populateViewerDisplayCache = async (
  params: PopulateViewerDisplayParams,
): Promise<boolean> => {
  if (params.runYmd.length !== YMD_LENGTH) return false;
  const published = await publishFinishPositionPredictionCache({
    bustCacheApi: true,
    category: params.category,
    env: params.env,
    keibajoCode: params.keibajoCode,
    raceBango: params.raceBango,
    runYmd: params.runYmd,
  });
  if (published.status !== "written" || typeof published.expectedGeneratedAt !== "string") {
    return false;
  }
  const internalToken = params.env.PC_KEIBA_VIEWER_INTERNAL_TOKEN?.trim();
  if (internalToken === undefined || internalToken.length === 0) return false;
  const warmParams: WarmRaceParams = buildWarmRaceParamsFromYmd(
    params.runYmd,
    params.keibajoCode,
    params.raceBango,
  );
  return warmViewerDisplayForRace({
    ...warmParams,
    expectedGeneratedAt: published.expectedGeneratedAt,
    internalToken,
    refresh: true,
    viewer: params.env.PC_KEIBA_VIEWER,
  });
};

export const retryPopulateViewerDisplayCache = async (
  params: PopulateViewerDisplayParams,
): Promise<boolean> => {
  const tryOnce = async (remaining: number): Promise<boolean> => {
    if (await populateViewerDisplayCache(params)) return true;
    if (remaining <= 1) return false;
    await sleep(POPULATE_RETRY_DELAY_MS);
    return tryOnce(remaining - 1);
  };
  return tryOnce(POPULATE_MAX_ATTEMPTS);
};
