// Run with bun. Best-effort viewer prediction cache warming for the cron Worker.
//
// After a rescore lands fresh predictions in Neon, the cron Worker calls the
// viewer's finish-prediction section API with __predictionRefresh=1 so the
// viewer recomputes from Neon and stores the weight-aware result in its Cache
// API. This makes the race detail page show the new prediction immediately.
// Warming is fire-and-forget: failures are logged and never thrown so they can
// never block the rescore ack path.

import type { Env, PredictCategory } from "./types";

const VIEWER_BASE_URL = "https://pc-keiba-viewer.kkk4oru.com";
const SECTION_PATH = "finish-prediction";
const PREDICTION_REFRESH_PARAM = "__predictionRefresh";
const PREDICTION_REFRESH_VALUE = "1";
const WARM_TIMEOUT_MS = 5000;
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
// ban-ei rows live under the nar source (keibajo 65/83); both nar categories map
// 1:1 to the nar source. Mirrors race-coordinator.ts CATEGORY_SOURCES — kept
// local so the warm path does not couple to the coordinator's internals.
const CATEGORY_SOURCES: Readonly<Record<PredictCategory, ReadonlyArray<string>>> = {
  "ban-ei": ["nar"],
  jra: ["jra"],
  nar: ["nar"],
};

interface WarmRaceParams {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  year: string;
}

interface WarmCategoryParams {
  category: PredictCategory;
  env: Env;
  runDate: string;
  runYmd: string;
}

interface RaceWarmRow {
  keibajo_code: string;
  race_bango: string;
}

const pad = (value: string, width: number): string => value.padStart(width, "0");

const buildPlaceholders = (count: number): string =>
  Array.from({ length: count }, () => "?").join(", ");

const buildSectionUrl = (params: WarmRaceParams): string =>
  `${VIEWER_BASE_URL}/api/races/${params.year}/${params.month}/${params.day}/${params.keibajoCode}/${params.raceNumber}/sections/${SECTION_PATH}?${PREDICTION_REFRESH_PARAM}=${PREDICTION_REFRESH_VALUE}`;

// Fire-and-forget warm of one race's viewer section. Returns true on a 2xx
// response; any non-2xx, timeout, or network error returns false (never throws).
export const warmPredictionCacheForRace = async (params: WarmRaceParams): Promise<boolean> => {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), WARM_TIMEOUT_MS);
  try {
    const response = await fetch(buildSectionUrl(params), { signal: controller.signal });
    return response.ok;
  } catch {
    return false;
  } finally {
    clearTimeout(timeoutId);
  }
};

const listRacesForCategory = async (params: WarmCategoryParams): Promise<RaceWarmRow[]> => {
  const sources = CATEGORY_SOURCES[params.category];
  const nen = params.runYmd.slice(RUN_YMD_NEN_START, RUN_YMD_NEN_END);
  const tsukihi = params.runYmd.slice(RUN_YMD_TSUKIHI_START, RUN_YMD_TSUKIHI_END);
  const sql = `select keibajo_code, race_bango
       from realtime_race_sources
      where source in (${buildPlaceholders(sources.length)})
        and kaisai_nen = ?
        and kaisai_tsukihi = ?
      order by keibajo_code, race_bango`;
  const result = await params.env.REALTIME_DB.prepare(sql)
    .bind(...sources, nen, tsukihi)
    .all<RaceWarmRow>();
  return result.results;
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
        year,
      }),
    ),
  );
  return warmed.filter((ok) => ok).length;
};
