// Run with bun. Pickup a completed /prewarm-day-base parquet from the
// container in-process store and PUT it through FEATURES_CACHE.
//
// Container R2 tokens are read-only (focused-full-cache-pickup.ts). Detached
// prewarm cannot SigV4 PUT; this is the only working write path.

import { proxyResultParquetsToR2 } from "./container-ndjson-proxy";
import type { DaybaseWatermark, PredictResultLine } from "./ndjson-stream";
import { listDayBasePickupDoNames } from "./predict-do-shard";
import type { Env, PredictCategory } from "./types";

type ResultLineType = "result";

interface PrewarmCachePickupParams {
  category: PredictCategory;
  debug?: boolean;
  env: Env;
  runYmd: string;
}

interface HeadDayBaseObjectParams {
  category: PredictCategory;
  env: Pick<Env, "FEATURES_CACHE">;
  runYmd: string;
}

interface PrewarmCacheResponseBody {
  found: boolean;
  parquetBase64?: string | null;
  parquetKey?: string | null;
  daybaseWatermark?: DaybaseWatermark | null;
  watermarkError?: string | null;
}

interface BuildDayBaseObjectKeyParams {
  category: PredictCategory;
  runYmd: string;
}

interface ToResultLineParams {
  category: PredictCategory;
  daybaseWatermark: DaybaseWatermark;
  parquetBase64: string;
  parquetKey: string;
}

const PREWARM_CACHE_PATH: string = "/prewarm-day-base-cache";
const PREDICT_HOST: string = "http://do";
const RESULT_LINE_TYPE: ResultLineType = "result";
const PLACEHOLDER_RACES_PREDICTED: number = 0;
const R2_DAY_BASE_PREFIX: string = "feat-daybase";
const R2_DAY_BASE_GENERATION: string = "catalog-v1";
const R2_DAY_BASE_FILE: string = "features.parquet";
const WATERMARK_META_MAX_UPDATED: string = "max-data-sakusei-nengappi";
const WATERMARK_META_ROW_COUNT: string = "row-count";
const WATERMARK_META_RS_PREDICTED_AT_MAX: string = "rs-predicted-at-max";
const WATERMARK_META_RS_ROW_COUNT: string = "rs-row-count";

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const optionalString = (value: unknown): string | null | undefined => {
  if (value === undefined || value === null) return value;
  return typeof value === "string" ? value : undefined;
};

const parseDaybaseWatermark = (value: unknown): DaybaseWatermark | null => {
  if (!isRecord(value)) return null;
  const maxDataSakuseiNengappi = value.maxDataSakuseiNengappi;
  const rowCount = value.rowCount;
  const rsPredictedAtMax = value.rsPredictedAtMax;
  const rsRowCount = value.rsRowCount;
  if (typeof maxDataSakuseiNengappi !== "string" || maxDataSakuseiNengappi.length === 0)
    return null;
  if (typeof rowCount !== "number") return null;
  if (typeof rsPredictedAtMax !== "string" || rsPredictedAtMax.length === 0) return null;
  if (typeof rsRowCount !== "number") return null;
  return { maxDataSakuseiNengappi, rowCount, rsPredictedAtMax, rsRowCount };
};

const parsePrewarmCacheResponse = (value: unknown): PrewarmCacheResponseBody | null => {
  if (!isRecord(value)) return null;
  if (typeof value.found !== "boolean") return null;
  return {
    found: value.found,
    parquetBase64: optionalString(value.parquetBase64),
    parquetKey: optionalString(value.parquetKey),
    daybaseWatermark: parseDaybaseWatermark(value.daybaseWatermark),
    watermarkError: optionalString(value.watermarkError),
  };
};

const hasDayBaseWatermarkMetadata = (value: R2Object): boolean => {
  const meta = value.customMetadata;
  if (meta === undefined) return false;
  const maxUpdated = meta[WATERMARK_META_MAX_UPDATED];
  const rowCount = meta[WATERMARK_META_ROW_COUNT];
  const rsPredicted = meta[WATERMARK_META_RS_PREDICTED_AT_MAX];
  const rsRowCount = meta[WATERMARK_META_RS_ROW_COUNT];
  return (
    typeof maxUpdated === "string" &&
    maxUpdated.length > 0 &&
    typeof rowCount === "string" &&
    rowCount.length > 0 &&
    typeof rsPredicted === "string" &&
    rsPredicted.length > 0 &&
    typeof rsRowCount === "string" &&
    rsRowCount.length > 0
  );
};

export const buildDayBaseObjectKey = (params: BuildDayBaseObjectKeyParams): string =>
  `${R2_DAY_BASE_PREFIX}/${R2_DAY_BASE_GENERATION}/${params.category}/${params.runYmd}/${R2_DAY_BASE_FILE}`;

export const headDayBaseObject = async (
  params: HeadDayBaseObjectParams,
): Promise<R2Object | null> => {
  const found = await params.env.FEATURES_CACHE.head(buildDayBaseObjectKey(params));
  if (found === null) return null;
  return hasDayBaseWatermarkMetadata(found) ? found : null;
};

const buildPrewarmCacheUrl = (params: PrewarmCachePickupParams): string => {
  const searchParams = new URLSearchParams({
    category: params.category,
    runDate: params.runYmd,
  });
  return `${PREDICT_HOST}${PREWARM_CACHE_PATH}?${searchParams.toString()}`;
};

const toResultLine = (params: ToResultLineParams): PredictResultLine => ({
  type: RESULT_LINE_TYPE,
  category: params.category,
  racesPredicted: PLACEHOLDER_RACES_PREDICTED,
  parquetBase64: params.parquetBase64,
  parquetKey: params.parquetKey,
  daybaseWatermark: params.daybaseWatermark,
});

const pickUpPrewarmDayBaseFromDo = async (
  params: PrewarmCachePickupParams,
  doName: string,
): Promise<boolean> => {
  const { env, category, runYmd, debug } = params;
  const url = buildPrewarmCacheUrl(params);
  const doId = env.FINISH_POSITION_PREDICT_CONTAINER.idFromName(doName);
  const stub = env.FINISH_POSITION_PREDICT_CONTAINER.get(doId);
  const response = await stub.fetch(new Request(url));
  if (!response.ok) {
    console.warn(
      `[day-base-prewarm-pickup] non-ok status=${response.status} doName=${doName} url=${url}`,
    );
    return false;
  }
  const body = parsePrewarmCacheResponse(await response.json());
  if (body === null || !body.found) return false;
  if (body.parquetBase64 === undefined || body.parquetBase64 === null) return false;
  if (body.parquetKey === undefined || body.parquetKey === null) return false;
  if (body.parquetBase64 === "" || body.parquetKey === "") return false;
  if (body.daybaseWatermark === undefined || body.daybaseWatermark === null) {
    const reason =
      body.watermarkError === undefined || body.watermarkError === null ? "-" : body.watermarkError;
    console.warn(
      `[day-base-prewarm-pickup] missing watermark category=${category} runYmd=${runYmd} reason=${reason}`,
    );
    return false;
  }
  await proxyResultParquetsToR2(
    toResultLine({
      category,
      daybaseWatermark: body.daybaseWatermark,
      parquetBase64: body.parquetBase64,
      parquetKey: body.parquetKey,
    }),
    env,
    debug === true,
  );
  return true;
};

export const pickUpPrewarmDayBase = async (params: PrewarmCachePickupParams): Promise<boolean> => {
  const { env, category, runYmd } = params;
  const tryDoNames = async (doNames: readonly string[]): Promise<boolean> => {
    const [doName, ...rest] = doNames;
    if (doName === undefined) return false;
    try {
      if (await pickUpPrewarmDayBaseFromDo(params, doName)) return true;
    } catch (err) {
      console.warn(
        `[day-base-prewarm-pickup] failed category=${category} runYmd=${runYmd} doName=${doName}: ${String(err)}`,
      );
    }
    return tryDoNames(rest);
  };
  return tryDoNames(listDayBasePickupDoNames({ category, env }));
};
