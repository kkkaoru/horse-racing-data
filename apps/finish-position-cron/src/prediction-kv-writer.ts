// Run with bun. After a focused-full or weight rescore lands in Neon, write
// the viewer's pred:fp:v1 KV payload (FinishPositionModelPredictionFeature[])
// and optionally bust the colo Cache API copy.

import { neon } from "@neondatabase/serverless";

import {
  predictionCacheSourceForCategory,
  triggerPredictionCacheBust,
} from "./prediction-cache-bust";
import {
  buildFinishPositionPredictionKvKey,
  getPredictionKvTtlSeconds,
  resolvePredictionCacheWindow,
} from "./prediction-kv-keys";
import type { Env, PredictCategory } from "./types";

const RUN_YMD_YEAR_END = 4;
const KEIBAJO_PAD_WIDTH = 2;
const RACE_BANGO_PAD_WIDTH = 2;
const CONFIDENCE_LOW_MAX_STDDEV = 1.3;
const CONFIDENCE_MID_MAX_STDDEV = 1.5;
const CONFIDENCE_MIN_VALID_SCORES = 2;
const BAN_EI_KEIBAJO_CODE = "83";

export type FinishPositionConfidenceTier = "high" | "low" | "mid";

export interface FinishPositionPredictionFeature {
  confidenceTier: FinishPositionConfidenceTier | null;
  horseNumber: string;
  modelVersion: string;
  predictedFinishNorm: number | null;
  predictedScoreStddev: number | null;
  showProbability: null;
  winProbability: null;
}

export interface FinishPositionPredictionRow {
  modelVersion: string;
  predictedRank: number;
  predictedScore: number | null;
  umaban: string;
}

export type PredictionKvPublishStatus =
  | "error"
  | "skipped-empty"
  | "skipped-no-kv"
  | "skipped-outside-window"
  | "written";

export interface PredictionKvPublishResult {
  busted: boolean;
  status: PredictionKvPublishStatus;
}

export interface PublishFinishPositionPredictionCacheParams {
  bustCacheApi: boolean;
  category: PredictCategory;
  env: Env;
  keibajoCode: string;
  nowMs?: number;
  raceBango: string;
  runYmd: string;
}

export interface PublishFinishPositionPredictionCacheForCategoryParams {
  bustCacheApi: boolean;
  category: PredictCategory;
  env: Env;
  nowMs?: number;
  runYmd: string;
}

interface RealtimeRaceRow {
  keibajo_code: string;
  race_bango: string;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const toFiniteNumber = (value: unknown): number | null => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value !== "string") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
};

const toUmabanString = (value: unknown): string | null => {
  if (typeof value === "number" && Number.isFinite(value)) return String(Math.trunc(value));
  if (typeof value === "string" && value.length > 0) return value;
  return null;
};

const resolveConfidenceTier = (stddev: number): FinishPositionConfidenceTier => {
  if (stddev < CONFIDENCE_LOW_MAX_STDDEV) return "low";
  if (stddev < CONFIDENCE_MID_MAX_STDDEV) return "mid";
  return "high";
};

const calculateScoreStddev = (scores: ReadonlyArray<number | null>): number | null => {
  const valid = scores.filter((value): value is number => value !== null && Number.isFinite(value));
  if (valid.length < CONFIDENCE_MIN_VALID_SCORES) return null;
  const mean = valid.reduce((total, value) => total + value, 0) / valid.length;
  const variance =
    valid.reduce((total, value) => total + (value - mean) ** 2, 0) / (valid.length - 1);
  return Math.sqrt(variance);
};

export const mapFinishPositionPredictionFeatures = (
  rows: ReadonlyArray<FinishPositionPredictionRow>,
): FinishPositionPredictionFeature[] => {
  if (rows.length === 0) return [];
  const stddev = calculateScoreStddev(rows.map((row) => row.predictedScore));
  const confidenceTier = stddev === null ? null : resolveConfidenceTier(stddev);
  const fieldSize = Math.max(1, rows.length);
  const denominator = Math.max(1, fieldSize - 1);
  return rows.map((row) => {
    const predictedFinishNorm = Math.min(1, Math.max(0, (row.predictedRank - 1) / denominator));
    return {
      confidenceTier,
      horseNumber: row.umaban,
      modelVersion: row.modelVersion,
      predictedFinishNorm,
      predictedScoreStddev: stddev,
      showProbability: null,
      winProbability: null,
    };
  });
};

const parsePredictionRow = (value: unknown): FinishPositionPredictionRow | null => {
  if (!isRecord(value) || typeof value.model_version !== "string") return null;
  const umaban = toUmabanString(value.umaban);
  const predictedRank = toFiniteNumber(value.predicted_rank);
  if (umaban === null || predictedRank === null) return null;
  return {
    modelVersion: value.model_version,
    predictedRank,
    predictedScore: toFiniteNumber(value.predicted_score),
    umaban,
  };
};

const SELECT_PREDICTIONS_SQL = `select umaban, model_version, predicted_rank, predicted_score
       from race_finish_position_model_predictions
      where source = $1
        and kaisai_nen = $2
        and kaisai_tsukihi = $3
        and keibajo_code = $4
        and race_bango = $5
        and model_version = (
          select model_version
            from race_finish_position_model_predictions
           where source = $1
             and kaisai_nen = $2
             and kaisai_tsukihi = $3
             and keibajo_code = $4
             and race_bango = $5
           order by prediction_generated_at desc nulls last
           limit 1
        )
      order by umaban`;

const fetchPredictionRows = async (
  env: Env,
  params: {
    keibajoCode: string;
    raceBango: string;
    runYmd: string;
    source: "jra" | "nar";
  },
): Promise<FinishPositionPredictionRow[]> => {
  const sql = neon(env.NEON_DATABASE_URL);
  const result: unknown = await sql.query(SELECT_PREDICTIONS_SQL, [
    params.source,
    params.runYmd.slice(0, RUN_YMD_YEAR_END),
    params.runYmd.slice(RUN_YMD_YEAR_END),
    params.keibajoCode,
    params.raceBango,
  ]);
  if (!Array.isArray(result)) return [];
  return result
    .map(parsePredictionRow)
    .filter((row): row is FinishPositionPredictionRow => row !== null);
};

const listRacesForCategory = async (
  env: Env,
  category: PredictCategory,
  runYmd: string,
): Promise<RealtimeRaceRow[]> => {
  const nen = runYmd.slice(0, RUN_YMD_YEAR_END);
  const tsukihi = runYmd.slice(RUN_YMD_YEAR_END);
  if (category === "jra") {
    const result = await env.REALTIME_DB.prepare(
      `select keibajo_code, race_bango
         from realtime_race_sources
        where source = ?
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
        order by keibajo_code, race_bango`,
    )
      .bind("jra", nen, tsukihi)
      .all<RealtimeRaceRow>();
    return result.results;
  }
  if (category === "ban-ei") {
    const result = await env.REALTIME_DB.prepare(
      `select keibajo_code, race_bango
         from realtime_race_sources
        where source = ?
          and kaisai_nen = ?
          and kaisai_tsukihi = ?
          and keibajo_code in (?)
        order by keibajo_code, race_bango`,
    )
      .bind("nar", nen, tsukihi, BAN_EI_KEIBAJO_CODE)
      .all<RealtimeRaceRow>();
    return result.results;
  }
  const result = await env.REALTIME_DB.prepare(
    `select keibajo_code, race_bango
       from realtime_race_sources
      where source = ?
        and kaisai_nen = ?
        and kaisai_tsukihi = ?
        and keibajo_code not in (?)
      order by keibajo_code, race_bango`,
  )
    .bind("nar", nen, tsukihi, BAN_EI_KEIBAJO_CODE)
    .all<RealtimeRaceRow>();
  return result.results;
};

const writeFinishPositionPredictionKv = async (
  params: PublishFinishPositionPredictionCacheParams,
): Promise<PredictionKvPublishResult> => {
  const nowMs = params.nowMs ?? Date.now();
  const window = resolvePredictionCacheWindow(params.runYmd, nowMs);
  const ttlSeconds = getPredictionKvTtlSeconds(window);
  if (ttlSeconds <= 0) {
    return { busted: false, status: "skipped-outside-window" };
  }
  const kv = params.env.DETAIL_SECTION_CACHE_KV;
  if (!kv) {
    return { busted: false, status: "skipped-no-kv" };
  }
  const keibajoCode = params.keibajoCode.padStart(KEIBAJO_PAD_WIDTH, "0");
  const raceBango = params.raceBango.padStart(RACE_BANGO_PAD_WIDTH, "0");
  const source = predictionCacheSourceForCategory(params.category);
  const rows = await fetchPredictionRows(params.env, {
    keibajoCode,
    raceBango,
    runYmd: params.runYmd,
    source,
  });
  const features = mapFinishPositionPredictionFeatures(rows);
  if (features.length === 0) {
    return { busted: false, status: "skipped-empty" };
  }
  const cacheKey = buildFinishPositionPredictionKvKey({
    keibajoCode,
    mmdd: params.runYmd.slice(RUN_YMD_YEAR_END),
    raceBango,
    year: params.runYmd.slice(0, RUN_YMD_YEAR_END),
  });
  await kv.put(cacheKey, JSON.stringify(features), { expirationTtl: ttlSeconds });
  if (!params.bustCacheApi) {
    return { busted: false, status: "written" };
  }
  const bust = await triggerPredictionCacheBust(params.env, {
    keibajoCode,
    mmdd: params.runYmd.slice(RUN_YMD_YEAR_END),
    raceBango,
    source,
    year: params.runYmd.slice(0, RUN_YMD_YEAR_END),
  });
  return { busted: bust.status === "ok", status: "written" };
};

export const publishFinishPositionPredictionCache = async (
  params: PublishFinishPositionPredictionCacheParams,
): Promise<PredictionKvPublishResult> => {
  try {
    return await writeFinishPositionPredictionKv(params);
  } catch (error) {
    console.warn(
      `prediction kv fp write failed category=${params.category} runYmd=${params.runYmd} keibajo=${params.keibajoCode} race=${params.raceBango}:`,
      String(error),
    );
    return { busted: false, status: "error" };
  }
};

export const publishFinishPositionPredictionCacheForCategory = async (
  params: PublishFinishPositionPredictionCacheForCategoryParams,
): Promise<number> => {
  try {
    const rows = await listRacesForCategory(params.env, params.category, params.runYmd);
    const results = await Promise.all(
      rows.map((row) =>
        publishFinishPositionPredictionCache({
          bustCacheApi: params.bustCacheApi,
          category: params.category,
          env: params.env,
          keibajoCode: row.keibajo_code,
          nowMs: params.nowMs,
          raceBango: row.race_bango,
          runYmd: params.runYmd,
        }),
      ),
    );
    return results.filter((result) => result.status === "written").length;
  } catch (error) {
    console.warn(
      `prediction kv fp category write failed category=${params.category} runYmd=${params.runYmd}:`,
      String(error),
    );
    return 0;
  }
};
