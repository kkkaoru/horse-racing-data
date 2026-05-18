// Run with bun. Per-race orchestrator for finish-position lite inference:
// pulls 19 features from Postgres, scores each horse via the LightGBM
// tree walker, writes a JSONL prediction file to R2, and updates the D1
// state row.

import type { Pool } from "pg";

import {
  assignRanksWithinRace,
  buildFeatureVector,
  computeFinishPositionScore,
  type CompactLightGBMModel,
  type RaceHorseRanked,
  type RaceHorseScore,
} from "./finish-position-lite-tree";
import {
  loadLiteFeaturesForRace,
  type LiteHorseFeatures,
  type LoadFeaturesParams,
} from "./finish-position-lite-features";

const MODEL_KEY_PREFIX = "finish-position/models";
const PREDICTIONS_KEY_PREFIX = "finish-position/predictions";
const MODEL_CACHE = new Map<string, CompactLightGBMModel>();

export interface InferenceRaceParams extends LoadFeaturesParams {
  modelVersion: string;
  predictedAt: string;
}

export interface InferenceResult {
  raceKey: string;
  modelVersion: string;
  predictionsR2Key: string;
  horseCount: number;
}

const buildRaceKey = (params: LoadFeaturesParams): string =>
  `${params.source}:${params.kaisaiNen}${params.kaisaiTsukihi}:${params.keibajoCode}:${params.raceBango}`;

const buildModelKey = (source: string): string =>
  `${MODEL_KEY_PREFIX}/${source}/lite-lgbm/latest.json`;

const buildPredictionsKey = (params: LoadFeaturesParams): string =>
  `${PREDICTIONS_KEY_PREFIX}/${params.source}/${params.kaisaiNen}${params.kaisaiTsukihi}/${buildRaceKey(params)}.jsonl`;

const loadModelFromR2 = async (
  bucket: R2Bucket,
  source: string,
  modelVersion: string,
): Promise<CompactLightGBMModel> => {
  const cacheKey = `${source}:${modelVersion}`;
  const cached = MODEL_CACHE.get(cacheKey);
  if (cached !== undefined) return cached;
  const key = buildModelKey(source);
  const object = await bucket.get(key);
  if (object === null) throw new Error(`finish-position model missing in R2: ${key}`);
  const text = await object.text();
  const model = JSON.parse(text) as CompactLightGBMModel;
  MODEL_CACHE.set(cacheKey, model);
  return model;
};

const scoreHorse = (
  horse: LiteHorseFeatures,
  model: CompactLightGBMModel,
): RaceHorseScore => {
  const vector = buildFeatureVector({
    featureNames: model.feature_names,
    values: horse.features,
  });
  return {
    kettoTorokuBango: horse.kettoTorokuBango,
    score: computeFinishPositionScore(model, vector),
    umaban: horse.umaban,
  };
};

const buildBameiLookup = (
  features: ReadonlyArray<LiteHorseFeatures>,
): Map<number, string | null> => {
  const lookup = new Map<number, string | null>();
  features.forEach((row) => lookup.set(row.umaban, row.bamei));
  return lookup;
};

const writePredictionsJsonl = async (
  bucket: R2Bucket,
  key: string,
  raceKey: string,
  ranked: ReadonlyArray<RaceHorseRanked>,
  bameiByUmaban: Map<number, string | null>,
  modelVersion: string,
  predictedAt: string,
): Promise<void> => {
  const lines = ranked.map((entry) =>
    JSON.stringify({
      bamei: bameiByUmaban.get(entry.umaban) ?? null,
      ketto_toroku_bango: entry.kettoTorokuBango,
      model_version: modelVersion,
      predicted_at: predictedAt,
      predicted_rank: entry.predictedRank,
      predicted_score: entry.score,
      race_id: raceKey,
      umaban: entry.umaban,
    }),
  );
  await bucket.put(key, `${lines.join("\n")}\n`, {
    httpMetadata: { contentType: "application/x-ndjson; charset=utf-8" },
  });
};

export const runFinishPositionLiteInference = async (
  pool: Pool,
  bucket: R2Bucket,
  params: InferenceRaceParams,
): Promise<InferenceResult> => {
  const horses = await loadLiteFeaturesForRace(pool, params);
  if (horses.length === 0) {
    throw new Error(`no entries found for race ${buildRaceKey(params)}`);
  }
  const model = await loadModelFromR2(bucket, params.source, params.modelVersion);
  const scores = horses.map((horse) => scoreHorse(horse, model));
  const ranked = assignRanksWithinRace(scores);
  const raceKey = buildRaceKey(params);
  const predictionsKey = buildPredictionsKey(params);
  await writePredictionsJsonl(
    bucket,
    predictionsKey,
    raceKey,
    ranked,
    buildBameiLookup(horses),
    params.modelVersion,
    params.predictedAt,
  );
  return {
    horseCount: ranked.length,
    modelVersion: params.modelVersion,
    predictionsR2Key: predictionsKey,
    raceKey,
  };
};
