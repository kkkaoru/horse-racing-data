// Run with bun. End-to-end orchestrator that loads the LightGBM model
// and per-horse feature batch from R2, computes race-internal field
// features on the fly, evaluates the JS tree ensemble, and batch-upserts
// predictions into D1. Called by both the Cron-scheduled scanner and the
// on-demand admin trigger.

import {
  computeFieldFeaturesPerHorse,
  type HorseFieldRow,
  type HorsePeerInputs,
} from "./running-style-field-features";
import {
  buildFeatureVector,
  predictRunningStyle,
  type CompactLightGBMModel,
} from "./running-style-lightgbm-tree";
import {
  loadFeaturesFromR2,
  loadLightGBMModelFromR2,
  type RaceHorseFeatureRow,
} from "./running-style-r2";
import {
  upsertRaceRunningStyles,
  type RaceRunningStyleRow,
} from "./running-style-d1";

export interface InferenceConfig {
  modelKey: string;
  featuresKey: string;
  predictedAt: string;
}

export interface InferenceSummary {
  raceCount: number;
  horseCount: number;
  writtenCount: number;
  modelVersion: string;
}

const groupByRace = (
  rows: ReadonlyArray<RaceHorseFeatureRow>,
): Map<string, RaceHorseFeatureRow[]> => {
  const grouped = new Map<string, RaceHorseFeatureRow[]>();
  rows.forEach((row) => {
    const list = grouped.get(row.raceKey);
    if (list === undefined) {
      grouped.set(row.raceKey, [row]);
      return;
    }
    list.push(row);
  });
  return grouped;
};

const mergeFeatureMap = (
  perHorse: Record<string, number | null>,
  fieldRow: HorseFieldRow,
): Record<string, number | null> => {
  const numericField: Record<string, number | null> = {
    field_avg_career_win_rate: fieldRow.field_avg_career_win_rate,
    field_avg_past_first_3f: fieldRow.field_avg_past_first_3f,
    field_avg_past_kohan_3f: fieldRow.field_avg_past_kohan_3f,
    field_avg_speed_index: fieldRow.field_avg_speed_index,
    field_has_pure_nige_horse: fieldRow.field_has_pure_nige_horse ? 1 : 0,
    field_max_past_corner_1_norm: fieldRow.field_max_past_corner_1_norm,
    field_min_past_corner_1_norm: fieldRow.field_min_past_corner_1_norm,
    field_nige_candidate_count: fieldRow.field_nige_candidate_count,
    field_nige_pressure: fieldRow.field_nige_pressure,
    field_oikomi_pressure: fieldRow.field_oikomi_pressure,
    field_pace_index: fieldRow.field_pace_index,
    field_sashi_pressure: fieldRow.field_sashi_pressure,
    field_senkou_pressure: fieldRow.field_senkou_pressure,
    field_spread_past_corner_1_norm: fieldRow.field_spread_past_corner_1_norm,
    field_top_speed_index: fieldRow.field_top_speed_index,
    self_nige_rate_minus_field_avg: fieldRow.self_nige_rate_minus_field_avg,
    self_speed_index_vs_field_top: fieldRow.self_speed_index_vs_field_top,
  };
  return { ...perHorse, ...numericField };
};

const extractPeerInputs = (rows: ReadonlyArray<RaceHorseFeatureRow>): ReadonlyArray<HorsePeerInputs> =>
  rows.map((row) => row.peerInputs);

const buildPredictionForHorse = (
  row: RaceHorseFeatureRow,
  fieldRow: HorseFieldRow,
  model: CompactLightGBMModel,
  predictedAt: string,
): RaceRunningStyleRow => {
  const features = mergeFeatureMap(row.perHorseFeatures, fieldRow);
  const vector = buildFeatureVector({ featureNames: model.feature_names, values: features });
  const prediction = predictRunningStyle(model, vector);
  return {
    bamei: row.bamei,
    category: row.category,
    horseNumber: row.umaban,
    kaisaiNen: row.kaisaiNen,
    kettoTorokuBango: row.kettoTorokuBango,
    modelVersion: model.model_version,
    pNige: prediction.probabilities.nige,
    pOikomi: prediction.probabilities.oikomi,
    pSashi: prediction.probabilities.sashi,
    pSenkou: prediction.probabilities.senkou,
    predictedAt,
    predictedLabel: prediction.predictedLabel,
    raceKey: row.raceKey,
  };
};

const predictRace = (
  rows: ReadonlyArray<RaceHorseFeatureRow>,
  model: CompactLightGBMModel,
  predictedAt: string,
): RaceRunningStyleRow[] => {
  const fieldRows = computeFieldFeaturesPerHorse(extractPeerInputs(rows));
  return rows.map((row, index) => {
    const fieldRow = fieldRows[index];
    if (fieldRow === undefined) throw new Error(`field row missing for index ${index}`);
    return buildPredictionForHorse(row, fieldRow, model, predictedAt);
  });
};

export const runRunningStyleInference = async (
  bucket: R2Bucket,
  db: D1Database,
  config: InferenceConfig,
): Promise<InferenceSummary> => {
  const model = await loadLightGBMModelFromR2(bucket, config.modelKey);
  const rows = await loadFeaturesFromR2(bucket, config.featuresKey);
  const grouped = groupByRace(rows);
  const predictions: RaceRunningStyleRow[] = [];
  grouped.forEach((raceRows) => {
    predictRace(raceRows, model, config.predictedAt).forEach((row) => predictions.push(row));
  });
  const writtenCount = await upsertRaceRunningStyles(db, predictions);
  return {
    horseCount: rows.length,
    modelVersion: model.model_version,
    raceCount: grouped.size,
    writtenCount,
  };
};
