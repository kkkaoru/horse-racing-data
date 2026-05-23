// Run with: imported by race detail and horse detail server components (bun runtime)

import "server-only";
import { sql } from "drizzle-orm";

import { getDb } from "./client";
import {
  buildRaceKey as buildRaceKeyFromParsers,
  isRunningStyleLabel,
  numericOrNull,
  RUNNING_STYLE_LABELS,
} from "./corner-running-style-parsers";
import type {
  RaceLookupKeys,
  RaceRunningStyleRow,
  RunningStyleLabel,
} from "./corner-running-style-parsers";

export type { RaceLookupKeys, RaceRunningStyleRow, RunningStyleLabel };
export { isRunningStyleLabel, numericOrNull, RUNNING_STYLE_LABELS };
export const buildRaceKey = buildRaceKeyFromParsers;

export interface RaceCornerPositionRow {
  source: string;
  kaisaiNen: string;
  kaisaiTsukihi: string;
  keibajoCode: string;
  raceBango: string;
  kettoTorokuBango: string;
  umaban: number;
  modelVersion: string;
  corner1Pred: number | null;
  corner3Pred: number | null;
  corner4Pred: number | null;
}

export interface ActiveModelMetadata {
  category: string;
  modelVersion: string;
  activatedAt: Date;
}

export interface CornerPositionMetrics {
  modelVersion: string;
  category: string;
  evaluationWindowFrom: string;
  evaluationWindowTo: string;
  raceCount: number;
  predictionCount: number;
  corner1Mae: number | null;
  corner3Mae: number | null;
  corner4Mae: number | null;
  meanMae: number | null;
  corner1Top3Agreement: number | null;
  evaluatedAt: Date;
}

export interface RunningStyleMetrics {
  modelVersion: string;
  category: string;
  evaluationWindowFrom: string;
  evaluationWindowTo: string;
  raceCount: number;
  predictionCount: number;
  accuracy: number | null;
  macroF1: number | null;
  precisionPerClass: Record<RunningStyleLabel, number | null>;
  recallPerClass: Record<RunningStyleLabel, number | null>;
  supportPerClass: Record<RunningStyleLabel, number | null>;
  kyakushitsuhanteiAgreement: number | null;
  evaluatedAt: Date;
}

export const getActiveRunningStyleModel = async (
  category: string,
): Promise<ActiveModelMetadata | null> => {
  const result = await getDb().execute<{
    category: string;
    model_version: string;
    activated_at: Date;
  }>(sql`
    select category, model_version, activated_at
      from running_style_active_models
     where category = ${category}
  `);
  const row = result.rows[0];
  if (row === undefined) return null;
  return {
    activatedAt: row.activated_at,
    category: row.category,
    modelVersion: row.model_version,
  };
};

export const getActiveCornerPositionModel = async (
  category: string,
): Promise<ActiveModelMetadata | null> => {
  const result = await getDb().execute<{
    category: string;
    model_version: string;
    activated_at: Date;
  }>(sql`
    select category, model_version, activated_at
      from corner_position_active_models
     where category = ${category}
  `);
  const row = result.rows[0];
  if (row === undefined) return null;
  return {
    activatedAt: row.activated_at,
    category: row.category,
    modelVersion: row.model_version,
  };
};

interface RunningStyleEvaluationRow extends Record<string, unknown> {
  model_version: string;
  category: string;
  evaluation_window_from: string;
  evaluation_window_to: string;
  race_count: number;
  prediction_count: number;
  accuracy: string | null;
  macro_f1: string | null;
  precision_nige: string | null;
  precision_senkou: string | null;
  precision_sashi: string | null;
  precision_oikomi: string | null;
  recall_nige: string | null;
  recall_senkou: string | null;
  recall_sashi: string | null;
  recall_oikomi: string | null;
  support_nige: number | null;
  support_senkou: number | null;
  support_sashi: number | null;
  support_oikomi: number | null;
  kyakushitsuhantei_agreement: string | null;
  evaluated_at: Date;
}

const parseRunningStyleMetricsRow = (row: RunningStyleEvaluationRow): RunningStyleMetrics => ({
  accuracy: numericOrNull(row.accuracy),
  category: row.category,
  evaluatedAt: row.evaluated_at,
  evaluationWindowFrom: row.evaluation_window_from,
  evaluationWindowTo: row.evaluation_window_to,
  kyakushitsuhanteiAgreement: numericOrNull(row.kyakushitsuhantei_agreement),
  macroF1: numericOrNull(row.macro_f1),
  modelVersion: row.model_version,
  precisionPerClass: {
    nige: numericOrNull(row.precision_nige),
    oikomi: numericOrNull(row.precision_oikomi),
    sashi: numericOrNull(row.precision_sashi),
    senkou: numericOrNull(row.precision_senkou),
  },
  predictionCount: row.prediction_count,
  raceCount: row.race_count,
  recallPerClass: {
    nige: numericOrNull(row.recall_nige),
    oikomi: numericOrNull(row.recall_oikomi),
    sashi: numericOrNull(row.recall_sashi),
    senkou: numericOrNull(row.recall_senkou),
  },
  supportPerClass: {
    nige: row.support_nige,
    oikomi: row.support_oikomi,
    sashi: row.support_sashi,
    senkou: row.support_senkou,
  },
});

export const getRunningStyleMetricsForActiveModel = async (
  category: string,
): Promise<RunningStyleMetrics | null> => {
  const active = await getActiveRunningStyleModel(category);
  if (active === null) return null;
  const result = await getDb().execute<RunningStyleEvaluationRow>(sql`
    select *
      from running_style_model_evaluations
     where model_version = ${active.modelVersion}
       and category = ${category}
     order by evaluated_at desc
     limit 1
  `);
  const row = result.rows[0];
  if (row === undefined) return null;
  return parseRunningStyleMetricsRow(row);
};

interface CornerPositionEvaluationRow extends Record<string, unknown> {
  model_version: string;
  category: string;
  evaluation_window_from: string;
  evaluation_window_to: string;
  race_count: number;
  prediction_count: number;
  corner_1_mae: string | null;
  corner_3_mae: string | null;
  corner_4_mae: string | null;
  mean_mae: string | null;
  corner_1_top3_agreement: string | null;
  evaluated_at: Date;
}

const parseCornerPositionMetricsRow = (
  row: CornerPositionEvaluationRow,
): CornerPositionMetrics => ({
  category: row.category,
  corner1Mae: numericOrNull(row.corner_1_mae),
  corner1Top3Agreement: numericOrNull(row.corner_1_top3_agreement),
  corner3Mae: numericOrNull(row.corner_3_mae),
  corner4Mae: numericOrNull(row.corner_4_mae),
  evaluatedAt: row.evaluated_at,
  evaluationWindowFrom: row.evaluation_window_from,
  evaluationWindowTo: row.evaluation_window_to,
  meanMae: numericOrNull(row.mean_mae),
  modelVersion: row.model_version,
  predictionCount: row.prediction_count,
  raceCount: row.race_count,
});

export const getCornerPositionMetricsForActiveModel = async (
  category: string,
): Promise<CornerPositionMetrics | null> => {
  const active = await getActiveCornerPositionModel(category);
  if (active === null) return null;
  const result = await getDb().execute<CornerPositionEvaluationRow>(sql`
    select *
      from corner_position_model_evaluations
     where model_version = ${active.modelVersion}
       and category = ${category}
     order by evaluated_at desc
     limit 1
  `);
  const row = result.rows[0];
  if (row === undefined) return null;
  return parseCornerPositionMetricsRow(row);
};

interface CornerPredictionRow extends Record<string, unknown> {
  source: string;
  kaisai_nen: string;
  kaisai_tsukihi: string;
  keibajo_code: string;
  race_bango: string;
  ketto_toroku_bango: string;
  umaban: number;
  model_version: string;
  corner_1_pred: string | null;
  corner_3_pred: string | null;
  corner_4_pred: string | null;
}

const parseCornerPredictionRow = (row: CornerPredictionRow): RaceCornerPositionRow => ({
  corner1Pred: numericOrNull(row.corner_1_pred),
  corner3Pred: numericOrNull(row.corner_3_pred),
  corner4Pred: numericOrNull(row.corner_4_pred),
  kaisaiNen: row.kaisai_nen,
  kaisaiTsukihi: row.kaisai_tsukihi,
  keibajoCode: row.keibajo_code,
  kettoTorokuBango: row.ketto_toroku_bango,
  modelVersion: row.model_version,
  raceBango: row.race_bango,
  source: row.source,
  umaban: row.umaban,
});

export const getRaceCornerPositionPredictions = async (
  keys: RaceLookupKeys,
  modelVersion: string,
): Promise<RaceCornerPositionRow[]> => {
  const result = await getDb().execute<CornerPredictionRow>(sql`
    select source, kaisai_nen, kaisai_tsukihi, keibajo_code, race_bango,
           ketto_toroku_bango, umaban, model_version,
           corner_1_pred, corner_3_pred, corner_4_pred
      from race_corner_position_model_predictions
     where model_version = ${modelVersion}
       and source = ${keys.source}
       and kaisai_nen = ${keys.kaisaiNen}
       and kaisai_tsukihi = ${keys.kaisaiTsukihi}
       and keibajo_code = ${keys.keibajoCode}
       and race_bango = ${keys.raceBango}
     order by umaban
  `);
  return result.rows.map(parseCornerPredictionRow);
};
