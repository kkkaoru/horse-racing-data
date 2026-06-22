// Run with bun. Worker-native JRA finish-position scoring core — the heart of
// Stage 2 per-race rescore. Pure function: given one race's entry records (read
// from the R2 feature cache with the 5 late-binding columns refreshed) plus the
// loaded CatBoost iter20 + XGBoost xgb-jra-2013-v8 JSON-tree models, it returns
// ranked predictions WITHOUT any DB / I/O. The rescore consumer (task B) reads
// the cache, refreshes late-binding columns, loads the models from R2, calls
// scoreJraRace, then UPSERTs the rows.
//
// Bit-exact parity with the Python production path
// (predict_upcoming._score_one_race_etop2):
//   1. project each entry onto the model's 244 feature_names order (positional)
//   2. CatBoost raw score (RawFormulaVal) + XGBoost raw score (rank:ndcg)
//   3. E-top2 override (CB#2 promotion when XGB#1 == CB#2 and class != 701)
//   4. rank by descending override score, ties broken on ketto (rank.py)
//
// Output mirrors upsert_sql.INSERT_COLUMNS: predicted_top1_prob /
// predicted_top3_prob / predicted_finish_position are null — the v7-lineage
// rankers emit a relevance score + rank, not calibrated probabilities (matches
// upcoming.build_prediction_rows).

import { scoreCatBoostModel, type CatBoostModel } from "catboost-json-tree";
import { scoreXgboostModel, type XgboostModel } from "xgboost-json-tree";

import { applyEtop2Scores, isEtop2OverrideActive } from "./etop2";
import { projectCatBoostCells, projectFeatureRow, type FeatureEntry } from "./feature-projection";

const FIRST_RANK = 1;
const KETTO_FIELD = "ketto_toroku_bango";
const UMABAN_FIELD = "umaban";

export interface JraRaceEntry {
  // The cached feature row + refreshed late-binding columns. Keys are the model
  // feature_names; ketto_toroku_bango / umaban identify the horse in the output.
  features: FeatureEntry;
  kettoTorokuBango: string;
  umaban: number;
}

export interface JraScoredPrediction {
  kettoTorokuBango: string;
  umaban: number;
  predictedScore: number;
  predictedRank: number;
  predictedTop1Prob: null;
  predictedTop3Prob: null;
  predictedFinishPosition: null;
}

export interface ScoreJraRaceInput {
  entries: ReadonlyArray<JraRaceEntry>;
  featureNames: ReadonlyArray<string>;
  catboostModel: CatBoostModel;
  xgboostModel: XgboostModel;
  // kyoso_joken_code for the race (701 == maiden, excluded from E-top2). null
  // when the column is absent (override eligible).
  raceClass: string | null;
}

interface RankableHorse {
  kettoTorokuBango: string;
  umaban: number;
  score: number;
}

// Sort by descending score, ties broken on ascending ketto string. Mirrors
const KETTO_BEFORE = -1;
const KETTO_AFTER = 1;
const KETTO_EQUAL = 0;

// Compare two ketto strings lexicographically (ASCII, matching Python's str <
// used by rank._sort_key). Returns -1 / 0 / 1.
const compareKetto = (left: string, right: string): number => {
  if (left < right) return KETTO_BEFORE;
  return left > right ? KETTO_AFTER : KETTO_EQUAL;
};

// rank._sort_key = (-score, ketto) so ranks are deterministic across re-runs.
const byScoreThenKetto = (left: RankableHorse, right: RankableHorse): number =>
  right.score !== left.score
    ? right.score - left.score
    : compareKetto(left.kettoTorokuBango, right.kettoTorokuBango);

export interface ScoreJraRaceResult {
  predictions: JraScoredPrediction[];
  // True when the E-top2 override changed the rank-1 horse (for smoke logging).
  etop2Fired: boolean;
}

export const scoreJraRace = (input: ScoreJraRaceInput): ScoreJraRaceResult => {
  const cbScores = input.entries.map((entry) =>
    scoreCatBoostModel({
      features: projectCatBoostCells(entry.features, input.featureNames),
      model: input.catboostModel,
    }),
  );
  const xgbScores = input.entries.map((entry) =>
    scoreXgboostModel({
      features: projectFeatureRow(entry.features, input.featureNames, "xgboost"),
      model: input.xgboostModel,
    }),
  );
  const overrideScores = applyEtop2Scores({
    cbScores,
    raceClass: input.raceClass,
    xgbScores,
  });
  const etop2Fired = isEtop2OverrideActive({ cbScores, raceClass: input.raceClass, xgbScores });

  const rankable = input.entries.map<RankableHorse>((entry, index) => ({
    kettoTorokuBango: entry.kettoTorokuBango,
    score: overrideScores[index]!,
    umaban: entry.umaban,
  }));
  const ranked = rankable.slice().sort(byScoreThenKetto);
  const predictions = ranked.map<JraScoredPrediction>((horse, index) => ({
    kettoTorokuBango: horse.kettoTorokuBango,
    predictedFinishPosition: null,
    predictedRank: index + FIRST_RANK,
    predictedScore: horse.score,
    predictedTop1Prob: null,
    predictedTop3Prob: null,
    umaban: horse.umaban,
  }));
  return { etop2Fired, predictions };
};

export { KETTO_FIELD, UMABAN_FIELD };
