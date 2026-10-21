// Run with bun. Read-only NAR Stage1 raw-score probe, not a serving scorer.
// Routing, Transformer fusion and post-model adjustments remain in Python.
import { scoreXgboostModel, type XgboostModel } from "xgboost-json-tree";

import { coerceFeature, projectFeatureRow, type FeatureEntry } from "./feature-projection";
import stage1Routing from "../../../finish-position-predict-container/src/predict_lib/stage1_routing.json";
import prophetPolicy from "../../../finish-position-predict-container/src/predict_lib/prophet_cell_policy.json";
import { adjustShadowProphetScores, type ShadowProphetConfig } from "./shadow-prophet-adjustment";

export interface NarStage1ShadowInput {
  entries: ReadonlyArray<FeatureEntry>;
  featureNames: ReadonlyArray<string>;
  model: XgboostModel;
  modelVersion: string;
}

export interface NarStage1RawPrediction {
  kettoTorokuBango: string;
  rawScore: number;
  umaban: number;
}

export interface NarStage1ShadowResult {
  modelVersion: string;
  predictions: NarStage1RawPrediction[];
  raceId: string;
  shadowOnly: true;
}

export interface NarStage1ProphetInput extends NarStage1ShadowInput {
  servedSignature: string;
  prophetConfig?: ShadowProphetConfig;
}
export interface NarStage1ProphetPrediction {
  kettoTorokuBango: string;
  predictedRank: number;
  predictedScore: number;
  umaban: number;
}
export interface NarStage1ProphetResult {
  modelVersion: string;
  predictions: NarStage1ProphetPrediction[];
  raceId: string;
  servedSignature: string;
  shadowOnly: true;
  scope: "stage1-plus-prophet-only";
}

const SUPPORTED_SIGNATURE =
  "v1;mode=nar-transformer-blend;stage2=transformer-fused;stage2-model=iter40-nar-settransformer-blend-v1-nowin1;stage1=odds-missing:direct-stage1;final=iter12-nar-xgb-hpo-v8-stage1-marketfree-184";
const PROPHET_DECISION = prophetPolicy.categories.nar.signatures.sim[SUPPORTED_SIGNATURE];

const comparePredictions = (
  left: NarStage1ProphetPrediction,
  right: NarStage1ProphetPrediction,
): number =>
  right.predictedScore - left.predictedScore ||
  left.kettoTorokuBango.localeCompare(right.kettoTorokuBango);

const isFiniteOrMissing = (value: number): boolean => Number.isNaN(value) || Number.isFinite(value);

const requireIdentity = (entry: FeatureEntry, name: string): string => {
  const value = entry[name];
  if ((typeof value !== "string" && typeof value !== "number") || String(value).trim() === "") {
    throw new Error(`NAR Stage1 shadow: missing ${name}`);
  }
  return String(value).trim();
};

const scoreEntry = (entry: FeatureEntry, input: NarStage1ShadowInput): NarStage1RawPrediction => {
  const kettoTorokuBango = requireIdentity(entry, "ketto_toroku_bango");
  const umaban = Number(requireIdentity(entry, "umaban"));
  if (!Number.isSafeInteger(umaban) || umaban <= 0) {
    throw new Error("NAR Stage1 shadow: invalid horse number");
  }
  input.featureNames.forEach((name) => {
    if (!Object.hasOwn(entry, name)) {
      throw new Error(`NAR Stage1 shadow: missing model feature ${name}`);
    }
    if (!Number.isFinite(coerceFeature(entry[name]))) {
      throw new Error(`NAR Stage1 shadow: non-finite model feature ${name}`);
    }
  });
  const features = projectFeatureRow(entry, input.featureNames, "xgboost");
  // NaN is a missing feature (XGBoost default_left); only +/-Infinity is an overflow.
  if (!features.every(isFiniteOrMissing)) {
    throw new Error("NAR Stage1 shadow: float32 feature overflow");
  }
  const rawScore = scoreXgboostModel({ features, model: input.model });
  if (!Number.isFinite(rawScore)) throw new Error("NAR Stage1 shadow: non-finite score");
  return { kettoTorokuBango, rawScore, umaban };
};

export const scoreNarStage1Shadow = (input: NarStage1ShadowInput): NarStage1ShadowResult => {
  if (input.modelVersion !== stage1Routing.nar.model_version) {
    throw new Error("NAR Stage1 shadow: unexpected model version");
  }
  if (
    input.featureNames.length !== stage1Routing.nar.feature_count ||
    input.model.num_feature !== input.featureNames.length ||
    input.featureNames.some((name) => name.trim() === "") ||
    new Set(input.featureNames).size !== input.featureNames.length ||
    (input.model.feature_names.length > 0 &&
      (input.model.feature_names.length !== input.featureNames.length ||
        input.model.feature_names.some((name, index) => name !== input.featureNames[index])))
  ) {
    throw new Error("NAR Stage1 shadow: invalid ordered feature metadata");
  }
  if (input.entries.length === 0) throw new Error("NAR Stage1 shadow: empty race");
  const raceIds = new Set(input.entries.map((entry) => requireIdentity(entry, "race_id")));
  const raceId = [...raceIds][0]!;
  if (raceIds.size !== 1 || !/^nar:\d{4}:\d{4}:\d{2}:\d{2}$/.test(raceId)) {
    throw new Error("NAR Stage1 shadow: mixed or invalid race identity");
  }
  const predictions = input.entries.map((entry) => scoreEntry(entry, input));
  if (
    new Set(predictions.map((prediction) => prediction.kettoTorokuBango)).size !==
      predictions.length ||
    new Set(predictions.map((prediction) => prediction.umaban)).size !== predictions.length
  ) {
    throw new Error("NAR Stage1 shadow: duplicate horse identity");
  }
  return { modelVersion: input.modelVersion, predictions, raceId, shadowOnly: true };
};

// Caller supplies the independently observed Python signature. This does not
// implement routing or MPX/neural overrides and must never serve predictions.
export const scoreNarStage1ProphetShadow = (
  input: NarStage1ProphetInput,
): NarStage1ProphetResult => {
  if (input.servedSignature !== SUPPORTED_SIGNATURE) {
    throw new Error("NAR Stage1 Prophet shadow: unsupported served signature");
  }
  const raw = scoreNarStage1Shadow(input);
  const predictions = adjustShadowProphetScores({
    category: "nar",
    config: input.prophetConfig,
    decision: PROPHET_DECISION,
    entries: input.entries,
    predictions: raw.predictions.map((prediction) => ({
      kettoTorokuBango: prediction.kettoTorokuBango,
      predictedRank: 0,
      predictedScore: prediction.rawScore,
      umaban: prediction.umaban,
    })),
  })
    .sort(comparePredictions)
    .map((prediction, index) => ({ ...prediction, predictedRank: index + 1 }));
  return {
    ...raw,
    predictions,
    servedSignature: input.servedSignature,
    scope: "stage1-plus-prophet-only",
  };
};
