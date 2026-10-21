// Run with bun. Positional feature projection, a faithful TS port of
// apps/finish-position-predict-container/src/predict_lib/scorer.py
// (build_feature_row / _coerce / _to_float32). This is the MOST safety-critical
// parity surface: CatBoost / XGBoost score POSITIONALLY, so an entry record is
// projected onto the model's exact ordered feature_names list and any column
// drift silently mis-scores. The order MUST match the model's metadata.json
// feature_names (= booster_pool's per-member ordered list).
//
// Coercion rules (scorer._coerce): None / undefined / "" -> missing (NaN); bool
// -> 1/0; number -> itself; other -> Number(String(value).trim()). A cached
// parquet NULL arrives as null/undefined and is projected as missing, as the
// models saw it in training (nulls reached CatBoost / XGBoost as NaN), so the
// CatBoost nan_value_treatment / XGBoost default_left branch is taken. XGBoost
// rows are float32-quantised for bit-faithful parity (scorer._to_float32).

import type { FeatureCell } from "catboost-json-tree";

export type ScoringArchitecture = "catboost" | "xgboost";

const ZERO = 0;
const ONE = 1;

const float32View = new Float32Array(ONE);

// Round-trip a float through 32-bit precision (mirrors struct.pack('f', ...)).
const toFloat32 = (value: number): number => {
  float32View[ZERO] = value;
  return float32View[ZERO]!;
};

// Coerce an arbitrary cell to a finite float, treating null / undefined / "" as
// 0.0. Mirrors scorer._coerce exactly so the projected vector matches the
// container row-for-row. hyparquet returns INT32/INT64 parquet columns as
// number / bigint, so bigint is converted explicitly (Python pandas yields int
// -> float on the same columns, so the float values agree).
export const coerceFeature = (value: unknown): number => {
  if (value === null || value === undefined) return ZERO;
  if (typeof value === "boolean") return value ? ONE : ZERO;
  if (typeof value === "number") return value;
  if (typeof value === "bigint") return Number(value);
  if (typeof value !== "string") return ZERO;
  const text = value.trim();
  if (text === "") return ZERO;
  return Number(text);
};

export type FeatureEntry = Record<string, unknown>;

const isMissingFeature = (value: unknown): boolean =>
  value === null || value === undefined || (typeof value === "string" && value.trim() === "");

// Model-input value: missing cells are NaN (training's missing representation);
// coerceFeature stays finite for null so callers can validate non-numeric input.
const projectFeatureValue = (value: unknown): number =>
  isMissingFeature(value) ? Number.NaN : coerceFeature(value);

// Project one entry onto feature_names order as a float row. XGBoost rows are
// float32-quantised; CatBoost rows stay float64. Mirrors build_feature_row.
export const projectFeatureRow = (
  entry: FeatureEntry,
  featureNames: ReadonlyArray<string>,
  architecture: ScoringArchitecture,
): number[] => {
  const raw = featureNames.map((name) => projectFeatureValue(entry[name]));
  if (architecture === "xgboost") return raw.map(toFloat32);
  return raw;
};

// CatBoost's TS scorer consumes FeatureCell{value, isMissing}; a missing cell is
// flagged so the model's nan_value_treatment applies, as in training and in the
// container (scorer._coerce -> NaN).
export const projectCatBoostCells = (
  entry: FeatureEntry,
  featureNames: ReadonlyArray<string>,
): FeatureCell[] =>
  featureNames.map((name) => ({
    isMissing: isMissingFeature(entry[name]),
    value: projectFeatureValue(entry[name]),
  }));

export const assertFeatureCount = (featureNames: ReadonlyArray<string>, expected: number): void => {
  if (featureNames.length !== expected) {
    throw new Error(`expected ${expected} features, metadata has ${featureNames.length}`);
  }
};
