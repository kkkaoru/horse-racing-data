// Run with bun. Positional feature projection, a faithful TS port of
// apps/finish-position-predict-container/src/predict_lib/scorer.py
// (build_feature_row / _coerce / _to_float32). This is the MOST safety-critical
// parity surface: CatBoost / XGBoost score POSITIONALLY, so an entry record is
// projected onto the model's exact ordered feature_names list and any column
// drift silently mis-scores. The order MUST match the model's metadata.json
// feature_names (= booster_pool's per-member ordered list).
//
// Coercion rules (scorer._coerce): None / undefined / "" -> 0.0; bool -> 1/0;
// number -> itself; other -> Number(String(value).trim()). A cached parquet NULL
// arrives as null/undefined and becomes 0.0 — so in the production path a
// missing feature is 0.0, NOT NaN, and the CatBoost nan_value_treatment branch
// is never reached for cached rows. XGBoost rows are float32-quantised for bit-
// faithful parity with the native runtime (scorer._to_float32).

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

// Project one entry onto feature_names order as a float row. XGBoost rows are
// float32-quantised; CatBoost rows stay float64. Mirrors build_feature_row.
export const projectFeatureRow = (
  entry: FeatureEntry,
  featureNames: ReadonlyArray<string>,
  architecture: ScoringArchitecture,
): number[] => {
  const raw = featureNames.map((name) => coerceFeature(entry[name]));
  if (architecture === "xgboost") return raw.map(toFloat32);
  return raw;
};

// CatBoost's TS scorer consumes FeatureCell{value, isMissing}. The container
// feeds CatBoost coerced 0.0 (never NaN) for cached rows, so every cell is
// non-missing — isMissing is always false here. Kept as a separate projection
// so the CatBoost scorer API is satisfied without re-introducing a NaN path the
// production container never takes.
export const projectCatBoostCells = (
  entry: FeatureEntry,
  featureNames: ReadonlyArray<string>,
): FeatureCell[] =>
  featureNames.map((name) => ({ isMissing: false, value: coerceFeature(entry[name]) }));

export const assertFeatureCount = (featureNames: ReadonlyArray<string>, expected: number): void => {
  if (featureNames.length !== expected) {
    throw new Error(`expected ${expected} features, metadata has ${featureNames.length}`);
  }
};
