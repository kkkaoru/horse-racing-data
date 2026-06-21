// Run with bun. Load the JRA finish-position models (CatBoost iter20 + XGBoost
// E-top2 companion + CatBoost metadata feature_names) from the R2 feature-cache
// bucket for the Worker-native rescore. Mirrors the container's model_meta R2
// object-key layout (build_r2_object_key / build_r2_xgb_etop2_key):
//   finish-position/{category}/{modelVersion}/{file}
//
// The container bakes these JSON artifacts into its image today, so the rescore
// consumer is the FIRST reader to load them from R2 — the upload step is still
// manual (documented in tmp/rescore-consumer-plan.md). An absent object throws
// so the consumer retries the message instead of silently scoring nothing.

import { parseCatBoostJsonModel, type CatBoostModel } from "catboost-json-tree";
import { parseXgboostJsonModel, type XgboostModel } from "xgboost-json-tree";

const R2_KEY_PREFIX = "finish-position";
const MODEL_FILE_NAME = "model.json";
const METADATA_FILE_NAME = "metadata.json";
const JRA_CATEGORY = "jra";
const FEATURE_NAMES_FIELD = "feature_names";

const JRA_CB_MODEL_VERSION = "iter20-jra-cb-2013-v8";
const JRA_XGB_MODEL_VERSION = "xgb-jra-2013-v8";
const JRA_ETOP2_MODEL_VERSION = "iter22-jra-etop2";

export interface JraModels {
  catboostModel: CatBoostModel;
  xgboostModel: XgboostModel;
  featureNames: string[];
}

// finish-position/{category}/{modelVersion}/{file} — model_meta.build_r2_object_key.
export const buildModelKey = (category: string, modelVersion: string, file: string): string =>
  `${R2_KEY_PREFIX}/${category}/${modelVersion}/${file}`;

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((entry) => typeof entry === "string");

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const getJson = async (bucket: R2Bucket, key: string): Promise<unknown> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  return object.json();
};

const featureNamesFrom = (metadata: unknown): string[] => {
  if (!isRecord(metadata)) throw new Error("metadata.json must be an object");
  const featureNames = metadata[FEATURE_NAMES_FIELD];
  if (!isStringArray(featureNames))
    throw new Error("metadata.json feature_names must be a string array");
  return featureNames;
};

// Load CB iter20 + XGB E-top2 + CB metadata.feature_names from R2 in parallel.
export const loadJraModels = async (bucket: R2Bucket): Promise<JraModels> => {
  const cbKey = buildModelKey(JRA_CATEGORY, JRA_CB_MODEL_VERSION, MODEL_FILE_NAME);
  const xgbKey = buildModelKey(JRA_CATEGORY, JRA_XGB_MODEL_VERSION, MODEL_FILE_NAME);
  const metadataKey = buildModelKey(JRA_CATEGORY, JRA_CB_MODEL_VERSION, METADATA_FILE_NAME);
  const [cbJson, xgbJson, metadataJson] = await Promise.all([
    getJson(bucket, cbKey),
    getJson(bucket, xgbKey),
    getJson(bucket, metadataKey),
  ]);
  return {
    catboostModel: parseCatBoostJsonModel(cbJson),
    featureNames: featureNamesFrom(metadataJson),
    xgboostModel: parseXgboostJsonModel(xgbJson),
  };
};

export { JRA_CB_MODEL_VERSION, JRA_ETOP2_MODEL_VERSION, JRA_XGB_MODEL_VERSION };
