// Run with bun. Worker-native shadow scorer for the two production Ban-ei
// CatBoost routes. It has no Neon/KV writer; callers must explicitly compare
// shadow output before enabling serving.

import { parseCatBoostJsonModel, scoreCatBoostModel, type CatBoostModel } from "catboost-json-tree";

import { buildModelKey } from "./model-loader";
import { coerceFeature, projectCatBoostCells, type FeatureEntry } from "./feature-projection";

const CATEGORY = "ban-ei";
const MODEL_FILE = "model.json";
const METADATA_FILE = "metadata.json";
const FIRST_RANK = 1;
// Model-version keys are immutable production artifacts. Cache only the last
// fully parsed model per R2 binding, after all metadata checks pass. This avoids
// repeated 4 MB R2 JSON reads and CatBoost tree parsing on warm Queue isolates,
// while bounding retained isolate memory when routes alternate.
const MODEL_CACHE = new WeakMap<R2Bucket, CachedBaneiShadowModel>();

export type BaneiShadowVariant = "base" | "sim";

export interface BaneiShadowModelSpec {
  architecture: "catboost";
  featureCount: number;
  modelVersion: string;
  variant: BaneiShadowVariant;
}

export const BANEI_SHADOW_MODEL_SPECS: Readonly<Record<BaneiShadowVariant, BaneiShadowModelSpec>> =
  {
    base: {
      architecture: "catboost",
      featureCount: 111,
      modelVersion: "banei-cb-v8-window2011-wf-15y",
      variant: "base",
    },
    sim: {
      architecture: "catboost",
      featureCount: 130,
      modelVersion: "banei-cb-v9-sim-2011",
      variant: "sim",
    },
  };

export interface LoadedBaneiShadowModel {
  featureNames: string[];
  model: CatBoostModel;
  spec: BaneiShadowModelSpec;
}

interface CachedBaneiShadowModel {
  loaded: LoadedBaneiShadowModel;
  modelVersion: string;
}

interface ModelMetadata {
  architecture?: unknown;
  feature_count?: unknown;
  feature_names?: unknown;
  model_version?: unknown;
}

export interface BaneiShadowPrediction {
  kettoTorokuBango: string;
  predictedRank: number;
  predictedScore: number;
  umaban: number;
}

export interface BaneiShadowScoreResult {
  gradeCode: string | null;
  modelVersion: string;
  predictions: BaneiShadowPrediction[];
  raceId: string;
  shadowOnly: true;
  variant: BaneiShadowVariant;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const textCell = (entry: FeatureEntry, field: string): string | null => {
  const value = entry[field];
  if (value === null || value === undefined) return null;
  if (
    typeof value !== "string" &&
    typeof value !== "number" &&
    typeof value !== "bigint" &&
    typeof value !== "boolean"
  )
    return null;
  const text = value.toString().trim();
  return text === "" ? null : text;
};

const constantCell = (
  entries: ReadonlyArray<FeatureEntry>,
  field: string,
  allowNull: boolean,
): string | null => {
  const values = new Set(entries.map((entry) => textCell(entry, field)));
  if (values.size !== 1 || (!allowNull && values.has(null))) {
    throw new Error(`Ban-ei final feature contract: ${field} must be constant within race`);
  }
  return [...values][0] ?? null;
};

const assertRaceRows = (entries: ReadonlyArray<FeatureEntry>): void => {
  if (entries.length === 0) throw new Error("Ban-ei final feature contract: race has no entries");
  constantCell(entries, "race_id", false);
  constantCell(entries, "grade_code", true);
  entries.forEach((entry, rowIndex) => {
    if (textCell(entry, "ketto_toroku_bango") === null || textCell(entry, "umaban") === null) {
      throw new Error(`Ban-ei final feature contract: row ${rowIndex} is missing horse identity`);
    }
  });
};

export const selectBaneiShadowModel = (
  entries: ReadonlyArray<FeatureEntry>,
): BaneiShadowModelSpec => {
  assertRaceRows(entries);
  return constantCell(entries, "grade_code", true) === "E"
    ? BANEI_SHADOW_MODEL_SPECS.base
    : BANEI_SHADOW_MODEL_SPECS.sim;
};

const parseMetadata = (value: unknown, spec: BaneiShadowModelSpec): string[] => {
  if (!isRecord(value)) throw new Error("Ban-ei model metadata must be an object");
  const metadata: ModelMetadata = value;
  if (!Array.isArray(metadata.feature_names)) {
    throw new Error("Ban-ei model metadata feature_names must be an array");
  }
  const featureNames = metadata.feature_names;
  if (!featureNames.every((name) => typeof name === "string" && name !== "")) {
    throw new Error("Ban-ei model metadata feature_names must contain non-empty strings");
  }
  if (new Set(featureNames).size !== featureNames.length) {
    throw new Error("Ban-ei model metadata feature_names must be unique");
  }
  if (metadata.feature_count !== spec.featureCount || featureNames.length !== spec.featureCount) {
    throw new Error(`Ban-ei model metadata feature count mismatch for ${spec.modelVersion}`);
  }
  if (
    typeof metadata.model_version !== "string" ||
    metadata.model_version.toLowerCase() !== spec.modelVersion.toLowerCase()
  ) {
    throw new Error(`Ban-ei model metadata version mismatch for ${spec.modelVersion}`);
  }
  if (typeof metadata.architecture !== "string" || !metadata.architecture.startsWith("catboost")) {
    throw new Error(`Ban-ei model metadata architecture mismatch for ${spec.modelVersion}`);
  }
  return featureNames as string[];
};

const getJson = async (bucket: R2Bucket, key: string): Promise<unknown> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  return object.json();
};

export const loadSelectedBaneiShadowModel = async (
  bucket: R2Bucket,
  spec: BaneiShadowModelSpec,
): Promise<LoadedBaneiShadowModel> => {
  const cached = MODEL_CACHE.get(bucket);
  if (cached?.modelVersion === spec.modelVersion) return cached.loaded;
  const [modelJson, metadataJson] = await Promise.all([
    getJson(bucket, buildModelKey(CATEGORY, spec.modelVersion, MODEL_FILE)),
    getJson(bucket, buildModelKey(CATEGORY, spec.modelVersion, METADATA_FILE)),
  ]);
  const loaded: LoadedBaneiShadowModel = {
    featureNames: parseMetadata(metadataJson, spec),
    model: parseCatBoostJsonModel(modelJson),
    spec,
  };
  MODEL_CACHE.set(bucket, { loaded, modelVersion: spec.modelVersion });
  return loaded;
};

const assertModelFeatures = (
  entries: ReadonlyArray<FeatureEntry>,
  featureNames: ReadonlyArray<string>,
): void => {
  entries.forEach((entry, rowIndex) => {
    const missing = featureNames.filter((name) => !Object.hasOwn(entry, name));
    if (missing.length > 0) {
      throw new Error(
        `Ban-ei final feature contract: row ${rowIndex} is missing model features: ${missing.join(", ")}`,
      );
    }
    const invalid = featureNames.filter((name) => {
      const value = entry[name];
      return value !== null && value !== undefined && !Number.isFinite(coerceFeature(value));
    });
    if (invalid.length > 0) {
      throw new Error(
        `Ban-ei final feature contract: row ${rowIndex} has non-numeric model features: ${invalid.join(", ")}`,
      );
    }
  });
};

export const scoreBaneiRaceShadow = (
  entries: ReadonlyArray<FeatureEntry>,
  loaded: LoadedBaneiShadowModel,
): BaneiShadowScoreResult => {
  assertRaceRows(entries);
  assertModelFeatures(entries, loaded.featureNames);
  const predictions = entries
    .map((entry) => ({
      kettoTorokuBango: textCell(entry, "ketto_toroku_bango")!,
      predictedRank: 0,
      predictedScore: scoreCatBoostModel({
        features: projectCatBoostCells(entry, loaded.featureNames),
        model: loaded.model,
      }),
      umaban: Number(textCell(entry, "umaban")),
    }))
    .sort((left, right) =>
      right.predictedScore !== left.predictedScore
        ? right.predictedScore - left.predictedScore
        : left.kettoTorokuBango !== right.kettoTorokuBango
          ? left.kettoTorokuBango.localeCompare(right.kettoTorokuBango)
          : left.umaban - right.umaban,
    )
    .map((prediction, index) => ({ ...prediction, predictedRank: index + FIRST_RANK }));
  return {
    gradeCode: constantCell(entries, "grade_code", true),
    modelVersion: loaded.spec.modelVersion,
    predictions,
    raceId: constantCell(entries, "race_id", false)!,
    shadowOnly: true,
    variant: loaded.spec.variant,
  };
};
