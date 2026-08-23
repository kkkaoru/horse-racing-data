// Worker-native shadow scorer for the current JRA production CatBoost routes.
// This module has deliberately no Neon/KV/Cache API writer: callers can compare
// its result with the container, but cannot accidentally serve it.

import { parseCatBoostJsonModel, scoreCatBoostModel, type CatBoostModel } from "catboost-json-tree";

import { buildModelKey } from "./model-loader";
import { coerceFeature, projectCatBoostCells, type FeatureEntry } from "./feature-projection";

const CATEGORY = "jra";
const MODEL_FILE = "model.json";
const METADATA_FILE = "metadata.json";
const STAGE1_SCORE_STDDEV_THRESHOLD = 0.4;
const FIRST_RANK = 1;

const ROUTING_FIELDS = [
  "grade_code",
  "keibajo_code",
  "kyoso_joken_code",
  "race_id",
  "track_code",
] as const;

const WITHIN_RACE_LEAK_COLUMNS = new Set([
  "target_corner_1_norm",
  "target_corner_2_norm",
  "target_corner_3_norm",
  "target_corner_4_norm",
  "target_running_style_class",
]);

const FATHER_FATHER_FEATURES = [
  "gsire_dist_surface_win_eb",
  "gsire_dist_surface_top3_eb",
  "gsire_venue_win_eb",
  "gsire_dist_surface_edge",
  "gsire_dist_surface_logn",
  "gsire_dist_surface_win_rank_in_race",
  "gsire_dist_surface_edge_rank_in_race",
] as const;

export type JraShadowVariant =
  | "sim"
  | "jockey_pedigree_703"
  | "prior_corner_dirt_smallfield_005"
  | "stage1_marketfree";

export interface JraShadowModelSpec {
  architecture: "catboost";
  featureCount: number;
  modelVersion: string;
  variant: JraShadowVariant;
}

export const JRA_SHADOW_MODEL_SPECS: Readonly<Record<JraShadowVariant, JraShadowModelSpec>> = {
  sim: {
    architecture: "catboost",
    featureCount: 250,
    modelVersion: "jra-cb-v9-sim-2013-clean",
    variant: "sim",
  },
  jockey_pedigree_703: {
    architecture: "catboost",
    featureCount: 269,
    modelVersion: "jra-cb-v9-sim-2013-clean-jockey-pedigree269",
    variant: "jockey_pedigree_703",
  },
  prior_corner_dirt_smallfield_005: {
    architecture: "catboost",
    featureCount: 274,
    modelVersion: "jra-cb-v10-prior-corner274-2013",
    variant: "prior_corner_dirt_smallfield_005",
  },
  stage1_marketfree: {
    architecture: "catboost",
    featureCount: 235,
    modelVersion: "jra-cb-stage1-marketfree235-2013",
    variant: "stage1_marketfree",
  },
};

export interface LoadedJraShadowModel {
  featureNames: string[];
  model: CatBoostModel;
  spec: JraShadowModelSpec;
}

interface ModelMetadata {
  architecture?: unknown;
  feature_count?: unknown;
  feature_names?: unknown;
  model_version?: unknown;
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const textCell = (entry: FeatureEntry, field: string): string | null => {
  const value = entry[field];
  if (value === null || value === undefined) return null;
  const text =
    typeof value === "string"
      ? value.trim()
      : typeof value === "number" || typeof value === "bigint" || typeof value === "boolean"
        ? value.toString().trim()
        : "";
  return text === "" ? null : text;
};

const consistentRaceValue = (entries: ReadonlyArray<FeatureEntry>, field: string): string => {
  const values = new Set(entries.map((entry) => textCell(entry, field)));
  if (values.size !== 1 || values.has(null)) {
    throw new Error(`final feature contract: ${field} must be present and constant within race`);
  }
  return [...values][0]!;
};

const consistentGradeCode = (entries: ReadonlyArray<FeatureEntry>): string | null => {
  if (entries.some((entry) => !Object.hasOwn(entry, "grade_code"))) {
    throw new Error("final feature contract: grade_code must be present on every row");
  }
  const values = new Set(entries.map((entry) => textCell(entry, "grade_code")));
  if (values.size !== 1) {
    throw new Error("final feature contract: grade_code must be constant within race");
  }
  return [...values][0]!;
};

const assertRoutingContract = (entries: ReadonlyArray<FeatureEntry>): void => {
  consistentGradeCode(entries);
  ROUTING_FIELDS.filter((field) => field !== "grade_code").forEach((field) =>
    consistentRaceValue(entries, field),
  );
};

const isJraDirt = (trackCode: string): boolean => {
  const code = Number(trackCode);
  return Number.isInteger(code) && code >= 23 && code <= 29;
};

const optionalNumber = (value: unknown): number | null => {
  if (value === null || value === undefined || value === "") return null;
  const number = coerceFeature(value);
  return Number.isFinite(number) ? number : null;
};

const isRankPermutation = (ranks: ReadonlyArray<number>): boolean =>
  new Set(ranks).size === ranks.length &&
  ranks.every((rank) => Number.isInteger(rank) && rank >= 1 && rank <= ranks.length);

const rankFromPopularityScore = (value: unknown, runnerCount: number): number | null => {
  const score = optionalNumber(value);
  if (score === null || score < 0 || score > 1) return null;
  const rankValue = score * (runnerCount - 1) + 1;
  const rank = Math.round(rankValue);
  return Math.abs(rankValue - rank) <= 1e-9 ? rank : null;
};

const hasFreshOdds = (
  entries: ReadonlyArray<FeatureEntry>,
  preservedOddsGateEnabled: boolean,
): boolean => {
  if (!preservedOddsGateEnabled) {
    return entries.some((entry) => {
      const rank = optionalNumber(entry.tansho_ninkijun);
      return rank !== null && rank > 0;
    });
  }
  if (entries.length < 2) return false;
  const canonicalRanks = entries.map((entry) => optionalNumber(entry.tansho_ninkijun));
  const ranks = canonicalRanks.every((rank) => rank !== null && rank > 0)
    ? (canonicalRanks as number[])
    : entries.map((entry) => rankFromPopularityScore(entry.popularity_score, entries.length));
  if (ranks.some((rank) => rank === null) || !isRankPermutation(ranks as number[])) return false;
  const odds = entries.map((entry) => optionalNumber(entry.tansho_odds));
  if (odds.some((odd) => odd === null || odd <= 0)) return false;
  const board = ranks
    .map((rank, index) => [rank!, odds[index]!] as const)
    .sort((left, right) => left[0] - right[0]);
  return board.every((entry, index) => index === 0 || board[index - 1]![1] <= entry[1]);
};

// Mirrors the current Python rule order. grade_code is retained as a mandatory
// final-cache dimension even though today's JRA rules do not branch on it.
export const selectJraShadowModel = (
  entries: ReadonlyArray<FeatureEntry>,
  options: { preservedOddsGateEnabled?: boolean } = {},
): JraShadowModelSpec => {
  if (entries.length === 0) throw new Error("final feature contract: race has no entries");
  assertRoutingContract(entries);
  if (!hasFreshOdds(entries, options.preservedOddsGateEnabled === true)) {
    return JRA_SHADOW_MODEL_SPECS.stage1_marketfree;
  }

  const first = entries[0]!;
  const raceClass = textCell(first, "kyoso_joken_code")!;
  if (raceClass === "703") return JRA_SHADOW_MODEL_SPECS.jockey_pedigree_703;

  const trackCode = textCell(first, "track_code")!;
  if (isJraDirt(trackCode) && entries.length <= 10 && raceClass === "005") {
    return JRA_SHADOW_MODEL_SPECS.prior_corner_dirt_smallfield_005;
  }
  if (textCell(first, "keibajo_code") === "02") {
    return JRA_SHADOW_MODEL_SPECS.jockey_pedigree_703;
  }
  return JRA_SHADOW_MODEL_SPECS.sim;
};

const parseMetadata = (value: unknown, spec: JraShadowModelSpec): string[] => {
  if (!isRecord(value)) throw new Error("model metadata must be an object");
  const metadata: ModelMetadata = value;
  if (!Array.isArray(metadata.feature_names)) {
    throw new Error("model metadata feature_names must be an array");
  }
  const featureNames = metadata.feature_names;
  if (!featureNames.every((name) => typeof name === "string" && name !== "")) {
    throw new Error("model metadata feature_names must contain non-empty strings");
  }
  if (new Set(featureNames).size !== featureNames.length) {
    throw new Error("model metadata feature_names must be unique");
  }
  if (metadata.feature_count !== spec.featureCount || featureNames.length !== spec.featureCount) {
    throw new Error(
      `model metadata feature count mismatch for ${spec.modelVersion}: expected ${spec.featureCount}`,
    );
  }
  if (
    typeof metadata.model_version !== "string" ||
    metadata.model_version.toLowerCase() !== spec.modelVersion.toLowerCase()
  ) {
    throw new Error(`model metadata version mismatch for ${spec.modelVersion}`);
  }
  if (typeof metadata.architecture !== "string" || !metadata.architecture.startsWith("catboost")) {
    throw new Error(`model metadata architecture mismatch for ${spec.modelVersion}`);
  }
  const leaks = featureNames.filter((name) => WITHIN_RACE_LEAK_COLUMNS.has(name));
  if (leaks.length > 0) {
    throw new Error(
      `model metadata contains within-race leak columns: ${leaks.sort((left, right) => left.localeCompare(right)).join(", ")}`,
    );
  }
  if (
    spec.variant === "jockey_pedigree_703" &&
    !FATHER_FATHER_FEATURES.every((name) => featureNames.includes(name))
  ) {
    throw new Error("jockey-pedigree model metadata is missing father-father features");
  }
  return featureNames as string[];
};

const getJson = async (bucket: R2Bucket, key: string): Promise<unknown> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  return object.json();
};

// Loads only the already-selected model (model JSON + its metadata), never all
// cell variants. This bounds both R2 reads and isolate memory.
export const loadSelectedJraShadowModel = async (
  bucket: R2Bucket,
  spec: JraShadowModelSpec,
): Promise<LoadedJraShadowModel> => {
  const [modelJson, metadataJson] = await Promise.all([
    getJson(bucket, buildModelKey(CATEGORY, spec.modelVersion, MODEL_FILE)),
    getJson(bucket, buildModelKey(CATEGORY, spec.modelVersion, METADATA_FILE)),
  ]);
  return {
    featureNames: parseMetadata(metadataJson, spec),
    model: parseCatBoostJsonModel(modelJson),
    spec,
  };
};

const assertFinalFeatureContract = (
  entries: ReadonlyArray<FeatureEntry>,
  loaded: LoadedJraShadowModel,
): void => {
  assertRoutingContract(entries);
  entries.forEach((entry, rowIndex) => {
    const missing = loaded.featureNames.filter((name) => !Object.hasOwn(entry, name));
    if (missing.length > 0) {
      throw new Error(
        `final feature contract: row ${rowIndex} is missing model features: ${missing.join(", ")}`,
      );
    }
    const invalid = loaded.featureNames.filter((name) => {
      const value = entry[name];
      return value !== null && value !== undefined && !Number.isFinite(coerceFeature(value));
    });
    if (invalid.length > 0) {
      throw new Error(
        `final feature contract: row ${rowIndex} has non-numeric model features: ${invalid.join(", ")}`,
      );
    }
    if (textCell(entry, "ketto_toroku_bango") === null || textCell(entry, "umaban") === null) {
      throw new Error(`final feature contract: row ${rowIndex} is missing horse identity`);
    }
  });
};

export interface JraShadowPrediction {
  kettoTorokuBango: string;
  predictedRank: number;
  predictedScore: number;
  umaban: number;
}

export interface JraShadowScoreResult {
  gradeCode: string | null;
  modelVersion: string;
  predictions: JraShadowPrediction[];
  raceId: string;
  scoreStddev: number;
  shadowOnly: true;
  stage1RescoreRequired: boolean;
  variant: JraShadowVariant;
}

const populationStddev = (scores: ReadonlyArray<number>): number => {
  if (scores.length < 2) return 0;
  const mean = scores.reduce((sum, score) => sum + score, 0) / scores.length;
  const variance = scores.reduce((sum, score) => sum + (score - mean) ** 2, 0) / scores.length;
  return Math.sqrt(variance);
};

export const scoreJraRaceShadow = (
  entries: ReadonlyArray<FeatureEntry>,
  loaded: LoadedJraShadowModel,
): JraShadowScoreResult => {
  if (entries.length === 0) throw new Error("final feature contract: race has no entries");
  assertFinalFeatureContract(entries, loaded);
  const scores = entries.map((entry) =>
    scoreCatBoostModel({
      features: projectCatBoostCells(entry, loaded.featureNames),
      model: loaded.model,
    }),
  );
  const scoreStddev = populationStddev(scores);
  const predictions = entries
    .map((entry, index) => ({
      kettoTorokuBango: textCell(entry, "ketto_toroku_bango")!,
      predictedRank: 0,
      predictedScore: scores[index]!,
      umaban: Number(textCell(entry, "umaban")),
    }))
    .sort((left, right) =>
      right.predictedScore !== left.predictedScore
        ? right.predictedScore - left.predictedScore
        : left.kettoTorokuBango < right.kettoTorokuBango
          ? -1
          : left.kettoTorokuBango > right.kettoTorokuBango
            ? 1
            : 0,
    )
    .map((prediction, index) => ({ ...prediction, predictedRank: index + FIRST_RANK }));
  return {
    gradeCode: consistentGradeCode(entries),
    modelVersion: loaded.spec.modelVersion,
    predictions,
    raceId: consistentRaceValue(entries, "race_id"),
    scoreStddev,
    shadowOnly: true,
    stage1RescoreRequired:
      loaded.spec.variant !== "stage1_marketfree" && scoreStddev < STAGE1_SCORE_STDDEV_THRESHOLD,
    variant: loaded.spec.variant,
  };
};
