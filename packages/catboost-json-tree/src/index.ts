// Run with bun. Pure-TypeScript CatBoost oblivious-tree scorer for the JSON
// produced by `Booster.save_model(format='json')`. Used by the Cloudflare
// Worker (sync-realtime-data-features) to score finish-position ranking on the
// fly for JRA + Ban-ei YetiRank models without pulling Python into the runtime.
//
// Oblivious trees are symmetric: every node at depth `d` uses the same split,
// so a leaf index is the bitmask of split outcomes (LSB = first split). For a
// FloatFeature split the bit is set when `value > border`. NaN / missing values
// follow `nan_value_treatment`: `AsTrue` -> bit 1, otherwise -> bit 0 (NaN is
// treated as smaller than every border). Categorical OneHotFeature splits set
// the bit when the integer hash matches one of the split's `values`. The raw
// ranking score for a row is `scale * sum(leaf_values) + bias`; horses are
// ranked by descending score. Verified bit-exact against CatBoost
// `predict(..., prediction_type='RawFormulaVal')` for the deployed
// jra-cb-v7-lineage model (diff 0.0).

const SPLIT_TYPE_FLOAT = "FloatFeature";
const SPLIT_TYPE_ONE_HOT = "OneHotFeature";
const NAN_TREATMENT_AS_TRUE = "AsTrue";
const BIT_SET = 1;
const BIT_UNSET = 0;
const FIRST_INDEX = 0;
const FIRST_RANK = 1;
const SCALE_PAIR_LENGTH = 2;

export type CatBoostSplitType = "FloatFeature" | "OneHotFeature";

export interface CatBoostFloatFeature {
  featureIndex: number;
  flatFeatureIndex: number;
  hasNans: boolean;
  nanTreatment: string;
}

export interface CatBoostCategoricalFeature {
  featureIndex: number;
  flatFeatureIndex: number;
}

export interface CatBoostSplit {
  splitType: CatBoostSplitType;
  featureIndex: number;
  border: number;
  oneHotValues: number[];
}

export interface CatBoostTree {
  splits: CatBoostSplit[];
  leafValues: number[];
}

export interface CatBoostModel {
  trees: CatBoostTree[];
  floatFeatures: CatBoostFloatFeature[];
  categoricalFeatures: CatBoostCategoricalFeature[];
  scale: number;
  bias: number;
}

export interface FeatureCell {
  value: number;
  isMissing: boolean;
}

export interface ScoreModelInput {
  model: CatBoostModel;
  features: ReadonlyArray<FeatureCell>;
}

export interface RankingRow {
  id: string;
  features: ReadonlyArray<FeatureCell>;
}

export interface ScoreRankingInput {
  model: CatBoostModel;
  rows: ReadonlyArray<RankingRow>;
}

export interface ScoredRow {
  id: string;
  score: number;
  predictedRank: number;
}

const asRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("CatBoost JSON model: expected an object");
  }
  return value as Record<string, unknown>;
};

const asArray = (value: unknown, label: string): unknown[] => {
  if (!Array.isArray(value)) {
    throw new Error(`CatBoost JSON model: expected array for ${label}`);
  }
  return value;
};

const asNumber = (value: unknown, label: string): number => {
  if (typeof value !== "number") {
    throw new Error(`CatBoost JSON model: expected number for ${label}`);
  }
  return value;
};

const asString = (value: unknown, label: string): string => {
  if (typeof value !== "string") {
    throw new Error(`CatBoost JSON model: expected string for ${label}`);
  }
  return value;
};

const parseFloatFeature = (raw: unknown): CatBoostFloatFeature => {
  const record = asRecord(raw);
  return {
    featureIndex: asNumber(record.feature_index, "float_features.feature_index"),
    flatFeatureIndex: asNumber(record.flat_feature_index, "float_features.flat_feature_index"),
    hasNans: record.has_nans === true,
    nanTreatment: asString(
      record.nan_value_treatment ?? "AsIs",
      "float_features.nan_value_treatment",
    ),
  };
};

const parseCategoricalFeature = (raw: unknown): CatBoostCategoricalFeature => {
  const record = asRecord(raw);
  return {
    featureIndex: asNumber(record.feature_index, "categorical_features.feature_index"),
    flatFeatureIndex: asNumber(
      record.flat_feature_index,
      "categorical_features.flat_feature_index",
    ),
  };
};

const isOneHotSplit = (splitType: string): boolean => splitType === SPLIT_TYPE_ONE_HOT;

const resolveSplitType = (splitType: string): CatBoostSplitType => {
  if (splitType === SPLIT_TYPE_FLOAT) return SPLIT_TYPE_FLOAT;
  if (isOneHotSplit(splitType)) return SPLIT_TYPE_ONE_HOT;
  throw new Error(`CatBoost JSON model: unsupported split_type ${splitType}`);
};

const parseOneHotValues = (record: Record<string, unknown>): number[] => {
  const raw = record.values;
  if (raw === undefined) return [];
  return asArray(raw, "splits.values").map((entry, index) =>
    asNumber(entry, `splits.values[${index}]`),
  );
};

const resolveSplitFeatureIndex = (
  record: Record<string, unknown>,
  splitType: CatBoostSplitType,
): number => {
  if (splitType === SPLIT_TYPE_ONE_HOT) {
    return asNumber(record.cat_feature_index, "splits.cat_feature_index");
  }
  return asNumber(record.float_feature_index, "splits.float_feature_index");
};

const resolveSplitBorder = (
  record: Record<string, unknown>,
  splitType: CatBoostSplitType,
): number => {
  if (splitType === SPLIT_TYPE_ONE_HOT) return 0;
  return asNumber(record.border, "splits.border");
};

const parseSplit = (raw: unknown): CatBoostSplit => {
  const record = asRecord(raw);
  const splitType = resolveSplitType(asString(record.split_type, "splits.split_type"));
  return {
    border: resolveSplitBorder(record, splitType),
    featureIndex: resolveSplitFeatureIndex(record, splitType),
    oneHotValues: parseOneHotValues(record),
    splitType,
  };
};

const parseLeafValues = (raw: unknown): number[] =>
  asArray(raw, "oblivious_trees.leaf_values").map((entry, index) =>
    asNumber(entry, `oblivious_trees.leaf_values[${index}]`),
  );

const parseTree = (raw: unknown): CatBoostTree => {
  const record = asRecord(raw);
  return {
    leafValues: parseLeafValues(record.leaf_values),
    splits: asArray(record.splits, "oblivious_trees.splits").map(parseSplit),
  };
};

const parseScaleAndBias = (raw: unknown): { bias: number; scale: number } => {
  if (raw === undefined) return { bias: 0, scale: 1 };
  const pair = asArray(raw, "scale_and_bias");
  if (pair.length !== SCALE_PAIR_LENGTH) {
    throw new Error("CatBoost JSON model: scale_and_bias must have 2 elements");
  }
  const biasList = asArray(pair[1], "scale_and_bias[1]");
  return {
    bias: asNumber(biasList[FIRST_INDEX], "scale_and_bias[1][0]"),
    scale: asNumber(pair[0], "scale_and_bias[0]"),
  };
};

const parseFeaturesInfo = (
  raw: unknown,
): { categoricalFeatures: CatBoostCategoricalFeature[]; floatFeatures: CatBoostFloatFeature[] } => {
  const record = asRecord(raw);
  const floatRaw = record.float_features ?? [];
  const categoricalRaw = record.categorical_features ?? [];
  return {
    categoricalFeatures: asArray(categoricalRaw, "features_info.categorical_features").map(
      parseCategoricalFeature,
    ),
    floatFeatures: asArray(floatRaw, "features_info.float_features").map(parseFloatFeature),
  };
};

export const parseCatBoostJsonModel = (json: unknown): CatBoostModel => {
  const record = asRecord(json);
  const featuresInfo = parseFeaturesInfo(record.features_info);
  const scaleAndBias = parseScaleAndBias(record.scale_and_bias);
  return {
    bias: scaleAndBias.bias,
    categoricalFeatures: featuresInfo.categoricalFeatures,
    floatFeatures: featuresInfo.floatFeatures,
    scale: scaleAndBias.scale,
    trees: asArray(record.oblivious_trees, "oblivious_trees").map(parseTree),
  };
};

const cellAt = (features: ReadonlyArray<FeatureCell>, index: number): FeatureCell => {
  const cell = features[index];
  if (cell === undefined) {
    throw new Error(`CatBoost scoring: missing feature cell at index ${index}`);
  }
  return cell;
};

const findFloatFeature = (
  model: CatBoostModel,
  featureIndex: number,
): CatBoostFloatFeature | undefined =>
  model.floatFeatures.find((feature) => feature.featureIndex === featureIndex);

const missingFloatGoesRight = (model: CatBoostModel, featureIndex: number): boolean => {
  const feature = findFloatFeature(model, featureIndex);
  if (feature === undefined) return false;
  return feature.nanTreatment === NAN_TREATMENT_AS_TRUE;
};

const floatSplitBit = (model: CatBoostModel, split: CatBoostSplit, cell: FeatureCell): number => {
  if (cell.isMissing || Number.isNaN(cell.value)) {
    return missingFloatGoesRight(model, split.featureIndex) ? BIT_SET : BIT_UNSET;
  }
  return cell.value > split.border ? BIT_SET : BIT_UNSET;
};

const oneHotSplitBit = (split: CatBoostSplit, cell: FeatureCell): number => {
  if (cell.isMissing) return BIT_UNSET;
  return split.oneHotValues.includes(cell.value) ? BIT_SET : BIT_UNSET;
};

const splitBit = (model: CatBoostModel, split: CatBoostSplit, cell: FeatureCell): number => {
  if (split.splitType === SPLIT_TYPE_ONE_HOT) return oneHotSplitBit(split, cell);
  return floatSplitBit(model, split, cell);
};

const leafIndexForTree = (
  model: CatBoostModel,
  tree: CatBoostTree,
  features: ReadonlyArray<FeatureCell>,
): number =>
  tree.splits.reduce<number>(
    (index, split, bitPosition) =>
      index | (splitBit(model, split, cellAt(features, split.featureIndex)) << bitPosition),
    0,
  );

const leafValueAt = (tree: CatBoostTree, leafIndex: number): number => {
  const value = tree.leafValues[leafIndex];
  if (value === undefined) {
    throw new Error(`CatBoost scoring: leaf index ${leafIndex} out of range`);
  }
  return value;
};

const treeContribution = (
  model: CatBoostModel,
  tree: CatBoostTree,
  features: ReadonlyArray<FeatureCell>,
): number => leafValueAt(tree, leafIndexForTree(model, tree, features));

export const scoreCatBoostModel = (input: ScoreModelInput): number => {
  const rawSum = input.model.trees.reduce<number>(
    (total, tree) => total + treeContribution(input.model, tree, input.features),
    0,
  );
  return input.model.scale * rawSum + input.model.bias;
};

const byScoreDescending = (left: { score: number }, right: { score: number }): number =>
  right.score - left.score;

export const scoreCatBoostRanking = (input: ScoreRankingInput): ScoredRow[] => {
  const scored = input.rows.map((row) => ({
    id: row.id,
    score: scoreCatBoostModel({ features: row.features, model: input.model }),
  }));
  const sorted = scored.slice().sort(byScoreDescending);
  return sorted.map((entry, position) => ({
    id: entry.id,
    predictedRank: position + FIRST_RANK,
    score: entry.score,
  }));
};
