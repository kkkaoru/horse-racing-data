// Run with: bun run --filter xgboost-json-tree test
// Pure-TS XGBoost tree ensemble evaluator targeted at the production JSON
// produced by `Booster.save_model('model.json')` (UBJ/JSON format, NOT the
// diagnostic `dump_model(dump_format='json')`). Used by the Cloudflare Worker
// to score NAR finish-position ranking (`nar-xgb-v7-lineage`,
// objective rank:pairwise) server-side without pulling Python into the runtime.
//
// save_model structure (XGBoost 3.x):
//   learner.gradient_booster.model.trees[] — each tree carries flat arrays:
//     split_indices[], split_conditions[], left_children[], right_children[],
//     base_weights[], default_left[] (0/1). A node is a leaf when
//     left_children[i] === LEAF_CHILD (-1); its output is base_weights[i].
//   learner.learner_model_param.base_score — string like "[-3.0519525E-9]".
//   learner.learner_model_param.num_feature — string like "175".
//   learner.feature_names — string[] (often empty for save_model exports).
// rank:pairwise raw score = sum of leaf base_weights across trees + base_score;
// horses are ranked by descending raw score.

export interface XgboostTree {
  split_indices: number[];
  split_conditions: number[];
  left_children: number[];
  right_children: number[];
  base_weights: number[];
  default_left: number[];
}

export interface XgboostModel {
  trees: XgboostTree[];
  base_score: number;
  num_feature: number;
  feature_names: string[];
}

export interface ScoreModelInput {
  model: XgboostModel;
  features: number[];
}

export interface RankingRow {
  umaban: number;
  features: number[];
}

export interface ScoreRankingInput {
  model: XgboostModel;
  rows: RankingRow[];
}

export interface ScoredRow {
  umaban: number;
  raw_score: number;
  predicted_rank: number;
}

interface WalkContext {
  tree: XgboostTree;
  features: number[];
}

const LEAF_CHILD = -1;
const DEFAULT_LEFT_FLAG = 1;
const ROOT_NODE_INDEX = 0;
const FIRST_RANK = 1;
const RANK_INCREMENT = 1;

const numberAt = (array: number[], index: number): number => array[index]!;

// base_score in save_model JSON is a bracketed string e.g. "[-3.0519525E-9]".
// Strip the brackets and parse the single contained float.
const parseBaseScore = (raw: unknown): number => {
  if (typeof raw === "number") return raw;
  if (typeof raw !== "string") throw new Error("XGBoost base_score must be a number or string");
  const parsed = Number(raw.replace("[", "").replace("]", "").trim());
  if (Number.isNaN(parsed)) throw new Error("XGBoost base_score is not a finite number");
  return parsed;
};

const parseNumFeature = (raw: unknown): number => {
  if (typeof raw === "number") return raw;
  if (typeof raw !== "string") throw new Error("XGBoost num_feature must be a number or string");
  const parsed = Number(raw.trim());
  if (Number.isNaN(parsed)) throw new Error("XGBoost num_feature is not a finite number");
  return parsed;
};

const isObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

const isNumberArray = (value: unknown): value is number[] =>
  Array.isArray(value) && value.every((entry) => typeof entry === "number");

const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every((entry) => typeof entry === "string");

const requireNumberArray = (value: unknown, field: string): number[] => {
  if (!isNumberArray(value)) throw new Error(`XGBoost tree field ${field} must be a number array`);
  return value;
};

const parseTree = (raw: unknown): XgboostTree => {
  if (!isObject(raw)) throw new Error("XGBoost tree entry must be an object");
  return {
    base_weights: requireNumberArray(raw.base_weights, "base_weights"),
    default_left: requireNumberArray(raw.default_left, "default_left"),
    left_children: requireNumberArray(raw.left_children, "left_children"),
    right_children: requireNumberArray(raw.right_children, "right_children"),
    split_conditions: requireNumberArray(raw.split_conditions, "split_conditions"),
    split_indices: requireNumberArray(raw.split_indices, "split_indices"),
  };
};

const resolveFeatureNames = (raw: unknown): string[] => {
  if (raw === undefined) return [];
  if (!isStringArray(raw)) throw new Error("XGBoost feature_names must be a string array");
  return raw;
};

export const parseXgboostJsonModel = (json: unknown): XgboostModel => {
  if (!isObject(json)) throw new Error("XGBoost model JSON must be an object");
  const learner = json.learner;
  if (!isObject(learner)) throw new Error("XGBoost model JSON is missing learner");
  const param = learner.learner_model_param;
  if (!isObject(param)) throw new Error("XGBoost model JSON is missing learner_model_param");
  const booster = learner.gradient_booster;
  if (!isObject(booster)) throw new Error("XGBoost model JSON is missing gradient_booster");
  const boosterModel = booster.model;
  if (!isObject(boosterModel))
    throw new Error("XGBoost model JSON is missing gradient_booster.model");
  const trees = boosterModel.trees;
  if (!Array.isArray(trees)) throw new Error("XGBoost model JSON is missing trees array");
  return {
    base_score: parseBaseScore(param.base_score),
    feature_names: resolveFeatureNames(learner.feature_names),
    num_feature: parseNumFeature(param.num_feature),
    trees: trees.map(parseTree),
  };
};

const isLeaf = (tree: XgboostTree, nodeIndex: number): boolean =>
  numberAt(tree.left_children, nodeIndex) === LEAF_CHILD;

const chooseNextNode = (context: WalkContext, nodeIndex: number): number => {
  const tree = context.tree;
  const featureIndex = numberAt(tree.split_indices, nodeIndex);
  const featureValue = context.features[featureIndex];
  const goesLeftByDefault = numberAt(tree.default_left, nodeIndex) === DEFAULT_LEFT_FLAG;
  if (featureValue === undefined || Number.isNaN(featureValue))
    return goesLeftByDefault
      ? numberAt(tree.left_children, nodeIndex)
      : numberAt(tree.right_children, nodeIndex);
  return featureValue < numberAt(tree.split_conditions, nodeIndex)
    ? numberAt(tree.left_children, nodeIndex)
    : numberAt(tree.right_children, nodeIndex);
};

const walkTree = (context: WalkContext, nodeIndex: number): number => {
  if (isLeaf(context.tree, nodeIndex)) return numberAt(context.tree.base_weights, nodeIndex);
  return walkTree(context, chooseNextNode(context, nodeIndex));
};

export const scoreXgboostModel = (input: ScoreModelInput): number => {
  const features = input.features;
  const leafSum = input.model.trees.reduce<number>(
    (acc, tree) => acc + walkTree({ features, tree }, ROOT_NODE_INDEX),
    0,
  );
  return leafSum + input.model.base_score;
};

const compareScoredRows = (left: ScoredRow, right: ScoredRow): number => {
  if (right.raw_score !== left.raw_score) return right.raw_score - left.raw_score;
  return left.umaban - right.umaban;
};

const withPredictedRank = (row: ScoredRow, index: number): ScoredRow => ({
  predicted_rank: FIRST_RANK + index * RANK_INCREMENT,
  raw_score: row.raw_score,
  umaban: row.umaban,
});

export const scoreXgboostRanking = (input: ScoreRankingInput): ScoredRow[] => {
  const model = input.model;
  const scored = input.rows.map<ScoredRow>((row) => ({
    predicted_rank: FIRST_RANK,
    raw_score: scoreXgboostModel({ features: row.features, model }),
    umaban: row.umaban,
  }));
  return scored.sort(compareScoredRows).map(withPredictedRank);
};
