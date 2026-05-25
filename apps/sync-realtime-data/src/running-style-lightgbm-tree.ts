// Run with bun. Pure-JS LightGBM tree ensemble evaluator targeted at the
// compact JSON produced by apps/pc-keiba-viewer/src/scripts/export_lightgbm_to_json.py.
// Used by the Cloudflare Worker to score running-style probabilities on the
// fly for upcoming races without pulling Python into the runtime.

export type RunningStyleClassLabel = "nige" | "senkou" | "sashi" | "oikomi";

export interface LightGBMLeafNode {
  leaf_value: number;
  leaf_index?: number;
}

export interface LightGBMSplitNode {
  split_feature: number;
  threshold: number | string;
  decision_type: string;
  default_left: boolean;
  missing_type?: string;
  left_child: LightGBMTreeNode;
  right_child: LightGBMTreeNode;
}

export type LightGBMTreeNode = LightGBMLeafNode | LightGBMSplitNode;

export interface LightGBMTreeEntry {
  tree_structure: LightGBMTreeNode;
}

export interface CompactLightGBMModel {
  model_version: string;
  objective: string;
  num_class: number;
  num_tree_per_iteration: number;
  class_labels: string[];
  feature_names: string[];
  categorical_features: string[];
  trees: LightGBMTreeEntry[];
}

export interface FeatureVector {
  values: Float64Array;
  missing: Uint8Array;
}

export interface RunningStylePrediction {
  probabilities: Record<RunningStyleClassLabel, number>;
  predictedLabel: RunningStyleClassLabel;
  predictedClass: number;
}

export interface BuildVectorInput {
  featureNames: string[];
  values: Record<string, number | null | undefined>;
}

const RUNNING_STYLE_LABELS: readonly RunningStyleClassLabel[] = [
  "nige",
  "senkou",
  "sashi",
  "oikomi",
];
const RUNNING_STYLE_LABEL_SET = new Set<string>(RUNNING_STYLE_LABELS);

const DECISION_TYPE_LEQ = "<=";
const DECISION_TYPE_EQ = "==";
const CATEGORICAL_THRESHOLD_DELIMITER = "||";
const MISSING_FLAG = 1;

const numberAt = (array: Float64Array | Uint8Array, index: number): number =>
  array[index]!;

const isSplitNode = (node: LightGBMTreeNode): node is LightGBMSplitNode => "split_feature" in node;

const evaluateNumericSplit = (decisionType: string, threshold: number, value: number): boolean =>
  decisionType === DECISION_TYPE_LEQ ? value <= threshold : value < threshold;

const evaluateCategoricalSplit = (rawThreshold: number | string, value: number): boolean => {
  const text = typeof rawThreshold === "string" ? rawThreshold : String(rawThreshold);
  return text.split(CATEGORICAL_THRESHOLD_DELIMITER).some((token) => Number(token) === value);
};

const goesLeftAtSplit = (node: LightGBMSplitNode, value: number): boolean => {
  if (node.decision_type === DECISION_TYPE_EQ)
    return evaluateCategoricalSplit(node.threshold, value);
  const threshold = typeof node.threshold === "number" ? node.threshold : Number(node.threshold);
  return evaluateNumericSplit(node.decision_type, threshold, value);
};

const chooseNextChild = (node: LightGBMSplitNode, vector: FeatureVector): LightGBMTreeNode => {
  const featureIndex = node.split_feature;
  if (numberAt(vector.missing, featureIndex) === MISSING_FLAG) {
    return node.default_left ? node.left_child : node.right_child;
  }
  return goesLeftAtSplit(node, numberAt(vector.values, featureIndex))
    ? node.left_child
    : node.right_child;
};

export const walkTree = (root: LightGBMTreeNode, vector: FeatureVector): number => {
  if (!isSplitNode(root)) return root.leaf_value;
  return walkTree(chooseNextChild(root, vector), vector);
};

const maxOfFloat64 = (values: Float64Array): number =>
  values.reduce<number>((acc, value) => (value > acc ? value : acc), numberAt(values, 0));

const sumOfFloat64 = (values: Float64Array): number =>
  values.reduce<number>((acc, value) => acc + value, 0);

const softmaxNormalize = (logits: Float64Array): Float64Array => {
  const maxLogit = maxOfFloat64(logits);
  const exps = Float64Array.from(logits, (value) => Math.exp(value - maxLogit));
  const sumExp = sumOfFloat64(exps);
  return Float64Array.from(exps, (value) => value / sumExp);
};

const accumulateClassLogits = (
  model: CompactLightGBMModel,
  vector: FeatureVector,
): Float64Array => {
  const logits = new Float64Array(model.num_class);
  model.trees.forEach((tree, treeIndex) => {
    const classIndex = treeIndex % model.num_tree_per_iteration;
    logits[classIndex] = numberAt(logits, classIndex) + walkTree(tree.tree_structure, vector);
  });
  return logits;
};

const argmaxIndex = (probs: Float64Array): number =>
  probs.reduce<number>(
    (bestIndex, value, index) => (value > numberAt(probs, bestIndex) ? index : bestIndex),
    0,
  );

const FALLBACK_LABEL: RunningStyleClassLabel = "nige";

const isRunningStyleLabel = (label: string | undefined): label is RunningStyleClassLabel =>
  label !== undefined && RUNNING_STYLE_LABEL_SET.has(label);

export const resolveRunningStyleLabels = (
  labels: readonly string[] | undefined,
  numClass: number,
): RunningStyleClassLabel[] =>
  Array.from({ length: numClass }, (_, index) => {
    const modelLabel = labels?.[index];
    if (isRunningStyleLabel(modelLabel)) return modelLabel;
    return RUNNING_STYLE_LABELS[index] ?? FALLBACK_LABEL;
  });

export const probsToRunningStyleMap = (
  probs: Float64Array,
  labels: readonly RunningStyleClassLabel[],
): Record<RunningStyleClassLabel, number> => {
  const mapped: Record<RunningStyleClassLabel, number> = {
    nige: 0,
    senkou: 0,
    sashi: 0,
    oikomi: 0,
  };
  labels.forEach((label, index) => {
    mapped[label] = numberAt(probs, index);
  });
  return mapped;
};

const labelAtIndex = (
  index: number,
  labels: readonly RunningStyleClassLabel[],
): RunningStyleClassLabel => labels[index] ?? FALLBACK_LABEL;

export const predictRunningStyle = (
  model: CompactLightGBMModel,
  vector: FeatureVector,
): RunningStylePrediction => {
  const logits = accumulateClassLogits(model, vector);
  const probs = softmaxNormalize(logits);
  const predictedClass = argmaxIndex(probs);
  const labels = resolveRunningStyleLabels(model.class_labels, model.num_class);
  return {
    predictedClass,
    predictedLabel: labelAtIndex(predictedClass, labels),
    probabilities: probsToRunningStyleMap(probs, labels),
  };
};

const resolveFeatureCell = (raw: number | null | undefined): { value: number; missing: number } => {
  if (raw === null || raw === undefined || Number.isNaN(raw))
    return { missing: MISSING_FLAG, value: 0 };
  return { missing: 0, value: raw };
};

export const buildFeatureVector = (input: BuildVectorInput): FeatureVector => {
  const cells = input.featureNames.map((name) => resolveFeatureCell(input.values[name]));
  return {
    missing: Uint8Array.from(cells, (cell) => cell.missing),
    values: Float64Array.from(cells, (cell) => cell.value),
  };
};
