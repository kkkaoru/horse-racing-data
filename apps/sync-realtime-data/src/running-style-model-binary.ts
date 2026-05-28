// Run with bun. Flat binary LightGBM model loader/evaluator for Workers.
// The JSON tree model is too large to parse reliably inside the 128 MB Worker
// memory ceiling, so verification and the future production path use this
// packed node layout from R2.

import {
  buildFeatureVector,
  probsToRunningStyleMap,
  resolveRunningStyleLabels,
  type FeatureVector,
  type RunningStylePrediction,
} from "./running-style-lightgbm-tree";

const MAGIC = "RSLGBM1\0";
const HEADER_LENGTH_OFFSET = MAGIC.length;
const HEADER_OFFSET = HEADER_LENGTH_OFFSET + 4;
const NODE_RECORD_BYTES = 40;

const NODE_LEAF = 0;
const NODE_CATEGORICAL_EQ = 2;
const MISSING_FLAG = 1;
const CATEGORICAL_THRESHOLD_DELIMITER = "||";

export interface FlatLightGBMHeader {
  categorical_features: string[];
  class_labels: string[];
  feature_names: string[];
  format: "rs-lgbm-flat-v1";
  model_version: string;
  node_count: number;
  num_class: number;
  num_tree_per_iteration: number;
  objective: string;
  tree_root_indices: number[];
  categorical_value_count: number;
}

export interface FlatLightGBMModel {
  buffer: ArrayBuffer;
  categoricalValuesOffset: number;
  dataView: DataView;
  header: FlatLightGBMHeader;
  nodeOffset: number;
}

const decoder = new TextDecoder();

const readHeader = (buffer: ArrayBuffer): { dataOffset: number; header: FlatLightGBMHeader } => {
  const prefix = decoder.decode(buffer.slice(0, MAGIC.length));
  if (prefix !== MAGIC) {
    throw new Error("invalid running-style binary model magic");
  }
  const view = new DataView(buffer);
  const headerLength = view.getUint32(HEADER_LENGTH_OFFSET, true);
  const headerBytes = buffer.slice(HEADER_OFFSET, HEADER_OFFSET + headerLength);
  const header = JSON.parse(decoder.decode(headerBytes)) as FlatLightGBMHeader;
  if (header.format !== "rs-lgbm-flat-v1") {
    throw new Error(`unsupported running-style binary model format: ${String(header.format)}`);
  }
  return { dataOffset: HEADER_OFFSET + headerLength, header };
};

export const decodeFlatLightGBMModel = (buffer: ArrayBuffer): FlatLightGBMModel => {
  const { dataOffset, header } = readHeader(buffer);
  const nodeBytes = header.node_count * NODE_RECORD_BYTES;
  return {
    buffer,
    categoricalValuesOffset: dataOffset + nodeBytes,
    dataView: new DataView(buffer),
    header,
    nodeOffset: dataOffset,
  };
};

export const buildRunningStyleFlatModelKey = (source: "jra" | "nar"): string =>
  `running-style/models/${source}/latest.flatbin`;

export const loadFlatLightGBMModelFromR2 = async (
  bucket: R2Bucket,
  key: string,
): Promise<FlatLightGBMModel> => {
  const object = await bucket.get(key);
  if (object === null) throw new Error(`R2 object not found: ${key}`);
  return decodeFlatLightGBMModel(await object.arrayBuffer());
};

const nodeBase = (model: FlatLightGBMModel, nodeIndex: number): number =>
  model.nodeOffset + nodeIndex * NODE_RECORD_BYTES;

const hasCategoricalValue = (
  model: FlatLightGBMModel,
  start: number,
  count: number,
  value: number,
): boolean => {
  for (let index = 0; index < count; index += 1) {
    const candidate = model.dataView.getFloat64(
      model.categoricalValuesOffset + (start + index) * 8,
      true,
    );
    if (candidate === value) return true;
  }
  return false;
};

const chooseChild = (
  model: FlatLightGBMModel,
  nodeIndex: number,
  vector: FeatureVector,
): number => {
  const base = nodeBase(model, nodeIndex);
  const kind = model.dataView.getUint8(base);
  const defaultLeft = model.dataView.getUint8(base + 1) === 1;
  const splitFeature = model.dataView.getInt32(base + 4, true);
  const leftChild = model.dataView.getInt32(base + 8, true);
  const rightChild = model.dataView.getInt32(base + 12, true);
  if (vector.missing[splitFeature] === MISSING_FLAG) {
    return defaultLeft ? leftChild : rightChild;
  }
  const value = vector.values[splitFeature]!;
  if (kind === NODE_CATEGORICAL_EQ) {
    const start = model.dataView.getInt32(base + 16, true);
    const count = model.dataView.getInt32(base + 20, true);
    return hasCategoricalValue(model, start, count, value) ? leftChild : rightChild;
  }
  const threshold = model.dataView.getFloat64(base + 24, true);
  return value <= threshold ? leftChild : rightChild;
};

const walkFlatTree = (
  model: FlatLightGBMModel,
  rootIndex: number,
  vector: FeatureVector,
): number => {
  let nodeIndex = rootIndex;
  for (;;) {
    const base = nodeBase(model, nodeIndex);
    const kind = model.dataView.getUint8(base);
    if (kind === NODE_LEAF) return model.dataView.getFloat64(base + 32, true);
    nodeIndex = chooseChild(model, nodeIndex, vector);
  }
};

const softmax = (logits: Float64Array): Float64Array => {
  const maxLogit = logits.reduce((best, value) => (value > best ? value : best), logits[0]!);
  const exps = Float64Array.from(logits, (value) => Math.exp(value - maxLogit));
  const sum = exps.reduce((total, value) => total + value, 0);
  return Float64Array.from(exps, (value) => value / sum);
};

export const predictFlatRunningStyle = (
  model: FlatLightGBMModel,
  values: Record<string, number | null | undefined>,
): RunningStylePrediction => {
  const vector = buildFeatureVector({ featureNames: model.header.feature_names, values });
  const logits = new Float64Array(model.header.num_class);
  model.header.tree_root_indices.forEach((rootIndex, treeIndex) => {
    const classIndex = treeIndex % model.header.num_tree_per_iteration;
    logits[classIndex] = logits[classIndex]! + walkFlatTree(model, rootIndex, vector);
  });
  const probabilities = softmax(logits);
  let predictedClass = 0;
  probabilities.forEach((value, index) => {
    if (value > probabilities[predictedClass]!) predictedClass = index;
  });
  const labels = resolveRunningStyleLabels(model.header.class_labels, model.header.num_class);
  return {
    predictedClass,
    predictedLabel: labels[predictedClass]!,
    probabilities: probsToRunningStyleMap(probabilities, labels),
  };
};

export const encodeCategoricalThreshold = (threshold: number | string): number[] =>
  String(threshold)
    .split(CATEGORICAL_THRESHOLD_DELIMITER)
    .map((token) => Number(token))
    .filter((value) => Number.isFinite(value));
