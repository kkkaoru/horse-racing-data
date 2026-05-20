// Run with bun. Convert the compact LightGBM JSON model into the flat binary
// format consumed by the Worker.

import { encodeCategoricalThreshold } from "../src/running-style-model-binary";

const MAGIC = "RSLGBM1\0";
const HEADER_LENGTH_BYTES = 4;
const NODE_RECORD_BYTES = 40;
const NODE_LEAF = 0;
const NODE_NUMERIC_LEQ = 1;
const NODE_CATEGORICAL_EQ = 2;
const NODE_NUMERIC_LT = 3;

interface LightGBMLeafNode {
  leaf_value: number;
}

interface LightGBMSplitNode {
  decision_type: string;
  default_left: boolean;
  left_child: LightGBMNode;
  missing_type?: string;
  right_child: LightGBMNode;
  split_feature: number;
  threshold: number | string;
}

type LightGBMNode = LightGBMLeafNode | LightGBMSplitNode;

interface CompactLightGBMModel {
  categorical_features: string[];
  class_labels: string[];
  feature_names: string[];
  model_version: string;
  num_class: number;
  num_tree_per_iteration: number;
  objective: string;
  trees: { tree_structure: LightGBMNode }[];
}

interface FlatNode {
  categoricalCount: number;
  categoricalStart: number;
  categoricalValues: number[];
  defaultLeft: boolean;
  kind: number;
  leafValue: number;
  leftChild: number;
  rightChild: number;
  splitFeature: number;
  threshold: number;
}

const parseArgs = (): { input: string; output: string } => {
  const args = Bun.argv.slice(2);
  const inputIndex = args.indexOf("--input");
  const outputIndex = args.indexOf("--output");
  const input = inputIndex >= 0 ? args[inputIndex + 1] : undefined;
  const output = outputIndex >= 0 ? args[outputIndex + 1] : undefined;
  if (!input || !output) {
    throw new Error(
      "Usage: bun run scripts/convert-running-style-model-to-binary.ts --input model.json --output model.flatbin",
    );
  }
  return { input, output };
};

const isLeaf = (node: LightGBMNode): node is LightGBMLeafNode => "leaf_value" in node;

const flattenTree = (
  node: LightGBMNode,
  nodes: FlatNode[],
  categoricalValues: number[],
): number => {
  const nodeIndex = nodes.length;
  nodes.push({
    categoricalCount: 0,
    categoricalStart: 0,
    categoricalValues: [],
    defaultLeft: true,
    kind: NODE_LEAF,
    leafValue: 0,
    leftChild: -1,
    rightChild: -1,
    splitFeature: -1,
    threshold: Number.NaN,
  });
  if (isLeaf(node)) {
    nodes[nodeIndex] = {
      ...nodes[nodeIndex],
      kind: NODE_LEAF,
      leafValue: node.leaf_value,
    };
    return nodeIndex;
  }
  const leftChild = flattenTree(node.left_child, nodes, categoricalValues);
  const rightChild = flattenTree(node.right_child, nodes, categoricalValues);
  const isCategorical = node.decision_type === "==";
  const encodedCategoricals = isCategorical ? encodeCategoricalThreshold(node.threshold) : [];
  const categoricalStart = categoricalValues.length;
  encodedCategoricals.forEach((value) => categoricalValues.push(value));
  nodes[nodeIndex] = {
    categoricalCount: encodedCategoricals.length,
    categoricalStart,
    categoricalValues: encodedCategoricals,
    defaultLeft: node.default_left,
    kind:
      node.decision_type === "=="
        ? NODE_CATEGORICAL_EQ
        : node.decision_type === "<="
          ? NODE_NUMERIC_LEQ
          : NODE_NUMERIC_LT,
    leafValue: 0,
    leftChild,
    rightChild,
    splitFeature: node.split_feature,
    threshold: typeof node.threshold === "number" ? node.threshold : Number(node.threshold),
  };
  return nodeIndex;
};

const writeHeader = (payload: object): Uint8Array => {
  const header = new TextEncoder().encode(JSON.stringify(payload));
  const output = new Uint8Array(MAGIC.length + HEADER_LENGTH_BYTES + header.length);
  output.set(new TextEncoder().encode(MAGIC), 0);
  new DataView(output.buffer).setUint32(MAGIC.length, header.length, true);
  output.set(header, MAGIC.length + HEADER_LENGTH_BYTES);
  return output;
};

const writeNodes = (nodes: FlatNode[]): Uint8Array => {
  const output = new Uint8Array(nodes.length * NODE_RECORD_BYTES);
  const view = new DataView(output.buffer);
  nodes.forEach((node, index) => {
    const base = index * NODE_RECORD_BYTES;
    view.setUint8(base, node.kind);
    view.setUint8(base + 1, node.defaultLeft ? 1 : 0);
    view.setInt32(base + 4, node.splitFeature, true);
    view.setInt32(base + 8, node.leftChild, true);
    view.setInt32(base + 12, node.rightChild, true);
    view.setInt32(base + 16, node.categoricalStart, true);
    view.setInt32(base + 20, node.categoricalCount, true);
    view.setFloat64(base + 24, node.threshold, true);
    view.setFloat64(base + 32, node.leafValue, true);
  });
  return output;
};

const writeCategoricalValues = (values: number[]): Uint8Array => {
  const output = new Uint8Array(values.length * 8);
  const view = new DataView(output.buffer);
  values.forEach((value, index) => view.setFloat64(index * 8, value, true));
  return output;
};

const concat = (parts: Uint8Array[]): Uint8Array => {
  const total = parts.reduce((size, part) => size + part.length, 0);
  const output = new Uint8Array(total);
  let offset = 0;
  parts.forEach((part) => {
    output.set(part, offset);
    offset += part.length;
  });
  return output;
};

const main = async (): Promise<void> => {
  const args = parseArgs();
  const model = JSON.parse(await Bun.file(args.input).text()) as CompactLightGBMModel;
  const nodes: FlatNode[] = [];
  const categoricalValues: number[] = [];
  const treeRootIndices = model.trees.map((tree) =>
    flattenTree(tree.tree_structure, nodes, categoricalValues),
  );
  const header = writeHeader({
    categorical_features: model.categorical_features,
    categorical_value_count: categoricalValues.length,
    class_labels: model.class_labels,
    feature_names: model.feature_names,
    format: "rs-lgbm-flat-v1",
    model_version: model.model_version,
    node_count: nodes.length,
    num_class: model.num_class,
    num_tree_per_iteration: model.num_tree_per_iteration,
    objective: model.objective,
    tree_root_indices: treeRootIndices,
  });
  const output = concat([header, writeNodes(nodes), writeCategoricalValues(categoricalValues)]);
  await Bun.write(args.output, output);
  console.log(
    JSON.stringify({
      categoricalValueCount: categoricalValues.length,
      input: args.input,
      nodes: nodes.length,
      output: args.output,
      sizeBytes: output.byteLength,
      trees: treeRootIndices.length,
    }),
  );
};

await main();
