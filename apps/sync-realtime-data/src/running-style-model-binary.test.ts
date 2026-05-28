// run with: bun run test
import { afterEach, expect, it, vi } from "vitest";
import {
  buildRunningStyleFlatModelKey,
  decodeFlatLightGBMModel,
  encodeCategoricalThreshold,
  loadFlatLightGBMModelFromR2,
  predictFlatRunningStyle,
} from "./running-style-model-binary";
import type { FlatLightGBMHeader } from "./running-style-model-binary";

const MAGIC = "RSLGBM1\0";
const NODE_BYTES = 40;
const FEATURE_NAMES = ["x"];
const CLASS_LABELS = ["nige", "senkou", "sashi", "oikomi"];
const NUM_CLASS = 4;

interface NodeSpec {
  categoricalCount?: number;
  categoricalStart?: number;
  defaultLeft?: 0 | 1;
  kind: 0 | 1 | 2;
  leafValue?: number;
  leftChild?: number;
  rightChild?: number;
  splitFeature?: number;
  threshold?: number;
}

const writeNode = (view: DataView, offset: number, spec: NodeSpec): void => {
  view.setUint8(offset + 0, spec.kind);
  view.setUint8(offset + 1, spec.defaultLeft ?? 0);
  view.setInt32(offset + 4, spec.splitFeature ?? 0, true);
  view.setInt32(offset + 8, spec.leftChild ?? 0, true);
  view.setInt32(offset + 12, spec.rightChild ?? 0, true);
  view.setInt32(offset + 16, spec.categoricalStart ?? 0, true);
  view.setInt32(offset + 20, spec.categoricalCount ?? 0, true);
  view.setFloat64(offset + 24, spec.threshold ?? 0, true);
  view.setFloat64(offset + 32, spec.leafValue ?? 0, true);
};

interface BuildBufferInput {
  categoricalValues?: number[];
  format?: string;
  magic?: string;
  nodes: NodeSpec[];
  treeRootIndices?: number[];
}

const buildBuffer = ({
  categoricalValues = [],
  format = "rs-lgbm-flat-v1",
  magic = MAGIC,
  nodes,
  treeRootIndices,
}: BuildBufferInput): ArrayBuffer => {
  const header = {
    categorical_features: [],
    categorical_value_count: categoricalValues.length,
    class_labels: CLASS_LABELS,
    feature_names: FEATURE_NAMES,
    format: format as "rs-lgbm-flat-v1",
    model_version: "test-v1",
    node_count: nodes.length,
    num_class: NUM_CLASS,
    num_tree_per_iteration: NUM_CLASS,
    objective: "multiclass",
    tree_root_indices: treeRootIndices ?? nodes.map((_, index) => index),
  } satisfies FlatLightGBMHeader;
  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const magicBytes = new TextEncoder().encode(magic);
  const totalLength =
    magicBytes.byteLength +
    4 +
    headerBytes.byteLength +
    nodes.length * NODE_BYTES +
    categoricalValues.length * 8;
  const buffer = new ArrayBuffer(totalLength);
  const view = new DataView(buffer);
  new Uint8Array(buffer).set(magicBytes, 0);
  view.setUint32(magicBytes.byteLength, headerBytes.byteLength, true);
  new Uint8Array(buffer).set(headerBytes, magicBytes.byteLength + 4);
  let cursor = magicBytes.byteLength + 4 + headerBytes.byteLength;
  nodes.forEach((node) => {
    writeNode(view, cursor, node);
    cursor += NODE_BYTES;
  });
  categoricalValues.forEach((value, index) => {
    view.setFloat64(cursor + index * 8, value, true);
  });
  return buffer;
};

afterEach(() => {
  vi.restoreAllMocks();
});

it("buildRunningStyleFlatModelKey builds the JRA model key", () => {
  expect(buildRunningStyleFlatModelKey("jra")).toBe("running-style/models/jra/latest.flatbin");
});

it("buildRunningStyleFlatModelKey builds the NAR model key", () => {
  expect(buildRunningStyleFlatModelKey("nar")).toBe("running-style/models/nar/latest.flatbin");
});

it("encodeCategoricalThreshold parses single numeric string", () => {
  expect(encodeCategoricalThreshold(5)).toStrictEqual([5]);
});

it("encodeCategoricalThreshold splits on '||' delimiter", () => {
  expect(encodeCategoricalThreshold("1||2||3")).toStrictEqual([1, 2, 3]);
});

it("encodeCategoricalThreshold filters out non-finite tokens", () => {
  expect(encodeCategoricalThreshold("1||abc||3")).toStrictEqual([1, 3]);
});

it("decodeFlatLightGBMModel rejects an invalid magic header", () => {
  expect(() => decodeFlatLightGBMModel(buildBuffer({ magic: "BADMAGIC", nodes: [] }))).toThrow(
    "invalid running-style binary model magic",
  );
});

it("decodeFlatLightGBMModel rejects an unsupported format", () => {
  expect(() =>
    decodeFlatLightGBMModel(buildBuffer({ format: "rs-lgbm-flat-v2", nodes: [{ kind: 0 }] })),
  ).toThrow("unsupported running-style binary model format: rs-lgbm-flat-v2");
});

it("decodeFlatLightGBMModel returns header and offsets when valid", () => {
  const model = decodeFlatLightGBMModel(buildBuffer({ nodes: [{ kind: 0, leafValue: 1.5 }] }));
  expect(model.header.node_count).toBe(1);
  expect(model.header.feature_names).toStrictEqual(FEATURE_NAMES);
});

it("loadFlatLightGBMModelFromR2 throws when the R2 object is missing", async () => {
  const get = vi.fn(async () => null);
  const bucket = { get } as unknown as R2Bucket;
  await expect(loadFlatLightGBMModelFromR2(bucket, "models/missing.flatbin")).rejects.toThrow(
    "R2 object not found: models/missing.flatbin",
  );
});

it("loadFlatLightGBMModelFromR2 decodes the buffer returned by R2", async () => {
  const buffer = buildBuffer({ nodes: [{ kind: 0, leafValue: 0.5 }] });
  const get = vi.fn(async () => ({
    arrayBuffer: async (): Promise<ArrayBuffer> => buffer,
  }));
  const bucket = { get } as unknown as R2Bucket;
  const model = await loadFlatLightGBMModelFromR2(bucket, "models/test.flatbin");
  expect(model.header.node_count).toBe(1);
});

it("predictFlatRunningStyle returns probabilities from a single-leaf tree per class", () => {
  const buffer = buildBuffer({
    nodes: [
      { kind: 0, leafValue: 1 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
    ],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: 0 });
  expect(prediction.predictedClass).toBe(0);
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle walks numeric LEQ branches", () => {
  const buffer = buildBuffer({
    nodes: [
      {
        kind: 1,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
        threshold: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 10 },
      { kind: 0, leafValue: -10 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: -1 });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle selects categorical match path", () => {
  const buffer = buildBuffer({
    categoricalValues: [3, 4, 5],
    nodes: [
      {
        categoricalCount: 3,
        categoricalStart: 0,
        kind: 2,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 5 },
      { kind: 0, leafValue: -5 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: 4 });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle follows defaultLeft when feature is missing", () => {
  const buffer = buildBuffer({
    nodes: [
      {
        defaultLeft: 1,
        kind: 1,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
        threshold: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 9 },
      { kind: 0, leafValue: -9 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: null });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle takes rightChild when defaultLeft is 0 and feature is missing", () => {
  const buffer = buildBuffer({
    nodes: [
      {
        defaultLeft: 0,
        kind: 1,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
        threshold: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: -9 },
      { kind: 0, leafValue: 9 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: null });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle takes rightChild on a categorical miss", () => {
  const buffer = buildBuffer({
    categoricalValues: [3, 4, 5],
    nodes: [
      {
        categoricalCount: 3,
        categoricalStart: 0,
        kind: 2,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: -5 },
      { kind: 0, leafValue: 5 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: 99 });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle takes rightChild when value exceeds the numeric threshold", () => {
  const buffer = buildBuffer({
    nodes: [
      {
        kind: 1,
        leftChild: 4,
        rightChild: 5,
        splitFeature: 0,
        threshold: 0,
      },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: -10 },
      { kind: 0, leafValue: 10 },
    ],
    treeRootIndices: [0, 1, 2, 3],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: 1 });
  expect(prediction.predictedLabel).toBe("nige");
});

it("predictFlatRunningStyle reassigns predictedClass when a later class scores higher", () => {
  const buffer = buildBuffer({
    nodes: [
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 0 },
      { kind: 0, leafValue: 5 },
      { kind: 0, leafValue: 0 },
    ],
  });
  const model = decodeFlatLightGBMModel(buffer);
  const prediction = predictFlatRunningStyle(model, { x: 0 });
  expect(prediction.predictedClass).toBe(2);
  expect(prediction.predictedLabel).toBe("sashi");
});
