// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { convertRunningStyleModelFile } from "./convert-running-style-model-to-binary";

interface BunGlobal {
  file: (path: string) => { text: () => Promise<string> };
  write: (path: string, data: Uint8Array) => Promise<number>;
}

const originalBun = (globalThis as { Bun?: BunGlobal }).Bun;

const MIN_MODEL = {
  categorical_features: ["cat_feature"],
  class_labels: ["nige", "senkou", "sashi", "oikomi"],
  feature_names: ["x", "cat_feature"],
  model_version: "convert-test-v1",
  num_class: 4,
  num_tree_per_iteration: 4,
  objective: "multiclass",
  trees: [
    { tree_structure: { leaf_value: 1 } },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
  ],
};

const SPLIT_MODEL = {
  ...MIN_MODEL,
  trees: [
    {
      tree_structure: {
        decision_type: "<=",
        default_left: true,
        left_child: { leaf_value: 1 },
        right_child: { leaf_value: -1 },
        split_feature: 0,
        threshold: 0.5,
      },
    },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
  ],
};

const CATEGORICAL_MODEL = {
  ...MIN_MODEL,
  trees: [
    {
      tree_structure: {
        decision_type: "==",
        default_left: false,
        left_child: { leaf_value: 1 },
        right_child: { leaf_value: 0 },
        split_feature: 1,
        threshold: "3||4||5",
      },
    },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
  ],
};

const LT_MODEL = {
  ...MIN_MODEL,
  trees: [
    {
      tree_structure: {
        decision_type: "<",
        default_left: true,
        left_child: { leaf_value: 0.7 },
        right_child: { leaf_value: -0.7 },
        split_feature: 0,
        threshold: 0.5,
      },
    },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
    { tree_structure: { leaf_value: 0 } },
  ],
};

beforeEach(() => {
  (globalThis as { Bun?: BunGlobal }).Bun = {
    file: (_path: string) => ({ text: async (): Promise<string> => "" }),
    write: async (_path: string, _data: Uint8Array): Promise<number> => 0,
  };
});

afterEach(() => {
  vi.restoreAllMocks();
  if (originalBun === undefined) {
    delete (globalThis as { Bun?: BunGlobal }).Bun;
  } else {
    (globalThis as { Bun?: BunGlobal }).Bun = originalBun;
  }
});

it("convertRunningStyleModelFile writes file with leaf-only model and reports counts", async () => {
  const writes: Array<[string, Uint8Array]> = [];
  (globalThis as { Bun?: BunGlobal }).Bun = {
    file: () => ({ text: async (): Promise<string> => JSON.stringify(MIN_MODEL) }),
    write: async (path: string, data: Uint8Array): Promise<number> => {
      writes.push([path, data]);
      return data.byteLength;
    },
  };
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.trees).toBe(4);
  expect(result.nodes).toBe(4);
  expect(result.categoricalValueCount).toBe(0);
  expect(writes.length).toBe(1);
  expect(writes[0]![0]).toBe("model.flatbin");
  expect(result.sizeBytes).toBe(writes[0]![1].byteLength);
});

it("convertRunningStyleModelFile handles a numeric LEQ split tree", async () => {
  const writes: Uint8Array[] = [];
  (globalThis as { Bun?: BunGlobal }).Bun = {
    file: () => ({ text: async (): Promise<string> => JSON.stringify(SPLIT_MODEL) }),
    write: async (_path: string, data: Uint8Array): Promise<number> => {
      writes.push(data);
      return data.byteLength;
    },
  };
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.nodes).toBe(6);
  expect(result.categoricalValueCount).toBe(0);
  expect(writes.length).toBe(1);
});

it("convertRunningStyleModelFile encodes a categorical EQ split into categoricalValues", async () => {
  const writes: Uint8Array[] = [];
  (globalThis as { Bun?: BunGlobal }).Bun = {
    file: () => ({ text: async (): Promise<string> => JSON.stringify(CATEGORICAL_MODEL) }),
    write: async (_path: string, data: Uint8Array): Promise<number> => {
      writes.push(data);
      return data.byteLength;
    },
  };
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.categoricalValueCount).toBe(3);
  expect(result.nodes).toBe(6);
});

it("convertRunningStyleModelFile handles numeric LT split decision_type", async () => {
  const writes: Uint8Array[] = [];
  (globalThis as { Bun?: BunGlobal }).Bun = {
    file: () => ({ text: async (): Promise<string> => JSON.stringify(LT_MODEL) }),
    write: async (_path: string, data: Uint8Array): Promise<number> => {
      writes.push(data);
      return data.byteLength;
    },
  };
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.nodes).toBe(6);
});
