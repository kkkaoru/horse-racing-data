// run with: bun run test
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import {
  convertRunningStyleModelFile,
  main,
  parseArgs,
} from "./convert-running-style-model-to-binary";

interface BunGlobal {
  argv?: string[];
  file?: (path: string) => { text: () => Promise<string> };
  write?: (path: string, data: Uint8Array) => Promise<number>;
}

const originalBun = (globalThis as { Bun?: BunGlobal }).Bun;

vi.mock("node:fs/promises", () => {
  const state: { readContent: string; writes: Array<[string, Uint8Array]> } = {
    readContent: "",
    writes: [],
  };
  return {
    readFile: vi.fn(async () => state.readContent),
    writeFile: vi.fn(async (path: string, data: Uint8Array) => {
      state.writes.push([path, data]);
    }),
    __setReadContent: (text: string) => {
      state.readContent = text;
    },
    __getWrites: () => state.writes,
    __resetWrites: () => {
      state.writes.length = 0;
    },
  };
});

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

interface FsMockHelpers {
  __getWrites: () => Array<[string, Uint8Array]>;
  __resetWrites: () => void;
  __setReadContent: (text: string) => void;
}

beforeEach(async () => {
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__resetWrites();
  fs.__setReadContent("");
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
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__setReadContent(JSON.stringify(MIN_MODEL));
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.trees).toBe(4);
  expect(result.nodes).toBe(4);
  expect(result.categoricalValueCount).toBe(0);
  const writes = fs.__getWrites();
  expect(writes.length).toBe(1);
  expect(writes[0]![0]).toBe("model.flatbin");
  expect(result.sizeBytes).toBe(writes[0]![1].byteLength);
});

it("convertRunningStyleModelFile handles a numeric LEQ split tree", async () => {
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__setReadContent(JSON.stringify(SPLIT_MODEL));
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.nodes).toBe(6);
  expect(result.categoricalValueCount).toBe(0);
  expect(fs.__getWrites().length).toBe(1);
});

it("convertRunningStyleModelFile encodes a categorical EQ split into categoricalValues", async () => {
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__setReadContent(JSON.stringify(CATEGORICAL_MODEL));
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.categoricalValueCount).toBe(3);
  expect(result.nodes).toBe(6);
});

it("convertRunningStyleModelFile handles numeric LT split decision_type", async () => {
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__setReadContent(JSON.stringify(LT_MODEL));
  const result = await convertRunningStyleModelFile("model.json", "model.flatbin");
  expect(result.nodes).toBe(6);
});

it("parseArgs reads --input and --output from process.argv", () => {
  const originalArgv = process.argv;
  process.argv = [
    "bun",
    "scripts/convert.ts",
    "--input",
    "model.json",
    "--output",
    "model.flatbin",
  ];
  try {
    expect(parseArgs()).toStrictEqual({ input: "model.json", output: "model.flatbin" });
  } finally {
    process.argv = originalArgv;
  }
});

it("parseArgs throws Usage message when --input is missing", () => {
  const originalArgv = process.argv;
  process.argv = ["bun", "scripts/convert.ts", "--output", "x.bin"];
  try {
    expect(() => parseArgs()).toThrow(/Usage/);
  } finally {
    process.argv = originalArgv;
  }
});

it("main calls convertRunningStyleModelFile and logs the JSON result", async () => {
  const originalArgv = process.argv;
  process.argv = [
    "bun",
    "scripts/convert.ts",
    "--input",
    "in.json",
    "--output",
    "out.flatbin",
  ];
  const fs = (await import("node:fs/promises")) as unknown as FsMockHelpers;
  fs.__setReadContent(JSON.stringify(MIN_MODEL));
  const logSpy = vi.spyOn(console, "log").mockImplementation(() => undefined);
  try {
    await main();
    expect(logSpy).toHaveBeenCalledTimes(1);
  } finally {
    process.argv = originalArgv;
  }
});
