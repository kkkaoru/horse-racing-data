import { mkdirSync, mkdtempSync as makeWorkspaceRoot, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { PredictionRow } from "./ensemble-predictions";
import {
  computeRaceNdcgAt3,
  generateWeightGrid,
  IDEAL_DCG,
  parseSearchArgs,
  pickBestStableTrial,
  readActualsCsv,
  round4,
  searchEnsembleWeights,
  type WeightTrialResult,
} from "./search-ensemble-weights";

let workspace: string;

beforeEach(() => {
  workspace = makeWorkspaceRoot(join(tmpdir(), "search-weights-test-"));
});

afterEach(() => {
  rmSync(workspace, { force: true, recursive: true });
});

const writeJsonl = (path: string, rows: PredictionRow[]): void => {
  writeFileSync(path, rows.map((row) => JSON.stringify(row)).join("\n"), "utf-8");
};

const writeActualsCsv = (path: string, header: string, rows: string[][]): void => {
  const body = [header, ...rows.map((row) => row.join(","))].join("\n");
  writeFileSync(path, body, "utf-8");
};

describe("ndcg helpers", () => {
  it("computes max ndcg when predictions match actuals exactly", () => {
    const actuals = new Map<string, number>([
      ["a", 1],
      ["b", 2],
      ["c", 3],
    ]);
    const predictions: PredictionRow[] = [
      {
        ketto_toroku_bango: "a",
        predicted_rank: 1,
        predicted_score: 0.9,
        race_id: "r1",
        umaban: 1,
      },
      {
        ketto_toroku_bango: "b",
        predicted_rank: 2,
        predicted_score: 0.7,
        race_id: "r1",
        umaban: 2,
      },
      {
        ketto_toroku_bango: "c",
        predicted_rank: 3,
        predicted_score: 0.5,
        race_id: "r1",
        umaban: 3,
      },
    ];
    expect(computeRaceNdcgAt3(predictions, actuals)).toBeCloseTo(1, 6);
  });

  it("returns zero when no top-3 prediction matches an actual placer", () => {
    const actuals = new Map<string, number>([
      ["a", 4],
      ["b", 5],
      ["c", 6],
    ]);
    const predictions: PredictionRow[] = [
      {
        ketto_toroku_bango: "a",
        predicted_rank: 1,
        predicted_score: 0.9,
        race_id: "r1",
        umaban: 1,
      },
      {
        ketto_toroku_bango: "b",
        predicted_rank: 2,
        predicted_score: 0.7,
        race_id: "r1",
        umaban: 2,
      },
      {
        ketto_toroku_bango: "c",
        predicted_rank: 3,
        predicted_score: 0.5,
        race_id: "r1",
        umaban: 3,
      },
    ];
    expect(computeRaceNdcgAt3(predictions, actuals)).toBe(0);
  });

  it("exposes a positive ideal dcg constant", () => {
    expect(IDEAL_DCG).toBeGreaterThan(0);
  });
});

describe("weight grid generation", () => {
  it("only emits weight sets that sum to 1 and have positive top3", () => {
    const grid = generateWeightGrid();
    expect(grid.length).toBeGreaterThan(0);
    for (const weights of grid) {
      expect(round4(weights.lambdarank + weights.top1 + weights.top3)).toBe(1);
      expect(weights.top3).toBeGreaterThan(0);
    }
  });
});

describe("trial selection", () => {
  const trials: WeightTrialResult[] = [
    {
      ndcg_at_3_max_year_gap: 0.04,
      ndcg_at_3_mean: 0.81,
      per_year: [],
      weights: { lambdarank: 0.6, top1: 0.2, top3: 0.2 },
    },
    {
      ndcg_at_3_max_year_gap: 0.01,
      ndcg_at_3_mean: 0.808,
      per_year: [],
      weights: { lambdarank: 0.5, top1: 0.3, top3: 0.2 },
    },
    {
      ndcg_at_3_max_year_gap: 0.02,
      ndcg_at_3_mean: 0.79,
      per_year: [],
      weights: { lambdarank: 0.7, top1: 0.1, top3: 0.2 },
    },
  ];

  it("prefers more stable trial within tolerance", () => {
    const best = pickBestStableTrial(trials, 0.005);
    expect(best.weights).toStrictEqual({ lambdarank: 0.5, top1: 0.3, top3: 0.2 });
  });

  it("returns the top trial when tolerance excludes others", () => {
    const best = pickBestStableTrial(trials, 0.0001);
    expect(best.weights).toStrictEqual({ lambdarank: 0.6, top1: 0.2, top3: 0.2 });
  });

  it("rejects empty trial set", () => {
    expect(() => pickBestStableTrial([], 0.005)).toThrow(/no trials/);
  });
});

describe("actuals csv reader", () => {
  it("filters rows by validation year and missing finish_position", async () => {
    const path = join(workspace, "actuals.csv");
    writeActualsCsv(path, "race_id,ketto_toroku_bango,finish_position", [
      ["jra:2024:0101:01:01", "a", "1"],
      ["jra:2024:0101:01:01", "b", "2"],
      ["jra:2024:0101:01:01", "c", ""],
      ["jra:2023:0101:01:01", "d", "1"],
    ]);
    const rows = await readActualsCsv(path, new Set([2024]));
    expect(rows).toHaveLength(2);
    expect(rows[0]).toStrictEqual({
      finishPosition: 1,
      kettoTorokuBango: "a",
      raceId: "jra:2024:0101:01:01",
    });
  });

  it("throws when required column is missing", async () => {
    const path = join(workspace, "actuals.csv");
    writeActualsCsv(path, "race_id,ketto_toroku_bango", [["x", "y"]]);
    await expect(readActualsCsv(path, new Set([2024]))).rejects.toThrow(/missing column/);
  });
});

describe("cli parsing", () => {
  it("requires all mandatory args", () => {
    expect(() => parseSearchArgs([])).toThrow(/all required/);
  });

  it("rejects unknown flags", () => {
    expect(() => parseSearchArgs(["--bogus", "value"])).toThrow(/Unknown argument/);
  });

  it("rejects empty validation-years", () => {
    expect(() =>
      parseSearchArgs([
        "--lambdarank-dir",
        "/a",
        "--top1-dir",
        "/b",
        "--top3-dir",
        "/c",
        "--actuals-csv",
        "/d.csv",
        "--validation-years",
        ",",
        "--output",
        "/e.json",
      ]),
    ).toThrow(/empty/);
  });

  it("parses a complete arg set", () => {
    const opts = parseSearchArgs([
      "--lambdarank-dir",
      "/abs/lambda",
      "--top1-dir",
      "/abs/top1",
      "--top3-dir",
      "/abs/top3",
      "--actuals-csv",
      "/abs/train.csv",
      "--validation-years",
      "2024,2025",
      "--output",
      "/abs/out.json",
      "--stability-tolerance",
      "0.01",
    ]);
    expect(opts.validationYears).toStrictEqual([2024, 2025]);
    expect(opts.stabilityTolerance).toBe(0.01);
  });
});

describe("search end-to-end", () => {
  it("finds best weight when all models agree", async () => {
    const lambdaDir = join(workspace, "lambda");
    const top1Dir = join(workspace, "top1");
    const top3Dir = join(workspace, "top3");
    mkdirSync(lambdaDir, { recursive: true });
    mkdirSync(top1Dir, { recursive: true });
    mkdirSync(top3Dir, { recursive: true });
    const predictions: PredictionRow[] = [
      {
        ketto_toroku_bango: "a",
        predicted_rank: 1,
        predicted_score: 0.9,
        race_id: "jra:2024:0101:01:01",
        umaban: 1,
      },
      {
        ketto_toroku_bango: "b",
        predicted_rank: 2,
        predicted_score: 0.7,
        race_id: "jra:2024:0101:01:01",
        umaban: 2,
      },
      {
        ketto_toroku_bango: "c",
        predicted_rank: 3,
        predicted_score: 0.5,
        race_id: "jra:2024:0101:01:01",
        umaban: 3,
      },
    ];
    writeJsonl(join(lambdaDir, "2024.jsonl"), predictions);
    writeJsonl(join(top1Dir, "2024.jsonl"), predictions);
    writeJsonl(join(top3Dir, "2024.jsonl"), predictions);
    const actualsPath = join(workspace, "actuals.csv");
    writeActualsCsv(actualsPath, "race_id,ketto_toroku_bango,finish_position", [
      ["jra:2024:0101:01:01", "a", "1"],
      ["jra:2024:0101:01:01", "b", "2"],
      ["jra:2024:0101:01:01", "c", "3"],
    ]);
    const result = await searchEnsembleWeights({
      actualsCsv: actualsPath,
      lambdarankDir: lambdaDir,
      output: join(workspace, "out.json"),
      stabilityTolerance: 0.001,
      top1Dir,
      top3Dir,
      validationYears: [2024],
    });
    expect(result.best.ndcg_at_3_mean).toBeCloseTo(1, 6);
    expect(result.best.per_year[0]?.race_count).toBe(1);
  });
});
