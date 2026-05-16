import { readFileSync } from "node:fs";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
  blendNormalizedScores,
  ensemblePredictions,
  normalizeWithinRace,
  parseEnsembleArgs,
  parsePredictionLine,
  parseWeights,
  rerankWithinRace,
  validateWeights,
  type EnsembleOptions,
  type PredictionRow,
} from "./ensemble-predictions";

let workspace: string;

beforeEach(() => {
  workspace = mkdtempSync(join(tmpdir(), "ensemble-test-"));
});

afterEach(() => {
  rmSync(workspace, { force: true, recursive: true });
});

const writeJsonl = (path: string, rows: PredictionRow[]): void => {
  writeFileSync(path, rows.map((row) => JSON.stringify(row)).join("\n"), "utf-8");
};

describe("ensemble helpers", () => {
  it("normalizes ranks within race", () => {
    const normalized = normalizeWithinRace([
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 10, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 5, race_id: "r1", umaban: 2 },
      { ketto_toroku_bango: "c", predicted_rank: 3, predicted_score: 1, race_id: "r1", umaban: 3 },
    ]);
    const byHorse = new Map(normalized.map((row) => [row.kettoTorokuBango, row.normalizedScore]));
    expect(byHorse.get("a")).toBe(1);
    expect(byHorse.get("b")).toBe(0.5);
    expect(byHorse.get("c")).toBe(0);
  });

  it("handles single runner race without division-by-zero", () => {
    const normalized = normalizeWithinRace([
      {
        ketto_toroku_bango: "a",
        predicted_rank: 1,
        predicted_score: 10,
        race_id: "solo",
        umaban: 1,
      },
    ]);
    expect(normalized).toStrictEqual([
      {
        kettoTorokuBango: "a",
        normalizedScore: 1,
        predictedRank: 1,
        raceId: "solo",
        umaban: 1,
      },
    ]);
  });

  it("blends three normalized scores with provided weights", () => {
    const lambdarank = normalizeWithinRace([
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ]);
    const top1 = normalizeWithinRace([
      { ketto_toroku_bango: "a", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 2 },
    ]);
    const top3 = normalizeWithinRace([
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ]);
    const blended = blendNormalizedScores(lambdarank, top1, top3, {
      lambdarank: 0.5,
      top1: 0.25,
      top3: 0.25,
    });
    const byHorse = new Map(blended.map((row) => [row.kettoTorokuBango, row.blendedScore]));
    expect(byHorse.get("a")).toBeCloseTo(0.75, 6);
    expect(byHorse.get("b")).toBeCloseTo(0.25, 6);
  });

  it("re-ranks combined rows within race by blended score descending", () => {
    const reranked = rerankWithinRace([
      { blendedScore: 0.4, kettoTorokuBango: "a", raceId: "r1", umaban: 1 },
      { blendedScore: 0.6, kettoTorokuBango: "b", raceId: "r1", umaban: 2 },
      { blendedScore: 0.1, kettoTorokuBango: "c", raceId: "r1", umaban: 3 },
    ]);
    const ranks = new Map(reranked.map((row) => [row.ketto_toroku_bango, row.predicted_rank]));
    expect(ranks.get("b")).toBe(1);
    expect(ranks.get("a")).toBe(2);
    expect(ranks.get("c")).toBe(3);
  });
});

describe("ensemble weight parsing", () => {
  it("accepts valid comma-separated weights summing to 1.0", () => {
    expect(parseWeights("0.6,0.2,0.2")).toStrictEqual({
      lambdarank: 0.6,
      top1: 0.2,
      top3: 0.2,
    });
  });

  it("rejects non-three-piece input", () => {
    expect(() => parseWeights("0.5,0.5")).toThrow(/three comma-separated/);
  });

  it("rejects non-numeric input", () => {
    expect(() => parseWeights("a,b,c")).toThrow(/finite numbers/);
  });

  it("rejects weights that do not sum to one", () => {
    expect(() => validateWeights({ lambdarank: 0.5, top1: 0.5, top3: 0.5 })).toThrow(/sum to 1/);
  });

  it("rejects negative weights", () => {
    expect(() => validateWeights({ lambdarank: 1.2, top1: -0.1, top3: -0.1 })).toThrow(/>= 0/);
  });
});

describe("ensemble cli parsing", () => {
  it("requires all five mandatory args", () => {
    expect(() => parseEnsembleArgs([])).toThrow(/all required/);
  });

  it("rejects unknown flag", () => {
    expect(() => parseEnsembleArgs(["--bogus", "value"])).toThrow(/Unknown argument/);
  });

  it("parses a full set of arguments", () => {
    const parsed = parseEnsembleArgs([
      "--lambdarank-jsonl",
      "/abs/a.jsonl",
      "--top1-jsonl",
      "/abs/b.jsonl",
      "--top3-jsonl",
      "/abs/c.jsonl",
      "--weights",
      "0.6,0.2,0.2",
      "--output",
      "/abs/out.jsonl",
    ]);
    expect(parsed.weights).toStrictEqual({ lambdarank: 0.6, top1: 0.2, top3: 0.2 });
    expect(parsed.lambdarankJsonl).toBe("/abs/a.jsonl");
    expect(parsed.output).toBe("/abs/out.jsonl");
  });
});

describe("ensemble end-to-end", () => {
  it("writes a re-ranked JSONL file when given balanced inputs", () => {
    const lambdarankRows: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const top1Rows: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const top3Rows: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const lambdaPath = join(workspace, "lambda.jsonl");
    const top1Path = join(workspace, "top1.jsonl");
    const top3Path = join(workspace, "top3.jsonl");
    const outputPath = join(workspace, "out.jsonl");
    writeJsonl(lambdaPath, lambdarankRows);
    writeJsonl(top1Path, top1Rows);
    writeJsonl(top3Path, top3Rows);
    const options: EnsembleOptions = {
      lambdarankJsonl: lambdaPath,
      output: outputPath,
      top1Jsonl: top1Path,
      top3Jsonl: top3Path,
      weights: { lambdarank: 0.6, top1: 0.2, top3: 0.2 },
    };
    const result = ensemblePredictions(options);
    expect(result).toHaveLength(2);
    const written = readFileSync(outputPath, "utf-8").trim().split("\n");
    expect(written).toHaveLength(2);
    const first = parsePredictionLine(written[0] ?? "");
    expect(first.race_id).toBe("r1");
    expect(first.predicted_rank).toBe(1);
    expect(first.ketto_toroku_bango).toBe("a");
  });

  it("falls back to lambdarank score when a horse is missing from a binary jsonl", () => {
    const lambdarankRows: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const top1Rows: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
    ];
    const top3Rows: PredictionRow[] = lambdarankRows;
    const lambdaPath = join(workspace, "lambda.jsonl");
    const top1Path = join(workspace, "top1.jsonl");
    const top3Path = join(workspace, "top3.jsonl");
    const outputPath = join(workspace, "out.jsonl");
    writeJsonl(lambdaPath, lambdarankRows);
    writeJsonl(top1Path, top1Rows);
    writeJsonl(top3Path, top3Rows);
    const result = ensemblePredictions({
      lambdarankJsonl: lambdaPath,
      output: outputPath,
      top1Jsonl: top1Path,
      top3Jsonl: top3Path,
      weights: { lambdarank: 0.5, top1: 0.25, top3: 0.25 },
    });
    expect(result).toHaveLength(2);
  });
});
