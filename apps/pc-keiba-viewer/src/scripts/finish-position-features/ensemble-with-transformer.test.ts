import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { PredictionRow } from "./ensemble-predictions";
import { parsePredictionLine } from "./ensemble-predictions";
import {
  blendTwoSources,
  blendWithTransformer,
  parseBlendArgs,
  validateTransformerWeight,
  type BlendWithTransformerOptions,
} from "./ensemble-with-transformer";

let workspace: string;

beforeEach(() => {
  workspace = mkdtempSync(join(tmpdir(), "ensemble-transformer-test-"));
});

afterEach(() => {
  rmSync(workspace, { force: true, recursive: true });
});

const writeJsonl = (path: string, rows: PredictionRow[]): void => {
  writeFileSync(path, rows.map((row) => JSON.stringify(row)).join("\n"), "utf-8");
};

describe("transformer weight validation", () => {
  it("accepts a default-ish weight", () => {
    expect(() => validateTransformerWeight(0.2)).not.toThrow();
  });

  it("rejects negative weight", () => {
    expect(() => validateTransformerWeight(-0.1)).toThrow(/transformer-weight/);
  });

  it("rejects weight greater than 1", () => {
    expect(() => validateTransformerWeight(1.5)).toThrow(/transformer-weight/);
  });

  it("rejects NaN", () => {
    expect(() => validateTransformerWeight(Number.NaN)).toThrow(/finite/);
  });
});

describe("blend math", () => {
  it("interpolates rank-normalized scores by transformer weight", () => {
    const lgbm: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const transformer: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const blended = blendTwoSources(lgbm, transformer, 0.5);
    const byHorse = new Map(blended.map((row) => [row.kettoTorokuBango, row.blendedScore]));
    expect(byHorse.get("a")).toBeCloseTo(0.5, 6);
    expect(byHorse.get("b")).toBeCloseTo(0.5, 6);
  });

  it("falls back to lgbm score when transformer JSONL lacks a horse", () => {
    const lgbm: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
    ];
    const transformer: PredictionRow[] = [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
    ];
    const blended = blendTwoSources(lgbm, transformer, 0.3);
    expect(blended).toHaveLength(2);
  });
});

describe("cli parsing", () => {
  it("requires all three mandatory args", () => {
    expect(() => parseBlendArgs([])).toThrow(/all required/);
  });

  it("rejects unknown arg", () => {
    expect(() => parseBlendArgs(["--bogus", "x"])).toThrow(/Unknown argument/);
  });

  it("applies default transformer weight when omitted", () => {
    const opts = parseBlendArgs([
      "--lgbm-ensemble-jsonl",
      "/a.jsonl",
      "--transformer-jsonl",
      "/b.jsonl",
      "--output",
      "/o.jsonl",
    ]);
    expect(opts.transformerWeight).toBe(0.2);
  });

  it("parses full options including weight override", () => {
    const opts = parseBlendArgs([
      "--lgbm-ensemble-jsonl",
      "/a.jsonl",
      "--transformer-jsonl",
      "/b.jsonl",
      "--transformer-weight",
      "0.35",
      "--output",
      "/o.jsonl",
    ]);
    expect(opts.transformerWeight).toBe(0.35);
  });
});

describe("end-to-end blend", () => {
  it("writes a re-ranked JSONL with PredictionRow schema", () => {
    const lgbmPath = join(workspace, "lgbm.jsonl");
    const transformerPath = join(workspace, "transformer.jsonl");
    const outputPath = join(workspace, "out.jsonl");
    writeJsonl(lgbmPath, [
      { ketto_toroku_bango: "a", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
      { ketto_toroku_bango: "c", predicted_rank: 3, predicted_score: 0, race_id: "r1", umaban: 3 },
    ]);
    writeJsonl(transformerPath, [
      { ketto_toroku_bango: "a", predicted_rank: 3, predicted_score: 0, race_id: "r1", umaban: 1 },
      { ketto_toroku_bango: "b", predicted_rank: 2, predicted_score: 0, race_id: "r1", umaban: 2 },
      { ketto_toroku_bango: "c", predicted_rank: 1, predicted_score: 0, race_id: "r1", umaban: 3 },
    ]);
    const options: BlendWithTransformerOptions = {
      lgbmEnsembleJsonl: lgbmPath,
      output: outputPath,
      transformerJsonl: transformerPath,
      transformerWeight: 0.5,
    };
    const result = blendWithTransformer(options);
    expect(result).toHaveLength(3);
    const written = readFileSync(outputPath, "utf-8").trim().split("\n");
    expect(written).toHaveLength(3);
    const first = parsePredictionLine(written[0] ?? "");
    expect(first.predicted_rank).toBe(1);
    expect(first.race_id).toBe("r1");
  });
});
