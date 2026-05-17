// Run with: bun run test src/scripts/finish-position-features/import-corner-position-predictions.test.ts

import { describe, expect, test } from "vitest";

import {
  DEFAULT_BATCH_SIZE,
  flattenForInsert,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
  parsePredictionLine,
  parseRaceId,
} from "./import-corner-position-predictions";

describe("isCategory", () => {
  test("accepts jra, nar and ban-ei", () => {
    expect(isCategory("jra")).toBe(true);
    expect(isCategory("nar")).toBe(true);
    expect(isCategory("ban-ei")).toBe(true);
  });

  test("rejects unknown values", () => {
    expect(isCategory("bogus")).toBe(false);
  });
});

describe("isTarget", () => {
  test("accepts local and neon", () => {
    expect(isTarget("local")).toBe(true);
    expect(isTarget("neon")).toBe(true);
  });

  test("rejects unknown values", () => {
    expect(isTarget("staging")).toBe(false);
  });
});

describe("parseRaceId", () => {
  test("splits five-part race ids into source and canonical keys", () => {
    const parts = parseRaceId("jra:2025:0101:05:01");
    expect(parts.source).toBe("jra");
    expect(parts.kaisai_nen).toBe("2025");
    expect(parts.kaisai_tsukihi).toBe("0101");
    expect(parts.keibajo_code).toBe("05");
    expect(parts.race_bango).toBe("01");
  });

  test("throws when race_id has wrong shape", () => {
    expect(() => parseRaceId("invalid")).toThrowError(/Invalid race_id/);
  });
});

describe("parsePredictionLine", () => {
  test("parses a well-formed JSONL record", () => {
    const line = JSON.stringify({
      race_id: "jra:2025:0101:05:01",
      ketto_toroku_bango: "2022100001",
      umaban: 3,
      corner_1_pred: 0.12,
      corner_3_pred: 0.18,
      corner_4_pred: 0.22,
    });
    const record = parsePredictionLine(line);
    expect(record.race_id).toBe("jra:2025:0101:05:01");
    expect(record.ketto_toroku_bango).toBe("2022100001");
    expect(record.corner_1_pred).toBe(0.12);
    expect(record.corner_4_pred).toBe(0.22);
  });

  test("converts missing numeric fields to null instead of throwing", () => {
    const line = JSON.stringify({
      race_id: "jra:2025:0101:05:01",
      ketto_toroku_bango: "h1",
      umaban: 1,
    });
    const record = parsePredictionLine(line);
    expect(record.corner_1_pred).toBe(null);
    expect(record.corner_3_pred).toBe(null);
    expect(record.corner_4_pred).toBe(null);
  });

  test("rejects non-object payloads", () => {
    expect(() => parsePredictionLine("[]")).toThrowError(/not an object/);
  });

  test("rejects records missing race_id", () => {
    expect(() =>
      parsePredictionLine(JSON.stringify({ ketto_toroku_bango: "h1", umaban: 1 })),
    ).toThrowError(/race_id/);
  });
});

describe("flattenForInsert", () => {
  test("expands a record into ordered insert parameters", () => {
    const flattened = flattenForInsert(
      {
        race_id: "jra:2025:0101:05:01",
        ketto_toroku_bango: "h1",
        umaban: 1,
        corner_1_pred: 0.1,
        corner_3_pred: 0.2,
        corner_4_pred: 0.3,
      },
      "jra-corner-v1.0",
    );
    expect(flattened).toStrictEqual([
      "jra-corner-v1.0",
      "jra",
      "2025",
      "0101",
      "05",
      "01",
      "h1",
      1,
      0.1,
      0.2,
      0.3,
    ]);
  });
});

describe("parseArgs", () => {
  test("requires --input and --model-version", () => {
    expect(() => parseArgs([])).toThrowError(/--input/);
    expect(() => parseArgs(["--input", "x.jsonl"])).toThrowError(/--model-version/);
  });

  test("parses target, batch-size and activate-category", () => {
    const options = parseArgs([
      "--input",
      "x.jsonl",
      "--model-version",
      "v1",
      "--target",
      "neon",
      "--batch-size",
      "500",
      "--activate-category",
      "jra",
    ]);
    expect(options.target).toBe("neon");
    expect(options.batchSize).toBe(500);
    expect(options.activateCategory).toBe("jra");
  });

  test("rejects --activate-category=all", () => {
    expect(() =>
      parseArgs(["--input", "x.jsonl", "--model-version", "v1", "--activate-category", "all"]),
    ).toThrowError(/jra, nar, or ban-ei/);
  });
});

describe("initialOptions", () => {
  test("defaults batch size to DEFAULT_BATCH_SIZE and target to local", () => {
    const options = initialOptions();
    expect(options.batchSize).toBe(DEFAULT_BATCH_SIZE);
    expect(options.target).toBe("local");
    expect(options.activateCategory).toBe(null);
  });
});
