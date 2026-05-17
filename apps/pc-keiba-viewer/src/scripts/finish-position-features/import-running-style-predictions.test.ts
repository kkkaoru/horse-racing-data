// Run with: bun run test src/scripts/finish-position-features/import-running-style-predictions.test.ts

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
} from "./import-running-style-predictions";

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
    const parts = parseRaceId("nar:2025:0228:42:11");
    expect(parts.source).toBe("nar");
    expect(parts.kaisai_nen).toBe("2025");
    expect(parts.kaisai_tsukihi).toBe("0228");
    expect(parts.keibajo_code).toBe("42");
    expect(parts.race_bango).toBe("11");
  });

  test("throws when race_id has wrong shape", () => {
    expect(() => parseRaceId("jra:2025:0101")).toThrowError(/Invalid race_id/);
  });
});

describe("parsePredictionLine", () => {
  test("parses a well-formed JSONL record", () => {
    const line = JSON.stringify({
      race_id: "jra:2025:0101:05:01",
      ketto_toroku_bango: "2022100001",
      umaban: 3,
      p_nige: 0.05,
      p_senkou: 0.62,
      p_sashi: 0.25,
      p_oikomi: 0.08,
      predicted_label: "senkou",
      predicted_class: 1,
    });
    const record = parsePredictionLine(line);
    expect(record.race_id).toBe("jra:2025:0101:05:01");
    expect(record.p_senkou).toBe(0.62);
    expect(record.predicted_label).toBe("senkou");
    expect(record.predicted_class).toBe(1);
  });

  test("rejects missing probability fields", () => {
    const line = JSON.stringify({
      race_id: "jra:2025:0101:05:01",
      ketto_toroku_bango: "h1",
      umaban: 1,
      p_nige: 0.05,
      predicted_label: "senkou",
      predicted_class: 1,
    });
    expect(() => parsePredictionLine(line)).toThrowError(/p_oikomi/);
  });

  test("rejects missing predicted_label", () => {
    const line = JSON.stringify({
      race_id: "jra:2025:0101:05:01",
      ketto_toroku_bango: "h1",
      umaban: 1,
      p_nige: 0.1,
      p_senkou: 0.3,
      p_sashi: 0.4,
      p_oikomi: 0.2,
      predicted_class: 2,
    });
    expect(() => parsePredictionLine(line)).toThrowError(/predicted_label/);
  });

  test("rejects non-object payloads", () => {
    expect(() => parsePredictionLine("[]")).toThrowError(/not an object/);
  });
});

describe("flattenForInsert", () => {
  test("expands a record into ordered insert parameters", () => {
    const flattened = flattenForInsert(
      {
        race_id: "jra:2025:0101:05:01",
        ketto_toroku_bango: "h1",
        umaban: 1,
        p_nige: 0.05,
        p_senkou: 0.62,
        p_sashi: 0.25,
        p_oikomi: 0.08,
        predicted_label: "senkou",
        predicted_class: 1,
      },
      "jra-rs-v1.0",
    );
    expect(flattened).toStrictEqual([
      "jra-rs-v1.0",
      "jra",
      "2025",
      "0101",
      "05",
      "01",
      "h1",
      1,
      0.05,
      0.62,
      0.25,
      0.08,
      "senkou",
      1,
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
      "250",
      "--activate-category",
      "nar",
    ]);
    expect(options.target).toBe("neon");
    expect(options.batchSize).toBe(250);
    expect(options.activateCategory).toBe("nar");
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
  });
});
