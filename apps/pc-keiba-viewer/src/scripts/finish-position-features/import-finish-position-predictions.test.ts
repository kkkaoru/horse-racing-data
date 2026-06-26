import { expect, test } from "vitest";

import {
  buildUsageText,
  DEFAULT_BATCH_SIZE,
  dedupeBatch,
  flattenForInsert,
  initialOptions,
  isCategory,
  isTarget,
  parseArgs,
  parsePredictionLine,
  parseRaceId,
} from "./import-finish-position-predictions";

test("DEFAULT_BATCH_SIZE batches one thousand rows", () => {
  expect(DEFAULT_BATCH_SIZE).toBe(1000);
});

test("buildUsageText documents the CLI", () => {
  expect(buildUsageText()).toContain("--model-version");
});

test("initialOptions defaults to local target with no activation", () => {
  expect(initialOptions()).toStrictEqual({
    activateCategory: null,
    batchSize: 1000,
    inputPath: "",
    modelVersion: "",
    target: "local",
  });
});

test("isCategory recognises the four supported categories", () => {
  expect(isCategory("jra")).toBe(true);
  expect(isCategory("nar")).toBe(true);
  expect(isCategory("ban-ei")).toBe(true);
  expect(isCategory("all")).toBe(true);
  expect(isCategory("other")).toBe(false);
});

test("isTarget recognises local and neon", () => {
  expect(isTarget("local")).toBe(true);
  expect(isTarget("neon")).toBe(true);
  expect(isTarget("staging")).toBe(false);
});

test("parseRaceId splits canonical race ids", () => {
  expect(parseRaceId("jra:2024:0114:05:11")).toStrictEqual({
    kaisai_nen: "2024",
    kaisai_tsukihi: "0114",
    keibajo_code: "05",
    race_bango: "11",
    source: "jra",
  });
});

test("parseRaceId rejects malformed identifiers", () => {
  expect(() => parseRaceId("jra:2024:bad")).toThrow("Invalid race_id format");
});

test("parsePredictionLine returns a typed prediction record", () => {
  const line = JSON.stringify({
    race_id: "jra:2024:0114:05:11",
    ketto_toroku_bango: "2020100501",
    umaban: 7,
    predicted_score: 0.4,
    predicted_rank: 2,
  });
  expect(parsePredictionLine(line)).toStrictEqual({
    class_code: null,
    distance_band: null,
    field_size_band: null,
    ketto_toroku_bango: "2020100501",
    predicted_rank: 2,
    predicted_score: 0.4,
    race_id: "jra:2024:0114:05:11",
    season_band: null,
    surface: null,
    umaban: 7,
  });
});

test("parsePredictionLine throws when race_id is missing", () => {
  expect(() => parsePredictionLine('{"ketto_toroku_bango":"x"}')).toThrow(
    "predicted record missing race_id",
  );
});

test("parsePredictionLine throws when ketto_toroku_bango is missing", () => {
  expect(() => parsePredictionLine('{"race_id":"jra:1:2:3:4"}')).toThrow(
    "predicted record missing ketto_toroku_bango",
  );
});

test("parsePredictionLine throws when umaban is not a number", () => {
  const line = JSON.stringify({
    race_id: "jra:1:2:3:4",
    ketto_toroku_bango: "x",
    umaban: "x",
    predicted_score: 0,
    predicted_rank: 0,
  });
  expect(() => parsePredictionLine(line)).toThrow("predicted record missing umaban");
});

test("flattenForInsert returns the row in INSERT_COLUMNS order", () => {
  const flat = flattenForInsert(
    {
      race_id: "jra:2024:0114:05:11",
      ketto_toroku_bango: "2020100501",
      umaban: 7,
      predicted_score: 1.234,
      predicted_rank: 3,
      distance_band: null,
      field_size_band: null,
      season_band: null,
      class_code: null,
      surface: null,
    },
    "lambdarank-jra-v1",
  );
  expect(flat).toStrictEqual([
    "lambdarank-jra-v1",
    "jra",
    "2024",
    "0114",
    "05",
    "11",
    "2020100501",
    7,
    1.234,
    3,
    null,
    null,
    3,
    null,
    null,
    null,
    null,
    null,
  ]);
});

test("dedupeBatch keeps every distinct primary key intact", () => {
  expect(
    dedupeBatch([
      {
        race_id: "nar:2025:0906:48:02",
        ketto_toroku_bango: "2021100501",
        umaban: 1,
        predicted_score: 0.1,
        predicted_rank: 1,
      },
      {
        race_id: "nar:2025:0906:48:02",
        ketto_toroku_bango: "2021100502",
        umaban: 2,
        predicted_score: 0.2,
        predicted_rank: 2,
      },
    ]),
  ).toStrictEqual([
    {
      race_id: "nar:2025:0906:48:02",
      ketto_toroku_bango: "2021100501",
      umaban: 1,
      predicted_score: 0.1,
      predicted_rank: 1,
    },
    {
      race_id: "nar:2025:0906:48:02",
      ketto_toroku_bango: "2021100502",
      umaban: 2,
      predicted_score: 0.2,
      predicted_rank: 2,
    },
  ]);
});

test("dedupeBatch collapses duplicate primary keys keeping the last occurrence", () => {
  expect(
    dedupeBatch([
      {
        race_id: "nar:2025:0906:48:02",
        ketto_toroku_bango: "0000000000",
        umaban: 11,
        predicted_score: 0.3,
        predicted_rank: 11,
      },
      {
        race_id: "nar:2025:0906:48:02",
        ketto_toroku_bango: "0000000000",
        umaban: 12,
        predicted_score: 0.4,
        predicted_rank: 12,
      },
    ]),
  ).toStrictEqual([
    {
      race_id: "nar:2025:0906:48:02",
      ketto_toroku_bango: "0000000000",
      umaban: 12,
      predicted_score: 0.4,
      predicted_rank: 12,
    },
  ]);
});

test("dedupeBatch returns an empty array for an empty batch", () => {
  expect(dedupeBatch([])).toStrictEqual([]);
});

test("parseArgs requires --input", () => {
  expect(() => parseArgs(["--model-version", "x"])).toThrow("--input is required.");
});

test("parseArgs requires --model-version", () => {
  expect(() => parseArgs(["--input", "x.jsonl"])).toThrow("--model-version is required.");
});

test("parseArgs accepts the full flag set", () => {
  const parsed = parseArgs([
    "--target",
    "neon",
    "--input",
    "tmp/p.jsonl",
    "--model-version",
    "v1",
    "--activate-category",
    "jra",
    "--batch-size",
    "500",
  ]);
  expect(parsed).toStrictEqual({
    activateCategory: "jra",
    batchSize: 500,
    inputPath: "tmp/p.jsonl",
    modelVersion: "v1",
    target: "neon",
  });
});

test("parseArgs rejects 'all' for --activate-category", () => {
  expect(() =>
    parseArgs(["--input", "x", "--model-version", "v", "--activate-category", "all"]),
  ).toThrow("--activate-category must be jra, nar, or ban-ei.");
});

test("parseArgs rejects unknown argument", () => {
  expect(() => parseArgs(["--input", "x", "--model-version", "v", "--bogus", "x"])).toThrow(
    "Unknown argument: --bogus",
  );
});
