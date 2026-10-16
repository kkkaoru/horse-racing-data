// Run with bun. Artifact reads fail closed without contacting Neon or mutating R2.
import { expect, test, vi } from "vitest";

import {
  readPredictionArtifact,
  type ReadPredictionArtifactParams,
} from "./prediction-result-artifact";

interface InvalidFieldCase {
  field: string;
  value: unknown;
}

const row1 = {
  horseId: "H1",
  horseNumber: "1",
  predictedRank: 2,
  predictedScore: 1,
  predictedTop1Prob: 0.25,
  predictedTop3Prob: 0.75,
};
const row2 = {
  horseId: "H2",
  horseNumber: "2",
  predictedRank: 1,
  predictedScore: 2,
  predictedTop1Prob: null,
  predictedTop3Prob: null,
};
const body = {
  schemaVersion: 1,
  category: "jra",
  runYmd: "20261004",
  keibajoCode: "05",
  raceBango: "01",
  generationId: "generation-1",
  inputFingerprint: "a".repeat(64),
  modelVersion: "model-A",
  predictionGeneratedAt: "2026-10-03T20:00:00.000Z",
  rows: [row2, row1],
};

const fixture = async (text: string): Promise<ReadPredictionArtifactParams> => {
  const bytes: ArrayBuffer = await new Response(text).arrayBuffer();
  const digest: ArrayBuffer = await crypto.subtle.digest("SHA-256", bytes);
  const sha256: string = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");
  return {
    category: "jra",
    runYmd: "20261004",
    keibajoCode: "05",
    raceBango: "01",
    bucket: {
      get: vi.fn(async () => ({ size: bytes.byteLength, arrayBuffer: async () => bytes })),
    },
    reference: {
      key: `prediction-results/v1/jra/20261004/05/01/generation-1/${sha256}.json`,
      sha256,
      generationId: "generation-1",
      inputFingerprint: "a".repeat(64),
      modelVersion: "model-A",
      predictionGeneratedAt: "2026-10-03T20:00:00.000Z",
      runners: [
        { horseId: "H1", horseNumber: "1" },
        { horseId: "H2", horseNumber: "2" },
      ],
    },
  };
};

test("reads the exact generation, orders by horse number, and preserves nullable probabilities", async () => {
  const params = await fixture(JSON.stringify(body));
  expect(await readPredictionArtifact(params)).toStrictEqual([
    {
      umaban: "1",
      modelVersion: "model-A",
      predictionGeneratedAt: "2026-10-03T20:00:00.000Z",
      predictedRank: 2,
      predictedScore: 1,
      predictedTop1Prob: 0.25,
      predictedTop3Prob: 0.75,
    },
    {
      umaban: "2",
      modelVersion: "model-A",
      predictionGeneratedAt: "2026-10-03T20:00:00.000Z",
      predictedRank: 1,
      predictedScore: 2,
      predictedTop1Prob: null,
      predictedTop3Prob: null,
    },
  ]);
});

test.each<InvalidFieldCase>([
  { field: "schemaVersion", value: 2 },
  { field: "category", value: "nar" },
  { field: "runYmd", value: "20261003" },
  { field: "keibajoCode", value: "08" },
  { field: "raceBango", value: "02" },
  { field: "generationId", value: "old" },
  { field: "inputFingerprint", value: "b".repeat(64) },
  { field: "modelVersion", value: "old-model" },
  { field: "predictionGeneratedAt", value: "2026-10-03T19:00:00.000Z" },
  { field: "rows", value: null },
  { field: "rows", value: [] },
])("rejects mismatched artifact $field", async ({ field, value }) => {
  const params = await fixture(JSON.stringify({ ...body, [field]: value }));
  await expect(readPredictionArtifact(params)).rejects.toThrow("generation or scope mismatch");
});

test.each([{ value: null }, { value: [] }, { value: "text" }])(
  "rejects non-object payload $value",
  async ({ value }) => {
    await expect(readPredictionArtifact(await fixture(JSON.stringify(value)))).rejects.toThrow(
      "generation or scope mismatch",
    );
  },
);

test.each<InvalidFieldCase>([
  { field: "horseId", value: "" },
  { field: "horseId", value: 1 },
  { field: "horseNumber", value: 1 },
  { field: "horseNumber", value: "01" },
  { field: "predictedRank", value: "1" },
  { field: "predictedRank", value: 1.5 },
  { field: "predictedRank", value: 0 },
  { field: "predictedRank", value: 3 },
  { field: "predictedScore", value: null },
  { field: "predictedTop1Prob", value: "0.5" },
  { field: "predictedTop1Prob", value: -0.1 },
  { field: "predictedTop1Prob", value: 1.1 },
  { field: "predictedTop3Prob", value: "bad" },
])("rejects malformed row $field", async ({ field, value }) => {
  const params = await fixture(
    JSON.stringify({ ...body, rows: [{ ...row2, [field]: value }, row1] }),
  );
  await expect(readPredictionArtifact(params)).rejects.toThrow("Invalid prediction artifact row");
});

test("rejects a non-object row", async () => {
  await expect(
    readPredictionArtifact(await fixture(JSON.stringify({ ...body, rows: [null, row1] }))),
  ).rejects.toThrow("Invalid prediction artifact row");
});

test.each(["predictedScore", "predictedTop1Prob"])(
  "rejects overflow JSON numeric %s",
  async (field) => {
    const text: string = JSON.stringify({
      ...body,
      rows: [{ ...row2, [field]: 987654321 }, row1],
    }).replace("987654321", "1e999");
    await expect(readPredictionArtifact(await fixture(text))).rejects.toThrow(
      "Invalid prediction artifact row",
    );
  },
);

test.each([
  { rows: [row1, row1] },
  { rows: [{ ...row2, horseId: "wrong" }, row1] },
  { rows: [{ ...row2, predictedRank: 2 }, row1] },
])("rejects duplicate/mismatched horses or ranks", async ({ rows }) => {
  await expect(
    readPredictionArtifact(await fixture(JSON.stringify({ ...body, rows }))),
  ).rejects.toThrow("roster or rank mismatch");
});

test.each([
  { sha256: "bad" },
  { inputFingerprint: "bad" },
  { generationId: "../other" },
  { modelVersion: " " },
  { predictionGeneratedAt: "bad-date" },
  { key: "unrelated/key" },
])("rejects an invalid reference before reading R2", async (patch) => {
  const params = await fixture(JSON.stringify(body));
  const getSpy = vi.spyOn(params.bucket, "get");
  await expect(
    readPredictionArtifact({ ...params, reference: { ...params.reference, ...patch } }),
  ).rejects.toThrow("Invalid prediction artifact reference");
  expect(getSpy).not.toHaveBeenCalled();
});

test.each([{ runYmd: "../bad" }, { keibajoCode: "../" }, { raceBango: "x" }])(
  "rejects invalid scope",
  async (patch) => {
    const params = await fixture(JSON.stringify(body));
    await expect(readPredictionArtifact({ ...params, ...patch })).rejects.toThrow(
      "Invalid prediction artifact reference",
    );
  },
);

test.each([
  { runners: [] },
  { runners: Array.from({ length: 100 }, () => ({ horseId: "H1", horseNumber: "1" })) },
  { runners: [{ horseId: "H1", horseNumber: "0" }] },
  { runners: [{ horseId: " ", horseNumber: "1" }] },
  {
    runners: [
      { horseId: "H1", horseNumber: "1" },
      { horseId: "H2", horseNumber: "1" },
    ],
  },
  {
    runners: [
      { horseId: "H1", horseNumber: "1" },
      { horseId: "H1", horseNumber: "2" },
    ],
  },
])("rejects invalid manifest roster", async ({ runners }) => {
  const params = await fixture(JSON.stringify(body));
  await expect(
    readPredictionArtifact({ ...params, reference: { ...params.reference, runners } }),
  ).rejects.toThrow("Invalid prediction artifact runner set");
});

test("rejects a missing object", async () => {
  const params = await fixture(JSON.stringify(body));
  await expect(
    readPredictionArtifact({ ...params, bucket: { get: async () => null } }),
  ).rejects.toThrow("Missing or oversized");
});

test("rejects oversized metadata without consuming the body", async () => {
  const params = await fixture(JSON.stringify(body));
  const arrayBuffer = vi.fn(async () => new ArrayBuffer(0));
  await expect(
    readPredictionArtifact({
      ...params,
      bucket: { get: async () => ({ size: 65537, arrayBuffer }) },
    }),
  ).rejects.toThrow("Missing or oversized");
  expect(arrayBuffer).not.toHaveBeenCalled();
});

test("rejects an oversized body despite incorrect metadata", async () => {
  const params = await fixture(JSON.stringify(body));
  await expect(
    readPredictionArtifact({
      ...params,
      bucket: { get: async () => ({ size: 1, arrayBuffer: async () => new ArrayBuffer(65537) }) },
    }),
  ).rejects.toThrow("Oversized prediction artifact body");
});

test("rejects changed bytes under the referenced key", async () => {
  const params = await fixture(JSON.stringify(body));
  await expect(
    readPredictionArtifact({
      ...params,
      bucket: { get: async () => ({ size: 0, arrayBuffer: async () => new ArrayBuffer(0) }) },
    }),
  ).rejects.toThrow("checksum mismatch");
});

test("rejects malformed JSON even with a matching checksum", async () => {
  await expect(readPredictionArtifact(await fixture("{"))).rejects.toThrow();
});
