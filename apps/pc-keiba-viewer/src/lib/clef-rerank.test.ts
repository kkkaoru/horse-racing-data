// Run with bun (bunx vitest).
import { expect, it } from "vitest";

import { buildClefRerankUrl, formatRankChange, readClefRerank } from "./clef-rerank";

const payload = {
  modelVersion: "clef-rerank-jra-v1",
  label: "clef-flash 補正",
  raceKey: "jra:2026:1018:08:11",
  baseModelVersion: "jra-cb-v9-sim-2013-clean-nowin1",
  baseGeneratedAt: "2026-10-18T00:00:00Z",
  generatedAt: "2026-10-18T00:10:00Z",
  predictions: [
    { horseNumber: 3, rank: 1, baseRank: 2, score: 1.2, clefWin: 0.3, clefTop3: 0.7 },
    { horseNumber: 1, rank: 2, baseRank: 1, score: 0.4, clefWin: 0.2, clefTop3: 0.5 },
  ],
};

it("builds the same-origin Worker URL with padded parts", () => {
  expect(
    buildClefRerankUrl({ year: "2026", month: "1", day: "8", keibajoCode: "08", raceNumber: "1" }),
  ).toBe("/api/clef-rerank/20260108/08/01");
});

it("decodes a valid payload and drops unused fields", () => {
  expect(readClefRerank(payload)).toStrictEqual({
    modelVersion: "clef-rerank-jra-v1",
    label: "clef-flash 補正",
    baseModelVersion: "jra-cb-v9-sim-2013-clean-nowin1",
    baseGeneratedAt: "2026-10-18T00:00:00Z",
    predictions: [
      { horseNumber: 3, rank: 1, baseRank: 2, clefWin: 0.3, clefTop3: 0.7 },
      { horseNumber: 1, rank: 2, baseRank: 1, clefWin: 0.2, clefTop3: 0.5 },
    ],
  });
});

it("rejects malformed payloads", () => {
  expect(readClefRerank({ ...payload, predictions: [] })).toBeNull();
  expect(readClefRerank({ ...payload, predictions: [{ horseNumber: 1 }] })).toBeNull();
  expect(readClefRerank({ ...payload, label: 1 })).toBeNull();
  expect(readClefRerank({ error: "clef rerank is not available" })).toBeNull();
  expect(readClefRerank([payload])).toBeNull();
  expect(readClefRerank(null)).toBeNull();
});

it("formats rank movement against the served model", () => {
  expect(formatRankChange({ rank: 1, baseRank: 3 })).toBe("↑2");
  expect(formatRankChange({ rank: 4, baseRank: 1 })).toBe("↓3");
  expect(formatRankChange({ rank: 2, baseRank: 2 })).toBe("→");
});
