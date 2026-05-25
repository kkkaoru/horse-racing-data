import { describe, expect, test } from "vitest";

import {
  evaluateRunningStyleCacheCoverage,
  isRunningStyleScratchStatus,
  listActiveRunningStyleHorseNumbers,
} from "../src/running-style-entry-coverage";
import type { RaceRunningStyleRow } from "../src/running-style-d1";

const buildRow = (horseNumber: number): RaceRunningStyleRow => ({
  bamei: `馬${horseNumber}`,
  category: "nar",
  horseNumber,
  kaisaiNen: "2026",
  kettoTorokuBango: `horse-${horseNumber}`,
  modelVersion: "v1",
  pNige: 0.1,
  pOikomi: 0.1,
  pSashi: 0.1,
  pSenkou: 0.7,
  predictedAt: "2026-05-24T00:00:00.000Z",
  predictedLabel: "senkou",
  raceKey: "nar:20260524:35:01",
});

describe("running-style entry coverage", () => {
  test("detects scratch statuses", () => {
    expect(isRunningStyleScratchStatus("出走取消")).toBe(true);
    expect(isRunningStyleScratchStatus("")).toBe(false);
    expect(isRunningStyleScratchStatus(null)).toBe(false);
  });

  test("lists only active horse numbers", () => {
    expect(
      listActiveRunningStyleHorseNumbers([
        { horseNumber: "01", status: null },
        { horseNumber: "02", status: "出走取消" },
        { horseNumber: "03", status: "取消" },
      ]),
    ).toEqual([1]);
  });

  test("allows cache when every active runner has a prediction", () => {
    const coverage = evaluateRunningStyleCacheCoverage(
      [
        { horseNumber: "1", status: null },
        { horseNumber: "2", status: "出走取消" },
        { horseNumber: "3", status: null },
      ],
      [buildRow(1), buildRow(3)],
    );
    expect(coverage).toEqual({
      activeHorseCount: 2,
      cacheable: true,
      cacheableRows: [buildRow(1), buildRow(3)],
    });
  });

  test("blocks cache when an active runner is missing", () => {
    const coverage = evaluateRunningStyleCacheCoverage(
      [
        { horseNumber: "1", status: null },
        { horseNumber: "3", status: null },
      ],
      [buildRow(1)],
    );
    expect(coverage.cacheable).toBe(false);
    expect(coverage.cacheableRows).toEqual([buildRow(1)]);
  });

  test("falls back to caching all rows when no entry snapshot is available", () => {
    const coverage = evaluateRunningStyleCacheCoverage(null, [buildRow(1), buildRow(2)]);
    expect(coverage).toStrictEqual({
      activeHorseCount: 2,
      cacheable: true,
      cacheableRows: [buildRow(1), buildRow(2)],
    });
  });

  test("treats empty entry snapshot list the same as null and caches all rows", () => {
    const coverage = evaluateRunningStyleCacheCoverage([], [buildRow(1)]);
    expect(coverage).toStrictEqual({
      activeHorseCount: 1,
      cacheable: true,
      cacheableRows: [buildRow(1)],
    });
  });

  test("returns not-cacheable when no rows and no entries", () => {
    const coverage = evaluateRunningStyleCacheCoverage(null, []);
    expect(coverage).toStrictEqual({
      activeHorseCount: 0,
      cacheable: false,
      cacheableRows: [],
    });
  });
});
