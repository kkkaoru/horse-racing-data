// Run with bun.
import { expect, it } from "vitest";

import { composeCatalogTimeScoreRows } from "./time-score-catalog";
import type { CatalogTimeScoreHorse, CatalogTimeScoreInput } from "./time-score-catalog";
import type { TimeScoreHistoryRow } from "./time-score-pipeline";

const target = {
  targetRaceTime: 940,
  targetLast3f: 340,
  targetBodyWeight: 480,
  targetCarriedWeight: 550,
  targetMargin: 5,
};

const horse = (overrides: Partial<CatalogTimeScoreHorse> = {}): CatalogTimeScoreHorse => ({
  horseNumber: "8",
  horseNumberSort: 8,
  horseName: "テストホース",
  historyHorseId: "2021106753",
  currentAge: 4,
  ...overrides,
});

const historyRow = (overrides: Partial<TimeScoreHistoryRow> = {}): TimeScoreHistoryRow => ({
  horseNumber: "8",
  raceDate: "20260601",
  keibajoCode: "05",
  distance: 1600,
  raceTime: 940,
  last3f: 340,
  bodyWeight: 480,
  carriedWeight: 550,
  margin: 5,
  ...overrides,
});

const input = (overrides: Partial<CatalogTimeScoreInput> = {}): CatalogTimeScoreInput => ({
  raceDate: "20260920",
  keibajoCode: "05",
  targetDistance: 1600,
  target,
  horses: [horse()],
  historyByHorseId: new Map([["2021106753", [historyRow()]]]),
  ...overrides,
});

it("emits one scored row per horse with history", () => {
  const rows = composeCatalogTimeScoreRows(input());
  expect(rows).toHaveLength(1);
  expect(rows[0]?.horseNumber).toBe("8");
  expect(rows[0]?.horseName).toBe("テストホース");
  expect(rows[0]?.details).toHaveLength(7);
  expect(rows[0]?.score).toBe(1);
});

it("emits a horse with no history using the 0.5 fallbacks", () => {
  const rows = composeCatalogTimeScoreRows(input({ historyByHorseId: new Map() }));
  expect(rows).toHaveLength(1);
  expect(rows[0]?.score).toBe(0.5);
  expect(rows[0]?.details[0]?.value).toBeNull();
});

it("drops a horse whose all-zero registration number never resolved", () => {
  const rows = composeCatalogTimeScoreRows(
    input({
      horses: [horse({ historyHorseId: null }), horse({ historyHorseId: "", horseNumber: "9" })],
    }),
  );
  expect(rows).toStrictEqual([]);
});

it("orders by score descending then horse-number sort ascending", () => {
  const rows = composeCatalogTimeScoreRows(
    input({
      horses: [
        horse({ horseNumber: "1", horseNumberSort: 1, historyHorseId: "1" }),
        horse({ horseNumber: "2", horseNumberSort: 2, historyHorseId: "2" }),
        horse({ horseNumber: "3", horseNumberSort: 3, historyHorseId: "3" }),
      ],
      historyByHorseId: new Map([
        ["1", [historyRow()]],
        ["2", [historyRow({ raceTime: 2000 })]],
        ["3", []],
      ]),
    }),
  );
  // Horse 1 is a perfect match (score 1); horse 2 loses only the 0.30 race-time
  // weight (0.70); horse 3 has no history at all (0.5) and ranks last.
  expect(rows.map((row) => row.horseNumber)).toStrictEqual(["1", "2", "3"]);
});

it("keeps two horses sharing one registration number apart by horse number", () => {
  const rows = composeCatalogTimeScoreRows(
    input({
      horses: [
        horse({ horseNumber: "1", horseNumberSort: 1, historyHorseId: "2021106753" }),
        horse({ horseNumber: "2", horseNumberSort: 2, historyHorseId: "2021106753" }),
      ],
    }),
  );
  expect(rows.map((row) => row.horseNumber)).toStrictEqual(["1", "2"]);
  expect(rows.map((row) => row.score)).toStrictEqual([1, 1]);
});
