// Run with bun.
import { expect, it } from "vitest";

import type { RaceHistoryRow } from "./race-history-catalog";
import {
  composeCatalogTimeScoreRows,
  groupHistoryByHorseId,
  toAppTimeScoreRows,
} from "./time-score-catalog";
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

const history = (overrides: Partial<RaceHistoryRow> = {}): RaceHistoryRow => ({
  kettoTorokuBango: "2021106753",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0601",
  keibajoCode: "05",
  raceBango: "08",
  umaban: "08",
  kyori: "1600",
  sohaTime: "1110",
  kohan3f: "0340",
  bataiju: "480",
  futanJuryo: "0550",
  timeSa: "0005",
  kakuteiChakujun: "01",
  ...overrides,
});

it("groups history rows by registration number and converts every field", () => {
  const grouped = groupHistoryByHorseId([
    history(),
    history({ umaban: "10", keibajoCode: "06" }),
    history({ kettoTorokuBango: "2022100001", kyori: null }),
  ]);
  expect([...grouped.keys()]).toStrictEqual(["2021106753", "2022100001"]);
  expect(grouped.get("2021106753")).toStrictEqual([
    {
      horseNumber: "8",
      raceDate: "20260601",
      keibajoCode: "05",
      distance: 1600,
      raceTime: 710,
      last3f: 340,
      bodyWeight: 480,
      carriedWeight: 550,
      margin: 5,
    },
    {
      horseNumber: "10",
      raceDate: "20260601",
      keibajoCode: "06",
      distance: 1600,
      raceTime: 710,
      last3f: 340,
      bodyWeight: 480,
      carriedWeight: 550,
      margin: 5,
    },
  ]);
  expect(grouped.get("2022100001")?.[0]?.distance).toBeNull();
});

it("treats an empty venue or registration number as absent", () => {
  const grouped = groupHistoryByHorseId([
    history({
      keibajoCode: "",
      umaban: null,
      kohan3f: null,
      futanJuryo: null,
      timeSa: null,
      sohaTime: null,
    }),
    history({ kettoTorokuBango: "  " }),
  ]);
  expect([...grouped.keys()]).toStrictEqual(["2021106753"]);
  const only = grouped.get("2021106753")?.[0];
  expect(only?.keibajoCode).toBeNull();
  expect(only?.horseNumber).toBe("0");
  expect(only?.raceTime).toBeNull();
  expect(only?.last3f).toBeNull();
  expect(only?.carriedWeight).toBeNull();
  expect(only?.margin).toBeNull();
  // A row whose metrics are all null is still emitted, scored only from the
  // distance and venue components.
  const scored = composeCatalogTimeScoreRows(input({ historyByHorseId: grouped }));
  expect(scored).toHaveLength(1);
  expect(scored[0]?.details.map((detail) => detail.score)).toStrictEqual([
    0.5, 0.5, 1, 0.5, 1, 0.5, 0.5,
  ]);
});

it("adds the jockey name required by the app row type", () => {
  const composed = composeCatalogTimeScoreRows(input());
  const rows = toAppTimeScoreRows(composed, new Map([["8", "ルメール"]]));
  expect(rows).toStrictEqual([
    {
      details: composed[0]?.details,
      horseName: "テストホース",
      horseNumber: "8",
      jockeyName: "ルメール",
      score: 1,
    },
  ]);
  // A horse the runner list never named falls back to an empty jockey name,
  // matching toTimeScoreRows' toStringValue of a missing field.
  expect(toAppTimeScoreRows(composed, new Map())[0]?.jockeyName).toBe("");
});

it("decodes mixed history venues into kg and tenths of kg before weighting", () => {
  const grouped = groupHistoryByHorseId([
    history({
      keibajoCode: "83",
      bataiju: "3E8",
      futanJuryo: "258",
      kohan3f: "600",
      timeSa: "+006",
    }),
    history({ keibajoCode: "83", bataiju: "3e8", futanJuryo: "24E", timeSa: "-014" }),
    history({ keibajoCode: " 83 ", bataiju: " 3E8 ", futanJuryo: "26C", timeSa: "000" }),
    history({ keibajoCode: "05", bataiju: "600", futanJuryo: "600", timeSa: "1000" }),
  ]);
  expect(
    grouped.get("2021106753")?.map(({ bodyWeight, carriedWeight, margin, last3f }) => ({
      bodyWeight,
      carriedWeight,
      margin,
      last3f,
    })),
  ).toStrictEqual([
    { bodyWeight: 1000, carriedWeight: 6000, margin: 6, last3f: 600 },
    { bodyWeight: 1000, carriedWeight: 5900, margin: 14, last3f: 340 },
    { bodyWeight: 1000, carriedWeight: 6200, margin: 0, last3f: 340 },
    { bodyWeight: 600, carriedWeight: 600, margin: 1000, last3f: 340 },
  ]);
});

it.each([null, "", " ", "000", "FFF", "3E8oops", "60kg", "-600", "60.0", "+600"])(
  "rejects unavailable or malformed decimal weights and last3f %j",
  (raw) => {
    const row = groupHistoryByHorseId([
      history({ bataiju: raw, futanJuryo: raw, kohan3f: raw }),
    ]).get("2021106753")?.[0];
    expect([row?.bodyWeight, row?.carriedWeight, row?.last3f]).toStrictEqual([null, null, null]);
  },
);

it.each([null, "", " ", "0", "000", "FFF", "fff", "3E8oops", "0x258", "-258", "24 E", "258.0"])(
  "rejects unavailable or malformed hexadecimal weights %j",
  (raw) => {
    const row = groupHistoryByHorseId([
      history({ keibajoCode: "83", bataiju: raw, futanJuryo: raw }),
    ]).get("2021106753")?.[0];
    expect([row?.bodyWeight, row?.carriedWeight]).toStrictEqual([null, null]);
  },
);

it.each(["", " ", "FFF", "6x", "1e2", "--014", "+-006", "+ 006", "0.6", "+"])(
  "rejects malformed margin %j without extracting digits",
  (raw) => {
    expect(
      groupHistoryByHorseId([history({ timeSa: raw })]).get("2021106753")?.[0]?.margin,
    ).toBeNull();
  },
);

it("interprets minutes only in sohaTime", () => {
  const row = groupHistoryByHorseId([
    history({ sohaTime: "1000", kohan3f: "1000", futanJuryo: "1000", timeSa: "1000" }),
  ]).get("2021106753")?.[0];
  expect([row?.raceTime, row?.last3f, row?.carriedWeight, row?.margin]).toStrictEqual([
    600, 1000, 1000, 1000,
  ]);
});

it("rejects numeric overflow without imposing weight ranges", () => {
  const grouped = groupHistoryByHorseId([
    history({ bataiju: "9".repeat(400), futanJuryo: "9".repeat(400) }),
    history({ keibajoCode: "83", bataiju: "F".repeat(400), futanJuryo: "F".repeat(400) }),
  ]);
  expect(
    grouped.get("2021106753")?.map(({ bodyWeight, carriedWeight }) => [bodyWeight, carriedWeight]),
  ).toStrictEqual([
    [null, null],
    [null, null],
  ]);
});
