// Run with bun (exercised via `bunx vitest run`).
import { describe, expect, it } from "vitest";

import {
  buildHorseRaceChartSeriesList,
  buildHorseRaceCorrelationRows,
  filterHorseRaceResultsToRecentYears,
  formatHorseRaceChartDate,
  getHorseRaceChartMetricValue,
  HORSE_RACE_CHART_METRIC_LABELS,
  HORSE_RACE_CHART_METRIC_UNITS,
  HORSE_RACE_CHART_METRICS,
  type HorseRaceChartRunner,
} from "./horse-race-results-chart-data";
import type { HorseRaceResult } from "./race-types";

const buildResult = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: null,
  bamei: "テストホース",
  banushimei: "テスト馬主",
  barei: "4",
  bataiju: "480",
  chokyoshimeiRyakusho: "田中",
  corner1: "01",
  corner2: "02",
  corner3: "02",
  corner4: "03",
  currentBarei: "4",
  currentJockey: "山田",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  futanJuryo: "560",
  gradeCode: null,
  hassoJikoku: "1200",
  juryoShubetsuCode: null,
  kaisaiNen: "2025",
  kaisaiTsukihi: "0112",
  kakuteiChakujun: "1",
  keibajoCode: "05",
  kettoTorokuBango: "2020000001",
  kishumeiRyakusho: "山田",
  kohan3f: "360",
  kyori: "1200",
  kyosoJokenCode: null,
  kyosoJokenMeisho: null,
  kyosoKigoCode: null,
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  kyosoShubetsuCode: null,
  raceBango: "01",
  seibetsuCode: "1",
  shussoTosu: "16",
  sohaTime: "1123",
  tanshoNinkijun: "1",
  tanshoOdds: "1.2",
  tenkoCode: "1",
  timeSa: null,
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

const buildRunner = (overrides: Partial<HorseRaceChartRunner>): HorseRaceChartRunner => ({
  bataiju: "486",
  kettoTorokuBango: "2020000001",
  umaban: "01",
  wakuban: "3",
  zogenFugo: "+",
  zogenSa: "006",
  ...overrides,
});

describe("horse race results chart data", () => {
  it("returns an empty array when results are empty", () => {
    expect(filterHorseRaceResultsToRecentYears([], 3)).toStrictEqual([]);
  });

  it("returns an empty array when every race date is invalid", () => {
    const filtered = filterHorseRaceResultsToRecentYears(
      [
        buildResult({ kaisaiNen: "20XY", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "041" }),
      ],
      3,
    );
    expect(filtered).toStrictEqual([]);
  });

  it("drops rows with malformed race dates and keeps valid rows", () => {
    const filtered = filterHorseRaceResultsToRecentYears(
      [
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "04A5" }),
      ],
      3,
    );
    expect(filtered.map((row) => row.kaisaiNen + row.kaisaiTsukihi)).toStrictEqual(["20250415"]);
  });

  it("keeps rows on the cutoff boundary and drops rows just before it", () => {
    const filtered = filterHorseRaceResultsToRecentYears(
      [
        buildResult({ kaisaiNen: "2026", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2023", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2023", kaisaiTsukihi: "0414" }),
      ],
      3,
    );
    expect(filtered.map((row) => row.kaisaiNen + row.kaisaiTsukihi)).toStrictEqual([
      "20260415",
      "20230415",
    ]);
  });

  it("derives the cutoff from the newest race date in any input order", () => {
    const filtered = filterHorseRaceResultsToRecentYears(
      [
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0101" }),
        buildResult({ kaisaiNen: "2026", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2023", kaisaiTsukihi: "0501" }),
        buildResult({ kaisaiNen: "2023", kaisaiTsukihi: "0101" }),
      ],
      3,
    );
    expect(filtered.map((row) => row.kaisaiNen + row.kaisaiTsukihi)).toStrictEqual([
      "20250101",
      "20260415",
      "20230501",
    ]);
  });

  it("subtracts the requested number of years from the newest date", () => {
    const filtered = filterHorseRaceResultsToRecentYears(
      [
        buildResult({ kaisaiNen: "2026", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0415" }),
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0414" }),
      ],
      1,
    );
    expect(filtered.map((row) => row.kaisaiNen + row.kaisaiTsukihi)).toStrictEqual([
      "20260415",
      "20250415",
    ]);
  });

  it("parses the finish position metric", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ kakuteiChakujun: "05" }), "finish")).toBe(5);
  });

  it("returns null for the all-zero finish sentinel", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ kakuteiChakujun: "00" }), "finish"),
    ).toBeNull();
  });

  it("returns null for a blank finish position", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ kakuteiChakujun: " " }), "finish"),
    ).toBeNull();
  });

  it("returns null for a non-numeric finish position", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ kakuteiChakujun: "中止" }), "finish"),
    ).toBeNull();
  });

  it("parses the popularity metric", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ tanshoNinkijun: "12" }), "popularity")).toBe(
      12,
    );
  });

  it("returns null for the all-zero popularity sentinel", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ tanshoNinkijun: "00" }), "popularity"),
    ).toBeNull();
  });

  it("returns null for a blank popularity", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ tanshoNinkijun: null }), "popularity"),
    ).toBeNull();
  });

  it("returns null for a non-numeric popularity", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ tanshoNinkijun: "1A" }), "popularity"),
    ).toBeNull();
  });

  it("parses a non-Ban-ei decimal weight", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "480", keibajoCode: "05" }), "weight"),
    ).toBe(480);
  });

  it("returns null for the all-zero weight sentinel", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ bataiju: "000" }), "weight")).toBeNull();
  });

  it("returns null for the FFF weight sentinel", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ bataiju: "FFF" }), "weight")).toBeNull();
  });

  it("returns null for the lowercase fff weight sentinel", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ bataiju: "fff" }), "weight")).toBeNull();
  });

  it("returns null for a blank weight", () => {
    expect(getHorseRaceChartMetricValue(buildResult({ bataiju: " " }), "weight")).toBeNull();
  });

  it("returns null for the non-Ban-ei unmeasured weight 999", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "999", keibajoCode: "05" }), "weight"),
    ).toBeNull();
  });

  it("decodes Ban-ei weights as hexadecimal", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "3E8", keibajoCode: "83" }), "weight"),
    ).toBe(1000);
  });

  it("keeps Ban-ei 999 as the hexadecimal value 2457", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "999", keibajoCode: "83" }), "weight"),
    ).toBe(2457);
  });

  it("returns null for a non-numeric non-Ban-ei weight", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "ABC", keibajoCode: "05" }), "weight"),
    ).toBeNull();
  });

  it("returns null for a non-hexadecimal Ban-ei weight", () => {
    expect(
      getHorseRaceChartMetricValue(buildResult({ bataiju: "GGG", keibajoCode: "83" }), "weight"),
    ).toBeNull();
  });

  it("parses a positive weight delta", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "+", zogenSa: "012" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBe(12);
  });

  it("parses a negative weight delta", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "-", zogenSa: "008" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBe(-8);
  });

  it("treats a blank sign as positive", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: " ", zogenSa: "004" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBe(4);
  });

  it("treats a null sign as positive", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: null, zogenSa: "006" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBe(6);
  });

  it("returns null for the all-zero delta sentinel", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "+", zogenSa: "000" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns null for the FFF delta sentinel", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "+", zogenSa: "FFF" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns null for the lowercase fff delta sentinel", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "+", zogenSa: "fff" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns null for a blank delta", () => {
    const row = buildResult({ bataiju: "480", zogenFugo: "+", zogenSa: null });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns null for the delta when the weight is invalid", () => {
    const row = buildResult({ bataiju: "000", zogenFugo: "+", zogenSa: "012" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns null for the delta when the weight is the unmeasured sentinel", () => {
    const row = buildResult({ bataiju: "999", keibajoCode: "05", zogenFugo: "-", zogenSa: "012" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("decodes Ban-ei deltas as hexadecimal", () => {
    const row = buildResult({ bataiju: "3E8", keibajoCode: "83", zogenFugo: "-", zogenSa: "00B" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBe(-11);
  });

  it("returns null for a non-numeric delta", () => {
    const row = buildResult({ bataiju: "480", keibajoCode: "05", zogenFugo: "+", zogenSa: "XYZ" });
    expect(getHorseRaceChartMetricValue(row, "weightDelta")).toBeNull();
  });

  it("returns an empty series list for empty results", () => {
    expect(buildHorseRaceChartSeriesList({ metric: "finish", results: [] })).toStrictEqual([]);
  });

  it("drops rows whose kettoTorokuBango is blank", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: " " }), buildResult({ kettoTorokuBango: null })],
    });
    expect(seriesList).toStrictEqual([]);
  });

  it("groups rows of the same horse and sorts points by date ascending", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", kakuteiChakujun: "03" }),
        buildResult({ kaisaiNen: "2024", kaisaiTsukihi: "0512", kakuteiChakujun: "01" }),
      ],
    });
    expect(seriesList).toStrictEqual([
      {
        bamei: "テストホース",
        color: "#d62728",
        frame: null,
        kettoTorokuBango: "2020000001",
        points: [
          {
            dateValue: 1715472000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20240512",
            value: 1,
          },
          {
            dateValue: 1736640000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20250112",
            value: 3,
          },
        ],
        umaban: 1,
      },
    ]);
  });

  it("breaks same-date point ties by raceBango ascending", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0601",
          kakuteiChakujun: "01",
          raceBango: "05",
        }),
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0601",
          kakuteiChakujun: "02",
          raceBango: "11",
        }),
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0601",
          kakuteiChakujun: "03",
          raceBango: "02",
        }),
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0601",
          kakuteiChakujun: "04",
          raceBango: "05",
        }),
      ],
    });
    expect(seriesList).toStrictEqual([
      {
        bamei: "テストホース",
        color: "#d62728",
        frame: null,
        kettoTorokuBango: "2020000001",
        points: [
          {
            dateValue: 1748736000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20250601",
            value: 3,
          },
          {
            dateValue: 1748736000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20250601",
            value: 1,
          },
          {
            dateValue: 1748736000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20250601",
            value: 4,
          },
          {
            dateValue: 1748736000000,
            jockey: "山田",
            kyori: "1200",
            raceDate: "20250601",
            value: 2,
          },
        ],
        umaban: 1,
      },
    ]);
  });

  it("keeps a series with zero points when the metric is always null", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ bataiju: "000" })],
    });
    expect(seriesList).toStrictEqual([
      {
        bamei: "テストホース",
        color: "#d62728",
        frame: null,
        kettoTorokuBango: "2020000001",
        points: [],
        umaban: 1,
      },
    ]);
  });

  it("falls back to 不明 for a blank horse name", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ bamei: " " })],
    });
    expect(seriesList[0]?.bamei).toBe("不明");
  });

  it("falls back to 不明 for a null horse name", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ bamei: null })],
    });
    expect(seriesList[0]?.bamei).toBe("不明");
  });

  it("parses currentUmaban into a numeric umaban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ currentUmaban: "07" })],
    });
    expect(seriesList[0]?.umaban).toBe(7);
  });

  it("treats an all-zero currentUmaban as a null umaban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ currentUmaban: "00" })],
    });
    expect(seriesList[0]?.umaban).toBeNull();
  });

  it("sorts series by umaban ascending", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "03", kettoTorokuBango: "2020000003" }),
        buildResult({ currentUmaban: "01", kettoTorokuBango: "2020000001" }),
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
      ],
    });
    expect(seriesList.map((series) => series.umaban)).toStrictEqual([1, 2, 3]);
    expect(seriesList.map((series) => series.color)).toStrictEqual([
      "#d62728",
      "#1f77b4",
      "#2ca02c",
    ]);
  });

  it("places a null umaban series after a numbered series", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: null, kettoTorokuBango: "2020000009" }),
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
      ],
    });
    expect(seriesList.map((series) => series.umaban)).toStrictEqual([2, null]);
  });

  it("keeps a numbered series before a null umaban series when already ordered", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "2020000009" }),
      ],
    });
    expect(seriesList.map((series) => series.umaban)).toStrictEqual([2, null]);
  });

  it("breaks equal umaban ties by kettoTorokuBango ascending", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "05", kettoTorokuBango: "2222222222" }),
        buildResult({ currentUmaban: "05", kettoTorokuBango: "1111111111" }),
      ],
    });
    expect(seriesList.map((series) => series.kettoTorokuBango)).toStrictEqual([
      "1111111111",
      "2222222222",
    ]);
  });

  it("breaks ties between two null umaban series by kettoTorokuBango", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: null, kettoTorokuBango: "9999999999" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "0000000001" }),
      ],
    });
    expect(seriesList.map((series) => series.kettoTorokuBango)).toStrictEqual([
      "0000000001",
      "9999999999",
    ]);
    expect(seriesList.map((series) => series.color)).toStrictEqual(["#d62728", "#1f77b4"]);
  });

  it("cycles palette colors so umaban 19 matches umaban 1", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "19", kettoTorokuBango: "2020000019" }),
        buildResult({ currentUmaban: "01", kettoTorokuBango: "2020000001" }),
      ],
    });
    expect(seriesList.map((series) => series.color)).toStrictEqual(["#d62728", "#d62728"]);
  });

  it("assigns the fallback color for an out-of-range umaban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ currentUmaban: "-3" })],
    });
    expect(seriesList[0]?.color).toBe("#52525b");
  });

  it("gives a null umaban series a color not used by umaban-keyed series", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "2020000009" }),
      ],
    });
    expect(seriesList.map((series) => series.color)).toStrictEqual(["#1f77b4", "#d62728"]);
  });

  it("cycles fallback colors through the palette entries unused by umaban-keyed series", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "01", kettoTorokuBango: "2020000001" }),
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000001" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000002" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000003" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000004" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000005" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000006" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000007" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000008" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000009" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000010" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000011" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000012" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000013" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000014" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000015" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000016" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000017" }),
      ],
    });
    expect(seriesList.map((series) => series.color)).toStrictEqual([
      "#d62728",
      "#1f77b4",
      "#2ca02c",
      "#ff7f0e",
      "#9467bd",
      "#8c564b",
      "#e377c2",
      "#17becf",
      "#bcbd22",
      "#7f7f7f",
      "#393b79",
      "#637939",
      "#8c6d31",
      "#843c39",
      "#7b4173",
      "#5254a3",
      "#0f766e",
      "#b45309",
      "#2ca02c",
    ]);
  });

  it("keeps the index-based fallback color when every palette entry is used", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ currentUmaban: "01", kettoTorokuBango: "2020000001" }),
        buildResult({ currentUmaban: "02", kettoTorokuBango: "2020000002" }),
        buildResult({ currentUmaban: "03", kettoTorokuBango: "2020000003" }),
        buildResult({ currentUmaban: "04", kettoTorokuBango: "2020000004" }),
        buildResult({ currentUmaban: "05", kettoTorokuBango: "2020000005" }),
        buildResult({ currentUmaban: "06", kettoTorokuBango: "2020000006" }),
        buildResult({ currentUmaban: "07", kettoTorokuBango: "2020000007" }),
        buildResult({ currentUmaban: "08", kettoTorokuBango: "2020000008" }),
        buildResult({ currentUmaban: "09", kettoTorokuBango: "2020000009" }),
        buildResult({ currentUmaban: "10", kettoTorokuBango: "2020000010" }),
        buildResult({ currentUmaban: "11", kettoTorokuBango: "2020000011" }),
        buildResult({ currentUmaban: "12", kettoTorokuBango: "2020000012" }),
        buildResult({ currentUmaban: "13", kettoTorokuBango: "2020000013" }),
        buildResult({ currentUmaban: "14", kettoTorokuBango: "2020000014" }),
        buildResult({ currentUmaban: "15", kettoTorokuBango: "2020000015" }),
        buildResult({ currentUmaban: "16", kettoTorokuBango: "2020000016" }),
        buildResult({ currentUmaban: "17", kettoTorokuBango: "2020000017" }),
        buildResult({ currentUmaban: "18", kettoTorokuBango: "2020000018" }),
        buildResult({ currentUmaban: null, kettoTorokuBango: "3000000001" }),
      ],
    });
    expect(seriesList[17]?.color).toBe("#b45309");
    expect(seriesList[18]?.color).toBe("#d62728");
  });

  it("skips rows with invalid dates when building points", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({ kaisaiNen: "2024", kaisaiTsukihi: "0229", kakuteiChakujun: "02" }),
        buildResult({ kaisaiNen: "2024", kaisaiTsukihi: "022", kakuteiChakujun: "04" }),
      ],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1709164800000, jockey: "山田", kyori: "1200", raceDate: "20240229", value: 2 },
    ]);
  });

  it("populates the frame from a matching runner wakuban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: "2020000001" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", wakuban: "3" })],
    });
    expect(seriesList[0]?.frame).toBe("3");
  });

  it("matches the runner frame on trimmed kettoTorokuBango", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: "  2020000005  " })],
      runners: [buildRunner({ kettoTorokuBango: " 2020000005 ", wakuban: "7" })],
    });
    expect(seriesList[0]?.frame).toBe("7");
  });

  it("leaves the frame null when no runner matches the horse", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: "2020000001" })],
      runners: [buildRunner({ kettoTorokuBango: "2020009999", wakuban: "5" })],
    });
    expect(seriesList[0]?.frame).toBeNull();
  });

  it("leaves the frame null when a matching runner has a blank wakuban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: "2020000001" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", wakuban: " " })],
    });
    expect(seriesList[0]?.frame).toBeNull();
  });

  it("skips runners with a blank kettoTorokuBango when resolving the frame", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kettoTorokuBango: "2020000001" })],
      runners: [
        buildRunner({ kettoTorokuBango: " ", wakuban: "8" }),
        buildRunner({ kettoTorokuBango: "2020000001", wakuban: "4" }),
      ],
    });
    expect(seriesList[0]?.frame).toBe("4");
  });

  it("appends an upcoming weight point with a null target keibajo code as decimal", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "486" })],
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("appends an upcoming weight point as the newest point", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("appends an upcoming weightDelta point with the runner sign and value", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weightDelta",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          bataiju: "486",
          zogenFugo: "-",
          zogenSa: "008",
        }),
      ],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 4 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: -8,
      },
    ]);
  });

  it("decodes the upcoming Ban-ei weight point as hexadecimal", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "3E8",
          keibajoCode: "83",
        }),
      ],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "3F2" })],
      targetKeibajoCode: "83",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 1000 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 1010,
      },
    ]);
  });

  it("omits the upcoming weight point when the runner weight is the 999 sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "999" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("omits the upcoming weight point when the runner weight is the FFF sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "FFF" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("omits the upcoming weight point when the runner weight is the 000 sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "000" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("omits the upcoming weight point when no runner matches the horse", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020009999", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("omits the upcoming weight point when the target race date is malformed", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "2026060",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("omits the upcoming weightDelta point when the runner delta is the all-zero sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weightDelta",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          bataiju: "486",
          zogenFugo: "+",
          zogenSa: "000",
        }),
      ],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 4 },
    ]);
  });

  it("appends an upcoming weight point from the realtime numeric override", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: 444, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 444,
      },
    ]);
  });

  it("appends an upcoming weightDelta point from the realtime negative override", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weightDelta",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: 444, weightDelta: -6 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 4 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: -6,
      },
    ]);
  });

  it("matches the realtime override on a normalized leading-zero umaban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "01", weight: 444, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 444,
      },
    ]);
  });

  it("falls back to the static runner weight when the override weight is null", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: null, weightDelta: null }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("falls back to the static runner weight when no override matches the umaban", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "7", weight: 444, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("drops the upcoming point when the override weight is null and the static weight is a sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "000" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: null, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("ignores an override whose umaban normalizes to the empty sentinel", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: " ", weight: 444, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("ignores the realtime override when the matched runner umaban is blank", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", bataiju: "480" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: " ", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: 444, weightDelta: 2 }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
      {
        dateValue: 1780272000000,
        isUpcoming: true,
        jockey: null,
        kyori: null,
        raceDate: "20260601",
        value: 486,
      },
    ]);
  });

  it("keeps a null upcoming weightDelta from the realtime override", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weightDelta",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: 444, weightDelta: null }],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 4 },
    ]);
  });

  it("does not append an upcoming point for the finish metric", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", kakuteiChakujun: "03" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: "山田", kyori: "1200", raceDate: "20250112", value: 3 },
    ]);
  });

  it("does not append an upcoming point for the popularity metric", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "popularity",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112", tanshoNinkijun: "02" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: "山田", kyori: "1200", raceDate: "20250112", value: 2 },
    ]);
  });

  it("carries kyori and jockey on popularity points", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "popularity",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          tanshoNinkijun: "04",
          kyori: "2000",
          kishumeiRyakusho: "佐藤",
        }),
      ],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: "佐藤", kyori: "2000", raceDate: "20250112", value: 4 },
    ]);
  });

  it("leaves kyori and jockey null on weight points", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "weight",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          kyori: "2000",
          kishumeiRyakusho: "佐藤",
        }),
      ],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 480 },
    ]);
  });

  it("sets a null kyori and jockey when the finish row has blank metadata", () => {
    const seriesList = buildHorseRaceChartSeriesList({
      metric: "finish",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          kakuteiChakujun: "03",
          kyori: " ",
          kishumeiRyakusho: null,
        }),
      ],
    });
    expect(seriesList[0]?.points).toStrictEqual([
      { dateValue: 1736640000000, jockey: null, kyori: null, raceDate: "20250112", value: 3 },
    ]);
  });

  it("returns an empty correlation list for an empty target kettoTorokuBango", () => {
    expect(
      buildHorseRaceCorrelationRows({ kettoTorokuBango: "", results: [buildResult({})] }),
    ).toStrictEqual([]);
  });

  it("returns an empty correlation list for a whitespace-only target kettoTorokuBango", () => {
    expect(
      buildHorseRaceCorrelationRows({ kettoTorokuBango: "  ", results: [buildResult({})] }),
    ).toStrictEqual([]);
  });

  it("returns an empty correlation list when no rows match the target horse", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kettoTorokuBango: "2020000002" })],
    });
    expect(rows).toStrictEqual([]);
  });

  it("builds a correlation row with all four metrics parsed", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ zogenFugo: "-", zogenSa: "008" })],
    });
    expect(rows).toStrictEqual([
      {
        dateValue: 1736640000000,
        finish: 1,
        popularity: 1,
        raceDate: "20250112",
        weight: 480,
        weightDelta: -8,
      },
    ]);
  });

  it("keeps a correlation row when all four metrics are null", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({
          bataiju: "000",
          kakuteiChakujun: "00",
          tanshoNinkijun: "00",
          zogenFugo: "+",
          zogenSa: "012",
        }),
      ],
    });
    expect(rows).toStrictEqual([
      {
        dateValue: 1736640000000,
        finish: null,
        popularity: null,
        raceDate: "20250112",
        weight: null,
        weightDelta: null,
      },
    ]);
  });

  it("drops correlation rows with malformed race dates", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" }),
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "011" }),
      ],
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20250112"]);
  });

  it("sorts correlation rows by date ascending", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0601" }),
        buildResult({ kaisaiNen: "2024", kaisaiTsukihi: "1201" }),
      ],
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20241201", "20250601"]);
  });

  it("breaks same-date correlation ties by raceBango ascending", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({ kakuteiChakujun: "05", raceBango: "11" }),
        buildResult({ kakuteiChakujun: "03", raceBango: "02" }),
      ],
    });
    expect(rows.map((row) => row.finish)).toStrictEqual([3, 5]);
  });

  it("matches the target horse on trimmed kettoTorokuBango on both sides", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: " 123 ",
      results: [buildResult({ kettoTorokuBango: "  123  " })],
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20250112"]);
  });

  it("returns a null correlation finish for the all-zero sentinel", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kakuteiChakujun: "00" })],
    });
    expect(rows[0]?.finish).toBeNull();
  });

  it("returns a null correlation weight and delta for the FFF sentinel", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ bataiju: "FFF", zogenFugo: "+", zogenSa: "004" })],
    });
    expect(rows[0]?.weight).toBeNull();
    expect(rows[0]?.weightDelta).toBeNull();
  });

  it("decodes Ban-ei correlation weight and delta as hexadecimal", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ bataiju: "3E8", keibajoCode: "83", zogenFugo: "-", zogenSa: "00B" })],
    });
    expect(rows).toStrictEqual([
      {
        dateValue: 1736640000000,
        finish: 1,
        popularity: 1,
        raceDate: "20250112",
        weight: 1000,
        weightDelta: -11,
      },
    ]);
  });

  it("appends the upcoming race as the newest correlation row", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          bataiju: "486",
          zogenFugo: "-",
          zogenSa: "008",
        }),
      ],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(rows).toStrictEqual([
      {
        dateValue: 1736640000000,
        finish: 1,
        popularity: 1,
        raceDate: "20250112",
        weight: 480,
        weightDelta: 4,
      },
      {
        dateValue: 1780272000000,
        finish: null,
        popularity: null,
        raceDate: "20260601",
        weight: 486,
        weightDelta: -8,
      },
    ]);
  });

  it("decodes the upcoming Ban-ei correlation row as hexadecimal", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "3E8",
          keibajoCode: "83",
        }),
      ],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          bataiju: "3F2",
          zogenFugo: "+",
          zogenSa: "00A",
        }),
      ],
      targetKeibajoCode: "83",
      targetRaceDate: "20260601",
    });
    expect(rows[1]).toStrictEqual({
      dateValue: 1780272000000,
      finish: null,
      popularity: null,
      raceDate: "20260601",
      weight: 1010,
      weightDelta: 10,
    });
  });

  it("omits the upcoming correlation row when no runner matches the horse", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" })],
      runners: [buildRunner({ kettoTorokuBango: "2020009999" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20250112"]);
  });

  it("omits the upcoming correlation row when the target race date is malformed", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001" })],
      targetKeibajoCode: "05",
      targetRaceDate: "bad",
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20250112"]);
  });

  it("keeps the upcoming correlation row weight when the runner delta is invalid", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" })],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          bataiju: "486",
          zogenFugo: "+",
          zogenSa: "FFF",
        }),
      ],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(rows[1]).toStrictEqual({
      dateValue: 1780272000000,
      finish: null,
      popularity: null,
      raceDate: "20260601",
      weight: 486,
      weightDelta: null,
    });
  });

  it("drops the upcoming correlation row when the static weight is a sentinel and no override matches", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" })],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "000" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
    });
    expect(rows.map((row) => row.raceDate)).toStrictEqual(["20250112"]);
  });

  it("appends the upcoming correlation row from the realtime numeric override", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [
        buildResult({
          kaisaiNen: "2025",
          kaisaiTsukihi: "0112",
          bataiju: "480",
          zogenFugo: "+",
          zogenSa: "004",
        }),
      ],
      runners: [buildRunner({ kettoTorokuBango: "2020000001", umaban: "01", bataiju: "486" })],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: 444, weightDelta: 2 }],
    });
    expect(rows).toStrictEqual([
      {
        dateValue: 1736640000000,
        finish: 1,
        popularity: 1,
        raceDate: "20250112",
        weight: 480,
        weightDelta: 4,
      },
      {
        dateValue: 1780272000000,
        finish: null,
        popularity: null,
        raceDate: "20260601",
        weight: 444,
        weightDelta: 2,
      },
    ]);
  });

  it("falls back to the static runner weight for the correlation row when the override weight is null", () => {
    const rows = buildHorseRaceCorrelationRows({
      kettoTorokuBango: "2020000001",
      results: [buildResult({ kaisaiNen: "2025", kaisaiTsukihi: "0112" })],
      runners: [
        buildRunner({
          kettoTorokuBango: "2020000001",
          umaban: "01",
          bataiju: "486",
          zogenFugo: "-",
          zogenSa: "008",
        }),
      ],
      targetKeibajoCode: "05",
      targetRaceDate: "20260601",
      upcomingWeights: [{ umaban: "1", weight: null, weightDelta: null }],
    });
    expect(rows[1]).toStrictEqual({
      dateValue: 1780272000000,
      finish: null,
      popularity: null,
      raceDate: "20260601",
      weight: 486,
      weightDelta: -8,
    });
  });

  it("exposes the chart metrics in display order", () => {
    expect(HORSE_RACE_CHART_METRICS).toStrictEqual([
      "finish",
      "popularity",
      "weight",
      "weightDelta",
    ]);
  });

  it("exposes Japanese labels for each metric", () => {
    expect(HORSE_RACE_CHART_METRIC_LABELS).toStrictEqual({
      finish: "着順",
      popularity: "人気",
      weight: "馬体重",
      weightDelta: "馬体重増減",
    });
  });

  it("exposes display units for each metric", () => {
    expect(HORSE_RACE_CHART_METRIC_UNITS).toStrictEqual({
      finish: "着",
      popularity: "番人気",
      weight: "kg",
      weightDelta: "kg",
    });
  });

  it("formats a UTC date with zero padding", () => {
    expect(formatHorseRaceChartDate(1772668800000)).toBe("2026/03/05");
  });

  it("formats a UTC date with two-digit month and day", () => {
    expect(formatHorseRaceChartDate(1763856000000)).toBe("2025/11/23");
  });
});
