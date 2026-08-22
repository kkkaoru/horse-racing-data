import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { HorseRaceResult, RaceTimeStats, Runner } from "../../../lib/race-types";
import { HorseRaceResultsTable } from "./horse-race-results-table";

vi.mock("next/navigation", () => ({
  usePathname: () => "/races/2026/03/22/05/01",
  useRouter: () => ({ replace: vi.fn<() => void>() }),
  useSearchParams: () => new URLSearchParams(),
}));

const result = (overrides: Partial<HorseRaceResult>): HorseRaceResult => ({
  babajotaiCodeDirt: "1",
  babajotaiCodeShiba: "0",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  bamei: "テストホース",
  chokyoshimeiRyakusho: "調教師",
  currentBarei: "04",
  currentJockey: "騎手",
  currentSeibetsuCode: "1",
  currentUmaban: "01",
  corner1: "03",
  corner2: "04",
  corner3: "05",
  corner4: "06",
  futanJuryo: "550",
  gradeCode: "00",
  hassoJikoku: "1200",
  jockeyName: "騎手",
  kakuteiChakujun: "01",
  kaisaiNen: "2026",
  kaisaiTsukihi: "0322",
  keibajoCode: "05",
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  kohan3f: "378",
  kyori: "1800",
  kyosoJokenCode: "005",
  kyosoJokenMeisho: "3歳",
  kyosoKigoCode: "000",
  kyosoShubetsuCode: "12",
  kyosomeiFukudai: null,
  kyosomeiHondai: "テストレース",
  kyosomeiKakkonai: null,
  raceBango: "01",
  seibetsuCode: "1",
  sohaTime: "1123",
  tanshoNinkijun: "01",
  tanshoOdds: "012",
  tenkoCode: "1",
  timeSa: null,
  trackCode: "24",
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "0",
  juryoShubetsuCode: "1",
  ...overrides,
});

const runner = (overrides: Partial<Runner>): Runner => ({
  bamei: "出走馬",
  banushimei: "馬主",
  barei: "04",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  damSireName: null,
  futanJuryo: "550",
  kakuteiChakujun: null,
  kettoTorokuBango: "2022100001",
  kishumeiRyakusho: "騎手",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: null,
  tanshoOdds: null,
  timeSa: null,
  umaban: "01",
  wakuban: "1",
  zogenFugo: null,
  zogenSa: null,
  ...overrides,
});

afterEach(cleanup);

describe("horse race results table", () => {
  it("shows only fifth-place or better results by default while matching rows remain", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({ bamei: "ランク内", currentUmaban: "01", kakuteiChakujun: "05" }),
          result({
            bamei: "ランク外",
            currentUmaban: "02",
            kakuteiChakujun: "06",
            kettoTorokuBango: "2022100002",
            umaban: "02",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByText("ランク内")).toBeTruthy();
    expect(screen.getByText("3-4-5-6")).toBeTruthy();
    expect(screen.queryByText("ランク外")).toBeNull();
  });

  it("disables the finish rank filter when it alone removes all rows", async () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "ランク外",
            currentUmaban: "01",
            kakuteiChakujun: "06",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("ランク外")).toBeTruthy();
    });
  });

  it("relaxes the default finish rank filter so every runner with history is visible", async () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({ bamei: "ランク内", currentUmaban: "01", kakuteiChakujun: "05" }),
          result({
            bamei: "ランク外",
            currentUmaban: "02",
            kakuteiChakujun: "06",
            kettoTorokuBango: "2022100002",
            umaban: "02",
          }),
        ]}
        runners={[
          runner({ bamei: "ランク内", kettoTorokuBango: "2022100001", umaban: "01" }),
          runner({ bamei: "ランク外", kettoTorokuBango: "2022100002", umaban: "02" }),
        ]}
        source="jra"
        sourceScope="all"
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("ランク外")).toBeTruthy();
    });
  });

  it("relaxes default nar filters so every non-debut runner with history is visible", async () => {
    render(
      <HorseRaceResultsTable
        classConditionName="C2"
        currentDistance="900"
        currentKeibajoCode="45"
        currentRaceDate="20260512"
        currentTrackCode="24"
        defaultIncludeClass={true}
        raceTimeStats={null}
        results={[
          result({
            bamei: "履歴あり",
            currentJockey: "予定騎手",
            currentUmaban: "01",
            kakuteiChakujun: "09",
            kaisaiNen: "2024",
            kaisaiTsukihi: "0101",
            keibajoCode: "43",
            kishumeiRyakusho: "過去騎手",
            kyori: "1200",
            kyosoJokenMeisho: "B1",
          }),
        ]}
        runners={[runner({ bamei: "履歴あり", kettoTorokuBango: "2022100001", umaban: "01" })]}
        source="nar"
        sourceScope="all"
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("履歴あり")).toBeTruthy();
    });
    await waitFor(() => {
      expect(screen.getByLabelText("表示期間（直近◯ヶ月）")).toHaveProperty("value", "29");
    });
    expect(screen.queryByText("条件に一致する競走成績はありません。")).toBeNull();
  });

  it("uses the supplemental horse name for a debut runner", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1600"
        currentKeibajoCode="A8"
        currentRaceDate="20260816"
        currentTrackCode="17"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[]}
        runners={[runner({ bamei: "補助前馬名", horseNameFull: "FULL HORSE NAME" })]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByText("FULL HORSE NAME")).toBeTruthy();
    expect(screen.queryByText("補助前馬名")).toBeNull();
  });

  it("defaults recent months to 7", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[result({ bamei: "対象", currentUmaban: "01" })]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByLabelText("表示期間（直近◯ヶ月）")).toHaveProperty("value", "7");
  });

  it("keeps a manually entered finish rank limit even when it filters out rows", async () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "ランク外",
            currentUmaban: "01",
            kakuteiChakujun: "06",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("ランク外")).toBeTruthy();
    });

    const finishRankInput = screen.getByLabelText("着順で絞り込む（◯着以内）");
    if (!(finishRankInput instanceof HTMLInputElement)) {
      throw new TypeError("finish rank control is not an input");
    }
    fireEvent.change(finishRankInput, { target: { value: "2" } });

    expect(finishRankInput.value).toBe("2");
    expect(screen.queryByText("ランク外")).toBeNull();
  });

  it("steps the recent months input by two", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[result({ bamei: "対象", currentUmaban: "01" })]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByLabelText("表示期間（直近◯ヶ月）")).toHaveProperty("step", "2");
  });

  it("renders the clearer filter labels", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[result({ bamei: "対象", currentUmaban: "01" })]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByText("着順で絞り込む（◯着以内）")).toBeTruthy();
    expect(screen.getByText("表示期間（直近◯ヶ月）")).toBeTruthy();
  });

  it("excludes rows without a finish from the main table but keeps them in the detail table", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "01",
            kaisaiTsukihi: "0301",
            kyosomeiHondai: "確定レース",
            raceBango: "05",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "00",
            kaisaiTsukihi: "0201",
            kyosomeiHondai: "未確定レース",
            raceBango: "06",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    expect(screen.getByText("確定レース")).toBeTruthy();
    expect(screen.queryByText("未確定レース")).toBeNull();

    fireEvent.click(screen.getByRole("button", { name: "詳細" }));

    expect(screen.getByText("未確定レース")).toBeTruthy();
  });

  it("orders same-distance clocks first, then other trips by pace", () => {
    const { container } = render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "01",
            kaisaiTsukihi: "0301",
            kyori: "1900",
            kyosomeiHondai: "速い1900",
            raceBango: "05",
            sohaTime: "1100",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "02",
            kaisaiTsukihi: "0201",
            kyori: "1800",
            kyosomeiHondai: "同距離1800",
            raceBango: "06",
            sohaTime: "1200",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "03",
            kaisaiTsukihi: "0101",
            kyori: "1700",
            kyosomeiHondai: "短い1700",
            raceBango: "07",
            sohaTime: "1000",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    fireEvent.change(screen.getByLabelText("馬ごとの表示数"), { target: { value: "all" } });

    const raceNameCells = [...container.querySelectorAll("tbody tr td.race-results-name-cell")].map(
      (cell) => cell.textContent,
    );

    expect(raceNameCells).toStrictEqual(["同距離1800", "速い1900", "短い1700"]);
  });

  it("prefers the longer trip when two races are equally far from the current distance", () => {
    const { container } = render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "01",
            kaisaiTsukihi: "0101",
            kyori: "1700",
            kyosomeiHondai: "ペースの良い1700",
            raceBango: "05",
            sohaTime: "1000",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "02",
            kaisaiTsukihi: "0201",
            kyori: "1900",
            kyosomeiHondai: "長い1900",
            raceBango: "06",
            sohaTime: "1300",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    fireEvent.change(screen.getByLabelText("馬ごとの表示数"), { target: { value: "all" } });

    const raceNameCells = [...container.querySelectorAll("tbody tr td.race-results-name-cell")].map(
      (cell) => cell.textContent,
    );

    expect(raceNameCells).toStrictEqual(["長い1900", "ペースの良い1700"]);
  });

  it("keeps the initial row order after returning to race-time ascending", () => {
    const { container } = render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "01",
            kaisaiTsukihi: "0301",
            kyori: "1900",
            kyosomeiHondai: "速い1900",
            raceBango: "05",
            sohaTime: "1100",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "02",
            kaisaiTsukihi: "0201",
            kyori: "1800",
            kyosomeiHondai: "同距離1800",
            raceBango: "06",
            sohaTime: "1200",
          }),
          result({
            bamei: "対象馬",
            currentUmaban: "01",
            kakuteiChakujun: "03",
            kaisaiTsukihi: "0101",
            kyori: "1700",
            kyosomeiHondai: "短い1700",
            raceBango: "07",
            sohaTime: "1000",
          }),
        ]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );

    fireEvent.change(screen.getByLabelText("馬ごとの表示数"), { target: { value: "all" } });
    const initialRaceNameCells = [
      ...container.querySelectorAll("tbody tr td.race-results-name-cell"),
    ].map((cell) => cell.textContent);

    fireEvent.click(screen.getByRole("button", { name: "日付を昇順で並び替え" }));
    fireEvent.click(screen.getByRole("button", { name: "レースタイムを昇順で並び替え" }));

    const restoredRaceNameCells = [
      ...container.querySelectorAll("tbody tr td.race-results-name-cell"),
    ].map((cell) => cell.textContent);

    expect(initialRaceNameCells).toStrictEqual(["同距離1800", "速い1900", "短い1700"]);
    expect(restoredRaceNameCells).toStrictEqual(["同距離1800", "速い1900", "短い1700"]);
  });

  it("sorts ban-ei clocks by decoded minutes instead of the packed numeric string", () => {
    const { container } = render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="200"
        currentKeibajoCode="83"
        currentRaceDate="20260322"
        currentTrackCode="90"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            currentUmaban: "01",
            keibajoCode: "83",
            kyori: "200",
            kyosomeiHondai: "遅いばんえい",
            raceBango: "01",
            sohaTime: "1999",
            source: "nar",
            trackCode: "90",
          }),
          result({
            currentUmaban: "02",
            keibajoCode: "83",
            kettoTorokuBango: "2022100002",
            kyori: "200",
            kyosomeiHondai: "速いばんえい",
            raceBango: "02",
            sohaTime: "2000",
            umaban: "02",
            source: "nar",
            trackCode: "90",
          }),
        ]}
        runners={[]}
        source="nar"
        sourceScope="all"
      />,
    );

    fireEvent.change(screen.getByLabelText("馬ごとの表示数"), { target: { value: "all" } });

    const raceNameCells = [...container.querySelectorAll("tbody tr td.race-results-name-cell")].map(
      (cell) => cell.textContent,
    );

    expect(raceNameCells).toStrictEqual(["速いばんえい", "遅いばんえい"]);
  });

  it("hides last 3F sort and values for ban-ei race detail pages", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="200"
        currentKeibajoCode="83"
        currentRaceDate="20260322"
        currentTrackCode="90"
        defaultIncludeClass={false}
        raceTimeStats={null}
        results={[
          result({
            keibajoCode: "83",
            kohan3f: "378",
            kyori: "200",
            sohaTime: "3188",
            source: "nar",
            trackCode: "90",
          }),
        ]}
        runners={[]}
        source="nar"
        sourceScope="all"
      />,
    );

    expect(screen.getByRole("button", { name: "レースタイムを降順で並び替え" })).toBeTruthy();
    expect(screen.getByText("3:18.8")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /上がり3F/u })).toBeNull();
    expect(screen.queryByText("37.8")).toBeNull();
  });

  it("renders compact time-trend metrics above the per-horse results table", () => {
    const raceTimeStats: RaceTimeStats = {
      averageKohan3f: 345,
      averageRaceTime: 965,
      correlationRows: [],
      fastestDetail: null,
      fastestKohan3f: 332,
      fastestRaceTime: 725,
      medianKohan3f: 341,
      medianRaceTime: 960,
      raceCount: 12,
      targetRaces: [],
    };
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        currentTrackCode="24"
        defaultIncludeClass={false}
        raceTimeStats={raceTimeStats}
        results={[result({ bamei: "対象", currentUmaban: "01" })]}
        runners={[]}
        source="jra"
        sourceScope="all"
      />,
    );
    const metrics = screen.getByLabelText("タイム傾向");
    const table = screen.getByRole("table");
    expect(metrics.compareDocumentPosition(table)).toStrictEqual(4);
    expect(screen.getByText("最速レースタイム")).toBeDefined();
    expect(screen.getByText("1:12.5")).toBeDefined();
    expect(screen.getByText("最速上がり3F")).toBeDefined();
  });
});
