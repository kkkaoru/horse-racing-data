import { cleanup, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { HorseRaceResult } from "../../../lib/race-types";
import { HorseRaceResultsTable } from "./horse-race-results-table";

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

afterEach(cleanup);

describe("horse race results table", () => {
  it("shows only fifth-place or better results by default while matching rows remain", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        defaultIncludeClass={false}
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
      />,
    );

    expect(screen.getByText("ランク内")).toBeTruthy();
    expect(screen.queryByText("ランク外")).toBeNull();
  });

  it("disables the finish rank filter when it alone removes all rows", async () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="1800"
        currentKeibajoCode="05"
        currentRaceDate="20260322"
        defaultIncludeClass={false}
        results={[
          result({
            bamei: "ランク外",
            currentUmaban: "01",
            kakuteiChakujun: "06",
          }),
        ]}
        runners={[]}
        source="jra"
      />,
    );

    await waitFor(() => {
      expect(screen.getByText("ランク外")).toBeTruthy();
    });
  });

  it("hides last 3F sort and values for ban-ei race detail pages", () => {
    render(
      <HorseRaceResultsTable
        classConditionName={null}
        currentDistance="200"
        currentKeibajoCode="83"
        currentRaceDate="20260322"
        defaultIncludeClass={false}
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
      />,
    );

    expect(screen.getByRole("button", { name: "レースタイムを降順で並び替え" })).toBeTruthy();
    expect(screen.getByText("3:18.8")).toBeTruthy();
    expect(screen.queryByRole("button", { name: /上がり3F/u })).toBeNull();
    expect(screen.queryByText("37.8")).toBeNull();
  });
});
