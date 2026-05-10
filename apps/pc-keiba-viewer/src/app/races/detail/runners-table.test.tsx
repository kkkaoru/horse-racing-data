import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { Runner } from "../../../lib/race-types";
import { RunnersTable } from "./runners-table";

const runner = (overrides: Partial<Runner>): Runner => ({
  barei: "03",
  banushimei: "馬主",
  bamei: "テストホース",
  bataiju: "480",
  chokyoshimeiRyakusho: "調教師",
  futanJuryo: "550",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "騎手",
  seibetsuCode: "1",
  sohaTime: null,
  timeSa: null,
  kohan3f: null,
  tanshoNinkijun: null,
  tanshoOdds: "0000",
  umaban: "01",
  wakuban: "1",
  zogenFugo: "+",
  zogenSa: "0",
  ...overrides,
});

const rowTexts = (): string[] =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => row.textContent ?? "");

afterEach(cleanup);

describe("runners table", () => {
  it("renders runner values in readable format", () => {
    render(<RunnersTable runners={[runner({ bamei: "牝馬", seibetsuCode: "2", umaban: "01" })]} />);

    expect(screen.getAllByText("1")).toHaveLength(2);
    expect(screen.getByText("牝 / 3歳")).toBeTruthy();
    expect(screen.getAllByText("馬主")).toHaveLength(2);
    expect(screen.queryByText("2023100001")).toBeNull();
    expect(screen.getAllByText("-").length).toBeGreaterThanOrEqual(2);
  });

  it("sorts by runner number, odds, and finish order", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "三番", kakuteiChakujun: "02", tanshoOdds: "0050", umaban: "03" }),
          runner({ bamei: "一番", kakuteiChakujun: "01", tanshoOdds: "0120", umaban: "01" }),
          runner({ bamei: "二番", kakuteiChakujun: "00", tanshoOdds: "0000", umaban: "02" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("一番");

    fireEvent.click(screen.getByRole("button", { name: "馬番号を昇順で並び替え" }));
    fireEvent.click(screen.getByRole("button", { name: "馬番号を降順で並び替え" }));
    expect(rowTexts()[0]).toContain("三番");

    fireEvent.click(screen.getByRole("button", { name: "単勝を昇順で並び替え" }));
    expect(rowTexts()[0]).toContain("三番");
    expect(rowTexts()[2]).toContain("二番");

    fireEvent.click(screen.getByRole("button", { name: "着順を昇順で並び替え" }));
    expect(rowTexts()[0]).toContain("一番");
    expect(rowTexts()[2]).toContain("二番");
  });

  it("uses odds as the default sort when finish order is empty", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "一番", tanshoOdds: "0120", umaban: "01" }),
          runner({ bamei: "二番", tanshoOdds: "0050", umaban: "02" }),
          runner({ bamei: "三番", tanshoOdds: "0000", umaban: "03" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("二番");
    expect(rowTexts()[2]).toContain("三番");
  });

  it("uses runner number as the default sort when finish order and odds are empty", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "三番", umaban: "03" }),
          runner({ bamei: "一番", umaban: "01" }),
          runner({ bamei: "二番", umaban: "02" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("一番");
    expect(rowTexts()[2]).toContain("三番");
  });

  it("uses realtime odds and horse weights when available", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: {
            fetchedAt: "2026-05-10T18:40:00+09:00",
            horses: [
              {
                changeAmount: 8,
                changeSign: "+",
                horseName: "一番",
                horseNumber: "1",
                weight: 512,
              },
            ],
          },
          odds: {
            fetchedAt: "2026-05-10T18:40:00+09:00",
            history: [],
            horseTrends: [],
            latest: {
              tansho: [
                { combination: "1", odds: 9.8, rank: 2 },
                { combination: "2", odds: 1.4, rank: 1 },
              ],
            },
          },
          raceKey: "nar:2026:0510:83:09",
          source: null,
        }}
        runners={[
          runner({ bamei: "一番", tanshoOdds: "9999", umaban: "01" }),
          runner({ bamei: "二番", tanshoOdds: "9999", umaban: "02" }),
        ]}
      />,
    );

    expect(screen.getByText("512kg (+8)")).toBeTruthy();
    expect(screen.getByText("9.8")).toBeTruthy();
    expect(screen.getByText("1.4")).toBeTruthy();

    expect(rowTexts()[0]).toContain("二番");
  });

  it("decodes ban-ei hexadecimal horse weights", () => {
    render(
      <RunnersTable
        decodeHexHorseWeight
        runners={[
          runner({
            bataiju: "4AE",
            bamei: "ばんえい馬",
            futanJuryo: "26C",
            umaban: "01",
            zogenSa: "008",
          }),
        ]}
      />,
    );

    expect(screen.getByText("1198kg (+8)")).toBeTruthy();
    expect(screen.getByText("620")).toBeTruthy();
  });
});
