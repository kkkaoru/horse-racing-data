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
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: "550",
  kakuteiChakujun: "00",
  kettoTorokuBango: "2023100001",
  kishumeiRyakusho: "騎手",
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
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
    expect(screen.getByRole("link", { name: "馬主" }).getAttribute("href")).toBe(
      "/owners/%E9%A6%AC%E4%B8%BB",
    );
    expect(screen.queryByText("2023100001")).toBeNull();
    expect(screen.getAllByText("-").length).toBeGreaterThanOrEqual(2);
  });

  it("sorts by runner number, odds, and finish order", () => {
    render(
      <RunnersTable
        runners={[
          runner({
            bamei: "三番",
            corner1: "04",
            corner2: "04",
            corner3: "03",
            corner4: "02",
            kakuteiChakujun: "02",
            tanshoOdds: "0050",
            umaban: "03",
          }),
          runner({
            bamei: "一番",
            corner1: "01",
            corner2: "01",
            corner3: "01",
            corner4: "01",
            kakuteiChakujun: "01",
            tanshoOdds: "0120",
            umaban: "01",
          }),
          runner({ bamei: "二番", kakuteiChakujun: "00", tanshoOdds: "0000", umaban: "02" }),
        ]}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "コーナー通過順" })).toBeTruthy();
    expect(screen.getByText("1-1-1-1")).toBeTruthy();
    expect(screen.getByText("4-4-3-2")).toBeTruthy();
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

    expect(screen.queryByRole("columnheader", { name: "コーナー通過順" })).toBeNull();
    expect(rowTexts()[0]).toContain("二番");
    expect(rowTexts()[2]).toContain("三番");
  });

  it("formats stored win odds as decimal odds", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "二番", tanshoOdds: "0046", umaban: "02" }),
          runner({ bamei: "九番", tanshoOdds: "1138", umaban: "09" }),
        ]}
      />,
    );

    expect(screen.getByText("4.6")).toBeTruthy();
    expect(screen.getByText("113.8")).toBeTruthy();
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

  it("keeps original order when sortable values are all unavailable", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "先頭維持", tanshoOdds: "abc", umaban: null }),
          runner({ bamei: "二番目維持", tanshoOdds: null, umaban: "" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("先頭維持");
    expect(rowTexts()[1]).toContain("二番目維持");
  });

  it("renders plain text when horse and person link keys are unavailable", () => {
    render(
      <RunnersTable
        runners={[
          runner({
            banushimei: "-",
            bamei: "",
            chokyoshimeiRyakusho: "",
            kettoTorokuBango: "",
            kishumeiRyakusho: "-",
            umaban: "01",
          }),
        ]}
      />,
    );

    expect(screen.queryByRole("link")).toBeNull();
    expect(screen.getAllByText("-").length).toBeGreaterThanOrEqual(3);
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
          raceEntries: null,
          raceResults: null,
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

  it("uses realtime race entries for changed jockeys and status", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: null,
          odds: null,
          raceEntries: {
            fetchedAt: "2026-05-10T18:30:00+09:00",
            horses: [
              {
                fetchedAt: "2026-05-10T18:30:00+09:00",
                horseName: "一番",
                horseNumber: "1",
                jockeyName: "替騎手",
                status: "取消",
              },
            ],
          },
          raceResults: null,
          raceKey: "nar:2026:0510:83:09",
          source: null,
        }}
        runners={[runner({ bamei: "一番", kishumeiRyakusho: "元騎手", umaban: "01" })]}
      />,
    );

    expect(screen.getByText("替騎手")).toBeTruthy();
    expect(screen.getByText("元 元騎手")).toBeTruthy();
    expect(screen.getAllByText("取消").length).toBeGreaterThan(0);
  });

  it("does not show changed jockey notes for names with the same first three characters", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: null,
          odds: null,
          raceEntries: {
            fetchedAt: "2026-05-10T18:30:00+09:00",
            horses: [
              {
                fetchedAt: "2026-05-10T18:30:00+09:00",
                horseName: "一番",
                horseNumber: "1",
                jockeyName: "増田充宏",
                status: null,
              },
              {
                fetchedAt: "2026-05-10T18:30:00+09:00",
                horseName: "二番",
                horseNumber: "2",
                jockeyName: "シャベス",
                status: null,
              },
            ],
          },
          raceResults: null,
          raceKey: "nar:2026:0510:83:09",
          source: null,
        }}
        runners={[
          runner({ bamei: "一番", kishumeiRyakusho: "増田充", umaban: "01" }),
          runner({ bamei: "二番", kishumeiRyakusho: "シャベ", umaban: "02" }),
        ]}
      />,
    );

    expect(screen.getByText("増田充")).toBeTruthy();
    expect(screen.getByText("シャベ")).toBeTruthy();
    expect(screen.queryByText("増田充宏")).toBeNull();
    expect(screen.queryByText("シャベス")).toBeNull();
    expect(screen.queryByText("元 増田充")).toBeNull();
    expect(screen.queryByText("元 シャベ")).toBeNull();
  });

  it("does not show changed jockey notes for whitespace-only name differences", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: null,
          odds: null,
          raceEntries: {
            fetchedAt: "2026-05-10T18:30:00+09:00",
            horses: [
              {
                fetchedAt: "2026-05-10T18:30:00+09:00",
                horseName: "一番",
                horseNumber: "1",
                jockeyName: "坂井 瑠星",
                status: null,
              },
            ],
          },
          raceResults: null,
          raceKey: "jra:2026:0510:08:01",
          source: null,
        }}
        runners={[runner({ bamei: "一番", kishumeiRyakusho: "坂井瑠星", umaban: "01" })]}
      />,
    );

    expect(screen.getByText("坂井瑠星")).toBeTruthy();
    expect(screen.queryByText("元 坂井瑠星")).toBeNull();
  });

  it("formats realtime horse weights with missing values", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: {
            fetchedAt: "2026-05-10T18:40:00+09:00",
            horses: [
              {
                changeAmount: null,
                changeSign: null,
                horseName: "一番",
                horseNumber: "1",
                weight: null,
              },
            ],
          },
          odds: {
            fetchedAt: "2026-05-10T18:40:00+09:00",
            history: [],
            horseTrends: [],
            latest: {
              tansho: [{ combination: "1" }],
            },
          },
          raceEntries: null,
          raceResults: null,
          raceKey: "nar:2026:0510:83:09",
          source: null,
        }}
        runners={[runner({ bamei: "一番", umaban: "01" })]}
      />,
    );

    expect(screen.getAllByText("-").length).toBeGreaterThanOrEqual(2);
  });

  it("uses realtime race results for finish order display and default sort", () => {
    render(
      <RunnersTable
        initialRealtimePayload={{
          horseWeights: null,
          odds: null,
          raceEntries: null,
          raceResults: {
            fetchedAt: "2026-05-10T18:50:00+09:00",
            horses: [
              {
                fetchedAt: "2026-05-10T18:50:00+09:00",
                finishPosition: "02",
                horseName: "一番",
                horseNumber: "1",
                time: "1:54.3",
              },
              {
                fetchedAt: "2026-05-10T18:50:00+09:00",
                finishPosition: "01",
                horseName: "二番",
                horseNumber: "2",
                time: "1:53.9",
              },
            ],
          },
          raceKey: "nar:2026:0510:83:09",
          source: null,
        }}
        runners={[
          runner({ bamei: "一番", kakuteiChakujun: "00", umaban: "01" }),
          runner({ bamei: "二番", kakuteiChakujun: "00", umaban: "02" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("二番");
    expect(rowTexts()[0]).toContain("1");
    expect(rowTexts()[1]).toContain("一番");
    expect(rowTexts()[1]).toContain("2");
  });

  it("shows the D1 finish position when PG kakuteiChakujun is empty and no realtime result exists", () => {
    render(
      <RunnersTable
        d1FinishPositions={[{ finishPosition: "3", horseNumber: "01" }]}
        runners={[runner({ bamei: "一番", kakuteiChakujun: "00", umaban: "01" })]}
      />,
    );

    expect(screen.getByText("3")).toBeTruthy();
  });

  it("prefers the realtime finish position over the D1 finish position", () => {
    render(
      <RunnersTable
        d1FinishPositions={[{ finishPosition: "7", horseNumber: "12" }]}
        initialRealtimePayload={{
          horseWeights: null,
          odds: null,
          raceEntries: null,
          raceResults: {
            fetchedAt: "2026-06-19T18:50:00+09:00",
            horses: [
              {
                fetchedAt: "2026-06-19T18:50:00+09:00",
                finishPosition: "1",
                horseName: "十二番",
                horseNumber: "12",
                time: "1:54.3",
              },
            ],
          },
          raceKey: "nar:2026:0619:45:11",
          source: null,
        }}
        runners={[runner({ bamei: "十二番", kakuteiChakujun: "00", umaban: "12", wakuban: "6" })]}
      />,
    );

    expect(screen.getByText("1")).toBeTruthy();
    expect(screen.queryByText("7")).toBeNull();
  });

  it("falls back to the PG finish position when the D1 finish is the all-zero placeholder", () => {
    render(
      <RunnersTable
        d1FinishPositions={[{ finishPosition: "0", horseNumber: "12" }]}
        runners={[runner({ bamei: "十二番", kakuteiChakujun: "9", umaban: "12", wakuban: "6" })]}
      />,
    );

    expect(screen.getByText("9")).toBeTruthy();
  });

  it("sorts by the D1 finish position when PG kakuteiChakujun is empty", () => {
    render(
      <RunnersTable
        d1FinishPositions={[
          { finishPosition: "2", horseNumber: "01" },
          { finishPosition: "1", horseNumber: "02" },
        ]}
        runners={[
          runner({ bamei: "一番", kakuteiChakujun: "00", umaban: "01" }),
          runner({ bamei: "二番", kakuteiChakujun: "00", umaban: "02" }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("二番");
    expect(rowTexts()[1]).toContain("一番");
  });

  it("renders the blinker column with maru when blinkerShiyoKubun is 1 and dash otherwise", () => {
    render(
      <RunnersTable
        runners={[
          runner({ bamei: "着用馬", blinkerShiyoKubun: "1", umaban: "01" }),
          runner({ bamei: "未着用馬", blinkerShiyoKubun: "0", umaban: "02" }),
        ]}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "ブリンカー" })).toBeTruthy();
    expect(screen.getByText("○")).toBeTruthy();
    expect(rowTexts()[0]).toContain("着用馬");
    expect(rowTexts()[0]).toContain("○");
    expect(rowTexts()[1]).toContain("未着用馬");
  });

  it("renders the blinker pattern badge with its compact Japanese label for horses present in blinkerPatterns", () => {
    render(
      <RunnersTable
        blinkerPatterns={[{ kettoTorokuBango: "2023100001", pattern: "A" }]}
        runners={[
          runner({ bamei: "初装着馬", kettoTorokuBango: "2023100001", umaban: "01" }),
          runner({ bamei: "対象外馬", kettoTorokuBango: "2023100002", umaban: "02" }),
        ]}
      />,
    );

    const badge = screen.getByTitle("初ブリンカー");
    expect(badge.textContent).toBe("初装着");
    expect(badge.className).toBe("runner-blinker-pattern-badge pattern-A");
    expect(rowTexts()[1]).toContain("対象外馬");
    expect(
      screen.getByText("対象外馬").closest("tr")?.querySelector(".runner-blinker-pattern-badge"),
    ).toBeNull();
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
