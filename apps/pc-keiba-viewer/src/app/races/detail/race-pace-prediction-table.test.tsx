import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import { RACE_PACE_PREDICTION_RESULTS_EVENT } from "../../../lib/race-pace-prediction";
import type { RacePacePredictionRow } from "../../../lib/race-types";
import { RacePacePredictionTable } from "./race-pace-prediction-table";

const row = (overrides: Partial<RacePacePredictionRow>): RacePacePredictionRow => ({
  confidence: 0.82,
  corner1: 1.2,
  corner2: 1.5,
  corner3: 2.1,
  corner4: 2.2,
  details: [
    {
      label: "馬自身の通過傾向",
      reason: "競走成績のコーナー通過順を評価",
      value: 1.8,
      weight: 0.7,
    },
  ],
  horseName: "テストホース",
  horseNumber: "01",
  predictedCorners: "1-2-2-2",
  ...overrides,
});

afterEach(cleanup);

describe("race pace prediction table", () => {
  it("renders corner orders and keeps horse table closed by default", () => {
    render(
      <RacePacePredictionTable
        rows={[
          row({ horseName: "先行馬", horseNumber: "01" }),
          row({
            corner1: 3.4,
            corner2: 3.2,
            corner3: 2.8,
            corner4: 2.4,
            horseName: "差し馬",
            horseNumber: "02",
            predictedCorners: "3-3-3-2",
          }),
        ]}
      />,
    );

    expect(screen.getByRole("columnheader", { name: "コーナー" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "通過順" })).toBeTruthy();
    expect(screen.getByRole("row", { name: "1コーナー 1-2" })).toBeTruthy();
    expect(screen.getByRole("row", { name: "2コーナー 1-2" })).toBeTruthy();
    expect(screen.getByRole("row", { name: "3コーナー 1-2" })).toBeTruthy();
    expect(screen.getByRole("row", { name: "4コーナー 1-2" })).toBeTruthy();
    expect(screen.getByText("馬ごとの予測").closest("details")?.hasAttribute("open")).toBe(false);
  });

  it("renders predicted corner details after opening horse table", () => {
    render(<RacePacePredictionTable rows={[row({})]} />);

    fireEvent.click(screen.getByText("馬ごとの予測"));

    expect(screen.getByRole("columnheader", { name: "コーナー通過予測" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "1C予測値" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "2C予測値" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "3C予測値" })).toBeTruthy();
    expect(screen.getByRole("columnheader", { name: "4C予測値" })).toBeTruthy();
    expect(screen.getByText("1-2-2-2")).toBeTruthy();
    expect(screen.getByText("1.2")).toBeTruthy();
    expect(screen.getByText("1.5")).toBeTruthy();
    expect(screen.getByText("2.1")).toBeTruthy();
    expect(screen.getByText("2.2")).toBeTruthy();
    expect(screen.getByText("0.82")).toBeTruthy();

    fireEvent.click(screen.getByRole("button", { name: "テストホースのレース展開予測詳細" }));

    expect(screen.getByText("馬自身の通過傾向")).toBeTruthy();
    expect(screen.getByText("競走成績のコーナー通過順を評価")).toBeTruthy();
  });

  it("updates corner order when race result filters publish recalculated rows", () => {
    render(<RacePacePredictionTable rows={[row({ horseName: "更新前", horseNumber: "01" })]} />);

    act(() => {
      window.dispatchEvent(
        new CustomEvent(RACE_PACE_PREDICTION_RESULTS_EVENT, {
          detail: {
            rows: [
              row({
                corner1: 2,
                horseName: "後方",
                horseNumber: "02",
                predictedCorners: "2-2-2-2",
              }),
              row({
                corner1: 1,
                horseName: "先頭",
                horseNumber: "03",
                predictedCorners: "1-1-1-1",
              }),
            ],
          },
        }),
      );
    });

    expect(screen.getByRole("row", { name: "1コーナー 3-2" })).toBeTruthy();
    expect(screen.queryByText("更新前")).toBeNull();
  });

  it("renders an empty state", () => {
    render(<RacePacePredictionTable rows={[]} />);

    expect(screen.getByText("レース展開予測を表示できるデータがありません。")).toBeTruthy();
  });
});
