import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { Training } from "../../../lib/race-types";
import { TrainingTable } from "./training-table";

const training = (overrides: Partial<Training>): Training => ({
  babamawari: "1",
  bamei: "テストホース",
  chokyoJikoku: "0600",
  chokyoNengappi: "20260516",
  course: "1",
  currentJockeyName: "騎手",
  lapTime10f: null,
  lapTime1f: "120",
  lapTime2f: null,
  lapTime3f: null,
  lapTime4f: null,
  lapTime5f: null,
  lapTime6f: null,
  lapTime7f: null,
  lapTime8f: null,
  lapTime9f: null,
  premiumEvaluationGrade: null,
  premiumEvaluationText: null,
  timeGokei10f: null,
  timeGokei2f: "240",
  timeGokei3f: "360",
  timeGokei4f: "480",
  timeGokei5f: null,
  timeGokei6f: null,
  timeGokei7f: null,
  timeGokei8f: null,
  timeGokei9f: null,
  tracenKubun: "1",
  trainerName: "調教師",
  trainingRiderName: "騎乗者",
  trainingType: "追切",
  umaban: "01",
  ...overrides,
});

afterEach(cleanup);

describe("training table", () => {
  it("shows only rows with premium grade by default including undated placeholders", () => {
    render(
      <TrainingTable
        sourceLabel="JRA"
        trainings={[
          training({ bamei: "記号あり", premiumEvaluationGrade: "A", umaban: "01" }),
          training({ bamei: "記号なし", premiumEvaluationGrade: null, umaban: "02" }),
          training({
            bamei: "公式調教なしだが記号あり",
            chokyoJikoku: "",
            chokyoNengappi: "",
            premiumEvaluationGrade: "B",
            trainingType: "-",
            umaban: "03",
          }),
        ]}
      />,
    );

    const gradeOnlyCheckbox = screen.getByRole("checkbox", { name: "記号ありのみを表示" });
    expect(gradeOnlyCheckbox).toBeInstanceOf(HTMLInputElement);
    expect(gradeOnlyCheckbox instanceof HTMLInputElement && gradeOnlyCheckbox.checked).toBe(true);
    expect(screen.getByText("記号あり")).toBeTruthy();
    expect(screen.getByText("公式調教なしだが記号あり")).toBeTruthy();
    expect(screen.queryByText("記号なし")).toBeNull();
    expect(screen.getByText("2 / 3 件")).toBeTruthy();

    fireEvent.click(screen.getByRole("checkbox", { name: "記号ありのみを表示" }));
    expect(screen.getByText("記号なし")).toBeTruthy();
    expect(screen.getByText("3 / 3 件")).toBeTruthy();
  });

  it("selects the best premium grade before the fastest 1F record when filtering graded rows", () => {
    render(
      <TrainingTable
        sourceLabel="JRA"
        trainings={[
          training({
            bamei: "同じ馬",
            chokyoJikoku: "0600",
            lapTime1f: "110",
            premiumEvaluationGrade: "2",
            premiumEvaluationText: "速いが記号は下",
            umaban: "01",
          }),
          training({
            bamei: "同じ馬",
            chokyoJikoku: "0610",
            lapTime1f: "124",
            premiumEvaluationGrade: "1",
            premiumEvaluationText: "良い記号",
            umaban: "01",
          }),
          training({
            bamei: "同じ馬",
            chokyoJikoku: "0620",
            lapTime1f: "120",
            premiumEvaluationGrade: "1",
            premiumEvaluationText: "良い記号で速い1F",
            umaban: "01",
          }),
        ]}
      />,
    );

    expect(screen.getByText("良い記号で速い1F")).toBeTruthy();
    expect(screen.queryByText("良い記号")).toBeNull();
    expect(screen.queryByText("速いが記号は下")).toBeNull();
  });

  it("shows a dash instead of a malformed date for entrants with no official workout data", () => {
    render(
      <TrainingTable
        sourceLabel="JRA"
        trainings={[
          training({
            bamei: "調教データなし馬",
            chokyoJikoku: "",
            chokyoNengappi: "",
            trainingType: "-",
            umaban: "05",
          }),
        ]}
      />,
    );

    expect(screen.getByText("調教データなし馬")).toBeTruthy();
    expect(screen.getByText("-")).toBeTruthy();
  });

  it("shows netkeiba fallback workout times together with its symbol", () => {
    render(
      <TrainingTable
        sourceLabel="JRA"
        trainings={[
          training({
            bamei: "現地調教馬",
            course: "札幌ダート",
            lapTime1f: "123",
            premiumEvaluationGrade: "S",
            premiumEvaluationText: "抜群",
            premiumWorkoutIndex: 0,
            timeGokei4f: "498",
            tracenKubun: "札幌",
            trainingDataSource: "netkeiba",
            trainingType: "ダート",
            umaban: "05",
          }),
        ]}
      />,
    );

    expect(screen.getByText("現地調教馬")).toBeTruthy();
    expect(screen.getByText("49.8")).toBeTruthy();
    expect(screen.getByText("12.3")).toBeTruthy();
    expect(screen.getByText("抜群")).toBeTruthy();
    expect(screen.getByText("S")).toBeTruthy();
  });
});
