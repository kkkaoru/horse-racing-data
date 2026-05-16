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
  it("shows only rows with premium grade by default when grade data exists", () => {
    render(
      <TrainingTable
        sourceLabel="JRA"
        trainings={[
          training({ bamei: "記号あり", premiumEvaluationGrade: "A", umaban: "01" }),
          training({ bamei: "記号なし", premiumEvaluationGrade: null, umaban: "02" }),
        ]}
      />,
    );

    const gradeOnlyCheckbox = screen.getByRole("checkbox", { name: "記号ありのみを表示" });
    expect(gradeOnlyCheckbox).toBeInstanceOf(HTMLInputElement);
    expect(gradeOnlyCheckbox instanceof HTMLInputElement && gradeOnlyCheckbox.checked).toBe(true);
    expect(screen.getByText("記号あり")).toBeTruthy();
    expect(screen.queryByText("記号なし")).toBeNull();

    fireEvent.click(screen.getByRole("checkbox", { name: "記号ありのみを表示" }));
    expect(screen.getByText("記号なし")).toBeTruthy();
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
});
