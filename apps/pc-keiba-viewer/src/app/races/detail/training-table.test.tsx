// This file runs with bun.

import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { Training } from "../../../lib/race-types";
import {
  loadTrainingChartForCurrentUser,
  loadTrainingScatterAllWorkoutsForCurrentUser,
  persistTrainingChartForCurrentUser,
  persistTrainingScatterAllWorkoutsForCurrentUser,
} from "../../../lib/user-preferences-indexeddb";
import { TrainingTable } from "./training-table";

vi.mock("../../../lib/user-preferences-indexeddb", () => ({
  loadTrainingChartForCurrentUser: vi.fn<() => Promise<boolean>>(async () => true),
  loadTrainingScatterAllWorkoutsForCurrentUser: vi.fn<() => Promise<boolean>>(async () => false),
  persistTrainingChartForCurrentUser: vi.fn<(showChart: boolean) => Promise<void>>(
    async () => undefined,
  ),
  persistTrainingScatterAllWorkoutsForCurrentUser: vi.fn<
    (showAllWorkouts: boolean) => Promise<void>
  >(async () => undefined),
}));

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

afterEach(() => {
  cleanup();
  vi.mocked(loadTrainingChartForCurrentUser).mockReset();
  vi.mocked(loadTrainingScatterAllWorkoutsForCurrentUser).mockReset();
  vi.mocked(persistTrainingChartForCurrentUser).mockReset();
  vi.mocked(persistTrainingScatterAllWorkoutsForCurrentUser).mockReset();
  vi.mocked(loadTrainingChartForCurrentUser).mockResolvedValue(true);
  vi.mocked(loadTrainingScatterAllWorkoutsForCurrentUser).mockResolvedValue(false);
  vi.mocked(persistTrainingChartForCurrentUser).mockResolvedValue(undefined);
  vi.mocked(persistTrainingScatterAllWorkoutsForCurrentUser).mockResolvedValue(undefined);
});

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

    fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
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

    fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
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

    fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
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

    fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
    expect(screen.getByText("現地調教馬")).toBeTruthy();
    expect(screen.getByText("49.8")).toBeTruthy();
    expect(screen.getByText("12.3")).toBeTruthy();
    expect(screen.getByText("抜群")).toBeTruthy();
    expect(screen.getByText("S")).toBeTruthy();
  });
});

it("shows the training chart by default instead of the table", () => {
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "グラフ馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  const filters = screen.getByLabelText("training filters");
  const toggle = screen.getByRole("group", { name: "調教追い切りの表示" });
  const chart = screen.getByRole("figure", { name: "調教追い切り散布図" });
  expect(filters.compareDocumentPosition(toggle)).toStrictEqual(4);
  expect(toggle.compareDocumentPosition(chart)).toStrictEqual(4);
  expect(screen.queryByRole("columnheader", { name: "馬名" })).toBeNull();
  expect(screen.getByRole("radio", { name: "グラフ" })).toHaveProperty("checked", true);
  expect(screen.getByRole("radio", { name: "テキスト" })).toHaveProperty("checked", false);
});

it("hides the chart and persists graph-off when テキスト is selected", () => {
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "テキスト馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
  expect(screen.getByRole("columnheader", { name: "馬名" })).toBeDefined();
  expect(screen.getByRole("radio", { name: "グラフ" })).toHaveProperty("checked", false);
  expect(screen.getByRole("radio", { name: "テキスト" })).toHaveProperty("checked", true);
  expect(vi.mocked(persistTrainingChartForCurrentUser).mock.calls).toStrictEqual([[false]]);
});

it("keeps the training chart on when preference load fails", async () => {
  vi.mocked(loadTrainingChartForCurrentUser).mockRejectedValue(new Error("idb unavailable"));
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "失敗馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  await act(async () => undefined);
  expect(screen.getByRole("figure", { name: "調教追い切り散布図" })).toBeDefined();
});

it("restores the training table when the stored preference is graph-off", async () => {
  vi.mocked(loadTrainingChartForCurrentUser).mockResolvedValue(false);
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "復元馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  await waitFor(() => {
    expect(screen.getByRole("columnheader", { name: "馬名" })).toBeDefined();
  });
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
});

it("keeps a training view toggle made before preference load finishes", async () => {
  const load: { resolve: (value: boolean) => void } = {
    resolve: (_value: boolean) => undefined,
  };
  vi.mocked(loadTrainingChartForCurrentUser).mockImplementation(
    () =>
      new Promise((resolve) => {
        load.resolve = resolve;
      }),
  );
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "先勝ち馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
  expect(screen.getByRole("columnheader", { name: "馬名" })).toBeDefined();
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
  load.resolve(true);
  await act(async () => undefined);
  expect(screen.getByRole("columnheader", { name: "馬名" })).toBeDefined();
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
});

it("ignores a training chart preference that arrives after unmount", async () => {
  const load: { resolve: (value: boolean) => void } = {
    resolve: (_value: boolean) => undefined,
  };
  vi.mocked(loadTrainingChartForCurrentUser).mockImplementation(
    () =>
      new Promise((resolve) => {
        load.resolve = resolve;
      }),
  );
  const view = render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "アンマウント馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  expect(screen.getByRole("figure", { name: "調教追い切り散布図" })).toBeDefined();
  view.unmount();
  load.resolve(false);
  await act(async () => undefined);
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
});

it("shows the empty state when there are no trainings", () => {
  render(<TrainingTable sourceLabel="JRA" trainings={[]} />);
  expect(
    screen.getByText("JRAの馬ごとの調教・追い切りデータは見つかりませんでした。"),
  ).toBeDefined();
});

it("swallows a failed training chart persist", () => {
  vi.mocked(persistTrainingChartForCurrentUser).mockRejectedValue(new Error("idb write failed"));
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "保存失敗馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
  expect(screen.queryByRole("figure", { name: "調教追い切り散布図" })).toBeNull();
});

it("persists graph-on when グラフ is selected again", () => {
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "再表示馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "テキスト" }));
  fireEvent.click(screen.getByRole("radio", { name: "グラフ" }));
  expect(screen.getByRole("figure", { name: "調教追い切り散布図" })).toBeDefined();
  expect(screen.queryByRole("columnheader", { name: "馬名" })).toBeNull();
  expect(vi.mocked(persistTrainingChartForCurrentUser).mock.calls).toStrictEqual([[false], [true]]);
});

it("plots one point per horse until 推移 is selected", () => {
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[
        training({
          bamei: "複数馬",
          chokyoNengappi: "20260510",
          premiumEvaluationGrade: "A",
          umaban: "01",
        }),
        training({
          bamei: "複数馬",
          chokyoNengappi: "20260516",
          premiumEvaluationGrade: "B",
          umaban: "01",
        }),
      ]}
    />,
  );
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(1);
  fireEvent.click(screen.getByRole("radio", { name: "推移" }));
  expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  expect(vi.mocked(persistTrainingScatterAllWorkoutsForCurrentUser).mock.calls).toStrictEqual([
    [true],
  ]);
});

it("restores all-workout scatter when the stored preference is on", async () => {
  vi.mocked(loadTrainingScatterAllWorkoutsForCurrentUser).mockResolvedValue(true);
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[
        training({
          bamei: "復元複数馬",
          chokyoNengappi: "20260510",
          premiumEvaluationGrade: "A",
          umaban: "01",
        }),
        training({
          bamei: "復元複数馬",
          chokyoNengappi: "20260516",
          premiumEvaluationGrade: "A",
          umaban: "01",
        }),
      ]}
    />,
  );
  await waitFor(() => {
    expect(document.querySelectorAll("circle.training-chart-point").length).toBe(2);
  });
  expect(screen.getByRole("radio", { name: "推移" })).toHaveProperty("checked", true);
});

it("swallows a failed training scatter persist", () => {
  vi.mocked(persistTrainingScatterAllWorkoutsForCurrentUser).mockRejectedValue(
    new Error("idb write failed"),
  );
  render(
    <TrainingTable
      sourceLabel="JRA"
      trainings={[training({ bamei: "散布保存失敗馬", premiumEvaluationGrade: "A" })]}
    />,
  );
  fireEvent.click(screen.getByRole("radio", { name: "推移" }));
  expect(screen.getByRole("radio", { name: "推移" })).toHaveProperty("checked", true);
});
