import { cleanup, render, screen } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import type { FinishPredictionBuildInputs } from "../../../lib/finish-position-prediction";
import { FINISH_POSITION_PREDICTION_EVALUATIONS } from "../../../lib/finish-position-prediction-evaluation";
import type { OverallScoreRow } from "../../../lib/race-types";
import { FinishPositionPredictionTable } from "./finish-position-prediction-table";
import { OverallScoreTable } from "./overall-score-table";
import { RealtimeRaceProvider, type RealtimeRaceRequest } from "./realtime-client";

const realtimeRequest: RealtimeRaceRequest = {
  apiBaseUrl: "",
  day: "14",
  keibajoCode: "45",
  month: "05",
  raceNumber: "09",
  source: "nar",
  year: "2026",
};

const realtimePayload: RealtimeRacePayload = {
  horseWeights: null,
  odds: {
    fetchedAt: "2026-05-14T18:35:00+09:00",
    history: [],
    horseTrends: [],
    latest: {
      tansho: [
        { combination: "1", odds: 4.5, rank: 1 },
        { combination: "2", odds: 99.9, rank: 12 },
      ],
    },
  },
  raceEntries: {
    fetchedAt: "2026-05-14T18:35:00+09:00",
    horses: [
      {
        fetchedAt: "2026-05-14T18:35:00+09:00",
        horseName: "取消馬",
        horseNumber: "2",
        jockeyName: "騎手",
        status: "出走取消",
      },
    ],
  },
  raceKey: "nar:2026:0514:45:09",
  raceResults: null,
  source: null,
};

const finishInputs = (): FinishPredictionBuildInputs => ({
  currentDistance: "1600",
  currentKeibajoCode: "45",
  currentRaceDate: "20260514",
  currentSource: "nar",
  modelPredictionFeatures: [
    {
      horseNumber: "1",
      modelVersion: "test",
      predictedFinishNorm: 0.8,
      showProbability: null,
      winProbability: null,
    },
    {
      horseNumber: "2",
      modelVersion: "test",
      predictedFinishNorm: 0.1,
      showProbability: null,
      winProbability: null,
    },
  ],
  results: [],
  runners: [
    {
      bamei: "通常馬",
      barei: "4",
      banushimei: null,
      bataiju: null,
      chokyoshimeiRyakusho: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      futanJuryo: null,
      kakuteiChakujun: null,
      kettoTorokuBango: null,
      kishumeiRyakusho: "騎手",
      kohan3f: null,
      seibetsuCode: "1",
      sohaTime: null,
      tanshoNinkijun: "03",
      tanshoOdds: "0055",
      timeSa: null,
      umaban: "01",
      wakuban: null,
      zogenFugo: null,
      zogenSa: null,
    },
    {
      bamei: "取消馬",
      barei: "4",
      banushimei: null,
      bataiju: null,
      chokyoshimeiRyakusho: null,
      corner1: null,
      corner2: null,
      corner3: null,
      corner4: null,
      futanJuryo: null,
      kakuteiChakujun: null,
      kettoTorokuBango: null,
      kishumeiRyakusho: "騎手",
      kohan3f: null,
      seibetsuCode: "1",
      sohaTime: null,
      tanshoNinkijun: "01",
      tanshoOdds: "0010",
      timeSa: null,
      umaban: "02",
      wakuban: null,
      zogenFugo: null,
      zogenSa: null,
    },
  ],
});

const overallRow = (overrides: Partial<OverallScoreRow>): OverallScoreRow => ({
  details: [
    {
      label: "タイム",
      reason: "時計を評価",
      score: 0.8,
      weight: 0.5,
    },
  ],
  horseName: "通常馬",
  horseNumber: "01",
  jockeyName: "騎手",
  score: 0.91,
  storedOdds: 5.5,
  storedPopularity: 3,
  ...overrides,
});

const renderWithRealtime = (children: React.ReactNode) =>
  render(
    <RealtimeRaceProvider initialPayload={realtimePayload} request={realtimeRequest}>
      {children}
    </RealtimeRaceProvider>,
  );

const rowTexts = (): string[] =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => row.textContent ?? "");

afterEach(cleanup);

describe("prediction tables scratched runners", () => {
  it("moves scratched finish prediction rows last and hides score and probabilities", () => {
    renderWithRealtime(
      <FinishPositionPredictionTable
        evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.nar}
        inputs={finishInputs()}
        realtimeRequest={realtimeRequest}
      />,
    );

    expect(rowTexts()[0]).toContain("通常馬");
    expect(rowTexts()[1]).toContain("取消馬");
    expect(rowTexts()[1]).toContain("出走取消");
    expect(rowTexts()[1]).toContain("対象外");
    expect(rowTexts()[1]).not.toContain("0.99");
    expect(rowTexts()[1]).not.toContain("99.9");
  });

  it("moves scratched overall score rows last and hides score and realtime odds", () => {
    renderWithRealtime(
      <OverallScoreTable
        realtimeRequest={realtimeRequest}
        rows={[
          overallRow({ horseName: "取消馬", horseNumber: "02", score: 1 }),
          overallRow({ horseName: "通常馬", horseNumber: "01", score: 0.1 }),
        ]}
      />,
    );

    expect(rowTexts()[0]).toContain("通常馬");
    expect(rowTexts()[1]).toContain("取消馬");
    expect(rowTexts()[1]).toContain("出走取消");
    expect(rowTexts()[1]).toContain("対象外");
    expect(rowTexts()[1]).not.toContain("1.00");
    expect(rowTexts()[1]).not.toContain("99.9");
  });
});
