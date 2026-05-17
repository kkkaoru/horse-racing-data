import { cleanup, render, screen } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import React from "react";
import { afterEach, describe, expect, it } from "vitest";

import { FINISH_POSITION_PREDICTION_EVALUATIONS } from "../../../lib/finish-position-prediction-evaluation";
import type { FinishPredictionRow, OverallScoreRow } from "../../../lib/race-types";
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

const finishRow = (overrides: Partial<FinishPredictionRow>): FinishPredictionRow => ({
  confidence: 0.7,
  details: [
    {
      label: "近走",
      reason: "近走を評価",
      value: 0.5,
      weight: 0.5,
    },
  ],
  horseName: "通常馬",
  horseNumber: "01",
  jockeyName: "騎手",
  predictedRank: 1,
  score: 0.91,
  showProbability: 0.55,
  storedOdds: 5.5,
  storedPopularity: 3,
  winProbability: 0.22,
  ...overrides,
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
        realtimeRequest={realtimeRequest}
        rows={[
          finishRow({ horseName: "取消馬", horseNumber: "02", predictedRank: 1, score: 0.99 }),
          finishRow({ horseName: "通常馬", horseNumber: "01", predictedRank: 2, score: 0.5 }),
        ]}
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
