// Run with: bunx vitest run src/app/races/detail/overall-score-table.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import React from "react";
import { afterEach, expect, test } from "vitest";

import type { OverallScoreRow } from "../../../lib/race-types";
import { OverallScoreTable } from "./overall-score-table";
import { RealtimeRaceProvider, type RealtimeRaceRequest } from "./realtime-client";

const realtimeRequest: RealtimeRaceRequest = {
  apiBaseUrl: "",
  day: "01",
  keibajoCode: "05",
  month: "06",
  raceNumber: "01",
  source: "jra",
  year: "2026",
};

const realtimePayload: RealtimeRacePayload = {
  horseWeights: null,
  odds: null,
  raceEntries: null,
  raceKey: "jra:2026:0601:05:01",
  raceResults: null,
  source: null,
};

const detailA = {
  label: "基本",
  reason: "base",
  score: 0.5,
  weight: 0.4,
};

const detailB = {
  label: "血統",
  reason: "blood",
  score: 0.3,
  weight: 0.3,
};

const detailC = {
  label: "勝率",
  reason: "win",
  score: 0.7,
  weight: 0.3,
};

const rowAlpha: OverallScoreRow = {
  details: [detailA, detailB, detailC],
  horseName: "アルファ",
  horseNumber: "01",
  jockeyName: "騎手A",
  score: 0.9,
  storedOdds: 2.5,
  storedPopularity: 1,
};

const rowBeta: OverallScoreRow = {
  details: [detailA, detailB, detailC],
  horseName: "ベータ",
  horseNumber: "02",
  jockeyName: "騎手B",
  score: 0.6,
  storedOdds: 5.5,
  storedPopularity: 2,
};

const renderWithRealtime = (children: React.ReactNode) =>
  render(
    <RealtimeRaceProvider initialPayload={realtimePayload} request={realtimeRequest}>
      {children}
    </RealtimeRaceProvider>,
  );

afterEach(cleanup);

test("OverallScoreTable shows all per-row details when expandAll is true", () => {
  renderWithRealtime(
    <OverallScoreTable
      expandAll={true}
      realtimeRequest={realtimeRequest}
      rows={[rowAlpha, rowBeta]}
    />,
  );
  const baseCells = screen.getAllByText("基本");
  expect(baseCells.length).toStrictEqual(2);
  const bloodCells = screen.getAllByText("血統");
  expect(bloodCells.length).toStrictEqual(2);
  const winCells = screen.getAllByText("勝率");
  expect(winCells.length).toStrictEqual(2);
});

test("OverallScoreTable respects per-row toggle when expandAll is false", () => {
  renderWithRealtime(
    <OverallScoreTable
      expandAll={false}
      realtimeRequest={realtimeRequest}
      rows={[rowAlpha, rowBeta]}
    />,
  );
  expect(screen.queryByText("基本")).toStrictEqual(null);
  const toggles = screen.getAllByRole("button", { name: /総合スコア詳細/ });
  const firstToggle = toggles.at(0);
  if (!firstToggle) {
    throw new Error("expected at least one toggle button");
  }
  fireEvent.click(firstToggle);
  const baseCells = screen.getAllByText("基本");
  expect(baseCells.length).toStrictEqual(1);
});
