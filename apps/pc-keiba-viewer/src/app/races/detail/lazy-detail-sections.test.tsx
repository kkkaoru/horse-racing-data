// Run with: bunx vitest run src/app/races/detail/lazy-detail-sections.test.tsx

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

interface MockMediaQueryListController {
  matches: boolean;
}

interface MockMediaQueryEvent {
  matches: boolean;
}

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

// NOTE: LazyOverallScoreSection (Issue E original target) is dead code — never imported outside its own file.
// Agent H re-targeted to LazyTimeScoreSection (the actual user-visible section). Tests below cover the live path.

vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("../../../lib/fetch-with-retry", () => ({
  fetchWithRetry: vi.fn<() => Promise<Response>>(
    () =>
      new Promise<Response>((resolve) => {
        resolve(
          new Response(
            JSON.stringify({
              bloodlineRows: [],
              bloodlineSettings: {},
              conditionLabels: {
                age: null,
                class: null,
                distance: null,
                frame: "",
                monthWindow: "",
                raceNumber: "",
                raceSubtitle: null,
                raceTitle: null,
                sex: null,
                surface: null,
                turn: null,
                venue: null,
                weight: null,
              },
              correlationRows: [],
              rows: [],
              runners: [],
              settings: {},
              similarRows: [],
              source: "jra",
              type: "time-score",
            }),
            { headers: { "content-type": "application/json" }, status: 200 },
          ),
        );
      }),
  ),
}));

vi.mock("./bloodline-similar-combined-table", () => ({
  BloodlineSimilarCombinedTable: () => (
    <div data-testid="bloodline-similar-combined-stub">combined</div>
  ),
}));

vi.mock("./bloodline-stats-table", () => ({
  BloodlineStatsTable: () => <div data-testid="bloodline-stats-stub">bloodline</div>,
}));

vi.mock("./similar-race-stats-table", () => ({
  SimilarRaceStatsTable: () => <div data-testid="similar-race-stats-stub">similar</div>,
}));

vi.mock("./overall-score-table", () => ({
  OverallScoreTable: () => <div data-testid="overall-score-table-stub">overall</div>,
}));

vi.mock("./horse-race-results-table", () => ({
  HorseRaceResultsTable: () => <div data-testid="horse-race-results-table-stub">results</div>,
}));

vi.mock("./training-table", () => ({
  TrainingTable: () => <div data-testid="training-table-stub">training</div>,
}));

vi.mock("./ability-test-table", () => ({
  AbilityTestTable: () => <div data-testid="ability-test-table-stub">ability</div>,
}));

vi.mock("./race-condition-analysis-section", () => ({
  RaceConditionAnalysisSection: () => (
    <div data-testid="race-condition-analysis-stub">condition</div>
  ),
}));

vi.mock("./race-pace-prediction-table", () => ({
  RacePacePredictionTable: () => <div data-testid="race-pace-prediction-stub">pace</div>,
}));

vi.mock("./premium-data-top-section", () => ({
  PremiumDataTopHorsesTable: () => <div data-testid="premium-data-top-stub">premium</div>,
}));

vi.mock("./finish-position-prediction-table", () => ({
  FinishPositionPredictionTable: () => (
    <div data-testid="finish-position-prediction-stub">finish</div>
  ),
}));

vi.mock("./finish-position-bucket-section", () => ({
  FinishPositionBucketEvaluationPanel: () => (
    <div data-testid="finish-position-bucket-stub">bucket</div>
  ),
}));

import { LazyTimeScoreSection } from "./lazy-detail-sections";

const installMatchMediaMockTimeScore = (initialMatches: boolean): MockMediaQueryListController => {
  const listeners = new Set<(event: MockMediaQueryEvent) => void>();
  const controller: MockMediaQueryListController = {
    matches: initialMatches,
  };
  const mediaQueryList = {
    addEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    addListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.add(listener);
    },
    get matches() {
      return controller.matches;
    },
    removeEventListener: (_: string, listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
    removeListener: (listener: (event: MockMediaQueryEvent) => void) => {
      listeners.delete(listener);
    },
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
  return controller;
};

interface TimeScoreSectionProps {
  day: string;
  keibajoCode: string;
  month: string;
  raceNumber: string;
  realtimeApiBaseUrl: string;
  source: "jra" | "nar";
  year: string;
}

const timeScoreSectionProps: TimeScoreSectionProps = {
  day: "01",
  keibajoCode: "05",
  month: "06",
  raceNumber: "01",
  realtimeApiBaseUrl: "",
  source: "jra",
  year: "2026",
};

test("LazyTimeScoreSection collapses by default on mobile viewport", async () => {
  installMatchMediaMockTimeScore(true);
  await act(async () => {
    render(<LazyTimeScoreSection {...timeScoreSectionProps} />);
  });
  await waitFor(() => {
    expect(screen.getByRole("button", { name: "総合評価スコア セクションを開く" })).toBeDefined();
  });
  const toggle = screen.getByRole("button", { name: "総合評価スコア セクションを開く" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("false");
  const bodyContent = screen.getByTestId("bloodline-similar-combined-stub");
  expect(bodyContent.parentElement?.parentElement?.hasAttribute("hidden")).toStrictEqual(true);
});

test("LazyTimeScoreSection expands by default on desktop viewport", async () => {
  installMatchMediaMockTimeScore(false);
  await act(async () => {
    render(<LazyTimeScoreSection {...timeScoreSectionProps} />);
  });
  await waitFor(() => {
    expect(screen.getByRole("button", { name: "総合評価スコア セクションを閉じる" })).toBeDefined();
  });
  await waitFor(() => {
    expect(screen.getByTestId("bloodline-similar-combined-stub")).toBeDefined();
  });
  const toggle = screen.getByRole("button", { name: "総合評価スコア セクションを閉じる" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("true");
  const bodyContent = screen.getByTestId("bloodline-similar-combined-stub");
  expect(bodyContent.parentElement?.parentElement?.hasAttribute("hidden")).toStrictEqual(false);
});
