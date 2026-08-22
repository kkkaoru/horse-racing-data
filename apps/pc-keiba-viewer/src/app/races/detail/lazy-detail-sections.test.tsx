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

// NOTE: LazyOverallScoreSection is unused on the race detail page. Combined overall
// score calculation still runs via the time-score payload used by finish prediction
// and the win-rate heatmap.

vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
}));
vi.mock("../../../lib/fetch-with-retry", () => ({
  fetchWithRetry: vi.fn<(input: RequestInfo | URL) => Promise<Response>>((input) => {
    const url = typeof input === "string" ? input : "";
    if (url.endsWith("/sections/results")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            classConditionName: null,
            currentDistance: null,
            currentKeibajoCode: "05",
            currentRaceDate: "20270601",
            currentTrackCode: null,
            defaultIncludeClass: false,
            results: [],
            runners: [],
            source: "jra",
            sourceScope: "all",
            type: "results",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url.endsWith("/sections/training")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            sourceLabel: "src",
            stableComments: [],
            trainings: [],
            type: "training",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url.endsWith("/sections/condition")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            frameStats: [{ frameNumber: "1" }],
            type: "condition",
          }),
          {
            headers: { "content-type": "application/json" },
            status: 200,
          },
        ),
      );
    }
    if (url.endsWith("/sections/win-rate-heatmap")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bloodlineRows: [],
            carriedWeightClassStats: [],
            frameStats: [{ frameNumber: "1" }],
            horseResults: [],
            runners: [],
            similarRows: [],
            type: "win-rate-heatmap",
            weightClassStats: [],
          }),
          {
            headers: { "content-type": "application/json" },
            status: 200,
          },
        ),
      );
    }
    return Promise.resolve(
      new Response(
        JSON.stringify({
          bloodlineRows: [],
          bloodlineSettings: {},
          bloodlineStatsIncomplete: true,
          bloodlineVenueFallback: true,
          conditionLabels: {
            age: null,
            class: null,
            distance: null,
            frame: "",
            grade: null,
            monthWindow: "",
            raceNumber: "",
            raceSubtitle: null,
            raceTitle: null,
            sex: null,
            surface: null,
            track: null,
            turn: null,
            venue: null,
            weight: null,
          },
          correlationRows: [],
          rows: [],
          runners: [],
          settings: {},
          similarRows: [],
          similarStatsFallback: true,
          similarStatsIncomplete: true,
          source: "jra",
          type: "time-score",
        }),
        { headers: { "content-type": "application/json" }, status: 200 },
      ),
    );
  }),
}));

vi.mock("./win-rate-heatmap-section", () => ({
  WinRateHeatmapSection: ({
    frameStats,
    horseResults,
  }: {
    frameStats: unknown[];
    horseResults: unknown[];
  }) => (
    <div
      data-frame-stats={frameStats.length}
      data-horse-results={horseResults.length}
      data-testid="win-rate-heatmap-stub"
    >
      heatmap
    </div>
  ),
}));

vi.mock("./overall-score-table", () => ({
  OverallScoreTable: () => <div data-testid="overall-score-table-stub">overall</div>,
}));

vi.mock("./horse-race-results-chart", () => ({
  HorseRaceResultsChart: ({
    day,
    keibajoCode,
    month,
    raceNumber,
    realtimeApiBaseUrl,
    runners,
    source,
    targetKeibajoCode,
    targetRaceDate,
    year,
  }: {
    day?: string;
    keibajoCode?: string;
    month?: string;
    raceNumber?: string;
    realtimeApiBaseUrl?: string;
    runners?: unknown[];
    source?: string;
    targetKeibajoCode?: string | null;
    targetRaceDate?: string | null;
    year?: string;
  }) => (
    <div
      data-day={day ?? ""}
      data-keibajo-code={keibajoCode ?? ""}
      data-month={month ?? ""}
      data-race-number={raceNumber ?? ""}
      data-realtime-api-base-url={realtimeApiBaseUrl ?? ""}
      data-runners-passed={runners === undefined ? "missing" : "present"}
      data-source={source ?? ""}
      data-target-keibajo-code={targetKeibajoCode ?? ""}
      data-target-race-date={targetRaceDate ?? ""}
      data-testid="horse-race-results-chart-stub"
      data-year={year ?? ""}
    >
      chart
    </div>
  ),
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
  RaceConditionAnalysisSection: ({ afterTargetRaces }: { afterTargetRaces?: React.ReactNode }) => (
    <>
      <h3>対象レース一覧</h3>
      {afterTargetRaces === undefined ? null : afterTargetRaces}
      <div data-testid="race-condition-analysis-stub">condition</div>
    </>
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

import { fetchWithRetry } from "../../../lib/fetch-with-retry";
import { LazyDetailSections } from "./lazy-detail-sections";

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

test("LazyDetailSections renders the results chart section directly below the results section", async () => {
  installMatchMediaMockTimeScore(false);
  await act(async () => {
    render(
      <LazyDetailSections
        day="11"
        keibajoCode="05"
        month="06"
        raceNumber="01"
        realtimeApiBaseUrl="https://realtime.example"
        source="jra"
        year="2027"
      />,
    );
  });
  await waitFor(() => {
    expect(screen.getByTestId("horse-race-results-chart-stub").textContent).toStrictEqual("chart");
  });
  await waitFor(() => {
    expect(
      screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
    ).toStrictEqual(["競走成績", "競走成績グラフ", "同条件レース分析"]);
  });
  await waitFor(() => {
    expect(screen.getByTestId("win-rate-heatmap-stub").textContent).toStrictEqual("heatmap");
  });
  const resultsStub = screen.getByTestId("horse-race-results-table-stub");
  const heatmapStub = screen.getByTestId("win-rate-heatmap-stub");
  const chartStub = screen.getByTestId("horse-race-results-chart-stub");
  expect(resultsStub.compareDocumentPosition(chartStub)).toStrictEqual(4);
  expect(chartStub.compareDocumentPosition(heatmapStub)).toStrictEqual(4);
  const conditionHeading = screen.getByRole("heading", { name: "同条件レース分析" });
  const targetRaceHeading = screen.getByRole("heading", { name: "対象レース一覧" });
  expect(conditionHeading.compareDocumentPosition(targetRaceHeading)).toStrictEqual(4);
  expect(targetRaceHeading.compareDocumentPosition(heatmapStub)).toStrictEqual(4);
  expect(screen.getByRole("heading", { name: "勝率ヒートマップ" }).tagName).toBe("H3");
  expect(heatmapStub.getAttribute("data-horse-results")).toStrictEqual("0");
  expect(heatmapStub.getAttribute("data-frame-stats")).toStrictEqual("1");
  expect(chartStub.getAttribute("data-runners-passed")).toStrictEqual("present");
  expect(chartStub.getAttribute("data-target-keibajo-code")).toStrictEqual("05");
  expect(chartStub.getAttribute("data-target-race-date")).toStrictEqual("20270601");
  expect(chartStub.getAttribute("data-year")).toStrictEqual("2027");
  expect(chartStub.getAttribute("data-month")).toStrictEqual("06");
  expect(chartStub.getAttribute("data-day")).toStrictEqual("11");
  expect(chartStub.getAttribute("data-keibajo-code")).toStrictEqual("05");
  expect(chartStub.getAttribute("data-race-number")).toStrictEqual("01");
  expect(chartStub.getAttribute("data-source")).toStrictEqual("jra");
  expect(chartStub.getAttribute("data-realtime-api-base-url")).toStrictEqual(
    "https://realtime.example",
  );
  const resultsFetchCalls = vi
    .mocked(fetchWithRetry)
    .mock.calls.filter(
      (call) => typeof call[0] === "string" && call[0].includes("/sections/results"),
    );
  expect(resultsFetchCalls.length).toBe(1);
  expect(resultsFetchCalls[0]?.[0]).toStrictEqual("/api/races/2027/06/11/05/01/sections/results");
});

test("LazyDetailSections renders a chart section error when the results fetch fails", async () => {
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url === "/api/races/2027/06/12/05/01/sections/results") {
      return Promise.resolve(
        new Response("", { status: 500, statusText: "Internal Server Error" }),
      );
    }
    if (url === "/api/races/2027/06/12/05/01/sections/training") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            sourceLabel: "src",
            stableComments: [],
            trainings: [],
            type: "training",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url === "/api/races/2027/06/12/05/01/sections/condition") {
      return Promise.resolve(
        new Response(JSON.stringify({ type: "condition" }), {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      );
    }
    if (url === "/api/races/2027/06/12/05/01/sections/win-rate-heatmap") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bloodlineRows: [],
            carriedWeightClassStats: [],
            frameStats: [],
            horseResults: [],
            runners: [],
            similarRows: [],
            type: "win-rate-heatmap",
            weightClassStats: [],
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    return Promise.resolve(
      new Response(JSON.stringify({ type: "time-score" }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    );
  });
  await act(async () => {
    render(
      <LazyDetailSections
        day="12"
        keibajoCode="05"
        month="06"
        raceNumber="01"
        realtimeApiBaseUrl=""
        source="jra"
        year="2027"
      />,
    );
  });
  await waitFor(() => {
    expect(
      screen.getAllByText("データを取得できませんでした: 500 Internal Server Error").length,
    ).toStrictEqual(2);
  });
  await waitFor(() => {
    expect(
      screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
    ).toStrictEqual(["競走成績", "競走成績グラフ", "同条件レース分析"]);
  });
});

test("LazyDetailSections renders a chart section error when the results payload type is invalid", async () => {
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url === "/api/races/2027/06/13/05/01/sections/results") {
      return Promise.resolve(
        new Response(JSON.stringify({ type: "bogus" }), {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      );
    }
    if (url === "/api/races/2027/06/13/05/01/sections/training") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            sourceLabel: "src",
            stableComments: [],
            trainings: [],
            type: "training",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url === "/api/races/2027/06/13/05/01/sections/condition") {
      return Promise.resolve(
        new Response(JSON.stringify({ type: "condition" }), {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      );
    }
    if (url === "/api/races/2027/06/13/05/01/sections/win-rate-heatmap") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bloodlineRows: [],
            carriedWeightClassStats: [],
            frameStats: [],
            horseResults: [],
            runners: [],
            similarRows: [],
            type: "win-rate-heatmap",
            weightClassStats: [],
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    return Promise.resolve(
      new Response(JSON.stringify({ type: "time-score" }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    );
  });
  await act(async () => {
    render(
      <LazyDetailSections
        day="13"
        keibajoCode="05"
        month="06"
        raceNumber="01"
        realtimeApiBaseUrl=""
        source="jra"
        year="2027"
      />,
    );
  });
  await waitFor(() => {
    expect(
      screen.getAllByText("データを取得できませんでした: Invalid section payload").length,
    ).toStrictEqual(2);
  });
  await waitFor(() => {
    expect(
      screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
    ).toStrictEqual(["競走成績", "競走成績グラフ", "同条件レース分析"]);
  });
});

test("LazyDetailSections renders training before heatmap and heatmap immediately above condition analysis", async () => {
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url.endsWith("/sections/results")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            classConditionName: null,
            currentDistance: null,
            currentKeibajoCode: "05",
            currentRaceDate: "20270601",
            currentTrackCode: null,
            defaultIncludeClass: false,
            results: [],
            runners: [],
            source: "jra",
            sourceScope: "all",
            type: "results",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url.endsWith("/sections/training")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            sourceLabel: "src",
            stableComments: [
              {
                commentText: "好調",
                evaluationGrade: 1,
                evaluationText: "好調",
                fetchedAt: "2027-06-14T00:00:00.000Z",
                frameNumber: "1",
                horseName: "テストホース",
                horseNumber: "01",
              },
            ],
            trainings: [{ bamei: "テストホース", umaban: "01" }],
            type: "training",
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url.endsWith("/sections/condition")) {
      return Promise.resolve(
        new Response(JSON.stringify({ type: "condition" }), {
          headers: { "content-type": "application/json" },
          status: 200,
        }),
      );
    }
    if (url.endsWith("/sections/win-rate-heatmap")) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bloodlineRows: [],
            carriedWeightClassStats: [],
            frameStats: [{ frameNumber: "1" }],
            horseResults: [],
            runners: [],
            similarRows: [],
            type: "win-rate-heatmap",
            weightClassStats: [],
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    return Promise.resolve(
      new Response(JSON.stringify({ type: "time-score" }), {
        headers: { "content-type": "application/json" },
        status: 200,
      }),
    );
  });
  await act(async () => {
    render(
      <LazyDetailSections
        day="14"
        keibajoCode="05"
        month="06"
        raceNumber="01"
        realtimeApiBaseUrl=""
        source="jra"
        year="2027"
      />,
    );
  });
  await waitFor(() => {
    expect(screen.getByTestId("training-table-stub").textContent).toStrictEqual("training");
  });
  await waitFor(() => {
    expect(
      screen.getAllByRole("heading", { level: 2 }).map((heading) => heading.textContent),
    ).toStrictEqual(["競走成績", "競走成績グラフ", "調教・追い切り", "同条件レース分析"]);
  });
  await waitFor(() => {
    expect(screen.getByTestId("win-rate-heatmap-stub").textContent).toStrictEqual("heatmap");
  });
  expect(screen.getByRole("heading", { name: "厩舎コメント" }).tagName).toBe("H3");
  expect(screen.getByRole("heading", { name: "勝率ヒートマップ" }).tagName).toBe("H3");
  const trainingHeading = screen.getByRole("heading", { name: "調教・追い切り" });
  const stableHeading = screen.getByRole("heading", { name: "厩舎コメント" });
  const targetRaceHeading = screen.getByRole("heading", { name: "対象レース一覧" });
  const heatmapStub = screen.getByTestId("win-rate-heatmap-stub");
  const conditionHeading = screen.getByRole("heading", { name: "同条件レース分析" });
  expect(trainingHeading.compareDocumentPosition(stableHeading)).toStrictEqual(4);
  expect(stableHeading.compareDocumentPosition(conditionHeading)).toStrictEqual(4);
  expect(conditionHeading.compareDocumentPosition(targetRaceHeading)).toStrictEqual(4);
  expect(targetRaceHeading.compareDocumentPosition(heatmapStub)).toStrictEqual(4);
});
