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

const recoverySearch = vi.hoisted(() => ({ query: "" }));

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  recoverySearch.query = "";
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

// NOTE: LazyOverallScoreSection is unused on the race detail page. Combined overall
// score calculation still runs via the time-score payload used by finish prediction
// and the win-rate heatmap.

vi.mock("./clef-rerank-table", () => ({ ClefRerankTable: () => null }));
vi.mock("./arc-prediction-table", () => ({
  ArcPredictionTable: ({ runners }: { runners: unknown[] }) => (
    <div data-testid="arc-prediction-stub" data-runner-count={runners.length} />
  ),
}));

vi.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(recoverySearch.query),
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
    arcCohort,
    arcQualifiedStats,
    bloodlineRows,
    frameStats,
    horseResults,
    presentation,
  }: {
    arcCohort?: string;
    arcQualifiedStats?: { rows: unknown[] };
    bloodlineRows: unknown[];
    frameStats: unknown[];
    horseResults: unknown[];
    presentation?: { version: number };
  }) => (
    <div
      data-bloodline-rows={bloodlineRows.length}
      data-frame-stats={frameStats.length}
      data-presentation-version={presentation?.version}
      data-horse-results={horseResults.length}
      data-arc-cohort={arcCohort}
      data-qualified-rows={arcQualifiedStats?.rows.length}
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
import {
  FINISH_PREDICTION_SECTION_PROMISE_TTL_MS,
  LazyDetailSections,
  LazyFinishPredictionSection,
} from "./lazy-detail-sections";

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
  expect(
    screen.getByText(
      "血統の勝率・連対率・複勝率は、父系（父・父父・父父父・父母父）と母父系（母父・母父父・母母父）それぞれで同名の種牡馬の成績を合算しています。チェックを外すと父系と母父系も合算します。同条件で父の勝ち例がない場合は、勝ち例を確認できる同会場の広い条件の過去成績で血統欄を補います。",
    ),
  ).toBeTruthy();
  expect(heatmapStub.getAttribute("data-horse-results")).toStrictEqual("0");
  expect(heatmapStub.getAttribute("data-frame-stats")).toStrictEqual("1");
  expect(heatmapStub.getAttribute("data-presentation-version")).toBeNull();
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

test("LazyFinishPredictionSection routes the exact Arc card to its dedicated display", async () => {
  vi.mocked(fetchWithRetry).mockResolvedValue(
    new Response(
      JSON.stringify({
        bucket: {},
        evaluation: null,
        inputs: { runners: [{ umaban: "01" }] },
        type: "finish-prediction",
      }),
      { headers: { "content-type": "application/json" } },
    ),
  );
  render(
    <LazyFinishPredictionSection
      day="4"
      keibajoCode="A8"
      month="10"
      raceNumber="5"
      realtimeApiBaseUrl="https://realtime.example"
      source="jra"
      year="2026"
    />,
  );
  expect((await screen.findByTestId("arc-prediction-stub")).getAttribute("data-runner-count")).toBe(
    "1",
  );
  expect(screen.queryByTestId("finish-position-prediction-stub")).toBeNull();
});

test("LazyFinishPredictionSection accepts an unavailable overseas evaluation", async () => {
  vi.mocked(fetchWithRetry).mockResolvedValue(
    new Response(
      JSON.stringify({
        bucket: {},
        evaluation: null,
        inputs: { modelPredictionFeatures: [], currentKeibajoCode: "A8" },
        type: "finish-prediction",
      }),
      { headers: { "content-type": "application/json" }, status: 200 },
    ),
  );
  render(
    <LazyFinishPredictionSection
      day="05"
      keibajoCode="A8"
      month="10"
      raceNumber="05"
      realtimeApiBaseUrl="https://realtime.example"
      source="jra"
      year="2026"
    />,
  );
  expect(await screen.findByTestId("finish-position-prediction-stub")).toBeTruthy();
});

test("LazyFinishPredictionSection refetches after its short promise-cache TTL", async () => {
  let nowMs = Date.parse("2026-08-23T01:00:00.000Z");
  vi.spyOn(Date, "now").mockImplementation(() => nowMs);
  const finishUrl = "/api/races/2026/08/23/01/01/sections/finish-prediction";
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url === finishUrl) {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bucket: {},
            evaluation: {},
            inputs: { modelPredictionFeatures: [] },
            type: "finish-prediction",
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
  const props = {
    day: "23",
    keibajoCode: "01",
    month: "08",
    raceNumber: "01",
    realtimeApiBaseUrl: "https://realtime.example",
    source: "jra" as const,
    year: "2026",
  };

  render(<LazyFinishPredictionSection {...props} />);
  await screen.findByTestId("finish-position-prediction-stub");
  cleanup();

  render(<LazyFinishPredictionSection {...props} />);
  await screen.findByTestId("finish-position-prediction-stub");
  expect(vi.mocked(fetchWithRetry).mock.calls.filter(([url]) => url === finishUrl)).toHaveLength(1);
  cleanup();

  nowMs += FINISH_PREDICTION_SECTION_PROMISE_TTL_MS + 1;
  render(<LazyFinishPredictionSection {...props} />);
  await waitFor(() => {
    expect(vi.mocked(fetchWithRetry).mock.calls.filter(([url]) => url === finishUrl)).toHaveLength(
      2,
    );
  });
});

test("LazyDetailSections refetches a warmed heatmap when revisiting after its promise TTL", async () => {
  installMatchMediaMockTimeScore(false);
  let nowMs = Date.parse("2026-09-27T00:00:00.000Z");
  vi.spyOn(Date, "now").mockImplementation(() => nowMs);
  const defaultFetch = vi.mocked(fetchWithRetry).getMockImplementation();
  if (!defaultFetch) throw new Error("missing fetch mock");
  const heatmapUrl = "/api/races/2026/09/27/06/09/sections/win-rate-heatmap";
  let hasBloodlineStats = false;
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    if (input === heatmapUrl) {
      return Promise.resolve(
        Response.json({
          bloodlineRows: hasBloodlineStats ? [{ category: "sire", name: "父", starts: 10 }] : [],
          frameStats: [],
          horseResults: [],
          runners: [],
          similarRows: [],
          type: "win-rate-heatmap",
        }),
      );
    }
    return defaultFetch(input);
  });
  const props: React.ComponentProps<typeof LazyDetailSections> = {
    day: "27",
    keibajoCode: "06",
    month: "09",
    raceNumber: "09",
    realtimeApiBaseUrl: "https://realtime.example",
    source: "jra",
    year: "2026",
  };

  render(<LazyDetailSections {...props} />);
  expect((await screen.findByTestId("win-rate-heatmap-stub")).dataset.bloodlineRows).toBe("0");
  cleanup();

  hasBloodlineStats = true;
  render(<LazyDetailSections {...props} />);
  expect((await screen.findByTestId("win-rate-heatmap-stub")).dataset.bloodlineRows).toBe("0");
  expect(vi.mocked(fetchWithRetry).mock.calls.filter(([url]) => url === heatmapUrl)).toHaveLength(
    1,
  );
  cleanup();

  nowMs += FINISH_PREDICTION_SECTION_PROMISE_TTL_MS + 1;
  render(<LazyDetailSections {...props} />);
  await waitFor(() => {
    expect(screen.getByTestId("win-rate-heatmap-stub").dataset.bloodlineRows).toBe("1");
    expect(vi.mocked(fetchWithRetry).mock.calls.filter(([url]) => url === heatmapUrl)).toHaveLength(
      2,
    );
  });
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

test("LazyDetailSections still shows the heatmap when the condition section fetch fails", async () => {
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url === "/api/races/2027/06/16/05/01/sections/condition") {
      return Promise.resolve(new Response("", { status: 502, statusText: "Bad Gateway" }));
    }
    if (url === "/api/races/2027/06/16/05/01/sections/win-rate-heatmap") {
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
    if (url === "/api/races/2027/06/16/05/01/sections/results") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            classConditionName: null,
            currentDistance: null,
            currentKeibajoCode: "05",
            currentRaceDate: "20270616",
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
        day="16"
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
    expect(screen.getByText("データを取得できませんでした: 502 Bad Gateway")).toBeDefined();
  });
  await waitFor(() => {
    expect(screen.getByTestId("win-rate-heatmap-stub").textContent).toStrictEqual("heatmap");
  });
  expect(screen.getByRole("heading", { name: "同条件レース分析" })).toBeDefined();
  expect(screen.getByRole("heading", { name: "勝率ヒートマップ" }).tagName).toBe("H3");
});

test("LazyDetailSections shows the heatmap while the condition section is still loading", async () => {
  installMatchMediaMockTimeScore(false);
  const pendingCondition = new Promise<Response>(() => undefined);
  vi.mocked(fetchWithRetry).mockImplementation((input) => {
    const url = typeof input === "string" ? input : "";
    if (url === "/api/races/2026/08/23/04/01/sections/condition") {
      return pendingCondition;
    }
    if (url === "/api/races/2026/08/23/04/01/sections/win-rate-heatmap") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            bloodlineRows: [],
            carriedWeightClassStats: [],
            frameStats: [],
            horseResults: [],
            runners: [{ umaban: "01" }],
            similarRows: [],
            type: "win-rate-heatmap",
            weightClassStats: [],
          }),
          { headers: { "content-type": "application/json" }, status: 200 },
        ),
      );
    }
    if (url === "/api/races/2026/08/23/04/01/sections/results") {
      return Promise.resolve(
        new Response(
          JSON.stringify({
            classConditionName: null,
            currentDistance: null,
            currentKeibajoCode: "04",
            currentRaceDate: "20260823",
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
        day="23"
        keibajoCode="04"
        month="08"
        raceNumber="01"
        realtimeApiBaseUrl=""
        source="jra"
        year="2026"
      />,
    );
  });
  await waitFor(() => {
    expect(screen.getByTestId("win-rate-heatmap-stub").textContent).toStrictEqual("heatmap");
  });
  expect(screen.getByRole("heading", { name: "勝率ヒートマップ" }).tagName).toBe("H3");
});

const recoveryProps: React.ComponentProps<typeof LazyDetailSections> = {
  day: "04",
  keibajoCode: "08",
  month: "10",
  raceNumber: "01",
  realtimeApiBaseUrl: "",
  source: "jra",
  year: "2035",
};
const readyHeatmap = {
  type: "win-rate-heatmap",
  bloodlineRows: [],
  frameStats: [],
  horseResults: [],
  runners: [],
  similarRows: [],
  carriedWeightClassStats: [],
  weightClassStats: [],
  presentation: { version: 1, rows: [] },
};

test.each([
  ["30", 30_000],
  ["0", 1_000],
  ["999999999", 300_000],
  [null, 30_000],
  ["invalid", 30_000],
  ["Thu, 04 Oct 2035 00:00:45 GMT", 45_000],
  ["Wed, 03 Oct 2035 00:00:00 GMT", 1_000],
])("mounted heatmap waits for Retry-After %s then becomes ready", async (header, delay) => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  vi.setSystemTime(new Date("2035-10-04T00:00:00Z"));
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(
      Response.json(
        { error: "heatmap_not_warmed" },
        {
          status: 503,
          headers: header === null ? {} : { "Retry-After": header },
        },
      ),
    )
    .mockResolvedValue(Response.json(readyHeatmap));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber={String(header)} />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(screen.getByRole("status").textContent).toBe(
    "勝率ヒートマップの取得を待機しています。自動で再確認します。",
  );
  await act(async () => {
    await vi.advanceTimersByTimeAsync(delay - 21);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(21);
  });
  expect(heatmap).toHaveBeenCalledTimes(2);
  expect(screen.getByTestId("win-rate-heatmap-stub").dataset.presentationVersion).toBe("1");
  expect(
    vi
      .mocked(fetchWithRetry)
      .mock.calls.find(([url]) => typeof url === "string" && url.includes("win-rate-heatmap"))?.[2],
  ).toStrictEqual({ attempts: 1, retryStatuses: new Set() });
});

test("heatmap offers retry after bounded transport failures", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockRejectedValue(new TypeError("Failed to fetch"));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="transport" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(4979);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(30021);
  });
  expect(heatmap).toHaveBeenCalledTimes(4);
  expect(screen.getByText("データを取得できませんでした: Failed to fetch")).toBeTruthy();
  heatmap.mockResolvedValue(Response.json(readyHeatmap));
  await act(async () => {
    screen.getByRole("button", { name: "勝率ヒートマップを再試行" }).click();
  });
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
});

test("heatmap automatically recovers a transient transport failure", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockRejectedValueOnce("network unavailable")
    .mockResolvedValue(Response.json(readyHeatmap));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="recover" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5020);
  });
  expect(heatmap).toHaveBeenCalledTimes(2);
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
});

test.each([
  [503, '{"error":"catalog_unavailable"}', "503 catalog_unavailable"],
  [503, "<html>unavailable</html>", "503"],
  [401, '{"error":"heatmap_not_warmed"}', "401 heatmap_not_warmed"],
  [403, "<html>Access</html>", "403"],
  [200, '{"type":"condition"}', "Invalid section payload"],
  [503, '{"error":42}', "503"],
])("heatmap does not label status %s body %s as pending", async (status, body, message) => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValue(new Response(body, { status }));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber={body + status} />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  expect(screen.queryByRole("status")).toBeNull();
  expect(screen.getAllByText(`データを取得できませんでした: ${message}`).length).toBeGreaterThan(0);
  expect(screen.getByRole("button", { name: "勝率ヒートマップを再試行" })).toBeTruthy();
});

test("two heatmap consumers share a promise even when one leaves", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const deferred = Promise.withResolvers<Response>();
  const heatmap = vi.fn<() => Promise<Response>>().mockReturnValue(deferred.promise);
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  const first = render(<LazyDetailSections {...recoveryProps} raceNumber="shared" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20_000);
  });
  render(<LazyDetailSections {...recoveryProps} raceNumber="shared" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  first.unmount();
  const signal = vi
    .mocked(fetchWithRetry)
    .mock.calls.find(
      ([url]) => typeof url === "string" && url.includes("/shared/sections/win-rate-heatmap"),
    )?.[1]?.signal;
  expect(signal?.aborted).toBe(false);
  await act(async () => {
    deferred.resolve(Response.json(readyHeatmap));
  });
  expect(screen.getAllByTestId("win-rate-heatmap-stub")).toHaveLength(1);
  expect(heatmap).toHaveBeenCalledTimes(1);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(30_000);
  });
  expect(signal?.aborted).toBe(false);
});

test.each([true, false])(
  "heatmap ignores old race response success=%s and cancels pending retries",
  async (success) => {
    vi.useFakeTimers();
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
      window.setTimeout(() => callback(0), 0),
    );
    installMatchMediaMockTimeScore(false);
    const deferred = Promise.withResolvers<Response>();
    const heatmap = vi
      .fn<() => Promise<Response>>()
      .mockReturnValueOnce(deferred.promise)
      .mockImplementation(() =>
        Promise.resolve(Response.json({ error: "heatmap_not_warmed" }, { status: 503 })),
      );
    vi.mocked(fetchWithRetry).mockImplementation((url) =>
      typeof url === "string" && url.includes("win-rate-heatmap")
        ? heatmap()
        : Promise.resolve(Response.json({ type: "time-score" })),
    );
    const view = render(<LazyDetailSections {...recoveryProps} raceNumber={`old-${success}`} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20);
    });
    view.rerender(<LazyDetailSections {...recoveryProps} raceNumber={`new-${success}`} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20);
    });
    await act(async () => {
      if (success) deferred.resolve(Response.json(readyHeatmap));
      else deferred.reject(new TypeError("old failure"));
    });
    expect(screen.queryByTestId("win-rate-heatmap-stub")).toBeNull();
    expect(screen.getByRole("status")).toBeTruthy();
    view.unmount();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(120_000);
    });
    expect(heatmap).toHaveBeenCalledTimes(2);
  },
);

test("heatmap pending continues beyond the transport retry budget and stops on navigation", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockImplementation(() =>
      Promise.resolve(
        Response.json(
          { error: "heatmap_not_warmed" },
          { status: 503, headers: { "Retry-After": "1" } },
        ),
      ),
    );
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  const view = render(<LazyDetailSections {...recoveryProps} raceNumber="pending-repeat" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5020);
  });
  expect(heatmap).toHaveBeenCalledTimes(6);
  expect(screen.queryByRole("button", { name: "勝率ヒートマップを再試行" })).toBeNull();
  heatmap.mockImplementation(() => Promise.resolve(Response.json(readyHeatmap)));
  view.rerender(<LazyDetailSections {...recoveryProps} raceNumber="pending-destination" />);
  expect(screen.queryByTestId("win-rate-heatmap-stub")).toBeNull();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5020);
  });
  expect(heatmap).toHaveBeenCalledTimes(7);
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
});

test("nonheatmap 503 keeps its original fetch policy and terminal error", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    Promise.resolve(
      typeof url === "string" && url.includes("win-rate-heatmap")
        ? Response.json(readyHeatmap)
        : Response.json(
            { error: "heatmap_not_warmed" },
            { status: 503, statusText: "Unavailable" },
          ),
    ),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="nonheatmap" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(120_000);
  });
  expect(
    screen.getAllByText("データを取得できませんでした: 503 Unavailable").length,
  ).toBeGreaterThan(0);
  expect(
    vi
      .mocked(fetchWithRetry)
      .mock.calls.filter(
        ([url]) => typeof url === "string" && url.includes("/nonheatmap/sections/results"),
      ),
  ).toHaveLength(2);
  expect(
    vi
      .mocked(fetchWithRetry)
      .mock.calls.find(
        ([url]) => typeof url === "string" && url.includes("/nonheatmap/sections/results"),
      )
      ?.slice(1),
  ).toStrictEqual([]);
});

test("heatmap query changes hide old ready data immediately and keep filtered requests distinct", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const filtered = Promise.withResolvers<Response>();
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(Response.json(readyHeatmap))
    .mockReturnValue(filtered.promise);
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  const view = render(<LazyDetailSections {...recoveryProps} raceNumber="query" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
  recoverySearch.query = "similarStatsVenue=08&statsYears=3";
  view.rerender(<LazyDetailSections {...recoveryProps} raceNumber="query" />);
  expect(screen.queryByTestId("win-rate-heatmap-stub")).toBeNull();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(
    vi
      .mocked(fetchWithRetry)
      .mock.calls.some(
        ([url]) =>
          url ===
          "/api/races/2035/10/04/08/query/sections/win-rate-heatmap?similarStatsVenue=08&statsYears=3",
      ),
  ).toBe(true);
  await act(async () => {
    filtered.resolve(
      Response.json({
        ...readyHeatmap,
        bloodlineRows: [
          { category: "sire", name: "アドマイヤマーズ", starts: 108, winCount: 9, winRate: 8.3 },
        ],
      }),
    );
  });
  expect(screen.getByTestId("win-rate-heatmap-stub").dataset.bloodlineRows).toBe("1");
});

test("unmount before scheduled heatmap load cancels the request", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "cancelAnimationFrame").mockImplementation((id) => window.clearTimeout(id));
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockClear();
  const view = render(<LazyDetailSections {...recoveryProps} raceNumber="cancel-before-load" />);
  view.unmount();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(fetchWithRetry).not.toHaveBeenCalled();
});

test("heatmap malformed success JSON is terminal with explicit retry", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValue(new Response("<html>Access login</html>"));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="malformed" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  expect(screen.queryByRole("status")).toBeNull();
  expect(screen.getByRole("button", { name: "勝率ヒートマップを再試行" })).toBeTruthy();
});

test("two mounted consumers coalesce pending responses and recover together", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(
      Response.json(
        { error: "heatmap_not_warmed" },
        { status: 503, headers: { "Retry-After": "30" } },
      ),
    )
    .mockResolvedValue(Response.json(readyHeatmap));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="shared-pending" />);
  render(<LazyDetailSections {...recoveryProps} raceNumber="shared-pending" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(heatmap).toHaveBeenCalledTimes(1);
  expect(screen.getAllByRole("status")).toHaveLength(2);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(30_000);
  });
  expect(heatmap).toHaveBeenCalledTimes(2);
  expect(screen.getAllByTestId("win-rate-heatmap-stub")).toHaveLength(2);
});

test.each(["transport", "body", "abort"])(
  "heatmap bounds stalled %s with a shared deadline and offers manual recovery after retries",
  async (stage) => {
    vi.useFakeTimers();
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
      window.setTimeout(() => callback(0), 0),
    );
    installMatchMediaMockTimeScore(false);
    const heatmap = vi
      .fn<(signal: AbortSignal | null | undefined) => Promise<Response>>()
      .mockImplementation((signal) => {
        if (stage === "body") return Promise.resolve(new Response(new ReadableStream()));
        const deferred = Promise.withResolvers<Response>();
        if (stage === "abort")
          signal?.addEventListener(
            "abort",
            () => {
              deferred.reject(new DOMException("aborted", "AbortError"));
            },
            { once: true },
          );
        return deferred.promise;
      });
    vi.mocked(fetchWithRetry).mockImplementation((url, init) =>
      typeof url === "string" && url.includes("win-rate-heatmap")
        ? heatmap(init?.signal)
        : Promise.resolve(Response.json({ type: "time-score" })),
    );
    render(<LazyDetailSections {...recoveryProps} raceNumber={`stalled-${stage}`} />);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(29_999);
    });
    expect(heatmap).toHaveBeenCalledTimes(1);
    expect(heatmap.mock.calls[0]?.[0]?.aborted).toBe(false);
    expect(screen.queryByRole("status")).toBeNull();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2);
    });
    expect(heatmap.mock.calls[0]?.[0]?.aborted).toBe(true);
    expect(screen.getByRole("status")).toBeTruthy();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_998);
    });
    expect(heatmap).toHaveBeenCalledTimes(1);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2);
    });
    expect(heatmap).toHaveBeenCalledTimes(2);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(39_998);
    });
    expect(heatmap).toHaveBeenCalledTimes(2);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2);
    });
    expect(heatmap).toHaveBeenCalledTimes(3);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(49_998);
    });
    expect(heatmap).toHaveBeenCalledTimes(3);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_002);
    });
    expect(heatmap).toHaveBeenCalledTimes(4);
    expect(
      screen.getByText("データを取得できませんでした: Heatmap request timed out"),
    ).toBeTruthy();
    heatmap.mockResolvedValue(Response.json(readyHeatmap));
    await act(async () => {
      screen.getByRole("button", { name: "勝率ヒートマップを再試行" }).click();
    });
    await act(async () => {
      await vi.advanceTimersByTimeAsync(20);
    });
    expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
  },
);

test.each([200, 503])("heatmap retries body stream failures for HTTP %s", async (status) => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockResolvedValueOnce(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.error(new TypeError("body stream interrupted"));
          },
        }),
        { status },
      ),
    )
    .mockResolvedValue(Response.json(readyHeatmap));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber={`body-error-${status}`} />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(screen.getByRole("status")).toBeTruthy();
  await act(async () => {
    await vi.advanceTimersByTimeAsync(5_000);
  });
  expect(heatmap).toHaveBeenCalledTimes(2);
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
});

test("abandoned heatmap times out without retrying and remount starts a fresh request", async () => {
  vi.useFakeTimers();
  vi.spyOn(window, "requestAnimationFrame").mockImplementation((callback) =>
    window.setTimeout(() => callback(0), 0),
  );
  installMatchMediaMockTimeScore(false);
  const deferred = Promise.withResolvers<Response>();
  const heatmap = vi
    .fn<() => Promise<Response>>()
    .mockReturnValueOnce(deferred.promise)
    .mockResolvedValue(Response.json(readyHeatmap));
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    typeof url === "string" && url.includes("win-rate-heatmap")
      ? heatmap()
      : Promise.resolve(Response.json({ type: "time-score" })),
  );
  const view = render(<LazyDetailSections {...recoveryProps} raceNumber="abandoned" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  const signal = vi
    .mocked(fetchWithRetry)
    .mock.calls.find(
      ([url]) => typeof url === "string" && url.includes("/abandoned/sections/win-rate-heatmap"),
    )?.[1]?.signal;
  view.unmount();
  expect(signal?.aborted).toBe(false);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(60_000);
  });
  expect(signal?.aborted).toBe(true);
  expect(heatmap).toHaveBeenCalledTimes(1);
  render(<LazyDetailSections {...recoveryProps} raceNumber="abandoned" />);
  await act(async () => {
    await vi.advanceTimersByTimeAsync(20);
  });
  expect(heatmap).toHaveBeenCalledTimes(2);
  expect(screen.getByTestId("win-rate-heatmap-stub")).toBeTruthy();
});

test("passes the explicit Arc marker from a delivered payload to the UI and suppresses broad fallback guidance", async () => {
  installMatchMediaMockTimeScore(false);
  vi.mocked(fetchWithRetry).mockImplementation((url) =>
    Promise.resolve(
      Response.json(
        typeof url === "string" && url.includes("win-rate-heatmap")
          ? {
              ...readyHeatmap,
              arcQualifiedStats: { rows: [] },
              arcCohort: "arc-20261004-jra-A8-05-v2",
            }
          : { type: "time-score" },
      ),
    ),
  );
  render(<LazyDetailSections {...recoveryProps} raceNumber="arc-marker" />);
  await waitFor(() =>
    expect(screen.getByTestId("win-rate-heatmap-stub").getAttribute("data-arc-cohort")).toBe(
      "arc-20261004-jra-A8-05-v2",
    ),
  );
  expect(screen.getByTestId("win-rate-heatmap-stub").getAttribute("data-qualified-rows")).toBe("0");
  expect(document.querySelector(".win-rate-heatmap-bloodline-note")).toBeNull();
});
