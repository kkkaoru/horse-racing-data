// Run with: bunx vitest run src/app/races/detail/finish-position-prediction-table.test.tsx

import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import type { RealtimeRacePayload } from "horse-racing-realtime/types";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import { FINISH_POSITION_PREDICTION_EVALUATIONS } from "../../../lib/finish-position-prediction-evaluation";

interface MockRealtimePayloadResult {
  error: string | null;
  payload: RealtimeRacePayload | null;
}

vi.mock("./realtime-client", () => ({
  useRealtimeRacePayload: vi.fn<() => MockRealtimePayloadResult>(() => ({
    error: null,
    payload: null,
  })),
}));

vi.mock("../../../lib/fetch-with-retry", () => ({
  fetchWithRetry: vi.fn<() => Promise<Response>>(() =>
    Promise.reject(new Error("no fetch in tests")),
  ),
}));

import type { FinishPredictionBuildInputs } from "../../../lib/finish-position-prediction";
import type { FinishPredictionRow, Runner } from "../../../lib/race-types";
import {
  buildAllOffToggles,
  buildAllOnToggles,
  buildTogglesFromStored,
  compareFinishPredictionRows,
  CorrectionMasterCheckbox,
  FinishPositionPredictionTable,
  getCorrectionTogglesSnapshot,
  WrappedFinishPredictionEvaluation,
} from "./finish-position-prediction-table";
import { useRealtimeRacePayload, type RealtimeRaceRequest } from "./realtime-client";

interface MockMediaQueryEvent {
  matches: boolean;
}

interface MockEntryStatuses {
  alpha: string | null;
  beta: string | null;
  gamma: string | null;
}

const installMatchMediaMock = (initialMatches: boolean) => {
  const mediaQueryList = {
    addEventListener: (_: string, __: (event: MockMediaQueryEvent) => void) => {},
    addListener: (_: (event: MockMediaQueryEvent) => void) => {},
    matches: initialMatches,
    removeEventListener: (_: string, __: (event: MockMediaQueryEvent) => void) => {},
    removeListener: (_: (event: MockMediaQueryEvent) => void) => {},
  };
  vi.stubGlobal(
    "matchMedia",
    vi.fn(() => mediaQueryList),
  );
};

const sampleRunner: Runner = {
  bamei: "テストホース",
  barei: "4",
  banushimei: null,
  bataiju: null,
  chokyoshimeiRyakusho: "調教師",
  corner1: null,
  corner2: null,
  corner3: null,
  corner4: null,
  damSireName: null,
  futanJuryo: null,
  kakuteiChakujun: null,
  kettoTorokuBango: null,
  kishumeiRyakusho: "騎手",
  kohan3f: null,
  seibetsuCode: "1",
  sireName: null,
  sireSireName: null,
  sohaTime: null,
  tanshoNinkijun: "01",
  tanshoOdds: "0020",
  timeSa: null,
  umaban: "01",
  wakuban: null,
  zogenFugo: null,
  zogenSa: null,
};

const sampleInputs: FinishPredictionBuildInputs = {
  currentDistance: "1600",
  currentKeibajoCode: "05",
  currentRaceDate: "20260607",
  currentSource: "jra",
  results: [],
  runners: [sampleRunner],
};

const sampleRequest: RealtimeRaceRequest = {
  apiBaseUrl: "",
  day: "07",
  keibajoCode: "05",
  month: "06",
  raceNumber: "01",
  source: "jra",
  year: "2026",
};

const buildThreeRunnerInputs = (): FinishPredictionBuildInputs => ({
  currentDistance: "1600",
  currentKeibajoCode: "05",
  currentRaceDate: "20260607",
  currentSource: "jra",
  results: [],
  runners: [
    { ...sampleRunner, bamei: "アルファ", tanshoNinkijun: "05", tanshoOdds: "0050", umaban: "01" },
    { ...sampleRunner, bamei: "ベータ", tanshoNinkijun: "02", tanshoOdds: "0020", umaban: "02" },
    { ...sampleRunner, bamei: "ガンマ", tanshoNinkijun: "01", tanshoOdds: "0010", umaban: "03" },
  ],
});

const buildRealtimePayload = (statuses: MockEntryStatuses): RealtimeRacePayload => ({
  horseWeights: null,
  odds: {
    fetchedAt: "2026-06-07T09:01:00Z",
    history: [],
    horseTrends: [],
    latest: {
      tansho: [
        { combination: "1", odds: 5, rank: 5 },
        { combination: "2", odds: 2, rank: 2 },
        { combination: "3", odds: 1, rank: 1 },
      ],
    },
  },
  raceEntries: {
    fetchedAt: "2026-06-07T09:01:00Z",
    horses: [
      {
        fetchedAt: "2026-06-07T09:01:00Z",
        horseName: "アルファ",
        horseNumber: "1",
        jockeyName: null,
        status: statuses.alpha,
      },
      {
        fetchedAt: "2026-06-07T09:01:00Z",
        horseName: "ベータ",
        horseNumber: "2",
        jockeyName: "騎手ベータ",
        status: statuses.beta,
      },
      {
        fetchedAt: "2026-06-07T09:01:00Z",
        horseName: "ガンマ",
        horseNumber: "3",
        jockeyName: "騎手ガンマ",
        status: statuses.gamma,
      },
    ],
  },
  raceKey: "jra:20260607:05:01",
  raceResults: null,
  source: null,
});

const renderedHorseNames = (): string[] =>
  screen
    .getAllByRole("row")
    .slice(1)
    .map((row) => row.querySelectorAll("td")[1]?.textContent ?? "");

const getCheckboxByQuery = (selector: string): HTMLInputElement => {
  const el = document.querySelector(selector);
  if (!(el instanceof HTMLInputElement)) {
    throw new Error(`expected input element for ${selector}`);
  }
  return el;
};

const getRoleCheckbox = (): HTMLInputElement => {
  const el = screen.getByRole("checkbox");
  if (!(el instanceof HTMLInputElement)) {
    throw new Error("expected checkbox element");
  }
  return el;
};

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

test("WrappedFinishPredictionEvaluation collapses panel by default on mobile viewport", () => {
  installMatchMediaMock(true);
  render(
    <WrappedFinishPredictionEvaluation evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.nar} />,
  );
  const toggle = screen.getByRole("button", { name: "着順予測精度 セクションを開く" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("false");
  const panel = document.querySelector(".finish-prediction-evaluation-panel");
  expect(panel?.closest("[hidden]") !== null).toStrictEqual(true);
});

test("WrappedFinishPredictionEvaluation shows panel by default on desktop viewport", () => {
  installMatchMediaMock(false);
  render(
    <WrappedFinishPredictionEvaluation evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra} />,
  );
  const toggle = screen.getByRole("button", { name: "着順予測精度 セクションを閉じる" });
  expect(toggle.getAttribute("aria-expanded")).toStrictEqual("true");
  const panel = document.querySelector(".finish-prediction-evaluation-panel");
  expect(panel?.closest("[hidden]")).toStrictEqual(null);
});

test("getCorrectionTogglesSnapshot regression guard (06-13 decided OFF, silently reverted ON by cd1e71ad same day, restored 07-11): returns all OFF with strength 0 when localStorage has no key", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => null>(() => null) } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: false,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: false,
    trainerEnabled: false,
  });
});

test("getCorrectionTogglesSnapshot returns all OFF when localStorage returns invalid JSON", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => string>(() => "not-json") } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: false,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: false,
    trainerEnabled: false,
  });
});

test("getCorrectionTogglesSnapshot returns all OFF when localStorage returns non-object JSON", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => string>(() => '"string"') } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: false,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: false,
    trainerEnabled: false,
  });
});

test("getCorrectionTogglesSnapshot reads the new shape from localStorage", () => {
  vi.stubGlobal("window", {
    localStorage: {
      getItem: vi.fn<() => string>(() =>
        JSON.stringify({
          formEnabled: false,
          jockeyEnabled: true,
          oddsPopularityStrength: 0.5,
          sameDayJockeyEnabled: false,
          trainerEnabled: true,
        }),
      ),
    },
  });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: true,
    oddsPopularityStrength: 0.5,
    sameDayJockeyEnabled: false,
    trainerEnabled: true,
  });
});

test("getCorrectionTogglesSnapshot migrates the legacy 8-boolean shape (odds OFF popularity OFF -> strength 0)", () => {
  vi.stubGlobal("window", {
    localStorage: {
      getItem: vi.fn<() => string>(() =>
        JSON.stringify({
          horse: true,
          jockey: true,
          odds: false,
          popularity: false,
          recent: true,
          sameDayJockey: true,
          similarity: true,
          trainer: true,
        }),
      ),
    },
  });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("getCorrectionTogglesSnapshot migrates the legacy shape with odds ON to strength 1", () => {
  vi.stubGlobal("window", {
    localStorage: {
      getItem: vi.fn<() => string>(() =>
        JSON.stringify({
          horse: false,
          recent: false,
          similarity: false,
          odds: true,
          popularity: false,
        }),
      ),
    },
  });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("buildTogglesFromStored migrates legacy form keys: all three false -> formEnabled false", () => {
  const result = buildTogglesFromStored({ horse: false, recent: false, similarity: false });
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("buildTogglesFromStored migrates legacy form keys: any one true -> formEnabled true", () => {
  const result = buildTogglesFromStored({ horse: false, recent: true, similarity: false });
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("buildTogglesFromStored clamps an out-of-range stored strength to the maximum", () => {
  const result = buildTogglesFromStored({ oddsPopularityStrength: 9 });
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 2,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("buildAllOnToggles returns every flag on and default strength", () => {
  const result = buildAllOnToggles();
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("buildAllOffToggles returns every flag off and strength 0 (raw model)", () => {
  const result = buildAllOffToggles();
  expect(result).toStrictEqual({
    formEnabled: false,
    jockeyEnabled: false,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: false,
    trainerEnabled: false,
  });
});

test("CorrectionMasterCheckbox: click when all-on writes all off (raw model) to localStorage", () => {
  installMatchMediaMock(false);
  const mockSetItem = vi.fn<(key: string, value: string) => void>();
  vi.stubGlobal("localStorage", {
    setItem: mockSetItem,
    getItem: vi.fn<(key: string) => string | null>(),
  });
  const dispatchSpy = vi.spyOn(window, "dispatchEvent");
  render(<CorrectionMasterCheckbox rawToggles={buildAllOnToggles()} />);
  fireEvent.click(screen.getByRole("checkbox"));
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: false,
      jockeyEnabled: false,
      oddsPopularityStrength: 0,
      sameDayJockeyEnabled: false,
      trainerEnabled: false,
    }),
  );
  expect(dispatchSpy).toHaveBeenCalledOnce();
});

test("CorrectionMasterCheckbox: click when all-off writes all on to localStorage", () => {
  installMatchMediaMock(false);
  const mockSetItem = vi.fn<(key: string, value: string) => void>();
  vi.stubGlobal("localStorage", {
    setItem: mockSetItem,
    getItem: vi.fn<(key: string) => string | null>(),
  });
  const dispatchSpy = vi.spyOn(window, "dispatchEvent");
  render(<CorrectionMasterCheckbox rawToggles={buildAllOffToggles()} />);
  fireEvent.click(screen.getByRole("checkbox"));
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: true,
      jockeyEnabled: true,
      oddsPopularityStrength: 1,
      sameDayJockeyEnabled: true,
      trainerEnabled: true,
    }),
  );
  expect(dispatchSpy).toHaveBeenCalledOnce();
});

test("CorrectionMasterCheckbox: click when mixed (flags on but strength 0) writes all on", () => {
  installMatchMediaMock(false);
  const mockSetItem = vi.fn<(key: string, value: string) => void>();
  vi.stubGlobal("localStorage", {
    setItem: mockSetItem,
    getItem: vi.fn<(key: string) => string | null>(),
  });
  const dispatchSpy = vi.spyOn(window, "dispatchEvent");
  const mixedToggles = {
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 0,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  };
  render(<CorrectionMasterCheckbox rawToggles={mixedToggles} />);
  fireEvent.click(screen.getByRole("checkbox"));
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: true,
      jockeyEnabled: true,
      oddsPopularityStrength: 1,
      sameDayJockeyEnabled: true,
      trainerEnabled: true,
    }),
  );
  expect(dispatchSpy).toHaveBeenCalledOnce();
});

test("CorrectionMasterCheckbox: checked true when all on with default strength", () => {
  installMatchMediaMock(false);
  render(<CorrectionMasterCheckbox rawToggles={buildAllOnToggles()} />);
  const checkbox = getRoleCheckbox();
  expect(checkbox.checked).toStrictEqual(true);
});

test("CorrectionMasterCheckbox: checked false when all off and strength 0", () => {
  installMatchMediaMock(false);
  render(<CorrectionMasterCheckbox rawToggles={buildAllOffToggles()} />);
  const checkbox = getRoleCheckbox();
  expect(checkbox.checked).toStrictEqual(false);
});

test("CorrectionMasterCheckbox: indeterminate true when a flag is off but strength is full", () => {
  installMatchMediaMock(false);
  const mixedToggles = {
    formEnabled: true,
    jockeyEnabled: false,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  };
  render(<CorrectionMasterCheckbox rawToggles={mixedToggles} />);
  const el = getCheckboxByQuery("#correction-checkbox-all");
  expect(el.indeterminate).toStrictEqual(true);
});

test("FinishPositionPredictionTable shows that predictions have not been generated", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(screen.getByText("このレースの着順予測はまだ生成されていません。")).toBeTruthy();
  expect(document.querySelector(".finish-prediction-generated-missing")).toBeTruthy();
});

test("FinishPositionPredictionTable shows the generated date and time immediately before the ranking table", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={{
        ...sampleInputs,
        modelPredictionFeatures: [
          {
            horseNumber: "01",
            modelVersion: "test-model",
            predictedFinishNorm: 0.2,
            predictionGeneratedAt: "2026-08-21T16:00:00.000Z",
            showProbability: null,
            winProbability: null,
          },
        ],
      }}
      realtimeRequest={sampleRequest}
    />,
  );
  const notice = screen.getByText("予測生成日時: 2026年8月22日 01:00:00");
  expect(notice.className).toBe("finish-prediction-generated-at");
  expect(notice.nextElementSibling?.className).toBe("stats-table-wrap");
});

test("FinishPositionPredictionTable shows a generated notice when the timestamp is missing", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={{
        ...sampleInputs,
        modelPredictionFeatures: [
          {
            horseNumber: "01",
            modelVersion: "test-model",
            predictedFinishNorm: 0.2,
            showProbability: null,
            winProbability: null,
          },
        ],
      }}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(screen.getByText("着順予測は生成済みです。").className).toBe(
    "finish-prediction-generated-at",
  );
});

test("FinishPositionPredictionTable renders the strength slider at 0 (raw model) with no stored preference", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const slider = getCheckboxByQuery("#correction-strength-slider");
  expect(slider.value).toStrictEqual("0");
  expect(slider.max).toStrictEqual("2");
  expect(slider.step).toStrictEqual("0.1");
});

test("FinishPositionPredictionTable slider change writes the new strength to localStorage, preserving the other default-off flags", () => {
  installMatchMediaMock(false);
  const mockSetItem = vi.fn<(key: string, value: string) => void>();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: mockSetItem,
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const slider = getCheckboxByQuery("#correction-strength-slider");
  fireEvent.change(slider, { target: { value: "1.5" } });
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: false,
      jockeyEnabled: false,
      oddsPopularityStrength: 1.5,
      sameDayJockeyEnabled: false,
      trainerEnabled: false,
    }),
  );
});

test("FinishPositionPredictionTable combined checkbox starts unchecked (default off) and opting in writes formEnabled true", () => {
  installMatchMediaMock(false);
  const mockSetItem = vi.fn<(key: string, value: string) => void>();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: mockSetItem,
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const combined = getCheckboxByQuery("#correction-checkbox-formEnabled");
  expect(combined.checked).toStrictEqual(false);
  fireEvent.click(combined);
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: true,
      jockeyEnabled: false,
      oddsPopularityStrength: 0,
      sameDayJockeyEnabled: false,
      trainerEnabled: false,
    }),
  );
});

test("FinishPositionPredictionTable shows the combined past-form label", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const label = document.querySelector('label[for="correction-checkbox-formEnabled"]');
  expect(label?.getAttribute("title")).toStrictEqual("過去成績補正（競走成績・近走・類似レース）");
});

test("FinishPositionPredictionTable accepts combined score data after the initial render", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const { rerender } = render(
    <FinishPositionPredictionTable
      combinedScoreData={null}
      combinedScoreLoading={true}
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );

  expect(
    document.querySelector(".finish-prediction-table tbody tr")?.querySelectorAll("td")[7]
      ?.textContent,
  ).toStrictEqual("");

  rerender(
    <FinishPositionPredictionTable
      combinedScoreData={{
        bloodlineRows: [],
        correlationRows: [],
        rows: [],
        runners: [sampleRunner],
        timeRows: [],
      }}
      combinedScoreLoading={false}
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );

  expect(
    document.querySelector(".finish-prediction-table tbody tr")?.querySelectorAll("td")[7]
      ?.textContent,
  ).toStrictEqual("1.00");
});

test("FinishPositionPredictionTable removes a scratched horse from the rendered prediction rows", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  vi.mocked(useRealtimeRacePayload).mockReturnValue({
    error: null,
    payload: buildRealtimePayload({ alpha: null, beta: "出走取消", gamma: "" }),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={buildThreeRunnerInputs()}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(screen.queryByText("ベータ")).toStrictEqual(null);
  expect(screen.getByText("アルファ").textContent).toStrictEqual("アルファ");
  expect(screen.getByText("ガンマ").textContent).toStrictEqual("ガンマ");
});

test("FinishPositionPredictionTable renders only non-scratched horses in predicted-rank order, following the model prediction rather than the (better) market odds of the loser", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  vi.mocked(useRealtimeRacePayload).mockReturnValue({
    error: null,
    payload: buildRealtimePayload({ alpha: "", beta: "出走取消", gamma: "" }),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...buildThreeRunnerInputs(),
    modelPredictionFeatures: [
      {
        horseNumber: "1",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.2,
        showProbability: null,
        winProbability: null,
      },
      {
        horseNumber: "3",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.8,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  // ガンマ (umaban 3) has the best market odds/popularity of the field but the worse
  // model prediction; アルファ (umaban 1) is the reverse. With corrections default off,
  // the market signal must not move the ranking.
  expect(renderedHorseNames()).toStrictEqual(["アルファ", "ガンマ"]);
});

test("FinishPositionPredictionTable regression guard (06-13 decided OFF, silently reverted ON by cd1e71ad same day, restored 07-11): default state ranks by the model alone, ignoring opposing market odds/popularity", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  // A prior test in this file leaves useRealtimeRacePayload's mock return value set via
  // mockReturnValue, which vi.restoreAllMocks() in afterEach does not undo — reset it
  // explicitly so this test does not inherit a stale scratched-horse payload.
  vi.mocked(useRealtimeRacePayload).mockReturnValue({ error: null, payload: null });
  const modelFavoredWorseMarketRunner: Runner = {
    ...sampleRunner,
    bamei: "モデル本命",
    tanshoNinkijun: "02",
    tanshoOdds: "9990",
    umaban: "01",
  };
  const marketFavoredWorseModelRunner: Runner = {
    ...sampleRunner,
    bamei: "市場本命",
    tanshoNinkijun: "01",
    tanshoOdds: "0010",
    umaban: "02",
  };
  const inputs: FinishPredictionBuildInputs = {
    currentDistance: "1600",
    currentKeibajoCode: "05",
    currentRaceDate: "20260607",
    currentSource: "jra",
    modelPredictionFeatures: [
      {
        horseNumber: "1",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.45,
        showProbability: null,
        winProbability: null,
      },
      {
        horseNumber: "2",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.55,
        showProbability: null,
        winProbability: null,
      },
    ],
    results: [],
    runners: [modelFavoredWorseMarketRunner, marketFavoredWorseModelRunner],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(renderedHorseNames()).toStrictEqual(["モデル本命", "市場本命"]);
});

test("FinishPositionPredictionTable regression guard: an explicit all-on stored preference still blends market odds into the ranking (opt-in keeps working)", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() =>
      JSON.stringify({
        formEnabled: true,
        jockeyEnabled: true,
        oddsPopularityStrength: 1,
        sameDayJockeyEnabled: true,
        trainerEnabled: true,
      }),
    ),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  // A prior test in this file leaves useRealtimeRacePayload's mock return value set via
  // mockReturnValue, which vi.restoreAllMocks() in afterEach does not undo — reset it
  // explicitly so this test does not inherit a stale scratched-horse payload.
  vi.mocked(useRealtimeRacePayload).mockReturnValue({ error: null, payload: null });
  const modelFavoredWorseMarketRunner: Runner = {
    ...sampleRunner,
    bamei: "モデル本命",
    tanshoNinkijun: "02",
    tanshoOdds: "9990",
    umaban: "01",
  };
  const marketFavoredWorseModelRunner: Runner = {
    ...sampleRunner,
    bamei: "市場本命",
    tanshoNinkijun: "01",
    tanshoOdds: "0010",
    umaban: "02",
  };
  const inputs: FinishPredictionBuildInputs = {
    currentDistance: "1600",
    currentKeibajoCode: "05",
    currentRaceDate: "20260607",
    currentSource: "jra",
    modelPredictionFeatures: [
      {
        horseNumber: "1",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.45,
        showProbability: null,
        winProbability: null,
      },
      {
        horseNumber: "2",
        modelVersion: "lightgbm-test",
        predictedFinishNorm: 0.55,
        showProbability: null,
        winProbability: null,
      },
    ],
    results: [],
    runners: [modelFavoredWorseMarketRunner, marketFavoredWorseModelRunner],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(renderedHorseNames()).toStrictEqual(["市場本命", "モデル本命"]);
});

test("FinishPositionPredictionTable renders no prediction rows when every horse is scratched", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  vi.mocked(useRealtimeRacePayload).mockReturnValue({
    error: null,
    payload: buildRealtimePayload({ alpha: "出走取消", beta: "出走取消", gamma: "出走取消" }),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={buildThreeRunnerInputs()}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(screen.queryByText("アルファ")).toStrictEqual(null);
  expect(screen.queryByText("ベータ")).toStrictEqual(null);
  expect(screen.queryByText("ガンマ")).toStrictEqual(null);
  expect(screen.getAllByRole("row").length).toStrictEqual(1);
});

test("compareFinishPredictionRows orders by ascending predicted rank", () => {
  const first: FinishPredictionRow = {
    confidence: 0.5,
    details: [],
    horseName: "アルファ",
    horseNumber: "7",
    jockeyName: "騎手",
    predictedRank: 1,
    score: 0.9,
    showProbability: 0.5,
    storedOdds: null,
    storedPopularity: null,
    winProbability: 0.5,
  };
  const second: FinishPredictionRow = {
    confidence: 0.5,
    details: [],
    horseName: "ガンマ",
    horseNumber: "2",
    jockeyName: "騎手",
    predictedRank: 3,
    score: 0.7,
    showProbability: 0.5,
    storedOdds: null,
    storedPopularity: null,
    winProbability: 0.5,
  };
  expect(compareFinishPredictionRows(first, second)).toStrictEqual(-2);
});

test("compareFinishPredictionRows breaks an equal predicted-rank tie by horse number", () => {
  const first: FinishPredictionRow = {
    confidence: 0.5,
    details: [],
    horseName: "アルファ",
    horseNumber: "5",
    jockeyName: "騎手",
    predictedRank: 2,
    score: 0.9,
    showProbability: 0.5,
    storedOdds: null,
    storedPopularity: null,
    winProbability: 0.5,
  };
  const second: FinishPredictionRow = {
    confidence: 0.5,
    details: [],
    horseName: "ガンマ",
    horseNumber: "3",
    jockeyName: "騎手",
    predictedRank: 2,
    score: 0.9,
    showProbability: 0.5,
    storedOdds: null,
    storedPopularity: null,
    winProbability: 0.5,
  };
  expect(compareFinishPredictionRows(first, second)).toStrictEqual(2);
});

test("FinishPositionPredictionTable renders the high confidence badge when modelPredictionFeatures carry confidenceTier high", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "high",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const badge = document.querySelector(".finish-prediction-confidence-badge-high");
  expect(badge?.textContent).toStrictEqual("予測の自信度: 高");
  expect(badge?.getAttribute("title")).toStrictEqual(
    "馬同士の予測スコアの差が大きいほど「自信度」を高く表示しています。的中を保証するものではありません。",
  );
});

test("FinishPositionPredictionTable renders the mid confidence badge label", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "mid",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const badge = document.querySelector(".finish-prediction-confidence-badge-mid");
  expect(badge?.textContent).toStrictEqual("予測の自信度: 中");
});

test("FinishPositionPredictionTable renders the low confidence badge label", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const badge = document.querySelector(".finish-prediction-confidence-badge-low");
  expect(badge?.textContent).toStrictEqual("予測の自信度: 低");
});

test("FinishPositionPredictionTable renders no confidence badge when confidenceTier is null", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(document.querySelector(".finish-prediction-confidence-badge")).toStrictEqual(null);
});

test("FinishPositionPredictionTable renders the upset warning badge for grade E at Hakodate (02)", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = { ...sampleInputs, currentGradeCode: "E" };
  const request: RealtimeRaceRequest = { ...sampleRequest, keibajoCode: "02" };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={request}
    />,
  );
  const badge = document.querySelector(".finish-prediction-upset-warning-badge");
  expect(badge?.textContent).toStrictEqual("⚠ 荒れやすい傾向のレース");
  expect(badge?.getAttribute("title")).toStrictEqual(
    "特別戦かつ函館・札幌開催のレースは、過去実績で人気薄の馬が勝つ割合が高めです。予測の精度自体が下がるわけではありませんが、波乱の可能性を念頭に置いてご覧ください。",
  );
});

test("FinishPositionPredictionTable renders the upset warning badge for grade E at Sapporo (01)", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = { ...sampleInputs, currentGradeCode: "E" };
  const request: RealtimeRaceRequest = { ...sampleRequest, keibajoCode: "01" };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={request}
    />,
  );
  expect(
    document.querySelector(".finish-prediction-upset-warning-badge")?.textContent,
  ).toStrictEqual("⚠ 荒れやすい傾向のレース");
});

test("FinishPositionPredictionTable does not render the upset warning badge for grade E at Fukushima (03), out of scope for this badge", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = { ...sampleInputs, currentGradeCode: "E" };
  const request: RealtimeRaceRequest = { ...sampleRequest, keibajoCode: "03" };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={request}
    />,
  );
  expect(document.querySelector(".finish-prediction-upset-warning-badge")).toStrictEqual(null);
});

test("FinishPositionPredictionTable does not render the upset warning badge for a non-E grade at Hakodate", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = { ...sampleInputs, currentGradeCode: "A" };
  const request: RealtimeRaceRequest = { ...sampleRequest, keibajoCode: "02" };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={request}
    />,
  );
  expect(document.querySelector(".finish-prediction-upset-warning-badge")).toStrictEqual(null);
});

test("FinishPositionPredictionTable renders the ranked table by default even when predictedScoreStddev is low (does not hide automatically)", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.08,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(document.querySelector(".finish-prediction-table")).not.toStrictEqual(null);
  expect(document.querySelector(".finish-prediction-odds-toggle")).not.toStrictEqual(null);
  expect(document.querySelector(".finish-prediction-hidden-by-user-message")).toStrictEqual(null);
});

test("FinishPositionPredictionTable shows the raw stddev value and explanation when predictedScoreStddev is present", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.08,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(document.querySelector(".finish-prediction-stddev-value")?.textContent).toStrictEqual(
    "予測スコアのばらつき (標準偏差): 0.08",
  );
  expect(
    document.querySelector(".finish-prediction-stddev-explanation")?.textContent,
  ).toStrictEqual(
    "標準偏差は、レース内の馬同士で予測スコアにどれだけ差がついているかを示す値です。値が高いほどモデルが馬の実力差を明確に区別できていることを、値が低いほど馬同士が横並びで区別できていないことを意味します。的中率を保証する数値ではありません。",
  );
});

test("FinishPositionPredictionTable does not show stddev info when predictedScoreStddev is null", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={sampleInputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(document.querySelector(".finish-prediction-stddev-info")).toStrictEqual(null);
});

test("FinishPositionPredictionTable shows the low-reliability notice and hide toggle (unchecked by default) when predictedScoreStddev is below the floor", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.49,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const notice = document.querySelector(".finish-prediction-low-reliability-notice");
  expect(notice?.textContent).toStrictEqual(
    "この数値は健全なレースの分布と比べて低く、予測の信頼性が低い可能性があります。この予測を非表示にする",
  );
  const checkbox = document.querySelector(".finish-prediction-hide-toggle input");
  if (!(checkbox instanceof HTMLInputElement)) {
    throw new Error("expected hide-toggle checkbox element");
  }
  expect(checkbox.checked).toStrictEqual(false);
});

test("FinishPositionPredictionTable does not show the low-reliability notice when predictedScoreStddev is exactly at the floor", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.5,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(document.querySelector(".finish-prediction-low-reliability-notice")).toStrictEqual(null);
});

test("FinishPositionPredictionTable hides the ranked table and shows the hidden-by-user message once the user checks the hide toggle", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.08,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const checkbox = document.querySelector(".finish-prediction-hide-toggle input");
  if (!(checkbox instanceof HTMLInputElement)) {
    throw new Error("expected hide-toggle checkbox element");
  }
  fireEvent.click(checkbox);
  expect(
    document.querySelector(".finish-prediction-hidden-by-user-message")?.textContent,
  ).toStrictEqual(
    "選択により、この予測の順位表を非表示にしています。上のチェックボックスを外すと再表示できます。",
  );
  expect(document.querySelector(".finish-prediction-table")).toStrictEqual(null);
  expect(document.querySelector(".finish-prediction-odds-toggle")).toStrictEqual(null);
});

test("FinishPositionPredictionTable shows the ranked table again once the user unchecks the hide toggle", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  const inputs: FinishPredictionBuildInputs = {
    ...sampleInputs,
    modelPredictionFeatures: [
      {
        confidenceTier: "low",
        horseNumber: "1",
        modelVersion: "test-model",
        predictedFinishNorm: 0.5,
        predictedScoreStddev: 0.08,
        showProbability: null,
        winProbability: null,
      },
    ],
  };
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={inputs}
      realtimeRequest={sampleRequest}
    />,
  );
  const checkbox = document.querySelector(".finish-prediction-hide-toggle input");
  if (!(checkbox instanceof HTMLInputElement)) {
    throw new Error("expected hide-toggle checkbox element");
  }
  fireEvent.click(checkbox);
  fireEvent.click(checkbox);
  expect(document.querySelector(".finish-prediction-hidden-by-user-message")).toStrictEqual(null);
  expect(document.querySelector(".finish-prediction-table")).not.toStrictEqual(null);
});
