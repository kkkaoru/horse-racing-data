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

test("getCorrectionTogglesSnapshot returns all ON with default strength when localStorage has no key", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => null>(() => null) } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("getCorrectionTogglesSnapshot returns all ON when localStorage returns invalid JSON", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => string>(() => "not-json") } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
  });
});

test("getCorrectionTogglesSnapshot returns all ON when localStorage returns non-object JSON", () => {
  vi.stubGlobal("window", { localStorage: { getItem: vi.fn<() => string>(() => '"string"') } });
  const result = getCorrectionTogglesSnapshot();
  expect(result).toStrictEqual({
    formEnabled: true,
    jockeyEnabled: true,
    oddsPopularityStrength: 1,
    sameDayJockeyEnabled: true,
    trainerEnabled: true,
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

test("FinishPositionPredictionTable renders the strength slider with default value", () => {
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
  expect(slider.value).toStrictEqual("1");
  expect(slider.max).toStrictEqual("2");
  expect(slider.step).toStrictEqual("0.1");
});

test("FinishPositionPredictionTable slider change writes the new strength to localStorage", () => {
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
  fireEvent.change(slider, { target: { value: "0" } });
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: true,
      jockeyEnabled: true,
      oddsPopularityStrength: 0,
      sameDayJockeyEnabled: true,
      trainerEnabled: true,
    }),
  );
});

test("FinishPositionPredictionTable combined checkbox toggle writes formEnabled false", () => {
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
  fireEvent.click(combined);
  expect(mockSetItem).toHaveBeenCalledWith(
    "pc-keiba:correction-toggles",
    JSON.stringify({
      formEnabled: false,
      jockeyEnabled: true,
      oddsPopularityStrength: 1,
      sameDayJockeyEnabled: true,
      trainerEnabled: true,
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

test("FinishPositionPredictionTable renders only non-scratched horses in predicted-rank order", () => {
  installMatchMediaMock(false);
  vi.stubGlobal("localStorage", {
    getItem: vi.fn<(key: string) => string | null>(() => null),
    setItem: vi.fn<(key: string, value: string) => void>(),
  });
  vi.mocked(useRealtimeRacePayload).mockReturnValue({
    error: null,
    payload: buildRealtimePayload({ alpha: "", beta: "出走取消", gamma: "" }),
  });
  render(
    <FinishPositionPredictionTable
      evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.jra}
      inputs={buildThreeRunnerInputs()}
      realtimeRequest={sampleRequest}
    />,
  );
  expect(renderedHorseNames()).toStrictEqual(["ガンマ", "アルファ"]);
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
