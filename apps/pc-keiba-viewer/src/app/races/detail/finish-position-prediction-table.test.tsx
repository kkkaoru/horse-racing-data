// Run with: bunx vitest run src/app/races/detail/finish-position-prediction-table.test.tsx

import { cleanup, render, screen } from "@testing-library/react";
import React from "react";
import { afterEach, expect, test, vi } from "vitest";

import { FINISH_POSITION_PREDICTION_EVALUATIONS } from "../../../lib/finish-position-prediction-evaluation";
import { WrappedFinishPredictionEvaluation } from "./finish-position-prediction-table";

interface MockMediaQueryEvent {
  matches: boolean;
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

test("RACE_FINISH_PREDICTION_RESULTS_EVENT is not dispatched when rendering evaluation panel", () => {
  // Regression: the event listener that unconditionally replaced displayRows
  // (ignoring oddsCorrectionEnabled) was removed. Verify no such event is dispatched.
  installMatchMediaMock(false);
  const dispatchSpy = vi.spyOn(window, "dispatchEvent");
  render(
    <WrappedFinishPredictionEvaluation evaluation={FINISH_POSITION_PREDICTION_EVALUATIONS.nar} />,
  );
  const dispatchedFinishEvents = dispatchSpy.mock.calls
    .map((call) => call[0])
    .filter((e): e is CustomEvent => e instanceof CustomEvent)
    .map((e) => e.type)
    .filter((type) => type === "race:finish-prediction-results");
  expect(dispatchedFinishEvents).toStrictEqual([]);
});
