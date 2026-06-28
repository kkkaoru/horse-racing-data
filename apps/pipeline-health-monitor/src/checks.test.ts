// Run with: bun run --filter pipeline-health-monitor test
import { expect, it } from "vitest";

import {
  CHECK_FETCH_RESULTS_STALENESS,
  CHECK_FETCH_WEIGHTS_STALENESS,
  CHECK_RACES_QUEUED,
  CHECK_RACES_STUCK,
  evaluateChecks,
  isWithinJstWindow,
} from "./checks";

// 2026-06-28 15:00 JST = 2026-06-28T06:00:00Z (inside JST 13:00–21:30 results
// window and JST 11:00–21:30 weights window).
const ON_WINDOW_NOW = new Date("2026-06-28T06:00:00Z");
// 2026-06-28 09:00 JST = 2026-06-28T00:00:00Z (outside both staleness windows).
const OFF_WINDOW_NOW = new Date("2026-06-28T00:00:00Z");
// 2026-06-28 12:00 JST = 2026-06-28T03:00:00Z (inside weights 11:00-21:30 but
// before results 13:00 start). Used to assert the two windows are independent.
const WEIGHTS_ONLY_NOW = new Date("2026-06-28T03:00:00Z");

it("isWithinJstWindow returns true at the start boundary minute", () => {
  // 2026-06-28 13:00 JST is exactly the start of the results staleness window.
  const inside = isWithinJstWindow({
    now: new Date("2026-06-28T04:00:00Z"),
    startHour: 13,
    startMin: 0,
    endHour: 21,
    endMin: 30,
  });
  expect(inside).toBe(true);
});

it("isWithinJstWindow returns true at the end boundary minute", () => {
  // 2026-06-28 21:30 JST = 12:30 UTC, exactly the end of the window.
  const inside = isWithinJstWindow({
    now: new Date("2026-06-28T12:30:00Z"),
    startHour: 13,
    startMin: 0,
    endHour: 21,
    endMin: 30,
  });
  expect(inside).toBe(true);
});

it("isWithinJstWindow returns false one minute before the start boundary", () => {
  // 2026-06-28 12:59 JST = 03:59 UTC — before 13:00 JST.
  const inside = isWithinJstWindow({
    now: new Date("2026-06-28T03:59:00Z"),
    startHour: 13,
    startMin: 0,
    endHour: 21,
    endMin: 30,
  });
  expect(inside).toBe(false);
});

it("evaluateChecks: outside the staleness window, the fetch-results check is skipped and ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: null,
      lastSuccessfulFetchWeightsAt: null,
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: OFF_WINDOW_NOW,
  });
  expect(results[0]).toStrictEqual({
    name: CHECK_FETCH_RESULTS_STALENESS,
    ok: true,
    skipped: true,
    value: -1,
    threshold: -1,
    message: "outside window",
  });
});

it("evaluateChecks: outside the staleness window, the fetch-weights check is skipped and ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: null,
      lastSuccessfulFetchWeightsAt: null,
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: OFF_WINDOW_NOW,
  });
  expect(results[1]).toStrictEqual({
    name: CHECK_FETCH_WEIGHTS_STALENESS,
    ok: true,
    skipped: true,
    value: -1,
    threshold: -1,
    message: "outside window",
  });
});

it("evaluateChecks: at 12:00 JST the weights check is active but the results check is skipped", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: null,
      lastSuccessfulFetchWeightsAt: null,
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: WEIGHTS_ONLY_NOW,
  });
  expect(results[0]?.skipped).toBe(true);
  expect(results[1]?.skipped).toBe(undefined);
});

it("evaluateChecks: null lastSuccessfulFetchResultsAt inside the window produces a not-ok result", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: null,
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[0]).toStrictEqual({
    name: CHECK_FETCH_RESULTS_STALENESS,
    ok: false,
    value: -1,
    threshold: 30,
    message: "no successful fetch recorded yet",
  });
});

it("evaluateChecks: lastSuccessfulFetchResultsAt 29 minutes ago is ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:31:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[0]).toStrictEqual({
    name: CHECK_FETCH_RESULTS_STALENESS,
    ok: true,
    value: 29,
    threshold: 30,
    message: "within freshness threshold",
  });
});

it("evaluateChecks: lastSuccessfulFetchResultsAt exactly 30 minutes ago is not ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:30:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[0]).toStrictEqual({
    name: CHECK_FETCH_RESULTS_STALENESS,
    ok: false,
    value: 30,
    threshold: 30,
    message: "exceeded freshness threshold",
  });
});

it("evaluateChecks: null lastSuccessfulFetchWeightsAt inside the window produces a not-ok result", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: null,
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[1]).toStrictEqual({
    name: CHECK_FETCH_WEIGHTS_STALENESS,
    ok: false,
    value: -1,
    threshold: 30,
    message: "no successful fetch recorded yet",
  });
});

it("evaluateChecks: lastSuccessfulFetchWeightsAt 10 minutes ago is ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[1]).toStrictEqual({
    name: CHECK_FETCH_WEIGHTS_STALENESS,
    ok: true,
    value: 10,
    threshold: 30,
    message: "within freshness threshold",
  });
});

it("evaluateChecks: racesQueuedNotFetchedToday below threshold is ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 9,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[2]).toStrictEqual({
    name: CHECK_RACES_QUEUED,
    ok: true,
    value: 9,
    threshold: 10,
    message: "below counter threshold",
  });
});

it("evaluateChecks: racesQueuedNotFetchedToday at threshold is not ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 10,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[2]).toStrictEqual({
    name: CHECK_RACES_QUEUED,
    ok: false,
    value: 10,
    threshold: 10,
    message: "exceeded counter threshold",
  });
});

it("evaluateChecks: racesStuckOverThirtyMin below threshold is ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[3]).toStrictEqual({
    name: CHECK_RACES_STUCK,
    ok: true,
    value: 0,
    threshold: 10,
    message: "below counter threshold",
  });
});

it("evaluateChecks: racesStuckOverThirtyMin at threshold is not ok", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 10,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results[3]).toStrictEqual({
    name: CHECK_RACES_STUCK,
    ok: false,
    value: 10,
    threshold: 10,
    message: "exceeded counter threshold",
  });
});

it("evaluateChecks returns exactly four checks in fixed order", () => {
  const results = evaluateChecks({
    metrics: {
      lastSuccessfulFetchResultsAt: "2026-06-28T05:50:00Z",
      lastSuccessfulFetchWeightsAt: "2026-06-28T05:50:00Z",
      racesQueuedNotFetchedToday: 0,
      racesStuckOverThirtyMin: 0,
    },
    nowJst: ON_WINDOW_NOW,
  });
  expect(results.map((check) => check.name)).toStrictEqual([
    "fetch-results-staleness",
    "fetch-weights-staleness",
    "races-queued-not-fetched-today",
    "races-stuck-over-thirty-min",
  ]);
});
