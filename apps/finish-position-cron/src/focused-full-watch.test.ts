// Run with bun. Tests for Durable focused-full Container completion watches.

import { expect, test, vi } from "vitest";
import {
  buildFocusedFullStatusUrl,
  createFocusedFullWatchTickMessage,
  FOCUSED_FULL_WATCH_PROGRESS_STALE_MS,
  FOCUSED_FULL_WATCH_TIMEOUT_MS,
  hasAcceptedResult,
  isFocusedFullPredictUrl,
  parseFocusedFullWatchHeader,
  pollFocusedFullWatchTick,
  WATCH_REQUEST_HEADER,
} from "./focused-full-watch";
import type {
  FocusedFullWatchBody,
  FocusedFullWatchTickDependencies,
  ValidatedFocusedFullWatchTickMessage,
  ValidatedFocusedFullWatchPayload,
} from "./focused-full-watch";

const BODY: FocusedFullWatchBody = {
  category: "jra",
  daysAhead: 0,
  keibajoCode: "05",
  mode: "full",
  raceBango: "09",
  runDate: "2026-08-24",
  runDateIso: "2026-08-24",
  runYmd: "20260824",
  skipDedup: true,
};

const WATCH: ValidatedFocusedFullWatchPayload = {
  body: BODY,
  doName: "predict-jra-race-1",
  role: "race-chain",
  watchId: "watch-123",
  workKey: "focused-full:20260824:jra:05:09",
};

const SCHEDULED: ValidatedFocusedFullWatchTickMessage = {
  ...WATCH,
  deadlineAtMs: 1_900_000,
  type: "focused-full-watch-tick",
  watchId: "watch-123",
};

const makeDependencies = (response: Response): FocusedFullWatchTickDependencies => ({
  now: vi.fn(() => 100_000),
  pollStatus: vi.fn(async () => response),
});

test("parses a validated focused-full watch request header", () => {
  const request = new Request("http://do/predict", {
    headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(WATCH) },
  });

  expect(parseFocusedFullWatchHeader(request)).toStrictEqual(WATCH);
});

test("rejects missing, malformed, and non-focused watch request headers", () => {
  expect(() => parseFocusedFullWatchHeader(new Request("http://do/predict"))).toThrow(
    "Missing x-focused-full-watch-payload header",
  );
  expect(() =>
    parseFocusedFullWatchHeader(
      new Request("http://do/predict", {
        headers: { [WATCH_REQUEST_HEADER]: "not-json" },
      }),
    ),
  ).toThrow();
  expect(() =>
    parseFocusedFullWatchHeader(
      new Request("http://do/predict", {
        headers: {
          [WATCH_REQUEST_HEADER]: JSON.stringify({ ...WATCH, body: { ...BODY, skipDedup: false } }),
        },
      }),
    ),
  ).toThrow("Invalid x-focused-full-watch-payload header");
});

test.each([
  null,
  {},
  { ...WATCH, body: null },
  { ...WATCH, body: { ...BODY, runDate: "" } },
  { ...WATCH, body: { ...BODY, runDateIso: "" } },
  { ...WATCH, body: { ...BODY, runYmd: "" } },
  { ...WATCH, body: { ...BODY, category: "invalid" } },
  { ...WATCH, body: { ...BODY, daysAhead: Number.NaN } },
  { ...WATCH, body: { ...BODY, mode: "rescore" } },
  { ...WATCH, body: { ...BODY, keibajoCode: "" } },
  { ...WATCH, body: { ...BODY, raceBango: "" } },
  { ...WATCH, doName: "" },
  { ...WATCH, role: "invalid" },
  { ...WATCH, workKey: "" },
])("rejects invalid watch payload field %#", (payload) => {
  const request = new Request("http://do/predict", {
    headers: { [WATCH_REQUEST_HEADER]: JSON.stringify(payload) },
  });

  expect(() => parseFocusedFullWatchHeader(request)).toThrow(
    "Invalid x-focused-full-watch-payload header",
  );
});

test("accepts the legacy role and every prediction category", () => {
  const legacy = new Request("http://do/predict", {
    headers: { [WATCH_REQUEST_HEADER]: JSON.stringify({ ...WATCH, role: "legacy" }) },
  });
  const nar = new Request("http://do/predict", {
    headers: {
      [WATCH_REQUEST_HEADER]: JSON.stringify({ ...WATCH, body: { ...BODY, category: "nar" } }),
    },
  });
  const banEi = new Request("http://do/predict", {
    headers: {
      [WATCH_REQUEST_HEADER]: JSON.stringify({ ...WATCH, body: { ...BODY, category: "ban-ei" } }),
    },
  });

  expect(parseFocusedFullWatchHeader(legacy).role).toBe("legacy");
  expect(parseFocusedFullWatchHeader(nar).body.category).toBe("nar");
  expect(parseFocusedFullWatchHeader(banEi).body.category).toBe("ban-ei");
});

test("recognizes only race-scoped full predict URLs", () => {
  expect(
    isFocusedFullPredictUrl(
      new URL(
        "http://do/predict?mode=full&category=jra&runDate=20260824&keibajoCode=05&raceBango=09",
      ),
    ),
  ).toBe(true);
  expect(isFocusedFullPredictUrl(new URL("http://do/predict?mode=rescore"))).toBe(false);
  expect(isFocusedFullPredictUrl(new URL("http://do/focused-full-status?mode=full"))).toBe(false);
  expect(isFocusedFullPredictUrl(new URL("http://do/predict?mode=full"))).toBe(false);
  expect(isFocusedFullPredictUrl(new URL("http://do/predict?mode=full&category=jra"))).toBe(false);
  expect(
    isFocusedFullPredictUrl(new URL("http://do/predict?mode=full&category=jra&keibajoCode=05")),
  ).toBe(false);
  expect(
    isFocusedFullPredictUrl(
      new URL("http://do/predict?mode=full&category=jra&keibajoCode=05&raceBango=09"),
    ),
  ).toBe(false);
});

test("recognizes accepted NDJSON results without accepting errors or malformed lines", async () => {
  await expect(
    hasAcceptedResult(
      new Response('{bad-json}\n{"type":"progress"}\n{"type":"result","status":"accepted"}\n'),
    ),
  ).resolves.toBe(true);
  await expect(
    hasAcceptedResult(new Response('{"type":"result","status":"success"}\n')),
  ).resolves.toBe(false);
  await expect(hasAcceptedResult(new Response("accepted", { status: 500 }))).resolves.toBe(false);
});

test("creates a stable Queue watch tick and absolute 31 minute deadline", () => {
  expect(createFocusedFullWatchTickMessage(WATCH, 40_000)).toStrictEqual({
    ...WATCH,
    deadlineAtMs: 40_000 + FOCUSED_FULL_WATCH_TIMEOUT_MS,
    type: "focused-full-watch-tick",
  });
});

test("builds the local focused-full status URL", () => {
  expect(buildFocusedFullStatusUrl(BODY)).toBe(
    "http://container/focused-full-status?category=jra&keibajoCode=05&raceBango=09&runDate=20260824",
  );
});

test("returns no terminal message while the pipeline heartbeat is fresh", async () => {
  const dependencies = makeDependencies(
    Response.json({
      error: null,
      lastProgressAtMs: 99_000,
      raceKey: "jra:20260824:05:09",
      status: "running",
    }),
  );

  await expect(pollFocusedFullWatchTick(SCHEDULED, dependencies)).resolves.toBeUndefined();

  expect(dependencies.pollStatus).toHaveBeenCalledWith(BODY);
});

test("returns an error when a running pipeline heartbeat is stale", async () => {
  const dependencies = makeDependencies(
    Response.json({
      error: null,
      lastProgressAtMs: 100_000,
      raceKey: "jra:20260824:05:09",
      status: "running",
    }),
  );
  dependencies.now = vi.fn(() => 100_000 + FOCUSED_FULL_WATCH_PROGRESS_STALE_MS + 1);

  await expect(pollFocusedFullWatchTick(SCHEDULED, dependencies)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    error: "Focused-full detached pipeline heartbeat stale: jra:20260824:05:09",
    outcome: "error",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
});

test("returns success with the same watch ID", async () => {
  const dependencies = makeDependencies(
    Response.json({ error: null, raceKey: "jra:20260824:05:09", status: "success" }),
  );

  await expect(pollFocusedFullWatchTick(SCHEDULED, dependencies)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    outcome: "success",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
});

test("sends missing and Container-reported error terminal outcomes", async () => {
  const missing = makeDependencies(
    Response.json({ error: null, raceKey: "jra:20260824:05:09", status: "missing" }),
  );
  const failed = makeDependencies(
    Response.json({ error: "pipeline exploded", raceKey: "jra:20260824:05:09", status: "error" }),
  );

  await expect(pollFocusedFullWatchTick(SCHEDULED, missing)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    outcome: "missing",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
  await expect(pollFocusedFullWatchTick(SCHEDULED, failed)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    error: "pipeline exploded",
    outcome: "error",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
});

test("returns timeout without polling after the deadline", async () => {
  const dependencies = makeDependencies(
    Response.json({ error: null, raceKey: "jra:20260824:05:09", status: "running" }),
  );
  dependencies.now = vi.fn(() => 1_900_000);

  expect(dependencies.pollStatus).not.toHaveBeenCalled();
  await expect(pollFocusedFullWatchTick(SCHEDULED, dependencies)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    error: "Focused-full completion watch timed out",
    outcome: "timeout",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
});

test("throws invalid and failed status responses so Queue retry remains durable", async () => {
  const invalid = makeDependencies(
    Response.json({ error: null, raceKey: "wrong", status: "running" }),
  );
  const unavailable = makeDependencies(new Response("offline", { status: 503 }));

  await expect(pollFocusedFullWatchTick(SCHEDULED, invalid)).rejects.toThrow("race key mismatch");
  await expect(pollFocusedFullWatchTick(SCHEDULED, unavailable)).rejects.toThrow(
    "status request returned 503",
  );
});

test("throws malformed status variants while preserving an explicit null-error failure", async () => {
  const nonObject = makeDependencies(Response.json(null));
  const invalidStatus = makeDependencies(
    Response.json({ error: null, raceKey: "jra:20260824:05:09", status: "accepted" }),
  );
  const invalidError = makeDependencies(
    Response.json({ error: 42, raceKey: "jra:20260824:05:09", status: "error" }),
  );
  const invalidProgress = makeDependencies(
    Response.json({
      error: null,
      lastProgressAtMs: "yesterday",
      raceKey: "jra:20260824:05:09",
      status: "running",
    }),
  );
  const invalidEvents = makeDependencies(
    Response.json({
      error: null,
      progressEvents: [42],
      raceKey: "jra:20260824:05:09",
      status: "success",
    }),
  );
  const missingError = makeDependencies(
    Response.json({ error: null, raceKey: "jra:20260824:05:09", status: "error" }),
  );
  const thrownValue = makeDependencies(Response.json({}));
  thrownValue.pollStatus = vi.fn(async () => {
    throw "network failed";
  });

  await expect(pollFocusedFullWatchTick(SCHEDULED, nonObject)).rejects.toThrow("not an object");
  await expect(pollFocusedFullWatchTick(SCHEDULED, invalidStatus)).rejects.toThrow(
    "invalid status",
  );
  await expect(pollFocusedFullWatchTick(SCHEDULED, invalidError)).rejects.toThrow("invalid error");
  await expect(pollFocusedFullWatchTick(SCHEDULED, invalidProgress)).rejects.toThrow(
    "invalid lastProgressAtMs",
  );
  await expect(pollFocusedFullWatchTick(SCHEDULED, invalidEvents)).rejects.toThrow(
    "invalid progressEvents",
  );
  await expect(pollFocusedFullWatchTick(SCHEDULED, missingError)).resolves.toStrictEqual({
    body: BODY,
    doName: "predict-jra-race-1",
    error: "Focused-full detached pipeline failed: jra:20260824:05:09",
    outcome: "error",
    role: "race-chain",
    type: "focused-full-completion",
    watchId: "watch-123",
    workKey: "focused-full:20260824:jra:05:09",
  });
  await expect(pollFocusedFullWatchTick(SCHEDULED, thrownValue)).rejects.toBe("network failed");
});
